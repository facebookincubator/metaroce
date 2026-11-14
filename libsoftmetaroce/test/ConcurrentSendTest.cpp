/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fmt/format.h>
#include <folly/io/Cursor.h>
#include <folly/synchronization/Baton.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/ConcurrentQpArbiter.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/Sender.h>
#include <libsoftmetaroce/congestion_control/FastStart.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <set>
#include <string>
#include <vector>

DECLARE_string(libsoftmetaroce_send);
DECLARE_uint32(libsoftmetaroce_min_rto_us);

using namespace testing;
namespace libsoftmetaroce {

namespace {
constexpr uint16_t kMetaRoceSendToPort = 0xace;

// Per-path send tallies plus the total frame count that drives the baton, so a
// test can confirm which egress path (write / writeGSO / writem) actually ran,
// not just that every frame went out.
struct SendPathTally {
  std::atomic<uint64_t> frames{0}; // segments/datagrams accepted (drives baton)
  std::atomic<uint64_t> plainWrites{0}; // write() calls
  std::atomic<uint64_t> gsoWrites{0}; // writeGSO() calls (incl. failed)
  std::atomic<uint64_t> writemWrites{0}; // writem() calls (incl. failed)
  std::atomic<uint64_t> writemFrames{0}; // datagrams writem() accepted
  std::mutex writemFsnsMutex;
  // Each flow's fsns, in the order writem() accepted them.
  std::map<uint16_t, std::vector<uint16_t>> writemFsnsByFlow;
};

// Mock socket whose sends are tallied per path; each send returns the chain
// length so it reads as successful.
std::pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
makeCountingSocket(
    folly::EventBase* evb,
    SendPathTally& tally,
    uint64_t expectedWrites,
    folly::Baton<>& allWritten,
    bool emulateGsoUnsupported = false,
    bool writemFails = false,
    bool writemPartial = false) {
  auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
  // Add each mock send's frame count to the running total, posting the baton
  // the first time it reaches `expectedWrites` so the test waits without
  // polling.
  auto recordWrites = [&tally, expectedWrites, &allWritten](uint64_t n) {
    const uint64_t prev = tally.frames.fetch_add(n, std::memory_order_relaxed);
    if (prev < expectedWrites && prev + n >= expectedWrites) {
      allWritten.post();
    }
  };
  ON_CALL(*socket, write(_, _))
      .WillByDefault([recordWrites, &tally](
                         const folly::SocketAddress&,
                         const std::unique_ptr<folly::IOBuf>& buf) {
        ++tally.plainWrites;
        recordWrites(1);
        return buf->computeChainDataLength();
      });
  ON_CALL(*socket, writeGSO(_, _, _))
      .WillByDefault(
          [recordWrites, emulateGsoUnsupported, &tally](
              const folly::SocketAddress&,
              const std::unique_ptr<folly::IOBuf>& buf,
              folly::AsyncUDPSocket::WriteOptions opts) -> ssize_t {
            ++tally.gsoWrites;
            if (emulateGsoUnsupported) {
              // Emulate a NIC lacking the checksum offload GSO needs: the send
              // fails at the syscall, exercising the per-packet fallback.
              errno = EIO;
              return -1;
            }
            const uint64_t total = buf->computeChainDataLength();
            EXPECT_LE(total, 65535u)
                << "a GSO datagram must fit one IPv6 packet (" << total
                << " bytes), else the kernel rejects it with EMSGSIZE";
            // Equal-size segments (the test sends full MTU); count them.
            recordWrites(
                opts.gso > 0 ? total / static_cast<uint64_t>(opts.gso) : 1);
            return static_cast<ssize_t>(total);
          });
  auto recordFsns = [&tally](const std::unique_ptr<folly::IOBuf>* bufs, int n) {
    std::lock_guard<std::mutex> lock(tally.writemFsnsMutex);
    for (int i = 0; i < n; ++i) {
      folly::io::Cursor cursor(bufs[i].get());
      BaseHeader baseHeader(cursor);
      DataHeader dataHeader(cursor);
      tally.writemFsnsByFlow[baseHeader.getFlowId()].push_back(
          dataHeader.getFsn());
    }
  };
  ON_CALL(*socket, writem(_, _, _))
      .WillByDefault(
          [recordWrites, recordFsns, writemFails, writemPartial, &tally](
              folly::Range<const folly::SocketAddress*>,
              const std::unique_ptr<folly::IOBuf>* bufs,
              size_t n) -> int {
            ++tally.writemWrites;
            if (writemFails) {
              // Emulate a NIC that cannot batch: writem fails, exercising the
              // per-packet fallback.
              errno = EIO;
              return -1;
            }
            if (writemPartial && n > 1) {
              // Emulate a short sendmmsg (the "N of M" partial): send all but
              // the last datagram; the sender must still deliver the unsent
              // tail.
              const int sent = static_cast<int>(n) - 1;
              tally.writemFrames += static_cast<uint64_t>(sent);
              recordFsns(bufs, sent);
              recordWrites(static_cast<uint64_t>(sent));
              return sent;
            }
            tally.writemFrames += n;
            recordFsns(bufs, static_cast<int>(n));
            recordWrites(n);
            return static_cast<int>(n);
          });
  return std::make_pair(std::move(socket), nullptr);
}
} // namespace

// N parallel consumers send K multi-frame QPs through a ConcurrentQpArbiter.
// Run repeatedly under TSAN for multi-thread coverage of the lock-free
// claim/release/signal path and the per-socket write lock; gsoEnabled=true also
// covers the GSO emit path (one writeGSO per same-socket run).
void runManyQps(
    bool gsoEnabled,
    bool emulateGsoUnsupported = false,
    bool sendmmsgEnabled = false,
    bool writemFails = false,
    bool writemPartial = false) {
  constexpr int kNumQps = 8;
  constexpr int kFramesPerQp = 16;
  constexpr uint32_t kNumSendingThreads = 4;
  // Large enough that one QP's frames (at the MTU below) exceed a single GSO
  // datagram, exercising both the batch path and emitRun's byte-cap
  // chunking into multiple writeGSO calls.
  constexpr uint32_t kSendBatch = 16;
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_send =
      sendmmsgEnabled ? "sendmmsg" : (gsoEnabled ? "gso" : "single");
  const uint64_t expected = uint64_t(kNumQps) * kFramesPerQp;

  SendPathTally tally;
  folly::Baton<> allWritten;
  auto socketMaker =
      [&tally, &allWritten, emulateGsoUnsupported, writemFails, writemPartial](
          folly::EventBase* evb) {
        return makeCountingSocket(
            evb,
            tally,
            expected,
            allWritten,
            emulateGsoUnsupported,
            writemFails,
            writemPartial);
      };

  HighResTimer retxTimer;
  MemoryRegions mrs;
  CompletionQueue cq;
  folly::IPAddress localAddr{"fc00::1"};
  folly::IPAddress remoteAddr{"fc00::101"};
  struct ibv_qp ibvQpStorage{};

  // Declaration order makes destruction run qps -> sender -> arbiter: ~Qp calls
  // arbiter.removeQp() and ~Sender shuts the arbiter down and joins the
  // consumers, so the arbiter must outlive both. payloads is declared first so
  // it is destroyed last: the consumers read the WQEs' raw payload pointers
  // until the sender joins them.
  std::vector<std::unique_ptr<char[]>> payloads;
  ConcurrentQpArbiter arbiter;
  Sender sender(
      arbiter,
      socketMaker,
      kMetaRoceSendToPort,
      kNumSendingThreads,
      kSendBatch);
  std::vector<std::unique_ptr<Qp>> qps;

  for (int i = 0; i < kNumQps; ++i) {
    uint32_t qpNum = 100 + i;
    auto qp = std::make_unique<Qp>(
        qpNum, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
    qp->addPortPair(localAddr, remoteAddr);
    qp->setRemoteQpNum(qpNum);
    qp->setMtu(4096);
    auto congestionController =
        std::make_unique<NiceMock<MockCongestionController>>();
    // A window large enough for the whole WQE lets every frame go out without
    // waiting for ACKs (there is no receiver in this test).
    ON_CALL(*congestionController, getAvailableWindowSize(_))
        .WillByDefault(Return(kFramesPerQp));
    qp->setCongestionController(std::move(congestionController));
    qp->setupFlows(1, 1, &sender);
    qp->startProcessingWqes();
    qps.push_back(std::move(qp));
  }

  for (auto& qp : qps) {
    const uint32_t length = kFramesPerQp * qp->getMtu();
    auto payload = std::make_unique<char[]>(length);
    ASSERT_TRUE(qp->postWqe(
        kMetaRoceInitialTag,
        std::make_unique<SendWqe>(
            0 /* workReqId */, oneSge(payload.get(), length), qp->getMtu())));
    payloads.push_back(std::move(payload));
  }

  // The mock socket never fails, so each write is a distinct frame: reaching
  // `expected` means every frame went out. A timeout here means the parallel
  // claim/release path stalled.
  EXPECT_TRUE(allWritten.try_wait_for(std::chrono::seconds(30)))
      << "Expected all " << expected << " frames written across "
      << kNumSendingThreads << " parallel consumers, but only "
      << tally.frames.load() << " were; the parallel send path stalled.";

  // Confirm the send took the path the mode selects, not just that frames went
  // out: each mode must use its own egress path and not another coalescing one.
  const uint64_t gso = tally.gsoWrites.load();
  const uint64_t writem = tally.writemWrites.load();
  const uint64_t plain = tally.plainWrites.load();
  if (sendmmsgEnabled) {
    EXPECT_GT(writem, 0u) << "sendmmsg mode must batch via writem";
    EXPECT_EQ(gso, 0u) << "sendmmsg mode must not use writeGSO";
    if (writemFails) {
      EXPECT_GT(plain, 0u)
          << "after writem fails, frames must fall back to per-packet write";
    }
  } else if (gsoEnabled) {
    EXPECT_GT(gso, 0u) << "gso mode must coalesce via writeGSO";
    EXPECT_EQ(writem, 0u) << "gso mode must not use writem";
    if (emulateGsoUnsupported) {
      EXPECT_GT(plain, 0u)
          << "after writeGSO fails, frames must fall back to per-packet write";
    }
  } else {
    EXPECT_GT(plain, 0u) << "single mode must send via per-packet write";
    EXPECT_EQ(gso, 0u) << "single mode must not use writeGSO";
    EXPECT_EQ(writem, 0u) << "single mode must not use writem";
  }
}

TEST(ConcurrentSendTest, ManyQpsThroughParallelConsumers) {
  runManyQps(/*gsoEnabled=*/false);
}

TEST(ConcurrentSendTest, ManyQpsThroughParallelConsumersGso) {
  runManyQps(/*gsoEnabled=*/true);
}

TEST(ConcurrentSendTest, ManyQpsThroughParallelConsumersSendmmsg) {
  runManyQps(
      /*gsoEnabled=*/false,
      /*emulateGsoUnsupported=*/false,
      /*sendmmsgEnabled=*/true);
}

TEST(ConcurrentSendTest, FallsBackToPerPacketWhenGsoWriteFails) {
  // GSO enabled but every writeGSO fails (EIO), as on a NIC without checksum
  // offload: the sender must disable GSO and complete via per-packet writes
  // instead of crashing on a fatal egress error.
  runManyQps(/*gsoEnabled=*/true, /*emulateGsoUnsupported=*/true);
}

// The whole WQE fits one GSO batch, so after the failed GSO write discards it,
// any frame reaching the wire is RTO recovering a discarded one.
TEST(ConcurrentSendTest, RtoRecoversFramesDiscardedByGsoFault) {
  constexpr int kFrames = 4;
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_send = "gso";
  // Fire RTO promptly so the recovery retransmit lands well within the wait.
  FLAGS_libsoftmetaroce_min_rto_us = 2000;

  std::mutex mtx;
  std::set<uint16_t> retransmittedFsns;
  folly::Baton<> recovered;
  std::atomic<uint64_t> gsoWrites{0};

  auto socketMaker = [&](folly::EventBase* evb) {
    auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
    // The GSO datagram never leaves the host, so the whole batch is discarded.
    ON_CALL(*socket, writeGSO(_, _, _))
        .WillByDefault(
            [&](const folly::SocketAddress&,
                const std::unique_ptr<folly::IOBuf>&,
                folly::AsyncUDPSocket::WriteOptions) -> ssize_t {
              ++gsoWrites;
              errno = EIO;
              return -1;
            });
    // GSO is disabled after the failure, so recovery retransmits arrive here
    // per-packet; record the RTO-driven ones (isRetransmit + isTimeout).
    ON_CALL(*socket, write(_, _))
        .WillByDefault(
            [&](const folly::SocketAddress&,
                const std::unique_ptr<folly::IOBuf>& buf) -> ssize_t {
              folly::io::Cursor cursor(buf.get());
              BaseHeader baseHeader(cursor);
              DataHeader dataHeader(cursor);
              if (dataHeader.getIsRetransmit() && dataHeader.getIsTimeout()) {
                std::lock_guard<std::mutex> lock(mtx);
                // RTO retransmits repeatedly with no receiver, but the baton
                // must be posted once; the first recovered frame is the proof.
                const bool first = retransmittedFsns.empty();
                retransmittedFsns.insert(dataHeader.getFsn());
                if (first) {
                  recovered.post();
                }
              }
              return static_cast<ssize_t>(buf->computeChainDataLength());
            });
    return std::make_pair(std::move(socket), nullptr);
  };

  HighResTimer retxTimer;
  MemoryRegions mrs;
  CompletionQueue cq;
  folly::IPAddress localAddr{"fc00::1"};
  folly::IPAddress remoteAddr{"fc00::101"};
  struct ibv_qp ibvQpStorage{};

  // Destruction order (reverse of declaration) runs qp -> sender -> arbiter,
  // with payload destroyed last since the sender reads it until it joins.
  std::unique_ptr<char[]> payload;
  ConcurrentQpArbiter arbiter;
  Sender sender(
      arbiter,
      socketMaker,
      kMetaRoceSendToPort,
      /*numSendingThreads=*/1u,
      /*sendBatch=*/static_cast<uint32_t>(kFrames));

  auto qp =
      std::make_unique<Qp>(100, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
  qp->addPortPair(localAddr, remoteAddr);
  qp->setRemoteQpNum(100);
  qp->setMtu(4096);
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  // A window covering the whole WQE lets all kFrames build into one GSO batch.
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(kFrames));
  qp->setCongestionController(std::move(congestionController));
  qp->setupFlows(1, 1, &sender);
  qp->startProcessingWqes();

  const uint32_t length = kFrames * qp->getMtu();
  payload = std::make_unique<char[]>(length);
  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          0 /* workReqId */, oneSge(payload.get(), length), qp->getMtu())))
      << "the WQE this case retransmits must post, or the RTO has nothing to "
         "recover and the test proves nothing";

  EXPECT_TRUE(recovered.try_wait_for(std::chrono::seconds(30)))
      << "the sender retransmitted no frame after the GSO batch was discarded; "
         "it must leave a frame built into a failed GSO datagram unacked so the "
         "RTO recovers it rather than losing it with the batch";

  EXPECT_GT(gsoWrites.load(), 0u)
      << "no GSO write was attempted, so the recovery above says nothing about "
         "a discarded GSO batch; the frames reached the wire some other way";

  qp.reset(); // remove from the arbiter before sender/arbiter tear down
}

TEST(ConcurrentSendTest, FallsBackToPerPacketWhenWritemFails) {
  // sendmmsg enabled but every writem fails (EIO), as on a NIC that cannot
  // batch: the sends must still complete. There is no writem-disable latch, so
  // the retransmit timer recovers them on the per-packet branch.
  runManyQps(
      /*gsoEnabled=*/false,
      /*emulateGsoUnsupported=*/false,
      /*sendmmsgEnabled=*/true,
      /*writemFails=*/true);
}

TEST(ConcurrentSendTest, ManyQpsWithPartialSendmmsg) {
  // sendmmsg enabled and every batched writem short-sends by one datagram (the
  // "N of M" partial seen under send-buffer / qdisc pressure). Every frame must
  // still be delivered: the sender must recover the unsent tail, not stall.
  runManyQps(
      /*gsoEnabled=*/false,
      /*emulateGsoUnsupported=*/false,
      /*sendmmsgEnabled=*/true,
      /*writemFails=*/false,
      /*writemPartial=*/true);
}

// send_batch>1 on the single-consumer QpArbiter takes the settled /
// no-reenqueue path; the QP must still drain via the ready set rather than
// strand.
TEST(ConcurrentSendTest, SendBatchWithSingleConsumerDrainsAllQps) {
  constexpr int kNumQps = 8;
  constexpr int kFramesPerQp = 16;
  constexpr uint32_t kNumSendingThreads = 1;
  constexpr uint32_t kSendBatch = 4;
  const uint64_t expected = uint64_t(kNumQps) * kFramesPerQp;

  SendPathTally tally;
  folly::Baton<> allWritten;
  auto socketMaker = [&tally, &allWritten](folly::EventBase* evb) {
    return makeCountingSocket(evb, tally, expected, allWritten);
  };

  HighResTimer retxTimer;
  MemoryRegions mrs;
  CompletionQueue cq;
  folly::IPAddress localAddr{"fc00::1"};
  folly::IPAddress remoteAddr{"fc00::101"};
  struct ibv_qp ibvQpStorage{};

  std::vector<std::unique_ptr<char[]>> payloads;
  QpArbiter arbiter;
  Sender sender(
      arbiter,
      socketMaker,
      kMetaRoceSendToPort,
      kNumSendingThreads,
      kSendBatch);
  std::vector<std::unique_ptr<Qp>> qps;

  for (int i = 0; i < kNumQps; ++i) {
    uint32_t qpNum = 100 + i;
    auto qp = std::make_unique<Qp>(
        qpNum, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
    qp->addPortPair(localAddr, remoteAddr);
    qp->setRemoteQpNum(qpNum);
    qp->setMtu(1000);
    auto congestionController =
        std::make_unique<NiceMock<MockCongestionController>>();
    ON_CALL(*congestionController, getAvailableWindowSize(_))
        .WillByDefault(Return(kFramesPerQp));
    qp->setCongestionController(std::move(congestionController));
    qp->setupFlows(1, 1, &sender);
    qp->startProcessingWqes();
    qps.push_back(std::move(qp));
  }

  for (auto& qp : qps) {
    const uint32_t length = kFramesPerQp * qp->getMtu();
    auto payload = std::make_unique<char[]>(length);
    ASSERT_TRUE(qp->postWqe(
        kMetaRoceInitialTag,
        std::make_unique<SendWqe>(
            0 /* workReqId */, oneSge(payload.get(), length), qp->getMtu())));
    payloads.push_back(std::move(payload));
  }

  EXPECT_TRUE(allWritten.try_wait_for(std::chrono::seconds(30)))
      << "Expected all " << expected
      << " frames drained by the single consumer at sendBatch=" << kSendBatch
      << ", but only " << tally.frames.load()
      << " were; a QP was stranded after its batch settled without a reenqueue.";
}

// The QP builds a batch a packet per flow in turn, each flow on its own
// socket, and the batch still takes one sendmmsg per socket.
TEST(ConcurrentSendTest, SendmmsgCarriesEachSocketsPacketsInOneCall) {
  constexpr int kNumFlows = 2;
  constexpr int kFramesPerFlow = 4;
  constexpr uint32_t kFrames = kNumFlows * kFramesPerFlow;
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_send = "sendmmsg";

  SendPathTally tally;
  folly::Baton<> allWritten;
  auto socketMaker = [&tally, &allWritten](folly::EventBase* evb) {
    return makeCountingSocket(evb, tally, kFrames, allWritten);
  };

  HighResTimer retxTimer;
  MemoryRegions mrs;
  CompletionQueue cq;
  struct ibv_qp ibvQpStorage{};

  // Destruction order (reverse of declaration) runs qp -> sender -> arbiter,
  // with payload destroyed last since the sender reads it until it joins.
  std::unique_ptr<char[]> payload;
  ConcurrentQpArbiter arbiter;
  // One send loop and a batch that holds the whole WQE, so the WQE goes out
  // as a single batch.
  Sender sender(
      arbiter,
      socketMaker,
      kMetaRoceSendToPort,
      /*numSendingThreads=*/1u,
      /*sendBatch=*/kFrames);

  auto qp =
      std::make_unique<Qp>(100, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
  qp->addPortPair(folly::IPAddress{"fc00::1"}, folly::IPAddress{"fc00::101"});
  qp->setRemoteQpNum(100);
  qp->setMtu(1000);
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(kFrames));
  qp->setCongestionController(std::move(congestionController));
  qp->setupFlows(kNumFlows, kNumFlows, &sender);
  qp->startProcessingWqes();

  const uint32_t length = kFrames * qp->getMtu();
  payload = std::make_unique<char[]>(length);
  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          0 /* workReqId */, oneSge(payload.get(), length), qp->getMtu())))
      << "precondition: the QP accepts the WQE, or no batch is built";

  ASSERT_TRUE(allWritten.try_wait_for(std::chrono::seconds(30)))
      << "the sender wrote only " << tally.frames.load() << " of " << kFrames
      << " frames, so the batch never finished sending";

  EXPECT_EQ(tally.writemFrames.load(), kFrames)
      << "every frame of the batch must go out in a sendmmsg; "
      << tally.plainWrites.load()
      << " went out one sendmsg each, which pays a system call per packet";
  EXPECT_EQ(tally.writemWrites.load(), static_cast<uint64_t>(kNumFlows))
      << "the batch must take one sendmmsg per socket; more calls mean a "
         "socket's packets were split across calls";
  std::vector<uint16_t> fsnsPerFlow(kFramesPerFlow);
  std::iota(fsnsPerFlow.begin(), fsnsPerFlow.end(), uint16_t{0});
  const std::map<uint16_t, std::vector<uint16_t>> expectedFsnsByFlow{
      {0, fsnsPerFlow}, {1, fsnsPerFlow}};
  {
    std::lock_guard<std::mutex> lock(tally.writemFsnsMutex);
    EXPECT_EQ(tally.writemFsnsByFlow, expectedFsnsByFlow)
        << "grouping must keep each socket's packets in build order; a flow "
           "reaching its socket out of fsn order shows the receiver a gap and "
           "then a late frame";
  }

  qp.reset(); // leave the arbiter before sender/arbiter tear down
}

namespace {
// One coalesced write as the wire would see it: which path carried it (by
// destination) and how many packets the datagram segments into.
struct GsoWrite {
  folly::IPAddress dest;
  uint64_t packets{0};
  bool coalesced{false};
};

std::string renderTrace(const std::vector<GsoWrite>& trace) {
  std::string out;
  size_t i = 0;
  while (i < trace.size()) {
    size_t run = 1;
    while (i + run < trace.size() && trace[i + run].dest == trace[i].dest &&
           trace[i + run].packets == trace[i].packets &&
           trace[i + run].coalesced == trace[i].coalesced) {
      ++run;
    }
    out += fmt::format(
        "{}{}{} x{}{}",
        out.empty() ? "" : ", ",
        trace[i].dest.str(),
        trace[i].coalesced ? "" : "(plain)",
        trace[i].packets,
        run > 1 ? fmt::format(" [{} writes]", run) : "");
    i += run;
  }
  return out;
}
} // namespace

/*
 * Shows the send pattern gso actually produces, in two phases, by recording
 * every write the socket receives: the destination, and for a coalesced write
 * the number of packets its GSO datagram segments into.
 *
 * Fast start owns the first phase and gso follows it rather than widening it:
 * an opening burst of fast_start_burst packets per path (which coalesce, being
 * consecutive on one socket), then one packet per path as configured by
 * packets_per_visit, which cannot coalesce and must not be made to. Once an ack
 * reporting congestion arrives fast start ends, and from then on the congestion
 * window decides: the window the ack frees on that path is sent as a single
 * coalesced datagram.
 */
void runGsoFastStartTrace(uint16_t packetsPerVisit) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_send = "gso";
  // Long enough that no retransmission perturbs the trace.
  FLAGS_libsoftmetaroce_min_rto_us = 5'000'000;
  const uint16_t burst =
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_fast_start_burst);
  constexpr int kNumFlows = 4;
  // One consumer, so the recorded order is the order the QP produced.
  constexpr uint32_t kNumSendingThreads = 1;
  const uint16_t mtu = 4096;
  // The opening burst on every path, then two spread rounds.
  const uint32_t kFastStartPackets =
      static_cast<uint32_t>(kNumFlows) * burst + 2 * kNumFlows;
  // Acking through this fsn on path 0 frees that many packets of its window.
  constexpr uint16_t kAckThroughFsn = 4;
  const uint16_t kFreedPackets = kAckThroughFsn + 1;

  std::mutex traceMutex;
  std::condition_variable traceCv;
  std::vector<GsoWrite> trace;
  uint64_t packetsWritten = 0;

  auto record =
      [&](const folly::SocketAddress& to, uint64_t packets, bool coalesced) {
        {
          std::lock_guard<std::mutex> lock(traceMutex);
          trace.push_back({to.getIPAddress(), packets, coalesced});
          packetsWritten += packets;
        }
        traceCv.notify_all();
      };
  auto waitForPackets = [&](uint64_t target) {
    std::unique_lock<std::mutex> lock(traceMutex);
    return traceCv.wait_for(lock, std::chrono::seconds(30), [&] {
      return packetsWritten >= target;
    });
  };
  // For the timeout diagnostics below: those fire while the send threads are
  // still recording, so the trace has to be read under the lock.
  auto renderTraceLocked = [&] {
    std::lock_guard<std::mutex> lock(traceMutex);
    return renderTrace(trace);
  };

  auto socketMaker = [&record](folly::EventBase* evb) {
    auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
    ON_CALL(*socket, writeGSO(_, _, _))
        .WillByDefault(
            [&record](
                const folly::SocketAddress& to,
                const std::unique_ptr<folly::IOBuf>& buf,
                folly::AsyncUDPSocket::WriteOptions opts) -> ssize_t {
              const uint64_t total = buf->computeChainDataLength();
              record(
                  to,
                  opts.gso > 0 ? total / static_cast<uint64_t>(opts.gso) : 1,
                  /*coalesced=*/true);
              return static_cast<ssize_t>(total);
            });
    ON_CALL(*socket, write(_, _))
        .WillByDefault(
            [&record](
                const folly::SocketAddress& to,
                const std::unique_ptr<folly::IOBuf>& buf) -> ssize_t {
              record(to, 1, /*coalesced=*/false);
              return static_cast<ssize_t>(buf->computeChainDataLength());
            });
    return std::make_pair(std::move(socket), nullptr);
  };

  HighResTimer retxTimer;
  MemoryRegions mrs;
  CompletionQueue cq;
  struct ibv_qp ibvQpStorage{};

  std::vector<std::unique_ptr<char[]>> payloads;
  ConcurrentQpArbiter arbiter;
  Sender sender(
      arbiter,
      socketMaker,
      kMetaRoceSendToPort,
      kNumSendingThreads,
      /*sendBatch=*/64);

  auto qp =
      std::make_unique<Qp>(100, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
  // A distinct remote address per path, so the trace identifies the path that
  // carried each write.
  for (int i = 0; i < kNumFlows; ++i) {
    qp->addPortPair(
        folly::IPAddress(fmt::format("fc00::1{}", i)),
        folly::IPAddress(fmt::format("fc00::10{}", i)));
  }
  qp->setRemoteQpNum(100);
  qp->setMtu(mtu);
  // The real controller, so fast start drives the schedule under test.
  auto cc = std::make_unique<FastStart>();
  cc->setPacketsPerVisit(packetsPerVisit);
  qp->setCongestionController(std::move(cc));
  qp->setupFlows(kNumFlows, kNumFlows, &sender);
  qp->startProcessingWqes();

  auto postPackets = [&](uint32_t packets) {
    const uint32_t length = packets * mtu;
    auto payload = std::make_unique<char[]>(length);
    ASSERT_TRUE(qp->postWqe(
        kMetaRoceInitialTag,
        std::make_unique<SendWqe>(
            0 /* workReqId */, oneSge(payload.get(), length), mtu)));
    payloads.push_back(std::move(payload));
  };

  // Phase 1: exactly the fast-start traffic, so the QP runs dry there.
  postPackets(kFastStartPackets);
  ASSERT_TRUE(waitForPackets(kFastStartPackets))
      << "fast start should have placed " << kFastStartPackets
      << " packets; trace: " << renderTraceLocked();
  const size_t fastStartWrites = [&] {
    std::lock_guard<std::mutex> lock(traceMutex);
    return trace.size();
  }();

  // Phase 2: the QP has run dry, so only an ack reporting congestion ends fast
  // start; this one does, and frees path 0's window.
  auto* flow0 = qp->getTxFlowById(0);
  ASSERT_NE(flow0, nullptr) << "path 0 should exist to ack on";
  flow0->processAckPacket(
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setCumulativeFsn(kAckThroughFsn)
          .setMaxRcvdFsn(kAckThroughFsn)
          .setAckStatus(AckStatus::ACK)
          .setCnpCE(1)
          .setTimestamp(0)
          .build());

  // Phase 3: only path 0 has window now, and gso should send all of it at once.
  postPackets(kFastStartPackets);
  ASSERT_TRUE(waitForPackets(kFastStartPackets + kFreedPackets))
      << "the window freed on path 0 should have been sent; trace: "
      << renderTraceLocked();

  qp.reset(); // leave the arbiter before sender/arbiter tear down

  std::lock_guard<std::mutex> lock(traceMutex);
  const std::string rendered = renderTrace(trace);
  // The point of this test is the pattern, so show it even when it passes,
  // with the settings that determine it.
  LOG(INFO) << fmt::format(
      "gso send trace ({} paths, fast_start_burst={}, packets_per_visit={}): {}",
      kNumFlows,
      burst,
      packetsPerVisit,
      rendered);

  // Fast start: a coalesced opening burst per path, then single packets, since
  // one packet per path is what packets_per_visit asks for.
  ASSERT_GE(trace.size(), static_cast<size_t>(kNumFlows) + 1)
      << "expected an opening burst per path; trace: " << rendered;
  std::set<folly::IPAddress> openingFlows;
  for (int i = 0; i < kNumFlows; ++i) {
    EXPECT_TRUE(trace[i].coalesced)
        << "opening write " << i
        << " should be a GSO datagram, since a burst is consecutive on one "
           "path; trace: "
        << rendered;
    EXPECT_EQ(trace[i].packets, burst)
        << "opening write " << i
        << " should carry a full fast-start burst; trace: " << rendered;
    openingFlows.insert(trace[i].dest);
  }
  EXPECT_EQ(openingFlows.size(), static_cast<size_t>(kNumFlows))
      << "each opening burst should land on a path of its own, so the probe "
         "spreads instead of piling onto one; trace: "
      << rendered;
  for (size_t i = kNumFlows; i < fastStartWrites; ++i) {
    EXPECT_EQ(trace[i].packets, packetsPerVisit)
        << "write " << i << " is past the opening burst, where each visit "
        << "places packets_per_visit=" << packetsPerVisit
        << " packets; trace: " << rendered;
    EXPECT_EQ(trace[i].coalesced, packetsPerVisit > 1)
        << "write " << i << " should coalesce only when a visit places more "
        << "than one packet, which is what gso can join; trace: " << rendered;
  }

  // After the ack: fast start is over and the window governs, so everything it
  // freed on path 0 leaves in one datagram.
  ASSERT_GT(trace.size(), fastStartWrites)
      << "the freed window should have produced a write; trace: " << rendered;
  const auto& freed = trace[fastStartWrites];
  EXPECT_TRUE(freed.coalesced)
      << "out of fast start the window bounds the run, so the packets it freed "
         "should coalesce rather than go one at a time; trace: "
      << rendered;
  EXPECT_EQ(freed.packets, kFreedPackets)
      << "the ack freed " << kFreedPackets
      << " packets of window on path 0, and all of them should ride one "
         "datagram; trace: "
      << rendered;
  EXPECT_EQ(freed.dest, trace[0].dest)
      << "the freed window is on the acked path, so the write should go there; "
         "trace: "
      << rendered;
}

/*
 * The configured spread: one packet per path after the opening burst, so
 * consecutive packets sit on different paths and nothing coalesces there.
 */
TEST(ConcurrentSendTest, GsoFollowsFastStartThenCoalescesTheFreedWindow) {
  runGsoFastStartTrace(/*packetsPerVisit=*/1);
}

/*
 * Asking for two packets per visit pairs them on a path, and gso joins each
 * pair -- the same schedule, with the run length the sender asked for.
 */
TEST(ConcurrentSendTest, GsoCoalescesPairedVisitsDuringFastStart) {
  runGsoFastStartTrace(/*packetsPerVisit=*/2);
}

} // namespace libsoftmetaroce
