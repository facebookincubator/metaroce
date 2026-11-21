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

// Microbenchmark for the cost of TxFlow::processAckPacket's SACK scan (loop 2)
// as a function of window depth.
//
// The "gappedRescan" case models the pathology found in profiling: a persistent
// head-of-line hole with every later FSN already SACKed, so loop 2's SACK scan
// re-touches the whole outstanding range every ACK -- O(window), ~90% of it
// already-resolved.

#include <folly/Benchmark.h>
#include <folly/CancellationToken.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/init/Init.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/portability/GMock.h>

#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/HighResTimer.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/codec/AckHeader.h>
#include <libsoftmetaroce/codec/BitVector.h>
#include <libsoftmetaroce/congestion_control/CongestionController.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>

#include <vector>

using namespace libsoftmetaroce;
using ::testing::NiceMock;

namespace {

constexpr uint16_t kMtu = 1000;
constexpr uint16_t kSendToPort = 0xace;

// Hand-written always-sending congestion controller. A gmock NiceMock adds
// per-call overhead that would dominate the per-ACK timing, so the benchmark
// uses a concrete fake with trivial inline methods instead.
class BenchCongestionController : public CongestionController {
 public:
  void onPacketSent(uint16_t /*flowId*/) override {}
  AckResult onPacketAck(uint16_t /*flowId*/, const AckEvent& /*ack*/) override {
    return {};
  }
  void onPacketLoss(uint16_t /*flowId*/, const LossEvent& /*loss*/) override {}
  void onFlowAdded(uint16_t /*flowId*/) override {}
  void onFlowDisabled(uint16_t /*flowId*/) override {}
  uint16_t getAvailableWindowSize(uint16_t /*flowId*/) const override {
    return 65535;
  }
  uint16_t getFwnd(uint16_t /*flowId*/) const override {
    return 65535;
  }
  uint32_t getQwnd() const override {
    return 65535;
  }
};

std::pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

// Process posted WQEs into their Tags once. Qp::processWqes() is a forever
// loop, so run it under a cancellation token and cancel it as soon as the
// expected WQEs have been posted (the pattern QpTestBase uses).
void processPostedWqes(Qp* qp, int numWqes) {
  folly::CancellationSource cs;
  auto procTask = [qp, cs]() -> folly::coro::Task<bool> {
    try {
      co_await folly::coro::co_withCancellation(
          cs.getToken(), qp->processWqes());
    } catch (const folly::OperationCancelled&) {
    }
    co_return true;
  };
  auto waitTask = [qp, numWqes, cs]() -> folly::coro::Task<bool> {
    bool found = qp->waitForWqesToBePosted(numWqes, std::chrono::seconds(5));
    cs.requestCancellation();
    co_return found;
  };
  auto [_, posted] = folly::coro::blockingWait(
      folly::coro::collectAll(
          folly::coro::co_withExecutor(
              folly::getGlobalCPUExecutor(), procTask()),
          folly::coro::co_withExecutor(
              folly::getGlobalCPUExecutor(), waitTask())));
  // A timed-out wait would leave the in-flight tracker empty, so the timed
  // processAckPacket calls would report meaningless numbers; abort instead.
  CHECK(posted) << numWqes
                << " WQE(s) were not posted within the setup timeout";
}

// A QP whose flow 0 has `window` UNACKED packets in flight (sndUna=0,
// sndNxt=window), so processAckPacket has a full tracker to scan.
struct AckBenchFixture {
  HighResTimer retxTimer;
  QpArbiter arbiter;
  MemoryRegions mrs;
  CompletionQueue cq;
  struct ibv_qp ibvQp{};
  std::vector<char> payload;
  std::unique_ptr<LimitedTestSender> sender;
  std::unique_ptr<Qp> qp;
  TxFlow* flow{nullptr};

  explicit AckBenchFixture(uint16_t window)
      : payload(static_cast<size_t>(window) * kMtu, 'x') {
    qp = std::make_unique<Qp>(10, retxTimer, arbiter, mrs, &cq, &ibvQp);
    qp->addPortPair(folly::IPAddress("fc00::1"), folly::IPAddress("fc00::101"));
    qp->setRemoteQpNum(100);
    qp->setMtu(kMtu);
    qp->setCongestionController(std::make_unique<BenchCongestionController>());
    sender = std::make_unique<LimitedTestSender>(
        arbiter,
        mockUDPSocketMaker,
        kSendToPort,
        LimitedTestSender::kDisableSendLoop);
    // NEVER mode: applying a SACK ACK exercises only loop 2, without queueing
    // retransmits (which would grow unboundedly across benchmark iterations).
    qp->setupFlows(
        1,
        1,
        sender.get(),
        SackRetransmissionConfig{SackRetransmissionMode::NEVER});
    // Configured here to keep the per-ACK unconfigured-cdmsn warning out of
    // the measurement.
    const bool configured =
        qp->getTag(kMetaRoceInitialTag)
            ->setReceiveSequenceNumbers({.nextMsn = 1, .nextCsn = 0});
    CHECK(configured) << "an unconfigured tag warns on every ACK, which would "
                         "dominate what this measures";
    const bool posted = qp->postWqe(
        kMetaRoceInitialTag,
        std::make_unique<SendWqe>(0, oneSge(payload), kMtu));
    CHECK(posted) << "the window this measures is the work of this one post";
    // Process the posted WQE into its Tag so findNextWqe (and thus
    // getNextPacketToSend) can produce its packets.
    processPostedWqes(qp.get(), 1);
    flow = qp->getTxFlowById(0);
    // Drain `window` packets onto the wire to populate the in-flight tracker.
    for (uint16_t i = 0; i < window; ++i) {
      auto pkt = flow->getNextPacketToSend();
      folly::doNotOptimizeAway(pkt);
    }
  }
};

// SACK ACK with a persistent hole at FSN 0 and FSNs [1, window) all SACKed.
// cumulativeFsn = sndUna-1 (0xFFFF) so it advances nothing; the whole range is
// re-examined by loop 2 each time it is applied.
AckHeader makeGappedSackAck(uint16_t window) {
  BitVector<AckHeader::kAckTrackerBits> fsnBits;
  // With cumulativeFsn = 0xFFFF, the bitmap index for FSN f is f itself.
  for (uint16_t f = 1; f < window; ++f) {
    fsnBits.setBit(f);
  }
  return AckHeader::AckHeaderBuilder()
      .setTag(kMetaRoceInitialTag)
      .setCumulativeFsn(static_cast<uint16_t>(0xFFFF))
      .setMaxRcvdFsn(static_cast<uint16_t>(window - 1))
      .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
      // A current-clock timestamp: zero is rejected as "not set" (and floods
      // the output with per-packet errors on the first apply, which newly-ACKs
      // [1, window)), while a stale value yields a bogus RTT. getNowNs() makes
      // the RTT update take the same valid path it would in production.
      .setTimestamp(getNowNs())
      .setFsnBitVector(fsnBits)
      .build();
}

void ackGappedRescan(unsigned n, int window) {
  folly::BenchmarkSuspender suspender;
  AckBenchFixture fx(static_cast<uint16_t>(window));
  AckHeader ack = makeGappedSackAck(static_cast<uint16_t>(window));
  // Hold the receipt time constant across iterations -- a nonzero RTT past the
  // ack's send timestamp -- so the timed loop measures the loop-2 scan, not a
  // per-iteration getNowNs() clock read.
  const uint32_t nowNs = ack.getTimestamp() + 1000;
  // Warm up: this first apply marks [1, window) ACKED; subsequent applies are
  // pure re-scans (the hole at 0 keeps sndUna pinned), so the per-iteration
  // state is stable and each iteration is exactly one loop-2 scan.
  fx.flow->processAckPacket(ack, nowNs);
  suspender.dismiss();
  for (unsigned i = 0; i < n; ++i) {
    fx.flow->processAckPacket(ack, nowNs);
    // Consume a value the scan actually wrote, so the call cannot be elided.
    folly::doNotOptimizeAway(fx.flow->getTxState().estRttNs);
  }
  // Re-suspend timing before fx is destroyed so teardown is not counted.
  suspender.rehire();
}

} // namespace

BENCHMARK_PARAM(ackGappedRescan, 16)
BENCHMARK_PARAM(ackGappedRescan, 64)
BENCHMARK_PARAM(ackGappedRescan, 256)

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
