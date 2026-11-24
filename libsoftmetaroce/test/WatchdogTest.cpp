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

#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Timeout.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Cqe.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/WatchdogTimer.h>
#include <libsoftmetaroce/test/AckProcessedAt.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <array>
#include <chrono>
#include <set>

// NOLINTNEXTLINE(facebook-hte-PreventUseOfDeclareGflag)
DECLARE_bool(libsoftmetaroce_slow_receiver);
// NOLINTNEXTLINE(facebook-hte-PreventUseOfDeclareGflag)
DECLARE_string(libsoftmetaroce_send);
// NOLINTNEXTLINE(facebook-hte-PreventUseOfDeclareGflag)
DECLARE_uint32(libsoftmetaroce_min_rto_us);

using namespace testing;
namespace libsoftmetaroce {

constexpr uint16_t kMetaRoceSendToPort = 0xace;
constexpr int kTestFrameCount = 4;
constexpr std::chrono::seconds kTestTimeout{10};
constexpr int kCongestionWindowSize = 10;
constexpr uint32_t kWatchdogIntervalMs = 50; // Short interval for testing

static std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

class WatchdogTest : public Test {
 protected:
  void SetUp() override {
    // Set a short watchdog interval for testing
    FLAGS_libsoftmetaroce_qp_watchdog_interval_ms = kWatchdogIntervalMs;
    // Off here: the slow-receiver prior seeds mdev at 250ms, which puts the
    // first RTO near a second.
    FLAGS_libsoftmetaroce_slow_receiver = false;

    buildQp();
  }

  void buildQp() {
    // Torn down first: two QPs sharing this number must not overlap.
    qp_.reset();
    qp_ = std::make_unique<Qp>(10, retxTimer_, arbiter_, mrs_, &cq_, &ibvQp_);
    qp_->addPortPair(localAddr_, remoteAddr_);
    qp_->setRemoteQpNum(100);
    qp_->setMtu(1000);
    auto congestionController =
        std::make_unique<NiceMock<MockCongestionController>>();
    rawCongestionController_ = congestionController.get();
    qp_->setCongestionController(std::move(congestionController));
  }

  void TearDown() override {
    qp_ = nullptr;
  }

  std::unique_ptr<LimitedTestSender> createTestSenderWithFlows(
      uint32_t numFlows,
      bool disableSendLoop = false) {
    auto sender = std::make_unique<LimitedTestSender>(
        arbiter_, mockUDPSocketMaker, kMetaRoceSendToPort, disableSendLoop);
    qp_->setupFlows(numFlows, numFlows, sender.get());
    return sender;
  }

  MockUDPSocket* getTestSocket(LimitedTestSender* sender) {
    auto socketId = sender->getSocketIdFromAddress(localAddr_);
    return dynamic_cast<MockUDPSocket*>(sender->getSocketById(socketId));
  }

  // Counts every socket write and signals each one.
  void startCountingWritesOn(MockUDPSocket* socket) {
    EXPECT_CALL(*socket, write(_, _))
        .WillRepeatedly([this](
                            const folly::SocketAddress&,
                            const std::unique_ptr<folly::IOBuf>& buf) {
          std::lock_guard<std::mutex> lock(packetMutex_);
          ++packetsSent_;
          packetSentCv_.notify_all();
          return buf.get()->computeChainDataLength();
        });
  }

  // Locked, since the socket mock may still be writing when a test reads it.
  [[nodiscard]] int packetsSent() const {
    std::lock_guard<std::mutex> lock(packetMutex_);
    return packetsSent_;
  }

  [[nodiscard]] bool waitForPacketsSentAtLeast(int count) {
    std::unique_lock<std::mutex> lock(packetMutex_);
    return packetSentCv_.wait_for(
        lock, kTestTimeout, [this, count]() { return packetsSent_ >= count; });
  }

  [[nodiscard]] bool waitForAnyPacketSent() {
    return waitForPacketsSentAtLeast(1);
  }

  void postTestWqe(int frames, char* payload, uint64_t workReqId = 0) {
    const uint32_t length = frames * qp_->getMtu();
    ASSERT_TRUE(qp_->postWqe(
        kMetaRoceInitialTag,
        std::make_unique<SendWqe>(
            workReqId, oneSge(payload, length), qp_->getMtu())));
  }

  HighResTimer retxTimer_;
  QpArbiter arbiter_;
  MemoryRegions mrs_;
  folly::IPAddress localAddr_{"fc00::1"};
  folly::IPAddress remoteAddr_{"fc00::101"};
  std::unique_ptr<Qp> qp_;
  CompletionQueue cq_;
  MockCongestionController* rawCongestionController_{nullptr};
  struct ibv_qp ibvQp_{};
  gflags::FlagSaver flagSaver_;

  mutable std::mutex packetMutex_;
  std::condition_variable packetSentCv_;
  int packetsSent_{0};
};

/*
 * Test that when no ACKs are received within the watchdog interval,
 * the watchdog expires and WQEs are completed in error.
 *
 * This test:
 * 1. Creates a QP with 1 path
 * 2. Posts 2 WQEs
 * 3. Sends packets but never ACKs them
 * 4. Waits for the watchdog to expire
 * 5. Verifies that WQEs are completed with kWcRetryExcErr and kWrFlushErr
 */
TEST_F(WatchdogTest, WatchdogExpiresWithoutAcks) {
  auto sender = createTestSenderWithFlows(1);
  auto socket = getTestSocket(sender.get());
  ASSERT_NE(socket, nullptr);

  ON_CALL(*rawCongestionController_, getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));

  startCountingWritesOn(socket);

  // Post 2 WQEs
  auto payload1 = std::make_unique<char[]>(kTestFrameCount * qp_->getMtu());
  auto payload2 = std::make_unique<char[]>(kTestFrameCount * qp_->getMtu());

  postTestWqe(kTestFrameCount, payload1.get(), 100);
  postTestWqe(kTestFrameCount, payload2.get(), 101);

  qp_->startProcessingWqes();

  ASSERT_TRUE(waitForAnyPacketSent())
      << "precondition: a packet has to be in flight for the watchdog to have "
         "work to fail";

  // Wait for the watchdog to expire and completions to arrive using async
  // pollCq with timeout (avoids sleep-based polling)
  auto cqe1 = folly::coro::blockingWait(
      folly::coro::timeout(cq_.pollCq(), kTestTimeout));
  auto cqe2 = folly::coro::blockingWait(
      folly::coro::timeout(cq_.pollCq(), kTestTimeout));

  // Verify the watchdog expired
  EXPECT_TRUE(qp_->hasWatchdogExpired())
      << "hasWatchdogExpired() should return true after watchdog timeout";

  // Verify completions
  // We posted 2 WQEs, so we should have 2 completions:
  // - 1 with kWcRetryExcErr (the first WQE - retry exceeded)
  // - 1 with kWrFlushErr (the second WQE - flushed)
  std::set<WorkCompletionStatus> expectedStatuses = {
      kWcRetryExcErr, kWrFlushErr};
  std::set<WorkCompletionStatus> actualStatuses = {cqe1.status, cqe2.status};
  EXPECT_EQ(actualStatuses, expectedStatuses)
      << "Should have one kWcRetryExcErr and one kWrFlushErr completion";

  // Verify work request IDs
  std::set<uint64_t> expectedWorkReqIds = {100, 101};
  std::set<uint64_t> actualWorkReqIds = {cqe1.workReqId, cqe2.workReqId};
  EXPECT_EQ(actualWorkReqIds, expectedWorkReqIds)
      << "Should have completions for both WQEs";

  EXPECT_TRUE(qp_->getTag(kMetaRoceInitialTag)->isInErrorState())
      << "the watchdog must leave the tag in error: a client that polls the "
         "state after its work fails has to see it, and the sender needs the "
         "sweep it asks for";

  LOG(INFO) << "Test completed: " << packetsSent() << " packets sent, "
            << cq_.getNumCompletedWqes() << " CQ completions";

  sender = nullptr;
}

/*
 * Test that an ACK records watchdog progress, which is what defers expiry.
 *
 * The message here completes in microseconds, far inside the 50ms interval, so
 * nothing is ever close to expiring: the watchdog's own expiry behavior is
 * covered by WatchdogExpiresWithoutAcks. What this pins is the input to it,
 * that each ACK moves the last-progress time forward. Recording progress is
 * asserted directly rather than inferred from a completion, so the check does
 * not depend on how long the test takes to run.
 */
TEST_F(WatchdogTest, WatchdogResetsOnAck) {
  // Disable the send loop and pump each frame from this thread, so the test
  // turns on the acks it issues rather than on how fast a live loop drains
  // four packets.
  auto sender =
      createTestSenderWithFlows(1, LimitedTestSender::kDisableSendLoop);

  ON_CALL(*rawCongestionController_, getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));

  auto flow0 = qp_->getTxFlowById(0);

  auto payload = std::make_unique<char[]>(kTestFrameCount * qp_->getMtu());
  postTestWqe(kTestFrameCount, payload.get(), 200);

  qp_->startProcessingWqes();
  ASSERT_TRUE(qp_->waitForWqesToBePosted(1))
      << "Timed out waiting for the WQE to be processed";

  // Sample after posting: posting also records progress, so taking the mark
  // here leaves the acks below as the only thing that can move it.
  const auto progressBeforeAcks = qp_->getLastProgressTime();

  // Send and ack every frame.
  for (int frame = 0; frame < kTestFrameCount; ++frame) {
    auto egressPacket = qp_->getNextPacketToSend();
    ASSERT_NE(egressPacket, nullptr) << "the QP should offer frame " << frame;
    ASSERT_NE(egressPacket->packet, nullptr)
        << "frame " << frame << " should carry a packet";
    auto* dataPacket = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    ASSERT_NE(dataPacket, nullptr)
        << "frame " << frame << " should be a data packet";
    const auto& dataHeader = dataPacket->getDataHeader();

    auto ack = AckHeader::AckHeaderBuilder()
                   .setTag(kMetaRoceInitialTag)
                   .setCumulativeFsn(dataHeader.getFsn())
                   .setMaxRcvdFsn(dataHeader.getFsn())
                   .setAckStatus(AckStatus::ACK)
                   .setTimestamp(dataHeader.getTimestamp())
                   .build();
    flow0->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));
  }

  EXPECT_GT(qp_->getLastProgressTime(), progressBeforeAcks)
      << "acking a frame must record watchdog progress; a sender that does not "
         "would let the watchdog expire under a message still being acked";

  // Dequeue rather than read the depth: the ack is applied on this thread but
  // the completion reaches the CQ through the tag's in-order path, so the
  // blocking poll is what waits for it to land.
  auto cqe = folly::coro::blockingWait(
      folly::coro::timeout(cq_.pollCq(), kTestTimeout));
  EXPECT_EQ(cqe.status, kSuccess)
      << "a fully acked WQE must complete successfully";
  EXPECT_EQ(cqe.workReqId, 200) << "Should have completion for our WQE";
  EXPECT_EQ(cq_.getNumCompletedWqes(), 0)
      << "no completion beyond the one polled above should be queued";

  sender = nullptr;
}

/*
 * Test that when every packet is ACKed with HRNR, after the watchdog interval
 * expires, the completion is raised with kWcRnrRetryExcErr.
 *
 * This test:
 * 1. Creates a QP with 1 path
 * 2. Posts 2 WQEs
 * 3. Responds to every packet with HRNR (Hard Receiver Not Ready)
 * 4. Waits for the watchdog to expire
 * 5. Verifies that the first WQE is completed with kWcRnrRetryExcErr
 *    (not kWcRetryExcErr) since we're in HRNR state
 */
TEST_F(WatchdogTest, WatchdogExpiresWithHrnrReturnsRnrRetryExcErr) {
  // Disable the send loop so no consumer thread reads hrnrState_ concurrently
  // with the HRNR backoff timer that the HRNR below schedules.
  auto sender =
      createTestSenderWithFlows(1, LimitedTestSender::kDisableSendLoop);

  ON_CALL(*rawCongestionController_, getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));

  auto flow0 = qp_->getTxFlowById(0);

  // Post 2 WQEs
  auto payload1 = std::make_unique<char[]>(kTestFrameCount * qp_->getMtu());
  auto payload2 = std::make_unique<char[]>(kTestFrameCount * qp_->getMtu());

  postTestWqe(kTestFrameCount, payload1.get(), 300);
  postTestWqe(kTestFrameCount, payload2.get(), 301);

  qp_->startProcessingWqes();
  ASSERT_TRUE(qp_->waitForWqesToBePosted(1))
      << "Timed out waiting for the WQE to be processed";

  // Pump one packet and respond with HRNR (Hard Receiver Not Ready) on the
  // test thread before any timer fires.
  auto egressPacket = qp_->getNextPacketToSend();
  ASSERT_NE(egressPacket, nullptr);
  ASSERT_NE(egressPacket->packet, nullptr);
  auto* dataPacket = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  ASSERT_NE(dataPacket, nullptr) << "first packet should be a data packet";
  const auto& dataHeader = dataPacket->getDataHeader();
  auto hrnrAck = AckHeader::AckHeaderBuilder()
                     .setTag(kMetaRoceInitialTag)
                     .setCumulativeFsn(dataHeader.getFsn())
                     .setMaxRcvdFsn(dataHeader.getFsn())
                     .setCumulRespDeliveredMsn(dataHeader.getMsn())
                     .setAckStatus(AckStatus::HRNR)
                     .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                     .setTimestamp(dataHeader.getTimestamp())
                     .build();
  flow0->processAckPacket(
      hrnrAck, simulatedProcessedAtNs(hrnrAck.getTimestamp()));
  // Assert the received count, not getHrnrState(): the backoff timer writes
  // hrnrState_ on its own thread, so reading it here would re-introduce a race.
  EXPECT_EQ(qp_->getHrnrPacketsReceived(), 1)
      << "the HRNR ACK should have been processed";

  // Wait for the watchdog to expire and completions to arrive using async
  // pollCq with timeout (avoids sleep-based polling)
  auto cqe1 = folly::coro::blockingWait(
      folly::coro::timeout(cq_.pollCq(), kTestTimeout));
  auto cqe2 = folly::coro::blockingWait(
      folly::coro::timeout(cq_.pollCq(), kTestTimeout));

  // Verify completions
  // We posted 2 WQEs, so we should have 2 completions:
  // - 1 with kWcRnrRetryExcErr (the first WQE - RNR retry exceeded because
  //   we're in HRNR state)
  // - 1 with kWrFlushErr (the second WQE - flushed)
  std::set<WorkCompletionStatus> expectedStatuses = {
      kWcRnrRetryExcErr, kWrFlushErr};
  std::set<WorkCompletionStatus> actualStatuses = {cqe1.status, cqe2.status};
  EXPECT_EQ(actualStatuses, expectedStatuses)
      << "Should have one kWcRnrRetryExcErr and one kWrFlushErr completion. "
      << "Got: " << static_cast<int>(cqe1.status) << " and "
      << static_cast<int>(cqe2.status);

  // Verify work request IDs
  std::set<uint64_t> expectedWorkReqIds = {300, 301};
  std::set<uint64_t> actualWorkReqIds = {cqe1.workReqId, cqe2.workReqId};
  EXPECT_EQ(actualWorkReqIds, expectedWorkReqIds)
      << "Should have completions for both WQEs";

  LOG(INFO) << "Test completed: " << cq_.getNumCompletedWqes()
            << " CQ completions with HRNR state";

  sender = nullptr;
}

TEST_F(WatchdogTest, AReceivePostDoesNotStandInForTransmitProgress) {
  std::array<char, 64> receiveBuffer{};
  const auto beforeReceive = qp_->getLastProgressTime();

  ASSERT_TRUE(qp_->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<ReceiveWqe>(
          /*workReqId=*/400, oneSge(receiveBuffer), qp_->getMtu())))
      << "precondition: the receive has to be accepted, or neither post below "
         "could have recorded progress";

  EXPECT_EQ(qp_->getLastProgressTime(), beforeReceive)
      << "receive queue entries come back as the peer's traffic arrives, so a "
         "queue pair still being sent to could hold its own stalled sender off "
         "expiry indefinitely";

  std::array<char, 64> sendBuffer{};
  ASSERT_TRUE(qp_->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          /*workReqId=*/401, oneSge(sendBuffer), qp_->getMtu())))
      << "precondition: the send has to be accepted for its post to record "
         "progress at all";

  EXPECT_GT(qp_->getLastProgressTime(), beforeReceive)
      << "transmit work is what the watchdog covers, so a send post has to go "
         "on recording progress rather than the receive check above passing "
         "against a progress time that never moves";
}

TEST_F(WatchdogTest, TheFirstRtoIsNeverTheOneThatFailsTheWork) {
  // The 1ms watchdog interval is raised to kMinWatchdogIntervalMs, and the RTO
  // floor is twice that, so the watchdog has expired by the first RTO.
  FLAGS_libsoftmetaroce_min_rto_us = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          kMinWatchdogIntervalMs * 2)
          .count());
  FLAGS_libsoftmetaroce_qp_watchdog_interval_ms = 1;
  // One write per frame, so the count below measures frames.
  FLAGS_libsoftmetaroce_send = "single";
  buildQp();

  auto sender = createTestSenderWithFlows(1);
  auto socket = getTestSocket(sender.get());
  ASSERT_NE(socket, nullptr);
  ON_CALL(*rawCongestionController_, getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));
  startCountingWritesOn(socket);

  auto payload = std::make_unique<char[]>(kTestFrameCount * qp_->getMtu());
  // The queue pair's flows dereference the sender, so the queue pair goes
  // first. Both read payload, so both are destroyed before payload is.
  SCOPE_EXIT {
    qp_.reset();
    sender = nullptr;
  };
  postTestWqe(kTestFrameCount, payload.get(), 100);
  qp_->startProcessingWqes();
  // The message is kTestFrameCount writes, so one past it is a retransmission.
  ASSERT_TRUE(waitForPacketsSentAtLeast(kTestFrameCount + 1))
      << "the first rto has to retransmit rather than fail the work, so a "
         "write past the message itself has to reach the socket; writes seen: "
      << packetsSent();

  auto cqe = folly::coro::blockingWait(
      folly::coro::timeout(cq_.pollCq(), kTestTimeout));
  EXPECT_EQ(cqe.status, kWcRetryExcErr)
      << "a later rto still has to fail the work, or the grace above never "
         "ends and a stalled queue pair runs forever";
}

/*
 * A configured interval, not the flag, is what fails the work. The flag is set
 * past the test's own patience, so a QP still measuring against it would never
 * complete and this would time out rather than pass.
 */
TEST_F(WatchdogTest, WorkFailsAtTheConfiguredIntervalRatherThanTheFlag) {
  FLAGS_libsoftmetaroce_qp_watchdog_interval_ms = 60000;
  buildQp();
  const auto derived = intervalsFromQpAttributes(
      /*localAckTimeoutExponent=*/14,
      /*retryCount=*/0); // 4.096us * 2^14 = 67ms
  qp_->configureWatchdogInterval(derived.watchdogIntervalMs);

  auto sender = createTestSenderWithFlows(1);
  auto socket = getTestSocket(sender.get());
  ASSERT_NE(socket, nullptr);
  ON_CALL(*rawCongestionController_, getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));

  startCountingWritesOn(socket);

  auto payload = std::make_unique<char[]>(kTestFrameCount * qp_->getMtu());
  // The queue pair's flows dereference the sender, so the queue pair goes
  // first. Both read payload, so both are destroyed before payload is.
  SCOPE_EXIT {
    qp_.reset();
    sender = nullptr;
  };
  postTestWqe(kTestFrameCount, payload.get(), 100);
  qp_->startProcessingWqes();
  ASSERT_TRUE(waitForAnyPacketSent())
      << "precondition: a packet has to be in flight for the watchdog to have "
         "work to fail";

  auto cqe = folly::coro::blockingWait(
      folly::coro::timeout(cq_.pollCq(), kTestTimeout));
  EXPECT_EQ(cqe.status, kWcRetryExcErr)
      << "a configured interval must drive the same expiry the flag does, or a "
         "client's interval is recorded and never applied";
}

} // namespace libsoftmetaroce
