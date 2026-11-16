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

#include <folly/coro/GmockHelpers.h>
#include <folly/coro/Task.h>
#include <gtest/gtest.h>

#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>

using namespace testing;
namespace libsoftmetaroce {

static std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

folly::coro::Task<void>
testRoundRobin(int baseQpNum, int numQps, int numPacketsPerQp) {
  auto remoteAddr = folly::IPAddress("fc00::101");
  auto localAddr = folly::IPAddress("fc00::1");
  HighResTimer retxTimer;
  QpArbiter arbiter;
  MemoryRegions mrs;
  CompletionQueue cq;
  std::vector<std::shared_ptr<Qp>> qps;
  struct ibv_qp ibvQpStorage{};
  struct ibv_qp* ibvQp = &ibvQpStorage;

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);

  // Create Qps and add them to the arbiter.
  for (int qpNum = baseQpNum; qpNum < baseQpNum + numQps; ++qpNum) {
    auto qp = std::make_shared<Qp>(qpNum, retxTimer, arbiter, mrs, &cq, ibvQp);
    qps.push_back(qp);
    qp->addPortPair(localAddr, remoteAddr);
    qp->setRemoteQpNum(qpNum);
    auto congestionController =
        std::make_unique<NiceMock<MockCongestionController>>();
    auto rawCongestionController = congestionController.get();
    qp->setCongestionController(std::move(congestionController));
    ON_CALL(*rawCongestionController, getAvailableWindowSize(0 /* flowId */))
        .WillByDefault(Return(1));
    qp->setupFlows(1, 1, &mockSender);
    EXPECT_EQ(qp->getNumPendingWqes(), 0);
    qp->startProcessingWqes();
  }

  // Shared payload buffer; assumes all qp's use the same mtu.
  const uint32_t length = numPacketsPerQp * qps[0]->getMtu();
  std::unique_ptr<char[]> payload(new char[length]);

  // Post a Wqe to each Qp.
  for (auto& qp : qps) {
    CHECK(qp->postWqe(
        kMetaRoceInitialTag,
        std::make_unique<SendWqe>(
            0 /* workReqId */,
            oneSge(
                payload.get(), /*address*/
                length),
            qp->getMtu())));
  }

  // Poll until WQEs have been processed into Tags.
  for (auto& qp : qps) {
    EXPECT_TRUE(qp->waitForWqesToBePosted(1));
  }

  // waitForAndGetNextPacketToSend() below blocks until a QP is ready, so the
  // round-robin claim loop synchronizes on readiness.

  // Verify that the Qps are serviced in round-robin order.
  std::unique_ptr<EgressPacket> egressPacket;
  int expectedQpNum;
  for (int i = 0; i < numQps * numPacketsPerQp; ++i) {
    egressPacket = co_await arbiter.waitForAndGetNextPacketToSend();
    EXPECT_NE(egressPacket, nullptr)
        << "QpArbiter should have found a packet to send "
        << "or waited until one was ready.";
    if (egressPacket) {
      expectedQpNum = baseQpNum + (i % numQps);
      EXPECT_EQ(egressPacket->qpNum, expectedQpNum)
          << "QpArbiter should have sent a packet #" << i << " from Qp "
          << expectedQpNum << ", but sent from Qp " << egressPacket->qpNum
          << " instead.";
      if (egressPacket->qpNum != expectedQpNum) {
        // if we are out of sync, do not expect to get back on track
        // providing expected packets at the expected time.
        break;
      }
    }
  }

  co_await arbiter.shutdown();
  egressPacket = co_await arbiter.waitForAndGetNextPacketToSend();
  EXPECT_EQ(egressPacket, nullptr)
      << "QpArbiter should not return packets after shutdown.";
}

CO_TEST(QpArbiterTest, DoesRoundRobinOnQpsOneQp) {
  co_await testRoundRobin(
      0 /* baseQpNum */, 1 /* numQps */, 1 /* numPacketsPerQp */);
}

CO_TEST(QpArbiterTest, DoesRoundRobinOnQpsFewQpsWithoutOffset) {
  co_await testRoundRobin(
      0 /* baseQpNum */, 2 /* numQps */, 1 /* numPacketsPerQp */);
}

CO_TEST(QpArbiterTest, DoesRoundRobinOnQpsFewQpsWithOffset) {
  co_await testRoundRobin(
      5 /* baseQpNum */, 2 /* numQps */, 1 /* numPacketsPerQp */);
}

CO_TEST(QpArbiterTest, DoesRoundRobinOnQpsManyQpsWithoutOffset) {
  co_await testRoundRobin(
      0 /* baseQpNum */, 10 /* numQps */, 10 /* numPacketsPerQp */);
}

CO_TEST(QpArbiterTest, DoesRoundRobinOnQpsManyQpsWithOffset) {
  co_await testRoundRobin(
      5 /* baseQpNum */, 10 /* numQps */, 10 /* numPacketsPerQp */);
}

CO_TEST(QpArbiterTest, HandleFaultEagainRemovesQpFromReadySet) {
  QpArbiter arbiter;
  int callCount = 0;
  auto remoteAddr = folly::IPAddress("fc00::1");

  arbiter.addQp(0, [&]() -> std::unique_ptr<EgressPacket> {
    ++callCount;
    BaseHeader::Opcode ackOpcode(BaseHeader::Opcode::OperationType::SEL_ACK);
    // The arbiter never sends the packet, so no socket needs to be resolved.
    return std::make_unique<EgressPacket>(
        std::make_unique<AckPacket>(
            BaseHeader::BaseHeaderBuilder()
                .setOpcode(std::move(ackOpcode))
                .setVersion(kMetaRoceProtocolVersion)
                .setFlowId(0)
                .setDstQp(0)
                .setSrcQp(0)
                .build(),
            AckHeader::AckHeaderBuilder()
                .setAckStatus(AckStatus::ACK)
                .setCumulativeFsn(0)
                .setMaxRcvdFsn(0)
                .setTimestamp(0)
                .setCumulRespDeliveredMsn(0)
                .build()),
        /*socketToSendOn=*/nullptr,
        remoteAddr,
        /*qpNum=*/0,
        EgressPacket::RxmitType::None);
  });

  // Signal ready and consume one packet.
  arbiter.signalReady(0);
  auto pkt = co_await arbiter.waitForAndGetNextPacketToSend();
  EXPECT_NE(pkt, nullptr) << "should get a packet after signalReady";
  EXPECT_EQ(callCount, 1);

  // Simulate EAGAIN: the packet's data was already moved to the socket,
  // so clear it before handing back to the fault handler.
  pkt->packet = nullptr;
  co_await arbiter.handleFaultOnEgress(std::move(pkt), EAGAIN);

  // The QP should no longer be in the ready set.
  EXPECT_FALSE(arbiter.isQpnReady(0))
      << "EAGAIN fault should remove the QP from the ready set";

  // Re-signal and verify the QP can send again.
  arbiter.signalReady(0);
  EXPECT_TRUE(arbiter.isQpnReady(0))
      << "QP should be ready again after re-signaling";
  pkt = co_await arbiter.waitForAndGetNextPacketToSend();
  EXPECT_NE(pkt, nullptr) << "should get a packet after re-signaling";
  EXPECT_EQ(callCount, 2);

  co_await arbiter.shutdown();
}

static std::unique_ptr<EgressPacket> makeArbiterTestPacket(uint32_t qpNum) {
  // The arbiter never sends the packet, so no socket needs to be resolved.
  return std::make_unique<EgressPacket>(
      std::make_unique<AckPacket>(
          BaseHeader::BaseHeaderBuilder()
              .setOpcode(
                  BaseHeader::Opcode(
                      BaseHeader::Opcode::OperationType::SEL_ACK))
              .setVersion(kMetaRoceProtocolVersion)
              .setFlowId(0)
              .setDstQp(0)
              .setSrcQp(qpNum)
              .build(),
          AckHeader::AckHeaderBuilder()
              .setAckStatus(AckStatus::ACK)
              .setCumulativeFsn(0)
              .setMaxRcvdFsn(0)
              .setTimestamp(0)
              .setCumulRespDeliveredMsn(0)
              .build()),
      /*socketToSendOn=*/nullptr,
      /*remoteAddr=*/folly::IPAddress(),
      qpNum,
      EgressPacket::RxmitType::None);
}

CO_TEST(QpArbiterTest, PostSignalDeliversViaLockFreeIntake) {
  QpArbiter arbiter;
  constexpr uint32_t kQpNum = 3;
  arbiter.addQp(kQpNum, [&]() { return makeArbiterTestPacket(kQpNum); });

  arbiter.postSignal(kQpNum);

  auto pkt = co_await arbiter.waitForAndGetNextPacketToSend();
  EXPECT_NE(pkt, nullptr)
      << "postSignal must enqueue the QP on the lock-free intake and make it "
         "selectable once the arbiter drains the intake on its next loop";
  EXPECT_EQ(pkt->qpNum, kQpNum)
      << "the lock-free-signaled QP must be the one serviced";

  co_await arbiter.shutdown();
}

CO_TEST(QpArbiterTest, StaleSignalForRemovedQpIsDroppedAndCounted) {
  QpArbiter arbiter;
  constexpr uint32_t kStaleQpNum = 1; // smaller qpNum is selected first
  constexpr uint32_t kLiveQpNum = 2;
  arbiter.addQp(
      kStaleQpNum, [&]() { return makeArbiterTestPacket(kStaleQpNum); });
  arbiter.addQp(
      kLiveQpNum, [&]() { return makeArbiterTestPacket(kLiveQpNum); });

  arbiter.postSignal(kStaleQpNum);
  arbiter.postSignal(kLiveQpNum);
  // removeQp does not purge the lock-free intake, so kStaleQpNum lingers there
  // with no callback -- the case the selection loop must tolerate.
  arbiter.removeQp(kStaleQpNum);

  auto pkt = co_await arbiter.waitForAndGetNextPacketToSend();
  EXPECT_NE(pkt, nullptr)
      << "the stale (removed) QP in the intake must not stall selection -- a "
         "packet must still be returned for the live QP";
  EXPECT_EQ(pkt->qpNum, kLiveQpNum)
      << "a signaled-then-removed QP must be skipped at selection and the live "
         "QP serviced instead";
  EXPECT_EQ(arbiter.getStaleSignalDropCount(), 1u)
      << "dropping the stale (removed) QP must be counted for observability";

  co_await arbiter.shutdown();
}

// Overrides nextPacketOrIdle alone, so buildBatchOrIdle is the interface's.
class PacketsLeftArbiter : public QpArbiter {
 public:
  explicit PacketsLeftArbiter(int packetsLeft) : packetsLeft_{packetsLeft} {}

  std::unique_ptr<EgressPacket> nextPacketOrIdle(uint32_t qpNum) override {
    return packetsLeft_-- > 0 ? makeArbiterTestPacket(qpNum) : nullptr;
  }

 private:
  int packetsLeft_;
};

TEST(QpArbiterTest, DefaultBuildBatchTakesPacketsFromNextPacketOrIdle) {
  constexpr uint32_t kQpNum = 1;
  PacketsLeftArbiter arbiter(/*packetsLeft=*/5);

  EXPECT_EQ(
      arbiter
          .buildBatchOrIdle(
              kQpNum, makeArbiterTestPacket(kQpNum), /*batchCap=*/3)
          .size(),
      3u)
      << "an arbiter that implements only nextPacketOrIdle still fills a "
         "batch, or sendmmsg would carry one packet per call";
  EXPECT_EQ(
      arbiter
          .buildBatchOrIdle(
              kQpNum, makeArbiterTestPacket(kQpNum), /*batchCap=*/8)
          .size(),
      4u)
      << "the batch ends at the first null, which has settled the QP to idle";
}

TEST(QpArbiterTest, PostSignalDropsAndCountsWhenIntakeFull) {
  QpArbiter arbiter;
  // Overfill the lock-free intake (capacity 8192) without draining it.
  for (int i = 0; i < 9000; ++i) {
    arbiter.postSignal(1);
  }
  EXPECT_GT(arbiter.getIntakeFullCount(), 0u)
      << "postSignal must drop and count signals once the intake ring is full, "
         "rather than block the producer";
}

} // namespace libsoftmetaroce
