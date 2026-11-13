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
#include <gtest/gtest.h>
#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Cqe.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/test/AckProcessedAt.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <iterator>
#include <set>
#include <thread>
#include <vector>

using namespace testing;
namespace libsoftmetaroce {

constexpr uint16_t kMetaRoceSendToPort = 0xace;
constexpr int kTestFrameCount = 8;
constexpr std::chrono::seconds kTestTimeout{30};
constexpr int kCongestionWindowSize = 10;

static std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

struct PacketInfo {
  uint16_t flowId;
  uint16_t posn;
  uint16_t fsn;
  uint16_t msn;
  uint32_t timestamp;
};

class NakTest : public Test {
 protected:
  void SetUp() override {
    qp = std::make_unique<Qp>(10, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
    qp->addPortPair(localAddr, remoteAddr);
    qp->setRemoteQpNum(100);
    qp->setMtu(1000);
    auto controller = std::make_unique<NiceMock<MockCongestionController>>();
    rawCongestionController = controller.get();
    qp->setCongestionController(std::move(controller));
  }
  void TearDown() override {
    qp = nullptr;
  }

  // Helper function to create a test sender with flows already set up
  std::unique_ptr<LimitedTestSender> createTestSenderWithFlows(
      uint32_t numFlows,
      bool disableSendLoop = LimitedTestSender::kEnableSendLoop) {
    auto sender = std::make_unique<LimitedTestSender>(
        arbiter, mockUDPSocketMaker, kMetaRoceSendToPort, disableSendLoop);
    qp->setupFlows(numFlows, numFlows, sender.get());
    return sender;
  }

  // Helper function to get the mock socket from a sender
  MockUDPSocket& getTestSocket(LimitedTestSender* sender) {
    auto& testSender = *CHECK_NOTNULL(sender);
    auto socketId = testSender.getSocketIdFromAddress(localAddr);
    return *CHECK_NOTNULL(
        dynamic_cast<MockUDPSocket*>(testSender.getSocketById(socketId)));
  }

  TxFlow& txFlow(uint16_t flowId) {
    return *CHECK_NOTNULL(qp->getTxFlowById(flowId));
  }

  MockCongestionController& congestionController() {
    return *CHECK_NOTNULL(rawCongestionController);
  }

  std::optional<PacketInfo> parseDataPacket(
      const std::unique_ptr<folly::IOBuf>& buf) {
    folly::io::Cursor cursor(buf.get());

    BaseHeader baseHeader(cursor);

    if (!baseHeader.getOpcode().isData()) {
      return std::nullopt;
    }

    DataHeader dataHeader(cursor);

    return PacketInfo{
        baseHeader.getFlowId(),
        dataHeader.getPosn(),
        dataHeader.getFsn(),
        dataHeader.getMsn(),
        dataHeader.getTimestamp()};
  }

  AckHeader buildNakFlowIdError(uint16_t fsn, uint32_t timestamp) {
    return AckHeader::AckHeaderBuilder()
        .setTag(kMetaRoceInitialTag)
        .setCumulativeFsn(fsn)
        .setMaxRcvdFsn(fsn)
        .setAckStatus(AckStatus::NAK_FLOW_ID_ERROR)
        .setTimestamp(timestamp)
        .build();
  }

  AckHeader buildAck(uint16_t fsn, uint32_t timestamp) {
    return AckHeader::AckHeaderBuilder()
        .setTag(kMetaRoceInitialTag)
        .setCumulativeFsn(fsn)
        .setMaxRcvdFsn(fsn)
        .setAckStatus(AckStatus::ACK)
        .setTimestamp(timestamp)
        .build();
  }

  std::set<uint16_t> computePosnUnion(
      const std::set<uint16_t>& set1,
      const std::set<uint16_t>& set2) {
    std::set<uint16_t> result;
    result.insert(set1.begin(), set1.end());
    result.insert(set2.begin(), set2.end());
    return result;
  }

  void expectAllPosnsPresent(
      const std::set<uint16_t>& actual,
      int frameCount,
      const std::string& context) {
    EXPECT_EQ(actual.size(), frameCount)
        << "All " << frameCount << " POSNs should appear " << context;

    std::set<uint16_t> expected;
    for (int i = 0; i < frameCount; ++i) {
      expected.insert(i);
    }
    EXPECT_EQ(actual, expected)
        << "POSNs " << context << " should be exactly {0.." << frameCount - 1
        << "}";
  }

  void postTestWqe(int frames, char* payload) {
    const uint32_t length = frames * qp->getMtu();
    ASSERT_TRUE(qp->postWqe(
        kMetaRoceInitialTag,
        std::make_unique<SendWqe>(
            0 /* workReqId */, oneSge(payload, length), qp->getMtu())));
  }

  HighResTimer retxTimer;
  QpArbiter arbiter;
  MemoryRegions mrs;
  folly::IPAddress localAddr{"fc00::1"};
  folly::IPAddress remoteAddr{"fc00::101"};
  std::unique_ptr<Qp> qp;
  CompletionQueue cq;
  MockCongestionController* rawCongestionController{nullptr};
  struct ibv_qp ibvQpStorage{};
};

/*
 * Test that when a sender transmits on two flows and flowId 1 receives
 * NAK_FLOW_ID_ERROR, all frames are eventually retransmitted on flowId 0.
 *
 * This test:
 * 1. Creates a QP with 2 flows
 * 2. Posts a WQE with 8 frames
 * 3. When packets are sent on flowId 1, responds with NAK_FLOW_ID_ERROR
 * 4. When packets are sent on flowId 0, responds with normal ACKs
 * 5. Verifies that all 8 POSNs eventually appear on flowId 0
 */
TEST_F(NakTest, NakFlowIdErrorCausesRetransmissionOnOtherFlow) {
  auto sender = createTestSenderWithFlows(2);
  auto& socket = getTestSocket(sender.get());

  ON_CALL(congestionController(), getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));

  std::mutex mtx;
  std::condition_variable cv;
  std::set<uint16_t> posnOnFlow0;
  std::set<uint16_t> posnOnFlow1;

  auto& flow0 = txFlow(0);
  auto& flow1 = txFlow(1);

  EXPECT_CALL(socket, write(_, _))
      .Times(AtLeast(kTestFrameCount))
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        auto packetInfo = parseDataPacket(buf);
        if (!packetInfo) {
          return buf.get()->computeChainDataLength();
        }

        LOG(INFO) << "Packet sent: flowId=" << packetInfo->flowId
                  << " posn=" << packetInfo->posn << " fsn=" << packetInfo->fsn;

        std::lock_guard<std::mutex> lock(mtx);

        if (packetInfo->flowId == 1) {
          LOG(INFO) << "Sending NAK_FLOW_ID_ERROR for packet on flowId 1";
          posnOnFlow1.insert(packetInfo->posn);
          flow1.processAckPacket(
              buildNakFlowIdError(packetInfo->fsn, packetInfo->timestamp),
              simulatedProcessedAtNs(packetInfo->timestamp));
        } else if (packetInfo->flowId == 0) {
          posnOnFlow0.insert(packetInfo->posn);
          flow0.processAckPacket(
              buildAck(packetInfo->fsn, packetInfo->timestamp),
              simulatedProcessedAtNs(packetInfo->timestamp));

          if (posnOnFlow0.size() == kTestFrameCount) {
            cv.notify_one();
          }
        }

        return buf.get()->computeChainDataLength();
      });

  auto payload = std::make_unique<char[]>(kTestFrameCount * qp->getMtu());
  postTestWqe(kTestFrameCount, payload.get());
  qp->startProcessingWqes();

  {
    std::unique_lock<std::mutex> lock(mtx);
    bool success = cv.wait_for(lock, kTestTimeout, [&posnOnFlow0]() {
      return posnOnFlow0.size() == kTestFrameCount;
    });

    EXPECT_TRUE(success) << "Timed out waiting for all frames on flow 0";
  }

  expectAllPosnsPresent(posnOnFlow0, kTestFrameCount, "on flow 0");
  EXPECT_GT(posnOnFlow1.size(), 0)
      << "At least one packet should have been sent on flow 1";

  LOG(INFO) << "Test completed: " << posnOnFlow0.size() << " POSNs on flow 0, "
            << posnOnFlow1.size() << " POSNs on flow 1";

  sender = nullptr;
}

/*
 * Every frame NAK'd off a bad flow must reappear on the one live flow.
 *
 * This mirrors a sender configured with more transmit flows than the receiver
 * has receive flows: flows 1-3 draw NAK_FLOW_ID_ERROR for every frame while
 * flow 0 acks normally. The property under test is conservation -- each POSN
 * rejected on a bad flow is re-offered and lands on flow 0 -- which is what
 * fails when released frames are never re-sent and the transfer stalls with
 * the message incomplete.
 *
 * Two things here differ from the tests above, and both are the point. The
 * NAKs are delivered through the received-event ring and applied by
 * applyHandedOffAcks(), the way the sender really sees them, rather than by
 * calling processAckPacket() directly. And this thread drives the sender
 * rather than starting its send loop, so a stall shows up as a missing POSN
 * rather than as a timeout.
 */
TEST_F(NakTest, EveryFrameNakdOffABadFlowReappearsOnTheLiveFlow) {
  constexpr int kFrameCount = 16;
  constexpr uint16_t kLiveFlowId = 0;
  // A stalled QP offers nothing, so the loop cannot end on a null packet; it
  // ends on the frame count or on this cap, and the assertions judge which.
  constexpr int kMaxIterations = 1000;

  auto sender =
      createTestSenderWithFlows(4, LimitedTestSender::kDisableSendLoop);

  ON_CALL(congestionController(), getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));

  ASSERT_TRUE(qp->usesSingleOwnerRxRing())
      << "this test hands NAKs to the sender over the ring, which is where a "
         "lost wakeup would hide; without the ring there is nothing to test";

  auto payload = std::make_unique<char[]>(kFrameCount * qp->getMtu());
  postTestWqe(kFrameCount, payload.get());
  qp->startProcessingWqes();
  ASSERT_TRUE(qp->waitForWqesToBePosted(1))
      << "Timed out waiting for the WQE to reach its Tag";

  std::set<uint16_t> posnNakdOffBadFlows;
  std::set<uint16_t> posnOnLiveFlow;

  int iterations = 0;
  bool applyHandedOff = false;
  while (static_cast<int>(posnOnLiveFlow.size()) < kFrameCount &&
         iterations++ < kMaxIterations) {
    if (applyHandedOff) {
      // The order an arbiter visit uses: apply what the receiver handed back,
      // then ask for the next packet.
      qp->applyHandedOffAcks();
    }

    auto egressPacket = qp->getNextPacketToSend();
    if ((egressPacket == nullptr || egressPacket->packet == nullptr) &&
        !applyHandedOff) {
      // Phase 1 is done: the sender has offered everything it will offer
      // before hearing anything back. Switch to phase 2 and drain the NAKs.
      applyHandedOff = true;
      continue;
    }
    if (egressPacket == nullptr || egressPacket->packet == nullptr) {
      continue;
    }
    const auto& baseHeader = egressPacket->packet->getBaseHeader();
    if (!baseHeader.getOpcode().isData()) {
      continue;
    }
    auto* dataPacket = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    if (dataPacket == nullptr) {
      continue;
    }
    const auto& dataHeader = dataPacket->getDataHeader();
    const uint16_t flowId = baseHeader.getFlowId();
    const uint16_t posn = dataHeader.getPosn();
    const uint16_t fsn = dataHeader.getFsn();
    const uint32_t timestamp = dataHeader.getTimestamp();

    if (flowId == kLiveFlowId) {
      posnOnLiveFlow.insert(posn);
      qp->enqueueReceivedAck(
          flowId, buildAck(fsn, timestamp), simulatedProcessedAtNs(timestamp));
    } else {
      posnNakdOffBadFlows.insert(posn);
      qp->enqueueReceivedAck(
          flowId,
          buildNakFlowIdError(fsn, timestamp),
          simulatedProcessedAtNs(timestamp));
    }
  }

  ASSERT_GT(posnNakdOffBadFlows.size(), 0u)
      << "the sender must have used the bad flows at all, or the conservation "
         "check below proves nothing";

  std::set<uint16_t> missing;
  std::set_difference(
      posnNakdOffBadFlows.begin(),
      posnNakdOffBadFlows.end(),
      posnOnLiveFlow.begin(),
      posnOnLiveFlow.end(),
      std::inserter(missing, missing.begin()));
  EXPECT_TRUE(missing.empty())
      << missing.size() << " of " << posnNakdOffBadFlows.size()
      << " POSNs rejected on a bad flow were never re-sent on flow 0; a "
         "released frame that is never re-offered stalls the message";

  expectAllPosnsPresent(posnOnLiveFlow, kFrameCount, "on the live flow");

  sender = nullptr;
}

/*
 * Test that when NAK_FLOW_ID_ERROR responses are queued and processed after
 * all frames have been initially transmitted, they still trigger retransmission
 * on the alternate flow.
 *
 * This test:
 * 1. Creates a QP with 2 flows
 * 2. Posts a WQE with 8 frames
 * 3. Queues NAK_FLOW_ID_ERROR headers for packets sent on flowId 1
 * 4. Waits for all 8 frames to be initially transmitted
 * 5. Processes all queued NAK headers
 * 6. Responds with normal ACKs for packets on flowId 0
 * 7. Verifies that all 8 POSNs eventually appear on flowId 0
 */
TEST_F(NakTest, NakFlowIdErrorQueuedAfterTransmission) {
  auto sender = createTestSenderWithFlows(2);
  auto& socket = getTestSocket(sender.get());

  ON_CALL(congestionController(), getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));

  std::mutex mtx;
  std::condition_variable cv;
  std::set<uint16_t> posnOnFlow0;
  std::set<uint16_t> posnOnFlow1;
  std::set<uint16_t> posnWithQueuedNak;
  std::vector<AckHeader> queuedNakHeaders;
  bool allFramesTransmitted = false;

  auto& flow0 = txFlow(0);
  auto& flow1 = txFlow(1);

  EXPECT_CALL(socket, write(_, _))
      .Times(AtLeast(kTestFrameCount))
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        auto packetInfo = parseDataPacket(buf);
        if (!packetInfo) {
          return buf.get()->computeChainDataLength();
        }

        LOG(INFO) << "Packet sent: flowId=" << packetInfo->flowId
                  << " posn=" << packetInfo->posn << " fsn=" << packetInfo->fsn;

        std::unique_lock<std::mutex> lock(mtx);

        if (packetInfo->flowId == 1 && !allFramesTransmitted) {
          posnOnFlow1.insert(packetInfo->posn);

          if (posnWithQueuedNak.find(packetInfo->posn) ==
              posnWithQueuedNak.end()) {
            LOG(INFO) << "Queueing NAK_FLOW_ID_ERROR for packet on flowId 1";
            posnWithQueuedNak.insert(packetInfo->posn);
            queuedNakHeaders.push_back(
                buildNakFlowIdError(packetInfo->fsn, packetInfo->timestamp));
          }
        } else if (packetInfo->flowId == 0) {
          posnOnFlow0.insert(packetInfo->posn);
        }

        auto allPosns = computePosnUnion(posnOnFlow0, posnOnFlow1);
        if (allPosns.size() == kTestFrameCount && !allFramesTransmitted) {
          allFramesTransmitted = true;
          // Apply the queued NAKs on the consumer thread, not the test thread;
          // this retransmits the flow-1 frames onto flow 0. Drop mtx first so
          // processAckPacket can never re-enter this write mock under the
          // (non-recursive) lock.
          lock.unlock();
          for (const auto& nakHeader : queuedNakHeaders) {
            flow1.processAckPacket(
                nakHeader, simulatedProcessedAtNs(nakHeader.getTimestamp()));
          }
          lock.lock();
          cv.notify_one();
        }

        if (packetInfo->flowId == 0) {
          const auto ack = buildAck(packetInfo->fsn, packetInfo->timestamp);
          lock.unlock();
          flow0.processAckPacket(
              ack, simulatedProcessedAtNs(packetInfo->timestamp));
          lock.lock();
          if (posnOnFlow0.size() == kTestFrameCount) {
            cv.notify_one();
          }
        }

        return buf.get()->computeChainDataLength();
      });

  auto payload = std::make_unique<char[]>(kTestFrameCount * qp->getMtu());
  postTestWqe(kTestFrameCount, payload.get());
  qp->startProcessingWqes();

  {
    std::unique_lock<std::mutex> lock(mtx);
    bool success = cv.wait_for(lock, kTestTimeout, [&allFramesTransmitted]() {
      return allFramesTransmitted;
    });

    EXPECT_TRUE(success)
        << "Timed out waiting for initial transmission of all frames";

    auto allPosns = computePosnUnion(posnOnFlow0, posnOnFlow1);
    EXPECT_EQ(allPosns.size(), kTestFrameCount)
        << "All 8 POSNs should have been initially transmitted";
  }

  {
    std::unique_lock<std::mutex> lock(mtx);
    bool success = cv.wait_for(lock, kTestTimeout, [&posnOnFlow0]() {
      return posnOnFlow0.size() == kTestFrameCount;
    });

    EXPECT_TRUE(success)
        << "Timed out waiting for flow-0 retransmissions triggered by the "
           "queued NAKs (applied in the write mock)";
  }

  expectAllPosnsPresent(posnOnFlow0, kTestFrameCount, "on flow 0");
  EXPECT_EQ(queuedNakHeaders.size(), posnOnFlow1.size())
      << "Should have queued NAK headers for all POSNs sent on flow 1";
  EXPECT_GT(posnOnFlow1.size(), 0)
      << "At least one POSN should have been sent on flow 1";

  LOG(INFO) << "Test completed: " << posnOnFlow0.size()
            << " POSNs on flow 0 (final), " << posnOnFlow1.size()
            << " POSNs on flow 1 (initial), " << queuedNakHeaders.size()
            << " NAK headers processed";

  sender = nullptr;
}

/*
 * A fatal NAK fails the tag's work requests rather than flushing them
 * where they stand. Each still reports, the first with the NAK's status and
 * the rest flushed, but only once the packets already on the wire are
 * accounted for -- so no completion is reported for a message the wire may
 * still be reading.
 *
 * This test:
 * 1. Creates a QP with 2 flows
 * 2. Posts 2 WQEs (each with multiple frames)
 * 3. Waits for packets to be transmitted on both flows
 * 4. Sends NAK_ADDRESS_INVALID_ACCESS_ERROR blaming the second work request
 * 5. Verifies both WQEs complete, 1 error + 1 flush, once they settle
 * 6. Verifies that no more packets are sent after the fatal NAK
 */
TEST_F(NakTest, NakFatalErrorCompletesTheTagsWqesOnceTheirPacketsSettle) {
  constexpr int kNumFlows = 2;
  constexpr int kFramesPerWqe = 4;

  auto sender = createTestSenderWithFlows(kNumFlows);
  auto& socket = getTestSocket(sender.get());

  ON_CALL(congestionController(), getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));
  // Erroring a tag does not itself disable the QP's flows -- but this QP has
  // only the one, so once it fails no tag is left to carry work and each flow
  // goes out of service exactly once.
  EXPECT_CALL(congestionController(), onFlowDisabled(_)).Times(kNumFlows);

  std::mutex mtx;
  std::condition_variable cv;
  std::set<uint16_t> posnOnFlow0;
  std::set<uint16_t> posnOnFlow1;
  bool fatalNakSent = false;
  int packetsAfterFatalNak = 0;
  std::optional<AckHeader> fatalNakHeader;
  std::optional<uint16_t> nakMsn;
  // Assigned before the work requests are posted, below.
  std::optional<uint16_t> firstWqeMsn;

  auto& flow0 = txFlow(0);

  EXPECT_CALL(socket, write(_, _))
      .Times(AtLeast(kNumFlows)) // At least one packet per flow
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        auto packetInfo = parseDataPacket(buf);
        if (!packetInfo) {
          return buf.get()->computeChainDataLength();
        }

        std::unique_lock<std::mutex> lock(mtx);

        if (fatalNakSent) {
          // Count packets sent after fatal NAK - should be 0
          packetsAfterFatalNak++;
          return buf.get()->computeChainDataLength();
        }

        LOG(INFO) << "Packet sent: flowId=" << packetInfo->flowId
                  << " posn=" << packetInfo->posn << " fsn=" << packetInfo->fsn;

        if (packetInfo->flowId == 0) {
          posnOnFlow0.insert(packetInfo->posn);
        } else if (packetInfo->flowId == 1) {
          posnOnFlow1.insert(packetInfo->posn);
        }

        // Apply the fatal NAK on the consumer thread, serialized with packet
        // generation, rather than racing the test thread. Drop mtx first so
        // processAckPacket can never re-enter this write mock under the
        // (non-recursive) lock.
        // An fsn is per-flow, so the cfsn must come from the flow the NAK is
        // delivered on.
        const bool packetOnTheNakFlow = packetInfo->flowId == 0;
        const bool flowOneHasSent = !posnOnFlow1.empty();
        const bool msnIsNotTheFirstWqe =
            firstWqeMsn.has_value() && packetInfo->msn != *firstWqeMsn;
        if (packetOnTheNakFlow && flowOneHasSent && msnIsNotTheFirstWqe) {
          LOG(INFO) << "Sending NAK_ADDRESS_INVALID_ACCESS_ERROR for msn "
                    << packetInfo->msn;
          nakMsn = packetInfo->msn;
          fatalNakHeader =
              AckHeader::AckHeaderBuilder()
                  .setTag(kMetaRoceInitialTag)
                  .setCumulativeFsn(packetInfo->fsn)
                  .setMaxRcvdFsn(packetInfo->fsn)
                  .setCumulRespDeliveredMsn(packetInfo->msn)
                  .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
                  .setTimestamp(packetInfo->timestamp)
                  .build();
          lock.unlock();
          flow0.processAckPacket(
              *fatalNakHeader,
              simulatedProcessedAtNs(fatalNakHeader->getTimestamp()));
          lock.lock();
          fatalNakSent = true;
          cv.notify_one();
        }

        return buf.get()->computeChainDataLength();
      });

  // Msns are assigned in posting order, so the first work request gets the msn
  // the tag is about to hand out. Written under the same lock the write mock
  // reads it beneath.
  {
    std::lock_guard<std::mutex> lock(mtx);
    firstWqeMsn = qp->getTag(kMetaRoceInitialTag)->getNextMsn();
  }
  auto payload1 = std::make_unique<char[]>(kFramesPerWqe * qp->getMtu());
  auto payload2 = std::make_unique<char[]>(kFramesPerWqe * qp->getMtu());

  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          100 /* workReqId */,
          oneSge(payload1.get(), kFramesPerWqe * qp->getMtu()),
          qp->getMtu())));
  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          101 /* workReqId */,
          oneSge(payload2.get(), kFramesPerWqe * qp->getMtu()),
          qp->getMtu())));

  qp->startProcessingWqes();

  // Wait for packets on both flows
  {
    std::unique_lock<std::mutex> lock(mtx);
    bool success = cv.wait_for(
        lock, kTestTimeout, [&fatalNakSent]() { return fatalNakSent; });
    ASSERT_TRUE(success) << "Timed out waiting for packets on both flows";
  }

  // Copied under mtx: the write mock is still inserting.
  size_t posnCountFlow0 = 0;
  size_t posnCountFlow1 = 0;
  {
    std::lock_guard<std::mutex> lock(mtx);
    posnCountFlow0 = posnOnFlow0.size();
    posnCountFlow1 = posnOnFlow1.size();
  }
  EXPECT_GT(posnCountFlow0, 0) << "Should have packets on flow 0";
  EXPECT_GT(posnCountFlow1, 0) << "Should have packets on flow 1";

  ASSERT_TRUE(fatalNakHeader.has_value())
      << "the write mock should have built and applied the fatal NAK on the "
         "consumer thread";

  // Both WQEs had packets in flight when the NAK arrived, so pollCq() waits
  // for those to be accounted for rather than assuming they already are.
  const auto cqe1 = folly::coro::blockingWait(cq.pollCq());
  const auto cqe2 = folly::coro::blockingWait(cq.pollCq());

  // The write mock still runs on the sender thread and writes these under
  // mtx, so read them once under the same lock rather than assert on them in
  // place.
  std::optional<uint16_t> observedNakMsn;
  int observedPacketsAfterFatalNak = 0;
  {
    std::lock_guard<std::mutex> lock(mtx);
    observedNakMsn = nakMsn;
    observedPacketsAfterFatalNak = packetsAfterFatalNak;
  }

  ASSERT_TRUE(observedNakMsn.has_value())
      << "the write mock must have set an msn to blame, or the completions "
         "below have nothing to pair against";
  ASSERT_NE(observedNakMsn.value(), firstWqeMsn.value())
      << "the nak must point to the second work request, or blaming the "
         "oldest would be indistinguishable from blaming the right one";
  // The NAK blamed the second work request, so the first is the one that
  // flushes behind it.
  const uint64_t blamedWrId = 101;
  const uint64_t flushedWrId = 100;
  const std::map<uint64_t, WorkCompletionStatus> statusByWorkReqId{
      {cqe1.workReqId, cqe1.status}, {cqe2.workReqId, cqe2.status}};

  ASSERT_EQ(statusByWorkReqId.size(), 2)
      << "both work requests must report exactly once";
  ASSERT_TRUE(
      statusByWorkReqId.contains(blamedWrId) &&
      statusByWorkReqId.contains(flushedWrId))
      << "the completions must carry the two work request ids posted, or the "
         "lookups below throw instead of reporting what went wrong";
  EXPECT_EQ(statusByWorkReqId.at(blamedWrId), kWrRemAccErr)
      << "the work request whose packet the peer rejected must report the "
         "access error, not its neighbor";
  EXPECT_EQ(statusByWorkReqId.at(flushedWrId), kWrFlushErr)
      << "the work request that was not at fault must report a flush";

  EXPECT_EQ(observedPacketsAfterFatalNak, 0)
      << "a tag in error must send no packet further, neither "
         "new packets nor retransmissions of the frames it abandoned";

  LOG(INFO) << "Test completed: " << posnCountFlow0 << " POSNs on flow 0, "
            << posnCountFlow1 << " POSNs on flow 1, "
            << observedPacketsAfterFatalNak << " packets after fatal NAK, "
            << cq.getNumCompletedWqes() << " CQ completions";

  sender = nullptr;
}

// Delivered from the test thread, unsynchronized with Sender::sendLoop, so the
// sender can still be handing out a WQE of the tag the NAK fails. That work
// must stay alive until its frames settle: freeing it shows up as a
// heap-use-after-free under ASAN when run repeatedly. The NAK carries no msn,
// so the earliest work request that can report takes the status.
TEST_F(NakTest, FatalNakConcurrentWithSendLoopLeavesSentWorkAlive) {
  auto sender = createTestSenderWithFlows(1); // single TX flow
  auto& socket = getTestSocket(sender.get());

  ON_CALL(congestionController(), getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));

  std::mutex mtx;
  std::atomic<bool> sawPacket{false};
  uint16_t lastFsn = 0;
  uint32_t lastTs = 0;

  EXPECT_CALL(socket, write(_, _))
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        if (auto packetInfo = parseDataPacket(buf)) {
          std::lock_guard<std::mutex> g(mtx);
          lastFsn = packetInfo->fsn;
          lastTs = packetInfo->timestamp;
          sawPacket.store(true, std::memory_order_release);
        }
        return buf.get()->computeChainDataLength();
      });

  // Keep the send loop busy with many in-flight Writes so the NAK arrives while
  // a WQE is still being sent.
  constexpr int kNumWqes = 32;
  constexpr int kFramesPerWqe = 8;
  std::vector<std::unique_ptr<char[]>> payloads;
  for (int i = 0; i < kNumWqes; ++i) {
    payloads.push_back(std::make_unique<char[]>(kFramesPerWqe * qp->getMtu()));
    ASSERT_TRUE(qp->postWqe(
        kMetaRoceInitialTag,
        std::make_unique<SendWqe>(
            1000 + i,
            oneSge(payloads.back().get(), kFramesPerWqe * qp->getMtu()),
            qp->getMtu())));
  }

  // Destroying the sender stops and joins the send loop, draining any WQE the
  // sender captured while the fatal NAK was failing its tag.
  SCOPE_EXIT {
    sender = nullptr;
  };
  auto& flow0 = txFlow(0);
  qp->startProcessingWqes();

  // Wait until the send loop is actively transmitting, then deliver the fatal
  // NAK from this thread, racing the send loop.
  const auto deadline = std::chrono::steady_clock::now() + kTestTimeout;
  while (!sawPacket.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(sawPacket.load(std::memory_order_acquire))
      << "precondition: the send loop transmits within " << kTestTimeout.count()
      << "s, so the fatal NAK below races a send in progress";
  auto nak = [&] {
    std::lock_guard<std::mutex> g(mtx);
    return AckHeader::AckHeaderBuilder()
        .setTag(kMetaRoceInitialTag)
        .setCumulativeFsn(lastFsn)
        .setMaxRcvdFsn(lastFsn)
        .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
        .setTimestamp(lastTs)
        .build();
  }();
  flow0.processAckPacket(nak, simulatedProcessedAtNs(nak.getTimestamp()));
}

/*
 * Test that WQEs posted AFTER a fatal NAK (when QP is in error state)
 * are flushed with kWrFlushErr, the send behind the one already in flight.
 *
 * This test:
 * 1. Creates a QP with 1 flow
 * 2. Posts a WQE with multiple frames
 * 3. Waits for at least one packet to be transmitted
 * 4. Sends NAK_ADDRESS_INVALID_ACCESS_ERROR to transition QP to error state
 * 5. Waits for the error to be processed
 * 6. Posts a NEW send WQE after error state is entered
 * 7. Posts a NEW receive WQE after error state is entered
 * 8. Verifies both new WQEs are flushed, the send behind the failing one
 */
TEST_F(NakTest, WqePostedAfterFatalNakIsFlushedBehindTheFailingWqe) {
  constexpr int kFramesPerWqe = 4;
  constexpr uint64_t kFirstWrId = 100;
  constexpr uint64_t kSecondWrId = 200;
  constexpr uint64_t kReceiveWrId = 300;

  auto sender = createTestSenderWithFlows(1);
  auto& socket = getTestSocket(sender.get());

  ON_CALL(congestionController(), getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));

  std::mutex mtx;
  std::condition_variable cv;
  bool packetSent = false;
  std::optional<AckHeader> fatalNakHeader;

  auto& flow0 = txFlow(0);

  EXPECT_CALL(socket, write(_, _))
      .Times(AtLeast(1))
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        auto packetInfo = parseDataPacket(buf);
        if (!packetInfo) {
          return buf.get()->computeChainDataLength();
        }

        std::unique_lock<std::mutex> lock(mtx);

        if (!packetSent) {
          LOG(INFO) << "First packet sent: flowId=" << packetInfo->flowId
                    << " posn=" << packetInfo->posn
                    << " fsn=" << packetInfo->fsn;

          // Apply the fatal NAK on the consumer thread, serialized with packet
          // generation, to enter the error state without racing the test
          // thread. Drop mtx first so processAckPacket can never re-enter this
          // write mock under the (non-recursive) lock.
          fatalNakHeader =
              AckHeader::AckHeaderBuilder()
                  .setTag(kMetaRoceInitialTag)
                  .setCumulativeFsn(packetInfo->fsn)
                  .setMaxRcvdFsn(packetInfo->fsn)
                  .setCumulRespDeliveredMsn(packetInfo->msn)
                  .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
                  .setTimestamp(packetInfo->timestamp)
                  .build();
          lock.unlock();
          flow0.processAckPacket(
              *fatalNakHeader,
              simulatedProcessedAtNs(fatalNakHeader->getTimestamp()));
          lock.lock();
          packetSent = true;
          cv.notify_one();
        }

        return buf.get()->computeChainDataLength();
      });

  // Post initial WQE
  auto payload1 = std::make_unique<char[]>(kFramesPerWqe * qp->getMtu());
  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          kFirstWrId,
          oneSge(payload1.get(), kFramesPerWqe * qp->getMtu()),
          qp->getMtu())));

  qp->startProcessingWqes();

  // Wait for at least one packet to be sent
  {
    std::unique_lock<std::mutex> lock(mtx);
    bool success =
        cv.wait_for(lock, kTestTimeout, [&packetSent]() { return packetSent; });
    ASSERT_TRUE(success) << "Timed out waiting for first packet";
  }

  ASSERT_TRUE(fatalNakHeader.has_value())
      << "the write mock should have built and applied the fatal NAK on the "
         "consumer thread, entering the error state";

  // Verify the QP is now in error state
  EXPECT_TRUE(qp->getTag(kMetaRoceInitialTag)->isInErrorState())
      << "QP should be in error state after fatal NAK";

  // The first WQE still has packets in flight, so it has not reported yet.
  // Work posted from here on queues behind it.

  auto payload2 = std::make_unique<char[]>(kFramesPerWqe * qp->getMtu());
  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          kSecondWrId,
          oneSge(payload2.get(), kFramesPerWqe * qp->getMtu()),
          qp->getMtu())));

  // A receive as well, on its own work queue rather than behind the sends.
  auto recvPayload = std::make_unique<char[]>(kFramesPerWqe * qp->getMtu());
  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<ReceiveWqe>(
          kReceiveWrId,
          oneSge(recvPayload.get(), kFramesPerWqe * qp->getMtu()),
          qp->getMtu())));

  // All three report; pollCq() blocks until the first WQE has settled.
  const std::vector<Cqe> completions = {
      folly::coro::blockingWait(cq.pollCq()),
      folly::coro::blockingWait(cq.pollCq()),
      folly::coro::blockingWait(cq.pollCq())};
  EXPECT_EQ(cq.getNumCompletedWqes(), 0)
      << "All completions should have been consumed";

  auto positionOf = [&completions](uint64_t workReqId) -> int {
    for (size_t i = 0; i < completions.size(); i++) {
      if (completions[i].workReqId == workReqId) {
        return static_cast<int>(i);
      }
    }
    return -1;
  };
  const int firstPos = positionOf(kFirstWrId);
  const int secondPos = positionOf(kSecondWrId);
  const int receivePos = positionOf(kReceiveWrId);
  ASSERT_GE(firstPos, 0) << "the work request in flight when the tag entered "
                            "error "
                            "must still report once it settles";
  ASSERT_GE(secondPos, 0) << "a send posted to a tag in error must report";
  ASSERT_GE(receivePos, 0) << "a receive posted to a tag in error must report";

  // Sends and receives are separate work queues, so only the two sends are
  // ordered against each other.
  EXPECT_LT(firstPos, secondPos)
      << "the send posted before the failure must report ahead of the one "
         "posted after it, even though the later one was ready to flush first";
  EXPECT_EQ(completions[firstPos].status, kWrRemAccErr)
      << "the work request the peer rejected must report why, not a flush";
  EXPECT_EQ(completions[secondPos].status, kWrFlushErr)
      << "Post-error send WQE should be completed with kWrFlushErr";
  EXPECT_EQ(completions[receivePos].status, kWrFlushErr)
      << "Post-error receive WQE should be completed with kWrFlushErr";

  sender = nullptr;
}

// A receiver may not reject flow 0, so a NAK blaming it must not take it out
// of service. Flow 1 is rejected on every frame; flow 0 once, then acked.
TEST_F(NakTest, FlowZeroIsNotTakenOutOfServiceByAFlowIdNak) {
  auto sender = createTestSenderWithFlows(2);
  auto& socket = getTestSocket(sender.get());

  ON_CALL(congestionController(), getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));
  EXPECT_CALL(congestionController(), onFlowDisabled(0)).Times(0);
  EXPECT_CALL(congestionController(), onFlowDisabled(1)).Times(AtLeast(1));

  std::mutex mtx;
  std::condition_variable cv;
  std::set<uint16_t> posnOnFlow0;
  bool rejectedFlowZeroOnce = false;

  auto& flow0 = txFlow(0);
  auto& flow1 = txFlow(1);

  EXPECT_CALL(socket, write(_, _))
      .Times(AtLeast(kTestFrameCount))
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        auto packetInfo = parseDataPacket(buf);
        if (!packetInfo) {
          return buf.get()->computeChainDataLength();
        }
        std::lock_guard<std::mutex> lock(mtx);
        if (packetInfo->flowId == 1) {
          flow1.processAckPacket(
              buildNakFlowIdError(packetInfo->fsn, packetInfo->timestamp),
              simulatedProcessedAtNs(packetInfo->timestamp));
        } else if (!rejectedFlowZeroOnce) {
          rejectedFlowZeroOnce = true;
          flow0.processAckPacket(
              buildNakFlowIdError(packetInfo->fsn, packetInfo->timestamp),
              simulatedProcessedAtNs(packetInfo->timestamp));
        } else {
          posnOnFlow0.insert(packetInfo->posn);
          flow0.processAckPacket(
              buildAck(packetInfo->fsn, packetInfo->timestamp),
              simulatedProcessedAtNs(packetInfo->timestamp));
          if (posnOnFlow0.size() == kTestFrameCount) {
            cv.notify_one();
          }
        }
        return buf.get()->computeChainDataLength();
      });

  auto payload = std::make_unique<char[]>(kTestFrameCount * qp->getMtu());
  postTestWqe(kTestFrameCount, payload.get());
  qp->startProcessingWqes();

  {
    std::unique_lock<std::mutex> lock(mtx);
    bool success = cv.wait_for(lock, kTestTimeout, [&posnOnFlow0]() {
      return posnOnFlow0.size() == kTestFrameCount;
    });
    EXPECT_TRUE(success)
        << "flow 0 was rejected once and must still carry the message; it "
           "delivered "
        << posnOnFlow0.size() << " of " << kTestFrameCount << " posns";
  }
  EXPECT_TRUE(rejectedFlowZeroOnce)
      << "the case never rejected flow 0, so it proves nothing about keeping "
         "it in service";

  sender = nullptr;
}

TEST_F(NakTest, AFlowIdNakIsReportedWithoutItsFlowCount) {
  auto sender =
      createTestSenderWithFlows(2, LimitedTestSender::kDisableSendLoop);
  ON_CALL(congestionController(), getAvailableWindowSize(_))
      .WillByDefault(Return(kCongestionWindowSize));
  auto payload = std::make_unique<char[]>(kTestFrameCount * qp->getMtu());
  postTestWqe(kTestFrameCount, payload.get());
  qp->startProcessingWqes();
  ASSERT_TRUE(qp->waitForWqesToBePosted(1))
      << "precondition: the WQE reaches its Tag, or flow 1 has nothing to send";
  auto sent = txFlow(1).getNextPacketToSend();
  ASSERT_NE(sent, nullptr);
  const auto* data = dynamic_cast<const DataPacket*>(sent->packet.get());
  ASSERT_NE(data, nullptr);
  constexpr uint32_t kSupportedFlowCount = 1;
  const uint32_t sentAtNs = data->getDataHeader().getTimestamp();
  const AckHeader nak = AckHeader::AckHeaderBuilder()
                            .setTag(kMetaRoceInitialTag)
                            .setCumulativeFsn(data->getDataHeader().getFsn())
                            .setMaxRcvdFsn(data->getDataHeader().getFsn())
                            .setAckStatus(AckStatus::NAK_FLOW_ID_ERROR)
                            .setRateHints(kSupportedFlowCount)
                            .setTimestamp(sentAtNs)
                            .build();

  CongestionController::AckEvent reported;
  EXPECT_CALL(congestionController(), onPacketAck(1, _))
      .WillOnce(SaveArg<1>(&reported));
  txFlow(1).processAckPacket(nak, simulatedProcessedAtNs(sentAtNs));

  EXPECT_EQ(reported.numAckedPackets, 0)
      << "the NAK releases the rejected frame to be sent again rather than "
         "acknowledging it";
  EXPECT_TRUE(reported.sendPending)
      << "a NAK arriving while frames of the send are unsent ends fast start";
  EXPECT_EQ(reported.measuredRttNs, 0)
      << "the NAK times the arrival of no frame, so it carries no RTT sample";
  EXPECT_EQ(reported.rcnDesiredRateGbps, 0)
      << "a flow ID NAK's rate hint field holds the supported flow count, not "
         "a rate";
}

} // namespace libsoftmetaroce
