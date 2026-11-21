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

#include <folly/synchronization/Baton.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/congestion_control/Baseline.h>
#include <libsoftmetaroce/congestion_control/BaselineWithRCN.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/QpTestBase.h>
#include <libsoftmetaroce/test/TxTimerSuppressor.h>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

namespace libsoftmetaroce {

class EcnSourcePortChangeTest : public QpTestBase {
 protected:
  class TestSender : public LimitedTestSender {
   public:
    using LimitedTestSender::LimitedTestSender;
    using Sender::writeEgress;
  };

  struct RtoGate {
    folly::Baton<> reached;
    folly::Baton<> resume;
  };

  class GatedBaseline : public Baseline {
   public:
    GatedBaseline(HighResTimer& timer, std::shared_ptr<RtoGate> gate)
        : Baseline(128, 0), timer_(timer), gate_(std::move(gate)) {}

    uint16_t getAvailableWindowSize(uint16_t id) const override {
      const auto window = Baseline::getAvailableWindowSize(id);
      if (id == 1 && window == 0 &&
          std::this_thread::get_id() != senderThread_) {
        // The marker parks after the RTO callback queues its retransmission.
        timer_.scheduleTimeout(
            std::chrono::microseconds(0),
            HighResTimer::Callback([gate = gate_] {
              gate->reached.post();
              gate->resume.wait();
            }));
      }
      return window;
    }

   private:
    HighResTimer& timer_;
    std::shared_ptr<RtoGate> gate_;
    const std::thread::id senderThread_{std::this_thread::get_id()};
  };
  void SetUp() override {
    QpTestBase::SetUp();
    sender_ =
        std::make_unique<TestSender>(arbiter, [this](folly::EventBase* evb) {
          if (failNextSocketCreation_) {
            failNextSocketCreation_ = false;
            throw std::runtime_error("injected source socket failure");
          }
          return std::make_pair(
              std::make_unique<testing::NiceMock<MockUDPSocket>>(evb),
              std::unique_ptr<Modifier>{});
        });
    sender_->setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  }

  void resetTarget() {
    if (rtoGate_) {
      rtoGate_->resume.post();
    }
    target_.reset();
    retxTimer.fenceCallbacks();
  }

  void TearDown() override {
    resetTarget();
    QpTestBase::TearDown();
  }

  void start(
      bool rcn,
      uint16_t markedWindow = 2,
      bool gateRto = false,
      uint32_t frames = 128,
      bool separateLocals = false) {
    resetTarget();
    frames_.clear();
    otherFrames_.clear();
    if (gateRto) {
      FLAGS_libsoftmetaroce_min_rto_us = 10000;
      FLAGS_libsoftmetaroce_max_rto_us = 10000;
      rtoGate_ = std::make_shared<RtoGate>();
    }
    target_ = std::make_unique<Qp>(
        30, retxTimer, arbiter, mrs, &cq, &otherIbvQpStorage);
    target_->setMtu(mtu);
    target_->setRemoteQpNum(0);
    std::unique_ptr<Baseline> controller = rcn
        ? std::make_unique<BaselineWithRCN>(128, 0)
        : std::make_unique<Baseline>(128, 0);
    if (gateRto) {
      controller = std::make_unique<GatedBaseline>(retxTimer, rtoGate_);
    }
    controller_ = controller.get();
    target_->setCongestionController(std::move(controller));
    ibv_gid_entry entry{};
    entry.gid_type = kGidTypeMetaRoceUdp;
    target_->addPortPair(
        0,
        folly::IPAddress("fc00::1"),
        entry,
        0,
        folly::IPAddress("fc00::101"));
    if (separateLocals) {
      target_->addPortPair(
          1,
          folly::IPAddress("fc00::2"),
          entry,
          1,
          folly::IPAddress("fc00::102"));
    }
    target_->setupFlows(rcn ? 7 : 2, 0, sender_.get());
    markedId_ = rcn ? 6 : 1;
    payload_.resize(frames * mtu);
    auto* wqe = postWqeHelper(
        target_.get(),
        {.workType = Wqe::WorkType::kSend,
         .msgSizeBytes = static_cast<uint32_t>(payload_.size()),
         .address = payload_.data()});
    ASSERT_NE(wqe, nullptr);
    for (uint16_t id = 0; id <= markedId_; ++id) {
      const uint16_t count = id == 0 ? (rcn ? 40 : 13)
          : id == markedId_          ? markedWindow
                                     : 1;
      for (uint16_t sent = 0; sent < count; ++sent) {
        auto packet = target_->getTxFlowById(id)->getNextPacketToSend();
        ASSERT_NE(packet, nullptr);
        if (id == markedId_ || id == 0) {
          const auto* data = dynamic_cast<DataPacket*>(packet->packet.get());
          ASSERT_NE(data, nullptr);
          (id == 0 ? otherFrames_ : frames_).push_back(data->getDataHeader());
        }
      }
    }
  }

  void acknowledge(size_t frame, uint8_t ce, uint32_t rate = 0) {
    const auto& header = frames_.at(frame);
    target_->getTxFlowById(markedId_)->processAckPacket(
        AckHeader::AckHeaderBuilder()
            .setCumulativeFsn(header.getFsn())
            .setMaxRcvdFsn(header.getFsn())
            .setAckStatus(AckStatus::ACK)
            .setCnpCE(ce)
            .setRateHints(rate)
            .setTimestamp(header.getTimestamp())
            .build(),
        header.getTimestamp() + 1000);
  }

  void giveMarkedFlowWindow(uint32_t rate = 0) {
    for (const auto& header : otherFrames_) {
      if (controller().getAvailableWindowSize(markedId_) > 0) {
        return;
      }
      target_->getTxFlowById(0)->processAckPacket(
          AckHeader::AckHeaderBuilder()
              .setCumulativeFsn(header.getFsn())
              .setMaxRcvdFsn(header.getFsn())
              .setAckStatus(AckStatus::ACK)
              .setRateHints(rate)
              .setTimestamp(header.getTimestamp())
              .build(),
          header.getTimestamp() + 5000000);
    }
    EXPECT_GT(controller().getAvailableWindowSize(markedId_), 0)
        << "other flows' ACKs must give the idle flow window for new data";
  }

  void writeAndCheckHopLimit(std::unique_ptr<EgressPacket> packet, bool fast) {
    auto* socket =
        dynamic_cast<MockUDPSocket*>(packet->socketToSendOn->getSocket());
    ASSERT_NE(socket, nullptr);
    folly::Function<folly::Optional<folly::SocketCmsgMap>()> cmsgs;
    ON_CALL(*socket, setAdditionalCmsgsFunc(testing::_))
        .WillByDefault([&](auto&& func) { cmsgs = std::move(func); });
    EXPECT_CALL(*socket, write(testing::_, testing::_))
        .WillOnce([&](const folly::SocketAddress&,
                      const std::unique_ptr<folly::IOBuf>& buffer) {
          const auto map = cmsgs ? cmsgs() : folly::none;
          if (fast) {
            EXPECT_TRUE(map.has_value())
                << "the sender must mark fast retransmissions with a hop limit";
            if (map) {
              EXPECT_EQ(map->at({IPPROTO_IPV6, IPV6_HOPLIMIT}), 192)
                  << "fast retransmissions on the original path must set the high bit";
            }
          } else {
            EXPECT_FALSE(map.has_value())
                << "an RTO with changed entropy must not set the hop-limit high bit";
          }
          return buffer->computeChainDataLength();
        });
    EXPECT_FALSE(
        folly::coro::blockingWait(sender_->writeEgress(std::move(packet))))
        << "the retransmission must reach its selected socket without an egress fault";
    testing::Mock::VerifyAndClearExpectations(socket);
    ON_CALL(*socket, setAdditionalCmsgsFunc(testing::_))
        .WillByDefault([](auto&&) {});
  }

  const Baseline& controller() const {
    return *controller_;
  }

  gflags::FlagSaver flags_;
  ScopedSuppressedTxTimers timers_;
  std::vector<char> payload_;
  std::unique_ptr<TestSender> sender_;
  std::unique_ptr<Qp> target_;
  Baseline* controller_{nullptr};
  uint16_t markedId_{0};
  std::vector<DataHeader> frames_;
  std::vector<DataHeader> otherFrames_;
  std::shared_ptr<RtoGate> rtoGate_;
  bool failNextSocketCreation_{false};
};

TEST_F(
    EcnSourcePortChangeTest,
    EcnDefersEntropyUntilNewDataAfterFastRetransmission) {
  start(false, 4);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  BitVector<AckHeader::kAckTrackerBits> received;
  received.setBit(1);
  received.setBit(2);
  flow->processAckPacket(
      AckHeader::AckHeaderBuilder()
          .setCumulativeFsn(frames_[0].getFsn())
          .setMaxRcvdFsn(frames_[3].getFsn())
          .setFsnBitVector(received)
          .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
          .setCnpCE(2)
          .setTimestamp(frames_[3].getTimestamp())
          .build(),
      frames_[3].getTimestamp() + 1000);
  ASSERT_EQ(controller().getFwnd(markedId_), 0)
      << "precondition: ECN must empty the window after the SACK loss reduction";
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "the ECN ACK must defer entropy changes until eligible transmission";
  auto packet = flow->getNextPacketToSend();
  ASSERT_NE(packet, nullptr);
  EXPECT_EQ(packet->rxmitType, EgressPacket::RxmitType::Fast)
      << "the SACK hole must use fast retransmission";
  const auto* data = dynamic_cast<DataPacket*>(packet->packet.get());
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(data->getDataHeader().getFsn(), frames_[1].getFsn())
      << "deferring entropy must preserve the missing frame's sequence number";
  EXPECT_EQ(
      packet->socketToSendOn,
      sender_->getSendSocket(originalSocket, EgressPacket::Kind::Data))
      << "fast retransmissions must stay on the socket that sent the original frame";
  writeAndCheckHopLimit(std::move(packet), true);
  acknowledge(3, 0);
  giveMarkedFlowWindow();
  auto newPacket = flow->getNextPacketToSend();
  ASSERT_NE(newPacket, nullptr);
  EXPECT_EQ(newPacket->rxmitType, EgressPacket::RxmitType::None)
      << "the next packet must carry new data after the hole is acknowledged";
  EXPECT_NE(flow->getSocketId(), originalSocket)
      << "new data must consume the deferred source-port change";
  EXPECT_EQ(
      newPacket->socketToSendOn,
      sender_->getSendSocket(flow->getSocketId(), EgressPacket::Kind::Data))
      << "the new-data packet must be built on the newly selected socket";
  EXPECT_EQ(
      sender_->getAddressFromSocketId(flow->getSocketId()),
      folly::IPAddress("fc00::1"))
      << "source-port entropy changes must retain the configured local address";
}

TEST_F(EcnSourcePortChangeTest, RtoAfterEcnChangesEntropyOnce) {
  FLAGS_libsoftmetaroce_rtos_before_repath = 1;
  start(false, 2, true);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  acknowledge(0, 1);
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "the ECN ACK must leave the socket unchanged before the timeout";
  ASSERT_TRUE(rtoGate_->reached.try_wait_for(std::chrono::seconds(5)))
      << "precondition: the timeout must queue its retransmission before the send visit";
  auto packet = flow->getNextPacketToSend();
  ASSERT_NE(packet, nullptr);
  EXPECT_EQ(packet->rxmitType, EgressPacket::RxmitType::Rto)
      << "the timeout must build an RTO retransmission rather than a fast retransmission";
  EXPECT_NE(flow->getSocketId(), originalSocket)
      << "the existing RTO change must also satisfy pending ECN entropy";
  EXPECT_EQ(
      packet->socketToSendOn,
      sender_->getSendSocket(flow->getSocketId(), EgressPacket::Kind::Data))
      << "the timeout retransmission must use the changed source port";
  writeAndCheckHopLimit(std::move(packet), false);
  const auto selectedSocket = flow->getSocketId();
  EXPECT_EQ(flow->getNextPacketToSend(), nullptr)
      << "the empty window must prevent new data after the retransmission";
  EXPECT_EQ(flow->getSocketId(), selectedSocket)
      << "another visit at the same timeout count must preserve the source socket";
  acknowledge(1, 0);
  giveMarkedFlowWindow();
  auto newPacket = flow->getNextPacketToSend();
  ASSERT_NE(newPacket, nullptr);
  EXPECT_EQ(flow->getSocketId(), selectedSocket)
      << "new data must not repeat the ECN change already satisfied by RTO";
}

TEST_F(EcnSourcePortChangeTest, RtoBelowThresholdDefersEntropyUntilNewData) {
  FLAGS_libsoftmetaroce_rtos_before_repath = 2;
  start(false, 2, true);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  acknowledge(0, 1);
  ASSERT_TRUE(rtoGate_->reached.try_wait_for(std::chrono::seconds(5)))
      << "precondition: the first timeout must queue its sole retransmission";
  auto packet = flow->getNextPacketToSend();
  ASSERT_NE(packet, nullptr);
  EXPECT_EQ(packet->rxmitType, EgressPacket::RxmitType::Rto)
      << "the queued frame must remain an RTO retransmission";
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "a timeout below the RTO repath threshold must retain pending ECN entropy";
  acknowledge(1, 0);
  giveMarkedFlowWindow();
  auto newPacket = flow->getNextPacketToSend();
  ASSERT_NE(newPacket, nullptr);
  EXPECT_EQ(newPacket->rxmitType, EgressPacket::RxmitType::None)
      << "old-frame resolution must permit a new-data packet";
  EXPECT_NE(flow->getSocketId(), originalSocket)
      << "the first new-data packet must consume the ECN change retained across RTO";
}

TEST_F(EcnSourcePortChangeTest, PendingEntropyWithoutTransmissionKeepsSocket) {
  start(false);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  acknowledge(1, 1);
  ASSERT_EQ(controller().getFwnd(markedId_), 0)
      << "precondition: ECN must empty the window while acknowledging all outstanding data";
  EXPECT_FALSE(flow->hasUnackedData())
      << "the idle flow must have no outstanding frame that could time out";
  for (int visit = 0; visit < 3; ++visit) {
    EXPECT_EQ(flow->getNextPacketToSend(), nullptr)
        << "a pending source-port change must not enable transmission without window";
    EXPECT_EQ(flow->getSocketId(), originalSocket)
        << "a flow that never sends again must retain its socket";
  }
}

TEST_F(EcnSourcePortChangeTest, SoleRetransmissionDefersEntropyUntilNewData) {
  start(false, 3);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  acknowledge(1, 2);
  ASSERT_EQ(controller().getFwnd(markedId_), 0)
      << "precondition: ECN must leave one outstanding frame in a zero-window flow";
  ASSERT_EQ(flow->prepareRetxPacketsIfAny(true), 1)
      << "precondition: the one-FSN cursor interval must queue its unresolved frame";
  auto packet = flow->getNextPacketToSend();
  ASSERT_NE(packet, nullptr);
  EXPECT_EQ(packet->rxmitType, EgressPacket::RxmitType::Fast)
      << "the queued frame must remain a fast retransmission without a timeout";
  const auto* data = dynamic_cast<DataPacket*>(packet->packet.get());
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(data->getDataHeader().getFsn(), frames_[2].getFsn())
      << "the sole retransmission must preserve its outstanding FSN";
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "even a sole retransmission must retain pending ECN entropy for new data";
  EXPECT_EQ(
      packet->socketToSendOn,
      sender_->getSendSocket(originalSocket, EgressPacket::Kind::Data))
      << "the sole retransmission must keep the existing source port";
  writeAndCheckHopLimit(std::move(packet), true);
  EXPECT_TRUE(flow->hasUnackedData())
      << "retransmission must retain the outstanding frame until its ACK";
  acknowledge(2, 0);
  giveMarkedFlowWindow();
  EXPECT_GT(controller().getAvailableWindowSize(markedId_), 0)
      << "the final ACK must permit growth because retransmission adds no outstanding frame";
  auto newPacket = flow->getNextPacketToSend();
  ASSERT_NE(newPacket, nullptr);
  EXPECT_EQ(newPacket->rxmitType, EgressPacket::RxmitType::None)
      << "the retransmitted frame must resolve before new data is sent";
  EXPECT_NE(flow->getSocketId(), originalSocket)
      << "the first new-data packet must consume the pending ECN change";
}

TEST_F(EcnSourcePortChangeTest, PacketlessVisitRetainsPendingEntropy) {
  start(false, 2, false, 15);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  acknowledge(0, 1);
  ASSERT_EQ(flow->prepareRetxPacketsIfAny(true), 1)
      << "precondition: the retransmission must be queued before its ACK resolves it";
  acknowledge(1, 0);
  giveMarkedFlowWindow();
  EXPECT_EQ(flow->getNextPacketToSend(), nullptr)
      << "a resolved queued retransmission and a fully sent WQE must produce no packet";
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "a packetless visit must preserve the pending source-port change";
  auto* wqe = postWqeHelper(
      target_.get(),
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .address = payload_.data()});
  ASSERT_NE(wqe, nullptr);
  auto packet = flow->getNextPacketToSend();
  ASSERT_NE(packet, nullptr);
  EXPECT_NE(flow->getSocketId(), originalSocket)
      << "later new data must consume the change retained across a packetless visit";
}

TEST_F(EcnSourcePortChangeTest, FailedSourcePortChangeRemainsPending) {
  start(false, 2, false, 128, true);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  acknowledge(1, 1);
  giveMarkedFlowWindow();
  failNextSocketCreation_ = true;
  auto packet = flow->getNextPacketToSend();
  ASSERT_NE(packet, nullptr);
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "a failed source-port change must leave this transmission on the existing socket";
  const auto* data = dynamic_cast<DataPacket*>(packet->packet.get());
  ASSERT_NE(data, nullptr);
  frames_.push_back(data->getDataHeader());
  acknowledge(2, 0);
  auto nextPacket = flow->getNextPacketToSend();
  ASSERT_NE(nextPacket, nullptr);
  EXPECT_NE(flow->getSocketId(), originalSocket)
      << "a later transmission must retry the pending change after socket acquisition fails";
}

TEST_F(EcnSourcePortChangeTest, LossToZeroPreservesEntropy) {
  start(false);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  BitVector<AckHeader::kAckTrackerBits> received;
  received.setBit(1);
  flow->processAckPacket(
      AckHeader::AckHeaderBuilder()
          .setCumulativeFsn(UINT16_MAX)
          .setMaxRcvdFsn(frames_[1].getFsn())
          .setFsnBitVector(received)
          .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
          .setTimestamp(frames_[1].getTimestamp())
          .build(),
      frames_[1].getTimestamp() + 1000);
  ASSERT_EQ(controller().getFwnd(markedId_), 0)
      << "precondition: the SACK loss must empty this flow's window without CE";
  auto retransmission = flow->getNextPacketToSend();
  ASSERT_NE(retransmission, nullptr);
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "loss alone must not schedule an ECN source-port change";
  acknowledge(1, 0);
  giveMarkedFlowWindow();
  auto newPacket = flow->getNextPacketToSend();
  ASSERT_NE(newPacket, nullptr);
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "reactivation after loss alone must preserve entropy for new data";
}

TEST_F(EcnSourcePortChangeTest, EcnLeavingPositiveWindowPreservesEntropy) {
  start(false, 3);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  acknowledge(0, 1);
  ASSERT_GT(controller().getFwnd(markedId_), 0)
      << "precondition: this ECN reduction must leave a positive window";
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "ECN must preserve entropy while the flow retains window";
  acknowledge(2, 0, 0);
  giveMarkedFlowWindow(0);
  auto packet = flow->getNextPacketToSend();
  ASSERT_NE(packet, nullptr);
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "new data after ECN leaves window must keep the source port";
}

TEST_F(EcnSourcePortChangeTest, RcnToZeroPreservesEntropy) {
  start(true);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  acknowledge(0, 0, 1);
  ASSERT_EQ(controller().getFwnd(markedId_), 0)
      << "precondition: RCN must empty an unprotected flow";
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "an RCN reduction to zero must preserve entropy";
  acknowledge(1, 0, 1000);
  giveMarkedFlowWindow(1000);
  auto packet = flow->getNextPacketToSend();
  ASSERT_NE(packet, nullptr);
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "new data after RCN recovery must keep the source port";
}

TEST_F(EcnSourcePortChangeTest, RcnPriorityOverCePreservesEntropy) {
  start(true);
  auto* flow = target_->getTxFlowById(markedId_);
  const auto originalSocket = flow->getSocketId();
  acknowledge(0, 1, 1);
  ASSERT_EQ(controller().getFwnd(markedId_), 0)
      << "precondition: RCN must perform the reduction on this mixed ACK";
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "CE on an ACK reduced by RCN must not trigger new entropy";
  acknowledge(1, 0, 1000);
  giveMarkedFlowWindow(1000);
  auto packet = flow->getNextPacketToSend();
  ASSERT_NE(packet, nullptr);
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "new data after a mixed CE/RCN reduction must keep the source port";
}

TEST_F(EcnSourcePortChangeTest, FurtherMarkedAcksAtZeroPreserveEntropy) {
  start(false);
  auto* flow = target_->getTxFlowById(markedId_);
  acknowledge(0, 1);
  const auto afterReduction = flow->getSocketId();
  acknowledge(1, 1);
  acknowledge(1, 1);
  EXPECT_EQ(controller().getFwnd(markedId_), 0)
      << "marked ACKs at zero must leave the flow waiting for window credit";
  EXPECT_EQ(flow->getSocketId(), afterReduction)
      << "a new ACK at zero and its duplicate must not repeatedly change entropy";
  EXPECT_FALSE(flow->hasUnackedData())
      << "entropy handling must let the final ACK retire the outstanding data";
}

} // namespace libsoftmetaroce
