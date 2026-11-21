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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/Flow.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/QpTestBase.h>
#include <libsoftmetaroce/test/TxTimerSuppressor.h>
#include <algorithm>
#include <array>

using namespace testing;
namespace libsoftmetaroce {
namespace {
auto mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb),
      std::unique_ptr<Modifier>{});
}
} // namespace

class RtoRetransmissionTest
    : public QpTestBase,
      public WithParamInterface<std::pair<uint32_t, uint16_t>> {
 protected:
  void SetUp() override {
    QpTestBase::SetUp();
    gflags::SetCommandLineOption(
        "libsoftmetaroce_rto_retransmission_count",
        std::to_string(GetParam().first).c_str());
    createAlwaysSendingMockCongestionController(qp);
    qp.setupFlows(1, 1, &sender_);
    flow_ = qp.getTxFlowById(0);
    auto& state = flow_->getTxState();
    state.sndUna = GetParam().second;
    state.sndNxt = state.sndUna;
    *state.sndCur.wlock() = state.sndUna;
    state.maxAckdFsn = state.sndUna - 1;
    wqe_ = postWqeHelper(
        &qp,
        {.workType = Wqe::WorkType::kSend,
         .msgSizeBytes = static_cast<uint32_t>(payload_.size()),
         .address = payload_.data()});
    for (uint16_t i = 0; i < kFrames; ++i) {
      ASSERT_NE(flow_->getNextPacketToSend(), nullptr);
    }
    ASSERT_EQ(flow_->getNextPacketToSend(), nullptr)
        << "precondition: initial WQE must emit exactly kFrames original packets";
  }

  void expectRetx(uint16_t offset, bool timeout) {
    auto packet = flow_->getNextPacketToSend();
    ASSERT_NE(packet, nullptr);
    const auto* data = dynamic_cast<const DataPacket*>(packet->packet.get());
    ASSERT_NE(data, nullptr);
    EXPECT_EQ(data->getDataHeader().getFsn(), fsn(offset))
        << "recovery must retransmit the next unresolved frame";
    EXPECT_TRUE(data->getDataHeader().getIsRetransmit())
        << "recovery packets must carry the retransmit bit";
    EXPECT_EQ(data->getDataHeader().getIsTimeout(), timeout)
        << "only timer-triggered retransmissions carry the timeout bit";
  }

  uint16_t fsn(uint16_t offset) const {
    return GetParam().second + offset;
  }

  static constexpr uint16_t kFrames = 6;
  gflags::FlagSaver flagSaver_;
  ScopedSuppressedTxTimers suppressTimers_;
  LimitedTestSender sender_{arbiter, mockUDPSocketMaker};
  std::array<char, kFrames * 4096> payload_{};
  TxFlow* flow_{nullptr};
  Wqe* wqe_{nullptr};
};

TEST_P(RtoRetransmissionTest, BoundsEachExpiryAndSkipsResolvedFrames) {
  auto& state = flow_->getTxState();
  for (const uint16_t offset : {1, 4}) {
    state.tracker[offset].state = Flow::FsnState::ACKED;
    wqe_->onFrameNewlyAcked(offset);
  }
  *state.sndCur.wlock() = state.sndNxt;
  const std::vector<uint16_t> unresolved{0, 2, 3, 5};
  const auto count = GetParam().first == 0
      ? unresolved.size()
      : std::min<size_t>(GetParam().first, unresolved.size());
  for (int expiry = 0; expiry < 2; ++expiry) {
    EXPECT_EQ(flow_->prepareRetxPacketsIfAny(true), count)
        << "each expiry must respect the configured burst limit";
    EXPECT_EQ(flow_->getNumPendingRetxPackets(), count)
        << "the burst must queue exactly the bounded unresolved frames";
    EXPECT_EQ(flow_->prepareRetxPacketsIfAny(true), 0)
        << "backpressure must not duplicate queued retransmissions";
    for (size_t i = 0; i < count; ++i) {
      expectRetx(unresolved[i], true);
    }
    EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
        << "recovery must not send beyond its burst or fresh ACK credit";
    EXPECT_EQ(*state.sndCur.rlock(), fsn(unresolved[count - 1] + 1))
        << "ACK continuation must resume after the emitted burst";
  }
}

TEST_P(RtoRetransmissionTest, FreshAcksContinueFromTheBurstEndpoint) {
  const auto count = GetParam().first == 0
      ? kFrames
      : std::min<uint32_t>(GetParam().first, kFrames);
  ASSERT_EQ(flow_->prepareRetxPacketsIfAny(true), count)
      << "precondition: the timeout queues the configured burst";
  for (uint16_t offset = 0; offset < count; ++offset) {
    expectRetx(offset, true);
  }
  const auto cumulativeAck = AckHeader::AckHeaderBuilder()
                                 .setCumulativeFsn(fsn(0))
                                 .setMaxRcvdFsn(fsn(0))
                                 .setAckStatus(AckStatus::ACK)
                                 .setTimestamp(0)
                                 .build();
  flow_->processAckPacket(cumulativeAck);
  if (count < kFrames) {
    expectRetx(count, false);
  }
  EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
      << "recovery must not send beyond its burst or fresh ACK credit";
  flow_->processAckPacket(cumulativeAck);
  EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
      << "recovery must not send beyond its burst or fresh ACK credit";

  BitVector<AckHeader::kAckTrackerBits> sack;
  sack.setBit(3);
  const auto selectiveAck = AckHeader::AckHeaderBuilder()
                                .setCumulativeFsn(fsn(0))
                                .setMaxRcvdFsn(fsn(4))
                                .setFsnBitVector(sack)
                                .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                                .setTimestamp(0)
                                .build();
  flow_->processAckPacket(selectiveAck);
  if (count + 1 < kFrames) {
    expectRetx(count + 1 == 4 ? 5 : count + 1, false);
  }
  EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
      << "recovery must not send beyond its burst or fresh ACK credit";
  flow_->processAckPacket(selectiveAck);
  EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
      << "recovery must not send beyond its burst or fresh ACK credit";

  const auto batchAck = AckHeader::AckHeaderBuilder()
                            .setCumulativeFsn(fsn(2))
                            .setMaxRcvdFsn(fsn(2))
                            .setAckStatus(AckStatus::ACK)
                            .setTimestamp(0)
                            .build();
  flow_->processAckPacket(batchAck);
  if (count == 1) {
    expectRetx(3, false);
    expectRetx(5, false);
  } else if (count == 2) {
    expectRetx(5, false);
  }
  EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
      << "recovery must not send beyond its burst or fresh ACK credit";
  flow_->processAckPacket(batchAck);
  EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
      << "recovery must not send beyond its burst or fresh ACK credit";
}

TEST_P(RtoRetransmissionTest, RecoveryDoesNotIncludePacketsSentAfterTimeout) {
  const auto count = GetParam().first == 0
      ? kFrames
      : std::min<uint32_t>(GetParam().first, kFrames);
  ASSERT_EQ(flow_->prepareRetxPacketsIfAny(true), count)
      << "precondition: timeout must queue the configured burst";
  for (uint16_t offset = 0; offset < count; ++offset) {
    expectRetx(offset, true);
  }
  // Grow sndNxt after the timeout's window has been captured.
  std::array<char, 4096> laterPayload{};
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = laterPayload.size(),
       .address = laterPayload.data()});
  auto original = flow_->getNextPacketToSend();
  ASSERT_NE(original, nullptr);
  const auto* data = dynamic_cast<const DataPacket*>(original->packet.get());
  ASSERT_NE(data, nullptr);
  ASSERT_EQ(data->getDataHeader().getFsn(), fsn(kFrames))
      << "precondition: newly posted WQE must extend the window at sndNxt";
  ASSERT_FALSE(data->getDataHeader().getIsRetransmit())
      << "precondition: newly posted WQE must emit an original packet";
  // Five fresh ACKs provide more credit than the old window's last packet.
  const auto ack = AckHeader::AckHeaderBuilder()
                       .setCumulativeFsn(fsn(kFrames - 2))
                       .setMaxRcvdFsn(fsn(kFrames - 2))
                       .setAckStatus(AckStatus::ACK)
                       .setTimestamp(0)
                       .build();
  flow_->processAckPacket(ack);
  if (count < kFrames) {
    expectRetx(kFrames - 1, false);
  }
  EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
      << "ACK continuation must stop at the timeout's original window end";
  flow_->processAckPacket(ack);
  EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
      << "duplicate ACK after original window end must not retransmit post-timeout packets";
  // A subsequent timeout must capture the expanded window.
  const auto nextCount = GetParam().first == 1 ? 1u : 2u;
  EXPECT_EQ(flow_->prepareRetxPacketsIfAny(true), nextCount)
      << "subsequent timeout must capture the expanded window including post-timeout packets";
  expectRetx(kFrames - 1, true);
  if (nextCount == 2) {
    expectRetx(kFrames, true);
  }
  EXPECT_EQ(flow_->getNextPacketToSend(), nullptr)
      << "subsequent timeout must not queue more than its configured burst";
}

INSTANTIATE_TEST_SUITE_P(
    CountsAndWrap,
    RtoRetransmissionTest,
    Values(
        std::pair{0u, uint16_t{0}},
        std::pair{1u, uint16_t{0}},
        std::pair{2u, uint16_t{0}},
        std::pair{9u, uint16_t{0}},
        std::pair{0u, uint16_t{65534}},
        std::pair{1u, uint16_t{65534}},
        std::pair{2u, uint16_t{65534}},
        std::pair{9u, uint16_t{65534}}),
    [](const auto& info) {
      return "Count" + std::to_string(info.param.first) + "Base" +
          std::to_string(info.param.second);
    });

} // namespace libsoftmetaroce
