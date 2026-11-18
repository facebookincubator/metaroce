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

#include <utility>

#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/io/async/EventBase.h>
#include <gmock/gmock-actions.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Ecn.h>
#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/IngressPacket.h>
#include <libsoftmetaroce/PacketModifierSocket.h>
#include <libsoftmetaroce/PingController.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/Receiver.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/ControlHeader.h>
#include <libsoftmetaroce/codec/PingHeader.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>

using namespace testing;
namespace libsoftmetaroce {

static std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

namespace {
constexpr uint16_t kMetaRoceBindToPort = 0xace;
constexpr uint16_t kMetaRoceSendToPort = 0xace;
constexpr uint32_t kDefaultControlHeaderTimestamp = 12345;

// Helper function to build a ping packet for testing
std::unique_ptr<PingPacket> buildTestPingPacket(
    uint32_t srcQp,
    uint32_t dstQp,
    uint16_t flowId,
    uint8_t tag,
    ControlPacketType type = ControlPacketType::PING,
    PingErrorState errorState = PingErrorState::NO_ERROR,
    uint16_t pingId = 0) {
  BaseHeader::Opcode opcode(
      BaseHeader::Opcode::OperationType::UNRELIABLE_CONTROL);
  auto baseHeader = BaseHeader::BaseHeaderBuilder()
                        .setOpcode(std::move(opcode))
                        .setVersion(0)
                        .setFlowId(flowId)
                        .setDstQp(dstQp)
                        .setSrcQp(srcQp)
                        .build();

  auto controlHeader = ControlHeader::ControlHeaderBuilder()
                           .setType(static_cast<uint16_t>(type))
                           .setTimestamp(kDefaultControlHeaderTimestamp)
                           .setPingId(pingId)
                           .build();

  // Larger than any flow id these tests use, so the receiver's supported-count
  // check trips only where a case means it to.
  auto pingHeader = PingHeader::PingHeaderBuilder()
                        .setTag(tag)
                        .setErrorState(errorState)
                        .setSupportedFlowCount(100)
                        .build();

  return std::make_unique<PingPacket>(
      std::move(baseHeader),
      std::move(controlHeader),
      std::move(pingHeader),
      nullptr,
      0);
}
} // namespace

// Mock class wrapping Receiver for ping testing
class MockPingReceiver {
 public:
  explicit MockPingReceiver(
      folly::IPAddress localAddr,
      folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>&
          qpNumToQp,
      NiceMock<MockUDPSocket>* mockSocket,
      PingController& pingController) {
    // Create a socket maker that returns our mock socket
    auto socketMaker = [mockSocket](folly::EventBase*) {
      return std::make_pair(
          std::unique_ptr<folly::AsyncUDPSocket>(mockSocket), nullptr);
    };

    // Create the receiver with default port
    receiver_ = std::make_unique<Receiver>(
        localAddr,
        qpNumToQp,
        socketMaker,
        kMetaRoceBindToPort,
        kMetaRoceSendToPort,
        pingController);
  }

  // Method to simulate receiving a packet
  void simulateReceivePacket(
      std::unique_ptr<Packet> packet,
      uint8_t tos,
      folly::SocketAddress sourceAddr) {
    // Get the PacketModifierSocket::ReadCallback interface
    auto readCallback =
        static_cast<PacketModifierSocket::ReadCallback*>(receiver_.get());

    // Create an IngressPacket
    auto ingressPacket = std::make_unique<IngressPacket>(
        IngressPacket{std::move(packet), std::move(sourceAddr), tos});

    // Drive onDataAvailable on the reader's EventBase thread, as production
    // does, so reply writes run on the owning thread.
    receiver_->readerEventBaseForTest().runInEventBaseThreadAndWait(
        [&] { readCallback->onDataAvailable(std::move(ingressPacket)); });
  }

 private:
  std::unique_ptr<Receiver> receiver_;
};

class PingReceiverTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create an EventBase for the mock socket
    evb_ = std::make_unique<folly::EventBase>();

    // Create a mock socket with the EventBase
    mockSocket_ = new NiceMock<MockUDPSocket>(evb_.get());

    // Create a receiver with an empty QP map
    createReceiver();

    retxTimer = std::make_shared<HighResTimer>();
    arbiter = std::make_shared<QpArbiter>();
    cq = std::make_shared<CompletionQueue>();

    // Create a data packet for a non-existent QP
    srcQp = 123;
    dstQp = 456;
    flowId = 0;

    // Create a qp
    auto qp = std::make_unique<Qp>(
        456 /*qpNum*/, *retxTimer, *arbiter, mrs, cq.get(), &ibvQpStorage);
    qp->setMtu(4096);
    qp->addPortPair(folly::IPAddress("fc00::1"), folly::IPAddress("fc00::101"));
    // Create a path with ID 0 for the QP
    LimitedTestSender mocksender(*arbiter, mockUDPSocketMaker);
    mocksender.setLimitNumSockets(2);
    qp->setupFlows(1, 1, &mocksender);
    qp->setIbvQpState(IBV_QPS_INIT);
    qp->setIbvQpState(IBV_QPS_RTR);
    // Add the QP to the qp map
    qpNumToQp_.wlock()->emplace(456, std::move(qp));
  }

  void TearDown() override {
    mockReceiver_.reset();
    qpNumToQp_.wlock()->clear();
    retxTimer.reset();
    arbiter.reset();
    cq.reset();
    evb_.reset();
  }

  // Helper method to create a receiver with the mock socket
  void createReceiver() {
    mockReceiver_ = std::make_unique<MockPingReceiver>(
        folly::IPAddress("fc00::1"), qpNumToQp_, mockSocket_, *pingController_);
  }

  // Map of QP numbers to QPs (empty for testing missing QP scenario)
  folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>
      qpNumToQp_;

  // Raw pointer to the mock socket
  NiceMock<MockUDPSocket>* mockSocket_ = nullptr;

  // EventBase for the mock socket
  std::unique_ptr<folly::EventBase> evb_;

  // PingController for the receiver
  std::unique_ptr<PingController> pingController_{
      std::make_unique<PingController>(
          mockUDPSocketMaker,
          kMetaRoceSendToPort)};

  // The mock receiver under test
  std::unique_ptr<MockPingReceiver> mockReceiver_;

  uint32_t srcQp = 123;
  uint32_t dstQp = 456;
  uint16_t flowId = 0;

  std::shared_ptr<HighResTimer> retxTimer;
  std::shared_ptr<QpArbiter> arbiter;
  std::shared_ptr<CompletionQueue> cq;
  MemoryRegions mrs;
  struct ibv_qp ibvQpStorage{};
};

struct PingErrorStateTestCase {
  std::string name;
  uint32_t dstQp;
  uint16_t flowId;
  uint8_t tag;
  std::string sourceAddr;
  PingErrorState expectedErrorState;
};

class ReceiverPingErrorStateTest
    : public PingReceiverTest,
      public ::testing::WithParamInterface<PingErrorStateTestCase> {};

TEST_P(ReceiverPingErrorStateTest, VerifiesCorrectErrorState) {
  const auto& testCase = GetParam();

  auto pingPacket = buildTestPingPacket(
      /*srcQp=*/999, testCase.dstQp, testCase.flowId, testCase.tag);

  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([&testCase](
                    const folly::SocketAddress&,
                    const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);
        EXPECT_EQ(
            baseHeader.getOpcode(),
            BaseHeader::Opcode(
                BaseHeader::Opcode::OperationType::UNRELIABLE_CONTROL))
            << "the response reuses the request's base header, so it carries "
               "the same unreliable control opcode the request arrived under";

        ControlHeader controlHeader(cursor);
        EXPECT_EQ(controlHeader.getType(), ControlPacketType::PING_RESPONSE);

        PingHeader pingHeader(cursor);
        EXPECT_EQ(pingHeader.getErrorState(), testCase.expectedErrorState);

        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(
      std::move(pingPacket),
      kEcnECT0,
      folly::SocketAddress(testCase.sourceAddr, 12345));
}

INSTANTIATE_TEST_SUITE_P(
    PingErrorStates,
    ReceiverPingErrorStateTest,
    ::testing::Values(
        PingErrorStateTestCase{
            .name = "ValidQpReturnsNoError",
            .dstQp = 456,
            .flowId = 0,
            .tag = 0,
            .sourceAddr = "fc00::101",
            .expectedErrorState = PingErrorState::NO_ERROR},
        PingErrorStateTestCase{
            .name = "UnknownQpReturnsQpNotPresent",
            .dstQp = 9999,
            .flowId = 0,
            .tag = 0,
            .sourceAddr = "2001:db8::1",
            .expectedErrorState = PingErrorState::QP_NOT_PRESENT_OR_UNKNOWN},
        PingErrorStateTestCase{
            .name = "UnknownTagReturnsTagNotAssociated",
            .dstQp = 456,
            .flowId = 0,
            .tag = 99,
            .sourceAddr = "fc00::101",
            .expectedErrorState = PingErrorState::TAG_NOT_ASSOCIATED_WITH_QP},
        PingErrorStateTestCase{
            .name = "UnknownSourceAddrReturnsSourceNotAssociated",
            .dstQp = 456,
            .flowId = 0,
            .tag = 0,
            .sourceAddr = "2001:db8::dead",
            .expectedErrorState =
                PingErrorState::SOURCE_ADDRESS_NOT_ASSOCIATED_WITH_QP},
        PingErrorStateTestCase{
            .name = "InvalidPathReturnsPathIdUnsupported",
            .dstQp = 456,
            .flowId = 99,
            .tag = 0,
            .sourceAddr = "fc00::101",
            .expectedErrorState =
                PingErrorState::FLOW_ID_UNSUPPORTED_OR_TOO_LARGE}),
    [](const ::testing::TestParamInfo<PingErrorStateTestCase>& info) {
      return info.param.name;
    });

/*
 * Verifies that the ping response echoes back the original control header
 * fields (timestamp, reserved/pingId) from the ping request.
 */
TEST_F(PingReceiverTest, PingResponsePreservesControlHeaderFields) {
  const uint16_t pingId = 42;
  auto pingPacket = buildTestPingPacket(
      /*srcQp=*/999,
      /*dstQp=*/456,
      /*flowId=*/0,
      /*tag=*/0,
      ControlPacketType::PING,
      PingErrorState::NO_ERROR,
      pingId);

  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([pingId](
                    const folly::SocketAddress&,
                    const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);
        EXPECT_EQ(
            baseHeader.getOpcode(),
            BaseHeader::Opcode(
                BaseHeader::Opcode::OperationType::UNRELIABLE_CONTROL))
            << "the response reuses the request's base header, so it carries "
               "the same unreliable control opcode the request arrived under";

        ControlHeader controlHeader(cursor);
        EXPECT_EQ(controlHeader.getType(), ControlPacketType::PING_RESPONSE);
        // The ping response reuses the same packet so timestamp and pingId
        // should be preserved
        EXPECT_EQ(controlHeader.getTimestamp(), kDefaultControlHeaderTimestamp);
        EXPECT_EQ(controlHeader.getPingId(), pingId);

        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(
      std::move(pingPacket),
      kEcnECT0,
      folly::SocketAddress("fc00::101", kMetaRoceSendToPort));
}

TEST_F(PingReceiverTest, PingResponseSwapsTheQueuePairs) {
  constexpr uint32_t kRequesterQpn = 999;
  constexpr uint32_t kResponderQpn = 456;
  auto pingPacket = buildTestPingPacket(
      kRequesterQpn,
      kResponderQpn,
      /*flowId=*/0,
      /*tag=*/0,
      ControlPacketType::PING,
      PingErrorState::NO_ERROR,
      /*pingId=*/7);

  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([kRequesterQpn, kResponderQpn](
                    const folly::SocketAddress&,
                    const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);
        EXPECT_EQ(baseHeader.getSrcQp(), kResponderQpn)
            << "the responder answers as itself, so the queue pair the request "
               "was addressed to is the one the response comes from";
        EXPECT_EQ(baseHeader.getDstQp(), kRequesterQpn)
            << "and it answers to the requester, so a response that kept the "
               "request's pairs would be addressed back at the responder";

        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(
      std::move(pingPacket),
      kEcnECT0,
      folly::SocketAddress("fc00::101", kMetaRoceSendToPort));
}
} // namespace libsoftmetaroce
