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

#include <atomic>
#include <cstring>
#include <functional>
#include <limits>
#include <set>
#include <thread>
#include <tuple>

#include <folly/ScopeGuard.h>
#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/coro/BlockingWait.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/EventBase.h>
#include <folly/synchronization/Baton.h>
#include <gmock/gmock-actions.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Ecn.h>
#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/Encapsulation.h>
#include <libsoftmetaroce/IngressPacket.h>
#include <libsoftmetaroce/PacketModifierSocket.h>
#include <libsoftmetaroce/PingController.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/Receiver.h>
#include <libsoftmetaroce/SharedReceiveQueue.h>
#include <libsoftmetaroce/codec/AckHeader.h>
#include <libsoftmetaroce/codec/AtomicHeader.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/DataHeader.h>
#include <libsoftmetaroce/codec/MetaRoceCodec.h>
#include <libsoftmetaroce/codec/Packet.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <libsoftmetaroce/test/PostToTag.h>
#include <libsoftmetaroce/test/SourcePortRecorder.h>

using namespace testing;
using testing::AtLeast;
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
// Away from zero, so a case must use the base its receives were given.
constexpr uint16_t kFixtureRxCsn = 101;
// The two ingress UDP source ports the ACK source-port cases move between.
constexpr uint16_t kIngressSourcePortA = 40000;
constexpr uint16_t kIngressSourcePortB = 40001;

class PayloadOnlyEncapsulation final : public Encapsulation {
 public:
  size_t frameOutgoing(folly::IOBufQueue&, std::optional<uint32_t>) noexcept
      override {
    return 0;
  }

  std::optional<IngressMetadata> deframeIncoming(
      std::unique_ptr<folly::IOBuf>&) noexcept override {
    return IngressMetadata{};
  }

  size_t payloadSize(const folly::io::Cursor& cursor) const noexcept override {
    return cursor.totalLength();
  }
};

// Helper function to build a data packet for testing
std::unique_ptr<DataPacket> buildTestDataPacket(
    uint32_t srcQp,
    uint32_t dstQp,
    uint16_t flowId,
    uint16_t fsn,
    uint16_t msn,
    uint8_t tag = 0,
    uint16_t posn = 0,
    const void* payload = nullptr,
    uint32_t payloadSize = 0,
    BaseHeader::Opcode::OperationType opType =
        BaseHeader::Opcode::OperationType::SEND,
    std::optional<uint32_t> maybeImmData = std::nullopt,
    uint8_t padding = 0,
    // The csn the fixture's first posted receive takes.
    uint16_t csn = kFixtureRxCsn,
    // The sender's snd.una; defaults to this packet's own fsn.
    std::optional<uint16_t> una = std::nullopt) {
  BaseHeader::Opcode opCode(opType);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(srcQp)
                                    .setDstQp(dstQp)
                                    .setFlowId(flowId)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();

  // Get current timestamp
  const auto nowNs = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count());

  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(fsn)
                                    .setUna(una.value_or(fsn))
                                    .setPosn(posn)
                                    .setMsn(msn)
                                    .setCsn(csn)
                                    .setTag(tag)
                                    .setTimestamp(nowNs)
                                    .setAckRequest(true)
                                    .setPadding(padding)
                                    .build();

  return std::make_unique<DataPacket>(
      baseHeader, dataHeader, payload, payloadSize, maybeImmData);
}

// An atomic ack carries its value in the AtomicAckHeader, which the receiver
// reaches by a dynamic_cast, so a plain DataPacket will not stand in.
std::unique_ptr<AtomicAckPacket> buildTestAtomicAckPacket(
    uint32_t srcQp,
    uint32_t dstQp,
    uint16_t flowId,
    uint16_t fsn,
    uint16_t msn,
    uint64_t origRemoteData) {
  BaseHeader::Opcode opCode(BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(srcQp)
                                    .setDstQp(dstQp)
                                    .setFlowId(flowId)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();

  const auto nowNs = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count());

  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(fsn)
                                    .setUna(fsn)
                                    .setPosn(0)
                                    .setMsn(msn)
                                    .setTag(0)
                                    .setTimestamp(nowNs)
                                    .setAckRequest(true)
                                    .build();

  return std::make_unique<AtomicAckPacket>(
      baseHeader, dataHeader, AtomicAckHeader(origRemoteData));
}

// Builds the packet the parser produces for an opcode it cannot decode. The
// tests below drive the receiver via simulateReceivePacket, which bypasses the
// parser, so they must construct this type directly.
std::unique_ptr<UnsupportedPacket> buildTestUnsupportedPacket(
    uint32_t srcQp,
    uint32_t dstQp,
    uint16_t flowId,
    uint16_t fsn,
    uint16_t msn,
    uint8_t tag,
    BaseHeader::Opcode::OperationType opType) {
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(srcQp)
                                    .setDstQp(dstQp)
                                    .setFlowId(flowId)
                                    .setOpcode(BaseHeader::Opcode(opType))
                                    .setVersion(0)
                                    .build();
  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(fsn)
                                    .setUna(fsn)
                                    .setPosn(0)
                                    .setMsn(msn)
                                    .setTag(tag)
                                    .setAckRequest(true)
                                    .build();
  return std::make_unique<UnsupportedPacket>(baseHeader, dataHeader);
}

// Helper function to build a data packet for testing
std::unique_ptr<DataPacket> buildTestWritePacket(
    uint32_t srcQp,
    uint32_t dstQp,
    uint16_t flowId,
    uint16_t fsn,
    uint16_t msn,
    uint64_t remoteAddress,
    uint32_t remoteKey,
    char* payload,
    int length, // of both payload and write
    BaseHeader::Opcode::OperationType opType =
        BaseHeader::Opcode::OperationType::WRITE,
    uint16_t posn = 0,
    std::optional<uint32_t> maybeImmData = std::nullopt,
    // Defaults to a single-packet message; pass a larger total to describe one
    // packet of several.
    std::optional<int> messageLength = std::nullopt,
    std::optional<uint16_t> una = std::nullopt) {
  BaseHeader::Opcode opCode(opType);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(srcQp)
                                    .setDstQp(dstQp)
                                    .setFlowId(flowId)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();

  // Get current timestamp
  const auto nowNs = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count());

  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(fsn)
                                    .setUna(una.value_or(fsn))
                                    .setPosn(posn)
                                    .setMsn(msn)
                                    .setTimestamp(nowNs)
                                    .setAckRequest(true)
                                    .build();

  const ReadWriteHeader readWriteHeader(
      remoteAddress, remoteKey, messageLength.value_or(length));

  return std::make_unique<ReadWritePacket>(
      baseHeader, dataHeader, readWriteHeader, payload, length, maybeImmData);
}

// The operand every atomic built here carries, so a case can recognize the
// result: what a fetch-add adds.
constexpr uint64_t kTestAtomicOperand = 42;

std::unique_ptr<AtomicPacket> buildTestAtomicPacket(
    uint32_t srcQp,
    uint32_t dstQp,
    uint16_t flowId,
    uint16_t fsn,
    uint16_t msn,
    uint64_t remoteAddr,
    uint32_t rkey,
    BaseHeader::Opcode::OperationType opType =
        BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD,
    uint16_t posn = 0,
    std::optional<uint16_t> una = std::nullopt) {
  BaseHeader::Opcode opCode(opType);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(srcQp)
                                    .setDstQp(dstQp)
                                    .setFlowId(flowId)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();

  const auto nowNs = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count());

  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(fsn)
                                    .setUna(una.value_or(fsn))
                                    .setPosn(posn)
                                    .setMsn(msn)
                                    .setTimestamp(nowNs)
                                    .setAckRequest(true)
                                    .build();

  AtomicHeader atomicHeader(
      remoteAddr, rkey, kTestAtomicOperand, /*compareData=*/0);

  return std::make_unique<AtomicPacket>(baseHeader, dataHeader, atomicHeader);
}

// A read request carries no payload; its header identifies the region to read.
std::unique_ptr<DataPacket> buildTestReadRequestPacket(
    uint32_t srcQp,
    uint32_t dstQp,
    uint16_t flowId,
    uint16_t fsn,
    uint16_t msn,
    uint64_t remoteAddress,
    uint32_t remoteKey,
    int readLength,
    std::optional<uint16_t> una = std::nullopt) {
  return buildTestWritePacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      remoteAddress,
      remoteKey,
      /*payload=*/nullptr,
      /*length=*/0,
      BaseHeader::Opcode::OperationType::READ,
      /*posn=*/0,
      /*maybeImmData=*/std::nullopt,
      readLength,
      una);
}

} // namespace

// Mock class for testing HRNR functionality
class MockHrnrReceiver {
 public:
  explicit MockHrnrReceiver(
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
      uint16_t sourcePort = 12345) {
    // Create a source address
    folly::SocketAddress sourceAddr("2001:db8::1", sourcePort);

    // Get the PacketModifierSocket::ReadCallback interface
    auto readCallback =
        static_cast<PacketModifierSocket::ReadCallback*>(receiver_.get());

    // Create an IngressPacket
    auto ingressPacket = std::make_unique<IngressPacket>(
        IngressPacket{std::move(packet), sourceAddr, tos});

    // Drive onDataAvailable on the reader's EventBase thread, as production
    // does, so reply writes run on the owning thread.
    receiver_->readerEventBaseForTest().runInEventBaseThreadAndWait(
        [&] { readCallback->onDataAvailable(std::move(ingressPacket)); });
  }

  [[nodiscard]] const Receiver& receiver() const {
    return *receiver_;
  }

 private:
  std::unique_ptr<Receiver> receiver_;
};

class ReceiverTest : public ::testing::Test {
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

    buildQp();
  }

  using SocketMaker = std::function<std::pair<
      std::unique_ptr<folly::AsyncUDPSocket>,
      std::unique_ptr<Modifier>>(folly::EventBase*)>;

  // Builds the fixture's QP, configured and then run, as a client's is.
  void buildQp(
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr,
      SocketMaker maker = mockUDPSocketMaker,
      unsigned socketLimit = 2,
      uint32_t rxFlowCount = 1) {
    // A tag is added before RTR, so a case rebuilding the queue pair starts it
    // over from where a client's begins.
    ibvQpStorage.state = IBV_QPS_RESET;
    auto qp = std::make_unique<Qp>(
        kFixtureQpNum,
        *retxTimer,
        *arbiter,
        mrs,
        cq.get(),
        &ibvQpStorage,
        /*rcnRateHint=*/nullptr,
        /*sqSigAll=*/false,
        /*maxInlineData=*/0,
        kMaxQpWorkRequests,
        kMaxQpWorkRequests,
        sharedReceiveQueue);
    qp->setIbvQpState(IBV_QPS_INIT);
    qp->setMtu(4096);
    qp->addPortPair(folly::IPAddress("fc00::1"), folly::IPAddress("fc00::101"));
    qp->setRemoteQpNum(srcQp);
    // Create a path with ID 0 for the QP
    mockSender_ =
        std::make_shared<LimitedTestSender>(*arbiter, std::move(maker));
    mockSender_->setLimitNumSockets(socketLimit);
    qp->setupFlows(1, rxFlowCount, mockSender_.get());
    const bool configured =
        qp->getTag(kMetaRoceInitialTag)
            ->setReceiveSequenceNumbers(
                {.nextMsn = kFixtureCdmsn + 1, .nextCsn = kFixtureRxCsn});
    CHECK(configured) << "the queue pair must carry the sequencing the cases "
                         "are written against";
    qp->setIbvQpState(IBV_QPS_RTR);
    qp->setIbvQpState(IBV_QPS_RTS);
    // Add the QP to the qp map
    qpNumToQp_.wlock()->emplace(kFixtureQpNum, std::move(qp));
  }

  void TearDown() override {
    // Clean up in the right order to avoid lifecycle issues
    qpNumToQp_.wlock()->clear();
    mockSender_.reset();
    mockReceiver_.reset();

    // Keep the shared components alive until the end
    retxTimer.reset();
    arbiter.reset();
    cq.reset();

    // Explicitly reset the mockReceiver_ first to ensure proper destruction
    // order
    mockReceiver_.reset();

    // Clean up the EventBase
    evb_.reset();
  }

  // Helper method to create a receiver with the mock socket
  void createReceiver() {
    mockReceiver_ = std::make_unique<MockHrnrReceiver>(
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
  std::unique_ptr<MockHrnrReceiver> mockReceiver_;

  // The queue pair the fixture builds. `dstQp` is what a case addresses, which
  // a case addressing a queue pair the map has none of reassigns.
  static constexpr uint32_t kFixtureQpNum = 456;
  static constexpr uint16_t kFixtureCdmsn = 20;

  uint32_t srcQp = 123;
  uint32_t dstQp = kFixtureQpNum;
  uint16_t flowId = 0;
  uint16_t fsn = 10;
  // The first msn the responder expects, so a case sending this one lands in
  // order and one past it waits for it.
  uint16_t msn = kFixtureCdmsn + 1;

  std::shared_ptr<HighResTimer> retxTimer;
  std::shared_ptr<QpArbiter> arbiter;
  std::shared_ptr<CompletionQueue> cq;
  std::shared_ptr<LimitedTestSender> mockSender_;
  MemoryRegions mrs;
  struct ibv_qp ibvQpStorage{};
  // Must be declared in the fixture so that it is destroyed with the qp.
  struct ibv_qp secondTagIbvQpStorage{};

  // Posts a read on the fixture's tag and answers with the tx msn it took.
  // `registeredSize` is the region registered for the response, possibly short.
  uint16_t postReadWqe(
      void* buffer,
      uint32_t size,
      uint32_t registeredSize,
      uint64_t workReqId = 42) {
    auto lockedMap = qpNumToQp_.rlock();
    const auto* qp = lockedMap->at(dstQp).get();
    auto tag = qp->getTag(kMetaRoceInitialTag);
    const uint16_t readMsn = tag->getNextMsn();
    const uint32_t lkey =
        mrs.reg_mr(nullptr, buffer, registeredSize, IBV_ACCESS_LOCAL_WRITE)
            ->lkey;
    auto readWqe = std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kRead,
        workReqId,
        oneSge(buffer, size, lkey),
        qp->getMtu(),
        /*remoteAddress=*/0x1000,
        /*rkey=*/1,
        /*send_flags=*/IBV_SEND_SIGNALED);
    postToTag(*tag, std::move(readWqe), qp->getMtu());
    return readMsn;
  }

  // Posts a read that scatters into `buffers`, each registered for local
  // write under its own key except the one at `unregisteredIndex`.
  uint16_t postSegmentedReadWqe(
      const std::vector<folly::MutableByteRange>& buffers,
      std::optional<size_t> unregisteredIndex = std::nullopt) {
    auto lockedMap = qpNumToQp_.rlock();
    const auto* qp = lockedMap->at(dstQp).get();
    auto tag = qp->getTag(kMetaRoceInitialTag);
    const uint16_t readMsn = tag->getNextMsn();
    LocalSges localSges;
    for (size_t i = 0; i < buffers.size(); ++i) {
      const folly::MutableByteRange buffer = buffers[i];
      const uint32_t lkey = i == unregisteredIndex
          ? 0xbadbad
          : mrs.reg_mr(
                   nullptr,
                   buffer.data(),
                   buffer.size(),
                   IBV_ACCESS_LOCAL_WRITE)
                ->lkey;
      localSges.push_back(
          {.address = buffer.data(),
           .length = static_cast<uint32_t>(buffer.size()),
           .lkey = lkey});
    }
    auto readWqe = std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kRead,
        /*workReqId=*/42,
        std::move(localSges),
        qp->getMtu(),
        /*remoteAddress=*/0x1000,
        /*rkey=*/1);
    postToTag(*tag, std::move(readWqe), qp->getMtu());
    return readMsn;
  }

  void postReceiveWqe(
      void* buffer,
      uint32_t bufferSize,
      uint64_t workReqId = 42,
      uint8_t tagNum = kMetaRoceInitialTag) {
    auto lockedMap = qpNumToQp_.rlock();
    auto* qp = lockedMap->at(dstQp).get();
    auto tag = qp->getTag(tagNum);
    // Carry a real lkey so the arriving message validates against the buffer; a
    // zero-length receive has none to register.
    const uint32_t lkey = (buffer != nullptr && bufferSize > 0)
        ? mrs.reg_mr(
                 nullptr,
                 buffer,
                 bufferSize,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE)
              ->lkey
        : 0;
    auto rwqe = std::make_unique<ReceiveWqe>(
        workReqId, oneSge(buffer, bufferSize, lkey), qp->getMtu());
    postToTag(*tag, std::move(rwqe), qp->getMtu());
  }

  // Header routing fields are validated by SendNakAddressError;
  // this helper checks only the NAK status.
  void expectNakAddressError() {
    EXPECT_CALL(*mockSocket_, write(_, _))
        .WillOnce([](const folly::SocketAddress&,
                     const std::unique_ptr<folly::IOBuf>& buf) {
          folly::io::Cursor cursor(buf.get());
          BaseHeader baseHeader(cursor);
          AckHeader ackHeader(cursor);
          EXPECT_EQ(
              ackHeader.getAckStatus(),
              AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
              << "address violation should produce NAK_ADDRESS_INVALID_ACCESS_ERROR";
          return buf->computeChainDataLength();
        });
  }

  void expectNakInvalidRequest() {
    EXPECT_CALL(*mockSocket_, write(_, _))
        .WillOnce([](const folly::SocketAddress&,
                     const std::unique_ptr<folly::IOBuf>& buf) {
          folly::io::Cursor cursor(buf.get());
          BaseHeader baseHeader(cursor);
          AckHeader ackHeader(cursor);
          EXPECT_EQ(ackHeader.getAckStatus(), AckStatus::NAK_INVALID_REQUEST)
              << "length violation should produce NAK_INVALID_REQUEST per spec 5.5";
          return buf->computeChainDataLength();
        });
  }

  void expectNakOperationalError() {
    EXPECT_CALL(*mockSocket_, write(_, _))
        .WillOnce([](const folly::SocketAddress&,
                     const std::unique_ptr<folly::IOBuf>& buf) {
          folly::io::Cursor cursor(buf.get());
          BaseHeader baseHeader(cursor);
          AckHeader ackHeader(cursor);
          EXPECT_EQ(ackHeader.getAckStatus(), AckStatus::NAK_OPERATIONAL_ERROR)
              << "a request the responder cannot serve should produce "
                 "NAK_OPERATIONAL_ERROR";
          return buf->computeChainDataLength();
        });
  }
};

/*
 * Tests the HRNR (Host Receiver Not Ready) mechanism when the receiver
 * doesn't have a QP for the incoming packet.
 *
 * This test verifies that when the Receiver receives a data packet for a QP
 * that doesn't exist, it sends an HRNR response back to the sender.
 *
 * The test checks:
 * 1. The HRNR packet is sent back to the source address
 * 2. The HRNR packet has the correct fields set (AckStatus::HRNR)
 * 3. The HRNR packet has the correct backoff time exponent
 */
TEST_F(ReceiverTest, SendHrnrForMissingQp) {
  dstQp = 1138; // not existing
  auto dataPacket = buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn);

  // Expect a write call to the socket with an HRNR packet
  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([&](const folly::SocketAddress& sockAddr,
                    const std::unique_ptr<folly::IOBuf>& buf) {
        // Parse the packet from the IOBuf
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);

        // Verify it's an ACK packet with the right opcode
        EXPECT_TRUE(baseHeader.getOpcode().isAnAck());

        // Verify the base header fields
        EXPECT_EQ(baseHeader.getSrcQp(), dstQp);
        EXPECT_EQ(baseHeader.getDstQp(), srcQp);
        EXPECT_EQ(baseHeader.getFlowId(), flowId);
        // Read the ACK header
        AckHeader ackHeader(cursor);

        // Verify the ACK header fields
        EXPECT_EQ(ackHeader.getAckStatus(), AckStatus::HRNR);
        EXPECT_EQ(ackHeader.getCumulativeFsn(), fsn);
        EXPECT_EQ(ackHeader.getMaxRcvdFsn(), fsn);
        EXPECT_EQ(ackHeader.getCumulRespDeliveredMsn(), msn);
        EXPECT_EQ(ackHeader.getRnrBackoffTimeExp(), kHrnrBackoffTimeExp);
        EXPECT_EQ(ackHeader.getRateHints(), 0u)
            << "the rate hint is reserved on an HRNR, and a reserved field a "
               "later revision may define must leave here as zero";

        // Confirm sent toward the metaroce port.
        EXPECT_EQ(sockAddr.getPort(), kMetaRoceSendToPort);
        EXPECT_EQ(sockAddr.describe(), "[2001:db8::1]:2766");

        return buf->computeChainDataLength();
      });

  //   Simulate receiving the data packet
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);
}

/*
 * Tests the NAK Flow ID Error mechanism when the receiver has a QP but
 * the flow ID in the incoming packet doesn't exist.
 *
 * This test verifies that when the Receiver receives a data packet for a QP
 * that exists but with a flow ID that doesn't exist, it sends a NAK with
 * AckStatus::NAK_FLOW_ID_ERROR back to the sender.
 *
 * The test checks:
 * 1. The NAK packet is sent back to the source address
 * 2. The NAK packet has the correct fields set (AckStatus::NAK_FLOW_ID_ERROR)
 * 3. The NAK packet has the correct FSN, MSN, and timestamp values
 */
TEST_F(ReceiverTest, SendNakForNonExistentFlow) {
  // Create a data packet for the existing QP but with non-existent flow ID (1)
  const uint16_t flowId = 1; // This flow doesn't exist (only flow 0 exists)
  auto dataPacket = buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn);

  // Expect a write call to the socket with a NAK Flow ID Error packet
  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([&](const folly::SocketAddress& sockAddr,
                    const std::unique_ptr<folly::IOBuf>& buf) {
        // Parse the packet from the IOBuf
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);

        // Verify it's an ACK packet with the right opcode
        EXPECT_TRUE(baseHeader.getOpcode().isAnAck());

        // Verify the base header fields
        EXPECT_EQ(baseHeader.getSrcQp(), dstQp);
        EXPECT_EQ(baseHeader.getDstQp(), srcQp);
        EXPECT_EQ(baseHeader.getFlowId(), flowId);

        // Read the ACK header
        AckHeader ackHeader(cursor);

        // Verify the ACK header fields
        EXPECT_EQ(ackHeader.getAckStatus(), AckStatus::NAK_FLOW_ID_ERROR);
        EXPECT_EQ(ackHeader.getCumulativeFsn(), fsn);
        EXPECT_EQ(ackHeader.getMaxRcvdFsn(), fsn);
        EXPECT_EQ(ackHeader.getRateHints(), 1u)
            << "a flow-ID NAK redefines the rate hint field as the flow count "
               "this responder supports, so zeroing it as reserved would "
               "withhold the one number the sender needs to recover";

        // Confirm sent toward the metaroce port.
        EXPECT_EQ(sockAddr.getPort(), kMetaRoceSendToPort);
        EXPECT_EQ(sockAddr.describe(), "[2001:db8::1]:2766");

        return buf->computeChainDataLength();
      });

  // Simulate receiving the data packet
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);
}

TEST_F(ReceiverTest, APacketFromAnotherSourceQpIsDiscardedSilently) {
  constexpr uint32_t kSize = 64;
  char recvBuffer[kSize] = {};
  postReceiveWqe(recvBuffer, kSize);
  char payload[kSize];
  std::memset(payload, 'S', kSize);

  EXPECT_CALL(*mockSocket_, write(_, _)).Times(0);

  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp + 1,
          dstQp,
          flowId,
          fsn,
          msn,
          /*tag=*/0,
          /*posn=*/0,
          payload,
          kSize),
      kEcnECT0);

  {
    auto lockedMap = qpNumToQp_.rlock();
    EXPECT_EQ(
        lockedMap->at(dstQp)->getRxFlowById(flowId)->getNextAckToSend(),
        nullptr)
        << "the responder should not acknowledge a packet from a queue pair "
           "it is not connected to";
  }
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "the responder should deliver nothing from that packet";
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "the responder should leave the queue pair running";
  EXPECT_EQ(mockReceiver_->receiver().getSrcQpMismatchDiscardCount(), 1)
      << "the responder discards this without a reply, so the count is how a "
         "stale or misdirected peer sending here can be found";

  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          /*tag=*/0,
          /*posn=*/0,
          payload,
          kSize),
      kEcnECT0);
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "the same fsn from the connected queue pair should still be "
         "delivered, so the discarded packet consumed none of the flow's or "
         "the message's state";
  EXPECT_EQ(std::memcmp(recvBuffer, payload, kSize), 0)
      << "the responder should scatter the connected queue pair's data";
  EXPECT_EQ(mockReceiver_->receiver().getSrcQpMismatchDiscardCount(), 1)
      << "a packet from the connected queue pair should not be counted as "
         "discarded";
}

TEST_F(ReceiverTest, APacketWithANonzeroVersionIsDiscardedAndCounted) {
  constexpr uint32_t kSize = 64;
  char recvBuffer[kSize] = {};
  postReceiveWqe(recvBuffer, kSize);
  char payload[kSize];
  std::memset(payload, 'V', kSize);

  EXPECT_CALL(*mockSocket_, write(_, _)).Times(0);

  auto versionedPacket = buildTestDataPacket(
      srcQp, dstQp, flowId, fsn, msn, /*tag=*/0, /*posn=*/0, payload, kSize);
  versionedPacket->getBaseHeader().setVersion(1);
  mockReceiver_->simulateReceivePacket(std::move(versionedPacket), kEcnECT0);

  EXPECT_EQ(mockReceiver_->receiver().getUnsupportedVersionDiscardCount(), 1)
      << "A packet with a nonzero version must be counted";
  {
    auto lockedMap = qpNumToQp_.rlock();
    EXPECT_EQ(
        lockedMap->at(dstQp)->getRxFlowById(flowId)->getNextAckToSend(),
        nullptr)
        << "the responder should not acknowledge a packet of a version it "
           "does not implement";
  }
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "the responder should deliver nothing from that packet";
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "the responder should leave the queue pair running";

  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          /*tag=*/0,
          /*posn=*/0,
          payload,
          kSize),
      kEcnECT0);
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "the same fsn at version zero should still be delivered, so the "
         "discarded packet consumed none of the flow's or the message's state";
  EXPECT_EQ(mockReceiver_->receiver().getUnsupportedVersionDiscardCount(), 1)
      << "a version-zero packet should not be counted as discarded";
}

TEST_F(ReceiverTest, AReadOfARegionWithoutRemoteReadGetsAnAddressNak) {
  constexpr uint32_t kSize = 64;
  char region[kSize] = {};
  auto* mr = mrs.reg_mr(
      nullptr, region, kSize, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(mr, nullptr);

  expectNakAddressError();
  mockReceiver_->simulateReceivePacket(
      buildTestReadRequestPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          reinterpret_cast<uint64_t>(region),
          mr->rkey,
          kSize),
      kEcnECT0);
}

/*
 * Tests the NAK Address Error mechanism.
 */
TEST_F(ReceiverTest, SendNakAddressError) {
  uint64_t remoteAddr = 1; // bad
  uint32_t remoteKey = 1;
  char buffer[20] = {};
  auto dataPacket = buildTestWritePacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      remoteAddr,
      remoteKey,
      buffer,
      sizeof(buffer));
  const uint8_t sentTag = dataPacket->getDataHeader().getTag();

  // Expect a write call to the socket with an address error NAK packet
  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([&](const folly::SocketAddress& sockAddr,
                    const std::unique_ptr<folly::IOBuf>& buf) {
        // Parse the packet from the IOBuf
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);

        // Verify it's an ACK packet with the right opcode
        EXPECT_TRUE(baseHeader.getOpcode().isAnAck());

        // Verify the base header fields
        EXPECT_EQ(baseHeader.getSrcQp(), dstQp);
        EXPECT_EQ(baseHeader.getDstQp(), srcQp);
        EXPECT_EQ(baseHeader.getFlowId(), flowId);

        // Read the ACK header
        AckHeader ackHeader(cursor);

        // Verify the ACK header fields
        EXPECT_EQ(
            ackHeader.getAckStatus(),
            AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR);
        EXPECT_EQ(ackHeader.getCumulativeFsn(), fsn);
        EXPECT_EQ(ackHeader.getMaxRcvdFsn(), fsn);

        EXPECT_EQ(ackHeader.getCumulRespDeliveredMsn(), msn)
            << "spec 8.5.5: on a NAK this field carries the msn of the errored "
               "message rather than a cumulative value";
        EXPECT_EQ(ackHeader.getTag(), sentTag)
            << "spec 8.5.5: a NAK reporting an errored msn must carry that "
               "message's tag";

        // Confirm sent toward the metaroce port.
        EXPECT_EQ(sockAddr.getPort(), kMetaRoceSendToPort);
        EXPECT_EQ(sockAddr.describe(), "[2001:db8::1]:2766");

        return buf->computeChainDataLength();
      });

  // Simulate receiving the data packet
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);
}

/*
 * A request that fails validation puts its own tag in error. Its sends stay
 * alive until the sender marks them failing: the tx tracker holds raw pointers
 * to those requests, so freeing one here leaves the frame it is carrying
 * pointing to a dead work request.
 */
TEST_F(ReceiverTest, ValidationFailureLeavesTheTrackedSendWqeAlive) {
  auto* qp = qpNumToQp_.rlock()->at(dstQp).get();
  auto* txFlow = qp->getTxFlowById(flowId);
  ASSERT_NE(txFlow, nullptr);

  // A send from this tag is already on the wire.
  std::array<char, 4096> payload{};
  auto sendWqe = std::make_unique<SendWqe>(
      /*workReqId=*/7, oneSge(payload), qp->getMtu());
  Wqe* inFlightWqe = sendWqe.get();
  auto tag = qp->getTag(kMetaRoceInitialTag);
  postToTag(*tag, std::move(sendWqe), qp->getMtu());
  auto& txState = txFlow->getTxState();
  txState.tracker.emplace_back(
      PacketId(
          inFlightWqe,
          kMetaRoceInitialTag,
          inFlightWqe->allocateNextPosn(),
          qp->getMtu()),
      Flow::FsnState::UNACKED);
  txState.sndNxt = txState.sndUna + 1;

  // A write arrives for a memory region that does not exist.
  expectNakAddressError();
  mockReceiver_->simulateReceivePacket(
      buildTestWritePacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          /*remoteAddress=*/1,
          /*remoteKey=*/1,
          payload.data(),
          20),
      kEcnECT0);

  EXPECT_TRUE(tag->isInErrorState())
      << "a request that fails validation must put its own tag in error";
  // Ownership first: the dereference below is only safe while the tag still
  // holds the work request, so a failure here has to stop the test.
  ASSERT_EQ(tag->getNumPendingWqes(), 1)
      << "the tag must still own the send the tracker points to; freeing it "
         "here leaves that frame pointing to freed memory";
  EXPECT_EQ(txState.tracker.size(), 1u)
      << "the shared tx tracker must keep the frame it is carrying; clearing "
         "it strands that packet with no entry left to acknowledge it";
  EXPECT_FALSE(inFlightWqe->isFailing())
      << "an unrelated send is not this thread's to write: its posn state is "
         "the sender's, and this runs on the receive path";

  // Stand in for the sender's arbiter visit.
  std::ignore = qp->getNextPacketToSend();

  EXPECT_TRUE(inFlightWqe->isFailing())
      << "the sender marks it failing, rather than completing it, since its "
         "packet may still be on the wire";
  EXPECT_EQ(txState.tracker.size(), 1u)
      << "marking the work failing must not retire its frame; only an ack for "
         "that frame or the retransmission scan may";
}

/*
 * The retransmission timer can fire between the validation failure and the
 * sender's visit. The failure is driven through the receive path, so the scan
 * meets the work requests that path really left behind -- which is what
 * catches a receiver that freed the ones the tracker still points to.
 */
TEST_F(ReceiverTest, AnRtoBeforeTheSendersVisitAbandonsTheErroredTagsFrame) {
  auto* qp = qpNumToQp_.rlock()->at(dstQp).get();
  auto* txFlow = qp->getTxFlowById(flowId);
  ASSERT_NE(txFlow, nullptr);

  // A send from this tag is already on the wire.
  std::array<char, 4096> payload{};
  auto sendWqe = std::make_unique<SendWqe>(
      /*workReqId=*/7, oneSge(payload), qp->getMtu());
  Wqe* inFlightWqe = sendWqe.get();
  auto tag = qp->getTag(kMetaRoceInitialTag);
  postToTag(*tag, std::move(sendWqe), qp->getMtu());
  auto& txState = txFlow->getTxState();
  txState.tracker.emplace_back(
      PacketId(
          inFlightWqe,
          kMetaRoceInitialTag,
          inFlightWqe->allocateNextPosn(),
          qp->getMtu()),
      Flow::FsnState::UNACKED);
  txState.sndNxt = txState.sndUna + 1;
  const uint16_t sndUnaBefore = txState.sndUna;

  // A write arrives for a memory region that does not exist.
  expectNakAddressError();
  mockReceiver_->simulateReceivePacket(
      buildTestWritePacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          /*remoteAddress=*/1,
          /*remoteKey=*/1,
          payload.data(),
          20),
      kEcnECT0);
  ASSERT_TRUE(tag->isInErrorState())
      << "the failure has to have reached the tag for the window under test to "
         "exist at all";
  ASSERT_FALSE(inFlightWqe->isFailing())
      << "and the sender must not have visited yet, or the scan would be "
         "reading the work request's own state rather than the tag's";

  const auto retxCount = txFlow->prepareRetxPacketsIfAny(/*isRtoEvent=*/true);

  EXPECT_EQ(retxCount, 0)
      << "the tag's work is about to be failed, so its frame must not go "
         "back on the wire for a message the client is being told failed";
  EXPECT_EQ(txState.sndUna, sndUnaBefore + 1)
      << "and the frame has to retire off the head of the window, or the "
         "work request waits on an acknowledgement no one is going to send";
  EXPECT_TRUE(txState.tracker.empty())
      << "which leaves no entry behind it in the tracker";

  // The sender's visit follows.
  std::ignore = qp->getNextPacketToSend();

  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "with its last frame retired the work request must report, since "
         "failing it is only useful if the client hears about it";
}

/*
 * A write-with-immediate spans two packets, and the requestor describes them
 * inconsistently: the first arrives as a plain WRITE, which makes the responder
 * create a virtual receive for the message, and the second arrives as
 * WRITE_IMM_P, which reports on a receive the client posted. One message cannot
 * be both, and the second packet must not be applied to the WQE the first
 * created.
 */
TEST_F(ReceiverTest, AWriteThenWriteWithImmediateOnOneMsnIsRejected) {
  // Two MTUs, so the region covers the whole message the packets describe and
  // the responder has no address error to reject them for instead.
  constexpr uint32_t kRecvBufferSize = 2 * 4096;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  auto* mr = mrs.reg_mr(
      nullptr,
      recvBuffer,
      kRecvBufferSize,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(mr, nullptr)
      << "the write has to target a registered region, or the responder "
         "rejects it for an address error before reaching the check under test";

  // A full MTU: a packet that is not the message's last must carry one, or the
  // responder rejects it for its length before reaching the check under test.
  std::array<char, 4096> payload{};
  auto* qp = qpNumToQp_.rlock()->at(dstQp).get();
  auto tag = qp->getTag(kMetaRoceInitialTag);

  // The message's first packet, carrying the opcode of a plain write.
  mockReceiver_->simulateReceivePacket(
      buildTestWritePacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          reinterpret_cast<uint64_t>(recvBuffer),
          mr->rkey,
          payload.data(),
          payload.size(),
          BaseHeader::Opcode::OperationType::WRITE,
          /*posn=*/0,
          /*maybeImmData=*/std::nullopt,
          /*messageLength=*/2 * qp->getMtu()),
      kEcnECT0);
  ASSERT_FALSE(tag->isInErrorState())
      << "a plain write is well formed on its own, so the tag has to still be "
         "running for the second packet to be what fails it";

  // The second packet claims the same message is a write with immediate.
  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(
      buildTestWritePacket(
          srcQp,
          dstQp,
          flowId,
          fsn + 1,
          msn,
          reinterpret_cast<uint64_t>(recvBuffer),
          mr->rkey,
          payload.data(),
          payload.size(),
          BaseHeader::Opcode::OperationType::WRITE_IMM_P,
          /*posn=*/1,
          /*maybeImmData=*/0xABCD,
          /*messageLength=*/2 * qp->getMtu()),
      kEcnECT0);

  EXPECT_TRUE(tag->isInErrorState())
      << "the responder must reject a requestor that describes one message two "
         "ways, rather than applying the immediate to the receive the first "
         "packet invented";
}

/*
 * Tests that when a WRITE packet arrives with a message length that exceeds
 * the registered memory region, the receiver generates a
 * NAK_ADDRESS_INVALID_ACCESS_ERROR and transitions the QP to error state.
 */
TEST_F(ReceiverTest, SendNakAddressErrorForWritePosnOutOfRange) {
  // Register a memory region of 4096 bytes (one MTU).
  char regionBuffer[4096] = {};
  auto* mr = mrs.reg_mr(
      nullptr,
      regionBuffer,
      sizeof(regionBuffer),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(mr, nullptr);

  // Message length of 8192 requires 2 posns at 4096-byte MTU, but the
  // registered region is only 4096 bytes — posn 1 is out of range.
  constexpr int kMessageLength = 8192;
  std::vector<char> payload(kMessageLength, 0);
  auto dataPacket = buildTestWritePacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      (uint64_t)regionBuffer,
      mr->rkey,
      payload.data(),
      kMessageLength);

  expectNakAddressError();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state when a WRITE targets memory "
         "beyond the registered region";
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "flush should not produce spurious completions when no WQEs are pending";
}

/*
 * Tests that when a SEND packet arrives with a posn whose byte offset exceeds
 * the posted receive WQE's buffer, the receiver generates a
 * NAK_INVALID_REQUEST (spec 5.5) and transitions the QP to error state.
 */
TEST_F(ReceiverTest, SendNakInvalidRequestForSendPosnOutOfRange) {
  constexpr uint32_t kRecvBufferSize = 100;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  // Posn 1 at MTU 4096 starts at byte offset 4096, far beyond 100 bytes.
  constexpr uint16_t kOutOfRangePosn = 1;
  auto dataPacket = buildTestDataPacket(
      srcQp, dstQp, flowId, fsn, msn, /*tag=*/0, kOutOfRangePosn);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state when a SEND posn exceeds "
         "the receive WQE buffer";
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "the pending receive WQE should be flushed on length error";
  auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.status, kWrFlushErr)
      << "flushed WQE should complete with flush error status";
  EXPECT_EQ(cqe.workReqId, 42)
      << "flushed completion should correspond to the posted receive WQE";
}

/*
 * Tests that when a SEND packet carries a payload larger than the posted
 * receive WQE buffer, the receiver generates a NAK_INVALID_REQUEST (spec 5.5)
 * and transitions the QP to error state.
 */
TEST_F(ReceiverTest, SendNakInvalidRequestForOversizedSendPayload) {
  constexpr uint32_t kRecvBufferSize = 100;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  // 200-byte payload at posn 0 overflows the 100-byte receive buffer.
  constexpr uint32_t kPayloadSize = 200;
  char payload[kPayloadSize] = {};
  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kPayloadSize);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state when a SEND payload exceeds "
         "the receive WQE buffer";
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "the pending receive WQE should be flushed on length error";
  auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.status, kWrFlushErr)
      << "flushed WQE should complete with flush error status";
  EXPECT_EQ(cqe.workReqId, 42)
      << "flushed completion should correspond to the posted receive WQE";
}

/*
 * Tests that when a SEND packet's last POSN carries fewer payload bytes than
 * the receive WQE expects, the receiver generates a NAK_INVALID_REQUEST
 * (spec 5.5).  This catches senders whose message size differs from the
 * receiver's posted buffer — the last POSN's payload must be exactly
 * messageSizeBytes - lastPosn * mtu.
 */
TEST_F(ReceiverTest, SendNakInvalidRequestForUndersizedLastPosnPayload) {
  // 5000-byte recv buffer at MTU 4096 → 2 POSNs:
  //   POSN 0: expects 4096 bytes
  //   POSN 1: expects 5000 - 4096 = 904 bytes
  constexpr uint32_t kRecvBufferSize = 5000;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  // Send POSN 0 with the correct MTU-sized payload — should succeed silently.
  constexpr uint32_t kMtuPayload = 4096;
  char posn0Payload[kMtuPayload] = {};
  auto posn0Packet = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      posn0Payload,
      kMtuPayload);
  mockReceiver_->simulateReceivePacket(std::move(posn0Packet), kEcnECT0);
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "correctly-sized POSN 0 should not trigger an error";

  // Send POSN 1 with 900 bytes instead of the expected 904.
  constexpr uint32_t kUndersizedPayload = 900;
  char posn1Payload[kUndersizedPayload] = {};
  auto posn1Packet = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      /*fsn=*/fsn + 1,
      msn,
      /*tag=*/0,
      /*posn=*/1,
      posn1Payload,
      kUndersizedPayload);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(posn1Packet), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state when the last POSN payload is "
         "smaller than the WQE expects";
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "the pending receive WQE should be flushed on length error";
  auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.status, kWrFlushErr)
      << "flushed WQE should complete with flush error status";
  EXPECT_EQ(cqe.workReqId, 42)
      << "flushed completion should correspond to the posted receive WQE";
}

TEST_F(ReceiverTest, SendImmCompletesBeforeReceiveWqeIsFull) {
  constexpr uint32_t kRecvBufferSize = 4096;
  constexpr uint32_t kPayloadSize = 128;
  constexpr uint32_t kImmData = 0x12345678;
  char recvBuffer[kRecvBufferSize] = {};
  char payload[kPayloadSize];
  std::memset(payload, 'S', sizeof(payload));
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kPayloadSize,
      BaseHeader::Opcode::OperationType::SEND_IMM_P,
      kImmData);
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "a terminal send shorter than the receive buffer should be accepted";
  EXPECT_EQ(std::memcmp(recvBuffer, payload, kPayloadSize), 0)
      << "the receiver should deliver the short send payload";
  ASSERT_EQ(cq->getNumCompletedWqes(), 1)
      << "SEND_IMM_P should complete the receive WQE at its final POSN";
  const auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.status, kSuccess)
      << "the short SEND_IMM_P should complete successfully";
  EXPECT_EQ(cqe.sizeBytes, kPayloadSize)
      << "the completion should report delivered bytes, not receive capacity";
  EXPECT_EQ(cqe.maybeImmData, kImmData)
      << "the receive completion should carry SEND_IMM_P immediate data";
  EXPECT_EQ(cqe.opcode, IBV_WC_RECV)
      << "send with immediate should retain the receive completion opcode";
}

TEST_F(ReceiverTest, CompletionSizeRespectsDisclosedPadding) {
  constexpr uint32_t kRecvBufferSize = 4;
  constexpr uint32_t kPayloadSize = 1;
  constexpr uint8_t kPadding = 3;
  constexpr uint32_t kImmData = 0x12345678;
  char recvBuffer[kRecvBufferSize] = {};
  char payload[kPayloadSize] = {'S'};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  auto wirePacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kPayloadSize,
      BaseHeader::Opcode::OperationType::SEND_IMM_P,
      kImmData,
      kPadding);
  folly::IOBufQueue wireBytes{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&wireBytes, wirePacket->getSizeBytes());
  wirePacket->serialize(appender);
  auto buffer = wireBytes.move();
  folly::io::Cursor cursor(buffer.get());
  const PayloadOnlyEncapsulation encapsulation;
  const MetaRoceCodec codec;

  auto decodedPacket = codec.parse(cursor, encapsulation);
  ASSERT_NE(decodedPacket, nullptr);
  mockReceiver_->simulateReceivePacket(std::move(decodedPacket), kEcnECT0);

  EXPECT_EQ(std::memcmp(recvBuffer, payload, kPayloadSize), 0)
      << "the receiver should deliver only the application payload";
  ASSERT_EQ(cq->getNumCompletedWqes(), 1)
      << "the terminal packet should complete the posted receive WQE";
  const auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.status, kSuccess)
      << "the padded send should complete successfully";
  EXPECT_EQ(cqe.sizeBytes, kPayloadSize)
      << "disclosed padding should not inflate the completion size";
  EXPECT_EQ(cqe.maybeImmData, kImmData)
      << "decoding padding should preserve the immediate data";
  EXPECT_EQ(cqe.opcode, IBV_WC_RECV)
      << "the padded send should produce a receive completion";
}

TEST_F(ReceiverTest, SendLastCompletesBeforeReceiveWqeIsFull) {
  constexpr uint32_t kRecvBufferSize = 4096;
  constexpr uint32_t kPayloadSize = 128;
  char recvBuffer[kRecvBufferSize] = {};
  char payload[kPayloadSize];
  std::memset(payload, 'S', sizeof(payload));
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kPayloadSize,
      BaseHeader::Opcode::OperationType::SEND_LAST);
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "a terminal send shorter than the receive buffer should be accepted";
  EXPECT_EQ(std::memcmp(recvBuffer, payload, kPayloadSize), 0)
      << "the receiver should deliver the short send payload";
  ASSERT_EQ(cq->getNumCompletedWqes(), 1)
      << "SEND_LAST should complete the receive WQE at its final POSN";
  const auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.status, kSuccess)
      << "the short SEND_LAST should complete successfully";
  EXPECT_EQ(cqe.sizeBytes, kPayloadSize)
      << "the completion should report delivered bytes, not receive capacity";
  EXPECT_FALSE(cqe.maybeImmData.has_value())
      << "SEND_LAST should not add immediate data to the completion";
  EXPECT_EQ(cqe.opcode, IBV_WC_RECV)
      << "SEND_LAST should produce an ordinary receive completion";
}

TEST_F(ReceiverTest, OutOfOrderSendImmWaitsForPrecedingPosns) {
  constexpr uint32_t kMtu = 4096;
  constexpr uint32_t kFinalPayloadSize = 128;
  constexpr uint32_t kRecvBufferSize = 3 * kMtu;
  constexpr uint32_t kDeliveredSize = kMtu + kFinalPayloadSize;
  constexpr uint32_t kImmData = 0x87654321;
  char recvBuffer[kRecvBufferSize] = {};
  char firstPayload[kMtu];
  char finalPayload[kFinalPayloadSize];
  std::memset(firstPayload, 'A', sizeof(firstPayload));
  std::memset(finalPayload, 'B', sizeof(finalPayload));
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  auto finalPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/1,
      finalPayload,
      kFinalPayloadSize,
      BaseHeader::Opcode::OperationType::SEND_IMM_P,
      kImmData);
  mockReceiver_->simulateReceivePacket(std::move(finalPacket), kEcnECT0);

  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "the final marker should wait for every preceding POSN";

  auto duplicateFinalPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/1,
      finalPayload,
      kFinalPayloadSize,
      BaseHeader::Opcode::OperationType::SEND_IMM_P,
      kImmData);
  mockReceiver_->simulateReceivePacket(
      std::move(duplicateFinalPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "an exact FSN retransmission of the final packet should be ignored";
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "a duplicate final packet should not complete the missing POSN";

  auto firstPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      /*fsn=*/fsn + 1,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      firstPayload,
      kMtu);
  mockReceiver_->simulateReceivePacket(std::move(firstPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "an out-of-order final marker should leave the QP operational";
  EXPECT_EQ(std::memcmp(recvBuffer, firstPayload, kMtu), 0)
      << "the receiver should deliver the preceding POSN";
  EXPECT_EQ(std::memcmp(recvBuffer + kMtu, finalPayload, kFinalPayloadSize), 0)
      << "the receiver should preserve the previously delivered final POSN";
  ASSERT_EQ(cq->getNumCompletedWqes(), 1)
      << "the receive should complete once every POSN through the marker arrives";
  const auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.status, kSuccess)
      << "the out-of-order short send should complete successfully";
  EXPECT_EQ(cqe.sizeBytes, kDeliveredSize)
      << "the completion should report the marker-defined message length";
  EXPECT_EQ(cqe.maybeImmData, kImmData)
      << "the completion should retain the final packet's immediate data";
}

TEST_F(ReceiverTest, OutOfOrderSendLastWaitsForPrecedingPosns) {
  constexpr uint32_t kMtu = 4096;
  constexpr uint32_t kFinalPayloadSize = 128;
  constexpr uint32_t kRecvBufferSize = 3 * kMtu;
  constexpr uint32_t kDeliveredSize = kMtu + kFinalPayloadSize;
  char recvBuffer[kRecvBufferSize] = {};
  char firstPayload[kMtu];
  char finalPayload[kFinalPayloadSize];
  std::memset(firstPayload, 'A', sizeof(firstPayload));
  std::memset(finalPayload, 'B', sizeof(finalPayload));
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  auto finalPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/1,
      finalPayload,
      kFinalPayloadSize,
      BaseHeader::Opcode::OperationType::SEND_LAST);
  mockReceiver_->simulateReceivePacket(std::move(finalPacket), kEcnECT0);

  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "the final marker should wait for every preceding POSN";

  auto firstPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      /*fsn=*/fsn + 1,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      firstPayload,
      kMtu);
  mockReceiver_->simulateReceivePacket(std::move(firstPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "an out-of-order final marker should leave the QP operational";
  EXPECT_EQ(std::memcmp(recvBuffer, firstPayload, kMtu), 0)
      << "the receiver should deliver the preceding POSN";
  EXPECT_EQ(std::memcmp(recvBuffer + kMtu, finalPayload, kFinalPayloadSize), 0)
      << "the receiver should preserve the previously delivered final POSN";
  ASSERT_EQ(cq->getNumCompletedWqes(), 1)
      << "the receive should complete once every POSN through the marker arrives";
  const auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.status, kSuccess)
      << "the out-of-order short send should complete successfully";
  EXPECT_EQ(cqe.sizeBytes, kDeliveredSize)
      << "the completion should report the marker-defined message length";
  EXPECT_FALSE(cqe.maybeImmData.has_value())
      << "SEND_LAST should not add immediate data to the completion";
}

/*
 * Tests that when an atomic request arrives with a misaligned target address
 * (not 8-byte aligned), the receiver generates NAK_INVALID_REQUEST per
 * spec 5.5 ("Invalid Atomic Request") and transitions the QP to error state.
 */
TEST_F(ReceiverTest, SendNakInvalidRequestForMisalignedAtomic) {
  // Address 1 is not 8-byte aligned.
  constexpr uint64_t kMisalignedAddr = 1;
  constexpr uint32_t kAnyRkey = 42;
  auto dataPacket = buildTestAtomicPacket(
      srcQp, dstQp, flowId, fsn, msn, kMisalignedAddr, kAnyRkey);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state when an atomic target is misaligned";
}

/*
 * Tests that when an atomic request arrives with a valid (8-byte aligned)
 * target address but an invalid rkey, the receiver generates
 * NAK_ADDRESS_INVALID_ACCESS_ERROR (MR validation failure).
 */
TEST_F(ReceiverTest, SendNakAddressErrorForAtomicBadRkey) {
  // Address 8 is aligned, but rkey 0xBAD won't match any registered MR.
  constexpr uint64_t kAlignedAddr = 8;
  constexpr uint32_t kBadRkey = 0xBAD;
  auto dataPacket = buildTestAtomicPacket(
      srcQp, dstQp, flowId, fsn, msn, kAlignedAddr, kBadRkey);

  expectNakAddressError();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state when an atomic rkey is invalid";
}

/*
 * An atomic reaches the responder's memory by rkey, so the region must carry
 * IBV_ACCESS_REMOTE_ATOMIC. This is the case the permission check exists for:
 * the region is registered and the address aligned, so nothing but the missing
 * permission stands between the request and the responder's memory.
 */
TEST_F(ReceiverTest, SendNakAddressErrorForAtomicOnRegionWithoutRemoteAtomic) {
  char regionBuffer[8] __attribute__((aligned(8))) = {};
  auto* mr = mrs.reg_mr(
      nullptr,
      regionBuffer,
      sizeof(regionBuffer),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(mr, nullptr);

  auto dataPacket = buildTestAtomicPacket(
      srcQp, dstQp, flowId, fsn, msn, (uint64_t)regionBuffer, mr->rkey);

  expectNakAddressError();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "an atomic on a region that never granted remote atomic must be "
         "refused, not executed against the client's memory";
}

/*
 * A region that grants remote atomic has its fetch-add executed against it.
 */
TEST_F(ReceiverTest, AnAtomicOnARegionThatGrantsItIsExecuted) {
  uint64_t regionValue = 0;
  auto* mr = mrs.reg_mr(
      nullptr,
      &regionValue,
      sizeof(regionValue),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  ASSERT_NE(mr, nullptr);

  // The ack the responder sends back is not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  mockReceiver_->simulateReceivePacket(
      buildTestAtomicPacket(
          srcQp, dstQp, flowId, fsn, msn, (uint64_t)&regionValue, mr->rkey),
      kEcnECT0);

  EXPECT_EQ(regionValue, kTestAtomicOperand)
      << "the responder must add the operand to the client's memory; leaving "
         "it unchanged means the atomic was turned away or dropped";
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "the region granted remote atomic, so the responder must serve the "
         "request rather than error the queue pair";
}

// The atomic arrives on the next fsn, so the flow delivers it and the missing
// earlier msn is the only thing holding it.
TEST_F(ReceiverTest, AnAtomicWaitsForEveryEarlierMsnToBeDelivered) {
  uint64_t regionValue = 0;
  auto* mr = mrs.reg_mr(
      nullptr,
      &regionValue,
      sizeof(regionValue),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  ASSERT_NE(mr, nullptr);

  constexpr uint32_t kRecvBufferSize = 64;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  // The ack the responder sends back is not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  {
    auto lockedMap = qpNumToQp_.rlock();
    const uint16_t cdmsn =
        lockedMap->at(dstQp)->getTag(kMetaRoceInitialTag)->getCDMSN();
    ASSERT_EQ(static_cast<uint16_t>(cdmsn + 1), msn)
        << "precondition: the responder is waiting for msn " << msn
        << ", so an atomic at the msn after it has exactly one earlier message "
           "to wait on";
  }

  mockReceiver_->simulateReceivePacket(
      buildTestAtomicPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          static_cast<uint16_t>(msn + 1),
          (uint64_t)&regionValue,
          mr->rkey),
      kEcnECT0);

  EXPECT_EQ(regionValue, 0u)
      << "the responder must hold the atomic while an earlier msn is still "
         "undelivered; touching the client's memory now applies it out of the "
         "order the requester issued it";

  // The message the atomic was waiting on, which releases it.
  char payload[kRecvBufferSize] = {};
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          msn,
          /*tag=*/0,
          /*posn=*/0,
          payload,
          kRecvBufferSize),
      kEcnECT0);

  EXPECT_EQ(regionValue, kTestAtomicOperand)
      << "delivering the earlier msn should carry the responder through to the "
         "atomic and run it, now that every msn before it has arrived";
}

TEST_F(ReceiverTest, AReadHoldsNoReceiveCompletionBehindIt) {
  constexpr uint32_t kSize = 64;
  char readSource[kSize] = {};
  auto* readMr = mrs.reg_mr(nullptr, readSource, kSize, IBV_ACCESS_REMOTE_READ);
  ASSERT_NE(readMr, nullptr);
  char recvBuffer[kSize] = {};
  postReceiveWqe(recvBuffer, kSize);

  // The acks the responder sends back are not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  mockReceiver_->simulateReceivePacket(
      buildTestReadRequestPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          reinterpret_cast<uint64_t>(readSource),
          readMr->rkey,
          kSize),
      kEcnECT0);
  char payload[kSize] = {};
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          static_cast<uint16_t>(msn + 1),
          /*tag=*/0,
          /*posn=*/0,
          payload,
          kSize),
      kEcnECT0);

  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "the responder should complete the send as soon as it is delivered: "
         "the read ahead of it completed when its request arrived, and the "
         "response still to be sent holds no completion back";
}

TEST_F(ReceiverTest, AReceiveArrivingAheadOfAnEarlierReadWaitsForItsRequest) {
  constexpr uint32_t kSize = 64;
  char readSource[kSize] = {};
  auto* readMr = mrs.reg_mr(nullptr, readSource, kSize, IBV_ACCESS_REMOTE_READ);
  ASSERT_NE(readMr, nullptr);
  char recvBuffer[kSize] = {};
  postReceiveWqe(recvBuffer, kSize);

  // The acks the responder sends back are not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  char payload[kSize] = {};
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          static_cast<uint16_t>(msn + 1),
          /*tag=*/0,
          /*posn=*/0,
          payload,
          kSize,
          BaseHeader::Opcode::OperationType::SEND,
          /*maybeImmData=*/std::nullopt,
          /*padding=*/0,
          kFixtureRxCsn,
          /*una=*/fsn),
      kEcnECT0);
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "the responder should hold the send while the read the requester "
         "issued before it has not arrived";

  mockReceiver_->simulateReceivePacket(
      buildTestReadRequestPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          reinterpret_cast<uint64_t>(readSource),
          readMr->rkey,
          kSize),
      kEcnECT0);
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "the read's request completes the read, which should release the "
         "send waiting behind it";
}

TEST_F(ReceiverTest, AtomicsExecuteInTheOrderTheRequesterIssuedThem) {
  uint64_t regionValue = 0;
  auto* mr = mrs.reg_mr(
      nullptr,
      &regionValue,
      sizeof(regionValue),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  ASSERT_NE(mr, nullptr);

  // The acks and atomic responses are not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  // Issued second, arriving first.
  mockReceiver_->simulateReceivePacket(
      buildTestAtomicPacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          static_cast<uint16_t>(msn + 1),
          reinterpret_cast<uint64_t>(&regionValue),
          mr->rkey,
          BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD,
          /*posn=*/0,
          /*una=*/fsn),
      kEcnECT0);
  // Issued first: swaps in the operand only where the word still holds zero.
  mockReceiver_->simulateReceivePacket(
      buildTestAtomicPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          reinterpret_cast<uint64_t>(&regionValue),
          mr->rkey,
          BaseHeader::Opcode::OperationType::ATOMIC_CMP_SWP),
      kEcnECT0);

  EXPECT_EQ(regionValue, 2 * kTestAtomicOperand)
      << "the responder should swap the zero for the operand and then add the "
         "operand to it; a word holding the operand alone means the fetch-add "
         "ran first, in arrival order, and the swap then found no zero";
}

TEST_F(ReceiverTest, AWriteBehindAHeldAtomicIsDeliveredAsItArrives) {
  uint64_t atomicTarget = 0;
  auto* atomicMr = mrs.reg_mr(
      nullptr,
      &atomicTarget,
      sizeof(atomicTarget),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  ASSERT_NE(atomicMr, nullptr);
  constexpr uint32_t kWriteSize = 64;
  char writeTarget[kWriteSize] = {};
  auto* writeMr = mrs.reg_mr(
      nullptr,
      writeTarget,
      kWriteSize,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(writeMr, nullptr);

  // The acks the responder sends back are not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  // msn itself never arrives, so the atomic after it stays held.
  mockReceiver_->simulateReceivePacket(
      buildTestAtomicPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          static_cast<uint16_t>(msn + 1),
          reinterpret_cast<uint64_t>(&atomicTarget),
          atomicMr->rkey),
      kEcnECT0);
  char payload[kWriteSize];
  std::memset(payload, 'W', kWriteSize);
  mockReceiver_->simulateReceivePacket(
      buildTestWritePacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          static_cast<uint16_t>(msn + 2),
          reinterpret_cast<uint64_t>(writeTarget),
          writeMr->rkey,
          payload,
          kWriteSize),
      kEcnECT0);

  ASSERT_EQ(atomicTarget, 0u)
      << "precondition: the atomic is still held behind the undelivered msn, "
         "or the write did not arrive behind a held atomic";
  EXPECT_EQ(std::memcmp(writeTarget, payload, kWriteSize), 0)
      << "the responder should write the data into memory as it arrives rather "
         "than hold it until the atomic ahead of it executes";
}

TEST_F(ReceiverTest, PipelinedSendsLandInTheReceivesTheirCsnsMatch) {
  constexpr uint32_t kSize = 64;
  constexpr uint64_t kFirstPosted = 501;
  constexpr uint64_t kSecondPosted = 502;
  char firstBuffer[kSize] = {};
  char secondBuffer[kSize] = {};
  postReceiveWqe(firstBuffer, kSize, kFirstPosted);
  postReceiveWqe(secondBuffer, kSize, kSecondPosted);

  // The acks the responder sends back are not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  char secondPayload[kSize];
  std::memset(secondPayload, 'B', kSize);
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          static_cast<uint16_t>(msn + 1),
          /*tag=*/0,
          /*posn=*/0,
          secondPayload,
          kSize,
          BaseHeader::Opcode::OperationType::SEND,
          /*maybeImmData=*/std::nullopt,
          /*padding=*/0,
          static_cast<uint16_t>(kFixtureRxCsn + 1),
          /*una=*/fsn),
      kEcnECT0);
  EXPECT_EQ(std::memcmp(secondBuffer, secondPayload, kSize), 0)
      << "the responder should scatter the second message into the receive "
         "posted second, which its csn matches, even though it arrived first";
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "the responder should hold the second message's completion until "
         "the first message completes";

  char firstPayload[kSize];
  std::memset(firstPayload, 'A', kSize);
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          /*tag=*/0,
          /*posn=*/0,
          firstPayload,
          kSize),
      kEcnECT0);
  EXPECT_EQ(std::memcmp(firstBuffer, firstPayload, kSize), 0)
      << "the responder should scatter the first message into the receive "
         "posted first";

  ASSERT_EQ(cq->getNumCompletedWqes(), 2)
      << "the first message's arrival should complete it and release the "
         "second";
  const auto firstCqe = folly::coro::blockingWait(cq->pollCq());
  const auto secondCqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(firstCqe.workReqId, kFirstPosted)
      << "the message sent first should complete first, on the receive posted "
         "first";
  EXPECT_EQ(secondCqe.workReqId, kSecondPosted)
      << "the message sent second should complete behind it, on the receive "
         "posted second";
}

TEST_F(ReceiverTest, APacketOfAMessageAlreadyAckedIsNotRefused) {
  constexpr uint32_t kPacketSize = 4096;
  constexpr uint32_t kRecvBufferSize = 2 * kPacketSize;
  std::vector<char> recvBuffer(kRecvBufferSize);
  postReceiveWqe(recvBuffer.data(), kRecvBufferSize);
  std::vector<char> payload(kPacketSize, 'P');

  // The acks are read back below.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  auto ackStatusAfter = [this](std::unique_ptr<Packet> packet) {
    mockReceiver_->simulateReceivePacket(std::move(packet), kEcnECT0);
    auto lockedMap = qpNumToQp_.rlock();
    auto egressPacket =
        lockedMap->at(dstQp)->getRxFlowById(flowId)->getNextAckToSend();
    EXPECT_NE(egressPacket, nullptr)
        << "the responder should answer every packet of the message";
    auto* ackPacket = egressPacket == nullptr
        ? nullptr
        : dynamic_cast<AckPacket*>(egressPacket->packet.get());
    EXPECT_NE(ackPacket, nullptr)
        << "the responder's answer should be an acknowledgment";
    return ackPacket == nullptr ? AckStatus::RESERVED
                                : ackPacket->getAckHeader().getAckStatus();
  };

  ASSERT_EQ(
      ackStatusAfter(buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          /*tag=*/0,
          /*posn=*/0,
          payload.data(),
          kPacketSize)),
      AckStatus::ACK)
      << "precondition: the message's first packet finds its receive and is "
         "acked";
  EXPECT_EQ(
      ackStatusAfter(buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          msn,
          /*tag=*/0,
          /*posn=*/1,
          payload.data(),
          kPacketSize)),
      AckStatus::ACK)
      << "the responder should ack the message's second packet: the receive "
         "its first packet found stays matched, so there is nothing to refuse";
}

TEST_F(ReceiverTest, AMessageMissingOnOneTagHoldsNoCompletionOnAnother) {
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  {
    auto lockedMap = qpNumToQp_.rlock();
    auto* qp = lockedMap->at(dstQp).get();
    // A tag joins in RESET and is brought up afterward.
    qp->addTag(kOtherTag, cq.get(), &secondTagIbvQpStorage);
    ASSERT_TRUE(qp->getTag(kOtherTag)->setReceiveSequenceNumbers(
        {.nextMsn = kFixtureCdmsn + 1, .nextCsn = kFixtureRxCsn}))
        << "precondition: the second tag is numbered like the first, so both "
           "expect the same msn next";
    secondTagIbvQpStorage.state = IBV_QPS_RTS;
  }
  constexpr uint32_t kSize = 64;
  char writeTarget[kSize] = {};
  auto* writeMr = mrs.reg_mr(
      nullptr,
      writeTarget,
      kSize,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(writeMr, nullptr);
  constexpr uint64_t kOtherTagReceiveId = 77;
  char recvBuffer[kSize] = {};
  postReceiveWqe(recvBuffer, kSize, kOtherTagReceiveId, kOtherTag);

  // The acks the responder sends back are not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  // The initial tag receives the message after msn but never msn itself.
  char writePayload[kSize] = {};
  mockReceiver_->simulateReceivePacket(
      buildTestWritePacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          static_cast<uint16_t>(msn + 1),
          reinterpret_cast<uint64_t>(writeTarget),
          writeMr->rkey,
          writePayload,
          kSize),
      kEcnECT0);
  char sendPayload[kSize];
  std::memset(sendPayload, 'T', kSize);
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          msn,
          kOtherTag,
          /*posn=*/0,
          sendPayload,
          kSize),
      kEcnECT0);

  ASSERT_EQ(cq->getNumCompletedWqes(), 1)
      << "the responder should complete the other tag's send at once; the "
         "same msn missing on the initial tag is that tag's alone to wait on";
  const auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.tag, kOtherTag)
      << "the completion should be the other tag's, not one the initial tag "
         "reported despite the msn it is missing";
  EXPECT_EQ(cqe.workReqId, kOtherTagReceiveId)
      << "the completion should report the receive posted on the other tag";
  EXPECT_EQ(std::memcmp(recvBuffer, sendPayload, kSize), 0)
      << "the responder should have scattered the send into the other tag's "
         "receive";
}

/*
 * Tests that an atomic request with nonzero POSN is rejected with
 * NAK_INVALID_REQUEST per spec 5.5.  Atomic operations are always
 * single-POSN (8 bytes); a nonzero POSN indicates a malformed request.
 */
TEST_F(ReceiverTest, SendNakInvalidRequestForAtomicNonzeroPosn) {
  char regionBuffer[8] __attribute__((aligned(8))) = {};
  auto* mr = mrs.reg_mr(
      nullptr,
      regionBuffer,
      sizeof(regionBuffer),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(mr, nullptr);
  auto dataPacket = buildTestAtomicPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      (uint64_t)regionBuffer,
      mr->rkey,
      BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD,
      /*posn=*/1);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state for atomic with nonzero POSN";
}

/*
 * Tests that when the QP is in ERR state, data packets are rejected with
 * NAK_OPERATIONAL_ERROR.
 */
TEST_F(ReceiverTest, RejectDataForQpInErrorState) {
  qpNumToQp_.rlock()->at(dstQp)->setIbvQpState(IBV_QPS_ERR);
  auto dataPacket = buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn);

  expectNakOperationalError();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);
}

TEST_F(ReceiverTest, AQpInErrorStateSendsOneNakPerInterval) {
  qpNumToQp_.rlock()->at(dstQp)->setIbvQpState(IBV_QPS_ERR);
  std::vector<AckStatus> replies;
  EXPECT_CALL(*mockSocket_, write(_, _))
      .Times(AnyNumber())
      .WillRepeatedly([&replies](
                          const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);
        AckHeader ackHeader(cursor);
        replies.push_back(ackHeader.getAckStatus());
        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn), kEcnECT0);
  ASSERT_EQ(replies, std::vector<AckStatus>{AckStatus::NAK_OPERATIONAL_ERROR})
      << "precondition: the first packet to the errored QP draws its NAK";

  // Hold the interval open however long the first packet took.
  qpNumToQp_.rlock()
      ->at(dstQp)
      ->getTag(kMetaRoceInitialTag)
      ->holdErrorNaks(std::numeric_limits<uint64_t>::max() / 2);
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(srcQp, dstQp, flowId, fsn + 1, msn + 1), kEcnECT0);
  EXPECT_EQ(replies.size(), 1)
      << "a packet within the interval should draw no second reply";
  EXPECT_EQ(
      qpNumToQp_.rlock()->at(dstQp)->getRxFlowById(flowId)->getNextAckToSend(),
      nullptr)
      << "the discarded packet should leave no ordinary ack queued for the "
         "sender to carry";
}

/*
 * A frame that was already received and acked is retransmitted because the
 * peer lost the ack, not because it needs delivering again. The tag entering
 * error in the meantime does not change what happened to that frame, and
 * re-acking it says only what was already said. Rejecting it would call a
 * request invalid that this side accepted and delivered.
 */
TEST_F(ReceiverTest, AnAlreadyAckedFrameIsReackedAfterItsTagEntersError) {
  // A zero-length receive WQE consumes the zero-payload SEND the helper builds.
  postReceiveWqe(/*buffer=*/nullptr, /*bufferSize=*/0);

  std::vector<AckStatus> replies;
  EXPECT_CALL(*mockSocket_, write(_, _))
      .Times(AnyNumber())
      .WillRepeatedly([&replies](
                          const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);
        AckHeader ackHeader(cursor);
        replies.push_back(ackHeader.getAckStatus());
        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn), kEcnECT0);
  ASSERT_EQ(cq->getNumCompletedWqes(), 1)
      << "the first arrival must be delivered, consuming the posted receive "
         "WQE, which is the precondition this test is about";
  replies.clear();

  {
    auto lockedMap = qpNumToQp_.rlock();
    lockedMap->at(dstQp)->getTag(kMetaRoceInitialTag)->getIbvQp().state =
        IBV_QPS_ERR;
  }

  // The same frame again: the peer lost the ack, so it asks for it back.
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn), kEcnECT0);
  // Only a fatal NAK is written to the socket here; an ordinary ack is left in
  // the coalescing slot for the sender to carry.
  EXPECT_EQ(
      std::count(
          replies.begin(), replies.end(), AckStatus::NAK_OPERATIONAL_ERROR),
      0)
      << "the frame was received and acked before the tag entered error, so "
         "the retransmission must not be refused as a request to the errored "
         "tag";
  {
    auto lockedMap = qpNumToQp_.rlock();
    EXPECT_NE(
        lockedMap->at(dstQp)->getRxFlowById(flowId)->getNextAckToSend(),
        nullptr)
        << "and it must still be answered: the peer retransmitted because it "
           "lost the ack, so staying silent leaves it retransmitting";
  }
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "and it must not be delivered a second time";
}

/*
 * A tag enters error on its own, so data addressed to a healthy tag has to
 * keep being accepted rather than being rejected for its neighbor's error.
 */
TEST_F(ReceiverTest, DataForAHealthyTagIsAcceptedWhileAnotherTagIsInError) {
  constexpr uint8_t kHealthyTag = kMetaRoceInitialTag + 1;
  {
    auto lockedMap = qpNumToQp_.rlock();
    auto* qp = lockedMap->at(dstQp).get();
    // A tag joins in RESET and is brought up afterwards.
    qp->addTag(kHealthyTag, cq.get(), &secondTagIbvQpStorage);
    secondTagIbvQpStorage.state = IBV_QPS_RTS;
    qp->getTag(kMetaRoceInitialTag)->getIbvQp().state = IBV_QPS_ERR;
  }

  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn, kHealthyTag),
      kEcnECT0);

  // The healthy tag has no receive WQE posted, so this is BRNR'd rather than
  // delivered. Either outcome is the tag's own business; what must not happen
  // is the rejection an errored tag's packets get.
  auto lockedMap = qpNumToQp_.rlock();
  auto egressPacket =
      lockedMap->at(dstQp)->getRxFlowById(flowId)->getNextAckToSend();
  ASSERT_NE(egressPacket, nullptr)
      << "precondition: the healthy tag's data must be answered at all, or "
         "there is no verdict here to read";
  auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
  ASSERT_NE(ackPacket, nullptr)
      << "precondition: the answer carries the status this test reads";
  EXPECT_NE(
      ackPacket->getAckHeader().getAckStatus(),
      AckStatus::NAK_OPERATIONAL_ERROR)
      << "a healthy tag's data must not be rejected because another tag on "
         "the same qp has failed";
}

/*
 * Tests that a plain WRITE packet with zero-byte payload is rejected with
 * NAK_INVALID_REQUEST. Per spec 8.2.3 table "Zero-byte opcodes", plain
 * WRITE is forbidden at zero length (no semantic meaning without payload).
 */
TEST_F(ReceiverTest, SendNakInvalidRequestForZeroPayloadWrite) {
  char regionBuffer[4096] = {};
  auto* mr = mrs.reg_mr(
      nullptr,
      regionBuffer,
      sizeof(regionBuffer),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(mr, nullptr);
  auto dataPacket = buildTestWritePacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      (uint64_t)regionBuffer,
      mr->rkey,
      nullptr,
      0);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state after zero-payload WRITE";
}

TEST_F(ReceiverTest, ASendOnASkippedCsnFailsTheTag) {
  // Sized to the payload below, so the message fills the receive and the
  // responder cannot reject it for its length instead.
  constexpr uint32_t kRecvBufferSize = 20;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);
  postReceiveWqe(recvBuffer, kRecvBufferSize);
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  std::array<char, kRecvBufferSize> payload{};
  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          msn,
          /*tag=*/0,
          /*posn=*/0,
          payload.data(),
          payload.size(),
          BaseHeader::Opcode::OperationType::SEND,
          /*maybeImmData=*/std::nullopt,
          /*padding=*/0,
          /*csn=*/kFixtureRxCsn + 2),
      kEcnECT0);

  auto lockedMap = qpNumToQp_.rlock();
  EXPECT_TRUE(
      lockedMap->at(dstQp)->getTag(kMetaRoceInitialTag)->isInErrorState())
      << "the send completed on a csn two past the one the sequence expects, "
         "so the responder fails the tag rather than moving the sequence over "
         "the receives in between";
}

TEST_F(ReceiverTest, AReadAdvancingOverASkippedCsnFailsTheTag) {
  constexpr uint32_t kSize = 20;
  char readSource[kSize] = {};
  auto* readMr = mrs.reg_mr(nullptr, readSource, kSize, IBV_ACCESS_REMOTE_READ);
  ASSERT_NE(readMr, nullptr);
  // Three, so the send below lands on a csn a posted receive took.
  char recvBuffer[kSize] = {};
  postReceiveWqe(recvBuffer, kSize);
  postReceiveWqe(recvBuffer, kSize);
  postReceiveWqe(recvBuffer, kSize);

  // The acks the responder sends back are not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  // Behind the read and numbered two csns on, so it binds a receive without
  // completing it and the read below is what runs delivery over it.
  char payload[kSize] = {};
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          static_cast<uint16_t>(msn + 1),
          /*tag=*/0,
          /*posn=*/0,
          payload,
          kSize,
          BaseHeader::Opcode::OperationType::SEND,
          /*maybeImmData=*/std::nullopt,
          /*padding=*/0,
          /*csn=*/kFixtureRxCsn + 2),
      kEcnECT0);

  mockReceiver_->simulateReceivePacket(
      buildTestReadRequestPacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          msn,
          reinterpret_cast<uint64_t>(readSource),
          readMr->rkey,
          kSize),
      kEcnECT0);

  auto lockedMap = qpNumToQp_.rlock();
  EXPECT_TRUE(
      lockedMap->at(dstQp)->getTag(kMetaRoceInitialTag)->isInErrorState())
      << "the read takes no receive of its own, but moving delivery past its "
         "msn completes one numbered out of sequence, so the responder fails "
         "the tag rather than leaving that receive with no completion";
}

/*
 * Tests that a WRITE_IMM (non-`_P` variant) packet with zero-byte payload
 * is rejected with NAK_INVALID_REQUEST. Per spec 8.2.3, WRITE_IMM is
 * forbidden at zero length; only the `_P` (Present) variant of WRITE w/imm
 * permits zero-byte payloads.
 */
TEST_F(ReceiverTest, SendNakInvalidRequestForZeroPayloadWriteImm) {
  char regionBuffer[4096] = {};
  auto* mr = mrs.reg_mr(
      nullptr,
      regionBuffer,
      sizeof(regionBuffer),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(mr, nullptr);
  auto dataPacket = buildTestWritePacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      (uint64_t)regionBuffer,
      mr->rkey,
      nullptr,
      0,
      BaseHeader::Opcode::OperationType::WRITE_IMM);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state after zero-payload WRITE_IMM";
}

TEST_F(ReceiverTest, ZeroPayloadWriteImmDrawsNoSharedReceive) {
  struct ibv_pd sharedPd{};
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2, /*maxSge=*/1, /*protectionDomain=*/sharedPd};
  qpNumToQp_.wlock()->clear();
  buildQp(&sharedQueue);
  // The queue pair holds a reference the shared queue's destructor checks for.
  auto dropTheQueuePair =
      folly::makeGuard([this] { qpNumToQp_.wlock()->clear(); });

  char receiveBuffer[4096] = {};
  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{.workRequestId = 777, .sges = oneSge(receiveBuffer)}))
      << "precondition: the queue holds the receive the immediate would draw";

  char regionBuffer[4096] = {};
  auto* mr = mrs.reg_mr(
      nullptr,
      regionBuffer,
      sizeof(regionBuffer),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(mr, nullptr);

  // A valid RETH, so only the empty payload can reject this request.
  auto dataPacket = buildTestWritePacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      (uint64_t)regionBuffer,
      mr->rkey,
      nullptr,
      0,
      BaseHeader::Opcode::OperationType::WRITE_IMM);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(sharedQueue.getNumPosted(), 1u)
      << "a request rejected for carrying no payload reports on nothing, so "
         "drawing a buffer would spend one that the queue pairs still running "
         "could have delivered into";
}

/*
 * A WRITE w/imm message's destination is validated once, from its first
 * packet, and cached on the receive WQE (spec 8.3: the RETH is identical
 * across a message, so the receiver may reuse the already-validated copy). A
 * later packet that carries a different RETH address must not redirect the
 * write: delivery stays at the cached, validated destination, so a misbehaving
 * sender cannot steer payload into memory that was never validated.
 */
TEST_F(ReceiverTest, WriteImmLaterPacketDoesNotRedirectDestination) {
  constexpr uint32_t kMtu = 4096;
  constexpr uint32_t kMessageLen = 2 * kMtu; // two POSNs at this MTU
  constexpr uint32_t kImmData = 0xABCD1234;

  char validatedRegion[kMessageLen] = {};
  char redirectRegion[kMessageLen] = {};
  auto* validatedMr = mrs.reg_mr(
      nullptr,
      validatedRegion,
      sizeof(validatedRegion),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(validatedMr, nullptr);
  auto* redirectMr = mrs.reg_mr(
      nullptr,
      redirectRegion,
      sizeof(redirectRegion),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(redirectMr, nullptr);

  // A write-with-immediate consumes a posted receive WQE; the RETH address
  // takes precedence over the WQE buffer, so a null buffer is sufficient here.
  postReceiveWqe(/*buffer=*/nullptr, /*bufferSize=*/0);

  // The immediate is Present only on the last packet (WRITE_IMM_P); earlier
  // packets of the same message are plain WRITE_IMM and carry no immediate.
  auto makeWritePacket = [&](uint16_t packetFsn,
                             uint16_t posn,
                             uint64_t rethAddr,
                             uint32_t rkey,
                             char* payload,
                             BaseHeader::Opcode::OperationType opType,
                             std::optional<uint32_t> immData) {
    BaseHeader::Opcode opCode(opType);
    BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                .setSrcQp(srcQp)
                                .setDstQp(dstQp)
                                .setFlowId(flowId)
                                .setOpcode(std::move(opCode))
                                .setVersion(0)
                                .build();
    DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                .setFsn(packetFsn)
                                .setUna(packetFsn)
                                .setPosn(posn)
                                .setMsn(msn)
                                .setCsn(kFixtureRxCsn)
                                .setTimestamp(0)
                                .setAckRequest(true)
                                .build();
    ReadWriteHeader readWriteHeader(rethAddr, rkey, kMessageLen);
    return std::make_unique<ReadWritePacket>(
        std::move(baseHeader),
        std::move(dataHeader),
        std::move(readWriteHeader),
        payload,
        kMtu,
        immData);
  };

  char posn0Payload[kMtu];
  char posn1Payload[kMtu];
  std::memset(posn0Payload, 0xAA, sizeof(posn0Payload));
  std::memset(posn1Payload, 0xBB, sizeof(posn1Payload));

  // The receiver may emit ACKs; no NAK is expected for this valid message.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AtLeast(0));

  // POSN 0 (WRITE_IMM, no immediate) validates and caches validatedRegion.
  mockReceiver_->simulateReceivePacket(
      makeWritePacket(
          fsn,
          /*posn=*/0,
          (uint64_t)validatedRegion,
          validatedMr->rkey,
          posn0Payload,
          BaseHeader::Opcode::OperationType::WRITE_IMM,
          std::nullopt),
      kEcnECT0);
  // POSN 1 (WRITE_IMM_P, last packet) points its RETH at redirectRegion; the
  // receiver must ignore it and deliver to the cached validated address.
  mockReceiver_->simulateReceivePacket(
      makeWritePacket(
          fsn + 1,
          /*posn=*/1,
          (uint64_t)redirectRegion,
          redirectMr->rkey,
          posn1Payload,
          BaseHeader::Opcode::OperationType::WRITE_IMM_P,
          kImmData),
      kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "a consistent-length WRITE w/imm should be delivered without error";
  EXPECT_EQ(0, std::memcmp(validatedRegion, posn0Payload, kMtu))
      << "POSN 0 payload should land at the validated destination";
  EXPECT_EQ(0, std::memcmp(validatedRegion + kMtu, posn1Payload, kMtu))
      << "POSN 1 must deliver to the cached validated address, not to the "
         "address carried in its own RETH";
  const char zeroRegion[kMessageLen] = {};
  EXPECT_EQ(0, std::memcmp(redirectRegion, zeroRegion, kMessageLen))
      << "the later packet's RETH address must receive no payload";
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "the WRITE w/imm should complete the posted receive WQE";
}

/*
 * Tests that a SEND packet with zero-byte payload is accepted. Per spec
 * 8.2.3 table "Zero-byte opcodes", SEND permits zero-byte payloads (used
 * for synchronization barriers and control signaling).
 */
TEST_F(ReceiverTest, AcceptZeroPayloadSend) {
  // Post a zero-length receive WQE to consume the incoming SEND.
  postReceiveWqe(/*buffer=*/nullptr, /*bufferSize=*/0);

  auto dataPacket = buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn);

  // No NAK should be generated; the receiver may emit an ACK on the socket.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AtLeast(0));
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should remain in normal state after zero-payload SEND is "
         "accepted per spec 8.2.3";
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "the posted recv WQE should complete with the zero-byte SEND";
}

// Tests for multi-GID Receiver functionality.
// The Receiver now supports listening on multiple local addresses (GIDs)
// on a single thread to avoid race conditions when processing packets
// from the same QP across different paths.

// TrackingMockSocket extends MockUDPSocket to track bind calls.
class TrackingMockSocket : public NiceMock<MockUDPSocket> {
 public:
  explicit TrackingMockSocket(
      folly::EventBase* evb,
      std::vector<folly::SocketAddress>& boundAddresses,
      std::vector<folly::EventBase*>& socketEventBases)
      : NiceMock<MockUDPSocket>(evb),
        boundAddresses_(boundAddresses),
        socketEventBases_(socketEventBases) {
    // Record the EventBase this socket is associated with.
    socketEventBases_.push_back(evb);

    // Set up the mock to track bind calls via bindImpl.
    ON_CALL(*this, bindImpl(::testing::_))
        .WillByDefault([this](const folly::SocketAddress& address) {
          boundAddresses_.push_back(address);
        });
  }

 private:
  std::vector<folly::SocketAddress>& boundAddresses_;
  std::vector<folly::EventBase*>& socketEventBases_;
};

class MultiGidReceiverTest : public ::testing::Test {
 protected:
  void SetUp() override {
    evb_ = std::make_unique<folly::EventBase>();
    boundAddresses_.clear();
    socketEventBases_.clear();
    resumeReadCallCount_ = 0;
  }

  void TearDown() override {
    qpNumToQp_.wlock()->clear();
    evb_.reset();
  }

  // Create a socket maker that tracks bind calls.
  std::function<std::pair<
      std::unique_ptr<folly::AsyncUDPSocket>,
      std::unique_ptr<Modifier>>(folly::EventBase*)>
  makeTrackingSocketMaker() {
    return [this](folly::EventBase* evb) {
      return std::make_pair(
          std::make_unique<TrackingMockSocket>(
              evb, boundAddresses_, socketEventBases_),
          nullptr);
    };
  }

  folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>
      qpNumToQp_;
  std::unique_ptr<folly::EventBase> evb_;
  std::unique_ptr<PingController> pingController_{
      std::make_unique<PingController>(
          mockUDPSocketMaker,
          kMetaRoceSendToPort)};
  std::vector<folly::SocketAddress> boundAddresses_;
  std::vector<folly::EventBase*> socketEventBases_;
  int resumeReadCallCount_ = 0;
};

// Test creating a Receiver with multiple local addresses verifies socket
// creation and binding.
TEST_F(MultiGidReceiverTest, CreateReceiverWithMultipleAddresses) {
  std::vector<folly::IPAddress> localAddrs = {
      folly::IPAddress("fc00::1"),
      folly::IPAddress("fc00::2"),
      folly::IPAddress("fc00::3"),
  };

  auto trackingSocketMaker = makeTrackingSocketMaker();

  auto receiver = std::make_unique<Receiver>(
      localAddrs,
      qpNumToQp_,
      trackingSocketMaker,
      kMetaRoceBindToPort,
      kMetaRoceSendToPort,
      *pingController_);

  // Verify all addresses are registered.
  EXPECT_TRUE(receiver->hasLocalAddress(folly::IPAddress("fc00::1")));
  EXPECT_TRUE(receiver->hasLocalAddress(folly::IPAddress("fc00::2")));
  EXPECT_TRUE(receiver->hasLocalAddress(folly::IPAddress("fc00::3")));

  // Verify address not in the list is not registered.
  EXPECT_FALSE(receiver->hasLocalAddress(folly::IPAddress("fc00::99")));

  // Verify that exactly 3 sockets were created and bound.
  EXPECT_EQ(boundAddresses_.size(), 3);

  // Verify each address was bound with the correct port.
  std::set<std::string> expectedAddrs = {"fc00::1", "fc00::2", "fc00::3"};
  std::set<std::string> actualAddrs;
  for (const auto& addr : boundAddresses_) {
    EXPECT_EQ(addr.getPort(), kMetaRoceBindToPort);
    actualAddrs.insert(addr.getIPAddress().str());
  }
  EXPECT_EQ(actualAddrs, expectedAddrs);

  // Verify all sockets share the same EventBase (single-threaded).
  EXPECT_EQ(socketEventBases_.size(), 3);
  folly::EventBase* firstEvb = socketEventBases_[0];
  for (auto* evb : socketEventBases_) {
    EXPECT_EQ(evb, firstEvb);
  }
}

// Test creating a Receiver with a single address (backwards compatibility).
TEST_F(MultiGidReceiverTest, CreateReceiverWithSingleAddress) {
  auto trackingSocketMaker = makeTrackingSocketMaker();

  auto receiver = std::make_unique<Receiver>(
      folly::IPAddress("fc00::1"),
      qpNumToQp_,
      trackingSocketMaker,
      kMetaRoceBindToPort,
      kMetaRoceSendToPort,
      *pingController_);

  EXPECT_TRUE(receiver->hasLocalAddress(folly::IPAddress("fc00::1")));
  EXPECT_FALSE(receiver->hasLocalAddress(folly::IPAddress("fc00::2")));

  // Verify exactly 1 socket was created and bound.
  EXPECT_EQ(boundAddresses_.size(), 1);
  EXPECT_EQ(boundAddresses_[0].getIPAddress().str(), "fc00::1");
  EXPECT_EQ(boundAddresses_[0].getPort(), kMetaRoceBindToPort);
}

// Test adding local addresses dynamically to an existing Receiver.
TEST_F(MultiGidReceiverTest, AddLocalAddressDynamically) {
  auto trackingSocketMaker = makeTrackingSocketMaker();

  auto receiver = std::make_unique<Receiver>(
      folly::IPAddress("fc00::1"),
      qpNumToQp_,
      trackingSocketMaker,
      kMetaRoceBindToPort,
      kMetaRoceSendToPort,
      *pingController_);

  // Initially only one address and one socket.
  EXPECT_TRUE(receiver->hasLocalAddress(folly::IPAddress("fc00::1")));
  EXPECT_FALSE(receiver->hasLocalAddress(folly::IPAddress("fc00::2")));
  EXPECT_EQ(boundAddresses_.size(), 1);

  // Add a new address dynamically.
  receiver->addLocalAddress(
      folly::IPAddress("fc00::2"), trackingSocketMaker, kMetaRoceBindToPort);

  // Now both addresses should be registered.
  EXPECT_TRUE(receiver->hasLocalAddress(folly::IPAddress("fc00::1")));
  EXPECT_TRUE(receiver->hasLocalAddress(folly::IPAddress("fc00::2")));

  // Verify a second socket was created and bound.
  EXPECT_EQ(boundAddresses_.size(), 2);

  // All sockets should share the same EventBase.
  EXPECT_EQ(socketEventBases_.size(), 2);
  EXPECT_EQ(socketEventBases_[0], socketEventBases_[1]);
}

// Test that adding a duplicate address is a no-op.
TEST_F(MultiGidReceiverTest, AddDuplicateAddressIsNoOp) {
  auto trackingSocketMaker = makeTrackingSocketMaker();

  auto receiver = std::make_unique<Receiver>(
      folly::IPAddress("fc00::1"),
      qpNumToQp_,
      trackingSocketMaker,
      kMetaRoceBindToPort,
      kMetaRoceSendToPort,
      *pingController_);

  // Initially one socket bound.
  EXPECT_EQ(boundAddresses_.size(), 1);

  // Add the same address again - should not create a new socket.
  receiver->addLocalAddress(
      folly::IPAddress("fc00::1"), trackingSocketMaker, kMetaRoceBindToPort);

  EXPECT_TRUE(receiver->hasLocalAddress(folly::IPAddress("fc00::1")));

  // Verify no new socket was created (still just 1).
  EXPECT_EQ(boundAddresses_.size(), 1);
}

/*
 * Tests that a BRNR (Buffer Receiver Not Ready) ACK is generated when
 * the receiver gets a data packet with an unregistered tag.
 *
 * This exercises the full Receiver -> RxFlow -> BRNR pipeline:
 * 1. Receiver skips tag-dependent operations for the unknown tag
 * 2. RxFlow::processNewDataPacket sets wqe=nullptr, triggering BRNR
 * 3. RxFlow::processDataPacket builds the BRNR ACK
 * 4. The ACK is enqueued on the RxFlow for the sender to retrieve
 */
TEST_F(ReceiverTest, SendBrnrForInvalidTag) {
  // Tag 99 does not exist on QP 456 (only tag 0 is registered by default)
  uint8_t invalidTag = 99;
  auto dataPacket =
      buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn, invalidTag);

  // Simulate receiving the packet — BRNR ACK is enqueued on the RxFlow,
  // not sent directly on the receiver socket.
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  // Dequeue the BRNR ACK from the RxFlow
  auto lockedMap = qpNumToQp_.rlock();
  auto& qp = lockedMap->at(dstQp);
  auto* rxFlow = qp->getRxFlowById(flowId);
  auto egressPacket = rxFlow->getNextAckToSend();
  ASSERT_NE(egressPacket, nullptr)
      << "a BRNR ACK should be enqueued for the invalid tag";

  auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
  ASSERT_NE(ackPacket, nullptr);

  EXPECT_EQ(ackPacket->getAckHeader().getAckStatus(), AckStatus::BRNR)
      << "invalid tag should produce a BRNR response";
  EXPECT_EQ(
      ackPacket->getAckHeader().getRnrBackoffTimeExp(), kBrnrBackoffTimeExp)
      << "backoff exponent should match kBrnrBackoffTimeExp";
  EXPECT_EQ(ackPacket->getAckHeader().getTag(), 0)
      << "BRNR ACK for an invalid tag should fall back to tag 0";
}

/*
 * Tests that a BRNR ACK is generated when the receiver gets a READ
 * packet with an unregistered tag.  READ operations normally bypass
 * the BRNR path (they don't consume receive WQEs), but an invalid
 * tag should still trigger BRNR regardless of opcode.
 */
TEST_F(ReceiverTest, SendBrnrForInvalidTagRead) {
  uint8_t invalidTag = 99;

  BaseHeader::Opcode opCode(BaseHeader::Opcode::OperationType::READ);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(srcQp)
                                    .setDstQp(dstQp)
                                    .setFlowId(flowId)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();

  const auto nowNs = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count());

  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(fsn)
                                    .setUna(fsn)
                                    .setPosn(0)
                                    .setMsn(msn)
                                    .setTag(invalidTag)
                                    .setTimestamp(nowNs)
                                    .setAckRequest(true)
                                    .build();

  // READ requires a ReadWriteHeader; values are irrelevant for invalid-tag path
  const ReadWriteHeader readWriteHeader(0, 0, 0);
  auto dataPacket = std::make_unique<ReadWritePacket>(
      baseHeader, dataHeader, readWriteHeader, nullptr, 0);

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  auto lockedMap = qpNumToQp_.rlock();
  auto& qp = lockedMap->at(dstQp);
  auto* rxFlow = qp->getRxFlowById(flowId);
  auto egressPacket = rxFlow->getNextAckToSend();
  ASSERT_NE(egressPacket, nullptr)
      << "a BRNR ACK should be enqueued for a READ with an invalid tag";

  auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
  ASSERT_NE(ackPacket, nullptr);

  EXPECT_EQ(ackPacket->getAckHeader().getAckStatus(), AckStatus::BRNR)
      << "READ with invalid tag should produce a BRNR response";
  EXPECT_EQ(
      ackPacket->getAckHeader().getRnrBackoffTimeExp(), kBrnrBackoffTimeExp)
      << "backoff exponent should match kBrnrBackoffTimeExp";
  EXPECT_EQ(ackPacket->getAckHeader().getTag(), 0)
      << "BRNR ACK for an invalid tag should fall back to tag 0";
}

/*
 * Tests that an inbound msn before the receive window (a READ with msn < cdmsn)
 * yields a BRNR, not a fatal NAK or QP error. An out-of-range msn on a
 * deliverable message is answered with an RNR (backoff + retry), leaving the QP
 * operational. A READ is used so the no-receive-WQE BRNR trigger does not fire
 * -- only the before-window msn produces the BRNR.
 */
TEST_F(ReceiverTest, SendBrnrForMsnBeforeWindow) {
  constexpr uint16_t kStaleMsn = 10; // before the fixture's cdmsn

  char readBuf[8] = {};
  auto dataPacket = buildTestWritePacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      kStaleMsn,
      /*remoteAddress=*/0,
      /*remoteKey=*/0,
      readBuf,
      sizeof(readBuf),
      BaseHeader::Opcode::OperationType::READ);

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  auto lockedMap = qpNumToQp_.rlock();
  auto& qp = lockedMap->at(dstQp);
  auto* rxFlow = qp->getRxFlowById(flowId);
  auto egressPacket = rxFlow->getNextAckToSend();
  ASSERT_NE(egressPacket, nullptr)
      << "an out-of-window msn should enqueue a BRNR ACK";
  auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
  ASSERT_NE(ackPacket, nullptr);
  EXPECT_EQ(ackPacket->getAckHeader().getAckStatus(), AckStatus::BRNR)
      << "an msn before the receive window must yield BRNR, not a fatal NAK or a "
         "QP-fatal error";
  EXPECT_EQ(
      ackPacket->getAckHeader().getRnrBackoffTimeExp(), kBrnrBackoffTimeExp)
      << "BRNR backoff exponent should match kBrnrBackoffTimeExp";
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "an out-of-window msn must leave the QP operational (recoverable)";
}

/*
 * An out-of-window SEND (msn <= cdmsn) must be BRNR'd before it binds the
 * posted receive WQE to its stale msn; otherwise a stale/replayed SEND steals
 * the WQE the legitimate request needs. SEND is used because its WQE binding
 * (assignInitiatorMsnToReceiveWqeIfNeeded) has no stale-msn guard of its own.
 */
TEST_F(ReceiverTest, OutOfWindowSendDoesNotBindPostedReceiveWqe) {
  constexpr uint32_t kBufSize = 16;
  char recvBuffer[kBufSize] = {};
  postReceiveWqe(recvBuffer, kBufSize);

  char payload[kBufSize];
  std::memset(payload, 'S', kBufSize);
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          /*msn=*/kFixtureCdmsn,
          /*tag=*/0,
          /*posn=*/0,
          payload,
          kBufSize),
      kEcnECT0);

  auto lockedMap = qpNumToQp_.rlock();
  auto& qp = lockedMap->at(dstQp);
  auto* rxFlow = qp->getRxFlowById(flowId);
  auto egressPacket = rxFlow->getNextAckToSend();
  ASSERT_NE(egressPacket, nullptr)
      << "an out-of-window SEND should enqueue a BRNR ACK";
  auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
  ASSERT_NE(ackPacket, nullptr);
  EXPECT_EQ(ackPacket->getAckHeader().getAckStatus(), AckStatus::BRNR)
      << "an out-of-window SEND must yield BRNR, not bind and deliver";
  EXPECT_EQ(
      qp->getTag(kMetaRoceInitialTag)
          ->getReceiveWqe(
              BaseHeader::Opcode::OperationType::SEND, kFixtureCdmsn),
      nullptr)
      << "the stale SEND must not bind the posted receive WQE to its "
         "out-of-window msn; the WQE must stay available for the real request";
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "an out-of-window SEND must leave the QP operational";
}

/*
 * An out-of-window WRITE (msn <= cdmsn) must be BRNR'd before it creates a
 * virtual receive WQE. WRITE is the partially-guarded case: createVirtualRWqe
 * self-guards only on `before(msn, cdmsn)` (strictly less-than), so at
 * msn == cdmsn it would otherwise register a virtual receive WQE for an
 * already-delivered msn; the out-of-window gate closes that boundary.
 */
TEST_F(ReceiverTest, OutOfWindowWriteDoesNotCreateVirtualReceiveWqe) {
  char regionBuffer[4096] = {};
  auto* mr = mrs.reg_mr(
      nullptr,
      regionBuffer,
      sizeof(regionBuffer),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(mr, nullptr);

  constexpr int kLen = 64;
  std::vector<char> payload(kLen, 'W');
  mockReceiver_->simulateReceivePacket(
      buildTestWritePacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          /*msn=*/kFixtureCdmsn,
          reinterpret_cast<uint64_t>(regionBuffer),
          mr->rkey,
          payload.data(),
          kLen,
          BaseHeader::Opcode::OperationType::WRITE),
      kEcnECT0);

  auto lockedMap = qpNumToQp_.rlock();
  auto& qp = lockedMap->at(dstQp);
  auto* rxFlow = qp->getRxFlowById(flowId);
  auto egressPacket = rxFlow->getNextAckToSend();
  ASSERT_NE(egressPacket, nullptr)
      << "an out-of-window WRITE should enqueue a BRNR ACK";
  auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
  ASSERT_NE(ackPacket, nullptr);
  EXPECT_EQ(ackPacket->getAckHeader().getAckStatus(), AckStatus::BRNR)
      << "an out-of-window WRITE at msn == cdmsn must yield BRNR";
  EXPECT_EQ(
      qp->getTag(kMetaRoceInitialTag)
          ->getReceiveWqe(
              BaseHeader::Opcode::OperationType::WRITE, kFixtureCdmsn),
      nullptr)
      << "the out-of-window WRITE must not create a virtual receive WQE at "
         "msn == cdmsn, the boundary createVirtualRWqe's before-guard misses";
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "an out-of-window WRITE must leave the QP operational";
}

/*
 * Tests that a READ_RESP whose msn matches no pending initiator WQE is
 * discarded (payload dropped, no completion, QP operational) and is not RNR'd
 * -- an RNR would tell the responder to retransmit a response we can never
 * match. An unmatched ATOMIC_ACK_RESP is handled the same.
 */
TEST_F(ReceiverTest, DiscardUnmatchedReadResp) {
  // No READ WQE is posted, so no pending msn matches this response.
  constexpr uint16_t kUnmatchedMsn = 7;
  char respPayload[64] = {};
  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      kUnmatchedMsn,
      /*tag=*/0,
      /*posn=*/0,
      respPayload,
      sizeof(respPayload),
      BaseHeader::Opcode::OperationType::READ_RESP);

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "an unmatched read response must leave the QP operational";
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "an unmatched read response completes no WQE";

  // Any ack that goes out (the FSN is still acked) must not be an RNR.
  auto lockedMap = qpNumToQp_.rlock();
  auto* rxFlow = lockedMap->at(dstQp)->getRxFlowById(flowId);
  auto egressPacket = rxFlow->getNextAckToSend();
  if (egressPacket != nullptr) {
    auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
    ASSERT_NE(ackPacket, nullptr);
    const auto status = ackPacket->getAckHeader().getAckStatus();
    EXPECT_NE(status, AckStatus::BRNR)
        << "an unmatched read response must be discarded, not BRNR'd";
    EXPECT_NE(status, AckStatus::HRNR)
        << "an unmatched read response must not be HRNR'd either";
  }
}

/*
 * Sends, writes, reads and atomics all draw msns from one sequence, so a
 * response's msn alone can match any of them. A peer answering a send is
 * contradicting the protocol: nothing was asked of it. Delivering anyway would
 * write the response into the send's source buffer, memory the client handed
 * over to be read and never to be written. The frame is dropped without an ack,
 * so the peer is not told it was taken.
 */
TEST_F(ReceiverTest, DiscardAReadRespMatchingASend) {
  constexpr uint32_t kSendSize = 4096;
  std::array<char, kSendSize> sendBuffer{};
  sendBuffer.fill('S');

  uint16_t sendMsn = 0;
  {
    auto lockedMap = qpNumToQp_.rlock();
    auto* qp = lockedMap->at(dstQp).get();
    auto tag = qp->getTag(kMetaRoceInitialTag);
    auto sendWqe = std::make_unique<SendWqe>(
        /*workReqId=*/99, oneSge(sendBuffer), qp->getMtu());
    Wqe* posted = sendWqe.get();
    postToTag(*tag, std::move(sendWqe), qp->getMtu());
    ASSERT_TRUE(posted->getMsn().has_value())
        << "precondition: the send must hold an msn for a response to match";
    sendMsn = posted->getMsn().value();
  }

  std::array<char, kSendSize> respPayload{};
  respPayload.fill('R');
  auto readRespPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      sendMsn,
      /*tag=*/0,
      /*posn=*/0,
      respPayload.data(),
      kSendSize,
      BaseHeader::Opcode::OperationType::READ_RESP);

  mockReceiver_->simulateReceivePacket(std::move(readRespPacket), kEcnECT0);

  std::array<char, kSendSize> untouched{};
  untouched.fill('S');
  EXPECT_EQ(sendBuffer, untouched)
      << "the send's buffer is its source, so a response written into it would "
         "corrupt memory the client only offered to be read";
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "a response the peer had no request for completes nothing";
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "one contradictory packet is bad input, not a reason to fail the qp";

  // A cumulative ack for earlier frames may still go out; what must not happen
  // is this frame being counted among them.
  auto lockedMap = qpNumToQp_.rlock();
  auto* rxFlow = lockedMap->at(dstQp)->getRxFlowById(flowId);
  auto egressPacket = rxFlow->getNextAckToSend();
  if (egressPacket != nullptr) {
    auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
    ASSERT_NE(ackPacket, nullptr);
    EXPECT_TRUE(before(ackPacket->getAckHeader().getCumulativeFsn(), fsn))
        << "acking through this frame tells the peer it was taken, and it was "
           "not";
  }
}

/*
 * A write is a ReadWriteWqe, the same class a read uses, so the type assert on
 * the read-response path accepts one and the only barrier left is the local
 * permission check -- which a buffer registered for local write passes. This
 * is the quiet case: no assert fires and the peer's payload lands in the
 * write's source buffer. The work type is what separates them.
 */
TEST_F(ReceiverTest, DiscardAReadRespMatchingAWrite) {
  constexpr uint32_t kWriteSize = 4096;
  std::array<char, kWriteSize> writeBuffer{};
  writeBuffer.fill('W');

  uint16_t writeMsn = 0;
  {
    auto lockedMap = qpNumToQp_.rlock();
    auto* qp = lockedMap->at(dstQp).get();
    auto tag = qp->getTag(kMetaRoceInitialTag);
    auto writeWqe = std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kWrite,
        /*workReqId=*/77,
        oneSge(
            writeBuffer,
            // Registered for local write, so the permission check cannot be
            // what saves the buffer here.
            mrs.reg_mr(
                   nullptr,
                   writeBuffer.data(),
                   kWriteSize,
                   IBV_ACCESS_LOCAL_WRITE)
                ->lkey),
        qp->getMtu(),
        /*remoteAddress=*/0x2000,
        /*rkey=*/1,
        /*send_flags=*/IBV_SEND_SIGNALED);
    Wqe* posted = writeWqe.get();
    postToTag(*tag, std::move(writeWqe), qp->getMtu());
    ASSERT_TRUE(posted->getMsn().has_value())
        << "precondition: the write must hold an msn for a response to match";
    writeMsn = posted->getMsn().value();
  }

  std::array<char, kWriteSize> respPayload{};
  respPayload.fill('R');
  auto readRespPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      writeMsn,
      /*tag=*/0,
      /*posn=*/0,
      respPayload.data(),
      kWriteSize,
      BaseHeader::Opcode::OperationType::READ_RESP);

  mockReceiver_->simulateReceivePacket(std::move(readRespPacket), kEcnECT0);

  std::array<char, kWriteSize> untouched{};
  untouched.fill('W');
  EXPECT_EQ(writeBuffer, untouched)
      << "the write's buffer holds what the client asked to be sent; a "
         "response written over it changes the data mid-flight";
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "a write asked the peer for no response, so none completes it";
}

/*
 * The atomic-ack branch is the one where a mismatch is undefined rather than
 * merely wrong: it casts the work request found for the msn to AtomicWqe to
 * read the key it writes the responder's value under, and the DCHECK standing
 * over that cast is compiled out of an opt build. A send reaching it is a
 * SendWqe, so the key comes off the wrong object.
 */
TEST_F(ReceiverTest, DiscardAnAtomicAckMatchingASend) {
  constexpr uint32_t kSendSize = 4096;
  std::array<char, kSendSize> sendBuffer{};
  sendBuffer.fill('S');

  uint16_t sendMsn = 0;
  {
    auto lockedMap = qpNumToQp_.rlock();
    auto* qp = lockedMap->at(dstQp).get();
    auto tag = qp->getTag(kMetaRoceInitialTag);
    auto sendWqe = std::make_unique<SendWqe>(
        /*workReqId=*/55, oneSge(sendBuffer), qp->getMtu());
    Wqe* posted = sendWqe.get();
    postToTag(*tag, std::move(sendWqe), qp->getMtu());
    ASSERT_TRUE(posted->getMsn().has_value())
        << "precondition: the send must hold an msn for an atomic ack to match";
    sendMsn = posted->getMsn().value();
  }

  mockReceiver_->simulateReceivePacket(
      buildTestAtomicAckPacket(
          srcQp, dstQp, flowId, fsn, sendMsn, /*origRemoteData=*/0xfeedfaceULL),
      kEcnECT0);

  std::array<char, kSendSize> untouched{};
  untouched.fill('S');
  EXPECT_EQ(sendBuffer, untouched)
      << "the atomic result is written straight to the work request's buffer "
         "address, so a send reaching that write loses the first eight bytes "
         "of what the client asked to be sent";
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "a send asked the peer for no atomic result, so none completes it";
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "one contradictory packet is bad input, not a reason to fail the qp";
}

/*
 * Tests that a padded last-posn packet with valid 4-byte alignment is accepted.
 * The receiver should accept the packet without sending a NAK when the payload
 * size is slightly larger than expected but aligns to a 4-byte boundary.
 *
 * Example: expected=101 bytes, actual=104 bytes (3 bytes padding to reach
 * 4-byte alignment)
 */
TEST_F(ReceiverTest, AcceptPaddedLastPosnPacket) {
  // Create a receive buffer of 101 bytes - not 4-byte aligned
  constexpr uint32_t kRecvBufferSize = 101;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  // Send a packet with 104 bytes payload (padded to 4-byte boundary)
  // This should be accepted because:
  // 1. It's the last posn (posn 0 for a sub-MTU message)
  // 2. actualPayloadSize (104) > expectedPayloadSize (101)
  // 3. actualPayloadSize (104) is a multiple of 4
  // 4. actualPayloadSize (104) <= paddedExpectedSize ((101+3) & ~3 = 104)
  constexpr uint32_t kPaddedPayloadSize = 104;
  char payload[kPaddedPayloadSize];
  std::memset(payload, 'X', kRecvBufferSize); // Data
  std::memset(payload + kRecvBufferSize, 0, 3); // Padding bytes

  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kPaddedPayloadSize);

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  // Verify QP is NOT in error state
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should not enter error state for a valid padded last-posn packet";

  // Verify that only the expected 101 bytes were delivered (padding ignored)
  for (uint32_t i = 0; i < kRecvBufferSize; i++) {
    EXPECT_EQ(recvBuffer[i], 'X')
        << "Byte " << i << " should contain the delivered data";
  }

  // Verify completion was generated
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "receive WQE should complete successfully";
}

TEST_F(ReceiverTest, PaddingInferenceRequiresZeroPaddingField) {
  constexpr uint32_t kRecvBufferSize = 101;
  constexpr uint32_t kPayloadSize = 104;
  constexpr uint8_t kPadding = 1;
  char recvBuffer[kRecvBufferSize] = {};
  char payload[kPayloadSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kPayloadSize,
      BaseHeader::Opcode::OperationType::SEND,
      std::nullopt,
      kPadding);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "a nonzero Padding field should make any remaining excess payload a "
         "length error";
}

/*
 * Tests that padded packets are only accepted for the last posn, not for
 * intermediate positions which must have exactly MTU bytes.
 */
TEST_F(ReceiverTest, RejectPaddedNonLastPosnPacket) {
  // Create a multi-packet message (5000 bytes at MTU 4096 = 2 POSNs)
  constexpr uint32_t kRecvBufferSize = 5000;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  // Send POSN 0 with 4100 bytes (not exactly MTU, has "padding")
  // This should be rejected because POSN 0 is not the last position
  // and must have exactly MTU (4096) bytes
  constexpr uint32_t kBadPayloadSize = 4100;
  char payload[kBadPayloadSize] = {};
  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kBadPayloadSize);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state when a non-last POSN has wrong size";
}

/*
 * Tests that the receiver rejects packets with too much padding (> 3 bytes)
 * on the last posn. Padding is only allowed up to 3 bytes to reach a 4-byte
 * boundary.
 */
TEST_F(ReceiverTest, RejectExcessivePaddingOnLastPosn) {
  // Create a receive buffer of 100 bytes (already 4-byte aligned)
  constexpr uint32_t kRecvBufferSize = 100;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  // Send a packet with 104 bytes payload - this is 4 bytes more than expected
  // Since expected (100) is already 4-byte aligned, no padding is allowed
  // paddedExpectedSize = (100 + 3) & ~3 = 100, so 104 > 100 is invalid
  constexpr uint32_t kExcessPaddedPayloadSize = 104;
  char payload[kExcessPaddedPayloadSize] = {};
  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kExcessPaddedPayloadSize);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state when padding exceeds 4-byte alignment";
}

/*
 * Tests that the receiver rejects padded packets that don't align to a
 * 4-byte boundary.
 */
TEST_F(ReceiverTest, RejectNonAlignedPaddingOnLastPosn) {
  // Create a receive buffer of 101 bytes
  constexpr uint32_t kRecvBufferSize = 101;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  // Send a packet with 103 bytes payload - this is 2 bytes of padding
  // but 103 is not a multiple of 4, so it should be rejected
  constexpr uint32_t kNonAlignedPayloadSize = 103;
  char payload[kNonAlignedPayloadSize] = {};
  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kNonAlignedPayloadSize);

  expectNakInvalidRequest();
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should enter error state when padding doesn't reach 4-byte alignment";
}

/*
 * Tests various valid padding amounts (1, 2, and 3 bytes) on the last posn.
 * All should be accepted as long as they result in a 4-byte aligned payload.
 * This test uses 1 byte of padding: expected=103, actual=104.
 */
TEST_F(ReceiverTest, AcceptOneBytePaddingOnLastPosn) {
  constexpr uint32_t kRecvBufferSize = 103;
  char recvBuffer[kRecvBufferSize];
  std::memset(recvBuffer, 0, sizeof(recvBuffer));
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  constexpr uint32_t kPaddedPayloadSize = 104;
  char payload[kPaddedPayloadSize];
  std::memset(payload, 'A', kRecvBufferSize);
  std::memset(payload + kRecvBufferSize, 0, 1); // 1 byte padding

  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kPaddedPayloadSize);

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "1 byte padding (103->104) should be accepted";

  // Verify only 103 bytes were delivered
  for (uint32_t i = 0; i < kRecvBufferSize; i++) {
    EXPECT_EQ(recvBuffer[i], 'A');
  }
}

/*
 * Tests 2 bytes of padding: expected=102, actual=104.
 */
TEST_F(ReceiverTest, AcceptTwoBytePaddingOnLastPosn) {
  constexpr uint32_t kRecvBufferSize = 102;
  char recvBuffer[kRecvBufferSize];
  std::memset(recvBuffer, 0, sizeof(recvBuffer));
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  constexpr uint32_t kPaddedPayloadSize = 104;
  char payload[kPaddedPayloadSize];
  std::memset(payload, 'B', kRecvBufferSize);
  std::memset(payload + kRecvBufferSize, 0, 2); // 2 bytes padding

  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kPaddedPayloadSize);

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "2 bytes padding (102->104) should be accepted";

  for (uint32_t i = 0; i < kRecvBufferSize; i++) {
    EXPECT_EQ(recvBuffer[i], 'B');
  }
}

/*
 * Tests 3 bytes of padding: expected=101, actual=104.
 */
TEST_F(ReceiverTest, AcceptThreeBytePaddingOnLastPosn) {
  constexpr uint32_t kRecvBufferSize = 101;
  char recvBuffer[kRecvBufferSize];
  std::memset(recvBuffer, 0, sizeof(recvBuffer));
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  constexpr uint32_t kPaddedPayloadSize = 104;
  char payload[kPaddedPayloadSize];
  std::memset(payload, 'C', kRecvBufferSize);
  std::memset(payload + kRecvBufferSize, 0, 3); // 3 bytes padding

  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kPaddedPayloadSize);

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "3 bytes padding (101->104) should be accepted";

  for (uint32_t i = 0; i < kRecvBufferSize; i++) {
    EXPECT_EQ(recvBuffer[i], 'C');
  }
}

/*
 * Tests that exact payload size (no padding) still works correctly.
 */
TEST_F(ReceiverTest, AcceptExactPayloadSizeOnLastPosn) {
  constexpr uint32_t kRecvBufferSize = 100;
  char recvBuffer[kRecvBufferSize] = {};
  postReceiveWqe(recvBuffer, kRecvBufferSize);

  char payload[kRecvBufferSize];
  std::memset(payload, 'Z', kRecvBufferSize);

  auto dataPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kRecvBufferSize);

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "Exact payload size should be accepted";
  EXPECT_EQ(cq->getNumCompletedWqes(), 1)
      << "receive WQE should complete successfully";

  for (uint32_t i = 0; i < kRecvBufferSize; i++) {
    EXPECT_EQ(recvBuffer[i], 'Z') << "All bytes should be delivered correctly";
  }
}

/*
 * A read's destination has one local key, so one region has to cover all of it.
 * A region covering only the first fragment cannot serve the read: it reports
 * a protection error, and no byte of the response reaches the buffer --
 * not even the fragment the short region would have admitted.
 */
TEST_F(ReceiverTest, AReadWhoseRegionCoversPartOfItsBufferWritesNoBytes) {
  constexpr uint32_t kFragment = 4096;
  constexpr uint32_t kReadSize = 2 * kFragment;
  char readBuffer[kReadSize];
  std::memset(readBuffer, 'x', kReadSize);

  // The region stops halfway through the destination.
  const uint16_t readMsn = postReadWqe(readBuffer, kReadSize, kFragment);

  // The first fragment arrives: it falls inside the short region, so a
  // fragment-by-fragment check would admit it.
  char payload[kFragment];
  std::memset(payload, 'R', kFragment);
  auto readRespPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      readMsn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kFragment,
      BaseHeader::Opcode::OperationType::READ_RESP);
  mockReceiver_->simulateReceivePacket(std::move(readRespPacket), kEcnECT0);

  const std::string_view scattered(readBuffer, kReadSize);
  const size_t firstByteWritten = scattered.find_first_not_of('x');
  EXPECT_EQ(firstByteWritten, std::string_view::npos)
      << "the read reports a protection error, so no part of the response may "
         "reach the buffer; byte "
      << firstByteWritten << " was written";

  {
    auto lockedMap = qpNumToQp_.rlock();
    EXPECT_TRUE(
        lockedMap->at(dstQp)->getTag(kMetaRoceInitialTag)->isInErrorState())
        << "leaving the buffer alone is only half of it: the tag must enter "
           "error, which is what has the sender fail this read rather than "
           "leave the client waiting on a response that will never land";
  }
}

/*
 * Tests that a READ_RESP packet with valid payload is processed without
 * generating a NAK.  A ReadWriteWqe (kRead) is posted first so that the
 * receiver can look up the destination buffer by MSN.  The test asserts that
 * no NAK is written to the socket and the QP stays out of error state.
 */
TEST_F(ReceiverTest, HandleReadResp) {
  constexpr uint32_t kReadSize = 4096;
  char readBuffer[kReadSize] = {};

  const uint16_t readMsn = postReadWqe(readBuffer, kReadSize, kReadSize);

  // Build a READ_RESP whose payload matches the READ WQE size.
  char payload[kReadSize];
  std::memset(payload, 'R', kReadSize);
  auto readRespPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      readMsn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kReadSize,
      BaseHeader::Opcode::OperationType::READ_RESP);

  // Any socket write would be a NAK — fail the test if one occurs.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(0);

  mockReceiver_->simulateReceivePacket(std::move(readRespPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "QP should not enter error state for a valid READ_RESP";

  // Verify the payload was actually written to the read buffer.
  EXPECT_EQ(std::memcmp(readBuffer, payload, kReadSize), 0)
      << "READ_RESP payload should be written to the read buffer";
}

/*
 * A read posted with several sges takes its response across them in order,
 * each sge validated under its own key.
 */
TEST_F(ReceiverTest, AReadResponseScattersAcrossTheReadsSges) {
  std::vector<uint8_t> first(1000, 'x');
  std::vector<uint8_t> second(2000, 'x');
  std::vector<uint8_t> third(1096, 'x');
  const uint16_t readMsn = postSegmentedReadWqe(
      {folly::MutableByteRange(first.data(), first.size()),
       folly::MutableByteRange(second.data(), second.size()),
       folly::MutableByteRange(third.data(), third.size())});

  std::vector<uint8_t> payload(first.size() + second.size() + third.size());
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<uint8_t>(i % 251);
  }
  auto readRespPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      readMsn,
      /*tag=*/0,
      /*posn=*/0,
      payload.data(),
      static_cast<uint32_t>(payload.size()),
      BaseHeader::Opcode::OperationType::READ_RESP);
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(0);
  mockReceiver_->simulateReceivePacket(std::move(readRespPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "every sge is registered for local write, so the response is valid";
  std::vector<uint8_t> scattered(first);
  scattered.insert(scattered.end(), second.begin(), second.end());
  scattered.insert(scattered.end(), third.begin(), third.end());
  EXPECT_EQ(scattered, payload)
      << "each response byte must land at its offset, whichever sge holds it";
}

/*
 * Each sge's key identifies its own region, so one unregistered sge fails the
 * read even where the others are valid, and no byte reaches any of them.
 */
TEST_F(ReceiverTest, AReadWithOneUnregisteredSgeWritesNoBytes) {
  std::vector<uint8_t> first(1000, 'x');
  std::vector<uint8_t> second(2000, 'x');
  std::vector<uint8_t> third(1096, 'x');
  const uint16_t readMsn = postSegmentedReadWqe(
      {folly::MutableByteRange(first.data(), first.size()),
       folly::MutableByteRange(second.data(), second.size()),
       folly::MutableByteRange(third.data(), third.size())},
      /*unregisteredIndex=*/1);

  std::vector<uint8_t> payload(
      first.size() + second.size() + third.size(), 'R');
  auto readRespPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      readMsn,
      /*tag=*/0,
      /*posn=*/0,
      payload.data(),
      static_cast<uint32_t>(payload.size()),
      BaseHeader::Opcode::OperationType::READ_RESP);
  mockReceiver_->simulateReceivePacket(std::move(readRespPacket), kEcnECT0);

  for (const auto* buffer : {&first, &second, &third}) {
    EXPECT_EQ(
        std::count(buffer->begin(), buffer->end(), 'x'),
        static_cast<std::ptrdiff_t>(buffer->size()))
        << "the read reports a protection error, so no sge may take any of "
           "the response, including the registered ones";
  }
  auto lockedMap = qpNumToQp_.rlock();
  EXPECT_TRUE(
      lockedMap->at(dstQp)->getTag(kMetaRoceInitialTag)->isInErrorState())
      << "a local protection error must put the tag in error";
}

TEST_F(ReceiverTest, ConcurrentReadsCompleteInTheOrderTheyWereIssued) {
  constexpr uint32_t kReadSize = 64;
  constexpr uint64_t kFirstReadId = 61;
  constexpr uint64_t kSecondReadId = 62;
  char firstBuffer[kReadSize] = {};
  char secondBuffer[kReadSize] = {};
  const uint16_t firstMsn =
      postReadWqe(firstBuffer, kReadSize, kReadSize, kFirstReadId);
  const uint16_t secondMsn =
      postReadWqe(secondBuffer, kReadSize, kReadSize, kSecondReadId);

  // The acks the requester sends back are not what this case is about.
  EXPECT_CALL(*mockSocket_, write(_, _)).Times(AnyNumber());

  char secondPayload[kReadSize];
  std::memset(secondPayload, 'B', kReadSize);
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          static_cast<uint16_t>(fsn + 1),
          secondMsn,
          /*tag=*/0,
          /*posn=*/0,
          secondPayload,
          kReadSize,
          BaseHeader::Opcode::OperationType::READ_RESP,
          /*maybeImmData=*/std::nullopt,
          /*padding=*/0,
          kFixtureRxCsn,
          /*una=*/fsn),
      kEcnECT0);
  EXPECT_EQ(std::memcmp(secondBuffer, secondPayload, kReadSize), 0)
      << "the requester should deliver the second read's response as it "
         "arrives";
  EXPECT_EQ(cq->getNumCompletedWqes(), 0)
      << "the requester should hold the second read's completion while the "
         "first read is still outstanding";

  char firstPayload[kReadSize];
  std::memset(firstPayload, 'A', kReadSize);
  mockReceiver_->simulateReceivePacket(
      buildTestDataPacket(
          srcQp,
          dstQp,
          flowId,
          fsn,
          firstMsn,
          /*tag=*/0,
          /*posn=*/0,
          firstPayload,
          kReadSize,
          BaseHeader::Opcode::OperationType::READ_RESP),
      kEcnECT0);

  ASSERT_EQ(cq->getNumCompletedWqes(), 2)
      << "the first read's response should complete it and release the "
         "second";
  const auto firstCqe = folly::coro::blockingWait(cq->pollCq());
  const auto secondCqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(firstCqe.workReqId, kFirstReadId)
      << "the read issued first should complete first";
  EXPECT_EQ(secondCqe.workReqId, kSecondReadId)
      << "the read issued second should complete behind it";
}

/*
 * Regression: a READ_RESP matching a pending read WQE must be delivered even
 * when cdmsn has advanced past the response's (tx-sequence) msn. The before-
 * window BRNR check excludes responses because a response msn is not in the
 * cdmsn sequence; without that exclusion this response would be wrongly BRNR'd
 * and the read WQE would hang.
 */
TEST_F(ReceiverTest, MatchedReadRespDeliveredDespiteAdvancedCdmsn) {
  constexpr uint32_t kReadSize = 4096;
  char readBuffer[kReadSize] = {};
  constexpr uint64_t kReadWrId = 77;

  const uint16_t readMsn =
      postReadWqe(readBuffer, kReadSize, kReadSize, kReadWrId);
  ASSERT_LT(readMsn, kFixtureCdmsn)
      << "precondition: the read's tx msn must fall before the fixture's cdmsn, "
         "or the response below is not the one this case is about";

  char payload[kReadSize];
  std::memset(payload, 'R', kReadSize);
  auto readRespPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      readMsn,
      /*tag=*/0,
      /*posn=*/0,
      payload,
      kReadSize,
      BaseHeader::Opcode::OperationType::READ_RESP);

  mockReceiver_->simulateReceivePacket(std::move(readRespPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "a matched read response must not error the QP";
  EXPECT_EQ(std::memcmp(readBuffer, payload, kReadSize), 0)
      << "the read response payload must be delivered, not BRNR'd, when cdmsn is "
         "past the response's tx msn";
  ASSERT_EQ(cq->getNumCompletedWqes(), 1) << "the read WQE should complete";
  auto cqe = folly::coro::blockingWait(cq->pollCq());
  EXPECT_EQ(cqe.status, kSuccess)
      << "the matched read response should complete the WQE successfully";
  EXPECT_EQ(cqe.workReqId, kReadWrId);
}

/*
 * A matched READ_RESP with an out-of-range posn must be rejected before
 * delivery, not delivered. The response path skips ensureAndValidateWqe, so the
 * shared posn/payload guard before delivery is what stops addressForPosnMtu
 * from CHECK-aborting the receiver on malformed peer input.
 */
TEST_F(ReceiverTest, MatchedReadRespWithOutOfRangePosnIsRejected) {
  constexpr uint32_t kReadSize = 4096; // one posn at 4096-byte MTU
  char readBuffer[kReadSize] = {};
  uint16_t readMsn = 0;
  {
    auto lockedMap = qpNumToQp_.rlock();
    auto* qp = lockedMap->at(dstQp).get();
    auto tag = qp->getTag(kMetaRoceInitialTag);
    readMsn = tag->getNextMsn();
    auto readWqe = std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kRead,
        /*workReqId=*/88,
        oneSge(readBuffer),
        qp->getMtu(),
        /*remoteAddress=*/0x1000,
        /*rkey=*/1);
    postToTag(*tag, std::move(readWqe), qp->getMtu());
  }

  char payload[kReadSize];
  std::memset(payload, 'R', kReadSize);
  // posn 1 is out of range for a single-posn (4096-byte) read.
  auto readRespPacket = buildTestDataPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      readMsn,
      /*tag=*/0,
      /*posn=*/1,
      payload,
      kReadSize,
      BaseHeader::Opcode::OperationType::READ_RESP);

  // Must not CHECK-abort; the malformed response errors the QP instead.
  mockReceiver_->simulateReceivePacket(std::move(readRespPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "a matched READ_RESP with an out-of-range posn must be rejected (tag "
         "moved to error), not delivered through a CHECK-aborting posn";
}

/*
 * End-to-end HRNR cancel test:
 * 1. Data packet arrives for a QP with no paths → Receiver sends HRNR NAK
 *    and sets rnrCancelPending on the QP.
 * 2. setupFlows() is called on the QP → rnrCancelPending is cleared and an
 *    RNR Cancel packet is scheduled.
 * 3. getNextPacketToSend() produces an RnrCancelPacket with correct fields.
 */
TEST_F(ReceiverTest, HrnrCancelOnSetupFlowsEndToEnd) {
  constexpr uint32_t noFlowsQpn = 790;
  struct ibv_qp noFlowsIbvQp{};
  auto noFlowsQp = std::make_unique<Qp>(
      noFlowsQpn, *retxTimer, *arbiter, mrs, cq.get(), &noFlowsIbvQp);
  noFlowsQp->setMtu(4096);
  noFlowsQp->addPortPair(
      folly::IPAddress("fc00::1"), folly::IPAddress("fc00::101"));
  noFlowsQp->setRemoteQpNum(srcQp);

  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(100));
  noFlowsQp->setCongestionController(std::move(congestionController));

  auto* qpRawPtr = noFlowsQp.get();
  qpNumToQp_.wlock()->emplace(noFlowsQpn, std::move(noFlowsQp));

  auto dataPacket =
      buildTestDataPacket(srcQp, noFlowsQpn, /*flowId=*/0, fsn, msn);

  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([](const folly::SocketAddress&,
                   const std::unique_ptr<folly::IOBuf>& buf) {
        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);
  ASSERT_TRUE(qpRawPtr->hasRnrCancelPending())
      << "rnrCancelPending should be set after HRNR NAK";

  LimitedTestSender hrnrMockSender(*arbiter, mockUDPSocketMaker);
  qpRawPtr->setupFlows(1, 1, &hrnrMockSender);

  EXPECT_FALSE(qpRawPtr->hasRnrCancelPending())
      << "rnrCancelPending should be cleared after setupFlows()";

  auto egressPacket = qpRawPtr->getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "RNR cancel packet should be available after setupFlows()";
  auto* rnrCancelPacket =
      dynamic_cast<RnrCancelPacket*>(egressPacket->packet.get());
  ASSERT_NE(rnrCancelPacket, nullptr) << "Packet should be an RnrCancelPacket";

  EXPECT_EQ(rnrCancelPacket->getRnrCancelHeader().getTag(), kMetaRoceInitialTag)
      << "HRNR cancel should target the initial tag";
}

TEST_F(ReceiverTest, HrnrCancelCarriesThePostedCsn) {
  constexpr uint32_t noFlowsQpn = 792;
  constexpr int kReceivesPosted = 3;
  struct ibv_qp noFlowsIbvQp{};
  auto noFlowsQp = std::make_unique<Qp>(
      noFlowsQpn, *retxTimer, *arbiter, mrs, cq.get(), &noFlowsIbvQp);
  noFlowsQp->setMtu(4096);
  noFlowsQp->addPortPair(
      folly::IPAddress("fc00::1"), folly::IPAddress("fc00::101"));
  noFlowsQp->setRemoteQpNum(srcQp);

  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(100));
  noFlowsQp->setCongestionController(std::move(congestionController));

  auto* qpRawPtr = noFlowsQp.get();
  qpNumToQp_.wlock()->emplace(noFlowsQpn, std::move(noFlowsQp));

  // Posted before the flows exist, so the cancel has a real CSN to carry.
  auto tag = qpRawPtr->getTag(kMetaRoceInitialTag);
  for (int i = 0; i < kReceivesPosted; ++i) {
    postToTag(
        *tag,
        std::make_unique<ReceiveWqe>(
            /*workReqId=*/42,
            oneSge(nullptr, 0, /*lkey=*/0),
            qpRawPtr->getMtu()),
        qpRawPtr->getMtu());
  }

  auto dataPacket =
      buildTestDataPacket(srcQp, noFlowsQpn, /*flowId=*/0, fsn, msn);
  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([](const folly::SocketAddress&,
                   const std::unique_ptr<folly::IOBuf>& buf) {
        return buf->computeChainDataLength();
      });
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);
  ASSERT_TRUE(qpRawPtr->hasRnrCancelPending())
      << "precondition: the data packet arriving before the flows exist is "
         "what arms the cancel; without it the QP coming up sends nothing for "
         "the rest of this test to inspect";

  LimitedTestSender hrnrMockSender(*arbiter, mockUDPSocketMaker);
  qpRawPtr->setupFlows(1, 1, &hrnrMockSender);

  auto egressPacket = qpRawPtr->getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  auto* rnrCancelPacket =
      dynamic_cast<RnrCancelPacket*>(egressPacket->packet.get());
  ASSERT_NE(rnrCancelPacket, nullptr);

  // Three receives from an initial CSN of zero leave two as the highest CSN
  // with a receive behind it.
  EXPECT_EQ(rnrCancelPacket->getRnrCancelHeader().getPostedCsn(), 2)
      << "the posted CSN bounds how much the peer may send before risking "
         "another RNR, so a cancel raised by the QP coming up carries the same "
         "number as one raised by a receive post; a zero here withholds three "
         "messages' worth of room the receiver already has";
}

TEST_F(ReceiverTest, RnrCancelIsHandedToTheSenderNotAppliedOnReceipt) {
  constexpr uint32_t cancelQpn = 791;
  struct ibv_qp cancelIbvQp{};
  auto cancelQp = std::make_unique<Qp>(
      cancelQpn, *retxTimer, *arbiter, mrs, cq.get(), &cancelIbvQp);
  cancelQp->setMtu(4096);
  cancelQp->addPortPair(
      folly::IPAddress("fc00::1"), folly::IPAddress("fc00::101"));
  cancelQp->setRemoteQpNum(srcQp);

  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(100));
  cancelQp->setCongestionController(std::move(congestionController));

  auto* qpRawPtr = cancelQp.get();
  qpNumToQp_.wlock()->emplace(cancelQpn, std::move(cancelQp));
  LimitedTestSender cancelMockSender(*arbiter, mockUDPSocketMaker);
  qpRawPtr->setupFlows(1, 1, &cancelMockSender);
  ASSERT_TRUE(qpRawPtr->usesSingleOwnerRxRing())
      << "the ring must exist for the receiver to defer the cancel onto it";

  qpRawPtr->enterHrnrBlock(60 * 1000 * 1000 /*60s*/);

  using OperationType = BaseHeader::Opcode::OperationType;
  const BaseHeader baseHeader =
      BaseHeader::BaseHeaderBuilder()
          .setOpcode(BaseHeader::Opcode(OperationType::RNR_CANCEL))
          .setVersion(kMetaRoceProtocolVersion)
          .setFlowId(0)
          .setDstQp(cancelQpn)
          .setSrcQp(srcQp)
          .build();
  const RnrCancelHeader rnrCancelHeader =
      RnrCancelHeader::RnrCancelHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setPostedCsn(0)
          .setFlowsAvailable(1)
          .build();
  mockReceiver_->simulateReceivePacket(
      std::make_unique<RnrCancelPacket>(baseHeader, rnrCancelHeader), kEcnECT0);

  EXPECT_EQ(qpRawPtr->getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "the reader thread must hand the cancel to the sender rather than "
         "apply it, so that it stays ordered behind any queued ack";

  // A sender visit, as the arbiter callback runs it: drain what the receiver
  // handed off, then send.
  qpRawPtr->applyHandedOffAcks();
  qpRawPtr->getNextPacketToSend();
  EXPECT_EQ(qpRawPtr->getHrnrState(), HrnrState::NORMAL_TX)
      << "the sender's visit must drain the cancel and lift the block";
}

TEST_F(ReceiverTest, RnrCancelBeforeFlowsExistIsDropped) {
  constexpr uint32_t cancelQpn = 792;
  struct ibv_qp cancelIbvQp{};
  auto cancelQp = std::make_unique<Qp>(
      cancelQpn, *retxTimer, *arbiter, mrs, cq.get(), &cancelIbvQp);
  cancelQp->setRemoteQpNum(srcQp);
  auto* qpRawPtr = cancelQp.get();
  qpNumToQp_.wlock()->emplace(cancelQpn, std::move(cancelQp));
  ASSERT_EQ(qpRawPtr->getNumTxFlows(), 0u)
      << "precondition: the QP is still being set up, so a Tag may be added";
  qpRawPtr->enterHrnrBlock(60 * 1000 * 1000 /*60s*/);

  using OperationType = BaseHeader::Opcode::OperationType;
  const BaseHeader baseHeader =
      BaseHeader::BaseHeaderBuilder()
          .setOpcode(BaseHeader::Opcode(OperationType::RNR_CANCEL))
          .setVersion(kMetaRoceProtocolVersion)
          .setFlowId(0)
          .setDstQp(cancelQpn)
          .setSrcQp(srcQp)
          .build();
  const RnrCancelHeader rnrCancelHeader =
      RnrCancelHeader::RnrCancelHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setPostedCsn(0)
          .setFlowsAvailable(1)
          .build();
  mockReceiver_->simulateReceivePacket(
      std::make_unique<RnrCancelPacket>(baseHeader, rnrCancelHeader), kEcnECT0);

  EXPECT_EQ(qpRawPtr->getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "a cancel reaching a QP that may still gain a Tag must be dropped "
         "before its tag is looked up without the lock that adds Tags";
}

TEST_F(ReceiverTest, ConstructorThrowsOnBindFailureInsteadOfAborting) {
  // The constructor's bind runs on a noexcept EventBase thread, so it marshals
  // a bind failure back to the caller rather than terminating the process.
  folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>
      emptyQpMap;

  auto failingSocketMaker = [](folly::EventBase* evb) {
    auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
    ON_CALL(*socket, bindImpl(::testing::_))
        .WillByDefault(
            ::testing::Throw(
                folly::AsyncSocketException(
                    folly::AsyncSocketException::NOT_OPEN,
                    "test: address already in use",
                    98 /*EADDRINUSE*/)));
    return std::make_pair(
        std::unique_ptr<folly::AsyncUDPSocket>(std::move(socket)), nullptr);
  };

  EXPECT_THROW(
      Receiver(
          folly::IPAddress("fc00::1"),
          emptyQpMap,
          failingSocketMaker,
          kMetaRoceBindToPort,
          kMetaRoceSendToPort,
          *pingController_),
      folly::AsyncSocketException)
      << "bind failure should propagate as an exception, not abort";
}

/*
 * Tests that setPendingAck accumulates CE counts when coalescing ACKs.
 * When a new ACK overwrites a pending one, the CE counts should be summed
 * to prevent congestion notification loss.
 */
TEST_F(ReceiverTest, SetPendingAckAccumulatesCeCount) {
  auto lockedMap = qpNumToQp_.rlock();
  auto& qp = lockedMap->at(dstQp);
  auto* rxFlow = qp->getRxFlowById(flowId);

  auto ackBuilder1 = AckHeader::AckHeaderBuilder()
                         .setCnpCE(3)
                         .setTag(0)
                         .setCumulativeFsn(5)
                         .setMaxRcvdFsn(5)
                         .setAckStatus(AckStatus::ACK)
                         .setCumulRespDeliveredMsn(0)
                         .setRateHints(0)
                         .setTimestamp(0);
  rxFlow->setPendingAck(std::move(ackBuilder1).build(), kIngressSourcePortA);

  auto ackBuilder2 = AckHeader::AckHeaderBuilder()
                         .setCnpCE(2)
                         .setTag(0)
                         .setCumulativeFsn(6)
                         .setMaxRcvdFsn(6)
                         .setAckStatus(AckStatus::ACK)
                         .setCumulRespDeliveredMsn(0)
                         .setRateHints(0)
                         .setTimestamp(0);
  rxFlow->setPendingAck(std::move(ackBuilder2).build(), kIngressSourcePortA);

  auto egressPacket = rxFlow->getNextAckToSend();
  ASSERT_NE(egressPacket, nullptr) << "a coalesced ACK should be pending";

  auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
  ASSERT_NE(ackPacket, nullptr);

  EXPECT_EQ(ackPacket->getAckHeader().getCnpCE(), 5)
      << "coalesced ACK should sum CE counts (3 + 2 = 5)";
  EXPECT_EQ(ackPacket->getAckHeader().getCumulativeFsn(), 6)
      << "coalesced ACK should carry the newer cumulative FSN";
}

/*
 * Two acks each carrying the count that asked for them can coalesce into one,
 * and their sum is past what the four-bit CNP field holds.
 */
TEST_F(ReceiverTest, SetPendingAckSaturatesACoalescedCeCount) {
  auto lockedMap = qpNumToQp_.rlock();
  auto& qp = lockedMap->at(dstQp);
  auto* rxFlow = qp->getRxFlowById(flowId);

  auto atThreshold = AckHeader::AckHeaderBuilder()
                         .setCnpCE(kCeCountAckThreshold)
                         .setTag(0)
                         .setCumulativeFsn(5)
                         .setMaxRcvdFsn(5)
                         .setAckStatus(AckStatus::ACK)
                         .setCumulRespDeliveredMsn(0)
                         .setRateHints(0)
                         .setTimestamp(0);
  rxFlow->setPendingAck(std::move(atThreshold).build(), kIngressSourcePortA);

  auto alsoAtThreshold = AckHeader::AckHeaderBuilder()
                             .setCnpCE(kCeCountAckThreshold)
                             .setTag(0)
                             .setCumulativeFsn(6)
                             .setMaxRcvdFsn(6)
                             .setAckStatus(AckStatus::ACK)
                             .setCumulRespDeliveredMsn(0)
                             .setRateHints(0)
                             .setTimestamp(0);
  rxFlow->setPendingAck(
      std::move(alsoAtThreshold).build(), kIngressSourcePortA);

  auto egressPacket = rxFlow->getNextAckToSend();
  ASSERT_NE(egressPacket, nullptr) << "a coalesced ACK should be pending";
  auto* ackPacket = dynamic_cast<AckPacket*>(egressPacket->packet.get());
  ASSERT_NE(ackPacket, nullptr);

  EXPECT_EQ(ackPacket->getAckHeader().getCnpCE(), kMaxCnpCeCount)
      << "sixteen marks reach the wire as none through a four-bit field, so "
         "the sender would be told of no congestion in the middle of it";
}

/*
 * Tests the full ACK slot lifecycle: empty → set → drain → empty.
 */
TEST_F(ReceiverTest, AckSlotLifecycle) {
  auto lockedMap = qpNumToQp_.rlock();
  auto& qp = lockedMap->at(dstQp);
  auto* rxFlow = qp->getRxFlowById(flowId);

  EXPECT_EQ(rxFlow->getNextAckToSend(), nullptr)
      << "fresh slot should be empty";

  auto ackBuilder = AckHeader::AckHeaderBuilder()
                        .setCnpCE(0)
                        .setTag(0)
                        .setCumulativeFsn(10)
                        .setMaxRcvdFsn(10)
                        .setAckStatus(AckStatus::ACK)
                        .setCumulRespDeliveredMsn(0)
                        .setRateHints(0)
                        .setTimestamp(0);
  rxFlow->setPendingAck(std::move(ackBuilder).build(), kIngressSourcePortA);

  auto first = rxFlow->getNextAckToSend();
  ASSERT_NE(first, nullptr) << "drain should return the pending ACK";

  EXPECT_EQ(rxFlow->getNextAckToSend(), nullptr)
      << "slot should be empty after drain";
}

/*
 * Tests the NAK Invalid Request mechanism when the receiver gets a packet
 * with an unsupported opcode (spec 5.5).
 *
 * ATOMIC_RESERVED_0 is a defined opcode that is not implemented.
 * The receiver must send NAK_INVALID_REQUEST back to the sender.
 */
TEST_F(ReceiverTest, SendNakInvalidRequestForUnsupportedOpcode) {
  auto dataPacket = buildTestUnsupportedPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      BaseHeader::Opcode::OperationType::ATOMIC_RESERVED_0);

  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([](const folly::SocketAddress&,
                   const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);
        AckHeader ackHeader(cursor);

        EXPECT_EQ(ackHeader.getAckStatus(), AckStatus::NAK_INVALID_REQUEST)
            << "spec 5.5 requires NAK_INVALID_REQUEST for unsupported opcodes";

        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "an unsupported opcode must move the QP to the error state";
}

TEST_F(ReceiverTest, ErrorNakLeavesTheReservedRateHintZero) {
  auto dataPacket = buildTestUnsupportedPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      BaseHeader::Opcode::OperationType::ATOMIC_RESERVED_0);

  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([](const folly::SocketAddress&,
                   const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);
        AckHeader ackHeader(cursor);

        EXPECT_EQ(ackHeader.getRateHints(), 0u)
            << "the rate hint is reserved on an error NAK, so sending a rate "
               "here would occupy bits a later revision may define";

        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);
}

/*
 * An undefined data opcode (BTH 110, op-type 0b00011, wire 0xC3) -- absent from
 * OperationType -- must draw a NAK-Invalid-Request and move the QP to the error
 * state.
 */
TEST_F(ReceiverTest, SendNakAndErrorQpForUndefinedOpcode) {
  auto dataPacket = buildTestUnsupportedPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/0,
      static_cast<BaseHeader::Opcode::OperationType>(0b00011));

  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([](const folly::SocketAddress&,
                   const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);
        AckHeader ackHeader(cursor);
        EXPECT_EQ(ackHeader.getAckStatus(), AckStatus::NAK_INVALID_REQUEST)
            << "an undefined opcode must draw NAK_INVALID_REQUEST";
        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "an undefined opcode must move the QP to the error state";
}

/*
 * An unsupported opcode whose tag is not present on the QP is NAK'd but leaves
 * the QP operational -- with no matching tag, there is nothing to move to
 * error.
 */
TEST_F(ReceiverTest, NakButNoErrorForUnsupportedOpcodeWithUnknownTag) {
  auto dataPacket = buildTestUnsupportedPacket(
      srcQp,
      dstQp,
      flowId,
      fsn,
      msn,
      /*tag=*/5,
      static_cast<BaseHeader::Opcode::OperationType>(0b00011));

  EXPECT_CALL(*mockSocket_, write(_, _))
      .WillOnce([](const folly::SocketAddress&,
                   const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());
        BaseHeader baseHeader(cursor);
        AckHeader ackHeader(cursor);
        EXPECT_EQ(ackHeader.getAckStatus(), AckStatus::NAK_INVALID_REQUEST)
            << "an unsupported opcode must draw NAK_INVALID_REQUEST";
        return buf->computeChainDataLength();
      });

  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "with no matching tag there is nothing to error; the QP stays alive";
}

/*
 * A packet with a non-MetaRoCE BTH prefix (e.g., RoCEv2 RC = 0b000) is
 * silently dropped: no NAK is sent and no QP state change occurs.
 */
TEST_F(ReceiverTest, NonMetaRoceBthPrefixSilentlyDropped) {
  auto dataPacket = buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn);

  // Overwrite the opcode to a RoCEv2 RC BTH prefix (000) with SEND lower bits.
  dataPacket->getBaseHeader().setOpcode(BaseHeader::Opcode(0x00));

  EXPECT_CALL(*mockSocket_, write(_, _)).Times(0);
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "a non-MetaRoCE BTH prefix should not affect QP state";
}

/*
 * A packet with an undefined BTH prefix (e.g., 0b001) is also silently
 * dropped, not NAK'd.
 */
TEST_F(ReceiverTest, UndefinedBthPrefixSilentlyDropped) {
  auto dataPacket = buildTestDataPacket(srcQp, dstQp, flowId, fsn, msn);

  // BTH prefix 001 (0x20) is undefined.
  dataPacket->getBaseHeader().setOpcode(BaseHeader::Opcode(0x20));

  EXPECT_CALL(*mockSocket_, write(_, _)).Times(0);
  mockReceiver_->simulateReceivePacket(std::move(dataPacket), kEcnECT0);

  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "an undefined BTH prefix should not affect QP state";
}

// Cases asserting which UDP source port a drained ACK leaves from. Each
// rebuilds the QP on a recording socket maker and posts a receive per flow.
class AckSourcePortTest : public ReceiverTest {
 protected:
  // `extraSockets` join the setup's socket on the flow's local address.
  void rebuild(
      unsigned socketLimit,
      unsigned extraSockets,
      uint32_t rxFlowCount = 1) {
    qpNumToQp_.wlock()->clear();
    mockSender_.reset();
    buildQp(nullptr, ports_.socketMaker(), socketLimit, rxFlowCount);
    for (unsigned i = 0; i < extraSockets; ++i) {
      std::ignore =
          mockSender_->getSocketIdFromAddress(folly::IPAddress("fc00::1"));
    }
    for (uint32_t i = 0; i < rxFlowCount; ++i) {
      postReceiveWqe(recvBuffers_[i].data(), kBufferSize);
    }
  }

  // Injects flow `id`'s SEND as if from `sourcePort`. Each flow has its own
  // message, so flow 1 takes the msn and csn after flow 0's.
  void
  injectSend(uint16_t sourcePort, uint8_t tos = kEcnECT0, uint16_t id = 0) {
    mockReceiver_->simulateReceivePacket(
        buildTestDataPacket(
            srcQp,
            dstQp,
            id,
            fsn,
            static_cast<uint16_t>(msn + id),
            /*tag=*/0,
            /*posn=*/0,
            payload_.data(),
            kBufferSize,
            BaseHeader::Opcode::OperationType::SEND,
            /*maybeImmData=*/std::nullopt,
            /*padding=*/0,
            static_cast<uint16_t>(kFixtureRxCsn + id)),
        tos,
        sourcePort);
  }

  // The QP map is untouched while a case runs, so the pointer stays valid
  // after the lookup's lock is released.
  RxFlow* rxFlowById(uint16_t id) {
    return qpNumToQp_.rlock()->at(dstQp)->getRxFlowById(id);
  }

  uint16_t ackSourcePortOf(const std::unique_ptr<EgressPacket>& egress) const {
    return ports_.portBySocket.at(egress->socketToSendOn->getSocket());
  }

  // The UDP source port flow `id`'s next ACK leaves from.
  uint16_t selectedAckSourcePort(uint16_t id = 0) {
    return ports_.portBySocket.at(mockSender_->getSocketById(
        rxFlowById(id)->getSocketId(), EgressPacket::Kind::Ack));
  }

  // Injects flow `id`'s SEND as if from `sourcePort` and returns the UDP
  // source port of the ACK for it, or 0 when no ACK was left to drain.
  uint16_t ackSourcePortAfterSend(uint16_t sourcePort, uint16_t id = 0) {
    injectSend(sourcePort, kEcnECT0, id);
    const auto egress = rxFlowById(id)->getNextAckToSend();
    EXPECT_NE(egress, nullptr)
        << "every SEND, retransmitted or not, must be acknowledged";
    return egress == nullptr ? 0 : ackSourcePortOf(egress);
  }

  static const AckHeader& ackHeaderOf(
      const std::unique_ptr<EgressPacket>& egress) {
    return CHECK_NOTNULL(dynamic_cast<AckPacket*>(egress->packet.get()))
        ->getAckHeader();
  }

  SourcePortRecorder ports_;
  static constexpr uint32_t kBufferSize = 64;
  std::array<std::array<char, kBufferSize>, 2> recvBuffers_{};
  std::array<char, kBufferSize> payload_{};
};

/*
 * After the sender repaths, retransmitted data arrives from a new UDP source
 * port. The ACK for it must leave from a different UDP source port,
 * even though the retransmission's FSN was already received.
 */
TEST_F(AckSourcePortTest, MovesOnDuplicateFromNewSourcePort) {
  rebuild(LimitedTestSender::kMaxSocketsNoLimit, /*extraSockets=*/1);
  const uint16_t firstPort = ackSourcePortAfterSend(kIngressSourcePortA);

  // The same FSN again: the peer's RTO repath retransmits what the lost ACK
  // covered, from the new source port.
  injectSend(kIngressSourcePortB);
  const auto second = rxFlowById(flowId)->getNextAckToSend();
  ASSERT_NE(second, nullptr)
      << "a retransmission must be acknowledged even though its FSN was received";
  EXPECT_NE(ackSourcePortOf(second), firstPort)
      << "the ACK for data from a new source port must take a "
         "different path back";
  EXPECT_EQ(ackHeaderOf(second).getAckStatus(), AckStatus::ACK)
      << "the moved ACK acknowledges an ordinary retransmission";
  EXPECT_EQ(ackHeaderOf(second).getCumulativeFsn(), fsn)
      << "the move must not disturb the ACK's coverage";
}

TEST_F(AckSourcePortTest, KeepsSourcePortWhileIngressSourcePortStable) {
  rebuild(LimitedTestSender::kMaxSocketsNoLimit, /*extraSockets=*/1);
  const uint16_t firstPort = ackSourcePortAfterSend(kIngressSourcePortA);

  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(ackSourcePortAfterSend(kIngressSourcePortA), firstPort)
        << "with the ingress source port unchanged, the ACK keeps its path";
  }
}

/*
 * With a drain at each step, every ingress source-port change moves the ACK.
 * Two sockets cycle back to the first on the third drain.
 */
TEST_F(AckSourcePortTest, MovesAtEachDrainedChange) {
  rebuild(/*socketLimit=*/2, /*extraSockets=*/1);
  const uint16_t firstPort = ackSourcePortAfterSend(kIngressSourcePortA);
  const uint16_t secondPort = ackSourcePortAfterSend(kIngressSourcePortB);
  EXPECT_NE(secondPort, firstPort)
      << "the first change must move the ACK source port";

  const uint16_t thirdPort = ackSourcePortAfterSend(kIngressSourcePortA);
  EXPECT_NE(thirdPort, secondPort)
      << "the second change must move the ACK source port again";
  EXPECT_EQ(thirdPort, firstPort)
      << "with two sockets, the cycle returns to the first";
}

TEST_F(AckSourcePortTest, CoalescedChangesMoveOnce) {
  rebuild(LimitedTestSender::kMaxSocketsNoLimit, /*extraSockets=*/1);
  const uint16_t setupPort = selectedAckSourcePort();

  injectSend(kIngressSourcePortA, kEcnCE);
  injectSend(kIngressSourcePortB, kEcnCE);
  injectSend(kIngressSourcePortA, kEcnCE);

  const auto only = rxFlowById(flowId)->getNextAckToSend();
  ASSERT_NE(only, nullptr) << "the three packets must leave one ACK to drain";
  EXPECT_NE(ackSourcePortOf(only), setupPort)
      << "a change that returns to the first source port still moves the ACK";
  EXPECT_EQ(ackHeaderOf(only).getCnpCE(), 3)
      << "every coalesced packet's CE mark must survive";
  EXPECT_EQ(ackHeaderOf(only).getCumulativeFsn(), fsn)
      << "the emitted ACK must describe the latest packet";
  EXPECT_EQ(rxFlowById(flowId)->getNextAckToSend(), nullptr)
      << "coalesced publications drain as a single ACK";
}

TEST_F(AckSourcePortTest, OtherFlowKeepsItsSourcePort) {
  rebuild(
      LimitedTestSender::kMaxSocketsNoLimit,
      /*extraSockets=*/1,
      /*rxFlowCount=*/2);
  const uint16_t firstFlowPort =
      ackSourcePortAfterSend(kIngressSourcePortA, /*id=*/0);
  const uint16_t secondFlowPort =
      ackSourcePortAfterSend(kIngressSourcePortA, /*id=*/1);

  ASSERT_NE(
      ackSourcePortAfterSend(kIngressSourcePortB, /*id=*/0), firstFlowPort)
      << "precondition: the first flow's source-port change moves its ACK";
  EXPECT_EQ(
      ackSourcePortAfterSend(kIngressSourcePortA, /*id=*/1), secondFlowPort)
      << "another flow's source-port change must not move this flow's ACK";
}

TEST_F(AckSourcePortTest, CreatesSocketWhenAloneWithRoom) {
  rebuild(LimitedTestSender::kMaxSocketsNoLimit, /*extraSockets=*/0);
  ASSERT_EQ(ports_.bound.size(), 2u)
      << "precondition: setup leaves one socket id, a data/ACK pair";
  const uint16_t firstPort = ackSourcePortAfterSend(kIngressSourcePortA);

  EXPECT_NE(ackSourcePortAfterSend(kIngressSourcePortB), firstPort)
      << "with no other socket, the move must create one";
  EXPECT_EQ(
      mockSender_->getAddressFromSocketId(rxFlowById(flowId)->getSocketId()),
      folly::IPAddress("fc00::1"))
      << "the ACK keeps the flow's local address while its source port moves";
}

TEST_F(AckSourcePortTest, KeepsSourcePortAtSocketLimit) {
  rebuild(/*socketLimit=*/1, /*extraSockets=*/0);
  ASSERT_EQ(ports_.bound.size(), 2u)
      << "precondition: the limit leaves one socket id";
  const uint16_t firstPort = ackSourcePortAfterSend(kIngressSourcePortA);

  EXPECT_EQ(ackSourcePortAfterSend(kIngressSourcePortB), firstPort)
      << "with no other socket and no room for one, the ACK keeps its socket";
}

// Under ThreadSanitizer, this checks that the publisher and the drainer share
// the source-port change flag without a race.
TEST_F(AckSourcePortTest, ConcurrentPublishAndDrainShareChangeFlag) {
  rebuild(LimitedTestSender::kMaxSocketsNoLimit, /*extraSockets=*/1);
  // Deliver first, so every racing packet is a duplicate re-ack; delivery
  // itself is not what races here.
  ASSERT_NE(ackSourcePortAfterSend(kIngressSourcePortA), 0)
      << "precondition: the first packet must leave an ACK to drain";

  constexpr int kPackets = 200;
  // A gate per thread: a Baton takes a single waiter.
  folly::Baton<> publisherStart;
  folly::Baton<> drainerStart;
  std::atomic<int> published{0};
  std::atomic<bool> publisherDone{false};
  std::atomic<int> drained{0};
  std::atomic<int> badHeaders{0};

  std::thread publisher([&] {
    publisherStart.wait();
    for (int i = 0; i < kPackets; ++i) {
      injectSend(i % 2 == 0 ? kIngressSourcePortA : kIngressSourcePortB);
      ++published;
    }
    publisherDone.store(true, std::memory_order_release);
  });

  // The slot coalesces, so fewer drains than publications is expected; what
  // must hold is that every drained ACK is well-formed and draining ends.
  std::thread drainer([&] {
    drainerStart.wait();
    bool done = false;
    std::unique_ptr<EgressPacket> egress;
    do {
      // Read completion before the slot, so the drain that ends the loop
      // follows the final publication.
      done = publisherDone.load(std::memory_order_acquire);
      egress = rxFlowById(flowId)->getNextAckToSend();
      if (egress != nullptr) {
        const auto& header = ackHeaderOf(egress);
        if (header.getCumulativeFsn() != fsn || header.getMaxRcvdFsn() != fsn ||
            egress->socketToSendOn == nullptr) {
          ++badHeaders;
        }
        ++drained;
      }
    } while (!done || egress != nullptr);
  });

  publisherStart.post();
  drainerStart.post();
  publisher.join();
  drainer.join();

  EXPECT_EQ(published.load(), kPackets)
      << "the publisher must have run, or the drainer raced nobody";
  EXPECT_GT(drained.load(), 0)
      << "the racing publications must leave ACKs to drain";
  EXPECT_EQ(badHeaders.load(), 0)
      << "every drained ACK must carry the retransmitted FSN on a socket";
  EXPECT_EQ(rxFlowById(flowId)->getNextAckToSend(), nullptr)
      << "the slot must be empty once the race is over";
}

} // namespace libsoftmetaroce
