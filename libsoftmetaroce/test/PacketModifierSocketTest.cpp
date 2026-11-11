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

#include <folly/String.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/net/NetOps.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/EsunEncapsulation.h>
#include <libsoftmetaroce/EthernetEncapsulation.h>
#include <libsoftmetaroce/HighResTimer.h>
#include <libsoftmetaroce/PacketModifierSocket.h>
#include <libsoftmetaroce/PacketSocket.h>
#include <libsoftmetaroce/UdpEncapsulation.h>
#include <libsoftmetaroce/codec/MetaRoceCodec.h>
#include <libsoftmetaroce/codec/Packet.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/modifier/PacingModifier.h>
#include <libsoftmetaroce/modifier/ReorderModifier.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <functional>
#include <optional>

using namespace testing;
namespace libsoftmetaroce {

std::unique_ptr<DataPacket> makeDataPacket() {
  auto base =
      BaseHeader::BaseHeaderBuilder()
          .setOpcode(
              BaseHeader::Opcode(BaseHeader::Opcode::OperationType::SEND))
          .setVersion(1)
          .setFlowId(0)
          .setDstQp(1)
          .setSrcQp(1)
          .build();
  auto data = DataHeader::DataHeaderBuilder()
                  .setFsn(0)
                  .setUna(0)
                  .setAckRequest(false)
                  .setPosn(0)
                  .setMsn(0)
                  .setCsn(0)
                  .setTimestamp(0)
                  .build();
  return std::make_unique<DataPacket>(std::move(base), std::move(data));
}

// Distinct sentinel values used to confirm each RnrCancelHeader field
// round-trips through serialization and parsing.
constexpr uint8_t kRnrCancelTag = 7;
constexpr uint16_t kRnrCancelPostedCsn = 42;
constexpr uint8_t kRnrCancelFlowsAvailable = 3;

std::unique_ptr<RnrCancelPacket> makeRnrCancelPacket() {
  auto base =
      BaseHeader::BaseHeaderBuilder()
          .setOpcode(
              BaseHeader::Opcode(BaseHeader::Opcode::OperationType::RNR_CANCEL))
          .setVersion(1)
          .setFlowId(0)
          .setDstQp(1)
          .setSrcQp(1)
          .build();
  auto rnrCancel = RnrCancelHeader::RnrCancelHeaderBuilder()
                       .setTag(kRnrCancelTag)
                       .setPostedCsn(kRnrCancelPostedCsn)
                       .setFlowsAvailable(kRnrCancelFlowsAvailable)
                       .build();
  return std::make_unique<RnrCancelPacket>(
      std::move(base), std::move(rnrCancel));
}

// Fake wire beneath PacketModifierSocket: records the framed buffers written
// down to it, and injects frames back up to the registered read callback.
class CapturingPacketSocket : public PacketSocket {
 public:
  void resumeRead(ReadCallback* callback) override {
    readCallback = callback;
  }
  void pauseRead() override {}
  ssize_t write(
      const folly::SocketAddress& dest,
      std::unique_ptr<folly::IOBuf> data) override {
    data->coalesce();
    captured.push_back({dest, std::move(data)});
    return static_cast<ssize_t>(captured.back().data->length());
  }
  ssize_t writeGSO(
      const folly::SocketAddress& dest,
      std::unique_ptr<folly::IOBuf> data,
      int gsoSegmentSize) override {
    data->coalesce();
    ++gsoCalls;
    gsoSegSize = gsoSegmentSize;
    captured.push_back({dest, std::move(data)});
    return static_cast<ssize_t>(captured.back().data->length());
  }
  void bind(const folly::SocketAddress&) override {}
  void close() override {}
  int fd() const override {
    return -1;
  }
  void setRcvBuf(int) override {}
  int getRcvBuf() override {
    return 0;
  }
  // Report GSO available so getGSO() reflects PacketModifierSocket's own latch.
  int getGSO() noexcept override {
    return 1;
  }
  bool supportsWritem() const noexcept override {
    return writemSupported;
  }
  int writem(
      folly::Range<const folly::SocketAddress*> addrs,
      const std::unique_ptr<folly::IOBuf>* bufs,
      size_t count) override {
    ++writemCalls;
    writemCount = count;
    for (size_t i = 0; i < count; ++i) {
      auto data = bufs[i]->clone();
      data->coalesce();
      captured.push_back({addrs[i], std::move(data)});
    }
    if (writemResult.has_value()) {
      errno = writemErrno;
      return *writemResult;
    }
    return static_cast<int>(count);
  }

  struct CapturedWrite {
    folly::SocketAddress dest;
    std::unique_ptr<folly::IOBuf> data;
  };
  void inject(
      std::unique_ptr<folly::IOBuf> data,
      const folly::SocketAddress& source,
      uint8_t tos) {
    readCallback->onPacketData(std::move(data), source, tos);
  }

  ReadCallback* readCallback{nullptr};
  std::vector<CapturedWrite> captured;
  int gsoCalls{0};
  int gsoSegSize{0};
  int writemCalls{0};
  size_t writemCount{0};
  bool writemSupported{true};
  // Set to force a short send or a failure instead of accepting every datagram.
  std::optional<int> writemResult;
  int writemErrno{0};
};

class MetadataEncapsulation final : public Encapsulation {
 public:
  explicit MetadataEncapsulation(std::optional<uint8_t> ecn) : ecn_(ecn) {}

  size_t frameOutgoing(folly::IOBufQueue&, std::optional<uint32_t>) noexcept
      override {
    return 0;
  }

  std::optional<IngressMetadata> deframeIncoming(
      std::unique_ptr<folly::IOBuf>&) noexcept override {
    return IngressMetadata{.ecn = ecn_};
  }

  size_t payloadSize(const folly::io::Cursor& cursor) const noexcept override {
    return cursor.totalLength();
  }

 private:
  std::optional<uint8_t> ecn_;
};

class CapturingReadCallback final : public PacketModifierSocket::ReadCallback {
 public:
  void onDataAvailable(
      std::unique_ptr<IngressPacket> ingressPacket) noexcept override {
    captured = std::move(ingressPacket);
  }

  std::unique_ptr<IngressPacket> captured;
};

std::unique_ptr<folly::IOBuf> serializePacket(const Packet& packet) {
  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);
  return bufferQueue.move();
}

class PacketModifierSocketTest : public ::testing::Test {
 protected:
  void SetUp() override {
    evb_ = std::make_unique<folly::EventBase>();

    auto socketMaker = [](folly::EventBase* evb) {
      return std::make_pair(
          std::make_unique<folly::AsyncUDPSocket>(evb),
          std::unique_ptr<Modifier>(nullptr));
    };

    socket_ = std::make_unique<PacketModifierSocket>(evb_.get(), socketMaker);
  }

  void TearDown() override {
    if (socket_) {
      socket_->close();
    }
  }

  std::unique_ptr<folly::EventBase> evb_;
  std::unique_ptr<PacketModifierSocket> socket_;
};

const unsigned char kRealMetaroceDataPacket[] = {
    0xc0, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x60, 0x72, 0x80, 0x4b, 0x00, 0x00, 0x00, 0x00};

class PacketModifierSocketIngressTest : public ::testing::Test {
 protected:
  void injectPacket(std::optional<uint8_t> framingEcn, uint8_t socketTos) {
    auto packetSocket = std::make_unique<CapturingPacketSocket>();
    auto* packetSocketPtr = packetSocket.get();
    socket_ = std::make_unique<PacketModifierSocket>(
        &evb_,
        std::move(packetSocket),
        std::make_unique<MetadataEncapsulation>(framingEcn),
        std::make_unique<MetaRoceCodec>());
    socket_->resumeRead(&callback_);
    EXPECT_NE(packetSocketPtr->readCallback, nullptr)
        << "resumeRead should install the receive callback on the packet socket";
    if (!packetSocketPtr->readCallback) {
      return;
    }

    packetSocketPtr->readCallback->onPacketData(
        serializePacket(*makeDataPacket()),
        folly::SocketAddress("2001:db8::1", 2766),
        socketTos);
  }

  folly::EventBase evb_;
  CapturingReadCallback callback_;
  std::unique_ptr<PacketModifierSocket> socket_;
};

TEST_F(PacketModifierSocketIngressTest, MetadataEcnPreservesSocketDscp) {
  constexpr uint8_t kSocketTos = 0xa8;
  injectPacket(0x03, kSocketTos);

  ASSERT_NE(callback_.captured, nullptr)
      << "a valid packet should reach the upper receive callback";
  EXPECT_EQ(callback_.captured->tos, 0xab)
      << "framing ECN should replace only the socket TOS ECN bits";
}

TEST_F(PacketModifierSocketIngressTest, MissingMetadataEcnPreservesSocketTos) {
  constexpr uint8_t kSocketTos = 0xa9;
  injectPacket(std::nullopt, kSocketTos);

  ASSERT_NE(callback_.captured, nullptr)
      << "a valid packet should reach the upper receive callback";
  EXPECT_EQ(callback_.captured->tos, kSocketTos)
      << "absent framing ECN should preserve the full socket TOS byte";
}

TEST_F(PacketModifierSocketTest, ParsePacketFromBufferDropsCorruptedData) {
  // 8 bytes is short of the base header.
  auto truncatedBuffer = folly::IOBuf::copyBuffer(kRealMetaroceDataPacket, 8);

  std::unique_ptr<Packet> result = nullptr;
  EXPECT_NO_THROW(result = socket_->parsePacketFromBuffer(truncatedBuffer));
  EXPECT_EQ(result, nullptr) << "a truncated frame should be dropped";
}

TEST_F(PacketModifierSocketTest, ParsePacketFromBufferParsesValidData) {
  auto completeBuffer = folly::IOBuf::copyBuffer(
      kRealMetaroceDataPacket, sizeof(kRealMetaroceDataPacket));

  std::unique_ptr<Packet> result = nullptr;
  EXPECT_NO_THROW(result = socket_->parsePacketFromBuffer(completeBuffer));
  EXPECT_NE(result, nullptr);

  // Verify the packet is correctly parsed as a data packet
  EXPECT_TRUE(result->getBaseHeader().getOpcode().isData());
}

TEST(PacketModifierSocketEsunTest, HeaderEcnOverridesSocketEcnBits) {
  folly::EventBase evb;
  auto capturingSocket = std::make_unique<CapturingPacketSocket>();
  auto* packetSocket = capturingSocket.get();
  const auto srcMac = folly::MacAddress("02:00:00:00:00:01");
  const auto dstMac = folly::MacAddress("02:00:00:00:00:02");
  EsunEncapsulation sender{
      srcMac, dstMac, EsunHeader(EsunHeader::Fields{.ecn = 1})};
  auto encapsulation = std::make_unique<EsunEncapsulation>(
      srcMac, dstMac, EsunHeader(EsunHeader::Fields{.ecn = 3}));
  PacketModifierSocket socket(
      &evb,
      std::move(capturingSocket),
      std::move(encapsulation),
      std::make_unique<MetaRoceCodec>());
  CapturingReadCallback callback;
  socket.resumeRead(&callback);

  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  auto packet = makeDataPacket();
  folly::io::QueueAppender appender(&queue, packet->getSizeBytes());
  packet->serialize(appender);
  sender.frameOutgoing(queue, std::nullopt);
  packetSocket->inject(
      queue.move(), folly::SocketAddress("::1", 0), /*tos=*/0xa2);

  ASSERT_NE(callback.captured, nullptr)
      << "a valid ESUN packet should reach the socket read callback";
  EXPECT_EQ(callback.captured->tos, 0xa1)
      << "the received frame's ECN should replace only the socket TOS low two "
         "bits, and should come from the frame rather than the receiving "
         "encapsulation's transmit configuration";
}

// A serialized RNR-cancel packet must parse back into an RnrCancelPacket;
// otherwise the sender drops it and stalls on the BRNR backoff timer until the
// QP watchdog fires. Guards the parser branch that decodes RNR_CANCEL so the
// receive path's dynamic_cast to RnrCancelPacket has a packet to act on.
TEST_F(PacketModifierSocketTest, ParsePacketFromBufferParsesRnrCancel) {
  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  auto packet = makeRnrCancelPacket();
  folly::io::QueueAppender appender(&bufferQueue, packet->getSizeBytes());
  packet->serialize(appender);
  auto buffer = bufferQueue.move();

  std::unique_ptr<Packet> result = nullptr;
  EXPECT_NO_THROW(result = socket_->parsePacketFromBuffer(buffer));
  ASSERT_NE(result, nullptr)
      << "RNR-cancel packets must parse rather than being dropped at the socket";

  auto* rnrCancelPacket = dynamic_cast<RnrCancelPacket*>(result.get());
  ASSERT_NE(rnrCancelPacket, nullptr)
      << "parsed packet must be an RnrCancelPacket so the receive path's "
         "dynamic_cast in Receiver succeeds instead of dereferencing null";
  EXPECT_TRUE(result->getBaseHeader().getOpcode().isRnrCancel())
      << "the parsed opcode should round-trip as RNR_CANCEL";

  const auto& header = rnrCancelPacket->getRnrCancelHeader();
  EXPECT_EQ(header.getTag(), kRnrCancelTag)
      << "tag should round-trip through serialization";
  EXPECT_EQ(header.getPostedCsn(), kRnrCancelPostedCsn)
      << "postedCsn should round-trip through serialization";
  EXPECT_EQ(header.getFlowsAvailable(), kRnrCancelFlowsAvailable)
      << "flowsAvailable should round-trip through serialization";
}

// Over UDP the mCRC directly follows a BRNR-NAK's RNR bit vector.
TEST_F(PacketModifierSocketTest, AnAbbreviatedRnrVectorDoesNotTakeInTheMcrc) {
  constexpr size_t kRnrVectorBytesSent = 28;
  constexpr size_t kRnrVectorBytes = AckHeader::kAckTrackerBits / 8;
  const BaseHeader base =
      BaseHeader::BaseHeaderBuilder()
          .setOpcode(
              BaseHeader::Opcode(BaseHeader::Opcode::OperationType::SEL_ACK))
          .setVersion(1)
          .setFlowId(0)
          .setDstQp(1)
          .setSrcQp(1)
          .build();
  constexpr size_t kRetainedRnrBit = 5;
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(kRetainedRnrBit);
  const AckHeader brnr = AckHeader::AckHeaderBuilder()
                             .setCumulativeFsn(100)
                             .setMaxRcvdFsn(110)
                             .setTimestamp(5678)
                             .setAckStatus(AckStatus::BRNR)
                             .setRnrBackoffTimeExp(3)
                             .setRnrBitVector(rnrBitVector)
                             .build();
  const AckPacket packet(base, brnr);
  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);
  auto buffer = bufferQueue.move();
  buffer->coalesce();
  buffer->trimEnd(kRnrVectorBytes - kRnrVectorBytesSent);
  // Stands in for the mCRC: ones, so any of it read as RNR bits shows.
  buffer->appendToChain(folly::IOBuf::copyBuffer(std::string(4, '\xff')));

  std::unique_ptr<Packet> result = nullptr;
  ASSERT_NO_THROW(result = socket_->parsePacketFromBuffer(buffer))
      << "the parser should accept a BRNR-NAK whose RNR vector is under 32 bytes";
  auto* ackPacket = dynamic_cast<AckPacket*>(result.get());
  ASSERT_NE(ackPacket, nullptr);
  const auto& rnrBits = ackPacket->getAckHeader().getRnrBitVector();
  EXPECT_TRUE(rnrBits.getBit(kRetainedRnrBit))
      << "the parser should keep an RNR bit the vector carries, or the sender "
         "never learns that frame was refused";
  for (size_t bit = kRnrVectorBytesSent * 8; bit < AckHeader::kAckTrackerBits;
       ++bit) {
    EXPECT_FALSE(rnrBits.getBit(bit))
        << "the parser should read no RNR bit from the mCRC behind the "
           "vector; bit "
        << bit << " would refuse a frame the peer never refused";
  }
}

// A data packet with an undefined opcode (op-type 0b00011, wire 0xC3) must
// parse into an UnsupportedPacket so the responder can reject it with a NAK.
TEST_F(PacketModifierSocketTest, ParsePacketFromBufferForwardsUndefinedOpcode) {
  // Same layout as a valid SEND data packet, but with the opcode byte set to
  // the undefined op-type 0b00011 (wire 0xC3).
  auto buffer = folly::IOBuf::copyBuffer(
      kRealMetaroceDataPacket, sizeof(kRealMetaroceDataPacket));
  buffer->writableData()[0] = 0xC3;

  std::unique_ptr<Packet> result = nullptr;
  EXPECT_NO_THROW(result = socket_->parsePacketFromBuffer(buffer));
  ASSERT_NE(result, nullptr)
      << "an undefined data opcode must be forwarded so the responder can NAK "
         "it";
  EXPECT_NE(dynamic_cast<UnsupportedPacket*>(result.get()), nullptr)
      << "an undefined opcode must parse to an UnsupportedPacket so the "
         "responder rejects it by type, not by re-deriving it from the opcode";
}

TEST_F(PacketModifierSocketTest, DontFragmentIgnoresPathMtuOnIpv6) {
  socket_->bind(folly::SocketAddress("::1", 0));

  socket_->setDontFragment();

  int mode = -1;
  socklen_t optLen = sizeof(mode);
  ASSERT_EQ(
      folly::netops::getsockopt(
          CHECK_NOTNULL(socket_->getSocket())->getNetworkSocket(),
          IPPROTO_IPV6,
          IPV6_MTU_DISCOVER,
          &mode,
          &optLen),
      0)
      << "precondition: the socket's path MTU discovery mode is readable: "
      << folly::errnoStr(errno);
  EXPECT_EQ(mode, IPV6_PMTUDISC_PROBE)
      << "a router must drop a packet larger than its next link's MTU; the "
         "sending host neither fragments it nor shrinks later packets after "
         "a Packet Too Big";
}

// Test that destroying a PacketModifierSocket with pending timer callbacks
// does not cause use-after-free. This tests the member destruction order:
// modifier_ must be destroyed before sendPacketFn_ to ensure timer callbacks
// are cancelled before the function they reference is destroyed.
TEST(PacketModifierSocketDestructionTest, DestroyWithPendingTimerCallbacks) {
  auto evb = std::make_unique<folly::EventBase>();

  // Create a ReorderModifier with a time-based delay to ensure packets are
  // held in the modifier's timer queue until the socket is destroyed.
  constexpr uint32_t kDelayUs = 1000; // 1ms
  auto modifierConfig = std::make_unique<ReorderModifierConfig>(
      ModifierConfig::ModifierFilter(
          ModifierConfig::ModifierFilter::Direction::Tx,
          ModifierConfig::ModifierFilter::PacketType::Data,
          folly::F14FastMap<std::string, FieldConditions>{}),
      /*count=*/9999,
      ReorderModifierConfig::Distance(
          kDelayUs, ReorderModifierConfig::Distance::Unit::TimeUs));

  auto socketMaker = [&modifierConfig](folly::EventBase* evb) {
    return std::make_pair(
        std::make_unique<folly::AsyncUDPSocket>(evb),
        std::make_unique<ReorderModifier>(std::move(modifierConfig)));
  };

  auto socket = std::make_unique<PacketModifierSocket>(evb.get(), socketMaker);
  socket->bind(folly::SocketAddress("::1", 0));

  // Write a packet - this schedules a timer callback
  const folly::SocketAddress destAddr("::1", 12345);
  socket->write(destAddr, makeDataPacket());

  // Destroy the socket immediately. If destruction order is wrong (modifier_
  // destroyed after sendPacketFn_), a pending timer callback could access the
  // destroyed sendPacketFn_, causing use-after-free detected by ASAN.
  socket.reset();

  // If we get here without ASAN errors, the test passes
  SUCCEED();
}

// Verify that when PacketModifierSocket sends packets to different
// destinations, each framed Ethernet header carries the correct
// destination MAC -- not the previous packet's.
TEST(PacketModifierSocketEthernetTest, EachFrameGetsCorrectDstMac) {
  folly::EventBase evb;
  const auto srcMac = folly::MacAddress("02:00:00:00:00:01");
  const auto dstMac1 = folly::MacAddress("02:00:00:00:00:02");
  const auto dstMac2 = folly::MacAddress("02:00:00:00:00:03");
  const auto addr1 = RawEncapsulation::linkLocalFromMac(dstMac1);
  const auto addr2 = RawEncapsulation::linkLocalFromMac(dstMac2);

  auto capturingSocket = std::make_unique<CapturingPacketSocket>();
  const auto* captured = capturingSocket.get();

  auto pms = std::make_unique<PacketModifierSocket>(
      &evb,
      std::move(capturingSocket),
      std::make_unique<EthernetEncapsulation>(srcMac, dstMac1),
      std::make_unique<MetaRoceCodec>());

  pms->write(folly::SocketAddress(addr1, 0), makeDataPacket());
  pms->write(folly::SocketAddress(addr2, 0), makeDataPacket());

  ASSERT_EQ(captured->captured.size(), 2u)
      << "both packets should reach the socket";

  const auto& frame1 = captured->captured[0].data;
  const auto& frame2 = captured->captured[1].data;

  ASSERT_GE(frame1->length(), folly::MacAddress::SIZE)
      << "frame must be large enough to contain a destination MAC";
  EXPECT_EQ(
      std::memcmp(frame1->data(), dstMac1.bytes(), folly::MacAddress::SIZE), 0)
      << "first frame should carry the MAC for addr1";

  ASSERT_GE(frame2->length(), folly::MacAddress::SIZE)
      << "frame must be large enough to contain a destination MAC";
  EXPECT_EQ(
      std::memcmp(frame2->data(), dstMac2.bytes(), folly::MacAddress::SIZE), 0)
      << "second frame should carry the MAC for addr2, not addr1";

  pms->close();
}

// Between beginGsoBatch/flushGsoBatch, framed PDUs accumulate and flush as one
// writeGSO of equal-size segments (the GSO fast path that amortizes sendmsg).
TEST(PacketModifierSocketGsoTest, BatchEmitsOneWriteGsoOfEqualSegments) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  const folly::SocketAddress dest("::1", 2766);
  constexpr int kNumSegments = 4;
  pms.beginGsoBatch();
  for (int i = 0; i < kNumSegments; ++i) {
    pms.write(dest, makeDataPacket());
  }
  const ssize_t flushed = pms.flushGsoBatch();

  ASSERT_EQ(cap->gsoCalls, 1)
      << "a multi-PDU batch must go out as one writeGSO, not per-packet writes";
  ASSERT_EQ(cap->captured.size(), 1u)
      << "the GSO flush must emit exactly one datagram, the coalesced buffer";
  EXPECT_GT(cap->gsoSegSize, 0)
      << "flushGsoBatch must record a positive segment size for the kernel to "
         "split the buffer on";
  const size_t total = cap->captured[0].data->computeChainDataLength();
  EXPECT_EQ(total, static_cast<size_t>(kNumSegments) * cap->gsoSegSize)
      << "the coalesced buffer must be N equal-size framed segments";
  EXPECT_EQ(flushed, static_cast<ssize_t>(total))
      << "flushGsoBatch must return the full coalesced byte count it sent";
}

// A batch holding a single PDU needs no GSO; it must use a plain write.
TEST(PacketModifierSocketGsoTest, SinglePduFlushUsesPlainWrite) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  pms.beginGsoBatch();
  pms.write(folly::SocketAddress("::1", 2766), makeDataPacket());
  pms.flushGsoBatch();

  EXPECT_EQ(cap->gsoCalls, 0)
      << "a single buffered PDU should flush via plain write, not writeGSO";
  EXPECT_EQ(cap->captured.size(), 1u)
      << "the single buffered PDU must still be written";
}

// abortGsoBatch must discard the batch and leave no stale state for the next
// one.
TEST(PacketModifierSocketGsoTest, AbortGsoBatchDiscardsAccumulatedPdus) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  const folly::SocketAddress dest("::1", 2766);
  pms.beginGsoBatch();
  pms.write(dest, makeDataPacket());
  pms.write(dest, makeDataPacket());
  pms.abortGsoBatch();

  EXPECT_EQ(cap->gsoCalls, 0) << "an aborted batch must not emit a writeGSO";
  EXPECT_EQ(cap->captured.size(), 0u) << "an aborted batch must send nothing";

  constexpr int kNumSegments = 3;
  pms.beginGsoBatch();
  for (int i = 0; i < kNumSegments; ++i) {
    pms.write(dest, makeDataPacket());
  }
  pms.flushGsoBatch();

  ASSERT_EQ(cap->gsoCalls, 1)
      << "the post-abort batch must flush as one writeGSO";
  ASSERT_EQ(cap->captured.size(), 1u)
      << "the post-abort batch must emit exactly one datagram";
  const size_t total = cap->captured[0].data->computeChainDataLength();
  EXPECT_EQ(total, static_cast<size_t>(kNumSegments) * cap->gsoSegSize)
      << "the post-abort batch must contain only its own segments, not the "
         "aborted ones";
}

// disableGso() latches GSO off once; getGSO() then reports it unavailable.
TEST(PacketModifierSocketGsoTest, DisableGsoLatchesOffOnce) {
  folly::EventBase evb;
  PacketModifierSocket pms(
      &evb,
      std::make_unique<CapturingPacketSocket>(),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  ASSERT_GE(pms.getGSO(), 0)
      << "GSO should be available before any failure: no modifier, not latched";

  EXPECT_TRUE(pms.disableGso())
      << "disableGso must report the enabled -> disabled transition the first "
         "time so the caller logs the fallback exactly once";
  EXPECT_LT(pms.getGSO(), 0)
      << "getGSO must report GSO unavailable once it is latched off";
  EXPECT_FALSE(pms.disableGso())
      << "a repeat disableGso must report no transition (already latched)";
}

TEST(PacketModifierSocketWritemTest, BatchEmitsOneWritemOfEveryDatagram) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  const folly::SocketAddress dest("::1", 2766);
  constexpr int kNumDatagrams = 4;
  pms.beginWritemBatch();
  for (int i = 0; i < kNumDatagrams; ++i) {
    pms.write(dest, makeDataPacket());
  }
  const int sent = pms.flushWritemBatch();

  ASSERT_EQ(cap->writemCalls, 1)
      << "the batch must reach the kernel as one sendmmsg, not per-packet "
         "writes; batching is the whole point of the writem path";
  EXPECT_EQ(cap->writemCount, static_cast<size_t>(kNumDatagrams))
      << "sendmmsg must be handed every accumulated datagram";
  EXPECT_EQ(sent, kNumDatagrams)
      << "flushWritemBatch must return the datagram count the kernel accepted, "
         "which the sender uses to detect a short send";
  ASSERT_EQ(cap->captured.size(), static_cast<size_t>(kNumDatagrams))
      << "each accumulated PDU must be passed through as its own datagram";
  for (const auto& write : cap->captured) {
    EXPECT_EQ(write.dest, dest)
        << "each datagram must keep the destination it was written to; "
           "sendmmsg addresses them individually";
    EXPECT_GT(write.data->computeChainDataLength(), 0u)
        << "each datagram must carry its framed PDU, not an empty buffer";
  }
}

// Unlike GSO, whose segments must be uniform, sendmmsg addresses each datagram
// separately, so a mixed-destination batch still goes out in one call.
TEST(PacketModifierSocketWritemTest, BatchKeepsPerDatagramDestinations) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  const folly::SocketAddress first("::1", 2766);
  const folly::SocketAddress second("::2", 2766);
  pms.beginWritemBatch();
  pms.write(first, makeDataPacket());
  pms.write(second, makeDataPacket());
  pms.flushWritemBatch();

  ASSERT_EQ(cap->writemCalls, 1)
      << "differing destinations must not split the batch: sendmmsg carries a "
         "per-datagram address";
  ASSERT_EQ(cap->captured.size(), 2u) << "both datagrams must be sent";
  EXPECT_EQ(cap->captured[0].dest, first)
      << "datagram order and addressing must be preserved through the batch";
  EXPECT_EQ(cap->captured[1].dest, second)
      << "datagram order and addressing must be preserved through the batch";
}

TEST(PacketModifierSocketWritemTest, EmptyBatchFlushMakesNoSyscall) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  pms.beginWritemBatch();
  const int sent = pms.flushWritemBatch();

  EXPECT_EQ(cap->writemCalls, 0)
      << "a turn that buffered nothing must not pay for an empty sendmmsg";
  EXPECT_EQ(sent, 0)
      << "an empty flush must report zero sent, not an error the sender would "
         "treat as a fault";
}

TEST(PacketModifierSocketWritemTest, AbortWritemBatchDiscardsAccumulated) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  const folly::SocketAddress dest("::1", 2766);
  pms.beginWritemBatch();
  pms.write(dest, makeDataPacket());
  pms.write(dest, makeDataPacket());
  pms.abortWritemBatch();

  EXPECT_EQ(cap->writemCalls, 0)
      << "an aborted batch must not reach the kernel";
  EXPECT_EQ(cap->captured.size(), 0u) << "an aborted batch must send nothing";

  constexpr int kNumDatagrams = 3;
  pms.beginWritemBatch();
  for (int i = 0; i < kNumDatagrams; ++i) {
    pms.write(dest, makeDataPacket());
  }
  pms.flushWritemBatch();

  ASSERT_EQ(cap->writemCalls, 1) << "the batch after an abort must still send";
  EXPECT_EQ(cap->writemCount, static_cast<size_t>(kNumDatagrams))
      << "the aborted datagrams must not leak into the next batch, which would "
         "resend PDUs the sender already gave up on";
}

// A short sendmmsg is back-pressure, not an error: the count must reach the
// caller so it can fault the first unsent packet and let the RTO cover the
// rest.
TEST(PacketModifierSocketWritemTest, PartialSendReturnsAcceptedCount) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  cap->writemResult = 2;
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  const folly::SocketAddress dest("::1", 2766);
  pms.beginWritemBatch();
  for (int i = 0; i < 4; ++i) {
    pms.write(dest, makeDataPacket());
  }
  const int sent = pms.flushWritemBatch();

  EXPECT_EQ(sent, 2)
      << "a short sendmmsg must report how many datagrams the kernel took, so "
         "the sender knows where the unsent tail begins";
}

// The syscall's errno must survive releasing the batch's buffers, which can
// itself set errno; the sender classifies EAGAIN off this value.
TEST(PacketModifierSocketWritemTest, FailedFlushPublishesSyscallErrno) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  cap->writemResult = -1;
  cap->writemErrno = ENOBUFS;
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  const folly::SocketAddress dest("::1", 2766);
  pms.beginWritemBatch();
  pms.write(dest, makeDataPacket());
  pms.write(dest, makeDataPacket());
  errno = 0;
  const int sent = pms.flushWritemBatch();

  EXPECT_EQ(sent, -1) << "a failed sendmmsg must surface as a failed flush";
  EXPECT_EQ(errno, ENOBUFS)
      << "flushWritemBatch must leave the syscall's errno visible to the "
         "caller, which decides between transient back-pressure and a real "
         "error";
}

// A failed batch must not strand its PDUs in the accumulator, or the next
// beginWritemBatch would resend them.
TEST(PacketModifierSocketWritemTest, FailedFlushClearsTheBatch) {
  folly::EventBase evb;
  auto capturing = std::make_unique<CapturingPacketSocket>();
  auto* cap = capturing.get();
  cap->writemResult = -1;
  cap->writemErrno = ENOBUFS;
  PacketModifierSocket pms(
      &evb,
      std::move(capturing),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());

  const folly::SocketAddress dest("::1", 2766);
  pms.beginWritemBatch();
  pms.write(dest, makeDataPacket());
  pms.flushWritemBatch();

  cap->writemResult.reset();
  pms.beginWritemBatch();
  pms.write(dest, makeDataPacket());
  pms.flushWritemBatch();

  EXPECT_EQ(cap->writemCount, 1u)
      << "the batch after a failure must hold only its own datagram; the "
         "failed one is recovered by the retransmit timer, not resent here";
}

TEST(PacketModifierSocketWritemTest, CanBatchWritemFollowsSocketCapability) {
  folly::EventBase evb;
  auto unsupported = std::make_unique<CapturingPacketSocket>();
  unsupported->writemSupported = false;
  PacketModifierSocket without(
      &evb,
      std::move(unsupported),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());
  EXPECT_FALSE(without.canBatchWritem())
      << "a socket kind that does not implement writem must say so up front, "
         "rather than the sender discovering it by failing a send";

  PacketModifierSocket with(
      &evb,
      std::make_unique<CapturingPacketSocket>(),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>());
  EXPECT_TRUE(with.canBatchWritem())
      << "a writem-capable socket with no modifier must allow batching";
}

// Flags whether two threads are ever inside write() at once. writeCount_ is
// deliberately not atomic, so a sanitizer reports the race directly.
class ConcurrencyProbeSocket : public PacketSocket {
 public:
  explicit ConcurrencyProbeSocket(int expectedWrites)
      : expectedWrites_(expectedWrites) {}

  void resumeRead(ReadCallback*) override {}
  void pauseRead() override {}
  ssize_t write(const folly::SocketAddress&, std::unique_ptr<folly::IOBuf> data)
      override {
    if (writersInside_.fetch_add(1, std::memory_order_acq_rel) != 0) {
      sawOverlap_.store(true, std::memory_order_relaxed);
    }
    const auto length = data
        ? static_cast<ssize_t>(data->computeChainDataLength())
        : ssize_t{0};
    const int written = ++writeCount_;
    writersInside_.fetch_sub(1, std::memory_order_acq_rel);
    if (written == expectedWrites_) {
      allWritesLanded.post();
    }
    return length;
  }
  ssize_t writeGSO(
      const folly::SocketAddress& dest,
      std::unique_ptr<folly::IOBuf> data,
      int /*gsoSegmentSize*/) override {
    return write(dest, std::move(data));
  }
  void bind(const folly::SocketAddress&) override {}
  void close() override {}
  int fd() const override {
    return -1;
  }
  void setRcvBuf(int) override {}
  int getRcvBuf() override {
    return 0;
  }
  // A modifier turns GSO off anyway; reporting it unavailable keeps every send
  // on the plain write path.
  int getGSO() noexcept override {
    return -1;
  }

  [[nodiscard]] bool sawOverlap() const {
    return sawOverlap_.load(std::memory_order_relaxed);
  }
  // Settled once the timer thread is joined.
  [[nodiscard]] int writeCount() const {
    return writeCount_;
  }

  folly::Baton<> allWritesLanded;

 private:
  const int expectedWrites_;
  int writeCount_{0};
  std::atomic<int> writersInside_{0};
  std::atomic<bool> sawOverlap_{false};
};

// Sends every packet twice: inline, and again from its own timer thread. Shares
// no member between the two, so the test measures the socket, not the modifier.
class InlineAndDeferredSendModifier : public Modifier {
 public:
  explicit InlineAndDeferredSendModifier(std::unique_ptr<ModifierConfig> config)
      : Modifier(std::move(config)) {}

  ssize_t modifyTxPacket(
      SendPacketFn& sendPacketFn,
      std::unique_ptr<TxPacketWithMetadata> txPacketData) override {
    auto& outgoing = *CHECK_NOTNULL(txPacketData.get());
    auto& original = *CHECK_NOTNULL(outgoing.packet.get());
    const auto destination = outgoing.remoteSocketAddr;
    auto deferred = original.clone();
    const auto sent =
        sendPacketFn(destination, std::move(outgoing.packet), std::nullopt);
    // Borrowed, not owned: close() destroys this modifier, joining the timer
    // thread, while the send function it refers to is still alive.
    auto send = std::ref(sendPacketFn);
    timer_.scheduleTimeout(
        std::chrono::microseconds(100),
        HighResTimer::Callback(
            [send, dest = destination, packet = std::move(deferred)]() mutable {
              send(dest, std::move(packet), std::nullopt);
            }));
    return sent;
  }

  void modifyRxPacket(ReceivePacketFn&, std::unique_ptr<IngressPacket>)
      override {}
  void onPacketSent() override {}

 private:
  HighResTimer timer_;
};

// Runs the inline and deferred sends against each other hard enough that they
// would overlap unserialized, then asserts they never did.
TEST(PacketModifierSocketConcurrencyTest, InlineAndTimerSendsAreSerialized) {
  folly::EventBase evb;
  constexpr int kPackets = 5000;
  // Each packet goes out inline and again from the timer.
  constexpr int kExpectedWrites = 2 * kPackets;
  auto probe = std::make_unique<ConcurrencyProbeSocket>(kExpectedWrites);
  auto& probed = *probe;

  auto config = std::make_unique<ModifierConfig>(
      ModifierConfig::ModifierType::Unknown,
      ModifierConfig::ModifierFilter(
          ModifierConfig::ModifierFilter::Direction::Tx,
          ModifierConfig::ModifierFilter::PacketType::Data,
          folly::F14FastMap<std::string, FieldConditions>{}),
      /*modificationTimes=*/std::nullopt);

  PacketModifierSocket pms(
      &evb,
      std::move(probe),
      std::make_unique<UdpEncapsulation>(),
      std::make_unique<MetaRoceCodec>(),
      std::make_unique<InlineAndDeferredSendModifier>(std::move(config)));

  const folly::SocketAddress dest("::1", 2766);
  for (int i = 0; i < kPackets; ++i) {
    pms.write(dest, makeDataPacket());
  }

  ASSERT_TRUE(probed.allWritesLanded.try_wait_for(std::chrono::seconds(60)))
      << "the timer thread never drained every deferred packet: "
      << probed.writeCount() << " of " << kExpectedWrites
      << " reached the socket";

  // Joins the timer thread, so the non-atomic counter is settled below.
  pms.close();

  EXPECT_FALSE(probed.sawOverlap())
      << "the send loop and the modifier's timer thread were inside the "
         "socket's write at the same time, so the two are not serialized";
  EXPECT_EQ(probed.writeCount(), kExpectedWrites)
      << "every inline packet and every deferred clone should reach the socket "
         "exactly once";
}

// The Receiver reads this to decide whether to take the QP's receive lock, so
// the socket has to pass the modifier's answer through unchanged.
TEST(
    PacketModifierSocketDeferredReceiveTest,
    DeferredReceiveIsReportedToTheReader) {
  folly::EventBase evb;
  std::vector<std::unique_ptr<PacketModifierSocket>> sockets;
  const auto makeSocket =
      [&](std::unique_ptr<Modifier> modifier) -> PacketModifierSocket& {
    sockets.push_back(
        std::make_unique<PacketModifierSocket>(
            &evb,
            std::make_unique<CapturingPacketSocket>(),
            std::make_unique<UdpEncapsulation>(),
            std::make_unique<MetaRoceCodec>(),
            std::move(modifier)));
    return *CHECK_NOTNULL(sockets.back().get());
  };
  const auto pacingFor = [](ModifierConfig::ModifierFilter::Direction dir) {
    return std::make_unique<PacingModifier>(
        std::make_unique<PacingModifierConfig>(
            ModifierConfig::ModifierFilter(
                dir,
                ModifierConfig::ModifierFilter::PacketType::Data,
                folly::F14FastMap<std::string, FieldConditions>{}),
            /*modificationTimes=*/std::nullopt,
            /*intervalUs=*/1000));
  };

  auto& withRx =
      makeSocket(pacingFor(ModifierConfig::ModifierFilter::Direction::Rx));
  EXPECT_TRUE(withRx.mayDeferReceive())
      << "an rx timer modifier delivers off the reader thread, and the reader "
         "cannot know that without being told";
  withRx.close();

  auto& withTx =
      makeSocket(pacingFor(ModifierConfig::ModifierFilter::Direction::Tx));
  EXPECT_FALSE(withTx.mayDeferReceive())
      << "a tx modifier leaves the receive path on one thread";
  withTx.close();

  auto& without = makeSocket(nullptr);
  EXPECT_FALSE(without.mayDeferReceive())
      << "no modifier, nothing to defer, no lock to pay for";
  without.close();
}

} // namespace libsoftmetaroce
