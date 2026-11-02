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

#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <gtest/gtest.h>
#include <array>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <libsoftmetaroce/Encapsulation.h>
#include <libsoftmetaroce/codec/BitVector.h>
#include <libsoftmetaroce/codec/MetaRoceCodec.h>
#include <libsoftmetaroce/codec/Packet.h>

namespace libsoftmetaroce {
namespace {

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

BaseHeader makeBaseHeader(
    BaseHeader::Opcode::OperationType operationType =
        BaseHeader::Opcode::OperationType::SEND) {
  return BaseHeader::BaseHeaderBuilder()
      .setOpcode(BaseHeader::Opcode(operationType))
      .setVersion(1)
      .setFlowId(2)
      .setDstQp(3)
      .setSrcQp(4)
      .build();
}

DataHeader makeDataHeader() {
  return DataHeader::DataHeaderBuilder()
      .setFsn(5)
      .setUna(6)
      .setAckRequest(true)
      .setPosn(7)
      .setMsn(8)
      .setCsn(9)
      .setTimestamp(10)
      .build();
}

DataHeader makeDataHeaderWithPadding(uint8_t padding) {
  return DataHeader::DataHeaderBuilder()
      .setFsn(5)
      .setUna(6)
      .setAckRequest(true)
      .setPadding(padding)
      .setPosn(7)
      .setMsn(8)
      .setCsn(9)
      .setTimestamp(10)
      .build();
}

std::unique_ptr<folly::IOBuf> serializePacket(const Packet& packet) {
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&queue, packet.getSizeBytes());
  packet.serialize(appender);
  return queue.move();
}

TEST(MetaRoceCodecTest, ParseRemovesDisclosedPayloadPadding) {
  const std::array<uint8_t, 1> payload = {0xAB};
  const DataPacket packet(
      makeBaseHeader(BaseHeader::Opcode::OperationType::SEND),
      makeDataHeaderWithPadding(3),
      payload.data(),
      payload.size());
  auto buffer = serializePacket(packet);
  folly::io::Cursor cursor(buffer.get());
  const PayloadOnlyEncapsulation encapsulation;
  const MetaRoceCodec codec;

  auto parsed = codec.parse(cursor, encapsulation);
  auto* dataPacket = dynamic_cast<DataPacket*>(parsed.get());
  ASSERT_NE(dataPacket, nullptr) << "a padded SEND must remain a data packet";
  EXPECT_EQ(dataPacket->getDataHeader().getPadding(), 3)
      << "the disclosed padding count must survive parsing";
  EXPECT_EQ(dataPacket->getPayloadSizeBytes(), payload.size())
      << "disclosed padding must not inflate the delivered payload length";
  std::array<uint8_t, 1> delivered{};
  dataPacket->deliverPayload(delivered.data(), delivered.size());
  EXPECT_EQ(delivered, payload)
      << "only the application payload must be delivered";
}

TEST(MetaRoceCodecTest, ParseReadRequestWithImmediatePresentRecoversImmData) {
  constexpr uint8_t kReadImmPresentOpcode = 0xCD;
  constexpr uint32_t kImmData = 0xFEEDFACE;
  const BaseHeader baseHeader =
      BaseHeader::BaseHeaderBuilder()
          .setOpcode(BaseHeader::Opcode(kReadImmPresentOpcode))
          .setVersion(1)
          .setFlowId(2)
          .setDstQp(3)
          .setSrcQp(4)
          .build();
  const ReadWritePacket packet(
      baseHeader,
      makeDataHeader(),
      ReadWriteHeader(0x1122334455667788, 0x99AA, 0xBBCC),
      /*payloadAddress=*/nullptr,
      /*payloadSizeBytes=*/0,
      kImmData);
  auto buffer = serializePacket(packet);
  folly::io::Cursor cursor(buffer.get());
  const PayloadOnlyEncapsulation encapsulation;
  const MetaRoceCodec codec;

  auto parsed = codec.parse(cursor, encapsulation);
  auto* readWritePacket = dynamic_cast<ReadWritePacket*>(parsed.get());
  ASSERT_NE(readWritePacket, nullptr)
      << "a read request with immediate present must parse as a "
         "read/write packet";
  ASSERT_TRUE(readWritePacket->getImmData().has_value())
      << "spec table 8.1 gives this opcode an ImmDt header, and leaving it "
         "unread shifts the payload by four bytes";
  EXPECT_EQ(readWritePacket->getImmData().value(), kImmData)
      << "the immediate must survive a serialize and parse round trip";
  EXPECT_EQ(readWritePacket->getPayloadSizeBytes(), 0u)
      << "the request is fixed size, so consuming the ImmDt must leave no "
         "bytes behind as payload";
}

TEST(MetaRoceCodecTest, ParseRejectsPaddingLargerThanFramedPayload) {
  const DataPacket packet(
      makeBaseHeader(BaseHeader::Opcode::OperationType::SEND),
      makeDataHeaderWithPadding(1),
      nullptr,
      0);
  auto buffer = serializePacket(packet);
  folly::io::Cursor cursor(buffer.get());
  const PayloadOnlyEncapsulation encapsulation;
  const MetaRoceCodec codec;

  EXPECT_EQ(codec.parse(cursor, encapsulation), nullptr)
      << "the parser must reject a padding count larger than the wire payload";
}

std::unique_ptr<folly::IOBuf> serializeWithCodec(
    const MetaRoceCodec& codec,
    const Packet& packet) {
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&queue, codec.getSerializedSize(packet));
  codec.serialize(packet, appender);
  return queue.move();
}

TEST(MetaRoceCodecTest, SerializationMatchesPacketSerializationForEveryKind) {
  using OperationType = BaseHeader::Opcode::OperationType;
  const std::array<uint8_t, 4> payload = {0x11, 0x22, 0x33, 0x44};
  const MetaRoceCodec codec;

  std::vector<std::pair<std::string, std::unique_ptr<Packet>>> packets;
  packets.emplace_back(
      "send",
      std::make_unique<DataPacket>(
          makeBaseHeader(), makeDataHeader(), payload.data(), payload.size()));
  packets.emplace_back(
      "send last",
      std::make_unique<DataPacket>(
          makeBaseHeader(OperationType::SEND_LAST),
          makeDataHeader(),
          payload.data(),
          payload.size()));
  packets.emplace_back(
      "read response",
      std::make_unique<DataPacket>(
          makeBaseHeader(OperationType::READ_RESP),
          makeDataHeader(),
          payload.data(),
          payload.size()));
  packets.emplace_back(
      "send with immediate",
      std::make_unique<DataPacket>(
          makeBaseHeader(OperationType::SEND_IMM_P),
          makeDataHeader(),
          payload.data(),
          payload.size(),
          0xFEED));
  packets.emplace_back(
      "write",
      std::make_unique<ReadWritePacket>(
          makeBaseHeader(OperationType::WRITE),
          makeDataHeader(),
          ReadWriteHeader(0x1122334455667788, 0x99AA, 0xBBCC),
          payload.data(),
          payload.size()));
  packets.emplace_back(
      "write with immediate",
      std::make_unique<ReadWritePacket>(
          makeBaseHeader(OperationType::WRITE_IMM_P),
          makeDataHeader(),
          ReadWriteHeader(0x1122334455667788, 0x99AA, 0xBBCC),
          payload.data(),
          payload.size(),
          0xFEED));
  packets.emplace_back(
      "compare and swap",
      std::make_unique<AtomicPacket>(
          makeBaseHeader(OperationType::ATOMIC_CMP_SWP),
          makeDataHeader(),
          AtomicHeader(0x1122334455667788, 0x99AA, 0xBBCC, 0xDDEE)));
  packets.emplace_back(
      "atomic ack",
      std::make_unique<AtomicAckPacket>(
          makeBaseHeader(OperationType::ATOMIC_ACK_RESP),
          makeDataHeader(),
          AtomicAckHeader(0x1122334455667788)));
  packets.emplace_back(
      "selective ack",
      std::make_unique<AckPacket>(
          makeBaseHeader(OperationType::SEL_ACK),
          AckHeader::AckHeaderBuilder()
              .setCnpCE(0)
              .setTag(0)
              .setCumulativeFsn(1)
              .setMaxRcvdFsn(2)
              .setAckStatus(AckStatus::ACK)
              .setCumulRespDeliveredMsn(3)
              .setActivePortSet(0)
              .setRateHints(0)
              .setTimestamp(4)
              .build()));
  packets.emplace_back(
      "rnr cancel",
      std::make_unique<RnrCancelPacket>(
          makeBaseHeader(OperationType::RNR_CANCEL),
          RnrCancelHeader(3, 0, 0x1234, 0)));
  packets.emplace_back(
      "ping",
      std::make_unique<PingPacket>(
          makeBaseHeader(OperationType::RELIABLE_CONTROL),
          ControlHeader(
              static_cast<uint16_t>(ControlPacketType::PING), 0, 0x1234),
          PingHeader(0, 0, 4),
          payload.data(),
          payload.size()));

  const PayloadOnlyEncapsulation encapsulation;
  for (const auto& [name, packet] : packets) {
    auto expected = serializePacket(*packet);
    auto actual = serializeWithCodec(codec, *packet);
    ASSERT_NE(expected, nullptr) << name << ": packet must serialize";
    ASSERT_NE(actual, nullptr) << name << ": codec must serialize";
    EXPECT_EQ(
        codec.getSerializedSize(*packet), actual->computeChainDataLength())
        << name << ": getSerializedSize must predict the byte count";
    EXPECT_EQ(expected->coalesce(), actual->coalesce())
        << name << ": the codec must preserve the existing wire bytes";

    auto toParse = actual->clone();
    toParse->coalesce();
    folly::io::Cursor cursor(toParse.get());
    auto parsed = codec.parse(cursor, encapsulation);
    ASSERT_NE(parsed, nullptr)
        << name << ": the codec must parse its own bytes";
    auto reserialized = serializeWithCodec(codec, *parsed);
    ASSERT_NE(reserialized, nullptr) << name
                                     << ": the parsed packet must "
                                        "serialize";
    EXPECT_EQ(actual->coalesce(), reserialized->coalesce())
        << name << ": a parse and re-serialize must reproduce the wire bytes";
  }
}

TEST(MetaRoceCodecTest, ShortDataPacketCountsMinimumFrameFillInPadding) {
  constexpr size_t kMinimumPacketSizeBytes = 38;
  const std::array<uint8_t, 1> payload = {0xAB};
  const DataPacket packet(
      makeBaseHeader(BaseHeader::Opcode::OperationType::SEND_LAST),
      makeDataHeaderWithPadding(Packet::payloadPadBytes(payload.size())),
      payload.data(),
      payload.size());
  ASSERT_LT(packet.getSizeBytes(), kMinimumPacketSizeBytes)
      << "precondition: the packet must be shorter than the minimum packet size";
  const MetaRoceCodec codec(kMinimumPacketSizeBytes);

  auto buffer = serializeWithCodec(codec, packet);
  ASSERT_NE(buffer, nullptr);
  EXPECT_EQ(buffer->computeChainDataLength(), kMinimumPacketSizeBytes)
      << "a data packet filled to the minimum leaves the encapsulation no fill "
         "to add outside the Padding field";
  EXPECT_EQ(codec.getSerializedSize(packet), buffer->computeChainDataLength())
      << "the sender sizes its buffer from getSerializedSize, so it must count "
         "the fill";
  buffer->coalesce();
  folly::io::Cursor cursor(buffer.get());
  auto parsed = codec.parse(cursor, PayloadOnlyEncapsulation());
  auto* dataPacket = dynamic_cast<DataPacket*>(parsed.get());
  ASSERT_NE(dataPacket, nullptr);
  EXPECT_EQ(dataPacket->getPayloadSizeBytes(), payload.size())
      << "spec 8.2: the receiver derives the payload length from the Padding "
         "field, so a send's delivered length is exact only if the fill is "
         "counted there";
}

TEST(
    MetaRoceCodecTest,
    MinimumPacketSizeLeavesNonDataAndFullSizePacketsUnchanged) {
  constexpr size_t kMinimumPacketSizeBytes = 38;
  const std::array<uint8_t, 4> payload = {0x11, 0x22, 0x33, 0x44};
  const AckPacket ack(
      makeBaseHeader(BaseHeader::Opcode::OperationType::SEL_ACK),
      AckHeader::AckHeaderBuilder()
          .setCnpCE(0)
          .setTag(0)
          .setCumulativeFsn(1)
          .setMaxRcvdFsn(2)
          .setAckStatus(AckStatus::ACK)
          .setCumulRespDeliveredMsn(3)
          .setActivePortSet(0)
          .setRateHints(0)
          .setTimestamp(4)
          .build());
  const ReadWritePacket write(
      makeBaseHeader(BaseHeader::Opcode::OperationType::WRITE),
      makeDataHeader(),
      ReadWriteHeader(0x1122334455667788, 0x99AA, 0xBBCC),
      payload.data(),
      payload.size());
  ASSERT_LT(ack.getSizeBytes(), kMinimumPacketSizeBytes)
      << "precondition: the ack must be shorter than the minimum packet size";
  ASSERT_GE(write.getSizeBytes(), kMinimumPacketSizeBytes)
      << "precondition: the write must already reach the minimum packet size";
  const MetaRoceCodec codec(kMinimumPacketSizeBytes);

  for (const Packet* packet :
       {static_cast<const Packet*>(&ack), static_cast<const Packet*>(&write)}) {
    auto expected = serializePacket(*packet);
    auto actual = serializeWithCodec(codec, *packet);
    ASSERT_NE(expected, nullptr);
    ASSERT_NE(actual, nullptr);
    EXPECT_EQ(expected->coalesce(), actual->coalesce())
        << "only a short data packet has a Padding field to count fill in; "
           "opcode "
        << packet->getBaseHeader().getOpcode();
  }
}

TEST(MetaRoceCodecTest, FilledPacketBytesOutliveSerialization) {
  constexpr size_t kMinimumPacketSizeBytes = 46;
  const std::array<uint8_t, 10> payload = {
      0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19};
  const DataPacket packet(
      makeBaseHeader(BaseHeader::Opcode::OperationType::SEND_LAST),
      makeDataHeaderWithPadding(Packet::payloadPadBytes(payload.size())),
      payload.data(),
      payload.size());
  ASSERT_LT(packet.getSizeBytes(), kMinimumPacketSizeBytes)
      << "precondition: the packet must be shorter than the minimum size";
  const MetaRoceCodec codec(kMinimumPacketSizeBytes);
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  // One-byte growth leaves too little tailroom to copy the payload into.
  folly::io::QueueAppender appender(&queue, /*growth=*/1);

  codec.serialize(packet, appender);
  auto buffer = queue.move();
  buffer->coalesce();
  folly::io::Cursor cursor(buffer.get());
  auto parsed = codec.parse(cursor, PayloadOnlyEncapsulation());
  auto* dataPacket = dynamic_cast<DataPacket*>(parsed.get());
  ASSERT_NE(dataPacket, nullptr);
  std::array<uint8_t, 10> delivered{};
  dataPacket->deliverPayload(delivered.data(), delivered.size());
  EXPECT_EQ(delivered, payload)
      << "the serialized bytes must stay valid after serialize() returns, "
         "since the sender frames and sends them later";
}

TEST(MetaRoceCodecTest, FixedSizePacketsAcceptTrailingFrameBytes) {
  using OperationType = BaseHeader::Opcode::OperationType;
  std::vector<std::pair<std::string, std::unique_ptr<Packet>>> packets;
  packets.emplace_back(
      "rnr cancel",
      std::make_unique<RnrCancelPacket>(
          makeBaseHeader(OperationType::RNR_CANCEL),
          RnrCancelHeader(1, 0, 2, 0)));
  packets.emplace_back(
      "compare and swap",
      std::make_unique<AtomicPacket>(
          makeBaseHeader(OperationType::ATOMIC_CMP_SWP),
          makeDataHeader(),
          AtomicHeader(0x1122334455667788, 0x99AA, 0xBBCC, 0xDDEE)));
  packets.emplace_back(
      "atomic ack",
      std::make_unique<AtomicAckPacket>(
          makeBaseHeader(OperationType::ATOMIC_ACK_RESP),
          makeDataHeader(),
          AtomicAckHeader(0x1122334455667788)));

  const MetaRoceCodec codec;
  const PayloadOnlyEncapsulation encapsulation;
  for (const auto& [name, packet] : packets) {
    auto framed = serializeWithCodec(codec, *packet);
    ASSERT_NE(framed, nullptr) << name;
    framed->appendChain(folly::IOBuf::copyBuffer(std::string(4, '\0')));
    framed->coalesce();
    folly::io::Cursor cursor(framed.get());
    EXPECT_NE(codec.parse(cursor, encapsulation), nullptr)
        << name << ": trailing Ethernet padding must not invalidate the packet";
  }
}

TEST(MetaRoceCodecTest, TruncatedFrameIsDroppedNotThrown) {
  const RnrCancelPacket packet(
      makeBaseHeader(BaseHeader::Opcode::OperationType::RNR_CANCEL),
      RnrCancelHeader::RnrCancelHeaderBuilder()
          .setFlowsAvailable(1)
          .setTag(0)
          .setPostedCsn(2)
          .setRateHints(0)
          .build());
  const MetaRoceCodec codec;
  const PayloadOnlyEncapsulation encapsulation;

  auto exact = serializeWithCodec(codec, packet);
  ASSERT_NE(exact, nullptr);
  // Every length a peer could truncate to, including shorter than the base
  // header, which a framing with a smaller minimum can deliver.
  for (size_t length = 0; length < exact->length(); ++length) {
    auto truncated = exact->clone();
    truncated->trimEnd(exact->length() - length);
    folly::io::Cursor cursor(truncated.get());
    EXPECT_EQ(codec.parse(cursor, encapsulation), nullptr)
        << "a frame truncated to " << length
        << " bytes should be dropped, not throw out of the receive loop";
  }
}

TEST(MetaRoceCodecTest, AckWithAbbreviatedBitVectorIsAccepted) {
  BitVector<AckHeader::kAckTrackerBits> fsnTracker;
  fsnTracker.setBit(0);
  const AckPacket packet(
      makeBaseHeader(BaseHeader::Opcode::OperationType::SEL_ACK),
      AckHeader::AckHeaderBuilder()
          .setCnpCE(0)
          .setTag(0)
          .setCumulativeFsn(1)
          .setMaxRcvdFsn(2)
          .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
          .setCumulRespDeliveredMsn(3)
          .setActivePortSet(0)
          .setRateHints(0)
          .setTimestamp(4)
          .setFsnBitVector(fsnTracker)
          .build());
  const MetaRoceCodec codec;
  const PayloadOnlyEncapsulation encapsulation;

  auto serialized = serializeWithCodec(codec, packet);
  ASSERT_NE(serialized, nullptr);
  serialized->coalesce();

  // Keep the first 8 bytes of the 32-byte tracker, as a peer that sends only
  // the bits between its cumulative and max FSN would.
  constexpr size_t kAbbreviatedTrackerBytes = 8;
  const size_t fullLength = serialized->length();
  ASSERT_GT(fullLength, AckHeader::kAckTrackerBits / 8);
  auto abbreviated = folly::IOBuf::copyBuffer(
      serialized->data(),
      fullLength - AckHeader::kAckTrackerBits / 8 + kAbbreviatedTrackerBytes);

  folly::io::Cursor cursor(abbreviated.get());
  auto parsed = codec.parse(cursor, encapsulation);
  ASSERT_NE(parsed, nullptr)
      << "an ack carrying an abbreviated bit vector should still parse";
  const auto* ackPacket = dynamic_cast<const AckPacket*>(parsed.get());
  ASSERT_NE(ackPacket, nullptr);
  EXPECT_EQ(ackPacket->getAckHeader().getCumulativeFsn(), 1)
      << "the cumulative FSN must survive so the sender can advance its window";
}

TEST(MetaRoceCodecTest, ParseRejectsNonMetaRoceOpcode) {
  std::array<uint8_t, BaseHeader::kBaseHeaderSizeBytes> bytes{};
  bytes[0] = 0xE0; // top three bits are a bth opcode other than MetaRoCE's
  auto buffer = folly::IOBuf::copyBuffer(bytes.data(), bytes.size());
  folly::io::Cursor cursor(buffer.get());
  const PayloadOnlyEncapsulation encapsulation;
  const MetaRoceCodec codec;

  EXPECT_EQ(codec.parse(cursor, encapsulation), nullptr)
      << "a codec must not interpret another protocol's opcode as MetaRoCE";
}

TEST(MetaRoceCodecTest, ParsedPayloadOutlivesInputBuffer) {
  const std::vector<uint8_t> payload = {0xDE, 0xAD, 0xBE, 0xEF};
  const DataPacket packet(
      makeBaseHeader(), makeDataHeader(), payload.data(), payload.size());
  auto buffer = serializePacket(packet);
  folly::io::Cursor cursor(buffer.get());
  const PayloadOnlyEncapsulation encapsulation;
  const MetaRoceCodec codec;

  auto parsed = codec.parse(cursor, encapsulation);
  buffer.reset();

  auto* dataPacket = dynamic_cast<DataPacket*>(parsed.get());
  ASSERT_NE(dataPacket, nullptr)
      << "a serialized SEND must parse as a data packet";
  std::vector<uint8_t> delivered(payload.size());
  dataPacket->deliverPayload(delivered.data(), delivered.size());
  EXPECT_EQ(delivered, payload)
      << "the parsed packet must own payload bytes after the input is released";
}

} // namespace
} // namespace libsoftmetaroce
