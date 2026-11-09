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

#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/codec/AckHeader.h>
#include <libsoftmetaroce/codec/AtomicHeader.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/DataHeader.h>
#include <libsoftmetaroce/codec/Packet.h>
#include <libsoftmetaroce/codec/PingHeader.h>
#include <libsoftmetaroce/codec/ReadWriteHeader.h>
#include <libsoftmetaroce/codec/RnrCancelHeader.h>
#include <cstdint>
#include <limits>

using namespace testing;
namespace libsoftmetaroce {

/*
 * Verifies that the opcode and its fields are properly parsed and correctly
 * populated. Then it changes the fields through the setters and confirms the
 * new values using the getters.
 */
TEST(CodecTest, BaseHeaderOpcodeInitSetGet) {
  BaseHeader::Opcode opcode(0x00);
  EXPECT_EQ(opcode.bthOpcode(), BaseHeader::Opcode::BthOpcode::ROCEv2_RC);
  opcode.setBthOpcode(BaseHeader::Opcode::BthOpcode::META_ROCE);
  EXPECT_EQ(opcode.bthOpcode(), BaseHeader::Opcode::BthOpcode::META_ROCE);

  opcode.setOperationType(BaseHeader::Opcode::OperationType::RELIABLE_CONTROL);
  EXPECT_TRUE(opcode.isControl());
  EXPECT_FALSE(opcode.isData());
  EXPECT_FALSE(opcode.isAnAck());

  opcode.setOperationType(BaseHeader::Opcode::OperationType::WRITE);
  EXPECT_FALSE(opcode.isControl());
  EXPECT_TRUE(opcode.isData());
  EXPECT_FALSE(opcode.isAnAck());
  EXPECT_TRUE(opcode.hasRwHdr());
  EXPECT_FALSE(opcode.hasImmData());

  opcode.setOperationType(BaseHeader::Opcode::OperationType::WRITE_IMM_P);
  EXPECT_FALSE(opcode.isControl());
  EXPECT_TRUE(opcode.isData());
  EXPECT_FALSE(opcode.isAnAck());
  EXPECT_TRUE(opcode.hasRwHdr());
  EXPECT_TRUE(opcode.hasImmData());

  opcode.setOperationType(BaseHeader::Opcode::OperationType::SEL_ACK);
  EXPECT_FALSE(opcode.isControl());
  EXPECT_TRUE(opcode.isAnAck());
}

TEST(CodecTest, ImmediateDataOpcodesMatchSpecTable) {
  using OperationType = BaseHeader::Opcode::OperationType;
  BaseHeader::Opcode opcode(0x00);
  for (const auto op :
       {OperationType::SEND_IMM_P,
        OperationType::WRITE_IMM_P,
        OperationType::READ_IMM_P}) {
    opcode.setOperationType(op);
    EXPECT_TRUE(opcode.hasImmData())
        << opcode
        << ": spec table 8.1 gives it an ImmDt header, so a parser that skips "
           "the field would misread the payload that follows";
  }
  for (const auto op :
       {OperationType::SEND,
        OperationType::WRITE,
        OperationType::WRITE_IMM,
        OperationType::READ,
        OperationType::READ_RESP}) {
    opcode.setOperationType(op);
    EXPECT_FALSE(opcode.hasImmData())
        << opcode
        << ": spec table 8.1 gives it no ImmDt header, so consuming four "
           "bytes for one would shift the payload";
  }
}

TEST(CodecTest, IsMetaRoceClassifiesBthPrefix) {
  EXPECT_TRUE(BaseHeader::Opcode(0xC0).isMetaRoce())
      << "BTH prefix 110 is MetaRoCE";
  EXPECT_FALSE(BaseHeader::Opcode(0x00).isMetaRoce())
      << "BTH prefix 000 (RoCEv2 RC) is not MetaRoCE";
  EXPECT_FALSE(BaseHeader::Opcode(0x80).isMetaRoce())
      << "BTH prefix 100 (RoCEv2 UD) is not MetaRoCE";
  EXPECT_FALSE(BaseHeader::Opcode(0x20).isMetaRoce())
      << "BTH prefix 001 (undefined) is not MetaRoCE";
}

namespace {
BaseHeader writeAndParseBaseHeader(const BaseHeader& header) {
  auto buf = folly::IOBuf::create(BaseHeader::kBaseHeaderSizeBytes);
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
  header.write(appender);

  std::unique_ptr<folly::IOBuf> movedBuf = bufferQueue.move();
  folly::io::Cursor cursor(movedBuf.get());
  return BaseHeader(cursor);
}

// Spelled out rather than taken from BaseHeader, so the cases pin the widths
// independently of the constants they check.
constexpr uint8_t kDistinctiveVersion = 0x5;
constexpr uint16_t kMaxWireFlowId = 0xFFF;
constexpr uint16_t kFlowIdTooBig = 0x1000;
constexpr uint32_t kSomeDstQp = 0x123456;
constexpr uint32_t kSomeSrcQp = 0x654321;
} // namespace

/*
 * Creates 10 BaseHeader objects with fields populated by random values using a
 * Builder. Then, it writes each object into an IOBuf, reads the content back
 * into a new BaseHeader object, and finally compares the original and newly
 * created BaseHeader objects for equality.
 */
TEST(CodecTest, InitWriteReadBaseHeader) {
  for (int i = 0; i < 10; i++) {
    const BaseHeader header =
        BaseHeader::BaseHeaderBuilder()
            .setOpcode(
                BaseHeader::Opcode(
                    folly::Random::rand32(
                        std::numeric_limits<uint16_t>::max())))
            .setVersion(folly::Random::rand32(1 << 4)) // version is 4 bits
            .setFlowId(folly::Random::rand32(1 << 12)) // flow id is 12 bits
            .setDstQp(
                folly::Random::rand32(std::numeric_limits<uint32_t>::max()))
            .setSrcQp(
                folly::Random::rand32(std::numeric_limits<uint32_t>::max()))
            .build();
    EXPECT_EQ(header, writeAndParseBaseHeader(header));
  }
}

/*
 * Round trip the largest legal flow id alongside a nonzero version, the
 * boundary at which the two fields would first disturb each other.
 */
TEST(CodecTest, BaseHeaderPreservesVersionAtMaxFlowId) {
  const BaseHeader header = BaseHeader::BaseHeaderBuilder()
                                .setOpcode(BaseHeader::Opcode(0xC0))
                                .setVersion(kDistinctiveVersion)
                                .setFlowId(kMaxWireFlowId)
                                .setDstQp(kSomeDstQp)
                                .setSrcQp(kSomeSrcQp)
                                .build();

  const BaseHeader parsed = writeAndParseBaseHeader(header);
  EXPECT_EQ(parsed.getVersion(), kDistinctiveVersion)
      << "write() should leave the version nibble alone at the largest flow id "
         "that fits in 12 bits";
  EXPECT_EQ(parsed.getFlowId(), kMaxWireFlowId)
      << "the largest 12-bit flow id should survive a write and parse round "
         "trip";
}

/*
 * The checked setters keep an out-of-range flow id out of a header in a build
 * with DCHECK enabled, leaving the value constructor as the raw path. write()
 * masks, so a release build truncates the flow id instead of ORing its high
 * nibble into the version.
 */
TEST(CodecTest, BaseHeaderWriteTruncatesOutOfRangeFlowId) {
  // The high nibble is disjoint from kDistinctiveVersion, so a leaked bit
  // changes the version rather than ORing into a bit it already has set.
  constexpr uint16_t kOutOfRangeFlowId = 0xA234;
  constexpr uint16_t kTruncatedFlowId = 0x234;
  const BaseHeader header(
      BaseHeader::Opcode(0xC0),
      kDistinctiveVersion,
      kOutOfRangeFlowId,
      kSomeDstQp,
      kSomeSrcQp);

  const BaseHeader parsed = writeAndParseBaseHeader(header);
  EXPECT_EQ(parsed.getVersion(), kDistinctiveVersion)
      << "write() should mask the flow id rather than let its high nibble land "
         "in the version field";
  EXPECT_EQ(parsed.getFlowId(), kTruncatedFlowId)
      << "an out-of-range flow id should truncate to its low 12 bits on the "
         "wire";
}

TEST(CodecDeathTest, BaseHeaderFlowIdSettersRejectOutOfRange) {
  BaseHeader header;
  EXPECT_DEBUG_DEATH(header.setFlowId(kFlowIdTooBig), "must fit in 12 bits")
      << "setFlowId should reject a flow id too wide for the 12-bit wire field";

  BaseHeader::BaseHeaderBuilder builder;
  EXPECT_DEBUG_DEATH(
      std::move(builder).setFlowId(kFlowIdTooBig), "must fit in 12 bits")
      << "the builder should reject a flow id too wide for the 12-bit wire "
         "field";
}

/*
 * Creates 10 DataHeader objects with fields populated by random values using a
 * Builder. Then, it writes each object into an IOBuf, reads the content back
 * into a new DataHeader object, and finally compares the original and newly
 * created DataHeader objects for equality.
 */
TEST(CodecTest, TestReadWriteDataHeader) {
  for (int i = 0; i < 10; i++) {
    const DataHeader header =
        DataHeader::DataHeaderBuilder()
            .setFsn(folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setUna(folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setAckRequest(folly::Random::rand32(2))
            .setPadding(folly::Random::rand32(16))
            .setPosn(
                folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setMsn(folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .build();

    auto buf = folly::IOBuf::create(sizeof(DataHeader));
    folly::IOBufQueue bufferQueue;
    bufferQueue.append(std::move(buf));
    folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
    header.write(appender);

    std::unique_ptr<folly::IOBuf> movedBuf = bufferQueue.move();
    folly::io::Cursor cursor(movedBuf.get());
    DataHeader parsedHeader(cursor);
    EXPECT_EQ(header, parsedHeader);
  }
}

TEST(CodecTest, DataHeaderIgnoresReservedBit) {
  constexpr uint8_t kReservedBit = 0x10;
  constexpr uint8_t kPadding = 3;
  constexpr size_t kFlagsByteOffset = 5;
  const DataHeader header = DataHeader::DataHeaderBuilder()
                                .setFsn(1)
                                .setUna(2)
                                .setPadding(kPadding)
                                .setPosn(3)
                                .setMsn(4)
                                .build();
  auto buf = folly::IOBuf::create(DataHeader::kDataHeaderSizeBytes);
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
  header.write(appender);

  auto wireBuffer = bufferQueue.move();
  wireBuffer->writableData()[kFlagsByteOffset] |= kReservedBit;
  folly::io::Cursor wireCursor(wireBuffer.get());
  const DataHeader parsed(wireCursor);
  EXPECT_TRUE(parsed.getReserved())
      << "the decoder should tolerate and expose a nonzero reserved bit";
  EXPECT_EQ(parsed.getPadding(), kPadding)
      << "the padding count should be independent of the reserved bit";

  auto outputBuf = folly::IOBuf::create(DataHeader::kDataHeaderSizeBytes);
  folly::IOBufQueue outputQueue;
  outputQueue.append(std::move(outputBuf));
  folly::io::QueueAppender outputAppender(&outputQueue, /* growth */ 0);
  parsed.write(outputAppender);

  auto output = outputQueue.move();
  folly::io::Cursor outputCursor(output.get());
  outputCursor.skip(kFlagsByteOffset);
  EXPECT_EQ(outputCursor.read<uint8_t>(), kPadding)
      << "the encoder should clear the reserved bit and preserve padding";

  folly::io::Cursor reparsedCursor(output.get());
  const DataHeader reparsed(reparsedCursor);
  EXPECT_EQ(parsed, reparsed)
      << "reserved bits should not affect semantic header equality";
}

/*
 * Creates 10 AckHeaders with fields, including fsn and rnr, populated by
 * random values using a Builder. Then, it writes each object into an IOBuf,
 * reads the content back into a new AckHeader object, and finally compares the
 * original and newly created AckHeader objects for equality.
 */

TEST(CodecTest, TestReadWriteAckHeaderWithFsnRnr) {
  for (int i = 0; i < 10; i++) {
    BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
    // set fsnBitVector to a random value
    for (int j = 0; j < AckHeader::kAckTrackerBits; j++) {
      if (folly::Random::rand32(2)) {
        fsnBitVector.setBit(j);
        EXPECT_EQ(fsnBitVector.getBit(j), 1);
      }
    }

    BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
    // set rnrBitVector to a random value
    for (int j = 0; j < AckHeader::kAckTrackerBits; j++) {
      if (folly::Random::rand32(2)) {
        rnrBitVector.setBit(j);
        EXPECT_EQ(rnrBitVector.getBit(j), 1);
      }
    }

    const AckHeader header =
        AckHeader::AckHeaderBuilder()
            .setCnpCE(
                folly::Random::rand32(std::numeric_limits<uint8_t>::max()) &
                0x0F)
            .setCumulativeFsn(
                folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setMaxRcvdFsn(
                folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setCumulRespDeliveredMsn(
                folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setTimestamp(
                folly::Random::rand32(std::numeric_limits<uint32_t>::max()))
            .setAckStatus(AckStatus::BRNR)
            .setRnrBackoffTimeExp(5)
            .setFsnBitVector(fsnBitVector)
            .setRnrBitVector(rnrBitVector)
            .build();

    auto buf = folly::IOBuf::create(sizeof(AckHeader));
    folly::IOBufQueue bufferQueue;
    bufferQueue.append(std::move(buf));
    folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
    header.write(appender);

    std::unique_ptr<folly::IOBuf> movedBuf = bufferQueue.move();
    folly::io::Cursor cursor(movedBuf.get());
    AckHeader parsedHeader(cursor);
    EXPECT_EQ(header, parsedHeader);
  }
}

/*
 * Creates 10 AckHeaders with fields populated by random values using a
 * Builder. Then, it writes each object into an IOBuf, reads the content back
 * into a new AckHeader object, and finally compares the original and newly
 * created AckHeader objects for equality.
 */

TEST(CodecTest, TestReadWriteAckHeader) {
  for (int i = 0; i < 10; i++) {
    const AckHeader header =
        AckHeader::AckHeaderBuilder()
            .setCnpCE(
                folly::Random::rand32(std::numeric_limits<uint8_t>::max()) &
                0x0F)
            .setCumulativeFsn(
                folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setMaxRcvdFsn(
                folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setCumulRespDeliveredMsn(
                folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setTimestamp(
                folly::Random::rand32(std::numeric_limits<uint32_t>::max()))
            .setAckStatus(AckStatus::ACK)
            .build();

    auto buf = folly::IOBuf::create(sizeof(AckHeader));
    folly::IOBufQueue bufferQueue;
    bufferQueue.append(std::move(buf));
    folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
    header.write(appender);

    std::unique_ptr<folly::IOBuf> movedBuf = bufferQueue.move();
    folly::io::Cursor cursor(movedBuf.get());
    AckHeader parsedHeader(cursor);
    EXPECT_EQ(header, parsedHeader);
  }
}

TEST(CodecTest, OpcodeOstream) {
  BaseHeader::Opcode ack(BaseHeader::Opcode::OperationType::SEL_ACK);
  BaseHeader::Opcode send(BaseHeader::Opcode::OperationType::SEND);
  BaseHeader::Opcode roce;
  roce.setBthOpcode(BaseHeader::Opcode::BthOpcode::ROCEv2_RC);
  std::ostringstream oss_ack, oss_send, oss_roce;
  oss_ack << ack;
  EXPECT_EQ(oss_ack.str(), "MR:0x11");
  oss_send << send;
  EXPECT_EQ(oss_send.str(), "MR:0x0");
  oss_roce << roce;
  EXPECT_EQ(oss_roce.str(), "RC:0x0");
}

/*
 * Creates 10 ReadWriteHeader objects with fields populated by random values
 * using a Builder. Then, it writes each object into an IOBuf, reads the content
 * back into a new ReadWriteHeader object, and finally compares the original and
 * newly created ReadWriteHeader objects for equality.
 */

TEST(CodecTest, TestReadWriteRWHeader) {
  for (int i = 0; i < 10; i++) {
    const ReadWriteHeader header =
        ReadWriteHeader::ReadWriteHeaderBuilder()
            .setKey(folly::Random::rand32(std::numeric_limits<uint32_t>::max()))
            .setMessageAddr(
                (static_cast<uint64_t>(folly::Random::rand32(
                     std::numeric_limits<uint32_t>::max()))
                 << 32) |
                folly::Random::rand32(std::numeric_limits<uint32_t>::max()))
            .setMessageLength(
                folly::Random::rand32(
                    std::numeric_limits<uint32_t>::max() & ((1 << 18) - 1)))
            .build();

    auto buf = folly::IOBuf::create(sizeof(ReadWriteHeader));
    folly::IOBufQueue bufferQueue;
    bufferQueue.append(std::move(buf));
    folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
    header.write(appender);

    std::unique_ptr<folly::IOBuf> movedBuf = bufferQueue.move();
    folly::io::Cursor cursor(movedBuf.get());
    ReadWriteHeader parsedHeader(cursor);
    EXPECT_EQ(header, parsedHeader);
  }
}

/*
 * Tests the RNR backoff time functionality with AckStatus enum
 */
TEST(CodecTest, RnrBackoffTime) {
  auto makeHrnr = [](uint8_t exp) {
    return AckHeader::AckHeaderBuilder()
        .setCumulativeFsn(100)
        .setMaxRcvdFsn(100)
        .setTimestamp(1234)
        .setAckStatus(AckStatus::HRNR)
        .setRnrBackoffTimeExp(exp)
        .build();
  };

  // Restore the global flag unconditionally even if an assertion below throws.
  const auto savedMinUs = FLAGS_libsoftmetaroce_min_rnr_backoff_us;
  SCOPE_EXIT {
    FLAGS_libsoftmetaroce_min_rnr_backoff_us = savedMinUs;
  };

  // Pin the floor low so the shifted value dominates, then assert the exact
  // backoff each exponent yields.
  FLAGS_libsoftmetaroce_min_rnr_backoff_us = 1;

  // exp 0 means "unspecified"; fall back to the configured minimum.
  const AckHeader zeroExpHeader = makeHrnr(0);
  EXPECT_EQ(zeroExpHeader.getAckStatus(), AckStatus::HRNR);
  EXPECT_TRUE(zeroExpHeader.isHrnr());
  EXPECT_FALSE(zeroExpHeader.isBrnr());
  EXPECT_TRUE(zeroExpHeader.isRnr());
  EXPECT_EQ(zeroExpHeader.getRnrBackoffTimeExp(), 0);
  EXPECT_EQ(
      zeroExpHeader.computeRnrBackoffTimeUs(),
      static_cast<uint32_t>(FLAGS_libsoftmetaroce_min_rnr_backoff_us))
      << "exp 0 should yield the configured minimum backoff";

  // A defined exp yields exactly the protocol base unit (5us) shifted by exp.
  for (uint8_t exp = 1; exp <= kMaxRnrBackoffTimeExp; exp++) {
    const AckHeader hrnrHeader = makeHrnr(exp);
    EXPECT_EQ(hrnrHeader.getAckStatus(), AckStatus::HRNR);
    EXPECT_TRUE(hrnrHeader.isHrnr());
    EXPECT_FALSE(hrnrHeader.isBrnr());
    EXPECT_TRUE(hrnrHeader.isRnr());
    EXPECT_EQ(hrnrHeader.getRnrBackoffTimeExp(), exp);
    EXPECT_EQ(
        hrnrHeader.computeRnrBackoffTimeUs(), static_cast<uint32_t>(5u << exp))
        << "backoff for exp " << static_cast<int>(exp)
        << " should be 5us << exp";
  }

  for (uint8_t exp = kMaxRnrBackoffTimeExp + 1; exp <= 0x1f; exp++) {
    EXPECT_EQ(
        makeHrnr(exp).computeRnrBackoffTimeUs(),
        kRnrBackoffBaseUs << kMaxRnrBackoffTimeExp)
        << "a reserved exponent " << static_cast<int>(exp)
        << " must not pause transmission longer than the 163.84 ms the "
           "largest defined exponent requests";
  }

  // When the shifted value is below the configured minimum, the minimum wins.
  FLAGS_libsoftmetaroce_min_rnr_backoff_us = 1'000'000;
  EXPECT_EQ(
      makeHrnr(1).computeRnrBackoffTimeUs(),
      static_cast<uint32_t>(FLAGS_libsoftmetaroce_min_rnr_backoff_us))
      << "a small exponent should be floored at the configured minimum";
}

/*
 * Tests the serialization and deserialization of AckHeader with
 * AckStatus enum
 */
TEST(CodecTest, SerializeDeserializeWithEnum) {
  // Test HRNR with backoff time
  const AckHeader hrnrHeader = AckHeader::AckHeaderBuilder()
                                   .setCumulativeFsn(100)
                                   .setMaxRcvdFsn(100)
                                   .setTimestamp(1234)
                                   .setAckStatus(AckStatus::HRNR)
                                   .setRnrBackoffTimeExp(5)
                                   .build();

  auto buf = folly::IOBuf::create(sizeof(AckHeader));
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
  hrnrHeader.write(appender);

  std::unique_ptr<folly::IOBuf> movedBuf = bufferQueue.move();
  folly::io::Cursor cursor(movedBuf.get());
  AckHeader parsedHrnrHeader(cursor);

  EXPECT_EQ(parsedHrnrHeader.getAckStatus(), AckStatus::HRNR);
  EXPECT_TRUE(parsedHrnrHeader.isHrnr());
  EXPECT_FALSE(parsedHrnrHeader.isBrnr());
  EXPECT_TRUE(parsedHrnrHeader.isRnr());
  EXPECT_EQ(parsedHrnrHeader.getRnrBackoffTimeExp(), 5);
}

TEST(CodecTest, DataPacketSerializedSizeCorrect) {
  DataPacket packet{BaseHeader{}, DataHeader{}, nullptr, 0};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes());
}

TEST(CodecTest, DataPacketWithImmDataSerializedSizeCorrect) {
  DataPacket packet{BaseHeader{}, DataHeader{}, nullptr, 0, 0};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes());
}

TEST(CodecTest, ReadWritePacketSerializedSizeCorrect) {
  BaseHeader baseHeader;
  baseHeader.setOpcode(
      BaseHeader::Opcode(BaseHeader::Opcode::OperationType::WRITE));
  ReadWritePacket packet{
      baseHeader, DataHeader{}, ReadWriteHeader{}, nullptr, 0};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes());
}

TEST(CodecTest, ReadWritePacketWithImmDataSerializedSizeCorrect) {
  BaseHeader baseHeader;
  baseHeader.setOpcode(
      BaseHeader::Opcode(BaseHeader::Opcode::OperationType::WRITE));
  ReadWritePacket packet{
      baseHeader, DataHeader{}, ReadWriteHeader{}, nullptr, 0, 0};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes());
}

TEST(CodecTest, AckPacketSerializedSizeCorrect) {
  AckPacket packet{BaseHeader{}, AckHeader{}};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes());
}

TEST(CodecTest, AckPacketOutOfOrderSerializedSizeCorrect) {
  AckHeader ackHeader;
  ackHeader.setAckStatus(AckStatus::ACK_OUT_OF_ORDER);
  AckPacket packet{BaseHeader{}, ackHeader};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes());
}

TEST(CodecTest, AckPacketBrnrSerializedSizeCorrect) {
  AckHeader ackHeader;
  ackHeader.setAckStatus(AckStatus::BRNR);
  AckPacket packet{BaseHeader{}, ackHeader};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes());
}

/*
 * Creates 10 RnrCancelHeader objects with fields populated by random values
 * using a Builder. Then, it writes each object into an IOBuf, reads the content
 * back into a new RnrCancelHeader object, and finally compares the original and
 * newly created RnrCancelHeader objects for equality.
 */
TEST(CodecTest, TestReadWriteRnrCancelHeader) {
  for (int i = 0; i < 10; i++) {
    const RnrCancelHeader header =
        RnrCancelHeader::RnrCancelHeaderBuilder()
            .setFlowsAvailable(
                folly::Random::rand32(std::numeric_limits<uint8_t>::max()))
            .setTag(folly::Random::rand32(std::numeric_limits<uint8_t>::max()))
            .setPostedCsn(
                folly::Random::rand32(std::numeric_limits<uint16_t>::max()))
            .setRateHints(
                folly::Random::rand32(std::numeric_limits<uint32_t>::max()))
            .build();

    auto buf = folly::IOBuf::create(RnrCancelHeader::kRnrCancelHeaderSizeBytes);
    folly::IOBufQueue bufferQueue;
    bufferQueue.append(std::move(buf));
    folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
    header.write(appender);

    std::unique_ptr<folly::IOBuf> movedBuf = bufferQueue.move();
    folly::io::Cursor cursor(movedBuf.get());
    RnrCancelHeader parsedHeader(cursor);
    EXPECT_EQ(header, parsedHeader);
  }
}

TEST(CodecTest, RnrCancelPacketSerializedSizeCorrect) {
  RnrCancelPacket packet{BaseHeader{}, RnrCancelHeader{}};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes());
}

/*
 * Verifies that RNR_CANCEL is not classified as data, while reliably
 * transmitted payload-carrying operations (sends, writes, reads, atomics)
 * are classified as data.
 */
TEST(CodecTest, RnrCancelIsNotData) {
  BaseHeader::Opcode rnrCancel(BaseHeader::Opcode::OperationType::RNR_CANCEL);
  EXPECT_TRUE(rnrCancel.isRnrCancel())
      << "RNR_CANCEL should be identified by isRnrCancel()";
  EXPECT_FALSE(rnrCancel.isData())
      << "RNR_CANCEL is a control packet, not data";
  EXPECT_FALSE(rnrCancel.isAnAck()) << "RNR_CANCEL is not an ACK";
  EXPECT_FALSE(rnrCancel.isControl())
      << "RNR_CANCEL is not a RELIABLE/UNRELIABLE_CONTROL opcode";

  // Atomics are reliably transmitted data and should classify as data
  BaseHeader::Opcode fetchAdd(
      BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD);
  EXPECT_TRUE(fetchAdd.isData())
      << "ATOMIC_FETCH_ADD carries data and should be classified as data";
  BaseHeader::Opcode cmpSwp(BaseHeader::Opcode::OperationType::ATOMIC_CMP_SWP);
  EXPECT_TRUE(cmpSwp.isData())
      << "ATOMIC_CMP_SWP carries data and should be classified as data";
  BaseHeader::Opcode atomicResp(
      BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP);
  EXPECT_TRUE(atomicResp.isData())
      << "ATOMIC_ACK_RESP carries data and should be classified as data";

  // READ_RESP is also data
  BaseHeader::Opcode readResp(BaseHeader::Opcode::OperationType::READ_RESP);
  EXPECT_TRUE(readResp.isData())
      << "READ_RESP carries payload and should be classified as data";
}

/*
 * Verifies that formatting a reserved AckStatus value does not recurse
 * infinitely and produces a string containing the numeric value.
 */
TEST(CodecTest, AckStatusReservedFormatting) {
  // 0b01100100 is the RESERVED enum value; formatting it should produce
  // a string like "Reserved (100)" rather than stack-overflowing.
  AckStatus reserved = uint8_tToAckStatus(0b01100100);
  EXPECT_EQ(reserved, AckStatus::RESERVED)
      << "0b01100100 should decode to RESERVED";
  std::ostringstream oss;
  oss << reserved;
  std::string result = oss.str();
  EXPECT_NE(result.find("Reserved"), std::string::npos)
      << "formatting RESERVED status should contain 'Reserved'";
  EXPECT_NE(result.find("100"), std::string::npos)
      << "formatting RESERVED status should contain the numeric value 100";
}

TEST(CodecTest, ABrnrNakKeepsTheRnrBitsItCarries) {
  // Build a BRNR header with known bit vectors
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(0);
  fsnBitVector.setBit(42);
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(7);
  rnrBitVector.setBit(200);

  const AckHeader header = AckHeader::AckHeaderBuilder()
                               .setCumulativeFsn(100)
                               .setMaxRcvdFsn(110)
                               .setTimestamp(5678)
                               .setAckStatus(AckStatus::BRNR)
                               .setRnrBackoffTimeExp(3)
                               .setFsnBitVector(fsnBitVector)
                               .setRnrBitVector(rnrBitVector)
                               .build();

  // Serialize the full BRNR header (20 main + 32 FSN bitvec + 32 RNR bitvec)
  auto buf = folly::IOBuf::create(128);
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, 0);
  header.write(appender);

  auto fullBuf = bufferQueue.move();
  size_t fullLen = fullBuf->computeChainDataLength();
  // BRNR: 20 main + 32 FSN + 32 RNR = 84 bytes
  EXPECT_EQ(fullLen, 84u) << "BRNR header should serialize to exactly 84 bytes";

  // Round-trip the full buffer: should succeed and match
  {
    folly::io::Cursor cursor(fullBuf.get());
    AckHeader parsed(cursor);
    EXPECT_EQ(parsed, header)
        << "round-tripped BRNR header should equal the original";
  }

  // Keep 20 (main) + 32 (FSN) + 16 (half of RNR) = 68 bytes, which carry RNR
  // bit 7 but not bit 200.
  {
    auto truncatedBuf = fullBuf->clone();
    truncatedBuf->trimEnd(fullLen - 68);
    folly::io::Cursor cursor(truncatedBuf.get());
    AckHeader parsed;
    ASSERT_NO_THROW(parsed = AckHeader(cursor))
        << "the parser should accept an RNR bitset of 16 bytes";
    EXPECT_TRUE(parsed.getRnrBitVector().getBit(7))
        << "the parser should keep an RNR bit the bitset carries, or the "
           "sender never learns that frame was refused";
    EXPECT_FALSE(parsed.getRnrBitVector().getBit(200))
        << "the parser should read an RNR bit past the bitset's end as zero";
  }
}

namespace {
std::unique_ptr<folly::IOBuf> serializeAckKeepingVectorBytes(
    const AckHeader& header,
    size_t vectorBytes) {
  constexpr size_t kFixedAckHeaderBytes = 20;
  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, 128);
  header.write(appender);
  auto buffer = bufferQueue.move();
  buffer->coalesce();
  buffer->trimEnd(buffer->length() - kFixedAckHeaderBytes - vectorBytes);
  return buffer;
}
} // namespace

TEST(CodecTest, AnOutOfOrderAckWithAnAbbreviatedFsnVectorIsAccepted) {
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(3);
  const AckHeader header = AckHeader::AckHeaderBuilder()
                               .setCumulativeFsn(100)
                               .setMaxRcvdFsn(120)
                               .setTimestamp(5678)
                               .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                               .setFsnBitVector(fsnBitVector)
                               .build();
  // The four bytes kept still span through the max fsn.
  auto buffer = serializeAckKeepingVectorBytes(header, /*vectorBytes=*/4);

  folly::io::Cursor cursor(buffer.get());
  AckHeader parsed;
  ASSERT_NO_THROW(parsed = AckHeader(cursor))
      << "the parser should accept a vector abbreviated to the bits the ack "
         "needs rather than reject the whole ack";
  EXPECT_EQ(parsed.getCumulativeFsn(), 100)
      << "the parser should hand on the cumulative fsn, which a sender must "
         "process for every accepted ack";
  EXPECT_TRUE(parsed.getFsnBitVector().getBit(3))
      << "the parser should keep the bits the abbreviated vector carries";
}

TEST(CodecTest, AnInOrderAckIgnoresTheBitVectorItCarries) {
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(3);
  const AckHeader header = AckHeader::AckHeaderBuilder()
                               .setCumulativeFsn(100)
                               .setMaxRcvdFsn(120)
                               .setTimestamp(5678)
                               .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                               .setFsnBitVector(fsnBitVector)
                               .build();
  auto buffer = serializeAckKeepingVectorBytes(
      header, /*vectorBytes=*/AckHeader::kAckTrackerBits / 8);
  // The builder writes a vector only for a status that calls for one.
  constexpr size_t kAckStatusOffset = 6;
  buffer->writableData()[kAckStatusOffset] =
      static_cast<uint8_t>(AckStatus::ACK);

  folly::io::Cursor cursor(buffer.get());
  AckHeader parsed;
  ASSERT_NO_THROW(parsed = AckHeader(cursor))
      << "the parser should accept an ack carrying a vector it does not read";
  ASSERT_EQ(parsed.getAckStatus(), AckStatus::ACK)
      << "precondition: the ack parses as an in-order ack";
  EXPECT_EQ(parsed.getCumulativeFsn(), 100)
      << "the parser should hand on the cumulative fsn of the accepted ack";
  EXPECT_FALSE(parsed.getFsnBitVector().getBit(3))
      << "the parser should read no bit from a vector an in-order ack does "
         "not call for, or a stray byte acknowledges a frame never received";
}

TEST(CodecTest, ABrnrNakTooShortForItsVectorsStillCarriesItsCumulativeFsn) {
  const AckHeader header = AckHeader::AckHeaderBuilder()
                               .setCumulativeFsn(100)
                               .setMaxRcvdFsn(110)
                               .setTimestamp(5678)
                               .setAckStatus(AckStatus::BRNR)
                               .setRnrBackoffTimeExp(3)
                               .build();
  auto buffer = serializeAckKeepingVectorBytes(header, /*vectorBytes=*/0);

  folly::io::Cursor cursor(buffer.get());
  AckHeader parsed;
  ASSERT_NO_THROW(parsed = AckHeader(cursor))
      << "the parser should accept a BRNR-NAK carrying no bit vectors";
  EXPECT_EQ(parsed.getCumulativeFsn(), 100)
      << "the parser should hand on the cumulative fsn even where the vectors "
         "behind it are missing";
}

/*
 * Tests that deliverPayload() copies only the expectedSize bytes,
 * ignoring any padding bytes that may be present in the packet.
 */
TEST(CodecTest, DeliverPayloadCopiesOnlyExpectedSize) {
  // Create a packet with 104 bytes of payload (padded to 4-byte boundary)
  // but the expected size is only 101 bytes
  char sourcePayload[104];
  for (int i = 0; i < 104; i++) {
    sourcePayload[i] = static_cast<char>(i);
  }

  DataPacket packet{BaseHeader{}, DataHeader{}, sourcePayload, 104};

  // Destination buffer - initialize to sentinel value to detect overwrites
  char destBuffer[128];
  std::memset(destBuffer, 0xFF, sizeof(destBuffer));

  // Deliver only 101 bytes (the expected size), ignoring 3 padding bytes
  uint32_t expectedSize = 101;
  packet.deliverPayload(destBuffer, expectedSize);

  // Verify only expectedSize bytes were copied
  for (uint32_t i = 0; i < expectedSize; i++) {
    EXPECT_EQ(destBuffer[i], static_cast<char>(i))
        << "Byte " << i << " should be copied from source payload";
  }

  // Verify bytes beyond expectedSize were NOT overwritten
  for (uint32_t i = expectedSize; i < 128; i++) {
    EXPECT_EQ(destBuffer[i], static_cast<char>(0xFF))
        << "Byte " << i << " beyond expectedSize should not be overwritten";
  }
}

/*
 * Tests that deliverPayload() handles the case where expectedSize equals
 * the actual payload size (no padding).
 */
TEST(CodecTest, DeliverPayloadExactSize) {
  char sourcePayload[100];
  for (int i = 0; i < 100; i++) {
    sourcePayload[i] = static_cast<char>(i + 10);
  }

  DataPacket packet{BaseHeader{}, DataHeader{}, sourcePayload, 100};

  char destBuffer[128];
  std::memset(destBuffer, 0xAB, sizeof(destBuffer));

  // Expected size equals actual payload size
  packet.deliverPayload(destBuffer, 100);

  // Verify all 100 bytes were copied
  for (int i = 0; i < 100; i++) {
    EXPECT_EQ(destBuffer[i], static_cast<char>(i + 10))
        << "All payload bytes should be copied when expectedSize equals payloadSize";
  }

  // Verify bytes beyond payload were not touched
  for (int i = 100; i < 128; i++) {
    EXPECT_EQ(destBuffer[i], static_cast<char>(0xAB))
        << "Bytes beyond payload should not be modified";
  }
}

/*
 * Tests that deliverPayload() safely handles the case where expectedSize
 * is larger than the actual payload size (defensive behavior).
 */
TEST(CodecTest, DeliverPayloadExpectedSizeLargerThanPayload) {
  char sourcePayload[50];
  for (int i = 0; i < 50; i++) {
    sourcePayload[i] = static_cast<char>(i);
  }

  DataPacket packet{BaseHeader{}, DataHeader{}, sourcePayload, 50};

  char destBuffer[128];
  std::memset(destBuffer, 0xCC, sizeof(destBuffer));

  // Request more bytes than available - should only copy what's available
  packet.deliverPayload(destBuffer, 100);

  // Verify only actual payload bytes were copied
  for (int i = 0; i < 50; i++) {
    EXPECT_EQ(destBuffer[i], static_cast<char>(i))
        << "Available payload bytes should be copied";
  }

  // Verify bytes beyond actual payload were not touched
  for (int i = 50; i < 128; i++) {
    EXPECT_EQ(destBuffer[i], static_cast<char>(0xCC))
        << "Bytes beyond actual payload should not be modified even if expectedSize is larger";
  }
}

/*
 * Tests that deliverPayload() handles zero-size payloads correctly.
 */
TEST(CodecTest, DeliverPayloadZeroSize) {
  DataPacket packet{BaseHeader{}, DataHeader{}, nullptr, 0};

  char destBuffer[16];
  std::memset(destBuffer, 0xDD, sizeof(destBuffer));

  // Deliver with zero expected size
  packet.deliverPayload(destBuffer, 0);

  // Verify buffer was not modified
  for (int i = 0; i < 16; i++) {
    EXPECT_EQ(destBuffer[i], static_cast<char>(0xDD))
        << "Zero-size delivery should not modify destination buffer";
  }
}

/*
 * Tests deliverPayload() with various padding amounts (1, 2, and 3 bytes).
 */
TEST(CodecTest, DeliverPayloadVariousPaddingAmounts) {
  // Test 1 byte of padding: expected=103, actual=104
  {
    char payload[104];
    std::memset(payload, 'A', 103);
    payload[103] = 'P'; // Padding byte

    DataPacket packet{BaseHeader{}, DataHeader{}, payload, 104};

    char dest[128];
    std::memset(dest, 0, sizeof(dest));
    packet.deliverPayload(dest, 103);

    EXPECT_EQ(std::string(dest, 103), std::string(103, 'A'))
        << "Should copy 103 bytes, ignoring 1 byte of padding";
    EXPECT_EQ(dest[103], '\0') << "Padding byte should not be copied";
  }

  // Test 2 bytes of padding: expected=102, actual=104
  {
    char payload[104];
    std::memset(payload, 'B', 102);
    payload[102] = 'P';
    payload[103] = 'P';

    DataPacket packet{BaseHeader{}, DataHeader{}, payload, 104};

    char dest[128];
    std::memset(dest, 0, sizeof(dest));
    packet.deliverPayload(dest, 102);

    EXPECT_EQ(std::string(dest, 102), std::string(102, 'B'))
        << "Should copy 102 bytes, ignoring 2 bytes of padding";
  }

  // Test 3 bytes of padding: expected=101, actual=104
  {
    char payload[104];
    std::memset(payload, 'C', 101);
    payload[101] = 'P';
    payload[102] = 'P';
    payload[103] = 'P';

    DataPacket packet{BaseHeader{}, DataHeader{}, payload, 104};

    char dest[128];
    std::memset(dest, 0, sizeof(dest));
    packet.deliverPayload(dest, 101);

    EXPECT_EQ(std::string(dest, 101), std::string(101, 'C'))
        << "Should copy 101 bytes, ignoring 3 bytes of padding";
  }
}

TEST(CodecTest, AtomicOpcodeClassification) {
  BaseHeader::Opcode fetchAdd(
      BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD);
  EXPECT_TRUE(fetchAdd.hasAtomicHdr());
  EXPECT_FALSE(fetchAdd.hasAtomicAckHdr());
  EXPECT_TRUE(fetchAdd.isData());
  EXPECT_FALSE(fetchAdd.hasRwHdr());

  BaseHeader::Opcode cmpSwp(BaseHeader::Opcode::OperationType::ATOMIC_CMP_SWP);
  EXPECT_TRUE(cmpSwp.hasAtomicHdr());
  EXPECT_FALSE(cmpSwp.hasAtomicAckHdr());
  EXPECT_TRUE(cmpSwp.isData());

  BaseHeader::Opcode atomicAck(
      BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP);
  EXPECT_FALSE(atomicAck.hasAtomicHdr());
  EXPECT_TRUE(atomicAck.hasAtomicAckHdr());
  EXPECT_TRUE(atomicAck.isData());

  // Non-atomic opcodes should not have atomic headers
  BaseHeader::Opcode write(BaseHeader::Opcode::OperationType::WRITE);
  EXPECT_FALSE(write.hasAtomicHdr());
  EXPECT_FALSE(write.hasAtomicAckHdr());

  BaseHeader::Opcode read(BaseHeader::Opcode::OperationType::READ);
  EXPECT_FALSE(read.hasAtomicHdr());
  EXPECT_FALSE(read.hasAtomicAckHdr());
}

TEST(CodecTest, TestReadWriteAtomicHeader) {
  const AtomicHeader header =
      AtomicHeader::AtomicHeaderBuilder()
          .setRemoteAddr(0x00007F0000001000ULL) // 8-byte aligned
          .setRkey(0xDEADBEEF)
          .setSwapOrAddData(0x0123456789ABCDEFULL)
          .setCompareData(0xFEDCBA9876543210ULL)
          .build();

  auto buf = folly::IOBuf::create(AtomicHeader::kAtomicHeaderSizeBytes);
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
  header.write(appender);

  std::unique_ptr<folly::IOBuf> movedBuf = bufferQueue.move();
  folly::io::Cursor cursor(movedBuf.get());
  AtomicHeader parsedHeader(cursor);
  EXPECT_EQ(header, parsedHeader)
      << "AtomicHeader should survive a write/read round-trip";
}

TEST(CodecTest, TestReadWriteAtomicAckHeader) {
  const AtomicAckHeader header = AtomicAckHeader::AtomicAckHeaderBuilder()
                                     .setOrigRemoteData(0xCAFEBABE12345678ULL)
                                     .build();

  auto buf = folly::IOBuf::create(AtomicAckHeader::kAtomicAckHeaderSizeBytes);
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
  header.write(appender);

  std::unique_ptr<folly::IOBuf> movedBuf = bufferQueue.move();
  folly::io::Cursor cursor(movedBuf.get());
  AtomicAckHeader parsedHeader(cursor);
  EXPECT_EQ(header, parsedHeader)
      << "AtomicAckHeader should survive a write/read round-trip";
}

TEST(CodecTest, AtomicPacketSerializedSizeCorrect) {
  BaseHeader baseHeader;
  baseHeader.setOpcode(
      BaseHeader::Opcode(BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD));
  AtomicPacket packet{baseHeader, DataHeader{}, AtomicHeader{}};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes())
      << "Serialized bytes should match getSizeBytes()";
  EXPECT_EQ(
      packet.getSizeBytes(),
      BaseHeader::kBaseHeaderSizeBytes + DataHeader::kDataHeaderSizeBytes +
          AtomicHeader::kAtomicHeaderSizeBytes)
      << "AtomicPacket size should be BaseHeader + DataHeader + AtomicHeader";
}

TEST(CodecTest, AtomicAckPacketSerializedSizeCorrect) {
  BaseHeader baseHeader;
  baseHeader.setOpcode(
      BaseHeader::Opcode(BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP));
  AtomicAckPacket packet{baseHeader, DataHeader{}, AtomicAckHeader{}};

  folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&bufferQueue, packet.getSizeBytes());
  packet.serialize(appender);

  EXPECT_EQ(bufferQueue.chainLength(), packet.getSizeBytes())
      << "Serialized bytes should match getSizeBytes()";
  EXPECT_EQ(
      packet.getSizeBytes(),
      BaseHeader::kBaseHeaderSizeBytes + DataHeader::kDataHeaderSizeBytes +
          AtomicAckHeader::kAtomicAckHeaderSizeBytes)
      << "AtomicAckPacket size should be BaseHeader + DataHeader + AtomicAckHeader";
}

TEST(CodecTest, AckHeaderActivePortSetRoundTrip) {
  const AckHeader header = AckHeader::AckHeaderBuilder()
                               .setCumulativeFsn(42)
                               .setMaxRcvdFsn(50)
                               .setTimestamp(9999)
                               .setAckStatus(AckStatus::ACK)
                               .setActivePortSet(0x00FF)
                               .build();

  EXPECT_EQ(header.getActivePortSet(), 0x00FF)
      << "active port set should reflect the value passed to the builder";

  auto buf = folly::IOBuf::create(AckHeader::kMainAckHeaderSizeBytes);
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, 0);
  header.write(appender);

  auto movedBuf = bufferQueue.move();
  folly::io::Cursor cursor(movedBuf.get());
  AckHeader parsed(cursor);
  EXPECT_EQ(parsed, header)
      << "ack header with active port set should survive round-trip";
  EXPECT_EQ(parsed.getActivePortSet(), 0x00FF)
      << "parsed active port set should match the original";
}

TEST(CodecTest, AckHeaderActivePortSetInequality) {
  auto buildWithPortSet = [](uint16_t portSet) {
    return AckHeader::AckHeaderBuilder()
        .setCumulativeFsn(10)
        .setMaxRcvdFsn(10)
        .setTimestamp(1)
        .setAckStatus(AckStatus::ACK)
        .setActivePortSet(portSet)
        .build();
  };

  AckHeader a = buildWithPortSet(0x000F);
  AckHeader b = buildWithPortSet(0x00F0);
  EXPECT_FALSE(a == b)
      << "ack headers with different active port sets should not be equal";
}

TEST(CodecTest, AckHeaderActivePortSetWithBrnr) {
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(3);
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(7);

  const AckHeader header = AckHeader::AckHeaderBuilder()
                               .setCumulativeFsn(100)
                               .setMaxRcvdFsn(110)
                               .setTimestamp(5678)
                               .setAckStatus(AckStatus::BRNR)
                               .setRnrBackoffTimeExp(2)
                               .setActivePortSet(0x000D)
                               .setFsnBitVector(fsnBitVector)
                               .setRnrBitVector(rnrBitVector)
                               .build();

  auto buf = folly::IOBuf::create(128);
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, 0);
  header.write(appender);

  auto movedBuf = bufferQueue.move();
  folly::io::Cursor cursor(movedBuf.get());
  AckHeader parsed(cursor);
  EXPECT_EQ(parsed, header)
      << "BRNR ack with active port set should survive round-trip";
  EXPECT_EQ(parsed.getActivePortSet(), 0x000D)
      << "active port set should be preserved alongside BRNR bit vectors";
}

TEST(CodecTest, PingHeaderActivePortSetRoundTrip) {
  const PingHeader header = PingHeader::PingHeaderBuilder()
                                .setTag(5)
                                .setErrorState(PingErrorState::NO_ERROR)
                                .setSupportedFlowCount(128)
                                .setActivePortSet(0x000F)
                                .build();

  EXPECT_EQ(header.getActivePortSet(), 0x000F)
      << "active port set should reflect the value passed to the builder";

  auto buf = folly::IOBuf::create(PingHeader::kPingHeaderSizeBytes);
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, 0);
  header.write(appender);

  auto movedBuf = bufferQueue.move();
  folly::io::Cursor cursor(movedBuf.get());
  PingHeader parsed(cursor);
  EXPECT_EQ(parsed.getTag(), 5) << "tag should survive ping header round-trip";
  EXPECT_EQ(parsed.getErrorState(), PingErrorState::NO_ERROR)
      << "error state should survive ping header round-trip";
  EXPECT_EQ(parsed.getSupportedFlowCount(), 128)
      << "supported flow count should survive ping header round-trip";
  EXPECT_EQ(parsed.getActivePortSet(), 0x000F)
      << "active port set should survive ping header round-trip";
}

TEST(CodecTest, PingHeaderReservedBitsIgnored) {
  auto buf = folly::IOBuf::create(PingHeader::kPingHeaderSizeBytes);
  folly::IOBufQueue bufferQueue;
  bufferQueue.append(std::move(buf));
  folly::io::QueueAppender appender(&bufferQueue, 0);
  appender.write<uint8_t>(3);
  appender.write<uint8_t>(0);
  appender.writeBE<uint16_t>(64);
  appender.writeBE<uint16_t>(0xBEEF);
  appender.writeBE<uint16_t>(0x00FF);

  auto movedBuf = bufferQueue.move();
  folly::io::Cursor cursor(movedBuf.get());
  PingHeader parsed(cursor);
  EXPECT_EQ(parsed.getTag(), 3)
      << "tag should be parsed correctly despite nonzero reserved bits";
  EXPECT_EQ(parsed.getSupportedFlowCount(), 64)
      << "flow count should be parsed correctly despite nonzero reserved bits";
  EXPECT_EQ(parsed.getActivePortSet(), 0x00FF)
      << "active port set should be parsed independently from reserved bits";
}

} // namespace libsoftmetaroce
