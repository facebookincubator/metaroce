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
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/Encapsulation.h>
#include <libsoftmetaroce/EsunEncapsulation.h>
#include <libsoftmetaroce/Icrc.h>
#include <libsoftmetaroce/ProtocolCompatibility.h>
#include <algorithm>
#include <array>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

namespace libsoftmetaroce {
namespace {

// Nonzero field values make misplaced bits visible in the encoded bytes.
EsunHeader::Fields testFields() {
  return EsunHeader::Fields{
      .revision = 2,
      .flowLabelValid = true,
      .classOfService = 5,
      .ecn = 3,
      .flowLabel = 0x1234,
      .ttl = 9,
      .userDefined = 2,
  };
}

EsunHeader testHeader() {
  return EsunHeader(testFields());
}

std::array<uint8_t, EsunHeader::kSizeBytes> headerBytes(
    const EsunHeader& header) {
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender{&queue, EsunHeader::kSizeBytes};
  header.write(appender);
  auto buffer = queue.move();
  std::array<uint8_t, EsunHeader::kSizeBytes> bytes{};
  std::memcpy(bytes.data(), buffer->data(), bytes.size());
  return bytes;
}

class EsunEncapsulationTest : public ::testing::Test {
 protected:
  std::unique_ptr<folly::IOBuf> frame(
      std::string_view payload,
      std::optional<uint32_t> icrc = std::nullopt) {
    folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
    queue.append(folly::IOBuf::copyBuffer(payload));
    encapsulation_.frameOutgoing(queue, icrc);
    auto buffer = queue.move();
    buffer->coalesce();
    return buffer;
  }

  folly::MacAddress srcMac_{"00:11:22:33:44:55"};
  folly::MacAddress dstMac_{"66:77:88:99:aa:bb"};
  EsunEncapsulation encapsulation_{srcMac_, dstMac_, testHeader()};
};

TEST(EsunHeaderTest, DefaultsAreSafeAndConsistent) {
  const EsunHeader defaultHeader;
  const EsunHeader fieldsHeader(EsunHeader::Fields{});

  EXPECT_EQ(defaultHeader, fieldsHeader)
      << "default construction paths should encode identical ESUN headers";
  EXPECT_EQ(defaultHeader.revision(), 0)
      << "the default header should select the base ESUN revision";
  EXPECT_FALSE(defaultHeader.isFlowLabelValid())
      << "the default header should not advertise an unconfigured flow label";
  EXPECT_EQ(defaultHeader.classOfService(), 0)
      << "the default header should not select a traffic class";
  EXPECT_EQ(defaultHeader.ecn(), 0)
      << "the default header should not report congestion";
  EXPECT_EQ(defaultHeader.flowLabel(), 0)
      << "the default header should carry no flow label";
  EXPECT_EQ(defaultHeader.ttl(), EsunHeader::kDefaultEsunTtl)
      << "the default header should provide the default ESUN hop budget";
  EXPECT_EQ(defaultHeader.userDefined(), 0)
      << "the default header should leave user-defined bits unset";
  EXPECT_EQ(defaultHeader.reserved(), 0)
      << "locally constructed headers should clear reserved bits";
}

TEST(EsunHeaderTest, EncodesKnownNetworkBytesAndRoundTripsFields) {
  const EsunHeader header = testHeader();

  EXPECT_EQ(
      headerBytes(header), (std::array<uint8_t, 4>{0xB7, 0x12, 0x34, 0x98}))
      << "the header's fields should encode into the expected ESUN wire bytes";
  EXPECT_EQ(header.revision(), 2)
      << "the revision accessor should decode the known wire value";
  EXPECT_TRUE(header.isFlowLabelValid())
      << "the flow-label-valid accessor should decode the known wire bit";
  EXPECT_EQ(header.classOfService(), 5)
      << "the class-of-service accessor should decode the known wire value";
  EXPECT_EQ(header.ecn(), 3)
      << "the ECN accessor should decode the known wire value";
  EXPECT_EQ(header.flowLabel(), 0x1234)
      << "the flow-label accessor should decode the known wire value";
  EXPECT_EQ(header.ttl(), 9)
      << "the TTL accessor should decode the known wire value";
  EXPECT_EQ(header.userDefined(), 2)
      << "the user-defined accessor should decode the known wire value";
  EXPECT_EQ(header.reserved(), 0)
      << "locally encoded headers should leave reserved bits clear";
}

TEST(EsunHeaderTest, SerializesMaximumFieldsAndWritesReservedBitsAsZero) {
  const EsunHeader header(
      EsunHeader::Fields{
          .revision = 3,
          .flowLabelValid = true,
          .classOfService = 7,
          .ecn = 3,
          .flowLabel = 0xffff,
          .ttl = 15,
          .userDefined = 3,
      });

  EXPECT_EQ(
      headerBytes(header), (std::array<uint8_t, 4>{0xff, 0xff, 0xff, 0xfc}))
      << "maximum valid fields should fill every non-reserved wire bit";
  EXPECT_EQ(header.reserved(), 0)
      << "locally serialized headers should clear the reserved bits";
}

TEST(EsunHeaderTest, RejectsOutOfRangeLocalFields) {
  EXPECT_DEATH(EsunHeader(EsunHeader::Fields{.revision = 4}), "fields.revision")
      << "revision values wider than two bits should fail locally";
  EXPECT_DEATH(
      EsunHeader(EsunHeader::Fields{.classOfService = 8}),
      "fields.classOfService")
      << "class-of-service values wider than three bits should fail locally";
  EXPECT_DEATH(EsunHeader(EsunHeader::Fields{.ecn = 4}), "fields.ecn")
      << "ECN values wider than two bits should fail locally";
  EXPECT_DEATH(EsunHeader(EsunHeader::Fields{.ttl = 16}), "fields.ttl")
      << "TTL values wider than four bits should fail locally";
  EXPECT_DEATH(
      EsunHeader(EsunHeader::Fields{.userDefined = 4}), "fields.userDefined")
      << "user-defined values wider than two bits should fail locally";
}

TEST(EsunHeaderTest, SetClassOfServiceChangesOnlyTheCosBits) {
  EsunHeader header = testHeader();
  auto expectedFields = testFields();
  expectedFields.classOfService = 2;

  header.setClassOfService(2);

  EXPECT_EQ(headerBytes(header), headerBytes(EsunHeader(expectedFields)))
      << "configuring CoS on a socket must leave the revision, flow label, "
         "ECN, TTL and user-defined bits it shares a header with untouched";
}

TEST(EsunHeaderTest, SetClassOfServiceRejectsValuesWiderThanThreeBits) {
  EsunHeader header;
  EXPECT_DEATH(header.setClassOfService(8), "cos")
      << "a CoS wider than three bits would spill into the flow-label-valid "
         "bit";
}

TEST(EsunHeaderTest, ToleratesNonzeroReservedBitsOnReceive) {
  auto reservedSet =
      folly::IOBuf::copyBuffer(std::string("\xff\xff\xff\xff", 4));
  folly::io::Cursor cursor{reservedSet.get()};

  const EsunHeader received{cursor};

  EXPECT_EQ(received.reserved(), 3)
      << "receivers should tolerate nonzero reserved bits";
}

TEST_F(EsunEncapsulationTest, InheritsDefaultRawAddressResolution) {
  const auto address = folly::IPAddressV6("fe80::211:22ff:fe33:4455");
  const auto mac = folly::MacAddress("00:11:22:33:44:55");

  EXPECT_EQ(encapsulation_.resolveDstMac(address), mac)
      << "ESUN framing should inherit the default EUI-64 destination map";
  EXPECT_EQ(encapsulation_.resolveSrcAddr(mac), address)
      << "ESUN framing should inherit the default EUI-64 source map";
}

TEST_F(EsunEncapsulationTest, FramesKnownOuterBytes) {
  auto buffer = frame("A");
  const auto* data = buffer->data();
  const std::array<uint8_t, 18> expected{
      0x66,
      0x77,
      0x88,
      0x99,
      0xaa,
      0xbb,
      0x00,
      0x11,
      0x22,
      0x33,
      0x44,
      0x55,
      0xfc,
      0x0e,
      0xb7,
      0x12,
      0x34,
      0x98,
  };

  EXPECT_EQ(
      std::vector<uint8_t>(data, data + expected.size()),
      std::vector<uint8_t>(expected.begin(), expected.end()))
      << "the Ethernet and ESUN headers should precede the PDU in wire order";
  EXPECT_EQ(data[18], 'A') << "the PDU should follow the outer headers";
}

TEST_F(EsunEncapsulationTest, ClassOfServiceSetThroughBaseReachesTheWire) {
  Encapsulation& base = encapsulation_;
  auto expectedFields = testFields();
  expectedFields.classOfService = 1;

  base.setClassOfService(1);
  auto buffer = frame("A");
  folly::io::Cursor cursor(buffer.get());
  cursor.skip(kEthernetHeaderSize);
  const EsunHeader framed{cursor};

  EXPECT_EQ(framed, EsunHeader(expectedFields))
      << "the sender configures CoS through the Encapsulation interface, so "
         "the ESUN override must reach every outgoing header; framed CoS is "
      << static_cast<int>(framed.classOfService());
}

TEST_F(EsunEncapsulationTest, IcrcCoversThePduAndMinimumFramePadding) {
  for (const size_t payloadSize : {size_t{1}, size_t{37}, size_t{38}}) {
    auto buffer = frame(std::string(payloadSize, 'x'));
    auto covered = buffer->clone();
    covered->trimStart(kEthernetHeaderSize + EsunHeader::kSizeBytes);
    folly::io::Cursor cursor(covered.get());
    EXPECT_EQ(Icrc::computeICRC(cursor), Icrc::kExpectedIcrcValue)
        << "the mCRC must cover both the " << payloadSize
        << "-byte PDU and any zero padding before it";
  }
}

TEST_F(EsunEncapsulationTest, OuterHeadersAreOutsideIcrc) {
  auto buffer = frame("payload");
  buffer->writableData()[0] ^= 0xff;
  buffer->writableData()[14] ^= 0x40;

  const auto framing = encapsulation_.deframeIncoming(buffer);
  ASSERT_TRUE(framing)
      << "outer-header changes should not invalidate the transport iCRC";
  EXPECT_EQ(framing->ecn, 3)
      << "the receiver should still report ECN from the modified outer header";
  EXPECT_EQ(encapsulation_.header().revision(), 2)
      << "received headers must not alter later transmit configuration";
}

TEST_F(EsunEncapsulationTest, RoundTripPreservesPduAndReportsReceivedEcn) {
  // A 40-byte PDU makes the complete frame 62 bytes, above Ethernet's minimum,
  // so framing adds no minimum-frame padding.
  const std::string payload(40, 'p');
  // Distinguish the sender's wire ECN from the receiver's configured ECN.
  EsunEncapsulation sender{
      srcMac_,
      dstMac_,
      EsunHeader(EsunHeader::Fields{.ecn = 1, .flowLabel = 0x1234, .ttl = 9})};
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  queue.append(folly::IOBuf::copyBuffer(payload));
  sender.frameOutgoing(queue, std::nullopt);
  auto buffer = queue.move();
  buffer->coalesce();

  const auto framing = encapsulation_.deframeIncoming(buffer);
  ASSERT_TRUE(framing) << "a valid ESUN frame should deframe";
  folly::io::Cursor cursor(buffer.get());
  EXPECT_EQ(encapsulation_.payloadSize(cursor), payload.size())
      << "a 4-byte-aligned PDU above the minimum frame needs no pad";
  EXPECT_EQ(
      std::string_view(
          reinterpret_cast<const char*>(buffer->data()), payload.size()),
      payload)
      << "an ESUN frame round trip should preserve the exact PDU bytes";
  EXPECT_EQ(framing->ecn, 1)
      << "the received ECN should come from the frame, not from "
         "the transmit header";
  EXPECT_EQ(encapsulation_.header().ecn(), 3)
      << "receive processing should preserve the configured transmit header";
}

TEST_F(EsunEncapsulationTest, RejectsShortWrongEthertypeAndCorruptIcrc) {
  auto shortFrame = folly::IOBuf::copyBuffer(std::string(21, '\0'));
  EXPECT_FALSE(encapsulation_.deframeIncoming(shortFrame).has_value())
      << "a frame shorter than the ESUN headers and mCRC must be rejected";

  auto wrongEthertype = frame("payload");
  wrongEthertype->writableData()[12] = 0x12;
  wrongEthertype->writableData()[13] = 0x34;
  EXPECT_FALSE(encapsulation_.deframeIncoming(wrongEthertype).has_value())
      << "an ESUN receiver must reject a frame with the wrong EtherType";

  auto corrupt = frame("payload");
  corrupt->writableData()[18] ^= 0x01;
  EXPECT_FALSE(encapsulation_.deframeIncoming(corrupt).has_value())
      << "an ESUN receiver must reject a frame with a corrupt mCRC";
}

TEST_F(EsunEncapsulationTest, PadsShortFramesBeforeTheIcrc) {
  constexpr size_t kMinimumEthernetFrameSize = 60;
  constexpr size_t kOuterSize = kEthernetHeaderSize + EsunHeader::kSizeBytes;
  constexpr size_t kPduSize = 13;
  constexpr size_t kPaddingSize =
      kMinimumEthernetFrameSize - kOuterSize - kPduSize - kIcrcSize;
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  queue.append(folly::IOBuf::copyBuffer(std::string(kPduSize, 'x')));

  const size_t framingBytes = encapsulation_.frameOutgoing(queue, std::nullopt);
  auto buffer = queue.move();
  buffer->coalesce();

  EXPECT_EQ(buffer->length(), kMinimumEthernetFrameSize)
      << "a short ESUN packet must reach the Ethernet minimum before send";
  EXPECT_EQ(framingBytes, encapsulation_.framingOverheadBytes() + kPaddingSize)
      << "the reported framing bytes must include the dynamic minimum-frame pad";
  const auto* padding = buffer->data() + kOuterSize + kPduSize;
  EXPECT_EQ(
      std::count(padding, padding + kPaddingSize, uint8_t{0}), kPaddingSize)
      << "the minimum-frame padding before the mCRC must be zero-filled";
  auto covered = buffer->clone();
  covered->trimStart(kOuterSize);
  folly::io::Cursor cursor(covered.get());
  EXPECT_EQ(Icrc::computeICRC(cursor), Icrc::kExpectedIcrcValue)
      << "the final mCRC must cover the PDU and its minimum-frame padding";
}

TEST_F(EsunEncapsulationTest, DoesNotPadFramesAtOrAboveTheMinimum) {
  constexpr size_t kPduAtMinimum =
      60 - kEthernetHeaderSize - EsunHeader::kSizeBytes - kIcrcSize;
  for (const size_t pduSize : {kPduAtMinimum, size_t{100}}) {
    folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
    queue.append(folly::IOBuf::copyBuffer(std::string(pduSize, 'x')));

    const size_t framingBytes =
        encapsulation_.frameOutgoing(queue, std::nullopt);

    EXPECT_EQ(
        queue.chainLength(), encapsulation_.framingOverheadBytes() + pduSize)
        << "a " << pduSize << "-byte PDU already reaches the minimum";
    EXPECT_EQ(framingBytes, encapsulation_.framingOverheadBytes())
        << "a frame at or above the minimum needs no dynamic padding";
  }
}

TEST_F(EsunEncapsulationTest, CorruptIcrcDoesNotPublishMetadataOrMutateTx) {
  auto corrupt = frame("payload");
  corrupt->writableData()[18] ^= 0x01;

  EXPECT_FALSE(encapsulation_.deframeIncoming(corrupt).has_value())
      << "a corrupt frame should be dropped";

  EXPECT_EQ(encapsulation_.header(), testHeader())
      << "a rejected receive must leave transmit configuration unchanged";
}

TEST_F(EsunEncapsulationTest, DisabledCrcValidationStillReportsEcn) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_validate_crc = false;
  auto corrupt = frame("payload");
  corrupt->writableData()[18] ^= 0x01;

  const auto framing = encapsulation_.deframeIncoming(corrupt);
  ASSERT_TRUE(framing)
      << "disabled CRC validation should preserve fault-injection behavior";
  EXPECT_EQ(framing->ecn, 3)
      << "disabling CRC validation should not discard valid ESUN metadata";
}

TEST_F(EsunEncapsulationTest, UsesProvidedIcrc) {
  constexpr uint32_t kIcrc = 0x12345678;
  auto buffer = frame("payload", kIcrc);
  uint32_t framedIcrc;
  std::memcpy(
      &framedIcrc,
      buffer->data() + buffer->length() - sizeof(framedIcrc),
      sizeof(framedIcrc));

  EXPECT_EQ(framedIcrc, kIcrc)
      << "the framing should write the supplied iCRC unchanged";
  EXPECT_FALSE(encapsulation_.deframeIncoming(buffer).has_value())
      << "an overridden iCRC should fail validation";
}

} // namespace
} // namespace libsoftmetaroce
