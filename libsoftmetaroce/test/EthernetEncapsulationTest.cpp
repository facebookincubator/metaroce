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
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/EgressProfile.h>
#include <libsoftmetaroce/EthernetEncapsulation.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/Codec.h>
#include <libsoftmetaroce/codec/DataHeader.h>
#include <libsoftmetaroce/codec/Packet.h>
#include <algorithm>
#include <array>
#include <string_view>
#include <vector>

namespace libsoftmetaroce {

class EthernetEncapsulationTest : public ::testing::Test {
 protected:
  folly::MacAddress srcMac_{"00:11:22:33:44:55"};
  folly::MacAddress dstMac_{"66:77:88:99:aa:bb"};
  EthernetEncapsulation encap_{srcMac_, dstMac_};
};

TEST_F(EthernetEncapsulationTest, PayloadSizeIsEntireBuffer) {
  auto buf = folly::IOBuf::copyBuffer("test_payload");
  folly::io::Cursor cursor(buf.get());
  EXPECT_EQ(encap_.payloadSize(cursor), cursor.totalLength())
      << "Ethernet has no trailing framing; all remaining bytes are payload";
}

TEST_F(EthernetEncapsulationTest, FrameOutgoingPrependsEthernetHeader) {
  // Create a small payload to frame.
  const std::string payload = "MetaRoCE";
  folly::IOBufQueue queue;
  queue.append(folly::IOBuf::copyBuffer(payload));

  size_t added = encap_.frameOutgoing(queue, std::nullopt);

  const size_t kPaddingSize =
      kMinimumEthernetFrameSizeBytes - kEthernetHeaderSize - payload.size();
  EXPECT_EQ(added, kEthernetHeaderSize + kPaddingSize)
      << "frameOutgoing should report its Ethernet header and minimum-frame "
         "padding";

  auto buf = queue.move();
  buf->coalesce();
  EXPECT_EQ(buf->length(), kMinimumEthernetFrameSizeBytes)
      << "a short Ethernet frame should reach the 60-byte minimum";

  // Verify Ethernet header fields.
  const uint8_t* data = buf->data();
  // Destination MAC at offset 0.
  EXPECT_EQ(std::memcmp(data, dstMac_.bytes(), 6), 0)
      << "first 6 bytes should be destination MAC";
  // Source MAC at offset 6.
  EXPECT_EQ(std::memcmp(data + 6, srcMac_.bytes(), 6), 0)
      << "bytes 6-11 should be source MAC";
  // Ethertype at offset 12 in network byte order.
  uint16_t ethertype;
  std::memcpy(&ethertype, data + 12, 2);
  EXPECT_EQ(ntohs(ethertype), kDefaultMetaRoceEthertype)
      << "ethertype should be the default MetaRoCE ethertype";
  // Payload follows.
  EXPECT_EQ(std::memcmp(data + 14, payload.data(), payload.size()), 0)
      << "payload should follow the Ethernet header unchanged";
  EXPECT_EQ(
      std::count(
          data + kEthernetHeaderSize + payload.size(),
          data + kMinimumEthernetFrameSizeBytes,
          uint8_t{0}),
      kPaddingSize)
      << "minimum-frame padding should be zero-filled";
}

TEST_F(EthernetEncapsulationTest, OriginalIcrcIsIgnored) {
  folly::IOBufQueue queue;
  const std::string payload(
      kMinimumEthernetFrameSizeBytes - kEthernetHeaderSize, 'x');
  queue.append(folly::IOBuf::copyBuffer(payload));

  // Even with an originalIcrc, no mCRC is added.
  size_t added = encap_.frameOutgoing(queue, 0xDEADBEEF);

  EXPECT_EQ(added, kEthernetHeaderSize)
      << "originalIcrc should be ignored for direct Ethernet";
  auto buf = queue.move();
  buf->coalesce();
  EXPECT_EQ(buf->length(), kMinimumEthernetFrameSizeBytes)
      << "no extra bytes beyond Ethernet header should be added";
}

TEST_F(EthernetEncapsulationTest, DeframeIncomingStripsHeader) {
  // Build a fake frame: 14-byte Ethernet header + MetaRoCE data.
  const std::string metaroceData = "METAROCE_PAYLOAD";
  std::vector<uint8_t> frame(14 + metaroceData.size());
  std::memcpy(frame.data(), dstMac_.bytes(), 6);
  std::memcpy(frame.data() + 6, srcMac_.bytes(), 6);
  uint16_t ethertype = htons(kDefaultMetaRoceEthertype);
  std::memcpy(frame.data() + 12, &ethertype, 2);
  std::memcpy(frame.data() + 14, metaroceData.data(), metaroceData.size());

  auto buf = folly::IOBuf::copyBuffer(frame.data(), frame.size());
  EXPECT_TRUE(encap_.deframeIncoming(buf).has_value())
      << "deframeIncoming should succeed";
  EXPECT_EQ(buf->length(), metaroceData.size())
      << "after stripping, only MetaRoCE data should remain";
  EXPECT_EQ(
      std::memcmp(buf->data(), metaroceData.data(), metaroceData.size()), 0)
      << "MetaRoCE data should be intact after stripping";
}

TEST_F(EthernetEncapsulationTest, DeframeRejectsTooShortFrame) {
  auto buf = folly::IOBuf::copyBuffer("short");
  EXPECT_FALSE(encap_.deframeIncoming(buf).has_value())
      << "frames shorter than 14 bytes should be rejected";
}

TEST_F(EthernetEncapsulationTest, RoundTrip) {
  const std::string payload(
      kMinimumEthernetFrameSizeBytes - kEthernetHeaderSize, 'x');
  folly::IOBufQueue queue;
  queue.append(folly::IOBuf::copyBuffer(payload));

  encap_.frameOutgoing(queue, std::nullopt);
  auto buf = queue.move();

  EXPECT_TRUE(encap_.deframeIncoming(buf).has_value());
  EXPECT_EQ(buf->length(), payload.size());
  EXPECT_EQ(std::memcmp(buf->data(), payload.data(), payload.size()), 0)
      << "round-trip should preserve the original payload";
}

TEST_F(
    EthernetEncapsulationTest,
    ShortDataFramesReachMinimumAndDiscloseAllPadding) {
  struct TestCase {
    std::string_view payload;
    uint8_t paddingBeforeFraming;
    uint8_t expectedPadding;
  };
  constexpr std::array<TestCase, 2> kTestCases{{
      {.payload = "", .paddingBeforeFraming = 0, .expectedPadding = 14},
      {.payload = "x", .paddingBeforeFraming = 3, .expectedPadding = 13},
  }};
  const auto codec = makeMetaRoceEthernetProfile().makeCodec();

  for (const auto& testCase : kTestCases) {
    SCOPED_TRACE(
        ::testing::Message() << "payload size: " << testCase.payload.size());
    const BaseHeader baseHeader =
        BaseHeader::BaseHeaderBuilder()
            .setOpcode(
                BaseHeader::Opcode(BaseHeader::Opcode::OperationType::SEND))
            .setVersion(0)
            .setFlowId(0)
            .setDstQp(1)
            .setSrcQp(2)
            .build();
    const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                      .setFsn(0)
                                      .setUna(0)
                                      .setPadding(testCase.paddingBeforeFraming)
                                      .setPosn(0)
                                      .setMsn(0)
                                      .build();
    const DataPacket packet{
        baseHeader,
        dataHeader,
        testCase.payload.data(),
        testCase.payload.size()};
    folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
    folly::io::QueueAppender appender{&queue, codec->getSerializedSize(packet)};
    codec->serialize(packet, appender);

    const size_t added = encap_.frameOutgoing(queue, std::nullopt);
    auto frame = queue.move();
    frame->coalesce();

    EXPECT_EQ(frame->length(), kMinimumEthernetFrameSizeBytes)
        << "a short data frame should reach the Ethernet minimum";
    EXPECT_EQ(added, kEthernetHeaderSize)
        << "the profile's codec fills the packet to the minimum, leaving "
           "framing no fill to add outside the Padding field";

    folly::io::Cursor cursor{frame.get()};
    cursor.skip(kEthernetHeaderSize);
    const BaseHeader parsedBaseHeader{cursor};
    ASSERT_TRUE(parsedBaseHeader.getOpcode().isData())
        << "the serialized packet should retain its data opcode";
    const DataHeader parsedDataHeader{cursor};
    EXPECT_EQ(parsedDataHeader.getPadding(), testCase.expectedPadding)
        << "the data header should disclose every trailing padding byte";
    cursor.skip(testCase.payload.size());
    std::vector<uint8_t> padding(testCase.expectedPadding);
    cursor.pull(padding.data(), padding.size());
    EXPECT_EQ(
        std::count(padding.begin(), padding.end(), uint8_t{0}), padding.size())
        << "minimum-frame padding should be zero-filled";

    ASSERT_TRUE(encap_.deframeIncoming(frame).has_value())
        << "precondition: the receiver accepts its own Ethernet frame";
    folly::io::Cursor parseCursor{frame.get()};
    const auto parsed = codec->parse(parseCursor, encap_);
    const auto* dataPacket = dynamic_cast<const DataPacket*>(parsed.get());
    ASSERT_NE(dataPacket, nullptr);
    EXPECT_EQ(dataPacket->getPayloadSizeBytes(), testCase.payload.size())
        << "the receiver derives the payload length from the Padding field, so "
           "a short send keeps its exact length";
  }
}

TEST_F(EthernetEncapsulationTest, MacFromLinkLocal) {
  // Link-local fe80::211:22ff:fe33:4455 embeds MAC 00:11:22:33:44:55
  // (EUI-64: byte[0] XOR 0x02, insert ff:fe in the middle).
  auto addr = folly::IPAddressV6("fe80::211:22ff:fe33:4455");
  auto mac = RawEncapsulation::macFromLinkLocal(addr);
  EXPECT_EQ(mac, folly::MacAddress("00:11:22:33:44:55"))
      << "macFromLinkLocal should reverse EUI-64 encoding";
}

TEST_F(EthernetEncapsulationTest, MacFromLinkLocalRejectsNonLinkLocal) {
  auto addr = folly::IPAddressV6("2001:db8::1");
  EXPECT_THROW(RawEncapsulation::macFromLinkLocal(addr), std::runtime_error)
      << "non-link-local addresses should throw";
}

TEST_F(EthernetEncapsulationTest, LinkLocalFromMac) {
  auto mac = folly::MacAddress("00:11:22:33:44:55");
  auto addr = RawEncapsulation::linkLocalFromMac(mac);
  EXPECT_EQ(addr, folly::IPAddressV6("fe80::211:22ff:fe33:4455"))
      << "linkLocalFromMac should produce EUI-64 link-local address";
}

TEST_F(EthernetEncapsulationTest, LinkLocalRoundTrip) {
  auto mac = folly::MacAddress("9e:8c:80:f0:a8:80");
  auto addr = RawEncapsulation::linkLocalFromMac(mac);
  auto recovered = RawEncapsulation::macFromLinkLocal(addr);
  EXPECT_EQ(recovered, mac)
      << "linkLocalFromMac -> macFromLinkLocal should recover the original MAC";
}

TEST_F(EthernetEncapsulationTest, InheritsDefaultRawAddressResolution) {
  const auto address = folly::IPAddressV6("fe80::211:22ff:fe33:4455");
  const auto mac = folly::MacAddress("00:11:22:33:44:55");

  EXPECT_EQ(encap_.resolveDstMac(address), mac)
      << "Ethernet framing should inherit the default EUI-64 destination map";
  EXPECT_EQ(encap_.resolveSrcAddr(mac), address)
      << "Ethernet framing should inherit the default EUI-64 source map";
}

TEST_F(EthernetEncapsulationTest, ConsecutiveFramesGetCorrectDstMac) {
  // Simulate two packets to different destinations. Each frame's
  // Ethernet header must carry the MAC for *that* packet's destination,
  // not the previous one's.
  const auto addr1 = folly::IPAddressV6("fe80::211:22ff:fe33:4455");
  const auto addr2 = folly::IPAddressV6("fe80::6877:88ff:fe99:aabb");
  const auto mac1 = RawEncapsulation::macFromLinkLocal(addr1);
  const auto mac2 = RawEncapsulation::macFromLinkLocal(addr2);

  for (int i = 0; i < 2; ++i) {
    const auto& addr = (i == 0) ? addr1 : addr2;
    const auto& expectedMac = (i == 0) ? mac1 : mac2;

    encap_.setDstMac(RawEncapsulation::macFromLinkLocal(addr));

    folly::IOBufQueue queue;
    queue.append(folly::IOBuf::copyBuffer("payload"));
    encap_.frameOutgoing(queue, std::nullopt);

    auto buf = queue.move();
    buf->coalesce();
    EXPECT_EQ(
        std::memcmp(buf->data(), expectedMac.bytes(), folly::MacAddress::SIZE),
        0)
        << "frame " << i << " should carry destination MAC for " << addr;
  }
}

} // namespace libsoftmetaroce
