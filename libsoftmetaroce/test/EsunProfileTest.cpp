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

#include <net/if.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/Ecn.h>
#include <libsoftmetaroce/EsunEncapsulation.h>
#include <libsoftmetaroce/EsunHeader.h>
#include <libsoftmetaroce/EsunProfile.h>
#include <libsoftmetaroce/GidDiscovery.h>
#include <libsoftmetaroce/Icrc.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/DataHeader.h>
#include <libsoftmetaroce/codec/MetaRoceCodec.h>
#include <libsoftmetaroce/codec/Packet.h>
#include <libsoftmetaroce/codec/ReadWriteHeader.h>

#include <array>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

DECLARE_string(libsoftmetaroce_esun_gids);
DECLARE_uint32(libsoftmetaroce_esun_ttl);

namespace libsoftmetaroce {
namespace {

DiscoveredAddress linkLocalCandidate() {
  return DiscoveredAddress{
      .scopelessAddress = folly::IPAddress("fe80::211:22ff:fe44:5566"),
      .interfaceName = "eth0",
      .interfaceIndex = 7,
      .scopeId = 7,
      .interfaceUp = true,
  };
}

ReadWritePacket makeWriteImmPacket() {
  return ReadWritePacket(
      BaseHeader(
          BaseHeader::Opcode(BaseHeader::Opcode::OperationType::WRITE_IMM_P),
          /*version=*/1,
          /*flowId=*/0,
          /*dstQp=*/1,
          /*srcQp=*/1),
      DataHeader(
          /*fsn=*/0,
          /*una=*/0,
          /*tag=*/0,
          /*ackRequest=*/false,
          /*isRetransmit=*/false,
          /*isTimeout=*/false,
          /*posn=*/0,
          /*csn=*/0,
          /*msn=*/0,
          /*rateHints=*/0,
          /*timestamp=*/0),
      ReadWriteHeader(/*messageAddr=*/0, /*key=*/0, /*messageLength=*/0),
      /*payloadAddress=*/nullptr,
      /*payloadSizeBytes=*/0,
      /*maybeImmData=*/0);
}

DataPacket makeSendLastPacket(const void* payload, uint32_t payloadSizeBytes) {
  return DataPacket(
      BaseHeader(
          BaseHeader::Opcode(BaseHeader::Opcode::OperationType::SEND_LAST),
          /*version=*/1,
          /*flowId=*/0,
          /*dstQp=*/1,
          /*srcQp=*/1),
      DataHeader(
          /*fsn=*/0,
          /*una=*/0,
          /*tag=*/0,
          /*ackRequest=*/false,
          /*isRetransmit=*/false,
          /*isTimeout=*/false,
          /*posn=*/0,
          /*csn=*/0,
          /*msn=*/0,
          /*rateHints=*/0,
          /*timestamp=*/0,
          /*padding=*/Packet::payloadPadBytes(payloadSizeBytes)),
      payload,
      payloadSizeBytes);
}

TEST(EsunProfileTest, EmptyFlagDoesNotRegisterRule) {
  const gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_esun_gids = " , ";
  GidRuleRegistry rules;

  auto addresses = configuredEsunAddresses();
  EXPECT_TRUE(addresses.empty())
      << "whitespace-only input must configure no MetaRoCE-over-ESUN addresses";
  registerEsunGidRule(rules, std::move(addresses));

  EXPECT_TRUE(rules.makeChain(ScanConditions{})
                  .classify(linkLocalCandidate())
                  .registered.empty())
      << "an empty address set must add no MetaRoCE-over-ESUN classification";
}

TEST(EsunProfileTest, RejectsNonLinkLocalAddress) {
  const gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_esun_gids = "2001:db8::1";

  EXPECT_THROW(configuredEsunAddresses(), std::runtime_error)
      << "a non-link-local address must be rejected before installation";
}

TEST(EsunProfileTest, FlagRegistersClassificationRule) {
  const gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_esun_gids = "fe80:0000:0000:0000:0211:22ff:fe44:5566";
  GidRuleRegistry rules;

  registerEsunGidRule(rules, configuredEsunAddresses());

  EXPECT_EQ(
      rules.makeChain(ScanConditions{})
          .classify(linkLocalCandidate())
          .registered,
      std::vector<uint32_t>{kGidTypeEsun})
      << "the configured address must select the MetaRoCE-over-ESUN profile";
}

TEST(EsunProfileTest, BuildsMetaRoceOverEsunProfile) {
  const auto profile = makeEsunProfile();
  const auto entry = buildGidEntry(linkLocalCandidate(), kGidTypeEsun);

  EXPECT_EQ(profile.socketKind, SocketKind::RAW)
      << "MetaRoCE-over-ESUN must select a raw AF_PACKET socket";
  EXPECT_FALSE(profile.supportsAlternateReceiver)
      << "raw AF_PACKET lacks the port demux a second receiver needs";
  EXPECT_EQ(profile.rawEthertype, kProvisionalEsunEthertype)
      << "the socket must filter on the EtherType the framing writes";
  EXPECT_NE(dynamic_cast<MetaRoceCodec*>(profile.makeCodec().get()), nullptr)
      << "the profile must preserve the MetaRoCE wire codec";
  EXPECT_NE(
      dynamic_cast<EsunEncapsulation*>(profile.makeEncapsulation(entry).get()),
      nullptr)
      << "the profile must build ESUN framing";
}

TEST(EsunProfileTest, EncapsulationUsesTtlFlag) {
  const gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_esun_ttl = 3;
  const auto encapsulation = makeEsunProfile().makeEncapsulation(
      buildGidEntry(linkLocalCandidate(), kGidTypeEsun));
  const auto* esun = dynamic_cast<EsunEncapsulation*>(encapsulation.get());
  ASSERT_NE(esun, nullptr);

  EXPECT_EQ(esun->header().ttl(), 3)
      << "operators rely on the flag to bound how many switches a frame may "
         "cross";
}

TEST(EsunProfileTest, EncapsulationMarksEct0) {
  const auto encapsulation = makeEsunProfile().makeEncapsulation(
      buildGidEntry(linkLocalCandidate(), kGidTypeEsun));
  const auto* esun = dynamic_cast<EsunEncapsulation*>(encapsulation.get());
  ASSERT_NE(esun, nullptr);

  EXPECT_EQ(esun->header().ecn(), kEcnECT0)
      << "spec 6.1 requires data packets to carry ECT(0) so switches can mark "
         "congestion";
}

TEST(EsunProfileTest, ShortSendKeepsItsLengthThroughEsunFraming) {
  const auto profile = makeEsunProfile();
  const auto codec = profile.makeCodec();
  const auto encapsulation = profile.makeEncapsulation(
      buildGidEntry(linkLocalCandidate(), kGidTypeEsun));
  const std::array<uint8_t, 1> payload = {0xAB};
  const auto packet = makeSendLastPacket(payload.data(), payload.size());
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&queue, codec->getSerializedSize(packet));
  codec->serialize(packet, appender);
  encapsulation->frameOutgoing(queue, std::nullopt);
  auto frame = queue.move();
  frame->coalesce();
  ASSERT_EQ(frame->length(), kMinimumEthernetFrameSizeBytes)
      << "precondition: a one-byte send needs minimum-frame fill over ESUN";

  ASSERT_TRUE(encapsulation->deframeIncoming(frame).has_value())
      << "precondition: the receiver accepts its own ESUN frame";
  folly::io::Cursor cursor(frame.get());
  const auto parsed = codec->parse(cursor, *encapsulation);
  const auto* dataPacket = dynamic_cast<const DataPacket*>(parsed.get());
  ASSERT_NE(dataPacket, nullptr);
  EXPECT_EQ(dataPacket->getPayloadSizeBytes(), payload.size())
      << "spec 5.2.5 makes fill that the Padding field does not count a "
         "length error, so the sender must count the minimum-frame fill there";
}

TEST(EsunProfileTest, MtuIncludesEsunFraming) {
  const auto profile = makeEsunProfile();
  auto entry = buildGidEntry(linkLocalCandidate(), kGidTypeEsun);
  entry.ndev_ifindex = if_nametoindex("lo");
  ASSERT_NE(entry.ndev_ifindex, 0u)
      << "precondition: the loopback interface must exist";

  const auto writeImmPacket = makeWriteImmPacket();
  constexpr size_t kLoopbackMtuBytes = 65536;
  constexpr size_t kEsunFramingBytesWithinMtu =
      EsunHeader::kSizeBytes + kIcrcSize;
  const size_t maximumQpMtuBytes = kLoopbackMtuBytes -
      writeImmPacket.getSizeBytes() - kEsunFramingBytesWithinMtu;
  ASSERT_LT(maximumQpMtuBytes, std::numeric_limits<uint16_t>::max())
      << "precondition: the first rejected MTU must not overflow uint16_t";
  const auto maximumQpMtu = static_cast<uint16_t>(maximumQpMtuBytes);

  EXPECT_TRUE(profile.validateMtu(entry, maximumQpMtu))
      << "the exact QP MTU boundary must include ESUN framing";
  EXPECT_FALSE(profile.validateMtu(entry, maximumQpMtu + 1))
      << "one byte beyond the ESUN-framed boundary must be rejected";
}

} // namespace
} // namespace libsoftmetaroce
