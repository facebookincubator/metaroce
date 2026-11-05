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

#include <libsoftmetaroce/EgressProfile.h>

#include <folly/SharedMutex.h>
#include <folly/container/F14Map.h>
#include <glog/logging.h>
#include <net/if.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/EthernetEncapsulation.h>
#include <libsoftmetaroce/UdpEncapsulation.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/DataHeader.h>
#include <libsoftmetaroce/codec/MetaRoceCodec.h>
#include <libsoftmetaroce/codec/ReadWriteHeader.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <cstdio>

namespace libsoftmetaroce {

bool validateRawMtu(
    const ibv_gid_entry& entry,
    uint16_t qpMtuBytes,
    uint16_t framingBytesWithinMtu) {
  char ifName[IF_NAMESIZE];
  if (!if_indextoname(entry.ndev_ifindex, ifName)) {
    return true;
  }
  struct ifreq ifr = {};
  static_assert(
      sizeof(ifr.ifr_name) == sizeof(ifName),
      "a divergence here would silently truncate the interface name");
  // snprintf always NUL-terminates within the bound.
  snprintf(ifr.ifr_name, IFNAMSIZ, "%s", ifName);
  const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    return true;
  }
  bool valid = true;
  if (::ioctl(fd, SIOCGIFMTU, &ifr) == 0) {
    // ifr_mtu is the L3 payload capacity (Ethernet header already excluded).
    constexpr size_t kImmediateSizeBytes = sizeof(uint32_t);
    constexpr size_t kWriteImmHeaderSizeBytes =
        BaseHeader::kBaseHeaderSizeBytes + DataHeader::kDataHeaderSizeBytes +
        ReadWriteHeader::kReadWriteHeaderSizeBytes + kImmediateSizeBytes;
    const int maximumQpMtuBytes = ifr.ifr_mtu -
        static_cast<int>(kWriteImmHeaderSizeBytes) - framingBytesWithinMtu;
    if (qpMtuBytes > maximumQpMtuBytes) {
      LOG(ERROR) << "QP MTU " << qpMtuBytes << " exceeds Ethernet interface "
                 << ifName << " capacity (maximum QP MTU " << maximumQpMtuBytes
                 << " bytes, interface MTU=" << ifr.ifr_mtu << ")";
      valid = false;
    }
  }
  ::close(fd);
  return valid;
}

namespace {

std::unique_ptr<Codec> makeMetaRoceCodec() {
  return std::make_unique<MetaRoceCodec>();
}

std::unique_ptr<Codec> makeMetaRoceEthernetCodec() {
  return std::make_unique<MetaRoceCodec>(
      EthernetEncapsulation::kMinimumPacketSizeBytes);
}

std::unique_ptr<Encapsulation> makeUdpEncapsulation(const ibv_gid_entry&) {
  return std::make_unique<UdpEncapsulation>();
}

std::unique_ptr<Encapsulation> makeEthernetEncapsulation(
    const ibv_gid_entry& entry) {
  const auto address = folly::IPAddressV6::fromBinary(
      folly::ByteRange(entry.gid.raw, sizeof(entry.gid.raw)));
  return std::make_unique<EthernetEncapsulation>(
      RawEncapsulation::macFromLinkLocal(address), folly::MacAddress());
}

bool validateUdpMtu(const ibv_gid_entry&, uint16_t) {
  return true;
}

EgressProfile makeMetaRoceUdpProfile() {
  EgressProfile profile{
      .socketKind = SocketKind::UDP,
      .makeCodec = makeMetaRoceCodec,
      .makeEncapsulation = makeUdpEncapsulation,
      .validateMtu = validateUdpMtu,
      .supportsAlternateReceiver = true,
      .rawEthertype = std::nullopt,
  };
  CHECK(profile.isUsable()) << "the built-in UDP profile must be usable";
  return profile;
}

} // namespace

bool EgressProfile::isUsable() const noexcept {
  if (!makeCodec || !makeEncapsulation || !validateMtu) {
    return false;
  }
  return socketKind != SocketKind::RAW ||
      (rawEthertype.has_value() && !supportsAlternateReceiver);
}

EgressProfile makeMetaRoceEthernetProfile() {
  EgressProfile profile{
      .socketKind = SocketKind::RAW,
      .makeCodec = makeMetaRoceEthernetCodec,
      .makeEncapsulation = makeEthernetEncapsulation,
      .validateMtu =
          [](const ibv_gid_entry& entry, uint16_t qpMtuBytes) {
            return validateRawMtu(
                entry, qpMtuBytes, /*framingBytesWithinMtu=*/0);
          },
      .supportsAlternateReceiver = false,
      .rawEthertype = kDefaultMetaRoceEthertype,
  };
  CHECK(profile.isUsable()) << "the built-in Ethernet profile must be usable";
  return profile;
}

EgressRegistry::EgressRegistry() {
  const bool seeded =
      registerProfile(kGidTypeMetaRoceUdp, makeMetaRoceUdpProfile());
  CHECK(seeded)
      << "the built-in UDP profile must register into a fresh registry";
}

// NOLINTNEXTLINE(performance-unnecessary-value-param)
bool EgressRegistry::registerProfile(uint32_t gidType, EgressProfile profile) {
  if (!profile.isUsable()) {
    return false;
  }
  return profiles_.wlock()->emplace(gidType, std::move(profile)).second;
}

EgressProfile EgressRegistry::getProfile(uint32_t gidType) const {
  const auto profiles = profiles_.rlock();
  const auto it = profiles->find(gidType);
  if (it != profiles->end()) {
    return it->second;
  }
  const auto udpIt = profiles->find(kGidTypeMetaRoceUdp);
  CHECK(udpIt != profiles->end()); // seeded by the constructor, never removed
  return udpIt->second;
}

bool EgressRegistry::validateMtu(const ibv_gid_entry& entry, uint16_t mtu)
    const {
  return getProfile(entry.gid_type).validateMtu(entry, mtu);
}

EgressRegistry& defaultEgressRegistry() {
  static EgressRegistry registry;
  return registry;
}

} // namespace libsoftmetaroce
