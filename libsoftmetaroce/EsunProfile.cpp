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

#include <libsoftmetaroce/EsunProfile.h>

#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/Ecn.h>
#include <libsoftmetaroce/EsunEncapsulation.h>
#include <libsoftmetaroce/EsunHeader.h>
#include <libsoftmetaroce/GidDiscovery.h>
#include <libsoftmetaroce/Icrc.h>
#include <libsoftmetaroce/RawEncapsulation.h>
#include <libsoftmetaroce/codec/MetaRoceCodec.h>

#include <fmt/core.h>
#include <folly/IPAddress.h>
#include <folly/MacAddress.h>
#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

DEFINE_string(
    libsoftmetaroce_esun_gids,
    "",
    "Comma-separated local IPv6 link-local addresses with embedded MACs to "
    "classify as MetaRoCE-over-ESUN GIDs. Empty disables classification.");

DEFINE_uint32(
    libsoftmetaroce_esun_ttl,
    libsoftmetaroce::EsunHeader::Fields{}.ttl,
    "Initial TTL for ESUN packets (1-15).");

static bool validateEsunTtl(const char* /*name*/, uint32_t value) {
  return value >= 1 && value <= 15;
}
DEFINE_validator(libsoftmetaroce_esun_ttl, &validateEsunTtl);

namespace libsoftmetaroce {
namespace {

std::unique_ptr<Encapsulation> makeEsunEncapsulation(
    const ibv_gid_entry& entry) {
  const auto address = folly::IPAddressV6::fromBinary(
      folly::ByteRange(entry.gid.raw, sizeof(entry.gid.raw)));
  return std::make_unique<EsunEncapsulation>(
      RawEncapsulation::macFromLinkLocal(address),
      folly::MacAddress(),
      EsunHeader(
          EsunHeader::Fields{
              .ecn = kEcnECT0,
              .ttl = static_cast<uint8_t>(FLAGS_libsoftmetaroce_esun_ttl)}));
}

} // namespace

EgressProfile makeEsunProfile() {
  EgressProfile profile{
      .socketKind = SocketKind::RAW,
      .makeCodec = [] { return std::make_unique<MetaRoceCodec>(); },
      .makeEncapsulation = makeEsunEncapsulation,
      .validateMtu =
          [](const ibv_gid_entry& entry, uint16_t qpMtuBytes) {
            constexpr uint16_t kEsunFramingBytesWithinMtu =
                EsunHeader::kSizeBytes + kIcrcSize;
            return validateRawMtu(
                entry, qpMtuBytes, kEsunFramingBytesWithinMtu);
          },
      .supportsAlternateReceiver = false,
      .rawEthertype = kProvisionalEsunEthertype,
  };
  CHECK(profile.isUsable()) << "the MetaRoCE-over-ESUN profile must be usable";
  return profile;
}

std::set<folly::IPAddress> configuredEsunAddresses() {
  std::set<folly::IPAddress> addresses;
  std::vector<folly::StringPiece> parts;
  folly::split(',', FLAGS_libsoftmetaroce_esun_gids, parts);
  for (const auto part : parts) {
    const auto trimmed = folly::trimWhitespace(part);
    if (trimmed.empty()) {
      continue;
    }
    auto address = folly::IPAddress::tryFromString(trimmed);
    if (address.hasError()) {
      throw std::runtime_error(
          fmt::format(
              "invalid address '{}' in the MetaRoCE-over-ESUN GID list",
              trimmed));
    }
    if (address->isV6() && address->asV6().getScopeId() != 0) {
      throw std::runtime_error(
          fmt::format(
              "address '{}' in the MetaRoCE-over-ESUN GID list must not carry a scope",
              trimmed));
    }
    if (!address->isV6() || !address->asV6().isLinkLocal()) {
      throw std::runtime_error(
          fmt::format(
              "address '{}' in the MetaRoCE-over-ESUN GID list must be an IPv6 "
              "link-local address with an embedded MAC",
              address->str()));
    }
    RawEncapsulation::macFromLinkLocal(address->asV6());
    addresses.emplace(std::move(address).value());
  }
  return addresses;
}

void registerEsunGidRule(
    GidRuleRegistry& rules,
    std::set<folly::IPAddress> addresses) {
  if (addresses.empty()) {
    return;
  }
  rules.registerRule(
      [addresses = std::move(addresses)](
          const DiscoveredAddress& candidate) -> std::optional<uint32_t> {
        return addresses.contains(candidate.scopelessAddress)
            ? std::optional<uint32_t>{kGidTypeEsun}
            : std::nullopt;
      });
}

} // namespace libsoftmetaroce
