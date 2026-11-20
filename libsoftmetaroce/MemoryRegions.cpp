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

#include <glog/logging.h>

#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/NextFreeId.h>
#include <cerrno>
#include <limits>

namespace libsoftmetaroce {
namespace {

// Called with the registry read-locked. Null where the key matches no region
// of pd, which an unknown key and another domain's key both come to.
const RegisteredMemoryRegion* FOLLY_NULLABLE regionUnder(
    const MemoryRegionsMap& regions,
    const struct ibv_pd* pd,
    uint32_t key) {
  const auto it = regions.find(key);
  if (it == regions.end()) {
    LOG(ERROR) << "Memory region validation failed key check; key:" << key
               << " is not among our registered memory regions";
    return nullptr;
  }
  if (it->second.mr.pd != pd) {
    LOG(ERROR) << "Memory region validation failed protection domain check. "
               << "key:" << key << " matches a region of another domain";
    return nullptr;
  }
  return &it->second;
}

} // namespace

std::optional<uint64_t> RegisteredMemoryRegion::resolvedAddress(
    uint64_t addressOrOffset) const {
  if (!isZeroBased()) {
    return addressOrOffset;
  }
  const uint64_t offset = addressOrOffset;
  const uint64_t base = baseAddress();
  const bool offsetWrapsPastTheTop =
      offset > std::numeric_limits<uint64_t>::max() - base;
  if (offsetWrapsPastTheTop) {
    LOG(ERROR) << "Memory region resolution failed zero-based check. key:"
               << key() << " offset:" << std::hex << offset
               << " cannot be added to base " << mr.addr << std::dec;
    return std::nullopt;
  }
  return base + offset;
}

bool RegisteredMemoryRegion::permits(
    uint64_t address,
    uint32_t length,
    int requiredAccess) const {
  const uint64_t base = baseAddress();
  // The length comes off the region end, so an address near the top of the
  // space cannot wrap past it into the region.
  if (address < base || length > mr.length ||
      address > base + (mr.length - length)) {
    LOG(ERROR) << "Memory region validation failed range check. key:" << key()
               << " destination addr:" << std::hex << address
               << " length:" << std::dec << length << " while range is "
               << std::hex << mr.addr << " len:" << std::dec << mr.length;
    return false;
  }
  if ((access & requiredAccess) != requiredAccess) {
    LOG(ERROR) << "Memory region validation failed access check. key:" << key()
               << " needs access 0x" << std::hex << requiredAccess
               << " but is registered with 0x" << access << std::dec;
    return false;
  }
  return true;
}

MemoryRegions::RemoteAtomicTarget MemoryRegions::targetForRemoteAtomic(
    const struct ibv_pd* pd,
    uint64_t addr,
    uint32_t rkey) const {
  const auto lockedRegions = memoryRegions_.rlock();
  const auto* const region = regionUnder(*lockedRegions, pd, rkey);
  if (region == nullptr) {
    return {.address = addr, .permitted = false};
  }
  const auto resolved = region->resolvedAddress(addr);
  if (!resolved.has_value()) {
    return {.address = addr, .permitted = false};
  }
  return {
      .address = *resolved,
      .permitted = region->permits(
          *resolved, kAtomicOperationSizeBytes, IBV_ACCESS_REMOTE_ATOMIC)};
}

bool MemoryRegions::isValidLocal(
    const struct ibv_pd* pd,
    char* FOLLY_NULLABLE address,
    uint32_t length,
    uint32_t key,
    int requiredAccess) const {
  // The address is compared with region bounds, never read, so null is safe.
  // NOLINTNEXTLINE(facebook-hte-NullableDereference)
  return checkedAddress(
             pd,
             reinterpret_cast<uint64_t>(address),
             length,
             key,
             requiredAccess,
             AddressKind::Virtual)
      .has_value();
}

bool MemoryRegions::isValidLocal(
    const struct ibv_pd* pd,
    std::span<const LocalSge> sges,
    uint32_t length,
    int requiredAccess) const {
  uint32_t remaining = length;
  for (const LocalSge& sge : sges) {
    const uint32_t covered = std::min(sge.length, remaining);
    if (!isValidLocal(
            pd,
            static_cast<char*>(sge.address),
            covered,
            sge.lkey,
            requiredAccess)) {
      return false;
    }
    remaining -= covered;
  }
  return remaining == 0;
}

void* FOLLY_NULLABLE MemoryRegions::localAddressOf(
    const struct ibv_pd* pd,
    uint64_t addr,
    uint32_t key) const {
  const auto lockedRegions = memoryRegions_.rlock();
  const auto it = lockedRegions->find(key);
  // A zero-length SGE may hold a key that identifies no region.
  const bool keyIdentifiesARegionOfPd =
      it != lockedRegions->end() && it->second.mr.pd == pd;
  // Only a nonzero base can overflow, and no such region contains address 0.
  const uint64_t resolved = keyIdentifiesARegionOfPd
      ? it->second.resolvedAddress(addr).value_or(0)
      : addr;
  // An SGE holds its address as a uint64_t; callers store it as a pointer.
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  return reinterpret_cast<void*>(resolved);
}

std::optional<uint64_t> MemoryRegions::checkedAddress(
    const struct ibv_pd* pd,
    uint64_t addr,
    uint32_t length,
    uint32_t key,
    int requiredAccess,
    AddressKind kind) const {
  // A zero-length access touches no memory, so no region or key governs it.
  if (length == 0) {
    return addr;
  }
  const auto lockedRegions = memoryRegions_.rlock();
  const auto* const region = regionUnder(*lockedRegions, pd, key);
  if (region == nullptr) {
    return std::nullopt;
  }
  const std::optional<uint64_t> resolved = kind == AddressKind::FromTheWire
      ? region->resolvedAddress(addr)
      : std::optional<uint64_t>{addr};
  if (!resolved.has_value() ||
      !region->permits(*resolved, length, requiredAccess)) {
    return std::nullopt;
  }
  return resolved;
}

struct ibv_mr* FOLLY_NULLABLE MemoryRegions::reg_mr(
    struct ibv_pd* pd,
    void* addr,
    size_t length,
    int access) {
  // A region ending past the top of the space gives every range check below
  // no end to measure an address against.
  if (length >
      std::numeric_limits<uint64_t>::max() - reinterpret_cast<uint64_t>(addr)) {
    errno = EINVAL;
    return nullptr;
  }
  // Remote write and remote atomic both write into the region, so either
  // requires local write beside it. A region registered without it could not
  // serve the access its own flags grant.
  const bool writesTheRegionRemotely =
      (access & (IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC)) != 0;
  if (writesTheRegionRemotely && (access & IBV_ACCESS_LOCAL_WRITE) == 0) {
    LOG(ERROR) << "reg_mr asks for remote write or remote atomic without local "
               << "write; access 0x" << std::hex << access << std::dec;
    errno = EINVAL;
    return nullptr;
  }
  // One hold for the count, the key and the insert that uses it, so two
  // registrations can neither pass the limit together nor share a key.
  auto lockedRegions = memoryRegions_.wlock();
  if (lockedRegions->size() >= kMaxMemoryRegions) {
    errno = ENOSPC;
    return nullptr;
  }
  // start with key == 1 to separate from uninitialized 0.
  const std::optional<uint32_t> maybeKey = nextFreeId(
      nextKeyToAssign_,
      1,
      std::numeric_limits<uint32_t>::max(),
      [&lockedRegions](uint32_t key) { return lockedRegions->contains(key); });
  CHECK(maybeKey.has_value())
      << "the count checked above leaves keys free, so nextFreeId finds one";
  const uint32_t key = *maybeKey;

  auto placed = lockedRegions->emplace(
      key,
      RegisteredMemoryRegion{
          .mr =
              {.context = nullptr,
               .pd = pd,
               .addr = addr,
               .length = length,
               .handle = key,
               .lkey = key,
               .rkey = key},
          .access = access});
  return &placed.first->second.mr;
}
} // namespace libsoftmetaroce
