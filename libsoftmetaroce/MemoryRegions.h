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

#pragma once

#include <folly/CppAttributes.h>
#include <folly/Synchronized.h>
#include <infiniband/verbs.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/SgeList.h>
#include <libsoftmetaroce/codec/ReadWriteHeader.h>
#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <unordered_map>

namespace libsoftmetaroce {

// A registered region and the permissions it grants. ibv_mr has no access
// field, so the flags are held alongside it. Registration hands the caller the
// ibv_mr, and its key is what finds this again.
struct RegisteredMemoryRegion {
  struct ibv_mr mr;
  int access;

  [[nodiscard]] bool isZeroBased() const {
    return (access & IBV_ACCESS_ZERO_BASED) != 0;
  }
  [[nodiscard]] uint64_t baseAddress() const {
    return reinterpret_cast<uint64_t>(mr.addr);
  }
  // Registration gives a region one key, as both its lkey and its rkey.
  [[nodiscard]] uint32_t key() const {
    return mr.rkey;
  }
  // Converts an address or zero-based offset to a virtual address.
  // Returns empty on overflow; range and permission checks are separate.
  [[nodiscard]] std::optional<uint64_t> resolvedAddress(
      uint64_t addressOrOffset) const;
  // Whether the region covers length bytes at address and grants
  // requiredAccess.
  [[nodiscard]] bool
  permits(uint64_t address, uint32_t length, int requiredAccess) const;
};

// Node-based, because reg_mr returns the address of an entry.
using MemoryRegionsMap = std::unordered_map<uint32_t, RegisteredMemoryRegion>;

class MemoryRegions {
 public:
  [[nodiscard]] std::optional<uint64_t> addressForRemoteWrite(
      const struct ibv_pd* pd,
      const ReadWriteHeader& rwData) const {
    return addressForRemote(pd, rwData, IBV_ACCESS_REMOTE_WRITE);
  }
  [[nodiscard]] std::optional<uint64_t> addressForRemoteRead(
      const struct ibv_pd* pd,
      const ReadWriteHeader& rwData) const {
    return addressForRemote(pd, rwData, IBV_ACCESS_REMOTE_READ);
  }
  // address is the resolved address, or the wire value where no region of pd
  // resolved it.
  struct RemoteAtomicTarget {
    uint64_t address;
    bool permitted;
  };
  [[nodiscard]] RemoteAtomicTarget targetForRemoteAtomic(
      const struct ibv_pd* pd,
      uint64_t addr,
      uint32_t rkey) const;
  // Returns nullptr with errno set to the code ibv_reg_mr reports.
  struct ibv_mr* FOLLY_NULLABLE
  reg_mr(struct ibv_pd* pd, void* addr, size_t length, int access);
  // True while any region is registered under `pd`, which is what
  // `ibv_dealloc_pd` returns `EBUSY` for.
  [[nodiscard]] bool holdsRegionOf(const struct ibv_pd* pd) const {
    const auto locked = memoryRegions_.rlock();
    return std::any_of(locked->begin(), locked->end(), [pd](const auto& entry) {
      return entry.second.mr.pd == pd;
    });
  }

  bool dereg_mr(const struct ibv_mr& mr) {
    if (!keyFieldsAgree(mr)) {
      return false;
    }
    return memoryRegions_.wlock()->erase(mr.rkey) != 0;
  }

  // pd is the protection domain of the queue pair the operation is for: a key
  // identifies a region only within the domain it was registered in.
  // requiredAccess is the ibv_access_flags mask the operation needs the region
  // to grant; 0 for a local read, which needs no permission.
  bool isValidLocal(
      const struct ibv_pd* pd,
      char* FOLLY_NULLABLE address,
      uint32_t length,
      uint32_t key,
      int requiredAccess) const;

  // Validates the first length bytes across sges under their lkeys.
  // Returns false if the sges are too short.
  bool isValidLocal(
      const struct ibv_pd* pd,
      std::span<const LocalSge> sges,
      uint32_t length,
      int requiredAccess) const;

  // The virtual address a local SGE identifies: for a zero-based region of pd,
  // addr added to the region's base; otherwise addr itself.
  [[nodiscard]] void* FOLLY_NULLABLE
  localAddressOf(const struct ibv_pd* pd, uint64_t addr, uint32_t key) const;

 protected:
  [[nodiscard]] std::optional<uint64_t> addressForRemote(
      const struct ibv_pd* pd,
      const ReadWriteHeader& rwData,
      int requiredAccess) const {
    return checkedAddress(
        pd,
        rwData.getMessageAddr(),
        rwData.getMessageLength(),
        rwData.getKey(),
        requiredAccess,
        AddressKind::FromTheWire);
  }

 private:
  // The local paths read an address as absolute; a wire address is an offset
  // into a zero-based region.
  enum class AddressKind { Virtual, FromTheWire };
  [[nodiscard]] std::optional<uint64_t> checkedAddress(
      const struct ibv_pd* pd,
      uint64_t addr,
      uint32_t length,
      uint32_t key,
      int requiredAccess,
      AddressKind kind) const;

  // reg_mr writes one value into handle, lkey and rkey.
  static bool keyFieldsAgree(const struct ibv_mr& mr) noexcept {
    return mr.handle == mr.rkey && mr.lkey == mr.rkey;
  }

  folly::Synchronized<MemoryRegionsMap> memoryRegions_;
  // first key will be 1. Read and written only under memoryRegions_'s write
  // lock, which is what keeps two registrations off the same key.
  uint32_t last_key_{0};
};

} // namespace libsoftmetaroce
