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

#include <gflags/gflags.h>
#include <atomic>
#include <cstdint>

DECLARE_uint32(libsoftmetaroce_nic_rate_gbps);
DECLARE_uint32(libsoftmetaroce_rcn_pacing_multiplier);

namespace libsoftmetaroce {

// Computes a per-QP RCN rate hint by dividing the NIC rate among active
// receiving QPs, mirroring the packsim reference (MetaRoceTransport::
// setRateHint). Activation/deactivation policy is owned by Qp.
class RcnRateHint {
 public:
  explicit RcnRateHint(uint32_t nicRateGbps) : nicRateGbps_(nicRateGbps) {}

  uint32_t getPerQpRateGbps() const {
    uint32_t count = activeRxQpCount_.load(std::memory_order_relaxed);
    if (count <= 1) {
      return nicRateGbps_;
    }
    // Round up: ceil(nicRate / count)
    return (nicRateGbps_ + count - 1) / count;
  }

  void onQpBecameActive() {
    activeRxQpCount_.fetch_add(1, std::memory_order_relaxed);
  }

  void onQpBecameInactive() {
    activeRxQpCount_.fetch_sub(1, std::memory_order_relaxed);
  }

 private:
  uint32_t nicRateGbps_;
  std::atomic<uint32_t> activeRxQpCount_{0};
};

} // namespace libsoftmetaroce
