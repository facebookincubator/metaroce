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

#include <cstdint>
#include <optional>

namespace libsoftmetaroce {

// Caps a queue pair's send rate below what its receiver can absorb.
//
// The pacer is a virtual send clock. The gate asks whether a packet may leave
// (isSendDue) and records each packet it builds (onPacketSent), charging its
// wire bytes. Admitted bytes over any window never exceed rate * window
// length + the burst allowance + one packet: the check runs before the
// packet's size is known, so one packet can overdraw.
// After an idle stretch the gate admits back to back only up to the
// allowance; older lateness is lost.
//
// A zero rate means unlimited, matching ibv_qp_rate_limit_attr. Time arrives
// as a parameter in the protocol clock's nanoseconds, so tests pass an
// injected clock. The pacer keeps no lock; its owner serializes access.
class QpPacer {
 public:
  // Bursts cover scheduling delay; larger ones are clamped.
  static constexpr uint32_t kMaxBurstBytes = 100 * 1024 * 1024;

  struct RateLimit {
    uint64_t rateBytesPerSec = 0;
    // Allowance in bytes, clamped at kMaxBurstBytes. uint32_t keeps the
    // nanosecond product below 2^64.
    uint32_t burstBytes = 0;
  };

  explicit QpPacer(RateLimit rateLimit);

  void setRateLimit(RateLimit rateLimit, uint64_t nowNs);
  RateLimit getRateLimit() const;

  bool isUnlimited() const;
  bool isSendDue(uint64_t nowNs) const;
  // Nanoseconds until the next send is due: positive while the pacer holds,
  // empty once a send is due.
  std::optional<uint64_t> nsUntilSendDue(uint64_t nowNs) const;
  uint64_t getNextSendTimeNs() const;
  void onPacketSent(uint64_t nowNs, uint32_t wireBytes);

 private:
  uint64_t allowanceNs() const;

  RateLimit rateLimit_;
  uint64_t nextSendTimeNs_{0};
};

} // namespace libsoftmetaroce
