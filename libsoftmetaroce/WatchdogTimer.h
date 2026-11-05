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
#include <chrono>
#include <cstdint>

DECLARE_uint32(libsoftmetaroce_qp_watchdog_interval_ms);

namespace libsoftmetaroce {

// Below this a watchdog fires on ordinary scheduling delay.
inline constexpr std::chrono::milliseconds kMinWatchdogIntervalMs{20};

struct QpAttributeDeadlines {
  // How long the QP tolerates no progress.
  std::chrono::milliseconds watchdogIntervalMs;
};

struct QpAttributesForInterval {
  uint8_t localAckTimeoutExponent;
  uint8_t retryCount;
};

// Returns a pair that deadlinesFromQpAttributes() maps to at least the
// interval. Saturates where no legal pair reaches the interval.
[[nodiscard]] QpAttributesForInterval qpAttributesForInterval(
    std::chrono::milliseconds intervalMs);

// Returns kDisabledTimeoutWatchdogMs for a zero exponent. Cuts an exponent
// above kLocalAckTimeoutExponentCeiling to that ceiling.
[[nodiscard]] QpAttributeDeadlines deadlinesFromQpAttributes(
    uint8_t localAckTimeoutExponent,
    uint8_t retryCount);

// Thread-safe watchdog timer that tracks progress and detects stalls.
// Uses atomic operations to safely handle concurrent reads and writes
// from different threads (e.g., ACK processing thread and timer callbacks).
class WatchdogTimer {
 public:
  WatchdogTimer();

  void recordProgress();

  // Replaces the last progress time even with an earlier moment.
  void recordProgressAt(std::chrono::steady_clock::time_point at);

  [[nodiscard]] bool hasExpired() const;

  // Overrides libsoftmetaroce_qp_watchdog_interval_ms for this watchdog.
  void configureInterval(std::chrono::milliseconds intervalMs);

  // Returns at least kMinWatchdogIntervalMs.
  [[nodiscard]] std::chrono::milliseconds getIntervalMs() const;

  // Returns true only to the first caller, which handles the expiry. Handling
  // can take long enough for several timer callbacks to see the expiry.
  [[nodiscard]] bool tryClaimExpiry();

  [[nodiscard]] std::chrono::steady_clock::time_point getLastProgressTime()
      const;

 private:
  static constexpr int64_t toNanos(std::chrono::steady_clock::time_point tp) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               tp.time_since_epoch())
        .count();
  }

  static constexpr std::chrono::steady_clock::time_point fromNanos(
      int64_t nanos) {
    return std::chrono::steady_clock::time_point(
        std::chrono::nanoseconds(nanos));
  }

  // Configuration.
  std::atomic<uint64_t> intervalMs_;

  // Runtime state.
  // Last time progress was made, stored as nanoseconds since epoch.
  // Using int64_t for guaranteed lock-free atomic operations.
  std::atomic<int64_t> lastProgressTimeNs_;

  // Guards against redundant expiry handling. Since the watchdog check runs
  // in multiple timer callbacks (retx, HRNR, BRNR), we use this to ensure
  // only one expiry is processed.
  std::atomic<bool> expiryScheduled_{false};
};

} // namespace libsoftmetaroce
