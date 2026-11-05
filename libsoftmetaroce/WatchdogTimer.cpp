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

#include <libsoftmetaroce/WatchdogTimer.h>

#include <glog/logging.h>
#include <libsoftmetaroce/Constants.h>
#include <algorithm>

DEFINE_uint32(
    libsoftmetaroce_qp_watchdog_interval_ms,
    5000,
    "Watchdog interval in milliseconds. If no progress (ACKs received) is made "
    "within this interval, the watchdog is considered expired. Used by a queue "
    "pair that has not configured an interval of its own.");

namespace libsoftmetaroce {

namespace {

constexpr std::chrono::milliseconds flooredInterval(
    std::chrono::milliseconds intervalMs) {
  return std::max(intervalMs, kMinWatchdogIntervalMs);
}

} // namespace

QpAttributesForInterval qpAttributesForInterval(
    std::chrono::milliseconds intervalMs) {
  // The narrowest exponent that reaches the interval within the retries IBTA
  // allows, since a wider one reports in coarser steps: at the ceiling, every
  // deadline under 2148ms rounds up to 2148ms.
  const int64_t attemptsAllowed = kMaxTransportRetryCount + 1;
  const auto intervalNs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(intervalMs).count();
  for (uint8_t exponent = 1; exponent <= kLocalAckTimeoutExponentCeiling;
       ++exponent) {
    // In nanoseconds, as deadlinesFromQpAttributes rounds the product rather
    // than each attempt; rounding here first would report too few attempts.
    const int64_t perAttemptNs = kLocalAckTimeoutBaseNs << exponent;
    if (perAttemptNs * attemptsAllowed < intervalNs) {
      continue;
    }
    const auto attempts =
        std::max<int64_t>(1, (intervalNs + perAttemptNs - 1) / perAttemptNs);
    return QpAttributesForInterval{
        exponent, static_cast<uint8_t>(attempts - 1)};
  }
  // Past what any legal pair reaches, so the widest one is the closest answer.
  return QpAttributesForInterval{
      kLocalAckTimeoutExponentCeiling, kMaxTransportRetryCount};
}

QpAttributeDeadlines deadlinesFromQpAttributes(
    uint8_t localAckTimeoutExponent,
    uint8_t retryCount) {
  if (localAckTimeoutExponent == kDisabledLocalAckTimeoutExponent) {
    return QpAttributeDeadlines{
        .watchdogIntervalMs = kDisabledTimeoutWatchdogMs};
  }

  const auto cappedExponent =
      std::min(localAckTimeoutExponent, kLocalAckTimeoutExponentCeiling);
  if (cappedExponent != localAckTimeoutExponent) {
    LOG_FIRST_N(WARNING, 10)
        << "cutting a local ack timeout exponent of "
        << static_cast<int>(localAckTimeoutExponent) << " to "
        << static_cast<int>(cappedExponent)
        << ", so the deadline is shorter than the client asked for";
  }
  const auto ackWaitNs =
      std::chrono::nanoseconds(kLocalAckTimeoutBaseNs << cappedExponent);
  const auto attempts = static_cast<uint64_t>(retryCount) + 1;
  // Rounded up so the deadline is never shorter than the waits it covers.
  const auto deadlineMs =
      std::chrono::ceil<std::chrono::milliseconds>(ackWaitNs * attempts);
  return QpAttributeDeadlines{.watchdogIntervalMs = deadlineMs};
}

WatchdogTimer::WatchdogTimer()
    : lastProgressTimeNs_(toNanos(std::chrono::steady_clock::now())) {
  const std::chrono::milliseconds flagMs(
      FLAGS_libsoftmetaroce_qp_watchdog_interval_ms);
  const auto flooredMs = flooredInterval(flagMs);
  if (flooredMs != flagMs) {
    // One report serves: the flag is one setting however many QPs read it.
    LOG_FIRST_N(WARNING, 1)
        << "the watchdog flag of " << flagMs.count()
        << "ms fires on scheduling delay rather than on a stall, so raising it to "
        << flooredMs.count() << "ms";
  }
  configureInterval(flooredMs);
}

void WatchdogTimer::recordProgress() {
  recordProgressAt(std::chrono::steady_clock::now());
}

void WatchdogTimer::recordProgressAt(std::chrono::steady_clock::time_point at) {
  lastProgressTimeNs_.store(toNanos(at), std::memory_order_relaxed);
}

bool WatchdogTimer::hasExpired() const {
  auto lastProgress =
      fromNanos(lastProgressTimeNs_.load(std::memory_order_relaxed));
  auto elapsed = std::chrono::steady_clock::now() - lastProgress;
  return elapsed > getIntervalMs();
}

void WatchdogTimer::configureInterval(std::chrono::milliseconds intervalMs) {
  const auto flooredMs = flooredInterval(intervalMs);
  if (flooredMs != intervalMs) {
    LOG_FIRST_N(WARNING, 10)
        << "a watchdog interval of " << intervalMs.count()
        << "ms fires on scheduling delay rather than on a stall, so raising it to "
        << flooredMs.count() << "ms";
  }
  intervalMs_.store(
      static_cast<uint64_t>(flooredMs.count()), std::memory_order_relaxed);
}

std::chrono::milliseconds WatchdogTimer::getIntervalMs() const {
  return std::chrono::milliseconds(intervalMs_.load(std::memory_order_relaxed));
}

bool WatchdogTimer::tryClaimExpiry() {
  bool expected = false;
  return expiryScheduled_.compare_exchange_strong(expected, true);
}

std::chrono::steady_clock::time_point WatchdogTimer::getLastProgressTime()
    const {
  return fromNanos(lastProgressTimeNs_.load(std::memory_order_relaxed));
}

} // namespace libsoftmetaroce
