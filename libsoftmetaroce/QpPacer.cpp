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

#include <libsoftmetaroce/QpPacer.h>

#include <libsoftmetaroce/Constants.h>

#include <glog/logging.h>

#include <algorithm>
#include <limits>

namespace libsoftmetaroce {
namespace {

// Rounds up, so every conversion to a send time runs the pacer slow rather
// than admitting past its ceiling.
uint64_t ceilDiv(uint64_t numerator, uint64_t denominator) {
  return numerator / denominator + (numerator % denominator != 0);
}

static_assert(
    kNsPerSecond <= std::numeric_limits<uint32_t>::max(),
    "a uint32_t count times kNsPerSecond then fits in uint64_t");

// Bytes at a rate, in nanoseconds.
uint64_t bytesToNs(uint32_t bytes, uint64_t rateBytesPerSec) {
  return ceilDiv(uint64_t{bytes} * kNsPerSecond, rateBytesPerSec);
}

QpPacer::RateLimit clampRateLimit(QpPacer::RateLimit rateLimit) {
  rateLimit.burstBytes =
      std::min(rateLimit.burstBytes, QpPacer::kMaxBurstBytes);
  return rateLimit;
}

// The oldest nextSendTimeNs_ the burst allowance permits at nowNs. Saturates
// at zero: an injected clock may start inside the allowance.
uint64_t earliestSendTimeNs(uint64_t nowNs, uint64_t allowanceNs) {
  return nowNs >= allowanceNs ? nowNs - allowanceNs : 0;
}

} // namespace

QpPacer::QpPacer(RateLimit rateLimit) : rateLimit_(clampRateLimit(rateLimit)) {}

void QpPacer::setRateLimit(RateLimit rateLimit, uint64_t nowNs) {
  const uint64_t oldRateBytesPerSec = rateLimit_.rateBytesPerSec;
  rateLimit_ = clampRateLimit(rateLimit);
  if (isUnlimited()) {
    // Unlimited, the next send is due now.
    nextSendTimeNs_ = nowNs;
    return;
  }
  if (nextSendTimeNs_ > nowNs) {
    // Rescale the remaining wait to cover the same bytes at the new rate.
    nextSendTimeNs_ = nowNs +
        ceilDiv((nextSendTimeNs_ - nowNs) * oldRateBytesPerSec,
                rateLimit_.rateBytesPerSec);
    return;
  }
  nextSendTimeNs_ =
      std::max(nextSendTimeNs_, earliestSendTimeNs(nowNs, allowanceNs()));
}

QpPacer::RateLimit QpPacer::getRateLimit() const {
  return rateLimit_;
}

bool QpPacer::isUnlimited() const {
  return rateLimit_.rateBytesPerSec == 0;
}

bool QpPacer::isSendDue(uint64_t nowNs) const {
  return !nsUntilSendDue(nowNs).has_value();
}

std::optional<uint64_t> QpPacer::nsUntilSendDue(uint64_t nowNs) const {
  if (isUnlimited() || nextSendTimeNs_ <= nowNs) {
    return std::nullopt;
  }
  return nextSendTimeNs_ - nowNs;
}

uint64_t QpPacer::getNextSendTimeNs() const {
  return nextSendTimeNs_;
}

void QpPacer::onPacketSent(uint64_t nowNs, uint32_t wireBytes) {
  DCHECK(isSendDue(nowNs)) << "callers charge only due packets";
  if (isUnlimited()) {
    return;
  }
  const uint64_t baseNs =
      std::max(nextSendTimeNs_, earliestSendTimeNs(nowNs, allowanceNs()));
  nextSendTimeNs_ = baseNs + bytesToNs(wireBytes, rateLimit_.rateBytesPerSec);
}

uint64_t QpPacer::allowanceNs() const {
  return uint64_t{rateLimit_.burstBytes} * kNsPerSecond /
      rateLimit_.rateBytesPerSec;
}

} // namespace libsoftmetaroce
