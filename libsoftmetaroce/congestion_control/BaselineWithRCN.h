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

#include <glog/logging.h>
#include <libsoftmetaroce/congestion_control/Baseline.h>
#include <algorithm>
#include <cstdint>

DECLARE_int32(libsoftmetaroce_fwnd_floor);

namespace libsoftmetaroce {

/* Congestion controller extending Baseline to support RCN (Rate-based
   Congestion Notification). Uses the rcnDesiredRateGbps from ack events
   to compute qwnd_max_ dynamically based on the desired rate and RTT. */
class BaselineWithRCN : public Baseline {
 public:
  // gamma is the BDP multiplier as a fixed-point ratio over kGammaDenominator
  // (16 = 1.0); omega is additive RTT headroom as a delay in nanoseconds.
  static constexpr uint32_t kGammaDenominator = 16;
  static constexpr uint32_t kDefaultGammaNumerator = 16;
  static constexpr uint32_t kDefaultOmegaNs = 1'000;

  BaselineWithRCN(
      int epsilon,
      int beta,
      uint32_t gammaNumerator = kDefaultGammaNumerator,
      uint32_t omegaNs = kDefaultOmegaNs,
      int lambda = kDefaultLambda)
      : Baseline(epsilon, beta, lambda),
        gammaNumerator_(gammaNumerator),
        omegaNs_(omegaNs) {
    // The factory rejects a zero before constructing, so reaching here with one
    // is a programming error rather than operator input.
    DCHECK_GT(gammaNumerator_, 0u)
        << "gammaNumerator divides the qwnd_max computation and must be nonzero";
  }
  ~BaselineWithRCN() override = default;

  uint32_t gammaNumerator() const {
    return gammaNumerator_;
  }

  uint32_t omegaNs() const {
    return omegaNs_;
  }

  void setMTU(uint32_t mtu) override {
    mtu_ = mtu;
  }

  void onPacketAck(uint16_t flowId, const AckEvent& event) override {
    // Spec 6.3.2:
    //   qwnd_max = max(QWND_min, gamma * rate * (rtt + omega) / (MTU * 8))
    //
    // gamma is a fixed-point ratio gammaNumerator_ / kGammaDenominator, so this
    // maps to the integer temporaries below (rounded up, floored at
    // kMinQwndMax):
    //
    //                rateGbps * rttPlusOmegaNs * gammaNumerator_
    //     qwnd_max = -------------------------------------------
    //                    8 * mtu_ * kGammaDenominator  (= denom)
    //
    // rateGbps (Gb/s) * rttPlusOmegaNs (ns) is exactly bits (1e9/1e-9 cancel),
    // so the formula needs no rescaling and stays in integer arithmetic.
    if (event.rcnDesiredRateGbps > 0 && mtu_ > 0) {
      static constexpr int kMinQwndMax = 10;
      const uint64_t rateGbps = event.rcnDesiredRateGbps;
      // smoothedQpRttNs is a QP RTT estimate seeded to a positive value; the
      // floor only guards a malformed zero so the division below stays safe.
      const uint64_t rttPlusOmegaNs =
          std::max<uint64_t>(1, event.smoothedQpRttNs) + omegaNs_;
      const uint64_t denom = uint64_t{kBitsPerByte} * mtu_ * kGammaDenominator;
      // Divide the ceiling down to a rate rather than forming the product,
      // which could overflow uint64 for an untrusted peer rate.
      const uint64_t maxRateGbps =
          uint64_t{kQwndMaxCeiling} * denom / rttPlusOmegaNs / gammaNumerator_;
      if (rateGbps > maxRateGbps) {
        qwnd_max_ = kQwndMaxCeiling;
      } else {
        const uint64_t computedQwndMax =
            (rateGbps * rttPlusOmegaNs * gammaNumerator_ + denom - 1) / denom;
        qwnd_max_ =
            std::max<int>(kMinQwndMax, static_cast<int>(computedQwndMax));
      }
      if (kMaxQwnd > 0) {
        qwnd_max_ = std::min(qwnd_max_, kMaxQwnd);
      }
    }

    // If qwnd exceeds max, RCN reduction takes priority over Baseline's
    // ECN reduction. Call FastStart directly to exit fast start and decrement
    // inflight, skipping Baseline's AIMD logic.
    bool rcnReductionApplies = qwnd_ > qwnd_max_;
    if (rcnReductionApplies) {
      FastStart::onPacketAck(flowId, event);
    } else {
      Baseline::onPacketAck(flowId, event);
    }

    // Apply RCN reduction if qwnd exceeds limits.
    // RCN uses fixed shift values independent of beta_:
    // - shift=0 (reduce by numAcked) when qwnd > 2*qwnd_max
    // - shift=1 (reduce by numAcked/2) when qwnd > qwnd_max
    auto& flow = flowStateForId(flowId);
    // Flow 0 keeps its floor at any max.
    const int64_t qwndThatWouldProtectThisFlow =
        (int64_t{flowId} + 1) * FLAGS_libsoftmetaroce_fwnd_floor;
    if ((flowId == 0 || qwnd_max_ >= qwndThatWouldProtectThisFlow) &&
        flow.fwnd <= FLAGS_libsoftmetaroce_fwnd_floor) {
      return;
    }
    if (qwnd_ > 2 * qwnd_max_) {
      reduceFwndBySignal(flow, event.numAckedPackets, 0);
      VLOG(2) << "RCN: qwnd " << qwnd_ << " > 2*qwnd_max " << (2 * qwnd_max_)
              << ", reduced by " << event.numAckedPackets;
    } else if (qwnd_ > qwnd_max_) {
      reduceFwndBySignal(flow, event.numAckedPackets, 1);
      VLOG(2) << "RCN: qwnd " << qwnd_ << " > qwnd_max " << qwnd_max_
              << ", reduced by " << (event.numAckedPackets >> 1);
    }
  }

 private:
  static constexpr uint32_t kBitsPerByte = 8;
  // Overflow guard: a hard ceiling on the computed qwnd_max so an untrusted
  // peer rate cannot overflow the uint64 numerator. Far above any real window;
  // the user-facing cap is FLAGS_libsoftmetaroce_max_qwnd (kMaxQwnd).
  static constexpr int kQwndMaxCeiling = 65536 * 256;
  // Configuration. mtu_ is set via setMTU(), so unlike gammaNumerator_/omegaNs_
  // it cannot be const.
  const uint32_t gammaNumerator_;
  const uint32_t omegaNs_;
  uint32_t mtu_{4096};
};

} // namespace libsoftmetaroce
