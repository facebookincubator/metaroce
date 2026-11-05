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

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <libsoftmetaroce/congestion_control/FastStart.h>
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>

DECLARE_int32(libsoftmetaroce_min_qwnd);
DECLARE_int32(libsoftmetaroce_max_qwnd);

namespace libsoftmetaroce {

/* Congestion controller implementation that extends fast start to implement
   baseline AIMD CC, increasing additively by epsilon per rtt and decreasing by
   beta (as inverse log 2, so "1" means by half.) on ecn. . Loss decreases by
   lambda, in the same encoding. */
class Baseline : public FastStart {
 public:
  static constexpr int kDefaultBeta = 1;
  static constexpr int kDefaultLambda = 0;
  Baseline(int epsilon, int beta, int lambda = kDefaultLambda)
      : epsilon_(epsilon), beta_(beta), lambda_(lambda) {}
  ~Baseline() override = default;

  bool fwndMayIncrease(const FlowState& flow) {
    return flow.fwnd < maxFwnd_;
  }

  bool eligibleByLowDelay(const AckEvent& event) {
    if (event.measuredRttNs == 0) {
      // Without a sample, the flow cannot be shown below the average.
      return false;
    }
    return event.measuredRttNs <= event.smoothedQpRttNs;
  }

  bool qwndMayIncrease() {
    return qwnd_ < qwnd_max_;
  }

  void onPacketAck(uint16_t flowId, const AckEvent& event) override {
    const uint16_t inflightBeforeAck = flowStateForId(flowId).inflightPackets;
    // exits fast start if needed, decrements inflight.
    FastStart::onPacketAck(flowId, event);
    if (inFastStart_) {
      // Fast start grows the window as packets are sent, not on acks.
      return;
    }

    auto& flow = flowStateForId(flowId);
    if (event.numAckedPacketsWithCE == 0) {
      // More than half the flow window outstanding counts as window limited.
      const bool windowLimited = flow.fwnd < 2 * inflightBeforeAck;
      if (!qwndMayIncrease() || !windowLimited) {
        return;
      }
      // additive increase on no congestion.
      // The fwnd only increases when the measured rtt on this ack is at or
      // below the smoothed rtt for the qp, to achieve flow balance.
      qwnd_fractional_ += std::min(epsilon_, qwnd_) * event.numAckedPackets;
      const bool lowDelay = eligibleByLowDelay(event);
      while (lowDelay && qwnd_fractional_ > qwnd_ && qwndMayIncrease() &&
             fwndMayIncrease(flow)) {
        qwnd_fractional_ -= qwnd_;
        qwnd_ += 1;
        flow.fwnd += 1;
      }
      VLOG(2) << "increased to " << trace(flowId, flow);
      return;
    } else {
      reduceFwndBySignal(flow, event.numAckedPacketsWithCE);
      VLOG(2) << "mark of " << event.numAckedPacketsWithCE << " reduced to "
              << trace(flowId, flow);
    }
  }

  void onPacketLoss(uint16_t flowId, const LossEvent& event) override {
    // base class method is a noop; calling in case that changes
    FastStart::onPacketLoss(flowId, event);
    auto& flow = flowStateForId(flowId);
    reduceFwndBySignal(flow, event.numLostPackets, lambda_);
    VLOG(2) << "loss of " << event.numLostPackets << " reduced to "
            << trace(flowId, flow);
  }

  void onFlowDisabled(uint16_t flowId) override {
    FastStart::onFlowDisabled(flowId);
    if (qwnd_ < kMinQwnd) {
      // Restored on flow 0, the flow that stays in service. qwnd_ counts
      // flow 0's window, so the sum fits a uint16_t.
      const uint16_t increment = kMinQwnd - qwnd_;
      qwnd_ += increment;
      flowStateForId(0).fwnd += increment;
    }
  }

  void onFlowAdded(uint16_t flowId) override {
    // base class creates a flow state (fwnd, inflight) entry
    FastStart::onFlowAdded(flowId);
    // non-RCN method of defining maximum qwnd
    qwnd_max_ =
        std::min(maxFwnd_ * numFlows(), FLAGS_libsoftmetaroce_initial_qwnd_max);
    if (kMaxQwnd > 0) {
      qwnd_max_ = std::min(qwnd_max_, kMaxQwnd);
    }
  }

 protected:
  std::string trace(uint16_t flowId, const FlowState& flow) const {
    return fmt::format(
        "qwnd_ {}.{}, flow {} to fwnd {}",
        qwnd_,
        qwnd_fractional_,
        flowId,
        flow.fwnd);
  }

  // Reduce fwnd and qwnd by signal >> shift. The shift parameter represents
  // the negative log base 2 of the reduction factor (0 = reduce by signal,
  // 1 = reduce by signal/2, etc.)
  void reduceFwndBySignal(FlowState& flow, int signal, int shift) {
    // cannot decrease from minimum.
    if (qwnd_ == kMinQwnd || flow.fwnd == 0) {
      return;
    }
    // compute integer and fractional reduction if multiple ce packets acked.
    int reduction = signal >> shift;
    int fractional_reduction = (qwnd_ >> shift) * (signal & ((1 << shift) - 1));

    // apply at least a small reaction even if shift is very small
    if (reduction == 0 && fractional_reduction == 0) {
      fractional_reduction = 1;
    }

    // reduce the fractional, borrow if necessary
    if (qwnd_fractional_ > fractional_reduction) {
      qwnd_fractional_ -= fractional_reduction;
    } else {
      qwnd_fractional_ += qwnd_ - fractional_reduction - 1;
      reduction++;
    }

    // cannot decrement past what this flow has as a window.
    if (reduction > flow.fwnd) {
      reduction = flow.fwnd;
    }

    const bool wouldFallBelowMin = reduction > (qwnd_ - kMinQwnd);
    if (wouldFallBelowMin) {
      reduction = (qwnd_ - kMinQwnd);
    };

    // decrement both simultaneously
    flow.fwnd -= reduction;
    qwnd_ -= reduction;
  }

  // Two-argument version uses beta_ as the shift (for ECN response)
  void reduceFwndBySignal(FlowState& flow, int signal) {
    reduceFwndBySignal(flow, signal, beta_);
  }
  // Configuration.
  const int kMinQwnd{FLAGS_libsoftmetaroce_min_qwnd};
  const int kMaxQwnd{FLAGS_libsoftmetaroce_max_qwnd};

 private:
  // Configuration.
  const int epsilon_;
  const int beta_;
  const int lambda_;

  // Runtime state.
  int qwnd_fractional_{0};
};
} // namespace libsoftmetaroce
