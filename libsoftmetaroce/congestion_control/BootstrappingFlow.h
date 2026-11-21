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

#include <libsoftmetaroce/congestion_control/FastStart.h>
#include <libsoftmetaroce/congestion_control/Flags.h>
#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>

namespace libsoftmetaroce {

class BootstrappingFlow {
 public:
  struct AckOutcome {
    bool congested{false};
    bool reduced{false};
  };

  // Handles the ACK's congestion/reduction outcome and selects an eligible
  // bootstrap using the windows after reduction, before additive growth.
  void onAck(
      const FastStart& windows,
      uint16_t ackedFlowId,
      const AckOutcome& outcome) {
    if (outcome.congested ||
        (outcome.reduced && windows.getFwnd(ackedFlowId) == 0)) {
      abandonBootstrap(ackedFlowId);
    }
    selectFlow(windows);
  }

  void abandonBootstrap(uint16_t flowId) {
    if (bootstrappingFlowId_ == flowId) {
      bootstrappingFlowId_.reset();
    }
  }

  std::optional<uint16_t> flowForWindowIncrease(
      const FastStart& windows,
      uint16_t ackedFlowId,
      bool eligibleByLowDelay) const {
    const auto& acked = windows.flowStateForId(ackedFlowId);
    if (acked.mayReceiveWindowIncrease() && eligibleByLowDelay &&
        acked.fwnd < windows.kFastStartBurst && acked.fwnd < windows.maxFwnd_) {
      return ackedFlowId;
    }
    if (bootstrappingFlowId_.has_value() &&
        windows.flowStateForId(*bootstrappingFlowId_)
            .mayReceiveWindowIncrease() &&
        (eligibleByLowDelay || bootstrappingFlowId_ != ackedFlowId) &&
        windows.getFwnd(*bootstrappingFlowId_) < windows.maxFwnd_) {
      return bootstrappingFlowId_;
    }
    return acked.mayReceiveWindowIncrease() && eligibleByLowDelay &&
            acked.fwnd < windows.maxFwnd_
        ? std::optional<uint16_t>{ackedFlowId}
        : std::nullopt;
  }

  void onWindowIncreased(const FastStart& windows, uint16_t flowId) {
    const uint16_t target = std::min(windows.kFastStartBurst, windows.maxFwnd_);
    if (bootstrappingFlowId_ == flowId && windows.getFwnd(flowId) >= target) {
      bootstrappingFlowId_.reset();
    }
  }

 private:
  static bool activationAllowed(const FastStart& windows) {
    if (!FLAGS_libsoftmetaroce_activate_tx_flows || windows.inFastStart_ ||
        windows.qwnd_ >= windows.qwnd_max_) {
      return false;
    }
    int activeFlows = 0;
    for (int id = 0; id < windows.numFlows(); ++id) {
      activeFlows += windows.getFwnd(id) > 0;
    }
    if (activeFlows == 0 || activeFlows == windows.numFlows()) {
      return false;
    }
    uint64_t threshold;
    if (FLAGS_libsoftmetaroce_exponential_flow_activation_threshold) {
      if (activeFlows >= std::numeric_limits<uint64_t>::digits) {
        return false;
      }
      threshold = uint64_t{3} << (activeFlows - 1);
    } else {
      threshold = static_cast<uint64_t>(activeFlows + 1)
          << FLAGS_libsoftmetaroce_average_window_log2;
    }
    return threshold < windows.getQwnd();
  }

  void selectFlow(const FastStart& windows) {
    if (bootstrappingFlowId_.has_value() || !activationAllowed(windows)) {
      return;
    }
    for (int id = 0; id < windows.numFlows(); ++id) {
      const auto& flow = windows.flowStateForId(id);
      if (flow.fwnd == 0 && flow.mayReceiveWindowIncrease()) {
        bootstrappingFlowId_ = id;
        return;
      }
    }
  }

  std::optional<uint16_t> bootstrappingFlowId_;
};

} // namespace libsoftmetaroce
