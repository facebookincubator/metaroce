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

#include <folly/container/F14Map.h>
#include <libsoftmetaroce/congestion_control/CongestionController.h>
#include <cstdint>
#include <stdexcept>

namespace libsoftmetaroce {

class StaticQwnd : public CongestionController {
 public:
  explicit StaticQwnd(uint16_t fwnd) : fwnd_(fwnd) {}
  ~StaticQwnd() override = default;

  void onPacketSent(uint16_t flowId) override {
    flowStateForId(flowId).inflightPackets += 1;
  }

  AckResult onPacketAck(uint16_t flowId, const AckEvent& event) override {
    FlowState& flow = flowStateForId(flowId);
    // it should not be possible to make inflightPackets negative, but
    // guard against it anyway.
    flow.inflightPackets -=
        std::min(event.numAckedPackets, flow.inflightPackets);
    return {};
  }

  void onPacketLoss(uint16_t /* flowId */, const LossEvent&) override {}

  void onFlowDisabled(uint16_t /* flowId */) override {}

  void onFlowAdded(uint16_t flowId) override {
    flowIdToFlowState_.emplace(flowId, FlowState{});
  }

  uint16_t getAvailableWindowSize(uint16_t flowId) const override {
    auto inflights = flowStateForId(flowId).inflightPackets;
    return fwnd_ > inflights ? fwnd_ - inflights : 0;
  }

  virtual uint16_t getFwnd(uint16_t /* flowId */) const override {
    return fwnd_;
  }

  uint32_t getQwnd() const override {
    return static_cast<uint32_t>(flowIdToFlowState_.size()) * fwnd_;
  }

 private:
  struct FlowState {
    uint16_t inflightPackets{0};
  };

  // Configuration.
  const uint16_t fwnd_;

  // Runtime state.
  folly::F14FastMap<uint16_t, FlowState> flowIdToFlowState_;
  FlowState& flowStateForId(uint16_t flowId) {
    try {
      return flowIdToFlowState_.at(flowId);
    } catch (const std::out_of_range&) {
      throw std::out_of_range(
          fmt::format("flow state not found for flow id {}", flowId));
    }
  }

  const FlowState& flowStateForId(uint16_t flowId) const {
    try {
      return flowIdToFlowState_.at(flowId);
    } catch (const std::out_of_range&) {
      throw std::out_of_range(
          fmt::format("flow state not found for flow id {}", flowId));
    }
  }
};

} // namespace libsoftmetaroce
