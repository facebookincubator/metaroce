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

#include <folly/container/F14Map.h>
#include <libsoftmetaroce/congestion_control/CongestionController.h>
#include <libsoftmetaroce/congestion_control/Flags.h>
#include <cstdint>
#include <optional>
#include <stdexcept>

DECLARE_uint32(libsoftmetaroce_fast_start_burst);
DECLARE_int32(libsoftmetaroce_initial_qwnd_max);

namespace libsoftmetaroce {

/* Congestion controller implementation that includes only fast
   start. In fast start, increment fwnd by every packet we send until
   we see an ack.  Any flows with zero fwnd will stay that way.  No
   other control (ecn response) is applied here. */
class FastStart : public CongestionController {
 public:
  static constexpr int kDefaultQwndMax = 2 << 16;

  FastStart() {}
  ~FastStart() override = default;

  void onPacketSent(uint16_t flowId) override {
    auto& flow = flowStateForId(flowId);
    if (inFastStart_) {
      flow.fwnd += 1;
      qwnd_++;
      inFastStart_ = qwnd_ < qwnd_max_;
    }
    flow.inflightPackets += 1;
  }

  void onPacketAck(uint16_t flowId, const AckEvent& event) override {
    if (inFastStart_) {
      const bool congested = event.numAckedPacketsWithCE > 0;
      inFastStart_ = !(event.sendPending || congested || qwnd_ >= qwnd_max_);
    }
    auto& flow = flowStateForId(flowId);
    // it should not be possible to make inflightPackets negative, but
    // guard against it anyway.
    flow.inflightPackets -=
        std::min(event.numAckedPackets, flow.inflightPackets);
  }

  void onPacketLoss(uint16_t /* flowId */, const LossEvent&) override {}

  void onFlowAdded(uint16_t flowId) override {
    // Resolution throws on disagreeing flag names, so a flow exists only once
    // its window bound does.
    maxFwnd_ = maxFwndFlag();
    flowIdToFlowState_.emplace(flowId, FlowState{});
  }

  void onFlowDisabled(uint16_t flowId) override {
    // Flow 0 stays in service, so its share is not shed.
    if (flowId == 0) {
      return;
    }
    auto& flow = flowStateForId(flowId);
    // qwnd_ is the sum of the flow windows.
    qwnd_ -= flow.fwnd;
    flow.fwnd = 0;
    flow.inflightPackets = 0;
  }

  uint16_t getAvailableWindowSize(uint16_t flowId) const override {
    auto& flow = flowStateForId(flowId);
    auto fwnd = flow.fwnd;
    if (inFastStart_) {
      // in fast start, all flows are always ready to send, until
      // reaching a maximum fwnd; returning nonzero is enough to
      // continue in fast start.
      return (fwnd < maxFwnd_ && qwnd_ < qwnd_max_) ? 1 : 0;
    }
    auto inflights = flow.inflightPackets;
    return fwnd > inflights ? fwnd - inflights : 0;
  }

  virtual bool shouldAdvanceFlowAfterSend(uint16_t flowId) const override {
    if (!inFastStart_) {
      return true;
    }
    auto fwnd = flowStateForId(flowId).fwnd;
    return (fwnd >= kFastStartBurst);
  }

  void setPacketsPerVisit(uint16_t packetsPerVisit) override {
    packetsPerVisit_ = std::max<uint16_t>(1, packetsPerVisit);
  }

  void setPortPairCount(uint16_t portPairCount) override {
    portPairCount_ = std::max<uint16_t>(1, portPairCount);
  }

  // Fast start walks flows on a fixed burst, indexed by the packets sent so far
  // (qwnd_). Outside fast start the window bounds a run, so flow choice is left
  // to the caller. Identifies a flow by position, which onFlowAdded registers
  // as the id itself; flows are never removed, so the two do not drift.
  std::optional<NextSend> flowForNextSend() const override {
    const auto numFlows = flowIdToFlowState_.size();
    if (!inFastStart_ || numFlows == 0) {
      return std::nullopt;
    }
    const uint32_t burst = std::max<uint16_t>(1, kFastStartBurst);
    const uint32_t sent = static_cast<uint32_t>(qwnd_);
    const uint32_t initialBurstPackets =
        burst * static_cast<uint32_t>(numFlows);
    if (sent < initialBurstPackets) {
      // Opening group: flows taken a set at a time, one per port pair, each set
      // interleaved a packet per flow until every flow in it holds its burst.
      const uint32_t setSize =
          std::min<uint32_t>(portPairCount_, static_cast<uint32_t>(numFlows));
      const uint32_t set = sent / (burst * setSize);
      const uint32_t firstFlow = set * setSize;
      const uint32_t flowsInSet = std::min<uint32_t>(
          setSize, static_cast<uint32_t>(numFlows) - firstFlow);
      const uint32_t withinSet = sent - set * burst * setSize;
      return NextSend{
          static_cast<uint16_t>(firstFlow + withinSet % flowsInSet),
          flowsInSet > 1 || ((withinSet + 1) % burst) == 0};
    }
    // Every flow holds its opening burst, so runs are now as long as the
    // sender asked for; the default of 1 spreads a packet per flow.
    const uint32_t after = sent - initialBurstPackets;
    return NextSend{
        static_cast<uint16_t>((after / packetsPerVisit_) % numFlows),
        ((after + 1) % packetsPerVisit_) == 0};
  }

  int numFlows() const {
    return static_cast<int>(flowIdToFlowState_.size());
  }

  virtual uint16_t getFwnd(uint16_t flowId) const override {
    const auto& flow = flowStateForId(flowId);
    return flow.fwnd;
  }

  uint32_t getQwnd() const override {
    return static_cast<uint32_t>(qwnd_);
  }

 protected:
  struct FlowState {
    uint16_t inflightPackets{0};
    uint16_t fwnd{0};
  };

  // Configuration.
  const uint16_t kFastStartBurst{
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_fast_start_burst)};
  uint16_t maxFwnd_{0};

  // Consecutive packets the sender wants per visit to a flow; applies only
  // after every flow holds its opening fast-start burst.
  uint16_t packetsPerVisit_{1};
  uint16_t portPairCount_{1};

  // Runtime state.
  bool inFastStart_{true};
  int qwnd_{0};
  int qwnd_max_{FLAGS_libsoftmetaroce_initial_qwnd_max};

 private:
  // Runtime state.
  folly::F14FastMap<uint16_t, FlowState> flowIdToFlowState_;

 protected:
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
