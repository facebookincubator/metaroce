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

class CongestionController {
 public:
  struct LossEvent {
    enum class LossType { OutOfOrder, RTO };
    uint16_t numLostPackets{0};
    LossType lossType;
    bool operator==(const LossEvent& other) const {
      return (
          numLostPackets == other.numLostPackets && lossType == other.lossType);
    }
  };
  struct AckEvent {
    uint16_t numAckedPackets{0};
    uint16_t numAckedPacketsWithCE{0}; // Congestion Experienced packets
    // Round-trip sample for this ack, or 0 when it has none: an ack the
    // peer sent without a timestamp, or an event that reports packets leaving
    // the window rather than a reply arriving. Zero is not a fast path.
    uint32_t measuredRttNs{0};
    uint32_t smoothedQpRttNs{0}; // Smoothed RTT across all flows in the QP
    uint32_t rcnDesiredRateGbps{0}; // RCN desired rate (0 if not using RCN)
    // Whether the QP holds data not yet sent when the ack arrives.
    bool sendPending{true};
    bool operator==(const AckEvent& other) const {
      return (
          numAckedPackets == other.numAckedPackets &&
          numAckedPacketsWithCE == other.numAckedPacketsWithCE &&
          measuredRttNs == other.measuredRttNs &&
          smoothedQpRttNs == other.smoothedQpRttNs &&
          rcnDesiredRateGbps == other.rcnDesiredRateGbps &&
          sendPending == other.sendPending);
    }
  };

  struct AckResult {
    bool changeSourcePort{false};
  };

  CongestionController() = default;
  virtual ~CongestionController() = default;

  virtual void onPacketSent(uint16_t flowId) = 0;
  virtual AckResult onPacketAck(uint16_t flowId, const AckEvent& ack) = 0;

  virtual void onPacketLoss(uint16_t flowId, const LossEvent& loss) = 0;
  virtual void onFlowAdded(uint16_t flowId) = 0;
  virtual void onFlowDisabled(uint16_t flowId) = 0;

  /* returns the size of available fwnd in packets */
  virtual uint16_t getAvailableWindowSize(uint16_t flowId) const = 0;
  virtual uint16_t getFwnd(uint16_t /* flowId */) const = 0;
  /* returns the aggregate queue-pair window in packets */
  virtual uint32_t getQwnd() const = 0;
  virtual bool shouldAdvanceFlowAfterSend(uint16_t /* flowId */) const {
    return true;
  }
  /* Accounts for one sent packet, then returns the resulting QP window. */
  virtual uint32_t onPacketSentAndGetQwnd(uint16_t flowId) {
    onPacketSent(flowId);
    return getQwnd();
  }

  virtual void setMTU(uint32_t /* mtu */) {}

  /* Consecutive packets a sender wants each time a flow is visited. 1 spreads
     every packet; larger keeps a run together for a sender that coalesces
     (GSO). Fast start sizes its own initial burst, which this cannot widen. */
  virtual void setPacketsPerVisit(uint16_t /* packetsPerVisit */) {}

  /* Port pairs the flows rotate across, so consecutive flows sit on different
     pairs. Fast start begins a flow on each. */
  virtual void setPortPairCount(uint16_t /* portPairCount */) {}

  struct NextSend {
    uint16_t flowId{0};
    /* The packet after this one moves to a different flow, so this one ends a
       run. Lets the caller request an ack per run rather than per packet. Says
       where the schedule ends a run, so it holds only while the caller sends on
       the flow given here; a caller that places the packet elsewhere carries on
       from the schedule's next position rather than its own. */
    bool lastOnFlow{false};
  };
  /* The flow the next packet should use. std::nullopt leaves the choice to the
     caller. Pure: asking does not advance the schedule, so it can be consulted
     while building. */
  virtual std::optional<NextSend> flowForNextSend() const {
    return std::nullopt;
  }
};

} // namespace libsoftmetaroce
