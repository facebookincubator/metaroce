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

#include <folly/Synchronized.h>
#include <folly/synchronization/RelaxedAtomic.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/Flow.h>
#include <libsoftmetaroce/HighResTimer.h>
#include <libsoftmetaroce/PacketId.h>
#include <libsoftmetaroce/Sender.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/codec/AckHeader.h>
#include <libsoftmetaroce/codec/Packet.h>
#include <bitset>
#include <deque>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

DECLARE_uint32(libsoftmetaroce_max_rto_us);
DECLARE_uint32(libsoftmetaroce_rtos_before_repath);
inline constexpr uint32_t kRtxRepathDisabledSentinel = 0;

namespace libsoftmetaroce {

// Controls when SACK-detected losses trigger retransmission.
enum class SackRetransmissionMode {
  IMMEDIATE, // Retransmit as soon as loss is detected (default)
  NEVER, // Suppress SACK retransmission; only RTO retransmits
  COUNT_BASED_PWND, // Retransmit when SACKed count > fraction × fwnd
  DISTANCE_BASED_PWND, // Retransmit when FSN distance > fraction × fwnd
};

// Configuration for SACK-based retransmission behavior, passed to TxFlow.
struct SackRetransmissionConfig {
  SackRetransmissionMode mode{SackRetransmissionMode::IMMEDIATE};
  double fraction{1.0}; // Threshold multiplier for COUNT/DISTANCE modes
};

std::optional<SackRetransmissionMode> parseSackRetransmissionMode(
    std::string_view mode);
std::string_view toStringView(SackRetransmissionMode mode);

class Qp;
class Tag;
struct EgressPacket;
class DataPacket;

class TxFlow : public Flow {
 public:
  // Smoothing parameter for per-flow RTT measurement
  static constexpr uint16_t kDefaultInverseLog2FlowAlpha = 4;
  // Smoothing parameter for per-flow RTT mean deviation
  static constexpr uint16_t kDefaultInverseLog2FlowRttBeta = 4;
  // Initial estimated RTT value (200us)
  static constexpr uint32_t kInitialEstRttNs = 200'000;
  // It is not permitted for any device to be farther away than 200ms.
  // Our RTO cannot be larger than this value, even after exponential
  // backoff.
  const uint32_t kDefaultMaxRtoNs{
      FLAGS_libsoftmetaroce_max_rto_us * kNsPerUs}; // 200ms

  struct TxState {
    explicit TxState() = default;

    struct TrackerState {
      PacketId id;
      FsnState state;
      constexpr bool isAcked() const {
        return state == FsnState::ACKED;
      }
      constexpr bool isUnacked() const {
        return state == FsnState::UNACKED;
      }
      constexpr bool isBrnr() const {
        return state == FsnState::BRNR;
      }
      constexpr bool isResolved() const {
        return state != FsnState::UNACKED;
      }
      // An abandoned frame: the sender stopped waiting on it. Its WQE may
      // already have completed, so the entry must not be followed to it.
      constexpr bool isAbandoned() const {
        return state == FsnState::ABANDONED;
      }
    };
    uint32_t estRttNs{kInitialEstRttNs};
    uint32_t mdevRttNs{kInitialMdevRttNs};
    bool haveRttSample{false};
    uint16_t sndNxt{0}; // Next FSN to be sent
    uint16_t sndUna{0}; // Next FSN to be (in-order) ACKed
    uint16_t maxAckdFsn{UINT16_MAX};
    folly::Synchronized<uint16_t> sndCur{0}; // Next packet to be retransmitted
    bool inRecycleQ{false};
    std::deque<TrackerState> tracker;

    // Instrumentation only: per-QP teardown diagnostics.
    uint16_t maxInflightFsns{0}; // High-water sndNxt - sndUna window.
  };

  explicit TxFlow(
      Qp& qp,
      uint16_t flowId,
      uint16_t socketId,
      HighResTimer* qpRetxTimer,
      uint16_t portPairIdx,
      SackRetransmissionConfig sackRetxConfig = {});

  TxFlow(TxFlow&& other) noexcept;

  TxFlow(const TxFlow& other) = delete;
  TxFlow& operator=(const TxFlow&) = delete;
  TxFlow& operator=(TxFlow&& other) = delete;

  void setRetransmissionTimer();
  void destroyRetransmissionTimer();

  ~TxFlow();

  [[nodiscard]] uint32_t computeRtoIntervalUs() const;

  /* It first checks and sends ack packets, if there are any acks that has
   * to be sent on this flow. After that, if there is any data to send and
   * there are available window on this flow, it prepares and returns the
   * packet.
   */
  [[nodiscard]] std::unique_ptr<struct EgressPacket> getNextPacketToSend();

  // Returns after(sndNxt, sndUna), safely holding the sndCur lock.
  [[nodiscard]] bool hasUnackedData() const;

  // Process an ACK at the current instant -- the common path. Never concurrent
  // with the send path: an inline-ACK receiver takes Qp::txStateLock_, and a
  // handed-off ACK is applied on the sender's own thread.
  void processAckPacket(const AckHeader& ack);
  // Overload for a caller that already holds "now" (to share one clock read
  // with instrumentation) or drives a specific RTT in a test. nowNs is the
  // processing instant (the RTT sample is nowNs - ack timestamp).
  void processAckPacket(const AckHeader& ack, uint32_t nowNs);

  // If we receive an HRNR. we need to pause the whole QP.
  void processHrnrPacket(const AckHeader& hrnr);

  // If we receive a BRNR, pause each BRNR'd frame's own tag at its
  // first-blocked MSN. Each frame's tag comes from the tracker
  // (minBrnrMsnByTag), so a BRNR spanning several tags pauses each of them.
  void processBrnrPacket(
      const AckHeader& brnr,
      const std::unordered_map<uint8_t, uint16_t>& minBrnrMsnByTag);

  // If we receive NAK_FLOW_ID_ERROR, release all frames and disable the flow.
  void processNakFlowIdError();

  // Handle fatal NAK errors (NAK_ADDRESS_INVALID_ACCESS_ERROR,
  // NAK_OPERATIONAL_ERROR or NAK_INVALID_REQUEST). Marks the work requests of
  // the NAK's tag failing and reports them as their packets settle, leaving
  // the other tags sharing this flow untouched.
  void processNakFatalError(const AckHeader& ack);

  // Called when either a loss by out-of-order is detected upon processing
  // an ACK or when retransmission timer expires. Returns the number of
  // packets queued for retransmission.
  uint16_t prepareRetxPacketsIfAny(bool isRtoEvent);

  [[nodiscard]] uint32_t computeRttNs(uint32_t timestampNs, uint32_t nowNs)
      const;
  void updateRtt(uint32_t timestampNs, uint32_t nowNs);

  [[nodiscard]] uint16_t getSocketId() const {
    return socketId_;
  }

  TxState& getTxState() {
    return txState_;
  }

  // For testing; except this leaked into Hrnr handling.
  [[nodiscard]] size_t getNumPendingRetxPackets() const {
    return retxPktsToSend_.rlock()->size();
  }

  void purgeRetxPktsToSend() {
    retxPktsToSend_.wlock()->clear();
  }

  // Adds affected tag IDs to the bitset, preserving existing marks.
  // After processing all flows, the caller retires settled WQEs for those tags.
  void abandonFramesOfFailingWqes(
      std::bitset<kMetaRoceTagCount>& tagsWithAbandonedFrames);

  // Takes this flow out of service without touching the tracker: it sends no
  // new data and ignores further acks, but the frames already on it are left
  // for the retransmission scan to resolve rather than abandoned here.
  void stopSending() {
    // One way: congestion control is told the flow went out of service, not
    // told again on every later visit that finds it already there.
    if (disabled_) {
      return;
    }
    disableAndNotifyCongestionControl();
  }

  [[nodiscard]] bool isBlockedMsn(const Tag& tag, uint16_t msn) const;

 private:
  // Body of retxTimerCallback_, so it runs on the timer thread.
  void onRetransmissionTimeout();

  [[nodiscard]] uint16_t findAlternatePortPair(
      uint16_t activePortSet,
      bool sameLocalOnly) const;
  void maybeFailoverRemotePort();

  void disableAndNotifyCongestionControl();

  // Computes rate hints based on unsent POSN count vs qwnd.
  // Returns 0 if unsent < qwnd (near end of transfer), else
  // kDefaultRateHintGbps. `qwnd` includes this packet because `onPacketSent()`
  // may change the window.
  [[nodiscard]] uint32_t computeRateHints(uint32_t qwnd) const;
  // Caller must hold sndCur.wlock(): this advances sndCur-guarded state.
  std::unique_ptr<struct EgressPacket> getNextPacketToSendFromWqe(
      Wqe& wqe,
      uint8_t tagNum);

  // Move the window past the frames at its head that are resolved -- acked,
  // BRNR'd or abandoned. Leaving any of them stalls snd_una behind work that
  // is already resolved. Caller must hold sndCur.wlock().
  // Returns how many of the retired frames were abandoned rather than
  // acknowledged; the caller returns their share of the window.
  uint16_t retireResolvedFramesAtWindowHead(uint16_t& sndCur);

  // Offset of an fsn within the tracker, empty when it falls outside the
  // window. Caller must hold sndCur.
  [[nodiscard]] std::optional<uint16_t> getTrackerOffset(uint16_t fsn) const;

  // One queued retransmission, or nullptr if none is waiting. Takes the queue
  // lock alone, so the caller can ask under the Tag whether the frame is still
  // worth sending.
  std::unique_ptr<DataPacket> popQueuedRetxPacket();
  // Returns the next queued retransmission worth sending, discarding those
  // whose frame is already resolved or whose work must stop being
  // retransmitted, or nullptr when none is left. Caller must hold sndCur: the
  // tracker decides whether a queued frame is still unresolved.
  std::unique_ptr<struct EgressPacket> tryGetRetransmissionPacket();

  // A message: an msn within a tag. Value-typed on purpose, so resolving a
  // frame hands out no Wqe* to be followed once sndCur is dropped.
  struct MessageId {
    uint8_t tagNum{0};
    // Empty for a response, whose msn belongs to the initiator.
    std::optional<uint16_t> msn;
  };
  // Resolves an FSN to the message that sent it, under sndCur. Empty once the
  // frame is no longer outstanding.
  [[nodiscard]] std::optional<MessageId> getOutstandingFrameMessage(
      uint16_t fsn) const;

  // Helper function to try getting a new packet from work queue element
  std::unique_ptr<struct EgressPacket> tryGetNewPacketFromWqe();

  // Count ACKED entries in the tracker in the range (sndCur, maxAckdFsn].
  uint16_t countSackedPackets(uint16_t sndCur) const;

  // Check whether SACK-based retransmission should trigger based on the
  // configured SackRetransmissionMode.
  bool isAllowedToFastRetransmit(uint16_t sndCur) const;

  // Compute the cursor limit for SACK-triggered retransmission.
  // IMMEDIATE: retransmit all unacked up to maxAckdFsn.
  // COUNT/DISTANCE_BASED_PWND: retransmit only packets outside the
  // reordering tolerance window (maxAckdFsn - fwnd*fraction).
  uint16_t sackRetxCursorLimit() const;

  // Injected dependency.
  HighResTimer* qpRetxTimer_;

  // Configuration.
  const SackRetransmissionConfig sackRetxConfig_;

  // Protocol / runtime state.
  TxState txState_;
  folly::relaxed_atomic<uint16_t> consecutiveRtosWithoutAck_{0};
  folly::Synchronized<std::deque<std::unique_ptr<DataPacket>>> retxPktsToSend_;
  std::unique_ptr<HighResTimer::Callback> retxTimerCallback_;
  bool disabled_{false};

  friend class QpTest_AFencedSendDoesNotStopTheRtoRetransmittingItsPrior_Test;
  friend class QpTest_ARetransmissionQueuedBeforeTheFailureIsNotSent_Test;
  friend class QpTest_CountSackedPacketsDirectWrap_Test;
  friend class QpTest_RtoWithQueuedPacketsKeepsTimerArmed_Test;
  friend class QpTest_RtoRequeuesQpAfterEagain_Test;
};

} // namespace libsoftmetaroce
