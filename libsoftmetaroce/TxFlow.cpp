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

#include <libsoftmetaroce/TxFlow.h>

#include <bitset>

#include <folly/lang/Switch.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/RnrState.h>
#include <libsoftmetaroce/Tag.h>
#include <libsoftmetaroce/TagSet.h>
#include <libsoftmetaroce/WrappingCompare.h>
#include <libsoftmetaroce/codec/Header.h>
#include <libsoftmetaroce/congestion_control/CongestionController.h>
#include <exception>
#include <limits>
#include <unordered_map>

DEFINE_uint32(
    libsoftmetaroce_rtos_before_repath,
    2,
    "Try to repath (change entropy) if this many RTOs have occurred on a socket");

DECLARE_uint32(libsoftmetaroce_min_rto_us);
DEFINE_bool(
    libsoftmetaroce_slow_receiver,
    true,
    "apply settings that accommodate a slow receiver: ignore staleness of acks and use long rto");

DEFINE_uint32(
    libsoftmetaroce_initial_est_rtt_us,
    200,
    "Smoothed RTT a new flow assumes before its first RTT sample. With "
    "--libsoftmetaroce_initial_mdev_rtt_us this fixes the cold-start RTO, "
    "which is estRtt + 4*mdev.");

DEFINE_uint32(
    libsoftmetaroce_initial_mdev_rtt_us,
    1000,
    "RTT mean deviation a new flow assumes before its first RTT sample. Left "
    "unset, --libsoftmetaroce_slow_receiver supplies 250ms instead, holding "
    "the cold-start RTO near a second; setting this takes precedence.");

DEFINE_bool(
    libsoftmetaroce_rfc6298_first_rtt_sample,
    true,
    "RFC 6298 section 2.2: on a flow's first RTT sample R, set srtt to R and "
    "mdev to R/2 outright instead of filtering R into the assumed values. "
    "Turned off, the assumed values decay at 1/16 per ACK, so a short flow can "
    "finish before the estimate ever reaches the real RTT.");

namespace {
// Both initial-estimate flags reach their uint32_t nanosecond fields as
// `value * kNsPerUs`, which wraps rather than saturating. Reject the value at
// parse time so no conversion site has to re-check it.
bool validateInitialRttUs(const char* flagname, uint32_t valueUs) {
  constexpr uint32_t kMaxUs =
      std::numeric_limits<uint32_t>::max() / libsoftmetaroce::kNsPerUs;
  if (valueUs > kMaxUs) {
    LOG(ERROR) << flagname << "=" << valueUs << "us exceeds the maximum "
               << kMaxUs << "us; converting it to nanoseconds would overflow";
    return false;
  }
  return true;
}
} // namespace

DEFINE_validator(libsoftmetaroce_initial_est_rtt_us, &validateInitialRttUs);
DEFINE_validator(libsoftmetaroce_initial_mdev_rtt_us, &validateInitialRttUs);

namespace libsoftmetaroce {

namespace {

// Convert fatal NAK AckStatus to the corresponding WorkCompletionStatus.
WorkCompletionStatus ackStatusToWorkCompletionStatus(AckStatus ackStatus) {
  FOLLY_NON_EXHAUSTIVE_SWITCH({
    switch (ackStatus) {
      case AckStatus::NAK_INVALID_REQUEST:
        return kWrRemInvReqErr;
      case AckStatus::NAK_OPERATIONAL_ERROR:
        return kWrRemOpsErr;
      case AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR:
        return kWrRemAccErr;
      default:
        LOG(DFATAL) << "Unexpected AckStatus for error completion: "
                    << static_cast<int>(ackStatus);
        return kWcGenErr;
    }
  });
}

} // namespace

TxFlow::TxFlow(
    Qp& qp,
    uint16_t flowId,
    uint16_t socketId,
    HighResTimer* qpRetxTimer,
    uint16_t portPairIdx,
    SackRetransmissionConfig sackRetxConfig)
    : Flow(qp, flowId, socketId, portPairIdx),
      qpRetxTimer_{qpRetxTimer},
      sackRetxConfig_{sackRetxConfig} {
  retxTimerCallback_ = std::make_unique<HighResTimer::Callback>(
      [this]() { onRetransmissionTimeout(); });
  txState_.estRttNs = FLAGS_libsoftmetaroce_initial_est_rtt_us * kNsPerUs;
  txState_.mdevRttNs = FLAGS_libsoftmetaroce_initial_mdev_rtt_us * kNsPerUs;
  // Slow-receiver mode supplies a prior, so an operator who set a mdev wins.
  if (FLAGS_libsoftmetaroce_slow_receiver &&
      gflags::GetCommandLineFlagInfoOrDie("libsoftmetaroce_initial_mdev_rtt_us")
          .is_default) {
    txState_.mdevRttNs = kSlowReceiverMdevRttNs;
  }

  VLOG(2) << "TxFlow Initialized, flowId: " << flowId
          << " socketId: " << socketId_
          << " repath-thresh: " << FLAGS_libsoftmetaroce_rtos_before_repath;
  LOG_FIRST_N(INFO, 1) << "SackRetransmissionMode: "
                       << toStringView(sackRetxConfig_.mode)
                       << " fraction: " << sackRetxConfig_.fraction;
}

TxFlow::TxFlow(TxFlow&& other) noexcept
    // Copy base intentionally: moving it would trigger use-after-move on
    // the subclass members accessed below.
    // NOLINTNEXTLINE(performance-move-constructor-init)
    : Flow(other),
      qpRetxTimer_{other.qpRetxTimer_},
      sackRetxConfig_{other.sackRetxConfig_},
      txState_{std::move(other.txState_)},
      consecutiveRtosWithoutAck_{other.consecutiveRtosWithoutAck_.load()},
      lastConsumedRtoCount_{other.lastConsumedRtoCount_} {
  other.qpRetxTimer_ = nullptr;
}

TxFlow::~TxFlow() {
  destroyRetransmissionTimer();
  if (txState_.sndUna != txState_.sndNxt) {
    LOG(WARNING) << "destroying flow with unacked data in flight";
    txState_.sndUna = txState_.sndNxt;
  }
  auto lockedRetx = retxPktsToSend_.wlock();
  if (!lockedRetx->empty()) {
    LOG(WARNING) << "destroying flow with pending retransmissions";
    lockedRetx->clear();
  }
}

uint32_t TxFlow::computeRtoIntervalUs() const {
  unsigned int exponent = std::min(
      kMaxRtoBackoffExponent, (unsigned int)consecutiveRtosWithoutAck_);
  // Allow up to 2s RTO for slow receivers; 1s initial.
  const auto maxRtoNs = FLAGS_libsoftmetaroce_slow_receiver
      ? kSlowReceiverMaxRtoNs
      : kDefaultMaxRtoNs;
  const auto rtoNs = std::max(
      (unsigned long)FLAGS_libsoftmetaroce_min_rto_us * kNsPerUs,
      std::min(
          (unsigned long)(txState_.estRttNs + (txState_.mdevRttNs << 2))
              << exponent,
          maxRtoNs));
  // RTO timeout precision is in microseconds
  // @lint-ignore CLANGTIDY clang-diagnostic-shorten-64-to-32
  return rtoNs / kNsPerUs;
}

void TxFlow::onRetransmissionTimeout() {
  if (qp_.isTearingDown()) {
    return;
  }
  if (!qpRetxTimer_) {
    LOG(ERROR)
        << "RTO handler invoked after timer destruction; this is an error, timers should be destroyed before QP teardown";
    return;
  }

  // If we can still send new data (CC window available, not HRNR blocked,
  // and have WQEs ready), skip retransmission and just signal to send.
  // This avoids wasteful retransmission when we simply haven't had CPU
  // time to send due to other transmissions or local contention.
  if (qp_.getHrnrState() != HrnrState::HRNR_PROBING &&
      qp_.getHrnrState() != HrnrState::HRNR_BLOCKED_TX &&
      qp_.congestionController_ &&
      qp_.congestionController_->getAvailableWindowSize(flowId_) > 0 &&
      static_cast<uint16_t>(txState_.sndNxt - txState_.sndUna) <
          kRtoSkipWindowThreshold) {
    if (auto wqeAndTag = qp_.tagSet_.findNextWqe(false /* intendToSend */)) {
      auto rtoUs = computeRtoIntervalUs();
      // Release the tag semaphore acquired by findNextWqe().
      wqeAndTag->second->markFree();
      // The QP may be out of the ready set (see handleFaultOnEgress), and a
      // dropped signal for an unsent WQE with no frame outstanding behind it
      // leaves no other path to retry it.
      setRetransmissionTimer();
      LOG_FIRST_N(INFO, 20)
          << "RetxTimer expired (rto=" << rtoUs << "us) for qp "
          << qp_.getLocalQpNum() << " flow " << flowId_
          << " but can still send (window="
          << qp_.congestionController_->getAvailableWindowSize(flowId_)
          << "); rescheduling";
      qp_.signalReadyAsync();
      return;
    }
  }

  bool watchdogExpired;
  {
    // Keep the outstanding check and count increment ordered with ACK resets.
    auto lockedSndCur = txState_.sndCur.rlock();
    if (txState_.sndUna == txState_.sndNxt) {
      return;
    }
    // The first RTO retransmits; only a later one may fail the work.
    watchdogExpired =
        consecutiveRtosWithoutAck_ > 0 && qp_.hasWatchdogExpired();
    if (!watchdogExpired) {
      ++consecutiveRtosWithoutAck_;
    }
  }
  if (watchdogExpired) {
    qp_.processWatchdogExpiryAsync();
    return;
  }

  VLOG(2) << "RetxTimer expired for qp: " << qp_.getLocalQpNum()
          << " flow: " << flowId_ << ". HRNR state is " << qp_.getHrnrState()
          << " una: " << txState_.sndUna << " nxt: " << txState_.sndNxt;
  prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  // Frames are still outstanding while sndUna trails sndNxt, and this timer
  // is the only thing that returns to them. Locked because reading the pair
  // unlocked could catch them equal mid-update and stop the timer for good.
  auto lockedSndCur = txState_.sndCur.rlock();
  if (txState_.sndUna != txState_.sndNxt) {
    setRetransmissionTimer();
  }
}

void TxFlow::setRetransmissionTimer() {
  if (qp_.isTearingDown()) {
    return;
  }
  if (qpRetxTimer_ && retxTimerCallback_) {
    qpRetxTimer_->scheduleTimeout(
        std::chrono::microseconds(computeRtoIntervalUs()),
        retxTimerCallback_.get());
  }
}

void TxFlow::destroyRetransmissionTimer() {
  if (qpRetxTimer_ && retxTimerCallback_) {
    auto cb = retxTimerCallback_.get();
    auto holder = std::move(retxTimerCallback_);
    retxTimerCallback_ = nullptr;
    qpRetxTimer_->cancelCallback(cb);
  }
  qpRetxTimer_ = nullptr;
}

std::optional<uint16_t> TxFlow::getTrackerOffset(uint16_t fsn) const {
  const uint16_t offset = fsn - txState_.sndUna;
  if (before(fsn, txState_.sndUna) || offset >= txState_.tracker.size()) {
    return std::nullopt;
  }
  return offset;
}

std::unique_ptr<DataPacket> TxFlow::popQueuedRetxPacket() {
  auto lockedDeque = retxPktsToSend_.wlock();
  if (lockedDeque->empty()) {
    return nullptr;
  }
  auto retxPacket = std::move(lockedDeque->front());
  lockedDeque->pop_front();
  return retxPacket;
}

std::unique_ptr<struct EgressPacket> TxFlow::tryGetRetransmissionPacket() {
  // Popped before the tag is taken: the queueing path owns a tag and then
  // takes this deque, so taking them the other way round here would deadlock.
  for (auto retxPacket = popQueuedRetxPacket(); retxPacket != nullptr;
       retxPacket = popQueuedRetxPacket()) {
    // snd_una is cumulative and says nothing about the frames behind it: one
    // selectively acked is delivered, and its WQE may since have been freed.
    const uint16_t fsn = retxPacket->getDataHeader().getFsn();
    const auto offsetInTracker = getTrackerOffset(fsn);
    if (!offsetInTracker.has_value()) {
      VLOG(2) << "Dropping queued retx packet for qp: " << qp_.getLocalQpNum()
              << " fsn: " << fsn << " which the tracker no longer holds";
      continue;
    }
    const auto& trackerEntry = txState_.tracker.at(*offsetInTracker);
    if (trackerEntry.isResolved()) {
      VLOG(2) << "Dropping queued retx packet for qp: " << qp_.getLocalQpNum()
              << " fsn: " << fsn << " whose frame is already resolved";
      continue;
    }
    const auto& trackerId = trackerEntry.id;
    bool stopRetransmitting = false;
    {
      // A frame in the tracker was built from a Tag of this QP, and a Tag is
      // never taken out of the set while the QP lives, so this resolves.
      auto tag = qp_.getTag(trackerId.tagNum);
      auto tagGuard = tag->scopedLock();
      stopRetransmitting =
          tag->mustStopRetransmitting(*CHECK_NOTNULL(trackerId.wqe));
    }
    if (stopRetransmitting) {
      VLOG(2) << "Dropping queued retx packet for qp: " << qp_.getLocalQpNum()
              << " fsn: " << fsn
              << " whose work request has no posn left to send";
      // The frame stays outstanding, and the scan that abandons it runs from
      // this timer; no other path arms it once we stop sending.
      setRetransmissionTimer();
      continue;
    }

    setRetransmissionTimer();
    VLOG(1) << "Sending retx packet for qp: " << qp_.getLocalQpNum()
            << " fsn: " << retxPacket->getDataHeader().getFsn()
            << " msn: " << retxPacket->getDataHeader().getMsn()
            << " posn: " << retxPacket->getDataHeader().getPosn()
            << " flow: " << flowId_ << " socketId: " << socketId_;

    return std::make_unique<struct EgressPacket>(
        std::move(retxPacket),
        qp_.getSender().getSendSocket(socketId_, EgressPacket::Kind::Data),
        getRemoteAddr(),
        qp_.getLocalQpNum(),
        consecutiveRtosWithoutAck_ ? EgressPacket::RxmitType::Rto
                                   : EgressPacket::RxmitType::Fast);
  }
  return nullptr;
}

std::unique_ptr<struct EgressPacket> TxFlow::tryGetNewPacketFromWqe() {
  Wqe* FOLLY_NULLABLE wqe = nullptr;
  std::unique_ptr<struct EgressPacket> packet;
  {
    // Acquire sndCur before findNextWqe (which acquires the Tag semaphore) so
    // the send path takes locks in sndCur->Tag order, matching the retransmit
    // path.
    auto lockedSndCur = txState_.sndCur.wlock();
    if (auto maybeWqeAndTag = qp_.tagSet_.findNextWqe(true /* intendToSend */);
        maybeWqeAndTag && (wqe = maybeWqeAndTag->first)) {
      auto tag = maybeWqeAndTag->second;
      const auto tagNum = tag->getTagNum();
      CHECK_NOTNULL(wqe); // for lint(facebook-hte-NullableDereference)
      if (!tag->isInErrorState()) {
        try {
          packet = getNextPacketToSendFromWqe(*wqe, tagNum);
          VLOG(2) << "TxFlow::tryGetNewPacketFromWqe() found a packet on WQE";
          tag->markProbeSentIfBlockedMsn(*wqe);
          // Release the tag semaphore acquired by findNextWqe().
          tag->markFree();
        } catch (const OversizeWqeException& ex) {
          LOG(ERROR) << "Oversize WQE error: " << ex.what();
          SenderWake tagErrorWake = SenderWake::NOT_NEEDED;
          {
            auto retirementGuard = tag->lockForWqeRetirement();
            // Handling this one work request here does not prove the tag has
            // no other pending work, so the sweep still has to run.
            tagErrorWake = qp_.enterTagErrorWithTagHeld(*tag);
            // Not one packet of this message was sent, so it is accounted for
            // in full here. sndCur and the Tag are both already held, in that
            // order.
            uint32_t abandonedCount = wqe->markFailing(kLocalLenErr);
            abandonedCount += tag->markPendingWqesFailing(kWrFlushErr);
            qp_.subtractUnsentPosns(abandonedCount);
            tag->flushPendingReceiveWqesInError();
            tag->completeSettledFailedWqes();
          }
          tag->markFree();
          lockedSndCur.unlock();
          // Waking the sender can block, so drop locks first.
          qp_.wakeSenderAfterTagError(tagErrorWake);
          return nullptr;
        }
      } else {
        // The WQEs live on until the tracker entries holding their frames
        // resolve; the posns abandoned here come off the unsent count now.
        qp_.subtractUnsentPosns(tag->failPendingWqesIfInErrorWithTagHeld());
        VLOG(2)
            << "TxFlow::tryGetNewPacketFromWqe() error state - no packet returned";
        tag->markFree();
      }
    } else {
      // either maybeWqeAndTag false or wqe was null
      if (wqe) {
        VLOG(2) << "TxFlow::tryGetNewPacketFromWqe() invalid WQE+tag";
      } else {
        VLOG(2) << "TxFlow::tryGetNewPacketFromWqe() invalid WQE";
      }
    }
  }
  return packet;
}

std::unique_ptr<struct EgressPacket> TxFlow::getNextPacketToSend() {
  if (disabled_) {
    VLOG(2) << "TxFlow::getNextPacketToSend() flow " << flowId_
            << " is disabled due to NAK_FLOW_ID_ERROR";
    return nullptr;
  }

  applyRtoRepath();
  maybeFailoverRemotePort();

  // sndCur is held across the attempt so the tracker can be consulted, in the
  // sndCur->retx order the queueing code uses; released before the write lock.
  {
    auto lockedSndCur = txState_.sndCur.rlock();
    if (auto retxPacket = tryGetRetransmissionPacket()) {
      return retxPacket;
    }
  }

  if (qp_.congestionController_ &&
      qp_.congestionController_->getAvailableWindowSize(flowId_) == 0) {
    VLOG(2) << "TxFlow: cannot return a pkt to send due to zero window";
    return nullptr;
  }

  if (auto newPacket = tryGetNewPacketFromWqe()) {
    return newPacket;
  }

  VLOG(2) << "TxFlow::getNextPacketToSend() found no packet to send";
  return nullptr;
}

bool TxFlow::hasUnackedData() const {
  auto lockedSndCur = txState_.sndCur.rlock();
  return after(txState_.sndNxt, txState_.sndUna);
}

/*
 * Build a packet to send from specified WQE+tag. Set RTX timer and
 * update CC.
 *
 * The caller must supply a WQE eligible to send. Returns a packet; an oversize
 * WQE throws OversizeWqeException instead.
 *
 * Caller (e.g., tryGetNewPacketFromWqe) must hold sndCur.wlock() and is
 * responsible for semaphore management. In particular, should call
 * qp->getTag(tagNum)->markFree() when finished with the tag.
 */
std::unique_ptr<struct EgressPacket> TxFlow::getNextPacketToSendFromWqe(
    Wqe& wqe,
    uint8_t tagNum) {
  auto posn = wqe.allocateNextPosn();
  qp_.tagSet_.decrementUnsentPosns();
  const uint32_t sendPayloadSize = wqe.canHaveSendPayload()
      ? wqe.getPosnPayloadSizeBytes(posn, qp_.getMtu())
      : 0;

  VLOG(2) << "TxFlow::getNextPacketToSendFromWqe: Sending data fsn: "
          << txState_.sndNxt << " msn: " << wqe.getMsn().value()
          << " posn: " << posn << " flow: " << flowId_
          << " payload(bytes): " << sendPayloadSize
          << " tagnum: " << static_cast<uint32_t>(tagNum)
          << " socketId: " << socketId_;

  // holding sndCur write lock
  txState_.tracker.push_back(
      {PacketId(&wqe, tagNum, posn, sendPayloadSize), FsnState::UNACKED});

  setRetransmissionTimer();

  uint32_t qwndAfterSend = 0;
  if (qp_.congestionController_) {
    qwndAfterSend = qp_.congestionController_->onPacketSentAndGetQwnd(flowId_);
  }

  uint32_t rateHints = computeRateHints(qwndAfterSend);

  std::unique_ptr<DataPacket> dataPacket = qp_.buildDataPacket(
      flowId_,
      qp_.getLocalQpNum(),
      qp_.getRemoteQpNum(),
      tagNum,
      txState_.sndNxt++,
      txState_.sndUna,
      wqe,
      posn,
      wqe.makeSendPayloadView(posn, qp_.getMtu(), sendPayloadSize),
      false, // not a retransmission
      false, // not a retransmission by timeout
      rateHints);

  // High-water inflight window (sndNxt - sndUna) for the QP-teardown BDP
  // estimate; both are mutated only under the sndCur write lock held here.
  const uint16_t inflightFsns = txState_.sndNxt - txState_.sndUna;
  txState_.maxInflightFsns = std::max(txState_.maxInflightFsns, inflightFsns);

  return std::make_unique<struct EgressPacket>(
      std::move(dataPacket),
      qp_.getSender().getSendSocket(socketId_, EgressPacket::Kind::Data),
      getRemoteAddr(),
      qp_.getLocalQpNum(),
      EgressPacket::RxmitType::None);
} // getNextPacketToSendFromWqe

void TxFlow::processHrnrPacket(const AckHeader& hrnr) {
  qp_.incrementHrnrPacketsReceived();
  uint32_t rnrBackoffTime = hrnr.computeRnrBackoffTimeUs();
  VLOG(1) << "Received HRNR, pausing qp: " << qp_.getLocalQpNum() << " for "
          << rnrBackoffTime << " us";
  qp_.enterHrnrBlock(rnrBackoffTime);
}

void TxFlow::processBrnrPacket(
    const AckHeader& brnr,
    const std::unordered_map<uint8_t, uint16_t>& minBrnrMsnByTag) {
  // A BRNR that blocks no new frame is a no-op: re-arming the pause on the
  // receiver's repeated NAKs would only defer the probe that clears it.
  if (minBrnrMsnByTag.empty()) {
    return;
  }

  // Without a timer to arm, no callback can resume the pause.
  if (!qpRetxTimer_ || !qp_.getBrnrTimerCallback()) {
    return;
  }
  const uint32_t rnrBackoffTime = brnr.computeRnrBackoffTimeUs();

  for (const auto& [tagNum, minBrnrMsn] : minBrnrMsnByTag) {
    // Tracker entries identify Tags retained until whole-QP teardown.
    const auto tagHandle = qp_.getTag(tagNum);
    Tag& tag = *CHECK_NOTNULL(tagHandle.get());
    auto tagGuard = tag.scopedLock();
    tag.brnr().blockAt(minBrnrMsn);
    VLOG(1) << "Received BRNR, pausing qp: " << qp_.getLocalQpNum()
            << " tag: " << (uint32_t)tagNum
            << " from msn: " << tag.getBrnrFirstBlockedMsn().value_or(-1)
            << " for " << rnrBackoffTime << " us";
  }

  // Schedule after setting the blocked state so it cannot fire prematurely.
  qp_.retxTimer_.scheduleTimeout(
      std::chrono::microseconds(rnrBackoffTime), qp_.getBrnrTimerCallback());
}

void TxFlow::processNakFlowIdError() {
  LOG(ERROR) << "Received NAK_FLOW_ID_ERROR for qp: " << qp_.getLocalQpNum()
             << " flow: " << flowId_;

  // Release all posns for frames that have been sent
  // Note: signalReadyAsync() must be called OUTSIDE the sndCur lock to avoid
  // deadlock with the arbiter which holds arbiterMutex_ while calling
  // getNextPacketToSend() which acquires sndCur.rlock().
  int numReleasedForRetry = 0;
  std::bitset<kMetaRoceTagCount> tagsWithAbandonedFrames;
  {
    auto lockedSndCur = txState_.sndCur.wlock();
    for (auto& trackerEntry : txState_.tracker) {
      if (trackerEntry.isResolved()) {
        // A resolved frame's wqe pointer may already be freed.
        continue;
      }
      const auto& trackerId = trackerEntry.id;
      // The work request's failing state, and the frame status it decides,
      // are written under the Tag, so read and act on them under it.
      auto tagGuard = qp_.getTag(trackerId.tagNum)->scopedLock();
      Wqe& wqe = *CHECK_NOTNULL(trackerId.wqe);
      if (!wqe.isFrameAllocated(trackerId.posn)) {
        continue;
      }
      const bool served = wqe.hasBeenServed();
      if (served) {
        LOG_FIRST_N(ERROR, 10)
            << "qp " << qp_.getLocalQpNum() << " flow " << flowId_
            << " was rejected after the peer served a read or atomic sent on "
            << "it; a peer cannot both refuse the request and have served it";
      }
      if (wqe.isFailing() || served) {
        // Handing the frame back offers it to another flow, and neither a
        // failing message nor a served read or atomic has any posn to send.
        wqe.abandonFrame(trackerId.posn);
        tagsWithAbandonedFrames.set(trackerId.tagNum);
      } else {
        wqe.releaseFrame(trackerId.posn);
        qp_.tagSet_.addUnsentPosns(1);
        numReleasedForRetry++;
      }
    }
    // A receiver is not permitted to reject flow 0.
    if (flowId_ == 0) {
      LOG_FIRST_N(ERROR, 10)
          << "keeping flow 0 in service on qp " << qp_.getLocalQpNum();
    } else {
      disableAndNotifyCongestionControl();
    }
    // Destroy the retransmission timer to prevent further retransmissions
    destroyRetransmissionTimer();
    // Do not process these tracker entries again if we see another
    // flow id error nak.
    txState_.tracker.clear();
    // Advance sndUna to sndNxt since we've released all frames
    txState_.sndUna = txState_.sndNxt;
  } // Release sndCur lock before calling signalReadyAsync()

  // The NAK is itself proof the responder is alive, so the HRNR block can go.
  qp_.cancelHrnrBackoff();

  qp_.retireTagsWithAbandonedFrames(tagsWithAbandonedFrames);

  // Only work that can still be sent is worth waking the sender for.
  if (numReleasedForRetry) {
    LOG(ERROR) << "Released " << numReleasedForRetry
               << " frames for retry and disabled flow; signaling qp ready for "
                  "transmission.";
    qp_.signalReadyAsync();
  }
}

std::optional<TxFlow::MessageId> TxFlow::getOutstandingFrameMessage(
    uint16_t fsn) const {
  auto lockedSndCur = txState_.sndCur.rlock();
  const auto offsetInTracker = getTrackerOffset(fsn);
  if (!offsetInTracker.has_value()) {
    return std::nullopt;
  }
  const auto& entry = txState_.tracker.at(*offsetInTracker);
  if (entry.isResolved()) {
    return std::nullopt;
  }
  // A tracker entry leaves UNACKED only under sndCur, and a work request is
  // retired only after it does, so an entry still UNACKED points to a WQE no
  // thread can be freeing. Work type and msn are fixed before publication.
  const auto& id = entry.id;
  const Wqe& wqe = *CHECK_NOTNULL(id.wqe);
  if (wqe.isVirtual()) {
    // A response carries the initiator's msn, from a space this side does not
    // assign, so comparing it with an msn a peer reports is meaningless.
    return MessageId{id.tagNum, std::nullopt};
  }
  return MessageId{id.tagNum, wqe.getMsn().value()};
}

void TxFlow::processNakFatalError(const AckHeader& ack) {
  // Fails all the tag's work, each kind where it lives: the send the NAK
  // identifies, the rest of the sends and the virtual ones in
  // markPendingWqesFailing(), the receives in flushPendingReceiveWqesInError().

  // Ahead of the consistency check below, so a peer using a tag this QP does
  // not have is turned away on that alone rather than on the check's verdict.
  if (!qp_.containsTag(ack.getTag())) {
    LOG(WARNING) << "discarding fatal NAK for unknown Tag: "
                 << (uint32_t)ack.getTag();
    return;
  }

  const auto nackedMessage = getOutstandingFrameMessage(ack.getCumulativeFsn());
  // Spec 8.5.5: a fatal NAK's tag/msn and its cfsn describe one frame two ways,
  // so a responder disagreeing with itself has left the message ambiguous.
  if (nackedMessage.has_value() && nackedMessage->msn.has_value() &&
      (nackedMessage->tagNum != ack.getTag() ||
       *nackedMessage->msn != ack.getCumulRespDeliveredMsn())) {
    LOG_FIRST_N(ERROR, 10) << "Fatal NAK carries tag " << (uint32_t)ack.getTag()
                           << " msn " << ack.getCumulRespDeliveredMsn()
                           << ", but its cfsn " << ack.getCumulativeFsn()
                           << " was sent for tag "
                           << (uint32_t)nackedMessage->tagNum << " msn "
                           << *nackedMessage->msn
                           << " on qp: " << qp_.getLocalQpNum()
                           << "; blaming the msn the responder sent";
  }

  auto tag = qp_.getTag(ack.getTag());

  // The Tag semaphore is what excludes the tx path from the WQEs being marked.
  // No call here holds sndCur, so the lock order is respected.
  SenderWake tagErrorWake = SenderWake::NOT_NEEDED;
  {
    auto tagGuard = tag->scopedLock();
    auto retirementGuard = tag->lockForWqeRetirement();
    const uint16_t failedMsn = ack.getCumulRespDeliveredMsn();
    const bool nackedFrameIsResponse = nackedMessage.has_value() &&
        nackedMessage->tagNum == ack.getTag() &&
        !nackedMessage->msn.has_value();
    const bool msnWasNeverIssued =
        !nackedFrameIsResponse && !before(failedMsn, tag->getNextMsn());
    if (msnWasNeverIssued) {
      // Erroring on it would let one bad packet fail every work request the
      // tag holds.
      LOG_FIRST_N(ERROR, 10)
          << "qp " << qp_.getLocalQpNum() << " tag "
          << static_cast<uint32_t>(ack.getTag()) << " discarding a fatal NAK "
          << "for msn " << failedMsn << ", never issued; next is "
          << tag->getNextMsn();
      return;
    }
    Wqe* failedWqe =
        nackedFrameIsResponse ? nullptr : tag->getPendingWqe(failedMsn);

    const bool failureAlreadyMarked =
        failedWqe != nullptr && failedWqe->isFailing();
    if (failureAlreadyMarked) {
      return;
    }
    // Past the return above, so the bound counts tags that failed rather than
    // the frames the peer NAKs for one of them.
    LOG_FIRST_N(ERROR, 10) << "Received fatal NAK (status="
                           << static_cast<int>(ack.getAckStatus())
                           << ") for qp: " << qp_.getLocalQpNum()
                           << " flow: " << flowId_
                           << " tag: " << (uint32_t)ack.getTag();
    // The error state is published under the tag, so the wake it returns is
    // issued after this scope.
    tagErrorWake = qp_.enterTagErrorWithTagHeld(*tag);
    const auto nakStatus = ackStatusToWorkCompletionStatus(ack.getAckStatus());

    uint32_t abandonedCount = 0;
    // A work request that cannot take this status -- gone, or already holding
    // one of its own -- hands it to the earliest that can, so it still reaches
    // a client. Either way one work request reports it and the rest flush.
    if (failedWqe == nullptr ||
        failedWqe->getCompletionStatusOverride().has_value()) {
      LOG_FIRST_N(WARNING, 10)
          << "fatal NAK for "
          << (nackedFrameIsResponse ? "the read response to initiator msn "
                                    : "msn ")
          << failedMsn
          << " which cannot report it on qp: " << qp_.getLocalQpNum()
          << " tag: " << (uint32_t)ack.getTag();
      abandonedCount += tag->markPendingWqesFailing(nakStatus);
    } else {
      abandonedCount += failedWqe->markFailing(nakStatus);
      abandonedCount += tag->markPendingWqesFailing(kWrFlushErr);
    }
    qp_.subtractUnsentPosns(abandonedCount);

    // No message will ever fill these now. The WQE-retirement guard above is
    // what serializes this against the other users of the bound-receive map.
    tag->flushPendingReceiveWqesInError();

    // A work request with no frame in flight is already fully accounted for and
    // has no ack coming to complete it.
    tag->completeSettledFailedWqes();
  }
  qp_.wakeSenderAfterTagError(tagErrorWake);
}

void TxFlow::abandonFramesOfFailingWqes(
    std::bitset<kMetaRoceTagCount>& tagsWithAbandonedFrames) {
  auto lockedSndCur = txState_.sndCur.wlock();
  for (auto& trackerEntry : txState_.tracker) {
    // A resolved entry's wqe pointer may already be freed.
    if (!trackerEntry.isResolved()) {
      const auto& trackerId = trackerEntry.id;
      // The states read below are written under the Tag.
      auto tagGuard = qp_.getTag(trackerId.tagNum)->scopedLock();
      Wqe& wqe = *CHECK_NOTNULL(trackerId.wqe);
      const bool wqeIsDoneWithTheFrame = wqe.isFailing() || wqe.hasBeenServed();
      if (wqe.isFrameAllocated(trackerId.posn) && wqeIsDoneWithTheFrame) {
        wqe.abandonFrame(trackerId.posn);
        // A later sack must not reach a work request this abandonment may
        // have retired.
        trackerEntry.state = FsnState::ABANDONED;
        tagsWithAbandonedFrames.set(trackerId.tagNum);
      }
    }
  }
}

void TxFlow::processAckPacket(const AckHeader& ack) {
  processAckPacket(ack, getNowNs());
}

void TxFlow::processAckPacket(const AckHeader& ack, uint32_t nowNs) {
  // RTT is measured at the ACK's processing time (nowNs).
  DCHECK(nowNs != ack.getTimestamp() || ack.getTimestamp() == 0)
      << "nowNs (" << nowNs << ") must be the ACK processing time, not the "
      << "echoed send timestamp -- equal-and-nonzero implies a zero-RTT caller "
      << "bug";
  if (disabled_) {
    VLOG(2) << "TxFlow::processAckPacket() flow " << flowId_
            << " is disabled, ignoring ACK";
    return;
  }

  uint16_t numNewlyLost = 0;
  uint16_t numPosnDeallocated = 0;
  uint16_t numRetiredAbandonedFrames = 0;
  std::vector<PacketId> newlyAckedPkts;
  VLOG(1) << "Received an ACK for qp: " << qp_.getLocalQpNum()
          << " flow: " << flowId_
          << " AckStatus: " << static_cast<int>(ack.getAckStatus())
          << " with cfsn " << ack.getCumulativeFsn()
          << " maxRcvdFsn: " << ack.getMaxRcvdFsn()
          << " cumulativeRespDeliveredMsn: " << ack.getCumulRespDeliveredMsn();

  if (ack.getAckStatus() == AckStatus::RESERVED) {
    LOG(WARNING) << "Received an ACK with reserved status";
    return;
  }

  if (ack.getAckStatus() == AckStatus::NAK_FLOW_ID_ERROR) {
    processNakFlowIdError();
    return;
  }

  if (ack.getAckStatus() == AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR ||
      ack.getAckStatus() == AckStatus::NAK_OPERATIONAL_ERROR ||
      ack.getAckStatus() == AckStatus::NAK_INVALID_REQUEST) {
    processNakFatalError(ack);
    return;
  }

  // Spec 8.5.5: only the in-order ack, the out-of-order ack and the BRNR carry
  // a cumulative cdmsn, and the Tag field is what scopes it.
  if (const AckStatus status = ack.getAckStatus(); status == AckStatus::ACK ||
      status == AckStatus::ACK_OUT_OF_ORDER || status == AckStatus::BRNR) {
    // A tag this QP does not have scopes no cdmsn, and the per-flow fsn
    // acknowledgment below still applies.
    if (const auto tag = qp_.getTag(ack.getTag()); tag != nullptr) {
      const uint16_t nextMsn = [&tag] {
        auto tagGuard = tag->scopedLock();
        return tag->getNextMsn();
      }();
      if (const uint16_t cdmsn = ack.getCumulRespDeliveredMsn();
          !before(cdmsn, nextMsn)) {
        LOG_FIRST_N(ERROR, 10)
            << "qp " << qp_.getLocalQpNum() << " tag "
            << static_cast<uint32_t>(ack.getTag())
            << " discarding an ack whose cdmsn " << cdmsn
            << " is at or after the next transmit msn " << nextMsn;
        return;
      }
    }
  }

  // With 256 packets per rtt, there should be 256 rtts before wrap of the 16bit
  // fsn.  any ack that arrives more than 64 rtts late should not be trusted.
  // Treat the rtt as srtt+mdev to be permissive for the first few acks, since
  // those are not excessively delayed.
  const auto rttNs = computeRttNs(ack.getTimestamp(), nowNs);
  if (ack.getTimestamp()) {
    const uint64_t maxRtoLimitNs = uint64_t{kDefaultMaxRtoNs} << 2;
    const uint64_t rttLimitNs =
        (uint64_t{txState_.estRttNs} + txState_.mdevRttNs) *
        kStaleAckRttMultiplier;
    // srtt and mdev hold configured initial values until the first RTT sample.
    const bool rttLimitApplies =
        txState_.haveRttSample && rttLimitNs < maxRtoLimitNs;
    const uint64_t ageLimitNs = rttLimitApplies ? rttLimitNs : maxRtoLimitNs;
    if (rttNs > ageLimitNs) {
      if (!FLAGS_libsoftmetaroce_slow_receiver) {
        const std::string limit = rttLimitApplies
            ? fmt::format(
                  "{} * (srtt {}us + mdev {}us)",
                  kStaleAckRttMultiplier,
                  txState_.estRttNs / 1000,
                  txState_.mdevRttNs / 1000)
            : "4 * max rto";
        LOG(WARNING) << "Discarding " << rttNs / 1000 << "us old ack of cfsn "
                     << ack.getCumulativeFsn() << ", over the " << limit
                     << " limit of " << ageLimitNs / 1000 << "us"
                     << (txState_.haveRttSample ? "" : " before any rtt sample")
                     << ", to protect against wrapped sequence.";
        return;
      } else {
        LOG_FIRST_N(WARNING, 1)
            << "Inhibiting stale ack discard rule with rtt of " << rttNs / 1000
            << "us";
      }
    }
  }

  if (before(ack.getMaxRcvdFsn(), txState_.sndUna)) {
    // The ack is redundant: will not advance sndUna and will not
    // tell us about packets newly received. (It tells us
    // that a stale packet left the network and this could be an
    // indication to wake up and consider sending, but that is
    // not here.)
    if (before(ack.getMaxRcvdFsn(), (uint16_t)(txState_.sndUna - 1))) {
      // an ack that is particularly stale is a sign of reordering
      // of acks; this ack has worse information than we already have.
      LOG_FIRST_N(WARNING, 10) << fmt::format(
          "discarding stale ack on flow {} because max {} is before {} "
          "while cfsn is {} and ts={}. Ack reordering is likely.",
          flowId_,
          ack.getMaxRcvdFsn(),
          txState_.sndUna,
          ack.getCumulativeFsn(),
          ack.getTimestamp());
    }
    // else, an ack that is just before snd una may be a repeated
    // ack for a spurious retransmission.  no need to log, no need
    // to process further..
    return;
  }

  if (!before(ack.getCumulativeFsn(), txState_.sndNxt)) {
    LOG(WARNING) << "discarding impossible ack";
    return;
  }

  if (before(ack.getMaxRcvdFsn(), ack.getCumulativeFsn())) {
    LOG(WARNING) << "discarding invalid ack";
    return;
  }

  // Discard malicious or buggy acks where max is impossibly large
  // (receiver claims to have received packets we haven't sent yet)
  if (!before(ack.getMaxRcvdFsn(), txState_.sndNxt)) {
    LOG_FIRST_N(ERROR, 5)
        << "POTENTIAL MALFUNCTION! Discarding malicious packet. Receiver is claiming to have received "
        << ack.getMaxRcvdFsn() << " on flow " << flowId_
        << " even though snd nxt is " << txState_.sndNxt;
    return;
  }

  // A frame cannot simultaneously have been received and rejected for RNR.
  // Reject the whole malformed ACK before applying its cumulative field or
  // changing any tracker/WQE state; choosing either bit after partial ACK
  // processing can strand the replacement behind an inconsistent window.
  uint16_t ackVectorBits =
      static_cast<uint16_t>(ack.getMaxRcvdFsn() - ack.getCumulativeFsn());
  if (ackVectorBits > AckHeader::kAckTrackerBits) {
    ackVectorBits = AckHeader::kAckTrackerBits;
  }
  for (uint16_t idx = 0; idx < ackVectorBits; ++idx) {
    if (ack.getFsnBitVector().getBit(idx) &&
        ack.getRnrBitVector().getBit(idx)) {
      LOG_FIRST_N(ERROR, 1)
          << "Discarding ack that set both BRNR and SACK for one frame.";
      return;
    }
  }

  // An HRNR's fsn fields both carry the soliciting packet's fsn, so the
  // staleness guards above apply to it too.
  if (ack.isHrnr()) {
    processHrnrPacket(ack);
    return;
  }

  // Highest acked MSN and lowest BRNR'd MSN per tag, keyed on the tag of each
  // frame recovered from the tracker. The ACK's Tag field only scopes the
  // CDMSN, so its FSN acknowledgment is applied even when that tag is unknown.
  std::unordered_map<uint8_t, uint16_t> highestAckedMsnByTag;
  std::unordered_map<uint8_t, uint16_t> minBrnrMsnByTag;
  auto recordHighestAckedMsn = [&highestAckedMsnByTag](const PacketId& id) {
    const uint16_t msn = CHECK_NOTNULL(id.wqe)->getMsn().value();
    auto it = highestAckedMsnByTag.find(id.tagNum);
    if (it == highestAckedMsnByTag.end()) {
      highestAckedMsnByTag.emplace(id.tagNum, msn);
    } else if (before(it->second, msn)) {
      it->second = msn;
    }
  };
  bool shouldSackRetransmit = false;
  // Tags whose frames are abandoned below, to complete outside the lock.
  std::bitset<kMetaRoceTagCount> tagsWithAbandonedFrames;
  {
    auto lockedSndCur = txState_.sndCur.wlock();
    // an fsn has been cumulatively acked relative to what we know if
    // cumulativeFsn >= una.
    if (!before((unsigned short)(ack.getCumulativeFsn()), txState_.sndUna)) {
      // Find cumulatively ACKed packets
      uint16_t cumulativelyAcked = ack.getCumulativeFsn() + 1 - txState_.sndUna;
      CHECK_GE(txState_.tracker.size(), cumulativelyAcked)
          << "ack cfsn: " << ack.getCumulativeFsn()
          << " una: " << txState_.sndUna;
      for (uint16_t i = 0; i < cumulativelyAcked; ++i) {
        // Iterate over cumulatively acked FSNs
        const auto& frontEntry = txState_.tracker.front();
        if (frontEntry.isUnacked()) {
          // Newly acked FSN
          newlyAckedPkts.push_back(frontEntry.id);
          recordHighestAckedMsn(frontEntry.id);
        } else if (frontEntry.isAbandoned()) {
          numRetiredAbandonedFrames++;
        }
        // A BRNR'd or abandoned frame is resolved already. Its WQE may have
        // completed and been freed, so it must not be reached for through the
        // tracker, and acknowledging its posn again would double-resolve it.

        // No need to keep track of this FSN anymore
        txState_.tracker.pop_front();
      }

      txState_.sndUna = ack.getCumulativeFsn() + 1;
    }

    consecutiveRtosWithoutAck_ = 0;
    lastConsumedRtoCount_ = 0;

    // With large windows or during retransmission, the maxRcvdFsn may
    // be more than 256 fsn's past lastContiguousFsn. Count this case
    // and only consider the first (valid) 256 bits.
    const uint16_t bitsInSack = ackVectorBits;

    // Set to true if current and all previous FSNs are either
    // acked or RNR'd.  This is used to determine if we can
    // advance sndUna
    bool stillContiguous = true;
    // Additional FSNs that sndUna can be advanced to
    uint16_t advanceSndUnaBy = 0;

    // Update tracker and find newly SACKed packets
    for (uint16_t idxInPacket = txState_.sndUna - ack.getCumulativeFsn() - 1,
                  idxInTracker = 0;
         idxInPacket < bitsInSack;
         ++idxInPacket, ++idxInTracker) {
      uint16_t fsn = ack.getCumulativeFsn() + 1 + idxInPacket;
      bool isFrameSacked = ack.getFsnBitVector().getBit(idxInPacket);
      bool isFrameBrnr = ack.getRnrBitVector().getBit(idxInPacket);
      CHECK_LT(idxInTracker, txState_.tracker.size())
          << "would out_of_range error looking at tx tracker where "
          << idxInTracker << "is too large for flow " << flowId_;
      auto& trackerEntry = txState_.tracker[idxInTracker];
      const auto& trackerId = trackerEntry.id;
      // A resolved frame's wqe pointer may already be freed, and a late sack
      // must not resolve it twice.
      bool isAlreadyResolved = trackerEntry.isResolved();
      bool unacked = !isFrameSacked && !isAlreadyResolved;

      if (isFrameBrnr && !isAlreadyResolved) {
        // This FSN is exlpicitly BRNR'd
        // The work request's failing state, and the frame status it decides,
        // are written under the Tag, so read and act on them under it.
        auto brnrTag = qp_.getTag(trackerId.tagNum);
        auto tagGuard = brnrTag->scopedLock();
        Wqe& wqe = *CHECK_NOTNULL(trackerId.wqe);
        const bool frameIsAllocated = wqe.isFrameAllocated(trackerId.posn);
        const bool served = frameIsAllocated && wqe.hasBeenServed();
        if (served) {
          LOG_FIRST_N(ERROR, 10)
              << "qp " << qp_.getLocalQpNum() << " received a BRNR on flow "
              << flowId_ << " for a read or atomic the peer had already "
              << "served";
        }
        if (frameIsAllocated && (wqe.isFailing() || served)) {
          // Nothing to gain by waiting out the RNR: the message is failing,
          // or the response is already here.
          wqe.abandonFrame(trackerId.posn);
          trackerEntry.state = FsnState::ABANDONED;
          tagsWithAbandonedFrames.set(trackerId.tagNum);
        } else {
          if (frameIsAllocated) {
            // If this frame has not been deallocated, do so
            wqe.releaseFrame(trackerId.posn);
            qp_.tagSet_.addUnsentPosns(1);
            numPosnDeallocated++;
          }
          CHECK(wqe.getMsn().has_value())
              << "WQE in tracker does not have an MSN set.";
          uint16_t msn = wqe.getMsn().value();
          const bool coveredByCancel = wqe.getCsn().has_value() &&
              brnrTag->brnr().cancelCovers(
                  wqe.getCsn().value(), nowNs, qp_.getEstQpRttNs());
          if (!coveredByCancel) {
            auto [it, inserted] =
                minBrnrMsnByTag.try_emplace(trackerId.tagNum, msn);
            if (inserted) {
              LOG_FIRST_N(INFO, 1000)
                  << "Received a BRNR for Tag: " << (uint32_t)trackerId.tagNum
                  << " msn: " << msn;
            } else if (before(msn, it->second)) {
              // Keep the lowest BRNR'd MSN seen for this tag.
              it->second = msn;
            }
          }
          trackerEntry.state = FsnState::BRNR;
        }
      } else if (isFrameSacked && !isAlreadyResolved) {
        // First time this FSN is ACKed. The state write below is what stops a
        // later ack appending it again, so an fsn enters newlyAckedPkts once.
        newlyAckedPkts.push_back(trackerId);
        trackerEntry.state = FsnState::ACKED;
        recordHighestAckedMsn(trackerId);
      } else if (unacked && after(fsn, txState_.maxAckdFsn)) {
        // If we have a hole after txState_.maxAckdFsn, it's a newly detected
        // loss
        numNewlyLost++;
      }
      if (stillContiguous) {
        if (isFrameSacked || isFrameBrnr || trackerEntry.isAbandoned()) {
          // If we have a contiguous ack/rnr sequence, we can advance sndUna
          // (this is because we've already acked all the packets up to
          // sndUna, so we can advance sndUna to the first FSN after the
          // contiguous ack/rnr sequence)
          advanceSndUnaBy++;
        } else {
          // After the first FSN that is not acked/rnr'd, we can't advance
          // sndUna any further
          stillContiguous = false;
        }
      }
    }

    txState_.sndUna += advanceSndUnaBy;
    for (int i = 0; i < advanceSndUnaBy; ++i) {
      if (txState_.tracker.front().isAbandoned()) {
        // An abandoned frame leaves the window at retirement, which is here,
        // where the tracker drops it.
        numRetiredAbandonedFrames++;
      }
      // No need to keep track of this FSN anymore
      txState_.tracker.pop_front();
    }
    *lockedSndCur =
        after(txState_.sndUna, *lockedSndCur) ? txState_.sndUna : *lockedSndCur;
    numRetiredAbandonedFrames +=
        retireResolvedFramesAtWindowHead(*lockedSndCur);

    txState_.maxAckdFsn = after(ack.getMaxRcvdFsn(), txState_.maxAckdFsn)
        ? ack.getMaxRcvdFsn()
        : txState_.maxAckdFsn;

    bool hasSackHoles = after(txState_.maxAckdFsn, *lockedSndCur);
    shouldSackRetransmit =
        hasSackHoles && isAllowedToFastRetransmit(*lockedSndCur);
  } // end scope holding sndCur lock

  qp_.retireTagsWithAbandonedFrames(tagsWithAbandonedFrames);

  // processBrnrPacket takes the tag lock, so call it outside the sndCur block.
  if (ack.isBrnr()) {
    processBrnrPacket(ack, minBrnrMsnByTag);
  }

  // Capture QP-level smoothed RTT before updates
  const auto smoothedQpRttNsBeforeUpdate = getQp().getEstQpRttNs();
  updateRtt(ack.getTimestamp(), nowNs);
  if (ack.getTimestamp() != 0) {
    getQp().updateQpRtt(rttNs);
    VLOG(1) << "packet_rtt qp=" << qp_.getLocalQpNum() << " flow_id=" << flowId_
            << " socket_id=" << socketId_ << " cfsn=" << ack.getCumulativeFsn()
            << " max_rcvd_fsn=" << ack.getMaxRcvdFsn() << " rtt_ns=" << rttNs
            << " send_ts_ns=" << ack.getTimestamp() << " now_ns=" << nowNs;
  }
  CongestionController& cc =
      *CHECK_NOTNULL(getQp().congestionController_.get());

  if (numNewlyLost > 0) {
    LOG_FIRST_N(INFO, 50) << "Received an ack on flow " << flowId_
                          << " with numNewlyLost/Acked/RNRd: " << numNewlyLost
                          << "/" << newlyAckedPkts.size() << "/"
                          << numPosnDeallocated << " with una..max "
                          << txState_.sndUna << ".." << txState_.maxAckdFsn
                          << " rtt: " << rttNs / 1000 << "us"
                          << " fwnd: " << cc.getFwnd(flowId_);
  }
  if (numPosnDeallocated > 0) {
    LOG_FIRST_N(INFO, 50) << "Received an ack on qp: "
                          << getQp().getLocalQpNum()
                          << " with BRNR and deallocated: "
                          << numPosnDeallocated << " posns across "
                          << minBrnrMsnByTag.size() << " tag(s)";
  }
  if (shouldSackRetransmit || numPosnDeallocated > 0) {
    auto retxCount = prepareRetxPacketsIfAny(false);
    if (retxCount > 0) {
      cc.onPacketLoss(
          flowId_,
          CongestionController::LossEvent{
              .numLostPackets = retxCount,
              .lossType =
                  CongestionController::LossEvent::LossType::OutOfOrder});
    }
  }

  // An abandoned frame leaves the window exactly as a BRNR'd one does, and
  // that already counts here: the sender waits for neither.
  uint16_t numFramesReturnedToCc =
      newlyAckedPkts.size() + numPosnDeallocated + numRetiredAbandonedFrames;
  if (numFramesReturnedToCc > 0) {
    cc.onPacketAck(
        flowId_,
        CongestionController::AckEvent{
            .numAckedPackets = numFramesReturnedToCc,
            .numAckedPacketsWithCE = ack.getCnpCE(),
            .measuredRttNs = rttNs,
            .smoothedQpRttNs = smoothedQpRttNsBeforeUpdate,
            .rcnDesiredRateGbps = ack.getRateHints(),
            .sendPending = qp_.getUnsentPosnCount() > 0,
        });
  }

  // A BRNR advances sndUna without any newly-acked frames, so the clears below
  // must still run.
  if (!newlyAckedPkts.empty()) {
    getQp().checkForTxMessageCompletion(newlyAckedPkts);
    if (getQp().hasPendingWqes()) {
      qp_.signalReadyAsync();
    }
  }
  // Exit RNR backoff now that the tracker reflects the ack, so a freshly-acked
  // packet is not re-queued for retransmission as we cancel. HRNR is whole-QP:
  // any ack proves the receiver is responsive, so clear it once.
  qp_.cancelHrnrBackoff();
  bool resumedAnyTag = false;
  // BRNR is per-tag and independent of HRNR: clear a tag's block only once an
  // acked outbound MSN reaches that tag's own first-blocked MSN.
  for (const auto& [tagNum, ackedMsn] : highestAckedMsnByTag) {
    // A BRNR for this tag takes precedence over accepted sibling fragments
    // carried by the same ACK; it does not prove the refused buffer is ready.
    if (minBrnrMsnByTag.contains(tagNum)) {
      continue;
    }
    // Tracker entries identify Tags retained until whole-QP teardown.
    const auto tagHandle = qp_.getTag(tagNum);
    Tag& tag = *CHECK_NOTNULL(tagHandle.get());
    // Checked and cleared under one hold: the BRNR timer moves this backoff
    // under the same Tag.
    auto tagGuard = tag.scopedLock();
    if (isBlockedMsn(tag, ackedMsn)) {
      tag.brnr().resume();
      resumedAnyTag = true;
    }
  }
  if (resumedAnyTag) {
    qp_.signalReadyAsync();
  }
}

uint16_t TxFlow::retireResolvedFramesAtWindowHead(uint16_t& sndCur) {
  uint16_t retiredCount = 0;
  uint16_t abandonedCount = 0;
  while (!txState_.tracker.empty() && txState_.tracker.front().isResolved()) {
    if (txState_.tracker.front().isAbandoned()) {
      abandonedCount++;
    }
    txState_.tracker.pop_front();
    retiredCount++;
  }
  if (retiredCount == 0) {
    return abandonedCount;
  }

  txState_.sndUna += retiredCount;
  sndCur = after(txState_.sndUna, sndCur) ? txState_.sndUna : sndCur;
  VLOG(1) << "Retired " << retiredCount
          << " resolved frame(s) on qp: " << qp_.getLocalQpNum()
          << " flow: " << flowId_ << " advancing una to " << txState_.sndUna;
  return abandonedCount;
}

uint16_t TxFlow::prepareRetxPacketsIfAny(bool isRtoEvent) {
  if (txState_.sndNxt == txState_.sndUna) {
    return 0;
  }
  if (isRtoEvent && !retxPktsToSend_.rlock()->empty()) {
    // An RTO resets sndCur to sndUna, so scanning again would queue the whole
    // window a second time. Signalled all the same: EAGAIN leaves the QP out
    // of the ready set expecting the RTO to put it back, and this firing is
    // the fallback that does not depend on an ack arriving to do it.
    qp_.signalReadyAsync();
    return 0;
  }
  uint16_t retxCount = 0;
  uint16_t numRetiredAbandonedFrames = 0;
  uint32_t retxRateHints = computeRateHints(
      qp_.congestionController_ ? qp_.congestionController_->getQwnd() : 0);
  // Tags whose frames are abandoned below, to complete outside the lock.
  std::bitset<kMetaRoceTagCount> tagsWithAbandonedFrames;
  {
    auto lockedSndCur = txState_.sndCur.wlock();
    if (isRtoEvent) {
      // reset sndCur to sndUna so that the unacked packets that were
      // retransmitted because of out-of-order can be retransmitted again.
      *lockedSndCur = txState_.sndUna;
    }

    // In case of RTO, all packets we've sent (and haven't been acked) may be
    // lost. For SACK retransmission with windowed modes, limit the cursor to
    // base on mode, retransmitting only packets outside the
    // reordering tolerance window.
    const auto cursorLimit =
        isRtoEvent ? txState_.sndNxt : sackRetxCursorLimit();
    for (; before(*lockedSndCur, cursorLimit); (*lockedSndCur)++) {
      // If sndCur is before cursorLimit and it is not acked, it can be
      // assumed lost.
      uint16_t offsetInTracker = *lockedSndCur - txState_.sndUna;
      DCHECK_LT(offsetInTracker, kMaxTrackerSize);
      auto& ts = txState_.tracker.at(offsetInTracker);
      // A frame in the tracker was built from a Tag of this QP, and a Tag is
      // never taken out of the set while the QP lives, so this resolves.
      auto tag = qp_.getTag(ts.id.tagNum);
      auto tagGuard = tag->scopedLock();

      // A tag marked for deletion keeps retransmitting its outstanding frames
      // so they complete; it is not removed until the whole QP is torn down.
      //
      // Tested before any use of ts.id.wqe, which may already have completed
      // and been freed once its frame is resolved.
      if (ts.isResolved()) {
        continue;
      }

      // Unacknowledged, so this frame is unresolved and its WQE still alive.
      // Only the frame: a read's response posns have state of their own.
      Wqe& retxWqe = *CHECK_NOTNULL(ts.id.wqe);
      if (tag->mustStopRetransmitting(retxWqe)) {
        VLOG(1) << "Abandoning frame qp: " << qp_.getLocalQpNum()
                << " fsn: " << *lockedSndCur
                << " Tag: " << (uint32_t)ts.id.tagNum
                << " msn: " << retxWqe.getMsn().value_or(-1)
                << " posn: " << ts.id.posn << " flow: " << flowId_;
        retxWqe.abandonFrame(ts.id.posn);
        ts.state = FsnState::ABANDONED;
        tagsWithAbandonedFrames.set(ts.id.tagNum);
        continue;
      }

      // A blocked msn still retransmits: either answer retires the frame.
      if (isBlockedMsn(*tag, retxWqe.getMsn().value())) {
        // Sending it makes it a probe whatever the backoff was doing: a
        // verdict on this msn is now outstanding.
        tag->brnr().probeSent();
      }
      VLOG(1) << "Prepare retx packet qp: " << qp_.getLocalQpNum()
              << " fsn: " << *lockedSndCur << " Tag: " << (uint32_t)ts.id.tagNum
              << " msn: " << retxWqe.getMsn().value_or(-1)
              << " posn: " << ts.id.posn
              << " csn: " << retxWqe.getCsn().value_or(-1)
              << " flow: " << flowId_ << " isRTO: " << isRtoEvent
              << " HRNR state: " << qp_.getHrnrState()
              << " BRNR state: " << tag->getBrnrState();

      ++retxCount;
      retxPktsToSend_.wlock()->emplace_back(qp_.buildDataPacket(
          flowId_,
          qp_.getLocalQpNum(),
          qp_.getRemoteQpNum(),
          ts.id.tagNum,
          *lockedSndCur,
          txState_.sndUna,
          retxWqe,
          ts.id.posn,
          retxWqe.makeSendPayloadView(
              ts.id.posn, qp_.getMtu(), ts.id.sizeBytes),
          /*isRetransmit=*/true,
          /*isTimeout=*/isRtoEvent,
          retxRateHints));
      if (isRtoEvent && qp_.getHrnrState() == HrnrState::HRNR_PROBING &&
          retxPktsToSend_.rlock()->size() > 0) {
        // In HRNR_PROBING, queue only a single packet as the probe and wait for
        // the next backoff or a sign of life (ACK/RNR-cancel).
        VLOG(2) << "Queueing a single probe packet after RTO";
        break;
      }
    }
    numRetiredAbandonedFrames = retireResolvedFramesAtWindowHead(*lockedSndCur);
  }
  if (numRetiredAbandonedFrames > 0 && qp_.congestionController_) {
    // Not acknowledged, but they leave the window the same way, and the acked
    // count is what returns it. No reply arrived, so there is no sample.
    qp_.congestionController_->onPacketAck(
        flowId_,
        CongestionController::AckEvent{
            .numAckedPackets = numRetiredAbandonedFrames,
            .numAckedPacketsWithCE = 0,
            .measuredRttNs = 0,
            .smoothedQpRttNs = qp_.getEstQpRttNs()});
  }

  qp_.retireTagsWithAbandonedFrames(tagsWithAbandonedFrames);

  qp_.signalReadyAsync();
  return retxCount;
}

std::optional<SackRetransmissionMode> parseSackRetransmissionMode(
    std::string_view mode) {
  if (mode == "immediate") {
    return SackRetransmissionMode::IMMEDIATE;
  } else if (mode == "never") {
    return SackRetransmissionMode::NEVER;
  } else if (mode == "count_based_pwnd") {
    return SackRetransmissionMode::COUNT_BASED_PWND;
  } else if (mode == "distance_based_pwnd") {
    return SackRetransmissionMode::DISTANCE_BASED_PWND;
  }
  return std::nullopt;
}

std::string_view toStringView(SackRetransmissionMode mode) {
  switch (mode) {
    case SackRetransmissionMode::IMMEDIATE:
      return "immediate";
    case SackRetransmissionMode::NEVER:
      return "never";
    case SackRetransmissionMode::COUNT_BASED_PWND:
      return "count_based_pwnd";
    case SackRetransmissionMode::DISTANCE_BASED_PWND:
      return "distance_based_pwnd";
  }
  // unreachable; satisfies -Wreturn-type
  LOG(FATAL) << "unhandled SackRetransmissionMode: " << static_cast<int>(mode);
}

uint16_t TxFlow::countSackedPackets(uint16_t sndCur) const {
  uint16_t startOffset = sndCur + 1 - txState_.sndUna;
  CHECK_LE(startOffset, txState_.tracker.size());
  return static_cast<uint16_t>(std::count_if(
      std::next(txState_.tracker.begin(), startOffset),
      txState_.tracker.end(),
      [](const auto& t) { return t.isAcked(); }));
}

bool TxFlow::isAllowedToFastRetransmit(uint16_t sndCur) const {
  switch (sackRetxConfig_.mode) {
    case SackRetransmissionMode::IMMEDIATE:
      return true;
    case SackRetransmissionMode::NEVER:
      return false;
    case SackRetransmissionMode::COUNT_BASED_PWND: {
      auto fwnd = getQp().congestionController_->getFwnd(flowId_);
      return countSackedPackets(sndCur) > fwnd * sackRetxConfig_.fraction;
    }
    case SackRetransmissionMode::DISTANCE_BASED_PWND: {
      auto fwnd = getQp().congestionController_->getFwnd(flowId_);
      return static_cast<uint16_t>(txState_.maxAckdFsn - sndCur) >
          fwnd * sackRetxConfig_.fraction;
    }
  }
  // unreachable; satisfies -Wreturn-type
  LOG(FATAL) << "unhandled SackRetransmissionMode: "
             << static_cast<int>(sackRetxConfig_.mode);
}

uint16_t TxFlow::sackRetxCursorLimit() const {
  auto fwnd = getQp().congestionController_->getFwnd(flowId_);
  auto window = static_cast<uint16_t>(fwnd * sackRetxConfig_.fraction);
  LOG_FIRST_N(INFO, 50) << "sackRetxCursorLimit flowId=" << flowId_
                        << " fwnd=" << fwnd << " window=" << window
                        << " maxAckdFsn=" << txState_.maxAckdFsn
                        << " sndUna=" << txState_.sndUna;

  switch (sackRetxConfig_.mode) {
    case SackRetransmissionMode::IMMEDIATE:
      return txState_.maxAckdFsn;
    case SackRetransmissionMode::NEVER:
      return txState_.sndUna;
    case SackRetransmissionMode::DISTANCE_BASED_PWND: {
      auto limit = static_cast<uint16_t>(txState_.maxAckdFsn - window + 1);
      return before(limit, txState_.sndUna) ? txState_.sndUna : limit;
    }
    case SackRetransmissionMode::COUNT_BASED_PWND: {
      // Walk backwards from maxAckdFsn, skip `window` ACKED entries.
      // Everything before the cursor is clearly lost (enough SACKed evidence);
      // everything at/after the cursor is within the tolerance window.
      uint16_t ackedSeen = 0;
      // Equivalent to NEVER: cursor at sndUna means no retransmission.
      uint16_t cursorLimit = txState_.sndUna;
      uint16_t endOffset = txState_.maxAckdFsn - txState_.sndUna;
      for (uint16_t i = endOffset; i > 0; --i) {
        if (txState_.tracker.at(i).isAcked()) {
          if (++ackedSeen >= window) {
            cursorLimit = static_cast<uint16_t>(txState_.sndUna + i + 1);
            break;
          }
        }
      }
      return cursorLimit;
    }
  }
  // unreachable; satisfies -Wreturn-type
  LOG(FATAL) << "unhandled SackRetransmissionMode: "
             << static_cast<int>(sackRetxConfig_.mode);
}

[[nodiscard]] uint32_t TxFlow::computeRttNs(
    uint32_t timestampNs,
    uint32_t nowNs) const {
  return nowNs - timestampNs;
}

void TxFlow::updateRtt(uint32_t timestampNs, uint32_t nowNs) {
  if (timestampNs == 0) {
    LOG(ERROR) << "Timestamp is not set in the received ack";
    return;
  }
  const auto rttNs = computeRttNs(timestampNs, nowNs);
  if (!txState_.haveRttSample) {
    txState_.haveRttSample = true;
    if (FLAGS_libsoftmetaroce_rfc6298_first_rtt_sample) {
      txState_.estRttNs = rttNs;
      txState_.mdevRttNs = rttNs / 2;
      return;
    }
  }
  const auto absoluteError = std::abs((int)(rttNs - txState_.estRttNs));
  txState_.estRttNs -= txState_.estRttNs >> kDefaultInverseLog2FlowAlpha;
  txState_.estRttNs += rttNs >> kDefaultInverseLog2FlowAlpha;

  txState_.mdevRttNs -= txState_.mdevRttNs >> kDefaultInverseLog2FlowRttBeta;
  txState_.mdevRttNs += absoluteError >> kDefaultInverseLog2FlowRttBeta;
}

[[nodiscard]] bool TxFlow::isBlockedMsn(const Tag& tag, uint16_t msn) const {
  return tag.brnr().state() != BrnrState::NORMAL_TX &&
      tag.brnr().isBlockedMsn(msn);
}

[[nodiscard]] uint32_t TxFlow::computeRateHints(uint32_t qwnd) const {
  if (qp_.congestionController_ && qp_.tagSet_.getUnsentPosnCount() < qwnd) {
    return 0;
  }
  // Only the sign is used: updateRcnActiveState activates RCN on a nonzero hint
  // and deactivates on zero, ignoring the magnitude -- so a fixed value
  // suffices.
  return kDefaultRateHintGbps;
}

void TxFlow::disableAndNotifyCongestionControl() {
  disabled_ = true;
  if (qp_.congestionController_) {
    qp_.congestionController_->onFlowDisabled(flowId_);
  }
}

// The caller serializes this helper with ACK resets, already holding
// Qp::txStateLock_ when ACKs are applied inline.
void TxFlow::applyRtoRepath() {
  const uint16_t rtoCount = consecutiveRtosWithoutAck_.load();
  if (rtoCount == lastConsumedRtoCount_) {
    return;
  }
  lastConsumedRtoCount_ = rtoCount;
  const auto threshold = FLAGS_libsoftmetaroce_rtos_before_repath;
  if (threshold == kRtxRepathDisabledSentinel || rtoCount < threshold) {
    return;
  }

  const auto oldSocketId = socketId_;
  try {
    socketId_ = qp_.getSender().getAlternateSocket(socketId_);
  } catch (const std::exception& ex) {
    // A failed source-port change does not prevent moving to another
    // configured pair.
    LOG_FIRST_N(WARNING, 10)
        << "QP " << qp_.getLocalQpNum() << " flow " << flowId_
        << ": could not change RTO source-port entropy: " << ex.what();
  }
  VLOG(2) << "QP " << qp_.getLocalQpNum() << " flow " << flowId_ << ": RTO "
          << rtoCount << " socket " << oldSocketId << " -> " << socketId_;

  if (rtoCount > threshold) {
    tryMoveToNextPortPair();
  }
}

void TxFlow::tryMoveToNextPortPair() {
  const auto numPairs = qp_.portPairs_.size();
  // Every live flow retains at least one configured port pair.
  CHECK_GT(numPairs, 0);
  if (numPairs == 1) {
    return;
  }
  const auto oldPairIdx = portPairIdx_;
  const auto newPairIdx = static_cast<uint16_t>((oldPairIdx + 1) % numPairs);
  const auto& oldPair = qp_.portPairs_.at(oldPairIdx);
  const auto& newPair = qp_.portPairs_.at(newPairIdx);
  if (newPair.localPortIdx != oldPair.localPortIdx) {
    const auto& newLocalPort = qp_.localPorts_.at(newPair.localPortIdx);
    try {
      socketId_ = qp_.getSender().getSocketIdFromAddress(
          newLocalPort.addr, newLocalPort.gid);
    } catch (const std::exception& ex) {
      LOG_FIRST_N(WARNING, 10)
          << "QP " << qp_.getLocalQpNum() << " flow " << flowId_
          << ": could not acquire a socket for RTO pair " << newPairIdx << ": "
          << ex.what();
      return;
    }
  }
  portPairIdx_ = newPairIdx;
  LOG(INFO) << "QP " << qp_.getLocalQpNum() << " flow " << flowId_
            << ": RTO move from pair " << oldPairIdx << " (local "
            << oldPair.localPortIdx << ", remote " << oldPair.remotePortIdx
            << ") to " << newPairIdx << " (local " << newPair.localPortIdx
            << ", remote " << newPair.remotePortIdx << ")";
}

uint16_t TxFlow::findAlternatePortPair(
    uint16_t activePortSet,
    bool sameLocalOnly) const {
  const auto pairIdx = portPairIdx_;
  const auto& pair = qp_.portPairs_.at(pairIdx);
  const auto numPairs = qp_.portPairs_.size();
  // Stagger the scan start by QP and flow so failover traffic spreads across
  // active alternatives instead of all flows picking the same first pair.
  const auto start =
      (pairIdx + qp_.getPortPairRotation() + flowId_ + 1) % numPairs;
  for (size_t offset = 0; offset < numPairs; offset++) {
    const auto candidateIdx =
        static_cast<uint16_t>((start + offset) % numPairs);
    if (candidateIdx == pairIdx) {
      continue;
    }
    const auto& candidate = qp_.portPairs_.at(candidateIdx);
    if (sameLocalOnly && candidate.localPortIdx != pair.localPortIdx) {
      continue;
    }
    if (qp_.isRemotePortActive(candidate.remotePortIdx, activePortSet)) {
      return candidateIdx;
    }
  }
  return std::numeric_limits<uint16_t>::max();
}

void TxFlow::maybeFailoverRemotePort() {
  const auto aps = qp_.getRemoteActivePortSet();
  if (aps == 0) {
    return;
  }
  const auto pairIdx = portPairIdx_;
  const auto& pair = qp_.portPairs_.at(pairIdx);
  const auto remotePortIdx = pair.remotePortIdx;
  const bool portIsUp = qp_.isRemotePortActive(remotePortIdx, aps);

  const auto& originalPair = qp_.portPairs_.at(originalPortPairIdx_);
  // RTO-selected pairs take precedence over automatic restoration until an
  // accepted ACK.
  if (consecutiveRtosWithoutAck_ == 0 && pairIdx != originalPortPairIdx_ &&
      qp_.isRemotePortActive(originalPair.remotePortIdx, aps)) {
    portPairIdx_ = originalPortPairIdx_;
    socketId_ = originalSocketId_;
    LOG(INFO) << "QP " << qp_.getLocalQpNum() << " flow " << flowId_
              << ": restored to original pair " << originalPortPairIdx_
              << " (local " << originalPair.localPortIdx << ", remote "
              << originalPair.remotePortIdx << ")";
    return;
  }

  if (portIsUp) {
    return;
  }

  // Choose another configured pair only. Prefer preserving the current local
  // port; if that has no active remote, allow failover to another configured
  // local/remote pair and update the bound socket.
  auto newPairIdx = findAlternatePortPair(aps, /*sameLocalOnly=*/true);
  if (newPairIdx == std::numeric_limits<uint16_t>::max()) {
    newPairIdx = findAlternatePortPair(aps, /*sameLocalOnly=*/false);
  }
  if (newPairIdx == std::numeric_limits<uint16_t>::max()) {
    return;
  }

  const auto& newPair = qp_.portPairs_.at(newPairIdx);
  if (newPair.localPortIdx != pair.localPortIdx) {
    const auto& newLocalPort = qp_.localPorts_.at(newPair.localPortIdx);
    try {
      socketId_ = qp_.getSender().getSocketIdFromAddress(
          newLocalPort.addr, newLocalPort.gid);
    } catch (const std::exception& ex) {
      VLOG(2) << "QP " << qp_.getLocalQpNum() << " flow " << flowId_
              << ": unable to acquire socket for APS failover to pair "
              << newPairIdx << ": " << ex.what();
      return;
    }
  }
  portPairIdx_ = newPairIdx;
  LOG(INFO) << "QP " << qp_.getLocalQpNum() << " flow " << flowId_
            << ": failover from pair " << pairIdx << " (local "
            << pair.localPortIdx << ", remote " << remotePortIdx << ") to "
            << newPairIdx << " (local " << newPair.localPortIdx << ", remote "
            << newPair.remotePortIdx << ")";
}

} // namespace libsoftmetaroce
