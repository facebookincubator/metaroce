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

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <glog/logging.h>
#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/PacketModifierSocket.h>
#include <libsoftmetaroce/ProtocolCompatibility.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/RnrState.h>
#include <libsoftmetaroce/RxFlow.h>
#include <libsoftmetaroce/Sender.h>
#include <libsoftmetaroce/TxFlow.h>
#include <algorithm>
#include <bitset>
#include <cstring>
#include <stdexcept>

DEFINE_uint32(
    libsoftmetaroce_min_rto_us,
    10000,
    "Prevent retransmissions by timeout sooner than this interval");

DEFINE_uint32(
    libsoftmetaroce_max_rto_us,
    200000,
    "Maximum retransmission timeout interval in microseconds.");

DEFINE_bool(
    libsoftmetaroce_single_owner_tx,
    true,
    "Make the sender the sole owner of transmit state: the receiver hands "
    "received ACKs to the sender over a per-QP ring instead of applying them "
    "inline, and the sender applies them at the QP's arbiter visit.");

DEFINE_bool(
    libsoftmetaroce_lockfree_signal,
    true,
    "Signal QP readiness via a lock-free intake ring drained by the arbiter, "
    "instead of taking the arbiter mutex (and a coroutine fallback) per signal.");

DEFINE_uint32(
    libsoftmetaroce_qp_teardown_log_limit,
    10,
    "Max number of per-QP diagnostic summary lines to log at INFO on QP "
    "destruction, across the process (0 = unlimited). Once the limit is "
    "reached, a single line notes the suppression; raise this flag to log "
    "more.");

namespace libsoftmetaroce {

namespace {
// Bound the total number of per-QP teardown diagnostic lines logged at INFO
// across the process, so a host with many QPs does not flood the log. Returns
// true if the caller should emit its line; on the first call past the limit it
// logs a single suppression notice. Shared by all ~Qp() diagnostics so the cap
// governs their combined output.
bool claimQpTeardownLogLine() {
  static std::atomic<uint64_t> logged{0};
  const uint32_t limit = FLAGS_libsoftmetaroce_qp_teardown_log_limit;
  const uint64_t n = logged.fetch_add(1, std::memory_order_relaxed);
  if (limit == 0 || n < limit) {
    return true;
  }
  if (n == limit) {
    LOG(INFO) << "qp teardown diagnostics suppressed after " << limit
              << " lines; raise --libsoftmetaroce_qp_teardown_log_limit to log "
                 "more";
  }
  return false;
}

bool gidEntriesEqual(const ibv_gid_entry& lhs, const ibv_gid_entry& rhs) {
  return std::memcmp(lhs.gid.raw, rhs.gid.raw, sizeof(lhs.gid.raw)) == 0 &&
      lhs.gid_index == rhs.gid_index && lhs.port_num == rhs.port_num &&
      lhs.gid_type == rhs.gid_type && lhs.ndev_ifindex == rhs.ndev_ifindex;
}

} // namespace

static BaseHeader::Opcode::OperationType operationTypeFromWqePosn(
    const Wqe& wqe,
    uint32_t posn) {
  switch (wqe.getWorkType()) {
    case Wqe::WorkType::kVirtualSend:
      return BaseHeader::Opcode::OperationType::READ_RESP;
    case Wqe::WorkType::kSend:
      return FLAGS_libsoftmetaroce_emit_send_last && wqe.isLastPosn(posn)
          ? BaseHeader::Opcode::OperationType::SEND_LAST
          : BaseHeader::Opcode::OperationType::SEND;
    case Wqe::WorkType::kSendWithImm:
      return (wqe.isLastPosn(posn))
          ? BaseHeader::Opcode::OperationType::SEND_IMM_P
          : BaseHeader::Opcode::OperationType::SEND;
    case Wqe::WorkType::kWrite:
      return BaseHeader::Opcode::OperationType::WRITE;
    case Wqe::WorkType::kWriteWithImm:
      return (wqe.isLastPosn(posn))
          ? BaseHeader::Opcode::OperationType::WRITE_IMM_P
          : BaseHeader::Opcode::OperationType::WRITE_IMM;
    case Wqe::WorkType::kRead:
      return BaseHeader::Opcode::OperationType::READ;
    case Wqe::WorkType::kReadWithImm:
      return BaseHeader::Opcode::OperationType::READ_IMM_P;
    case Wqe::WorkType::kAtomicFetchAdd:
      return BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD;
    case Wqe::WorkType::kAtomicCmpSwp:
      return BaseHeader::Opcode::OperationType::ATOMIC_CMP_SWP;
    case Wqe::WorkType::kVirtualAtomicAck:
      return BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP;
    default:
      LOG(FATAL) << fmt::format("unsupported wqe type {}", wqe.getWorkType());
  }
}

std::unique_ptr<DataPacket> Qp::buildDataPacket(
    uint16_t flowId,
    uint32_t localQpNum,
    uint32_t remoteQpNum,
    uint8_t tagNum,
    uint16_t sndNxt,
    uint16_t sndUna,
    const Wqe& wqe,
    uint32_t sndNxtPosn,
    const folly::IOBuf& payload,
    bool isRetransmit,
    bool isTimeout,
    uint32_t rateHints) {
  const auto payloadSizeBytes =
      static_cast<uint32_t>(payload.computeChainDataLength());
  // A payload within one buffer is sent from it in place; one that spans
  // sges is gathered into a buffer the packet owns.
  std::unique_ptr<folly::IOBuf> gathered =
      payload.isChained() ? payload.cloneCoalesced() : nullptr;
  const void* const payloadAddress =
      payloadSizeBytes == 0 || gathered != nullptr ? nullptr : payload.data();
  const auto operationType = operationTypeFromWqePosn(wqe, sndNxtPosn);
  const BaseHeader::Opcode opcode(operationType);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setOpcode(opcode)
                                    .setVersion(kMetaRoceProtocolVersion)
                                    .setFlowId(flowId)
                                    .setDstQp(remoteQpNum)
                                    .setSrcQp(localQpNum)
                                    .build();
  const auto nowNs = getNowNs();
  const DataHeader dataHeader =
      DataHeader::DataHeaderBuilder()
          .setFsn(sndNxt)
          .setUna(sndUna)
          .setTag(tagNum)
          .setAckRequest(true)
          .setIsRetransmit(isRetransmit)
          .setIsTimeout(isTimeout)
          .setPadding(Packet::payloadPadBytes(payloadSizeBytes))
          .setPosn(sndNxtPosn)
          .setMsn(wqe.getMsn().value_or(0))
          .setCsn(wqe.getCsn().value_or(0))
          .setRateHints(rateHints)
          .setTimestamp(nowNs)
          .build();
  if (opcode.hasRwHdr()) {
    const ReadWriteWqe& rdma_wqe =
        *CHECK_NOTNULL(dynamic_cast<const ReadWriteWqe*>(&wqe));
    if (wqe.getMessageSizeBytes() != 0) {
      LOG_IF(ERROR, rdma_wqe.getKey() == 0) << "Rkey should be non-zero";
    }
    const ReadWriteHeader rwHeader =
        ReadWriteHeader::ReadWriteHeaderBuilder()
            .setMessageLength(wqe.getMessageSizeBytes())
            .setKey(rdma_wqe.getKey())
            .setMessageAddr(rdma_wqe.getAddress())
            .build();
    const auto maybeImmdt =
        opcode.hasImmData() ? wqe.getImmediateData() : std::nullopt;
    if (gathered != nullptr) {
      return std::make_unique<ReadWritePacket>(
          baseHeader, dataHeader, rwHeader, std::move(gathered), maybeImmdt);
    }
    return std::make_unique<ReadWritePacket>(
        baseHeader,
        dataHeader,
        rwHeader,
        payloadAddress,
        payloadSizeBytes,
        maybeImmdt);
  } else if (
      operationType == BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD ||
      operationType == BaseHeader::Opcode::OperationType::ATOMIC_CMP_SWP) {
    const AtomicWqe& atomicWqe =
        *CHECK_NOTNULL(dynamic_cast<const AtomicWqe*>(&wqe));
    // A misaligned remote address is validated by the responder, which NAKs it;
    // send it rather than aborting here.
    const AtomicHeader atomicHeader =
        AtomicHeader::AtomicHeaderBuilder()
            .setRemoteAddr(atomicWqe.getRemoteAddress())
            .setRkey(atomicWqe.getRkey())
            .setSwapOrAddData(atomicWqe.getSwapOrAddData())
            .setCompareData(atomicWqe.getCompareData())
            .build();
    return std::make_unique<AtomicPacket>(baseHeader, dataHeader, atomicHeader);
  } else if (
      operationType == BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP) {
    const VirtualAtomicAckWqe& atomicAckWqe =
        *CHECK_NOTNULL(dynamic_cast<const VirtualAtomicAckWqe*>(&wqe));
    const AtomicAckHeader atomicAckHeader(atomicAckWqe.getOrigRemoteData());
    return std::make_unique<AtomicAckPacket>(
        baseHeader, dataHeader, atomicAckHeader);
  } else {
    const auto maybeImmdt =
        (operationType == BaseHeader::Opcode::OperationType::SEND_IMM_P)
        ? wqe.getImmediateData()
        : std::nullopt;
    if (gathered != nullptr) {
      return std::make_unique<DataPacket>(
          baseHeader, dataHeader, std::move(gathered), maybeImmdt);
    }
    return std::make_unique<DataPacket>(
        baseHeader, dataHeader, payloadAddress, payloadSizeBytes, maybeImmdt);
  }
}

Qp::Qp(
    uint32_t qpNum,
    HighResTimer& timer,
    QpArbiterIface& arbiter,
    MemoryRegions& memoryRegions,
    CompletionQueue* sendCq,
    CompletionQueue* recvCq,
    struct ibv_qp* ibvQp,
    RcnRateHint* rcnRateHint,
    bool sqSigAll,
    uint32_t maxInlineData,
    uint32_t sendQueueDepth,
    uint32_t receiveQueueDepth,
    SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue)
    : retxTimer_(timer),
      arbiter_(arbiter),
      memoryRegions_(memoryRegions),
      sender_{nullptr},
      rcnRateHint_{rcnRateHint},
      localQpNum_{qpNum},
      remoteQpNum_{0},
      gsoEnabled_{FLAGS_libsoftmetaroce_send == "gso"},
      uniqueSender_{nullptr},
      // Capturing this is safe: a receive post is the only caller, and none
      // can reach a Qp still being constructed.
      tagSet_{qpNum, [this](uint8_t tag, uint16_t postedCsn) {
                this->scheduleRnrCancelPacket(tag, postedCsn);
              }} {
  readySignal_ = arbiter.addQp(
      localQpNum_,
      [this]() { return this->getNextPacketToSend(); },
      [this]() { return this->applyHandedOffAcks(); });
  // Construction failure skips ~Qp, so unregister callbacks before unwinding.
  auto removeFromArbiter =
      folly::makeGuard([&] { arbiter_.removeQp(localQpNum_); });
  addTag(
      kMetaRoceInitialTag,
      sendCq,
      recvCq,
      ibvQp,
      sqSigAll,
      maxInlineData,
      sendQueueDepth,
      receiveQueueDepth,
      sharedReceiveQueue);
  hrnrTimerCallback_ = std::make_unique<HighResTimer::Callback>([&]() {
    VLOG(2) << "HrnrTimer expired for QP: " << localQpNum_;
    if (isTearingDown()) {
      return;
    }
    if (hasWatchdogExpired()) {
      processWatchdogExpiryAsync();
      return;
    }
    // Wake the sender; it owns the BLOCKED_TX -> PROBING transition and issues
    // the probe.
    signalReadyAsync();
  });
  brnrTimerCallback_ = std::make_unique<HighResTimer::Callback>([&]() {
    VLOG(2) << "BrnrTimer expired for QP: " << localQpNum_;
    if (isTearingDown()) {
      return;
    }
    if (hasWatchdogExpired()) {
      processWatchdogExpiryAsync();
      return;
    }
    bool newlyProbing = false;
    bool retryLimitExceeded = false;
    for (const auto tagNum : tagSet_.getTagNums()) {
      auto tag = getTag(tagNum);
      auto tagGuard = tag->scopedLock();
      if (tag->getBrnrState() == BrnrState::BRNR_BLOCKED_TX) {
        // Each backoff expiry initiates one more probe (retry). A finite
        // rnr_retry limit enters the error state once its retries are spent
        // rather than probing forever -- on the expiry after the last probe: an
        // arriving BRNR is what arms this timer, so getting here again is the
        // responder refusing that probe too.
        if (tag->hasSpentRnrRetries()) {
          retryLimitExceeded |= !tag->isInErrorState();
          // Already in error; the timer keeps firing for the other tags.
          continue;
        }
        tag->brnr().beginProbe();
        newlyProbing = true;
      }
    }
    if (retryLimitExceeded) {
      // Applied by the sender at its next visit: failing the work writes send
      // WQE posn state, which only the tx owner may touch. Both sides of the
      // flag are relaxed because it publishes no data of its own: the Tag mutex
      // publishes every field the sweep goes on to read under it.
      rnrRetryExceededPending_.store(true, std::memory_order_relaxed);
    }
    if (newlyProbing || retryLimitExceeded) {
      // Signalled even when only the sweep is left to run, so the sender visits
      // to do it, and signalled even when a sibling Tag ran out, so this Tag's
      // probe is not stranded behind the sibling.
      signalReadyAsync();
    }
  });
  removeFromArbiter.dismiss();
}

bool Qp::logTeardownDiagnostics() {
  const auto enqueued = soAcksEnqueued_.load(std::memory_order_relaxed);
  const auto dropped = soAcksDropped_.load(std::memory_order_relaxed);
  const auto cancelsEnqueued =
      soRnrCancelsEnqueued_.load(std::memory_order_relaxed);
  const auto cancelsDropped =
      soRnrCancelsDropped_.load(std::memory_order_relaxed);
  const bool anyDropped = dropped > 0 || cancelsDropped > 0;
  // Claim a budget slot only for a QP that enqueued, so idle ones don't spend
  // it. A drop reports outside the budget, which healthy QPs could exhaust
  // first.
  const bool logTeardown = anyDropped ||
      ((enqueued > 0 || cancelsEnqueued > 0) && claimQpTeardownLogLine());
  if (logTeardown) {
    const auto drained = soAcksDrained_.load(std::memory_order_relaxed);
    const auto visits = soDrainVisits_.load(std::memory_order_relaxed);
    const auto recvToProcessNs =
        soAckRecvToProcessNs_.load(std::memory_order_relaxed);
    const auto ackApplyNs = soAckApplyNs_.load(std::memory_order_relaxed);
    // arbiterIntakeFull is a single counter on the shared QpArbiter, so it is
    // process-wide (summed across all QPs) -- labeled as such to avoid reading
    // it as a per-QP number. Omit the pair on the usual line: RNR-cancels are
    // rare.
    const auto rnrCancels = (cancelsEnqueued > 0 || cancelsDropped > 0)
        ? fmt::format(
              " rnrCancelsEnqueued={} rnrCancelsDropped={}",
              cancelsEnqueued,
              cancelsDropped)
        : std::string{};
    const auto diagnostics = fmt::format(
        "single-owner-tx qp {} diagnostics: acksEnqueued={} acksDrained={} "
        "acksDropped={} drainVisits={} avgBatch={:.2f} avgAckRecvToProcessUs={:.2f} "
        "ackApplyMs={:.1f} avgApplyUsPerAck={:.2f} "
        "signalReadyFallbacks={} procWideArbiterIntakeFull={} "
        "procWideStaleSignalDrop={}{}",
        localQpNum_,
        enqueued,
        drained,
        dropped,
        visits,
        visits ? static_cast<double>(drained) / static_cast<double>(visits)
               : 0.0,
        drained ? static_cast<double>(recvToProcessNs) /
                static_cast<double>(drained) / 1000.0
                : 0.0,
        static_cast<double>(ackApplyNs) / 1e6,
        drained ? static_cast<double>(ackApplyNs) /
                static_cast<double>(drained) / 1000.0
                : 0.0,
        getSignalReadyFallbacks(),
        arbiter_.getIntakeFullCount(),
        arbiter_.getStaleSignalDropCount(),
        rnrCancels);
    // A drop reports at WARNING so tuning finds it without grepping the
    // routine line.
    if (anyDropped) {
      LOG(WARNING) << diagnostics;
    } else {
      LOG(INFO) << diagnostics;
    }
  }
  return logTeardown;
}

Qp::~Qp() {
  // Stamp teardown start now: the window stall is closed and lifetime measured
  // against this instant, so the cancellation/join/removeQp steps below are not
  // counted as window-blocked.
  const uint64_t teardownStartNs = steadyNowNs();
  const bool logTeardown = logTeardownDiagnostics();
  if (rcnActive_ && rcnRateHint_) {
    deactivateRcn();
  }
  // Refuses work at each token check from here on.
  cancellationSource_.requestCancellation();
  // Waits out a callback that read the token before it was set.
  retxTimer_.fenceCallbacks();
  // Closed before the join. Released before it: a coroutine on the scope can
  // spawn onto it again, through processWatchdogExpiry().
  closeScopeAdmission();
  folly::coro::blockingWait(
      [this]() -> folly::coro::Task<> { co_await asyncScope_.joinAsync(); }());
  // Waits out the sender, whose build the arbiter holds its lock across, and
  // frees the readiness handle the steps above drained the readers of.
  arbiter_.removeQp(localQpNum_);
  // Durable now: the sender is gone, and a late expiry returns at its check.
  for (auto& p : txFlows_) {
    p.destroyRetransmissionTimer();
  }
  if (hrnrTimerCallback_) {
    retxTimer_.cancelCallback(hrnrTimerCallback_.get());
  }
  if (brnrTimerCallback_) {
    retxTimer_.cancelCallback(brnrTimerCallback_.get());
  }

  // Complete outstanding WQEs with a flush error (verbs contract on destroy).
  // Safe without first clearing the tx trackers' raw Wqe* refs: teardown has
  // quiesced the timers, coroutines, and receives that would dereference them.
  for (const auto tagNum : tagSet_.getTagNums()) {
    auto tag = getTag(tagNum);
    auto retirementGuard = tag->lockForWqeRetirement();
    tag->flushAllPendingWqesInError();
  }

  // removeQp above quiesced the sender loop, so these sender-thread-only
  // window-stall counters can be read here without synchronization.
  {
    const bool stallOpenedBeforeTeardown =
        qwndBlockedSinceNs_ != 0 && teardownStartNs >= qwndBlockedSinceNs_;
    if (stallOpenedBeforeTeardown) {
      qwndBlockedNs_ += teardownStartNs - qwndBlockedSinceNs_;
    }
    // Surface the window-stall summary even with no single-owner ACK activity
    // (which alone gates logTeardown), reusing that slot or claiming one.
    const bool logWindowStall =
        qwndBlockedEvents_ > 0 && (logTeardown || claimQpTeardownLogLine());
    if (logWindowStall) {
      const uint64_t createdAtNs = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              createdAt_.time_since_epoch())
              .count());
      const uint64_t lifetimeNs =
          teardownStartNs > createdAtNs ? teardownStartNs - createdAtNs : 0;
      const double blockedPct = lifetimeNs
          ? 100.0 * static_cast<double>(qwndBlockedNs_) /
              static_cast<double>(lifetimeNs)
          : 0.0;
      // maxInflight sums per-flow high-water windows, so it is an upper bound:
      // per-flow peaks need not coincide.
      uint32_t maxInflightFsns = 0;
      for (auto& p : txFlows_) {
        maxInflightFsns += p.getTxState().maxInflightFsns;
      }
      const double srttUs = getEstQpRttNs() / 1e3;
      LOG(INFO) << fmt::format(
          "qp {} send-stall summary: lifetime={:.3f}ms "
          "windowBlocked={:.3f}ms ({:.1f}% of lifetime, {} stalls) "
          "maxInflight={} pkts srtt={:.1f}us",
          localQpNum_,
          lifetimeNs / 1e6,
          qwndBlockedNs_ / 1e6,
          blockedPct,
          qwndBlockedEvents_,
          maxInflightFsns,
          srttUs);
    }
  }
  // clear all flows, which have a reference back to the qp.
  txFlows_.clear();
}

[[nodiscard]] bool Qp::hasWatchdogExpired() const {
  return watchdog_.hasExpired();
}

std::vector<uint16_t> Qp::buildFlowIdToPortPairIdxMap(uint32_t maxFlowCount) {
  std::vector<uint16_t> result;
  result.reserve(maxFlowCount);
  CHECK(!portPairs_.empty());
  for (uint32_t flowId = 0; flowId < maxFlowCount; flowId++) {
    result.push_back(
        static_cast<uint16_t>(
            (getPortPairRotation() + flowId) %
            static_cast<uint32_t>(portPairs_.size())));
  }
  return result;
}

uint32_t Qp::getPortPairRotation() const {
  return (localQpNum_ >> 1) + (remoteQpNum_ >> 1);
}

std::pair<uint16_t, bool> Qp::addOrGetLocalPort(
    uint16_t localPortId,
    const folly::IPAddress& localAddr,
    const ibv_gid_entry& entry) {
  if (localPortId >= kMaxPorts) {
    throw std::length_error("local port id exceeds active-port bitset width");
  }
  if (auto it = localPortIdToIdx_.find(localPortId);
      it != localPortIdToIdx_.end()) {
    const auto idx = it->second;
    const auto& existing = localPorts_.at(idx);
    if (existing.addr != localAddr || !gidEntriesEqual(existing.gid, entry)) {
      throw std::runtime_error("conflicting local port id");
    }
    return {idx, false};
  }
  const auto idx = static_cast<uint16_t>(localPorts_.size());
  localPorts_.push_back(LocalPort{localPortId, localAddr, entry});
  // Undo the push if the map insert below throws.
  auto undoPush = folly::makeGuard([&] { localPorts_.pop_back(); });
  localPortIdToIdx_.emplace(localPortId, idx);
  undoPush.dismiss();
  return {idx, true};
}

std::pair<uint16_t, bool> Qp::addOrGetRemotePort(
    uint16_t remotePortId,
    const folly::IPAddress& remoteAddr) {
  if (remotePortId >= kMaxPorts) {
    throw std::length_error("remote port id exceeds active-port bitset width");
  }
  if (auto it = remotePortIdToIdx_.find(remotePortId);
      it != remotePortIdToIdx_.end()) {
    const auto idx = it->second;
    const auto& existing = remotePorts_.at(idx);
    if (existing.addr != remoteAddr) {
      throw std::runtime_error("conflicting remote port id");
    }
    return {idx, false};
  }
  const auto idx = static_cast<uint16_t>(remotePorts_.size());
  remotePorts_.push_back(RemotePort{remotePortId, remoteAddr});
  // Undo the push if the map insert below throws.
  auto undoPush = folly::makeGuard([&] { remotePorts_.pop_back(); });
  remotePortIdToIdx_.emplace(remotePortId, idx);
  undoPush.dismiss();
  return {idx, true};
}

void Qp::addPortPair(
    uint16_t localPortId,
    const folly::IPAddress& localAddr,
    const ibv_gid_entry& entry,
    uint16_t remotePortId,
    const folly::IPAddress& remoteAddr) {
  // These helpers intern by logical id. That lets multiple pairs share one
  // logical port, while conflicting reuse of an id fails fast.
  const auto [localPortIdx, localInserted] =
      addOrGetLocalPort(localPortId, localAddr, entry);
  // Remove a newly added local port if a later step throws.
  auto rollbackLocal = folly::makeGuard([&] {
    if (localInserted) {
      localPortIdToIdx_.erase(localPortId);
      localPorts_.pop_back();
    }
  });
  const auto [remotePortIdx, remoteInserted] =
      addOrGetRemotePort(remotePortId, remoteAddr);
  // Remove a newly added remote port if the pair insert below throws.
  auto rollbackRemote = folly::makeGuard([&] {
    if (remoteInserted) {
      remotePortIdToIdx_.erase(remotePortId);
      remotePorts_.pop_back();
    }
  });
  const PortPair pair{localPortIdx, remotePortIdx};
  if (std::find_if(
          portPairs_.begin(), portPairs_.end(), [&](const auto& existing) {
            return existing.localPortIdx == pair.localPortIdx &&
                existing.remotePortIdx == pair.remotePortIdx;
          }) != portPairs_.end()) {
    rollbackRemote.dismiss();
    rollbackLocal.dismiss();
    return;
  }
  portPairs_.push_back(pair);
  rollbackRemote.dismiss();
  rollbackLocal.dismiss();
}

void Qp::addPortPair(
    const folly::IPAddress& localAddr,
    const ibv_gid_entry& entry,
    const folly::IPAddress& remoteAddr) {
  const auto existingPair =
      std::find_if(portPairs_.begin(), portPairs_.end(), [&](const auto& pair) {
        const auto& localPort = localPorts_.at(pair.localPortIdx);
        const auto& remotePort = remotePorts_.at(pair.remotePortIdx);
        return localPort.addr == localAddr &&
            gidEntriesEqual(localPort.gid, entry) &&
            remotePort.addr == remoteAddr;
      });
  if (existingPair != portPairs_.end()) {
    return;
  }
  const auto portId = static_cast<uint16_t>(portPairs_.size());
  addPortPair(portId, localAddr, entry, portId, remoteAddr);
}

bool Qp::isRemotePortActive(uint16_t remotePortIdx, uint16_t activePortSet)
    const {
  const auto remotePortId = getRemotePortId(remotePortIdx);
  return ((activePortSet >> remotePortId) & 1) != 0;
}

void Qp::setupFlows(
    uint32_t txFlowCount,
    uint32_t rxFlowCount,
    Sender* sender /*optional*/,
    SackRetransmissionConfig sackRetxConfig) {
  if (portPairs_.empty()) {
    throw std::runtime_error("No port pairs");
  }
  // Before any flow exists: the receiver applies no ACK for a flow it cannot
  // find, so none is applied inline once the ring makes the sender the owner.
  if (FLAGS_libsoftmetaroce_single_owner_tx && !pendingRxEvents_) {
    pendingRxEvents_ =
        std::make_unique<folly::ProducerConsumerQueue<PendingRxEvent>>(
            kPendingRxEventsCapacity);
  }
  txFlows_.reserve(txFlowCount);
  rxFlows_.reserve(rxFlowCount);
  if (sender == nullptr) {
    uniqueSender_ = std::make_unique<Sender>(arbiter_); // default port
    sender_ = uniqueSender_.get();
  } else {
    uniqueSender_ = nullptr;
    sender_ = sender;
  }
  maxHeldSendsPerFlow_ = sender_->getSendBatch();
  const auto maxFlowCount = std::max(txFlowCount, rxFlowCount);
  const auto portMap = buildFlowIdToPortPairIdxMap(maxFlowCount);
  if (congestionController_) {
    // A gso burst to one path rides a single datagram.
    congestionController_->setPortPairCount(
        gsoEnabled_ ? 1 : static_cast<uint16_t>(portPairs_.size()));
  }

  // Create TX and RX flows, assigning each a socket bound to its local address.
  std::unordered_map<folly::IPAddress, std::list<uint16_t>> addrToSocketId;
  for (uint32_t flowId = 0; flowId < maxFlowCount; flowId++) {
    const auto portPairIdx = portMap[flowId];
    const auto& pair = portPairs_.at(portPairIdx);
    const auto& localPort = localPorts_.at(pair.localPortIdx);
    const auto& localAddr = localPort.addr;
    const auto& entry = localPort.gid;

    uint16_t rxFlowSocketId = 0;
    if (flowId < txFlowCount) {
      auto socketId = sender_->getSocketIdFromAddress(localAddr, entry);
      addrToSocketId[localAddr].push_back(socketId);
      rxFlowSocketId = socketId;
      txFlows_.emplace_back(
          *this, flowId, socketId, &retxTimer_, portPairIdx, sackRetxConfig);
      if (congestionController_) {
        congestionController_->onFlowAdded(flowId);
      } else {
        LOG(ERROR) << "CongestionController is not set while adding flowId: "
                   << flowId << "; in unit testing, this is fine.";
      }
    } else if (flowId < rxFlowCount) {
      if (addrToSocketId.find(localAddr) == addrToSocketId.end()) {
        auto socketId = sender_->getSocketIdFromAddress(localAddr, entry);
        addrToSocketId[localAddr].push_back(socketId);
      }
      // Round robin through the sockets for this address
      rxFlowSocketId = addrToSocketId[localAddr].front();
      addrToSocketId[localAddr].pop_front();
      addrToSocketId[localAddr].push_back(rxFlowSocketId);
    }
    if (flowId < rxFlowCount) {
      rxFlows_.emplace_back(*this, flowId, rxFlowSocketId, portPairIdx);
    }
  }

  // If an HRNR NAK was sent before flows were set up, the sender is
  // waiting in HRNR backoff. Now that flows are ready, send an RNR cancel
  // to allow the sender to resume immediately. An HRNR holds the QP rather
  // than a tag, so one cancel releases it whichever tag it carries.
  if (rnrCancelPending_) {
    rnrCancelPending_ = false;
    const auto tag = getTag(kMetaRoceInitialTag);
    CHECK(tag) << "the initial tag lives as long as the queue pair";
    scheduleRnrCancelPacket(kMetaRoceInitialTag, tag->getNextRxCsn() - 1);
  }
}

void Qp::setHrnrState(HrnrState newState) {
  VLOG(2) << "Setting HRNR state to " << hrnrStateToString(newState)
          << " for qp: " << localQpNum_;
  CHECK(canTransitionHrnr(hrnrState_, newState));
  hrnrState_ = newState;
}

void Qp::enterHrnrBlock(uint32_t backoffUs) {
  hrnrBackoffDeadlineNs_ =
      getNowNs64() + static_cast<uint64_t>(backoffUs) * kNsPerUs;
  // BLOCKED_TX is reachable from any state, so bypass setHrnrState()'s
  // transition check.
  hrnrState_ = HrnrState::HRNR_BLOCKED_TX;
  retxTimer_.scheduleTimeout(
      std::chrono::microseconds(backoffUs), hrnrTimerCallback_.get());
}

void Qp::updateRcnActiveState(uint32_t incomingRateHint) {
  if (!rcnRateHint_) {
    return;
  }
  if (incomingRateHint > 0) {
    usingPeerDrivenActivation_ = true;
  }
  // In peer-driven mode, activate only when the peer signals a non-zero rate
  // hint. Otherwise, activate on any data-packet arrival.
  if (!rcnActive_ && (!usingPeerDrivenActivation_ || incomingRateHint > 0)) {
    rcnActive_ = true;
    rcnRateHint_->onQpBecameActive();
  } else if (
      rcnActive_ && incomingRateHint == 0 && usingPeerDrivenActivation_) {
    // Only honor the rate_hints==0 deactivation in peer-driven mode.
    deactivateRcn();
  }
}

void Qp::updateRemoteActivePortSet(uint16_t reportedPortSet) {
  if (reportedPortSet == 0) {
    return;
  }
  uint16_t validMask = 0;
  for (const auto& port : remotePorts_) {
    validMask |= static_cast<uint16_t>(1u << port.id);
  }
  reportedPortSet &= validMask;
  const auto nowNs = getNowNs64();
  const uint16_t stored = remoteActivePortSet_;
  const uint16_t wentDown = stored & ~reportedPortSet;
  const uint16_t cameUp = ~stored & reportedPortSet;
  uint16_t accepted = stored & ~wentDown;
  if (wentDown) {
    lastPortDownNs_ = nowNs;
  }
  if (cameUp) {
    const uint64_t elapsed = nowNs - lastPortDownNs_;
    if (lastPortDownNs_ == 0 || elapsed >= kPortReadyDebounceNs) {
      accepted |= cameUp;
    }
  }
  if (accepted != stored) {
    LOG(INFO) << "QP " << localQpNum_ << " remote active port set: 0x"
              << std::hex << stored << " -> 0x" << accepted << std::dec;
    remoteActivePortSet_ = accepted;
  }
}

void Qp::checkRcnActiveOnWqeCompletion() {
  if (!rcnActive_ || !rcnRateHint_) {
    return;
  }
  // Stay active if any other receive WQE is still actively receiving data
  for (const auto tagNum : tagSet_.getTagNums()) {
    if (getTag(tagNum)->hasPartiallyReceivedRWqe()) {
      return;
    }
  }
  deactivateRcn();
}

void Qp::deactivateRcn() {
  rcnActive_ = false;
  CHECK_NOTNULL(rcnRateHint_)->onQpBecameInactive();
}

bool Qp::checkForRxMessageCompletion(
    Wqe* FOLLY_NULLABLE wqe,
    const BaseHeader::Opcode::OperationType opType,
    const uint8_t tagNum,
    const uint16_t msn,
    const uint16_t posn) {
  auto tag = getTag(tagNum);
  if (opType == BaseHeader::Opcode::OperationType::READ ||
      opType == BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD ||
      opType == BaseHeader::Opcode::OperationType::ATOMIC_CMP_SWP) {
    // rx packet is a read/atomic request: update cdmsn state and return
    return tag->markDeliveredAndCompleteInOrder(msn);
  }

  if (opType == BaseHeader::Opcode::OperationType::READ_IMM_P) {
    // Unlike the read/atomic branch above, this advances no cdmsn state yet:
    // the MSN is incorporated when the generated read response is
    // acknowledged, which completes the bound receive as well.
    return true;
  }

  if (wqe == nullptr) {
    LOG(ERROR) << "Provided WQE is null";
    return true;
  }

  if (!wqe->hasOutstandingPosns()) {
    LOG_FIRST_N(ERROR, 10) << fmt::format(
        "Failing tag {} on qp {} msn {}: posn {} arrived for a message already holding a packet for every posn it has",
        tagNum,
        localQpNum_,
        msn,
        posn);
    return false;
  }

  wqe->onPosnReceived(posn);
  if (wqe->getOutstandingPosnCount() == 0) {
    if (!wqe->everyPosnArrivedOnce()) {
      LOG_FIRST_N(ERROR, 10) << fmt::format(
          "Failing tag {} on qp {} msn {}: a posn arrived more than once, so the message is short one that never did and can no longer complete",
          tagNum,
          localQpNum_,
          msn);
      return false;
    }
    const bool everyCsnInSequence =
        tag->handleRxWqeCompletion(*wqe, opType, msn);
    // Every posn of the message has arrived, so it no longer counts as
    // partially received, even where its csn is out of sequence.
    checkRcnActiveOnWqeCompletion();
    if (!everyCsnInSequence) {
      return false;
    }
  }
  return true;
}

void Qp::checkForTxMessageCompletion(
    const std::vector<PacketId>& newlyAckedPackets) {
  DCHECK(!newlyAckedPackets.empty())
      << "recording progress for an ack that resolved no frame would hide a "
         "stall from the watchdog";
  // The tag comes from the tracker id: reading the work request at all is what
  // needs its Tag held.
  std::bitset<kMetaRoceTagCount> tagsAcked;
  // A work request's type is fixed at construction, so which tags need the
  // wider guard below can be answered before any guard is held.
  std::bitset<kMetaRoceTagCount> tagsNeedingRetirementGuard;
  for (const auto& packetId : newlyAckedPackets) {
    tagsAcked.set(packetId.tagNum);
    Wqe& wqe = *CHECK_NOTNULL(packetId.wqe);
    if (wqe.hasRequestFrame() || wqe.completesReceiveOnAck()) {
      tagsNeedingRetirementGuard.set(packetId.tagNum);
    }
  }
  watchdog_.recordProgress();

  SenderWake tagErrorWake = SenderWake::NOT_NEEDED;
  for (size_t tagNum = 0; tagNum < tagsAcked.size(); tagNum++) {
    if (tagsAcked.test(tagNum)) {
      auto tag = getTag(static_cast<uint8_t>(tagNum));
      auto tagGuard = tag->scopedLock();
      // Settling a request frame, or a read-with-immediate response completing
      // its paired receive, changes state the receive path consults; a batch of
      // plain sends and writes leaves that state undisturbed.
      std::unique_lock<folly::SharedMutex> retirementGuard;
      if (tagsNeedingRetirementGuard.test(tagNum)) {
        retirementGuard = tag->lockForWqeRetirement();
      }

      std::vector<std::reference_wrapper<Wqe>> wqesToComplete;
      for (const auto& packetId : newlyAckedPackets) {
        if (packetId.tagNum == tagNum) {
          Wqe& wqe = *CHECK_NOTNULL(packetId.wqe);
          wqe.onFrameNewlyAcked(packetId.posn);
          if (wqe.hasRequestFrame()) {
            // The response completes a read, so this ack can settle only a
            // failing work request, or one held back for the ack
            // alone.
            tag->retireAfterRequestFrameSettled(wqe);
          } else if (wqe.isCompletedByAcks()) {
            wqesToComplete.emplace_back(wqe);
          }
        }
      }

      if (!wqesToComplete.empty()) {
        // Retiring frees WQEs the receive path may still be reading.
        if (!retirementGuard.owns_lock()) {
          retirementGuard = tag->lockForWqeRetirement();
        }
        if (!tag->handleTxWqesCompletion(wqesToComplete)) {
          tagErrorWake |= enterTagErrorWithTagHeld(*tag);
          tag->flushPendingReceiveWqesInError();
        }
      }
    }
  }
  wakeSenderAfterTagError(tagErrorWake);
}

void Qp::clearHrnrBlock() {
  if (hrnrState_ == HrnrState::NORMAL_TX) {
    return;
  }
  setHrnrState(HrnrState::NORMAL_TX);
  if (hrnrTimerCallback_) {
    retxTimer_.cancelCallback(hrnrTimerCallback_.get());
  }
}

void Qp::cancelHrnrBackoff() {
  if (hrnrState_ == HrnrState::NORMAL_TX) {
    return;
  }
  VLOG(1) << "Cancelling HRNR (and associated timer callback) for qp: "
          << localQpNum_;
  clearHrnrBlock();
  // Rebuild the retransmission queues for each flow
  for (auto& txFlow : txFlows_) {
    VLOG(1) << "Rebuilding retransmission queue for qp " << localQpNum_
            << " flow: " << txFlow.getFlowId();
    txFlow.purgeRetxPktsToSend();
    txFlow.prepareRetxPacketsIfAny(true /*isRTOEvent*/);
  }
  signalReadyAsync();
}

void Qp::cancelRnrBackoff(uint8_t tagNum) {
  applyRnrCancel(tagNum, std::nullopt, 0);
}

void Qp::applyRnrCancel(
    uint8_t tagNum,
    std::optional<uint16_t> postedCsn,
    uint32_t nowNs) {
  VLOG(1) << "Cancelling RNR backoff for qp: " << localQpNum_;

  cancelHrnrBackoff();

  // Clear this tag's BRNR block under its semaphore. The shared BRNR timer is
  // left to self-expire: its callback is a no-op once no tag is blocked.
  auto tag = getTag(tagNum);
  {
    auto tagGuard = tag->scopedLock();
    if (postedCsn.has_value()) {
      tag->brnr().cancelThrough(*postedCsn, nowNs);
    } else {
      tag->brnr().resume();
    }
  }

  // Signal that the QP is ready to send packets
  signalReadyAsync();
}

std::unique_ptr<RnrCancelPacket> Qp::buildRnrCancelPacket(
    uint16_t flowId,
    uint32_t localQpNum,
    uint32_t remoteQpNum,
    uint8_t tag,
    uint16_t postedCsn) {
  using OperationType = BaseHeader::Opcode::OperationType;
  const BaseHeader baseHeader =
      BaseHeader::BaseHeaderBuilder()
          .setOpcode(BaseHeader::Opcode(OperationType::RNR_CANCEL))
          .setVersion(kMetaRoceProtocolVersion)
          .setFlowId(flowId)
          .setDstQp(remoteQpNum)
          .setSrcQp(localQpNum)
          .build();
  const RnrCancelHeader rnrCancelHeader =
      RnrCancelHeader::RnrCancelHeaderBuilder()
          .setTag(tag)
          .setPostedCsn(postedCsn)
          .setFlowsAvailable(static_cast<uint8_t>(rxFlows_.size()))
          .build();
  return std::make_unique<RnrCancelPacket>(baseHeader, rnrCancelHeader);
}

void Qp::setRnrCancelPending(bool pending) {
  rnrCancelPending_ = pending;
}

bool Qp::hasRnrCancelPending() const {
  return rnrCancelPending_;
}

void Qp::scheduleRnrCancelPacket(uint8_t tag, uint16_t postedCsn) {
  VLOG(1) << "Scheduling RNR cancel packet for qp: " << localQpNum_
          << " tag: " << (uint32_t)tag;
  pendingRnrCancels_.lock()->push({tag, postedCsn});
  signalReadyAsync();
}

bool Qp::enqueueRxEvent(PendingRxEvent&& event) {
  DCHECK(pendingRxEvents_)
      << "enqueueing a received event requires the single-owner ring; callers "
         "gate on usesSingleOwnerRxRing()";
  const bool written = pendingRxEvents_->write(std::move(event));
  // Nudge the sender either way: the ring holds un-applied events, so it should
  // drain them now rather than wait on a later enqueue or the retransmit timer.
  signalReadyAsync();
  return written;
}

void Qp::enqueueReceivedAck(
    uint16_t flowId,
    const AckHeader& ack,
    uint64_t recvNowNs) {
  if (enqueueRxEvent(
          PendingAck{
              .ackHeader = ack, .flowId = flowId, .recvNowNs = recvNowNs})) {
    soAcksEnqueued_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  soAcksDropped_.fetch_add(1, std::memory_order_relaxed);
  LOG_FIRST_N(WARNING, 10)
      << "received-event ring full on qp " << localQpNum_
      << "; dropping an ACK -- a later ACK recovers cumulative progress, but "
         "non-cumulative NAK/HRNR/BRNR/SACK rely on the retransmit timer";
}

void Qp::enqueueReceivedRnrCancel(const RnrCancelHeader& rnrCancel) {
  if (enqueueRxEvent(rnrCancel)) {
    soRnrCancelsEnqueued_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  soRnrCancelsDropped_.fetch_add(1, std::memory_order_relaxed);
  LOG_FIRST_N(WARNING, 10)
      << "received-event ring full on qp " << localQpNum_
      << "; dropping an RNR-cancel -- the block it would have lifted now waits "
         "out its backoff, whose probe draws the ack that lifts it";
}

bool Qp::applyHandedOffAcks() {
  // A receiver that applies ACKs on its own thread hands none off, so this
  // lock and the ring below are never both in use.
  auto inlineAckGuard = lockForInlineAckApply();
  // Single-owner-tx: apply any events the receiver handed off, on this (the
  // sender's) thread, before deciding what to send. Drain iff the ring exists
  // (allocated once, at setupFlows), so this stays consistent with enqueue
  // even if the flag is flipped at runtime.
  if (pendingRxEvents_) {
    static_assert(
        std::variant_size_v<PendingRxEvent> == 2,
        "the loop below treats any event that is not an ACK as an "
        "RNR-cancel; a third alternative would be applied as one");
    const bool profileApply = FLAGS_libsoftmetaroce_profile_send_egress;
    const uint64_t applyStartNs = profileApply ? steadyNowNs() : 0;
    uint64_t acksDrained = 0;
    // Not acksDrained: the ring also holds RNR-cancels, which apply no ack,
    // and the documented return is whether any entry was consumed.
    uint64_t eventsConsumed = 0;
    while (PendingRxEvent* rx = pendingRxEvents_->frontPtr()) {
      ++eventsConsumed;
      if (auto* pending = std::get_if<PendingAck>(rx)) {
        ++acksDrained;
        // Only a known flowId updates TX state and the active-port set; an
        // unknown one is dropped below.
        if (auto* txFlow = getTxFlowById(pending->flowId)) {
          updateRemoteActivePortSet(pending->ackHeader.getActivePortSet());
          // Measure RTT at processing time: an ACK can wait in the ring behind
          // other work, and folding that delay into the RTT would loosen the
          // RTO. Track the receipt->process wait separately.
          const uint32_t processNowNs = getNowNs();
          soAckRecvToProcessNs_.fetch_add(
              steadyNowNs() - pending->recvNowNs, std::memory_order_relaxed);
          txFlow->processAckPacket(pending->ackHeader, processNowNs);
        } else {
          LOG_FIRST_N(WARNING, 10)
              << "dropping received ACK for unknown flowId " << pending->flowId
              << " on qp " << localQpNum_
              << " (should not happen for a live QP)";
        }
      } else {
        // The tag was checked at ingress, and Tags are not deleted from a live
        // datapath.
        applyRnrCancel(
            std::get<RnrCancelHeader>(*rx).getTag(),
            std::get<RnrCancelHeader>(*rx).getPostedCsn(),
            getNowNs());
      }
      pendingRxEvents_->popFront();
    }
    // Keyed on acksDrained, not events: the averages these feed are per-ACK, so
    // a cancel-only visit would count as one that batched none.
    if (acksDrained > 0) {
      // Read the clock before the counters below, so their cost stays out of
      // the apply time.
      if (profileApply) {
        soAckApplyNs_.fetch_add(
            steadyNowNs() - applyStartNs, std::memory_order_relaxed);
      }
      soAcksDrained_.fetch_add(acksDrained, std::memory_order_relaxed);
      soDrainVisits_.fetch_add(1, std::memory_order_relaxed);
    }
    return eventsConsumed > 0;
  }
  return false;
}

std::unique_ptr<struct EgressPacket> Qp::getNextPacketToSend() {
  // When a receiver applies ACKs on its own thread (not the default), this lock
  // keeps it from writing transmit state while this build reads it.
  auto inlineAckGuard = lockForInlineAckApply();

  // Any Tag that has spent its rnr retries enters the error state here, on the
  // sender's thread, once handed-off ACKs have had their chance to unblock it.
  // Its wake is dropped: the latch it raised is swept below, on this thread.
  if (rnrRetryExceededPending_.exchange(false, std::memory_order_relaxed)) {
    std::ignore = processRnrRetryExceeded();
  }

  // Ahead of every early return below: a visit with no packet to send must
  // still fail the work of a tag in error. Downstream of the rnr handler
  // above, so a Tag it has just put in error is swept this visit.
  tagSet_.failWqesOfTagsInError();

  // On the tx owner's thread, and only once no Tag is left: the Tags share
  // these flows. The trackers stand -- those frames are not ours to give up.
  if (tagSet_.takeTagLeftServiceNotice() && tagSet_.noTagSurvives()) {
    LOG_FIRST_N(ERROR, 10) << "no surviving tag on qp " << localQpNum_
                           << "; taking its tx flows out of service";
    for (auto& txFlow : txFlows_) {
      txFlow.stopSending();
    }
    clearHrnrBlock();
  }

  std::optional<PendingRnrCancel> takenCancel;
  {
    // Held over the dequeue alone: the build below reaches flows and the
    // clock.
    auto locked = pendingRnrCancels_.lock();
    if (!locked->empty()) {
      takenCancel = locked->front();
      locked->pop();
    }
  }
  if (takenCancel.has_value()) {
    const PendingRnrCancel pending = *takenCancel;
    // An RNR-cancel is ready to send, so this is not a window stall; close any
    // open stall.
    foldQwndBlockedTime(/*windowBlocked=*/false);
    auto rnrCancelPacket = buildRnrCancelPacket(
        nextFlowIdToSendOn_,
        localQpNum_,
        remoteQpNum_,
        pending.tag,
        pending.postedCsn);
    auto txFlow = getTxFlowById(nextFlowIdToSendOn_);
    if (!txFlow) {
      LOG(ERROR) << "internal error: rnrCancel: unknown flowId "
                 << nextFlowIdToSendOn_ << " (should never happen)";
      return std::unique_ptr<struct EgressPacket>();
    }
    return std::make_unique<struct EgressPacket>(
        std::move(rnrCancelPacket),
        getSender().getSendSocket(
            txFlow->getSocketId(), EgressPacket::Kind::Ack),
        txFlow->getRemoteAddr(),
        localQpNum_,
        EgressPacket::RxmitType::None);
  }
  // Strict priority: acks over data. Reasonable in hardware where acks are
  // small and cheap, but per-packet overhead dominates in software.
  for (size_t i = 0; i < rxFlows_.size(); ++i) {
    auto egressPacket = rxFlows_[nextFlowIdToAckOn_].getNextAckToSend();
    nextFlowIdToAckOn_ = (nextFlowIdToAckOn_ + 1) % rxFlows_.size();
    if (egressPacket && egressPacket->packet) {
      foldQwndBlockedTime(/*windowBlocked=*/false);
      return egressPacket;
    };
  }

  // Direct write, not setHrnrState: a clear to NORMAL_TX racing this read from
  // the receiver thread would trip its transition check.
  if (hrnrState_ == HrnrState::HRNR_BLOCKED_TX &&
      getNowNs64() >= hrnrBackoffDeadlineNs_) {
    hrnrState_ = HrnrState::HRNR_PROBING;
  }

  // HRNR does not block sending acks, but will block sending data.
  if (hrnrState_ == HrnrState::HRNR_BLOCKED_TX) {
    VLOG(1) << "qp: " << getLocalQpNum()
            << " is in HRNR BLOCKED_TX state, not sending data packets";
    foldQwndBlockedTime(/*windowBlocked=*/false);
    return std::unique_ptr<struct EgressPacket>();
  }

  // A controller may own flow selection (fast start walks a fixed burst per
  // flow); start from its choice, and fall through to the rotation below if
  // that flow cannot send.
  if (congestionController_) {
    if (const auto next = congestionController_->flowForNextSend()) {
      // setupFlows registers each txFlow with the controller under the same id,
      // so a flow the controller chooses indexes txFlows_ directly.
      DCHECK_LT(next->flowId, txFlows_.size()) << "controller chose a flow the "
                                                  "QP does not have";
      // Only on a real switch: the controller chooses the same flow for every
      // send of a burst, so clearing the hold count unconditionally would
      // restart it each call and the cap below would never fire.
      if (next->flowId != nextFlowIdToSendOn_) {
        nextFlowIdToSendOn_ = next->flowId;
        sendsOnHeldFlow_ = 0;
      }
    }
  }

  // The for-loop checks each flow at most once. However, the flow currently
  // being checked is determined not by the loop variable i, but by
  // nextFlowIdToSendOn_.
  for (size_t i = 0; i < txFlows_.size(); ++i) {
    auto& txFlow = txFlows_[nextFlowIdToSendOn_];
    const bool probing = hrnrState_ == HrnrState::HRNR_PROBING;
    if (probing && txFlow.getNumPendingRetxPackets() == 0) {
      // Reclassify unacked data as a retransmission so the send below can
      // claim it as the probe.
      if (txFlow.hasUnackedData()) {
        txFlow.prepareRetxPacketsIfAny(true /*isRTOEvent*/);
      }
    }
    auto egressPacket = txFlow.getNextPacketToSend();
    const bool hasPacket = egressPacket && egressPacket->packet;
    // A retransmission is not window-gated and emitRun will not coalesce one,
    // so holding the flow for it buys no GSO and defers every other flow.
    const bool holdForGso = gsoEnabled_ && hasPacket &&
        egressPacket->rxmitType == EgressPacket::RxmitType::None;
    bool advanceFlow;
    if (holdForGso) {
      ++sendsOnHeldFlow_;
      // Advance if the hold spent a turn's batch, or the controller is missing.
      advanceFlow =
          !congestionController_ || sendsOnHeldFlow_ >= maxHeldSendsPerFlow_;
    } else {
      advanceFlow = !congestionController_ || !egressPacket ||
          congestionController_->shouldAdvanceFlowAfterSend(
              nextFlowIdToSendOn_);
    }
    if (advanceFlow) {
      nextFlowIdToSendOn_ = (nextFlowIdToSendOn_ + 1) % txFlows_.size();
      sendsOnHeldFlow_ = 0;
    }
    if (hasPacket) {
      // Leave HRNR_PROBING only once a probe (data packet) is actually sent:
      // one probe, then block and re-arm the backoff so the next probe waits
      // it out.
      if (probing) {
        enterHrnrBlock(txFlow.computeRtoIntervalUs());
      }
      foldQwndBlockedTime(/*windowBlocked=*/false);
      return egressPacket;
    };
  }
  // Only the single-owner ring makes the sender the sole writer of TX state;
  // otherwise a receiver-thread ACK races this read.
  DCHECK(!usesSingleOwnerRxRing() || hrnrState_ != HrnrState::HRNR_PROBING)
      << "single-owner sender left the probe loop still in HRNR_PROBING on qp "
      << localQpNum_;
  // Count a congestion-window stall only when the QP has unacked data in flight
  // but no flow has window. Pending retransmits do not count: they are never
  // gated by the window (retx can always be sent), so a retx-only flow is not
  // window-blocked. While HRNR gates the QP (PROBING/BLOCKED_TX), a stall is
  // the HRNR state machine.
  bool windowBlocked = false;
  if (congestionController_ && hrnrState_ != HrnrState::HRNR_PROBING &&
      hrnrState_ != HrnrState::HRNR_BLOCKED_TX) {
    // Per flow: a flow is window-blocked if it has unacked data in flight but
    // no window. Tested per flow so a busy flow's stall is not hidden by spare
    // window on an idle flow.
    for (size_t i = 0; i < txFlows_.size(); ++i) {
      if (txFlows_[i].hasUnackedData() &&
          congestionController_->getAvailableWindowSize(
              static_cast<uint16_t>(i)) == 0) {
        windowBlocked = true;
        break;
      }
    }
  }
  foldQwndBlockedTime(windowBlocked);
  return std::unique_ptr<struct EgressPacket>();
}

// Accumulate per-QP congestion-window-blocked time. windowBlocked starts/
// continues a stall (stamped once on entry); !windowBlocked closes one. Called
// only on the sender thread, so the counters need no synchronization.
void Qp::foldQwndBlockedTime(bool windowBlocked) {
  if (windowBlocked) {
    if (qwndBlockedSinceNs_ == 0) {
      qwndBlockedSinceNs_ = steadyNowNs();
      ++qwndBlockedEvents_;
    }
    return;
  }
  if (qwndBlockedSinceNs_ != 0) {
    qwndBlockedNs_ += steadyNowNs() - qwndBlockedSinceNs_;
    qwndBlockedSinceNs_ = 0;
  }
}

[[nodiscard]] RxFlow* FOLLY_NULLABLE Qp::getRxFlowById(uint16_t flowId) {
  if (flowId >= rxFlows_.size()) {
    return nullptr;
  }
  return &rxFlows_[flowId];
}

[[nodiscard]] TxFlow* FOLLY_NULLABLE Qp::getTxFlowById(uint16_t flowId) {
  if (flowId >= txFlows_.size()) {
    return nullptr;
  }
  return &txFlows_[flowId];
}

uint32_t Qp::getNumTxFlows() const {
  return (uint32_t)txFlows_.size();
}

uint32_t Qp::getNumRxFlows() const {
  return (uint32_t)rxFlows_.size();
}

void Qp::abandonFramesOfFailingWqes() {
  std::bitset<kMetaRoceTagCount> tagsWithAbandonedFrames;
  for (auto& txFlow : txFlows_) {
    txFlow.abandonFramesOfFailingWqes(tagsWithAbandonedFrames);
  }
  retireTagsWithAbandonedFrames(tagsWithAbandonedFrames);
}

void Qp::retireTagsWithAbandonedFrames(
    const std::bitset<kMetaRoceTagCount>& tagsWithAbandonedFrames) {
  for (size_t tagNum = 0; tagNum < tagsWithAbandonedFrames.size(); tagNum++) {
    if (!tagsWithAbandonedFrames.test(tagNum)) {
      continue;
    }
    auto tag = getTag(static_cast<uint8_t>(tagNum));
    auto tagGuard = tag->scopedLock();
    auto retirementGuard = tag->lockForWqeRetirement();
    tag->completeSettledFailedWqes();
  }
}

void Qp::onEnteringRts() {
  // Every Tag's timeout and retry count are refused past RTR, and one timer
  // serves the QP, so the longest deadline any Tag sets wins.
  std::chrono::milliseconds longestIntervalMs{0};
  for (const auto tagNum : tagSet_.getTagNums()) {
    const auto tag = getTag(tagNum);
    const auto exponent = tag->getLocalAckTimeoutExponent();
    // A retry count multiplies a wait, so a Tag setting one without a timeout
    // has set no wait for it to multiply and derives no deadline here.
    if (exponent.has_value()) {
      const auto deadlines = deadlinesFromQpAttributes(
          exponent.value(), tag->getTransportRetryCount());
      longestIntervalMs =
          std::max(longestIntervalMs, deadlines.watchdogIntervalMs);
    }
  }
  // A QP no Tag configured keeps the interval its watchdog was built with.
  if (longestIntervalMs > std::chrono::milliseconds::zero()) {
    configureWatchdogInterval(longestIntervalMs);
  }
}

void Qp::processWatchdogExpiry() {
  LOG(ERROR) << "Watchdog expired for qp: " << localQpNum_
             << ". No progress (ACKs) for " << watchdog_.getIntervalMs().count()
             << "ms";

  // Three passes, and the order is what keeps the tx tracker safe: it holds
  // work requests by raw pointer, so no thread may free one until the frames
  // it still holds are abandoned.

  // Determine if we're in an RNR state (HRNR or BRNR not in NORMAL_TX)
  bool isRnrState = (hrnrState_ != HrnrState::NORMAL_TX);

  // One: mark the work failing and error the tags. No work request is freed
  // here -- one with a frame in flight is not settled, so it does not retire.
  SenderWake tagErrorWake = SenderWake::NOT_NEEDED;
  for (const auto tagNum : tagSet_.getTagNums()) {
    auto tag = getTag(tagNum);
    auto tagGuard = tag->scopedLock();
    auto retirementGuard = tag->lockForWqeRetirement();

    // A tag in BRNR_CANCELED is sending, so work failing on it reports the
    // transport retry timeout.
    const BrnrState brnrState = tag->getBrnrState();
    const bool tagInBrnr = brnrState != BrnrState::NORMAL_TX &&
        brnrState != BrnrState::BRNR_CANCELED;

    // Every pending work request: the sweep abandons a frame only of one
    // already failing, and the flush below frees them all.
    WorkCompletionStatus errorStatus =
        (isRnrState || tagInBrnr) ? kWcRnrRetryExcErr : kWcRetryExcErr;
    subtractUnsentPosns(tag->markPendingWqesFailing(errorStatus));
    tag->completeSettledFailedWqes();

    tagErrorWake |= enterTagErrorWithTagHeld(*tag);
  }

  // Two: acks have stopped, so no ack is coming to resolve these frames.
  // Here, before the flush, while the work requests the tracker holds are
  // alive.
  abandonFramesOfFailingWqes();

  // Three: whatever the sweep did not settle. A tag with no send of its own is
  // finished with the receives it holds: it is in error as of the first pass,
  // so no arriving packet will fill them.
  for (const auto tagNum : tagSet_.getTagNums()) {
    auto tag = getTag(tagNum);
    auto tagGuard = tag->scopedLock();
    auto retirementGuard = tag->lockForWqeRetirement();
    tag->flushAllPendingWqesInError();
  }
  // Every Tag guard above has closed by here. The flows are left in service:
  // no tag survives, so the sender takes them out at its next visit.
  wakeSenderAfterTagError(tagErrorWake);
}

SenderWake Qp::processRnrRetryExceeded() {
  // The retry count is kept per Tag, so only the tags that have spent theirs
  // enter the error state. The rest are still making progress, or waiting on a
  // receiver that has not yet run out of chances to post a buffer.
  SenderWake tagErrorWake = SenderWake::NOT_NEEDED;
  for (const auto tagNum : tagSet_.getTagNums()) {
    auto tag = getTag(tagNum);
    auto tagGuard = tag->scopedLock();
    // The same fact the backoff timer acted on: spending an allowance is
    // arithmetic, and a zero one reads as spent before any receiver spoke.
    if (tag->getBrnrState() != BrnrState::BRNR_BLOCKED_TX ||
        !tag->hasSpentRnrRetries() || tag->isInErrorState()) {
      continue;
    }
    auto retirementGuard = tag->lockForWqeRetirement();
    LOG(ERROR) << "RNR retry limit (" << (int)tag->getRnrRetryLimit()
               << ") exceeded for qp: " << localQpNum_
               << " tag: " << (uint32_t)tagNum;
    // Through the funnel even though this marks the work failing right here;
    // the sweep it asks for finds no work left.
    tagErrorWake |= enterTagErrorWithTagHeld(*tag);
    // The receiver reported the message it had no buffer for, and that is the
    // one the error belongs to; it need not be the earliest still pending.
    const auto blockedMsn = tag->getBrnrFirstBlockedMsn();
    Wqe* blockedWqe = blockedMsn.has_value()
        ? tag->getPendingWqe(blockedMsn.value())
        : nullptr;
    uint32_t abandonedCount = 0;
    if (blockedWqe != nullptr) {
      abandonedCount += blockedWqe->markFailing(kWcRnrRetryExcErr);
      abandonedCount += tag->markPendingWqesFailing(kWrFlushErr);
    } else {
      // Not pending any more, so the earliest that is reports it instead.
      abandonedCount += tag->markPendingWqesFailing(kWcRnrRetryExcErr);
    }
    tagSet_.subtractUnsentPosns(abandonedCount);
    tag->flushPendingReceiveWqesInError();
    tag->completeSettledFailedWqes();
  }
  // Handed back rather than signalled here: every Tag guard above has closed,
  // but the sender may be the caller, and a signal to itself calls in nobody.
  return tagErrorWake;
}

void Qp::processWatchdogExpiryAsync() {
  // Use test-and-set to ensure only one watchdog expiry is scheduled.
  // Multiple timer callbacks (retx, HRNR, BRNR) may all detect the expiry.
  if (!watchdog_.tryClaimExpiry()) {
    // Already scheduled, no need to schedule again
    return;
  }

  // Schedule the watchdog expiry processing on a CPU executor to avoid
  // blocking the timer thread. The coroutine respects cancellation.
  addToScopeIfOpen(
      folly::coro::co_withExecutor(
          folly::getGlobalCPUExecutor(),
          folly::coro::co_withCancellation(
              cancellationSource_.getToken(),
              // Safe: destructor calls asyncScope_.joinAsync() after requesting
              // cancellation, ensuring this coroutine completes before Qp is
              // destroyed.
              // NOLINTNEXTLINE(facebook-folly-coro-temporary-by-ref)
              [this]() -> folly::coro::Task<void> {
                // Check for cancellation before doing any work.
                // This prevents use-after-free if the QP is being destroyed.
                auto token =
                    co_await folly::coro::co_current_cancellation_token;
                if (token.isCancellationRequested()) {
                  co_return;
                }
                processWatchdogExpiry();
                co_return;
              }())));
}

} // namespace libsoftmetaroce
