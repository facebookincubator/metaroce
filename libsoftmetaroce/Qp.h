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

#include <atomic>
#include <bitset>
#include <set>
#include <variant>

#include <folly/IPAddress.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/Synchronized.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Task.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <gflags/gflags.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/Cqe.h>
#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/Flow.h>
#include <libsoftmetaroce/HighResTimer.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/PacketId.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/QpPacer.h>
#include <libsoftmetaroce/RcnRateHint.h>
#include <libsoftmetaroce/RnrState.h>
#include <libsoftmetaroce/Tag.h>
#include <libsoftmetaroce/TagSet.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/WatchdogTimer.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/WrappingCompare.h>
#include <libsoftmetaroce/codec/AckHeader.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/DataHeader.h>
#include <libsoftmetaroce/codec/ReadWriteHeader.h>
#include <libsoftmetaroce/codec/RnrCancelHeader.h>
#include <libsoftmetaroce/congestion_control/CongestionController.h>
#include <libsoftmetaroce/congestion_control/SynchronizedCongestionController.h>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

DECLARE_bool(libsoftmetaroce_single_owner_tx);
DECLARE_bool(libsoftmetaroce_lockfree_signal);
DECLARE_bool(libsoftmetaroce_rfc6298_first_rtt_sample);
DECLARE_uint32(libsoftmetaroce_initial_est_rtt_us);

namespace libsoftmetaroce {

// For validation, we bind to the port configured by
// libsoftmetaroce_bind_to_port but also listen on kMetaRoceBindToPortAlternate.
inline constexpr uint16_t kMetaRoceBindToPortAlternate = 64206;

inline constexpr uint8_t kBrnrBackoffTimeExp = kDefaultRnrBackoffTimeExp;

// The first Tag on a QP, which is required for a QP to be useful, will use this
// tagNum.
inline constexpr uint8_t kMetaRoceInitialTag = 0;

class Sender;
class RxFlow;
class TxFlow;

// One received ACK handed from the receiver to the sender (the owner of TX
// state when --libsoftmetaroce_single_owner_tx is set). recvNowNs is the
// receipt time, kept so the sender can account the receipt-to-process delay;
// RTT itself is measured at processing time, when the ACK is applied. It is on
// the steady clock: the wait feeds a run-long average, which the 32-bit
// protocol clock cannot express past its ~4.3s wrap.
struct PendingAck {
  AckHeader ackHeader;
  uint16_t flowId{0};
  uint64_t recvNowNs{0};
};
// One received event, an ACK or an RNR-cancel, on one queue so the two stay
// mutually ordered.
using PendingRxEvent = std::variant<PendingAck, RnrCancelHeader>;
inline constexpr uint32_t kPendingRxEventsCapacity = 1024;

class Qp {
 public:
  // A QP's network topology is the explicit set of PortPair edges below.
  // Port ids are stable logical identifiers used by peer-visible state such as
  // active-port bitsets. Vector indexes are dense internal handles only.
  struct LocalPort {
    uint16_t id{}; // logical local port id, not an index into localPorts_
    folly::IPAddress addr;
    ibv_gid_entry gid{};
  };

  struct RemotePort {
    uint16_t id{}; // logical remote port id, not an index into remotePorts_
    folly::IPAddress addr;
  };

  // One configured edge from a local port to a remote port. Flows and failover
  // select only among the pairs configured here.
  struct PortPair {
    uint16_t localPortIdx;
    uint16_t remotePortIdx;
  };

  struct PendingRnrCancel {
    uint8_t tag;
    uint16_t postedCsn;
  };

  friend class TxFlow;
  friend class RxFlow;
  [[nodiscard]] static std::unique_ptr<DataPacket> buildDataPacket(
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
      uint32_t rateHints = kDefaultRateHintGbps);

  explicit Qp(
      uint32_t qpNum,
      HighResTimer& timer,
      QpArbiterIface& arbiter,
      MemoryRegions& memoryRegions,
      CompletionQueue* sendCq,
      CompletionQueue* recvCq,
      struct ibv_qp* ibvQp,
      RcnRateHint* rcnRateHint = nullptr,
      bool sqSigAll = false,
      // Handed to the initial Tag, which owns it: the QP keeps no cap of its
      // own, since every Tag negotiates one in its own creation call.
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr);

  explicit Qp(
      uint32_t qpNum,
      HighResTimer& timer,
      QpArbiterIface& arbiter,
      MemoryRegions& memoryRegions,
      CompletionQueue* cq,
      struct ibv_qp* ibvQp,
      RcnRateHint* rcnRateHint = nullptr,
      bool sqSigAll = false,
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr)
      : Qp(qpNum,
           timer,
           arbiter,
           memoryRegions,
           cq,
           nullptr,
           ibvQp,
           rcnRateHint,
           sqSigAll,
           maxInlineData,
           sendQueueDepth,
           receiveQueueDepth,
           sharedReceiveQueue) {}

  ~Qp();

  // Qp is not copyable or movable
  Qp(const Qp&) = delete;
  Qp& operator=(const Qp&) = delete;
  Qp(Qp&&) = delete;
  Qp& operator=(Qp&&) = delete;

  [[nodiscard]] bool containsTag(uint8_t tagNum) const {
    return tagSet_.contains(tagNum);
  }

  [[nodiscard]] std::shared_ptr<Tag> getTag(uint8_t tagNum) const {
    return tagSet_.getTagByNumber(tagNum);
  }

  [[nodiscard]] Tag& existingTag(uint8_t tagNum) const {
    return *CHECK_NOTNULL(tagSet_.getTagByNumber(tagNum).get());
  }

  void addTag(
      uint8_t tagNum,
      CompletionQueue* sendCq,
      CompletionQueue* recvCq,
      struct ibv_qp* ibvQp,
      bool sqSigAll = false,
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr) {
    tagSet_.add(
        tagNum,
        sendCq,
        recvCq,
        ibvQp,
        sqSigAll,
        maxInlineData,
        sendQueueDepth,
        receiveQueueDepth,
        sharedReceiveQueue);
  }

  void addTag(
      uint8_t tagNum,
      CompletionQueue* cq,
      struct ibv_qp* ibvQp,
      bool sqSigAll = false,
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr) {
    addTag(
        tagNum,
        cq,
        nullptr,
        ibvQp,
        sqSigAll,
        maxInlineData,
        sendQueueDepth,
        receiveQueueDepth,
        sharedReceiveQueue);
  }

  // The client path for adding a Tag to a QP that already exists; see
  // TagSet::addIfAllTagsInReset(). The QP's own first Tag uses addTag().
  // Returns the added Tag, or nullptr if a Tag had already left RESET.
  [[nodiscard]] std::shared_ptr<Tag> addTagIfAllInReset(
      uint8_t tagNum,
      CompletionQueue* sendCq,
      CompletionQueue* recvCq,
      struct ibv_qp* ibvQp,
      bool sqSigAll = false,
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr) {
    return tagSet_.addIfAllTagsInReset(
        tagNum,
        sendCq,
        recvCq,
        ibvQp,
        sqSigAll,
        maxInlineData,
        sendQueueDepth,
        receiveQueueDepth,
        sharedReceiveQueue);
  }

  [[nodiscard]] struct ibv_pd* getProtectionDomain() const {
    return tagSet_.protectionDomain();
  }

  [[nodiscard]] size_t numTags() const {
    return tagSet_.size();
  }

  // Posns across this QP's Tags that are still waiting to go out, which the
  // rate hint is derived from.
  [[nodiscard]] uint32_t getUnsentPosnCount() const {
    return tagSet_.getUnsentPosnCount();
  }

  [[nodiscard]] bool allTagsMarkedForDeletion() const {
    return tagSet_.allTagsMarkedForDeletion();
  }

  [[nodiscard]] std::vector<uint8_t> getTagNums() const {
    return tagSet_.getTagNums();
  }

  // Stop counting posns that were unsent work and will never be sent. TagSet
  // keeps that count for the rate hint.
  void subtractUnsentPosns(uint32_t count) {
    tagSet_.subtractUnsentPosns(count);
  }

  // For testing; see TagSet::allTagsInState().
  [[nodiscard]] bool allTagsInState(ibv_qp_state state) const {
    return tagSet_.allTagsInState(state);
  }

  // Signals inline on an error transition, so the caller must not hold a Tag
  // semaphore, sndCur or TagSet::mutex_.
  void setIbvQpState(ibv_qp_state state) {
    tagSet_.setIbvQpState(state);
    if (state == IBV_QPS_ERR) {
      // After setIbvQpState() has returned and dropped TagSet::mutex_: the
      // sender fails a tag in error at its next visit, and a QP with every
      // posn already claimed has no other event to make it ready.
      signalReadyAsync();
    }
  }

  // Caller holds the Tag's semaphore and must issue the returned wakeup once
  // it has dropped that semaphore and every guard enclosing it.
  [[nodiscard]] SenderWake enterTagErrorWithTagHeld(Tag& tag) {
    return tagSet_.enterTagErrorWithTagHeld(tag);
  }

  // Separate from enterTagErrorWithTagHeld() because signalling can block, and
  // the arbiter's consumers wait on the guards an error path holds.
  void wakeSenderAfterTagError(SenderWake wake) {
    if (wake == SenderWake::NEEDED) {
      signalReadyAsync();
    }
  }

  // Drops the work where teardown has closed admission.
  template <typename Awaitable>
  void addToScopeIfOpen(Awaitable&& awaitable) {
    std::lock_guard<std::mutex> admitted(scopeAdmission_);
    if (scopeAdmissionClosed_) {
      return;
    }
    asyncScope_.add(static_cast<Awaitable&&>(awaitable));
  }

  // Taken and released around the flag: the join must not hold it, since a
  // coroutine on the scope can spawn onto it again.
  void closeScopeAdmission() {
    std::lock_guard<std::mutex> admitted(scopeAdmission_);
    scopeAdmissionClosed_ = true;
  }

  // One-way, set first in ~Qp().
  [[nodiscard]] bool isTearingDown() const {
    return cancellationSource_.isCancellationRequested();
  }

  void startProcessingWqes() {
    addToScopeIfOpen(co_withExecutor(
        folly::getGlobalCPUExecutor(),
        folly::coro::co_withCancellation(
            cancellationSource_.getToken(), processWqes())));
  }

  folly::coro::Task<void> processWqes() {
    co_await tagSet_.processWqes(readySignal(), mtu_);
  }

  [[nodiscard]] bool postWqe(uint8_t tagNum, std::unique_ptr<Wqe> wqe) {
    // Read before the post below moves the WQE away.
    Wqe& posted = *CHECK_NOTNULL(wqe.get());
    const bool isReceive = posted.getWorkType() == Wqe::WorkType::kReceive;
    if (!tagSet_.postWqe(tagNum, std::move(wqe))) {
      // A refused post is not progress.
      return false;
    }
    // The deadline covers transmit work, which a receive post does not advance.
    if (!isReceive) {
      watchdog_.recordProgress();
    }
    return true;
  }

  [[nodiscard]] uint32_t getLocalQpNum() const {
    return localQpNum_;
  }

  void setRemoteQpNum(uint32_t remoteQpNum) {
    remoteQpNum_ = remoteQpNum;
  }
  [[nodiscard]] uint32_t getRemoteQpNum() const {
    return remoteQpNum_;
  }

  void updateRemoteActivePortSet(uint16_t reportedPortSet);

  [[nodiscard]] uint16_t getRemoteActivePortSet() const {
    return remoteActivePortSet_;
  }

  // Canonical topology API. The ids are the externally meaningful logical
  // ports; reusing an id with a different address/GID is rejected.
  void addPortPair(
      uint16_t localPortId,
      const folly::IPAddress& localAddr,
      const ibv_gid_entry& entry,
      uint16_t remotePortId,
      const folly::IPAddress& remoteAddr);

  // Verbs compatibility API. IBV_QP_AV supplies SGID/DGID addresses but no port
  // id, so this assigns local and remote ids by pair insertion order and dedups
  // repeated SGID/DGID triples. These synthetic ids do not correspond to the
  // peer's reported active-port bitset, so this overload is for single-pair
  // (single-AV) QPs; multiport configurations that rely on peer-driven failover
  // must use the id-explicit overload, whose ids match the peer's port
  // numbering.
  void addPortPair(
      const folly::IPAddress& localAddr,
      const ibv_gid_entry& entry,
      const folly::IPAddress& remoteAddr);

  // Test/simple UDP convenience API. This is equivalent to the verbs
  // compatibility overload with a default RoCEv2/UDP GID entry.
  void addPortPair(
      const folly::IPAddress& localAddr,
      const folly::IPAddress& remoteAddr) {
    ibv_gid_entry entry{};
    entry.gid_type = kGidTypeMetaRoceUdp;
    addPortPair(localAddr, entry, remoteAddr);
  }

  // portIdx is a dense internal remotePorts_ index. Use getRemotePortId() when
  // mapping to active-port bit positions or other protocol-visible ids.
  [[nodiscard]] const folly::IPAddress& getRemotePortAddr(
      uint16_t portIdx) const {
    DCHECK_LT(portIdx, remotePorts_.size())
        << "port index refers to a nonexistent remote port address";
    return remotePorts_[portIdx].addr;
  }

  // portIdx is a dense internal localPorts_ index.
  [[nodiscard]] const folly::IPAddress& getLocalPortAddr(
      uint16_t portIdx) const {
    DCHECK_LT(portIdx, localPorts_.size())
        << "port index refers to a nonexistent local port address";
    return localPorts_[portIdx].addr;
  }

  [[nodiscard]] const ibv_gid_entry& getLocalPortGid(uint16_t portIdx) const {
    DCHECK_LT(portIdx, localPorts_.size())
        << "port index refers to a nonexistent local port gid";
    return localPorts_[portIdx].gid;
  }

  [[nodiscard]] const PortPair& getPortPair(uint16_t portPairIdx) const {
    DCHECK_LT(portPairIdx, portPairs_.size())
        << "port pair index refers to a nonexistent port pair";
    return portPairs_[portPairIdx];
  }

  // Convert an internal local port index to its logical id.
  [[nodiscard]] uint16_t getLocalPortId(uint16_t portIdx) const {
    DCHECK_LT(portIdx, localPorts_.size())
        << "port index refers to a nonexistent local port";
    return localPorts_[portIdx].id;
  }

  // Convert an internal remote port index to its logical id. Active-port
  // bitsets are keyed by this id.
  [[nodiscard]] uint16_t getRemotePortId(uint16_t portIdx) const {
    DCHECK_LT(portIdx, remotePorts_.size())
        << "port index refers to a nonexistent remote port";
    return remotePorts_[portIdx].id;
  }

  [[nodiscard]] std::set<folly::IPAddress> getLocalAddrs() const {
    std::set<folly::IPAddress> addrs;
    for (const auto& port : localPorts_) {
      addrs.insert(port.addr);
    }
    return addrs;
  }
  [[nodiscard]] std::set<folly::IPAddress> getRemoteAddrs() const {
    std::set<folly::IPAddress> addrs;
    for (const auto& port : remotePorts_) {
      addrs.insert(port.addr);
    }
    return addrs;
  }
  [[nodiscard]] size_t getNumRemotePorts() const {
    return remotePorts_.size();
  }

  // Number of configured edges. This can differ from both local and remote
  // port counts for one-to-many, many-to-one, and sparse topologies.
  [[nodiscard]] size_t getNumPortPairs() const {
    return portPairs_.size();
  }

  void setupFlows(
      uint32_t txFlowCount,
      uint32_t rxFlowCount,
      Sender* sender = nullptr,
      SackRetransmissionConfig sackRetxConfig = {});

  void setMtu(uint16_t mtu) {
    mtu_ = mtu;
  }
  [[nodiscard]] uint32_t getMtu() const {
    return mtu_;
  }

  [[nodiscard]] MemoryRegions& getMemoryRegions() {
    return memoryRegions_;
  }

  // setupFlows() installs the sender before it builds the flows that reach it,
  // so a flow asking always finds one.
  [[nodiscard]] Sender& getSender() const {
    return *CHECK_NOTNULL(sender_);
  }

  // Installed by the constructor and never reset, so always non-null. Usable
  // only until removeQp() (see readySignal_); that lifetime is the arbiter's
  // contract, not something this check proves.
  [[nodiscard]] QpArbiterIface::ReadySignal& readySignal() const {
    return *CHECK_NOTNULL(readySignal_);
  }

  [[nodiscard]] int getNumPendingRWqes() const {
    return tagSet_.getNumPendingRWqes();
  }

  [[nodiscard]] size_t getNumPendingWqes() const {
    return tagSet_.getNumPendingWqes();
  }

  [[nodiscard]] bool hasPendingWqes() const {
    return tagSet_.hasPendingWqes();
  }

  // For testing. Wait for a specific number of WQEs to reach their Tags.
  [[nodiscard]] bool waitForWqesToBePosted(
      int numWqes,
      std::chrono::milliseconds timeoutMs = std::chrono::milliseconds(100)) {
    return tagSet_.waitForWqesToBePosted(numWqes, timeoutMs);
  }

  // For testing. Wait for a specific number of RWQEs to reach their Tags.
  [[nodiscard]] bool waitForRWqesToBePosted(
      int numWqes,
      std::chrono::milliseconds timeoutMs = std::chrono::milliseconds(10)) {
    return tagSet_.waitForRWqesToBePosted(numWqes, timeoutMs);
  }

  [[nodiscard]] HighResTimer::Callback* getHrnrTimerCallback() const {
    return hrnrTimerCallback_.get();
  }

  [[nodiscard]] HighResTimer::Callback* getBrnrTimerCallback() const {
    return brnrTimerCallback_.get();
  }

  [[nodiscard]] HighResTimer::Callback* getPacingTimerCallback() const {
    return pacingTimerCallback_.get();
  }

  void incrementHrnrPacketsReceived() {
    hrnrPacketsReceived_++;
  }

  [[nodiscard]] uint64_t getHrnrPacketsReceived() const {
    return hrnrPacketsReceived_;
  }

  [[nodiscard]] std::chrono::steady_clock::time_point getLastProgressTime()
      const {
    return watchdog_.getLastProgressTime();
  }

  // Returns true if the time since last progress exceeds the watchdog interval.
  [[nodiscard]] bool hasWatchdogExpired() const;

  // What the QP settles on the move to RTS, before any Tag can post a send:
  // the watchdog deadline, from every Tag, so the QP holds one view of it.
  void onEnteringRts();

  void configureWatchdogInterval(std::chrono::milliseconds intervalMs) {
    watchdog_.configureInterval(intervalMs);
  }

  [[nodiscard]] std::chrono::milliseconds getWatchdogIntervalMs() const {
    return watchdog_.getIntervalMs();
  }

  [[nodiscard]] HrnrState getHrnrState() const {
    return hrnrState_;
  }

  void setHrnrState(HrnrState newState);

  // Cancel RNR backoff and resume normal operation
  void cancelRnrBackoff(uint8_t tagNum);
  // An RNR-Cancel reports the csn the responder has a receive for; an ack
  // reaching the blocked msn reports none.
  void applyRnrCancel(
      uint8_t tagNum,
      std::optional<uint16_t> postedCsn,
      uint32_t nowNs);

  // Cancel the whole-QP HRNR backoff only, leaving per-tag BRNR blocks intact.
  void cancelHrnrBackoff();

  // Clears the block without resuming transmission.
  void clearHrnrBlock();

  // Blocks the QP with its backoff deadline and wake timer together, so a
  // block is never unarmed. The deadline and the state are unsynchronized: a
  // sender racing this sees a stale one of the two and probes an interval
  // early, or a visit late with the wake timer as the backstop.
  void enterHrnrBlock(uint32_t backoffUs);

  // Schedule an RNR-Cancel packet (to be sent by Qp::getNextPacketToSend()).
  // The posted CSN is the highest CSN this tag has a receive posted for, which
  // bounds how much the peer may send before risking another RNR. Where the tag
  // has none it is one below the tag's first CSN, read circularly like every
  // other sequence number rather than as the large value it wraps to.
  void scheduleRnrCancelPacket(uint8_t tag, uint16_t postedCsn);

  // Track that an RNR cancel needs to be sent for this QP (e.g., after an
  // HRNR NAK was sent before flows were set up).
  void setRnrCancelPending(bool pending);
  [[nodiscard]] bool hasRnrCancelPending() const;

  // Build RNR-Cancel packet
  std::unique_ptr<RnrCancelPacket> buildRnrCancelPacket(
      uint16_t flowId,
      uint32_t localQpNum,
      uint32_t remoteQpNum,
      uint8_t tag,
      uint16_t postedCsn);

  void setCongestionController(std::unique_ptr<CongestionController> cc) {
    congestionController_ = cc
        ? std::make_unique<SynchronizedCongestionController>(std::move(cc))
        : nullptr;
  }

  // RCN rate hint: per-QP share of NIC rate for congestion feedback.
  // updateRcnActiveState is called on every data packet; activation is inline
  // there (single site), while deactivateRcn() handles the three deactivation
  // sites (rate hint drop, WQE completion, destructor).
  [[nodiscard]] uint32_t getPerQpRateGbps() const {
    return rcnRateHint_ ? rcnRateHint_->getPerQpRateGbps()
                        : kDefaultRateHintGbps;
  }
  void updateRcnActiveState(uint32_t incomingRateHint);
  void checkRcnActiveOnWqeCompletion();

  // A zero rate lifts the limit and a zero burst takes the default, as in
  // ibv_qp_rate_limit_attr. Accepts any rate; callers enforce a floor.
  void setRateLimit(QpPacer::RateLimit rateLimit);

  [[nodiscard]] bool isRateLimited() const;

  // For testing: the rate currently programmed into the pacer.
  [[nodiscard]] QpPacer::RateLimit getEffectiveRateLimit() const;

  [[nodiscard]] RxFlow* FOLLY_NULLABLE getRxFlowById(uint16_t flowId);

  [[nodiscard]] TxFlow* FOLLY_NULLABLE getTxFlowById(uint16_t flowId);

  [[nodiscard]] uint32_t getNumTxFlows() const;

  [[nodiscard]] uint32_t getNumRxFlows() const;

  // Tells the sender a Tag has stopped being able to serve, so its next visit
  // reconsiders whether any is left. Called where a Tag is destroyed; the
  // error transition raises it from inside the TagSet.
  void notifyTagLeftService() {
    tagSet_.notifySenderTagLeftService();
  }

  // Abandoning a frame can resolve a message's last outstanding posn, so these
  // complete here. Outside sndCur, and Tag before retirement guard.
  void retireTagsWithAbandonedFrames(
      const std::bitset<kMetaRoceTagCount>& tagsWithAbandonedFrames);

  // Sweeps every flow, and retires the tags that settles.
  void abandonFramesOfFailingWqes();

  // Handle watchdog expiry: error every tag, abandon the frames its failing
  // work left on the wire, then flush what that did not settle. The flows stay
  // in service here; the sender takes them out once no tag survives.
  void processWatchdogExpiry();

  // Async version of processWatchdogExpiry for calling from timer callbacks.
  // Uses the async scope to avoid blocking the timer thread.
  void processWatchdogExpiryAsync();

  // Fails the work of every Tag that has spent its rnr retries. Returns the
  // wake the caller must issue, which the sender itself may drop: what records
  // that a sweep is needed is TagSet's tag-error latch.
  [[nodiscard]] SenderWake processRnrRetryExceeded();

  folly::coro::Task<void> co_signalReady() {
    co_await readySignal().co_signalReady();
  }

  // Signals without awaiting a coroutine, so timer callbacks and receiver
  // threads can call it -- though not while holding a Tag semaphore, sndCur or
  // TagSet::mutex_, which a build under arbiterMutex_ waits on.
  void signalReadyAsync() {
    if (isTearingDown()) {
      return;
    }
    if (FLAGS_libsoftmetaroce_lockfree_signal) {
      // Signal via the arbiter's lock-free intake instead of the mutex path.
      readySignal().postSignal();
      return;
    }
    if (readySignal().trySignalReady()) {
      return;
    }
    signalReadyFallbacks_.fetch_add(1, std::memory_order_relaxed);
    addToScopeIfOpen(
        folly::coro::co_withExecutor(
            folly::getGlobalCPUExecutor(),
            folly::coro::co_withCancellation(
                cancellationSource_.getToken(), co_signalReady())));
  }

  [[nodiscard]] uint64_t getSignalReadyFallbacks() const {
    return signalReadyFallbacks_.load(std::memory_order_relaxed);
  }

  // Received ACKs discarded because the ring was full, meaning the sender did
  // not visit this QP before the receiver filled it. Reported at teardown.
  [[nodiscard]] uint64_t getAcksDroppedOnFullRing() const {
    return soAcksDropped_.load(std::memory_order_relaxed);
  }

  // Apply the ACKs a receiver handed off for this QP, on the caller's thread.
  // Mutates transmit state and can retire WQEs, so it invalidates packets
  // already built for this QP. Returns whether any ring entries were consumed,
  // which is not the same as transmit state changing: a stale ACK is dropped.
  bool applyHandedOffAcks();

  /*
   * Check flows until we find one packet to send.
   */
  std::unique_ptr<struct EgressPacket> getNextPacketToSend();

  // Enqueue a received ACK or RNR-cancel for the sender to apply at its next
  // arbiter visit. Callers must hold receiveLock_ -- it keeps this QP's
  // received-event ring single-producer.
  void
  enqueueReceivedAck(uint16_t flowId, const AckHeader& ack, uint64_t recvNowNs);
  void enqueueReceivedRnrCancel(const RnrCancelHeader& rnrCancel);

  [[nodiscard]] bool usesSingleOwnerRxRing() const {
    return pendingRxEvents_ != nullptr;
  }

  // Checks if the msg corresponding to the incoming data packet is completed
  // and updates the state tracking the completion of msgs (ie WQE, CDMSN). If
  // completed, it posts a CQE.
  // False where the peer sent a posn this message cannot account for, which
  // is for the caller to answer with a NAK once it has dropped its guards.
  [[nodiscard]] bool checkForRxMessageCompletion(
      Wqe* wqe,
      const BaseHeader::Opcode::OperationType opType,
      const uint8_t tagNum,
      const uint16_t msn,
      const uint16_t posn);

  /*
   * Checks if all the packets for a wqe are acked and the message is
   * completed.
   */
  // Callers pass only frames the tracker has just moved out of UNACKED, and
  // never an empty batch.
  void checkForTxMessageCompletion(
      const std::vector<PacketId>& newlyAckedPackets);

  void updateQpRtt(uint32_t rttNs) {
    if (!haveQpRttSample_) {
      haveQpRttSample_ = true;
      if (FLAGS_libsoftmetaroce_rfc6298_first_rtt_sample) {
        estQpRttNs_ = rttNs;
        return;
      }
    }
    // EWMA: estQpRttNs_ += (rttNs - estQpRttNs_) >> alpha
    auto diff = static_cast<int64_t>(rttNs) - estQpRttNs_;
    estQpRttNs_ += diff >> kDefaultInverseLog2QpRttAlpha;
  }

  [[nodiscard]] uint32_t getEstQpRttNs() const {
    return estQpRttNs_;
  }

  // Serializes a QP's receive processing across reader threads, which
  // SO_REUSEPORT spreads its packets over; not taken with a single reader.
  // Lock ordering: callers release the qpNumToQp_ map lock before taking this;
  // the two are never held together, so no lock-acquisition-order deadlock.
  [[nodiscard]] std::mutex& receiveLock() {
    return receiveLock_;
  }

  // Serializes a receiver applying an ACK inline against the sender's arbiter
  // visit, the two writers of this QP's transmit state where the sender does
  // not own it alone; unengaged in the mode that hands ACKs over instead. It
  // sits under receiveLock_, which a multi-reader receiver is already holding
  // when it gets here, and above every transmit lock, which both sites take
  // under it.
  [[nodiscard]] std::unique_lock<std::mutex> lockForInlineAckApply() {
    return usesSingleOwnerRxRing() ? std::unique_lock<std::mutex>()
                                   : std::unique_lock<std::mutex>(txStateLock_);
  }

 private:
  // Port topology.
  // Assigns each flowId a configured pair by rotating through portPairs_. When
  // there are more pairs than flows, the surplus pairs get no flow at setup
  // and remain available as failover targets.
  std::vector<uint16_t> buildFlowIdToPortPairIdxMap(uint32_t maxFlowCount);
  [[nodiscard]] uint32_t getPortPairRotation() const;

  // Resolve a logical port id to the dense storage index used in PortPair.
  // Reusing an id is allowed only when it identifies the same endpoint as
  // before; this supports one-to-many/many-to-one pairs without silently
  // merging distinct logical ports that happen to share an address.
  // Also reports whether the port was newly inserted.
  [[nodiscard]] std::pair<uint16_t, bool> addOrGetLocalPort(
      uint16_t localPortId,
      const folly::IPAddress& localAddr,
      const ibv_gid_entry& entry);
  [[nodiscard]] std::pair<uint16_t, bool> addOrGetRemotePort(
      uint16_t remotePortId,
      const folly::IPAddress& remoteAddr);
  [[nodiscard]] bool isRemotePortActive(
      uint16_t remotePortIdx,
      uint16_t activePortSet) const;

  // Received-event ring.
  // Returns false if the ring was full; the caller counts the loss for its
  // event kind.
  bool enqueueRxEvent(PendingRxEvent&& event);

  // RCN rate hinting.
  void deactivateRcn();
  // Programs the pacer with the verbs-installed limit; a zero burst takes one
  // send batch of full-size frames.
  void refreshPacerRate();

  // Instrumentation.
  // Accumulate per-QP window-blocked time: open a stall while windowBlocked,
  // fold the finished interval into qwndBlockedNs_ when it clears.
  // Sender-thread only, so the qwndBlocked* counters need no synchronization.
  void foldQwndBlockedTime(bool windowBlocked);

  // Logs the single-owner transmit diagnostics line when this QP enqueued or
  // dropped, subject to the process-wide teardown budget. Returns whether it
  // logged, so the window-stall summary can reuse the budget decision.
  [[nodiscard]] bool logTeardownDiagnostics();

  // Injected dependencies (owned by MetaRoceVerbs unless noted).
  HighResTimer& retxTimer_;
  QpArbiterIface& arbiter_;
  // Readiness handle from arbiter_.addQp(); owned by the arbiter, valid until
  // removeQp(). Signal readiness through this handle directly.
  QpArbiterIface::ReadySignal* readySignal_{nullptr};
  MemoryRegions& memoryRegions_;
  // Aliases uniqueSender_ (this QP's Sender) or an externally supplied one.
  Sender* sender_;
  RcnRateHint* rcnRateHint_{nullptr};

  // Configuration. remoteQpNum_/mtu_ are set later (create-then-modify), so
  // unlike localQpNum_ they cannot be const.
  const uint32_t localQpNum_;
  uint32_t remoteQpNum_;
  uint16_t mtu_{kDefaultMtu};
  // Under gso, getNextPacketToSend holds a flow until it drains (window spent)
  // rather than rotating per send, so a turn's sends coalesce into a GSO run.
  const bool gsoEnabled_;
  // Ceiling on consecutive sends held on one tx flow under gso, taken from the
  // sender's per-turn cap at setupFlows. Bounds the hold: the window alone does
  // not, since acks refill it between turns.
  uint32_t maxHeldSendsPerFlow_{1};

  // Protocol state.
  // Set by the BRNR backoff timer, consumed by the sender at its arbiter visit.
  std::atomic<bool> rnrRetryExceededPending_{false};
  std::vector<TxFlow> txFlows_;
  std::vector<RxFlow> rxFlows_;
  // SPSC ring of receive events for the sender to process on an arbiter visit.
  // Allocated at setupFlows when --libsoftmetaroce_single_owner_tx is set.
  std::unique_ptr<folly::ProducerConsumerQueue<PendingRxEvent>>
      pendingRxEvents_;
  uint16_t nextFlowIdToSendOn_{0};
  // Consecutive sends already held on nextFlowIdToSendOn_; reset on advance.
  uint32_t sendsOnHeldFlow_{0};
  uint16_t nextFlowIdToAckOn_{0};
  uint64_t hrnrPacketsReceived_{0};
  HrnrState hrnrState_{HrnrState::NORMAL_TX};
  // Deadline (ClockSource ns) after which the sender leaves HRNR_BLOCKED_TX
  // for HRNR_PROBING.
  uint64_t hrnrBackoffDeadlineNs_{0};
  // Enqueued by the post coroutine and by any thread that finds a tag ready,
  // dequeued by the sender inside a build it may already be part way through.
  folly::Synchronized<std::queue<PendingRnrCancel>, std::mutex>
      pendingRnrCancels_;
  bool rnrCancelPending_{false};
  std::mutex receiveLock_;
  // The whole-QP transmit lock of lockForInlineAckApply(); unused when the
  // sender owns transmit state alone.
  std::mutex txStateLock_;
  // RCN rate hint state (the hint source, rcnRateHint_, is an injected
  // dependency above).
  bool rcnActive_{false};
  // Sticky one-way switch: once this QP's peer has signaled a non-zero rate
  // hint, switch from data-arrival activation to peer-driven activation for
  // this QP.
  bool usingPeerDrivenActivation_{false};
  // A leaf lock, taken by the send-path gate and by refreshPacerRate. Starts
  // unlimited.
  folly::Synchronized<QpPacer, std::mutex> pacer_{
      QpPacer{QpPacer::RateLimit{}}};
  // Inputs to the pacer's rate; a zero burst stands for the default. Lock
  // order: pacingInputs_ before pacer_.
  struct PacingInputs {
    QpPacer::RateLimit verbsLimit;
  };
  folly::Synchronized<PacingInputs, std::mutex> pacingInputs_;

  // QP-level smoothed RTT
  static constexpr uint16_t kDefaultInverseLog2QpRttAlpha = 4;
  uint32_t estQpRttNs_{FLAGS_libsoftmetaroce_initial_est_rtt_us * kNsPerUs};
  bool haveQpRttSample_{false};

  static constexpr uint64_t kPortReadyDebounceNs = 5'000'000;
  // Receiver writes, Sender reads
  std::atomic<uint16_t> remoteActivePortSet_{0};
  uint64_t lastPortDownNs_{0}; // debounce: time any port last went down
  // The vectors below are storage tables. Topology lives in portPairs_, and
  // protocol-visible active-port bits use RemotePort::id.
  std::vector<LocalPort> localPorts_;
  std::vector<RemotePort> remotePorts_;
  std::vector<PortPair> portPairs_;
  std::unordered_map<uint16_t, uint16_t> localPortIdToIdx_;
  std::unordered_map<uint16_t, uint16_t> remotePortIdToIdx_;

  // Private so setCongestionController() is the only way in. TxFlow is a
  // friend and reads it directly.
  std::unique_ptr<CongestionController> congestionController_;

  // Owned subsystems.
  folly::CancellationSource cancellationSource_;
  folly::coro::AsyncScope asyncScope_;
  // Every add() runs under this lock, which teardown closes before joining,
  // so permission and spawn are one step.
  std::mutex scopeAdmission_;
  bool scopeAdmissionClosed_{false};
  // Owns this QP's Sender when it creates its own; sender_ (above) aliases it.
  std::unique_ptr<Sender> uniqueSender_;
  std::unique_ptr<HighResTimer::Callback> hrnrTimerCallback_;
  std::unique_ptr<HighResTimer::Callback> brnrTimerCallback_;
  std::unique_ptr<HighResTimer::Callback> pacingTimerCallback_;
  TagSet tagSet_;
  // Thread-safe watchdog timer for detecting stalls (no ACKs for too long).
  WatchdogTimer watchdog_;

  // Instrumentation: optional diagnostics, grouped at the end; not required for
  // correctness.

  // Diagnostic counters (relaxed) for the single-owner-tx path. Drained/visits
  // gives the average ACKs applied per *draining* arbiter visit -- visits is
  // bumped only when at least one ACK was applied (deferral amortization).
  // Logged at QP destruction.
  std::atomic<uint64_t> soAcksEnqueued_{0};
  std::atomic<uint64_t> soAcksDrained_{0};
  std::atomic<uint64_t> soDrainVisits_{0};
  // ACKs shed because the ring was full: a host silently dropping ACKs shows up
  // here.
  std::atomic<uint64_t> soAcksDropped_{0};
  // RNR-cancels share the ring but are counted apart: they are rare, and
  // folding them into the ACK counts would skew the per-ACK averages above.
  std::atomic<uint64_t> soRnrCancelsEnqueued_{0};
  std::atomic<uint64_t> soRnrCancelsDropped_{0};
  // Cumulative receipt->process delay over drained ACKs: how long each ACK
  // waited in the ring before its RTT was measured. Over acksDrained this is
  // the average queueing delay deliberately kept out of the RTT.
  std::atomic<uint64_t> soAckRecvToProcessNs_{0};
  // Send-thread time in the drain loop, timed only under
  // --libsoftmetaroce_profile_send_egress and only on visits that apply an ACK.
  std::atomic<uint64_t> soAckApplyNs_{0};
  // Send-stall diagnostics: QP lifetime, used to express qwnd-blocked time as a
  // fraction of wall time.
  std::chrono::steady_clock::time_point createdAt_{
      std::chrono::steady_clock::now()};
  // Per-QP congestion-window-blocked time (aggregated per QP; the congestion
  // window itself is per-flow). qwndBlockedSinceNs_ == 0 means "not currently
  // blocked". Steady-clock ns (64-bit, no wrap). Sender-thread only; no
  // synchronization.
  uint64_t qwndBlockedNs_{0};
  uint64_t qwndBlockedSinceNs_{0};
  uint64_t qwndBlockedEvents_{0};
  // Counts signalReadyAsync calls that missed the try_lock fast path and fell
  // back to scheduling a coroutine -- a measure of arbiter-signal contention.
  std::atomic<uint64_t> signalReadyFallbacks_{0};
};

inline folly::IPAddress Flow::getRemoteAddr() const {
  return qp_.getRemotePortAddr(getRemotePortIdx());
}

inline uint16_t Flow::getLocalPortIdx() const {
  return qp_.getPortPair(portPairIdx_).localPortIdx;
}

inline uint16_t Flow::getRemotePortIdx() const {
  return qp_.getPortPair(portPairIdx_).remotePortIdx;
}

inline uint16_t Flow::getOriginalRemotePortIdx() const {
  return qp_.getPortPair(originalPortPairIdx_).remotePortIdx;
}

} // namespace libsoftmetaroce
