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

#include <algorithm>
#include <atomic>
#include <map>
#include <optional>
#include <shared_mutex>

#include <folly/ScopeGuard.h>
#include <folly/SharedMutex.h>
#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Task.h>
#include <folly/fibers/Semaphore.h>

#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/QpState.h>
#include <libsoftmetaroce/ReceiveSequence.h>
#include <libsoftmetaroce/RnrState.h>
#include <libsoftmetaroce/SharedReceiveQueue.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/WrappingCompare.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/ReadWriteHeader.h>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace libsoftmetaroce {

enum class RequestValidationError : uint8_t {
  kNone,
  kAddressError, // MR validation -> NAK_ADDRESS_INVALID_ACCESS_ERROR
  kInvalidRequest, // misalignment, length -> NAK_INVALID_REQUEST
  kOperationalError, // responder-local failure -> NAK_OPERATIONAL_ERROR
};

class Tag {
  // TagSet writes the error state below, making the publication a Tag cannot
  // make for itself.
  friend class TagSet;

 public:
  explicit Tag(
      uint32_t qpNum,
      uint8_t tagNum,
      CompletionQueue* sendCq,
      CompletionQueue* recvCq,
      struct ibv_qp* ibvQp,
      bool sqSigAll = false,
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr);
  explicit Tag(
      uint32_t qpNum,
      uint8_t tagNum,
      CompletionQueue* cq,
      struct ibv_qp* ibvQp,
      bool sqSigAll = false,
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr)
      : Tag(qpNum,
            tagNum,
            cq,
            nullptr,
            ibvQp,
            sqSigAll,
            maxInlineData,
            sendQueueDepth,
            receiveQueueDepth,
            sharedReceiveQueue) {}

  ~Tag();

  Tag(const Tag&) = delete;
  Tag& operator=(const Tag&) = delete;
  Tag(Tag&&) = delete;
  Tag& operator=(Tag&&) = delete;

  [[nodiscard]] uint32_t getSendQueueDepth() const {
    return sendQueueDepth_;
  }
  [[nodiscard]] uint32_t getReceiveQueueDepth() const {
    return receiveQueueDepth_;
  }
  [[nodiscard]] uint16_t getNextMsn() const;
  [[nodiscard]] bool setNextMsn(uint16_t v);
  [[nodiscard]] uint16_t getAndIncrementNextMsn();
  [[nodiscard]] uint16_t getAndIncrementNextCsn();
  [[nodiscard]] uint16_t getNextCsn() const;
  [[nodiscard]] bool setNextCsn(uint16_t v);
  [[nodiscard]] uint16_t getNextRxCsn() const;
  [[nodiscard]] uint16_t getCDMSN() const;
  // Returns false outside RESET, INIT, or RTR, leaving send numbering
  // unchanged.
  [[nodiscard]] bool setSendSequenceNumbers(SequenceNumbers numbering);
  // False once the tag can already receive, which is too late to renumber it.
  [[nodiscard]] bool setReceiveSequenceNumbers(SequenceNumbers numbering);
  [[nodiscard]] uint16_t getTxCompletedMsn() const;
  [[nodiscard]] int getNumPendingRWqes() const;
  [[nodiscard]] int getNumPendingWqes() const;
  // Claims a queue entry in the send depth this tag advertises; false when the
  // tag is already at that depth. The entry comes back when the work request
  // retires.
  bool tryReserveSendQueueEntry();
  void releaseSendQueueEntry();

  [[nodiscard]] bool postWillBeFlushed() const {
    return isInErrorState() || isMarkedForDeletion();
  }

  // Keep the tag from assigning a nextMsn that would evaluate as before the
  // next msn to retire.
  [[nodiscard]] bool hasMsnSpanRoom() const;
  folly::coro::Task<void> co_waitForMsnSpanRoom();
  void signalMsnSpanRoom() {
    msnSpanBaton_.post();
  }

  // Keep the tag from admitting more reads and atomics than the depth its
  // client negotiated, which is the peer's responder table.
  [[nodiscard]] bool hasReadAtomicRoom() const;
  folly::coro::Task<void> co_waitForReadAtomicRoom();
  void signalReadAtomicRoom() {
    readAtomicBaton_.post();
  }
  // A responder-created receive never passes through a post, so only what a
  // client posted is counted against the depth.
  bool tryReserveReceiveQueueEntry();
  void releaseReceiveQueueEntry();
  [[nodiscard]] int32_t getAcceptedReceiveQueueEntries() const;
  [[nodiscard]] int32_t getAcceptedSendQueueEntries() const;
  // Whether any work request is still waiting to complete. Msns are handed out
  // in sequence and commit in that order, so the work outstanding is the run
  // starting one past the last committed msn: ask whether that one is here.
  [[nodiscard]] bool hasPendingWqes() const;
  // Returns whether the post left the sender work to transmit. Caller holds
  // the Tag semaphore; a post landing on a tag already in error takes the
  // WQE-retirement guard here to retire settled work.
  [[nodiscard]] bool processTransmitWqe(std::unique_ptr<Wqe> wqe);
  // Caller holds the Tag semaphore. Returns the csn to send an RNR-Cancel
  // for, which is empty whenever none is due: a post the tag flushes, and the
  // ordinary post that finds no RNR-Cancel request waiting.
  [[nodiscard]] std::optional<uint16_t> processReceiveWqe(
      std::unique_ptr<ReceiveWqe> wqe,
      uint32_t mtu);
  // Creates a vRWQE for the first received write packet.
  void createVirtualRWqeIfNeeded(
      const uint16_t msn,
      const ReadWriteHeader& rwData,
      int* FOLLY_NONNULL addressError,
      MemoryRegions& memoryRegions,
      uint32_t mtu);
  // Creates a vSWQE upon arrival of a read request.
  void createVirtualSendWqeIfNeeded(
      const uint16_t msn,
      const ReadWriteHeader& rwData,
      int* FOLLY_NONNULL addressError,
      MemoryRegions& memoryRegions,
      uint32_t mtu);
  // Creates the response to a read-with-immediate request; acking it
  // completes the paired posted receive.
  void createVirtualReadImmResponseWqeIfNeeded(
      const uint16_t msn,
      const ReadWriteHeader& rwData,
      int* FOLLY_NONNULL addressError,
      MemoryRegions& memoryRegions,
      uint32_t mtu);
  // Creates a VirtualAtomicAckWqe upon arrival of an atomic request.
  void createVirtualAtomicAckWqeIfNeeded(
      const uint16_t msn,
      uint64_t origRemoteData);
  // address is the resolved address, or the wire value where no region of the
  // queue pair's domain resolved it.
  struct AtomicTarget {
    RequestValidationError error;
    uint64_t address;
  };
  // Validates an atomic request's target address for alignment and MR access.
  [[nodiscard]] AtomicTarget validateAtomicRequest(
      uint64_t targetAddr,
      uint32_t rkey,
      MemoryRegions& memoryRegions) const;
  // Creates a VirtualAtomicExecWqe to defer atomic execution until all prior
  // operations complete. Caller must validate the request first.
  void createVirtualAtomicExecWqeIfNeeded(
      uint16_t msn,
      bool isFetchAdd,
      uint64_t targetAddr,
      uint64_t swapOrAddData,
      uint64_t compareData);
  // The domain a receive is scattered under. A shared queue's receives were
  // registered in its domain, not in the drawing queue pair's.
  [[nodiscard]] struct ibv_pd* receiveProtectionDomain() const {
    return sharedReceiveQueue_ != nullptr
        ? sharedReceiveQueue_->getProtectionDomain()
        : ibvQp_.pd;
  }

  // Binds this msn's RWQE if not already bound; returns true iff this call
  // performed the bind, so the caller can validate the buffer once per message.
  // `mtu` is read only where the queue pair draws from a shared receive queue
  // and one is taken to answer this csn.
  bool assignInitiatorMsnToReceiveWqeIfNeeded(
      uint16_t csn,
      uint16_t msn,
      uint32_t mtu);
  // Like the two parameter version, but first validates ReadWriteHeader, then
  // populates the ReceiveWqe with the RDMA Write's address.
  void assignInitiatorMsnToReceiveWqeIfNeeded(
      uint16_t csn,
      uint16_t msn,
      const ReadWriteHeader& rwData,
      int* FOLLY_NONNULL addressError,
      MemoryRegions& memoryRegions,
      uint32_t mtu);
  // True if this MSN's receive WQE failed local-buffer validation, which leaves
  // the error on it for the flush to report; false if it validated or has no
  // applicable WQE (unknown MSN, virtual/non-receive, or zero-length).
  bool recordLocalProtectionErrorIfUnwritable(
      uint16_t msn,
      const MemoryRegions& memoryRegions);
  // Widens the window of messages that can be recorded until msn falls in it.
  void extendCdmsnTrackerIfNeeded(uint16_t msn);
  Wqe* FOLLY_NULLABLE findNextWqe();
  // Only a blocked, non-virtual request counts as a probe.
  bool isProbeWqe(const Wqe& wqe) const {
    return !wqe.isVirtual() && isBlockedMsn(wqe.getMsn().value());
  }
  // Caller holds the Tag semaphore and has built a packet from wqe.
  void markProbeSentIfBlockedMsn(const Wqe& wqe) {
    if (brnr_.state() == BrnrState::BRNR_PROBING && isProbeWqe(wqe)) {
      brnr_.probeSent();
    }
  }
  [[nodiscard]] std::optional<uint16_t> getBrnrFirstBlockedMsn() const;
  [[nodiscard]] bool isBlockedMsn(uint16_t msn) const;
  BrnrState getBrnrState() const;
  [[nodiscard]] BrnrBackoff& brnr() {
    return brnr_;
  }
  [[nodiscard]] const BrnrBackoff& brnr() const {
    return brnr_;
  }
  // Set from ibv_modify_qp's rnr_retry on this Tag's own ibv_qp, which is why
  // it lives here and not on the QP the tags share. IBTA 7 means unlimited.
  static constexpr uint8_t kRnrRetryInfinite = 7;
  void setRnrRetryLimit(uint8_t limit) {
    rnrRetryLimit_ = limit;
  }
  [[nodiscard]] uint8_t getRnrRetryLimit() const {
    return rnrRetryLimit_;
  }
  // Records the depth ibv_modify_qp's max_rd_atomic and max_dest_rd_atomic
  // asked for on this Tag's own ibv_qp.
  void setMaxRdAtomic(uint8_t depth) {
    maxRdAtomic_ = depth;
  }
  [[nodiscard]] uint8_t getMaxRdAtomic() const {
    return maxRdAtomic_;
  }
  void setMaxDestRdAtomic(uint8_t depth) {
    maxDestRdAtomic_ = depth;
  }
  [[nodiscard]] uint8_t getMaxDestRdAtomic() const {
    return maxDestRdAtomic_;
  }
  // Refills visits on change, so visits match the new weight.
  void setServiceLevel(uint8_t serviceLevel) {
    if (serviceLevel_ != serviceLevel) {
      serviceLevel_ = serviceLevel;
      refillVisits();
    }
  }
  [[nodiscard]] uint8_t getServiceLevel() const {
    return serviceLevel_;
  }
  // A level-zero tag still sends, so the weight is the level plus one.
  // Capped so the plus one cannot wrap to zero and exclude the tag.
  [[nodiscard]] uint8_t getWeight() const {
    return cappedWeight(getServiceLevel());
  }
  [[nodiscard]] uint8_t getVisitsLeft() const {
    return visitsLeft_;
  }
  void refillVisits() {
    visitsLeft_ = getWeight();
  }
  void decrementVisits() {
    if (visitsLeft_ > 0) {
      --visitsLeft_;
    }
  }
  void forfeitVisits() {
    visitsLeft_ = 0;
  }
  // Whether this tag has used every rnr_retry it was given. An infinite limit
  // never has. Being spent is arithmetic: read it only of a tag the receiver
  // has actually told to wait.
  [[nodiscard]] bool hasSpentRnrRetries() const {
    return rnrRetryLimit_ != kRnrRetryInfinite &&
        brnr_.retryCount() >= rnrRetryLimit_;
  }
  static constexpr uint8_t kDefaultTransportRetryCount = 7; // IBTA's default.
  void setLocalAckTimeoutExponent(uint8_t exponent) {
    localAckTimeoutExponent_ = exponent;
  }
  [[nodiscard]] std::optional<uint8_t> getLocalAckTimeoutExponent() const {
    return localAckTimeoutExponent_;
  }
  void setTransportRetryCount(uint8_t count) {
    transportRetryCount_ = count;
  }
  [[nodiscard]] uint8_t getTransportRetryCount() const {
    return transportRetryCount_;
  }
  struct ibv_qp& getIbvQp() const;
  // The inline-data cap this Tag was granted; an inline send or write payload
  // posted to it may hold at most this many bytes.
  [[nodiscard]] uint32_t getMaxInlineData() const {
    return maxInlineData_;
  }
  [[nodiscard]] bool getSqSigAll() const {
    return sqSigAll_;
  }
  // Read under no lock, so it can lag a transition another thread has made.
  // The load is atomic and the error state is terminal, so a stale read says
  // "not yet" and never "no".
  [[nodiscard]] bool isInErrorState() const;
  // Whether an error NAK built within kErrorNakIntervalNs before nowNs holds
  // this tag's next one back.
  [[nodiscard]] bool isErrorNakHeld(uint64_t nowNs) const;
  // Whether this tag has ever sent an error NAK, answered without the clock
  // read isErrorNakHeld() needs.
  [[nodiscard]] bool hasSentErrorNak() const;
  void holdErrorNaks(uint64_t nowNs);
  // Whether this work request's frames must not go back on the wire: it is
  // failing, or its tag is in error and the sender's next visit will mark it.
  // Read under the Tag's semaphore, which the work request half requires.
  [[nodiscard]] bool mustStopRetransmitting(const Wqe& wqe) const;
  [[nodiscard]] uint8_t getTagNum() const;

  // Returns the WQE corresponding to the given MSN. If the MSN is not found,
  // returns nullptr. It uses opType to lookup the MSN in the correct map.
  Wqe* FOLLY_NULLABLE getReceiveWqe(
      const BaseHeader::Opcode::OperationType opType,
      const uint16_t msn);
  [[nodiscard]] bool markDeliveredAndCompleteInOrder(const uint16_t msn);
  // Reports a receive completing on a csn the sequence does not reach next,
  // bounded so a peer cannot flood the log.
  void logCsnOutOfSequence(const uint16_t msn, const uint16_t csn) const;

  [[nodiscard]] bool handleRxWqeCompletion(
      Wqe& wqe,
      const BaseHeader::Opcode::OperationType opType,
      const uint16_t msn);
  // Marks delivered, and completes in order, the receive-side work at each
  // acked send's msn. False where one of those receives completes on a csn out
  // of sequence, which the caller fails the tag on.
  [[nodiscard]] bool completeReceivesReleasedBySends(
      const std::vector<std::reference_wrapper<Wqe>>& sends);

  // Caller holds the WQE-retirement guard. False where a completed send
  // released a receive out of sequence: fail the tag and flush.
  [[nodiscard]] bool handleTxWqesCompletion(
      const std::vector<std::reference_wrapper<Wqe>>& wqesToComplete);

  // The following two functions form a mutex that indicates that the Tag is
  // engaged in a long-lived operation (such as being used to send a WQE). A raw
  // mutex or folly::Synchronized would be inconvenient because the Tag (or WQE)
  // may be used by multiple entities throughout that operation. The primary use
  // is to keep a Tag's WQEs from being retired while a packet for one of them
  // is being sent or prepared for retransmission.
  // See below for how to determine if a Tag has been marked for deletion.
  void waitForFree();
  // The same acquire for a caller on an executor, which must not block its
  // thread while the tx path holds the Tag to build a packet.
  folly::coro::Task<void> co_waitForFree();
  void markFree();

  // RAII scoped acquire of the busy semaphore above, for call sites that
  // acquire and release it within a single scope.
  [[nodiscard]] auto scopedLock() {
    waitForFree();
    return folly::makeGuard([this] { markFree(); });
  }

  // The guard between receive processing and WQE retirement: held shared while
  // the receive path uses a Wqe* this Tag owns, exclusively while the Tag moves
  // to error or retires one. It is not a data lock -- a receiver still binds
  // and erases under it, which is safe only because one thread owns the receive
  // side -- and it has no upgrade, so a receiver that decides the Tag must fail
  // applies that after releasing. Taken after the Tag semaphore.
  class ReceiveAccess {
   public:
    explicit ReceiveAccess(Tag& tag)
        : lock_(tag.wqeRetirementMutex_), tag_(tag) {}

    // Every path that fails a Tag sets its state under this same guard, so the
    // answer cannot go stale before the use it covers. A client erroring its
    // own QP through the verbs API writes that state under the Tag map
    // instead, unordered against this, and retires no WQE; a post landing on
    // that Tag afterward retires the settled ones, under the guard.
    [[nodiscard]] bool isTagInError() const {
      return tag_.isInErrorState();
    }

   private:
    std::shared_lock<folly::SharedMutex> lock_;
    const Tag& tag_;
  };
  [[nodiscard]] ReceiveAccess accessForReceive() {
    return ReceiveAccess(*this);
  }

  // The exclusive counterpart, for moving this Tag to the error state and for
  // retiring any WQE it owns.
  [[nodiscard]] std::unique_lock<folly::SharedMutex> lockForWqeRetirement() {
    return std::unique_lock<folly::SharedMutex>(wqeRetirementMutex_);
  }

  // markForDeletion() half-closes the Tag (FIN-WAIT-like): it takes no new work
  // but carries on with the work it already accepted -- sending what is left of
  // it as well as retransmitting what is on the wire -- and lingers in the
  // TagSet until the whole QP is torn down, since readers may still dereference
  // it until then.

  // Concurrency: read under the Tag's semaphore, which is what the paths that
  // set it hold. A marked Tag refuses new work.
  bool isMarkedForDeletion() const;
  void markForDeletion();

  // Marks the work request failing and reports it through the in-order path
  // once its frames settle, recording `status` unless it already holds one.
  // Returns the posns abandoned, which the caller takes off the unsent count.
  // Report it before moving the tag to error.
  // Caller holds the Tag semaphore and the WQE-retirement guard exclusively.
  uint32_t completeWqeThatFailsTheTag(
      Wqe& wqe,
      enum WorkCompletionStatus status);

  // Complete every pending send and receive WQE with WR_FLUSH_ERR. Used on the
  // error path (watchdog) and on teardown; does not require or change the ibv
  // QP state, but does advance msnSpan_.inOrderCompletedMsn past everything
  // flushed,
  // so a later completion does not wait behind msns that will never come.
  // Caller must hold the WQE-retirement guard exclusively: the receive WQEs
  // freed here are the ones the receive path scatters into. It frees the send
  // WQEs without resolving their frames, so no tx tracker may still hold one:
  // the caller clears the trackers first, or has quiesced everything that
  // would follow an entry.
  void flushAllPendingWqesInError();

  // Complete the pending receive WQEs only. A receive has no packet on the
  // wire to wait for, unlike a send. Caller must hold the Tag semaphore and the
  // WQE-retirement guard exclusively.
  void flushPendingReceiveWqesInError();

  // Caller holds the Tag semaphore, has errored the tag and released the
  // pending-receive lock. Takes the retirement guard itself.
  void flushReceivesForStrandedCsn(uint16_t csn, ReceiveWqe& rejectedPost);

  // Marks this tag's pending work requests failing and completes those already
  // finished. Returns the posns it abandoned: a Tag cannot reach the TagSet
  // that counts them, so its owner subtracts them. Takes the retirement guard.
  uint32_t failPendingWqesIfInErrorWithTagHeld();

  // Mark every WQE this tag still holds failing, leaving the QP's other tags
  // untouched. The earliest pending WQE not already failing reports `status`,
  // the rest flush, and each reports in posting order once it has no posn
  // outstanding. Returns the number of posns abandoned, for the caller to
  // subtract from the unsent-posn count. Hold the Tag semaphore.
  uint32_t markPendingWqesFailing(WorkCompletionStatus status);
  // Hand every failing work request with no posn outstanding to in-order
  // completion, whether it holds the error or is flushing behind one.
  // Caller must hold the Tag semaphore and the WQE-retirement guard
  // exclusively.
  void completeSettledFailedWqes();
  // Takes the span's lock; a caller already holding it asks the span instead.
  [[nodiscard]] bool isTxCompletionCommitted(uint16_t msn) const;
  // Retire a response-driven WQE whose request frame the caller has just
  // settled by ack or abandonment, if no other claim remains. Caller must
  // hold the Tag semaphore and the WQE-retirement guard exclusively.
  void retireAfterRequestFrameSettled(Wqe& wqe);

  // How many send WQEs the tag still holds, counting those already reported
  // and kept only for a request frame. getNumPendingWqes() excludes those, so
  // it cannot see one that is retained after it should have been retired.
  [[nodiscard]] size_t getNumHeldTxWqes() const {
    return msnSpan_.rlock()->wqes.size();
  }

  // The map lock is released on return, so the caller must hold the Tag
  // semaphore and the WQE-retirement guard for as long as it uses the pointer.
  Wqe* FOLLY_NULLABLE getPendingWqe(uint16_t msn);

  // Returns the earliest posted pending (non-virtual) WQE, or nullptr if none.
  // Earliest is by sequence order, which is not numeric order across a wrap.
  // Used by watchdog expiry to complete the earliest WQE with retry exceeded
  // error.
  Wqe* FOLLY_NULLABLE getEarliestPendingWqe();

  // Returns true if any receive WQE has received some but not all of its
  // data (i.e., is actively in-progress).
  [[nodiscard]] bool hasPartiallyReceivedRWqe() const;

  // Check if all prior WQEs (MSN < currentMsn) have completed in-order.
  // Used by fence logic to determine if a fenced WQE can start transmitting.
  // Returns true only when msnSpan_.inOrderCompletedMsn == currentMsn - 1,
  // meaning every earlier WQE has been ACKed and completed.
  // Logs an error if msnSpan_.inOrderCompletedMsn has advanced past
  // currentMsn - 1,
  // which would indicate the fenced WQE itself has already been completed.
  bool arePriorWqesCompleteForFence(uint16_t currentMsn) const;

 private:
  static uint8_t cappedWeight(uint8_t serviceLevel) {
    return static_cast<uint8_t>(std::min(serviceLevel, kMaxServiceLevel) + 1);
  }

  [[nodiscard]] uint16_t getAndIncrementNextRxCsn();

  // For one message the cumulative delivered msn has run over. False where
  // that receive's csn is not the one after the completed edge.
  [[nodiscard]] bool completeDeliveredReceive(uint16_t msn);

  // The address to respond from, or nullopt when no response should be
  // stored; only the unresolvable case sets addressError.
  std::optional<char*> addressForVirtualResponseWqe(
      const uint16_t msn,
      const ReadWriteHeader& rwData,
      int* FOLLY_NONNULL addressError,
      MemoryRegions& memoryRegions);

  // The write itself is atomic, but only the owner's guard orders it against
  // the work it fails, so the owner must call
  // TagSet::notifySenderTagEnteredError() before dropping that guard, or no
  // sweep comes to fail the work this Tag still holds.
  void setErrorStateWithoutNotification() {
    storeQpErrorState(ibvQp_);
  }

  // The msns this tag has issued and the work they identify. One lock covers
  // all of it, so inOrderCompletedMsn and the work it bounds are never read a
  // moment apart.
  struct MsnSpan {
    explicit MsnSpan(uint16_t next)
        : nextMsn(next),
          inOrderCompletedMsn(static_cast<uint16_t>(next - 1)),
          inOrderRetiredMsn(static_cast<uint16_t>(next - 1)) {}

    // Client-posted WQEs, keyed by the msn issued at post.
    std::map<uint16_t, std::unique_ptr<Wqe>> wqes;
    // Next msn to issue. Assigned only from TagSet::processWqes().
    uint16_t nextMsn;
    // Through which msn work has completed in order: it has taken its place in
    // msn order and been reported if it had a client to tell.
    uint16_t inOrderCompletedMsn;
    // Through which msn the tag has erased the WQE, so the left edge of wqes.
    // Trails inOrderCompletedMsn while one is held for its request frame.
    uint16_t inOrderRetiredMsn;

    // True once this msn has taken its place in msn order, so a WQE still here
    // for it lingers only for its request frame.
    [[nodiscard]] bool isTxCompletionCommitted(uint16_t msn) const {
      return !after(msn, inOrderCompletedMsn);
    }

    // The next msn is usable while it still evaluates as after the retired one.
    [[nodiscard]] bool hasRoom() const {
      return after(nextMsn, inOrderRetiredMsn);
    }

    // Msns are handed out in sequence and commit in that order, so the
    // pending work is the run starting one past the committed msn.
    [[nodiscard]] bool hasPending() const {
      return wqes.contains(static_cast<uint16_t>(inOrderCompletedMsn + 1));
    }

    // Every msn between the cursors is in the map, so their distance counts
    // the committed ones.
    [[nodiscard]] int countPending() const {
      const uint16_t committed =
          static_cast<uint16_t>(inOrderCompletedMsn - inOrderRetiredMsn);
      DCHECK_LE(committed, wqes.size())
          << "the committed run is part of the map, so counting more of it "
             "than the map holds would wrap the pending count";
      const int pending = static_cast<int>(wqes.size() - committed);
      DCHECK(pending == 0 || hasPending())
          << "the work still pending runs from one past the last committed "
             "msn; a gap at its start would leave a walk from there reaching "
             "none of it";
      return pending;
    }
  };

  // The WQEs this tag has yet to complete, in posting order; the caller holds
  // the msn span. Collected, so the caller may alter the map afterward.
  std::vector<std::reference_wrapper<Wqe>> pendingWqesInMsnOrder(
      const MsnSpan& lockedSpan) const;

  Wqe* findNextWqeInMap(
      const std::map<uint16_t, std::unique_ptr<Wqe>>& readLockedWqeMap,
      bool msnsAreRemote,
      uint16_t startMsn);
  [[nodiscard]] Cqe cqeForWqe(const Wqe& wqe, WorkCompletionStatus status)
      const;
  void enqueueCqeForWqe(const Wqe& wqe, WorkCompletionStatus status);

  // Answers the queue a completion of this kind goes to, or null once this Tag
  // has given its references back, when a destroy may already have freed it.
  [[nodiscard]] CompletionQueue* FOLLY_NULLABLE
  completionQueue(bool forReceive);
  // Drops this Tag's reference on each of its two queues, and is idempotent.
  // Allows the completion queues to be destroyed before the Tag itself is.
  void releaseCompletionQueueReferences();
  // Erase the contiguous run of completed WQEs at the map's head, advancing
  // inOrderRetiredMsn over it. Stops at one whose request frame the tracker
  // still holds. Answers whether it erased any. Caller holds the msn span's
  // wlock.
  bool retireCompletedWqesAtMapHead(
      folly::Synchronized<MsnSpan>::LockedPtr& lockedSpan);

  // The caller's span lock keeps `wqe` alive across the enqueue.
  void completeSingleTxWqe(
      const Wqe& wqe,
      WorkCompletionStatus status,
      folly::Synchronized<MsnSpan>::LockedPtr& lockedSpan);

  using PendingReceiveWqes =
      folly::F14FastMap<uint16_t, std::unique_ptr<ReceiveWqe>>;

  struct PendingReceives {
    // Posted receives waiting for their initiator's data packet, keyed by csn.
    // A bound receive moves to initiatorMsnToResponderWqes_.
    PendingReceiveWqes receivesByCsn;
    // Coalesced RNR-Cancel request from dedicated-queue receive misses whose
    // frames are recorded BRNR.
    bool rnrCancelRequested{false};
  };

  // Records the request a receive miss leaves for the next receive posted to
  // this tag to clear. A tag drawing from the shared queue records none.
  void recordDedicatedReceiveCancelRequest(PendingReceives& receives);

  // Null where the queue holds no receive this csn may take. Called with the
  // map it will be bound in write-locked.
  // `refusalRaisesBrnr` is false where validation has already chosen a NAK,
  // so a refused take leaves no record for a later post to cancel for.
  [[nodiscard]] std::unique_ptr<ReceiveWqe> takeReceiveFromSharedQueue(
      uint16_t csn,
      uint32_t mtu,
      bool refusalRaisesBrnr);
  // The receive `csn` binds to, taking one from the shared queue where
  // `pending` holds none. `pending.end()` where neither has one. Write-locked.
  [[nodiscard]] PendingReceiveWqes::iterator findReceiveTakingFromSharedQueue(
      PendingReceiveWqes& pending,
      uint16_t csn,
      uint32_t mtu,
      bool refusalRaisesBrnr);

  // Injected dependencies.

  // Null during teardown, once this tag's reference is dropped; the queues
  // may be destroyed ahead of the tag.
  CompletionQueue* FOLLY_NULLABLE sendCq_;
  CompletionQueue* FOLLY_NULLABLE recvCq_;
  struct ibv_qp& ibvQp_;
  // Null where the queue pair posts its own receives.
  SharedReceiveQueue* const FOLLY_NULLABLE sharedReceiveQueue_;

  // Configuration, set once at construction.
  const uint32_t qpNum_;
  const uint8_t tagNum_;
  // qp_init_attr.sq_sig_all: complete every successful send WR.
  const bool sqSigAll_;
  // qp_init_attr.cap.max_inline_data, as granted at creation. Each Tag is
  // created by its own verbs call and negotiates its own cap, so this belongs
  // to the Tag rather than to the QP they share.
  const uint32_t maxInlineData_;
  // The depths acceptedSendWqes_ and acceptedReceiveWqes_ are held to.
  const uint32_t sendQueueDepth_;
  const uint32_t receiveQueueDepth_;

  // The verbs attribute is accepted only before RTS and a BRNR needs traffic,
  // so this is frozen before any read and needs no synchronization.
  uint8_t rnrRetryLimit_{kRnrRetryInfinite};
  // Defaults to the advertised ceiling, so a client that never sets the mask
  // bit is read as permitting the full depth.
  uint8_t maxRdAtomic_{kMaxOutstandingReadAtomic};
  uint8_t maxDestRdAtomic_{kMaxOutstandingReadAtomic};
  // Frozen at RTR entry like the other setup values, so it needs no
  // synchronization.
  uint8_t serviceLevel_{0};

  // Empty when the client set no IBV_QP_TIMEOUT on this Tag.
  std::optional<uint8_t> localAckTimeoutExponent_;
  uint8_t transportRetryCount_{kDefaultTransportRetryCount};

  // Protocol state.

  // Under the TagSet lock, except the setup refill, which predates serving
  // like the other setup writes.
  uint8_t visitsLeft_{1};
  ReceiveSequence receiveSequence_;
  // Reads and atomics the msn span holds, written under its lock. One
  // processWqes() coroutine per QP counts in, so a room check may read stale.
  std::atomic<uint16_t> outstandingReadAtomic_{0};
  // Next completion sequence number to assign.
  uint16_t nextCsn_{kUnconfiguredStartingCsn};

  folly::Synchronized<MsnSpan> msnSpan_;
  // Send work requests accepted and not yet completed.
  std::atomic<int32_t> acceptedSendWqes_{0};
  // Receive work requests accepted and not yet completed.
  std::atomic<int32_t> acceptedReceiveWqes_{0};
  // The protocol time before which this tag sends no further error NAK. Only
  // the receive path, which handles one packet of a QP at a time, touches it.
  uint64_t nextErrorNakAtNs_{0};

  folly::Synchronized<PendingReceives> pendingReceives_;
  // Posted receives, virtual receives for writes, and atomic exec WQEs,
  // keyed by the initiator's msn.
  folly::F14FastMap<uint16_t, std::unique_ptr<Wqe>>
      initiatorMsnToResponderWqes_;
  // Stores VirtualSendWqes, keyed by the initiator's msn.
  folly::Synchronized<std::map<uint16_t, std::unique_ptr<Wqe>>> remoteMsnToWqe_;
  BrnrBackoff brnr_;

  // Track whether the Tag is busy, meaning that one of its WQEs is currently
  // being processed for the purpose of sending a packet. This is used to
  // guarantee that we do not retire a WQE while it is in use.
  // 1 == Tag is available for use.
  // 0 == Tag is busy.
  folly::fibers::Semaphore freeSem_{1};
  folly::coro::Baton msnSpanBaton_;
  folly::coro::Baton readAtomicBaton_;

  // The receive/WQE-retirement guard; see accessForReceive() above.
  folly::SharedMutex wqeRetirementMutex_;

  // Whether the tagged ibv_qp has been destroyed; see markForDeletion() above.
  folly::Synchronized<bool> markedForDeletion_{false};
};

} // namespace libsoftmetaroce
