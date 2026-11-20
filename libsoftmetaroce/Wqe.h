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
#include <glog/logging.h>
#include <algorithm>
#include <bit>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

#include <fmt/core.h>
#include <fmt/format.h>
#include <string_view>

#include <folly/Range.h>
#include <folly/io/IOBuf.h>
#include <infiniband/verbs.h>

#include <libsoftmetaroce/Cqe.h>
#include <libsoftmetaroce/PosnAuditor.h>
#include <libsoftmetaroce/RejectedPosnSet.h>
#include <libsoftmetaroce/SgeList.h>

namespace libsoftmetaroce {

// Whether a work request holds a PosnAuditor beside its counts.
bool posnAuditingEnabled();

class OversizeWqeException : public std::runtime_error {
 public:
  OversizeWqeException(uint32_t messageSize, uint32_t packetCount)
      : std::runtime_error(
            fmt::format(
                "Oversize WQE: message size {} bytes requires {} packets (max packets per message: {})",
                messageSize,
                packetCount,
                kMaxPacketsPerMessage)) {}

  static constexpr uint32_t kMaxPacketsPerMessage = 1 << 16;
};

struct Wqe {
  // Atomic WQEs always operate on 8 bytes, so any valid MTU produces 1 POSN.
  // Use a fixed value to avoid threading MTU through atomic WQE constructors.
  static constexpr uint16_t kAtomicMtu = 256;

  enum class WorkType {
    kSend,
    kSendWithImm,
    kReceive,
    kRead,
    kReadWithImm,
    kWrite,
    kWriteWithImm,
    kVirtualReceive, // created at responder for write
    kVirtualSend, // created at responder for read
    kAtomicFetchAdd,
    kAtomicCmpSwp,
    kVirtualAtomicExec, // created at responder for deferred atomic execution
    kVirtualAtomicAck, // created at responder for atomic ack response
  };

  enum class PosnStatus : uint8_t {
    PENDING, // not yet sent
    ALLOCATED, // sent, in the tx tracker, no response yet
    REJECTED, // sent, brnr'd or path down, allocate again
    ACKED, // peer sent a good ack/sack
    ABANDONED, // off the count with no peer ack
  };

  friend std::ostream& operator<<(std::ostream& os, const PosnStatus& status) {
    switch (status) {
      case PosnStatus::PENDING:
        return os << "PENDING";
      case PosnStatus::ALLOCATED:
        return os << "ALLOCATED";
      case PosnStatus::REJECTED:
        return os << "REJECTED";
      case PosnStatus::ACKED:
        return os << "ACKED";
      case PosnStatus::ABANDONED:
        return os << "ABANDONED";
    }
    return os << "UNKNOWN";
  }

  // Work requests are identified by the tag and msn that own them: two distinct
  // requests can agree on every field compared here.
  bool operator==(const Wqe&) const = delete;

  friend std::ostream& operator<<(std::ostream& os, const Wqe& wqe);
  friend std::ostream& operator<<(
      std::ostream& os,
      const Wqe::WorkType& workType);

  [[nodiscard]] WorkType getWorkType() const {
    return workType_;
  }
  [[nodiscard]] const std::optional<uint64_t>& getWorkReqId() const {
    return maybeWorkReqId_;
  }

  [[nodiscard]] const std::optional<uint16_t>& getMsn() const {
    return maybeMsn_;
  }

  [[nodiscard]] const std::optional<uint16_t>& getCsn() const {
    return maybeCsn_;
  }

  void setCsn(uint16_t csn) {
    CHECK(!maybeCsn_.has_value()) << "CSN must not be changed once set.";
    maybeCsn_ = csn;
  }

  void setMsn(uint16_t msn) {
    CHECK(!maybeMsn_.has_value()) << "MSN must not be changed once set.";
    maybeMsn_ = msn;
  }

  [[nodiscard]] uint32_t getMessageSizeBytes() const {
    return sizeBytes_;
  }

  // With several sges, the first non-empty one's address.
  [[nodiscard]] void* getBufferAddress() const {
    return address_;
  }

  [[nodiscard]] bool hasLocalSges() const {
    return segmented_ != nullptr;
  }

  [[nodiscard]] const LocalSges& getLocalSges() const {
    DCHECK(hasLocalSges())
        << "a contiguous buffer keeps no sge list; check hasLocalSges()";
    return segmented_->sges;
  }

  // The posn a request frame carries on the wire. It points to the start of
  // the response the responder will send back.
  static constexpr uint32_t kRequestFrameWirePosn = 0;

  // The one frame a read or atomic sends is not one of the posns its
  // response will fill, so it has a status of its own.
  [[nodiscard]] bool hasRequestFrame() const {
    return workType_ == WorkType::kRead ||
        workType_ == WorkType::kReadWithImm ||
        workType_ == WorkType::kAtomicFetchAdd ||
        workType_ == WorkType::kAtomicCmpSwp;
  }

  [[nodiscard]] bool canHaveSendPayload() const {
    // Read and atomic request frames carry no payload bytes.
    if (hasRequestFrame()) {
      return false;
    }

    // Responder atomic operations put their values in headers.
    if (workType_ == WorkType::kVirtualAtomicAck ||
        workType_ == WorkType::kVirtualAtomicExec) {
      return false;
    }

    // Other work types may carry payload bytes.
    return true;
  }

  [[nodiscard]] bool posnsAreReceived() const {
    return hasRequestFrame() || workType_ == WorkType::kReceive ||
        workType_ == WorkType::kVirtualReceive;
  }

  [[nodiscard]] uint32_t getReceivedPosnCount() const {
    return receivedPosnCount_;
  }

  [[nodiscard]] bool everyPosnArrivedOnce() const {
    return receivedPosnCount_ == numPosn_ &&
        receivedPosnSum_ ==
        static_cast<uint64_t>(numPosn_) * (numPosn_ - 1) / 2;
  }

  // Posns the sender will put on the wire, which is what the arbiter is told
  // to expect: one frame for a request, every posn of a message.
  [[nodiscard]] uint32_t getSendablePosnCount() const {
    return hasRequestFrame() ? 1 : static_cast<uint32_t>(numPosn_);
  }

  // Only the sender's own frames reach the tracker, so a tracker posn on a
  // read or atomic identifies its request frame, never a response posn. A
  // resolved posn also reads true, so ask only about a frame the tracker
  // still holds.
  [[nodiscard]] bool isFrameAllocated(uint32_t posn) const {
    if (hasRequestFrame()) {
      return requestFrameStatus_ == PosnStatus::ALLOCATED;
    }
    const bool allocated = posn < sndNxtPosn_ && !rejectedPosns_.contains(posn);
    if (auditor_ != nullptr) {
      CHECK_EQ(
          allocated, auditor_->stateOf(posn) == PosnAuditor::State::ALLOCATED)
          << "posn " << posn << " is "
          << PosnAuditor::name(auditor_->stateOf(posn))
          << ", which the cursor and the rejected bits read differently";
    }
    return allocated;
  }

  void abandonFrame(uint32_t posn) {
    if (hasRequestFrame()) {
      DCHECK(!isRequestFrameResolved())
          << "Attempted to abandon a request frame that is already resolved; "
          << "a frame reaches exactly one terminal state";
      requestFrameStatus_ = PosnStatus::ABANDONED;
      return;
    }
    abandonPosn(posn);
  }

  void releaseFrame(uint32_t posn) {
    DCHECK(!isFailing_)
        << "a failing WQE must abandon a frame rather than release it for "
           "retry; no flow is going to send it again";
    if (hasRequestFrame()) {
      DCHECK_EQ(requestFrameStatus_, PosnStatus::ALLOCATED)
          << "Can only release an ALLOCATED request frame";
      requestFrameStatus_ = PosnStatus::REJECTED;
      return;
    }
    releasePosn(posn);
  }

  // Callers hand over frames the tracker has just moved out of UNACKED, so a
  // frame arrives here exactly once; acking one twice would take its posn
  // off the outstanding count a second time.
  void onFrameNewlyAcked(uint32_t posn) {
    if (hasRequestFrame()) {
      DCHECK_EQ(posn, kRequestFrameWirePosn)
          << "A read or atomic sends one frame, and that frame carries posn zero";
      if (requestFrameStatus_ != PosnStatus::ALLOCATED) {
        // A BRNR handed this frame back for a resend, so the ack points to
        // a frame that is no longer the one on the wire.
        return;
      }
      requestFrameStatus_ = PosnStatus::ACKED;
      return;
    }
    acknowledgePosn(posn);
  }

  // A read or atomic is finished by the response that fills its posns, so an
  // ack never finishes a read successfully.
  [[nodiscard]] bool isCompletedByAcks() const {
    return !hasRequestFrame() && outstandingPosnCount_ == 0;
  }

  // The responder fills a response posn only by serving the request, so one
  // filled posn proves it whether or not the rest have landed.
  [[nodiscard]] bool hasBeenServed() const {
    return hasRequestFrame() && outstandingPosnCount_ < numPosn_;
  }

  [[nodiscard]] uint32_t allocateNextPosn() {
    const uint32_t n = static_cast<uint32_t>(numPosn_);
    if (hasRequestFrame()) {
      // The responder replies with one packet per posn, so a read too long
      // for the wire to number them is unsendable for the same reason an
      // oversize write is.
      if (requestFrameStatus_ == PosnStatus::PENDING &&
          n > OversizeWqeException::kMaxPacketsPerMessage) {
        throw OversizeWqeException(sizeBytes_, n);
      }
      requestFrameStatus_ = PosnStatus::ALLOCATED;
      return kRequestFrameWirePosn;
    }
    if (sndNxtPosn_ == 0 && n > OversizeWqeException::kMaxPacketsPerMessage) {
      // we are starting to send a message that is too long to be
      // sent at the current mtu.  throw exception to complete in error.
      throw OversizeWqeException(sizeBytes_, n);
    }

    // A rejected posn is the oldest thing left to send, so it is offered
    // before the cursor, which therefore only moves forward.
    if (!rejectedPosns_.empty()) {
      const uint32_t posn = rejectedPosns_.takeLowest();
      if (auditor_ != nullptr) {
        auditor_->onAllocated(posn);
      }
      return posn;
    }

    if (sndNxtPosn_ == n) {
      return n;
    }
    const uint32_t allocatedPosn = sndNxtPosn_++;
    if (auditor_ != nullptr) {
      auditor_->onAllocated(allocatedPosn);
    }
    return allocatedPosn;
  }

  void releasePosn(uint32_t posn) {
    DCHECK(isFrameAllocated(posn))
        << "posn " << posn
        << " is handed back by the tracker, so the cursor "
           "has passed it and it is not already waiting to go out again";
    DCHECK(!isFailing_)
        << "a failing WQE must abandon a posn rather than release it: a "
           "rejected posn is offered again, past the freeze markFailing() set";
    rejectedPosns_.add(posn, sndNxtPosn_);
    if (auditor_ != nullptr) {
      auditor_->onReleased(posn);
    }
  }

  void abandonPosn(uint32_t posn) {
    // An oversize message fails with more posns than a message may have
    // packets, so the count here is not bounded by that maximum.
    CHECK_GT(outstandingPosnCount_, 0u)
        << "fault - abandoning posn " << posn << " in a " << numPosn_
        << " packet message would take the outstanding count below zero";
    rejectedPosns_.remove(posn);
    outstandingPosnCount_--;
    if (auditor_ != nullptr) {
      auditor_->onResolved(posn);
    }
  }

  // A packet filled this posn: a read's response, or the data a receive work
  // request was posted for.
  void onPosnReceived(uint32_t posn) {
    DCHECK(posnsAreReceived())
        << "a posn an arriving packet fills belongs to a receive, or to the "
           "response of a read or atomic";
    DCHECK_LT(posn, numPosn_);
    DCHECK_GT(outstandingPosnCount_, 0u)
        << "posn " << posn << " arrived for a " << numPosn_
        << " packet message with none left outstanding, which would take the "
           "count below zero";
    ++receivedPosnCount_;
    receivedPosnSum_ += posn;
    outstandingPosnCount_--;
    if (auditor_ != nullptr) {
      auditor_->onReceived(posn);
    }
  }

  [[nodiscard]] bool hasOutstandingPosns() const {
    return outstandingPosnCount_ > 0;
  }

  // An ack covered this posn, which is how a send or a write resolves one.
  void acknowledgePosn(uint32_t posn) {
    resolvePosn(posn);
  }
  // Fail the rest of this message, leaving the packets already in flight to be
  // waited out. Returns the number of posns abandoned, for the caller to
  // remove from the unsent-posn rate hint. Caller must hold the Tag semaphore,
  // which is what excludes a concurrent allocateNextPosn().
  uint32_t markFailing(WorkCompletionStatus status) {
    if (isFailing()) {
      // Already failing. The first failure is the one that caused this, so it
      // keeps the status; there is no posn left to abandon either way.
      return 0;
    }
    isFailing_ = true;
    setCompletionStatusOverrideIfUnset(status);
    const uint32_t neverReached = numPosn_ - sndNxtPosn_;
    sndNxtPosn_ = static_cast<uint32_t>(numPosn_);

    if (hasRequestFrame()) {
      if (!isRequestFrameUnsent()) {
        return 0;
      }
      abandonFrame(kRequestFrameWirePosn);
      return 1;
    }

    if (posnsAreReceived()) {
      const uint32_t unreceived = numPosn_ - receivedPosnCount_;
      DCHECK_LE(unreceived, outstandingPosnCount_)
          << "abandoning the posns that never arrived cannot underflow the "
             "outstanding count";
      outstandingPosnCount_ -= unreceived;
      if (auditor_ != nullptr) {
        auditor_->markFailing();
      }
      return unreceived;
    }

    // Only a posn still waiting to be sent was counted as unsent work; an
    // allocated posn came off that count when the sender claimed it.
    const uint32_t abandonedUnsentCount = neverReached + rejectedPosns_.count();
    CHECK_LE(abandonedUnsentCount, outstandingPosnCount_)
        << "fault - abandoning the " << abandonedUnsentCount
        << " posns that never went out of a " << numPosn_
        << " packet message would take the outstanding count below zero";
    outstandingPosnCount_ -= abandonedUnsentCount;
    rejectedPosns_.clear();
    if (auditor_ != nullptr) {
      auditor_->markFailing();
    }
    return abandonedUnsentCount;
  }

  // Whether this WQE is failing: allocation is frozen and it is only waiting
  // out the packets it already has in flight. Such a WQE is never
  // retransmitted.
  [[nodiscard]] bool isFailing() const {
    return isFailing_;
  }

  // Whether the tracker still holds this WQE's request frame, so the sender
  // can retransmit it and no path may free the WQE yet.
  [[nodiscard]] bool isRequestFrameAllocated() const {
    return hasRequestFrame() && requestFrameStatus_ == PosnStatus::ALLOCATED;
  }

  // An intentional alias, for callers asking about retirement.
  [[nodiscard]] bool isHeldForRequestFrame() const {
    return isRequestFrameAllocated();
  }

  // A read or atomic waits on this frame alone: its response posns are the
  // responder's to fill, so they never reach a terminal state here.
  [[nodiscard]] bool isRequestFrameResolved() const {
    return hasRequestFrame() &&
        (requestFrameStatus_ == PosnStatus::ACKED ||
         requestFrameStatus_ == PosnStatus::ABANDONED);
  }

  // Settled spans acked and abandoned, which is where a frame never sent ends
  // up: marking abandons it rather than leaving it unsent. A read or atomic
  // waits only on its request frame; no responder will fill its response posns
  // now.
  [[nodiscard]] bool isFailingAndSettled() const {
    if (!isFailing_) {
      return false;
    }
    if (hasRequestFrame()) {
      DCHECK(!isRequestFrameUnsent())
          << "markFailing() abandons an unsent request at once, and every path "
             "that takes a frame back abandons it for a failing work request";
      return isRequestFrameResolved();
    }
    return outstandingPosnCount_ == 0;
  }

  [[nodiscard]] bool isReadyToComplete() const {
    return outstandingPosnCount_ == 0 || isFailingAndSettled();
  }

  [[nodiscard]] uint32_t getOutstandingPosnCount() const {
    return outstandingPosnCount_;
  }

  // Posns still to send count too. A read or atomic resolves on its request
  // frame alone: its response posns are the responder's to fill.
  [[nodiscard]] uint32_t getUnresolvedTxCount() const {
    if (hasRequestFrame()) {
      return isRequestFrameResolved() ? 0u : 1u;
    }
    return outstandingPosnCount_;
  }

  [[nodiscard]] uint32_t getNumPosn() const {
    return numPosn_;
  }

  [[nodiscard]] bool isPosnOutOfRange(uint16_t posn) const {
    return posn >= getNumPosn();
  }

  [[nodiscard]] bool isVirtual() const {
    return isVirtual_;
  }

  // Consulted before any lock is held, so it must be answerable from the
  // work request alone.
  [[nodiscard]] virtual bool completesReceiveOnAck() const {
    return false;
  }

  [[nodiscard]] bool hasFenceFlag() const {
    return hasFenceFlag_;
  }

  void setFenceFlag(bool flag) {
    hasFenceFlag_ = flag;
  }

  // Whether a successful completion for this WR should produce a CQE. Defaults
  // to true so only work requests that explicitly opt out are suppressed.
  [[nodiscard]] bool isSignaled() const {
    return isSignaled_;
  }

  // The status this WQE reports instead of the one its neighbors get. A work
  // request that failed on its own account has a distinct error, and
  // reports it in MSN order among the requests around it.
  void setCompletionStatusOverride(WorkCompletionStatus status) {
    maybeCompletionStatusOverride_ = status;
  }

  // Records `status` only where the WQE has none: the error it was marked
  // failing with is what ended it, and a later sweep must not replace it.
  void setCompletionStatusOverrideIfUnset(WorkCompletionStatus status) {
    if (!maybeCompletionStatusOverride_.has_value()) {
      setCompletionStatusOverride(status);
    }
  }

  [[nodiscard]] const std::optional<WorkCompletionStatus>&
  getCompletionStatusOverride() const {
    return maybeCompletionStatusOverride_;
  }

  [[nodiscard]] const std::optional<uint32_t>& getImmediateData() const {
    return maybeImmediateData_;
  }

  [[nodiscard]] bool hasImmediateData() const {
    return maybeImmediateData_.has_value();
  }

  [[nodiscard]] bool hasBeenSent() const {
    if (hasRequestFrame()) {
      // A read that fails while its request frame is unsent has no packet on
      // the wire to resolve that frame, so the failing state is what keeps it
      // from being offered again.
      return isFailing_ || !isRequestFrameUnsent();
    }
    return sndNxtPosn_ == numPosn_ && rejectedPosns_.empty();
  }

  [[nodiscard]] bool isLastPosn(uint32_t posn) const {
    return posn == numPosn_ - 1;
  }

  [[nodiscard]] uint32_t getPosnPayloadSizeBytes(uint32_t posn, uint16_t mtu)
      const {
    if (!isLastPosn(posn)) {
      return mtu;
    } else {
      return sizeBytes_ - posn * mtu;
    }
  }

  virtual ~Wqe() = default;
  virtual enum ibv_wc_opcode ibvCompletionOpcode() const = 0;

  // It is possible to post a recv wqe in INIT state, before MTU has been
  // set; this allows fixing the wqe before it becomes active.
  void updateMtu(uint16_t mtu) {
    CHECK_GT(mtu, 0);
    CHECK(
        mtu ==
            1000 || // temporarily; this is used in testing but not realistic.
        mtu == 1024 ||
        mtu == 256 || mtu == 2048 || mtu == 4096 || mtu == 512 || mtu == 8192)
        << "mtu " << mtu << " not supported";
    CHECK_EQ(receivedPosnCount_, 0u)
        << "fault - the posn state is rebuilt from the packet count a new mtu "
           "gives, so the "
        << receivedPosnCount_
        << " arrivals this work request already took would go unaccounted for";
    CHECK_EQ(sndNxtPosn_, 0u)
        << "fault - the same rebuild leaves the send cursor where it was, so "
           "the "
        << sndNxtPosn_ << " posns already on the wire would read as never sent";
    uint32_t numPosn = (sizeBytes_ == 0) ? 1 : 1 + ((sizeBytes_ - 1) / mtu);
    if (!posnsAreReceived()) {
      rejectedPosns_.clear();
    }
    if (posnAuditingEnabled()) {
      // A fresh auditor counts numPosn outstanding, which the assignment at
      // the end of this function gives the work request too.
      auditor_ = std::make_unique<PosnAuditor>(
          numPosn, outstandingPosnCount_, rejectedPosns_);
    }
    numPosn_ = numPosn;
    if (outstandingPosnCount_ != 0 && outstandingPosnCount_ != numPosn) {
      LOG_FIRST_N(INFO, 5) << "Updating wqe " << *this << " to have " << numPosn
                           << " instead of " << outstandingPosnCount_
                           << " outstanding Posn";
    }
    outstandingPosnCount_ = numPosn;
  }

  void setImmediateData(uint32_t immediateData) {
    maybeImmediateData_ = immediateData;
  }

  void* addressForPosnMtu(uint16_t posn, uint16_t mtu) {
    DCHECK(!hasLocalSges()) << "addressForPosnMtu requires contiguous storage";
    CHECK_NE(address_, nullptr);
    CHECK_LT(mtu * posn, sizeBytes_);
    return (void*)((char*)address_ + (mtu * posn));
  }

  // The referenced payload storage must outlive the returned view.
  [[nodiscard]] folly::IOBuf makeSendPayloadView(
      uint32_t posn,
      uint16_t mtu,
      uint32_t payloadSizeBytes) const;

  // Copies bytes into the buffer at posn's offset, across sges as needed.
  void deliverPosn(uint32_t posn, uint16_t mtu, folly::ByteRange bytes);

 protected:
  // Responder-side work, whose buffer a request's remote address identifies,
  // or which has none.
  explicit Wqe(
      WorkType workType,
      std::optional<uint64_t> workReqId, // not present in VirtualReceiveWqes
      std::optional<uint16_t> msn, // not initialized with ReceiveWqes
      void* address,
      uint32_t size,
      uint16_t mtu,
      bool isVirtual,
      std::optional<uint32_t> immediateData)
      : workType_(workType),
        maybeWorkReqId_(workReqId),
        maybeMsn_(msn),
        maybeCsn_(std::nullopt),
        isVirtual_(isVirtual),
        address_(address),
        sizeBytes_(size),
        maybeImmediateData_(immediateData) {
    CHECK_NE(mtu, 0);
    updateMtu(mtu);
  }

  // Posted work, whose buffer is the sges the caller posted.
  explicit Wqe(
      WorkType workType,
      std::optional<uint64_t> workReqId,
      std::optional<uint16_t> msn, // not initialized with ReceiveWqes
      LocalSges sges,
      uint16_t mtu,
      bool isVirtual,
      std::optional<uint32_t> immediateData,
      // When set, the WQE owns a copy of the payload, so the caller may reuse
      // its sources once the post returns; no lkey is consulted.
      bool isInline = false)
      : Wqe(workType,
            workReqId,
            msn,
            sges.empty() ? nullptr : sges.front().address,
            totalSgeLength(sges),
            mtu,
            isVirtual,
            immediateData) {
    if (isInline && sizeBytes_ > 0) {
      copyInlineSges(sges);
      address_ = inlineData_.data();
    } else if (sges.size() > 1) {
      segmented_ = makeSegmentedBuffer(std::move(sges));
    }
  }

  void copyInlineSges(const LocalSges& sges);

  // address_ can alias the owned inlineData_ buffer (inline WQEs), so a copy or
  // move would leave the target's address_ dangling; Wqes are held only via
  // unique_ptr.
  Wqe(const Wqe&) = delete;
  Wqe& operator=(const Wqe&) = delete;
  Wqe(Wqe&&) = delete;
  Wqe& operator=(Wqe&&) = delete;

  // An ack and an arriving packet each resolve a posn, which happens exactly
  // once and takes it off the outstanding count.
  void resolvePosn(uint32_t posn) {
    outstandingPosnCount_--;
    if (auditor_ != nullptr) {
      auditor_->onResolved(posn);
    }
    CHECK_LE(outstandingPosnCount_, OversizeWqeException::kMaxPacketsPerMessage)
        << "fault - should not resolve posn " << posn << " in a " << numPosn_
        << " packet message to cause outstanding packets to go negative";
  }

  WorkType workType_;
  std::optional<uint64_t> maybeWorkReqId_;
  std::optional<uint16_t> maybeMsn_;
  std::optional<uint16_t> maybeCsn_;
  uint32_t sndNxtPosn_{0};
  bool isVirtual_{false};
  bool hasFenceFlag_{false};
  bool isSignaled_{true};
  std::optional<WorkCompletionStatus> maybeCompletionStatusOverride_;
  // Set only by markFailing(), so it means the WQE is failing and allocation is
  // frozen. Written and read under the Tag semaphore.
  bool isFailing_{false};

 protected:
  void* address_;
  uint32_t sizeBytes_;
  // Unresolved posns: what a send or write waits on to complete. A read or
  // atomic waits on its request frame instead, leaving this to count response
  // posns, which stay PENDING.
  uint32_t outstandingPosnCount_{0};
  PosnStatus requestFrameStatus_{PosnStatus::PENDING};
  uint32_t numPosn_{0};
  uint32_t receivedPosnCount_{0};
  uint64_t receivedPosnSum_{0};
  RejectedPosnSet rejectedPosns_;
  std::optional<uint32_t> maybeImmediateData_;
  // Owns an inline payload copied from the work request; empty otherwise.
  std::vector<uint8_t> inlineData_;
  // Allocated apart so a single-buffer work request does not reserve room for
  // every sge.
  struct SegmentedBuffer {
    LocalSges sges;
    // Non-owning views of the SGE memory.
    folly::IOBuf chain;
  };
  static std::unique_ptr<SegmentedBuffer> makeSegmentedBuffer(LocalSges sges);
  std::unique_ptr<SegmentedBuffer> segmented_;

  // Instrumentation: the full state, recorded where the audit flag is set,
  // and off otherwise.
  std::unique_ptr<PosnAuditor> auditor_;

 private:
  [[nodiscard]] bool isRequestFrameUnsent() const {
    return requestFrameStatus_ == PosnStatus::PENDING ||
        requestFrameStatus_ == PosnStatus::REJECTED;
  }
};

struct SendWqe : public Wqe {
  explicit SendWqe(
      uint64_t workReqId,
      LocalSges sges,
      uint16_t mtu,
      int send_flags = IBV_SEND_SIGNALED)
      : Wqe(WorkType::kSend,
            workReqId,
            std::nullopt, /* msn */
            std::move(sges),
            mtu,
            false, /* isVirtual */
            std::nullopt, /*immediateData*/
            (send_flags & IBV_SEND_INLINE) != 0) {
    hasFenceFlag_ = send_flags & IBV_SEND_FENCE;
    isSignaled_ = send_flags & IBV_SEND_SIGNALED;
  }
  ~SendWqe() override = default;
  virtual enum ibv_wc_opcode ibvCompletionOpcode() const override {
    return IBV_WC_SEND;
  }
};

struct SendWithImmWqe : public SendWqe {
  explicit SendWithImmWqe(
      uint64_t workReqId,
      LocalSges sges,
      uint16_t mtu,
      uint32_t immediateData,
      int send_flags = IBV_SEND_SIGNALED)
      : SendWqe(workReqId, std::move(sges), mtu, send_flags) {
    setImmediateData(immediateData);
    workType_ = WorkType::kSendWithImm;
  }
  ~SendWithImmWqe() override = default;
};

struct ReceiveWqe : public Wqe {
  explicit ReceiveWqe(uint64_t workReqId, LocalSges sges, uint16_t mtu)
      : Wqe(WorkType::kReceive,
            workReqId,
            std::nullopt, /* msn */
            sges,
            mtu,
            false, /* isVirtual */
            std::nullopt /*immediateData*/),
        lkey_(sges.empty() ? 0 : sges.front().lkey) {}
  ~ReceiveWqe() override = default;

  // Kept only so the responder can validate the receive buffer lazily, when
  // a message arrives to be scattered in; a receive is not validated at post.
  [[nodiscard]] uint32_t getLkey() const {
    return lkey_;
  }

  // Shortens this receive to the terminal extent advertised by OpCode.
  // False leaves the receive as it was, for the caller to nak.
  [[nodiscard]] bool
  setFinalPacket(uint16_t posn, uint32_t payloadSizeBytes, uint16_t mtu) {
    if (receivedPosnCount_ > posn) {
      LOG_FIRST_N(ERROR, 10)
          << receivedPosnCount_ << " posns have arrived for a message the "
          << "sender now ends at posn " << posn;
      return false;
    }
    // Arrivals all at or below posn cannot sum past the top of that range, so
    // a larger sum places one of them beyond the packet called last.
    const uint64_t largestSumWithin =
        static_cast<uint64_t>(receivedPosnCount_) * posn -
        static_cast<uint64_t>(receivedPosnCount_) * (receivedPosnCount_ - 1) /
            2;
    if (receivedPosnSum_ > largestSumWithin) {
      LOG_FIRST_N(ERROR, 10)
          << receivedPosnCount_ << " posns summing to " << receivedPosnSum_
          << " have arrived for a message the sender now ends at posn " << posn;
      return false;
    }
    // The count and the sum cannot tell arrivals past this posn from arrivals
    // within it, so where the full state is kept it decides instead.
    if (auditor_ != nullptr && auditor_->anyArrivedAtOrAbove(posn + 1u)) {
      LOG_FIRST_N(ERROR, 10)
          << "a posn past " << posn
          << " has arrived for a message the sender now ends there";
      return false;
    }
    const uint32_t posnOffset = posn * static_cast<uint32_t>(mtu);
    const uint32_t availableSizeBytes = sizeBytes_ - posnOffset;
    const uint32_t deliveredPayloadSizeBytes =
        std::min(payloadSizeBytes, availableSizeBytes);
    const uint32_t removedPendingPosnCount = numPosn_ - (posn + 1u);
    DCHECK_LE(removedPendingPosnCount, outstandingPosnCount_)
        << "removing pending POSNs cannot underflow the outstanding count";
    numPosn_ = posn + 1u;
    outstandingPosnCount_ -= removedPendingPosnCount;
    if (auditor_ != nullptr) {
      auditor_->truncateTo(numPosn_);
    }

    sizeBytes_ = posnOffset + deliveredPayloadSizeBytes;
    return true;
  }

  // The completion opcode this receive reports (IBV_WC_RECV by default). The
  // responder sets it from the arriving operation, so the WQE need not know
  // which one that was.
  void setIbvCompletionOpcode(enum ibv_wc_opcode opcode) {
    completionOpcode_ = opcode;
  }

  virtual enum ibv_wc_opcode ibvCompletionOpcode() const override {
    return completionOpcode_;
  }
  // write/imm have receive wqes that may have null address and zero size, or
  // may have buffers attached that will be unused.  The address from the packet
  // takes precedence.  While a VirtualReceiveWqe is created from the packet's
  // addresses on creation, a non-virtual ReceiveWqe may need to overwrite the
  // address (and size) with what came in the packet.
  void
  updateDestinationAddress(uint64_t rdma_address, uint32_t size, uint16_t mtu) {
    // The packet field is a uint64_t; we store a pointer as a void *.
    // NOLINTNEXTLINE(performance-no-int-to-ptr)
    void* address = (void*)rdma_address;
    if (address_ != address || sizeBytes_ != size || hasLocalSges()) {
      address_ = address;
      sizeBytes_ = size;
      segmented_.reset();
      updateMtu(mtu);
      // The key registered the buffer this replaces; the packet's rkey is what
      // validated the target.
      lkey_ = 0;
    }
  }

  // The completion reports the length of the read, not the posted size.
  void updateSizeForReadWithImm(uint32_t readLength) {
    sizeBytes_ = readLength;
  }

 private:
  uint32_t lkey_{0};
  enum ibv_wc_opcode completionOpcode_ { IBV_WC_RECV };
};

struct VirtualReceiveWqe : public Wqe {
  explicit VirtualReceiveWqe(
      uint16_t msn,
      void* address,
      uint32_t size,
      uint16_t mtu)
      : Wqe(WorkType::kVirtualReceive,
            std::nullopt, /*workReqId*/
            msn,
            address,
            size,
            mtu,
            true, /* isVirtual */
            std::nullopt /*immediateData*/) {}

  ~VirtualReceiveWqe() override = default;

  virtual enum ibv_wc_opcode ibvCompletionOpcode() const override {
    LOG(FATAL) << "a virtual receive wqe has no completions";
  }
};

struct ReadWriteWqe : public Wqe {
  explicit ReadWriteWqe(
      WorkType workType,
      uint64_t workReqId,
      LocalSges sges,
      uint16_t mtu,
      uint64_t remoteAddress,
      uint32_t rkey,
      int send_flags = IBV_SEND_SIGNALED)
      : Wqe(workType,
            workReqId,
            std::nullopt, /* msn */
            sges,
            mtu,
            false, /* isVirtual */
            std::nullopt, /*immediateData*/
            (send_flags & IBV_SEND_INLINE) != 0),
        rdma_(remoteAddress, rkey),
        lkey_(sges.empty() ? 0 : sges.front().lkey) {
    if (workType != WorkType::kRead && workType != WorkType::kReadWithImm &&
        workType != WorkType::kWrite && workType != WorkType::kWriteWithImm) {
      throw std::runtime_error("Invalid work type for ReadWriteWqe");
    }
    LOG_IF(ERROR, rkey == 0) << "Rkey should be non-zero";
    hasFenceFlag_ = send_flags & IBV_SEND_FENCE;
    isSignaled_ = send_flags & IBV_SEND_SIGNALED;
  }
  ~ReadWriteWqe() override = default;
  virtual enum ibv_wc_opcode ibvCompletionOpcode() const override {
    return getWorkType() == WorkType::kWrite ? IBV_WC_RDMA_WRITE
                                             : IBV_WC_RDMA_READ;
  }

  [[nodiscard]] uint32_t getKey() const {
    return rdma_.rkey;
  }
  [[nodiscard]] uint64_t getAddress() const {
    return rdma_.remote_addr;
  }
  // Kept only to validate the local buffer when a read response is written
  // back; a read executes remotely before the local key is checked.
  [[nodiscard]] uint32_t getLkey() const {
    return lkey_;
  }

 private:
  struct rdma {
    uint64_t remote_addr;
    uint32_t rkey;
  } rdma_;
  uint32_t lkey_{0};
};

struct WriteWithImmWqe : public ReadWriteWqe {
  explicit WriteWithImmWqe(
      uint64_t workReqId,
      LocalSges sges,
      uint16_t mtu,
      uint64_t remoteAddress,
      uint32_t rkey,
      uint32_t immediateData,
      int send_flags = IBV_SEND_SIGNALED)
      : ReadWriteWqe(
            WorkType::kWriteWithImm,
            workReqId,
            std::move(sges),
            mtu,
            remoteAddress,
            rkey,
            send_flags) {
    setImmediateData(immediateData);
  }
  ~WriteWithImmWqe() override = default;
  virtual enum ibv_wc_opcode ibvCompletionOpcode() const override {
    return IBV_WC_RDMA_WRITE; // seems no difference on write side.
  }
};

struct ReadWithImmWqe : public ReadWriteWqe {
  explicit ReadWithImmWqe(
      uint64_t workReqId,
      LocalSges sges,
      uint16_t mtu,
      uint64_t remoteAddress,
      uint32_t rkey,
      uint32_t immediateData,
      int send_flags = IBV_SEND_SIGNALED)
      : ReadWriteWqe(
            WorkType::kReadWithImm,
            workReqId,
            std::move(sges),
            mtu,
            remoteAddress,
            rkey,
            send_flags) {
    setImmediateData(immediateData);
  }
  ~ReadWithImmWqe() override = default;
};

// Wqe created on responder side after receiving a read request
struct VirtualSendWqe : public Wqe {
  explicit VirtualSendWqe(
      uint16_t msn,
      void* address,
      uint32_t size,
      uint16_t mtu)
      : Wqe(WorkType::kVirtualSend,
            std::nullopt, /*workReqId*/
            msn,
            address,
            size,
            mtu,
            true, /* isVirtual */
            std::nullopt /*immediateData*/) {}
  ~VirtualSendWqe() override = default;
  virtual enum ibv_wc_opcode ibvCompletionOpcode() const override {
    LOG(FATAL) << "a virtual send wqe has no completions";
  }
};

// Response to a read-with-immediate; on the wire a plain read response.
struct VirtualReadImmResponseWqe : public VirtualSendWqe {
  using VirtualSendWqe::VirtualSendWqe;
  ~VirtualReadImmResponseWqe() override = default;
  [[nodiscard]] bool completesReceiveOnAck() const override {
    return true;
  }
};

struct AtomicWqe : public Wqe {
  explicit AtomicWqe(
      WorkType workType,
      uint64_t workReqId,
      const LocalSge& localBuffer,
      uint16_t mtu,
      uint64_t remoteAddress,
      uint32_t rkey,
      uint64_t swapOrAddData,
      uint64_t compareData,
      int send_flags)
      : Wqe(workType,
            workReqId,
            std::nullopt, /* msn */
            // Atomic ops always operate on 8 bytes, whatever length was posted.
            LocalSges{{
                .address = localBuffer.address,
                .length = 8,
                .lkey = localBuffer.lkey,
            }},
            mtu,
            false, /* isVirtual */
            std::nullopt /*immediateData*/),
        remoteAddress_(remoteAddress),
        rkey_(rkey),
        swapOrAddData_(swapOrAddData),
        compareData_(compareData),
        lkey_(localBuffer.lkey) {
    if (workType != WorkType::kAtomicFetchAdd &&
        workType != WorkType::kAtomicCmpSwp) {
      throw std::runtime_error("Invalid work type for AtomicWqe");
    }
    LOG_IF(ERROR, rkey == 0) << "Rkey should be non-zero";
    hasFenceFlag_ = send_flags & IBV_SEND_FENCE;
    isSignaled_ = send_flags & IBV_SEND_SIGNALED;
  }
  ~AtomicWqe() override = default;
  virtual enum ibv_wc_opcode ibvCompletionOpcode() const override {
    return getWorkType() == WorkType::kAtomicFetchAdd ? IBV_WC_FETCH_ADD
                                                      : IBV_WC_COMP_SWAP;
  }

  [[nodiscard]] uint64_t getRemoteAddress() const {
    return remoteAddress_;
  }
  [[nodiscard]] uint32_t getRkey() const {
    return rkey_;
  }
  [[nodiscard]] uint64_t getSwapOrAddData() const {
    return swapOrAddData_;
  }
  [[nodiscard]] uint64_t getCompareData() const {
    return compareData_;
  }
  // Kept only to validate the local buffer when the atomic response is
  // written back; the atomic executes remotely before the key is checked.
  [[nodiscard]] uint32_t getLkey() const {
    return lkey_;
  }

 private:
  uint64_t remoteAddress_;
  uint32_t rkey_;
  uint64_t swapOrAddData_;
  uint64_t compareData_;
  uint32_t lkey_;
};

struct VirtualAtomicAckWqe : public Wqe {
  explicit VirtualAtomicAckWqe(uint16_t msn, uint64_t origRemoteData)
      : Wqe(WorkType::kVirtualAtomicAck,
            std::nullopt, /*workReqId*/
            msn,
            nullptr,
            8, /* value goes in header, but need 1 POSN for TX */
            kAtomicMtu, /* MTU irrelevant: 8 bytes always fits in 1 POSN */
            true, /* isVirtual */
            std::nullopt /*immediateData*/),
        origRemoteData_(origRemoteData) {}
  ~VirtualAtomicAckWqe() override = default;
  virtual enum ibv_wc_opcode ibvCompletionOpcode() const override {
    LOG(FATAL) << "a virtual atomic ack wqe has no completions";
  }

  [[nodiscard]] uint64_t getOrigRemoteData() const {
    return origRemoteData_;
  }

 private:
  uint64_t origRemoteData_;
};

// Virtual WQE created at responder to defer atomic execution until all prior
// operations complete (CDMSN catches up). Stores the atomic operation
// parameters without executing. Execution happens in
// markDeliveredAndCompleteInOrder.
struct VirtualAtomicExecWqe : public Wqe {
  // isFetchAdd: true for ATOMIC_FETCH_ADD, false for ATOMIC_CMP_SWP
  explicit VirtualAtomicExecWqe(
      uint16_t msn,
      bool isFetchAdd,
      uint64_t targetAddr,
      uint64_t swapOrAddData,
      uint64_t compareData)
      : Wqe(WorkType::kVirtualAtomicExec,
            std::nullopt, /*workReqId*/
            msn,
            nullptr,
            8, /* atomic ops always operate on 8 bytes */
            kAtomicMtu, /* MTU irrelevant: 8 bytes always fits in 1 POSN */
            true, /* isVirtual */
            std::nullopt /*immediateData*/),
        isFetchAdd_(isFetchAdd),
        targetAddr_(targetAddr),
        swapOrAddData_(swapOrAddData),
        compareData_(compareData) {}
  ~VirtualAtomicExecWqe() override = default;
  virtual enum ibv_wc_opcode ibvCompletionOpcode() const override {
    LOG(FATAL) << "a virtual atomic exec wqe has no completions";
  }

  [[nodiscard]] bool isFetchAdd() const {
    return isFetchAdd_;
  }
  [[nodiscard]] uint64_t getTargetAddr() const {
    return targetAddr_;
  }
  [[nodiscard]] uint64_t getSwapOrAddData() const {
    return swapOrAddData_;
  }
  [[nodiscard]] uint64_t getCompareData() const {
    return compareData_;
  }

 private:
  bool isFetchAdd_;
  uint64_t targetAddr_;
  uint64_t swapOrAddData_;
  uint64_t compareData_;
};

} // namespace libsoftmetaroce

template <>
struct fmt::formatter<libsoftmetaroce::Wqe::WorkType>
    : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(libsoftmetaroce::Wqe::WorkType w, FormatContext& ctx) const {
    string_view name = "unknown";
    switch (w) {
      case libsoftmetaroce::Wqe::WorkType::kSend:
        name = "Send";
        break;
      case libsoftmetaroce::Wqe::WorkType::kSendWithImm:
        name = "SendWithImm";
        break;
      case libsoftmetaroce::Wqe::WorkType::kReceive:
        name = "Receive";
        break;
      case libsoftmetaroce::Wqe::WorkType::kRead:
        name = "Read";
        break;
      case libsoftmetaroce::Wqe::WorkType::kReadWithImm:
        name = "ReadWithImm";
        break;
      case libsoftmetaroce::Wqe::WorkType::kWrite:
        name = "Write";
        break;
      case libsoftmetaroce::Wqe::WorkType::kWriteWithImm:
        name = "WriteWithImm";
        break;
      case libsoftmetaroce::Wqe::WorkType::kVirtualReceive:
        name = "VirtualReceive";
        break;
      case libsoftmetaroce::Wqe::WorkType::kVirtualSend:
        name = "VirtualSend";
        break;
      case libsoftmetaroce::Wqe::WorkType::kAtomicFetchAdd:
        name = "AtomicFetchAdd";
        break;
      case libsoftmetaroce::Wqe::WorkType::kAtomicCmpSwp:
        name = "AtomicCmpSwp";
        break;
      case libsoftmetaroce::Wqe::WorkType::kVirtualAtomicAck:
        name = "VirtualAtomicAck";
        break;
      case libsoftmetaroce::Wqe::WorkType::kVirtualAtomicExec:
        name = "VirtualAtomicExec";
        break;
    }
    return fmt::formatter<string_view>::format(name, ctx);
  }
};
