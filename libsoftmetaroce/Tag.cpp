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

#include <atomic>

#include <folly/coro/BlockingWait.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/ProtocolCompatibility.h>
#include <libsoftmetaroce/QpState.h>
#include <libsoftmetaroce/Tag.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/WrappingCompare.h>
#include <libsoftmetaroce/WrappingMapIterator.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/ReadWriteHeader.h>
#include <algorithm>

namespace libsoftmetaroce {

DEFINE_int32(
    libsoftmetaroce_initialmsn,
    1,
    "Default initial MetaRoCE MSN to send and to expect");

namespace {

// Wait on `baton` until `ready` holds. Armed before the check, so a retirement
// between the two leaves it posted and the wait returns rather than missing it.
template <typename Ready>
folly::coro::Task<void> waitUntil(folly::coro::Baton& baton, Ready ready) {
  while (true) {
    baton.reset();
    if (ready()) {
      co_return;
    }
    co_await baton;
  }
}

} // namespace

Tag::Tag(
    uint32_t qpNum,
    uint8_t tagNum,
    CompletionQueue* sendCq,
    CompletionQueue* recvCq,
    struct ibv_qp* ibvQp,
    bool sqSigAll,
    uint32_t maxInlineData,
    uint32_t sendQueueDepth,
    uint32_t receiveQueueDepth,
    SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue)
    : sendCq_{sendCq},
      recvCq_{recvCq != nullptr ? recvCq : sendCq},
      ibvQp_{*CHECK_NOTNULL(ibvQp)},
      sharedReceiveQueue_{sharedReceiveQueue},
      qpNum_{qpNum},
      tagNum_{tagNum},
      sqSigAll_{sqSigAll},
      maxInlineData_{maxInlineData},
      sendQueueDepth_{sendQueueDepth},
      receiveQueueDepth_{receiveQueueDepth},
      receiveSequence_(
          SequenceNumbers{
              .nextMsn =
                  static_cast<uint16_t>(FLAGS_libsoftmetaroce_initialmsn),
              .nextCsn = kUnconfiguredStartingCsn}),
      msnSpan_(
          MsnSpan(static_cast<uint16_t>(FLAGS_libsoftmetaroce_initialmsn))) {
  CHECK(isBeforeRtr(ibvQp_.state))
      << "Tags may be added in INIT or RESET state only, not "
      << qpStateToString(ibvQp_.state);
  // Taken separately even where one queue serves both directions, so the
  // release pairs off against them.
  CHECK_NOTNULL(sendCq_)->takeReference();
  CHECK_NOTNULL(recvCq_)->takeReference();
}

Tag::~Tag() {
  releaseCompletionQueueReferences();
}

CompletionQueue* FOLLY_NULLABLE Tag::completionQueue(bool forReceive) {
  CompletionQueue* const cq = forReceive ? recvCq_ : sendCq_;
  DCHECK(cq != nullptr)
      << "a Tag reaching its completion queue after giving its references back "
         "is this library's bug: the destroy those references held off was "
         "free to release the queue";
  if (cq == nullptr) {
    LOG_FIRST_N(ERROR, 20)
        << "qp " << qpNum_ << " tag " << (uint32_t)tagNum_
        << " reached for a completion queue after giving its references back, "
           "so the completion is dropped rather than staged into memory "
           "ibv_destroy_cq was free to release";
    return nullptr;
  }
  return cq;
}

void Tag::releaseCompletionQueueReferences() {
  if (sendCq_ == nullptr || recvCq_ == nullptr) {
    return;
  }
  sendCq_->dropReference();
  recvCq_->dropReference();
  sendCq_ = nullptr;
  recvCq_ = nullptr;
}

[[nodiscard]] uint16_t Tag::getNextMsn() const {
  return msnSpan_.rlock()->nextMsn;
}

bool Tag::setNextMsn(uint16_t v) {
  if (!isBeforeRts(loadQpState(ibvQp_))) {
    return false;
  }
  auto lockedSpan = msnSpan_.wlock();
  CHECK(lockedSpan->wqes.empty())
      << "renumbering builds a fresh span, so work already keyed by the old "
      << "msns would be dropped without completing; the tag holds "
      << lockedSpan->wqes.size();
  *lockedSpan = MsnSpan(v);
  return true;
}

[[nodiscard]] uint16_t Tag::getAndIncrementNextMsn() {
  return msnSpan_.wlock()->nextMsn++;
}

[[nodiscard]] uint16_t Tag::getAndIncrementNextCsn() {
  return nextCsn_++;
}

[[nodiscard]] uint16_t Tag::getNextCsn() const {
  return nextCsn_;
}

bool Tag::setNextCsn(uint16_t v) {
  if (!isBeforeRts(loadQpState(ibvQp_))) {
    return false;
  }
  nextCsn_ = v;
  return true;
}

[[nodiscard]] uint16_t Tag::getAndIncrementNextRxCsn() {
  return receiveSequence_.takeNextCsn();
}

[[nodiscard]] uint16_t Tag::getNextRxCsn() const {
  return receiveSequence_.nextCsn();
}

[[nodiscard]] uint16_t Tag::getCDMSN() const {
  return receiveSequence_.cumulativeDelivered();
}

bool Tag::setSendSequenceNumbers(SequenceNumbers numbering) {
  auto lockedSpan = msnSpan_.wlock();
  if (!isBeforeRts(loadQpState(ibvQp_))) {
    return false;
  }
  CHECK(lockedSpan->wqes.empty())
      << "renumbering builds a fresh span, so work already keyed by the old "
      << "msns would be dropped without completing; the tag holds "
      << lockedSpan->wqes.size();
  *lockedSpan = MsnSpan(numbering.nextMsn);
  nextCsn_ = numbering.nextCsn;
  return true;
}

bool Tag::setReceiveSequenceNumbers(SequenceNumbers numbering) {
  // A tag answers inbound requests from RTR onward, too late to renumber it.
  if (!isBeforeRtr(loadQpState(ibvQp_))) {
    return false;
  }
  receiveSequence_ = ReceiveSequence(numbering);
  return true;
}

[[nodiscard]] uint16_t Tag::getTxCompletedMsn() const {
  return msnSpan_.rlock()->inOrderCompletedMsn;
}

[[nodiscard]] int Tag::getNumPendingRWqes() const {
  return (int)(pendingReceives_.rlock()->receivesByCsn.size() +
               initiatorMsnToResponderWqes_.size());
}

[[nodiscard]] bool Tag::hasPartiallyReceivedRWqe() const {
  for (const auto& [msn, wqe] : initiatorMsnToResponderWqes_) {
    const auto outstanding = wqe->getOutstandingPosnCount();
    if (outstanding > 0 && outstanding < wqe->getNumPosn()) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] int Tag::getNumPendingWqes() const {
  return msnSpan_.rlock()->countPending() +
      static_cast<int>(remoteMsnToWqe_.rlock()->size());
}

[[nodiscard]] bool Tag::hasPendingWqes() const {
  if (!remoteMsnToWqe_.rlock()->empty()) {
    return true;
  }
  return msnSpan_.rlock()->hasPending();
}

bool Tag::tryReserveSendQueueEntry() {
  const int32_t reserved =
      acceptedSendWqes_.fetch_add(1, std::memory_order_relaxed) + 1;
  if (reserved > static_cast<int32_t>(sendQueueDepth_)) {
    acceptedSendWqes_.fetch_sub(1, std::memory_order_relaxed);
    return false;
  }
  return true;
}

[[nodiscard]] bool Tag::hasMsnSpanRoom() const {
  return msnSpan_.rlock()->hasRoom();
}

[[nodiscard]] bool Tag::hasReadAtomicRoom() const {
  return outstandingReadAtomic_.load(std::memory_order_relaxed) < maxRdAtomic_;
}

folly::coro::Task<void> Tag::co_waitForReadAtomicRoom() {
  co_await waitUntil(readAtomicBaton_, [this] {
    return hasReadAtomicRoom() || postWillBeFlushed();
  });
}

folly::coro::Task<void> Tag::co_waitForMsnSpanRoom() {
  // processTransmitWqe() flushes a deleted tag's post before it takes an msn;
  // an errored tag's post takes one first, so that one waits for real span.
  co_await waitUntil(msnSpanBaton_, [this] {
    return hasMsnSpanRoom() || isMarkedForDeletion();
  });
}

void Tag::releaseSendQueueEntry() {
  const int32_t held =
      acceptedSendWqes_.fetch_sub(1, std::memory_order_relaxed);
  CHECK_GT(held, 0) << "tag " << (uint32_t)tagNum_ << " on QP " << qpNum_
                    << " returned a queue entry it never reserved; every later "
                       "post reads the depth as full";
}

int32_t Tag::getAcceptedReceiveQueueEntries() const {
  return acceptedReceiveWqes_.load(std::memory_order_relaxed);
}

bool Tag::tryReserveReceiveQueueEntry() {
  // A tag drawing from a shared queue has no depth of its own to reserve.
  if (sharedReceiveQueue_ != nullptr) {
    return false;
  }
  const int32_t reserved =
      acceptedReceiveWqes_.fetch_add(1, std::memory_order_relaxed) + 1;
  if (reserved > static_cast<int32_t>(receiveQueueDepth_)) {
    acceptedReceiveWqes_.fetch_sub(1, std::memory_order_relaxed);
    return false;
  }
  return true;
}

void Tag::releaseReceiveQueueEntry() {
  // A tag drawing from a shared queue holds no entry of its own: ibv_post_recv
  // refuses it, so the shared queue's depth is what bounds its receives.
  if (sharedReceiveQueue_ != nullptr) {
    return;
  }
  const int32_t held =
      acceptedReceiveWqes_.fetch_sub(1, std::memory_order_relaxed);
  CHECK_GT(held, 0) << "tag " << (uint32_t)tagNum_ << " on QP " << qpNum_
                    << " returned a receive queue entry it never handed out; "
                       "every later post reads the depth as full";
}

int32_t Tag::getAcceptedSendQueueEntries() const {
  return acceptedSendWqes_.load(std::memory_order_relaxed);
}

bool Tag::processTransmitWqe(std::unique_ptr<Wqe> ownedWqe) {
  Wqe& wqe = *CHECK_NOTNULL(ownedWqe.get());
  VLOG(2) << "Tag " << (uint32_t)tagNum_ << " processing WQE: " << wqe;
  // Every way out of here but taking the work on gives the queue entry back,
  // including the throw below.
  bool takenOn = false;
  auto queueEntryGuard = folly::makeGuard([this, &takenOn] {
    if (!takenOn) {
      releaseSendQueueEntry();
    }
  });
  if (isMarkedForDeletion()) {
    // A tag being removed rejects new work: flush the WQE in error so the
    // client still gets a completion. Advisory -- a WQE racing the mark is
    // instead flushed by ~Qp() at teardown.
    enqueueCqeForWqe(wqe, kWrFlushErr);
    return false;
  }
  const auto wqeWorkType = wqe.getWorkType();
  if (wqeWorkType == Wqe::WorkType::kSend ||
      wqeWorkType == Wqe::WorkType::kSendWithImm ||
      wqeWorkType == Wqe::WorkType::kWrite ||
      wqeWorkType == Wqe::WorkType::kWriteWithImm ||
      wqeWorkType == Wqe::WorkType::kRead ||
      wqeWorkType == Wqe::WorkType::kReadWithImm ||
      wqeWorkType == Wqe::WorkType::kAtomicFetchAdd ||
      wqeWorkType == Wqe::WorkType::kAtomicCmpSwp) {
    auto lockedSpan = msnSpan_.wlock();
    const auto msn = lockedSpan->nextMsn++;
    wqe.setMsn(msn);
    if (wqeWorkType == Wqe::WorkType::kSend ||
        wqeWorkType == Wqe::WorkType::kSendWithImm ||
        wqeWorkType == Wqe::WorkType::kWriteWithImm ||
        wqeWorkType == Wqe::WorkType::kReadWithImm) {
      wqe.setCsn(getAndIncrementNextCsn());
    }
    // Must precede both emplace paths: the error branch below moves the WQE,
    // so a wrap there consumes it without inserting it and it never reports.
    if (lockedSpan->wqes.contains(msn)) {
      LOG(FATAL) << fmt::format(
          "Send WQE MSN map wrapped at msn={}; libsoftmetaroce supports "
          "at most {} pending send WQEs per tag and expects the caller to "
          "stay below this limit when posting send WQEs",
          msn,
          UINT16_MAX);
    }
    if (wqe.hasRequestFrame()) {
      outstandingReadAtomic_.fetch_add(1, std::memory_order_relaxed);
    }
    const uint16_t retiredCursor = lockedSpan->inOrderRetiredMsn;
    DCHECK(after(msn, retiredCursor))
        << "posting msn " << msn << " at or before the retired cursor "
        << retiredCursor << ", which the map is the span ahead of";
    // Checked under the map lock, held through the insert below so a flush
    // cannot land between them.
    if (isInErrorState()) {
      // Bounded: the error state is terminal, so a client that keeps posting
      // reaches this on every one of them.
      LOG_FIRST_N(WARNING, 10)
          << "Tag " << (uint32_t)tagNum_
          << " marking newly posted WQE failing, tag is in error: " << wqe;
      // Queued behind the work requests posted before it, and flushed whatever
      // status it arrived with: the tag was in error before it was posted, so
      // an error of its own is not what ended it.
      wqe.setCompletionStatusOverride(kWrFlushErr);
      // The abandoned posns are dropped rather than subtracted: this returns
      // false, and the caller counts a post's posns only when it returns true.
      wqe.markFailing(kWrFlushErr);
      // Taken on like any other post: the retirement below returns its entry.
      lockedSpan->wqes.emplace(msn, std::move(ownedWqe));
      takenOn = true;
      lockedSpan.unlock();
      // Excludes receive delivery from the WQEs retired below; taken after
      // msnSpan_ is released, keeping the semaphore -> guard -> map order.
      auto retirementGuard = lockForWqeRetirement();
      completeSettledFailedWqes();
      return false;
    }
    const bool hasCompletionOverride =
        wqe.getCompletionStatusOverride().has_value();
    auto [postedIt, inserted] =
        lockedSpan->wqes.emplace(msn, std::move(ownedWqe));
    CHECK(inserted) << "an msn is handed out once and retired before it comes "
                       "round again, so the map cannot already hold this one";
    // The iterator holds only as long as the map lock, released below.
    Wqe& postedWqe = *postedIt->second;
    takenOn = true;
    if (hasCompletionOverride) {
      // Error the tag under the map lock, the same lock a concurrent post
      // checks it on. This is the one site with no owner to hand the
      // transition to, so TagSet::processWqes() publishes it after this
      // returns.
      setErrorStateWithoutNotification();
      // Released before the receive maps below, which the send map is never
      // held across.
      lockedSpan.unlock();
      // Excludes receive delivery from the flush below, and every path that
      // could retire `postedWqe`; taken after msnSpan_ is released, keeping
      // the semaphore -> guard -> map order.
      auto retirementGuard = lockForWqeRetirement();
      // Safe to mark here: the tag is in error, so the tx path will not pull
      // from it. Keeps the error it arrived with rather than the flush.
      postedWqe.markFailing(postedWqe.getCompletionStatusOverride().value());
      // The work requests ahead of it belong to the sender: they may have
      // packets on the wire, and their posn state is the tx owner's to write.
      // The error state set above is what tells it to fail them.
      flushPendingReceiveWqesInError();
      return false;
    }
    return true;
  } else {
    throw std::runtime_error(
        fmt::format(
            "posted wqe.workType {} is not supported", wqe.getWorkType()));
  }
}

void Tag::flushReceivesForStrandedCsn(uint16_t csn, ReceiveWqe& rejectedPost) {
  LOG(ERROR) << fmt::format(
      "Tag {} still holds a receive for csn {} a whole csn space after it was "
      "numbered, so the peer never sent the message that csn was posted for. "
      "The tag can number no further receive and fails here",
      (uint32_t)tagNum_,
      csn);
  auto retirementGuard = lockForWqeRetirement();
  flushPendingReceiveWqesInError();
  enqueueCqeForWqe(rejectedPost, kWrFlushErr);
}

std::optional<uint16_t> Tag::processReceiveWqe(
    std::unique_ptr<ReceiveWqe> ownedWqe,
    uint32_t mtu) {
  ReceiveWqe& receiveWqe = *CHECK_NOTNULL(ownedWqe.get());
  VLOG(2) << "Tag " << (uint32_t)tagNum_ << " processing WQE: " << receiveWqe;
  // Every way out of here but taking the work on gives the queue entry back.
  bool takenOn = false;
  auto queueEntryGuard = folly::makeGuard([this, &takenOn] {
    if (!takenOn) {
      releaseReceiveQueueEntry();
    }
  });
  if (isMarkedForDeletion()) {
    enqueueCqeForWqe(receiveWqe, kWrFlushErr);
    return std::nullopt;
  }
  receiveWqe.updateMtu(mtu);
  const auto csn = getAndIncrementNextRxCsn();
  receiveWqe.setCsn(csn);
  auto lockedReceives = pendingReceives_.wlock();
  if (isInErrorState()) {
    LOG(WARNING) << "Tag " << (uint32_t)tagNum_
                 << " flushing newly posted receive WQE in error state: "
                 << receiveWqe;
    enqueueCqeForWqe(receiveWqe, kWrFlushErr);
    return std::nullopt;
  }
  const bool numbered =
      lockedReceives->receivesByCsn.try_emplace(csn, std::move(ownedWqe))
          .second;
  if (!numbered) {
    // Errored under the map lock, the same lock a concurrent post checks it
    // on. TagSet::processWqes() publishes the transition after this returns.
    setErrorStateWithoutNotification();
    // Released before the retirement guard the flush takes, keeping the
    // semaphore -> guard -> map order.
    lockedReceives.unlock();
    // Still owned here: try_emplace moves only on the insert.
    flushReceivesForStrandedCsn(csn, receiveWqe);
    return std::nullopt;
  }
  takenOn = true;
  // Under the same lock, a racing miss either finds this receive or records
  // the request this post clears.
  if (lockedReceives->rnrCancelRequested) {
    lockedReceives->rnrCancelRequested = false;
    return csn;
  }
  return std::nullopt;
}

void Tag::createVirtualRWqeIfNeeded(
    const uint16_t msn,
    const ReadWriteHeader& rwData,
    int* FOLLY_NONNULL addressError,
    MemoryRegions& memoryRegions,
    uint32_t mtu) {
  *addressError = 0;
  const uint16_t cdmsn = getCDMSN();
  if (before(msn, cdmsn)) {
    LOG(ERROR) << "msn: " << msn << " is already completed.";
    return;
  }
  if (!initiatorMsnToResponderWqes_.contains(msn)) {
    if (after(msn, (uint16_t)(cdmsn + 1))) {
      LOG_FIRST_N(INFO, 20)
          << "msn: " << msn
          << " is not in sequence, expecting msn: " << (cdmsn + 1) << ".";
    }
    const auto address = memoryRegions.addressForRemoteWrite(ibvQp_.pd, rwData);
    if (!address) {
      // Cause the receiver to recognize the failed validation so that an
      // address error message is generated.
      *addressError = 1;
      // Do not create a virtual receive wqe for an invalid address message.
      return;
    }
    initiatorMsnToResponderWqes_.emplace(
        msn,
        std::make_unique<VirtualReceiveWqe>(
            msn,
            // NOLINTNEXTLINE(performance-no-int-to-ptr)
            reinterpret_cast<char*>(*address),
            rwData.getMessageLength(),
            mtu));
  }
}

std::optional<char*> Tag::addressForVirtualResponseWqe(
    const uint16_t msn,
    const ReadWriteHeader& rwData,
    int* FOLLY_NONNULL addressError,
    MemoryRegions& memoryRegions) {
  *addressError = 0;
  // This prevents the creation of a response for a read request that is
  // spuriously retransmitted.
  const uint16_t cdmsn = getCDMSN();
  if (!after(msn, cdmsn)) {
    const auto inProgress = remoteMsnToWqe_.rlock()->size();
    LOG(ERROR)
        << "msn: " << msn
        << " is already completely delivered (we may not have finished it). "
        << inProgress << " responses in progress";
    return std::nullopt;
  }

  const auto address = memoryRegions.addressForRemoteRead(ibvQp_.pd, rwData);
  if (!address) {
    LOG(ERROR) << "msn=" << msn << " validation failed for " << rwData.getKey();
    *addressError = 1;
    return std::nullopt;
  }
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  return reinterpret_cast<char*>(*address);
}

void Tag::createVirtualSendWqeIfNeeded(
    const uint16_t msn,
    const ReadWriteHeader& rwData,
    int* FOLLY_NONNULL addressError,
    MemoryRegions& memoryRegions,
    uint32_t mtu) {
  const auto address =
      addressForVirtualResponseWqe(msn, rwData, addressError, memoryRegions);
  if (!address.has_value()) {
    return;
  }
  remoteMsnToWqe_.wlock()->emplace(
      msn,
      std::make_unique<VirtualSendWqe>(
          msn, // initiator side msn
          *address,
          rwData.getMessageLength(),
          mtu));
}

void Tag::createVirtualReadImmResponseWqeIfNeeded(
    const uint16_t msn,
    const ReadWriteHeader& rwData,
    int* FOLLY_NONNULL addressError,
    MemoryRegions& memoryRegions,
    uint32_t mtu) {
  const auto address =
      addressForVirtualResponseWqe(msn, rwData, addressError, memoryRegions);
  if (!address.has_value()) {
    return;
  }
  remoteMsnToWqe_.wlock()->emplace(
      msn,
      std::make_unique<VirtualReadImmResponseWqe>(
          msn, // initiator side msn
          *address,
          rwData.getMessageLength(),
          mtu));
}

void Tag::createVirtualAtomicAckWqeIfNeeded(
    const uint16_t msn,
    uint64_t origRemoteData) {
  auto lockedMap = remoteMsnToWqe_.wlock();
  if (lockedMap->contains(msn)) {
    return;
  }
  lockedMap->emplace(
      msn, std::make_unique<VirtualAtomicAckWqe>(msn, origRemoteData));
}

Tag::AtomicTarget Tag::validateAtomicRequest(
    uint64_t targetAddr,
    uint32_t rkey,
    MemoryRegions& memoryRegions) const {
  const auto resolved =
      memoryRegions.targetForRemoteAtomic(ibvQp_.pd, targetAddr, rkey);
  RequestValidationError error = RequestValidationError::kNone;
  // The 8-byte requirement (IBTA A19.4.2) is on the address the responder
  // touches, so a zero-based region's base is added before it is checked.
  // A request both unaligned and badly keyed NAKs invalid request, so the
  // misalignment is answered before the access check below.
  if (resolved.address % 8 != 0) {
    error = RequestValidationError::kInvalidRequest;
  } else if (!resolved.permitted) {
    error = RequestValidationError::kAddressError;
  }
  return {.error = error, .address = resolved.address};
}

void Tag::createVirtualAtomicExecWqeIfNeeded(
    uint16_t msn,
    bool isFetchAdd,
    uint64_t targetAddr,
    uint64_t swapOrAddData,
    uint64_t compareData) {
  const uint16_t cdmsn = getCDMSN();
  if (before(msn, cdmsn)) {
    LOG(ERROR) << "msn: " << msn << " is already completed.";
    return;
  }
  if (initiatorMsnToResponderWqes_.contains(msn)) {
    return;
  }
  initiatorMsnToResponderWqes_.emplace(
      msn,
      std::make_unique<VirtualAtomicExecWqe>(
          msn, isFetchAdd, targetAddr, swapOrAddData, compareData));
}

std::unique_ptr<ReceiveWqe> Tag::takeReceiveFromSharedQueue(
    uint16_t csn,
    uint32_t mtu,
    bool refusalRaisesBrnr) {
  // Work bound to a failing tag is flushed, so the buffer stays for the queue
  // pairs still running.
  if (postWillBeFlushed()) {
    return nullptr;
  }
  CHECK_GT(mtu, 0u)
      << "a queue pair negotiates its mtu on the move to RTR and answers no "
         "message before that, so every caller reaching here holds the one it "
         "settled on";
  // A csn at or behind the edge wraps high here, so a stale retransmit takes
  // no receive.
  const uint16_t missingAhead = static_cast<uint16_t>(
      csn - receiveSequence_.cumulativeCompletedCsn() - 1);
  // The cancel carries the first uncompleted csn.
  const uint16_t csnExpected =
      static_cast<uint16_t>(receiveSequence_.cumulativeCompletedCsn() + 1);
  // A refusal that sends no BRNR starts no backoff, so it records nothing.
  const bool brnrEnabled =
      refusalRaisesBrnr && !FLAGS_libsoftmetaroce_dont_send_brnr;
  std::optional<SharedReceive> taken =
      CHECK_NOTNULL(sharedReceiveQueue_)
          ->tryTakeOldestLeavingAtLeast(
              missingAhead,
              SharedReceiveTake{
                  .taker = {.qpNum = qpNum_, .tagNum = tagNum_},
                  .csnToRecordOnRefusal = brnrEnabled
                      ? std::optional<uint16_t>{csnExpected}
                      : std::nullopt});
  if (!taken) {
    return nullptr;
  }
  auto receiveWqe = std::make_unique<ReceiveWqe>(
      taken->workRequestId, std::move(taken->sges), mtu);
  receiveWqe->setCsn(csn);
  return receiveWqe;
}

Tag::PendingReceiveWqes::iterator Tag::findReceiveTakingFromSharedQueue(
    PendingReceiveWqes& pending,
    uint16_t csn,
    uint32_t mtu,
    bool refusalRaisesBrnr) {
  if (sharedReceiveQueue_ == nullptr) {
    return pending.find(csn);
  }
  // Find-or-reserve in one lookup: it returns the receive this csn already
  // holds, or reserves its place for the draw below to fill.
  auto [wqeIt, reserved] = pending.try_emplace(csn);
  if (!reserved) {
    return wqeIt;
  }
  wqeIt->second = takeReceiveFromSharedQueue(csn, mtu, refusalRaisesBrnr);
  if (wqeIt->second == nullptr) {
    pending.erase(wqeIt);
    return pending.end();
  }
  return wqeIt;
}

void Tag::recordDedicatedReceiveCancelRequest(PendingReceives& receives) {
  // A receive posted to the shared queue is not posted to this tag, so a
  // request recorded here would have no post of its own to clear it.
  if (sharedReceiveQueue_ != nullptr) {
    return;
  }
  // No BRNR is sent, so the initiator starts no backoff for a cancel to lift.
  if (FLAGS_libsoftmetaroce_dont_send_brnr) {
    return;
  }
  // Called with pendingReceives_ write-locked, so a racing post either leaves
  // a receive for this lookup to find or clears the request recorded here.
  receives.rnrCancelRequested = true;
}

bool Tag::assignInitiatorMsnToReceiveWqeIfNeeded(
    uint16_t csn,
    uint16_t msn,
    uint32_t mtu) {
  if (initiatorMsnToResponderWqes_.contains(msn)) {
    // Already bound.
    return false;
  }

  auto lockedReceives = pendingReceives_.wlock();
  auto wqeIt = findReceiveTakingFromSharedQueue(
      lockedReceives->receivesByCsn, csn, mtu, /*refusalRaisesBrnr=*/true);
  if (wqeIt == lockedReceives->receivesByCsn.end()) {
    // Wqe doesn't exist in both maps, this can be either due to a spurious
    // retransmission for a RWQE that is already completed in the rx side or
    // the receiver hasn't posted a RWQE.
    VLOG(1) << "pendingReceives_ lacked csn " << csn << " and has "
            << lockedReceives->receivesByCsn.size() << " entries";
    recordDedicatedReceiveCancelRequest(*lockedReceives);
    return false;
  }

  initiatorMsnToResponderWqes_.emplace(msn, std::move(wqeIt->second));
  lockedReceives->receivesByCsn.erase(wqeIt);
  return true;
}

// Like the two parameter version, but first validates ReadWriteHeader, then
// populates the ReceiveWqe with the RDMA Write's address.
void Tag::assignInitiatorMsnToReceiveWqeIfNeeded(
    uint16_t csn,
    uint16_t msn,
    const ReadWriteHeader& rwData,
    int* FOLLY_NONNULL addressError,
    MemoryRegions& memoryRegions,
    uint32_t mtu) {
  *addressError = 0;
  if (initiatorMsnToResponderWqes_.contains(msn)) {
    // Already exists; no need to act.
    return;
  }

  const auto address = memoryRegions.addressForRemoteWrite(ibvQp_.pd, rwData);
  if (!address) {
    LOG(INFO) << "msn=" << msn << " validation failed for " << rwData.getKey();
    *addressError = 1;
    // The consumed receive WQE holds its own protection error so the flush
    // reports it in the order the client posted it, behind the receives
    // still pending ahead of it.
    auto lockedReceives = pendingReceives_.wlock();
    auto wqeIt = findReceiveTakingFromSharedQueue(
        lockedReceives->receivesByCsn, csn, mtu, /*refusalRaisesBrnr=*/false);
    if (wqeIt != lockedReceives->receivesByCsn.end()) {
      wqeIt->second->setCompletionStatusOverride(kLocalProtErr);
    }
    // Address validation produces a fatal NAK rather than a BRNR, so this
    // miss records no RNR-Cancel request.
    return;
  }

  auto lockedReceives = pendingReceives_.wlock();
  auto wqeIt = findReceiveTakingFromSharedQueue(
      lockedReceives->receivesByCsn, csn, mtu, /*refusalRaisesBrnr=*/true);
  if (wqeIt == lockedReceives->receivesByCsn.end()) {
    // Wqe doesn't exist in both maps, this can be either due to a spurious
    // retransmission for a RWQE that is already completed in the rx side or
    // the receiver hasn't posted a RWQE.
    VLOG(1) << "pendingReceives_ lacked csn " << csn << " and has "
            << lockedReceives->receivesByCsn.size() << " entries";
    recordDedicatedReceiveCancelRequest(*lockedReceives);
    return;
  }
  wqeIt->second->updateDestinationAddress(
      *address, rwData.getMessageLength(), mtu);
  initiatorMsnToResponderWqes_.emplace(msn, std::move(wqeIt->second));
  lockedReceives->receivesByCsn.erase(wqeIt);
}

bool Tag::recordLocalProtectionErrorIfUnwritable(
    uint16_t msn,
    const MemoryRegions& memoryRegions) {
  // initiatorMsnToResponderWqes_ takes no lock of its own: the caller holds the
  // WQE-retirement guard shared for this delivery, which is what excludes the
  // flush that completes and erases these.
  const auto it = initiatorMsnToResponderWqes_.find(msn);
  if (it == initiatorMsnToResponderWqes_.end()) {
    return false;
  }
  Wqe& wqe = *it->second;
  // Virtual receive WQEs (RDMA write/read targets) are validated against the
  // packet's remote address, not here.
  if (wqe.isVirtual() || wqe.getWorkType() != Wqe::WorkType::kReceive) {
    return false;
  }
  // A zero-length receive has no buffer to scatter into, so there is no region
  // to validate; any other receive is checked against its registered region (an
  // unregistered key, including 0, fails).
  if (wqe.getMessageSizeBytes() == 0) {
    return false;
  }
  const bool valid = wqe.hasLocalSges()
      ? memoryRegions.isValidLocal(
            receiveProtectionDomain(),
            wqe.getLocalSges(),
            wqe.getMessageSizeBytes(),
            IBV_ACCESS_LOCAL_WRITE)
      : memoryRegions.isValidLocal(
            receiveProtectionDomain(),
            static_cast<char*>(wqe.getBufferAddress()),
            wqe.getMessageSizeBytes(),
            static_cast<ReceiveWqe&>(wqe).getLkey(),
            IBV_ACCESS_LOCAL_WRITE);
  if (valid) {
    return false;
  }
  // Reported by the flush that follows, in posting order. Completing it here
  // would put it ahead of the receives posted before it.
  wqe.setCompletionStatusOverride(kLocalProtErr);
  return true;
}

void Tag::extendCdmsnTrackerIfNeeded(uint16_t msn) {
  receiveSequence_.noteInFlight(msn);
}

[[nodiscard]] bool Tag::isBlockedMsn(uint16_t msn) const {
  return brnr_.isBlockedMsn(msn);
}

Wqe* FOLLY_NULLABLE Tag::findNextWqe() {
  // Hold the Tag's semaphore across the WQE lookup and until the caller
  // markFree()s: a concurrent completion takes the same semaphore before
  // freeing WQEs, so acquiring it up front keeps the returned pointer alive for
  // the caller. The Tag may also be unavailable because it is in the
  // retransmission callback.
  waitForFree();
  Wqe* wqeReadyToSend = nullptr;
  const uint16_t cdmsn = getCDMSN();
  // First, check remote MSN WQEs.
  {
    auto lockedRemoteMap = remoteMsnToWqe_.rlock();
    wqeReadyToSend =
        findNextWqeInMap(*lockedRemoteMap, true /* msnsAreRemote */, cdmsn);
  }
  // Second, check locally-generated WQEs.
  if (wqeReadyToSend == nullptr) {
    auto lockedSpan = msnSpan_.rlock();
    wqeReadyToSend = findNextWqeInMap(
        lockedSpan->wqes, false /* msnsAreRemote */, lockedSpan->nextMsn);
  }
  // Release the semaphore taken above when there is no WQE to hand back.
  if (wqeReadyToSend == nullptr) {
    markFree();
    return nullptr;
  }
  // A tag in error still returns the WQE, for the send path to fail it. This
  // check runs after the lookup's msnSpan_ lock is released, since it takes
  // that lock again.
  if (wqeReadyToSend->hasFenceFlag() && !isInErrorState() &&
      !arePriorWqesCompleteForFence(wqeReadyToSend->getMsn().value())) {
    markFree();
    return nullptr;
  }
  return wqeReadyToSend;
}

Wqe* Tag::findNextWqeInMap(
    const std::map<uint16_t, std::unique_ptr<Wqe>>& readLockedWqeMap,
    bool msnsAreRemote,
    uint16_t startMsn) {
  auto range = iterateFrom(readLockedWqeMap, startMsn);
  auto it = std::find_if(
      range.begin(),
      range.end(),
      [this, msnsAreRemote](const auto& msnWqePair) {
        // Must return true if the WQE has data that is can send and false
        // otherwise.
        const auto& [msn, wqe] = msnWqePair;
        if (wqe->hasBeenSent()) {
          return false;
        }
        if (msnsAreRemote) {
          return true;
        }
        if (!isBlockedMsn(msn)) {
          return true;
        }
        // BRNR: This is a blocked MSN, so whether it can be sent depends on the
        // BRNR state.
        if (brnr_.state() == BrnrState::BRNR_BLOCKED_TX) {
          // We are in BRNR blocked state, so don't send any packets from
          // blocked MSNs
          return false;
        } else if (brnr_.state() == BrnrState::BRNR_PROBE_SENT) {
          // We are in BRNR probing state but a probe has already been
          // scheduled, so skip this WQE; we may proceed to a wqe that
          // does not requires an RWQE.
          return false;
        } else if (brnr_.state() == BrnrState::BRNR_PROBING) {
          return true;
        } else {
          LOG(FATAL) << "Unexpected BRNR state: " << brnr_.state()
                     << " with a blocked msn.";
        }
      });
  return (it != range.end()) ? (*it).second.get() : nullptr;
}

[[nodiscard]] std::optional<uint16_t> Tag::getBrnrFirstBlockedMsn() const {
  return brnr_.firstBlockedMsn();
}

BrnrState Tag::getBrnrState() const {
  return brnr_.state();
}

struct ibv_qp& Tag::getIbvQp() const {
  return ibvQp_;
}

bool Tag::isInErrorState() const {
  return loadQpState(ibvQp_) == IBV_QPS_ERR;
}

bool Tag::isErrorNakHeld(uint64_t nowNs) const {
  return nowNs < nextErrorNakAtNs_;
}

bool Tag::hasSentErrorNak() const {
  return nextErrorNakAtNs_ != 0;
}

void Tag::holdErrorNaks(uint64_t nowNs) {
  nextErrorNakAtNs_ = nowNs + kErrorNakIntervalNs;
}

bool Tag::mustStopRetransmitting(const Wqe& wqe) const {
  return wqe.isFailing() || wqe.hasBeenServed() || isInErrorState();
}

Wqe* FOLLY_NULLABLE Tag::getReceiveWqe(
    const BaseHeader::Opcode::OperationType opType,
    const uint16_t msn) {
  switch (opType) {
    case BaseHeader::Opcode::OperationType::READ:
    case BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD:
    case BaseHeader::Opcode::OperationType::ATOMIC_CMP_SWP:
      // For read/atomic requests, there are no receive WQEs.
      return nullptr;
    case BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP:
    case BaseHeader::Opcode::OperationType::READ_RESP: {
      // The corresponding receive WQE for this read response packet is the
      // original read WQE stored in msnSpan_.wqes (locally-generated).
      auto lockedSpan = msnSpan_.rlock();
      if (lockedSpan->isTxCompletionCommitted(msn)) {
        // Every posn is in and the completion is committed; the WQE lingers
        // only for its request frame's ack. Delivering into it again would
        // rewrite the buffer behind a work request already reported.
        return nullptr;
      }
      auto wqeIt = lockedSpan->wqes.find(msn);
      if (wqeIt == lockedSpan->wqes.end()) {
        LOG(INFO) << fmt::format(
            "Received read response packet with msn: {}, "
            "but that WQE is not in the map; next MSN is {}",
            msn,
            lockedSpan->nextMsn);
        return nullptr;
      }
      return wqeIt->second.get();
    }
    default: {
      auto wqeIt = initiatorMsnToResponderWqes_.find(msn);
      if (wqeIt == initiatorMsnToResponderWqes_.end()) {
        LOG(INFO) << fmt::format(
            "No Receive WQE found for msn={}: qp={}, tag={}, cdmsn={}, "
            "map size={}",
            msn,
            qpNum_,
            static_cast<unsigned>(tagNum_),
            receiveSequence_.cumulativeDelivered(),
            initiatorMsnToResponderWqes_.size());
        return nullptr;
      }
      return wqeIt->second.get();
    }
  }
}

// A client receive reports a completion; a write's virtual receive and an
// atomic's deferred exec report none, and an msn with neither is a read.
bool Tag::completeDeliveredReceive(const uint16_t msn) {
  // False where this receive's csn is not the one the sequence expects next.
  auto wqeIt = initiatorMsnToResponderWqes_.find(msn);
  // A read binds no receive, so the edge runs over its msn with none here.
  if (wqeIt == initiatorMsnToResponderWqes_.end()) {
    return true;
  }
  Wqe& wqe = *CHECK_NOTNULL(wqeIt->second.get());
  if (wqe.getWorkType() == Wqe::WorkType::kVirtualAtomicExec) {
    auto* execWqe = static_cast<VirtualAtomicExecWqe*>(&wqe);
    // NOLINTNEXTLINE(performance-no-int-to-ptr)
    auto* target = reinterpret_cast<uint64_t*>(execWqe->getTargetAddr());
    uint64_t origVal;
    if (execWqe->isFetchAdd()) {
      std::atomic_ref<uint64_t> ref(*target);
      origVal = ref.fetch_add(execWqe->getSwapOrAddData());
    } else {
      std::atomic_ref<uint64_t> ref(*target);
      uint64_t expected = execWqe->getCompareData();
      ref.compare_exchange_strong(expected, execWqe->getSwapOrAddData());
      origVal = expected;
    }
    createVirtualAtomicAckWqeIfNeeded(msn, origVal);
  } else if (!wqe.isVirtual()) {
    CHECK(wqe.getCsn().has_value())
        << "a receive is bound to a csn before it can complete, and the edge "
           "cannot run over one holding none";
    if (!receiveSequence_.tryMarkCsnCompleted(wqe.getCsn().value())) {
      logCsnOutOfSequence(msn, wqe.getCsn().value());
      // The receive stays in the map; the flush that follows the caller's
      // refusal reports it with the rest the tag holds.
      return false;
    }
    releaseReceiveQueueEntry();
    enqueueCqeForWqe(wqe, kSuccess);
  }
  initiatorMsnToResponderWqes_.erase(wqeIt);
  return true;
}

void Tag::logCsnOutOfSequence(const uint16_t msn, const uint16_t csn) const {
  LOG_FIRST_N(ERROR, 10) << fmt::format(
      "tag {} on qp {} msn {}: a receive completed on csn {} where the sequence reaches {} next, so the peer skipped at least one csn",
      tagNum_,
      qpNum_,
      msn,
      csn,
      static_cast<uint16_t>(receiveSequence_.cumulativeCompletedCsn() + 1));
}

bool Tag::markDeliveredAndCompleteInOrder(const uint16_t msn) {
  // `markDelivered` returns how far the edge advanced, so one arrival can
  // complete a run. False leaves the rest of that run to the tag's flush.
  uint16_t deliveredMsn = receiveSequence_.cumulativeDelivered();
  const uint16_t delivered = receiveSequence_.markDelivered(msn);
  for (uint16_t i = 0; i < delivered; ++i) {
    if (!completeDeliveredReceive(++deliveredMsn)) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] uint8_t Tag::getTagNum() const {
  return tagNum_;
}

bool Tag::handleRxWqeCompletion(
    Wqe& wqe,
    const BaseHeader::Opcode::OperationType opType,
    const uint16_t msn) {
  VLOG(2) << "Rx: WQE completed on QP: " << qpNum_
          << " Tag: " << (uint32_t)tagNum_ << " : " << wqe;
  if (opType == BaseHeader::Opcode::OperationType::READ_RESP ||
      opType == BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP) {
    return handleTxWqesCompletion({wqe});
  }
  return markDeliveredAndCompleteInOrder(msn);
}

Cqe Tag::cqeForWqe(const Wqe& wqe, WorkCompletionStatus status) const {
  DCHECK(wqe.getWorkReqId().has_value());
  // A byte count describes a transfer that happened; an error completion has
  // none to describe.
  const uint32_t sizeBytes = status == kSuccess ? wqe.getMessageSizeBytes() : 0;
  return {
      .workReqId = wqe.getWorkReqId().value(),
      .status = status,
      .maybeImmData = wqe.getImmediateData(),
      .sizeBytes = sizeBytes,
      .qpn = qpNum_,
      .tag = tagNum_,
      .opcode = wqe.ibvCompletionOpcode()};
}

void Tag::enqueueCqeForWqe(const Wqe& wqe, WorkCompletionStatus status) {
  const bool isRecv = wqe.getWorkType() == Wqe::WorkType::kReceive ||
      wqe.getWorkType() == Wqe::WorkType::kVirtualReceive;
  CompletionQueue* cq = completionQueue(isRecv);
  if (cq == nullptr) {
    return;
  }
  cq->enqueue(cqeForWqe(wqe, status));
}

void Tag::completeSingleTxWqe(
    const Wqe& wqe,
    WorkCompletionStatus status,
    folly::Synchronized<MsnSpan>::LockedPtr&) {
  const bool reportsCompletion =
      status != kSuccess || wqe.isSignaled() || sqSigAll_;
  // The entry goes back before the completion that lets a client reuse it.
  releaseSendQueueEntry();
  if (reportsCompletion) {
    CompletionQueue* sendCq = completionQueue(/*forReceive=*/false);
    if (sendCq != nullptr && !sendCq->tryEnqueue(cqeForWqe(wqe, status))) {
      // The work request completes either way; only the report is lost.
      LOG_FIRST_N(ERROR, 20)
          << "qp " << qpNum_ << " tag " << (uint32_t)tagNum_
          << " dropped the completion for work request "
          << wqe.getWorkReqId().value_or(0)
          << ": the queue holds the depth given to ibv_create_cq, so a "
             "client polling the queue will not see it";
    }
  }
}

bool Tag::retireCompletedWqesAtMapHead(
    folly::Synchronized<MsnSpan>::LockedPtr& lockedSpan) {
  auto& wqes = lockedSpan->wqes;
  // The work at `msn` if the head can retire through it. Emplaced at post and
  // erased only here, so at() finds every msn through the completed one.
  auto retirableHead = [&wqes, &lockedSpan](uint16_t msn) -> Wqe* {
    if (!beforeOrEqual(msn, lockedSpan->inOrderCompletedMsn)) {
      return nullptr;
    }
    Wqe& head = *CHECK_NOTNULL(wqes.at(msn).get());
    return head.isHeldForRequestFrame() ? nullptr : &head;
  };

  const uint16_t retiredBefore = lockedSpan->inOrderRetiredMsn;
  uint16_t retiredThrough = retiredBefore;
  uint16_t readAtomicsRetired = 0;
  for (uint16_t msn = retiredBefore + 1; Wqe* head = retirableHead(msn);
       ++msn) {
    readAtomicsRetired += head->hasRequestFrame() ? 1 : 0;
    wqes.erase(msn);
    retiredThrough = msn;
  }

  lockedSpan->inOrderRetiredMsn = retiredThrough;
  // One read or atomic held a responder slot for each request frame retired.
  if (readAtomicsRetired > 0) {
    outstandingReadAtomic_.fetch_sub(
        readAtomicsRetired, std::memory_order_relaxed);
  }
  return retiredThrough != retiredBefore;
}

// A responder's send holds an msn in the initiator's order, so the receive-side
// work behind that msn completes only once the send is acked.
bool Tag::completeReceivesReleasedBySends(
    const std::vector<std::reference_wrapper<Wqe>>& sends) {
  bool everyCsnInSequence = true;
  for (Wqe& wqe : sends) {
    CHECK(wqe.getMsn().has_value())
        << "a virtual WQE is keyed by the initiator's MSN, so one reaching "
           "completion without an MSN could not have been sent";
    const uint16_t msn = wqe.getMsn().value();
    const bool sendReachedTheInitiator = !wqe.isFailing();
    const bool receiveSideHoldsThisMsn =
        initiatorMsnToResponderWqes_.contains(msn);
    if (sendReachedTheInitiator && receiveSideHoldsThisMsn &&
        !markDeliveredAndCompleteInOrder(msn)) {
      everyCsnInSequence = false;
    }
  }
  return everyCsnInSequence;
}

bool Tag::handleTxWqesCompletion(
    const std::vector<std::reference_wrapper<Wqe>>& wqesToComplete) {
  // Split into virtual (remoteMsnToWqe_) and local (msnSpan_) WQEs
  // to avoid acquiring locks on maps we don't need.
  std::vector<std::reference_wrapper<Wqe>> virtualWqes;
  std::vector<std::reference_wrapper<Wqe>> localWqes;
  for (Wqe& wqe : wqesToComplete) {
    if (wqe.isVirtual()) {
      virtualWqes.emplace_back(wqe);
    } else {
      localWqes.emplace_back(wqe);
    }
  }

  bool everyCsnInSequence = true;
  if (!virtualWqes.empty()) {
    everyCsnInSequence = completeReceivesReleasedBySends(virtualWqes);
    // Taken after those completions, which reach the receive maps and the
    // completion queue.
    auto lockedRemoteMap = remoteMsnToWqe_.wlock();
    for (Wqe& wqe : virtualWqes) {
      LOG(INFO) << "tx: VirtualSendWqe completed on QP: " << qpNum_
                << " Tag: " << (uint32_t)tagNum_ << ": " << wqe;
      lockedRemoteMap->erase(wqe.getMsn().value());
    }
  }

  if (!localWqes.empty()) {
    auto lockedSpan = msnSpan_.wlock();
    WorkCompletionStatus status = isInErrorState() ? kWrFlushErr : kSuccess;

    for (Wqe& wqe : localWqes) {
      if (!after(wqe.getMsn().value(), lockedSpan->inOrderCompletedMsn)) {
        LOG(ERROR) << "Tx: WQE completed on QP: " << qpNum_
                   << " Tag: " << (uint32_t)tagNum_ << " (already processed)"
                   << " wqe=" << wqe;
      }
    }

    // The work at the head of the window, where it is finished and so due to
    // report next. Read again after each completion, which moves the head.
    auto nextToReport = [&lockedSpan]() -> Wqe* {
      auto wqeIt = lockedSpan->wqes.find(
          static_cast<uint16_t>(lockedSpan->inOrderCompletedMsn + 1));
      return wqeIt != lockedSpan->wqes.end() &&
              wqeIt->second->isReadyToComplete()
          ? wqeIt->second.get()
          : nullptr;
    };

    // Process completions in MSN order.
    for (Wqe* wqe = nextToReport(); wqe != nullptr; wqe = nextToReport()) {
      LOG_FIRST_N(INFO, 20) << "tx: WQE completed on QP: " << qpNum_
                            << " Tag: " << (uint32_t)tagNum_ << ": " << *wqe;
      completeSingleTxWqe(
          *wqe,
          wqe->getCompletionStatusOverride().value_or(status),
          lockedSpan);
      ++lockedSpan->inOrderCompletedMsn;
    }
    retireCompletedWqesAtMapHead(lockedSpan);
  }
  signalMsnSpanRoom();
  signalReadAtomicRoom();
  return everyCsnInSequence;
}

void Tag::waitForFree() {
  freeSem_.wait();
}

folly::coro::Task<void> Tag::co_waitForFree() {
  co_await freeSem_.co_wait();
}

void Tag::markFree() {
  freeSem_.signal();
}

bool Tag::isMarkedForDeletion() const {
  return *markedForDeletion_.rlock();
}

void Tag::markForDeletion() {
  // Deliberately does not take the Tag: the caller holds the QP-map write lock,
  // and waiting for a Tag beneath it would stall every QP lookup in the
  // process. A Tag never leaves IBV_QPS_ERR, so an abandonment already decided
  // on stays decided whatever this does.
  //
  // Idempotent: a repeated or concurrent destroy of the same ibv_qp (Tag) may
  // mark it more than once.
  *markedForDeletion_.wlock() = true;
  signalMsnSpanRoom();
  signalReadAtomicRoom();
}

uint32_t Tag::completeWqeThatFailsTheTag(
    Wqe& wqe,
    enum WorkCompletionStatus status) {
  DCHECK(!wqe.isVirtual())
      << "a virtual send's posns never entered the unsent count, so the count "
         "returned here would take off posns that were never added";
  const uint32_t abandonedPosnCount = wqe.markFailing(status);
  completeSettledFailedWqes();
  return abandonedPosnCount;
}

uint32_t Tag::failPendingWqesIfInErrorWithTagHeld() {
  if (!isInErrorState()) {
    return 0;
  }
  auto retirementGuard = lockForWqeRetirement();
  // A tag in error never fills its receives.
  flushPendingReceiveWqesInError();
  // Pending work counts the virtual sends; held work counts the local map,
  // including an entry reported and kept only for a request frame. Skipping
  // the sweep takes both.
  if (getNumPendingWqes() == 0 && getNumHeldTxWqes() == 0) {
    return 0;
  }
  // Marking settles a work request whose posns were all unsent.
  const uint32_t abandonedPosnCount = markPendingWqesFailing(kWrFlushErr);
  // No ack or retransmission scan comes back for a settled work request.
  completeSettledFailedWqes();
  return abandonedPosnCount;
}

std::vector<std::reference_wrapper<Wqe>> Tag::pendingWqesInMsnOrder(
    const MsnSpan& lockedSpan) const {
  std::vector<std::reference_wrapper<Wqe>> pending;
  pending.reserve(lockedSpan.wqes.size());
  for (const auto [msn, wqe] :
       iterateFrom(lockedSpan.wqes, lockedSpan.inOrderCompletedMsn)) {
    if (lockedSpan.isTxCompletionCommitted(msn)) {
      // The walk starts just past inOrderCompletedMsn and wraps, so the
      // reported WQEs come last: the first of them ends the run this tag has
      // yet to complete. They are held only until their request frames are
      // acked.
      break;
    }
    pending.emplace_back(*wqe);
  }
  return pending;
}

bool Tag::isTxCompletionCommitted(uint16_t msn) const {
  return msnSpan_.rlock()->isTxCompletionCommitted(msn);
}

void Tag::retireAfterRequestFrameSettled(Wqe& wqe) {
  DCHECK(wqe.getMsn().has_value());
  if (wqe.isFailingAndSettled()) {
    // The result is dropped: it flags a receive refused for an out-of-
    // sequence csn, and a failing send releases no receive to refuse.
    std::ignore = handleTxWqesCompletion({wqe});
    return;
  }
  {
    auto lockedSpan = msnSpan_.wlock();
    if (!retireCompletedWqesAtMapHead(lockedSpan)) {
      return;
    }
  }
  signalMsnSpanRoom();
  signalReadAtomicRoom();
}

uint32_t Tag::markPendingWqesFailing(WorkCompletionStatus status) {
  uint32_t abandonedCount = 0;
  {
    auto lockedSpan = msnSpan_.wlock();
    bool errorAssigned = false;
    for (Wqe& wqe : pendingWqesInMsnOrder(*lockedSpan)) {
      // A work request already failing keeps the error it failed with, so
      // passing over it hands `status` to the first that can still report one.
      if (!wqe.isFailing()) {
        // A WQE holding a status of its own keeps that status, so it cannot
        // report this error either.
        const bool carriesTheError =
            !errorAssigned && !wqe.getCompletionStatusOverride().has_value();
        errorAssigned |= carriesTheError;
        abandonedCount +=
            wqe.markFailing(carriesTheError ? status : kWrFlushErr);
        // The count is the posns still on the wire, which tells an operator
        // how long this work request may take to report.
        LOG(WARNING) << "Failing send WQE, " << wqe.getUnresolvedTxCount()
                     << " still unresolved: " << wqe;
      }
    }
  }

  // VirtualSendWqes produce no CQE, so their status is immaterial; they are
  // marked to stop the tx path pulling further packets from them. Their posns
  // never entered the unsent count, so they are not returned to it.
  {
    auto lockedRemoteMsnToWqe = remoteMsnToWqe_.wlock();
    for (auto& [msn, wqe] : *lockedRemoteMsnToWqe) {
      wqe->markFailing(kWrFlushErr);
    }
  }
  return abandonedCount;
}

void Tag::completeSettledFailedWqes() {
  std::vector<std::reference_wrapper<Wqe>> failedWqes;
  // Already reported and held only for a request frame that has since left the
  // wire. The ack path settles those where it stands; abandonment has no wqe
  // to hand anywhere, so this sweep is where they are found.
  bool hasSettledAfterCompletion = false;
  {
    auto lockedSpan = msnSpan_.rlock();
    for (const auto& [msn, wqe] : lockedSpan->wqes) {
      if (wqe->isFailingAndSettled()) {
        failedWqes.emplace_back(*wqe);
      } else if (
          lockedSpan->isTxCompletionCommitted(msn) &&
          !wqe->isHeldForRequestFrame()) {
        hasSettledAfterCompletion = true;
      }
    }
  }
  {
    // A virtual send reports no CQE, but it still holds the WQE and its posns
    // until it is retired -- which is how it completes -- so no other path
    // finds it left behind.
    auto lockedRemoteMsnToWqe = remoteMsnToWqe_.rlock();
    for (const auto& [msn, wqe] : *lockedRemoteMsnToWqe) {
      if (wqe->isFailingAndSettled()) {
        failedWqes.emplace_back(*wqe);
      }
    }
  }
  if (!failedWqes.empty()) {
    // The result is dropped: it flags a receive refused for an out-of-
    // sequence csn, and every WQE here is failing, so none releases one.
    std::ignore = handleTxWqesCompletion(failedWqes);
  }
  if (hasSettledAfterCompletion) {
    // One pass retires the whole run of them at the map head, so a pointer to
    // each would leave every one but the first pointing to a freed work
    // request.
    {
      auto lockedSpan = msnSpan_.wlock();
      retireCompletedWqesAtMapHead(lockedSpan);
    }
    signalMsnSpanRoom();
    signalReadAtomicRoom();
  }
}

void Tag::flushAllPendingWqesInError() {
  // Flush all send WQEs in msnSpan_.wqes; one holding its own error reports
  // that instead of the flush.
  {
    auto lockedSpan = msnSpan_.wlock();
    std::optional<uint16_t> retiredThroughMsn;
    for (Wqe& wqe : pendingWqesInMsnOrder(*lockedSpan)) {
      LOG(WARNING) << "Flushing pending send WQE in error: " << wqe;
      enqueueCqeForWqe(
          wqe, wqe.getCompletionStatusOverride().value_or(kWrFlushErr));
      DCHECK(wqe.getMsn().has_value());
      retiredThroughMsn = wqe.getMsn().value();
      releaseSendQueueEntry();
    }
    lockedSpan->wqes.clear();
    outstandingReadAtomic_.store(0, std::memory_order_relaxed);
    if (retiredThroughMsn.has_value()) {
      const uint16_t retiredCursor = lockedSpan->inOrderRetiredMsn;
      CHECK(after(*retiredThroughMsn, retiredCursor))
          << "the map held msn " << *retiredThroughMsn
          << " and the retired cursor was " << retiredCursor
          << ", further apart than the modular span is wide";
      lockedSpan->inOrderCompletedMsn = *retiredThroughMsn;
    }
    // The clear above erased the msns held for their request frames too, which
    // have no pending work request to advance the cursor.
    lockedSpan->inOrderRetiredMsn = lockedSpan->inOrderCompletedMsn;
  }
  signalMsnSpanRoom();
  signalReadAtomicRoom();

  // Clear VirtualSendWqes in remoteMsnToWqe_ (no CQE needed for virtual WQEs)
  remoteMsnToWqe_.wlock()->clear();

  flushPendingReceiveWqesInError();
}

void Tag::flushPendingReceiveWqesInError() {
  // The two maps hold one sequence between them, ordered by csn: a receive
  // moves to the bound map when a message binds to it, and a message arriving
  // out of order binds a later-posted receive first. Ordering each map on its
  // own key would report them out of the order the client posted them.
  //
  // initiatorMsnToResponderWqes_ takes no lock of its own: the WQE-retirement
  // guard the caller holds is what excludes the receive path, its only other
  // user, from the WQEs being completed and erased.
  std::vector<std::pair<uint16_t, Wqe*>> orderedByCsn;
  orderedByCsn.reserve(initiatorMsnToResponderWqes_.size());
  for (auto& [msn, wqe] : initiatorMsnToResponderWqes_) {
    if (wqe->isVirtual()) {
      // A virtual receive reports no CQE, so it only needs clearing below.
      continue;
    }
    DCHECK(wqe->getCsn().has_value());
    orderedByCsn.emplace_back(wqe->getCsn().value(), wqe.get());
  }

  auto lockedReceives = pendingReceives_.wlock();
  for (auto& [csn, wqe] : lockedReceives->receivesByCsn) {
    // ReceiveWqes are never virtual
    orderedByCsn.emplace_back(csn, wqe.get());
  }

  // Csns wrap, so order by distance from the last one completed, which every
  // outstanding receive sits after.
  const uint16_t base = receiveSequence_.cumulativeCompletedCsn();
  std::sort(
      orderedByCsn.begin(),
      orderedByCsn.end(),
      [base](const auto& a, const auto& b) {
        return static_cast<uint16_t>(a.first - base) <
            static_cast<uint16_t>(b.first - base);
      });

  for (auto& [csn, wqe] : orderedByCsn) {
    LOG(WARNING) << "Flushing pending receive WQE (csn=" << csn
                 << ") in error: " << *wqe;
    releaseReceiveQueueEntry();
    enqueueCqeForWqe(
        *wqe, wqe->getCompletionStatusOverride().value_or(kWrFlushErr));
  }

  initiatorMsnToResponderWqes_.clear();
  lockedReceives->receivesByCsn.clear();
}

Wqe* FOLLY_NULLABLE Tag::getPendingWqe(uint16_t msn) {
  auto lockedSpan = msnSpan_.rlock();
  if (lockedSpan->isTxCompletionCommitted(msn)) {
    // Already reported, so it cannot report an error to the client.
    return nullptr;
  }
  auto wqeIt = lockedSpan->wqes.find(msn);
  if (wqeIt == lockedSpan->wqes.end()) {
    return nullptr;
  }
  // Only a posted send WQE is keyed by local msn; the virtual ones live in
  // remoteMsnToWqe_ and initiatorMsnToResponderWqes_.
  DCHECK(!wqeIt->second->isVirtual());

  return wqeIt->second.get();
}

Wqe* FOLLY_NULLABLE Tag::getEarliestPendingWqe() {
  auto lockedSpan = msnSpan_.rlock();
  // Msns wrap, so the numerically smallest key is not the earliest posted once
  // the outstanding window crosses zero. The window follows the last msn
  // completed in order, where the wrapping iteration starts.
  const auto pending = pendingWqesInMsnOrder(*lockedSpan);
  if (pending.empty()) {
    return nullptr;
  }
  Wqe& earliestWqe = pending.front();
  // Only a posted send WQE is keyed by local msn; the virtual ones live in
  // remoteMsnToWqe_ and initiatorMsnToResponderWqes_.
  DCHECK(!earliestWqe.isVirtual());
  return &earliestWqe;
}

bool Tag::arePriorWqesCompleteForFence(uint16_t currentMsn) const {
  auto lockedSpan = msnSpan_.rlock();
  uint16_t prevMsn = currentMsn - 1;

  // Expected case: previous WQE is exactly the last completed in order.
  // First-WQE case also hits this path: msnSpan_.inOrderCompletedMsn starts at
  // (initialMsn - 1), so prevMsn matches it immediately.
  if (prevMsn == lockedSpan->inOrderCompletedMsn) {
    return true;
  }

  // Prior WQEs still incomplete; fence cannot be released yet.
  if (after(prevMsn, lockedSpan->inOrderCompletedMsn)) {
    return false;
  }

  // msnSpan_.inOrderCompletedMsn has advanced past prevMsn (i.e., it equals
  // currentMsn or beyond), meaning the fenced WQE itself has already been
  // completed. We should not be trying to transmit it.
  LOG(ERROR) << "arePriorWqesCompleteForFence: inOrderCompletedMsn ("
             << lockedSpan->inOrderCompletedMsn
             << ") has advanced past prevMsn (" << prevMsn
             << ") for currentMsn " << currentMsn
             << " — operating on an already-completed WQE";
  return true;
}

} // namespace libsoftmetaroce
