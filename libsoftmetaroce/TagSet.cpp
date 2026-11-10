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

#include <folly/ScopeGuard.h>
#include <algorithm>
#include <bitset>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/MultiUnboundedQueue.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/QpState.h>
#include <libsoftmetaroce/Tag.h>
#include <libsoftmetaroce/TagSet.h>
#include <libsoftmetaroce/Wqe.h>

namespace libsoftmetaroce {

TagSet::TagSet(
    uint32_t qpNum,
    std::function<void(uint8_t, uint16_t)> rnrCancelCallback)
    : rnrCancelCallback_(std::move(rnrCancelCallback)), qpNum_(qpNum) {
  CHECK(rnrCancelCallback_);
}

TagSet::~TagSet() {
  for (auto& tag : tags_) {
    tag->releaseCompletionQueueReferences();
  }
}

void TagSet::throwIfCannotAdd(
    uint8_t tagNum,
    CompletionQueue* sendCq,
    struct ibv_qp* ibvQp) const {
  if (!ibvQp) {
    throw std::invalid_argument(
        fmt::format(
            "ibv_qp cannot be null when adding Tag: '{}'", (uint32_t)tagNum));
  }
  if (!sendCq) {
    throw std::invalid_argument(
        fmt::format(
            "send CompletionQueue cannot be null when adding Tag: '{}'",
            (uint32_t)tagNum));
  }
}

void TagSet::add(
    uint8_t tagNum,
    CompletionQueue* sendCq,
    CompletionQueue* recvCq,
    struct ibv_qp* ibvQp,
    bool sqSigAll,
    uint32_t maxInlineData,
    uint32_t sendQueueDepth,
    uint32_t receiveQueueDepth,
    SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue) {
  throwIfCannotAdd(tagNum, sendCq, ibvQp);
  std::scoped_lock lock(stateLock_);
  addWithLockHeld(
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

Tag* FOLLY_NULLABLE TagSet::addIfAllTagsInReset(
    uint8_t tagNum,
    CompletionQueue* sendCq,
    CompletionQueue* recvCq,
    struct ibv_qp* ibvQp,
    bool sqSigAll,
    uint32_t maxInlineData,
    uint32_t sendQueueDepth,
    uint32_t receiveQueueDepth,
    SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue) {
  throwIfCannotAdd(tagNum, sendCq, ibvQp);
  std::scoped_lock lock(stateLock_);
  if (!allTagsInStateWithLockHeld(IBV_QPS_RESET)) {
    return nullptr;
  }
  return addWithLockHeld(
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

Tag* TagSet::addWithLockHeld(
    uint8_t tagNum,
    CompletionQueue* sendCq,
    CompletionQueue* recvCq,
    struct ibv_qp* ibvQp,
    bool sqSigAll,
    uint32_t maxInlineData,
    uint32_t sendQueueDepth,
    uint32_t receiveQueueDepth,
    SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue) {
  const auto pos = std::lower_bound(
      tags_.begin(), tags_.end(), tagNum, [](const auto& tag, uint8_t num) {
        return tag->getTagNum() < num;
      });
  if (pos != tags_.end() && (*pos)->getTagNum() == tagNum) {
    throw std::invalid_argument(
        fmt::format(
            "Tag: '{}' already exists on QP: '{}'", (uint32_t)tagNum, qpNum_));
  }
  const auto index = pos - tags_.begin();
  CHECK(!wqeQueue_.containsQueue(tagNum))
      << "a WQE queue is created with its Tag, and this Tag is new";
  // Reserved before the queue is added, so the insert below cannot throw and
  // leave a queue without its Tag.
  tags_.reserve(tags_.size() + 1);
  auto tag = std::make_unique<Tag>(
      qpNum_,
      tagNum,
      sendCq,
      recvCq,
      ibvQp,
      sqSigAll,
      maxInlineData,
      sendQueueDepth,
      receiveQueueDepth,
      sharedReceiveQueue);
  wqeQueue_.addQueue(tagNum);
  return tags_.insert(tags_.begin() + index, std::move(tag))->get();
}

void TagSet::add(
    uint8_t tagNum,
    CompletionQueue* cq,
    struct ibv_qp* ibvQp,
    bool sqSigAll,
    uint32_t maxInlineData,
    uint32_t sendQueueDepth,
    uint32_t receiveQueueDepth,
    SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue) {
  add(tagNum,
      cq,
      nullptr,
      ibvQp,
      sqSigAll,
      maxInlineData,
      sendQueueDepth,
      receiveQueueDepth,
      sharedReceiveQueue);
}

size_t TagSet::size() const {
  std::scoped_lock lock(stateLock_);
  return tags_.size();
}

Tag& TagSet::at(uint8_t tagNum) const {
  Tag* const tag = getTagByNumber(tagNum);
  if (tag == nullptr) {
    throw std::out_of_range(
        fmt::format("Tag: {} not found for QP: {}", (uint32_t)tagNum, qpNum_));
  }
  return *tag;
}

Tag* FOLLY_NULLABLE TagSet::getTagByNumber(uint8_t tagNum) const {
  for (const auto& tag : tags_) {
    if (tag->getTagNum() == tagNum) {
      return tag.get();
    }
  }
  return nullptr;
}

Tag* FOLLY_NULLABLE TagSet::getTagInAnyState(uint8_t tagNum) const {
  std::scoped_lock lock(stateLock_);
  return getTagByNumber(tagNum);
}

struct ibv_pd* TagSet::protectionDomain() const {
  std::scoped_lock lock(stateLock_);
  return tags_.empty() ? nullptr : tags_.front()->getIbvQp().pd;
}

uint32_t TagSet::getQpNum() const {
  return qpNum_;
}

bool TagSet::contains(uint8_t tagNum) const {
  std::scoped_lock lock(stateLock_);
  return getTagByNumber(tagNum) != nullptr;
}

bool TagSet::allTagsMarkedForDeletion() const {
  std::scoped_lock lock(stateLock_);
  for (const auto& tag : tags_) {
    if (!tag->isMarkedForDeletion()) {
      return false;
    }
  }
  return true;
}

bool TagSet::postWqe(uint8_t tagNum, std::unique_ptr<Wqe> wqe) {
  Tag& tag = at(tagNum);
  const bool reserved = wqe->getWorkType() == Wqe::WorkType::kReceive
      ? tag.tryReserveReceiveQueueEntry()
      : tag.tryReserveSendQueueEntry();
  if (!reserved) {
    return false;
  }
  wqeQueue_.enqueue(tagNum, std::move(wqe));
  return true;
}

folly::coro::Task<void> TagSet::processWqes(
    QpArbiterIface::ReadySignal& readySignal,
    uint16_t mtu) {
  while (true) {
    co_await folly::coro::co_safe_point;
    auto [tagNum, wqe] = co_await wqeQueue_.co_dequeue();
    bool hasSendableWork = false;
    bool tagIsInError = false;
    bool postErroredTheTag = false;
    // Read before the post below moves the WQE away.
    const uint32_t sendablePosnCount = wqe->getSendablePosnCount();
    std::optional<uint16_t> maybeRnrCancelCsn;
    // A Tag lives in the set until the whole QP is torn down, so a WQE the
    // queue accepted still has one; at() says so by throwing if it does not.
    Tag& tag = at(tagNum);

    // Only this coroutine assigns msns, so room found here is still there
    // below.
    const bool isReceiveWqe = wqe->getWorkType() == Wqe::WorkType::kReceive;
    const bool takesAnMsn = !isReceiveWqe;
    if (takesAnMsn && !tag.isMarkedForDeletion() && !tag.hasMsnSpanRoom()) {
      // Once on entering the stall rather than once per wake, so one stall
      // does not spend the whole bound and hide the next.
      LOG_FIRST_N(WARNING, 10)
          << "qp " << qpNum_ << " tag " << (uint32_t)tagNum
          << " has a full msn-span of work held rather than retired; this "
          << "queue pair posts no further work, on any tag, until the retired "
          << "cursor moves";
      co_await tag.co_waitForMsnSpanRoom();
    }

    const bool takesAReadAtomicSlot = wqe->hasRequestFrame();
    // Only this coroutine counts one in, and retirement only counts them out.
    if (takesAReadAtomicSlot && !tag.postWillBeFlushed() &&
        !tag.hasReadAtomicRoom()) {
      LOG_FIRST_N(WARNING, 10)
          << "qp " << qpNum_ << " tag " << (uint32_t)tagNum
          << " holds its negotiated depth of reads and atomics; this queue "
          << "pair posts no further work, on any tag, until one of them "
          << "retires";
      co_await tag.co_waitForReadAtomicRoom();
    }

    // Hold the tag across the post so that posting a wqe does not race a tag
    // error raised by a flow that owns the Tag. The client's own transition
    // owns no Tag; the flag it raises is what covers that one.
    co_await tag.co_waitForFree();
    {
      auto tagGuard = folly::makeGuard([&tag] { tag.markFree(); });
      const bool wasInError = tag.isInErrorState();
      if (isReceiveWqe) {
        std::unique_ptr<ReceiveWqe> receiveWqe(
            static_cast<ReceiveWqe*>(wqe.release()));
        maybeRnrCancelCsn = tag.processReceiveWqe(std::move(receiveWqe), mtu);
      } else {
        hasSendableWork = tag.processTransmitWqe(std::move(wqe));
      }
      if (hasSendableWork) {
        // Under the same Tag hold as the post: a sweep that got in between
        // would subtract these posns before this adds them, underflowing the
        // unsent count the sender's rate hint reads.
        unsentPosnCount_.fetch_add(
            sendablePosnCount, std::memory_order_relaxed);
      }
      // Read under the guard the post was made beneath, so it reflects the
      // state this post left behind rather than a later thread's.
      tagIsInError = tag.isInErrorState();
      // The one error transition with no owner to publish it. A tag already in
      // error needs none: that post reported its own work as it was made.
      postErroredTheTag = !wasInError && tagIsInError;
    }
    // Signals the arbiter, so it stays outside the Tag guard.
    if (maybeRnrCancelCsn.has_value()) {
      rnrCancelCallback_(tagNum, *maybeRnrCancelCsn);
    }
    if (postErroredTheTag) {
      notifySenderTagEnteredError();
    }
    if (hasSendableWork || tagIsInError) {
      // Signalled with the Tag guard dropped and no TagSet lock held: the
      // arbiter waits on both to find WQEs, so signalling under either would
      // close a cycle.
      co_await readySignal.co_signalReady();
    }
    // After the ready signal: a waiter that sees the count move drives the
    // arbiter next, so the QP must already be registered ready.
    if (co_await wqeQueue_.co_empty()) {
      std::lock_guard<std::mutex> lock(postWqeCvMutex_);
      postWqeCv_.notify_all();
    }
  }
  co_return;
}

[[nodiscard]] int TagSet::getNumPendingRWqes() const {
  int numPendingRWqes = 0;
  for (const auto& tag : tags_) {
    numPendingRWqes += tag->getNumPendingRWqes();
  }
  return numPendingRWqes;
}

bool TagSet::waitForWqesToBePostedGeneric(
    std::chrono::milliseconds timeoutMs,
    const std::function<bool()>& foundDesiredWqes) {
  std::unique_lock<std::mutex> lock(postWqeCvMutex_);
  auto start = std::chrono::steady_clock::now();
  // Loop to handle spurious wakeups.
  while (!foundDesiredWqes()) {
    // Check time diff instead of returned timeout status to account for
    // multiple iterations due to spurious wakeups.
    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    if (diff >= timeoutMs) {
      return false;
    }
    postWqeCv_.wait_for(lock, timeoutMs - diff, foundDesiredWqes);
  }
  return true;
}

bool TagSet::waitForWqesToBePosted(
    int numWqes,
    std::chrono::milliseconds timeoutMs) {
  return waitForWqesToBePostedGeneric(
      timeoutMs, [this, numWqes]() { return getNumPendingWqes() >= numWqes; });
}

bool TagSet::waitForRWqesToBePosted(
    int numWqes,
    std::chrono::milliseconds timeoutMs) {
  return waitForWqesToBePostedGeneric(
      timeoutMs, [this, numWqes]() { return getNumPendingRWqes() >= numWqes; });
}

[[nodiscard]] bool TagSet::hasPendingWqes() const {
  return std::any_of(tags_.begin(), tags_.end(), [](const auto& tag) {
    return tag->hasPendingWqes();
  });
}

[[nodiscard]] int TagSet::getNumPendingWqes() const {
  int numPendingWqes = 0;
  for (const auto& tag : tags_) {
    numPendingWqes += tag->getNumPendingWqes();
  }
  return numPendingWqes;
}

Tag& TagSet::asSchedulable(TagsByNum::value_type& entry) {
  return *entry;
}

std::optional<std::pair<Wqe*, Tag*>> TagSet::findNextWqe(bool intendToSend) {
  if (!intendToSend) {
    return peekNextWqe();
  }
  return findNextWqeToSend();
}

std::optional<std::pair<Wqe*, Tag*>> TagSet::peekNextWqe() {
  // A readiness peek changes no scheduler state, and takes Tag semaphores
  // without holding schedulerLock_.
  std::optional<size_t> startIdx;
  {
    std::scoped_lock lock(schedulerLock_);
    auto start = scheduler_.peek(tags_.begin(), tags_.end(), asSchedulable);
    if (start != tags_.end()) {
      startIdx = static_cast<size_t>(start - tags_.begin());
    }
  }
  if (!startIdx.has_value()) {
    return std::nullopt;
  }
  for (size_t i = 0; i < tags_.size(); ++i) {
    Tag& tag = *tags_[(*startIdx + i) % tags_.size()];
    if (auto* wqe = tag.findNextWqe()) {
      return std::make_pair(wqe, &tag);
    }
  }
  return std::nullopt;
}

std::optional<std::pair<Wqe*, Tag*>> TagSet::findNextWqeToSend() {
  // Each tag is tried at most once per scan, even after the scheduler refills.
  std::bitset<std::numeric_limits<uint8_t>::max() + 1> tried;
  const auto wasTried = [&tried](const TagsByNum::value_type& entry) {
    return tried.test(entry->getTagNum());
  };
  for (size_t attempt = 0; attempt < tags_.size(); ++attempt) {
    // Held across the Tag semaphore wait. No Tag is added once the QP has left
    // RESET, so it stays valid.
    TagsByNum::iterator selected;
    // Claim this turn up front, so a hit costs one lock hold; a miss
    // forfeits the claimed visit with the rest below.
    {
      std::scoped_lock lock(schedulerLock_);
      selected = scheduler_.select(
          tags_.begin(), tags_.end(), asSchedulable, wasTried);
      if (selected == tags_.end()) {
        return std::nullopt;
      }
      tried.set((*selected)->getTagNum());
      scheduler_.noteServed(selected, asSchedulable);
    }
    Tag& tag = **selected;
    if (auto* wqe = tag.findNextWqe()) {
      return std::make_pair(wqe, &tag);
    }
    std::scoped_lock lock(schedulerLock_);
    scheduler_.noteSkipped(selected, asSchedulable);
  }
  return std::nullopt;
}

void TagSet::notifySenderTagEnteredError() {
  tagErrorSweepPending_.store(true, std::memory_order_release);
}

void TagSet::notifySenderTagLeftService() {
  tagLeftServicePending_.store(true, std::memory_order_release);
}

bool TagSet::takeTagLeftServiceNotice() {
  // Read before the exchange, as the sweep latch is: this runs on every packet
  // the sender builds, and an unconditional read-modify-write would dirty the
  // line each time for a flag that is almost always clear.
  if (!tagLeftServicePending_.load(std::memory_order_acquire)) {
    return false;
  }
  return tagLeftServicePending_.exchange(false, std::memory_order_acq_rel);
}

bool TagSet::noTagSurvives() const {
  for (const auto& tag : tags_) {
    if (!tag->isInErrorState() && !tag->isMarkedForDeletion()) {
      return false;
    }
  }
  return true;
}

SenderWake TagSet::enterTagErrorWithTagHeld(Tag& tag) {
  {
    // Under stateLock_ so this serializes with the whole-QP walk in
    // setIbvQpState(); owning a Tag and then taking it briefly is the send
    // path's own order.
    std::scoped_lock lock(stateLock_);
    tag.setErrorStateWithoutNotification();
  }
  notifySenderTagEnteredError();
  notifySenderTagLeftService();
  tag.signalMsnSpanRoom();
  tag.signalReadAtomicRoom();
  return SenderWake::NEEDED;
}

void TagSet::failWqesOfTagsInError() {
  // Read before the exchange: this runs on every packet the sender builds, and
  // an unconditional read-modify-write would dirty the line each time for a
  // flag that is almost always clear.
  if (!tagErrorSweepPending_.load(std::memory_order_acquire)) {
    return;
  }
  // Cleared before the walk: a Tag entering error after this raises it again,
  // so a Tag the walk below misses is swept on a later visit.
  if (!tagErrorSweepPending_.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  // A Tag in error stays there, so no Tag is added while this walks.
  for (const auto& tag : tags_) {
    auto tagGuard = tag->scopedLock();
    const uint32_t abandonedPosnCount =
        tag->failPendingWqesIfInErrorWithTagHeld();
    subtractUnsentPosns(abandonedPosnCount);
  }
}

bool TagSet::allTagsInState(ibv_qp_state state) const {
  std::scoped_lock lock(stateLock_);
  return allTagsInStateWithLockHeld(state);
}

bool TagSet::allTagsInStateWithLockHeld(ibv_qp_state state) const {
  return std::all_of(tags_.begin(), tags_.end(), [state](const auto& tag) {
    return loadQpState(tag->getIbvQp()) == state;
  });
}

void TagSet::setIbvQpState(ibv_qp_state state) {
  std::scoped_lock lock(stateLock_);
  for (auto& tag : tags_) {
    // IBV_QPS_ERR is terminal, so a Tag already there keeps it.
    if (tag->isInErrorState()) {
      continue;
    }
    if (state == IBV_QPS_ERR) {
      tag->setErrorStateWithoutNotification();
      tag->signalMsnSpanRoom();
      tag->signalReadAtomicRoom();
    } else {
      // A concurrent error publication wins over this walk: the
      // compare-and-set moves only a Tag not already in error, and
      // either outcome leaves the Tag where it belongs.
      ibv_qp_state observed = loadQpState(tag->getIbvQp());
      if (observed != IBV_QPS_ERR) {
        std::ignore = compareAndSetQpState(tag->getIbvQp(), observed, state);
      }
    }
  }
  if (state == IBV_QPS_ERR) {
    // The stores above are race-free against the sweep's loads on their
    // own; this requests that it run, and its release ordering makes the
    // publication visible to it.
    notifySenderTagEnteredError();
  }
}

void TagSet::addUnsentPosns(uint32_t count) {
  unsentPosnCount_.fetch_add(count, std::memory_order_relaxed);
}

void TagSet::decrementUnsentPosns() {
  unsentPosnCount_.fetch_sub(1, std::memory_order_relaxed);
}

void TagSet::subtractUnsentPosns(uint32_t count) {
  unsentPosnCount_.fetch_sub(count, std::memory_order_relaxed);
}

uint32_t TagSet::getUnsentPosnCount() const {
  return unsentPosnCount_.load(std::memory_order_relaxed);
}

} // namespace libsoftmetaroce
