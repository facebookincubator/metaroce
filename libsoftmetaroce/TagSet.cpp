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
  for (auto& [_, tag] : tagNumToTag_) {
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
  std::scoped_lock lock(mutex_);
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

std::shared_ptr<Tag> TagSet::addIfAllTagsInReset(
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
  std::scoped_lock lock(mutex_);
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

std::shared_ptr<Tag> TagSet::addWithLockHeld(
    uint8_t tagNum,
    CompletionQueue* sendCq,
    CompletionQueue* recvCq,
    struct ibv_qp* ibvQp,
    bool sqSigAll,
    uint32_t maxInlineData,
    uint32_t sendQueueDepth,
    uint32_t receiveQueueDepth,
    SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue) {
  auto [it, inserted] = tagNumToTag_.emplace(
      tagNum,
      std::make_shared<Tag>(
          qpNum_,
          tagNum,
          sendCq,
          recvCq,
          ibvQp,
          sqSigAll,
          maxInlineData,
          sendQueueDepth,
          receiveQueueDepth,
          sharedReceiveQueue));
  if (inserted) {
    // Erase the Tag if queue setup throws; a Tag without its queue is unusable.
    auto rollback = folly::makeGuard([&] { tagNumToTag_.erase(tagNum); });
    CHECK(!wqeQueue_.containsQueue(tagNum));
    wqeQueue_.addQueue(tagNum);
    rollback.dismiss();
    return it->second;
  } else {
    throw std::invalid_argument(
        fmt::format(
            "Tag: '{}' already exists on QP: '{}'", (uint32_t)tagNum, qpNum_));
  }
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
  std::shared_lock lock(mutex_);
  return tagNumToTag_.size();
}

std::shared_ptr<Tag> TagSet::at(uint8_t tagNum) const {
  std::shared_lock lock(mutex_);
  auto tagIt = tagNumToTag_.find(tagNum);
  if (tagIt == tagNumToTag_.end()) {
    throw std::out_of_range(
        fmt::format("Tag: {} not found for QP: {}", (uint32_t)tagNum, qpNum_));
  }
  return tagIt->second;
}

std::shared_ptr<Tag> TagSet::getTagByNumber(uint8_t tagNum) const {
  std::shared_lock lock(mutex_);
  auto tagIt = tagNumToTag_.find(tagNum);
  return tagIt == tagNumToTag_.end() ? nullptr : tagIt->second;
}

struct ibv_pd* TagSet::protectionDomain() const {
  std::shared_lock lock(mutex_);
  return tagNumToTag_.empty() ? nullptr
                              : tagNumToTag_.begin()->second->getIbvQp().pd;
}

uint32_t TagSet::getQpNum() const {
  return qpNum_;
}

std::vector<uint8_t> TagSet::getTagNums() const {
  std::shared_lock lock(mutex_);
  std::vector<uint8_t> tagNums;
  tagNums.reserve(tagNumToTag_.size());
  for (const auto& [tagNum, _] : tagNumToTag_) {
    tagNums.push_back(tagNum);
  }
  return tagNums;
}

bool TagSet::contains(uint8_t tagNum) const {
  std::shared_lock lock(mutex_);
  return tagNumToTag_.contains(tagNum);
}

bool TagSet::allTagsMarkedForDeletion() const {
  std::shared_lock lock(mutex_);
  for (const auto& [_, tag] : tagNumToTag_) {
    if (!tag->isMarkedForDeletion()) {
      return false;
    }
  }
  return true;
}

bool TagSet::postWqe(uint8_t tagNum, std::unique_ptr<Wqe> wqe) {
  std::scoped_lock lock(mutex_);
  // Read from the map, not through at(), which takes mutex_ this already holds.
  Tag& tag = *tagNumToTag_.at(tagNum);
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
    // The map lock is not held past this: it guards the map.
    auto tag = at(tagNum);

    // Only this coroutine assigns msns, so room found here is still there
    // below.
    const bool isReceiveWqe = wqe->getWorkType() == Wqe::WorkType::kReceive;
    const bool takesAnMsn = !isReceiveWqe;
    if (takesAnMsn && !tag->isMarkedForDeletion() && !tag->hasMsnSpanRoom()) {
      // Once on entering the stall rather than once per wake, so one stall
      // does not spend the whole bound and hide the next.
      LOG_FIRST_N(WARNING, 10)
          << "qp " << qpNum_ << " tag " << (uint32_t)tagNum
          << " has a full msn-span of work held rather than retired; this "
          << "queue pair posts no further work, on any tag, until the retired "
          << "cursor moves";
      co_await tag->co_waitForMsnSpanRoom();
    }

    const bool takesAReadAtomicSlot = wqe->hasRequestFrame();
    // Only this coroutine counts one in, and retirement only counts them out.
    if (takesAReadAtomicSlot && !tag->postWillBeFlushed() &&
        !tag->hasReadAtomicRoom()) {
      LOG_FIRST_N(WARNING, 10)
          << "qp " << qpNum_ << " tag " << (uint32_t)tagNum
          << " holds its negotiated depth of reads and atomics; this queue "
          << "pair posts no further work, on any tag, until one of them "
          << "retires";
      co_await tag->co_waitForReadAtomicRoom();
    }

    // Hold the tag across the post so that posting a wqe does not race a tag
    // error raised by a flow that owns the Tag. The client's own transition
    // owns no Tag; the flag it raises is what covers that one.
    co_await tag->co_waitForFree();
    {
      auto tagGuard = folly::makeGuard([&tag] { tag->markFree(); });
      const bool wasInError = tag->isInErrorState();
      if (isReceiveWqe) {
        std::unique_ptr<ReceiveWqe> receiveWqe(
            static_cast<ReceiveWqe*>(wqe.release()));
        maybeRnrCancelCsn = tag->processReceiveWqe(std::move(receiveWqe), mtu);
      } else {
        hasSendableWork = tag->processTransmitWqe(std::move(wqe));
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
      tagIsInError = tag->isInErrorState();
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
  std::shared_lock lock(mutex_);
  // sum over all Tags in tagNumToTag_.
  int numPendingRWqes = 0;
  for (const auto& [_, tag] : tagNumToTag_) {
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
  std::shared_lock lock(mutex_);
  return std::any_of(
      tagNumToTag_.begin(), tagNumToTag_.end(), [](const auto& entry) {
        return entry.second->hasPendingWqes();
      });
}

[[nodiscard]] int TagSet::getNumPendingWqes() const {
  std::shared_lock lock(mutex_);
  // sum over all Tags in tagNumToTag_.
  int numPendingWqes = 0;
  for (const auto& [_, tag] : tagNumToTag_) {
    numPendingWqes += tag->getNumPendingWqes();
  }
  return numPendingWqes;
}

std::optional<std::pair<Wqe*, std::shared_ptr<Tag>>> TagSet::findNextWqe(
    bool intendToSend) {
  std::vector<std::pair<uint8_t, std::shared_ptr<Tag>>> tags;
  {
    std::scoped_lock lock(mutex_);
    tags.reserve(tagNumToTag_.size());
    auto nextTagIt = tagNumToTag_.upper_bound(lastTagNumCheckedForWqes_);
    for (size_t i = 0; i < tagNumToTag_.size(); ++i) {
      if (nextTagIt == tagNumToTag_.end()) {
        nextTagIt = tagNumToTag_.begin();
      }
      tags.emplace_back(nextTagIt->first, nextTagIt->second);
      ++nextTagIt;
    }
  }

  // Snapshot so the scan can wait on a busy Tag's semaphore without holding
  // mutex_. Two callers may snapshot the same starting Tag, which costs
  // fairness, not safety: the Tag semaphore serializes WQE access.
  for (const auto& [tagNum, tag] : tags) {
    if (auto* wqe = tag->findNextWqe()) {
      if (intendToSend) {
        std::scoped_lock lock(mutex_);
        lastTagNumCheckedForWqes_ = tagNum;
      }
      return std::make_pair(wqe, tag);
    }
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
  std::shared_lock lock(mutex_);
  for (const auto& [_, tag] : tagNumToTag_) {
    if (!tag->isInErrorState() && !tag->isMarkedForDeletion()) {
      return false;
    }
  }
  return true;
}

SenderWake TagSet::enterTagErrorWithTagHeld(Tag& tag) {
  {
    // Under mutex_ so this serializes with the whole-QP walk in
    // setIbvQpState(); owning a Tag and then taking it briefly is the send
    // path's own order.
    std::scoped_lock lock(mutex_);
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
  // Snapshot under mutex_ and take each Tag outside it: waiting on a Tag
  // semaphore while holding mutex_ would close a cycle against the send path,
  // which owns a Tag and then takes mutex_ to find the next one.
  std::vector<std::shared_ptr<Tag>> tags;
  {
    std::shared_lock lock(mutex_);
    tags.reserve(tagNumToTag_.size());
    for (const auto& [_, tag] : tagNumToTag_) {
      tags.push_back(tag);
    }
  }
  for (const auto& tag : tags) {
    auto tagGuard = tag->scopedLock();
    const uint32_t abandonedPosnCount =
        tag->failPendingWqesIfInErrorWithTagHeld();
    subtractUnsentPosns(abandonedPosnCount);
  }
}

bool TagSet::allTagsInState(ibv_qp_state state) const {
  std::shared_lock lock(mutex_);
  return allTagsInStateWithLockHeld(state);
}

bool TagSet::allTagsInStateWithLockHeld(ibv_qp_state state) const {
  return std::all_of(
      tagNumToTag_.begin(), tagNumToTag_.end(), [state](const auto& entry) {
        return loadQpState(entry.second->getIbvQp()) == state;
      });
}

void TagSet::setIbvQpState(ibv_qp_state state) {
  std::scoped_lock lock(mutex_);
  for (auto& [_, tag] : tagNumToTag_) {
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
