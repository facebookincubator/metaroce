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

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <utility>

#include <fmt/format.h>
#include <folly/Optional.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Error.h>
#include <folly/coro/Mutex.h>
#include <folly/coro/Task.h>
#include <folly/fibers/Semaphore.h>
#include <libsoftmetaroce/BlockingCoroLock.h>

namespace libsoftmetaroce {

// MultiUnboundedQueue is a wrapper around multiple folly::UnboundedQueue
// instances, where each queue is identified by a key. Dequeues are round-robin
// based on the natural ordering of the keys. This is used to simplify async
// waiting on multiple queues.
//
// Thread safety: Enqueues can be concurrent with each other and dequeues.
// Dequeues cannot be concurrent with each other.
template <typename K, typename T>
class MultiUnboundedQueue {
  static constexpr size_t kLgEntriesPerSegment = 5;
  using Queue =
      folly::UnboundedQueue<T, false, true, false, kLgEntriesPerSegment>;

 public:
  folly::coro::Task<void> co_addQueue(K queueKey) {
    auto lock = co_await mutex_.co_scoped_lock();
    addQueueLocked(queueKey);
  }

  void addQueue(K queueKey) {
    withCoroMutexHeld(mutex_, [&] { addQueueLocked(queueKey); });
  }

  folly::coro::Task<bool> co_containsQueue(K queueKey) {
    auto lock = co_await mutex_.co_scoped_lock();
    co_return queueKeyToQueue_.contains(queueKey);
  }

  bool containsQueue(K queueKey) {
    return withCoroMutexHeld(
        mutex_, [&] { return queueKeyToQueue_.contains(queueKey); });
  }

  folly::coro::Task<void> co_removeQueue(K queueKey) {
    size_t numToRemove = 0;
    {
      auto lock = co_await mutex_.co_scoped_lock();
      numToRemove = removeQueueLocked(queueKey);
    }
    releaseRemovedCounts(numToRemove);
  }

  void removeQueue(K queueKey) {
    releaseRemovedCounts(
        withCoroMutexHeld(mutex_, [&] { return removeQueueLocked(queueKey); }));
  }

  folly::coro::Task<void> co_enqueue(K queueKey, T&& val) {
    auto lock = co_await mutex_.co_scoped_lock();
    enqueueLocked(queueKey, std::move(val));
  }

  // May be called by multiple threads concurrently.
  void enqueue(K queueKey, T&& val) {
    withCoroMutexHeld(mutex_, [&] { enqueueLocked(queueKey, std::move(val)); });
  }

  // Dequeue round-robin based on the natural ordering of the queue keys.
  // Returns a pair containing the queue key from which a value was chosen and
  // the value itself. Should only be called by one thread at a time.
  folly::coro::Task<std::pair<K, T>> co_dequeue() {
    do {
      co_await folly::coro::co_safe_point;
      // Always need to wait on countSem_ even if readyQueueKeys_ is not empty
      // because countSem_ tracks the total number of elements in the
      // MultiUnboundedQueue and therefore must be decremented on a dequeue.
      co_await countSem_.co_wait();
      co_await mutex_.co_lock();
    } while (readyQueueKeys_.empty() && (mutex_.unlock(), true));

    // Convert to scoped_lock to simplify unlocking.
    std::scoped_lock<folly::coro::Mutex> lock{std::adopt_lock, mutex_};

    if (nextQueueKeyToSendOn_.has_value()) {
      // Pick the next queue to service according to strict round-robin based
      // on the natural ordering of the queue keys.
      auto nextIt = readyQueueKeys_.upper_bound(nextQueueKeyToSendOn_.value());
      if (nextIt == readyQueueKeys_.end()) {
        // We know that readyQueueKeys_ is not empty, so begin() must be
        // valid.
        nextIt = readyQueueKeys_.begin();
      }
      nextQueueKeyToSendOn_ = *nextIt;
    } else {
      // First ever call to dequeue(), so start with the first queue.
      nextQueueKeyToSendOn_ = *readyQueueKeys_.begin();
    }
    K keyUsed = nextQueueKeyToSendOn_.value();
    auto queueIt = queueKeyToQueue_.find(keyUsed);
    if (queueIt == queueKeyToQueue_.end()) {
      throw std::out_of_range(fmt::format("Queue '{}' not found", keyUsed));
    }
    folly::Optional<T> val = queueIt->second->try_dequeue();
    if (!val.has_value()) {
      throw std::runtime_error(
          fmt::format(
              "Unexpected empty queue: '{}'. This should never happen.",
              keyUsed));
    }
    if (queueIt->second->empty()) {
      readyQueueKeys_.erase(nextQueueKeyToSendOn_.value());
    }
    co_return {keyUsed, std::move(val.value())};
  }

  std::pair<K, T> dequeue() {
    return folly::coro::blockingWait(co_dequeue());
  }

  folly::coro::Task<size_t> co_size() {
    auto lock = co_await mutex_.co_scoped_lock();
    co_return countSem_.getAvailableTokens();
  }

  size_t size() {
    return withCoroMutexHeld(
        mutex_, [&] { return countSem_.getAvailableTokens(); });
  }

  folly::coro::Task<bool> co_empty() {
    auto lock = co_await mutex_.co_scoped_lock();
    co_return readyQueueKeys_.empty();
  }

  bool empty() {
    return withCoroMutexHeld(mutex_, [&] { return readyQueueKeys_.empty(); });
  }

 private:
  // The *Locked members expect mutex_ held.
  void addQueueLocked(K queueKey) {
    auto [it, inserted] =
        queueKeyToQueue_.emplace(queueKey, std::make_unique<Queue>());
    if (!inserted) {
      throw std::invalid_argument(
          fmt::format("Queue '{}' already exists.", queueKey));
    }
  }

  // Returns how many entries the removed queue held.
  size_t removeQueueLocked(K queueKey) {
    auto queueIt = queueKeyToQueue_.find(queueKey);
    if (queueIt == queueKeyToQueue_.end()) {
      throw std::out_of_range(fmt::format("Queue '{}' not found", queueKey));
    }
    const size_t numRemoved = queueIt->second->size();
    queueKeyToQueue_.erase(queueKey);
    readyQueueKeys_.erase(queueKey);
    return numRemoved;
  }

  // For every entry in a removed queue, decrement the countSem_.
  void releaseRemovedCounts(size_t numRemoved) {
    for (size_t i = 0; i < numRemoved; ++i) {
      countSem_.try_wait();
    }
  }

  void enqueueLocked(K queueKey, T&& val) {
    auto queueIt = queueKeyToQueue_.find(queueKey);
    if (queueIt == queueKeyToQueue_.end()) {
      throw std::out_of_range(fmt::format("Queue '{}' not found", queueKey));
    }
    queueIt->second->enqueue(std::move(val));
    readyQueueKeys_.insert(queueKey);
    countSem_.signal();
  }

  std::map<K, std::unique_ptr<Queue>> queueKeyToQueue_;
  std::set<K> readyQueueKeys_;
  std::optional<K> nextQueueKeyToSendOn_;
  folly::fibers::Semaphore countSem_{0};
  folly::coro::Mutex mutex_;
};

} // namespace libsoftmetaroce
