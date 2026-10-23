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

#include <folly/Synchronized.h>
#include <folly/coro/Task.h>
#include <folly/coro/UnboundedQueue.h>

#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/Cqe.h>
#include <condition_variable>
#include <functional>
#include <mutex>

namespace libsoftmetaroce {

class CompletionQueue {
 public:
  // Calls reportOverflow when tryEnqueue rejects a completion at capacity.
  // Calls raiseCompletionEvent for the first completion enqueued after arming.
  // Both callbacks run after admission_ is released.
  explicit CompletionQueue(
      uint32_t depth = kMaxCqEntries,
      std::function<void()> reportOverflow = {},
      std::function<void()> raiseCompletionEvent = {})
      : reportOverflow_{std::move(reportOverflow)},
        raiseCompletionEvent_{std::move(raiseCompletionEvent)},
        depth_{depth} {}
  ~CompletionQueue();

  CompletionQueue(const CompletionQueue&) = delete;
  CompletionQueue& operator=(const CompletionQueue&) = delete;
  CompletionQueue(CompletionQueue&&) = delete;
  CompletionQueue& operator=(CompletionQueue&&) = delete;

  folly::coro::Task<Cqe> pollCq();
  [[nodiscard]] int getNumCompletedWqes() const;

  // Takes the completion, or answers false and drops it where the queue holds
  // the depth `ibv_create_cq` set. A dropped completion reaches a client as
  // `IBV_EVENT_CQ_ERR`.
  [[nodiscard]] bool tryEnqueue(Cqe cqe);

  // Takes the completion without consulting the depth.
  void enqueue(Cqe cqe);

  // Held by each work queue for as long as it can stage a completion here.
  void takeReference();
  void dropReference();
  // `ibv_destroy_cq` answers `EBUSY` while this holds.
  [[nodiscard]] bool isReferenced() const;

  // Held by an async event raised for this queue until the client acks it.
  void takeEventReference();
  void releaseEventReferences(size_t count);
  // Blocks until every event raised for this queue has been acked, which
  // `ibv_get_async_event(3)` requires of the destroy that frees it.
  void waitForEventsAcked();

  // Arms one completion event, as `ibv_req_notify_cq(3)` does.
  void requestNotification();
  // Completion events a client has taken with `ibv_get_cq_event` and not yet
  // acked; `ibv_destroy_cq` refuses while any are.
  void takeRetrievedCompletionEvent();
  void releaseRetrievedCompletionEvents(size_t count);
  [[nodiscard]] bool hasUnackedCompletionEvents() const;

 private:
  // Injected dependencies.
  const std::function<void()> reportOverflow_;
  const std::function<void()> raiseCompletionEvent_;

  // Configuration.
  // What `ibv_create_cq` asked for. The queue can hold one past it: a poll that
  // has claimed an entry without taking it yet reads as having taken it.
  const uint32_t depth_;

  // Runtime state.
  // Covers the size check and the placement together, so two queue pairs
  // sharing this queue cannot both take the last entry. It also covers the
  // notification arm, so a re-arm cannot land between a completion's
  // placement and the consume that reports it.
  std::mutex admission_;
  static constexpr size_t kLgEntriesPerSegment = 5;
  folly::coro::UnboundedQueue<Cqe, false, false, false, kLgEntriesPerSegment>
      completionQueue_;
  struct References {
    // A destroy refuses while a work queue can still stage a completion here.
    size_t workQueues{0};
    // A destroy waits on an event raised for this queue and not yet acked.
    size_t outstandingEvents{0};
    // A destroy refuses while a taken event is still unacked.
    size_t retrievedCompletionEvents{0};
  };
  folly::Synchronized<References, std::mutex> references_;
  std::condition_variable allEventsAcked_;
  // Set by requestNotification, consumed by the next accepted completion.
  // Guarded by admission_.
  bool armed_{false};
};

} // namespace libsoftmetaroce
