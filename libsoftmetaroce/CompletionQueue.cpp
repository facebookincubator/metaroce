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

#include <libsoftmetaroce/CompletionQueue.h>

#include <glog/logging.h>

namespace libsoftmetaroce {

CompletionQueue::~CompletionQueue() {
  auto locked = references_.lock();
  CHECK_EQ(locked->workQueues, 0u)
      << "a work queue still holds this completion queue, so the free releases "
         "memory it can still stage a completion into";
  CHECK_EQ(locked->outstandingEvents, 0u)
      << "an event raised for this completion queue is still outstanding, so "
         "a client can still read the queue out of it";
}

folly::coro::Task<Cqe> CompletionQueue::pollCq() {
  co_return co_await completionQueue_.dequeue();
}

[[nodiscard]] int CompletionQueue::getNumCompletedWqes() const {
  return (int)completionQueue_.size();
}

bool CompletionQueue::tryEnqueue(Cqe cqe) {
  bool refused = false;
  bool raise = false;
  {
    const std::lock_guard<std::mutex> guard{admission_};
    refused = completionQueue_.size() >= depth_;
    if (!refused) {
      completionQueue_.enqueue(std::move(cqe));
      // Consumed under the same lock as the enqueue, so a re-arm cannot land
      // between this enqueue and the consume below.
      raise = armed_;
      armed_ = false;
    }
  }
  if (refused && reportOverflow_ != nullptr) {
    reportOverflow_();
  }
  if (raise && raiseCompletionEvent_ != nullptr) {
    raiseCompletionEvent_();
  }
  return !refused;
}

void CompletionQueue::enqueue(Cqe cqe) {
  bool raise = false;
  {
    const std::lock_guard<std::mutex> guard{admission_};
    completionQueue_.enqueue(std::move(cqe));
    raise = armed_;
    armed_ = false;
  }
  if (raise && raiseCompletionEvent_ != nullptr) {
    raiseCompletionEvent_();
  }
}

void CompletionQueue::requestNotification() {
  const std::lock_guard<std::mutex> guard{admission_};
  armed_ = true;
}

void CompletionQueue::takeRetrievedCompletionEvent() {
  ++references_.lock()->retrievedCompletionEvents;
}

void CompletionQueue::releaseRetrievedCompletionEvents(size_t count) {
  auto locked = references_.lock();
  if (locked->retrievedCompletionEvents < count) {
    LOG_FIRST_N(ERROR, 20)
        << "more completion events were acked than were taken from this "
           "completion queue's channel";
    locked->retrievedCompletionEvents = 0;
  } else {
    locked->retrievedCompletionEvents -= count;
  }
}

bool CompletionQueue::hasUnackedCompletionEvents() const {
  return references_.lock()->retrievedCompletionEvents > 0;
}

void CompletionQueue::takeReference() {
  ++references_.lock()->workQueues;
}

void CompletionQueue::dropReference() {
  auto locked = references_.lock();
  CHECK_GT(locked->workQueues, 0u)
      << "a work queue dropped a reference to this completion queue without "
         "holding one";
  --locked->workQueues;
}

bool CompletionQueue::isReferenced() const {
  return references_.lock()->workQueues > 0;
}

void CompletionQueue::takeEventReference() {
  ++references_.lock()->outstandingEvents;
}

void CompletionQueue::releaseEventReferences(size_t count) {
  auto locked = references_.lock();
  // The invariant that events are acked once is the caller's responsibility.
  if (locked->outstandingEvents < count) {
    LOG_FIRST_N(ERROR, 20)
        << "more events were given back to this completion queue than were "
           "raised for it, so a destroy waiting here is released early";
    locked->outstandingEvents = 0;
  } else {
    locked->outstandingEvents -= count;
  }
  if (locked->outstandingEvents == 0) {
    // The wait reacquires this lock to return, so the notify completes first.
    allEventsAcked_.notify_all();
  }
}

void CompletionQueue::waitForEventsAcked() {
  auto locked = references_.lock();
  allEventsAcked_.wait(
      locked.as_lock(), [&locked] { return locked->outstandingEvents == 0; });
}

} // namespace libsoftmetaroce
