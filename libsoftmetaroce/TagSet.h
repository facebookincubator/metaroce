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
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <utility>
#include <vector>

#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/MultiUnboundedQueue.h>
#include <libsoftmetaroce/QpArbiterIface.h>
#include <libsoftmetaroce/Tag.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/WrrScheduler.h>

namespace libsoftmetaroce {

// A wakeup the caller must issue for the sender after leaving the guarded
// region that raised it: signalling can block on guards the arbiter's consumers
// want.
enum class SenderWake { NOT_NEEDED, NEEDED };

// Collects, so a wake any one Tag asked for survives the rest of the loop.
constexpr SenderWake& operator|=(SenderWake& collected, SenderWake wake) {
  if (wake == SenderWake::NEEDED) {
    collected = SenderWake::NEEDED;
  }
  return collected;
}

class TagSet {
 public:
  // The callback receives (tagNum, postedCsn).
  TagSet(
      uint32_t qpNum,
      std::function<void(uint8_t, uint16_t)> rnrCancelCallback);

  ~TagSet();

  TagSet(const TagSet&) = delete;
  TagSet& operator=(const TagSet&) = delete;
  TagSet(TagSet&&) = delete;
  TagSet& operator=(TagSet&&) = delete;

  // Register a new Tag. Throws std::invalid_argument if the Tag already exists.
  void add(
      uint8_t tagNum,
      CompletionQueue* sendCq,
      CompletionQueue* recvCq,
      struct ibv_qp* ibvQp,
      bool sqSigAll = false,
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr);
  void add(
      uint8_t tagNum,
      CompletionQueue* cq,
      struct ibv_qp* ibvQp,
      bool sqSigAll = false,
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr);
  // As add(), but only if every Tag is still in IBV_QPS_RESET, tested and
  // inserted under one lock: a transition landing between the two would leave
  // a RESET Tag on a running QP. Returns the added Tag, or nullptr without
  // inserting.
  [[nodiscard]] std::shared_ptr<Tag> addIfAllTagsInReset(
      uint8_t tagNum,
      CompletionQueue* sendCq,
      CompletionQueue* recvCq,
      struct ibv_qp* ibvQp,
      bool sqSigAll = false,
      uint32_t maxInlineData = 0,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr);
  [[nodiscard]] std::shared_ptr<Tag> at(uint8_t tagNum) const;
  // Returns the Tag for tagNum, or nullptr if it does not exist.
  // Unlike at(), this does not throw on missing tags.
  [[nodiscard]] std::shared_ptr<Tag> getTagByNumber(uint8_t tagNum) const;
  // The domain the Tags of this QP were created under. Every Tag shares it, so
  // any of them answers; empty only where the set is.
  [[nodiscard]] struct ibv_pd* protectionDomain() const;
  [[nodiscard]] size_t size() const;
  [[nodiscard]] bool contains(uint8_t tagNum) const;
  // True when every Tag in the set is marked for deletion (the whole QP can be
  // torn down). Vacuously true for an empty set.
  [[nodiscard]] bool allTagsMarkedForDeletion() const;
  [[nodiscard]] uint32_t getQpNum() const;
  // Ascending, as the map holds them.
  [[nodiscard]] std::vector<uint8_t> getTagNums() const;
  // Post a WQE for a specific Tag. New WQEs are first queued and then processed
  // into their Tags in round-robin order. Throws std::out_of_range from the
  // queue if tagNum matches no queue, which means no such Tag: a queue is
  // created with its Tag and neither is removed while the QP lives. False when
  // the tag already holds the send depth it advertises: no work is queued, and
  // the verbs caller reports the refusal through its return rather than
  // through a completion.
  bool postWqe(uint8_t tagNum, std::unique_ptr<Wqe> wqe);
  // Continuously dequeue new WQEs from the WQE queue (in round-robin order) and
  // process them into their Tags.
  folly::coro::Task<void> processWqes(
      QpArbiterIface::ReadySignal& readySignal,
      uint16_t mtu);
  [[nodiscard]] int getNumPendingRWqes() const;
  [[nodiscard]] int getNumPendingWqes() const;
  [[nodiscard]] bool hasPendingWqes() const;

  // Return the next WQE with pending packets, if any, along with its Tag. Tags
  // are serviced in round-robin order. This is called in two contexts, as
  // indicate by intendToSend: (1) when simply checking if a WQE with pending
  // packets exists but the caller has no intent to actually send a packet from
  // the WQE (as in the retx callback), and (2) the caller intends to send (as
  // in the QpArbiter), in which case the TagSet should advance its round-robin
  // to the next Tag and the found WQE's Tag should update it's BRNR state, if
  // necessary. A returned Tag holds its semaphore; the caller must call
  // Tag::markFree() after it finishes using the WQE. Call without holding
  // mutex_.
  [[nodiscard]] std::optional<std::pair<Wqe*, std::shared_ptr<Tag>>>
  findNextWqe(bool intendToSend);
  // Set the QP state of every Tag in this TagSet. A Tag already in
  // IBV_QPS_ERR is left there, and one that enters error mid-walk wins over
  // the compare-and-set: the state is terminal, and a Tag already in error
  // is not brought back by a transition meant for the others.
  void setIbvQpState(ibv_qp_state state);

  // Track the number of unsent POSNs across all WQEs in this TagSet.
  void addUnsentPosns(uint32_t count);
  void decrementUnsentPosns();
  // Stop counting posns that were unsent work and will never be sent. The
  // count is re-entered instead when a claimed posn is released back to
  // PENDING, which addUnsentPosns() does.
  void subtractUnsentPosns(uint32_t count);
  [[nodiscard]] uint32_t getUnsentPosnCount() const;

  // A tag failure is split across two threads: the one that notices it
  // publishes it, and the sender that owns posn state fails the work.

  // Notice and publish. Caller holds that Tag's semaphore; this takes mutex_
  // briefly under it and returns the wake it cannot perform there.
  [[nodiscard]] SenderWake enterTagErrorWithTagHeld(Tag& tag);

  // Sweep. Runs on the sender's thread, the only one that may write posn
  // state, and fails the pending work of every Tag left in the error state.
  // Cheap when no Tag has entered error since the last call.
  void failWqesOfTagsInError();
  // Raised where a Tag stops serving: entering error, or being destroyed.
  // Distinct from the sweep latch, which asks whose work needs failing.
  void notifySenderTagLeftService();

  // Consumes the notice above, so the walk below runs once per change rather
  // than once per packet.
  [[nodiscard]] bool takeTagLeftServiceNotice();

  // Whether no Tag is left to serve: each is in error or destroyed. Walks the
  // Tags, so read it only behind notifySenderTagLeftService().
  [[nodiscard]] bool noTagSurvives() const;

  // For testing: these observe state a test has no other handle on.

  // Wait up to timeoutMs for the count of WQEs processed into their Tags to
  // reach numWqes. Returns whether it did.
  [[nodiscard]] bool waitForWqesToBePosted(
      int numWqes,
      std::chrono::milliseconds timeoutMs);
  [[nodiscard]] bool waitForRWqesToBePosted(
      int numWqes,
      std::chrono::milliseconds timeoutMs);
  // True when every Tag is in `state` -- each has its own ibv_qp and fails
  // on its own, so they need not agree. Vacuously true for an empty set. The
  // internal caller holds mutex_ and uses the private form.
  [[nodiscard]] bool allTagsInState(ibv_qp_state state) const;

 private:
  using TagsByNum = std::map<uint8_t, std::shared_ptr<Tag>>;

  // Tells the sender a Tag has entered error with work still to be failed, so
  // its next visit sweeps. Any thread. Raising it for a Tag whose work is
  // already failed costs the next sweep a walk of every Tag that finds no
  // work to fail.
  void notifySenderTagEnteredError();

  // Both add paths, with mutex_ already held. Returns the inserted Tag.
  std::shared_ptr<Tag> addWithLockHeld(
      uint8_t tagNum,
      CompletionQueue* sendCq,
      CompletionQueue* recvCq,
      struct ibv_qp* ibvQp,
      bool sqSigAll,
      uint32_t maxInlineData,
      uint32_t sendQueueDepth = kMaxQpWorkRequests,
      uint32_t receiveQueueDepth = kMaxQpWorkRequests,
      SharedReceiveQueue* FOLLY_NULLABLE sharedReceiveQueue = nullptr);
  // Rejects what no Tag can be built from, before any lock is taken.
  void throwIfCannotAdd(
      uint8_t tagNum,
      CompletionQueue* sendCq,
      struct ibv_qp* ibvQp) const;
  // Whether every Tag is in this state. Caller holds mutex_.
  [[nodiscard]] bool allTagsInStateWithLockHeld(ibv_qp_state state) const;
  [[nodiscard]] std::optional<std::pair<Wqe*, std::shared_ptr<Tag>>>
  peekNextWqe();
  [[nodiscard]] std::optional<std::pair<Wqe*, std::shared_ptr<Tag>>>
  findNextWqeToSend();
  static Tag& asSchedulable(TagsByNum::value_type& entry);

  // Injected dependency.
  const std::function<void(uint8_t, uint16_t)> rnrCancelCallback_;

  // Configuration.
  const uint32_t qpNum_;

  // Protocol / runtime state.
  MultiUnboundedQueue<uint8_t, std::unique_ptr<Wqe>> wqeQueue_;
  // Tags are not removed once added.
  TagsByNum tagNumToTag_;
  WrrScheduler<TagsByNum::iterator> scheduler_;
  // Protects adding and counting Tags. Mutable because read-only accessors
  // need to acquire a shared lock.
  mutable std::shared_mutex mutex_;
  // Raised when a Tag enters the error state, cleared by the sweep that fails
  // its work. Lets the sender's visit skip the walk when no Tag has entered
  // error, rather than taking every Tag's semaphore on the hot path.
  std::atomic<bool> tagErrorSweepPending_{false};
  std::atomic<bool> tagLeftServicePending_{false};
  // Number of POSNs across all WQEs that have not yet been allocated for
  // sending. Incremented when a send WQE is processed into a Tag, decremented
  // when allocateNextPosn() is called from the TX path.
  std::atomic<uint32_t> unsentPosnCount_{0};

  // Test-only support.
  // Waits up to timeoutMs for foundDesiredWqes() and reports whether it passed;
  // woken on queue drain, since posting does not move the counts it waits on.
  bool waitForWqesToBePostedGeneric(
      std::chrono::milliseconds timeoutMs,
      const std::function<bool()>& foundDesiredWqes);
  std::mutex postWqeCvMutex_;
  std::condition_variable postWqeCv_;
};
} // namespace libsoftmetaroce
