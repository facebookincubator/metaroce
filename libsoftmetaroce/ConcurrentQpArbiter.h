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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <vector>

#include <folly/CppAttributes.h>
#include <folly/Function.h>
#include <folly/MPMCQueue.h>
#include <folly/SharedMutex.h>
#include <folly/container/F14Map.h>
#include <folly/coro/Task.h>
#include <folly/lang/Align.h>

#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/QpArbiterIface.h>

namespace libsoftmetaroce {

// Multi-consumer QpArbiter: N send consumers share one MPMC ready queue, each
// claiming a QP, building one packet, then releasing it (one packet per turn,
// back of the queue) for approximate round-robin fairness. Per-QP readiness is
// a single atomic counter and the ready set takes no central lock on the
// per-packet path, so consumers scale without serializing on a shared mutex. A
// QP is owned by at most one consumer at a time, so per-QP transmit/ack state
// stays single-owner.
//
// Consumers never block on the ready queue: reenqueueQp is non-blocking, and on
// a full queue the consumer keeps sending the QP in place via nextPacketOrIdle
// -- so a sending thread always exists and it cannot deadlock at any capacity.
// Only new-readiness signals block when full.
class ConcurrentQpArbiter : public QpArbiterIface {
 public:
  ConcurrentQpArbiter();
  // Explicit ready-queue capacity, for tests that need a small queue.
  explicit ConcurrentQpArbiter(uint32_t readyQueueCapacity);

  ReadySignal* addQp(
      uint32_t qpNum,
      folly::Function<std::unique_ptr<struct EgressPacket>(void)>
          getNextPacketCallback,
      folly::Function<bool()> applyHandedOffAcksCallback) override;
  using QpArbiterIface::addQp;
  void removeQp(uint32_t qpNum) override;
  folly::coro::Task<void> shutdown() override;

  folly::coro::Task<void> handleFaultOnEgress(
      std::unique_ptr<struct EgressPacket> failed,
      int errnoOnWrite) override;

  // This arbiter has no separate intake stage; readiness goes straight to the
  // ready queue, so there is no intake-full count.
  uint64_t getIntakeFullCount() const noexcept override {
    return 0;
  }
  uint64_t getStaleSignalDropCount() const noexcept override {
    return staleSignalDropCount_.load(std::memory_order_relaxed);
  }

  folly::coro::Task<ClaimedPacket> claimNextPacketToSend() override;
  [[nodiscard]] bool reenqueueQp(uint32_t qpNum) noexcept override;
  std::unique_ptr<struct EgressPacket> nextPacketOrIdle(
      uint32_t qpNum) override;
  std::vector<std::unique_ptr<struct EgressPacket>> buildBatch(
      uint32_t qpNum,
      std::unique_ptr<struct EgressPacket> first,
      size_t batchCap) override;

 private:
  using RegistryReadLock = std::shared_lock<folly::SharedMutex>;
  // A QP's send registration: how to build its next packet, plus its readiness
  // counter. `pending` is both the enqueue-dedup trigger (its 0 -> 1 bump
  // enqueues the QP) and the "signaled during a turn" record; `pending >= 1` is
  // the single-owner token held for the whole span a QP is queued or in
  // service. Padded so a QP's signal traffic does not false-share another QP's
  // counter.
  struct RegisteredQp : public ReadySignal {
    RegisteredQp(
        ConcurrentQpArbiter& arbiter,
        uint32_t qpNum,
        folly::Function<std::unique_ptr<struct EgressPacket>(void)> cb,
        folly::Function<bool()> applyAcks)
        : arbiter(arbiter),
          qpNum(qpNum),
          buildNextPacket(std::move(cb)),
          applyHandedOffAcks(std::move(applyAcks)) {}

    // Lock-free: a counter bump plus an enqueue on the 0 -> 1 edge. The QP
    // holds this handle, so signaling bumps the counter directly, staying off
    // the registry lock.
    folly::coro::Task<void> co_signalReady() override {
      signal();
      co_return;
    }
    void postSignal() noexcept override {
      signal();
    }
    // signal() is lock-free and always delivers, so the fast path never fails.
    bool trySignalReady() noexcept override {
      signal();
      return true;
    }

    ConcurrentQpArbiter& arbiter;
    const uint32_t qpNum;
    folly::Function<std::unique_ptr<struct EgressPacket>(void)> buildNextPacket;
    // May not be set in tests that register a QP directly.
    folly::Function<bool()> applyHandedOffAcks;
    alignas(folly::hardware_destructive_interference_size)
        std::atomic<uint64_t> pending{0};
    // `pending` snapshotted right before the build; nextPacketOrIdle (and the
    // EAGAIN fault path) CAS against it, so the QP settles to idle only when no
    // signal() raced the build.
    uint64_t pendingBeforeBuild{0};

   private:
    void signal() noexcept;
  };

  // Returns nullptr if the QP was removed while queued or mid-turn. The caller
  // must hold the registry read lock while using the returned pointer.
  RegisteredQp* FOLLY_NULLABLE
  findSignaledQp(uint32_t qpNum, const RegistryReadLock& registryHeld);
  // Returns nullptr only after settling the QP to idle.
  static std::unique_ptr<struct EgressPacket> buildOrIdle(
      RegisteredQp& regQp,
      const RegistryReadLock& registryHeld);
  // Returns nullptr if the QP has no packet ready.
  static std::unique_ptr<struct EgressPacket> buildWithoutSettling(
      RegisteredQp& regQp,
      const RegistryReadLock& registryHeld);
  // Returns nullptr if the QP has no packet ready.
  static std::unique_ptr<struct EgressPacket> applyAcksAndBuild(
      RegisteredQp& regQp);

  // F14NodeMap gives each RegisteredQp a stable address (nodes do not move on
  // rehash), so it is stored by value -- no extra indirection. registryMutex_
  // is shared-locked for lookup + build (builds run concurrently) and
  // exclusive-locked for add/remove, so a removal waits out in-flight builds.
  folly::SharedMutex registryMutex_;
  folly::F14NodeMap<uint32_t, RegisteredQp> registeredQps_;

  // Holds at most one entry per ready QP (pending dedups); capacity is set in
  // the constructor from --libsoftmetaroce_arbiter_ready_queue_capacity.
  folly::MPMCQueue<uint32_t> readyQueue_;

  std::atomic<bool> shutdown_{false};
  std::atomic<uint64_t> staleSignalDropCount_{0};
};

} // namespace libsoftmetaroce
