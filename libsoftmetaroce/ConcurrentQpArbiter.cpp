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

#include <cerrno>
#include <mutex>
#include <shared_mutex>

#include <folly/String.h>
#include <folly/coro/Coroutine.h>
#include <glog/logging.h>

#include "libsoftmetaroce/ConcurrentQpArbiter.h"

DEFINE_uint32(
    libsoftmetaroce_arbiter_ready_queue_capacity,
    8192,
    "Capacity of the multi-consumer arbiter ready queue, which holds at most one "
    "entry per ready QP. A fairness knob, not a correctness bound: sizing it at "
    "or above the peak concurrently-ready QP count avoids re-enqueue overflows "
    "under load. Each slot costs a few bytes, so raising it is cheap.");

namespace libsoftmetaroce {

namespace {
// Ready-queue token used only to wake blocked consumers at shutdown. addQp
// rejects this value as a qpNum, so it never collides with a real QP wakeup.
constexpr uint32_t kShutdownSentinel = ~uint32_t{0};
} // namespace

ConcurrentQpArbiter::ConcurrentQpArbiter()
    : ConcurrentQpArbiter(FLAGS_libsoftmetaroce_arbiter_ready_queue_capacity) {}

ConcurrentQpArbiter::ConcurrentQpArbiter(uint32_t readyQueueCapacity)
    : readyQueue_(readyQueueCapacity) {}

QpArbiterIface::ReadySignal* ConcurrentQpArbiter::addQp(
    uint32_t qpNum,
    folly::Function<std::unique_ptr<struct EgressPacket>(void)>
        getNextPacketCallback,
    folly::Function<bool()> applyHandedOffAcksCallback) {
  CHECK_NE(qpNum, kShutdownSentinel)
      << "qpNum collides with the arbiter's shutdown sentinel";
  std::unique_lock lock(registryMutex_);
  auto [it, inserted] = registeredQps_.try_emplace(
      qpNum,
      *this,
      qpNum,
      std::move(getNextPacketCallback),
      std::move(applyHandedOffAcksCallback));
  // A qpNum is assigned once and removed on QP destruction, so a second
  // registration is a bug.
  CHECK(inserted) << "QP " << qpNum << " registered with the arbiter twice";
  return &it->second;
}

void ConcurrentQpArbiter::removeQp(uint32_t qpNum) {
  // Exclusive lock waits out any in-flight build before the entry is freed.
  std::unique_lock lock(registryMutex_);
  registeredQps_.erase(qpNum);
}

folly::coro::Task<void> ConcurrentQpArbiter::shutdown() {
  shutdown_.store(true, std::memory_order_release);
  // Wake one consumer; each re-enqueues a sentinel as it exits, cascading to
  // the rest. Non-blocking is safe: a parked consumer means an empty queue so
  // the seed fits, and a full queue already has items to wake on.
  (void)readyQueue_.write(kShutdownSentinel);
  co_return;
}

void ConcurrentQpArbiter::RegisteredQp::signal() noexcept {
  // Bump even when the QP is already queued or in service, so a signal that
  // arrives mid-turn is recorded and not lost; only the 0 -> 1 edge enqueues.
  // A lost readiness signal would strand a QP, so the enqueue blocks when the
  // queue is full.
  if (pending.fetch_add(1, std::memory_order_acq_rel) == 0) {
    arbiter.readyQueue_.blockingWrite(qpNum);
  }
}

folly::coro::Task<QpArbiterIface::ClaimedPacket>
ConcurrentQpArbiter::claimNextPacketToSend() {
  // Each consumer owns a dedicated single-thread executor, so the blocking
  // queue reads/writes here park only that consumer's thread, starving no
  // other consumer. blockingRead is not cancellable, so co_safe_point cannot
  // free a parked consumer -- shutdown() is what unblocks them.
  while (true) {
    co_await folly::coro::co_safe_point;
    uint32_t qpNum = 0;
    readyQueue_.blockingRead(qpNum);
    if (shutdown_.load(std::memory_order_acquire)) {
      // Re-enqueue one wakeup so the next blocked consumer also wakes, then
      // exit. This cascades shutdown to all consumers without counting them. A
      // real qpNum dequeued here is dropped -- benign because shutdown is
      // one-way and the arbiter stops servicing.
      readyQueue_.blockingWrite(kShutdownSentinel);
      co_return ClaimedPacket{};
    }
    // A null result means the dequeued QP settled to idle (or was removed) with
    // no packet to send, so claim the next.
    if (std::unique_ptr<struct EgressPacket> packet = nextPacketOrIdle(qpNum)) {
      co_return ClaimedPacket{qpNum, std::move(packet)};
    }
  }
}

std::unique_ptr<struct EgressPacket> ConcurrentQpArbiter::nextPacketOrIdle(
    uint32_t qpNum) {
  std::shared_lock lock(registryMutex_);
  auto it = registeredQps_.find(qpNum);
  if (it == registeredQps_.end()) {
    // QP removed since it was signaled: no packet to build.
    staleSignalDropCount_.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
  }
  RegisteredQp& regQp = it->second;
  // Snapshot pending before building. The common path builds a packet and
  // returns it; the loop repeats only when a signal() races the turn and the
  // build finds no packet, so we re-take the turn in place.
  regQp.pendingBeforeBuild = regQp.pending.load(std::memory_order_acquire);
  do {
    // Applied inside the loop, so a retry re-applies: an ACK handed over after
    // the last attempt would otherwise have its signal consumed by the exchange
    // below without ever being applied.
    if (regQp.applyHandedOffAcks) {
      regQp.applyHandedOffAcks();
    }
    // buildNextPacket() must not throw: enforce it here (a throw terminates) so
    // a throwing build can never leave the QP dedup-stranded with pending >= 1.
    std::unique_ptr<struct EgressPacket> packet = [&]() noexcept {
      return regQp.buildNextPacket();
    }();
    if (packet && packet->packet) {
      return packet;
    }
    // No packet built: settle the QP to idle (pending -> 0) iff no signal() has
    // been requested since the snapshot. If one has, compare_exchange leaves
    // pending untouched, reloads pendingBeforeBuild, and returns false -- so we
    // loop and re-take the turn.
  } while (!regQp.pending.compare_exchange_strong(
      regQp.pendingBeforeBuild, 0, std::memory_order_acq_rel));
  return nullptr;
}

bool ConcurrentQpArbiter::reenqueueQp(uint32_t qpNum) noexcept {
  return readyQueue_.write(qpNum);
}

folly::coro::Task<void> ConcurrentQpArbiter::handleFaultOnEgress(
    std::unique_ptr<struct EgressPacket> failed,
    int errnoOnWrite) {
  DCHECK(failed) << "handleFaultOnEgress requires the faulted packet";
  if (errnoOnWrite == EAGAIN) {
    // Socket buffer full: recover as with ordinary packet loss.
    std::shared_lock lock(registryMutex_);
    auto it = registeredQps_.find(failed->qpNum);
    if (it == registeredQps_.end()) {
      // QP removed between claim and fault; no QP to re-signal. Count it so
      // this rare race stays visible at teardown.
      staleSignalDropCount_.fetch_add(1, std::memory_order_relaxed);
      co_return;
    }
    // Re-enqueue only if a new ready signal was posted since the frame was
    // built.
    uint64_t expected = it->second.pendingBeforeBuild;
    const bool reenqueue = !it->second.pending.compare_exchange_strong(
        expected, 0, std::memory_order_acq_rel);
    if (reenqueue && !readyQueue_.write(failed->qpNum)) {
      // Ready queue full: park the QP to idle (can't keep sending into a full
      // socket buffer) and let the retransmit timer recover it.
      it->second.pending.store(0, std::memory_order_release);
    }
    co_return;
  }
  LOG(FATAL) << "Fatal error sending frame, errno: " << errnoOnWrite << " ("
             << folly::errnoStr(errnoOnWrite) << ")";
}

} // namespace libsoftmetaroce
