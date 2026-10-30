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

#include <limits>

#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/coro/BlockingWait.h>

#include "libsoftmetaroce/BlockingCoroLock.h"
#include "libsoftmetaroce/ClockSource.h"
#include "libsoftmetaroce/QpArbiter.h"

namespace libsoftmetaroce {

QpArbiter::QpArbiter() {
  // Selection takes the next ready Qp after this one, wrapping, so starting
  // past every qp number selects the lowest-numbered ready Qp first.
  nextQpNumToSendOn_ = std::numeric_limits<uint32_t>::max();
}

QpArbiter::~QpArbiter() {
  // The idle counter is a single process-wide value (one arbiter), so report it
  // once here rather than on each QP's send-stall line.
  const uint64_t idleNs = idleNs_.load(std::memory_order_relaxed);
  if (idleNs > 0) {
    LOG(INFO) << "arbiter idle (process-wide): " << idleNs / 1e6 << "ms";
  }
}

QpArbiterIface::ReadySignal* QpArbiter::addQp(
    uint32_t qpNum,
    folly::Function<std::unique_ptr<struct EgressPacket>(void)>
        getNextPacketCallback,
    folly::Function<bool()> applyHandedOffAcksCallback) {
  return withCoroMutexHeld(
      arbiterMutex_, [&]() -> QpArbiterIface::ReadySignal* {
        auto [it, inserted] = readySignals_.try_emplace(qpNum, this, qpNum);
        // A qpNum is assigned once and removed on QP destruction, so a second
        // registration is a bug. Check before moving the callback in, so a
        // duplicate leaves both maps untouched.
        CHECK(inserted) << "QP " << qpNum
                        << " registered with the arbiter twice";
        // Roll back the entries already made if a later insert throws, so the
        // maps never diverge and no live ReadySignal outlives a missing
        // callback.
        auto rollback = folly::makeGuard([&] {
          readySignals_.erase(qpNum);
          qpNumToCallback_.erase(qpNum);
        });
        qpNumToCallback_[qpNum] = std::move(getNextPacketCallback);
        if (applyHandedOffAcksCallback) {
          qpNumToAckApplier_[qpNum] = std::move(applyHandedOffAcksCallback);
        }
        rollback.dismiss();
        return &it->second;
      });
}

void QpArbiter::removeQp(uint32_t qpNum) {
  withCoroMutexHeld(arbiterMutex_, [&] {
    qpNumToCallback_.erase(qpNum);
    qpNumToAckApplier_.erase(qpNum);
    readyQpNums_.erase(qpNum);
    readySignals_.erase(qpNum);
  });
}

folly::coro::Task<void> QpArbiter::shutdown() {
  auto arbiterLock = co_await arbiterMutex_.co_scoped_lock();
  shutdown_ = true;
  arbiterBaton_.post();
  co_return;
}

folly::coro::Task<void> QpArbiter::co_signalReady(uint32_t qpNum) {
  auto arbiterLock = co_await arbiterMutex_.co_scoped_lock();
  readyQpNums_.insert(qpNum);
  arbiterBaton_.post();
  co_return;
}

bool QpArbiter::trySignalReady(uint32_t qpNum) noexcept {
  return tryWithCoroMutexHeld(arbiterMutex_, [&] {
    readyQpNums_.insert(qpNum);
    arbiterBaton_.post();
  });
}

void QpArbiter::signalReady(uint32_t qpNum) {
  folly::coro::blockingWait(
      [&]() -> folly::coro::Task<> { co_await co_signalReady(qpNum); }());
}

void QpArbiter::postSignal(uint32_t qpNum) noexcept {
  if (!toSignal_.write(qpNum)) {
    // Intake full (consumer backed up): drop this signal (counted) rather than
    // block the producer; the retransmit timer re-marks the QP ready.
    intakeFullCount_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  arbiterBaton_.post();
}

void QpArbiter::drainSignalIntake() {
  // Caller holds arbiterMutex_. removeQp does not purge toSignal_; a stale
  // qpNum folded in here is harmless (the selection loop drops it, or a
  // re-added qpNum costs one empty visit).
  uint32_t qpNum = 0;
  while (toSignal_.read(qpNum)) {
    readyQpNums_.insert(qpNum);
  }
}

folly::coro::Task<void> QpArbiter::handleFaultOnEgress(
    std::unique_ptr<struct EgressPacket> failed,
    int errnoOnWrite) {
  // if the sender socket buffer is full, we treat this as if it
  // were a lost / dropped packet and expect the retransmission
  // timer to re-enable the qp if needed.  A better approach may
  // be to preemptively put this frame in the retransmission queue
  // (since we know it was lost) or to undo the transmission.
  // However, we don't have the packet contents to re-buffer the
  // frame.
  if (errnoOnWrite == EAGAIN) {
    auto arbiterLock = co_await arbiterMutex_.co_scoped_lock();
    LOG(INFO) << "Handling EAGAIN by removing qp:" << failed->qpNum
              << " from the ready set";
    readyQpNums_.erase(failed->qpNum);
    co_return;
  }
  LOG(FATAL) << "Fatal error sending frame, errno: " << errnoOnWrite << " ("
             << folly::errnoStr(errnoOnWrite) << ")";
}

bool QpArbiter::isQpnReady(uint32_t qpNum) {
  return withCoroMutexHeld(
      arbiterMutex_, [&] { return readyQpNums_.contains(qpNum); });
}

folly::coro::Task<std::unique_ptr<struct EgressPacket>>
QpArbiter::waitForAndGetNextPacketToSend(uint32_t* claimedQpNum) {
  // Loop until we find a Qp with a packet to send or shutdown() is called.
  while (true) {
    co_await folly::coro::co_safe_point;
    co_await arbiterMutex_.co_lock();
    drainSignalIntake();

    // If no Qps are ready, then wait for a Qp to become ready.
    if (readyQpNums_.empty()) {
      arbiterMutex_.unlock();
      // Sender idle time: how long the single sender was parked here with no QP
      // ready. Account via a scope guard so a cancellation out of the baton
      // wait at shutdown still records the interval.
      {
        const uint64_t parkedAt = steadyNowNs();
        SCOPE_EXIT {
          idleNs_.fetch_add(
              steadyNowNs() - parkedAt, std::memory_order_relaxed);
        };
        co_await arbiterBaton_;
      }
      arbiterBaton_.reset();
      co_await arbiterMutex_.co_lock();
      drainSignalIntake();
    }

    // Check for shutdown after waiting on arbiterBaton_ in case we woke up
    // because shutdown() was called, instead of because a Qp became ready. We
    // do not need to check shutdown_ before waiting on arbiterBation_ because a
    // baton operates like a semaphore (okay to post before waiting). If
    // shutdown() is called before waiting, then the wait will return
    // immediately and we will catch shutdown_ here.
    if (shutdown_) {
      arbiterMutex_.unlock();
      co_return std::unique_ptr<struct EgressPacket>();
    }

    // If a wait was performed, then it is possible that readyQpNums_ has become
    // empty due to calls to removeQp() between releasing the lock, waiting
    // for arbiterBation_, and relocking arbiterMutex_. This is redundant in the
    // case where no wait was performed, but we keep this check here anyway for
    // simplicity (to avoid needing two separate codepaths for wait/no-wait).
    if (readyQpNums_.empty()) {
      arbiterMutex_.unlock();
      continue;
    }

    // Move to the next Qp that is ready. Suppose we have 3 QPs: A, B, and C.
    // Suppose that A and C are ready, and we just serviced A. We are
    // late-binding on the process of selecting the next Qp so that if B
    // suddenly becomes ready, we will select it instead of selecting C and
    // wrapping around. This preserves the strict round-robin order.
    auto nextIt = readyQpNums_.upper_bound(nextQpNumToSendOn_);
    if (nextIt == readyQpNums_.end()) {
      // We know that readyQpNums_ is not empty, so begin() must be valid.
      nextIt = readyQpNums_.begin();
    }
    nextQpNumToSendOn_ = *nextIt;
    auto callbackIt = qpNumToCallback_.find(nextQpNumToSendOn_);
    if (callbackIt == qpNumToCallback_.end()) {
      // This qpNum was signalled, then removeQp() cleared its callback before
      // the arbiter reached it here. Skip and count it rather than deref null.
      LOG_FIRST_N(WARNING, 10)
          << "arbiter: ready qp " << nextQpNumToSendOn_
          << " has no callback (removed after signal); dropping";
      staleSignalDropCount_.fetch_add(1, std::memory_order_relaxed);
      readyQpNums_.erase(nextQpNumToSendOn_);
      arbiterMutex_.unlock();
      continue;
    }

    // This Qp should have a packet to send, so try to get it.
    // This arbiter serializes the whole visit and never batches, so no packet
    // built for this QP is outstanding here.
    if (auto applierIt = qpNumToAckApplier_.find(nextQpNumToSendOn_);
        applierIt != qpNumToAckApplier_.end()) {
      applierIt->second();
    }
    std::unique_ptr<struct EgressPacket> egressPacket = callbackIt->second();
    if (egressPacket && egressPacket->packet) {
      // Capture the claimed qpNum while still holding the mutex, so a
      // concurrent consumer that later advances nextQpNumToSendOn_ cannot make
      // the caller pair this packet with the wrong QP.
      if (claimedQpNum != nullptr) {
        *claimedQpNum = nextQpNumToSendOn_;
      }
      arbiterMutex_.unlock();
      co_return std::move(egressPacket);
    }

    // This Qp did not have a packet to send after all, so remove it from
    // readyQpNums_.
    readyQpNums_.erase(nextQpNumToSendOn_);
    arbiterMutex_.unlock();
  }
  co_return std::unique_ptr<struct EgressPacket>();
}

folly::coro::Task<QpArbiterIface::ClaimedPacket>
QpArbiter::claimNextPacketToSend() {
  // The arbiter writes claimedQpNum only alongside a real packet; on shutdown
  // it returns a null packet and leaves this at its 0 default (never read).
  uint32_t claimedQpNum = 0;
  std::unique_ptr<struct EgressPacket> packet =
      co_await waitForAndGetNextPacketToSend(&claimedQpNum);
  co_return ClaimedPacket{claimedQpNum, std::move(packet)};
}

bool QpArbiter::reenqueueQp(uint32_t /*qpNum*/) noexcept {
  // A serviced QP stays in the ready set, re-evaluated on the next claim.
  return true;
}

} // namespace libsoftmetaroce
