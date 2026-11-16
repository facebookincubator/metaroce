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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <folly/Function.h>
#include <folly/coro/Task.h>

#include <libsoftmetaroce/EgressPacket.h>

namespace libsoftmetaroce {

// Abstract interface for the QP send arbiter. The owning MetaRoceVerbs holds a
// QpArbiterIface& to register QPs and run lifecycle; the Sender drives
// consumption through the uniform claim/release API. The implementation is
// selected at construction by the send-thread count. Each QP is serviced by at
// most one consumer at a time, so per-QP transmit/ack state stays single-owner.
class QpArbiterIface {
 public:
  virtual ~QpArbiterIface() = default;

  // Handle returned by addQp() through which a QP marks itself ready to send,
  // without a qpNum lookup. The QP holds it for its lifetime; it is owned by
  // the arbiter and valid until removeQp(). Signaling after removeQp() is
  // unsupported. Implementations make this as cheap as they can.
  class ReadySignal {
   public:
    virtual ~ReadySignal() = default;
    // Awaitable signal for coroutine callers.
    virtual folly::coro::Task<void> co_signalReady() = 0;
    // Fire-and-forget readiness signal; cheap and non-blocking in normal
    // operation (behavior under backpressure is implementation-defined).
    virtual void postSignal() noexcept = 0;
    // Non-blocking fast path; returns false if it could not signal
    // synchronously (the caller should then schedule co_signalReady()).
    virtual bool trySignalReady() noexcept = 0;
  };

  // Register a QP and return its ReadySignal handle. Required before a QP can
  // send; does not by itself make the QP ready. `applyHandedOffAcksCallback`
  // applies the ACKs a receiver handed to this QP. Every QP sets it; it may be
  // unset in tests that register a QP directly.
  virtual ReadySignal* addQp(
      uint32_t qpNum,
      folly::Function<std::unique_ptr<struct EgressPacket>(void)>
          getNextPacketCallback,
      folly::Function<bool()> applyHandedOffAcksCallback) = 0;

  ReadySignal* addQp(
      uint32_t qpNum,
      folly::Function<std::unique_ptr<struct EgressPacket>(void)>
          getNextPacketCallback) {
    return addQp(
        qpNum, std::move(getNextPacketCallback), folly::Function<bool()>{});
  }

  // Remove a QP. Must happen before the QP is deallocated; invalidates the QP's
  // ReadySignal handle.
  virtual void removeQp(uint32_t qpNum) = 0;

  // Stop servicing QPs and release any consumers blocked waiting for work.
  virtual folly::coro::Task<void> shutdown() = 0;

  // Handle a failed egress attempt for the QP owning `failed` (must be non-
  // null; read failed->qpNum). Called once per fault: it parks the QP (removes
  // it from the ready set) so the sender stops servicing it. Unsent in-flight
  // data is recovered by the QP's retransmit timer, so callers need not report
  // each unsent packet.
  virtual folly::coro::Task<void> handleFaultOnEgress(
      std::unique_ptr<struct EgressPacket> failed,
      int errnoOnWrite) = 0;

  // Diagnostics (cumulative; read at teardown).
  virtual uint64_t getIntakeFullCount() const noexcept = 0;
  virtual uint64_t getStaleSignalDropCount() const noexcept = 0;

  // --- Consume API (uniform across implementations) ---

  struct ClaimedPacket {
    uint32_t qpNum{0};
    std::unique_ptr<struct EgressPacket> packet; // null only on shutdown
  };

  // Blocks until a packet is available, returning null only on shutdown (never
  // transiently). Any number of consumers may call concurrently; the caller
  // writes the packet, then reenqueueQp(qpNum), or keeps sending via
  // nextPacketOrIdle(qpNum) if that is
  // full.
  virtual folly::coro::Task<ClaimedPacket> claimNextPacketToSend() = 0;

  // Re-enqueue a claimed QP for another turn. Non-blocking and lossy: if the
  // ready queue is full the write is dropped and this returns false, and the
  // caller must then keep the QP serviced itself (e.g. keep sending it in place
  // via nextPacketOrIdle()) rather than assume it was enqueued.
  [[nodiscard]] virtual bool reenqueueQp(uint32_t qpNum) noexcept = 0;

  // Next packet for a QP the caller holds claimed, bypassing the ready queue;
  // or nullptr once the QP has no packet to build, having marked the QP idle
  // (retaking the turn if a signal() raced). Default: no batching.
  virtual std::unique_ptr<struct EgressPacket> nextPacketOrIdle(
      uint32_t /*qpNum*/) {
    return nullptr;
  }

  // The batch starts with `first`. A QP that runs out of packets before
  // `batchCap` settles to idle. The default takes each further packet from
  // nextPacketOrIdle().
  virtual std::vector<std::unique_ptr<struct EgressPacket>> buildBatchOrIdle(
      uint32_t qpNum,
      std::unique_ptr<struct EgressPacket> first,
      size_t batchCap) {
    std::vector<std::unique_ptr<struct EgressPacket>> batch;
    batch.push_back(std::move(first));
    std::unique_ptr<struct EgressPacket> packet;
    while (batch.size() < batchCap &&
           (packet = nextPacketOrIdle(qpNum)) != nullptr) {
      batch.push_back(std::move(packet));
    }
    return batch;
  }
};

} // namespace libsoftmetaroce
