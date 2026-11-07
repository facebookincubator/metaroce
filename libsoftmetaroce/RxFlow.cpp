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

#include <libsoftmetaroce/RxFlow.h>

#include <folly/ScopeGuard.h>
#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/Ecn.h>
#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/ProtocolCompatibility.h>
#include <libsoftmetaroce/Qp.h>

namespace libsoftmetaroce {

static BitVector<AckHeader::kAckTrackerBits> buildBitVectorForState(
    const std::deque<Flow::FsnState>& bitmap,
    Flow::FsnState state) {
  BitVector<AckHeader::kAckTrackerBits> bv;
  for (size_t i = 0; i < std::min(bitmap.size(), AckHeader::kAckTrackerBits);
       ++i) {
    if (bitmap[i] == state) {
      bv.setBit(i);
    }
  }
  return bv;
}

// Single-writer: called only from the Receiver's onDataAvailable (one
// EventBase thread per RxFlow). Concurrent writers would mix fields
// from different ACKs in pendingAck_.
void RxFlow::setPendingAck(AckHeader ackHeader, uint16_t ingressSourcePort) {
  if (lastIngressSourcePort_.has_value() &&
      ingressSourcePort != *lastIngressSourcePort_) {
    ingressSourcePortChanged_.store(true, std::memory_order_release);
  }
  lastIngressSourcePort_ = ingressSourcePort;
  uint32_t seq = ackSeq_.load(std::memory_order_acquire);
  CHECK_EQ(seq & 1, 0)
      << "concurrent setPendingAck: seqlock expects a single writer. "
         "Multiple writers require writer-side synchronization to ensure "
         "pendingAck_ integrity";

  // Mark writing (odd). Exchange detects if the reader drained between
  // our load and now (prev will be 0 instead of seq).
  uint32_t prev = ackSeq_.exchange(seq + 1, std::memory_order_acq_rel);

  // Accumulate CE count from the superseded ACK, but only if the slot
  // was occupied AND the reader didn't drain between our load and
  // exchange. After exchange, ackSeq_ is odd so the reader spins —
  // pendingAck_ is safe to read here.
  if (prev == seq && seq > 0) {
    // Two counts can sum past what the four-bit field holds, and saturating
    // reports the most it can express where wrapping would report none.
    const uint32_t coalesced =
        static_cast<uint32_t>(ackHeader.getCnpCE()) + pendingAck_.getCnpCE();
    ackHeader.setCnpCE(
        static_cast<uint8_t>(std::min<uint32_t>(coalesced, kMaxCnpCeCount)));
  }

  pendingAck_ = std::move(ackHeader);
  ackSeq_.store(seq + 2, std::memory_order_release);

  const bool slotWasEmpty = prev != seq || seq == 0;
  if (slotWasEmpty) {
    qp_.signalReadyAsync();
  }
}

std::unique_ptr<struct EgressPacket> RxFlow::getNextAckToSend() {
  uint32_t seq = ackSeq_.load(std::memory_order_acquire);
  if (seq == 0) {
    return nullptr;
  }

  // If the writer is mid-write (odd seq), wait for it to finish.
  // Bounded: the writer performs one struct assignment + one atomic store.
  while (seq & 1) {
    seq = ackSeq_.load(std::memory_order_acquire);
  }

  // Defensive: only necessary if multiple readers drain concurrently,
  // which is not how the code is structured (single sender thread).
  if (seq == 0) {
    return nullptr;
  }

  // Copy the ACK from the shared slot before draining via CAS.
  // @lint-ignore INFERCPP intentional copy: pendingAck_ is shared with writer
  AckHeader ackHeader = pendingAck_;
  if (!ackSeq_.compare_exchange_strong(seq, 0, std::memory_order_acq_rel)) {
    // A new write started between our copy and the CAS. Re-signal
    // so the arbiter retries after the writer finishes.
    qp_.signalReadyAsync();
    return nullptr;
  }

  // Every source-port change since the last drain moves the ACK socket once;
  // ack-source-port.md says why the ACK does not copy the source port instead.
  if (ingressSourcePortChanged_.exchange(false, std::memory_order_acq_rel)) {
    socketId_ = qp_.getSender().getAlternateAckSocket(socketId_);
  }

  using OperationType = BaseHeader::Opcode::OperationType;
  BaseHeader baseHeader =
      BaseHeader::BaseHeaderBuilder()
          .setOpcode(BaseHeader::Opcode(OperationType::SEL_ACK))
          .setVersion(kMetaRoceProtocolVersion)
          .setFlowId(flowId_)
          .setDstQp(qp_.getRemoteQpNum())
          .setSrcQp(qp_.getLocalQpNum())
          .build();

  return std::make_unique<struct EgressPacket>(
      std::make_unique<AckPacket>(std::move(baseHeader), std::move(ackHeader)),
      qp_.getSender().getSendSocket(socketId_, EgressPacket::Kind::Ack),
      getRemoteAddr(),
      qp_.getLocalQpNum(),
      EgressPacket::RxmitType::None);
}

bool RxFlow::shouldAckForEcn(uint8_t ipTos) {
  if (!isEcnCapable(ipTos)) {
    LOG_EVERY_N(ERROR, 1000)
        << "ECT was not set on a received Meta-RoCE packet (ECN value = "
        << (int)(ipTos & kEcnMask) << "). Fix sender!";
  }

  if (!isEcnMarked(ipTos)) {
    return false;
  }
  ++rxState_.receivedCeCount;
  // The ack reports and clears the count, and is enqueued early enough that
  // marks arriving before it goes out still fit the field.
  return rxState_.receivedCeCount >= kCeCountAckThreshold;
}

// Move the tag to the error state, flush its receives and mark a failed
// response, once the receive path that found the failure has released it. Only
// this tag enters error: its sends report once their packets settle, so no
// tracker is cleared and the QP's other tags keep the work they have in
// flight.
void RxFlow::enterTagErrorAndFlushReceives(
    Tag& tag,
    const std::optional<FailedMessage>& failedMessage) {
  SenderWake tagErrorWake = SenderWake::NOT_NEEDED;
  {
    // The receive flush below requires both, and the tx path holds raw Wqe*
    // pointers under the semaphore, so take them in the order every other flush
    // site uses.
    auto tagGuard = tag.scopedLock();
    auto retirementGuard = tag.lockForWqeRetirement();
    tagErrorWake = qp_.enterTagErrorWithTagHeld(tag);
    if (failedMessage.has_value()) {
      // This thread already owns a read or atomic's posns, and marking that
      // work request before the sender's flush of the rest is what keeps its
      // own status.
      // The check is for a response retired since it was delivered.
      if (Wqe* wqe =
              tag.getReceiveWqe(failedMessage->opType, failedMessage->msn)) {
        qp_.subtractUnsentPosns(wqe->markFailing(failedMessage->status));
      }
    }
    // Receives are this thread's to complete and have no packet on the wire
    // to wait for. The send side is the sender's: its posn state is the tx
    // owner's to write, so this thread leaves the sends to the error state
    // set above, and the wake brings the visit that acts on it.
    tag.flushPendingReceiveWqesInError();
  }
  qp_.wakeSenderAfterTagError(tagErrorWake);
}

static AckHeader makeNakAck(
    const DataHeader& dataHeader,
    RequestValidationError error);

// Read/atomic responses are matched to a local read/atomic WQE by the tx msn;
// they are never window-checked or BRNR'd.
static bool isResponse(BaseHeader::Opcode::OperationType op) {
  return op == BaseHeader::Opcode::OperationType::READ_RESP ||
      op == BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP;
}

static bool isSend(BaseHeader::Opcode::OperationType op) {
  return op == BaseHeader::Opcode::OperationType::SEND ||
      op == BaseHeader::Opcode::OperationType::SEND_IMM_P ||
      op == BaseHeader::Opcode::OperationType::SEND_LAST;
}

static bool isFinalSendPacket(BaseHeader::Opcode::OperationType op) {
  return op == BaseHeader::Opcode::OperationType::SEND_IMM_P ||
      op == BaseHeader::Opcode::OperationType::SEND_LAST;
}

// Plain reads and atomics generate their own response and need none.
static bool requiresReceive(const BaseHeader::Opcode& opcode) {
  return opcode.operationType() != BaseHeader::Opcode::OperationType::READ &&
      !opcode.hasAtomicHdr();
}

// One msn sequence covers sends, writes, reads and atomics, so a response's
// msn alone can match any of them.
static bool isResponseTo(
    BaseHeader::Opcode::OperationType opType,
    Wqe::WorkType workType) {
  if (opType == BaseHeader::Opcode::OperationType::READ_RESP) {
    return workType == Wqe::WorkType::kRead ||
        workType == Wqe::WorkType::kReadWithImm;
  }
  if (opType == BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP) {
    return workType == Wqe::WorkType::kAtomicFetchAdd ||
        workType == Wqe::WorkType::kAtomicCmpSwp;
  }
  return false;
}

std::optional<AckHeader> RxFlow::processNewDataPacket(
    const DataPacket& packet) {
  // Precondition: this fsn is new, may be in order or out of order, but
  // the data is to be delivered.
  const auto& dataHeader = packet.getDataHeader();
  const auto fsn = dataHeader.getFsn();
  VLOG(1) << "RxFlow received data packet fsn=" << fsn << " flowId=" << flowId_
          << " qp=" << qp_.getLocalQpNum();
  const auto msn = dataHeader.getMsn();
  const auto posn = dataHeader.getPosn();
  const auto sndUna = dataHeader.getUna();
  const auto& opcode = packet.getBaseHeader().getOpcode();
  const auto opType = opcode.operationType();

  Tag* const tag = qp_.getTag(dataHeader.getTag());

  // Declared before the acquisition below so that they outlive it: on every
  // path out of here the tag is released first, and only then does the guard
  // apply what was recorded, which needs it exclusively.
  bool failTag = false;
  std::optional<FailedMessage> failedMessage;
  auto applyFailureOnceReleased = folly::makeGuard([&] {
    if (failTag) {
      enterTagErrorAndFlushReceives(*CHECK_NOTNULL(tag), failedMessage);
    }
  });

  // Spec 5.2.6 allows rate-limiting these responses: each NAK for a tag's
  // failure holds that tag's next one back for an interval.
  auto nakTagFailure = [&tag, &dataHeader](RequestValidationError error) {
    // A packet for an unknown tag is BRNR'd, never NAKed as a tag failure.
    CHECK_NOTNULL(tag)->holdErrorNaks(getNowNs64());
    return makeNakAck(dataHeader, error);
  };

  // Held for the span below in which this uses a Wqe* the tag owns, which is
  // also what makes the error check meaningful: without it the tag can fail,
  // and free what the delivery is about to write into, right after it passes.
  std::optional<Tag::ReceiveAccess> access;
  if (tag != nullptr) {
    access.emplace(*tag);
    if (access->isTagInError()) {
      return nakTagFailure(RequestValidationError::kOperationalError);
    }
  }

  Wqe* wqe = nullptr;
  bool brnrTriggered = false;
  if (tag == nullptr) {
    brnrTriggered = true;
    VLOG(1) << "data packet for unknown Tag: " << dataHeader.getTag()
            << " on qp: " << qp_.getLocalQpNum() << ", will trigger BRNR";
  } else if (isResponse(opType)) {
    // Matched to a local read/atomic WQE by the tx msn; delivered if matched,
    // silently discarded if not, never BRNR'd.
    wqe = tag->getReceiveWqe(opType, msn);
    if (wqe != nullptr && !isResponseTo(opType, wqe->getWorkType())) {
      // Delivering would write the response into a send's or write's source
      // buffer, so drop the frame unacked.
      LOG_FIRST_N(ERROR, 10)
          << "qp " << qp_.getLocalQpNum() << " tag "
          << static_cast<uint32_t>(dataHeader.getTag()) << " discarding a "
          << opcode << " for msn " << msn << ", a " << wqe->getWorkType();
      return std::nullopt;
    }
  } else {
    // Only requests occupy the cdmsn window, so an msn at or before cdmsn is a
    // duplicate or stale retransmit: BRNR it before creating or binding any
    // receive WQE, validating, or extending the tracker.
    if (!after(msn, tag->getCDMSN())) {
      brnrTriggered = true;
    } else {
      if (auto err = ensureAndValidateWqe(*tag, packet);
          err != RequestValidationError::kNone) {
        failTag = true;
        return nakTagFailure(err);
      }
      wqe = tag->getReceiveWqe(opType, msn);
      tag->extendCdmsnTrackerIfNeeded(msn);
      brnrTriggered = wqe == nullptr && requiresReceive(opcode);
    }
  }

  // Non-terminal payloads must match the receive WQE's capacity-derived span.
  // SEND_IMM_P and SEND_LAST each establish a possibly shorter final span.
  // A capacity-derived last POSN with a zero Padding field may carry up to 3
  // bytes of undisclosed alignment padding (spec 5.2.5).
  // Skipped for atomics (data is in headers). Applies to matched responses too,
  // so a bad response posn cannot CHECK-abort delivery.
  uint32_t expectedPayloadSize = 0;
  if (wqe != nullptr &&
      opType != BaseHeader::Opcode::OperationType::READ_IMM_P &&
      !opcode.hasAtomicHdr() && !opcode.hasAtomicAckHdr()) {
    if (wqe->isPosnOutOfRange(posn)) {
      failTag = true;
      return nakTagFailure(RequestValidationError::kInvalidRequest);
    }
    const uint32_t actualPayloadSize = packet.getPayloadSizeBytes();
    if (isFinalSendPacket(opType)) {
      auto* receiveWqe = dynamic_cast<ReceiveWqe*>(wqe);
      if (receiveWqe == nullptr) {
        failTag = true;
        return nakTagFailure(RequestValidationError::kInvalidRequest);
      }
      if (!receiveWqe->setFinalPacket(posn, actualPayloadSize, qp_.getMtu())) {
        failTag = true;
        return nakTagFailure(RequestValidationError::kInvalidRequest);
      }
    }

    expectedPayloadSize = wqe->getPosnPayloadSizeBytes(posn, qp_.getMtu());
    bool payloadSizeValid = (actualPayloadSize == expectedPayloadSize);
    if (!payloadSizeValid && dataHeader.getPadding() == 0 &&
        wqe->isLastPosn(posn)) {
      const uint32_t paddedExpectedSize =
          (expectedPayloadSize + kPayloadAlignmentBytes - 1) &
          ~static_cast<uint32_t>(kPayloadAlignmentBytes - 1);
      payloadSizeValid =
          (actualPayloadSize > expectedPayloadSize &&
           actualPayloadSize <= paddedExpectedSize &&
           (actualPayloadSize % kPayloadAlignmentBytes) == 0);
    }
    if (!payloadSizeValid) {
      failTag = true;
      return nakTagFailure(RequestValidationError::kInvalidRequest);
    }
  }
  if (brnrTriggered && FLAGS_libsoftmetaroce_dont_send_brnr) {
    LOG(WARNING)
        << "Received msn: " << msn
        << " for which we have no rwqe; discarding where BRNR is expected";
    return std::nullopt;
  }

  if (wqe && opType == BaseHeader::Opcode::OperationType::READ_RESP) {
    // A read executed remotely; validate the local buffer before writing the
    // response into it. A bad local key (including an unregistered 0) is a
    // local protection error with the buffer left untouched.
    DCHECK(dynamic_cast<ReadWriteWqe*>(wqe) != nullptr);
    const MemoryRegions& memoryRegions = qp_.getMemoryRegions();
    struct ibv_pd* const localPd = CHECK_NOTNULL(tag)->getIbvQp().pd;
    if (expectedPayloadSize != 0 &&
        !(wqe->hasLocalSges() ? memoryRegions.isValidLocal(
                                    localPd,
                                    wqe->getLocalSges(),
                                    wqe->getMessageSizeBytes(),
                                    IBV_ACCESS_LOCAL_WRITE)
                              : memoryRegions.isValidLocal(
                                    localPd,
                                    static_cast<char*>(wqe->getBufferAddress()),
                                    wqe->getMessageSizeBytes(),
                                    static_cast<ReadWriteWqe*>(wqe)->getLkey(),
                                    IBV_ACCESS_LOCAL_WRITE))) {
      failTag = true;
      failedMessage = {.opType = opType, .msn = msn, .status = kLocalProtErr};
      wqe = nullptr;
    }
  }

  if (wqe && opType == BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP) {
    // Atomic ack: data is in the AtomicAckHeader.
    const AtomicAckPacket& atomicAckPacket =
        *CHECK_NOTNULL(dynamic_cast<const AtomicAckPacket*>(&packet));
    DCHECK(dynamic_cast<AtomicWqe*>(wqe) != nullptr);
    uint64_t origRemoteData =
        atomicAckPacket.getAtomicAckHeader().getOrigRemoteData();
    // Raw address (may be null for a bad atomic buffer); isValidLocal rejects
    // it below, so avoid addressForPosnMtu's non-null assertion.
    void* localAddr = wqe->getBufferAddress();
    if (qp_.getMemoryRegions().isValidLocal(
            CHECK_NOTNULL(tag)->getIbvQp().pd,
            static_cast<char*>(localAddr),
            kAtomicOperationSizeBytes,
            static_cast<AtomicWqe*>(wqe)->getLkey(),
            IBV_ACCESS_LOCAL_WRITE)) {
      *static_cast<uint64_t*>(CHECK_NOTNULL(localAddr)) = origRemoteData;
    } else {
      // The atomic executed remotely, but its result cannot be written to the
      // local buffer (bad local key): a local protection error, buffer
      // untouched. The tag enters error with this status; the sends
      // behind it are marked failing at the sender's next visit, so RC's
      // in-order completion still holds.
      failTag = true;
      failedMessage = {.opType = opType, .msn = msn, .status = kLocalProtErr};
      wqe = nullptr;
    }
  } else if (wqe && opType != BaseHeader::Opcode::OperationType::READ_IMM_P) {
    // Read-with-immediate carries no payload, so delivery is skipped.
    const folly::ByteRange payload = packet.getPayload();
    CHECK_GE(payload.size(), expectedPayloadSize)
        << "the size check above admits only the expected size, or that plus "
           "the last posn's alignment padding";
    const folly::ByteRange unpaddedPayload =
        payload.subpiece(0, expectedPayloadSize);
    wqe->deliverPosn(posn, qp_.getMtu(), unpaddedPayload);
  }

  rxState_.maxRcvdFsn =
      after(fsn, rxState_.maxRcvdFsn) ? fsn : rxState_.maxRcvdFsn;

  if (fsn == rxState_.rcvNxt) {
    // We received a packet with the FSN that we were expecting
    if (rxState_.rcvBitmap.empty()) {
      if (brnrTriggered) {
        // Add bit to track the current packet that triggered BRNR
        // rxState_.rcvNxt should not advance in this case
        rxState_.rcvBitmap.push_front(FsnState::BRNR);
      } else {
        // No out of order or BRNR packet is being tracked, so just advance
        // rcvNxt
        ++rxState_.rcvNxt;
      }
    } else {
      // Some packets were received out-of-order/triggered BRNR before. Pop
      // off the front of the rcv bitmap until we get to an unacked packet (we
      // don't need to keep tracking acked and BRNR packets, since these FSNs
      // are already accounted for)

      // Update bitmap to reflect the current packet
      if (brnrTriggered) {
        rxState_.rcvBitmap.front() = FsnState::BRNR;
      } else {
        rxState_.rcvBitmap.front() = FsnState::ACKED;
      }
    }
  } else if (after(fsn, rxState_.rcvNxt)) {
    // We received a packet with a FSN that is greater than the FSN we were
    // expecting.

    // Calculate what is the offset of the current FSN from rcvNxt
    // i.e. the bit index of the current FSN in the bitmap
    uint16_t bitmapOffset = fsn - rxState_.rcvNxt;
    if (rxState_.rcvBitmap.size() <= bitmapOffset) {
      // We need to resize the bitmap to account for the new FSN
      // but we'll deal with the state for the new FSN later
      rxState_.rcvBitmap.resize(bitmapOffset + 1, FsnState::UNACKED);
    } else if (rxState_.rcvBitmap[bitmapOffset] != FsnState::UNACKED) {
      // In this case, we have already seen packet(s) with FSN greater than
      // the current one, so we don't need to resize the bitmap.

      // We also already have a record of this FSN (its state is not UNACKED),
      // so it's a duplicate
      LOG(FATAL) << "incorrectly working on a duplicate, state: "
                 << rxState_.rcvBitmap[bitmapOffset] << " packet, fsn: " << fsn
                 << ", offset: " << bitmapOffset
                 << " in the new-data handling path on rx flow id: " << flowId_;
    }
    if (brnrTriggered) {
      rxState_.rcvBitmap[bitmapOffset] = FsnState::BRNR;
    } else {
      rxState_.rcvBitmap[bitmapOffset] = FsnState::ACKED;
    }
  } else {
    LOG(FATAL)
        << "incorrectly working on a duplicate packet in the new-data handling path";
  }

  // Clean up receive bitmap
  while (!rxState_.rcvBitmap.empty() &&
         (rxState_.rcvBitmap.front() == FsnState::ACKED ||
          after(sndUna, rxState_.rcvNxt))) {
    ++rxState_.rcvNxt;
    rxState_.rcvBitmap.pop_front();
  }

  // Make sure that we can account for all pending FSNs
  const uint16_t rcvWindowSize = rxState_.maxRcvdFsn - rxState_.rcvNxt + 1;
  CHECK_EQ(rcvWindowSize, rxState_.rcvBitmap.size()) << fmt::format(
      "Receive window tracking is out of sync with bitmap; maxFsn: {} rcvNxt: {} bitmap.size: {}",
      rxState_.maxRcvdFsn,
      rxState_.rcvNxt,
      rxState_.rcvBitmap.size());

  if (wqe != nullptr &&
      (opType == BaseHeader::Opcode::OperationType::WRITE_IMM_P ||
       opType == BaseHeader::Opcode::OperationType::SEND_IMM_P ||
       opType == BaseHeader::Opcode::OperationType::READ_IMM_P)) {
    DCHECK(packet.getImmData().has_value())
        << "parse populates the immediate for every opcode with hasImmData; "
           "a frame truncated inside it is dropped in onPacketData before "
           "RxFlow runs";
    wqe->setImmediateData(packet.getImmData().value());
    if (opType == BaseHeader::Opcode::OperationType::WRITE_IMM_P ||
        opType == BaseHeader::Opcode::OperationType::READ_IMM_P) {
      // Write reports RECV_RDMA_WITH_IMM; read-with-immediate reports
      // IBV_WC_DRIVER1, so the responder can tell the two apart.
      DCHECK(dynamic_cast<ReceiveWqe*>(wqe) != nullptr);
      static_cast<ReceiveWqe*>(wqe)->setIbvCompletionOpcode(
          opType == BaseHeader::Opcode::OperationType::READ_IMM_P
              ? IBV_WC_DRIVER1
              : IBV_WC_RECV_RDMA_WITH_IMM);
    }
  }

  // For non-READ operations, wqe must be non-null to track posn completion.
  // For READ and atomic request ops, checkForRxMessageCompletion updates
  // CDMSN state.
  if (!requiresReceive(opcode) && tag != nullptr && !brnrTriggered) {
    if (!qp_.checkForRxMessageCompletion(
            nullptr, opType, dataHeader.getTag(), msn, posn)) {
      failTag = true;
      return nakTagFailure(RequestValidationError::kInvalidRequest);
    }
  } else if (wqe != nullptr) {
    if (!qp_.checkForRxMessageCompletion(
            wqe, opType, dataHeader.getTag(), msn, posn)) {
      failTag = true;
      return nakTagFailure(RequestValidationError::kInvalidRequest);
    }
  }
  return std::nullopt;
}

RequestValidationError RxFlow::ensureAndValidateWqe(
    Tag& tag,
    const DataPacket& packet) {
  using OperationType = BaseHeader::Opcode::OperationType;
  const auto& opcode = packet.getBaseHeader().getOpcode();
  const auto& dataHeader = packet.getDataHeader();
  const auto msn = dataHeader.getMsn();
  const auto* readWritePacket = dynamic_cast<const ReadWritePacket*>(&packet);

  RequestValidationError validationError = RequestValidationError::kNone;
  // A marked (half-closed) tag NAKs READ/READ_IMM_P/ATOMIC: it would owe a
  // response WQE it never sends. SEND/WRITE (incl. *_IMM) only drain the
  // receive side, so accept.
  if (tag.isMarkedForDeletion() &&
      (opcode.operationType() == OperationType::READ ||
       opcode.operationType() == OperationType::READ_IMM_P ||
       opcode.operationType() == OperationType::ATOMIC_FETCH_ADD ||
       opcode.operationType() == OperationType::ATOMIC_CMP_SWP)) {
    return RequestValidationError::kInvalidRequest;
  }
  // READ_IMM_P carries no payload; reject before anything binds, so a
  // malformed request consumes no receive.
  if (opcode.operationType() == OperationType::READ_IMM_P &&
      packet.getPayloadSizeBytes() != 0) {
    return RequestValidationError::kInvalidRequest;
  }
  if (opcode.requiresPayload() && packet.getPayloadSizeBytes() == 0) {
    return RequestValidationError::kInvalidRequest;
  }
  if (opcode.operationType() == OperationType::WRITE) {
    const ReadWritePacket& rwPacket = *CHECK_NOTNULL(readWritePacket);
    int addrErr = 0;
    tag.createVirtualRWqeIfNeeded(
        msn,
        rwPacket.getReadWriteHeader(),
        &addrErr,
        qp_.getMemoryRegions(),
        qp_.getMtu());
    if (addrErr) {
      validationError = RequestValidationError::kAddressError;
    }
  } else if (isSend(opcode.operationType())) {
    // A SEND carries no rkey; a receive buffer the responder cannot scatter
    // into is a responder-local failure, reported as a remote operational
    // error. Validated once, at the bind (a multi-packet SEND finds it already
    // bound and skips the check).
    if (tag.assignInitiatorMsnToReceiveWqeIfNeeded(
            dataHeader.getCsn(), msn, qp_.getMtu()) &&
        tag.recordLocalProtectionErrorIfUnwritable(
            msn, qp_.getMemoryRegions())) {
      validationError = RequestValidationError::kOperationalError;
    }
  } else if (
      opcode.operationType() == OperationType::WRITE_IMM ||
      opcode.operationType() == OperationType::WRITE_IMM_P) {
    const ReadWritePacket& rwPacket = *CHECK_NOTNULL(readWritePacket);
    // An immediate reports on a posted receive; a plain WRITE on this msn
    // would have created a virtual one instead, and one message is not both.
    if (const Wqe* boundWqe = tag.getReceiveWqe(opcode.operationType(), msn);
        boundWqe != nullptr &&
        boundWqe->getWorkType() != Wqe::WorkType::kReceive) {
      return RequestValidationError::kInvalidRequest;
    }
    int addrErr = 0;
    tag.assignInitiatorMsnToReceiveWqeIfNeeded(
        dataHeader.getCsn(),
        msn,
        rwPacket.getReadWriteHeader(),
        &addrErr,
        qp_.getMemoryRegions(),
        qp_.getMtu());
    if (addrErr) {
      validationError = RequestValidationError::kAddressError;
    }
  } else if (opcode.operationType() == OperationType::READ) {
    const ReadWritePacket& rwPacket = *CHECK_NOTNULL(readWritePacket);
    int addrErr = 0;
    tag.createVirtualSendWqeIfNeeded(
        msn,
        rwPacket.getReadWriteHeader(),
        &addrErr,
        qp_.getMemoryRegions(),
        qp_.getMtu());
    if (addrErr) {
      validationError = RequestValidationError::kAddressError;
    }
  } else if (opcode.operationType() == OperationType::READ_IMM_P) {
    const ReadWritePacket& rwPacket = *CHECK_NOTNULL(readWritePacket);
    // A read-with-immediate reports on a posted receive, but a plain READ
    // on this msn would have created a virtual one: one message is not both.
    if (const Wqe* boundWqe = tag.getReceiveWqe(opcode.operationType(), msn);
        boundWqe != nullptr &&
        boundWqe->getWorkType() != Wqe::WorkType::kReceive) {
      return RequestValidationError::kInvalidRequest;
    }
    // Binds a posted receive as a completion notification, not a scatter
    // destination; with none posted, the request is left to BRNR.
    tag.assignInitiatorMsnToReceiveWqeIfNeeded(
        dataHeader.getCsn(), msn, qp_.getMtu());
    if (auto* boundReceive = tag.getReceiveWqe(opcode.operationType(), msn);
        boundReceive != nullptr) {
      DCHECK(boundReceive->getWorkType() == Wqe::WorkType::kReceive)
          << "binding takes only posted receives, and a virtual WQE on this "
             "msn was rejected above";
      // The completion reports the length of the read, not the posted size.
      static_cast<ReceiveWqe*>(boundReceive)
          ->updateSizeForReadWithImm(
              rwPacket.getReadWriteHeader().getMessageLength());
      int addrErr = 0;
      tag.createVirtualReadImmResponseWqeIfNeeded(
          msn,
          rwPacket.getReadWriteHeader(),
          &addrErr,
          qp_.getMemoryRegions(),
          qp_.getMtu());
      if (addrErr) {
        validationError = RequestValidationError::kAddressError;
      }
    }
  } else if (
      opcode.operationType() == OperationType::ATOMIC_FETCH_ADD ||
      opcode.operationType() == OperationType::ATOMIC_CMP_SWP) {
    // Spec 5.5: atomic ops are always single-POSN (8 bytes).
    if (dataHeader.getPosn() != 0) {
      validationError = RequestValidationError::kInvalidRequest;
    } else {
      const AtomicPacket& atomicPacket =
          *CHECK_NOTNULL(dynamic_cast<const AtomicPacket*>(&packet));
      const auto& atomicHeader = atomicPacket.getAtomicHeader();
      const auto atomicTarget = tag.validateAtomicRequest(
          atomicHeader.getRemoteAddr(),
          atomicHeader.getRkey(),
          qp_.getMemoryRegions());
      validationError = atomicTarget.error;
      if (validationError == RequestValidationError::kNone) {
        tag.createVirtualAtomicExecWqeIfNeeded(
            msn,
            opcode.operationType() == OperationType::ATOMIC_FETCH_ADD,
            atomicTarget.address,
            atomicHeader.getSwapOrAddData(),
            atomicHeader.getCompareData());
      }
    }
  }
  return validationError;
}

// Only the ack status is meaningful; AckHeaderBuilder requires the other
// fields, so fill them with the spec's NAK values (8.5.6: CumulativeFSN =
// failed FSN).
static AckHeader makeNakAck(
    const DataHeader& dataHeader,
    RequestValidationError error) {
  DCHECK_NE(
      static_cast<int>(error), static_cast<int>(RequestValidationError::kNone))
      << "a NAK is built only for a request that failed validation; passing "
         "no error here would send the peer an invalid-request NAK for a "
         "request this responder accepted";
  AckStatus status = AckStatus::NAK_INVALID_REQUEST;
  switch (error) {
    case RequestValidationError::kAddressError:
      status = AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR;
      break;
    case RequestValidationError::kOperationalError:
      status = AckStatus::NAK_OPERATIONAL_ERROR;
      break;
    // kInvalidRequest maps to the initialized default; kNone is excluded above.
    case RequestValidationError::kNone:
    case RequestValidationError::kInvalidRequest:
      break;
  }
  return AckHeader::AckHeaderBuilder()
      .setTag(dataHeader.getTag())
      .setCumulativeFsn(dataHeader.getFsn())
      .setMaxRcvdFsn(dataHeader.getFsn())
      .setTimestamp(dataHeader.getTimestamp())
      .setCumulRespDeliveredMsn(dataHeader.getMsn())
      .setAckStatus(status)
      .setRateHints(/* reserved on nak */ 0)
      .build();
}

std::optional<AckHeader> RxFlow::processDataPacket(
    const DataPacket* const dataPacket,
    const uint8_t ipTos) {
  const DataPacket& packet = *CHECK_NOTNULL(dataPacket);
  const auto& dataHeader = packet.getDataHeader();
  const auto fsn = dataHeader.getFsn();
  const auto& opcode = packet.getBaseHeader().getOpcode();

  // An opcode the parser could not decode: NAK it and error its tag, like any
  // other request-validation failure. Checked before the duplicate test so it
  // applies regardless of whether the fsn looks like a duplicate.
  if (dynamic_cast<const UnsupportedPacket*>(&packet) != nullptr) {
    LOG_FIRST_N(ERROR, 10) << "Received unsupported opcode " << opcode
                           << " on qp: " << qp_.getLocalQpNum()
                           << ", sending NAK Invalid Request";
    if (Tag* const tag = qp_.getTag(dataHeader.getTag()); tag) {
      if (tag->isErrorNakHeld(getNowNs64())) {
        return std::nullopt;
      }
      enterTagErrorAndFlushReceives(*tag, std::nullopt);
      tag->holdErrorNaks(getNowNs64());
    }
    return makeNakAck(dataHeader, RequestValidationError::kInvalidRequest);
  }

  // if we have already acked (or brnr'd) this packet, we treat it as a
  // duplicate: re-ack it, but do not re-validate or re-deliver.
  const uint16_t offset = fsn - rxState_.rcvNxt;
  const bool isDuplicate = before(fsn, rxState_.rcvNxt) ||
      (rxState_.rcvBitmap.size() > offset &&
       rxState_.rcvBitmap[offset] != FsnState::UNACKED);

  const bool ackForEcn = shouldAckForEcn(ipTos);
  // Send an ACK:
  // 1. if requested to
  // 2. if there are out of order packets
  // 3. if this packet is out of order
  // 4. if the ECN status on the packet requires it
  const bool genAck = dataHeader.getAckRequest() ||
      (rxState_.rcvBitmap.size() > 0) || (fsn != rxState_.rcvNxt) || ackForEcn;

  // Validate and deliver a new FSN; a duplicate skips both.
  if (!isDuplicate) {
    // Only an error NAK holds a tag, so a held tag is already in error. A tag
    // that has never sent one keeps healthy traffic off the clock.
    if (Tag* const tag = qp_.getTag(dataHeader.getTag()); tag != nullptr &&
        tag->hasSentErrorNak() && tag->isErrorNakHeld(getNowNs64())) {
      return std::nullopt;
    }
    if (auto nak = processNewDataPacket(packet)) {
      return nak;
    }
    qp_.updateRcnActiveState(dataHeader.getRateHints());
  }

  // Logged after validation so an invalid new FSN (NAK'd above) is not logged.
  VLOG(2) << "Data received fsn: " << fsn << " posn: " << dataHeader.getPosn()
          << " flow id: " << flowId_ << " msn: " << dataHeader.getMsn()
          << " csn: " << dataHeader.getCsn();

  if (genAck) {
    const uint8_t ackTag =
        qp_.containsTag(dataHeader.getTag()) ? dataHeader.getTag() : 0;
    const Tag& ackingTag = qp_.existingTag(ackTag);
    auto ackHeaderBuilder = AckHeader::AckHeaderBuilder()
                                .setCnpCE(rxState_.getAndClearReceivedCeCount())
                                .setTag(ackTag)
                                .setCumulativeFsn(rxState_.rcvNxt - 1)
                                .setMaxRcvdFsn(rxState_.maxRcvdFsn)
                                .setTimestamp(dataHeader.getTimestamp())
                                .setCumulRespDeliveredMsn(ackingTag.getCDMSN())
                                .setRateHints(qp_.getPerQpRateGbps());

    if (rxState_.rcvBitmap.empty()) {
      // No out of order packets and the current packet did
      // not trigger a BRNR, so send normal ACK
      ackHeaderBuilder.setAckStatus(AckStatus::ACK);
    } else {
      ackHeaderBuilder.setFsnBitVector(
          buildBitVectorForState(rxState_.rcvBitmap, FsnState::ACKED));
      if (rxState_.hasRnrBitsSet()) {
        ackHeaderBuilder
            .setRnrBitVector(
                buildBitVectorForState(rxState_.rcvBitmap, FsnState::BRNR))
            .setAckStatus(AckStatus::BRNR)
            .setRnrBackoffTimeExp(kBrnrBackoffTimeExp);
      } else {
        ackHeaderBuilder.setAckStatus(AckStatus::ACK_OUT_OF_ORDER);
      }
    }
    return std::move(ackHeaderBuilder).build();
  }
  return std::nullopt;
}

} // namespace libsoftmetaroce
