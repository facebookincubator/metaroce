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

#include <libsoftmetaroce/Sender.h>

#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/Ecn.h>
#include <libsoftmetaroce/PacketModifierSocket.h>
#include <libsoftmetaroce/PacketSocketFactory.h>
#include <algorithm>
#include <cerrno>
#include <string>

namespace libsoftmetaroce {

Sender::~Sender() {
  shutdownAndJoinSendLoops();
  // Socket creation populates both per-CoS entries or throws.
  const uint16_t socketCount = socketAssignment_.lock()->socketCount;
  for (uint16_t socketId = 0; socketId < socketCount; ++socketId) {
    // Installed only by createSocketWithLocalAddr, from a throwing factory.
    CHECK_NOTNULL(sockets_[socketId].data.get())->close();
    CHECK_NOTNULL(sockets_[socketId].ack.get())->close();
  }

  if (FLAGS_libsoftmetaroce_profile_send_egress) {
    const uint64_t writes = sendWriteCount_.load(std::memory_order_relaxed);
    if (writes > 0) {
      const uint64_t lockWaitNs =
          sendLockWaitNs_.load(std::memory_order_relaxed);
      const uint64_t writeNs = sendWriteNs_.load(std::memory_order_relaxed);
      const uint64_t lockPlusWriteNs = lockWaitNs + writeNs;
      const double lockWaitPct = lockPlusWriteNs > 0
          ? 100.0 * static_cast<double>(lockWaitNs) /
              static_cast<double>(lockPlusWriteNs)
          : 0.0;
      LOG(INFO) << "send-egress profile: writes=" << writes
                << " avgWrite=" << (writeNs / writes)
                << "ns avgLockWait=" << (lockWaitNs / writes)
                << "ns lockWait=" << lockWaitPct << "% of (lock+write)";
    }
  }
}

uint16_t Sender::getSocketIdFromAddress(
    const folly::IPAddress& localAddr,
    const ibv_gid_entry& entry) {
  // Socket creation/selection can run concurrently (verbs-thread setup vs
  // retx-timer repath), so serialize the whole check-then-create/select.
  auto assignment = socketAssignment_.lock();
  if (hasRoomForSocket(assignment)) {
    return createSocketWithLocalAddr(assignment, localAddr, entry);
  }
  return getLeastUsedSocketId(assignment, localAddr);
}

bool Sender::hasRoomForSocket(const LockedSocketAssignment& assignment) const {
  return assignment->socketCount <
      std::min<size_t>(socketLimit(), kMaxNumSockets);
}

folly::IPAddress Sender::getAddressFromSocketId(const uint16_t socketId) {
  const auto assignment = socketAssignment_.lock();
  return getAddressFromSocketIdLocked(assignment, socketId);
}

uint16_t Sender::getAlternateSocket(const uint16_t socketId) {
  return getAlternateSocketImpl(socketId, EgressPacket::Kind::Data);
}

uint16_t Sender::getAlternateAckSocket(const uint16_t socketId) {
  return getAlternateSocketImpl(socketId, EgressPacket::Kind::Ack);
}

uint16_t Sender::getAlternateSocketImpl(
    const uint16_t socketId,
    EgressPacket::Kind kind) {
  auto assignment = socketAssignment_.lock();
  CHECK_LT(socketId, assignment->socketCount) << "socket id out of range";
  // Socket creation populates both per-CoS entries or throws.
  auto& socket = *CHECK_NOTNULL(socketForKind(sockets_[socketId], kind));
  // A raw transport has no UDP source port to vary.
  if (socket.getSocket() == nullptr) {
    return socketId;
  }
  const folly::IPAddress& localAddr =
      getAddressFromSocketIdLocked(assignment, socketId);
  const auto it = assignment->localAddrToSocketIds.find(localAddr);
  CHECK(it != assignment->localAddrToSocketIds.end())
      << "socket has no registered address";
  const auto& ids = it->second;
  auto next = ids.find(socketId);
  CHECK(next != ids.end()) << "socket id not registered under its address";
  if (++next == ids.end()) {
    next = ids.begin();
  }
  if (*next != socketId) {
    return *next;
  }
  if (!hasRoomForSocket(assignment)) {
    return socketId;
  }
  return createSocketWithLocalAddr(assignment, localAddr, ibv_gid_entry{});
}

void Sender::shutdownAndJoinSendLoops() noexcept {
  cancellationSource_.requestCancellation();
  folly::coro::blockingWait([this]() -> folly::coro::Task<> {
    co_await arbiter_.shutdown();
    co_await asyncScope_.joinAsync();
  }());
}

Sender::Sender(
    QpArbiterIface& arbiter,
    std::function<std::pair<
        std::unique_ptr<folly::AsyncUDPSocket>,
        std::unique_ptr<Modifier>>(folly::EventBase*)> socketMaker,
    uint16_t sendToPort,
    bool startSendLoop,
    uint32_t numSendingThreads,
    uint32_t sendBatch)
    : arbiter_(arbiter),
      socketMaker_{std::move(socketMaker)},
      parallelSend_{numSendingThreads >= 2},
      gsoEnabled_{FLAGS_libsoftmetaroce_send == "gso"},
      sendmmsgEnabled_{FLAGS_libsoftmetaroce_send == "sendmmsg"},
      sendBatch_{sendBatch},
      sendToPort_{sendToPort} {
  // The gflag validator enforces this, but a direct caller could pass 0.
  CHECK_GE(numSendingThreads, 1u);
  if (startSendLoop) {
    if (parallelSend_) {
      // Create the executors before adding any coroutine: a throw here then
      // leaves asyncScope_ empty, so a ctor failure needs no drain.
      for (uint32_t i = 0; i < numSendingThreads; ++i) {
        sendExecutors_.push_back(
            std::make_unique<folly::CPUThreadPoolExecutor>(
                1,
                std::make_shared<folly::NamedThreadFactory>(
                    "lsmrSend" + std::to_string(i))));
      }
      try {
        for (uint32_t i = 0; i < numSendingThreads; ++i) {
          asyncScope_.add(co_withExecutor(
              sendExecutors_[i].get(),
              folly::coro::co_withCancellation(
                  cancellationSource_.getToken(), sendLoop())));
        }
      } catch (...) {
        // A partial add leaves started send loops parked in a non-cancellable
        // claim; join them (via shutdown) before asyncScope_ is destroyed,
        // since ~Sender does not run on constructor failure.
        shutdownAndJoinSendLoops();
        throw;
      }
    } else {
      asyncScope_.add(co_withExecutor(
          folly::getGlobalCPUExecutor(),
          folly::coro::co_withCancellation(
              cancellationSource_.getToken(), sendLoop())));
    }
  }
}

folly::coro::Task<void> Sender::sendLoop() {
  while (true) {
    co_await folly::coro::co_safe_point;
    QpArbiterIface::ClaimedPacket claimed =
        co_await arbiter_.claimNextPacketToSend();
    if (!claimed.packet) {
      co_return; // shutdown: an empty claim tells this consumer to exit.
    }
    // Assert: a claim always holds a built frame, so a null inner packet
    // is an arbiter bug rather than the empty claim that signals shutdown.
    DCHECK(claimed.packet->packet) << "claimed packet holds no frame";
    // The chosen sendClaimedBatch* helper sends this QP's batch and yields
    // the QP, or keeps sending in place on a full ready queue.
    bool faulted;
    if (sendmmsgEnabled_) {
      faulted = co_await sendClaimedBatchInRuns(
          claimed.qpNum,
          std::move(claimed.packet),
          &Sender::sameMmsgRun,
          &Sender::emitMmsgRun);
    } else if (gsoEnabled_) {
      faulted = co_await sendClaimedBatchInRuns(
          claimed.qpNum,
          std::move(claimed.packet),
          &Sender::sameGsoRun,
          &Sender::emitRun);
    } else {
      faulted =
          co_await sendClaimedBatch(claimed.qpNum, std::move(claimed.packet));
    }
    (void)faulted;
  }
}

folly::coro::Task<bool>
Sender::yieldForAnotherTurn(uint32_t qpNum, bool faulted, bool settled) {
  std::unique_ptr<struct EgressPacket> next;
  while (!faulted && !settled && !arbiter_.reenqueueQp(qpNum) &&
         (next = arbiter_.nextPacketOrIdle(qpNum)) != nullptr) {
    faulted = co_await writeEgress(std::move(next));
  }
  co_return faulted;
}

folly::coro::Task<bool> Sender::sendClaimedBatch(
    uint32_t qpNum,
    std::unique_ptr<struct EgressPacket> first) {
  bool faulted = co_await writeEgress(std::move(first));
  std::unique_ptr<struct EgressPacket> next;
  uint32_t sent = 1;
  for (; !faulted && sent < sendBatch_ &&
       (next = arbiter_.nextPacketOrIdle(qpNum)) != nullptr;
       ++sent) {
    faulted = co_await writeEgress(std::move(next));
  }
  // Settled to idle iff the build ran dry rather than hitting the cap or
  // faulting: only then is sent still below the batch cap.
  const bool settled = !faulted && sent < sendBatch_;
  co_return co_await yieldForAnotherTurn(qpNum, faulted, settled);
}

folly::coro::Task<bool> Sender::sendClaimedBatchInRuns(
    uint32_t qpNum,
    std::unique_ptr<struct EgressPacket> first,
    SameRunFn sameRun,
    EmitRunFn emitRunFn) {
  std::vector<std::unique_ptr<struct EgressPacket>> batch;
  batch.reserve(sendBatch_);
  batch.push_back(std::move(first));
  std::unique_ptr<struct EgressPacket> next;
  while (batch.size() < sendBatch_ &&
         (next = arbiter_.nextPacketOrIdle(qpNum)) != nullptr) {
    batch.push_back(std::move(next));
  }
  // Settled to idle iff the build ran dry rather than hitting the cap: only
  // then is the batch still short of the cap.
  const bool settled = batch.size() < sendBatch_;
  // A faulting run already parked the QP, so stop there.
  bool faulted = false;
  for (size_t runStart = 0; !faulted && runStart < batch.size();) {
    size_t runEnd = runStart + 1;
    while (runEnd < batch.size() && sameRun(*batch[runStart], *batch[runEnd])) {
      ++runEnd;
    }
    faulted = co_await (this->*emitRunFn)(batch, runStart, runEnd);
    runStart = runEnd;
  }
  co_return co_await yieldForAnotherTurn(qpNum, faulted, settled);
}

folly::coro::Task<bool> Sender::emitRun(
    std::vector<std::unique_ptr<struct EgressPacket>>& batch,
    size_t runStart,
    size_t runEnd) {
  // Every claimed egress packet has a resolved send socket.
  auto& socket = *CHECK_NOTNULL(batch[runStart]->socketToSendOn);
  // getGSO() is read unlocked with no unsynchronized writer: gsoUnsupported_
  // is atomic, and modifier_ is set at construction and cleared only in
  // close(), which runs after these send loops drain.
  const bool useGso = (runEnd - runStart) > 1 &&
      batch[runStart]->rxmitType == EgressPacket::RxmitType::None &&
      socket.getGSO() >= 0;
  if (!useGso) {
    for (size_t k = runStart; k < runEnd; ++k) {
      if (co_await writeEgress(std::move(batch[k]))) {
        co_return true;
      }
    }
    co_return false;
  }
  // Same-run packets are equal size, so a fixed segment count fits the byte
  // budget; a longer run becomes several writeGSO calls.
  const size_t segBytes =
      std::max<size_t>(1, batch[runStart]->packet->getSizeBytes());
  const size_t maxSegs = std::max<size_t>(
      1, std::min<size_t>(kGsoMaxSegments, kMaxGsoBytes / segBytes));
  for (size_t chunk = runStart; chunk < runEnd; chunk += maxSegs) {
    const size_t end = std::min(runEnd, chunk + maxSegs);
    ssize_t written = 0;
    int writeErrno = 0;
    {
      // AsyncUDPSocket is not thread-safe; serialize the GSO frame+write on
      // this socket when senders share it; a no-op lock for a single sender.
      auto lock = socket.lockForWrite();
      socket.beginGsoBatch();
      try {
        for (size_t k = chunk; k < end; ++k) {
          socket.write(
              folly::SocketAddress(batch[k]->remoteAddr, sendToPort_),
              std::move(batch[k]->packet));
        }
        // flushGsoBatch performs the send and reports a throwing syscall as a
        // failed write, so no exception escapes to strand the claimed QP.
        written = socket.flushGsoBatch();
        // Read errno immediately after the flush, before any later call can
        // overwrite the thread-local value.
        writeErrno = errno;
      } catch (const std::exception& ex) {
        // A throw must not escape without releasing the QP; treat it as a
        // fault, mirroring writeEgress.
        socket.abortGsoBatch();
        written = -1;
        writeErrno = EIO;
        LOG(WARNING) << "GSO accumulate threw, treating as egress fault: "
                     << ex.what();
      } catch (...) {
        socket.abortGsoBatch();
        written = -1;
        writeErrno = EIO;
        LOG(DFATAL)
            << "GSO accumulate threw a non-std exception, treating as fault";
      }
    }
    if (written <= 0) {
      // Only a buffer-full EAGAIN is transient (keep GSO, park the QP); any
      // other non-positive result -- a real errno or an unexpected 0 --
      // disables GSO so we fall back to per-packet instead of retrying GSO.
      const bool bufferFull = written < 0 && writeErrno == EAGAIN;
      if (!bufferFull && socket.disableGso()) {
        LOG(WARNING) << "GSO write failed (written=" << written
                     << " errno=" << writeErrno
                     << "); falling back to per-packet writes";
      }
      // Report EAGAIN even for a real errno so handleFaultOnEgress parks the
      // QP instead of LOG(FATAL)-ing; GSO is now off, so a real error
      // resurfaces on the per-packet fallback and the RTO covers the drop.
      // batch[chunk]'s frame was already moved into the GSO batch above, so
      // its packet is null here; the handler re-enqueues by qpNum.
      co_await arbiter_.handleFaultOnEgress(std::move(batch[chunk]), EAGAIN);
      co_return true;
    }
  }
  co_return false;
}

folly::coro::Task<bool> Sender::emitMmsgRun(
    std::vector<std::unique_ptr<struct EgressPacket>>& batch,
    size_t runStart,
    size_t runEnd) {
  // Every claimed egress packet has a resolved send socket.
  auto& socket = *CHECK_NOTNULL(batch[runStart]->socketToSendOn);
  const bool useMmsg = (runEnd - runStart) > 1 &&
      batch[runStart]->rxmitType == EgressPacket::RxmitType::None &&
      socket.canBatchWritem();
  if (!useMmsg) {
    for (size_t k = runStart; k < runEnd; ++k) {
      if (co_await writeEgress(std::move(batch[k]))) {
        co_return true;
      }
    }
    co_return false;
  }
  int sent = 0;
  {
    // AsyncUDPSocket is not thread-safe; serialize the writem on this socket
    // when senders share it; a no-op lock for a single sender.
    auto lock = socket.lockForWrite();
    socket.beginWritemBatch();
    try {
      for (size_t k = runStart; k < runEnd; ++k) {
        socket.write(
            folly::SocketAddress(batch[k]->remoteAddr, sendToPort_),
            std::move(batch[k]->packet));
      }
      sent = socket.flushWritemBatch();
    } catch (const std::exception& ex) {
      // A throw must not escape without releasing the QP; treat it as a
      // fault, mirroring writeEgress.
      socket.abortWritemBatch();
      sent = -1;
      LOG(WARNING) << "writem accumulate threw, treating as egress fault: "
                   << ex.what();
    } catch (...) {
      socket.abortWritemBatch();
      sent = -1;
      LOG(DFATAL)
          << "writem accumulate threw a non-std exception, treating as fault";
    }
  }
  if (sent <= 0) {
    // sendmmsg does not return 0 for a non-empty batch, so only -1 is
    // expected; the consumed frames recover via the retransmit timer.
    // flushWritemBatch already logged the failing errno.
    co_await arbiter_.handleFaultOnEgress(std::move(batch[runStart]), EAGAIN);
    co_return true;
  }
  if (static_cast<size_t>(sent) < runEnd - runStart) {
    // Partial sendmmsg: the unsent tail's IOBufs are already consumed, so
    // fault the first unsent packet to re-service the QP; the dropped tail
    // recovers via the retransmit timer.
    co_await arbiter_.handleFaultOnEgress(
        std::move(batch[runStart + sent]), EAGAIN);
    co_return true;
  }
  co_return false;
}

folly::coro::Task<bool> Sender::writeEgress(
    std::unique_ptr<struct EgressPacket> egressPacket) {
  // writeEgress receives only claimed egress packets.
  auto& egress = *CHECK_NOTNULL(egressPacket.get());
  // Every claimed egress packet has a resolved send socket.
  auto& socket = *CHECK_NOTNULL(egress.socketToSendOn);
  ssize_t written = 0;
  int writeErrno = 0;
  // Only the parallel path takes the per-socket lock, so the lock-wait sample
  // is meaningful only then; skip profiling in single-consumer runs.
  const bool profileEgress =
      FLAGS_libsoftmetaroce_profile_send_egress && parallelSend_;
  // Reads the clock only while profiling, so the disabled path stays a plain
  // write with no added clock reads.
  const auto sampleNs = [profileEgress]() -> uint64_t {
    return profileEgress ? steadyNowNs() : 0;
  };
  const uint64_t beforeLockNs = sampleNs();
  uint64_t afterLockNs = 0;
  uint64_t beforeWriteNs = 0;
  uint64_t afterWriteNs = 0;
  {
    // AsyncUDPSocket is not thread-safe; serialize the cmsg-set + write on
    // this socket so parallel senders servicing QPs that share it do not
    // race. Released before the fault co_await below; a no-op lock in single
    // send.
    auto sockLk = socket.lockForWrite();
    afterLockNs = sampleNs();
    // A Fast retransmit lowers the packet's hop limit via a non-default cmsg;
    // the func persists on this socket, which is shared across QPs.
    const bool fastRetx = egress.rxmitType == EgressPacket::RxmitType::Fast;
    if (fastRetx) {
      socket.setAdditionalCmsgsFunc(([]() {
        folly::SocketCmsgMap map;
        map[{IPPROTO_IPV6, IPV6_HOPLIMIT}] = kFastRetransmitHopLimit;
        return map;
      }));
    }
    // Restore on every exit path, incl. a throwing write(): a leaked
    // fast-retransmit hop limit would corrupt later sends on this shared
    // socket.
    SCOPE_EXIT {
      if (fastRetx) {
        socket.setAdditionalCmsgsFunc(([]() { return folly::none; }));
      }
    };
    // Build the destination first: a temporary destroyed between write() and
    // the errno snapshot below could clobber errno.
    const folly::SocketAddress dest(egress.remoteAddr, sendToPort_);
    beforeWriteNs = sampleNs();
    try {
      written = socket.write(dest, std::move(egress.packet));
      // Snapshot write()'s errno now: a later call could overwrite the
      // thread-local errno before the fault handler below reads it.
      writeErrno = errno;
    } catch (const std::exception& ex) {
      // A throwing write() must not escape: it would unwind sendLoop before
      // the QP is returned and strand the claimed QP. Treat it as a fault so
      // the arbiter parks the QP and the send loop survives.
      written = -1;
      writeErrno = EIO;
      LOG(WARNING) << "socket->write() threw, treating as egress fault: "
                   << ex.what();
    } catch (...) {
      // A non-std::exception throwable is unexpected (likely a programming
      // error); surface it in dev/test via DFATAL, but keep the same
      // don't-strand contract in prod.
      written = -1;
      writeErrno = EIO;
      LOG(DFATAL)
          << "socket->write() threw a non-std exception, treating as fault";
    }
    afterWriteNs = sampleNs();
  }
  // Record profiling outside the critical section so the atomics do not
  // extend the hold time and inflate the lock-wait they measure.
  if (profileEgress) {
    sendLockWaitNs_.fetch_add(
        afterLockNs - beforeLockNs, std::memory_order_relaxed);
    sendWriteNs_.fetch_add(
        afterWriteNs - beforeWriteNs, std::memory_order_relaxed);
    sendWriteCount_.fetch_add(1, std::memory_order_relaxed);
  }
  if (written <= 0) {
    // The packet was std::move()d above. Ensure the fault handler doesn't use
    // it.
    egress.packet = nullptr;
    co_await arbiter_.handleFaultOnEgress(std::move(egressPacket), writeErrno);
    co_return true;
  }
  co_return false;
}

const folly::IPAddress& Sender::getAddressFromSocketIdLocked(
    const LockedSocketAssignment& assignment,
    const uint16_t socketId) {
  for (const auto& [addr, socketIds] : assignment->localAddrToSocketIds) {
    if (socketIds.find(socketId) != socketIds.end()) {
      return addr;
    }
  }
  LOG(FATAL) << "getAddressFromSocketId: no address found for socket ID "
             << socketId;
}

uint16_t Sender::getLeastUsedSocketId(
    LockedSocketAssignment& assignment,
    const folly::IPAddress& localAddr) {
  auto it = assignment->localAddrToSocketIds.find(localAddr);
  if (it == assignment->localAddrToSocketIds.end()) {
    throw std::runtime_error(
        "No sockets available for address " + localAddr.str() +
        " and no more sockets can be created.");
  }
  // Iterate over the set of socket ids for the given address and find the
  // one with the lowest frequency
  uint16_t leastUsedSocketId = *it->second.begin();
  for (auto socketId : it->second) {
    if (assignment->socketIdFreqMap[socketId] <
        assignment->socketIdFreqMap[leastUsedSocketId]) {
      leastUsedSocketId = socketId;
    }
  }
  // Increment the frequency of the least used socket id
  assignment->socketIdFreqMap[leastUsedSocketId]++;
  return leastUsedSocketId;
}

uint16_t Sender::createSocketWithLocalAddr(
    LockedSocketAssignment& assignment,
    folly::IPAddress localAddr,
    const ibv_gid_entry& entry) {
  // The caller checked hasRoomForSocket() under this lock; sockets_ is fixed
  // storage.
  CHECK_LT(assignment->socketCount, kMaxNumSockets);
  auto makeSocket = [&](uint8_t dscp, uint8_t esunCos) {
    auto owned = makePacketModifierSocket(&evb_, entry, socketMaker_);
    // makePacketModifierSocket throws instead of returning null.
    auto& socket = *CHECK_NOTNULL(owned.get());
    socket.sizeSndBufBeforeBind();
    assignment->sourcePorts.bindNext(socket, localAddr);
    socket.checkSndBuf();
    // Place the DSCP code point in the upper 6 bits of the TOS byte and
    // OR in ECT(0) in the lower 2 (ECN) bits to keep the socket
    // ECN-capable. No-op for raw Ethernet, which has no IP TOS.
    socket.setTosOrTrafficClass(static_cast<uint8_t>((dscp << 2) | kEcnECT0));
    socket.setEsunCos(esunCos);
    // Under parallel send, consumers sharing this socket serialize on its
    // own write mutex.
    if (parallelSend_) {
      socket.enableWriteMutex();
    }
    return owned;
  };

  PerCosSockets perCosSockets{
      .data = makeSocket(
          static_cast<uint8_t>(FLAGS_libsoftmetaroce_data_dscp),
          static_cast<uint8_t>(FLAGS_libsoftmetaroce_esun_data_cos)),
      .ack = makeSocket(
          static_cast<uint8_t>(FLAGS_libsoftmetaroce_ack_dscp),
          static_cast<uint8_t>(FLAGS_libsoftmetaroce_esun_ack_cos)),
  };

  // Store the socket pair and track which address it's bound to. socketId is
  // the new entry's index; entries are never removed.
  uint16_t socketId = assignment->socketCount;
  sockets_[socketId] = std::move(perCosSockets);
  ++assignment->socketCount;
  assignment->socketIdFreqMap[socketId] = 1;
  assignment->localAddrToSocketIds[localAddr].insert(socketId);
  return socketId;
}

} // namespace libsoftmetaroce
