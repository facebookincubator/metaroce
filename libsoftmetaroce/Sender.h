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

#include <folly/CancellationToken.h>
#include <folly/Synchronized.h>
#include <folly/Utility.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/Task.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/io/async/EventBase.h>
#include <infiniband/verbs.h>
#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/PacketModifierSocket.h>
#include <libsoftmetaroce/QpArbiterIface.h>
#include <libsoftmetaroce/SourcePortCounter.h>
#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace libsoftmetaroce {
/*
 * This class manages a pool of sockets that are assigned to QPs via
 * getNextSocketIdToAssign(). It also runs a loop to send packets.
 *
 * Each logical socketId backs two physical sender sockets: one for data packets
 * and one for ACK / control packets. Each socket has its IP TOS / Traffic Class
 * set once at creation from FLAGS_libsoftmetaroce_{data,ack}_dscp shifted into
 * the upper 6 bits, with ECT(0) OR'd into the lower 2 (ECN) bits.
 */
class Sender : public folly::NonCopyableNonMovable {
 public:
  constexpr static uint16_t kMaxNumSockets = 128;
  constexpr static uint16_t kDefaultSendToPort = 0xace; // will be replaced

  explicit Sender(
      QpArbiterIface& arbiter,
      std::function<std::pair<
          std::unique_ptr<folly::AsyncUDPSocket>,
          std::unique_ptr<Modifier>>(folly::EventBase*)> socketMaker,
      uint16_t sendToPort = kDefaultSendToPort,
      uint32_t numSendingThreads = 1,
      uint32_t sendBatch = 1)
      : Sender(
            arbiter,
            std::move(socketMaker),
            sendToPort,
            true,
            numSendingThreads,
            sendBatch) {}

  explicit Sender(
      QpArbiterIface& arbiter,
      uint16_t sendToPort = kDefaultSendToPort,
      uint32_t numSendingThreads = 1)
      : Sender(
            arbiter,
            [](folly::EventBase* evb) {
              return std::make_pair(
                  std::make_unique<folly::AsyncUDPSocket>(evb), nullptr);
            },
            sendToPort,
            true,
            numSendingThreads) {}

  virtual ~Sender();

  /*
   * Creates up to kMaxNumSockets sockets and binds them. Returns the next
   * socket Id, in a round robin fashion, to be used by a flow.
   */
  // Non-virtual convenience overload for callers that do not have an entry.
  uint16_t getSocketIdFromAddress(const folly::IPAddress& localAddr) {
    return getSocketIdFromAddress(localAddr, ibv_gid_entry{});
  }

  uint16_t getSocketIdFromAddress(
      const folly::IPAddress& localAddr,
      const ibv_gid_entry& entry);

  // Lock-free hot path: sockets_ is fixed storage.
  PacketModifierSocket* getSendSocket(
      uint16_t socketId,
      EgressPacket::Kind kind) {
    return socketForKind(sockets_[socketId], kind);
  }

  /*
   * Return the local address for a given socketId
   */
  folly::IPAddress getAddressFromSocketId(const uint16_t socketId);

  // Return a same-address alternative, creating one if allowed, or socketId.
  [[nodiscard]] uint16_t getAlternateSocket(const uint16_t socketId);

  /*
   * Return the next socket on socketId's local address, so successive calls
   * cycle through that address's sockets; each binds its own UDP source port,
   * which changes the ACK entropy. Where socketId is alone on its address,
   * creates another there if the socket limit allows. Returns socketId on a
   * raw transport or when it cannot create one.
   */
  [[nodiscard]] uint16_t getAlternateAckSocket(uint16_t socketId);

  // The per-turn send cap, so a QP can bound how long it holds one tx flow.
  [[nodiscard]] uint32_t getSendBatch() const {
    return sendBatch_;
  }

  // This is for testing purpose only.
  folly::AsyncUDPSocket* getSocketById(
      uint16_t socketId,
      EgressPacket::Kind kind = EgressPacket::Kind::Data) {
    const auto assignment = socketAssignment_.lock();
    if (socketId >= assignment->socketCount) {
      throw std::out_of_range("socket id not found");
    }
    // Socket creation populates both per-CoS entries or throws.
    return CHECK_NOTNULL(socketForKind(sockets_[socketId], kind))->getSocket();
  }

 private:
  [[nodiscard]] uint16_t getAlternateSocketImpl(
      uint16_t socketId,
      EgressPacket::Kind kind);

 protected:
  // Called under socketAssignment_, so an override must not lock; the result is
  // clamped to kMaxNumSockets.
  virtual size_t socketLimit() const {
    return kMaxNumSockets;
  }

  // What decides which socket a flow is given, or whether a new one is made.
  struct SocketAssignment {
    std::unordered_map<uint16_t, uint32_t> socketIdFreqMap;
    std::unordered_map<folly::IPAddress, std::unordered_set<uint16_t>>
        localAddrToSocketIds;
    SourcePortCounter sourcePorts;
    // How many entries of sockets_ are built.
    uint16_t socketCount{0};
  };
  using LockedSocketAssignment =
      folly::Synchronized<SocketAssignment, std::mutex>::LockedPtr;

  // Holds the pair of per-CoS sender sockets
  struct PerCosSockets {
    std::unique_ptr<PacketModifierSocket> data;
    std::unique_ptr<PacketModifierSocket> ack;
  };

  static PacketModifierSocket* socketForKind(
      const PerCosSockets& perCosSockets,
      EgressPacket::Kind kind) {
    return kind == EgressPacket::Kind::Ack ? perCosSockets.ack.get()
                                           : perCosSockets.data.get();
  }

  // Join asyncScope_ before it is destroyed (folly AsyncScope contract).
  // noexcept: these should not throw, and a throw during teardown or
  // construction failure could only make things worse.
  void shutdownAndJoinSendLoops() noexcept;

  /*
   * Protected constructor used by test subclasses (e.g., LimitedTestSender)
   * to control whether the send loops are started. When startSendLoop
   * is false, the Sender can be used purely for socket management without the
   * automatic packet sending behavior, allowing tests to manually control
   * packet transmission via getNextPacketToSend().
   */
  explicit Sender(
      QpArbiterIface& arbiter,
      std::function<std::pair<
          std::unique_ptr<folly::AsyncUDPSocket>,
          std::unique_ptr<Modifier>>(folly::EventBase*)> socketMaker,
      uint16_t sendToPort,
      bool startSendLoop,
      uint32_t numSendingThreads = 1,
      uint32_t sendBatch = 1);

  /*
   * One send loop: claim a QP's built packet from the arbiter, write it,
   * then release the QP. Runs as the single sender, or as one of N parallel
   * send loops; the arbiter serializes per-QP servicing either way.
   */
  folly::coro::Task<void> sendLoop();

  // Yield the claimed QP for a fair turn; if the ready queue is full, keep
  // sending its packets in place rather than dropping it.
  folly::coro::Task<bool>
  yieldForAnotherTurn(uint32_t qpNum, bool faulted, bool settled);

  // Send a short batch while we own this QP: fewer ready-queue round-trips and
  // better cache locality, bounded by sendBatch_ so other QPs still progress
  // and shutdown waits at most one short batch of non-blocking sends.
  folly::coro::Task<bool> sendClaimedBatch(
      uint32_t qpNum,
      std::unique_ptr<struct EgressPacket> first);

  // Whether two adjacent packets belong to the same emit run.
  using SameRunFn =
      bool (*)(const struct EgressPacket&, const struct EgressPacket&);
  // Emits one run; the coalescing form (writeGSO or sendmmsg) is the emitter's
  // choice.
  using EmitRunFn = folly::coro::Task<bool> (Sender::*)(
      std::vector<std::unique_ptr<struct EgressPacket>>&,
      size_t,
      size_t);

  // Build the whole batch before locking, so the per-socket write lock covers
  // only the emit. Then walk the batch as maximal same-run slices [runStart,
  // runEnd) and emit each.
  folly::coro::Task<bool> sendClaimedBatchInRuns(
      uint32_t qpNum,
      std::unique_ptr<struct EgressPacket> first,
      SameRunFn sameRun,
      EmitRunFn emitRunFn);

  // Packets that may share one GSO datagram: same send socket (which implies
  // the CoS), destination, framed size, and retransmit type. GSO segments are
  // uniform, go to one destination (one gsoDest_), and share one cmsg/TOS.
  static bool sameGsoRun(
      const struct EgressPacket& a,
      const struct EgressPacket& b) {
    // The arbiter supplies frames before run formation; emission moves them.
    return a.socketToSendOn == b.socketToSendOn &&
        a.remoteAddr == b.remoteAddr && a.rxmitType == b.rxmitType &&
        CHECK_NOTNULL(a.packet.get())->getSizeBytes() ==
        CHECK_NOTNULL(b.packet.get())->getSizeBytes();
  }

  // Emit one same-run group: per-packet unless the run holds more than one
  // packet, none a retransmit (which carries a per-packet cmsg), and the socket
  // supports GSO -- then as writeGSO calls each capped at kMaxGsoBytes/
  // kGsoMaxSegments so the kernel never sees an over-size datagram (EMSGSIZE).
  folly::coro::Task<bool> emitRun(
      std::vector<std::unique_ptr<struct EgressPacket>>& batch,
      size_t runStart,
      size_t runEnd);

  // Packets that may share one sendmmsg call: same send socket (which implies
  // the CoS) and retransmit type. Datagram sizes may differ.
  static bool sameMmsgRun(
      const struct EgressPacket& a,
      const struct EgressPacket& b) {
    return a.socketToSendOn == b.socketToSendOn && a.rxmitType == b.rxmitType;
  }

  // Emit one same-run group: per-packet unless the run holds more than one
  // packet, none a retransmit (which carries a per-packet cmsg), and the socket
  // supports writem -- then as one sendmmsg holding the per-socket write lock.
  folly::coro::Task<bool> emitMmsgRun(
      std::vector<std::unique_ptr<struct EgressPacket>>& batch,
      size_t runStart,
      size_t runEnd);

  /*
   * Write one built packet to its socket. Returns true if the write faulted
   * (already handed to the arbiter via handleFaultOnEgress).
   */
  folly::coro::Task<bool> writeEgress(
      std::unique_ptr<struct EgressPacket> egressPacket);

  static const folly::IPAddress& getAddressFromSocketIdLocked(
      const LockedSocketAssignment& assignment,
      const uint16_t socketId);

  static uint16_t getLeastUsedSocketId(
      LockedSocketAssignment& assignment,
      const folly::IPAddress& localAddr);

  bool hasRoomForSocket(const LockedSocketAssignment& assignment) const;

  /*
   * Helper method to build one socket per CoS class (data and ACK), each bound
   * to localAddr with its own source port from the assignment, and sets each
   * socket's TOS once at creation time so per-packet setsockopt calls are not
   * required.
   */
  uint16_t createSocketWithLocalAddr(
      LockedSocketAssignment& assignment,
      folly::IPAddress localAddr,
      const ibv_gid_entry& entry);

  folly::EventBase evb_;
  // Serializes socket-set creation/selection across the verbs-setup and
  // retx-timer repath threads; getSendSocket() stays lock-free.
  folly::Synchronized<SocketAssignment, std::mutex> socketAssignment_;
  // Indexed by socketId; an entry is built under socketAssignment_ before its
  // id is returned.
  std::array<PerCosSockets, kMaxNumSockets> sockets_;
  folly::coro::AsyncScope asyncScope_;
  folly::CancellationSource cancellationSource_;
  // Injected dependencies (arbiter_ owned by MetaRoceVerbs).
  QpArbiterIface& arbiter_;
  std::function<std::pair<
      std::unique_ptr<folly::AsyncUDPSocket>,
      std::unique_ptr<Modifier>>(folly::EventBase*)>
      socketMaker_;

  // Configuration.
  // True when N>=2 send loops run; gates the per-socketId write lock.
  const bool parallelSend_{false};
  // Coalesce each turn's batch into GSO writes (--libsoftmetaroce_send=gso).
  const bool gsoEnabled_{false};
  // Coalesce each turn's batch into one sendmmsg
  // (--libsoftmetaroce_send=sendmmsg).
  const bool sendmmsgEnabled_{false};
  // Max packets a send loop sends from one claimed QP per turn (1 = one per
  // claim); >1 amortizes the ready-queue round-trip and improves cache
  // locality.
  const uint32_t sendBatch_{1};
  const uint16_t sendToPort_;

  // Runtime state.
  // One dedicated executor per parallel send loop (empty in single-send mode).
  // Outlives the send loops, which ~Sender joins before these are destroyed.
  std::vector<std::unique_ptr<folly::CPUThreadPoolExecutor>> sendExecutors_;
  // Optional send-egress timing (--libsoftmetaroce_profile_send_egress): the
  // per-socket write-lock wait vs the socket->write() (sendmsg) cost in the
  // per-packet writeEgress path only; batched send modes are not timed. Logged
  // at teardown.
  std::atomic<uint64_t> sendLockWaitNs_{0};
  std::atomic<uint64_t> sendWriteNs_{0};
  std::atomic<uint64_t> sendWriteCount_{0};
};

} // namespace libsoftmetaroce
