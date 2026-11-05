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
#include <folly/Utility.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/io/async/EventBase.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/Ecn.h>
#include <libsoftmetaroce/PacketModifierSocket.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/RxFlow.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/DataHeader.h>
#include <libsoftmetaroce/codec/PingHeader.h>
#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace libsoftmetaroce {

class PingController;

inline constexpr uint8_t kHrnrBackoffTimeExp = kDefaultRnrBackoffTimeExp;

class Receiver : public PacketModifierSocket::ReadCallback,
                 public folly::NonCopyableNonMovable {
 public:
  // Constructor for multiple local addresses (multi-GID support). With
  // libsoftmetaroce_recv_threads > 1, packets spread across reader threads and
  // a per-QP lock serializes each QP's receive state.
  explicit Receiver(
      std::vector<folly::IPAddress> localAddrs,
      folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>&
          qpNumToQp,
      const std::function<std::pair<
          std::unique_ptr<folly::AsyncUDPSocket>,
          std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
      const uint16_t metaRoceBindToPort,
      const uint16_t metaRoceSendToPort,
      PingController& pingController,
      const ibv_gid_entry& gidEntry = ibv_gid_entry{},
      const std::atomic<uint16_t>* activePortSet = nullptr);

  // Legacy constructor for single local address (backwards compatibility).
  explicit Receiver(
      folly::IPAddress localAddr,
      folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>&
          qpNumToQp,
      const std::function<std::pair<
          std::unique_ptr<folly::AsyncUDPSocket>,
          std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
      const uint16_t metaRoceBindToPort,
      const uint16_t metaRoceSendToPort,
      PingController& pingController,
      const ibv_gid_entry& gidEntry = ibv_gid_entry{},
      const std::atomic<uint16_t>* activePortSet = nullptr);

  ~Receiver() override;

  // Add a new local address to listen on. Must be called before the receiver
  // is fully started (i.e., during initialization).
  void addLocalAddress(
      folly::IPAddress localAddr,
      const std::function<std::pair<
          std::unique_ptr<folly::AsyncUDPSocket>,
          std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
      const uint16_t metaRoceBindToPort,
      const ibv_gid_entry& entry = ibv_gid_entry{});

  // Check if this receiver is listening on the given address.
  bool hasLocalAddress(const folly::IPAddress& addr) const;

  // Wait out any in-flight receive callback on every reader thread, so a caller
  // that removed a QP from the map can destroy it without racing a reader. Call
  // from off the reader threads.
  void drainInFlightReceives();

  [[nodiscard]] uint64_t getUnsupportedVersionDiscardCount() const {
    return unsupportedVersionDiscards_.load(std::memory_order_relaxed);
  }

  // Test-only: a reader's EventBase, so tests drive the receive path on the
  // thread that owns the reader sockets (matching production).
  folly::EventBase& readerEventBaseForTest(size_t index = 0) {
    return readerThreads_[index].evb;
  }

 private:
  // PacketModifierSocket::ReadCallback, on a reader thread; processes inline.
  void onDataAvailable(
      std::unique_ptr<IngressPacket> ingressPacket) noexcept override;

  // Null when there is no such queue pair. The result stays valid after the map
  // lock is released: teardown drains the readers before destroying a Qp.
  Qp* findQp(uint32_t qpn);

  // Per-packet receive processing (QP lookup, RxFlow, ACK/NAK), on a reader
  // thread. With multiple readers a per-QP lock serializes a QP's processing.
  void processIngress(std::unique_ptr<IngressPacket> ingressPacket) noexcept;

  // Send a NAK/HRNR/ping reply on the calling reader's own socket.
  void writeReplyPacket(
      const folly::SocketAddress& dest,
      std::unique_ptr<Packet> packet);

  struct DataPacketInfoForNak {
    uint16_t tagNum;
    uint16_t fsn;
    uint16_t msn;
    uint32_t timestamp;
  };

  DataPacketInfoForNak extractPacketInfo(const Packet& packet);

  BaseHeader createReplyBaseHeader(const BaseHeader& rxBaseHeader);

  void sendNakResponse(
      const Packet& packet,
      const folly::SocketAddress& sourceSocketAddress,
      AckStatus ackStatus,
      std::optional<uint8_t> rnrBackoffTimeExp = std::nullopt,
      std::optional<uint16_t> flowCount = std::nullopt);

  // Send a caller-prepared NAK/HRNR AckHeader immediately on the receive
  // socket.
  void sendPreparedNak(
      const Packet& packet,
      const folly::SocketAddress& sourceSocketAddress,
      AckHeader ackHeader);

  void replyWithFlowIdNak(
      const Packet& packet,
      const folly::SocketAddress& sourceSocketAddress,
      uint16_t flowCount);

  void replyWithInvalidRequestNak(
      const Packet& packet,
      const folly::SocketAddress& sourceSocketAddress);

  void replyWithHrnr(
      const Packet& packet,
      const folly::SocketAddress& sourceSocketAddress);

  // One receive thread: its own EventBase/thread and one SO_REUSEPORT socket
  // per local GID. libsoftmetaroce_recv_threads=N makes N of these, all binding
  // the same ports so the kernel load-balances a GID's reads across them.
  struct ReaderThread {
    folly::EventBase evb;
    std::thread thread;
    // One socket per local GID (address), all on this thread's EventBase.
    std::vector<std::unique_ptr<PacketModifierSocket>> sockets;
  };

  // Helper to create and bind a socket for a given local address on a reader.
  void createSocketForAddress(
      ReaderThread& reader,
      folly::IPAddress localAddr,
      const std::function<std::pair<
          std::unique_ptr<folly::AsyncUDPSocket>,
          std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
      const uint16_t metaRoceBindToPort,
      const ibv_gid_entry& entry);

  void handlePing(
      std::unique_ptr<PingPacket> pingPacket,
      const folly::SocketAddress& echoDestinationSocketAddress);

  // Injected dependencies.
  // Owned by MetaRoceVerbs.
  folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>&
      qpNumToQp_;
  // Required PingController for dispatching ping responses.
  PingController& pingController_;
  // Points to MetaRoceVerbs::activePortSet_ when multiport is active.
  const std::atomic<uint16_t>* activePortSet_{nullptr};

  // Configuration. localAddrs_ is appended to after construction, so not const.
  std::vector<folly::IPAddress> localAddrs_; // all local ipv6 addresses/gids
  const uint16_t metaRoceSendToPort_; // for naks we send without state.

  // Runtime state.

  // An EventBase cannot be moved and a deque never relocates; [0] always
  // exists, [1..] only when libsoftmetaroce_recv_threads > 1.
  std::deque<ReaderThread> readerThreads_;

  // Caches readerThreads_.size() > 1.
  bool multiReader_{false};

  // A modifier's timer thread costs the reader threads their exclusion just as
  // a second reader would. Set per reader; multiReader_ short-circuits it.
  bool modifierDefersReceive_{false};

  // Counters.
  std::atomic<uint64_t> unsupportedVersionDiscards_{0};

  [[nodiscard]] uint16_t loadActivePortSet() const {
    return activePortSet_ ? activePortSet_->load(std::memory_order_relaxed) : 0;
  }
};

} // namespace libsoftmetaroce
