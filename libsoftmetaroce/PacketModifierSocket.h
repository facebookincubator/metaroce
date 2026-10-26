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

#include <memory>
#include <mutex>

#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/io/async/EventBase.h>
#include <gflags/gflags.h>
#include <libsoftmetaroce/Encapsulation.h>
#include <libsoftmetaroce/Icrc.h>
#include <libsoftmetaroce/IngressPacket.h>
#include <libsoftmetaroce/PacketSocket.h>
#include <libsoftmetaroce/RawEncapsulation.h>
#include <libsoftmetaroce/codec/Codec.h>
#include <libsoftmetaroce/codec/Packet.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <atomic>
#include <functional>
#include <vector>

DECLARE_uint32(libsoftmetaroce_data_dscp);
DECLARE_uint32(libsoftmetaroce_ack_dscp);
DECLARE_uint32(libsoftmetaroce_esun_data_cos);
DECLARE_uint32(libsoftmetaroce_esun_ack_cos);
DECLARE_bool(libsoftmetaroce_profile_send_egress);
DECLARE_string(libsoftmetaroce_send);

namespace libsoftmetaroce {

class PacketModifierSocket : public PacketSocket::ReadCallback {
 public:
  class ReadCallback {
   public:
    virtual void onDataAvailable(
        std::unique_ptr<IngressPacket> ingressPacket) noexcept = 0;
    virtual ~ReadCallback() = default;
  };

  // Primary constructor: takes a PacketSocket directly.
  PacketModifierSocket(
      folly::EventBase* evb,
      std::unique_ptr<PacketSocket> packetSocket,
      std::unique_ptr<Encapsulation> encapsulation,
      std::unique_ptr<Codec> codec,
      std::unique_ptr<Modifier> modifier = nullptr);

  // Backwards-compatible constructor: wraps AsyncUDPSocket in UdpPacketSocket.
  explicit PacketModifierSocket(
      folly::EventBase* evb,
      const std::function<std::pair<
          std::unique_ptr<folly::AsyncUDPSocket>,
          std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
      std::unique_ptr<Encapsulation> encapsulation = nullptr);

  void resumeRead(ReadCallback* rcb) {
    readCallback_ = CHECK_NOTNULL(rcb);
    packetSocket_->resumeRead(this);
  }

  void pauseRead() {
    packetSocket_->pauseRead();
  }

  void bind(const folly::SocketAddress& address) {
    packetSocket_->bind(address);
  }

  ssize_t write(
      const folly::SocketAddress& remoteAddr,
      std::unique_ptr<Packet> packet);

  // Accumulate framed PDUs between begin/flush, then emit them as one writeGSO
  // (plain write for one PDU). No-modifier path only; the caller must confirm
  // the socket supports GSO first, as folly requires before any writeGSO.
  // noexcept: flushGsoBatch reports a failed or throwing send via its return
  // value and errno, so the send loop never unwinds and strands a QP.
  void beginGsoBatch() noexcept;
  ssize_t flushGsoBatch() noexcept;
  // Discard an in-progress batch without sending it, so a caller whose
  // accumulate loop threw before flushGsoBatch does not leak a partial batch
  // into the next one. A no-op after a normal flushGsoBatch.
  void abortGsoBatch() noexcept;
  // Accumulate framed PDUs between begin/flush, then emit them as one sendmmsg
  // (folly writem). No-modifier path only; returns the writem result (datagrams
  // accepted, or -1). A partial send drops the unsent tail (the RTO recovers
  // it); noexcept, so a throwing writem is reported as a failed send.
  void beginWritemBatch() noexcept;
  int flushWritemBatch() noexcept;
  // Discard an in-progress writem batch without sending it, so a caller whose
  // accumulate loop throws mid-append does not leak a partial batch into the
  // next one. A no-op after a normal flush.
  void abortWritemBatch() noexcept;
  // >= 0 if GSO is available on the underlying socket; a modifier or a prior
  // GSO send failure (disableGso) disables it. The modifier_ read needs no
  // lock: it is set at construction and cleared only in close(), which runs
  // after the send loops that call this are drained.
  int getGSO() noexcept {
    return (modifier_ || gsoUnsupported_.load(std::memory_order_relaxed))
        ? -1
        : packetSocket_->getGSO();
  }
  // Latch GSO off on this socket after a GSO write fails (some NICs lack the
  // checksum offload GSO needs and only fail at send time). Returns true once,
  // on the enabled -> disabled transition.
  bool disableGso() noexcept {
    return !gsoUnsupported_.exchange(true, std::memory_order_relaxed);
  }

  // Batched writem (sendmmsg) is available without a modifier, on a socket kind
  // that implements it.
  bool canBatchWritem() const noexcept {
    return !modifier_ && packetSocket_->supportsWritem();
  }

  // Whether a packet read here may be delivered from a timer thread instead of
  // the one that read it, so the reader can stop relying on being alone.
  [[nodiscard]] bool mayDeferReceive() const noexcept {
    return modifier_ && modifier_->mayDeferReceive();
  }

  void close() {
    // Destroy modifier first to cancel any pending timer callbacks
    // before closing the socket.
    modifier_.reset();
    packetSocket_->close();
  }

  void setAdditionalCmsgsFunc(
      folly::Function<folly::Optional<folly::SocketCmsgMap>()>&& a) {
    packetSocket_->setAdditionalCmsgsFunc(std::move(a));
  }

  void setTosOrTrafficClass(uint8_t tosOrTclass) {
    unmodifiedTosOrTrafficClass_ = tosOrTclass;
    packetSocket_->setTosOrTrafficClass(tosOrTclass);
  }
  void setEsunCos(uint8_t cos) noexcept {
    encapsulation_->setClassOfService(cos);
  }
  void setRecvTos(bool recvTos) {
    packetSocket_->setRecvTos(recvTos);
  }

  void setRcvBuf(int bytes) {
    packetSocket_->setRcvBuf(bytes);
  }
  int getRcvBuf() {
    return packetSocket_->getRcvBuf();
  }
  bool supportsSndBuf() {
    return packetSocket_->supportsSndBuf();
  }
  void setSndBuf(int bytes) {
    packetSocket_->setSndBuf(bytes);
  }
  int getSndBuf() {
    return packetSocket_->getSndBuf();
  }
  // Raise SO_SNDBUF toward FLAGS_libsoftmetaroce_socket_sndbuf; call before
  // bind(), since SO_SNDBUF only takes effect on an unbound socket.
  void sizeSndBufBeforeBind();
  void setMaxReadsPerEvent(uint16_t maxReads) {
    packetSocket_->setMaxReadsPerEvent(maxReads);
  }
  void setReusePort(bool reusePort) {
    packetSocket_->setReusePort(reusePort);
  }
  // Gives this socket a write mutex for parallel send: AsyncUDPSocket is not
  // thread-safe, so consumers sharing this socket serialize on it.
  // lockForWrite() is a no-op until enabled.
  void enableWriteMutex() {
    writeMutex_ = std::make_unique<std::mutex>();
  }
  // A lock held on this socket's write mutex, or an empty (no-op) lock when the
  // mutex is not enabled (single sender). Held around the cmsg-set + write.
  [[nodiscard]] std::unique_lock<std::mutex> lockForWrite() {
    return writeMutex_ ? std::unique_lock<std::mutex>(*writeMutex_)
                       : std::unique_lock<std::mutex>();
  }
  void checkRcvBuf();
  void checkSndBuf();

  // Test-only: returns the underlying AsyncUDPSocket so tests can
  // dynamic_cast to MockUDPSocket. Returns nullptr for non-UDP transports.
  [[nodiscard]] folly::AsyncUDPSocket* getSocket();

  void setRxZeroChksum6(bool bVal) {
    packetSocket_->setRxZeroChksum6(bVal);
  }

  std::unique_ptr<Packet> parsePacketFromBuffer(
      const std::unique_ptr<folly::IOBuf>& buffer);

 private:
  // PacketSocket::ReadCallback
  void onPacketData(
      std::unique_ptr<folly::IOBuf> data,
      const folly::SocketAddress& source,
      uint8_t tos) noexcept override;

  void initSendAndReceiveFns();

  void resetGsoBatch() noexcept;
  void resetWritemBatch() noexcept;

  ReadCallback* readCallback_{nullptr};
  folly::EventBase* evb_;
  // If a modifier alters the traffic class, remember the intended value.
  uint8_t unmodifiedTosOrTrafficClass_{0};
  std::unique_ptr<Encapsulation> encapsulation_;
  std::unique_ptr<Codec> codec_;
  RawEncapsulation* rawEncap_{nullptr};
  std::unique_ptr<PacketSocket> packetSocket_;
  std::unique_ptr<folly::IOBuf> recvBuffer_;
  // Non-null only in parallel send; serializes this socket's cmsg-set + write.
  std::unique_ptr<std::mutex> writeMutex_;
  // Serializes the send lambda, which a modifier's timer thread also runs.
  // Before modifier_, whose teardown joins that thread.
  std::mutex modifierSendMutex_;
  // Declare the callbacks before modifier_ so modifier teardown stops deferred
  // callbacks before destroying the functions they reference.
  SendPacketFn sendPacketFn_;
  ReceivePacketFn receivePacketFn_;
  std::unique_ptr<Modifier> modifier_;

  // Which per-turn batch accumulation is active, if any. At most one at a time:
  // the Sender begins a single mode per turn. Touched on the sending thread
  // only.
  enum class BatchSendMode { None, Gso, Writem };
  BatchSendMode batchSendMode_{BatchSendMode::None};

  // GSO accumulation state, active only while batchSendMode_ ==
  // BatchSendMode::Gso.
  folly::IOBufQueue gsoQueue_{folly::IOBufQueue::cacheChainLength()};
  folly::SocketAddress gsoDest_;
  size_t gsoSegSize_{0};
  size_t gsoSegCount_{0};
  // Latched on the first GSO send failure. Atomic: read outside the per-socket
  // write lock by consumers that may share this socket.
  std::atomic<bool> gsoUnsupported_{false};

  // sendmmsg accumulation state, active only while batchSendMode_ ==
  // BatchSendMode::Writem.
  std::vector<std::unique_ptr<folly::IOBuf>> writemBufs_;
  std::vector<folly::SocketAddress> writemAddrs_;
};

} // namespace libsoftmetaroce
