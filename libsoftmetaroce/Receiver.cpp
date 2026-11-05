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

#include <algorithm>
#include <atomic>
#include <exception>
#include <fstream>
#include <mutex>

#include <folly/MapUtil.h>
#include <folly/system/ThreadName.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/EgressProfile.h>
#include <libsoftmetaroce/PacketSocketFactory.h>
#include <libsoftmetaroce/PingController.h>
#include <libsoftmetaroce/Receiver.h>

DECLARE_uint32(libsoftmetaroce_socket_rcvbuf);

DEFINE_uint32(
    libsoftmetaroce_recv_threads,
    1,
    "Number of receive threads. Each binds one SO_REUSEPORT socket per local "
    "GID on the shared MetaRoCE port, and the kernel load-balances a GID's "
    "datagrams across the threads by 4-tuple so recvmsg/CRC/parse run in "
    "parallel; 1 (default) is a single receive thread. Each QP's cross-flow "
    "receive state is guarded by a per-QP lock.");

DEFINE_uint32(
    libsoftmetaroce_socket_max_reads_per_event,
    0,
    "Datagrams to drain from a receive socket per event-loop wakeup. 0 means "
    "drain until EAGAIN. folly's AsyncUDPSocket defaults to 1, which caps "
    "receive throughput at the event-loop iteration rate (one epoll round-trip "
    "per packet) regardless of available CPU.");
// AsyncUDPSocket::setMaxReadsPerEvent takes a uint16_t, so reject values that
// would silently wrap when narrowed (e.g. 65536 -> 0 = "drain until EAGAIN")
// rather than apply a surprising cap.
static bool validateMaxReadsPerEvent(const char* /*name*/, uint32_t value) {
  return value <= 0xFFFFu;
}
DEFINE_validator(
    libsoftmetaroce_socket_max_reads_per_event,
    &validateMaxReadsPerEvent);

namespace libsoftmetaroce {

namespace {
std::atomic<uint32_t> nextReaderId{0};

// net.core.rmem_default: the SO_RCVBUF a fresh socket gets. 0 if unreadable, so
// the caller falls back to always raising the buffer.
int systemDefaultRcvBuf() {
  static const int bytes = [] {
    std::ifstream f("/proc/sys/net/core/rmem_default");
    int b = 0;
    return (f >> b) ? b : 0;
  }();
  return bytes;
}

void writeOnSocket(
    PacketModifierSocket& socket,
    const folly::SocketAddress& dest,
    std::unique_ptr<Packet> packet) {
  try {
    auto ret = socket.write(dest, std::move(packet));
    if (ret > 0) {
      VLOG(1) << "Sent reply packet to: " << dest;
    } else {
      LOG(WARNING) << "Failed to send reply packet to " << dest << ": "
                   << folly::errnoStr(errno);
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "Exception sending reply packet to " << dest << ": "
               << e.what();
  }
}

std::string_view socketKindName(SocketKind kind) {
  switch (kind) {
    case SocketKind::UDP:
      return "UDP";
    case SocketKind::RAW:
      return "RAW";
  }
  return "unknown"; // unreachable; satisfies -Wreturn-type
}
} // namespace

Receiver::Receiver(
    std::vector<folly::IPAddress> localAddrs,
    folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>&
        qpNumToQp,
    const std::function<std::pair<
        std::unique_ptr<folly::AsyncUDPSocket>,
        std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
    const uint16_t metaRoceBindToPort,
    const uint16_t metaRoceSendToPort,
    PingController& pingController,
    const ibv_gid_entry& gidEntry,
    const std::atomic<uint16_t>* activePortSet)
    : qpNumToQp_(qpNumToQp),
      pingController_(pingController),
      activePortSet_(activePortSet),
      localAddrs_{std::move(localAddrs)},
      metaRoceSendToPort_{metaRoceSendToPort} {
  const uint32_t numReaders =
      std::max<uint32_t>(1, FLAGS_libsoftmetaroce_recv_threads);
  multiReader_ = numReaders > 1;
  for (uint32_t i = 0; i < numReaders; ++i) {
    readerThreads_.emplace_back();
  }
  try {
    for (auto& reader : readerThreads_) {
      auto name = "lsmrRecv" +
          std::to_string(nextReaderId.fetch_add(1, std::memory_order_relaxed));
      reader.thread = std::thread([r = &reader, name = std::move(name)]() {
        folly::setThreadName(name);
        r->evb.loopForever();
      });
    }
  } catch (...) {
    // A throw mid-startup (e.g. thread-resource exhaustion) would unwind the
    // constructor with already-started joinable threads, whose destructors call
    // std::terminate. Stop and join the ones started so far before propagating.
    for (auto& reader : readerThreads_) {
      if (reader.thread.joinable()) {
        reader.evb.terminateLoopSoon();
        reader.thread.join();
      }
    }
    throw;
  }

  std::exception_ptr ex;
  for (auto& reader : readerThreads_) {
    reader.evb.runInEventBaseThreadAndWait([&, socketMaker]() {
      try {
        for (const auto& localAddr : localAddrs_) {
          createSocketForAddress(
              reader, localAddr, socketMaker, metaRoceBindToPort, gidEntry);
        }
      } catch (...) {
        ex = std::current_exception();
      }
    });
    if (ex) {
      break;
    }
  }
  if (ex) {
    for (auto& reader : readerThreads_) {
      reader.evb.runInEventBaseThreadAndWait([&]() {
        for (auto& ownedSocket : reader.sockets) {
          PacketModifierSocket& socket = *CHECK_NOTNULL(ownedSocket.get());
          socket.pauseRead();
          socket.close();
        }
        reader.evb.terminateLoopSoon();
      });
    }
    for (auto& reader : readerThreads_) {
      if (reader.thread.joinable()) {
        reader.thread.join();
      }
    }
    std::rethrow_exception(ex);
  }
}

Receiver::Receiver(
    folly::IPAddress localAddr,
    folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>&
        qpNumToQp,
    const std::function<std::pair<
        std::unique_ptr<folly::AsyncUDPSocket>,
        std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
    const uint16_t metaRoceBindToPort,
    const uint16_t metaRoceSendToPort,
    PingController& pingController,
    const ibv_gid_entry& gidEntry,
    const std::atomic<uint16_t>* activePortSet)
    : Receiver(
          std::vector<folly::IPAddress>{localAddr},
          qpNumToQp,
          socketMaker,
          metaRoceBindToPort,
          metaRoceSendToPort,
          pingController,
          gidEntry,
          activePortSet) {}

Receiver::~Receiver() {
  for (auto& reader : readerThreads_) {
    reader.evb.runInEventBaseThreadAndWait([&]() {
      for (auto& ownedSocket : reader.sockets) {
        PacketModifierSocket& socket = *CHECK_NOTNULL(ownedSocket.get());
        socket.pauseRead();
        socket.close();
      }
      reader.evb.terminateLoopSoon();
    });
  }
  for (auto& reader : readerThreads_) {
    if (reader.thread.joinable()) {
      reader.thread.join();
    }
  }
}

void Receiver::createSocketForAddress(
    ReaderThread& reader,
    folly::IPAddress localAddr,
    const std::function<std::pair<
        std::unique_ptr<folly::AsyncUDPSocket>,
        std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
    const uint16_t metaRoceBindToPort,
    const ibv_gid_entry& entry) {
  auto ownedSocket = makePacketModifierSocket(&reader.evb, entry, socketMaker);
  auto& socket = *CHECK_NOTNULL(ownedSocket.get());
  // Read ToS from MSG_ERRQUEUE for received packets. This must come before
  // bind(). This allows reading ECN marks; we do not write on this socket.
  socket.setRecvTos(true);
  // Bind several reader sockets to the same port so the kernel load-balances
  // datagrams across them by 4-tuple. Must precede bind().
  if (multiReader_) {
    socket.setReusePort(true);
  }
  // Raise SO_RCVBUF to the target; leaves it alone if the default is known to
  // be larger.
  const uint32_t desiredRcvBuf = FLAGS_libsoftmetaroce_socket_rcvbuf;
  if (desiredRcvBuf > 0 &&
      systemDefaultRcvBuf() < 2 * static_cast<int64_t>(desiredRcvBuf)) {
    socket.setRcvBuf(static_cast<int>(desiredRcvBuf));
  }
  // Drain multiple datagrams per wakeup instead of folly's one-per-event
  // default, so the receive rate is not pinned to the event-loop iteration
  // rate while CPU sits idle.
  socket.setMaxReadsPerEvent(
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_socket_max_reads_per_event));
  try {
    socket.bind(folly::SocketAddress(localAddr, metaRoceBindToPort));
  } catch (...) {
    LOG(ERROR) << "Exception when binding receiver socket to " << localAddr
               << ":" << metaRoceBindToPort << ". "
               << "Maybe there is another MetaRoCE implementation running.";
    throw;
  }
  // socket buffer size is set during bind(), check actual size
  socket.checkRcvBuf();
  socket.setRxZeroChksum6(true);
  // Before resumeRead: the first packet may arrive on the next line, and its
  // dispatch reads this.
  if (socket.mayDeferReceive()) {
    modifierDefersReceive_ = true;
  }
  socket.resumeRead(this);
  LOG(INFO)
      << "Receiver listening on " << localAddr << " ("
      << socketKindName(
             defaultEgressRegistry().getProfile(entry.gid_type).socketKind)
      << ")";
  reader.sockets.push_back(std::move(ownedSocket));
}

void Receiver::addLocalAddress(
    folly::IPAddress localAddr,
    const std::function<std::pair<
        std::unique_ptr<folly::AsyncUDPSocket>,
        std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
    const uint16_t metaRoceBindToPort,
    const ibv_gid_entry& entry) {
  if (hasLocalAddress(localAddr)) {
    LOG(INFO) << "Receiver already listening on " << localAddr;
    return;
  }
  std::exception_ptr ex;
  size_t created = 0;
  for (auto& reader : readerThreads_) {
    reader.evb.runInEventBaseThreadAndWait([&, socketMaker]() {
      try {
        createSocketForAddress(
            reader, localAddr, socketMaker, metaRoceBindToPort, entry);
      } catch (...) {
        ex = std::current_exception();
      }
    });
    if (ex) {
      break;
    }
    ++created;
  }
  if (ex) {
    // Roll back the sockets already bound on earlier readers so a failed add
    // releases what it partially bound, keeping readers all-or-nothing and a
    // retry clean.
    for (size_t i = 0; i < created; ++i) {
      auto& reader = readerThreads_[i];
      reader.evb.runInEventBaseThreadAndWait([&reader]() {
        reader.sockets.back()->pauseRead();
        reader.sockets.back()->close();
        reader.sockets.pop_back();
      });
    }
    std::rethrow_exception(ex);
  }
  localAddrs_.push_back(localAddr);
}

bool Receiver::hasLocalAddress(const folly::IPAddress& addr) const {
  return std::find(localAddrs_.begin(), localAddrs_.end(), addr) !=
      localAddrs_.end();
}

void Receiver::drainInFlightReceives() {
  // Each receive callback runs to completion inline on its reader thread, so a
  // round-trip through every reader EventBase waits out any callback in flight.
  for (auto& reader : readerThreads_) {
    // Must run off the reader threads: runInEventBaseThreadAndWait would
    // otherwise block waiting on the very thread it needs to run the drain.
    CHECK(!reader.evb.isInEventBaseThread());
    reader.evb.runInEventBaseThreadAndWait([] {});
  }
}

void Receiver::onDataAvailable(
    std::unique_ptr<IngressPacket> ingressPacket) noexcept {
  processIngress(std::move(ingressPacket));
}

Qp* Receiver::findQp(uint32_t qpn) {
  auto locked = qpNumToQp_.rlock();
  auto it = locked->find(qpn);
  return it == locked->end() ? nullptr : it->second.get();
}

void Receiver::processIngress(
    std::unique_ptr<IngressPacket> ingressPacket) noexcept {
  IngressPacket& ingress = *CHECK_NOTNULL(ingressPacket.get());
  const Packet& packet = *CHECK_NOTNULL(ingress.packet.get());
  const auto& rxBaseHeader = packet.getBaseHeader();
  const auto& opcode = rxBaseHeader.getOpcode();

  if (!opcode.isMetaRoce()) {
    VLOG(1) << "Dropping packet with non-MetaRoCE BTH prefix: " << opcode;
    return;
  }

  if (rxBaseHeader.getVersion() != kMetaRoceProtocolVersion) {
    unsupportedVersionDiscards_.fetch_add(1, std::memory_order_relaxed);
    LOG_FIRST_N(WARNING, 10) << "discarding packet with version "
                             << static_cast<int>(rxBaseHeader.getVersion());
    return;
  }

  const auto qpn = rxBaseHeader.getDstQp();

  // check for control packets and handle them early, as we may want to treat
  // errors (e.g. in QP lookup) differently
  if (opcode.isControl()) {
    VLOG(1) << "Received CONTROL packet";
    const ControlPacket& controlPacket =
        *CHECK_NOTNULL(dynamic_cast<const ControlPacket*>(&packet));
    auto controlHeader = controlPacket.getControlHeader();
    switch (controlHeader.getType()) {
      case ControlPacketType::PING: {
        auto pingPacket = dynamic_cast<PingPacket*>(ingress.packet.release());
        DCHECK(pingPacket);
        VLOG(1) << "Received PING on qp: " << qpn;
        handlePing(
            std::unique_ptr<PingPacket>(pingPacket),
            ingress.sourceSocketAddress);
        break;
      }
      case ControlPacketType::PING_RESPONSE: {
        const PingPacket& pingPacket =
            *CHECK_NOTNULL(dynamic_cast<const PingPacket*>(&controlPacket));
        auto pingHeader = pingPacket.getPingHeader();
        VLOG(1) << "Received PING_RESPONSE with Ping ID: "
                << controlHeader.getPingId() << ", errorState: "
                << static_cast<int>(pingHeader.getErrorState());
        pingController_.handlePingResponse(
            controlHeader.getPingId(), pingHeader.getErrorState());
        break;
      }
      default:
        LOG(ERROR) << "Received unknown Control Packet with type : "
                   << static_cast<uint16_t>(controlHeader.getType());
        break;
    }
    return;
  }

  if (qpn & kQpTagMask) {
    LOG_FIRST_N(WARNING, 1)
        << "libsoftmetaroce will not assign qpn's larger than 24 bits, "
        << "reserving the highest bits to hold tag numbers. Receiving a "
        << "packet with destination qpn outside the valid range indicates "
        << "an error in the sender. qpn=0x" << std::hex << qpn;
  }

  Qp* maybeQp = findQp(qpn);
  if (maybeQp == nullptr) {
    VLOG(1) << "dst qp: " << qpn << " not found";
    replyWithHrnr(packet, ingress.sourceSocketAddress);
    return;
  }
  auto& qp = *maybeQp;

  // A normal ack defers its send (setPendingAck), so the lock spans an inline
  // socket write only on the rare error replies below (HRNR/NAK).
  std::unique_lock<std::mutex> recvGuard;
  if (multiReader_ || modifierDefersReceive_) {
    recvGuard = std::unique_lock<std::mutex>(qp.receiveLock());
  }

  const uint32_t srcQp = rxBaseHeader.getSrcQp();
  const uint32_t expectedRemoteQp = qp.getRemoteQpNum();
  if (srcQp != expectedRemoteQp) {
    srcQpMismatchDiscards_.fetch_add(1, std::memory_order_relaxed);
    LOG_FIRST_N(WARNING, 10)
        << "discarding packet on qp " << qpn << " with source qp " << srcQp
        << " not matching expected remote qp " << expectedRemoteQp;
    return;
  }

  if (opcode.isAnAck()) {
    const auto flowId = rxBaseHeader.getFlowId();
    if (flowId >= qp.getNumTxFlows()) {
      LOG(WARNING) << "ack for unknown flow id: " << flowId
                   << " on qp: " << qpn;
      return;
    }
    const AckPacket& ackPacket =
        *CHECK_NOTNULL(dynamic_cast<const AckPacket*>(&packet));
    auto rxAckHeader = ackPacket.getAckHeader();

    VLOG(2) << "Ack received for flow id:" << flowId
            << " cfsn: " << rxAckHeader.getCumulativeFsn()
            << " maxRcvdFsn: " << rxAckHeader.getMaxRcvdFsn();

    if (qp.usesSingleOwnerRxRing()) {
      // Hand the ACK to the sender (the TX-state owner) to apply at the QP's
      // arbiter visit. The receipt time is for the receipt-to-process
      // diagnostic, so it is on the steady clock; RTT is measured against the
      // echoed protocol timestamp when the ACK is applied. The receiver does
      // not touch TX state in this mode.
      qp.enqueueReceivedAck(flowId, rxAckHeader, steadyNowNs());
    } else {
      // Applied here, on transmit state the sender writes at its own visit;
      // the QP-wide lock is what keeps the two from doing so at once.
      auto inlineAckGuard = qp.lockForInlineAckApply();
      qp.updateRemoteActivePortSet(rxAckHeader.getActivePortSet());
      CHECK_NOTNULL(qp.getTxFlowById(flowId))->processAckPacket(rxAckHeader);
    }

  } else if (opcode.isData()) {
    RxFlow* rxFlow = qp.getRxFlowById(rxBaseHeader.getFlowId());
    if (!rxFlow) {
      if (qp.getNumRxFlows() == 0) {
        // we have not initialized flows, and thus are not yet in RTR state.
        LOG(INFO) << "qp: " << qpn
                  << " is not ready to receive yet, sending HRNR.";
        qp.setRnrCancelPending(true);
        replyWithHrnr(packet, ingress.sourceSocketAddress);
      } else {
        // we have some initialized flows, but this is not one of them.
        VLOG(1) << "flow id: " << rxBaseHeader.getFlowId()
                << " not found for qp: " << qpn;
        replyWithFlowIdNak(
            packet, ingress.sourceSocketAddress, (uint16_t)qp.getNumRxFlows());
      }
      return;
    }

    auto dataPacket = dynamic_cast<DataPacket const* const>(&packet);
    DCHECK(dataPacket);

    auto maybeAck = rxFlow->processDataPacket(dataPacket, ingress.tos);
    if (maybeAck) {
      maybeAck->setActivePortSet(loadActivePortSet());
      const AckStatus ackStatus = maybeAck->getAckStatus();
      if (ackStatus == AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR ||
          ackStatus == AckStatus::NAK_INVALID_REQUEST ||
          ackStatus == AckStatus::NAK_OPERATIONAL_ERROR) {
        // Validation failed and the tag it carries is now in error; send the
        // fatal NAK immediately rather than through the coalescing pending-ack
        // slot, which a later ack could overwrite.
        sendPreparedNak(
            packet, ingress.sourceSocketAddress, std::move(maybeAck.value()));
      } else {
        VLOG(2) << "Ack prepared for flow id: " << rxFlow->getFlowId()
                << " cfsn: " << maybeAck.value().getCumulativeFsn()
                << " maxRcvdFsn: " << maybeAck.value().getMaxRcvdFsn();
        rxFlow->setPendingAck(
            std::move(maybeAck.value()), ingress.sourceSocketAddress.getPort());
      }
    }
  } else if (opcode.isRnrCancel()) {
    const RnrCancelPacket& rnrCancel =
        *CHECK_NOTNULL(dynamic_cast<const RnrCancelPacket*>(&packet));
    auto rnrCancelHeader = rnrCancel.getRnrCancelHeader();
    if (!qp.containsTag(rnrCancelHeader.getTag())) {
      LOG(WARNING) << "rnr cancel for unknown tag: "
                   << (int)rnrCancelHeader.getTag() << " on qp: " << qpn;
      return;
    }
    if (qp.usesSingleOwnerRxRing()) {
      // The cancel stays ordered behind an HRNR that may still be on the ring.
      qp.enqueueReceivedRnrCancel(rnrCancelHeader);
    } else {
      qp.applyRnrCancel(
          rnrCancelHeader.getTag(), rnrCancelHeader.getPostedCsn(), getNowNs());
    }
  } else {
    LOG(ERROR) << "Received unsupported opcode: " << opcode << " on qp: " << qpn
               << ", sending NAK Invalid Request";
    replyWithInvalidRequestNak(packet, ingress.sourceSocketAddress);
  }
}

Receiver::DataPacketInfoForNak Receiver::extractPacketInfo(
    const Packet& packet) {
  const auto& rxBaseHeader = packet.getBaseHeader();
  if (rxBaseHeader.getOpcode().isData()) {
    DataPacket const& dataPacket =
        *CHECK_NOTNULL(dynamic_cast<DataPacket const* const>(&packet));
    auto dataHeader = dataPacket.getDataHeader();
    return {
        dataHeader.getTag(),
        dataHeader.getFsn(),
        dataHeader.getMsn(),
        dataHeader.getTimestamp()};
  }
  return {0, 0, 0, 0};
}

BaseHeader Receiver::createReplyBaseHeader(const BaseHeader& rxBaseHeader) {
  using OperationType = BaseHeader::Opcode::OperationType;
  return BaseHeader::BaseHeaderBuilder()
      .setOpcode(BaseHeader::Opcode(OperationType::SEL_ACK))
      .setVersion(kMetaRoceProtocolVersion)
      .setFlowId(rxBaseHeader.getFlowId())
      .setDstQp(rxBaseHeader.getSrcQp())
      .setSrcQp(rxBaseHeader.getDstQp())
      .build();
}

void Receiver::sendNakResponse(
    const Packet& packet,
    const folly::SocketAddress& sourceSocketAddress,
    AckStatus ackStatus,
    std::optional<uint8_t> rnrBackoffTimeExp,
    std::optional<uint16_t> flowCount) {
  const auto& rxBaseHeader = packet.getBaseHeader();

  if (ackStatus == AckStatus::HRNR && !rxBaseHeader.getOpcode().isData()) {
    VLOG(1) << "HRNR packet is not sent for non-data packet type: "
            << rxBaseHeader.getOpcode();
    return;
  }

  auto dataPacketInfo = extractPacketInfo(packet);
  AckHeader::AckHeaderBuilder ackHeaderBuilder =
      AckHeader::AckHeaderBuilder()
          .setTag(dataPacketInfo.tagNum)
          .setCumulativeFsn(dataPacketInfo.fsn)
          .setMaxRcvdFsn(dataPacketInfo.fsn)
          .setTimestamp(dataPacketInfo.timestamp)
          .setAckStatus(ackStatus)
          .setCumulRespDeliveredMsn(dataPacketInfo.msn)
          .setActivePortSet(loadActivePortSet())
          .setRateHints(/* reserved on nak */ 0);

  if (rnrBackoffTimeExp) {
    ackHeaderBuilder.setRnrBackoffTimeExp(*rnrBackoffTimeExp);
  }

  if (ackStatus == AckStatus::NAK_FLOW_ID_ERROR) {
    CHECK(flowCount.has_value());
    CHECK_NE(flowCount.value(), 0);
    ackHeaderBuilder.setRateHints(flowCount.value());
  }

  sendPreparedNak(
      packet, sourceSocketAddress, std::move(ackHeaderBuilder).build());
}

void Receiver::sendPreparedNak(
    const Packet& packet,
    const folly::SocketAddress& sourceSocketAddress,
    AckHeader ackHeader) {
  auto ackPacket = std::make_unique<AckPacket>(
      createReplyBaseHeader(packet.getBaseHeader()), std::move(ackHeader));
  folly::SocketAddress dest = sourceSocketAddress;
  dest.setPort(metaRoceSendToPort_);
  writeReplyPacket(dest, std::move(ackPacket));
}

void Receiver::writeReplyPacket(
    const folly::SocketAddress& dest,
    std::unique_ptr<Packet> packet) {
  // Runs on the reader EventBase thread that owns this packet; any of its
  // sockets can carry a UDP reply (they share only the SO_REUSEPORT port), so
  // sockets[0] is fine. Reached only on cold error replies.
  for (auto& reader : readerThreads_) {
    if (reader.evb.isInEventBaseThread()) {
      DCHECK(!reader.sockets.empty())
          << "a reader is created with a socket per local GID, never empty here";
      PacketModifierSocket& socket = *CHECK_NOTNULL(reader.sockets[0].get());
      writeOnSocket(socket, dest, std::move(packet));
      return;
    }
  }
  LOG(FATAL) << "writeReplyPacket must run on a reader EventBase thread";
}

void Receiver::replyWithFlowIdNak(
    const Packet& packet,
    const folly::SocketAddress& sourceSocketAddress,
    uint16_t flowCount) {
  sendNakResponse(
      packet,
      sourceSocketAddress,
      AckStatus::NAK_FLOW_ID_ERROR,
      std::nullopt,
      flowCount);
}

void Receiver::replyWithInvalidRequestNak(
    const Packet& packet,
    const folly::SocketAddress& sourceSocketAddress) {
  sendNakResponse(
      packet,
      sourceSocketAddress,
      AckStatus::NAK_INVALID_REQUEST,
      std::nullopt,
      std::nullopt);
}

void Receiver::replyWithHrnr(
    const Packet& packet,
    const folly::SocketAddress& sourceSocketAddress) {
  sendNakResponse(
      packet,
      sourceSocketAddress,
      AckStatus::HRNR,
      kHrnrBackoffTimeExp,
      std::nullopt);
}

void Receiver::handlePing(
    std::unique_ptr<PingPacket> ownedPingPacket,
    const folly::SocketAddress& echoDestinationSocketAddress) {
  PingPacket& pingPacket = *CHECK_NOTNULL(ownedPingPacket.get());
  const auto& baseHeader = pingPacket.getBaseHeader();
  const auto& pingHeader = pingPacket.getPingHeader();
  auto pingTag = pingHeader.getTag();
  auto pingFlowId = baseHeader.getFlowId();
  auto pingQpn = baseHeader.getDstQp();
  auto pingSupportedFlowCount = pingHeader.getSupportedFlowCount();

  auto errorState = PingErrorState::NO_ERROR;

  Qp* qp = nullptr;
  {
    auto lockedMap = qpNumToQp_.rlock();
    auto* qpPtr = folly::get_ptr(*lockedMap, pingQpn);
    if (qpPtr) {
      qp = qpPtr->get();
    }
  }
  // Guards the QP reads below against a concurrent data packet mutating the
  // same QP (see processIngress).
  std::unique_lock<std::mutex> recvGuard;
  if (qp != nullptr && (multiReader_ || modifierDefersReceive_)) {
    recvGuard = std::unique_lock<std::mutex>(qp->receiveLock());
  }
  if (!qp) {
    errorState = PingErrorState::QP_NOT_PRESENT_OR_UNKNOWN;
  } else if (!qp->containsTag(pingTag)) {
    errorState = PingErrorState::TAG_NOT_ASSOCIATED_WITH_QP;
  } else if (!qp->getRemoteAddrs().contains(
                 echoDestinationSocketAddress.getIPAddress())) {
    errorState = PingErrorState::SOURCE_ADDRESS_NOT_ASSOCIATED_WITH_QP;
  } else if (
      pingFlowId >= pingSupportedFlowCount ||
      pingFlowId >= qp->getNumRxFlows()) {
    errorState = PingErrorState::FLOW_ID_UNSUPPORTED_OR_TOO_LARGE;
  }

  // The response travels back the way the request came, so its queue pairs are
  // the request's the other way round, as an acknowledgment's are.
  auto& responseBaseHeader = pingPacket.getBaseHeader();
  const uint32_t requesterQpn = responseBaseHeader.getSrcQp();
  responseBaseHeader.setSrcQp(responseBaseHeader.getDstQp());
  responseBaseHeader.setDstQp(requesterQpn);

  pingPacket.getControlHeader().setType(
      static_cast<uint16_t>(ControlPacketType::PING_RESPONSE));
  pingPacket.getPingHeader().setErrorState(errorState);
  pingPacket.getPingHeader().setSupportedFlowCount(
      qp ? qp->getNumRxFlows() : 0);
  pingPacket.getPingHeader().setActivePortSet(loadActivePortSet());

  folly::SocketAddress echoDestinationSocketAddr = echoDestinationSocketAddress;
  echoDestinationSocketAddr.setPort(metaRoceSendToPort_);
  writeReplyPacket(echoDestinationSocketAddr, std::move(ownedPingPacket));
}

} // namespace libsoftmetaroce
