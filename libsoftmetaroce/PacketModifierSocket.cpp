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

#include "libsoftmetaroce/PacketModifierSocket.h"
#include <folly/ScopeGuard.h>
#include <folly/synchronization/RelaxedAtomic.h>
#include <gflags/gflags.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/UdpEncapsulation.h>
#include <libsoftmetaroce/UdpPacketSocket.h>
#include <libsoftmetaroce/codec/MetaRoceCodec.h>
#include <algorithm>
#include <cerrno>
#include <fstream>
#include <limits>

DEFINE_uint32(
    libsoftmetaroce_socket_rcvbuf,
    16 * 1024 * 1024,
    "Minimum SO_RCVBUF (bytes) for the UDP receive sockets: a socket is raised "
    "to this only if the host default is smaller, never lowered. The kernel "
    "doubles the value, so the 16MB default needs net.core.rmem_max >= 32MB. "
    "Sized to hold a large in-flight window when the sender delivers it in "
    "back-to-back bursts (faster than the readers drain) rather than paced, so "
    "the receive buffer is not overrun. At MTU 1024 a 16MB buffer holds ~10k "
    "datagrams (payload plus sk_buff truesize).");
// SO_RCVBUF/SO_SNDBUF are passed to the kernel as an int; reject a value that
// would wrap negative when narrowed.
static bool validateFitsInInt(const char* /*name*/, uint32_t value) {
  return value <= static_cast<uint32_t>(std::numeric_limits<int>::max());
}
DEFINE_validator(libsoftmetaroce_socket_rcvbuf, &validateFitsInInt);

DEFINE_uint32(
    libsoftmetaroce_socket_sndbuf,
    16 * 1024 * 1024,
    "Minimum SO_SNDBUF (bytes) for the UDP send sockets, applied before bind(). "
    "Headroom so a coalesced GSO/sendmmsg burst is buffered instead of hitting "
    "EAGAIN; a GSO write that EAGAINs drops its whole batch to the retransmit "
    "timer, which collapses throughput with large (jumbo-MTU) packets. The "
    "kernel doubles the value, so the 16MB default needs net.core.wmem_max >= "
    "32MB; 0 leaves the host default.");
DEFINE_validator(libsoftmetaroce_socket_sndbuf, &validateFitsInInt);

DEFINE_uint32(
    libsoftmetaroce_data_dscp,
    libsoftmetaroce::kDefaultDataDscp,
    "DSCP code point (0-63) for the per-CoS sender socket that carries data "
    "packets. ECN bits are set by LSMR and are not part of this value.");

DEFINE_uint32(
    libsoftmetaroce_ack_dscp,
    libsoftmetaroce::kDefaultAckDscp,
    "DSCP code point (0-63) for the per-CoS sender socket that carries ACK "
    "and other control (e.g. RNR-cancel) packets. ECN bits are set by LSMR "
    "and are not part of this value.");

DEFINE_uint32(
    libsoftmetaroce_esun_data_cos,
    libsoftmetaroce::kDefaultEsunDataCos,
    "ESUN CoS (0-7) for the sender socket that carries data packets. "
    "Independent of IP DSCP.");

DEFINE_uint32(
    libsoftmetaroce_esun_ack_cos,
    libsoftmetaroce::kDefaultEsunAckCos,
    "ESUN CoS (0-7) for the sender socket that carries ACK and other control "
    "(e.g. RNR-cancel) packets. Independent of IP DSCP.");

namespace {
// Reject values that don't fit in a 6-bit DSCP code point.
bool isValidDscp(const char* /*flagname*/, uint32_t value) {
  return value <= 63u;
}
bool isValidEsunCos(const char* /*flagname*/, uint32_t value) {
  return value <= 7u;
}
// Valid send egress modes for --libsoftmetaroce_send.
bool isValidSendMode(const char* /*flagname*/, const std::string& value) {
  return value == "single" || value == "gso" || value == "sendmmsg";
}
} // namespace

DEFINE_validator(libsoftmetaroce_data_dscp, &isValidDscp);
DEFINE_validator(libsoftmetaroce_ack_dscp, &isValidDscp);
DEFINE_validator(libsoftmetaroce_esun_data_cos, &isValidEsunCos);
DEFINE_validator(libsoftmetaroce_esun_ack_cos, &isValidEsunCos);

// Defined here (with the other Sender-path socket flags) rather than in
// MetaRoceVerbs so test binaries that link the Sender without MetaRoceVerbs
// still resolve it.
DEFINE_bool(
    libsoftmetaroce_profile_send_egress,
    false,
    "Time the per-socket write-lock wait vs the socket->write() (sendmsg) cost "
    "in the send path and log the per-packet breakdown at teardown, to "
    "attribute the send bottleneck (lock contention vs sendmsg).");

DEFINE_string(
    libsoftmetaroce_send,
    "single",
    "Send egress mode: 'single' = one sendmsg per packet; 'gso' = coalesce a "
    "consumer's per-turn run of equal-size data PDUs on one socket into a single "
    "UDP GSO write to amortize the per-packet sendmsg cost (requires NIC "
    "tx-checksum offload; falls back to per-packet on a write error); "
    "'sendmmsg' = coalesce a per-turn run into one sendmmsg/writem syscall (no "
    "checksum offload required, so it works on NICs that cannot GSO).");
DEFINE_validator(libsoftmetaroce_send, &isValidSendMode);

namespace {
// net.core.wmem_default: the SO_SNDBUF a fresh socket gets. 0 if unreadable, so
// the caller falls back to always raising the buffer.
int systemDefaultSndBuf() {
  static const int bytes = [] {
    std::ifstream f("/proc/sys/net/core/wmem_default");
    int b = 0;
    return (f >> b) ? b : 0;
  }();
  return bytes;
}
} // namespace

namespace libsoftmetaroce {
namespace {

constexpr uint8_t applyFramingEcn(
    uint8_t socketTos,
    std::optional<uint8_t> framingEcn) {
  return framingEcn
      ? static_cast<uint8_t>((socketTos & 0xfc) | (*framingEcn & 0x03))
      : socketTos;
}

} // namespace

PacketModifierSocket::PacketModifierSocket(
    folly::EventBase* evb,
    std::unique_ptr<PacketSocket> packetSocket,
    std::unique_ptr<Encapsulation> encapsulation,
    std::unique_ptr<Codec> codec,
    std::unique_ptr<Modifier> modifier)
    : evb_(evb),
      encapsulation_(std::move(encapsulation)),
      codec_(std::move(codec)),
      packetSocket_(std::move(packetSocket)) {
  modifier_ = std::move(modifier);
  initSendAndReceiveFns();
}

PacketModifierSocket::PacketModifierSocket(
    folly::EventBase* evb,
    const std::function<std::pair<
        std::unique_ptr<folly::AsyncUDPSocket>,
        std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker,
    std::unique_ptr<Encapsulation> encapsulation)
    : evb_(evb),
      encapsulation_(
          encapsulation ? std::move(encapsulation)
                        : std::make_unique<UdpEncapsulation>()),
      codec_(std::make_unique<MetaRoceCodec>()) {
  auto [socket, modifier] = socketMaker(evb_);
  packetSocket_ = std::make_unique<UdpPacketSocket>(std::move(socket));
  modifier_ = std::move(modifier);
  initSendAndReceiveFns();
}

void PacketModifierSocket::initSendAndReceiveFns() {
  rawEncap_ = dynamic_cast<RawEncapsulation*>(encapsulation_.get());

  sendPacketFn_ = [this](
                      const folly::SocketAddress& address,
                      std::unique_ptr<Packet> packet,
                      std::optional<uint32_t> originalIcrc = std::nullopt) {
    // A modifier's timer thread runs this too. Held for the whole body: the
    // framing and the ECN bracket are shared state too.
    std::unique_lock<std::mutex> modifierSendLock;
    if (modifier_) {
      modifierSendLock = std::unique_lock<std::mutex>(modifierSendMutex_);
    }
    const auto serializedSize = codec_->getSerializedSize(*packet);
    auto packetBuf = folly::IOBuf::create(serializedSize);
    folly::IOBufQueue bufferQueue{folly::IOBufQueue::cacheChainLength()};
    bufferQueue.append(std::move(packetBuf));
    folly::io::QueueAppender appender(&bufferQueue, /* growth */ 0);
    codec_->serialize(*packet, appender);
    DCHECK_EQ(bufferQueue.chainLength(), serializedSize)
        << "codec getSerializedSize must match serialized length";

    // For raw encapsulations (Ethernet), set the destination MAC from
    // the target address before framing so the Ethernet header carries
    // the correct destination for this specific packet.
    if (rawEncap_) {
      rawEncap_->setDstMac(
          rawEncap_->resolveDstMac(address.getIPAddress().asV6()));
    }

    // Add framing (mCRC for UDP, Ethernet header for direct L2, etc.)
    size_t framingBytes =
        encapsulation_->frameOutgoing(bufferQueue, originalIcrc);

    if (batchSendMode_ == BatchSendMode::Gso) {
      // Batching runs only without a modifier, and the per-packet ECN value
      // below is modifier-only, so it never applies on this accumulate path.
      auto framed = bufferQueue.move();
      const size_t framedSize = framed->computeChainDataLength();
      if (gsoSegCount_ == 0) {
        gsoDest_ = address;
        gsoSegSize_ = framedSize;
      } else {
        // A GSO batch is one run to one destination at a uniform size; callers
        // accumulate only such a run, so every later PDU matches the first.
        DCHECK_EQ(framedSize, gsoSegSize_);
        DCHECK_EQ(address, gsoDest_);
      }
      gsoQueue_.append(std::move(framed));
      ++gsoSegCount_;
      // During a batch this returns the framed bytes accepted: the send happens
      // later in flushGsoBatch(), whose return is the real result.
      return static_cast<ssize_t>(framedSize);
    }

    if (batchSendMode_ == BatchSendMode::Writem) {
      // Accumulate the framed PDU + its dest; flushWritemBatch() emits them as
      // one sendmmsg. Per-packet TOS/ECN below is modifier-only, skipped here.
      auto framed = bufferQueue.move();
      const auto framedSize =
          static_cast<ssize_t>(framed->computeChainDataLength());
      writemBufs_.push_back(std::move(framed));
      // Keep writemBufs_/writemAddrs_ in lockstep: if the address push throws,
      // drop the buffer just added so a torn append cannot mis-pair a batch.
      SCOPE_FAIL {
        writemBufs_.pop_back();
      };
      writemAddrs_.push_back(address);
      return framedSize;
    }

    // Check if this packet has ECN marking and set ECN marking if requested
    std::optional<uint8_t> ecnValue = packet->getEcnValue();
    bool tosWasSet = false;
    if (ecnValue.has_value()) {
      VLOG(1) << fmt::format(
          "PacketModifierSocket: Detected ECN-marked packet with value 0x{:02X}",
          ecnValue.value());
      // Preserve DSCP bits (upper 6) while setting ECN bits (lower 2)
      uint8_t tosValue =
          (unmodifiedTosOrTrafficClass_ & 0xFC) | (ecnValue.value() & 0x03);
      try {
        packetSocket_->setTosOrTrafficClass(tosValue);
        tosWasSet = true;
      } catch (const std::exception& e) {
        LOG(WARNING) << fmt::format(
            "PacketModifierSocket: Failed to set ECN marking: {}", e.what());
      }
    }

    const auto bytesWritten = packetSocket_->write(address, bufferQueue.move());

    // Reset TOS to the original value after sending the packet
    if (tosWasSet) {
      try {
        packetSocket_->setTosOrTrafficClass(unmodifiedTosOrTrafficClass_);
      } catch (const std::exception& e) {
        LOG(WARNING) << fmt::format(
            "PacketModifierSocket: Failed to reset TOS after packet send: {}",
            e.what());
      }
    }
    if (bytesWritten == -1) {
      LOG(ERROR) << fmt::format(
          "Error writing {} bytes to {} on socket: {}",
          serializedSize,
          address.describe(),
          folly::errnoStr(errno));
    } else if (
        bytesWritten != static_cast<ssize_t>(serializedSize + framingBytes)) {
      LOG(ERROR) << fmt::format(
          "Partial write to socket. Wrote {} bytes, expected {} bytes.",
          bytesWritten,
          serializedSize + framingBytes);
    }
    return bytesWritten;
  }; // sendPktFn
  receivePacketFn_ = [this](std::unique_ptr<IngressPacket> packet) {
    readCallback_->onDataAvailable(std::move(packet));
  };
}

void PacketModifierSocket::onPacketData(
    std::unique_ptr<folly::IOBuf> data,
    const folly::SocketAddress& source,
    uint8_t tos) noexcept {
  recvBuffer_ = std::move(data);

  const auto ingressMetadata = encapsulation_->deframeIncoming(recvBuffer_);
  if (!ingressMetadata) {
    return;
  }

  std::unique_ptr<Packet> packet;
  try {
    packet = parsePacketFromBuffer(recvBuffer_);
    if (!packet) {
      return;
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << fmt::format(
        "Failed to parse packet from buffer (length={}): {}",
        recvBuffer_->length(),
        e.what());
    return;
  }
  const uint8_t ingressTos = applyFramingEcn(tos, ingressMetadata->ecn);
  auto ingressPacket =
      std::make_unique<IngressPacket>(std::move(packet), source, ingressTos);
  if (modifier_) {
    modifier_->modifyRxPacketIfMatches(
        receivePacketFn_, std::move(ingressPacket));
  } else {
    receivePacketFn_(std::move(ingressPacket));
  }
}

ssize_t PacketModifierSocket::write(
    const folly::SocketAddress& remoteAddr,
    std::unique_ptr<Packet> packet) {
  if (modifier_) {
    return modifier_->modifyTxPacketIfMatches(
        sendPacketFn_,
        std::make_unique<Modifier::TxPacketWithMetadata>(
            remoteAddr, std::move(packet)));
  }
  return sendPacketFn_(remoteAddr, std::move(packet), std::nullopt);
}

void PacketModifierSocket::beginGsoBatch() noexcept {
  // Batching only on the plain path: a modifier may delay or reorder sends,
  // which a single batched syscall cannot express.
  if (modifier_) {
    return;
  }
  // A prior batch must have been flushed (gsoQueue_ drained); an unflushed one
  // would leak its PDUs into this batch, and a still-active writem batch would
  // be silently orphaned.
  DCHECK(batchSendMode_ == BatchSendMode::None);
  DCHECK_EQ(gsoSegCount_, 0u);
  DCHECK(gsoQueue_.empty());
  batchSendMode_ = BatchSendMode::Gso;
}

ssize_t PacketModifierSocket::flushGsoBatch() noexcept {
  if (gsoSegCount_ == 0) {
    resetGsoBatch();
    return 0;
  }
  auto buf = gsoQueue_.move();
  const size_t count = gsoSegCount_;
  const int segSize = static_cast<int>(gsoSegSize_);
  const size_t batchBytes = buf->computeChainDataLength();
  resetGsoBatch();
  // A throw here must not escape and strand the serviced QP, so report it as a
  // failed write, which the caller already handles like any returned error.
  ssize_t ret = 0;
  try {
    ret = count == 1
        ? packetSocket_->write(gsoDest_, std::move(buf))
        : packetSocket_->writeGSO(gsoDest_, std::move(buf), segSize);
  } catch (const std::exception& ex) {
    LOG(WARNING) << "GSO batch flush threw, treating as write failure: "
                 << ex.what();
    errno = EIO;
    return -1;
  }
  // Snapshot errno right after the syscall and restore it before returning: the
  // logging below can overwrite the thread-local, and the caller classifies
  // EAGAIN off errno after this returns.
  const int savedErrno = errno;
  if (ret <= 0) {
    // EAGAIN is normal send-buffer back-pressure; the caller parks the QP and
    // retries, so do not warn on it. writeGSO can return <= 0 without setting
    // errno, so report ret as the primary signal and errno only as a hint.
    LOG_IF(WARNING, savedErrno != EAGAIN)
        << "GSO batch flush failed (ret=" << ret
        << ", errno=" << folly::errnoStr(savedErrno) << ", segments=" << count
        << ", segSize=" << segSize << ")";
  } else if (static_cast<size_t>(ret) < batchBytes) {
    // A short write drops the tail of the batch; the unacked FSNs recover via
    // the retransmit timer, but surface it as the per-packet path does.
    LOG(WARNING) << "GSO batch partial write: " << ret << " of " << batchBytes
                 << " bytes (segments=" << count << ", segSize=" << segSize
                 << ")";
  }
  errno = savedErrno;
  return ret;
}

void PacketModifierSocket::abortGsoBatch() noexcept {
  // Drop the accumulated batch unsent; the next beginGsoBatch starts clean.
  resetGsoBatch();
}

void PacketModifierSocket::resetGsoBatch() noexcept {
  batchSendMode_ = BatchSendMode::None;
  gsoSegSize_ = 0;
  gsoSegCount_ = 0;
  // Frames remain queued only on the abort path (a batch accumulated but never
  // flushed); drop them so the stale batch cannot leak into the next one.
  gsoQueue_.move();
}

void PacketModifierSocket::beginWritemBatch() noexcept {
  // Batching only on the plain path: a modifier may delay or reorder sends,
  // which a single batched syscall cannot express.
  if (modifier_) {
    return;
  }
  // A prior batch must have been flushed; an unflushed one would leak its PDUs
  // into this batch, and a still-active GSO batch would be silently orphaned.
  DCHECK(batchSendMode_ == BatchSendMode::None);
  DCHECK(writemBufs_.empty());
  DCHECK(writemAddrs_.empty());
  batchSendMode_ = BatchSendMode::Writem;
}

int PacketModifierSocket::flushWritemBatch() noexcept {
  DCHECK(batchSendMode_ != BatchSendMode::Gso)
      << "the writem flush resets the shared batchSendMode_, which would "
         "orphan an accumulating GSO batch";
  if (writemBufs_.empty()) {
    resetWritemBatch();
    return 0;
  }
  // Publish the syscall's errno to the caller last: releasing the buffers in
  // clearBatch can itself set errno (free() may). Guards run in reverse
  // construction order, so this one runs after clearBatch.
  int reportedErrno = 0;
  auto restoreErrno =
      folly::makeGuard([&reportedErrno] { errno = reportedErrno; });
  // Clear the accumulators even if writem throws, so a later beginWritemBatch
  // does not trip its empty DCHECK on stale state.
  auto clearBatch = folly::makeGuard([this] { resetWritemBatch(); });
  // writemBufs_ and writemAddrs_ are appended in lockstep while accumulating;
  // if they ever desync (e.g. a throwing push_back mid-append) clamp to the
  // shorter so writem never reads past the address range.
  DCHECK_EQ(writemBufs_.size(), writemAddrs_.size());
  const size_t datagramsToSend =
      std::min(writemBufs_.size(), writemAddrs_.size());
  int ret = 0;
  try {
    ret = packetSocket_->writem(
        folly::Range<const folly::SocketAddress*>(
            writemAddrs_.data(), datagramsToSend),
        writemBufs_.data(),
        datagramsToSend);
  } catch (const std::exception& ex) {
    // A throw here must not escape and strand the serviced QP; report it as a
    // failed write, which the caller handles like any returned error.
    LOG(WARNING) << "writem batch flush threw, treating as write failure: "
                 << ex.what();
    reportedErrno = EIO;
    return -1;
  }
  reportedErrno = errno;
  // glog's EVERY_N counter isn't thread-safe.
  static folly::relaxed_atomic<uint64_t> failedFlushes{0};
  static folly::relaxed_atomic<uint64_t> partialSends{0};
  if (ret < 0) {
    // Rate-limit: a persistent error (e.g. ENOBUFS) would otherwise flood the
    // log at the send rate.
    LOG_IF(
        WARNING,
        reportedErrno != EAGAIN && failedFlushes.fetch_add(1) % 1000 == 0)
        << "writem batch flush failed (ret=" << ret
        << ", errno=" << folly::errnoStr(reportedErrno)
        << ", datagrams=" << datagramsToSend << ")";
  } else if (static_cast<size_t>(ret) < datagramsToSend) {
    LOG_IF(WARNING, partialSends.fetch_add(1) % 1000 == 0)
        << "writem batch partial send: " << ret << " of " << datagramsToSend
        << " datagrams (short sendmmsg; the unsent tail re-services)";
  }
  return ret;
}

void PacketModifierSocket::abortWritemBatch() noexcept {
  // Drop the accumulated batch unsent; the next beginWritemBatch starts clean.
  resetWritemBatch();
}

void PacketModifierSocket::resetWritemBatch() noexcept {
  batchSendMode_ = BatchSendMode::None;
  writemBufs_.clear();
  writemAddrs_.clear();
}

void PacketModifierSocket::checkRcvBuf() {
  if (FLAGS_libsoftmetaroce_socket_rcvbuf == 0) {
    return;
  }
  int recvBufferSize = getRcvBuf();
  VLOG(2) << "SO_RCVBUF size: " << recvBufferSize;
  // The kernel reports back 2x the value it set, so a fully-granted request
  // reads as 2x the target. Warn rather than crash when granted less: the
  // transport still runs with reduced burst headroom.
  if (static_cast<int64_t>(recvBufferSize) <
      2 * static_cast<int64_t>(FLAGS_libsoftmetaroce_socket_rcvbuf)) {
    // The shortfall is host-global (net.core.rmem_max), so every receiver
    // socket would log the same line; with multiple readers, warn once.
    LOG_FIRST_N(WARNING, 1)
        << "SO_RCVBUF granted " << recvBufferSize / 2 << " bytes "
        << "(kernel reports " << recvBufferSize << "), below the "
        << "requested " << FLAGS_libsoftmetaroce_socket_rcvbuf
        << " bytes; raise net.core.rmem_max to avoid UDP drops at "
           "large in-flight windows.";
  }
}

void PacketModifierSocket::sizeSndBufBeforeBind() {
  const uint32_t desired = FLAGS_libsoftmetaroce_socket_sndbuf;
  // Skip transports with no kernel send buffer (e.g. raw Ethernet); on those
  // getSndBuf() is not even callable before bind().
  if (desired == 0 || !supportsSndBuf()) {
    return;
  }
  // Raise SO_SNDBUF to the target only when the host default is smaller, never
  // lowering a host-tuned socket. The kernel reports back 2x what it set, hence
  // the doubled comparison.
  if (systemDefaultSndBuf() < 2 * static_cast<int64_t>(desired)) {
    setSndBuf(static_cast<int>(desired));
  }
}

void PacketModifierSocket::checkSndBuf() {
  if (FLAGS_libsoftmetaroce_socket_sndbuf == 0 || !supportsSndBuf()) {
    return;
  }
  const int sendBufferSize = getSndBuf();
  VLOG(2) << "SO_SNDBUF size: " << sendBufferSize;
  // The kernel reports back 2x the value it set, so a fully-granted request
  // reads as 2x the target. Warn rather than crash when granted less: the
  // transport still runs, but with less burst headroom.
  if (static_cast<int64_t>(sendBufferSize) <
      2 * static_cast<int64_t>(FLAGS_libsoftmetaroce_socket_sndbuf)) {
    // The shortfall is host-global (net.core.wmem_max), so every send socket
    // would log the same line; with several send sockets, warn once.
    LOG_FIRST_N(WARNING, 1)
        << "SO_SNDBUF granted " << sendBufferSize / 2 << " bytes "
        << "(kernel reports " << sendBufferSize << "), below the "
        << "requested " << FLAGS_libsoftmetaroce_socket_sndbuf
        << " bytes; raise net.core.wmem_max to avoid GSO EAGAIN batch drops "
           "at large MTU.";
  }
}

// Test-only: see PacketModifierSocket.h comment.
folly::AsyncUDPSocket* PacketModifierSocket::getSocket() {
  auto* udp = dynamic_cast<UdpPacketSocket*>(packetSocket_.get());
  return udp ? udp->getUdpSocket() : nullptr;
}

std::unique_ptr<Packet> PacketModifierSocket::parsePacketFromBuffer(
    const std::unique_ptr<folly::IOBuf>& buffer) {
  folly::io::Cursor cursor(buffer.get());
  return codec_->parse(cursor, *encapsulation_);
}

} // namespace libsoftmetaroce
