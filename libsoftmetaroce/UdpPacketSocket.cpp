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

#include <libsoftmetaroce/UdpPacketSocket.h>

#include <folly/net/NetOps.h>

namespace libsoftmetaroce {

UdpPacketSocket::UdpPacketSocket(std::unique_ptr<folly::AsyncUDPSocket> socket)
    : socket_(std::move(socket)) {}

void UdpPacketSocket::resumeRead(PacketSocket::ReadCallback* cb) {
  readCallback_ = cb;
  socket_->resumeRead(this);
}

void UdpPacketSocket::pauseRead() {
  socket_->pauseRead();
}

ssize_t UdpPacketSocket::write(
    const folly::SocketAddress& dest,
    std::unique_ptr<folly::IOBuf> data) {
  return socket_->write(dest, data);
}

ssize_t UdpPacketSocket::writeGSO(
    const folly::SocketAddress& dest,
    std::unique_ptr<folly::IOBuf> data,
    int gsoSegmentSize) {
  const folly::AsyncUDPSocket::WriteOptions options(
      gsoSegmentSize, /*zerocopyVal=*/false);
  return socket_->writeGSO(dest, data, options);
}

int UdpPacketSocket::getGSO() noexcept {
  return socket_->getGSO();
}

int UdpPacketSocket::writem(
    folly::Range<const folly::SocketAddress*> addrs,
    const std::unique_ptr<folly::IOBuf>* bufs,
    size_t count) {
  return socket_->writem(addrs, bufs, count);
}

void UdpPacketSocket::bind(const folly::SocketAddress& address) {
  socket_->bind(address);
}

void UdpPacketSocket::close() {
  socket_->close();
}

int UdpPacketSocket::fd() const {
  return socket_->getNetworkSocket().toFd();
}

void UdpPacketSocket::setTosOrTrafficClass(uint8_t tos) {
  socket_->setTosOrTrafficClass(tos);
}

void UdpPacketSocket::setRecvTos(bool recvTos) {
  socket_->setRecvTos(recvTos);
}

void UdpPacketSocket::setAdditionalCmsgsFunc(
    folly::Function<folly::Optional<folly::SocketCmsgMap>()>&& func) {
  socket_->setAdditionalCmsgsFunc(std::move(func));
}

void UdpPacketSocket::setRxZeroChksum6(bool bVal) {
  socket_->setRxZeroChksum6(bVal);
}

void UdpPacketSocket::setRcvBuf(int bytes) {
  socket_->setRcvBuf(bytes);
}

void UdpPacketSocket::setSndBuf(int bytes) {
  socket_->setSndBuf(bytes);
}

void UdpPacketSocket::setMaxReadsPerEvent(uint16_t maxReads) {
  socket_->setMaxReadsPerEvent(maxReads);
}

void UdpPacketSocket::setReusePort(bool reusePort) {
  socket_->setReusePort(reusePort);
}

void UdpPacketSocket::setDontFragment() {
  socket_->setDFAndTurnOffPMTU();
}

int UdpPacketSocket::getRcvBuf() {
  int value;
  socklen_t optLen = sizeof(value);
  if (folly::netops::getsockopt(
          socket_->getNetworkSocket(),
          SOL_SOCKET,
          SO_RCVBUF,
          &value,
          &optLen) != 0) {
    throw folly::AsyncSocketException(
        folly::AsyncSocketException::NOT_OPEN,
        "failed to get SO_RCVBUF on the socket",
        errno);
  }
  return value;
}

int UdpPacketSocket::getSndBuf() {
  int value;
  socklen_t optLen = sizeof(value);
  if (folly::netops::getsockopt(
          socket_->getNetworkSocket(),
          SOL_SOCKET,
          SO_SNDBUF,
          &value,
          &optLen) != 0) {
    throw folly::AsyncSocketException(
        folly::AsyncSocketException::NOT_OPEN,
        "failed to get SO_SNDBUF on the socket",
        errno);
  }
  return value;
}

// AsyncUDPSocket calls getReadBuffer and onDataAvailable in strict
// alternation, so allocating a fresh IOBuf here avoids a copy in
// onDataAvailable (vs. the traditional reusable-array pattern).
void UdpPacketSocket::getReadBuffer(void** buf, size_t* len) noexcept {
  readBuf_ = folly::IOBuf::create(kMaxPacketSizeBytes);
  *buf = readBuf_->writableData();
  *len = readBuf_->capacity();
}

// can't modify the signature
// @infer-ignore PULSE_CONST_REFABLE
void UdpPacketSocket::onDataAvailable(
    const folly::SocketAddress& client,
    size_t len,
    bool truncated,
    OnDataAvailableParams params) noexcept {
  if (truncated) {
    LOG(ERROR) << "packet truncated: " << len << " expected "
               << readBuf_->capacity();
  }
  readBuf_->append(len);
  readCallback_->onPacketData(std::move(readBuf_), client, params.tos);
}

void UdpPacketSocket::onReadError(
    const folly::AsyncSocketException& ex) noexcept {
  LOG(ERROR) << "Socket read error: " << ex.what();
}

void UdpPacketSocket::onReadClosed() noexcept {
  socket_->pauseRead();
}

} // namespace libsoftmetaroce
