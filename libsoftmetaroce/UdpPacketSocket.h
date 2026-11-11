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

#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/PacketSocket.h>

#include <folly/io/async/AsyncUDPSocket.h>
#include <memory>

namespace libsoftmetaroce {

// PacketSocket implementation backed by AsyncUDPSocket.
class UdpPacketSocket : public PacketSocket,
                        private folly::AsyncUDPSocket::ReadCallback {
 public:
  explicit UdpPacketSocket(std::unique_ptr<folly::AsyncUDPSocket> socket);

  void resumeRead(PacketSocket::ReadCallback* cb) override;
  void pauseRead() override;
  ssize_t write(
      const folly::SocketAddress& dest,
      std::unique_ptr<folly::IOBuf> data) override;
  ssize_t writeGSO(
      const folly::SocketAddress& dest,
      std::unique_ptr<folly::IOBuf> data,
      int gsoSegmentSize) override;
  int getGSO() noexcept override;
  bool supportsWritem() const noexcept override {
    return true;
  }
  int writem(
      folly::Range<const folly::SocketAddress*> addrs,
      const std::unique_ptr<folly::IOBuf>* bufs,
      size_t count) override;
  void bind(const folly::SocketAddress& address) override;
  void close() override;
  int fd() const override;

  void setTosOrTrafficClass(uint8_t tos) override;
  void setRecvTos(bool recvTos) override;
  void setAdditionalCmsgsFunc(
      folly::Function<folly::Optional<folly::SocketCmsgMap>()>&& func) override;
  void setRxZeroChksum6(bool bVal) override;
  void setRcvBuf(int bytes) override;
  int getRcvBuf() override;
  bool supportsSndBuf() override {
    return true;
  }
  void setSndBuf(int bytes) override;
  int getSndBuf() override;
  void setMaxReadsPerEvent(uint16_t maxReads) override;
  void setReusePort(bool reusePort) override;
  void setDontFragment() override;

  // Testing accessor: returns the underlying AsyncUDPSocket.
  [[nodiscard]] folly::AsyncUDPSocket* getUdpSocket() {
    return socket_.get();
  }

 private:
  // AsyncUDPSocket::ReadCallback
  void getReadBuffer(void** buf, size_t* len) noexcept override;
  void onDataAvailable(
      const folly::SocketAddress& client,
      size_t len,
      bool truncated,
      OnDataAvailableParams params) noexcept override;
  void onReadError(const folly::AsyncSocketException& ex) noexcept override;
  void onReadClosed() noexcept override;

  std::unique_ptr<folly::AsyncUDPSocket> socket_;
  PacketSocket::ReadCallback* readCallback_{nullptr};
  std::unique_ptr<folly::IOBuf> readBuf_;
};

} // namespace libsoftmetaroce
