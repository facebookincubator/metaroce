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

#include <folly/Function.h>
#include <folly/Optional.h>
#include <folly/Range.h>
#include <folly/SocketAddress.h>
#include <folly/io/IOBuf.h>
#include <folly/io/SocketOptionMap.h>
#include <cerrno>
#include <memory>

namespace libsoftmetaroce {

// Transport-agnostic socket interface for packet I/O. Concrete
// implementations wrap a specific socket type (UDP, raw Ethernet, etc.)
// and normalize the read path into a common callback.
class PacketSocket {
 public:
  class ReadCallback {
   public:
    virtual void onPacketData(
        std::unique_ptr<folly::IOBuf> data,
        const folly::SocketAddress& source,
        uint8_t tos) noexcept = 0;
    virtual ~ReadCallback() = default;
  };

  virtual ~PacketSocket() = default;

  virtual void resumeRead(ReadCallback* cb) = 0;
  virtual void pauseRead() = 0;
  virtual ssize_t write(
      const folly::SocketAddress& dest,
      std::unique_ptr<folly::IOBuf> data) = 0;
  // Send a buffer of equal-size segments as one GSO datagram batch (the last
  // segment may be shorter). Returns -1 with errno set (ENOTSUP by default) if
  // GSO is unsupported. The default consumes and drops `data`, so call this
  // only on a socket known to support GSO. Default: unsupported.
  virtual ssize_t writeGSO(
      const folly::SocketAddress& /*dest*/,
      std::unique_ptr<folly::IOBuf> /*data*/,
      int /*gsoSegmentSize*/) {
    errno = ENOTSUP;
    return -1;
  }
  // >= 0 if GSO is available on this socket/platform, negative otherwise.
  virtual int getGSO() noexcept {
    return -1;
  }
  // Whether writem() below is implemented; a socket kind that cannot batch
  // says so here rather than by failing the first send.
  virtual bool supportsWritem() const noexcept {
    return false;
  }
  // Send `count` datagrams in one sendmmsg syscall: bufs[i] to addrs[i].
  // Returns the number of datagrams accepted by the kernel, or -1 on error.
  // Only callable when supportsWritem() is true; the default is unsupported.
  virtual int writem(
      folly::Range<const folly::SocketAddress*> /*addrs*/,
      const std::unique_ptr<folly::IOBuf>* /*bufs*/,
      size_t /*count*/) {
    errno = ENOSYS;
    return -1;
  }
  virtual void bind(const folly::SocketAddress& address) = 0;
  virtual void close() = 0;
  virtual int fd() const = 0;

  virtual void setTosOrTrafficClass(uint8_t) {}
  virtual void setRecvTos(bool) {}
  virtual void setAdditionalCmsgsFunc(
      folly::Function<folly::Optional<folly::SocketCmsgMap>()>&&) {}
  virtual void setRxZeroChksum6(bool) {}
  virtual void setRcvBuf(int bytes) = 0;
  virtual int getRcvBuf() = 0;
  // Whether this transport has a kernel send buffer to size (SO_SNDBUF). False
  // for transports that don't (e.g. raw Ethernet), so callers can skip sizing
  // without querying getSndBuf(), which may be uncallable before bind().
  virtual bool supportsSndBuf() {
    return false;
  }
  // Minimum SO_SNDBUF (bytes), applied before bind(). Default no-op for
  // transports with no kernel send buffer to size.
  virtual void setSndBuf(int /*bytes*/) {}
  // Current SO_SNDBUF (bytes). 0 for transports with no kernel send buffer.
  virtual int getSndBuf() {
    return 0;
  }
  // Datagrams to drain per event-loop wakeup; 0 means until EAGAIN.
  virtual void setMaxReadsPerEvent(uint16_t) {}

  // Enable SO_REUSEPORT before bind() so several sockets can share one
  // address/port and the kernel load-balances datagrams across them by 4-tuple.
  virtual void setReusePort(bool) {}

  // Disables fragmentation, where supported.
  virtual void setDontFragment() {}
};

} // namespace libsoftmetaroce
