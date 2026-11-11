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

#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/portability/GMock.h>

namespace libsoftmetaroce {

class MockUDPSocket : public folly::AsyncUDPSocket {
 public:
  explicit MockUDPSocket(folly::EventBase* evb) : folly::AsyncUDPSocket(evb) {
    ON_CALL(*this, write(::testing::_, ::testing::_))
        .WillByDefault([](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
          return buf.get()->computeChainDataLength();
        });
    ON_CALL(*this, writeGSO(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault(
            [](const folly::SocketAddress&,
               const std::unique_ptr<folly::IOBuf>& buf,
               WriteOptions) { return buf.get()->computeChainDataLength(); });
    ON_CALL(*this, getGSO()).WillByDefault(::testing::Return(1));
    ON_CALL(*this, writem(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault([](folly::Range<const folly::SocketAddress*>,
                          const std::unique_ptr<folly::IOBuf>*,
                          size_t count) { return static_cast<int>(count); });
  }
  ~MockUDPSocket() override = default;
  MOCK_METHOD(void, bindImpl, (const folly::SocketAddress&));
  void bind(
      const folly::SocketAddress& address,
      BindOptions options = BindOptions()) override {
    // AsyncUDPSocket::init() creates the socket/fd_ and partially
    // configures it; it is the first step of AsyncUDPSocket::bind.
    // This allows the base setTosOrTrafficClass() method to execute
    // unmodified.
    init(address.getFamily(), options);
    bindImpl(address);
  }
  MOCK_METHOD(
      ssize_t,
      write,
      (const folly::SocketAddress&, const std::unique_ptr<folly::IOBuf>&));
  MOCK_METHOD(
      ssize_t,
      writeGSO,
      (const folly::SocketAddress&,
       const std::unique_ptr<folly::IOBuf>&,
       WriteOptions));
  MOCK_METHOD(int, getGSO, ());
  MOCK_METHOD(
      int,
      writem,
      (folly::Range<const folly::SocketAddress*>,
       const std::unique_ptr<folly::IOBuf>*,
       size_t));
  MOCK_METHOD(void, resumeRead, (ReadCallback * cob));
  MOCK_METHOD(void, setDFAndTurnOffPMTU, ());
  MOCK_METHOD(
      void,
      setAdditionalCmsgsFunc,
      (folly::Function<folly::Optional<folly::SocketCmsgMap>()>&&));
};

} // namespace libsoftmetaroce
