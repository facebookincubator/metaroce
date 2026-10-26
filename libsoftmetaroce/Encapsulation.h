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

#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <cstdint>
#include <memory>
#include <optional>

namespace libsoftmetaroce {

// Abstraction for packet framing above the socket layer. Different
// encapsulations (UDP with mCRC, direct Ethernet, OCP ESUN, PSP)
// implement this interface to add/validate their framing without
// coupling to the socket I/O type.
// ESUN: OCP Ethernet-based Scalable Unified Networking
// (https://www.opencompute.org/wiki/Networking/ESUN)
//
// Encapsulation handles packet framing only -- the wire-format envelope
// around the MetaRoCE PDU. Address handling (mapping between transport-
// level addresses and folly::SocketAddress) is intentionally outside
// this interface; see RawEncapsulation for raw-socket address resolution.
class Encapsulation {
 public:
  virtual ~Encapsulation() = default;

  // Set the outgoing framing's class of service.
  // Encapsulations without a CoS field ignore this setting.
  virtual void setClassOfService(uint8_t /*cos*/) noexcept {}

  // TX: add framing bytes to the serialized MetaRoCE packet.
  // originalIcrc, when present, overrides the computed integrity value
  // (used by the corrupt modifier for testing).
  // Returns the number of framing bytes added.
  virtual size_t frameOutgoing(
      folly::IOBufQueue& bufferQueue,
      std::optional<uint32_t> originalIcrc) noexcept = 0;

  // Metadata recovered while removing the ingress framing.
  struct IngressMetadata {
    std::optional<uint8_t> ecn;
  };

  // RX: validate incoming packet framing and prepare the buffer for
  // MetaRoCE parsing. May strip leading headers (e.g., Ethernet).
  // Returns the recovered metadata, or nullopt to drop the frame.
  [[nodiscard]] virtual std::optional<IngressMetadata> deframeIncoming(
      std::unique_ptr<folly::IOBuf>& buffer) noexcept = 0;

  // Compute the MetaRoCE payload size from a cursor positioned after the
  // last parsed header. The encapsulation knows how much of the remaining
  // buffer is payload vs. framing (e.g., a 4-byte mCRC for UDP).
  [[nodiscard]] virtual size_t payloadSize(
      const folly::io::Cursor& cursor) const noexcept = 0;
};

} // namespace libsoftmetaroce
