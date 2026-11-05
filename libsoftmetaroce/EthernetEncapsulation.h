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

#include <folly/IPAddress.h>
#include <folly/MacAddress.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/RawEncapsulation.h>
#include <memory>
#include <optional>

namespace libsoftmetaroce {

// Direct Ethernet encapsulation: L2 + MetaRoCE (no UDP, no mCRC).
// Per spec section 4.1, when carried directly by Ethernet where the
// FCS is unaltered end-to-end, the mCRC is omitted.
class EthernetEncapsulation : public RawEncapsulation {
 public:
  // The shortest MetaRoCE packet whose frame reaches the Ethernet minimum.
  static constexpr size_t kMinimumPacketSizeBytes =
      kMinimumEthernetFrameSizeBytes - kEthernetHeaderSize;

  EthernetEncapsulation(
      folly::MacAddress srcMac,
      folly::MacAddress dstMac,
      uint16_t ethertype = kDefaultMetaRoceEthertype) noexcept;

  size_t frameOutgoing(
      folly::IOBufQueue& bufferQueue,
      std::optional<uint32_t> originalIcrc) noexcept override;

  std::optional<IngressMetadata> deframeIncoming(
      std::unique_ptr<folly::IOBuf>& buffer) noexcept override;

  [[nodiscard]] size_t payloadSize(
      const folly::io::Cursor& cursor) const noexcept override {
    return cursor.totalLength();
  }

  // RawEncapsulation: update the destination MAC for per-packet addressing.
  void setDstMac(folly::MacAddress dstMac) noexcept override {
    dstMac_ = dstMac;
  }

 private:
  folly::MacAddress srcMac_;
  folly::MacAddress dstMac_;
  uint16_t ethertype_;
};

} // namespace libsoftmetaroce
