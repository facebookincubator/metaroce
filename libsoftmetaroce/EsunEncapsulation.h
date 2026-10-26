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
#include <libsoftmetaroce/EsunHeader.h>
#include <libsoftmetaroce/Icrc.h>
#include <libsoftmetaroce/RawEncapsulation.h>

namespace libsoftmetaroce {

class EsunEncapsulation : public RawEncapsulation {
 public:
  EsunEncapsulation(
      folly::MacAddress srcMac,
      folly::MacAddress dstMac,
      EsunHeader header = EsunHeader{},
      uint16_t ethertype = kProvisionalEsunEthertype) noexcept;

  size_t frameOutgoing(
      folly::IOBufQueue& bufferQueue,
      std::optional<uint32_t> originalIcrc) noexcept override;

  void setClassOfService(uint8_t cos) noexcept override {
    header_.setClassOfService(cos);
  }

  std::optional<IngressMetadata> deframeIncoming(
      std::unique_ptr<folly::IOBuf>& buffer) noexcept override;

  [[nodiscard]] size_t payloadSize(
      const folly::io::Cursor& cursor) const noexcept override;

  // Include mCRC because ESUN's per-hop TTL prevents an unchanging CRC from
  // protecting the frame end to end (section 4.1).
  [[nodiscard]] size_t framingOverheadBytes() const noexcept {
    return kEthernetHeaderSize + EsunHeader::kSizeBytes + kIcrcSize;
  }

  void setDstMac(folly::MacAddress dstMac) noexcept override {
    dstMac_ = dstMac;
  }

  [[nodiscard]] EsunHeader header() const noexcept {
    return header_;
  }

 private:
  folly::MacAddress dstMac_;
  folly::MacAddress srcMac_;
  uint16_t ethertype_;
  EsunHeader header_;
};

} // namespace libsoftmetaroce
