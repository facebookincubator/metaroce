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

#include <libsoftmetaroce/EthernetEncapsulation.h>

#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <glog/logging.h>
#include <array>

namespace libsoftmetaroce {
namespace {

size_t getBufferQueueLength(const folly::IOBufQueue& bufferQueue) {
  return bufferQueue.options().cacheChainLength
      ? bufferQueue.chainLength()
      : bufferQueue.front()->computeChainDataLength();
}

} // namespace

EthernetEncapsulation::EthernetEncapsulation(
    folly::MacAddress srcMac,
    folly::MacAddress dstMac,
    uint16_t ethertype) noexcept
    : srcMac_(srcMac), dstMac_(dstMac), ethertype_(ethertype) {}

struct EthernetHeader {
  uint8_t dst[folly::MacAddress::SIZE];
  uint8_t src[folly::MacAddress::SIZE];
  uint16_t ethertype;
} __attribute__((packed));
static_assert(sizeof(EthernetHeader) == kEthernetHeaderSize);

size_t EthernetEncapsulation::frameOutgoing(
    folly::IOBufQueue& bufferQueue,
    std::optional<uint32_t> /* originalIcrc */) noexcept {
  DCHECK(!bufferQueue.empty()) << "bufferQueue must hold the serialized packet";
  const size_t unpaddedFrameSize =
      kEthernetHeaderSize + getBufferQueueLength(bufferQueue);
  const size_t paddingSize = unpaddedFrameSize < kMinimumEthernetFrameSizeBytes
      ? kMinimumEthernetFrameSizeBytes - unpaddedFrameSize
      : 0;
  if (paddingSize > 0) {
    constexpr std::array<uint8_t, kMinimumEthernetFrameSizeBytes>
        kZeroPadding{};
    folly::io::QueueAppender paddingAppender{&bufferQueue, paddingSize};
    paddingAppender.push(kZeroPadding.data(), paddingSize);
  }

  EthernetHeader hdr{};
  std::copy_n(dstMac_.bytes(), folly::MacAddress::SIZE, hdr.dst);
  std::copy_n(srcMac_.bytes(), folly::MacAddress::SIZE, hdr.src);
  hdr.ethertype = htons(ethertype_);

  auto ethHdr = folly::IOBuf::copyBuffer(&hdr, sizeof(hdr));
  auto payload = bufferQueue.move();
  DCHECK(payload) << "bufferQueue must contain the serialized packet";
  ethHdr->appendToChain(std::move(payload));
  bufferQueue.append(std::move(ethHdr));
  return kEthernetHeaderSize + paddingSize;
}

std::optional<Encapsulation::IngressMetadata>
EthernetEncapsulation::deframeIncoming(
    std::unique_ptr<folly::IOBuf>& buffer) noexcept {
  if (buffer->computeChainDataLength() < kEthernetHeaderSize) {
    LOG_FIRST_N(ERROR, 5) << "received frame too short for Ethernet header: "
                          << buffer->computeChainDataLength();
    return std::nullopt;
  }
  buffer->coalesce();
  // The AF_PACKET socket filters by ethertype, so this check is likely
  // redundant -- but guards against misconfiguration or unexpected frames.
  const auto* data = buffer->data();
  const uint16_t ethertype = (data[12] << 8) | data[13];
  if (ethertype != ethertype_) {
    LOG_FIRST_N(WARNING, 5)
        << "unexpected ethertype 0x" << std::hex << ethertype << ", expected 0x"
        << ethertype_ << std::dec;
    return std::nullopt;
  }
  buffer->trimStart(kEthernetHeaderSize);
  return IngressMetadata{};
}

} // namespace libsoftmetaroce
