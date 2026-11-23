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

#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/Icrc.h>
#include <libsoftmetaroce/ProtocolCompatibility.h>
#include <libsoftmetaroce/UdpEncapsulation.h>

namespace libsoftmetaroce {

TEST(UdpEncapsulationTest, DeframeRuntFrameDropsWithoutThrow) {
  UdpEncapsulation encap;
  // A buffer shorter than the trailing mCRC must drop.
  for (size_t len = 0; len < kIcrcSize; ++len) {
    auto buf = folly::IOBuf::copyBuffer(std::string(len, 'x'));
    EXPECT_FALSE(encap.deframeIncoming(buf))
        << len << " bytes cannot carry a 4-byte mCRC, so it must drop";
  }
  // A chained runt reads by its total length, not its head length.
  auto head = folly::IOBuf::copyBuffer("xy");
  head->appendToChain(folly::IOBuf::copyBuffer("z"));
  EXPECT_FALSE(encap.deframeIncoming(head))
      << "3 bytes across two buffers is still a runt";
}

TEST(UdpEncapsulationTest, DeframeChainedFrameWithShortHeadValidates) {
  UdpEncapsulation encap;
  folly::IOBufQueue queue;
  queue.append(folly::IOBuf::copyBuffer("MetaRoCE"));
  encap.frameOutgoing(queue, std::nullopt);
  auto framed = queue.move();
  framed->coalesce();

  // Re-chain as a 2-byte head plus the rest: a short head must not read as
  // a runt while the chain carries a complete frame.
  auto head = folly::IOBuf::copyBuffer(framed->data(), 2);
  auto tail = folly::IOBuf::copyBuffer(
      static_cast<const uint8_t*>(framed->data()) + 2, framed->length() - 2);
  head->appendToChain(std::move(tail));

  EXPECT_TRUE(encap.deframeIncoming(head).has_value())
      << "a chained frame with a short head still carries a valid mCRC";
}

TEST(UdpEncapsulationTest, FrameThenDeframeRoundTrips) {
  UdpEncapsulation encap;
  const std::string payload = "MetaRoCE";
  folly::IOBufQueue queue;
  queue.append(folly::IOBuf::copyBuffer(payload));

  size_t added = encap.frameOutgoing(queue, std::nullopt);

  EXPECT_EQ(added, kIcrcSize) << "frameOutgoing should append a 4-byte mCRC";
  auto buf = queue.move();
  buf->coalesce();
  EXPECT_EQ(buf->length(), payload.size() + kIcrcSize)
      << "framed buffer should be payload + mCRC";

  auto metadata = encap.deframeIncoming(buf);
  EXPECT_TRUE(metadata.has_value())
      << "a frame with a freshly computed mCRC must validate";
}

TEST(UdpEncapsulationTest, DeframeFrameWithCorruptMcrcDrops) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_validate_crc = true;
  UdpEncapsulation encap;
  folly::IOBufQueue queue;
  queue.append(folly::IOBuf::copyBuffer("MetaRoCE"));
  encap.frameOutgoing(queue, std::nullopt);
  auto framed = queue.move();
  framed->coalesce();
  framed->writableData()[framed->length() - 1] ^= 0x01;

  EXPECT_FALSE(encap.deframeIncoming(framed))
      << "the receiver must drop a frame whose mCRC does not match its "
         "contents rather than act on corrupted bytes";
}

TEST(UdpEncapsulationTest, PayloadSizeExcludesMcrc) {
  UdpEncapsulation encap;
  const std::string payload = "test_payload";
  folly::IOBufQueue queue;
  queue.append(folly::IOBuf::copyBuffer(payload));
  encap.frameOutgoing(queue, std::nullopt);

  auto buf = queue.move();
  buf->coalesce();
  folly::io::Cursor cursor(buf.get());
  EXPECT_EQ(encap.payloadSize(cursor), payload.size())
      << "payload size should exclude the trailing mCRC";
}

} // namespace libsoftmetaroce
