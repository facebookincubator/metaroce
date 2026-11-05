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

#include <libsoftmetaroce/codec/Codec.h>
#include <memory>

namespace libsoftmetaroce {

class MetaRoceCodec final : public Codec {
 public:
  explicit MetaRoceCodec(size_t minimumPacketSizeBytes = 0);

  [[nodiscard]] size_t getSerializedSize(const Packet& packet) const override;

  void serialize(const Packet& packet, folly::io::QueueAppender& appender)
      const override;

  [[nodiscard]] std::unique_ptr<Packet> parse(
      folly::io::Cursor& cursor,
      const Encapsulation& encapsulation) const override;

 private:
  [[nodiscard]] size_t bytesNeededToFillMinimumFrame(
      const Packet& packet) const;

  void serializeToMinimumFrame(
      const Packet& packet,
      size_t fillBytes,
      folly::io::QueueAppender& appender) const;

  const size_t minimumPacketSizeBytes_;
};

} // namespace libsoftmetaroce
