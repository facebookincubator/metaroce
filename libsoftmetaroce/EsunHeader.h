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
#include <glog/logging.h>
#include <array>
#include <cstddef>
#include <cstdint>

namespace libsoftmetaroce {

// OCP ESUN 1.0 Ethernet header layout:
// https://www.opencompute.org/wiki/Networking/ESUN
//
//   byte 0:    revision(2) flowLabelValid(1) classOfService(3) ecn(2)
//   bytes 1-2: flowLabel(16)
//   byte 3:    ttl(4) userDefined(2) reserved(2)
//
// Stores received wire bytes directly, preserving unknown bits. `Fields`
// provides named construction for locally generated headers.
class EsunHeader {
 public:
  static constexpr size_t kSizeBytes = 4;
  static constexpr uint8_t kReservedMask = 0x3;
  static constexpr uint8_t kDefaultEsunTtl = 7;

  struct Fields {
    uint8_t revision{0};
    bool flowLabelValid{false};
    uint8_t classOfService{0};
    uint8_t ecn{0};
    uint16_t flowLabel{0};
    uint8_t ttl{kDefaultEsunTtl};
    uint8_t userDefined{0};
  };

  EsunHeader() noexcept : EsunHeader(Fields{}) {}

  explicit EsunHeader(Fields fields) noexcept {
    CHECK_LE(fields.revision, 0x3);
    CHECK_LE(fields.classOfService, 0x7);
    CHECK_LE(fields.ecn, 0x3);
    CHECK_LE(fields.ttl, 0xf);
    CHECK_LE(fields.userDefined, 0x3);

    encoded_ = {
        static_cast<uint8_t>(
            fields.revision << 6 |
            static_cast<int>(fields.flowLabelValid) << 5 |
            fields.classOfService << 2 | fields.ecn),
        static_cast<uint8_t>(fields.flowLabel >> 8),
        static_cast<uint8_t>(fields.flowLabel & 0xff),
        static_cast<uint8_t>(fields.ttl << 4 | fields.userDefined << 2)};
  }

  // Reads the header at the cursor's current position. The caller must ensure
  // that enough bytes remain.
  explicit EsunHeader(folly::io::Cursor& cursor) {
    cursor.pull(encoded_.data(), encoded_.size());
  }

  void write(folly::io::QueueAppender& appender) const noexcept {
    appender.push(encoded_.data(), encoded_.size());
  }

  bool operator==(const EsunHeader& other) const noexcept {
    return encoded_[0] == other.encoded_[0] &&
        encoded_[1] == other.encoded_[1] && encoded_[2] == other.encoded_[2] &&
        (encoded_[3] & ~kReservedMask) == (other.encoded_[3] & ~kReservedMask);
  }

  [[nodiscard]] constexpr uint8_t revision() const noexcept {
    return (encoded_[0] >> 6) & 0x3;
  }
  [[nodiscard]] constexpr bool isFlowLabelValid() const noexcept {
    return ((encoded_[0] >> 5) & 0x1) != 0;
  }
  [[nodiscard]] constexpr uint8_t classOfService() const noexcept {
    return (encoded_[0] >> 2) & 0x7;
  }
  void setClassOfService(uint8_t cos) noexcept {
    CHECK_LE(cos, 0x7);
    encoded_[0] = static_cast<uint8_t>((encoded_[0] & ~0x1c) | (cos << 2));
  }
  [[nodiscard]] constexpr uint8_t ecn() const noexcept {
    return encoded_[0] & 0x3;
  }
  [[nodiscard]] constexpr uint16_t flowLabel() const noexcept {
    return static_cast<uint16_t>(encoded_[1] << 8 | encoded_[2]);
  }
  [[nodiscard]] constexpr uint8_t ttl() const noexcept {
    return (encoded_[3] >> 4) & 0xf;
  }
  [[nodiscard]] constexpr uint8_t userDefined() const noexcept {
    return (encoded_[3] >> 2) & 0x3;
  }
  [[nodiscard]] constexpr uint8_t reserved() const noexcept {
    return encoded_[3] & kReservedMask;
  }

 private:
  std::array<uint8_t, kSizeBytes> encoded_{};
};

static_assert(sizeof(EsunHeader) == EsunHeader::kSizeBytes);

} // namespace libsoftmetaroce
