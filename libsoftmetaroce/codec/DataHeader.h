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

#include <libsoftmetaroce/codec/Header.h>
#include <cstdint>
#include <optional>

namespace libsoftmetaroce {

class DataHeader : public Header {
 public:
  static constexpr size_t kDataHeaderSizeBytes = 20;
  static constexpr size_t kFlagsOffsetBytes = 5;
  static constexpr uint8_t kPaddingMask = 0x0F;
  DataHeader() = default;
  explicit DataHeader(folly::io::Cursor& cursor);
  DataHeader(
      uint16_t fsn,
      uint16_t una,
      uint8_t tag,
      bool ackRequest,
      bool isRetransmit,
      bool isTimeout,
      uint16_t posn,
      uint16_t csn,
      uint16_t msn,
      uint32_t rateHints,
      uint32_t timestamp,
      uint8_t padding = 0)
      : fsn_{fsn},
        una_{una},
        tag_{tag},
        ackRequest_{ackRequest},
        isRetransmit_{isRetransmit},
        isTimeout_{isTimeout},
        padding_{static_cast<uint8_t>(padding & kPaddingMask)},
        posn_{posn},
        csn_{csn},
        msn_{msn},
        rateHints_{rateHints},
        timestamp_{timestamp} {}
  bool operator==(const DataHeader& other) const {
    bool eq = (fsn_ == other.fsn_);
    eq &= (una_ == other.una_);
    eq &= (tag_ == other.tag_);
    eq &= (ackRequest_ == other.ackRequest_);
    eq &= (isRetransmit_ == other.isRetransmit_);
    eq &= (isTimeout_ == other.isTimeout_);
    eq &= (padding_ == other.padding_);
    eq &= (posn_ == other.posn_);
    eq &= (csn_ == other.csn_);
    eq &= (msn_ == other.msn_);
    eq &= (rateHints_ == other.rateHints_);
    eq &= (timestamp_ == other.timestamp_);
    return eq;
  }

  [[nodiscard]] uint16_t getFsn() const {
    return fsn_;
  }
  [[nodiscard]] uint16_t getUna() const {
    return una_;
  }
  [[nodiscard]] uint8_t getTag() const {
    return tag_;
  }
  [[nodiscard]] bool getAckRequest() const {
    return ackRequest_;
  }
  [[nodiscard]] bool getIsRetransmit() const {
    return isRetransmit_;
  }
  [[nodiscard]] bool getIsTimeout() const {
    return isTimeout_;
  }
  [[nodiscard]] bool getReserved() const {
    return reserved_;
  }
  [[nodiscard]] uint8_t getPadding() const {
    return padding_;
  }
  [[nodiscard]] uint16_t getPosn() const {
    return posn_;
  }
  [[nodiscard]] uint16_t getCsn() const {
    return csn_;
  }
  [[nodiscard]] uint16_t getMsn() const {
    return msn_;
  }
  [[nodiscard]] uint32_t getRateHints() const {
    return rateHints_;
  }
  [[nodiscard]] uint32_t getTimestamp() const {
    return timestamp_;
  }

  // Setters for packet modifiers
  void setFsn(uint16_t fsn) {
    fsn_ = fsn;
  }
  void setUna(uint16_t una) {
    una_ = una;
  }
  void setTag(uint8_t tag) {
    tag_ = tag;
  }
  void setAckRequest(bool ackRequest) {
    ackRequest_ = ackRequest;
  }
  void setIsRetransmit(bool isRetransmit) {
    isRetransmit_ = isRetransmit;
  }
  void setIsTimeout(bool isTimeout) {
    isTimeout_ = isTimeout;
  }
  void setPosn(uint16_t posn) {
    posn_ = posn;
  }
  void setCsn(uint16_t csn) {
    csn_ = csn;
  }
  void setMsn(uint16_t msn) {
    msn_ = msn;
  }
  void setRateHints(uint32_t rateHints) {
    rateHints_ = rateHints;
  }
  void setTimestamp(uint32_t timestamp) {
    timestamp_ = timestamp;
  }

  // Unlike the fields above, the Padding count depends on the output
  // encapsulation's minimum frame.
  void setPadding(uint8_t padding);

  void write(folly::io::QueueAppender& appender) const override;

  class DataHeaderBuilder {
   public:
    DataHeaderBuilder&& setFsn(uint16_t fsn);
    DataHeaderBuilder&& setUna(uint16_t una);
    DataHeaderBuilder&& setTag(uint8_t tag);
    DataHeaderBuilder&& setAckRequest(bool ackRequest);
    DataHeaderBuilder&& setIsRetransmit(bool isRetransmit);
    DataHeaderBuilder&& setIsTimeout(bool isTimeout);
    DataHeaderBuilder&& setPadding(uint8_t padding);
    DataHeaderBuilder&& setPosn(uint16_t posn);
    DataHeaderBuilder&& setCsn(uint16_t csn);
    DataHeaderBuilder&& setMsn(uint16_t msn);
    DataHeaderBuilder&& setRateHints(uint32_t rateHints);
    DataHeaderBuilder&& setTimestamp(uint32_t timestamp);
    DataHeader build() &&;
    explicit DataHeaderBuilder() = default;

   private:
    std::optional<uint16_t> maybeFsn_;
    std::optional<uint16_t> maybeUna_;
    std::optional<uint8_t> maybeTag_;
    std::optional<bool> maybeAckRequest_;
    std::optional<bool> maybeIsRetransmit_;
    std::optional<bool> maybeIsTimeout_;
    std::optional<uint8_t> maybePadding_;
    std::optional<uint16_t> maybePosn_;
    std::optional<uint16_t> maybeCsn_;
    std::optional<uint16_t> maybeMsn_;
    std::optional<uint32_t> maybeRateHints_;
    std::optional<uint32_t> maybeTimestamp_;
  };

 private:
  uint16_t fsn_{0}; // Flow Sequence Number
  uint16_t una_{0}; // Flow Sequence Number Clear
  uint8_t tag_{0};
  bool ackRequest_{false};
  bool isRetransmit_{false};
  bool isTimeout_{false};
  bool reserved_{false};
  uint8_t padding_{0};
  uint16_t posn_{0}; // Packet Offset Sequence Number
  uint16_t csn_{0}; // Completion Sequence Number
  uint16_t msn_{0}; // Message Sequence Number
  uint32_t rateHints_{kDefaultRateHintGbps};
  uint32_t timestamp_{0};
};

} // namespace libsoftmetaroce
