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
#include <libsoftmetaroce/codec/BitVector.h>
#include <libsoftmetaroce/codec/Header.h>
#include <cstdint>
#include <optional>

DECLARE_int32(libsoftmetaroce_min_rnr_backoff_us);

namespace libsoftmetaroce {

enum class AckStatus : uint8_t {
  ACK = 0b00000000,
  ACK_OUT_OF_ORDER = 0b00000001,
  HRNR = 0b00100000,
  BRNR = 0b01000000,
  NAK_FLOW_ID_ERROR = 0b01100000,
  NAK_INVALID_REQUEST = 0b01100001,
  NAK_ADDRESS_INVALID_ACCESS_ERROR = 0b01100010,
  NAK_OPERATIONAL_ERROR = 0b01100011,
  // one reserved enum value represents all reserved/unused codes.
  RESERVED = 0b01100100
};

AckStatus uint8_tToAckStatus(uint8_t ackStatus);
std::ostream& operator<<(std::ostream& os, const AckStatus& st);

class AckHeader : public Header {
 public:
  static constexpr size_t kMainAckHeaderSizeBytes = 20;
  static constexpr size_t kAckTrackerBits = 256;
  AckHeader() = default;
  explicit AckHeader(folly::io::Cursor& cursor);
  explicit AckHeader(
      uint8_t cnpCE,
      uint8_t tag,
      uint16_t cumulativeFsn,
      uint16_t maxRcvdFsn,
      uint8_t ackStatus,
      uint16_t cumulRespDeliveredMsn,
      uint32_t rateHints,
      uint32_t timestamp,
      BitVector<kAckTrackerBits> fsnBitVector,
      BitVector<kAckTrackerBits> rnrBitVector,
      uint16_t activePortSet = 0)
      : cnpCE_{cnpCE},
        tag_{tag},
        cumulativeFsn_{cumulativeFsn},
        maxRcvdFsn_{maxRcvdFsn},
        ackStatus_{ackStatus},
        cumulRespDeliveredMsn_{cumulRespDeliveredMsn},
        activePortSet_{activePortSet},
        rateHints_{rateHints},
        timestamp_{timestamp},
        fsnBitVector_{std::move(fsnBitVector)},
        rnrBitVector_{std::move(rnrBitVector)} {}

  bool operator==(const AckHeader& other) const {
    bool eq = (cnpCE_ == other.cnpCE_);
    eq &= (tag_ == other.tag_);
    eq &= (cumulativeFsn_ == other.cumulativeFsn_);
    eq &= (maxRcvdFsn_ == other.maxRcvdFsn_);
    eq &= (ackStatus_ == other.ackStatus_);
    eq &= (cumulRespDeliveredMsn_ == other.cumulRespDeliveredMsn_);
    eq &= (activePortSet_ == other.activePortSet_);
    eq &= (rateHints_ == other.rateHints_);
    eq &= (timestamp_ == other.timestamp_);
    if (getAckStatus() == AckStatus::ACK_OUT_OF_ORDER ||
        getAckStatus() == AckStatus::BRNR) {
      eq &= (fsnBitVector_ == other.fsnBitVector_);
    }
    if (getAckStatus() == AckStatus::BRNR) {
      eq &= (rnrBitVector_ == other.rnrBitVector_);
    }
    return eq;
  }

  [[nodiscard]] uint8_t getCnpCE() const {
    return cnpCE_;
  }
  [[nodiscard]] uint8_t getTag() const {
    return tag_;
  }
  [[nodiscard]] uint16_t getCumulativeFsn() const {
    return cumulativeFsn_;
  }
  [[nodiscard]] uint16_t getMaxRcvdFsn() const {
    return maxRcvdFsn_;
  }
  [[nodiscard]] AckStatus getAckStatus() const {
    return uint8_tToAckStatus(ackStatus_);
  }
  [[nodiscard]] uint8_t getRnrBackoffTimeExp() const {
    CHECK(isRnr()) << "RNR backoff time exp is only valid for RNRs";
    return ackStatus_ & 0b00011111;
  }
  [[nodiscard]] uint16_t getCumulRespDeliveredMsn() const {
    return cumulRespDeliveredMsn_;
  }
  [[nodiscard]] uint16_t getActivePortSet() const {
    return activePortSet_;
  }
  [[nodiscard]] uint32_t getRateHints() const {
    return rateHints_;
  }
  [[nodiscard]] uint32_t getTimestamp() const {
    return timestamp_;
  }
  [[nodiscard]] const BitVector<kAckTrackerBits>& getFsnBitVector() const {
    return fsnBitVector_;
  }
  [[nodiscard]] const BitVector<kAckTrackerBits>& getRnrBitVector() const {
    return rnrBitVector_;
  }

  bool isHrnr() const {
    return uint8_tToAckStatus(ackStatus_) == AckStatus::HRNR;
  }

  bool isBrnr() const {
    return uint8_tToAckStatus(ackStatus_) == AckStatus::BRNR;
  }

  bool isRnr() const {
    return isHrnr() || isBrnr();
  }

  // Every NAK but a BRNR identifies the frame it reports by its cumulative FSN.
  bool isFrameIdentifyingNak() const {
    const AckStatus status = uint8_tToAckStatus(ackStatus_);
    return status != AckStatus::ACK && status != AckStatus::ACK_OUT_OF_ORDER &&
        status != AckStatus::BRNR;
  }

  uint32_t computeRnrBackoffTimeUs() const;

  // Setters for packet modifiers
  void setCnpCE(uint8_t cnpCE) {
    cnpCE_ = cnpCE;
  }
  void setTag(uint8_t tag) {
    tag_ = tag;
  }
  void setCumulativeFsn(uint16_t cumulativeFsn) {
    cumulativeFsn_ = cumulativeFsn;
  }
  void setMaxRcvdFsn(uint16_t maxRcvdFsn) {
    maxRcvdFsn_ = maxRcvdFsn;
  }
  void setAckStatus(AckStatus ackStatus) {
    ackStatus_ = static_cast<uint8_t>(ackStatus);
  }
  void setRnrBackoffTimeExp(uint8_t rnrBackoffTimeExp) {
    CHECK(isRnr()) << "RNR backoff time exp is only valid for RNRs";
    ackStatus_ = (ackStatus_ & 0b11100000) | (rnrBackoffTimeExp & 0b00011111);
  }
  void setCumulRespDeliveredMsn(uint16_t cumulRespDeliveredMsn) {
    cumulRespDeliveredMsn_ = cumulRespDeliveredMsn;
  }
  void setActivePortSet(uint16_t activePortSet) {
    activePortSet_ = activePortSet;
  }
  void setRateHints(uint32_t rateHints) {
    rateHints_ = rateHints;
  }
  void setTimestamp(uint32_t timestamp) {
    timestamp_ = timestamp;
  }
  void setFsnBitVector(const BitVector<kAckTrackerBits>& fsnBitVector) {
    fsnBitVector_ = fsnBitVector;
  }
  void setRnrBitVector(const BitVector<kAckTrackerBits>& rnrBitVector) {
    rnrBitVector_ = rnrBitVector;
  }

  void write(folly::io::QueueAppender& appender) const override;

  class AckHeaderBuilder {
   public:
    AckHeaderBuilder&& setCnpCE(uint8_t cnpCE);
    AckHeaderBuilder&& setTag(uint8_t tag);
    AckHeaderBuilder&& setCumulativeFsn(uint16_t cumulativeFsn);
    AckHeaderBuilder&& setMaxRcvdFsn(uint16_t maxRcvdFsn);
    AckHeaderBuilder&& setAckStatus(AckStatus ackStatus);
    AckHeaderBuilder&& setRnrBackoffTimeExp(uint8_t rnrBackoffTimeExp);
    AckHeaderBuilder&& setCumulRespDeliveredMsn(uint16_t cumulRespDeliveredMsn);
    AckHeaderBuilder&& setActivePortSet(uint16_t activePortSet);
    AckHeaderBuilder&& setRateHints(uint32_t rateHints);
    AckHeaderBuilder&& setTimestamp(uint32_t timestamp);
    AckHeaderBuilder&& setFsnBitVector(
        const BitVector<kAckTrackerBits>& fsnBitVector);
    AckHeaderBuilder&& setRnrBitVector(
        const BitVector<kAckTrackerBits>& rnrBitVector);
    AckHeader build() &&;
    explicit AckHeaderBuilder() = default;

   private:
    std::optional<uint8_t> maybeCnpCE_;
    std::optional<uint8_t> maybeTag_;
    std::optional<uint16_t> maybeCumulativeFsn_;
    std::optional<uint16_t> maybeMaxRcvdFsn_;
    std::optional<AckStatus> maybeAckStatus_;
    std::optional<uint8_t> maybeRnrBackoffTimeExp_;
    std::optional<uint16_t> maybeCumulRespDeliveredMsn_;
    std::optional<uint16_t> maybeActivePortSet_;
    std::optional<uint32_t> maybeRateHints_;
    std::optional<uint32_t> maybeTimestamp_;
    std::optional<BitVector<kAckTrackerBits>> maybeFsnBitVector_;
    std::optional<BitVector<kAckTrackerBits>> maybeRnrBitVector_;
  };

 private:
  uint8_t reserved1_{0}; // 4 bits
  uint8_t cnpCE_{
      0}; // count of how many ECN marks received since last ACK, 4 bits
  uint8_t tag_{0};
  uint16_t cumulativeFsn_;
  uint16_t maxRcvdFsn_;
  uint8_t ackStatus_{0};
  uint8_t reserved2_{0};
  // Cumulative Responder Delivered MSN
  uint16_t cumulRespDeliveredMsn_;
  uint16_t activePortSet_{0};
  uint32_t rateHints_{kDefaultRateHintGbps};
  uint32_t timestamp_{0};
  BitVector<kAckTrackerBits> fsnBitVector_;
  BitVector<kAckTrackerBits> rnrBitVector_;
};

} // namespace libsoftmetaroce
