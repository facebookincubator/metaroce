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

#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/codec/AckHeader.h>
#include <algorithm>
#include <array>
#include <bitset>

DEFINE_int32(
    libsoftmetaroce_min_rnr_backoff_us,
    0,
    "When receiving a BRNR, wait at least this many microseconds, in case receiver is optimistic");

namespace libsoftmetaroce {

namespace {
BitVector<AckHeader::kAckTrackerBits> readBitVectorZeroFillingAbsentBytes(
    folly::io::Cursor& cursor) {
  std::array<uint8_t, AckHeader::kAckTrackerBits / 8> bytes{};
  cursor.pull(bytes.data(), std::min(cursor.totalLength(), bytes.size()));
  BitVector<AckHeader::kAckTrackerBits>::BitVectorArray words{};
  for (size_t i = 0; i < words.size(); ++i) {
    const uint8_t* word = bytes.data() + i * sizeof(uint32_t);
    words[i] = (uint32_t{word[0]} << 24) | (uint32_t{word[1]} << 16) |
        (uint32_t{word[2]} << 8) | uint32_t{word[3]};
  }
  return BitVector<AckHeader::kAckTrackerBits>(words);
}
} // namespace

AckHeader::AckHeader(folly::io::Cursor& cursor) {
  uint8_t rsvd1CnpCE = cursor.read<uint8_t>();
  reserved1_ = rsvd1CnpCE >> 4;
  cnpCE_ = rsvd1CnpCE & 0x0F;
  tag_ = cursor.read<uint8_t>();
  cumulativeFsn_ = cursor.readBE<uint16_t>();
  maxRcvdFsn_ = cursor.readBE<uint16_t>();
  ackStatus_ = cursor.read<uint8_t>();
  reserved2_ = cursor.read<uint8_t>();
  cumulRespDeliveredMsn_ = cursor.readBE<uint16_t>();
  activePortSet_ = cursor.readBE<uint16_t>();
  rateHints_ = cursor.readBE<uint32_t>();
  timestamp_ = cursor.readBE<uint32_t>();

  AckStatus ackStatus = uint8_tToAckStatus(ackStatus_);
  const bool callsForFsnBitset =
      ackStatus == AckStatus::ACK_OUT_OF_ORDER || ackStatus == AckStatus::BRNR;
  if (!callsForFsnBitset) {
    return;
  }
  // Each bitset is 256 bits long; ensure we have 256 / 8 = 32  bytes
  constexpr auto bitsetLenInBytes = kAckTrackerBits / 8;
  // The RNR bitset is found behind a whole FSN bitset.
  if (ackStatus == AckStatus::BRNR && cursor.totalLength() < bitsetLenInBytes) {
    return;
  }
  fsnBitVector_ = readBitVectorZeroFillingAbsentBytes(cursor);
  if (ackStatus == AckStatus::BRNR) {
    rnrBitVector_ = readBitVectorZeroFillingAbsentBytes(cursor);
  }
}

std::ostream& operator<<(std::ostream& os, const AckStatus& st) {
  switch (st) {
    case AckStatus::ACK:
      os << "ACK";
      break;
    case AckStatus::ACK_OUT_OF_ORDER:
      os << "SACK";
      break;
    case AckStatus::HRNR:
      os << "HRNR";
      break;
    case AckStatus::BRNR:
      os << "BRNR";
      break;
    case AckStatus::NAK_FLOW_ID_ERROR:
      os << "Flow ID Error";
      break;
    case AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR:
      os << "Address Invalid Error";
      break;
    case AckStatus::NAK_OPERATIONAL_ERROR:
      os << "Operational Error";
      break;
    case AckStatus::NAK_INVALID_REQUEST:
      os << "NAK Invalid Request";
      break;
    default:
      os << "Reserved (" << static_cast<int>(st) << ")";
      break;
  }
  return os;
}

AckStatus uint8_tToAckStatus(uint8_t bits) {
  if ((bits & 0b11100000) == static_cast<uint8_t>(AckStatus::HRNR)) {
    return AckStatus::HRNR;
  } else if ((bits & 0b11100000) == static_cast<uint8_t>(AckStatus::BRNR)) {
    return AckStatus::BRNR;
  } else {
    switch (bits) {
      case static_cast<uint8_t>(AckStatus::ACK):
        return AckStatus::ACK;
      case static_cast<uint8_t>(AckStatus::ACK_OUT_OF_ORDER):
        return AckStatus::ACK_OUT_OF_ORDER;
      case static_cast<uint8_t>(AckStatus::NAK_FLOW_ID_ERROR):
        return AckStatus::NAK_FLOW_ID_ERROR;
      case static_cast<uint8_t>(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR):
        return AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR;
      case static_cast<uint8_t>(AckStatus::NAK_OPERATIONAL_ERROR):
        return AckStatus::NAK_OPERATIONAL_ERROR;
      case static_cast<uint8_t>(AckStatus::NAK_INVALID_REQUEST):
        return AckStatus::NAK_INVALID_REQUEST;
      default:
        LOG(WARNING) << "Reserved ack status: " << std::bitset<8>(bits);
        return AckStatus::RESERVED;
    }
  }
}

AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setCnpCE(
    uint8_t cnpCE) {
  maybeCnpCE_ = cnpCE;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setTag(uint8_t tag) {
  maybeTag_ = tag;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setCumulativeFsn(
    uint16_t cumulativeFsn) {
  maybeCumulativeFsn_ = cumulativeFsn;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setMaxRcvdFsn(
    uint16_t maxRcvdFsn) {
  maybeMaxRcvdFsn_ = maxRcvdFsn;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setAckStatus(
    AckStatus ackStatus) {
  maybeAckStatus_ = ackStatus;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setRnrBackoffTimeExp(
    uint8_t rnrBackoffTimeExp) {
  maybeRnrBackoffTimeExp_ = rnrBackoffTimeExp;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&&
AckHeader::AckHeaderBuilder::setCumulRespDeliveredMsn(
    uint16_t cumulRespDeliveredMsn) {
  maybeCumulRespDeliveredMsn_ = cumulRespDeliveredMsn;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setActivePortSet(
    uint16_t activePortSet) {
  maybeActivePortSet_ = activePortSet;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setRateHints(
    uint32_t rateHints) {
  maybeRateHints_ = rateHints;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setTimestamp(
    uint32_t timestamp) {
  maybeTimestamp_ = timestamp;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setFsnBitVector(
    const BitVector<kAckTrackerBits>& fsnBitVector) {
  maybeFsnBitVector_ = fsnBitVector;
  return std::move(*this);
}
AckHeader::AckHeaderBuilder&& AckHeader::AckHeaderBuilder::setRnrBitVector(
    const BitVector<kAckTrackerBits>& rnrBitVector) {
  maybeRnrBitVector_ = rnrBitVector;
  return std::move(*this);
}
AckHeader AckHeader::AckHeaderBuilder::build() && {
  CHECK(maybeCumulativeFsn_.has_value())
      << "AckHeader.cumulativeFsn must be set";
  CHECK(maybeMaxRcvdFsn_.has_value()) << "AckHeader.maxRcvdFsn must be set";
  CHECK(maybeAckStatus_.has_value()) << "AckHeader.ackStatus must be set";
  CHECK(maybeTimestamp_.has_value()) << "AckHeader.timestamp must be set";
  bool isRnr =
      (maybeAckStatus_.value() == AckStatus::HRNR ||
       maybeAckStatus_.value() == AckStatus::BRNR);
  if (isRnr) {
    CHECK(maybeRnrBackoffTimeExp_.has_value())
        << "AckHeader.rnrBackoffTimeExp must be set if ackStatus is HRNR or BRNR";
  } else {
    CHECK(!maybeRnrBackoffTimeExp_.has_value())
        << "AckHeader.rnrBackoffTimeExp must not be set if ackStatus is not HRNR or BRNR";
  }
  return AckHeader(
      maybeCnpCE_.value_or(0),
      maybeTag_.value_or(0),
      maybeCumulativeFsn_.value(),
      maybeMaxRcvdFsn_.value(),
      static_cast<uint8_t>(maybeAckStatus_.value()) |
          maybeRnrBackoffTimeExp_.value_or(0),
      maybeCumulRespDeliveredMsn_.value_or(0xFFFF),
      maybeRateHints_.value_or(kDefaultRateHintGbps),
      maybeTimestamp_.value_or(0),
      maybeFsnBitVector_.value_or(BitVector<kAckTrackerBits>()),
      maybeRnrBitVector_.value_or(BitVector<kAckTrackerBits>()),
      maybeActivePortSet_.value_or(0));
}

uint32_t AckHeader::computeRnrBackoffTimeUs() const {
  const uint8_t rnrBackoffTimeExp =
      std::min(getRnrBackoffTimeExp(), kMaxRnrBackoffTimeExp);
  if (rnrBackoffTimeExp == 0) {
    return FLAGS_libsoftmetaroce_min_rnr_backoff_us;
  } else {
    return std::max(
        FLAGS_libsoftmetaroce_min_rnr_backoff_us,
        static_cast<int>(kRnrBackoffBaseUs << rnrBackoffTimeExp));
  }
}

void AckHeader::write(folly::io::QueueAppender& appender) const {
  using BVAValueType = BitVector<kAckTrackerBits>::BitVectorArray::value_type;
  uint8_t rsvd1CnpCE = cnpCE_ & 0x0F; // upper 4 bits reserved
  appender.write<uint8_t>(rsvd1CnpCE);
  appender.write<uint8_t>(tag_);
  appender.writeBE<uint16_t>(cumulativeFsn_);
  appender.writeBE<uint16_t>(maxRcvdFsn_);
  appender.write<uint8_t>(ackStatus_);
  appender.write<uint8_t>(0); // reserved
  appender.writeBE<uint16_t>(cumulRespDeliveredMsn_);
  appender.writeBE<uint16_t>(activePortSet_);
  appender.writeBE<uint32_t>(rateHints_);
  appender.writeBE<uint32_t>(timestamp_);
  if (uint8_tToAckStatus(ackStatus_) == AckStatus::ACK_OUT_OF_ORDER ||
      uint8_tToAckStatus(ackStatus_) == AckStatus::BRNR) {
    // If out-of-order ACK or BRNR
    for (auto& fsnBits : fsnBitVector_.getArray()) {
      appender.writeBE<BVAValueType>(fsnBits);
    }
  }
  if (uint8_tToAckStatus(ackStatus_) == AckStatus::BRNR) {
    // BRNR-NAK: RNR bitset present
    for (auto& rnrBits : rnrBitVector_.getArray()) {
      appender.writeBE<BVAValueType>(rnrBits);
    }
  }
}

} // namespace libsoftmetaroce
