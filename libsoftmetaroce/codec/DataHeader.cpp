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

#include <libsoftmetaroce/codec/DataHeader.h>

namespace libsoftmetaroce {

DataHeader::DataHeader(folly::io::Cursor& cursor) {
  fsn_ = cursor.readBE<uint16_t>();
  una_ = cursor.readBE<uint16_t>();
  tag_ = cursor.read<uint8_t>();
  uint8_t flags = cursor.read<uint8_t>();
  ackRequest_ = (flags >> 7) & 0x1;
  isRetransmit_ = (flags >> 6) & 0x1;
  isTimeout_ = (flags >> 5) & 0x1;
  reserved_ = (flags >> 4) & 0x1;
  padding_ = flags & kPaddingMask;
  posn_ = cursor.readBE<uint16_t>();
  csn_ = cursor.readBE<uint16_t>();
  msn_ = cursor.readBE<uint16_t>();
  rateHints_ = cursor.readBE<uint32_t>();
  timestamp_ = cursor.readBE<uint32_t>();
}

void DataHeader::setPadding(uint8_t padding) {
  CHECK_LE(padding, kPaddingMask)
      << "the Padding field is four bits wide on the wire";
  padding_ = padding;
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setFsn(
    uint16_t fsn) {
  maybeFsn_ = fsn;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setUna(
    uint16_t una) {
  maybeUna_ = una;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setTag(
    uint8_t tag) {
  maybeTag_ = tag;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setAckRequest(
    bool ackRequest) {
  maybeAckRequest_ = ackRequest;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setIsRetransmit(
    bool isRetransmit) {
  maybeIsRetransmit_ = isRetransmit;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setIsTimeout(
    bool isTimeout) {
  maybeIsTimeout_ = isTimeout;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setPadding(
    uint8_t padding) {
  DCHECK_LE(padding, kPaddingMask);
  maybePadding_ = padding;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setPosn(
    uint16_t posn) {
  maybePosn_ = posn;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setCsn(
    uint16_t csn) {
  maybeCsn_ = csn;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setMsn(
    uint16_t msn) {
  maybeMsn_ = msn;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setRateHints(
    uint32_t rateHints) {
  maybeRateHints_ = rateHints;
  return std::move(*this);
}

DataHeader::DataHeaderBuilder&& DataHeader::DataHeaderBuilder::setTimestamp(
    uint32_t timestamp) {
  maybeTimestamp_ = timestamp;
  return std::move(*this);
}

DataHeader DataHeader::DataHeaderBuilder::build() && {
  CHECK(maybeFsn_.has_value()) << "DataHeader.fsn must be set";
  CHECK(maybeUna_.has_value()) << "DataHeader.una must be set";
  CHECK(maybePosn_.has_value()) << "DataHeader.posn must be set";
  CHECK(maybeMsn_.has_value()) << "DataHeader.msn must be set";
  return DataHeader(
      maybeFsn_.value(),
      maybeUna_.value(),
      maybeTag_.value_or(0),
      maybeAckRequest_.value_or(false),
      maybeIsRetransmit_.value_or(false),
      maybeIsTimeout_.value_or(false),
      maybePosn_.value(),
      maybeCsn_.value_or(0),
      maybeMsn_.value(),
      maybeRateHints_.value_or(kDefaultRateHintGbps),
      maybeTimestamp_.value_or(0),
      maybePadding_.value_or(0));
}

void DataHeader::write(folly::io::QueueAppender& appender) const {
  appender.writeBE<uint16_t>(fsn_);
  appender.writeBE<uint16_t>(una_);
  appender.write<uint8_t>(tag_);
  uint8_t flags = static_cast<uint8_t>(ackRequest_ << 7) |
      (isRetransmit_ << 6) | (isTimeout_ << 5) | (padding_ & kPaddingMask);
  appender.write<uint8_t>(flags);
  appender.writeBE<uint16_t>(posn_);
  appender.writeBE<uint16_t>(csn_);
  appender.writeBE<uint16_t>(msn_);
  appender.writeBE<uint32_t>(rateHints_);
  appender.writeBE<uint32_t>(timestamp_);
}

} // namespace libsoftmetaroce
