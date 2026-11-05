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

#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/ProtocolCompatibility.h>
#include <libsoftmetaroce/codec/AckHeader.h>
#include <libsoftmetaroce/codec/AtomicHeader.h>
#include <libsoftmetaroce/codec/BaseHeader.h>
#include <libsoftmetaroce/codec/ControlHeader.h>
#include <libsoftmetaroce/codec/DataHeader.h>
#include <libsoftmetaroce/codec/PingHeader.h>
#include <libsoftmetaroce/codec/ReadWriteHeader.h>
#include <libsoftmetaroce/codec/RnrCancelHeader.h>
#include <array>
#include <memory>
#include <optional>

namespace libsoftmetaroce {
class Packet {
 public:
  Packet(const Packet& other)
      : baseHeader_(other.baseHeader_), ecnValue_(other.ecnValue_) {}

  Packet& operator=(const Packet& other) {
    if (this != &other) {
      baseHeader_ = other.baseHeader_;
      ecnValue_ = other.ecnValue_;
    }
    return *this;
  }

  // Returns a copy that owns its payload and so outlives the source buffer.
  virtual std::unique_ptr<Packet> clone() const {
    return std::make_unique<Packet>(*this);
  }

  // Deep-copy the payload so the packet outlives its source buffer. A packet
  // type with a payload must override this no-op.
  virtual void ownPayload() {}

  static void writePayload(
      const void* address,
      uint32_t payloadSizeBytes,
      folly::io::QueueAppender& appender) {
    if (payloadSizeBytes == 0) {
      return;
    }
    appender.insert(folly::IOBuf::wrapBuffer(address, payloadSizeBytes));
  }

  // Returns how many bytes pad a length of n to a 4-byte boundary.
  static constexpr uint32_t padBytesFor(uint32_t n) {
    return (4 - (n & 3u)) & 3u;
  }

  static uint32_t payloadPadBytes(uint32_t payloadSizeBytes) {
    return FLAGS_libsoftmetaroce_omit_padding ? 0u
                                              : padBytesFor(payloadSizeBytes);
  }

  static void writePayloadPadding(
      uint32_t payloadSizeBytes,
      folly::io::QueueAppender& appender) {
    const uint32_t pad = payloadPadBytes(payloadSizeBytes);
    if (pad > 0) {
      constexpr std::array<uint8_t, 3> kZero{};
      appender.push(kZero.data(), pad);
    }
  }

  virtual ~Packet() = default;

  virtual void serialize(folly::io::QueueAppender& appender) const {
    baseHeader_.write(appender);
  }

  [[nodiscard]] const BaseHeader& getBaseHeader() const {
    return baseHeader_;
  }

  // This getter is for packet modifiers.
  [[nodiscard]] BaseHeader& getBaseHeader() {
    return baseHeader_;
  }

  [[nodiscard]] virtual size_t getSizeBytes() const {
    return BaseHeader::kBaseHeaderSizeBytes;
  }

  void setEcnValue(uint8_t ecnValue) {
    ecnValue_ = ecnValue;
  }

  [[nodiscard]] std::optional<uint8_t> getEcnValue() const {
    return ecnValue_;
  }

 protected:
  explicit Packet(BaseHeader baseHeader) : baseHeader_(std::move(baseHeader)) {}

 private:
  BaseHeader baseHeader_;
  std::optional<uint8_t> ecnValue_;
};

class DataPacket : public Packet {
 public:
  DataPacket(
      BaseHeader baseHeader,
      DataHeader dataHeader,
      const void* payloadAddress,
      size_t payloadSizeBytes,
      std::optional<uint32_t> maybeImmData = std::nullopt)
      : Packet(std::move(baseHeader)),
        dataHeader_(std::move(dataHeader)),
        payloadAddress_(payloadAddress),
        payloadSizeBytes_(static_cast<uint32_t>(payloadSizeBytes)),
        maybeImmData_(maybeImmData) {
    // a payload is a single wire segment (<= MTU)
    DCHECK_LE(payloadSizeBytes, kMaxPacketSizeBytes);
  }
  DataPacket(BaseHeader baseHeader, DataHeader dataHeader)
      : DataPacket(std::move(baseHeader), std::move(dataHeader), nullptr, 0) {}

  DataPacket(
      BaseHeader baseHeader,
      DataHeader dataHeader,
      std::unique_ptr<folly::IOBuf> ownedPayload,
      std::optional<uint32_t> maybeImmData = std::nullopt)
      : Packet(std::move(baseHeader)),
        dataHeader_(std::move(dataHeader)),
        ownedPayloadBuf_(std::move(ownedPayload)),
        payloadAddress_(ownedPayloadBuf_->data()),
        payloadSizeBytes_(static_cast<uint32_t>(ownedPayloadBuf_->length())),
        maybeImmData_(maybeImmData) {
    // a payload is a single wire segment (<= MTU)
    DCHECK_LE(ownedPayloadBuf_->length(), kMaxPacketSizeBytes);
  }

  DataPacket(const DataPacket& other)
      : Packet(other),
        dataHeader_(other.dataHeader_),
        ownedPayloadBuf_(
            other.ownedPayloadBuf_ ? other.ownedPayloadBuf_->clone() : nullptr),
        payloadAddress_(
            ownedPayloadBuf_ ? ownedPayloadBuf_->data()
                             : other.payloadAddress_),
        payloadSizeBytes_(other.payloadSizeBytes_),
        maybeImmData_(other.maybeImmData_) {}

  DataPacket& operator=(const DataPacket& other) {
    if (this != &other) {
      Packet::operator=(other);
      dataHeader_ = other.dataHeader_;
      ownedPayloadBuf_ =
          other.ownedPayloadBuf_ ? other.ownedPayloadBuf_->clone() : nullptr;
      payloadAddress_ =
          ownedPayloadBuf_ ? ownedPayloadBuf_->data() : other.payloadAddress_;
      payloadSizeBytes_ = other.payloadSizeBytes_;
      maybeImmData_ = other.maybeImmData_;
    }
    return *this;
  }

  std::unique_ptr<Packet> clone() const override {
    auto copy = std::make_unique<DataPacket>(*this);
    // expect the clone may outlive the original buffer
    copy->ownPayload();
    return copy;
  }

  void serialize(folly::io::QueueAppender& appender) const override {
    Packet::serialize(appender);
    dataHeader_.write(appender);
    if (maybeImmData_) {
      appender.write<unsigned int>(maybeImmData_.value());
    }
    Packet::writePayload(payloadAddress_, payloadSizeBytes_, appender);
    Packet::writePayloadPadding(payloadSizeBytes_, appender);
  }

  [[nodiscard]] size_t getSizeBytes() const override {
    return Packet::getSizeBytes() + DataHeader::kDataHeaderSizeBytes +
        (maybeImmData_.has_value() ? 4 : 0) + payloadSizeBytes_ +
        Packet::payloadPadBytes(payloadSizeBytes_);
  }

  [[nodiscard]] const DataHeader& getDataHeader() const {
    return dataHeader_;
  }

  // This getter is for packet modifiers.
  [[nodiscard]] DataHeader& getDataHeader() {
    return dataHeader_;
  }

  // Copies at most expectedSize bytes to the destination, ignoring padding.
  void deliverPayload(void* destinationAddress, size_t expectedSize) const {
    uint32_t bytesToCopy = static_cast<uint32_t>(
        std::min<size_t>(payloadSizeBytes_, expectedSize));
    if (bytesToCopy > 0) {
      memcpy(destinationAddress, payloadAddress_, bytesToCopy);
    }
  }

  [[nodiscard]] folly::ByteRange getPayload() const {
    return {static_cast<const uint8_t*>(payloadAddress_), payloadSizeBytes_};
  }

  [[nodiscard]] const std::optional<uint32_t>& getImmData() const {
    return maybeImmData_;
  }

  [[nodiscard]] uint32_t getPayloadSizeBytes() const {
    return payloadSizeBytes_;
  }

  void ownPayload() override {
    const bool nothingToOwn = ownedPayloadBuf_ || payloadSizeBytes_ == 0;
    if (nothingToOwn) {
      return;
    }
    ownedPayloadBuf_ =
        folly::IOBuf::copyBuffer(payloadAddress_, payloadSizeBytes_);
    payloadAddress_ = ownedPayloadBuf_->data();
  }

  // This mutator is for packet modifiers. A size at or above the current
  // payload leaves the payload unchanged.
  void truncatePayload(uint32_t newSizeBytes = 0) {
    if (newSizeBytes >= payloadSizeBytes_) {
      return;
    }
    payloadSizeBytes_ = newSizeBytes;
    if (newSizeBytes == 0) {
      ownedPayloadBuf_.reset();
      payloadAddress_ = nullptr;
    } else if (ownedPayloadBuf_ != nullptr) {
      // Keep the owned buffer's length consistent with payloadSizeBytes_.
      ownedPayloadBuf_->trimEnd(ownedPayloadBuf_->length() - newSizeBytes);
      payloadAddress_ = ownedPayloadBuf_->data();
    }
  }

 protected:
  DataHeader dataHeader_;
  std::unique_ptr<folly::IOBuf> ownedPayloadBuf_;
  const void* payloadAddress_;
  uint32_t payloadSizeBytes_;
  std::optional<uint32_t> maybeImmData_;
};

// The packet the parser produces for an opcode it cannot decode; the responder
// rejects it with a NAK.
class UnsupportedPacket : public DataPacket {
 public:
  using DataPacket::DataPacket;

  std::unique_ptr<Packet> clone() const override {
    auto copy = std::make_unique<UnsupportedPacket>(*this);
    // expect the clone may outlive the original buffer
    copy->ownPayload();
    return copy;
  }

  // An unsupported packet is received but never sent: its header layout is
  // unknown. This override of Packet::serialize is fatal.
  void serialize(folly::io::QueueAppender& /*appender*/) const override {
    LOG(FATAL) << "UnsupportedPacket must never be serialized";
  }
};

class ReadWritePacket : public DataPacket {
 public:
  ReadWritePacket(
      BaseHeader baseHeader,
      DataHeader dataHeader,
      ReadWriteHeader rwHeader,
      const void* payloadAddress,
      size_t payloadSizeBytes,
      std::optional<uint32_t> maybeImmData = std::nullopt)
      : DataPacket(
            std::move(baseHeader),
            std::move(dataHeader),
            payloadAddress,
            payloadSizeBytes,
            maybeImmData),
        rwHeader_(std::move(rwHeader)) {
    DCHECK(getBaseHeader().getOpcode().hasRwHdr());
  }

  ReadWritePacket(
      BaseHeader baseHeader,
      DataHeader dataHeader,
      ReadWriteHeader rwHeader,
      std::unique_ptr<folly::IOBuf> ownedPayload,
      std::optional<uint32_t> maybeImmData = std::nullopt)
      : DataPacket(
            std::move(baseHeader),
            std::move(dataHeader),
            std::move(ownedPayload),
            maybeImmData),
        rwHeader_(std::move(rwHeader)) {
    DCHECK(getBaseHeader().getOpcode().hasRwHdr());
  }

  ReadWritePacket(const ReadWritePacket& other)
      : DataPacket(other), rwHeader_(other.rwHeader_) {}

  ReadWritePacket& operator=(const ReadWritePacket& other) {
    if (this != &other) {
      DataPacket::operator=(other);
      rwHeader_ = other.rwHeader_;
    }
    return *this;
  }

  std::unique_ptr<Packet> clone() const override {
    auto copy = std::make_unique<ReadWritePacket>(*this);
    // expect the clone may outlive the original buffer
    copy->ownPayload();
    return copy;
  }

  void serialize(folly::io::QueueAppender& appender) const override {
    Packet::serialize(appender);
    dataHeader_.write(appender);
    rwHeader_.write(appender);
    if (maybeImmData_) {
      appender.write<unsigned int>(maybeImmData_.value());
    }
    Packet::writePayload(payloadAddress_, payloadSizeBytes_, appender);
    Packet::writePayloadPadding(payloadSizeBytes_, appender);
  }

  [[nodiscard]] size_t getSizeBytes() const override {
    return Packet::getSizeBytes() + DataHeader::kDataHeaderSizeBytes +
        ReadWriteHeader::kReadWriteHeaderSizeBytes +
        (maybeImmData_.has_value() ? 4 : 0) + payloadSizeBytes_ +
        Packet::payloadPadBytes(payloadSizeBytes_);
  }

  [[nodiscard]] const ReadWriteHeader& getReadWriteHeader() const {
    return rwHeader_;
  }

  // This getter is for packet modifiers.
  [[nodiscard]] ReadWriteHeader& getReadWriteHeader() {
    return rwHeader_;
  }

 private:
  ReadWriteHeader rwHeader_;
};

class AtomicPacket : public DataPacket {
 public:
  AtomicPacket(
      BaseHeader baseHeader,
      DataHeader dataHeader,
      AtomicHeader atomicHeader)
      : DataPacket(std::move(baseHeader), std::move(dataHeader), nullptr, 0),
        atomicHeader_(std::move(atomicHeader)) {
    DCHECK(getBaseHeader().getOpcode().hasAtomicHdr());
  }

  std::unique_ptr<Packet> clone() const override {
    return std::make_unique<AtomicPacket>(*this);
  }

  void serialize(folly::io::QueueAppender& appender) const override {
    Packet::serialize(appender);
    dataHeader_.write(appender);
    atomicHeader_.write(appender);
  }

  [[nodiscard]] size_t getSizeBytes() const override {
    return Packet::getSizeBytes() + DataHeader::kDataHeaderSizeBytes +
        AtomicHeader::kAtomicHeaderSizeBytes;
  }

  [[nodiscard]] const AtomicHeader& getAtomicHeader() const {
    return atomicHeader_;
  }

  [[nodiscard]] AtomicHeader& getAtomicHeader() {
    return atomicHeader_;
  }

 private:
  AtomicHeader atomicHeader_;
};

class AtomicAckPacket : public DataPacket {
 public:
  AtomicAckPacket(
      BaseHeader baseHeader,
      DataHeader dataHeader,
      AtomicAckHeader atomicAckHeader)
      : DataPacket(std::move(baseHeader), std::move(dataHeader), nullptr, 0),
        atomicAckHeader_(std::move(atomicAckHeader)) {
    DCHECK(getBaseHeader().getOpcode().hasAtomicAckHdr());
  }

  std::unique_ptr<Packet> clone() const override {
    return std::make_unique<AtomicAckPacket>(*this);
  }

  void serialize(folly::io::QueueAppender& appender) const override {
    Packet::serialize(appender);
    dataHeader_.write(appender);
    atomicAckHeader_.write(appender);
  }

  [[nodiscard]] size_t getSizeBytes() const override {
    return Packet::getSizeBytes() + DataHeader::kDataHeaderSizeBytes +
        AtomicAckHeader::kAtomicAckHeaderSizeBytes;
  }

  [[nodiscard]] const AtomicAckHeader& getAtomicAckHeader() const {
    return atomicAckHeader_;
  }

  [[nodiscard]] AtomicAckHeader& getAtomicAckHeader() {
    return atomicAckHeader_;
  }

 private:
  AtomicAckHeader atomicAckHeader_;
};

class AckPacket : public Packet {
 public:
  AckPacket(BaseHeader baseHeader, AckHeader ackHeader)
      : Packet(std::move(baseHeader)), ackHeader_(std::move(ackHeader)) {}

  AckPacket(const AckPacket& other)
      : Packet(other), ackHeader_(other.ackHeader_) {}

  AckPacket& operator=(const AckPacket& other) {
    if (this != &other) {
      Packet::operator=(other);
      ackHeader_ = other.ackHeader_;
    }
    return *this;
  }

  std::unique_ptr<Packet> clone() const override {
    return std::make_unique<AckPacket>(*this);
  }

  void serialize(folly::io::QueueAppender& appender) const override {
    Packet::serialize(appender);
    ackHeader_.write(appender);
  }

  [[nodiscard]] size_t getSizeBytes() const override {
    constexpr size_t kBitVectorSizeBytes = AckHeader::kAckTrackerBits / 8;
    const auto mainAckPacketSize =
        Packet::getSizeBytes() + AckHeader::kMainAckHeaderSizeBytes;
    AckStatus ackStatus = ackHeader_.getAckStatus();
    if (ackStatus == AckStatus::BRNR) {
      // BRNR-NAK so add FSN and RNR bit vector
      return mainAckPacketSize + kBitVectorSizeBytes + kBitVectorSizeBytes;
    } else if (ackStatus == AckStatus::ACK_OUT_OF_ORDER) {
      // out-of-order ACK so add FSN bit vector
      return mainAckPacketSize + kBitVectorSizeBytes;
    } else {
      return mainAckPacketSize;
    }
  }

  [[nodiscard]] const AckHeader& getAckHeader() const {
    return ackHeader_;
  }

  // This getter is for packet modifiers.
  [[nodiscard]] AckHeader& getAckHeader() {
    return ackHeader_;
  }

 private:
  AckHeader ackHeader_;
};

class RnrCancelPacket : public Packet {
 public:
  RnrCancelPacket(BaseHeader baseHeader, RnrCancelHeader rnrCancelHeader)
      : Packet(std::move(baseHeader)),
        rnrCancelHeader_(std::move(rnrCancelHeader)) {}

  RnrCancelPacket(const RnrCancelPacket& other)
      : Packet(other), rnrCancelHeader_(other.rnrCancelHeader_) {}

  RnrCancelPacket& operator=(const RnrCancelPacket& other) {
    if (this != &other) {
      Packet::operator=(other);
      rnrCancelHeader_ = other.rnrCancelHeader_;
    }
    return *this;
  }

  std::unique_ptr<Packet> clone() const override {
    return std::make_unique<RnrCancelPacket>(*this);
  }

  void serialize(folly::io::QueueAppender& appender) const override {
    Packet::serialize(appender);
    rnrCancelHeader_.write(appender);
  }

  [[nodiscard]] size_t getSizeBytes() const override {
    return Packet::getSizeBytes() + RnrCancelHeader::kRnrCancelHeaderSizeBytes;
  }

  [[nodiscard]] const RnrCancelHeader& getRnrCancelHeader() const {
    return rnrCancelHeader_;
  }

  // This getter is for packet modifiers.
  [[nodiscard]] RnrCancelHeader& getRnrCancelHeader() {
    return rnrCancelHeader_;
  }

 private:
  RnrCancelHeader rnrCancelHeader_;
};

class ControlPacket : public Packet {
 public:
  ControlPacket(BaseHeader baseHeader, ControlHeader controlHeader)
      : Packet(std::move(baseHeader)),
        controlHeader_(std::move(controlHeader)) {}

  ControlPacket(const ControlPacket& other)
      : Packet(other), controlHeader_(other.controlHeader_) {}

  ControlPacket(ControlPacket&&) = default;

  ControlPacket& operator=(const ControlPacket& other) {
    if (this != &other) {
      Packet::operator=(other);
      controlHeader_ = other.controlHeader_;
    }
    return *this;
  }

  ControlPacket& operator=(ControlPacket&&) = default;

  ~ControlPacket() override = default;

  std::unique_ptr<Packet> clone() const override {
    return std::make_unique<ControlPacket>(*this);
  }

  void serialize(folly::io::QueueAppender& appender) const override {
    Packet::serialize(appender);
    controlHeader_.write(appender);
  }

  [[nodiscard]] size_t getSizeBytes() const override {
    return Packet::getSizeBytes() + ControlHeader::kControlHeaderSizeBytes;
  }

  [[nodiscard]] const ControlHeader& getControlHeader() const {
    return controlHeader_;
  }

  // This getter is for packet modifiers.
  [[nodiscard]] ControlHeader& getControlHeader() {
    return controlHeader_;
  }

 private:
  ControlHeader controlHeader_;
};

class PingPacket : public ControlPacket {
 public:
  PingPacket(
      BaseHeader baseHeader,
      ControlHeader controlHeader,
      PingHeader pingHeader,
      const void* payloadAddress,
      size_t payloadSizeBytes)
      : ControlPacket(std::move(baseHeader), std::move(controlHeader)),
        pingHeader_(std::move(pingHeader)),
        payloadAddress_(payloadAddress),
        payloadSizeBytes_(static_cast<uint32_t>(payloadSizeBytes)) {
    // a payload is a single wire segment (<= MTU)
    DCHECK_LE(payloadSizeBytes, kMaxPacketSizeBytes);
    CHECK(
        getControlHeader().getType() == ControlPacketType::PING ||
        getControlHeader().getType() == ControlPacketType::PING_RESPONSE);
  }

  PingPacket(
      BaseHeader baseHeader,
      ControlHeader controlHeader,
      PingHeader pingHeader,
      std::unique_ptr<folly::IOBuf> ownedPayload)
      : ControlPacket(std::move(baseHeader), std::move(controlHeader)),
        pingHeader_(std::move(pingHeader)),
        ownedPayload_(std::move(ownedPayload)),
        payloadAddress_(ownedPayload_->data()),
        payloadSizeBytes_(static_cast<uint32_t>(ownedPayload_->length())) {
    // a payload is a single wire segment (<= MTU)
    DCHECK_LE(ownedPayload_->length(), kMaxPacketSizeBytes);
    CHECK(
        getControlHeader().getType() == ControlPacketType::PING ||
        getControlHeader().getType() == ControlPacketType::PING_RESPONSE);
  }

  PingPacket(const PingPacket& other)
      : ControlPacket(other),
        pingHeader_(other.pingHeader_),
        ownedPayload_(
            other.ownedPayload_ ? other.ownedPayload_->clone() : nullptr),
        payloadAddress_(
            ownedPayload_ ? ownedPayload_->data() : other.payloadAddress_),
        payloadSizeBytes_(other.payloadSizeBytes_) {}

  PingPacket(PingPacket&&) = default;

  PingPacket& operator=(const PingPacket& other) {
    if (this != &other) {
      ControlPacket::operator=(other);
      pingHeader_ = other.pingHeader_;
      ownedPayload_ =
          other.ownedPayload_ ? other.ownedPayload_->clone() : nullptr;
      payloadAddress_ =
          ownedPayload_ ? ownedPayload_->data() : other.payloadAddress_;
      payloadSizeBytes_ = other.payloadSizeBytes_;
    }
    return *this;
  }

  PingPacket& operator=(PingPacket&&) = default;

  ~PingPacket() override = default;

  std::unique_ptr<Packet> clone() const override {
    auto copy = std::make_unique<PingPacket>(*this);
    // expect the clone may outlive the original buffer
    copy->ownPayload();
    return copy;
  }

  void ownPayload() override {
    const bool nothingToOwn = ownedPayload_ || payloadSizeBytes_ == 0;
    if (nothingToOwn) {
      return;
    }
    ownedPayload_ =
        folly::IOBuf::copyBuffer(payloadAddress_, payloadSizeBytes_);
    payloadAddress_ = ownedPayload_->data();
  }

  void serialize(folly::io::QueueAppender& appender) const override {
    ControlPacket::serialize(appender);
    pingHeader_.write(appender);

    Packet::writePayload(payloadAddress_, payloadSizeBytes_, appender);
    Packet::writePayloadPadding(payloadSizeBytes_, appender);
  }

  [[nodiscard]] size_t getSizeBytes() const override {
    return ControlPacket::getSizeBytes() + PingHeader::kPingHeaderSizeBytes +
        payloadSizeBytes_ + Packet::payloadPadBytes(payloadSizeBytes_);
  }

  [[nodiscard]] const PingHeader& getPingHeader() const {
    return pingHeader_;
  }

  // This getter is for packet modifiers.
  [[nodiscard]] PingHeader& getPingHeader() {
    return pingHeader_;
  }

 private:
  PingHeader pingHeader_;
  std::unique_ptr<folly::IOBuf> ownedPayload_;
  const void* payloadAddress_;
  uint32_t payloadSizeBytes_;
};
} // namespace libsoftmetaroce
