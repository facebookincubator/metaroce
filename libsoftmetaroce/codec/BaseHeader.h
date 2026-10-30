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
#include <optional>

namespace libsoftmetaroce {

class BaseHeader : public Header {
 public:
  static constexpr size_t kBaseHeaderSizeBytes = 12;
  static constexpr size_t kFlowIdBits = 12;
  static constexpr uint16_t kFlowIdMask = (1 << kFlowIdBits) - 1;

  class Opcode {
   public:
    enum class OperationType : uint8_t {
      SEND = 0b00000,
      SEND_IMM_P = 0b00001,
      WRITE_IMM_P = 0b00010,
      SEND_LAST = 0b00100,
      WRITE = 0b00110,
      WRITE_IMM = 0b00111,
      READ = 0b01100,
      READ_IMM_P = 0b01101,
      // Read Response
      READ_RESP = 0b01111,
      // RNR-Cancel, Receiver is Ready
      RNR_CANCEL = 0b10000,
      SEL_ACK = 0b10001,
      ATOMIC_ACK_RESP = 0b10010,
      ATOMIC_FETCH_ADD = 0b10100,
      ATOMIC_CMP_SWP = 0b10101,
      ATOMIC_RESERVED_0 = 0b10110,
      ATOMIC_RESERVED_1 = 0b10111,
      RELIABLE_CONTROL = 0b11110, // not supported in all implementations
      UNRELIABLE_CONTROL = 0b11111,
    };
    constexpr Opcode() = default;
    constexpr explicit Opcode(uint8_t val) : value_(val) {}
    // This is used exclusively for MetaRoCE opcodes.
    constexpr explicit Opcode(OperationType opType)
        : value_(
              (static_cast<uint8_t>(BthOpcode::META_ROCE) << 5) |
              static_cast<uint8_t>(opType)) {}
    constexpr bool operator==(const Opcode& other) const {
      return value_ == other.value_;
    }
    constexpr uint8_t value() const {
      return value_;
    }

    // All reliably transmitted payload-carrying operations qualify as data,
    // including reads, writes, atomics, and their responses.  Control packets
    // (ACKs, RNR_CANCEL) do not.
    constexpr bool isData() const {
      return !isControl() && !isAnAck() && !isRnrCancel();
    }

    constexpr bool isControl() const {
      return operationType() == OperationType::RELIABLE_CONTROL ||
          operationType() == OperationType::UNRELIABLE_CONTROL;
    }

    constexpr bool isAnAck() const {
      return operationType() == OperationType::SEL_ACK;
    }

    constexpr bool isRnrCancel() const {
      return operationType() == OperationType::RNR_CANCEL;
    }

    // Get the opcode type. Only valid if the Bth is META_ROCE.
    constexpr OperationType operationType() const {
      return OperationType(value_ & 0x1F);
    }

    // Set the opcode type. Only valid if the Bth is META_ROCE.
    void setOperationType(OperationType type) {
      value_ = (static_cast<uint8_t>(BthOpcode::META_ROCE) << 5) |
          static_cast<uint8_t>(type);
    }

    constexpr bool hasRwHdr() const {
      const auto op = operationType();
      return (
          op == OperationType::WRITE_IMM_P || op == OperationType::READ_IMM_P ||
          op == OperationType::READ || op == OperationType::WRITE ||
          op == OperationType::WRITE_IMM);
      // OperationType::READ_RESP does not have a read/write header,
      // as the destination is known.
    }

    constexpr bool hasAtomicHdr() const {
      const auto op = operationType();
      return op == OperationType::ATOMIC_FETCH_ADD ||
          op == OperationType::ATOMIC_CMP_SWP;
    }

    constexpr bool hasAtomicAckHdr() const {
      return operationType() == OperationType::ATOMIC_ACK_RESP;
    }

    constexpr bool hasImmData() const {
      return operationType() == OperationType::SEND_IMM_P ||
          operationType() == OperationType::WRITE_IMM_P ||
          operationType() == OperationType::READ_IMM_P;
    }

    // Opcodes for which a zero-byte payload is forbidden, per spec 8.2.3
    // table "Zero-byte opcodes". A data packet for these opcodes that
    // arrives with `getPayloadSizeBytes() == 0` must be rejected.
    //
    // Spec-permitted (return false): SEND, SEND_IMM_P, SEND_LAST,
    // WRITE_IMM_P, READ_RESP (zero-length read confirms flush of prior
    // writes), and control packets.
    // Spec-forbidden (return true): plain WRITE and WRITE_IMM (non-`_P`).
    // Read requests and atomics carry no payload by design.
    bool requiresPayload() const {
      const auto op = operationType();
      return op == OperationType::WRITE || op == OperationType::WRITE_IMM;
    }

    // Base transport header opcodes. 3 MSBs of the opcode.
    enum class BthOpcode : uint8_t {
      ROCEv2_RC = 0b000,
      ROCEv2_UD = 0b100,
      META_ROCE = 0b110,
    };

    constexpr BthOpcode bthOpcode() const {
      return BthOpcode((value_ >> 5) & 0x7);
    }

    constexpr bool isMetaRoce() const {
      return bthOpcode() == BthOpcode::META_ROCE;
    }

    void setBthOpcode(BthOpcode bth) {
      uint8_t v = static_cast<uint8_t>(bth) & 0x7;
      value_ &= ~(0x7 << 5);
      value_ |= (v << 5);
    }

    friend std::ostream& operator<<(std::ostream& os, const Opcode& op) {
      switch (op.bthOpcode()) {
        case BthOpcode::META_ROCE:
          os << "MR:0x" << std::hex << (int)op.operationType();
          break;
        case BthOpcode::ROCEv2_RC:
          os << "RC:0x" << std::hex << (int)op.operationType();
          break;
        case BthOpcode::ROCEv2_UD:
          os << "UD:0x" << std::hex << (int)op.operationType();
          break;
        default:
          os << "??:0x" << std::hex << op.value();
          break;
      }
      return os;
    }

   private:
    uint8_t value_{0};
  };

  BaseHeader() = default;
  explicit BaseHeader(folly::io::Cursor& cursor);
  BaseHeader(
      Opcode opcode,
      uint8_t version,
      uint16_t flowId,
      uint32_t dstQp,
      uint32_t srcQp)
      : opcode_{opcode},
        version_{version},
        flowId_{flowId},
        dstQp_{dstQp},
        srcQp_{srcQp} {}
  bool operator==(const BaseHeader& other) const {
    bool eq = (opcode_ == other.opcode_);
    eq &= (version_ == other.version_);
    eq &= (flowId_ == other.flowId_);
    eq &= (dstQp_ == other.dstQp_);
    eq &= (srcQp_ == other.srcQp_);
    return eq;
  }

  [[nodiscard]] Opcode getOpcode() const {
    return opcode_;
  }
  [[nodiscard]] uint8_t getVersion() const {
    return version_;
  }
  [[nodiscard]] uint16_t getFlowId() const {
    return flowId_;
  }
  [[nodiscard]] uint32_t getDstQp() const {
    return dstQp_;
  }
  [[nodiscard]] uint32_t getSrcQp() const {
    return srcQp_;
  }

  // Setters for packet modifiers
  void setOpcode(const Opcode& opcode) {
    opcode_ = opcode;
  }
  void setVersion(uint8_t version) {
    version_ = version;
  }
  void setFlowId(uint16_t flowId) {
    DCHECK_LE(flowId, kFlowIdMask) << "BaseHeader.flowId must fit in 12 bits";
    flowId_ = flowId;
  }
  void setDstQp(uint32_t dstQp) {
    dstQp_ = dstQp;
  }
  void setSrcQp(uint32_t srcQp) {
    srcQp_ = srcQp;
  }

  void write(folly::io::QueueAppender& appender) const override;

  class BaseHeaderBuilder {
   public:
    BaseHeaderBuilder&& setOpcode(Opcode opcode);
    BaseHeaderBuilder&& setVersion(uint8_t version);
    BaseHeaderBuilder&& setFlowId(uint16_t flowId);
    BaseHeaderBuilder&& setDstQp(uint32_t dstQp);
    BaseHeaderBuilder&& setSrcQp(uint32_t srcQp);
    BaseHeader build() &&;
    explicit BaseHeaderBuilder() = default;

   private:
    std::optional<Opcode> maybeOpcode_;
    std::optional<uint8_t> maybeVersion_;
    std::optional<uint16_t> maybeFlowId_;
    std::optional<uint32_t> maybeDstQp_;
    std::optional<uint32_t> maybeSrcQp_;
  };

 private:
  Opcode opcode_;
  uint8_t reserved_{0};
  uint8_t version_{0};
  uint16_t flowId_{0};
  uint32_t dstQp_{0};
  uint32_t srcQp_{0};
};

} // namespace libsoftmetaroce
