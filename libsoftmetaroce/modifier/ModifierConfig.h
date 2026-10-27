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

#include <folly/IPAddress.h>
#include <folly/SocketAddress.h>
#include <folly/container/F14Map.h>
#include <folly/json/json.h>
#include <libsoftmetaroce/codec/Packet.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace libsoftmetaroce {

struct StripedMessageWaveState;

enum class ComparisonOperator {
  Eq, // Equal (==)
  Ne, // Not Equal (!=)
  Gt, // Greater Than (>)
  Lt, // Less Than (<)
  Ge, // Greater or Equal (>=)
  Le, // Less or Equal (<=)
  In, // In a set of values
  Mod, // Modulo (%)
};

// Represents a single field condition with an operator and value(s)
struct FieldCondition {
  ComparisonOperator op{ComparisonOperator::Eq};
  uint32_t singleValue{0};
  std::unordered_set<uint32_t> valueSet; // For 'In' operator
  uint32_t moduloRemainder{0}; // For 'Mod' operator

  FieldCondition() = default;
  explicit FieldCondition(uint32_t val)
      : op(ComparisonOperator::Eq), singleValue(val) {}
  FieldCondition(ComparisonOperator op, uint32_t val)
      : op(op), singleValue(val) {}
  FieldCondition(ComparisonOperator op, uint32_t divisor, uint32_t remainder)
      : op(op), singleValue(divisor), moduloRemainder(remainder) {}
  explicit FieldCondition(std::unordered_set<uint32_t> vals)
      : op(ComparisonOperator::In), valueSet(std::move(vals)) {}

  [[nodiscard]] bool evaluate(uint32_t actualValue) const {
    switch (op) {
      case ComparisonOperator::Eq:
        return actualValue == singleValue;
      case ComparisonOperator::Ne:
        return actualValue != singleValue;
      case ComparisonOperator::Gt:
        return actualValue > singleValue;
      case ComparisonOperator::Lt:
        return actualValue < singleValue;
      case ComparisonOperator::Ge:
        return actualValue >= singleValue;
      case ComparisonOperator::Le:
        return actualValue <= singleValue;
      case ComparisonOperator::In:
        return valueSet.count(actualValue) > 0;
      case ComparisonOperator::Mod:
        return singleValue != 0 && actualValue % singleValue == moduloRemainder;
    }
    return false;
  }
};

// Represents multiple conditions for a field, combined with AND logic
struct FieldConditions {
  std::vector<FieldCondition> conditions;

  FieldConditions() = default;
  explicit FieldConditions(FieldCondition condition) {
    conditions.push_back(std::move(condition));
  }
  explicit FieldConditions(std::vector<FieldCondition> conds)
      : conditions(std::move(conds)) {}

  // Returns true only if ALL conditions are satisfied
  [[nodiscard]] bool evaluate(uint32_t actualValue) const {
    for (const auto& condition : conditions) {
      if (!condition.evaluate(actualValue)) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] ComparisonOperator op() const {
    if (conditions.size() > 1) {
      throw std::runtime_error(
          "op() is ambiguous for FieldConditions with multiple conditions");
    }
    return conditions.empty() ? ComparisonOperator::Eq : conditions[0].op;
  }
  [[nodiscard]] uint32_t value() const {
    if (conditions.size() > 1) {
      throw std::runtime_error(
          "value() is ambiguous for FieldConditions with multiple conditions");
    }
    return conditions.empty() ? 0 : conditions[0].singleValue;
  }
};

class ModifierConfig {
 public:
  enum class ModifierType {
    Drop,
    Reorder,
    StripedMessageWave,
    Inject,
    Duplicate,
    Corrupt,
    WriteEdit,
    HeaderEdit,
    MarkEcn,
    Pacing,
    Truncate,
    Unknown,
  };

  class ModifierFilter {
   public:
    enum class Direction {
      Rx,
      Tx,
    };

    enum class PacketType {
      Data,
      Ack,
    };

    explicit ModifierFilter(
        Direction direction,
        PacketType packetType,
        folly::F14FastMap<std::string, FieldConditions> fields)
        : direction(direction),
          packetType(packetType),
          fields(std::move(fields)) {}

    explicit ModifierFilter(const folly::dynamic* filterPtr);

    Direction direction{Direction::Tx};
    PacketType packetType{PacketType::Data};
    double randomPercent{100.0};
    // Key is packet field name, value is the conditions to match
    folly::F14FastMap<std::string, FieldConditions> fields;
    // If set, matchesFilter only matches when the remote peer's IP equals this.
    std::optional<folly::IPAddress> remoteAddr;

   private:
    static ComparisonOperator parseOperator(const std::string& opStr);
    static FieldCondition parseFieldCondition(const folly::dynamic& fieldValue);
    static FieldConditions parseFieldConditions(
        const folly::dynamic& fieldValue);
  };

  [[nodiscard]] static std::unique_ptr<ModifierConfig>
  buildModifierConfigFromJson(const folly::dynamic& configJson);

  [[nodiscard]] static std::vector<std::unique_ptr<ModifierConfig>>
  buildModifierConfigsFromJson(const std::string& configJsonStr);

  explicit ModifierConfig(
      ModifierType type,
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      const std::optional<std::string>& maybeName = std::nullopt)
      : name(maybeName.value_or("")),
        type(type),
        filter(filter),
        modificationTimes(modificationTimes),
        appliedCount(std::make_shared<std::atomic<uint32_t>>(0)) {}
  virtual ~ModifierConfig() {}

  [[nodiscard]] bool matchesFilter(
      const Packet& packet,
      const ModifierConfig::ModifierFilter::Direction& direction,
      const folly::SocketAddress& remoteAddr) const;

  std::string name;
  ModifierType type{ModifierType::Unknown};
  ModifierFilter filter;
  // The number of times the modifier will be applied in total.
  // Note that this applies to all the packets that match the filter.
  // If set to 'unlimited', 'always', 'infinite' or not set a value, applies to
  // all the times.
  std::optional<uint32_t> modificationTimes;
  // Atomic counter backing modificationTimes. Shared across per-path Modifier
  // instances so the cap is global.
  std::shared_ptr<std::atomic<uint32_t>> appliedCount;
};

class DropModifierConfig : public ModifierConfig {
 public:
  explicit DropModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      const std::optional<std::string>& maybeName = std::nullopt)
      : ModifierConfig(
            ModifierType::Drop,
            filter,
            modificationTimes,
            maybeName) {}
};

class ReorderModifierConfig : public ModifierConfig {
 public:
  class Distance {
    /*
     * Class to represent the how far a packet has to be reordered from its
     * original.
     */
   public:
    enum class Unit { Packet, TimeUs };
    explicit Distance(uint32_t value, Unit unit) : value(value), unit(unit) {}
    explicit Distance(const folly::dynamic* distancePtr);
    bool operator==(const Distance& other) const {
      return (value == other.value && unit == other.unit);
    }
    uint32_t value;
    Unit unit{Unit::TimeUs};
  };
  explicit ReorderModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      const Distance& distance,
      const std::optional<std::string>& maybeName = std::nullopt)
      : ModifierConfig(
            ModifierType::Reorder,
            filter,
            modificationTimes,
            maybeName),
        distance(distance) {}
  Distance distance;
};

class StripedMessageWaveModifierConfig : public ModifierConfig {
 public:
  explicit StripedMessageWaveModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      uint32_t qpCount,
      uint16_t tailPosn,
      uint32_t maxPendingPackets,
      uint32_t releaseIntervalUs,
      const std::optional<std::string>& maybeName = std::nullopt,
      std::optional<uint32_t> tailReleaseIntervalUs = std::nullopt)
      : ModifierConfig(
            ModifierType::StripedMessageWave,
            filter,
            modificationTimes,
            maybeName),
        qpCount(qpCount),
        tailPosn(tailPosn),
        maxPendingPackets(maxPendingPackets),
        releaseIntervalUs(releaseIntervalUs),
        tailReleaseIntervalUs(tailReleaseIntervalUs) {}

  uint32_t qpCount;
  uint16_t tailPosn;
  uint32_t maxPendingPackets;
  uint32_t releaseIntervalUs;
  std::optional<uint32_t> tailReleaseIntervalUs;
  std::shared_ptr<StripedMessageWaveState> state;
};

class DuplicateModifierConfig : public ModifierConfig {
 public:
  explicit DuplicateModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      uint32_t delayUs,
      const std::optional<std::string>& maybeName = std::nullopt)
      : ModifierConfig(
            ModifierType::Duplicate,
            filter,
            modificationTimes,
            maybeName),
        delayUs(delayUs) {}
  uint32_t delayUs;
};

class CorruptModifierConfig : public ModifierConfig {
 public:
  explicit CorruptModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      uint32_t positionToCorrupt = 0,
      std::optional<uint8_t> valueToSet = std::nullopt,
      bool calculateNewIcrc = false,
      const std::optional<std::string>& maybeName = std::nullopt)
      : ModifierConfig(
            ModifierType::Corrupt,
            filter,
            modificationTimes,
            maybeName),
        positionToCorrupt(positionToCorrupt),
        valueToSet(valueToSet),
        calculateNewIcrc(calculateNewIcrc) {}

  uint32_t positionToCorrupt{0};
  std::optional<uint8_t> valueToSet;
  bool calculateNewIcrc{false};
};

class WriteHeaderEditModifierConfig : public ModifierConfig {
 public:
  explicit WriteHeaderEditModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      const std::string& target = "rkey",
      std::optional<uint64_t> valueToSet = std::nullopt,
      const std::optional<std::string>& maybeName = std::nullopt)
      : ModifierConfig(
            ModifierType::WriteEdit,
            filter,
            modificationTimes,
            maybeName),
        target(target),
        valueToSet(valueToSet) {}

  std::string target{"rkey"};
  std::optional<uint64_t> valueToSet;
};

class HeaderEditModifierConfig : public ModifierConfig {
 public:
  enum class HeaderType {
    Base,
    Data,
    ReadWrite,
    Ack,
    RnrCancel,
  };

  explicit HeaderEditModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      HeaderType headerType,
      const std::string& fieldName,
      std::optional<uint64_t> valueToSet = std::nullopt,
      const std::optional<std::string>& maybeName = std::nullopt,
      std::optional<uint64_t> valueToSubtract = std::nullopt)
      : ModifierConfig(
            ModifierType::HeaderEdit,
            filter,
            modificationTimes,
            maybeName),
        headerType(headerType),
        fieldName(fieldName),
        valueToSet(valueToSet),
        valueToSubtract(valueToSubtract) {}

  // A flag or a status code has no magnitude, so an arithmetic edit has no
  // meaning for it and the parser rejects one.
  [[nodiscard]] static bool isNonNumericField(const std::string& fieldName);

  // Callers narrow the result to the edited field's own width, so subtracting
  // past zero wraps there.
  [[nodiscard]] uint64_t editedValue(uint64_t currentValue) const {
    if (valueToSubtract.has_value()) {
      return currentValue - *valueToSubtract;
    }
    if (valueToSet.has_value()) {
      return *valueToSet;
    }
    return currentValue ^ 0xFF;
  }

  [[nodiscard]] std::string_view operationName() const {
    if (valueToSubtract.has_value()) {
      return "subtract";
    }
    return valueToSet.has_value() ? "set_value" : "xor_0xFF";
  }

  HeaderType headerType;
  std::string fieldName;
  // At most one of these is set; which one names the operation.
  std::optional<uint64_t> valueToSet;
  std::optional<uint64_t> valueToSubtract;
};

class TruncateModifierConfig : public ModifierConfig {
 public:
  explicit TruncateModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      uint32_t payloadSizeBytes = 0,
      const std::optional<std::string>& maybeName = std::nullopt)
      : ModifierConfig(
            ModifierType::Truncate,
            filter,
            modificationTimes,
            maybeName),
        payloadSizeBytes(payloadSizeBytes) {}

  // Target payload size to shrink the packet to; the modifier never grows it.
  uint32_t payloadSizeBytes{0};
};

class MarkModifierConfig : public ModifierConfig {
 public:
  explicit MarkModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      const std::optional<std::string>& maybeName = std::nullopt)
      : ModifierConfig(
            ModifierType::MarkEcn,
            filter,
            modificationTimes,
            maybeName) {}
};

class PacingModifierConfig : public ModifierConfig {
 public:
  explicit PacingModifierConfig(
      const ModifierFilter& filter,
      std::optional<uint32_t> modificationTimes,
      uint32_t intervalUs,
      const std::optional<std::string>& maybeName = std::nullopt)
      : ModifierConfig(
            ModifierType::Pacing,
            filter,
            modificationTimes,
            maybeName),
        intervalUs(intervalUs) {}
  uint32_t intervalUs;
};

} // namespace libsoftmetaroce
