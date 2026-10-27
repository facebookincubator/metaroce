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

#include <libsoftmetaroce/modifier/ModifierConfig.h>

#include <cmath>
#include <random>

namespace libsoftmetaroce {

namespace {
constexpr double kAlwaysPercent = 100.0;

// Helper function to parse modification_times field from JSON
std::optional<uint32_t> parseModificationTimes(
    const folly::dynamic* modTimesPtr) {
  if (!modTimesPtr) {
    // If not specified, default to unlimited (same as "always")
    return std::nullopt;
  }

  if (modTimesPtr->isString()) {
    auto valueStr = modTimesPtr->asString();
    folly::toLowerAscii(valueStr);

    if (valueStr == "always" || valueStr == "unlimited" ||
        valueStr == "infinite") {
      return std::nullopt;
    }

    // Non-numeric string that's not in the allowed set
    throw std::runtime_error(
        fmt::format(
            "Invalid modification_times value '{}'. Use a number (0 to 4294967295) or one of: 'always', 'unlimited', 'infinite'.",
            valueStr));
  }

  // Handle numeric values
  if (modTimesPtr->isInt()) {
    auto intVal = modTimesPtr->asInt();
    if (intVal < 0 || intVal > std::numeric_limits<uint32_t>::max()) {
      throw std::out_of_range(
          fmt::format("modification_times value {} out of range", intVal));
    }
    return static_cast<uint32_t>(intVal);
  }

  throw std::runtime_error(
      "modification_times must be a number or one of: 'always', 'unlimited', 'infinite'.");
}

double parseRandomPercent(const folly::dynamic* randomPercentPtr) {
  if (!randomPercentPtr) {
    return kAlwaysPercent;
  }

  double randomPercent = 0.0;
  if (randomPercentPtr->isInt()) {
    randomPercent = static_cast<double>(randomPercentPtr->asInt());
  } else if (randomPercentPtr->isDouble()) {
    randomPercent = randomPercentPtr->asDouble();
  } else {
    throw std::runtime_error("random_percent must be a number.");
  }

  if (!std::isfinite(randomPercent) || randomPercent <= 0.0 ||
      randomPercent > 100.0) {
    throw std::out_of_range(
        fmt::format("random_percent value {} out of range", randomPercent));
  }
  return randomPercent;
}

bool randomPercentMatches(double randomPercent) {
  if (randomPercent >= kAlwaysPercent) {
    return true;
  }

  thread_local std::mt19937 randomGenerator(std::random_device{}());
  std::bernoulli_distribution shouldMatch(randomPercent / 100.0);
  return shouldMatch(randomGenerator);
}
} // namespace

bool HeaderEditModifierConfig::isNonNumericField(const std::string& fieldName) {
  static const std::unordered_set<std::string> kNonNumericFields = {
      "ack_request",
      "ackRequest",
      "flush_ack",
      "flushAck",
      "is_retransmit",
      "isRetransmit",
      "is_timeout",
      "isTimeout",
      "ack_status",
      "ackStatus",
  };
  return kNonNumericFields.contains(fieldName);
}

// ModifierFilter

ModifierConfig::ModifierFilter::ModifierFilter(
    const folly::dynamic* filterPtr) {
  if (!filterPtr) {
    throw std::runtime_error(
        "Modifier config has no filter. A modifier with no filter reaches "
        "every tx data packet; write \"filter\": {} to ask for that.");
  }

  if (!filterPtr->isObject()) {
    throw std::runtime_error("Malformed modifier filter");
  }

  // Parse and populate direction
  if (auto directionPtr = filterPtr->get_ptr("direction")) {
    auto directionStr = directionPtr->asString();
    folly::toLowerAscii(directionStr);
    if (directionStr == "tx") {
      direction = ModifierFilter::Direction::Tx;
    } else if (directionStr == "rx") {
      direction = ModifierFilter::Direction::Rx;
    } else {
      throw std::runtime_error(
          fmt::format("Unknown filter direction type {}", directionStr));
    }
  }

  // Parse and populate packet type
  if (auto pktTypePtr = filterPtr->get_ptr("packet_type")) {
    auto pktTypeStr = pktTypePtr->asString();
    folly::toLowerAscii(pktTypeStr);
    if (pktTypeStr == "data") {
      packetType = ModifierFilter::PacketType::Data;
    } else if (pktTypeStr == "ack") {
      packetType = ModifierFilter::PacketType::Ack;
    } else {
      throw std::runtime_error(
          fmt::format("Unknown filter packetType {}", pktTypeStr));
    }
  }

  if (auto randomPercentPtr = filterPtr->get_ptr("random_percent")) {
    randomPercent = parseRandomPercent(randomPercentPtr);
  }

  // Parse and populate fields
  if (auto fieldsPtr = filterPtr->get_ptr("fields")) {
    if (!fieldsPtr->isObject()) {
      throw std::runtime_error("Malformed filter fields");
    }

    for (const auto& [fieldName, fieldValue] : fieldsPtr->items()) {
      if (fieldName.asString() == "fsn_bit_vector") {
        if (!fieldValue.isArray()) {
          throw std::runtime_error(
              "Expecting an array for fields.fsn_bit_vector");
        }

        auto fsnBitArrayIndex = 0;
        for (const auto& val : fieldValue) {
          fields[fmt::format("fsn_bit_vector_{}", fsnBitArrayIndex)] =
              FieldConditions(FieldCondition(
                  static_cast<uint32_t>(
                      std::stoul(val.asString(), nullptr, 16))));
          fsnBitArrayIndex++;
        };
        if (fsnBitArrayIndex != 8) {
          throw std::runtime_error(
              "Expecting an array of eight 32bit hex values for fields.fsn_bit_vector");
        }

      } else if (fieldName.asString() == "remote_addr") {
        if (!fieldValue.isString()) {
          throw std::runtime_error("Expecting a string for fields.remote_addr");
        }
        auto addrStr = fieldValue.asString();
        auto maybeAddr = folly::IPAddress::tryFromString(addrStr);
        if (maybeAddr.hasError()) {
          throw std::runtime_error(
              fmt::format(
                  "Invalid IP address '{}' for fields.remote_addr", addrStr));
        }
        remoteAddr = maybeAddr.value();
      } else {
        fields[fieldName.asString()] = parseFieldConditions(fieldValue);
      }
    }
  }
}

ComparisonOperator ModifierConfig::ModifierFilter::parseOperator(
    const std::string& opStr) {
  std::string lowerOpStr = opStr;
  folly::toLowerAscii(lowerOpStr);
  if (lowerOpStr == "=" || lowerOpStr == "==") {
    return ComparisonOperator::Eq;
  } else if (lowerOpStr == "!=") {
    return ComparisonOperator::Ne;
  } else if (lowerOpStr == ">") {
    return ComparisonOperator::Gt;
  } else if (lowerOpStr == "<") {
    return ComparisonOperator::Lt;
  } else if (lowerOpStr == ">=") {
    return ComparisonOperator::Ge;
  } else if (lowerOpStr == "<=") {
    return ComparisonOperator::Le;
  } else if (lowerOpStr == "in") {
    return ComparisonOperator::In;
  } else if (lowerOpStr == "%" || lowerOpStr == "mod") {
    return ComparisonOperator::Mod;
  } else {
    throw std::runtime_error(
        fmt::format(
            "Unknown comparison operator '{}'. Valid operators: =, ==, !=, >, <, >=, <=, In, %, Mod",
            opStr));
  }
}

FieldCondition ModifierConfig::ModifierFilter::parseFieldCondition(
    const folly::dynamic& fieldValue) {
  if (fieldValue.isInt()) {
    return FieldCondition(static_cast<uint32_t>(fieldValue.asInt()));
  } else if (fieldValue.isObject()) {
    auto opPtr = fieldValue.get_ptr("op");
    auto valuePtr = fieldValue.get_ptr("value");

    if (!opPtr || !valuePtr) {
      throw std::runtime_error(
          "Field condition object must have 'op' and 'value' fields");
    }

    auto op = parseOperator(opPtr->asString());

    if (op == ComparisonOperator::In) {
      if (!valuePtr->isArray()) {
        throw std::runtime_error("'In' operator requires an array of values");
      }
      std::unordered_set<uint32_t> valueSet;
      for (const auto& item : *valuePtr) {
        if (!item.isInt()) {
          throw std::runtime_error(
              "All values in 'In' operator array must be integers");
        }
        valueSet.insert(static_cast<uint32_t>(item.asInt()));
      }
      if (valueSet.empty()) {
        throw std::runtime_error("'In' operator requires at least one value");
      }
      return FieldCondition(std::move(valueSet));
    }

    auto singleValue = static_cast<uint32_t>(valuePtr->asInt());

    if (op == ComparisonOperator::Mod) {
      if (singleValue == 0) {
        throw std::runtime_error(
            "'%' (Mod) operator requires a non-zero divisor");
      }
      uint32_t remainder = 0;
      if (auto remainderPtr = fieldValue.get_ptr("remainder")) {
        remainder = static_cast<uint32_t>(remainderPtr->asInt());
      }
      if (remainder >= singleValue) {
        throw std::runtime_error(
            fmt::format(
                "'%' (Mod) operator remainder ({}) must be less than divisor ({})",
                remainder,
                singleValue));
      }
      return FieldCondition(op, singleValue, remainder);
    }

    return FieldCondition(op, singleValue);
  } else {
    throw std::runtime_error(
        "Field value must be an integer or an object with 'op' and 'value'");
  }
}

FieldConditions ModifierConfig::ModifierFilter::parseFieldConditions(
    const folly::dynamic& fieldValue) {
  if (fieldValue.isArray()) {
    bool isInShorthand = true;
    for (const auto& item : fieldValue) {
      if (!item.isInt()) {
        isInShorthand = false;
        break;
      }
    }

    if (isInShorthand) {
      std::unordered_set<uint32_t> valueSet;
      for (const auto& item : fieldValue) {
        valueSet.insert(static_cast<uint32_t>(item.asInt()));
      }
      if (valueSet.empty()) {
        throw std::runtime_error(
            "Array shorthand for 'In' operator requires at least one value");
      }
      return FieldConditions(FieldCondition(std::move(valueSet)));
    } else {
      // Array of condition objects: AND logic
      std::vector<FieldCondition> conditions;
      for (const auto& item : fieldValue) {
        conditions.push_back(parseFieldCondition(item));
      }
      if (conditions.empty()) {
        throw std::runtime_error(
            "Field conditions array must have at least one condition");
      }
      return FieldConditions(std::move(conditions));
    }
  } else {
    return FieldConditions(parseFieldCondition(fieldValue));
  }
}

// ReorderModifierConfig::Distance

ReorderModifierConfig::Distance::Distance(const folly::dynamic* distancePtr) {
  if (!distancePtr) {
    throw std::runtime_error("Distance is not specified.");
  }
  if (!distancePtr->isObject()) {
    throw std::runtime_error("Malformed distance");
  }
  if (auto unitPtr = distancePtr->get_ptr("unit")) {
    auto unitStr = unitPtr->asString();
    folly::toLowerAscii(unitStr);
    if (unitStr == "time_us") {
      unit = ReorderModifierConfig::Distance::Unit::TimeUs;
    } else if (unitStr == "packet") {
      unit = ReorderModifierConfig::Distance::Unit::Packet;
    } else {
      throw std::runtime_error(
          fmt::format("Unknown distance unit {}", unitStr));
    }
  } else {
    throw std::runtime_error("Distance unit is not specified.");
  }
  if (auto valPtr = distancePtr->get_ptr("value")) {
    auto intVal = valPtr->asInt();
    if (intVal < 0 || intVal > std::numeric_limits<uint32_t>::max()) {
      throw std::out_of_range(
          fmt::format("Distance value {} out of range", intVal));
    }
    value = static_cast<uint32_t>(intVal);
  } else {
    throw std::runtime_error("Distance value is not specified.");
  }
}

std::unique_ptr<ModifierConfig> ModifierConfig::buildModifierConfigFromJson(
    const folly::dynamic& configJson) {
  std::optional<std::string> maybeModifierName;
  if (auto namePtr = configJson.get_ptr("name")) {
    maybeModifierName = namePtr->asString();
  }

  ModifierFilter modifierFilter(configJson.get_ptr("filter"));

  auto modificationTimes =
      parseModificationTimes(configJson.get_ptr("modification_times"));

  if (auto typePtr = configJson.get_ptr("type")) {
    auto typeStr = typePtr->asString();
    folly::toLowerAscii(typeStr);
    if (!maybeModifierName) {
      maybeModifierName = typeStr;
    }
    if (typeStr == "drop") {
      return std::make_unique<DropModifierConfig>(
          modifierFilter, modificationTimes, maybeModifierName);
    } else if (typeStr == "reorder") {
      return std::make_unique<ReorderModifierConfig>(
          modifierFilter,
          modificationTimes,
          ReorderModifierConfig::Distance(configJson.get_ptr("distance")),
          maybeModifierName);
    } else if (typeStr == "striped_message_wave") {
      auto getPositive = [&configJson](const char* name, uint64_t maximum) {
        auto* valuePtr = configJson.get_ptr(name);
        if (!valuePtr) {
          throw std::runtime_error(
              fmt::format(
                  "{} is not specified for striped_message_wave", name));
        }
        auto value = valuePtr->asInt();
        if (value <= 0 || static_cast<uint64_t>(value) > maximum) {
          throw std::out_of_range(
              fmt::format("striped_message_wave {} out of range", name));
        }
        return static_cast<uint64_t>(value);
      };
      std::optional<uint32_t> tailReleaseIntervalUs;
      if (configJson.get_ptr("tail_release_interval_us")) {
        tailReleaseIntervalUs = static_cast<uint32_t>(getPositive(
            "tail_release_interval_us", std::numeric_limits<uint32_t>::max()));
      }
      return std::make_unique<StripedMessageWaveModifierConfig>(
          modifierFilter,
          modificationTimes,
          static_cast<uint32_t>(
              getPositive("qp_count", std::numeric_limits<uint32_t>::max())),
          static_cast<uint16_t>(
              getPositive("tail_posn", std::numeric_limits<uint16_t>::max())),
          static_cast<uint32_t>(getPositive(
              "max_pending_packets", std::numeric_limits<uint32_t>::max())),
          static_cast<uint32_t>(getPositive(
              "release_interval_us", std::numeric_limits<uint32_t>::max())),
          maybeModifierName,
          tailReleaseIntervalUs);
    } else if (typeStr == "duplicate") {
      uint32_t delayUs = 0;
      if (auto delayPtr = configJson.get_ptr("delay_us")) {
        delayUs = static_cast<uint32_t>(delayPtr->asInt());
      } else {
        throw std::runtime_error(
            "delay_us is not specified for duplicate modifier.");
      }
      return std::make_unique<DuplicateModifierConfig>(
          modifierFilter, modificationTimes, delayUs, maybeModifierName);
    } else if (typeStr == "corrupt") {
      uint32_t positionToCorrupt = 0;
      if (auto posPtr = configJson.get_ptr("position_to_corrupt")) {
        positionToCorrupt = static_cast<uint32_t>(posPtr->asInt());
      }

      std::optional<uint8_t> valueToSet;
      if (auto valuePtr = configJson.get_ptr("value_to_set")) {
        uint32_t value = static_cast<uint32_t>(valuePtr->asInt());
        if (value > 255) {
          throw std::runtime_error(
              "value_to_set must be between 0 and 255 for corrupt modifier.");
        }
        valueToSet = static_cast<uint8_t>(value);
      }

      bool calculateNewIcrc = false;
      if (auto icrcPtr = configJson.get_ptr("calculate_new_icrc")) {
        calculateNewIcrc = icrcPtr->asBool();
      }

      return std::make_unique<CorruptModifierConfig>(
          modifierFilter,
          modificationTimes,
          positionToCorrupt,
          valueToSet,
          calculateNewIcrc,
          maybeModifierName);
    } else if (typeStr == "write_edit") {
      std::string target = "rkey";

      if (auto targetPtr = configJson.get_ptr("target")) {
        target = targetPtr->asString();
        folly::toLowerAscii(target);
        if (target != "rkey" && target != "address") {
          throw std::runtime_error(
              fmt::format(
                  "Unknown write_edit target '{}'. Valid values: 'rkey', 'address'",
                  target));
        }
      }

      std::optional<uint64_t> valueToSet;
      if (auto valuePtr = configJson.get_ptr("value_to_set")) {
        uint64_t value = static_cast<uint64_t>(valuePtr->asInt());
        valueToSet = value;
      }

      return std::make_unique<WriteHeaderEditModifierConfig>(
          modifierFilter,
          modificationTimes,
          target,
          valueToSet,
          maybeModifierName);
    } else if (typeStr == "header_edit") {
      HeaderEditModifierConfig::HeaderType headerType;
      if (auto headerTypePtr = configJson.get_ptr("header_type")) {
        auto headerTypeStr = headerTypePtr->asString();
        folly::toLowerAscii(headerTypeStr);
        if (headerTypeStr == "base") {
          headerType = HeaderEditModifierConfig::HeaderType::Base;
        } else if (headerTypeStr == "data") {
          headerType = HeaderEditModifierConfig::HeaderType::Data;
        } else if (
            headerTypeStr == "readwrite" || headerTypeStr == "read_write") {
          headerType = HeaderEditModifierConfig::HeaderType::ReadWrite;
        } else if (headerTypeStr == "ack") {
          headerType = HeaderEditModifierConfig::HeaderType::Ack;
        } else if (
            headerTypeStr == "rnrcancel" || headerTypeStr == "rnr_cancel") {
          headerType = HeaderEditModifierConfig::HeaderType::RnrCancel;
        } else {
          throw std::runtime_error(
              fmt::format(
                  "Unknown header_type '{}'. Valid values: 'base', 'data', 'readwrite', 'ack', 'rnrcancel'",
                  headerTypeStr));
        }
      } else {
        throw std::runtime_error(
            "header_type is not specified for header_edit modifier.");
      }

      std::string fieldName;
      if (auto fieldNamePtr = configJson.get_ptr("field_name")) {
        fieldName = fieldNamePtr->asString();
      } else {
        throw std::runtime_error(
            "field_name is not specified for header_edit modifier.");
      }

      std::optional<uint64_t> valueToSet;
      if (auto valuePtr = configJson.get_ptr("value_to_set")) {
        valueToSet = static_cast<uint64_t>(valuePtr->asInt());
      }

      std::optional<uint64_t> valueToSubtract;
      if (auto valuePtr = configJson.get_ptr("value_to_subtract")) {
        valueToSubtract = static_cast<uint64_t>(valuePtr->asInt());
      }

      if (valueToSet.has_value() && valueToSubtract.has_value()) {
        throw std::runtime_error(
            "header_edit takes value_to_set or value_to_subtract, not both.");
      }

      if (valueToSubtract.has_value() &&
          HeaderEditModifierConfig::isNonNumericField(fieldName)) {
        throw std::runtime_error(
            fmt::format(
                "header_edit value_to_subtract has no meaning for the flag or "
                "status field '{}'.",
                fieldName));
      }

      return std::make_unique<HeaderEditModifierConfig>(
          modifierFilter,
          modificationTimes,
          headerType,
          fieldName,
          valueToSet,
          maybeModifierName,
          valueToSubtract);
    } else if (typeStr == "truncate") {
      uint32_t payloadSizeBytes = 0;
      if (auto sizePtr = configJson.get_ptr("payload_size")) {
        if (!sizePtr->isInt()) {
          throw std::invalid_argument(
              fmt::format(
                  "payload_size must be an integer, got {}",
                  folly::toJson(*sizePtr)));
        }
        auto sizeVal = sizePtr->asInt();
        if (sizeVal < 0 || sizeVal > std::numeric_limits<uint32_t>::max()) {
          throw std::out_of_range(
              fmt::format("payload_size value {} out of range", sizeVal));
        }
        payloadSizeBytes = static_cast<uint32_t>(sizeVal);
      }
      return std::make_unique<TruncateModifierConfig>(
          modifierFilter,
          modificationTimes,
          payloadSizeBytes,
          maybeModifierName);
    } else if (typeStr == "markecn") {
      return std::make_unique<MarkModifierConfig>(
          modifierFilter, modificationTimes, maybeModifierName);
    } else if (typeStr == "pacing") {
      if (auto intervalPtr = configJson.get_ptr("interval_us")) {
        auto intVal = intervalPtr->asInt();
        if (intVal <= 0 || intVal > std::numeric_limits<uint32_t>::max()) {
          throw std::out_of_range(
              fmt::format("interval_us value {} out of range", intVal));
        }
        return std::make_unique<PacingModifierConfig>(
            modifierFilter,
            modificationTimes,
            static_cast<uint32_t>(intVal),
            maybeModifierName);
      } else {
        throw std::runtime_error(
            "interval_us is not specified for pacing modifier.");
      }
    } else {
      throw std::runtime_error(
          fmt::format("Unknown modifier type {}", typeStr));
    }
  } else {
    throw std::runtime_error("Modifier type is not specified.");
  }
}

std::vector<std::unique_ptr<ModifierConfig>>
ModifierConfig::buildModifierConfigsFromJson(const std::string& configJsonStr) {
  const auto configJson = folly::parseJson(configJsonStr);
  std::vector<std::unique_ptr<ModifierConfig>> configs;

  if (configJson.isArray()) {
    for (const auto& modifierJson : configJson) {
      VLOG(1) << fmt::format(
          "Parsing modifier config: {}", folly::toJson(modifierJson));
      configs.push_back(buildModifierConfigFromJson(modifierJson));
    }
  } else if (configJson.isObject()) {
    VLOG(1) << fmt::format(
        "Parsing modifier config: {}", folly::toJson(configJson));
    configs.push_back(buildModifierConfigFromJson(configJson));
  } else {
    throw std::runtime_error(
        "Config must be either a JSON object or an array of objects.");
  }

  return configs;
}

bool ModifierConfig::matchesFilter(
    const Packet& packet,
    const ModifierConfig::ModifierFilter::Direction& direction,
    const folly::SocketAddress& remoteAddr) const {
  if (filter.direction != direction) {
    return false;
  }

  if (filter.remoteAddr.has_value() &&
      remoteAddr.getIPAddress() != *filter.remoteAddr) {
    return false;
  }

  const auto& baseHeader = packet.getBaseHeader();
  if (baseHeader.getOpcode().isData()) {
    if (filter.packetType != ModifierFilter::PacketType::Data) {
      return false;
    }
    const auto* dataPacket = dynamic_cast<const DataPacket*>(&packet);
    CHECK(dataPacket)
        << "Opcode isData() but packet is not a DataPacket - deserialization bug";
    const auto& dataHeader = dataPacket->getDataHeader();

    for (const auto& [fName, condition] : filter.fields) {
      if (fName == "src_qp" && !condition.evaluate(baseHeader.getSrcQp())) {
        return false;
      } else if (
          fName == "dst_qp" && !condition.evaluate(baseHeader.getDstQp())) {
        return false;
      } else if (
          (fName == "flow_id" || fName == "path_id") &&
          !condition.evaluate(baseHeader.getFlowId())) {
        return false;
      } else if (fName == "fsn" && !condition.evaluate(dataHeader.getFsn())) {
        return false;
      } else if (fName == "una" && !condition.evaluate(dataHeader.getUna())) {
        return false;
      } else if (fName == "posn" && !condition.evaluate(dataHeader.getPosn())) {
        return false;
      } else if (fName == "csn" && !condition.evaluate(dataHeader.getCsn())) {
        return false;
      } else if (fName == "msn" && !condition.evaluate(dataHeader.getMsn())) {
        return false;
      }
    }

  } else if (baseHeader.getOpcode().isAnAck()) {
    if (filter.packetType != ModifierFilter::PacketType::Ack) {
      return false;
    }
    const auto* ackPacket = dynamic_cast<const AckPacket*>(&packet);
    CHECK(ackPacket)
        << "Opcode isAnAck() but packet is not an AckPacket - deserialization bug";
    const auto& ackHeader = ackPacket->getAckHeader();

    for (const auto& [fName, condition] : filter.fields) {
      if (fName == "src_qp" && !condition.evaluate(baseHeader.getSrcQp())) {
        return false;
      } else if (
          fName == "dst_qp" && !condition.evaluate(baseHeader.getDstQp())) {
        return false;
      } else if (
          (fName == "flow_id" || fName == "path_id") &&
          !condition.evaluate(baseHeader.getFlowId())) {
        return false;
      } else if (
          fName == "cumulative_fsn" &&
          !condition.evaluate(ackHeader.getCumulativeFsn())) {
        return false;
      } else if (
          fName == "max_rcvd_fsn" &&
          !condition.evaluate(ackHeader.getMaxRcvdFsn())) {
        return false;
      } else if (
          fName == "cumul_resp_delivered_msn" &&
          !condition.evaluate(ackHeader.getCumulRespDeliveredMsn())) {
        return false;
      } else if (
          fName == "ack_status" &&
          !condition.evaluate(
              static_cast<uint32_t>(ackHeader.getAckStatus()))) {
        return false;
      } else if (fName.starts_with("fsn_bit_vector_")) {
        auto fsnBitArrayIndex =
            std::stoi(fName.substr(fName.find_last_of('_') + 1), nullptr, 10);
        if (!condition.evaluate(
                ackHeader.getFsnBitVector().getArray()[fsnBitArrayIndex])) {
          return false;
        }
      }
    }
  }
  return randomPercentMatches(filter.randomPercent);
}

} // namespace libsoftmetaroce
