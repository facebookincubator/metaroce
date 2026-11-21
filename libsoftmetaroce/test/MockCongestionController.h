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

#include <folly/portability/GMock.h>
#include <libsoftmetaroce/congestion_control/CongestionController.h>
#include <optional>

namespace libsoftmetaroce {

// One second in nanoseconds (for RTT sanity checks in tests)
constexpr uint32_t kOneSecondNs = 1'000'000'000;

class MockCongestionController : public CongestionController {
 public:
  explicit MockCongestionController() = default;
  ~MockCongestionController() override = default;
  MOCK_METHOD(void, onPacketSent, (uint16_t flowId));
  MOCK_METHOD(AckResult, onPacketAck, (uint16_t flowId, const AckEvent& ack));
  MOCK_METHOD(void, onPacketLoss, (uint16_t flowId, const LossEvent& loss));
  MOCK_METHOD(void, onFlowAdded, (uint16_t flowId));
  MOCK_METHOD(void, onFlowDisabled, (uint16_t flowId));
  MOCK_METHOD(
      uint16_t,
      getAvailableWindowSize,
      (uint16_t flowId),
      (const, override));
  // Defaults to nullopt (the base behavior), so only a test that stubs it makes
  // the controller own flow selection.
  MOCK_METHOD(std::optional<NextSend>, flowForNextSend, (), (const, override));
  MOCK_METHOD(uint16_t, getFwnd, (uint16_t flowId), (const, override));
  MOCK_METHOD(uint32_t, getQwnd, (), (const, override));
  MOCK_METHOD(void, setPortPairCount, (uint16_t portPairCount), (override));
};

} // namespace libsoftmetaroce
