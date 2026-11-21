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

#include <folly/Synchronized.h>

#include <libsoftmetaroce/congestion_control/CongestionController.h>
#include <mutex>

#include <memory>
#include <optional>
#include <utility>

namespace libsoftmetaroce {

// Serializes one QP's congestion controller. Innermost lock; see
// locking-discipline.md.
class SynchronizedCongestionController : public CongestionController {
 public:
  explicit SynchronizedCongestionController(
      std::unique_ptr<CongestionController> inner)
      : box_{Box{std::move(inner)}} {}

  void onPacketSent(uint16_t flowId) override {
    box_.lock()->mut().onPacketSent(flowId);
  }

  uint32_t onPacketSentAndGetQwnd(uint16_t flowId) override {
    const auto locked = box_.lock();
    locked->mut().onPacketSent(flowId);
    return locked->get().getQwnd();
  }

  AckResult onPacketAck(uint16_t flowId, const AckEvent& ack) override {
    return box_.lock()->mut().onPacketAck(flowId, ack);
  }

  void onPacketLoss(uint16_t flowId, const LossEvent& loss) override {
    box_.lock()->mut().onPacketLoss(flowId, loss);
  }

  void onFlowAdded(uint16_t flowId) override {
    box_.lock()->mut().onFlowAdded(flowId);
  }

  void onFlowDisabled(uint16_t flowId) override {
    box_.lock()->mut().onFlowDisabled(flowId);
  }

  void setMTU(uint32_t mtu) override {
    box_.lock()->mut().setMTU(mtu);
  }

  void setPacketsPerVisit(uint16_t packetsPerVisit) override {
    box_.lock()->mut().setPacketsPerVisit(packetsPerVisit);
  }

  void setPortPairCount(uint16_t portPairCount) override {
    box_.lock()->mut().setPortPairCount(portPairCount);
  }

  [[nodiscard]] uint16_t getAvailableWindowSize(
      uint16_t flowId) const override {
    return box_.lock()->get().getAvailableWindowSize(flowId);
  }

  [[nodiscard]] uint16_t getFwnd(uint16_t flowId) const override {
    return box_.lock()->get().getFwnd(flowId);
  }

  [[nodiscard]] uint32_t getQwnd() const override {
    return box_.lock()->get().getQwnd();
  }

  [[nodiscard]] bool shouldAdvanceFlowAfterSend(
      uint16_t flowId) const override {
    return box_.lock()->get().shouldAdvanceFlowAfterSend(flowId);
  }

  [[nodiscard]] std::optional<NextSend> flowForNextSend() const override {
    return box_.lock()->get().flowForNextSend();
  }

 private:
  struct Box {
    std::unique_ptr<CongestionController> inner;

    // get() and mut() keep const-ness with the compiler: the lock alone hands
    // back a mutable controller even from a const method.
    CongestionController& mut() {
      return *inner;
    }
    const CongestionController& get() const {
      return *inner;
    }
  };

  // Plain mutex: sections are a few arithmetic operations, and a shared lock
  // measured no better on SendPathBench.
  folly::Synchronized<Box, std::mutex> box_;
};

} // namespace libsoftmetaroce
