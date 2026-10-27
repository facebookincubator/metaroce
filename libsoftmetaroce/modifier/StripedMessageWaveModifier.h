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

#include <folly/Function.h>
#include <libsoftmetaroce/DeadlineQueue.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <tuple>
#include <vector>

namespace libsoftmetaroce {

struct StripedMessageWaveState {
  using DelayedSend = folly::Function<void()>;
  using DelayedSends = std::vector<std::unique_ptr<DelayedSend>>;
  using MessageKey = std::pair<uint32_t, uint32_t>;
  using FragmentKey = std::tuple<uint32_t, uint32_t, uint16_t>;

  struct PendingMessage {
    explicit PendingMessage(size_t numFragments) : packetIds(numFragments, 0) {}

    std::vector<uint64_t> packetIds;
    size_t received{0};
  };

  struct ScheduledSend {
    MessageKey key;
    std::unique_ptr<DelayedSend> send;
    size_t historyLimit{0};
    uint32_t releaseIntervalUs{0};
  };

  struct RecoverySend {
    FragmentKey key;
    std::unique_ptr<DelayedSend> send;
    uint64_t releaseIntervalUs{0};
  };

  StripedMessageWaveState() = default;
  StripedMessageWaveState(const StripedMessageWaveState&) = delete;
  StripedMessageWaveState& operator=(const StripedMessageWaveState&) = delete;
  StripedMessageWaveState(StripedMessageWaveState&&) = delete;
  StripedMessageWaveState& operator=(StripedMessageWaveState&&) = delete;

  ~StripedMessageWaveState() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      stopping = true;
    }
    releaseQueue.stop();
  }

  // Runtime state.
  std::mutex mutex;
  std::map<MessageKey, PendingMessage> pendingMessages;
  std::map<uint32_t, std::deque<uint32_t>> readyMessagesByQp;
  std::set<uint32_t> waveQps;
  std::map<MessageKey, size_t> scheduledMessages;
  std::set<MessageKey> completedMessages;
  std::deque<MessageKey> completedMessageOrder;
  std::map<uint64_t, std::unique_ptr<DelayedSend>> packets;
  std::deque<ScheduledSend> scheduledSends;
  std::set<FragmentKey> recoveryQueued;
  std::deque<RecoverySend> recoverySends;
  uint64_t nextPacketId{1};
  uint64_t releasedWaves{0};
  std::atomic<size_t> scheduledPackets{0};
  std::atomic<size_t> recoveryPackets{0};
  bool lastSendWasRecovery{false};
  bool releaseArmed{false};
  bool stopping{false};
  bool bypass{false};

  // Test-only support.
  std::condition_variable scheduledPacketsCv;

  // Declared after callback state so scheduler destruction happens first.
  DeadlineQueue releaseQueue{"lsmrStripe"};
};

struct StripedMessageWaveSendTarget {
  std::mutex mutex;
  SendPacketFn* sendPacketFn{nullptr};
};

class StripedMessageWaveModifier : public Modifier {
 public:
  explicit StripedMessageWaveModifier(std::unique_ptr<ModifierConfig> config);
  ~StripedMessageWaveModifier() override;
  StripedMessageWaveModifier(const StripedMessageWaveModifier&) = delete;
  StripedMessageWaveModifier& operator=(const StripedMessageWaveModifier&) =
      delete;
  StripedMessageWaveModifier(StripedMessageWaveModifier&&) = delete;
  StripedMessageWaveModifier& operator=(StripedMessageWaveModifier&&) = delete;

  ssize_t modifyTxPacket(
      SendPacketFn& sendPacketFn,
      std::unique_ptr<TxPacketWithMetadata> txPacketData) override;

  void modifyRxPacket(
      ReceivePacketFn& receivePacketFn,
      std::unique_ptr<IngressPacket> ingressPacket) override;

  void onPacketSent() override {}
  void stopTimers() override;

  [[nodiscard]] size_t numQueuedPackets() const;
  bool waitForDrainForTest(std::chrono::milliseconds timeout) const;

 private:
  using MessageKey = StripedMessageWaveState::MessageKey;

  void enqueuePacket(
      SendPacketFn& sendPacketFn,
      std::unique_ptr<TxPacketWithMetadata> txPacketData);
  void releaseReadyMessageWaves();
  StripedMessageWaveState::DelayedSends enterBypass(const std::string& reason);
  static void sendBypassPackets(StripedMessageWaveState::DelayedSends packets);
  void invalidateSendTarget();
  static void finishScheduledPacket(
      StripedMessageWaveState* state,
      const MessageKey& key,
      size_t historyLimit);
  void armWaveRelease();
  static void dispatchWavePacket(
      StripedMessageWaveState* state,
      uint64_t notBeforeNs);
  static void scheduleWavePacket(
      StripedMessageWaveState* state,
      uint64_t notBeforeNs,
      uint64_t releaseIntervalUs);
  void queueWavePacket(
      const MessageKey& key,
      uint64_t packetId,
      uint32_t releaseIntervalUs);

  // Configuration.
  const StripedMessageWaveModifierConfig* config_{nullptr};

  // Runtime state.
  std::shared_ptr<StripedMessageWaveState> state_;
  std::shared_ptr<StripedMessageWaveSendTarget> sendTarget_;
};

} // namespace libsoftmetaroce
