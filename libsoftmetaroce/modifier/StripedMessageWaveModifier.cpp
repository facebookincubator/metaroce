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

#include <libsoftmetaroce/modifier/StripedMessageWaveModifier.h>

#include <glog/logging.h>

namespace libsoftmetaroce {

namespace {
constexpr uint32_t kRecoveryIntervalMultiplier = 2;
// Bounds both early timer arming and the final spin on the release thread.
constexpr uint64_t kMaxPacingWakeLeadNs = 20'000;

void sendWavePacketNoThrow(
    StripedMessageWaveState::DelayedSend& send,
    const char* queueName) noexcept {
  try {
    send();
  } catch (const std::exception& ex) {
    LOG(ERROR) << "StripedMessageWaveModifier: " << queueName
               << " packet send failed: " << ex.what();
  } catch (...) {
    LOG(ERROR) << "StripedMessageWaveModifier: " << queueName
               << " packet send failed with an unknown exception";
  }
}
} // namespace

StripedMessageWaveModifier::StripedMessageWaveModifier(
    std::unique_ptr<ModifierConfig> config)
    : Modifier(std::move(config)) {
  config_ =
      dynamic_cast<StripedMessageWaveModifierConfig*>(&getModifierConfig());
  if (!config_) {
    throw std::runtime_error(
        "StripedMessageWaveModifier: invalid modifier config");
  }
  if (actsOnReceive()) {
    throw std::runtime_error("StripedMessageWaveModifier supports tx only");
  }
  state_ = config_->state;
  if (!state_) {
    state_ = std::make_shared<StripedMessageWaveState>();
  }
  sendTarget_ = std::make_shared<StripedMessageWaveSendTarget>();
}

StripedMessageWaveModifier::~StripedMessageWaveModifier() {
  invalidateSendTarget();
}

void StripedMessageWaveModifier::stopTimers() {
  invalidateSendTarget();
}

void StripedMessageWaveModifier::invalidateSendTarget() {
  std::lock_guard<std::mutex> lock(sendTarget_->mutex);
  sendTarget_->sendPacketFn = nullptr;
}

ssize_t StripedMessageWaveModifier::modifyTxPacket(
    SendPacketFn& sendPacketFn,
    std::unique_ptr<TxPacketWithMetadata> txPacketData) {
  const auto size = txPacketData->packet->getSizeBytes();
  txPacketData->packet->ownPayload();
  enqueuePacket(sendPacketFn, std::move(txPacketData));
  return size;
}

void StripedMessageWaveModifier::modifyRxPacket(
    ReceivePacketFn& receivePacketFn,
    std::unique_ptr<IngressPacket> ingressPacket) {
  receivePacketFn(std::move(ingressPacket));
}

size_t StripedMessageWaveModifier::numQueuedPackets() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->packets.size() +
      state_->scheduledPackets.load(std::memory_order_relaxed) +
      state_->recoveryPackets.load(std::memory_order_relaxed);
}

bool StripedMessageWaveModifier::waitForDrainForTest(
    std::chrono::milliseconds timeout) const {
  std::unique_lock<std::mutex> lock(state_->mutex);
  return state_->scheduledPacketsCv.wait_for(lock, timeout, [this] {
    return state_->packets.empty() &&
        state_->scheduledPackets.load(std::memory_order_relaxed) == 0 &&
        state_->recoveryPackets.load(std::memory_order_relaxed) == 0;
  });
}

void StripedMessageWaveModifier::enqueuePacket(
    SendPacketFn& sendPacketFn,
    std::unique_ptr<TxPacketWithMetadata> txPacketData) {
  auto& state = *CHECK_NOTNULL(state_.get());
  std::unique_lock<std::mutex> lock(state.mutex);
  auto sendTarget = sendTarget_;
  CHECK(sendTarget);
  {
    std::lock_guard<std::mutex> sendLock(sendTarget->mutex);
    if (sendTarget->sendPacketFn) {
      CHECK_EQ(sendTarget->sendPacketFn, &sendPacketFn);
    } else {
      sendTarget->sendPacketFn = &sendPacketFn;
    }
  }
  auto* dataPacket = dynamic_cast<DataPacket*>(txPacketData->packet.get());
  if (!dataPacket) {
    auto bypassPackets = enterBypass("matched packet is not a data packet");
    lock.unlock();
    sendBypassPackets(std::move(bypassPackets));
    sendPacketFn(
        txPacketData->remoteSocketAddr,
        std::move(txPacketData->packet),
        std::nullopt);
    return;
  }
  if (state.bypass) {
    lock.unlock();
    sendPacketFn(
        txPacketData->remoteSocketAddr,
        std::move(txPacketData->packet),
        std::nullopt);
    return;
  }

  const auto& dataHeader = dataPacket->getDataHeader();
  const uint32_t qp = dataPacket->getBaseHeader().getSrcQp();
  const uint32_t msn = dataHeader.getMsn();
  const uint16_t posn = dataHeader.getPosn();
  if (posn > config_->tailPosn ||
      (state.waveQps.size() == config_->qpCount &&
       !state.waveQps.contains(qp))) {
    auto bypassPackets =
        enterBypass("packet is outside the configured message wave");
    lock.unlock();
    sendBypassPackets(std::move(bypassPackets));
    sendPacketFn(
        txPacketData->remoteSocketAddr,
        std::move(txPacketData->packet),
        std::nullopt);
    return;
  }

  const MessageKey key{qp, msn};
  if (state.scheduledMessages.contains(key)) {
    return;
  }
  if (state.completedMessages.contains(key)) {
    if (dataHeader.getIsRetransmit()) {
      const StripedMessageWaveState::FragmentKey fragmentKey{qp, msn, posn};
      if (state.recoveryQueued.insert(fragmentKey).second) {
        state.recoveryPackets.fetch_add(1, std::memory_order_relaxed);
        state.recoverySends.push_back(
            StripedMessageWaveState::RecoverySend{
                fragmentKey,
                std::make_unique<
                    StripedMessageWaveState::
                        DelayedSend>([sendTarget = std::move(sendTarget),
                                      address = txPacketData->remoteSocketAddr,
                                      packet = std::move(
                                          txPacketData->packet)]() mutable {
                  std::lock_guard<std::mutex> lock(sendTarget->mutex);
                  if (sendTarget->sendPacketFn) {
                    (*sendTarget->sendPacketFn)(
                        address, std::move(packet), std::nullopt);
                  } else {
                    LOG_FIRST_N(WARNING, 1)
                        << "StripedMessageWaveModifier: dropping recovery packet after "
                           "send target teardown";
                  }
                })});
        armRecoveryRelease();
      }
    }
    return;
  }
  auto [messageIt, inserted] = state.pendingMessages.try_emplace(
      key, static_cast<size_t>(config_->tailPosn) + 1);
  auto& message = messageIt->second;
  if (message.packetIds[posn] != 0) {
    const bool qpSetComplete = state.waveQps.size() == config_->qpCount;
    if (dataHeader.getIsRetransmit() && !qpSetComplete) {
      auto bypassPackets = enterBypass("retransmit reached a pending message");
      lock.unlock();
      sendBypassPackets(std::move(bypassPackets));
    }
    return;
  }
  if (state.packets.size() >= config_->maxPendingPackets) {
    if (inserted) {
      state.pendingMessages.erase(messageIt);
    }
    auto bypassPackets =
        enterBypass("max_pending_packets reached before a wave completed");
    lock.unlock();
    sendBypassPackets(std::move(bypassPackets));
    sendPacketFn(
        txPacketData->remoteSocketAddr,
        std::move(txPacketData->packet),
        std::nullopt);
    return;
  }

  state.waveQps.insert(qp);
  const uint64_t packetId = state.nextPacketId++;
  message.packetIds[posn] = packetId;
  ++message.received;
  state.packets.emplace(
      packetId,
      std::make_unique<StripedMessageWaveState::
                           DelayedSend>([sendTarget = std::move(sendTarget),
                                         address =
                                             txPacketData->remoteSocketAddr,
                                         packet = std::move(
                                             txPacketData->packet)]() mutable {
        std::lock_guard<std::mutex> lock(sendTarget->mutex);
        if (sendTarget->sendPacketFn) {
          (*sendTarget->sendPacketFn)(address, std::move(packet), std::nullopt);
        } else {
          LOG_FIRST_N(WARNING, 1)
              << "StripedMessageWaveModifier: dropping scheduled packet after send "
                 "target teardown";
        }
      }));
  if (message.received == message.packetIds.size()) {
    state.readyMessagesByQp[qp].push_back(msn);
  }
  releaseReadyMessageWaves();
  if (!state.bypass && state.packets.size() >= config_->maxPendingPackets) {
    auto bypassPackets =
        enterBypass("max_pending_packets filled without a complete wave");
    lock.unlock();
    sendBypassPackets(std::move(bypassPackets));
  }
}

void StripedMessageWaveModifier::releaseReadyMessageWaves() {
  auto& state = *CHECK_NOTNULL(state_.get());
  if (state.waveQps.size() != config_->qpCount) {
    return;
  }
  while (std::all_of(
      state.waveQps.begin(), state.waveQps.end(), [&state](uint32_t qp) {
        auto it = state.readyMessagesByQp.find(qp);
        return it != state.readyMessagesByQp.end() && !it->second.empty();
      })) {
    std::vector<MessageKey> wave;
    wave.reserve(state.waveQps.size());
    for (uint32_t qp : state.waveQps) {
      wave.emplace_back(qp, state.readyMessagesByQp.at(qp).front());
    }
    for (uint16_t posn = 0; posn < config_->tailPosn; ++posn) {
      for (const auto& key : wave) {
        queueWavePacket(
            key,
            state.pendingMessages.at(key).packetIds[posn],
            config_->releaseIntervalUs);
      }
    }
    for (const auto& key : wave) {
      queueWavePacket(
          key,
          state.pendingMessages.at(key).packetIds[config_->tailPosn],
          config_->tailReleaseIntervalUs.value_or(config_->releaseIntervalUs));
      state.readyMessagesByQp.at(key.first).pop_front();
      state.pendingMessages.erase(key);
    }
    ++state.releasedWaves;
    LOG(INFO) << "StripedMessageWaveModifier: released striped_message_wave="
              << state.releasedWaves << " qps=" << wave.size()
              << " interval_us=" << config_->releaseIntervalUs
              << " pending_packets=" << state.packets.size()
              << " scheduled_packets="
              << state.scheduledPackets.load(std::memory_order_relaxed);
  }
  armWaveRelease();
}

void StripedMessageWaveModifier::queueWavePacket(
    const MessageKey& key,
    uint64_t packetId,
    uint32_t releaseIntervalUs) {
  auto& state = *CHECK_NOTNULL(state_.get());
  auto it = state.packets.find(packetId);
  CHECK(it != state.packets.end());
  auto send = std::move(it->second);
  state.packets.erase(it);
  ++state.scheduledMessages[key];
  state.scheduledPackets.fetch_add(1, std::memory_order_relaxed);
  const size_t historyLimit = std::max<size_t>(
      1,
      config_->maxPendingPackets /
          (static_cast<size_t>(config_->tailPosn) + 1));
  state.scheduledSends.push_back(
      StripedMessageWaveState::ScheduledSend{
          key, std::move(send), historyLimit, releaseIntervalUs});
}

void StripedMessageWaveModifier::armWaveRelease() {
  auto& state = *CHECK_NOTNULL(state_.get());
  if (state.releaseArmed || state.scheduledSends.empty()) {
    return;
  }
  state.releaseArmed = true;
  auto* statePtr = &state;
  const uint64_t notBeforeNs =
      getNowNs64() + static_cast<uint64_t>(config_->releaseIntervalUs) * 1000;
  scheduleWavePacket(statePtr, notBeforeNs, config_->releaseIntervalUs);
}

void StripedMessageWaveModifier::scheduleWavePacket(
    StripedMessageWaveState* state,
    uint64_t notBeforeNs,
    uint32_t releaseIntervalUs) {
  const uint64_t wakeLeadNs =
      static_cast<uint64_t>(releaseIntervalUs) * 1000 > kMaxPacingWakeLeadNs
      ? kMaxPacingWakeLeadNs
      : 0;
  state->releaseQueue.scheduleAt(
      notBeforeNs - wakeLeadNs,
      [state, notBeforeNs] { dispatchWavePacket(state, notBeforeNs); });
}

void StripedMessageWaveModifier::armRecoveryRelease() {
  auto& state = *CHECK_NOTNULL(state_.get());
  if (state.recoveryArmed || state.recoverySends.empty()) {
    return;
  }
  state.recoveryArmed = true;
  const uint64_t recoveryIntervalUs =
      static_cast<uint64_t>(config_->releaseIntervalUs) *
      kRecoveryIntervalMultiplier;
  auto* statePtr = &state;
  const uint64_t notBeforeNs = getNowNs64() + recoveryIntervalUs * 1000;
  state.recoveryQueue.scheduleAt(
      notBeforeNs, [statePtr, recoveryIntervalUs, notBeforeNs] {
        dispatchRecoveryPacket(statePtr, recoveryIntervalUs, notBeforeNs);
      });
}

void StripedMessageWaveModifier::dispatchRecoveryPacket(
    StripedMessageWaveState* state,
    uint64_t recoveryIntervalUs,
    uint64_t notBeforeNs) {
  if (getNowNs64() < notBeforeNs) {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->stopping) {
      state->recoveryArmed = false;
    } else {
      state->recoveryQueue.scheduleAt(
          notBeforeNs, [state, recoveryIntervalUs, notBeforeNs] {
            dispatchRecoveryPacket(state, recoveryIntervalUs, notBeforeNs);
          });
    }
    return;
  }
  StripedMessageWaveState::RecoverySend recovery;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    CHECK(!state->recoverySends.empty());
    recovery = std::move(state->recoverySends.front());
    state->recoverySends.pop_front();
  }
  sendWavePacketNoThrow(*recovery.send, "recovery");
  state->recoveryPackets.fetch_sub(1, std::memory_order_relaxed);

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->recoveryQueued.erase(recovery.key);
    if (state->recoverySends.empty() || state->stopping) {
      state->recoveryArmed = false;
    } else {
      const uint64_t nextNotBeforeNs = getNowNs64() + recoveryIntervalUs * 1000;
      state->recoveryQueue.scheduleAt(
          nextNotBeforeNs, [state, recoveryIntervalUs, nextNotBeforeNs] {
            dispatchRecoveryPacket(state, recoveryIntervalUs, nextNotBeforeNs);
          });
    }
    if (state->scheduledPackets.load(std::memory_order_relaxed) == 0 &&
        state->recoveryPackets.load(std::memory_order_relaxed) == 0) {
      state->scheduledPacketsCv.notify_all();
    }
  }
}

void StripedMessageWaveModifier::dispatchWavePacket(
    StripedMessageWaveState* state,
    uint64_t notBeforeNs) {
  // The steady-clock bound limits spinning if the protocol clock moves back.
  const uint64_t spinUntilNs = steadyNowNs() + kMaxPacingWakeLeadNs;
  while (getNowNs64() < notBeforeNs && steadyNowNs() < spinUntilNs) {
  }
  if (getNowNs64() < notBeforeNs) {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->stopping) {
      state->releaseArmed = false;
    } else {
      state->releaseQueue.scheduleAt(notBeforeNs, [state, notBeforeNs] {
        dispatchWavePacket(state, notBeforeNs);
      });
    }
    return;
  }
  StripedMessageWaveState::ScheduledSend scheduled;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    CHECK(!state->scheduledSends.empty());
    scheduled = std::move(state->scheduledSends.front());
    state->scheduledSends.pop_front();
  }
  sendWavePacketNoThrow(*scheduled.send, "scheduled");
  const uint64_t sentAtNs = getNowNs64();
  finishScheduledPacket(state, scheduled.key, scheduled.historyLimit);

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->scheduledSends.empty() || state->stopping) {
      state->releaseArmed = false;
    } else {
      const uint64_t nextNotBeforeNs =
          sentAtNs + static_cast<uint64_t>(scheduled.releaseIntervalUs) * 1000;
      scheduleWavePacket(state, nextNotBeforeNs, scheduled.releaseIntervalUs);
    }
  }
}

void StripedMessageWaveModifier::finishScheduledPacket(
    StripedMessageWaveState* state,
    const MessageKey& key,
    size_t historyLimit) {
  std::lock_guard<std::mutex> lock(state->mutex);
  state->scheduledPackets.fetch_sub(1, std::memory_order_relaxed);
  auto scheduled = state->scheduledMessages.find(key);
  CHECK(scheduled != state->scheduledMessages.end());
  CHECK_GT(scheduled->second, 0);
  if (--scheduled->second != 0) {
    return;
  }
  state->scheduledMessages.erase(scheduled);
  state->completedMessages.insert(key);
  state->completedMessageOrder.push_back(key);
  if (state->completedMessageOrder.size() > historyLimit) {
    state->completedMessages.erase(state->completedMessageOrder.front());
    state->completedMessageOrder.pop_front();
  }
  if (state->scheduledPackets.load(std::memory_order_relaxed) == 0 &&
      state->recoveryPackets.load(std::memory_order_relaxed) == 0) {
    state->scheduledPacketsCv.notify_all();
  }
}

StripedMessageWaveState::DelayedSends StripedMessageWaveModifier::enterBypass(
    const std::string& reason) {
  auto& state = *CHECK_NOTNULL(state_.get());
  LOG(INFO)
      << "StripedMessageWaveModifier: striped_message_wave fallback reason="
      << reason << " pending_packets=" << state.packets.size()
      << " observed_qps=" << state.waveQps.size();
  StripedMessageWaveState::DelayedSends packets;
  packets.reserve(state.packets.size());
  for (auto& [_, send] : state.packets) {
    packets.push_back(std::move(send));
  }
  state.packets.clear();
  state.pendingMessages.clear();
  state.readyMessagesByQp.clear();
  state.waveQps.clear();
  state.completedMessages.clear();
  state.completedMessageOrder.clear();
  state.bypass = true;
  return packets;
}

void StripedMessageWaveModifier::sendBypassPackets(
    StripedMessageWaveState::DelayedSends packets) {
  for (auto& send : packets) {
    sendWavePacketNoThrow(*send, "bypass");
  }
}

} // namespace libsoftmetaroce
