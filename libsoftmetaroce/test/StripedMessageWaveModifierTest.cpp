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

#include <folly/IPAddress.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/modifier/StripedMessageWaveModifier.h>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <tuple>
#include <vector>

using namespace testing;
namespace libsoftmetaroce {

namespace {
constexpr uint32_t kDefaultDstQp = 5;
constexpr uint32_t kDefaultFlowId = 1;
constexpr uint32_t kDefaultUna = 5;
constexpr uint32_t kDefaultCsn = 1;
constexpr uint32_t kDefaultTimestamp = 0;
constexpr uint8_t kDefaultVersion = 0;
const auto kDefaultRemoteAddr = folly::SocketAddress("fc00::1", 2766);

BaseHeader createBaseHeader(
    BaseHeader::Opcode::OperationType opType,
    uint32_t srcQp) {
  return BaseHeader::BaseHeaderBuilder()
      .setOpcode(BaseHeader::Opcode(opType))
      .setVersion(kDefaultVersion)
      .setFlowId(kDefaultFlowId)
      .setDstQp(kDefaultDstQp)
      .setSrcQp(srcQp)
      .build();
}

DataHeader createDataHeader(uint32_t fsn, uint32_t msn, uint16_t posn) {
  return DataHeader::DataHeaderBuilder()
      .setFsn(fsn)
      .setUna(kDefaultUna)
      .setAckRequest(true)
      .setPosn(posn)
      .setMsn(msn)
      .setCsn(kDefaultCsn)
      .setTimestamp(kDefaultTimestamp)
      .build();
}
} // namespace

TEST(
    StripedMessageWaveModifierTest,
    TxMessageWaveStripesIndependentMsnAndAlignsTail) {
  constexpr uint32_t kNumQps = 4;
  constexpr uint16_t kNumPosns = 8;
  constexpr uint16_t kTailPosn = kNumPosns - 1;
  constexpr std::array<uint32_t, kNumQps> kMsns = {62456, 6786, 38564, 6947};
  const auto jsonStr = R"({
        "name": "aligned_tail_fragments",
        "type": "striped_message_wave",
        "filter": {
          "direction": "tx",
          "packet_type": "data"
        },
        "qp_count": 4,
        "tail_posn": 7,
        "max_pending_packets": 32,
        "release_interval_us": 1000
      })";
  auto waveState = std::make_shared<StripedMessageWaveState>();
  std::array<std::unique_ptr<StripedMessageWaveModifier>, kNumQps> modifiers;
  for (auto& modifier : modifiers) {
    auto config =
        ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
    auto* waveConfig =
        dynamic_cast<StripedMessageWaveModifierConfig*>(config.get());
    ASSERT_NE(waveConfig, nullptr);
    waveConfig->state = waveState;
    modifier = std::make_unique<StripedMessageWaveModifier>(std::move(config));
  }

  using Fragment = std::tuple<uint32_t, uint32_t, uint16_t>;
  std::vector<Fragment> delivered;
  std::vector<uint64_t> deliveredAtNs;
  std::mutex deliveredMutex;
  std::condition_variable deliveredCv;
  std::atomic<bool> wrongCallback{false};
  std::array<SendPacketFn, kNumQps> sendFns;
  for (uint32_t qp = 0; qp < kNumQps; ++qp) {
    sendFns[qp] = [&delivered,
                   &deliveredAtNs,
                   &deliveredMutex,
                   &deliveredCv,
                   &wrongCallback,
                   qp](
                      const folly::SocketAddress&,
                      std::unique_ptr<Packet> packet,
                      std::optional<uint32_t>) {
      const uint64_t callbackStartedAtNs = getNowNs64();
      const auto* dataPacket = dynamic_cast<const DataPacket*>(packet.get());
      if (dataPacket == nullptr) {
        wrongCallback = true;
        return -1L;
      }
      if (packet->getBaseHeader().getSrcQp() != qp) {
        wrongCallback = true;
      }
      {
        std::lock_guard<std::mutex> lock(deliveredMutex);
        delivered.emplace_back(
            packet->getBaseHeader().getSrcQp(),
            dataPacket->getDataHeader().getMsn(),
            dataPacket->getDataHeader().getPosn());
        deliveredAtNs.push_back(callbackStartedAtNs);
      }
      deliveredCv.notify_all();
      return static_cast<ssize_t>(packet->getSizeBytes());
    };
  }

  const auto submitPacket =
      [&](uint32_t qp, uint16_t posn, bool isRetransmit = false) {
        auto dataHeader = createDataHeader(posn, kMsns[qp], posn);
        dataHeader.setIsRetransmit(isRetransmit);
        auto packet = std::make_unique<DataPacket>(
            createBaseHeader(BaseHeader::Opcode::OperationType::SEND, qp),
            dataHeader,
            nullptr,
            0);
        modifiers[qp]->modifyTxPacketIfMatches(
            sendFns[qp],
            std::make_unique<Modifier::TxPacketWithMetadata>(
                Modifier::TxPacketWithMetadata{
                    kDefaultRemoteAddr, std::move(packet)}));
      };
  constexpr std::array<uint16_t, kNumPosns> kArrivalPosns = {
      5, 2, 7, 1, 6, 3, 0, 4};
  for (uint32_t qp = 0; qp < kNumQps; ++qp) {
    for (uint16_t posn : kArrivalPosns) {
      submitPacket(qp, posn);
      if (qp == 0 && posn == kArrivalPosns.front()) {
        submitPacket(qp, posn);
        submitPacket(qp, posn);
      }
    }
  }
  submitPacket(0, 2);
  submitPacket(0, 2);

  std::vector<Fragment> expected;
  for (uint16_t posn = 0; posn < kTailPosn; ++posn) {
    for (uint32_t qp = 0; qp < kNumQps; ++qp) {
      expected.emplace_back(qp, kMsns[qp], posn);
    }
  }
  for (uint32_t qp = 0; qp < kNumQps; ++qp) {
    expected.emplace_back(qp, kMsns[qp], kTailPosn);
  }

  {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    ASSERT_TRUE(deliveredCv.wait_for(lock, std::chrono::seconds(2), [&] {
      return delivered.size() == expected.size();
    })) << "paced message wave did not drain";
  }
  EXPECT_EQ(delivered, expected)
      << "the modifier should normalize scrambled arrivals into complete "
         "ascending stripes and leave the tail stripe aligned";
  EXPECT_FALSE(wrongCallback)
      << "each packet must use the flow callback that accepted it";
  ASSERT_TRUE(modifiers[0]->waitForDrainForTest(std::chrono::seconds(2)))
      << "paced message wave remained queued after all callbacks completed";
  for (size_t index = 1; index < deliveredAtNs.size(); ++index) {
    EXPECT_GE(deliveredAtNs[index] - deliveredAtNs[index - 1], 1'000'000)
        << "self-chained pacing must not catch up with a packet burst";
  }
  for (const auto& modifier : modifiers) {
    EXPECT_EQ(modifier->numQueuedPackets(), 0)
        << "a complete message wave should leave no packets queued";
  }

  const auto deliveredBeforeCompletedRepeat = delivered.size();
  submitPacket(0, 0, false);
  EXPECT_EQ(delivered.size(), deliveredBeforeCompletedRepeat)
      << "a non-retransmitted duplicate must remain suppressed";
  submitPacket(0, 0, true);
  submitPacket(0, 0, true);
  submitPacket(1, 1, true);
  {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    ASSERT_TRUE(deliveredCv.wait_for(lock, std::chrono::seconds(2), [&] {
      return delivered.size() == deliveredBeforeCompletedRepeat + 2;
    })) << "retransmit recovery queue did not drain";
  }
  ASSERT_TRUE(modifiers[0]->waitForDrainForTest(std::chrono::seconds(2)))
      << "retransmit recovery remained queued after both callbacks completed";
  EXPECT_EQ(std::get<0>(delivered[deliveredBeforeCompletedRepeat]), 0)
      << "the first retransmit recovery should retain its QP";
  EXPECT_EQ(std::get<0>(delivered[deliveredBeforeCompletedRepeat + 1]), 1)
      << "the second retransmit recovery should retain its QP";
  const auto recoveryGapNs = deliveredAtNs[deliveredBeforeCompletedRepeat + 1] -
      deliveredAtNs[deliveredBeforeCompletedRepeat];
  EXPECT_GE(recoveryGapNs, 2'000'000)
      << "retransmit recovery must remain paced by its self-chained queue";

  submitPacket(0, 0, true);
  {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    ASSERT_TRUE(deliveredCv.wait_for(lock, std::chrono::seconds(2), [&] {
      return delivered.size() == deliveredBeforeCompletedRepeat + 3;
    })) << "a later retransmit should be eligible after recovery emits";
  }
  ASSERT_TRUE(modifiers[0]->waitForDrainForTest(std::chrono::seconds(2)))
      << "the later recovery must leave no queued packets";
  EXPECT_EQ(modifiers[0]->numQueuedPackets(), 0)
      << "a suppressed repeated key must not open a new wave";
}

TEST(
    StripedMessageWaveModifierTest,
    QueuedMessagesCannotReplaceMissingWaveQps) {
  using Fragment = std::tuple<uint32_t, uint32_t, uint16_t>;
  std::vector<Fragment> delivered;
  std::mutex deliveredMutex;
  std::condition_variable deliveredCv;
  SendPacketFn send = [&](const folly::SocketAddress&,
                          std::unique_ptr<Packet> packet,
                          std::optional<uint32_t>) {
    const auto* data = dynamic_cast<const DataPacket*>(packet.get());
    CHECK(data);
    {
      std::lock_guard<std::mutex> lock(deliveredMutex);
      delivered.emplace_back(
          packet->getBaseHeader().getSrcQp(),
          data->getDataHeader().getMsn(),
          data->getDataHeader().getPosn());
    }
    deliveredCv.notify_all();
    return static_cast<ssize_t>(packet->getSizeBytes());
  };
  auto config = std::make_unique<StripedMessageWaveModifierConfig>(
      ModifierConfig::ModifierFilter(
          ModifierConfig::ModifierFilter::Direction::Tx,
          ModifierConfig::ModifierFilter::PacketType::Data,
          folly::F14FastMap<std::string, FieldConditions>{}),
      std::nullopt,
      2,
      1,
      8,
      1000);
  StripedMessageWaveModifier modifier(std::move(config));
  const auto submit = [&](uint32_t qp, uint32_t msn, uint16_t posn) {
    modifier.modifyTxPacketIfMatches(
        send,
        std::make_unique<Modifier::TxPacketWithMetadata>(
            kDefaultRemoteAddr,
            std::make_unique<DataPacket>(
                createBaseHeader(BaseHeader::Opcode::OperationType::SEND, qp),
                createDataHeader(msn * 2 + posn, msn, posn),
                nullptr,
                0)));
  };
  submit(1, 300, 0);
  for (uint32_t msn : {100, 101}) {
    submit(0, msn, 0);
    submit(0, msn, 1);
  }
  {
    std::lock_guard<std::mutex> lock(deliveredMutex);
    EXPECT_TRUE(delivered.empty())
        << "one QP's queued messages must not release another QP's partial wave";
  }
  submit(1, 300, 1);
  {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    ASSERT_TRUE(deliveredCv.wait_for(lock, std::chrono::seconds(2), [&] {
      return delivered.size() == 4;
    })) << "the first complete wave must release while another message waits";
  }
  submit(1, 301, 0);
  submit(1, 301, 0);
  submit(1, 301, 1);
  ASSERT_TRUE(modifier.waitForDrainForTest(std::chrono::seconds(2)))
      << "a QP that still has a ready message must join the next completed wave";
  const std::vector<Fragment> expected = {
      {0, 100, 0},
      {1, 300, 0},
      {0, 100, 1},
      {1, 300, 1},
      {0, 101, 0},
      {1, 301, 0},
      {0, 101, 1},
      {1, 301, 1}};
  EXPECT_EQ(delivered, expected)
      << "staggered readiness and duplicate arrivals must preserve complete waves";
}

TEST(
    StripedMessageWaveModifierTest,
    SendCallbackWorkConsumesTheReleaseInterval) {
  constexpr uint32_t kReleaseIntervalUs = 100'000;
  constexpr uint64_t kCallbackWorkNs = 80'000'000;
  constexpr size_t kPackets = 4;
  std::vector<uint64_t> sendStarts;
  SendPacketFn sendFn = [&sendStarts](
                            const folly::SocketAddress&,
                            std::unique_ptr<Packet> packet,
                            std::optional<uint32_t>) {
    sendStarts.push_back(getNowNs64());
    const uint64_t workDeadlineNs = steadyNowNs() + kCallbackWorkNs;
    while (steadyNowNs() < workDeadlineNs) {
    }
    return static_cast<ssize_t>(packet->getSizeBytes());
  };
  auto config = std::make_unique<StripedMessageWaveModifierConfig>(
      ModifierConfig::ModifierFilter(
          ModifierConfig::ModifierFilter::Direction::Tx,
          ModifierConfig::ModifierFilter::PacketType::Data,
          folly::F14FastMap<std::string, FieldConditions>{}),
      std::nullopt,
      2,
      1,
      kPackets,
      kReleaseIntervalUs);
  StripedMessageWaveModifier modifier(std::move(config));
  for (uint32_t qp = 0; qp < 2; ++qp) {
    for (uint16_t posn = 0; posn < 2; ++posn) {
      modifier.modifyTxPacketIfMatches(
          sendFn,
          std::make_unique<Modifier::TxPacketWithMetadata>(
              kDefaultRemoteAddr,
              std::make_unique<DataPacket>(
                  createBaseHeader(BaseHeader::Opcode::OperationType::SEND, qp),
                  createDataHeader(posn, 1, posn),
                  nullptr,
                  0)));
    }
  }
  ASSERT_TRUE(modifier.waitForDrainForTest(std::chrono::seconds(3)))
      << "callback work must finish before reading the paced send times";
  ASSERT_EQ(sendStarts.size(), kPackets)
      << "the complete wave must deliver every fragment";
  const uint64_t intervalNs = kReleaseIntervalUs * 1000;
  for (size_t index = 1; index < sendStarts.size(); ++index) {
    EXPECT_GE(sendStarts[index] - sendStarts[index - 1], intervalNs)
        << "callback work must preserve the minimum start-to-start interval";
  }
  const uint64_t elapsedNs = sendStarts.back() - sendStarts.front();
  EXPECT_LT(elapsedNs, (kPackets - 1) * (intervalNs + kCallbackWorkNs / 2))
      << "start-to-start pacing must spend callback work out of the interval; "
      << "elapsed_ns=" << elapsedNs;
}

TEST(
    StripedMessageWaveModifierTest,
    RecoversSentHeadWhileTailsRemainScheduled) {
  const auto jsonStr = R"({
        "type": "striped_message_wave",
        "filter": {"direction": "tx", "packet_type": "data"},
        "qp_count": 2,
        "tail_posn": 1,
        "max_pending_packets": 4,
        "release_interval_us": 1000000
      })";
  auto config =
      ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
  StripedMessageWaveModifier modifier(std::move(config));
  using Fragment = std::tuple<uint32_t, uint16_t, bool>;
  std::vector<Fragment> delivered;
  std::mutex deliveredMutex;
  std::condition_variable deliveredCv;
  SendPacketFn sendFn = [&](const folly::SocketAddress&,
                            std::unique_ptr<Packet> packet,
                            std::optional<uint32_t>) {
    const auto* dataPacket = dynamic_cast<const DataPacket*>(packet.get());
    CHECK(dataPacket);
    {
      std::lock_guard<std::mutex> lock(deliveredMutex);
      delivered.emplace_back(
          packet->getBaseHeader().getSrcQp(),
          dataPacket->getDataHeader().getPosn(),
          dataPacket->getDataHeader().getIsRetransmit());
    }
    deliveredCv.notify_all();
    return static_cast<ssize_t>(packet->getSizeBytes());
  };
  const auto submit = [&](uint32_t qp, uint16_t posn, bool retry) {
    auto header = createDataHeader(posn, 1, posn);
    header.setIsRetransmit(retry);
    modifier.modifyTxPacketIfMatches(
        sendFn,
        std::make_unique<Modifier::TxPacketWithMetadata>(
            kDefaultRemoteAddr,
            std::make_unique<DataPacket>(
                createBaseHeader(BaseHeader::Opcode::OperationType::SEND, qp),
                header,
                nullptr,
                0)));
  };
  for (uint32_t qp = 0; qp < 2; ++qp) {
    for (uint16_t posn = 0; posn < 2; ++posn) {
      submit(qp, posn, false);
    }
  }
  {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    ASSERT_TRUE(deliveredCv.wait_for(lock, std::chrono::seconds(10), [&] {
      return delivered.size() >= 2;
    })) << "precondition: both original heads must reach the sender";
    ASSERT_EQ(delivered.size(), 2)
        << "precondition: tails must remain scheduled when recovery arrives";
  }
  submit(0, 0, true);
  submit(0, 0, true);
  submit(1, 1, true);
  {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    ASSERT_TRUE(deliveredCv.wait_for(lock, std::chrono::seconds(10), [&] {
      return delivered.size() >= 5;
    })) << "a sent head must recover while its original tail is still queued";
  }
  ASSERT_TRUE(modifier.waitForDrainForTest(std::chrono::seconds(10)))
      << "original and recovery callbacks must finish before teardown";
  const std::vector<Fragment> expected = {
      {0, 0, false}, {1, 0, false}, {0, 0, true}, {0, 1, false}, {1, 1, false}};
  EXPECT_EQ(delivered, expected)
      << "recovery must preserve original stripes, suppress queued duplicates, "
         "and leave unsent tails on the original schedule";
}

TEST(StripedMessageWaveModifierTest, RecoverySharesOriginalPacketPacing) {
  const auto jsonStr = R"({
        "type": "striped_message_wave",
        "filter": {"direction": "tx", "packet_type": "data"},
        "qp_count": 2,
        "tail_posn": 1,
        "max_pending_packets": 4,
        "release_interval_us": 10000
      })";
  auto config =
      ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
  StripedMessageWaveModifier modifier(std::move(config));
  using Fragment = std::tuple<uint32_t, uint32_t, uint16_t, bool>;
  std::vector<Fragment> delivered;
  std::vector<uint64_t> deliveredAtNs;
  std::mutex deliveredMutex;
  std::condition_variable deliveredCv;
  SendPacketFn sendFn = [&](const folly::SocketAddress&,
                            std::unique_ptr<Packet> packet,
                            std::optional<uint32_t>) {
    const uint64_t callbackStartedAtNs = getNowNs64();
    const auto* dataPacket = dynamic_cast<const DataPacket*>(packet.get());
    CHECK(dataPacket);
    const auto& header = dataPacket->getDataHeader();
    {
      std::lock_guard<std::mutex> lock(deliveredMutex);
      delivered.emplace_back(
          packet->getBaseHeader().getSrcQp(),
          header.getMsn(),
          header.getPosn(),
          header.getIsRetransmit());
      deliveredAtNs.push_back(callbackStartedAtNs);
    }
    deliveredCv.notify_all();
    return static_cast<ssize_t>(packet->getSizeBytes());
  };
  const auto submit = [&](uint32_t qp,
                          uint32_t msn,
                          uint16_t posn,
                          bool retry) {
    auto header = createDataHeader(msn * 2 + posn, msn, posn);
    header.setIsRetransmit(retry);
    modifier.modifyTxPacketIfMatches(
        sendFn,
        std::make_unique<Modifier::TxPacketWithMetadata>(
            kDefaultRemoteAddr,
            std::make_unique<DataPacket>(
                createBaseHeader(BaseHeader::Opcode::OperationType::SEND, qp),
                header,
                nullptr,
                0)));
  };
  for (uint32_t qp = 0; qp < 2; ++qp) {
    for (uint16_t posn = 0; posn < 2; ++posn) {
      submit(qp, 1, posn, false);
    }
  }
  ASSERT_TRUE(modifier.waitForDrainForTest(std::chrono::seconds(2)))
      << "precondition: the first wave must complete before recovery is queued";
  for (uint32_t qp = 0; qp < 2; ++qp) {
    for (uint16_t posn = 0; posn < 2; ++posn) {
      submit(qp, 2, posn, false);
    }
  }
  submit(0, 1, 0, true);
  submit(1, 1, 1, true);
  {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    ASSERT_TRUE(deliveredCv.wait_for(lock, std::chrono::seconds(2), [&] {
      return delivered.size() == 10;
    })) << "mixed original and recovery traffic must deliver every fragment";
  }
  ASSERT_TRUE(modifier.waitForDrainForTest(std::chrono::seconds(2)))
      << "mixed traffic must release all deferred callbacks before teardown";
  std::vector<Fragment> originals;
  bool previousWasRecovery = false;
  for (size_t index = 4; index < delivered.size(); ++index) {
    const bool isRecovery = std::get<3>(delivered[index]);
    if (!isRecovery) {
      originals.push_back(delivered[index]);
    } else if (originals.size() < 4) {
      EXPECT_FALSE(previousWasRecovery)
          << "pending originals must make progress between recovery packets";
    }
    previousWasRecovery = isRecovery;
    const uint64_t expectedIntervalNs =
        std::get<3>(delivered[index - 1]) ? 20'000'000 : 10'000'000;
    EXPECT_GE(
        deliveredAtNs[index] - deliveredAtNs[index - 1], expectedIntervalNs)
        << "recovery and originals must share one delivery-rate budget; "
        << "previous retry=" << std::get<3>(delivered[index - 1])
        << ", current retry=" << std::get<3>(delivered[index]);
  }
  const std::vector<Fragment> expectedOriginals = {
      {0, 2, 0, false}, {1, 2, 0, false}, {0, 2, 1, false}, {1, 2, 1, false}};
  EXPECT_EQ(originals, expectedOriginals)
      << "recovery must preserve the original fragment stripes and tail order";
}

TEST(StripedMessageWaveModifierTest, ShortTailsRetainFullFragmentSpacing) {
  const auto jsonStr = R"({
        "type": "striped_message_wave",
        "filter": {"direction": "tx", "packet_type": "data"},
        "qp_count": 2,
        "tail_posn": 1,
        "max_pending_packets": 4,
        "release_interval_us": 2000000,
        "tail_release_interval_us": 10000
      })";
  auto config =
      ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
  StripedMessageWaveModifier modifier(std::move(config));
  std::vector<std::pair<uint32_t, uint16_t>> delivered;
  std::vector<uint64_t> deliveredAtNs;
  std::mutex deliveredMutex;
  std::condition_variable deliveredCv;
  SendPacketFn sendFn = [&](const folly::SocketAddress&,
                            std::unique_ptr<Packet> packet,
                            std::optional<uint32_t>) {
    const uint64_t callbackStartedAtNs = getNowNs64();
    const auto* dataPacket = dynamic_cast<const DataPacket*>(packet.get());
    CHECK(dataPacket);
    {
      std::lock_guard<std::mutex> lock(deliveredMutex);
      delivered.emplace_back(
          packet->getBaseHeader().getSrcQp(),
          dataPacket->getDataHeader().getPosn());
      deliveredAtNs.push_back(callbackStartedAtNs);
    }
    deliveredCv.notify_all();
    return static_cast<ssize_t>(packet->getSizeBytes());
  };
  for (uint32_t qp = 0; qp < 2; ++qp) {
    for (uint16_t posn = 0; posn < 2; ++posn) {
      modifier.modifyTxPacketIfMatches(
          sendFn,
          std::make_unique<Modifier::TxPacketWithMetadata>(
              kDefaultRemoteAddr,
              std::make_unique<DataPacket>(
                  createBaseHeader(BaseHeader::Opcode::OperationType::SEND, qp),
                  createDataHeader(posn, 100 + qp, posn),
                  nullptr,
                  0)));
    }
  }
  {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    ASSERT_TRUE(deliveredCv.wait_for(lock, std::chrono::seconds(12), [&] {
      return delivered.size() == 4;
    })) << "the full-fragment and tail stripes must both drain";
  }
  ASSERT_TRUE(modifier.waitForDrainForTest(std::chrono::seconds(2)))
      << "completed tail delivery must leave no scheduled callbacks";
  const std::vector<std::pair<uint32_t, uint16_t>> expected = {
      {0, 0}, {1, 0}, {0, 1}, {1, 1}};
  EXPECT_EQ(delivered, expected)
      << "tail-specific pacing must preserve ascending fragment stripes";
  for (size_t index = 1; index < 3; ++index) {
    EXPECT_GE(deliveredAtNs[index] - deliveredAtNs[index - 1], 2'000'000'000)
        << "full fragments must retain their configured transmission interval";
  }
  const auto tailGapNs = deliveredAtNs[3] - deliveredAtNs[2];
  EXPECT_GE(tailGapNs, 10'000'000)
      << "short tails must retain their own minimum transmission interval";
  EXPECT_LT(tailGapNs, 1'000'000'000)
      << "a short tail must not consume a full-fragment interval";
}

TEST(StripedMessageWaveModifierTest, RejectsInvalidTailIntervals) {
  auto json = folly::parseJson(R"({
        "type": "striped_message_wave",
        "filter": {"direction": "tx", "packet_type": "data"},
        "qp_count": 2,
        "tail_posn": 1,
        "max_pending_packets": 4,
        "release_interval_us": 1000
      })");
  for (int64_t intervalUs : {int64_t{-1}, int64_t{0}, int64_t{1} << 32}) {
    json["tail_release_interval_us"] = intervalUs;
    EXPECT_THROW(
        std::ignore = ModifierConfig::buildModifierConfigFromJson(json),
        std::out_of_range)
        << "tail intervals must be positive and fit the scheduler's duration";
  }
}

TEST(StripedMessageWaveModifierTest, TxMessageWaveFallsBackAtPendingPacketCap) {
  const auto jsonStr = R"({
        "name": "bounded_message_wave",
        "type": "striped_message_wave",
        "filter": {"direction": "tx", "packet_type": "data"},
        "qp_count": 4,
        "tail_posn": 7,
        "max_pending_packets": 1,
        "release_interval_us": 1
      })";
  auto waveState = std::make_shared<StripedMessageWaveState>();
  auto config =
      ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
  auto* waveConfig =
      dynamic_cast<StripedMessageWaveModifierConfig*>(config.get());
  ASSERT_NE(waveConfig, nullptr);
  waveConfig->state = waveState;
  StripedMessageWaveModifier modifier(std::move(config));
  std::vector<uint16_t> delivered;
  bool callbacksRanWithoutWaveLock = true;
  SendPacketFn sendFn = [&callbacksRanWithoutWaveLock, &delivered, &waveState](
                            const folly::SocketAddress&,
                            std::unique_ptr<Packet> packet,
                            std::optional<uint32_t>) {
    std::unique_lock<std::mutex> waveLock(waveState->mutex, std::try_to_lock);
    callbacksRanWithoutWaveLock &= waveLock.owns_lock();
    const auto* dataPacket = dynamic_cast<const DataPacket*>(packet.get());
    CHECK(dataPacket);
    delivered.push_back(dataPacket->getDataHeader().getPosn());
    return static_cast<ssize_t>(packet->getSizeBytes());
  };

  for (uint16_t posn = 0; posn < 3; ++posn) {
    modifier.modifyTxPacketIfMatches(
        sendFn,
        std::make_unique<Modifier::TxPacketWithMetadata>(
            kDefaultRemoteAddr,
            std::make_unique<DataPacket>(
                createBaseHeader(BaseHeader::Opcode::OperationType::SEND, 0),
                createDataHeader(posn, 123, posn),
                nullptr,
                0)));
  }

  EXPECT_EQ(delivered, (std::vector<uint16_t>{0, 1, 2}))
      << "the pending-packet cap should release held packets and bypass later packets";
  EXPECT_EQ(modifier.numQueuedPackets(), 0)
      << "the fallback path should leave no packets queued";
  EXPECT_TRUE(callbacksRanWithoutWaveLock)
      << "fallback sends must run after releasing the shared wave mutex";
}

TEST(StripedMessageWaveModifierTest, RxMessageWaveIsRejectedAtConstruction) {
  auto config = std::make_unique<StripedMessageWaveModifierConfig>(
      ModifierConfig::ModifierFilter(
          ModifierConfig::ModifierFilter::Direction::Rx,
          ModifierConfig::ModifierFilter::PacketType::Data,
          folly::F14FastMap<std::string, FieldConditions>{}),
      std::nullopt,
      4,
      7,
      32,
      1);

  EXPECT_THROW(
      { StripedMessageWaveModifier modifier(std::move(config)); },
      std::runtime_error)
      << "striped message waves are a transmit-only modifier";
}

TEST(
    StripedMessageWaveModifierTest,
    TxMessageWaveDropsScheduledWorkForDestroyedFlow) {
  const auto jsonStr = R"({
        "name": "paced_message_wave",
        "type": "striped_message_wave",
        "filter": {"direction": "tx", "packet_type": "data"},
        "qp_count": 2,
        "tail_posn": 1,
        "max_pending_packets": 4,
        "release_interval_us": 20000
      })";
  auto waveState = std::make_shared<StripedMessageWaveState>();
  const auto makeModifier = [&] {
    auto config =
        ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
    auto* waveConfig =
        dynamic_cast<StripedMessageWaveModifierConfig*>(config.get());
    CHECK(waveConfig);
    waveConfig->state = waveState;
    return std::make_unique<StripedMessageWaveModifier>(std::move(config));
  };
  auto closedFlow = makeModifier();
  auto liveFlow = makeModifier();
  std::mutex deliveredMutex;
  std::condition_variable deliveredCv;
  std::vector<uint32_t> deliveredQps;
  SendPacketFn closedSend = [&](const folly::SocketAddress&,
                                std::unique_ptr<Packet> packet,
                                std::optional<uint32_t>) {
    std::lock_guard<std::mutex> lock(deliveredMutex);
    deliveredQps.push_back(packet->getBaseHeader().getSrcQp());
    deliveredCv.notify_all();
    return 1L;
  };
  SendPacketFn liveSend = [&](const folly::SocketAddress&,
                              std::unique_ptr<Packet> packet,
                              std::optional<uint32_t>) {
    std::lock_guard<std::mutex> lock(deliveredMutex);
    deliveredQps.push_back(packet->getBaseHeader().getSrcQp());
    deliveredCv.notify_all();
    return 1L;
  };
  const auto submit = [&](StripedMessageWaveModifier& modifier,
                          SendPacketFn& send,
                          uint32_t qp,
                          uint16_t posn) {
    modifier.modifyTxPacketIfMatches(
        send,
        std::make_unique<Modifier::TxPacketWithMetadata>(
            kDefaultRemoteAddr,
            std::make_unique<DataPacket>(
                createBaseHeader(BaseHeader::Opcode::OperationType::SEND, qp),
                createDataHeader(posn, qp + 100, posn),
                nullptr,
                0)));
  };
  submit(*closedFlow, closedSend, 0, 0);
  submit(*closedFlow, closedSend, 0, 1);
  submit(*liveFlow, liveSend, 1, 0);
  submit(*liveFlow, liveSend, 1, 1);

  closedFlow->stopTimers();
  closedFlow.reset();
  {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    ASSERT_TRUE(deliveredCv.wait_for(lock, std::chrono::seconds(1), [&] {
      return deliveredQps.size() == 2;
    })) << "the live flow's paced packets did not drain";
  }
  EXPECT_EQ(deliveredQps, (std::vector<uint32_t>{1, 1}))
      << "destroying one flow should discard only that flow's scheduled packets";
}

TEST(StripedMessageWaveModifierTest, PendingRetransmitFlushesIncompleteWave) {
  const auto jsonStr = R"({
        "name": "incomplete_message_wave",
        "type": "striped_message_wave",
        "filter": {"direction": "tx", "packet_type": "data"},
        "qp_count": 2,
        "tail_posn": 1,
        "max_pending_packets": 8,
        "release_interval_us": 1
      })";
  auto config =
      ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
  StripedMessageWaveModifier modifier(std::move(config));
  std::vector<uint16_t> delivered;
  SendPacketFn sendFn = [&delivered](
                            const folly::SocketAddress&,
                            std::unique_ptr<Packet> packet,
                            std::optional<uint32_t>) {
    const auto* dataPacket = dynamic_cast<const DataPacket*>(packet.get());
    CHECK(dataPacket);
    delivered.push_back(dataPacket->getDataHeader().getPosn());
    return static_cast<ssize_t>(packet->getSizeBytes());
  };
  const auto submit = [&](uint16_t posn, bool isRetransmit) {
    auto dataHeader = createDataHeader(posn, 123, posn);
    dataHeader.setIsRetransmit(isRetransmit);
    modifier.modifyTxPacketIfMatches(
        sendFn,
        std::make_unique<Modifier::TxPacketWithMetadata>(
            kDefaultRemoteAddr,
            std::make_unique<DataPacket>(
                createBaseHeader(BaseHeader::Opcode::OperationType::SEND, 0),
                dataHeader,
                nullptr,
                0)));
  };

  submit(0, false);
  EXPECT_TRUE(delivered.empty())
      << "an incomplete wave must hold the original fragment";
  submit(0, true);
  submit(1, false);

  EXPECT_EQ(delivered, (std::vector<uint16_t>{0, 1}))
      << "a retransmit must release a wave that cannot reach its QP count";
  EXPECT_EQ(modifier.numQueuedPackets(), 0)
      << "the bypass transition must drain all held fragments";
}

TEST(StripedMessageWaveModifierTest, PendingRetransmitWaitsForKnownQpSet) {
  const auto jsonStr = R"({
        "name": "complete_qp_set",
        "type": "striped_message_wave",
        "filter": {"direction": "tx", "packet_type": "data"},
        "qp_count": 2,
        "tail_posn": 1,
        "max_pending_packets": 8,
        "release_interval_us": 1
      })";
  auto config =
      ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
  StripedMessageWaveModifier modifier(std::move(config));
  using Fragment = std::tuple<uint32_t, uint32_t, uint16_t>;
  std::vector<Fragment> delivered;
  std::mutex deliveredMutex;
  std::condition_variable deliveredCv;
  SendPacketFn sendFn = [&delivered, &deliveredMutex, &deliveredCv](
                            const folly::SocketAddress&,
                            std::unique_ptr<Packet> packet,
                            std::optional<uint32_t>) {
    const auto* dataPacket = dynamic_cast<const DataPacket*>(packet.get());
    CHECK(dataPacket);
    {
      std::lock_guard<std::mutex> lock(deliveredMutex);
      delivered.emplace_back(
          packet->getBaseHeader().getSrcQp(),
          dataPacket->getDataHeader().getMsn(),
          dataPacket->getDataHeader().getPosn());
    }
    deliveredCv.notify_all();
    return static_cast<ssize_t>(packet->getSizeBytes());
  };
  const auto submit = [&](uint32_t qp,
                          uint32_t msn,
                          uint16_t posn,
                          bool isRetransmit = false) {
    auto dataHeader = createDataHeader(posn, msn, posn);
    dataHeader.setIsRetransmit(isRetransmit);
    modifier.modifyTxPacketIfMatches(
        sendFn,
        std::make_unique<Modifier::TxPacketWithMetadata>(
            kDefaultRemoteAddr,
            std::make_unique<DataPacket>(
                createBaseHeader(BaseHeader::Opcode::OperationType::SEND, qp),
                dataHeader,
                nullptr,
                0)));
  };
  const auto waitForDelivered = [&](size_t count) {
    std::unique_lock<std::mutex> lock(deliveredMutex);
    return deliveredCv.wait_for(lock, std::chrono::seconds(1), [&] {
      return delivered.size() == count;
    });
  };

  for (uint32_t qp = 0; qp < 2; ++qp) {
    submit(qp, 100 + qp, 0);
    submit(qp, 100 + qp, 1);
  }
  ASSERT_TRUE(waitForDelivered(4));
  ASSERT_TRUE(modifier.waitForDrainForTest(std::chrono::seconds(1)));

  submit(0, 200, 0);
  submit(0, 200, 0, true);
  {
    std::lock_guard<std::mutex> lock(deliveredMutex);
    EXPECT_EQ(delivered.size(), 4)
        << "a held fragment's retransmit must remain suppressed";
  }
  submit(0, 200, 1);
  submit(1, 201, 0);
  submit(1, 201, 1);

  ASSERT_TRUE(waitForDelivered(8));
  EXPECT_TRUE(modifier.waitForDrainForTest(std::chrono::seconds(1)));
  EXPECT_EQ(modifier.numQueuedPackets(), 0);
}

} // namespace libsoftmetaroce
