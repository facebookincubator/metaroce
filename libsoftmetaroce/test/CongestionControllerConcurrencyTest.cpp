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

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include <libsoftmetaroce/congestion_control/Baseline.h>
#include <libsoftmetaroce/congestion_control/SynchronizedCongestionController.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/QpTestBase.h>

#include <atomic>
#include <thread>

using ::testing::NiceMock;

namespace libsoftmetaroce {
namespace {

constexpr uint8_t kErroredTag = 1;

std::pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

} // namespace

class CongestionControllerConcurrencyTest : public QpTestBase {};

// ThreadSanitizer is the oracle here: unlocked this reports, locked it does
// not.
TEST_F(CongestionControllerConcurrencyTest, RetransmitTimerAndSenderDoNotRace) {
  // FastStart-derived: the colliding fields, inFastStart_ and qwnd_, are
  // QP-wide, and these threads work on different flows.
  qp.setCongestionController(std::make_unique<Baseline>(2 /*epsilon*/, 1));

  LimitedTestSender sender(
      arbiter, mockUDPSocketMaker, 0xace, LimitedTestSender::kDisableSendLoop);
  qp.setupFlows(2 /*txFlowCount*/, 2 /*rxFlowCount*/, &sender);

  // Two flows, so each thread takes its own sndCur and only the controller
  // is left to serialize them.
  auto* sendFlow = qp.getTxFlowById(0);
  auto* retxFlow = qp.getTxFlowById(1);
  ASSERT_NE(sendFlow, nullptr);
  ASSERT_NE(retxFlow, nullptr);

  // Its own ibv_qp, since a Tag's error state is that object's state field.
  // Posted before Tag A so flow 1 strands this work and not Tag A's.
  qp.addTag(kErroredTag, &cq, &otherIbvQpStorage);
  std::array<char, 8192> retxPayload{};
  Wqe* strandedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kWrite,
       .msgSizeBytes = 4096,
       .tagNum = kErroredTag,
       .address = retxPayload.data()});
  ASSERT_NE(strandedWqe, nullptr)
      << "precondition: the errored tag needs work, or the retransmit scan "
         "below finds nothing to abandon and never reaches the controller";
  ASSERT_TRUE(allocateSendablePosns(strandedWqe))
      << "precondition: the stranded work needs the posn its tracker entry "
         "names, or the scan walks a frame with nothing behind it";

  // Outstanding on flow 1, so the scan has a frame to walk.
  auto& retxState = retxFlow->getTxState();
  retxState.sndUna = 0;
  *(retxState.sndCur.wlock()) = 0;
  retxState.sndNxt = 1;
  retxState.tracker.emplace_back(
      PacketId(strandedWqe, kErroredTag, 0 /*posn*/, mtu),
      Flow::FsnState::UNACKED);

  // In error, so mustStopRetransmitting() is true for that frame and the scan
  // abandons it, retires it, and reports the retirement to the controller.
  {
    auto erroredTag = qp.getTag(kErroredTag);
    erroredTag->waitForFree();
    std::ignore = qp.enterTagErrorWithTagHeld(*erroredTag);
    erroredTag->markFree();
  }
  ASSERT_EQ(otherIbvQpStorage.state, IBV_QPS_ERR)
      << "precondition: the second tag is the one in error";
  ASSERT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "precondition: the sending tag must not have been errored with it, "
         "or the sender thread below has nothing to send";

  // Tag A keeps the sender busy for the whole run.
  std::array<char, 65536> sendPayload{};
  Wqe* sendWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kWrite,
       .msgSizeBytes = 32768,
       .tagNum = kMetaRoceInitialTag,
       .address = sendPayload.data()});
  ASSERT_NE(sendWqe, nullptr) << "precondition: the sender needs work";

  folly::Baton<> start;
  std::atomic<int> attempts{0};

  // Refused sends still race: each attempt reads inFastStart_ before it
  // decides, and the scan below writes it.
  std::thread senderThread([&] {
    start.wait();
    for (int i = 0; i < 64; ++i) {
      // The unique_ptr's destructor keeps the call from being elided.
      auto packet = sendFlow->getNextPacketToSend();
      ++attempts;
    }
  });

  start.post();
  // One call: this scan retires every abandoned frame.
  retxFlow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);
  senderThread.join();

  EXPECT_EQ(attempts.load(), 64)
      << "the sending thread must have run, or the scan below raced nobody";
  EXPECT_EQ(retxState.sndUna, 1)
      << "the retransmit scan must have abandoned and retired the stranded "
         "frame, which is what reports a retirement to the controller; "
         "without that this test exercises only the sending side";
}

// ThreadSanitizer is the oracle here too: the controller's methods write a
// plain counter, which races unless the wrapper serializes them.
TEST_F(CongestionControllerConcurrencyTest, ControllerMethodsExcludeEachOther) {
  class CountingController : public CongestionController {
   public:
    mutable int calls{0};

    AckResult onPacketAck(uint16_t, const AckEvent&) override {
      ++calls;
      return {};
    }
    uint16_t getFwnd(uint16_t) const override {
      ++calls;
      return 0;
    }

    void onPacketSent(uint16_t) override {}
    void onPacketLoss(uint16_t, const LossEvent&) override {}
    void onFlowAdded(uint16_t) override {}
    void onFlowDisabled(uint16_t) override {}
    uint16_t getAvailableWindowSize(uint16_t) const override {
      return 0;
    }
    uint32_t getQwnd() const override {
      return 0;
    }
  };

  constexpr int kCallsPerThread = 256;
  auto inner = std::make_unique<CountingController>();
  auto* raw = inner.get();
  SynchronizedCongestionController guarded{std::move(inner)};

  std::thread acker([&] {
    for (int i = 0; i < kCallsPerThread; ++i) {
      guarded.onPacketAck(0, CongestionController::AckEvent{});
    }
  });
  std::thread reader([&] {
    for (int i = 0; i < kCallsPerThread; ++i) {
      (void)guarded.getFwnd(0);
    }
  });
  acker.join();
  reader.join();

  EXPECT_EQ(raw->calls, 2 * kCallsPerThread)
      << "a lost increment means two callers were inside the controller at "
         "once, so the wrapper is not excluding them";
}

} // namespace libsoftmetaroce
