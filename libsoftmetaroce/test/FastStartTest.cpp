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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/congestion_control/FastStart.h>

using namespace testing;
namespace libsoftmetaroce {

class FastStartTest : public ::testing::Test {};

/*
 * Simple test of fast start cc.  Create flow, send packets, ack packets,
 * confirm window is limited to the number of packets sent before the ack.
 */
TEST_F(FastStartTest, Basic) {
  int expectedWindow;
  const int fwndLimit = 5;
  FastStart cc;

  const uint16_t flowId = 0;
  cc.onFlowAdded(flowId);

  EXPECT_GT(cc.getAvailableWindowSize(flowId), 0)
      << "fast start must offer window before anything is sent, or a QP could "
         "never start";
  for (int i = 0; i < fwndLimit; i++) {
    cc.onPacketSent(flowId);
    EXPECT_GT(cc.getAvailableWindowSize(flowId), 0)
        << "fast start grows the window as it sends, so send " << i
        << " must not stall the probe it is driving";
  }

  // reuse the ack event for a one-packet ack.
  auto ackEventNoCE = CongestionController::AckEvent{.numAckedPackets = 1};

  // Process an ack.  On this ack, the cc will exit fast start,
  // meaning the qwnd and fwnd windows stop growing.  Because this
  // packet is acked, the available window should be 1.
  cc.onPacketAck(flowId, ackEventNoCE);
  EXPECT_EQ(cc.getAvailableWindowSize(flowId), 1)
      << "the ack ends fast start and frees the acked packet's slot, so the "
         "window opens by exactly that one packet";

  // simulate expected behavior, where we would send into this available
  // window.
  cc.onPacketSent(flowId);
  EXPECT_EQ(cc.getAvailableWindowSize(flowId), 0)
      << "sending into the last free slot must close the window, or the flow "
         "would carry more than fwnd in flight";

  // simulate acking the rest of the window to reach full size.
  for (expectedWindow = 1; expectedWindow <= fwndLimit; expectedWindow++) {
    cc.onPacketAck(flowId, ackEventNoCE);
    EXPECT_EQ(cc.getAvailableWindowSize(flowId), expectedWindow)
        << "each ack returns one slot, so the window recovers toward the fwnd "
           "fast start grew";
  }

  // it would be invalid to get more acks than there are in flight packets,
  // but let's confirm that the window doesn't exceed the max.
  cc.onPacketAck(flowId, ackEventNoCE);
  EXPECT_EQ(cc.getAvailableWindowSize(flowId), fwndLimit)
      << "the window must stay at the fwnd fast start grew, so a stray ack "
         "cannot inflate it";
}

namespace {
// Drive one send on whichever flow fast start selects, returning that choice.
CongestionController::NextSend sendNext(FastStart& cc) {
  const auto next = cc.flowForNextSend();
  EXPECT_TRUE(next.has_value())
      << "fast start must choose the flow while it owns the schedule";
  cc.onPacketSent(next->flowId);
  return *next;
}
} // namespace

/*
 * Fast start gives each flow fast_start_burst packets, then spreads one packet
 * per flow -- the schedule recommended by MetaRoCE spec figure 6.2.
 */
TEST_F(FastStartTest, FastStartBurstsThenSpreadsOnePerFlow) {
  const uint16_t burst =
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_fast_start_burst);
  constexpr uint16_t kNumFlows = 3;
  FastStart cc;
  for (uint16_t flowId = 0; flowId < kNumFlows; ++flowId) {
    cc.onFlowAdded(flowId);
  }

  for (uint16_t flowId = 0; flowId < kNumFlows; ++flowId) {
    for (uint16_t i = 0; i < burst; ++i) {
      const auto next = sendNext(cc);
      EXPECT_EQ(next.flowId, flowId)
          << "packet " << i << " belongs on the flow taking its opening burst";
      EXPECT_EQ(next.lastOnFlow, i + 1 == burst)
          << "only the burst's final packet ends the run, so an ack need be "
             "requested once per run rather than per packet";
    }
  }

  // Every flow holds its opening burst, so the default spreads from here.
  for (uint16_t round = 0; round < 2; ++round) {
    for (uint16_t flowId = 0; flowId < kNumFlows; ++flowId) {
      const auto next = sendNext(cc);
      EXPECT_EQ(next.flowId, flowId)
          << "once every flow holds its opening burst the default is round "
             "robin, one packet per flow";
      EXPECT_TRUE(next.lastOnFlow)
          << "a one-packet run ends on its only packet";
    }
  }
}

TEST_F(FastStartTest, TwoPortPairsInterleaveTheOpeningBursts) {
  const uint16_t burst =
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_fast_start_burst);
  constexpr uint16_t kNumFlows = 4;
  FastStart cc;
  cc.setPortPairCount(2);
  for (uint16_t flowId = 0; flowId < kNumFlows; ++flowId) {
    cc.onFlowAdded(flowId);
  }

  for (uint16_t firstOfPair : {0, 2}) {
    for (uint16_t i = 0; i < 2 * burst; ++i) {
      const auto next = sendNext(cc);
      EXPECT_EQ(next.flowId, firstOfPair + i % 2)
          << "packet " << i << " should alternate between the two flows on "
          << "different port pairs, so fast start begins on both at once";
      EXPECT_TRUE(next.lastOnFlow)
          << "the next packet goes to the other flow, so each ends a run";
    }
  }

  for (uint16_t flowId = 0; flowId < kNumFlows; ++flowId) {
    EXPECT_EQ(sendNext(cc).flowId, flowId)
        << "once every flow holds its burst the default is round robin";
  }
}

TEST_F(FastStartTest, AFlowLeftOverByThePortPairsTakesItsBurstAlone) {
  const uint16_t burst =
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_fast_start_burst);
  FastStart cc;
  cc.setPortPairCount(2);
  for (uint16_t flowId = 0; flowId < 3; ++flowId) {
    cc.onFlowAdded(flowId);
  }
  for (uint16_t i = 0; i < 2 * burst; ++i) {
    ASSERT_EQ(sendNext(cc).flowId, i % 2)
        << "precondition: the first two flows interleave their bursts";
  }

  for (uint16_t i = 0; i < burst; ++i) {
    const auto next = sendNext(cc);
    EXPECT_EQ(next.flowId, 2)
        << "the third flow, with no partner on the other pair, should take "
           "its whole burst";
    EXPECT_EQ(next.lastOnFlow, i + 1 == burst)
        << "a lone flow's burst is one run, ending on its final packet";
  }
}

TEST_F(FastStartTest, AnAckWithNothingToSendLeavesFastStartRunning) {
  FastStart cc;
  cc.onFlowAdded(0);
  cc.onPacketSent(0);
  cc.onPacketSent(0);

  cc.onPacketAck(
      0,
      CongestionController::AckEvent{
          .numAckedPackets = 1, .sendPending = false});

  EXPECT_TRUE(cc.flowForNextSend().has_value())
      << "an ack arriving with no data left to send should leave fast start "
         "running, so a following message is not held to a window sized for "
         "a small one";
}

TEST_F(FastStartTest, ACongestionMarkEndsFastStartWithNothingToSend) {
  FastStart cc;
  cc.onFlowAdded(0);
  cc.onPacketSent(0);
  cc.onPacketSent(0);

  cc.onPacketAck(
      0,
      CongestionController::AckEvent{
          .numAckedPackets = 1,
          .numAckedPacketsWithCE = 1,
          .sendPending = false});

  EXPECT_FALSE(cc.flowForNextSend().has_value())
      << "an ack reporting congestion should end fast start even with no "
         "data left to send";
}

/*
 * After the opening burst, runs are as long as the sender asked for -- so a
 * coalescing sender keeps a run together rather than having it spread a packet
 * per flow.
 */
TEST_F(FastStartTest, PacketsPerVisitSetsRunLengthAfterOpeningBurst) {
  const uint16_t burst =
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_fast_start_burst);
  constexpr uint16_t kNumFlows = 3;
  constexpr uint16_t kRunPackets = 2;
  FastStart cc;
  for (uint16_t flowId = 0; flowId < kNumFlows; ++flowId) {
    cc.onFlowAdded(flowId);
  }
  cc.setPacketsPerVisit(kRunPackets);

  // The opening group still places a fast-start burst on each flow in turn.
  for (uint16_t flowId = 0; flowId < kNumFlows; ++flowId) {
    for (uint16_t i = 0; i < burst; ++i) {
      EXPECT_EQ(sendNext(cc).flowId, flowId)
          << "opening packet " << i << " belongs on the flow being filled";
    }
  }

  // Two full rounds of runs, so the wrap back to flow 0 is covered.
  for (uint16_t round = 0; round < 2; ++round) {
    for (uint16_t flowId = 0; flowId < kNumFlows; ++flowId) {
      for (uint16_t i = 0; i < kRunPackets; ++i) {
        const auto next = sendNext(cc);
        EXPECT_EQ(next.flowId, flowId)
            << "round " << round << " packet " << i
            << " should stay on the flow for the requested run length";
        EXPECT_EQ(next.lastOnFlow, i + 1 == kRunPackets)
            << "round " << round << " packet " << i
            << ": the run's final packet ends it, so the ack request lands "
               "once per run rather than per packet";
      }
    }
  }
}

/*
 * The request sizes runs after the opening burst, never the opening burst
 * itself: a request narrower than fast_start_burst still gets a full burst on
 * the first visit, since fast start owns that size.
 */
TEST_F(FastStartTest, PacketsPerVisitDoesNotResizeTheFastStartBurst) {
  const uint16_t burst =
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_fast_start_burst);
  ASSERT_GT(burst, 2u) << "test needs a burst wider than the request below";
  constexpr uint16_t kNumFlows = 2;
  FastStart cc;
  for (uint16_t flowId = 0; flowId < kNumFlows; ++flowId) {
    cc.onFlowAdded(flowId);
  }
  cc.setPacketsPerVisit(2);

  for (uint16_t i = 0; i < burst; ++i) {
    EXPECT_EQ(sendNext(cc).flowId, 0)
        << "packet " << i
        << " stays on the first flow: fast start sizes the opening burst "
           "regardless of the narrower request";
  }
  EXPECT_EQ(sendNext(cc).flowId, 1)
      << "the walk advances only once the fast-start burst is placed";
}

/*
 * Asking must not advance the schedule: the ack-request decision is made while
 * building the packet, before the send is reported.
 */
TEST_F(FastStartTest, FlowForNextSendIsPure) {
  FastStart cc;
  cc.onFlowAdded(0);
  cc.onFlowAdded(1);

  const auto first = cc.flowForNextSend();
  const auto second = cc.flowForNextSend();
  ASSERT_TRUE(first.has_value() && second.has_value())
      << "fast start owns the schedule here, so both queries must report a "
         "flow";
  EXPECT_EQ(first->flowId, second->flowId)
      << "repeated queries must agree; a query that advanced the schedule "
         "could not be used to look ahead";
  EXPECT_EQ(first->lastOnFlow, second->lastOnFlow)
      << "the end-of-run answer must be stable too, since it is what decides "
         "the ack request on a packet not yet built";
}

/*
 * Out of fast start the window bounds a run, so the controller stops selecting
 * flows and the caller keeps its own rotation.
 */
TEST_F(FastStartTest, NoFlowChoiceOutsideFastStart) {
  FastStart cc;
  cc.onFlowAdded(0);
  EXPECT_TRUE(cc.flowForNextSend().has_value())
      << "fast start must select a flow before the ack, so the change below is "
         "the ack taking effect rather than the flow never being selected";

  cc.onPacketAck(0, CongestionController::AckEvent{.numAckedPackets = 1});
  EXPECT_FALSE(cc.flowForNextSend().has_value())
      << "after fast start the caller owns flow selection";
}

/*
 * qwnd is maintained where fwnd is, so a bare FastStart reports it too.
 */
TEST_F(FastStartTest, QwndTracksPacketsSentInFastStart) {
  FastStart cc;
  cc.onFlowAdded(0);
  cc.onPacketSent(0);
  cc.onPacketSent(0);
  EXPECT_EQ(cc.getQwnd(), 2u)
      << "qwnd counts packets sent in fast start; it moves with fwnd rather "
         "than relying on a subclass to increment it";
}

// Window held by a flow that will not send again is capacity the QP cannot
// use, and rate control keeps reducing against it.
TEST_F(FastStartTest, DisablingAFlowShedsItsShareOfTheQpWindow) {
  FastStart cc;
  constexpr uint16_t kLiveFlow = 0;
  constexpr uint16_t kDoomedFlow = 1;
  cc.onFlowAdded(kLiveFlow);
  cc.onFlowAdded(kDoomedFlow);

  for (int i = 0; i < 4; i++) {
    cc.onPacketSent(kLiveFlow);
  }
  for (int i = 0; i < 3; i++) {
    cc.onPacketSent(kDoomedFlow);
  }

  const uint32_t qwndBefore = cc.getQwnd();
  const uint16_t doomedFwnd = cc.getFwnd(kDoomedFlow);
  const uint16_t liveFwnd = cc.getFwnd(kLiveFlow);
  ASSERT_GT(doomedFwnd, 0)
      << "the flow must hold window for this case to have anything to shed";
  ASSERT_GT(liveFwnd, 0)
      << "flow 0 must already have window, or this exercises the donation "
         "case instead";

  cc.onFlowDisabled(kDoomedFlow);

  EXPECT_EQ(cc.getQwnd(), qwndBefore - doomedFwnd)
      << "the aggregate must shed exactly the disabled flow's window; keeping "
         "it reports capacity no flow can use";
  EXPECT_EQ(cc.getFwnd(kDoomedFlow), 0)
      << "a flow that will not send again must hold no window";
  EXPECT_EQ(cc.getFwnd(kLiveFlow), liveFwnd)
      << "flow 0 already had window, so it must not be topped up as well";

  cc.onFlowDisabled(kDoomedFlow);
  EXPECT_EQ(cc.getQwnd(), qwndBefore - doomedFwnd)
      << "a repeated disable must not subtract twice; callers may disable a "
         "flow that is already out of service";
}

// Flow 0 stays in service, so a report that it is disabled must not shed it.
TEST_F(FastStartTest, DisablingFlowZeroLeavesItsWindowAlone) {
  FastStart cc;
  constexpr uint16_t kLiveFlow = 0;
  constexpr uint16_t kOtherFlow = 1;
  cc.onFlowAdded(kLiveFlow);
  cc.onFlowAdded(kOtherFlow);

  for (int i = 0; i < 4; i++) {
    cc.onPacketSent(kLiveFlow);
    cc.onPacketSent(kOtherFlow);
  }

  const uint32_t qwndBefore = cc.getQwnd();
  const uint16_t liveFwnd = cc.getFwnd(kLiveFlow);
  ASSERT_GT(liveFwnd, 0)
      << "flow 0 must hold window for this case to show it is left alone";

  cc.onFlowDisabled(kLiveFlow);

  EXPECT_EQ(cc.getFwnd(kLiveFlow), liveFwnd)
      << "flow 0 stays in service, so its window must not be shed";
  EXPECT_EQ(cc.getQwnd(), qwndBefore)
      << "the aggregate must be unchanged, since nothing left it";
}

} // namespace libsoftmetaroce
