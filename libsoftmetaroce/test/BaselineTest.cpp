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
#include <libsoftmetaroce/congestion_control/Baseline.h>

DECLARE_int32(libsoftmetaroce_max_qwnd);

using namespace testing;
namespace libsoftmetaroce {

// Test subclass to expose protected members for verification
class TestableBaseline : public Baseline {
 public:
  using Baseline::Baseline;

  int getQwndMax() const {
    return qwnd_max_;
  }
};

class BaselineTest : public ::testing::Test {};

/*
 * Simple test of fast start cc.  Create flow, send packets, ack packets,
 * confirm window is limited to the number of packets sent before the ack.
 */
TEST_F(BaselineTest, Basic) {
  const int fwndLimit = 5;
  const int epsilon = 2;
  const int beta = 1; // managed as inverse log (1 -> 1/2^{1})
  const int flowId = 0;
  Baseline cc(epsilon, beta);

  cc.onFlowAdded(flowId);

  // should ack as fast start, there is positive awnd on all flows.
  EXPECT_GT(cc.getAvailableWindowSize(flowId), 0);
  for (int i = 0; i < fwndLimit; i++) {
    cc.onPacketSent(flowId);
    EXPECT_GT(cc.getAvailableWindowSize(flowId), 0);
  }

  // reuse the ack event for a one-packet ack. It holds a real
  // sample below the average: an ack with none is not evidence of low delay.
  auto ackEventNoCE = CongestionController::AckEvent{
      .numAckedPackets = 1, .measuredRttNs = 1000, .smoothedQpRttNs = 2000};

  // Process an ack.  On this ack, the cc will exit fast start,
  // meaning the qwnd and fwnd windows stop growing.  Because this
  // packet is acked, the available window should be 1.
  cc.onPacketAck(flowId, ackEventNoCE);
  EXPECT_EQ(cc.getAvailableWindowSize(flowId), 1);

  // simulate expected behavior, where we would send into this
  // available window.  in flight = fwnd limit
  cc.onPacketSent(flowId);
  EXPECT_EQ(cc.getAvailableWindowSize(flowId), 0);

  // two packets acked with ce should allow us to sent one packet.
  auto ackEvent2CE = CongestionController::AckEvent{
      .numAckedPackets = 2, .numAckedPacketsWithCE = 2};
  cc.onPacketAck(flowId, ackEvent2CE);
  // in flight = fwndLimit - 2
  // fwnd = fwndLimit - 1
  EXPECT_EQ(cc.getAvailableWindowSize(flowId), 1);

  // simulate expected behavior, fill the window.
  cc.onPacketSent(flowId);
  // in flight = fwnd = fwndLimit -1
  EXPECT_EQ(cc.getAvailableWindowSize(flowId), 0);

  // ack the whole window uncongested -> increment the window by epsilon.
  for (int i = 1; i < fwndLimit; i++) {
    cc.onPacketAck(flowId, ackEventNoCE);
    // as we ack one, awnd should increase >= one: more if fwnd is
    // increased.
    EXPECT_GE(cc.getAvailableWindowSize(flowId), 1)
        << "each acked packet should free at least its own slot in the window";
    while (cc.getAvailableWindowSize(flowId) > 0) {
      cc.onPacketSent(flowId);
    }
  }

  // with epsilon = 2, fwnd should have increased by 2 with a window's
  // worth of acknowledgment after having been decremented by one.
  EXPECT_EQ(cc.getFwnd(flowId), fwndLimit + 1)
      << "a window of uncongested acks from a window-limited flow should grow "
         "it by epsilon";
}

/*
 * An ack event can carry no round-trip sample: the peer sent no timestamp, or
 * the event reports packets leaving the window rather than a reply arriving.
 * Zero says nothing about delay, and must not be read as the fastest possible
 * flow, which is what comparing it against the average would make it.
 */
TEST_F(BaselineTest, AnAckWithNoRttSampleDoesNotGrowTheWindow) {
  const int epsilon = 2;
  const int beta = 1;
  const int flowId = 0;
  Baseline cc(epsilon, beta);
  cc.onFlowAdded(flowId);

  const int initialFwnd = 4;
  for (int i = 0; i < initialFwnd; i++) {
    cc.onPacketSent(flowId);
  }

  // Exit fast start on a sample the flow really produced.
  cc.onPacketAck(
      flowId,
      CongestionController::AckEvent{
          .numAckedPackets = 1,
          .measuredRttNs = 1000,
          .smoothedQpRttNs = 2000});
  const uint32_t qwndAfterFastStart = cc.getQwnd();
  const uint16_t fwndAfterFastStart = cc.getFwnd(flowId);

  const auto noSampleAck = CongestionController::AckEvent{
      .numAckedPackets = 1, .measuredRttNs = 0, .smoothedQpRttNs = 2000};
  for (int i = 1; i < initialFwnd; i++) {
    cc.onPacketAck(flowId, noSampleAck);
  }

  EXPECT_EQ(cc.getQwnd(), qwndAfterFastStart)
      << "an ack carrying no delay measurement is not evidence the flow is "
         "running below the average, so it must not earn window growth";
  EXPECT_EQ(cc.getFwnd(flowId), fwndAfterFastStart)
      << "the same must hold for the flow window, which the qwnd invariant "
         "ties to it";
}

/*
 * Verify that high-delay acks do not grow the window, and that
 * low-delay acks on the same flow do.
 */
TEST_F(BaselineTest, HighDelayAckDoesNotIncreaseWindow) {
  const int epsilon = 2;
  const int beta = 1;
  const int flowId = 0;
  Baseline cc(epsilon, beta);
  cc.onFlowAdded(flowId);

  // send enough to exit fast start
  const int initialFwnd = 4;
  for (int i = 0; i < initialFwnd; i++) {
    cc.onPacketSent(flowId);
  }

  // first ack exits fast start
  auto lowDelayAck = CongestionController::AckEvent{
      .numAckedPackets = 1, .measuredRttNs = 1000, .smoothedQpRttNs = 2000};
  cc.onPacketAck(flowId, lowDelayAck);

  uint32_t qwndAfterFastStart = cc.getQwnd();
  uint16_t fwndAfterFastStart = cc.getFwnd(flowId);
  EXPECT_EQ(qwndAfterFastStart, fwndAfterFastStart)
      << "qwnd must equal fwnd on a single flow";

  // ack remaining packets with high delay (mrtt > srtt); window must
  // not grow even though there are enough acks for epsilon to trigger.
  auto highDelayAck = CongestionController::AckEvent{
      .numAckedPackets = 1, .measuredRttNs = 5000, .smoothedQpRttNs = 2000};
  for (int i = 1; i < initialFwnd; i++) {
    cc.onPacketAck(flowId, highDelayAck);
  }
  EXPECT_EQ(cc.getQwnd(), qwndAfterFastStart)
      << "high-delay acks should not grow qwnd";
  EXPECT_EQ(cc.getFwnd(flowId), fwndAfterFastStart)
      << "high-delay acks should not grow fwnd";

  // now ack with low delay; the accumulated fractional credit should
  // cause the window to increase. The window is filled first, so the flow is
  // window limited when the ack arrives.
  for (int i = 0; i < initialFwnd; i++) {
    cc.onPacketSent(flowId);
  }
  cc.onPacketAck(flowId, lowDelayAck);
  EXPECT_GT(cc.getQwnd(), qwndAfterFastStart)
      << "low-delay ack should allow deferred window increase";
  EXPECT_EQ(cc.getQwnd(), cc.getFwnd(flowId))
      << "qwnd must equal fwnd on a single flow after increase";
}

TEST_F(BaselineTest, QwndMaxDefaultIsMaxFwndTimesNumFlows) {
  const int flowId = 0;
  TestableBaseline cc(2, 1);
  cc.onFlowAdded(flowId);

  EXPECT_EQ(cc.getQwndMax(), 128)
      << "qwnd_max should be maxFwndFlag() (128) * numFlows (1)";
}

TEST_F(BaselineTest, AnAckThatLeavesFastStartRunningDoesNotGrowTheWindow) {
  Baseline cc(/*epsilon=*/16, 1);
  cc.onFlowAdded(0);
  for (int sent = 0; sent < 4; ++sent) {
    cc.onPacketSent(0);
  }

  cc.onPacketAck(
      0,
      CongestionController::AckEvent{
          .numAckedPackets = 2,
          .measuredRttNs = 1000,
          .smoothedQpRttNs = 2000,
          .sendPending = false});

  EXPECT_EQ(cc.getQwnd(), 4)
      << "while fast start runs the window grows as packets are sent; the "
         "ack should not grow it too";
}

TEST_F(BaselineTest, AnAckFromAFlowNotWindowLimitedDoesNotGrowTheWindow) {
  constexpr int kEpsilon = 16;
  Baseline cc(kEpsilon, 1);
  cc.onFlowAdded(0);
  for (int sent = 0; sent < 8; ++sent) {
    cc.onPacketSent(0);
  }
  const auto lowDelayAck = [](uint16_t acked) {
    return CongestionController::AckEvent{
        .numAckedPackets = acked,
        .measuredRttNs = 1000,
        .smoothedQpRttNs = 2000};
  };
  cc.onPacketAck(0, lowDelayAck(6));
  ASSERT_EQ(cc.getQwnd(), 12)
      << "precondition: with all eight outstanding the flow was window "
         "limited, so the ack of six grows the window";

  cc.onPacketAck(0, lowDelayAck(1));

  EXPECT_EQ(cc.getQwnd(), 12)
      << "with two of twelve outstanding the flow was not window limited, "
         "so its ack should not grow the window";
}

TEST_F(BaselineTest, LossReducesTheWindowByLambdaNotBeta) {
  constexpr int kBetaEighth = 3;
  constexpr int kLambdaWhole = 0;
  constexpr int kSentInFastStart = 8;
  constexpr uint16_t kLost = 2;
  Baseline cc(/*epsilon=*/2, kBetaEighth, kLambdaWhole);
  cc.onFlowAdded(0);
  for (int sent = 0; sent < kSentInFastStart; ++sent) {
    cc.onPacketSent(/*flowId=*/0);
  }
  cc.onPacketAck(
      /*flowId=*/0,
      CongestionController::AckEvent{
          .numAckedPackets = 1,
          .measuredRttNs = 1000,
          .smoothedQpRttNs = 2000});
  ASSERT_EQ(cc.getQwnd(), kSentInFastStart)
      << "precondition: the ack ends fast start without growing the window";

  cc.onPacketLoss(
      /*flowId=*/0,
      CongestionController::LossEvent{
          .numLostPackets = kLost,
          .lossType = CongestionController::LossEvent::LossType::OutOfOrder});

  EXPECT_EQ(cc.getQwnd(), kSentInFastStart - kLost)
      << "a lambda of zero should take a whole packet off the window per lost "
         "packet; beta, an eighth here, is for congestion marks";
}

TEST_F(BaselineTest, AnAckOfSeveralPacketsCanClaimSeveralIncreases) {
  constexpr int kEpsilon = 8;
  constexpr int kSentInFastStart = 4;
  Baseline cc(kEpsilon, 1);
  cc.onFlowAdded(0);
  for (int sent = 0; sent < kSentInFastStart; ++sent) {
    cc.onPacketSent(0);
  }
  ASSERT_EQ(cc.getQwnd(), kSentInFastStart)
      << "precondition: fast start leaves the window at the packets sent, "
         "below epsilon";

  cc.onPacketAck(
      0,
      CongestionController::AckEvent{
          .numAckedPackets = 3,
          .measuredRttNs = 1000,
          .smoothedQpRttNs = 2000});

  // Three packets each add min(epsilon, qwnd) = 4 to the fractional, 12 in
  // all: enough to claim at 12 > 4 and again at 8 > 5, but not at 3 > 6.
  EXPECT_EQ(cc.getQwnd(), 6)
      << "an ack whose packets bank more than a window of credit should "
         "claim each whole packet of it, not one per ack";
  EXPECT_EQ(cc.getFwnd(0), 6)
      << "the acked flow's window should take each claim, keeping qwnd the "
         "sum of the flow windows";
}

TEST_F(BaselineTest, CreditAFlowCannotClaimWaitsForAnotherFlowsAck) {
  Baseline cc(/*epsilon=*/8, 1);
  cc.onFlowAdded(0);
  cc.onFlowAdded(1);
  for (uint16_t flowId : {0, 0, 1, 1}) {
    cc.onPacketSent(flowId);
  }
  cc.onPacketAck(
      1,
      CongestionController::AckEvent{
          .numAckedPackets = 2,
          .measuredRttNs = 5000,
          .smoothedQpRttNs = 2000});
  ASSERT_EQ(cc.getQwnd(), 4)
      << "precondition: flow 1's ack came back slower than the QP's smoothed "
         "rtt, so it claims none of the credit it banks";

  cc.onPacketAck(
      0,
      CongestionController::AckEvent{
          .numAckedPackets = 1,
          .measuredRttNs = 1000,
          .smoothedQpRttNs = 2000});

  // Flow 1 banked 2 * 4 = 8; flow 0 adds 4 more, claiming at 12 > 4 and 8 > 5.
  EXPECT_EQ(cc.getQwnd(), 6)
      << "the credit flow 1 could not claim should stay banked for flow 0's "
         "ack to claim";
  EXPECT_EQ(cc.getFwnd(0), 4)
      << "flow 0, the eligible flow, should take the claimed window";
}

TEST_F(BaselineTest, FastStartEndsWhenTheWindowReachesQwndMax) {
  gflags::FlagSaver saver;
  constexpr int kQwndMax = 3;
  gflags::SetCommandLineOption(
      "libsoftmetaroce_max_qwnd", std::to_string(kQwndMax).c_str());
  TestableBaseline cc(2, 1);
  cc.onFlowAdded(0);
  cc.onFlowAdded(1);
  ASSERT_EQ(cc.getQwndMax(), kQwndMax)
      << "precondition: the qwnd cap sets the window fast start may reach";

  for (int sent = 0; sent < kQwndMax - 1; ++sent) {
    cc.onPacketSent(0);
  }
  ASSERT_TRUE(cc.flowForNextSend().has_value())
      << "precondition: below the cap the controller is still in fast start";
  cc.onPacketSent(0);

  EXPECT_FALSE(cc.flowForNextSend().has_value())
      << "the controller should leave fast start once the window reaches "
         "qwnd_max, rather than wait for an ack";
}

TEST_F(BaselineTest, TheFormerMaxWindowNameStillSizesTheWindow) {
  gflags::FlagSaver saver;
  ASSERT_EQ(FLAGS_libsoftmetaroce_max_qwnd, 0)
      << "precondition: the qwnd cap is off, so the flag under test is what "
         "decides the window";
  gflags::SetCommandLineOption("libsoftmetaroce_max_pwnd", "64");

  TestableBaseline cc(2, 1);
  cc.onFlowAdded(0);

  EXPECT_EQ(cc.getQwndMax(), 64)
      << "an operator still setting --libsoftmetaroce_max_pwnd has to keep "
         "getting the window they asked for; an alias that resolves but never "
         "reaches the controller silently hands them the default instead";
}

TEST_F(BaselineTest, TheCurrentMaxWindowNameSizesTheWindow) {
  gflags::FlagSaver saver;
  ASSERT_EQ(FLAGS_libsoftmetaroce_max_qwnd, 0)
      << "precondition: the qwnd cap is off, so the flag under test is what "
         "decides the window";
  gflags::SetCommandLineOption("libsoftmetaroce_max_fwnd", "64");

  TestableBaseline cc(2, 1);
  cc.onFlowAdded(0);

  EXPECT_EQ(cc.getQwndMax(), 64)
      << "the new name has to reach the same window as the one it replaces, or "
         "nobody can migrate off the former name";
}

TEST_F(BaselineTest, QwndMaxLimitedByFlag) {
  FLAGS_libsoftmetaroce_max_qwnd = 5;
  const int flowId = 0;
  TestableBaseline cc(2, 1);
  cc.onFlowAdded(flowId);

  EXPECT_EQ(cc.getQwndMax(), 5)
      << "qwnd_max should be capped by the max_qwnd flag";

  // During fast start, window should close after 5 packets
  for (int i = 0; i < 5; i++) {
    EXPECT_GT(cc.getAvailableWindowSize(flowId), 0)
        << "should have window available at packet " << i;
    cc.onPacketSent(flowId);
  }
  EXPECT_EQ(cc.getAvailableWindowSize(flowId), 0)
      << "window should be exhausted after qwnd_max packets";

  FLAGS_libsoftmetaroce_max_qwnd = 0;
}

// A flow left with no window draws no ack, so nothing grows it back.
TEST_F(BaselineTest, DisablingAFlowRestoresTheMinimumWindowOnFlowZero) {
  constexpr int epsilon = 2;
  constexpr int beta = 1;
  Baseline cc(epsilon, beta);
  constexpr uint16_t kLiveFlow = 0;
  constexpr uint16_t kDoomedFlow = 1;
  cc.onFlowAdded(kLiveFlow);
  cc.onFlowAdded(kDoomedFlow);

  // Only the doomed flow sends, so flow 0 is left with no window.
  for (int i = 0; i < 3; i++) {
    cc.onPacketSent(kDoomedFlow);
  }
  ASSERT_EQ(cc.getFwnd(kLiveFlow), 0)
      << "flow 0 must start with no window for this case to test the minimum";

  cc.onFlowDisabled(kDoomedFlow);

  EXPECT_EQ(cc.getQwnd(), static_cast<uint32_t>(FLAGS_libsoftmetaroce_min_qwnd))
      << "the QP must be left with its minimum window";
  EXPECT_EQ(
      cc.getFwnd(kLiveFlow),
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_min_qwnd))
      << "that minimum must sit on flow 0, the flow still in service; on any "
         "other flow the QP still cannot send";
  EXPECT_GT(cc.getAvailableWindowSize(kLiveFlow), 0)
      << "the restored window must be usable, not merely recorded";
}

} // namespace libsoftmetaroce
