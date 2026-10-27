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
#include <libsoftmetaroce/congestion_control/BaselineWithRCN.h>

DECLARE_int32(libsoftmetaroce_max_qwnd);
DECLARE_int32(libsoftmetaroce_fwnd_floor);

using namespace testing;
namespace libsoftmetaroce {

// Test subclass to expose protected members for verification
class TestableBaselineWithRCN : public BaselineWithRCN {
 public:
  using BaselineWithRCN::BaselineWithRCN;

  int getQwndMax() const {
    return qwnd_max_;
  }

  uint32_t getQwnd() const override {
    return static_cast<uint32_t>(qwnd_);
  }
};

class BaselineWithRCNTest : public ::testing::Test {
 protected:
  static constexpr int kEpsilon = 2;
  static constexpr int kBeta = 1;
  static constexpr uint32_t kGammaNumerator = 16; // gamma = 1.0
  static constexpr uint32_t kOmegaNs = 0;
  static constexpr int kFlowId = 0;
  static constexpr uint32_t kOneMillisecondNs = 1'000'000;
  static constexpr uint32_t kOneSecondNs = 1'000'000'000;
  static constexpr uint32_t kOneGbps = 1;

  // Helper to create AckEvent with defaults
  static CongestionController::AckEvent makeAck(
      uint16_t numAcked = 1,
      uint16_t numCE = 0,
      uint32_t rttNs = kOneMillisecondNs,
      uint32_t rcnRate = kOneGbps) {
    return {
        .numAckedPackets = numAcked,
        .numAckedPacketsWithCE = numCE,
        .measuredRttNs = rttNs,
        .smoothedQpRttNs = rttNs,
        .rcnDesiredRateGbps = rcnRate,
    };
  }

  // Helper to send N packets
  static void sendPackets(TestableBaselineWithRCN& cc, int count) {
    for (int i = 0; i < count; i++) {
      cc.onPacketSent(kFlowId);
    }
  }

  // Helper to exit fast start (required before testing RCN reduction)
  // Sets a high RCN (1Tbps) to avoid dropping qwnd eagerly.
  static void exitFastStart(TestableBaselineWithRCN& cc) {
    cc.onPacketAck(kFlowId, makeAck(1, 0, kOneMillisecondNs, 1000));
  }

  // Helper to open ten packets on each of numFlows flows and exit fast start.
  static void openFlows(TestableBaselineWithRCN& cc, int numFlows) {
    for (int f = 0; f < numFlows; f++) {
      cc.onFlowAdded(static_cast<uint16_t>(f));
    }
    for (int f = 0; f < numFlows; f++) {
      for (int i = 0; i < 10; i++) {
        cc.onPacketSent(static_cast<uint16_t>(f));
      }
    }
    exitFastStart(cc);
  }

  // Helper to ack every flow round-robin at the given rate and RTT, driving
  // the aggregate toward the max those compute.
  static void ackAllFlows(
      TestableBaselineWithRCN& cc,
      int numFlows,
      int rounds,
      uint32_t rttNs,
      uint32_t rateGbps) {
    for (int i = 0; i < rounds; i++) {
      for (int f = 0; f < numFlows; f++) {
        cc.onPacketAck(
            static_cast<uint16_t>(f), makeAck(1, 0, rttNs, rateGbps));
      }
    }
  }
};

TEST_F(BaselineWithRCNTest, QwndMaxUpdatedFromRCN) {
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 5);

  cc.onPacketAck(kFlowId, makeAck(1, 0, kOneSecondNs, kOneGbps));

  EXPECT_EQ(cc.getQwndMax(), 30518)
      << "ceil(16 * 1Gbps * (1e9 + 0)ns / (4096 * 8 * 16)) = ceil(30517.578) = "
         "30518";
}

TEST_F(BaselineWithRCNTest, QwndMaxWithShortRtt) {
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 5);

  cc.onPacketAck(kFlowId, makeAck());

  EXPECT_EQ(cc.getQwndMax(), 31)
      << "ceil(16 * 1Gbps * (1e6 + 0)ns / (4096 * 8 * 16)) = ceil(30.517) = 31";
}

TEST_F(BaselineWithRCNTest, QwndMaxWithCustomMTU) {
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.setMTU(2048);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 5);

  cc.onPacketAck(kFlowId, makeAck());

  EXPECT_EQ(cc.getQwndMax(), 62)
      << "ceil(16 * 1Gbps * (1e6 + 0)ns / (2048 * 8 * 16)) = ceil(61.035) = 62";
}

TEST_F(BaselineWithRCNTest, QwndReducedWhenExceedsMax) {
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 40);
  EXPECT_EQ(cc.getQwnd(), 40);
  exitFastStart(cc);
  EXPECT_EQ(cc.getQwnd(), 40);

  cc.onPacketAck(kFlowId, makeAck(2, 0));

  EXPECT_EQ(cc.getQwndMax(), 31);
  EXPECT_EQ(cc.getQwnd(), 39)
      << "qwnd(40) > qwnd_max(31): reduction = numAcked(2) >> 1 = 1";
}

TEST_F(BaselineWithRCNTest, QwndReducedFasterWhenExceedsTwiceMax) {
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 70);
  exitFastStart(cc);
  EXPECT_EQ(cc.getQwnd(), 70);

  cc.onPacketAck(kFlowId, makeAck(2, 0));

  EXPECT_EQ(cc.getQwndMax(), 31);
  EXPECT_EQ(cc.getQwnd(), 68)
      << "qwnd(70) > 2*qwnd_max(62): reduction = numAcked(2) >> 0 = 2";
}

TEST_F(BaselineWithRCNTest, QwndMaxWithDefaultGammaAndOmega) {
  TestableBaselineWithRCN cc(
      kEpsilon, kBeta); // Uses default gamma=1.0 (16/16), omega=1000ns
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 5);

  cc.onPacketAck(kFlowId, makeAck());

  EXPECT_EQ(cc.getQwndMax(), 31)
      << "ceil(16 * 1Gbps * (1e6 + 1000)ns / (4096 * 8 * 16)) = ceil(30.548) = "
         "31";
}

TEST_F(BaselineWithRCNTest, EcnSuppressedWhenQwndExceedsMax) {
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 40);
  exitFastStart(cc);
  EXPECT_EQ(cc.getQwnd(), 40);

  cc.onPacketAck(kFlowId, makeAck(2, 4)); // 4 CE marks

  EXPECT_EQ(cc.getQwndMax(), 31);
  EXPECT_EQ(cc.getQwnd(), 39)
      << "qwnd(40) > qwnd_max(31): RCN reduction = numAcked(2) >> 1 = 1, "
         "ECN marks suppressed";
}

TEST_F(BaselineWithRCNTest, EcnAppliedWhenQwndBelowMax) {
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 30);
  exitFastStart(cc);
  EXPECT_EQ(cc.getQwnd(), 30);

  cc.onPacketAck(kFlowId, makeAck(2, 4)); // 4 CE marks

  EXPECT_EQ(cc.getQwndMax(), 31);
  EXPECT_EQ(cc.getQwnd(), 28)
      << "qwnd(30) <= qwnd_max(31): ECN reduction = numCE(4) >> beta(1) = 2";
}

TEST_F(BaselineWithRCNTest, RcnReductionIndependentOfBeta) {
  // Use beta=3 (ECN would reduce by signal >> 3 = signal/8)
  // RCN should still use fixed shift=1 (reduce by signal >> 1 = signal/2)
  const int betaThree = 3;
  TestableBaselineWithRCN cc(kEpsilon, betaThree, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 40);
  exitFastStart(cc);
  EXPECT_EQ(cc.getQwnd(), 40);

  // Send 8 acked packets to make the difference visible:
  // If RCN used beta=3: reduction = 8 >> 3 = 1
  // With fixed shift=1: reduction = 8 >> 1 = 4
  cc.onPacketAck(kFlowId, makeAck(8, 0));

  EXPECT_EQ(cc.getQwndMax(), 31);
  EXPECT_EQ(cc.getQwnd(), 36)
      << "RCN reduction uses fixed shift=1: numAcked(8) >> 1 = 4, "
         "independent of beta(3)";
}

TEST_F(BaselineWithRCNTest, QwndMaxCappedByFlag) {
  FLAGS_libsoftmetaroce_max_qwnd = 20;
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 5);

  // RCN would compute qwnd_max = ceil(16 * 1Gbps * (1e6 + 0)ns /
  // (4096 * 8 * 16)) = 31, but the flag caps it to 20.
  cc.onPacketAck(kFlowId, makeAck());

  EXPECT_EQ(cc.getQwndMax(), 20)
      << "RCN-computed qwnd_max(31) should be capped by flag to 20";

  FLAGS_libsoftmetaroce_max_qwnd = 0;
}

TEST_F(BaselineWithRCNTest, QwndMaxNotCappedWhenFlagIsZero) {
  FLAGS_libsoftmetaroce_max_qwnd = 0;
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 5);

  cc.onPacketAck(kFlowId, makeAck());

  EXPECT_EQ(cc.getQwndMax(), 31)
      << "flag=0 means unlimited; RCN-computed qwnd_max should be used as-is";
}

/*
 * RCN must leave a protected flow alone (protocol spec 6.3.3).
 *
 * A flow is protected when it is already at or below fwnd_floor and the
 * computed max is at least (flowId + 1) * fwnd_floor: the max covers the floors
 * of this flow and every lower-numbered one, so rate reduction takes its
 * packets from higher-numbered flows. The flowId term protects low-numbered
 * flows deterministically, and flow 0 is protected at any max.
 *
 * This is a floor rather than a minimum -- congestion response may still take a
 * flow below it. Only RCN is constrained here.
 *
 * The second flow matters: it holds window of its own, so the aggregate stays
 * above the QP-wide minimum and that separate rule never engages. Without the
 * protected-flow rule the acking flow is reduced to a zero window, which stalls
 * it for good, since it cannot send, so it draws no ack, so nothing grows it
 * back.
 */
TEST_F(BaselineWithRCNTest, RcnLeavesAProtectedFlowAlone) {
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  constexpr uint16_t kOtherFlowId = 1;
  cc.onFlowAdded(kFlowId);
  cc.onFlowAdded(kOtherFlowId);

  sendPackets(cc, 32);
  for (int i = 0; i < 32; i++) {
    cc.onPacketSent(kOtherFlowId);
  }
  exitFastStart(cc);

  // A tiny desired rate holds the cap far below the aggregate, so every ack
  // asks for a reduction, and they all land on the flow that is acking.
  for (int i = 0; i < 64; i++) {
    cc.onPacketAck(kFlowId, makeAck(1, 0, kOneMillisecondNs, kOneGbps));
    ASSERT_GT(cc.getFwnd(kFlowId), 0)
        << "ack " << i
        << " left flow 0 with no window; RCN may not reduce a protected flow "
           "below the floor, and a flow with no window draws no ack to grow "
           "it back";
  }

  EXPECT_GT(cc.getAvailableWindowSize(kFlowId), 0)
      << "a protected flow with nothing in flight must still be able to send, "
         "or the QP stalls with work queued";
}

/*
 * Spec 6.3.3 excepts flow 0 from RCN reduction below the floor, while any
 * other flow keeps its floor only when the max is at least the floors
 * through it. A floor above the computed max makes the contrast visible in
 * one run: flow 0 stays although the max is below even one floor, while
 * flow 1 sheds although both sit below the floor.
 */
TEST_F(BaselineWithRCNTest, RcnProtectsFlowZeroButNotFlowOneBelowTheFloor) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_fwnd_floor = 100;
  constexpr uint16_t kOtherFlowId = 1;

  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  cc.onFlowAdded(kOtherFlowId);
  sendPackets(cc, 32);
  for (int i = 0; i < 32; i++) {
    cc.onPacketSent(kOtherFlowId);
  }
  exitFastStart(cc);

  ASSERT_LT(cc.getFwnd(kFlowId), FLAGS_libsoftmetaroce_fwnd_floor)
      << "flow 0 must start under the floor, or the protected-flow rule never "
         "applies and the case proves nothing about the threshold";
  ASSERT_LT(cc.getFwnd(kOtherFlowId), FLAGS_libsoftmetaroce_fwnd_floor)
      << "flow 1 must start under the floor too, so the threshold each flow "
         "compares against is the only reason the two are treated differently";
  for (int i = 0; i < 64; i++) {
    cc.onPacketAck(kFlowId, makeAck(1, 0, kOneMillisecondNs, kOneGbps));
    cc.onPacketAck(kOtherFlowId, makeAck(1, 0, kOneMillisecondNs, kOneGbps));
  }

  EXPECT_GT(cc.getFwnd(kFlowId), 0)
      << "flow 0 is excepted from RCN reduction below the floor, so RCN "
         "must leave it alone whatever the floor is set to";
  EXPECT_LT(cc.getFwnd(kOtherFlowId), cc.getFwnd(kFlowId))
      << "flow 1 keeps its floor only while the max is at least the two "
         "floors through it; here it is not, so RCN may reduce flow 1 "
         "like any other flow";
}

/*
 * RCN sheds the flows past what the max keeps until the aggregate meets
 * it. With max 10 and the floor 2, flows 0-4 keep their floor while
 * flows 5-7 shed to zero, since the max is exactly five floors.
 */
TEST_F(BaselineWithRCNTest, RcnShedsHighFlowsToReachMax) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_fwnd_floor = 2;
  constexpr int kNumFlows = 8;
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  openFlows(cc, kNumFlows);
  ASSERT_EQ(cc.getQwnd(), 80)
      << "precondition: ten sends on each of eight flows open an aggregate "
         "of 80, or the descent below never starts above the max";

  // Rate 1Gbps at 300us computes max 10, below the floored sum 16, so every
  // ack asks for a reduction until the aggregate meets the max.
  constexpr uint32_t kRttNs = 300'000;
  ackAllFlows(cc, kNumFlows, 400, kRttNs, kOneGbps);

  ASSERT_EQ(cc.getQwndMax(), 10)
      << "precondition: RCN derives this max from the rate and RTT, or the "
         "descent below exercises the wrong regime; rate is "
      << kOneGbps << " Gbps and RTT is " << kRttNs << "ns";
  EXPECT_EQ(cc.getQwnd(), 10)
      << "reduction must stop at the computed max; an aggregate parked at "
         "the floored sum 16 sits above the maximum";
  for (int f = 0; f <= 4; f++) {
    EXPECT_EQ(cc.getFwnd(static_cast<uint16_t>(f)), 2)
        << "flow " << f
        << ": the max is at least the floors through this flow, so RCN "
           "must leave its floor alone";
  }
  for (int f = 5; f < kNumFlows; f++) {
    EXPECT_EQ(cc.getFwnd(static_cast<uint16_t>(f)), 0)
        << "flow " << f
        << " is past what the max keeps, so RCN must shed it to reach 10";
  }
}

/*
 * A max past a whole number of floors holds the remainder past the kept
 * floors. With max 11 and the floor 2, flows 0-4 keep theirs while
 * flows 5-7 shed all but one packet, since nothing fires once the
 * aggregate meets the max.
 */
TEST_F(BaselineWithRCNTest, RcnHoldsOnePacketPastTheKeptFloors) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_fwnd_floor = 2;
  constexpr int kNumFlows = 8;
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  openFlows(cc, kNumFlows);
  ASSERT_EQ(cc.getQwnd(), 80)
      << "precondition: ten sends on each of eight flows open an aggregate "
         "of 80, or the descent below never starts above the max";

  // Rate 1Gbps at 350us computes max 11. Reductions fire only while the
  // aggregate is above the max, so the descent stops at 11 rather than at
  // the five kept floors.
  constexpr uint32_t kRttNs = 350'000;
  ackAllFlows(cc, kNumFlows, 400, kRttNs, kOneGbps);

  ASSERT_EQ(cc.getQwndMax(), 11)
      << "precondition: RCN derives this max from the rate and RTT, or the "
         "descent below exercises the wrong regime; rate is "
      << kOneGbps << " Gbps and RTT is " << kRttNs << "ns";
  EXPECT_EQ(cc.getQwnd(), 11)
      << "the descent must stop at the computed max; shedding the last "
         "packet would drop the aggregate below the maximum";
  for (int f = 0; f <= 4; f++) {
    EXPECT_EQ(cc.getFwnd(static_cast<uint16_t>(f)), 2)
        << "flow " << f
        << ": the max is at least the floors through this flow, so RCN "
           "must leave its floor alone";
  }
  EXPECT_EQ(cc.getFwnd(5) + cc.getFwnd(6) + cc.getFwnd(7), 1)
      << "one packet above the kept floors must survive past them, "
         "since nothing fires once the aggregate meets the max; qwnd is "
      << cc.getQwnd();
}

/*
 * A higher max keeps higher-numbered floors. With max 13 and the floor 2,
 * flows 0-5 keep theirs while flows 6-7 shed all but one packet.
 */
TEST_F(BaselineWithRCNTest, RcnKeepsAnotherFloorAtHigherMax) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_fwnd_floor = 2;
  constexpr int kNumFlows = 8;
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  openFlows(cc, kNumFlows);
  ASSERT_EQ(cc.getQwnd(), 80)
      << "precondition: ten sends on each of eight flows open an aggregate "
         "of 80, or the descent below never starts above the max";

  // Rate 1Gbps at 410us computes max 13, which is six floors with one
  // packet over.
  constexpr uint32_t kRttNs = 410'000;
  ackAllFlows(cc, kNumFlows, 400, kRttNs, kOneGbps);

  ASSERT_EQ(cc.getQwndMax(), 13)
      << "precondition: RCN derives this max from the rate and RTT, or the "
         "descent below exercises the wrong regime; rate is "
      << kOneGbps << " Gbps and RTT is " << kRttNs << "ns";
  EXPECT_EQ(cc.getQwnd(), 13)
      << "the descent must stop at the computed max; shedding the last "
         "packet would drop the aggregate below the maximum";
  for (int f = 0; f <= 5; f++) {
    EXPECT_EQ(cc.getFwnd(static_cast<uint16_t>(f)), 2)
        << "flow " << f
        << ": the max is at least the floors through this flow, so RCN "
           "must leave its floor alone";
  }
  EXPECT_EQ(cc.getFwnd(6) + cc.getFwnd(7), 1)
      << "one packet above the kept floors must survive past them, "
         "since nothing fires once the aggregate meets the max; qwnd is "
      << cc.getQwnd();
}

/*
 * A floor, not a minimum: the protected-flow rule constrains RCN alone, so
 * congestion response may still take a flow below it. Holding the rate cap
 * above the aggregate keeps RCN out of the way, leaving the CE marks as the
 * only thing reducing the flow.
 */
TEST_F(BaselineWithRCNTest, CongestionResponseStillReducesBelowTheFloor) {
  TestableBaselineWithRCN cc(kEpsilon, kBeta, kGammaNumerator, kOmegaNs);
  cc.onFlowAdded(kFlowId);
  sendPackets(cc, 32);
  exitFastStart(cc);

  const int floor = FLAGS_libsoftmetaroce_fwnd_floor;
  for (int i = 0; i < 64 && cc.getFwnd(kFlowId) >= floor; i++) {
    cc.onPacketAck(kFlowId, makeAck(1, 4, kOneMillisecondNs, 1000));
  }

  EXPECT_LT(cc.getFwnd(kFlowId), floor)
      << "ECN must still be able to reduce a flow below the floor; a floor "
         "that congestion response could not cross would be a minimum";
}

} // namespace libsoftmetaroce
