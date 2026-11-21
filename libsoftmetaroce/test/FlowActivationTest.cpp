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
#include <libsoftmetaroce/EgressPacket.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/congestion_control/Baseline.h>
#include <libsoftmetaroce/congestion_control/BaselineWithRCN.h>
#include <libsoftmetaroce/congestion_control/BootstrappingFlow.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/QpTestBase.h>
#include <libsoftmetaroce/test/TxTimerSuppressor.h>
#include <array>
#include <memory>
#include <set>
#include <vector>

namespace libsoftmetaroce {

class FlowActivationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_libsoftmetaroce_activate_tx_flows = true;
    FLAGS_libsoftmetaroce_exponential_flow_activation_threshold = false;
    FLAGS_libsoftmetaroce_average_window_log2 = 1;
    FLAGS_libsoftmetaroce_fast_start_burst = kBurst;
  }

  static void fill(CongestionController& cc, uint16_t flowId) {
    while (cc.getAvailableWindowSize(flowId) > 0) {
      cc.onPacketSent(flowId);
    }
  }

  static void ack(
      CongestionController& cc,
      uint16_t flowId = 0,
      uint16_t packets = 1,
      uint32_t measuredRttNs = 1000,
      uint16_t ce = 0) {
    fill(cc, flowId);
    cc.onPacketAck(
        flowId,
        {.numAckedPackets = packets,
         .numAckedPacketsWithCE = ce,
         .measuredRttNs = measuredRttNs,
         .smoothedQpRttNs = 2000});
  }

  static void open(
      CongestionController& cc,
      const std::vector<uint16_t>& windows) {
    for (uint16_t id = 0; id < windows.size(); ++id) {
      cc.onFlowAdded(id);
    }
    for (uint16_t id = 0; id < windows.size(); ++id) {
      for (uint16_t sent = 0; sent < windows[id]; ++sent) {
        cc.onPacketSent(id);
      }
    }
    cc.onPacketAck(0, {.sendPending = true});
  }

  static void grow(CongestionController& cc, uint16_t flowId) {
    const auto before = cc.getFwnd(flowId);
    for (int event = 0; cc.getFwnd(flowId) == before && event < 8; ++event) {
      ack(cc, 0, 1, 5000);
    }
  }

  static void expectWindowSum(const CongestionController& cc, uint16_t flows) {
    uint32_t sum = 0;
    for (uint16_t id = 0; id < flows; ++id) {
      sum += cc.getFwnd(id);
    }
    EXPECT_EQ(cc.getQwnd(), sum)
        << "every whole QWND change must land on a flow in the same operation";
  }

  static constexpr uint16_t kBurst = 4;
  gflags::FlagSaver flags_;
};

TEST_F(FlowActivationTest, ZeroWindowFlowsBootstrapOneAtATimeLowestFirst) {
  Baseline cc(128, 0);
  open(cc, {9, 0, 0, 0});
  for (uint16_t chosen = 1; chosen < 4; ++chosen) {
    for (int event = 0; cc.getFwnd(chosen) < kBurst && event < 20; ++event) {
      ack(cc);
      for (uint16_t later = chosen + 1; later < 4; ++later) {
        EXPECT_EQ(cc.getFwnd(later), 0)
            << "a later flow must wait until the chosen flow holds its burst";
      }
      expectWindowSum(cc, 4);
    }
    ASSERT_EQ(cc.getFwnd(chosen), kBurst)
        << "the lowest eligible flow must complete bootstrap before the next starts";
  }
}

TEST_F(
    FlowActivationTest,
    CongestionExitFromFastStartActivatesAZeroWindowFlow) {
  Baseline cc(128, 0);
  cc.onFlowAdded(0);
  cc.onFlowAdded(1);
  for (int sent = 0; sent < 13; ++sent) {
    cc.onPacketSent(0);
  }
  cc.onPacketAck(0, {.numAckedPackets = 1, .numAckedPacketsWithCE = 1});
  EXPECT_EQ(cc.getFwnd(1), 0) << "a CE ack must not claim a window increase";
  grow(cc, 1);
  EXPECT_GT(cc.getFwnd(1), 0)
      << "a congestion ack ending fast start must permit activation after the reduced window recovers";
  expectWindowSum(cc, 2);
}

TEST_F(
    FlowActivationTest,
    ExponentialThresholdHoldsAtEachThresholdAndStartsAboveIt) {
  FLAGS_libsoftmetaroce_exponential_flow_activation_threshold = true;
  constexpr std::array<uint16_t, 5> kThresholds{3, 6, 12, 24, 48};
  for (uint16_t active = 1; active <= kThresholds.size(); ++active) {
    for (uint16_t extra : {0, 1}) {
      Baseline cc(128, 0);
      std::vector<uint16_t> windows(active + 1, 1);
      windows[0] = kThresholds[active - 1] + extra - (active - 1);
      windows[active] = 0;
      open(cc, windows);
      ack(cc, 0, 8, 5000);
      if (extra == 0) {
        EXPECT_EQ(cc.getFwnd(active), 0)
            << "the log rule must hold activation at its strict threshold; active flows "
            << active;
        EXPECT_EQ(cc.getQwnd(), kThresholds[active - 1])
            << "banked growth must not bypass the activation threshold";
      } else {
        EXPECT_GT(cc.getFwnd(active), 0)
            << "the log rule must start the next flow immediately above its threshold; active flows "
            << active;
      }
      expectWindowSum(cc, active + 1);
    }
  }
}

TEST_F(
    FlowActivationTest,
    ExponentialThresholdAdmitsAFlowWhenWindowCrossesItsThreshold) {
  FLAGS_libsoftmetaroce_exponential_flow_activation_threshold = true;
  Baseline cc(128, 0);
  open(cc, {3, 0});
  ack(cc);
  ack(cc);
  EXPECT_EQ(cc.getQwnd(), 4)
      << "low-delay acks must grow QWND past the second-flow threshold";
  grow(cc, 1);
  EXPECT_GT(cc.getFwnd(1), 0)
      << "the ACK after crossing the exponential threshold must allow the zero-window flow to claim";
}

TEST_F(FlowActivationTest, ExponentialThresholdDoesNotWrapWithManyActiveFlows) {
  FLAGS_libsoftmetaroce_exponential_flow_activation_threshold = true;
  Baseline cc(128, 0);
  std::vector<uint16_t> windows(66, 1);
  windows.back() = 0;
  open(cc, windows);
  ack(cc, 0, 8, 5000);
  EXPECT_EQ(cc.getFwnd(65), 0)
      << "a large flow count must not wrap the exponential threshold and activate another flow";
  EXPECT_EQ(cc.getQwnd(), 65)
      << "high-delay growth must remain banked when no chosen flow can claim";
}

TEST_F(FlowActivationTest, AverageRuleHoldsAtEqualityAndStartsAboveIt) {
  FLAGS_libsoftmetaroce_average_window_log2 = 2;
  for (uint16_t qwnd : {8, 9}) {
    Baseline cc(128, 0);
    open(cc, {qwnd, 0});
    ack(cc, 0, 4, 5000);
    if (qwnd == 8) {
      EXPECT_EQ(cc.getFwnd(1), 0)
          << "the alternative average-window rule must hold activation at equality";
      EXPECT_EQ(cc.getQwnd(), qwnd)
          << "the average-window threshold must bank high-delay increases";
    } else {
      EXPECT_GT(cc.getFwnd(1), 0)
          << "the alternative average-window rule must activate above average times the new flow count";
    }
  }
}

TEST_F(FlowActivationTest, AverageRuleReconsidersAfterWindowGrowth) {
  FLAGS_libsoftmetaroce_average_window_log2 = 2;
  Baseline cc(128, 0);
  open(cc, {8, 0});
  ack(cc);
  ack(cc);
  EXPECT_EQ(cc.getQwnd(), 9)
      << "window-limited acks must grow QWND past the average-window threshold";
  grow(cc, 1);
  EXPECT_GT(cc.getFwnd(1), 0)
      << "the ACK after crossing the alternative threshold must let a zero-window flow claim";
}

TEST_F(FlowActivationTest, ThresholdCrossingDefersChoiceUntilTheNextAck) {
  Baseline cc(128, 0);
  open(cc, {4, 0, 0});
  ack(cc, 0, 3);
  ASSERT_GT(cc.getQwnd(), 4)
      << "precondition: batched additive growth must cross the threshold";
  EXPECT_EQ(cc.getFwnd(1) + cc.getFwnd(2), 0)
      << "crossing the threshold must not select a bootstrap within the growth loop";
  grow(cc, 1);
  EXPECT_GT(cc.getFwnd(1), 0)
      << "the next ACK must select the lowest eligible flow";
  EXPECT_EQ(cc.getFwnd(2), 0)
      << "a zero-window flow must wait for the lower-numbered bootstrap to complete";
  expectWindowSum(cc, 3);
}

TEST_F(FlowActivationTest, ChoosingARecipientDoesNotCompleteBootstrap) {
  Baseline cc(128, 0);
  open(cc, {9, 0});
  BootstrappingFlow bootstrap;
  bootstrap.onAck(cc, 0, {});
  for (int growth = 0; growth < 3; ++growth) {
    grow(cc, 1);
  }
  ASSERT_EQ(cc.getFwnd(1), kBurst - 1)
      << "precondition: the next credited packet would complete bootstrap";
  EXPECT_EQ(bootstrap.flowForWindowIncrease(cc, 0, false), 1)
      << "recipient selection must identify the flow that needs its final bootstrap credit";
  EXPECT_EQ(bootstrap.flowForWindowIncrease(cc, 0, false), 1)
      << "selecting a recipient must leave bootstrap state intact until the window increases";
  EXPECT_EQ(cc.getFwnd(1), kBurst - 1)
      << "recipient selection must not consume or apply window credit";
}

TEST_F(
    FlowActivationTest,
    SmallerEligibleAckedFlowClaimsBeforeBootstrappingFlow) {
  Baseline cc(128, 0);
  open(cc, {9, 1, 0});
  for (int event = 0; cc.getFwnd(1) == 1 && event < 4; ++event) {
    ack(cc, 1);
  }
  EXPECT_GT(cc.getFwnd(1), 1)
      << "an eligible acked flow below the burst may claim ahead of the chosen flow";
  EXPECT_EQ(cc.getFwnd(2), 0)
      << "the chosen flow must yield this increase to the smaller eligible flow";
}

TEST_F(FlowActivationTest, AckedFlowAtTheBurstYieldsToBootstrappingFlow) {
  Baseline cc(128, 0);
  open(cc, {4, 5, 0});
  for (int event = 0; cc.getFwnd(2) == 0 && event < 4; ++event) {
    ack(cc);
  }
  EXPECT_EQ(cc.getFwnd(0), kBurst)
      << "an acked flow at the burst must yield to the chosen flow";
  EXPECT_GT(cc.getFwnd(2), 0)
      << "the chosen flow must claim growth to start sending";
}

TEST_F(FlowActivationTest, HighDelayAckFeedsBootstrap) {
  Baseline cc(128, 0);
  open(cc, {9, 0});
  grow(cc, 1);
  EXPECT_GT(cc.getFwnd(1), 0)
      << "delay in the acked flow must not prevent growth on the chosen flow";
  EXPECT_EQ(cc.getFwnd(0), 9)
      << "a high-delay ack must leave the acked flow's growth for another flow";
  expectWindowSum(cc, 2);
}

TEST_F(FlowActivationTest, HighDelaySmallAckedFlowYieldsToBootstrappingFlow) {
  Baseline cc(128, 0);
  open(cc, {9, 1, 0});
  for (int event = 0; cc.getFwnd(2) == 0 && event < 4; ++event) {
    ack(cc, 1, 1, 5000);
  }
  EXPECT_GT(cc.getFwnd(2), 0)
      << "the chosen flow must claim when a small acked flow has high delay";
  EXPECT_EQ(cc.getFwnd(1), 1)
      << "the smaller-flow exception must still require low delay";
}

TEST_F(FlowActivationTest, BootstrapOwnHighDelayAckCannotGrowIt) {
  Baseline cc(128, 0);
  open(cc, {13, 0});
  grow(cc, 1);
  ASSERT_GT(cc.getFwnd(1), 0)
      << "precondition: the bootstrap must have window to send an ACK";
  const auto before = cc.getQwnd();
  const auto flowBefore = cc.getFwnd(1);
  for (int event = 0; event < 4; ++event) {
    ack(cc, 1, 1, 5000);
  }
  EXPECT_EQ(cc.getFwnd(1), flowBefore)
      << "the bootstrap's own high delay must keep its growth banked";
  EXPECT_EQ(cc.getQwnd(), before)
      << "an ineligible recipient must not consume whole QWND credit";
  ack(cc, 0, 1, 5000);
  EXPECT_GT(cc.getFwnd(1), flowBefore)
      << "another flow's high-delay ACK may feed the waiting bootstrap";
  expectWindowSum(cc, 2);
}

TEST_F(FlowActivationTest, LossToZeroRestartsTheLowestEligibleFlow) {
  for (auto type :
       {CongestionController::LossEvent::LossType::OutOfOrder,
        CongestionController::LossEvent::LossType::RTO}) {
    Baseline cc(128, 0, 0);
    open(cc, {13, 0, 0});
    grow(cc, 1);
    cc.onPacketLoss(1, {.numLostPackets = 1, .lossType = type});
    ASSERT_EQ(cc.getFwnd(1), 0)
        << "precondition: loss must empty the matching bootstrap";
    grow(cc, 1);
    EXPECT_GT(cc.getFwnd(1), 0)
        << "selection after loss must reuse the lowest eligible flow";
    EXPECT_EQ(cc.getFwnd(2), 0)
        << "loss must not exclude the lowest flow from the next selection";
    expectWindowSum(cc, 3);
  }
}

TEST_F(FlowActivationTest, ZeroWindowWaitsForFinalOutstandingFrame) {
  for (bool activation : {false, true}) {
    FLAGS_libsoftmetaroce_activate_tx_flows = activation;
    Baseline cc(128, 0);
    open(cc, {13, 3, 0, 0});
    cc.onPacketAck(1, {.numAckedPackets = 1, .numAckedPacketsWithCE = 3});
    ASSERT_EQ(cc.getFwnd(1), 0)
        << "precondition: ECN must empty the window with two frames outstanding";
    for (int event = 0; event < 8; ++event) {
      ack(cc, 0, 1, 5000);
      expectWindowSum(cc, 4);
    }
    EXPECT_EQ(cc.getFwnd(1), 0)
        << "bootstrap must skip a zero-window flow while old frames remain";
    if (activation) {
      EXPECT_GT(cc.getFwnd(2), 0)
          << "selection must skip lower zero-window flows with outstanding frames";
      EXPECT_GT(cc.getFwnd(3), 0)
          << "later selection must keep skipping the paused lower-numbered flow";
    }
    cc.onPacketAck(
        1,
        {.numAckedPackets = 1, .measuredRttNs = 1000, .smoothedQpRttNs = 2000});
    EXPECT_EQ(cc.getFwnd(1), 0)
        << "the ACKed-flow fallback must not reopen a flow with one frame outstanding";
    cc.onPacketAck(
        1,
        {.numAckedPackets = 1, .measuredRttNs = 1000, .smoothedQpRttNs = 2000});
    EXPECT_GT(cc.getFwnd(1), 0)
        << "resolving the final old frame must make banked growth available again";
    expectWindowSum(cc, 4);
  }
}

TEST_F(FlowActivationTest, CongestionRestartsTheLowestEligibleFlow) {
  Baseline cc(128, 0);
  open(cc, {13, 0, 0, 0});
  for (int event = 0; event < 3; ++event) {
    grow(cc, 1);
    ASSERT_EQ(cc.getFwnd(1), 1)
        << "precondition: CE must empty the flow being bootstrapped";
    ack(cc, 1, 1, 1000, 1);
    grow(cc, 1);
    EXPECT_GT(cc.getFwnd(1), 0)
        << "CE must allow the lowest eligible flow to bootstrap again";
    EXPECT_EQ(cc.getFwnd(2) + cc.getFwnd(3), 0)
        << "selection after congestion must not skip the lowest eligible flow";
    ack(cc, 1, 1, 1000, 1);
    expectWindowSum(cc, 4);
  }
}

TEST_F(FlowActivationTest, LossEndsBootstrap) {
  for (auto type :
       {CongestionController::LossEvent::LossType::OutOfOrder,
        CongestionController::LossEvent::LossType::RTO}) {
    Baseline cc(128, 0, 0);
    open(cc, {13, 0, 0});
    grow(cc, 1);
    grow(cc, 1);
    const auto before = cc.getFwnd(1);
    cc.onPacketLoss(1, {.numLostPackets = 1, .lossType = type});
    EXPECT_LT(cc.getFwnd(1), before)
        << "abandoning the choice must retain the normal loss reduction";
    grow(cc, 2);
    EXPECT_GT(cc.getFwnd(2), 0)
        << "either kind of detected loss must move bootstrapping off that flow";
    EXPECT_LT(cc.getFwnd(1), before)
        << "the flow whose bootstrap ended must not consume its replacement's growth";
    expectWindowSum(cc, 3);
  }
}

TEST_F(FlowActivationTest, CongestionEndsBootstrapWithoutAnotherEligibleFlow) {
  Baseline cc(128, 0);
  open(cc, {13, 0});
  grow(cc, 1);
  grow(cc, 1);
  ack(cc, 1, 1, 1000, 1);
  ASSERT_EQ(cc.getFwnd(1), 1)
      << "precondition: CE must leave window on the flow whose bootstrap ended";
  for (int event = 0; cc.getFwnd(0) == 13 && event < 4; ++event) {
    ack(cc);
  }
  EXPECT_GT(cc.getFwnd(0), 13)
      << "clearing the choice must allow the acked flow to claim again";
  EXPECT_EQ(cc.getFwnd(1), 1)
      << "a flow whose bootstrap ended must not keep bootstrapping without another eligible flow";
}

TEST_F(FlowActivationTest, DisabledFlowsAreNeverChosen) {
  Baseline cc(128, 0);
  for (uint16_t id = 0; id < 4; ++id) {
    cc.onFlowAdded(id);
  }
  cc.onFlowDisabled(1);
  for (int sent = 0; sent < 13; ++sent) {
    cc.onPacketSent(0);
  }
  cc.onPacketAck(0, {.sendPending = true});
  grow(cc, 2);
  EXPECT_GT(cc.getFwnd(2), 0)
      << "permanently disabled flows must be skipped when selecting a zero-window flow";
  cc.onFlowDisabled(2);
  grow(cc, 3);
  EXPECT_GT(cc.getFwnd(3), 0)
      << "disabling the chosen flow must move the choice to a usable flow";
  EXPECT_EQ(cc.getFwnd(1) + cc.getFwnd(2), 0)
      << "activation must never restore a disabled flow's window";
}

TEST_F(FlowActivationTest, DisabledFlowZeroIsNotChosen) {
  Baseline cc(128, 0);
  for (uint16_t id = 0; id < 3; ++id) {
    cc.onFlowAdded(id);
  }
  for (int sent = 0; sent < 13; ++sent) {
    cc.onPacketSent(2);
  }
  cc.onFlowDisabled(0);
  cc.onPacketAck(2, {.sendPending = true});
  for (int event = 0; cc.getFwnd(1) == 0 && event < 4; ++event) {
    ack(cc, 2, 1, 5000);
  }
  EXPECT_GT(cc.getFwnd(1), 0)
      << "permanent disablement must exclude flow 0 from bootstrap selection too";
  EXPECT_EQ(cc.getFwnd(0), 0)
      << "a disabled flow must not receive window through activation";
}

TEST_F(FlowActivationTest, BatchedAckDefersTheNextChoiceAfterPromotion) {
  Baseline cc(128, 0);
  open(cc, {13, 0, 0});
  ack(cc);
  ack(cc);
  ASSERT_GT(cc.getFwnd(1), 0)
      << "precondition: the chosen flow must have started before the batch";
  const auto beforeBatch = cc.getQwnd();
  ack(cc, 0, 10);
  EXPECT_GT(cc.getQwnd(), beforeBatch + 1)
      << "a batched ACK must consume multiple whole increments of accumulated credit";
  EXPECT_EQ(cc.getFwnd(1), kBurst)
      << "the batch must finish bootstrapping the current flow";
  EXPECT_EQ(cc.getFwnd(2), 0)
      << "remaining increases in a promotion ack must not start another eligible flow";
  ack(cc, 0, 1, 5000);
  EXPECT_GT(cc.getFwnd(2), 0)
      << "the next ack must reconsider activation after the promotion";
}

TEST_F(FlowActivationTest, PromotionDefersChoiceUntilTheNextCongestionAck) {
  Baseline cc(128, 0);
  open(cc, {13, 0, 2, 0});
  ack(cc, 2, 1, 1000, 1);
  ASSERT_EQ(cc.getFwnd(2), 0)
      << "precondition: the first CE ack must leave a zero-window flow with another packet outstanding";
  ack(cc);
  ack(cc);
  ack(cc, 0, 10);
  ASSERT_EQ(cc.getFwnd(1), kBurst)
      << "precondition: the batch must promote the chosen flow";
  // A flow can still report CE after its window becomes zero.
  cc.onPacketAck(2, {.numAckedPackets = 1, .numAckedPacketsWithCE = 1});
  grow(cc, 2);
  EXPECT_GT(cc.getFwnd(2), 0)
      << "the next ack must choose the lowest eligible flow instead of abandoning a premature choice";
  EXPECT_EQ(cc.getFwnd(3), 0)
      << "promotion must defer choosing until the next ack observes its congestion signal";
}

TEST_F(FlowActivationTest, RcnEmptiesTheChoiceAndHoldsActivationAtItsMax) {
  BaselineWithRCN cc(128, 0);
  open(cc, {40, 1, 1, 1, 1, 1, 0});
  grow(cc, 6);
  ASSERT_GT(cc.getFwnd(6), 0)
      << "precondition: the unprotected chosen flow must hold window RCN can remove";
  cc.onPacketAck(
      6,
      {.numAckedPackets = 4,
       .measuredRttNs = 1000,
       .smoothedQpRttNs = 1000,
       .rcnDesiredRateGbps = 1});
  EXPECT_EQ(cc.getFwnd(6), 0)
      << "RCN must empty an unprotected chosen flow above its target";
  cc.onPacketAck(5, {.numAckedPackets = 4});
  EXPECT_EQ(cc.getFwnd(5), 0)
      << "precondition: another lower eligible flow must distinguish a cleared choice";
  for (int event = 0; cc.getQwnd() > 10 && event < 100; ++event) {
    ack(cc, 0, 1, 1000);
  }
  EXPECT_EQ(cc.getQwnd(), 10)
      << "RCN reductions must settle at the rate-derived window maximum";
  ack(cc, 0, 4, 5000);
  EXPECT_EQ(cc.getFwnd(5) + cc.getFwnd(6), 0)
      << "a QP held at its RCN maximum must not start a zero-window flow";
  cc.onPacketAck(0, {.smoothedQpRttNs = 1000, .rcnDesiredRateGbps = 1000});
  for (int event = 0; cc.getQwnd() <= 12 && event < 8; ++event) {
    ack(cc);
  }
  grow(cc, 5);
  EXPECT_GT(cc.getFwnd(5), 0)
      << "raising the RCN limit must choose the lowest eligible flow after clearing the emptied choice";
  EXPECT_EQ(cc.getFwnd(6), 0)
      << "the emptied choice must yield to the newly chosen lower flow";
}

TEST_F(FlowActivationTest, RcnKeepsTheUnstartedChoiceBelowItsMax) {
  BaselineWithRCN cc(128, 0);
  open(cc, {9, 0, 0});
  ack(cc, 0, 0);
  grow(cc, 1);
  EXPECT_GT(cc.getFwnd(1), 0)
      << "RCN must preserve an unstarted choice when the rate permits window growth";
  EXPECT_EQ(cc.getFwnd(2), 0)
      << "an unstarted choice must finish bootstrapping before another eligible flow";
}

TEST_F(FlowActivationTest, RcnProtectedCongestedFlowAbandonsTheChoice) {
  BaselineWithRCN cc(128, 0);
  open(cc, {40, 0, 0});
  grow(cc, 1);
  cc.onPacketAck(
      1,
      {.numAckedPackets = 1,
       .numAckedPacketsWithCE = 1,
       .measuredRttNs = 1000,
       .smoothedQpRttNs = 1000,
       .rcnDesiredRateGbps = 1});
  EXPECT_EQ(cc.getFwnd(1), 1)
      << "RCN's protected flow floor must keep this flow's window";
  cc.onPacketAck(0, {.smoothedQpRttNs = 1000, .rcnDesiredRateGbps = 1000});
  grow(cc, 2);
  EXPECT_GT(cc.getFwnd(2), 0)
      << "after the rate limit rises the congested flow must have stopped bootstrapping";
  EXPECT_EQ(cc.getFwnd(1), 1)
      << "RCN's floor must not preserve a congestion-signaled choice";
}

TEST_F(FlowActivationTest, BootstrapCompletesAtTheMaximumAttainableWindow) {
  gflags::SetCommandLineOption("libsoftmetaroce_max_fwnd", "3");
  Baseline cc(128, 0);
  open(cc, {3, 3, 3, 0, 0});
  for (uint16_t flowId : {3, 4}) {
    for (int event = 0; cc.getFwnd(flowId) < 3 && event < 20; ++event) {
      ack(cc, 0, 1, 5000);
      expectWindowSum(cc, 5);
    }
    EXPECT_EQ(cc.getFwnd(flowId), 3)
        << "bootstrap must complete at the attainable maximum so the next eligible flow can start; flow "
        << flowId;
  }
}

TEST_F(FlowActivationTest, WholeIncreasesWaitWhenEveryClaimantIsAtItsMaximum) {
  gflags::SetCommandLineOption("libsoftmetaroce_max_fwnd", "5");
  Baseline cc(128, 0);
  open(cc, {5, 0});
  for (int event = 0; cc.getFwnd(1) < kBurst && event < 10; ++event) {
    ack(cc);
  }
  ASSERT_EQ(cc.getFwnd(1), kBurst)
      << "precondition: the chosen flow must have finished bootstrapping";
  const auto before = cc.getQwnd();
  for (int event = 0; event < 4; ++event) {
    ack(cc);
  }
  EXPECT_EQ(cc.getQwnd(), before)
      << "a whole increase must stay banked when the acked flow cannot claim";
  ack(cc, 1);
  EXPECT_EQ(cc.getFwnd(1), 5)
      << "the eligible flow below its maximum must claim the banked increase";
  expectWindowSum(cc, 2);
}

TEST_F(FlowActivationTest, FlagOffPreservesAckedFlowAllocation) {
  FLAGS_libsoftmetaroce_activate_tx_flows = false;
  Baseline cc(128, 0);
  open(cc, {9, 0, 0});
  for (int event = 0; event < 20; ++event) {
    ack(cc);
  }
  EXPECT_GT(cc.getFwnd(0), 9)
      << "without activation the eligible acked flow must claim window growth";
  EXPECT_EQ(cc.getFwnd(1) + cc.getFwnd(2), 0)
      << "without activation flows left at zero must retain their zero windows";
  const auto before = cc.getQwnd();
  ack(cc, 0, 1, 5000);
  EXPECT_EQ(cc.getQwnd(), before)
      << "without a chosen flow a high-delay ack must bank its increase";
}

class QpFlowActivationTest : public QpTestBase {
 protected:
  void SetUp() override {
    FLAGS_libsoftmetaroce_activate_tx_flows = true;
    FLAGS_libsoftmetaroce_exponential_flow_activation_threshold = true;
    FLAGS_libsoftmetaroce_fast_start_burst = 4;
    QpTestBase::SetUp();
  }

  gflags::FlagSaver flags_;
  ScopedSuppressedTxTimers timers_;
};

TEST_F(QpFlowActivationTest, GrowingWindowSendsAcrossEveryPortPair) {
  constexpr uint16_t kFlows = 16;
  constexpr uint16_t kPairs = 4;
  constexpr uint32_t kFrames = 1000;
  LimitedTestSender sender(arbiter, [](folly::EventBase* evb) {
    return std::make_pair(
        std::make_unique<testing::NiceMock<MockUDPSocket>>(evb),
        std::unique_ptr<Modifier>{});
  });
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  Qp target(30, retxTimer, arbiter, mrs, &cq, &otherIbvQpStorage);
  target.setMtu(mtu);
  target.setRemoteQpNum(0);
  target.setCongestionController(std::make_unique<Baseline>(128, 0));
  ibv_gid_entry entry{};
  entry.gid_type = kGidTypeMetaRoceUdp;
  std::set<folly::IPAddress> configuredRemotes;
  for (uint16_t pair = 0; pair < kPairs; ++pair) {
    const folly::IPAddress local("fc00::" + std::to_string(pair + 1));
    const folly::IPAddress remote("fc00::" + std::to_string(pair + 101));
    target.addPortPair(pair, local, entry, pair, remote);
    configuredRemotes.insert(remote);
  }
  target.setupFlows(kFlows, 0, &sender);
  std::vector<uint16_t> initialSockets;
  initialSockets.reserve(kFlows);
  for (uint16_t id = 0; id < kFlows; ++id) {
    initialSockets.push_back(target.getTxFlowById(id)->getSocketId());
  }
  std::vector<char> payload(kFrames * mtu);
  auto* wqe = postWqeHelper(
      &target,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(payload.size()),
       .address = payload.data()});
  ASSERT_NE(wqe, nullptr);
  std::set<folly::IPAddress> usedRemotes;
  std::set<uint16_t> usedFlows;
  std::vector<std::unique_ptr<EgressPacket>> pending;
  for (int visit = 0; usedRemotes.size() < kPairs && visit < 2000; ++visit) {
    // Only two packets leave before the first ack, leaving port pairs unused.
    const int burst = visit == 0 ? 2 : 100;
    for (int sent = 0; sent < burst; ++sent) {
      auto packet = target.getNextPacketToSend();
      if (!packet) {
        break;
      }
      pending.push_back(std::move(packet));
    }
    ASSERT_FALSE(pending.empty())
        << "the growing QP window must keep the transfer making progress; visit "
        << visit << ", flows used " << usedFlows.size();
    for (const auto& packet : pending) {
      const auto* data = dynamic_cast<DataPacket*>(packet->packet.get());
      ASSERT_NE(data, nullptr);
      const auto& header = data->getDataHeader();
      const uint16_t id = data->getBaseHeader().getFlowId();
      usedFlows.insert(id);
      usedRemotes.insert(packet->remoteAddr);
      target.getTxFlowById(id)->processAckPacket(
          AckHeader::AckHeaderBuilder()
              .setCumulativeFsn(header.getFsn())
              .setMaxRcvdFsn(header.getFsn())
              .setAckStatus(AckStatus::ACK)
              .setRateHints(0)
              .setTimestamp(header.getTimestamp())
              .build(),
          header.getTimestamp() + 1000);
    }
    pending.clear();
    if (visit == 0) {
      ASSERT_EQ(usedRemotes.size(), 2)
          << "precondition: fast start must leave two port pairs unused";
    }
  }
  EXPECT_EQ(usedRemotes, configuredRemotes)
      << "the QP must spread the transfer across every configured port pair";
  std::vector<uint16_t> finalSockets;
  finalSockets.reserve(kFlows);
  for (uint16_t id = 0; id < kFlows; ++id) {
    finalSockets.push_back(target.getTxFlowById(id)->getSocketId());
  }
  EXPECT_EQ(finalSockets, initialSockets)
      << "bootstrap selection and growth must preserve each flow's entropy";
}

} // namespace libsoftmetaroce
