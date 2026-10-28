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

#include <gtest/gtest.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <limits>
#include <string>

// NOLINTNEXTLINE(facebook-hte-PreventUseOfDeclareGflag)
DECLARE_bool(libsoftmetaroce_slow_receiver);
// NOLINTNEXTLINE(facebook-hte-PreventUseOfDeclareGflag)
DECLARE_bool(libsoftmetaroce_rfc6298_first_rtt_sample);
// NOLINTNEXTLINE(facebook-hte-PreventUseOfDeclareGflag)
DECLARE_uint32(libsoftmetaroce_initial_est_rtt_us);
// NOLINTNEXTLINE(facebook-hte-PreventUseOfDeclareGflag)
DECLARE_uint32(libsoftmetaroce_initial_mdev_rtt_us);

using namespace testing;
namespace libsoftmetaroce {

namespace {

constexpr uint16_t kMetaRoceSendToPort = 0xace;

// A send timestamp must be nonzero: updateRtt treats zero as "not set".
constexpr uint32_t kSentAtNs = 1'000;

// 50ms. Far enough above the 200us assumed RTT that a filtered-in sample and a
// seeded one cannot be confused, and low enough that 3x it stays under the
// 200ms max RTO.
constexpr uint32_t kRttNs = 50'000'000;

std::pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

} // namespace

class RttEstimatorTest : public Test {
 protected:
  void SetUp() override {
    FLAGS_libsoftmetaroce_slow_receiver = false;
  }

  void TearDown() override {
    qp_ = nullptr;
    sender_ = nullptr;
  }

  // Both the QP and the flows read the initial-value flags when they are
  // constructed, so tests set those before building either.
  Qp& makeQp() {
    qp_ = std::make_unique<Qp>(10, retxTimer_, arbiter_, mrs_, &cq_, &ibvQp_);
    qp_->addPortPair(localAddr_, remoteAddr_);
    qp_->setRemoteQpNum(100);
    qp_->setMtu(1000);
    qp_->setCongestionController(
        std::make_unique<NiceMock<MockCongestionController>>());
    return *qp_;
  }

  TxFlow* makeFlow() {
    if (!qp_) {
      makeQp();
    }
    sender_ = std::make_unique<LimitedTestSender>(
        arbiter_,
        mockUDPSocketMaker,
        kMetaRoceSendToPort,
        /*disableSendLoop=*/true);
    qp_->setupFlows(1, 1, sender_.get());
    return qp_->getTxFlowById(0);
  }

  // Declared first so it outlives the QP and restores every flag a test set.
  gflags::FlagSaver flagSaver_;
  HighResTimer retxTimer_;
  QpArbiter arbiter_;
  MemoryRegions mrs_;
  folly::IPAddress localAddr_{"fc00::1"};
  folly::IPAddress remoteAddr_{"fc00::101"};
  CompletionQueue cq_;
  struct ibv_qp ibvQp_{};
  std::unique_ptr<Qp> qp_;
  std::unique_ptr<LimitedTestSender> sender_;
};

TEST_F(RttEstimatorTest, FirstSampleReplacesTheAssumedEstimate) {
  auto* flow = makeFlow();
  ASSERT_NE(flow, nullptr)
      << "the fixture must produce flow 0, or the rest of this test crashes "
         "instead of reporting a setup failure";

  flow->updateRtt(kSentAtNs, kSentAtNs + kRttNs);

  const auto& txState = flow->getTxState();
  EXPECT_EQ(txState.estRttNs, kRttNs)
      << "the values a flow starts with are an assumption, so the first "
         "measurement must replace srtt outright; filtering it in leaves the "
         "estimate chasing a flow this slow for ~16 acks";
  EXPECT_EQ(txState.mdevRttNs, kRttNs / 2)
      << "RFC 6298 seeds mdev at half the first sample, which is what places "
         "the cold-start rto at 3x the measured rtt";
}

TEST_F(RttEstimatorTest, FirstSamplePutsRtoAtThreeTimesTheMeasurement) {
  auto* flow = makeFlow();
  ASSERT_NE(flow, nullptr)
      << "the fixture must produce flow 0, or the rest of this test crashes "
         "instead of reporting a setup failure";

  flow->updateRtt(kSentAtNs, kSentAtNs + kRttNs);

  EXPECT_EQ(flow->computeRtoIntervalUs(), 3 * kRttNs / kNsPerUs)
      << "rto is srtt + 4*mdev, so seeding R and R/2 must land on exactly 3R; "
         "anything shorter retransmits into a receiver already measured to be "
         "this slow";
}

TEST_F(RttEstimatorTest, LaterSamplesAreFilteredRatherThanAdopted) {
  auto* flow = makeFlow();
  ASSERT_NE(flow, nullptr)
      << "the fixture must produce flow 0, or the rest of this test crashes "
         "instead of reporting a setup failure";
  flow->updateRtt(kSentAtNs, kSentAtNs + kRttNs);

  flow->updateRtt(kSentAtNs, kSentAtNs + 2 * kRttNs);

  const auto estRttNs = flow->getTxState().estRttNs;
  EXPECT_GT(estRttNs, kRttNs)
      << "a second, larger sample must still move the estimate upward";
  EXPECT_LT(estRttNs, 2 * kRttNs)
      << "only the first sample is adopted outright; letting every sample "
         "replace the estimate would hand the rto to a single outlier";
}

TEST_F(RttEstimatorTest, SeedingDisabledLeavesTheFirstSampleFiltered) {
  FLAGS_libsoftmetaroce_rfc6298_first_rtt_sample = false;
  auto* flow = makeFlow();
  ASSERT_NE(flow, nullptr)
      << "the fixture must produce flow 0, or the rest of this test crashes "
         "instead of reporting a setup failure";

  flow->updateRtt(kSentAtNs, kSentAtNs + kRttNs);

  EXPECT_LT(flow->getTxState().estRttNs, kRttNs)
      << "the flag has to restore the filtered behavior exactly, so the "
         "seeding change can be turned off in the field without a rollback";
}

TEST_F(RttEstimatorTest, InitialEstimateFlagsDecideTheColdStartRto) {
  constexpr uint32_t kInitialEstRttUs = 5'000;
  constexpr uint32_t kInitialMdevRttUs = 10'000;
  FLAGS_libsoftmetaroce_initial_est_rtt_us = kInitialEstRttUs;
  FLAGS_libsoftmetaroce_initial_mdev_rtt_us = kInitialMdevRttUs;

  auto* flow = makeFlow();
  ASSERT_NE(flow, nullptr)
      << "the fixture must produce flow 0, or the rest of this test crashes "
         "instead of reporting a setup failure";

  EXPECT_EQ(
      flow->computeRtoIntervalUs(), kInitialEstRttUs + 4 * kInitialMdevRttUs)
      << "before any sample the flags are the only thing deciding how long a "
         "flow waits to retransmit, so they must reach the rto unmodified";
}

TEST_F(RttEstimatorTest, SlowReceiverSuppliesTheMdevPriorWhenNoneWasSet) {
  FLAGS_libsoftmetaroce_slow_receiver = true;

  auto* flow = makeFlow();
  ASSERT_NE(flow, nullptr)
      << "the fixture must produce flow 0, or the rest of this test crashes "
         "instead of reporting a setup failure";

  EXPECT_EQ(
      flow->computeRtoIntervalUs(),
      FLAGS_libsoftmetaroce_initial_est_rtt_us +
          4 * kSlowReceiverMdevRttNs / kNsPerUs)
      << "this is the shipped configuration, so the ~1s cold-start rto it "
         "produces must stay pinned even as the initial-value flags move";
}

TEST_F(RttEstimatorTest, ExplicitMdevFlagBeatsTheSlowReceiverPrior) {
  FLAGS_libsoftmetaroce_slow_receiver = true;
  constexpr uint32_t kMdevUs = 10'000;
  // gflags only marks a flag non-default when it is set through the registry;
  // assigning FLAGS_... directly would leave is_default true and this test
  // would pass for the wrong reason.
  gflags::SetCommandLineOption(
      "libsoftmetaroce_initial_mdev_rtt_us", std::to_string(kMdevUs).c_str());

  auto* flow = makeFlow();
  ASSERT_NE(flow, nullptr)
      << "the fixture must produce flow 0, or the rest of this test crashes "
         "instead of reporting a setup failure";

  EXPECT_EQ(
      flow->computeRtoIntervalUs(),
      FLAGS_libsoftmetaroce_initial_est_rtt_us + 4 * kMdevUs)
      << "slow-receiver mode only supplies a prior for a deployment that set "
         "no mdev; silently replacing an operator's mdev with 250ms would make "
         "the flag inert in the one configuration that ships";
}

TEST_F(RttEstimatorTest, ExplicitMaxRtoFlagBeatsTheSlowReceiverCap) {
  FLAGS_libsoftmetaroce_slow_receiver = true;
  constexpr uint32_t kMaxRtoUs = 200'000;
  const auto flagSetResult = gflags::SetCommandLineOption(
      "libsoftmetaroce_max_rto_us", std::to_string(kMaxRtoUs).c_str());
  ASSERT_FALSE(flagSetResult.empty())
      << "precondition: gflags must accept the explicit cap so the test exercises "
         "operator configuration";
  ASSERT_FALSE(
      gflags::GetCommandLineFlagInfoOrDie("libsoftmetaroce_max_rto_us")
          .is_default)
      << "precondition: explicitly setting even the compiled default must "
         "override the slow-receiver cap";

  auto* flow = makeFlow();
  ASSERT_NE(flow, nullptr);
  const auto& txState = flow->getTxState();
  ASSERT_GT((txState.estRttNs + 4 * txState.mdevRttNs) / kNsPerUs, kMaxRtoUs)
      << "precondition: the initial estimate must exceed the explicit cap so "
         "the test exercises RTO limiting without backoff";

  EXPECT_EQ(flow->computeRtoIntervalUs(), kMaxRtoUs)
      << "an explicit maximum must bound retransmission delays even in "
         "slow-receiver mode; substituting the 2s cap makes operator "
         "configuration ineffective";
}

TEST_F(RttEstimatorTest, InitialRttFlagsRejectValuesThatOverflowNanoseconds) {
  constexpr uint32_t kMaxUs = std::numeric_limits<uint32_t>::max() / kNsPerUs;

  for (const auto* flag :
       {"libsoftmetaroce_initial_est_rtt_us",
        "libsoftmetaroce_initial_mdev_rtt_us"}) {
    EXPECT_EQ(
        gflags::SetCommandLineOption(flag, std::to_string(kMaxUs + 1).c_str()),
        "")
        << flag
        << " must reject a value whose nanosecond conversion wraps; accepting "
           "it would silently seed the estimator with a tiny rtt";
    EXPECT_NE(
        gflags::SetCommandLineOption(flag, std::to_string(kMaxUs).c_str()), "")
        << flag << " must still accept the largest value that converts cleanly";
  }
}

TEST_F(RttEstimatorTest, QpEstimatorTakesItsInitialValueFromTheFlag) {
  constexpr uint32_t kInitialEstRttUs = 7'000;
  FLAGS_libsoftmetaroce_initial_est_rtt_us = kInitialEstRttUs;

  auto& qp = makeQp();

  EXPECT_EQ(qp.getEstQpRttNs(), kInitialEstRttUs * kNsPerUs)
      << "the qp estimate drives low-delay flow balancing, so it has to start "
         "from the same assumption the flows do, not a separate constant";
}

TEST_F(RttEstimatorTest, QpEstimatorAdoptsItsFirstSample) {
  auto& qp = makeQp();

  qp.updateQpRtt(kRttNs);

  EXPECT_EQ(qp.getEstQpRttNs(), kRttNs)
      << "the qp estimate had the same cold-start defect as the flow estimate; "
         "filtering its first sample leaves flow balancing ranking real delays "
         "against an assumed one";
}

TEST_F(RttEstimatorTest, QpEstimatorFiltersLaterSamples) {
  auto& qp = makeQp();
  qp.updateQpRtt(kRttNs);

  qp.updateQpRtt(2 * kRttNs);

  EXPECT_GT(qp.getEstQpRttNs(), kRttNs)
      << "a second, larger sample must still move the qp estimate upward";
  EXPECT_LT(qp.getEstQpRttNs(), 2 * kRttNs)
      << "only the first qp sample is adopted outright; letting every sample "
         "replace the estimate would make flow balancing track the last ack";
}

} // namespace libsoftmetaroce
