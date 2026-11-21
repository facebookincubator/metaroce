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

// Microbenchmark for building packets on the send path, where the congestion
// controller is consulted for every packet.

#include <folly/Benchmark.h>
#include <folly/CancellationToken.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/init/Init.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/portability/GMock.h>

#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/HighResTimer.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/congestion_control/CongestionController.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>

#include <vector>

using namespace libsoftmetaroce;
using ::testing::NiceMock;

namespace {

constexpr uint16_t kMtu = 1000;
constexpr uint16_t kSendToPort = 0xace;

// Trivial bodies keep per-call overhead out of what the timed loop measures.
class BenchCongestionController : public CongestionController {
 public:
  void onPacketSent(uint16_t /*flowId*/) override {}
  AckResult onPacketAck(uint16_t /*flowId*/, const AckEvent& /*ack*/) override {
    return {};
  }
  void onPacketLoss(uint16_t /*flowId*/, const LossEvent& /*loss*/) override {}
  void onFlowAdded(uint16_t /*flowId*/) override {}
  void onFlowDisabled(uint16_t /*flowId*/) override {}
  uint16_t getAvailableWindowSize(uint16_t /*flowId*/) const override {
    return 65535;
  }
  uint16_t getFwnd(uint16_t /*flowId*/) const override {
    return 65535;
  }
  uint32_t getQwnd() const override {
    return 65535;
  }
};

std::pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

// Has the Tags adopt the posted WQEs. Qp::processWqes() is a forever loop, so
// run it under a cancellation token and cancel it once they are adopted.
void processPostedWqes(Qp* qp, int numWqes) {
  folly::CancellationSource cs;
  auto procTask = [qp, cs]() -> folly::coro::Task<bool> {
    try {
      co_await folly::coro::co_withCancellation(
          cs.getToken(), qp->processWqes());
    } catch (const folly::OperationCancelled&) {
    }
    co_return true;
  };
  auto waitTask = [qp, numWqes, cs]() -> folly::coro::Task<bool> {
    bool found = qp->waitForWqesToBePosted(numWqes, std::chrono::seconds(5));
    cs.requestCancellation();
    co_return found;
  };
  auto [_, posted] = folly::coro::blockingWait(
      folly::coro::collectAll(
          folly::coro::co_withExecutor(
              folly::getGlobalCPUExecutor(), procTask()),
          folly::coro::co_withExecutor(
              folly::getGlobalCPUExecutor(), waitTask())));
  CHECK(posted) << numWqes
                << " WQE(s) were not adopted within the setup timeout";
}

// One WQE with `packets` unsent posns; the timed loop builds their packets.
struct SendBenchFixture {
  HighResTimer retxTimer;
  QpArbiter arbiter;
  MemoryRegions mrs;
  CompletionQueue cq;
  struct ibv_qp ibvQp{};
  std::vector<char> payload;
  std::unique_ptr<LimitedTestSender> sender;
  std::unique_ptr<Qp> qp;
  TxFlow* flow{nullptr};

  explicit SendBenchFixture(uint16_t packets)
      : payload(static_cast<size_t>(packets) * kMtu, 'x') {
    qp = std::make_unique<Qp>(10, retxTimer, arbiter, mrs, &cq, &ibvQp);
    qp->addPortPair(folly::IPAddress("fc00::1"), folly::IPAddress("fc00::101"));
    qp->setRemoteQpNum(100);
    qp->setMtu(kMtu);
    qp->setCongestionController(std::make_unique<BenchCongestionController>());
    // kDisableSendLoop: a background sender would race the timed loop for the
    // work posted below, so what the loop measures would vary run to run.
    sender = std::make_unique<LimitedTestSender>(
        arbiter,
        mockUDPSocketMaker,
        kSendToPort,
        LimitedTestSender::kDisableSendLoop);
    qp->setupFlows(
        1,
        1,
        sender.get(),
        SackRetransmissionConfig{SackRetransmissionMode::NEVER});
    const bool configured =
        qp->getTag(kMetaRoceInitialTag)
            ->setReceiveSequenceNumbers({.nextMsn = 1, .nextCsn = 0});
    CHECK(configured) << "an unconfigured tag warns on every send, which would "
                         "dominate what this measures";
    const bool posted = qp->postWqe(
        kMetaRoceInitialTag,
        std::make_unique<SendWqe>(0, oneSge(payload), kMtu));
    CHECK(posted) << "the timed loop builds packets from this WQE's posns";
    processPostedWqes(qp.get(), 1);
    flow = qp->getTxFlowById(0);
  }
};

// The flow-level send path: the window check, then the controller's
// accounting for each packet built.
void flowBuildPackets(unsigned n, int packets) {
  for (unsigned i = 0; i < n; ++i) {
    folly::BenchmarkSuspender suspender;
    SendBenchFixture fx(static_cast<uint16_t>(packets));
    int built = 0;
    suspender.dismiss();
    for (int p = 0; p < packets; ++p) {
      auto pkt = fx.flow->getNextPacketToSend();
      built += pkt ? 1 : 0;
      folly::doNotOptimizeAway(pkt);
    }
    suspender.rehire();
    CHECK_EQ(built, packets)
        << "every posn must yield a built packet, or this benchmark is timing "
           "refusals rather than the send path";
  }
}

// The QP-level send path, which adds flowForNextSend and
// shouldAdvanceFlowAfterSend per packet, and the per-flow stall sweep once the
// work runs out.
void qpBuildPackets(unsigned n, int packets) {
  for (unsigned i = 0; i < n; ++i) {
    folly::BenchmarkSuspender suspender;
    SendBenchFixture fx(static_cast<uint16_t>(packets));
    int built = 0;
    suspender.dismiss();
    for (int p = 0; p < packets; ++p) {
      auto pkt = fx.qp->getNextPacketToSend();
      built += pkt ? 1 : 0;
      folly::doNotOptimizeAway(pkt);
    }
    suspender.rehire();
    CHECK_EQ(built, packets)
        << "every posn must yield a built packet, or this benchmark is timing "
           "refusals rather than the send path";
  }
}

} // namespace

BENCHMARK_PARAM(flowBuildPackets, 16)
BENCHMARK_PARAM(flowBuildPackets, 64)
BENCHMARK_PARAM(flowBuildPackets, 256)

BENCHMARK_DRAW_LINE();

BENCHMARK_PARAM(qpBuildPackets, 16)
BENCHMARK_PARAM(qpBuildPackets, 64)
BENCHMARK_PARAM(qpBuildPackets, 256)

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
