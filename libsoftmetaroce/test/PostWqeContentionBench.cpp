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

// Microbenchmark for what a client's post costs the sender.
//
// TagSet::postWqe() and the sender's findNextWqe() meet on TagSet::mutex_. A
// post that holds that lock across the queue's own enqueue makes every reader
// wait behind it, the sender's path among them. One group of cases posts with a
// sender running alongside and one with none, which separates what the posts
// cost each other from what the sender costs them.
//
// The absolute times mean little on their own; what the benchmark shows is how
// they move as posting threads are added.

#include <folly/Benchmark.h>
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

#include <atomic>
#include <thread>
#include <vector>

using namespace libsoftmetaroce;
using ::testing::NiceMock;

namespace {

constexpr uint16_t kMtu = 1000;
constexpr uint16_t kSendToPort = 0xace;
// Held constant across cases so the times compare: the same total work is split
// over more threads. Well under kMaxQpWorkRequests, so no post is refused for
// depth -- a refused post returns early and would time the wrong thing.
constexpr int kTotalPosts = 8000;

// A concrete always-sending controller: a gmock NiceMock adds per-call cost
// that would sit inside the timed region.
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

// A QP with one tag and one path, and no send loop running: the benchmark
// drives the sender's side itself so the reader's rate is measured, not the
// arbiter's scheduling.
struct PostBenchFixture {
  HighResTimer retxTimer;
  QpArbiter arbiter;
  MemoryRegions mrs;
  CompletionQueue cq;
  struct ibv_qp ibvQp{};
  std::vector<char> payload;
  std::unique_ptr<LimitedTestSender> sender;
  std::unique_ptr<Qp> qp;
  TxFlow* flow{nullptr};

  PostBenchFixture() : payload(kMtu, 'x') {
    qp = std::make_unique<Qp>(10, retxTimer, arbiter, mrs, &cq, &ibvQp);
    qp->addPortPair(folly::IPAddress("fc00::1"), folly::IPAddress("fc00::101"));
    qp->setRemoteQpNum(100);
    qp->setMtu(kMtu);
    qp->setCongestionController(std::make_unique<BenchCongestionController>());
    sender = std::make_unique<LimitedTestSender>(
        arbiter,
        mockUDPSocketMaker,
        kSendToPort,
        LimitedTestSender::kDisableSendLoop);
    qp->setupFlows(1, 1, sender.get());
    const bool configured =
        qp->getTag(kMetaRoceInitialTag)
            ->setReceiveSequenceNumbers({.nextMsn = 1, .nextCsn = 0});
    CHECK(configured) << "an unconfigured tag warns on every post, which would "
                         "dominate what this measures";
    flow = qp->getTxFlowById(0);
  }

  std::unique_ptr<Wqe> makeSend(uint64_t id) {
    return std::make_unique<SendWqe>(id, oneSge(payload), kMtu);
  }
};

// posterThreads clients post concurrently; one thread stands in for the sender,
// calling getNextPacketToSend() -- which reaches findNextWqe() and so contends
// for mutex_ with every post. Returns how many times the sender got through.
uint64_t runRound(int posterThreads, bool withSender) {
  PostBenchFixture f;
  std::atomic<bool> posting{true};
  std::atomic<uint64_t> senderPasses{0};

  std::thread senderThread;
  if (withSender) {
    senderThread = std::thread([&] {
      while (posting.load(std::memory_order_relaxed)) {
        std::ignore = f.flow->getNextPacketToSend();
        senderPasses.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  // The first threads take the odd post when the width does not divide the
  // total, so every case posts kTotalPosts whatever its thread count.
  const int postsPerThread = kTotalPosts / posterThreads;
  const int extraPosts = kTotalPosts % posterThreads;
  std::vector<std::thread> posters;
  posters.reserve(posterThreads);
  uint64_t nextPostId = 0;
  for (int t = 0; t < posterThreads; ++t) {
    const int postsForThread = postsPerThread + (t < extraPosts ? 1 : 0);
    const uint64_t firstPostId = nextPostId;
    nextPostId += postsForThread;
    posters.emplace_back([&f, postsForThread, firstPostId] {
      for (int i = 0; i < postsForThread; ++i) {
        std::ignore =
            f.qp->postWqe(kMetaRoceInitialTag, f.makeSend(firstPostId + i));
      }
    });
  }
  for (auto& p : posters) {
    p.join();
  }
  posting.store(false, std::memory_order_relaxed);
  if (withSender) {
    senderThread.join();
  }
  return senderPasses.load(std::memory_order_relaxed);
}

void postUnderSenderContention(unsigned iters, int posterThreads) {
  uint64_t passes = 0;
  for (unsigned i = 0; i < iters; ++i) {
    passes += runRound(posterThreads, /*withSender=*/true);
  }
  // Kept only so the sender loop is not optimized away: the total is a
  // function of how long the round ran, so it compares nothing.
  folly::doNotOptimizeAway(passes);
}

// The same posts with no sender running, which separates what the posts cost
// each other from what the sender costs them.
void postAlone(unsigned iters, int posterThreads) {
  for (unsigned i = 0; i < iters; ++i) {
    runRound(posterThreads, /*withSender=*/false);
  }
}

} // namespace

BENCHMARK_NAMED_PARAM(postUnderSenderContention, 1_poster, 1)
BENCHMARK_RELATIVE_NAMED_PARAM(postUnderSenderContention, 2_posters, 2)
BENCHMARK_RELATIVE_NAMED_PARAM(postUnderSenderContention, 4_posters, 4)
BENCHMARK_RELATIVE_NAMED_PARAM(postUnderSenderContention, 8_posters, 8)

BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(postAlone, 1_poster, 1)
BENCHMARK_RELATIVE_NAMED_PARAM(postAlone, 2_posters, 2)
BENCHMARK_RELATIVE_NAMED_PARAM(postAlone, 4_posters, 4)
BENCHMARK_RELATIVE_NAMED_PARAM(postAlone, 8_posters, 8)

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
