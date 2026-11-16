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

#include <folly/coro/GmockHelpers.h>
#include <folly/coro/GtestHelpers.h>
#include <folly/coro/Task.h>
#include <gtest/gtest.h>

#include <libsoftmetaroce/ConcurrentQpArbiter.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/EgressPacket.h>

using namespace testing;
namespace libsoftmetaroce {

static std::unique_ptr<EgressPacket> makeTestPacket(uint32_t qpNum) {
  // The arbiter never sends the packet, so no socket needs to be resolved.
  return std::make_unique<EgressPacket>(
      std::make_unique<AckPacket>(
          BaseHeader::BaseHeaderBuilder()
              .setOpcode(
                  BaseHeader::Opcode(
                      BaseHeader::Opcode::OperationType::SEL_ACK))
              .setVersion(kMetaRoceProtocolVersion)
              .setFlowId(0)
              .setDstQp(0)
              .setSrcQp(qpNum)
              .build(),
          AckHeader::AckHeaderBuilder()
              .setAckStatus(AckStatus::ACK)
              .setCumulativeFsn(0)
              .setMaxRcvdFsn(0)
              .setTimestamp(0)
              .setCumulRespDeliveredMsn(0)
              .build()),
      /*socketToSendOn=*/nullptr,
      folly::IPAddress("fc00::1"),
      qpNum,
      EgressPacket::RxmitType::None);
}

CO_TEST(
    ConcurrentQpArbiterTest,
    ARacedSignalRetriesTheAckApplyNotJustTheBuild) {
  ConcurrentQpArbiter arbiter;
  int applies = 0;
  int builds = 0;
  QpArbiterIface::ReadySignal* signal = nullptr;
  // The first build stands in for a receiver handing an ACK over mid-turn: it
  // signals, and the exchange that would end the turn consumes that signal, so
  // the retry has to apply again or the ACK behind it is never applied.
  auto builder = [&]() -> std::unique_ptr<EgressPacket> {
    if (++builds == 1) {
      signal->postSignal();
      return nullptr;
    }
    return makeTestPacket(8);
  };
  auto applier = [&]() {
    ++applies;
    return false;
  };
  signal = arbiter.addQp(8, builder, applier);
  CO_ASSERT_NE(signal, nullptr) << "precondition: addQp must return a handle";

  signal->postSignal();
  auto claimed = co_await arbiter.claimNextPacketToSend();
  EXPECT_NE(claimed.packet, nullptr)
      << "the second build produces a packet, so the claim must return one";
  EXPECT_EQ(builds, 2)
      << "a signal raced against the turn must make the arbiter build again "
         "rather than settle on the first empty build";
  EXPECT_EQ(applies, builds)
      << "every build attempt must be preceded by an ACK apply, or an ACK "
         "handed over between attempts is stranded until the RTO";

  co_await arbiter.shutdown();
}

CO_TEST(ConcurrentQpArbiterTest, SignalThenClaimServicesSignaledQp) {
  ConcurrentQpArbiter arbiter;
  int builderCalls = 0;
  auto builder = [&]() {
    ++builderCalls;
    return makeTestPacket(7);
  };
  auto* signal = arbiter.addQp(7, builder);
  CO_ASSERT_NE(signal, nullptr) << "addQp must return a readiness handle";

  EXPECT_TRUE(signal->trySignalReady())
      << "trySignalReady accepts the signal on the lock-free fast path";
  auto claimed = co_await arbiter.claimNextPacketToSend();
  EXPECT_NE(claimed.packet, nullptr)
      << "a signaled QP must be claimable without blocking";
  EXPECT_EQ(claimed.qpNum, 7u)
      << "the claim must service the QP that was signaled";
  EXPECT_EQ(builderCalls, 1) << "the arbiter invokes the builder exactly once";

  co_await arbiter.shutdown();
}

CO_TEST(ConcurrentQpArbiterTest, ReenqueueQpMakesClaimableAgain) {
  ConcurrentQpArbiter arbiter;
  int builderCalls = 0;
  auto builder = [&]() {
    ++builderCalls;
    return makeTestPacket(5);
  };
  auto* signal = arbiter.addQp(5, builder);

  EXPECT_TRUE(signal->trySignalReady())
      << "trySignalReady accepts the signal on the lock-free fast path";
  auto first = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(first.qpNum, 5u);
  EXPECT_TRUE(arbiter.reenqueueQp(5))
      << "reenqueueQp succeeds when the ready queue has room";

  auto second = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(second.qpNum, 5u)
      << "reenqueueQp puts the QP back, so it is claimable again";
  EXPECT_EQ(builderCalls, 2);

  co_await arbiter.shutdown();
}

CO_TEST(ConcurrentQpArbiterTest, EagainFaultParksQpUntilResignaled) {
  ConcurrentQpArbiter arbiter;
  int builderCalls = 0;
  auto builder = [&]() {
    ++builderCalls;
    return makeTestPacket(9);
  };
  auto* signal = arbiter.addQp(9, builder);

  signal->postSignal();
  auto claimed = co_await arbiter.claimNextPacketToSend();
  CO_ASSERT_NE(claimed.packet, nullptr)
      << "a signaled QP must yield a claimable packet before the fault path";

  // EAGAIN: the packet's data was already moved to the socket, so clear it
  // before handing it back to the fault handler.
  claimed.packet->packet = nullptr;
  co_await arbiter.handleFaultOnEgress(std::move(claimed.packet), EAGAIN);

  signal->postSignal();
  claimed = co_await arbiter.claimNextPacketToSend();
  EXPECT_NE(claimed.packet, nullptr)
      << "after an EAGAIN park, a fresh signal makes the QP claimable again";
  EXPECT_EQ(builderCalls, 2);

  co_await arbiter.shutdown();
}

CO_TEST(ConcurrentQpArbiterTest, EagainFaultReEnqueuesWhenSignaledDuringTurn) {
  ConcurrentQpArbiter arbiter;
  int builderCalls = 0;
  auto builder = [&]() {
    ++builderCalls;
    return makeTestPacket(11);
  };
  auto* signal = arbiter.addQp(11, builder);

  signal->postSignal();
  auto claimed = co_await arbiter.claimNextPacketToSend();
  CO_ASSERT_NE(claimed.packet, nullptr)
      << "a signaled QP must yield a claimable packet before the EAGAIN race path";

  // A signal arrives during the (about-to-fail) write: it bumps pending past
  // pendingBeforeBuild without enqueuing, since the QP is already owned.
  signal->postSignal();
  claimed.packet->packet = nullptr;
  co_await arbiter.handleFaultOnEgress(std::move(claimed.packet), EAGAIN);

  // The raced signal must not be lost: the EAGAIN park must re-enqueue rather
  // than release to idle, so the QP is immediately claimable without a fresh
  // signal.
  claimed = co_await arbiter.claimNextPacketToSend();
  EXPECT_NE(claimed.packet, nullptr)
      << "a raced signal must re-enqueue a claimable packet, not an empty one";
  EXPECT_EQ(claimed.qpNum, 11u)
      << "a signal racing an EAGAIN fault must re-enqueue the QP, not park it";
  EXPECT_EQ(builderCalls, 2);

  co_await arbiter.shutdown();
}

CO_TEST(ConcurrentQpArbiterTest, EmptyBuildReleasesQpAndClaimsNext) {
  ConcurrentQpArbiter arbiter;
  int emptyBuilds = 0;
  // q1 has nothing to send: its build yields null, so the claim must release it
  // to idle and move on rather than hand back an empty packet.
  auto* s1 = arbiter.addQp(1, [&]() -> std::unique_ptr<EgressPacket> {
    ++emptyBuilds;
    return nullptr;
  });
  auto* s2 = arbiter.addQp(2, [&]() { return makeTestPacket(2); });

  s1->postSignal(); // enqueued first, so claimed first
  s2->postSignal();

  auto claimed = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(claimed.qpNum, 2u)
      << "a QP whose build yields nothing is released and the next QP serviced";
  EXPECT_EQ(emptyBuilds, 1)
      << "the empty QP's build runs once, then the QP is released to idle";

  co_await arbiter.shutdown();
}

CO_TEST(ConcurrentQpArbiterTest, SignalDuringEmptyBuildRetakesTurnInPlace) {
  ConcurrentQpArbiter arbiter;
  int builderCalls = 0;
  QpArbiterIface::ReadySignal* s = nullptr;
  // The first build yields nothing but posts a signal mid-turn (bumping pending
  // past pendingBeforeBuild); the second build produces the packet. The claim
  // must re-take its turn in place -- same QP, no re-enqueue -- rather than
  // release the QP and strand the raced signal.
  s = arbiter.addQp(4, [&]() -> std::unique_ptr<EgressPacket> {
    if (++builderCalls == 1) {
      s->postSignal(); // races the turn; pending bumps, QP already owned
      return nullptr;
    }
    return makeTestPacket(4);
  });

  s->postSignal();
  auto claimed = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(claimed.qpNum, 4u)
      << "a signal racing an empty build re-takes the turn in place, not idle";
  EXPECT_EQ(builderCalls, 2)
      << "the raced signal forces a second build within the same claimed turn";

  co_await arbiter.shutdown();
}

CO_TEST(ConcurrentQpArbiterTest, StaleSignalForRemovedQpDroppedAndCounted) {
  ConcurrentQpArbiter arbiter;
  constexpr uint32_t kStaleQpNum = 1; // enqueued first, so claimed first
  constexpr uint32_t kLiveQpNum = 2;
  auto* staleSignal =
      arbiter.addQp(kStaleQpNum, [&]() { return makeTestPacket(kStaleQpNum); });
  auto* liveSignal =
      arbiter.addQp(kLiveQpNum, [&]() { return makeTestPacket(kLiveQpNum); });

  staleSignal->postSignal();
  liveSignal->postSignal();
  // The queued entry for kStaleQpNum outlives its registration; the claim loop
  // must skip it rather than dereference a missing registration.
  arbiter.removeQp(kStaleQpNum);

  auto claimed = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(claimed.qpNum, kLiveQpNum)
      << "a signaled-then-removed QP is skipped and the live QP serviced";
  EXPECT_EQ(arbiter.getStaleSignalDropCount(), 1u)
      << "dropping the stale (removed) QP is counted for observability";

  co_await arbiter.shutdown();
}

CO_TEST(ConcurrentQpArbiterTest, ShutdownUnblocksClaimWithEmptyPacket) {
  ConcurrentQpArbiter arbiter;
  co_await arbiter.shutdown();

  auto claimed = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(claimed.packet, nullptr)
      << "claimNextPacketToSend returns an empty packet once shut down, so "
         "consumers exit instead of blocking";
}

CO_TEST(ConcurrentQpArbiterTest, BuildBatchStopsAtCapAndKeepsTheTurn) {
  ConcurrentQpArbiter arbiter;
  int packetsLeft = 5;
  auto* signal = arbiter.addQp(7, [&]() -> std::unique_ptr<EgressPacket> {
    return packetsLeft-- > 0 ? makeTestPacket(7) : nullptr;
  });
  auto* other = arbiter.addQp(8, [] { return makeTestPacket(8); });
  signal->postSignal();
  auto claimed = co_await arbiter.claimNextPacketToSend();
  CO_ASSERT_EQ(claimed.qpNum, 7u)
      << "precondition: the claimed packet is the signaled QP's";

  EXPECT_EQ(
      arbiter.buildBatch(7, std::move(claimed.packet), /*batchCap=*/3).size(),
      3u)
      << "a QP with packets to spare fills the batch to its cap and no further";

  auto more = arbiter.nextPacketOrIdle(7);
  CO_ASSERT_NE(more, nullptr)
      << "precondition: the QP has one packet left to start the next batch";
  EXPECT_EQ(arbiter.buildBatch(7, std::move(more), /*batchCap=*/8).size(), 2u)
      << "the batch takes every packet the QP still has, then stops when it "
         "runs dry";

  packetsLeft = 1;
  signal->postSignal();
  other->postSignal();
  auto next = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(next.qpNum, 8u)
      << "a dry build must keep the turn, so a signal during the send is "
         "recorded rather than queuing the QP for another send thread";
  EXPECT_NE(arbiter.nextPacketOrIdle(7), nullptr)
      << "the same turn builds the packet that signal readied";
  EXPECT_EQ(arbiter.nextPacketOrIdle(7), nullptr)
      << "with no packet left, the QP settles to idle, ending the turn";

  const uint64_t staleBefore = arbiter.getStaleSignalDropCount();
  EXPECT_EQ(
      arbiter.buildBatch(99, makeTestPacket(99), /*batchCap=*/8).size(), 1u)
      << "a QP the arbiter does not hold adds no packets to the one it was "
         "given";
  EXPECT_EQ(arbiter.getStaleSignalDropCount(), staleBefore + 1)
      << "a build for a removed QP is a stale signal, and teardown diagnostics "
         "count it";

  co_await arbiter.shutdown();
}

TEST(ConcurrentQpArbiterTest, ABatchOfOneLooksUpNoQp) {
  ConcurrentQpArbiter arbiter;

  EXPECT_EQ(
      arbiter.buildBatch(99, makeTestPacket(99), /*batchCap=*/1).size(), 1u)
      << "a cap of one is filled by the packet the caller already holds";
  EXPECT_EQ(arbiter.getStaleSignalDropCount(), 0u)
      << "a batch of one needs no registry lookup, so a QP removed mid-turn "
         "is not counted as a stale signal";
}

// On a full ready queue reenqueueQp reports the overflow; the caller then
// keeps sending the QP in place via nextPacketOrIdle -- packets first, then a
// null that settles it to idle -- so the work is never stranded.
CO_TEST(ConcurrentQpArbiterTest, ReenqueueOverflowKeepsSendingThenSettles) {
  ConcurrentQpArbiter arbiter(/*readyQueueCapacity=*/2);
  bool q3HasData = true;
  auto* s1 = arbiter.addQp(1, [] { return makeTestPacket(1); });
  auto* s2 = arbiter.addQp(2, [] { return makeTestPacket(2); });
  auto* s3 = arbiter.addQp(3, [&]() -> std::unique_ptr<EgressPacket> {
    return q3HasData ? makeTestPacket(3) : nullptr;
  });

  // Own q3 (claimed, off the queue).
  s3->postSignal();
  auto c3 = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(c3.qpNum, 3u);

  // Fill the two-slot queue so a re-enqueue cannot land.
  s1->postSignal();
  s2->postSignal();
  EXPECT_FALSE(arbiter.reenqueueQp(3))
      << "reenqueueQp reports the full ready queue instead of blocking";

  // The caller keeps sending q3 in place: another packet without a queue
  // slot...
  EXPECT_NE(arbiter.nextPacketOrIdle(3), nullptr)
      << "nextPacketOrIdle yields the claimed QP's next packet off-queue";
  // ...then a null that settles it to idle once it has nothing to send.
  q3HasData = false;
  EXPECT_EQ(arbiter.nextPacketOrIdle(3), nullptr)
      << "a dry build settles the QP to idle";

  // Proof the token dropped to 0: a fresh signal re-enqueues q3 (the 0->1
  // edge). Claim one off the queue first so the enqueue has room, then let q3
  // send again.
  q3HasData = true;
  auto c1 = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(c1.qpNum, 1u);
  s3->postSignal();
  auto n1 = co_await arbiter.claimNextPacketToSend();
  auto n2 = co_await arbiter.claimNextPacketToSend();
  EXPECT_EQ(n1.qpNum, 2u) << "the QP queued before the re-signal goes first";
  EXPECT_EQ(n2.qpNum, 3u)
      << "if the settle had stranded q3, this 0->1 signal would be a no-op bump "
         "and q3 would never return";

  co_await arbiter.shutdown();
}

} // namespace libsoftmetaroce
