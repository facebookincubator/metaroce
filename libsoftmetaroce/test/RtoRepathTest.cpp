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
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/test/AckProcessedAt.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <libsoftmetaroce/test/QpTestBase.h>
#include <array>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <vector>

DECLARE_bool(libsoftmetaroce_slow_receiver);
DECLARE_uint32(libsoftmetaroce_min_rto_us);

using namespace testing;
namespace libsoftmetaroce {
namespace {

ibv_gid_entry makeUdpEntry() {
  ibv_gid_entry entry{};
  entry.gid_type = kGidTypeMetaRoceUdp;
  return entry;
}

} // namespace

class RtoRepathTest : public QpTestBase, public WithParamInterface<bool> {
 protected:
  enum class AckKind { Cumulative, Sack, Brnr };

  struct Pair {
    uint16_t localId;
    folly::IPAddress local;
    uint16_t remoteId;
    folly::IPAddress remote;
  };

  // Owned markers park after the RTO returns and releases the CC lock. A
  // bootstrap marker parks before any frames arm their timers.
  struct TimerGate {
    void park() {
      std::unique_lock lock(mutex);
      ++parks;
      changed.notify_all();
      changed.wait(lock, [this] { return open || permits >= parks; });
    }

    bool waitPending() {
      std::unique_lock lock(mutex);
      return changed.wait_for(
          lock, std::chrono::seconds(5), [this] { return parks > permits; });
    }

    void permit() {
      std::lock_guard lock(mutex);
      ++permits;
      changed.notify_all();
    }

    void release() {
      std::lock_guard lock(mutex);
      open = true;
      changed.notify_all();
    }

    std::thread::id timerThread;
    std::mutex mutex;
    std::condition_variable changed;
    size_t parks{0};
    size_t permits{0};
    bool open{false};
  };

  void SetUp() override {
    FLAGS_libsoftmetaroce_single_owner_tx = GetParam();
    FLAGS_libsoftmetaroce_rtos_before_repath = 2;
    FLAGS_libsoftmetaroce_min_rto_us = 1000;
    FLAGS_libsoftmetaroce_max_rto_us = 1000;
    FLAGS_libsoftmetaroce_slow_receiver = false;
    QpTestBase::SetUp();
    auto identification = std::make_shared<TimerGate>();
    auto identified = std::make_shared<folly::Baton<>>();
    retxTimer.scheduleTimeout(
        std::chrono::microseconds(0),
        HighResTimer::Callback([identification, identified] {
          identification->timerThread = std::this_thread::get_id();
          identified->post();
        }));
    ASSERT_TRUE(identified->try_wait_for(std::chrono::seconds(5)))
        << "precondition: the public timer callback identifies the gated thread";
    timerThread = identification->timerThread;
    sender = std::make_unique<LimitedTestSender>(
        arbiter,
        [this](folly::EventBase* evb) {
          if (failNextSocketCreation) {
            failNextSocketCreation = false;
            ++socketCreationFailures;
            throw std::runtime_error("injected socket construction failure");
          }
          ++socketCreations;
          auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
          ON_CALL(*socket, bindImpl(_))
              .WillByDefault([this](const folly::SocketAddress& address) {
                boundLocals.push_back(address.getIPAddress());
              });
          return std::make_pair(std::move(socket), std::unique_ptr<Modifier>{});
        },
        LimitedTestSender::kDefaultSendToPort,
        LimitedTestSender::kDisableSendLoop);
    sender->setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  }

  void resetQp() {
    if (gate) {
      gate->release();
    }
    testQp.reset();
    retxTimer.fenceCallbacks();
  }

  void TearDown() override {
    resetQp();
    QpTestBase::TearDown();
  }

  bool advanceTimeouts(size_t count) {
    for (size_t i = 0; i < count; ++i) {
      gate->permit();
      if (!gate->waitPending()) {
        return false;
      }
    }
    return true;
  }

  bool sendFrames(uint32_t frames, uint16_t numFlows = 1) {
    if (!postMessage(*testQp, frames * numFlows)) {
      return false;
    }
    for (uint32_t i = 0; i < frames; ++i) {
      for (uint16_t flowId = 0; flowId < numFlows; ++flowId) {
        auto packet = testQp->getTxFlowById(flowId)->getNextPacketToSend();
        if (!packet) {
          ADD_FAILURE() << "precondition: flow " << flowId
                        << " must send frame " << i << " before its RTO";
          return false;
        }
      }
    }
    return gate->waitPending();
  }

  std::vector<Pair> distinctPairs(size_t count = 2) const {
    std::vector<Pair> pairs;
    pairs.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
      pairs.push_back({i, locals[i], i, remotes[i]});
    }
    return pairs;
  }

  std::unique_ptr<Qp> makeQp(
      const std::vector<Pair>& pairs,
      uint32_t qpNum = 30,
      uint32_t numFlows = 1) {
    auto result = std::make_unique<Qp>(
        qpNum, retxTimer, arbiter, mrs, &cq, &otherIbvQpStorage);
    result->setMtu(mtu);
    result->setRemoteQpNum(0);
    result->configureWatchdogInterval(std::chrono::hours(1));
    gate = std::make_shared<TimerGate>();
    gate->timerThread = timerThread;
    auto cc = std::make_unique<NiceMock<MockCongestionController>>();
    ON_CALL(*cc, getAvailableWindowSize(_))
        .WillByDefault([state = gate, timer = &retxTimer](uint16_t) {
          if (std::this_thread::get_id() != state->timerThread) {
            return uint16_t{100};
          }
          // The wheel queues a zero-duration marker without invoking it inline.
          // The outer RTO returns before this marker can park the timer thread.
          timer->scheduleTimeout(
              std::chrono::microseconds(0),
              HighResTimer::Callback([state] { state->park(); }));
          return uint16_t{0};
        });
    result->setCongestionController(std::move(cc));
    const auto entry = makeUdpEntry();
    for (const auto& pair : pairs) {
      result->addPortPair(
          pair.localId, pair.local, entry, pair.remoteId, pair.remote);
    }
    result->setupFlows(numFlows, 0, sender.get());
    retxTimer.scheduleTimeout(
        std::chrono::microseconds(0),
        HighResTimer::Callback([state = gate] { state->park(); }));
    const bool bootstrapped = gate->waitPending();
    EXPECT_TRUE(bootstrapped)
        << "precondition: the bootstrap marker parks before any frame arms an RTO";
    if (!bootstrapped) {
      gate->release();
    }
    return result;
  }

  bool postMessage(Qp& target, uint32_t frames) {
    auto wqe = std::make_unique<SendWqe>(
        23, oneSge(payload.data(), frames * mtu), mtu);
    return target.postWqe(kMetaRoceInitialTag, std::move(wqe)) &&
        processWqes(&target, 1, 0);
  }

  auto route(const TxFlow& flow) const {
    return std::make_tuple(
        sender->getAddressFromSocketId(flow.getSocketId()),
        flow.getRemoteAddr(),
        flow.getSocketId());
  }

  void expectPacketRoute(
      const EgressPacket& packet,
      const TxFlow& flow,
      const std::pair<folly::IPAddress, folly::IPAddress>& expectedEndpoints)
      const {
    EXPECT_EQ(
        std::make_pair(
            sender->getAddressFromSocketId(flow.getSocketId()),
            packet.remoteAddr),
        expectedEndpoints)
        << "retransmissions must use the selected configured endpoints";
    EXPECT_EQ(
        packet.socketToSendOn,
        sender->getSendSocket(flow.getSocketId(), EgressPacket::Kind::Data))
        << "retransmissions must use the source socket selected for the flow";
  }

  void expectRepeatedVisitPreservesRoute(TxFlow& flow) {
    const auto selectedRoute = route(flow);
    (void)flow.getNextPacketToSend();
    EXPECT_EQ(route(flow), selectedRoute)
        << "the sender must apply RTO repathing only once per observed timeout count";
  }

  AckHeader makePartialAck(AckKind ackKind) const {
    BitVector<AckHeader::kAckTrackerBits> sackBits;
    BitVector<AckHeader::kAckTrackerBits> rnrBits;
    if (ackKind == AckKind::Sack) {
      sackBits.setBit(2);
    } else if (ackKind == AckKind::Brnr) {
      rnrBits.setBit(2);
    }
    const auto ackStatus = ackKind == AckKind::Brnr ? AckStatus::BRNR
        : ackKind == AckKind::Sack ? AckStatus::ACK_OUT_OF_ORDER
                                   : AckStatus::ACK;
    auto ackBuilder =
        AckHeader::AckHeaderBuilder()
            .setTag(kMetaRoceInitialTag)
            .setCumulativeFsn(ackKind == AckKind::Cumulative ? 0 : UINT16_MAX)
            .setMaxRcvdFsn(ackKind == AckKind::Cumulative ? 0 : 2)
            .setAckStatus(ackStatus)
            .setCumulRespDeliveredMsn(0)
            .setFsnBitVector(sackBits)
            .setRnrBitVector(rnrBits)
            .setTimestamp(getNowNs() - kNsPerUs);
    if (ackKind == AckKind::Brnr) {
      ackBuilder.setRnrBackoffTimeExp(kBrnrBackoffTimeExp);
    }
    return std::move(ackBuilder).build();
  }

  void applyAck(Qp& target, const AckHeader& ack) {
    if (target.usesSingleOwnerRxRing()) {
      {
        auto receiveGuard = std::unique_lock(target.receiveLock());
        target.enqueueReceivedAck(0, ack, steadyNowNs());
      }
      target.applyHandedOffAcks();
    } else {
      auto inlineGuard = target.lockForInlineAckApply();
      target.getTxFlowById(0)->processAckPacket(
          ack, simulatedProcessedAtNs(ack.getTimestamp()));
    }
  }

  gflags::FlagSaver flagSaver;
  std::thread::id timerThread;
  std::shared_ptr<TimerGate> gate;
  const std::array<folly::IPAddress, 4> locals{
      folly::IPAddress("fc00::1"),
      folly::IPAddress("fc00::2"),
      folly::IPAddress("fc00::3"),
      folly::IPAddress("fc00::4")};
  const std::array<folly::IPAddress, 4> remotes{
      folly::IPAddress("fc00::101"),
      folly::IPAddress("fc00::102"),
      folly::IPAddress("fc00::103"),
      folly::IPAddress("fc00::104")};
  // QP teardown reaches its WQEs and Sender, including the captured counter.
  std::vector<char> payload = std::vector<char>(4096 * 16);
  size_t socketCreations{0};
  size_t socketCreationFailures{0};
  bool failNextSocketCreation{false};
  std::vector<folly::IPAddress> boundLocals;
  std::unique_ptr<LimitedTestSender> sender;
  std::unique_ptr<Qp> testQp;
};

TEST_P(RtoRepathTest, EntropyAppliesOncePerObservedTimeoutCount) {
  testQp = makeQp(distinctPairs(1));
  auto* flow = testQp->getTxFlowById(0);
  ASSERT_NE(flow, nullptr);
  ASSERT_TRUE(sendFrames(1))
      << "precondition: an outstanding frame arms the entropy recovery timer";
  const auto original = route(*flow);
  const auto configuredEndpoints = std::make_pair(locals[0], remotes[0]);

  ASSERT_TRUE(advanceTimeouts(1))
      << "the below-threshold callback must return before the sender visits";
  auto belowThreshold = flow->getNextPacketToSend();
  ASSERT_TRUE(belowThreshold)
      << "the sender must retransmit outstanding data below the entropy threshold";
  EXPECT_EQ(route(*flow), original)
      << "a below-threshold sender visit must preserve the route and source entropy";

  ASSERT_TRUE(advanceTimeouts(1))
      << "the threshold callback must return before observing timer-owned work";
  EXPECT_EQ(route(*flow), original)
      << "the timer must leave threshold entropy selection to the sender";
  auto thresholdPacket = flow->getNextPacketToSend();
  ASSERT_TRUE(thresholdPacket)
      << "the threshold sender visit must construct a retransmission";
  EXPECT_NE(flow->getSocketId(), std::get<2>(original))
      << "the threshold sender visit must vary UDP source-port entropy";
  expectPacketRoute(*thresholdPacket, *flow, configuredEndpoints);
  const auto thresholdRoute = route(*flow);
  expectRepeatedVisitPreservesRoute(*flow);

  ASSERT_TRUE(advanceTimeouts(3))
      << "coalesced callbacks must return before the sender consumes their count";
  EXPECT_EQ(route(*flow), thresholdRoute)
      << "coalesced timer callbacks must preserve the sender-owned route";
  auto coalescedPacket = flow->getNextPacketToSend();
  ASSERT_TRUE(coalescedPacket)
      << "the sender must retransmit outstanding data after coalesced callbacks";
  EXPECT_NE(flow->getSocketId(), std::get<2>(thresholdRoute))
      << "a newly observed coalesced timeout count must vary source-port entropy";
  expectPacketRoute(*coalescedPacket, *flow, configuredEndpoints);
  expectRepeatedVisitPreservesRoute(*flow);
}

TEST_P(RtoRepathTest, EntropyAcquisitionFailureRetriesOnlyAfterNewTimeout) {
  testQp = makeQp(distinctPairs(1));
  auto* flow = testQp->getTxFlowById(0);
  ASSERT_NE(flow, nullptr);
  ASSERT_EQ(testQp->usesSingleOwnerRxRing(), GetParam())
      << "precondition: the QP uses the requested ACK application mode";
  ASSERT_TRUE(postMessage(*testQp, 1))
      << "precondition: an outstanding frame is needed to drive entropy recovery";
  ASSERT_TRUE(testQp->getNextPacketToSend())
      << "precondition: the frame must be sent before it can time out";
  ASSERT_TRUE(gate->waitPending())
      << "precondition: the timer must park before advancing the timeout count";
  const auto original = route(*flow);
  const auto configuredEndpoints = std::make_pair(locals[0], remotes[0]);
  const auto creationsBefore = socketCreations;
  failNextSocketCreation = true;

  ASSERT_TRUE(advanceTimeouts(2))
      << "entropy acquisition must follow complete threshold timer callbacks";
  std::unique_ptr<EgressPacket> retransmission;
  ASSERT_NO_THROW(retransmission = testQp->getNextPacketToSend())
      << "entropy acquisition failure must not escape the sender visit";
  ASSERT_TRUE(retransmission)
      << "entropy acquisition failure must not suppress outstanding retransmissions";
  EXPECT_EQ(socketCreationFailures, 1u)
      << "the retransmission must survive an actual socket construction exception";
  EXPECT_EQ(socketCreations, creationsBefore)
      << "the failed acquisition must not retry construction within the same visit";
  EXPECT_EQ(route(*flow), original)
      << "the failed acquisition must retain the complete usable old route";
  EXPECT_EQ(retransmission->rxmitType, EgressPacket::RxmitType::Rto)
      << "socket acquisition failure must preserve timer-driven retransmission";
  expectPacketRoute(*retransmission, *flow, configuredEndpoints);

  for (int visit = 0; visit < 3; ++visit) {
    (void)testQp->getNextPacketToSend();
  }
  EXPECT_EQ(socketCreations, creationsBefore)
      << "repeated visits must not retry acquisition for a consumed timeout count";
  EXPECT_EQ(route(*flow), original)
      << "repeated visits must preserve the old route until another timeout";

  ASSERT_TRUE(advanceTimeouts(1))
      << "a new timer callback must permit another entropy acquisition attempt";
  auto renewedPacket = testQp->getNextPacketToSend();
  ASSERT_TRUE(renewedPacket)
      << "a new timeout must retransmit the frame after the acquisition failure";
  EXPECT_GT(socketCreations, creationsBefore)
      << "a new timeout must permit construction after the one-shot failure";
  EXPECT_NE(flow->getSocketId(), std::get<2>(original))
      << "a successful acquisition after a new timeout must vary source entropy";
  expectPacketRoute(*renewedPacket, *flow, configuredEndpoints);
}

TEST_P(RtoRepathTest, AcceptedAcksResetEntropyConsumption) {
  for (const auto ackKind :
       {AckKind::Cumulative, AckKind::Sack, AckKind::Brnr}) {
    SCOPED_TRACE(static_cast<int>(ackKind));
    resetQp();
    testQp = makeQp(distinctPairs(1));
    auto* flow = testQp->getTxFlowById(0);
    ASSERT_NE(flow, nullptr);
    ASSERT_EQ(testQp->usesSingleOwnerRxRing(), GetParam())
        << "precondition: ACKs use the requested owner or inline application mode";
    ASSERT_TRUE(sendFrames(4))
        << "precondition: each ACK needs sent frames and must leave data unacked";
    ASSERT_EQ(testQp->getHrnrState(), HrnrState::NORMAL_TX)
        << "precondition: ACK application must not synchronously cancel HRNR timers";
    const auto originalSocket = flow->getSocketId();
    const auto configuredEndpoints = std::make_pair(locals[0], remotes[0]);
    ASSERT_TRUE(advanceTimeouts(3))
        << "the first entropy selection must follow three complete timer callbacks";
    auto entropyPacket = flow->getNextPacketToSend();
    ASSERT_TRUE(entropyPacket)
        << "the sender must expose the consumed timeout count on a retransmission";
    ASSERT_NE(flow->getSocketId(), originalSocket)
        << "precondition: the ACK follows a consumed count that selected new entropy";
    const auto entropyRoute = route(*flow);
    expectPacketRoute(*entropyPacket, *flow, configuredEndpoints);

    const auto ack = makePartialAck(ackKind);
    applyAck(*testQp, ack);
    EXPECT_EQ(route(*flow), entropyRoute)
        << "an accepted ACK must retain the route while resetting timeout consumption";
    ASSERT_TRUE(flow->hasUnackedData())
        << "precondition: accepted ACKs leave frames to drive a new timeout run";

    ASSERT_TRUE(advanceTimeouts(3))
        << "three new callbacks must repeat the consumed count without a sender visit";
    EXPECT_EQ(route(*flow), entropyRoute)
        << "timer callbacks after an ACK must leave entropy selection to the sender";
    auto renewedPacket = flow->getNextPacketToSend();
    ASSERT_TRUE(renewedPacket)
        << "the sender must retransmit the frames left unacked after the ACK";
    EXPECT_NE(flow->getSocketId(), std::get<2>(entropyRoute))
        << "an accepted ACK must allow a repeated timeout count to select entropy again";
    expectPacketRoute(*renewedPacket, *flow, configuredEndpoints);
  }
}

TEST_P(RtoRepathTest, DisabledAndOversizedThresholdPreserveRoute) {
  for (const uint32_t threshold : {0u, std::numeric_limits<uint32_t>::max()}) {
    SCOPED_TRACE(threshold);
    resetQp();
    FLAGS_libsoftmetaroce_rtos_before_repath = threshold;
    testQp = makeQp(distinctPairs());
    auto* flow = testQp->getTxFlowById(0);
    ASSERT_NE(flow, nullptr);
    ASSERT_TRUE(sendFrames(1))
        << "precondition: an unacked frame keeps genuine timeouts armed";
    const auto original = route(*flow);
    const auto creationsBefore = socketCreations;
    for (int i = 0; i < 5; ++i) {
      ASSERT_TRUE(advanceTimeouts(1))
          << "route preservation must be observed after each real timeout";
      ASSERT_TRUE(flow->getNextPacketToSend())
          << "disabled or oversized thresholds must still retransmit data";
      EXPECT_EQ(route(*flow), original)
          << "disabled or unreachable thresholds must preserve routing after "
          << i + 1 << " timeouts";
    }
    EXPECT_EQ(socketCreations, creationsBefore)
        << "disabled or oversized thresholds must not allocate entropy sockets";
  }
}

TEST_P(RtoRepathTest, DisabledFlowDoesNotApplyRtoRepath) {
  testQp = makeQp(distinctPairs());
  auto* flow = testQp->getTxFlowById(0);
  ASSERT_NE(flow, nullptr);
  ASSERT_TRUE(sendFrames(1))
      << "precondition: a sent frame keeps timeouts armed after disabling the flow";
  flow->stopSending();
  const auto original = route(*flow);
  const auto creationsBefore = socketCreations;

  ASSERT_TRUE(advanceTimeouts(3))
      << "the disabled flow must receive actual post-threshold timeouts";
  EXPECT_EQ(flow->getNextPacketToSend(), nullptr)
      << "a disabled flow must not emit packets during RTO recovery";
  EXPECT_EQ(route(*flow), original)
      << "a disabled flow must not apply pending routing changes";
  EXPECT_EQ(socketCreations, creationsBefore)
      << "a disabled flow must not acquire recovery sockets";
}

TEST_P(RtoRepathTest, SinglePairPreservesEndpoints) {
  testQp = makeQp(distinctPairs(1));
  auto* flow = testQp->getTxFlowById(0);
  ASSERT_NE(flow, nullptr);
  ASSERT_TRUE(sendFrames(1))
      << "precondition: an outstanding frame is needed to drive RTO recovery";
  const auto originalSocket = flow->getSocketId();
  for (const uint16_t count : {1, 2, 3}) {
    ASSERT_TRUE(advanceTimeouts(1))
        << "each real timeout must finish before retransmission";
    ASSERT_TRUE(flow->getNextPacketToSend())
        << "an unanswered frame must keep retransmitting on its configured pair";
    EXPECT_EQ(flow->getRemoteAddr(), remotes[0])
        << "a pinned flow must retain its only configured destination";
    EXPECT_EQ(sender->getAddressFromSocketId(flow->getSocketId()), locals[0])
        << "a pinned flow may vary UDP entropy but must retain its local address";
    if (count == 2) {
      EXPECT_NE(flow->getSocketId(), originalSocket)
          << "a single configured pair still permits source-port entropy changes";
    }
  }
}

TEST_P(RtoRepathTest, TimeoutPublishesWorkOnlyWithOutstandingFrames) {
  testQp = makeQp(distinctPairs());
  auto* flow = testQp->getTxFlowById(0);
  ASSERT_NE(flow, nullptr);
  const auto original = route(*flow);
  const auto creationsBefore = socketCreations;
  for (int i = 0; i < 3; ++i) {
    flow->setRetransmissionTimer();
    ASSERT_TRUE(advanceTimeouts(1))
        << "the owned marker must park after the explicitly armed idle callback returns";
    EXPECT_EQ(flow->getNumPendingRetxPackets(), 0u)
        << "idle callbacks must not publish retransmission work";
  }
  EXPECT_EQ(route(*flow), original)
      << "idle timer callbacks must preserve the route for future work";
  EXPECT_EQ(socketCreations, creationsBefore)
      << "a timer callback must leave socket acquisition to the owner";
  ASSERT_TRUE(postMessage(*testQp, 1))
      << "precondition: new work must become sendable after the idle callbacks";
  auto first = flow->getNextPacketToSend();
  ASSERT_TRUE(first)
      << "idle callbacks must not prevent the next message from being sent";
  EXPECT_EQ(route(*flow), original)
      << "new work must not inherit a repath from idle timeout callbacks";
  EXPECT_EQ(
      first->socketToSendOn,
      sender->getSendSocket(std::get<2>(original), EgressPacket::Kind::Data))
      << "the first new packet must use the original source socket";

  ASSERT_TRUE(gate->waitPending())
      << "the first actual send must autoarm a gated RTO callback";
  ASSERT_TRUE(advanceTimeouts(1))
      << "the first outstanding callback must return before the owner visits";
  auto belowThreshold = flow->getNextPacketToSend();
  ASSERT_TRUE(belowThreshold)
      << "the first outstanding callback must publish a retransmission";
  EXPECT_EQ(route(*flow), original)
      << "idle callbacks must not consume the next work's RTO allowance";
  ASSERT_TRUE(advanceTimeouts(1))
      << "the second outstanding callback must return before the owner visits";
  ASSERT_GT(flow->getNumPendingRetxPackets(), 0u)
      << "the timer must make outstanding data ready for retransmission";
  EXPECT_EQ(route(*flow), original)
      << "positive RTO callbacks must queue work while retaining the owner-held route";
  EXPECT_EQ(socketCreations, creationsBefore)
      << "positive timer callbacks must not allocate source-port entropy sockets";
  auto retransmission = flow->getNextPacketToSend();
  ASSERT_TRUE(retransmission)
      << "the owner must send the timer's queued retransmission";
  EXPECT_EQ(retransmission->rxmitType, EgressPacket::RxmitType::Rto)
      << "timer-to-owner handoff must preserve the retransmission's RTO classification";
  EXPECT_NE(flow->getSocketId(), std::get<2>(original))
      << "the owner must apply the published threshold before assembling the packet";
  EXPECT_EQ(retransmission->remoteAddr, std::get<1>(original))
      << "the first threshold crossing must retransmit to the original destination";
  EXPECT_EQ(
      retransmission->socketToSendOn,
      sender->getSendSocket(flow->getSocketId(), EgressPacket::Kind::Data))
      << "the retransmission must use the owner-selected entropy socket";
}

INSTANTIATE_TEST_SUITE_P(OwnershipModes, RtoRepathTest, Bool());

} // namespace libsoftmetaroce
