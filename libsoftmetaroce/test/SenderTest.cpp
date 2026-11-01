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

#include <folly/MPMCQueue.h>
#include <folly/ScopeGuard.h>
#include <folly/container/F14Set.h>
#include <folly/synchronization/Baton.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/ClockSource.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/Sender.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/modifier/DuplicateModifier.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/modifier/ReorderModifier.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <libsoftmetaroce/test/SourcePortRecorder.h>
#include <libsoftmetaroce/test/TxTimerSuppressor.h>
#include <array>
#include <memory>
#include <set>
#include <thread>
#include <tuple>
#include <vector>

// NOLINTNEXTLINE(facebook-hte-PreventUseOfDeclareGflag)
DECLARE_bool(libsoftmetaroce_slow_receiver);

using namespace testing;
namespace libsoftmetaroce {

constexpr uint16_t kMetaRoceSendToPort = 0xace;

static std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

class SenderTest : public TestWithParam<int> {
 protected:
  void SetUp() override {
    qp = std::make_unique<Qp>(10, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
    qp->addPortPair(localAddr, remoteAddr);
    qp->setRemoteQpNum(100);
    qp->setMtu(1000);
    auto congestionController =
        std::make_unique<NiceMock<MockCongestionController>>();
    rawCongestionController = congestionController.get();
    qp->setCongestionController(std::move(congestionController));
  }
  void TearDown() override {
    qp = nullptr;
  }

  // Helper function to create a test sender with paths already set up.
  // Tests that drive packet generation deterministically on the test thread
  // pass kDisableSendLoop so no consumer thread races their TxFlow accesses.
  std::unique_ptr<LimitedTestSender> createTestSenderWithFlows(
      const std::function<std::pair<
          std::unique_ptr<folly::AsyncUDPSocket>,
          std::unique_ptr<Modifier>>(folly::EventBase*)>& socketMaker =
          mockUDPSocketMaker,
      bool disableSendLoop = false) {
    auto sender = std::make_unique<LimitedTestSender>(
        arbiter, socketMaker, kMetaRoceSendToPort, disableSendLoop);
    qp->setupFlows(1, 1, sender.get());
    return sender;
  }

  // Helper function to get the mock socket from a sender
  MockUDPSocket* getTestSocket(LimitedTestSender* sender) {
    auto socketId = sender->getSocketIdFromAddress(localAddr);
    return dynamic_cast<MockUDPSocket*>(sender->getSocketById(socketId));
  }

  // A sender without a send loop or the test sender's one-socket limit.
  std::unique_ptr<LimitedTestSender> createUnlimitedTestSender() {
    auto sender = std::make_unique<LimitedTestSender>(
        arbiter,
        mockUDPSocketMaker,
        kMetaRoceSendToPort,
        LimitedTestSender::kDisableSendLoop);
    sender->setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
    return sender;
  }

  // Struct to hold payload for WQE - keeps payload alive
  struct TestWqePayload {
    std::unique_ptr<char[]> payload;
    int frames;
    uint32_t length;
  };

  // Helper function to create and post a test WQE
  TestWqePayload createAndPostTestWqe(
      int frames,
      uint8_t tag = kMetaRoceInitialTag,
      uint64_t workReqId = 0) {
    const uint32_t length = frames * qp->getMtu();
    auto payload = std::make_unique<char[]>(length);
    CHECK(qp->postWqe(
        tag,
        std::make_unique<SendWqe>(
            workReqId, oneSge(payload.get(), length), qp->getMtu())));
    return TestWqePayload{std::move(payload), frames, length};
  }

  // Helper function to set up tx state with the specified number of sent
  // packets
  void setupTxStateWithSentPackets(
      TxFlow* flow,
      Wqe* wqe,
      uint16_t numFrames,
      uint16_t mtu) {
    auto& txState = flow->getTxState();
    for (uint16_t i = 0; i < numFrames; ++i) {
      const uint32_t posn = wqe->allocateNextPosn();
      txState.tracker.emplace_back(
          PacketId(wqe, 0, posn, mtu), Flow::FsnState::UNACKED);
    }
    txState.sndNxt = numFrames;
  }

  HighResTimer retxTimer;
  QpArbiter arbiter;
  MemoryRegions mrs;
  folly::IPAddress localAddr{"fc00::1"};
  folly::IPAddress remoteAddr{"fc00::101"};
  std::unique_ptr<Qp> qp;
  CompletionQueue cq;
  MockCongestionController* rawCongestionController{nullptr};
  struct ibv_qp ibvQpStorage{};
};

/*
 * Creates up to kMaxNumSockets sockets by calling getNextSocketIdToAssign()
 * and checks whether the returned Id is assigned before.
 */
TEST_F(SenderTest, InitAndGetNextSocketId) {
  folly::F14FastSet<uint16_t> assignedSocketIds;
  LimitedTestSender sender(
      arbiter,
      mockUDPSocketMaker,
      kMetaRoceSendToPort,
      LimitedTestSender::kEnableSendLoop);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  for (auto i = 0; i < Sender::kMaxNumSockets; i++) {
    auto socketId = sender.getSocketIdFromAddress(localAddr);
    EXPECT_FALSE(assignedSocketIds.contains(socketId));
    assignedSocketIds.insert(socketId);
  }
  EXPECT_EQ(assignedSocketIds.size(), Sender::kMaxNumSockets);
  EXPECT_TRUE(
      assignedSocketIds.contains(sender.getSocketIdFromAddress(localAddr)));
}

TEST_F(SenderTest, ConcurrentSocketCreationAndSelectionAreRaceFree) {
  constexpr unsigned int kSocketLimit = 4;
  constexpr int kAssigners = 4;
  constexpr int kAssignmentsEach = 200;
  const std::array<folly::IPAddress, 2> localAddrs{
      folly::IPAddress("fc00::1"), folly::IPAddress("fc00::2")};
  LimitedTestSender sender(
      arbiter,
      mockUDPSocketMaker,
      kMetaRoceSendToPort,
      LimitedTestSender::kDisableSendLoop);
  sender.setLimitNumSockets(kSocketLimit);
  std::array<uint16_t, 2> seeded{};
  for (size_t i = 0; i < localAddrs.size(); ++i) {
    seeded[i] = sender.getSocketIdFromAddress(localAddrs[i]);
  }

  folly::MPMCQueue<uint16_t> handedOff(kAssigners * kAssignmentsEach);
  std::thread reader([&] {
    for (int read = 0; read < kAssigners * kAssignmentsEach; ++read) {
      uint16_t socketId = 0;
      handedOff.blockingRead(socketId);
      EXPECT_LT(socketId, kSocketLimit)
          << "the limit caps how many sockets concurrent assignments create";
      EXPECT_NE(
          sender.getSendSocket(socketId, EgressPacket::Kind::Data), nullptr)
          << "an id handed over after its assignment names a built socket";
    }
  });
  std::vector<std::thread> assigners;
  assigners.reserve(kAssigners);
  for (int a = 0; a < kAssigners; ++a) {
    assigners.emplace_back([&, a] {
      uint16_t socketId = seeded[a % seeded.size()];
      for (int i = 0; i < kAssignmentsEach; ++i) {
        socketId = (i % 2 == 0)
            ? sender.getSocketIdFromAddress(localAddrs[(a + i) % 2])
            : sender.getAlternateSocket(socketId);
        handedOff.blockingWrite(socketId);
      }
    });
  }
  for (auto& assigner : assigners) {
    assigner.join();
  }
  reader.join();
}

TEST_F(SenderTest, RepathAtTheSocketLimitKeepsItsSocket) {
  LimitedTestSender sender(
      arbiter,
      mockUDPSocketMaker,
      kMetaRoceSendToPort,
      LimitedTestSender::kDisableSendLoop);
  sender.setLimitNumSockets(1);
  const uint16_t socketId = sender.getSocketIdFromAddress(localAddr);

  EXPECT_EQ(sender.getAlternateSocket(socketId), socketId)
      << "with one socket allowed, a repath has no other socket to move to";
  EXPECT_THROW(sender.getSocketById(socketId + 1), std::out_of_range)
      << "the socket limit caps the sockets a repath creates, as it caps "
         "those a new assignment creates";
}

TEST_F(SenderTest, RepathStaysOnItsLocalAddress) {
  auto sender = createUnlimitedTestSender();
  const folly::IPAddress otherAddr{"fc00::2"};
  std::set<uint16_t> localSockets;
  for (int i = 0; i < 3; ++i) {
    localSockets.insert(sender->getSocketIdFromAddress(localAddr));
    std::ignore = sender->getSocketIdFromAddress(otherAddr);
  }
  const uint16_t first = *localSockets.begin();
  uint16_t current = first;
  std::set<uint16_t> visited{first};
  for (size_t i = 0; i < 3 * localSockets.size(); ++i) {
    const uint16_t next = sender->getAlternateSocket(current);
    EXPECT_NE(next, current)
        << "an available alternative must change the flow's source-port entropy";
    EXPECT_EQ(sender->getAddressFromSocketId(next), localAddr)
        << "changing the local address would leave the configured port pair";
    visited.insert(next);
    current = next;
  }
  EXPECT_EQ(visited, localSockets)
      << "repathing must use every socket on its address without crossing to "
         "another address";
  EXPECT_EQ(current, first)
      << "each complete cycle must reuse the first source port only after "
         "trying the other ports on the address";
}

TEST_F(SenderTest, RepathCreatesOnItsAddressWhenAlone) {
  auto sender = createUnlimitedTestSender();
  const uint16_t only = sender->getSocketIdFromAddress(localAddr);
  const folly::IPAddress otherAddr{"fc00::2"};
  const std::set<uint16_t> existing{
      only,
      sender->getSocketIdFromAddress(otherAddr),
      sender->getSocketIdFromAddress(otherAddr)};

  const uint16_t created = sender->getAlternateSocket(only);
  EXPECT_FALSE(existing.contains(created))
      << "a lone socket needs a new source port on its own address to repath";
  EXPECT_EQ(sender->getAddressFromSocketId(created), localAddr)
      << "a socket on another address would form an unconfigured port pair";
}

TEST_F(SenderTest, RepathAtLimitKeepsItsLocalAddress) {
  auto sender = createUnlimitedTestSender();
  sender->setLimitNumSockets(3);
  const uint16_t only = sender->getSocketIdFromAddress(localAddr);
  const folly::IPAddress otherAddr{"fc00::2"};
  std::ignore = sender->getSocketIdFromAddress(otherAddr);
  std::ignore = sender->getSocketIdFromAddress(otherAddr);

  EXPECT_EQ(sender->getAlternateSocket(only), only)
      << "when no same-address alternative can be created, the flow must keep "
         "its configured port pair";
}

TEST_F(SenderTest, RtoMovesTheWholePairAfterEntropy) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_slow_receiver = false;
  FLAGS_libsoftmetaroce_rtos_before_repath = 1;
  qp->setRemoteQpNum(102);
  const folly::IPAddress otherLocalAddr{"fc00::2"};
  const folly::IPAddress otherRemoteAddr{"fc00::102"};
  qp->addPortPair(otherLocalAddr, otherRemoteAddr);
  qp->configureWatchdogInterval(std::chrono::hours(1));
  std::vector<char> payload(qp->getMtu());
  std::mutex mutex;
  std::condition_variable written;
  std::vector<std::tuple<folly::IPAddress, folly::IPAddress, bool>>
      transmissions;
  using AddressPair = std::pair<folly::IPAddress, folly::IPAddress>;
  std::vector<AddressPair> routeChanges;
  const std::vector<AddressPair> expectedRouteChanges{
      {localAddr, remoteAddr},
      {otherLocalAddr, otherRemoteAddr},
      {localAddr, remoteAddr},
      {otherLocalAddr, otherRemoteAddr}};
  std::set<uint16_t> sourcePorts;
  auto socketMaker = [&](folly::EventBase* evb) {
    auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
    auto boundAddress = std::make_shared<folly::SocketAddress>();
    ON_CALL(*socket, bindImpl(_))
        .WillByDefault([boundAddress](const folly::SocketAddress& address) {
          *boundAddress = address;
        });
    ON_CALL(*socket, write(_, _))
        .WillByDefault([&, boundAddress](
                           const folly::SocketAddress& destination,
                           const std::unique_ptr<folly::IOBuf>& buf) {
          std::lock_guard<std::mutex> lock(mutex);
          folly::io::Cursor cursor(buf.get());
          BaseHeader baseHeader(cursor);
          DataHeader dataHeader(cursor);
          transmissions.emplace_back(
              boundAddress->getIPAddress(),
              destination.getIPAddress(),
              dataHeader.getIsTimeout());
          const AddressPair pair{
              boundAddress->getIPAddress(), destination.getIPAddress()};
          if (routeChanges.empty() || routeChanges.back() != pair) {
            routeChanges.push_back(pair);
          }
          sourcePorts.insert(boundAddress->getPort());
          written.notify_one();
          return buf->computeChainDataLength();
        });
    return std::make_pair(std::move(socket), nullptr);
  };
  auto sender = std::make_unique<LimitedTestSender>(
      arbiter,
      socketMaker,
      kMetaRoceSendToPort,
      LimitedTestSender::kEnableSendLoop);
  sender->setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  SCOPE_EXIT {
    qp = nullptr;
    sender = nullptr;
  };
  qp->setupFlows(2, 2, sender.get());
  std::ignore = sender->getSocketIdFromAddress(localAddr);
  auto* flow = qp->getTxFlowById(0);
  ASSERT_NE(flow, nullptr);
  ASSERT_EQ(flow->getLocalPortIdx(), 0)
      << "precondition: the flow must start on the first configured pair";
  ASSERT_EQ(sender->getAddressFromSocketId(flow->getSocketId()), localAddr)
      << "precondition: the initial socket must match the pair's local address";
  ON_CALL(*rawCongestionController, getAvailableWindowSize(0))
      .WillByDefault(Return(1));
  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          0, oneSge(payload.data(), payload.size()), qp->getMtu())))
      << "precondition: an outstanding frame is needed to drive RTO repathing";
  qp->startProcessingWqes();
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(written.wait_for(lock, std::chrono::seconds(30), [&] {
      return routeChanges.size() >= expectedRouteChanges.size();
    })) << "unanswered retransmissions must keep moving between configured pairs";
  }
  qp = nullptr;
  sender = nullptr;

  const std::set<std::pair<folly::IPAddress, folly::IPAddress>> configuredPairs{
      {localAddr, remoteAddr}, {otherLocalAddr, otherRemoteAddr}};
  for (size_t i = 0; i < transmissions.size(); ++i) {
    const auto& [source, destination, isTimeout] = transmissions[i];
    EXPECT_TRUE(configuredPairs.contains({source, destination}))
        << "RTO recovery must change the source and destination as a configured "
           "pair, never combine addresses from different pairs";
    EXPECT_EQ(isTimeout, i != 0)
        << "each repeat of the unanswered frame must identify timeout recovery";
  }
  ASSERT_GE(routeChanges.size(), expectedRouteChanges.size())
      << "continued RTOs must produce further pair moves without an ACK or APS update";
  routeChanges.resize(expectedRouteChanges.size());
  EXPECT_EQ(routeChanges, expectedRouteChanges)
      << "each later RTO must continue searching after a successful pair move";
  EXPECT_GT(sourcePorts.size(), 1u)
      << "the test must observe source-port changes rather than a pinned flow";
}

TEST_F(SenderTest, AckSocketCyclesThroughSameAddressSockets) {
  auto sender = createUnlimitedTestSender();
  const uint16_t first = sender->getSocketIdFromAddress(localAddr);
  std::ignore = sender->getSocketIdFromAddress(localAddr);
  std::ignore = sender->getSocketIdFromAddress(localAddr);

  const uint16_t next = sender->getAlternateAckSocket(first);
  const uint16_t after = sender->getAlternateAckSocket(next);
  EXPECT_EQ((std::set<uint16_t>{first, next, after}).size(), 3u)
      << "successive changes give the ACK a source port it has not used yet";
  EXPECT_EQ(sender->getAlternateAckSocket(after), first)
      << "the ACK returns to its first socket only after using each other "
         "socket on the address";
}

TEST_F(SenderTest, AckSocketCreatesOnItsAddressWhenAlone) {
  auto sender = createUnlimitedTestSender();
  const uint16_t only = sender->getSocketIdFromAddress(localAddr);
  std::ignore = sender->getSocketIdFromAddress(folly::IPAddress{"fc00::2"});

  const uint16_t created = sender->getAlternateAckSocket(only);
  EXPECT_NE(created, only)
      << "a lone socket cannot change the ACK source port, so one is created";
  EXPECT_EQ(sender->getAddressFromSocketId(created), localAddr)
      << "the ACK keeps the flow's local address while its source port moves";
}

TEST_F(SenderTest, AckSocketAtLimitKeepsCurrent) {
  LimitedTestSender sender(
      arbiter,
      mockUDPSocketMaker,
      kMetaRoceSendToPort,
      LimitedTestSender::kDisableSendLoop);
  sender.setLimitNumSockets(1);
  const uint16_t socketId = sender.getSocketIdFromAddress(localAddr);

  EXPECT_EQ(sender.getAlternateAckSocket(socketId), socketId)
      << "with one socket allowed and no alternative, the ACK keeps its "
         "socket";
}

TEST_F(SenderTest, SendSocketsTakeSuccessivePortsInTheSpecRange) {
  SourcePortRecorder ports;
  LimitedTestSender sender(
      arbiter,
      ports.socketMaker(),
      kMetaRoceSendToPort,
      LimitedTestSender::kDisableSendLoop);
  std::ignore = sender.getSocketIdFromAddress(localAddr);

  ASSERT_EQ(ports.bound.size(), 2u)
      << "precondition: one socket id binds a data and an ACK socket";
  EXPECT_GE(ports.bound[0], SourcePortCounter::kFirstPort)
      << "source ports come from the range 49152-65535";
  EXPECT_EQ(
      ports.bound[1],
      ports.bound[0] == 65535 ? SourcePortCounter::kFirstPort
                              : ports.bound[0] + 1)
      << "the sender's sockets share one counter, so the ACK socket takes the "
         "port after the data socket's";
}

/*
 * Tests the number of packets sents (ie number of calls to mockSocket.write and
 * mockCongestionController.onPacketSent) when a send WQE is posted.
 */
TEST_F(SenderTest, SendLoop) {
  auto sender = createTestSenderWithFlows();
  auto socket = getTestSocket(sender.get());

  // create a path
  const auto flowId = 0;
  ON_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .WillByDefault(Return(1));

  EXPECT_CALL(*rawCongestionController, onPacketSent(flowId))
      .Times(AtLeast(11)); // 11 packets, the rest are retransmissions

  std::mutex mtx;
  std::condition_variable cv;
  int writeCount = 0;
  EXPECT_CALL(*socket, write(_, _))
      .Times(AtLeast(11)) // 11 packets, the rest are retransmissions
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        std::lock_guard<std::mutex> lock(mtx);
        if (++writeCount == 11) {
          cv.notify_one();
        }
        return buf.get()->computeChainDataLength();
      });

  const int frames = 10; // plus one partial
  const uint32_t length = frames * qp->getMtu() + 1;
  std::unique_ptr<char[]> payload(new char[length]);
  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          0 /* workReqId */,
          oneSge(
              payload.get(), /*address*/
              length),
          qp->getMtu())));
  qp->startProcessingWqes();

  {
    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(30), [&writeCount] {
      return writeCount >= 11;
    })) << "timed out waiting for the send loop to transmit 11 packets";
  }
  sender = nullptr;
}

/*
 * Tests the number of retransmission packets sents when an ack with a hole in
 * SACK is processed.
 */
TEST_F(SenderTest, SendRetxPacketsDueToLoss) {
  // Drive retransmission generation on the test thread: with the send loop
  // disabled, processAckPacket and the getNextPacketToSend pump below run
  // serially with no consumer thread racing TxFlow state.
  auto sender = createTestSenderWithFlows(
      mockUDPSocketMaker, LimitedTestSender::kDisableSendLoop);
  // create a path; we have sent five packets (fsn=0..4), una is 0.
  // acked from remote side is -1 (not yet acked zero)
  const auto mtu = qp->getMtu();
  const auto flowId = 0;
  auto flow = qp->getTxFlowById(flowId);

  const int frames = 5;
  const uint32_t length = frames * qp->getMtu();
  std::unique_ptr<char[]> payload(new char[length]);
  auto wqe = std::make_unique<SendWqe>(
      0 /* workReqId */,
      oneSge(
          payload.get(), /*address*/
          length),
      qp->getMtu());

  wqe->setMsn(0);
  wqe->setCsn(0);
  auto& txState = flow->getTxState();
  EXPECT_EQ(txState.sndUna, 0) << "new txstate should have fsn 0 unacked";
  EXPECT_EQ(txState.maxAckdFsn, UINT16_MAX) << "max acked should be -1";

  setupTxStateWithSentPackets(flow, wqe.get(), frames, mtu);

  // Only send retransmission packets
  ON_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .WillByDefault(Return(0));

  // 1, 2, and 3 are lost;  this ack says 0 and 4 are received.
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(3); // packet 4 is acked out of order
  EXPECT_FALSE(fsnBitVector.getBit(0))
      << "bitvector should be initialized false";
  EXPECT_FALSE(fsnBitVector.getBit(1))
      << "bitvector should be initialized false";
  EXPECT_FALSE(fsnBitVector.getBit(2))
      << "bitvector should be initialized false";
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(0)
                                  .setMaxRcvdFsn(4)
                                  .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                                  .setFsnBitVector(fsnBitVector)
                                  .setTimestamp(0)
                                  .build();

  EXPECT_CALL(
      *rawCongestionController,
      onPacketAck(
          flowId,
          AllOf(
              Field(
                  &CongestionController::AckEvent::numAckedPackets,
                  Eq(static_cast<uint16_t>(2))),
              Field(
                  &CongestionController::AckEvent::numAckedPacketsWithCE,
                  Eq(static_cast<uint16_t>(0))),
              // smoothedQpRttNs is the estRttNs before updateRtt()
              Field(
                  &CongestionController::AckEvent::smoothedQpRttNs,
                  Eq(TxFlow::kInitialEstRttNs)),
              // rcnDesiredRateGbps defaults to kDefaultRateHintGbps (>=
              // kMinRateHintGbps)
              Field(
                  &CongestionController::AckEvent::rcnDesiredRateGbps,
                  Ge(kMinRateHintGbps)))))
      .Times(1);
  EXPECT_CALL(
      *rawCongestionController,
      onPacketLoss(
          flowId,
          CongestionController::LossEvent{
              .numLostPackets = 3,
              .lossType =
                  CongestionController::LossEvent::LossType::OutOfOrder}))
      .Times(1);

  flow->processAckPacket(ackHeader);

  EXPECT_EQ(txState.sndUna, (uint16_t)(1))
      << "tx state Una should be 1 because cumulative acked 0.";
  EXPECT_GE(*txState.sndCur.rlock(), (uint16_t)(2))
      << "tx state should advance Cur to at least 2, retransmitting.";
  EXPECT_EQ(txState.maxAckdFsn, (uint16_t)(4))
      << "tx state should advance maxAckdFsn to 4.";
  EXPECT_EQ(txState.tracker.size(), (uint16_t)(4))
      << "tx state tracker window should decrease to 4.";

  // The hole (fsns 1,2,3) should be queued for retransmission. Pump them out
  // on the test thread; the window is 0, so only retransmissions are produced.
  std::set<uint16_t> retransmitted;
  for (int i = 0; i < frames && retransmitted.size() < 3; ++i) {
    auto egressPacket = qp->getNextPacketToSend();
    if (!egressPacket || !egressPacket->packet) {
      break;
    }
    auto* dataPacket = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    ASSERT_NE(dataPacket, nullptr)
        << "a retransmission should be a data packet";
    const auto& dataHeader = dataPacket->getDataHeader();
    EXPECT_TRUE(dataHeader.getIsRetransmit())
        << "each packet should be a marked retransmission.";
    EXPECT_FALSE(dataHeader.getIsTimeout())
        << "no packet should be a retransmission by timeout.";
    retransmitted.insert(dataHeader.getFsn());
  }
  EXPECT_EQ(retransmitted, (std::set<uint16_t>{1, 2, 3}))
      << "the SACK holes (fsns 1, 2, 3) should be retransmitted.";

  qp = nullptr;
  sender = nullptr;
}

/* Walk through the transmission and reception of N x 4KB frames, where ACKs
 * are synthesized. This is meant to confirm that all the packets are sent and
 * no sequence numbers (e.g., -1) are accidentally made special cases.
 */
TEST_P(SenderTest, SendNFrames) {
  const int frames = GetParam();
  const auto mtu = 4096;
  const auto flowId = 0;

  qp->setMtu(mtu);

  // Keep all TxFlow state on the test thread: no send loop, and the transmit
  // timers pinned so the retransmission timer never fires mid-transfer.
  ScopedSuppressedTxTimers suppressTimers;

  ON_CALL(*rawCongestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(1));
  EXPECT_CALL(*rawCongestionController, onPacketSent(flowId)).Times(frames);
  // unclear how many acks we'll get.
  EXPECT_CALL(*rawCongestionController, onPacketAck(flowId, _))
      .Times(AnyNumber());
  EXPECT_CALL(*rawCongestionController, onFlowAdded(flowId)).Times(1);
  EXPECT_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .Times(AnyNumber());

  auto sender = createTestSenderWithFlows(
      mockUDPSocketMaker, LimitedTestSender::kDisableSendLoop);

  // qp->postWqe will set the first send wqe sequence numbers based on
  // these initial sequence number values.
  const uint16_t msn = 2;
  const uint16_t csn = 1;
  ASSERT_TRUE(qp->getTag(kMetaRoceInitialTag)->setNextMsn(msn))
      << "precondition: the send must start from the test sequence numbers";
  ASSERT_TRUE(qp->getTag(kMetaRoceInitialTag)->setNextCsn(csn))
      << "precondition: the send must start from the test sequence numbers";
  auto flow = qp->getTxFlowById(flowId);

  const uint32_t length = frames * qp->getMtu();
  std::unique_ptr<char[]> payload(new char[length]);
  auto wqe = std::make_unique<SendWqe>(
      0 /*workReqId*/,
      oneSge(
          payload.get(), /*address*/
          length),
      mtu);
  auto& txState = flow->getTxState();
  ASSERT_TRUE(qp->postWqe(kMetaRoceInitialTag, std::move(wqe)));
  EXPECT_EQ(txState.sndUna, 0) << "new txstate should have fsn 0 unacked";
  EXPECT_EQ(txState.maxAckdFsn, UINT16_MAX) << "max acked should be -1";

  qp->startProcessingWqes();
  ASSERT_TRUE(qp->waitForWqesToBePosted(1));

  // Pump each packet out and ack the fsn just produced, all on the test thread.
  uint16_t lastAckFsn = 0xffff;
  for (int sent = 0; sent < frames; ++sent) {
    auto egressPacket = qp->getNextPacketToSend();
    ASSERT_TRUE(egressPacket && egressPacket->packet)
        << "expected packet " << sent + 1 << " of " << frames;
    auto* packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    ASSERT_NE(packet, nullptr) << "sent packet should be a data packet";
    const auto& baseHeader = packet->getBaseHeader();
    EXPECT_EQ(baseHeader.getDstQp(), 100) << "Sent packet validation failure";
    EXPECT_EQ(baseHeader.getSrcQp(), 10) << "Sent packet validation failure";
    const auto& dataHeader = packet->getDataHeader();
    EXPECT_EQ(dataHeader.getMsn(), msn) << "Sent packet validation failure";
    EXPECT_EQ(dataHeader.getCsn(), csn) << "Sent packet validation failure";

    const auto fsn = dataHeader.getFsn();
    const auto timestamp = dataHeader.getTimestamp();
    const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                    .setCumulativeFsn(fsn)
                                    .setMaxRcvdFsn(fsn)
                                    .setAckStatus(AckStatus::ACK)
                                    .setTimestamp(timestamp)
                                    .build();
    const auto nowNs = getNowNs();
    flow->processAckPacket(ackHeader, nowNs);
    lastAckFsn = fsn;

    auto rttNs = flow->computeRttNs(timestamp, nowNs);
    if (rttNs < 64 * (txState.estRttNs + txState.mdevRttNs)) {
      EXPECT_TRUE(after(txState.sndUna, fsn))
          << "tx state should accept ack " << fsn << " and advance Una "
          << txState.sndUna;
    }
  }
  EXPECT_EQ(lastAckFsn, static_cast<uint16_t>(frames - 1))
      << "all " << frames << " frames should have been produced and acked";

  qp = nullptr;
  sender = nullptr;
}

INSTANTIATE_TEST_SUITE_P(
    SenderTestSendNFramesSuite,
    SenderTest,
    Values(
        1,
        2,
        10,
        100,
        1024,
        16384,
        32768,
        65536 // max message size of 256MB
        ));

TEST_F(SenderTest, SendLoopWithDropModifier) {
  const auto mockUDPSocketMakerWModifier = [](folly::EventBase* evb) {
    // modification_times = 1000 to ensure all the retx will be dropped as well
    const auto jsonStr = R"({
        "name": "test_modifier",
        "type": "drop",
        "filter": {
            "direction": "tx",
            "packet_type": "data",
            "fields": {"fsn": 2, "path_id": 0, "dst_qp": 100}
        },
        "modification_times": 1000
    })";

    auto modifierConfig =
        ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
    auto modifier = std::make_unique<DropModifier>(std::move(modifierConfig));
    return std::make_pair(
        std::make_unique<NiceMock<MockUDPSocket>>(evb), std::move(modifier));
  };

  auto sender = createTestSenderWithFlows(mockUDPSocketMakerWModifier);
  auto socket = getTestSocket(sender.get());

  // create a path
  const auto flowId = 0;

  ON_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .WillByDefault(Return(1));

  const int frames = 11;
  auto wqePayload = createAndPostTestWqe(frames);
  std::mutex mtx;
  std::condition_variable cv;
  folly::F14FastSet<uint16_t> sentFsns;

  EXPECT_CALL(*socket, write(_, _))
      .Times(AtLeast(10))
      .WillRepeatedly(
          [&](const folly::SocketAddress&,
              const std::unique_ptr<folly::IOBuf>& buf) {
            folly::io::Cursor cursor(buf.get());

            BaseHeader baseHeader(cursor);

            DataHeader dataHeader(cursor);
            const auto fsn = dataHeader.getFsn();
            EXPECT_NE(fsn, 2)
                << "Packet with fsn=2 has to be dropped by the modifier";
            {
              std::lock_guard<std::mutex> lock(mtx);
              sentFsns.insert(fsn);
              cv.notify_one();
            }
            return buf.get()->computeChainDataLength();
          });

  qp->startProcessingWqes();

  // Poll until WQEs have been processed into Tags.
  ASSERT_TRUE(qp->waitForWqesToBePosted(1));

  {
    std::unique_lock<std::mutex> lock(mtx);
    while (sentFsns.size() != 10) { // 11 fsns except fsn = 2
      cv.wait(lock, [&sentFsns]() { return sentFsns.size() == 10; });
    }
  }

  sender = nullptr;
}

std::string sequenceToString(std::deque<uint16_t> dq) {
  std::string ret;
  for (auto it = dq.begin(); it != dq.end(); ++it) {
    if (it != dq.begin()) {
      ret += " ";
    }
    ret += fmt::format("{}", *it);
  }
  return ret;
}
/*
 * This tests sends 10 packets on a single path and use reorder modifier to
 * delay sending packet with fsn=3. This tests verifies that fsn=4 is sent right
 * after fsn=2 and fsn=3 is eventually sent.
 */
TEST_F(SenderTest, SendLoopWithTimeBasedReorderModifier) {
  const int kExpectedDelayUs = 3000; // update in value below.
  const auto mockUDPSocketMakerWModifier = [](folly::EventBase* evb) {
    // modification_times = 5; retx would be delayed as well.
    // delay is 3ms since few-ms delays are possible
    // in stress testing
    const auto jsonStr = R"({
        "name": "test_modifier",
        "type": "reorder",
        "filter": {
            "direction": "tx",
            "packet_type": "data",
            "fields": {"fsn": 3, "path_id": 0, "dst_qp": 100}
        },
        "modification_times": 5,
        "distance": {
            "unit": "time_us",
            "value": 3000
        }
    })";

    auto modifierConfig =
        ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
    auto modifier =
        std::make_unique<ReorderModifier>(std::move(modifierConfig));
    return std::make_pair(
        std::make_unique<NiceMock<MockUDPSocket>>(evb), std::move(modifier));
  };

  auto sender = createTestSenderWithFlows(mockUDPSocketMakerWModifier);
  auto socket = getTestSocket(sender.get());

  // create a path
  const auto flowId = 0;
  ON_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .WillByDefault(Return(1));

  const int frames = 10;
  auto wqePayload = createAndPostTestWqe(frames);

  std::mutex mtx;
  std::condition_variable cv;
  folly::F14FastSet<uint16_t> sentFsns;
  std::deque<uint16_t> actualSendOrderFsns = {};

  // The test will confirm that delayed packet 3 doesn't appear
  // before start of experiment plus delay in microseconds.
  std::chrono::microseconds startUs;

  uint16_t lastSentFsn = static_cast<uint16_t>(-1);
  EXPECT_CALL(*socket, write(_, _))
      .Times(AtLeast(10))
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());

        BaseHeader baseHeader(cursor);

        DataHeader dataHeader(cursor);

        const auto fsn = dataHeader.getFsn();

        std::lock_guard<std::mutex> lock(mtx);
        actualSendOrderFsns.push_back(fsn);

        if (fsn == 3) {
          // confirm that fsn 3 was sent at least delay after start.
          const auto nowUs =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch());
          const auto delayUs = (nowUs - startUs).count();
          // 0.9x tolerance for timer jitter
          EXPECT_GT(delayUs, 0.9 * kExpectedDelayUs)
              << " fsn 3 should not arrive before " << kExpectedDelayUs
              << "us after start; received ("
              << sequenceToString(actualSendOrderFsns) << ")";
        }
        sentFsns.insert(fsn);
        lastSentFsn = fsn;
        if (sentFsns.size() >= 10) {
          cv.notify_one();
        }
        return buf.get()->computeChainDataLength();
      });

  startUs = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::high_resolution_clock::now().time_since_epoch());

  qp->startProcessingWqes();

  // Poll until WQEs have been processed into Tags.
  ASSERT_TRUE(qp->waitForWqesToBePosted(1));

  {
    std::unique_lock<std::mutex> lock(mtx);
    while (sentFsns.size() != 10) {
      cv.wait(lock, [&sentFsns]() { return sentFsns.size() == 10; });
    }
  }

  // the LimitedTestSender in this test has a time based reorder modifier; the
  // timer must be destroyed or cancelled to prevent it from firing
  // after the test completes.
  sender = nullptr;
}

/*
 * This tests sends 10 packets on a single path and use reorder modifier to
 * delay sending packet with fsn=3 by 3 packets. Then it verifies that fsn=3
 * is sent right after fsn=6.
 */
TEST_F(SenderTest, SendLoopWithPacketBasedReorderModifier) {
  const auto mockUDPSocketMakerWModifier = [](folly::EventBase* evb) {
    const auto jsonStr = R"({
        "name": "test_modifier",
        "type": "reorder",
        "filter": {
            "direction": "tx",
            "packet_type": "data",
            "fields": {"fsn": 3, "path_id": 0, "dst_qp": 100}
        },
        "modification_times": 5,
        "distance": {
            "unit": "packet",
            "value": 3
        }
    })";

    auto modifierConfig =
        ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
    auto modifier =
        std::make_unique<ReorderModifier>(std::move(modifierConfig));
    return std::make_pair(
        std::make_unique<NiceMock<MockUDPSocket>>(evb), std::move(modifier));
  };
  auto sender = createTestSenderWithFlows(mockUDPSocketMakerWModifier);
  auto socket = getTestSocket(sender.get());

  // create a path
  const auto flowId = 0;
  ON_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .WillByDefault(Return(1));

  const int frames = 10;
  auto wqePayload = createAndPostTestWqe(frames);

  std::mutex mtx;
  std::condition_variable cv;
  std::deque<uint16_t> expectedSendOrderFsns = {0, 1, 2, 4, 5, 6, 3, 7, 8, 9};
  std::deque<uint16_t> actualSendOrderFsns = {};
  folly::F14FastSet<uint16_t> sentFsns;

  EXPECT_CALL(*socket, write(_, _))
      .Times(AtLeast(10))
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());

        BaseHeader baseHeader(cursor);

        DataHeader dataHeader(cursor);
        const auto fsn = dataHeader.getFsn();

        {
          std::lock_guard<std::mutex> lock(mtx);

          actualSendOrderFsns.push_back(fsn);
          // ignore retransmissions when comparing order to expected.
          if (!sentFsns.contains(fsn)) {
            sentFsns.insert(fsn);
            if (expectedSendOrderFsns.size() > 0) {
              EXPECT_EQ(fsn, expectedSendOrderFsns.front())
                  << "Actual order: (" << sequenceToString(actualSendOrderFsns)
                  << ") Remaining expected: ("
                  << sequenceToString(expectedSendOrderFsns) << ")";
              ;
              expectedSendOrderFsns.pop_front();
            }
          } else {
            EXPECT_TRUE(dataHeader.getIsRetransmit())
                << "we have already seen fsn " << fsn
                << " but the packet was not marked as a retransmission";
          }
          cv.notify_one();
        }
        return buf.get()->computeChainDataLength();
      });

  // start wqe processing only after the mock is in place.
  qp->startProcessingWqes();

  // Poll until WQEs have been processed into Tags.
  ASSERT_TRUE(qp->waitForWqesToBePosted(1));

  {
    std::unique_lock<std::mutex> lock(mtx);
    while (expectedSendOrderFsns.size() != 0) {
      cv.wait(lock, [&expectedSendOrderFsns]() {
        return expectedSendOrderFsns.size() == 0;
      });
    }
  }
  sender = nullptr;
}

/*
 * This test sends 10 packets on a single path and uses duplicate modifier to
 * duplicate packets with a delay. It verifies that the original packets are
 * sent immediately and the duplicates are sent after the delay.
 */
TEST_F(SenderTest, SendLoopWithDuplicateModifier) {
  const int kExpectedDelayUs = 3000;
  const auto mockUDPSocketMakerWModifier = [](folly::EventBase* evb) {
    const auto jsonStr = R"({
        "name": "test_modifier",
        "type": "duplicate",
        "filter": {
            "direction": "tx",
            "packet_type": "data",
            "fields": {"fsn": 3, "path_id": 0, "dst_qp": 100}
        },
        "modification_times": 1,
        "delay_us": 3000
    })";

    auto modifierConfig =
        ModifierConfig::buildModifierConfigFromJson(folly::parseJson(jsonStr));
    auto modifier =
        std::make_unique<DuplicateModifier>(std::move(modifierConfig));
    return std::make_pair(
        std::make_unique<NiceMock<MockUDPSocket>>(evb), std::move(modifier));
  };

  auto sender = createTestSenderWithFlows(mockUDPSocketMakerWModifier);
  auto socket = getTestSocket(sender.get());

  // create a path
  const auto flowId = 0;
  ON_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .WillByDefault(Return(1));

  const int frames = 10;
  auto wqePayload = createAndPostTestWqe(frames);

  std::mutex mtx;
  std::condition_variable cv;
  folly::F14FastSet<uint16_t> sentFsns;
  std::map<uint16_t, std::vector<std::chrono::microseconds>> fsnTimestamps;

  EXPECT_CALL(*socket, write(_, _))
      .Times(
          AtLeast(11)) // At least 10 original packets + 1 duplicate for fsn=3
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());

        BaseHeader baseHeader(cursor);

        DataHeader dataHeader(cursor);

        const auto fsn = dataHeader.getFsn();
        const auto nowUs =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch());

        std::lock_guard<std::mutex> lock(mtx);
        sentFsns.insert(fsn);
        fsnTimestamps[fsn].push_back(nowUs);

        // If we've seen fsn=3 twice (original + duplicate)
        if (fsn == 3 && fsnTimestamps[fsn].size() == 2) {
          // Verify the duplicate was sent after the delay
          const auto delayUs =
              (fsnTimestamps[fsn][1] - fsnTimestamps[fsn][0]).count();
          EXPECT_GT(
              delayUs,
              0.9 * kExpectedDelayUs) // Reduced from 0.9 to 0.35
              << "Duplicate packet for fsn=3 should arrive at least "
              << (0.9 * kExpectedDelayUs) << "us after the original";
        }
        if (fsnTimestamps[3].size() == 2 && fsnTimestamps.size() == 10) {
          // we're done, both a duplicate for fsn=3 has been seen, and
          // all the packets have arrived.
          cv.notify_one();
        }
        return buf.get()->computeChainDataLength();
      });

  qp->startProcessingWqes();

  // Poll until WQEs have been processed into Tags.
  ASSERT_TRUE(qp->waitForWqesToBePosted(1));

  {
    std::unique_lock<std::mutex> lock(mtx);
    // Wait until we've seen the duplicate for fsn=3
    cv.wait_for(
        lock,
        std::chrono::milliseconds(kExpectedDelayUs * 10 / 1000),
        [&fsnTimestamps]() { return fsnTimestamps[3].size() >= 2; });

    // Verify we got the duplicate
    EXPECT_GE(fsnTimestamps[3].size(), 2)
        << "Expected to see fsn=3 at least twice (original + duplicate)";
  }

  // The LimitedTestSender in this test has a duplicate modifier with a timer;
  // the timer must be destroyed or cancelled to prevent it from firing
  // after the test completes.
  sender = nullptr;
}

/*
 * Tests that getNextSocketIdToAssignFromAddrs() properly cycles through local
 * addresses for socket creation when creating multiple sockets.
 */
TEST_F(SenderTest, MultipleAddressesRoundRobinSocketCreation) {
  const folly::IPAddress addr1("fc00::10");
  const folly::IPAddress addr2("fc00::20");
  const folly::IPAddress addr3("fc00::30");

  std::vector<folly::IPAddress> localAddrs = {addr1, addr2, addr3};

  // Create a custom socket maker that tracks which addresses are used
  struct TestSocketMaker {
    std::vector<folly::IPAddress> usedAddresses;

    std::pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    operator()(folly::EventBase* evb) {
      return std::make_pair(
          std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
    }
  };

  TestSocketMaker testSocketMaker;

  // Use a lambda that captures testSocketMaker to track socket creation
  auto socketMakerWithTracking = [&testSocketMaker](folly::EventBase* evb) {
    return testSocketMaker(evb);
  };

  LimitedTestSender sender(
      arbiter,
      socketMakerWithTracking,
      kMetaRoceSendToPort,
      LimitedTestSender::kEnableSendLoop);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);

  // Create several sockets
  std::vector<uint16_t> socketIds;
  for (int i = 0; i < 2 * Sender::kMaxNumSockets; i++) {
    auto socketId =
        sender.getSocketIdFromAddress(localAddrs[i % localAddrs.size()]);
    socketIds.push_back(socketId);
  }

  // Verify all socket IDs are unique until we hit the max
  std::set<uint16_t> uniqueSocketIds(socketIds.begin(), socketIds.end());
  EXPECT_EQ(uniqueSocketIds.size(), static_cast<int>(Sender::kMaxNumSockets));
}

/*
 * Tests that when socket limit is reached, getNextSocketIdToAssignFromAddrs()
 * returns a socket bound to one of the provided addresses.
 */
TEST_F(SenderTest, SocketLimitReturnsBoundAddress) {
  const folly::IPAddress addr1("fc00::10");
  const folly::IPAddress addr2("fc00::20");
  const folly::IPAddress addr3("fc00::30");
  const folly::IPAddress addr4("fc00::40");

  std::vector<folly::IPAddress> initialAddrs = {addr1, addr2, addr3};
  std::vector<folly::IPAddress> newAddrs = {
      addr2, addr4}; // addr2 overlaps, addr4 is new

  LimitedTestSender sender(
      arbiter,
      mockUDPSocketMaker,
      kMetaRoceSendToPort,
      LimitedTestSender::kEnableSendLoop);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);

  // First, create enough sockets to reach the limit using addr1, addr2, addr3
  std::vector<uint16_t> initialSocketIds;
  for (int i = 0; i < Sender::kMaxNumSockets; i++) {
    auto socketId =
        sender.getSocketIdFromAddress(initialAddrs[i % initialAddrs.size()]);
    initialSocketIds.push_back(socketId);
  }

  // Verify we created exactly kMaxNumSockets unique sockets
  std::set<uint16_t> uniqueInitialIds(
      initialSocketIds.begin(), initialSocketIds.end());
  EXPECT_EQ(uniqueInitialIds.size(), static_cast<int>(Sender::kMaxNumSockets));

  // Now when we request a socket with newAddrs (containing addr2 and addr4),
  // it should return a socket bound to addr2 (since addr2 was used in initial
  // creation)
  auto socketIdForNewAddrs0 = sender.getSocketIdFromAddress(newAddrs[0]);

  // The returned socket ID should be one of the existing sockets (<
  // kMaxNumSockets)
  EXPECT_LT(socketIdForNewAddrs0, Sender::kMaxNumSockets);

  // Attempting to create another socket with a new address should throw an
  // exception
  EXPECT_THROW(sender.getSocketIdFromAddress(newAddrs[1]), std::runtime_error);
}

/*
 * Test EAGAIN handling.  Start a wqe with 2 frames; drop the first
 * due to EAGAIN.  Expect transmission after rto.  When RTO fires in
 * fast start, it may not yield a retransmission because we could have
 * sent the next frame.  It might be reasonable to leave fast start on
 * EAGAIN, so this test is not picky about whether first or second
 * packet is sent.
 */
TEST_F(SenderTest, SendEagain) {
  // The slow-receiver default seeds mdev at 250ms, making the first RTO a
  // second; this test waits for a retransmission, not for that RTT model.
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_slow_receiver = false;
  auto sender = createTestSenderWithFlows();
  auto socket = getTestSocket(sender.get());

  // create a path
  const auto flowId = 0;
  ON_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .WillByDefault(Return(1));

  std::mutex mtx;
  std::condition_variable cv;
  folly::F14FastSet<uint16_t> sentFsns;

  EXPECT_CALL(*socket, write(_, _))
      .Times(AtLeast(2))
      .WillOnce([&](const folly::SocketAddress&,
                    const std::unique_ptr<folly::IOBuf>&) {
        LOG(INFO) << "returning with EAGAIN";
        errno = EAGAIN;
        return -1;
      })
      .WillRepeatedly([&](const folly::SocketAddress&,
                          const std::unique_ptr<folly::IOBuf>& buf) {
        folly::io::Cursor cursor(buf.get());

        BaseHeader baseHeader(cursor);

        DataHeader dataHeader(cursor);

        const auto fsn = dataHeader.getFsn();
        LOG(INFO) << "writing success fsn:" << fsn;
        {
          std::lock_guard<std::mutex> lock(mtx);
          sentFsns.insert(fsn);
          cv.notify_one();
        }
        return buf.get()->computeChainDataLength();
      });

  const int frames = 2;
  auto wqePayload = createAndPostTestWqe(frames);
  // Stops the send loop before the state its callbacks touch is destroyed.
  SCOPE_EXIT {
    sender = nullptr;
  };

  qp->startProcessingWqes();

  // busy loop until wqe is processed from wqQueue to sendWqeQueue.
  while (qp->getNumPendingWqes() == 0) {
  }

  const int kRunForSeconds = 20;
  auto start = std::chrono::steady_clock::now();
  {
    std::unique_lock<std::mutex> lock(mtx);
    while (std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::steady_clock::now() - start)
                   .count() < kRunForSeconds &&
           sentFsns.size() == 0) {
      cv.wait_for(lock, std::chrono::seconds(kRunForSeconds), [&sentFsns]() {
        return sentFsns.size() >= 1;
      });
    }
  }
}

/*
 * Test EAGAIN handling.  Start a wqe with 2 frames; drop the second
 * due to EAGAIN.  Expect transmission after rto.  To make rto retransmit
 * the second frame, we synthesize an ack for the first.
 */
TEST_F(SenderTest, SendEagainSecond) {
  // The slow-receiver default seeds mdev at 250ms, making the first RTO a
  // second; this test waits for a retransmission, not for that RTT model.
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_slow_receiver = false;
  auto sender = createTestSenderWithFlows();
  auto socket = getTestSocket(sender.get());

  // create a path
  const auto flowId = 0;
  ON_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .WillByDefault(Return(1));
  auto flow = qp->getTxFlowById(flowId);

  std::mutex mtx;
  std::condition_variable cv;
  folly::F14FastSet<uint16_t> sentFsns;

  auto deliver = [&](const folly::SocketAddress&,
                     const std::unique_ptr<folly::IOBuf>& buf) {
    folly::io::Cursor cursor(buf.get());

    BaseHeader baseHeader(cursor);

    DataHeader dataHeader(cursor);

    const auto fsn = dataHeader.getFsn();

    // need to ack 0 to prevent getting stuck retransmitting zero.
    if (fsn == 0) {
      const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                      .setCumulativeFsn(0)
                                      .setMaxRcvdFsn(0)
                                      .setAckStatus(AckStatus::ACK)
                                      .setTimestamp(dataHeader.getTimestamp())
                                      .build();
      flow->processAckPacket(ackHeader);
    }
    LOG(INFO) << "writing success fsn:" << fsn;
    {
      std::lock_guard<std::mutex> lock(mtx);
      sentFsns.insert(fsn);
      cv.notify_one();
    }
    return buf.get()->computeChainDataLength();
  };

  EXPECT_CALL(*socket, write(_, _))
      // fsn=0 with success, fsn=1 with eagain, fsn=1 with success.
      .Times(AtLeast(3))
      .WillOnce(deliver)
      .WillOnce([&](const folly::SocketAddress&,
                    const std::unique_ptr<folly::IOBuf>&) {
        LOG(INFO) << "returning with EAGAIN";
        errno = EAGAIN;
        return -1;
      })
      .WillRepeatedly(deliver);

  const int frames = 2;
  auto wqePayload = createAndPostTestWqe(frames);
  // Stops the send loop before the state its callbacks touch is destroyed.
  SCOPE_EXIT {
    sender = nullptr;
  };

  qp->startProcessingWqes();

  // busy loop until wqe is processed from wqQueue to sendWqeQueue.
  while (qp->getNumPendingWqes() == 0) {
  }

  const int kRunForSeconds = 20;
  auto start = std::chrono::steady_clock::now();
  {
    std::unique_lock<std::mutex> lock(mtx);
    while (std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::steady_clock::now() - start)
                   .count() < kRunForSeconds &&
           sentFsns.size() < 2) {
      cv.wait_for(lock, std::chrono::seconds(kRunForSeconds), [&sentFsns]() {
        return sentFsns.size() >= 2;
      });
    }
  }
}

/*
 * Test that processAckPacket accepts a one-second-old ACK when
 * FLAGS_libsoftmetaroce_slow is set to true.
 */
TEST_F(SenderTest, ProcessAckPacketAcceptsOldAckWhenSlowFlagIsTrue) {
  bool originalSlowFlag = FLAGS_libsoftmetaroce_slow_receiver;
  FLAGS_libsoftmetaroce_slow_receiver = true;

  // Pure processAckPacket unit test: no send loop, so the test thread owns all
  // TxFlow state.
  auto sender = createTestSenderWithFlows(
      mockUDPSocketMaker, LimitedTestSender::kDisableSendLoop);

  const auto flowId = 0;
  auto flow = qp->getTxFlowById(flowId);

  // Verify that with FLAGS_libsoftmetaroce_slow set and before any packets
  // are sent or ACKs received, computeRtoIntervalUs returns 1-2 seconds
  uint32_t rtoUs = flow->computeRtoIntervalUs();
  EXPECT_GE(rtoUs, 1'000'000)
      << "RTO should be at least 1 second with slow flag";
  EXPECT_LE(rtoUs, 2'000'000)
      << "RTO should be at most 2 seconds with slow flag";

  const auto mtu = qp->getMtu();

  const int frames = 5;
  const uint32_t length = frames * mtu;
  std::unique_ptr<char[]> payload(new char[length]);
  auto wqe = std::make_unique<SendWqe>(
      0 /* workReqId */,
      oneSge(
          payload.get(), /*address*/
          length),
      mtu);

  wqe->setMsn(0);
  wqe->setCsn(0);
  auto& txState = flow->getTxState();

  setupTxStateWithSentPackets(flow, wqe.get(), frames, mtu);
  txState.sndUna = 0;

  // Create an ACK with a timestamp from 1 second ago
  const uint32_t nowNs = getNowNs();
  const uint32_t oldTimestampNs = nowNs - 1'000'000'000;

  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setTag(kMetaRoceInitialTag)
                                  .setCumulativeFsn(frames - 1)
                                  .setMaxRcvdFsn(frames - 1)
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(oldTimestampNs)
                                  .build();

  // Process the ACK with FLAGS_libsoftmetaroce_slow = true
  flow->processAckPacket(ackHeader);

  EXPECT_EQ(txState.sndUna, frames)
      << "sndUna should advance to 5 because all 5 packets (fsn 0-4) should be "
         "ACKed when FLAGS_libsoftmetaroce_slow is true";

  EXPECT_EQ(txState.tracker.size(), 0)
      << "tracker should be empty after all packets are ACKed";

  FLAGS_libsoftmetaroce_slow_receiver = originalSlowFlag;

  qp = nullptr;
  sender = nullptr;
}

/*
 * Test that processAckPacket ignores a one-second-old ACK when
 * FLAGS_libsoftmetaroce_slow_receiver is set to false.
 */
TEST_F(SenderTest, ProcessAckPacketIgnoresOldAckWhenSlowFlagIsFalse) {
  bool originalSlowFlag = FLAGS_libsoftmetaroce_slow_receiver;
  FLAGS_libsoftmetaroce_slow_receiver = false;

  // Pure processAckPacket unit test: no send loop, so the test thread owns all
  // TxFlow state.
  auto sender = createTestSenderWithFlows(
      mockUDPSocketMaker, LimitedTestSender::kDisableSendLoop);

  const auto flowId = 0;
  auto flow = qp->getTxFlowById(flowId);
  const auto mtu = qp->getMtu();

  const int frames = 5;
  const uint32_t length = frames * mtu;
  std::unique_ptr<char[]> payload(new char[length]);
  auto wqe = std::make_unique<SendWqe>(
      0 /* workReqId */,
      oneSge(
          payload.get(), /*address*/
          length),
      mtu);

  wqe->setMsn(0);
  wqe->setCsn(0);
  auto& txState = flow->getTxState();

  setupTxStateWithSentPackets(flow, wqe.get(), frames, mtu);
  txState.sndUna = 0;

  // Create an ACK with a timestamp from 1 second ago
  const uint32_t nowNs = getNowNs();
  const uint32_t oldTimestampNs = nowNs - 1'000'000'000;

  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setTag(kMetaRoceInitialTag)
                                  .setCumulativeFsn(4)
                                  .setMaxRcvdFsn(4)
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(oldTimestampNs)
                                  .build();

  // Process the ACK with FLAGS_libsoftmetaroce_slow = false
  flow->processAckPacket(ackHeader);

  EXPECT_EQ(txState.sndUna, 0)
      << "sndUna should remain 0 because the ACK should be ignored when "
         "FLAGS_libsoftmetaroce_slow is false";

  EXPECT_EQ(txState.tracker.size(), frames)
      << "tracker should still contain all 5 packets when ACK is ignored";

  FLAGS_libsoftmetaroce_slow_receiver = originalSlowFlag;

  qp = nullptr;
  sender = nullptr;
}

// Each test starts with kFrames frames in flight, slow-receiver mode off, and
// no RTT sample.
class StaleAckTest : public SenderTest {
 protected:
  static constexpr uint16_t kFrames = 5;

  void SetUp() override {
    SenderTest::SetUp();
    FLAGS_libsoftmetaroce_slow_receiver = false;
    FLAGS_libsoftmetaroce_rfc6298_first_rtt_sample = true;
    sender_ = createTestSenderWithFlows(
        mockUDPSocketMaker, LimitedTestSender::kDisableSendLoop);
    flow_ = qp->getTxFlowById(0);
    ASSERT_NE(flow_, nullptr);
    const auto mtu = qp->getMtu();
    payload_ = std::make_unique<char[]>(kFrames * mtu);
    wqe_ = std::make_unique<SendWqe>(
        0 /* workReqId */, oneSge(payload_.get(), kFrames * mtu), mtu);
    wqe_->setMsn(0);
    wqe_->setCsn(0);
    setupTxStateWithSentPackets(flow_, wqe_.get(), kFrames, mtu);
  }

  // Processes an ack of every fsn through cumulativeFsn, echoing a timestamp
  // rttNs before now.
  void ackThrough(uint16_t cumulativeFsn, uint32_t rttNs) {
    const uint32_t nowNs = getNowNs();
    flow_->processAckPacket(
        AckHeader::AckHeaderBuilder()
            .setTag(kMetaRoceInitialTag)
            .setCumulativeFsn(cumulativeFsn)
            .setMaxRcvdFsn(cumulativeFsn)
            .setAckStatus(AckStatus::ACK)
            .setTimestamp(nowNs - rttNs)
            .build(),
        nowNs);
  }

  static uint64_t maxRtoLimitNs() {
    return uint64_t{4} * FLAGS_libsoftmetaroce_max_rto_us * kNsPerUs;
  }

  uint64_t rttLimitNs() {
    const auto& txState = flow_->getTxState();
    return (uint64_t{txState.estRttNs} + txState.mdevRttNs) *
        kStaleAckRttMultiplier;
  }

  std::string estimatorState() {
    const auto& txState = flow_->getTxState();
    return fmt::format(
        "srtt {}ns, mdev {}ns, haveRttSample {}, sndUna {}",
        txState.estRttNs,
        txState.mdevRttNs,
        txState.haveRttSample,
        txState.sndUna);
  }

  gflags::FlagSaver flagSaver_;
  std::unique_ptr<LimitedTestSender> sender_;
  TxFlow* flow_{nullptr};
  std::unique_ptr<char[]> payload_;
  std::unique_ptr<SendWqe> wqe_;
};

TEST_F(StaleAckTest, FirstAckOlderThanInitialRttLimitSeedsTheEstimator) {
  constexpr uint32_t kRttNs = 300'000'000;
  ASSERT_GT(kRttNs, rttLimitNs())
      << "precondition: the RTT limit from the initial srtt and mdev, "
      << rttLimitNs() << "ns, is shorter than this ack's RTT";
  ASSERT_LT(kRttNs, maxRtoLimitNs())
      << "precondition: the max-RTO limit, " << maxRtoLimitNs()
      << "ns, accepts this ack";

  ackThrough(kFrames - 1, kRttNs);

  const auto& txState = flow_->getTxState();
  EXPECT_EQ(txState.sndUna, kFrames)
      << "a flow with no RTT sample has to accept an ack within the max-RTO "
         "limit; a discard leaves the initial srtt and mdev in place, and "
         "every later ack as old is discarded the same way; "
      << estimatorState();
  EXPECT_EQ(txState.estRttNs, kRttNs)
      << "the accepted ack's RTT has to reach the estimator, which on a first "
         "sample sets srtt to it (RFC 6298 2.2) so later acks are judged "
         "against the measured path; "
      << estimatorState();
  EXPECT_EQ(txState.mdevRttNs, kRttNs / 2)
      << "a first sample sets mdev to half the RTT (RFC 6298 2.2); an initial "
         "mdev left in place keeps the RTT limit partly configured; "
      << estimatorState();
}

TEST_F(StaleAckTest, FirstAckOlderThanMaxRtoLimitIsDiscarded) {
  const auto rttNs = static_cast<uint32_t>(maxRtoLimitNs() + 100'000'000);

  ackThrough(kFrames - 1, rttNs);

  const auto& txState = flow_->getTxState();
  EXPECT_EQ(txState.sndUna, 0)
      << "an ack older than 4 * max RTO may report an fsn from before a wrap, "
         "so the max-RTO limit discards it whether or not the flow has an RTT "
         "sample; "
      << estimatorState();
  EXPECT_FALSE(txState.haveRttSample)
      << "a discarded ack is dropped whole, including its RTT, which must not "
         "become the flow's first sample; "
      << estimatorState();
}

TEST_F(StaleAckTest, AckOlderThanMeasuredRttLimitIsDiscarded) {
  ackThrough(1, 100'000 /* rttNs */);
  const auto& txState = flow_->getTxState();
  ASSERT_TRUE(txState.haveRttSample)
      << "precondition: the first ack gives the flow an RTT sample; "
      << estimatorState();
  const uint16_t sndUnaAfterSample = txState.sndUna;

  constexpr uint32_t kRttNs = 50'000'000;
  ASSERT_GT(kRttNs, rttLimitNs())
      << "precondition: the RTT limit from the sample, " << rttLimitNs()
      << "ns, is shorter than this ack's RTT";
  ASSERT_LT(kRttNs, maxRtoLimitNs())
      << "precondition: the max-RTO limit, " << maxRtoLimitNs()
      << "ns, accepts this ack, so only the RTT limit can discard it";

  ackThrough(kFrames - 1, kRttNs);

  EXPECT_EQ(txState.sndUna, sndUnaAfterSample)
      << "once the flow has an RTT sample, the RTT limit is the tighter of the "
         "two and has to discard an ack far older than the measured round "
         "trip; "
      << estimatorState();
}

/*
 * A 1 s max RTO puts the max-RTO limit at 4 s, past the 2^32 ns that srtt +
 * mdev reaches after a first sample of about 2.86 s.
 */
class StaleAckLongMaxRtoTest : public StaleAckTest {
 protected:
  void SetUp() override {
    FLAGS_libsoftmetaroce_max_rto_us = 1'000'000;
    StaleAckTest::SetUp();
  }
};

TEST_F(StaleAckLongMaxRtoTest, RttLimitHoldsWhenSrttPlusMdevPassesUint32) {
  // srtt + mdev after this sample is R + R / 2 = 2^32 ns.
  constexpr uint32_t kFirstRttNs = 2'863'311'531;
  ASSERT_LT(kFirstRttNs, maxRtoLimitNs())
      << "precondition: the max-RTO limit, " << maxRtoLimitNs()
      << "ns, accepts the first ack";
  ackThrough(1, kFirstRttNs);
  const auto& txState = flow_->getTxState();
  ASSERT_GT(uint64_t{txState.estRttNs} + txState.mdevRttNs, UINT32_MAX)
      << "precondition: the first sample puts srtt + mdev past 32 bits; "
      << estimatorState();

  ackThrough(kFrames - 1, 100'000 /* rttNs */);

  EXPECT_EQ(txState.sndUna, kFrames)
      << "an ack far younger than the measured round trip has to be accepted; "
         "srtt + mdev summed in 32 bits wraps the RTT limit to zero, which "
         "discards every later ack; "
      << estimatorState();
}

/*
 * Repathing: a flow that keeps timing out without an ack moves to another
 * socket, so its retransmission hashes to a different network path.
 */

TEST_F(SenderTest, SendPathShift) {
  // The slow-receiver default seeds mdev at 250ms, making the first RTO a
  // second; this test waits for a retransmission, not for that RTT model.
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_slow_receiver = false;
  // Track whether we've seen the initial send and the retransmission
  std::mutex mtx;
  std::condition_variable cv;
  bool sawInitialSend = false;
  bool sawRetransmit = false;
  uint16_t initialSocketId = 0;
  uint16_t retransmitSocketId = 0;

  // Create a socket maker that sets up write expectations on all sockets
  Qp* qpPtr = qp.get();
  auto customSocketMaker = [&mtx,
                            &cv,
                            &sawInitialSend,
                            &sawRetransmit,
                            &initialSocketId,
                            &retransmitSocketId,
                            qpPtr](folly::EventBase* evb) {
    auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
    ON_CALL(*socket, write(_, _))
        .WillByDefault([&mtx,
                        &cv,
                        &sawInitialSend,
                        &sawRetransmit,
                        &initialSocketId,
                        &retransmitSocketId,
                        qpPtr](
                           const folly::SocketAddress&,
                           const std::unique_ptr<folly::IOBuf>& buf) {
          folly::io::Cursor cursor(buf.get());
          BaseHeader baseHeader(cursor);
          DataHeader dataHeader(cursor);

          std::lock_guard<std::mutex> lock(mtx);
          if (!sawInitialSend) {
            // First packet - record initial socket ID
            sawInitialSend = true;
            initialSocketId = qpPtr->getTxFlowById(0)->getSocketId();
            VLOG(2) << "Initial send on socketId: " << initialSocketId;
          } else if (
              dataHeader.getIsRetransmit() && dataHeader.getIsTimeout()) {
            // This is a timeout-based retransmission - repath should have
            // occurred
            sawRetransmit = true;
            retransmitSocketId = qpPtr->getTxFlowById(0)->getSocketId();
            VLOG(2) << "Retransmit on socketId: " << retransmitSocketId;
            cv.notify_one();
          }

          return buf.get()->computeChainDataLength();
        });
    return std::make_pair(std::move(socket), nullptr);
  };

  // Need multiple sockets to test repath behavior
  auto sender = std::make_unique<LimitedTestSender>(
      arbiter,
      customSocketMaker,
      kMetaRoceSendToPort,
      LimitedTestSender::kEnableSendLoop);
  sender->setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  qp->setupFlows(1, 1, sender.get());

  // ensure the repath threshold is one RTO
  auto old_libsoftmetaroce_rtos_before_repath =
      FLAGS_libsoftmetaroce_rtos_before_repath;
  FLAGS_libsoftmetaroce_rtos_before_repath = 1;

  constexpr uint16_t flowId = 0; // work with flowId zero
  ON_CALL(*rawCongestionController, getAvailableWindowSize(flowId))
      .WillByDefault(Return(1));

  // one frame is enough to get a RTO
  const int frames = 1;

  // create a Send WQE and post it
  const uint32_t length = frames * qp->getMtu();
  std::unique_ptr<char[]> payload(new char[length]);
  ASSERT_TRUE(qp->postWqe(
      kMetaRoceInitialTag,
      std::make_unique<SendWqe>(
          0 /* workReqId */,
          oneSge(
              payload.get(), /*address*/
              length),
          qp->getMtu())));

  qp->startProcessingWqes();
  ASSERT_TRUE(qp->waitForWqesToBePosted(1)); // waits up to 100ms

  // Wait for the retransmit packet (triggered by RTO)
  {
    std::unique_lock<std::mutex> lock(mtx);
    // Wait up to 5s for the RTO retransmission to occur
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&sawRetransmit] {
      return sawRetransmit;
    })) << "Did not see a timeout retransmission";
  }

  // cancel RTX timers
  const auto ntxflows = qp->getNumTxFlows();
  for (auto flownum = 0; flownum < ntxflows; flownum++) {
    auto txp = qp->getTxFlowById(flownum);
    txp->destroyRetransmissionTimer();
  }

  // verify socket on path changed after RTO (repath occurred)
  ASSERT_NE(initialSocketId, retransmitSocketId)
      << "Socket ID should have changed after RTO repath";

  sender = nullptr;

  // restore original flags
  FLAGS_libsoftmetaroce_rtos_before_repath =
      old_libsoftmetaroce_rtos_before_repath;
}

/*
 * Verifies that for a given logical socketId the data and ACK CoS sockets
 * are distinct underlying sockets. This is the structural invariant that
 * lets the sendLoop pick the correct CoS without per-packet setsockopt.
 */
TEST_F(SenderTest, PerCosSocketsAreDistinct) {
  auto sender = createTestSenderWithFlows();
  const auto socketId = sender->getSocketIdFromAddress(localAddr);
  const auto* const dataSock =
      sender->getSocketById(socketId, EgressPacket::Kind::Data);
  const auto* const ackSock =
      sender->getSocketById(socketId, EgressPacket::Kind::Ack);
  EXPECT_NE(dataSock, nullptr)
      << "Data CoS socket should exist for every logical socketId";
  EXPECT_NE(ackSock, nullptr)
      << "ACK CoS socket should exist for every logical socketId";
  EXPECT_NE(dataSock, ackSock)
      << "Data and ACK CoS sockets must be distinct so each can carry "
         "its own TOS without per-packet setsockopt";
}

namespace {
// Builds a minimal SEL_ACK EgressPacket for routing tests. Only `kind` and
// `socketToSendOn` are inspected by the sendLoop; the contents of the AckHeader
// are placeholder values.
std::unique_ptr<EgressPacket> makeAckEgressPacket(
    const uint32_t qpNum,
    const uint16_t flowId,
    PacketModifierSocket* socket,
    folly::IPAddress remoteAddr) {
  return std::make_unique<EgressPacket>(
      std::make_unique<AckPacket>(
          BaseHeader::BaseHeaderBuilder()
              .setOpcode(
                  BaseHeader::Opcode(
                      BaseHeader::Opcode::OperationType::SEL_ACK))
              .setVersion(kMetaRoceProtocolVersion)
              .setFlowId(flowId)
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
      socket,
      std::move(remoteAddr),
      qpNum,
      EgressPacket::RxmitType::None);
}
} // namespace

// An EgressPacket with kind=Ack must be written to the ACK CoS socket
// only, never to the data CoS socket.
TEST_F(SenderTest, AckPacketRoutesToAckSocket) {
  // The fixture's `qp` was registered with `arbiter` in SetUp(). Drop it
  // so the only QP the arbiter knows about is the synthetic one below.
  qp = nullptr;

  auto sender = std::make_unique<LimitedTestSender>(
      arbiter,
      mockUDPSocketMaker,
      kMetaRoceSendToPort,
      LimitedTestSender::kEnableSendLoop);
  sender->setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  const auto socketId = sender->getSocketIdFromAddress(localAddr);

  auto* const dataSock = dynamic_cast<MockUDPSocket*>(
      sender->getSocketById(socketId, EgressPacket::Kind::Data));
  auto* const ackSock = dynamic_cast<MockUDPSocket*>(
      sender->getSocketById(socketId, EgressPacket::Kind::Ack));
  ASSERT_NE(dataSock, nullptr);
  ASSERT_NE(ackSock, nullptr);

  // The data socket must never see this ACK packet.
  EXPECT_CALL(*dataSock, write(_, _)).Times(0);

  // The ACK socket should see exactly one write. Post a baton on arrival
  // so the test thread can wait deterministically.
  folly::Baton<> ackWritten;
  EXPECT_CALL(*ackSock, write(_, _))
      .Times(1)
      .WillOnce([&](const folly::SocketAddress&,
                    const std::unique_ptr<folly::IOBuf>& buf) {
        ackWritten.post();
        return buf->computeChainDataLength();
      });

  // Register a synthetic QP whose first call yields one kind=Ack packet
  // and subsequent calls return nullptr. The sendLoop coroutine will
  // pick it up, route via socketForKind(...), and write it.
  constexpr uint32_t kSyntheticQpNum = 7;
  constexpr uint16_t kSyntheticFlowId = 0;
  bool delivered = false;
  arbiter.addQp(kSyntheticQpNum, [&]() -> std::unique_ptr<EgressPacket> {
    if (delivered) {
      return nullptr;
    }
    delivered = true;
    return makeAckEgressPacket(
        kSyntheticQpNum,
        kSyntheticFlowId,
        sender->getSendSocket(socketId, EgressPacket::Kind::Ack),
        remoteAddr);
  });
  arbiter.signalReady(kSyntheticQpNum);

  EXPECT_TRUE(ackWritten.try_wait_for(std::chrono::seconds(2)))
      << "ACK packet was not written to the ACK socket within 2s";

  sender = nullptr; // Tear down sendLoop before the baton goes out of scope.
}

TEST_F(SenderTest, SingleOwnerTxRingNotAllocatedWhenFlagOff) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_single_owner_tx = false;

  // The ring is allocated at setupFlows from the flag, so bring a fresh QP
  // through it with the flag off.
  qp = nullptr;
  qp = std::make_unique<Qp>(10, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
  qp->addPortPair(localAddr, remoteAddr);
  auto sender = createTestSenderWithFlows(
      mockUDPSocketMaker, LimitedTestSender::kDisableSendLoop);

  EXPECT_FALSE(qp->usesSingleOwnerRxRing())
      << "without single_owner_tx the QP must not allocate the ACK ring, so the "
         "receiver keeps applying ACKs inline";
}

/*
 * The QP-wide lock exists for the mode where the receiver applies ACKs itself,
 * and the sender's visit takes it on every pass. With the hand-off ring there
 * is no second writer to exclude, so the visit must find nothing to take: that
 * is what keeps the default path free of it.
 */
TEST_F(SenderTest, InlineAckLockIsEngagedOnlyWhenTheReceiverAppliesAcks) {
  gflags::FlagSaver flagSaver;

  // The ring is allocated at setupFlows, so each mode needs its own QP.
  FLAGS_libsoftmetaroce_single_owner_tx = true;
  qp = nullptr;
  qp = std::make_unique<Qp>(10, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
  qp->addPortPair(localAddr, remoteAddr);
  auto sender = createTestSenderWithFlows(
      mockUDPSocketMaker, LimitedTestSender::kDisableSendLoop);
  EXPECT_FALSE(qp->lockForInlineAckApply().owns_lock())
      << "with the hand-off ring the sender owns transmit state alone, so its "
         "visit must not pay for a lock with nothing to exclude";

  FLAGS_libsoftmetaroce_single_owner_tx = false;
  qp = nullptr;
  qp = std::make_unique<Qp>(10, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
  EXPECT_TRUE(qp->lockForInlineAckApply().owns_lock())
      << "without the ring a receiver applies ACKs itself, so the visit must "
         "take the lock that keeps the two apart";
}

TEST_F(SenderTest, SingleOwnerTxEnqueuedAckIsAppliedBySenderDrain) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_single_owner_tx = true;

  // The flag above keeps the ring allocated regardless of the default; rebuild
  // the QP to wire the send-loop-disabled sender below (the test thread must be
  // the ring's sole consumer). Reset first so the old QP leaves the arbiter.
  qp = nullptr;
  qp = std::make_unique<Qp>(10, retxTimer, arbiter, mrs, &cq, &ibvQpStorage);
  qp->addPortPair(localAddr, remoteAddr);
  qp->setRemoteQpNum(100);
  qp->setMtu(1000);
  qp->setCongestionController(
      std::make_unique<NiceMock<MockCongestionController>>());

  // No send loop: the test thread is the sole consumer of the SPSC ACK ring
  // (it drives getNextPacketToSend directly); a background sender draining the
  // ring concurrently would violate the single-consumer contract.
  auto sender = std::make_unique<LimitedTestSender>(
      arbiter,
      mockUDPSocketMaker,
      kMetaRoceSendToPort,
      LimitedTestSender::kDisableSendLoop);
  qp->setupFlows(1, 1, sender.get());
  const uint16_t flowId = 0;
  auto* flow = qp->getTxFlowById(flowId);

  EXPECT_TRUE(qp->usesSingleOwnerRxRing())
      << "single_owner_tx must allocate the ACK ring by setupFlows so the "
         "receiver hands ACKs off instead of applying them inline";

  const uint16_t frames = 5;
  const uint32_t length = frames * qp->getMtu();
  std::unique_ptr<char[]> payload(new char[length]);
  auto wqe =
      std::make_unique<SendWqe>(0, oneSge(payload.get(), length), qp->getMtu());
  wqe->setMsn(0);
  wqe->setCsn(0);
  auto& txState = flow->getTxState();
  setupTxStateWithSentPackets(flow, wqe.get(), frames, qp->getMtu());
  txState.sndUna = 0;

  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setTag(kMetaRoceInitialTag)
                                  .setCumulativeFsn(frames - 1)
                                  .setMaxRcvdFsn(frames - 1)
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(getNowNs())
                                  .build();

  // Hand the ACK to the QP the way the receiver does: a steady-clock receipt
  // time already in the past, so the receipt-to-process wait is positive.
  qp->enqueueReceivedAck(flowId, ackHeader, steadyNowNs() - 1000);

  EXPECT_EQ(txState.sndUna, 0)
      << "enqueueReceivedAck must defer application: the receiver hands the ACK "
         "off without touching TX state";

  // The deferred ACK is applied when the sender drains what was handed off.
  qp->applyHandedOffAcks();

  EXPECT_EQ(txState.sndUna, frames)
      << "the sender drain must apply the deferred ACK, advancing sndUna to all "
         "5 frames -- the same TX state the inline processAckPacket path reaches";
  EXPECT_EQ(txState.tracker.size(), 0)
      << "tracker should be empty once the drained ACK acks every in-flight frame";

  qp = nullptr;
  sender = nullptr;
}

} // namespace libsoftmetaroce
