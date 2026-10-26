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

#include <folly/coro/BlockingWait.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/synchronization/Baton.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/Ecn.h>
#include <libsoftmetaroce/Flow.h>
#include <libsoftmetaroce/ProtocolCompatibility.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/RxFlow.h>
#include <libsoftmetaroce/TxFlow.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/test/AckProcessedAt.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <libsoftmetaroce/test/PostToTag.h>
#include <libsoftmetaroce/test/QpTestBase.h>
#include <libsoftmetaroce/test/TxTimerSuppressor.h>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>

DECLARE_bool(libsoftmetaroce_slow_receiver);
DECLARE_uint32(libsoftmetaroce_min_rto_us);
DECLARE_string(libsoftmetaroce_send);

using namespace testing;
namespace libsoftmetaroce {

static std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

namespace {

// A second endpoint's qpNum, distinct from the fixture qp: the shared test
// arbiter registers each qpNum once, so a second Qp must not reuse it.
constexpr uint32_t kSecondQpNum = 11;

// The UDP source port of the data an ACK published directly acknowledges.
constexpr uint16_t kIngressSourcePort = 40000;

// processDataPacket validates payload size against the receive WQE, so a packet
// that matches a WQE must carry its per-posn payload (one MTU here).
const std::vector<char>& testPayload() {
  static const std::vector<char> payload(kMaxPacketSizeBytes);
  return payload;
}

const DataPacket buildDataPacketFromTestDataHeader(
    const Qp& qp,
    const DataHeader& dataHeader) {
  BaseHeader::Opcode opCode(BaseHeader::Opcode::OperationType::SEND);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(qp.getLocalQpNum())
                                    .setDstQp(qp.getRemoteQpNum())
                                    .setFlowId(0)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();
  return DataPacket(baseHeader, dataHeader, testPayload().data(), qp.getMtu());
}

[[nodiscard]] bool allocatePosns(Wqe& wqe, uint32_t count) {
  for (uint32_t posn = 0; posn < count; ++posn) {
    if (wqe.allocateNextPosn() != posn) {
      ADD_FAILURE()
          << "precondition: posns go out in order, so the tracker entries a "
             "case builds below match the posns allocated here";
      return false;
    }
  }
  return true;
}

ibv_gid_entry makeUdpEntry() {
  ibv_gid_entry entry{};
  entry.gid_type = kGidTypeMetaRoceUdp;
  return entry;
}

// Leaves the tag blocked at msn with probes charged against its rnr_retry
// allowance.
void blockTagAfterProbes(Tag& tag, uint16_t msn, int probes) {
  tag.brnr().blockAt(msn);
  for (int i = 0; i < probes; ++i) {
    tag.brnr().beginProbe();
    tag.brnr().blockAt(msn);
  }
}

} // namespace

class QpTest : public QpTestBase {};

TEST_F(QpTest, SendRecordsWirePadding) {
  ScopedSuppressedTxTimers suppressTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  std::array<char, 1> payload{};
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .address = payload.data()});

  auto egressPacket = qp.getTxFlowById(0)->getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "a posted one-byte send must produce one data packet";
  const auto* dataPacket =
      dynamic_cast<const DataPacket*>(egressPacket->packet.get());
  ASSERT_NE(dataPacket, nullptr);
  EXPECT_EQ(dataPacket->getDataHeader().getPadding(), 3)
      << "the METH must disclose the three bytes added for wire alignment";
}

/*
 * A send posted with several sges puts them on the wire back to back. A packet
 * within one sge is sent from it in place; one crossing a boundary is gathered.
 */
TEST_F(QpTest, ASegmentedSendCarriesItsSgesInOrder) {
  ScopedSuppressedTxTimers suppressTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  std::vector<uint8_t> first(mtu / 2);
  std::vector<uint8_t> second(mtu);
  std::vector<uint8_t> third(mtu * 3 / 2);
  std::vector<uint8_t> message;
  for (auto* buffer : {&first, &second, &third}) {
    for (auto& byte : *buffer) {
      byte = static_cast<uint8_t>(message.size() * 7 + 3);
      message.push_back(byte);
    }
  }
  auto wqe = std::make_unique<SendWqe>(
      23,
      LocalSges{
          {.address = first.data(),
           .length = static_cast<uint32_t>(first.size())},
          {.address = second.data(),
           .length = static_cast<uint32_t>(second.size())},
          {.address = third.data(),
           .length = static_cast<uint32_t>(third.size())},
      },
      mtu);
  ASSERT_TRUE(qp.postWqe(kMetaRoceInitialTag, std::move(wqe)))
      << "precondition: the send fits the send queue";
  ASSERT_TRUE(processWqes(&qp, 1, 0))
      << "precondition: the send reaches its tag before packets are pulled";

  std::vector<uint8_t> sent;
  const uint8_t* lastPayload = nullptr;
  for (int i = 0; i < 3; ++i) {
    auto egressPacket = qp.getTxFlowById(0)->getNextPacketToSend();
    ASSERT_TRUE(egressPacket) << "a three-mtu send must produce packet " << i;
    const auto* dataPacket =
        dynamic_cast<const DataPacket*>(egressPacket->packet.get());
    ASSERT_NE(dataPacket, nullptr);
    const folly::ByteRange payload = dataPacket->getPayload();
    EXPECT_EQ(payload.size(), mtu)
        << "packet " << i << " is a full mtu, whether gathered or in place";
    sent.insert(sent.end(), payload.begin(), payload.end());
    lastPayload = payload.data();
  }
  EXPECT_EQ(sent, message)
      << "the packets together must carry the sges in order, with nothing "
         "dropped or repeated at a boundary";
  EXPECT_EQ(lastPayload, third.data() + mtu / 2)
      << "the last packet lies within the third sge, so it is sent in place";
}

/*
 * Tests the number, size, and fields of the packets generated by
 * TxFlow::getNextPacketToSend() by creating a QP and a TxFlow and
 * posting a writeImm WQE.
 */
TEST_F(QpTest, GetNextPacketToSend) {
  EXPECT_EQ(qp.getLocalQpNum(), 10);
  EXPECT_EQ(qp.getMtu(), mtu);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getTxFlowById(0);
  auto egressPacket = flow->getNextPacketToSend();
  EXPECT_FALSE(egressPacket)
      << "Newly created flow should have nothing to send";

  const uint16_t msgSizeBytes = mtu * 2 + uint16_t(mtu / 2); // 3 packets
  uint16_t msgSizeToSendBytes = msgSizeBytes;
  const uint32_t immediateData = 1234;
  auto payload = std::make_unique<char[]>(msgSizeBytes);
  const auto postedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kWriteWithImm,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get(),
       .maybeImmediateData = immediateData});
  const uint16_t msn = postedWqe->getMsn().value();
  const uint16_t csn = postedWqe->getCsn().value();

  uint16_t sndNext = 0;

  while (msgSizeToSendBytes > 0) {
    egressPacket = flow->getNextPacketToSend();
    ASSERT_TRUE(egressPacket);
    const uint16_t payloadSize = std::min(mtu, msgSizeToSendBytes);
    const auto packetShouldHaveImmdt = (msgSizeToSendBytes <= mtu);
    const auto optionalImmdtSize =
        packetShouldHaveImmdt ? sizeof(immediateData) : 0;
    ASSERT_NE(egressPacket->packet, nullptr);
    EXPECT_EQ(
        egressPacket->packet->getSizeBytes(),
        BaseHeader::kBaseHeaderSizeBytes + DataHeader::kDataHeaderSizeBytes +
            ReadWriteHeader::kReadWriteHeaderSizeBytes + optionalImmdtSize +
            payloadSize);
    msgSizeToSendBytes -= payloadSize;

    auto packet = dynamic_cast<ReadWritePacket*>(egressPacket->packet.get());
    const auto& baseHeader = packet->getBaseHeader();

    const auto opcode = baseHeader.getOpcode();
    EXPECT_EQ(opcode.bthOpcode(), BaseHeader::Opcode::BthOpcode::META_ROCE);

    EXPECT_EQ(baseHeader.getFlowId(), 0);
    EXPECT_EQ(baseHeader.getSrcQp(), 10);

    const auto& dataHeader = packet->getDataHeader();
    EXPECT_EQ(dataHeader.getFsn(), sndNext);
    EXPECT_EQ(dataHeader.getPosn(), sndNext);
    EXPECT_EQ(dataHeader.getMsn(), msn);
    EXPECT_EQ(dataHeader.getCsn(), csn);
    EXPECT_EQ(dataHeader.getAckRequest(), true);
    EXPECT_EQ(dataHeader.getUna(), 0);

    const auto& rwHeader = packet->getReadWriteHeader();
    EXPECT_EQ(rwHeader.getMessageLength(), msgSizeBytes);
    if (msgSizeToSendBytes > 0) {
      EXPECT_EQ(
          opcode.operationType(), BaseHeader::Opcode::OperationType::WRITE_IMM);
      ASSERT_FALSE(packet->getImmData().has_value())
          << "immediate not expected in packets before the last";
    } else {
      EXPECT_EQ(
          opcode.operationType(),
          BaseHeader::Opcode::OperationType::WRITE_IMM_P);
      ASSERT_TRUE(packet->getImmData().has_value())
          << "immediate expected in last packet";
      EXPECT_EQ(packet->getImmData().value(), immediateData);
    }

    sndNext++;
  }
  EXPECT_EQ(sndNext, 3); // sent 3 packets
  EXPECT_EQ(msgSizeToSendBytes, 0);
}

TEST_F(QpTest, SendLastMarksFinalPacketAndRetransmission) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_emit_send_last = true;
  ScopedSuppressedTxTimers suppressTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getTxFlowById(0);
  uint16_t msgSizeToSendBytes = mtu * 3; // 3 packets
  auto payload = std::make_unique<char[]>(msgSizeToSendBytes);

  const auto postedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = payload.get()});
  const uint16_t msn = postedWqe->getMsn().value();
  const uint16_t csn = postedWqe->getCsn().value();

  uint16_t sndNext = 0;

  while (msgSizeToSendBytes > 0) {
    auto egressPacket = flow->getNextPacketToSend();
    EXPECT_TRUE(egressPacket);
    const uint16_t payloadSize = std::min(mtu, msgSizeToSendBytes);
    EXPECT_EQ(
        egressPacket->packet->getSizeBytes(),
        BaseHeader::kBaseHeaderSizeBytes + DataHeader::kDataHeaderSizeBytes +
            payloadSize);
    msgSizeToSendBytes -= payloadSize;

    auto packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    const auto& dataHeader = packet->getDataHeader();
    EXPECT_EQ(dataHeader.getFsn(), sndNext);
    EXPECT_EQ(dataHeader.getPosn(), sndNext);
    EXPECT_EQ(dataHeader.getMsn(), msn);
    EXPECT_EQ(dataHeader.getCsn(), csn);
    EXPECT_EQ(dataHeader.getAckRequest(), true);
    EXPECT_EQ(dataHeader.getUna(), 0);
    const auto expectedOperation = sndNext == 2
        ? BaseHeader::Opcode::OperationType::SEND_LAST
        : BaseHeader::Opcode::OperationType::SEND;
    EXPECT_EQ(
        packet->getBaseHeader().getOpcode().operationType(), expectedOperation)
        << "only the final packet of the send must carry SEND_LAST";

    sndNext++;
  }
  EXPECT_EQ(sndNext, 3); // sent 3 packets
  EXPECT_EQ(msgSizeToSendBytes, 0);

  auto shouldNotBeReady = flow->getNextPacketToSend();
  EXPECT_FALSE(shouldNotBeReady)
      << "All packets are sent and retransmission timeout hasn't expired yet.";
  // Drive the retransmission timeout deterministically on the test thread
  // (the real RTO timer is suppressed), so no timer-thread work races the
  // getNextPacketToSend pump below.
  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  for (auto i = 0; i < 3;
       i++) { // all 3 packets are expected to be retransmitted
    auto egressPacket = flow->getNextPacketToSend();
    ASSERT_TRUE(egressPacket)
        << "Retransmission packet " << i + 1 << " of 3 was not made ready";
    EXPECT_EQ(
        egressPacket->packet->getSizeBytes(),
        BaseHeader::kBaseHeaderSizeBytes + DataHeader::kDataHeaderSizeBytes +
            mtu);

    auto packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    const auto& dataHeader = packet->getDataHeader();
    EXPECT_EQ(dataHeader.getFsn(), i);
    EXPECT_EQ(dataHeader.getPosn(), i);
    EXPECT_EQ(dataHeader.getMsn(), msn);
    EXPECT_EQ(dataHeader.getCsn(), csn);
    EXPECT_EQ(dataHeader.getAckRequest(), true);
    EXPECT_EQ(dataHeader.getUna(), 0);
    EXPECT_TRUE(dataHeader.getIsRetransmit());
    EXPECT_TRUE(dataHeader.getIsTimeout());
    const auto expectedOperation = i == 2
        ? BaseHeader::Opcode::OperationType::SEND_LAST
        : BaseHeader::Opcode::OperationType::SEND;
    EXPECT_EQ(
        packet->getBaseHeader().getOpcode().operationType(), expectedOperation)
        << "a retransmission must preserve the packet's terminal marker";
  }
}

TEST_F(QpTest, ZeroLengthSendUsesSendLast) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_emit_send_last = true;
  ScopedSuppressedTxTimers suppressTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = 0,
       .address = nullptr});

  auto egressPacket = qp.getTxFlowById(0)->getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "a zero-length send must produce its permitted terminal packet";
  const auto* dataPacket =
      dynamic_cast<const DataPacket*>(egressPacket->packet.get());
  ASSERT_NE(dataPacket, nullptr);
  EXPECT_EQ(
      dataPacket->getBaseHeader().getOpcode().operationType(),
      BaseHeader::Opcode::OperationType::SEND_LAST)
      << "a zero-length send consists of one SEND_LAST packet";
  EXPECT_EQ(dataPacket->getDataHeader().getPosn(), 0)
      << "the only packet of a zero-length send must have POSN zero";
  EXPECT_EQ(dataPacket->getPayloadSizeBytes(), 0)
      << "a zero-length send must not carry application payload";
  EXPECT_FALSE(qp.getTxFlowById(0)->getNextPacketToSend())
      << "a zero-length send must contain exactly one packet";
}

TEST_F(QpTest, SendLastEmissionCanBeDisabled) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_emit_send_last = false;
  ScopedSuppressedTxTimers suppressTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  std::array<char, 1> payload{};
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .address = payload.data()});

  auto egressPacket = qp.getTxFlowById(0)->getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "the compatibility setting must not suppress the send packet";
  const auto* dataPacket =
      dynamic_cast<const DataPacket*>(egressPacket->packet.get());
  ASSERT_NE(dataPacket, nullptr);
  EXPECT_EQ(
      dataPacket->getBaseHeader().getOpcode().operationType(),
      BaseHeader::Opcode::OperationType::SEND)
      << "disabling Send Last must retain the legacy Send opcode";
}

TEST_F(QpTest, RtoWithQueuedPacketsKeepsTimerArmed) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_slow_receiver = false;
  FLAGS_libsoftmetaroce_min_rto_us = 1000;
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto* const flow = qp.getTxFlowById(0);

  constexpr uint16_t kFrames = 4;
  std::vector<char> payload(kFrames * mtu);
  auto wqe = std::make_unique<SendWqe>(23 /*workReqId*/, oneSge(payload), mtu);
  wqe->setMsn(initialTag->getAndIncrementNextMsn());
  wqe->setCsn(initialTag->getAndIncrementNextCsn());

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = kFrames;
  txState.maxAckdFsn = 2;
  for (uint16_t fsn = 0; fsn < kFrames; ++fsn) {
    const uint32_t posn = wqe->allocateNextPosn();
    const auto state =
        fsn == 2 ? Flow::FsnState::ACKED : Flow::FsnState::UNACKED;
    txState.tracker.emplace_back(
        PacketId(wqe.get(), kMetaRoceInitialTag, posn, mtu), state);
    if (state == Flow::FsnState::ACKED) {
      wqe->onFrameNewlyAcked(posn);
    }
  }

  ASSERT_EQ(flow->prepareRetxPacketsIfAny(false /*isRtoEvent*/), 2)
      << "the two holes before the selectively acknowledged frame must be "
         "queued";
  auto firstRetx = flow->getNextPacketToSend();
  ASSERT_NE(firstRetx, nullptr)
      << "the first queued hole must be retransmitted before its timer can "
         "expire";
  ASSERT_EQ(flow->getNumPendingRetxPackets(), 1u)
      << "one queued packet must remain when the retransmission timer fires";

  auto* const callback = flow->retxTimerCallback_.get();
  retxTimer.cancelCallback(callback);
  (*callback)();

  const AckHeader ack = AckHeader::AckHeaderBuilder()
                            .setCumulativeFsn(2)
                            .setMaxRcvdFsn(2)
                            .setAckStatus(AckStatus::ACK)
                            .setTimestamp(0)
                            .build();
  flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));

  // folly::coro::timeout() cannot interrupt the baton the arbiter parks on, so
  // bound the wait by ending it: a regression must fail here, not hang. On its
  // own timer so it is cancelled on return, including an early one; the
  // fixture destroys arbiter before retxTimer, and a callback left armed there
  // could outlive arbiter.
  HighResTimer waitBoundTimer;
  waitBoundTimer.scheduleTimeout(
      std::chrono::seconds(10), HighResTimer::Callback([this]() {
        folly::coro::blockingWait(arbiter.shutdown());
      }));

  auto packet =
      folly::coro::blockingWait(arbiter.waitForAndGetNextPacketToSend());
  ASSERT_NE(packet, nullptr)
      << "a null packet means the bound above ended the wait, so the rearmed "
         "timer never retransmitted the frame still outstanding";
  auto* const dataPacket = dynamic_cast<DataPacket*>(packet->packet.get());
  ASSERT_NE(dataPacket, nullptr)
      << "the rearmed timer must eventually retransmit the frame still "
         "outstanding";
  EXPECT_EQ(dataPacket->getDataHeader().getFsn(), 3)
      << "the acknowledged queued frame must be discarded, leaving only the "
         "later outstanding frame to retransmit";

  flow->destroyRetransmissionTimer();
}

/*
 * EAGAIN takes the QP out of the ready set and leaves the rest of a
 * retransmission queue unsent, with nothing scheduled to drain it. The RTO is
 * the fallback that does not depend on an ack arriving: even with nothing new
 * to queue, it has to put the QP back in the ready set.
 */
TEST_F(QpTest, RtoRequeuesQpAfterEagain) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_slow_receiver = false;
  FLAGS_libsoftmetaroce_min_rto_us = 1000;
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto* const flow = qp.getTxFlowById(0);

  constexpr uint16_t kFrames = 2;
  std::vector<char> payload(kFrames * mtu);
  auto wqe = std::make_unique<SendWqe>(26 /*workReqId*/, oneSge(payload), mtu);
  wqe->setMsn(initialTag->getAndIncrementNextMsn());
  wqe->setCsn(initialTag->getAndIncrementNextCsn());

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = kFrames;
  for (uint16_t fsn = 0; fsn < kFrames; ++fsn) {
    const uint32_t posn = wqe->allocateNextPosn();
    txState.tracker.emplace_back(
        PacketId(wqe.get(), kMetaRoceInitialTag, posn, mtu),
        Flow::FsnState::UNACKED);
  }

  ASSERT_EQ(flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/), kFrames)
      << "both outstanding frames must be queued, or there is no leftover "
         "queue for the EAGAIN below to strand";

  // Claimed through the arbiter so the readiness signal
  // prepareRetxPacketsIfAny() posted is drained here; left in the intake, the
  // next drain folds it back in and undoes the removal below.
  auto firstRetx =
      folly::coro::blockingWait(arbiter.waitForAndGetNextPacketToSend());
  ASSERT_NE(firstRetx, nullptr) << "the first queued frame must be dequeued";
  folly::coro::blockingWait(
      arbiter.handleFaultOnEgress(std::move(firstRetx), EAGAIN));
  ASSERT_FALSE(arbiter.isQpnReady(qp.getLocalQpNum()))
      << "EAGAIN must leave the QP out of the ready set, which is the state "
         "this test is about";
  ASSERT_EQ(flow->getNumPendingRetxPackets(), 1u)
      << "a queued frame must remain, with nothing left to send it";

  // Bounded by ending the wait, and on its own timer, for the reasons given
  // in RtoWithQueuedPacketsKeepsTimerArmed.
  HighResTimer waitBoundTimer;
  waitBoundTimer.scheduleTimeout(
      std::chrono::seconds(10), HighResTimer::Callback([this]() {
        folly::coro::blockingWait(arbiter.shutdown());
      }));

  auto* const callback = flow->retxTimerCallback_.get();
  retxTimer.cancelCallback(callback);
  (*callback)();

  auto packet =
      folly::coro::blockingWait(arbiter.waitForAndGetNextPacketToSend());
  ASSERT_NE(packet, nullptr)
      << "a null packet means the bound above ended the wait: the RTO left "
         "the QP out of the ready set, so the queued frame was never sent";
  auto* const dataPacket = dynamic_cast<DataPacket*>(packet->packet.get());
  ASSERT_NE(dataPacket, nullptr)
      << "the RTO must put the QP back so the frame left in the queue goes "
         "out";
  EXPECT_EQ(dataPacket->getDataHeader().getFsn(), 1)
      << "the frame still queued must be the one sent, not a fresh scan of "
         "the window";

  flow->destroyRetransmissionTimer();
}

// Verified the send path and the retransmit path both take this flow's sndCur
// and a Tag's semaphore; they must take them in the same order (sndCur then
// Tag) to avoid an ABBA deadlock.
TEST_F(QpTest, SendPathAcquiresSndCurBeforeTag) {
  ScopedSuppressedTxTimers suppressTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  uint16_t msgSizeBytes = mtu;
  auto payload = std::make_unique<char[]>(msgSizeBytes);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get()});

  initialTag->waitForFree();

  folly::Baton<> workerRunning;
  std::thread sender([&]() {
    workerRunning.post();
    (void)flow->getNextPacketToSend();
  });
  workerRunning.wait();

  bool sndCurHeldWhileBlocked = false;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (std::chrono::steady_clock::now() < deadline) {
    if (auto locked = flow->getTxState().sndCur.tryWLock(); !locked) {
      sndCurHeldWhileBlocked = true;
      break;
    }
    std::this_thread::yield();
  }
  EXPECT_TRUE(sndCurHeldWhileBlocked)
      << "the send path must acquire sndCur before the Tag semaphore; it did "
         "not, so it inverts lock order vs the retransmit path (ABBA)";

  initialTag->markFree();
  sender.join();
}

/*
 * Verifies that rxState, specifically rcvNxt and rcvBitmap fields, is
 * properly updated when a data packet is received and processDataPacket is
 * called.
 */
TEST_F(QpTest, ProcessDataPackets) {
  // create a qp and a flow
  const auto flowId = 0;

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto congestionController = std::make_unique<MockCongestionController>();
  qp.setCongestionController(std::move(congestionController));

  auto flow = qp.getRxFlowById(flowId);

  const uint16_t msn = qp.getTag(kMetaRoceInitialTag)->getCDMSN() + 1;
  const uint32_t length = 3u * mtu;
  auto payload = std::make_unique<char[]>(length);
  postInSequenceReceiveWqe(qp, payload.get(), length);

  const auto pktFsn = 10;
  auto& rxState = flow->getRxState();
  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(pktFsn)
                                    .setUna(pktFsn)
                                    .setPosn(1)
                                    .setMsn(msn)
                                    .build();
  auto dataPacket = buildDataPacketFromTestDataHeader(qp, dataHeader);
  {
    // Cases when fsn == rxState.rcvNxt
    {
      // Case when rcvBitmap is empty
      rxState.rcvNxt = pktFsn;
      flow->processDataPacket(&dataPacket, kEcnECT0);
      EXPECT_EQ(rxState.rcvNxt, pktFsn + 1);
    }

    {
      // Case when rcvBitmap is NOT empty
      rxState.rcvNxt = pktFsn; // reset rcvNxt to 10
      rxState.maxRcvdFsn = pktFsn + 4; // create a hole in rcvBitmap
      rxState.rcvBitmap.resize(5, Flow::FsnState::UNACKED);
      rxState.rcvBitmap.at(1) = Flow::FsnState::ACKED; // set fsn=11 as acked
      rxState.rcvBitmap.at(4) = Flow::FsnState::ACKED; // set fsn=14 as acked
      flow->processDataPacket(&dataPacket, kEcnECT0);
      EXPECT_EQ(rxState.rcvNxt, pktFsn + 2); // rcvNxt will be 12
      // FSNs 10 and 11 are acked, 12 and 13 are unacked, 14 is acked
      EXPECT_EQ(rxState.rcvBitmap.size(), 3);
    }
  }

  {
    // Case when fsn > rxState.rcvNxt
    rxState.rcvNxt = 5;
    rxState.maxRcvdFsn = pktFsn; // reset it to the received fsn
    rxState.rcvBitmap.resize(0);

    const DataHeader newDataHeader = DataHeader::DataHeaderBuilder()
                                         .setFsn(pktFsn)
                                         .setUna(rxState.rcvNxt)
                                         .setPosn(1)
                                         .setMsn(msn)
                                         .build();
    dataPacket = buildDataPacketFromTestDataHeader(qp, newDataHeader);
    flow->processDataPacket(&dataPacket, kEcnECT0);
    EXPECT_GE(rxState.rcvBitmap.size(), 6);
    EXPECT_EQ(rxState.rcvBitmap.at(5), Flow::FsnState::ACKED);
  }
}

/*
 * Tests that an ack is generated only if the ack request bit is set, there are
 * out of order packets in rcvBitmap, or if this packet is out of order. It also
 * verifies the fields of the generated ack.
 */
TEST_F(QpTest, SendAckCondition) {
  // create a qp and a flow
  const auto flowId = 0;

  auto congestionController = std::make_unique<MockCongestionController>();
  qp.setCongestionController(std::move(congestionController));

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getRxFlowById(flowId);

  const uint32_t length = 3u * mtu;
  auto payload = std::make_unique<char[]>(length);
  const auto postedWqe = postInSequenceReceiveWqe(qp, payload.get(), length);
  const auto expectedMsn = qp.getTag(kMetaRoceInitialTag)->getCDMSN() + 1;
  const auto expectedCsn = postedWqe->getCsn().value();

  const auto pktFsn = 10;
  const auto nowNs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count();
  DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                              .setFsn(pktFsn)
                              .setUna(pktFsn)
                              .setPosn(1)
                              .setMsn(expectedMsn)
                              .setCsn(expectedCsn)
                              .setTimestamp(nowNs)
                              .build();
  auto dataPacket = buildDataPacketFromTestDataHeader(qp, dataHeader);
  auto& rxState = flow->getRxState();

  {
    // Case when fsn == rxState.rcvNxt, ack request is not set, and recvBitmap
    // is emtpy: no ack needed
    rxState.rcvNxt = pktFsn;
    EXPECT_FALSE(flow->processDataPacket(&dataPacket, kEcnECT0));
  }

  {
    // Case when fsn < rxState.rcvNxt (spurious retx)
    rxState.rcvNxt = pktFsn + 1;
    auto maybeAck = flow->processDataPacket(&dataPacket, kEcnECT0);
    EXPECT_TRUE(maybeAck.has_value());
    EXPECT_EQ(maybeAck.value().getCumulativeFsn(), pktFsn);
    EXPECT_EQ(maybeAck.value().getMaxRcvdFsn(), pktFsn);
    EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::ACK);
    EXPECT_EQ(maybeAck.value().getTimestamp(), static_cast<uint32_t>(nowNs));
  }
}

/*
 * Tests the BRNR (Buffer Receiver Not Ready) receiver functionality.
 *
 * This test verifies the behavior when a QP receives a packet for which it has
 * no corresponding WQE (Work Queue Entry). In this scenario:
 *
 * 1. When the receiver cannot find a WQE for a packet and the operation type is
 * not READ, it should respond with a BRNR (Backoff Receiver Not Ready) status
 * 2. The BRNR response includes a backoff time exponent that tells the sender
 *    how long to wait before retrying
 * 3. This mechanism prevents the sender from overwhelming a receiver that isn't
 *    ready to process packets for a particular MSN (Message Sequence Number)
 *
 * The test specifically checks:
 * - That a BRNR response is generated when receiving a packet with no matching
 * WQE
 * - That the backoff time exponent in the response is set to the expected value
 * - That the ACK status is correctly set to BRNR
 */
TEST_F(QpTest, BrnrReceiver) {
  // STEP 1: Create and initialize the QP and flow
  // --------------------------------------
  const auto flowId = 0;

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Set up a mock congestion controller that always allows sending packets
  createAlwaysSendingMockCongestionController(qp);

  auto flow = qp.getRxFlowById(flowId);
  auto& rxState = flow->getRxState();

  // Initialize receiver state with a specific FSN (Frame Sequence Number)
  rxState.rcvNxt = 10; // Next expected FSN
  rxState.maxRcvdFsn = 10; // Highest received FSN (same as rcvNxt initially)

  // STEP 2: Create a data packet with an MSN that doesn't have a corresponding
  // WQE
  // --------------------------------------
  const uint16_t pktFsn = 10; // FSN matching rcvNxt (in-order packet)
  const uint16_t msnWithoutWqe =
      100; // Using MSN 100 which has no corresponding WQE

  // Create a SEND packet (not READ, as READ operations are handled differently)
  BaseHeader::Opcode opCode(BaseHeader::Opcode::OperationType::SEND);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(qp.getRemoteQpNum())
                                    .setDstQp(qp.getLocalQpNum())
                                    .setFlowId(flowId)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();

  const DataHeader dataHeader =
      DataHeader::DataHeaderBuilder()
          .setFsn(pktFsn)
          .setUna(pktFsn)
          .setPosn(1) // Position in the message
          .setMsn(msnWithoutWqe) // MSN with no matching WQE
          .setAckRequest(true) // Ack request bit set
          .build();

  DataPacket dataPacket(baseHeader, dataHeader, nullptr, 0);

  // STEP 3: Process the data packet and check the response
  // --------------------------------------
  // When processing a packet with no matching WQE, the QP should generate a
  // BRNR response
  auto maybeAck = flow->processDataPacket(&dataPacket, kEcnECT0);

  // Verify that an ACK was generated (the response wasn't empty)
  ASSERT_TRUE(maybeAck.has_value()) << "Expected an ACK to be generated";

  // Verify that the ACK has BRNR status, indicating the receiver isn't ready
  // for this MSN and the sender should back off
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::BRNR)
      << "Expected ACK to have BRNR status";

  // Verify that the backoff time exponent is set correctly
  // This exponent determines how long the sender should wait before retrying
  EXPECT_EQ(maybeAck.value().getRnrBackoffTimeExp(), kBrnrBackoffTimeExp)
      << "Expected backoff time exponent to be set to kBrnrBackoffTimeExp";

  // Verify the contents of the bit vectors in the ACK
  // The FSN bit vector should be empty (no out-of-order packets)
  // The RNR bit vector should have bit 0 set (for FSN 10)
  auto fsnBitVector = maybeAck.value().getFsnBitVector();
  auto rnrBitVector = maybeAck.value().getRnrBitVector();

  BitVector<256> expectedFsnBitVector{}; // Empty FSN bit vector
  BitVector<256> expectedRnrBitVector{};
  expectedRnrBitVector.setBit(0); // Set bit for FSN 10

  EXPECT_EQ(fsnBitVector, expectedFsnBitVector);
  EXPECT_EQ(rnrBitVector, expectedRnrBitVector);
}

/*
 * Tests the bitmap handling for BRNR (Backoff Receiver Not Ready) states with
 * out-of-order packets.
 *
 * This test verifies the correct handling of FSN (Frame Sequence Number) and
 * RNR (Receiver Not Ready) bit vectors when packets arrive out of order or
 * without corresponding WQEs. It tests:
 *
 * 1. How the receiver tracks out-of-order packets in its bitmap
 * 2. How BRNR states are properly set when packets arrive without matching WQEs
 * 3. How the FSN and RNR bit vectors are constructed in ACK responses
 * 4. The state transitions as packets arrive in different orders
 * 5. The proper handling of bitmap state when packets from different MSNs
 * (Message Sequence Numbers) are interleaved
 */
TEST_F(QpTest, BrnrBitmapHandling) {
  // Set up a mock congestion controller that always allows sending packets
  createAlwaysSendingMockCongestionController(qp);

  // Create a QP and flow for testing bitmap handling
  const auto flowId = 0;

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(0, 1, &mockSender);

  // Create a receive WQE that can handle 4 MTU-sized packets
  const uint32_t length = 4u * mtu;
  auto payload = std::make_unique<char[]>(length);
  const auto postedWqe = postInSequenceReceiveWqe(qp, payload.get(), length);
  const auto existingWqeMsn = qp.getTag(kMetaRoceInitialTag)->getCDMSN() + 1;
  const auto existingWqeCsn = postedWqe->getCsn().value();

  auto flow = qp.getRxFlowById(flowId);
  auto& rxState = flow->getRxState();

  // STEP 1: Simulate receiving the third packet of the existing WQE (out of
  // order) This tests how the bitmap tracks out-of-order packets with valid
  // WQEs
  // --------------------------------------
  uint16_t pktFsn = 2; // Frame Sequence Number 2 (third packet)
  uint16_t sndUna = 0; // Sender's Unacknowledged FSN
  auto opCode = BaseHeader::Opcode(BaseHeader::Opcode::OperationType::SEND);
  BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                              .setSrcQp(qp.getRemoteQpNum())
                              .setDstQp(qp.getLocalQpNum())
                              .setFlowId(flowId)
                              .setOpcode(std::move(opCode))
                              .setVersion(0)
                              .build();

  DataHeader dataHeader =
      DataHeader::DataHeaderBuilder()
          .setFsn(pktFsn)
          .setUna(sndUna)
          .setPosn(pktFsn) // Position matches FSN for this test
          .setMsn(existingWqeMsn) // Using the MSN of our existing WQE
          .setCsn(existingWqeCsn)
          .build();

  DataPacket dataPacket(
      baseHeader, dataHeader, testPayload().data(), qp.getMtu());

  // Process the data packet - this should generate an ACK_OUT_OF_ORDER response
  auto maybeAck = flow->processDataPacket(&dataPacket, kEcnECT0);

  // Verify that an ACK was generated with ACK_OUT_OF_ORDER status
  // This indicates the packet was accepted but arrived out of order
  ASSERT_TRUE(maybeAck.has_value());
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::ACK_OUT_OF_ORDER);

  // Verify the contents of the bit vectors in the ACK
  // The FSN bit vector should have bit 2 set (indicating packet 2 was received)
  // The RNR bit vector should be empty (no RNR conditions)
  auto fsnBitVector = maybeAck.value().getFsnBitVector();
  auto rnrBitVector = maybeAck.value().getRnrBitVector();

  BitVector<256> expectedFsnBitVector{};
  expectedFsnBitVector.setBit(2); // Expect bit 2 to be set (FSN 2 received)

  BitVector<256> expectedRnrBitVector{}; // Expect empty RNR bit vector

  EXPECT_EQ(fsnBitVector, expectedFsnBitVector);
  EXPECT_EQ(rnrBitVector, expectedRnrBitVector);

  // STEP 2: Create a SEND packet with no corresponding WQE (should trigger
  // BRNR) This tests how the bitmap handles packets that have no matching
  // receive WQE
  // --------------------------------------
  pktFsn++; // Increment to FSN 3
  uint16_t msnWithoutWqe = 100; // Using MSN 100 which has no corresponding WQE

  opCode = BaseHeader::Opcode(BaseHeader::Opcode::OperationType::SEND);
  baseHeader = BaseHeader::BaseHeaderBuilder()
                   .setSrcQp(qp.getRemoteQpNum())
                   .setDstQp(qp.getLocalQpNum())
                   .setFlowId(flowId)
                   .setOpcode(std::move(opCode))
                   .setVersion(0)
                   .build();

  dataHeader = DataHeader::DataHeaderBuilder()
                   .setFsn(pktFsn)
                   .setUna(sndUna)
                   .setPosn(0) // First position in the message
                   .setMsn(msnWithoutWqe) // Using MSN with no WQE
                   .build();

  dataPacket = DataPacket(baseHeader, dataHeader, nullptr, 0);

  // Process the data packet - this should generate a BRNR response
  // since there's no WQE for this MSN
  maybeAck = flow->processDataPacket(&dataPacket, kEcnECT0);

  // Verify that an ACK was generated with BRNR (Backoff RNR) status
  // This indicates the receiver doesn't have a WQE for this MSN
  ASSERT_TRUE(maybeAck.has_value());
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::BRNR);

  // Verify that the bitmap has the correct state - the FSN should be marked as
  // BRNR
  EXPECT_EQ(rxState.rcvBitmap.at(pktFsn - rxState.rcvNxt), Flow::FsnState::BRNR)
      << "Expected FSN state for out-of-order packet to be set to BRNR";

  // Verify the contents of the bit vectors in the ACK
  // The FSN bit vector should still have bit 2 set (from previous packet)
  // The RNR bit vector should have bit 3 set (indicating BRNR for FSN 3)
  fsnBitVector = maybeAck.value().getFsnBitVector();
  rnrBitVector = maybeAck.value().getRnrBitVector();

  expectedRnrBitVector.setBit(pktFsn - rxState.rcvNxt); // Set bit for FSN 3

  EXPECT_EQ(fsnBitVector, expectedFsnBitVector);
  EXPECT_EQ(rnrBitVector, expectedRnrBitVector);

  // STEP 3: Send the last packet of the existing WQE
  // This tests how the bitmap handles additional out-of-order packets with
  // valid WQEs
  // --------------------------------------
  pktFsn++; // Increment to FSN 4
  const DataHeader nextDataHeader =
      DataHeader::DataHeaderBuilder()
          .setFsn(pktFsn)
          .setUna(sndUna)
          .setPosn(3) // Fourth position in the message
          .setMsn(existingWqeMsn) // Using the valid WQE MSN
          .build();

  DataPacket nextDataPacket(
      baseHeader, nextDataHeader, testPayload().data(), qp.getMtu());

  // Process the data packet - this should still generate a BRNR response
  // because we have a gap in the received packets (FSNs 0 and 1 are missing)
  maybeAck = flow->processDataPacket(&nextDataPacket, kEcnECT0);

  // Verify that an ACK was generated with BRNR status
  // The status is BRNR because we have a mix of valid and invalid packets
  ASSERT_TRUE(maybeAck.has_value());
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::BRNR);

  // Verify that the bitmaps have the correct state
  // FSN bit vector should now have bits 2 and 4 set
  // RNR bit vector should have bit 3 set
  fsnBitVector = maybeAck.value().getFsnBitVector();
  rnrBitVector = maybeAck.value().getRnrBitVector();

  expectedFsnBitVector.setBit(pktFsn - rxState.rcvNxt); // Set bit for FSN 4
  // Note: We don't set bit 3 in RNR vector again as it's already set

  EXPECT_EQ(fsnBitVector, expectedFsnBitVector);
  EXPECT_EQ(rnrBitVector, expectedRnrBitVector);

  // STEP 4: Send the first packet of the existing WQE
  // This tests how the bitmap is updated when we receive the first packet in
  // sequence
  // --------------------------------------
  pktFsn = 0; // First packet (FSN 0)
  const DataHeader firstDataHeader =
      DataHeader::DataHeaderBuilder()
          .setFsn(pktFsn)
          .setUna(sndUna)
          .setPosn(0) // First position in the message
          .setMsn(existingWqeMsn) // Using the valid WQE MSN
          .build();

  DataPacket firstDataPacket(
      baseHeader, firstDataHeader, testPayload().data(), qp.getMtu());

  // Process the data packet - this should advance rcvNxt to 1
  // but still generate a BRNR response due to the BRNR state at FSN 3
  maybeAck = flow->processDataPacket(&firstDataPacket, kEcnECT0);

  // Verify that an ACK was generated with BRNR status
  // The status is BRNR because we still have the BRNR state at FSN 3
  ASSERT_TRUE(maybeAck.has_value());
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::BRNR);

  // Verify that the bitmaps have the correct state
  // After receiving FSN 0, rcvNxt advances to 1
  // FSN bit vector should now have bits 1 and 3 set (relative to new rcvNxt=1)
  // RNR bit vector should have bit 2 set (relative to new rcvNxt=1)
  fsnBitVector = maybeAck.value().getFsnBitVector();
  rnrBitVector = maybeAck.value().getRnrBitVector();

  // Reset expected bit vectors for the new rcvNxt value
  expectedFsnBitVector = BitVector<256>{};
  expectedFsnBitVector.setBit(
      1); // FSN 2 is now at position 1 relative to rcvNxt=1
  expectedFsnBitVector.setBit(
      3); // FSN 4 is now at position 3 relative to rcvNxt=1

  expectedRnrBitVector = BitVector<256>{};
  expectedRnrBitVector.setBit(
      2); // FSN 3 is now at position 2 relative to rcvNxt=1

  EXPECT_EQ(fsnBitVector, expectedFsnBitVector);
  EXPECT_EQ(rnrBitVector, expectedRnrBitVector);

  // STEP 5: Send the second packet of the existing WQE
  // This tests how the bitmap is updated when we receive packets in sequence
  // --------------------------------------
  pktFsn = 1; // Second packet (FSN 1)
  sndUna = 1; // Sender's Unacknowledged FSN is now 1
  const DataHeader secondDataHeader =
      DataHeader::DataHeaderBuilder()
          .setFsn(pktFsn)
          .setUna(sndUna)
          .setPosn(1) // Second position in the message
          .setMsn(existingWqeMsn) // Using the valid WQE MSN
          .build();

  DataPacket secondDataPacket(
      baseHeader, secondDataHeader, testPayload().data(), qp.getMtu());

  // Process the data packet - this should advance rcvNxt to 3
  // (skipping FSN 2 which was already received)
  maybeAck = flow->processDataPacket(&secondDataPacket, kEcnECT0);

  // All FSNs up to 2 are now accounted for, but we still have a BRNR at FSN 3
  ASSERT_TRUE(maybeAck.has_value());
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::BRNR);

  // Verify that the bitmaps have the correct state
  // After receiving FSN 1, rcvNxt advances to 3
  // FSN bit vector should now have bit 1 (relative to rcvNxt=3)
  // RNR bit vector should have bit 0 set (relative to rcvNxt=3)
  fsnBitVector = maybeAck.value().getFsnBitVector();
  rnrBitVector = maybeAck.value().getRnrBitVector();

  // Reset expected bit vectors for the new rcvNxt value
  expectedFsnBitVector = BitVector<256>{};
  expectedFsnBitVector.setBit(
      1); // FSN 4 is now at position 1 relative to rcvNxt=3

  expectedRnrBitVector = BitVector<256>{};
  expectedRnrBitVector.setBit(
      0); // FSN 3 is now at position 0 relative to rcvNxt=3

  EXPECT_EQ(fsnBitVector, expectedFsnBitVector);
  EXPECT_EQ(rnrBitVector, expectedRnrBitVector);
  EXPECT_EQ(maybeAck.value().getCumulativeFsn(), 2);

  // CFSN: 2
  // FSNV:   0 1
  // RNRV:   1 0

  // STEP 6: Send one more packet of the non-existing WQE
  // This tests how the bitmap handles additional packets with no matching WQEs
  // --------------------------------------
  pktFsn = 5; // New packet with FSN 5
  // All FSNs up to 4 have been accounted for (including BRNR at FSN 3)
  // so we can advance sndUna to 5
  sndUna = pktFsn;

  const DataHeader lastDataHeader =
      DataHeader::DataHeaderBuilder()
          .setFsn(pktFsn)
          .setUna(pktFsn)
          .setPosn(1) // Second position in the message
          .setMsn(msnWithoutWqe) // Using MSN with no WQE
          .build();

  DataPacket lastDataPacket(baseHeader, lastDataHeader, nullptr, 0);

  // Process the data packet - this should generate a BRNR response
  // since there's no WQE for this MSN
  maybeAck = flow->processDataPacket(&lastDataPacket, kEcnECT0);

  // Verify that an ACK was generated with BRNR status
  ASSERT_TRUE(maybeAck.has_value());
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::BRNR);

  // Verify that the bitmaps have the correct state
  const auto& ack = maybeAck.value();

  EXPECT_EQ(ack.getCumulativeFsn(), 4) << "rcvNxt should be at FSN 5";

  expectedFsnBitVector = BitVector<256>{};
  EXPECT_EQ(ack.getFsnBitVector(), expectedFsnBitVector)
      << "FSN bit vector should not have any bits set";

  expectedRnrBitVector = BitVector<256>{};
  // FSN 5 is now at position 0 relative to rcvNxt=5
  expectedRnrBitVector.setBit(0);
  EXPECT_EQ(ack.getRnrBitVector(), expectedRnrBitVector)
      << "RNR bit vector should have bit 0 set (for FSN 5)";
}

/*
 * Verifies that the presence of CE marks in the received data packet is
 * properly tracked by RxState and incorporated into egress ACKs.
 */
TEST_F(QpTest, CeReception) {
  // create a qp and a flow
  const auto flowId = 0;

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getRxFlowById(flowId);
  auto pktFsn = 10;
  auto& rxState = flow->getRxState();

  auto congestionController = std::make_unique<MockCongestionController>();
  qp.setCongestionController(std::move(congestionController));

  const uint32_t length = 20u * mtu;
  auto payload = std::make_unique<char[]>(length);
  const auto receiveWqe = postInSequenceReceiveWqe(qp, payload.get(), length);
  const auto msn = qp.getTag(kMetaRoceInitialTag)->getCDMSN() + 1;
  const auto csn = receiveWqe->getCsn().value();

  // CE is set and an ACK is requested with ackRequest.
  {
    rxState.rcvNxt = pktFsn;
    rxState.rcvBitmap.resize(0);
    rxState.maxRcvdFsn = pktFsn - 1;
    DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                .setFsn(pktFsn)
                                .setUna(pktFsn)
                                .setPosn(1)
                                .setMsn(msn)
                                .setCsn(csn)
                                .setAckRequest(true)
                                .build();
    auto dataPacket = buildDataPacketFromTestDataHeader(qp, dataHeader);
    std::optional<AckHeader> ackHdr =
        flow->processDataPacket(&dataPacket, kEcnCE);
    EXPECT_TRUE(ackHdr.has_value());
    EXPECT_EQ(ackHdr.value().getCnpCE(), 1);
    EXPECT_EQ(rxState.receivedCeCount, 0);
  }

  // CE is not set and an ACK is requested with ackRequest.
  {
    rxState.rcvNxt = pktFsn;
    rxState.rcvBitmap.resize(0);
    rxState.maxRcvdFsn = pktFsn - 1;
    DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                .setFsn(pktFsn)
                                .setUna(pktFsn)
                                .setPosn(1)
                                .setMsn(msn)
                                .setCsn(csn)
                                .setAckRequest(true)
                                .build();
    auto dataPacket = buildDataPacketFromTestDataHeader(qp, dataHeader);
    std::optional<AckHeader> ackHdr =
        flow->processDataPacket(&dataPacket, kEcnECT0);
    EXPECT_TRUE(ackHdr.has_value());
    EXPECT_EQ(ackHdr.value().getCnpCE(), 0);
    EXPECT_EQ(rxState.receivedCeCount, 0);
  }
}

/*
 * Spec 4.2.2 asks for an acknowledgment before the count of CE-marked packets
 * could outgrow the four-bit field reporting it, and for nothing on a change of
 * congestion state alone. Marked packets arrive here with the ack request bit
 * clear and in order, so the count is the only thing that can ask for one.
 */
TEST_F(QpTest, AckOnceTheCeCountReachesItsThreshold) {
  const auto flowId = 0;

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getRxFlowById(flowId);
  auto pktFsn = 0;
  auto& rxState = flow->getRxState();

  auto congestionController = std::make_unique<MockCongestionController>();
  qp.setCongestionController(std::move(congestionController));

  const uint32_t length = 20u * mtu;
  auto payload = std::make_unique<char[]>(length);
  const auto receiveWqe = postInSequenceReceiveWqe(qp, payload.get(), length);
  const auto msn = qp.getTag(kMetaRoceInitialTag)->getCDMSN() + 1;
  const auto csn = receiveWqe->getCsn().value();

  rxState.rcvNxt = pktFsn;
  rxState.rcvBitmap.resize(0);
  rxState.maxRcvdFsn = UINT16_MAX;

  auto buildPacket = [&](uint16_t fsn) {
    return DataHeader::DataHeaderBuilder()
        .setFsn(fsn)
        .setUna(fsn)
        .setPosn(fsn)
        .setMsn(msn)
        .setCsn(csn)
        .setAckRequest(false)
        .build();
  };

  auto deliver = [&](uint8_t ecn) {
    auto dh = buildPacket(pktFsn);
    auto dp = buildDataPacketFromTestDataHeader(qp, dh);
    ++pktFsn;
    return flow->processDataPacket(&dp, ecn);
  };

  EXPECT_FALSE(deliver(kEcnECT0).has_value())
      << "an unmarked packet carries no reason to acknowledge";
  EXPECT_FALSE(deliver(kEcnCE).has_value())
      << "one mark is far short of what the cnp field holds, so reporting it "
         "at once would spend an ack on nothing the sender can act on";
  EXPECT_FALSE(deliver(kEcnECT0).has_value())
      << "congestion clearing is not a reason to acknowledge: the sender "
         "learns it from the next ack's count either way";

  // Seven more marks, the last of which reaches the threshold.
  for (int i = 0; i < kCeCountAckThreshold - 2; ++i) {
    EXPECT_FALSE(deliver(kEcnCE).has_value())
        << "acknowledged early at mark " << i + 2 << " of "
        << static_cast<int>(kCeCountAckThreshold);
  }

  const auto ack = deliver(kEcnCE);
  ASSERT_TRUE(ack.has_value())
      << "the count has to be reported before it can outgrow the four bits "
         "carrying it, or the sender is told of less congestion than arrived";
  EXPECT_EQ(ack.value().getCnpCE(), kCeCountAckThreshold)
      << "the ack reports every mark counted since the last one";
  EXPECT_EQ(rxState.receivedCeCount, 0)
      << "reporting the count clears it, so the next ack starts from zero";
}

/*
 * A selective ack for the later frames of a window queues retransmissions for
 * the holes behind them. A second ack can fill one of those holes before its
 * retransmission has been sent, which leaves a queued packet for a frame the
 * peer has told us it received. It must not go out: it is wasted bandwidth,
 * and the work request its payload points into may have completed and been
 * freed by the time the packet would be assembled.
 *
 * snd_una cannot answer this on its own -- it is still held down by the frame
 * ahead, so the queued packet is not behind it.
 */
TEST_F(QpTest, AFrameSelectivelyAckedWhileQueuedIsNotRetransmitted) {
  const auto flowId = 0;
  auto congestionController = std::make_unique<MockCongestionController>();
  auto rawCongestionController = congestionController.get();
  qp.setCongestionController(std::move(congestionController));
  ON_CALL(*rawCongestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(0)); // no window for new packets, only retx
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(flowId);

  constexpr int kFrames = 4;
  const uint32_t length = kFrames * mtu;
  auto payload = std::make_unique<char[]>(length);
  auto wqe = std::make_unique<SendWqe>(
      23 /*workReqId*/, oneSge(payload.get(), length), mtu);
  wqe->setMsn(initialTag->getAndIncrementNextMsn());
  wqe->setCsn(initialTag->getAndIncrementNextCsn());

  // Frames 1..4 are on the wire, none acknowledged.
  auto& txState = flow->getTxState();
  txState.sndUna = 1;
  *(txState.sndCur.wlock()) = 1;
  txState.maxAckdFsn = 0;
  txState.sndNxt = 5;
  for (int i = 0; i < kFrames; i++) {
    txState.tracker.emplace_back(
        PacketId(wqe.get(), 0, wqe->allocateNextPosn(), mtu),
        Flow::FsnState::UNACKED);
  }

  // The peer reports 3 and 4, leaving 1 and 2 as holes to retransmit.
  BitVector<AckHeader::kAckTrackerBits> sackThreeAndFour;
  sackThreeAndFour.setBit(2);
  sackThreeAndFour.setBit(3);
  const AckHeader firstAck = AckHeader::AckHeaderBuilder()
                                 .setCumulativeFsn(0)
                                 .setMaxRcvdFsn(4)
                                 .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                                 .setFsnBitVector(sackThreeAndFour)
                                 .setTimestamp(0)
                                 .build();
  flow->processAckPacket(
      firstAck, simulatedProcessedAtNs(firstAck.getTimestamp()));
  ASSERT_EQ(flow->getNumPendingRetxPackets(), 2)
      << "the two holes behind the selective ack must be queued to resend";

  // 2 arrives after all, before its queued retransmission has gone out. snd_una
  // stays at 1, so the queued packet for 2 is not behind it.
  BitVector<AckHeader::kAckTrackerBits> sackTwoThreeAndFour;
  sackTwoThreeAndFour.setBit(1);
  sackTwoThreeAndFour.setBit(2);
  sackTwoThreeAndFour.setBit(3);
  const AckHeader secondAck = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(0)
                                  .setMaxRcvdFsn(4)
                                  .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                                  .setFsnBitVector(sackTwoThreeAndFour)
                                  .setTimestamp(0)
                                  .build();
  flow->processAckPacket(
      secondAck, simulatedProcessedAtNs(secondAck.getTimestamp()));
  ASSERT_EQ(txState.sndUna, 1)
      << "snd_una must still be held down by frame 1, which is the whole "
         "reason the queued packet for 2 is not caught by comparing to it";

  std::set<uint16_t> retransmittedFsns;
  while (auto egressPacket = flow->getNextPacketToSend()) {
    auto* dataPacket = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    ASSERT_NE(dataPacket, nullptr);
    retransmittedFsns.insert(dataPacket->getDataHeader().getFsn());
  }

  EXPECT_THAT(retransmittedFsns, Contains(1u))
      << "frame 1 is still unacknowledged and must be resent";
  EXPECT_THAT(retransmittedFsns, Not(Contains(2u)))
      << "frame 2 was acknowledged while its retransmission sat queued; "
         "resending it wastes the wire and assembles from a work request that "
         "may already have completed";
}

/*
 * The setup the give-up tests share: one flow carrying one message, a window
 * that starts at fsn 10, and the frames the caller lists already in the
 * tracker. Each test then says only what is its own -- what it does to the
 * message, the ack that arrives, and what must follow.
 *
 * Frames are added in the order given and take consecutive fsns from 10, so
 * `posns[i]` is the posn of the frame at fsn 10 + i.
 */
struct TrackedMessageFixture {
  LimitedTestSender mockSender;
  MockCongestionController* congestionController;
  TxFlow* flow;
  TxFlow::TxState* txState;
  std::unique_ptr<char[]> payload;
  std::unique_ptr<SendWqe> wqe;
  std::vector<uint32_t> posns;

  TrackedMessageFixture(
      QpArbiter& arbiter,
      Qp& qp,
      uint16_t mtu,
      std::shared_ptr<Tag>& initialTag,
      std::initializer_list<Flow::FsnState> frames,
      uint16_t sndCur,
      int messageFrames = 4)
      : mockSender(arbiter, mockUDPSocketMaker) {
    auto ownedController = std::make_unique<MockCongestionController>();
    congestionController = ownedController.get();
    qp.setCongestionController(std::move(ownedController));
    qp.setupFlows(1, 1, &mockSender);
    flow = qp.getTxFlowById(0);

    const uint32_t length = messageFrames * mtu;
    payload = std::make_unique<char[]>(length);
    wqe = std::make_unique<SendWqe>(
        23 /*workReqId*/, oneSge(payload.get(), length), mtu);
    wqe->setMsn(initialTag->getAndIncrementNextMsn());
    wqe->setCsn(initialTag->getAndIncrementNextCsn());

    txState = &flow->getTxState();
    txState->sndUna = 10;
    *(txState->sndCur.wlock()) = sndCur;
    txState->maxAckdFsn = 9;
    txState->sndNxt = 10 + frames.size();
    for (const auto state : frames) {
      posns.push_back(wqe->allocateNextPosn());
      txState->tracker.emplace_back(
          PacketId(wqe.get(), 0, posns.back(), mtu), state);
    }
  }
};

// A single BRNR NAK accepts FSN 10 and refuses FSN 11 of the same message.
// Keep the tag BRNR_BLOCKED_TX, retain its blocked MSN, and leave the refused
// POSN unallocated. Previously, ACK-based resume processing canceled the block
// installed earlier while processing the accepted sibling in this same NAK.
TEST_F(QpTest, BrnrAndAckForOneMessageKeepsTheTagBlocked) {
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED, Flow::FsnState::UNACKED},
      /*sndCur=*/12);
  BitVector<AckHeader::kAckTrackerBits> rnrBits;
  rnrBits.setBit(0);
  const AckHeader ack = AckHeader::AckHeaderBuilder()
                            .setCumulativeFsn(10)
                            .setMaxRcvdFsn(11)
                            .setAckStatus(AckStatus::BRNR)
                            .setRnrBackoffTimeExp(15)
                            .setRnrBitVector(rnrBits)
                            .setTimestamp(0)
                            .build();

  fixture.flow->processAckPacket(ack, simulatedProcessedAtNs(0));

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "accepting a sibling fragment must not release the refused fragment";
  ASSERT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value())
      << "the sender needs the blocked MSN to hold the refused message";
  EXPECT_EQ(
      initialTag->getBrnrFirstBlockedMsn().value(),
      fixture.wqe->getMsn().value())
      << "the block must remain attached to the message refused by this ACK";
  EXPECT_FALSE(fixture.wqe->isFrameAllocated(fixture.posns[1]))
      << "the refused sibling must not be allocated for transmission";
}

/*
 * Retiring the frames given up on at the head of the window can reveal one
 * that was selectively acked earlier. Stopping there leaves snd_una, the
 * timers and "is there unacked data" behind a frame that is already resolved,
 * and
 * nothing else will move it until the peer happens to say something.
 */
TEST_F(QpTest, RetiringTheWindowHeadDoesNotStopAtAnAlreadyAckedFrame) {
  // Frame 10 was given up on, 11 was sacked out of order, 12 is still in
  // flight. The fixture claims both posns before the give-up below, which is
  // what freezes the send cursor.
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::ABANDONED, Flow::FsnState::ACKED},
      /*sndCur=*/10);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  auto* const wqe = fixture.wqe.get();
  const uint32_t abandonedPosn = fixture.posns[0];
  wqe->markFailing(kWcRetryExcErr);
  wqe->abandonPosn(abandonedPosn);
  txState.sndNxt = 13; // the healthy frame added below is 12

  // Frame 12 belongs to a message that is doing nothing wrong, so it stays in
  // flight and the window must stop there.
  auto healthyWqe = std::make_unique<SendWqe>(
      24 /*workReqId*/, oneSge(fixture.payload.get(), mtu), mtu);
  healthyWqe->setMsn(initialTag->getAndIncrementNextMsn());
  healthyWqe->setCsn(initialTag->getAndIncrementNextCsn());
  txState.tracker.emplace_back(
      PacketId(healthyWqe.get(), 0, healthyWqe->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  EXPECT_EQ(txState.sndUna, 12)
      << "the window must move past both the frame given up on and the one "
         "already acked behind it, not stop at the first kind";
  ASSERT_EQ(txState.tracker.size(), 1u)
      << "only the frame still in flight belongs in the tracker";
  EXPECT_TRUE(txState.tracker.at(0).isUnacked());
}

/*
 * A tag that fails rejects the data that would have filled the receives posted
 * on it, so leaving them pending strands them for good. They have no packet
 * on the wire to wait for, so they report as soon as the tag enters error.
 */
TEST_F(QpTest, AFatalNakCompletesTheTagsReceivesRatherThanStrandingThem) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> boundBuffer{};
  std::array<char, 4096> pendingBuffer{};
  // One receive already matched to an incoming message, one still waiting.
  Wqe* boundReceive = postInSequenceReceiveWqe(
      qp, boundBuffer.data(), boundBuffer.size(), kMetaRoceInitialTag);
  Wqe* pendingReceive = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kReceive,
       .msgSizeBytes = pendingBuffer.size(),
       .tagNum = kMetaRoceInitialTag,
       .address = pendingBuffer.data()});
  ASSERT_NE(boundReceive, nullptr)
      << "the nak must find a receive already bound to a message, or this is "
         "not the case being exercised";
  ASSERT_NE(pendingReceive, nullptr)
      << "a second, unbound receive is what shows the flush reaches past the "
         "bound one";
  ASSERT_EQ(cq.getNumCompletedWqes(), 0)
      << "the queue must start empty so the completions counted below are the "
         "two the nak produces";

  const AckHeader nak =
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
          .setTimestamp(0)
          .build();
  flow->processNakFatalError(nak);

  // Fatal: polling below would block forever on a stranded receive.
  ASSERT_EQ(cq.getNumCompletedWqes(), 2u)
      << "both receives must report; the tag nak's the data that would have "
         "filled them, so no later event will complete them";
  for (int i = 0; i < 2; i++) {
    const auto cqe = folly::coro::blockingWait(cq.pollCq());
    EXPECT_EQ(cqe.status, kWrFlushErr)
        << "a receive the tag can no longer fill is flushed";
  }
}

/*
 * A fatal NAK identifies the message that failed, but that work request may
 * already carry an error of its own -- the oversize rejection records one
 * without marking the work failing -- and the error it carries stands. The
 * nak's status has to reach a work request that can still report it, or the
 * client never learns the peer rejected its work.
 */
TEST_F(QpTest, AFatalNakForAMessageCarryingItsOwnErrorHandsTheStatusOn) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* wqeCarryingItsOwnError = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  Wqe* reportingWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 200,
       .address = payload.data()});
  Wqe* trailingWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 300,
       .address = payload.data()});
  ASSERT_NE(wqeCarryingItsOwnError, nullptr)
      << "the nak identifies the first work request, so it has to exist";
  ASSERT_NE(reportingWqe, nullptr)
      << "a second work request is the earliest that can carry the nak's "
         "status once the first refuses it";
  ASSERT_NE(trailingWqe, nullptr)
      << "a third is what tells the nak's status apart from a flush: with two "
         "the peer's status reaching every work request looks the same";

  // A status carried without the failing mark, so the hand-off below is
  // reached by the status rather than by the failing state.
  wqeCarryingItsOwnError->setCompletionStatusOverride(kLocalLenErr);

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 3;
  txState.tracker.emplace_back(
      PacketId(
          wqeCarryingItsOwnError,
          kMetaRoceInitialTag,
          wqeCarryingItsOwnError->allocateNextPosn(),
          mtu),
      Flow::FsnState::UNACKED);
  txState.tracker.emplace_back(
      PacketId(
          reportingWqe,
          kMetaRoceInitialTag,
          reportingWqe->allocateNextPosn(),
          mtu),
      Flow::FsnState::UNACKED);
  txState.tracker.emplace_back(
      PacketId(
          trailingWqe,
          kMetaRoceInitialTag,
          trailingWqe->allocateNextPosn(),
          mtu),
      Flow::FsnState::UNACKED);

  const AckHeader nak =
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setCumulRespDeliveredMsn(wqeCarryingItsOwnError->getMsn().value())
          .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
          .setTimestamp(0)
          .build();
  flow->processNakFatalError(nak);

  EXPECT_EQ(wqeCarryingItsOwnError->getCompletionStatusOverride(), kLocalLenErr)
      << "the error this work request already carries is the one that explains "
         "how it ended, so the nak must not replace it";
  EXPECT_EQ(reportingWqe->getCompletionStatusOverride(), kWrRemAccErr)
      << "the peer's rejection has to reach some work request; landing on none "
         "of them would report the failure as an ordinary flush";
  EXPECT_EQ(trailingWqe->getCompletionStatusOverride(), kWrFlushErr)
      << "only one work request carries the peer's rejection; the rest failed "
         "behind it and say so";
}

/*
 * Failing a Tag's work writes its work requests' posn state, which only the
 * sender may touch, so the backoff timer records it and the sender applies it
 * at its next visit. A Tag that still has retries must not be held up by one
 * that has run out: its probe is signalled either way.
 */
TEST_F(QpTest, RnrExhaustionIsAppliedOnTheSendersVisit) {
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  constexpr uint8_t kRetryLimit = 2;
  qp.addTag(kOtherTag, &cq, &otherIbvQpStorage);
  for (const auto tagNum : qp.getTagNums()) {
    qp.getTag(tagNum)->setRnrRetryLimit(kRetryLimit);
  }
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  std::array<char, 4096> payload{};
  ASSERT_NE(
      postWqeHelper(
          &qp,
          {.workType = Wqe::WorkType::kSend,
           .msgSizeBytes = payload.size(),
           .tagNum = kMetaRoceInitialTag,
           .workReqId = 100,
           .address = payload.data()}),
      nullptr);
  ASSERT_NE(
      postWqeHelper(
          &qp,
          {.workType = Wqe::WorkType::kSend,
           .msgSizeBytes = payload.size(),
           .tagNum = kOtherTag,
           .workReqId = 200,
           .address = payload.data()}),
      nullptr);

  // One Tag has spent its retries; the other is blocked but has some left.
  auto exhaustedTag = qp.getTag(kMetaRoceInitialTag);
  const uint16_t exhaustedMsn = exhaustedTag->getTxCompletedMsn() + 1;
  blockTagAfterProbes(*exhaustedTag, exhaustedMsn, kRetryLimit);
  auto probingTag = qp.getTag(kOtherTag);
  probingTag->brnr().blockAt(probingTag->getTxCompletedMsn() + 1);

  (*qp.getBrnrTimerCallback())();

  EXPECT_EQ(probingTag->getBrnrState(), BrnrState::BRNR_PROBING)
      << "the Tag with retries left must still be moved to probe; a sibling "
         "running out must not cut the sweep short";
  EXPECT_FALSE(exhaustedTag->isInErrorState())
      << "the timer thread must not fail the Tag's work itself: that writes "
         "posn state only the sender may touch";

  std::ignore = qp.getNextPacketToSend();

  EXPECT_TRUE(exhaustedTag->isInErrorState())
      << "the sender must apply the pending give-up at its visit";
  EXPECT_FALSE(probingTag->isInErrorState())
      << "the Tag that still has retries must be left alone";
  ASSERT_EQ(cq.getNumCompletedWqes(), 1u)
      << "only the Tag that ran out may report";
  const auto cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.workReqId, 100u)
      << "the completion must be the exhausted Tag's work; reporting the Tag "
         "that is still probing would fail work with retries left to spend";
  EXPECT_EQ(cqe.status, kWcRnrRetryExcErr)
      << "a receiver that never became ready is an rnr retry exceeded error";
}

/*
 * The rnr_retry count is kept per tag, so running out of chances to reach one
 * receiver leaves the other tags' allowances untouched. Only the tag that
 * spent its retries enters the error state.
 */
TEST_F(QpTest, RnrRetriesRunningOutErrorsOnlyTheTagThatSpentThem) {
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  constexpr uint8_t kRetryLimit = 3;
  qp.addTag(kOtherTag, &cq, &otherIbvQpStorage);
  qp.getTag(kMetaRoceInitialTag)->setRnrRetryLimit(kRetryLimit);

  std::array<char, 4096> payload{};
  // Two work requests on the blocked tag: the receiver had no buffer for the
  // second, which is not the earliest still pending.
  Wqe* earlierWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 99,
       .address = payload.data()});
  ASSERT_NE(earlierWqe, nullptr)
      << "the blocked tag needs a work request ahead of the blocked one, or "
         "there is no ordering for the error to be reported against";
  Wqe* blockedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(blockedWqe, nullptr)
      << "the work request the receiver had no buffer for is the one the error "
         "belongs to, so it has to exist";
  Wqe* bystanderWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kOtherTag,
       .workReqId = 200,
       .address = payload.data()});
  ASSERT_NE(bystanderWqe, nullptr)
      << "without work on the other tag this test has no survivor to check";

  // One tag has spent every retry blocked on a receiver that posts no buffer.
  auto blockedTag = qp.getTag(kMetaRoceInitialTag);
  ASSERT_TRUE(blockedWqe->getMsn().has_value())
      << "the tag records the blocked message by msn, so it must have one";
  blockTagAfterProbes(*blockedTag, blockedWqe->getMsn().value(), kRetryLimit);

  std::ignore = qp.processRnrRetryExceeded();

  EXPECT_TRUE(blockedTag->isInErrorState())
      << "the tag that ran out of retries must enter the error state";
  auto bystanderTag = qp.getTag(kOtherTag);
  EXPECT_FALSE(bystanderTag->isInErrorState())
      << "a tag whose receiver never said it was not ready must not be taken "
         "down for one that did";
  EXPECT_EQ(bystanderTag->getNumPendingWqes(), 1u)
      << "the other tag must keep its work request";

  ASSERT_EQ(cq.getNumCompletedWqes(), 2u)
      << "only the tag that ran out may report: the two completions are its "
         "own work requests, and a third would be the bystander's";

  // Completions follow posting order, and the error belongs to the work
  // request the receiver had no buffer for.
  const auto earlierCqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(earlierCqe.workReqId, 99u)
      << "completions follow posting order, so the earlier work request "
         "reports first";
  EXPECT_EQ(earlierCqe.status, kWrFlushErr)
      << "the work request ahead of the blocked one is flushed, not blamed";
  const auto blockedCqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(blockedCqe.workReqId, 100u)
      << "the blocked work request reports behind the one posted before it";
  EXPECT_EQ(blockedCqe.status, kWcRnrRetryExcErr)
      << "the message the receiver was never ready for is the one that ran out "
         "of retries";
}

/*
 * rnr_retry is set through ibv_modify_qp on one ibv_qp, and an ibv_qp is one
 * tag, so two tags of the same QP can be given different allowances. Each
 * spends its own: the tag with the smaller one runs out while the other is
 * still entitled to probe.
 */
TEST_F(QpTest, EachTagSpendsItsOwnRnrRetryAllowance) {
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  constexpr uint8_t kSmallLimit = 2;
  constexpr uint8_t kLargeLimit = 5;
  constexpr int kProbesSpent = 3; // past the small limit, short of the large
  qp.addTag(kOtherTag, &cq, &otherIbvQpStorage);

  auto strictTag = qp.getTag(kMetaRoceInitialTag);
  auto patientTag = qp.getTag(kOtherTag);
  strictTag->setRnrRetryLimit(kSmallLimit);
  patientTag->setRnrRetryLimit(kLargeLimit);

  std::array<char, 4096> payload{};
  for (const auto [tagNum, workReqId] :
       {std::pair<uint8_t, uint64_t>{kMetaRoceInitialTag, 100},
        std::pair<uint8_t, uint64_t>{kOtherTag, 200}}) {
    Wqe* blockedWqe = postWqeHelper(
        &qp,
        {.workType = Wqe::WorkType::kSend,
         .msgSizeBytes = payload.size(),
         .tagNum = tagNum,
         .workReqId = workReqId,
         .address = payload.data()});
    ASSERT_NE(blockedWqe, nullptr)
        << "both tags need work of their own, or one of them has no work to "
           "fail and none to keep";
    auto tag = qp.getTag(tagNum);
    ASSERT_TRUE(blockedWqe->getMsn().has_value())
        << "the tag records the blocked message by msn, so it must have one";
    // Both receivers have been probed the same number of times.
    blockTagAfterProbes(*tag, blockedWqe->getMsn().value(), kProbesSpent);
  }
  ASSERT_TRUE(strictTag->hasSpentRnrRetries())
      << "the tag allowed fewer probes than have been made must be out";
  ASSERT_FALSE(patientTag->hasSpentRnrRetries())
      << "the tag allowed more must not be, or the two are not being judged "
         "against their own limits";

  std::ignore = qp.processRnrRetryExceeded();

  EXPECT_TRUE(strictTag->isInErrorState())
      << "the tag that spent its allowance must enter the error state";
  EXPECT_FALSE(patientTag->isInErrorState())
      << "the tag with retries left must keep probing; a limit shared across "
         "the QP would end both at whichever value was set last";
  EXPECT_EQ(patientTag->getNumPendingWqes(), 1u)
      << "the tag that is still trying must keep its work request";
  EXPECT_EQ(cq.getNumCompletedWqes(), 1u)
      << "only the tag that ran out may report";
}

/*
 * IBTA gives rnr_retry the value 7 the meaning "unlimited", and it is the value
 * a Tag holds until a client sets the attribute -- so it is what every existing
 * queue pair runs with. Probing past seven backoffs must not exhaust it.
 */
TEST_F(QpTest, TheDefaultRnrAllowanceIsNeverSpent) {
  auto tag = qp.getTag(kMetaRoceInitialTag);
  ASSERT_EQ(tag->getRnrRetryLimit(), Tag::kRnrRetryInfinite)
      << "precondition: a Tag no client configured holds the unlimited value, "
         "which is the configuration under test";

  constexpr uint16_t kBlockedMsn = 1;
  tag->brnr().blockAt(kBlockedMsn);
  for (int i = 0; i < Tag::kRnrRetryInfinite + 3; ++i) {
    tag->brnr().beginProbe();
    ASSERT_FALSE(tag->hasSpentRnrRetries())
        << "an unlimited allowance must survive probe " << i + 1
        << "; reading it as spent fails every queue pair that never set "
           "rnr_retry";
    tag->brnr().blockAt(kBlockedMsn);
  }
}

/*
 * The allowance is per RNR episode, not per queue pair lifetime. A receiver
 * that becomes ready ends the episode, and the probes it took to get there are
 * not charged against the next one -- otherwise a long-lived queue pair
 * accumulates across unrelated episodes and eventually fails a tag whose
 * receiver was answering fine.
 */
TEST_F(QpTest, BecomingReadyGivesTheTagAFreshRnrAllowance) {
  constexpr uint8_t kRetryLimit = 2;
  auto tag = qp.getTag(kMetaRoceInitialTag);
  tag->setRnrRetryLimit(kRetryLimit);

  // First episode: the tag probes up to its limit without exhausting it.
  constexpr uint16_t kBlockedMsn = 1;
  blockTagAfterProbes(*tag, kBlockedMsn, kRetryLimit - 1);
  ASSERT_FALSE(tag->hasSpentRnrRetries())
      << "precondition: the first episode stops one probe short of the limit";

  // The receiver posts a buffer, which is what ends the episode; the cancel is
  // the production path that acts on it.
  qp.cancelRnrBackoff(kMetaRoceInitialTag);

  // Second episode: the same number of probes again.
  blockTagAfterProbes(*tag, kBlockedMsn, kRetryLimit - 1);

  EXPECT_FALSE(tag->hasSpentRnrRetries())
      << "the second episode gets the whole allowance; carrying the first "
         "episode's probes over fails a tag whose receiver did become ready";
}

/*
 * Spending an allowance is not a state: a tag configured for no retries at all
 * reads as spent from the moment it is created, before any receiver has said
 * it is not ready. The sweep must act on the tags the backoff timer found
 * blocked, not on every tag whose arithmetic happens to be exhausted.
 */
TEST_F(QpTest, ATagThatWasNeverBlockedSurvivesAnotherTagsExhaustion) {
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  qp.addTag(kOtherTag, &cq, &otherIbvQpStorage);

  auto blockedTag = qp.getTag(kMetaRoceInitialTag);
  auto idleTag = qp.getTag(kOtherTag);
  // Zero is a legal rnr_retry: this receiver gets no second chance. The tag is
  // healthy and has never been told a receiver was unready.
  idleTag->setRnrRetryLimit(0);
  blockedTag->setRnrRetryLimit(1);
  ASSERT_TRUE(idleTag->hasSpentRnrRetries())
      << "a zero allowance reads as spent with no probes made, which is "
         "exactly why spending one cannot stand in for being blocked";
  ASSERT_EQ(idleTag->getBrnrState(), BrnrState::NORMAL_TX)
      << "the idle tag must not be in a receiver-not-ready state, or it is not "
         "the bystander this test needs";

  std::array<char, 4096> payload{};
  Wqe* idleWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kOtherTag,
       .workReqId = 200,
       .address = payload.data()});
  ASSERT_NE(idleWqe, nullptr)
      << "the bystander needs work of its own for its loss to be visible";
  Wqe* blockedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(blockedWqe, nullptr)
      << "the blocked tag needs work for its receiver to be unready for";

  // Only this tag has been told its receiver has no buffer, and it has spent
  // the one chance it was given.
  ASSERT_TRUE(blockedWqe->getMsn().has_value())
      << "the tag records the blocked message by msn, so it must have one";
  blockTagAfterProbes(*blockedTag, blockedWqe->getMsn().value(), 1);

  std::ignore = qp.processRnrRetryExceeded();

  EXPECT_TRUE(blockedTag->isInErrorState())
      << "the tag whose receiver never became ready must enter the error "
         "state";
  EXPECT_FALSE(idleTag->isInErrorState())
      << "a tag that was never told a receiver was unready must survive, "
         "however small the allowance it was configured with";
  EXPECT_EQ(idleTag->getNumPendingWqes(), 1u)
      << "the bystander must keep its work request";
  EXPECT_EQ(cq.getNumCompletedWqes(), 1u) << "only the blocked tag may report";
}

/*
 * A tag is one of several sharing a QP and its tx flows. A fatal NAK carries
 * one of them in its Tag field, and the work the others have in flight must
 * survive it.
 */
TEST_F(QpTest, AFatalNakErrorsOnlyTheTagItCarries) {
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  qp.addTag(kOtherTag, &cq, &otherIbvQpStorage);

  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* rejectedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  Wqe* bystanderWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kOtherTag,
       .workReqId = 200,
       .address = payload.data()});
  ASSERT_NE(rejectedWqe, nullptr)
      << "the tag the nak carries must have work of its own to fail";
  ASSERT_NE(bystanderWqe, nullptr)
      << "without work on the other tag this test has no survivor to check";

  // Both tags have a packet in flight on the shared flow.
  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 2;
  txState.tracker.emplace_back(
      PacketId(
          rejectedWqe,
          kMetaRoceInitialTag,
          rejectedWqe->allocateNextPosn(),
          mtu),
      Flow::FsnState::UNACKED);
  txState.tracker.emplace_back(
      PacketId(bystanderWqe, kOtherTag, bystanderWqe->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  const AckHeader nak =
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setCumulRespDeliveredMsn(rejectedWqe->getMsn().value())
          .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
          .setTimestamp(0)
          .build();
  flow->processNakFatalError(nak);

  EXPECT_TRUE(rejectedWqe->isFailing())
      << "the work request on the tag the peer rejected must be failing";
  EXPECT_FALSE(bystanderWqe->isFailing())
      << "another tag's work must survive a failure on a tag it merely shares "
         "a wire with";
  EXPECT_NE(otherIbvQpStorage.state, IBV_QPS_ERR)
      << "the other tag's qp must not be taken down for a failure on a tag it "
         "merely shares a wire with";
  EXPECT_EQ(qp.getTag(kOtherTag)->getNumPendingWqes(), 1u)
      << "the other tag's work request must still be pending, not flushed";
}

/*
 * A responder NAKs the frames of a failed message as they reach it, so one tag
 * can draw several naks. Only the first has work to fail. Several things
 * enforce that between them -- the early return on the tag's state, and the
 * checks that pass over a work request already failing -- so this pins the
 * outcome the client sees rather than any one of them.
 */
TEST_F(QpTest, ASecondFatalNakForTheSameTagReportsNothingFurther) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* wqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(wqe, nullptr)
      << "the tag the nak carries must have work of its own to fail";

  // Two frames of the message are on the wire, so the responder has two to NAK.
  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 2;
  txState.tracker.emplace_back(
      PacketId(wqe, kMetaRoceInitialTag, wqe->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);
  txState.tracker.emplace_back(
      PacketId(wqe, kMetaRoceInitialTag, wqe->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  const auto nakForFsn = [&](uint16_t fsn) {
    return AckHeader::AckHeaderBuilder()
        .setTag(kMetaRoceInitialTag)
        .setCumulativeFsn(fsn)
        .setMaxRcvdFsn(fsn)
        .setCumulRespDeliveredMsn(wqe->getMsn().value())
        .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
        .setTimestamp(0)
        .build();
  };

  flow->processNakFatalError(nakForFsn(0));
  ASSERT_TRUE(wqe->isFailing())
      << "precondition: the first nak fails the work request at its msn";
  const auto statusAfterFirstNak = wqe->getCompletionStatusOverride();
  const int completionsAfterFirstNak = cq.getNumCompletedWqes();

  flow->processNakFatalError(nakForFsn(1));

  EXPECT_EQ(cq.getNumCompletedWqes(), completionsAfterFirstNak)
      << "a nak for a tag already in error must report nothing further; a "
         "second completion tells the client twice about one work request";
  EXPECT_EQ(wqe->getCompletionStatusOverride(), statusAfterFirstNak)
      << "the status the first nak recorded is the one reported to the client";
}

/*
 * A failure confined to one tag leaves the shared flow able to carry another
 * tag's frame: the flow keeps its tracker and hands out the surviving tag's
 * remaining work when it is next pumped.
 */
TEST_F(QpTest, ATagFailureLeavesAnotherTagSendingOnTheSharedFlow) {
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  qp.addTag(kOtherTag, &cq, &otherIbvQpStorage);

  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 2 * 4096> payload{};
  Wqe* rejectedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  // Two frames, so one can be in flight while the other is still to send.
  Wqe* bystanderWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(2 * mtu),
       .tagNum = kOtherTag,
       .workReqId = 200,
       .address = payload.data()});
  ASSERT_NE(rejectedWqe, nullptr)
      << "precondition: the nak'd tag has work, which is what the nak below "
         "is meant to fail";
  ASSERT_NE(bystanderWqe, nullptr)
      << "precondition: a second tag has work of its own, which is what the "
         "nak must leave alone";
  const uint16_t bystanderMsn = bystanderWqe->getMsn().value();

  // Both tags have a frame in flight on the shared flow.
  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 2;
  txState.tracker.emplace_back(
      PacketId(
          rejectedWqe,
          kMetaRoceInitialTag,
          rejectedWqe->allocateNextPosn(),
          mtu),
      Flow::FsnState::UNACKED);
  txState.tracker.emplace_back(
      PacketId(bystanderWqe, kOtherTag, bystanderWqe->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  const AckHeader nak =
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setCumulRespDeliveredMsn(rejectedWqe->getMsn().value())
          .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
          .setTimestamp(0)
          .build();
  flow->processNakFatalError(nak);

  ASSERT_TRUE(rejectedWqe->isFailing())
      << "the tag the nak carries must have failing work for this to be "
         "testing the tags beside it at all";
  ASSERT_FALSE(bystanderWqe->isFailing())
      << "the nak carried one tag, so the work of the tag beside it must be "
         "left running";

  auto egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "the surviving tag still has a frame to send on this flow; a failure "
         "confined to one tag must leave the flow able to carry it";
  const auto* packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  ASSERT_NE(packet, nullptr)
      << "the packet the flow carried must be data, or there is no msn below "
         "to check";
  EXPECT_EQ(packet->getDataHeader().getMsn(), bystanderMsn)
      << "the frame on the wire must be the surviving tag's, not the failed "
         "tag's work being retransmitted";
}

TEST_F(QpTest, TakingTheFlowsOutOfServiceEndsHrnrBackoff) {
  constexpr uint16_t kAlwaysOpenWindow = 4096;
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(kAlwaysOpenWindow));
  qp.setCongestionController(std::move(congestionController));

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* inFlightWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(inFlightWqe, nullptr)
      << "precondition: the tag needs work, or no frame is in flight";

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          inFlightWqe,
          kMetaRoceInitialTag,
          inFlightWqe->allocateNextPosn(),
          mtu),
      Flow::FsnState::UNACKED);

  qp.setHrnrState(HrnrState::HRNR_BLOCKED_TX);
  initialTag->markForDeletion();
  qp.notifyTagLeftService();

  qp.getNextPacketToSend();

  EXPECT_EQ(qp.getHrnrState(), HrnrState::NORMAL_TX)
      << "the visit that takes the flows out of service ends the HRNR "
         "backoff, which no probe can end once the flows send nothing";
}

/*
 * Tags share the QP's flows, so one failing leaves them up for the others. A
 * QP whose last tag has failed has nobody left to carry work for, and keeping
 * the flows in service costs it retransmission timers and congestion state for
 * work that will never be sent. The frames already on the wire are not given
 * up here: they are still the retransmission scan's to resolve.
 */
TEST_F(QpTest, TheLastTagFailingTakesTheQpsFlowsOutOfService) {
  constexpr uint16_t kAlwaysOpenWindow = 4096;
  auto congestionController = std::make_unique<MockCongestionController>();
  auto* rawCongestionController = congestionController.get();
  EXPECT_CALL(*rawCongestionController, getAvailableWindowSize(_))
      .WillRepeatedly(Return(kAlwaysOpenWindow));
  qp.setCongestionController(std::move(congestionController));

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* rejectedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(rejectedWqe, nullptr)
      << "precondition: the QP's only tag has work, or there is nothing for "
         "the nak below to fail";

  // In flight, so the tracker has an entry that must survive the flows going
  // out of service.
  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          rejectedWqe,
          kMetaRoceInitialTag,
          rejectedWqe->allocateNextPosn(),
          mtu),
      Flow::FsnState::UNACKED);

  const AckHeader nak =
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setCumulRespDeliveredMsn(rejectedWqe->getMsn().value())
          .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
          .setTimestamp(0)
          .build();
  flow->processNakFatalError(nak);

  EXPECT_CALL(*rawCongestionController, onFlowDisabled(0)).Times(1);

  // The sender's visit, which is where the QP notices it has no tag left.
  std::ignore = qp.getNextPacketToSend();

  EXPECT_EQ(txState.tracker.size(), 1u)
      << "taking the flows out of service must not clear the tracker; the "
         "frame on the wire is still unresolved, and clearing it here "
         "would retire a work request whose packet may still be read";
}

/*
 * Two routes can fail the same tag. The receive path publishes the error state
 * and flushes only its receives, leaving the sends for the sender's sweep; a
 * fatal NAK arriving in that window is the only thing that knows what the peer
 * actually objected to. Turning it away because the tag is already in error
 * hands the client a flush where the peer gave a reason.
 */
TEST_F(QpTest, AFatalNakStillReportsItsStatusOnATagErroredByTheReceivePath) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* sendWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(sendWqe, nullptr)
      << "precondition: the tag has a send, or there is no work for the nak's "
         "status to reach";

  // What RxFlow::enterTagErrorAndFlushReceives leaves behind: the tag in
  // error, its sends untouched.
  {
    auto tag = qp.getTag(kMetaRoceInitialTag);
    auto tagGuard = tag->scopedLock();
    std::ignore = qp.enterTagErrorWithTagHeld(*tag);
  }
  ASSERT_FALSE(sendWqe->isFailing())
      << "precondition: the receive path leaves the send unmarked, which is "
         "the window this case is about";

  const AckHeader nak =
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setCumulRespDeliveredMsn(sendWqe->getMsn().value())
          .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
          .setTimestamp(0)
          .build();
  flow->processNakFatalError(nak);

  ASSERT_EQ(cq.getNumCompletedWqes(), 1u)
      << "the send has no frame on the wire, so the nak settles it at once";
  const auto cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.status, kWrRemAccErr)
      << "the peer reported a remote access error; reporting the sweep's flush "
         "instead tells the client its send was abandoned rather than refused";
}

/*
 * A fatal NAK errors the whole tag, so the msn it blames decides how much work
 * one packet can fail. An msn this side never assigned belongs to no message
 * of ours, and acting on it would let a peer fail every work request a tag
 * holds by sending a number. The msn window is the test: anything at or past
 * the next msn to assign was never issued.
 */
TEST_F(QpTest, AFatalNakForAnMsnWeNeverIssuedIsDiscarded) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* sendWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(sendWqe, nullptr);

  auto tag = qp.getTag(kMetaRoceInitialTag);
  const uint16_t neverIssued = tag->getNextMsn();
  ASSERT_TRUE(
      sendWqe->getMsn().has_value() && before(*sendWqe->getMsn(), neverIssued))
      << "precondition: the tag must hold work at an msn other than the "
         "nak's, or there is no untouched send for the discard to leave";

  const AckHeader nak =
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setCumulRespDeliveredMsn(neverIssued)
          .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
          .setTimestamp(0)
          .build();
  flow->processNakFatalError(nak);

  EXPECT_FALSE(tag->isInErrorState())
      << "one packet for a message this side never sent must not take the "
         "tag down with every work request on it";
  EXPECT_FALSE(sendWqe->isFailing())
      << "the posted send is untouched: the nak was never for it";
  EXPECT_EQ(cq.getNumCompletedWqes(), 0u)
      << "nothing failed, so nothing reports";
}

TEST_F(QpTest, AFatalNakForAReadResponseFailsTheResponderTag) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  // A response carries the initiator's msn, which this side's own sends never
  // reach.
  const uint16_t initiatorMsn = initialTag->getNextMsn();
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(receiveBuffer.data()), mrKey, mtu);
  int addressError = -1;
  initialTag->createVirtualSendWqeIfNeeded(
      initiatorMsn, rwHeader, &addressError, mrs, mtu);
  ASSERT_EQ(addressError, 0)
      << "precondition: the read's region resolves, or no response is built";
  Wqe* const response = initialTag->findNextWqe();
  ASSERT_NE(response, nullptr);
  ASSERT_TRUE(response->isVirtual())
      << "precondition: the frame on the wire is a read response";

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          response, kMetaRoceInitialTag, response->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);
  initialTag->markFree();

  const AckHeader nak = AckHeader::AckHeaderBuilder()
                            .setTag(kMetaRoceInitialTag)
                            .setCumulativeFsn(0)
                            .setMaxRcvdFsn(0)
                            .setCumulRespDeliveredMsn(initiatorMsn)
                            .setAckStatus(AckStatus::NAK_OPERATIONAL_ERROR)
                            .setTimestamp(0)
                            .build();
  flow->processNakFatalError(nak);

  EXPECT_TRUE(initialTag->isInErrorState())
      << "the requester rejected a response this tag sent, so the tag enters "
         "the error state rather than discarding the nak as one for a message "
         "it never issued";
}

TEST_F(QpTest, AFatalNakNamingAnotherTagThanTheResponseIsCheckedAsThatTags) {
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  qp.addTag(kOtherTag, &cq, &otherIbvQpStorage);
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(receiveBuffer.data()), mrKey, mtu);
  int addressError = -1;
  initialTag->createVirtualSendWqeIfNeeded(
      initialTag->getNextMsn(), rwHeader, &addressError, mrs, mtu);
  ASSERT_EQ(addressError, 0)
      << "precondition: the read's region resolves, or no response is built";
  Wqe* const response = initialTag->findNextWqe();
  ASSERT_NE(response, nullptr);
  ASSERT_TRUE(response->isVirtual())
      << "precondition: the frame on the wire is a read response";

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          response, kMetaRoceInitialTag, response->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);
  initialTag->markFree();

  auto otherTag = qp.getTag(kOtherTag);
  const AckHeader nak = AckHeader::AckHeaderBuilder()
                            .setTag(kOtherTag)
                            .setCumulativeFsn(0)
                            .setMaxRcvdFsn(0)
                            .setCumulRespDeliveredMsn(otherTag->getNextMsn())
                            .setAckStatus(AckStatus::NAK_OPERATIONAL_ERROR)
                            .setTimestamp(0)
                            .build();
  flow->processNakFatalError(nak);

  EXPECT_FALSE(otherTag->isInErrorState())
      << "the frame at the cfsn is another tag's response, so this nak is for "
         "an msn the named tag never issued, and one bad packet must not take "
         "that tag down";
  EXPECT_FALSE(initialTag->isInErrorState())
      << "the nak names a different tag, so it is no rejection of the response "
         "this tag still has outstanding";
}

TEST_F(QpTest, AFatalNakForAReadResponseSelectsNoSendByTheInitiatorsMsn) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  // The second local send below takes this msn too.
  const uint16_t initiatorMsn = initialTag->getNextMsn() + 1;
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(receiveBuffer.data()), mrKey, mtu);
  int addressError = -1;
  initialTag->createVirtualSendWqeIfNeeded(
      initiatorMsn, rwHeader, &addressError, mrs, mtu);
  ASSERT_EQ(addressError, 0)
      << "precondition: the read's region resolves, or no response is built";
  Wqe* const response = initialTag->findNextWqe();
  ASSERT_NE(response, nullptr);
  ASSERT_TRUE(response->isVirtual())
      << "precondition: the frame on the wire is a read response";

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          response, kMetaRoceInitialTag, response->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);
  initialTag->markFree();

  std::array<char, 64> payload{};
  Wqe* const earlierSend = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .workReqId = 100,
       .address = payload.data()});
  Wqe* const sendSharingTheMsn = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .workReqId = 101,
       .address = payload.data()});
  ASSERT_NE(earlierSend, nullptr);
  ASSERT_NE(sendSharingTheMsn, nullptr);
  ASSERT_EQ(sendSharingTheMsn->getMsn(), initiatorMsn)
      << "precondition: a local send shares the response's msn, or nothing "
         "here could select it by that msn";

  const AckHeader nak = AckHeader::AckHeaderBuilder()
                            .setTag(kMetaRoceInitialTag)
                            .setCumulativeFsn(0)
                            .setMaxRcvdFsn(0)
                            .setCumulRespDeliveredMsn(initiatorMsn)
                            .setAckStatus(AckStatus::NAK_OPERATIONAL_ERROR)
                            .setTimestamp(0)
                            .build();
  flow->processNakFatalError(nak);

  // Neither send has a frame in flight, so both complete during the nak and
  // are gone; their statuses are read from the completion queue.
  ASSERT_EQ(cq.getNumCompletedWqes(), 2u)
      << "both sends fail with the tag, one reporting and one flushing";
  std::map<uint64_t, WorkCompletionStatus> statusByWorkReqId;
  for (int i = 0; i < 2; ++i) {
    const auto cqe = folly::coro::blockingWait(cq.pollCq());
    statusByWorkReqId.emplace(cqe.workReqId, cqe.status);
  }
  EXPECT_EQ(statusByWorkReqId.at(100), kWrRemOpsErr)
      << "with no local send named, the earliest one reports the rejection";
  EXPECT_EQ(statusByWorkReqId.at(101), kWrFlushErr)
      << "the initiator's msn says nothing about this side's sends, so the "
         "send that shares it only flushes behind the earliest";
}

/*
 * HRNR is the whole QP's backoff, entered because the responder said it has no
 * receives at all. One tag being rejected does not show that, and leaving
 * the backoff here would let the other tags transmit again without the
 * retransmission queues that coming out of it is paired with.
 */
TEST_F(QpTest, AFatalNakLeavesTheQpsHrnrBackoffAlone) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* rejectedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(rejectedWqe, nullptr)
      << "the tag the nak carries must have work of its own to fail";

  // In flight, so the work request is still there to be read once the nak has
  // been handled rather than retired as it settles.
  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          rejectedWqe,
          kMetaRoceInitialTag,
          rejectedWqe->allocateNextPosn(),
          mtu),
      Flow::FsnState::UNACKED);

  qp.setHrnrState(HrnrState::HRNR_BLOCKED_TX);

  const AckHeader nak =
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setCumulRespDeliveredMsn(rejectedWqe->getMsn().value())
          .setAckStatus(AckStatus::NAK_ADDRESS_INVALID_ACCESS_ERROR)
          .setTimestamp(0)
          .build();
  flow->processNakFatalError(nak);

  ASSERT_TRUE(rejectedWqe->isFailing())
      << "the tag the nak carries must have failing work, or the backoff "
         "survives only because no work moved at all";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "the responder's backoff covers every tag on this qp; a failure on "
         "one of them must not release the rest from waiting it out";
}

/*
 * Abandoned frames leave the wire with no ack, so nothing tells congestion
 * control they are gone. Left counted in flight they hold their share of the
 * flow window for good, and the window is shared: the tags still working lose
 * that capacity permanently.
 */
TEST_F(QpTest, AbandonedFramesAreReturnedToTheCongestionWindow) {
  constexpr int kInFlight = 4;
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED},
      /*sndCur=*/10);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  auto* const wqe = fixture.wqe.get();
  auto* const rawCongestionController = fixture.congestionController;

  uint32_t reportedAbandoned = 0;
  uint32_t reportedZeroRtt = 0;
  ON_CALL(*rawCongestionController, onPacketAck(_, _))
      .WillByDefault([&](uint16_t, const CongestionController::AckEvent& ack) {
        reportedAbandoned += ack.numAckedPackets;
        if (ack.measuredRttNs == 0) {
          reportedZeroRtt++;
        }
      });

  wqe->markFailing(kWcRetryExcErr);
  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  ASSERT_EQ(txState.sndUna, txState.sndNxt)
      << "the frames must have been given up on for this to say anything "
         "about how they were accounted";
  EXPECT_EQ(reportedAbandoned, kInFlight)
      << "every frame given up on must count against what congestion control "
         "believes is in flight: no ack is coming for it, so this is the only "
         "thing that returns its share of the shared flow window to the tags "
         "still sending";
  EXPECT_EQ(reportedZeroRtt, 1u)
      << "the report must carry no round-trip sample: no reply arrived, and a "
         "made-up one would feed the estimator off the back of a failure";
}

/*
 * A frame BRNR'd while its message is still live has its posn released and is
 * accounted against the flow window there and then, while its tracker entry
 * stays. Giving up on the message afterward must not account for it a second
 * time when the frame retires, or the flow's window grows on the back of a
 * failure.
 */
TEST_F(QpTest, ABrnrdFrameAbandonedLaterIsAccountedOnce) {
  // Two frames, so the BRNR'd one sits behind an unacked hole and stays in
  // the tracker for the retransmission scan to reach. A BRNR at the head is
  // retired by the ack that carries it and is never scanned.
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED, Flow::FsnState::UNACKED},
      /*sndCur=*/10);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  auto* const wqe = fixture.wqe.get();
  auto* const rawCongestionController = fixture.congestionController;

  uint32_t reportedAbandoned = 0;
  ON_CALL(*rawCongestionController, onPacketAck(_, _))
      .WillByDefault([&](uint16_t, const CongestionController::AckEvent& ack) {
        reportedAbandoned += ack.numAckedPackets;
      });

  // The message is still live, so the BRNR releases the second frame's posn
  // for a resend and accounts for it against the window there and then.
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(1);
  const AckHeader brnrAck = AckHeader::AckHeaderBuilder()
                                .setCumulativeFsn(9)
                                .setMaxRcvdFsn(11)
                                .setAckStatus(AckStatus::BRNR)
                                .setRnrBitVector(rnrBitVector)
                                .setRnrBackoffTimeExp(0)
                                .setTimestamp(0)
                                .build();
  flow->processAckPacket(
      brnrAck, simulatedProcessedAtNs(brnrAck.getTimestamp()));

  ASSERT_EQ(txState.tracker.size(), 2u)
      << "the BRNR'd frame must still be in the tracker behind the hole, or "
         "the scan below has nothing to reach and this proves nothing";
  ASSERT_EQ(reportedAbandoned, 1u)
      << "the BRNR must account for the frame once it releases the posn for "
         "a resend, which is the precondition this test is about";

  // Now abandon the message and drive the retransmission scan over it.
  // The BRNR queued a fast retransmit; drain it so the scan below is not
  // short-circuited by a non-empty retransmission queue.
  while (flow->getNextPacketToSend() != nullptr) {
  }

  wqe->markFailing(kWcRetryExcErr);
  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  ASSERT_TRUE(txState.tracker.empty())
      << "both frames must have left the tracker for this to say anything "
         "about how the window accounted for them";
  EXPECT_EQ(reportedAbandoned, 2u)
      << "the hole is abandoned and accounted for, but the BRNR'd frame was "
         "accounted for when its posn was released, so abandoning the message "
         "must not count it again; a third report would inflate the window the "
         "tags still sending have to share";
}

/*
 * A failed read is answered by nothing: its response positions are filled by
 * the receive side, which the tag's error state has already shut off. The one
 * frame it put on the wire is all that is left to account for, so the ack for
 * that frame is what lets it report.
 */
TEST_F(QpTest, AFailingReadReportsWhenItsRequestFrameIsAcked) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto payload = std::make_unique<char[]>(2 * mtu);
  Wqe* const wqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = static_cast<uint32_t>(2 * mtu),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 77,
       .address = payload.get(),
       .remote_addr = 0x1000,
       .rkey = 42});
  ASSERT_NE(wqe, nullptr)
      << "the post has to reach the tag, or the ack path below has nothing to "
         "settle";

  // No tracker entry: this drives the ack path directly, and an entry left
  // behind would point to a wqe the completion below retires.
  const uint32_t posn = wqe->allocateNextPosn();

  {
    // The discipline every production failure path uses.
    auto tagGuard = initialTag->scopedLock();
    auto retirementGuard = initialTag->lockForWqeRetirement();
    initialTag->getIbvQp().state = IBV_QPS_ERR;
    initialTag->markPendingWqesFailing(kWrRemAccErr);
  }

  ASSERT_EQ(cq.getNumCompletedWqes(), 0u)
      << "marking must not report on its own; the request is still on the "
         "wire and can be retransmitted";

  qp.checkForTxMessageCompletion(
      {PacketId(wqe, kMetaRoceInitialTag, posn, 0 /*sizeBytes*/)});

  ASSERT_EQ(cq.getNumCompletedWqes(), 1u)
      << "with its request acked nothing further can arrive for a failed "
         "read, so it must report rather than wait for responses that the "
         "tag's error state has already shut off";
  const auto cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.workReqId, 77u)
      << "the read that failed must be the one reported";
  EXPECT_EQ(cqe.status, kWrRemAccErr)
      << "it must carry the error that put the tag in error rather than a "
         "success";
  EXPECT_EQ(initialTag->getNumPendingWqes(), 0)
      << "a reported work request must be retired, not held until teardown";
}

/*
 * A tag can hold a work request it has already reported: a read kept only
 * until its request frame is acked. The watchdog's search finds no work to
 * fail there, and the expiry has already taken the flows down, so a tag left
 * out of error would keep accepting posts no flow will ever send.
 */
TEST_F(QpTest, TheWatchdogErrorsATagHoldingOnlyAReportedRead) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto payload = std::make_unique<char[]>(mtu);
  Wqe* const read = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 88,
       .address = payload.get(),
       .remote_addr = 0x1000,
       .rkey = 42});
  ASSERT_NE(read, nullptr)
      << "the post has to reach the tag, or the expiry below acts on an empty "
         "tag and the case says nothing";

  ASSERT_EQ(read->allocateNextPosn(), Wqe::kRequestFrameWirePosn)
      << "precondition: a read puts one frame on the wire, and it is the request frame";
  // The response lands and reports the read; the ack for its request frame
  // never arrives, which is what the watchdog is for.
  read->onPosnReceived(0);
  {
    auto retirementGuard = initialTag->lockForWqeRetirement();
    EXPECT_TRUE(initialTag->handleTxWqesCompletion({*read}))
        << "precondition: completing the send finds no csn out of sequence";
  }
  ASSERT_EQ(cq.getNumCompletedWqes(), 1u)
      << "precondition: the read reports on its response, which is what leaves "
         "the tag holding a work request already reported";
  ASSERT_FALSE(initialTag->isInErrorState())
      << "precondition: the tag is healthy going in, or the expiry below "
         "cannot be what put it in error";

  qp.processWatchdogExpiry();

  EXPECT_TRUE(initialTag->isInErrorState())
      << "acks have stopped and the flows are down, so the tag must be errored "
         "rather than left to accept work that will never be sent";
}

/*
 * The same read, before its request settles. Nothing may report while the
 * frame is still allocated, because the retransmission scan can still pick it
 * up and the tracker still points to the wqe.
 */
TEST_F(QpTest, AFailingReadIsHeldUntilItsRequestFrameSettles) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto payload = std::make_unique<char[]>(2 * mtu);
  Wqe* const wqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = static_cast<uint32_t>(2 * mtu),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 78,
       .address = payload.get(),
       .remote_addr = 0x1000,
       .rkey = 42});
  ASSERT_NE(wqe, nullptr)
      << "the post has to reach the tag, or the ack path below has nothing to "
         "settle";
  std::ignore = wqe->allocateNextPosn();

  {
    auto tagGuard = initialTag->scopedLock();
    auto retirementGuard = initialTag->lockForWqeRetirement();
    initialTag->getIbvQp().state = IBV_QPS_ERR;
    initialTag->markPendingWqesFailing(kWrRemAccErr);
    initialTag->completeSettledFailedWqes();
  }

  EXPECT_EQ(cq.getNumCompletedWqes(), 0u)
      << "the completion sweep must pass over a read whose request frame has not "
         "settled; it can still be retransmitted, so the read is not free of "
         "the wire yet";
  EXPECT_EQ(initialTag->getNumPendingWqes(), 1)
      << "it must still be held for the frame that has not settled";
}

/*
 * The other way a failed read's request leaves the wire: the sender abandons
 * the frame instead of the peer acking it. The same settling rule applies.
 */
TEST_F(QpTest, AnRtoAbandonsAFailingReadsRequestAndReportsItOnce) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto* const flow = qp.getTxFlowById(0);
  auto& txState = flow->getTxState();

  auto payload = std::make_unique<char[]>(2 * mtu);
  Wqe* const wqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = static_cast<uint32_t>(2 * mtu),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 79,
       .address = payload.get(),
       .remote_addr = 0x1000,
       .rkey = 42});
  ASSERT_NE(wqe, nullptr)
      << "the post has to reach the tag, or the timeout below has no read "
         "to abandon";

  const uint32_t posn = wqe->allocateNextPosn();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.maxAckdFsn = 9;
  txState.sndNxt = 11;
  txState.tracker.emplace_back(
      PacketId(wqe, kMetaRoceInitialTag, posn, 0 /*sizeBytes*/),
      Flow::FsnState::UNACKED);

  {
    auto tagGuard = initialTag->scopedLock();
    auto retirementGuard = initialTag->lockForWqeRetirement();
    initialTag->getIbvQp().state = IBV_QPS_ERR;
    initialTag->markPendingWqesFailing(kWrRemAccErr);
  }

  // Checked before the scan retires the wqe, which frees it.
  EXPECT_EQ(wqe->getReceivedPosnCount(), 0u)
      << "the positions a response would have filled are receive-owned; the "
         "abandonment must leave them to the side that writes them";
  EXPECT_EQ(wqe->getOutstandingPosnCount(), 2u)
      << "the response positions stay outstanding, so only the request frame "
         "decides when the read has settled";

  const auto retxCount = flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  EXPECT_EQ(retxCount, 0) << "a failing read must not go back on the wire";
  EXPECT_TRUE(txState.tracker.empty())
      << "the abandoned frame must leave the tracker, which otherwise keeps "
         "a pointer to a wqe that has been retired";
  ASSERT_EQ(cq.getNumCompletedWqes(), 1u)
      << "abandoning the request settles the last thing the read was waiting "
         "on, so the scan's completion sweep must report it exactly once";
  const auto cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.workReqId, 79u)
      << "the read that failed must be the one reported";
  EXPECT_EQ(cqe.status, kWrRemAccErr)
      << "it must carry the error the tag failed with";
  EXPECT_EQ(initialTag->getNumPendingWqes(), 0)
      << "the reported read must be retired rather than held to teardown";
}

/*
 * Completions follow posting order. A failed read whose request has settled is
 * ready to report, but an earlier work request still has a packet on the wire,
 * so the read has to wait behind it rather than answering out of order.
 */
TEST_F(QpTest, AFailingReadWaitsForAnEarlierWorkRequestBeforeReporting) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  std::array<char, 4096> sendPayload{};
  Wqe* const earlierSend = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = sendPayload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 80,
       .address = sendPayload.data()});
  auto readPayload = std::make_unique<char[]>(2 * mtu);
  Wqe* const laterRead = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = static_cast<uint32_t>(2 * mtu),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 81,
       .address = readPayload.get(),
       .remote_addr = 0x1000,
       .rkey = 42});
  ASSERT_NE(earlierSend, nullptr)
      << "both posts have to reach the tag, or their reporting order says "
         "nothing";
  ASSERT_NE(laterRead, nullptr)
      << "both posts have to reach the tag, or their reporting order says "
         "nothing";

  // Both still have a frame allocated, so marking alone settles neither.
  const uint32_t sendPosn = earlierSend->allocateNextPosn();
  const uint32_t readPosn = laterRead->allocateNextPosn();

  {
    auto tagGuard = initialTag->scopedLock();
    auto retirementGuard = initialTag->lockForWqeRetirement();
    initialTag->getIbvQp().state = IBV_QPS_ERR;
    initialTag->markPendingWqesFailing(kWrRemAccErr);
  }

  // The later read settles first.
  qp.checkForTxMessageCompletion(
      {PacketId(laterRead, kMetaRoceInitialTag, readPosn, 0 /*sizeBytes*/)});

  EXPECT_EQ(cq.getNumCompletedWqes(), 0u)
      << "the read has settled but an earlier work request has not reported "
         "yet; answering now would hand the client its completions out of the "
         "order it posted them";

  // The earlier one settles, releasing both.
  qp.checkForTxMessageCompletion(
      {PacketId(earlierSend, kMetaRoceInitialTag, sendPosn, mtu)});

  ASSERT_EQ(cq.getNumCompletedWqes(), 2u)
      << "once the earlier work request reports, the read behind it must "
         "follow rather than staying pending for good";
  const auto first = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(first.workReqId, 80u)
      << "the earlier posted work request must report first";
  const auto second = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(second.workReqId, 81u)
      << "the read must report after it, not before";
}

/*
 * A read that fails before its request reaches the wire has no frame to wait
 * on, so the marking alone settles it. It must still report behind the work
 * request posted ahead of it, and the sender must not pick it up to send a
 * request it has already abandoned.
 */
TEST_F(QpTest, AnUnsentFailingReadIsNotSentAndStillWaitsItsTurn) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  std::array<char, 4096> sendPayload{};
  Wqe* const earlierSend = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = sendPayload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 82,
       .address = sendPayload.data()});
  auto readPayload = std::make_unique<char[]>(2 * mtu);
  Wqe* const laterRead = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = static_cast<uint32_t>(2 * mtu),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 83,
       .address = readPayload.get(),
       .remote_addr = 0x1000,
       .rkey = 42});
  ASSERT_NE(earlierSend, nullptr)
      << "both posts have to reach the tag, or their reporting order says "
         "nothing";
  ASSERT_NE(laterRead, nullptr)
      << "both posts have to reach the tag, or their reporting order says "
         "nothing";

  // Only the earlier work request reaches the wire; the read's request frame
  // is left pending, which is the state under test.
  const uint32_t sendPosn = earlierSend->allocateNextPosn();

  {
    auto tagGuard = initialTag->scopedLock();
    auto retirementGuard = initialTag->lockForWqeRetirement();
    initialTag->getIbvQp().state = IBV_QPS_ERR;
    initialTag->markPendingWqesFailing(kWrRemAccErr);
  }

  // Released before the check so a failure reports rather than wedging the
  // Tag for everything after it.
  Wqe* const selected = initialTag->findNextWqe();
  if (selected != nullptr) {
    initialTag->markFree();
  }
  EXPECT_EQ(selected, nullptr)
      << "a request abandoned before it was sent must not be offered to the "
         "sender; sending it would put a read on the wire for a work request "
         "the client is about to be told failed";

  // Stands in for the sweep the tx path runs over the work of a failing
  // tag.
  const auto sweepFailedWqes = [this]() {
    auto tagGuard = initialTag->scopedLock();
    auto retirementGuard = initialTag->lockForWqeRetirement();
    initialTag->completeSettledFailedWqes();
  };

  sweepFailedWqes();
  EXPECT_EQ(cq.getNumCompletedWqes(), 0u)
      << "the read has nothing left to wait on, but the work request posted "
         "ahead of it has not reported, so answering now would hand the "
         "client its completions out of the order it posted them";

  // The earlier one settles, releasing both.
  qp.checkForTxMessageCompletion(
      {PacketId(earlierSend, kMetaRoceInitialTag, sendPosn, mtu)});
  sweepFailedWqes();

  ASSERT_EQ(cq.getNumCompletedWqes(), 2u)
      << "once the earlier work request reports, the read behind it must "
         "follow rather than staying pending for good";
  const auto first = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(first.workReqId, 82u)
      << "the earlier posted work request must report first";
  const auto second = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(second.workReqId, 83u)
      << "the read must report after it, not before";
}

/*
 * A tag enters error and the sender has not visited it yet to abandon its work
 * requests. An RTO in that window must abandon the frames rather than queue
 * them: no other check stops them going out before the visit.
 */
TEST_F(QpTest, AnRtoScanInTheFailureWindowQueuesNoRetransmission) {
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED, Flow::FsnState::UNACKED},
      /*sndCur=*/10,
      /*messageFrames=*/4);
  auto* const flow = fixture.flow;
  auto* const wqe = fixture.wqe.get();

  // No retransmission queued yet, so the scan is what this measures.
  ASSERT_EQ(flow->getNumPendingRetxPackets(), 0)
      << "a queued packet would let the scan return before it looked at the "
         "tag, which is what this test is about";

  {
    SenderWake tagErrorWake = SenderWake::NOT_NEEDED;
    {
      auto tagGuard = initialTag->scopedLock();
      tagErrorWake = qp.enterTagErrorWithTagHeld(*initialTag);
    }
    qp.wakeSenderAfterTagError(tagErrorWake);
  }
  ASSERT_FALSE(wqe->isFailing())
      << "the sender has not visited yet; this is the window under test";

  const auto retxCount = flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  EXPECT_EQ(retxCount, 0)
      << "an errored tag is about to have its work abandoned, so the scan "
         "must abandon its frames rather than queue them again";
  EXPECT_EQ(flow->getNumPendingRetxPackets(), 0)
      << "and no packet may be left waiting in the queue for a later visit";
}

/*
 * A retransmission queued before the message failed is still sitting in the
 * queue when the sender reaches it. Sending it puts a packet back on
 * the wire for a message it has just abandoned, and dropping the packet is not
 * enough on its own: the frame it carried stays outstanding, so the timer that
 * brings the scan to abandon it has to be armed again on the way out.
 */
TEST_F(QpTest, ARetransmissionQueuedBeforeTheFailureIsNotSent) {
  // The re-arming this test reads has to outlast the read.
  ScopedSuppressedTxTimers suppressTimers;
  TrackedMessageFixture fixture(
      arbiter, qp, mtu, initialTag, {Flow::FsnState::UNACKED}, /*sndCur=*/10);
  auto* const flow = fixture.flow;
  auto* const wqe = fixture.wqe.get();

  // A retransmission is prepared while the message is still healthy.
  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);
  ASSERT_EQ(flow->getNumPendingRetxPackets(), 1)
      << "the frame must be queued to resend before the message fails";

  wqe->markFailing(kWcRetryExcErr);

  auto* const callback = flow->retxTimerCallback_.get();
  retxTimer.cancelCallback(callback);
  ASSERT_FALSE(retxTimer.isScheduled(callback))
      << "the timer has to start disarmed, or the arming below shows nothing "
         "this test can read";

  auto egressPacket = flow->getNextPacketToSend();

  EXPECT_EQ(flow->getNumPendingRetxPackets(), 0)
      << "the queued packet must be taken off the queue either way";
  EXPECT_EQ(egressPacket, nullptr)
      << "it must not go out: the message it belongs to is failing, so the "
         "wire time buys no progress";
  EXPECT_TRUE(retxTimer.isScheduled(callback))
      << "and the timer must be armed again on the way out: its frame is still "
         "outstanding, and this scan is the only path that will abandon it";
}

/*
 * A retransmission queued while the tag was healthy is still in the queue
 * when the tag enters error. The tracker still calls its frame unacked,
 * so only the tag's state can stop it.
 */
TEST_F(QpTest, AQueuedRetransmissionIsDroppedOnceItsTagIsInError) {
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED, Flow::FsnState::UNACKED},
      /*sndCur=*/10,
      /*messageFrames=*/4);
  auto* const flow = fixture.flow;

  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);
  ASSERT_GT(flow->getNumPendingRetxPackets(), 0)
      << "the frames must be queued while the tag is healthy, or there is no "
         "queued packet for the failure to catch";

  {
    SenderWake tagErrorWake = SenderWake::NOT_NEEDED;
    {
      auto tagGuard = initialTag->scopedLock();
      tagErrorWake = qp.enterTagErrorWithTagHeld(*initialTag);
    }
    qp.wakeSenderAfterTagError(tagErrorWake);
  }

  auto egressPacket = flow->getNextPacketToSend();
  EXPECT_EQ(egressPacket, nullptr)
      << "the queued packet belongs to work the next visit abandons; sending "
         "it wastes the wire on a message the client is being told failed";

  // Dropping the packet leaves its frame outstanding, so the timer that fires
  // next is what has to resolve it.
  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  EXPECT_TRUE(fixture.txState->tracker.empty())
      << "the frames must retire off the window rather than sit there unacked; "
         "dropping the packet only helps if a later scan still closes them out";
}

/*
 * A tag can reach the error state without the sender having been told: the
 * sweep runs at the head of the visit, so a transition landing after it is not
 * seen until the next one. The work request the visit then hands back may have
 * frames in the tracker, and the tracker holds a raw pointer to it.
 */
TEST_F(QpTest, AnErroredTagsPartlySentWorkRequestOutlivesTheVisitThatFindsIt) {
  constexpr uint64_t kWrId = 0xF00D;
  ScopedSuppressedTxTimers noTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto* flow = qp.getTxFlowById(0);

  // Two packets, posted and sent the way production does so the tracker entry
  // and the unsent-posn count are both what the sender would have left behind.
  auto payload = std::make_unique<char[]>(2 * mtu);
  auto* const postedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(2 * mtu),
       .workReqId = kWrId,
       .address = payload.get()});
  ASSERT_NE(postedWqe, nullptr)
      << "the post has to have reached the tag, or the assertions below never "
         "reach the sender's branch for a tag in error";
  ASSERT_TRUE(flow->getNextPacketToSend())
      << "the first packet has to reach the wire, or there is no tracker entry "
         "pointing to this work request for the visit below to endanger";

  // Written without the notification that would have the sweep pick it up,
  // which is what a transition landing after the sweep leaves behind.
  initialTag->getIbvQp().state = IBV_QPS_ERR;

  EXPECT_FALSE(qp.getNextPacketToSend())
      << "the visit must hand back no packet for a tag it finds in error";

  ASSERT_EQ(qp.getNumPendingWqes(), 1)
      << "the work request still has a frame in the tracker, so retiring it "
         "here would leave that entry pointing into freed memory";
  EXPECT_TRUE(postedWqe->isFailing())
      << "it must be marked failing rather than sent, since its tag has failed";
  EXPECT_EQ(cq.getNumCompletedWqes(), 0)
      << "and it must not report while a frame of it is still outstanding";

  // The timer resolves the frame it was waiting on.
  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  auto& txState = flow->getTxState();
  EXPECT_TRUE(txState.tracker.empty())
      << "the frame has to leave the tracker before the work request is freed; "
         "that ordering is the whole point of not retiring it above";
  ASSERT_EQ(cq.getNumCompletedWqes(), 1)
      << "once no posn of it is outstanding the client has to hear about it";
  EXPECT_EQ(folly::coro::blockingWait(cq.pollCq()).workReqId, kWrId)
      << "and the completion has to report the work request that failed";
}

/*
 * A failing message is not retransmitted. Whether its packets are ever
 * acknowledged stops mattering once it is failing, so the sender abandons them
 * itself and the message reports instead of retrying until the watchdog fires.
 */
TEST_F(QpTest, RtoAbandonsAFailingWqesFrameInsteadOfRetransmitting) {
  // Half the message is on the wire; the rest has not been sent.
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED},
      /*sndCur=*/10,
      /*messageFrames=*/8);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  auto* const wqe = fixture.wqe.get();

  wqe->markFailing(kWcRetryExcErr);
  const auto retxCount = flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  EXPECT_EQ(retxCount, 0)
      << "a message that has been given up on must not go back on the wire; "
         "resending it would only delay the error the client is waiting for";
  EXPECT_EQ(flow->getNumPendingRetxPackets(), 0)
      << "no packet from a failing wqe may be queued for retransmission";
  EXPECT_EQ(txState.sndUna, txState.sndNxt)
      << "the sender must move its window past frames nothing will ever "
         "acknowledge, or the flow stalls on them forever";
  EXPECT_TRUE(txState.tracker.empty())
      << "a retired frame must leave the tracker, which otherwise keeps a "
         "pointer to a wqe that has already completed";
  EXPECT_EQ(wqe->getOutstandingPosnCount(), 0u)
      << "every posn must be accounted for so the message completes; leaving "
         "one outstanding is the hang this replaces";
}

/*
 * The sender retires the frames of a message it has given up on, but it is the
 * Tag that owns the work request and reports it to the client.
 */
TEST_F(QpTest, AnRtoOnAFailingTagReportsOneErrorAndRetiresTheWorkRequest) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto* const flow = qp.getTxFlowById(0);
  auto& txState = flow->getTxState();

  std::array<char, 4096> payload{};
  Wqe* const wqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(wqe, nullptr);

  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.maxAckdFsn = 9;
  txState.sndNxt = 11;
  txState.tracker.emplace_back(
      PacketId(wqe, kMetaRoceInitialTag, wqe->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  {
    // markPendingWqesFailing() is Tag-guarded state; the RTO path takes the
    // Tag itself, so the guard has to be gone before it runs.
    auto tagGuard = initialTag->scopedLock();
    initialTag->markPendingWqesFailing(kWcRetryExcErr);
  }

  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  ASSERT_EQ(cq.getNumCompletedWqes(), 1u)
      << "the tag reports exactly once for a work request it gave up on: "
         "none leaves the client waiting, and more than one is a double "
         "completion it cannot tell apart from a second request";
  const auto cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.workReqId, 100u)
      << "the answer must identify the work request that failed";
  EXPECT_EQ(cqe.status, kWcRetryExcErr)
      << "it must carry the error that put the tag in error, since a failing "
         "work request would otherwise report a success the message never "
         "achieved";
  EXPECT_EQ(initialTag->getNumPendingWqes(), 0)
      << "a reported work request must also be retired; leaving it pending "
         "holds its wqe and posns until the qp is torn down";
}

/*
 * A client erroring the whole QP has to wake the sender. The failing of that
 * work happens at the sender's next visit, and a QP whose posns are all
 * claimed has nothing else coming to make it ready, so an unsignalled
 * transition leaves the work for a visit that never arrives.
 */
TEST_F(QpTest, ErroringTheQpWakesTheSenderThatWillFailItsWork) {
  gflags::FlagSaver flagSaver;
  // The lock-free intake reaches the ready set only through a drain, and this
  // reads the set directly.
  FLAGS_libsoftmetaroce_lockfree_signal = false;

  ASSERT_FALSE(arbiter.isQpnReady(qp.getLocalQpNum()))
      << "precondition: the QP is not already ready, or the signal below "
         "cannot be told apart from what was there before";

  qp.setIbvQpState(IBV_QPS_ERR);

  EXPECT_TRUE(arbiter.isQpnReady(qp.getLocalQpNum()))
      << "the transition must put the QP back in the ready set; the sweep "
         "that fails its work only runs on a visit, and nothing else is "
         "coming to schedule one";
}

/*
 * The watchdog fires because acks have stopped, so a frame on the wire has no
 * ack coming to resolve it and the work holding it would never settle. It
 * abandons those frames itself. It does so while the work requests are still
 * here to point to: the tracker holds raw pointers to them, and the flush that
 * frees them runs after.
 */
TEST_F(QpTest, TheWatchdogAbandonsTheFramesOfTheWorkItFails) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* stalledWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(stalledWqe, nullptr);

  // In flight and unacked, which is the state the watchdog finds after the
  // peer has gone quiet.
  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          stalledWqe, kMetaRoceInitialTag, stalledWqe->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  qp.processWatchdogExpiry();

  EXPECT_EQ(cq.getNumCompletedWqes(), 1)
      << "the stalled work must report; leaving it unsettled waits on an ack "
         "the watchdog has already concluded is not coming";
  ASSERT_EQ(txState.tracker.size(), 1u)
      << "the entry must still be here for the checks below to read its "
         "state";
  EXPECT_TRUE(txState.tracker[0].isAbandoned())
      << "the frame must read as resolved, or snd_una stalls behind it and a "
         "late sack reaches a work request the flush has already freed";
}

/*
 * The sweep abandons a frame only of a work request already failing, and the
 * flush that follows frees every pending one. So a tag holding more than one
 * must have them all marked: a frame left unresolved points to a work request
 * that is gone, on a flow the watchdog leaves in service. Run under ASAN,
 * where the entry the sender would follow is a use-after-free.
 */
TEST_F(QpTest, TheWatchdogLeavesNoFrameOfASecondWorkRequestUnresolved) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  std::vector<Wqe*> stalled;
  for (uint64_t id : {100, 101}) {
    Wqe* wqe = postWqeHelper(
        &qp,
        {.workType = Wqe::WorkType::kSend,
         .msgSizeBytes = mtu,
         .tagNum = kMetaRoceInitialTag,
         .workReqId = id,
         .address = payload.data()});
    ASSERT_NE(wqe, nullptr);
    stalled.push_back(wqe);
  }

  // Both in flight and unacked: only the earliest is the one the timeout is
  // reported on, so the second is what the sweep must not leave behind.
  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = static_cast<uint16_t>(stalled.size());
  for (Wqe* wqe : stalled) {
    txState.tracker.emplace_back(
        PacketId(wqe, kMetaRoceInitialTag, wqe->allocateNextPosn(), mtu),
        Flow::FsnState::UNACKED);
  }

  qp.processWatchdogExpiry();

  EXPECT_EQ(cq.getNumCompletedWqes(), stalled.size())
      << "every stalled work request must report, not only the one carrying "
         "the timeout status";
  ASSERT_EQ(txState.tracker.size(), stalled.size())
      << "the entries must still be here for the checks below to read them";
  for (size_t i = 0; i < txState.tracker.size(); ++i) {
    EXPECT_TRUE(txState.tracker[i].isResolved())
        << "tracker entry " << i
        << " still points to a work request the flush freed, so the sender "
           "would follow it on a flow left in service";
  }
}

/*
 * A read its response has already completed is held in the map for its request
 * frame alone, which puts it behind the completion cursor the failing pass
 * walks from: that pass never reaches it, so it never reads as failing. Its
 * frame is still on the wire and no ack is coming for it, so the sweep gives up
 * on it by the same failing-or-served test `abandonFramesOfFailingWqes()`
 * uses. Left unresolved, the
 * entry holds `snd_una` behind it on a flow the watchdog leaves in service.
 */
TEST_F(QpTest, TheWatchdogResolvesTheFrameOfAReadItsResponseHasCompleted) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* heldRead = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 300,
       .address = payload.data()});
  ASSERT_NE(heldRead, nullptr);

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          heldRead, kMetaRoceInitialTag, heldRead->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  heldRead->onPosnReceived(0);
  {
    auto retirementGuard = initialTag->lockForWqeRetirement();
    EXPECT_TRUE(initialTag->handleTxWqesCompletion({*heldRead}))
        << "precondition: completing the send finds no csn out of sequence";
  }
  ASSERT_TRUE(initialTag->isTxCompletionCommitted(heldRead->getMsn().value()))
      << "precondition: the read must have reported on its response, or it is "
         "still pending and the failing pass reaches it like any other work";

  qp.processWatchdogExpiry();

  ASSERT_EQ(txState.tracker.size(), 1u)
      << "the entry must still be here for the check below to read it";
  EXPECT_TRUE(txState.tracker[0].isResolved())
      << "the frame must be given up on: nothing is going to ack it, and an "
         "entry left unacked holds snd_una behind it for good";
}

/*
 * The watchdog errors every tag of the QP, so a tag with no send of its own is
 * finished too: a tag in error takes no arriving packet, so nothing will fill
 * the receives it holds, and work posted to it afterwards could never leave. It
 * ends in the error state with its receives reported, like the tag whose send
 * stalled.
 */
TEST_F(QpTest, TheWatchdogErrorsATagThatOnlyHoldsReceives) {
  constexpr uint8_t kReceiveOnlyTag = kMetaRoceInitialTag + 1;
  qp.addTag(kReceiveOnlyTag, &cq, &otherIbvQpStorage);

  auto payload = std::make_unique<char[]>(mtu);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 70,
       .address = payload.get()});
  postInSequenceReceiveWqe(qp, receiveBuffer.data(), mtu, kReceiveOnlyTag);

  qp.processWatchdogExpiry();

  EXPECT_EQ(otherIbvQpStorage.state, IBV_QPS_ERR)
      << "a tag left running on a QP the watchdog has errored would accept "
         "work that is never going to be acknowledged";
  EXPECT_EQ(cq.getNumCompletedWqes(), 2)
      << "the stalled send reports its error and the receive is flushed; an "
         "unreported receive leaves the client waiting for a message that can "
         "no longer arrive";
}

/*
 * The sweep that fails an errored tag's work runs at the top of the sender's
 * visit, ahead of the returns that end it early. An rnr cancel is answered by
 * the first of those returns, and the tag still has its work to fail on
 * that visit all the same.
 */
TEST_F(QpTest, AVisitThatReturnsEarlyStillFailsAnErroredTagsWork) {
  // No window: the visit reaches its early return without building a packet.
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  std::array<char, 4096> payload{};
  Wqe* const wqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(wqe, nullptr)
      << "the tag needs work of its own for the sweep to have anything to do";

  SenderWake wake = SenderWake::NOT_NEEDED;
  {
    auto tagGuard = initialTag->scopedLock();
    wake = qp.enterTagErrorWithTagHeld(*initialTag);
  }
  qp.wakeSenderAfterTagError(wake);
  ASSERT_FALSE(wqe->isFailing())
      << "entering the error state must not touch posn state itself; the "
         "sender does that at its visit";

  // Answered by the earliest return in the visit, before it looks at any work.
  qp.scheduleRnrCancelPacket(
      kMetaRoceInitialTag, initialTag->getNextRxCsn() - 1);
  auto egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "the rnr cancel must be what this visit returns, or it is not the "
         "early return this test is about";
  ASSERT_NE(dynamic_cast<RnrCancelPacket*>(egressPacket->packet.get()), nullptr)
      << "the visit must have taken the rnr-cancel return";

  // The work request is failed, settled and retired by the sweep, so it is
  // read through the tag and the queue rather than through the pointer above.
  EXPECT_EQ(initialTag->getNumPendingWqes(), 0)
      << "the visit must still fail the errored tag's work; a sweep below "
         "the return this visit took would leave it pending";
  EXPECT_EQ(cq.getNumCompletedWqes(), 1u)
      << "nothing was on the wire, so failing the work request settles it at "
         "once and the client is told on this visit";
}

/*
 * The peer can cumulatively acknowledge a frame that was given up on, since
 * giving up is the sender's decision and the packet may well have arrived. The
 * frame is already resolved, and the work request behind it may have
 * completed and been freed, so the tracker entry must not be followed.
 */
TEST_F(QpTest, ACumulativeAckOverAnAbandonedFrameDoesNotFollowIt) {
  TrackedMessageFixture fixture(
      arbiter, qp, mtu, initialTag, {Flow::FsnState::ABANDONED}, /*sndCur=*/11);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  auto* const wqe = fixture.wqe.get();
  const uint32_t abandonedPosn = fixture.posns[0];

  wqe->markFailing(kWcRetryExcErr);
  wqe->abandonPosn(abandonedPosn);
  const uint32_t outstandingBefore = wqe->getOutstandingPosnCount();

  // The peer reports frame 10 as cumulatively received.
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(10)
                                  .setMaxRcvdFsn(10)
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(0)
                                  .build();
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  EXPECT_EQ(wqe->getOutstandingPosnCount(), outstandingBefore)
      << "a frame given up on must not be resolved a second time by a "
         "cumulative ack, which would drop the count below what is in flight";
  EXPECT_TRUE(txState.tracker.empty())
      << "the acknowledged frame must still leave the tracker";
  EXPECT_EQ(txState.sndUna, 11) << "the window must still advance";
}

/*
 * A frame the peer reported as BRNR is resolved: its posn went back to be
 * sent again under a new frame. A later ack that sacks the old frame must not
 * resolve it a second time -- and by then the work request may have been given
 * up on and freed, so the tracker entry must not be followed at all.
 */
TEST_F(QpTest, ASackForAFrameAlreadyBrnrdIsNotCountedAgain) {
  // Frame 10 was BRNR'd, so its posn went back to pending; frame 11 is in
  // flight and holds snd_una down.
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::BRNR, Flow::FsnState::UNACKED},
      /*sndCur=*/12);
  auto* const flow = fixture.flow;
  auto* const wqe = fixture.wqe.get();
  const uint32_t brnrPosn = fixture.posns[0];
  wqe->releasePosn(brnrPosn);
  const uint32_t outstandingBefore = wqe->getOutstandingPosnCount();

  // The peer sacks frame 10, which it had already reported as BRNR.
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(0);
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(9)
                                  .setMaxRcvdFsn(10)
                                  .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                                  .setFsnBitVector(fsnBitVector)
                                  .setTimestamp(0)
                                  .build();
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  EXPECT_EQ(wqe->getOutstandingPosnCount(), outstandingBefore)
      << "a BRNR'd frame is already resolved; resolving it again on a "
         "sack claims delivery of a packet the peer said it could not take";
  EXPECT_FALSE(wqe->isFrameAllocated(brnrPosn))
      << "the posn must stay waiting to be sent again, not be marked acked";
}

/*
 * SACK and BRNR are contradictory outcomes for the same frame. A malformed
 * peer packet that sets both must be rejected atomically: applying the
 * cumulative ACK first and then treating the overlapping bit as BRNR can
 * release a posn while advancing the send window around its original frame.
 */
TEST_F(QpTest, AnAckWithOverlappingSackAndBrnrBitsIsDiscarded) {
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED},
      /*sndCur=*/14);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  const uint32_t outstandingBefore = fixture.wqe->getOutstandingPosnCount();

  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  fsnBitVector.setBit(0);
  rnrBitVector.setBit(0);
  const AckHeader malformedAck = AckHeader::AckHeaderBuilder()
                                     .setCumulativeFsn(9)
                                     .setMaxRcvdFsn(13)
                                     .setAckStatus(AckStatus::BRNR)
                                     .setFsnBitVector(fsnBitVector)
                                     .setRnrBitVector(rnrBitVector)
                                     .setRnrBackoffTimeExp(0)
                                     .setTimestamp(0)
                                     .build();

  flow->processAckPacket(
      malformedAck, simulatedProcessedAtNs(malformedAck.getTimestamp()));

  EXPECT_EQ(txState.sndUna, 10) << "the malformed ACK must not advance sndUna";
  EXPECT_EQ(txState.maxAckdFsn, 9)
      << "the malformed ACK must not advance maxAckdFsn";
  ASSERT_EQ(txState.tracker.size(), 4u)
      << "the malformed ACK must not erase tracker entries";
  EXPECT_EQ(fixture.wqe->getOutstandingPosnCount(), outstandingBefore)
      << "the malformed ACK must not release an outstanding packet";
  for (const auto posn : fixture.posns) {
    EXPECT_TRUE(fixture.wqe->isFrameAllocated(posn))
        << "the malformed ACK must not mutate WQE packet state";
  }
  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value())
      << "the malformed ACK must not install BRNR state";
}

namespace {
// One in-order ACK carrying the given cdmsn, which is the only thing the two
// cdmsn cases below vary.
void processInOrderAckWithCdmsn(TxFlow* flow, uint16_t cdmsn) {
  const AckHeader ack = AckHeader::AckHeaderBuilder()
                            .setTag(kMetaRoceInitialTag)
                            .setCumulativeFsn(10)
                            .setMaxRcvdFsn(10)
                            .setCumulRespDeliveredMsn(cdmsn)
                            .setAckStatus(AckStatus::ACK)
                            .setTimestamp(0)
                            .build();
  flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));
}
} // namespace

TEST_F(QpTest, AnAckWhoseCdmsnPrecedesTheNextTransmitMsnIsProcessed) {
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED},
      /*sndCur=*/14);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;

  const uint16_t nextMsn = qp.getTag(kMetaRoceInitialTag)->getNextMsn();
  const uint16_t behind = static_cast<uint16_t>(nextMsn - 100);
  ASSERT_TRUE(before(behind, nextMsn))
      << "precondition: the cdmsn under test must sit behind the next transmit "
         "msn, or this is the rejected case rather than the accepted one";

  processInOrderAckWithCdmsn(flow, behind);

  EXPECT_EQ(txState.sndUna, 11)
      << "a cdmsn behind the next transmit msn is ordinary progress, so the "
         "ack must still advance the send window";
}

TEST_F(QpTest, AnAckWhoseCdmsnReachesTheNextTransmitMsnIsDiscarded) {
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED},
      /*sndCur=*/14);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  const uint16_t sndUnaBefore = txState.sndUna;

  // Subtracting past the half synthesizes a stale cdmsn forward of the window.
  const uint16_t nextMsn = qp.getTag(kMetaRoceInitialTag)->getNextMsn();
  const uint16_t ahead = static_cast<uint16_t>(nextMsn - 40000);
  ASSERT_FALSE(before(ahead, nextMsn))
      << "precondition: the cdmsn under test must reach the next transmit msn, "
         "or no ack is rejected and the case exercises no discard";

  processInOrderAckWithCdmsn(flow, ahead);

  EXPECT_EQ(txState.sndUna, sndUnaBefore)
      << "a cdmsn from a previous trip through the sequence space matches no "
         "outstanding message, so the ack must be discarded whole";
}

TEST_F(QpTest, AnAckTellsTheControllerWhetherDataIsWaitingToBeSent) {
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED, Flow::FsnState::UNACKED},
      /*sndCur=*/10);
  ASSERT_EQ(qp.getUnsentPosnCount(), 0u)
      << "precondition: the fixture's frames belong to work no tag holds, so "
         "nothing waits to be sent";
  EXPECT_CALL(
      *fixture.congestionController,
      onPacketAck(
          0, Field(&CongestionController::AckEvent::sendPending, false)))
      .Times(1);

  const AckHeader ack = AckHeader::AckHeaderBuilder()
                            .setCumulativeFsn(10)
                            .setMaxRcvdFsn(10)
                            .setAckStatus(AckStatus::ACK)
                            .setTimestamp(0)
                            .build();
  fixture.flow->processAckPacket(ack, simulatedProcessedAtNs(0));
}

TEST_F(QpTest, AnAckClaimingAFrameNotYetSentIsDiscardedWhole) {
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED,
       Flow::FsnState::UNACKED},
      /*sndCur=*/10);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  const uint32_t outstandingBefore = fixture.wqe->getOutstandingPosnCount();

  const AckHeader ack = AckHeader::AckHeaderBuilder()
                            .setCumulativeFsn(10)
                            .setMaxRcvdFsn(txState.sndNxt)
                            .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                            .setTimestamp(0)
                            .build();
  flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));

  EXPECT_EQ(txState.sndUna, 10)
      << "the sender should apply no part of an ack claiming a frame it never "
         "sent, the cumulative fsn included";
  EXPECT_EQ(txState.tracker.size(), 4u)
      << "the sender should leave every frame in the tracker";
  EXPECT_EQ(fixture.wqe->getOutstandingPosnCount(), outstandingBefore)
      << "the work request should still count every frame outstanding, "
         "agreeing with the tracker that holds them";
  EXPECT_EQ(txState.maxAckdFsn, 9)
      << "the sender should record no frame as received from the discarded "
         "ack";
}

/*
 * Giving up on a packet races the peer acknowledging it: the ack may already
 * be in flight, or the packet may have arrived and only its ack was lost.
 */
TEST_F(QpTest, AnAckForAnAlreadyAbandonedFrameIsNotCountedTwice) {
  // Frame 10 was given up on; frame 11 is still legitimately in flight.
  TrackedMessageFixture fixture(
      arbiter,
      qp,
      mtu,
      initialTag,
      {Flow::FsnState::ABANDONED, Flow::FsnState::UNACKED},
      /*sndCur=*/12);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  auto* const wqe = fixture.wqe.get();
  const uint32_t abandonedPosn = fixture.posns[0];
  wqe->markFailing(kWcRetryExcErr);
  wqe->abandonPosn(abandonedPosn);
  const uint32_t outstandingBefore = wqe->getOutstandingPosnCount();

  // The peer reports it received frame 10 after all.
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(0);
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(9)
                                  .setMaxRcvdFsn(10)
                                  .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                                  .setFsnBitVector(fsnBitVector)
                                  .setTimestamp(0)
                                  .build();
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  EXPECT_EQ(wqe->getOutstandingPosnCount(), outstandingBefore)
      << "a posn already given up on must not be resolved a second time, "
         "which would drop the outstanding count below what is in flight";
  EXPECT_EQ(txState.sndUna, 11)
      << "the window must still advance past the retired frame";
  ASSERT_EQ(txState.tracker.size(), 1u)
      << "only the frame still in flight belongs in the tracker";
  EXPECT_TRUE(txState.tracker.at(0).isUnacked())
      << "the frame still in flight must remain awaiting its acknowledgement";
}

/*
 * A receiver with no buffer posted asks for a packet to be sent again later.
 * There is no point holding a message that has been given up on for that: the
 * retry would carry data the client is never told arrived.
 */
TEST_F(QpTest, BrnrOnAFailingWqeAbandonsRatherThanWaitingToResend) {
  TrackedMessageFixture fixture(
      arbiter, qp, mtu, initialTag, {Flow::FsnState::UNACKED}, /*sndCur=*/11);
  auto* const flow = fixture.flow;
  auto& txState = *fixture.txState;
  auto* const wqe = fixture.wqe.get();
  auto* const rawCongestionController = fixture.congestionController;
  wqe->markFailing(kWcRetryExcErr);

  uint32_t reportedAbandoned = 0;
  ON_CALL(*rawCongestionController, onPacketAck(_, _))
      .WillByDefault([&](uint16_t, const CongestionController::AckEvent& ack) {
        reportedAbandoned += ack.numAckedPackets;
      });

  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(0);
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(9)
                                  .setMaxRcvdFsn(10)
                                  .setAckStatus(AckStatus::BRNR)
                                  .setRnrBitVector(rnrBitVector)
                                  .setRnrBackoffTimeExp(0)
                                  .setTimestamp(0)
                                  .build();
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  EXPECT_EQ(wqe->getOutstandingPosnCount(), 0u)
      << "abandoning it must account for the posn, letting the message complete "
         "with its error instead of waiting on a resend that never happens";
  EXPECT_EQ(txState.tracker.size(), 0u)
      << "the frame must leave the tracker for this to say anything about how "
         "its share of the window was returned";
  EXPECT_EQ(reportedAbandoned, 1u)
      << "a frame abandoned in answer to a BRNR must return its share of "
         "the shared flow window, exactly as one abandoned by the "
         "retransmission scan does; no ack is coming to return it later";
}

/*
 * Verifies that txState is properly updated when an ack packet, with a gap in
 * the SACK field, is received and processed.
 */
TEST_F(QpTest, ProcessAnAckPacket) {
  // create a qp and a flow
  const auto flowId = 0;

  auto congestionController = std::make_unique<MockCongestionController>();
  auto rawCongestionController = congestionController.get();
  qp.setCongestionController(std::move(congestionController));

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getTxFlowById(flowId);

  const int frames = 20;
  const uint32_t length = frames * mtu;
  auto payload = std::make_unique<char[]>(length);
  auto wqe = std::make_unique<SendWqe>(
      23 /*workReqId*/,
      oneSge(
          payload.get(), /*address*/
          length),
      mtu);
  wqe->setMsn(initialTag->getAndIncrementNextMsn());
  wqe->setCsn(initialTag->getAndIncrementNextCsn());

  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.maxAckdFsn = 12;
  for (uint32_t posn = 0; posn < 6; ++posn) {
    const uint32_t fsn = txState.sndUna + posn;
    ASSERT_EQ(wqe->allocateNextPosn(), posn)
        << "precondition: posns are allocated in order, so the tracker entry "
           "below matches the posn its frame carries";
    txState.tracker.emplace_back(
        PacketId(wqe.get(), 0, posn, mtu),
        fsn == 12 ? Flow::FsnState::ACKED : Flow::FsnState::UNACKED);
  }
  txState.sndNxt = 16;

  // 13 and 14 are lost
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(2); // 15 is acked out of order
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(12)
                                  .setMaxRcvdFsn(15)
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
                  Eq(static_cast<uint16_t>(3))),
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
              .numLostPackets = 2,
              .lossType =
                  CongestionController::LossEvent::LossType::OutOfOrder}))
      .Times(1);

  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));
  EXPECT_EQ(txState.sndUna, 13);
  EXPECT_EQ(*(txState.sndCur.rlock()), 15); // 14 and 15 are retransmitted
  EXPECT_EQ(flow->getNumPendingRetxPackets(), 2);
  EXPECT_EQ(txState.maxAckdFsn, 15);
  ASSERT_EQ(txState.tracker.size(), 3);

  // fsns 13, 14 and 15 are what remain, this message's posns 3, 4 and 5.
  EXPECT_EQ(txState.tracker.at(0).id.posn, 3);
  EXPECT_TRUE(txState.tracker.at(0).isUnacked());

  EXPECT_EQ(txState.tracker.at(1).id.posn, 4);
  EXPECT_TRUE(txState.tracker.at(1).isUnacked());

  EXPECT_EQ(txState.tracker.at(2).id.posn, 5);
  EXPECT_TRUE(txState.tracker.at(2).isAcked());
}

/*
 * Creates a receive WQE with 3 outstanding packets and verifies that when all
 * of them are marked as received, the WQE is marked as completed.
 */
TEST_F(QpTest, CheckForRxMessageCompletion) {
  const auto opType = BaseHeader::Opcode::OperationType::SEND;

  const uint32_t length = 3u * mtu;
  auto payload = std::make_unique<char[]>(length);
  const auto receiveWqe = postInSequenceReceiveWqe(qp, payload.get(), length);
  const auto msn = initialTag->getCDMSN() + 1;

  // mark posn = 0 as received
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 0));
  EXPECT_EQ(qp.getNumPendingRWqes(), 1);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);
  EXPECT_EQ(initialTag->getCDMSN(), msn - 1);

  // mark posn = 2 as received
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 2));
  EXPECT_EQ(qp.getNumPendingRWqes(), 1);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);
  EXPECT_EQ(initialTag->getCDMSN(), msn - 1);

  // mark posn = 1 as received
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 1));
  EXPECT_EQ(qp.getNumPendingRWqes(), 0);
  EXPECT_EQ(cq.getNumCompletedWqes(), 1);
  EXPECT_EQ(initialTag->getCDMSN(), msn);
}

TEST_F(QpTest, APosnArrivingTwiceIsRefusedRatherThanCompleting) {
  const auto opType = BaseHeader::Opcode::OperationType::SEND;

  const uint32_t length = 3u * mtu;
  auto payload = std::make_unique<char[]>(length);
  const auto receiveWqe = postInSequenceReceiveWqe(qp, payload.get(), length);
  const auto msn = initialTag->getCDMSN() + 1;

  ASSERT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 0))
      << "precondition: two posns arrive cleanly, so the refusal below is the "
         "duplicate rather than something earlier going wrong";
  ASSERT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 1))
      << "precondition: the posn the duplicate below stands in for";
  EXPECT_FALSE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 1))
      << "the sum says one posn stood in for another, so the message can "
         "never complete, and saying so is what lets the caller nak it";

  EXPECT_EQ(cq.getNumCompletedWqes(), 0)
      << "posn 2 never arrived, so reporting the receive hands the client a "
         "buffer with a packet missing from the middle of it";
  EXPECT_EQ(qp.getNumPendingRWqes(), 1)
      << "failing the tag belongs to the caller that holds the guards, so "
         "this call leaves the work request where it was";
}

TEST_F(QpTest, AReadAdvancingOverASkippedCsnIsRefused) {
  const uint32_t length = mtu;
  auto payload = std::make_unique<char[]>(length);
  // Enough posted that taking one for a later csn strands no receive ahead.
  std::vector<Wqe*> posted;
  posted.reserve(3);
  for (int i = 0; i < 3; ++i) {
    posted.push_back(postWqeHelper(
        &qp,
        {.workType = Wqe::WorkType::kReceive,
         .msgSizeBytes = length,
         .tagNum = kMetaRoceInitialTag,
         .address = payload.get()}));
  }
  const uint16_t readMsn = initialTag->getCDMSN() + 1;
  const uint16_t behindMsn = readMsn + 1;
  initialTag->extendCdmsnTrackerIfNeeded(readMsn);
  initialTag->extendCdmsnTrackerIfNeeded(behindMsn);
  ASSERT_TRUE(initialTag->assignInitiatorMsnToReceiveWqeIfNeeded(
      posted[2]->getCsn().value(), behindMsn, mtu))
      << "precondition: a receive waits two csns past the edge, so the edge "
         "cannot run over it";
  Wqe* const behind = initialTag->getReceiveWqe(
      BaseHeader::Opcode::OperationType::SEND, behindMsn);
  ASSERT_NE(behind, nullptr)
      << "precondition: the receive the message behind the read is bound to";

  ASSERT_TRUE(qp.checkForRxMessageCompletion(
      behind,
      BaseHeader::Opcode::OperationType::SEND,
      kMetaRoceInitialTag,
      behindMsn,
      0))
      << "precondition: the message arrives out of order, so no receive "
         "completes yet and the read below is what moves the edge";

  EXPECT_FALSE(qp.checkForRxMessageCompletion(
      /*wqe=*/nullptr,
      BaseHeader::Opcode::OperationType::READ,
      kMetaRoceInitialTag,
      readMsn,
      0))
      << "the read takes no receive of its own but moves the edge over one "
         "numbered out of sequence, and the caller fails the tag on that";
}

TEST_F(QpTest, APosnArrivingWithNoneOutstandingIsRefusedRatherThanCounted) {
  const auto opType = BaseHeader::Opcode::OperationType::SEND;

  const uint32_t length = 3u * mtu;
  auto payload = std::make_unique<char[]>(length);
  const auto receiveWqe = postInSequenceReceiveWqe(qp, payload.get(), length);
  const auto msn = initialTag->getCDMSN() + 1;

  ASSERT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 0))
      << "precondition: the first of the three arrivals that spend the count";
  ASSERT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 1))
      << "precondition: the second of the three arrivals that spend the count";
  ASSERT_FALSE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 1))
      << "precondition: three arrivals for a three packet message spend the "
         "count while the message is still short a posn, which is the state "
         "the arrival below lands in";

  EXPECT_FALSE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 2))
      << "the count is spent, so taking this packet would wrap it past the "
         "length of the message; a peer that keeps sending must not be able "
         "to fault the process";
}

/*
 * Creates a send WQE with 5 outstanding packets and verifies that when all
 * of them are marked as received, the WQE is marked as completed.
 */
TEST_F(QpTest, CheckForTxMessageCompletion) {
  const uint32_t msgSizeBytes = 5 * mtu;

  auto payload = std::make_unique<char[]>(msgSizeBytes);
  auto rawWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get()});
  ASSERT_TRUE(allocateSendablePosns(rawWqe))
      << "precondition: the send's frames are on the wire, so the completion check below has frames to account for";

  std::vector<PacketId> newlyAckedPackets;
  // ack packet 0, 1, 3
  newlyAckedPackets.emplace_back(rawWqe, 0 /* tagNum */, 0 /*posn*/, mtu);
  newlyAckedPackets.emplace_back(rawWqe, 0 /* tagNum */, 1 /*posn*/, mtu);
  newlyAckedPackets.emplace_back(rawWqe, 0 /* tagNum */, 3 /*posn*/, mtu);
  qp.checkForTxMessageCompletion(newlyAckedPackets);
  EXPECT_EQ(qp.getNumPendingWqes(), 1);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);

  // ack packet 4
  newlyAckedPackets.clear();
  newlyAckedPackets.emplace_back(rawWqe, 0 /* tagNum */, 4 /*posn*/, mtu);
  qp.checkForTxMessageCompletion(newlyAckedPackets);
  EXPECT_EQ(qp.getNumPendingWqes(), 1);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);

  // ack packet 2
  newlyAckedPackets.clear();
  newlyAckedPackets.emplace_back(rawWqe, 0 /* tagNum */, 2 /*posn*/, mtu);
  qp.checkForTxMessageCompletion(newlyAckedPackets);
  EXPECT_EQ(qp.getNumPendingWqes(), 0);
  EXPECT_EQ(cq.getNumCompletedWqes(), 1);
}

/*
 * One ack can carry frames belonging to several tags, and each frame is work
 * of the tag its tracker id carries. A message reports only once every posn of
 * it is in, so a batch that finishes one tag's message and part of another's
 * has exactly one completion to make.
 */
TEST_F(QpTest, AnAckBatchAppliesEachFrameToItsOwnTag) {
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  qp.addTag(kOtherTag, &cq, ibvQp);

  auto payload = std::make_unique<char[]>(2 * mtu);
  Wqe* singlePosnSend = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 70,
       .address = payload.get()});
  ASSERT_NE(singlePosnSend, nullptr);
  ASSERT_TRUE(allocateSendablePosns(singlePosnSend))
      << "precondition: the first tag's single frame is on the wire for the batch below to ack";
  Wqe* twoPosnSend = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(2 * mtu),
       .tagNum = kOtherTag,
       .workReqId = 71,
       .address = payload.get()});
  ASSERT_NE(twoPosnSend, nullptr);
  ASSERT_TRUE(allocateSendablePosns(twoPosnSend))
      << "precondition: the second tag's two frames are on the wire, so acking one leaves the message unfinished";

  // A frame of each, acked together.
  std::vector<PacketId> newlyAckedPackets;
  newlyAckedPackets.emplace_back(
      singlePosnSend, kMetaRoceInitialTag, 0 /*posn*/, mtu);
  newlyAckedPackets.emplace_back(twoPosnSend, kOtherTag, 0 /*posn*/, mtu);
  qp.checkForTxMessageCompletion(newlyAckedPackets);

  ASSERT_EQ(cq.getNumCompletedWqes(), 1)
      << "the second tag's message still has a posn outstanding, so the "
         "batch has one completion to make, not two";
  EXPECT_EQ(folly::coro::blockingWait(cq.pollCq()).workReqId, 70)
      << "and it must be the message every frame of which is in; reporting "
         "the other one means a frame was applied to the wrong tag's work";

  // The posn the second tag's message was still waiting on.
  newlyAckedPackets.clear();
  newlyAckedPackets.emplace_back(twoPosnSend, kOtherTag, 1 /*posn*/, mtu);
  qp.checkForTxMessageCompletion(newlyAckedPackets);

  ASSERT_EQ(cq.getNumCompletedWqes(), 1)
      << "the second tag's message reports once its last posn is acked";
  EXPECT_EQ(folly::coro::blockingWait(cq.pollCq()).workReqId, 71)
      << "the frames of one batch must not have completed it early";
}

/*
 * Destroying a QP that still has an outstanding send WQE must complete it with
 * a flush error rather than freeing it silently -- the verbs contract for a
 * destroyed QP, and how in-flight WQEs are cleared on teardown.
 */
TEST_F(QpTest, DestroyingQpFlushesOutstandingWqeInError) {
  constexpr uint64_t kWrId = 0xF1A5;
  struct ibv_qp localIbvQp{};
  {
    Qp localQp{kSecondQpNum, retxTimer, arbiter, mrs, &cq, &localIbvQp};
    auto payload = std::make_unique<char[]>(mtu);
    postWqeHelper(
        &localQp,
        {.workType = Wqe::WorkType::kSend,
         .msgSizeBytes = mtu,
         .workReqId = kWrId,
         .address = payload.get()});
    ASSERT_EQ(localQp.getNumPendingWqes(), 1)
        << "the posted send WQE must be outstanding before the QP is destroyed";
    // localQp is destroyed here; ~Qp() must flush the outstanding WQE in error.
  }

  ibv_wc wc{};
  auto pollCq = [this, &wc]() -> folly::coro::Task<> {
    Cqe cqe = co_await cq.pollCq();
    wc.status = static_cast<ibv_wc_status>(cqe.status);
    wc.wr_id = cqe.workReqId;
  };
  folly::coro::blockingWait(
      folly::coro::timeout(pollCq(), std::chrono::milliseconds(1000)));

  EXPECT_EQ(wc.wr_id, kWrId)
      << "the flushed completion must carry the outstanding WQE's wr_id";
  EXPECT_EQ(wc.status, IBV_WC_WR_FLUSH_ERR)
      << "destroying a QP must complete its outstanding WQE with WR_FLUSH_ERR";
}

/*
 * A WQE posted to a Tag that is then marked for deletion is completed with a
 * flush error instead of being sent, and consumes no MSN -- so teardown cannot
 * leave a gap in the Tag's outbound sequence.
 */
TEST_F(QpTest, MarkedTagFlushesNewWqeWithoutConsumingMsn) {
  constexpr uint64_t kWrId = 0xB0B;
  const auto msnBefore = initialTag->getNextMsn();

  initialTag->markForDeletion();

  // Process the WQE directly (rather than via the async pump) so the marked-tag
  // rejection is exercised synchronously.
  auto payload = std::make_unique<char[]>(mtu);
  auto wqe = std::make_unique<SendWqe>(kWrId, oneSge(payload.get(), mtu), mtu);
  EXPECT_FALSE(postToTag(*initialTag, std::move(wqe), mtu))
      << "a marked tag must reject the WQE, not signal ready to transmit";

  EXPECT_EQ(initialTag->getNextMsn(), msnBefore)
      << "a rejected WQE must not consume an MSN, so no gap opens in the Tag's "
         "outbound sequence";
  EXPECT_EQ(qp.getNumPendingWqes(), 0)
      << "a rejected WQE must not become outstanding work on the Tag";

  ibv_wc wc{};
  auto pollCq = [this, &wc]() -> folly::coro::Task<> {
    Cqe cqe = co_await cq.pollCq();
    wc.status = static_cast<ibv_wc_status>(cqe.status);
    wc.wr_id = cqe.workReqId;
  };
  folly::coro::blockingWait(
      folly::coro::timeout(pollCq(), std::chrono::milliseconds(1000)));

  EXPECT_EQ(wc.wr_id, kWrId)
      << "the client must still receive a completion for the rejected WQE";
  EXPECT_EQ(wc.status, IBV_WC_WR_FLUSH_ERR)
      << "a WQE posted to a Tag being torn down must complete with WR_FLUSH_ERR";
}

/*
 * A tag marked for deletion NAKs an incoming READ request (it will never build
 * a virtual response WQE for a half-closed tag) rather than serving it.
 */
TEST_F(QpTest, MarkedTagNaksIncomingReadRequest) {
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  createAlwaysSendingMockCongestionController(qp);
  auto flow = qp.getRxFlowById(0);

  const uint16_t pktFsn = 10;
  auto& rxState = flow->getRxState();
  rxState.rcvNxt = pktFsn;

  BaseHeader::Opcode opCode(BaseHeader::Opcode::OperationType::READ);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(qp.getRemoteQpNum())
                                    .setDstQp(qp.getLocalQpNum())
                                    .setFlowId(0)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();
  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(pktFsn)
                                    .setUna(pktFsn)
                                    .setPosn(0)
                                    .setMsn(initialTag->getCDMSN() + 1)
                                    .setAckRequest(true)
                                    .build();
  DataPacket readPacket(baseHeader, dataHeader, nullptr, 0);

  initialTag->markForDeletion();
  auto maybeAck = flow->processDataPacket(&readPacket, kEcnECT0);
  ASSERT_TRUE(maybeAck.has_value())
      << "a READ to a marked tag must produce a NAK ack, not be silently dropped";
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::NAK_INVALID_REQUEST)
      << "a marked tag must NAK an incoming READ rather than serve it";
}

TEST_F(QpTest, MarkedTagNaksIncomingReadWithImmediate) {
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  createAlwaysSendingMockCongestionController(qp);
  auto flow = qp.getRxFlowById(0);

  const uint16_t pktFsn = 10;
  auto& rxState = flow->getRxState();
  rxState.rcvNxt = pktFsn;

  BaseHeader::Opcode opCode(BaseHeader::Opcode::OperationType::READ_IMM_P);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(qp.getRemoteQpNum())
                                    .setDstQp(qp.getLocalQpNum())
                                    .setFlowId(0)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();
  const ReadWriteHeader readHeader =
      ReadWriteHeader::ReadWriteHeaderBuilder()
          .setKey(mrKey)
          .setMessageAddr(reinterpret_cast<uint64_t>(receiveBuffer.data()))
          .setMessageLength(mtu)
          .build();
  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(pktFsn)
                                    .setUna(pktFsn)
                                    .setPosn(0)
                                    .setMsn(initialTag->getCDMSN() + 1)
                                    .setAckRequest(true)
                                    .build();
  const ReadWritePacket readImmPacket(
      baseHeader,
      dataHeader,
      readHeader,
      /*payloadAddress=*/nullptr,
      /*payloadSizeBytes=*/0,
      /*maybeImmData=*/0x12345678);

  initialTag->markForDeletion();
  auto maybeAck = flow->processDataPacket(&readImmPacket, kEcnECT0);
  ASSERT_TRUE(maybeAck.has_value())
      << "a read-with-immediate to a marked tag must produce a NAK ack, not "
         "be silently dropped";
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::NAK_INVALID_REQUEST)
      << "a marked tag must NAK an incoming read-with-immediate rather than "
         "serve it";
}

TEST_F(QpTest, ReadWithImmediateCarryingPayloadIsNakd) {
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  createAlwaysSendingMockCongestionController(qp);
  auto flow = qp.getRxFlowById(0);

  const uint16_t pktFsn = 10;
  auto& rxState = flow->getRxState();
  rxState.rcvNxt = pktFsn;

  BaseHeader::Opcode opCode(BaseHeader::Opcode::OperationType::READ_IMM_P);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(qp.getRemoteQpNum())
                                    .setDstQp(qp.getLocalQpNum())
                                    .setFlowId(0)
                                    .setOpcode(std::move(opCode))
                                    .setVersion(0)
                                    .build();
  const ReadWriteHeader readHeader =
      ReadWriteHeader::ReadWriteHeaderBuilder()
          .setKey(mrKey)
          .setMessageAddr(reinterpret_cast<uint64_t>(receiveBuffer.data()))
          .setMessageLength(mtu)
          .build();
  auto* postedReceive = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kReceive,
       .msgSizeBytes = mtu,
       .address = receiveBuffer.data()});
  ASSERT_NE(postedReceive, nullptr);
  ASSERT_TRUE(postedReceive->getCsn().has_value())
      << "precondition: the posted receive carries the csn the request binds";
  const uint16_t msn = initialTag->getCDMSN() + 1;
  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(pktFsn)
                                    .setUna(pktFsn)
                                    .setPosn(0)
                                    .setMsn(msn)
                                    .setCsn(postedReceive->getCsn().value())
                                    .setAckRequest(true)
                                    .build();
  char payloadByte = 0;
  const ReadWritePacket readImmPacket(
      baseHeader,
      dataHeader,
      readHeader,
      /*payloadAddress=*/&payloadByte,
      /*payloadSizeBytes=*/1,
      /*maybeImmData=*/0x12345678);
  auto maybeAck = flow->processDataPacket(&readImmPacket, kEcnECT0);
  ASSERT_TRUE(maybeAck.has_value())
      << "a payload-carrying read-with-immediate must produce a NAK ack, not "
         "be silently dropped";
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::NAK_INVALID_REQUEST)
      << "a read-with-immediate carrying payload is not a well-formed request";
  EXPECT_EQ(qp.getNumPendingWqes(), 0)
      << "a NAK'd request creates no read response";
  auto tag = qp.getTag(kMetaRoceInitialTag);
  ASSERT_NE(tag, nullptr);
  EXPECT_EQ(
      tag->getReceiveWqe(BaseHeader::Opcode::OperationType::READ_IMM_P, msn),
      nullptr)
      << "a malformed request binds no receive";
}

/*
 * A tag marked for deletion still accepts and delivers incoming SEND data so
 * its receive side can drain; only READ/ATOMIC are NAK'd.
 */
TEST_F(QpTest, MarkedTagStillAcceptsIncomingSendData) {
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto congestionController = std::make_unique<MockCongestionController>();
  qp.setCongestionController(std::move(congestionController));
  auto flow = qp.getRxFlowById(0);

  const uint16_t msn = qp.getTag(kMetaRoceInitialTag)->getCDMSN() + 1;
  const uint32_t length = 3u * mtu;
  auto payload = std::make_unique<char[]>(length);
  postInSequenceReceiveWqe(qp, payload.get(), length);

  const uint16_t pktFsn = 10;
  auto& rxState = flow->getRxState();
  rxState.rcvNxt = pktFsn;
  const DataHeader dataHeader =
      DataHeader::DataHeaderBuilder()
          .setFsn(pktFsn)
          .setUna(pktFsn)
          .setPosn(1)
          .setMsn(msn)
          .setAckRequest(true) // force an ack to inspect
          .build();
  auto dataPacket = buildDataPacketFromTestDataHeader(qp, dataHeader);

  initialTag->markForDeletion();
  auto maybeAck = flow->processDataPacket(&dataPacket, kEcnECT0);

  EXPECT_EQ(rxState.rcvNxt, pktFsn + 1)
      << "a marked tag must still deliver incoming SEND data (rcvNxt advances)";
  ASSERT_TRUE(maybeAck.has_value())
      << "the delivered SEND must produce an ack, not be dropped";
  EXPECT_NE(maybeAck.value().getAckStatus(), AckStatus::NAK_INVALID_REQUEST)
      << "a marked tag must accept and ack incoming SEND, not NAK it";
}

/*
 * A tag marked for deletion keeps retransmitting its outstanding frames so they
 * can drain to completion; it is not skipped by the retransmission path.
 */
TEST_F(QpTest, MarkedTagKeepsRetransmittingOutstandingFrames) {
  ScopedSuppressedTxTimers suppressTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  uint16_t msgSizeToSendBytes = mtu * 2; // 2 packets
  auto payload = std::make_unique<char[]>(msgSizeToSendBytes);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = payload.get()});

  int sent = 0;
  while (auto egressPacket = flow->getNextPacketToSend()) {
    sent++;
  }
  ASSERT_EQ(sent, 2)
      << "Precondition: both frames are sent before the retransmission is driven";

  // Mark the tag only after its frames are outstanding, then drive the RTO.
  initialTag->markForDeletion();
  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  int retransmitted = 0;
  while (auto egressPacket = flow->getNextPacketToSend()) {
    auto* packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    ASSERT_NE(packet, nullptr)
        << "each frame the marked tag re-sends must be a data packet";
    EXPECT_TRUE(packet->getDataHeader().getIsRetransmit())
        << "the marked tag's outstanding frames must go out as retransmissions";
    retransmitted++;
  }
  EXPECT_EQ(retransmitted, 2)
      << "a marked tag must keep retransmitting its outstanding frames to drain";
}

/*
 * A tag marked for deletion still completes its outstanding SEND when acked,
 * draining it successfully (not in error) via the normal completion path.
 */
TEST_F(QpTest, MarkedTagOutstandingSendCompletesViaAck) {
  constexpr uint64_t kWrId = 0x5E4D;
  auto payload = std::make_unique<char[]>(mtu);
  auto* rawWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .workReqId = kWrId,
       .address = payload.get()});
  ASSERT_TRUE(allocateSendablePosns(rawWqe))
      << "precondition: the send's frame is on the wire, so the ack below can complete it";
  ASSERT_EQ(qp.getNumPendingWqes(), 1)
      << "Precondition: the posted send is outstanding before the tag is marked "
         "and acked";

  initialTag->markForDeletion();

  std::vector<PacketId> ackedPackets;
  ackedPackets.emplace_back(rawWqe, kMetaRoceInitialTag, 0 /*posn*/, mtu);
  qp.checkForTxMessageCompletion(ackedPackets);

  EXPECT_EQ(qp.getNumPendingWqes(), 0)
      << "a marked tag's outstanding SEND must still complete when acked";

  ibv_wc wc{};
  auto pollCq = [this, &wc]() -> folly::coro::Task<> {
    Cqe cqe = co_await cq.pollCq();
    wc.status = static_cast<ibv_wc_status>(cqe.status);
    wc.wr_id = cqe.workReqId;
  };
  folly::coro::blockingWait(
      folly::coro::timeout(pollCq(), std::chrono::milliseconds(1000)));

  EXPECT_EQ(wc.wr_id, kWrId)
      << "the completion must carry the drained SEND's wr_id";
  EXPECT_EQ(wc.status, IBV_WC_SUCCESS)
      << "a drained (acked) SEND must complete successfully, not in error";
}

/*
 * Creates a virtual receive WQE with 2 outstanding packets and verifies that
 * when all of them are marked as received, the vRWQE is marked as completed.
 */
TEST_F(QpTest, VirtualReceiveWQECreationAndCompletion) {
  const auto msn = initialTag->getNextMsn();
  const auto csn = 0;
  const auto opType = BaseHeader::Opcode::OperationType::WRITE;

  const auto dataHeader = DataHeader::DataHeaderBuilder()
                              .setFsn(0)
                              .setUna(0)
                              .setPosn(0)
                              .setMsn(msn)
                              .setCsn(csn)
                              .build();
  const auto rwHeader = ReadWriteHeader::ReadWriteHeaderBuilder()
                            .setMessageLength(mtu * 2)
                            .setMessageAddr((unsigned long)receiveBuffer.data())
                            .setKey(mrKey)
                            .build();
  EXPECT_EQ(initialTag->getReceiveWqe(opType, msn), nullptr);
  int addressError{0};
  initialTag->createVirtualRWqeIfNeeded(
      msn, rwHeader, &addressError, mrs, qp.getMtu());
  EXPECT_EQ(qp.getNumPendingRWqes(), 1);
  EXPECT_EQ(addressError, 0);

  auto receiveWqe = initialTag->getReceiveWqe(opType, msn);
  EXPECT_NE(receiveWqe, nullptr);
  // mark posn = 0 as received
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 0));
  EXPECT_EQ(qp.getNumPendingRWqes(), 1);
  EXPECT_EQ(initialTag->getCDMSN(), (msn - 1));

  // mark posn = 1 as received
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 1));
  EXPECT_EQ(qp.getNumPendingRWqes(), 0);
  EXPECT_EQ(initialTag->getCDMSN(), msn);
}

/*
 * Posts a read WQE with 3 outstanding packets and verifies that when all
 * of them are marked as received, the WQE is marked as completed.
 */
TEST_F(QpTest, ReadRxMessageCompletion) {
  const auto msgSizeBytes = 2u * mtu;
  auto payload = std::make_unique<char[]>(msgSizeBytes);
  const auto readWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get()});
  const auto msn = readWqe->getMsn().value();

  const auto opType = BaseHeader::Opcode::OperationType::READ_RESP;
  // mark posn = 0 as received
  Wqe* receiveWqe = initialTag->getReceiveWqe(opType, msn);
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 0));
  EXPECT_EQ(qp.getNumPendingRWqes(), 0);
  EXPECT_EQ(qp.getNumPendingWqes(), 1);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);
  EXPECT_EQ(initialTag->getCDMSN(), msn - 1);

  // mark posn = 1 as received
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 1));
  EXPECT_EQ(qp.getNumPendingWqes(), 0);
  EXPECT_EQ(cq.getNumCompletedWqes(), 1);
  EXPECT_EQ(initialTag->getCDMSN(), msn - 1);
}

/*
 * On the requester, a read-with-immediate behaves exactly like a plain read:
 * the response data fills its posns and it completes with IBV_WC_RDMA_READ once
 * every posn has arrived. The immediate the request carried is for the
 * responder's completion, not the requester's data transfer.
 */
TEST_F(QpTest, ReadWithImmediateInitiatorCompletesAsRdmaRead) {
  const auto msgSizeBytes = 2u * mtu;
  auto payload = std::make_unique<char[]>(msgSizeBytes);
  constexpr uint64_t kWorkReqId = 0x777;
  constexpr uint32_t kImmediateData = 0xABCD1234;
  const auto readWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kReadWithImm,
       .msgSizeBytes = msgSizeBytes,
       .workReqId = kWorkReqId,
       .address = payload.get(),
       .maybeImmediateData = kImmediateData});
  ASSERT_NE(readWqe, nullptr);
  ASSERT_TRUE(readWqe->getCsn().has_value())
      << "a read-with-immediate carries a csn like other immediate operations";
  const auto msn = readWqe->getMsn().value();

  const auto opType = BaseHeader::Opcode::OperationType::READ_RESP;
  Wqe* receiveWqe = initialTag->getReceiveWqe(opType, msn);
  ASSERT_EQ(receiveWqe, readWqe)
      << "the read response is matched back to the posted read-with-immediate";

  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 0))
      << "precondition: delivering the first response posn succeeds";
  EXPECT_EQ(cq.getNumCompletedWqes(), 0)
      << "the read is not complete until every response posn has arrived";
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, msn, 1))
      << "precondition: delivering the second response posn succeeds";
  EXPECT_EQ(qp.getNumPendingWqes(), 0)
      << "the completed read leaves no pending work";
  ASSERT_EQ(cq.getNumCompletedWqes(), 1)
      << "precondition: the read posted exactly one completion to inspect";
  const Cqe cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.workReqId, kWorkReqId)
      << "the completion belongs to the posted read-with-immediate";
  EXPECT_EQ(cqe.opcode, IBV_WC_RDMA_READ)
      << "the requester reports a read-with-immediate as a plain RDMA read";
}

/*
 * The responder side of read-with-immediate. A request that finds no posted
 * receive is BRNR'd; once a receive is posted and the request retried, the
 * responder consumes that receive as a completion notification and emits one
 * read response. Table 3.1 completes a read-with-immediate at the responder
 * once all data is acknowledged by the initiator, so the receive completion
 * (carrying the immediate and IBV_WC_DRIVER1) is deferred until the response
 * is acknowledged: the responder's application never observes the immediate
 * before the data has reached the initiator.
 */
TEST_F(QpTest, ReadWithImmediateRetryConsumesReceiveAndCreatesResponse) {
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  createAlwaysSendingMockCongestionController(qp);

  auto flow = qp.getRxFlowById(0);
  auto& rxState = flow->getRxState();
  rxState.rcvNxt = 0;
  rxState.maxRcvdFsn = 0;

  constexpr uint16_t msn = kFixtureCdmsn + 1;
  constexpr uint32_t immediateData = 0x12345678;
  const BaseHeader::Opcode opcode(
      BaseHeader::Opcode::OperationType::READ_IMM_P);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(qp.getRemoteQpNum())
                                    .setDstQp(qp.getLocalQpNum())
                                    .setFlowId(0)
                                    .setOpcode(opcode)
                                    .setVersion(0)
                                    .build();
  const ReadWriteHeader readHeader =
      ReadWriteHeader::ReadWriteHeaderBuilder()
          .setKey(mrKey)
          .setMessageAddr(reinterpret_cast<uint64_t>(receiveBuffer.data()))
          .setMessageLength(mtu)
          .build();

  const DataHeader firstHeader = DataHeader::DataHeaderBuilder()
                                     .setFsn(0)
                                     .setUna(0)
                                     .setAckRequest(true)
                                     .setPosn(0)
                                     .setMsn(msn)
                                     .setCsn(kFixtureRxCsn)
                                     .build();
  const ReadWritePacket firstPacket(
      baseHeader,
      firstHeader,
      readHeader,
      /*payloadAddress=*/nullptr,
      /*payloadSizeBytes=*/0,
      immediateData);
  auto firstAck = flow->processDataPacket(&firstPacket, kEcnECT0);
  ASSERT_TRUE(firstAck.has_value())
      << "precondition: the unbound request produces an ack to inspect";
  EXPECT_EQ(firstAck->getAckStatus(), AckStatus::BRNR)
      << "a read-with-immediate with no posted receive is BRNR'd";
  EXPECT_EQ(qp.getNumPendingWqes(), 0)
      << "no read response is created before a receive is bound";

  constexpr uint64_t workReqId = 0x322;
  auto* receiveWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kReceive,
       .msgSizeBytes = 2u * mtu,
       .workReqId = workReqId,
       .address = receiveBuffer.data()});
  ASSERT_NE(receiveWqe, nullptr);
  ASSERT_EQ(receiveWqe->getCsn(), kFixtureRxCsn)
      << "precondition: the posted receive carries the csn the retry binds";

  const DataHeader retryHeader = DataHeader::DataHeaderBuilder()
                                     .setFsn(1)
                                     .setUna(1)
                                     .setIsRetransmit(true)
                                     .setAckRequest(true)
                                     .setPosn(0)
                                     .setMsn(msn)
                                     .setCsn(kFixtureRxCsn)
                                     .build();
  const ReadWritePacket retryPacket(
      baseHeader,
      retryHeader,
      readHeader,
      /*payloadAddress=*/nullptr,
      /*payloadSizeBytes=*/0,
      immediateData);
  auto retryAck = flow->processDataPacket(&retryPacket, kEcnECT0);
  ASSERT_TRUE(retryAck.has_value())
      << "precondition: the retried request produces an ack to inspect";
  EXPECT_EQ(retryAck->getAckStatus(), AckStatus::ACK)
      << "the retried request finds the posted receive and is accepted";
  EXPECT_EQ(qp.getNumPendingRWqes(), 1)
      << "the bound receive is held until the read response is acknowledged";
  EXPECT_EQ(qp.getNumPendingWqes(), 1)
      << "the accepted read-immediate must create one read response";

  bool sawReadResponse = false;
  uint16_t responseFsn = 0;
  for (int i = 0; i < 2 && !sawReadResponse; ++i) {
    auto egress = waitForPacket(qp, 100000);
    ASSERT_NE(egress, nullptr);
    ASSERT_NE(egress->packet, nullptr);
    sawReadResponse =
        egress->packet->getBaseHeader().getOpcode().operationType() ==
        BaseHeader::Opcode::OperationType::READ_RESP;
    if (sawReadResponse) {
      responseFsn =
          dynamic_cast<DataPacket&>(*egress->packet).getDataHeader().getFsn();
    }
  }
  EXPECT_TRUE(sawReadResponse) << "the responder emits the read response";
  EXPECT_EQ(cq.getNumCompletedWqes(), 0)
      << "the receive must not complete before its read response is acknowledged";

  auto txFlow = qp.getTxFlowById(0);
  const AckHeader responseAck = AckHeader::AckHeaderBuilder()
                                    .setCumulativeFsn(responseFsn)
                                    .setMaxRcvdFsn(responseFsn)
                                    .setAckStatus(AckStatus::ACK)
                                    .setTimestamp(0)
                                    .build();
  txFlow->processAckPacket(
      responseAck, simulatedProcessedAtNs(responseAck.getTimestamp()));

  EXPECT_EQ(qp.getNumPendingRWqes(), 0)
      << "the acknowledged response releases the bound receive";
  ASSERT_EQ(cq.getNumCompletedWqes(), 1)
      << "precondition: the acknowledged response completed exactly one receive";
  const Cqe cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.workReqId, workReqId)
      << "the completion belongs to the bound posted receive";
  EXPECT_EQ(cqe.opcode, IBV_WC_DRIVER1)
      << "a read-with-immediate reports the provider-defined DRIVER1, distinct "
         "from a write-with-immediate";
  ASSERT_TRUE(cqe.maybeImmData.has_value())
      << "precondition: the completion carries an immediate to check";
  EXPECT_EQ(cqe.maybeImmData.value(), immediateData)
      << "the responder reports the immediate the request carried";
  EXPECT_EQ(cqe.sizeBytes, mtu)
      << "the responder's completion reports the length of the read, not the "
         "posted receive size";
}

/*
 * A read-with-immediate response fails when its tag enters error while the
 * response is outstanding; the error flush then owns the bound receive, so
 * a late ack for the failed response must not success-complete it.
 * markFailing here stands in for tag-error entry without the flush, isolating
 * the gate: no completion, receive still bound. The test then runs the flush
 * every error path ends in and expects the bound receive to report flushed,
 * proving the failed response orphans no completion.
 */
TEST_F(QpTest, FailingReadImmResponseLeavesReceiveForErrorFlush) {
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  createAlwaysSendingMockCongestionController(qp);

  auto flow = qp.getRxFlowById(0);
  auto& rxState = flow->getRxState();
  rxState.rcvNxt = 0;
  rxState.maxRcvdFsn = 0;

  constexpr uint16_t msn = kFixtureCdmsn + 1;
  constexpr uint32_t immediateData = 0x12345678;
  auto* receiveWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kReceive,
       .msgSizeBytes = 2u * mtu,
       .workReqId = 0x322,
       .address = receiveBuffer.data()});
  ASSERT_NE(receiveWqe, nullptr);

  const BaseHeader::Opcode opcode(
      BaseHeader::Opcode::OperationType::READ_IMM_P);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setSrcQp(qp.getRemoteQpNum())
                                    .setDstQp(qp.getLocalQpNum())
                                    .setFlowId(0)
                                    .setOpcode(opcode)
                                    .setVersion(0)
                                    .build();
  const ReadWriteHeader readHeader =
      ReadWriteHeader::ReadWriteHeaderBuilder()
          .setKey(mrKey)
          .setMessageAddr(reinterpret_cast<uint64_t>(receiveBuffer.data()))
          .setMessageLength(mtu)
          .build();
  const DataHeader requestHeader = DataHeader::DataHeaderBuilder()
                                       .setFsn(0)
                                       .setUna(0)
                                       .setAckRequest(true)
                                       .setPosn(0)
                                       .setMsn(msn)
                                       .setCsn(kFixtureRxCsn)
                                       .build();
  const ReadWritePacket requestPacket(
      baseHeader,
      requestHeader,
      readHeader,
      /*payloadAddress=*/nullptr,
      /*payloadSizeBytes=*/0,
      immediateData);
  auto requestAck = flow->processDataPacket(&requestPacket, kEcnECT0);
  ASSERT_TRUE(requestAck.has_value())
      << "precondition: the bound request produces an ack to inspect";
  EXPECT_EQ(requestAck->getAckStatus(), AckStatus::ACK)
      << "the request finds the posted receive and is accepted";

  bool sawReadResponse = false;
  uint16_t responseFsn = 0;
  for (int i = 0; i < 2 && !sawReadResponse; ++i) {
    auto egress = waitForPacket(qp, 100000);
    ASSERT_NE(egress, nullptr);
    ASSERT_NE(egress->packet, nullptr);
    sawReadResponse =
        egress->packet->getBaseHeader().getOpcode().operationType() ==
        BaseHeader::Opcode::OperationType::READ_RESP;
    if (sawReadResponse) {
      responseFsn =
          dynamic_cast<DataPacket&>(*egress->packet).getDataHeader().getFsn();
    }
  }
  ASSERT_TRUE(sawReadResponse)
      << "precondition: the responder emits the read response";

  auto txFlow = qp.getTxFlowById(0);
  auto& txState = txFlow->getTxState();
  ASSERT_EQ(txState.tracker.size(), 1)
      << "precondition: only the read response frame is in flight";
  Wqe* response = txState.tracker.front().id.wqe;
  ASSERT_NE(response, nullptr);
  ASSERT_TRUE(response->isVirtual())
      << "precondition: the in-flight frame belongs to the virtual response";
  response->markFailing(kWrFlushErr);

  const AckHeader responseAck = AckHeader::AckHeaderBuilder()
                                    .setCumulativeFsn(responseFsn)
                                    .setMaxRcvdFsn(responseFsn)
                                    .setAckStatus(AckStatus::ACK)
                                    .setTimestamp(0)
                                    .build();
  txFlow->processAckPacket(
      responseAck, simulatedProcessedAtNs(responseAck.getTimestamp()));

  EXPECT_EQ(cq.getNumCompletedWqes(), 0)
      << "a failing response must not success-complete its paired receive";
  EXPECT_EQ(qp.getNumPendingRWqes(), 1)
      << "the receive stays bound for the error flush to report";
  EXPECT_EQ(qp.getNumPendingWqes(), 0)
      << "the acked response still retires; only its receive completion is "
         "suppressed";

  {
    auto tagGuard = initialTag->scopedLock();
    auto retirementGuard = initialTag->lockForWqeRetirement();
    initialTag->flushPendingReceiveWqesInError();
  }

  EXPECT_EQ(qp.getNumPendingRWqes(), 0)
      << "the error flush releases the bound receive";
  ASSERT_EQ(cq.getNumCompletedWqes(), 1)
      << "the error flush reports the receive the failed response left bound";
  const Cqe cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.workReqId, 0x322)
      << "the error completion belongs to the bound posted receive";
  EXPECT_EQ(cqe.status, kWrFlushErr)
      << "a failed read-with-immediate reports its receive as flushed";
}

/*
 * A read and the send after it on the same QP+Tag take consecutive MSNs, so the
 * send completes in MSN order behind the read. Even when the send's packets are
 * acked before the read response arrives, the send is held until the read
 * completes; the read's CQE is posted first and the send drains behind it.
 */
TEST_F(QpTest, SendCompletionWaitsForEarlierRead) {
  auto readPayload = std::make_unique<char[]>(mtu);
  auto sendPayload = std::make_unique<char[]>(mtu);

  constexpr uint64_t kReadWorkReqId = 101;
  constexpr uint64_t kSendWorkReqId = 202;

  const auto readWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = mtu,
       .workReqId = kReadWorkReqId,
       .address = readPayload.get()});
  ASSERT_EQ(readWqe->allocateNextPosn(), Wqe::kRequestFrameWirePosn)
      << "precondition: a read puts one frame on the wire, and it is the request frame";
  const auto sendWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .workReqId = kSendWorkReqId,
       .address = sendPayload.get()});
  ASSERT_TRUE(allocateSendablePosns(sendWqe))
      << "precondition: the send's frame is on the wire, so only the earlier read holds its completion back";
  const auto readMsn = readWqe->getMsn().value();

  // The send's packets are acked before the read response arrives.
  std::vector<PacketId> ackedSendPackets;
  ackedSendPackets.emplace_back(
      sendWqe, kMetaRoceInitialTag, 0 /* posn */, mtu);
  qp.checkForTxMessageCompletion(ackedSendPackets);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0)
      << "the send must not complete while the read ordered ahead of it is "
         "still outstanding";
  EXPECT_EQ(qp.getNumPendingWqes(), 2)
      << "the read and the send both stay pending until the read completes";

  // The read response arrives, completing the read and releasing the send.
  const auto opType = BaseHeader::Opcode::OperationType::READ_RESP;
  Wqe* receiveWqe = initialTag->getReceiveWqe(opType, readMsn);
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, opType, kMetaRoceInitialTag, readMsn, 0));

  ASSERT_EQ(cq.getNumCompletedWqes(), 2)
      << "completing the read must release its own CQE and the send behind it";
  EXPECT_EQ(qp.getNumPendingWqes(), 0)
      << "no WQE stays pending once the read and the send both complete";

  const auto firstCqe = folly::coro::blockingWait(cq.pollCq());
  const auto secondCqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(firstCqe.workReqId, kReadWorkReqId)
      << "the earlier read completes before the later send";
  EXPECT_EQ(secondCqe.workReqId, kSendWorkReqId)
      << "the send completes in MSN order behind the read";
}

/*
 * Creates a receive WQE and virtual WQE each with 1 outstanding packet and
 * verifies that when the packets are marked as received, the WQEs are marked as
 * completed in order and CDMSN field is populated correctly.
 */
TEST_F(QpTest, SendAndWriteOrderedCompletion) {
  const auto sendOpType = BaseHeader::Opcode::OperationType::SEND;

  auto payload = std::make_unique<char[]>(mtu);
  const auto receiveWqe = postInSequenceReceiveWqe(qp, payload.get(), mtu);
  const auto expectedMsn = initialTag->getCDMSN() + 1;

  // Wqe* receiveWqe = qp.getReceiveWqe(sendOpType, msn);
  // mark posn = 0 (the only posn) as received
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      receiveWqe, sendOpType, kMetaRoceInitialTag, expectedMsn, 0));
  EXPECT_EQ(cq.getNumCompletedWqes(), 1);
  EXPECT_EQ(initialTag->getCDMSN(), expectedMsn);

  const uint16_t nextMsn = expectedMsn + 1;

  const auto dataHeader =
      DataHeader::DataHeaderBuilder()
          .setFsn(1)
          .setUna(1)
          .setPosn(0)
          .setMsn(nextMsn)
          .setCsn(0) // does not matter; write without imm packet
          .build();
  const auto rwHeader = ReadWriteHeader::ReadWriteHeaderBuilder()
                            .setMessageLength(mtu)
                            .setMessageAddr((unsigned long)receiveBuffer.data())
                            .setKey(mrKey)
                            .build();
  const auto writeOpType = BaseHeader::Opcode::OperationType::WRITE;
  int addressError{0};
  initialTag->createVirtualRWqeIfNeeded(
      nextMsn, rwHeader, &addressError, mrs, qp.getMtu());
  initialTag->extendCdmsnTrackerIfNeeded(nextMsn);
  EXPECT_EQ(qp.getNumPendingRWqes(), 1);
  EXPECT_EQ(addressError, 0);

  // mark posn = 0 as received
  const auto virtualReceiveWqe =
      initialTag->getReceiveWqe(writeOpType, nextMsn);
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      virtualReceiveWqe, writeOpType, kMetaRoceInitialTag, nextMsn, 0));
  EXPECT_EQ(qp.getNumPendingRWqes(), 0);
  EXPECT_EQ(cq.getNumCompletedWqes(), 1);
  EXPECT_EQ(initialTag->getCDMSN(), nextMsn);
}

/*
 * Receives 100 write requests, each with 2 outstanding packets, that arrive out
 * of order, and verifies that the CDMSN is updated correctly as the vRWQE are
 * marked as completed.
 */
TEST_F(QpTest, ManyWritesUnorderedArrivalOrderedCompletion) {
  const auto csn = 0;
  const auto opType = BaseHeader::Opcode::OperationType::WRITE;
  const auto rwHeader = ReadWriteHeader::ReadWriteHeaderBuilder()
                            .setMessageLength(mtu * 2)
                            .setMessageAddr((unsigned long)receiveBuffer.data())
                            .setKey(mrKey)
                            .build();

  std::vector<uint16_t> msnsList(100);
  for (int i = 0; i < 100; ++i) {
    msnsList[i] = i + initialTag->getNextMsn();
  }
  auto lastMsn = 99 + initialTag->getNextMsn();
  std::mt19937 rng(0);
  std::shuffle(msnsList.begin(), msnsList.end(), rng);
  int addressError{0};
  for (int i = 0; i < 100; ++i) {
    const auto msn = msnsList[i];
    const auto dataHeader = DataHeader::DataHeaderBuilder()
                                .setFsn(i)
                                .setUna(i)
                                .setPosn(0)
                                .setMsn(msn)
                                .setCsn(csn)
                                .build();
    initialTag->createVirtualRWqeIfNeeded(
        msn, rwHeader, &addressError, mrs, qp.getMtu());
    EXPECT_EQ(addressError, 0);
    initialTag->extendCdmsnTrackerIfNeeded(msn);
    // Mark posn = 0 as received
    Wqe* receiveWqe = initialTag->getReceiveWqe(opType, msn);
    EXPECT_TRUE(qp.checkForRxMessageCompletion(
        receiveWqe, opType, kMetaRoceInitialTag, msn, 0));
  }

  EXPECT_EQ(qp.getNumPendingRWqes(), 100);
  auto expectedMinCDMSN = 0;
  // Mark each vRWQE as completed by marking posn = 1 as received for each msn
  // in reverse order of their first packet arrival.
  // This is due to the fact that it only processes RWQEs at the front
  // of the receive queue.
  for (int i = 99; i >= 0; i--) {
    Wqe* receiveWqe = initialTag->getReceiveWqe(opType, msnsList[i]);
    EXPECT_TRUE(qp.checkForRxMessageCompletion(
        receiveWqe, opType, kMetaRoceInitialTag, msnsList[i], 1 /*posn*/));

    if (msnsList[i] == expectedMinCDMSN) {
      const auto cdmsn = initialTag->getCDMSN();
      EXPECT_GE(cdmsn, expectedMinCDMSN);
      // Update expectedMinCDMSN to the next gap in tracker
      expectedMinCDMSN = cdmsn + 1;
    }
  }
  EXPECT_EQ(qp.getNumPendingRWqes(), 0);
  EXPECT_EQ(initialTag->getCDMSN(), lastMsn);
}

/*
 * Creates two WQEs with 2 and 1 outstanding packets, respecively, marks
 * the second's WQE's packet as received before the first WQE's packets. The
 * expectation is that the second WQE is marked as completed before the first
 * WQE.
 */
TEST_F(QpTest, TwoWQEsTxSideUnorderedCompletion) {
  // shared buffer for sending one or two packets
  auto payload = std::make_unique<char[]>(mtu * 2);
  auto wqe1 = std::make_unique<SendWqe>(
      23 /*workReqId*/,
      oneSge(payload.get(), mtu * 2), // 2 packets
      mtu);
  Wqe* rawWqe1 = wqe1.get();
  ASSERT_TRUE(qp.postWqe(kMetaRoceInitialTag, std::move(wqe1)));

  auto wqe2 = std::make_unique<SendWqe>(
      24 /*workReqId*/,
      oneSge(payload.get(), mtu), // 1 packet
      mtu);
  Wqe* rawWqe2 = wqe2.get();
  ASSERT_TRUE(qp.postWqe(kMetaRoceInitialTag, std::move(wqe2)));
  ASSERT_TRUE(allocateSendablePosns(rawWqe1))
      << "precondition: the two-packet send is on the wire, and is still outstanding when the later send completes";
  ASSERT_TRUE(allocateSendablePosns(rawWqe2))
      << "precondition: the one-packet send is on the wire, so the ack below completes it first";

  // wait for the wqes to be processed by the async task
  try {
    folly::coro::blockingWait(
        folly::coro::timeout(qp.processWqes(), std::chrono::milliseconds(100)));
  } catch (const folly::FutureTimeout&) {
  }

  // Ack wqe2's packet first
  std::vector<PacketId> wqe2NewlyAckedPackets;
  wqe2NewlyAckedPackets.emplace_back(rawWqe2, 0 /* tagNum */, 0 /*posn*/, mtu);
  qp.checkForTxMessageCompletion(wqe2NewlyAckedPackets);
  EXPECT_EQ(qp.getNumPendingWqes(), 2)
      << "WQE 2 acknowledged out-of-order, but both WQE 1 and 2 remain pending "
      << "until WQE 1 completes (in-order completion)";
  EXPECT_EQ(cq.getNumCompletedWqes(), 0)
      << "No completions posted yet since WQE 1 has not completed";

  // Ack both wqe1's packets
  std::vector<PacketId> wqe1NewlyAckedPackets;
  wqe1NewlyAckedPackets.emplace_back(rawWqe1, 0 /* tagNum */, 1 /*posn*/, mtu);
  wqe1NewlyAckedPackets.emplace_back(rawWqe1, 0 /* tagNum */, 0 /*posn*/, mtu);
  qp.checkForTxMessageCompletion(wqe1NewlyAckedPackets);
  EXPECT_EQ(qp.getNumPendingWqes(), 0);
  EXPECT_EQ(cq.getNumCompletedWqes(), 2);
}

/*
 * Creates a virtual RWQE for a write and a receive WQE for a write immediate
 * with 2 and 1 outstanding packets, respecively, marks the second's WQE's
 * packet as received before the first WQE's packets. The expectation is that
 * write immediate is only completed when write is delivered.
 */
TEST_F(QpTest, WriteAndWriteImmRxSideOrderedCompletion) {
  auto msn = initialTag->getCDMSN() + 1;
  auto csn = 0;

  const auto dataHeader = DataHeader::DataHeaderBuilder()
                              .setFsn(0)
                              .setUna(0)
                              .setPosn(0)
                              .setMsn(msn)
                              .setCsn(csn)
                              .build();
  const auto rwHeader = ReadWriteHeader::ReadWriteHeaderBuilder()
                            .setMessageLength(mtu * 2)
                            .setMessageAddr((unsigned long)receiveBuffer.data())
                            .setKey(mrKey)
                            .build();
  int addressError = 0;
  initialTag->createVirtualRWqeIfNeeded(
      msn, rwHeader, &addressError, mrs, qp.getMtu());
  EXPECT_EQ(addressError, 0);
  initialTag->extendCdmsnTrackerIfNeeded(msn);

  msn++;
  auto payload = std::make_unique<char[]>(mtu);
  const auto wImmReceiveWqe = postInSequenceReceiveWqe(qp, payload.get(), mtu);
  initialTag->extendCdmsnTrackerIfNeeded(msn);

  EXPECT_EQ(qp.getNumPendingRWqes(), 2);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);

  const auto wImmOpType = BaseHeader::Opcode::OperationType::WRITE_IMM;
  // const auto wImmReceiveWqe = qp.getReceiveWqe(wImmOpType, msn);
  // mark writeImm's only packet as received
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      wImmReceiveWqe, wImmOpType, kMetaRoceInitialTag, msn, 0 /*posn*/));
  EXPECT_EQ(qp.getNumPendingRWqes(), 2);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);

  const auto wOpType = BaseHeader::Opcode::OperationType::WRITE;
  // mark posn = 0 and posn = 1 of the write as received
  const auto virtualReceiveWqe = initialTag->getReceiveWqe(wOpType, msn - 1);
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      virtualReceiveWqe, wOpType, kMetaRoceInitialTag, msn - 1, 0 /*posn*/));
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      virtualReceiveWqe, wOpType, kMetaRoceInitialTag, msn - 1, 1 /*posn*/));
  EXPECT_EQ(qp.getNumPendingRWqes(), 0);
  EXPECT_EQ(cq.getNumCompletedWqes(), 1);
}

/*
 * Tests the HRNR (Host Receiver Not Ready) backoff mechanism and state
 * transitions.
 *
 * This test verifies the behavior of a QP when it receives an HRNR packet,
 * which indicates that the receiver is not ready to process more packets. The
 * test checks:
 *
 * 1. When an HRNR is received, the QP pauses sending packets for the specified
 *    backoff period
 * 2. After the backoff timer expires, the QP sends a single packet and then
 *    waits for RTO
 * 3. If an ACK is received during backoff, the QP transitions to
 *    NORMAL_TX state and resumes sending packets
 *
 */
TEST_F(QpTest, GetNextPacketToSendWithHrnr) {
  ScopedSuppressedTxTimers suppressTimers;
  // Add congestion controller and flow to QP
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getTxFlowById(0);
  // Create a WQE with 3 packets
  uint16_t msgSizeToSendBytes = mtu * 3;
  auto payload = std::make_unique<char[]>(msgSizeToSendBytes);
  const auto postedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = payload.get()});
  const uint16_t msn = postedWqe->getMsn().value();
  const uint16_t csn = postedWqe->getCsn().value();

  // STEP 1: Send the first packet
  // --------------------------------------
  // Get the first packet - should be available immediately without waiting
  LOG(INFO) << "STEP 1: Send the first packet";
  auto egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "QP should be sending packets in normal operation";

  // Check the first packet
  auto packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  const auto& dataHeader0 = packet->getDataHeader();
  EXPECT_EQ(dataHeader0.getFsn(), 0);
  EXPECT_EQ(dataHeader0.getPosn(), 0);
  EXPECT_EQ(dataHeader0.getMsn(), msn);
  EXPECT_EQ(dataHeader0.getCsn(), csn);
  EXPECT_EQ(dataHeader0.getAckRequest(), true);
  EXPECT_EQ(dataHeader0.getUna(), 0);

  // STEP 2: Simulate receiving an HRNR packet
  // --------------------------------------
  // Create an HRNR packet with a backoff time of 5 * 2^10 = 5120 microseconds
  LOG(INFO) << "STEP 2: Simulate receiving an HRNR packet";
  const auto pktFsn = 0;
  const uint32_t rnrBackoffTimeExp = 10;
  const uint32_t rnrBackoffTimeUs = (5 << rnrBackoffTimeExp);
  LOG(INFO) << "rnrBackoffTimeUs: " << rnrBackoffTimeUs;

  // Get current timestamp for the HRNR packet
  const auto nowNs = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count());

  // Build an HRNR packet
  const AckHeader hrnrHeader =
      AckHeader::AckHeaderBuilder()
          .setAckStatus(AckStatus::HRNR)
          .setRnrBackoffTimeExp(
              rnrBackoffTimeExp) // HRNR status with backoff exponent
          .setCumulativeFsn(pktFsn)
          .setMaxRcvdFsn(pktFsn)
          .setTimestamp(nowNs) // use current time as timestamp instead of echo
          .build();

  // Process the HRNR packet - this should put the QP in BACKOFF state
  flow->processAckPacket(
      hrnrHeader, simulatedProcessedAtNs(hrnrHeader.getTimestamp()));

  // Verify that the HRNR counter is incremented
  EXPECT_EQ(qp.getHrnrPacketsReceived(), 1);

  // Verify that the QP is paused (no packets should be sent during backoff)
  egressPacket = qp.getNextPacketToSend();
  EXPECT_FALSE(egressPacket) << "QP should be paused due to HRNR";

  // STEP 3: Verify single packet behavior during backoff
  // --------------------------------------
  // Drive the backoff-expiry transition deterministically: cancel the real
  // HRNR timer so it cannot fire on the timer thread, then apply the same
  // BLOCKED_TX -> PROBING transition it would and pull the probe packet.
  LOG(INFO) << "STEP 3: Verify single packet behavior during backoff";
  retxTimer.cancelCallback(qp.getHrnrTimerCallback());
  qp.setHrnrState(HrnrState::HRNR_PROBING);
  auto afterRnrTimeoutPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(afterRnrTimeoutPacket)
      << "no post-rnr probe packet was generated.";

  // STEP 4: Verify single packet behavior after backoff
  // --------------------------------------
  // Only a single packet is sent after the backoff timer expires
  // Only one packet is sent to test if receiver is ready
  LOG(INFO) << "STEP 4: Verify single packet behavior after backoff";
  ASSERT_TRUE(afterRnrTimeoutPacket)
      << "QP should send a single packet after backoff time";
  EXPECT_EQ(
      afterRnrTimeoutPacket->packet->getSizeBytes(),
      BaseHeader::kBaseHeaderSizeBytes + DataHeader::kDataHeaderSizeBytes +
          mtu);

  // Verify the packet contents
  packet = dynamic_cast<DataPacket*>(afterRnrTimeoutPacket->packet.get());
  const auto& dataHeader0retx1 = packet->getDataHeader();
  EXPECT_EQ(dataHeader0retx1.getFsn(), 0);
  EXPECT_EQ(dataHeader0retx1.getPosn(), 0);
  EXPECT_EQ(dataHeader0retx1.getMsn(), msn);
  EXPECT_EQ(dataHeader0retx1.getCsn(), csn);
  EXPECT_EQ(dataHeader0retx1.getAckRequest(), true);
  EXPECT_EQ(dataHeader0retx1.getUna(), 0);

  LOG(INFO) << "STEP 4.1: expecting not to get another packet until RTO";
  // Verify that the QP is paused again
  egressPacket = qp.getNextPacketToSend();
  EXPECT_FALSE(egressPacket)
      << "QP should be paused after sending a single packet";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX);

  // The next probe is driven by the HRNR backoff timer re-firing (shorter than
  // the RTO); drive that BLOCKED_TX -> PROBING transition here.
  qp.setHrnrState(HrnrState::HRNR_PROBING);
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket) << "QP should send a packet after RTO";

  // Verify packet contents
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  const auto& dataHeader0retx2 = packet->getDataHeader();
  EXPECT_EQ(dataHeader0retx2.getFsn(), 0);
  EXPECT_EQ(dataHeader0retx2.getPosn(), 0);
  EXPECT_EQ(dataHeader0retx2.getMsn(), msn);
  EXPECT_EQ(dataHeader0retx2.getCsn(), csn);
  EXPECT_EQ(dataHeader0retx2.getAckRequest(), true);
  EXPECT_EQ(dataHeader0retx2.getUna(), 0);

  // STEP 5: Simulate receiving an ACK during backoff
  // --------------------------------------
  // Create a normal ACK packet (not HRNR)
  LOG(INFO) << "STEP 5: Simulate receiving an ACK during backoff";
  const AckHeader ackHeader =
      AckHeader::AckHeaderBuilder()
          .setCumulativeFsn(pktFsn)
          .setMaxRcvdFsn(pktFsn)
          .setTimestamp(nowNs)
          .setAckStatus(AckStatus::ACK) // Normal ACK status
          .build();

  // Process the ACK packet - this should transition the QP to
  // NORMAL_TX state
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  // STEP 6: Verify normal operation resumes
  // --------------------------------------
  // Get the second packet - should be available immediately without waiting
  LOG(INFO) << "STEP 6: Verify normal operation resumes";
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "QP should continue sending packets in normal operation";
  // Check the second packet - we should now be sending packets normally
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  const auto& dataHeader1 = packet->getDataHeader();
  EXPECT_EQ(dataHeader1.getFsn(), 1); // Second packet in sequence
  EXPECT_EQ(dataHeader1.getPosn(), 1); // Second position in WQE
  EXPECT_EQ(dataHeader1.getMsn(), msn);
  EXPECT_EQ(dataHeader1.getCsn(), csn);
  EXPECT_EQ(dataHeader1.getAckRequest(), true);
  EXPECT_EQ(
      dataHeader1.getUna(),
      1); // Una updated to 1 after receiving ACK for packet 0

  // Get the third packet - should be available immediately without waiting
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "QP should continue sending packets in normal operation";

  // Check the third packet
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  const auto& dataHeader2 = packet->getDataHeader();
  EXPECT_EQ(dataHeader2.getFsn(), 2); // Third packet in sequence
  EXPECT_EQ(dataHeader2.getPosn(), 2); // Third position in WQE
  EXPECT_EQ(dataHeader2.getMsn(), msn);
  EXPECT_EQ(dataHeader2.getCsn(), csn);
  EXPECT_EQ(dataHeader2.getAckRequest(), true);
  EXPECT_EQ(
      dataHeader2.getUna(), 1); // Una still 1 as we haven't received more ACKs
}

/*
 * HRNR_PROBING lets traffic pass until exactly one *data* packet is sent, then
 * blocks. A queued ack (from a received data packet) is emitted without ending
 * PROBING; only the first data probe transitions PROBING -> BLOCKED_TX, after
 * which no further packet goes out this cycle even though retransmissions are
 * still pending. The test does not pin *when* the ack leaves, only that it does
 * and that PROBING persists until the first data packet.
 */
TEST_F(QpTest, HrnrProbingSendsOneDataProbeDespiteQueuedAck) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);

  // Post a three-packet message and send it so three FSNs are in flight, then
  // queue them all for retransmission (as an RTO would) so more than one probe
  // candidate is pending.
  const uint16_t msgSizeToSendBytes = mtu * 3;
  auto sendPayload = std::make_unique<char[]>(msgSizeToSendBytes);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = sendPayload.get()});
  while (qp.getNextPacketToSend() != nullptr) {
    // send the initial transmission
  }
  txFlow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);
  ASSERT_GT(txFlow->getNumPendingRetxPackets(), 1u)
      << "test needs multiple pending retransmissions to show only one is sent";

  // Queue an ack: it must neither stand in for the probe nor block it.
  qp.getRxFlowById(0)->setPendingAck(
      AckHeader::AckHeaderBuilder()
          .setAckStatus(AckStatus::ACK)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setTimestamp(0)
          .build(),
      kIngressSourcePort);

  // Enter blocked (as an HRNR NAK would) then probing (as the backoff-timer
  // expiry would); PROBING is only reachable from BLOCKED_TX.
  qp.setHrnrState(HrnrState::HRNR_BLOCKED_TX);
  qp.setHrnrState(HrnrState::HRNR_PROBING);

  // Only a data packet ends PROBING: exactly one probe is sent despite the
  // queued ack, and any ack sent first leaves the state untouched.
  int dataProbes = 0;
  for (int i = 0; i < 8 && dataProbes == 0; ++i) {
    auto egress = qp.getNextPacketToSend();
    ASSERT_TRUE(egress)
        << "PROBING must send a data probe (any queued ack first)";
    if (egress->packet->getBaseHeader().getOpcode().isData()) {
      ++dataProbes;
      EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
          << "the data probe must end PROBING";
    } else {
      EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_PROBING)
          << "a non-data packet (an ack) must not end PROBING";
    }
  }

  EXPECT_EQ(dataProbes, 1) << "exactly one data probe should end PROBING";
  EXPECT_FALSE(qp.getNextPacketToSend())
      << "after one data probe the QP must block, not keep sending pending "
         "retransmissions while still in PROBING";
}

TEST_F(QpTest, HrnrBlockedTxSendsAcksButNotData) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);

  // Post a three-packet message and send it, then queue it for retransmission
  // so there is data that would be sent if HRNR were not blocking.
  const uint16_t msgSizeToSendBytes = mtu * 3;
  auto sendPayload = std::make_unique<char[]>(msgSizeToSendBytes);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = sendPayload.get()});
  while (qp.getNextPacketToSend() != nullptr) {
    // send the initial transmission
  }
  txFlow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);
  ASSERT_GT(txFlow->getNumPendingRetxPackets(), 0u)
      << "test needs pending data to prove data stays blocked under HRNR";

  // Queue an ack for transmission (as receiving a data packet would).
  qp.getRxFlowById(0)->setPendingAck(
      AckHeader::AckHeaderBuilder()
          .setAckStatus(AckStatus::ACK)
          .setCumulativeFsn(0)
          .setMaxRcvdFsn(0)
          .setTimestamp(0)
          .build(),
      kIngressSourcePort);

  // Block on HRNR with a live backoff (as an HRNR NAK would): new data is held
  // until the backoff elapses, but acks still flow.
  qp.enterHrnrBlock(60 * 1000 * 1000 /*60s*/);

  // The pending ack is still sent: acks are unaffected by the send-side block.
  auto ack = qp.getNextPacketToSend();
  ASSERT_TRUE(ack && ack->packet)
      << "a pending ack must be sent even while HRNR_BLOCKED_TX so the peer can "
         "recover and clear the HRNR";
  EXPECT_FALSE(ack->packet->getBaseHeader().getOpcode().isData())
      << "the packet sent under HRNR_BLOCKED_TX must be the ack, not data";

  // ...but new data stays blocked, and sending the ack did not change state.
  EXPECT_FALSE(qp.getNextPacketToSend())
      << "new data must stay blocked while HRNR_BLOCKED_TX even though "
         "retransmissions are pending";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "sending an ack must not change the HRNR state";
}

TEST_F(QpTest, ReceivedAckRingCountsWhatItDrops) {
  ScopedSuppressedTxTimers suppressTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  ASSERT_TRUE(qp.usesSingleOwnerRxRing())
      << "the ring must exist on this QP, since the drop under test happens on "
         "enqueue to that ring";

  const AckHeader ack = AckHeader::AckHeaderBuilder()
                            .setAckStatus(AckStatus::ACK)
                            .setCumulativeFsn(0)
                            .setMaxRcvdFsn(0)
                            .setTimestamp(0)
                            .build();

  // Enqueue past the ring's depth without ever visiting the sender, so nothing
  // drains. The overrun has to be counted for teardown to report it.
  for (uint32_t i = 0; i < kPendingRxEventsCapacity + 8; ++i) {
    qp.enqueueReceivedAck(0 /*flowId*/, ack, 0 /*recvNowNs*/);
  }

  EXPECT_GT(qp.getAcksDroppedOnFullRing(), 0u)
      << "overrunning the ring must be counted, not silently discarded";
}

TEST_F(QpTest, RnrCancelIsAppliedAtTheSendersVisitNotOnReceipt) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  ASSERT_TRUE(qp.usesSingleOwnerRxRing())
      << "the ring must exist on this QP for the cancel to be deferred at all";

  qp.enterHrnrBlock(60 * 1000 * 1000 /*60s*/);
  qp.enqueueReceivedRnrCancel(RnrCancelHeader(
      0 /*flowsAvailable*/,
      kMetaRoceInitialTag,
      0 /*postedCsn*/,
      0 /*rateHints*/));
  EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "the sender applies an enqueued RNR-cancel at its next arbiter visit, "
         "so the block still stands until it drains";

  // A sender visit, as the arbiter callback runs it: drain what the receiver
  // handed off, then send.
  qp.applyHandedOffAcks();
  qp.getNextPacketToSend();
  EXPECT_EQ(qp.getHrnrState(), HrnrState::NORMAL_TX)
      << "the sender's visit must drain the RNR-cancel and lift the block";
}

TEST_F(QpTest, RnrCancelQueuedBehindAnHrnrIsAppliedAfterIt) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Send a three-packet message so the HRNR below covers outstanding data and
  // is not discarded as stale.
  const uint16_t msgSizeToSendBytes = mtu * 3;
  auto sendPayload = std::make_unique<char[]>(msgSizeToSendBytes);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = sendPayload.get()});
  while (qp.getNextPacketToSend() != nullptr) {
    // send the initial transmission
  }

  // The peer NAKs HRNR and then, once its QP is up, cancels. Sharing one ring
  // keeps that order: applied out of order the cancel is consumed first and the
  // HRNR blocks a QP the peer has already told to resume.
  const AckHeader hrnr = AckHeader::AckHeaderBuilder()
                             .setAckStatus(AckStatus::HRNR)
                             .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                             .setCumulativeFsn(0)
                             .setMaxRcvdFsn(0)
                             .setTimestamp(0)
                             .build();
  qp.enqueueReceivedAck(0 /*flowId*/, hrnr, 0 /*recvNowNs*/);
  qp.enqueueReceivedRnrCancel(RnrCancelHeader(
      0 /*flowsAvailable*/,
      kMetaRoceInitialTag,
      0 /*postedCsn*/,
      0 /*rateHints*/));

  // A sender visit, as the arbiter callback runs it: drain what the receiver
  // handed off, then send.
  qp.applyHandedOffAcks();
  qp.getNextPacketToSend();

  ASSERT_EQ(qp.getHrnrPacketsReceived(), 1u)
      << "the drain must have applied the HRNR, or the resumed state below "
         "says nothing about the order the two were applied in";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::NORMAL_TX)
      << "the cancel arrived after the HRNR and must be applied after it, "
         "leaving the QP resumed rather than blocked";
}

TEST_F(QpTest, HrnrClearsOnAnAckThatAdvancesNothing) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);

  // Send a three-packet message, so fsn 0..2 are outstanding.
  const uint16_t msgSizeToSendBytes = mtu * 3;
  auto sendPayload = std::make_unique<char[]>(msgSizeToSendBytes);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = sendPayload.get()});
  while (qp.getNextPacketToSend() != nullptr) {
    // send the initial transmission
  }

  // Sack fsn 2 out of order. cumulativeFsn sits one before sndUna, so nothing
  // is cumulatively acked and sndUna stays at 0 with fsn 0 and 1 outstanding.
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(2);
  const AckHeader sackAck = AckHeader::AckHeaderBuilder()
                                .setCumulativeFsn(0xffff)
                                .setMaxRcvdFsn(2)
                                .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                                .setFsnBitVector(fsnBitVector)
                                .setTimestamp(0)
                                .build();
  txFlow->processAckPacket(
      sackAck, simulatedProcessedAtNs(sackAck.getTimestamp()));
  ASSERT_EQ(qp.getHrnrState(), HrnrState::NORMAL_TX)
      << "the QP must start unblocked, so the block set below is the only one "
         "this test observes being lifted";

  // Block on HRNR (as an HRNR NAK would), then replay the same sack: every
  // frame it covers is acked already, so it advances nothing, but it is still
  // a live ack from the receiver.
  qp.setHrnrState(HrnrState::HRNR_BLOCKED_TX);
  txFlow->processAckPacket(
      sackAck, simulatedProcessedAtNs(sackAck.getTimestamp()));

  EXPECT_EQ(qp.getHrnrState(), HrnrState::NORMAL_TX)
      << "an ack proves the receiver is responsive whether or not it advances "
         "sndUna, so it must lift the HRNR block";
}

TEST_F(QpTest, HrnrClearsOnABrnrThatDrainsTheLastUnackedFrame) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);

  // Send a three-packet message, so fsn 0..2 are outstanding.
  const uint16_t msgSizeToSendBytes = mtu * 3;
  auto sendPayload = std::make_unique<char[]>(msgSizeToSendBytes);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = sendPayload.get()});
  while (qp.getNextPacketToSend() != nullptr) {
    // send the initial transmission
  }

  // Block on HRNR (as an HRNR NAK would), then RNR every outstanding frame:
  // a BRNR'd frame advances sndUna without counting as newly acked, draining
  // the QP of anything a probe could carry.
  qp.setHrnrState(HrnrState::HRNR_BLOCKED_TX);
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(0);
  rnrBitVector.setBit(1);
  rnrBitVector.setBit(2);
  const AckHeader brnr = AckHeader::AckHeaderBuilder()
                             .setCumulativeFsn(0xffff)
                             .setMaxRcvdFsn(2)
                             .setAckStatus(AckStatus::BRNR)
                             .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                             .setRnrBitVector(rnrBitVector)
                             .setTimestamp(0)
                             .build();
  txFlow->processAckPacket(brnr, simulatedProcessedAtNs(brnr.getTimestamp()));

  ASSERT_EQ(txFlow->getTxState().sndUna, txFlow->getTxState().sndNxt)
      << "the BRNR must leave the flow with nothing unacked, the state in "
         "which an armed HRNR block has no probe left that could lift it";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::NORMAL_TX)
      << "the block must go with the frames a probe would have carried, or the "
         "QP stalls until an RNR-cancel or the watchdog";
}

TEST_F(QpTest, HrnrForAnAckedFsnIsIgnored) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);

  // Send a three-packet message and ack all of it, so the QP holds no
  // unacked posn.
  const uint16_t msgSizeToSendBytes = mtu * 3;
  auto sendPayload = std::make_unique<char[]>(msgSizeToSendBytes);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = sendPayload.get()});
  while (qp.getNextPacketToSend() != nullptr) {
    // send the initial transmission
  }
  const AckHeader fullAck = AckHeader::AckHeaderBuilder()
                                .setAckStatus(AckStatus::ACK)
                                .setCumulativeFsn(2)
                                .setMaxRcvdFsn(2)
                                .setTimestamp(0)
                                .build();
  txFlow->processAckPacket(
      fullAck, simulatedProcessedAtNs(fullAck.getTimestamp()));
  ASSERT_EQ(txFlow->getTxState().sndUna, txFlow->getTxState().sndNxt)
      << "the QP must have no unacked data left, so the HRNR below refers "
         "only to a packet that has since been acked";

  // An HRNR carries the soliciting packet's fsn in both fsn fields. This one
  // arrives reordered behind the ack that covered fsn 2, so it reports a
  // condition the receiver has since resolved.
  const AckHeader lateHrnr = AckHeader::AckHeaderBuilder()
                                 .setAckStatus(AckStatus::HRNR)
                                 .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                                 .setCumulativeFsn(2)
                                 .setMaxRcvdFsn(2)
                                 .setTimestamp(0)
                                 .build();
  txFlow->processAckPacket(
      lateHrnr, simulatedProcessedAtNs(lateHrnr.getTimestamp()));

  EXPECT_EQ(qp.getHrnrState(), HrnrState::NORMAL_TX)
      << "an HRNR for an already-acked packet is stale and must not block the "
         "QP";
}

// Fixture for the HRNR probe cycle, whose tests all need a QP holding unacked
// data that a probe can carry.
class QpHrnrProbeTest : public QpTest {
 protected:
  // Post a three-packet message, leaving it unsent.
  void postProbeMessage() {
    const uint32_t msgSizeToSendBytes = mtu * 3;
    sendPayload_ = std::make_unique<char[]>(msgSizeToSendBytes);
    postWqeHelper(
        &qp,
        {.workType = Wqe::WorkType::kSend,
         .msgSizeBytes = msgSizeToSendBytes,
         .address = sendPayload_.get()});
  }

  // Post a three-packet message and send it, then queue it for retransmission
  // so the send path has a retransmission available to use as the probe.
  void primeProbeableData(TxFlow* txFlow) {
    postProbeMessage();
    while (qp.getNextPacketToSend() != nullptr) {
      // send the initial transmission
    }
    txFlow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);
  }

  // Outlives the WQE that points to it.
  std::unique_ptr<char[]> sendPayload_;
};

TEST_F(QpHrnrProbeTest, HrnrBlockedTxProbesFromSendPathAfterBackoff) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);
  primeProbeableData(txFlow);
  ASSERT_GT(txFlow->getNumPendingRetxPackets(), 0u)
      << "test needs pending data to probe with";

  // Block on HRNR with a deadline far in the future: the sender must hold the
  // data, not probe, before the backoff elapses.
  qp.enterHrnrBlock(60 * 1000 * 1000 /*60s*/);
  EXPECT_FALSE(qp.getNextPacketToSend())
      << "the sender must not probe before the backoff deadline";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "the sender must stay BLOCKED_TX before the deadline";

  // Once the backoff has elapsed, the sender itself leaves BLOCKED_TX for
  // PROBING and emits exactly one data probe from the send path -- the timer
  // never writes hrnrState_.
  qp.enterHrnrBlock(0 /*deadline is now*/);
  auto probe = qp.getNextPacketToSend();
  ASSERT_TRUE(probe && probe->packet)
      << "the sender must emit a probe once the backoff has elapsed";
  EXPECT_TRUE(probe->packet->getBaseHeader().getOpcode().isData())
      << "the probe must be a data packet";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "after one probe the sender must return to BLOCKED_TX";

  // Sending the probe re-arms the backoff, so the next visit does not
  // immediately re-probe: one probe per backoff, not a drain of all
  // retransmissions.
  EXPECT_FALSE(qp.getNextPacketToSend())
      << "the sender must wait out the re-armed backoff before the next probe";
}

TEST_F(QpHrnrProbeTest, HrnrProbeDrawsOnNewWqeWhenNothingToRetransmit) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);
  postProbeMessage();
  ASSERT_EQ(txFlow->getNumPendingRetxPackets(), 0u)
      << "the probe here has to come from the unsent WQE, so the flow must "
         "hold no retransmission to draw on";

  qp.enterHrnrBlock(0 /*deadline is now*/);
  auto probe = qp.getNextPacketToSend();
  ASSERT_TRUE(probe && probe->packet)
      << "the sender must probe with new data when no retransmission is "
         "pending";
  EXPECT_TRUE(probe->packet->getBaseHeader().getOpcode().isData())
      << "the probe must be a data packet";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "the sender must re-enter the block after the single probe";

  EXPECT_FALSE(qp.getNextPacketToSend())
      << "one packet per backoff, even when the probe came from a WQE with "
         "further packets behind it";
}

TEST_F(QpTest, HrnrBackoffTimerCallbackSignalsSenderReady) {
  ScopedSuppressedTxTimers suppressTimers;
  // A running sender loop is what drains the arbiter's lock-free intake, and
  // this test has none; the mutex path records readiness where isQpnReady()
  // can observe it.
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_lockfree_signal = false;

  qp.enterHrnrBlock(60 * 1000 * 1000 /*60s*/);
  ASSERT_FALSE(arbiter.isQpnReady(qp.getLocalQpNum()))
      << "entering the HRNR block must leave the sender parked, so that the "
         "readiness checked below is attributable to the timer callback";

  (*qp.getHrnrTimerCallback())();
  EXPECT_TRUE(arbiter.isQpnReady(qp.getLocalQpNum()))
      << "the HRNR backoff timer callback must wake the sender to revisit the "
         "QP; nothing else brings the sender back to a blocked QP at its "
         "deadline";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "the HRNR backoff timer callback must not write hrnrState_; the "
         "sender owns the BLOCKED_TX -> PROBING transition";
}

TEST_F(QpHrnrProbeTest, HrnrBackoffTimerCallbackDrivesSenderProbe) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);
  primeProbeableData(txFlow);
  ASSERT_GT(txFlow->getNumPendingRetxPackets(), 0u)
      << "test needs pending data to probe with";

  // An already-elapsed deadline stands in for a backoff that ran out, so the
  // callback and the sender visit it wakes run the cycle end to end.
  qp.enterHrnrBlock(0 /*deadline is now*/);
  (*qp.getHrnrTimerCallback())();
  ASSERT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "the HRNR backoff timer callback must not write hrnrState_; the "
         "probe below has to come from the sender's own transition";

  auto probe = qp.getNextPacketToSend();
  ASSERT_TRUE(probe && probe->packet)
      << "the woken sender must emit a probe for a backoff that has elapsed";
  EXPECT_TRUE(probe->packet->getBaseHeader().getOpcode().isData())
      << "the probe must be a data packet, not an ack";
  EXPECT_EQ(qp.getHrnrState(), HrnrState::HRNR_BLOCKED_TX)
      << "after one probe the sender must re-enter the block";
}

/*
 * Tests the RNR (Receiver Not Ready) cancel feature.
 *
 * This test verifies that the cancelRnrBackoff() method properly cancels
 * the HRNR backoff state and allows the QP to immediately resume normal
 * operation without waiting for the backoff timer to expire. The test follows
 * these steps:
 *
 * 1. Creates a WQE and sends some data packets
 * 2. Simulates receiving an HRNR packet which puts the QP in
 *    HrnrState::BLOCKED_TX
 * 3. Verifies that the QP is paused (no packets can be sent during backoff)
 * 4. Calls the cancelRnrBackoff() method to explicitly cancel the backoff
 * 5. Verifies that the QP immediately resumes normal operation
 * 6. Confirms that remaining packets can be sent without waiting for the
 *    backoff timer
 *
 */
TEST_F(QpTest, CancelRnrBackoff) {
  // STEP 1: Setup the QP and create a WQE with 3 packets
  // --------------------------------------
  LOG(INFO) << "STEP 1: Setup the QP and create a WQE with 3 packets";
  // Add congestion controller and flow to QP
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getTxFlowById(0);

  // Create a WQE with 3 packets
  uint16_t msgSizeToSendBytes = mtu * 3;
  auto payload = std::make_unique<char[]>(msgSizeToSendBytes);
  const auto postedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = payload.get()});
  const uint16_t msn = postedWqe->getMsn().value();
  const uint16_t csn = postedWqe->getCsn().value();

  // STEP 2: Send the first packet
  // --------------------------------------
  LOG(INFO) << "STEP 2: Send the first packet";
  auto egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket) << "First packet should be available to send";

  auto packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  const auto& dataHeader = packet->getDataHeader();
  EXPECT_EQ(dataHeader.getFsn(), 0);
  EXPECT_EQ(dataHeader.getPosn(), 0);

  // STEP 3: Simulate receiving an HRNR packet
  // --------------------------------------
  LOG(INFO) << "STEP 3: Simulate receiving an HRNR packet";
  // Create an HRNR packet with a backoff time of 5 * 2^10 = 5120 microseconds
  const auto pktFsn = 0;
  const uint32_t rnrBackoffTimeExp = 10;
  const uint32_t rnrBackoffTimeUs =
      (5 << rnrBackoffTimeExp); // 5120 us backoff time
  LOG(INFO) << "rnrBackoffTimeUs: " << rnrBackoffTimeUs;

  // Get current timestamp for the HRNR packet
  const auto nowNs = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count());

  // Build an HRNR packet
  const AckHeader hrnrHeader = AckHeader::AckHeaderBuilder()
                                   .setAckStatus(AckStatus::HRNR)
                                   .setRnrBackoffTimeExp(rnrBackoffTimeExp)
                                   .setCumulativeFsn(pktFsn)
                                   .setMaxRcvdFsn(pktFsn)
                                   .setTimestamp(nowNs)
                                   .build();

  // Process the HRNR packet - this should put the QP in
  // HrnrState::BLOCKED_TX
  flow->processAckPacket(
      hrnrHeader, simulatedProcessedAtNs(hrnrHeader.getTimestamp()));

  // STEP 4: Verify QP is paused due to HRNR
  // --------------------------------------
  LOG(INFO) << "STEP 4: Verify QP is paused due to HRNR";
  // Verify that the HRNR received count is incremented
  EXPECT_EQ(qp.getHrnrPacketsReceived(), 1);

  // Verify that the QP is paused (no packets should be sent during backoff)
  egressPacket = qp.getNextPacketToSend();
  EXPECT_FALSE(egressPacket) << "QP should be paused due to HRNR";

  // STEP 5: Cancel the RNR backoff
  // --------------------------------------
  LOG(INFO) << "STEP 5: Cancel the RNR backoff";
  qp.cancelRnrBackoff(kMetaRoceInitialTag);

  // STEP 6: Verify QP immediately resumes normal operation
  // --------------------------------------
  LOG(INFO) << "STEP 6: Verify QP immediately resumes normal operation";
  // Verify that packets can be sent immediately after cancellation
  // without waiting for the backoff timer to expire
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "QP should resume sending packets immediately after RNR cancel";

  // All packets should be sent immediately after canceling RNR
  // since nothing has been acked by now
  // Verify the first packet contents
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  const auto& dataHeader0 = packet->getDataHeader();
  EXPECT_EQ(dataHeader0.getFsn(), 0);
  EXPECT_EQ(dataHeader0.getPosn(), 0);
  EXPECT_EQ(dataHeader0.getMsn(), msn);
  EXPECT_EQ(dataHeader0.getCsn(), csn);

  // Verify the second packet contents
  egressPacket = qp.getNextPacketToSend();
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  const auto& dataHeader1 = packet->getDataHeader();
  EXPECT_EQ(dataHeader1.getFsn(), 1);
  EXPECT_EQ(dataHeader1.getPosn(), 1);
  EXPECT_EQ(dataHeader1.getMsn(), msn);
  EXPECT_EQ(dataHeader1.getCsn(), csn);

  // Get the third packet - should be available immediately without waiting
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket)
      << "QP should continue sending packets in normal operation";

  // Verify the third packet contents
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  const auto& dataHeader2 = packet->getDataHeader();
  EXPECT_EQ(dataHeader2.getFsn(), 2);
  EXPECT_EQ(dataHeader2.getPosn(), 2);
  EXPECT_EQ(dataHeader2.getMsn(), msn);
  EXPECT_EQ(dataHeader2.getCsn(), csn);

  // Verify no more packets are available to send
  egressPacket = qp.getNextPacketToSend();
  EXPECT_FALSE(egressPacket) << "All packets should have been sent";
}

/*
 * Tests the BRNR sender functionality for
 * processing ACK packets with RNR bit vector set.
 *
 * This test verifies that when an ACK packet with RNR bit vector is received:
 * 1. The sender correctly advances sndUna for contiguous ACK/RNR sequences
 * 2. Positions for RNR'd FSNs are properly deallocated
 * 3. The rnrFirstBlockedMsn_ field is correctly updated
 */
TEST_F(QpTest, ProcessAckPacketWithRnrBitVector) {
  // Create and add flow
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Get the flow
  auto flow = qp.getTxFlowById(0);

  // Create a WQE with multiple packets
  const int frames = 20;
  const uint32_t length = frames * mtu;
  auto payload = std::make_unique<char[]>(length);
  auto existingWqe = std::make_unique<SendWqe>(
      23 /* workReqId */,
      oneSge(
          payload.get(), /*address*/
          length),
      mtu);
  existingWqe->setMsn(initialTag->getAndIncrementNextMsn());
  existingWqe->setCsn(initialTag->getAndIncrementNextCsn());
  uint16_t msn = existingWqe->getMsn().value();

  auto nonExistingWqe = std::make_unique<SendWqe>(
      23 /*workReqId*/,
      oneSge(
          payload.get(), /*address*/
          length), /*length*/
      mtu);
  nonExistingWqe->setMsn(initialTag->getAndIncrementNextMsn());
  nonExistingWqe->setCsn(initialTag->getAndIncrementNextCsn());

  // Set up the txState with specific values
  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.maxAckdFsn = 12;

  // Add packets to the tracker
  // FSNs 10-15 with different states
  txState.tracker.emplace_back(
      PacketId(existingWqe.get(), 0, 10, mtu),
      Flow::FsnState::UNACKED); // FSN 10
  txState.tracker.emplace_back(
      PacketId(existingWqe.get(), 0, 11, mtu),
      Flow::FsnState::UNACKED); // FSN 11
  txState.tracker.emplace_back(
      PacketId(existingWqe.get(), 0, 12, mtu),
      Flow::FsnState::ACKED); // FSN 12
  txState.tracker.emplace_back(
      PacketId(nonExistingWqe.get(), 0, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 13
  txState.tracker.emplace_back(
      PacketId(nonExistingWqe.get(), 0, 1, mtu),
      Flow::FsnState::UNACKED); // FSN 14
  txState.tracker.emplace_back(
      PacketId(existingWqe.get(), 0, 13, mtu),
      Flow::FsnState::UNACKED); // FSN 15
  txState.sndNxt = 16;

  ASSERT_TRUE(allocatePosns(*existingWqe, 14));
  for (uint32_t posn = 0; posn < 10; ++posn) {
    existingWqe->acknowledgePosn(posn);
  }
  existingWqe->acknowledgePosn(12);
  ASSERT_TRUE(allocatePosns(*nonExistingWqe, 2));

  // Create an ACK packet with RNR bit vector set
  // FSN 13 is RNR'd, FSN 15 is ACKed
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(
      2); // FSN 15 is acked out of order (position 2 after cumulativeFsn 12)

  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(0); // FSN 13 is RNR'd (position 0 after cumulativeFsn 12)

  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(12)
                                  .setMaxRcvdFsn(15)
                                  .setAckStatus(AckStatus::BRNR)
                                  .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                                  .setFsnBitVector(fsnBitVector)
                                  .setRnrBitVector(rnrBitVector)
                                  .setTimestamp(0)
                                  .build();

  // Verify that rnrFirstBlockedMsn_ is not set before processing the ACK
  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value());

  // Process the ACK packet
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  // Verify that sndUna is advanced correctly
  // sndUna should be 14 (advanced past FSN 13 which is RNR'd)
  EXPECT_EQ(txState.sndUna, 14);

  // Verify that the tracker size is reduced
  // FSNs 10, 11, 12, and 13 should be removed from the tracker
  EXPECT_EQ(txState.tracker.size(), 2);

  // Verify the remaining tracker entries
  EXPECT_EQ(txState.tracker.at(0).id.posn, 1);
  EXPECT_TRUE(txState.tracker.at(0).isUnacked());

  EXPECT_EQ(txState.tracker.at(1).id.posn, 13);
  EXPECT_TRUE(txState.tracker.at(1).isAcked());

  // Verify that rnrFirstBlockedMsn_ is set correctly
  ASSERT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), msn + 1);
}

/*
 * A single BRNR ack can carry BRNR'd frames from different tags on the same
 * flow. Each frame's tag is recovered from the tracker, so every affected tag
 * is paused at its own first-blocked MSN; the ACK's Tag field does not decide
 * which tag is paused.
 */
TEST_F(QpTest, ProcessBrnrAckPausesEachFramesOwnTag) {
  qp.addTag(1, &cq, ibvQp);
  auto tag0 = qp.getTag(kMetaRoceInitialTag);
  auto tag1 = qp.getTag(1);

  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  // One in-flight frame per tag on this flow, with distinct MSNs so a fix that
  // mis-attributes both to a single tag is caught.
  constexpr uint16_t msn0 = 3;
  constexpr uint16_t msn1 = 1;
  auto wqe0 =
      std::make_unique<SendWqe>(23, oneSge(receiveBuffer.data(), mtu), mtu);
  wqe0->setMsn(msn0);
  ASSERT_TRUE(allocatePosns(*wqe0, 1));

  auto wqe1 =
      std::make_unique<SendWqe>(24, oneSge(receiveBuffer.data(), mtu), mtu);
  wqe1->setMsn(msn1);
  ASSERT_TRUE(allocatePosns(*wqe1, 1));

  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.sndNxt = 12;
  txState.maxAckdFsn = 9;
  txState.tracker.emplace_back(
      PacketId(wqe0.get(), kMetaRoceInitialTag, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 10, tag 0
  txState.tracker.emplace_back(
      PacketId(wqe1.get(), 1, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 11, tag 1

  // BRNR both frames: cumulativeFsn 9 leaves the sndUna=10 window intact and
  // the RNR bits at offsets 0 and 1 cover FSN 10 (tag 0) and FSN 11 (tag 1).
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(0);
  rnrBitVector.setBit(1);
  const AckHeader ackHeader =
      AckHeader::AckHeaderBuilder()
          .setTag(kMetaRoceInitialTag) // for tag 0; tag 1 must still pause
          .setCumulativeFsn(9)
          .setMaxRcvdFsn(11)
          .setAckStatus(AckStatus::BRNR)
          .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
          .setRnrBitVector(rnrBitVector)
          .setTimestamp(0)
          .build();

  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  ASSERT_TRUE(tag0->getBrnrFirstBlockedMsn().has_value())
      << "tag 0 owns a BRNR'd frame and must be paused";
  EXPECT_EQ(tag0->getBrnrFirstBlockedMsn().value(), msn0)
      << "tag 0 must be paused at its own frame's MSN";
  ASSERT_TRUE(tag1->getBrnrFirstBlockedMsn().has_value())
      << "tag 1 owns a BRNR'd frame and must be paused even though the ACK's "
         "Tag field holds tag 0";
  EXPECT_EQ(tag1->getBrnrFirstBlockedMsn().value(), msn1)
      << "tag 1 must be paused at its own frame's MSN, not tag 0's";
}

// One message per frame, so each tracker entry has its own csn. Frames
// start at fsn 10, where the window below is opened.
struct BrnrPerCsnFixture {
  LimitedTestSender mockSender;
  TxFlow* flow;
  TxFlow::TxState* txState;
  std::vector<std::unique_ptr<char[]>> payloads;
  std::vector<std::unique_ptr<SendWqe>> wqes;
  std::vector<uint16_t> csns;
  std::vector<uint16_t> msns;

  BrnrPerCsnFixture(
      QpArbiter& arbiter,
      Qp& qp,
      uint16_t mtu,
      std::shared_ptr<Tag>& tag,
      int count)
      : mockSender(arbiter, mockUDPSocketMaker) {
    qp.setCongestionController(std::make_unique<MockCongestionController>());
    qp.setupFlows(1, 1, &mockSender);
    flow = qp.getTxFlowById(0);
    txState = &flow->getTxState();
    txState->sndUna = 10;
    *(txState->sndCur.wlock()) = 10 + count;
    txState->maxAckdFsn = 9;
    txState->sndNxt = 10 + count;
    for (int i = 0; i < count; ++i) {
      payloads.push_back(std::make_unique<char[]>(mtu));
      auto wqe = std::make_unique<SendWqe>(
          100 + i, oneSge(payloads.back().get(), mtu), mtu);
      wqe->setMsn(tag->getAndIncrementNextMsn());
      wqe->setCsn(tag->getAndIncrementNextCsn());
      msns.push_back(wqe->getMsn().value());
      csns.push_back(wqe->getCsn().value());
      const uint32_t posn = wqe->allocateNextPosn();
      txState->tracker.emplace_back(
          PacketId(wqe.get(), 0, posn, mtu), Flow::FsnState::UNACKED);
      wqes.push_back(std::move(wqe));
    }
  }

  [[nodiscard]] AckHeader brnrForAllFrames() const {
    std::vector<size_t> all;
    all.reserve(wqes.size());
    for (size_t i = 0; i < wqes.size(); ++i) {
      all.push_back(i);
    }
    return brnrForFrames(all);
  }

  // A BRNR reporting only the frames at these tracker offsets, as a responder
  // refusing them one message at a time reports them.
  [[nodiscard]] AckHeader brnrForFrames(
      const std::vector<size_t>& which) const {
    BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
    for (const size_t i : which) {
      rnrBitVector.setBit(i);
    }
    return AckHeader::AckHeaderBuilder()
        .setTag(kMetaRoceInitialTag)
        .setCumulativeFsn(9)
        .setMaxRcvdFsn(9 + wqes.size())
        .setAckStatus(AckStatus::BRNR)
        .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
        .setRnrBitVector(rnrBitVector)
        .setTimestamp(0)
        .build();
  }
};

TEST_F(QpTest, AnRnrCancelCoveringEveryRefusedFrameLeavesTheTagSending) {
  BrnrPerCsnFixture fixture(arbiter, qp, mtu, initialTag, /*count=*/3);
  constexpr uint32_t kCanceledAtNs = 1000;

  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[2], kCanceledAtNs);
  ASSERT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_CANCELED)
      << "precondition: the cancel puts the tag in the canceled state";

  fixture.flow->processAckPacket(fixture.brnrForAllFrames(), kCanceledAtNs);

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_CANCELED)
      << "every reported frame is covered, so pausing on any would hold the "
         "tag off a waiting receive";
}

TEST_F(QpTest, AnRnrCancelCoveringOneFrameLeavesTheTagBlockedAtTheNext) {
  BrnrPerCsnFixture fixture(arbiter, qp, mtu, initialTag, /*count=*/3);
  constexpr uint32_t kCanceledAtNs = 1000;

  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[0], kCanceledAtNs);
  fixture.flow->processAckPacket(fixture.brnrForAllFrames(), kCanceledAtNs);

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "only the first frame is covered, so the rest pause the tag";
  ASSERT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value())
      << "the send path reads the blocked msn to hold frames back";
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), fixture.msns[1])
      << "the pause starts at the first frame the cancel left refused";
}

// The cancel can overtake the BRNR it follows.
TEST_F(QpTest, AnRnrCancelForATagNeverPausedStillCoversItsFrames) {
  BrnrPerCsnFixture fixture(arbiter, qp, mtu, initialTag, /*count=*/1);
  constexpr uint32_t kCanceledAtNs = 1000;

  ASSERT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX)
      << "precondition: the tag is sending, so the cancel finds no pause";
  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[0], kCanceledAtNs);
  fixture.flow->processAckPacket(fixture.brnrForAllFrames(), kCanceledAtNs);

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_CANCELED)
      << "a cancel arriving before the refusal still covers it";
}

TEST_F(QpTest, ALaterRnrCancelCoversTheFramesBehindIt) {
  BrnrPerCsnFixture fixture(arbiter, qp, mtu, initialTag, /*count=*/3);
  constexpr uint32_t kCanceledAtNs = 1000;

  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[0], kCanceledAtNs);
  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[2], kCanceledAtNs);
  fixture.flow->processAckPacket(fixture.brnrForAllFrames(), kCanceledAtNs);

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_CANCELED)
      << "receives are posted through the later csn, so every reported frame "
         "is covered";
}

// Cancels are unsequenced, so the later one can arrive first.
TEST_F(QpTest, AnRnrCancelArrivingBehindALaterOneReportsNothing) {
  BrnrPerCsnFixture fixture(arbiter, qp, mtu, initialTag, /*count=*/3);
  constexpr uint32_t kCanceledAtNs = 1000;

  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[2], kCanceledAtNs);
  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[0], kCanceledAtNs);
  fixture.flow->processAckPacket(fixture.brnrForAllFrames(), kCanceledAtNs);

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_CANCELED)
      << "a stale cancel cannot lower the level, so every reported frame stays "
         "covered";
}

// A tag resumed at a csn can lose that receive to another tag drawing on the
// same shared queue, and be refused again at the csn it still waits on.
TEST_F(QpTest, AnRnrCancelRepeatingTheHeldCsnLiftsThePause) {
  constexpr uint16_t kBlockedMsn = 7;
  constexpr uint16_t kPostedCsn = 42;
  constexpr uint32_t kCanceledAtNs = 1000;
  BrnrBackoff& brnr = initialTag->brnr();

  brnr.blockAt(kBlockedMsn);
  brnr.cancelThrough(kPostedCsn, kCanceledAtNs);
  ASSERT_EQ(brnr.state(), BrnrState::BRNR_CANCELED)
      << "precondition: the first cancel lifts the pause, so the tag can be "
         "refused again";

  brnr.blockAt(kBlockedMsn);
  ASSERT_EQ(brnr.state(), BrnrState::BRNR_BLOCKED_TX)
      << "precondition: another tag took that receive, so this tag is paused "
         "again at the same csn";

  brnr.cancelThrough(kPostedCsn, kCanceledAtNs + 1);

  EXPECT_EQ(brnr.state(), BrnrState::BRNR_CANCELED)
      << "the repeat reports a second receive, so the tag resumes rather than "
         "waiting out its backoff";
  EXPECT_FALSE(brnr.firstBlockedMsn().has_value())
      << "the pause it lifts leaves no blocked msn behind it";
}

TEST_F(QpTest, AnRnrCancelBehindTheHeldLevelLeavesAPauseStanding) {
  BrnrPerCsnFixture fixture(arbiter, qp, mtu, initialTag, /*count=*/3);
  constexpr uint32_t kCanceledAtNs = 1000;

  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[1], kCanceledAtNs);
  fixture.flow->processAckPacket(fixture.brnrForFrames({2}), kCanceledAtNs + 1);
  ASSERT_EQ(initialTag->getBrnrFirstBlockedMsn(), fixture.msns[2])
      << "precondition: the frame past the posted csn pauses the tag";

  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[0], kCanceledAtNs + 2);

  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn(), fixture.msns[2])
      << "a stale cancel must not lift a pause on a frame the responder "
         "refused";
}

// Each refusal has its own packet, so a covered one can arrive after an
// uncovered one.
TEST_F(QpTest, ACoveredFrameArrivingAfterAPauseDoesNotDragItBack) {
  BrnrPerCsnFixture fixture(arbiter, qp, mtu, initialTag, /*count=*/3);
  constexpr uint32_t kCanceledAtNs = 1000;

  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[0], kCanceledAtNs);
  fixture.flow->processAckPacket(fixture.brnrForFrames({2}), kCanceledAtNs + 1);
  ASSERT_EQ(initialTag->getBrnrFirstBlockedMsn(), fixture.msns[2])
      << "precondition: the uncovered frame pauses the tag at its msn";

  fixture.flow->processAckPacket(fixture.brnrForFrames({0}), kCanceledAtNs + 2);

  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn(), fixture.msns[2])
      << "the covered frame must not move the pause back over a message the "
         "responder can take";
}

// `Tag::findNextWqeInMap` aborts on a state it does not expect over a blocked
// msn, so the two may not outlive each other.
TEST_F(QpTest, ACancelLeavesAPausedTagNoBlockedMsn) {
  BrnrPerCsnFixture fixture(arbiter, qp, mtu, initialTag, /*count=*/1);
  constexpr uint32_t kCanceledAtNs = 1000;

  fixture.flow->processAckPacket(fixture.brnrForAllFrames(), kCanceledAtNs);
  ASSERT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "precondition: the tag is paused";
  ASSERT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value())
      << "precondition: the pause recorded the msn it starts at";

  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[0], kCanceledAtNs);

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_CANCELED)
      << "the cancel clears the blocked msn the pause recorded";
  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value())
      << "a canceled tag sends, so the send path must find no blocked msn";
}

// A covered refusal is treated as in flight for one round trip after the
// cancel.
TEST_F(QpTest, ABrnrLongAfterAnRnrCancelBlocksTheTag) {
  BrnrPerCsnFixture fixture(arbiter, qp, mtu, initialTag, /*count=*/1);
  constexpr uint32_t kCanceledAtNs = 1000;

  qp.applyRnrCancel(kMetaRoceInitialTag, fixture.csns[0], kCanceledAtNs);
  const uint32_t longAfterNs = kCanceledAtNs + qp.getEstQpRttNs() + 1;
  fixture.flow->processAckPacket(fixture.brnrForAllFrames(), longAfterNs);

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "a BRNR arriving after that round trip pauses the tag";
}

// processBrnrPacket must set the tag's state and blocked MSN as a pair; the
// send side reads them together (Tag::findNextWqeInMap aborts on a mismatch).
TEST_F(QpTest, ProcessBrnrPacketSetsStateAndBlockedMsnTogether) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto* flow = qp.getTxFlowById(0);
  auto tag = qp.getTag(kMetaRoceInitialTag);

  ASSERT_EQ(tag->getBrnrState(), BrnrState::NORMAL_TX)
      << "precondition: a fresh tag is not in BRNR backoff";
  ASSERT_FALSE(tag->getBrnrFirstBlockedMsn().has_value())
      << "precondition: a fresh tag has no blocked MSN";

  // Well-formed BRNR NAK: maxRcvdFsn is past cumulativeFsn and the RNR bitset
  // covers that gap. (processBrnrPacket takes the blocked MSNs from the map.)
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(0);
  rnrBitVector.setBit(1);
  const AckHeader brnr = AckHeader::AckHeaderBuilder()
                             .setTag(kMetaRoceInitialTag)
                             .setCumulativeFsn(0)
                             .setMaxRcvdFsn(2)
                             .setAckStatus(AckStatus::BRNR)
                             .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                             .setRnrBitVector(rnrBitVector)
                             .setTimestamp(0)
                             .build();

  constexpr uint16_t kFirstMsn = 5;
  const std::unordered_map<uint8_t, uint16_t> blockFirst{
      {kMetaRoceInitialTag, kFirstMsn}};
  flow->processBrnrPacket(brnr, blockFirst);
  EXPECT_EQ(tag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "processBrnrPacket must move the tag into BRNR backoff, not leave it "
         "NORMAL_TX with a blocked MSN";
  ASSERT_TRUE(tag->getBrnrFirstBlockedMsn().has_value())
      << "a BRNR_BLOCKED_TX tag must carry the blocked MSN set alongside its "
         "state";
  EXPECT_EQ(tag->getBrnrFirstBlockedMsn().value(), kFirstMsn)
      << "the tag must pause at the BRNR'd MSN it was given";

  // A repeated BRNR with a lower MSN pulls the block earlier; the pair stays
  // consistent (still BRNR_BLOCKED_TX, MSN lowered).
  constexpr uint16_t kLowerMsn = 3;
  const std::unordered_map<uint8_t, uint16_t> blockLower{
      {kMetaRoceInitialTag, kLowerMsn}};
  flow->processBrnrPacket(brnr, blockLower);
  EXPECT_EQ(tag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "a repeated BRNR keeps the tag paused";
  EXPECT_EQ(tag->getBrnrFirstBlockedMsn().value(), kLowerMsn)
      << "a lower BRNR'd MSN must pull the block earlier";

  // A BRNR with a higher MSN must not relax the existing, earlier block.
  constexpr uint16_t kHigherMsn = 8;
  const std::unordered_map<uint8_t, uint16_t> blockHigher{
      {kMetaRoceInitialTag, kHigherMsn}};
  flow->processBrnrPacket(brnr, blockHigher);
  EXPECT_EQ(tag->getBrnrFirstBlockedMsn().value(), kLowerMsn)
      << "a higher BRNR'd MSN must not move the block later";
}

/*
 * A BRNR blocks a tag's msns from a first-blocked msn onward, but the frames
 * already on the wire for those msns still have to be retransmitted. Either
 * answer retires such a frame -- a BRNR advances snd_una over it, an ack
 * advances snd_una and clears the block -- while withholding it leaves the
 * frame unacked, so snd_una stops there for the whole backoff and every frame
 * behind it on the flow waits, including frames of unblocked tags.
 */
TEST_F(QpTest, BrnrBlockedMsnIsStillRetransmitted) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);

  // Post and send a three-packet message so its frames are outstanding.
  const uint16_t msgSizeToSendBytes = mtu * 3;
  auto sendPayload = std::make_unique<char[]>(msgSizeToSendBytes);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = sendPayload.get()});
  while (qp.getNextPacketToSend() != nullptr) {
    // send the initial transmission
  }

  // Block the tag from the msn just sent, as a BRNR NAK would, and stay in the
  // backoff rather than advancing to a probe.
  auto tag = qp.getTag(kMetaRoceInitialTag);
  const uint16_t blockedMsn = tag->getTxCompletedMsn() + 1;
  {
    auto tagGuard = tag->scopedLock();
    tag->brnr().blockAt(blockedMsn);
  }

  EXPECT_GT(txFlow->prepareRetxPacketsIfAny(true /*isRtoEvent*/), 0u)
      << "a frame whose msn is BRNR-blocked must still be retransmitted: it is "
         "the retransmission that draws the BRNR or ack that lets snd_una move "
         "past it";
  EXPECT_EQ(tag->getBrnrState(), BrnrState::BRNR_PROBE_SENT)
      << "a blocked msn put back on the wire is a probe whatever the backoff "
         "was doing, since it is what draws the answer that resolves the block";
}

/*
 * A BRNR that rejects one frame and reports later ones as received tells the
 * sender about holes between them. Its arrival runs the SACK scan against a tag
 * it has just blocked, which is the case that decides whether those holes wait
 * out the backoff. They are the frames whose answer would move snd_una, so they
 * go back on the wire on arrival rather than at the next RTO.
 */
TEST_F(QpTest, BrnrRetransmitsTheHolesItReportsOnArrival) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto txFlow = qp.getTxFlowById(0);

  // Four single-frame messages occupy fsn 0..3, one msn each.
  constexpr int kNumWqes = 4;
  auto payload = std::make_unique<char[]>(mtu);
  std::vector<uint16_t> msns;
  for (int i = 0; i < kNumWqes; ++i) {
    const auto* sendWqe = postWqeHelper(
        &qp,
        {.workType = Wqe::WorkType::kSend,
         .msgSizeBytes = mtu,
         .address = payload.get()});
    msns.push_back(sendWqe->getMsn().value());
  }
  for (int i = 0; i < kNumWqes; ++i) {
    ASSERT_TRUE(qp.getNextPacketToSend())
        << "frame " << i << " must reach the wire before the BRNR can report "
        << "on it";
  }

  // Reject fsn 0 and report fsn 3 as the highest received. Nothing is
  // cumulatively acked, so fsn 1 and 2 are holes the receiver has reported, and
  // every msn is blocked behind the one it rejected.
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(0);
  const AckHeader brnr = AckHeader::AckHeaderBuilder()
                             .setTag(kMetaRoceInitialTag)
                             .setCumulativeFsn(UINT16_MAX)
                             .setMaxRcvdFsn(3)
                             .setAckStatus(AckStatus::BRNR)
                             .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                             .setRnrBitVector(rnrBitVector)
                             .setTimestamp(0)
                             .build();
  txFlow->processAckPacket(brnr, simulatedProcessedAtNs(brnr.getTimestamp()));

  ASSERT_EQ(initialTag->getBrnrFirstBlockedMsn().value_or(0), msns.front())
      << "the BRNR rejected the oldest message, so the tag blocks from it "
         "onward and the holes below belong to blocked msns";

  // No timer has fired: anything available now was queued by the BRNR's own
  // scan.
  std::vector<uint16_t> retransmittedFsns;
  while (auto egressPacket = qp.getNextPacketToSend()) {
    const auto& header =
        dynamic_cast<DataPacket*>(egressPacket->packet.get())->getDataHeader();
    EXPECT_TRUE(header.getIsRetransmit() && !header.getIsTimeout())
        << "the receiver reported these frames missing, so they are resends "
           "prompted by its report rather than by a timeout";
    retransmittedFsns.push_back(header.getFsn());
  }

  EXPECT_EQ(retransmittedFsns, (std::vector<uint16_t>{1, 2}))
      << "withholding a blocked frame cannot produce the answer that frees it, "
         "so the holes the BRNR reported must be resent while it is in "
         "backoff, not held until the retransmit timer expires";
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_PROBE_SENT)
      << "those resends put blocked msns back on the wire, so a verdict is "
         "outstanding and the backoff must not release another probe";
}

// cancelRnrBackoff must clear the tag's state and blocked MSN as a pair, so it
// is never left NORMAL_TX with a lingering blocked MSN.
TEST_F(QpTest, CancelRnrBackoffClearsStateAndBlockedMsnTogether) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto tag = qp.getTag(kMetaRoceInitialTag);

  constexpr uint16_t kBlockedMsn = 5;
  tag->brnr().blockAt(kBlockedMsn);
  ASSERT_EQ(tag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "precondition: the tag is paused in BRNR backoff";
  ASSERT_TRUE(tag->getBrnrFirstBlockedMsn().has_value())
      << "precondition: the paused tag carries a blocked MSN";

  qp.cancelRnrBackoff(kMetaRoceInitialTag);

  EXPECT_EQ(tag->getBrnrState(), BrnrState::NORMAL_TX)
      << "cancelRnrBackoff must return the tag to NORMAL_TX";
  EXPECT_FALSE(tag->getBrnrFirstBlockedMsn().has_value())
      << "cancelRnrBackoff must clear the blocked MSN together with the state, "
         "so the tag is never NORMAL_TX with a lingering blocked MSN";
}

/*
 * An ACK whose Tag field holds a tag this QP does not have must still apply its
 * per-flow FSN acknowledgment: the Tag field only scopes the CDMSN. The known
 * tag's in-flight frame is cumulatively acked even though the ACK carries a tag
 * the QP never created.
 */
TEST_F(QpTest, ProcessAckWithUnknownTagStillAdvancesFsn) {
  constexpr uint8_t kUnknownTag = 1;
  ASSERT_FALSE(qp.containsTag(kUnknownTag))
      << "the ACK below must carry a tag the QP does not have";

  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  auto wqe0 =
      std::make_unique<SendWqe>(23, oneSge(receiveBuffer.data(), mtu), mtu);
  wqe0->setMsn(3);
  ASSERT_TRUE(allocatePosns(*wqe0, 1));

  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.sndNxt = 11;
  txState.maxAckdFsn = 9;
  txState.tracker.emplace_back(
      PacketId(wqe0.get(), kMetaRoceInitialTag, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 10, known tag 0

  // The ACK cumulatively acks FSN 10 but carries the unknown tag.
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setTag(kUnknownTag)
                                  .setCumulativeFsn(10)
                                  .setMaxRcvdFsn(10)
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(0)
                                  .build();

  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  EXPECT_EQ(txState.sndUna, 11)
      << "the known tag's frame must be cumulatively acked even though the "
         "ACK carries a tag the QP does not have";
  EXPECT_TRUE(txState.tracker.empty())
      << "the cumulatively acked frame must be removed from the tracker";
}

/*
 * BRNR backoff lives in the outbound MSN space: a tag exits backoff only when a
 * newly-acked outbound frame reaches its first blocked MSN. The tag's inbound
 * CDMSN is a separate sequence space -- driven high (past 2^15) here -- and
 * must not influence the decision either way.
 */
TEST_F(QpTest, ProcessAckExitsBrnrBackoffOnlyWhenBlockedOutboundMsnAcked) {
  constexpr uint16_t kBlockedMsn = 100; // outbound MSN space
  constexpr uint16_t kBelowBlockMsn = 50;
  constexpr uint16_t kAboveBlockMsn = 150; // past the block, exercises the '>'
  constexpr uint16_t kInboundCdmsn = 49152; // > 2^15, unrelated inbound space

  auto tag = qp.getTag(kMetaRoceInitialTag);
  ASSERT_TRUE(tag->setReceiveSequenceNumbers(
      {.nextMsn = kInboundCdmsn + 1, .nextCsn = kFixtureRxCsn}))
      << "precondition: the case turns on an inbound cdmsn past 2^15; with the "
         "fixture's low one the wrap it exercises never arises";
  tag->brnr().blockAt(kBlockedMsn);

  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  auto wqeBelow =
      std::make_unique<SendWqe>(23, oneSge(receiveBuffer.data(), mtu), mtu);
  wqeBelow->setMsn(kBelowBlockMsn);
  ASSERT_TRUE(allocatePosns(*wqeBelow, 1));
  auto wqeAboveBlock =
      std::make_unique<SendWqe>(24, oneSge(receiveBuffer.data(), mtu), mtu);
  wqeAboveBlock->setMsn(kAboveBlockMsn);
  ASSERT_TRUE(allocatePosns(*wqeAboveBlock, 1));

  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.sndNxt = 12;
  txState.maxAckdFsn = 9;
  txState.tracker.emplace_back(
      PacketId(wqeBelow.get(), kMetaRoceInitialTag, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 10, outbound MSN below the block
  txState.tracker.emplace_back(
      PacketId(wqeAboveBlock.get(), kMetaRoceInitialTag, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 11, outbound MSN past the block

  // Acking only the below-block frame leaves the block unresolved.
  const AckHeader belowAck = AckHeader::AckHeaderBuilder()
                                 .setTag(kMetaRoceInitialTag)
                                 .setCumulativeFsn(10)
                                 .setMaxRcvdFsn(10)
                                 .setAckStatus(AckStatus::ACK)
                                 .setTimestamp(0)
                                 .build();
  flow->processAckPacket(
      belowAck, simulatedProcessedAtNs(belowAck.getTimestamp()));

  EXPECT_EQ(tag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "acking an outbound frame below the block must not exit BRNR backoff, "
         "no matter how high the inbound CDMSN is";

  // Acking a frame past the block resolves it and exits backoff.
  const AckHeader aboveAck = AckHeader::AckHeaderBuilder()
                                 .setTag(kMetaRoceInitialTag)
                                 .setCumulativeFsn(11)
                                 .setMaxRcvdFsn(11)
                                 .setAckStatus(AckStatus::ACK)
                                 .setTimestamp(0)
                                 .build();
  flow->processAckPacket(
      aboveAck, simulatedProcessedAtNs(aboveAck.getTimestamp()));

  EXPECT_EQ(tag->getBrnrState(), BrnrState::NORMAL_TX)
      << "acking an outbound frame past the block must exit BRNR backoff";
  EXPECT_FALSE(tag->getBrnrFirstBlockedMsn().has_value())
      << "exiting backoff must clear the first-blocked MSN";
}

/*
 * HRNR (whole-QP) and BRNR (per-tag) backoff exits are independent. An ack that
 * clears the QP's HRNR block must not also clear a tag's BRNR block whose own
 * outbound frames have not been acked up to its first blocked MSN.
 */
TEST_F(QpTest, ProcessAckClearsHrnrWithoutClearingUnrelatedBrnrBlock) {
  constexpr uint16_t kBlockedMsn = 100;
  constexpr uint16_t kBelowBlockMsn = 50;

  auto tag = qp.getTag(kMetaRoceInitialTag);
  tag->brnr().blockAt(kBlockedMsn);

  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  qp.setHrnrState(HrnrState::HRNR_BLOCKED_TX);

  auto wqe0 =
      std::make_unique<SendWqe>(23, oneSge(receiveBuffer.data(), mtu), mtu);
  wqe0->setMsn(kBelowBlockMsn);
  ASSERT_TRUE(allocatePosns(*wqe0, 1));

  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.sndNxt = 11;
  txState.maxAckdFsn = 9;
  txState.tracker.emplace_back(
      PacketId(wqe0.get(), kMetaRoceInitialTag, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 10, outbound MSN below the block

  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setTag(kMetaRoceInitialTag)
                                  .setCumulativeFsn(10)
                                  .setMaxRcvdFsn(10)
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(0)
                                  .build();

  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  EXPECT_EQ(qp.getHrnrState(), HrnrState::NORMAL_TX)
      << "any ack must clear the whole-QP HRNR block";
  EXPECT_EQ(tag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "clearing HRNR must not clear a tag's BRNR block whose own frames are "
         "not yet acked up to the block";
  EXPECT_EQ(tag->getBrnrFirstBlockedMsn().value_or(0), kBlockedMsn)
      << "the tag's first-blocked MSN must survive the HRNR clear";
}

/*
 * Tests the BRNR sender functionality for updating rnrFirstBlockedMsn_ when
 * multiple RNR'd packets with different MSNs are received.
 *
 * This test verifies that:
 * 1. The rnrFirstBlockedMsn_ field is updated to the lowest MSN of RNR'd
 * packets
 * 2. The sender correctly handles multiple RNR'd packets in the same ACK
 */
TEST_F(QpTest, ProcessAckPacketWithMultipleRnrMsns) {
  // Create and add flow
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Get the flow
  auto flow = qp.getTxFlowById(0);

  const int frames = 10;
  const uint32_t length = frames * mtu;
  auto payload = std::make_unique<char[]>(length);

  // Create WQEs with different MSNs
  auto wqe1 = std::make_unique<SendWqe>(
      23 /*workReqId*/,
      oneSge(
          payload.get(), /*address*/
          length),
      mtu);
  wqe1->setMsn(30);
  ASSERT_TRUE(allocatePosns(*wqe1, 10));

  auto wqe2 = std::make_unique<SendWqe>(
      24 /*workReqId*/,
      oneSge(
          payload.get(), /*address*/
          length),
      mtu);
  wqe2->setMsn(20);
  ASSERT_TRUE(allocatePosns(*wqe2, 10));

  auto wqe3 = std::make_unique<SendWqe>(
      25 /*workReqId*/,
      oneSge(
          payload.get(), /*address*/
          length),
      mtu);
  wqe3->setMsn(10);
  ASSERT_TRUE(allocatePosns(*wqe3, 10));

  // Set up the txState with specific values
  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.maxAckdFsn = 9;

  // Add packets to the tracker with different MSNs
  txState.tracker.emplace_back(
      PacketId(wqe1.get(), 0, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 10
  txState.tracker.emplace_back(
      PacketId(wqe1.get(), 0, 1, mtu),
      Flow::FsnState::UNACKED); // FSN 11
  txState.tracker.emplace_back(
      PacketId(wqe2.get(), 0, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 12
  txState.tracker.emplace_back(
      PacketId(wqe3.get(), 0, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 13
  txState.tracker.emplace_back(
      PacketId(wqe3.get(), 0, 1, mtu),
      Flow::FsnState::UNACKED); // FSN 14
  txState.sndNxt = 15;

  // Create an ACK packet with RNR bit vector set for multiple packets
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(1); // FSN 11 is RNR'd (MSN 30)
  rnrBitVector.setBit(2); // FSN 12 is RNR'd (MSN 20)

  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(9)
                                  .setMaxRcvdFsn(12)
                                  .setAckStatus(AckStatus::BRNR)
                                  .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                                  .setRnrBitVector(rnrBitVector)
                                  .setTimestamp(0)
                                  .build();

  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value())
      << "rnrFirstBlockedMsn_ should not be set before processing the ACK";

  // Process the ACK packet
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  // Verify that rnrFirstBlockedMsn_ is set to the lower MSN (20)
  EXPECT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), 20);

  // Create another ACK packet with a lower MSN RNR'd
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector2;
  rnrBitVector2.setBit(3); // FSN 13 is RNR'd (MSN 10)

  const AckHeader ackHeader2 = AckHeader::AckHeaderBuilder()
                                   .setCumulativeFsn(9)
                                   .setMaxRcvdFsn(13)
                                   .setAckStatus(AckStatus::BRNR)
                                   .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                                   .setRnrBitVector(rnrBitVector2)
                                   .setTimestamp(0)
                                   .build();

  // Process the second ACK packet
  flow->processAckPacket(
      ackHeader2, simulatedProcessedAtNs(ackHeader2.getTimestamp()));

  // Verify that rnrFirstBlockedMsn_ is now 10
  EXPECT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), 10);
}

/*
 * Tests the BRNR sender functionality for contiguous ACK/RNR sequences.
 *
 * This test verifies that:
 * 1. The sender correctly advances sndUna for contiguous ACK/RNR sequences
 * 2. Positions for RNR'd FSNs are properly deallocated
 * 3. The tracker is updated correctly
 */
TEST_F(QpTest, ProcessAckPacketWithContiguousAckRnrSequence) {
  // Create and add flow
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Get the flow
  auto flow = qp.getTxFlowById(0);

  const int frames = 20;
  const uint32_t length = frames * mtu;
  auto payload = std::make_unique<char[]>(length);
  // Create a WQE
  auto wqe = std::make_unique<SendWqe>(
      23 /*workReqId*/,
      oneSge(
          payload.get(), /*address*/
          length),
      mtu);
  wqe->setMsn(0);
  ASSERT_TRUE(allocatePosns(*wqe, 20));

  // Set up the txState with specific values
  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 15;
  txState.maxAckdFsn = 9;

  // Add packets to the tracker
  // FSNs 10-15
  txState.tracker.emplace_back(
      PacketId(wqe.get(), 0, 0, mtu),
      Flow::FsnState::UNACKED); // FSN 10
  txState.tracker.emplace_back(
      PacketId(wqe.get(), 0, 1, mtu),
      Flow::FsnState::UNACKED); // FSN 11
  txState.tracker.emplace_back(
      PacketId(wqe.get(), 0, 2, mtu),
      Flow::FsnState::UNACKED); // FSN 12
  txState.tracker.emplace_back(
      PacketId(wqe.get(), 0, 3, mtu),
      Flow::FsnState::UNACKED); // FSN 13
  txState.tracker.emplace_back(
      PacketId(wqe.get(), 0, 4, mtu),
      Flow::FsnState::UNACKED); // FSN 14
  txState.tracker.emplace_back(
      PacketId(wqe.get(), 0, 5, mtu),
      Flow::FsnState::UNACKED); // FSN 15
  txState.sndNxt = 16;

  // Create an ACK packet with a contiguous sequence of ACKs and RNRs
  // FSN 10 is cumulatively ACKed
  // FSN 11 is RNR'd
  // FSN 12 is ACKed
  // FSN 13 is RNR'd
  // FSN 14 is ACKed
  // FSN 15 is not ACKed or RNR'd (gap)
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  fsnBitVector.setBit(1); // FSN 12 is ACKed
  fsnBitVector.setBit(3); // FSN 14 is ACKed

  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(0); // FSN 11 is RNR'd
  rnrBitVector.setBit(2); // FSN 13 is RNR'd

  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(10)
                                  .setMaxRcvdFsn(14)
                                  .setAckStatus(AckStatus::BRNR)
                                  .setRnrBackoffTimeExp(kBrnrBackoffTimeExp)
                                  .setFsnBitVector(fsnBitVector)
                                  .setRnrBitVector(rnrBitVector)
                                  .setTimestamp(0)
                                  .build();

  // Process the ACK packet
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  // Verify that sndUna is advanced correctly
  // sndUna should be 15 (advanced past FSN 14 which is the last contiguous
  // ACK/RNR)
  EXPECT_EQ(txState.sndUna, 15);

  // Verify that the tracker size is reduced
  // FSNs 10-14 should be removed from the tracker
  EXPECT_EQ(txState.tracker.size(), 1);

  // Verify the remaining tracker entry
  EXPECT_EQ(txState.tracker.at(0).id.posn, 5);
  EXPECT_TRUE(txState.tracker.at(0).isUnacked());

  EXPECT_EQ(wqe->getNumPosn(), 20u)
      << "the message is the length the counts below are read against, so a "
         "wrong length here would blame the ack path for a sizing fault";
  EXPECT_EQ(wqe->getOutstandingPosnCount(), 17u)
      << "the three sacked posns are accounted for, and the two refused ones "
         "are still outstanding";
  EXPECT_EQ(wqe->allocateNextPosn(), 1u)
      << "posn 1 was refused, so it is the first thing to go out again";
  EXPECT_EQ(wqe->allocateNextPosn(), 3u)
      << "posn 3 follows it, the two refusals being all that came back";
  EXPECT_EQ(wqe->allocateNextPosn(), 20u)
      << "every posn of the message is allocated and none was refused, so "
         "the message has no posn left to offer";
}

TEST_F(QpTest, AnAlternatingBrnrAndSackReturnsOnlyTheRefusedPosns) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  constexpr uint32_t kFrames = 10;
  const uint32_t length = kFrames * mtu;
  auto payload = std::make_unique<char[]>(length);
  auto wqe = std::make_unique<SendWqe>(
      23 /*workReqId*/, oneSge(payload.get(), length), mtu);
  wqe->setMsn(0);

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.maxAckdFsn = UINT16_MAX;
  for (uint32_t posn = 0; posn < kFrames; ++posn) {
    ASSERT_EQ(wqe->allocateNextPosn(), posn)
        << "precondition: every posn of the message is on the wire before the "
           "peer answers, so the ack below can speak about all of them";
    txState.tracker.emplace_back(
        PacketId(wqe.get(), 0, posn, mtu), Flow::FsnState::UNACKED);
  }
  txState.sndNxt = kFrames;

  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  for (uint32_t posn = 0; posn < kFrames; ++posn) {
    if (posn % 2 == 0) {
      fsnBitVector.setBit(posn);
    } else {
      rnrBitVector.setBit(posn);
    }
  }
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(UINT16_MAX)
                                  .setMaxRcvdFsn(kFrames - 1)
                                  .setAckStatus(AckStatus::BRNR)
                                  .setFsnBitVector(fsnBitVector)
                                  .setRnrBitVector(rnrBitVector)
                                  .setRnrBackoffTimeExp(0)
                                  .setTimestamp(0)
                                  .build();

  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  EXPECT_EQ(wqe->getOutstandingPosnCount(), kFrames / 2)
      << "the five taken posns are accounted for, and only the five refused "
         "ones are still outstanding";

  for (const uint32_t expected : {1u, 3u, 5u, 7u, 9u}) {
    EXPECT_EQ(wqe->allocateNextPosn(), expected)
        << "the refused posns go back out lowest first, and only those: an "
           "acked posn between them is finished with";
  }
  EXPECT_EQ(wqe->allocateNextPosn(), kFrames)
      << "the message has nothing further to send once the refused posns are "
         "back out";
}
/*
 * Tests the BRNR (Buffer Receiver Not Ready) sender functionality.
 *
 * This test verifies the behavior when a QP receives a BRNR ACK packet,
 * which indicates that specific MSNs are blocked at the receiver. The test
 * checks:
 *
 * 1. When a BRNR ACK is received, the QP sets rnrFirstBlockedMsn_
 * 2. Packets from blocked MSNs are not transmitted in BRNR_BLOCKED_TX state
 * 3. After BRNR timer expires, QP transitions to BRNR_PROBING state
 * 4. In BRNR_PROBING state, only one packet is sent as a probe
 * 5. Normal ACKs cancel the BRNR backoff and resume normal transmission
 */
TEST_F(QpTest, BrnrSenderBasicFunctionality) {
  ScopedSuppressedTxTimers suppressTimers;
  // STEP 1: Setup QP with congestion controller and flow
  // --------------------------------------
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getTxFlowById(0);

  // Create two WQEs with different MSNs
  // Post first WQE (this will be sent without BRNR)
  uint32_t msgSizeToSendBytes = mtu * 3;
  auto payload = std::make_unique<char[]>(msgSizeToSendBytes);
  const auto normalWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeToSendBytes,
       .address = payload.get()});

  // Post second WQE (this will be blocked by BRNR)
  const auto blockedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu * 2),
       .address = payload.get()});

  const uint16_t normalMsn = normalWqe->getMsn().value();
  const uint16_t blockedMsn = blockedWqe->getMsn().value();

  // STEP 2: Send initial packets
  // --------------------------------------
  // Send first packet from normal MSN
  auto egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  auto packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), normalMsn);
  EXPECT_EQ(packet->getDataHeader().getFsn(), 0);

  // Send second packet from normal MSN
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), normalMsn);
  EXPECT_EQ(packet->getDataHeader().getFsn(), 1);

  // Send third packet from normal MSN
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), normalMsn);
  EXPECT_EQ(packet->getDataHeader().getFsn(), 2);

  // Send first packet from blocked MSN
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), blockedMsn);
  EXPECT_EQ(packet->getDataHeader().getFsn(), 3);

  // STEP 3: Simulate receiving BRNR ACK for the blocked MSN
  // and no ack for the last packet of the normal MSN
  // --------------------------------------
  const uint32_t brnrBackoffTimeExp = 12; // 5 * 2^12 = 20480 microseconds

  // Create BRNR ACK packet
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(1); // FSN 3 is BRNR'd

  AckHeader brnrHeader = AckHeader::AckHeaderBuilder()
                             .setCumulativeFsn(1) // FSN 0 is not ACKed
                             .setMaxRcvdFsn(3)
                             .setAckStatus(AckStatus::BRNR)
                             .setRnrBackoffTimeExp(brnrBackoffTimeExp)
                             .setRnrBitVector(rnrBitVector)
                             .setTimestamp(0)
                             .build();
  // This acc implies:
  // 1. FSN 0 is ACKed
  // 2. FSN 1 is ACKed
  // 3. FSN 2 is not ACKed
  // 4. FSN 3 is RNR'd

  // Process the BRNR packet
  flow->processAckPacket(
      brnrHeader, simulatedProcessedAtNs(brnrHeader.getTimestamp()));

  // Verify that rnrFirstBlockedMsn_ is set
  EXPECT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), blockedMsn);

  // Verify QP is in BRNR_BLOCKED_TX state
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);

  // Step 4: Verify normal MSN is still retransmitted
  // --------------------------------------
  // The unacked fsn 2 of the normal MSN is a SACK hole queued for retransmit
  // during processAckPacket, so it is available immediately.
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), normalMsn);
  EXPECT_EQ(packet->getDataHeader().getFsn(), 2);

  EXPECT_TRUE(packet->getDataHeader().getIsRetransmit())
      << "The third packet of msn " << normalMsn
      << " should be retransmitted despite msn " << blockedMsn
      << " being blocked.";

  // Step 5: ACK retransmitted packet from normal MSN
  // --------------------------------------
  rnrBitVector.unsetBit(1);
  rnrBitVector.setBit(0); // FSN 3 is BRNR'd and CFSN is 2
  brnrHeader = AckHeader::AckHeaderBuilder()
                   .setCumulativeFsn(2)
                   .setMaxRcvdFsn(3)
                   .setAckStatus(AckStatus::BRNR)
                   .setRnrBackoffTimeExp(brnrBackoffTimeExp)
                   .setRnrBitVector(rnrBitVector)
                   .setTimestamp(0)
                   .build();
  // This acc implies:
  // 1. FSN 0 is ACKed
  // 2. FSN 1 is ACKed
  // 3. FSN 2 is ACKED
  // 4. FSN 3 is RNR'd

  // Process the BRNR packet
  flow->processAckPacket(
      brnrHeader, simulatedProcessedAtNs(brnrHeader.getTimestamp()));

  // STEP 6: Verify blocked MSN packets are not sent
  // --------------------------------------
  // Try to get next packet - but it should not be available
  egressPacket = qp.getNextPacketToSend();
  EXPECT_FALSE(egressPacket);

  // STEP 7: Drive the BRNR backoff expiry and transition to PROBING
  // --------------------------------------
  // Cancel the real BRNR timer so it cannot fire on the timer thread, and
  // begin the probe from here.
  retxTimer.cancelCallback(qp.getBrnrTimerCallback());
  initialTag->brnr().beginProbe();
  auto probePacket = qp.getNextPacketToSend();
  ASSERT_TRUE(probePacket) << "Expected probe packet after BRNR timer expiry";

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_PROBE_SENT)
      << "The BRNR timer has expired and we have a packet that should be"
      << " a BRNR probe.  The state should be BRNR_PROBE_SENT";

  // Verify the probe packet is from the blocked MSN
  packet = dynamic_cast<DataPacket*>(probePacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), blockedMsn);
  // FSN should be 4 since FSN 3 was RNR'd
  EXPECT_EQ(packet->getDataHeader().getFsn(), 4);
  EXPECT_EQ(packet->getDataHeader().getPosn(), 0);

  EXPECT_FALSE(packet->getDataHeader().getIsRetransmit())
      << "First BRNR probe should not be marked as a retransmission.";

  // STEP 8: Drive the RTO retransmission and verify only one probe is sent
  // --------------------------------------
  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);
  probePacket = qp.getNextPacketToSend();
  ASSERT_TRUE(probePacket) << "probe packet should be sent after RTO expiry";
  packet = dynamic_cast<DataPacket*>(probePacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), blockedMsn);
  EXPECT_EQ(packet->getDataHeader().getFsn(), 4)
      << "FSN should be 4 since FSN 3 was RNR'd";
  EXPECT_EQ(packet->getDataHeader().getPosn(), 0);

  EXPECT_TRUE(packet->getDataHeader().getIsRetransmit())
      << "Expecting a retransmission of a BRNR probe.";
  EXPECT_TRUE(packet->getDataHeader().getIsTimeout())
      << "Expecting a retransmission by timeout of a BRNR probe.";

  // STEP 8: Simulate receiving normal ACK to cancel BRNR
  // --------------------------------------
  const AckHeader normalAck = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(4)
                                  .setMaxRcvdFsn(4)
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(0)
                                  .build();

  // Process the normal ACK
  flow->processAckPacket(
      normalAck, simulatedProcessedAtNs(normalAck.getTimestamp()));

  // Verify QP returns to NORMAL_TX state
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);

  // Verify rnrFirstBlockedMsn_ is cleared
  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value());

  // STEP 9: Verify normal transmission resumes
  // --------------------------------------
  // After the normal ACK clears BRNR, the next new packet is available
  // immediately.
  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), blockedMsn);
  EXPECT_EQ(packet->getDataHeader().getFsn(), 5);
  EXPECT_EQ(packet->getDataHeader().getPosn(), 1);
}

/*
 * Tests BRNR functionality with multiple MSNs.
 *
 * This test verifies:
 * 1. Multiple MSNs can be blocked simultaneously
 * 2. rnrFirstBlockedMsn_ tracks the lowest blocked MSN
 * 3. Packets from all blocked MSNs are properly handled
 */
TEST_F(QpTest, BrnrMultipleMsns) {
  ScopedSuppressedTxTimers suppressTimers;
  // Setup QP
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getTxFlowById(0);

  uint32_t msgSizeBytes = mtu * 2;
  auto payload = std::make_unique<char[]>(msgSizeBytes);

  // Create multiple WQEs with different MSNs
  // Will be RNR'd second (delayed)
  const auto delayedWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get()});
  const auto msn1 = delayedWqe->getMsn().value();

  // Will be RNR'd first
  const auto rnrdWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get()});
  const auto msn2 = rnrdWqe->getMsn().value();

  // Will be blocked
  // unused const auto blockedWqe =
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get()});
  // unused: const auto msn3 = blockedWqe->getMsn().value();

  uint32_t timestampOfFsn2 = 0;
  // Send some packets
  for (int i = 0; i < 4; i++) {
    auto egressPacket = qp.getNextPacketToSend();
    ASSERT_TRUE(egressPacket);
    // Confirm that the packets are scheduled as expected,
    // else the rest of the test will be confused.
    auto packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    EXPECT_EQ(packet->getBaseHeader().getFlowId(), 0);
    EXPECT_EQ(packet->getDataHeader().getFsn(), i);
    if (packet->getDataHeader().getFsn() == 2) {
      EXPECT_EQ(packet->getDataHeader().getMsn(), 2);
      EXPECT_EQ(packet->getDataHeader().getPosn(), 0);
    }
    EXPECT_FALSE(packet->getDataHeader().getIsRetransmit())
        << "Initial packets in brnr test should not be retransmissions";
    EXPECT_FALSE(packet->getDataHeader().getIsTimeout())
        << "Initial packets in brnr test should not be retransmissions";
    if (i == 2) {
      timestampOfFsn2 = packet->getDataHeader().getTimestamp();
    }
  }

  // In this scenario, msn2 posn 0 is received first
  // and it's RNR'd, then msn1 posn 1 is received and it's RNR'd
  // and the other packets are lost

  // Simulate BRNR for msn2 (higher MSN blocked first)
  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(2); // FSN 2 corresponds to first packet of msn2

  const uint32_t brnrBackoffTimeExp = 8; // 5 * 2^8 = 1280 microseconds

  AckHeader brnrHeader = AckHeader::AckHeaderBuilder()
                             .setCumulativeFsn(0xFFFF)
                             .setMaxRcvdFsn(2)
                             .setAckStatus(AckStatus::BRNR)
                             .setRnrBackoffTimeExp(brnrBackoffTimeExp)
                             .setRnrBitVector(rnrBitVector)
                             .setTimestamp(timestampOfFsn2)
                             .build();

  flow->processAckPacket(
      brnrHeader, simulatedProcessedAtNs(brnrHeader.getTimestamp()));

  // Verify rnrFirstBlockedMsn_ is set to msn2
  EXPECT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), msn2);

  // Packets from msn1 should be immediately available
  // because they were skipped in the ACK
  auto egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket) << "expecting retransmission of fsn0 due to sack";
  auto packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), msn1);
  EXPECT_EQ(packet->getDataHeader().getFsn(), 0);
  EXPECT_EQ(packet->getDataHeader().getPosn(), 0);

  // msn1 was a retransmission due to missing in the sack
  EXPECT_TRUE(packet->getDataHeader().getIsRetransmit())
      << "Retransmit of the not-blocked packet should be flagged";
  EXPECT_FALSE(packet->getDataHeader().getIsTimeout())
      << "Should have been a sack retransmission";

  egressPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket) << "expecting retransmission of fsn1 due to sack";
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), msn1);
  EXPECT_EQ(packet->getDataHeader().getFsn(), 1);
  EXPECT_EQ(packet->getDataHeader().getPosn(), 1);
  auto packetTimestamp = packet->getDataHeader().getTimestamp();

  // Simulate BRNR for msn1 (lower MSN blocked later)
  rnrBitVector = BitVector<AckHeader::kAckTrackerBits>{};
  rnrBitVector.setBit(1); // FSN 1 corresponds to second packet of msn1
  rnrBitVector.setBit(2); // FSN 2 corresponds to first packet of msn2

  brnrHeader = AckHeader::AckHeaderBuilder()
                   .setCumulativeFsn(0xFFFF)
                   .setMaxRcvdFsn(2)
                   .setAckStatus(AckStatus::BRNR)
                   .setRnrBackoffTimeExp(8)
                   .setRnrBitVector(rnrBitVector)
                   .setTimestamp(packetTimestamp)
                   .build();

  flow->processAckPacket(
      brnrHeader, simulatedProcessedAtNs(brnrHeader.getTimestamp()));

  // Verify rnrFirstBlockedMsn_ is updated to msn1 (lower MSN)
  EXPECT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), msn1);

  // Verify QP is in BRNR_BLOCKED_TX state
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);

  // Try to get packets but all should be blocked
  egressPacket = qp.getNextPacketToSend();
  EXPECT_FALSE(egressPacket);

  // Cancel the real BRNR timer and begin the probe from here, so the packet
  // arrives on the test thread.
  retxTimer.cancelCallback(qp.getBrnrTimerCallback());
  initialTag->brnr().beginProbe();
  auto probePacket = qp.getNextPacketToSend();
  ASSERT_TRUE(probePacket) << "expected a probe packet; BRNR state is "
                           << initialTag->getBrnrState()
                           << " and first blocked msn "
                           << initialTag->getBrnrFirstBlockedMsn().value_or(-1);
  packet = dynamic_cast<DataPacket*>(probePacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getMsn(), msn1)
      << "brnr probe packet should start from lowest msn, while qp BRNR state was "
      << initialTag->getBrnrState() << " and first blocked msn "
      << initialTag->getBrnrFirstBlockedMsn().value_or(-1);
  auto probeFsn = packet->getDataHeader().getFsn();

  // No other packet should be available until a timeout expires (a
  // retransmission by timeout would be acceptable) or an ack (which this test
  // doesn't provide) is received.
  egressPacket = qp.getNextPacketToSend();
  if (egressPacket) {
    auto dataPacket = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    if (dataPacket->getDataHeader().getIsRetransmit() &&
        dataPacket->getDataHeader().getIsTimeout()) {
      LOG(INFO) << "Timeout based retransmissions are allowed for stress-test";
      return;
    }
    EXPECT_FALSE(egressPacket)
        << "Saw flow: " << dataPacket->getBaseHeader().getFlowId()
        << " fsn: " << dataPacket->getDataHeader().getFsn()
        << " msn: " << dataPacket->getDataHeader().getMsn()
        << " posn: " << dataPacket->getDataHeader().getPosn()
        << " while in BRNR state " << initialTag->getBrnrState();
    EXPECT_EQ(dataPacket->getDataHeader().getMsn(), msn1)
        << "Though we did not expect a second brnr probe packet so soon, this packet probes for an unexpected msn";
    EXPECT_EQ(dataPacket->getBaseHeader().getFlowId(), 0)
        << "Though we did not expect a second brnr probe packet so soon, this packet is on an unexpected flow id";
    EXPECT_EQ(dataPacket->getDataHeader().getFsn(), probeFsn + 1)
        << "Though we did not expect a second brnr probe packet so soon, this packet is not the next fsn in sequence";

    // Do not want to flag BRNR-probes as if they are
    // network loss retransmissions
    EXPECT_FALSE(dataPacket->getDataHeader().getIsRetransmit())
        << "BRNR probes should not appear as retransmissions";
    EXPECT_FALSE(dataPacket->getDataHeader().getIsTimeout())
        << "BRNR probes should not appear as timeout retransmissions";
  }
}

/*
 * Tests BRNR cancel functionality.
 *
 * This test verifies that cancelRnrBackoff() properly handles BRNR state:
 * 1. Cancels BRNR timer callback
 * 2. Resets BRNR probe scheduling flags
 * 3. Clears rnrFirstBlockedMsn_
 * 4. Transitions to NORMAL_TX state
 */
TEST_F(QpTest, BrnrCancelBackoff) {
  // Setup QP
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Set up BRNR state
  const uint16_t blockedMsn = initialTag->getNextMsn();
  initialTag->brnr().blockAt(blockedMsn);

  // Verify initial state
  EXPECT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);

  // Cancel RNR backoff
  qp.cancelRnrBackoff(kMetaRoceInitialTag);

  // Verify state is reset
  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);
}

// A re-solicitation re-sends an already-received FSN with no payload; it must
// be re-acked, not re-validated (validation runs only on a new FSN).
TEST_F(QpTest, ResolicitedFsnReacked) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(0, 1, &mockSender);
  auto* receiversFlow = qp.getRxFlowById(0);

  // Post a matching receive WQE so the first SEND is delivered, not BRNR'd.
  std::unique_ptr<char[]> rcvPayload(new char[mtu]);
  postInSequenceReceiveWqe(qp, rcvPayload.get(), mtu);

  // A separate QP generates a well-formed SEND (FSN 0, one full MTU payload).
  Qp sendingQp{kSecondQpNum, retxTimer, arbiter, mrs, &cq, ibvQp};
  sendingQp.setMtu(mtu);
  createAlwaysSendingMockCongestionController(sendingQp);
  sendingQp.addPortPair(
      *qp.getRemoteAddrs().begin(), *qp.getLocalAddrs().begin());
  LimitedTestSender sendingMockSender(arbiter, mockUDPSocketMaker);
  sendingQp.setupFlows(1, 0, &sendingMockSender);

  auto sendPayload = std::make_unique<char[]>(mtu);
  postWqeHelper(
      &sendingQp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .address = sendPayload.get()});
  auto egress = sendingQp.getNextPacketToSend();
  ASSERT_TRUE(egress);
  auto* firstPacket = dynamic_cast<DataPacket*>(egress->packet.get());
  ASSERT_TRUE(firstPacket);
  ASSERT_EQ(firstPacket->getDataHeader().getFsn(), 0);

  // Deliver FSN 0: validated, delivered, acked; rcvNxt advances past it.
  auto firstAck = receiversFlow->processDataPacket(firstPacket, kEcnECT0);
  ASSERT_TRUE(firstAck.has_value());
  EXPECT_EQ(firstAck->getAckStatus(), AckStatus::ACK)
      << "the RxFlow delivers and acks the initial valid SEND";
  EXPECT_EQ(receiversFlow->getRxState().rcvNxt, 1);

  // Re-send FSN 0 with no payload (the ack re-solicitation form). It is already
  // received, so it must be re-acked without re-validation; an empty SEND would
  // otherwise fail validation and move the QP to error.
  DataPacket resolicit(
      firstPacket->getBaseHeader(), firstPacket->getDataHeader(), nullptr, 0);
  auto resolicitAck = receiversFlow->processDataPacket(&resolicit, kEcnECT0);
  ASSERT_TRUE(resolicitAck.has_value())
      << "the RxFlow re-acks a received FSN when it is re-solicited";
  EXPECT_EQ(resolicitAck->getAckStatus(), AckStatus::ACK)
      << "the RxFlow re-acks the re-solicitation rather than NAKing its empty payload";
  EXPECT_NE(ibvQpStorage.state, IBV_QPS_ERR)
      << "the RxFlow does not re-validate an already-received FSN into a QP error";
}

/*
 * Tests clearing of rxState when having a BRNR.
 *
 * This test verifies that when a packet is BRNR'd
 * and then received again with a new FSN, the
 * rxState at the receiver is updated correctly.
 * 1. Set up a sending QP and posts a send WQE
 * 2. Send a packet
 * 3. BRNR packet
 * 4. Post receive WQE
 * 5. Send packet again with new FSN
 * 6. Receive new packet and update rxState
 */
TEST_F(QpTest, BrnrFirstPacket) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(0, 1, &mockSender);

  auto receiversFlow = qp.getRxFlowById(0);
  auto& rxState = receiversFlow->getRxState();
  EXPECT_EQ(rxState.rcvNxt, 0);
  EXPECT_EQ(rxState.maxRcvdFsn, UINT16_MAX);

  // STEP 1: Setup QP with congestion controller and flow
  // --------------------------------------

  Qp sendingQp{kSecondQpNum, retxTimer, arbiter, mrs, &cq, ibvQp};
  sendingQp.setMtu(mtu);
  createAlwaysSendingMockCongestionController(sendingQp);
  sendingQp.addPortPair(
      *qp.getRemoteAddrs().begin(), *qp.getLocalAddrs().begin());

  LimitedTestSender sendingMocksender(arbiter, mockUDPSocketMaker);
  sendingQp.setupFlows(1, 0, &sendingMocksender);

  auto sendersFlow = sendingQp.getTxFlowById(0);

  // STEP 2: Send a data packet to QP for which there's no corresponding WQE
  // --------------------------------------

  auto payload = std::make_unique<char[]>(mtu);
  memset(payload.get(), 22, mtu);
  postWqeHelper(
      &sendingQp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .address = payload.get()});

  auto egressPacket = sendingQp.getNextPacketToSend();
  auto packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  auto initialFsn = packet->getDataHeader().getFsn();

  // STEP 3: Process the data packet - QP should respond with BRNR
  // --------------------------------------
  auto maybeAck = receiversFlow->processDataPacket(packet, kEcnECT0);

  // Verify that a BRNR ACK was generated
  ASSERT_TRUE(maybeAck.has_value()) << "Expected BRNR ACK to be generated";
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::BRNR)
      << "Expected ACK to have BRNR status";
  EXPECT_EQ(maybeAck.value().getRnrBackoffTimeExp(), kBrnrBackoffTimeExp)
      << "Expected backoff time exponent to be set correctly";

  // Verify the RNR bit vector has the correct bit set
  auto rnrBitVector = maybeAck.value().getRnrBitVector();
  BitVector<256> expectedRnrBitVector{};
  expectedRnrBitVector.setBit(0); // Bit for FSN 0 relative to rcvNxt=0
  EXPECT_EQ(rnrBitVector, expectedRnrBitVector);

  // STEP 4: Post receive WQE
  // --------------------------------------
  std::unique_ptr<char[]> rcvPayload(new char[mtu]);
  postInSequenceReceiveWqe(qp, rcvPayload.get(), mtu);

  // STEP 5: Simulate sender advancing and retransmitting with new FSN
  // --------------------------------------
  maybeAck.value().setTimestamp(0); // ensure not discarded due to timing;
  sendersFlow->processAckPacket(
      maybeAck.value(),
      simulatedProcessedAtNs(maybeAck.value().getTimestamp()));
  // Drive the BRNR backoff expiry deterministically (the real RTO timer is
  // suppressed) so the probe with the advanced FSN is produced on the test
  // thread instead of racing the timer.
  retxTimer.cancelCallback(sendingQp.getBrnrTimerCallback());
  sendingQp.getTag(kMetaRoceInitialTag)->brnr().beginProbe();
  egressPacket = sendingQp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  packet = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  EXPECT_EQ(packet->getDataHeader().getFsn(), initialFsn + 1);

  // STEP 6: Process the packet with the same data but new FSN
  // --------------------------------------
  maybeAck = receiversFlow->processDataPacket(packet, kEcnECT0);

  // Verify that an ACK has been generated
  ASSERT_TRUE(maybeAck.has_value()) << "Expected ACK for retransmitted packet";
  const auto& ack = maybeAck.value();
  EXPECT_EQ(ack.getAckStatus(), AckStatus::ACK)
      << "Expected ACK to have ACK status for retransmitted packet";

  // Verify the cumulative FSN and max received FSN are updated correctly
  EXPECT_EQ(ack.getCumulativeFsn(), initialFsn + 1);
  EXPECT_EQ(ack.getMaxRcvdFsn(), initialFsn + 1);
  EXPECT_EQ(rxState.rcvNxt, initialFsn + 2);

  // Check that the data of the single packet were delivered into the receive
  // buffer
  EXPECT_EQ(rcvPayload.get()[0], 22) << "buffer did not get the data";
  EXPECT_EQ(rcvPayload.get()[mtu - 1], 22) << "buffer did not get the data";
}

TEST_F(QpTest, BrnrSingleInSequence) {
  ScopedSuppressedTxTimers suppressTimers;
  // congestion controller to verify that brnr packets are not classified
  // as lost.  They do not need to be classified as successful transmissions
  // since we are already in need of slowing down.
  auto congestionController = std::make_unique<MockCongestionController>();
  EXPECT_CALL(*congestionController, onFlowAdded(0)).Times(1);
  EXPECT_CALL(*congestionController, onPacketSent(0)).Times(2);
  EXPECT_CALL(*congestionController, onPacketLoss(0, _)).Times(0);
  // onPacketAck must be called to decrement packets in flight.
  // Use Field matchers since RTT and rate fields have runtime values.
  EXPECT_CALL(
      *congestionController,
      onPacketAck(
          0,
          AllOf(
              Field(
                  &CongestionController::AckEvent::numAckedPackets,
                  Eq(static_cast<uint16_t>(1))),
              Field(
                  &CongestionController::AckEvent::numAckedPacketsWithCE,
                  Eq(static_cast<uint16_t>(0))),
              // smoothedQpRttNs is the estRttNs before updateRtt()
              Field(
                  &CongestionController::AckEvent::smoothedQpRttNs,
                  Eq(TxFlow::kInitialEstRttNs)),
              // measuredRttNs is actual elapsed time, should be < 1 second in
              // tests
              Field(
                  &CongestionController::AckEvent::measuredRttNs,
                  Lt(kOneSecondNs)),
              // rcnDesiredRateGbps defaults to kDefaultRateHintGbps (>=
              // kMinRateHintGbps)
              Field(
                  &CongestionController::AckEvent::rcnDesiredRateGbps,
                  Ge(kMinRateHintGbps)))))
      .Times(1);
  EXPECT_CALL(*congestionController, getAvailableWindowSize(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(100));
  qp.setCongestionController(std::move(congestionController));

  // qp is just a sending qp, no receive flows.
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 0, &mockSender);

  // create a receiving qp/flow to simplify making the brnr nak
  Qp receivingQp{kSecondQpNum, retxTimer, arbiter, mrs, &cq, ibvQp};
  receivingQp.addPortPair(
      *qp.getRemoteAddrs().begin(), *qp.getLocalAddrs().begin());

  LimitedTestSender receivingMocksender(arbiter, mockUDPSocketMaker);
  receivingQp.setupFlows(0, 1, &receivingMocksender);

  auto receivingFlow = receivingQp.getRxFlowById(0);

  auto payload = std::make_unique<char[]>(4096);
  memset(payload.get(), 33, mtu);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kWriteWithImm,
       .msgSizeBytes = 4096,
       .address = payload.get(),
       .maybeImmediateData = 121});

  auto packet = qp.getTxFlowById(0)->getNextPacketToSend();
  ASSERT_TRUE(packet) << "expected to have a sendable packet after posted wqe";

  // the receiver will now brnr this packet, because the receiver is not ready
  // for it.
  auto dataPacket = dynamic_cast<DataPacket*>(packet->packet.get());
  EXPECT_EQ(dataPacket->getDataHeader().getFsn(), 0);
  EXPECT_EQ(dataPacket->getDataHeader().getUna(), 0);
  EXPECT_EQ(dataPacket->getDataHeader().getPosn(), 0);
  auto msn = dataPacket->getDataHeader().getMsn();
  auto csn = dataPacket->getDataHeader().getCsn();
  auto maybeAck = receivingFlow->processDataPacket(dataPacket, kEcnECT0);
  ASSERT_TRUE(maybeAck.has_value()) << "Expected BRNR ACK";
  const auto& ack = maybeAck.value();
  EXPECT_EQ(ack.getAckStatus(), AckStatus::BRNR)
      << "Expected ACK of unexpected wr/imm to carry brnr";

  EXPECT_EQ(qp.getHrnrState(), HrnrState::NORMAL_TX);
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);

  qp.getTxFlowById(0)->processAckPacket(
      ack, simulatedProcessedAtNs(ack.getTimestamp()));

  // cc should have one packet "acked" for cc purposes, and none lost,
  // from expectations above.

  // Cancel the real BRNR timer and begin the probe from here, which the strict
  // onPacketSent/onPacketLoss expectations above need.
  retxTimer.cancelCallback(qp.getBrnrTimerCallback());
  initialTag->brnr().beginProbe();
  auto resendPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(resendPacket) << "did not get a retry packet; qp in BRNR state "
                            << initialTag->getBrnrState();
  dataPacket = dynamic_cast<DataPacket*>(resendPacket->packet.get());
  EXPECT_FALSE(dataPacket->getDataHeader().getIsRetransmit());
  EXPECT_EQ(dataPacket->getDataHeader().getFsn(), 1)
      << "Retry of BRNR should be on next fsn";
  EXPECT_EQ(dataPacket->getDataHeader().getUna(), 1)
      << "Retry of BRNR should advance una over brnr.";
  EXPECT_EQ(dataPacket->getDataHeader().getPosn(), 0)
      << "Retry of BRNR posn should still be zero.";
  EXPECT_EQ(dataPacket->getDataHeader().getMsn(), msn)
      << "Retry of BRNR should have same msn";
  EXPECT_EQ(dataPacket->getDataHeader().getCsn(), csn)
      << "Retry of BRNR should have same csn";
}

TEST_F(QpTest, BrnrDoubleInSequence) {
  ScopedSuppressedTxTimers suppressTimers;
  // congestion controller to verify that brnr packets are not classified
  // as lost.  They do not need to be classified as successful transmissions
  // since we are already in need of slowing down.
  auto congestionController = std::make_unique<MockCongestionController>();
  EXPECT_CALL(*congestionController, onFlowAdded(0)).Times(1);
  EXPECT_CALL(*congestionController, onPacketSent(0)).Times(4);
  EXPECT_CALL(*congestionController, onPacketLoss(0, _)).Times(0);
  // onPacketAck must be called to decrement packets in flight.
  // Use Field matchers since RTT and rate fields have runtime values.
  // Note: smoothedQpRttNs changes across acks, so we just verify it's positive.
  EXPECT_CALL(
      *congestionController,
      onPacketAck(
          0,
          AllOf(
              Field(
                  &CongestionController::AckEvent::numAckedPackets,
                  Eq(static_cast<uint16_t>(1))),
              Field(
                  &CongestionController::AckEvent::numAckedPacketsWithCE,
                  Eq(static_cast<uint16_t>(0))),
              // smoothedQpRttNs changes across acks, verify it's positive
              Field(&CongestionController::AckEvent::smoothedQpRttNs, Gt(0u)),
              // Note: measuredRttNs cannot be validated here since some acks
              // don't have timestamps set (BRNR resend acks)
              // rcnDesiredRateGbps defaults to kDefaultRateHintGbps (>=
              // kMinRateHintGbps)
              Field(
                  &CongestionController::AckEvent::rcnDesiredRateGbps,
                  Ge(kMinRateHintGbps)))))
      .Times(3);
  EXPECT_CALL(*congestionController, getAvailableWindowSize(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(100));
  qp.setCongestionController(std::move(congestionController));

  // qp is just a sending qp, no receive flows.
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 0, &mockSender);

  // create a receiving qp/flow to simplify making the brnr nak
  Qp receivingQp{kSecondQpNum, retxTimer, arbiter, mrs, &cq, ibvQp};
  receivingQp.setMtu(mtu);
  receivingQp.addPortPair(
      *qp.getRemoteAddrs().begin(), *qp.getLocalAddrs().begin());

  LimitedTestSender receivingMocksender2(arbiter, mockUDPSocketMaker);
  receivingQp.setupFlows(0, 1, &receivingMocksender2);

  auto receivingFlow = receivingQp.getRxFlowById(0);

  auto payload = std::make_unique<char[]>(4096);
  memset(payload.get(), 33, mtu);
  LOG(INFO) << "Posting two Write+Imm WQEs, each consisting of one data packet";
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kWriteWithImm,
       .msgSizeBytes = 4096,
       .address = payload.get(),
       .maybeImmediateData = 121});
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kWriteWithImm,
       .msgSizeBytes = 4096,
       .address = payload.get(),
       .maybeImmediateData = 121});

  LOG(INFO) << "Getting first sent data packet";
  auto packet = qp.getTxFlowById(0)->getNextPacketToSend();
  ASSERT_TRUE(packet) << "expected to have a sendable packet after posted wqe";

  // the receiver will now brnr this packet, because the receiver is not ready
  // for it.
  auto dataPacket = dynamic_cast<DataPacket*>(packet->packet.get());
  EXPECT_EQ(dataPacket->getDataHeader().getFsn(), 0);
  EXPECT_EQ(dataPacket->getDataHeader().getUna(), 0);
  EXPECT_EQ(dataPacket->getDataHeader().getPosn(), 0);
  auto msn = dataPacket->getDataHeader().getMsn();
  auto csn = dataPacket->getDataHeader().getCsn();

  LOG(INFO) << "Getting second sent data packet";
  auto packet2 = qp.getTxFlowById(0)->getNextPacketToSend();
  ASSERT_TRUE(packet2) << "expected to have a sendable packet after posted wqe";
  auto dataPacket2 = dynamic_cast<DataPacket*>(packet2->packet.get());
  EXPECT_EQ(dataPacket2->getDataHeader().getFsn(), 1);
  EXPECT_EQ(dataPacket2->getDataHeader().getUna(), 0);
  EXPECT_EQ(dataPacket2->getDataHeader().getPosn(), 0);
  EXPECT_EQ(dataPacket2->getDataHeader().getMsn(), msn + 1);
  EXPECT_EQ(dataPacket2->getDataHeader().getCsn(), csn + 1);

  LOG(INFO) << "Receiving first data packet, should be a BRNR";
  auto maybeAck = receivingFlow->processDataPacket(dataPacket, kEcnECT0);
  ASSERT_TRUE(maybeAck.has_value()) << "Expected BRNR ACK";
  const auto& ack = maybeAck.value();
  EXPECT_EQ(ack.getAckStatus(), AckStatus::BRNR)
      << "Expected ACK of unexpected wr/imm to carry brnr";

  LOG(INFO) << "Receiving ACK for first data packet, should enter BRNR state";
  qp.getTxFlowById(0)->processAckPacket(
      ack, simulatedProcessedAtNs(ack.getTimestamp()));

  // cc should have one packet "acked" for cc purposes, and none lost,
  // from expectations above.

  LOG(INFO) << "Getting resend of first data packet, should use a new FSN";
  // Drive the BRNR backoff expiry deterministically (real timers suppressed).
  retxTimer.cancelCallback(qp.getBrnrTimerCallback());
  initialTag->brnr().beginProbe();
  auto resendPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(resendPacket) << "did not get a retry packet; qp in BRNR state "
                            << initialTag->getBrnrState();
  dataPacket = dynamic_cast<DataPacket*>(resendPacket->packet.get());
  EXPECT_EQ(dataPacket->getDataHeader().getFsn(), 2)
      << "Retry of BRNR should be on next fsn";
  EXPECT_EQ(dataPacket->getDataHeader().getUna(), 1)
      << "Retry of BRNR should advance una over brnr.";
  EXPECT_EQ(dataPacket->getDataHeader().getPosn(), 0)
      << "Retry of BRNR posn should still be zero.";
  EXPECT_EQ(dataPacket->getDataHeader().getMsn(), msn)
      << "Retry of BRNR should have same msn";
  EXPECT_EQ(dataPacket->getDataHeader().getCsn(), csn)
      << "Retry of BRNR should have same csn";

  // this is a write-imm receive wqe; assume we don't know where it's going.
  // Post without a pre-assigned MSN so the RETH sizes the WQE on arrival.
  LOG(INFO)
      << "Posting one receive WQE, should resolve BRNR for first data packet";
  postWqeHelper(
      &receivingQp,
      {.workType = Wqe::WorkType::kReceive,
       .msgSizeBytes = 0,
       .address = nullptr});

  // receive second packet after first wqe was posted -> brnr
  LOG(INFO) << "Receiving second data packet, should be a BRNR";
  auto maybeAck2 = receivingFlow->processDataPacket(dataPacket2, kEcnECT0);
  ASSERT_TRUE(maybeAck2.has_value()) << "Expected BRNR ACK";
  auto& ack2 = maybeAck2.value();
  EXPECT_EQ(ack2.getAckStatus(), AckStatus::BRNR)
      << "Expected ACK of unexpected wr/imm to carry brnr";
  EXPECT_EQ(ack.getCumulativeFsn(), 0xffff)
      << "Receiver cannot advance without an update";

  // Processing a wqe in the test between packet generation and ack processing
  // inflates the measured rtt.  Prevent discard.
  ack2.setTimestamp(0);

  LOG(INFO) << "Receiving ACK for second data packet, should enter BRNR state";
  qp.getTxFlowById(0)->processAckPacket(
      ack2, simulatedProcessedAtNs(ack2.getTimestamp()));

  // Note that the correct behavior here is that a BRNR'd packet is not resent
  // as a retransmission, but on an entirely new FSN (in this case 2 and 3).

  // receive resend of first packet after first wqe was posted;
  LOG(INFO) << "Receiving resend of first data packet, should be okay";
  auto maybeAckResend = receivingFlow->processDataPacket(dataPacket, kEcnECT0);
  EXPECT_EQ(receivingFlow->getRxState().rcvNxt, 1);
  EXPECT_EQ(receivingFlow->getRxState().maxRcvdFsn, 2);
  ASSERT_TRUE(maybeAckResend.has_value()) << "Expected ACK after resend";
  auto& ackResend = maybeAckResend.value();
  EXPECT_EQ(ackResend.getAckStatus(), AckStatus::BRNR)
      << "Expected ACK of unexpected wr/imm to carry brnr";
  EXPECT_EQ(ackResend.getCumulativeFsn(), 0)
      << "Receiver should advance cfsn to zero after seeing una 1";

  // prevent discard where test rtt includes wqe post delay
  ackResend.setTimestamp(0);

  LOG(INFO)
      << "Receiving ACK for resend of first data packet, should clear BRNR";
  qp.getTxFlowById(0)->processAckPacket(
      ackResend, simulatedProcessedAtNs(ackResend.getTimestamp()));

  // The second ACK cleared BRNR, so the tag is back in NORMAL_TX; msn2's packet
  // is still unacked, so an RTO retransmits it on a new fsn. Drive that
  // deterministically (real timers suppressed).
  LOG(INFO) << "Getting resend of second data packet";
  ASSERT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);
  qp.getTxFlowById(0)->prepareRetxPacketsIfAny(true /*isRtoEvent*/);
  resendPacket = qp.getNextPacketToSend();
  ASSERT_TRUE(resendPacket) << "did not get a retry packet; qp in BRNR state "
                            << initialTag->getBrnrState();
  dataPacket = dynamic_cast<DataPacket*>(resendPacket->packet.get());
  EXPECT_EQ(dataPacket->getDataHeader().getMsn(), 2)
      << "Retry of BRNR should have same msn";
  EXPECT_EQ(dataPacket->getDataHeader().getFsn(), 3)
      << "Retry of BRNR should be on next fsn";
  EXPECT_GE(dataPacket->getDataHeader().getUna(), 1)
      << "Retry of BRNR should advance una over brnr.";
  EXPECT_EQ(dataPacket->getDataHeader().getPosn(), 0)
      << "Retry of BRNR posn should still be zero.";
  EXPECT_GE(dataPacket->getDataHeader().getMsn(), msn)
      << "Retry of BRNR should have same msn";
  EXPECT_GE(dataPacket->getDataHeader().getCsn(), csn)
      << "Retry of BRNR should have same csn";

  // Check that the data of the single packet were delivered into the receive
  // buffer The posted write wqes used the default destination buffer.
  EXPECT_EQ(receiveBuffer[0], 33) << "buffer did not get the data";
  EXPECT_EQ(receiveBuffer[mtu - 1], 33) << "buffer did not get the data";
}

TEST_F(QpTest, SetupTellsTheControllerHowManyPortPairsTheFlowsSpan) {
  Qp multiportQp{31, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiportQp.setMtu(mtu);
  multiportQp.addPortPair(
      folly::IPAddress("fc00::101"), folly::IPAddress("fc00::1"));
  multiportQp.addPortPair(
      folly::IPAddress("fc00::102"), folly::IPAddress("fc00::2"));
  multiportQp.addPortPair(
      folly::IPAddress("fc00::103"), folly::IPAddress("fc00::3"));
  auto controller = std::make_unique<NiceMock<MockCongestionController>>();
  EXPECT_CALL(*controller, setPortPairCount(3)).Times(1);
  multiportQp.setCongestionController(std::move(controller));

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  mockSender.setLimitNumSockets(0);
  multiportQp.setupFlows(6, 6, &mockSender);
}

TEST_F(QpTest, WithGsoSetupKeepsEachOpeningBurstOnOnePath) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_send = "gso";
  // Constructed after the flag, since the QP reads the send mode then.
  Qp gsoQp{31, retxTimer, arbiter, mrs, &cq, ibvQp};
  gsoQp.setMtu(mtu);
  gsoQp.addPortPair(folly::IPAddress("fc00::101"), folly::IPAddress("fc00::1"));
  gsoQp.addPortPair(folly::IPAddress("fc00::102"), folly::IPAddress("fc00::2"));
  gsoQp.addPortPair(folly::IPAddress("fc00::103"), folly::IPAddress("fc00::3"));
  auto controller = std::make_unique<NiceMock<MockCongestionController>>();
  // One port pair keeps each flow's opening burst consecutive, so gso can send
  // it as one datagram.
  EXPECT_CALL(*controller, setPortPairCount(1)).Times(1);
  gsoQp.setCongestionController(std::move(controller));

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  mockSender.setLimitNumSockets(0);
  gsoQp.setupFlows(6, 6, &mockSender);
}

/*
 * Tests that the multiport functionality works correctly with packet
 * generation. This test verifies that packets use the correct remote address
 * from round-robin selection.
 */
TEST_F(QpTest, MultiportPacketGeneration) {
  const folly::IPAddress localAddr1("fc00::101");
  const folly::IPAddress localAddr2("fc00::102");
  const folly::IPAddress localAddr3("fc00::103");
  const folly::IPAddress remoteAddr1("fc00::1");
  const folly::IPAddress remoteAddr2("fc00::2");
  const folly::IPAddress remoteAddr3("fc00::3");

  // Create a fresh QP for this test
  Qp multiportQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiportQp.setMtu(mtu);
  multiportQp.addPortPair(localAddr1, remoteAddr1);
  multiportQp.addPortPair(localAddr2, remoteAddr2);
  multiportQp.addPortPair(localAddr3, remoteAddr3);

  createAlwaysSendingMockCongestionController(multiportQp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  mockSender.setLimitNumSockets(0);
  multiportQp.setupFlows(6, 6, &mockSender);

  const uint16_t msgSizeBytes = mtu * 6;
  auto payload = std::make_unique<char[]>(msgSizeBytes);

  // Post a WQE that will generate multiple packets
  postWqeHelper(
      &multiportQp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(msgSizeBytes),
       .address = payload.get()});

  // Get packets and verify they use different remote addresses in
  // round-robin fashion
  std::vector<folly::IPAddress> usedAddresses;
  for (int i = 0; i < 6; i++) {
    auto egressPacket = multiportQp.getNextPacketToSend();
    ASSERT_TRUE(egressPacket) << "Expected packet " << i;
    usedAddresses.push_back(egressPacket->remoteAddr);
  }

  // With the default diagonal wiring each flow stays on its own plane, so the
  // spray cycles remoteAddr1, remoteAddr2, remoteAddr3, then repeats.
  EXPECT_THAT(
      usedAddresses,
      ::testing::ElementsAre(
          remoteAddr1,
          remoteAddr2,
          remoteAddr3,
          remoteAddr1,
          remoteAddr2,
          remoteAddr3));
}

// Under gso, getNextPacketToSend holds a QP on one flow until that flow can no
// longer send (its window is spent), then advances -- so a turn's sends
// coalesce into a GSO run per flow, instead of the default's per-send flow
// rotation.
TEST_F(QpTest, GsoHoldsFlowUntilWindowDrainsThenAdvances) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_send = "gso";

  const folly::IPAddress localAddr1("fc00::101");
  const folly::IPAddress localAddr2("fc00::102");
  const folly::IPAddress remoteAddr1("fc00::1");
  const folly::IPAddress remoteAddr2("fc00::2");

  // gsoEnabled_ is latched in the ctor, so construct the QP under the flag.
  Qp gsoQp{31, retxTimer, arbiter, mrs, &cq, ibvQp};
  gsoQp.setMtu(4096);
  gsoQp.addPortPair(localAddr1, remoteAddr1);
  gsoQp.addPortPair(localAddr2, remoteAddr2);

  // Inflight-derived per-flow window: a flow blocks once it has kWindow
  // unacked, so the QP must advance -- exercising drain-then-advance.
  constexpr uint16_t kWindow = 4;
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault([&gsoQp](uint16_t flowId) -> uint16_t {
        const auto& txState = gsoQp.getTxFlowById(flowId)->getTxState();
        const uint16_t inflight = txState.sndNxt - txState.sndUna;
        return inflight < kWindow ? kWindow - inflight : 0;
      });
  gsoQp.setCongestionController(std::move(congestionController));

  // Holding is a within-turn effect, so the turn must be able to carry the
  // whole window; at the default one-packet batch there is nothing to coalesce
  // and the QP rotates per send.
  LimitedTestSender mockSender(
      arbiter,
      mockUDPSocketMaker,
      Sender::kDefaultSendToPort,
      LimitedTestSender::kDisableSendLoop,
      /*sendBatch=*/kWindow + 1);
  mockSender.setLimitNumSockets(0);
  gsoQp.setupFlows(2, 2, &mockSender);

  // One send past a full window, so both the hold and the advance are visible.
  const uint32_t msgSizeBytes =
      static_cast<uint32_t>(gsoQp.getMtu()) * (kWindow + 1);
  auto payload = std::make_unique<char[]>(msgSizeBytes);
  postWqeHelper(
      &gsoQp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get()});

  std::vector<folly::IPAddress> addrs;
  for (int i = 0; i < kWindow + 1; ++i) {
    auto egressPacket = gsoQp.getNextPacketToSend();
    ASSERT_TRUE(egressPacket) << "expected packet " << i;
    addrs.push_back(egressPacket->remoteAddr);
  }

  const folly::IPAddress heldFlowAddr = addrs.front();
  for (int i = 1; i < kWindow; ++i) {
    EXPECT_EQ(addrs[i], heldFlowAddr)
        << "GSO must hold the flow while it has window so the run coalesces; send "
        << i << " left the flow early";
  }
  EXPECT_NE(addrs[kWindow], heldFlowAddr)
      << "the QP must advance to another flow once the held flow's window is "
         "spent";
}

// A count carried across the switch would release the new flow early.
TEST_F(QpTest, GsoHoldCountRestartsWhenTheControllerSwitchesFlows) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_send = "gso";

  const folly::IPAddress localAddr1("fc00::101");
  const folly::IPAddress localAddr2("fc00::102");
  const folly::IPAddress remoteAddr1("fc00::1");
  const folly::IPAddress remoteAddr2("fc00::2");

  // gsoEnabled_ is latched in the ctor, so construct the QP under the flag.
  Qp gsoQp{33, retxTimer, arbiter, mrs, &cq, ibvQp};
  gsoQp.setMtu(4096);
  gsoQp.addPortPair(localAddr1, remoteAddr1);
  gsoQp.addPortPair(localAddr2, remoteAddr2);

  constexpr uint16_t kAlwaysOpenWindow = 4096;
  constexpr uint32_t kBatch = 4;
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(kAlwaysOpenWindow));
  // Two sends on flow 0, then hand the QP to flow 1 and stop steering, so the
  // count flow 1 starts from is the only thing under test.
  auto calls = std::make_shared<int>(0);
  ON_CALL(*congestionController, flowForNextSend())
      .WillByDefault(
          [calls]() -> std::optional<CongestionController::NextSend> {
            ++*calls;
            if (*calls <= 2) {
              return CongestionController::NextSend{0, false};
            }
            if (*calls == 3) {
              return CongestionController::NextSend{1, false};
            }
            return std::nullopt;
          });
  gsoQp.setCongestionController(std::move(congestionController));

  LimitedTestSender mockSender(
      arbiter,
      mockUDPSocketMaker,
      Sender::kDefaultSendToPort,
      LimitedTestSender::kDisableSendLoop,
      /*sendBatch=*/kBatch);
  mockSender.setLimitNumSockets(0);
  gsoQp.setupFlows(2, 2, &mockSender);

  constexpr uint32_t kSends = 8;
  const uint32_t msgSizeBytes = static_cast<uint32_t>(gsoQp.getMtu()) * kSends;
  auto payload = std::make_unique<char[]>(msgSizeBytes);
  postWqeHelper(
      &gsoQp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get()});

  std::vector<folly::IPAddress> addrs;
  for (uint32_t i = 0; i < kSends; ++i) {
    auto egressPacket = gsoQp.getNextPacketToSend();
    ASSERT_TRUE(egressPacket) << "expected packet " << i;
    addrs.push_back(egressPacket->remoteAddr);
  }

  // Length of the first run on the flow the controller switched to.
  size_t switchAt = 0;
  while (switchAt < addrs.size() && addrs[switchAt] == addrs.front()) {
    ++switchAt;
  }
  ASSERT_LT(switchAt, addrs.size())
      << "the controller should have moved the QP to a second flow";
  size_t runOnSwitchedFlow = 0;
  while (switchAt + runOnSwitchedFlow < addrs.size() &&
         addrs[switchAt + runOnSwitchedFlow] == addrs[switchAt]) {
    ++runOnSwitchedFlow;
  }
  EXPECT_EQ(runOnSwitchedFlow, kBatch)
      << "the switched-to flow must get a full turn's sends (" << kBatch
      << "), not inherit the previous flow's count; it got "
      << runOnSwitchedFlow;
}

// The starvation case the per-turn cap exists for: a flow whose window is
// always open never goes dry, so the hold has to be released on send count
// alone or the other flows never run.
TEST_F(QpTest, GsoHoldReleasesAnAlwaysReadyFlowSoOthersStillSend) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_send = "gso";

  const folly::IPAddress localAddr1("fc00::101");
  const folly::IPAddress localAddr2("fc00::102");
  const folly::IPAddress remoteAddr1("fc00::1");
  const folly::IPAddress remoteAddr2("fc00::2");

  // gsoEnabled_ is latched in the ctor, so construct the QP under the flag.
  Qp gsoQp{32, retxTimer, arbiter, mrs, &cq, ibvQp};
  gsoQp.setMtu(4096);
  gsoQp.addPortPair(localAddr1, remoteAddr1);
  gsoQp.addPortPair(localAddr2, remoteAddr2);

  // A window that never closes stands in for acks refilling the held flow
  // between turns, so neither "ran dry" nor "window spent" can end the hold.
  constexpr uint16_t kAlwaysOpenWindow = 4096;
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(kAlwaysOpenWindow));
  gsoQp.setCongestionController(std::move(congestionController));

  // Several turns' worth of sends, so a flow held past its turn is visible.
  constexpr uint32_t kBatch = 4;
  constexpr uint32_t kSends = kBatch * 3;
  LimitedTestSender mockSender(
      arbiter,
      mockUDPSocketMaker,
      Sender::kDefaultSendToPort,
      LimitedTestSender::kDisableSendLoop,
      /*sendBatch=*/kBatch);
  mockSender.setLimitNumSockets(0);
  gsoQp.setupFlows(2, 2, &mockSender);

  const uint32_t msgSizeBytes = static_cast<uint32_t>(gsoQp.getMtu()) * kSends;
  auto payload = std::make_unique<char[]>(msgSizeBytes);
  postWqeHelper(
      &gsoQp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSizeBytes,
       .address = payload.get()});

  std::vector<folly::IPAddress> addrs;
  for (uint32_t i = 0; i < kSends; ++i) {
    auto egressPacket = gsoQp.getNextPacketToSend();
    ASSERT_TRUE(egressPacket) << "expected packet " << i;
    addrs.push_back(egressPacket->remoteAddr);
  }

  uint32_t run = 1;
  uint32_t longestRun = 1;
  for (size_t i = 1; i < addrs.size(); ++i) {
    run = (addrs[i] == addrs[i - 1]) ? run + 1 : 1;
    if (run > longestRun) {
      longestRun = run;
    }
  }
  // A run covering every send is total starvation; any run past kBatch is a
  // hold outliving its turn. One bound catches both.
  EXPECT_LE(longestRun, kBatch)
      << "the hold must end after one turn's sends (" << kBatch
      << ") so a refilled window cannot keep the other flows waiting; the QP "
         "stayed on "
      << addrs.front() << " for " << longestRun << " of " << kSends << " sends";
}

/*
 * An oversize failure marks the whole tag's work failing while another flow
 * still has a frame in flight, and that flow's tracker holds the work request
 * it belongs to by raw pointer. No WQE under a tracker may be freed: a work
 * request is retired only once its packets have settled. Nor does any flow
 * send for the tag while it is in error. The tag's held count is what says
 * the WQE survived; ASAN catches the dangling read as well.
 */
TEST_F(QpTest, OversizeWqeDoesNotFreeAWqeAnotherFlowsTrackerHolds) {
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(2, 1, &mockSender);
  auto flow0 = qp.getTxFlowById(0);
  auto flow1 = qp.getTxFlowById(1);
  auto payload = std::make_unique<char[]>(1 << 16);

  // Sent on flow 1 and never acknowledged, so that flow's tracker holds a raw
  // pointer into a WQE the flush must not free.
  auto inFlightWqe = postWqeHelper(
      &qp,
      {
          .workType = Wqe::WorkType::kSend,
          .msgSizeBytes = 1024,
          .workReqId = 1,
          .address = payload.get(),
      });
  ASSERT_NE(inFlightWqe, nullptr)
      << "precondition: the earlier send must be posted, or there is no work "
         "ahead of the oversize one to report behind";
  ASSERT_TRUE(flow1->getNextPacketToSend())
      << "flow 1 must send a frame so its tracker holds a work request";

  auto oversizeWqe = postWqeHelper(
      &qp,
      {
          .workType = Wqe::WorkType::kSend,
          .msgSizeBytes = 512 * 1024 * 1024,
          .workReqId = 2,
          .address = payload.get(),
      });
  ASSERT_NE(oversizeWqe, nullptr)
      << "precondition: the oversize send must be posted, or the failure "
         "under test never happens";

  EXPECT_FALSE(flow0->getNextPacketToSend())
      << "the oversize work request yields no packet";
  EXPECT_FALSE(flow1->getNextPacketToSend())
      << "a tag in error has no work for any flow to send, whichever flow the "
         "failure was found on";
  EXPECT_EQ(initialTag->getNumHeldTxWqes(), 2u)
      << "the tag must still hold both work requests: flow 1's tracker "
         "points to the earlier one by raw pointer, so freeing it here would "
         "leave that entry dangling";
}

/*
 * An oversize work request fails once the sender reaches it, which can be
 * behind work requests already sent and awaiting acknowledgement. Those are
 * marked failing rather than reported where they stand, since their packets
 * are still on the wire, and the completion queue still reads in posting order.
 */
TEST_F(QpTest, OversizeWqeReportsBehindEarlierWorkOnceItSettles) {
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);
  auto payload = std::make_unique<char[]>(1 << 16);

  // Sent but never acknowledged, so it is still pending when the oversize work
  // request behind it fails.
  auto earlierWqe = postWqeHelper(
      &qp,
      {
          .workType = Wqe::WorkType::kSend,
          .msgSizeBytes = 1024,
          .workReqId = 1,
          .address = payload.get(),
      });
  ASSERT_NE(earlierWqe, nullptr)
      << "precondition: the earlier send must be posted, or the oversize one "
         "has nothing to settle behind";
  ASSERT_TRUE(flow->getNextPacketToSend())
      << "the earlier work request must be sent so it stays pending unacknowledged";

  auto oversizeWqe = postWqeHelper(
      &qp,
      {
          .workType = Wqe::WorkType::kSend,
          .msgSizeBytes = 512 * 1024 * 1024,
          .workReqId = 2,
          .address = payload.get(),
      });
  ASSERT_NE(oversizeWqe, nullptr)
      << "precondition: the oversize send must be posted, or the failure "
         "under test never happens";

  EXPECT_FALSE(flow->getNextPacketToSend())
      << "the oversize work request yields no packet";

  EXPECT_EQ(cq.getNumCompletedWqes(), 0)
      << "the earlier work request still has a packet on the wire, so no "
         "completion may be reported while the buffer behind it may still be "
         "read";

  // The sender reaches the abandoned packet and stops retransmitting it, which
  // resolves the earlier work request's last outstanding posn.
  flow->prepareRetxPacketsIfAny(true /*isRtoEvent*/);

  ASSERT_EQ(cq.getNumCompletedWqes(), 2)
      << "the oversize failure must account for the whole queue, not just "
         "itself";

  Cqe first = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(first.workReqId, 1)
      << "the work request posted first must complete first";
  EXPECT_EQ(first.status, kWrFlushErr)
      << "the queue pair flushes a work request left pending when it entered "
         "the error state";

  Cqe second = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(second.workReqId, 2)
      << "the oversize work request completes behind the one posted before it";
  EXPECT_EQ(second.status, kLocalLenErr)
      << "the oversize work request reports a length error, not the flush status";
}

/*
 * Confirm wqe in error for oversize send wqe.
 */
TEST_F(QpTest, OversizeWqeError) {
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);
  // 512 MB - it is possible to send this with 8k mtu, so not inherently
  // busted.
  const size_t msgSizeBytes = 512 * 1024 * 1024;
  auto payload = std::make_unique<char[]>(
      1 << 16); // 64KB (avoid memory allocation failure issues.
  auto expectedWrId = 1212;
  auto postedWqe = postWqeHelper(
      &qp,
      {
          .workType = Wqe::WorkType::kSend,
          .msgSizeBytes = msgSizeBytes,
          .workReqId = expectedWrId,
          .address = payload.get(),
      });

  EXPECT_NE(postedWqe, nullptr);
  EXPECT_EQ(qp.getNumPendingWqes(), 1);

  auto egressPacket = flow->getNextPacketToSend();
  EXPECT_FALSE(egressPacket) << "should get no packets from the wqe in error";

  EXPECT_EQ(cq.getNumCompletedWqes(), 1);

  ibv_wc wc{};
  auto ibv_poll_cq_lambda = [this, &wc]() -> folly::coro::Task<> {
    Cqe cqe = co_await cq.pollCq();
    wc.status = static_cast<ibv_wc_status>(cqe.status);
    wc.wr_id = cqe.workReqId;
    wc.qp_num = cqe.qpn;
    wc.byte_len = cqe.sizeBytes;
    wc.opcode = cqe.opcode;
    if (cqe.maybeImmData.has_value()) {
      wc.wc_flags = IBV_WC_WITH_IMM;
      wc.imm_data = cqe.maybeImmData.value();
    }
  };

  folly::coro::blockingWait(
      folly::coro::timeout(
          ibv_poll_cq_lambda(), std::chrono::milliseconds(1000)));
  postedWqe = nullptr; // The postedWqe has been consumed and destroyed.
  EXPECT_NE(wc.status, 0)
      << "an oversize send wqe should fail; we don't especially care which code.";
  EXPECT_EQ(wc.wr_id, expectedWrId) << "the wrong wqe was completed first";

  EXPECT_EQ(ibvQpStorage.state, IBV_QPS_ERR)
      << "the qp was not placed in error after returning with an error cqe";

  expectedWrId += 1111;
  // posting another wqe should yield a qp in error and a completion
  // flushed in error; ensuring that it is undersized.
  postedWqe = postWqeHelper(
      &qp,
      {
          .workType = Wqe::WorkType::kSend,
          .msgSizeBytes = 1024,
          .workReqId = expectedWrId,
          .address = payload.get(),
      });
  EXPECT_NE(postedWqe, nullptr);

  // simulate an arbiter visit to ensure liveness.  else we might wait below
  // forever.
  egressPacket = flow->getNextPacketToSend();
  EXPECT_FALSE(egressPacket) << "should get no packets from the wqe in error";

  // A clone of the ibv_poll_cq body. The Cqe to ibv_wc mapping is not shared,
  // so this copy sets only the fields the checks below read.
  folly::coro::blockingWait(
      folly::coro::timeout(
          ibv_poll_cq_lambda(), std::chrono::milliseconds(1000)));
  postedWqe = nullptr; // The postedWqe has been consumed and destroyed.
  EXPECT_EQ(wc.status, IBV_WC_WR_FLUSH_ERR)
      << "a wqe posted after a failed wqe should be flushed in error.";
  EXPECT_EQ(wc.wr_id, expectedWrId) << "the wrong wqe was completed second";
}

/*
 * Tests that TX CQEs are posted in order even when they are ACKed
 * out of order.
 *
 * This test verifies the tx in-order completion mechanism:
 * 1. Posts three send WQEs
 * 2. ACKs packets from WQE 2 and WQE 3 first (out of order)
 * 3. Verifies that no CQEs are generated yet
 * 4. ACKs packets from WQE 1
 * 5. Verifies that all three CQEs are posted in order (1, 2, 3)
 */
TEST_F(QpTest, TxWqeInOrderCompletion) {
  // Setup QP with flows
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Create three WQEs, each with a single packet
  auto payload = std::make_unique<char[]>(mtu * 3);
  const auto wqe1 = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .workReqId = 100,
       .address = payload.get()});
  ASSERT_TRUE(allocateSendablePosns(wqe1))
      << "precondition: the first send is on the wire, so the acks below complete it first";

  const auto wqe2 = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .workReqId = 200,
       .address = payload.get()});
  ASSERT_TRUE(allocateSendablePosns(wqe2))
      << "precondition: the second send is on the wire, so the acks below complete it second";

  const auto wqe3 = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .workReqId = 300,
       .address = payload.get()});
  ASSERT_TRUE(allocateSendablePosns(wqe3))
      << "precondition: the third send is on the wire, so the acks below complete it last";

  const uint16_t msn1 = wqe1->getMsn().value();
  const uint16_t msn3 = wqe3->getMsn().value();

  EXPECT_EQ(qp.getNumPendingWqes(), 3);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);
  EXPECT_EQ(initialTag->getTxCompletedMsn(), msn1 - 1);

  // ACK packets from WQE 2 (out of order)
  std::vector<PacketId> ackedPackets;
  ackedPackets.emplace_back(wqe2, 0, 0, mtu);
  qp.checkForTxMessageCompletion(ackedPackets);

  // Verify WQE 2 is not completed yet (waiting for WQE 1)
  EXPECT_EQ(qp.getNumPendingWqes(), 3);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);
  EXPECT_EQ(initialTag->getTxCompletedMsn(), msn1 - 1);

  // ACK packets from WQE 3 (out of order)
  ackedPackets.clear();
  ackedPackets.emplace_back(wqe3, 0, 0, mtu);
  qp.checkForTxMessageCompletion(ackedPackets);

  // Verify WQE 3 is not completed yet (waiting for WQE 1 and 2)
  EXPECT_EQ(qp.getNumPendingWqes(), 3);
  EXPECT_EQ(cq.getNumCompletedWqes(), 0);
  EXPECT_EQ(initialTag->getTxCompletedMsn(), msn1 - 1);

  // ACK packets from WQE 1 (the missing one)
  ackedPackets.clear();
  ackedPackets.emplace_back(wqe1, 0, 0, mtu);
  qp.checkForTxMessageCompletion(ackedPackets);

  // Verify all three WQEs are completed in order
  EXPECT_EQ(qp.getNumPendingWqes(), 0);
  EXPECT_EQ(cq.getNumCompletedWqes(), 3);
  EXPECT_EQ(initialTag->getTxCompletedMsn(), msn3);

  // Verify completion order by checking CQ
  const std::vector<uint64_t> expectedWorkReqIds{100, 200, 300};
  std::vector<uint64_t> actualWorkReqIds;
  for (size_t i = 0; i < 3; ++i) {
    auto cqe = folly::coro::blockingWait(cq.pollCq());
    actualWorkReqIds.push_back(cqe.workReqId);
  }
  EXPECT_EQ(actualWorkReqIds, expectedWorkReqIds);
}

/*
 * Test execution fence with multiple packets: verify fence blocks until
 * ALL packets of previous WQE are ACKed.
 */
TEST_F(QpTest, ExecutionFenceMultiplePackets) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  // Create WQE1 with 3 packets (msgSize > mtu)
  const uint16_t msgSize1 = mtu * 2 + mtu / 2; // 3 packets
  auto payload1 = std::make_unique<char[]>(msgSize1);
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSize1,
       .address = payload1.get()});

  // Transmit all 3 packets of WQE1
  for (int i = 0; i < 3; i++) {
    auto packet = flow->getNextPacketToSend();
    ASSERT_TRUE(packet) << "Should send packet " << i << " of WQE1";
  }

  // Post WQE2 with fence
  const uint16_t msgSize2 = mtu;
  auto payload2 = std::make_unique<char[]>(msgSize2);
  auto wqe2 = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = msgSize2,
       .address = payload2.get()},
      /*doProcessWqe=*/false);
  wqe2->setFenceFlag(true);
  // Must wait for both WQE1 and WQE2 so that WQE2's MSN is assigned.
  processWqes(&qp, 2, 0);
  uint16_t msn2 = wqe2->getMsn().value();

  // WQE2 should be blocked
  EXPECT_FALSE(flow->getNextPacketToSend())
      << "WQE2 blocked: WQE1 has 3 unACKed packets";

  // ACK first 2 packets of WQE1 (still incomplete)
  auto& txState = flow->getTxState();
  uint32_t baseFsn = txState.sndUna;

  // Note: processAckPacket is cumulative, so we ACK up to FSN individually
  for (int i = 0; i < 2; i++) {
    uint32_t fsn = baseFsn + i;
    const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                    .setCumulativeFsn(fsn)
                                    .setMaxRcvdFsn(fsn)
                                    .setAckStatus(AckStatus::ACK)
                                    .setTimestamp(0)
                                    .build();
    flow->processAckPacket(
        ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));
  }

  // WQE2 should STILL be blocked (1 packet remaining)
  EXPECT_FALSE(flow->getNextPacketToSend())
      << "WQE2 still blocked: WQE1 has 1 unACKed packet";
  // ACK final packet of WQE1
  uint32_t finalFsn = baseFsn + 2;
  const AckHeader ackHeader3 = AckHeader::AckHeaderBuilder()
                                   .setCumulativeFsn(finalFsn)
                                   .setMaxRcvdFsn(finalFsn)
                                   .setAckStatus(AckStatus::ACK)
                                   .setTimestamp(0)
                                   .build();
  flow->processAckPacket(
      ackHeader3, simulatedProcessedAtNs(ackHeader3.getTimestamp()));

  // Now WQE2 should be allowed
  auto packet2 = flow->getNextPacketToSend();
  ASSERT_TRUE(packet2) << "WQE2 allowed after all WQE1 packets ACKed";
  auto dataPacket2 = dynamic_cast<DataPacket*>(packet2->packet.get());
  EXPECT_EQ(dataPacket2->getDataHeader().getMsn(), msn2);

  LOG(INFO) << "ExecutionFenceMultiplePackets test passed";
}

TEST_F(QpTest, AnExecutionFenceHoldsTheWorkPostedBehindIt) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  auto payload = std::make_unique<char[]>(mtu);
  const WqeData oneFrameSend{
      .workType = Wqe::WorkType::kSend,
      .msgSizeBytes = mtu,
      .address = payload.get()};
  const Wqe* const first = postWqeHelper(&qp, oneFrameSend);
  auto firstPacket = flow->getNextPacketToSend();
  ASSERT_TRUE(firstPacket)
      << "precondition: the first send is on the wire and unacked, which is "
         "what the fence waits on";
  auto* firstData = dynamic_cast<DataPacket*>(firstPacket->packet.get());
  ASSERT_NE(firstData, nullptr);
  ASSERT_EQ(firstData->getDataHeader().getMsn(), first->getMsn().value())
      << "precondition: the packet on the wire is the first send's, so the "
         "fence has exactly that work to wait on";
  const uint16_t firstFsn = flow->getTxState().sndUna;

  auto* fenced = postWqeHelper(&qp, oneFrameSend, /*doProcessWqe=*/false);
  fenced->setFenceFlag(true);
  auto* behindFence = postWqeHelper(&qp, oneFrameSend, /*doProcessWqe=*/false);
  ASSERT_TRUE(processWqes(&qp, 3, 0))
      << "precondition: all three sends are on the tag, so each has its msn";

  EXPECT_FALSE(flow->getNextPacketToSend())
      << "the sender should hold the unfenced send posted behind the fenced "
         "one; sending it would run it ahead of the fence";

  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(firstFsn)
                                  .setMaxRcvdFsn(firstFsn)
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(0)
                                  .build();
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  auto afterFence = flow->getNextPacketToSend();
  ASSERT_TRUE(afterFence)
      << "the first send's completion should release the fenced send";
  auto* afterFenceData = dynamic_cast<DataPacket*>(afterFence->packet.get());
  ASSERT_NE(afterFenceData, nullptr);
  EXPECT_EQ(afterFenceData->getDataHeader().getMsn(), fenced->getMsn().value())
      << "the fenced send should go out first once the work before it "
         "completes";
  auto last = flow->getNextPacketToSend();
  ASSERT_TRUE(last) << "the send behind the fence should follow it";
  auto* lastData = dynamic_cast<DataPacket*>(last->packet.get());
  ASSERT_NE(lastData, nullptr);
  EXPECT_EQ(lastData->getDataHeader().getMsn(), behindFence->getMsn().value())
      << "the unfenced send should go out behind the fenced one";
}

TEST_F(QpTest, AFenceHoldsBackOnlyItsOwnTag) {
  ScopedSuppressedTxTimers suppressTimers;
  constexpr uint8_t kOtherTag = kMetaRoceInitialTag + 1;
  qp.addTag(kOtherTag, &cq, &otherIbvQpStorage);
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  auto payload = std::make_unique<char[]>(mtu);
  const WqeData initialTagSend{
      .workType = Wqe::WorkType::kSend,
      .msgSizeBytes = mtu,
      .tagNum = kMetaRoceInitialTag,
      .address = payload.get()};
  WqeData otherTagSend = initialTagSend;
  otherTagSend.tagNum = kOtherTag;
  const auto nextDataHeader = [&flow]() -> std::optional<DataHeader> {
    auto egressPacket = flow->getNextPacketToSend();
    if (!egressPacket) {
      return std::nullopt;
    }
    auto* dataPacket = dynamic_cast<DataPacket*>(egressPacket->packet.get());
    if (dataPacket == nullptr) {
      return std::nullopt;
    }
    return dataPacket->getDataHeader();
  };

  auto* prior = postWqeHelper(&qp, initialTagSend, /*doProcessWqe=*/false);
  ASSERT_NE(postWqeHelper(&qp, otherTagSend, /*doProcessWqe=*/false), nullptr);
  ASSERT_TRUE(processWqes(&qp, 2, 0))
      << "precondition: both tags hold their first send";
  const auto priorHeader = nextDataHeader();
  ASSERT_TRUE(priorHeader.has_value())
      << "precondition: the prior is on the wire and unacked, which is what "
         "the fence waits on";
  ASSERT_EQ(priorHeader->getMsn(), prior->getMsn().value())
      << "precondition: the first packet carries the prior";
  ASSERT_TRUE(nextDataHeader().has_value())
      << "precondition: the other tag's first send went out last, so the "
         "next scan starts at the fenced tag";

  auto* fenced = postWqeHelper(&qp, initialTagSend, /*doProcessWqe=*/false);
  fenced->setFenceFlag(true);
  auto* other = postWqeHelper(&qp, otherTagSend, /*doProcessWqe=*/false);
  ASSERT_TRUE(processWqes(&qp, 4, 0))
      << "precondition: the fenced send and the other tag's second send are "
         "on their tags";

  const auto otherHeader = nextDataHeader();
  ASSERT_TRUE(otherHeader.has_value())
      << "a fenced send whose prior is unacked must not stop the scan from "
         "serving another tag";
  EXPECT_EQ(otherHeader->getTag(), kOtherTag)
      << "the packet the scan finds past the fence must come from the tag "
         "that has sendable work";
  EXPECT_EQ(otherHeader->getMsn(), other->getMsn().value())
      << "the packet the scan finds past the fence must be the other tag's "
         "second send";
  EXPECT_FALSE(nextDataHeader().has_value())
      << "with the fenced send held and the other tag empty, the sender must "
         "find nothing to send";

  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(priorHeader->getFsn())
                                  .setMaxRcvdFsn(priorHeader->getFsn())
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(priorHeader->getTimestamp())
                                  .build();
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));
  const auto fencedHeader = nextDataHeader();
  ASSERT_TRUE(fencedHeader.has_value())
      << "the prior's completion must release the fenced send";
  EXPECT_EQ(fencedHeader->getMsn(), fenced->getMsn().value())
      << "once its prior completes, the fenced send is the one sent";
}

TEST_F(QpTest, AFencedSendDoesNotStopTheRtoRetransmittingItsPrior) {
  ScopedSuppressedTxTimers suppressTimers;
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto* const flow = qp.getTxFlowById(0);

  auto payload = std::make_unique<char[]>(mtu);
  const WqeData oneFrameSend{
      .workType = Wqe::WorkType::kSend,
      .msgSizeBytes = mtu,
      .address = payload.get()};
  ASSERT_NE(postWqeHelper(&qp, oneFrameSend), nullptr);
  ASSERT_TRUE(flow->getNextPacketToSend())
      << "precondition: the prior is on the wire and unacked";
  auto* fenced = postWqeHelper(&qp, oneFrameSend, /*doProcessWqe=*/false);
  fenced->setFenceFlag(true);
  ASSERT_TRUE(processWqes(&qp, 2, 0))
      << "precondition: the fenced send is on the tag behind its prior";
  ASSERT_EQ(flow->getNumPendingRetxPackets(), 0u)
      << "precondition: nothing is queued for retransmission before the "
         "timer fires";

  auto* const callback = flow->retxTimerCallback_.get();
  retxTimer.cancelCallback(callback);
  (*callback)();

  EXPECT_EQ(flow->getNumPendingRetxPackets(), 1u)
      << "the fenced send cannot go until its prior is acked, so the RTO must "
         "retransmit the prior rather than wait for new data to be sent";
}

/*
 * End-to-end test for the receiver-side BRNR cancel trigger.
 *
 * Verifies the full flow:
 * 1. Sender sends a data packet for which the receiver has no receive WQE.
 * 2. The failed receive lookup records an RNR-Cancel request, and receive
 *    processing records BRNR for the refused frame.
 * 3. A receive WQE is posted to the receiver.
 * 4. Adopting that receive clears the request and fires the RNR cancel
 *    callback into the Qp.
 * 5. Qp::getNextPacketToSend() produces an RnrCancelPacket with correct
 *    header fields (tag, postedCsn, flowsAvailable, opcode).
 */
TEST_F(QpTest, BrnrCancelTriggerEndToEnd) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  qp.startProcessingWqes();

  auto receiversFlow = qp.getRxFlowById(0);
  auto& rxState = receiversFlow->getRxState();
  EXPECT_EQ(rxState.rcvNxt, 0);

  // -- STEP 1: Set up a separate sending QP and send a data packet ----------
  Qp sendingQp{kSecondQpNum, retxTimer, arbiter, mrs, &cq, ibvQp};
  sendingQp.setMtu(mtu);
  createAlwaysSendingMockCongestionController(sendingQp);
  sendingQp.addPortPair(
      *qp.getRemoteAddrs().begin(), *qp.getLocalAddrs().begin());

  LimitedTestSender sendingMockSender(arbiter, mockUDPSocketMaker);
  sendingQp.setupFlows(1, 0, &sendingMockSender);

  auto payload = std::make_unique<char[]>(mtu);
  postWqeHelper(
      &sendingQp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .address = payload.get()});

  auto egressPacket = sendingQp.getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  auto* dataPacket = dynamic_cast<DataPacket*>(egressPacket->packet.get());
  ASSERT_NE(dataPacket, nullptr);

  // -- STEP 2: Process at receiver — should BRNR (no receive WQE posted) ----
  auto maybeAck = receiversFlow->processDataPacket(dataPacket, kEcnECT0);
  ASSERT_TRUE(maybeAck.has_value()) << "Expected a BRNR ACK";
  EXPECT_EQ(maybeAck.value().getAckStatus(), AckStatus::BRNR);

  // -- STEP 3: Post a receive WQE — this should trigger the cancel ----------
  auto rcvPayload = std::make_unique<char[]>(mtu);
  postInSequenceReceiveWqe(qp, rcvPayload.get(), mtu);

  // -- STEP 4: Verify an RnrCancelPacket is produced -------------------------
  auto cancelEgressPacket = waitForPacket(qp, 50000);
  ASSERT_TRUE(cancelEgressPacket) << "RNR cancel packet should be available";
  auto* rnrCancelPacket =
      dynamic_cast<RnrCancelPacket*>(cancelEgressPacket->packet.get());
  ASSERT_NE(rnrCancelPacket, nullptr) << "Packet should be an RnrCancelPacket";

  const auto& rnrCancelHeader = rnrCancelPacket->getRnrCancelHeader();
  EXPECT_EQ(rnrCancelHeader.getTag(), kMetaRoceInitialTag);
  EXPECT_EQ(rnrCancelHeader.getFlowsAvailable(), 1);
  EXPECT_TRUE(rnrCancelPacket->getBaseHeader().getOpcode().isRnrCancel());
}

/*
 * genAck is computed before the receive miss records BRNR. With AckRequest
 * clear, the next ACK reports that BRNR. buildDataPacket() always sets
 * AckRequest, so this test constructs the header directly.
 */
TEST_F(QpTest, AReceiveMissWithoutAckRequestIsReportedByALaterAck) {
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  qp.startProcessingWqes();

  auto flow = qp.getRxFlowById(0);
  auto& rxState = flow->getRxState();
  const auto msn = qp.getTag(kMetaRoceInitialTag)->getCDMSN() + 1;

  // No ACK reaches the initiator here, so its unacknowledged base stays at the
  // refused frame: advancing una would retire the BRNR from the bitmap.
  auto dataHeaderWithoutAckRequest = [&](uint16_t fsn) {
    return DataHeader::DataHeaderBuilder()
        .setFsn(fsn)
        .setUna(0)
        .setPosn(0)
        .setMsn(msn)
        .setCsn(kFixtureRxCsn)
        .setAckRequest(false)
        .build();
  };

  auto missedHeader = dataHeaderWithoutAckRequest(/*fsn=*/0);
  auto missed = buildDataPacketFromTestDataHeader(qp, missedHeader);
  EXPECT_FALSE(flow->processDataPacket(&missed, kEcnECT0).has_value())
      << "an immediate ACK would avoid the cancel-before-BRNR ordering this "
         "test must exercise";
  ASSERT_TRUE(rxState.hasRnrBitsSet())
      << "the miss must latch the BRNR even when no ACK goes out, or the "
         "refusal is never reported to the initiator at all";

  auto rcvPayload = std::make_unique<char[]>(mtu);
  postInSequenceReceiveWqe(qp, rcvPayload.get(), mtu);

  auto cancelEgressPacket = waitForPacket(qp, 50000);
  ASSERT_TRUE(cancelEgressPacket)
      << "the miss records its request whether or not its BRNR has been "
         "reported, so this post must still send the cancel";
  auto* rnrCancelPacket =
      dynamic_cast<RnrCancelPacket*>(cancelEgressPacket->packet.get());
  ASSERT_NE(rnrCancelPacket, nullptr)
      << "another packet type would leave the deferred BRNR able to back off "
         "the initiator despite the available receive";
  EXPECT_EQ(rnrCancelPacket->getRnrCancelHeader().getPostedCsn(), kFixtureRxCsn)
      << "the RNR-Cancel must carry the csn the posted receive serves, or it "
         "lifts the backoff for a message the tag still cannot receive";

  auto retransmitHeader = dataHeaderWithoutAckRequest(/*fsn=*/1);
  auto retransmit = buildDataPacketFromTestDataHeader(qp, retransmitHeader);
  const auto maybeDeferredAck = flow->processDataPacket(&retransmit, kEcnECT0);
  ASSERT_TRUE(maybeDeferredAck.has_value())
      << "without a later ACK, the initiator would never learn that the "
         "earlier frame was refused";
  EXPECT_EQ(maybeDeferredAck->getAckStatus(), AckStatus::BRNR)
      << "this ACK is the first to report the earlier refusal, so dropping the "
         "BRNR here would leave the initiator with no record that the frame "
         "was refused";
}

TEST_F(QpTest, RnrCancelOffersTheReceiveFlowCount) {
  ScopedSuppressedTxTimers suppressTimers;
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  // Asymmetric on purpose: an equal count cannot tell the two apart.
  qp.setupFlows(/*txFlowCount=*/1, /*rxFlowCount=*/2, &mockSender);

  const auto rnrCancelPacket = qp.buildRnrCancelPacket(
      /*flowId=*/0,
      qp.getLocalQpNum(),
      qp.getRemoteQpNum(),
      kMetaRoceInitialTag,
      /*postedCsn=*/0);

  ASSERT_NE(rnrCancelPacket, nullptr);
  EXPECT_EQ(rnrCancelPacket->getRnrCancelHeader().getFlowsAvailable(), 2)
      << "flows available tells the peer how many flows this side can receive "
         "on, so offering the transmit count (1 here) would size the peer's "
         "sending to the wrong direction";
}

TEST_F(QpTest, PostAtomicWqeAndFindNextWqe) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto flow = qp.getTxFlowById(0);
  uint64_t localBuffer = 0;
  uint64_t remoteAddr =
      reinterpret_cast<uint64_t>(receiveBuffer.data()) & ~uint64_t(7);

  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kAtomicFetchAdd,
       .msgSizeBytes = 8,
       .address = &localBuffer,
       .remote_addr = remoteAddr,
       .rkey = mrKey,
       .swapOrAddData = 42});

  auto egressPacket = flow->getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  ASSERT_TRUE(egressPacket->packet);
  EXPECT_EQ(
      egressPacket->packet->getBaseHeader().getOpcode().operationType(),
      BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD);
}

// Test responder-side atomic fetch-and-add.
// The atomic operation is deferred until CDMSN advances to this MSN,
// ensuring all prior operations complete first.
TEST_F(QpTest, AtomicFetchAddResponder) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Set up target memory in the registered MR
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  uint64_t* target = reinterpret_cast<uint64_t*>(
      (reinterpret_cast<uintptr_t>(receiveBuffer.data()) + 7) & ~uintptr_t(7));
  *target = 100;

  // 1. Create a VirtualAtomicExecWqe
  const uint16_t msn = 1;
  initialTag->createVirtualAtomicExecWqeIfNeeded(
      msn,
      true, // isFetchAdd
      reinterpret_cast<uint64_t>(target),
      42, // swapOrAddData
      0); // compareData (unused for fetch-add)
  initialTag->extendCdmsnTrackerIfNeeded(msn);

  // Atomic should NOT have executed yet
  EXPECT_EQ(*target, 100u);

  // 2. Advance CDMSN — this triggers the deferred atomic execution
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      nullptr,
      BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD,
      initialTag->getTagNum(),
      msn,
      0));

  // Verify the target memory was updated (100 + 42 = 142)
  EXPECT_EQ(*target, 142u);

  // Verify a VirtualAtomicAckWqe was created and can be sent
  auto flow = qp.getTxFlowById(0);
  auto egressPacket = flow->getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  auto* atomicAckPacket =
      dynamic_cast<AtomicAckPacket*>(egressPacket->packet.get());
  ASSERT_NE(atomicAckPacket, nullptr);
  EXPECT_EQ(atomicAckPacket->getAtomicAckHeader().getOrigRemoteData(), 100u);
}

// Test responder-side atomic compare-and-swap.
// The atomic operation is deferred until CDMSN advances to this MSN.
TEST_F(QpTest, AtomicCmpSwpResponder) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Set up target memory in the registered MR
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  uint64_t* target = reinterpret_cast<uint64_t*>(
      (reinterpret_cast<uintptr_t>(receiveBuffer.data()) + 7) & ~uintptr_t(7));
  *target = 100;

  // 1. Create a VirtualAtomicExecWqe for CMP_SWP
  const uint16_t msn = 1;
  initialTag->createVirtualAtomicExecWqeIfNeeded(
      msn, false, reinterpret_cast<uint64_t>(target), 200, 100);
  initialTag->extendCdmsnTrackerIfNeeded(msn);

  // Atomic should NOT have executed yet
  EXPECT_EQ(*target, 100u);

  // 2. Advance CDMSN — this triggers the deferred atomic execution
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      nullptr,
      BaseHeader::Opcode::OperationType::ATOMIC_CMP_SWP,
      initialTag->getTagNum(),
      msn,
      0));

  EXPECT_EQ(*target, 200u);

  // Verify a VirtualAtomicAckWqe was created with original value
  auto flow = qp.getTxFlowById(0);
  auto egressPacket = flow->getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  auto* atomicAckPacket =
      dynamic_cast<AtomicAckPacket*>(egressPacket->packet.get());
  ASSERT_NE(atomicAckPacket, nullptr);
  EXPECT_EQ(atomicAckPacket->getAtomicAckHeader().getOrigRemoteData(), 100u);
}

// Test responder-side atomic compare-and-swap when compare fails.
// The target value should remain unchanged.
TEST_F(QpTest, AtomicCmpSwpResponderCompareFails) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Set up target memory — value does NOT match compareData
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  uint64_t* target = reinterpret_cast<uint64_t*>(
      (reinterpret_cast<uintptr_t>(receiveBuffer.data()) + 7) & ~uintptr_t(7));
  *target = 999;

  const uint16_t msn = 1;
  initialTag->createVirtualAtomicExecWqeIfNeeded(
      msn, false, reinterpret_cast<uint64_t>(target), 200, 100);
  initialTag->extendCdmsnTrackerIfNeeded(msn);

  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      nullptr,
      BaseHeader::Opcode::OperationType::ATOMIC_CMP_SWP,
      initialTag->getTagNum(),
      msn,
      0));

  // Compare failed (999 != 100), target should remain unchanged
  EXPECT_EQ(*target, 999u);

  // Ack should carry the actual value (999)
  auto flow = qp.getTxFlowById(0);
  auto egressPacket = flow->getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  auto* atomicAckPacket =
      dynamic_cast<AtomicAckPacket*>(egressPacket->packet.get());
  ASSERT_NE(atomicAckPacket, nullptr);
  EXPECT_EQ(atomicAckPacket->getAtomicAckHeader().getOrigRemoteData(), 999u);
}

// Test that atomic execution is deferred until all prior operations complete.
// A write at MSN 1 must complete before the atomic at MSN 2 executes.
TEST_F(QpTest, AtomicDeferredAfterWrite) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Set up target memory for the atomic operation
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  uint64_t* target = reinterpret_cast<uint64_t*>(
      (reinterpret_cast<uintptr_t>(receiveBuffer.data()) + 7) & ~uintptr_t(7));
  *target = 100;

  // MSN 1: a write operation (single-posn, messageLength = mtu)
  const uint16_t writeMsn = 1;
  const auto rwHeader = ReadWriteHeader::ReadWriteHeaderBuilder()
                            .setMessageLength(mtu)
                            .setMessageAddr((unsigned long)receiveBuffer.data())
                            .setKey(mrKey)
                            .build();
  int addressError = 0;
  initialTag->createVirtualRWqeIfNeeded(
      writeMsn, rwHeader, &addressError, mrs, qp.getMtu());
  EXPECT_EQ(addressError, 0);
  initialTag->extendCdmsnTrackerIfNeeded(writeMsn);

  // MSN 2: an atomic fetch-add operation
  const uint16_t atomicMsn = 2;
  initialTag->createVirtualAtomicExecWqeIfNeeded(
      atomicMsn, true, reinterpret_cast<uint64_t>(target), 42, 0);
  initialTag->extendCdmsnTrackerIfNeeded(atomicMsn);

  // Complete the atomic (MSN 2) first — out of order
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      nullptr,
      BaseHeader::Opcode::OperationType::ATOMIC_FETCH_ADD,
      initialTag->getTagNum(),
      atomicMsn,
      0));

  // Atomic should NOT have executed — MSN 1 (write) is still pending
  EXPECT_EQ(*target, 100u);

  // Now complete the write (MSN 1) — CDMSN catches up through both
  const auto writeOpType = BaseHeader::Opcode::OperationType::WRITE;
  auto* writeWqe = initialTag->getReceiveWqe(writeOpType, writeMsn);
  ASSERT_NE(writeWqe, nullptr);
  EXPECT_TRUE(qp.checkForRxMessageCompletion(
      writeWqe, writeOpType, initialTag->getTagNum(), writeMsn, 0));

  // Now the atomic should have executed (100 + 42 = 142)
  EXPECT_EQ(*target, 142u);

  // Verify the atomic ack was created
  auto flow = qp.getTxFlowById(0);
  auto egressPacket = flow->getNextPacketToSend();
  ASSERT_TRUE(egressPacket);
  auto* atomicAckPacket =
      dynamic_cast<AtomicAckPacket*>(egressPacket->packet.get());
  ASSERT_NE(atomicAckPacket, nullptr);
  EXPECT_EQ(atomicAckPacket->getAtomicAckHeader().getOrigRemoteData(), 100u);
}

// Test requester-side ATOMIC_ACK_RESP handling
TEST_F(QpTest, AtomicAckRespRequester) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto rxFlow = qp.getRxFlowById(0);

  // Post an AtomicWqe (the requester's outgoing atomic request)
  uint64_t localBuffer = 0;
  uint64_t remoteAddr =
      reinterpret_cast<uint64_t>(receiveBuffer.data()) & ~uint64_t(7);
  auto* atomicWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kAtomicFetchAdd,
       .msgSizeBytes = 8,
       .address = &localBuffer,
       .remote_addr = remoteAddr,
       .rkey = mrKey,
       .swapOrAddData = 42});
  const auto msn = atomicWqe->getMsn().value();

  // Build an incoming ATOMIC_ACK_RESP packet (the response from the responder)
  BaseHeader::Opcode opcode(BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP);
  BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                              .setOpcode(std::move(opcode))
                              .setVersion(0)
                              .setFlowId(0)
                              .setDstQp(qp.getLocalQpNum())
                              .setSrcQp(qp.getRemoteQpNum())
                              .build();
  DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                              .setFsn(0)
                              .setUna(0)
                              .setTag(kMetaRoceInitialTag)
                              .setAckRequest(true)
                              .setPosn(0)
                              .setMsn(msn)
                              .setCsn(0)
                              .build();
  AtomicAckHeader atomicAckHeader =
      AtomicAckHeader::AtomicAckHeaderBuilder()
          .setOrigRemoteData(0x12345678ABCDEF00ULL)
          .build();
  AtomicAckPacket atomicAckPacket(baseHeader, dataHeader, atomicAckHeader);

  auto maybeAck = rxFlow->processDataPacket(&atomicAckPacket, kEcnECT0);

  // Verify the original remote data was written to the local buffer
  EXPECT_EQ(localBuffer, 0x12345678ABCDEF00ULL);

  EXPECT_EQ(cq.getNumCompletedWqes(), 1);
}

// A local-protection error on the atomic-ack writeback must flush the WRs
// posted behind the atomic, not just error the atomic: RC completes in order,
// so a trailing WR would otherwise stall the completion queue forever.
TEST_F(QpTest, AtomicAckRespLocalProtErrorFlushesTrailingWqe) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto* const rxFlow = qp.getRxFlowById(0);

  // Atomic whose local key is unregistered: the responder executes it remotely,
  // but the ack writeback fails with a local protection error.
  uint64_t localBuffer = 0;
  const uint64_t remoteAddr =
      reinterpret_cast<uint64_t>(receiveBuffer.data()) & ~uint64_t(7);
  auto* const atomicWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kAtomicFetchAdd,
       .msgSizeBytes = 8,
       .address = &localBuffer,
       .remote_addr = remoteAddr,
       .rkey = mrKey,
       .swapOrAddData = 42,
       .lkeyOverride = 0xbadbad});
  const auto atomicMsn = atomicWqe->getMsn().value();

  // A SEND posted behind the atomic; it must be flushed, not stalled.
  char sendBuffer[64] = {};
  postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = sizeof(sendBuffer),
       .address = sendBuffer});

  BaseHeader::Opcode opcode(BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setOpcode(std::move(opcode))
                                    .setVersion(0)
                                    .setFlowId(0)
                                    .setDstQp(qp.getLocalQpNum())
                                    .setSrcQp(qp.getRemoteQpNum())
                                    .build();
  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(0)
                                    .setUna(0)
                                    .setTag(kMetaRoceInitialTag)
                                    .setAckRequest(true)
                                    .setPosn(0)
                                    .setMsn(atomicMsn)
                                    .setCsn(0)
                                    .build();
  const AtomicAckHeader atomicAckHeader =
      AtomicAckHeader::AtomicAckHeaderBuilder()
          .setOrigRemoteData(0x12345678ABCDEF00ULL)
          .build();
  const AtomicAckPacket atomicAckPacket(
      baseHeader, dataHeader, atomicAckHeader);
  rxFlow->processDataPacket(&atomicAckPacket, kEcnECT0);
  // Marking the tag's sends failing is the sender's to do, at its next visit.
  std::ignore = qp.getNextPacketToSend();

  ASSERT_EQ(cq.getNumCompletedWqes(), 2)
      << "the failed atomic and the trailing WR behind it must both complete";
  const auto atomicCqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(atomicCqe.status, kLocalProtErr)
      << "the atomic whose writeback failed completes with a local protection error";
  const auto trailingCqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(trailingCqe.status, kWrFlushErr)
      << "the WR posted behind the failed atomic must be flushed, not stalled";

  EXPECT_EQ(localBuffer, 0u)
      << "the atomic result must not be written into an unregistered buffer";
}

// A null local buffer is validated at the ack writeback like any other bad
// atomic buffer: it completes with a local protection error rather than
// tripping the non-null address assertion in addressForPosnMtu.
TEST_F(QpTest, AtomicAckRespNullLocalAddrCompletesLocalProtError) {
  createAlwaysSendingMockCongestionController(qp);

  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  auto* const rxFlow = qp.getRxFlowById(0);

  const uint64_t remoteAddr =
      reinterpret_cast<uint64_t>(receiveBuffer.data()) & ~uint64_t(7);
  // No local buffer (null address) and an unregistered key.
  auto* const nullBufferAtomic = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kAtomicFetchAdd,
       .msgSizeBytes = 8,
       .remote_addr = remoteAddr,
       .rkey = mrKey,
       .swapOrAddData = 42,
       .lkeyOverride = 0xbadbad});
  const auto atomicMsn = nullBufferAtomic->getMsn().value();

  BaseHeader::Opcode opcode(BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP);
  const BaseHeader baseHeader = BaseHeader::BaseHeaderBuilder()
                                    .setOpcode(std::move(opcode))
                                    .setVersion(0)
                                    .setFlowId(0)
                                    .setDstQp(qp.getLocalQpNum())
                                    .setSrcQp(qp.getRemoteQpNum())
                                    .build();
  const DataHeader dataHeader = DataHeader::DataHeaderBuilder()
                                    .setFsn(0)
                                    .setUna(0)
                                    .setTag(kMetaRoceInitialTag)
                                    .setAckRequest(true)
                                    .setPosn(0)
                                    .setMsn(atomicMsn)
                                    .setCsn(0)
                                    .build();
  const AtomicAckHeader atomicAckHeader =
      AtomicAckHeader::AtomicAckHeaderBuilder()
          .setOrigRemoteData(0x12345678ABCDEF00ULL)
          .build();
  const AtomicAckPacket atomicAckPacket(
      baseHeader, dataHeader, atomicAckHeader);
  rxFlow->processDataPacket(&atomicAckPacket, kEcnECT0);
  // Marking the tag's sends failing is the sender's to do, at its next visit.
  std::ignore = qp.getNextPacketToSend();

  ASSERT_EQ(cq.getNumCompletedWqes(), 1)
      << "the atomic with a null local buffer must complete, not abort";
  const auto cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.status, kLocalProtErr)
      << "a null/unregistered atomic local buffer completes with a local "
         "protection error";
}

// Helper that sets up the common state for SACK retransmission mode tests.
struct SackRetxTestFixture {
  TxFlow* flow;
  TxFlow::TxState* txState;
  MockCongestionController* rawCC;
  std::unique_ptr<SendWqe> wqe;
  std::unique_ptr<char[]> payload;
  LimitedTestSender mockSender;
  AckHeader ackHeader;

  SackRetxTestFixture(
      QpArbiter& arbiter,
      Qp& qp,
      uint16_t mtu,
      std::shared_ptr<Tag>& initialTag,
      SackRetransmissionConfig sackRetxConfig = {})
      : mockSender(arbiter, mockUDPSocketMaker) {
    auto congestionController = std::make_unique<MockCongestionController>();
    rawCC = congestionController.get();
    qp.setCongestionController(std::move(congestionController));

    qp.setupFlows(1, 1, &mockSender, sackRetxConfig);
    flow = qp.getTxFlowById(0);

    const uint32_t length = 20 * mtu;
    payload = std::make_unique<char[]>(length);
    wqe = std::make_unique<SendWqe>(
        23 /*workReqId*/, oneSge(payload.get(), length), mtu);
    wqe->setMsn(initialTag->getAndIncrementNextMsn());
    wqe->setCsn(initialTag->getAndIncrementNextCsn());

    txState = &flow->getTxState();
  }

  // Standard tracker: FSN 10-15 (12 pre-ACKed), ACK with cfsn=12, maxRcvdFsn=15
  [[nodiscard]] bool setupStandardTrackerAndAck(
      uint32_t mtu,
      std::optional<uint16_t> mockFwnd = std::nullopt) {
    txState->sndUna = 10;
    *(txState->sndCur.wlock()) = 10;
    txState->maxAckdFsn = 12;
    for (uint32_t posn = 0; posn < 6; ++posn) {
      const uint32_t fsn = txState->sndUna + posn;
      if (wqe->allocateNextPosn() != posn) {
        ADD_FAILURE()
            << "precondition: posns are allocated in order, so the tracker "
               "entry below matches the posn its frame carries";
        return false;
      }
      txState->tracker.emplace_back(
          PacketId(wqe.get(), 0, posn, mtu),
          fsn == 12 ? Flow::FsnState::ACKED : Flow::FsnState::UNACKED);
    }
    txState->sndNxt = 16;

    if (mockFwnd.has_value()) {
      ON_CALL(*rawCC, getFwnd(0)).WillByDefault(Return(mockFwnd.value()));
    }

    BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
    fsnBitVector.setBit(1); // FSN 14 SACKed
    fsnBitVector.setBit(2); // FSN 15 SACKed
    ackHeader = AckHeader::AckHeaderBuilder()
                    .setCumulativeFsn(12)
                    .setMaxRcvdFsn(15)
                    .setAckStatus(AckStatus::ACK_OUT_OF_ORDER)
                    .setFsnBitVector(fsnBitVector)
                    .setTimestamp(0)
                    .build();
    return true;
  }

  void expectNoLoss() {
    EXPECT_CALL(*rawCC, onPacketLoss(_, _)).Times(0);
  }

  void expectLoss(uint16_t numLost) {
    EXPECT_CALL(
        *rawCC,
        onPacketLoss(
            0,
            CongestionController::LossEvent{
                .numLostPackets = numLost,
                .lossType =
                    CongestionController::LossEvent::LossType::OutOfOrder}))
        .Times(1);
  }

  void expectAck(uint16_t numAcked = 4) {
    EXPECT_CALL(
        *rawCC,
        onPacketAck(
            0,
            Field(
                &CongestionController::AckEvent::numAckedPackets,
                Eq(numAcked))))
        .Times(1);
  }
};

class SackRetransmissionTest : public QpTest {};

TEST_F(SackRetransmissionTest, ImmediateMode) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::IMMEDIATE});
  ASSERT_TRUE(f.setupStandardTrackerAndAck(mtu));

  f.expectLoss(1);
  f.expectAck();
  f.flow->processAckPacket(
      f.ackHeader, simulatedProcessedAtNs(f.ackHeader.getTimestamp()));

  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 1)
      << "IMMEDIATE mode should queue retx for the SACK hole";
  EXPECT_EQ(*(f.txState->sndCur.rlock()), 15)
      << "sndCur should advance past retransmitted packets";
}

TEST_F(SackRetransmissionTest, ARepeatedSackForTheSameHoleSendsNoSecondCopy) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::IMMEDIATE});
  ASSERT_TRUE(f.setupStandardTrackerAndAck(mtu))
      << "precondition: the tracker holds the hole the sack reports";
  f.expectLoss(1);
  f.expectAck();

  constexpr uint16_t kHoleFsn = 13;
  f.flow->processAckPacket(
      f.ackHeader, simulatedProcessedAtNs(f.ackHeader.getTimestamp()));
  auto firstCopy = f.flow->getNextPacketToSend();
  ASSERT_TRUE(firstCopy)
      << "precondition: the first sack sends the hole's frame again";
  auto* firstCopyData = dynamic_cast<DataPacket*>(firstCopy->packet.get());
  ASSERT_NE(firstCopyData, nullptr);
  ASSERT_EQ(firstCopyData->getDataHeader().getFsn(), kHoleFsn)
      << "precondition: the frame sent again is the one the sack reports "
         "missing";

  f.flow->processAckPacket(
      f.ackHeader, simulatedProcessedAtNs(f.ackHeader.getTimestamp()));

  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 0u)
      << "the sender should not queue the hole's frame again: the cursor is "
         "already past it, and only an RTO moves it back";
  auto next = f.flow->getNextPacketToSend();
  auto* nextData =
      next == nullptr ? nullptr : dynamic_cast<DataPacket*>(next->packet.get());
  EXPECT_TRUE(
      nextData == nullptr || nextData->getDataHeader().getFsn() != kHoleFsn)
      << "the sender should send no second copy of the hole's frame";
}

TEST_F(SackRetransmissionTest, NeverMode) {
  SackRetxTestFixture f(
      arbiter, qp, mtu, initialTag, {.mode = SackRetransmissionMode::NEVER});
  ASSERT_TRUE(f.setupStandardTrackerAndAck(mtu));

  f.expectNoLoss();
  f.expectAck();
  f.flow->processAckPacket(
      f.ackHeader, simulatedProcessedAtNs(f.ackHeader.getTimestamp()));

  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 0)
      << "NEVER mode should not queue retx packets";
  EXPECT_EQ(*(f.txState->sndCur.rlock()), 13)
      << "sndCur should not advance when retransmission is suppressed";
}

TEST_F(SackRetransmissionTest, CountBasedBelowThreshold) {
  // fwnd=10: countSacked(2) > 10*1.0 is false → no retransmit
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::COUNT_BASED_PWND, .fraction = 1.0});
  ASSERT_TRUE(f.setupStandardTrackerAndAck(mtu, /*mockFwnd=*/10));

  f.expectNoLoss();
  f.expectAck();
  f.flow->processAckPacket(
      f.ackHeader, simulatedProcessedAtNs(f.ackHeader.getTimestamp()));

  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 0)
      << "COUNT_BASED below threshold should not queue retx packets";
}

TEST_F(SackRetransmissionTest, CountBasedAboveThreshold) {
  // fwnd=1: countSacked(2) > 1*1.0 is true → retransmit
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::COUNT_BASED_PWND, .fraction = 1.0});
  ASSERT_TRUE(f.setupStandardTrackerAndAck(mtu, /*mockFwnd=*/1));

  f.expectLoss(1);
  f.expectAck();
  f.flow->processAckPacket(
      f.ackHeader, simulatedProcessedAtNs(f.ackHeader.getTimestamp()));

  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 1)
      << "COUNT_BASED above threshold should retransmit packets outside "
         "the reordering tolerance window";
  EXPECT_EQ(*(f.txState->sndCur.rlock()), 16)
      << "sndCur should advance to count-based cursorLimit "
         "(1 ACKED entry from end, +1 for exclusive boundary)";
}

TEST_F(SackRetransmissionTest, DistanceBasedBelowThreshold) {
  // fwnd=10: distance(2) > 10*1.0 is false → no retransmit
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::DISTANCE_BASED_PWND, .fraction = 1.0});
  ASSERT_TRUE(f.setupStandardTrackerAndAck(mtu, /*mockFwnd=*/10));

  f.expectNoLoss();
  f.expectAck();
  f.flow->processAckPacket(
      f.ackHeader, simulatedProcessedAtNs(f.ackHeader.getTimestamp()));

  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 0)
      << "DISTANCE_BASED below threshold should not queue retx packets";
}

TEST_F(SackRetransmissionTest, DistanceBasedAboveThreshold) {
  // fwnd=1: distance(2) > 1*1.0 is true → retransmit
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::DISTANCE_BASED_PWND, .fraction = 1.0});
  ASSERT_TRUE(f.setupStandardTrackerAndAck(mtu, /*mockFwnd=*/1));

  f.expectLoss(1);
  f.expectAck();
  f.flow->processAckPacket(
      f.ackHeader, simulatedProcessedAtNs(f.ackHeader.getTimestamp()));

  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 1)
      << "DISTANCE_BASED above threshold should retransmit packets outside "
         "the reordering tolerance window";
  EXPECT_EQ(*(f.txState->sndCur.rlock()), 15)
      << "sndCur should advance to cursorLimit (maxAckdFsn - window + 1)";
}

TEST_F(SackRetransmissionTest, FractionParameter) {
  // fwnd=3, fraction=0.5: countSacked(2) > 3*0.5=1.5 is true → retransmit
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::COUNT_BASED_PWND, .fraction = 0.5});
  ASSERT_TRUE(f.setupStandardTrackerAndAck(mtu, /*mockFwnd=*/3));

  f.expectLoss(1);
  f.expectAck();
  f.flow->processAckPacket(
      f.ackHeader, simulatedProcessedAtNs(f.ackHeader.getTimestamp()));

  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 1)
      << "COUNT_BASED with fraction=0.5 and fwnd=3 should retransmit "
         "(countSacked=2 > 1.5)";
}

// Direct test of countSackedPackets with FSN wrap-around.
// Calls the private method directly via FRIEND_TEST to verify
// the uint16_t offset arithmetic without needing full ACK processing.
TEST_F(QpTest, CountSackedPacketsDirectWrap) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::COUNT_BASED_PWND, .fraction = 1.0});

  constexpr uint16_t sndUna = 65530;
  f.txState->sndUna = sndUna;

  // 12 tracker entries spanning the wrap: FSN 65530..65535, 0..5
  // Mark FSN 4 and 5 (indices 10, 11) as ACKED.
  //
  // Index:  0      1      2      3      4      5     6  7  8  9  10  11
  // FSN:  65530  65531  65532  65533  65534  65535   0  1  2  3   4   5
  // State: UNACK  UNACK  UNACK  UNACK  UNACK  UNACK  UN UN UN UN ACK ACK

  constexpr uint16_t numEntries = 12;
  for (uint16_t i = 0; i < numEntries; i++) {
    const uint32_t posn = f.wqe->allocateNextPosn();
    uint16_t fsn = static_cast<uint16_t>(sndUna + i);
    auto state = (fsn == 4 || fsn == 5) ? Flow::FsnState::ACKED
                                        : Flow::FsnState::UNACKED;
    f.txState->tracker.emplace_back(PacketId(f.wqe.get(), 0, posn, mtu), state);
  }

  // sndCur=3: countSackedPackets should count from FSN 4 onward.
  // startOffset = uint16_t(3 + 1 - 65530) = uint16_t(-65526) = 10
  EXPECT_EQ(f.flow->countSackedPackets(3), 2)
      << "Should find 2 SACKed packets (FSN 4, 5) after sndCur=3 with wrap";

  // No wrap case: sndCur=0, should count from FSN 1 onward (indices 7..11).
  // FSN 4 and 5 are SACKed → 2
  EXPECT_EQ(f.flow->countSackedPackets(0), 2)
      << "Should find 2 SACKed packets (FSN 4, 5) after sndCur=0 without wrap";
}

// Multi-packet SACK recovery tests.
// These use a 48-FSN tracker to verify retransmission behavior with
// realistic loss patterns, complementing the existing 6-FSN mode-gating
// tests above.

// Helper to set up a 48-FSN tracker with all entries UNACKED.
void setupLargeTracker(
    SackRetxTestFixture& f,
    uint32_t mtu,
    std::shared_ptr<Tag>& initialTag,
    uint16_t numFsns = 48) {
  const uint32_t length = (numFsns + 1) * mtu;
  f.payload = std::make_unique<char[]>(length);
  f.wqe = std::make_unique<SendWqe>(23, oneSge(f.payload.get(), length), mtu);
  f.wqe->setMsn(initialTag->getAndIncrementNextMsn());
  f.wqe->setCsn(initialTag->getAndIncrementNextCsn());

  f.txState->sndUna = 0;
  *(f.txState->sndCur.wlock()) = 0;
  f.txState->maxAckdFsn = UINT16_MAX;
  for (uint16_t fsn = 0; fsn < numFsns; ++fsn) {
    const uint32_t posn = f.wqe->allocateNextPosn();
    f.txState->tracker.emplace_back(
        PacketId(f.wqe.get(), 0, posn, mtu), Flow::FsnState::UNACKED);
  }
  f.txState->sndNxt = numFsns;
}

// Build an AckHeader reflecting that a specific set of FSNs have been received.
// Does not modify the tracker — processAckPacket handles that internally.
AckHeader buildSackAck(uint16_t sndUna, const std::set<int>& allReceivedFsns) {
  CHECK(!allReceivedFsns.empty()) << "must have at least one received FSN";
  // Compute cumulative FSN: advance from sndUna through contiguous received
  uint16_t cumFsn = sndUna;
  while (allReceivedFsns.count(cumFsn)) {
    cumFsn++;
  }
  // cumFsn is now the first unreceived FSN; cumlativeFsn = cumFsn - 1.
  // If nothing from sndUna was received, cumFsn == sndUna, and
  // cumlativeFsn should be sndUna - 1 (the "nothing new" value).
  cumFsn = static_cast<uint16_t>(cumFsn - 1);
  const uint16_t maxFsn = *allReceivedFsns.rbegin();
  BitVector<AckHeader::kAckTrackerBits> fsnBitVector;
  for (int fsn : allReceivedFsns) {
    uint16_t bitIdx = static_cast<uint16_t>(fsn) - cumFsn - 1;
    if (bitIdx < AckHeader::kAckTrackerBits) {
      fsnBitVector.setBit(bitIdx);
    }
  }
  auto status =
      (maxFsn != cumFsn) ? AckStatus::ACK_OUT_OF_ORDER : AckStatus::ACK;
  return AckHeader::AckHeaderBuilder()
      .setCumulativeFsn(cumFsn)
      .setMaxRcvdFsn(maxFsn)
      .setAckStatus(status)
      .setFsnBitVector(fsnBitVector)
      .setTimestamp(0)
      .build();
}

class SackRecoveryTest : public QpTest {};

TEST_F(SackRecoveryTest, ImmediateMode10Pct) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::IMMEDIATE});
  setupLargeTracker(f, mtu, initialTag);

  ON_CALL(*f.rawCC, getAvailableWindowSize(0)).WillByDefault(Return(1000));
  ON_CALL(*f.rawCC, getFwnd(0)).WillByDefault(Return(48));

  const std::vector<int> arrivedFsns = {4, 11, 19, 31, 42};
  std::set<int> allReceived;

  EXPECT_CALL(*f.rawCC, onPacketAck(_, _))
      .Times(static_cast<int>(arrivedFsns.size()));
  EXPECT_CALL(*f.rawCC, onPacketLoss(_, _)).Times(AtLeast(1));

  size_t totalRetx = 0;
  for (int fsn : arrivedFsns) {
    allReceived.insert(fsn);
    auto ack = buildSackAck(f.txState->sndUna, allReceived);
    f.flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));

    size_t pending = f.flow->getNumPendingRetxPackets();
    totalRetx += pending;
    // The per-SACK retransmission burst count is not limited here;
    // it could be made configurable.
    f.flow->purgeRetxPktsToSend();
  }

  EXPECT_GT(totalRetx, 0u) << "IMMEDIATE mode should retransmit for SACK holes";
  EXPECT_GT(totalRetx, arrivedFsns.size())
      << "total retransmissions should exceed the number of ACKs since "
         "each ACK can trigger multiple retransmissions (no burst limit)";
}

TEST_F(SackRecoveryTest, ImmediateMode50Pct) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::IMMEDIATE});
  setupLargeTracker(f, mtu, initialTag);

  ON_CALL(*f.rawCC, getAvailableWindowSize(0)).WillByDefault(Return(1000));
  ON_CALL(*f.rawCC, getFwnd(0)).WillByDefault(Return(48));

  const std::vector<int> arrivedFsns = {0,  2,  5,  7,  9,  11, 13, 15,
                                        17, 19, 21, 23, 25, 27, 29, 31,
                                        33, 35, 37, 39, 41, 43, 45, 47};
  std::set<int> allReceived;

  EXPECT_CALL(*f.rawCC, onPacketAck(_, _))
      .Times(static_cast<int>(arrivedFsns.size()));
  EXPECT_CALL(*f.rawCC, onPacketLoss(_, _)).Times(AtLeast(1));

  size_t totalRetx = 0;
  for (int fsn : arrivedFsns) {
    allReceived.insert(fsn);
    auto ack = buildSackAck(f.txState->sndUna, allReceived);
    f.flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));

    totalRetx += f.flow->getNumPendingRetxPackets();
    f.flow->purgeRetxPktsToSend();
  }

  EXPECT_EQ(totalRetx, 24u)
      << "24 missing FSNs should each be retransmitted in IMMEDIATE mode";
}

TEST_F(SackRecoveryTest, BatchDelivery) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::IMMEDIATE});
  setupLargeTracker(f, mtu, initialTag);

  ON_CALL(*f.rawCC, getAvailableWindowSize(0)).WillByDefault(Return(1000));
  ON_CALL(*f.rawCC, getFwnd(0)).WillByDefault(Return(48));

  std::set<int> allReceived = {3, 4, 5, 6, 7};
  auto ack = buildSackAck(f.txState->sndUna, allReceived);

  // Precondition: SACK bit vector correctly encodes out-of-order FSNs
  // when cumFsn wraps (sndUna=0, nothing contiguously received).
  ASSERT_EQ(ack.getCumulativeFsn(), static_cast<uint16_t>(UINT16_MAX))
      << "precondition: cumFsn should wrap when no FSN from sndUna is received";
  ASSERT_EQ(ack.getMaxRcvdFsn(), 7u)
      << "precondition: maxRcvdFsn should be the highest delivered FSN";
  for (int fsn : {3, 4, 5, 6, 7}) {
    uint16_t bitIdx = static_cast<uint16_t>(fsn) - ack.getCumulativeFsn() - 1;
    ASSERT_TRUE(ack.getFsnBitVector().getBit(bitIdx))
        << "precondition: SACK bit for delivered FSN " << fsn << " (bitIdx "
        << bitIdx << ") must be set";
  }
  for (int fsn : {0, 1, 2}) {
    uint16_t bitIdx = static_cast<uint16_t>(fsn) - ack.getCumulativeFsn() - 1;
    ASSERT_FALSE(ack.getFsnBitVector().getBit(bitIdx))
        << "precondition: SACK bit for missing FSN " << fsn
        << " must not be set";
  }

  // One processAckPacket call delivers 5 FSNs in a single SACK.
  EXPECT_CALL(*f.rawCC, onPacketAck(_, _)).Times(1);
  // 3 holes (FSNs 0-2) detected as losses.
  EXPECT_CALL(*f.rawCC, onPacketLoss(_, _)).Times(AtLeast(1));
  f.flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));

  // Verify: processAckPacket correctly updates tracker from SACK bits
  for (int fsn : {3, 4, 5, 6, 7}) {
    EXPECT_TRUE(f.txState->tracker[fsn].isAcked())
        << "processAckPacket should mark SACKed FSN " << fsn << " as ACKED";
  }
  for (int fsn : {0, 1, 2}) {
    EXPECT_FALSE(f.txState->tracker[fsn].isAcked())
        << "FSN " << fsn << " should remain UNACKED (not in SACK bitmap)";
  }

  // Verify: retransmission queued only for the 3 holes
  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 3u)
      << "should retransmit only the 3 holes (FSNs 0, 1, 2)";
  EXPECT_EQ(*(f.txState->sndCur.rlock()), 7u)
      << "sndCur should advance to maxAckdFsn after retransmitting holes";
}

TEST_F(SackRecoveryTest, NeverMode) {
  SackRetxTestFixture f(
      arbiter, qp, mtu, initialTag, {.mode = SackRetransmissionMode::NEVER});
  setupLargeTracker(f, mtu, initialTag);

  ON_CALL(*f.rawCC, getAvailableWindowSize(0)).WillByDefault(Return(1000));
  ON_CALL(*f.rawCC, getFwnd(0)).WillByDefault(Return(48));

  const std::vector<int> arrivedFsns = {4, 11, 19, 31, 42};
  std::set<int> allReceived;

  EXPECT_CALL(*f.rawCC, onPacketAck(_, _))
      .Times(static_cast<int>(arrivedFsns.size()));
  // NEVER mode does not detect SACK losses; only RTO triggers onPacketLoss.
  EXPECT_CALL(*f.rawCC, onPacketLoss(_, _)).Times(0);

  for (int fsn : arrivedFsns) {
    allReceived.insert(fsn);
    auto ack = buildSackAck(f.txState->sndUna, allReceived);
    f.flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));

    EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 0u)
        << "NEVER mode should not queue retransmissions regardless of holes";
  }
}

TEST_F(SackRecoveryTest, WindowDelay) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::DISTANCE_BASED_PWND, .fraction = 1.0});
  setupLargeTracker(f, mtu, initialTag);

  ON_CALL(*f.rawCC, getAvailableWindowSize(0)).WillByDefault(Return(1000));
  ON_CALL(*f.rawCC, getFwnd(0)).WillByDefault(Return(10));

  const std::vector<int> arrivedFsns = {2, 7, 12, 15, 20, 25, 30, 35, 40, 45};
  std::set<int> allReceived;

  // One onPacketAck call per processAckPacket invocation.
  EXPECT_CALL(*f.rawCC, onPacketAck(_, _))
      .Times(static_cast<int>(arrivedFsns.size()));
  EXPECT_CALL(*f.rawCC, onPacketLoss(_, _)).Times(AnyNumber());

  int retxCount = 0;
  int noRetxCount = 0;
  for (int fsn : arrivedFsns) {
    allReceived.insert(fsn);
    auto ack = buildSackAck(f.txState->sndUna, allReceived);
    f.flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));

    size_t pending = f.flow->getNumPendingRetxPackets();
    if (pending > 0) {
      ++retxCount;
      f.flow->purgeRetxPktsToSend();
    } else {
      ++noRetxCount;
    }
  }

  EXPECT_GE(noRetxCount, 1)
      << "first SACK should be within reordering tolerance (no retransmission)";
  EXPECT_GE(retxCount, 1)
      << "distance should eventually exceed threshold and trigger retransmission";
}

// Verify that SACK retransmission does not retransmit packets beyond
// maxAckdFsn.  Packets between maxAckdFsn+1 and sndNxt are still in
// flight and should not be retransmitted.
// Uses IMMEDIATE mode (which sets cursorLimit = maxAckdFsn) to get a
// clean measurement.
TEST_F(SackRetransmissionTest, CursorLimitedToMaxAckdFsn) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::IMMEDIATE});
  setupLargeTracker(f, mtu, initialTag);

  ON_CALL(*f.rawCC, getAvailableWindowSize(0)).WillByDefault(Return(1000));
  ON_CALL(*f.rawCC, getFwnd(0)).WillByDefault(Return(48));

  // Deliver FSNs 4, 11, 19, 31, 42 out of order.
  // After each ACK, count retransmissions and verify sndCur does not
  // advance beyond maxAckdFsn.
  const std::vector<int> arrivedFsns = {4, 11, 19, 31, 42};
  std::set<int> allReceived;

  EXPECT_CALL(*f.rawCC, onPacketAck(_, _))
      .Times(static_cast<int>(arrivedFsns.size()));
  EXPECT_CALL(*f.rawCC, onPacketLoss(_, _)).Times(AtLeast(1));

  size_t totalRetx = 0;
  for (int fsn : arrivedFsns) {
    allReceived.insert(fsn);
    auto ack = buildSackAck(f.txState->sndUna, allReceived);
    f.flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));

    size_t pending = f.flow->getNumPendingRetxPackets();
    totalRetx += pending;

    uint16_t cursorAfter = *(f.txState->sndCur.rlock());
    EXPECT_LE(cursorAfter, static_cast<uint16_t>(fsn + 1))
        << "after ACK for FSN " << fsn << ", sndCur=" << cursorAfter
        << " should not advance beyond maxAckdFsn+1 (" << (fsn + 1) << ")"
        << " because packets beyond that point are still in flight";

    f.flow->purgeRetxPktsToSend();
  }

  EXPECT_GT(totalRetx, 0u) << "IMMEDIATE mode should retransmit for SACK holes";

  // With 5 ACKs and holes at 0-3,5-10,12-18,20-30,32-41, the total
  // retransmissions should be at most the number of missing FSNs up to
  // maxAckdFsn=42 (i.e., 38 missing), not up to sndNxt=48.
  EXPECT_LE(totalRetx, 38u)
      << "total retransmissions should not include in-flight packets"
         " beyond maxAckdFsn";
}

// Same cursor-limit test but with DISTANCE_BASED_PWND mode.
// The cursor limit is maxAckdFsn - window + 1, which is even more
// conservative than IMMEDIATE, so in-flight packets beyond maxAckdFsn
// should never be retransmitted.
TEST_F(SackRetransmissionTest, DistanceBasedCursorLimitedToMaxAckdFsn) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::DISTANCE_BASED_PWND, .fraction = 1.0});
  setupLargeTracker(f, mtu, initialTag);

  ON_CALL(*f.rawCC, getAvailableWindowSize(0)).WillByDefault(Return(1000));
  ON_CALL(*f.rawCC, getFwnd(0)).WillByDefault(Return(3));

  // With fwnd=3 and fraction=1.0, the distance threshold is 3.
  // Deliver FSNs spaced far enough apart that the distance check triggers.
  const std::vector<int> arrivedFsns = {5, 10, 15, 20, 25, 30, 35, 40, 45};
  std::set<int> allReceived;

  EXPECT_CALL(*f.rawCC, onPacketAck(_, _))
      .Times(static_cast<int>(arrivedFsns.size()));
  EXPECT_CALL(*f.rawCC, onPacketLoss(_, _)).Times(AnyNumber());

  size_t totalRetx = 0;
  for (int fsn : arrivedFsns) {
    allReceived.insert(fsn);
    auto ack = buildSackAck(f.txState->sndUna, allReceived);
    f.flow->processAckPacket(ack, simulatedProcessedAtNs(ack.getTimestamp()));

    size_t pending = f.flow->getNumPendingRetxPackets();
    totalRetx += pending;

    uint16_t cursorAfter = *(f.txState->sndCur.rlock());
    EXPECT_LE(cursorAfter, static_cast<uint16_t>(fsn + 1))
        << "after ACK for FSN " << fsn << ", sndCur=" << cursorAfter
        << " should not advance beyond maxAckdFsn+1 (" << (fsn + 1) << ")"
        << " because packets beyond that point are still in flight";

    f.flow->purgeRetxPktsToSend();
  }

  EXPECT_GT(totalRetx, 0u)
      << "distance-based mode should retransmit once distance exceeds threshold";

  // With 9 ACKs at FSNs {5,10,...,45} and sndNxt=48, there are 37 missing
  // FSNs up to maxAckdFsn=45.  Retransmissions should not exceed that.
  EXPECT_LE(totalRetx, 37u)
      << "total retransmissions should not include in-flight packets"
         " beyond maxAckdFsn";
}

TEST_F(QpTest, RtoRetransmissionAllPackets) {
  SackRetxTestFixture f(
      arbiter,
      qp,
      mtu,
      initialTag,
      {.mode = SackRetransmissionMode::IMMEDIATE});
  setupLargeTracker(f, mtu, initialTag);

  ON_CALL(*f.rawCC, getAvailableWindowSize(0)).WillByDefault(Return(1000));
  ON_CALL(*f.rawCC, getFwnd(0)).WillByDefault(Return(48));

  size_t retxCount = f.flow->prepareRetxPacketsIfAny(true);

  // The RTO burst count is not limited here; a configurable limit would
  // reduce retxCount below 48.
  EXPECT_EQ(retxCount, 48u)
      << "RTO should queue all 48 unacked packets (no burst limit)";
  EXPECT_EQ(f.flow->getNumPendingRetxPackets(), 48u)
      << "all 48 should be pending";
}

TEST_F(QpTest, ExplicitPortPairsAllowOneLocalToManyRemotes) {
  const auto local = folly::IPAddress("fc00::1");
  const auto remote0 = folly::IPAddress("fc00::101");
  const auto remote1 = folly::IPAddress("fc00::102");

  Qp multiQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  const auto entry = makeUdpEntry();
  multiQp.addPortPair(0, local, entry, 0, remote0);
  multiQp.addPortPair(0, local, entry, 1, remote1);

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(2, 0, &sender);

  std::set<folly::IPAddress> usedLocals;
  std::set<folly::IPAddress> usedRemotes;
  std::set<uint16_t> usedRemoteIds;
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    usedLocals.insert(sender.getAddressFromSocketId(flow->getSocketId()));
    usedRemotes.insert(flow->getRemoteAddr());
    usedRemoteIds.insert(multiQp.getRemotePortId(flow->getRemotePortIdx()));
  }

  EXPECT_EQ(usedLocals, std::set<folly::IPAddress>({local}));
  EXPECT_EQ(usedRemotes, std::set<folly::IPAddress>({remote0, remote1}));
  EXPECT_EQ(usedRemoteIds, std::set<uint16_t>({0, 1}));
}

TEST_F(QpTest, ExplicitPortPairsAllowManyLocalsToOneRemote) {
  const auto local0 = folly::IPAddress("fc00::1");
  const auto local1 = folly::IPAddress("fc00::2");
  const auto remote = folly::IPAddress("fc00::101");

  Qp multiQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  const auto entry = makeUdpEntry();
  multiQp.addPortPair(0, local0, entry, 0, remote);
  multiQp.addPortPair(1, local1, entry, 0, remote);

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(2, 0, &sender);

  std::set<folly::IPAddress> usedLocals;
  std::set<uint16_t> usedRemoteIds;
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    usedLocals.insert(sender.getAddressFromSocketId(flow->getSocketId()));
    EXPECT_EQ(flow->getRemoteAddr(), remote);
    usedRemoteIds.insert(multiQp.getRemotePortId(flow->getRemotePortIdx()));
  }

  EXPECT_EQ(usedLocals, std::set<folly::IPAddress>({local0, local1}));
  EXPECT_EQ(usedRemoteIds, std::set<uint16_t>({0}));
}

TEST_F(QpTest, FailedPortPairAddRollsBackTheNewLocalPort) {
  const auto local0 = folly::IPAddress("fc00::1");
  const auto local1 = folly::IPAddress("fc00::2");
  const auto localRetry = folly::IPAddress("fc00::3");
  const auto remote0 = folly::IPAddress("fc00::101");
  const auto remote1 = folly::IPAddress("fc00::102");

  Qp qp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  qp.setMtu(mtu);
  const auto entry = makeUdpEntry();
  qp.addPortPair(0, local0, entry, 0, remote0);
  ASSERT_EQ(qp.getNumPortPairs(), 1)
      << "precondition: without the seeded pair, the rollback assertions "
         "below would pass against an empty table";

  // A valid new local id with an over-capacity remote id inserts the local
  // port and then fails, exercising the local rollback guard.
  EXPECT_THROW(
      qp.addPortPair(
          1, local1, entry, static_cast<uint16_t>(kMaxPorts), remote1),
      std::length_error)
      << "without a mid-add failure, nothing would exercise the local "
         "rollback guard";
  EXPECT_EQ(qp.getLocalAddrs().size(), 1)
      << "a lingering local port would attach a ghost endpoint to later pairs";
  EXPECT_EQ(qp.getLocalAddrs().count(local1), 0)
      << "the rolled-back address must not survive under the reused id";
  EXPECT_EQ(qp.getNumRemotePorts(), 1)
      << "the failed add must not grow the remote table";
  EXPECT_EQ(qp.getNumPortPairs(), 1)
      << "the failed add must not leave a pair behind";

  // The rolled-back id stays reusable for a different endpoint.
  qp.addPortPair(1, localRetry, entry, 1, remote1);
  EXPECT_EQ(qp.getNumPortPairs(), 2)
      << "a ghost entry would reserve the id and refuse the new endpoint";
}

TEST_F(QpTest, DuplicateRemoteAddressCanRepresentDistinctRemotePorts) {
  const auto local0 = folly::IPAddress("fc00::1");
  const auto local1 = folly::IPAddress("fc00::2");
  const auto remote = folly::IPAddress("fc00::101");

  Qp multiQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  const auto entry = makeUdpEntry();
  multiQp.addPortPair(0, local0, entry, 0, remote);
  multiQp.addPortPair(1, local1, entry, 1, remote);

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(2, 0, &sender);

  std::set<uint16_t> usedRemoteIds;
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    EXPECT_EQ(flow->getRemoteAddr(), remote);
    usedRemoteIds.insert(multiQp.getRemotePortId(flow->getRemotePortIdx()));
  }

  EXPECT_EQ(multiQp.getRemoteAddrs(), std::set<folly::IPAddress>({remote}));
  EXPECT_EQ(multiQp.getNumRemotePorts(), 2u);
  EXPECT_EQ(usedRemoteIds, std::set<uint16_t>({0, 1}));
}

TEST_F(QpTest, VerbsCompatibilityPortPairsAreIdempotentForRepeatedAv) {
  const auto local0 = folly::IPAddress("fc00::1");
  const auto local1 = folly::IPAddress("fc00::2");
  const auto remote0 = folly::IPAddress("fc00::101");
  const auto remote1 = folly::IPAddress("fc00::102");

  Qp multiQp{2, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  multiQp.setRemoteQpNum(2);
  const auto entry = makeUdpEntry();
  multiQp.addPortPair(local0, entry, remote0);
  multiQp.addPortPair(local1, entry, remote1);
  multiQp.addPortPair(local0, entry, remote0);

  EXPECT_EQ(multiQp.getNumPortPairs(), 2u);
  EXPECT_EQ(multiQp.getNumRemotePorts(), 2u);

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(2, 0, &sender);

  std::set<folly::IPAddress> usedRemotes;
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    usedRemotes.insert(flow->getRemoteAddr());
  }

  EXPECT_EQ(usedRemotes, std::set<folly::IPAddress>({remote0, remote1}));
}

TEST_F(QpTest, FailoverOnlyChoosesConfiguredPortPairs) {
  ScopedSuppressedTxTimers suppressTimers;
  const auto local0 = folly::IPAddress("fc00::1");
  const auto local1 = folly::IPAddress("fc00::2");
  const auto remote0 = folly::IPAddress("fc00::101");
  const auto remote1 = folly::IPAddress("fc00::102");

  Qp multiQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  const auto entry = makeUdpEntry();
  multiQp.addPortPair(0, local0, entry, 0, remote0);
  multiQp.addPortPair(1, local1, entry, 1, remote1);

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(2, 0, &sender);

  TxFlow* flowOnRemote0 = nullptr;
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    if (multiQp.getRemotePortId(flow->getRemotePortIdx()) == 0) {
      flowOnRemote0 = flow;
      break;
    }
  }
  ASSERT_NE(flowOnRemote0, nullptr);

  multiQp.updateRemoteActivePortSet(0x2);
  (void)flowOnRemote0->getNextPacketToSend();

  EXPECT_EQ(multiQp.getRemotePortId(flowOnRemote0->getRemotePortIdx()), 1);
  EXPECT_EQ(
      sender.getAddressFromSocketId(flowOnRemote0->getSocketId()), local1);
  EXPECT_EQ(flowOnRemote0->getRemoteAddr(), remote1);
}

TEST_F(QpTest, FailoverSocketAcquisitionFailurePreservesRoute) {
  gflags::FlagSaver flagSaver;
  ScopedSuppressedTxTimers suppressTimers;
  const auto local0 = folly::IPAddress("fc00::1");
  const auto local1 = folly::IPAddress("fc00::2");
  const auto remote0 = folly::IPAddress("fc00::101");
  const auto remote1 = folly::IPAddress("fc00::102");

  for (const bool singleOwner : {false, true}) {
    SCOPED_TRACE(singleOwner);
    FLAGS_libsoftmetaroce_single_owner_tx = singleOwner;
    LimitedTestSender sender(arbiter, mockUDPSocketMaker);
    sender.setLimitNumSockets(1);
    Qp multiQp{8, retxTimer, arbiter, mrs, &cq, ibvQp};
    multiQp.setMtu(mtu);
    createAlwaysSendingMockCongestionController(multiQp);
    const auto entry = makeUdpEntry();
    multiQp.addPortPair(0, local0, entry, 0, remote0);
    multiQp.addPortPair(1, local1, entry, 1, remote1);
    multiQp.setupFlows(1, 0, &sender);
    auto* flow = multiQp.getTxFlowById(0);
    ASSERT_NE(flow, nullptr);
    ASSERT_EQ(flow->getRemoteAddr(), remote0)
        << "precondition: the sole flow starts on the pair whose remote is down";
    ASSERT_EQ(multiQp.usesSingleOwnerRxRing(), singleOwner)
        << "precondition: packet building uses the requested ACK ownership mode";
    const auto originalSocket = flow->getSocketId();

    multiQp.updateRemoteActivePortSet(0x2);
    EXPECT_NO_THROW((void)multiQp.getNextPacketToSend())
        << "failed APS socket acquisition must not escape the packet builder";
    EXPECT_EQ(flow->getSocketId(), originalSocket)
        << "failed APS acquisition must retain the bound socket";
    EXPECT_EQ(
        std::make_pair(
            sender.getAddressFromSocketId(flow->getSocketId()),
            flow->getRemoteAddr()),
        std::make_pair(local0, remote0))
        << "failed APS acquisition must preserve both configured endpoints";

    sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
    EXPECT_NO_THROW((void)multiQp.getNextPacketToSend())
        << "a later APS visit must retry after socket capacity becomes available";
    EXPECT_EQ(
        std::make_pair(
            sender.getAddressFromSocketId(flow->getSocketId()),
            flow->getRemoteAddr()),
        std::make_pair(local1, remote1))
        << "a successful APS retry must move the complete configured pair";
  }
}

TEST_F(QpTest, FailoverScanIsOffsetByQpRotation) {
  ScopedSuppressedTxTimers suppressTimers;
  const auto local = folly::IPAddress("fc00::1");
  const auto remote0 = folly::IPAddress("fc00::101");
  const auto remote1 = folly::IPAddress("fc00::102");
  const auto remote2 = folly::IPAddress("fc00::103");
  const auto remote3 = folly::IPAddress("fc00::104");

  Qp multiQp{2, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  const auto entry = makeUdpEntry();
  multiQp.addPortPair(0, local, entry, 0, remote0);
  multiQp.addPortPair(0, local, entry, 1, remote1);
  multiQp.addPortPair(0, local, entry, 2, remote2);
  multiQp.addPortPair(0, local, entry, 3, remote3);

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(1, 0, &sender);

  auto* flow = multiQp.getTxFlowById(0);
  ASSERT_EQ(multiQp.getRemotePortId(flow->getRemotePortIdx()), 1);

  multiQp.updateRemoteActivePortSet(0xD);
  (void)flow->getNextPacketToSend();

  EXPECT_EQ(multiQp.getRemotePortId(flow->getRemotePortIdx()), 3);
  EXPECT_EQ(flow->getRemoteAddr(), remote3);
}

TEST_F(QpTest, PathRotationStartsDifferentQpsOnDifferentPairs) {
  // Rotation seeds each QP at pair (localQpNum >> 1) % numPairs, so QPs with
  // different rotations begin flow 0 on different configured pairs and spread
  // their initial traffic across the fabric.
  const std::vector<folly::IPAddress> locals{
      folly::IPAddress("fc00::1"),
      folly::IPAddress("fc00::2"),
      folly::IPAddress("fc00::3"),
      folly::IPAddress("fc00::4"),
  };
  const std::vector<folly::IPAddress> remotes{
      folly::IPAddress("fc00::101"),
      folly::IPAddress("fc00::102"),
      folly::IPAddress("fc00::103"),
      folly::IPAddress("fc00::104"),
  };
  const auto entry = makeUdpEntry();

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);

  const auto configure = [&](Qp& q) {
    q.setMtu(mtu);
    for (uint16_t i = 0; i < locals.size(); i++) {
      q.addPortPair(i, locals[i], entry, i, remotes[i]);
    }
    q.setupFlows(1, 0, &sender);
  };

  Qp qpA{3, retxTimer, arbiter, mrs, &cq, ibvQp}; // rotation 1
  Qp qpB{5, retxTimer, arbiter, mrs, &cq, ibvQp}; // rotation 2
  configure(qpA);
  configure(qpB);

  auto* flowA = qpA.getTxFlowById(0);
  auto* flowB = qpB.getTxFlowById(0);

  EXPECT_NE(
      qpA.getRemotePortId(flowA->getRemotePortIdx()),
      qpB.getRemotePortId(flowB->getRemotePortIdx()))
      << "QPs with different rotations should start flow 0 on different remote "
         "ports";
  EXPECT_NE(
      sender.getAddressFromSocketId(flowA->getSocketId()),
      sender.getAddressFromSocketId(flowB->getSocketId()))
      << "QPs with different rotations should start flow 0 on different local "
         "addresses";
}

TEST_F(QpTest, PathRotationCoversAllConfiguredPairs) {
  // With as many flows as configured pairs, rotation assigns each flow a
  // distinct pair so every configured local/remote edge carries a flow.
  const std::vector<folly::IPAddress> locals{
      folly::IPAddress("fc00::1"),
      folly::IPAddress("fc00::2"),
      folly::IPAddress("fc00::3"),
      folly::IPAddress("fc00::4"),
  };
  const std::vector<folly::IPAddress> remotes{
      folly::IPAddress("fc00::101"),
      folly::IPAddress("fc00::102"),
      folly::IPAddress("fc00::103"),
      folly::IPAddress("fc00::104"),
  };
  const auto entry = makeUdpEntry();

  Qp multiQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  for (uint16_t i = 0; i < locals.size(); i++) {
    multiQp.addPortPair(i, locals[i], entry, i, remotes[i]);
  }

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(static_cast<uint32_t>(locals.size()), 0, &sender);

  std::set<folly::IPAddress> usedLocals;
  std::set<folly::IPAddress> usedRemotes;
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    usedLocals.insert(sender.getAddressFromSocketId(flow->getSocketId()));
    usedRemotes.insert(flow->getRemoteAddr());
  }

  EXPECT_EQ(
      usedLocals, std::set<folly::IPAddress>(locals.begin(), locals.end()))
      << "every configured local port should carry a flow";
  EXPECT_EQ(
      usedRemotes, std::set<folly::IPAddress>(remotes.begin(), remotes.end()))
      << "every configured remote port should carry a flow";
}

TEST_F(QpTest, ZeroActivePortSetDoesNotTriggerFailover) {
  // A zero active-port report means the peer does not advertise port state, not
  // that all ports are down; it must not clear the known set or move any flow.
  ScopedSuppressedTxTimers suppressTimers;
  const auto local0 = folly::IPAddress("fc00::1");
  const auto local1 = folly::IPAddress("fc00::2");
  const auto remote0 = folly::IPAddress("fc00::101");
  const auto remote1 = folly::IPAddress("fc00::102");

  Qp multiQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  const auto entry = makeUdpEntry();
  multiQp.addPortPair(0, local0, entry, 0, remote0);
  multiQp.addPortPair(1, local1, entry, 1, remote1);

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(2, 0, &sender);
  multiQp.updateRemoteActivePortSet(0x3);

  std::vector<uint16_t> originalRemoteIds;
  std::vector<uint16_t> originalSockets;
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    originalRemoteIds.push_back(
        multiQp.getRemotePortId(flow->getRemotePortIdx()));
    originalSockets.push_back(flow->getSocketId());
  }

  multiQp.updateRemoteActivePortSet(0x0);
  EXPECT_EQ(multiQp.getRemoteActivePortSet(), 0x3)
      << "a zero report should be ignored, leaving the known active set intact";

  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    (void)multiQp.getTxFlowById(i)->getNextPacketToSend();
  }

  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    EXPECT_EQ(
        multiQp.getRemotePortId(flow->getRemotePortIdx()), originalRemoteIds[i])
        << "flow " << i
        << " should stay on its configured remote after a zero report";
    EXPECT_EQ(flow->getSocketId(), originalSockets[i])
        << "flow " << i << " should keep its local socket after a zero report";
  }
}

TEST_F(QpTest, FailoverRestoresOriginalPairWhenRemoteRecovers) {
  // When the original remote port recovers, the flow returns to its complete
  // original pair: local, remote, and bound socket.
  ScopedSuppressedTxTimers suppressTimers;
  const auto local0 = folly::IPAddress("fc00::1");
  const auto local1 = folly::IPAddress("fc00::2");
  const auto remote0 = folly::IPAddress("fc00::101");
  const auto remote1 = folly::IPAddress("fc00::102");

  Qp multiQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  const auto entry = makeUdpEntry();
  multiQp.addPortPair(0, local0, entry, 0, remote0);
  multiQp.addPortPair(1, local1, entry, 1, remote1);

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(2, 0, &sender);

  TxFlow* flowOnRemote0 = nullptr;
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    if (multiQp.getRemotePortId(flow->getRemotePortIdx()) == 0) {
      flowOnRemote0 = flow;
      break;
    }
  }
  ASSERT_NE(flowOnRemote0, nullptr);
  const auto originalSocket = flowOnRemote0->getSocketId();

  // Activate only remote port 1 straight from the empty set: no port-down is
  // recorded, so the later recovery of remote port 0 is not debounced.
  multiQp.updateRemoteActivePortSet(0x2);
  (void)flowOnRemote0->getNextPacketToSend();
  ASSERT_EQ(multiQp.getRemotePortId(flowOnRemote0->getRemotePortIdx()), 1)
      << "precondition: flow should fail over off the down remote port";

  multiQp.updateRemoteActivePortSet(0x3);
  (void)flowOnRemote0->getNextPacketToSend();

  EXPECT_EQ(multiQp.getRemotePortId(flowOnRemote0->getRemotePortIdx()), 0)
      << "flow should restore to its original remote port once it recovers";
  EXPECT_EQ(sender.getAddressFromSocketId(flowOnRemote0->getSocketId()), local0)
      << "flow should restore to its original local port";
  EXPECT_EQ(flowOnRemote0->getSocketId(), originalSocket)
      << "flow should restore to its original socket";
  EXPECT_EQ(flowOnRemote0->getRemoteAddr(), remote0)
      << "restored flow should target the original remote address";
}

TEST_F(QpTest, FailoverPrefersSameLocalPortWhenAvailable) {
  // When the current local port can still reach another active remote, failover
  // stays on that local port instead of switching to a different one.
  ScopedSuppressedTxTimers suppressTimers;
  const auto local0 = folly::IPAddress("fc00::1");
  const auto local1 = folly::IPAddress("fc00::2");
  const auto remote0 = folly::IPAddress("fc00::101");
  const auto remote1 = folly::IPAddress("fc00::102");
  const auto remote2 = folly::IPAddress("fc00::103");

  Qp multiQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  const auto entry = makeUdpEntry();
  multiQp.addPortPair(0, local0, entry, 0, remote0);
  multiQp.addPortPair(0, local0, entry, 1, remote1);
  multiQp.addPortPair(1, local1, entry, 2, remote2);

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  multiQp.setupFlows(1, 0, &sender);

  auto* flow = multiQp.getTxFlowById(0);
  ASSERT_EQ(multiQp.getRemotePortId(flow->getRemotePortIdx()), 0)
      << "precondition: flow 0 should start on the local0->remote0 pair";
  const auto originalSocket = flow->getSocketId();

  multiQp.updateRemoteActivePortSet(0x7); // all remotes up
  multiQp.updateRemoteActivePortSet(
      0x6); // remote0 down, remote1 and remote2 up
  (void)flow->getNextPacketToSend();

  EXPECT_EQ(multiQp.getRemotePortId(flow->getRemotePortIdx()), 1)
      << "failover should move to remote1, reachable from the same local port";
  EXPECT_EQ(flow->getSocketId(), originalSocket)
      << "failover should keep the same local socket when that local still "
         "reaches an active remote";
  EXPECT_EQ(flow->getRemoteAddr(), remote1)
      << "failover target should be the same-local pair's remote address";
}

TEST_F(QpTest, MultiplaneSharedRemoteAddressSpreadsAndFailsOver) {
  // Several planes can share one remote address as distinct remote ports. Flows
  // spread across all planes, and when a plane's port goes down every flow on
  // it fails over while still targeting the shared address.
  ScopedSuppressedTxTimers suppressTimers;
  const std::vector<folly::IPAddress> locals{
      folly::IPAddress("fc00::1"),
      folly::IPAddress("fc00::2"),
      folly::IPAddress("fc00::3"),
      folly::IPAddress("fc00::4"),
  };
  const auto remote = folly::IPAddress("fc00::101");
  const auto entry = makeUdpEntry();

  Qp multiQp{30, retxTimer, arbiter, mrs, &cq, ibvQp};
  multiQp.setMtu(mtu);
  for (uint16_t i = 0; i < locals.size(); i++) {
    multiQp.addPortPair(i, locals[i], entry, i, remote);
  }

  LimitedTestSender sender(arbiter, mockUDPSocketMaker);
  sender.setLimitNumSockets(LimitedTestSender::kMaxSocketsNoLimit);
  constexpr uint32_t kNumFlows = 8; // more flows than planes
  multiQp.setupFlows(kNumFlows, 0, &sender);

  std::set<folly::IPAddress> usedLocals;
  std::set<uint16_t> usedRemoteIds;
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    EXPECT_EQ(flow->getRemoteAddr(), remote)
        << "every plane shares the single remote address";
    usedLocals.insert(sender.getAddressFromSocketId(flow->getSocketId()));
    usedRemoteIds.insert(multiQp.getRemotePortId(flow->getRemotePortIdx()));
  }
  EXPECT_EQ(
      usedLocals, std::set<folly::IPAddress>(locals.begin(), locals.end()))
      << "flows should spread across every plane's local port";
  EXPECT_EQ(usedRemoteIds, std::set<uint16_t>({0, 1, 2, 3}))
      << "flows should spread across every remote port sharing the address";

  // Take plane 3's remote port down; no flow may remain on it.
  multiQp.updateRemoteActivePortSet(0xF);
  multiQp.updateRemoteActivePortSet(0x7);
  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    (void)multiQp.getTxFlowById(i)->getNextPacketToSend();
  }

  for (uint32_t i = 0; i < multiQp.getNumTxFlows(); i++) {
    auto* flow = multiQp.getTxFlowById(i);
    EXPECT_NE(multiQp.getRemotePortId(flow->getRemotePortIdx()), 3)
        << "flow " << i << " should fail over off the down plane";
    EXPECT_EQ(flow->getRemoteAddr(), remote)
        << "failed-over flow should still target the shared remote address";
  }
}

/*
 * An ack covers frames from every tag sharing the flow, so it is not filtered
 * by the tag it happens to carry. A failing tag's frames need it: acknowledged
 * is how they leave the wire without waiting out a retransmission timeout.
 */
TEST_F(QpTest, AnAckIsProcessedForATagThatHasFailed) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  Wqe* wqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  ASSERT_NE(wqe, nullptr);

  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 10;
  txState.sndNxt = 11;
  txState.tracker.emplace_back(
      PacketId(wqe, kMetaRoceInitialTag, wqe->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  auto tag = qp.getTag(kMetaRoceInitialTag);
  {
    auto tagGuard = tag->scopedLock();
    tag->markPendingWqesFailing(kWrRemAccErr);
  }
  ASSERT_NE(wqe->getOutstandingPosnCount(), 0u)
      << "the packet on the wire must still be outstanding after marking";

  // The peer acknowledges the packet that was already on the wire.
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setTag(kMetaRoceInitialTag)
                                  .setCumulativeFsn(10)
                                  .setMaxRcvdFsn(10)
                                  .setAckStatus(AckStatus::ACK)
                                  .setTimestamp(0)
                                  .build();
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  // The work request has completed and been freed by now, so it is only safe to
  // ask the tracker and the completion queue about it.
  EXPECT_TRUE(txState.tracker.empty())
      << "the acknowledged frame must leave the tracker";
  ASSERT_EQ(cq.getNumCompletedWqes(), 1u)
      << "discarding the ack would leave the frame outstanding until an rto, "
         "delaying the error the client is waiting for";
  const auto cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.workReqId, 100u)
      << "the work request the tag gave up on must be the one reported";
  EXPECT_EQ(cqe.status, kWrRemAccErr)
      << "it must report the error the tag failed with, not a success";
}

/*
 * A BRNR that gives up on a message's last outstanding packet leaves nothing
 * behind to complete it: no ack is coming for an abandoned posn, and a message
 * with nothing outstanding runs no retransmission timer. The completion has to
 * be produced where the frame is abandoned.
 */
TEST_F(QpTest, ABrnrThatAbandonsTheLastPosnCompletesTheMessage) {
  const auto flowId = 0;
  auto congestionController = std::make_unique<MockCongestionController>();
  qp.setCongestionController(std::move(congestionController));
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(flowId);

  // Posted through the tag, so the tag owns it and can retire it; a WQE the
  // tag does not hold could not be completed however the ack is handled.
  auto payload = std::make_unique<char[]>(mtu);
  auto* const givenUpWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = mtu,
       .address = payload.get()});

  auto& txState = flow->getTxState();
  txState.sndUna = 10;
  *(txState.sndCur.wlock()) = 11;
  txState.maxAckdFsn = 9;
  txState.sndNxt = 11;
  // One packet, so abandoning it takes the message to nothing outstanding.
  const uint32_t posn = givenUpWqe->allocateNextPosn();
  txState.tracker.emplace_back(
      PacketId(givenUpWqe, 0, posn, mtu), Flow::FsnState::UNACKED);
  givenUpWqe->markFailing(kWcRetryExcErr);
  ASSERT_EQ(cq.getNumCompletedWqes(), 0)
      << "the message must still be outstanding before the ack arrives, or "
         "this says nothing about what the ack did";

  BitVector<AckHeader::kAckTrackerBits> rnrBitVector;
  rnrBitVector.setBit(0);
  const AckHeader ackHeader = AckHeader::AckHeaderBuilder()
                                  .setCumulativeFsn(9)
                                  .setMaxRcvdFsn(10)
                                  .setAckStatus(AckStatus::BRNR)
                                  .setRnrBitVector(rnrBitVector)
                                  .setRnrBackoffTimeExp(0)
                                  .setTimestamp(0)
                                  .build();
  flow->processAckPacket(
      ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));

  // Nothing may touch the WQE from here: completing it retires it, so the
  // completion queue is the only place left to observe what happened.
  ASSERT_EQ(cq.getNumCompletedWqes(), 1)
      << "nothing else will complete this message: its last packet was "
         "abandoned rather than acknowledged, and a message with nothing "
         "outstanding runs no timer";
  const auto cqe = folly::coro::blockingWait(cq.pollCq());
  EXPECT_EQ(cqe.status, kWcRetryExcErr)
      << "the completion must carry the error the message was given up with, "
         "not a flush status";
}

/*
 * A flow id error hands its frames back so another flow can carry them. Work
 * that has been given up on has no successor to carry it, so its frame is
 * abandoned instead: returning it would offer a failing read to the sender
 * again and count a posn that will never be sent.
 */
TEST_F(QpTest, AFlowIdErrorAbandonsAFailingReadsFrameRatherThanHandingItBack) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  // Posted first and left pending, so in-order completion cannot retire the
  // read below while this test still inspects it.
  Wqe* blockingWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 100,
       .address = payload.data()});
  Wqe* failingRead = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 200,
       .address = payload.data()});
  ASSERT_NE(blockingWqe, nullptr)
      << "without an earlier pending work request the read could retire "
         "before the checks below read it";
  ASSERT_NE(failingRead, nullptr)
      << "the read is what this test is about, so it has to exist";

  // Its request is on the wire when the tag gives up on it.
  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          failingRead,
          kMetaRoceInitialTag,
          failingRead->allocateNextPosn(),
          mtu),
      Flow::FsnState::UNACKED);
  {
    auto tagGuard = initialTag->scopedLock();
    failingRead->markFailing(kWcRetryExcErr);
  }
  const uint32_t unsentBefore = qp.getUnsentPosnCount();

  flow->processNakFlowIdError();

  EXPECT_TRUE(failingRead->isRequestFrameResolved())
      << "the frame must be given up on, not handed back: nothing is going to "
         "send it, so leaving it unresolved would keep the read from settling";
  EXPECT_TRUE(failingRead->hasBeenSent())
      << "a read that has been given up on must not become sendable again";
  EXPECT_EQ(qp.getUnsentPosnCount(), unsentBefore)
      << "a posn nothing will send must not go back into the unsent count the "
         "rate hint is built from";
}

/*
 * A peer that rejects a flow id after serving a read on it is contradicting
 * itself: it cannot both refuse the request and have served it. The frame is
 * abandoned rather than handed back, so the contradiction costs nothing --
 * handing it back would offer the read to the sender again and put a request
 * on the wire for a message the peer has already served.
 */
TEST_F(QpTest, AFlowIdErrorAbandonsAServedReadsFrameRatherThanHandingItBack) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  // Posted first and left pending, so in-order completion cannot retire the
  // read below while this test still inspects it.
  Wqe* blockingWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 300,
       .address = payload.data()});
  Wqe* servedRead = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 301,
       .address = payload.data()});
  ASSERT_NE(blockingWqe, nullptr)
      << "without an earlier pending work request the read could retire "
         "before the checks below read it";
  ASSERT_NE(servedRead, nullptr)
      << "the read is what this test is about, so it has to exist";

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          servedRead, kMetaRoceInitialTag, servedRead->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  // The responses land while the request's own ack is still outstanding: posn
  // zero is both the slot the request occupies and the one the first response
  // fills.
  servedRead->onPosnReceived(Wqe::kRequestFrameWirePosn);
  ASSERT_TRUE(servedRead->hasBeenServed())
      << "precondition: the peer has served the read, which is what makes "
         "handing its request frame back pointless";
  const uint32_t unsentBefore = qp.getUnsentPosnCount();

  flow->processNakFlowIdError();

  EXPECT_TRUE(servedRead->isRequestFrameResolved())
      << "the frame must be abandoned, not handed back: the read is already "
         "served, so nothing is going to ask for it again";
  EXPECT_TRUE(servedRead->hasBeenSent())
      << "a served read must not become sendable again, or the sender puts a "
         "request back on the wire for data the responder has already sent";
  EXPECT_EQ(qp.getUnsentPosnCount(), unsentBefore)
      << "a posn nothing will send must not go back into the unsent count the "
         "rate hint is built from";
}

// Takes the counter by value, so the coroutine frame does not outlive a
// capturing lambda the caller has already destroyed.
static folly::coro::Task<void> countOneRun(std::atomic<int>* ran) {
  ran->fetch_add(1, std::memory_order_relaxed);
  co_return;
}

TEST_F(QpTest, ClosingAdmissionDoesNotRaceSubmission) {
  // The sanitizer is this test's oracle; the expectation below is secondary.
  std::atomic<int> ran{0};
  {
    struct ibv_qp racedIbvQp{};
    Qp raced{kSecondQpNum, retxTimer, arbiter, mrs, &cq, &racedIbvQp};
    raced.addToScopeIfOpen(
        folly::coro::co_withExecutor(
            folly::getGlobalCPUExecutor(), countOneRun(&ran)));
    std::thread submitter([&] {
      for (int i = 0; i < 200; ++i) {
        raced.addToScopeIfOpen(
            folly::coro::co_withExecutor(
                folly::getGlobalCPUExecutor(), countOneRun(&ran)));
      }
    });
    raced.closeScopeAdmission();
    submitter.join();
    // ~Qp joins the scope, so every admitted task has run below.
  }

  EXPECT_GE(ran.load(std::memory_order_relaxed), 1)
      << "work admitted before teardown closed the gate must still run, or "
         "the gate is refusing everything rather than only what follows it";
}

/*
 * The retransmission scan is the ordinary way an unacked frame is resolved,
 * and a served read's request frame is the one case where resending achieves
 * nothing: the responder re-acks a duplicate rather than serving it again.
 * The frame blocks retirement, holding its work request in the tag's map
 * after the response reported it.
 */
TEST_F(QpTest, TheRetransmitScanAbandonsAServedReadsRequestFrame) {
  qp.setCongestionController(std::make_unique<MockCongestionController>());
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  auto flow = qp.getTxFlowById(0);

  std::array<char, 4096> payload{};
  // Posted first and left pending, so in-order completion cannot retire the
  // read below while this test still inspects it.
  Wqe* blockingWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 300,
       .address = payload.data()});
  Wqe* servedRead = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = payload.size(),
       .tagNum = kMetaRoceInitialTag,
       .workReqId = 301,
       .address = payload.data()});
  ASSERT_NE(blockingWqe, nullptr);
  ASSERT_NE(servedRead, nullptr);

  auto& txState = flow->getTxState();
  txState.sndUna = 0;
  *(txState.sndCur.wlock()) = 0;
  txState.sndNxt = 1;
  txState.tracker.emplace_back(
      PacketId(
          servedRead, kMetaRoceInitialTag, servedRead->allocateNextPosn(), mtu),
      Flow::FsnState::UNACKED);

  // The response lands while the request's own ack is still outstanding, which
  // is the window the scan finds it in.
  servedRead->onPosnReceived(Wqe::kRequestFrameWirePosn);
  ASSERT_TRUE(servedRead->hasBeenServed())
      << "precondition: the peer has served the read, or resending its "
         "request is the right thing to do and the case proves nothing";

  const uint16_t queuedForRetx = flow->prepareRetxPacketsIfAny(true);

  EXPECT_TRUE(servedRead->isRequestFrameResolved())
      << "the scan must settle the frame: left allocated, it holds the read "
         "in the map after the response reported it, and nothing else on a "
         "quiet flow will resolve it";
  EXPECT_EQ(queuedForRetx, 0)
      << "and it must not resend: the peer answered this request already, so "
         "the copy would reach it as a duplicate";
  EXPECT_EQ(flow->getNumPendingRetxPackets(), 0u)
      << "nothing queued behind it either, or the frame is resolved and a "
         "packet for it still goes out";
}

TEST_F(QpTest, AReceiveMissRecordsTheRnrCancelRequestOnItsOwnTag) {
  constexpr uint8_t kDeliveredTag = kMetaRoceInitialTag + 1;
  qp.addTag(kDeliveredTag, &cq, &otherIbvQpStorage);
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);
  qp.startProcessingWqes();
  auto receiversFlow = qp.getRxFlowById(0);

  Qp sendingQp{kSecondQpNum, retxTimer, arbiter, mrs, &cq, ibvQp};
  sendingQp.setMtu(mtu);
  createAlwaysSendingMockCongestionController(sendingQp);
  sendingQp.addPortPair(
      *qp.getRemoteAddrs().begin(), *qp.getLocalAddrs().begin());
  sendingQp.addTag(kDeliveredTag, &cq, &otherIbvQpStorage);
  LimitedTestSender sendingMockSender(arbiter, mockUDPSocketMaker);
  sendingQp.setupFlows(1, 0, &sendingMockSender);

  // The refused tag goes first, so the flow holds its bit when the delivered
  // tag's packet arrives behind it.
  auto refusedPayload = std::make_unique<char[]>(mtu);
  Wqe* const refusedWqe = postWqeHelper(
      &sendingQp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .tagNum = kMetaRoceInitialTag,
       .address = refusedPayload.get()});
  ASSERT_NE(refusedWqe, nullptr);
  auto refusedEgress = sendingQp.getNextPacketToSend();
  ASSERT_NE(refusedEgress, nullptr);
  auto* refusedPacket = dynamic_cast<DataPacket*>(refusedEgress->packet.get());
  ASSERT_NE(refusedPacket, nullptr);
  const auto refusedAck =
      receiversFlow->processDataPacket(refusedPacket, kEcnECT0);
  ASSERT_TRUE(refusedAck.has_value())
      << "precondition: without an ACK for the refused message, this test "
         "would not reach the BRNR it reports";
  ASSERT_EQ(refusedAck.value().getAckStatus(), AckStatus::BRNR)
      << "precondition: the responder refuses the first tag's message, which "
         "latches BRNR on the flow";

  auto deliveredPayload = std::make_unique<char[]>(mtu);
  postInSequenceReceiveWqe(qp, deliveredPayload.get(), mtu, kDeliveredTag);
  Wqe* const deliveredWqe = postWqeHelper(
      &sendingQp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .tagNum = kDeliveredTag,
       .address = deliveredPayload.get()});
  ASSERT_NE(deliveredWqe, nullptr);
  auto deliveredEgress = sendingQp.getNextPacketToSend();
  ASSERT_NE(deliveredEgress, nullptr);
  auto* deliveredPacket =
      dynamic_cast<DataPacket*>(deliveredEgress->packet.get());
  ASSERT_NE(deliveredPacket, nullptr);
  const auto deliveredAck =
      receiversFlow->processDataPacket(deliveredPacket, kEcnECT0);
  ASSERT_TRUE(deliveredAck.has_value())
      << "precondition: the delivered packet produces an ACK, which reports "
         "the flow's earlier BRNR bit";
  ASSERT_EQ(deliveredAck.value().getAckStatus(), AckStatus::BRNR)
      << "precondition: the flow still holds the first tag's bit, so this "
         "acknowledgment reports BRNR though the responder delivered this "
         "packet";

  // A post clears its tag's RNR-Cancel request and returns the csn the cancel
  // reports, so posting to each tag shows which tag the miss recorded.
  auto deliveredBuffer = std::make_unique<char[]>(mtu);
  EXPECT_FALSE(postReceiveToTag(
                   *qp.getTag(kDeliveredTag),
                   std::make_unique<ReceiveWqe>(
                       1, oneSge(deliveredBuffer.get(), mtu), mtu),
                   mtu)
                   .has_value())
      << "this tag bound its receive, so a cancel here would claim it refused "
         "a message though the ACK's BRNR came from the other tag";

  auto refusedBuffer = std::make_unique<char[]>(mtu);
  EXPECT_TRUE(postReceiveToTag(
                  *qp.getTag(kMetaRoceInitialTag),
                  std::make_unique<ReceiveWqe>(
                      2, oneSge(refusedBuffer.get(), mtu), mtu),
                  mtu)
                  .has_value())
      << "the initiator is backed off on the refused tag, so the post to it "
         "must produce the RNR-Cancel";
}

TEST_F(QpTest, ACancelIsEnqueuedWhileTheSendPathIsAlreadyBuilding) {
  constexpr uint8_t kSendingTag = kMetaRoceInitialTag + 1;
  constexpr int kCancels = 2000;
  qp.addTag(kSendingTag, &cq, &otherIbvQpStorage);
  createAlwaysSendingMockCongestionController(qp);
  LimitedTestSender mockSender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(1, 1, &mockSender);

  // Work on one tag keeps each visit building, so a cancel for another tag
  // arrives while the send path is already inside one.
  std::array<char, 4 * 4096> payload{};
  Wqe* const sendingWqe = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(payload.size()),
       .tagNum = kSendingTag,
       .workReqId = 300,
       .address = payload.data()});
  ASSERT_NE(sendingWqe, nullptr);
  ASSERT_TRUE(processWqes(&qp, 1, 0))
      << "precondition: the sending tag holds the work that keeps the build "
         "busy";

  std::thread producer([&] {
    for (int n = 0; n < kCancels; ++n) {
      qp.scheduleRnrCancelPacket(kMetaRoceInitialTag, static_cast<uint16_t>(n));
    }
  });

  int dequeued = 0;
  // Driven by what the send path has dequeued, so the producer finishing first
  // cannot skip the dequeue this case exists to exercise. The deadline is
  // only so a build that declines to send a cancel cannot hang the suite.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (dequeued < kCancels && std::chrono::steady_clock::now() < deadline) {
    auto egressPacket = qp.getNextPacketToSend();
    if (egressPacket != nullptr &&
        dynamic_cast<RnrCancelPacket*>(egressPacket->packet.get()) != nullptr) {
      ++dequeued;
    }
  }
  producer.join();

  EXPECT_EQ(dequeued, kCancels)
      << "the send path must dequeue every enqueued cancel; short of that, "
         "the drain reached its deadline and the two threads may never have "
         "overlapped on the queue for a sanitizer to observe";
}

} // namespace libsoftmetaroce
