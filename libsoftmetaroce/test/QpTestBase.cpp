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

#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>

#include <libsoftmetaroce/test/OneSge.h>
#include <libsoftmetaroce/test/QpTestBase.h>

using namespace ::testing;
namespace libsoftmetaroce {

void QpTestBase::sleepForUs(uint32_t duration) {
  std::condition_variable cv;
  std::mutex mtx;
  std::unique_lock<std::mutex> lock(mtx);
  auto start = std::chrono::steady_clock::now();
  cv.wait_for(lock, std::chrono::microseconds(duration), [start, duration]() {
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start);
    return diff >= std::chrono::microseconds(duration);
  });
}

std::unique_ptr<struct EgressPacket> QpTestBase::waitForPacket(
    Qp& qp,
    uint32_t timeoutUs) {
  auto start = std::chrono::steady_clock::now();
  std::unique_ptr<struct EgressPacket> egressPacket;
  while (std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - start)
             .count() < timeoutUs) {
    egressPacket = qp.getNextPacketToSend();
    if (egressPacket && egressPacket->packet) {
      return egressPacket;
    };
    sleepForUs(100);
  }
  return egressPacket;
}

void QpTestBase::createAlwaysSendingMockCongestionController(Qp& qp) {
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  ON_CALL(*congestionController, getAvailableWindowSize(_))
      .WillByDefault(Return(100));
  ON_CALL(*congestionController, onPacketSent(_)).WillByDefault(Return());
  ON_CALL(*congestionController, onPacketAck(_, _))
      .WillByDefault(Return(CongestionController::AckResult{}));
  ON_CALL(*congestionController, onPacketLoss(_, _)).WillByDefault(Return());
  ON_CALL(*congestionController, onFlowAdded(_)).WillByDefault(Return());
  qp.setCongestionController(std::move(congestionController));
}

void QpTestBase::SetUp() {
  qp.setMtu(mtu);
  struct ibv_mr* mr = mrs.reg_mr(
      &pd,
      receiveBuffer.data(),
      receiveBuffer.size(),
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_LOCAL_WRITE);
  mrKey = mr->rkey;
  qp.addPortPair(folly::IPAddress("fc00::1"), folly::IPAddress("fc00::101"));
  initialTag = qp.getTag(kMetaRoceInitialTag);
  ASSERT_TRUE(initialTag->setReceiveSequenceNumbers(
      {.nextMsn = kFixtureCdmsn + 1, .nextCsn = kFixtureRxCsn}))
      << "precondition: the cases here read receive sequence numbers this "
         "stands in for an rq_psn to supply; unconfigured, they would read a "
         "tag that never took any";
}

bool QpTestBase::allocateSendablePosns(Wqe* wqe) {
  if (wqe->hasRequestFrame()) {
    ADD_FAILURE()
        << "precondition: a read or atomic sends one frame rather than a posn "
           "of its own, so its caller allocates that frame and says which it is";
    return false;
  }
  const uint32_t posnCount = wqe->getSendablePosnCount();
  for (uint32_t expected = 0; expected < posnCount; ++expected) {
    if (wqe->allocateNextPosn() != expected) {
      ADD_FAILURE()
          << "precondition: posns are allocated in order, so an ack addressing "
             "posn "
          << expected << " matches the frame allocated here";
      return false;
    }
  }
  return true;
}

Wqe* QpTestBase::postWqeHelper(Qp* postQp, WqeData wqeData, bool doProcessWqe) {
  static int workReqId = 23;
  Wqe* wqe_p = nullptr;
  CHECK(!wqeData.msn.has_value())
      << "tests using postWqeHelper may not set msn (it is set in order of posting)";
  CHECK(!wqeData.csn.has_value())
      << "tests using postWqeHelper may not set csn (it is set in order of posting)";
  if (wqeData.mtu.has_value() == false) {
    wqeData.mtu = postQp->getMtu();
  }
  if (wqeData.workReqId.has_value() == false) {
    wqeData.workReqId = workReqId++;
  }
  if (wqeData.address.has_value() == false) {
    wqeData.address = nullptr;
  }
  if (wqeData.tagNum.has_value() == false) {
    wqeData.tagNum = 0; // Default to tag 0
  }

  std::unique_ptr<Wqe> wqe;
  bool isRWqe = false;

  switch (wqeData.workType) {
    case Wqe::WorkType::kWriteWithImm:
      CHECK(wqeData.maybeImmediateData.has_value())
          << "Write with immediate requires immediate data!";
      wqe = std::make_unique<WriteWithImmWqe>(
          wqeData.workReqId.value(),
          oneSge(wqeData.address.value(), wqeData.msgSizeBytes),
          wqeData.mtu.value(),
          wqeData.remote_addr.value_or((unsigned long)receiveBuffer.data()),
          wqeData.rkey.value_or(1),
          wqeData.maybeImmediateData.value());
      break;
    case Wqe::WorkType::kSend:
      wqe = std::make_unique<SendWqe>(
          wqeData.workReqId.value(),
          oneSge(wqeData.address.value(), wqeData.msgSizeBytes),
          wqeData.mtu.value());
      break;
    case Wqe::WorkType::kReceive: {
      // Carry a real lkey so an arriving message validates against the buffer;
      // a zero-length receive has none to register.
      const uint32_t lkey =
          (wqeData.address.value() != nullptr && wqeData.msgSizeBytes > 0)
          ? mrs.reg_mr(
                   &pd,
                   wqeData.address.value(),
                   wqeData.msgSizeBytes,
                   IBV_ACCESS_LOCAL_WRITE)
                ->lkey
          : 0;
      wqe = std::make_unique<ReceiveWqe>(
          wqeData.workReqId.value(),
          oneSge(wqeData.address.value(), wqeData.msgSizeBytes, lkey),
          wqeData.mtu.value());
      isRWqe = true;
      break;
    }
    case Wqe::WorkType::kRead:
    case Wqe::WorkType::kWrite: {
      // A read validates its local buffer when the response is written back, so
      // register it for a real lkey; a write never consults the lkey, so leave
      // it 0.
      const uint32_t lkey =
          (wqeData.workType == Wqe::WorkType::kRead &&
           wqeData.address.value() != nullptr && wqeData.msgSizeBytes > 0)
          ? mrs.reg_mr(
                   &pd,
                   wqeData.address.value(),
                   wqeData.msgSizeBytes,
                   IBV_ACCESS_LOCAL_WRITE)
                ->lkey
          : 0;
      wqe = std::make_unique<ReadWriteWqe>(
          wqeData.workType,
          wqeData.workReqId.value(),
          oneSge(wqeData.address.value(), wqeData.msgSizeBytes, lkey),
          wqeData.mtu.value(),
          wqeData.remote_addr.value_or((unsigned long)receiveBuffer.data()),
          wqeData.rkey.value_or(1),
          /*send_flags=*/IBV_SEND_SIGNALED);
      break;
    }
    case Wqe::WorkType::kReadWithImm: {
      CHECK(wqeData.maybeImmediateData.has_value())
          << "Read with immediate requires immediate data!";
      // Register the local scatter buffer for a real lkey.
      const uint32_t lkey =
          (wqeData.address.value() != nullptr && wqeData.msgSizeBytes > 0)
          ? mrs.reg_mr(
                   &pd,
                   wqeData.address.value(),
                   wqeData.msgSizeBytes,
                   IBV_ACCESS_LOCAL_WRITE)
                ->lkey
          : 0;
      wqe = std::make_unique<ReadWithImmWqe>(
          wqeData.workReqId.value(),
          oneSge(wqeData.address.value(), wqeData.msgSizeBytes, lkey),
          wqeData.mtu.value(),
          wqeData.remote_addr.value_or((unsigned long)receiveBuffer.data()),
          wqeData.rkey.value_or(1),
          wqeData.maybeImmediateData.value(),
          /*send_flags=*/IBV_SEND_SIGNALED);
      break;
    }
    case Wqe::WorkType::kAtomicFetchAdd:
    case Wqe::WorkType::kAtomicCmpSwp: {
      // Carry a real lkey so the atomic response validates against the buffer,
      // unless the test overrides it to exercise a local-protection error.
      uint32_t lkey = 0;
      if (wqeData.lkeyOverride.has_value()) {
        lkey = wqeData.lkeyOverride.value();
      } else if (wqeData.address.value() != nullptr) {
        lkey = mrs.reg_mr(
                      &pd,
                      wqeData.address.value(),
                      kAtomicOperationSizeBytes,
                      IBV_ACCESS_LOCAL_WRITE)
                   ->lkey;
      }
      wqe = std::make_unique<AtomicWqe>(
          wqeData.workType,
          wqeData.workReqId.value(),
          LocalSge{
              .address = wqeData.address.value(),
              .length = kAtomicOperationSizeBytes,
              .lkey = lkey,
          },
          wqeData.mtu.value(),
          wqeData.remote_addr.value_or((unsigned long)receiveBuffer.data()),
          wqeData.rkey.value_or(1),
          wqeData.swapOrAddData,
          wqeData.compareData,
          /*send_flags=*/IBV_SEND_SIGNALED);
      break;
    }
    default:
      LOG(FATAL) << "Unknown WorkType: " << static_cast<int>(wqeData.workType);
  }
  if (wqeData.msgSizeBytes > 0) {
    EXPECT_EQ(
        wqe->getOutstandingPosnCount(),
        (wqeData.msgSizeBytes + wqeData.mtu.value() - 1) / wqeData.mtu.value());
  }
  wqe_p = wqe.get();

  // Read only where they are used: each walks every Tag of the QP, taking two
  // locks per Tag, which a caller posting in bulk pays for nothing.
  const auto prevNumWqes = doProcessWqe ? qp.getNumPendingWqes() : 0;
  const auto prevNumRWqes = doProcessWqe ? qp.getNumPendingRWqes() : 0;
  CHECK(postQp->postWqe(wqeData.tagNum.value(), std::move(wqe)));

  // Wait for the wqe to be processed by the async task
  if (doProcessWqe) {
    // Do not enforce that this returns true (i.e., that the number of pending
    // WQEs equals the expected value) because, in the general case, it is
    // possible that a WQE was completed elsewhere while posting the current
    // WQE, leaving the number of pending WQEs unchanged.
    processWqes(
        postQp,
        static_cast<int>(prevNumWqes) + !isRWqe,
        static_cast<int>(prevNumRWqes) + isRWqe);
  }

  return wqe_p;
}

// Process posted WQEs into their Tags until either the expected number of WQEs
// are present or the specified duration has elapsed. Returns whether the
// expected number of WQEs were found.
bool QpTestBase::processWqes(
    Qp* postQp,
    int expectedNumWqes,
    int expectedNumRWqes,
    std::chrono::milliseconds durationMs) {
  folly::CancellationSource cancellationSource;
  // Task to process WQEs forever. Will be cancelled by waitTask.
  auto processTask = [postQp, cancellationSource]() -> folly::coro::Task<bool> {
    try {
      co_await folly::coro::co_withCancellation(
          cancellationSource.getToken(), postQp->processWqes());
    } catch (const folly::OperationCancelled&) {
    }
    co_return true;
  };
  // Task to wait for a certain number of WQEs to be posted, at which point
  // it will cancel processTask. Returns whether the desired number of WQEs was
  // found.
  auto waitTask = [postQp,
                   expectedNumWqes,
                   expectedNumRWqes,
                   durationMs,
                   cancellationSource]() -> folly::coro::Task<bool> {
    bool foundWqes =
        postQp->waitForWqesToBePosted(expectedNumWqes, durationMs) &&
        postQp->waitForRWqesToBePosted(expectedNumRWqes, durationMs);
    LOG(INFO) << (foundWqes ? "found wqes" : "timed out")
              << ", requesting cancellation";
    cancellationSource.requestCancellation();
    co_return foundWqes;
  };
  // Execute both tasks and collect the results.
  std::tuple<bool, bool> res = folly::coro::blockingWait(
      folly::coro::collectAll(
          folly::coro::co_withExecutor(
              folly::getGlobalCPUExecutor(), processTask()),
          folly::coro::co_withExecutor(
              folly::getGlobalCPUExecutor(), waitTask())));
  // Result 1 corresponds to waitTask, and whether the desired number of WQEs
  // were found.
  return std::get<1>(res);
}

Wqe* QpTestBase::postInSequenceReceiveWqe(
    Qp& postQp,
    void* address,
    uint32_t msgSizeBytes,
    uint8_t tagNum) {
  Wqe* receiveWqe = postWqeHelper(
      &postQp,
      {.workType = Wqe::WorkType::kReceive,
       .msgSizeBytes = msgSizeBytes,
       .tagNum = tagNum,
       .address = address});
  auto postQpTag = postQp.getTag(tagNum);
  const uint16_t msn = postQpTag->getCDMSN() + postQpTag->getNumPendingRWqes();
  receiveWqe->setMsn(msn);
  const uint16_t csn = receiveWqe->getCsn().value();
  postQpTag->assignInitiatorMsnToReceiveWqeIfNeeded(csn, msn, postQp.getMtu());
  return receiveWqe;
}

} // namespace libsoftmetaroce
