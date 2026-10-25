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

#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include <folly/coro/BlockingWait.h>
#include <folly/executors/ManualExecutor.h>
#include <folly/synchronization/Baton.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <utility>

#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/ProtocolCompatibility.h>
#include <libsoftmetaroce/Tag.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/WrappingCompare.h>
#include <libsoftmetaroce/codec/ReadWriteHeader.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <libsoftmetaroce/test/PostToTag.h>

using namespace ::testing;

namespace libsoftmetaroce {

// Puts the queue pair in `state` for the scope and restores what it found, so a
// case can reach a setting a tag takes only in an earlier state.
class QpStateGuard {
 public:
  QpStateGuard(struct ibv_qp& ibvQp, ibv_qp_state state)
      : ibvQp_(ibvQp), resumeState_(ibvQp.state) {
    ibvQp_.state = state;
  }
  QpStateGuard(const QpStateGuard&) = delete;
  QpStateGuard& operator=(const QpStateGuard&) = delete;
  ~QpStateGuard() {
    ibvQp_.state = resumeState_;
  }

 private:
  struct ibv_qp& ibvQp_;
  const ibv_qp_state resumeState_;
};

class TagTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cq_ = std::make_unique<CompletionQueue>();
    memoryRegions_ = std::make_unique<MemoryRegions>();
    // The queue pair shares a protection domain with the regions registered
    // below, as a client's does.
    ibvQpStorage_.pd = &pd_;
    tag_ = makeTagInInit(*cq_);
    // A client's queue pair runs once its tags are added, and a case wanting an
    // earlier state sets it.
    ibvQpStorage_.state = IBV_QPS_RTS;
    configureReceiveSequencing(kFixtureCdmsn);

    // Register memory regions for testing
    buffer_ = std::make_unique<std::array<char, kBufferSize>>();
    ibvMr_ = memoryRegions_->reg_mr(
        &pd_,
        buffer_->data(),
        kBufferSize,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_READ);
    ASSERT_NE(ibvMr_, nullptr)
        << "every test here addresses memory through this region; without it "
           "they would exercise the failure path instead of what they test";
  }

  // A tag is built before its queue pair runs, as a client's is.
  std::unique_ptr<Tag> makeTagInInit(
      CompletionQueue& cq,
      bool sqSigAll = false,
      SharedReceiveQueue* sharedReceiveQueue = nullptr) {
    const QpStateGuard inInit{ibvQpStorage_, IBV_QPS_INIT};
    return std::make_unique<Tag>(
        kQpNum,
        kTagNum,
        &cq,
        ibvQp_,
        sqSigAll,
        /*maxInlineData=*/0,
        kMaxQpWorkRequests,
        kMaxQpWorkRequests,
        sharedReceiveQueue);
  }

  // Rebuilds the fixture's tag drawing from `sharedQueue`, sequenced as SetUp
  // left it. A tag takes its queue at construction.
  void rebuildTagDrawingFrom(SharedReceiveQueue& sharedQueue) {
    tag_ = makeTagInInit(*cq_, /*sqSigAll=*/false, &sharedQueue);
    configureReceiveSequencing(kFixtureCdmsn);
  }

  // This tag has no TagSet, so there is no set lock to hold. A tag takes the
  // sequencing only before RTR.
  void configureReceiveSequencing(uint16_t cdmsn) {
    const QpStateGuard inInit{ibvQpStorage_, IBV_QPS_INIT};
    EXPECT_TRUE(tag_->setReceiveSequenceNumbers(
        {.nextMsn = static_cast<uint16_t>(cdmsn + 1),
         .nextCsn = kFixtureRxCsn}))
        << "precondition: the receive cases number their work from this base; "
           "a tag that refused it would hand out sequence numbers they never "
           "expect";
  }

  // A tag takes its msn only before RTS.
  void numberTransmitFrom(uint16_t msn) {
    const QpStateGuard inRtr{ibvQpStorage_, IBV_QPS_RTR};
    EXPECT_TRUE(tag_->setNextMsn(msn))
        << "precondition: the transmit cases must accept their starting msn";
  }

  void TearDown() override {
    tag_.reset();
    memoryRegions_.reset();
    cq_.reset();
    buffer_.reset();
  }

  // Puts the work's one frame on the wire and answers it, so the work reports.
  // Posn zero is that frame for a send, and a read's first response.
  void answerTheOnlyFrame(Tag& tag, Wqe& wqe) {
    // CHECK because a helper cannot abandon the case, and carrying on hangs it.
    CHECK_EQ(wqe.getNumPosn(), 1u)
        << "answering posn zero alone leaves work of " << wqe.getNumPosn()
        << " frames short of complete, reporting "
        << "nothing and hanging a case that waits on the CQE";
    std::ignore = wqe.allocateNextPosn();
    if (wqe.hasRequestFrame()) {
      wqe.onPosnReceived(0);
    } else {
      wqe.acknowledgePosn(0);
    }
    auto retirementGuard = tag.lockForWqeRetirement();
    EXPECT_TRUE(tag.handleTxWqesCompletion({wqe}))
        << "precondition: completing the send finds no csn out of sequence";
  }

  void answerTheOnlyFrame(Wqe& wqe) {
    answerTheOnlyFrame(*tag_, wqe);
  }

  static constexpr uint8_t kQpNum = 20;
  static constexpr uint8_t kTagNum = 5;
  // Below every msn the cases use, so each of their messages is still to come.
  static constexpr uint16_t kFixtureCdmsn = 0;
  // Away from zero, so a case must use the base its receives were given.
  static constexpr uint16_t kFixtureRxCsn = 101;
  static constexpr uint32_t kBufferSize = 8192;
  static constexpr uint16_t kMtu = 4096;
  static constexpr uint64_t kWorkReqId = 123;

  std::unique_ptr<CompletionQueue> cq_;
  std::unique_ptr<MemoryRegions> memoryRegions_;
  std::unique_ptr<Tag> tag_;
  std::unique_ptr<std::array<char, kBufferSize>> buffer_;
  struct ibv_pd pd_{nullptr, 1};
  struct ibv_mr* ibvMr_{nullptr};
  struct ibv_qp ibvQpStorage_{};
  struct ibv_qp* ibvQp_{&ibvQpStorage_};
};

TEST_F(TagTest, ACaseStartsWithTheQueuePairRunning) {
  EXPECT_EQ(ibvQp_->state, IBV_QPS_RTS)
      << "a case is handed a running queue pair, as a client's is once its tags "
         "are added, so one building a tag of its own takes the queue pair back "
         "to INIT for the constructor";
}

TEST_F(TagTest, AnErrorNakHoldsTheNextForAnInterval) {
  constexpr uint64_t kFirstNakNs = 5'000'000;
  EXPECT_FALSE(tag_->hasSentErrorNak())
      << "the receive path skips the clock read only for such a tag";
  EXPECT_FALSE(tag_->isErrorNakHeld(kFirstNakNs))
      << "a tag that has sent no error NAK should hold none back";
  tag_->holdErrorNaks(kFirstNakNs);
  EXPECT_TRUE(tag_->hasSentErrorNak())
      << "once it has sent one the receive path must read the clock to "
         "honor the hold";
  EXPECT_TRUE(tag_->isErrorNakHeld(kFirstNakNs + kErrorNakIntervalNs - 1))
      << "an error NAK should hold the next one back for the interval";
  EXPECT_FALSE(tag_->isErrorNakHeld(kFirstNakNs + kErrorNakIntervalNs))
      << "once the interval has passed the next NAK should go out, so a "
         "retransmission is answered if the first NAK was lost";
}

// A queue of one entry under work that reports twice, so the second report
// finds no entry and is dropped.
TEST_F(TagTest, WorkWhoseReportFindsNoEntryStillCompletes) {
  CompletionQueue boundedCq{1};
  const std::unique_ptr<Tag> tag = makeTagInInit(boundedCq, /*sqSigAll=*/true);

  std::vector<Wqe*> posted;
  for (uint64_t i = 0; i < 2; ++i) {
    auto wqe = std::make_unique<SendWqe>(
        kWorkReqId + i, oneSge(buffer_->data(), kMtu), kMtu);
    Wqe* accepted = wqe.get();
    ASSERT_TRUE(postToTag(*tag, std::move(wqe), kMtu))
        << "precondition: work request " << i << " is accepted";
    posted.push_back(accepted);
  }
  // Read before completing: a completed work request is retired, and retiring
  // is the tag erasing the object these point at.
  const uint16_t lastMsn = posted.back()->getMsn().value();
  for (Wqe* wqe : posted) {
    wqe->acknowledgePosn(wqe->allocateNextPosn());
    auto retirementGuard = tag->lockForWqeRetirement();
    EXPECT_TRUE(tag->handleTxWqesCompletion({*wqe}))
        << "precondition: completing the send finds no csn out of sequence";
  }

  EXPECT_EQ(boundedCq.getNumCompletedWqes(), 1)
      << "the queue took the first report and dropped the second, holding the "
         "depth given to ibv_create_cq and no more";
  EXPECT_EQ(tag->getAcceptedSendQueueEntries(), 0)
      << "both work requests gave their send queue entry back, so a client can "
         "post again: a lost report does not hold the queue pair back";
  EXPECT_EQ(tag->getTxCompletedMsn(), lastMsn)
      << "and both took their place in msn order, so the work behind them is "
         "not stuck behind a report that was never made";
}

TEST_F(TagTest, AReorderedArrivalTakesTheBufferThatWasWaiting) {
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/4, /*maxSge=*/1, /*protectionDomain=*/pd_};
  rebuildTagDrawingFrom(sharedQueue);
  constexpr uint64_t kFirstPosted = 700;
  constexpr uint64_t kSecondPosted = 701;
  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = kFirstPosted,
          .sges = oneSge(buffer_->data(), kMtu)}))
      << "precondition: the queue holds the receive posted first";
  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = kSecondPosted,
          .sges = oneSge(buffer_->data() + kMtu, kMtu)}))
      << "precondition: the queue holds the receive posted second";

  // The later csn arrives first.
  constexpr uint16_t kEarlierCsn = kFixtureRxCsn;
  constexpr uint16_t kLaterCsn = kFixtureRxCsn + 1;
  ASSERT_TRUE(
      tag_->assignInitiatorMsnToReceiveWqeIfNeeded(kLaterCsn, /*msn=*/2, kMtu))
      << "precondition: a message finds a buffer whatever csn it carries, or "
         "the tag would answer it with an RNR while the queue holds two";
  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kEarlierCsn, /*msn=*/1, kMtu))
      << "precondition: the message behind it finds the buffer still waiting";

  const Wqe* const secondSent =
      tag_->getReceiveWqe(BaseHeader::Opcode::OperationType::SEND, /*msn=*/2);
  const Wqe* const firstSent =
      tag_->getReceiveWqe(BaseHeader::Opcode::OperationType::SEND, /*msn=*/1);
  ASSERT_NE(secondSent, nullptr);
  ASSERT_NE(firstSent, nullptr);

  ASSERT_TRUE(secondSent->getWorkReqId().has_value())
      << "the message that arrived first must carry the id of the receive "
         "it took, or it got none from the shared queue at all";
  ASSERT_TRUE(firstSent->getWorkReqId().has_value())
      << "the message behind it must carry one too, or the shared queue "
         "served only the first of the two";

  EXPECT_EQ(*secondSent->getWorkReqId(), kFirstPosted)
      << "the message that arrived first took the buffer that had been "
         "waiting longest, since a shared queue cannot hold that buffer back "
         "for a csn another queue pair may claim";
  EXPECT_EQ(*firstSent->getWorkReqId(), kSecondPosted)
      << "the message behind it took the next buffer, so both are served; a "
         "client reads which buffer carried which message from the work "
         "request id its completion carries";
}

// An address-invalid write is already bound for an address-error NAK, so a
// refused take leaves no waiter: the sender was never told to back off.
TEST_F(TagTest, AWriteToABadAddressRefusedByASharedQueueRecordsNoTag) {
  std::vector<uint8_t> wokenTags;
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2,
      /*maxSge=*/1,
      pd_,
      /*reportLimitReached=*/{},
      [&wokenTags](SharedReceiveTaker refused, uint16_t) {
        wokenTags.push_back(refused.tagNum);
        return true;
      }};
  rebuildTagDrawingFrom(sharedQueue);

  ReadWriteHeader rwHeader(0x12345678, 999, kMtu); // Invalid address and key
  int addressError = -1;
  tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 1)
      << "precondition: the address does not resolve, so the responder replies "
         "to this write with an address-error NAK rather than a BRNR";

  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = kWorkReqId, .sges = oneSge(buffer_->data(), kMtu)}))
      << "precondition: a receive arrives for any waiter the refusal left";

  EXPECT_TRUE(wokenTags.empty())
      << "the write produced no BRNR, so a cancel here spends the receive on a "
         "tag that is not backing off";
}

// The write overload reaches the same empty-queue miss as a plain send.
TEST_F(TagTest, AWriteRefusedByASharedQueueRecordsItsTagThere) {
  std::vector<uint8_t> wokenTags;
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2,
      /*maxSge=*/1,
      pd_,
      /*reportLimitReached=*/{},
      [&wokenTags](SharedReceiveTaker refused, uint16_t) {
        wokenTags.push_back(refused.tagNum);
        return true;
      }};
  rebuildTagDrawingFrom(sharedQueue);

  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, kMtu);
  int addressError = -1;
  tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 0)
      << "precondition: the address resolves, so what follows is the empty "
         "queue refusing the take";

  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = kWorkReqId, .sges = oneSge(buffer_->data(), kMtu)}))
      << "precondition: a receive arrives for whatever waiter was recorded";

  EXPECT_EQ(wokenTags, (std::vector<uint8_t>{kTagNum}))
      << "the write's refusal recorded its waiter on the queue, so the queue "
         "is what cancels for this tag when a receive arrives";
}

TEST_F(TagTest, ACsnRefusedByAnEmptyQueueTakesTheBufferPostedAfterIt) {
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2, /*maxSge=*/1, /*protectionDomain=*/pd_};
  rebuildTagDrawingFrom(sharedQueue);

  ASSERT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, kMtu))
      << "precondition: an empty queue binds no receive, which is what the "
         "tag answers with an RNR";

  constexpr uint64_t kPostedAfter = 900;
  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = kPostedAfter,
          .sges = oneSge(buffer_->data(), kMtu)}))
      << "precondition: the client posts a receive the retransmission can take";

  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, kMtu))
      << "the retransmitted message must bind the receive posted since, or "
         "the refusal left the csn holding a place no buffer can reach";
  const Wqe* const bound =
      tag_->getReceiveWqe(BaseHeader::Opcode::OperationType::SEND, /*msn=*/1);
  ASSERT_NE(bound, nullptr);
  ASSERT_TRUE(bound->getWorkReqId().has_value())
      << "the bound receive must carry the id of the buffer it took, or the "
         "tag bound a place the refusal left rather than a real receive";
  EXPECT_EQ(*bound->getWorkReqId(), kPostedAfter)
      << "and it must be the buffer posted after the refusal, which is the "
         "only one the client ever gave this queue";
}

TEST_F(TagTest, ASharedQueueTagReservesNoReceiveDepthOfItsOwn) {
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2, /*maxSge=*/1, /*protectionDomain=*/pd_};
  rebuildTagDrawingFrom(sharedQueue);

  EXPECT_FALSE(tag_->tryReserveReceiveQueueEntry())
      << "a tag drawing from a shared queue refuses the reservation, so a "
         "receive posted to it is turned away rather than counted";
  EXPECT_EQ(tag_->getAcceptedReceiveQueueEntries(), 0)
      << "and its depth is untouched, so the entry the refusal withheld is "
         "not one a release has to give back";
}

TEST_F(TagTest, ASharedQueueTagRecordsNoRnrCancelRequestOfItsOwn) {
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2, /*maxSge=*/1, /*protectionDomain=*/pd_};
  rebuildTagDrawingFrom(sharedQueue);

  ASSERT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, kMtu))
      << "without a shared-queue miss, this test would not reach the path "
         "that must record no dedicated cancel state";

  EXPECT_FALSE(tag_->processReceiveWqe(
                       std::make_unique<ReceiveWqe>(
                           kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu),
                       kMtu)
                   .has_value())
      << "recording a dedicated cancel request for a shared-queue miss would "
         "leave no post of this tag's own that can clear it";
}

TEST_F(TagTest, AFailingTagLeavesTheSharedQueueAlone) {
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2, /*maxSge=*/1, /*protectionDomain=*/pd_};
  rebuildTagDrawingFrom(sharedQueue);
  constexpr uint64_t kUntouched = 910;
  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = kUntouched, .sges = oneSge(buffer_->data(), kMtu)}))
      << "precondition: the queue holds a receive the failing tag could take";

  ibvQp_->state = IBV_QPS_ERR;
  ASSERT_TRUE(tag_->isInErrorState())
      << "precondition: the tag reads as failing, or the draw below is the "
         "ordinary one rather than the case under test";

  EXPECT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, kMtu))
      << "a failing tag binds no receive, since work bound to it is flushed "
         "rather than reported";
  EXPECT_EQ(sharedQueue.getNumPosted(), 1u)
      << "and the buffer stays posted for the queue pairs still running";
}

TEST_F(TagTest, ARefusedTagIsTheOneTheNextReceiveCancelsFor) {
  std::vector<std::pair<uint32_t, uint8_t>> canceledTags;
  std::vector<uint16_t> wokenCsns;
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2,
      /*maxSge=*/1,
      /*protectionDomain=*/pd_,
      /*reportLimitReached=*/{},
      [&canceledTags, &wokenCsns](SharedReceiveTaker refused, uint16_t csn) {
        canceledTags.emplace_back(refused.qpNum, refused.tagNum);
        wokenCsns.push_back(csn);
        return true;
      }};
  rebuildTagDrawingFrom(sharedQueue);

  // One csn short of the left edge, so the message ahead needs the only
  // receive and this one is refused.
  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = 900, .sges = oneSge(buffer_->data(), kMtu)}))
      << "precondition: the queue holds the receive the csn ahead needs";
  ASSERT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn + 1, /*msn=*/2, kMtu))
      << "precondition: this message is refused, which is what the responder "
         "reports with BRNR";

  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = 901, .sges = oneSge(buffer_->data() + kMtu, kMtu)}));
  EXPECT_EQ(
      canceledTags,
      (std::vector<std::pair<uint32_t, uint8_t>>{{kQpNum, kTagNum}}))
      << "the queue cancels for the tag that was refused, so the initiator "
         "backed off on that tag is the one that resumes";
  EXPECT_EQ(wokenCsns, (std::vector<uint16_t>{kFixtureRxCsn}))
      << "the csn recorded is the first the tag has not completed, which is "
         "what the cancel tells the initiator it may send from";
}

TEST_F(TagTest, ABrnrSuppressedTakeRecordsNoTag) {
  std::vector<std::pair<uint32_t, uint8_t>> canceledTags;
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2,
      /*maxSge=*/1,
      /*protectionDomain=*/pd_,
      /*reportLimitReached=*/{},
      [&canceledTags](SharedReceiveTaker refused, uint16_t) {
        canceledTags.emplace_back(refused.qpNum, refused.tagNum);
        return true;
      }};
  rebuildTagDrawingFrom(sharedQueue);

  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = 900, .sges = oneSge(buffer_->data(), kMtu)}))
      << "precondition: the queue holds the receive the csn ahead needs";
  {
    const gflags::FlagSaver saver;
    FLAGS_libsoftmetaroce_dont_send_brnr = true;
    ASSERT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
        kFixtureRxCsn + 1, /*msn=*/2, kMtu))
        << "precondition: this message is refused while BRNR is suppressed";
  }

  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = 901, .sges = oneSge(buffer_->data() + kMtu, kMtu)}))
      << "precondition: a second receive arrives after the refusal";
  EXPECT_TRUE(canceledTags.empty())
      << "the suppressed refusal recorded no waiter, so the post cancels for "
         "none";

  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn + 1, /*msn=*/2, kMtu))
      << "suppression drops only the record, so the retry binds the receive "
         "posted since";
}

TEST_F(TagTest, AMessageIsRefusedUnlessItLeavesTheCsnsAheadTheirReceives) {
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2, /*maxSge=*/1, /*protectionDomain=*/pd_};
  rebuildTagDrawingFrom(sharedQueue);
  for (uint64_t i = 0; i < 2; ++i) {
    ASSERT_TRUE(sharedQueue.tryPost(
        SharedReceive{
            .workRequestId = 900 + i,
            .sges = oneSge(buffer_->data() + i * kMtu, kMtu)}))
        << "precondition: the queue holds receive " << i;
  }

  // Two csns past the window's left edge, so the two csns ahead need receives
  // first.
  EXPECT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn + 2, /*msn=*/3, kMtu))
      << "the queue holds only what the two csns ahead need, so this message "
         "takes none of it and the responder refuses it with BRNR";
  EXPECT_EQ(sharedQueue.getNumPosted(), 2u)
      << "both receives stay, which is what keeps the two messages ahead from "
         "arriving to an empty queue and stranding this one behind them";

  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, kMtu))
      << "the message at the left edge waits on no receive of its own, so it "
         "takes even when the queue is down to what it needs";
  EXPECT_EQ(sharedQueue.getNumPosted(), 1u)
      << "the message at the left edge took one receive and left the one the "
         "next csn still needs";
}

TEST_F(TagTest, AReceiveCompletingOnASkippedCsnIsRefusedThenFlushed) {
  SharedReceiveQueue sharedQueue{/*maxWorkRequests=*/4, /*maxSge=*/1, pd_};
  rebuildTagDrawingFrom(sharedQueue);
  for (uint64_t i = 0; i < 4; ++i) {
    ASSERT_TRUE(sharedQueue.tryPost(
        SharedReceive{
            .workRequestId = 950 + i, .sges = oneSge(buffer_->data(), kMtu)}))
        << "precondition: the queue holds receive " << i;
  }

  tag_->extendCdmsnTrackerIfNeeded(/*msn=*/1);
  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, kMtu))
      << "precondition: the first message binds on the csn at the left edge";
  tag_->extendCdmsnTrackerIfNeeded(/*msn=*/2);
  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn + 2, /*msn=*/2, kMtu))
      << "precondition: the message behind it binds, two csns on from the "
         "edge rather than the next one";

  EXPECT_TRUE(tag_->markDeliveredAndCompleteInOrder(/*msn=*/1))
      << "the first receive completes on the csn the edge expects, so the "
         "edge runs over it";
  EXPECT_FALSE(tag_->markDeliveredAndCompleteInOrder(/*msn=*/2))
      << "the peer skipped a csn, so the responder refuses rather than moving "
         "the edge past a receive that never completed; its caller fails the "
         "tag on that refusal";
  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "only the receive that completed in sequence has reported, so the "
         "refused one has not yet reported a completion";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushPendingReceiveWqesInError();
  }

  EXPECT_EQ(cq_->getNumCompletedWqes(), 2)
      << "the flush the failed tag brings reports the refused receive, so the "
         "buffer it took from the shared queue returns to the client";
}

TEST_F(TagTest, AnAckedVirtualSendOverASkippedCsnReportsTheRefusal) {
  SharedReceiveQueue sharedQueue{/*maxWorkRequests=*/4, /*maxSge=*/1, pd_};
  rebuildTagDrawingFrom(sharedQueue);
  for (uint64_t i = 0; i < 4; ++i) {
    ASSERT_TRUE(sharedQueue.tryPost(
        SharedReceive{
            .workRequestId = 960 + i, .sges = oneSge(buffer_->data(), kMtu)}))
        << "precondition: the queue holds receive " << i;
  }

  tag_->extendCdmsnTrackerIfNeeded(/*msn=*/1);
  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, kMtu))
      << "precondition: the first message binds on the csn at the left edge";
  tag_->extendCdmsnTrackerIfNeeded(/*msn=*/2);
  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn + 2, /*msn=*/2, kMtu))
      << "precondition: the message the send below releases skipped a csn";
  ASSERT_TRUE(tag_->markDeliveredAndCompleteInOrder(/*msn=*/1))
      << "precondition: the edge reaches the msn the send holds";

  // The responder's send sits on the same initiator msn as that receive, so
  // acking it is what runs delivery over the skipped csn.
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, kMtu);
  int addressError = -1;
  tag_->createVirtualSendWqeIfNeeded(
      /*msn=*/2, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 0)
      << "precondition: the region resolves, or no send is built to ack";
  Wqe* const virtualWqe = tag_->findNextWqe();
  ASSERT_NE(virtualWqe, nullptr);
  ASSERT_TRUE(virtualWqe->isVirtual())
      << "precondition: the responder's own send is the one acked below";
  tag_->markFree();

  const size_t reportedBefore = cq_->getNumCompletedWqes();
  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    EXPECT_FALSE(tag_->handleTxWqesCompletion({*virtualWqe}))
        << "the ack ran delivery over a receive numbered out of sequence, and "
           "reporting that is what lets the sender fail the tag rather than "
           "leave the receive with no path left to complete it";
  }
  ASSERT_EQ(cq_->getNumCompletedWqes(), reportedBefore)
      << "the refused receive has not reported, so the client has yet to "
         "receive a completion for it";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushPendingReceiveWqesInError();
  }

  EXPECT_EQ(cq_->getNumCompletedWqes(), reportedBefore + 1)
      << "the flush the sender's refusal brings reports it, the only path "
         "left: delivery has already run past that msn";
}

TEST_F(TagTest, AWriteWithImmediateToABadAddressStillTakesABuffer) {
  SharedReceiveQueue sharedQueue{
      /*maxWorkRequests=*/2, /*maxSge=*/1, /*protectionDomain=*/pd_};
  rebuildTagDrawingFrom(sharedQueue);
  constexpr uint64_t kTakenId = 800;
  ASSERT_TRUE(sharedQueue.tryPost(
      SharedReceive{
          .workRequestId = kTakenId, .sges = oneSge(buffer_->data(), kMtu)}))
      << "precondition: the queue holds the receive the immediate consumes";

  ReadWriteHeader rwHeader(0x12345678, 999, 1000); // Invalid address and key
  int addressError = -1;
  tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn,
      /*msn=*/1,
      rwHeader,
      &addressError,
      *memoryRegions_,
      kMtu);

  EXPECT_EQ(addressError, 1)
      << "the remote address does not validate, which the responder answers "
         "with an address error";
  EXPECT_EQ(sharedQueue.getNumPosted(), 0u)
      << "a write with immediate reports on a posted receive whether or not "
         "its address validates, so the tag takes one from the shared queue "
         "rather than leaving the client without a completion";
  EXPECT_EQ(tag_->getNumPendingRWqes(), 1)
      << "the receive it took is the tag's to report, carrying the protection "
         "error the flush gives it in the order the client posted";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushPendingReceiveWqesInError();
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "the flush must report the receive it took, or the write consumed a "
         "buffer the client never hears about again";
  const auto cqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe.workReqId, kTakenId)
      << "the completion must carry the work request id of the receive the "
         "write took, so the client can tell which one it spent";
  EXPECT_EQ(cqe.status, kLocalProtErr)
      << "it must keep the protection error the bad address raised rather "
         "than reporting a plain flush";
}

TEST_F(TagTest, AReceiveStrandedAcrossTheCsnSpaceFailsTheTagNotTheProcess) {
  constexpr uint64_t kStranded = 500;
  constexpr uint64_t kColliding = 501;
  postToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kStranded, oneSge(buffer_->data(), kMtu), kMtu),
      kMtu);
  ASSERT_EQ(tag_->getNumPendingRWqes(), 1)
      << "precondition: the receive the peer never answers is posted";

  {
    const QpStateGuard inInit{ibvQpStorage_, IBV_QPS_INIT};
    ASSERT_TRUE(tag_->setReceiveSequenceNumbers(
        {.nextMsn = kFixtureCdmsn + 1, .nextCsn = kFixtureRxCsn}))
        << "precondition: the counter is back at the stranded receive's csn";
  }

  postToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kColliding, oneSge(buffer_->data(), kMtu), kMtu),
      kMtu);

  EXPECT_TRUE(tag_->isInErrorState())
      << "a peer that strands a csn must cost its own connection and no other";
  EXPECT_EQ(tag_->getNumPendingRWqes(), 0)
      << "the tag holds no receive it could still answer";
  ASSERT_EQ(cq_->getNumCompletedWqes(), 2)
      << "both the stranded receive and the one that collided with it must "
         "report, or the client waits forever on buffers it still owns";
  const auto first = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(first.workReqId, kStranded)
      << "the receive the tag was holding reports before the post that found "
         "its csn taken";
  EXPECT_EQ(first.status, kWrFlushErr)
      << "the stranded receive carried no error of its own: the tag ended "
         "under it, so it is flushed rather than blamed";
  const auto second = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(second.workReqId, kColliding)
      << "the post that found the collision reports after the receive the tag "
         "was already holding";
  EXPECT_EQ(second.status, kWrFlushErr)
      << "it was never admitted, so the client must be told its buffer is "
         "free rather than left believing the receive is armed";
}

TEST_F(TagTest, ARefusedMessageRecordsAnRnrCancelRequestTheNextPostClears) {
  ASSERT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn,
      /*msn=*/1,
      kMtu))
      << "without a receive miss, this test would not exercise RNR-Cancel "
         "request recording";

  const auto cancelCsn = postReceiveToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu),
      kMtu);
  ASSERT_TRUE(cancelCsn.has_value())
      << "without an RNR-Cancel, the available receive would leave the "
         "initiator backed off until its timer expires";
  EXPECT_EQ(*cancelCsn, kFixtureRxCsn)
      << "the RNR-Cancel must carry the csn of the receive that became "
         "available, or it covers a message the tag still has no receive for";
}

TEST_F(TagTest, AReceivePostedBeforeTheMessageRecordsNoRnrCancelRequest) {
  const auto cancelCsn = postReceiveToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu),
      kMtu);
  EXPECT_FALSE(cancelCsn.has_value())
      << "a cancel before any refusal would claim that an initiator had "
         "entered BRNR backoff";

  EXPECT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn,
      /*msn=*/1,
      kMtu))
      << "failing to bind would produce BRNR despite an available receive";

  const auto laterCsn = postReceiveToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId + 1, oneSge(buffer_->data(), kMtu), kMtu),
      kMtu);
  EXPECT_FALSE(laterCsn.has_value())
      << "a later cancel would claim a refusal occurred even though the "
         "message bound";
}

TEST_F(TagTest, AMessageThatSendsNoBrnrRecordsNoRnrCancelRequest) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_dont_send_brnr = true;

  ASSERT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn,
      /*msn=*/1,
      kMtu))
      << "without a receive miss, this test would not exercise the no-BRNR "
         "compatibility path";

  const auto cancelCsn = postReceiveToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu),
      kMtu);
  EXPECT_FALSE(cancelCsn.has_value())
      << "a responder that sends no BRNR leaves its initiator with no backoff "
         "for a cancel to lift";
  EXPECT_EQ(tag_->getNumPendingRWqes(), 1)
      << "the receive must remain posted because no message adopted it";
}

TEST_F(TagTest, AWriteWithImmediateToABadAddressRecordsNoRnrCancelRequest) {
  ReadWriteHeader rwHeader(0x12345678, 999, 1000); // Invalid address and key
  int addressError = -1;
  tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 1)
      << "precondition: the write is refused for its address, with the receive "
         "queue empty so the bind missed as well";

  const auto cancelCsn = postReceiveToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu),
      kMtu);
  EXPECT_FALSE(cancelCsn.has_value())
      << "an address-error NAK starts no BRNR backoff, so an RNR-Cancel "
         "would misreport the outcome";
}

TEST_F(TagTest, AReceiveFlushedAfterAMissSchedulesNoRnrCancel) {
  ASSERT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn,
      /*msn=*/1,
      kMtu))
      << "without a recorded request, this test could not show that flushing "
         "a receive schedules no cancel";

  ibvQp_->state = IBV_QPS_ERR;
  ASSERT_TRUE(tag_->isInErrorState())
      << "precondition: the receive below is flushed rather than adopted";

  const auto cancelCsn = postReceiveToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu),
      kMtu);
  EXPECT_FALSE(cancelCsn.has_value())
      << "a flushed receive is never adopted, so lifting backoff would leave "
         "the tag without a receive for the retried message";
}

TEST_F(TagTest, OneRefusalMakesOnlyTheFirstReceivePostCancel) {
  ASSERT_FALSE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn,
      /*msn=*/1,
      kMtu))
      << "without one recorded RNR-Cancel request, this test could not prove "
         "that only one later post clears it";

  EXPECT_TRUE(postReceiveToTag(
                  *tag_,
                  std::make_unique<ReceiveWqe>(
                      kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu),
                  kMtu)
                  .has_value())
      << "without clearing the RNR-Cancel request, the first available "
         "receive would leave the initiator backed off";
  EXPECT_FALSE(postReceiveToTag(
                   *tag_,
                   std::make_unique<ReceiveWqe>(
                       kWorkReqId + 1, oneSge(buffer_->data(), kMtu), kMtu),
                   kMtu)
                   .has_value())
      << "the request is cleared once, so a second cancel here would lift a "
         "backoff the first cancel already lifted";
}

TEST_F(TagTest, AReceiveMissRacingAPostEitherBindsOrCancels) {
  constexpr int kRounds = 1000;
  for (int round = 0; round < kRounds; ++round) {
    tag_ = makeTagInInit(*cq_);
    configureReceiveSequencing(kFixtureCdmsn);

    std::atomic<int> atTheLine{0};
    auto waitForTheOther = [&atTheLine] {
      atTheLine.fetch_add(1, std::memory_order_acq_rel);
      while (atTheLine.load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
      }
    };

    std::atomic<bool> bound{false};
    std::optional<uint16_t> cancelCsn;
    std::thread responder([&] {
      waitForTheOther();
      bound.store(
          tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
              kFixtureRxCsn,
              /*msn=*/1,
              kMtu),
          std::memory_order_release);
    });
    std::thread poster([&] {
      waitForTheOther();
      cancelCsn = postReceiveToTag(
          *tag_,
          std::make_unique<ReceiveWqe>(
              kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu),
          kMtu);
    });
    responder.join();
    poster.join();

    ASSERT_NE(bound.load(std::memory_order_acquire), cancelCsn.has_value())
        << "round " << round
        << ": the race must either bind the receive or produce its cancel. "
           "Neither leaves the initiator backed off with a receive available; "
           "both send a cancel although a successful bind produced no BRNR";
  }
}

TEST_F(TagTest, BrnrStateManagement) {
  EXPECT_EQ(tag_->getBrnrState(), BrnrState::NORMAL_TX)
      << "a tag starts in normal transmission, with no msn blocked";
  EXPECT_FALSE(tag_->getBrnrFirstBlockedMsn().has_value())
      << "normal transmission carries no blocked msn";

  tag_->brnr().blockAt(10);
  EXPECT_EQ(tag_->getBrnrState(), BrnrState::BRNR_BLOCKED_TX)
      << "blocking at an msn should put the tag in backoff";
  ASSERT_TRUE(tag_->getBrnrFirstBlockedMsn().has_value())
      << "the tag should record the msn the receiver was not ready for";
  EXPECT_EQ(tag_->getBrnrFirstBlockedMsn().value(), 10)
      << "the recorded msn should be the one the block was given";

  EXPECT_TRUE(tag_->isBlockedMsn(10))
      << "the block covers the msn it started at";
  EXPECT_TRUE(tag_->isBlockedMsn(11))
      << "the block covers every msn after the first blocked one";
  EXPECT_FALSE(tag_->isBlockedMsn(9))
      << "an msn before the first blocked one is not covered";

  tag_->brnr().resume();
  EXPECT_EQ(tag_->getBrnrState(), BrnrState::NORMAL_TX)
      << "resuming should return the tag to normal transmission";
  EXPECT_FALSE(tag_->getBrnrFirstBlockedMsn().has_value())
      << "normal transmission carries no blocked msn, so resuming clears it";
  EXPECT_FALSE(tag_->isBlockedMsn(10))
      << "no msn is blocked once the episode is over";
}

// Test processing SendWqe
TEST_F(TagTest, ProcessSendWqe) {
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);

  EXPECT_FALSE(sendWqe->getMsn().has_value());
  EXPECT_FALSE(sendWqe->getCsn().has_value());

  auto result = postToTag(*tag_, std::move(sendWqe), kMtu);

  EXPECT_TRUE(result); // Send WQEs return true
  EXPECT_EQ(tag_->getNumPendingWqes(), 1);
  EXPECT_EQ(tag_->getNextMsn(), 2); // MSN should be incremented
}

// Test processing SendWithImmWqe
TEST_F(TagTest, ProcessSendWithImmWqe) {
  auto sendWithImmWqe = std::make_unique<SendWithImmWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu, 0xDEADBEEF);

  auto result = postToTag(*tag_, std::move(sendWithImmWqe), kMtu);

  EXPECT_TRUE(result); // Send WQEs return true
  EXPECT_EQ(tag_->getNumPendingWqes(), 1);
  EXPECT_EQ(tag_->getNextMsn(), 2); // MSN should be incremented
}

// Test processing ReceiveWqe
TEST_F(TagTest, ProcessReceiveWqe) {
  auto receiveWqe = std::make_unique<ReceiveWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);

  auto result = postToTag(*tag_, std::move(receiveWqe), kMtu);

  EXPECT_FALSE(result); // Receive WQEs return false
  EXPECT_EQ(tag_->getNumPendingRWqes(), 1);
  EXPECT_EQ(
      tag_->getNumPendingWqes(), 0); // Receive WQEs don't count as pending WQEs
}

// Test processing ReadWqe
TEST_F(TagTest, ProcessReadWqe) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), 1000),
      kMtu,
      0x1000,
      ibvMr_->rkey);

  auto result = postToTag(*tag_, std::move(readWqe), kMtu);

  EXPECT_TRUE(result); // Read WQEs return true
  EXPECT_EQ(tag_->getNumPendingWqes(), 1);
  EXPECT_EQ(tag_->getNextMsn(), 2); // MSN should be incremented
}

// Test processing WriteWqe
TEST_F(TagTest, ProcessWriteWqe) {
  auto writeWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kWrite,
      kWorkReqId,
      oneSge(buffer_->data(), 1000),
      kMtu,
      0x2000,
      ibvMr_->rkey);

  auto result = postToTag(*tag_, std::move(writeWqe), kMtu);

  EXPECT_TRUE(result); // Write WQEs return true
  EXPECT_EQ(tag_->getNumPendingWqes(), 1);
  EXPECT_EQ(tag_->getNextMsn(), 2); // MSN should be incremented
}

// Test processing WriteWithImmWqe
TEST_F(TagTest, ProcessWriteWithImmWqe) {
  auto writeWithImmWqe = std::make_unique<WriteWithImmWqe>(
      kWorkReqId,
      oneSge(buffer_->data(), 1000),
      kMtu,
      0x3000,
      ibvMr_->rkey,
      0xCAFEBABE);

  auto result = postToTag(*tag_, std::move(writeWithImmWqe), kMtu);

  EXPECT_TRUE(result); // Write WQEs return true
  EXPECT_EQ(tag_->getNumPendingWqes(), 1);
  EXPECT_EQ(tag_->getNextMsn(), 2); // MSN should be incremented
}

// Test multiple WQE processing
TEST_F(TagTest, ProcessMultipleWqes) {
  // Add several WQEs
  auto sendWqe1 = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  auto sendWqe2 = std::make_unique<SendWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 2000), kMtu);
  auto receiveWqe = std::make_unique<ReceiveWqe>(
      kWorkReqId + 2, oneSge(buffer_->data(), 1500), kMtu);

  postToTag(*tag_, std::move(sendWqe1), kMtu);
  postToTag(*tag_, std::move(sendWqe2), kMtu);
  postToTag(*tag_, std::move(receiveWqe), kMtu);

  EXPECT_EQ(tag_->getNumPendingWqes(), 2); // 2 send WQEs
  EXPECT_EQ(tag_->getNumPendingRWqes(), 1); // 1 receive WQE
  EXPECT_EQ(tag_->getNextMsn(), 3); // MSN incremented twice
}

/*
 * Destroying a tagged QP half-closes its Tag. It takes no new work, but a
 * message it had already started sending has to be finished: retransmission
 * only resends what is already outstanding, so without this the unsent rest of
 * that message never reaches the wire, the work request never completes, and
 * the peer is left holding a receive it can never fill.
 */
TEST_F(TagTest, AHalfClosedTagFinishesAMessageItStarted) {
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 2 * kMtu), kMtu);
  Wqe* accepted = sendWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(sendWqe), kMtu));

  // Its first packet is on the wire when the client destroys the tagged QP.
  const uint32_t firstPosn = accepted->allocateNextPosn();
  ASSERT_EQ(firstPosn, 0u);
  tag_->markForDeletion();

  Wqe* found = tag_->findNextWqe();
  ASSERT_EQ(found, accepted)
      << "a half-closed tag must still hand over the message it had started, "
         "which is the only way the rest of it reaches the wire";
  const uint32_t secondPosn = found->allocateNextPosn();
  tag_->markFree();
  EXPECT_EQ(secondPosn, 1u)
      << "and the packet it hands over must be the one still unsent";

  accepted->acknowledgePosn(firstPosn);
  accepted->acknowledgePosn(secondPosn);
  ASSERT_EQ(accepted->getOutstandingPosnCount(), 0u)
      << "both packets are accounted for once the peer acknowledges them";
  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    EXPECT_TRUE(tag_->handleTxWqesCompletion({*accepted}))
        << "precondition: completing the send finds no csn out of sequence";
  }
  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "so the message completes rather than waiting for the whole QP to be "
         "torn down";
  const auto cqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe.status, kSuccess)
      << "and it reports the success it earned: every packet reached the peer";
}

/*
 * The other half of half-closed: work posted after the destroy is refused, and
 * reports rather than vanishing.
 */
TEST_F(TagTest, AHalfClosedTagRefusesNewWork) {
  tag_->markForDeletion();

  auto posted = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu);
  EXPECT_FALSE(postToTag(*tag_, std::move(posted), kMtu))
      << "a half-closed tag takes no new work";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "and the refused work request must report rather than vanish";
}

// Test findNextWqe functionality
TEST_F(TagTest, FindNextWqe) {
  // Initially no WQEs
  EXPECT_EQ(tag_->findNextWqe(), nullptr);

  // Add a send WQE
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  postToTag(*tag_, std::move(sendWqe), kMtu);

  // Should find the WQE
  Wqe* foundWqe = tag_->findNextWqe();
  EXPECT_NE(foundWqe, nullptr);
  EXPECT_EQ(foundWqe->getWorkType(), Wqe::WorkType::kSend);
  tag_->markFree();
}

// Test findNextWqe with blocked MSN
TEST_F(TagTest, FindNextWqeWithBlockedMsn) {
  // Add a send WQE (will get MSN 1)
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  postToTag(*tag_, std::move(sendWqe), kMtu);

  tag_->brnr().blockAt(1);

  EXPECT_EQ(tag_->findNextWqe(), nullptr)
      << "a tag in backoff should offer no work for the msn the receiver is "
         "not ready for";

  tag_->brnr().beginProbe();
  Wqe* foundWqe = tag_->findNextWqe();
  EXPECT_NE(foundWqe, nullptr)
      << "a probing tag should offer the blocked msn's work as its one probe";
  tag_->markFree();

  EXPECT_EQ(tag_->getBrnrState(), BrnrState::BRNR_PROBING)
      << "finding the blocked msn's request must not send it; without a "
         "built packet the backoff stays probing";
}

TEST_F(TagTest, ProbeCommitTransitionsOnlyForBuiltBlockedProbe) {
  // One send WQE (msn 1).
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  postToTag(*tag_, std::move(sendWqe), kMtu);

  // Outside an episode there is no probe to record.
  Wqe* foundWqe = tag_->findNextWqe();
  ASSERT_NE(foundWqe, nullptr)
      << "precondition: the tag has to return its unblocked request, or "
         "the commit below has nothing to judge";
  tag_->markProbeSentIfBlockedMsn(*foundWqe);
  EXPECT_EQ(tag_->getBrnrState(), BrnrState::NORMAL_TX)
      << "outside an episode, recording a built packet must not move the "
         "backoff state";
  tag_->markFree();

  // Blocked and probing: the blocked request is the probe.
  tag_->brnr().blockAt(1);
  tag_->brnr().beginProbe();
  foundWqe = tag_->findNextWqe();
  ASSERT_NE(foundWqe, nullptr)
      << "precondition: the probing tag has to return the blocked msn's "
         "work, or the commit below has no probe to record";
  tag_->markProbeSentIfBlockedMsn(*foundWqe);
  EXPECT_EQ(tag_->getBrnrState(), BrnrState::BRNR_PROBE_SENT)
      << "a built packet from the probing tag's blocked msn is the probe "
         "itself, so committing it must move the backoff to PROBE_SENT";
  tag_->markFree();

  // A virtual response shares the msn space but is responder work, not new
  // work, so it must never count as a probe.
  tag_->brnr().blockAt(2);
  tag_->brnr().beginProbe();
  VirtualSendWqe virtualWqe(5, buffer_->data(), 100, kMtu);
  tag_->markProbeSentIfBlockedMsn(virtualWqe);
  EXPECT_EQ(tag_->getBrnrState(), BrnrState::BRNR_PROBING)
      << "a built responder message must not move the backoff even when its "
         "msn sits inside the blocked range";

  // Resume clears the blocked range, so re-blocking past msn 1 leaves msn 1
  // unblocked while probing again.
  tag_->brnr().resume();
  tag_->brnr().blockAt(5);
  tag_->brnr().beginProbe();
  foundWqe = tag_->findNextWqe();
  ASSERT_NE(foundWqe, nullptr)
      << "precondition: the unblocked msn still has to be returned, or the "
         "commit below proves nothing about ordinary work";
  tag_->markProbeSentIfBlockedMsn(*foundWqe);
  EXPECT_EQ(tag_->getBrnrState(), BrnrState::BRNR_PROBING)
      << "an ordinary request built while probing is no probe, so "
         "committing it must not move the backoff";
  tag_->markFree();
}

// Test extendCdmsnTrackerIfNeeded
TEST_F(TagTest, ExtendCdmsnTracker) {
  EXPECT_EQ(tag_->getCDMSN(), 0);

  // Extend tracker for MSN 5
  tag_->extendCdmsnTrackerIfNeeded(5);

  // Extend tracker for MSN 10 (should extend further)
  tag_->extendCdmsnTrackerIfNeeded(10);

  // No exceptions should be thrown
  SUCCEED();
}

// Test assignInitiatorMsnToReceiveWqeIfNeeded
TEST_F(TagTest, AssignInitiatorMsnToReceiveWqe) {
  // Add a receive WQE first
  auto receiveWqe = std::make_unique<ReceiveWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  postToTag(*tag_, std::move(receiveWqe), kMtu);

  EXPECT_EQ(tag_->getNumPendingRWqes(), 1);

  // Try to assign initiator MSN (CSN 0 should exist)
  tag_->assignInitiatorMsnToReceiveWqeIfNeeded(0, 100, kMtu);

  // The WQE should be moved from pendingReceives_ to
  // initiatorMsnToResponderWqes_
  EXPECT_EQ(tag_->getNumPendingRWqes(), 1); // Total count unchanged

  // Trying to assign same MSN again should do nothing
  tag_->assignInitiatorMsnToReceiveWqeIfNeeded(0, 100, kMtu);
  EXPECT_EQ(tag_->getNumPendingRWqes(), 1);
}

// Test error handling for unsupported WQE types
TEST_F(TagTest, UnsupportedWqeType) {
  // Create a VirtualReceiveWqe (which should not be processed through
  // processTransmitWqe)
  auto virtualWqe =
      std::make_unique<VirtualReceiveWqe>(1, buffer_->data(), 1000, kMtu);

  EXPECT_THROW(
      postToTag(*tag_, std::move(virtualWqe), kMtu), std::runtime_error);
}

// Test createVirtualRWqeIfNeeded with valid memory region
TEST_F(TagTest, CreateVirtualRWqeValid) {
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 1000);

  int addressError = -1;
  tag_->createVirtualRWqeIfNeeded(
      2, rwHeader, &addressError, *memoryRegions_, kMtu);

  EXPECT_EQ(addressError, 0); // No error
  EXPECT_EQ(tag_->getNumPendingRWqes(), 1); // Virtual RWqe created
}

// Test createVirtualRWqeIfNeeded with invalid memory region
TEST_F(TagTest, CreateVirtualRWqeInvalid) {
  ReadWriteHeader rwHeader(0x12345678, 999, 1000); // Invalid address and key

  int addressError = -1;
  tag_->createVirtualRWqeIfNeeded(
      2, rwHeader, &addressError, *memoryRegions_, kMtu);

  EXPECT_EQ(addressError, 1); // Error should be set
  EXPECT_EQ(tag_->getNumPendingRWqes(), 0); // No virtual RWqe created
}

// Test createVirtualSendWqeIfNeeded with valid memory region
TEST_F(TagTest, CreateVirtualSendWqeValid) {
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 1000);

  int addressError = -1;
  tag_->createVirtualSendWqeIfNeeded(
      2, rwHeader, &addressError, *memoryRegions_, kMtu);

  EXPECT_EQ(addressError, 0); // No error
  EXPECT_EQ(tag_->getNumPendingWqes(), 1); // Virtual SendWqe created
}

// Test createVirtualSendWqeIfNeeded with invalid memory region
TEST_F(TagTest, CreateVirtualSendWqeInvalid) {
  ReadWriteHeader rwHeader(0x12345678, 999, 1000); // Invalid address and key

  int addressError = -1;
  tag_->createVirtualSendWqeIfNeeded(
      2, rwHeader, &addressError, *memoryRegions_, kMtu);

  EXPECT_EQ(addressError, 1); // Error should be set
  EXPECT_EQ(tag_->getNumPendingWqes(), 0); // No virtual SendWqe created
}

// Test createVirtualSendWqeIfNeeded with already completed MSN
TEST_F(TagTest, CreateVirtualSendWqeAlreadyCompleted) {
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 1000);

  constexpr uint16_t kDeliveredMsn = 5;
  constexpr uint16_t kMsnBehindIt = kDeliveredMsn - 2;
  configureReceiveSequencing(kDeliveredMsn);

  int addressError = -1;
  tag_->createVirtualSendWqeIfNeeded(
      kMsnBehindIt, rwHeader, &addressError, *memoryRegions_, kMtu);

  EXPECT_EQ(addressError, 0); // No addressing error, just already completed
  EXPECT_EQ(tag_->getNumPendingWqes(), 0); // No virtual SendWqe created
}

// Test assignInitiatorMsnToReceiveWqeIfNeeded with ReadWriteHeader validation
TEST_F(TagTest, AssignWithReadWriteHeaderValid) {
  // Add a receive WQE first
  auto receiveWqe = std::make_unique<ReceiveWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  postToTag(*tag_, std::move(receiveWqe), kMtu);

  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 1000);

  int addressError = -1;
  tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, 100, rwHeader, &addressError, *memoryRegions_, kMtu);

  EXPECT_EQ(addressError, 0); // No error
  EXPECT_EQ(tag_->getNumPendingRWqes(), 1);
}

// Test assignInitiatorMsnToReceiveWqeIfNeeded with invalid ReadWriteHeader
TEST_F(TagTest, AssignWithReadWriteHeaderInvalid) {
  // Add a receive WQE first
  auto receiveWqe = std::make_unique<ReceiveWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  postToTag(*tag_, std::move(receiveWqe), kMtu);

  ReadWriteHeader rwHeader(0x12345678, 999, 1000); // Invalid address and key

  int addressError = -1;
  tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, 100, rwHeader, &addressError, *memoryRegions_, kMtu);

  EXPECT_EQ(addressError, 1)
      << "an invalid write-with-immediate remote access must flag an address error";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "the consumed receive WQE carries the error rather than reporting it "
         "on the spot, so no completion appears until the flush";

  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushAllPendingWqesInError();
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "the consumed receive WQE must produce one completion";
  const auto cqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe.workReqId, kWorkReqId)
      << "the completion belongs to the posted receive WQE";
  EXPECT_EQ(cqe.status, kLocalProtErr)
      << "a write-with-immediate to an invalid rkey reports a local protection error, not the flush status its siblings get";
}

/*
 * The receive a bad-rkey write-with-immediate consumes need not be the oldest
 * one posted. Carrying the error instead of reporting it where it is found is
 * what keeps it behind the receives the client posted first.
 */
TEST_F(TagTest, ABadRkeyWriteReportsItsReceiveBehindEarlierReceives) {
  for (uint64_t workReqId : {kWorkReqId, kWorkReqId + 1}) {
    auto rwqe = std::make_unique<ReceiveWqe>(
        workReqId, oneSge(buffer_->data(), 1000, ibvMr_->lkey), kMtu);
    postToTag(*tag_, std::move(rwqe), kMtu);
  }

  // The write consumes the second-posted receive, leaving the first pending.
  ReadWriteHeader rwHeader(0x12345678, 999, 1000); // Invalid address and key
  int addressError = -1;
  tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn + 1,
      /*msn=*/1,
      rwHeader,
      &addressError,
      *memoryRegions_,
      kMtu);
  ASSERT_EQ(addressError, 1)
      << "the assertions below hold only if the write was rejected for its "
         "rkey";

  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushAllPendingWqesInError();
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 2)
      << "both receives must report once the queue is flushed";
  const auto first = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(first.workReqId, kWorkReqId)
      << "the receive posted first must report first, even though it is the "
         "later one that the write failed on";
  EXPECT_EQ(first.status, kWrFlushErr)
      << "the receive that was merely pending is flushed";
  const auto second = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(second.workReqId, kWorkReqId + 1)
      << "the consumed receive reports behind the one posted before it";
  EXPECT_EQ(second.status, kLocalProtErr)
      << "it must keep the protection error rather than reporting a flush";
}

TEST_F(TagTest, SendSequenceNumbersAcceptPreRtsStates) {
  SequenceNumbers numbering{.nextMsn = 100, .nextCsn = 200};
  for (const auto state : {IBV_QPS_RESET, IBV_QPS_INIT, IBV_QPS_RTR}) {
    SCOPED_TRACE(qpStateToString(state));
    const QpStateGuard beforeRts{ibvQpStorage_, state};
    ASSERT_TRUE(tag_->setSendSequenceNumbers(numbering))
        << "send numbering must remain configurable before RTS";
    EXPECT_EQ(tag_->getNextMsn(), numbering.nextMsn)
        << "the next send must use the MSN supplied during setup";
    EXPECT_EQ(tag_->getNextCsn(), numbering.nextCsn)
        << "send completions must use the CSN supplied during setup";
    ++numbering.nextMsn;
    ++numbering.nextCsn;
  }
}

TEST_F(TagTest, SendSequenceNumbersRejectRts) {
  {
    const QpStateGuard inRtr{ibvQpStorage_, IBV_QPS_RTR};
    ASSERT_TRUE(tag_->setSendSequenceNumbers({.nextMsn = 100, .nextCsn = 200}))
        << "precondition: the tag must have numbering to preserve on refusal";
  }
  const QpStateGuard inRts{ibvQpStorage_, IBV_QPS_RTS};
  EXPECT_FALSE(tag_->setSendSequenceNumbers({.nextMsn = 300, .nextCsn = 400}))
      << "a tag that can already send must refuse renumbering rather than "
         "change numbers its peer is tracking";
  EXPECT_EQ(tag_->getNextMsn(), 100)
      << "a refused renumbering must leave the message numbering in place";
  EXPECT_EQ(tag_->getNextCsn(), 200)
      << "a refused renumbering must leave the completion numbering in place";
}

TEST_F(TagTest, SendSequenceNumbersRejectErrorAfterValidation) {
  const QpStateGuard inRtr{ibvQpStorage_, IBV_QPS_RTR};
  ASSERT_TRUE(tag_->setSendSequenceNumbers({.nextMsn = 100, .nextCsn = 200}))
      << "precondition: the tag must have numbering to preserve on refusal";
  ASSERT_TRUE(isBeforeRts(loadQpState(ibvQpStorage_)))
      << "precondition: an initial state check must permit send numbering";

  storeQpErrorState(ibvQpStorage_);
  EXPECT_FALSE(tag_->setSendSequenceNumbers({.nextMsn = 300, .nextCsn = 400}))
      << "an error after state validation must refuse renumbering without aborting";
  EXPECT_FALSE(tag_->setNextMsn(300))
      << "the msn setter must report a concurrent error as a refusal";
  EXPECT_FALSE(tag_->setNextCsn(400))
      << "the csn setter must report a concurrent error as a refusal";
  EXPECT_EQ(tag_->getNextMsn(), 100)
      << "an errored tag must retain its message numbering";
  EXPECT_EQ(tag_->getNextCsn(), 200)
      << "an errored tag must retain its completion numbering";
  EXPECT_EQ(loadQpState(ibvQpStorage_), IBV_QPS_ERR)
      << "a renumbering refusal must leave the tag in its terminal error state";
}

// Test edge case: CSN/MSN sequence enforcement
TEST_F(TagTest, CsnMsnSequenceEnforcement) {
  // Add some WQEs first
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  postToTag(*tag_, std::move(sendWqe), kMtu);

  // Now we have pending WQEs, so setting CSN should fail in debug mode
  // This tests the DCHECK in setNextCsn
  EXPECT_EQ(tag_->getNumPendingWqes(), 1);

  // Similarly for RX CSN with pending RWQEs
  auto receiveWqe = std::make_unique<ReceiveWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 1000), kMtu);
  postToTag(*tag_, std::move(receiveWqe), kMtu);

  EXPECT_EQ(tag_->getNumPendingRWqes(), 1);
}

/*
 * A buffer scattered into must be registered for local write. The key and range
 * can both be right while the permission is missing, so that case has to be
 * rejected on its own; a local read needs no permission at all.
 */
TEST_F(TagTest, LocalWriteRequiresTheRegionToPermitIt) {
  auto* const readOnly =
      memoryRegions_->reg_mr(&pd_, buffer_->data(), kBufferSize, 0);
  ASSERT_NE(readOnly, nullptr)
      << "precondition: a region registers with no access flags, which is what "
         "the permission checks below are asked about";

  EXPECT_TRUE(memoryRegions_->isValidLocal(
      &pd_, buffer_->data(), kBufferSize, readOnly->lkey, 0))
      << "reading a local buffer needs no registered permission";
  EXPECT_FALSE(memoryRegions_->isValidLocal(
      &pd_,
      buffer_->data(),
      kBufferSize,
      readOnly->lkey,
      IBV_ACCESS_LOCAL_WRITE))
      << "a region registered without local write must not accept a scatter";
  EXPECT_TRUE(memoryRegions_->isValidLocal(
      &pd_, buffer_->data(), kBufferSize, ibvMr_->lkey, IBV_ACCESS_LOCAL_WRITE))
      << "a region registered for local write must accept a scatter";
}

/*
 * A key identifies a region only inside the domain it was registered in, so
 * holding the key is not enough: a queue pair of another domain reaches
 * nothing with it.
 */
TEST_F(TagTest, ARegionIsReachableOnlyFromItsOwnProtectionDomain) {
  struct ibv_pd otherPd{nullptr, 2};
  const auto addr = reinterpret_cast<uint64_t>(buffer_->data());
  ReadWriteHeader rwHeader(addr, ibvMr_->rkey, kBufferSize);

  EXPECT_EQ(memoryRegions_->addressForRemoteWrite(&pd_, rwHeader), addr)
      << "the region carries remote write and was registered in this domain, "
         "so the write is served, and with the address the header specifies";
  EXPECT_EQ(
      memoryRegions_->addressForRemoteWrite(&otherPd, rwHeader), std::nullopt)
      << "a queue pair of another domain holds a key that identifies a "
         "region it was never granted";
  EXPECT_FALSE(memoryRegions_->isValidLocal(
      &otherPd,
      buffer_->data(),
      kBufferSize,
      ibvMr_->lkey,
      IBV_ACCESS_LOCAL_WRITE))
      << "the local key is scoped to its domain the same way the remote one is";
}

TEST_F(TagTest, AnAtomicResolvesNoAddressOutsideItsOwnProtectionDomain) {
  struct ibv_pd otherPd{nullptr, 2};
  auto* const zeroBased = memoryRegions_->reg_mr(
      &pd_,
      buffer_->data(),
      kBufferSize,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC |
          IBV_ACCESS_ZERO_BASED);
  ASSERT_NE(zeroBased, nullptr);
  constexpr uint64_t kOffset = 8;
  const auto base = reinterpret_cast<uint64_t>(buffer_->data());

  const auto own =
      memoryRegions_->targetForRemoteAtomic(&pd_, kOffset, zeroBased->rkey);
  ASSERT_EQ(own.address, base + kOffset)
      << "precondition: the region's own domain resolves the offset against "
         "its base, or the two domains below are not being told apart";

  const auto foreign =
      memoryRegions_->targetForRemoteAtomic(&otherPd, kOffset, zeroBased->rkey);
  EXPECT_EQ(foreign.address, kOffset)
      << "a domain the region was never granted to must not have that region's "
         "base added for it, since the resolved address is what the alignment "
         "verdict, and so the peer's NAK, turns on";
  EXPECT_FALSE(foreign.permitted)
      << "and the operation itself stays refused across the domain boundary";
}

/*
 * A region reached by rkey must carry the permission the operation needs. The
 * key and range can both be right while the permission is missing, and each of
 * the three remote operations asks for a different bit.
 */
TEST_F(TagTest, RemoteOperationsRequireTheirOwnPermission) {
  auto* const readOnly = memoryRegions_->reg_mr(
      &pd_, buffer_->data(), kBufferSize, IBV_ACCESS_REMOTE_READ);
  ASSERT_NE(readOnly, nullptr);

  const auto addr = reinterpret_cast<uint64_t>(buffer_->data());
  ReadWriteHeader rwHeader(addr, readOnly->rkey, kBufferSize);

  EXPECT_EQ(memoryRegions_->addressForRemoteRead(&pd_, rwHeader), addr)
      << "a region registered for remote read must serve a read and answer "
         "with the address the header carries";
  EXPECT_EQ(memoryRegions_->addressForRemoteWrite(&pd_, rwHeader), std::nullopt)
      << "a read-only region must not accept a remote write";
  EXPECT_FALSE(memoryRegions_->targetForRemoteAtomic(&pd_, addr, readOnly->rkey)
                   .permitted)
      << "a read-only region must not accept a remote atomic";

  auto* const remoteWritable = memoryRegions_->reg_mr(
      &pd_,
      buffer_->data(),
      kBufferSize,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(remoteWritable, nullptr);
  ReadWriteHeader remoteWritableHeader(addr, remoteWritable->rkey, kBufferSize);
  EXPECT_EQ(
      memoryRegions_->addressForRemoteRead(&pd_, remoteWritableHeader),
      std::nullopt)
      << "a region registered without remote read must not serve a read";
  EXPECT_EQ(
      memoryRegions_->addressForRemoteWrite(&pd_, remoteWritableHeader), addr)
      << "a region registered for remote write must accept a write";
  EXPECT_FALSE(
      memoryRegions_->targetForRemoteAtomic(&pd_, addr, remoteWritable->rkey)
          .permitted)
      << "remote write does not carry remote atomic with it, so an atomic on "
         "this region is refused as it is on the read-only one";

  auto* const atomicCapable = memoryRegions_->reg_mr(
      &pd_,
      buffer_->data(),
      kBufferSize,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  ASSERT_NE(atomicCapable, nullptr);
  const auto atomicTarget =
      memoryRegions_->targetForRemoteAtomic(&pd_, addr, atomicCapable->rkey);
  EXPECT_EQ(atomicTarget.address, addr)
      << "a region not registered zero-based resolves to the address the "
         "request carries, which is what the operation would touch";
  EXPECT_TRUE(atomicTarget.permitted)
      << "a region registered for remote atomic must accept an atomic";
}

TEST_F(TagTest, ALengthPastTheRegionEndIsRefused) {
  // The base is measured, never read, and sits below the length asked for.
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  char* const lowBase = reinterpret_cast<char*>(0x1000);
  auto* const lowBased = memoryRegions_->reg_mr(
      &pd_,
      lowBase,
      kBufferSize,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(lowBased, nullptr);

  constexpr uint32_t kLengthPastTheEnd = 0x80000000;
  ReadWriteHeader longHeader(
      reinterpret_cast<uint64_t>(lowBase), lowBased->rkey, kLengthPastTheEnd);
  EXPECT_EQ(
      memoryRegions_->addressForRemoteWrite(&pd_, longHeader), std::nullopt)
      << "a length longer than the region must be refused on its own account, "
         "since the remaining space it would otherwise be measured against "
         "underflows past the top of the address space";
}

TEST_F(TagTest, AnAddressTheAccessLengthWrapsPastIsRefused) {
  auto* const remoteWritable = memoryRegions_->reg_mr(
      &pd_,
      buffer_->data(),
      kBufferSize,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  ASSERT_NE(remoteWritable, nullptr);

  constexpr uint32_t kLength = 0x100;
  ReadWriteHeader wrapHeader(
      std::numeric_limits<uint64_t>::max() - kLength / 2,
      remoteWritable->rkey,
      kLength);
  EXPECT_EQ(
      memoryRegions_->addressForRemoteWrite(&pd_, wrapHeader), std::nullopt)
      << "the region grants remote write and the key identifies it, so the "
         "range check is the only guard left to refuse an address that lies "
         "outside every region";
}

/*
 * Msn is a 16 bit sequence number, so an outstanding window that crosses zero
 * is not in numeric order: msns 65534, 65535, 0, 1 sort as 0, 1, 65534, 65535.
 * A flush must still report completions in the order the work requests were
 * posted.
 */
TEST_F(TagTest, FlushReportsCompletionsInPostingOrderAcrossMsnWrap) {
  numberTransmitFrom(65534);
  for (uint64_t wrId : {1ULL, 2ULL, 3ULL, 4ULL}) {
    auto wqe =
        std::make_unique<SendWqe>(wrId, oneSge(buffer_->data(), 1000), kMtu);
    postToTag(*tag_, std::move(wqe), kMtu);
  }
  ASSERT_EQ(tag_->getNumPendingWqes(), 4)
      << "all four work requests must be pending before the flush";

  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushAllPendingWqesInError();
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 4)
      << "every pending work request must be flushed";
  for (uint64_t expectedWrId : {1ULL, 2ULL, 3ULL, 4ULL}) {
    const auto cqe = folly::coro::blockingWait(cq_->pollCq());
    EXPECT_EQ(cqe.workReqId, expectedWrId)
        << "completions must follow posting order even where the msn wraps past zero";
  }
}

TEST_F(TagTest, FlushAllPendingWqesInErrorGeneratesCompletions) {
  auto sendWqe1 = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  auto sendWqe2 = std::make_unique<SendWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 1000), kMtu);
  auto sendWqe3 = std::make_unique<SendWqe>(
      kWorkReqId + 2, oneSge(buffer_->data(), 1000), kMtu);

  Wqe* wqe1Ptr = sendWqe1.get();
  postToTag(*tag_, std::move(sendWqe1), kMtu);
  postToTag(*tag_, std::move(sendWqe2), kMtu);
  postToTag(*tag_, std::move(sendWqe3), kMtu);

  ASSERT_EQ(tag_->getNumPendingWqes(), 3) << "should have 3 pending WQEs";
  ASSERT_EQ(cq_->getNumCompletedWqes(), 0)
      << "CQ should be empty before any completions";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeWqeThatFailsTheTag(*wqe1Ptr, kWrRemAccErr);
  }

  EXPECT_EQ(tag_->getNumPendingWqes(), 2)
      << "first WQE removed, 2 should remain";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 1) << "one error CQE for the first WQE";

  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushAllPendingWqesInError();
  }

  EXPECT_EQ(tag_->getNumPendingWqes(), 0) << "all WQEs should be flushed";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 3)
      << "CQEs for all 3 WQEs: 1 error + 2 flush";

  auto cqe1 = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe1.workReqId, kWorkReqId) << "first CQE work request ID";
  EXPECT_EQ(cqe1.status, kWrRemAccErr) << "first WQE has remote access error";

  auto cqe2 = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe2.workReqId, kWorkReqId + 1) << "second CQE work request ID";
  EXPECT_EQ(cqe2.status, kWrFlushErr) << "second WQE flushed with flush error";

  auto cqe3 = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe3.workReqId, kWorkReqId + 2) << "third CQE work request ID";
  EXPECT_EQ(cqe3.status, kWrFlushErr) << "third WQE flushed with flush error";
}

/*
 * Completing one work request in error reports that request. It does not move
 * the tag: only its owner can do that, because only the owner can pair the
 * transition with the record that the sender has a sweep to run and the
 * wakeup that brings one.
 */
TEST_F(TagTest, CompletingAWorkRequestInErrorLeavesTheTagRunning) {
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  Wqe* posted = sendWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(sendWqe), kMtu))
      << "precondition: the send is accepted, or there is no work here to "
         "complete in error";
  ASSERT_FALSE(tag_->isInErrorState())
      << "the tag has to start out running, or this proves nothing";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeWqeThatFailsTheTag(*posted, kWrRemAccErr);
  }

  EXPECT_FALSE(tag_->isInErrorState())
      << "a tag left in error by the completion alone would be one the sender "
         "was never told to sweep";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "the work request still reports the error it was completed with";
}

/*
 * A work request rejected by validation carries its own error, but posting it
 * to a tag that had already failed makes it one of the flushed: the tag was
 * finished before this work was offered, so a defect of its own is not what
 * ended it. The error it arrived with is overwritten rather than preserved,
 * which is the opposite of the rule for work already accepted.
 */
TEST_F(TagTest, PostingToATagInErrorFlushesWhateverErrorTheWqeArrivedWith) {
  tag_->getIbvQp().state = IBV_QPS_ERR;

  auto rejected = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  rejected->setCompletionStatusOverride(kLocalProtErr);
  postToTag(*tag_, std::move(rejected), kMtu);

  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "a post to a tag in error must still report, or the client waits on a "
         "completion for work the tag will never do";
  const auto cqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe.status, kWrFlushErr)
      << "the tag was in error before this work request was posted, so it "
         "reports a "
         "flush rather than the validation error it arrived carrying";
}

/*
 * Msns wrap, and the pending work requests are held in a map keyed by msn. The
 * earliest posted is the one the watchdog and the rnr retry limit report their
 * error on, so picking the numerically smallest key blames the wrong work
 * request for the whole window either side of a wrap.
 */
TEST_F(TagTest, EarliestPendingWqeIsPostedOrderNotNumericOrder) {
  numberTransmitFrom(65534);
  // Msns 65534, 65535, 0, 1 -- posting order, but not numeric order.
  for (uint64_t workReqId : {1ULL, 2ULL, 3ULL, 4ULL}) {
    auto wqe = std::make_unique<SendWqe>(
        workReqId, oneSge(buffer_->data(), 1000), kMtu);
    postToTag(*tag_, std::move(wqe), kMtu);
  }
  ASSERT_EQ(tag_->getNumPendingWqes(), 4)
      << "all four work requests must be pending";

  Wqe* earliestWqe = tag_->getEarliestPendingWqe();

  ASSERT_NE(earliestWqe, nullptr)
      << "a tag with pending work must report one of them as its earliest";
  ASSERT_TRUE(earliestWqe->getWorkReqId().has_value())
      << "a posted work request carries the id the client gave it";
  EXPECT_EQ(earliestWqe->getWorkReqId().value(), 1ULL)
      << "the earliest posted work request is the one that wrapped, not the "
         "one with the smallest msn; blaming the latter reports the error on a "
         "work request posted after the one that actually stalled";
}

/*
 * Receives report in posting order, which is csn order. A receive moves to the
 * bound map when a message binds to it, and a message arriving out of order
 * binds a later-posted receive first -- so ordering each map on its own key
 * would report a later-posted receive ahead of an earlier one.
 */
TEST_F(TagTest, ReceivesAreFlushedInPostingOrderEvenWhenALaterOneBoundFirst) {
  std::vector<uint64_t> workReqIds;
  for (uint64_t workReqId : {10ULL, 11ULL, 12ULL}) {
    auto rwqe = std::make_unique<ReceiveWqe>(
        workReqId, oneSge(buffer_->data(), 1000, ibvMr_->lkey), kMtu);
    postToTag(*tag_, std::move(rwqe), kMtu);
    workReqIds.push_back(workReqId);
  }
  ASSERT_EQ(tag_->getNumPendingRWqes(), 3)
      << "the ordering under test only shows up across several posted "
         "receives; if they are not all pending, what follows proves nothing";

  // The middle receive is bound first, as an out-of-order message would.
  const uint16_t middleCsn = kFixtureRxCsn + 1;
  ASSERT_TRUE(
      tag_->assignInitiatorMsnToReceiveWqeIfNeeded(middleCsn, /*msn=*/1, kMtu))
      << "the second-posted receive must bind to the arriving message";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushPendingReceiveWqesInError();
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 3)
      << "every posted receive must report";
  for (uint64_t expectedWorkReqId : workReqIds) {
    const auto cqe = folly::coro::blockingWait(cq_->pollCq());
    EXPECT_EQ(cqe.workReqId, expectedWorkReqId)
        << "receives must report in the order they were posted; the one that "
           "happened to bind first is not the one posted first";
  }
}

/*
 * A tag that gives up hands its work requests back in posting order, the first
 * carrying the error that caused the failure. Marking does not complete
 * anything by itself; the completions come once nothing is outstanding.
 */
TEST_F(TagTest, MarkPendingWqesFailingReportsTheErrorOnceAndFlushesTheRest) {
  std::vector<std::reference_wrapper<Wqe>> wqes;
  for (uint64_t wrId : {kWorkReqId, kWorkReqId + 1, kWorkReqId + 2}) {
    auto wqe =
        std::make_unique<SendWqe>(wrId, oneSge(buffer_->data(), 1000), kMtu);
    wqes.emplace_back(*wqe);
    postToTag(*tag_, std::move(wqe), kMtu);
  }

  const uint32_t abandonedCount = tag_->markPendingWqesFailing(kWcRetryExcErr);

  EXPECT_EQ(abandonedCount, wqes.size())
      << "none of these work requests had sent a packet, so every posn must "
         "be abandoned for them to have nothing outstanding";
  EXPECT_EQ(tag_->getNumPendingWqes(), wqes.size())
      << "marking must not retire a work request on its own; that has to wait "
         "until nothing is outstanding, so completions stay in msn order";
  ASSERT_EQ(cq_->getNumCompletedWqes(), 0)
      << "no completion may be reported while packets are outstanding";

  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    EXPECT_TRUE(tag_->handleTxWqesCompletion(wqes))
        << "precondition: completing the send finds no csn out of sequence";
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), wqes.size())
      << "every work request the tag gave up on must produce a completion, or "
         "the caller waits forever";
  const auto firstCqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(firstCqe.workReqId, kWorkReqId)
      << "completions must still follow posting order";
  EXPECT_EQ(firstCqe.status, kWcRetryExcErr)
      << "the earliest work request must report the error that caused the "
         "failure, since a failed wqe would otherwise report success";
  for (uint64_t expectedWrId : {kWorkReqId + 1, kWorkReqId + 2}) {
    const auto cqe = folly::coro::blockingWait(cq_->pollCq());
    EXPECT_EQ(cqe.workReqId, expectedWrId)
        << "completions must still follow posting order";
    EXPECT_EQ(cqe.status, kWrFlushErr)
        << "work requests behind the failure are flushed, not blamed for it";
  }
}

/*
 * A virtual send -- the response to a remote read -- reports no CQE, so
 * nothing notices if it is left behind. Failing the tag has to retire it
 * anyway: retiring is how it completes, and until then it holds the WQE and
 * the posns it was sending from.
 */
TEST_F(TagTest, FailingATagRetiresAVirtualSendThatWillNeverBeSent) {
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 1000);
  int addressError = -1;
  tag_->createVirtualSendWqeIfNeeded(
      2, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 0)
      << "the response to the remote read must have been created; "
         "with no wqe there is nothing for the give-up below to retire";
  ASSERT_EQ(tag_->getNumPendingWqes(), 1)
      << "the response to the remote read must be pending";

  tag_->markPendingWqesFailing(kWrFlushErr);
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }

  EXPECT_EQ(tag_->getNumPendingWqes(), 0)
      << "a response that will never be sent must be retired when the tag "
         "fails; leaving it holds its wqe until the qp is torn down";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "a virtual send has no client, so retiring it -- which is how it "
         "completes -- must not enqueue a cqe";
}

/*
 * A work request that fails validation at post time arrives carrying its own
 * error. The ones ahead of it may have packets on the wire, and their posn
 * state belongs to the sender, so the posting thread leaves them to it. The
 * error state it sets is what the sender reads to mark them failing.
 */
TEST_F(TagTest, PostingAFailedWqeLeavesTheRestToTheSender) {
  auto inFlight = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 2 * kMtu), kMtu);
  Wqe* inFlightWqe = inFlight.get();
  postToTag(*tag_, std::move(inFlight), kMtu);
  // One of its packets is on the wire.
  std::ignore = inFlightWqe->allocateNextPosn();

  auto rejected = std::make_unique<SendWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 1000), kMtu);
  rejected->setCompletionStatusOverride(kLocalProtErr);
  postToTag(*tag_, std::move(rejected), kMtu);

  EXPECT_TRUE(tag_->isInErrorState())
      << "a work request that fails validation must error its tag";
  EXPECT_FALSE(inFlightWqe->isFailing())
      << "the earlier work request's posn state is the sender's to write; the "
         "posting thread must not touch it";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "neither work request may report yet: the earlier one has not settled "
         "and the rejected one completes in order behind it";

  // Stand in for the sender's arbiter visit, which holds the Tag semaphore
  // across the marking as the walk that finds the Tag does.
  {
    auto tagGuard = tag_->scopedLock();
    tag_->failPendingWqesIfInErrorWithTagHeld();
  }
  EXPECT_TRUE(inFlightWqe->isFailing())
      << "the sender marks it failing once it sees the tag in error";

  // The packet on the wire is acknowledged, so the earlier one settles.
  inFlightWqe->acknowledgePosn(0);
  tag_->completeSettledFailedWqes();

  ASSERT_EQ(cq_->getNumCompletedWqes(), 2)
      << "both work requests must report once no packet is left on the wire";
  const auto first = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(first.workReqId, kWorkReqId)
      << "completions must follow posting order";
  EXPECT_EQ(first.status, kWrFlushErr)
      << "the work request that was merely in flight is flushed";
  const auto second = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(second.workReqId, kWorkReqId + 1)
      << "the rejected work request reports behind the earlier one";
  EXPECT_EQ(second.status, kLocalProtErr)
      << "it must report the error it was rejected for, not a flush";
}

/*
 * A receive whose buffer the responder cannot scatter into fails when the
 * message binds to it, which can be before the receives posted ahead of it
 * have reported. It carries its error until the flush reports the queue in
 * posting order.
 */
TEST_F(TagTest, AReceiveThatCannotBeWrittenReportsBehindEarlierReceives) {
  auto writableRwqe = std::make_unique<ReceiveWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000, ibvMr_->lkey), kMtu);
  postToTag(*tag_, std::move(writableRwqe), kMtu);
  auto unwritableRwqe = std::make_unique<ReceiveWqe>(
      kWorkReqId + 1,
      oneSge(buffer_->data(), 1000, 0xbadbad /*never registered*/),
      kMtu);
  postToTag(*tag_, std::move(unwritableRwqe), kMtu);

  // A message binds the second receive, and its buffer does not validate.
  const uint16_t unwritableCsn = kFixtureRxCsn + 1;
  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      unwritableCsn, /*msn=*/1, kMtu))
      << "precondition: the message binds the unwritable receive, or the "
         "validation below runs against no work request at all";
  EXPECT_TRUE(tag_->recordLocalProtectionErrorIfUnwritable(
      /*msn=*/1, *memoryRegions_))
      << "a receive buffer that is not registered for local write must fail";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "it must not report yet: the receive posted before it has not, and "
         "reporting here would put them out of the order they were posted";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushPendingReceiveWqesInError();
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 2)
      << "both receives must report once the queue is flushed";
  const auto first = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(first.workReqId, kWorkReqId)
      << "the receive posted first must report first";
  EXPECT_EQ(first.status, kWrFlushErr)
      << "the receive that was merely pending is flushed";
  const auto second = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(second.workReqId, kWorkReqId + 1)
      << "the receive that failed reports behind the one posted before it";
  EXPECT_EQ(second.status, kLocalProtErr)
      << "it must keep the protection error rather than reporting a flush";
}

/*
 * A read is completed by its response, not by the ack for its request, so its
 * posns count responses. Once it is failing no response is coming, and the
 * only frame left to wait for is its request leaving the wire.
 */
TEST_F(TagTest, AFailingReadCompletesOnceItsRequestFrameSettles) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), 2 * kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* wqe = readWqe.get();
  postToTag(*tag_, std::move(readWqe), kMtu);

  // The request frame goes out; the response positions stay untouched.
  std::ignore = wqe->allocateNextPosn();

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->markPendingWqesFailing(kWrRemAccErr);
  }
  EXPECT_NE(wqe->getOutstandingPosnCount(), 0u)
      << "the posns a response would have landed in are written by the thread "
         "that delivers them; failing the work request must leave them alone";
  EXPECT_FALSE(wqe->isFailingAndSettled())
      << "the request frame is still claimed, so the read is not settled";
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }
  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "the request is still on the wire and can be retransmitted, so the "
         "read must not be reported while a frame of it is still outstanding";

  // The request is acked, so nothing can retransmit it.
  wqe->onFrameNewlyAcked(Wqe::kRequestFrameWirePosn);
  EXPECT_TRUE(wqe->isFailingAndSettled())
      << "with its request frame settled the read is too, whatever its "
         "response posns say";
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "with the request off the wire nothing is left to wait for; leaving "
         "it pending is a read that never reports";
  const auto cqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe.status, kWrRemAccErr)
      << "the read must report the error it failed with";
}

// Test WQE lifecycle with multiple operations
TEST_F(TagTest, WqeLifecycle) {
  // Start with empty tag
  EXPECT_EQ(tag_->getNumPendingWqes(), 0);
  EXPECT_EQ(tag_->getNumPendingRWqes(), 0);

  // Add various types of WQEs
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  auto receiveWqe = std::make_unique<ReceiveWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 2000), kMtu);
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId + 2,
      oneSge(buffer_->data(), 1500),
      kMtu,
      0x4000,
      ibvMr_->rkey);

  // Process WQEs
  postToTag(*tag_, std::move(sendWqe), kMtu);
  EXPECT_EQ(tag_->getNumPendingWqes(), 1);
  EXPECT_EQ(tag_->getNextMsn(), 2);

  postToTag(*tag_, std::move(receiveWqe), kMtu);
  EXPECT_EQ(tag_->getNumPendingRWqes(), 1);

  postToTag(*tag_, std::move(readWqe), kMtu);
  EXPECT_EQ(tag_->getNumPendingWqes(), 2);
  EXPECT_EQ(tag_->getNextMsn(), 3);

  // Test finding WQEs
  Wqe* nextWqe = tag_->findNextWqe();
  EXPECT_NE(nextWqe, nullptr);
  EXPECT_EQ(nextWqe->getWorkType(), Wqe::WorkType::kSend);
  tag_->markFree();
}

// Test that handleTxWqesCompletion correctly cleans up VirtualSendWqes
// from remoteMsnToWqe_ without generating CQEs, while local WQEs
// still produce CQEs through the in-order completion path.
TEST_F(TagTest, HandleTxWqesCompletionCleansUpVirtualSendWqe) {
  // Create a local SendWqe (MSN 1, goes into msnSpan_.wqes)
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  postToTag(*tag_, std::move(sendWqe), kMtu);

  // Create a VirtualSendWqe (MSN 42, goes into remoteMsnToWqe_)
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 1000);
  int addressError = -1;
  tag_->createVirtualSendWqeIfNeeded(
      42, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 0);

  // Both WQEs should be pending: 1 local + 1 virtual
  ASSERT_EQ(tag_->getNumPendingWqes(), 2)
      << "should have 1 local and 1 virtual WQE pending";
  ASSERT_EQ(cq_->getNumCompletedWqes(), 0)
      << "CQ should be empty before completion";

  // findNextWqe checks remoteMsnToWqe_ first, so the virtual WQE is returned
  Wqe* virtualWqePtr = tag_->findNextWqe();
  ASSERT_NE(virtualWqePtr, nullptr);
  ASSERT_TRUE(virtualWqePtr->isVirtual())
      << "findNextWqe should return the virtual WQE first";
  tag_->markFree();

  // Allocate all POSNs on the virtual WQE so hasBeenSent() returns true
  // and findNextWqe will skip it, returning the local WQE next
  while (!virtualWqePtr->hasBeenSent()) {
    (void)virtualWqePtr->allocateNextPosn();
  }
  Wqe* localWqePtr = tag_->findNextWqe();
  ASSERT_NE(localWqePtr, nullptr);
  ASSERT_FALSE(localWqePtr->isVirtual())
      << "second findNextWqe should return the local WQE";
  tag_->markFree();

  std::ignore = localWqePtr->allocateNextPosn();
  localWqePtr->acknowledgePosn(0);

  // Complete both WQEs in a single handleTxWqesCompletion call
  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    EXPECT_TRUE(tag_->handleTxWqesCompletion({*virtualWqePtr, *localWqePtr}))
        << "precondition: completing the send finds no csn out of sequence";
  }

  EXPECT_EQ(tag_->getNumPendingWqes(), 0)
      << "both WQEs should be cleaned up after completion";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "only the local WQE should generate a CQE, not the virtual one";

  // Verify the CQE is for the local SendWqe
  auto cqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe.workReqId, kWorkReqId) << "CQE should be for the local WQE";
  EXPECT_EQ(cqe.status, kSuccess) << "CQE should report success";
}

// A successful unsignaled WR raises no CQE but must still be retired so
// in-order completion can advance.
TEST_F(TagTest, UnsignaledSuccessProducesNoCompletion) {
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu, /*send_flags=*/0);
  Wqe* wqePtr = sendWqe.get();
  ASSERT_FALSE(wqePtr->isSignaled())
      << "send_flags without IBV_SEND_SIGNALED must yield an unsignaled WQE";
  postToTag(*tag_, std::move(sendWqe), kMtu);

  ASSERT_EQ(tag_->getNumPendingWqes(), 1) << "the WQE should be pending";
  ASSERT_EQ(cq_->getNumCompletedWqes(), 0)
      << "CQ should be empty before completion";

  answerTheOnlyFrame(*wqePtr);

  EXPECT_EQ(tag_->getNumPendingWqes(), 0)
      << "the WQE must still be retired so in-order completion can advance";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "a successful unsignaled WR must not raise a completion";
}

// An sq_sig_all Tag completes a successful WR even when it is unsignaled.
TEST_F(TagTest, SqSigAllCompletesUnsignaledSuccess) {
  auto signaledTag = makeTagInInit(*cq_, /*sqSigAll=*/true);
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu, /*send_flags=*/0);
  Wqe* wqePtr = sendWqe.get();
  ASSERT_FALSE(wqePtr->isSignaled())
      << "the WR itself carries no IBV_SEND_SIGNALED";
  postToTag(*signaledTag, std::move(sendWqe), kMtu);

  answerTheOnlyFrame(*signaledTag, *wqePtr);

  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "an sq_sig_all Tag completes a successful WR even when it is unsignaled";
}

/*
 * A virtual send's packets are frames this side put on the wire, so the
 * transmit tracker holds them until they are acked. Completing it in error
 * must leave the work request alive while any of them are outstanding.
 */
TEST_F(TagTest, AVirtualSendCompletedInErrorIsHeldUntilItsResponseSettles) {
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 2 * kMtu);
  int addressError = -1;
  tag_->createVirtualSendWqeIfNeeded(
      2, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 0)
      << "precondition: the response to the remote read must exist, or there "
         "is no virtual send here to hold";

  Wqe* response = tag_->findNextWqe();
  ASSERT_NE(response, nullptr);
  ASSERT_TRUE(response->isVirtual())
      << "precondition: the tag's only work is the read response";

  // findNextWqe returns with the tag held, which the posn calls below require.
  const uint32_t firstPosn = response->allocateNextPosn();
  const uint32_t secondPosn = response->allocateNextPosn();
  response->acknowledgePosn(firstPosn);
  ASSERT_EQ(response->getOutstandingPosnCount(), 1u)
      << "precondition: one response packet is still on the wire, which is "
         "what the completion below must wait for";
  tag_->markFree();

  {
    auto tagGuard = tag_->scopedLock();
    tag_->markPendingWqesFailing(kWrFlushErr);
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }

  EXPECT_EQ(tag_->getNumPendingWqes(), 1)
      << "the response must stay while a packet of it is outstanding; freeing "
         "it leaves the transmit tracker holding a work request already gone";
  {
    auto tagGuard = tag_->scopedLock();
    EXPECT_TRUE(response->isFailing())
        << "the mark is what stops the tx path pulling more packets from a "
           "response already completed in error";
  }

  // In production this ack reaches the work request through
  // Qp::checkForTxMessageCompletion; completeSettledFailedWqes() is the
  // retirement it performs.
  {
    auto tagGuard = tag_->scopedLock();
    response->acknowledgePosn(secondPosn);
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }

  EXPECT_EQ(tag_->getNumPendingWqes(), 0)
      << "once its last packet is acked the response has nothing left to wait "
         "for, and retiring it is how it completes";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "a virtual send has no client, so none of this may enqueue a cqe";
}

TEST_F(TagTest, TheTagAcceptsTheSendDepthItAdvertisesAndNoMore) {
  for (uint32_t i = 0; i < kMaxQpWorkRequests; ++i) {
    ASSERT_TRUE(tag_->tryReserveSendQueueEntry())
        << "precondition: the tag accepts work up to the depth it advertises; "
           "it refused at "
        << i;
  }

  EXPECT_FALSE(tag_->tryReserveSendQueueEntry())
      << "past that depth the tag must refuse, so the caller reports ENOMEM "
         "rather than seating an msn outside the window";

  tag_->releaseSendQueueEntry();
  EXPECT_TRUE(tag_->tryReserveSendQueueEntry())
      << "a returned queue entry must be reusable; a leaked one makes every later "
         "post read the tag as full for good";
}

/*
 * `TagSet::mutex_` already excludes this race, so the case holds the helper to
 * its contract rather than covering a live hazard.
 */
TEST_F(TagTest, ConcurrentPostsDoNotBothClaimTheLastPlace) {
  for (uint32_t i = 0; i < kMaxQpWorkRequests - 1; ++i) {
    ASSERT_TRUE(tag_->tryReserveSendQueueEntry())
        << "precondition: the tag fills to one short of its depth; it refused "
           "at "
        << i;
  }

  // Released and retaken each round, so every round races for one entry.
  constexpr int kRounds = 200;
  constexpr int kPosters = 8;
  for (int round = 0; round < kRounds; ++round) {
    std::atomic<bool> go{false};
    std::atomic<uint32_t> claimed{0};
    std::vector<std::thread> posters;
    posters.reserve(kPosters);
    for (int t = 0; t < kPosters; ++t) {
      posters.emplace_back([&] {
        while (!go.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        if (tag_->tryReserveSendQueueEntry()) {
          claimed.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
    go.store(true, std::memory_order_release);
    for (auto& poster : posters) {
      poster.join();
    }

    ASSERT_EQ(claimed.load(), 1u)
        << "exactly one racing post may take the last queue entry; more than one "
           "seats an msn outside the window, none refuses work the tag had "
           "room for. Round "
        << round;
    tag_->releaseSendQueueEntry();
  }
}

/*
 * A tag that refuses the work still gives the queue entry back. Both refusals
 * are states a client can keep posting into, so an entry kept on either one
 * leaves
 * the tag reading full for the life of the queue pair.
 */
TEST_F(TagTest, WorkTheTagRefusesGivesBackItsQueueEntry) {
  ibvQp_->state = IBV_QPS_ERR;
  ASSERT_FALSE(postToTag(
      *tag_,
      std::make_unique<SendWqe>(
          kWorkReqId, oneSge(buffer_->data(), 1000), kMtu),
      kMtu))
      << "precondition: a tag in error refuses newly posted work";
  EXPECT_EQ(tag_->getAcceptedSendQueueEntries(), 0)
      << "a tag in error must give the queue entry back; a client that keeps posting "
         "to one would otherwise consume the depth for good";

  ibvQp_->state = IBV_QPS_RTS;
  tag_->markForDeletion();
  ASSERT_FALSE(postToTag(
      *tag_,
      std::make_unique<SendWqe>(
          kWorkReqId + 1, oneSge(buffer_->data(), 1000), kMtu),
      kMtu))
      << "precondition: a tag being removed refuses newly posted work";
  EXPECT_EQ(tag_->getAcceptedSendQueueEntries(), 0)
      << "a tag being removed must give the queue entry back too";
}

TEST_F(TagTest, CompletingWorkReturnsCapacityToTheDepth) {
  for (uint32_t i = 0; i < kMaxQpWorkRequests; ++i) {
    ASSERT_TRUE(postToTag(
        *tag_,
        std::make_unique<SendWqe>(
            kWorkReqId + i, oneSge(buffer_->data(), 1000), kMtu),
        kMtu))
        << "precondition: the tag takes the work it accepted; it stopped at "
        << i;
  }
  EXPECT_FALSE(tag_->tryReserveSendQueueEntry())
      << "a tag at its full depth must refuse, or a later post seats an msn "
         "outside the window";

  Wqe* earliest = tag_->getEarliestPendingWqe();
  ASSERT_NE(earliest, nullptr)
      << "precondition: the tag holds the work it accepted, or there is "
         "nothing to complete";
  answerTheOnlyFrame(*earliest);

  EXPECT_TRUE(tag_->tryReserveSendQueueEntry())
      << "a completion must return its queue entry, or a queue pair working "
         "normally still refuses every later post";
}

/*
 * The watchdog completes the earliest pending work request, and a tag that has
 * already failed its work still has one. What ended that work request is the
 * error it failed with, so the status the watchdog passes must not report in
 * its place.
 */
TEST_F(TagTest, CompletingInErrorKeepsAStatusTheWqeAlreadyCarries) {
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  Wqe* posted = sendWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(sendWqe), kMtu))
      << "precondition: the send is accepted, or there is no work here to "
         "fail";

  // Failed as a fatal NAK does, carrying the status the peer reported, and
  // under the Tag semaphore that marking requires.
  {
    auto tagGuard = tag_->scopedLock();
    tag_->markPendingWqesFailing(kWrRemAccErr);
  }
  ASSERT_EQ(posted->getCompletionStatusOverride(), kWrRemAccErr)
      << "the work request must be carrying an error already, or the "
         "completion below has no status to preserve";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeWqeThatFailsTheTag(*posted, kWcRetryExcErr);
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "the work request must report whichever error it ends up carrying";
  const auto cqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe.status, kWrRemAccErr)
      << "the peer's rejection is what ended this work request; reporting the "
         "watchdog's timeout would blame the symptom the client sees instead";
}

/*
 * A send that already succeeded, waiting its turn behind one that failed,
 * reports the success it earned. The failing one carries its own error, and
 * reporting it must not repaint the work behind it.
 */
TEST_F(TagTest, WorkThatSucceededBehindAFailureStillReportsSuccess) {
  auto firstSend = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  auto secondSend = std::make_unique<SendWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 1000), kMtu);
  Wqe* first = firstSend.get();
  Wqe* second = secondSend.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(firstSend), kMtu))
      << "precondition: the tag accepts the first send, or there is nothing "
         "for the second to wait behind";
  ASSERT_TRUE(postToTag(*tag_, std::move(secondSend), kMtu))
      << "precondition: the tag accepts the second send, or there is no "
         "queued success to repaint";

  // The second is acked while the first is still outstanding, so it waits its
  // turn with nothing wrong with it.
  answerTheOnlyFrame(*second);
  ASSERT_EQ(cq_->getNumCompletedWqes(), 0)
      << "precondition: the second waits behind the first, or there is no "
         "queued success here to repaint";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeWqeThatFailsTheTag(*first, kWrRemAccErr);
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 2)
      << "failing the first releases both in order";
  const auto firstCqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(firstCqe.status, kWrRemAccErr)
      << "the send that failed reports its own error";
  const auto secondCqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(secondCqe.status, kSuccess)
      << "the send that succeeded must still report success; erroring the tag "
         "first would tell the client its completed work failed";
}

/*
 * A work request completed in error reports where it sits in the queue, not
 * when the caller reaches it. Reporting it ahead of the work posted before it
 * would hand the client a completion out of order.
 */
TEST_F(TagTest, CompletingInErrorWaitsForTheWorkPostedBeforeIt) {
  auto firstSend = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  auto secondSend = std::make_unique<SendWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 1000), kMtu);
  Wqe* first = firstSend.get();
  Wqe* second = secondSend.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(firstSend), kMtu))
      << "precondition: the tag accepts the first send, or the failed one has "
         "nothing to wait behind";
  ASSERT_TRUE(postToTag(*tag_, std::move(secondSend), kMtu))
      << "precondition: the tag accepts the second send, so it sits behind the "
         "first in the completion window";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeWqeThatFailsTheTag(*second, kWrRemAccErr);
  }

  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "the client must not be told the second send ended while the first "
         "one it posted is still outstanding";
  EXPECT_EQ(tag_->getNumPendingWqes(), 2)
      << "the failed send stays posted until its turn; freeing it here would "
         "leave the tracker holding frames of a work request already gone";
  {
    auto tagGuard = tag_->scopedLock();
    EXPECT_TRUE(second->isFailing())
        << "the mark is what stops the tx path sending more packets for it "
           "while it waits its turn to report";
  }

  answerTheOnlyFrame(*first);

  ASSERT_EQ(cq_->getNumCompletedWqes(), 2)
      << "the first send completing must carry the failed one out behind it";
  const auto firstCqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(firstCqe.workReqId, kWorkReqId)
      << "the send posted first must report first";
  EXPECT_EQ(firstCqe.status, kSuccess)
      << "the first send was acknowledged, so its neighbour's failure must not "
         "reach it";
  const auto secondCqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(secondCqe.workReqId, kWorkReqId + 1)
      << "the failed send reports once its msn reaches the head of the window";
  EXPECT_EQ(secondCqe.status, kWrRemAccErr)
      << "waiting its turn must not cost the failed send the error it was "
         "completed with";
}

/*
 * A flush retires everything it holds around the completed-msn tracker, so the
 * cursor has to move with it. Work posted afterwards is what notices: it would
 * wait at the head of the window for msns that were flushed and will never be
 * reported.
 *
 * The map here straddles zero, which is where the cursor's new value has to
 * come from a modular walk: std::map orders 0 ahead of 65535, so the furthest
 * msn retired is the last one reached from the cursor, not the last key.
 *
 * The flush runs on a tag that is not in error, which no production caller
 * does; at this revision a post to an errored tag never reaches the map, so
 * the sequence is reachable only from here.
 */
TEST_F(TagTest, WorkPostedAfterAFlushDoesNotWaitOnWhatWasFlushed) {
  numberTransmitFrom(65535);
  for (int i = 0; i < 2; ++i) {
    auto flushedSend = std::make_unique<SendWqe>(
        kWorkReqId + i, oneSge(buffer_->data(), 1000), kMtu);
    ASSERT_TRUE(postToTag(*tag_, std::move(flushedSend), kMtu))
        << "precondition: the tag accepts both sends, or the flush below does "
           "not span the wrap";
  }

  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushAllPendingWqesInError();
  }
  ASSERT_EQ(cq_->getNumCompletedWqes(), 2)
      << "precondition: the flush reports both sends it retired";
  folly::coro::blockingWait(cq_->pollCq());
  folly::coro::blockingWait(cq_->pollCq());

  auto laterSend = std::make_unique<SendWqe>(
      kWorkReqId + 2, oneSge(buffer_->data(), 1000), kMtu);
  Wqe* later = laterSend.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(laterSend), kMtu))
      << "precondition: the tag accepts a send posted after the flush, or "
         "there is nothing left to report on its own";

  answerTheOnlyFrame(*later);

  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "a send posted after the flush must report on its own; the msns the "
         "flush retired are not coming to release it";
  EXPECT_EQ(tag_->getNumPendingWqes(), 0)
      << "a send left pending here is one the client is still waiting on";
}

/*
 * A work request completed in error still has packets on the wire, and the
 * transmit tracker holds them until they resolve. Reporting it then would free
 * a work request the tracker still points to, so it waits for the ack.
 */
TEST_F(TagTest, CompletingInErrorWaitsForItsOwnPacketsToSettle) {
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 2 * kMtu), kMtu);
  Wqe* posted = sendWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(sendWqe), kMtu))
      << "precondition: the send is accepted, or there is nothing here to "
         "complete";

  // The tag is held across the posn calls, which is what excludes an
  // allocation concurrent with the markFailing below.
  uint32_t firstPosn = 0;
  uint32_t secondPosn = 0;
  {
    auto tagGuard = tag_->scopedLock();
    firstPosn = posted->allocateNextPosn();
    secondPosn = posted->allocateNextPosn();
    posted->acknowledgePosn(firstPosn);
  }
  ASSERT_EQ(posted->getOutstandingPosnCount(), 1u)
      << "precondition: one packet is still on the wire, which is what the "
         "completion below must wait for";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeWqeThatFailsTheTag(*posted, kWrRemAccErr);
  }

  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "the client must not be told the send ended while a packet of it is "
         "still on the wire, pointing to a work request the report would free";
  {
    auto tagGuard = tag_->scopedLock();
    EXPECT_TRUE(posted->isFailing())
        << "the mark is what stops the tx path sending more packets for a "
           "work request already completed in error";
  }

  // The ack for the remaining packet is what settles it.
  {
    auto tagGuard = tag_->scopedLock();
    posted->acknowledgePosn(secondPosn);
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }

  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "once its last packet is acked the send has nothing left to wait for "
         "and must report";
  const auto cqe = folly::coro::blockingWait(cq_->pollCq());
  EXPECT_EQ(cqe.status, kWrRemAccErr)
      << "waiting for its packets must not cost the send the error it was "
         "completed with";
}

// A virtual send with nothing on the wire settles the moment it is marked, so
// completing it in error retires it in the same call and enqueues no CQE.
TEST_F(TagTest, CompleteVirtualSendWqeInError) {
  // Create a VirtualSendWqe
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 1000);
  int addressError = -1;
  tag_->createVirtualSendWqeIfNeeded(
      42, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 0);
  ASSERT_EQ(tag_->getNumPendingWqes(), 1)
      << "should have 1 virtual WQE pending";

  // Get a pointer to the VirtualSendWqe
  Wqe* virtualWqePtr = tag_->findNextWqe();
  ASSERT_NE(virtualWqePtr, nullptr);
  ASSERT_TRUE(virtualWqePtr->isVirtual());
  tag_->markFree();

  {
    auto tagGuard = tag_->scopedLock();
    tag_->markPendingWqesFailing(kWrFlushErr);
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }

  EXPECT_EQ(tag_->getNumPendingWqes(), 0)
      << "with no posn allocated the response is settled the moment it is "
         "marked, so the retirement that follows takes it";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "virtual WQEs should not generate CQEs even in error";
}

// Send selection must not hand back a WQE that a concurrent fatal-NAK flush is
// freeing. findNextWqe() returns a raw Wqe* that points into msnSpan_.wqes,
// and flushAllPendingWqesInError() (the receiver-thread fatal-NAK path in
// direct ack mode) frees those WQEs while holding the tag semaphore. If the
// semaphore is acquired only after the map lookup, a flush can free the WQE in
// the window between the lookup and the acquire, and the caller then
// dereferences freed memory. Run under ASAN/TSAN, where that
// lookup-before-acquire ordering trips a heap-use-after-free within a few
// rounds.
TEST_F(TagTest, FindNextWqeVsFatalNakFlushNoUseAfterFree) {
  // Regression for the findNextWqe-vs-fatal-NAK-flush use-after-free. Posting
  // runs only while the finders are idle, to keep this off the separate
  // nextMsn_ post-vs-send race.
  const int32_t savedMinLogLevel = FLAGS_minloglevel;
  FLAGS_minloglevel = google::GLOG_ERROR; // quiet the per-WQE flush WARNINGs

  constexpr int kRounds = 100;
  constexpr int kWqesPerRound = 32;
  constexpr int kFinders = 4;
  std::atomic<uint64_t> derefs{0};

  for (int round = 0; round < kRounds; ++round) {
    // Refill while the finders are idle (posting writes nextMsn_).
    ibvQp_->state = IBV_QPS_RTS;
    for (int i = 0; i < kWqesPerRound; ++i) {
      postToTag(
          *tag_,
          std::make_unique<SendWqe>(
              kWorkReqId, oneSge(buffer_->data(), 1000), kMtu),
          kMtu);
    }

    std::atomic<bool> flushed{false};
    std::vector<std::thread> finders;
    finders.reserve(kFinders);
    for (int t = 0; t < kFinders; ++t) {
      finders.emplace_back([&] {
        while (!flushed.load(std::memory_order_acquire)) {
          Wqe* wqe = tag_->findNextWqe();
          if (wqe != nullptr) {
            // On a freed WQE this read is the use-after-free under test.
            if (wqe->getMsn().has_value()) {
              derefs.fetch_add(1, std::memory_order_relaxed);
            }
            tag_->markFree();
          }
        }
      });
    }

    // Fatal NAK: error the QP and free all pending WQEs, racing the finders.
    ibvQp_->state = IBV_QPS_ERR;
    {
      auto guard = tag_->scopedLock();
      auto retirementGuard = tag_->lockForWqeRetirement();
      tag_->flushAllPendingWqesInError();
    }

    flushed.store(true, std::memory_order_release);
    for (auto& finder : finders) {
      finder.join();
    }
  }
  FLAGS_minloglevel = savedMinLogLevel;

  EXPECT_GT(derefs.load(), 0u)
      << "the finders should have selected and read at least one WQE";
  EXPECT_TRUE(tag_->isInErrorState())
      << "the flusher leaves the tag in the error state after the final round";
}

/*
 * Receive processing resolves a raw Wqe* out of the bound-receive map and
 * reads through it while a thread giving up on the tag frees those same WQEs;
 * accessForReceive() holds that off for the span the pointer is live. The
 * yield stands in for the validate-and-scatter span, without which the two
 * almost never collide. The signal is a sanitizer report rather than a failed
 * expectation, and only mostly reliable: dropping the acquisition trips ASAN
 * in 8 of 10 stress runs.
 */
TEST_F(TagTest, ReceiveDeliveryVsFlushNoUseAfterFree) {
  const int32_t savedMinLogLevel = FLAGS_minloglevel;
  FLAGS_minloglevel = google::GLOG_ERROR; // quiet the per-WQE flush WARNINGs

  constexpr int kRounds = 400;
  constexpr uint16_t kReceivesPerRound = 32;
  constexpr int kReceivers = 4;
  std::atomic<uint64_t> derefs{0};

  for (int round = 0; round < kRounds; ++round) {
    // Post and bind while the receivers are idle: binding writes the map they
    // read from, and the receive side has one writer.
    ibvQp_->state = IBV_QPS_RTS;
    for (uint16_t i = 0; i < kReceivesPerRound; ++i) {
      postToTag(
          *tag_,
          std::make_unique<ReceiveWqe>(
              kWorkReqId, oneSge(buffer_->data(), 1000), kMtu),
          kMtu);
    }
    // The Tag hands out csns in one sequence across rounds, so this round's
    // start where the last round's ended.
    for (uint16_t msn = 1,
                  csn = static_cast<uint16_t>(
                      kFixtureRxCsn + round * kReceivesPerRound);
         msn <= kReceivesPerRound;
         ++msn, ++csn) {
      ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(csn, msn, kMtu))
          << "every posted receive must bind to a message before the race";
    }

    std::atomic<bool> flushed{false};
    std::vector<std::thread> receivers;
    receivers.reserve(kReceivers);
    for (int t = 0; t < kReceivers; ++t) {
      receivers.emplace_back([&, t] {
        const uint16_t msn = static_cast<uint16_t>((t % kReceivesPerRound) + 1);
        while (!flushed.load(std::memory_order_acquire)) {
          auto access = tag_->accessForReceive();
          if (access.isTagInError()) {
            continue;
          }
          Wqe* wqe =
              tag_->getReceiveWqe(BaseHeader::Opcode::OperationType::SEND, msn);
          if (wqe != nullptr) {
            std::this_thread::yield();
            // On a freed WQE this read is the use-after-free under test.
            if (wqe->getMessageSizeBytes() > 0) {
              derefs.fetch_add(1, std::memory_order_relaxed);
            }
          }
        }
      });
    }

    // Give up on the tag, freeing every bound receive under the receivers.
    // The transition goes under the guard with the flush, as every failing
    // path does, so it is not itself a race with the receivers reading it.
    {
      auto guard = tag_->scopedLock();
      auto retirementGuard = tag_->lockForWqeRetirement();
      ibvQp_->state = IBV_QPS_ERR;
      tag_->flushAllPendingWqesInError();
    }

    flushed.store(true, std::memory_order_release);
    for (auto& receiver : receivers) {
      receiver.join();
    }
  }
  FLAGS_minloglevel = savedMinLogLevel;

  EXPECT_GT(derefs.load(), 0u)
      << "the receivers should have resolved and read at least one receive WQE";
  EXPECT_EQ(tag_->getNumPendingRWqes(), 0)
      << "the flusher leaves no receive outstanding after the final round";
}

/*
 * A read response resolves its original read WQE out of the send map and reads
 * through that pointer, and a post landing on a tag already in error retires
 * the work requests it finds settled -- freeing WQEs delivery may still hold.
 * processTransmitWqe() takes the retirement guard before retiring, which is
 * what holds the free off until the reader is done.
 *
 * The receivers stand for one that passed its error check while the tag was
 * healthy and is still inside the span the guard covers, so they do not
 * re-check. Erroring the tag before they start models that receiver, and keeps
 * the deliberately unsynchronized verbs-API state write out of the race. As
 * with the sibling case above the signal is a sanitizer report rather than a
 * failed expectation, and here dropping the acquisition reports the free on
 * every run.
 */
TEST_F(TagTest, PostToErroredTagVsResponseDeliveryNoUseAfterFree) {
  const int32_t savedMinLogLevel = FLAGS_minloglevel;
  FLAGS_minloglevel = google::GLOG_ERROR; // quiet the per-WQE flush WARNINGs

  constexpr int kRounds = 200;
  constexpr uint16_t kReadsPerRound = 16;
  constexpr int kReceivers = 4;
  // Stands in for the validate-and-scatter span a receiver holds a WQE across.
  // Shortening it to one read lets every receiver finish before the post below
  // reaches its retirement.
  constexpr int kReadsPerDelivery = 256;
  std::atomic<uint64_t> derefs{0};

  auto makeRead = [this] {
    return std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kRead,
        kWorkReqId,
        oneSge(buffer_->data(), 1000),
        kMtu,
        0xDEAD0000,
        42);
  };

  for (int round = 0; round < kRounds; ++round) {
    // Post while the receivers are idle, so the map they read is written by
    // one thread.
    ibvQp_->state = IBV_QPS_RTS;
    const uint16_t firstMsn = tag_->getNextMsn();
    for (uint16_t i = 0; i < kReadsPerRound; ++i) {
      postToTag(*tag_, makeRead(), kMtu);
    }
    {
      // Settles the work requests the post below retires, and errors the tag
      // so that post takes its error branch. Both happen before the receivers
      // start, so neither paces the race.
      auto guard = tag_->scopedLock();
      auto retirementGuard = tag_->lockForWqeRetirement();
      tag_->markPendingWqesFailing(kWrFlushErr);
      ibvQp_->state = IBV_QPS_ERR;
    }

    std::atomic<bool> retired{false};
    std::atomic<int> delivering{0};
    std::vector<std::thread> receivers;
    receivers.reserve(kReceivers);
    for (int t = 0; t < kReceivers; ++t) {
      receivers.emplace_back([&, t] {
        const uint16_t msn = firstMsn + static_cast<uint16_t>(t);
        bool announced = false;
        while (!retired.load(std::memory_order_acquire)) {
          auto access = tag_->accessForReceive();
          Wqe* wqe = tag_->getReceiveWqe(
              BaseHeader::Opcode::OperationType::READ_RESP, msn);
          if (wqe != nullptr) {
            if (!announced) {
              announced = true;
              delivering.fetch_add(1, std::memory_order_release);
            }
            for (int i = 0; i < kReadsPerDelivery; ++i) {
              std::this_thread::yield();
              // On a freed WQE this read is the use-after-free under test.
              if (wqe->getMessageSizeBytes() > 0) {
                derefs.fetch_add(1, std::memory_order_relaxed);
              }
            }
          }
        }
      });
    }

    // Post only once every receiver is delivering, so the retirement lands on
    // threads holding a WQE rather than on threads that have not started.
    // Bounded so that a receiver which stops resolving its WQE reports here
    // instead of spinning until the target times out.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (delivering.load(std::memory_order_acquire) < kReceivers &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    const bool allDelivering =
        delivering.load(std::memory_order_acquire) == kReceivers;

    // The post that retires those work requests, racing the receivers.
    {
      auto guard = tag_->scopedLock();
      postToTag(*tag_, makeRead(), kMtu);
    }

    retired.store(true, std::memory_order_release);
    for (auto& receiver : receivers) {
      receiver.join();
    }
    ASSERT_TRUE(allDelivering)
        << "every receiver must resolve its read WQE before the post retires "
           "them, or nothing is holding one when the retirement runs";
  }
  FLAGS_minloglevel = savedMinLogLevel;

  EXPECT_GT(derefs.load(), 0u)
      << "the receivers should have resolved and read at least one read WQE, "
         "without which no delivery is holding one when the retirement runs";
  EXPECT_EQ(tag_->getNumPendingWqes(), 0)
      << "the post to the errored tag should have retired every settled "
         "failing WQE, which is the free the receivers race";
}

/*
 * The guard excludes WQE retirement, not other receivers, so two receivers hold
 * it at once. A plain mutex in its place would deadlock here: the second
 * acquire would wait on the first, which is waiting for it. As with the other
 * lock tests in this suite, the hang is the signal -- the target timeout fires
 * and the expectation below is never reached.
 */
TEST_F(TagTest, ReceiveAccessAdmitsTwoReceiversAtOnce) {
  folly::Baton<> firstIsIn;
  folly::Baton<> secondIsIn;
  std::atomic<int> heldTogether{0};

  std::thread first([&] {
    auto access = tag_->accessForReceive();
    firstIsIn.post();
    // Held until the second receiver is also in, which it can only manage if
    // this acquisition does not exclude it.
    secondIsIn.wait();
    heldTogether.fetch_add(1);
  });
  std::thread second([&] {
    firstIsIn.wait();
    auto access = tag_->accessForReceive();
    heldTogether.fetch_add(1);
    secondIsIn.post();
  });
  first.join();
  second.join();

  EXPECT_EQ(heldTogether.load(), 2)
      << "both receivers must have held the tag at the same time";
}

/*
 * A tag that refuses a receive still gives its queue entry back. Both refusals
 * are states a client can keep posting into, so an entry kept on either one
 * leaves the receive depth smaller for good.
 */
TEST_F(TagTest, AReceiveTheTagRefusesGivesBackItsQueueEntry) {
  ibvQp_->state = IBV_QPS_ERR;
  // The return says whether to signal the sender, which a receive never is, so
  // it cannot tell a refusal from a receive taken on.
  postToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId, oneSge(buffer_->data(), 1000), kMtu),
      kMtu);
  ASSERT_EQ(tag_->getNumPendingRWqes(), 0)
      << "precondition: a tag in error refuses a newly posted receive rather "
         "than holding it";
  EXPECT_EQ(tag_->getAcceptedReceiveQueueEntries(), 0)
      << "a tag in error must give the queue entry back, or the depth closes "
         "on a client that never got one back";

  ibvQp_->state = IBV_QPS_RTS;
  tag_->markForDeletion();
  postToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId + 1, oneSge(buffer_->data(), 1000), kMtu),
      kMtu);
  ASSERT_EQ(tag_->getNumPendingRWqes(), 0)
      << "precondition: a tag being removed refuses a newly posted receive "
         "rather than holding it";
  EXPECT_EQ(tag_->getAcceptedReceiveQueueEntries(), 0)
      << "a tag being removed must give the queue entry back too";
}

/*
 * The queue entry a receive claims at the post comes back when the receive
 * reports. A client that posts and reaps at the advertised depth depends on
 * it: unreleased, the depth closes after the first full round.
 */
TEST_F(TagTest, CompletingAReceiveReturnsItsQueueEntry) {
  auto rwqe = std::make_unique<ReceiveWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000, ibvMr_->lkey), kMtu);
  postToTag(*tag_, std::move(rwqe), kMtu);
  ASSERT_EQ(tag_->getAcceptedReceiveQueueEntries(), 1)
      << "precondition: the post claimed a queue entry, or there is nothing "
         "for the completion to return";

  // The tag's first posted receive takes the starting csn.
  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, kMtu))
      << "precondition: the arriving message binds to the posted receive";
  ASSERT_TRUE(tag_->markDeliveredAndCompleteInOrder(/*msn=*/1))
      << "precondition: the receive completes on the csn the sequence expects";
  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "precondition: the receive reports, or no completion releases its entry";

  EXPECT_EQ(tag_->getAcceptedReceiveQueueEntries(), 0)
      << "a completed receive must return its queue entry; held, it consumes "
         "the depth for the life of the queue pair";
}

/*
 * A responder builds its own receives into the same map a client's posts land
 * in, but they never pass through a post and so hold no queue entry. Counting
 * one would take depth from the client for work the client never asked for.
 */
TEST_F(TagTest, AVirtualReceiveHoldsNoQueueEntry) {
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 1000);
  int addressError = -1;
  tag_->createVirtualRWqeIfNeeded(
      /*msn=*/1, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 0)
      << "precondition: the write is accepted, or no virtual receive exists";
  EXPECT_EQ(tag_->getAcceptedReceiveQueueEntries(), 0)
      << "a responder's own receive claims no queue entry";

  EXPECT_TRUE(tag_->markDeliveredAndCompleteInOrder(/*msn=*/1))
      << "a virtual receive has no csn, so completing one leaves the "
         "sequence where it stood rather than reading as out of order";
  EXPECT_EQ(tag_->getAcceptedReceiveQueueEntries(), 0)
      << "and returns none when it completes; releasing one here would abort "
         "inside that completion, which checks the count it decrements";
}

TEST_F(TagTest, FlushingReceivesInErrorReturnsTheirQueueEntries) {
  for (uint64_t i = 0; i < 3; ++i) {
    postToTag(
        *tag_,
        std::make_unique<ReceiveWqe>(
            kWorkReqId + i, oneSge(buffer_->data(), 1000, ibvMr_->lkey), kMtu),
        kMtu);
  }
  ASSERT_EQ(tag_->getAcceptedReceiveQueueEntries(), 3)
      << "precondition: three posts claimed three queue entries";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushPendingReceiveWqesInError();
  }

  EXPECT_EQ(tag_->getAcceptedReceiveQueueEntries(), 0)
      << "every flushed receive must give its queue entry back; kept, they "
         "close the depth on a queue pair the client may still reset and reuse";
}

/*
 * A receive whose buffer no longer validates is reported where it stands, and
 * gives its queue entry back there rather than waiting for a completion that
 * will not come.
 */
TEST_F(TagTest, AReceiveEndedByALocalProtectionErrorReturnsItsQueueEntry) {
  postToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId, oneSge(buffer_->data(), 1000, /*lkey=*/0), kMtu),
      kMtu);
  ASSERT_EQ(tag_->getAcceptedReceiveQueueEntries(), 1)
      << "precondition: the post claimed a queue entry";
  // The tag's first posted receive takes the starting csn.
  ASSERT_TRUE(tag_->assignInitiatorMsnToReceiveWqeIfNeeded(
      kFixtureRxCsn, /*msn=*/1, kMtu))
      << "precondition: the arriving message binds to the posted receive";

  ASSERT_TRUE(tag_->recordLocalProtectionErrorIfUnwritable(1, *memoryRegions_))
      << "precondition: an unregistered key fails validation, or nothing ends "
         "the receive early";
  EXPECT_EQ(tag_->getAcceptedReceiveQueueEntries(), 1)
      << "recording the error does not report the receive, so it still holds "
         "the queue entry it was posted into";

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushPendingReceiveWqesInError();
  }

  EXPECT_EQ(tag_->getAcceptedReceiveQueueEntries(), 0)
      << "the flush that reports it must return its queue entry too; kept, it "
         "closes the depth on a client that never got one back";
}

/*
 * The sweep is how a tag in error reaches work no other path owns. A tag can
 * hold receives and no send work at all -- the send side completed, or there
 * never was any -- and deciding on the pending send count alone would return
 * before the receives were reported, stranding them until the queue pair is
 * torn down.
 */
TEST_F(TagTest, TheSweepFlushesReceivesOnATagHoldingNoSendWork) {
  postToTag(
      *tag_,
      std::make_unique<ReceiveWqe>(
          kWorkReqId, oneSge(buffer_->data(), 1000, ibvMr_->lkey), kMtu),
      kMtu);
  ASSERT_EQ(tag_->getNumPendingRWqes(), 1)
      << "precondition: the tag holds a posted receive";
  ASSERT_EQ(tag_->getNumPendingWqes(), 0)
      << "precondition: and no send work, which is the count the sweep must "
         "not decide on alone";

  ibvQp_->state = IBV_QPS_ERR;
  {
    auto tagGuard = tag_->scopedLock();
    tag_->failPendingWqesIfInErrorWithTagHeld();
  }

  EXPECT_EQ(tag_->getNumPendingRWqes(), 0)
      << "the sweep must report the receives of a tag in error whether or not "
         "it also holds send work; left pending, no message ever fills them";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "the receive it posted must be reported to the client";
}

/*
 * A read held for its request frame's ack stays in the map after reporting, at
 * an msn the completion cursor has passed. The walk that fails a tag's work
 * starts from that cursor, so the held read falls outside it and the work
 * posted behind -- the work still pending -- is what the walk reaches.
 */
TEST_F(TagTest, WorkBehindAHeldReadIsStillReachedByTheMapWalk) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* read = readWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(readWqe), kMtu))
      << "precondition: the tag accepts the read, or nothing is held at the "
         "head of the map";
  // The request frame goes out; the read holds the map head until it settles.
  std::ignore = read->allocateNextPosn();

  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 1000), kMtu);
  Wqe* send = sendWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(sendWqe), kMtu))
      << "precondition: the send is accepted, so it sits behind the read";

  // The response reports the read, but its request frame keeps the WQE in the
  // map: the head is now an entry the walk must pass rather than stop at.
  read->onPosnReceived(0);
  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    EXPECT_TRUE(tag_->handleTxWqesCompletion({*read}))
        << "precondition: completing the send finds no csn out of sequence";
  }
  ASSERT_TRUE(tag_->isTxCompletionCommitted(read->getMsn().value()))
      << "precondition: the read reports on its response, or it is not the "
         "held entry this test is about";

  {
    auto tagGuard = tag_->scopedLock();
    tag_->markPendingWqesFailing(kWrRemAccErr);
  }

  EXPECT_TRUE(send->isFailing())
      << "the send behind the held read must be reached and failed, or the "
         "client waits on a completion that never comes";
  EXPECT_FALSE(read->isFailing())
      << "the read already reported, so failing it again would report a work "
         "request the client has been told about";
}

/*
 * Posn 0 is both the slot a read's request occupies in flight and the one its
 * first response lands in, so a response resolves it. The request's ack must
 * still be recorded, and until it is the WQE has to stay: the transmit tracker
 * points to it and would follow a freed pointer.
 */
TEST_F(TagTest, ResponseBeforeRequestAckHoldsTheReadUntilItIsAcked) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* read = readWqe.get();
  postToTag(*tag_, std::move(readWqe), kMtu);
  const uint16_t msn = read->getMsn().value();
  answerTheOnlyFrame(*read);

  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "the responses settle a read, so its completion is reported on them";
  EXPECT_TRUE(tag_->isTxCompletionCommitted(msn))
      << "a delivered completion must count as committed, or the walk would "
         "still treat the work request as unreported";
  EXPECT_EQ(
      tag_->getReceiveWqe(BaseHeader::Opcode::OperationType::READ_RESP, msn),
      nullptr)
      << "the read is reported, so a further response for it is a duplicate "
         "and must not be handed a WQE to scatter into";

  // The WQE is nonetheless still held: the request frame is on the wire and
  // the tracker points to it. Reaching it here at all is what says so --
  // freeing it under the ack path would make this a use-after-free.
  read->onFrameNewlyAcked(Wqe::kRequestFrameWirePosn);
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->retireAfterRequestFrameSettled(*read);
  }

  EXPECT_EQ(
      tag_->getReceiveWqe(BaseHeader::Opcode::OperationType::READ_RESP, msn),
      nullptr)
      << "with the request acked no claim holds the read, so it retires";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "retiring a read that already reported must not report it twice";
}

/*
 * A read reported by its responses is held only for the request frame the
 * tracker still points to. If the tag then fails, the marking passes over it
 * -- it has already reported -- so the settling rule never applies to it. The
 * frame leaving the wire by abandonment rather than by ack has to retire it
 * just the same, or no later event will.
 */
TEST_F(
    TagTest,
    AReportedReadRetiresWhenItsRequestIsAbandonedAfterTheTagErrors) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* wqe = readWqe.get();
  postToTag(*tag_, std::move(readWqe), kMtu);
  const uint16_t msn = wqe->getMsn().value();
  const uint32_t requestPosn = wqe->allocateNextPosn();

  // The response lands and reports the read; the request is still on the wire.
  wqe->onPosnReceived(0);
  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    EXPECT_TRUE(tag_->handleTxWqesCompletion({*wqe}))
        << "precondition: completing the send finds no csn out of sequence";
  }
  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "the responses settle a read, so it reports on them";
  ASSERT_TRUE(tag_->isTxCompletionCommitted(msn))
      << "it has reported, so it is held only for its request frame";
  ASSERT_EQ(tag_->getNumHeldTxWqes(), 1u)
      << "the tracker still holds the frame, so the wqe may not be freed yet";

  tag_->markPendingWqesFailing(kWrRemAccErr);
  EXPECT_FALSE(wqe->isFailing())
      << "a work request that has already reported is not marked again";

  // The scan abandons the frame rather than the peer acking it.
  wqe->abandonFrame(requestPosn);
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }

  EXPECT_EQ(tag_->getNumHeldTxWqes(), 0u)
      << "with its request off the wire no claim holds the read; leaving it in "
         "the map strands a work request until the qp is torn down";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "it reported when its responses landed, so retiring it here must not "
         "report it a second time";
}

/*
 * A read reported on its responses is held only until its request frame is
 * acked. Tearing the QP down in that window must not report it a second time:
 * the client has already been told it succeeded.
 */
TEST_F(TagTest, TeardownDoesNotReportAReadItAlreadyCompleted) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* wqe = readWqe.get();
  postToTag(*tag_, std::move(readWqe), kMtu);
  answerTheOnlyFrame(*wqe);
  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "the responses report the read before teardown begins";
  const auto success = folly::coro::blockingWait(cq_->pollCq());
  ASSERT_EQ(success.status, kSuccess)
      << "the read succeeded, which is what must not be contradicted below";

  // What ~Qp() does to each tag.
  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushAllPendingWqesInError();
  }

  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "a work request reported once must not be reported again; a flush "
         "here would follow success with WR_FLUSH_ERR for the same wr_id";
}

/*
 * The watchdog asks for the earliest work request still waiting to complete. A
 * read that has already reported is not one, however long its request frame
 * takes to be acked.
 */
TEST_F(TagTest, TheEarliestPendingWqeSkipsOneAlreadyReported) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* wqe = readWqe.get();
  postToTag(*tag_, std::move(readWqe), kMtu);
  answerTheOnlyFrame(*wqe);

  EXPECT_EQ(tag_->getEarliestPendingWqe(), nullptr)
      << "the only work request this tag holds has been reported, so the "
         "watchdog must not treat it as work making no progress";
}

/*
 * The ack can beat the responses. Resolving the request then leaves the read
 * waiting only on them, and the completion retires it.
 */
TEST_F(TagTest, RequestAckBeforeResponseRetiresOnTheResponse) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* read = readWqe.get();
  postToTag(*tag_, std::move(readWqe), kMtu);
  const uint16_t msn = read->getMsn().value();
  // The request frame goes out; the ack below identifies it by its wire posn.
  std::ignore = read->allocateNextPosn();

  read->onFrameNewlyAcked(Wqe::kRequestFrameWirePosn);
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->retireAfterRequestFrameSettled(*read);
  }

  EXPECT_EQ(cq_->getNumCompletedWqes(), 0)
      << "the request's ack does not decide the read's result, which "
         "only its responses report";
  EXPECT_NE(
      tag_->getReceiveWqe(BaseHeader::Opcode::OperationType::READ_RESP, msn),
      nullptr)
      << "the read is still awaiting its responses, so it stays";

  read->onPosnReceived(0);
  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    EXPECT_TRUE(tag_->handleTxWqesCompletion({*read}))
        << "precondition: completing the send finds no csn out of sequence";
  }

  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "the responses are in, which is what the caller waits on to learn the "
         "read's result";
  EXPECT_EQ(
      tag_->getReceiveWqe(BaseHeader::Opcode::OperationType::READ_RESP, msn),
      nullptr)
      << "with the request already acked the completion is free to retire it";
}

/*
 * Completions are delivered in msn order, so a later read whose responses
 * arrive first is not reported until the earlier one is. Its request ack lands
 * in that window, before its completion is committed, and must leave the WQE in
 * place for the completion to retire.
 */
TEST_F(TagTest, RequestAckBeforeCommittedCompletionLeavesTheReadToComplete) {
  std::vector<Wqe*> reads;
  for (int i = 0; i < 2; ++i) {
    auto readWqe = std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kRead,
        kWorkReqId + i,
        oneSge(buffer_->data(), kMtu),
        kMtu,
        0x1000,
        ibvMr_->rkey);
    reads.push_back(readWqe.get());
    postToTag(*tag_, std::move(readWqe), kMtu);
    // Each read puts its request frame on the wire ahead of the responses.
    std::ignore = reads.back()->allocateNextPosn();
  }
  const uint16_t laterMsn = reads[1]->getMsn().value();

  // The second read's responses arrive first; in-order delivery holds its
  // completion behind the first.
  reads[1]->onPosnReceived(0);
  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    EXPECT_TRUE(tag_->handleTxWqesCompletion({*reads[1]}))
        << "precondition: completing the send finds no csn out of sequence";
  }
  ASSERT_EQ(cq_->getNumCompletedWqes(), 0)
      << "precondition: the earlier read has not completed, so this one cannot "
         "be reported yet without delivering completions out of order";
  EXPECT_FALSE(tag_->isTxCompletionCommitted(laterMsn))
      << "precondition: the completion is uncommitted, which is what the "
         "request ack below has to recognize";

  // Its request is acked while its completion is still pending.
  reads[1]->onFrameNewlyAcked(Wqe::kRequestFrameWirePosn);
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->retireAfterRequestFrameSettled(*reads[1]);
  }
  EXPECT_NE(
      tag_->getReceiveWqe(
          BaseHeader::Opcode::OperationType::READ_RESP, laterMsn),
      nullptr)
      << "its completion has not been delivered, so retiring it here would "
         "lose the report the caller has not had";

  // The earlier read completes, releasing both in order.
  reads[0]->onPosnReceived(0);
  reads[0]->onFrameNewlyAcked(Wqe::kRequestFrameWirePosn);
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->retireAfterRequestFrameSettled(*reads[0]);
  }
  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    EXPECT_TRUE(tag_->handleTxWqesCompletion({*reads[0]}))
        << "precondition: completing the send finds no csn out of sequence";
  }

  EXPECT_EQ(cq_->getNumCompletedWqes(), 2)
      << "both reads must report once the earlier one unblocks the order";
  EXPECT_EQ(
      tag_->getReceiveWqe(
          BaseHeader::Opcode::OperationType::READ_RESP, laterMsn),
      nullptr)
      << "its request was already acked, so its completion retires it";
}

/*
 * A read held only for its request ack has already reported. Failing the tag's
 * work must pass over it: marking it failing would report it a second time.
 */
TEST_F(TagTest, ErroringATagDoesNotReportAReadHeldOnlyForItsRequestAck) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* wqe = readWqe.get();
  postToTag(*tag_, std::move(readWqe), kMtu);
  answerTheOnlyFrame(*wqe);
  ASSERT_EQ(cq_->getNumCompletedWqes(), 1)
      << "precondition: the read has already reported, which is the report the "
         "failure below must not duplicate";
  EXPECT_EQ(tag_->getNumPendingWqes(), 0)
      << "a read that has reported is finished work, not pending work";

  tag_->markPendingWqesFailing(kWrFlushErr);
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }

  EXPECT_EQ(cq_->getNumCompletedWqes(), 1)
      << "it already reported success; a flush must not report it again";
}

TEST_F(TagTest, FlushingAMapOfOnlyHeldReadsLeavesTheCursorsAgreeing) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* wqe = readWqe.get();
  postToTag(*tag_, std::move(readWqe), kMtu);
  answerTheOnlyFrame(*wqe);
  ASSERT_EQ(tag_->getNumHeldTxWqes(), 1u)
      << "precondition: the read reported and stayed for its request frame, so "
         "the map holds an msn the completed one has already passed";
  ASSERT_EQ(tag_->getNumPendingWqes(), 0)
      << "precondition: no pending work request remains to carry the retired "
         "msn over the held one, which is what leaves the flush below nothing "
         "to move the cursor with";

  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushAllPendingWqesInError();
  }

  EXPECT_EQ(tag_->getNumHeldTxWqes(), 0u)
      << "the flush erases the held read along with the rest of the map";
  EXPECT_EQ(tag_->getNumPendingWqes(), 0)
      << "an emptied map holds no pending work; a count taken as the distance "
         "between the cursors would answer for work the tag cannot produce";
}

/*
 * One pass retires the whole run of settled work at the map head, so a sweep
 * that collected a pointer to each member of that run holds freed pointers for
 * every one but the first.
 */
TEST_F(TagTest, ARunOfReadsAbandonedTogetherRetiresInOnePass) {
  auto holdReportedRead = [this](uint64_t workReqId) {
    auto readWqe = std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kRead,
        workReqId,
        oneSge(buffer_->data(), kMtu),
        kMtu,
        0x1000,
        ibvMr_->rkey);
    Wqe* read = readWqe.get();
    EXPECT_TRUE(postToTag(*tag_, std::move(readWqe), kMtu))
        << "precondition: the tag accepts the read, or there is no run to "
           "retire";
    answerTheOnlyFrame(*read);
    EXPECT_TRUE(tag_->isTxCompletionCommitted(read->getMsn().value()))
        << "precondition: the response reports the read, or it is not the "
           "reported-and-retained case this test is about";
    EXPECT_TRUE(read->isRequestFrameAllocated())
        << "precondition: the request frame keeps the read in the map, or it "
           "has already retired and there is no run";
    return read;
  };

  Wqe* first = holdReportedRead(kWorkReqId);
  Wqe* second = holdReportedRead(kWorkReqId + 1);
  ASSERT_EQ(tag_->getNumHeldTxWqes(), 2)
      << "precondition: both reads sit in the map, contiguous at its head";

  // Both requests leave the wire without an ack, which is what the sweep finds.
  first->abandonFrame(0);
  second->abandonFrame(0);

  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->completeSettledFailedWqes();
  }

  EXPECT_EQ(tag_->getNumHeldTxWqes(), 0)
      << "a read left in the map is one nothing will come back for, since its "
         "request frame is gone and its completion is already reported";
  EXPECT_EQ(cq_->getNumCompletedWqes(), 2)
      << "each read reported once on its response; retiring must not report "
         "either of them again";
}

/*
 * The retirement walk leaves through the cursor it entered by, and the msn
 * before zero is where that conversion is modular rather than arithmetic.
 */
TEST_F(TagTest, RetiringTheMsnBeforeZeroLeavesTheCursorOnIt) {
  constexpr uint16_t kLastMsn = 65535;
  numberTransmitFrom(kLastMsn);

  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  Wqe* send = sendWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(sendWqe), kMtu))
      << "precondition: the tag accepts the send that sits on the last msn";
  ASSERT_EQ(send->getMsn().value(), kLastMsn)
      << "precondition: it holds the msn before zero, or the walk never "
         "reaches the wrap";

  answerTheOnlyFrame(*send);

  EXPECT_EQ(tag_->getNumHeldTxWqes(), 0)
      << "the send retires: nothing holds it and its completion is committed";
  EXPECT_TRUE(tag_->hasMsnSpanRoom())
      << "the cursor must come to rest on the msn before zero rather than on "
         "zero itself, which would read as a whole sequence space ahead of the "
         "next msn and refuse every later post";
}

/*
 * A work request gives its queue entry back when it reports, and leaves the
 * map only when it retires. Retirement is in order and stops at a read whose
 * request frame the wire has not answered, so what accumulates between the two
 * is counted by nothing: the queue depth has already released it, and the only
 * remaining limit is the 16-bit msn the map is keyed on.
 */
TEST_F(TagTest, WorkHeldBehindAStalledRequestFrameOutgrowsTheSendQueue) {
  // The retired cursor stays here: the read below holds the map head for the
  // whole case.
  const uint16_t retiredCursor = tag_->getNextMsn() - 1;

  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* read = readWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(readWqe), kMtu))
      << "precondition: the tag accepts the read that blocks the head";
  answerTheOnlyFrame(*read);
  ASSERT_TRUE(read->isRequestFrameAllocated())
      << "precondition: the read is reported but held for its request frame, "
         "or nothing behind it is blocked and the case proves nothing";

  // Every send reports, so the entry it took is back before the next is
  // posted and the depth never refuses one.
  const size_t heldBehindRead = kMaxQpWorkRequests - 1;
  for (size_t i = 0; i < heldBehindRead; ++i) {
    auto sendWqe = std::make_unique<SendWqe>(
        kWorkReqId + 1 + i, oneSge(buffer_->data(), 1000), kMtu);
    Wqe* send = sendWqe.get();
    ASSERT_TRUE(postToTag(*tag_, std::move(sendWqe), kMtu))
        << "precondition: the send queue refused post " << i
        << ", so the depth is bounding what the map holds and the span this "
           "case is about cannot form";
    answerTheOnlyFrame(*send);
  }

  EXPECT_EQ(tag_->getNumHeldTxWqes(), kMaxQpWorkRequests)
      << "the map holds a full send queue's worth of work that has already "
         "reported, all of it behind the one read";
  EXPECT_EQ(tag_->getAcceptedSendQueueEntries(), 0)
      << "and the queue reads empty, so the depth refuses nothing: the entry "
         "count releases at completion and cannot see what retirement holds";

  EXPECT_FALSE(after(tag_->getNextMsn(), retiredCursor))
      << "the next msn to assign reads as before the retired cursor, so the "
         "post below is the one that would leave the ordering window";

  EXPECT_FALSE(tag_->hasMsnSpanRoom())
      << "the tag reports no room for another msn, which is what holds the "
         "next post in the queue: an msn past the window is one the tag "
         "cannot place against its own cursors, and retirement stops "
         "advancing for good once that comparison inverts";

  // The run behind the read was retirable throughout; every completion above
  // turned back at the head.
  read->onFrameNewlyAcked(Wqe::kRequestFrameWirePosn);
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->retireAfterRequestFrameSettled(*read);
  }

  EXPECT_EQ(tag_->getNumHeldTxWqes(), 0)
      << "one read's request frame was holding a full queue depth of work "
         "that had already reported, and releasing it drains all of them in "
         "one pass";
  EXPECT_TRUE(tag_->hasMsnSpanRoom())
      << "and the hold was backpressure, not a wedge: the window reopens as "
         "soon as the work behind the read retires";
}

/*
 * A tag in error can hold work that has already reported and stayed only for
 * its request frame. Counting the work still pending does not see that work,
 * so a sweep that decides on the count alone leaves it in the map.
 */
TEST_F(TagTest, TheErrorSweepRetiresWorkHeldOnlyForARequestFrameThatHasGone) {
  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* read = readWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(readWqe), kMtu))
      << "precondition: the tag accepts the read, or it holds nothing for the "
         "sweep to reach";
  answerTheOnlyFrame(*read);
  // The request leaves the wire without an ack, so no ack path will come back
  // to retire the read: the sweep is the only thing left that can.
  read->abandonFrame(0);

  ASSERT_EQ(tag_->getNumPendingWqes(), 0)
      << "precondition: the read has reported, so it counts as no pending "
         "work -- which is the count the sweep must not decide on alone";
  ASSERT_EQ(tag_->getNumHeldTxWqes(), 1u)
      << "precondition: the map still holds the read";

  ibvQp_->state = IBV_QPS_ERR;
  {
    auto tagGuard = tag_->scopedLock();
    tag_->failPendingWqesIfInErrorWithTagHeld();
  }

  EXPECT_EQ(tag_->getNumHeldTxWqes(), 0u)
      << "the sweep must retire a read whose request frame has gone; left in "
         "the map it is work nothing will ever come back for";
}

/*
 * A read holds a slot in the peer's responder table, and max_rd_atomic is how
 * many the client said the peer would hold. The tag counts the reads its map
 * holds against that, so a read admitted past the depth is one the peer never
 * agreed to answer.
 */
TEST_F(TagTest, AReadSpendsTheDepthTheClientNegotiated) {
  tag_->setMaxRdAtomic(1);
  ASSERT_TRUE(tag_->hasReadAtomicRoom())
      << "precondition: a tag negotiated one slot starts with it free";

  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  ASSERT_TRUE(postToTag(*tag_, std::move(readWqe), kMtu))
      << "precondition: the tag accepts the read";

  EXPECT_FALSE(tag_->hasReadAtomicRoom())
      << "the read holds the only slot the client negotiated, so admitting a "
         "second would put more on the wire than the peer agreed to answer";
}

/*
 * Only reads and atomics draw on that depth. A send is answered by an ack
 * rather than a response, so it occupies nothing in the responder's table.
 */
TEST_F(TagTest, ASendDoesNotSpendTheReadAtomicDepth) {
  tag_->setMaxRdAtomic(1);

  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu);
  ASSERT_TRUE(postToTag(*tag_, std::move(sendWqe), kMtu))
      << "precondition: the tag accepts the send";

  EXPECT_TRUE(tag_->hasReadAtomicRoom())
      << "a send takes no responder slot, so counting it would stall reads "
         "behind work the peer never has to hold";
}

/*
 * The slot a read holds is the peer's, released when the read retires. Counting
 * the map's contents means retirement gives the slot back on its own, so a read
 * that has come and gone leaves the depth free for the next one.
 */
TEST_F(TagTest, RetiringAReadReturnsTheSlotItSpent) {
  tag_->setMaxRdAtomic(1);

  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  Wqe* read = readWqe.get();
  ASSERT_TRUE(postToTag(*tag_, std::move(readWqe), kMtu))
      << "precondition: the tag accepts the read";
  ASSERT_FALSE(tag_->hasReadAtomicRoom())
      << "precondition: the read holds the only slot the client negotiated";

  // The read reports but stays in the map, held for the request frame the wire
  // has not answered; the slot is not returned until that frame settles.
  answerTheOnlyFrame(*read);
  EXPECT_FALSE(tag_->hasReadAtomicRoom())
      << "a read reported but still in the map is a slot the peer still holds, "
         "so the depth stays spent until it retires";

  read->onFrameNewlyAcked(Wqe::kRequestFrameWirePosn);
  {
    auto tagGuard = tag_->scopedLock();
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->retireAfterRequestFrameSettled(*read);
  }
  ASSERT_EQ(tag_->getNumHeldTxWqes(), 0)
      << "precondition: the read has retired and left the map";

  EXPECT_TRUE(tag_->hasReadAtomicRoom())
      << "retirement returns the responder slot the read held, so the next "
         "read is free to go out";
}

/*
 * An error flush discards the map without retiring each entry through the head,
 * so the count is reset outright rather than decremented. A tag recovering from
 * error must not carry a phantom slot held by work it threw away.
 */
TEST_F(TagTest, TheErrorFlushReturnsEveryReadAtomicSlot) {
  tag_->setMaxRdAtomic(1);

  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  ASSERT_TRUE(postToTag(*tag_, std::move(readWqe), kMtu))
      << "precondition: the tag accepts the read";
  ASSERT_FALSE(tag_->hasReadAtomicRoom())
      << "precondition: the read holds the only slot the client negotiated";

  {
    auto retirementGuard = tag_->lockForWqeRetirement();
    tag_->flushAllPendingWqesInError();
  }

  EXPECT_TRUE(tag_->hasReadAtomicRoom())
      << "the flush clears the map and resets the count, so the depth is free "
         "again rather than held by a read the flush discarded";
}

// Teardown joins this coroutine before the flush; the mark posts the batons.
TEST_F(TagTest, MarkingForDeletionReleasesAPostWaitingForRoom) {
  tag_->setMaxRdAtomic(1);

  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  ASSERT_TRUE(postToTag(*tag_, std::move(readWqe), kMtu))
      << "precondition: the tag accepts the read";
  ASSERT_FALSE(tag_->hasReadAtomicRoom())
      << "precondition: the read holds the only slot the client negotiated, so "
         "a further read waits for room";

  folly::ManualExecutor executor;
  auto waiting =
      folly::coro::co_withExecutor(&executor, tag_->co_waitForReadAtomicRoom())
          .start();
  executor.drain();
  ASSERT_FALSE(waiting.isReady())
      << "precondition: the wait has not returned, or the mark below is not "
         "what releases it";

  tag_->markForDeletion();

  executor.drain();
  const bool wokeOnTheMark = waiting.isReady();
  EXPECT_TRUE(wokeOnTheMark)
      << "the mark must wake a post already waiting, since teardown joins "
         "this coroutine before the flush that would post the baton";
  if (!wokeOnTheMark) {
    // Releases the coroutine so the get() below returns and this reports.
    tag_->signalReadAtomicRoom();
    executor.drain();
  }
  std::move(waiting).get();
}

// The read/atomic wait needs no sequencing, so an errored post is exempt too.
TEST_F(TagTest, AnErroredTagNeedsNoReadAtomicRoom) {
  tag_->setMaxRdAtomic(1);

  auto readWqe = std::make_unique<ReadWriteWqe>(
      Wqe::WorkType::kRead,
      kWorkReqId,
      oneSge(buffer_->data(), kMtu),
      kMtu,
      0x1000,
      ibvMr_->rkey);
  ASSERT_TRUE(postToTag(*tag_, std::move(readWqe), kMtu))
      << "precondition: the tag accepts the read";
  ASSERT_FALSE(tag_->hasReadAtomicRoom())
      << "precondition: the read holds the only slot the client negotiated";

  ibvQp_->state = IBV_QPS_ERR;

  folly::ManualExecutor executor;
  auto wait =
      folly::coro::co_withExecutor(&executor, tag_->co_waitForReadAtomicRoom())
          .start();
  executor.drain();
  EXPECT_TRUE(wait.isReady())
      << "an errored tag's post is flushed, so it spends no responder slot "
         "and must not be held waiting for one";
  if (!wait.isReady()) {
    // Grants real room so the get() below returns and this reports.
    tag_->setMaxRdAtomic(2);
    tag_->signalReadAtomicRoom();
    executor.drain();
  }
  std::move(wait).get();
}

} // namespace libsoftmetaroce
