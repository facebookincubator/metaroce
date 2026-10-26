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
#include <barrier>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/Task.h>
#include <folly/coro/Timeout.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/ManualExecutor.h>
#include <tuple>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <infiniband/verbs.h>

#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Cqe.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/QpState.h>
#include <libsoftmetaroce/TagSet.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <libsoftmetaroce/test/PostToTag.h>

using namespace ::testing;

namespace libsoftmetaroce {

class MockQpArbiter {
 public:
  MOCK_METHOD(folly::coro::Task<void>, co_signalReady, (uint32_t qpNum));

  // Simple wrapper to make the method accessible
  folly::coro::Task<void> signalReady(uint32_t qpNum) {
    return co_signalReady(qpNum);
  }
};

class TagSetTest : public ::testing::Test {
 protected:
  void SetUp() override {
    tagSet_ = std::make_unique<TagSet>(
        kQpNum, [this](uint8_t tagNum, uint16_t postedCsn) {
          rnrCancels_.emplace_back(tagNum, postedCsn);
        });
    memoryRegions_ = std::make_unique<MemoryRegions>();

    // Set up memory regions for testing
    buffer_ = std::make_unique<std::array<char, kBufferSize>>();
    ibvMr_ = memoryRegions_->reg_mr(
        &pd_,
        buffer_->data(),
        kBufferSize,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_READ);

    // Create completion queues for testing
    for (int i = 0; i < kMaxTags; ++i) {
      cqs_.push_back(std::make_unique<CompletionQueue>());
    }
  }

  void TearDown() override {
    if (admissionLoop_.has_value()) {
      // The loop holds tagSet_, so it has to finish before the resets below.
      stopAdmissionLoop();
    }
    tagSet_.reset();
    memoryRegions_.reset();
    cqs_.clear();
    buffer_.reset();
  }

  std::unique_ptr<Wqe> makeReadWqe(uint64_t workReqId) {
    return std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kRead,
        workReqId,
        oneSge(buffer_->data(), kMtu),
        kMtu,
        /*remoteAddr=*/0x1000,
        ibvMr_->rkey);
  }

  // Puts the work's one frame on the wire and answers it, so the work reports.
  // Posn zero is that frame for a send, and a read's first response.
  void answerTheOnlyFrame(Tag& tag, Wqe& wqe) {
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

  // Retires a read that has already reported, by answering the request frame
  // it was held for; that is what returns the responder slot it spent.
  void settleRequestFrame(Tag& tag, Wqe& read) {
    read.onFrameNewlyAcked(Wqe::kRequestFrameWirePosn);
    auto tagGuard = tag.scopedLock();
    auto retirementGuard = tag.lockForWqeRetirement();
    tag.retireAfterRequestFrameSettled(read);
  }

  // Adds tag 1, spends its one negotiated slot with a read the loop admits,
  // and returns that read, or nullptr where either precondition failed. The
  // loop is left running for the case's own posts.
  Wqe* admitAReadThatSpendsTheOnlySlot() {
    tagSet_->add(1, cqs_[0].get(), ibvQp_);
    auto tag = tagSet_->at(1);
    tag->setMaxRdAtomic(1);
    auto readWqe = makeReadWqe(kWorkReqId);
    Wqe* read = readWqe.get();
    if (!tagSet_->postWqe(1, std::move(readWqe))) {
      ADD_FAILURE()
          << "precondition: the queue accepts the read that spends the only "
             "slot";
      return nullptr;
    }
    startAdmissionLoop();
    admissionExecutor_.drain();
    if (tag->hasReadAtomicRoom()) {
      ADD_FAILURE() << "precondition: the read spends the only slot the client "
                       "negotiated, so a second read waits for one";
      return nullptr;
    }
    return read;
  }

  // Errors the tag under the Tag hold the call requires. The wake it returns
  // asks for a sender thread no case here runs.
  void enterTagError(Tag& tag) {
    auto tagGuard = tag.scopedLock();
    std::ignore = tagSet_->enterTagErrorWithTagHeld(tag);
  }

  // Adds `count` tags in INIT, each with its own ibv_qp.
  std::vector<std::shared_ptr<Tag>> addInitTags(int count);

  // Runs the dequeue loop a post crosses, so a case can drive admission itself.
  void startAdmissionLoop() {
    admissionReadySignal_ = admissionArbiter_.addQp(
        /*qpNum=*/0, []() { return std::unique_ptr<EgressPacket>(); });
    admissionLoop_ =
        folly::coro::co_withExecutor(
            &admissionExecutor_,
            folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
              try {
                co_await folly::coro::co_withCancellation(
                    admissionCancellation_.getToken(),
                    tagSet_->processWqes(*admissionReadySignal_, kMtu));
              } catch (const folly::OperationCancelled&) {
              }
            }))
            .start();
  }

  // A suspended coroutine holds a keep-alive on admissionExecutor_, so a loop
  // left waiting deadlocks its destructor.
  void stopAdmissionLoop() {
    admissionCancellation_.requestCancellation();
    admissionExecutor_.drain();
    if (!admissionLoop_->isReady()) {
      for (const auto tagNum : tagSet_->getTagNums()) {
        tagSet_->at(tagNum)->markForDeletion();
      }
      admissionExecutor_.drain();
    }
    EXPECT_TRUE(admissionLoop_->isReady())
        << "the loop must exit, or the executor below it never destructs";
    if (admissionLoop_->isReady()) {
      std::move(*admissionLoop_).get();
    }
  }

  static constexpr uint32_t kQpNum = 42;
  static constexpr uint8_t kMaxTags = 5;
  static constexpr uint32_t kBufferSize = 8192;
  static constexpr uint16_t kMtu = 4096;
  static constexpr uint64_t kWorkReqId = 123;
  static constexpr std::chrono::seconds kTestTimeout{10};

  QpArbiter admissionArbiter_;
  QpArbiterIface::ReadySignal* admissionReadySignal_{nullptr};
  folly::ManualExecutor admissionExecutor_;
  folly::CancellationSource admissionCancellation_;
  std::optional<folly::SemiFuture<folly::Unit>> admissionLoop_;

  // The (tagNum, postedCsn) of every RNR-Cancel the set reported.
  std::vector<std::pair<uint8_t, uint16_t>> rnrCancels_;
  // Storage for addInitTags(), declared before tagSet_ so it is destroyed
  // after it: each Tag holds a reference to one entry.
  std::vector<struct ibv_qp> initTagStorage_;
  std::unique_ptr<TagSet> tagSet_;
  std::unique_ptr<MemoryRegions> memoryRegions_;
  std::vector<std::unique_ptr<CompletionQueue>> cqs_;
  std::unique_ptr<std::array<char, kBufferSize>> buffer_;
  struct ibv_pd pd_{nullptr, 1};
  struct ibv_mr* ibvMr_{nullptr};
  struct ibv_qp ibvQpStorage_{};
  struct ibv_qp* ibvQp_{&ibvQpStorage_};
};

struct ibv_qp makeMockQp(ibv_qp_state state, struct ibv_pd* pd = nullptr) {
  struct ibv_qp qp{};
  qp.state = state;
  qp.pd = pd;
  return qp;
}

std::vector<std::shared_ptr<Tag>> TagSetTest::addInitTags(int count) {
  initTagStorage_.resize(count);
  std::vector<std::shared_ptr<Tag>> tags;
  tags.reserve(count);
  for (int i = 0; i < count; ++i) {
    initTagStorage_[i] = makeMockQp(IBV_QPS_INIT);
    tagSet_->add(i + 1, cqs_[i].get(), &initTagStorage_[i]);
    tags.push_back(tagSet_->at(i + 1));
  }
  return tags;
}

TEST_F(TagSetTest, Construction) {
  EXPECT_EQ(tagSet_->getQpNum(), kQpNum);
  EXPECT_EQ(tagSet_->size(), 0);
  EXPECT_EQ(tagSet_->getNumPendingWqes(), 0);
  EXPECT_EQ(tagSet_->getNumPendingRWqes(), 0);
  EXPECT_FALSE(tagSet_->findNextWqe(false).has_value());
}

TEST_F(TagSetTest, AddTag) {
  EXPECT_EQ(tagSet_->size(), 0);

  // Add first tag
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  EXPECT_EQ(tagSet_->size(), 1);

  // Verify tag was added
  auto tag = tagSet_->at(1);
  ASSERT_NE(tag, nullptr);
  EXPECT_EQ(tag->getTagNum(), 1);

  // Add second tag
  tagSet_->add(2, cqs_[1].get(), ibvQp_);
  EXPECT_EQ(tagSet_->size(), 2);

  // Verify both tags exist
  auto tag1 = tagSet_->at(1);
  auto tag2 = tagSet_->at(2);
  ASSERT_NE(tag1, nullptr);
  ASSERT_NE(tag2, nullptr);
  EXPECT_EQ(tag1->getTagNum(), 1);
  EXPECT_EQ(tag2->getTagNum(), 2);
}

TEST_F(TagSetTest, AddMultipleTags) {
  for (uint8_t i = 0; i < kMaxTags; ++i) {
    tagSet_->add(i, cqs_[i].get(), ibvQp_);
  }

  EXPECT_EQ(tagSet_->size(), kMaxTags);

  // Verify all tags exist and have correct tag numbers
  for (uint8_t i = 0; i < kMaxTags; ++i) {
    auto tag = tagSet_->at(i);
    ASSERT_NE(tag, nullptr);
    EXPECT_EQ(tag->getTagNum(), i);
  }
}

TEST_F(TagSetTest, GetNonExistentTag) {
  EXPECT_THROW(std::ignore = tagSet_->at(99), std::out_of_range);
}

// Test getTags method
TEST_F(TagSetTest, GetTagNumss) {
  // Initially empty
  EXPECT_TRUE(tagSet_->getTagNums().empty());

  // Add some tags
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  tagSet_->add(3, cqs_[1].get(), ibvQp_);
  tagSet_->add(5, cqs_[2].get(), ibvQp_);

  EXPECT_THAT(tagSet_->getTagNums(), ::testing::ElementsAre(1, 3, 5))
      << "the tags added, in ascending order, and no others";
}

TEST_F(TagSetTest, PostWqe) {
  // Add a tag first
  tagSet_->add(1, cqs_[0].get(), ibvQp_);

  // Create and post a WQE
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);

  tagSet_->postWqe(1, std::move(sendWqe));

  // The WQE should be in the queue, but we can't directly verify this
  // without processWqes or dequeue operations
  SUCCEED();
}

TEST_F(TagSetTest, PostMultipleWqes) {
  // Add multiple tags
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  tagSet_->add(2, cqs_[1].get(), ibvQp_);

  // Post WQEs to different tags
  auto sendWqe1 = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  auto sendWqe2 = std::make_unique<SendWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 2000), kMtu);
  auto sendWqe3 = std::make_unique<SendWqe>(
      kWorkReqId + 2, oneSge(buffer_->data(), 1500), kMtu);

  tagSet_->postWqe(1, std::move(sendWqe1));
  tagSet_->postWqe(2, std::move(sendWqe2));
  tagSet_->postWqe(1, std::move(sendWqe3));

  SUCCEED();
}

TEST_F(TagSetTest, PostWqeNonExistentTag) {
  // Create WQE for tag that doesn't exist
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);

  // This should throw an exception since the tag doesn't exist
  EXPECT_THROW(tagSet_->postWqe(99, std::move(sendWqe)), std::out_of_range);
}

TEST_F(TagSetTest, GetNumPendingWqes) {
  // Initially no pending WQEs
  EXPECT_EQ(tagSet_->getNumPendingWqes(), 0);

  // Add tags
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  tagSet_->add(2, cqs_[1].get(), ibvQp_);
  EXPECT_EQ(tagSet_->getNumPendingWqes(), 0);

  // Process WQEs to make them pending in tags
  auto sendWqe1 = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  auto sendWqe2 = std::make_unique<SendWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 2000), kMtu);
  auto sendWqe3 = std::make_unique<SendWqe>(
      kWorkReqId + 2, oneSge(buffer_->data(), 1500), kMtu);

  // Process WQEs directly in tags
  auto tag1 = tagSet_->at(1);
  auto tag2 = tagSet_->at(2);

  postToTag(*tag1, std::move(sendWqe1), kMtu);
  EXPECT_EQ(tagSet_->getNumPendingWqes(), 1);

  postToTag(*tag2, std::move(sendWqe2), kMtu);
  EXPECT_EQ(tagSet_->getNumPendingWqes(), 2);

  postToTag(*tag1, std::move(sendWqe3), kMtu);
  EXPECT_EQ(tagSet_->getNumPendingWqes(), 3);
}

// Test getNumPendingRWqes aggregation across tags
TEST_F(TagSetTest, GetNumPendingRWqes) {
  // Initially no pending RWQEs
  EXPECT_EQ(tagSet_->getNumPendingRWqes(), 0);

  // Add tags
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  tagSet_->add(2, cqs_[1].get(), ibvQp_);
  EXPECT_EQ(tagSet_->getNumPendingRWqes(), 0);

  // Process receive WQEs to make them pending in tags
  auto receiveWqe1 = std::make_unique<ReceiveWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  auto receiveWqe2 = std::make_unique<ReceiveWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 2000), kMtu);

  // Process WQEs directly in tags
  auto tag1 = tagSet_->at(1);
  auto tag2 = tagSet_->at(2);

  postToTag(*tag1, std::move(receiveWqe1), kMtu);
  EXPECT_EQ(tagSet_->getNumPendingRWqes(), 1);

  postToTag(*tag2, std::move(receiveWqe2), kMtu);
  EXPECT_EQ(tagSet_->getNumPendingRWqes(), 2);
}

TEST_F(TagSetTest, FindNextWqeRoundRobin) {
  // Add tags
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  tagSet_->add(2, cqs_[1].get(), ibvQp_);
  tagSet_->add(3, cqs_[2].get(), ibvQp_);

  // Initially no WQE to find
  EXPECT_FALSE(tagSet_->findNextWqe(false).has_value());

  // Add WQEs to tags
  auto tag1 = tagSet_->at(1);
  auto tag2 = tagSet_->at(2);
  auto tag3 = tagSet_->at(3);

  auto sendWqe1 = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  auto sendWqe2 = std::make_unique<SendWqe>(
      kWorkReqId + 1, oneSge(buffer_->data(), 2000), kMtu);
  auto sendWqe3 = std::make_unique<SendWqe>(
      kWorkReqId + 2, oneSge(buffer_->data(), 1500), kMtu);

  // Process WQEs in tags
  postToTag(*tag1, std::move(sendWqe1), kMtu);
  postToTag(*tag2, std::move(sendWqe2), kMtu);
  postToTag(*tag3, std::move(sendWqe3), kMtu);

  // Find WQEs in round-robin order
  // The order should follow the tag number ordering (1, 2, 3, 1, 2, 3, ...)
  auto maybeWqeAndTag1 = tagSet_->findNextWqe(true);
  EXPECT_TRUE(maybeWqeAndTag1.has_value());
  EXPECT_NE(maybeWqeAndTag1.value().first, nullptr);
  EXPECT_EQ(maybeWqeAndTag1.value().second->getTagNum(), 1);
  maybeWqeAndTag1.value().second->markFree();

  auto maybeWqeAndTag2 = tagSet_->findNextWqe(true);
  EXPECT_TRUE(maybeWqeAndTag2.has_value());
  EXPECT_NE(maybeWqeAndTag2.value().first, nullptr);
  EXPECT_EQ(maybeWqeAndTag2.value().second->getTagNum(), 2);
  maybeWqeAndTag2.value().second->markFree();

  auto maybeWqeAndTag3 = tagSet_->findNextWqe(true);
  EXPECT_TRUE(maybeWqeAndTag3.has_value());
  EXPECT_NE(maybeWqeAndTag3.value().first, nullptr);
  EXPECT_EQ(maybeWqeAndTag3.value().second->getTagNum(), 3);
  maybeWqeAndTag3.value().second->markFree();

  // Should cycle back to tag 1
  auto maybeWqeAndTag4 = tagSet_->findNextWqe(true);
  EXPECT_TRUE(maybeWqeAndTag4.has_value());
  EXPECT_NE(maybeWqeAndTag4.value().first, nullptr);
  EXPECT_EQ(maybeWqeAndTag4.value().second->getTagNum(), 1);
  maybeWqeAndTag4.value().second->markFree();
}

TEST_F(TagSetTest, FindNextWqeSparseTagsStayInNumericOrder) {
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  tagSet_->add(5, cqs_[1].get(), ibvQp_);
  tagSet_->add(200, cqs_[2].get(), ibvQp_);

  ASSERT_TRUE(postToTag(
      *tagSet_->at(1),
      std::make_unique<SendWqe>(
          kWorkReqId, oneSge(buffer_->data(), 1000), kMtu),
      kMtu))
      << "precondition: tag 1 holds work, or the order below is untested";
  ASSERT_TRUE(postToTag(
      *tagSet_->at(5),
      std::make_unique<SendWqe>(
          kWorkReqId + 1, oneSge(buffer_->data(), 2000), kMtu),
      kMtu))
      << "precondition: tag 5 holds work, or the order below is untested";
  ASSERT_TRUE(postToTag(
      *tagSet_->at(200),
      std::make_unique<SendWqe>(
          kWorkReqId + 2, oneSge(buffer_->data(), 1500), kMtu),
      kMtu))
      << "precondition: tag 200 holds work, or the order below is untested";

  std::vector<uint8_t> selected;
  for (int i = 0; i < 4; ++i) {
    auto found = tagSet_->findNextWqe(true);
    ASSERT_TRUE(found.has_value())
        << "precondition: every tag still holds work on every visit";
    selected.push_back(found->second->getTagNum());
    found->second->markFree();
  }
  const std::vector<uint8_t> expected{1, 5, 200, 1};
  EXPECT_EQ(selected, expected)
      << "gaps in the tag range must not reorder or strand service; "
         "selection order is numeric with wraparound";
}

TEST_F(TagSetTest, FindNextWqeSkipsTagsWithoutWork) {
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  tagSet_->add(2, cqs_[1].get(), ibvQp_);
  tagSet_->add(3, cqs_[2].get(), ibvQp_);

  ASSERT_TRUE(postToTag(
      *tagSet_->at(1),
      std::make_unique<SendWqe>(
          kWorkReqId, oneSge(buffer_->data(), 1000), kMtu),
      kMtu))
      << "precondition: tag 1 holds work, or the skip below is untested";
  ASSERT_TRUE(postToTag(
      *tagSet_->at(3),
      std::make_unique<SendWqe>(
          kWorkReqId + 2, oneSge(buffer_->data(), 1500), kMtu),
      kMtu))
      << "precondition: tag 3 holds work, or the skip below is untested";

  std::vector<uint8_t> selected;
  for (int i = 0; i < 4; ++i) {
    auto found = tagSet_->findNextWqe(true);
    ASSERT_TRUE(found.has_value())
        << "precondition: an idle tag must not end the scan while work "
           "remains elsewhere";
    selected.push_back(found->second->getTagNum());
    found->second->markFree();
  }
  const std::vector<uint8_t> expected{1, 3, 1, 3};
  EXPECT_EQ(selected, expected)
      << "a tag with no work must yield its turn without consuming it, or "
         "one idle tag would deny the others their turns";
}

TEST_F(TagSetTest, FindNextWqeProbesLeaveSelectionOrderUnchanged) {
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  tagSet_->add(2, cqs_[1].get(), ibvQp_);

  ASSERT_TRUE(postToTag(
      *tagSet_->at(1),
      std::make_unique<SendWqe>(
          kWorkReqId, oneSge(buffer_->data(), 1000), kMtu),
      kMtu))
      << "precondition: tag 1 holds work, or the order below is untested";
  ASSERT_TRUE(postToTag(
      *tagSet_->at(2),
      std::make_unique<SendWqe>(
          kWorkReqId + 1, oneSge(buffer_->data(), 2000), kMtu),
      kMtu))
      << "precondition: tag 2 holds work, or the order below is untested";

  std::vector<uint8_t> selected;
  std::vector<uint8_t> probed;
  for (int i = 0; i < 3; ++i) {
    auto send = tagSet_->findNextWqe(true);
    ASSERT_TRUE(send.has_value())
        << "precondition: both tags hold work for the whole sequence";
    selected.push_back(send->second->getTagNum());
    send->second->markFree();
    if (i == 2) {
      break;
    }
    auto probe = tagSet_->findNextWqe(false);
    ASSERT_TRUE(probe.has_value())
        << "precondition: a probe must still observe the pending work";
    probed.push_back(probe->second->getTagNum());
    probe->second->markFree();
  }
  const std::vector<uint8_t> expectedSelected{1, 2, 1};
  EXPECT_EQ(selected, expectedSelected)
      << "readiness probes between sends must leave the selection order "
         "unchanged, or the retransmission path's probes would reorder "
         "traffic";
  const std::vector<uint8_t> expectedProbed{2, 1};
  EXPECT_EQ(probed, expectedProbed)
      << "a probe must not consume the turn it observes";
}

TEST_F(TagSetTest, FindNextWqeDoesNotDeadlockWithConcurrentTagHolder) {
  constexpr uint8_t kTags = 2;
  for (uint8_t tagNum = 1; tagNum <= kTags; ++tagNum) {
    tagSet_->add(tagNum, cqs_[tagNum - 1].get(), ibvQp_);
  }

  constexpr int kIterations = 100'000;
  std::barrier start{2};
  std::thread holder([&] {
    start.arrive_and_wait();
    for (int i = 0; i < kIterations; ++i) {
      auto tag = tagSet_->at(static_cast<uint8_t>((i % kTags) + 1));
      auto guard = tag->scopedLock();
      std::this_thread::yield();
      (void)tagSet_->size();
    }
  });
  std::thread finder([&] {
    start.arrive_and_wait();
    for (int i = 0; i < kIterations; ++i) {
      if (auto wqeAndTag = tagSet_->findNextWqe(false)) {
        wqeAndTag->second->markFree();
      }
    }
  });

  holder.join();
  finder.join();
}

// Test processWqes with real arbiter (testing the coroutine behavior)
TEST_F(TagSetTest, ProcessWqesBasic) {
  // Add a tag
  tagSet_->add(1, cqs_[0].get(), ibvQp_);

  // Create a real QpArbiter for this test and register a QP to get a readiness
  // handle for processWqes to signal through.
  QpArbiter arbiter;
  auto* readySignal = arbiter.addQp(
      /*qpNum=*/0, []() { return std::unique_ptr<EgressPacket>(); });

  // Post a WQE
  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), 1000), kMtu);
  tagSet_->postWqe(1, std::move(sendWqe));

  // Start processWqes in a coroutine (it runs indefinitely, so we need to test
  // carefully) We'll use a timeout to prevent infinite waiting
  auto processTask = [&]() -> folly::coro::Task<void> {
    // This will process one WQE and signal the arbiter
    co_await folly::coro::timeout(
        tagSet_->processWqes(*readySignal, kMtu),
        std::chrono::milliseconds(100));
  };

  // The processWqes should process the WQE and call arbiter.co_signalReady
  // We can't easily verify the exact behavior without a more complex setup,
  // but we can verify it doesn't crash
  try {
    folly::coro::blockingWait(processTask());
  } catch (const folly::FutureTimeout&) {
    // This is expected since processWqes runs indefinitely
    SUCCEED();
  }

  // Verify the WQE was processed
  EXPECT_EQ(tagSet_->getNumPendingWqes(), 1);
}

/*
 * A work request posted to a tag that is already in error is flushed where it
 * is processed, not queued: its posns must never be counted as unsent, because
 * no visit will ever send them. The tag still needs the sender's sweep, and the
 * post is where that need gets published -- the tag itself entered error
 * somewhere it could not reach the owner.
 */
TEST_F(TagSetTest, PostingToATagInErrorFlushesWithoutCountingItsPosns) {
  struct ibv_qp mockQp = makeMockQp(IBV_QPS_INIT);
  tagSet_->add(1, cqs_[0].get(), &mockQp);
  auto tag = tagSet_->at(1);
  enterTagError(*tag);
  ASSERT_EQ(tagSet_->getUnsentPosnCount(), 0u)
      << "nothing has been posted yet, so the count starts clean";

  QpArbiter arbiter;
  auto* readySignal = arbiter.addQp(
      /*qpNum=*/0, []() { return std::unique_ptr<EgressPacket>(); });
  tagSet_->postWqe(
      1, std::make_unique<SendWqe>(kWorkReqId, oneSge(*buffer_), kMtu));

  // The completion is what says the post was processed, so the test waits for
  // it and then cancels the loop.
  folly::CancellationSource cancellationSource;
  auto processTask = [&]() -> folly::coro::Task<void> {
    try {
      co_await folly::coro::co_withCancellation(
          cancellationSource.getToken(),
          tagSet_->processWqes(*readySignal, kMtu));
    } catch (const folly::OperationCancelled&) {
    }
  };
  auto awaitCompletionTask = [&]() -> folly::coro::Task<Cqe> {
    // Cancel however this exits: processWqes answers only to this source, and
    // collectAll waits for it before reporting a timeout raised here.
    auto cancelOnExit =
        folly::makeGuard([&] { cancellationSource.requestCancellation(); });
    co_return co_await folly::coro::timeout(cqs_[0]->pollCq(), kTestTimeout);
  };
  folly::CPUThreadPoolExecutor executor(2);
  auto [_, cqe] = folly::coro::blockingWait(
      folly::coro::collectAll(
          folly::coro::co_withExecutor(&executor, processTask()),
          folly::coro::co_withExecutor(&executor, awaitCompletionTask())));

  EXPECT_EQ(cqe.status, kWrFlushErr)
      << "a work request posted to a tag in error must report straight away, "
         "since nothing later will ever send or complete it";
  EXPECT_EQ(cqs_[0]->getNumCompletedWqes(), 0)
      << "and the completion polled above is the only one it produces";
  EXPECT_EQ(tagSet_->getUnsentPosnCount(), 0u)
      << "counting its posns as unsent would leave the rate hint claiming work "
         "the sender can never drain";
  EXPECT_EQ(tagSet_->getNumPendingWqes(), 0)
      << "and it must not be left queued behind the tag's failure";
}

TEST_F(TagSetTest, BoundaryConditions) {
  // Test with maximum uint8_t tag number
  tagSet_->add(255, cqs_[0].get(), ibvQp_);
  EXPECT_EQ(tagSet_->size(), 1);

  auto tag = tagSet_->at(255);
  ASSERT_NE(tag, nullptr);
  EXPECT_EQ(tag->getTagNum(), 255);

  // Test with tag number 0
  tagSet_->add(0, cqs_[1].get(), ibvQp_);
  EXPECT_EQ(tagSet_->size(), 2);

  auto tag0 = tagSet_->at(0);
  ASSERT_NE(tag0, nullptr);
  EXPECT_EQ(tag0->getTagNum(), 0);
}

TEST_F(TagSetTest, ErrorConditions) {
  // Test operations on empty TagSet
  EXPECT_EQ(tagSet_->size(), 0);
  EXPECT_EQ(tagSet_->getNumPendingWqes(), 0);
  EXPECT_EQ(tagSet_->getNumPendingRWqes(), 0);
  EXPECT_FALSE(tagSet_->findNextWqe(false).has_value());
  EXPECT_FALSE(tagSet_->findNextWqe(true).has_value());
  EXPECT_TRUE(tagSet_->getTagNums().empty());

  EXPECT_THROW(std::ignore = tagSet_->at(1), std::out_of_range);
  EXPECT_THROW(std::ignore = tagSet_->at(0), std::out_of_range);
  EXPECT_THROW(std::ignore = tagSet_->at(255), std::out_of_range);
}

TEST_F(TagSetTest, AllTagsInStateWithMultipleTags) {
  struct ibv_qp mockQp1 = makeMockQp(IBV_QPS_INIT);
  struct ibv_qp mockQp2 = makeMockQp(IBV_QPS_INIT);
  struct ibv_qp mockQp3 = makeMockQp(IBV_QPS_INIT);

  // Add multiple tags and set up their ibv_qps with the same state
  tagSet_->add(1, cqs_[0].get(), &mockQp1);
  tagSet_->add(2, cqs_[1].get(), &mockQp2);
  tagSet_->add(3, cqs_[2].get(), &mockQp3);

  EXPECT_TRUE(tagSet_->allTagsInState(IBV_QPS_INIT))
      << "every Tag was added in INIT, so the query must agree they are all "
         "in it";
}

TEST_F(TagSetTest, AllTagsInStateWithMismatchedStates) {
  struct ibv_qp mockQp1 = makeMockQp(IBV_QPS_INIT);
  struct ibv_qp mockQp2 = makeMockQp(IBV_QPS_RESET);

  // Add multiple tags with different states
  tagSet_->add(1, cqs_[0].get(), &mockQp1);
  tagSet_->add(2, cqs_[1].get(), &mockQp2);

  EXPECT_FALSE(tagSet_->allTagsInState(IBV_QPS_INIT))
      << "tags fail one at a time, so disagreeing is ordinary; a Tag left in "
         "RESET must make the answer no rather than throw at a caller that "
         "only wanted to look";
  EXPECT_FALSE(tagSet_->allTagsInState(IBV_QPS_RESET))
      << "and the same holds whichever of the two states is asked about";
}

/*
 * A Tag may only join a QP that has not left RESET: the transition a client
 * drives is validated against the one Tag it addresses and then written to
 * all of them, so a RESET Tag on a running QP would let a later transition
 * on it carry the others backwards.
 */
TEST_F(TagSetTest, ATagIsRefusedOnceTheQpHasLeftReset) {
  struct ibv_qp mockQp1 = makeMockQp(IBV_QPS_RESET);
  struct ibv_qp mockQp2 = makeMockQp(IBV_QPS_RESET);
  tagSet_->add(1, cqs_[0].get(), &mockQp1);
  tagSet_->setIbvQpState(IBV_QPS_INIT);

  EXPECT_EQ(
      tagSet_->addIfAllTagsInReset(2, cqs_[1].get(), nullptr, &mockQp2),
      nullptr)
      << "the QP has left RESET, so it may not take another Tag";
  EXPECT_EQ(tagSet_->size(), 1u)
      << "and the refusal must not leave the Tag half-added";
}

/*
 * A tag that a non-owner thread errored has its work abandoned by the sender
 * at its next visit, since abandoning writes posn state only the sender may
 * write. Its unsent posns are given back: none of them will be sent.
 */
TEST_F(TagSetTest, TheSendersVisitFailsTheWqesOfATagInError) {
  struct ibv_qp mockQp = makeMockQp(IBV_QPS_INIT);
  tagSet_->add(1, cqs_[0].get(), &mockQp);
  auto tag = tagSet_->at(1);

  auto sendWqe = std::make_unique<SendWqe>(kWorkReqId, oneSge(*buffer_), kMtu);
  Wqe* postedWqe = sendWqe.get();
  postToTag(*tag, std::move(sendWqe), kMtu);
  tagSet_->addUnsentPosns(postedWqe->getNumPosn());
  // The first of its two packets is on the wire, so the work request cannot
  // drain where it is abandoned, and outlives the visit for the checks below.
  std::ignore = postedWqe->allocateNextPosn();
  tagSet_->decrementUnsentPosns();
  enterTagError(*tag);

  // What the sender's visit does before it looks for a packet to send.
  tagSet_->failWqesOfTagsInError();

  EXPECT_TRUE(postedWqe->isFailing())
      << "the sender must mark failing the work of a tag it finds in error";
  EXPECT_EQ(cqs_[0]->getNumCompletedWqes(), 0)
      << "the work request still has a packet on the wire, so it must report "
         "once that packet settles rather than here";
  EXPECT_EQ(tagSet_->getUnsentPosnCount(), 0u)
      << "the posn that will now never be sent must stop counting as unsent "
         "work";

  // The sweep has settled, so a later visit finds nothing to do; it must not
  // report the work request twice or take its posn off again.
  tagSet_->failWqesOfTagsInError();
  EXPECT_EQ(cqs_[0]->getNumCompletedWqes(), 0)
      << "a repeat visit must not complete a work request still on the wire";
  EXPECT_EQ(tagSet_->getUnsentPosnCount(), 0u)
      << "nor subtract the same posn twice, which would underflow the count";
}

/*
 * A client erroring the whole QP must fail its work like a Tag that entered
 * error on its own. The transition writes each Tag's state under mutex_, and
 * the sweep reads the published flag rather than those writes, so a whole-QP
 * transition that does not publish leaves work no visit ever fails.
 */
TEST_F(TagSetTest, ErroringTheWholeQpLeavesItsWorkForTheSendersVisit) {
  struct ibv_qp mockQp = makeMockQp(IBV_QPS_INIT);
  tagSet_->add(1, cqs_[0].get(), &mockQp);
  auto tag = tagSet_->at(1);

  auto sendWqe = std::make_unique<SendWqe>(
      kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu);
  postToTag(*tag, std::move(sendWqe), kMtu);
  ASSERT_EQ(tag->getNumPendingWqes(), 1)
      << "precondition: the work request is pending before the transition, or "
         "the sweep below has nothing to find";

  tagSet_->setIbvQpState(IBV_QPS_ERR);

  // What the sender's visit does before it looks for a packet to send.
  tagSet_->failWqesOfTagsInError();

  EXPECT_EQ(cqs_[0]->getNumCompletedWqes(), 1)
      << "the transition errored the tag and nothing of its work reached the "
         "wire, so the sender's visit must find it and report it; leaving it "
         "pending strands the client's completion for good";
  EXPECT_EQ(tag->getNumPendingWqes(), 0)
      << "and the work request must be retired rather than held to teardown";
}

/*
 * A Tag destroyed after it failed still has its work abandoned. Whether the
 * sender happens to visit between the failure and the destroy is a scheduling
 * accident, and the client sees the same two calls either way, so the error
 * has to decide the outcome rather than the order of the two.
 */
TEST_F(TagSetTest, DestroyingATagDoesNotCancelAnErrorThatCameFirst) {
  struct ibv_qp mockQp = makeMockQp(IBV_QPS_INIT);
  tagSet_->add(1, cqs_[0].get(), &mockQp);
  auto tag = tagSet_->getTagByNumber(1);
  ASSERT_NE(tag, nullptr);

  auto sendWqe = std::make_unique<SendWqe>(
      /*workReqId=*/7, oneSge(nullptr, 0), /*mtu=*/4096);
  postToTag(*tag, std::move(sendWqe), /*mtu=*/4096);
  enterTagError(*tag);
  // Destroyed before the sender ever visits: the accident this case is about.
  tag->markForDeletion();

  // What the sender's visit does before it looks for a packet to send.
  tagSet_->failWqesOfTagsInError();

  EXPECT_EQ(cqs_[0]->getNumCompletedWqes(), 1)
      << "the work request has no packet on the wire, so abandoning it reports "
         "it; skipping the Tag because it is being destroyed would strand a "
         "work request whose error the client never receives";
}

/*
 * One tag entering error leaves the others as they were. The sweep visits them
 * all, so it has to fail only the work of the tags actually in error, and it
 * has to catch a tag that enters error after it has already run once.
 */
TEST_F(TagSetTest, TheSendersVisitLeavesTheWorkOfATagThatHasNotFailed) {
  struct ibv_qp mockQp1 = makeMockQp(IBV_QPS_INIT);
  struct ibv_qp mockQp2 = makeMockQp(IBV_QPS_INIT);
  tagSet_->add(1, cqs_[0].get(), &mockQp1);
  tagSet_->add(2, cqs_[1].get(), &mockQp2);

  // One packet each, none on the wire, so failing a tag's work reports its
  // work request at the visit that finds it.
  for (uint8_t tagNum : {1, 2}) {
    auto sendWqe = std::make_unique<SendWqe>(
        kWorkReqId, oneSge(buffer_->data(), kMtu), kMtu);
    tagSet_->addUnsentPosns(sendWqe->getNumPosn());
    postToTag(*tagSet_->at(tagNum), std::move(sendWqe), kMtu);
  }

  enterTagError(*tagSet_->at(1));
  tagSet_->failWqesOfTagsInError();

  EXPECT_EQ(cqs_[0]->getNumCompletedWqes(), 1)
      << "the work of the tag that failed must be marked failing";
  EXPECT_EQ(cqs_[1]->getNumCompletedWqes(), 0)
      << "the sweep walks every tag, so a tag still running must come through "
         "it with its work intact";
  EXPECT_EQ(tagSet_->getUnsentPosnCount(), 1u)
      << "only the failed tag's posn stops counting; the other tag still has "
         "one to send";

  // The second tag enters error after a sweep has already run.
  enterTagError(*tagSet_->at(2));
  tagSet_->failWqesOfTagsInError();

  EXPECT_EQ(cqs_[1]->getNumCompletedWqes(), 1)
      << "a tag that fails after an earlier sweep must get one of its own; "
         "settling the flag has to be per failure, not once for the QP";
  EXPECT_EQ(tagSet_->getUnsentPosnCount(), 0u)
      << "and its posn must stop counting as unsent work too";
}

/*
 * A virtual send answers a read request rather than a client's post, so its
 * posns were never counted as unsent work. Abandoning a virtual send has to
 * stop it sending without adding posns to a count they never entered.
 */
TEST_F(TagSetTest, AbandoningAVirtualSendReturnsNoPosnsToTheUnsentCount) {
  // Shares a protection domain with the region registered in SetUp, so the
  // read response below is accepted and there is a virtual send to abandon.
  struct ibv_qp mockQp = makeMockQp(IBV_QPS_INIT, &pd_);
  tagSet_->add(1, cqs_[0].get(), &mockQp);
  auto tag = tagSet_->at(1);

  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(buffer_->data()), ibvMr_->rkey, 2 * kMtu);
  int addressError = -1;
  tag->createVirtualSendWqeIfNeeded(
      /*msn=*/1, rwHeader, &addressError, *memoryRegions_, kMtu);
  ASSERT_EQ(addressError, 0) << "the read response must be accepted for the "
                                "test to have a virtual send to abandon";
  ASSERT_TRUE(tagSet_->findNextWqe(/*intendToSend=*/true).has_value())
      << "the read response must have packets to send before the tag fails, "
         "or the check below would pass for the wrong reason";
  // A found WQE comes back with its Tag still held, for the sender to send
  // under.
  tag->markFree();

  enterTagError(*tag);
  tagSet_->failWqesOfTagsInError();

  EXPECT_EQ(tagSet_->getUnsentPosnCount(), 0u)
      << "the count never held these posns, so returning them would underflow "
         "it and leave the QP looking permanently ready to send";
  EXPECT_FALSE(tagSet_->findNextWqe(/*intendToSend=*/true).has_value())
      << "the read response must stop producing packets once the tag it "
         "answers on has failed";
  EXPECT_EQ(cqs_[0]->getNumCompletedWqes(), 0)
      << "a virtual send answers the remote side, so there is no client work "
         "request to report a completion for";
}

/*
 * Tags fail one at a time, so by the time a client errors the QP one of them
 * may already be in IBV_QPS_ERR. Asking whether they agree first would throw
 * on exactly the transition that most needs to go through.
 */
TEST_F(TagSetTest, ErroringTheQpSucceedsWhenATagIsAlreadyInError) {
  struct ibv_qp mockQp1 = makeMockQp(IBV_QPS_RESET);
  struct ibv_qp mockQp2 = makeMockQp(IBV_QPS_RESET);
  tagSet_->add(1, cqs_[0].get(), &mockQp1);
  tagSet_->add(2, cqs_[1].get(), &mockQp2);
  tagSet_->setIbvQpState(IBV_QPS_RTS);

  // Tag 1 fails on its own, as a fatal NAK for it would leave it.
  mockQp1.state = IBV_QPS_ERR;

  tagSet_->setIbvQpState(IBV_QPS_ERR);

  EXPECT_EQ(tagSet_->at(1)->getIbvQp().state, IBV_QPS_ERR)
      << "the Tag that was already in error stays in error";
  EXPECT_EQ(tagSet_->at(2)->getIbvQp().state, IBV_QPS_ERR)
      << "and the transition must still reach the Tag that had not; leaving "
         "it running is a QP the client asked to error that keeps sending";
}

/*
 * The error state is terminal. A transition aimed at the Tags still running
 * must not carry one that has already failed back out of it: its work is
 * failing and its error already reported.
 */
TEST_F(TagSetTest, ATransitionDoesNotBringATagInErrorBackOutOfIt) {
  struct ibv_qp mockQp1 = makeMockQp(IBV_QPS_RESET);
  struct ibv_qp mockQp2 = makeMockQp(IBV_QPS_RESET);
  tagSet_->add(1, cqs_[0].get(), &mockQp1);
  tagSet_->add(2, cqs_[1].get(), &mockQp2);
  tagSet_->setIbvQpState(IBV_QPS_RTR);

  // Tag 1 fails on its own, as a fatal NAK for it would leave it.
  mockQp1.state = IBV_QPS_ERR;

  tagSet_->setIbvQpState(IBV_QPS_RTS);

  EXPECT_EQ(tagSet_->at(1)->getIbvQp().state, IBV_QPS_ERR)
      << "the Tag in error must stay in error; sending on it again would put "
         "work on the wire for a client that has been told it failed";
  EXPECT_EQ(tagSet_->at(2)->getIbvQp().state, IBV_QPS_RTS)
      << "the Tag that had not failed must still take the transition";
}

TEST_F(TagSetTest, SetIbvQpStateMultipleTags) {
  struct ibv_qp mockQp1 = makeMockQp(IBV_QPS_RESET);
  struct ibv_qp mockQp2 = makeMockQp(IBV_QPS_RESET);
  struct ibv_qp mockQp3 = makeMockQp(IBV_QPS_RESET);

  // Add multiple tags
  tagSet_->add(1, cqs_[0].get(), &mockQp1);
  tagSet_->add(2, cqs_[1].get(), &mockQp2);
  tagSet_->add(3, cqs_[2].get(), &mockQp3);

  // Change the state on all tags
  tagSet_->setIbvQpState(IBV_QPS_RTS);

  // Verify all tags have the new state
  EXPECT_EQ(tagSet_->at(1)->getIbvQp().state, IBV_QPS_RTS);
  EXPECT_EQ(tagSet_->at(2)->getIbvQp().state, IBV_QPS_RTS);
  EXPECT_EQ(tagSet_->at(3)->getIbvQp().state, IBV_QPS_RTS);
  EXPECT_TRUE(tagSet_->allTagsInState(IBV_QPS_RTS))
      << "setting the state must move every Tag, not just the first";
}

TEST_F(TagSetTest, SetIbvQpStateTransitions) {
  struct ibv_qp mockQp1 = makeMockQp(IBV_QPS_RESET);
  struct ibv_qp mockQp2 = makeMockQp(IBV_QPS_RESET);

  // Test multiple state transitions
  tagSet_->add(1, cqs_[0].get(), &mockQp1);
  tagSet_->add(2, cqs_[1].get(), &mockQp2);

  // Transition through various states
  tagSet_->setIbvQpState(IBV_QPS_INIT);
  EXPECT_TRUE(tagSet_->allTagsInState(IBV_QPS_INIT))
      << "the RESET to INIT transition must reach every Tag";

  tagSet_->setIbvQpState(IBV_QPS_RTR);
  EXPECT_TRUE(tagSet_->allTagsInState(IBV_QPS_RTR))
      << "and so must INIT to RTR";

  tagSet_->setIbvQpState(IBV_QPS_RTS);
  EXPECT_TRUE(tagSet_->allTagsInState(IBV_QPS_RTS)) << "and RTR to RTS";

  // Verify both tags have the final state
  EXPECT_EQ(tagSet_->at(1)->getIbvQp().state, IBV_QPS_RTS);
  EXPECT_EQ(tagSet_->at(2)->getIbvQp().state, IBV_QPS_RTS);
}

TEST_F(TagSetTest, AReadPastTheNegotiatedDepthIsHeldAtAdmission) {
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  auto tag = tagSet_->at(1);
  tag->setMaxRdAtomic(1);

  for (int i = 0; i < 2; ++i) {
    ASSERT_TRUE(tagSet_->postWqe(1, makeReadWqe(kWorkReqId + i)))
        << "precondition: the queue accepts the read, or the count below "
           "measures a refusal rather than the depth";
  }

  startAdmissionLoop();
  admissionExecutor_.drain();

  EXPECT_EQ(tagSet_->getNumPendingWqes(), 1)
      << "the first read spends the only slot the client negotiated, so the "
         "second is held here and never reaches the Tag";

  tag->setMaxRdAtomic(2);
  tag->signalReadAtomicRoom();
  admissionExecutor_.drain();

  EXPECT_EQ(tagSet_->getNumPendingWqes(), 2)
      << "raising the depth is what releases it, so the depth is what held it "
         "rather than the msn span or a queue that had not been read yet";
}

// Admission and the wait both exempt a flushed post; either alone admits it.
TEST_F(TagSetTest, AnErroredTagsReadIsAdmittedRatherThanHeld) {
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  auto tag = tagSet_->at(1);
  tag->setMaxRdAtomic(1);

  startAdmissionLoop();

  ASSERT_TRUE(tagSet_->postWqe(1, makeReadWqe(kWorkReqId)))
      << "precondition: the queue accepts the read that spends the slot";
  admissionExecutor_.drain();
  ASSERT_FALSE(tag->hasReadAtomicRoom())
      << "precondition: the first read spends the only slot, so a second would "
         "be held if the tag were not in error";

  enterTagError(*tag);
  ASSERT_EQ(tagSet_->getNumPendingWqes(), 1)
      << "precondition: the first read is the Tag's only work, so the count "
         "below rises by what the two posts add and no more";

  // One coroutine dequeues both in order, so the send lands only if the read
  // ahead of it was let through.
  ASSERT_TRUE(tagSet_->postWqe(1, makeReadWqe(kWorkReqId + 1)))
      << "precondition: the queue accepts the second read";
  ASSERT_TRUE(tagSet_->postWqe(
      1,
      std::make_unique<SendWqe>(
          kWorkReqId + 2, oneSge(buffer_->data(), kMtu), kMtu)))
      << "precondition: the queue accepts the send behind it";
  admissionExecutor_.drain();

  EXPECT_EQ(tagSet_->getNumPendingWqes(), 3)
      << "the read and the send behind it both reach the Tag, since a post "
         "bound to be flushed spends no read/atomic room";
}

// The error state is one half of postWillBeFlushed(); a tag being deleted is
// the other, and reaches the same check through the same loop.
TEST_F(TagSetTest, AMarkedForDeletionTagsReadIsAdmittedRatherThanHeld) {
  ASSERT_NE(admitAReadThatSpendsTheOnlySlot(), nullptr)
      << "precondition: a read holds the only slot the tag negotiated";
  auto tag = tagSet_->at(1);
  ASSERT_EQ(cqs_[0]->getNumCompletedWqes(), 0)
      << "precondition: the admitted read has not reported, so the count "
         "below is the flushed post on its own";

  tag->markForDeletion();

  ASSERT_TRUE(tagSet_->postWqe(1, makeReadWqe(kWorkReqId + 1)))
      << "precondition: the queue accepts the second read";
  admissionExecutor_.drain();

  EXPECT_EQ(cqs_[0]->getNumCompletedWqes(), 1)
      << "the read reached the Tag, which flushed it: a tag being deleted "
         "spends no responder slot, so the depth has no call to hold it";
}

TEST_F(TagSetTest, RetiringAReadReleasesTheReadHeldBehindIt) {
  Wqe* firstRead = admitAReadThatSpendsTheOnlySlot();
  ASSERT_NE(firstRead, nullptr)
      << "precondition: the read that spends the only slot reached its Tag";
  auto tag = tagSet_->at(1);

  ASSERT_TRUE(tagSet_->postWqe(1, makeReadWqe(kWorkReqId + 1)))
      << "precondition: the queue accepts the second read";
  admissionExecutor_.drain();

  answerTheOnlyFrame(*tag, *firstRead);
  // Reporting posts the baton without returning a slot. Draining consumes that
  // wake here, so retirement's is the only one left to reach the held read.
  admissionExecutor_.drain();
  ASSERT_EQ(tagSet_->getNumPendingWqes(), 0)
      << "precondition: the first read has reported and the second is still "
         "held, so the count below starts from nothing pending";
  ASSERT_FALSE(tag->hasReadAtomicRoom())
      << "precondition: a read that reported still holds its slot until the "
         "request frame settles, or retirement is not what releases it";

  settleRequestFrame(*tag, *firstRead);
  admissionExecutor_.drain();

  EXPECT_EQ(tagSet_->getNumPendingWqes(), 1)
      << "retiring the first read gives its slot back, which is what lets "
         "the second one reach its Tag";
}

TEST_F(TagSetTest, WorkThatSpendsNoSlotIsAdmittedWhileAReadHoldsTheDepth) {
  ASSERT_NE(admitAReadThatSpendsTheOnlySlot(), nullptr)
      << "precondition: a read holds the only slot the tag negotiated";

  ASSERT_TRUE(tagSet_->postWqe(
      1,
      std::make_unique<SendWqe>(
          kWorkReqId + 1, oneSge(buffer_->data(), kMtu), kMtu)))
      << "precondition: the queue accepts the send";
  ASSERT_TRUE(tagSet_->postWqe(
      1,
      std::make_unique<ReceiveWqe>(
          kWorkReqId + 2, oneSge(buffer_->data(), kMtu), kMtu)))
      << "precondition: the queue accepts the receive";
  admissionExecutor_.drain();

  EXPECT_EQ(tagSet_->getNumPendingWqes(), 2)
      << "a send reaches its Tag behind a read holding the depth, since a "
         "send spends no responder read/atomic slot";
  EXPECT_EQ(tagSet_->getNumPendingRWqes(), 1)
      << "a receive reaches its Tag behind a read holding the depth, since "
         "it carries no request frame and so spends no slot";
}

TEST_F(TagSetTest, ErroringATagReleasesAReadAlreadyHeldAtAdmission) {
  ASSERT_NE(admitAReadThatSpendsTheOnlySlot(), nullptr)
      << "precondition: a read holds the only slot the tag negotiated";
  auto tag = tagSet_->at(1);

  ASSERT_TRUE(tagSet_->postWqe(1, makeReadWqe(kWorkReqId + 1)))
      << "precondition: the queue accepts the second read";
  admissionExecutor_.drain();
  ASSERT_EQ(tagSet_->getNumPendingWqes(), 1)
      << "precondition: the second read is held at admission, so the error "
         "transition below is what lets it through";

  enterTagError(*tag);
  admissionExecutor_.drain();

  EXPECT_EQ(tagSet_->getNumPendingWqes(), 2)
      << "the error transition wakes a read already suspended on the depth, "
         "so a post bound to be flushed stops waiting for a slot it never "
         "uses";
}

TEST_F(TagSetTest, AMarkedForDeletionTagHoldsNoOneBehindItsFullMsnSpan) {
  tagSet_->add(1, cqs_[0].get(), ibvQp_);
  tagSet_->add(2, cqs_[1].get(), ibvQp_);
  auto stalled = tagSet_->at(1);
  auto healthy = tagSet_->at(2);

  // One read holds the map head while a full send queue's worth of reported
  // work piles up behind it, which is what closes the msn window.
  auto readWqe = makeReadWqe(kWorkReqId);
  Wqe& read = *readWqe;
  ASSERT_TRUE(postToTag(*stalled, std::move(readWqe), kMtu))
      << "precondition: the tag accepts the read that blocks the head";
  answerTheOnlyFrame(*stalled, read);
  const uint32_t sendsHeldBehindTheRead = kMaxQpWorkRequests - 1;
  for (uint32_t i = 0; i < sendsHeldBehindTheRead; ++i) {
    auto sendWqe = std::make_unique<SendWqe>(
        kWorkReqId + 1 + i, oneSge(buffer_->data(), 1000), kMtu);
    Wqe& send = *sendWqe;
    ASSERT_TRUE(postToTag(*stalled, std::move(sendWqe), kMtu))
        << "precondition: the send queue refused post " << i
        << ", so the span this case needs never forms";
    answerTheOnlyFrame(*stalled, send);
  }
  ASSERT_FALSE(stalled->hasMsnSpanRoom())
      << "precondition: the tag has no room for another msn, or the posts "
         "below cross a span that was never full";
  ASSERT_EQ(stalled->getAcceptedSendQueueEntries(), 0)
      << "precondition: a send returns its queue entry when it reports, so the "
         "depth admits the posts below; retirement keeps the map entry instead";

  stalled->markForDeletion();
  startAdmissionLoop();

  ASSERT_TRUE(tagSet_->postWqe(
      1,
      std::make_unique<SendWqe>(
          kWorkReqId + kMaxQpWorkRequests,
          oneSge(buffer_->data(), kMtu),
          kMtu)))
      << "precondition: the queue accepts a post for the marked tag";
  // Drained with only this post queued, so the loop reaches the healthy tag's
  // post below only by getting past the marked tag's.
  admissionExecutor_.drain();

  ASSERT_TRUE(tagSet_->postWqe(
      2,
      std::make_unique<SendWqe>(
          kWorkReqId + kMaxQpWorkRequests + 1,
          oneSge(buffer_->data(), kMtu),
          kMtu)))
      << "precondition: the queue accepts a post for the healthy tag";
  admissionExecutor_.drain();

  EXPECT_EQ(healthy->getNumPendingWqes(), 1)
      << "a tag being deleted holds no post behind its full msn span, so the "
         "dequeue loop they share moves on to the healthy tag's post";
}

/*
 * A normal transition racing an error publication must lose: ERR is terminal,
 * so a tag the race errored stays errored however the two writes interleave.
 * The error here arrives the way a failing post delivers it -- under the
 * tag's own map lock, without the TagSet mutex the transition holds -- which
 * is the interleaving a check-then-write under one guard cannot see. The
 * readers hold no lock at all, the way datapath error checks run.
 */
TEST_F(TagSetTest, NormalTransitionRacingErrorPublicationKeepsErr) {
  constexpr int kTags = 4;
  constexpr int kTransitionRounds = 200;
  auto tags = addInitTags(kTags);
  ASSERT_EQ(tags.size(), static_cast<size_t>(kTags))
      << "precondition: every tag is added, or the race below has nothing to "
         "transition";

  std::atomic<bool> done{false};
  std::atomic<bool> sawNonErrAfterErr{false};
  std::barrier start{4};
  auto readErrors = [&]() {
    start.arrive_and_wait();
    std::vector<bool> seenErr(tags.size(), false);
    while (!done.load(std::memory_order_acquire)) {
      for (size_t i = 0; i < tags.size(); ++i) {
        const bool inError = tags[i]->isInErrorState();
        if (seenErr[i] && !inError) {
          sawNonErrAfterErr.store(true, std::memory_order_relaxed);
        }
        seenErr[i] = seenErr[i] || inError;
      }
    }
  };
  std::thread readerA(readErrors);
  std::thread readerB(readErrors);
  std::thread transitioner([&]() {
    start.arrive_and_wait();
    for (int i = 0; i < kTransitionRounds; ++i) {
      tagSet_->setIbvQpState(IBV_QPS_RTS);
    }
  });
  std::thread poster([&]() {
    start.arrive_and_wait();
    for (size_t i = 0; i < tags.size(); ++i) {
      auto wqe = std::make_unique<SendWqe>(
          kWorkReqId + i, oneSge(buffer_->data(), kMtu), kMtu);
      wqe->setCompletionStatusOverride(kLocalLenErr);
      postToTag(*tags[i], std::move(wqe), kMtu);
    }
  });
  poster.join();
  transitioner.join();
  done.store(true, std::memory_order_release);
  readerA.join();
  readerB.join();

  EXPECT_FALSE(sawNonErrAfterErr.load())
      << "once a reader sees a tag in error it must never see it leave: ERR "
         "is terminal";
  for (size_t i = 0; i < tags.size(); ++i) {
    EXPECT_TRUE(tags[i]->isInErrorState())
        << "the error publication on tag " << (i + 1)
        << " raced a normal transition, and the transition must lose every "
           "time";
  }
}

/*
 * Error publication is idempotent: any number of threads may error the whole
 * QP at once, and every tag ends errored. Every write stores the same value,
 * so no thread can observe a tag the publication left behind.
 */
TEST_F(TagSetTest, ConcurrentErrorPublicationStaysTerminal) {
  constexpr int kTags = 4;
  constexpr int kPublishers = 3;
  auto tags = addInitTags(kTags);
  ASSERT_EQ(tags.size(), static_cast<size_t>(kTags))
      << "precondition: every tag is added, or the publications below have "
         "nothing to error";

  std::barrier start{kPublishers};
  std::vector<std::thread> publishers;
  publishers.reserve(kPublishers);
  for (int i = 0; i < kPublishers; ++i) {
    publishers.emplace_back([&]() {
      start.arrive_and_wait();
      tagSet_->setIbvQpState(IBV_QPS_ERR);
    });
  }
  for (auto& publisher : publishers) {
    publisher.join();
  }

  for (size_t i = 0; i < tags.size(); ++i) {
    EXPECT_TRUE(tags[i]->isInErrorState())
        << "every publisher errored tag " << (i + 1)
        << ", so none may read back anything but error";
  }
}

/*
 * A compare-and-set racing an error publication loses without moving the
 * state: the failure reports the error that beat it, so a caller looping
 * back acts on the concurrent transition rather than its own stale read.
 */
TEST_F(TagSetTest, CompareAndSetWithStaleExpectedKeepsErr) {
  struct ibv_qp qp = makeMockQp(IBV_QPS_INIT);
  ibv_qp_state expected = loadQpState(qp);
  storeQpErrorState(qp);
  EXPECT_FALSE(compareAndSetQpState(qp, expected, IBV_QPS_RTS))
      << "the error was published first, so the transition must lose";
  EXPECT_EQ(loadQpState(qp), IBV_QPS_ERR)
      << "and the losing transition must not move the state it raced";
  EXPECT_EQ(expected, IBV_QPS_ERR)
      << "the failure reports what beat it, so a caller looping back acts on "
         "the concurrent transition";
}

} // namespace libsoftmetaroce
