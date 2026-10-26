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

#include <cerrno>

#include <folly/ScopeGuard.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/MetaRoceVerbs.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/RecordingConnectionObserver.h>

using namespace testing;

namespace libsoftmetaroce {
DECLARE_int32(libsoftmetaroce_initialmsn);

static std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr /*modifier*/);
}

class TaggedVerbsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pd_ = verbs_.ibv_alloc_pd(&context_);
    ASSERT_NE(pd_, nullptr);
    cq_ = verbs_.ibv_create_cq(&context_, kCqEntries, nullptr, nullptr, 0);
    ASSERT_NE(cq_, nullptr);
  }

  void TearDown() override {
    if (cq_ != nullptr) {
      EXPECT_EQ(verbs_.ibv_destroy_cq(cq_), 0);
    }
    if (pd_ != nullptr) {
      EXPECT_EQ(verbs_.ibv_dealloc_pd(pd_), 0);
    }
  }

  struct ibv_qp*
  addTag(uint32_t baseQpNum, uint8_t tagNum, struct ibv_srq* srq = nullptr) {
    struct ibv_qp_init_attr_ex attr{};
    attr.send_cq = cq_;
    attr.recv_cq = cq_;
    attr.qp_type = IBV_QPT_RC;
    attr.pd = pd_;
    attr.srq = srq;
    attr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
    attr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
    attr.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, tagNum);
    return verbs_.ibv_create_qp_ex(&context_, &attr);
  }

  struct ibv_qp* makeQp() {
    struct ibv_qp_init_attr attr{};
    attr.send_cq = cq_;
    attr.recv_cq = cq_;
    return verbs_.ibv_create_qp(pd_, &attr);
  }

  static constexpr int kCqEntries = 10;

  libsoftmetaroce::MetaRoceVerbs verbs_{mockUDPSocketMaker};
  struct ibv_context context_{};
  struct ibv_cq* cq_{nullptr};
  struct ibv_pd* pd_{nullptr};
};

TEST_F(TaggedVerbsTest, OneTagDrawsFromASharedQueueWhileItsSiblingDoesNot) {
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 1}};
  struct ibv_srq* srq = verbs_.ibv_create_srq(pd_, &srqInitAttr);
  ASSERT_NE(srq, nullptr);

  struct ibv_qp* ownQueue = makeQp();
  ASSERT_NE(ownQueue, nullptr);
  struct ibv_qp* sharedQueue =
      addTag(MetaRoceVerbs::qpAndTagToQp(ownQueue->qp_num), /*tagNum=*/1, srq);
  ASSERT_NE(sharedQueue, nullptr)
      << "precondition: a tag added to a running queue pair may be given a "
         "shared receive queue of its own";

  EXPECT_EQ(sharedQueue->srq, srq)
      << "the tag created with the queue carries it, which is what a client "
         "reads back to tell where its receives belong";
  EXPECT_EQ(ownQueue->srq, nullptr)
      << "giving one tag a shared queue leaves its sibling with the receive "
         "queue it already had: the queue belongs to the tag, not to the queue "
         "pair they share";

  ownQueue->state = IBV_QPS_INIT;
  sharedQueue->state = IBV_QPS_INIT;
  struct ibv_recv_wr recvWr{};
  struct ibv_recv_wr* badWr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_recv(ownQueue, &recvWr, &badWr), 0)
      << "the tag created with no shared queue still takes receives of its "
         "own";
  EXPECT_EQ(verbs_.ibv_post_recv(sharedQueue, &recvWr, &badWr), EINVAL)
      << "its sibling has none to take, so the receive belongs on the shared "
         "queue instead";

  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), EBUSY)
      << "a tag that can still draw from the queue keeps it";
  EXPECT_EQ(verbs_.ibv_destroy_qp(sharedQueue), 0);
  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), EBUSY)
      << "destroying one tag leaves the queue pair standing and every tag's "
         "handle with it, so the reference is not back yet";
  EXPECT_EQ(verbs_.ibv_destroy_qp(ownQueue), 0);
  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0)
      << "the last tag went with the queue pair, so nothing is left to draw";
}

TEST_F(TaggedVerbsTest, TagCreationEncodingAndLifecycle) {
  // Part 1: Create initial QP and verify Tag 0 properties
  struct ibv_qp* qp_tag0 = makeQp();

  // Verify initial Tag number is 0
  EXPECT_EQ(MetaRoceVerbs::qpAndTagToTag(qp_tag0->qp_num), 0);

  // Verify QP number is correctly extracted
  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);
  EXPECT_EQ(baseQpNum, verbs_.getQpByNum(baseQpNum)->getLocalQpNum());

  // Verify QP exists and has one Tag
  auto qp = verbs_.getQpByNum(baseQpNum);
  EXPECT_NE(qp, nullptr);
  EXPECT_EQ(qp->numTags(), 1);

  std::vector<uint8_t> extraTagNums = {1, 2, 15, 63, 127, 128, 254, 255};
  std::vector<uint8_t> allTagNums(extraTagNums.begin(), extraTagNums.end());
  allTagNums.insert(allTagNums.begin(), kMetaRoceInitialTag);

  // Part 2: Test encoding/decoding for various Tag numbers
  for (uint8_t tagNum : allTagNums) {
    uint32_t combined = MetaRoceVerbs::combineQpAndTag(baseQpNum, tagNum);

    // Verify encoding/decoding roundtrip
    EXPECT_EQ(MetaRoceVerbs::qpAndTagToQp(combined), baseQpNum);
    EXPECT_EQ(MetaRoceVerbs::qpAndTagToTag(combined), tagNum);

    // Verify the combined value has the correct structure
    EXPECT_EQ((combined >> 24) & 0xFF, tagNum);
    EXPECT_EQ(combined & 0x00FFFFFF, baseQpNum);
  }

  // Part 3: Create additional Tags using ibv_create_qp_ex
  std::vector<struct ibv_qp*> all_tags;
  all_tags.push_back(qp_tag0);

  struct ibv_qp_init_attr_ex qp_init_attr_ex{};
  qp_init_attr_ex.send_cq = cq_;
  qp_init_attr_ex.recv_cq = cq_;
  qp_init_attr_ex.qp_type = IBV_QPT_RC;
  qp_init_attr_ex.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  qp_init_attr_ex.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  qp_init_attr_ex.pd = pd_;

  for (uint8_t tagNum : extraTagNums) {
    qp_init_attr_ex.source_qpn =
        MetaRoceVerbs::combineQpAndTag(baseQpNum, tagNum);
    struct ibv_qp* tag_qp =
        verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);

    EXPECT_NE(tag_qp, nullptr);
    EXPECT_EQ(MetaRoceVerbs::qpAndTagToTag(tag_qp->qp_num), tagNum);
    EXPECT_EQ(MetaRoceVerbs::qpAndTagToQp(tag_qp->qp_num), baseQpNum);

    all_tags.push_back(tag_qp);
  }
  EXPECT_EQ(qp->numTags(), extraTagNums.size() + 1);

  // Test duplicate Tag creation - refused with EINVAL
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(
      baseQpNum, extraTagNums[0]); // Already exists
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex), nullptr)
      << "the queue pair holds this tag already, which the verb reports by "
         "returning no queue pair";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  // Verify Tag count remains unchanged after failed duplicate creation
  EXPECT_EQ(qp->numTags(), extraTagNums.size() + 1);

  // Part 4: Verify all Tags share the same underlying QP
  EXPECT_EQ(qp->getLocalQpNum(), baseQpNum);

  // Verify each Tag points to the correct QP number and Tag number
  for (size_t i = 0; i < all_tags.size(); ++i) {
    EXPECT_EQ(MetaRoceVerbs::qpAndTagToTag(all_tags[i]->qp_num), allTagNums[i]);
    EXPECT_EQ(MetaRoceVerbs::qpAndTagToQp(all_tags[i]->qp_num), baseQpNum);
  }

  // Part 5: Test Tag destruction and QP cleanup.
  // Destroying a Tag marks it for deletion but leaves it in the table.
  for (size_t i = 1; i < all_tags.size(); ++i) {
    EXPECT_EQ(verbs_.ibv_destroy_qp(all_tags[i]), 0)
        << "precondition: destroying the Tag must succeed, or the QP read "
           "below still holds every Tag live, for tag "
        << (uint32_t)allTagNums[i];
    qp = verbs_.getQpByNum(baseQpNum);
    EXPECT_NE(qp, nullptr)
        << "the QP must stay live until all its Tags are destroyed";
    EXPECT_EQ(qp->numTags(), all_tags.size())
        << "a destroyed Tag must stay in the table until the whole QP is torn "
           "down";
  }

  EXPECT_EQ(verbs_.ibv_destroy_qp(all_tags[0]), 0)
      << "precondition: destroying the last Tag must succeed, or the QP read "
         "below is still live because that Tag is";
  qp = verbs_.getQpByNum(baseQpNum);
  EXPECT_EQ(qp, nullptr)
      << "destroying the last live Tag must tear the whole QP down";
}

TEST_F(TaggedVerbsTest, DestroyingTagMarksItAndKeepsQpLive) {
  // Create a QP with Tag 0 and add Tag 1, so the QP stays live after one Tag is
  // destroyed.
  struct ibv_qp* qp_tag0 = makeQp();
  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  struct ibv_qp* qp_tag1 = addTag(baseQpNum, 1);

  auto qp = verbs_.getQpByNum(baseQpNum);
  ASSERT_NE(qp, nullptr);

  // Destroying Tag 1 marks it but leaves it in the table; the QP stays live for
  // its remaining Tag.
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0)
      << "precondition: destroying Tag 1 must succeed, or it is not marked and "
         "left in the table as the checks below read it";
  qp = verbs_.getQpByNum(baseQpNum);
  ASSERT_NE(qp, nullptr)
      << "destroying one Tag of a multi-Tag QP must not tear the QP down";
  EXPECT_EQ(qp->numTags(), 2)
      << "the destroyed Tag must stay in the table so readers can still ack its "
         "in-flight WQEs";
  EXPECT_TRUE(qp->getTag(1)->isMarkedForDeletion())
      << "a destroyed Tag must be marked so it accepts no new work";
  EXPECT_FALSE(qp->getTag(0)->isMarkedForDeletion())
      << "destroying one Tag must not disturb its siblings";
  // The lingering Tag still dereferences its ibv_qp handle on the ack/retx/rx
  // error paths, so the handle must stay valid until QP teardown -- reading it
  // here is a use-after-free if ibv_destroy_qp freed it early.
  EXPECT_EQ(
      qp->getTag(1)->getIbvQp().qp_num,
      MetaRoceVerbs::combineQpAndTag(baseQpNum, 1))
      << "a destroyed Tag's ibv_qp handle must stay valid while it lingers";

  // Destroying the last live Tag marks the final Tag, so the whole QP is torn
  // down (which also exercises allTagsMarkedForDeletion() returning true).
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0)
      << "precondition: destroying the last Tag must succeed, or the QP read "
         "below is still live because that Tag is";
  EXPECT_EQ(verbs_.getQpByNum(baseQpNum), nullptr)
      << "the QP must be reclaimed once its last Tag is destroyed";
}

TEST(MetaRoceVerbsWithTagsTest, ObserverRetiresQpOnlyWithFinalTag) {
  RecordingConnectionObserver observer;
  MetaRoceVerbs verbs(mockUDPSocketMaker);
  verbs.registerConnectionObserver(observer);

  struct ibv_context context{};
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_cq* cq = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  struct ibv_qp_init_attr qpInitAttr{
      .send_cq = cq,
      .recv_cq = cq,
  };
  struct ibv_qp* tag0 = verbs.ibv_create_qp(pd, &qpInitAttr);
  const auto localQpNum = MetaRoceVerbs::qpAndTagToQp(tag0->qp_num);

  struct ibv_qp_init_attr_ex tagInitAttr{
      .send_cq = cq,
      .recv_cq = cq,
      .qp_type = IBV_QPT_RC,
      .comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS,
      .pd = pd,
      .create_flags = IBV_QP_CREATE_SOURCE_QPN,
  };
  tagInitAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(localQpNum, 1);
  struct ibv_qp* tag1 = verbs.ibv_create_qp_ex(&context, &tagInitAttr);
  ASSERT_NE(tag1, nullptr) << "the sibling tag must be created";

  ASSERT_EQ(verbs.ibv_destroy_qp(tag1), 0)
      << "destroying a non-final tag must succeed";
  EXPECT_TRUE(observer.retiringQps.empty())
      << "a non-final tag must not retire the shared QP";
  ASSERT_NE(verbs.getQpByNum(localQpNum), nullptr)
      << "precondition: the shared QP must still be alive for the final-tag "
         "destroy below to be the one that retires it";

  ASSERT_EQ(verbs.ibv_destroy_qp(tag0), 0)
      << "destroying the final tag must succeed";
  ASSERT_EQ(observer.retiringQps.size(), 1)
      << "destroying the final tag must publish one QP retirement";
  EXPECT_EQ(observer.retiringQps[0], localQpNum)
      << "the retirement callback must identify the local QP being retired";

  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

TEST(
    MetaRoceVerbsWithTagsTest,
    ADestroyedTagHoldsItsQueueUntilTheQpIsTornDown) {
  MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_context context{};
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_cq* cq0 = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(cq0, nullptr);
  struct ibv_cq* cq1 = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(cq1, nullptr);

  struct ibv_qp_init_attr qpInitAttr{.send_cq = cq0, .recv_cq = cq0};
  struct ibv_qp* tag0 = verbs.ibv_create_qp(pd, &qpInitAttr);
  ASSERT_NE(tag0, nullptr);
  const auto localQpNum = MetaRoceVerbs::qpAndTagToQp(tag0->qp_num);

  // A tag of its own completion queue, so the destroy below takes a queue no
  // surviving tag reports into.
  struct ibv_qp_init_attr_ex tagInitAttr{
      .send_cq = cq1,
      .recv_cq = cq1,
      .qp_type = IBV_QPT_RC,
      .comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS,
      .pd = pd,
      .create_flags = IBV_QP_CREATE_SOURCE_QPN,
  };
  tagInitAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(localQpNum, 1);
  struct ibv_qp* tag1 = verbs.ibv_create_qp_ex(&context, &tagInitAttr);
  ASSERT_NE(tag1, nullptr)
      << "precondition: the sibling tag must be created, or the destroy below "
         "tears the queue pair down instead of leaving one tag lingering";

  ASSERT_EQ(verbs.ibv_destroy_qp(tag1), 0)
      << "precondition: destroying the sibling tag must succeed";
  ASSERT_EQ(verbs.ibv_destroy_cq(cq1), EBUSY)
      << "a destroyed tag carries on with the work it already accepted, so it "
         "can still report into its completion queue until the queue pair it "
         "shares is torn down";

  ASSERT_EQ(verbs.ibv_destroy_qp(tag0), 0)
      << "precondition: destroying the last tag tears the queue pair down, "
         "which is what flushes the lingering tag and frees its queue";
  EXPECT_EQ(verbs.ibv_destroy_cq(cq1), 0)
      << "the teardown flush left the lingering tag nothing further to report";
  EXPECT_EQ(verbs.ibv_destroy_cq(cq0), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

TEST_F(TaggedVerbsTest, DoubleDestroyOfALingeringTagIsRefused) {
  struct ibv_qp* qp_tag0 = makeQp();
  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  struct ibv_qp* qp_tag1 = addTag(baseQpNum, 1);

  // Destroying a non-last Tag leaves it marked and lingering (its handle is not
  // freed until the whole QP is torn down), so a second destroy of the same
  // handle is observable and must be rejected as misuse.
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0)
      << "precondition: the first destroy must take, or the second is not a "
         "double destroy";
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), ENOENT)
      << "a double destroy of a Tag must be detected and reported through the "
         "return a C caller can read";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
}

TEST_F(TaggedVerbsTest, InvalidTagOperations) {
  // Test: Try to create Tag on non-existent QP
  struct ibv_qp_init_attr_ex qp_init_attr_ex{};
  qp_init_attr_ex.send_cq = cq_;
  qp_init_attr_ex.recv_cq = cq_;
  qp_init_attr_ex.qp_type = IBV_QPT_RC;
  qp_init_attr_ex.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  qp_init_attr_ex.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  qp_init_attr_ex.source_qpn =
      MetaRoceVerbs::combineQpAndTag(9999, 1); // Non-existent QP
  qp_init_attr_ex.pd = pd_;

  struct ibv_qp* invalid_qp =
      verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);
  EXPECT_EQ(invalid_qp, nullptr);

  // Create a valid QP first
  struct ibv_qp* valid_qp = makeQp();

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(valid_qp->qp_num);

  // Transition QP to non-RESET state
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  struct ibv_qp_attr init_attr{};
  memcpy(init_attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  init_attr.dest_qp_num = 10;
  init_attr.path_mtu = IBV_MTU_4096;
  init_attr.qp_state = IBV_QPS_INIT;
  verbs_.ibv_modify_qp(
      valid_qp,
      &init_attr,
      IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);

  // Test: Try to add Tag to QP not in RESET state
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  struct ibv_qp* invalid_state_qp =
      verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);
  EXPECT_EQ(invalid_state_qp, nullptr);

  EXPECT_EQ(verbs_.ibv_destroy_qp(valid_qp), 0);
}

TEST_F(TaggedVerbsTest, ModifyingATagTheQueuePairNeverHadIsRefused) {
  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);

  // A fabricated handle: the queue pair is real, but the tag is missing.
  struct ibv_qp fabricated = *qp;
  fabricated.qp_num = MetaRoceVerbs::combineQpAndTag(
      MetaRoceVerbs::qpAndTagToQp(qp->qp_num), 7);

  struct ibv_qp_attr attr{};
  attr.timeout = 14;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(&fabricated, &attr, IBV_QP_TIMEOUT), ENOENT)
      << "a tag the queue pair never had is one lsmr looked for and does not "
         "hold, which is what its sibling guards answer for a handle lsmr did "
         "not issue";
  EXPECT_EQ(errno, ENOENT)
      << "an int-returning verb sets errno as well as returning it; a caller "
         "reading errno otherwise sees whatever failed before this call";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
}

TEST_F(TaggedVerbsTest, PostWqesWithDifferentTags) {
  // Create initial QP with Tag 0 (in RESET state)
  struct ibv_qp* qp_tag0 = makeQp();
  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  // Add Tag 1 while QP is still in RESET state
  struct ibv_qp* qp_tag1 = addTag(baseQpNum, 1);
  EXPECT_NE(qp_tag1, nullptr);

  // Now setup both Tags to RTS
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  const auto destQpNum = 10;

  struct ibv_qp_attr init_attr{};
  memcpy(init_attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  init_attr.dest_qp_num = destQpNum;
  init_attr.path_mtu = IBV_MTU_4096;
  init_attr.qp_state = IBV_QPS_INIT;
  verbs_.ibv_modify_qp(
      qp_tag0,
      &init_attr,
      IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);
  init_attr.qp_state = IBV_QPS_RTR;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);
  init_attr.qp_state = IBV_QPS_RTS;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);

  auto qp = verbs_.getQpByNum(baseQpNum);
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  EXPECT_CALL(*congestionController, getAvailableWindowSize(_))
      .WillRepeatedly(Return(1));
  qp->setCongestionController(std::move(congestionController));

  // Create memory regions for WQEs
  const uint32_t wqeSizeBytes = 1024;
  std::unique_ptr<char[]> payload0(new char[wqeSizeBytes]);
  std::unique_ptr<char[]> payload1(new char[wqeSizeBytes]);

  ibv_mr* mr0 = verbs_.ibv_reg_mr_internal(
      pd_,
      payload0.get(),
      wqeSizeBytes,
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_LOCAL_WRITE);
  ibv_mr* mr1 = verbs_.ibv_reg_mr_internal(
      pd_,
      payload1.get(),
      wqeSizeBytes,
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_LOCAL_WRITE);
  ASSERT_NE(mr0, nullptr);
  ASSERT_NE(mr1, nullptr);

  // Post send WQE on Tag 0
  struct ibv_sge sge0 = {
      .addr = (uint64_t)payload0.get(),
      .length = wqeSizeBytes,
      .lkey = mr0->lkey};
  struct ibv_send_wr send_wr0 = {
      .wr_id = 100,
      .sg_list = &sge0,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED};
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(qp_tag0, &send_wr0, &bad_wr), 0);

  // Post send WQE on Tag 1
  struct ibv_sge sge1 = {
      .addr = (uint64_t)payload1.get(),
      .length = wqeSizeBytes,
      .lkey = mr1->lkey};
  struct ibv_send_wr send_wr1 = {
      .wr_id = 200,
      .sg_list = &sge1,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED};
  EXPECT_EQ(verbs_.ibv_post_send(qp_tag1, &send_wr1, &bad_wr), 0);

  // Poll until WQEs have been processed into Tags.
  ASSERT_TRUE(qp->waitForWqesToBePosted(2));

  // Verify both WQEs are posted to the same underlying QP
  EXPECT_EQ(qp->getNumPendingWqes(), 2);

  EXPECT_EQ(verbs_.ibv_dereg_mr(mr0), 0);
  EXPECT_EQ(verbs_.ibv_dereg_mr(mr1), 0);
  // Destroy Tag 1 first: it lingers marked with its send still outstanding.
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0)
      << "precondition: destroying Tag 1 must succeed, or it is not the "
         "lingering Tag whose send the teardown below flushes";
  // Destroying the last Tag tears the whole QP down and flushes every Tag's
  // outstanding send in error -- including Tag 1's, which drained via teardown.
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0)
      << "precondition: tearing the QP down is what flushes both Tags";

  struct ibv_wc wc{};
  bool tag0Flushed = false;
  bool tag1Flushed = false;
  while (verbs_.ibv_poll_cq(cq_, 1, &wc) > 0) {
    if (wc.wr_id == 100) {
      tag0Flushed = (wc.status == IBV_WC_WR_FLUSH_ERR);
    } else if (wc.wr_id == 200) {
      tag1Flushed = (wc.status == IBV_WC_WR_FLUSH_ERR);
    }
  }
  EXPECT_TRUE(tag1Flushed)
      << "a lingering Tag's outstanding send must flush in error at QP teardown";
  EXPECT_TRUE(tag0Flushed)
      << "the last Tag's outstanding send must flush in error at QP teardown";
}

TEST_F(TaggedVerbsTest, TagStateManagementAndIsolation) {
  // Create QP with multiple Tags
  struct ibv_qp* qp_tag0 = makeQp();

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  // Create additional Tags
  struct ibv_qp_init_attr_ex qp_init_attr_ex{};
  qp_init_attr_ex.send_cq = cq_;
  qp_init_attr_ex.recv_cq = cq_;
  qp_init_attr_ex.qp_type = IBV_QPT_RC;
  qp_init_attr_ex.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  qp_init_attr_ex.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  qp_init_attr_ex.pd = pd_;

  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  struct ibv_qp* qp_tag1 = verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);

  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 2);
  struct ibv_qp* qp_tag2 = verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);

  auto qp = verbs_.getQpByNum(baseQpNum);
  EXPECT_EQ(qp->numTags(), 3);

  // Setup to RTS state for testing Tag-specific operations
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  struct ibv_qp_attr init_attr{};
  memcpy(init_attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  init_attr.dest_qp_num = 10;
  init_attr.path_mtu = IBV_MTU_4096;
  init_attr.qp_state = IBV_QPS_INIT;
  verbs_.ibv_modify_qp(
      qp_tag0,
      &init_attr,
      IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);
  auto tag0 = qp->getTag(kMetaRoceInitialTag);
  auto tag1 = qp->getTag(1);
  auto tag2 = qp->getTag(2);

  // Verify Tags share the same QP but have independent Tag-specific state
  EXPECT_EQ(tag0->getNextMsn(),
            tag1->getNextMsn()); // Initially same
  EXPECT_EQ(tag1->getNextMsn(), tag2->getNextMsn()); // Initially same

  // Set different MSN for different Tags, which a tag takes only while it
  // cannot yet send.
  ASSERT_TRUE(tag0->setNextMsn(100))
      << "precondition: each tag must accept its independent send numbering";
  ASSERT_TRUE(tag1->setNextMsn(200))
      << "precondition: each tag must accept its independent send numbering";
  ASSERT_TRUE(tag2->setNextMsn(300))
      << "precondition: each tag must accept its independent send numbering";

  init_attr.qp_state = IBV_QPS_RTR;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);
  init_attr.qp_state = IBV_QPS_RTS;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);

  // Test that each Tag can have different sequence numbers (MSN/CSN)
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  EXPECT_CALL(*congestionController, getAvailableWindowSize(_))
      .WillRepeatedly(Return(1));
  qp->setCongestionController(std::move(congestionController));

  EXPECT_EQ(tag0->getNextMsn(), 100);
  EXPECT_EQ(tag1->getNextMsn(), 200);
  EXPECT_EQ(tag2->getNextMsn(), 300);

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag2), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
}

TEST_F(TaggedVerbsTest, ConcurrentTagOperations) {
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  const auto destQpNum = 10;

  // Create multiple QPs with Tags (first create all Tags while in RESET state)
  std::vector<std::pair<struct ibv_qp*, uint32_t>> qps_with_tags;

  for (int i = 0; i < 3; ++i) {
    // Create initial QP with Tag 0 (in RESET state)
    struct ibv_qp* qp_tag0 = makeQp();
    uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);
    qps_with_tags.emplace_back(qp_tag0, baseQpNum);

    // Add additional Tags while QP is still in RESET state
    for (uint8_t tagNum = 1; tagNum <= 2; ++tagNum) {
      struct ibv_qp* tag_qp = addTag(baseQpNum, tagNum);
      EXPECT_NE(tag_qp, nullptr);

      qps_with_tags.emplace_back(tag_qp, baseQpNum);
    }

    // Now setup to RTS state (only need to setup one Tag per QP)
    struct ibv_qp_attr init_attr{};
    memcpy(init_attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
    init_attr.dest_qp_num = destQpNum;
    init_attr.path_mtu = IBV_MTU_4096;
    init_attr.qp_state = IBV_QPS_INIT;
    verbs_.ibv_modify_qp(
        qp_tag0,
        &init_attr,
        IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);
    init_attr.qp_state = IBV_QPS_RTR;
    verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);
    init_attr.qp_state = IBV_QPS_RTS;
    verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);
  }

  // Setup congestion controllers for all underlying QPs
  for (int i = 0; i < 3; ++i) {
    uint32_t baseQpNum = qps_with_tags[i * 3].second; // First Tag of each QP
    auto qp = verbs_.getQpByNum(baseQpNum);
    EXPECT_EQ(qp->numTags(), 3);

    auto congestionController =
        std::make_unique<NiceMock<MockCongestionController>>();
    EXPECT_CALL(*congestionController, getAvailableWindowSize(_))
        .WillRepeatedly(Return(1));
    qp->setCongestionController(std::move(congestionController));
  }

  // Post WQEs concurrently on multiple Tags across multiple QPs

  std::vector<std::unique_ptr<char[]>> payloads;
  std::vector<ibv_mr*> memory_regions;

  const uint32_t wqeSizeBytes = 512;
  int wqe_id = 1000;

  for (size_t i = 0; i < qps_with_tags.size(); ++i) {
    auto payload = std::make_unique<char[]>(wqeSizeBytes);
    ibv_mr* mr = verbs_.ibv_reg_mr_internal(
        pd_,
        payload.get(),
        wqeSizeBytes,
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
            IBV_ACCESS_LOCAL_WRITE);
    ASSERT_NE(mr, nullptr);

    struct ibv_sge sge = {
        .addr = (uint64_t)payload.get(),
        .length = wqeSizeBytes,
        .lkey = mr->lkey};
    struct ibv_send_wr send_wr = {
        .wr_id = static_cast<uint64_t>(wqe_id++),
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED};
    struct ibv_send_wr* bad_wr = nullptr;
    EXPECT_EQ(
        verbs_.ibv_post_send(qps_with_tags[i].first, &send_wr, &bad_wr), 0);

    payloads.push_back(std::move(payload));
    memory_regions.push_back(mr);
  }

  // Poll until WQEs have been processed into Tags.
  for (auto& [qp, _] : qps_with_tags) {
    // 3 Tags per QP each get 1 WQE, so expect 3 WQEs per QP.
    ASSERT_TRUE(verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(qp->qp_num))
                    ->waitForWqesToBePosted(3));
  }

  // Verify all WQEs are posted correctly across different QPs and Tags
  int total_pending = 0;
  for (int i = 0; i < 3; ++i) {
    uint32_t baseQpNum = qps_with_tags[i * 3].second;
    auto qp = verbs_.getQpByNum(baseQpNum);
    total_pending += qp->getNumPendingWqes();
  }
  EXPECT_EQ(total_pending, 9); // 3 QPs × 3 Tags each = 9 WQEs

  // Clean up
  for (auto* mr : memory_regions) {
    const auto lkey = mr->lkey;
    EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0)
        << "for the region with lkey " << lkey;
  }
  for (auto& qp_tag_pair : qps_with_tags) {
    const auto qpNum = qp_tag_pair.first->qp_num;
    EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag_pair.first), 0)
        << "for the handle with qp_num " << qpNum;
  }
}

TEST_F(TaggedVerbsTest, TagSequenceNumberIsolation) {
  // Create QP with multiple Tags
  struct ibv_qp* qp_tag0 = makeQp();

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  struct ibv_qp* qp_tag1 = addTag(baseQpNum, 1);

  // Setup QP to RTS
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  struct ibv_qp_attr init_attr{};
  memcpy(init_attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  init_attr.dest_qp_num = 10;
  init_attr.path_mtu = IBV_MTU_4096;
  init_attr.qp_state = IBV_QPS_INIT;
  verbs_.ibv_modify_qp(
      qp_tag0,
      &init_attr,
      IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);
  auto qp = verbs_.getQpByNum(baseQpNum);
  auto tag0 = qp->getTag(kMetaRoceInitialTag);
  auto tag1 = qp->getTag(1);

  // Test that different Tags maintain independent sequence numbers
  EXPECT_EQ(tag0->getNextMsn(), tag1->getNextMsn()); // Initially same

  // Advance MSN for Tag 0. A tag takes its numbering only while it cannot yet
  // send, and the tags reach RTS together, so both are numbered first.
  ASSERT_TRUE(tag0->setNextMsn(100))
      << "precondition: each tag must accept its independent send numbering";
  EXPECT_EQ(tag0->getNextMsn(), 100);
  EXPECT_NE(tag1->getNextMsn(), 100); // Tag 1 should be different

  // Advance MSN for Tag 1
  ASSERT_TRUE(tag1->setNextMsn(200))
      << "precondition: each tag must accept its independent send numbering";
  EXPECT_EQ(tag0->getNextMsn(), 100); // Tag 0 unchanged
  EXPECT_EQ(tag1->getNextMsn(), 200); // Tag 1 updated

  init_attr.qp_state = IBV_QPS_RTR;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);
  init_attr.qp_state = IBV_QPS_RTS;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);

  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  EXPECT_CALL(*congestionController, getAvailableWindowSize(_))
      .WillRepeatedly(Return(1));
  qp->setCongestionController(std::move(congestionController));

  EXPECT_EQ(tag0->getNextMsn(), 100) << "the numbering survives reaching RTS";
  EXPECT_EQ(tag1->getNextMsn(), 200) << "the numbering survives reaching RTS";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
}

TEST(MetaRoceVerbsWithTagsTest, TagEncodingBoundaryValues) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  // Test boundary values for QP numbers and Tag numbers
  std::vector<std::pair<uint32_t, uint8_t>> testCases = {
      {0, 0}, // Minimum values
      {1, 1}, // Small values
      {0xFFFFFF, 0}, // Max QP number, min Tag
      {0, 0xFF}, // Min QP number, max Tag
      {0xFFFFFF, 0xFF}, // Maximum values
      {0x123456, 0x78}, // Random middle values
      {0x800000, 0x80}, // Mid-range values
  };

  for (const auto& [qpNum, tagNum] : testCases) {
    uint32_t combined = MetaRoceVerbs::combineQpAndTag(qpNum, tagNum);

    // Verify encoding/decoding roundtrip
    EXPECT_EQ(MetaRoceVerbs::qpAndTagToQp(combined), qpNum)
        << "Failed QP encoding/decoding for QP:" << qpNum
        << " Tag:" << (int)tagNum;
    EXPECT_EQ(MetaRoceVerbs::qpAndTagToTag(combined), tagNum)
        << "Failed Tag encoding/decoding for QP:" << qpNum
        << " Tag:" << (int)tagNum;

    // Verify bit structure
    EXPECT_EQ((combined >> 24) & 0xFF, tagNum)
        << "Tag bits incorrect for QP:" << qpNum << " Tag:" << (int)tagNum;
    EXPECT_EQ(combined & 0x00FFFFFF, qpNum)
        << "QP bits incorrect for QP:" << qpNum << " Tag:" << (int)tagNum;
  }
}

TEST(MetaRoceVerbsWithTagsTest, TagsWithDifferentCompletionQueues) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_context context{};
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);

  // Create multiple completion queues
  struct ibv_cq* cq1 = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  struct ibv_cq* cq2 = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  struct ibv_cq* cq3 = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);

  // Create initial QP with Tag 0 using cq1
  struct ibv_qp_init_attr qp_init_attr{};
  qp_init_attr.send_cq = cq1;
  qp_init_attr.recv_cq = cq1;
  struct ibv_qp* qp_tag0 = verbs.ibv_create_qp(pd, &qp_init_attr);

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  // Create Tag 1 with cq2
  struct ibv_qp_init_attr_ex qp_init_attr_ex{};
  qp_init_attr_ex.send_cq = cq2;
  qp_init_attr_ex.recv_cq = cq2;
  qp_init_attr_ex.qp_type = IBV_QPT_RC;
  qp_init_attr_ex.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  qp_init_attr_ex.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  qp_init_attr_ex.pd = pd;

  struct ibv_qp* qp_tag1 = verbs.ibv_create_qp_ex(&context, &qp_init_attr_ex);
  EXPECT_NE(qp_tag1, nullptr);

  // Create Tag 2 with cq3
  qp_init_attr_ex.send_cq = cq3;
  qp_init_attr_ex.recv_cq = cq3;
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 2);
  struct ibv_qp* qp_tag2 = verbs.ibv_create_qp_ex(&context, &qp_init_attr_ex);
  EXPECT_NE(qp_tag2, nullptr);

  // Verify Tags are associated with different CQs
  EXPECT_EQ(qp_tag0->send_cq->handle, cq1->handle);
  EXPECT_EQ(qp_tag1->send_cq->handle, cq2->handle);
  EXPECT_EQ(qp_tag2->send_cq->handle, cq3->handle);

  // Verify all Tags belong to same QP
  auto qp = verbs.getQpByNum(baseQpNum);
  EXPECT_EQ(qp->numTags(), 3);

  EXPECT_EQ(verbs.ibv_destroy_qp(qp_tag2), 0);
  EXPECT_EQ(verbs.ibv_destroy_qp(qp_tag1), 0);
  EXPECT_EQ(verbs.ibv_destroy_qp(qp_tag0), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq3), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq2), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq1), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

TEST_F(TaggedVerbsTest, InvalidTagNumberHandling) {
  // Create initial QP
  struct ibv_qp* initial_qp = makeQp();

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(initial_qp->qp_num);

  // Test valid edge cases first
  struct ibv_qp_init_attr_ex qp_init_attr_ex{};
  qp_init_attr_ex.send_cq = cq_;
  qp_init_attr_ex.recv_cq = cq_;
  qp_init_attr_ex.qp_type = IBV_QPT_RC;
  qp_init_attr_ex.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  qp_init_attr_ex.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  qp_init_attr_ex.pd = pd_;

  // Test maximum valid Tag number (255)
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 255);
  struct ibv_qp* max_tag_qp =
      verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);
  EXPECT_NE(max_tag_qp, nullptr);
  EXPECT_EQ(MetaRoceVerbs::qpAndTagToTag(max_tag_qp->qp_num), 255);

  // Test creating duplicate Tag (refused with EINVAL)
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 0);
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex), nullptr)
      << "the initial tag is one the queue pair holds";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  // Test creating duplicate of the max Tag
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 255);
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex), nullptr)
      << "the tag at the top of the range is held like any other";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  EXPECT_EQ(verbs_.ibv_destroy_qp(max_tag_qp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(initial_qp), 0);
}

TEST_F(TaggedVerbsTest, TagBehaviorDuringQpStateTransitions) {
  // Create QP with multiple Tags in RESET state
  struct ibv_qp* qp_tag0 = makeQp();

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  struct ibv_qp_init_attr_ex qp_init_attr_ex{};
  qp_init_attr_ex.send_cq = cq_;
  qp_init_attr_ex.recv_cq = cq_;
  qp_init_attr_ex.qp_type = IBV_QPT_RC;
  qp_init_attr_ex.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  qp_init_attr_ex.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  qp_init_attr_ex.pd = pd_;

  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  struct ibv_qp* qp_tag1 = verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);

  // Verify both Tags are in RESET state
  EXPECT_EQ(qp_tag0->state, IBV_QPS_RESET);
  EXPECT_EQ(qp_tag1->state, IBV_QPS_RESET);

  auto qp = verbs_.getQpByNum(baseQpNum);
  EXPECT_EQ(qp->numTags(), 2);

  // Cannot add more Tags once QP moves out of RESET
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  struct ibv_qp_attr init_attr{};
  memcpy(init_attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  init_attr.dest_qp_num = 10;
  init_attr.path_mtu = IBV_MTU_4096;
  init_attr.qp_state = IBV_QPS_INIT;
  verbs_.ibv_modify_qp(
      qp_tag0,
      &init_attr,
      IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);

  EXPECT_TRUE(qp->allTagsInState(IBV_QPS_INIT))
      << "modifying the QP to INIT must move every Tag on it";

  // Try to add Tag 2 - should fail because QP is no longer in RESET
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 2);
  struct ibv_qp* qp_tag2 = verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);
  EXPECT_EQ(qp_tag2, nullptr);

  // Verify Tag count unchanged
  EXPECT_EQ(qp->numTags(), 2);

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
}

TEST_F(TaggedVerbsTest, TagPsnHandling) {
  // Create QP with multiple Tags
  struct ibv_qp* qp_tag0 = makeQp();

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  struct ibv_qp* qp_tag1 = addTag(baseQpNum, 1);

  // Test PSN modification for different Tags
  bool oldIgnoreModifyPsn = FLAGS_libsoftmetaroce_ignore_modify_psn;
  FLAGS_libsoftmetaroce_ignore_modify_psn = false;

  struct ibv_qp_attr attr{};
  attr.sq_psn = 0x12345678; // MSN in lower 16 bits, CSN in upper 16 bits
  attr.rq_psn = 0x9ABCDEF0;

  // Modify PSN for Tag 0
  verbs_.ibv_modify_qp(qp_tag0, &attr, IBV_QP_SQ_PSN | IBV_QP_RQ_PSN);

  auto qp = verbs_.getQpByNum(baseQpNum);
  auto tag0 = qp->getTag(kMetaRoceInitialTag);
  auto tag1 = qp->getTag(1);

  // Verify Tag 0 has the set PSN values
  EXPECT_EQ(tag0->getNextMsn(), 0x5678); // Lower 16 bits
  EXPECT_EQ(tag0->getNextCsn(), 0x1234); // Upper 16 bits
  EXPECT_EQ(tag0->getCDMSN(), 0xDEEF); // Lower 16 bits - 1
  EXPECT_EQ(tag0->getNextRxCsn(), 0x9ABC); // Upper 16 bits

  // Modify PSN for Tag 1 with different values
  attr.sq_psn = 0xAABBCCDD;
  attr.rq_psn = 0x11223344;
  verbs_.ibv_modify_qp(qp_tag1, &attr, IBV_QP_SQ_PSN | IBV_QP_RQ_PSN);

  // Verify Tag 1 has different PSN values
  EXPECT_EQ(tag1->getNextMsn(), 0xCCDD);
  EXPECT_EQ(tag1->getNextCsn(), 0xAABB);
  EXPECT_EQ(tag1->getCDMSN(), 0x3343); // Lower 16 bits - 1
  EXPECT_EQ(tag1->getNextRxCsn(), 0x1122);

  // Verify Tag 0 values are unchanged
  EXPECT_EQ(tag0->getNextMsn(), 0x5678);
  EXPECT_EQ(tag0->getNextCsn(), 0x1234);

  FLAGS_libsoftmetaroce_ignore_modify_psn =
      oldIgnoreModifyPsn; // Reset to default

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
}

TEST(MetaRoceVerbsWithTagsTest, IgnoredPsnPublishesCoreOwnCsn) {
  RecordingConnectionObserver observer;
  MetaRoceVerbs verbs(mockUDPSocketMaker);
  verbs.registerConnectionObserver(observer);

  struct ibv_context context{};
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_cq* cq = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  struct ibv_qp_init_attr qpInitAttr{
      .send_cq = cq,
      .recv_cq = cq,
  };
  struct ibv_qp* ibvqp = verbs.ibv_create_qp(pd, &qpInitAttr);
  observer.tagConfigs.clear();
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_ignore_modify_psn = true;

  struct ibv_qp_attr attr{
      .qp_state = IBV_QPS_INIT,
      .rq_psn = 0x12340001,
      .sq_psn = 0x56780002,
  };
  ASSERT_EQ(
      verbs.ibv_modify_qp(
          ibvqp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_RQ_PSN),
      0)
      << "precondition: this case needs a modify core accepts while the flag "
         "discards the caller's PSN";
  ASSERT_EQ(observer.tagConfigs.size(), 1)
      << "the successful modify must publish the retained sequence state";
  EXPECT_EQ(observer.tagConfigs[0].sendCsn, kUnconfiguredStartingCsn)
      << "an ignored SQ PSN must publish core's own CSN, not the caller's";
  EXPECT_EQ(observer.tagConfigs[0].receiveCsn, kUnconfiguredStartingCsn)
      << "an ignored RQ PSN must publish core's own CSN, not the caller's";
  EXPECT_EQ(
      observer.tagConfigs[0].sendMsn,
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_initialmsn))
      << "an ignored SQ PSN must publish the send msn core starts on, not the "
         "caller's";
  EXPECT_EQ(
      observer.tagConfigs[0].receiveMsn,
      static_cast<uint16_t>(FLAGS_libsoftmetaroce_initialmsn - 1))
      << "an ignored RQ PSN must publish the receive msn core starts on, one "
         "below the first it expects to deliver";

  EXPECT_EQ(verbs.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

TEST_F(TaggedVerbsTest, TagWqePostingDifferentOpcodes) {
  // Create QP with Tags and setup to RTS
  struct ibv_qp* qp_tag0 = makeQp();

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  struct ibv_qp* qp_tag1 = addTag(baseQpNum, 1);

  // Setup to RTS
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  struct ibv_qp_attr init_attr{};
  memcpy(init_attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  init_attr.dest_qp_num = 10;
  init_attr.path_mtu = IBV_MTU_4096;
  init_attr.qp_state = IBV_QPS_INIT;
  verbs_.ibv_modify_qp(
      qp_tag0,
      &init_attr,
      IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);
  init_attr.qp_state = IBV_QPS_RTR;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);
  init_attr.qp_state = IBV_QPS_RTS;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);

  auto qp = verbs_.getQpByNum(baseQpNum);
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  EXPECT_CALL(*congestionController, getAvailableWindowSize(_))
      .WillRepeatedly(Return(1));
  qp->setCongestionController(std::move(congestionController));

  // Create memory for WQEs
  const uint32_t wqeSizeBytes = 1024;
  std::unique_ptr<char[]> payload(new char[wqeSizeBytes]);
  ibv_mr* mr = verbs_.ibv_reg_mr_internal(
      pd_,
      payload.get(),
      wqeSizeBytes,
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_LOCAL_WRITE);

  struct ibv_sge sge = {
      .addr = (uint64_t)payload.get(),
      .length = wqeSizeBytes,
      .lkey = mr->lkey};

  // Test different WQE opcodes on Tag 0
  std::vector<ibv_wr_opcode> opcodes = {
      IBV_WR_SEND,
      IBV_WR_SEND_WITH_IMM,
      IBV_WR_RDMA_WRITE,
      IBV_WR_RDMA_WRITE_WITH_IMM,
      IBV_WR_RDMA_READ};

  uint64_t wr_id = 1000;
  for (auto opcode : opcodes) {
    struct ibv_send_wr send_wr = {
        .wr_id = wr_id++,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = opcode,
        .send_flags = IBV_SEND_SIGNALED,
        .imm_data = 0x12345678};

    if (opcode == IBV_WR_RDMA_WRITE || opcode == IBV_WR_RDMA_WRITE_WITH_IMM ||
        opcode == IBV_WR_RDMA_READ) {
      send_wr.wr.rdma.remote_addr = 0x1000000;
      send_wr.wr.rdma.rkey = 0x1234;
    }

    struct ibv_send_wr* bad_wr = nullptr;
    EXPECT_EQ(verbs_.ibv_post_send(qp_tag0, &send_wr, &bad_wr), 0)
        << "Failed to post WQE with opcode: " << opcode;
  }

  // Test some opcodes on Tag 1
  struct ibv_send_wr send_wr = {
      .wr_id = wr_id++,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED};
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(qp_tag1, &send_wr, &bad_wr), 0);

  // Poll until WQEs have been processed into Tags.
  ASSERT_TRUE(qp->waitForWqesToBePosted(6));

  // Verify WQEs were posted (5 on Tag 0, 1 on Tag 1)
  EXPECT_EQ(qp->getNumPendingWqes(), 6);

  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
}

TEST_F(TaggedVerbsTest, TagRemovalStressTest) {
  // Create many Tags and remove them in different orders
  std::vector<struct ibv_qp*> tag_qps;

  // Create initial QP with Tag 0
  struct ibv_qp* initial_qp = makeQp();
  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(initial_qp->qp_num);
  tag_qps.push_back(initial_qp);

  // Create Tags 1-9
  for (uint8_t tagNum = 1; tagNum <= 9; ++tagNum) {
    struct ibv_qp* tag_qp = addTag(baseQpNum, tagNum);
    EXPECT_NE(tag_qp, nullptr);
    tag_qps.push_back(tag_qp);
  }

  auto qp = verbs_.getQpByNum(baseQpNum);
  EXPECT_EQ(qp->numTags(), 10);

  // Destroy Tags in non-sequential order. Each destroy marks a Tag but leaves
  // it in the table.
  std::vector<int> removal_order = {5, 2, 8, 1, 9, 3, 7, 0, 6, 4};

  for (int i = 0; i < 9; ++i) { // Destroy 9 out of 10 Tags
    int tag_index = removal_order[i];
    EXPECT_EQ(verbs_.ibv_destroy_qp(tag_qps[tag_index]), 0)
        << "precondition: destroying the Tag must succeed, or the count read "
           "below is of a table nothing was taken from, for tag "
        << tag_index;
    tag_qps[tag_index] = nullptr;

    qp = verbs_.getQpByNum(baseQpNum);
    EXPECT_NE(qp, nullptr)
        << "the QP must stay live until all its Tags are destroyed";
    EXPECT_EQ(qp->numTags(), 10)
        << "a destroyed Tag must stay in the table until the whole QP is torn "
           "down";
  }

  int last_tag_index = removal_order[9];
  EXPECT_EQ(verbs_.ibv_destroy_qp(tag_qps[last_tag_index]), 0)
      << "precondition: destroying the last Tag must succeed, or the QP read "
         "below is still live because that Tag is";

  qp = verbs_.getQpByNum(baseQpNum);
  EXPECT_EQ(qp, nullptr)
      << "destroying the last live Tag must tear the whole QP down";
}

TEST_F(TaggedVerbsTest, TagReceiveWqePosting) {
  // Create QP with multiple Tags
  struct ibv_qp* qp_tag0 = makeQp();
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
  };

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  struct ibv_qp* qp_tag1 = addTag(baseQpNum, 1);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0);
  };

  // Transition to INIT state first
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  struct ibv_qp_attr init_attr{};
  memcpy(init_attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  init_attr.dest_qp_num = 10;
  init_attr.path_mtu = IBV_MTU_4096;
  init_attr.qp_state = IBV_QPS_INIT;
  verbs_.ibv_modify_qp(
      qp_tag0,
      &init_attr,
      IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);

  // Now setup both Tags to RTS
  init_attr.qp_state = IBV_QPS_RTR;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);
  init_attr.qp_state = IBV_QPS_RTS;
  verbs_.ibv_modify_qp(qp_tag0, &init_attr, IBV_QP_STATE);

  // Create memory for receive buffers
  const uint32_t bufferSize = 1024;
  std::unique_ptr<char[]> buffer0(new char[bufferSize]);
  std::unique_ptr<char[]> buffer1(new char[bufferSize]);
  std::unique_ptr<char[]> buffer2(new char[0]);

  ibv_mr* mr0 = verbs_.ibv_reg_mr_internal(
      pd_,
      buffer0.get(),
      bufferSize,
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_LOCAL_WRITE);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_dereg_mr(mr0), 0);
  };
  ibv_mr* mr1 = verbs_.ibv_reg_mr_internal(
      pd_,
      buffer1.get(),
      bufferSize,
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_LOCAL_WRITE);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_dereg_mr(mr1), 0);
  };
  ibv_mr* mr2 = verbs_.ibv_reg_mr_internal(
      pd_,
      buffer2.get(),
      bufferSize,
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_LOCAL_WRITE);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_dereg_mr(mr2), 0);
  };

  // Post receive WQEs on different Tags
  struct ibv_sge sge0 = {
      .addr = (uint64_t)buffer0.get(), .length = bufferSize, .lkey = mr0->lkey};
  struct ibv_recv_wr recv_wr0 = {.wr_id = 2000, .sg_list = &sge0, .num_sge = 1};
  struct ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_recv(qp_tag0, &recv_wr0, &bad_wr), 0);

  struct ibv_sge sge1 = {
      .addr = (uint64_t)buffer1.get(), .length = bufferSize, .lkey = mr1->lkey};
  struct ibv_recv_wr recv_wr1 = {.wr_id = 3000, .sg_list = &sge1, .num_sge = 1};
  EXPECT_EQ(verbs_.ibv_post_recv(qp_tag1, &recv_wr1, &bad_wr), 0);

  auto qp = verbs_.getQpByNum(baseQpNum);

  // Poll until WQEs have been processed into Tags.
  ASSERT_TRUE(qp->waitForRWqesToBePosted(2));

  // Verify both receive WQEs are posted to the same underlying QP
  EXPECT_EQ(qp->getNumPendingRWqes(), 2);
  EXPECT_EQ(qp->getNumPendingWqes(), 0);

  // Test posting receive with zero-length buffer
  struct ibv_sge sge_empty = {
      .addr = (uint64_t)buffer2.get(), .length = 0, .lkey = mr2->lkey};
  struct ibv_recv_wr recv_wr_empty = {
      .wr_id = 4000, .sg_list = &sge_empty, .num_sge = 1};
  EXPECT_EQ(verbs_.ibv_post_recv(qp_tag0, &recv_wr_empty, &bad_wr), 0);

  // Poll until WQEs have been processed into Tags.
  ASSERT_TRUE(qp->waitForRWqesToBePosted(3));
}

TEST_F(TaggedVerbsTest, TagMixedQpTypesAndInvalidConfigurations) {
  // Create initial RC QP
  struct ibv_qp_init_attr qp_init_attr{};
  qp_init_attr.send_cq = cq_;
  qp_init_attr.recv_cq = cq_;
  qp_init_attr.qp_type = IBV_QPT_RC;
  struct ibv_qp* rc_qp = verbs_.ibv_create_qp(pd_, &qp_init_attr);

  uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(rc_qp->qp_num);

  // Test: Try to create Tag with wrong QP type (not RC)
  struct ibv_qp_init_attr_ex qp_init_attr_ex{};
  qp_init_attr_ex.send_cq = cq_;
  qp_init_attr_ex.recv_cq = cq_;
  qp_init_attr_ex.qp_type = IBV_QPT_UD; // Wrong type
  qp_init_attr_ex.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  qp_init_attr_ex.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  qp_init_attr_ex.pd = pd_;

  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex), nullptr)
      << "lsmr builds only RC queue pairs, so a UD create neither joins the RC "
         "queue pair as a tag nor gets a queue pair of its own";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  // Test: Try to create Tag without IBV_QP_CREATE_SOURCE_QPN flag
  qp_init_attr_ex.qp_type = IBV_QPT_RC;
  qp_init_attr_ex.create_flags = 0; // No SOURCE_QPN flag
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 2);

  // Should fall back to creating a new QP
  struct ibv_qp* no_flag_qp =
      verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);
  EXPECT_NE(no_flag_qp, nullptr);

  uint32_t noFlagQpNum = MetaRoceVerbs::qpAndTagToQp(no_flag_qp->qp_num);
  EXPECT_NE(noFlagQpNum, baseQpNum);

  // Test: Try to create Tag without comp_mask
  qp_init_attr_ex.comp_mask = 0; // No comp_mask
  qp_init_attr_ex.create_flags = IBV_QP_CREATE_SOURCE_QPN;

  // Should fall back to creating a new QP
  struct ibv_qp* no_mask_qp =
      verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);
  EXPECT_NE(no_mask_qp, nullptr);

  uint32_t noMaskQpNum = MetaRoceVerbs::qpAndTagToQp(no_mask_qp->qp_num);
  EXPECT_NE(noMaskQpNum, baseQpNum);

  // Test: Create a valid Tag on the RC QP
  qp_init_attr_ex.qp_type = IBV_QPT_RC;
  qp_init_attr_ex.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  qp_init_attr_ex.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  qp_init_attr_ex.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);

  struct ibv_qp* valid_tag_qp =
      verbs_.ibv_create_qp_ex(&context_, &qp_init_attr_ex);
  EXPECT_NE(valid_tag_qp, nullptr);
  EXPECT_EQ(MetaRoceVerbs::qpAndTagToQp(valid_tag_qp->qp_num), baseQpNum);
  EXPECT_EQ(MetaRoceVerbs::qpAndTagToTag(valid_tag_qp->qp_num), 1);

  // Verify RC QP now has 2 Tags
  auto qp = verbs_.getQpByNum(baseQpNum);
  EXPECT_EQ(qp->numTags(), 2);

  EXPECT_EQ(verbs_.ibv_destroy_qp(valid_tag_qp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(no_mask_qp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(no_flag_qp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(rc_qp), 0);
}

TEST_F(TaggedVerbsTest, EachTagTakesItsCdmsnFromItsOwnRqPsn) {
  // Helper to create a QP with 2 tags
  auto createQpWithTags = [&]() -> std::tuple<ibv_qp*, ibv_qp*, uint32_t> {
    struct ibv_qp* qp_tag0 = makeQp();

    uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

    struct ibv_qp* qp_tag1 = addTag(baseQpNum, 1);

    return {qp_tag0, qp_tag1, baseQpNum};
  };

  // Scenario 1: neither tag has been given an rq_psn. The warning each read
  // logs is not checked here; what is checked is the value it answers with.
  {
    auto [qp_tag0, qp_tag1, baseQpNum] = createQpWithTags();
    auto qp = verbs_.getQpByNum(baseQpNum);
    auto tag0 = qp->getTag(kMetaRoceInitialTag);
    auto tag1 = qp->getTag(1);

    const auto initialCdmsn =
        static_cast<uint16_t>(FLAGS_libsoftmetaroce_initialmsn - 1);

    EXPECT_EQ(tag0->getCDMSN(), initialCdmsn)
        << "a tag never given an rq_psn must answer with the msn before the "
           "first one it expects, or it acks inbound requests it never saw";
    EXPECT_EQ(tag0->getCDMSN(), initialCdmsn)
        << "reading reports the value without configuring the tag, so the "
           "second read must answer as the first did";
    EXPECT_EQ(tag1->getCDMSN(), initialCdmsn)
        << "each tag carries its own cdmsn, so reading tag 0 must leave tag 1 "
           "unconfigured and answering the same";

    EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0);
    EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
  }

  // Scenario 2: getCDMSN called after an rq_psn configured it (should not
  // warn)
  {
    auto [qp_tag0, qp_tag1, baseQpNum] = createQpWithTags();
    auto qp = verbs_.getQpByNum(baseQpNum);
    auto tag0 = qp->getTag(kMetaRoceInitialTag);
    auto tag1 = qp->getTag(1);

    bool oldIgnoreModifyPsn = FLAGS_libsoftmetaroce_ignore_modify_psn;
    FLAGS_libsoftmetaroce_ignore_modify_psn = false;

    // Configure RQ_PSN for tag0, which sets its cdmsn
    struct ibv_qp_attr attr{};
    attr.rq_psn = 0x12345678;
    verbs_.ibv_modify_qp(qp_tag0, &attr, IBV_QP_RQ_PSN);

    EXPECT_EQ(tag0->getCDMSN(), 0x5677)
        << "the msn is the low sixteen bits of rq_psn, and the cdmsn is the "
           "one before it, so 0x12345678 configures 0x5677";
    EXPECT_EQ(tag0->getCDMSN(), 0x5677)
        << "a configured tag answers from what it was given, so a second read "
           "must not move it";

    // Configure RQ_PSN for tag1
    attr.rq_psn = 0xAABBCCDD;
    verbs_.ibv_modify_qp(qp_tag1, &attr, IBV_QP_RQ_PSN);

    EXPECT_EQ(tag1->getCDMSN(), 0xCCDC)
        << "rq_psn reaches one ibv_qp, which is one tag, so tag 1 must take "
           "the value its own modify carried rather than tag 0's";

    FLAGS_libsoftmetaroce_ignore_modify_psn = oldIgnoreModifyPsn;

    EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0);
    EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
  }
}

// Brings every Tag of the QP behind ibvqp to RTS so sends may be posted on it.
static void bringTagToRts(
    libsoftmetaroce::MetaRoceVerbs& verbs,
    struct ibv_qp* ibvqp) {
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  struct ibv_qp_attr attr{};
  memcpy(attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  attr.dest_qp_num = MetaRoceVerbs::combineQpAndTag(10, kMetaRoceInitialTag);
  attr.path_mtu = IBV_MTU_4096;
  attr.qp_state = IBV_QPS_INIT;
  verbs.ibv_modify_qp(
      ibvqp,
      &attr,
      IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);
  attr.qp_state = IBV_QPS_RTR;
  verbs.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE);
  attr.qp_state = IBV_QPS_RTS;
  verbs.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE);
}

// Reports the inline-data cap ibv_query_qp gives for this Tag.
static uint32_t queriedInlineCap(
    libsoftmetaroce::MetaRoceVerbs& verbs,
    struct ibv_qp* ibvqp) {
  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr initAttr{};
  verbs.ibv_query_qp(ibvqp, &attr, IBV_QP_CAP, &initAttr);
  return attr.cap.max_inline_data;
}

/*
 * Each Tag is created by its own verbs call and negotiates its own inline cap,
 * so one Tag's grant does not constrain another's on the same QP.
 */
TEST_F(TaggedVerbsTest, EachTagNegotiatesItsOwnInlineCap) {
  constexpr uint32_t kParentCap = 256;
  constexpr uint32_t kChildCap = 64;

  struct ibv_qp_init_attr initAttr{};
  initAttr.send_cq = cq_;
  initAttr.recv_cq = cq_;
  initAttr.cap.max_inline_data = kParentCap;
  struct ibv_qp* parent = verbs_.ibv_create_qp(pd_, &initAttr);
  ASSERT_NE(parent, nullptr);

  const uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(parent->qp_num);
  struct ibv_qp_init_attr_ex exAttr{};
  exAttr.send_cq = cq_;
  exAttr.recv_cq = cq_;
  exAttr.qp_type = IBV_QPT_RC;
  exAttr.pd = pd_;
  exAttr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  exAttr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  exAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  exAttr.cap.max_inline_data = kChildCap;
  struct ibv_qp* child = verbs_.ibv_create_qp_ex(&context_, &exAttr);
  ASSERT_NE(child, nullptr);

  EXPECT_EQ(queriedInlineCap(verbs_, parent), kParentCap)
      << "a Tag must report the cap its own creation call was granted";
  EXPECT_EQ(queriedInlineCap(verbs_, child), kChildCap)
      << "the second Tag asked for less and must be told so, rather than "
         "inheriting what the first Tag on the QP was granted";

  EXPECT_EQ(verbs_.ibv_destroy_qp(child), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(parent), 0);
}

/*
 * The cap that bounds a post is the one belonging to the Tag it is addressed
 * to, so the same payload can be legal on one Tag of a QP and not on another.
 */
TEST_F(TaggedVerbsTest, AnInlinePostIsBoundedByTheTagItAddresses) {
  constexpr uint32_t kParentCap = 256;
  constexpr uint32_t kChildCap = 64;
  // Fits the parent's grant and exceeds the child's, so only the Tag decides.
  constexpr uint32_t kPayloadBytes = 128;

  struct ibv_qp_init_attr initAttr{};
  initAttr.send_cq = cq_;
  initAttr.recv_cq = cq_;
  initAttr.cap.max_inline_data = kParentCap;
  struct ibv_qp* parent = verbs_.ibv_create_qp(pd_, &initAttr);
  ASSERT_NE(parent, nullptr);

  const uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(parent->qp_num);
  struct ibv_qp_init_attr_ex exAttr{};
  exAttr.send_cq = cq_;
  exAttr.recv_cq = cq_;
  exAttr.qp_type = IBV_QPT_RC;
  exAttr.pd = pd_;
  exAttr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  exAttr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  exAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  exAttr.cap.max_inline_data = kChildCap;
  struct ibv_qp* child = verbs_.ibv_create_qp_ex(&context_, &exAttr);
  ASSERT_NE(child, nullptr);

  bringTagToRts(verbs_, parent);

  auto* qp = verbs_.getQpByNum(baseQpNum);
  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  EXPECT_CALL(*congestionController, getAvailableWindowSize(_))
      .WillRepeatedly(Return(1));
  qp->setCongestionController(std::move(congestionController));

  std::array<char, kPayloadBytes> payload{};
  struct ibv_sge sge{};
  sge.addr = reinterpret_cast<uint64_t>(payload.data());
  sge.length = kPayloadBytes;
  // Deliberately never registered: inline data is copied at post time, so the
  // local key is not consulted and an unregistered source is legal.
  sge.lkey = 0xbadbad;

  struct ibv_send_wr wr{};
  wr.wr_id = 1;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
  wr.num_sge = 1;
  wr.sg_list = &sge;
  struct ibv_send_wr* badWr = nullptr;

  EXPECT_EQ(verbs_.ibv_post_send(parent, &wr, &badWr), 0)
      << "a payload within this Tag's grant must be accepted, and an "
         "unregistered source must not stand in the way of an inline payload";

  wr.wr_id = 2;
  badWr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(child, &wr, &badWr), EINVAL)
      << "the same payload exceeds the grant this Tag negotiated, and the cap "
         "that applies is the one of the Tag addressed";
  EXPECT_EQ(badWr, &wr)
      << "a rejected post must identify the work request that failed";

  EXPECT_EQ(verbs_.ibv_destroy_qp(child), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(parent), 0);
}

/*
 * `ibv_query_qp` reports one scatter-gather entry per work request, so a
 * caller asking for more must be turned away where it asks. Granted silently,
 * the mismatch surfaces at the first post, out of a C API that has no way to
 * report it.
 */
TEST_F(TaggedVerbsTest, AQpAskingForMoreSgesThanSupportedIsRefused) {
  struct ibv_device_attr deviceAttr{};
  ASSERT_EQ(verbs_.ibv_query_device(&context_, &deviceAttr), 0);
  EXPECT_EQ(deviceAttr.max_sge, kMaxSgesPerWorkRequest)
      << "the device advertises the bound create_qp enforces below; a caller "
         "that sizes its work requests off this number would otherwise be told "
         "a different limit than the one it is held to";

  struct ibv_qp_init_attr initAttr{};
  initAttr.send_cq = cq_;
  initAttr.recv_cq = cq_;
  initAttr.cap.max_send_sge = kMaxSgesPerWorkRequest + 1;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp(pd_, &initAttr), nullptr)
      << "a send list longer than one entry is not something this provider can "
         "grant, and reporting one while accepting the request would be a lie";
  EXPECT_EQ(errno, EINVAL) << "create_qp reports its refusal through errno";

  initAttr.cap.max_send_sge = kMaxSgesPerWorkRequest;
  initAttr.cap.max_recv_sge = kMaxSgesPerWorkRequest + 1;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp(pd_, &initAttr), nullptr)
      << "the receive list is bounded the same way";
  EXPECT_EQ(errno, EINVAL) << "create_qp reports its refusal through errno";

  // What the provider does support, and what an untouched attr asks for.
  initAttr.cap.max_recv_sge = kMaxSgesPerWorkRequest;
  struct ibv_qp* granted = verbs_.ibv_create_qp(pd_, &initAttr);
  ASSERT_NE(granted, nullptr)
      << "one entry is what the provider reports, so asking for it must "
         "succeed";

  struct ibv_qp_init_attr_ex exAttr{};
  exAttr.send_cq = cq_;
  exAttr.recv_cq = cq_;
  exAttr.qp_type = IBV_QPT_RC;
  exAttr.pd = pd_;
  exAttr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  exAttr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  exAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(
      MetaRoceVerbs::qpAndTagToQp(granted->qp_num), 1);
  exAttr.cap.max_send_sge = kMaxSgesPerWorkRequest + 1;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, &exAttr), nullptr)
      << "a Tag added to an existing QP is bounded the same way";
  EXPECT_EQ(errno, EINVAL) << "create_qp_ex reports its refusal through errno";

  EXPECT_EQ(verbs_.ibv_destroy_qp(granted), 0);
}

/*
 * Zero is the grant a QP that never asks for inline receives, so it is what
 * every existing caller holds. A Tag granted nothing must refuse a payload of
 * any size: accepting one would copy into a WQE the caller was never told it
 * could stop owning.
 */
TEST_F(TaggedVerbsTest, ATagGrantedNoInlineDataRefusesAnInlinePost) {
  struct ibv_qp_init_attr initAttr{};
  initAttr.send_cq = cq_;
  initAttr.recv_cq = cq_;
  // Left unset, as a caller with no interest in inline leaves it.
  struct ibv_qp* qp = verbs_.ibv_create_qp(pd_, &initAttr);
  ASSERT_NE(qp, nullptr);
  ASSERT_EQ(queriedInlineCap(verbs_, qp), 0u)
      << "precondition: an unasked-for cap is zero, which is the grant under "
         "test";

  bringTagToRts(verbs_, qp);
  ASSERT_EQ(qp->state, IBV_QPS_RTS)
      << "precondition: the Tag is sendable, or the refusal below is a state "
         "error and says nothing about the cap";

  char payload = 'x';
  struct ibv_sge sge{};
  sge.addr = reinterpret_cast<uint64_t>(&payload);
  sge.length = sizeof(payload);
  sge.lkey = 0xbadbad;

  struct ibv_send_wr wr{};
  wr.wr_id = 1;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
  wr.num_sge = 1;
  wr.sg_list = &sge;
  struct ibv_send_wr* badWr = nullptr;

  EXPECT_EQ(verbs_.ibv_post_send(qp, &wr, &badWr), EINVAL)
      << "one byte is over a grant of none; treating zero as unset would let "
         "an inline post through on every queue pair that never asked";
  EXPECT_EQ(badWr, &wr)
      << "a rejected post must identify the work request that failed";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
}

/*
 * The device maximum bounds what any Tag may be granted, on whichever call
 * creates it.
 */
TEST_F(TaggedVerbsTest, AnInlineCapOverTheDeviceMaximumIsRefused) {
  constexpr uint32_t kOverDeviceMaximum = kMaxInlineDataBytes + 1;

  struct ibv_qp_init_attr initAttr{};
  initAttr.send_cq = cq_;
  initAttr.recv_cq = cq_;
  initAttr.cap.max_inline_data = kOverDeviceMaximum;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp(pd_, &initAttr), nullptr)
      << "a QP asking for more inline data than the device offers must be "
         "refused rather than silently granted less";
  EXPECT_EQ(errno, EINVAL) << "create_qp reports its refusal through errno";

  initAttr.cap.max_inline_data = 0;
  struct ibv_qp* parent = verbs_.ibv_create_qp(pd_, &initAttr);
  ASSERT_NE(parent, nullptr);

  struct ibv_qp_init_attr_ex exAttr{};
  exAttr.send_cq = cq_;
  exAttr.recv_cq = cq_;
  exAttr.qp_type = IBV_QPT_RC;
  exAttr.pd = pd_;
  exAttr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  exAttr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  exAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(
      MetaRoceVerbs::qpAndTagToQp(parent->qp_num), 1);
  exAttr.cap.max_inline_data = kOverDeviceMaximum;
  // Cleared again: the refusal above already left EINVAL set, so without this
  // the check below would pass even if create_qp_ex left errno untouched.
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, &exAttr), nullptr)
      << "a Tag added to an existing QP is bounded by the same device maximum";
  EXPECT_EQ(errno, EINVAL) << "create_qp_ex reports its refusal through errno";

  EXPECT_EQ(verbs_.ibv_destroy_qp(parent), 0);
}

// Reports the rnr_retry limit ibv_query_qp gives for this Tag.
static uint8_t queriedRnrRetry(
    libsoftmetaroce::MetaRoceVerbs& verbs,
    struct ibv_qp* ibvqp) {
  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr initAttr{};
  verbs.ibv_query_qp(ibvqp, &attr, IBV_QP_RNR_RETRY, &initAttr);
  return attr.rnr_retry;
}

/*
 * rnr_retry arrives through ibv_modify_qp, which addresses one ibv_qp and so
 * one Tag. Two tags of the same QP can be modified separately and must keep
 * the allowance each was given.
 */
TEST_F(TaggedVerbsTest, EachTagKeepsItsOwnRnrRetryLimit) {
  constexpr uint8_t kParentRetries = 2;
  constexpr uint8_t kChildRetries = 5;

  struct ibv_qp* parent = makeQp();
  ASSERT_NE(parent, nullptr);

  struct ibv_qp* child = addTag(MetaRoceVerbs::qpAndTagToQp(parent->qp_num), 1);
  ASSERT_NE(child, nullptr);

  struct ibv_qp_attr attr{};
  attr.rnr_retry = kParentRetries;
  verbs_.ibv_modify_qp(parent, &attr, IBV_QP_RNR_RETRY);
  attr.rnr_retry = kChildRetries;
  verbs_.ibv_modify_qp(child, &attr, IBV_QP_RNR_RETRY);

  EXPECT_EQ(queriedRnrRetry(verbs_, parent), kParentRetries)
      << "a Tag must keep the allowance its own ibv_qp was modified with";
  EXPECT_EQ(queriedRnrRetry(verbs_, child), kChildRetries)
      << "the second Tag was modified after the first, so a limit shared "
         "across the QP would report its value for both";

  EXPECT_EQ(verbs_.ibv_destroy_qp(child), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(parent), 0);
}

// Reports the local ack timeout and retry count ibv_query_qp gives this Tag.
static std::pair<uint8_t, uint8_t> queriedTimeoutAndRetryCount(
    libsoftmetaroce::MetaRoceVerbs& verbs,
    struct ibv_qp* ibvqp) {
  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr initAttr{};
  verbs.ibv_query_qp(
      ibvqp, &attr, IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT, &initAttr);
  return {attr.timeout, attr.retry_cnt};
}

TEST_F(TaggedVerbsTest, ATimeoutAndRetryCountAreReportedBack) {
  constexpr uint8_t kTimeoutCode = 14; // 4.096us * 2^14 = 67.108ms
  constexpr uint8_t kRetryCount = 2;

  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);

  struct ibv_qp_attr attr{};
  attr.timeout = kTimeoutCode;
  attr.retry_cnt = kRetryCount;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(qp, &attr, IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT), 0)
      << "precondition: the modify has to be accepted, or the query below "
         "reads back a Tag nothing was ever set on";

  EXPECT_EQ(
      queriedTimeoutAndRetryCount(verbs_, qp),
      std::make_pair(kTimeoutCode, kRetryCount))
      << "a client that set these has no other way to read back what the QP "
         "is holding for it";

  verbs_.ibv_destroy_qp(qp);
}

/*
 * ibv_qp_attr::timeout holds an exponent in five bits, so a larger value
 * describes no wait the client could have meant. rxe refuses the same value
 * rather than interpreting it.
 */
TEST_F(TaggedVerbsTest, ATimeoutPastTheIbtaFieldIsRefused) {
  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);

  struct ibv_qp_attr attr{};
  attr.timeout = kMaxLocalAckTimeoutExponent + 1;
  EXPECT_EQ(verbs_.ibv_modify_qp(qp, &attr, IBV_QP_TIMEOUT), EINVAL)
      << "accepting it would hand the QP a deadline derived from a number the "
         "field cannot carry, and hide the client's bug behind it";

  verbs_.ibv_destroy_qp(qp);
}

/*
 * Every Tag's configuration is frozen before the QP carries traffic, so these
 * are refused on a running QP as rnr_retry is rather than changing a deadline
 * under it.
 */
TEST_F(TaggedVerbsTest, TimeoutCannotBeChangedOnARunningQp) {
  constexpr uint8_t kBringUpCode = 14;

  struct ibv_qp* ibvqp = makeQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_qp_attr attr{};
  attr.timeout = kBringUpCode;
  attr.retry_cnt = 2;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_TIMEOUT), 0)
      << "the deadline must be settable before the QP is running";

  bringTagToRts(verbs_, ibvqp);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the refusal below is about the running state, so the "
         "QP has to have reached it";

  attr.timeout = 10;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_TIMEOUT), EINVAL)
      << "a deadline that could move under a running QP would leave the tags' "
         "configuration something the timers cannot treat as frozen";
  EXPECT_EQ(queriedTimeoutAndRetryCount(verbs_, ibvqp).first, kBringUpCode)
      << "a refused modify must leave the value it was given at bring-up";

  verbs_.ibv_destroy_qp(ibvqp);
}

/*
 * The BRNR timer reads the allowance without taking a lock, which is only
 * sound because the value is frozen before traffic can start. Setting it on a
 * running QP is refused rather than raced.
 */
TEST_F(TaggedVerbsTest, RnrRetryCannotBeChangedOnARunningQp) {
  constexpr uint8_t kBringUpRetries = 3;
  struct ibv_qp* ibvqp = makeQp();
  ASSERT_NE(ibvqp, nullptr);

  // Set on the way up, which is where the attribute belongs.
  struct ibv_qp_attr attr{};
  attr.rnr_retry = kBringUpRetries;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_RNR_RETRY), 0)
      << "the allowance must be settable before the QP is running";

  bringTagToRts(verbs_, ibvqp);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "the refusal below is about the running state, so the QP has to have "
         "reached it";

  attr.rnr_retry = 1;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_RNR_RETRY), EINVAL)
      << "changing the allowance under a running QP would race the BRNR timer "
         "that reads it unsynchronized";
  EXPECT_EQ(queriedRnrRetry(verbs_, ibvqp), kBringUpRetries)
      << "a refused modify must leave the value it was given at bring-up";

  // A tag entering error leaves RTS without clearing its BRNR
  // state, so the timer keeps reading the allowance for it.
  verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num))
      ->setIbvQpState(IBV_QPS_ERR);
  ASSERT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "the refusal below is about the error state, so the QP has to be in "
         "it";

  attr.rnr_retry = 1;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_RNR_RETRY), EINVAL)
      << "a QP that has run and failed must stay closed to the attribute; the "
         "timer is still reading it";
  EXPECT_EQ(queriedRnrRetry(verbs_, ibvqp), kBringUpRetries)
      << "a refused modify must leave the value it was given at bring-up";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(TaggedVerbsTest, ATagTakesTheSendDepthItsOwnCreateAsksFor) {
  struct ibv_qp* qp_tag0 = makeQp();
  ASSERT_NE(qp_tag0, nullptr);
  const uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp_tag0->qp_num);

  struct ibv_qp_init_attr_ex exAttr{};
  exAttr.send_cq = cq_;
  exAttr.recv_cq = cq_;
  exAttr.qp_type = IBV_QPT_RC;
  exAttr.pd = pd_;
  exAttr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  exAttr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  exAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  exAttr.cap.max_send_wr = 5;
  struct ibv_qp* qp_tag1 = verbs_.ibv_create_qp_ex(&context_, &exAttr);
  ASSERT_NE(qp_tag1, nullptr);

  EXPECT_EQ(exAttr.cap.max_send_wr, 5u)
      << "the tag's own create answers on the attributes it was given, as the "
         "queue pair's create does";

  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr queried{};
  ASSERT_EQ(verbs_.ibv_query_qp(qp_tag1, &attr, IBV_QP_CAP, &queried), 0)
      << "precondition: the query answers, or the cap read below is whatever "
         "the attributes held before it";
  EXPECT_EQ(attr.cap.max_send_wr, 5u)
      << "a tag negotiates its own depth, so it reports what its create asked "
         "for rather than the depth the queue pair it joined uses";

  struct ibv_qp_attr tag0Attr{};
  ASSERT_EQ(verbs_.ibv_query_qp(qp_tag0, &tag0Attr, IBV_QP_CAP, &queried), 0)
      << "precondition: the query answers for the first tag too";
  EXPECT_EQ(tag0Attr.cap.max_send_wr, kDefaultQpWorkRequests)
      << "and the tag it joined keeps the depth its own create asked for";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag1), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp_tag0), 0);
}

TEST_F(TaggedVerbsTest, ATagTakesTheDomainOfTheQueuePairItJoins) {
  struct ibv_pd* otherPd = verbs_.ibv_alloc_pd(&context_);
  ASSERT_NE(otherPd, nullptr);

  struct ibv_qp* parent = makeQp();
  ASSERT_NE(parent, nullptr);
  const uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(parent->qp_num);

  struct ibv_qp_init_attr_ex exAttr{};
  exAttr.send_cq = cq_;
  exAttr.recv_cq = cq_;
  exAttr.qp_type = IBV_QPT_RC;
  exAttr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  exAttr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  exAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);

  // The field is unset as far as comp_mask is concerned, so the tag takes the
  // domain of the queue pair it joins rather than whatever the field holds.
  exAttr.pd = nullptr;
  struct ibv_qp* tag = verbs_.ibv_create_qp_ex(&context_, &exAttr);
  ASSERT_NE(tag, nullptr)
      << "a tag must be created without the caller specifying a domain, "
         "since the queue pair it joins already has one";
  EXPECT_EQ(tag->pd, pd_)
      << "and it must be that queue pair's domain, which is what its sends "
         "are validated against";

  exAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 2);
  exAttr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS | IBV_QP_INIT_ATTR_PD;
  exAttr.pd = otherPd;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, &exAttr), nullptr)
      << "a caller that specifies a domain must specify the one the queue "
         "pair holds; tags of one pair cannot span domains";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  exAttr.pd = pd_;
  struct ibv_qp* agreeing = verbs_.ibv_create_qp_ex(&context_, &exAttr);
  EXPECT_NE(agreeing, nullptr)
      << "specifying the same domain the queue pair holds must be accepted";

  EXPECT_EQ(verbs_.ibv_destroy_qp(agreeing), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(tag), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(parent), 0);
  EXPECT_EQ(verbs_.ibv_dealloc_pd(otherPd), 0);
}

// Bundles sl with a usable address into an address-vector update; the
// success-path modifies need the address accepted to pass.
static struct ibv_qp_attr makeAvAttrWithSl(
    uint8_t sl,
    const char* dgid = "fc00::1") {
  struct ibv_qp_attr attr{};
  const auto remoteAddr = folly::IPAddressV6(dgid);
  memcpy(attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  attr.ah_attr.sl = sl;
  return attr;
}

// Reports the service level ibv_query_qp gives this Tag.
static uint8_t queriedServiceLevel(
    libsoftmetaroce::MetaRoceVerbs& verbs,
    struct ibv_qp* ibvqp) {
  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr initAttr{};
  verbs.ibv_query_qp(ibvqp, &attr, IBV_QP_AV, &initAttr);
  return attr.ah_attr.sl;
}

TEST_F(TaggedVerbsTest, ServiceLevelDefaultsToZero) {
  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);

  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr initAttr{};
  // Mark the queried fields, so the assertions below prove what the query
  // wrote rather than what zero-initialization left.
  attr.ah_attr.sl = 0xFF;
  memset(attr.ah_attr.grh.dgid.raw, 0xAB, sizeof(attr.ah_attr.grh.dgid.raw));
  ASSERT_EQ(verbs_.ibv_query_qp(qp, &attr, IBV_QP_AV, &initAttr), 0)
      << "precondition: the query has to succeed for the assertions below";
  EXPECT_EQ(attr.ah_attr.sl, 0)
      << "the marked service level must read back zero, proving the query "
         "wrote it";
  uint8_t zeroGid[sizeof(attr.ah_attr.grh.dgid.raw)] = {};
  EXPECT_EQ(memcmp(attr.ah_attr.grh.dgid.raw, zeroGid, sizeof(zeroGid)), 0)
      << "the query clears the address fields it does not report, so a "
         "caller never reads its own buffer contents as an address";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
}

TEST_F(TaggedVerbsTest, ServiceLevelConfiguresTagZero) {
  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);

  struct ibv_qp_attr attr = makeAvAttrWithSl(11);
  ASSERT_EQ(verbs_.ibv_modify_qp(qp, &attr, IBV_QP_AV), 0)
      << "precondition: the modify has to be accepted, or the query below "
         "reads back a tag nothing was ever set on";
  EXPECT_EQ(queriedServiceLevel(verbs_, qp), 11)
      << "tag zero is addressed by the queue pair's own handle, so its "
         "service level configures through that handle like any other tag";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
}

TEST_F(TaggedVerbsTest, EachTagKeepsItsOwnServiceLevel) {
  constexpr uint8_t kParentSl = 3;
  constexpr uint8_t kChildSl = 9;

  struct ibv_qp* parent = makeQp();
  ASSERT_NE(parent, nullptr);

  struct ibv_qp* child = addTag(MetaRoceVerbs::qpAndTagToQp(parent->qp_num), 1);
  ASSERT_NE(child, nullptr);

  struct ibv_qp_attr attr = makeAvAttrWithSl(kParentSl);
  ASSERT_EQ(verbs_.ibv_modify_qp(parent, &attr, IBV_QP_AV), 0)
      << "precondition: the parent modify has to be accepted, or the query "
         "below reads back a tag nothing was ever set on";
  attr = makeAvAttrWithSl(kChildSl);
  ASSERT_EQ(verbs_.ibv_modify_qp(child, &attr, IBV_QP_AV), 0)
      << "precondition: the child modify has to be accepted, or the query "
         "below reads back a tag nothing was ever set on";

  EXPECT_EQ(queriedServiceLevel(verbs_, parent), kParentSl)
      << "a tag must keep the service level its own ibv_qp was modified with";
  EXPECT_EQ(queriedServiceLevel(verbs_, child), kChildSl)
      << "the second tag was modified after the first, so a level shared "
         "across the queue pair would report its value for both";

  EXPECT_EQ(verbs_.ibv_destroy_qp(child), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(parent), 0);
}

TEST_F(TaggedVerbsTest, ServiceLevelBoundariesAreAccepted) {
  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);

  struct ibv_qp_attr attr = makeAvAttrWithSl(kMaxServiceLevel);
  EXPECT_EQ(verbs_.ibv_modify_qp(qp, &attr, IBV_QP_AV), 0)
      << "the top of the four-bit field must be usable, or part of the "
         "weight range is unreachable";
  EXPECT_EQ(queriedServiceLevel(verbs_, qp), kMaxServiceLevel)
      << "the boundary value has to read back what the modify carried";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
}

TEST_F(TaggedVerbsTest, ServiceLevelPastTheFieldIsRefused) {
  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);
  const auto baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp->qp_num);

  struct ibv_qp_attr attr = makeAvAttrWithSl(kMaxServiceLevel + 1);
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(qp, &attr, IBV_QP_AV), EINVAL)
      << "a service level the four-bit field cannot carry must be refused "
         "rather than masked into range";
  EXPECT_EQ(errno, EINVAL) << "the refusal has to reach a caller reading "
                              "errno after the return value";
  EXPECT_EQ(queriedServiceLevel(verbs_, qp), 0)
      << "a refused modify must leave the value the tag held before it";
  EXPECT_EQ(verbs_.getQpByNum(baseQpNum)->getNumPortPairs(), 0)
      << "the refusal lands before the address side effects, so no port "
         "pair may be left behind";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
}

TEST_F(TaggedVerbsTest, InvalidAddressVectorInstallsNoServiceLevel) {
  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);
  const auto baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp->qp_num);

  struct ibv_qp_attr attr = makeAvAttrWithSl(5);
  attr.ah_attr.grh.sgid_index = 0xFF;
  EXPECT_EQ(verbs_.ibv_modify_qp(qp, &attr, IBV_QP_AV), EINVAL)
      << "a gid index the test device cannot hold must be refused";
  EXPECT_EQ(queriedServiceLevel(verbs_, qp), 0)
      << "an address vector that fails validation must not install its "
         "service level on the way out";
  EXPECT_EQ(verbs_.getQpByNum(baseQpNum)->getNumPortPairs(), 0)
      << "the failed update must leave no port pair behind either";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
}

TEST_F(TaggedVerbsTest, UnknownTagServiceLevelIsRefused) {
  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);

  // A fabricated handle: the queue pair is real, but the tag is missing.
  struct ibv_qp fabricated = *qp;
  fabricated.qp_num = MetaRoceVerbs::combineQpAndTag(
      MetaRoceVerbs::qpAndTagToQp(qp->qp_num), 7);

  struct ibv_qp_attr attr = makeAvAttrWithSl(5);
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(&fabricated, &attr, IBV_QP_AV), ENOENT)
      << "a tag the queue pair never had is one lsmr looked for and does "
         "not hold";
  EXPECT_EQ(errno, ENOENT)
      << "an int-returning verb sets errno as well as returning it";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
}

TEST_F(TaggedVerbsTest, ServiceLevelCannotChangeOnARunningQp) {
  constexpr uint8_t kBringUpSl = 3;
  struct ibv_qp* ibvqp = makeQp();
  ASSERT_NE(ibvqp, nullptr);

  // Bring-up carries the level through, as the multipath provider does.
  struct ibv_qp_attr attr = makeAvAttrWithSl(kBringUpSl);
  attr.dest_qp_num = MetaRoceVerbs::combineQpAndTag(10, kMetaRoceInitialTag);
  attr.path_mtu = IBV_MTU_4096;
  attr.qp_state = IBV_QPS_INIT;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(
          ibvqp,
          &attr,
          IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE),
      0)
      << "precondition: the bring-up update has to land, or the frozen "
         "value below is one nothing ever set";
  attr.qp_state = IBV_QPS_RTR;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), 0)
      << "precondition: the refusal below is about the running state, so "
         "the queue pair has to have left bring-up";

  attr = makeAvAttrWithSl(9);
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_AV), EINVAL)
      << "the freeze lands at RTR entry, so a changed level in RTR state "
         "must already be refused";
  EXPECT_EQ(queriedServiceLevel(verbs_, ibvqp), kBringUpSl)
      << "the refused RTR-state update must leave the bring-up value";

  attr.qp_state = IBV_QPS_RTS;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), 0)
      << "precondition: the refusal below is about the running state, so "
         "the queue pair has to have reached it";

  attr = makeAvAttrWithSl(7, "fc00::2");
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_AV), EINVAL)
      << "weights freeze when the queue pair starts running, so a changed "
         "level past that point must be refused rather than half adopted";
  EXPECT_EQ(queriedServiceLevel(verbs_, ibvqp), kBringUpSl)
      << "a refused modify must leave the frozen bring-up value";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(TaggedVerbsTest, UnchangedServiceLevelUpdateSucceedsOnARunningQp) {
  struct ibv_qp* ibvqp = makeQp();
  ASSERT_NE(ibvqp, nullptr);
  const auto baseQpNum = MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num);

  bringTagToRts(verbs_, ibvqp);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the update below is about the running state, so the "
         "queue pair has to have reached it";

  struct ibv_qp_attr attr = makeAvAttrWithSl(0, "fc00::2");
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_AV), 0)
      << "repeating the stored level must stay accepted, or multipath "
         "bring-up after the first path could not add more";
  EXPECT_EQ(queriedServiceLevel(verbs_, ibvqp), 0)
      << "the accepted repeat must leave the stored level where it was";
  EXPECT_EQ(verbs_.getQpByNum(baseQpNum)->getNumPortPairs(), 2)
      << "the repeat is an address update like any other, so its new path "
         "lands beside the bring-up one";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(TaggedVerbsTest, RepeatedAddressVectorUpdatesKeepServiceLevel) {
  struct ibv_qp* qp = makeQp();
  ASSERT_NE(qp, nullptr);
  const auto baseQpNum = MetaRoceVerbs::qpAndTagToQp(qp->qp_num);

  struct ibv_qp_attr attr = makeAvAttrWithSl(4);
  ASSERT_EQ(verbs_.ibv_modify_qp(qp, &attr, IBV_QP_AV), 0)
      << "precondition: the first update has to land, or the repeat below "
         "repeats nothing";
  ASSERT_EQ(verbs_.ibv_modify_qp(qp, &attr, IBV_QP_AV), 0)
      << "precondition: multipath bring-up repeats the update per path, so "
         "a repeat must be accepted";
  EXPECT_EQ(queriedServiceLevel(verbs_, qp), 4)
      << "repeating the update must not move the stored level";
  EXPECT_EQ(verbs_.getQpByNum(baseQpNum)->getNumPortPairs(), 1)
      << "the repeated identical path deduplicates rather than accumulating";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
}

} // namespace libsoftmetaroce
