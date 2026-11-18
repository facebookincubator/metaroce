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

#include <libsoftmetaroce/Sender.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/test/LimitedTestSender.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <libsoftmetaroce/test/QpTestBase.h>
#include <tuple>

using namespace testing;
namespace libsoftmetaroce {

static std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr);
}

class QpWithTagsTest : public QpTestBase {};

TEST_F(QpWithTagsTest, AddTags) {
  EXPECT_EQ(qp.numTags(), 1); // Initial tag added in setup

  // Add additional tags
  qp.addTag(1, &cq, ibvQp);
  EXPECT_EQ(qp.numTags(), 2);

  qp.addTag(2, &cq, ibvQp);
  EXPECT_EQ(qp.numTags(), 3);

  // Verify tags can be accessed through QP
  EXPECT_NO_THROW({ (void)initialTag->getNextMsn(); });
  EXPECT_NO_THROW({ (void)qp.getTag(1)->getNextMsn(); });
  EXPECT_NO_THROW({ (void)qp.getTag(2)->getNextMsn(); });
}

TEST_F(QpWithTagsTest, MultiTagMsnCsnManagement) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Test MSN management on different tags
  EXPECT_EQ(initialTag->getNextMsn(), 1);
  EXPECT_EQ(tag1->getNextMsn(), 1);
  EXPECT_EQ(tag2->getNextMsn(), 1);

  // Test increment MSN on each tag independently
  EXPECT_EQ(initialTag->getAndIncrementNextMsn(), 1);
  EXPECT_EQ(initialTag->getNextMsn(), 2);
  EXPECT_EQ(tag1->getNextMsn(), 1); // Other tags unchanged

  EXPECT_EQ(tag1->getAndIncrementNextMsn(), 1);
  EXPECT_EQ(tag1->getNextMsn(), 2);

  // Test CSN management
  EXPECT_EQ(initialTag->getAndIncrementNextCsn(), 0);
  EXPECT_EQ(tag1->getAndIncrementNextCsn(), 0);
  EXPECT_EQ(tag2->getAndIncrementNextCsn(), 0);

  // Test CDMSN management. Only the initial tag is configured in setup; the
  // tags added above have taken no rq_psn of their own.
  EXPECT_EQ(initialTag->getCDMSN(), kFixtureCdmsn);
  EXPECT_EQ(tag1->getCDMSN(), 0);
  EXPECT_EQ(tag2->getCDMSN(), 0);

  constexpr uint16_t kRenumberedCdmsn = 10;
  ASSERT_TRUE(initialTag->setReceiveSequenceNumbers(
      {.nextMsn = kRenumberedCdmsn + 1, .nextCsn = kFixtureRxCsn}))
      << "precondition: the initial tag takes the renumber before RTR, or the "
         "per-tag independence checked below is read off an unchanged cdmsn";
  EXPECT_EQ(initialTag->getCDMSN(), kRenumberedCdmsn);
  EXPECT_EQ(tag1->getCDMSN(), 0); // Other tags unchanged
}

TEST_F(QpWithTagsTest, MultiTagWqeProcessing) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  EXPECT_EQ(qp.getNumPendingWqes(), 0);

  // Create WQEs for different tags
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .tagNum = 0,
       .address = receiveBuffer.data()});
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .tagNum = 1,
       .address = receiveBuffer.data()});
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kReceive,
       .msgSizeBytes = static_cast<uint32_t>(mtu),
       .tagNum = 2,
       .address = receiveBuffer.data()});

  // Should have pending WQEs and RWQEs
  EXPECT_EQ(qp.getNumPendingWqes(), 2);
  EXPECT_EQ(qp.getNumPendingRWqes(), 1);
}

TEST_F(QpWithTagsTest, VirtualWqeCreationWithTags) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Create ReadWriteHeader for testing
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(receiveBuffer.data()), mrKey, 1000);

  int addressError = -1;

  // Test virtual RWqe creation on different tags
  initialTag->createVirtualRWqeIfNeeded(1, rwHeader, &addressError, mrs, mtu);
  EXPECT_EQ(addressError, 0);

  tag1->createVirtualRWqeIfNeeded(2, rwHeader, &addressError, mrs, mtu);
  EXPECT_EQ(addressError, 0);

  // Test virtual SendWqe creation
  tag2->createVirtualSendWqeIfNeeded(3, rwHeader, &addressError, mrs, mtu);
  EXPECT_EQ(addressError, 0);

  // Should have pending RWQEs and WQEs
  EXPECT_EQ(qp.getNumPendingRWqes(), 2);
  EXPECT_EQ(qp.getNumPendingWqes(), 1);
}

TEST_F(QpWithTagsTest, ReceiveWqeAssignmentPerTag) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);
  qp.setIbvQpState(IBV_QPS_RTR);
  qp.setIbvQpState(IBV_QPS_RTS);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Create and post receive WQEs to different tags
  (void)postInSequenceReceiveWqe(qp, receiveBuffer.data(), 1000, 0);
  (void)postInSequenceReceiveWqe(qp, receiveBuffer.data(), 1000, 1);
  (void)postInSequenceReceiveWqe(qp, receiveBuffer.data(), 1000, 2);

  ASSERT_TRUE(qp.waitForRWqesToBePosted(3))
      << "precondition: all three receives must reach their Tags, or the "
         "per-tag assignment below is made against work that is not there yet";

  EXPECT_EQ(qp.getNumPendingRWqes(), 3);

  // Test assigning initiator MSNs to receive WQEs on different tags
  initialTag->assignInitiatorMsnToReceiveWqeIfNeeded(
      0, 100, mtu); // CSN 0, MSN 100
  tag1->assignInitiatorMsnToReceiveWqeIfNeeded(0, 200, mtu); // CSN 0, MSN 200
  tag2->assignInitiatorMsnToReceiveWqeIfNeeded(0, 300, mtu); // CSN 0, MSN 300

  // Test with ReadWriteHeader validation
  ReadWriteHeader rwHeader(
      reinterpret_cast<uint64_t>(receiveBuffer.data()), mrKey, 1000);
  int addressError = -1;

  initialTag->assignInitiatorMsnToReceiveWqeIfNeeded(
      1, 101, rwHeader, &addressError, mrs, mtu);
  EXPECT_EQ(addressError, 0);
}

TEST_F(QpWithTagsTest, CdmsnTrackerExtensionPerTag) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Extend CDMSN tracker for different MSNs on different tags
  initialTag->extendCdmsnTrackerIfNeeded(10);
  tag1->extendCdmsnTrackerIfNeeded(20);
  tag2->extendCdmsnTrackerIfNeeded(30);

  // Should not throw exceptions
  SUCCEED();

  // Test extending with larger MSNs
  initialTag->extendCdmsnTrackerIfNeeded(50);
  tag1->extendCdmsnTrackerIfNeeded(60);
  tag2->extendCdmsnTrackerIfNeeded(70);

  SUCCEED();
}

TEST_F(QpWithTagsTest, MixedWqeTypesWithTags) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  // Create different types of WQEs for different tags
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = 1000,
       .tagNum = 0,
       .address = receiveBuffer.data()});
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kReceive,
       .msgSizeBytes = 1000,
       .tagNum = 1,
       .address = receiveBuffer.data()});
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kRead,
       .msgSizeBytes = 1000,
       .tagNum = 2,
       .address = receiveBuffer.data(),
       .remote_addr = 0x1000,
       .rkey = mrKey});

  // Should have pending WQEs and RWQEs
  EXPECT_EQ(qp.getNumPendingWqes(), 2);
  EXPECT_EQ(qp.getNumPendingRWqes(), 1);
}

TEST_F(QpWithTagsTest, FlowIntegrationWithMultiTags) {
  // Setup paths
  qp.addPortPair(folly::IPAddressV6("fc00::1"), folly::IPAddressV6("fc00::2"));

  LimitedTestSender mocksender(arbiter, mockUDPSocketMaker);
  qp.setupFlows(2, 2, &mocksender);

  EXPECT_EQ(qp.getNumTxFlows(), 2);
  EXPECT_EQ(qp.getNumRxFlows(), 2);

  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  // Create WQEs for different tags
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = 1000,
       .tagNum = 0,
       .address = receiveBuffer.data()});
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = 1000,
       .tagNum = 1,
       .address = receiveBuffer.data()});

  // Should have pending WQEs
  EXPECT_EQ(qp.getNumPendingWqes(), 2);
}

TEST_F(QpWithTagsTest, PostManyWqesOnManyTags) {
  const int numTags = 256;
  // Start at 1 to account for the initial tag
  for (int tagNum = 1; tagNum < numTags; ++tagNum) {
    qp.addTag((uint8_t)tagNum, &cq, ibvQp);
  }
  ASSERT_EQ(qp.numTags(), numTags)
      << "precondition: this covers work spread across every Tag, so one that "
         "failed to register would quietly shrink what it exercises";

  // Post WQEs to different tags
  // Post half send, half receive
  const int numWqesPerTag = 100;
  // Counted rather than derived: an odd numWqesPerTag leaves each Tag with one
  // more receive than send, and half of each would never arrive.
  int numSendWqes = 0;
  int numReceiveWqes = 0;
  for (int tagNum = 0; tagNum < numTags; ++tagNum) {
    for (int i = 0; i < numWqesPerTag; ++i) {
      const bool isSend = i % 2;
      (void)postWqeHelper(
          &qp,
          {.workType = isSend ? Wqe::WorkType::kSend : Wqe::WorkType::kReceive,
           .msgSizeBytes = 1000,
           .tagNum = (uint8_t)tagNum,
           .address = receiveBuffer.data()},
          false /* doProcessWqes */);
      ++(isSend ? numSendWqes : numReceiveWqes);
    }
  }

  // Process all WQEs at the end instead of per-post.
  EXPECT_TRUE(processWqes(
      &qp, numSendWqes, numReceiveWqes, std::chrono::milliseconds(10000)))
      << "a QP holding a Tag on every number must process what was posted "
         "across all of them, rather than leave some Tag's WQEs unreached";

  EXPECT_EQ(qp.getNumPendingWqes(), numSendWqes)
      << "processWqes only waits for the counts to be reached, so the exact "
         "count is what would catch a WQE processed twice";
  EXPECT_EQ(qp.getNumPendingRWqes(), numReceiveWqes)
      << "and a receive must be counted as one: the two kinds are tracked "
         "separately, so a WQE landing in the wrong one fails both";
}

TEST_F(QpWithTagsTest, ErrorConditionsMultiTag) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  EXPECT_FALSE(qp.containsTag(99));
  EXPECT_EQ(qp.getTag(99), nullptr);

  // Test posting WQE with non-existent tag
  auto invalidWqe = std::make_unique<SendWqe>(
      1, oneSge(receiveBuffer.data(), 1000), qp.getMtu());
  EXPECT_THROW(
      std::ignore = qp.postWqe(99, std::move(invalidWqe)), std::out_of_range);
}

TEST_F(QpWithTagsTest, IndependentBrnrStateManagement) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Initially all tags should be in NORMAL_TX state
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::NORMAL_TX);

  // Take each tag to a different backoff state.
  initialTag->brnr().blockAt(10);
  tag1->brnr().blockAt(20);
  tag1->brnr().beginProbe();
  tag2->brnr().blockAt(30);
  tag2->brnr().beginProbe();
  tag2->brnr().probeSent();

  // Verify states are independent
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::BRNR_PROBING);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::BRNR_PROBE_SENT);

  // Reset one tag without affecting others
  initialTag->brnr().resume();
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::BRNR_PROBING);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::BRNR_PROBE_SENT);
}

TEST_F(QpWithTagsTest, IndependentBrnrBlockedMsnManagement) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Initially no tags should have blocked MSNs
  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_FALSE(tag1->getBrnrFirstBlockedMsn().has_value());
  EXPECT_FALSE(tag2->getBrnrFirstBlockedMsn().has_value());

  // Set different blocked MSNs for different tags
  initialTag->brnr().blockAt(10);
  tag1->brnr().blockAt(20);
  tag2->brnr().blockAt(30);

  // Verify blocked MSNs are independent
  EXPECT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), 10);
  EXPECT_TRUE(tag1->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(tag1->getBrnrFirstBlockedMsn().value(), 20);
  EXPECT_TRUE(tag2->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(tag2->getBrnrFirstBlockedMsn().value(), 30);

  // Clear one tag's blocked MSN without affecting others
  tag1->brnr().resume();
  EXPECT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), 10);
  EXPECT_FALSE(tag1->getBrnrFirstBlockedMsn().has_value());
  EXPECT_TRUE(tag2->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(tag2->getBrnrFirstBlockedMsn().value(), 30);
}

TEST_F(QpWithTagsTest, IndependentBrnrCancellation) {
  // Setup paths and congestion controller
  qp.addPortPair(folly::IPAddressV6("fc00::1"), folly::IPAddressV6("fc00::2"));

  LimitedTestSender mocksender(arbiter, mockUDPSocketMaker);
  mocksender.setLimitNumSockets(2);
  qp.setupFlows(1, 1, &mocksender);

  createAlwaysSendingMockCongestionController(qp);

  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Put tags in BRNR states with blocked MSNs, tag 2 with its probe sent.
  initialTag->brnr().blockAt(5);
  tag1->brnr().blockAt(10);
  tag2->brnr().blockAt(15);
  tag2->brnr().beginProbe();
  tag2->brnr().probeSent();

  // Verify initial BRNR states
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::BRNR_PROBE_SENT);

  // Cancel BRNR for tag 1 only
  qp.cancelRnrBackoff(1);

  // Verify only tag 1 is reset to NORMAL_TX
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::BRNR_PROBE_SENT);

  // Verify blocked MSNs - only tag 1 should be cleared
  EXPECT_TRUE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_FALSE(tag1->getBrnrFirstBlockedMsn().has_value());
  EXPECT_TRUE(tag2->getBrnrFirstBlockedMsn().has_value());
}

TEST_F(QpWithTagsTest, BrnrProcessingWithMultipleTagWqes) {
  // Setup paths and congestion controller
  qp.addPortPair(folly::IPAddressV6("fc00::1"), folly::IPAddressV6("fc00::2"));

  LimitedTestSender mocksender(arbiter, mockUDPSocketMaker);
  mocksender.setLimitNumSockets(2);
  qp.setupFlows(1, 1, &mocksender);

  createAlwaysSendingMockCongestionController(qp);

  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Post WQEs to different tags
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = 1000,
       .tagNum = 0,
       .address = receiveBuffer.data()});
  auto sendWqe1 = postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = 1000,
       .tagNum = 1,
       .address = receiveBuffer.data()});
  (void)postWqeHelper(
      &qp,
      {.workType = Wqe::WorkType::kSend,
       .msgSizeBytes = 1000,
       .tagNum = 2,
       .address = receiveBuffer.data()});

  EXPECT_EQ(qp.getNumPendingWqes(), 3);

  // Set BRNR state for tag 1 only - this should block only tag 1's WQEs
  tag1->brnr().blockAt(sendWqe1->getMsn().value());

  // Tags 0 and 2 should remain in NORMAL_TX
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::NORMAL_TX);

  // Verify only tag 1 has blocked MSN
  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_TRUE(tag1->getBrnrFirstBlockedMsn().has_value());
  EXPECT_FALSE(tag2->getBrnrFirstBlockedMsn().has_value());
}

TEST_F(QpWithTagsTest, BrnrStateTransitionsPerTag) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Test state transition sequence for tag 0
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);

  initialTag->brnr().blockAt(10);
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);

  initialTag->brnr().beginProbe();
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_PROBING);

  initialTag->brnr().probeSent();
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_PROBE_SENT);

  initialTag->brnr().resume();
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);

  // Verify other tags remain in NORMAL_TX throughout
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::NORMAL_TX);
}

TEST_F(QpWithTagsTest, ConcurrentBrnrOperationsMultipleTags) {
  const int numTags = 4;
  // Start at 1 to account for the initial tag
  for (uint8_t tagNum = 1; tagNum < numTags; ++tagNum) {
    qp.addTag(tagNum, &cq, ibvQp);
  }

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);
  auto tag3 = qp.getTag(3);

  // Take each tag to a different backoff state, leaving tag 0 in normal
  // transmission.
  tag1->brnr().blockAt(20);

  tag2->brnr().blockAt(30);
  tag2->brnr().beginProbe();

  tag3->brnr().blockAt(40);
  tag3->brnr().beginProbe();
  tag3->brnr().probeSent();

  // Verify each tag has the correct state
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::BRNR_PROBING);
  EXPECT_EQ(tag3->getBrnrState(), BrnrState::BRNR_PROBE_SENT);

  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_TRUE(tag1->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(tag1->getBrnrFirstBlockedMsn().value(), 20);
  EXPECT_TRUE(tag2->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(tag2->getBrnrFirstBlockedMsn().value(), 30);
  EXPECT_TRUE(tag3->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(tag3->getBrnrFirstBlockedMsn().value(), 40);

  // Perform concurrent operations
  // Cancel BRNR on tag 1, transition tag 2 to BLOCKED, reset tag 3 to NORMAL
  qp.cancelRnrBackoff(1);
  tag2->brnr().blockAt(30);
  tag3->brnr().resume();

  // Verify final states
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(tag3->getBrnrState(), BrnrState::NORMAL_TX);

  // Verify blocked MSN states
  EXPECT_FALSE(initialTag->getBrnrFirstBlockedMsn().has_value());
  EXPECT_FALSE(tag1->getBrnrFirstBlockedMsn().has_value()); // Cleared by cancel
  EXPECT_TRUE(tag2->getBrnrFirstBlockedMsn().has_value());
  EXPECT_FALSE(tag3->getBrnrFirstBlockedMsn().has_value()); // Manually cleared
}

TEST_F(QpWithTagsTest, BrnrErrorHandlingInvalidTags) {
  qp.addTag(1, &cq, ibvQp);

  auto tag1 = qp.getTag(1);

  // Valid operations should work
  EXPECT_NO_THROW((void)initialTag->getBrnrState());
  EXPECT_NO_THROW((void)tag1->getBrnrState());
  EXPECT_NO_THROW(initialTag->brnr().blockAt(10));
  EXPECT_NO_THROW(tag1->brnr().blockAt(10));
  EXPECT_EQ(qp.getTag(99), nullptr);
  EXPECT_NO_THROW((void)initialTag->getBrnrState());
  EXPECT_NO_THROW((void)tag1->getBrnrState());
}

TEST_F(QpWithTagsTest, BrnrPersistenceAcrossTagAddition) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Take each tag to a different backoff state.
  initialTag->brnr().blockAt(5);
  tag1->brnr().blockAt(10);
  tag1->brnr().beginProbe();
  tag2->brnr().blockAt(15);
  tag2->brnr().beginProbe();
  tag2->brnr().probeSent();

  // Verify states are set
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::BRNR_PROBING);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::BRNR_PROBE_SENT);

  // A tag joining the set arrives in NORMAL_TX and must not disturb the BRNR
  // state the others are holding.
  qp.addTag(3, &cq, ibvQp);
  auto tag3 = qp.getTag(3);
  EXPECT_EQ(tag3->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_FALSE(tag3->getBrnrFirstBlockedMsn().has_value());

  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::BRNR_PROBING);
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::BRNR_PROBE_SENT);
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), 5);
  EXPECT_EQ(tag1->getBrnrFirstBlockedMsn().value(), 10);
  EXPECT_EQ(tag2->getBrnrFirstBlockedMsn().value(), 15);
}

TEST_F(QpWithTagsTest, BrnrIsolationBetweenTags) {
  qp.addTag(1, &cq, ibvQp);
  qp.addTag(2, &cq, ibvQp);

  auto tag1 = qp.getTag(1);
  auto tag2 = qp.getTag(2);

  // Set initial states
  initialTag->brnr().blockAt(100);
  tag1->brnr().resume();
  tag2->brnr().blockAt(200);
  tag2->brnr().beginProbe();

  // Run tag 0 through ten probes, each one blocked again at a later msn.
  for (int i = 0; i < 10; ++i) {
    initialTag->brnr().beginProbe();
    initialTag->brnr().probeSent();
    initialTag->brnr().blockAt(100 + i);
  }

  // Verify tag 1 and 2 states are completely unaffected
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_FALSE(tag1->getBrnrFirstBlockedMsn().has_value());
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::BRNR_PROBING);
  EXPECT_EQ(tag2->getBrnrFirstBlockedMsn().value(), 200);

  // Now perform operations on tag 2
  qp.cancelRnrBackoff(2);

  // Verify tag 0 and 1 are unaffected
  EXPECT_EQ(initialTag->getBrnrState(), BrnrState::BRNR_BLOCKED_TX);
  EXPECT_EQ(initialTag->getBrnrFirstBlockedMsn().value(), 100)
      << "an episode keeps the earliest msn it was blocked at, so ten later "
         "blocks in the same episode should not move it forward";
  EXPECT_EQ(tag1->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_FALSE(tag1->getBrnrFirstBlockedMsn().has_value());

  // Tag 2 should be reset
  EXPECT_EQ(tag2->getBrnrState(), BrnrState::NORMAL_TX);
  EXPECT_FALSE(tag2->getBrnrFirstBlockedMsn().has_value());
}

} // namespace libsoftmetaroce
