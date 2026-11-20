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

#include <arpa/inet.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/RejectedPosnSet.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/test/OneSge.h>
#include <array>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

using namespace testing;
namespace libsoftmetaroce {
class WqeTest : public Test {};

/*
 * A posn nothing will acknowledge leaves the outstanding count when it is
 * abandoned, so the message can complete once the packets still in flight are
 * accounted for.
 */
TEST_F(WqeTest, AbandonedPosnLeavesTheOutstandingCount) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), mtu);
  ASSERT_EQ(wqe.getOutstandingPosnCount(), numPackets)
      << "the abandon below is judged by the drop from this count, so a wrong "
         "starting count would blame abandonPosn for a sizing fault";

  wqe.abandonPosn(0);
  EXPECT_EQ(wqe.getOutstandingPosnCount(), numPackets - 1)
      << "abandoning a posn must reduce the outstanding count exactly once";
}

/*
 * A message completes when nothing is outstanding, whichever way each posn got
 * there.
 */
TEST_F(WqeTest, MessageIsCompleteWhenEveryPosnIsResolvedEitherWay) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), mtu);

  ASSERT_EQ(wqe.allocateNextPosn(), 0u)
      << "precondition: the posn acked below is the one this side put on the wire";
  wqe.acknowledgePosn(0);
  wqe.abandonPosn(1);
  EXPECT_NE(wqe.getOutstandingPosnCount(), 0u)
      << "a message must not look complete while a packet is still in flight";

  wqe.abandonPosn(2);
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 0u)
      << "a mix of acknowledged and abandoned posns must still let the message "
         "complete, otherwise a failing wqe would never retire";
}

/*
 * Giving up on a message stops it feeding the tx path and accounts for the
 * packets it never sent, while the ones already in flight stay outstanding.
 */
TEST_F(WqeTest, MarkFailingFreezesSendingAndAbandonsUnsentPosns) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 4;
  constexpr uint32_t size = numPackets * mtu;
  constexpr uint32_t numInFlight = 2;
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), mtu);
  for (uint32_t i = 0; i < numInFlight; i++) {
    std::ignore = wqe.allocateNextPosn();
  }

  const uint32_t abandoned = wqe.markFailing(kWcRetryExcErr);

  EXPECT_EQ(abandoned, numPackets - numInFlight)
      << "every posn that was never sent must be abandoned, since nothing "
         "else will ever account for it";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), numInFlight)
      << "packets already in flight must stay outstanding, since the peer may "
         "still acknowledge them";
  EXPECT_EQ(wqe.allocateNextPosn(), numPackets)
      << "a failing wqe must refuse to hand the tx path another packet, "
         "otherwise it would keep sending a message it has given up on";
  EXPECT_TRUE(wqe.isFailing())
      << "the tx path relies on this to skip retransmitting a wqe it has "
         "given up on";
  ASSERT_TRUE(wqe.getCompletionStatusOverride().has_value())
      << "the status below is read out of this optional; with nothing in it "
         "the read aborts the run instead of reporting the missing override";
  EXPECT_EQ(wqe.getCompletionStatusOverride().value(), kWcRetryExcErr)
      << "the wqe must report the error that caused the failure rather than "
         "the flush its siblings get";
}

/*
 * The last acknowledgement of an in-flight packet leaves nothing outstanding,
 * so a failing message settles rather than hanging. Settling is what makes it
 * eligible; completing it in order, and reporting it where a CQE is called
 * for, are the tag's.
 */
TEST_F(WqeTest, FailingWqeSettlesWhenItsInFlightPacketsAreAcked) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), mtu);
  const uint32_t firstInFlight = wqe.allocateNextPosn();
  const uint32_t secondInFlight = wqe.allocateNextPosn();

  wqe.markFailing(kWcRetryExcErr);
  wqe.acknowledgePosn(firstInFlight);
  EXPECT_NE(wqe.getOutstandingPosnCount(), 0u)
      << "the wqe must not settle while one of its packets is still "
         "unaccounted for, or its buffer could be freed while in use";

  wqe.acknowledgePosn(secondInFlight);
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 0u)
      << "a failing wqe must still settle, or the tag never completes it and "
         "the caller waits on a cqe that never arrives";
}

/*
 * A tag can fail again while it is already failing.
 */
TEST_F(WqeTest, MarkFailingAgainKeepsTheFirstStatus) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 2;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), mtu);

  wqe.markFailing(kWcRetryExcErr);
  const uint32_t abandoned = wqe.markFailing(kWrFlushErr);

  EXPECT_EQ(abandoned, 0u)
      << "a second failure has nothing left to abandon; counting the same "
         "posns twice would corrupt the unsent-posn rate hint";
  EXPECT_EQ(wqe.getCompletionStatusOverride().value(), kWcRetryExcErr)
      << "the first failure is the one that caused this, so it is the one "
         "worth reporting to the caller";
}

/*
 * A work request can carry an error before it is marked failing: the path that
 * completes one in error records the status and leaves it to report in order.
 * The sweep that follows must not relabel it as a flush.
 */
TEST_F(WqeTest, MarkFailingKeepsAStatusRecordedBeforeIt) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 2;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), mtu);

  wqe.setCompletionStatusOverride(kLocalLenErr);
  ASSERT_FALSE(wqe.isFailing())
      << "recording a status must not by itself mark the work request failing, "
         "or the sweep would skip it for the wrong reason";

  wqe.markFailing(kWrFlushErr);

  EXPECT_EQ(wqe.getCompletionStatusOverride().value(), kLocalLenErr)
      << "the error the work request already failed with is the one "
         "the client needs; a flush would hide why it failed";
}

constexpr uint16_t kInlineMtu = 4096;
constexpr uint64_t kInlineWorkReqId = 1;
constexpr uint64_t kInlineRemoteAddress = 0x1000;
constexpr uint32_t kInlineRkey = 0x2000;
constexpr uint32_t kInlineImmediateData = 0xd00d;

struct InlineCapableWqe {
  std::string_view name;
  std::unique_ptr<Wqe> (
      *construct)(void* address, uint32_t size, int sendFlags);
};

/*
 * The four opcodes ibv_post_send admits an inline payload on. They share one
 * copy, in the Wqe constructor, but each reaches it by forwarding send_flags
 * along a chain of its own: the two with an immediate inherit theirs, and the
 * write shares a class with the read, which may not be posted inline at all.
 * Covering the constructors together is what keeps a forward dropped from one
 * of those chains from leaving that opcode alone reading a freed buffer.
 */
const std::array<InlineCapableWqe, 4> kInlineCapableWqes{
    {{"send",
      [](void* address, uint32_t size, int sendFlags) -> std::unique_ptr<Wqe> {
        return std::make_unique<SendWqe>(
            kInlineWorkReqId, oneSge(address, size), kInlineMtu, sendFlags);
      }},
     {"send with immediate",
      [](void* address, uint32_t size, int sendFlags) -> std::unique_ptr<Wqe> {
        return std::make_unique<SendWithImmWqe>(
            kInlineWorkReqId,
            oneSge(address, size),
            kInlineMtu,
            kInlineImmediateData,
            sendFlags);
      }},
     {"write",
      [](void* address, uint32_t size, int sendFlags) -> std::unique_ptr<Wqe> {
        return std::make_unique<ReadWriteWqe>(
            Wqe::WorkType::kWrite,
            kInlineWorkReqId,
            oneSge(address, size),
            kInlineMtu,
            kInlineRemoteAddress,
            kInlineRkey,
            sendFlags);
      }},
     {"write with immediate",
      [](void* address, uint32_t size, int sendFlags) -> std::unique_ptr<Wqe> {
        return std::make_unique<WriteWithImmWqe>(
            kInlineWorkReqId,
            oneSge(address, size),
            kInlineMtu,
            kInlineRemoteAddress,
            kInlineRkey,
            kInlineImmediateData,
            sendFlags);
      }}}};

/*
 * An inline work request carries its payload in the WQE, so the caller may
 * reuse or free the source buffer as soon as the post returns.
 */
TEST_F(WqeTest, AnInlinePayloadIsCopiedSoItsSourceMayChange) {
  for (const auto& opcode : kInlineCapableWqes) {
    SCOPED_TRACE(opcode.name);
    std::array<char, 8> payload{};
    payload.fill('a');
    const std::unique_ptr<Wqe> wqe = opcode.construct(
        payload.data(), payload.size(), IBV_SEND_SIGNALED | IBV_SEND_INLINE);

    EXPECT_NE(wqe->getBufferAddress(), payload.data())
        << "an inline work request must point to its own copy, or overwriting "
           "the source below cannot tell a copy from a reference";

    payload.fill('z');

    const auto* sent = static_cast<const char*>(wqe->getBufferAddress());
    EXPECT_EQ(
        std::string(sent, payload.size()), std::string(payload.size(), 'a'))
        << "the bytes the wqe will send must be the ones posted; a caller is "
           "entitled to reuse its buffer the moment the post returns";
  }
}

/*
 * A work request that is not inline keeps referring to the caller's buffer,
 * which is what the local key registers and what the sender reads at send time.
 */
TEST_F(WqeTest, ANonInlinePayloadIsLeftWhereTheCallerPutIt) {
  for (const auto& opcode : kInlineCapableWqes) {
    SCOPED_TRACE(opcode.name);
    std::array<char, 8> payload{};
    const std::unique_ptr<Wqe> wqe =
        opcode.construct(payload.data(), payload.size(), IBV_SEND_SIGNALED);

    EXPECT_EQ(wqe->getBufferAddress(), payload.data())
        << "copying a non-inline payload would both cost a copy and lose the "
           "aliasing the registered region is built on";
  }
}

TEST_F(WqeTest, AnInlinePayloadIsGatheredFromItsSges) {
  std::array<uint8_t, 3> first{1, 2, 3};
  std::array<uint8_t, 2> second{4, 5};
  const LocalSges sges{
      {.address = first.data(), .length = 3},
      {.address = second.data(), .length = 2},
  };
  const auto payloadOf = [](const Wqe& wqe) {
    const folly::IOBuf payload =
        wqe.makeSendPayloadView(0, kInlineMtu, wqe.getMessageSizeBytes());
    std::vector<uint8_t> bytes;
    for (const folly::ByteRange range : payload) {
      bytes.insert(bytes.end(), range.begin(), range.end());
    }
    return bytes;
  };
  const int flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
  const SendWqe send(kInlineWorkReqId, sges, kInlineMtu, flags);
  const WriteWithImmWqe write(
      kInlineWorkReqId,
      sges,
      kInlineMtu,
      kInlineRemoteAddress,
      kInlineRkey,
      kInlineImmediateData,
      flags);
  first.fill(0);
  second.fill(0);

  const std::vector<uint8_t> expected{1, 2, 3, 4, 5};
  EXPECT_EQ(payloadOf(send), expected)
      << "the bytes are the sges' as posted, in order, not the sources as "
         "they are now";
  EXPECT_EQ(payloadOf(write), expected)
      << "a write takes its inline payload the same way a send does";
}

TEST_F(WqeTest, AFinalPacketAliasedByTheSumIsRefusedUnderAuditing) {
  gflags::FlagSaver flagSaver;
  gflags::SetCommandLineOption("libsoftmetaroce_audit_posn_state", "true");

  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 4;
  constexpr uint32_t size = numPackets * mtu;
  constexpr uint32_t lastPosn = 2;
  std::vector<char> payload(size);
  ReceiveWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  wqe.onPosnReceived(0);
  wqe.onPosnReceived(3);
  const uint32_t outstandingBefore = wqe.getOutstandingPosnCount();

  EXPECT_FALSE(wqe.setFinalPacket(lastPosn, mtu, mtu))
      << "two arrivals summing to three fit a message ending at posn 2, so "
         "the count and the sum take this sender; the full state knows posn 3 "
         "arrived, and refusing is what leaves the caller a nak to send";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), outstandingBefore)
      << "the refusal comes before the counts move, so nothing is left for a "
         "later arrival to disagree with";
}

TEST_F(WqeTest, TheBitsAreSizedFromTheCursorEachTimeTheyAreNeeded) {
  RejectedPosnSet set;

  set.add(5, 10);
  EXPECT_EQ(set.takeLowest(), 5u);
  ASSERT_TRUE(set.empty())
      << "precondition: draining the set is what drops the bits, so the add "
         "below sizes them afresh rather than reusing the first sizing";

  set.add(5000, 6000);
  EXPECT_TRUE(set.contains(5000))
      << "the bits are sized from the cursor handed in, so a posn the earlier "
         "sizing could not have held is still recorded rather than dropped";
  EXPECT_EQ(set.count(), 1u);
}

TEST_F(WqeTest, ClearingTheSetReleasesTheBitsRatherThanZeroingThem) {
  RejectedPosnSet set;

  set.add(5, 10);
  set.clear();
  ASSERT_TRUE(set.empty())
      << "precondition: clearing leaves no posn waiting, so the add below "
         "sizes the bits afresh rather than reusing the first sizing";

  set.add(5000, 6000);
  EXPECT_TRUE(set.contains(5000))
      << "clearing releases the bits rather than zeroing them where they are, "
         "so a posn the first sizing could not have held is still recorded";
  EXPECT_EQ(set.count(), 1u);
}

TEST_F(WqeTest, RejectedPosnsAreOfferedLowestFirst) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 6;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  for (uint32_t expected = 0; expected < numPackets; ++expected) {
    ASSERT_EQ(wqe.allocateNextPosn(), expected)
        << "precondition: the cursor hands out posns in order until every one "
           "of them is on the wire";
  }

  wqe.releasePosn(4);
  wqe.releasePosn(1);

  EXPECT_EQ(wqe.allocateNextPosn(), 1u)
      << "the lower of the two rejected posns must go back on the wire first, "
         "since it is the older work and the message fills from its front";
  EXPECT_EQ(wqe.allocateNextPosn(), 4u)
      << "the other rejected posn follows, and the order does not depend on "
         "which of them was rejected first";
  EXPECT_EQ(wqe.allocateNextPosn(), numPackets)
      << "nothing is left to send once both are back out, and the cursor has "
         "no posn beyond the message to offer";
}

TEST_F(WqeTest, TestClaimSndNxtPosn) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 5;
  constexpr uint32_t size = (numPackets - 1) * mtu + mtu / 2;
  std::array<char, size> payload{};
  SendWqe wqe(
      1, // workReqId
      oneSge(payload),
      mtu);

  // Test initial state
  EXPECT_EQ(wqe.getOutstandingPosnCount(), numPackets)
      << "Initial outstanding position count should match number of packets";
  EXPECT_FALSE(wqe.hasBeenSent())
      << "WQE should not be marked as sent initially";

  // Allocate and verify positions
  uint32_t posn;
  for (int i = 0; i < numPackets - 1; i++) {
    posn = wqe.allocateNextPosn();
    EXPECT_EQ(posn, i) << "Position " << i
                       << " should be allocated sequentially";
    EXPECT_EQ(wqe.getPosnPayloadSizeBytes(posn, mtu), mtu)
        << "Non-last position should have full MTU size";
    EXPECT_FALSE(wqe.isLastPosn(posn))
        << "Position " << i << " should not be marked as last position";
    EXPECT_TRUE(wqe.isFrameAllocated(posn))
        << "Position should be allocated after allocation";
  }

  // Test last position
  posn = wqe.allocateNextPosn();
  EXPECT_EQ(posn, numPackets - 1)
      << "Last position should be at index numPackets-1";
  EXPECT_EQ(wqe.getPosnPayloadSizeBytes(posn, mtu), mtu / 2)
      << "Last position should have half MTU size";
  EXPECT_TRUE(wqe.isLastPosn(posn))
      << "Position " << posn << " should be marked as last position";

  // Test hasBeenSent after allocating all positions
  EXPECT_TRUE(wqe.hasBeenSent())
      << "WQE should be marked as sent after allocating all positions";

  // Test releasing and reallocating a position
  uint32_t middlePosn = 2;
  wqe.releasePosn(middlePosn);
  EXPECT_FALSE(wqe.isFrameAllocated(middlePosn))
      << "a released position is no longer the one on the wire, and waits to "
         "be sent again";
  EXPECT_FALSE(wqe.hasBeenSent())
      << "WQE should not be marked as sent after releasing a position";

  // Reallocate the released position
  posn = wqe.allocateNextPosn();
  EXPECT_EQ(posn, middlePosn) << "Released position should be reallocated next";
  EXPECT_TRUE(wqe.hasBeenSent())
      << "WQE should be marked as sent after reallocating all positions";

  // Test marking positions as acknowledged
  wqe.acknowledgePosn(0);
  EXPECT_EQ(wqe.getOutstandingPosnCount(), numPackets - 1)
      << "Outstanding position count should decrease after acking";
}

/*
 * One response proves the peer received the read and served it. The rest may
 * still be in flight, but putting the request back on the wire would ask for
 * a message the peer has already delivered.
 */
TEST_F(WqeTest, AReadIsServedByItsFirstResponse) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numResponses = 3;
  std::array<char, numResponses * mtu> payload{};
  ReadWriteWqe read(Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0x1000, 42);

  EXPECT_FALSE(read.hasBeenServed())
      << "nothing has come back yet, so the request still has something to "
         "fetch and handing its frame back is what recovers it";

  read.onPosnReceived(0);

  EXPECT_TRUE(read.hasBeenServed())
      << "the peer has served the request, which a resend cannot improve on, "
         "even with responses still outstanding";
  EXPECT_NE(read.getOutstandingPosnCount(), 0u)
      << "the read is served while incomplete, which is the case a whole "
         "response would not distinguish";
}

TEST_F(WqeTest, TestSingleMtuMessage) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t size = mtu; // Exactly one MTU
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), mtu);

  // Verify we have exactly one position
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 1)
      << "Single MTU message should have one position";

  uint32_t posn = wqe.allocateNextPosn();
  EXPECT_EQ(posn, 0) << "First position should be 0";
  EXPECT_EQ(wqe.getPosnPayloadSizeBytes(posn, mtu), mtu)
      << "Single position should have full MTU size";
  EXPECT_TRUE(wqe.isLastPosn(posn))
      << "Single position should be marked as last position";
  EXPECT_TRUE(wqe.hasBeenSent())
      << "WQE should be marked as sent after allocating all positions";
}

TEST_F(WqeTest, TestZeroLengthMessageHasSinglePosn) {
  constexpr uint16_t mtu = 4096;
  // Spec 8.2.3 permits zero-byte payloads for SEND, SEND_IMM_P, WRITE_IMM_P,
  // READ_RESP. The WQE must allocate exactly one POSN (one data packet with
  // empty payload) without underflowing the numPosn formula.
  SendWqe wqe(1, oneSge(/* address */ nullptr, /* size */ 0), mtu);

  EXPECT_EQ(wqe.getOutstandingPosnCount(), 1u)
      << "Zero-length message should have exactly one POSN, not underflow";

  uint32_t posn = wqe.allocateNextPosn();
  EXPECT_EQ(posn, 0u)
      << "First (and only) POSN of a zero-length message should be 0";
  EXPECT_TRUE(wqe.isLastPosn(posn))
      << "The single POSN of a zero-length message is the last POSN";
  EXPECT_EQ(wqe.getPosnPayloadSizeBytes(posn, mtu), 0u)
      << "Zero-length message reports 0-byte payload for its only POSN";
  EXPECT_TRUE(wqe.hasBeenSent())
      << "WQE should be marked as sent after allocating its single POSN";
}

TEST_F(WqeTest, TestSubMtuMessage) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t size = mtu / 4; // Smaller than one MTU
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), mtu);

  // Verify we have exactly one position
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 1)
      << "Sub-MTU message should have one position";

  uint32_t posn = wqe.allocateNextPosn();
  EXPECT_EQ(posn, 0) << "First position should be 0";
  EXPECT_EQ(wqe.getPosnPayloadSizeBytes(posn, mtu), size)
      << "Position should have size equal to message size";
  EXPECT_TRUE(wqe.isLastPosn(posn))
      << "Single position should be marked as last position";
}

TEST_F(WqeTest, TestExactMultipleMtu) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size =
      numPackets * mtu; // Exactly multiple MTUs (no partial packet)
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), mtu);

  EXPECT_EQ(wqe.getOutstandingPosnCount(), numPackets)
      << "Should have exactly numPackets positions";

  // Allocate and verify all positions
  for (uint32_t i = 0; i < numPackets; i++) {
    uint32_t posn = wqe.allocateNextPosn();
    EXPECT_EQ(posn, i) << "Position " << i
                       << " should be allocated sequentially";
    EXPECT_EQ(wqe.getPosnPayloadSizeBytes(posn, mtu), mtu)
        << "All positions should have full MTU size";
    EXPECT_EQ(wqe.isLastPosn(posn), (i == numPackets - 1))
        << "Only position " << (numPackets - 1) << " should be last";
  }

  EXPECT_TRUE(wqe.hasBeenSent())
      << "WQE should be marked as sent after allocating all positions";
}

TEST_F(WqeTest, TestMtuUpdate) {
  constexpr uint16_t initialMtu = 4096;
  constexpr uint32_t size = 3 * initialMtu + initialMtu / 2; // 3.5 MTUs
  std::array<char, size> payload{};
  SendWqe wqe(1, oneSge(payload), initialMtu);

  EXPECT_EQ(wqe.getOutstandingPosnCount(), 4)
      << "Should have 4 positions with initial MTU";

  // Update MTU to half the original size
  uint16_t newMtu = initialMtu / 2;
  wqe.updateMtu(newMtu);

  // Now we should have 7 positions (size / newMtu = ceil(3.5*4096 / 2048) =
  // 7)
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 7)
      << "Should have 7 positions after MTU update";

  // Allocate positions with new MTU
  for (int i = 0; i < 7; i++) {
    uint32_t posn = wqe.allocateNextPosn();
    // With size = 3.5 * initialMtu = 14336 bytes and newMtu = 2048,
    // we need 7 positions (14336 / 2048 = 7 exactly)
    // So all positions will have full newMtu size
    EXPECT_EQ(wqe.getPosnPayloadSizeBytes(posn, newMtu), newMtu)
        << "Position " << posn << " should have correct size after MTU update";
  }

  EXPECT_TRUE(wqe.hasBeenSent())
      << "WQE should be marked as sent after allocating all positions";
}

TEST_F(WqeTest, TestWqeOutputStreamWithImmediateData) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t size = mtu / 2;
  constexpr uint32_t immediateData =
      0x12345678; // Test value in network byte order
  std::array<char, size> payload{};

  SendWithImmWqe wqe(1, oneSge(payload), mtu, immediateData);

  // Convert WQE to string using operator<<
  std::ostringstream oss;
  oss << wqe;
  std::string output = oss.str();

  // Verify that immediate data appears in the output
  // The immediate data should be converted from network to host byte order
  uint32_t expectedHostOrder = ntohl(immediateData);
  std::string expectedImmediateDataStr =
      ", immediateData: " + std::to_string(expectedHostOrder);

  EXPECT_NE(output.find(expectedImmediateDataStr), std::string::npos)
      << "Output should contain immediate data in host byte order. Output: "
      << output;

  // Also verify the work type appears
  EXPECT_NE(output.find("type: SendWithImm"), std::string::npos)
      << "Output should contain work type. Output: " << output;
}

TEST_F(WqeTest, TestOversizeWqeThrowsOnAllocate) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t size = 512 << 20; // 512 MB, which is too big.
  std::array<char, 4096> payload{}; // undersize payload, no need to allocate.

  SendWqe wqe(1, oneSge(payload.data(), size), mtu);

  EXPECT_EQ(wqe.getOutstandingPosnCount(), 2 << 16)
      << "Should require 128K (4096-byte) packets to send 512MB";

  EXPECT_ANY_THROW((void)wqe.allocateNextPosn());
}

TEST_F(WqeTest, AnOversizeReadIsRejectedLikeAnOversizeSend) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t size = 512 << 20; // 512 MB, which is too big.
  std::array<char, 4096> payload{}; // undersize payload, no need to allocate.

  ReadWriteWqe wqe(
      Wqe::WorkType::kRead,
      1,
      oneSge(payload.data(), size),
      mtu,
      0xDEAD0000,
      42);

  EXPECT_EQ(wqe.getOutstandingPosnCount(), 2 << 16)
      << "a 512MB read is answered in 128K (4096-byte) response packets";

  EXPECT_ANY_THROW((void)wqe.allocateNextPosn())
      << "the wire numbers a response packet in 16 bits, so a read whose "
         "response cannot be numbered must be rejected rather than sent";
}

TEST_F(WqeTest, AtomicFetchAddWqeCreation) {
  constexpr uint16_t mtu = 4096;
  uint64_t localBuffer = 0;
  uint64_t remoteAddr = 0x1000;
  uint32_t rkey = 1;
  uint64_t addData = 0x42;

  AtomicWqe wqe(
      Wqe::WorkType::kAtomicFetchAdd,
      1, // workReqId
      LocalSge{
          .address = &localBuffer, .length = sizeof(localBuffer), .lkey = 1},
      mtu,
      remoteAddr,
      rkey,
      addData,
      0 /* compareData unused for FA */,
      /*send_flags=*/0);

  EXPECT_EQ(wqe.getWorkType(), Wqe::WorkType::kAtomicFetchAdd);
  EXPECT_EQ(wqe.getRemoteAddress(), remoteAddr);
  EXPECT_EQ(wqe.getRkey(), rkey);
  EXPECT_EQ(wqe.getSwapOrAddData(), addData);
  EXPECT_EQ(wqe.getCompareData(), 0u);
  EXPECT_EQ(wqe.getMessageSizeBytes(), 8u);
  EXPECT_EQ(wqe.ibvCompletionOpcode(), IBV_WC_FETCH_ADD);
}

TEST_F(WqeTest, AtomicCmpSwpWqeCreation) {
  constexpr uint16_t mtu = 4096;
  uint64_t localBuffer = 0;
  uint64_t remoteAddr = 0x2000;
  uint32_t rkey = 2;
  uint64_t swapData = 0xDEAD;
  uint64_t compareData = 0xBEEF;

  AtomicWqe wqe(
      Wqe::WorkType::kAtomicCmpSwp,
      2, // workReqId
      LocalSge{
          .address = &localBuffer, .length = sizeof(localBuffer), .lkey = 1},
      mtu,
      remoteAddr,
      rkey,
      swapData,
      compareData,
      /*send_flags=*/0);

  EXPECT_EQ(wqe.getWorkType(), Wqe::WorkType::kAtomicCmpSwp);
  EXPECT_EQ(wqe.getRemoteAddress(), remoteAddr);
  EXPECT_EQ(wqe.getRkey(), rkey);
  EXPECT_EQ(wqe.getSwapOrAddData(), swapData);
  EXPECT_EQ(wqe.getCompareData(), compareData);
  EXPECT_EQ(wqe.getMessageSizeBytes(), 8u);
  EXPECT_EQ(wqe.ibvCompletionOpcode(), IBV_WC_COMP_SWAP);
}

TEST_F(WqeTest, AtomicWqeAlwaysSinglePosn) {
  constexpr uint16_t mtu = 1024;
  uint64_t localBuffer = 0;

  AtomicWqe wqe(
      Wqe::WorkType::kAtomicFetchAdd,
      1,
      LocalSge{
          .address = &localBuffer, .length = sizeof(localBuffer), .lkey = 1},
      mtu,
      0x1000, // remoteAddr
      1, // rkey
      0x42, // swapOrAddData
      0 /* compareData */,
      /*send_flags=*/0);

  EXPECT_EQ(wqe.getNumPosn(), 1u)
      << "Atomic ops (8 bytes) always fit in a single packet";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 1u);
}

TEST_F(WqeTest, AtomicWqeInvalidWorkType) {
  uint64_t localBuffer = 0;
  EXPECT_THROW(
      AtomicWqe(
          Wqe::WorkType::kSend,
          1,
          LocalSge{
              .address = &localBuffer,
              .length = sizeof(localBuffer),
              .lkey = 0},
          4096,
          0x1000,
          1,
          0,
          0,
          /*send_flags=*/0),
      std::runtime_error);
}

// getPosnPayloadSizeBytes reports the data payload size for a READ WQE: the
// Receiver validates incoming READ_RESP packets and the RxFlow delivers their
// data using this size, so a READ posn reports the full message size.
TEST_F(WqeTest, ReadWqeReportsCorrectPayloadSize) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t size = mtu; // Single-MTU read
  std::array<char, size> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead,
      1, // workReqId
      oneSge(payload),
      mtu,
      0xDEAD0000, // remoteAddress
      42); // rkey

  EXPECT_EQ(wqe.getOutstandingPosnCount(), 1)
      << "Single-MTU read should have one position";

  EXPECT_EQ(wqe.getPosnPayloadSizeBytes(0, mtu), size)
      << "READ WQE posn 0 should report full message size for RX validation";
}

TEST_F(WqeTest, ReadWqeMultiPosnReportsCorrectPayloadSize) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu - mtu / 2; // 10240 bytes
  std::array<char, size> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);

  EXPECT_EQ(wqe.getOutstandingPosnCount(), numPackets)
      << "Multi-MTU read should have " << numPackets << " positions";

  // The responder fills these positions, so they are read directly rather
  // than allocated: allocating is what this side does to its request frame.
  for (uint32_t posn = 0; posn < numPackets; posn++) {
    uint32_t expected = (posn < numPackets - 1) ? mtu : (size - posn * mtu);
    EXPECT_EQ(wqe.getPosnPayloadSizeBytes(posn, mtu), expected)
        << "READ WQE posn " << posn
        << " should report correct payload size for RX validation";
  }
}

TEST_F(WqeTest, ReadWithImmWqeCarriesImmediateAndActsLikeARead) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 2;
  constexpr uint32_t size = numPackets * mtu;
  constexpr uint32_t immediateData = 0xFEEDBEEF;
  std::array<char, size> payload{};
  ReadWithImmWqe wqe(
      1, // workReqId
      oneSge(payload),
      mtu,
      0xDEAD0000, // remoteAddress
      42, // rkey
      immediateData);

  EXPECT_EQ(wqe.getWorkType(), Wqe::WorkType::kReadWithImm)
      << "the work type routes this WQE to READ_IMM_P on the wire";
  ASSERT_TRUE(wqe.getImmediateData().has_value())
      << "a read-with-immediate carries the immediate for the responder's "
         "completion";
  EXPECT_EQ(wqe.getImmediateData().value(), immediateData)
      << "the responder reports this value, so it must arrive intact";
  EXPECT_EQ(wqe.ibvCompletionOpcode(), IBV_WC_RDMA_READ)
      << "the requester reports it as a plain RDMA read";
  EXPECT_TRUE(wqe.makeSendPayloadView(0, mtu, mtu).empty())
      << "a read request carries no send payload, immediate or not";
  EXPECT_EQ(wqe.allocateNextPosn(), Wqe::kRequestFrameWirePosn)
      << "like a plain read it sends one request frame";
  EXPECT_EQ(wqe.getSendablePosnCount(), 1u)
      << "only the request frame is sendable; the response positions belong "
         "to the responder";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), numPackets)
      << "it still awaits every response packet after its request goes out";
}

TEST_F(WqeTest, SendingAReadRequestLeavesTheResponsePositionsAlone) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);

  EXPECT_EQ(wqe.allocateNextPosn(), Wqe::kRequestFrameWirePosn)
      << "A read allocates the one frame it sends, which carries position "
      << "zero on the wire to identify the start of the response";
  EXPECT_TRUE(wqe.hasBeenSent())
      << "One request frame is everything a read sends, so it has nothing "
      << "further to offer the sender";
  EXPECT_EQ(wqe.getReceivedPosnCount(), 0u)
      << "the response packets fill these positions, so sending the request "
      << "must not consume one";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), numPackets)
      << "A read still awaits every packet of its response after its request "
      << "goes out";
  EXPECT_EQ(wqe.getSendablePosnCount(), 1u)
      << "The arbiter has one send to make for a read, not one per response "
      << "packet it will never transmit";
}

TEST_F(WqeTest, AckingAReadRequestDoesNotCompleteTheRead) {
  constexpr uint16_t mtu = 4096;
  std::array<char, mtu> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);
  const uint32_t posn = wqe.allocateNextPosn();

  wqe.onFrameNewlyAcked(posn);
  EXPECT_FALSE(wqe.isCompletedByAcks())
      << "A read is completed by the response that carries its data, so the "
      << "ack for its request is never the event that completes it";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 1u)
      << "Acking the request must not consume the position the response will "
      << "fill";
}

TEST_F(WqeTest, AReleasedReadRequestFrameMayBeSentAgain) {
  constexpr uint16_t mtu = 4096;
  std::array<char, mtu> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);
  const uint32_t posn = wqe.allocateNextPosn();
  ASSERT_TRUE(wqe.isFrameAllocated(posn))
      << "A sent request frame is allocated until it is acked or released";

  wqe.releaseFrame(posn);
  EXPECT_FALSE(wqe.hasBeenSent())
      << "A request frame returned by a BRNR must go out again, so the read "
      << "must offer itself to the sender again";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 1u)
      << "Releasing the request frame says nothing about the response, which "
      << "is still outstanding";
}

TEST_F(
    WqeTest,
    AReleasedReadRequestFrameReallocatesWithoutTouchingTheResponse) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);
  const uint32_t posn = wqe.allocateNextPosn();
  wqe.releaseFrame(posn);

  EXPECT_EQ(wqe.allocateNextPosn(), Wqe::kRequestFrameWirePosn)
      << "a retried read request frame must carry posn zero on the wire, "
         "where its response starts";
  EXPECT_TRUE(wqe.hasBeenSent())
      << "otherwise the sender would put the read request on the wire twice";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), numPackets)
      << "the response posns are the responder's to fill, so a retry must leave "
         "their count alone";
}

TEST_F(WqeTest, AReadFailingAfterItsRequestFrameWasReleasedSettlesAtOnce) {
  constexpr uint16_t mtu = 4096;
  std::array<char, mtu> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);
  const uint32_t posn = wqe.allocateNextPosn();
  wqe.releaseFrame(posn);

  EXPECT_EQ(wqe.markFailing(kWcRetryExcErr), 1u)
      << "a REJECTED request frame is unsent, so failing the read removes it "
         "from the unsent count";
  EXPECT_TRUE(wqe.isFailingAndSettled())
      << "no packet on the wire can resolve a REJECTED request frame, so the "
         "read settles at once";
  EXPECT_TRUE(wqe.hasBeenSent())
      << "a failing read must not offer its request to the sender again";
  EXPECT_EQ(wqe.markFailing(kWrFlushErr), 0u)
      << "the first call abandoned the request frame, so a second call must "
         "not subtract it from the unsent count again";
}

/*
 * A read marked failing before its request went out: a pending request reads
 * as unsent, so without the failing state the sender would be offered a
 * message that has stopped trying to be delivered.
 */
TEST_F(WqeTest, AReadMarkedFailingBeforeItsRequestWentOutIsNotOffered) {
  constexpr uint16_t mtu = 4096;
  std::array<char, mtu> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);
  ASSERT_FALSE(wqe.hasBeenSent())
      << "the request has not gone out yet, so the sender still has it to "
         "send";

  wqe.markFailing(kWcRetryExcErr);

  EXPECT_TRUE(wqe.hasBeenSent())
      << "a read that has been given up on must not offer itself to the "
         "sender again; its request is never going out";
}

TEST_F(WqeTest, AnAckForAReadRequestABrnrTookBackDoesNotStandInForTheResend) {
  constexpr uint16_t mtu = 4096;
  std::array<char, mtu> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);
  const uint32_t posn = wqe.allocateNextPosn();
  wqe.releaseFrame(posn);

  wqe.onFrameNewlyAcked(posn);
  EXPECT_FALSE(wqe.hasBeenSent())
      << "the ack points to the frame the BRNR took back, so treating it as "
         "an ack would leave the read believing it had been sent and never "
         "resend it";
}

TEST_F(WqeTest, FailingAReadReturnsOneUnsentFrameNotOnePerResponsePosn) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 4;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  ReadWriteWqe unsent(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);

  EXPECT_EQ(unsent.markFailing(kWcRetryExcErr), 1u)
      << "the sender was told this message had one frame to send, so "
         "failing it may hand back one; counting a response position it never "
         "had to "
         "send would inflate the unsent total for work that never existed";

  ReadWriteWqe sent(
      Wqe::WorkType::kRead, 2, oneSge(payload), mtu, 0xDEAD0000, 42);
  std::ignore = sent.allocateNextPosn();
  EXPECT_EQ(sent.markFailing(kWcRetryExcErr), 0u)
      << "a request already on the wire left the unsent count when it was "
         "claimed, so abandoning it must not return it a second time";
}

/*
 * What a work request still waits on is the request frame for a read, and the
 * unresolved posns for a send -- including posns not yet sent, which the
 * sender has yet to put on the wire.
 */
TEST_F(WqeTest, TheUnresolvedTxCountIsAReadsRequestAndASendsPosns) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 4;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};

  ReadWriteWqe read(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);
  const uint32_t requestPosn = read.allocateNextPosn();
  EXPECT_EQ(read.getUnresolvedTxCount(), 1u)
      << "a read on the wire waits on the one frame it sent, not on the four "
         "posns its response would fill";

  read.abandonFrame(requestPosn);
  EXPECT_EQ(read.getUnresolvedTxCount(), 0u)
      << "the request reached a terminal state, so the read has nothing left "
         "on the wire even though its response posns stay counted";
  EXPECT_EQ(read.getOutstandingPosnCount(), numPackets)
      << "the response posns are the responder's to fill and are untouched, "
         "which is what makes the two counts differ for a read";

  SendWqe send(2, oneSge(payload), mtu);
  EXPECT_EQ(send.getUnresolvedTxCount(), numPackets)
      << "a send waits on every posn, sent or not: each one is a packet this "
         "side has yet to send, or has sent and not yet had answered";
  const uint32_t firstPosn = send.allocateNextPosn();
  send.acknowledgePosn(firstPosn);
  EXPECT_EQ(send.getUnresolvedTxCount(), numPackets - 1)
      << "acking one posn resolves it and leaves the rest";
}

/*
 * An atomic settles on the same rule as a read: it too is answered by a
 * response, so its one request frame is all the sender has left to account
 * for once the message is failing.
 */
TEST_F(WqeTest, AFailingAtomicSettlesOnceItsRequestFrameDoes) {
  uint64_t localBuffer = 0;
  AtomicWqe wqe(
      Wqe::WorkType::kAtomicFetchAdd,
      1, // workReqId
      LocalSge{
          .address = &localBuffer, .length = sizeof(localBuffer), .lkey = 1},
      Wqe::kAtomicMtu,
      0x1000, // remoteAddress
      1, // rkey
      0x42, // swapOrAddData
      0, // compareData unused for fetch-add
      /*send_flags=*/0);
  const uint32_t posn = wqe.allocateNextPosn();

  wqe.markFailing(kWcRetryExcErr);
  EXPECT_FALSE(wqe.isFailingAndSettled())
      << "the request frame is still claimed and can be retransmitted, so the "
         "atomic is not settled yet";

  wqe.onFrameNewlyAcked(posn);
  EXPECT_TRUE(wqe.isFailingAndSettled())
      << "with its request settled nothing else can arrive for a failed "
         "atomic, so it must be eligible for its error completion";
  EXPECT_EQ(wqe.getReceivedPosnCount(), 0u)
      << "the position its answer would have filled is receive-owned and must "
         "be left alone by the abandonment";
}

TEST_F(WqeTest, AbandoningAReadRequestFrameLeavesItsResponsePositionAlone) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 2;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kRead, 1, oneSge(payload), mtu, 0xDEAD0000, 42);
  const uint32_t posn = wqe.allocateNextPosn();

  wqe.abandonFrame(posn);

  EXPECT_TRUE(wqe.isRequestFrameResolved())
      << "the frame the tracker holds is the request, so giving up on it has "
         "to resolve the request rather than a position of the response";
  EXPECT_EQ(wqe.getReceivedPosnCount(), 0u)
      << "position zero belongs to the first response packet and merely "
         "shares a number with the request frame";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), numPackets)
      << "no response has arrived, so giving up on the request frame must "
         "leave every response position outstanding";
}

TEST_F(WqeTest, AWriteIsCompletedByAcksForEveryPositionItSent) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  ReadWriteWqe wqe(
      Wqe::WorkType::kWrite, 1, oneSge(payload), mtu, 0xDEAD0000, 42);

  for (uint32_t i = 0; i < numPackets; i++) {
    const uint32_t posn = wqe.allocateNextPosn();
    wqe.onFrameNewlyAcked(posn);
    EXPECT_EQ(wqe.isCompletedByAcks(), i == numPackets - 1)
        << "A write is complete once the last of its packets is acked, and "
        << "not before";
  }
}

TEST_F(WqeTest, FailingAReceiveTwiceLeavesItsOutstandingCountAlone) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 4;
  constexpr uint32_t size = numPackets * mtu;
  std::vector<char> payload(size);
  ReceiveWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  wqe.onPosnReceived(0);
  ASSERT_EQ(wqe.markFailing(kWrFlushErr), numPackets - 1)
      << "precondition: the first failure abandons the three posns that never "
         "arrived, which is what the second must not abandon again";
  ASSERT_EQ(wqe.getOutstandingPosnCount(), 0u)
      << "precondition: the message settles, so a second subtraction has "
         "nothing left to take and would wrap the count";

  EXPECT_EQ(wqe.markFailing(kWcRetryExcErr), 0u)
      << "a failing receive is already failing, so the second call abandons "
         "nothing rather than subtracting the unarrived posns a second time";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 0u)
      << "the count stays settled; taking the unarrived posns off it twice "
         "would wrap it to a huge value and hang the tag on a message that "
         "never completes";
}

TEST_F(WqeTest, AFinalPacketBehindAPosnThatArrivedOutOfOrderIsRefused) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 10;
  constexpr uint32_t size = numPackets * mtu;
  std::vector<char> payload(size);
  ReceiveWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  wqe.onPosnReceived(7);
  const uint32_t outstandingBefore = wqe.getOutstandingPosnCount();

  EXPECT_FALSE(wqe.setFinalPacket(2, mtu, mtu))
      << "one arrival leaves the count saying nothing, and posn 7 sits past a "
         "message ending at posn 2, so only the sum carries that; taking it "
         "would subtract that arrival a second time";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), outstandingBefore)
      << "the count is left where it was rather than dropping below what the "
         "message still has outstanding";
}

TEST_F(WqeTest, AFinalPacketBehindWhatAlreadyArrivedIsRefused) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 8;
  constexpr uint32_t arrived = 5;
  constexpr uint32_t size = numPackets * mtu;
  std::vector<char> payload(size);
  ReceiveWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  for (uint32_t posn = 0; posn < arrived; ++posn) {
    wqe.onPosnReceived(posn);
  }
  const uint32_t outstandingBefore = wqe.getOutstandingPosnCount();
  ASSERT_EQ(outstandingBefore, numPackets - arrived)
      << "precondition: five of eight posns are in, which is what the sender "
         "contradicts below";

  // The sender now calls posn 1 its last, behind five it already sent.
  EXPECT_FALSE(wqe.setFinalPacket(1, mtu, mtu))
      << "a message ending at posn 1 holds two posns, and five have arrived, "
         "so taking this would subtract the extra three a second time";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), outstandingBefore)
      << "the count is left where it was rather than wrapping below zero, "
         "which is what keeps a peer from deciding when this process stops";
  EXPECT_EQ(wqe.getNumPosn(), numPackets)
      << "the message keeps the length it was posted with, since the claim "
         "that shortened it was refused";
}

// Both send-with-immediate and rdma-write-with-immediate deliver immediate
// data through a receive WQE, so the completion opcode cannot key off
// immediate data alone: only rdma-write-with-immediate reports
// IBV_WC_RECV_RDMA_WITH_IMM.
TEST_F(WqeTest, ReceiveWqeImmediateCompletionOpcodeDistinguishesSendVsWrite) {
  constexpr uint16_t mtu = 4096;
  std::array<char, 64> payload{};

  ReceiveWqe sendImm(1 /*workReqId*/, oneSge(payload), mtu);
  sendImm.setImmediateData(0xABCD);
  EXPECT_EQ(sendImm.ibvCompletionOpcode(), IBV_WC_RECV)
      << "a receive consumed by send-with-immediate completes IBV_WC_RECV even "
         "though it carries immediate data";

  ReceiveWqe writeImm(2 /*workReqId*/, oneSge(payload), mtu);
  writeImm.setImmediateData(0xABCD);
  writeImm.setIbvCompletionOpcode(IBV_WC_RECV_RDMA_WITH_IMM);
  EXPECT_EQ(writeImm.ibvCompletionOpcode(), IBV_WC_RECV_RDMA_WITH_IMM)
      << "a receive consumed by rdma-write-with-immediate completes "
         "IBV_WC_RECV_RDMA_WITH_IMM";
}

namespace {

constexpr uint32_t kSweepNumPosn = 3;

// The posn state machine `dictionary.md` describes. A case drives it and a
// real work request through the same events and compares their answers.
// The width is fixed so the sweep can copy one per edge without the heap.
class PosnModel {
 public:
  PosnModel() {
    status_.fill(Wqe::PosnStatus::PENDING);
  }

  [[nodiscard]] static constexpr uint32_t numPosn() {
    return kSweepNumPosn;
  }

  [[nodiscard]] bool isRejected(uint32_t posn) const {
    return status_[posn] == Wqe::PosnStatus::REJECTED;
  }
  [[nodiscard]] bool isAllocated(uint32_t posn) const {
    return status_[posn] == Wqe::PosnStatus::ALLOCATED;
  }
  [[nodiscard]] bool isResolved(uint32_t posn) const {
    return status_[posn] == Wqe::PosnStatus::ACKED ||
        status_[posn] == Wqe::PosnStatus::ABANDONED;
  }
  [[nodiscard]] bool isFailing() const {
    return failing_;
  }

  uint32_t allocate() {
    for (uint32_t posn = 0; posn < numPosn(); ++posn) {
      if (isRejected(posn)) {
        status_[posn] = Wqe::PosnStatus::ALLOCATED;
        return posn;
      }
    }
    if (cursor_ == numPosn()) {
      return numPosn();
    }
    const uint32_t posn = cursor_++;
    status_[posn] = Wqe::PosnStatus::ALLOCATED;
    return posn;
  }

  void reject(uint32_t posn) {
    status_[posn] = Wqe::PosnStatus::REJECTED;
  }
  void ack(uint32_t posn) {
    status_[posn] = Wqe::PosnStatus::ACKED;
  }
  void abandon(uint32_t posn) {
    status_[posn] = Wqe::PosnStatus::ABANDONED;
  }

  uint32_t markFailing() {
    if (failing_) {
      return 0;
    }
    failing_ = true;
    cursor_ = numPosn();
    uint32_t abandoned = 0;
    for (auto& status : status_) {
      if (status == Wqe::PosnStatus::PENDING ||
          status == Wqe::PosnStatus::REJECTED) {
        status = Wqe::PosnStatus::ABANDONED;
        ++abandoned;
      }
    }
    return abandoned;
  }

  [[nodiscard]] uint32_t outstanding() const {
    uint32_t count = 0;
    for (uint32_t posn = 0; posn < numPosn(); ++posn) {
      count += isResolved(posn) ? 0 : 1;
    }
    return count;
  }

  [[nodiscard]] bool hasBeenSent() const {
    if (cursor_ != numPosn()) {
      return false;
    }
    for (uint32_t posn = 0; posn < numPosn(); ++posn) {
      if (isRejected(posn)) {
        return false;
      }
    }
    return true;
  }

 private:
  std::array<Wqe::PosnStatus, kSweepNumPosn> status_{};
  uint32_t cursor_{0};
  bool failing_{false};
};

struct PosnOp {
  enum class Kind { kAllocate, kReject, kAck, kAbandon, kMarkFailing };
  Kind kind{Kind::kAllocate};
  uint32_t posn{0};
};

// A state offers an allocate and a markFailing, and each posn at most an ack,
// a reject and an abandon, so what is legal never outgrows this.
struct LegalOps {
  std::array<PosnOp, 2 + 3 * kSweepNumPosn> ops{};
  size_t count{0};

  void add(PosnOp op) {
    ops.at(count++) = op;
  }
  [[nodiscard]] const PosnOp* begin() const {
    return ops.data();
  }
  [[nodiscard]] const PosnOp* end() const {
    return ops.data() + count;
  }
};

LegalOps legalOps(const PosnModel& model) {
  LegalOps ops;
  ops.add({PosnOp::Kind::kAllocate, 0});
  // markFailing() is idempotent, so a second one costs a step and no state.
  if (!model.isFailing()) {
    ops.add({PosnOp::Kind::kMarkFailing, 0});
  }
  for (uint32_t posn = 0; posn < model.numPosn(); ++posn) {
    if (model.isAllocated(posn)) {
      ops.add({PosnOp::Kind::kAck, posn});
      if (!model.isFailing()) {
        ops.add({PosnOp::Kind::kReject, posn});
      }
    }
    if (model.isAllocated(posn) || model.isRejected(posn)) {
      ops.add({PosnOp::Kind::kAbandon, posn});
    }
  }
  return ops;
}

// The one place an op moves the model, so the walk and the replay cannot come
// to explore one state machine and compare another.
uint32_t apply(PosnModel& model, PosnOp op) {
  switch (op.kind) {
    case PosnOp::Kind::kAllocate:
      return model.allocate();
    case PosnOp::Kind::kReject:
      model.reject(op.posn);
      return 0;
    case PosnOp::Kind::kAck:
      model.ack(op.posn);
      return 0;
    case PosnOp::Kind::kAbandon:
      model.abandon(op.posn);
      return 0;
    case PosnOp::Kind::kMarkFailing:
      return model.markFailing();
  }
  return 0; // unreachable; satisfies -Wreturn-type
}

std::string describe(const std::vector<PosnOp>& ops, size_t upTo) {
  std::string out;
  for (size_t i = 0; i <= upTo && i < ops.size(); ++i) {
    switch (ops[i].kind) {
      case PosnOp::Kind::kAllocate:
        out += "allocate ";
        break;
      case PosnOp::Kind::kReject:
        out += "reject(" + std::to_string(ops[i].posn) + ") ";
        break;
      case PosnOp::Kind::kAck:
        out += "ack(" + std::to_string(ops[i].posn) + ") ";
        break;
      case PosnOp::Kind::kAbandon:
        out += "abandon(" + std::to_string(ops[i].posn) + ") ";
        break;
      case PosnOp::Kind::kMarkFailing:
        out += "markFailing ";
        break;
    }
  }
  return out;
}

void runPosnEventSweep() {
  constexpr size_t kDepth = 8;
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t size = kSweepNumPosn * mtu;
  std::array<char, size> payload{};

  std::string firstFailure;
  size_t sequencesChecked = 0;
  std::vector<PosnOp> sequence;
  sequence.reserve(kDepth);

  const auto note = [&](const std::vector<PosnOp>& ops,
                        size_t upTo,
                        const std::string& what) {
    if (firstFailure.empty()) {
      firstFailure = what + " after: " + describe(ops, upTo);
    }
  };

  // A Wqe cannot be copied, so each sequence runs against a fresh one.
  const auto replay = [&](const std::vector<PosnOp>& ops) {
    SendWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);
    PosnModel model;
    for (size_t i = 0; i < ops.size() && firstFailure.empty(); ++i) {
      uint32_t got = 0;
      std::string_view called;
      switch (ops[i].kind) {
        case PosnOp::Kind::kAllocate:
          got = wqe.allocateNextPosn();
          called = "allocateNextPosn";
          break;
        case PosnOp::Kind::kReject:
          wqe.releasePosn(ops[i].posn);
          break;
        case PosnOp::Kind::kAck:
          wqe.acknowledgePosn(ops[i].posn);
          break;
        case PosnOp::Kind::kAbandon:
          wqe.abandonPosn(ops[i].posn);
          break;
        case PosnOp::Kind::kMarkFailing:
          got = wqe.markFailing(kWcRetryExcErr);
          called = "markFailing";
          break;
      }
      const uint32_t want = apply(model, ops[i]);
      if (got != want) {
        note(
            ops,
            i,
            std::string(called) + " gave " + std::to_string(got) +
                " rather than " + std::to_string(want));
      }
      if (wqe.getOutstandingPosnCount() != model.outstanding()) {
        note(
            ops,
            i,
            "outstanding count is " +
                std::to_string(wqe.getOutstandingPosnCount()) +
                " rather than " + std::to_string(model.outstanding()));
      }
      if (wqe.hasBeenSent() != model.hasBeenSent()) {
        note(
            ops,
            i,
            "hasBeenSent is " + std::to_string(wqe.hasBeenSent()) +
                " rather than " + std::to_string(model.hasBeenSent()));
      }
    }
    ++sequencesChecked;
  };

  const std::function<void(const PosnModel&)> explore =
      [&](const PosnModel& model) {
        if (!firstFailure.empty()) {
          return;
        }
        if (sequence.size() == kDepth) {
          replay(sequence);
          return;
        }
        for (const auto& op : legalOps(model)) {
          PosnModel next = model;
          std::ignore = apply(next, op);
          sequence.push_back(op);
          explore(next);
          sequence.pop_back();
        }
      };
  explore(PosnModel{});

  EXPECT_TRUE(firstFailure.empty()) << firstFailure;
  if (firstFailure.empty()) {
    EXPECT_GT(sequencesChecked, 1000u)
        << "the sweep must actually reach a useful number of sequences, or it "
           "is asserting nothing about states it never built";
    EXPECT_LT(sequencesChecked, 40000u)
        << "the walk costs a replay per sequence, so a depth or posn count "
           "raised past what this bound allows buys coverage at a price the "
           "case was never sized to pay";
  }
}

} // namespace

TEST_F(WqeTest, EveryShortSequenceOfPosnEventsAnswersAsTheModelDoes) {
  runPosnEventSweep();
}

TEST_F(WqeTest, TheAuditorAgreesWithTheCountsAcrossEveryShortSequence) {
  gflags::FlagSaver flagSaver;
  gflags::SetCommandLineOption("libsoftmetaroce_audit_posn_state", "true");
  runPosnEventSweep();
}

TEST_F(WqeTest, TheAuditorCatchesAPosnHandedBackAfterItWasAcked) {
  gflags::FlagSaver flagSaver;
  gflags::SetCommandLineOption("libsoftmetaroce_audit_posn_state", "true");

  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  ASSERT_EQ(wqe.allocateNextPosn(), 0u)
      << "precondition: posn 0 goes out, so the ack below names a frame that "
         "was on the wire";
  wqe.acknowledgePosn(0);

  // Both messages come from the auditor: the cross-check inside
  // isFrameAllocated() where DCHECKs are kept, onReleased() otherwise.
  EXPECT_DEATH(
      wqe.releasePosn(0), "handed back while resolved|read differently")
      << "an acked posn is still below the cursor and out of the rejected "
         "set, so the work request reads it as allocated; the full state is "
         "the only thing that knows the frame is already answered";
}

TEST_F(WqeTest, APosnHandedBackThatNeverWentOutStopsTheProcess) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  // Two guards stop this, and which one speaks depends on the build: the
  // work request's DCHECK where those are kept, the set's CHECK otherwise.
  EXPECT_DEATH(
      wqe.releasePosn(0),
      "handed back by the tracker|comes back without having gone out")
      << "the bits reach only as far as the cursor, so a posn the sender "
         "never claimed would index past them; stopping is what keeps that "
         "from being a write, and it holds with auditing off";
}

TEST_F(WqeTest, APosnHandedBackTwiceStopsTheProcess) {
  // The work request's DCHECK catches this where those are kept; the auditor
  // is what still catches it where they are compiled out.
  gflags::FlagSaver flagSaver;
  gflags::SetCommandLineOption("libsoftmetaroce_audit_posn_state", "true");

  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  ASSERT_EQ(wqe.allocateNextPosn(), 0u)
      << "precondition: the posn handed back below is one that went out, "
         "which leaves the second hand-back as the only fault";
  wqe.releasePosn(0);

  EXPECT_DEATH(
      wqe.releasePosn(0),
      "handed back by the tracker|handed back while rejected")
      << "a posn already waiting is no longer on the wire, so the tracker "
         "cannot be handing it back a second time; the set takes it again "
         "without complaint, so one of the two guards has to say so";
}

TEST_F(WqeTest, APosnArrivingTwiceLeavesTheAuditorAbleToFailTheMessage) {
  gflags::FlagSaver flagSaver;
  gflags::SetCommandLineOption("libsoftmetaroce_audit_posn_state", "true");

  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 3;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  ReceiveWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  wqe.onPosnReceived(0);
  wqe.onPosnReceived(0);
  ASSERT_EQ(wqe.getOutstandingPosnCount(), 1u)
      << "precondition: the repeat took the count down a second time, which "
         "is the early completion a sender reusing a posn causes";

  EXPECT_EQ(wqe.markFailing(kWrFlushErr), 1u)
      << "failing the message after a repeat arrival must run to the end "
         "rather than take the auditor's own count below zero";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 0u)
      << "a failed message settles, or the tag waits on a posn the sender "
         "already spent on another frame";
}

TEST_F(WqeTest, AnMtuUpdateAfterAnArrivalStopsTheProcess) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t startingPackets = 4;
  constexpr uint32_t grownPackets = 7;
  std::vector<char> payload(grownPackets * mtu);
  ReceiveWqe wqe(
      1 /*workReqId*/, oneSge(payload.data(), startingPackets * mtu), mtu);

  wqe.onPosnReceived(0);

  EXPECT_DEATH(
      wqe.updateDestinationAddress(
          reinterpret_cast<uint64_t>(payload.data()), grownPackets * mtu, mtu),
      "arrivals this work request already took")
      << "the mtu is set while a receive is posted and before it is bound to "
         "a message, so an arrival before it means the caller has the order "
         "wrong and the posns that arrived would be lost";
}

TEST_F(WqeTest, AnMtuUpdateAfterAPosnWentOutStopsTheProcess) {
  constexpr uint16_t mtu = 4096;
  constexpr uint32_t size = 4 * mtu;
  std::array<char, size> payload{};
  SendWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  (void)wqe.allocateNextPosn();

  EXPECT_DEATH(wqe.updateMtu(mtu / 2), "posns already on the wire")
      << "the mtu is set as a send is built, so a posn allocated before it "
         "means the caller has the order wrong and the cursor would outrun "
         "the state the update rebuilds";
}

TEST_F(WqeTest, AReceiveShortenedToItsLastPacketKeepsTheAuditorInStep) {
  gflags::FlagSaver flagSaver;
  gflags::SetCommandLineOption("libsoftmetaroce_audit_posn_state", "true");

  constexpr uint16_t mtu = 4096;
  constexpr uint32_t numPackets = 4;
  constexpr uint32_t size = numPackets * mtu;
  std::array<char, size> payload{};
  ReceiveWqe wqe(1 /*workReqId*/, oneSge(payload), mtu);

  wqe.onPosnReceived(0);
  ASSERT_TRUE(wqe.setFinalPacket(1, mtu, mtu))
      << "precondition: one posn has arrived and the sender's last is posn 1, "
         "so the message still holds everything that came";

  EXPECT_EQ(wqe.getNumPosn(), 2u)
      << "the sender's last packet is posn 1, so the message is two packets "
         "rather than the four the buffer had room for";
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 1u)
      << "posn 0 has arrived and posn 1 has not, and the two posns past the "
         "end are no longer owed";

  wqe.onPosnReceived(1);
  EXPECT_EQ(wqe.getOutstandingPosnCount(), 0u)
      << "the last packet of the shortened message completes it, the posns "
         "past the end having gone with the truncation";
}

/*
 * A send posted with several sges sends them back to back, so a packet whose
 * bytes cross an sge boundary carries the end of one and the start of the next.
 */
TEST_F(WqeTest, ASegmentedSendGathersEachPosnFromTheSgesItSpans) {
  constexpr uint16_t mtu = 256;
  std::vector<uint8_t> first(100);
  std::vector<uint8_t> second(500);
  std::vector<uint8_t> third(168);
  std::vector<uint8_t> message;
  for (auto* buffer : {&first, &second, &third}) {
    for (auto& byte : *buffer) {
      byte = static_cast<uint8_t>(message.size() * 7);
      message.push_back(byte);
    }
  }
  const SendWqe wqe(
      1,
      LocalSges{
          {.address = first.data(), .length = 100},
          {.address = second.data(), .length = 500},
          {.address = third.data(), .length = 168},
      },
      mtu);
  ASSERT_EQ(wqe.getNumPosn(), 3u)
      << "precondition: 768 bytes at a 256-byte mtu make three posns";

  std::vector<uint8_t> sent;
  for (uint32_t posn = 0; posn < wqe.getNumPosn(); ++posn) {
    const uint32_t size = wqe.getPosnPayloadSizeBytes(posn, mtu);
    const folly::IOBuf payload = wqe.makeSendPayloadView(posn, mtu, size);
    EXPECT_EQ(payload.computeChainDataLength(), size)
        << "posn " << posn << " must carry its whole payload, however many "
        << "sges it spans";
    for (const folly::ByteRange range : payload) {
      sent.insert(sent.end(), range.begin(), range.end());
    }
  }
  EXPECT_EQ(sent, message)
      << "the packets together must carry the sges in order, with nothing "
         "dropped or repeated at a boundary";

  EXPECT_TRUE(wqe.makeSendPayloadView(0, mtu, mtu).isChained())
      << "posn 0 spans the first two sges, so it references both";
  EXPECT_FALSE(wqe.makeSendPayloadView(1, mtu, mtu).isChained())
      << "posn 1 lies within the second sge, so it is sent in place";
}

/*
 * A receive posted with several sges fills them in order, so a packet whose
 * offset crosses an sge boundary is split between them.
 */
TEST_F(WqeTest, ASegmentedReceiveScattersEachPosnAcrossItsSges) {
  constexpr uint16_t mtu = 256;
  std::vector<uint8_t> first(10, 0);
  std::vector<uint8_t> second(400, 0);
  std::vector<uint8_t> third(102, 0);
  ReceiveWqe wqe(
      1,
      LocalSges{
          {.address = first.data(), .length = 10},
          {.address = second.data(), .length = 400},
          {.address = third.data(), .length = 102},
      },
      mtu);

  std::vector<uint8_t> message(512);
  for (size_t i = 0; i < message.size(); ++i) {
    message[i] = static_cast<uint8_t>(i * 13 + 1);
  }
  // Out of order, as packets may arrive.
  wqe.deliverPosn(1, mtu, folly::ByteRange(message.data() + mtu, mtu));
  wqe.deliverPosn(0, mtu, folly::ByteRange(message.data(), mtu));

  std::vector<uint8_t> received(first);
  received.insert(received.end(), second.begin(), second.end());
  received.insert(received.end(), third.begin(), third.end());
  EXPECT_EQ(received, message)
      << "each byte must land at its message offset, whichever sge holds it";
}

TEST_F(WqeTest, NoMoreThanOneSgeKeepsTheBufferContiguous) {
  std::array<uint8_t, 64> buffer{};
  EXPECT_FALSE(
      SendWqe(1, LocalSges{{.address = buffer.data(), .length = 64}}, 256)
          .hasLocalSges())
      << "a single-buffer work request must not pay for segmented storage";
  EXPECT_FALSE(SendWqe(1, LocalSges{}, 256).hasLocalSges())
      << "a zero-length work request has no buffer to segment";
}

TEST_F(WqeTest, AContiguousSendPayloadIsReferencedInPlaceAtItsOffset) {
  constexpr uint16_t mtu = 256;
  std::vector<uint8_t> buffer(600);
  SendWqe wqe(1, oneSge(buffer), mtu);
  const folly::IOBuf payload = wqe.makeSendPayloadView(2, mtu, 88);
  EXPECT_EQ(payload.data(), buffer.data() + 2 * mtu)
      << "the send reads the caller's buffer at the posn's offset, uncopied";
  EXPECT_EQ(payload.length(), 88u)
      << "the payload is exactly the size the sender asked for";
  EXPECT_FALSE(payload.isChained())
      << "a contiguous buffer yields one buffer per posn";
}

TEST_F(WqeTest, AContiguousReceiveTakesBytesAtThePosnsOffset) {
  constexpr uint16_t mtu = 256;
  std::vector<uint8_t> buffer(600, 0);
  ReceiveWqe wqe(1, oneSge(buffer), mtu);
  const std::vector<uint8_t> bytes(88, 0xAB);
  wqe.deliverPosn(2, mtu, folly::ByteRange(bytes.data(), bytes.size()));
  EXPECT_EQ(std::count(buffer.begin(), buffer.begin() + 2 * mtu, 0), 2 * mtu)
      << "the posns before this one must be left alone";
  EXPECT_TRUE(std::equal(bytes.begin(), bytes.end(), buffer.begin() + 2 * mtu))
      << "the bytes must land at the posn's offset";
}

TEST_F(WqeTest, AReceiveRefusesBytesPastThePosn) {
  constexpr uint16_t mtu = 256;
  std::vector<uint8_t> buffer(512);
  ReceiveWqe wqe(1, oneSge(buffer), mtu);
  const std::vector<uint8_t> oversize(mtu + 1);
  EXPECT_DEATH(
      wqe.deliverPosn(0, mtu, folly::ByteRange(oversize)),
      "no more than the posn holds")
      << "the extra byte would land in posn 1, which another packet fills";
}

void expectCanHaveSendPayload(const Wqe& wqe, bool expected) {
  EXPECT_EQ(wqe.canHaveSendPayload(), expected)
      << wqe.getWorkType()
      << (expected ? " may put message bytes on the wire"
                   : " sends a request frame or header values only");
}

TEST_F(WqeTest, SendPathWorkTypesReportTheirPayloadRequirement) {
  std::array<uint8_t, 64> buffer{};
  const auto size = static_cast<uint32_t>(buffer.size());
  expectCanHaveSendPayload(
      SendWqe(kInlineWorkReqId, oneSge(buffer), kInlineMtu), true);
  expectCanHaveSendPayload(
      SendWithImmWqe(
          kInlineWorkReqId, oneSge(buffer), kInlineMtu, kInlineImmediateData),
      true);
  expectCanHaveSendPayload(
      ReadWriteWqe(
          Wqe::WorkType::kWrite,
          kInlineWorkReqId,
          oneSge(buffer),
          kInlineMtu,
          kInlineRemoteAddress,
          kInlineRkey),
      true);
  expectCanHaveSendPayload(
      WriteWithImmWqe(
          kInlineWorkReqId,
          oneSge(buffer),
          kInlineMtu,
          kInlineRemoteAddress,
          kInlineRkey,
          kInlineImmediateData),
      true);
  expectCanHaveSendPayload(
      VirtualSendWqe(1, buffer.data(), size, kInlineMtu), true);

  expectCanHaveSendPayload(
      ReadWriteWqe(
          Wqe::WorkType::kRead,
          kInlineWorkReqId,
          oneSge(buffer),
          kInlineMtu,
          kInlineRemoteAddress,
          kInlineRkey),
      false);
  expectCanHaveSendPayload(
      ReadWithImmWqe(
          kInlineWorkReqId,
          oneSge(buffer),
          kInlineMtu,
          kInlineRemoteAddress,
          kInlineRkey,
          kInlineImmediateData),
      false);
  for (const auto workType :
       {Wqe::WorkType::kAtomicFetchAdd, Wqe::WorkType::kAtomicCmpSwp}) {
    expectCanHaveSendPayload(
        AtomicWqe(
            workType,
            kInlineWorkReqId,
            LocalSge{
                .address = buffer.data(), .length = kAtomicOperationSizeBytes},
            kInlineMtu,
            kInlineRemoteAddress,
            kInlineRkey,
            /*swapOrAddData=*/1,
            /*compareData=*/0,
            IBV_SEND_SIGNALED),
        false);
  }
  expectCanHaveSendPayload(VirtualAtomicAckWqe(1, 0), false);
  expectCanHaveSendPayload(VirtualAtomicExecWqe(1, true, 0, 1, 0), false);
}
} // namespace libsoftmetaroce
