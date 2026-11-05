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

#include <libsoftmetaroce/QpPacer.h>

#include <libsoftmetaroce/Constants.h>

#include <cstdint>
#include <optional>

#include <gtest/gtest.h>

namespace libsoftmetaroce {
namespace {

// One byte per nanosecond keeps the expected send times exact by inspection.
constexpr uint64_t kRateBytesPerSec = 1'000'000'000;
constexpr uint32_t kBurstBytes = 8000;
constexpr uint32_t kPacketBytes = 1000;

QpPacer makePacer() {
  return QpPacer(
      QpPacer::RateLimit{
          .rateBytesPerSec = kRateBytesPerSec, .burstBytes = kBurstBytes});
}

} // namespace

TEST(QpPacerTest, SpacesPacketsBySizeOverRate) {
  QpPacer pacer = makePacer();
  ASSERT_TRUE(pacer.isSendDue(0))
      << "precondition: a fresh pacer holds a full burst allowance, so the "
         "first packet leaves at once";
  pacer.onPacketSent(0, kPacketBytes);
  EXPECT_EQ(pacer.getNextSendTimeNs(), kPacketBytes)
      << "each packet must delay the next by its size over the rate, or the "
         "pacer sends faster than its rate";
  EXPECT_FALSE(pacer.isSendDue(kPacketBytes - 1))
      << "the gate holds a packet until its send time arrives";
  EXPECT_TRUE(pacer.isSendDue(kPacketBytes))
      << "the gate releases a packet once its send time arrives";
}

TEST(QpPacerTest, ReportsTheWaitUntilSendIsDue) {
  QpPacer pacer = makePacer();
  pacer.onPacketSent(0, kPacketBytes);
  EXPECT_EQ(pacer.nsUntilSendDue(1), kPacketBytes - 1)
      << "a held pacer reports the time left, so a wake armed for it lands "
         "when the send is due";
  EXPECT_EQ(pacer.nsUntilSendDue(kPacketBytes), std::nullopt)
      << "once the send time arrives the pacer holds no packet, so the caller "
         "arms no wake";

  const QpPacer unlimited(QpPacer::RateLimit{});
  EXPECT_EQ(unlimited.nsUntilSendDue(0), std::nullopt)
      << "an unlimited pacer never holds, so it reports no wait";
}

TEST(QpPacerTest, CapsCatchUpAtBurstAllowance) {
  QpPacer pacer = makePacer();
  constexpr uint64_t kNowNs = 1'000'000'000;
  uint64_t admittedBytes = 0;
  while (pacer.isSendDue(kNowNs)) {
    pacer.onPacketSent(kNowNs, kPacketBytes);
    admittedBytes += kPacketBytes;
  }
  EXPECT_EQ(admittedBytes, kBurstBytes + kPacketBytes)
      << "idleness older than the allowance is lost: the pacer admits the allowance "
         "back to back, plus the one packet the check admits before seeing its size";
  EXPECT_EQ(pacer.getNextSendTimeNs(), kNowNs + kPacketBytes)
      << "once the allowance is spent each packet waits its own send time, or "
         "catch-up exceeds the burst";
}

TEST(QpPacerTest, WaitsAtMostOnePacketTime) {
  QpPacer pacer = makePacer();
  pacer.onPacketSent(0, kPacketBytes);
  const uint64_t dueNs = pacer.getNextSendTimeNs();
  pacer.onPacketSent(dueNs, kPacketBytes);
  EXPECT_EQ(pacer.getNextSendTimeNs() - dueNs, kPacketBytes)
      << "an on-time send must wait one packet time for the next, or a steady "
         "sender drifts from its rate";

  QpPacer latePacer = makePacer();
  constexpr uint64_t kLateNs = 5000;
  latePacer.onPacketSent(kLateNs, kPacketBytes);
  EXPECT_TRUE(latePacer.isSendDue(kLateNs))
      << "a send made with credit to spare leaves the next send already due";

  QpPacer burstPacer = makePacer();
  constexpr uint64_t kNowNs = 1'000'000'000;
  burstPacer.onPacketSent(kNowNs, kBurstBytes);
  burstPacer.onPacketSent(kNowNs, kPacketBytes);
  EXPECT_EQ(burstPacer.getNextSendTimeNs() - kNowNs, kPacketBytes)
      << "the overdraw is one packet, which bounds how long the pacer holds the "
         "next one";
}

TEST(QpPacerTest, StaysWithinCeilingOverTime) {
  QpPacer pacer = makePacer();
  constexpr uint64_t kStepNs = 100;
  constexpr uint64_t kWindowNs = 100'000;
  uint64_t admittedBytes = 0;
  for (uint64_t nowNs = 0; nowNs < kWindowNs; nowNs += kStepNs) {
    if (pacer.isSendDue(nowNs)) {
      pacer.onPacketSent(nowNs, kPacketBytes);
      admittedBytes += kPacketBytes;
    }
  }
  const uint64_t ceilingBytes =
      kWindowNs * kRateBytesPerSec / kNsPerSecond + kBurstBytes + kPacketBytes;
  EXPECT_LE(admittedBytes, ceilingBytes)
      << "a paced receiver absorbs at most the ceiling, so admitting above it overruns "
         "the receiver";
}

TEST(QpPacerTest, StaysWithinCeilingAtNonDividingRate) {
  QpPacer pacer(
      QpPacer::RateLimit{
          .rateBytesPerSec = 3'000'000'000, .burstBytes = kBurstBytes});
  constexpr uint64_t kStepNs = 1;
  constexpr uint64_t kWindowNs = 4'000'000;
  uint64_t admittedBytes = 0;
  for (uint64_t nowNs = 0; nowNs < kWindowNs; nowNs += kStepNs) {
    if (pacer.isSendDue(nowNs)) {
      pacer.onPacketSent(nowNs, kPacketBytes);
      admittedBytes += kPacketBytes;
    }
  }
  const uint64_t ceilingBytes =
      kWindowNs * 3'000'000'000 / kNsPerSecond + kBurstBytes + kPacketBytes;
  EXPECT_LE(admittedBytes, ceilingBytes)
      << "the ceiling holds where the packet cost is fractional, or the pacer "
         "runs fast and overruns the receiver";
}

TEST(QpPacerTest, RescalesTheWaitOnRateChange) {
  QpPacer pacer = makePacer();
  pacer.onPacketSent(0, kPacketBytes);
  pacer.setRateLimit(
      QpPacer::RateLimit{
          .rateBytesPerSec = 2 * kRateBytesPerSec, .burstBytes = kBurstBytes},
      0);
  EXPECT_EQ(pacer.getNextSendTimeNs(), kPacketBytes / 2)
      << "a rate change rescales the remaining wait, so the same bytes wait half "
         "as long at twice the rate";
  pacer.setRateLimit(
      QpPacer::RateLimit{
          .rateBytesPerSec = kRateBytesPerSec / 2, .burstBytes = kBurstBytes},
      0);
  EXPECT_EQ(pacer.getNextSendTimeNs(), 2 * kPacketBytes)
      << "a rate cut stretches the remaining wait over the same bytes, or a "
         "slowed queue pair sends them at the old rate";
}

TEST(QpPacerTest, ClampsAllowanceToNewBurst) {
  QpPacer pacer = makePacer();
  constexpr uint64_t kNowNs = 1'000'000'000;
  pacer.setRateLimit(
      QpPacer::RateLimit{
          .rateBytesPerSec = kRateBytesPerSec, .burstBytes = kPacketBytes},
      kNowNs);
  EXPECT_EQ(pacer.getNextSendTimeNs(), kNowNs - kPacketBytes)
      << "shrinking the burst shrinks the allowance, so old lateness is lost";
}

TEST(QpPacerTest, AdmitsWithoutLimitWhenUnlimited) {
  QpPacer pacer(QpPacer::RateLimit{});
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(pacer.isSendDue(0))
        << "a zero rate lifts the ceiling, so back-to-back packets at one timestamp "
           "all leave";
    pacer.onPacketSent(0, kPacketBytes);
  }
}

TEST(QpPacerTest, ClearsTheWaitWhenUnlimited) {
  QpPacer pacer = makePacer();
  pacer.onPacketSent(0, kPacketBytes);
  ASSERT_FALSE(pacer.isSendDue(kPacketBytes / 2))
      << "precondition: the pacer holds a packet before the change";
  pacer.setRateLimit(QpPacer::RateLimit{}, kPacketBytes / 2);
  EXPECT_TRUE(pacer.isSendDue(kPacketBytes / 2))
      << "lifting the limit ends the wait, so queued work leaves at once";
}

TEST(QpPacerTest, CapsBurstAt100MiB) {
  QpPacer pacer(
      QpPacer::RateLimit{
          .rateBytesPerSec = kRateBytesPerSec,
          .burstBytes = 200 * 1024 * 1024});
  constexpr uint64_t kNowNs = 1'000'000'000;
  uint64_t admittedBytes = 0;
  while (pacer.isSendDue(kNowNs)) {
    pacer.onPacketSent(kNowNs, kPacketBytes);
    admittedBytes += kPacketBytes;
  }
  const uint64_t cappedBursts = QpPacer::kMaxBurstBytes / kPacketBytes + 1;
  EXPECT_EQ(admittedBytes, cappedBursts * kPacketBytes)
      << "a burst past the cap paces as 100 MiB, so bulk-sized bursts cannot "
         "buy bulk-sized catch-up";
}

} // namespace libsoftmetaroce
