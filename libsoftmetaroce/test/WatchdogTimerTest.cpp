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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/WatchdogTimer.h>
#include <chrono>

namespace libsoftmetaroce {

class WatchdogTimerTest : public ::testing::Test {
 protected:
  // Restores every flag a case overrides when the fixture is destroyed.
  gflags::FlagSaver flagSaver_;
};

TEST_F(WatchdogTimerTest, AnExponentDerivesTheWaitOneAttemptGets) {
  // 4.096us * 2^14 = 67.108864ms.
  const auto derived = intervalsFromQpAttributes(
      /*localAckTimeoutExponent=*/14, /*retryCount=*/0);
  // One attempt of 4.096us << 14, rounded up.
  EXPECT_EQ(derived.watchdogIntervalMs, std::chrono::milliseconds(68))
      << "the interval is built from that wait, so reading the exponent wrong "
         "derives it from a duration the client never specified";
}

// The IBTA local ack timeout is an exponent, and the interval covers the first
// send plus one wait per retry.
TEST_F(WatchdogTimerTest, AnIbtaExponentBecomesItsDurationOncePerAttempt) {
  // 4.096us * 2^14 = 67.108ms, and 3 attempts of it, rounded up.
  const auto derived = intervalsFromQpAttributes(
      /*localAckTimeoutExponent=*/14, /*retryCount=*/2);
  EXPECT_EQ(derived.watchdogIntervalMs, std::chrono::milliseconds(202))
      << "a client that asked to wait 4.096us*2^14 three times should have "
         "its work failed only after all three of those waits";
}

TEST_F(WatchdogTimerTest, AnExponentAboveTheCeilingIsCutToIt) {
  // The longest single wait the ceiling admits, rounded up so the interval
  // covers it. Derived here so moving the ceiling does not move this test.
  const auto kAtCeiling =
      std::chrono::ceil<std::chrono::milliseconds>(std::chrono::nanoseconds(
          kLocalAckTimeoutBaseNs << kLocalAckTimeoutExponentCeiling));
  const auto atCeiling = intervalsFromQpAttributes(
      kLocalAckTimeoutExponentCeiling, /*retryCount=*/0);
  const auto aboveCeiling =
      intervalsFromQpAttributes(kMaxLocalAckTimeoutExponent, /*retryCount=*/0);
  EXPECT_EQ(atCeiling.watchdogIntervalMs, kAtCeiling)
      << "the ceiling exponent has to derive the wait it specifies, or the "
         "value everything above it is cut to is itself wrong";
  EXPECT_EQ(aboveCeiling.watchdogIntervalMs, kAtCeiling)
      << "without a ceiling a client could push the interval out arbitrarily "
         "by specifying a wider exponent, so the widest one IBTA allows must "
         "wait no longer than the ceiling does";
}

TEST_F(WatchdogTimerTest, TheZeroExponentDerivesTheDisabledWatchdog) {
  EXPECT_EQ(
      intervalsFromQpAttributes(
          kDisabledLocalAckTimeoutExponent, /*retryCount=*/7)
          .watchdogIntervalMs,
      kDisabledTimeoutWatchdogMs)
      << "IBTA's zero asks for no timeout, and a QP that never ends is worse "
         "than one held to an interval long enough never to pass";
}

TEST_F(
    WatchdogTimerTest,
    AnUnconfiguredQpReportsAttributesThatDeriveItsWatchdogInterval) {
  // The ceiling exponent alone would round every one of these up to 2148ms.
  for (const auto intervalMs :
       {std::chrono::milliseconds(20),
        std::chrono::milliseconds(202),
        std::chrono::milliseconds(5000)}) {
    const auto reported = qpAttributesForInterval(intervalMs);
    EXPECT_LE(reported.retryCount, kMaxTransportRetryCount)
        << "a count past three bits is not portable, so a client writing back "
           "what it read would ask rxe to retry forever";
    const auto round = intervalsFromQpAttributes(
        reported.localAckTimeoutExponent, reported.retryCount);
    EXPECT_GE(round.watchdogIntervalMs, intervalMs)
        << "the pair has to cover at least the interval the QP is held to, or "
           "a client reading it back would shorten its own interval, at "
        << intervalMs.count() << "ms";
    EXPECT_LE(round.watchdogIntervalMs.count(), intervalMs.count() * 2)
        << "the pair stands for this interval, so reporting more than double "
           "it describes a QP the client does not have, at "
        << intervalMs.count() << "ms";
  }
}

TEST_F(WatchdogTimerTest, AnIntervalPastWhatTheAttributesReachSaturates) {
  const auto reported =
      qpAttributesForInterval(std::chrono::milliseconds(300000));
  EXPECT_EQ(reported.retryCount, kMaxTransportRetryCount)
      << "no legal pair reaches five minutes, so the report gives the widest "
         "one rather than an illegal count";
}

TEST_F(
    WatchdogTimerTest,
    AnIntervalOnlyTheCeilingReachesKeepsItsOwnRetryCount) {
  const auto ceilingAttemptNs = std::chrono::nanoseconds(
      static_cast<int64_t>(kLocalAckTimeoutBaseNs)
      << kLocalAckTimeoutExponentCeiling);
  // Every attempt the ceiling is allowed, 17179ms today; the exponent below it
  // reaches half that. Derived so moving the ceiling does not move this test.
  const auto ceilingReachMs = std::chrono::floor<std::chrono::milliseconds>(
      ceilingAttemptNs * (kMaxTransportRetryCount + 1));
  const auto intervalMs = ceilingReachMs * 3 / 4;

  const auto reported = qpAttributesForInterval(intervalMs);
  EXPECT_EQ(reported.localAckTimeoutExponent, kLocalAckTimeoutExponentCeiling)
      << "no narrower exponent reaches " << intervalMs.count()
      << "ms within the retries IBTA allows, so the ceiling is the only "
         "exponent that can reach this interval";
  EXPECT_LT(reported.retryCount, kMaxTransportRetryCount)
      << "the widest count is what an interval past every legal pair reports, "
         "so giving it for one the ceiling covers tells a client its queue "
         "pair waits "
      << ceilingReachMs.count() << "ms when it waits " << intervalMs.count()
      << "ms";
  EXPECT_GE(
      intervalsFromQpAttributes(
          reported.localAckTimeoutExponent, reported.retryCount)
          .watchdogIntervalMs,
      intervalMs)
      << "the pair still has to cover the interval the queue pair is held to, "
         "or a client reading it back would shorten its own";
}

TEST_F(WatchdogTimerTest, AnExponentShorterThanTheMinimumIsRaisedToIt) {
  // 4.096us * 2^1, even eight times over, is far under the minimum.
  const auto derived = intervalsFromQpAttributes(
      /*localAckTimeoutExponent=*/1, /*retryCount=*/7);

  WatchdogTimer timer;
  timer.configureInterval(derived.watchdogIntervalMs);
  EXPECT_EQ(timer.getIntervalMs(), kMinWatchdogIntervalMs)
      << "a sub-millisecond interval would fail work on ordinary scheduling "
         "delay rather than on a stall";
}

TEST_F(WatchdogTimerTest, AFlagBelowTheFloorIsRaisedToIt) {
  FLAGS_libsoftmetaroce_qp_watchdog_interval_ms = 5;

  WatchdogTimer timer;
  EXPECT_EQ(timer.getIntervalMs(), kMinWatchdogIntervalMs)
      << "an operator setting less than the floor would have every QP fail its "
         "work on ordinary scheduling delay";
}

TEST_F(WatchdogTimerTest, AnUnconfiguredTimerFollowsTheFlag) {
  const auto aboveFloorMs = kMinWatchdogIntervalMs * 10;
  FLAGS_libsoftmetaroce_qp_watchdog_interval_ms =
      static_cast<uint32_t>(aboveFloorMs.count());

  WatchdogTimer timer;
  EXPECT_EQ(timer.getIntervalMs(), aboveFloorMs)
      << "a queue pair that configured no interval of its own must keep the "
         "one the operator set";
}

TEST_F(WatchdogTimerTest, AConfiguredIntervalWinsOverTheFlag) {
  FLAGS_libsoftmetaroce_qp_watchdog_interval_ms = 1234;

  WatchdogTimer timer;
  timer.configureInterval(std::chrono::milliseconds(250));

  EXPECT_EQ(timer.getIntervalMs(), std::chrono::milliseconds(250))
      << "a caller that configured an interval must be measured against it, or "
         "the flag would override what it asked for";
}

TEST_F(WatchdogTimerTest, AConfiguredIntervalBelowTheFloorIsRaisedToIt) {
  FLAGS_libsoftmetaroce_qp_watchdog_interval_ms = 1234;

  WatchdogTimer timer;
  timer.configureInterval(std::chrono::milliseconds(5));

  EXPECT_EQ(timer.getIntervalMs(), kMinWatchdogIntervalMs)
      << "the floor has to cover a configured interval as well as the flag, or "
         "a client setting a few milliseconds has its work failed on ordinary "
         "scheduling delay";
}

TEST_F(WatchdogTimerTest, ANonPositiveConfigurationIsRaisedToTheFloor) {
  FLAGS_libsoftmetaroce_qp_watchdog_interval_ms = 1234;

  WatchdogTimer timer;
  timer.configureInterval(std::chrono::milliseconds(-5));

  EXPECT_EQ(timer.getIntervalMs(), kMinWatchdogIntervalMs)
      << "the floor has to catch a negative before it reaches unsigned "
         "storage, where it would read as an interval the queue pair never "
         "reaches";
}

TEST_F(WatchdogTimerTest, AConfiguredIntervalDecidesThatWorkHasStalled) {
  // Far past the test's own patience, so only the configured interval can
  // reach expiry.
  FLAGS_libsoftmetaroce_qp_watchdog_interval_ms = 60000;

  WatchdogTimer timer;
  timer.configureInterval(std::chrono::milliseconds(50));
  timer.recordProgressAt(
      std::chrono::steady_clock::now() - std::chrono::milliseconds(100));

  EXPECT_TRUE(timer.hasExpired())
      << "hasExpired() has to measure against the interval the queue pair "
         "configured, or its work waits on the operator's far longer one";
}

TEST_F(WatchdogTimerTest, AConfiguredIntervalHoldsBackAnEarlyExpiry) {
  FLAGS_libsoftmetaroce_qp_watchdog_interval_ms = 20;

  WatchdogTimer timer;
  timer.configureInterval(std::chrono::milliseconds(60000));
  timer.recordProgressAt(
      std::chrono::steady_clock::now() - std::chrono::milliseconds(100));

  EXPECT_FALSE(timer.hasExpired())
      << "a configured interval longer than the flag has to hold the queue "
         "pair back from failing work its peer may still ack";
}

} // namespace libsoftmetaroce
