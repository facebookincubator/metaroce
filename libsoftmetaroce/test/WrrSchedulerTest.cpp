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

#include <algorithm>
#include <cstdint>
#include <map>
#include <vector>

#include <gtest/gtest.h>

#include <libsoftmetaroce/WrrScheduler.h>

namespace libsoftmetaroce {

// Minimal schedulable: round state without identity, keyed by the map.
struct FakeSchedulable {
  uint8_t weight;
  uint8_t visits;
  uint8_t getWeight() const {
    return weight;
  }
  uint8_t getVisitsLeft() const {
    return visits;
  }
  void refillVisits() {
    visits = weight;
  }
  void decrementVisits() {
    if (visits > 0) {
      --visits;
    }
  }
  void forfeitVisits() {
    visits = 0;
  }
};

using FakeSet = std::map<uint8_t, FakeSchedulable>;
using FakeScheduler = WrrScheduler<FakeSet::iterator>;

FakeSchedulable& asFake(FakeSet::value_type& entry) {
  return entry.second;
}

FakeSchedulable joining(uint8_t weight) {
  return FakeSchedulable{weight, weight};
}

FakeSet::iterator selectIt(FakeScheduler& scheduler, FakeSet& set) {
  return scheduler.select(set.begin(), set.end(), asFake);
}

uint8_t selectTag(FakeScheduler& scheduler, FakeSet& set) {
  auto it = selectIt(scheduler, set);
  EXPECT_NE(it, set.end());
  return it == set.end() ? 0 : it->first;
}

uint8_t peekTag(const FakeScheduler& scheduler, FakeSet& set) {
  auto it = scheduler.peek(set.begin(), set.end(), asFake);
  EXPECT_NE(it, set.end());
  return it == set.end() ? 0 : it->first;
}

void serveTag(FakeScheduler& scheduler, FakeSet& set, uint8_t tag) {
  const auto it = set.find(tag);
  EXPECT_NE(it, set.end());
  if (it != set.end()) {
    scheduler.noteServed(it, asFake);
  }
}

void skipTag(FakeScheduler& scheduler, FakeSet& set, uint8_t tag) {
  const auto it = set.find(tag);
  EXPECT_NE(it, set.end());
  if (it != set.end()) {
    scheduler.noteSkipped(it, asFake);
  }
}

TEST(WrrScheduler, NewTagsJoinWithFullVisitsAndOpenAtLowest) {
  FakeScheduler scheduler;
  FakeSet set{{1, joining(2)}, {2, joining(1)}};
  EXPECT_EQ(selectTag(scheduler, set), 1)
      << "the rotation must start at the lowest live tag";
  serveTag(scheduler, set, 1);
  EXPECT_EQ(selectTag(scheduler, set), 2)
      << "service must advance the cursor past the served tag";
}

TEST(WrrScheduler, ServedTagRejoinsAfterRoundRefill) {
  FakeScheduler scheduler;
  FakeSet set{{1, joining(1)}};
  ASSERT_EQ(selectTag(scheduler, set), 1)
      << "precondition: a lone tag must be offered, or the refill below is "
         "untested";
  serveTag(scheduler, set, 1);
  EXPECT_EQ(selectTag(scheduler, set), 1)
      << "a spent round must refill so a lone tag keeps serving";
}

TEST(WrrScheduler, SkippedTagForfeitsVisits) {
  FakeScheduler scheduler;
  FakeSet set{{1, joining(2)}, {2, joining(1)}};
  ASSERT_EQ(selectTag(scheduler, set), 1)
      << "precondition: the rotation must start at tag 1, or the skip below "
         "records against the wrong tag";
  skipTag(scheduler, set, 1);
  EXPECT_EQ(selectTag(scheduler, set), 2)
      << "a miss must advance past the idle tag";
  serveTag(scheduler, set, 2);
  serveTag(scheduler, set, 1);
  EXPECT_EQ(selectTag(scheduler, set), 2)
      << "tag 1's forfeited visits must not return early: tag 2 serves "
         "again before tag 1 despite tag 1's higher weight";
}

TEST(WrrScheduler, SharesRatioOverRounds) {
  FakeScheduler scheduler;
  FakeSet set{{1, joining(2)}, {2, joining(1)}};
  std::vector<uint8_t> servedTags;
  for (int i = 0; i < 6; ++i) {
    const uint8_t tag = selectTag(scheduler, set);
    servedTags.push_back(tag);
    serveTag(scheduler, set, tag);
  }
  EXPECT_EQ(std::count(servedTags.begin(), servedTags.end(), 1), 4)
      << "weight 2 must serve the heavier tag twice as often";
  EXPECT_EQ(std::count(servedTags.begin(), servedTags.end(), 2), 2)
      << "weight 1 must still serve the lighter tag";
}

TEST(WrrScheduler, EmptySetReconciles) {
  FakeScheduler scheduler;
  FakeSet set;
  EXPECT_EQ(scheduler.select(set.begin(), set.end(), asFake), set.end())
      << "an empty set must select no entry";
  set.emplace(5, joining(1));
  EXPECT_EQ(selectTag(scheduler, set), 5)
      << "rejoining after an empty set must start at the live tag";
}

TEST(WrrScheduler, PeekDoesNotRefill) {
  FakeSet set{{1, joining(2)}, {2, joining(2)}};
  FakeScheduler scheduler;
  // Spend the round: 1, 2, 1, 2.
  for (int i = 0; i < 2; ++i) {
    ASSERT_EQ(selectTag(scheduler, set), 1)
        << "precondition: tag 1 must lead each serve pair";
    serveTag(scheduler, set, 1);
    ASSERT_EQ(selectTag(scheduler, set), 2)
        << "precondition: tag 2 must follow tag 1";
    serveTag(scheduler, set, 2);
  }
  EXPECT_EQ(peekTag(scheduler, set), 1)
      << "a peek identifies the next candidate without refilling";
  EXPECT_EQ(set.at(1).visits, 0) << "a peek must not refill a spent round";
  EXPECT_EQ(set.at(2).visits, 0) << "a peek must not refill a spent round";
  ASSERT_EQ(selectTag(scheduler, set), 1)
      << "service resumes where the peek left it";
  serveTag(scheduler, set, 1);
  EXPECT_EQ(set.at(1).visits, 1)
      << "the peek must leave the round's refill intact";
  EXPECT_EQ(set.at(2).visits, 2)
      << "the peek must leave the round's refill intact";
}

} // namespace libsoftmetaroce
