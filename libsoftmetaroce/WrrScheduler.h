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

#pragma once

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <optional>

namespace libsoftmetaroce {

// Weighted round robin over schedulables holding their own visits. Each
// call takes iterators into one collection of entries, which holds
// identity and order, plus a projection from each entry to its schedulable;
// the round robin keeps a position in that collection, so callers leave
// every recorded entry in place. Each schedulable models four operations:
//   uint8_t getVisitsLeft() const;
//   void refillVisits();  // leaves at least one visit
//   void decrementVisits();
//   void forfeitVisits();
template <typename EntryIt>
class WrrScheduler {
 public:
  WrrScheduler() = default;

  // Returns the next entry holding visits, starting after the entry last served
  // or skipped. Refills every entry first when none holds visits.
  template <typename SchedProj>
  EntryIt select(EntryIt begin, EntryIt end, SchedProj proj) {
    return select(begin, end, proj, [](const auto&) { return false; });
  }

  // Skips each entry `excluded` matches, neither choosing nor refilling it.
  // Returns end when every entry is excluded.
  template <typename SchedProj, typename Excluded>
  EntryIt
  select(EntryIt begin, EntryIt end, SchedProj proj, Excluded excluded) {
    const bool noneHoldsVisits =
        std::none_of(begin, end, [&proj, &excluded](auto& entry) {
          return !excluded(entry) && proj(entry).getVisitsLeft() > 0;
        });
    if (noneHoldsVisits) {
      for (auto it = begin; it != end; ++it) {
        if (!excluded(*it)) {
          proj(*it).refillVisits();
        }
      }
    }
    return nextEligible(begin, end, proj, excluded).value_or(end);
  }

  // This is a non-mutating select(): it returns the same entry while any entry
  // holds visits, and the first entry otherwise.
  template <typename SchedProj>
  EntryIt peek(EntryIt begin, EntryIt end, SchedProj proj) const {
    if (begin == end) {
      return end;
    }
    // No visits anywhere; offer the lowest entry so selection advances.
    return nextEligible(begin, end, proj, [](const auto&) { return false; })
        .value_or(begin);
  }

  // The entry must be one select() returned and still in the collection.
  template <typename SchedProj>
  void noteServed(EntryIt it, SchedProj proj) {
    proj(*it).decrementVisits();
    cursor_ = it;
  }
  template <typename SchedProj>
  void noteSkipped(EntryIt it, SchedProj proj) {
    proj(*it).forfeitVisits();
    cursor_ = it;
  }

 private:
  template <typename SchedProj, typename Excluded>
  std::optional<EntryIt> nextEligible(
      EntryIt begin,
      EntryIt end,
      SchedProj proj,
      Excluded excluded) const {
    const EntryIt resume = cursor_.has_value() && std::next(*cursor_) != end
        ? std::next(*cursor_)
        : begin;
    const auto isEligible = [&proj, &excluded](auto& entry) {
      return !excluded(entry) && proj(entry).getVisitsLeft() > 0;
    };
    for (auto it = resume; it != end; ++it) {
      if (isEligible(*it)) {
        return it;
      }
    }
    for (auto it = begin; it != resume; ++it) {
      if (isEligible(*it)) {
        return it;
      }
    }
    return std::nullopt;
  }

  // Last recorded position; selection starts after it. Recorded entries are
  // never erased, so this stays valid.
  std::optional<EntryIt> cursor_;
};

} // namespace libsoftmetaroce
