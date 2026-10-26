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

  // Next entry holding visits over [begin, end), from after `cursor_` with
  // wrap. Refills every entry first when none holds visits.
  template <typename SchedProj>
  EntryIt select(EntryIt begin, EntryIt end, SchedProj proj) {
    if (begin == end) {
      return end;
    }
    const bool noneHoldsVisits = std::none_of(begin, end, [&proj](auto& entry) {
      return proj(entry).getVisitsLeft() > 0;
    });
    if (noneHoldsVisits) {
      for (auto it = begin; it != end; ++it) {
        proj(*it).refillVisits();
      }
    }
    return peek(begin, end, proj);
  }

  // The same choice without the refill, for readiness peeks: it identifies
  // the next candidate even when the round is spent.
  template <typename SchedProj>
  EntryIt peek(EntryIt begin, EntryIt end, SchedProj proj) const {
    if (begin == end) {
      return end;
    }
    const EntryIt resume = cursor_.has_value() && std::next(*cursor_) != end
        ? std::next(*cursor_)
        : begin;
    for (auto it = resume; it != end; ++it) {
      if (proj(*it).getVisitsLeft() > 0) {
        return it;
      }
    }
    for (auto it = begin; it != resume; ++it) {
      if (proj(*it).getVisitsLeft() > 0) {
        return it;
      }
    }
    // No visits anywhere; offer the lowest entry so selection advances.
    return begin;
  }

  // The entry must be live: callers pass only what select returned.
  template <typename SchedProj>
  void noteServed(EntryIt it, SchedProj proj) {
    proj(*it).decrementVisits();
    cursor_ = it;
  }

  // The entry must be live: callers pass only what select returned.
  template <typename SchedProj>
  void noteSkipped(EntryIt it, SchedProj proj) {
    proj(*it).forfeitVisits();
    cursor_ = it;
  }

 private:
  // Last recorded position; selection starts after it. Recorded entries are
  // never erased, so this stays valid.
  std::optional<EntryIt> cursor_;
};

} // namespace libsoftmetaroce
