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

#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>

#include <glog/logging.h>

namespace libsoftmetaroce {

enum class HrnrState {
  NORMAL_TX, // Normal operation, no backoff.
  HRNR_BLOCKED_TX, // In HRNR backoff, not sending any packets.
                   // On valid ACK or RNR-Cancel, return to normal.
  HRNR_PROBING, // Ready to send a single packet and wait to hear back.
                // On valid ACK or RNR-Cancel, return to normal.
                // Once the probe packet has been sent, return to blocked.
};

enum class BrnrState {
  NORMAL_TX, // Normal operation, no backoff.
  BRNR_BLOCKED_TX, // In BRNR backoff, not sending packets of affected MSNs.
  BRNR_PROBING, // Ready to send a single packet from a blocked MSN.
  BRNR_PROBE_SENT, // Packet from BRNR is in flight; On BRNR, go to
                   // blocked; On ack of blocked MSN or on RNR-Cancel,
                   // return to normal. On RTO, stay.
  BRNR_CANCELED, // An RNR-Cancel reported a posted csn; a BRNR reporting a
                 // frame at or before it was refused before that posting.
};

constexpr std::string_view hrnrStateToString(HrnrState state) noexcept {
  switch (state) {
    case HrnrState::NORMAL_TX:
      return "NORMAL_TX";
    case HrnrState::HRNR_BLOCKED_TX:
      return "HRNR_BLOCKED_TX";
    case HrnrState::HRNR_PROBING:
      return "HRNR_PROBING";
  }
  return "UNKNOWN";
}

constexpr std::string_view brnrStateToString(BrnrState state) noexcept {
  switch (state) {
    case BrnrState::NORMAL_TX:
      return "NORMAL_TX";
    case BrnrState::BRNR_BLOCKED_TX:
      return "BRNR_BLOCKED_TX";
    case BrnrState::BRNR_PROBING:
      return "BRNR_PROBING";
    case BrnrState::BRNR_PROBE_SENT:
      return "BRNR_PROBE_SENT";
    case BrnrState::BRNR_CANCELED:
      return "BRNR_CANCELED";
  }
  return "UNKNOWN";
}
std::ostream& operator<<(std::ostream& os, const HrnrState& state);
std::ostream& operator<<(std::ostream& os, const BrnrState& state);
bool canTransitionHrnr(HrnrState oldState, HrnrState newState);
bool canTransitionBrnr(BrnrState oldState, BrnrState newState);

class BrnrBackoff {
 public:
  [[nodiscard]] BrnrState state() const noexcept {
    return state_;
  }

  [[nodiscard]] std::optional<uint16_t> firstBlockedMsn() const noexcept {
    return firstBlockedMsn_;
  }
  // Whether the block covers this msn, which it does from the first blocked
  // one onward.
  [[nodiscard]] bool isBlockedMsn(uint16_t msn) const noexcept;

  [[nodiscard]] uint32_t retryCount() const noexcept {
    return retryCount_;
  }

  // An episode already under way keeps the earliest msn it was given, and a
  // cancel's csn stands through it.
  void blockAt(uint16_t msn);
  // The backoff expired, so a single packet may go out to ask whether the
  // receiver is ready, which spends one of the tag's retries.
  void beginProbe();
  void probeSent();
  // Returns to normal transmission, ending the episode.
  void resume();

  // An RNR-Cancel reports the highest csn the responder has a receive posted
  // for, and when it reached the initiator.
  void cancelThrough(uint16_t postedCsn, uint32_t nowNs);
  [[nodiscard]] std::optional<uint16_t> canceledThroughCsn() const {
    return canceledThroughCsn_;
  }
  // Whether a frame at this csn is one the cancel covers, which holds only
  // while a BRNR raised before the cancel could still be in flight.
  [[nodiscard]] bool
  cancelCovers(uint16_t csn, uint32_t nowNs, uint32_t withinNs) const;

 private:
  void setState(BrnrState newState);

  std::optional<uint16_t> firstBlockedMsn_;
  std::optional<uint16_t> canceledThroughCsn_;
  uint32_t canceledAtNs_{0};
  BrnrState state_{BrnrState::NORMAL_TX};
  uint32_t retryCount_{0};
};

} // namespace libsoftmetaroce
