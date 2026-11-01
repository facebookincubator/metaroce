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

#include <ostream>
#include <string>

#include <fmt/format.h>

#include <libsoftmetaroce/RnrState.h>
#include <libsoftmetaroce/WrappingCompare.h>

namespace libsoftmetaroce {

std::ostream& operator<<(std::ostream& os, const HrnrState& state) {
  return os << hrnrStateToString(state);
}

std::ostream& operator<<(std::ostream& os, const BrnrState& state) {
  return os << brnrStateToString(state);
}

bool canTransitionHrnr(HrnrState oldState, HrnrState newState) {
  switch (newState) {
    case HrnrState::NORMAL_TX:
      // can transition to normal from anywhere, e.g., on ack or cancel.
      break;
    case HrnrState::HRNR_BLOCKED_TX:
      // can transition to blocked from anywhere, e.g., on nak.
      break;
    case HrnrState::HRNR_PROBING:
      if (oldState != HrnrState::HRNR_BLOCKED_TX) {
        LOG(ERROR)
            << "can only transition to probing from blocked (timer expires)";
        return false;
      }
      break;
    default:
      throw std::runtime_error(
          fmt::format("Invalid HrnrState: {}", (int)newState));
  }
  return true;
}

bool canTransitionBrnr(BrnrState oldState, BrnrState newState) {
  switch (newState) {
    case BrnrState::NORMAL_TX:
      // can transition to normal from anywhere, e.g., on ack or cancel.
      break;
    case BrnrState::BRNR_BLOCKED_TX:
      // can transition to blocked from anywhere, e.g., on nak.
      break;
    case BrnrState::BRNR_PROBING:
      if (oldState != BrnrState::BRNR_BLOCKED_TX) {
        LOG(ERROR)
            << "can only transition to probing from blocked (timer expires)";
        return false;
      }
      break;
    case BrnrState::BRNR_CANCELED:
      // A cancel reaches a tag in any state, including one never blocked.
      break;
    case BrnrState::BRNR_PROBE_SENT:
      // Any retransmission of a blocked msn is a probe, so this is reachable
      // from every blocked state.
      if (oldState == BrnrState::NORMAL_TX) {
        LOG(ERROR) << "a probe is sent for a blocked msn, and a tag in normal "
                      "transmission has none";
        return false;
      }
      break;
    default:
      throw std::runtime_error(
          fmt::format("Invalid BrnrState: {}", (int)newState));
  }
  return true;
}

void BrnrBackoff::setState(BrnrState newState) {
  CHECK(canTransitionBrnr(state_, newState));
  state_ = newState;
}

void BrnrBackoff::blockAt(uint16_t msn) {
  setState(BrnrState::BRNR_BLOCKED_TX);
  if (!firstBlockedMsn_.has_value() || before(msn, firstBlockedMsn_.value())) {
    firstBlockedMsn_ = msn;
  }
}

void BrnrBackoff::beginProbe() {
  ++retryCount_;
  setState(BrnrState::BRNR_PROBING);
}

void BrnrBackoff::probeSent() {
  setState(BrnrState::BRNR_PROBE_SENT);
}

void BrnrBackoff::resume() {
  setState(BrnrState::NORMAL_TX);
  firstBlockedMsn_.reset();
  retryCount_ = 0;
  canceledThroughCsn_.reset();
  canceledAtNs_ = 0;
}

void BrnrBackoff::cancelThrough(uint16_t postedCsn, uint32_t nowNs) {
  // Cancels are unsequenced, so a lower csn can arrive after a higher one.
  if (canceledThroughCsn_.has_value() &&
      before(postedCsn, canceledThroughCsn_.value())) {
    return;
  }
  setState(BrnrState::BRNR_CANCELED);
  firstBlockedMsn_.reset();
  retryCount_ = 0;
  canceledThroughCsn_ = postedCsn;
  canceledAtNs_ = nowNs;
}

bool BrnrBackoff::cancelCovers(uint16_t csn, uint32_t nowNs, uint32_t withinNs)
    const {
  const bool holdsACancel = canceledThroughCsn_.has_value();
  const bool cancelIsStale =
      static_cast<uint32_t>(nowNs - canceledAtNs_) > withinNs;
  return holdsACancel && !cancelIsStale &&
      beforeOrEqual(csn, canceledThroughCsn_.value());
}

bool BrnrBackoff::isBlockedMsn(uint16_t msn) const noexcept {
  return firstBlockedMsn_.has_value() &&
      afterOrEqual(msn, firstBlockedMsn_.value());
}

} // namespace libsoftmetaroce
