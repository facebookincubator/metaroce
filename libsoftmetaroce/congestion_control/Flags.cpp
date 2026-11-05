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
#include <glog/logging.h>

#include <libsoftmetaroce/RenamedFlag.h>
#include <libsoftmetaroce/congestion_control/CongestionControllerFactory.h>
#include <libsoftmetaroce/congestion_control/FastStart.h>
#include <libsoftmetaroce/congestion_control/Flags.h>

namespace {
bool validateUint16Range(const char* flagname, uint32_t value) {
  if (value >= 1 && value <= 65535) {
    return true;
  }
  LOG(ERROR) << "Invalid value for --" << flagname << ": " << value
             << ". Must be in range [1, 65535] to fit in uint16_t.";
  return false;
}

bool validatePositive(const char* flagname, int32_t value) {
  if (value > 0) {
    return true;
  }
  LOG(ERROR) << "Invalid value for --" << flagname << ": " << value
             << ". Must be positive.";
  return false;
}

bool validateCcAlgorithm(const char* flagname, const std::string& spec) {
  // Reject an unknown algorithm or malformed args at startup (an invalid CLI
  // argument) rather than silently falling back to a default.
  if (libsoftmetaroce::CongestionControllerFactory{}.makeCongestionController(
          spec) != nullptr) {
    return true;
  }
  LOG(ERROR) << "Invalid value for --" << flagname << ": '" << spec
             << "'. Expected <name>[:arg,...] with name one of baseline_rcn, "
                "baseline, faststart, static.";
  return false;
}
} // namespace

// Flags for FastStart congestion controller
DEFINE_uint32(
    libsoftmetaroce_fast_start_burst,
    4,
    "Number of packets to send in a burst during fast start phase");
DEFINE_validator(libsoftmetaroce_fast_start_burst, &validateUint16Range);

LSMR_DEFINE_RENAMED_FLAG(
    uint32,
    libsoftmetaroce_max_fwnd,
    libsoftmetaroce_max_pwnd,
    128,
    &validateUint16Range,
    "Default maximum flow window (fwnd) size");

// Flags for Baseline congestion controller
DEFINE_int32(
    libsoftmetaroce_min_qwnd,
    1,
    "Minimum QWND value for congestion control");

DEFINE_int32(
    libsoftmetaroce_fwnd_floor,
    2,
    "Flow window below which RCN stops reducing a protected flow. A floor, "
    "not a minimum: congestion response may still take a flow below it. "
    "Reasonable values lie between the minimum QWND and the fast start "
    "burst.");

DEFINE_int32(
    libsoftmetaroce_max_qwnd,
    0,
    "Maximum QWND value. 0 means unlimited (use computed value).");

DEFINE_int32(
    libsoftmetaroce_initial_qwnd_max,
    libsoftmetaroce::FastStart::kDefaultQwndMax,
    "QWND in packets at which fast start ends.");
DEFINE_validator(libsoftmetaroce_initial_qwnd_max, &validatePositive);

// Congestion-control selection, used to run the "raise the window above what it
// needs" experiment: --libsoftmetaroce_cc_algorithm=static:<large> makes the
// flow window a large constant, so a throughput rise vs the default
// (baseline_rcn) implies the workload was congestion-window-bound.
DEFINE_string(
    libsoftmetaroce_cc_algorithm,
    "baseline_rcn",
    "Congestion control as <name>[:arg,...]: baseline_rcn[:epsilon,beta,"
    "gammaNumerator,omegaNs,lambda] | baseline[:epsilon,beta,lambda] | "
    "faststart | static[:fwnd]. beta (ECN) and lambda (loss) are decreases as "
    "an inverse log2 (0 = a full packet per marked or lost packet, 1 = half); "
    "gammaNumerator is a sixteenth (16 = 1.0) and must be nonzero; omegaNs is "
    "RTT headroom in nanoseconds. Omitted trailing args use the default (e.g. "
    "static:128, baseline:2,1).");
DEFINE_validator(libsoftmetaroce_cc_algorithm, &validateCcAlgorithm);

namespace libsoftmetaroce {

uint16_t maxFwndFlag() {
  return static_cast<uint16_t>(LSMR_RESOLVE_RENAMED_FLAG(
      libsoftmetaroce_max_fwnd, libsoftmetaroce_max_pwnd));
}

} // namespace libsoftmetaroce
