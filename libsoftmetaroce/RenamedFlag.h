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

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <stdexcept>
#include <string>

namespace libsoftmetaroce {

// gflags clears is_default once anything names the flag, so an operator who
// chose the default value explicitly still counts as having set it.
inline bool flagIsSet(const char* name) {
  return !gflags::GetCommandLineFlagInfoOrDie(name).is_default;
}

inline std::string flagDefault(const char* name) {
  return gflags::GetCommandLineFlagInfoOrDie(name).default_value;
}

// Keyed by name so that pairs sharing a value type do not silence each other.
bool claimRenamedFlagWarning(const char* formerName);

/* Returns whichever of the two names the operator set, and throws when both are
   set and disagree: dropping a value someone wrote is how a stale config goes
   unnoticed.

   Resolve on the path that uses the value. A constructor can run inside a flag
   validator, which already holds the gflags lock this needs. */
template <typename T>
T resolveRenamedFlag(
    const char* currentName,
    const T& currentValue,
    const char* formerName,
    const T& formerValue) {
  if (!flagIsSet(formerName)) {
    return currentValue;
  }
  if (flagIsSet(currentName) && currentValue != formerValue) {
    throw std::runtime_error(
        fmt::format(
            "--{}={} and --{}={} are two names for one setting and disagree; "
            "set one of them",
            currentName,
            currentValue,
            formerName,
            formerValue));
  }
  if (claimRenamedFlagWarning(formerName)) {
    LOG(WARNING) << fmt::format(
        "--{} is now --{}; the former name still resolves, and stops being "
        "accepted once the rename finishes",
        formerName,
        currentName);
  }
  return formerValue;
}

} // namespace libsoftmetaroce

// Defines a flag under its current name and under the name it replaces, from
// one default and one validator, so the pair cannot drift apart. `type` is a
// gflags suffix: uint32, int32, string.
#define LSMR_DEFINE_RENAMED_FLAG(                          \
    type, name, formerName, defaultValue, validator, help) \
  DEFINE_##type(name, defaultValue, help);                 \
  DEFINE_validator(name, validator);                       \
  DEFINE_##type(                                           \
      formerName,                                          \
      defaultValue,                                        \
      "Former name of --" #name                            \
      ", still accepted. Set the new name "                \
      "instead.");                                         \
  DEFINE_validator(formerName, validator)

// Reads a pair defined by LSMR_DEFINE_RENAMED_FLAG. Every call asks gflags
// which of the two names were set, so a caller on a hot path holds the answer
// rather than resolving per packet.
#define LSMR_RESOLVE_RENAMED_FLAG(name, formerName) \
  ::libsoftmetaroce::resolveRenamedFlag(            \
      #name, FLAGS_##name, #formerName, FLAGS_##formerName)
