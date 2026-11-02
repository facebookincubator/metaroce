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

#include "rdma_gen/transport/RdmaGenVerbsMode.h"

#include <algorithm>
#include <unordered_map>

#include <glog/logging.h>

#include <fmt/format.h>

namespace facebook::neteng::ai::rdma_gen {

constexpr std::string_view kRdmaCore = "rdmacore";
constexpr std::string_view kLibSoftMetaRoCE = "libsoftmetaroce";

VerbsMode::Value verbsModeFromString(std::string mode) {
  static const std::unordered_map<std::string_view, VerbsMode::Value>
      kVerbsModeMap{
          {kRdmaCore, VerbsMode::RdmaCore},
          {kLibSoftMetaRoCE, VerbsMode::LibSoftMetaRoCE},
      };
  std::transform(mode.begin(), mode.end(), mode.begin(), ::tolower);

  if (!kVerbsModeMap.contains(mode)) {
    LOG(FATAL) << fmt::format("Unknown Verbs mode: {}", mode);
  }
  return kVerbsModeMap.at(mode);
}

std::string_view verbsModeToString(const VerbsMode::Value& mode) {
  switch (mode) {
    case VerbsMode::RdmaCore:
      return kRdmaCore;
    case VerbsMode::LibSoftMetaRoCE:
      return kLibSoftMetaRoCE;
    default:
      LOG(FATAL) << "Unknown RDMA Mode";
  }
}

} // namespace facebook::neteng::ai::rdma_gen
