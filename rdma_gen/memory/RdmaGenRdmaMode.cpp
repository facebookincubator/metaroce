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

#include "rdma_gen/memory/RdmaGenRdmaMode.h"

#include <algorithm>
#include <unordered_map>

#include <glog/logging.h>

#include <fmt/format.h>

namespace facebook::neteng::ai::rdma_gen {

constexpr std::string_view kCpuRdma = "cpu";
constexpr std::string_view kGpuRdma = "gpu";
constexpr std::string_view kRocmRdma = "rocm";

RdmaMode::Value rdmaModeFromString(std::string mode) {
  static const std::unordered_map<std::string_view, RdmaMode::Value>
      kRdmaModeMap{
          {kCpuRdma, RdmaMode::CpuRdma},
          {kGpuRdma, RdmaMode::GpuRdma},
          {kRocmRdma, RdmaMode::RocmRdma},
      };
  std::transform(mode.begin(), mode.end(), mode.begin(), ::tolower);

  if (!kRdmaModeMap.contains(mode)) {
    LOG(FATAL) << fmt::format("Unknown RDMA mode: {}", mode);
  }
  return kRdmaModeMap.at(mode);
}

std::string_view rdmaModeToString(const RdmaMode::Value& mode) {
  switch (mode) {
    case RdmaMode::CpuRdma:
      return kCpuRdma;
    case RdmaMode::GpuRdma:
      return kGpuRdma;
    case RdmaMode::RocmRdma:
      return kRocmRdma;
    default:
      LOG(FATAL) << "Unknown RDMA Mode";
  }
}

} // namespace facebook::neteng::ai::rdma_gen
