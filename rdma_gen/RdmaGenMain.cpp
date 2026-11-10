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

#include "rdma_gen/RdmaGenMain.h"

#include <cstdint>

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "rdma_gen/RdmaGenClientServerMain.h"
#include "rdma_gen/RdmaGenFlags.h"
#include "rdma_gen/parsers/OperationTypeParser.h"
#include "rdma_gen/transport/GetTransportProvider.h"

DEFINE_string(
    rdma_mode,
    "cpu",
    "Controls what memory is used: either 'cpu' (CPU host memory RDMA), or 'gpu' (GPU-Direct RDMA)");

namespace facebook::neteng::ai::rdma_gen {

Params buildParamsFromFlags() {
  Params params(
      {.rdma_mode = rdmaModeFromString(FLAGS_rdma_mode),
       .verbs_mode = verbsModeFromString(FLAGS_verbs_mode),
       .ib_dev_name = FLAGS_ib_dev,
       .rc_cm_port = FLAGS_rc_cm_port,
       .ib_ports = {},
       .message_size = {},
       .batch_size = FLAGS_batch_size,
       .service_level = static_cast<uint8_t>(FLAGS_service_level),
       .gid_idx = {},
       .gpu_idx = FLAGS_gpu_idx,
       .use_event = FLAGS_use_event,
       .use_block = FLAGS_use_block,
       .max_rd = static_cast<uint8_t>(FLAGS_max_rd),
       .min_rnr_timer = static_cast<uint8_t>(FLAGS_min_rnr_timer),
       .rnr_retry = static_cast<uint8_t>(FLAGS_rnr_retry),
       .retry_cnt = static_cast<uint8_t>(FLAGS_retry_cnt),
       .writes_before_imm = FLAGS_writes_before_imm,
       .num_completions_expected = FLAGS_num_completions_expected,
       .validate_immediate_data = FLAGS_validate_immediate_data,
       .psn = getFlagValueIfSet("psn", FLAGS_psn),
       .use_qpt_driver = FLAGS_use_qpt_driver,
       .use_huge_pages = FLAGS_use_huge_pages,
       .relaxed_ordering = FLAGS_enable_pcie_relaxed,
       .sqp_per_qp = static_cast<uint8_t>(FLAGS_sqp_per_qp),
       .tx_path_count = static_cast<uint8_t>(FLAGS_tx_path_count),
       .rx_path_count = static_cast<uint8_t>(FLAGS_rx_path_count),
       .completion_hooks = FLAGS_completion_hooks,
       .completion_hook_timeout_sec = FLAGS_completion_hook_timeout_sec});

  if (!FLAGS_gid_idx.empty()) {
    folly::split(',', FLAGS_gid_idx, params.gid_idx);
  }

  if (!FLAGS_gid_addresses.empty()) {
    CHECK(FLAGS_gid_idx.empty())
        << "--gid_addresses and --gid_idx cannot be used together";
    folly::split(',', FLAGS_gid_addresses, params.gid_addresses);
  }

  {
    std::vector<int> parsedPorts;
    folly::split(',', FLAGS_ib_port, parsedPorts);
    for (auto p : parsedPorts) {
      params.ib_ports.push_back(static_cast<uint8_t>(p));
    }
  }
  CHECK(!params.ib_ports.empty()) << "ib_port must specify at least one port";

  try {
    params.platform_type = platformTypeFromString(FLAGS_platform_type);
  } catch (...) {
    params.platform_type = PlatformType::UNKNOWN;
    LOG(INFO) << "Unknown platform type";
  }

  return params;
}

} // namespace facebook::neteng::ai::rdma_gen

using namespace facebook::neteng::ai::rdma_gen;

int RdmaGenMain(int argc, char* argv[]) {
  LOG(INFO) << fmt::format("Command line: {}", getCommandLineStr(argc, argv));
  folly::Init init(&argc, &argv);

  Params params = buildParamsFromFlags();
  return runClientOrServer(
      params,
      parseOperationType(FLAGS_op_type),
      RdmaGenResources{.transport_provider = getTransportProvider(params)});
}
