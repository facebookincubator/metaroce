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

#include <gflags/gflags.h>
#include <cstdint>
#include <optional>

// Common flags for both clients and servers.
DECLARE_string(op_type);
DECLARE_string(verbs_mode);
DECLARE_string(platform_type);
DECLARE_string(ib_dev);
DECLARE_string(ib_port);
DECLARE_string(message_size);
DECLARE_uint32(batch_size);
DECLARE_uint32(wr_batch_size);
DECLARE_uint32(buffer_cap);
DECLARE_uint32(service_level);
DECLARE_string(gid_idx);
DECLARE_string(gid_addresses);
DECLARE_int32(gpu_idx);
DECLARE_bool(prefix_logs);
DECLARE_bool(use_event);
DECLARE_bool(use_block);
DECLARE_bool(single_cq_thread);
DECLARE_int32(cq_pcq_depth);
DECLARE_string(json_path);
DECLARE_string(mtu);
// Only affects read operation
DECLARE_uint32(max_rd);
// Only affects write immediate operation
DECLARE_uint32(writes_before_imm);
DECLARE_uint32(min_rnr_timer);
DECLARE_uint32(rnr_retry);
DECLARE_uint32(retry_cnt);
DECLARE_uint32(psn);
DECLARE_bool(use_qpt_driver);
DECLARE_bool(use_huge_pages);
DECLARE_bool(enable_pcie_relaxed);
DECLARE_uint32(sqp_per_qp);
DECLARE_uint32(tx_path_count);
DECLARE_uint32(rx_path_count);
DECLARE_int32(wqe_error_exit_code);
DECLARE_int32(metadata_exchange_failure_exit_code);
DECLARE_bool(verify_payload_exchange);
DECLARE_string(payload_exchange_pattern);
DECLARE_bool(randomize_payload);
DECLARE_int64(random_seed);
DECLARE_string(completion_hooks);
DECLARE_int32(completion_hook_timeout_sec);

// Flags for server only.
DECLARE_int32(rc_cm_port);
DECLARE_uint32(qp_cap);
DECLARE_int32(num_clients_to_complete);
DECLARE_int32(num_completions_expected);
DECLARE_bool(validate_immediate_data);
DECLARE_int32(completion_barrier_timeout_secs);
DECLARE_bool(use_completion_barrier);

// Flags for client only.
DECLARE_int32(rc_connect_timeout_ms);
DECLARE_string(rc_connect_ports);
DECLARE_string(num_messages);
DECLARE_bool(equal_messages_per_server);
DECLARE_string(rc_servers);
DECLARE_string(cos);
DECLARE_int32(duration_secs);
DECLARE_int32(sample_int);
DECLARE_bool(log_msg_latencies);
DECLARE_string(target_qp_state);
DECLARE_bool(measure_cp_latency);

namespace facebook::neteng::ai::rdma_gen {

// Default value for FLAGS_qp_cap, defined here so that the DEFINE_uint32
// site in RdmaGenFlags.cpp and validation code share a single source of truth.
constexpr uint32_t kDefaultQpCap = 1;

/**
 * Helper function to check if a command-line flag was explicitly set by the
 * user and return its value if it was set otherwise return std::nullopt
 *
 * @tparam T The type of the flag value
 * @param flagName The name of the flag to check
 * @param flagValue The current value of the flag (FLAGS_<flagName>)
 * @return std::optional<T> containing the flag value if it was explicitly set,
 *         std::nullopt otherwise
 */
template <typename T>
std::optional<T> getFlagValueIfSet(
    const std::string& flagName,
    const T& flagValue) {
  gflags::CommandLineFlagInfo flagInfo;
  if (gflags::GetCommandLineFlagInfo(flagName.c_str(), &flagInfo) &&
      !flagInfo.is_default) {
    return flagValue;
  }
  return std::nullopt;
}

} // namespace facebook::neteng::ai::rdma_gen
