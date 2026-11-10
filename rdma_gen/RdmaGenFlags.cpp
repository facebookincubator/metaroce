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

#include "RdmaGenFlags.h"

// Common flags for both clients and servers.
DEFINE_string(
    op_type,
    "send_recv",
    "Type of operation: send_recv, write, write_imm, read, atomic_fetch_add, or atomic_cmp_swap");
DEFINE_string(
    verbs_mode,
    "rdmacore",
    "Controls what verbs implementation is used: either 'rdmacore' (libibverbs), or 'libsoftmetaroce'");
DEFINE_string(platform_type, "", "Platform type.");
DEFINE_string(ib_dev, "", "IB device. null if not specified.");
DEFINE_string(
    ib_port,
    "1",
    "Comma-separated list of port numbers for IB device (e.g., '1' or '1,2,3')");
DEFINE_string(
    message_size,
    "64000",
    "Message size-can be array of sizes per connection. If too short will be extended with last value");
DEFINE_uint32(batch_size, 1, "Number of CQs that could be polled in a batch");
DEFINE_uint32(wr_batch_size, 1, "Number of work requests in a batch");
DEFINE_uint32(buffer_cap, 500, "Number of receives to post at a time");
DEFINE_uint32(service_level, 0, "Service level for RoCE messages");
DEFINE_string(
    gid_idx,
    "",
    "Local port GID indices, comma-separated. When multiple GIDs are specified (e.g., '0,1,2'), "
    "multiport traffic will be enabled and all indices will be used to establish multiple paths "
    "for RDMA connections. Default (empty) uses auto-detection. "
    "Note: in libsoftmetaroce mode, gid indices reflect the order returned by getifaddrs() and "
    "can vary between hosts. Use --libsoftmetaroce_gid to pin the GID table to a specific list "
    "of IP addresses for deterministic indexing.");
DEFINE_string(
    gid_addresses,
    "",
    "Local port GID IPv6 addresses, comma-separated. Works like --gid_idx but specifies "
    "GIDs by IPv6 address instead of by index. The GID table is scanned to find the "
    "matching index for each address. Cannot be used together with --gid_idx.");
DEFINE_int32(gpu_idx, 0, "GPU idx to use");
DEFINE_bool(prefix_logs, false, "Prefix logs with server or client role");
DEFINE_bool(use_event, false, "Sleep on CQ events. Poll otherwise.");
DEFINE_bool(use_block, false, "Use single MR-intended for GPU perf");
DEFINE_bool(
    single_cq_thread,
    false,
    "Single thread for CQ processing-may increase latency for small messages, but less threads needed");
DEFINE_int32(
    cq_pcq_depth,
    20,
    "producer consumer queue depth for multi-threaded CQ processing");
DEFINE_string(json_path, "", "Output location for the result JSON file");
DEFINE_string(
    mtu,
    "IBV_MTU_4096",
    "mtu to be used, if multiple split with commas. Support IBV_MTU_256, IBV_MTU_512,"
    " IBV_MTU_1024, IBV_MTU_2048, IBV_MTU_4096.");
// Only affects read operation
DEFINE_uint32(
    max_rd,
    0,
    "Number of outstanding RDMA reads for both directions");
// Only affects write immediate operation
DEFINE_uint32(
    writes_before_imm,
    0,
    "Number of write operations followed by a single write immediate.");
DEFINE_uint32(min_rnr_timer, 12, "RNR NAK Timer");
// The maximum legal value for min_rnr_timer is 31. Unfortunately there is no
// enum or anything available, so we need to use the numeric value.
DEFINE_validator(min_rnr_timer, [](const char* _, const uint32_t value) {
  return value <= 31;
});
DEFINE_uint32(
    rnr_retry,
    7,
    "RNR retry count, 0-7. 7 requests infinite retries, in which case no "
    "RNR-retry-exceeded error is ever reported. Set 0-6 for a finite limit.");
// rnr_retry is a 3-bit IB field, so 7 is the maximum legal value.
DEFINE_validator(rnr_retry, [](const char* _, const uint32_t value) {
  return value <= 7;
});
DEFINE_uint32(
    retry_cnt,
    7,
    "Transport retry count, 0-7. 7 requests infinite retries, "
    "in which case no retry-exceeded error is ever reported. Set 0-6 for a "
    "finite limit.");
// retry_cnt is a 3-bit IB field, so 7 is the maximum legal value.
DEFINE_validator(retry_cnt, [](const char* _, const uint32_t value) {
  return value <= 7;
});
DEFINE_uint32(
    psn,
    0,
    "Base sequence number for QP initialization. Verbs transports use the "
    "low 24 bits as the PSN; MetaRoCE interprets bits [15:0] as MSN and "
    "[31:16] as CSN. If specified, each QP uses psn + qp_index.");
// Flags for server only.
DEFINE_int32(rc_cm_port, 18515, "TCP server port to Listen on/connect");
DEFINE_uint32(
    qp_cap,
    facebook::neteng::ai::rdma_gen::kDefaultQpCap,
    "Number of qps to use (server only)");
DEFINE_int32(
    num_clients_to_complete,
    1,
    "Number clients that this receiver expects to serve before self termination");
DEFINE_int32(
    num_completions_expected,
    -1,
    "Limit to the number of receive WQEs that will be posted.  If negative, it will post unlimited receive WQEs, until the test is stopped.");
DEFINE_bool(
    validate_immediate_data,
    true,
    "Stop server if immediate data is received out of order");
DEFINE_int32(
    completion_barrier_timeout_secs,
    30,
    "Seconds to wait at the end-of-test completion barrier before giving up");
DEFINE_bool(
    use_completion_barrier,
    false,
    "If true, the client blocks on a per-server completion barrier RPC before "
    "sending terminate, so the server has a chance to drain all expected RX "
    "completions. If false (default), the client skips the barrier and sends "
    "terminate immediately, preserving the pre-barrier behaviour.");

// Flags for client only.
DEFINE_int32(
    rc_connect_timeout_ms,
    60000,
    "Absolute timeout in milliseconds for the per-QP Thrift control "
    "connection retry loop.");
DEFINE_string(
    rc_connect_ports,
    "18515",
    "TCP server ports to rc_servers list."

    "Use format <port>#<repeat> to denote the number of times to repeat port");
DEFINE_string(
    num_messages,
    "0",
    "Number of messages per server, split by comma");
DEFINE_bool(
    equal_messages_per_server,
    true,
    "If true, each server should get exactly --num_messages. If false, --num_messages is treated as the average messages per server. Setting this to false will be ignored if array specified in num_messages");
DEFINE_string(
    rc_servers,
    "",
    "Servers to send to, if multiple split with commas."
    "Use format <server>#<repeat> to denote the number of times to repeat server");
DEFINE_string(
    cos,
    "",
    "Traffic classes, if multiple split with commas. Will default to all 0");
DEFINE_int32(duration_secs, 30, "Number of seconds to run test-0 == infinite");
DEFINE_int32(sample_int, 1, "Number of seconds per sample");
DEFINE_bool(log_msg_latencies, false, "Log percentiles of message latencies");
DEFINE_string(
    target_qp_state,
    "",
    "End state of QP, (client only) for control plane level testing. Accompany with num_messages=0 ");
DEFINE_bool(
    measure_cp_latency,
    true,
    "If true, we measure the time taken to create QPs. Useful in combination with target_qp_state & bulk QP creation");
DEFINE_bool(
    use_qpt_driver,
    false,
    "If true, we create IBV_QPT_DRIVER for the QP type. If false, we use back the default IBV_QPT_RC.");
DEFINE_bool(
    use_huge_pages,
    false,
    "If true, we provide hints using madvise for 2MB Page Size.");
DEFINE_bool(
    enable_pcie_relaxed,
    false,
    "If true, register memory regions with IBV_ACCESS_RELAXED_ORDERING.");
DEFINE_uint32(sqp_per_qp, 1, "Number of SQPs (tags) per QP.");
DEFINE_validator(sqp_per_qp, [](const char*, const uint32_t value) {
  return value >= 1;
});
DEFINE_uint32(tx_path_count, 1, "Number of transmit paths per QP.");
DEFINE_validator(tx_path_count, [](const char*, const uint32_t value) {
  return value >= 1 && value <= 255;
});
DEFINE_uint32(rx_path_count, 1, "Number of receive paths per QP.");
DEFINE_validator(rx_path_count, [](const char*, const uint32_t value) {
  return value >= 1 && value <= 255;
});
DEFINE_int32(
    wqe_error_exit_code,
    0,
    "Exit code to use when a WQE completes with an error. If non-zero, the program will exit with this value after showing instrumentation.");
DEFINE_int32(
    metadata_exchange_failure_exit_code,
    0,
    "Exit code to use when metadata exchange with server fails. "
    "If non-zero, the program will exit with this value. "
    "This includes failures due to network issues (e.g., no route to host), "
    "timeouts during the metadata exchange retry loop, or server unavailability.");
DEFINE_bool(
    verify_payload_exchange,
    false,
    "Verify that data exchanged via RDMA matches the expected pattern. "
    "When used without --payload_exchange_pattern, a deterministic per-MTU "
    "character pattern (a-z,A-Z,1-9) is generated and verified. Must be used "
    "symmetrically at all endpoints to enforce the payload that is expected "
    "to be verified (symmetricity validated via OOB metadata exchange)");
DEFINE_string(
    payload_exchange_pattern,
    "",
    "Path to a file whose content is used as the RDMA payload. "
    "When used with --verify_payload_exchange, the same file must be used "
    "on all endpoints (validated via OOB metadata exchange). Can be used "
    "independently of --verify_payload_exchange to fill buffers without "
    "verification.");
DEFINE_bool(
    randomize_payload,
    false,
    "Fill RDMA send buffers with pseudo-random data instead of zeros. "
    "Mutually exclusive with --verify_payload_exchange and "
    "--payload_exchange_pattern.");
DEFINE_int64(
    random_seed,
    -1,
    "Seed for pseudo-random payload generation (requires --randomize_payload). "
    "If negative, a random seed is used.");
DEFINE_string(
    completion_hooks,
    "",
    "Comma-delimited, ordered list of 'threshold:command' hooks that fire based "
    "on the observed work-completion count (client/requester only). When the "
    "completion count reaches a hook's <threshold> (a leading positive integer), "
    "its <command> (everything after the first ':') is run via '/bin/sh -c' and "
    "the completion poll loop blocks until it exits; a non-zero exit, kill, or "
    "timeout aborts the run. Hooks are sorted ascending by threshold regardless "
    "of input order. Blocking is deliberate so the command runs at a "
    "deterministic completion count. Caveat: ',' is the entry delimiter, so a "
    "command that needs a literal comma must be wrapped in a small script. "
    "Example: '1000:/path/link_down.sh 0 3,5000:/path/link_up.sh 0 3'. "
    "Empty (default) disables this behavior.");
DEFINE_int32(
    completion_hook_timeout_sec,
    0,
    "Per-hook wall-clock timeout in seconds for --completion_hooks commands. "
    "0 (default) waits indefinitely. When > 0, a hook that does not exit within "
    "this many seconds is terminated/killed and aborts the run.");
