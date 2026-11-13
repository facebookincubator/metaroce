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

#include "rdma_gen/RdmaGenClientServerMain.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <optional>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <re2/re2.h>
#include <numeric>

#include <folly/String.h>
#include <folly/logging/LoggerDB.h>

#include "libsoftmetaroce/ClockSource.h"
#include "rdma_gen/Common.h"
#include "rdma_gen/PayloadPattern.h"
#include "rdma_gen/RdmaGenFlags.h"
#include "rdma_gen/RdmaGenMtu.h"
#include "rdma_gen/RdmaGenReadResponder.h"
#include "rdma_gen/RdmaGenReceiver.h"
#include "rdma_gen/RdmaGenRequesterFactory.h"
#include "rdma_gen/RdmaGenResponderFactory.h"
#include "rdma_gen/RdmaGenStallWatchdog.h"
#include "rdma_gen/RdmaGenWriteResponder.h"

namespace facebook::neteng::ai::rdma_gen {

// 6-bit DSCP values are between 0 and 63 (inclusive).
constexpr int kMaxDscpValue = 63;

// Exit status when --stall_timeout_secs expires.
constexpr int kStallExitCode = 3;

namespace {

bool hasOutstandingSends(const RdmaGenRequester& client) {
  const auto counts = client.waitingCompletionCounts();
  return std::any_of(
      counts.begin(), counts.end(), [](uint32_t count) { return count > 0; });
}

[[noreturn]] void exitOnStall(
    const RdmaGenRequester& client,
    std::chrono::milliseconds flat) {
  LOG(ERROR) << fmt::format(
      "Stalled: no send completion for {} ms while sends are outstanding "
      "(--stall_timeout_secs={}); completions so far: {}; outstanding per QP: "
      "[{}]",
      flat.count(),
      FLAGS_stall_timeout_secs,
      client.numCompletions(),
      folly::join(", ", client.waitingCompletionCounts()));
  google::FlushLogFiles(google::GLOG_INFO);
  std::_Exit(kStallExitCode);
}

} // namespace

int runClientOrServer(
    Params& params,
    OperationType opType,
    RdmaGenResources&& resources) {
  if (!FLAGS_rc_servers.empty()) {
    return clientMain(params, opType, resources);
  }
  // Common and role-specific params are not separated here.
  params.rx_depth = FLAGS_buffer_cap;
  return serverMain(params, opType, resources);
}

// Helper to check for WQE errors and return appropriate exit code
template <typename T>
int exitCodeForWqeErrors(const T& endpoint) {
  if (endpoint->hasWqeErrors()) {
    LOG(WARNING) << "WQE errors detected, exiting with code "
                 << FLAGS_wqe_error_exit_code;
    return FLAGS_wqe_error_exit_code;
  }
  return 0;
}

std::string getCommandLineStr(int argc, char* argv[]) {
  std::string out;
  for (int i = 0; i < argc; ++i) {
    out.append(argv[i]);
    out.append(" ");
  }
  return out;
}

int clientMain(
    Params& params,
    OperationType opType,
    RdmaGenResources& resources) {
  // Construct sending RC client.

  ClientParams clientParams(
      {.logMsgLatencies = FLAGS_log_msg_latencies,
       .equalMessagesPerServer = FLAGS_equal_messages_per_server,
       .outputJsonPath = FLAGS_json_path,
       .sampleRate = FLAGS_sample_int});

  params.tx_depth = FLAGS_buffer_cap;
  params.rx_depth = 0;
  params.single_cq_thread = FLAGS_single_cq_thread;
  params.cq_pcq_depth = FLAGS_cq_pcq_depth;

  std::vector<std::string> ports_str;
  std::vector<std::string> cos_str;
  std::vector<std::string> mtu_per_server;
  std::vector<std::string> servers_str;

  // Param : servers/ports list can be of format "<string>#<number>"
  // hence defining valid specifier to be '2' for the check.
  constexpr int kParamStrId = 0;
  constexpr int kParamStrCount = 1;
  constexpr int kParamStrPairSize = 2;

  if (FLAGS_prefix_logs) {
    folly::LoggerDB::get().addContextCallback(LogHelper::logPrefixClient);
  }

  folly::split(',', FLAGS_rc_servers, servers_str);
  // Expand the server names based on # of times to repeat.
  for (int i = 0; i < servers_str.size(); i++) {
    static const RE2 myRegex("#");
    if (RE2::PartialMatch(servers_str[i], myRegex)) {
      std::vector<std::string> server_and_count;
      folly::split('#', servers_str[i], server_and_count);
      try {
        if (server_and_count.size() == kParamStrPairSize) {
          int cnt = std::stoi(server_and_count[kParamStrCount]);
          for (int j = 0; j < cnt; j++) {
            clientParams.servers.push_back(server_and_count[kParamStrId]);
          }
        } else {
          LOG(ERROR) << fmt::format(
              "Error parsing parameter: {}", servers_str[i]);
        }
      } catch (...) {
        LOG(ERROR) << fmt::format(
            "{} is an invalid input for the number of servers", servers_str[i]);
        return -1;
      }
    } else {
      clientParams.servers.push_back(servers_str[i]);
    }
  }

  // Expand the port number based on # of times to repeat.
  folly::split(',', FLAGS_rc_connect_ports, ports_str);
  for (int i = 0; i < ports_str.size(); i++) {
    static const RE2 myRegex("#");
    if (RE2::PartialMatch(ports_str[i], myRegex)) {
      std::vector<std::string> port_and_count;
      folly::split('#', ports_str[i], port_and_count);
      try {
        if (port_and_count.size() == kParamStrPairSize) {
          int cnt = std::stoi(port_and_count[kParamStrCount]);
          for (int j = 0; j < cnt; j++) {
            clientParams.ports.push_back(
                std::stoi(port_and_count[kParamStrId]));
          }
        } else {
          LOG(ERROR) << fmt::format(
              "Error parsing parameter: {}", ports_str[i]);
        }
      } catch (...) {
        LOG(ERROR) << fmt::format(
            "{} is an invalid input for the number of ports", ports_str[i]);
        return -1;
      }
    } else {
      clientParams.ports.push_back(std::stoi(ports_str[i]));
    }
  }

  if (!FLAGS_cos.empty()) {
    folly::split(',', FLAGS_cos, cos_str);
  }

  clientParams.mtus = parseMtuFlagList(FLAGS_mtu, clientParams.servers.size());

  // We currently support only the following states:
  // By default the state will be READY TO SEND.
  clientParams.targetQpState = IBV_QPS_RTS;
  if (!FLAGS_target_qp_state.empty()) {
    try {
      if (FLAGS_target_qp_state == "RESET") {
        clientParams.targetQpState = IBV_QPS_RESET;
      } else if (FLAGS_target_qp_state == "INIT") {
        clientParams.targetQpState = IBV_QPS_INIT;
      } else if (FLAGS_target_qp_state == "RTR") {
        clientParams.targetQpState = IBV_QPS_RTR;
      } else if (FLAGS_target_qp_state == "RTS") {
        clientParams.targetQpState = IBV_QPS_RTS;
      } else if (FLAGS_target_qp_state == "ERROR") {
        clientParams.targetQpState = IBV_QPS_ERR;
      } else {
        LOG(ERROR)
            << "Invalid QP state. Sould be one of RESET, INIT, RTR, RTS or ERROR.";
        return -1;
      }
    } catch (...) {
      LOG(ERROR) << "Invalid input for qp state, check the syntax";
      return -1;
    }
  }

  if (!FLAGS_num_messages.empty()) {
    folly::split(',', FLAGS_num_messages, clientParams.numMessages);
  }

  if (!FLAGS_message_size.empty()) {
    folly::split(',', FLAGS_message_size, params.message_size);
  }

  if (clientParams.servers.size() != clientParams.ports.size()) {
    LOG(ERROR) << "list of servers and ports need to match";
    return -1;
  }

  if (clientParams.servers.size() < cos_str.size()) {
    LOG(ERROR) << "List of traffic classes is too long";
    return -1;
  }

  if (clientParams.servers.size() < clientParams.numMessages.size()) {
    LOG(ERROR) << "List of number of messages per server is too long";
    return -1;
  }

  if (clientParams.servers.size() < params.message_size.size()) {
    LOG(ERROR) << "List of message sizes is too long";
    return -1;
  }

  params.qp_cap = static_cast<uint32_t>(clientParams.servers.size());

  for (int i = 0; i < cos_str.size(); i++) {
    try {
      int cos_val = std::stoi(cos_str[i]);
      if ((0 <= cos_val) && (cos_val <= kMaxDscpValue)) {
        clientParams.cos.push_back(cos_val);
      } else {
        LOG(ERROR) << "Traffic classes value too high";
        return -1;
      }
    } catch (...) {
      LOG(ERROR) << "Invalid input for cos, check the syntax";
      return -1;
    }
  }

  if (clientParams.servers.size() > clientParams.cos.size()) {
    LOG(WARNING) << "List of traffic classes is too short, zero extending";
    for (auto i = clientParams.cos.size(); i < clientParams.servers.size();
         i++) {
      clientParams.cos.push_back(0);
    }
  }

  if (clientParams.servers.size() > clientParams.numMessages.size()) {
    LOG(WARNING)
        << "List of number of messages per server is too short, extending final value";
    int64_t nm = clientParams.numMessages.back();
    for (auto i = clientParams.numMessages.size();
         i < clientParams.servers.size();
         i++) {
      clientParams.numMessages.push_back(nm);
    }
  }

  if (clientParams.servers.size() > params.message_size.size()) {
    LOG(WARNING) << "List of message sizes is too short, extending final value";
    int ms = params.message_size.back();
    for (auto i = params.message_size.size(); i < clientParams.servers.size();
         i++) {
      params.message_size.push_back(ms);
    }
  }

  const auto cp_startTime = std::chrono::high_resolution_clock::now();

  CHECK(
      !FLAGS_randomize_payload ||
      (!FLAGS_verify_payload_exchange &&
       FLAGS_payload_exchange_pattern.empty()))
      << "--randomize_payload is mutually exclusive with "
         "--verify_payload_exchange and --payload_exchange_pattern";

  std::vector<uint8_t> fileData;
  if (!FLAGS_payload_exchange_pattern.empty()) {
    fileData = loadFilePattern(FLAGS_payload_exchange_pattern);
  }
  const bool fillPayload = FLAGS_verify_payload_exchange ||
      !FLAGS_payload_exchange_pattern.empty() || FLAGS_randomize_payload;
  if (fillPayload && !FLAGS_randomize_payload) {
    CHECK(!clientParams.mtus.empty()) << "No MTUs configured for payload fill";
  }

  if (FLAGS_randomize_payload) {
    const uint64_t seed = FLAGS_random_seed >= 0
        ? static_cast<uint64_t>(FLAGS_random_seed)
        : folly::Random::rand64();
    seedPayloadRng(seed);
    LOG(INFO) << fmt::format("Randomize payload seed: {}", seed);
  }

  std::unique_ptr<RdmaGenRequester> client;
  try {
    client = RdmaGenRequesterFactory().makeRequester(
        params, opType, clientParams, resources);
  } catch (const std::exception& e) {
    LOG(ERROR) << "Exception when constructing RDMAGen requester: " << e.what();
    client = nullptr;
  }
  if (!client) {
    if (FLAGS_metadata_exchange_failure_exit_code != 0) {
      return FLAGS_metadata_exchange_failure_exit_code;
    }
    return -1;
  }

  // Exchange and validate payload metadata with each server before data
  // transfer
  auto localPayloadMeta = buildPayloadMetadata(
      FLAGS_verify_payload_exchange, FLAGS_payload_exchange_pattern);
  std::vector<std::map<std::string, std::string>> remotePayloadMetaPerServer;
  if (!client->exchangeMetadata(localPayloadMeta, remotePayloadMetaPerServer)) {
    LOG(ERROR) << "Failed to exchange payload metadata with server(s)";
    if (FLAGS_metadata_exchange_failure_exit_code != 0) {
      return FLAGS_metadata_exchange_failure_exit_code;
    }
    return -1;
  }
  for (const auto& remotePayloadMeta : remotePayloadMetaPerServer) {
    auto validationSuccessful =
        validatePayloadMetadata(localPayloadMeta, remotePayloadMeta);
    LOG_IF(FATAL, !validationSuccessful)
        << "Payload metadata validation failed";
  }

  std::optional<double> cp_latencyUsecs;
  if (FLAGS_measure_cp_latency) {
    const auto cp_stopTime = std::chrono::high_resolution_clock::now();
    cp_latencyUsecs =
        duration_cast<std::chrono::microseconds>(cp_stopTime - cp_startTime)
            .count();
    LOG(INFO) << fmt::format(
        "Control plane latency measurement took: {} usecs",
        cp_latencyUsecs.value());
  }

  CHECK_EQ(client->getRemoteRcQpInfoSize(), params.qp_cap)
      << "set up rdma connections failed";
  for (auto i = 0; i < params.qp_cap; i++) {
    LOG(INFO) << fmt::format(
        "Local QP Info: {}",
        RdmaGenEndpoint::printRcQpInfo(client->getLocalRcQpInfo(i)));
    LOG(INFO) << fmt::format(
        "Remote QP Info: {}",
        RdmaGenEndpoint::printRcQpInfo(client->getRemoteRcQpInfo(i)));
  }

  auto clockSource = libsoftmetaroce::getClockSourceInstance();
  const auto start = clockSource->now();
  const auto deadline = start + std::chrono::seconds(FLAGS_duration_secs);
  const auto numEffectiveQps = params.effectiveQpCap();
  std::vector<int> msgSentVec(numEffectiveQps, 0);
  int msgSent = 0;
  std::optional<RdmaGenStallWatchdog> stallWatchdog;
  if (FLAGS_stall_timeout_secs > 0) {
    stallWatchdog.emplace(
        std::chrono::seconds(FLAGS_stall_timeout_secs),
        [&client] { return client->numCompletions(); },
        [&client] { return hasOutstandingSends(*client); },
        [&client](std::chrono::milliseconds flat) {
          exitOnStall(*client, flat);
        });
  }
  const auto writesBeforeImm = params.writes_before_imm;

  // To make linter happy
  CHECK(!msgSentVec.empty());

  const int totalMessages = static_cast<int>(std::reduce(
      clientParams.numMessages.begin(), clientParams.numMessages.end()));
  for (int startServerIdx = 0; msgSent < totalMessages; ++msgSent) {
    if (clockSource->now() > deadline) {
      LOG(INFO) << fmt::format(
          "Exiting send loop: --duration_secs={} expired after {} of {} messages sent",
          FLAGS_duration_secs,
          msgSent,
          totalMessages);
      break;
    }
    auto rcBuf = client->getBuffer(startServerIdx);
    const auto baseIdx = params.baseQpIndex(rcBuf.qp_idx);
    rcBuf.len = params.message_size[baseIdx];
    startServerIdx = (rcBuf.qp_idx + 1) % numEffectiveQps;
    // Only post a write with immediate after sending writesBeforeImm
    // writes.
    if ((opType == OperationType::WriteImmediate ||
         opType == OperationType::SendImmediate) &&
        msgSentVec[rcBuf.qp_idx] % (writesBeforeImm + 1) == writesBeforeImm) {
      rcBuf.maybe_imm_data = msgSentVec[rcBuf.qp_idx] / (writesBeforeImm + 1);
    }
    if (fillPayload && rcBuf.buf != nullptr) {
      if (FLAGS_randomize_payload) {
        fillBufferWithRandom(rcBuf.buf, rcBuf.len);
        VLOG(1) << fmt::format(
            "msg[{}] qp[{}] len={}: {}",
            msgSent,
            rcBuf.qp_idx,
            rcBuf.len,
            hexDumpBuffer(rcBuf.buf, rcBuf.len, 64));
      } else if (!fileData.empty()) {
        // Fill the buffer with the file pattern - Always start from the
        // beginning of the file for each message (fo = 0). Support for
        // spreading the file pattern across multiple messages would be doable,
        // by moving the fo variable to the outer loop, but starting defensively
        // here.
        size_t fo = 0;
        fillBufferFromFilePattern(rcBuf.buf, rcBuf.len, fileData, fo);
      } else {
        size_t ci = 0;
        uint32_t mtuBytes = ibvMtuToBytes(clientParams.mtus.at(baseIdx));
        fillBufferWithPattern(rcBuf.buf, rcBuf.len, mtuBytes, ci);
      }
    }
    client->postBuffer(rcBuf);
    msgSentVec[rcBuf.qp_idx]++;
  }

  LOG(INFO) << fmt::format(
      "{} messages sent/posted, waiting for completion...", msgSent);

  const auto stop = client->stop(msgSent);

  // Get results
  const double runTimeUsecs =
      duration_cast<std::chrono::microseconds>(stop - start).count();
  const double msgtput = (msgSent / runTimeUsecs);
  double gbpstput = 0;
  std::vector<double> msgSentTputVec(numEffectiveQps, 0);

  for (uint32_t eqpIdx = 0; eqpIdx < numEffectiveQps; eqpIdx++) {
    msgSentTputVec[eqpIdx] = (msgSentVec[eqpIdx] / runTimeUsecs);
    gbpstput += (msgSentTputVec[eqpIdx] * 8.0 *
                 params.message_size[params.baseQpIndex(eqpIdx)]) /
        1000.0;
  }

  client->writeJsonReport(
      gbpstput, msgtput, opType, params.verbs_mode, cp_latencyUsecs);

  LOG(INFO) << fmt::format(
      "Done in: {} microseconds at: {:.5g} Mmpps {:.5g} Gbps",
      runTimeUsecs,
      msgtput,
      gbpstput);

  return exitCodeForWqeErrors(client);
}

namespace {
// Give 10 seconds to the server before aborting when the metadata validation is
// failing. We need to give some time to the server before killing it, to allow
// clients to connect and get back the metadata response so that they are also
// aware of the mismatch.
constexpr size_t kAbortTimeSeconds = 10;

void metadataCallback(
    const std::map<std::string, std::string>& clientMetadata,
    const std::map<std::string, std::string>& serverMetadata,
    const std::string& clientAddress) {
  auto validPayloadFlags =
      validatePayloadMetadata(serverMetadata, clientMetadata);
  if (!validPayloadFlags) {
    LOG(ERROR) << fmt::format(
        "Payload metadata validation failed for client: {}", clientAddress);

    static folly::Synchronized<bool> abortIssued{false};
    auto locked = abortIssued.wlock();
    if (*locked) {
      return;
    }
    *locked = true;

    LOG(ERROR) << fmt::format(
        "The instance will abort in {} seconds", kAbortTimeSeconds);

    std::jthread([]() {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::seconds(kAbortTimeSeconds));
      LOG(FATAL) << "Aborting due to metadata validation failure";
    }).detach();
  }
}

const MetadataCallback kMetadataCallbackFn = metadataCallback;
} // namespace

int serverMain(
    Params& params,
    OperationType opType,
    RdmaGenResources& resources) {
  if (FLAGS_prefix_logs) {
    folly::LoggerDB::get().addContextCallback(LogHelper::logPrefixServer);
  }
  // Construct receiving RC server.
  CHECK_EQ(FLAGS_num_messages, "0")
      << "--num_messages must not be set for rc_server";

  params.tx_depth = 0;
  params.qp_cap = FLAGS_qp_cap;

  params.mtu = parseMtuFlagSingle(FLAGS_mtu);
  LOG(INFO) << fmt::format("Using MTU: {}", params.mtu);

  LOG(INFO) << fmt::format(
      "params.message_size.size(): {}", params.message_size.size());

  if (!FLAGS_message_size.empty()) {
    folly::split(',', FLAGS_message_size, params.message_size);
  } else {
    params.message_size.push_back(64000);
  }

  LOG(INFO) << fmt::format(
      "params.message_size.size(): {} FLAGS_message_size: {}",
      params.message_size.size(),
      FLAGS_message_size);

  // making linter happy-this should never trigger unless something is wrong
  assert(params.message_size.size() > 0);

  if (params.message_size.size() < params.qp_cap) {
    LOG(WARNING) << "List of message sizes is too short, extending final value";
    int ms = params.message_size.back();
    for (auto i = params.message_size.size(); i < params.qp_cap; i++) {
      params.message_size.push_back(ms);
    }
  }

  if (params.message_size.size() > params.qp_cap) {
    LOG(ERROR) << "params.message_size.size() longer then params.qp_cap";
    return -1;
  }
  ServerParams serverParams(
      {.numClients = FLAGS_num_clients_to_complete,
       .outputJsonPath = FLAGS_json_path});

  std::unique_ptr<RdmaGenResponder> server =
      RdmaGenResponderFactory().makeResponder(
          params, opType, serverParams, resources);
  if (!server) {
    return -1;
  }

  // Set payload metadata on the control server for OOB exchange with clients
  auto serverPayloadMeta = buildPayloadMetadata(
      FLAGS_verify_payload_exchange, FLAGS_payload_exchange_pattern);
  server->setControlMetadata(serverPayloadMeta, kMetadataCallbackFn);

  std::vector<uint8_t> fileData;
  if (!FLAGS_payload_exchange_pattern.empty()) {
    fileData = loadFilePattern(FLAGS_payload_exchange_pattern);
  }
  const bool verifyPayload = FLAGS_verify_payload_exchange;
  const uint32_t mtuBytes = verifyPayload ? ibvMtuToBytes(params.mtu) : 0;

  switch (opType) {
    case OperationType::SendReceive:
    case OperationType::SendImmediate:
    case OperationType::WriteImmediate: {
      auto serverDerived = (RdmaGenReceiver*)server.get();
      LOG(INFO) << "Number of expected completions (total across QPs): "
                << serverDerived->getNumCompletionsExpectedTotal();
      while (true) {
        auto rcBuf = serverDerived->fetchDataReadyBuffer();
        if (rcBuf.len == 0) {
          // Fast-path completeness sampled before stop(); if it already holds
          // we are done. Otherwise re-check after stop() below: stop() joins
          // the RX poll thread and drains completions that were still in
          // flight, so a pre-stop snapshot can look incomplete under load even
          // when every expected message ultimately arrives.
          const bool rxComplete = serverDerived->rxAccountingComplete();
          server->stop();
          server->writeJsonReport(opType);
          if (serverDerived->getNumCompletionsExpectedTotal() < 0 ||
              rxComplete) {
            // We have received all the messages we need to receive
            LOG(INFO) << "Number of completed: "
                      << serverDerived->getNumCompletionsDelivered()
                      << " expected total: "
                      << serverDerived->getNumCompletionsExpectedTotal();
            return exitCodeForWqeErrors(server);
          }
          // stop() has now drained the poll thread; re-check against the final
          // delivered count. If all expected completions arrived, the barrier
          // deadline was merely too tight (the RX path had not been scheduled
          // yet under load), not a genuine drop -- so do not fail the run.
          if (serverDerived->rxAccountingComplete()) {
            LOG(WARNING) << fmt::format(
                "Completion barrier deadline reached, but all {} expected "
                "completions drained after stop(); treating as success",
                serverDerived->getNumCompletionsExpectedTotal());
            return exitCodeForWqeErrors(server);
          }
          LOG(WARNING) << fmt::format(
              "Completion barrier timed out: server completed {} of {} "
              "expected; treating as a real drop",
              serverDerived->getNumCompletionsDelivered(),
              serverDerived->getNumCompletionsExpectedTotal());
          return 1;
        }
        if (verifyPayload && rcBuf.buf != nullptr) {
          if (!fileData.empty()) {
            size_t fo = 0;
            CHECK(
                verifyBufferFromFilePattern(rcBuf.buf, rcBuf.len, fileData, fo))
                << "Data verification failed for recv buffer";
          } else {
            size_t ci = 0;
            CHECK(verifyBufferPattern(rcBuf.buf, rcBuf.len, mtuBytes, ci))
                << "Data verification failed for recv buffer";
          }
        }
        serverDerived->returnBuffer(rcBuf);
      }
    }
    case OperationType::Write: {
      auto serverDerived = (RdmaGenWriteResponder*)server.get();
      const auto numEffectiveQps = params.effectiveQpCap();
      for (uint32_t eqpIdx = 0; eqpIdx < numEffectiveQps; eqpIdx++) {
        // used as a blocking call to wait for all clients to complete in the
        // write case
        serverDerived->fetchDataReadyBuffer(eqpIdx);
      }
      server->writeJsonReport(opType);
      server->stop();
      return exitCodeForWqeErrors(server);
    }
    case OperationType::Read:
    case OperationType::AtomicFetchAdd:
    case OperationType::AtomicCmpSwap: {
      auto serverDerived = (RdmaGenReadResponder*)server.get();
      serverDerived->waitForStop();
      server->writeJsonReport(opType);
      server->stop();
      return exitCodeForWqeErrors(server);
    }
    default:
      return -1;
  }
}

} // namespace facebook::neteng::ai::rdma_gen
