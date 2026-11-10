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

#include "rdma_gen/RdmaGenEndpoint.h"

#include <infiniband/verbs.h>
#include <algorithm>
#include <cstdint>
#include <utility>

namespace facebook::neteng::ai::rdma_gen {

DEFINE_bool(
    allow_incorrect_queried_qp_state,
    false,
    "Assume that a successful ibv_modify_qp sets the state, do not fail if reading back with ibv_query_qp does not see the correct QP state ({INIT, RTR, RTS}).");

DEFINE_bool(
    skip_ibv_query_qp_verification,
    false,
    "Skip ibv_query_qp verification for RTR, RTS, and ERROR states. "
    "On some platforms, ibv_query_qp can take about a second "
    "for these states. We trust the return value of ibv_modify_qp instead.");

void RdmaGenEndpoint::initIbDevice(
    const std::string& ib_dev_name,
    RcQpContext* ctx) {
  if (!ib_dev_name.empty()) {
    LOG(INFO) << fmt::format("Looking for ib dev name: {} ", ib_dev_name);
  }
  auto num_devices = ctx->transport_provider.getDeviceCount();
  if (num_devices == 0) {
    LOG(ERROR) << "Cannot continue with zero devices";
    exit(1);
  }
  CHECK_GT(num_devices, 0) << "Found zero IB devices";
  auto devices = ctx->transport_provider.getDevices();
  std::string deviceNames(devices[0]->getName());
  for (int i = 1; i < num_devices; i++) {
    deviceNames += ", ";
    deviceNames += devices[i]->getName();
  }
  LOG(INFO) << fmt::format("Found {} IB devices: {}", num_devices, deviceNames);

  if (ib_dev_name.empty()) {
    // choose first device found if a specific one was not requested.
    ctx->dev_adapter = std::move(devices[0]);
  } else {
    ctx->dev_adapter = nullptr;
    for (int i = 0; i < num_devices; i++) {
      if (devices[i]->getName() == ib_dev_name) {
        ctx->dev_adapter = std::move(devices[i]);
        break;
      }
    }
    CHECK(ctx->dev_adapter)
        << "No IB devices named '" << ib_dev_name << "' were found";
  }
}

void RdmaGenEndpoint::launchIbAsyncEventHandlingThread() {
  // Only launch when the transport provider exposes an async event channel.
  // Providers without one (e.g. libfabric) have no fd/verbs to poll.
  if (!ctx()->ctx_adapter->hasAsyncEvents()) {
    LOG(INFO)
        << "Transport provider has no async event channel; skipping async event handling";
    return;
  }

  ibAsyncEventHandlingThread_ =
      std::thread(&RdmaGenEndpoint::runIbAsyncEventHandlingInThread, this);

  LOG(INFO) << "Launched thread for async IB event handling";
}

void RdmaGenEndpoint::runIbAsyncEventHandlingInThread() {
  LOG(INFO) << "Started thread for async IB event handling";

  while (continueIbAsyncEventHandling_) {
    std::optional<AsyncEventInfo> event =
        ctx()->ctx_adapter->pollAsyncEvent(50 /* timeout in ms */);
    if (!event) {
      // No event within the timeout, or the provider surfaced none.
      continue;
    }

    if (event->isError) {
      LOG(ERROR) << fmt::format(
          "Received error IB_EVENT ({}) - {}",
          event->eventType,
          event->description);
    } else {
      LOG(INFO) << fmt::format(
          "Received IB_EVENT ({}) - {}", event->eventType, event->description);
    }
  }
}

unsigned int RdmaGenEndpoint::getAccessFlags() {
  unsigned int flags = IBV_ACCESS_LOCAL_WRITE;
  if ((params_.role == EndpointRole::WriteResponder) ||
      (params_.role == EndpointRole::Receiver)) {
    flags |= IBV_ACCESS_REMOTE_WRITE;
  } else if (params_.role == EndpointRole::ReadResponder) {
    flags |= IBV_ACCESS_REMOTE_READ;
  } else if (params_.role == EndpointRole::AtomicResponder) {
    flags |= IBV_ACCESS_REMOTE_ATOMIC;
  } else {
    flags |= IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
  }
  return flags;
}

unsigned int RdmaGenEndpoint::getMrAccessFlags() {
  unsigned int flags = getAccessFlags();
  if (params_.relaxed_ordering) {
    flags |= IBV_ACCESS_RELAXED_ORDERING;
  }
  return flags;
}

static void limitInt(uint32_t& v, uint32_t limit, const std::string& name) {
  if (v > limit) {
    LOG(INFO) << fmt::format(
        "Limiting {}, desired {} to device supported {}", name, v, limit);
    v = limit;
  }
}

void RdmaGenEndpoint::limitParamsToDevice() {
  auto device_attr = ctx()->ctx_adapter->queryAttributes();

  limitInt(params_.tx_depth, device_attr.max_qp_wr, "tx buffers");
  limitInt(params_.rx_depth, device_attr.max_cqe, "rx buffers");
  limitInt(params_.tx_depth, device_attr.max_mr, "tx memory regions");
  limitInt(params_.rx_depth, device_attr.max_mr, "rx memory regions");
}

// initRcQpContext()
// Init & open an IB device, creates QP for completion queue.
// On server, creates all the required QPs (i.e. qp_cap) to talk with clients.
// On client, creates one QP to each server.
void RdmaGenEndpoint::initRcQpContext(
    ITransportProvider& transportProvider,
    enum ibv_qp_state targetQPState) {
  ctx_ = std::make_unique<RcQpContext>(transportProvider);
  CHECK(ctx());

  initIbDevice(params_.ib_dev_name, ctx_.get());
  // Let an optional policy decide use_qpt_driver from the device we just bound
  // (ib_dev_name may have been empty). No-op if unset.
  if (params_.use_qpt_driver_resolver) {
    params_.use_qpt_driver = params_.use_qpt_driver_resolver(
        ctx()->dev_adapter->getName(), params_.use_qpt_driver);
  }
  auto settings = params_.createContextSettings();
  settings.skip_ibv_query_qp_verification =
      FLAGS_skip_ibv_query_qp_verification;
  settings.allow_incorrect_queried_qp_state =
      FLAGS_allow_incorrect_queried_qp_state;

  ctx()->ctx_adapter = ctx()->dev_adapter->createContext(settings);

  ctx()->pd_adapter = ctx()->ctx_adapter->createDomain();

  launchIbAsyncEventHandlingThread();

  limitParamsToDevice();

  if (params_.use_event) {
    ctx()->channel =
        verbs()->ibv_create_comp_channel(ctx()->ctx_adapter->getIbContextPtr());
    CHECK(ctx()->channel) << "ibv_create_comp_channel failed";
    pollfd_.fd = ctx()->channel->fd;
    pollfd_.events = POLLIN;
    pollfd_.revents = 0;
  }

  const uint32_t effective_qp_cap = params_.effectiveQpCap();
  uint32_t cq_depth = effective_qp_cap * (params_.rx_depth + params_.tx_depth);
  limitInt(
      cq_depth, ctx()->ctx_adapter->queryAttributes().max_cqe, "cq entries");
  ctx()->cq_adapter = ctx()->pd_adapter->createCq(cq_depth, ctx()->channel);

  if (params_.use_event) {
    LOG(INFO) << "Registering for completion event notification";
    CHECK_EQ(verbs()->ibv_req_notify_cq(ctx()->cq_adapter->getIbCqPtr(), 0), 0)
        << "Unable to register completion event notification";
  }

  PeerCreationParams base_attrs{
      .scatter_gather_max = 1,
      .use_qpt_driver = params_.use_qpt_driver,
      .use_selective_signaling = true,
      .max_send_wr = std::max(params_.tx_depth, 1U),
      .max_recv_wr = std::max(params_.rx_depth, 1U),
      .max_inline_data = 0,
      .access_flags = getAccessFlags(),
      .sqp_per_qp = params_.sqp_per_qp,
      .tx_path_count = params_.tx_path_count,
      .rx_path_count = params_.rx_path_count,
  };

  auto attr = ctx()->pd_adapter->getPortInfo();
  params_.gid_idx = attr.gid_idx;
  ctx()->gids = attr.gids;

  ctx()->base_send_flags = IBV_SEND_SIGNALED;

  for (uint32_t i = 0; i < params_.qp_cap; i++) {
    // When sqp_per_qp > 1, create the base QP in RESET state so child SQPs
    // can be added before transitioning to INIT.
    const auto baseQpState =
        params_.sqp_per_qp > 1 ? IBV_QPS_RESET : targetQPState;
    auto peer = ctx()->pd_adapter->createPeer(
        base_attrs, ctx()->cq_adapter.get(), baseQpState);

    // Use the transport-agnostic peer id (verbs: qp_num) so this works for
    // both the verbs and libfabric transport providers.
    const uint32_t parent_qpn = peer->getId();
    ctx()->peer_list.push_back(std::move(peer));

    for (uint8_t s = 1; s < params_.sqp_per_qp; s++) {
      auto child = ctx()->pd_adapter->createChildPeer(
          base_attrs, ctx()->cq_adapter.get(), parent_qpn, s);
      ctx()->peer_list.push_back(std::move(child));
    }

    // Move the base QP to INIT after all child SQPs are added
    auto& base_peer = ctx()->peer_list[i * params_.sqp_per_qp];
    if (params_.sqp_per_qp > 1 && targetQPState >= IBV_QPS_INIT) {
      base_peer->modifyRcQpInit(targetQPState);
    }

    uint32_t max_inline_data = 0;
    if (!FLAGS_allow_incorrect_queried_qp_state) {
      PeerCreationParams queried_attr = base_peer->queryCreationParams();
      max_inline_data = queried_attr.max_inline_data;
      if (max_inline_data > 0) {
        LOG_FIRST_N(INFO, 5)
            << fmt::format("RC control max_inline_data: {}", max_inline_data);
      }
    }
    // Push base QP's max_inline_data first to match peer_list ordering,
    // then 0 for each child SQP.
    ctx()->max_inline_data.push_back(max_inline_data);
    for (uint8_t s = 1; s < params_.sqp_per_qp; s++) {
      ctx()->max_inline_data.push_back(0);
    }
  }
}

void RdmaGenEndpoint::buildLocalRcQpInfo() {
  auto portInfo = ctx()->pd_adapter->getPortInfo();
  for (uint32_t i = 0; i < ctx()->peer_list.size(); i += params_.sqp_per_qp) {
    const auto conn_info = ctx()->peer_list[i]->getConnectionData();
    const bool isLast = (i + params_.sqp_per_qp >= ctx()->peer_list.size());
    RcQpInfo temp_rc{
        .lid = portInfo.lid,
        .qpn = static_cast<int>(conn_info.qp_num),
        .psn = conn_info.psn,
        .gids = isLast ? std::move(portInfo.gids) : portInfo.gids,
        .eroceQpCfg = conn_info.eroceQpCfg,
    };
    localRcQpInfo_.push_back(std::move(temp_rc));
  }
}

/* modifyRcQpsRts()
Modifies a QP's state from INIT to RTR and then RTS.
*/
bool RdmaGenEndpoint::modifyRcQpsRts(
    const RcQpInfo& remote_qp_info,
    int qp_idx,
    int qp_cos,
    enum ibv_mtu mtu,
    enum ibv_qp_state targetQpState) {
  CHECK_GE(qp_idx, 0);
  CHECK_LT(qp_idx, ctx()->peer_list.size());
  CHECK_GE(remote_qp_info.qpn, 0);

  PeerConnectData peer_data{
      .psn = remote_qp_info.psn,
      .qp_num = (uint32_t)remote_qp_info.qpn,
      .eroceQpCfg = remote_qp_info.eroceQpCfg,
      .mtu = mtu,
      .traffic_class = qp_cos,
  };

  PortConnectData port_data{
      .lid = remote_qp_info.lid,
      .gids = remote_qp_info.gids,
  };

  return ctx()->peer_list[qp_idx]->finishConnect(
      peer_data, port_data, targetQpState);
}

std::string RdmaGenEndpoint::printRcQpInfo(const RcQpInfo& info) {
  return fmt::format(
      "LID: {:#0x} QPN: {:#0x} PSN: {:#0x} GIDs: [{}]",
      info.lid,
      info.qpn,
      info.psn,
      formatGids(info.gids));
}

int RdmaGenEndpoint::notifyCqChannelEvents() {
  if (params_.use_event) {
    auto ret = poll(&pollfd_, 1, 1000); // 1000ms timeout
    if (ret <= 0) {
      return ret;
    }

    struct ibv_cq* cq;
    void* cq_ctx;
    // Wait for the completion event
    if (verbs()->ibv_get_cq_event(ctx()->channel, &cq, &cq_ctx)) {
      LOG(ERROR) << "Unable to get completion event notification";
      return -1;
    }

    // Ack the event. Add batching which could be cheaper.
    verbs()->ibv_ack_cq_events(cq, 1);

    // Request notification for next completion event.
    if (verbs()->ibv_req_notify_cq(cq, 0)) {
      LOG(ERROR) << "Unable to re-register completion event notification";
      return -1;
    }
  }

  return 1;
}

bool RdmaGenEndpoint::getLocalRcQpInfo(
    int start_idx,
    int end_idx,
    std::vector<RcQpInfo>& qpInfo) const {
  if (start_idx < 0 || end_idx > static_cast<int>(localRcQpInfo_.size()) ||
      start_idx >= end_idx) {
    LOG(ERROR) << fmt::format(
        "Invalid indices: start_idx={}, end_idx={}, localRcQpInfo_.size()={}",
        start_idx,
        end_idx,
        localRcQpInfo_.size());
    return false;
  }
  qpInfo.clear();
  qpInfo.reserve(end_idx - start_idx);
  for (int i = start_idx; i < end_idx; i++) {
    qpInfo.push_back(localRcQpInfo_[i]);
  }
  return true;
}

const RcQpInfo* FOLLY_NULLABLE
RdmaGenEndpoint::getLocalRcQpInfoByQpn(int qpn) const {
  for (const auto& qpInfo : localRcQpInfo_) {
    if (qpInfo.qpn == qpn) {
      return &qpInfo;
    }
  }
  return nullptr;
}

ibv_qp_state RdmaGenEndpoint::getQpState(const int qp_idx) {
  CHECK_LT(qp_idx, ctx()->peer_list.size());
  auto peerState = ctx()->peer_list[qp_idx]->queryState();
  return peerState.qp_state;
}

uint32_t RdmaGenEndpoint::getSendFlags(const int qp_idx, const uint32_t len) {
  uint32_t inline_send_flag = 0;
  uint32_t execution_fence_flag =
      params_.use_execution_fence ? IBV_SEND_FENCE : 0;
  if (len <= ctx()->max_inline_data[qp_idx]) {
    inline_send_flag = IBV_SEND_INLINE;
  }
  return ctx()->base_send_flags | inline_send_flag | execution_fence_flag;
}

} // namespace facebook::neteng::ai::rdma_gen
