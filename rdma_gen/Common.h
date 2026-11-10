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

#include <arpa/inet.h>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <folly/Synchronized.h>

#ifdef RDMA_GEN_OSS
#include "rdma_gen/transport/RdmaVerbs.h"
#else
#include "neteng/ai/common/RdmaVerbs.h"
#endif
#include "rdma_gen/RdmaGenUtils.h"
#include "rdma_gen/memory/CudaWrapper.h"
#include "rdma_gen/memory/RdmaGenRdmaMode.h"
#include "rdma_gen/transport/ITransportProvider.h"
#include "rdma_gen/transport/RdmaGenVerbsMode.h"

namespace facebook::neteng::ai::rdma_gen {

// Taken from Fluent C++:
// https://www.fluentcpp.com/2016/12/08/strong-types-for-strong-interfaces/
// A simple wrapper for creating strong types
template <typename T, typename Parameter>
class StrongType {
 public:
  StrongType() : value_() {} // Default constructor
  explicit StrongType(T const& value) : value_(value) {}
  explicit StrongType(T&& value) : value_(std::move(value)) {}
  [[nodiscard]] constexpr operator T const&() const {
    return value_;
  }
  [[nodiscard]] constexpr operator T&() {
    return value_;
  }
  [[nodiscard]] T getValue() const {
    return value_;
  }

 private:
  T value_;
};

// Queue pair data structures.
struct RcQpContext {
  explicit RcQpContext(ITransportProvider& transportProvider)
      : transport_provider(transportProvider) {}
  virtual ~RcQpContext();

  std::vector<void*> buffers;
  // One for each entry in buffers
  std::vector<void*> gpu_buffers;
  // Buffers are allocated by an RdmaGenMemory instance;
  // permit specific logic to free buffers.
  std::function<void(RcQpContext*)> freeBuffers;

  struct ibv_comp_channel* channel = nullptr;

  // Non-owning handle to the transport provider. Lifetime is bounded by the
  // RdmaGenResources instance that owns the underlying unique_ptr.
  ITransportProvider& transport_provider;
  std::unique_ptr<ITransportDevice> dev_adapter;
  std::unique_ptr<ITransportContext> ctx_adapter;
  std::unique_ptr<ITransportDomain> pd_adapter;
  // use shared CQ
  std::unique_ptr<ITransportCq> cq_adapter;
  std::vector<std::unique_ptr<ITransportMr>> mr_list;
  std::vector<std::unique_ptr<ITransportPeer>> peer_list;

  uint8_t pf_id = 0;

  // Multi-GID support: stores all configured GIDs for multiport traffic
  std::vector<union ibv_gid> gids;

  uint32_t base_send_flags = 0;
  std::vector<uint32_t> max_inline_data;
  int pending{};

#ifndef NO_CUDA
  // Only relevant for GPU RDMA
  CUcontext cuContext{};
#endif
};

struct ContinueFlag {
  folly::Synchronized<bool> flag{true};
  std::condition_variable_any cv;

  // Set the flag and notify all waiters, including any CVs registered
  // via registerCv (e.g., the pool manager's CV for shutdown wakeup).
  void set(bool value) {
    auto locked = flag.wlock();
    *locked = value;
    cv.notify_all();
    std::lock_guard<std::mutex> guard(extraCvsMutex_);
    for (auto* extraCv : extraCvs_) {
      extraCv->notify_all();
    }
  }

  bool get() const {
    auto locked = flag.rlock();
    return *locked;
  }

  // Register an additional CV to be notified when set() is called.
  // Caller must call unregisterCv before the CV is destroyed.
  void registerCv(std::condition_variable_any* target) {
    std::lock_guard<std::mutex> guard(extraCvsMutex_);
    extraCvs_.push_back(target);
  }

  void unregisterCv(std::condition_variable_any* target) {
    std::lock_guard<std::mutex> guard(extraCvsMutex_);
    for (auto it = extraCvs_.begin(); it != extraCvs_.end(); ++it) {
      if (*it == target) {
        extraCvs_.erase(it);
        return;
      }
    }
  }

 private:
  std::mutex extraCvsMutex_;
  std::vector<std::condition_variable_any*> extraCvs_;
};

// Globally unique QP info, sufficient to establish RC connections.
// Supports multiple GIDs for multi-port configurations.
struct RcQpInfo {
  uint16_t lid = 0;
  int qpn = 0;
  uint32_t psn = 0;
  uint32_t customId = 0;
  std::vector<union ibv_gid> gids;
  std::optional<EroceQpCfg> eroceQpCfg;
};

// Format a vector of GIDs as a comma-separated string.
// Handles both IPv6 and IPv4-mapped IPv6 addresses.
std::string formatGids(const std::vector<union ibv_gid>& gids);

struct RcQpLink {
  struct RcQpInfo local;
  struct RcQpInfo remote;
  std::string localDevName;
  std::string remoteDevName;
  int localRank = 0;
  int remoteRank = 0;
  uint8_t localPfId = 0;
  std::vector<ITransportMr*> localMrList;
};

struct RdmaGenResources {
  std::unique_ptr<ITransportProvider> transport_provider;
};

// One-shot work the requester runs once its QPs together have completed
// `after_completions` work requests.
struct CompletionCallback {
  uint64_t after_completions;
  std::function<void()> action;
};

// Params to configure QP.
struct Params {
  RdmaMode::Value rdma_mode = RdmaMode::CpuRdma;
  VerbsMode::Value verbs_mode = VerbsMode::RdmaCore;
  std::string ib_dev_name;
  PlatformType::Value platform_type = PlatformType::UNKNOWN;
  int rc_cm_port = 0;
  std::vector<uint8_t> ib_ports = {1};
  std::vector<uint32_t> message_size = {};
  uint32_t qp_cap = 0;
  uint32_t tx_depth = 0;
  uint32_t rx_depth = 0;
  uint32_t batch_size = 1;
  uint8_t service_level = 0;
  std::vector<int> gid_idx{};
  std::vector<std::string> gid_addresses{};
  int gpu_idx = 0;
  bool use_event = false;
  bool use_block = false;
  EndpointRole role = EndpointRole::Unknown;
  enum ibv_mtu mtu = IBV_MTU_1024;
  uint8_t max_rd = 1; // only affects read operations
  uint8_t min_rnr_timer = 12;
  // 7 requests infinite RNR retries, which disables any RNR-retry-exceeded
  // error reporting. Use 0-6 for a finite limit.
  uint8_t rnr_retry = 7;
  uint8_t retry_cnt = 7;
  bool single_cq_thread = false;
  int cq_pcq_depth = 10;
  uint32_t writes_before_imm = 0; // only used for write immediate operation
  int num_completions_expected = -1;
  bool validate_immediate_data = true;
  bool send_inline_data = false;
  bool use_execution_fence = false;
  // Base sequence value for QP initialization. Verbs uses its low 24 bits as
  // PSN; MetaRoCE uses its low/high 16-bit halves as MSN/CSN. Each QP uses
  // psn + qp_index.
  std::optional<uint32_t> psn = std::nullopt;
  bool use_qpt_driver = false;
  // Optional policy to decide use_qpt_driver from the bound device name;
  // (devName, requested) -> effective flag. Null -> requested is used as-is.
  std::function<bool(std::string_view devName, bool requested)>
      use_qpt_driver_resolver;
  bool use_huge_pages = false;
  // Registers memory regions with IBV_ACCESS_RELAXED_ORDERING.
  bool relaxed_ordering = false;
  uint8_t sqp_per_qp = 1;
  uint8_t tx_path_count = 1;
  uint8_t rx_path_count = 1;
  // Comma-delimited, ordered list of 'threshold:command' hooks run (via
  // /bin/sh -c) from the requester CQ polling loop when the observed completion
  // count reaches each threshold. Empty disables this behavior. Parsed and
  // consumed entirely by the requester; never forwarded to the provider.
  std::string completion_hooks;
  // Per-hook wall-clock timeout in seconds for completion_hooks commands.
  // 0 waits indefinitely.
  int32_t completion_hook_timeout_sec = 0;
  // Run on the requester's CQ polling thread; an action that throws ends the
  // run, as a failing completion hook does.
  std::vector<CompletionCallback> completion_callbacks;

  uint32_t effectiveQpCap() const {
    return qp_cap * sqp_per_qp;
  }

  // Maps a peer_list index (which includes both base QPs and child SQPs)
  // back to the base QP index.
  uint32_t baseQpIndex(uint32_t peerIdx) const {
    return peerIdx / sqp_per_qp;
  }

  ContextCreationParams createContextSettings() const {
    CHECK(!ib_ports.empty()) << "ib_ports must not be empty";
    return ContextCreationParams{
        .ib_ports = ib_ports,
        .preferred_gid_idx = gid_idx,
        .preferred_gid_addresses = gid_addresses,
        .enable_muti_port = verbs_mode == VerbsMode::LibSoftMetaRoCE,
        .max_rd = max_rd,
        .min_rnr_timer = min_rnr_timer,
        .rnr_retry = rnr_retry,
        .retry_cnt = retry_cnt,
        .service_level = service_level,
        .psn = psn,
        .verbs_mode = verbs_mode,
    };
  }
};

// Represents memory buffer between RC sender/receiver and higher application.
struct RcBuffer {
  // Device-agnostic buffer for RDMA operations, i.e., this can be CPU or GPU
  // buffer depending RDMA mode.
  void* buf = nullptr;

  uint32_t len = 0;
  // Opaque token. Internally, this is index in RC registered memory buffers.
  uint32_t token = 0;
  // qp_idx is used to track the next qp on which to assign work, the algorithm
  // used is a simple non-blocking RR (If a QP has no un allocated WRs it moves
  // onto the next)
  uint32_t qp_idx = 0;
  std::optional<uint32_t> maybe_imm_data = std::nullopt;
};

struct CqeInfo {
  std::mutex wcMutex;
  std::vector<WorkCompletionAdapter> wcVec;
  int ne = 0;
  // wcFlag is used as a bit flag to indicate which processing thread should
  // be handling entry
  bool wcFlag = false;
  uint64_t wcTimeStamp{};

  // Constructor
  CqeInfo() = default;

  // Destructor
  ~CqeInfo() = default;

  // Copy Constructor
  CqeInfo(const CqeInfo& other)
      : wcMutex(),
        wcVec(),
        ne(other.ne),
        wcFlag(other.wcFlag),
        wcTimeStamp(0) {}

  // Copy assignment operator
  CqeInfo& operator=(const CqeInfo& other) {
    // wcMutex doesn't need to be copied
    wcVec = other.wcVec;
    ne = other.ne;
    wcFlag = other.wcFlag;
    wcTimeStamp = other.wcTimeStamp;
    return *this;
  }

  CqeInfo(CqeInfo&&) = delete;
  CqeInfo& operator=(CqeInfo&&) = delete;
};

using MetadataCallback = std::function<void(
    const std::map<std::string, std::string>& clientMetadata,
    const std::map<std::string, std::string>& serverMetadata,
    const std::string& clientAddress)>;

} // namespace facebook::neteng::ai::rdma_gen

namespace std {
// This provides a hash function so that StrongType objects can be used as keys
// in unordered maps and sets.
template <typename T, typename Parameter>
struct hash<facebook::neteng::ai::rdma_gen::StrongType<T, Parameter>> {
  size_t operator()(
      facebook::neteng::ai::rdma_gen::StrongType<T, Parameter> const& s)
      const noexcept {
    return s.getValue();
  }
};

} // namespace std

template <typename T, typename Parameter>
struct fmt::formatter<facebook::neteng::ai::rdma_gen::StrongType<T, Parameter>>
    : fmt::formatter<T> {
  auto format(
      const facebook::neteng::ai::rdma_gen::StrongType<T, Parameter>& s,
      fmt::format_context& ctx) const {
    return fmt::formatter<T>::format(s.getValue(), ctx);
  }
};
