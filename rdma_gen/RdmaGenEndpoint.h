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

#include <poll.h>

#include <ctime>
#include <memory>

#include "rdma_gen/Common.h"
#include "rdma_gen/memory/CpuMemory.h"
#include "rdma_gen/memory/GpuMemory.h"
#include "rdma_gen/memory/RdmaGenMemory.h"
#include "rdma_gen/memory/RdmaGenMemoryFactory.h"
#include "rdma_gen/memory/RocmMemoryRegistration.h"

namespace facebook::neteng::ai::rdma_gen {

// Encapsulate common QP operations for Rc sender and receiver.
//
// Subclassing contract: the async-event thread polls the transport context for
// as long as it runs, so it must be joined before anything it touches goes
// away. A subclass whose destructor tears down the transport context, the
// provider, or any state reachable through ctx() must call
// stopIbAsyncEventHandling() as its first act -- the base destructor runs too
// late, after the subclass members are already gone.
class RdmaGenEndpoint {
 public:
  explicit RdmaGenEndpoint(const Params& params) : params_(params) {
    continueIbAsyncEventHandling_ = true;

    auto& factory = RdmaGenMemoryFactory::getInstance();
    factory.registerMemoryCreator(
        RdmaMode::CpuRdma,
        [](Params& p,
           RcQpContext* ctx,
           int accessFlags,
           std::unique_ptr<CudaWrapper>) {
          return std::make_unique<CpuMemory>(p, ctx, accessFlags);
        });
    factory.registerMemoryCreator(
        RdmaMode::GpuRdma,
        [](Params& p,
           RcQpContext* ctx,
           int accessFlags,
           std::unique_ptr<CudaWrapper> cudaWrapper) {
          return std::make_unique<GpuMemory>(
              p, ctx, accessFlags, std::move(cudaWrapper));
        });
    // Register ROCm memory creator (no-op for non-ROCm builds)
    registerRocmMemoryCreator(factory);
  }

  virtual ~RdmaGenEndpoint() {
    stopIbAsyncEventHandling();
  }

  // Stop and join the async-event thread. Idempotent. Must be callable while
  // the transport context/provider is still alive: the thread polls
  // ctx()->ctx_adapter, whose underlying ibv_context is owned by the provider,
  // so it has to be joined before the provider is torn down. Owners that hand
  // out endpoints as shared_ptr (which can outlive the provider) call this at
  // teardown rather than relying on the destructor.
  void stopIbAsyncEventHandling() {
    continueIbAsyncEventHandling_.store(false);
    if (ibAsyncEventHandlingThread_.joinable()) {
      ibAsyncEventHandlingThread_.join();
    }
  }

  RdmaGenEndpoint(const RdmaGenEndpoint&) = delete;
  RdmaGenEndpoint& operator=(const RdmaGenEndpoint&) = delete;
  RdmaGenEndpoint(RdmaGenEndpoint&&) = delete;
  RdmaGenEndpoint& operator=(RdmaGenEndpoint&&) = delete;

  // Prints out RC QP info in human readable format.
  static std::string printRcQpInfo(const RcQpInfo& info);

  const Params& params() const {
    return params_;
  }

  virtual RcQpContext* ctx() {
    return ctx_.get();
  }

  const RcQpInfo& getLocalRcQpInfo(int idx) const {
    return localRcQpInfo_[idx];
  }

  bool getLocalRcQpInfo(
      int start_idx,
      int end_idx,
      std::vector<RcQpInfo>& QpInfo) const;

  const RcQpInfo* FOLLY_NULLABLE getLocalRcQpInfoByQpn(int qpn) const;

  unsigned long getLocalRcQpCount() const {
    return localRcQpInfo_.size();
  }

  virtual void initMemory(std::unique_ptr<CudaWrapper> cudaWrapper) {
    auto& factory = RdmaGenMemoryFactory::getInstance();
    memory_ = factory.createMemory(
        params_.rdma_mode,
        params_,
        ctx(),
        getMrAccessFlags(),
        std::move(cudaWrapper));
  }

  // Initializes local RC QP states
  void init(
      int bufferCap,
      ITransportProvider& transportProvider,
      std::unique_ptr<CudaWrapper> cudaWrapper,
      const enum ibv_qp_state targetQPState = IBV_QPS_INIT) {
    CHECK(params_.role != EndpointRole::SenderReceiver)
        << "Endpoint role cannot be SenderReceiver";
    LOG(INFO) << fmt::format("{}: buffers: {}", __func__, bufferCap);
    LOG(INFO) << fmt::format("{}: qps: {}", __func__, params_.qp_cap);
    LOG(INFO) << fmt::format("{}: rx_depth: {}", __func__, params_.rx_depth);
    srand48(getpid() * time(nullptr));

    initRcQpContext(transportProvider, targetQPState);
    int limitedBufferCap = std::min(
        bufferCap, std::max((int)params_.rx_depth, (int)params_.tx_depth));
    CHECK_GT(limitedBufferCap, 0);

    initMemory(std::move(cudaWrapper));
    CHECK(memory_);
    memory_->setUpBuffers(limitedBufferCap);

    buildLocalRcQpInfo();
  }

  // Helper to bring QP state to RTS.
  bool modifyRcQpsRts(
      const RcQpInfo& remote_qp_info,
      int idx,
      int qp_cos,
      enum ibv_mtu mtu,
      enum ibv_qp_state targetQPState = IBV_QPS_RTS);

  // Event driven polling mechanimsm instead of mad polling
  int notifyCqChannelEvents();

  // Get an return the state for the QP with index qp_idx
  ibv_qp_state getQpState(const int qp_idx);

  // Get pointer to RdmaVerbs
  IRdmaVerbs* verbs() {
    CHECK_NOTNULL(ctx());
    return ctx()->transport_provider.getVerbs();
  }

  void* getBuffer(int bufIdx) {
    return memory_->getBuffer(bufIdx);
  }
  std::vector<void*>& getBuffers() {
    return memory_->getBuffers();
  }

  // Compute supported send flags for a QP, given message len
  uint32_t getSendFlags(const int qp_idx, const uint32_t len);

 protected:
  // Helper method to init IB device.
  void initIbDevice(const std::string& ib_dev_name, RcQpContext* ctx);

  void runIbAsyncEventHandlingInThread();

  // access flags for both qp_access_flags and memory regions
  unsigned int getAccessFlags();
  // getAccessFlags() plus the flags that only apply to memory regions
  unsigned int getMrAccessFlags();

  void limitParamsToDevice();

  // Helper method to init ibv_* states.
  void initRcQpContext(
      ITransportProvider& transportProvider,
      enum ibv_qp_state targetQPState = IBV_QPS_INIT);

  // Helper method to build QP info from ibv_ states.
  void buildLocalRcQpInfo();

  Params params_;
  std::unique_ptr<RcQpContext> ctx_;
  std::vector<RcQpInfo> localRcQpInfo_;
  // common interface to either cpu or gpu memory
  std::unique_ptr<RdmaGenMemory> memory_;
  struct pollfd pollfd_;
  std::thread ibAsyncEventHandlingThread_;
  std::atomic<bool> continueIbAsyncEventHandling_;

 protected:
  virtual void launchIbAsyncEventHandlingThread();
};

} // namespace facebook::neteng::ai::rdma_gen
