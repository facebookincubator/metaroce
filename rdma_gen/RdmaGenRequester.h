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

#include <atomic>
#include <condition_variable>
#include <shared_mutex>

#include <folly/Synchronized.h>
#include "libsoftmetaroce/ClockSource.h"
#include "rdma_gen/CqeLogEntry.h"
#include "rdma_gen/RdmaGenClientStatsRecorder.h"
#include "rdma_gen/RdmaGenControlFactory.h"
#include "rdma_gen/RdmaGenEndpoint.h"
#include "rdma_gen/RdmaGenPoolManager.h"

namespace facebook::neteng::ai::rdma_gen {

class RdmaGenRequester {
 public:
  // A single completion-count-triggered shell command. When the observed
  // completion count reaches `threshold`, `cmd` is run via /bin/sh -c.
  struct CompletionHook {
    uint64_t threshold;
    std::string cmd;
  };

  RdmaGenRequester(
      const Params& params,
      const std::vector<int64_t>& numMessagesPerServer,
      bool equalMessagesPerServer,
      const std::vector<std::string>& rcReceivers,
      const std::vector<int>& rcCmPorts,
      bool logMessageLatencies,
      const std::vector<int>& rcCos = {},
      const std::vector<enum ibv_mtu>& mtus = {},
      const enum ibv_qp_state targetQPState = IBV_QPS_RTS,
      const std::string& jsonPath = "");

  virtual ~RdmaGenRequester();

  RdmaGenRequester(const RdmaGenRequester&) = delete;
  RdmaGenRequester& operator=(const RdmaGenRequester&) = delete;
  RdmaGenRequester(RdmaGenRequester&&) = delete;
  RdmaGenRequester& operator=(RdmaGenRequester&&) = delete;

  // Should be called exactly once at start before any other methods.
  void init(
      int sample_int,
      ITransportProvider& transportProvider,
      std::unique_ptr<CudaWrapper> cudaWrapper =
          std::make_unique<CudaWrapper>());

  // Fetches a RcBuffer that is ready to fill in control. Starts fetching from
  // the server at startQpIdx of the list of servers, but if the queue
  // pair of that server is full, then continues to the next server in the list
  // and so on.
  //
  // Blocking call if no data is available. RcBuffer's len field denotes the
  // buffer capacity.  RcBuffer's token field should not be modified.
  RcBuffer getBuffer(int startQpIdx);

  // Posts RcBuffer for RC send WR. RcBuffer's len field should be modified to
  // the actual payload size.
  void postBuffer(const RcBuffer& rcBuf);

  // Should be called exactly once at the end after the experiment is done.
  auto stop(int stopCompletions) {
    setMaxCompletions(stopCompletions);
    LOG(INFO) << "Waiting for cqPollThread_ to join...";
    if (cqPollThread_.joinable()) {
      cqPollThread_.join();
    }

    if (cqeProcessThread_.joinable()) {
      cqeProcessThread_.join();
    }

    const auto stopTime = clkSrc_->now();
    // Two-phase teardown: first block on the server's completion barrier until
    // it confirms it has drained all expected RX completions, and only then
    // send terminate. This avoids the race where terminate stops the server's
    // RX poll thread before the last completions are polled.
    barrierAllServers(stopCompletions);
    signalAllServers(/* terminate = */ true);
    return stopTime;
  }

  // Client-side stats recorder write json report
  void writeJsonReport(
      double gbps,
      double mpps,
      OperationType opType,
      VerbsMode::Value verbsMode,
      std::optional<double> cpLatencyUsecs = std::nullopt);

  // Exchanges metadata with all remote servers.
  // Returns true if all exchanges succeeded. Each server's response is stored
  // as a separate entry in remoteMetadataPerServer.
  bool exchangeMetadata(
      const std::map<std::string, std::string>& localMetadata,
      std::vector<std::map<std::string, std::string>>& remoteMetadataPerServer);

  // Store CQE with timestamp for error logging
  void storeCqeWithTimestamp(
      const WorkCompletionAdapter& cqe,
      int64_t timestamp,
      std::optional<uint32_t> maybeImmData = std::nullopt,
      std::optional<uint32_t> maybeSendFlags = std::nullopt);

  // Returns true if any WQE completed with an error
  bool hasWqeErrors() const {
    return firstWqeError_.load(std::memory_order_relaxed) != IBV_WC_SUCCESS;
  }

  // Returns the first WQE error status observed, or IBV_WC_SUCCESS if none
  int getFirstWqeError() const {
    return firstWqeError_.load(std::memory_order_relaxed);
  }

  // Getter for RC receiver QP info.
  std::shared_ptr<std::vector<RcQpInfo>>& getRemoteRcQpInfo() {
    return remoteRcQpInfo_;
  }

  // Getter for RC receiver QP info.
  const RcQpInfo& getRemoteRcQpInfo(int idx) {
    return getRemoteRcQpInfo()->at(idx);
  }

  const RcQpInfo& getLocalRcQpInfo(int idx) const {
    return endpoint_->getLocalRcQpInfo(idx);
  }

  int getRemoteRcQpInfoSize() const {
    return static_cast<int>(remoteRcQpInfo_->size());
  }

  // Get send flags for a given qp and message size.
  inline uint32_t getSendFlags(const int qp_idx, const uint32_t len) {
    return endpoint_->getSendFlags(qp_idx, len);
  }

  // Returns the associated pool manager.
  std::unique_ptr<RdmaGenPoolManager>& getPoolManager();

  RcQpContext* ctx() {
    return endpoint_->ctx();
  }

  IRdmaVerbs* verbs() {
    return endpoint_->verbs();
  }

  const Params& params() const {
    return endpoint_->params();
  }

  void notifyOnCompletionCount(uint64_t numCompletions) {
    nextNotifyCompletionCount_ = numCompletions;
    completionCountNeedToNotify_ = true;
  }

  void waitForCompletionCount() {
    std::unique_lock<std::mutex> ul(completionCountMutex_);
    completionCountCv_.wait(ul, [this]() {
      return this->numCompletions_ >= this->nextNotifyCompletionCount_;
    });
    completionCountNeedToNotify_ = false;
  }

  void checkCompletionCount() {
    if ((numCompletions_ >= nextNotifyCompletionCount_) &&
        completionCountNeedToNotify_) {
      std::unique_lock ul(completionCountMutex_);
      completionCountCv_.notify_one();
    }
  }

  uint64_t numCompletions() const {
    return numCompletions_.load(std::memory_order_relaxed);
  }

  // Posted sends still waiting for a completion, per QP.
  std::vector<uint32_t> waitingCompletionCounts() const {
    std::vector<uint32_t> counts(poolManager_->getWaitingCompletionPoolSize());
    for (uint32_t qp = 0; qp < counts.size(); ++qp) {
      counts[qp] = poolManager_->getWaitingCompletionPoolSizeForQp(qp);
    }
    return counts;
  }

  void setMaxCompletions(int maxCompletions) {
    std::unique_lock lock(maxCompletionMutex_);
    maxCompletions_ = maxCompletions;
  }

  int getMaxCompletions() const {
    std::shared_lock lock(maxCompletionMutex_);
    return maxCompletions_;
  }

  // Returns the number of detected sequence gaps/reorderings
  uint64_t getNumSeqGaps() const;

  void runClientRcCqPollingInThread(int sample_int);
  void runClientRcCqPollingMultiThread(int sample_int);

  // Parses the --completion_hooks flag string into an ordered list of hooks.
  // Splits on ',' into entries, each split on the FIRST ':' into
  // {threshold, command}. Validates threshold > 0 and non-empty command, and
  // returns the hooks sorted ascending by threshold (stable, so equal
  // thresholds keep input order). An empty string yields no hooks. Throws
  // std::invalid_argument on any malformed entry. Pure and unit-testable.
  static std::vector<CompletionHook> parseCompletionHooks(
      const std::string& spec);

 protected:
  // A non-zero exit, kill, or (with --completion_hook_timeout_sec) timeout
  // aborts the run via LOG(FATAL).
  void runCompletionHook(const CompletionHook& hook);
  // Runs every completion callback whose count has been reached but has not yet
  // run, in ascending order of count, advancing nextCallbackIdx_.
  void runDueCompletionCallbacks();
  // Single entry point invoked from the producer poll loops right after
  // numCompletions_ advances.
  void checkCompletionTriggers();
  // Returns the next send sequence number for a QP and increments the counter
  uint16_t getNextSendSeqNum(uint16_t qpIdx) {
    return nextSendSeqNumPerQp_[qpIdx]++;
  }

  // Connects to RC receiver's control management channel. If argument is false,
  // it retrieves QP info. If argument is true, it sends terminate signal.
  virtual void signalAllServers(bool terminate);
  // Runs CQ polling & CQE processing in a separate threads.
  virtual void runClientRcCqPolling(int sample_int);

  const std::vector<int64_t>& numMessagesPerServer_;
  const bool equalMessagesPerServer_;
  const std::vector<std::string> rcReceivers_;
  const std::vector<int> rcCmPorts_;
  const std::vector<int> rcCos_;
  const std::vector<enum ibv_mtu> mtus_;
  const enum ibv_qp_state targetQPState_;

  std::shared_ptr<std::vector<RcQpInfo>> remoteRcQpInfo_;
  std::shared_ptr<RdmaGenEndpoint> endpoint_;
  std::unique_ptr<IControlClient> cmClient_;
  std::unique_ptr<RdmaGenClientStatsRecorder> statsRecorder_;
  std::unique_ptr<RdmaGenPoolManager> poolManager_;

 private:
  void processClientCqeInThread(int sample_int);
  void terminateSignal();
  // Blocks on each unique server's completion barrier until it confirms RX
  // completion (or times out). Logs a warning and continues on any failure;
  // never throws.
  void barrierAllServers(int completed);

  // Common per-completion processing: parses wr_id, checks sequence gaps,
  // logs errors, stores CQE, records stats, and frees the buffer.
  void processCompletion(const WorkCompletionAdapter& wc, int64_t timestamp);

  // Wrapper method of ibv_post_send for three operations send/write/read.
  virtual int postRequest(const RcBuffer& rcBuf) = 0;

  mutable std::shared_mutex maxCompletionMutex_;
  int maxCompletions_;

  std::thread cqPollThread_;
  std::thread cqeProcessThread_;
  // The poll thread updates this count and runs completion callbacks.
  std::atomic<uint64_t> numCompletions_{0};
  // params.completion_callbacks and the --completion_hooks commands, sorted by
  // after_completions.
  std::vector<CompletionCallback> completionCallbacks_;
  size_t nextCallbackIdx_{0};
  std::atomic<bool> completionCountNeedToNotify_{false};
  std::atomic<uint64_t> nextNotifyCompletionCount_{UINT64_MAX};
  std::condition_variable completionCountCv_;
  std::mutex completionCountMutex_;

  // These are the structures needed to split the CQE processing & CQE polling
  // This is important to ensure we don't add extra latency between posting WQE
  // and actually polling for the completions.

  std::mutex cqStartMutex_;
  std::vector<struct CqeInfo> cqInfoVec_;

  std::unique_ptr<libsoftmetaroce::ClockSource> clkSrc_;
  folly::Synchronized<std::vector<CqeLogEntry>> errCqeLog_;
  folly::Synchronized<std::vector<CqeLogEntry>> allCqeLog_;
  std::atomic<int> firstWqeError_{IBV_WC_SUCCESS};

  // Sequence number tracking for debugging completion ordering per QP
  // (16-bit, wraps at 65536). lastCompletedSeqNumPerQp_ initialized to 65535
  // so first expected seqNum (65535+1 wrapped) equals 0.
  std::vector<uint16_t> nextSendSeqNumPerQp_;
  std::vector<uint16_t> lastCompletedSeqNumPerQp_;
  std::atomic<uint64_t> numSeqGaps_ = 0;

  void setFirstWqeError(int status) {
    if (status == IBV_WC_SUCCESS) {
      return;
    }
    int expected = IBV_WC_SUCCESS;
    firstWqeError_.compare_exchange_strong(
        expected, status, std::memory_order_relaxed);
  }
};

} // namespace facebook::neteng::ai::rdma_gen
