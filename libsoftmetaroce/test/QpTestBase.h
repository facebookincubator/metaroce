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

#include <array>
#include <chrono>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Timeout.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/CompletionQueue.h>
#include <libsoftmetaroce/Ecn.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/test/MockCongestionController.h>

namespace libsoftmetaroce {

/**
 * Base test class for QP tests with common setup and helper methods
 */
class QpTestBase : public ::testing::Test {
 protected:
  /**
   * Structure to hold WQE creation parameters
   */
  struct WqeData {
    Wqe::WorkType workType = Wqe::WorkType::kWrite;
    uint32_t msgSizeBytes = 0;
    std::optional<uint8_t> tagNum{kMetaRoceInitialTag};
    std::optional<uint16_t> msn{};
    std::optional<uint16_t> csn{};
    std::optional<uint16_t> mtu{};
    std::optional<uint64_t> workReqId{};
    std::optional<void*> address{};
    std::optional<uint32_t> maybeImmediateData{};
    std::optional<uint64_t> remote_addr{};
    std::optional<uint32_t> rkey{};
    uint64_t swapOrAddData{0};
    uint64_t compareData{0};
    // When set, the WQE carries this local key instead of registering a real MR
    // for `address` -- used to exercise local-protection error paths.
    std::optional<uint32_t> lkeyOverride{};
  };

  void SetUp() override;

  /**
   * Sleep for the specified duration in microseconds
   */
  static void sleepForUs(uint32_t duration);

  /**
   * Wait for a packet from the QP with a timeout
   */
  static std::unique_ptr<struct EgressPacket> waitForPacket(
      Qp& qp,
      uint32_t timeoutUs);

  /**
   * Create a mock congestion controller that always allows sending
   */
  static void createAlwaysSendingMockCongestionController(Qp& qp);

  /**
   * Helper to create and post WQEs with various options.
   *
   * If doProcessWqe is true, then also process the WQE into its Tag. If posting
   * many WQEs in sequence, it is most efficient to use doProcessWqe = false and
   * then call processWqes() once after all posts.
   */
  Wqe* postWqeHelper(Qp* postQp, WqeData wqeData, bool doProcessWqe = true);

  /** Allocate the posns a posted send or write will put on the wire, in
   * order, so an ack addressing one by number matches the frame allocated
   * here. False where it could not, which leaves the caller nothing to ack. */
  [[nodiscard]] bool allocateSendablePosns(Wqe* wqe);

  /**
   * Invoke Qp::processWqes() to process posted WQEs into their Tags. Returns
   * true if the specified number of WQEs were posted, or false otherwise.
   */
  bool processWqes(
      Qp* qp,
      int numWqes,
      int numRWqes,
      std::chrono::milliseconds durationMs = std::chrono::milliseconds(100));

  /**
   * Helper to post receive WQEs in sequence
   */
  Wqe* postInSequenceReceiveWqe(
      Qp& postQp,
      void* address,
      uint32_t msgSizeBytes,
      uint8_t tagNum = 0);

  // Below every msn the cases use, so each of their messages is still to come.
  static constexpr uint16_t kFixtureCdmsn = 0;
  // Away from zero, so a case must specify the base its receives were given.
  static constexpr uint16_t kFixtureRxCsn = 101;

  HighResTimer retxTimer;
  QpArbiter arbiter;
  CompletionQueue cq;
  std::array<char, 4096 * 4> receiveBuffer{};
  MemoryRegions mrs;
  uint16_t mrKey{0};
  ibv_pd pd{nullptr, 1};
  struct ibv_qp ibvQpStorage{.pd = &pd};
  struct ibv_qp* ibvQp = &ibvQpStorage;
  // Declared before qp so it is destroyed after it: a Tag reaches its ibv_qp
  // while the Qp is being torn down, and a derived-class member would already
  // be gone by then.
  struct ibv_qp otherIbvQpStorage{.pd = &pd};
  Qp qp{10 /*qpNum*/, retxTimer, arbiter, mrs, &cq, ibvQp};
  Tag* initialTag{nullptr};
  uint16_t mtu{4096};
};

} // namespace libsoftmetaroce
