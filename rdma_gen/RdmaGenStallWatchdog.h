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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>

namespace facebook::neteng::ai::rdma_gen {

// Tracks how long a progress counter has stayed flat while work is
// outstanding.
class RdmaGenStallDetector {
 public:
  using Clock = std::chrono::steady_clock;
  using Duration = std::chrono::milliseconds;

  RdmaGenStallDetector(uint64_t progress, Clock::time_point now)
      : last_{progress}, lastChange_{now} {}

  // Returns how long `progress` has been unchanged with work outstanding, or
  // zero if it changed or nothing is outstanding.
  Duration update(uint64_t progress, bool outstanding, Clock::time_point now) {
    if (progress != last_ || !outstanding) {
      last_ = progress;
      lastChange_ = now;
      return Duration::zero();
    }
    return std::chrono::duration_cast<Duration>(now - lastChange_);
  }

 private:
  uint64_t last_;
  Clock::time_point lastChange_;
};

// Calls `onStall` once if `progress()` stays unchanged for `timeout` while
// `outstanding()` is true. Polls every `pollInterval` on its own thread, which
// the destructor stops and joins. `onStall` runs on that thread and receives
// how long progress has been flat.
class RdmaGenStallWatchdog {
 public:
  using Duration = std::chrono::milliseconds;

  RdmaGenStallWatchdog(
      Duration timeout,
      std::function<uint64_t()> progress,
      std::function<bool()> outstanding,
      std::function<void(Duration)> onStall,
      Duration pollInterval = std::chrono::seconds(1))
      : timeout_(timeout),
        pollInterval_(pollInterval),
        progress_(std::move(progress)),
        outstanding_(std::move(outstanding)),
        onStall_(std::move(onStall)),
        thread_([this] { run(); }) {}

  RdmaGenStallWatchdog(const RdmaGenStallWatchdog&) = delete;
  RdmaGenStallWatchdog& operator=(const RdmaGenStallWatchdog&) = delete;
  RdmaGenStallWatchdog(RdmaGenStallWatchdog&&) = delete;
  RdmaGenStallWatchdog& operator=(RdmaGenStallWatchdog&&) = delete;

  ~RdmaGenStallWatchdog() {
    {
      std::lock_guard lock(mutex_);
      stop_ = true;
    }
    cv_.notify_all();
    thread_.join();
  }

 private:
  void run() {
    using Clock = RdmaGenStallDetector::Clock;
    RdmaGenStallDetector detector(progress_(), Clock::now());
    std::unique_lock lock(mutex_);
    while (!cv_.wait_for(lock, pollInterval_, [this] { return stop_; })) {
      const auto flat =
          detector.update(progress_(), outstanding_(), Clock::now());
      if (flat >= timeout_) {
        onStall_(flat);
        return;
      }
    }
  }

  const Duration timeout_;
  const Duration pollInterval_;
  const std::function<uint64_t()> progress_;
  const std::function<bool()> outstanding_;
  const std::function<void(Duration)> onStall_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stop_{false};
  std::thread thread_;
};

} // namespace facebook::neteng::ai::rdma_gen
