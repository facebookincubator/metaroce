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

#include <libsoftmetaroce/Constants.h>

#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/Utility.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/EventUtil.h>
#include <folly/system/ThreadName.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <sys/eventfd.h>
#include <sys/timerfd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace libsoftmetaroce {

// ClockSource wrapper for std::chrono::high_resolution_clock
class ClockSource : public folly::NonCopyableNonMovable {
 public:
  using rep = long long;
  using period = std::nano;
  using duration = std::chrono::duration<rep, period>;
  using time_point = std::chrono::time_point<ClockSource, duration>;

  virtual clockid_t GetClockId() {
    return CLOCK_REALTIME;
  }
  virtual time_point now() {
    return time_point(
        duration(std::chrono::high_resolution_clock::now().time_since_epoch()));
  }
  virtual ~ClockSource() = default;
};

// PTPClockSource for ptp clock
class PTPClockSource : public ClockSource {
 public:
  explicit PTPClockSource(const std::string& dev) : dev_(dev) {
    fd_ = folly::fileops::open(dev_.c_str(), O_RDWR);
    if (fd_ < 0) {
      throw std::runtime_error(
          "failed to open ptp dev " + dev_ + " error " + folly::errnoStr(fd_));
    }
    // fd to clockid magic from
    // https://github.com/torvalds/linux/blob/43e9ad0c55a369ecc84a4788d06a8a6bfa634f1c/tools/testing/selftests/ptp/testptp.c#L89-L93
    clockid_ = ((~(clockid_t)(fd_) << 3) | 3);
  }

  PTPClockSource(const PTPClockSource&) = delete;
  PTPClockSource& operator=(const PTPClockSource&) = delete;
  PTPClockSource(PTPClockSource&&) = delete;
  PTPClockSource& operator=(PTPClockSource&&) = delete;

  virtual ~PTPClockSource() override {
    if (fd_ > 0) {
      folly::fileops::close(fd_);
      fd_ = -1;
      clockid_ = -1;
    }
  }

  virtual clockid_t GetClockId() override final {
    return clockid_;
  }

  static constexpr bool is_steady = false;

  virtual ClockSource::time_point now() override final {
    struct timespec ts{};
    int ret = ::clock_gettime(clockid_, &ts);
    if (ret < 0) {
      throw std::runtime_error(
          "failed to clock_gettime: " + folly::errnoStr(ret));
    }
    return ClockSource::time_point(
        ClockSource::duration(ts.tv_sec * kNsPerSecond + ts.tv_nsec));
  }

 private:
  int fd_{-1};
  std::string dev_;
  clockid_t clockid_{-1};
};

// clock-domains.md records which of the three readings below to use.
std::unique_ptr<ClockSource> getClockSourceInstance();
uint32_t getNowNs();

uint32_t nextPtpTimerId();

// The protocol/timer clock at full width, for deadlines that must not wrap.
uint64_t getNowNs64();

// Monotonic 64-bit nanosecond clock for cumulative diagnostics. The protocol
// clock getNowNs() is 32-bit and wraps every ~4.3s, too short to accumulate
// over a whole run.
inline uint64_t steadyNowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
// folly::io::async::TimerFD but with ClockSource picked by
// getClockSourceInstance.
// For PTP (dynamic) clocks, timerfd_create doesn't support dynamic clock IDs,
// so we use eventfd + a persistent busy-poll thread instead.
class TimerFD : public folly::EventHandler {
 public:
  explicit TimerFD(folly::EventBase* eventBase)
      : folly::EventHandler(eventBase) {
    clkSrc_ = getClockSourceInstance();
    // timerfd_create doesn't support dynamic (PTP) clock IDs; use eventfd +
    // busy-poll thread instead
    fd_ = isPtpClock()
        ? ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)
        : ::timerfd_create(clkSrc_->GetClockId(), TFD_NONBLOCK | TFD_CLOEXEC);
    if (fd_ < 0) {
      throw std::runtime_error(
          "failed to create timer fd: " + folly::errnoStr(errno));
    }
    initHandler(eventBase, folly::NetworkSocket::fromFd(fd_));
    if (isPtpClock()) {
      threadRunning_.store(true);
      timerThread_ = std::thread(
          [this, name = "lsmrPtp" + std::to_string(nextPtpTimerId())] {
            folly::setThreadName(name);
            ptpTimerThreadFunc();
          });
    }
  }

  ~TimerFD() override {
    if (fd_ > 0) {
      unregisterHandler();
      stopPtpThread();
      folly::fileops::close(fd_);
      fd_ = -1;
    }
  }

  virtual void onTimeout() noexcept = 0;

  void schedule(std::chrono::microseconds timeout) {
    setTimer(timeout.count() ? timeout : std::chrono::microseconds(1));
  }
  void cancel() {
    setTimer(std::chrono::microseconds(0));
  }

  void handlerReady(uint16_t /*events*/) noexcept override {
    uint64_t data = 0;
    auto res = folly::fileops::read(fd_, &data, sizeof(data));
    if (res < 0) {
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        VLOG(2) << "timerfd read error: " << folly::errnoStr(errno);
      }
      return;
    }
    onTimeout();
  }

  TimerFD(const TimerFD&) = delete;
  TimerFD& operator=(const TimerFD&) = delete;
  TimerFD(TimerFD&&) = delete;
  TimerFD& operator=(TimerFD&&) = delete;

 protected:
  void close() {
    if (fd_ > 0) {
      unregisterHandler();
      stopPtpThread();
      folly::fileops::close(fd_);
      fd_ = -1;
    }
  }

 private:
  std::unique_ptr<ClockSource> clkSrc_;
  int fd_{-1};
  bool isPtpClock() {
    auto const clkid = clkSrc_->GetClockId();
    // from timerfd_create(2) man page
    return clkid != CLOCK_REALTIME && clkid != CLOCK_MONOTONIC &&
        clkid != CLOCK_BOOTTIME && clkid != CLOCK_REALTIME_ALARM &&
        clkid != CLOCK_BOOTTIME_ALARM;
  }

  // PTP timer thread state
  std::thread timerThread_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> threadRunning_{false};
  // Monotonic cancellation token: bumped on reschedule/cancel/stop to
  // break the lock-free busy-poll loop in ptpTimerThreadFunc.
  std::atomic<uint64_t> generation_{0};
  ClockSource::time_point targetTime_;
  bool timerPending_{false}; // guarded by mutex_

  void stopPtpThread() {
    if (isPtpClock() && threadRunning_.load()) {
      threadRunning_.store(false);
      generation_.fetch_add(1); // break any in-progress busy-poll
      cv_.notify_one();
      if (timerThread_.joinable()) {
        timerThread_.join();
      }
    }
  }

  void ptpTimerThreadFunc() {
    while (threadRunning_.load()) {
      ClockSource::time_point target;
      uint64_t gen;
      {
        std::unique_lock<std::mutex> lk(mutex_);
        cv_.wait(
            lk, [this] { return timerPending_ || !threadRunning_.load(); });
        if (!threadRunning_.load()) {
          break;
        }
        target = targetTime_;
        timerPending_ = false;
        gen = generation_.load(); // snapshot to detect cancellation
      }

      // Busy-poll PTP clock until target time or generation change.
      // PTP (emulated) clocks may not correlate with system monotonic
      // time, so we cannot use timed sleeps. yield() reduces CPU waste.
      while (clkSrc_->now() < target && generation_.load() == gen) {
        std::this_thread::yield();
      }

      // Only fire if this timer wasn't canceled or rescheduled
      if (generation_.load() == gen) {
        uint64_t val = 1;
        auto const ret = folly::fileops::write(fd_, &val, sizeof(val));
        CHECK_GE(ret, 0) << "failed to write to eventfd: "
                         << folly::errnoStr(errno);
      }
    }
  }

  bool setTimer(std::chrono::microseconds useconds) {
    if (fd_ <= 0) {
      return false;
    }

    bool shouldRegister = (useconds.count() > 0);

    if (isPtpClock()) {
      std::lock_guard<std::mutex> lk(mutex_);
      generation_.fetch_add(1); // invalidate any in-progress busy-poll
      if (shouldRegister) {
        targetTime_ = clkSrc_->now() +
            std::chrono::duration_cast<ClockSource::duration>(useconds);
        timerPending_ = true;
        cv_.notify_one();
      } else {
        timerPending_ = false;
      }
    } else {
      struct itimerspec val{};
      val.it_interval = {0, 0};
      val.it_value.tv_sec =
          std::chrono::duration_cast<std::chrono::seconds>(useconds).count();
      val.it_value.tv_nsec =
          std::chrono::duration_cast<std::chrono::nanoseconds>(useconds)
              .count() %
          1000000000LL;

      if (::timerfd_settime(fd_, 0, &val, nullptr) != 0) {
        return false;
      }
    }

    if (shouldRegister && !isHandlerRegistered()) {
      registerHandler(folly::EventHandler::READ);
    } else if (!shouldRegister && isHandlerRegistered()) {
      unregisterHandler();
    }

    return true;
  }
};

class TimeoutManager : public folly::TimeoutManager, TimerFD {
 public:
  explicit TimeoutManager(folly::EventBase* eventBase)
      : TimerFD(eventBase), eventBase_(eventBase) {}
  ~TimeoutManager() override {
    cancel();
    close();
  }

  /**
   * Attaches/detaches TimeoutManager to AsyncTimeout
   */
  void attachTimeoutManager(folly::AsyncTimeout*, InternalEnum) final {}
  void detachTimeoutManager(folly::AsyncTimeout* obj) final {
    cancelTimeout(obj);
  }

  /**
   * Schedules AsyncTimeout to fire after `timeout` milliseconds
   */
  bool scheduleTimeout(folly::AsyncTimeout* obj, timeout_type timeout) final {
    timeout_type_high_res high_res_timeout = timeout;
    return scheduleTimeoutHighRes(obj, high_res_timeout);
  }

  /**
   * Schedules AsyncTimeout to fire after `timeout` microseconds
   */
  bool scheduleTimeoutHighRes(
      folly::AsyncTimeout* obj,
      timeout_type_high_res timeout) final {
    CHECK(obj_ == nullptr || obj_ == obj)
        << "Scheduling multiple timeouts on a single timeout manager is not allowed!";
    // no need to cancel - just reschedule
    obj_ = obj;
    setActive(obj, true);
    schedule(timeout);

    return true;
  }

  /**
   * Cancels the AsyncTimeout, if scheduled
   */
  void cancelTimeout(folly::AsyncTimeout* obj) final {
    if (obj == obj_) {
      setActive(obj, false);
      obj_ = nullptr;
      cancel();
    }
  }

  /**
   * This is used to mark the beginning of a new loop cycle by the
   * first handler fired within that cycle.
   */
  void bumpHandlingTime() final {}

  /**
   * Helper method to know whether we are running in the timeout manager
   * thread
   */
  bool isInTimeoutManagerThread() final {
    return eventBase_->isInEventBaseThread();
  }

  // from TimerFD
  void onTimeout() noexcept final {
    if (obj_) {
      auto* obj = obj_;
      obj_ = nullptr;
      setActive(obj, false);
      obj->timeoutExpired();
    }
  }

  TimeoutManager(const TimeoutManager&) = delete;
  TimeoutManager& operator=(const TimeoutManager&) = delete;
  TimeoutManager(TimeoutManager&&) = delete;
  TimeoutManager& operator=(TimeoutManager&&) = delete;

 private:
  static void setActive(folly::AsyncTimeout* obj, bool active) {
    if (obj) {
      auto* ev = obj->getEvent();
      if (active) {
        folly::event_ref_flags(ev->getEvent()) |= EVLIST_ACTIVE;
      } else {
        folly::event_ref_flags(ev->getEvent()) &= ~EVLIST_ACTIVE;
      }
    }
  }
  folly::EventBase* eventBase_{nullptr};
  folly::AsyncTimeout* obj_{nullptr};
};

} // namespace libsoftmetaroce
