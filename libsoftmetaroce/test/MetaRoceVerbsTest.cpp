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

#include <fmt/core.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <libsoftmetaroce/MetaRoceVerbs.h>
#include <libsoftmetaroce/QpState.h>
#include <libsoftmetaroce/test/AckProcessedAt.h>
#include <libsoftmetaroce/test/MockCongestionController.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>
#include <libsoftmetaroce/test/RecordingConnectionObserver.h>

#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/synchronization/Baton.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <linux/membarrier.h>
#include <net/if.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

using namespace testing;

DECLARE_string(libsoftmetaroce_gid);

namespace libsoftmetaroce {

DECLARE_int32(libsoftmetaroce_initialmsn);

static std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
    mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<MockUDPSocket>>(evb), nullptr /*modifier*/);
}

static std::function<std::pair<
    std::unique_ptr<folly::AsyncUDPSocket>,
    std::unique_ptr<Modifier>>(folly::EventBase*)>
alternatePortBindFailing(int bindErrno) {
  return [bindErrno](folly::EventBase* evb) {
    auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
    ON_CALL(*socket, bindImpl(_))
        .WillByDefault([bindErrno](const folly::SocketAddress& address) {
          if (address.getPort() == kMetaRoceBindToPortAlternate) {
            throw folly::AsyncSocketException(
                folly::AsyncSocketException::NOT_OPEN,
                "failed to bind the async udp socket",
                bindErrno);
          }
        });
    return std::make_pair(
        std::move(socket), std::unique_ptr<Modifier>(nullptr));
  };
}

/*
 * Polls until one completion arrives or the deadline passes.
 *
 * A posted WR reaches the completion queue only after the WQE processor thread
 * handles it, so even a work request that fails local validation completes
 * asynchronously; a single poll right after ibv_post_send races that thread.
 */
[[nodiscard]] static bool pollOneCompletion(
    libsoftmetaroce::MetaRoceVerbs& verbs,
    struct ibv_cq* cq,
    struct ibv_wc& wc) {
  constexpr auto kCompletionTimeout = std::chrono::seconds(10);
  const auto deadline = std::chrono::steady_clock::now() + kCompletionTimeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (verbs.ibv_poll_cq(cq, 1, &wc) == 1) {
      return true;
    }
    std::this_thread::yield();
  }
  return false;
}

class RetiringQpObserver final : public ConnectionObserver {
 public:
  // Called once the queue pair exists, which is after registration.
  void expectRetirementOf(uint32_t localQpNum) {
    expected_.store(localQpNum);
  }

  void onTagConfigured(const AppliedTagConfig& /*config*/) noexcept override {}
  void onQpRetiring(uint32_t localQpNum) noexcept override {
    // Atomic operations and folly::Baton::post() cannot throw, so this
    // callback upholds the noexcept contract.
    if (localQpNum != expected_.load()) {
      return;
    }
    // folly::Baton takes one post per lifetime and asserts on a second.
    if (!posted_.exchange(true)) {
      retired_.post();
    }
  }

  [[nodiscard]] bool tryWaitForRetirement() {
    constexpr auto kRetirementTimeout = std::chrono::seconds(10);
    return retired_.try_wait_for(kRetirementTimeout);
  }

 private:
  std::atomic<uint32_t> expected_{kMetaRoceInitialQpId - 1};
  std::atomic<bool> posted_{false};
  folly::Baton<> retired_;
};

/*
 * Creates a QP with the given parameters, modifies it to the IBV_QPS_RTS state
 * and returns it.
 */
static struct ibv_qp* setup_qp(
    libsoftmetaroce::MetaRoceVerbs& verbs,
    struct ibv_cq* cq,
    struct ibv_pd* pd,
    const folly::IPAddressV6& remote_addr,
    const uint32_t& dest_qp_num,
    const enum ibv_mtu& mtu,
    bool sqSigAll = true,
    std::optional<uint8_t> maxRdAtomic = std::nullopt,
    uint32_t maxWr = 0,
    uint32_t maxInlineData = 0) {
  struct ibv_qp_init_attr qp_init_attr{};
  qp_init_attr.send_cq = cq;
  qp_init_attr.recv_cq = cq;
  // Most tests expect every posted WR to complete; unsignaled-completion tests
  // pass false to exercise the per-WR IBV_SEND_SIGNALED behavior.
  qp_init_attr.sq_sig_all = sqSigAll ? 1 : 0;
  qp_init_attr.cap.max_send_wr = maxWr;
  qp_init_attr.cap.max_recv_wr = maxWr;
  qp_init_attr.cap.max_inline_data = maxInlineData;
  struct ibv_qp* ibvqp = verbs.ibv_create_qp(pd, &qp_init_attr);

  struct ibv_qp_attr init_attr{};
  memcpy(init_attr.ah_attr.grh.dgid.raw, remote_addr.toBinary().data(), 16);
  init_attr.dest_qp_num =
      MetaRoceVerbs::combineQpAndTag(dest_qp_num, kMetaRoceInitialTag);
  init_attr.path_mtu = mtu;
  init_attr.qp_state = IBV_QPS_INIT;
  int initMask = IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE;
  if (maxRdAtomic.has_value()) {
    init_attr.max_rd_atomic = *maxRdAtomic;
    initMask |= IBV_QP_MAX_QP_RD_ATOMIC;
  }
  verbs.ibv_modify_qp(ibvqp, &init_attr, initMask);
  init_attr.qp_state = IBV_QPS_RTR;
  verbs.ibv_modify_qp(ibvqp, &init_attr, IBV_QP_STATE);
  init_attr.qp_state = IBV_QPS_RTS;
  verbs.ibv_modify_qp(ibvqp, &init_attr, IBV_QP_STATE);
  return ibvqp;
}

class VerbsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cq_ = verbs_.ibv_create_cq(&context_, kCqEntries, nullptr, nullptr, 0);
    ASSERT_NE(cq_, nullptr);
    pd_ = verbs_.ibv_alloc_pd(&context_);
    ASSERT_NE(pd_, nullptr);
  }

  void TearDown() override {
    if (qp_ != nullptr) {
      EXPECT_EQ(verbs_.ibv_destroy_qp(qp_), 0);
    }
    if (pd_ != nullptr) {
      EXPECT_EQ(verbs_.ibv_dealloc_pd(pd_), 0);
    }
    if (cq_ != nullptr) {
      EXPECT_EQ(verbs_.ibv_destroy_cq(cq_), 0);
    }
  }

  // Returns the fixture's queue pair, in RTS under its protection domain and
  // on its completion queue for both send and receive.
  struct ibv_qp* fixtureQp() {
    if (qp_ == nullptr) {
      qp_ = setup_qp(
          verbs_,
          cq_,
          pd_,
          folly::IPAddressV6("fc00::1"),
          kDestQpNum,
          IBV_MTU_2048);
    }
    return qp_;
  }

  // One arrangement of two tags waiting on a shared receive queue, run from
  // the case below so a fatal check ends the arrangement, not the case.
  void runErroredTagLeavesTheReceive(
      bool erroredRefusedFirst,
      bool errorTheFirstCreated);

  static constexpr int kCqEntries = 10;
  static constexpr uint32_t kDestQpNum = 10;

  libsoftmetaroce::MetaRoceVerbs verbs_{mockUDPSocketMaker};
  struct ibv_context context_{};
  struct ibv_cq* cq_{nullptr};
  struct ibv_pd* pd_{nullptr};
  struct ibv_qp* qp_{nullptr};
};

// Listed ahead of `VerbsTest` below, so the observer is destroyed after the
// verbs object it joins, for as long as `registerConnectionObserver` asks.
class RecordingObserverHolder {
 protected:
  RecordingConnectionObserver observer_;
};

// Records what the verbs object publishes. An observer joins before any queue
// pair exists, so registration comes before the setup that can make one.
class ObservedVerbsTest : public RecordingObserverHolder, public VerbsTest {
 protected:
  void SetUp() override {
    verbs_.registerConnectionObserver(observer_);
    VerbsTest::SetUp();
  }
};

TEST(MetaRoceVerbsTest, ClosingADeviceGivesBackTheDescriptorItPolls) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  int numDevices = 0;
  struct ibv_device** deviceList = verbs.ibv_get_device_list(&numDevices);
  ASSERT_NE(deviceList, nullptr);
  ASSERT_GT(numDevices, 0)
      << "precondition: a device to open, or there is no context to close";

  struct ibv_context* context = verbs.ibv_open_device(deviceList[0]);
  ASSERT_NE(context, nullptr);
  // Read before the close, which frees the struct holding it.
  const int asyncFd = context->async_fd;

  EXPECT_EQ(verbs.ibv_close_device(context), 0);

  EXPECT_EQ(fcntl(asyncFd, F_GETFD), -1)
      << "the close gives back the async_fd a client polls, so the descriptor "
         "is no longer open";
}

TEST(MetaRoceVerbsTest, ConstructionRegistersTheProcessForHeavyFences) {
  // MEMBARRIER_CMD_GET_REGISTRATIONS, which older kernel headers lack.
  constexpr long kGetRegistrations = 1 << 9;
  const long supported = ::syscall(__NR_membarrier, MEMBARRIER_CMD_QUERY, 0, 0);
  if (supported < 0 || (supported & kGetRegistrations) == 0 ||
      (supported & MEMBARRIER_CMD_PRIVATE_EXPEDITED) == 0) {
    GTEST_SKIP() << "the kernel cannot report a private expedited membarrier "
                    "registration";
  }
  auto isRegistered = [] {
    return (::syscall(__NR_membarrier, kGetRegistrations, 0, 0) &
            MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED) != 0;
  };

  // A re-executed child starts unregistered, whatever earlier tests did.
  const std::string previousStyle = GTEST_FLAG_GET(death_test_style);
  SCOPE_EXIT {
    GTEST_FLAG_SET(death_test_style, previousStyle);
  };
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_EXIT(
      {
        const bool registeredBefore = isRegistered();
        libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
        std::fputs(
            registeredBefore     ? "registered before construction"
                : isRegistered() ? "construction registered the process"
                                 : "construction left the process unregistered",
            stderr);
        std::_Exit(0);
      },
      ExitedWithCode(0),
      "construction registered the process")
      << "construction registers a fresh process for private expedited "
         "membarrier, so no executor task pays for that registration once "
         "traffic flows";
}

TEST(MetaRoceVerbsTest, ANullCountStillReturnsTheDeviceList) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_device** deviceList = verbs.ibv_get_device_list(nullptr);
  EXPECT_NE(deviceList, nullptr)
      << "a caller that passes no count still gets the list, which is the "
         "return; rdma-core tolerates a null count the same way";
}

TEST(MetaRoceVerbsTest, TheDescriptorAClientPollsIsNotInheritedByAnExec) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  int numDevices = 0;
  struct ibv_device** deviceList = verbs.ibv_get_device_list(&numDevices);
  ASSERT_NE(deviceList, nullptr);
  ASSERT_GT(numDevices, 0)
      << "precondition: a device to open, or there is no descriptor to read";

  struct ibv_context* context = verbs.ibv_open_device(deviceList[0]);
  ASSERT_NE(context, nullptr);

  const int flags = fcntl(context->async_fd, F_GETFD);
  ASSERT_NE(flags, -1);
  EXPECT_TRUE(flags & FD_CLOEXEC)
      << "a client that forks and execs hands the child every descriptor "
         "without this flag, and the child would hold the pipe behind "
         "async_fd open past the close that gives it back";

  EXPECT_EQ(verbs.ibv_close_device(context), 0);
}

TEST(MetaRoceVerbsTest, ClosingNoContextAnswersMinusOne) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  errno = 0;
  EXPECT_EQ(verbs.ibv_close_device(nullptr), -1)
      << "ibv_close_device(3) answers -1 where it fails, not the errno value "
         "the destroy and post verbs answer with, so a caller testing against "
         "-1 sees the refusal";
  EXPECT_EQ(errno, EINVAL)
      << "and the reason is left in errno, which is where a -1 verb puts it";
}

TEST(MetaRoceVerbsTest, AContextRaisedNoEventAnswersThatNoneIsWaiting) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  int numDevices = 0;
  struct ibv_device** deviceList = verbs.ibv_get_device_list(&numDevices);
  ASSERT_GT(numDevices, 0)
      << "precondition: a device to open, or there is no context to ask";
  struct ibv_context* context = verbs.ibv_open_device(deviceList[0]);
  ASSERT_NE(context, nullptr);

  struct ibv_async_event event{};
  errno = 0;
  EXPECT_EQ(verbs.ibv_get_async_event(context, &event), -1)
      << "a context this library has raised no event against answers that none "
         "is waiting, rather than forwarding to ibv_get_async_event(3), whose "
         "read blocks on a descriptor this library never writes to";
  EXPECT_EQ(errno, EAGAIN)
      << "the caller polls a descriptor that is not ready, so the answer reads "
         "as it would from a non-blocking read finding nothing";

  EXPECT_EQ(verbs.ibv_close_device(context), 0);
}

TEST(
    MetaRoceVerbsTest,
    AnAsyncEventIsRefusedForAContextThisLibraryNeverOpened) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_context unopenedContext{};
  struct ibv_async_event event{};
  errno = 0;
  ASSERT_EQ(verbs.ibv_get_async_event(&unopenedContext, &event), -1)
      << "a context this object never opened holds no events of ours, and "
         "reading its async_fd would read a descriptor we know nothing about";
  EXPECT_EQ(errno, ENOENT) << "the context is not one this library holds";
}

TEST(MetaRoceVerbsTest, ClosingAContextThisLibraryNeverOpenedIsRefused) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_context unopenedContext{};
  errno = 0;
  EXPECT_EQ(verbs.ibv_close_device(&unopenedContext), -1)
      << "the close frees the struct it is given and closes the descriptors it "
         "reads out of it, so a context this object never opened is refused "
         "before either happens";
  EXPECT_EQ(errno, ENOENT) << "the context is not one this library holds";
}

TEST(MetaRoceVerbsTest, ACompletionQueueHandleIsNotReissuedToTheNextCreate) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_context context{};
  struct ibv_cq* first = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(first, nullptr);
  const uint32_t releasedHandle = first->handle;
  ASSERT_EQ(verbs.ibv_destroy_cq(first), 0)
      << "precondition: the destroy releases the handle, or the comparison "
         "below passes on one still held";

  struct ibv_cq* second =
      verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(second, nullptr);
  EXPECT_NE(second->handle, releasedHandle)
      << "a handle a destroy released must not identify the queue created "
         "next, or a struct still carrying that handle would reach the wrong "
         "queue";

  EXPECT_EQ(verbs.ibv_destroy_cq(second), 0);
}

TEST_F(VerbsTest, ACompletionQueueOfNoEntriesIsRefused) {
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_cq(&context_, 0, nullptr, nullptr, 0), nullptr)
      << "a queue that can hold no completion is one every post would overrun, "
         "so the request is refused rather than answered with a queue no "
         "caller can drain";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
}

TEST_F(VerbsTest, ACompletionQueueOfFewerThanNoEntriesIsRefused) {
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_cq(&context_, -1, nullptr, nullptr, 0), nullptr)
      << "the depth is signed, so a count below zero passes a check written "
         "only against the advertised ceiling";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
}

TEST_F(VerbsTest, ACompletionQueuePastTheAdvertisedDepthIsRefused) {
  errno = 0;
  EXPECT_EQ(
      verbs_.ibv_create_cq(&context_, kMaxCqEntries + 1, nullptr, nullptr, 0),
      nullptr)
      << "ibv_query_device advertises max_cqe as the deepest queue this device "
         "builds, so honoring a deeper one would make that answer a lie";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
}

TEST_F(VerbsTest, ACompletionQueueAtTheAdvertisedDepthIsCreated) {
  struct ibv_cq* deepest =
      verbs_.ibv_create_cq(&context_, kMaxCqEntries, nullptr, nullptr, 0);
  ASSERT_NE(deepest, nullptr)
      << "the advertised maximum is a depth the device claims to build, so "
         "refusing it would leave max_cqe advertising a queue no caller can "
         "have";
  EXPECT_GE(static_cast<uint32_t>(deepest->cqe), kMaxCqEntries)
      << "the queue reports at least the depth it was asked for, which is what "
         "a caller sizing its batch reads back";
  EXPECT_EQ(verbs_.ibv_destroy_cq(deepest), 0);
}

TEST_F(VerbsTest, ANotificationRequestOnAQueueWithoutAChannelIsRefused) {
  errno = 0;
  EXPECT_EQ(verbs_.ibv_req_notify_cq(cq_, 0), EINVAL)
      << "a queue created without a channel has nowhere to deliver the event, "
         "so arming it would promise a wakeup that can never come";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
}

// The fixture's queue pair reports into a completion queue created on a
// channel.
class CompChannelVerbsTest : public VerbsTest {
 protected:
  void SetUp() override {
    channel_ = verbs_.ibv_create_comp_channel(&context_);
    ASSERT_NE(channel_, nullptr);
    cq_ = verbs_.ibv_create_cq(&context_, kCqEntries, &cqContext_, channel_, 0);
    ASSERT_NE(cq_, nullptr);
    pd_ = verbs_.ibv_alloc_pd(&context_);
    ASSERT_NE(pd_, nullptr);
  }

  void TearDown() override {
    if (secondQp_ != nullptr) {
      EXPECT_EQ(verbs_.ibv_destroy_qp(secondQp_), 0);
    }
    if (secondCq_ != nullptr) {
      EXPECT_EQ(verbs_.ibv_destroy_cq(secondCq_), 0);
    }
    VerbsTest::TearDown();
    EXPECT_EQ(verbs_.ibv_destroy_comp_channel(channel_), 0);
  }

  // A second completion queue on the fixture's channel, with a queue pair
  // reporting into it.
  void addSecondQueue() {
    secondCq_ = verbs_.ibv_create_cq(
        &context_, kCqEntries, &secondCqContext_, channel_, 0);
    ASSERT_NE(secondCq_, nullptr);
    secondQp_ = setup_qp(
        verbs_,
        secondCq_,
        pd_,
        folly::IPAddressV6("fc00::1"),
        kDestQpNum + 1,
        IBV_MTU_2048);
    ASSERT_NE(secondQp_, nullptr);
  }

  // Completes one flushed send on the fixture's completion queue.
  void completeOneSend() {
    completeOneSendOn(fixtureQp(), cq_);
  }

  void completeOneSendOn(struct ibv_qp* ibvqp, struct ibv_cq* cq) {
    ASSERT_NE(ibvqp, nullptr);
    if (loadQpState(*ibvqp) != IBV_QPS_ERR) {
      struct ibv_qp_attr attr{};
      attr.qp_state = IBV_QPS_ERR;
      ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), 0);
    }
    struct ibv_send_wr wr{
        .wr_id = 1, .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED};
    struct ibv_send_wr* badWr = nullptr;
    ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &wr, &badWr), 0);
    struct ibv_wc wc{};
    ASSERT_TRUE(pollOneCompletion(verbs_, cq, wc))
        << "precondition: the send completes before the test checks the "
           "notification it raises";
  }

  bool channelHasAnEventWaiting() const {
    struct pollfd waiting{.fd = channel_->fd, .events = POLLIN};
    return poll(&waiting, 1, 0) == 1;
  }

  int cqContext_{0};
  int secondCqContext_{0};
  struct ibv_comp_channel* channel_{nullptr};
  struct ibv_cq* secondCq_{nullptr};
  struct ibv_qp* secondQp_{nullptr};
};

TEST_F(CompChannelVerbsTest, AnArmedQueueWakesItsChannelOncePerRequest) {
  completeOneSend();
  EXPECT_FALSE(channelHasAnEventWaiting())
      << "Notifications are opt-in; completing work must not wake an unarmed "
         "channel.";

  ASSERT_EQ(verbs_.ibv_req_notify_cq(cq_, 0), 0)
      << "precondition: the queue is armed before its completions are "
         "checked for events";
  completeOneSend();
  completeOneSend();
  ASSERT_TRUE(channelHasAnEventWaiting())
      << "the armed queue's completion wakes the waiting channel";
  struct ibv_cq* woken = nullptr;
  void* context = nullptr;
  ASSERT_EQ(verbs_.ibv_get_cq_event(channel_, &woken, &context), 0)
      << "precondition: the event is retrieved before the test checks which "
         "queue it names";
  EXPECT_EQ(woken, cq_)
      << "A shared channel must identify which CQ the client should poll.";
  EXPECT_EQ(context, &cqContext_)
      << "the event carries the context the queue was created with";
  EXPECT_FALSE(channelHasAnEventWaiting())
      << "a notification request arms one event, so a second completion "
         "raises none";
  verbs_.ibv_ack_cq_events(cq_, 1);
}

TEST_F(CompChannelVerbsTest, AQueueWithAnUnackedEventIsNotDestroyed) {
  ASSERT_EQ(verbs_.ibv_req_notify_cq(cq_, 0), 0)
      << "precondition: the queue is armed before its completions are "
         "checked for events";
  completeOneSend();
  struct ibv_cq* woken = nullptr;
  void* context = nullptr;
  ASSERT_EQ(verbs_.ibv_get_cq_event(channel_, &woken, &context), 0)
      << "precondition: the event is retrieved before the destroy that must "
         "refuse it";
  ASSERT_EQ(verbs_.ibv_destroy_qp(qp_), 0);
  qp_ = nullptr;

  errno = 0;
  EXPECT_EQ(verbs_.ibv_destroy_cq(cq_), EBUSY)
      << "The client still holds the CQ through the retrieved event.";
  EXPECT_EQ(errno, EBUSY)
      << "This failure must be reported through errno as well as the return "
         "value.";

  verbs_.ibv_ack_cq_events(woken, 1);
  EXPECT_EQ(verbs_.ibv_destroy_cq(cq_), 0)
      << "once the event is acked nothing holds the queue";
  cq_ = nullptr;
}

TEST_F(CompChannelVerbsTest, EventsPastThePipesCapacityAreAllDelivered) {
  const int capacity = fcntl(channel_->fd, F_SETPIPE_SZ, 4096);
  ASSERT_GT(capacity, 0)
      << "precondition: the pipe is shrunk so filling it stays quick";
  const int queued = capacity + 1;
  for (int raised = 0; raised < queued; ++raised) {
    ASSERT_EQ(verbs_.ibv_req_notify_cq(cq_, 0), 0)
        << "precondition: the queue is armed before its completions are "
           "checked for events";
    ASSERT_NO_FATAL_FAILURE(completeOneSend())
        << "precondition: a completion is taken before the test counts the "
           "event it raises";
  }
  ASSERT_EQ(fcntl(channel_->fd, F_SETFL, O_NONBLOCK), 0)
      << "precondition: reads fail rather than hang once no wakeup remains";

  struct ibv_cq* woken = nullptr;
  void* context = nullptr;
  int delivered = 0;
  for (; delivered < queued &&
       verbs_.ibv_get_cq_event(channel_, &woken, &context) == 0;
       ++delivered) {
  }
  EXPECT_EQ(delivered, queued)
      << "Pipe capacity must not limit the number of queued notifications.";
  verbs_.ibv_ack_cq_events(cq_, delivered);
}

TEST_F(CompChannelVerbsTest, QueuesSharingAChannelEachReportTheirOwnEvent) {
  ASSERT_NO_FATAL_FAILURE(addSecondQueue())
      << "precondition: a second queue shares the channel for the test to "
         "tell their events apart";
  ASSERT_EQ(verbs_.ibv_req_notify_cq(cq_, 0), 0)
      << "precondition: the queue is armed before its completions are "
         "checked for events";
  ASSERT_EQ(verbs_.ibv_req_notify_cq(secondCq_, 0), 0)
      << "precondition: the second queue is armed before its completions "
         "are checked for events";
  ASSERT_NO_FATAL_FAILURE(completeOneSend())
      << "precondition: a completion is taken before the test checks the "
         "event it raises";
  ASSERT_NO_FATAL_FAILURE(completeOneSendOn(secondQp_, secondCq_))
      << "precondition: the second queue takes a completion before the test "
         "checks the event it raises";
  ASSERT_EQ(fcntl(channel_->fd, F_SETFL, O_NONBLOCK), 0)
      << "precondition: reads fail rather than hang once no wakeup remains";

  struct ibv_cq* woken = nullptr;
  void* context = nullptr;
  ASSERT_EQ(verbs_.ibv_get_cq_event(channel_, &woken, &context), 0)
      << "precondition: the event is retrieved before the test checks which "
         "queue it names";
  EXPECT_EQ(woken, cq_) << "events come back oldest first";
  EXPECT_EQ(context, &cqContext_)
      << "the event carries the context the queue was created with";
  verbs_.ibv_ack_cq_events(woken, 1);
  ASSERT_EQ(verbs_.ibv_get_cq_event(channel_, &woken, &context), 0)
      << "taking one waiting event leaves the other for the next reader";
  EXPECT_EQ(woken, secondCq_)
      << "A shared channel must identify which CQ the client should poll.";
  EXPECT_EQ(context, &secondCqContext_)
      << "the event carries the context the queue was created with";
  verbs_.ibv_ack_cq_events(woken, 1);

  errno = 0;
  EXPECT_EQ(verbs_.ibv_get_cq_event(channel_, &woken, &context), -1)
      << "two events raised are two retrieved, with none invented after them";
  EXPECT_EQ(errno, EAGAIN) << "no wakeup reads as EAGAIN rather than a failure";
}

TEST_F(CompChannelVerbsTest, TwoReadersEachTakeOneOfTwoWaitingEvents) {
  for (int raised = 0; raised < 2; ++raised) {
    ASSERT_EQ(verbs_.ibv_req_notify_cq(cq_, 0), 0)
        << "precondition: the queue is armed before its completions are "
           "checked for events";
    ASSERT_NO_FATAL_FAILURE(completeOneSend())
        << "precondition: a completion is taken before the test checks the "
           "event it raises";
  }

  std::array<int, 2> results{-1, -1};
  std::array<struct ibv_cq*, 2> woken{};
  std::array<std::thread, 2> readers;
  for (size_t reader = 0; reader < readers.size(); ++reader) {
    readers[reader] = std::thread{[&, reader] {
      void* context = nullptr;
      results[reader] =
          verbs_.ibv_get_cq_event(channel_, &woken[reader], &context);
    }};
  }
  for (auto& reader : readers) {
    reader.join();
  }
  for (size_t reader = 0; reader < readers.size(); ++reader) {
    EXPECT_EQ(results[reader], 0)
        << "the reader holding the only wakeup hands it on while events "
           "remain";
    EXPECT_EQ(woken[reader], cq_)
        << "both waiting events belong to the one armed queue";
  }
  verbs_.ibv_ack_cq_events(cq_, 2);
}

TEST_F(CompChannelVerbsTest, ADestroyedQueuesEventDoesNotHideAnotherQueues) {
  ASSERT_NO_FATAL_FAILURE(addSecondQueue())
      << "precondition: a second queue shares the channel for the test to "
         "tell their events apart";
  ASSERT_EQ(verbs_.ibv_req_notify_cq(secondCq_, 0), 0)
      << "precondition: the second queue is armed before its completions "
         "are checked for events";
  ASSERT_NO_FATAL_FAILURE(completeOneSendOn(secondQp_, secondCq_))
      << "precondition: the second queue takes a completion before the test "
         "checks the event it raises";
  ASSERT_EQ(verbs_.ibv_destroy_qp(secondQp_), 0);
  secondQp_ = nullptr;
  EXPECT_EQ(verbs_.ibv_destroy_cq(secondCq_), 0)
      << "an event never taken holds no reference to its queue";
  secondCq_ = nullptr;

  ASSERT_EQ(verbs_.ibv_req_notify_cq(cq_, 0), 0)
      << "precondition: the queue is armed before its completions are "
         "checked for events";
  ASSERT_NO_FATAL_FAILURE(completeOneSend())
      << "precondition: a completion is taken before the test checks the "
         "event it raises";
  struct ibv_cq* woken = nullptr;
  void* context = nullptr;
  ASSERT_EQ(verbs_.ibv_get_cq_event(channel_, &woken, &context), 0)
      << "precondition: the event is retrieved before the test checks which "
         "queue it names";
  EXPECT_EQ(woken, cq_)
      << "the destroyed queue's event goes with it, leaving the live "
         "queue's event";
  verbs_.ibv_ack_cq_events(woken, 1);
}

TEST_F(CompChannelVerbsTest, AChannelAQueueReportsIntoIsNotDestroyed) {
  errno = 0;
  EXPECT_EQ(verbs_.ibv_destroy_comp_channel(channel_), EBUSY)
      << "a live queue would raise its next event into a freed channel";
  EXPECT_EQ(errno, EBUSY)
      << "This failure must be reported through errno as well as the return "
         "value.";
}

TEST_F(VerbsTest, ANullCompletionQueueIsRefusedAsAMalformedArgument) {
  struct ibv_qp_init_attr initAttr{};
  initAttr.qp_type = IBV_QPT_RC;
  // The guard reads the send queue first, so a null receive queue reaches it
  // only on the second pass.
  for (const bool nullSendCq : {true, false}) {
    initAttr.send_cq = nullSendCq ? nullptr : cq_;
    initAttr.recv_cq = nullSendCq ? cq_ : nullptr;
    errno = 0;
    EXPECT_EQ(verbs_.ibv_create_qp(pd_, &initAttr), nullptr)
        << "a null completion queue is a malformed argument, which the verbs "
           "answer with EINVAL; ENOENT would say lsmr looked and did not hold "
           "it. Null queue was the "
        << (nullSendCq ? "send" : "receive") << " one";
    EXPECT_EQ(errno, EINVAL)
        << "and the same code reaches a caller reading errno";
  }

  // The tag path reads its own queues, so the plain path's refusal does not
  // stand for it.
  struct ibv_qp_init_attr parentAttr{};
  parentAttr.send_cq = cq_;
  parentAttr.recv_cq = cq_;
  struct ibv_qp* parent = verbs_.ibv_create_qp(pd_, &parentAttr);
  ASSERT_NE(parent, nullptr);
  struct ibv_qp_init_attr_ex tagAttr{};
  tagAttr.qp_type = IBV_QPT_RC;
  tagAttr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  tagAttr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  tagAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(
      MetaRoceVerbs::qpAndTagToQp(parent->qp_num), 1);
  tagAttr.pd = pd_;
  tagAttr.send_cq = nullptr;
  tagAttr.recv_cq = cq_;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, &tagAttr), nullptr)
      << "adding a Tag under a null completion queue must be refused, since "
         "the Tag would post completions to a queue that is not there";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  errno = 0;
  EXPECT_EQ(verbs_.ibv_destroy_cq(nullptr), EINVAL)
      << "a null queue is the same malformed argument the create path above "
         "refuses";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  struct ibv_wc wc{};
  errno = 0;
  EXPECT_EQ(verbs_.ibv_poll_cq(nullptr, 1, &wc), -1)
      << "polling answers a count, so a null queue is refused with -1 rather "
         "than the zero completions an empty queue reports";
  EXPECT_EQ(errno, EINVAL) << "and the reason reaches a caller reading errno";

  EXPECT_EQ(verbs_.ibv_destroy_qp(parent), 0);
}

TEST_F(VerbsTest, ANullOutParameterIsRefusedAsAMalformedArgument) {
  errno = 0;
  EXPECT_EQ(verbs_.ibv_poll_cq(cq_, 1, nullptr), -1)
      << "polling copies completions into the array it is given, so a null "
         "one is refused with the count this verb answers failures by";
  EXPECT_EQ(errno, EINVAL) << "and the reason reaches a caller reading errno";
}

TEST_F(VerbsTest, PollingForNoCompletionsAnswersNone) {
  struct ibv_wc wc{};
  errno = 0;
  EXPECT_EQ(verbs_.ibv_poll_cq(cq_, 0, &wc), 0)
      << "ibv_poll_cq(3) makes a count of none a request the verb can serve, "
         "so a caller sizing its batch at zero is answered none rather than "
         "handed a failure";
}

TEST_F(VerbsTest, PollingForANegativeCountIsRefusedAsAMalformedArgument) {
  struct ibv_wc wc{};
  errno = 0;
  EXPECT_EQ(verbs_.ibv_poll_cq(cq_, -1, &wc), -1)
      << "a count below zero describes no array the verb could fill, and the "
         "caller reads the refusal off the return as it does for a null array";
  EXPECT_EQ(errno, EINVAL) << "and the reason reaches a caller reading errno";
}

TEST_F(VerbsTest, ANullWorkRequestIsRefusedAsAMalformedArgument) {
  // A queue pair lsmr holds, so the refusals below are the work request's.
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_send_wr sendWr{};
  struct ibv_send_wr* badSendWr = &sendWr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, nullptr, &badSendWr), EINVAL)
      << "posting reads the work request, so a null one is refused before "
         "that read";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badSendWr, nullptr)
      << "a caller reading bad_wr after a refusal must not find what it "
         "happened to hold; no request was posted, so none stopped a chain";
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &sendWr, nullptr), EINVAL)
      << "bad_wr is where a refusal reports the work request it stopped at, so "
         "a null one leaves the answer nowhere to go";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  struct ibv_recv_wr recvWr{};
  struct ibv_recv_wr* badRecvWr = &recvWr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, nullptr, &badRecvWr), EINVAL)
      << "posting a receive reads its work request the same way";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badRecvWr, nullptr)
      << "and the receive path clears bad_wr on that refusal as the send path "
         "does, so a caller reads the same thing from either";
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &recvWr, nullptr), EINVAL)
      << "and it needs somewhere to report the refused request just as much";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
}

TEST_F(VerbsTest, AQueuePairTypeLsmrDoesNotServeIsRefused) {
  struct ibv_qp_init_attr initAttr{};
  initAttr.send_cq = cq_;
  initAttr.recv_cq = cq_;
  for (const auto type :
       {IBV_QPT_UC,
        IBV_QPT_UD,
        IBV_QPT_RAW_PACKET,
        IBV_QPT_XRC_SEND,
        IBV_QPT_XRC_RECV,
        static_cast<enum ibv_qp_type>(0xf0)}) {
    initAttr.qp_type = type;
    errno = 0;
    EXPECT_EQ(verbs_.ibv_create_qp(pd_, &initAttr), nullptr)
        << "lsmr builds only RC queue pairs, so a create asking for type "
        << static_cast<int>(type) << " would get one it did not ask for";
    EXPECT_EQ(errno, EINVAL)
        << "and the same code reaches a caller reading errno, for type "
        << static_cast<int>(type);
  }
}

TEST_F(VerbsTest, ANullAttributeSetIsRefusedAsAMalformedArgument) {
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp(pd_, nullptr), nullptr)
      << "creating a queue pair reads the attributes before anything else, so "
         "a null set is refused there";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, nullptr), nullptr)
      << "the Tag path reads its own attributes, so it is refused the same way";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, nullptr, IBV_QP_STATE), EINVAL)
      << "modify reads the attributes it is told to apply, so a null set is "
         "refused before that read";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
}

/*
 * The verbs that take a queue pair dereference it, so a null one has to be
 * refused before any of them reads a field.
 */
TEST(MetaRoceVerbsTest, ANullQueuePairIsRefusedAsAMalformedArgument) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_qp_attr attr{};

  errno = 0;
  EXPECT_EQ(verbs.ibv_destroy_qp(nullptr), EINVAL)
      << "a null queue pair is a malformed argument rather than one lsmr "
         "looked for and did not hold";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  errno = 0;
  EXPECT_EQ(verbs.ibv_modify_qp(nullptr, &attr, IBV_QP_STATE), EINVAL)
      << "modify reads the handle first, so a null queue pair must be refused "
         "before that read";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  struct ibv_send_wr sendWr{};
  struct ibv_send_wr* badSendWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs.ibv_post_send(nullptr, &sendWr, &badSendWr), EINVAL)
      << "posting a send resolves the queue pair before it reads the work "
         "request, so a null one must be refused there too";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badSendWr, &sendWr)
      << "a refused post reports the work request it stopped at, which is what "
         "a caller reads after a non-zero return";

  struct ibv_recv_wr recvWr{};
  struct ibv_recv_wr* badRecvWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs.ibv_post_recv(nullptr, &recvWr, &badRecvWr), EINVAL)
      << "posting a receive resolves the queue pair the same way";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badRecvWr, &recvWr)
      << "and it reports the refused request the same way";
}

TEST(MetaRoceVerbsTest, AHandleMatchingNoQueuePairIsRefused) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  // These verbs read only qp_num and state, so a local handle stands in for a
  // destroyed one without reaching freed memory.
  struct ibv_qp stale{};
  stale.qp_num = 0x0badc0de;
  stale.state = IBV_QPS_RTS;

  struct ibv_qp_attr attr{};

  errno = 0;
  EXPECT_EQ(verbs.ibv_modify_qp(&stale, &attr, IBV_QP_STATE), ENOENT)
      << "modify acts on the queue pair itself, so a handle matching none is a "
         "queue pair this device does not hold rather than a malformed call";
  EXPECT_EQ(errno, ENOENT)
      << "and the same code reaches a caller reading errno";

  struct ibv_send_wr sendWr{};
  struct ibv_send_wr* badSendWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs.ibv_post_send(&stale, &sendWr, &badSendWr), EINVAL)
      << "posting a send reaches the queue pair for its tag's inline limit, so "
         "it has to refuse a handle matching none before that read";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badSendWr, &sendWr)
      << "a refused post reports the work request it stopped at, which is what "
         "a caller reads after a non-zero return";

  struct ibv_recv_wr recvWr{};
  struct ibv_recv_wr* badRecvWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs.ibv_post_recv(&stale, &recvWr, &badRecvWr), EINVAL)
      << "posting a receive hands the request to the queue pair, so it refuses "
         "a handle matching none the same way";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badRecvWr, &recvWr)
      << "and it reports the refused request the same way";

  struct ibv_qp_init_attr initAttr{};
  errno = 0;
  EXPECT_EQ(verbs.ibv_query_qp(&stale, &attr, IBV_QP_STATE, &initAttr), ENOENT)
      << "a state-only query is still a question about the queue pair, so it "
         "is refused on the same grounds rather than answered from the "
         "caller's own handle";
  EXPECT_EQ(errno, ENOENT)
      << "and the same code reaches a caller reading errno";
}

TEST_F(VerbsTest, ATagTheQueuePairDoesNotHoldIsRefused) {
  struct ibv_qp* qp = fixtureQp();
  ASSERT_NE(qp, nullptr);

  // The fixture holds only the initial Tag, so any other number matches none.
  struct ibv_qp unheldTag = *qp;
  unheldTag.qp_num = libsoftmetaroce::MetaRoceVerbs::combineQpAndTag(
      libsoftmetaroce::MetaRoceVerbs::qpAndTagToQp(qp->qp_num),
      kMetaRoceInitialTag + 1);

  struct ibv_send_wr sendWr{};
  sendWr.opcode = IBV_WR_SEND;
  struct ibv_send_wr* badSendWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(&unheldTag, &sendWr, &badSendWr), EINVAL)
      << "the send path reads the Tag's inline cap, so a Tag the queue pair "
         "does not hold has to be refused before that read";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badSendWr, &sendWr)
      << "a refused post reports the work request it stopped at";

  struct ibv_recv_wr recvWr{};
  struct ibv_recv_wr* badRecvWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_recv(&unheldTag, &recvWr, &badRecvWr), EINVAL)
      << "the receive path hands the work to the Tag, whose lookup throws "
         "rather than answering, so it refuses the same way";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badRecvWr, &recvWr)
      << "and it reports the refused request the same way";
}

TEST_F(VerbsTest, ASgeCountWithoutAListIsRefused) {
  struct ibv_qp* qp = fixtureQp();
  ASSERT_NE(qp, nullptr);

  struct ibv_send_wr sendWr{};
  sendWr.opcode = IBV_WR_SEND;
  sendWr.num_sge = 1;
  sendWr.sg_list = nullptr;
  struct ibv_send_wr* badSendWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(qp, &sendWr, &badSendWr), EINVAL)
      << "a count of one promises an entry, and the send path reads that "
         "entry's address and length; without a refusal a client that promised "
         "one and passed none ends the process";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badSendWr, &sendWr)
      << "a refused post reports the work request it stopped at, which is what "
         "a caller reads after a non-zero return";

  struct ibv_recv_wr recvWr{};
  recvWr.num_sge = 1;
  recvWr.sg_list = nullptr;
  struct ibv_recv_wr* badRecvWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_recv(qp, &recvWr, &badRecvWr), EINVAL)
      << "the receive path reads the same promised entry to keep its lkey on "
         "the work queue entry, so it refuses the same way";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badRecvWr, &recvWr)
      << "and it reports the refused request the same way";
}

TEST_F(VerbsTest, ANegativeSgeCountIsRefused) {
  struct ibv_qp* qp = fixtureQp();
  ASSERT_NE(qp, nullptr);

  // num_sge is signed, so a count below zero passes a check written only
  // against the upper bound.
  struct ibv_sge sge{};
  struct ibv_send_wr sendWr{};
  sendWr.opcode = IBV_WR_SEND;
  sendWr.num_sge = -1;
  sendWr.sg_list = &sge;
  struct ibv_send_wr* badSendWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(qp, &sendWr, &badSendWr), EINVAL)
      << "a count below zero describes no entry the send path can read; "
         "without a refusal it reaches the throw that ends the process, which "
         "is the answer a malformed count must not get";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badSendWr, &sendWr)
      << "a refused post reports the work request it stopped at";

  struct ibv_recv_wr recvWr{};
  recvWr.num_sge = -1;
  recvWr.sg_list = &sge;
  struct ibv_recv_wr* badRecvWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_recv(qp, &recvWr, &badRecvWr), EINVAL)
      << "the receive path throws on the same count, so it refuses it the same "
         "way";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badRecvWr, &recvWr)
      << "and it reports the refused request the same way";
}

// A null domain is no entry to look for, which is a different answer from
// looking and finding none.
TEST(MetaRoceVerbsTest, ANullProtectionDomainIsRefusedAsAMalformedArgument) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  errno = 0;
  EXPECT_EQ(verbs.ibv_dealloc_pd(nullptr), EINVAL)
      << "a null domain is a malformed argument rather than one lsmr looked "
         "for and did not hold";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
}

TEST(
    MetaRoceVerbsTest,
    AByteIdenticalCopyOfAProtectionDomainIsRefusedAtReleaseAndRegistration) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  struct ibv_context context{};
  struct ibv_pd* const allocated = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(allocated, nullptr);
  struct ibv_pd copy = *allocated;

  errno = 0;
  EXPECT_EQ(verbs.ibv_dealloc_pd(&copy), ENOENT)
      << "lsmr holds the domain it handed out, not every struct carrying the "
         "same bytes, so there is no domain here for it to release";

  std::array<char, 64> buffer{};
  errno = 0;
  EXPECT_EQ(
      verbs.ibv_reg_mr_internal(&copy, buffer.data(), buffer.size(), 0),
      nullptr)
      << "a region reaches its domain through the same membership, so a copy "
         "is refused before any memory is looked at";
  EXPECT_EQ(errno, EINVAL)
      << "the domain is where the region goes rather than what the verb acts "
         "on, which returns EINVAL";

  EXPECT_EQ(verbs.ibv_dealloc_pd(allocated), 0);
}

TEST(MetaRoceVerbsTest, AProtectionDomainPointingToAnotherContextIsRefused) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  struct ibv_context allocatingContext{};
  struct ibv_context otherContext{};
  struct ibv_pd* const pd = verbs.ibv_alloc_pd(&allocatingContext);
  ASSERT_NE(pd, nullptr);

  pd->context = &otherContext;
  errno = 0;
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), ENOENT)
      << "the domain belongs to the context it was allocated on, so the other "
         "context holds no such domain to release";

  pd->context = &allocatingContext;
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0)
      << "the refused release left the domain held by its own context";
}

// A registration and a release racing on one domain, run under
// AddressSanitizer, which reports a region left pointing at a freed domain.
TEST(MetaRoceVerbsTest, ADomainIsNotFreedUnderAConcurrentRegistration) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  struct ibv_context context{};
  struct ibv_pd* const pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);

  std::atomic<bool> released{false};
  std::array<char, 4096> buffer{};
  std::thread registrar([&]() {
    while (!released.load(std::memory_order_acquire)) {
      struct ibv_mr* const mr =
          verbs.ibv_reg_mr_internal(pd, buffer.data(), buffer.size(), 0);
      if (mr != nullptr) {
        EXPECT_EQ(verbs.ibv_dereg_mr(mr), 0);
      }
    }
  });

  int releaseResult = EBUSY;
  for (int attempt = 0; attempt < 100000 && releaseResult == EBUSY; ++attempt) {
    releaseResult = verbs.ibv_dealloc_pd(pd);
  }
  released.store(true, std::memory_order_release);
  registrar.join();

  EXPECT_EQ(releaseResult, 0)
      << "the registrar leaves the domain empty between registrations, so the "
         "release finds a moment with no region rather than being refused "
         "forever";
}

TEST(MetaRoceVerbsTest, ASecondReleaseOfAProtectionDomainIsRefused) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  struct ibv_context context{};
  struct ibv_pd* const pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  ASSERT_EQ(verbs.ibv_dealloc_pd(pd), 0)
      << "precondition: the domain is given up here, or the release below "
         "reaches one lsmr still holds";

  errno = 0;
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), ENOENT)
      << "the release gave the domain up, so a second one finds nothing to "
         "free rather than freeing it twice";
  EXPECT_EQ(errno, ENOENT)
      << "and the same code reaches a caller reading errno";
}

/*
 * A null region is no entry to look for, which is a different answer from
 * looking and finding none.
 */
TEST(MetaRoceVerbsTest, ANullRegionIsRefusedAsAMalformedArgument) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  errno = 0;
  EXPECT_EQ(verbs.ibv_dereg_mr(nullptr), EINVAL)
      << "a null region is a malformed argument, which the verbs answer with "
         "EINVAL; ENOENT would say lsmr looked and did not hold it";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
}

TEST(MetaRoceVerbsTest, ATagNumberTheQueuePairAlreadyHoldsIsRefused) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  struct ibv_context context{};
  struct ibv_cq* const cq =
      verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(cq, nullptr);
  struct ibv_pd* const pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);

  struct ibv_qp_init_attr initAttr{};
  initAttr.send_cq = cq;
  initAttr.recv_cq = cq;
  struct ibv_qp* const tag0 = verbs.ibv_create_qp(pd, &initAttr);
  ASSERT_NE(tag0, nullptr)
      << "precondition: the queue pair holding the tag number asked for again "
         "below, left in RESET so the add is attempted";

  struct ibv_qp_init_attr_ex again{};
  again.send_cq = cq;
  again.recv_cq = cq;
  again.qp_type = IBV_QPT_RC;
  again.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  again.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  again.source_qpn = tag0->qp_num;

  errno = 0;
  EXPECT_EQ(verbs.ibv_create_qp_ex(&context, &again), nullptr)
      << "the queue pair holds this tag already, which the verb reports by "
         "returning no queue pair";
  EXPECT_EQ(errno, EINVAL)
      << "asking for a tag the queue pair holds is a caller's mistake, which "
         "is the code the refusals beside it report";

  EXPECT_EQ(verbs.ibv_destroy_qp(tag0), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
}

// `ConnectionObserver::onTagConfigured` documents that the observer may call
// back into verbs.
class AllocatingObserver final : public ConnectionObserver {
 public:
  AllocatingObserver(
      libsoftmetaroce::MetaRoceVerbs& verbs,
      struct ibv_context& context)
      : verbs_(verbs), context_(context) {}

  void onTagConfigured(const AppliedTagConfig& /*config*/) noexcept override {
    allocated = verbs_.ibv_alloc_pd(&context_);
  }
  void onQpRetiring(uint32_t /*localQpNum*/) noexcept override {}

  struct ibv_pd* allocated{nullptr};

 private:
  libsoftmetaroce::MetaRoceVerbs& verbs_;
  struct ibv_context& context_;
};

TEST(MetaRoceVerbsTest, AnObserverAllocatesAProtectionDomainFromTheCallback) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  struct ibv_context context{};
  struct ibv_cq* const cq =
      verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(cq, nullptr);
  struct ibv_pd* const pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);

  AllocatingObserver observer(verbs, context);
  verbs.registerConnectionObserver(observer);

  struct ibv_qp_init_attr initAttr{};
  initAttr.send_cq = cq;
  initAttr.recv_cq = cq;
  struct ibv_qp* const qp = verbs.ibv_create_qp(pd, &initAttr);
  ASSERT_NE(qp, nullptr)
      << "precondition: the create that runs the observer below";

  EXPECT_NE(observer.allocated, nullptr)
      << "allocating a domain takes the write lock the creating verb read, "
         "which one thread cannot hold twice";

  EXPECT_EQ(verbs.ibv_destroy_qp(qp), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(observer.allocated), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
}

TEST_F(VerbsTest, ARegionRunningPastTheTopOfTheSpaceIsRefused) {
  // Registration records the start and the length; neither is read here.
  constexpr uint64_t kNearTheTop = std::numeric_limits<uint64_t>::max() - 8;
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  void* const nearTheTop = reinterpret_cast<void*>(kNearTheTop);
  errno = 0;
  EXPECT_EQ(verbs_.ibv_reg_mr_internal(pd_, nearTheTop, 4096, 0), nullptr)
      << "a start and a length whose end runs past the top of the space "
         "address pages no process holds";
  EXPECT_EQ(errno, EFAULT)
      << "and the same code reaches a caller reading errno";
}

TEST_F(VerbsTest, AnUnmappedRegionIsRefused) {
  // Reserved and released, so the span is addressable but held by no mapping.
  const size_t pageSize = sysconf(_SC_PAGESIZE);
  void* const released =
      mmap(nullptr, pageSize, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(released, MAP_FAILED);
  ASSERT_EQ(munmap(released, pageSize), 0)
      << "precondition: the reserved page goes back, or the registration below "
         "is offered a span that is still mapped and the refusal is not the "
         "one under test";

  errno = 0;
  EXPECT_EQ(verbs_.ibv_reg_mr_internal(pd_, released, pageSize, 0), nullptr)
      << "a device pins the pages of a region as it registers them, so a span "
         "no mapping holds is one no device could take";
  EXPECT_EQ(errno, EFAULT)
      << "and the same code reaches a caller reading errno";
}

TEST_F(VerbsTest, AReservedButUntouchedRegionIsRegistered) {
  const size_t pageSize = sysconf(_SC_PAGESIZE);
  void* const untouched = mmap(
      nullptr,
      pageSize * 4,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1,
      0);
  ASSERT_NE(untouched, MAP_FAILED);

  struct ibv_mr* const mr =
      verbs_.ibv_reg_mr_internal(pd_, untouched, pageSize * 4, 0);
  EXPECT_NE(mr, nullptr)
      << "a region a client has reserved and not yet written to is mapped, "
         "and registering it is what lets the client write to it";

  if (mr != nullptr) {
    EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
  }
  EXPECT_EQ(munmap(untouched, pageSize * 4), 0);
}

TEST_F(VerbsTest, ARegionOfNoLengthIsStillHeldToItsAddress) {
  const size_t pageSize = sysconf(_SC_PAGESIZE);
  void* const released =
      mmap(nullptr, pageSize, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(released, MAP_FAILED);
  ASSERT_EQ(munmap(released, pageSize), 0)
      << "precondition: the reserved page goes back, or the address below is "
         "one the process still holds";

  errno = 0;
  EXPECT_EQ(verbs_.ibv_reg_mr_internal(pd_, released, 0, 0), nullptr)
      << "a region of no length still carries an address, and mmap returns a "
         "page boundary, which is where a span of none reaches no page at all";
  EXPECT_EQ(errno, EFAULT)
      << "and the same code reaches a caller reading errno";
}

TEST_F(VerbsTest, ARegionReachingIntoAnUnmappedPageIsRefused) {
  const size_t pageSize = sysconf(_SC_PAGESIZE);
  char* const pair = static_cast<char*>(mmap(
      nullptr, pageSize * 2, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  ASSERT_NE(pair, MAP_FAILED);
  ASSERT_EQ(munmap(pair + pageSize, pageSize), 0)
      << "precondition: the second page goes back while the first stays, or "
         "the region below lies wholly inside one mapping";

  errno = 0;
  EXPECT_EQ(
      verbs_.ibv_reg_mr_internal(pd_, pair + pageSize - 8, 16, 0), nullptr)
      << "a region starting inside a mapped page and ending in an unmapped "
         "one is held by no single mapping, so its tail decides it";
  EXPECT_EQ(errno, EFAULT)
      << "and the same code reaches a caller reading errno";

  EXPECT_EQ(munmap(pair, pageSize), 0);
}

/*
 * Telling an mr from its neighbor needs a neighbor, so the case below starts
 * from two live registrations.
 */
class TwoRegisteredRegionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pd_ = verbs_.ibv_alloc_pd(&context_);
    ASSERT_NE(pd_, nullptr);
    first_ = verbs_.ibv_reg_mr_internal(
        pd_, firstBuffer_.data(), firstBuffer_.size(), IBV_ACCESS_LOCAL_WRITE);
    ASSERT_NE(first_, nullptr);
    second_ = verbs_.ibv_reg_mr_internal(
        pd_,
        secondBuffer_.data(),
        secondBuffer_.size(),
        IBV_ACCESS_LOCAL_WRITE);
    ASSERT_NE(second_, nullptr);
    ASSERT_NE(second_->rkey, first_->rkey)
        << "precondition: the two regions must hold different keys, or the "
           "copies below change nothing";
  }

  void TearDown() override {
    EXPECT_EQ(verbs_.ibv_dealloc_pd(pd_), 0);
  }

  libsoftmetaroce::MetaRoceVerbs verbs_{mockUDPSocketMaker};
  struct ibv_context context_{};
  struct ibv_pd* pd_{nullptr};
  std::array<char, 128> firstBuffer_{};
  std::array<char, 128> secondBuffer_{};
  struct ibv_mr* first_{nullptr};
  struct ibv_mr* second_{nullptr};
};

/*
 * reg_mr writes one value into handle, lkey and rkey, so overwriting the
 * handle alone leaves an mr no client of this instance was ever handed.
 */
TEST_F(TwoRegisteredRegionsTest, ARegionWearingAnotherHandleIsRefused) {
  const uint32_t allocatedHandle = second_->handle;
  second_->handle = first_->handle;
  errno = 0;
  // A wrongly accepted dereg frees the entry second_ points into, so the
  // restore below would write through it.
  ASSERT_EQ(verbs_.ibv_dereg_mr(second_), ENOENT)
      << "a region whose handle disagrees with its keys must be refused "
         "before any lookup, since no registration here produced one";
  EXPECT_EQ(errno, ENOENT)
      << "and the same code reaches a caller reading errno";

  second_->handle = allocatedHandle;
  EXPECT_EQ(verbs_.ibv_dereg_mr(first_), 0)
      << "the refused call must have left the region whose handle it borrowed "
         "registered, which is the registration it would have dropped";
  EXPECT_EQ(verbs_.ibv_dereg_mr(second_), 0)
      << "and its own registration untouched";
}

/*
 * A client errors its own queue pair to abandon work it no longer wants, and
 * the state it reads back is what tells it the abandonment took. lsmr raises
 * the same state from the watchdog and from a fatal NAK, so the client's edge
 * is one more writer of a state those sweeps already own.
 */
TEST_F(VerbsTest, AClientMayErrorItsQueuePair) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the queue pair must be running, or the transition "
         "below is not the one from service into error";

  struct ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_ERR;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), 0)
      << "erroring a queue pair is a transition every client may make; "
         "refusing it leaves the client no way to abandon its own work";
  EXPECT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "the state a client reads back is what tells it the queue pair will "
         "take no more work";

  // Terminal, so the sweeps that raise it find it already there.
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), 0)
      << "a second error transition must report success rather than fail a "
         "client for asking twice";
  EXPECT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "the repeat must leave the state where it was; moving it would give "
         "the sweeps that raise it a queue pair back in service";
}

// 8192 has no name in `ibv_mtu`; a client asks for it as one past the enum.
TEST_F(VerbsTest, AnMtuOutsideTheSupportedRunIsRefused) {
  struct ibv_qp_init_attr qp_init_attr{};
  qp_init_attr.send_cq = cq_;
  qp_init_attr.recv_cq = cq_;
  qp_init_attr.sq_sig_all = 1;
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qp_init_attr);
  ASSERT_NE(ibvqp, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  };

  struct ibv_qp_attr attr{};
  attr.path_mtu = static_cast<enum ibv_mtu>(0);
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_PATH_MTU), EINVAL)
      << "ibv_modify_qp refuses the value below the enum, which would "
         "otherwise set an mtu of 128, a size no work request frames";

  attr.path_mtu = static_cast<enum ibv_mtu>(IBV_MTU_4096 + 2);
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_PATH_MTU), EINVAL)
      << "ibv_modify_qp refuses the first value past 8192, which would "
         "otherwise set an mtu of 16384 and abort the work request built on "
         "it";

  attr.path_mtu = static_cast<enum ibv_mtu>(IBV_MTU_4096 + 1);
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_PATH_MTU), 0)
      << "ibv_modify_qp accepts one past the enum, which is how a client "
         "asks for the 8192 this library frames at";
}

/*
 * A client may write the timeout and the retry count in one call or in two,
 * and both are refused past RTR. The deadline is derived on the move to RTS,
 * where each is final, so how the client split the writes cannot change it.
 */
TEST_F(VerbsTest, SplittingTheAttributeWritesDoesNotChangeTheDeadline) {
  constexpr uint8_t kTimeoutExponent = 14;
  constexpr uint8_t kRetryCount = 2;

  struct ibv_qp_init_attr qp_init_attr{};
  qp_init_attr.send_cq = cq_;
  qp_init_attr.recv_cq = cq_;
  qp_init_attr.sq_sig_all = 1;
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qp_init_attr);
  ASSERT_NE(ibvqp, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  };

  struct ibv_qp_attr attr{};
  memcpy(
      attr.ah_attr.grh.dgid.raw,
      folly::IPAddressV6("fc00::1").toBinary().data(),
      16);
  attr.dest_qp_num = MetaRoceVerbs::combineQpAndTag(10, kMetaRoceInitialTag);
  attr.path_mtu = IBV_MTU_2048;
  attr.qp_state = IBV_QPS_INIT;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(
          ibvqp,
          &attr,
          IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE),
      0)
      << "precondition: the QP must reach INIT, or the attribute writes below "
         "are refused for the state rather than accepted and derived from";

  // The timeout first and alone, so a deadline derived at either write would
  // take the retry count the QP was created with rather than the one below.
  attr.timeout = kTimeoutExponent;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_TIMEOUT), 0)
      << "precondition: the timeout must be accepted on its own, since writing "
         "it apart from the retry count is what this case exercises";
  attr.retry_cnt = kRetryCount;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_RETRY_CNT), 0)
      << "precondition: the retry count must be accepted in a later call, or "
         "the deadline below is derived from one value rather than two";

  attr.qp_state = IBV_QPS_RTR;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), 0)
      << "precondition: the QP must reach RTR, the state the move to RTS is "
         "taken from";
  attr.qp_state = IBV_QPS_RTS;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), 0)
      << "the move to RTS is where the deadline is derived, so a refused "
         "transition leaves nothing for the check below to read";

  // 4.096us << 14 is 67.108864ms, and three attempts of it round up to 202ms.
  constexpr auto kExpectedDeadline = std::chrono::milliseconds(202);
  auto* qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  EXPECT_EQ(qp->getWatchdogIntervalMs(), kExpectedDeadline)
      << "a client that writes the two attributes separately must get the "
         "deadline both of them set";
}

TEST_F(VerbsTest, TheLongestDeadlineAcrossTagsGovernsTheQp) {
  struct ibv_qp_init_attr qp_init_attr{};
  qp_init_attr.send_cq = cq_;
  qp_init_attr.recv_cq = cq_;
  qp_init_attr.sq_sig_all = 1;
  struct ibv_qp* tag0 = verbs_.ibv_create_qp(pd_, &qp_init_attr);
  ASSERT_NE(tag0, nullptr);
  const uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(tag0->qp_num);

  // Added while every Tag is still in RESET, which is the only time a Tag
  // joins a QP that already exists.
  struct ibv_qp_init_attr_ex tag1_attr{};
  tag1_attr.send_cq = cq_;
  tag1_attr.recv_cq = cq_;
  tag1_attr.qp_type = IBV_QPT_RC;
  tag1_attr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  tag1_attr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  tag1_attr.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  struct ibv_qp* tag1 = verbs_.ibv_create_qp_ex(&context_, &tag1_attr);
  ASSERT_NE(tag1, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_destroy_qp(tag1), 0);
    EXPECT_EQ(verbs_.ibv_destroy_qp(tag0), 0);
  };

  struct ibv_qp_attr attr{};
  memcpy(
      attr.ah_attr.grh.dgid.raw,
      folly::IPAddressV6("fc00::1").toBinary().data(),
      16);
  attr.dest_qp_num = MetaRoceVerbs::combineQpAndTag(10, kMetaRoceInitialTag);
  attr.path_mtu = IBV_MTU_2048;
  attr.qp_state = IBV_QPS_INIT;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(
          tag0,
          &attr,
          IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE),
      0)
      << "precondition: the QP must reach INIT, or the attribute writes below "
         "are refused for the state rather than accepted and derived from";

  // 4.096us << 14, three attempts: 202ms.
  attr.timeout = 14;
  attr.retry_cnt = 2;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(tag0, &attr, IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT), 0)
      << "precondition: the longer pair has to be accepted, or there is no "
         "deadline for the shorter one to lose to";
  // 4.096us << 12, one attempt: 17ms.
  attr.timeout = 12;
  attr.retry_cnt = 0;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(tag1, &attr, IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT), 0)
      << "precondition: the shorter pair has to be accepted on the second Tag, "
         "or this case has only one deadline in play";

  attr.qp_state = IBV_QPS_RTR;
  ASSERT_EQ(verbs_.ibv_modify_qp(tag0, &attr, IBV_QP_STATE), 0)
      << "precondition: the QP must reach RTR, the state the move to RTS is "
         "taken from";
  attr.qp_state = IBV_QPS_RTS;
  ASSERT_EQ(verbs_.ibv_modify_qp(tag0, &attr, IBV_QP_STATE), 0)
      << "the move to RTS is where the deadline is derived, so a refused "
         "transition leaves nothing for the check below to read";

  auto* qp = verbs_.getQpByNum(baseQpNum);
  ASSERT_NE(qp, nullptr);
  EXPECT_EQ(qp->getWatchdogIntervalMs(), std::chrono::milliseconds(202))
      << "one timer serves every Tag, so the Tag asking for the shorter "
         "deadline must not fail the work of the Tag that asked to wait "
         "longer";
}

/*
 * No timeout is a longer wait than any exponent can express, so a Tag asking
 * for it takes the same longest-wins rule and the QP is held to the disabled
 * watchdog rather than the other Tag's shorter deadline.
 */
TEST_F(VerbsTest, ATagAskingForNoTimeoutGovernsTheQpsDeadline) {
  struct ibv_qp_init_attr qp_init_attr{};
  qp_init_attr.send_cq = cq_;
  qp_init_attr.recv_cq = cq_;
  qp_init_attr.sq_sig_all = 1;
  struct ibv_qp* tag0 = verbs_.ibv_create_qp(pd_, &qp_init_attr);
  ASSERT_NE(tag0, nullptr);
  const uint32_t baseQpNum = MetaRoceVerbs::qpAndTagToQp(tag0->qp_num);

  struct ibv_qp_init_attr_ex tag1_attr{};
  tag1_attr.send_cq = cq_;
  tag1_attr.recv_cq = cq_;
  tag1_attr.qp_type = IBV_QPT_RC;
  tag1_attr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  tag1_attr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  tag1_attr.source_qpn = MetaRoceVerbs::combineQpAndTag(baseQpNum, 1);
  struct ibv_qp* tag1 = verbs_.ibv_create_qp_ex(&context_, &tag1_attr);
  ASSERT_NE(tag1, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_destroy_qp(tag1), 0);
    EXPECT_EQ(verbs_.ibv_destroy_qp(tag0), 0);
  };

  struct ibv_qp_attr attr{};
  memcpy(
      attr.ah_attr.grh.dgid.raw,
      folly::IPAddressV6("fc00::1").toBinary().data(),
      16);
  attr.dest_qp_num = MetaRoceVerbs::combineQpAndTag(10, kMetaRoceInitialTag);
  attr.path_mtu = IBV_MTU_2048;
  attr.qp_state = IBV_QPS_INIT;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(
          tag0,
          &attr,
          IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE),
      0)
      << "precondition: the QP must reach INIT, or the attribute writes below "
         "are refused for the state rather than accepted and derived from";

  // 4.096us << 14, three attempts: 202ms.
  attr.timeout = 14;
  attr.retry_cnt = 2;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(tag0, &attr, IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT), 0)
      << "precondition: the finite pair has to be accepted, or the disabled "
         "Tag below has nothing shorter to beat";
  attr.timeout = kDisabledLocalAckTimeoutExponent;
  ASSERT_EQ(verbs_.ibv_modify_qp(tag1, &attr, IBV_QP_TIMEOUT), 0)
      << "precondition: IBTA's zero has to be accepted as a timeout, since it "
         "is the request this case is about";

  attr.qp_state = IBV_QPS_RTR;
  ASSERT_EQ(verbs_.ibv_modify_qp(tag0, &attr, IBV_QP_STATE), 0)
      << "precondition: the QP must reach RTR, the state the move to RTS is "
         "taken from";
  attr.qp_state = IBV_QPS_RTS;
  ASSERT_EQ(verbs_.ibv_modify_qp(tag0, &attr, IBV_QP_STATE), 0)
      << "the move to RTS is where the deadline is derived, so a refused "
         "transition leaves nothing for the check below to read";

  auto* qp = verbs_.getQpByNum(baseQpNum);
  ASSERT_NE(qp, nullptr);
  EXPECT_EQ(qp->getWatchdogIntervalMs(), kDisabledTimeoutWatchdogMs)
      << "a Tag that asked for no timeout would otherwise have its work failed "
         "at another Tag's deadline, which is the opposite of what it asked";
}

/*
 * A client registers memory from whichever thread reaches it, and the receive
 * path validates against the same map while it does. The key counter and the
 * map it keys are read and written together, so a registration that sees a
 * torn map, or two that are handed one key, is a region the datapath cannot
 * find. Run under TSAN, where the unguarded pair reports a race.
 */
TEST_F(VerbsTest, RegistersMemoryFromManyThreadsAtOnce) {
  constexpr int kThreadCount = 8;
  constexpr int kRegionsPerThread = 64;
  std::vector<std::array<char, 128>> buffers(kThreadCount * kRegionsPerThread);
  std::mutex collectedMutex;
  std::set<uint32_t> keys;
  std::vector<ibv_mr*> regions;

  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);
  for (int t = 0; t < kThreadCount; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < kRegionsPerThread; ++i) {
        auto& buffer = buffers[t * kRegionsPerThread + i];
        ibv_mr* mr = verbs_.ibv_reg_mr_internal(
            pd_, buffer.data(), buffer.size(), IBV_ACCESS_LOCAL_WRITE);
        EXPECT_NE(mr, nullptr);
        if (mr != nullptr) {
          std::scoped_lock lock(collectedMutex);
          keys.insert(mr->lkey);
          regions.push_back(mr);
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(keys.size(), (size_t)(kThreadCount * kRegionsPerThread))
      << "every region must be handed a key of its own; two sharing one leaves "
         "the datapath validating against whichever registration landed last";

  for (ibv_mr* mr : regions) {
    EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
  }
}

TEST(MetaRoceVerbsTest, NextFreeIdSkipsNumbersInUse) {
  const std::unordered_set<uint32_t> inUse = {2, 3};
  uint32_t next = 2;
  const auto free =
      nextFreeId(next, 2, 5, [&](uint32_t n) { return inUse.contains(n); });
  EXPECT_EQ(free, 4u) << "allocation must skip numbers already in use";
  EXPECT_EQ(next, 5u) << "the cursor advances past the number handed out";
}

TEST(MetaRoceVerbsTest, NextFreeIdWrapsToReuseFreedNumbers) {
  const std::unordered_set<uint32_t> inUse = {4, 5};
  uint32_t next = 4;
  const auto free =
      nextFreeId(next, 2, 5, [&](uint32_t n) { return inUse.contains(n); });
  EXPECT_EQ(free, 2u)
      << "at the top of the range the scan must wrap and reuse a freed number";
}

TEST(MetaRoceVerbsTest, NextFreeIdFailsWhenEveryNumberInUse) {
  const std::unordered_set<uint32_t> inUse = {2, 3, 4, 5};
  uint32_t next = 2;
  EXPECT_FALSE(
      nextFreeId(next, 2, 5, [&](uint32_t n) { return inUse.contains(n); })
          .has_value())
      << "with the whole range in use the scan must come back around and give "
         "up rather than hand out a number it already passed";
}

/*
 * Writes a ModifierConfig to a temp file and passes the path to the file to
 * readModifierConfigFile()
 */
TEST(MetaRoceVerbsTest, ReadModifierConfig) {
  char tmpl[] = "/tmp/modifier_config_testXXXXXX";
  int fd = mkstemp(tmpl);
  ASSERT_NE(fd, -1)
      << "precondition: unique temp path for the config round-trip";
  close(fd);
  const std::string tmpFile = tmpl;

  std::ofstream writer(tmpFile);
  ASSERT_TRUE(writer.is_open()) << "Failed to open the file for writing";
  const auto modifierConfigStr = R"({
        "name": "test_modifier",
        "type": "drop",
        "filter": {
            "direction": "rx",
            "packet_type": "data",
            "fields": {"fsn": 0, "flowId": 0, "dst_qp": 10}
        },
        "modification_times": 1
    })";
  writer << modifierConfigStr;
  writer.close();
  EXPECT_EQ(MetaRoceVerbs::readModifierConfigFile(tmpFile), modifierConfigStr);
  std::filesystem::remove(tmpFile);
}

/*
 * Creates two QPs and verifies that they are assigned different QP numbers.
 */
TEST_F(VerbsTest, CreateMultipleQps) {
  struct ibv_qp_init_attr qp_init_attr{};
  qp_init_attr.send_cq = cq_;
  qp_init_attr.recv_cq = cq_;
  struct ibv_qp* qp1 = verbs_.ibv_create_qp(pd_, &qp_init_attr);
  struct ibv_qp* qp2 = verbs_.ibv_create_qp(pd_, &qp_init_attr);
  const auto localQpNum1 = MetaRoceVerbs::qpAndTagToQp(qp1->qp_num);
  const auto localQpNum2 = MetaRoceVerbs::qpAndTagToQp(qp2->qp_num);

  EXPECT_NE(localQpNum1, localQpNum2);
  EXPECT_EQ(
      localQpNum1,
      verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(localQpNum1))
          ->getLocalQpNum());
  EXPECT_EQ(
      localQpNum2,
      verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(localQpNum2))
          ->getLocalQpNum());

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp1), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(qp2), 0);
}

/*
 * Regression test for the receive-vs-destroy use-after-free. A receive callback
 * caches a raw Qp* after dropping the qpNumToQp_ read lock, so ibv_destroy_qp
 * must wait out any in-flight receive before freeing the Qp. This pins a task
 * on the reader EventBase that holds the Qp* and is released only once the
 * destroy is under way, then reads through the pointer: if destroy freed the Qp
 * out from under the reader (no drain), the read is a use-after-free that ASAN
 * catches.
 */
TEST(MetaRoceVerbsTest, DestroyQpWaitsForInFlightReceive) {
  RetiringQpObserver retirement;
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  verbs.registerConnectionObserver(retirement);

  struct ibv_context context{};
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_cq* cq = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);

  struct ibv_qp* ibvqp =
      setup_qp(verbs, cq, pd, folly::IPAddressV6("fc00::1"), 10, IBV_MTU_2048);

  const auto localQpNum = MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num);
  retirement.expectRetirementOf(localQpNum);
  auto* qp = verbs.getQpByNum(localQpNum);
  ASSERT_NE(qp, nullptr) << "connected QP must exist before the destroy race";
  ASSERT_FALSE(qp->getLocalAddrs().empty())
      << "connected QP must carry a local GID so we can reach its receiver";
  auto* receiver = verbs.getReceiver(*qp->getLocalAddrs().begin());

  folly::Baton<> readerHoldingQp;
  folly::Baton<> qpErased;
  folly::Baton<> readerTouchedQp;
  // Mimic onDataAvailable holding the raw Qp* past the map lock, on the reader
  // thread, across the window in which ibv_destroy_qp runs.
  receiver->readerEventBaseForTest().runInEventBaseThread([&]() {
    Qp* held = qp;
    readerHoldingQp.post();
    qpErased.wait();
    EXPECT_EQ(held->getLocalQpNum(), localQpNum)
        << "the Qp must stay alive while an in-flight receive still holds it";
    readerTouchedQp.post();
  });

  readerHoldingQp.wait();
  std::thread destroyer([&]() {
    EXPECT_EQ(verbs.ibv_destroy_qp(ibvqp), 0)
        << "precondition: the erase waited for below is this destroy's";
  });
  // Release the reader only after the erase, so its touch below lands in the
  // window during which the Qp must remain alive despite being removed.
  // The retirement runs under the map's write lock and the erase follows it
  // there, so a lookup taking the read lock cannot observe the pair again.
  const bool retired = retirement.tryWaitForRetirement();
  EXPECT_TRUE(retired)
      << "precondition: the destroy publishes a retirement, which is what the "
         "wait ends on";
  if (retired) {
    EXPECT_EQ(verbs.getQpByNum(localQpNum), nullptr)
        << "precondition: the erase follows the retirement, so the touch that "
           "comes next is inside the removal window";
  }
  // Released on a timeout too: tearDownQp() drains in-flight receives before
  // dropping the pair, so the reader's pointer stays live either way.
  qpErased.post();
  destroyer.join();
  readerTouchedQp.wait();

  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

/*
 * Creates and modifies a QP then posts a send WQE. For each operation, it
 * verifies the attributes set by that operation.
 */
TEST(MetaRoceVerbsTest, PostSentWQE) {
  constexpr uint32_t numPacketsToSend = 5;
  std::atomic<uint32_t> numPacketsRegistered{0};
  std::atomic_flag allPacketsSentPosted = ATOMIC_FLAG_INIT;
  folly::Baton<> allPacketsSent;
  const auto trackedSocketMaker =
      [&](folly::EventBase* evb) -> SocketWithModifier {
    auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
    ON_CALL(*socket, write(_, _))
        .WillByDefault([&](const folly::SocketAddress&,
                           const std::unique_ptr<folly::IOBuf>& buf) {
          if (numPacketsRegistered.load(std::memory_order_acquire) >=
                  numPacketsToSend &&
              !allPacketsSentPosted.test_and_set(std::memory_order_relaxed)) {
            allPacketsSent.post();
          }
          return CHECK_NOTNULL(buf.get())->computeChainDataLength();
        });
    return std::make_pair(std::move(socket), nullptr /*modifier*/);
  };
  libsoftmetaroce::MetaRoceVerbs verbs(trackedSocketMaker);

  struct ibv_context context{};
  struct ibv_cq* cq = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);

  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  const auto destQpNum = 10;
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_qp* ibvqp =
      setup_qp(verbs, cq, pd, remoteAddr, destQpNum, IBV_MTU_2048);

  const auto localQpNum = MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num);
  const auto qp = verbs.getQpByNum(localQpNum);
  EXPECT_EQ(qp->getLocalQpNum(), localQpNum);
  EXPECT_EQ(qp->getRemoteQpNum(), destQpNum);
  EXPECT_EQ(qp->getRemoteAddrs().size(), 1);
  EXPECT_EQ(*qp->getRemoteAddrs().begin(), remoteAddr);
  EXPECT_EQ(qp->getMtu(), 2048);

  auto congestionController =
      std::make_unique<NiceMock<MockCongestionController>>();
  EXPECT_CALL(*congestionController, getAvailableWindowSize(_))
      .WillRepeatedly(Return(1));
  ON_CALL(*congestionController, onPacketSent(_)).WillByDefault([&](uint16_t) {
    numPacketsRegistered.fetch_add(1, std::memory_order_release);
  });
  qp->setCongestionController(std::move(congestionController));

  auto wqeSizeBytes = qp->getMtu() * numPacketsToSend;
  uint64_t wqeWrId = 200;
  std::unique_ptr<char[]> payload(new char[wqeSizeBytes]);
  ibv_mr* mr = verbs.ibv_reg_mr_internal(
      pd,
      payload.get(),
      wqeSizeBytes,
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_LOCAL_WRITE);
  ASSERT_NE(mr, nullptr);

  struct ibv_sge send_sge[1] = {
      {.addr = (uint64_t)payload.get(),
       .length = wqeSizeBytes,
       .lkey = mr->lkey}};
  struct ibv_send_wr send_wr = {
      .wr_id = wqeWrId,
      .sg_list = send_sge,
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_WRITE,
      .wr = {.rdma = {.remote_addr = 1, .rkey = 1}}};
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(qp->getNumPendingWqes(), 0)
      << "QP should have no pending wqe's before they are posted and processing starts";
  EXPECT_EQ(verbs.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0);
  // wqe processing was started by setup_qp when transitioning to RTS.

  ASSERT_TRUE(CHECK_NOTNULL(qp)->waitForWqesToBePosted(1));
  ASSERT_TRUE(allPacketsSent.try_wait_for(std::chrono::seconds(5)))
      << "sender did not write all packets before the ACK timeout";

  EXPECT_EQ(qp->getNumPendingWqes(), 1)
      << "wqe processing should make the send wqe pending";

  // Packets are assigned to paths in round-robin order, 1 packet
  // at a time. Calculate the number of packets expected on each path and
  // generate an ACK for that path accordingly.
  const auto packetsPerFlow = numPacketsToSend / qp->getNumTxFlows();
  const auto flowsWithExtraPacket = numPacketsToSend % qp->getNumTxFlows();
  for (uint16_t flowId = 0;
       flowId < std::min(numPacketsToSend, qp->getNumTxFlows());
       ++flowId) {
    const auto packetsToExpectOnThisFlow =
        flowId < flowsWithExtraPacket ? packetsPerFlow + 1 : packetsPerFlow;
    const auto nowNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    const auto ackHeader = AckHeader::AckHeaderBuilder()
                               .setCumulativeFsn(packetsToExpectOnThisFlow - 1)
                               .setMaxRcvdFsn(packetsToExpectOnThisFlow - 1)
                               .setAckStatus(AckStatus::ACK)
                               .setTimestamp(nowNs)
                               .build();
    qp->getTxFlowById(flowId)->processAckPacket(
        ackHeader, simulatedProcessedAtNs(ackHeader.getTimestamp()));
  }

  EXPECT_EQ(qp->getNumPendingWqes(), 0)
      << "WQE should have completed after processing the acks on all paths.";
  struct ibv_wc wc;
  if (verbs.ibv_poll_cq(cq, 1, &wc) > 0) {
    EXPECT_EQ(wc.status, WorkCompletionStatus::kSuccess);
    EXPECT_EQ(wc.byte_len, wqeSizeBytes);
    EXPECT_EQ(wc.wr_id, wqeWrId);
    EXPECT_EQ(
        wc.qp_num,
        MetaRoceVerbs::combineQpAndTag(
            qp->getLocalQpNum(), MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num)));
    EXPECT_EQ(wc.opcode, IBV_WC_RDMA_WRITE);
  } else {
    EXPECT_TRUE(false) << "expected to find a completion in the queue";
  }

  EXPECT_EQ(verbs.ibv_dereg_mr(mr), 0);
  EXPECT_EQ(verbs.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

/*
 * Tests that ibv_post_send accepts a zero-length IBV_WR_SEND posted with
 * a single SGE of length 0.
 */
TEST_F(VerbsTest, PostZeroLengthSendIsAccepted) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  // Register a small dummy MR so the SGE's lkey is valid; the SEND itself
  // carries no payload so the buffer is never read.
  char dummy[1] = {};
  ibv_mr* mr = verbs_.ibv_reg_mr_internal(pd_, dummy, sizeof(dummy), 0);
  ASSERT_NE(mr, nullptr);

  struct ibv_sge sge{
      .addr = (uint64_t)dummy,
      .length = 0,
      .lkey = mr->lkey,
  };
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "zero-length IBV_WR_SEND should be accepted";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";

  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
}

/*
 * Tests that ibv_post_send accepts a zero-length IBV_WR_SEND posted with
 * num_sge=0 (no SGE list). This is the other valid spelling of a zero-byte
 * SEND, posted with a null buffer address.
 */
TEST_F(VerbsTest, PostZeroLengthSendWithNoSgeIsAccepted) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "zero-length IBV_WR_SEND with num_sge=0 should be accepted";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";
}

TEST_F(VerbsTest, AMessageLongerThanItsQpsMtuCarriesIsRefused) {
  struct ibv_qp* const ibvqp = setup_qp(
      verbs_,
      cq_,
      pd_,
      folly::IPAddressV6("fc00::1"),
      kDestQpNum,
      IBV_MTU_256,
      /*sqSigAll=*/true,
      /*maxRdAtomic=*/1);
  ASSERT_NE(ibvqp, nullptr);
  constexpr uint32_t kLongest =
      OversizeWqeException::kMaxPacketsPerMessage * 256;

  struct ibv_sge sge{.addr = 0x1000, .length = kLongest + 1};
  for (const ibv_wr_opcode opcode : {IBV_WR_SEND, IBV_WR_RDMA_READ}) {
    struct ibv_send_wr wr{
        .wr_id = 1,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = opcode,
        .wr = {.rdma = {.remote_addr = 0x1000, .rkey = 1}}};
    struct ibv_send_wr* badWr = nullptr;
    EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &wr, &badWr), EINVAL)
        << "opcode " << opcode
        << " needs more than kMaxPacketsPerMessage packets at this mtu";
    EXPECT_EQ(badWr, &wr)
        << "a refusal points at the work request it stopped at";
  }

  sge.length = kLongest;
  struct ibv_send_wr send{
      .wr_id = 2, .sg_list = &sge, .num_sge = 1, .opcode = IBV_WR_SEND};
  struct ibv_send_wr* badWr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send, &badWr), 0)
      << "a message of exactly the longest length is posted";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(VerbsTest, PostSendWithMoreSgesThanTheDeviceReportsIsRefused) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  std::array<ibv_sge, kMaxSgesPerWorkRequest + 1> sges{};
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = sges.data(),
      .num_sge = static_cast<int>(sges.size()),
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  const int rc = verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr);
  EXPECT_EQ(rc, EINVAL)
      << "a rejected post must return the errno value libibverbs callers read "
         "from the return";
  EXPECT_EQ(bad_wr, &send_wr)
      << "a rejected WR must be reported via bad_wr, not abort the process";
}

TEST_F(VerbsTest, PostSendWithEverySgeTheDeviceReportsIsAccepted) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  std::array<char, 64 * kMaxSgesPerWorkRequest> buffer{};
  ibv_mr* mr = verbs_.ibv_reg_mr_internal(pd_, buffer.data(), buffer.size(), 0);
  ASSERT_NE(mr, nullptr);
  std::array<ibv_sge, kMaxSgesPerWorkRequest> sges{};
  for (size_t i = 0; i < sges.size(); ++i) {
    sges[i] = {
        .addr = reinterpret_cast<uint64_t>(buffer.data() + 64 * i),
        .length = 64,
        .lkey = mr->lkey,
    };
  }
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = sges.data(),
      .num_sge = static_cast<int>(sges.size()),
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "ibv_query_device advertises this many sges, so a WR using them all "
         "must be accepted";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";

  EXPECT_EQ(verbs_.ibv_destroy_qp(std::exchange(qp_, nullptr)), 0);
  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
}

/*
 * Each sge is validated under its own lkey, so one unregistered entry fails the
 * work request as a whole even when the others are registered.
 */
TEST_F(VerbsTest, PostSendWithOneUnregisteredSgeCompletesInError) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  std::array<char, 64> registered{};
  std::array<char, 64> unregistered{};
  ibv_mr* mr =
      verbs_.ibv_reg_mr_internal(pd_, registered.data(), registered.size(), 0);
  ASSERT_NE(mr, nullptr);
  std::array<ibv_sge, 2> sges{{
      {.addr = reinterpret_cast<uint64_t>(registered.data()),
       .length = registered.size(),
       .lkey = mr->lkey},
      {.addr = reinterpret_cast<uint64_t>(unregistered.data()),
       .length = unregistered.size(),
       .lkey = mr->lkey},
  }};
  struct ibv_send_wr send_wr{
      .wr_id = 7,
      .sg_list = sges.data(),
      .num_sge = static_cast<int>(sges.size()),
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "an out-of-range SGE must be reported through a completion";

  struct ibv_wc wc{};
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "the local protection error must surface as a completion";
  EXPECT_EQ(wc.status, IBV_WC_LOC_PROT_ERR)
      << "the second sge lies outside the region its lkey registered";
  EXPECT_EQ(wc.wr_id, 7) << "the completion must report the originating wr_id";

  EXPECT_EQ(verbs_.ibv_destroy_qp(std::exchange(qp_, nullptr)), 0);
  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
}

TEST_F(VerbsTest, AnInlineSendCopiesItsSgesAroundEmptyOnes) {
  qp_ = setup_qp(
      verbs_,
      cq_,
      pd_,
      folly::IPAddressV6("fc00::1"),
      kDestQpNum,
      IBV_MTU_2048,
      /*sqSigAll=*/true,
      /*maxRdAtomic=*/std::nullopt,
      /*maxWr=*/0,
      /*maxInlineData=*/64);
  struct ibv_qp* ibvqp = qp_;
  ASSERT_NE(ibvqp, nullptr);
  auto* qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  // A closed window keeps the WQE queued, so its payload can be read back.
  qp->setCongestionController(
      std::make_unique<NiceMock<MockCongestionController>>());

  std::array<uint8_t, 16> first{};
  std::array<uint8_t, 16> second{};
  std::vector<uint8_t> expected;
  for (auto* source : {&first, &second}) {
    for (auto& byte : *source) {
      byte = static_cast<uint8_t>(expected.size() + 1);
      expected.push_back(byte);
    }
  }
  const ibv_sge empty{.addr = 0, .length = 0};
  std::array<ibv_sge, 5> sges{{
      empty,
      {.addr = reinterpret_cast<uint64_t>(first.data()),
       .length = first.size()},
      empty,
      {.addr = reinterpret_cast<uint64_t>(second.data()),
       .length = second.size()},
      empty,
  }};
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = sges.data(),
      .num_sge = static_cast<int>(sges.size()),
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "empty sges, null-addressed or not, must not stop an inline payload "
         "being posted";
  first.fill(0);
  second.fill(0);

  ASSERT_TRUE(qp->waitForWqesToBePosted(1, std::chrono::milliseconds(1000)))
      << "precondition: the send reaches its tag to be read back";
  const Wqe* const wqe = qp->getTag(MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num))
                             ->getEarliestPendingWqe();
  ASSERT_NE(wqe, nullptr);
  ASSERT_EQ(wqe->getMessageSizeBytes(), expected.size())
      << "the message is the non-empty sges' bytes only";
  const folly::IOBuf payload =
      wqe->makeSendPayloadView(0, qp->getMtu(), wqe->getMessageSizeBytes());
  std::vector<uint8_t> sent;
  for (const folly::ByteRange range : payload) {
    sent.insert(sent.end(), range.begin(), range.end());
  }
  EXPECT_EQ(sent, expected)
      << "the WQE must hold the bytes as posted, gathered in sge order, even "
         "though the caller has since overwritten its sources";
}

TEST_F(VerbsTest, EmptySgesAreDroppedFromAPostedBuffer) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  auto* qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  // A closed window keeps the WQE queued, so its buffer can be inspected.
  qp->setCongestionController(
      std::make_unique<NiceMock<MockCongestionController>>());

  std::array<char, 64> buffer{};
  ibv_mr* mr = verbs_.ibv_reg_mr_internal(pd_, buffer.data(), buffer.size(), 0);
  ASSERT_NE(mr, nullptr);
  std::array<ibv_sge, 2> sges{{
      {.addr = 0, .length = 0, .lkey = 0xbadbad},
      {.addr = reinterpret_cast<uint64_t>(buffer.data()),
       .length = buffer.size(),
       .lkey = mr->lkey},
  }};
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = sges.data(),
      .num_sge = static_cast<int>(sges.size()),
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "zero-length SGEs require no local-key validation";

  ASSERT_TRUE(qp->waitForWqesToBePosted(1, std::chrono::milliseconds(1000)))
      << "precondition: the send reaches its tag to be inspected";
  const Wqe* const wqe = qp->getTag(MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num))
                             ->getEarliestPendingWqe();
  ASSERT_NE(wqe, nullptr);
  EXPECT_FALSE(wqe->hasLocalSges())
      << "dropping empty SGEs must preserve the single-buffer storage path";
  EXPECT_EQ(wqe->getBufferAddress(), buffer.data())
      << "the buffer is the one sge that holds bytes";
  EXPECT_EQ(wqe->getMessageSizeBytes(), buffer.size())
      << "the message is the bytes the sges hold";

  EXPECT_EQ(verbs_.ibv_destroy_qp(std::exchange(qp_, nullptr)), 0);
  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
}

TEST_F(VerbsTest, AnAtomicWithTwoSgesIsRefused) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  std::array<uint64_t, 2> operand{};
  std::array<ibv_sge, 2> sges{{
      {.addr = reinterpret_cast<uint64_t>(&operand[0]), .length = 4},
      {.addr = reinterpret_cast<uint64_t>(&operand[1]), .length = 4},
  }};
  struct ibv_send_wr atomic_wr{
      .wr_id = 1,
      .sg_list = sges.data(),
      .num_sge = static_cast<int>(sges.size()),
      .opcode = IBV_WR_ATOMIC_FETCH_AND_ADD,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &atomic_wr, &bad_wr), EINVAL)
      << "this provider takes an atomic's operand from exactly one sge";
  EXPECT_EQ(bad_wr, &atomic_wr) << "a rejected WR must be reported via bad_wr";
}

TEST_F(VerbsTest, PostSendWithNullSgListReturnsEinval) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr)
      << "precondition: this case posts to a live queue pair";

  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = nullptr,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), EINVAL)
      << "dereferencing the null list would crash instead of rejecting the WR";
  EXPECT_EQ(bad_wr, &send_wr)
      << "a rejected WR must be reported via bad_wr, not abort the process";
}

// ibv_post_send(3) posts a chain in order and stops at the first work request
// it refuses, which bad_wr reports.
TEST_F(VerbsTest, AChainedSendStopsAtTheWorkRequestItRefuses) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_send_wr third{
      .wr_id = 3,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr refused{
      .wr_id = 2,
      .next = &third,
      .sg_list = nullptr,
      .num_sge = 2,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr first{
      .wr_id = 1,
      .next = &refused,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &first, &bad_wr), EINVAL)
      << "the second work request asks for two scatter-gather entries without "
         "a list to hold them";
  EXPECT_EQ(bad_wr, &refused)
      << "bad_wr tells the caller which work request was not posted, so it "
         "knows the ones ahead of it were";

  auto* qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  EXPECT_TRUE(qp->waitForWqesToBePosted(1, std::chrono::milliseconds(1000)))
      << "the send ahead of the refused one must be posted, since a caller "
         "trusting bad_wr will not post it again";
}

TEST_F(VerbsTest, AChainOfValidSendsIsPostedWhole) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_send_wr second{
      .wr_id = 2,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr first{
      .wr_id = 1,
      .next = &second,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = &first;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &first, &bad_wr), 0)
      << "a client batching sends into one post expects every one of them to "
         "go out";
  EXPECT_EQ(bad_wr, nullptr) << "a post that refused nothing names no work "
                                "request in bad_wr";

  auto* qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  EXPECT_TRUE(qp->waitForWqesToBePosted(2, std::chrono::milliseconds(1000)))
      << "a zero return tells the caller every send in the chain was posted, "
         "so it will not post any of them again";
}

/*
 * ibv_post_send rejects a WR posted on a QP outside RTS: it returns an error
 * and reports the WR through bad_wr instead of aborting. Rejecting the WR
 * rather than queuing it is lsmr's design choice; this test takes that choice
 * as a precondition and verifies the rejection is graceful -- the process
 * survives.
 */
TEST_F(VerbsTest, PostSendOutsideRtsDoesNotAbort) {
  struct ibv_qp_init_attr init{.send_cq = cq_, .recv_cq = cq_};
  // A freshly created QP is in RESET, never brought up to RTS.
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &init);
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RESET);

  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  const int rc = verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr);
  EXPECT_EQ(rc, EINVAL)
      << "a rejected post must return the errno value libibverbs callers read "
         "from the return";
  EXPECT_EQ(bad_wr, &send_wr)
      << "ibv_post_send reports the rejected WR through bad_wr and returns an "
         "error rather than aborting the process";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(VerbsTest, AQueuePairReportsTheSendDepthItAskedFor) {
  struct ibv_qp_init_attr init{.send_cq = cq_, .recv_cq = cq_};
  init.cap.max_send_wr = 4;
  init.cap.max_recv_wr = 6;
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &init);
  ASSERT_NE(ibvqp, nullptr);

  EXPECT_EQ(init.cap.max_send_wr, 4u)
      << "ibv_create_qp(3) answers on the attributes it was given, so a caller "
         "sizing its work against the answer reads it here";

  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr queried{};
  ASSERT_EQ(verbs_.ibv_query_qp(ibvqp, &attr, IBV_QP_CAP, &queried), 0)
      << "precondition: the query answers, or the caps read below are "
         "whatever the attributes held before it";
  EXPECT_EQ(attr.cap.max_send_wr, 4u)
      << "a query reports the depth the create asked for; reporting the device "
         "ceiling instead would tell a caller it may post far more than the "
         "queue pair will accept";
  EXPECT_EQ(attr.cap.max_recv_wr, 6u)
      << "the receive depth is taken and reported the same way";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(VerbsTest, AQueuePairAskingForNoDepthTakesTheDefault) {
  struct ibv_qp_init_attr init{.send_cq = cq_, .recv_cq = cq_};
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &init);
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr queried{};
  ASSERT_EQ(verbs_.ibv_query_qp(ibvqp, &attr, IBV_QP_CAP, &queried), 0)
      << "precondition: the query answers, or the caps read below are "
         "whatever the attributes held before it";
  EXPECT_EQ(attr.cap.max_send_wr, kDefaultQpWorkRequests)
      << "a caller setting no depth gets a queue it can work with rather than "
         "the device's ceiling, which nothing asking for none has use for";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

static void expectCapGrantedAtCreate(
    const ibv_qp_cap& reported,
    const ibv_qp_cap& granted,
    std::string_view output) {
  EXPECT_EQ(reported.max_send_wr, granted.max_send_wr)
      << output << ": ibv_query_qp(3) reports the send depth create granted";
  EXPECT_EQ(reported.max_recv_wr, granted.max_recv_wr)
      << output << ": ibv_query_qp(3) reports the receive depth create granted";
  EXPECT_EQ(reported.max_send_sge, granted.max_send_sge)
      << output
      << ": ibv_query_qp(3) reports the send sge limit create granted";
  EXPECT_EQ(reported.max_recv_sge, granted.max_recv_sge)
      << output
      << ": ibv_query_qp(3) reports the receive sge limit create granted";
  EXPECT_EQ(reported.max_inline_data, granted.max_inline_data)
      << output << ": ibv_query_qp(3) reports the inline cap create granted";
}

TEST_F(VerbsTest, AStateOnlyQueryReportsHowTheQueuePairWasCreated) {
  struct ibv_cq* recvCq =
      verbs_.ibv_create_cq(&context_, kCqEntries, nullptr, nullptr, 0);
  ASSERT_NE(recvCq, nullptr);
  constexpr ibv_qp_cap kAskedCap{
      .max_send_wr = 4,
      .max_recv_wr = 6,
      .max_send_sge = 1,
      .max_recv_sge = 1,
      .max_inline_data = 32};
  int qpContext = 0;
  struct ibv_qp_init_attr init{
      .qp_context = &qpContext,
      .send_cq = cq_,
      .recv_cq = recvCq,
      .cap = kAskedCap,
      .qp_type = IBV_QPT_RC,
      .sq_sig_all = 1};
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &init);
  ASSERT_NE(ibvqp, nullptr);

  // Distinct initial values expose fields the query leaves unchanged.
  int otherContext = 0;
  struct ibv_srq otherSrq{};
  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr queried{
      .qp_context = &otherContext,
      .send_cq = recvCq,
      .recv_cq = cq_,
      .srq = &otherSrq,
      .cap = {.max_send_wr = 8, .max_recv_wr = 8, .max_inline_data = 64},
      .qp_type = IBV_QPT_UC,
      .sq_sig_all = 0};
  ASSERT_EQ(verbs_.ibv_query_qp(ibvqp, &attr, IBV_QP_STATE, &queried), 0)
      << "precondition: the query succeeds, or init_attr holds whatever it "
         "held before";
  EXPECT_EQ(attr.qp_state, IBV_QPS_RESET)
      << "ibv_query_qp(3) reports the state an IBV_QP_STATE mask asks for, "
         "alongside the init_attr it fills";
  EXPECT_EQ(queried.qp_context, &qpContext)
      << "ibv_query_qp(3) reports the qp_context given at create";
  EXPECT_EQ(queried.send_cq, cq_)
      << "ibv_query_qp(3) reports the send CQ bound at create";
  EXPECT_EQ(queried.recv_cq, recvCq)
      << "ibv_query_qp(3) reports the receive CQ bound at create, which "
         "differs from the send CQ here";
  EXPECT_EQ(queried.srq, nullptr)
      << "ibv_query_qp(3) reports no SRQ for a queue pair created without "
         "one";
  EXPECT_EQ(queried.qp_type, IBV_QPT_RC)
      << "ibv_query_qp(3) reports the transport type given at create";
  EXPECT_EQ(queried.sq_sig_all, 1)
      << "ibv_query_qp(3) reports the sq_sig_all given at create";
  expectCapGrantedAtCreate(
      queried.cap, init.cap, "a state-only query's init_attr");

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_cq(recvCq), 0);
}

TEST_F(VerbsTest, ACapQueryReportsOneCapThroughBothOutputs) {
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 1}};
  struct ibv_srq* srq = verbs_.ibv_create_srq(pd_, &srqInitAttr);
  ASSERT_NE(srq, nullptr);
  constexpr ibv_qp_cap kAskedCap{
      .max_send_wr = 5,
      .max_recv_wr = 3,
      .max_send_sge = 1,
      .max_recv_sge = 1,
      .max_inline_data = 100};
  struct ibv_qp_init_attr init{
      .send_cq = cq_,
      .recv_cq = cq_,
      .srq = srq,
      .cap = kAskedCap,
      .qp_type = IBV_QPT_RC,
      .sq_sig_all = 0};
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &init);
  ASSERT_NE(ibvqp, nullptr);

  // Each output starts with a different cap, so neither can pass by being
  // left as it was.
  struct ibv_qp_attr attr{
      .cap = {.max_send_wr = 9, .max_recv_wr = 9, .max_inline_data = 1}};
  struct ibv_qp_init_attr queried{
      .cap = {.max_send_wr = 7, .max_recv_wr = 7, .max_inline_data = 2},
      .sq_sig_all = 1};
  ASSERT_EQ(verbs_.ibv_query_qp(ibvqp, &attr, IBV_QP_CAP, &queried), 0)
      << "precondition: the query succeeds, or both caps hold whatever they "
         "held before";
  expectCapGrantedAtCreate(attr.cap, init.cap, "attr->cap");
  expectCapGrantedAtCreate(queried.cap, init.cap, "init_attr->cap");
  EXPECT_EQ(queried.srq, srq)
      << "ibv_query_qp(3) reports the SRQ given at create";
  EXPECT_EQ(queried.sq_sig_all, 0)
      << "ibv_query_qp(3) reports the sq_sig_all given at create, including "
         "when it is clear";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0);
}

TEST_F(VerbsTest, ASendDepthPastTheDeviceMaximumIsRefused) {
  struct ibv_qp_init_attr init{.send_cq = cq_, .recv_cq = cq_};
  init.cap.max_send_wr = kMaxQpWorkRequests + 1;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp(pd_, &init), nullptr)
      << "ibv_query_device advertises max_qp_wr as the deepest queue this "
         "device builds, so granting a deeper one would make that answer a lie";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
}

TEST_F(VerbsTest, ARefusedCreateLeavesTheRequestedDepthsAlone) {
  // No depth set, which a create that succeeds answers with the default.
  struct ibv_qp_init_attr init{.send_cq = cq_, .recv_cq = cq_};

  ASSERT_EQ(verbs_.ibv_create_qp(nullptr, &init), nullptr)
      << "precondition: the create is refused for its protection domain, "
         "which is checked after the depths are read";

  EXPECT_EQ(init.cap.max_send_wr, 0u)
      << "ibv_create_qp answers with the depths it granted only on a create "
         "that succeeded; a caller that retries this request must find what it "
         "asked for, not a default it never set";
  EXPECT_EQ(init.cap.max_recv_wr, 0u)
      << "and the receive depth is left as the caller wrote it for the same "
         "reason";
}

TEST_F(VerbsTest, PostingPastTheSendDepthIsRefused) {
  struct ibv_qp* ibvqp = setup_qp(
      verbs_,
      cq_,
      pd_,
      folly::IPAddressV6("fc00::1"),
      kDestQpNum,
      IBV_MTU_2048,
      true,
      std::nullopt,
      /*maxWr=*/4);
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_send_wr* bad_wr = nullptr;
  for (int posted = 0; posted < 4; ++posted) {
    struct ibv_send_wr send{
        .wr_id = static_cast<uint64_t>(posted),
        .sg_list = nullptr,
        .num_sge = 0,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };
    ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &send, &bad_wr), 0)
        << "precondition: the depth admits this many, or the refusal below is "
           "not the one under test; stopped at "
        << posted;
  }

  struct ibv_send_wr overflowing{
      .wr_id = 4,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &overflowing, &bad_wr), ENOMEM)
      << "ibv_post_send(3) answers ENOMEM for a send queue with no room, which "
         "is what tells a client to drain before posting again";
  EXPECT_EQ(bad_wr, &overflowing)
      << "a refused post reports the work request it stopped at";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

// ibv_post_recv(3) posts a chain in order and stops at the first work request
// it refuses, which bad_wr reports.
TEST_F(VerbsTest, AChainedRecvPostsEachWorkRequestAheadOfTheOneItRefuses) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_recv_wr refused{
      .wr_id = 2,
      .sg_list = nullptr,
      .num_sge = 2,
  };
  struct ibv_recv_wr first{
      .wr_id = 1,
      .next = &refused,
      .sg_list = nullptr,
      .num_sge = 0,
  };
  struct ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &first, &bad_wr), EINVAL)
      << "the second work request asks for two scatter-gather entries without "
         "a list to hold them";
  EXPECT_EQ(bad_wr, &refused)
      << "bad_wr tells the caller which work request was not posted";

  auto* qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  EXPECT_TRUE(qp->waitForRWqesToBePosted(1, std::chrono::milliseconds(1000)))
      << "the receive ahead of the refused one must be posted, or a sender "
         "counting on it meets a receiver not ready";
}

TEST_F(VerbsTest, PostRecvWithMoreSgesThanTheDeviceReportsIsRefused) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  std::array<ibv_sge, kMaxSgesPerWorkRequest + 1> sges{};
  struct ibv_recv_wr rcv_wr{
      .wr_id = 1,
      .sg_list = sges.data(),
      .num_sge = static_cast<int>(sges.size()),
  };
  struct ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &rcv_wr, &bad_wr), EINVAL)
      << "a rejected post must return the errno value libibverbs callers read "
         "from the return";
  EXPECT_EQ(bad_wr, &rcv_wr)
      << "a rejected multi-SGE recv WR must be reported via bad_wr, not abort";
}

TEST_F(VerbsTest, PostRecvWithNullSgListReturnsEinval) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr)
      << "precondition: this case posts to a live queue pair";

  struct ibv_recv_wr rcv_wr{
      .wr_id = 1,
      .sg_list = nullptr,
      .num_sge = 1,
  };
  struct ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &rcv_wr, &bad_wr), EINVAL)
      << "dereferencing the null list would crash instead of rejecting the WR";
  EXPECT_EQ(bad_wr, &rcv_wr)
      << "a rejected recv WR must be reported via bad_wr, not abort";
}

/*
 * ibv_post_recv rejects a WR posted on a QP still in RESET gracefully, before
 * the QP has been brought up.
 */
TEST_F(VerbsTest, PostRecvOnResetQpReturnsEinval) {
  struct ibv_qp_init_attr init{.send_cq = cq_, .recv_cq = cq_};
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &init);
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RESET);

  struct ibv_recv_wr rcv_wr{
      .wr_id = 1,
      .sg_list = nullptr,
      .num_sge = 0,
  };
  struct ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &rcv_wr, &bad_wr), EINVAL)
      << "a rejected post must return the errno value libibverbs callers read "
         "from the return";
  EXPECT_EQ(bad_wr, &rcv_wr)
      << "a recv WR rejected in RESET must be reported via bad_wr, not abort";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(VerbsTest, ASendFromANullRegisteredBufferCompletesInError) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  std::array<char, 64> registered{};
  ibv_mr* mr =
      verbs_.ibv_reg_mr_internal(pd_, registered.data(), registered.size(), 0);
  ASSERT_NE(mr, nullptr);
  ibv_sge sge{.addr = 0, .length = 64, .lkey = mr->lkey};
  struct ibv_send_wr send_wr{
      .wr_id = 9,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "a null non-inline source must be reported through a completion";

  struct ibv_wc wc{};
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "the local protection error must surface as a completion";
  EXPECT_EQ(wc.status, IBV_WC_LOC_PROT_ERR)
      << "no registered region holds address zero";
  EXPECT_EQ(wc.wr_id, 9) << "the completion must report the originating wr_id";

  EXPECT_EQ(verbs_.ibv_destroy_qp(std::exchange(qp_, nullptr)), 0);
  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
}

TEST_F(VerbsTest, AnInlineSendFromANullBufferIsRefused) {
  qp_ = setup_qp(
      verbs_,
      cq_,
      pd_,
      folly::IPAddressV6("fc00::1"),
      kDestQpNum,
      IBV_MTU_2048,
      /*sqSigAll=*/true,
      /*maxRdAtomic=*/std::nullopt,
      /*maxWr=*/0,
      /*maxInlineData=*/64);
  ASSERT_NE(qp_, nullptr);

  ibv_sge sge{.addr = 0, .length = 16};
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(qp_, &send_wr, &bad_wr), EINVAL)
      << "inline posting must reject a null source before copying";
  EXPECT_EQ(bad_wr, &send_wr) << "a rejected WR must be reported via bad_wr";
}

TEST_F(VerbsTest, AReceiveIntoANullBufferIsPostedForLazyValidation) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  ibv_sge sge{.addr = 0, .length = 64};
  struct ibv_recv_wr rcv_wr{.wr_id = 1, .sg_list = &sge, .num_sge = 1};
  struct ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &rcv_wr, &bad_wr), 0)
      << "receive-buffer validation is deferred until data arrives";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";
}

/*
 * An invalid local key is a terminal local protection error: the WR is accepted
 * and completes with IBV_WC_LOC_PROT_ERR, and the QP moves to the error state
 * (rather than the post being rejected).
 */
TEST_F(VerbsTest, PostSendInvalidLkeyCompletesInErrorAndErrorsQp) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  char buffer[64] = {};
  ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(buffer),
      .length = sizeof(buffer),
      .lkey = 0xbadbad, // never registered
  };
  struct ibv_send_wr send_wr{
      .wr_id = 7,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "an invalid local key is accepted and completed in error, not rejected";

  struct ibv_wc wc{};
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "the local protection error must surface as a completion";
  EXPECT_EQ(wc.status, IBV_WC_LOC_PROT_ERR)
      << "the completion must surface the local protection error status";
  EXPECT_EQ(wc.wr_id, 7) << "the completion must carry the originating wr_id";
  EXPECT_EQ(wc.opcode, IBV_WC_SEND)
      << "the error completion must report the originating SEND opcode";
  EXPECT_EQ(wc.byte_len, 0u)
      << "the send was refused before it reached the wire, so a byte count "
         "here would describe a transfer that never happened";
  EXPECT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "a local protection error must move the QP to the error state";
}

/*
 * Remote write and remote atomic each write into the region, so libibverbs
 * requires local write beside either. A registration asking for one without it
 * is refused rather than granted a region that cannot serve the access its own
 * flags carry.
 */
TEST_F(VerbsTest, RegisteringRemoteAccessWithoutLocalWriteIsRefused) {
  char buffer[64] = {};

  errno = 0;
  EXPECT_EQ(
      verbs_.ibv_reg_mr_internal(
          pd_, buffer, sizeof(buffer), IBV_ACCESS_REMOTE_WRITE),
      nullptr)
      << "a peer writing this region needs the responder able to write it too";
  EXPECT_EQ(errno, EINVAL) << "a refused registration reports through errno";

  errno = 0;
  EXPECT_EQ(
      verbs_.ibv_reg_mr_internal(
          pd_, buffer, sizeof(buffer), IBV_ACCESS_REMOTE_ATOMIC),
      nullptr)
      << "an atomic writes the region as well, so it carries the same "
         "requirement as a remote write";
  EXPECT_EQ(errno, EINVAL) << "a refused registration reports through errno";

  ibv_mr* const granted = verbs_.ibv_reg_mr_internal(
      pd_,
      buffer,
      sizeof(buffer),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  EXPECT_NE(granted, nullptr)
      << "with local write beside it the same request is registrable, so the "
         "refusals above are about the missing flag and not the region";

  EXPECT_EQ(verbs_.ibv_dereg_mr(granted), 0);
}

/*
 * IBV_SEND_SIGNALED gates only successful completions: a failed WR must still
 * raise a CQE even when the QP is not sq_sig_all and the WR is unsignaled, so
 * an error can never be silently dropped.
 */
TEST_F(VerbsTest, UnsignaledErrorStillCompletes) {
  struct ibv_qp* ibvqp = setup_qp(
      verbs_,
      cq_,
      pd_,
      folly::IPAddressV6("fc00::1"),
      /*dest_qp_num=*/10,
      IBV_MTU_2048,
      /*sqSigAll=*/false);

  char buffer[64] = {};
  ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(buffer),
      .length = sizeof(buffer),
      .lkey = 0xbadbad, // never registered
  };
  struct ibv_send_wr send_wr{
      .wr_id = 7,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = 0, // unsignaled: a successful WR here would raise no CQE
  };
  struct ibv_send_wr* bad_wr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "an invalid local key is accepted and completed in error, not rejected";

  struct ibv_wc wc{};
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "an errored WR must complete even when unsignaled; IBV_SEND_SIGNALED "
         "suppresses only successful completions";
  EXPECT_EQ(wc.status, IBV_WC_LOC_PROT_ERR)
      << "the completion must surface the local protection error status";
  EXPECT_EQ(wc.wr_id, 7) << "the completion must carry the originating wr_id";
  EXPECT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "a local protection error must move the QP to the error state "
         "regardless of whether the WR was signaled";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

/*
 * An invalid local key on an RDMA write completes in error like a send, but the
 * completion must report the originating write opcode, not a hardcoded SEND.
 */
TEST_F(VerbsTest, PostWriteInvalidLkeyCompletesWithWriteOpcode) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  char buffer[64] = {};
  ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(buffer),
      .length = sizeof(buffer),
      .lkey = 0xbadbad, // never registered
  };
  struct ibv_send_wr write_wr{
      .wr_id = 9,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_WRITE,
      .send_flags = IBV_SEND_SIGNALED,
  };
  write_wr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(buffer);
  write_wr.wr.rdma.rkey = 1;
  struct ibv_send_wr* bad_wr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &write_wr, &bad_wr), 0)
      << "an invalid local key is accepted and completed in error, not rejected";

  struct ibv_wc wc{};
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "the local protection error must surface as a completion";
  EXPECT_EQ(wc.status, IBV_WC_LOC_PROT_ERR)
      << "the completion must surface the local protection error status";
  EXPECT_EQ(wc.opcode, IBV_WC_RDMA_WRITE)
      << "the error completion must report the originating write opcode, not SEND";
}

/*
 * Once a QP is in the error state, a further posted send is flushed with
 * IBV_WC_WR_FLUSH_ERR rather than rejected, so a caller draining its send queue
 * after a failure sees a completion for every work request.
 */
TEST_F(VerbsTest, PostSendToErrorQpCompletesWithFlushError) {
  struct ibv_qp* const ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  char buffer[64] = {};
  ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(buffer),
      .length = sizeof(buffer),
      .lkey = 0xbadbad, // never registered -> drives the QP to error
  };
  struct ibv_send_wr* bad_wr = nullptr;
  struct ibv_send_wr badWr{
      .wr_id = 1,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &badWr, &bad_wr), 0)
      << "an invalid local key is accepted and completed in error, not rejected";
  struct ibv_wc wc{};
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "the local key error completes";
  ASSERT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "the local protection error must move the QP to the error state";
  EXPECT_EQ(wc.status, IBV_WC_LOC_PROT_ERR)
      << "the completion must carry the local protection error status";

  struct ibv_send_wr wr2{
      .wr_id = 2,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_WRITE,
      .send_flags = IBV_SEND_SIGNALED,
  };
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &wr2, &bad_wr), 0)
      << "a post to an error-state QP is flushed, not rejected";
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "the flushed WR must surface as a completion";
  EXPECT_EQ(wc.status, IBV_WC_WR_FLUSH_ERR)
      << "a WR posted to an error-state QP must complete with a flush error";
  EXPECT_EQ(wc.wr_id, 2)
      << "the flush completion must carry the originating wr_id";
}

/*
 * A work request that fails local validation reports its error only after the
 * requests posted before it. Those flush first, in posting order, so a caller
 * draining the completion queue never sees a later work request complete ahead
 * of an earlier one.
 */
TEST_F(VerbsTest, LocalErrorCompletesAfterEarlierWorkRequests) {
  struct ibv_qp* const ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  char buffer[64] = {};
  ibv_mr* const mr = verbs_.ibv_reg_mr_internal(pd_, buffer, sizeof(buffer), 0);
  ASSERT_NE(mr, nullptr);

  // The mock socket never acknowledges, so both of these are still pending when
  // the third work request fails and takes the queue pair down.
  ibv_sge validSge{
      .addr = reinterpret_cast<uint64_t>(buffer),
      .length = sizeof(buffer),
      .lkey = mr->lkey,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  for (uint64_t wrId : {1ULL, 2ULL}) {
    struct ibv_send_wr wr{
        .wr_id = wrId,
        .sg_list = &validSge,
        .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };
    ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &wr, &bad_wr), 0)
        << "a well formed send must be accepted";
  }

  ibv_sge badSge{
      .addr = reinterpret_cast<uint64_t>(buffer),
      .length = sizeof(buffer),
      .lkey = 0xbadbad, // never registered
  };
  struct ibv_send_wr badWr{
      .wr_id = 3,
      .sg_list = &badSge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
  };
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &badWr, &bad_wr), 0)
      << "an invalid local key is accepted and completed in error, not rejected";

  struct ibv_wc wc{};
  for (uint64_t expectedWrId : {1ULL, 2ULL}) {
    ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
        << "a work request posted before the failing one must still complete";
    EXPECT_EQ(wc.wr_id, expectedWrId)
        << "completions must be reported in the order the requests were posted";
    EXPECT_EQ(wc.status, IBV_WC_WR_FLUSH_ERR)
        << "a work request left pending when the queue pair errors is flushed";
  }
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "the failing work request must complete";
  EXPECT_EQ(wc.wr_id, 3)
      << "the failing request completes behind the requests posted before it";
  EXPECT_EQ(wc.status, IBV_WC_LOC_PROT_ERR)
      << "the failing request reports its own error, not the flush status";

  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
}

/*
 * A receive buffer is validated lazily, when a message arrives to be scattered
 * in, not at post time; posting one with an unregistered key is accepted.
 */
TEST_F(VerbsTest, PostRecvWithInvalidLkeyIsAcceptedForLazyValidation) {
  struct ibv_qp* const ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  char buffer[64] = {};
  ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(buffer),
      .length = sizeof(buffer),
      .lkey = 0xbadbad, // never registered
  };
  struct ibv_recv_wr rcv_wr{
      .wr_id = 9,
      .sg_list = &sge,
      .num_sge = 1,
  };
  struct ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &rcv_wr, &bad_wr), 0)
      << "an unregistered receive buffer is accepted; the protection error is "
         "reported when a message arrives, not rejected at post time";
  EXPECT_EQ(bad_wr, nullptr)
      << "a successful post_recv reports no failing WR via bad_wr";
  EXPECT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "posting a receive must not, on its own, move the QP to error";
}

/*
 * Tests that ibv_post_send accepts a zero-length IBV_WR_SEND_WITH_IMM.
 * Per spec 8.2.3, SEND with Immediate Present permits zero-byte payloads —
 * the immediate data carries the message meaning.
 */
TEST_F(VerbsTest, PostZeroLengthSendWithImmIsAccepted) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  char dummy[1] = {};
  ibv_mr* mr = verbs_.ibv_reg_mr_internal(pd_, dummy, sizeof(dummy), 0);
  ASSERT_NE(mr, nullptr);

  struct ibv_sge sge{
      .addr = (uint64_t)dummy,
      .length = 0,
      .lkey = mr->lkey,
  };
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND_WITH_IMM,
      .send_flags = IBV_SEND_SIGNALED,
      .imm_data = 0xDEADBEEF,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "zero-length IBV_WR_SEND_WITH_IMM should be accepted per spec 8.2.3";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";

  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
}

/*
 * Tests that ibv_post_send accepts a zero-length IBV_WR_SEND_WITH_IMM posted
 * with num_sge=0 (no SGE list), where the immediate data is the whole message.
 */
TEST_F(VerbsTest, PostZeroLengthSendWithImmAndNoSgeIsAccepted) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_SEND_WITH_IMM,
      .send_flags = IBV_SEND_SIGNALED,
      .imm_data = 0xDEADBEEF,
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "zero-length IBV_WR_SEND_WITH_IMM with num_sge=0 should be accepted";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";
}

/*
 * Tests that ibv_modify_qp() uses the full 32-bit dest_qp_num when setting
 * the remote QP number, rather than extracting only the lower 24 bits.
 */
TEST_F(VerbsTest, ModifyQpUsesFullDestQpNum) {
  struct ibv_qp_init_attr qp_init_attr = {
      .send_cq = cq_,
      .recv_cq = cq_,
  };
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qp_init_attr);
  auto qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));

  const uint32_t remoteQpNum = 0xFFFFFFFF;
  struct ibv_qp_attr attr = {
      .qp_state = IBV_QPS_INIT,
      .path_mtu = IBV_MTU_4096,
      .dest_qp_num = remoteQpNum,
  };

  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_DEST_QPN), 0);
  EXPECT_EQ(qp->getRemoteQpNum(), remoteQpNum);

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(ObservedVerbsTest, SuccessfulModifyPublishesDirectionalConfiguration) {
  gflags::FlagSaver flagSaver;
  FLAGS_libsoftmetaroce_ignore_modify_psn = false;

  struct ibv_qp_init_attr qpInitAttr{
      .send_cq = cq_,
      .recv_cq = cq_,
  };
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qpInitAttr);
  const auto localQpNum = MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num);
  ASSERT_EQ(observer_.tagConfigs.size(), 1)
      << "creating a tag must publish the sequence numbers it starts on";
  EXPECT_EQ(observer_.tagConfigs[0].sendCsn, kUnconfiguredStartingCsn)
      << "a created tag must start on core's own send CSN";
  EXPECT_EQ(observer_.tagConfigs[0].receiveCsn, kUnconfiguredStartingCsn)
      << "a created tag must start on core's own receive CSN";
  EXPECT_EQ(
      observer_.tagConfigs[0].sendMsn,
      static_cast<uint32_t>(FLAGS_libsoftmetaroce_initialmsn))
      << "a created tag must report the send msn core starts it on";
  EXPECT_FALSE(observer_.tagConfigs[0].receiveMsn.has_value())
      << "the tag-creation notification must leave receiveMsn absent because "
         "no receive MSN has been applied";
  observer_.tagConfigs.clear();

  constexpr uint16_t kSendCsn = 0x4567;
  constexpr uint16_t kReceiveCsn = 0x89AB;
  constexpr uint32_t kSendMsn = 0x0001;
  constexpr uint32_t kReceiveMsn = 0x0002;
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  struct ibv_qp_attr attr{};
  memcpy(attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  attr.dest_qp_num = 0x12345678;
  attr.sq_psn = (static_cast<uint32_t>(kSendCsn) << kPsnCsnShift) | kSendMsn;
  attr.rq_psn =
      (static_cast<uint32_t>(kReceiveCsn) << kPsnCsnShift) | kReceiveMsn;
  attr.path_mtu = IBV_MTU_4096;
  attr.qp_state = IBV_QPS_INIT;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(
          ibvqp,
          &attr,
          IBV_QP_AV | IBV_QP_DEST_QPN | IBV_QP_SQ_PSN | IBV_QP_RQ_PSN |
              IBV_QP_PATH_MTU | IBV_QP_STATE),
      0)
      << "the initial directional configuration must succeed";
  ASSERT_EQ(observer_.tagConfigs.size(), 1)
      << "the combined modify must publish one tag configuration";
  EXPECT_EQ(observer_.tagConfigs[0].localQpNum, localQpNum)
      << "the tag configuration must identify the local QP";
  EXPECT_EQ(observer_.tagConfigs[0].tag, kMetaRoceInitialTag)
      << "the tag configuration must identify the addressed tag";
  EXPECT_EQ(observer_.tagConfigs[0].remoteQpNum, attr.dest_qp_num)
      << "the tag configuration must carry the configured remote QP";
  EXPECT_EQ(observer_.tagConfigs[0].sendCsn, kSendCsn)
      << "ibv_modify_qp must publish the CSN field of SQ PSN, not the whole "
         "PSN";
  EXPECT_EQ(observer_.tagConfigs[0].receiveCsn, kReceiveCsn)
      << "ibv_modify_qp must publish the CSN field of RQ PSN, not the whole "
         "PSN";
  EXPECT_EQ(observer_.tagConfigs[0].sendMsn, kSendMsn)
      << "the send msn comes from the low half of SQ PSN, the half the CSN is "
         "not taken from";
  EXPECT_EQ(observer_.tagConfigs[0].receiveMsn, kReceiveMsn - 1)
      << "the receive side records the last delivered msn, one below the next "
         "msn the caller asked it to expect";

  attr.qp_state = IBV_QPS_RTR;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), 0)
      << "INIT to RTR must succeed once setup completes";
  EXPECT_EQ(observer_.tagConfigs.size(), 1)
      << "a state-only transition applies no connection parameter to publish";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(ObservedVerbsTest, FailedModifyPublishesWhatItApplied) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  observer_.tagConfigs.clear();

  struct ibv_qp_attr attr{};
  attr.dest_qp_num = 77;
  attr.rq_psn = 0x12340001;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_DEST_QPN | IBV_QP_RQ_PSN),
      EINVAL)
      << "precondition: this case needs a combined modify that core rejects "
         "only after applying its first field";
  ASSERT_EQ(
      verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num))
          ->getRemoteQpNum(),
      attr.dest_qp_num)
      << "precondition: the rejected modify leaves the destination field "
         "applied, which is what the observer has to be told about";
  ASSERT_EQ(observer_.tagConfigs.size(), 1)
      << "a partially applied modify must publish one configuration";
  ASSERT_TRUE(observer_.tagConfigs.back().remoteQpNum.has_value())
      << "the published configuration must carry the field the modify applied "
         "before it failed";
  EXPECT_EQ(*observer_.tagConfigs.back().remoteQpNum, attr.dest_qp_num)
      << "the published destination must match the one core kept";
  EXPECT_FALSE(observer_.tagConfigs.back().receiveCsn.has_value())
      << "the receive side was rejected before it applied, so publishing it "
         "would report a value core never took";
}

TEST_F(ObservedVerbsTest, RefusedModifyPublishesEarlierFieldUpdates) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  observer_.tagConfigs.clear();

  // Destination changes precede state validation, so refusal retains and
  // publishes the new destination.
  struct ibv_qp_attr attr{};
  attr.dest_qp_num = 88;
  attr.qp_state = IBV_QPS_INIT;
  errno = 0;
  EXPECT_EQ(
      verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_DEST_QPN | IBV_QP_STATE),
      EINVAL)
      << "EINVAL distinguishes an invalid state transition from a legal "
         "transition the implementation does not support";
  EXPECT_EQ(errno, EINVAL)
      << "C-style callers read errno even when earlier attributes were "
         "applied";
  EXPECT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "changing state would make the refused transition partially succeed";
  ASSERT_EQ(
      verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num))
          ->getRemoteQpNum(),
      attr.dest_qp_num)
      << "state refusal must not roll back the destination applied before "
         "validation";
  ASSERT_EQ(observer_.tagConfigs.size(), 1)
      << "without one publication, observers would miss the retained "
         "destination update";
  ASSERT_TRUE(observer_.tagConfigs.back().remoteQpNum.has_value())
      << "the publication must identify which field survived the refusal";
  EXPECT_EQ(*observer_.tagConfigs.back().remoteQpNum, attr.dest_qp_num)
      << "a different published destination would disagree with retained QP "
         "state";
}

TEST_F(VerbsTest, MovingBackFromRtsToRtrIsRefused) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: without RTS, this case would not exercise the "
         "backward transition";

  struct ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RTR;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), EINVAL)
      << "EINVAL distinguishes invalid backward sequencing from a legal "
         "transition the implementation does not support";
  EXPECT_EQ(errno, EINVAL)
      << "C-style callers rely on errno to observe the refusal";
  EXPECT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "changing state would make the refused transition partially succeed";
}

// ERR->RESET is legal, but this implementation cannot reset tags and flows.
TEST_F(VerbsTest, ErroredQueuePairReportsResetAsUnimplemented) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the fixture must start in RTS so this test can "
         "drive the QP to ERR";

  struct ibv_qp_attr errAttr{};
  errAttr.qp_state = IBV_QPS_ERR;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &errAttr, IBV_QP_STATE), 0)
      << "precondition: without entering ERR, this case would not exercise "
         "ERR-to-RESET";

  struct ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RESET;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), ENOSYS)
      << "ENOSYS distinguishes missing reset support from an invalid "
         "transition";
  EXPECT_EQ(errno, ENOSYS)
      << "C-style callers rely on errno to observe the unsupported "
         "transition";
  EXPECT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "moving to RESET would report failure after applying the unsupported "
         "reset";
}

// RTS->SQD is legal, but this implementation cannot suspend sends.
TEST_F(VerbsTest, RtsToSqdReportsUnimplemented) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: without RTS, this case would not exercise RTS-to-SQD";

  struct ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_SQD;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), ENOSYS)
      << "ENOSYS distinguishes missing send suspension from an invalid "
         "transition";
  EXPECT_EQ(errno, ENOSYS)
      << "C-style callers rely on errno to observe the unsupported "
         "transition";
  EXPECT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "moving to SQD would report failure after applying unsupported "
         "suspension";
}

TEST_F(VerbsTest, MovingToRtrWithoutAddressSetupIsRefused) {
  struct ibv_qp_init_attr qp_init_attr{
      .send_cq = cq_,
      .recv_cq = cq_,
  };
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qp_init_attr);
  ASSERT_NE(ibvqp, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  };

  struct ibv_qp_attr initAttr{};
  initAttr.qp_state = IBV_QPS_INIT;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &initAttr, IBV_QP_STATE), 0)
      << "precondition: without INIT, this case would not isolate missing "
         "address setup";

  struct ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RTR;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_STATE), EINVAL)
      << "accepting RTR without a port pair would expose a QP with no flows";
  EXPECT_EQ(errno, EINVAL)
      << "C-style callers rely on errno to diagnose missing address setup";
  EXPECT_EQ(ibvqp->state, IBV_QPS_INIT)
      << "entering RTR after refusal would expose a QP with no flows";
}

TEST_F(VerbsTest, AddressPairPastThePortTableIsRefused) {
  struct ibv_qp_init_attr qp_init_attr{
      .send_cq = cq_,
      .recv_cq = cq_,
  };
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qp_init_attr);
  ASSERT_NE(ibvqp, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  };

  struct ibv_qp_attr initAttr{};
  initAttr.qp_state = IBV_QPS_INIT;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &initAttr, IBV_QP_STATE), 0)
      << "precondition: without INIT, repeated IBV_QP_AV modifies would not "
         "exercise the supported address-setup path";

  struct ibv_qp_attr attr{};
  attr.ah_attr.grh.sgid_index = 0;
  for (uint32_t i = 0; i < kMaxPorts; ++i) {
    memset(attr.ah_attr.grh.dgid.raw, 0, 16);
    attr.ah_attr.grh.dgid.raw[15] = static_cast<uint8_t>(i + 1);
    ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_AV), 0)
        << "precondition: pair " << i
        << " failing before kMaxPorts would underfill the active-port "
           "representation";
  }
  memset(attr.ah_attr.grh.dgid.raw, 0, 16);
  attr.ah_attr.grh.dgid.raw[15] = static_cast<uint8_t>(kMaxPorts + 1);
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_AV), ENOSPC)
      << "accepting another logical port would require an unrepresentable "
         "active-port bit";
  EXPECT_EQ(errno, ENOSPC)
      << "ENOMEM would misreport fixed port capacity as allocator failure";
  EXPECT_EQ(
      verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num))
          ->getNumPortPairs(),
      kMaxPorts)
      << "growing the table would make the ENOSPC result disagree with QP "
         "state";
}

TEST(QpStateTransition, ClassifyQpTransition) {
  struct Case {
    ibv_qp_state from;
    ibv_qp_state to;
    QpTransition expected;
  };
  // Every pair of named states. Out-of-range values would trip UBSan on load,
  // so the classifier's default arms stay defensive and untested.
  const Case kCases[] = {
      // Requesting the current state changes nothing, except UNKNOWN and SQE
      // targets.
      {IBV_QPS_RESET, IBV_QPS_RESET, QpTransition::SUPPORTED},
      {IBV_QPS_INIT, IBV_QPS_INIT, QpTransition::SUPPORTED},
      {IBV_QPS_RTR, IBV_QPS_RTR, QpTransition::SUPPORTED},
      {IBV_QPS_RTS, IBV_QPS_RTS, QpTransition::SUPPORTED},
      {IBV_QPS_SQD, IBV_QPS_SQD, QpTransition::SUPPORTED},
      {IBV_QPS_SQE, IBV_QPS_SQE, QpTransition::INVALID},
      {IBV_QPS_ERR, IBV_QPS_ERR, QpTransition::SUPPORTED},
      {IBV_QPS_UNKNOWN, IBV_QPS_UNKNOWN, QpTransition::INVALID},
      // Forward bring-up performs.
      {IBV_QPS_RESET, IBV_QPS_INIT, QpTransition::SUPPORTED},
      {IBV_QPS_INIT, IBV_QPS_RTR, QpTransition::SUPPORTED},
      {IBV_QPS_RTR, IBV_QPS_RTS, QpTransition::SUPPORTED},
      // Operational states move to ERR.
      {IBV_QPS_INIT, IBV_QPS_ERR, QpTransition::SUPPORTED},
      {IBV_QPS_RTR, IBV_QPS_ERR, QpTransition::SUPPORTED},
      {IBV_QPS_RTS, IBV_QPS_ERR, QpTransition::SUPPORTED},
      {IBV_QPS_SQD, IBV_QPS_ERR, QpTransition::SUPPORTED},
      {IBV_QPS_SQE, IBV_QPS_ERR, QpTransition::SUPPORTED},
      // Every state recycles through RESET; only the bring-up performs one.
      {IBV_QPS_INIT, IBV_QPS_RESET, QpTransition::LEGAL_BUT_UNSUPPORTED},
      {IBV_QPS_RTR, IBV_QPS_RESET, QpTransition::LEGAL_BUT_UNSUPPORTED},
      {IBV_QPS_RTS, IBV_QPS_RESET, QpTransition::LEGAL_BUT_UNSUPPORTED},
      {IBV_QPS_SQD, IBV_QPS_RESET, QpTransition::LEGAL_BUT_UNSUPPORTED},
      {IBV_QPS_SQE, IBV_QPS_RESET, QpTransition::LEGAL_BUT_UNSUPPORTED},
      {IBV_QPS_ERR, IBV_QPS_RESET, QpTransition::LEGAL_BUT_UNSUPPORTED},
      // Suspending and resuming are legal but unimplemented.
      {IBV_QPS_RTS, IBV_QPS_SQD, QpTransition::LEGAL_BUT_UNSUPPORTED},
      {IBV_QPS_SQD, IBV_QPS_RTS, QpTransition::LEGAL_BUT_UNSUPPORTED},
      {IBV_QPS_SQE, IBV_QPS_RTS, QpTransition::LEGAL_BUT_UNSUPPORTED},
      // SQE is never a valid modify target.
      {IBV_QPS_RESET, IBV_QPS_SQE, QpTransition::INVALID},
      {IBV_QPS_INIT, IBV_QPS_SQE, QpTransition::INVALID},
      {IBV_QPS_RTR, IBV_QPS_SQE, QpTransition::INVALID},
      {IBV_QPS_RTS, IBV_QPS_SQE, QpTransition::INVALID},
      {IBV_QPS_SQD, IBV_QPS_SQE, QpTransition::INVALID},
      {IBV_QPS_ERR, IBV_QPS_SQE, QpTransition::INVALID},
      {IBV_QPS_UNKNOWN, IBV_QPS_SQE, QpTransition::INVALID},
      // Skipped states, backward steps, and anything UNKNOWN touches.
      {IBV_QPS_RTR, IBV_QPS_INIT, QpTransition::INVALID},
      {IBV_QPS_RTS, IBV_QPS_INIT, QpTransition::INVALID},
      {IBV_QPS_SQD, IBV_QPS_INIT, QpTransition::INVALID},
      {IBV_QPS_SQE, IBV_QPS_INIT, QpTransition::INVALID},
      {IBV_QPS_ERR, IBV_QPS_INIT, QpTransition::INVALID},
      {IBV_QPS_UNKNOWN, IBV_QPS_INIT, QpTransition::INVALID},
      {IBV_QPS_RESET, IBV_QPS_RTR, QpTransition::INVALID},
      {IBV_QPS_RTS, IBV_QPS_RTR, QpTransition::INVALID},
      {IBV_QPS_SQD, IBV_QPS_RTR, QpTransition::INVALID},
      {IBV_QPS_SQE, IBV_QPS_RTR, QpTransition::INVALID},
      {IBV_QPS_ERR, IBV_QPS_RTR, QpTransition::INVALID},
      {IBV_QPS_UNKNOWN, IBV_QPS_RTR, QpTransition::INVALID},
      {IBV_QPS_RESET, IBV_QPS_RTS, QpTransition::INVALID},
      {IBV_QPS_INIT, IBV_QPS_RTS, QpTransition::INVALID},
      {IBV_QPS_ERR, IBV_QPS_RTS, QpTransition::INVALID},
      {IBV_QPS_UNKNOWN, IBV_QPS_RTS, QpTransition::INVALID},
      {IBV_QPS_RESET, IBV_QPS_SQD, QpTransition::INVALID},
      {IBV_QPS_INIT, IBV_QPS_SQD, QpTransition::INVALID},
      {IBV_QPS_RTR, IBV_QPS_SQD, QpTransition::INVALID},
      {IBV_QPS_SQE, IBV_QPS_SQD, QpTransition::INVALID},
      {IBV_QPS_ERR, IBV_QPS_SQD, QpTransition::INVALID},
      {IBV_QPS_UNKNOWN, IBV_QPS_SQD, QpTransition::INVALID},
      {IBV_QPS_UNKNOWN, IBV_QPS_RESET, QpTransition::INVALID},
      {IBV_QPS_RESET, IBV_QPS_ERR, QpTransition::INVALID},
      {IBV_QPS_UNKNOWN, IBV_QPS_ERR, QpTransition::INVALID},
      {IBV_QPS_RESET, IBV_QPS_UNKNOWN, QpTransition::INVALID},
      {IBV_QPS_INIT, IBV_QPS_UNKNOWN, QpTransition::INVALID},
      {IBV_QPS_RTR, IBV_QPS_UNKNOWN, QpTransition::INVALID},
      {IBV_QPS_RTS, IBV_QPS_UNKNOWN, QpTransition::INVALID},
      {IBV_QPS_SQD, IBV_QPS_UNKNOWN, QpTransition::INVALID},
      {IBV_QPS_SQE, IBV_QPS_UNKNOWN, QpTransition::INVALID},
      {IBV_QPS_ERR, IBV_QPS_UNKNOWN, QpTransition::INVALID},
  };
  for (const auto& [from, to, expected] : kCases) {
    SCOPED_TRACE(
        testing::Message() << qpStateToString(from) << "->"
                           << qpStateToString(to));
    EXPECT_EQ(classifyQpTransition(from, to), expected)
        << "misclassification would refuse a legal transition or accept an "
           "invalid one";
  }
}

TEST_F(ObservedVerbsTest, UnrelatedModifyDoesNotNotifyObserver) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  observer_.tagConfigs.clear();

  struct ibv_qp_attr attr{};
  attr.dest_qp_num = 77;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_ALT_PATH), 0)
      << "precondition: this case needs a modify core accepts and applies no "
         "connection parameter for";
  EXPECT_TRUE(observer_.tagConfigs.empty())
      << "an unrelated successful modify applied no connection parameter, so "
         "it must not publish one";
}

TEST(MetaRoceVerbsTest, ObserverRegistrationIsPerVerbsInstance) {
  RecordingConnectionObserver observer;
  MetaRoceVerbs observedVerbs(mockUDPSocketMaker);
  MetaRoceVerbs otherVerbs(mockUDPSocketMaker);
  observedVerbs.registerConnectionObserver(observer);

  struct ibv_context observedContext{};
  struct ibv_context otherContext{};
  struct ibv_cq* observedCq =
      observedVerbs.ibv_create_cq(&observedContext, 10, nullptr, nullptr, 0);
  struct ibv_cq* otherCq =
      otherVerbs.ibv_create_cq(&otherContext, 10, nullptr, nullptr, 0);
  struct ibv_qp_init_attr observedInitAttr{
      .send_cq = observedCq,
      .recv_cq = observedCq,
  };
  struct ibv_qp_init_attr otherInitAttr{
      .send_cq = otherCq,
      .recv_cq = otherCq,
  };
  struct ibv_pd* observedPd = observedVerbs.ibv_alloc_pd(&observedContext);
  ASSERT_NE(observedPd, nullptr);
  struct ibv_pd* otherPd = otherVerbs.ibv_alloc_pd(&otherContext);
  ASSERT_NE(otherPd, nullptr);
  struct ibv_qp* observedQp =
      observedVerbs.ibv_create_qp(observedPd, &observedInitAttr);
  struct ibv_qp* otherQp = otherVerbs.ibv_create_qp(otherPd, &otherInitAttr);

  ASSERT_EQ(observer.tagConfigs.size(), 1)
      << "exactly one instance's tag creation must be observed";
  EXPECT_EQ(
      observer.tagConfigs[0].localQpNum,
      MetaRoceVerbs::qpAndTagToQp(observedQp->qp_num))
      << "the creation record must carry the shared first-QP number";
  observer.tagConfigs.clear();

  // Each instance numbers its own QPs, so both first QPs carry the same local
  // number; only a distinct remote QP number can tell the two updates apart.
  constexpr uint32_t kUnobservedRemoteQpNum = 91;
  constexpr uint32_t kObservedRemoteQpNum = 92;

  struct ibv_qp_attr unobservedAttr{.dest_qp_num = kUnobservedRemoteQpNum};
  ASSERT_EQ(
      otherVerbs.ibv_modify_qp(otherQp, &unobservedAttr, IBV_QP_DEST_QPN), 0)
      << "precondition: the unobserved instance must accept its own update";
  EXPECT_TRUE(observer.tagConfigs.empty())
      << "registration on one verbs instance must not observe another instance";

  struct ibv_qp_attr observedAttr{.dest_qp_num = kObservedRemoteQpNum};
  ASSERT_EQ(
      observedVerbs.ibv_modify_qp(observedQp, &observedAttr, IBV_QP_DEST_QPN),
      0)
      << "precondition: the observed instance must accept its update";
  ASSERT_EQ(observer.tagConfigs.size(), 1)
      << "the registered instance must synchronously publish its own update";
  EXPECT_EQ(observer.tagConfigs[0].remoteQpNum, kObservedRemoteQpNum)
      << "the published update must carry the remote QP the observed instance "
         "was given, not the one the unobserved instance was";

  otherVerbs.ibv_destroy_qp(otherQp);
  observedVerbs.ibv_destroy_qp(observedQp);
  otherVerbs.ibv_destroy_cq(otherCq);
  observedVerbs.ibv_destroy_cq(observedCq);
  EXPECT_EQ(otherVerbs.ibv_dealloc_pd(otherPd), 0);
  EXPECT_EQ(observedVerbs.ibv_dealloc_pd(observedPd), 0);
}

/*
 * Creates and modifies a QP then posts a receive WQE, receives all the packets
 * and verifies that the RWQE is completed.
 */
TEST_F(VerbsTest, PostReceiveWQE) {
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  const auto destQpNum = 10;
  struct ibv_qp* ibvqp =
      setup_qp(verbs_, cq_, pd_, remoteAddr, destQpNum, IBV_MTU_2048);

  const auto localQpNum = MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num);
  const auto qp = verbs_.getQpByNum(localQpNum);
  EXPECT_EQ(qp->getLocalQpNum(), localQpNum);
  EXPECT_EQ(qp->getRemoteQpNum(), destQpNum);
  EXPECT_EQ(qp->getRemoteAddrs().size(), 1);
  EXPECT_EQ(*qp->getRemoteAddrs().begin(), remoteAddr);
  EXPECT_EQ(qp->getMtu(), 2048);

  const uint32_t numPacketsToReceive = 5;
  auto wqeSizeBytes = qp->getMtu() * numPacketsToReceive;
  uint64_t wqeWrId = 200;
  std::unique_ptr<char[]> payload(new char[wqeSizeBytes]);
  ibv_mr* mr = verbs_.ibv_reg_mr_internal(
      pd_,
      payload.get(),
      wqeSizeBytes,
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_LOCAL_WRITE);
  ASSERT_NE(mr, nullptr);

  struct ibv_sge rcv_sge[1] = {
      {.addr = (uint64_t)payload.get(),
       .length = wqeSizeBytes,
       .lkey = mr->lkey}};
  struct ibv_recv_wr rcv_wr = {
      .wr_id = wqeWrId, .sg_list = rcv_sge, .num_sge = 1};
  struct ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(qp->getNumPendingWqes(), 0);
  auto ret = verbs_.ibv_post_recv(ibvqp, &rcv_wr, &bad_wr);
  EXPECT_EQ(ret, 0);

  ASSERT_TRUE(CHECK_NOTNULL(qp)->waitForRWqesToBePosted(
      1, std::chrono::milliseconds(100)));

  EXPECT_EQ(qp->getNumPendingRWqes(), 1)
      << "wqe processing should make the receive wqe pending";

  auto congestionController = std::make_unique<MockCongestionController>();
  qp->setCongestionController(std::move(congestionController));
  auto tag = qp->getTag(MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num));

  const auto csn = 0;
  const auto msn = tag->getNextMsn();
  // Each full-MTU posn of a SEND carries an MTU payload; request validation
  // rejects a payload-less data packet, so feed real bytes as the wire would.
  const std::vector<char> sendPayload(qp->getMtu());
  BaseHeader::Opcode opCode(BaseHeader::Opcode::OperationType::SEND);
  // Packets are received on the paths in round-robin order, 1 packet
  // at a time.
  for (int psn = 0; psn < numPacketsToReceive; ++psn) {
    const auto nowNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    const auto dataHeader = DataHeader::DataHeaderBuilder()
                                .setFsn(psn / qp->getNumRxFlows())
                                .setUna(0)
                                .setAckRequest(false)
                                .setPosn(psn)
                                .setMsn(msn)
                                .setCsn(csn)
                                .setTimestamp(nowNs)
                                .build();
    const auto baseHeader = BaseHeader::BaseHeaderBuilder()
                                .setSrcQp(qp->getLocalQpNum())
                                .setDstQp(qp->getRemoteQpNum())
                                .setOpcode(opCode)
                                .setVersion(0)
                                .setFlowId(psn % qp->getNumRxFlows())
                                .build();
    const auto dataPacket =
        DataPacket(baseHeader, dataHeader, sendPayload.data(), qp->getMtu());
    tag->assignInitiatorMsnToReceiveWqeIfNeeded(csn, msn, qp->getMtu());
    qp->getRxFlowById(psn % qp->getNumTxFlows())
        ->processDataPacket(&dataPacket, kEcnECT0);
  }

  EXPECT_EQ(qp->getNumPendingWqes(), 0) << "WQE should have completed.";
  struct ibv_wc wc;
  verbs_.ibv_poll_cq(cq_, 1, &wc);
  EXPECT_EQ(wc.status, WorkCompletionStatus::kSuccess);
  EXPECT_EQ(wc.byte_len, wqeSizeBytes);
  EXPECT_EQ(wc.wr_id, wqeWrId);
  EXPECT_EQ(
      wc.qp_num,
      MetaRoceVerbs::combineQpAndTag(
          qp->getLocalQpNum(), MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num)));
  EXPECT_EQ(wc.opcode, IBV_WC_RECV);

  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(VerbsTest, ImmediateDataPropagation) {
  // Setup a queue pair (QP) with the given parameters
  const auto remoteAddr = folly::IPAddressV6("fc00::1");
  const auto destQpNum = 10;
  struct ibv_qp* ibvqp =
      setup_qp(verbs_, cq_, pd_, remoteAddr, destQpNum, IBV_MTU_2048);

  // Retrieve the QP object using its QP number
  const auto localQpNum = MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num);
  const auto qp = verbs_.getQpByNum(localQpNum);
  auto tag = qp->getTag(MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num));

  // Prepare to receive a single zero-byte WRITE_IMM_P packet; the recv
  // WQE needs neither a buffer nor an SGE.
  uint64_t wqeWrId = 200;
  struct ibv_recv_wr rcv_wr = {
      .wr_id = wqeWrId, .sg_list = nullptr, .num_sge = 0};
  struct ibv_recv_wr* bad_wr = nullptr;
  EXPECT_EQ(qp->getNumPendingWqes(), 0);
  verbs_.ibv_post_recv(ibvqp, &rcv_wr, &bad_wr);

  ASSERT_TRUE(CHECK_NOTNULL(qp)->waitForRWqesToBePosted(
      1, std::chrono::milliseconds(100)));

  // Verify that the receive WQE is pending
  EXPECT_EQ(qp->getNumPendingRWqes(), 1)
      << "wqe processing should make the receive wqe pending";

  // Set a mock congestion controller for the QP
  auto congestionController = std::make_unique<MockCongestionController>();
  qp->setCongestionController(std::move(congestionController));

  // Prepare and process a data packet with immediate data
  const auto csn = 0;
  const auto msn = tag->getNextMsn();
  const auto psn = 0;
  const uint32_t immData = 0xdeadbeef;
  BaseHeader::Opcode opCode(BaseHeader::Opcode::OperationType::WRITE_IMM_P);
  const auto nowNs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count();
  const auto flowId = psn % qp->getNumRxFlows();
  const auto dataHeader = DataHeader::DataHeaderBuilder()
                              .setFsn(psn / qp->getNumRxFlows())
                              .setUna(0)
                              .setAckRequest(false)
                              .setPosn(psn)
                              .setMsn(msn)
                              .setCsn(csn)
                              .setTimestamp(nowNs)
                              .build();
  const auto baseHeader = BaseHeader::BaseHeaderBuilder()
                              .setSrcQp(qp->getLocalQpNum())
                              .setDstQp(qp->getRemoteQpNum())
                              .setOpcode(std::move(opCode))
                              .setVersion(0)
                              .setFlowId(flowId)
                              .build();
  // Zero-byte WRITE_IMM_P: messageLength is 0, no payload, no target.
  // The receiver skips address and rkey handling when messageLength == 0
  // (RxFlow.cpp).
  const auto readWriteHeader = ReadWriteHeader::ReadWriteHeaderBuilder()
                                   .setKey(0)
                                   .setMessageAddr(0)
                                   .setMessageLength(0)
                                   .build();
  const auto readWritePacket = ReadWritePacket(
      baseHeader,
      dataHeader,
      readWriteHeader,
      /*payloadAddress=*/nullptr,
      /*payloadSizeBytes=*/0,
      immData);
  tag->assignInitiatorMsnToReceiveWqeIfNeeded(csn, msn, qp->getMtu());
  qp->getRxFlowById(flowId)->processDataPacket(&readWritePacket, kEcnECT0);

  // Poll the completion queue and verify the work completion status and
  // immediate data.
  struct ibv_wc wc{};
  verbs_.ibv_poll_cq(cq_, 1, &wc);
  EXPECT_EQ(wc.status, WorkCompletionStatus::kSuccess);
  EXPECT_EQ(wc.byte_len, 0u)
      << "zero-byte WRITE_IMM_P should report 0 bytes received";
  EXPECT_EQ(wc.imm_data, immData)
      << "The completion and the request should have identical immData";
  EXPECT_EQ(
      wc.qp_num,
      MetaRoceVerbs::combineQpAndTag(
          qp->getLocalQpNum(), MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num)));
  EXPECT_EQ(wc.opcode, IBV_WC_RECV_RDMA_WITH_IMM);
  EXPECT_TRUE(wc.wc_flags & IBV_WC_WITH_IMM);

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

/*
 * A receive posted with several sges takes an arriving message across them in
 * order, splitting bytes that cross an sge boundary.
 */
TEST_F(VerbsTest, AMultiSgeReceiveScattersAnArrivingSend) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  const auto qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  auto tag = qp->getTag(MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num));

  std::array<uint8_t, 30> first{};
  std::array<uint8_t, 30> second{};
  std::array<uint8_t, 40> third{};
  std::array<ibv_sge, 3> sges{};
  std::vector<ibv_mr*> mrs;
  for (auto [sge, segment] :
       {std::pair{&sges[0], folly::MutableByteRange(first)},
        std::pair{&sges[1], folly::MutableByteRange(second)},
        std::pair{&sges[2], folly::MutableByteRange(third)}}) {
    ibv_mr* mr = verbs_.ibv_reg_mr_internal(
        pd_, segment.data(), segment.size(), IBV_ACCESS_LOCAL_WRITE);
    ASSERT_NE(mr, nullptr);
    mrs.push_back(mr);
    *sge = {
        .addr = reinterpret_cast<uint64_t>(segment.data()),
        .length = static_cast<uint32_t>(segment.size()),
        .lkey = mr->lkey,
    };
  }
  struct ibv_recv_wr rcv_wr{
      .wr_id = 200,
      .sg_list = sges.data(),
      .num_sge = static_cast<int>(sges.size()),
  };
  struct ibv_recv_wr* bad_wr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_recv(ibvqp, &rcv_wr, &bad_wr), 0)
      << "precondition: a receive of three registered sges is accepted";
  ASSERT_TRUE(qp->waitForRWqesToBePosted(1, std::chrono::milliseconds(100)))
      << "precondition: the receive is pending before the send arrives";
  qp->setCongestionController(std::make_unique<MockCongestionController>());

  std::array<uint8_t, 100> message{};
  for (size_t i = 0; i < message.size(); ++i) {
    message[i] = static_cast<uint8_t>(i + 1);
  }
  const auto csn = 0;
  const auto msn = tag->getNextMsn();
  const auto flowId = 0;
  const auto dataHeader = DataHeader::DataHeaderBuilder()
                              .setFsn(0)
                              .setUna(0)
                              .setAckRequest(false)
                              .setPosn(0)
                              .setMsn(msn)
                              .setCsn(csn)
                              .build();
  const auto baseHeader =
      BaseHeader::BaseHeaderBuilder()
          .setSrcQp(qp->getLocalQpNum())
          .setDstQp(qp->getRemoteQpNum())
          .setOpcode(
              BaseHeader::Opcode(BaseHeader::Opcode::OperationType::SEND_IMM_P))
          .setVersion(0)
          .setFlowId(flowId)
          .build();
  const DataPacket packet(
      baseHeader, dataHeader, message.data(), message.size(), 0x1234);
  tag->assignInitiatorMsnToReceiveWqeIfNeeded(csn, msn, qp->getMtu());
  qp->getRxFlowById(flowId)->processDataPacket(&packet, kEcnECT0);

  struct ibv_wc wc{};
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "the whole message arrived, so the receive must complete";
  EXPECT_EQ(wc.status, IBV_WC_SUCCESS)
      << "every sge is registered for local write";
  EXPECT_EQ(wc.wr_id, 200u) << "the completion must name the posted receive";
  EXPECT_EQ(wc.byte_len, message.size())
      << "the length reported is the message's, summed across sges";
  std::vector<uint8_t> received(first.begin(), first.end());
  received.insert(received.end(), second.begin(), second.end());
  received.insert(received.end(), third.begin(), third.end());
  EXPECT_EQ(received, std::vector<uint8_t>(message.begin(), message.end()))
      << "the sges together must hold the message in order";

  EXPECT_EQ(verbs_.ibv_destroy_qp(std::exchange(qp_, nullptr)), 0);
  for (ibv_mr* mr : mrs) {
    EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
  }
}

/*
 * Tests that ibv_post_send accepts a zero-length IBV_WR_RDMA_READ. Per
 * spec 8.2.3 caption, "A read request of length zero confirms flush of
 * prior writes" — a useful fence primitive. Verifies only the verbs entry
 * point; full flush ordering (preceding WRITE drained before READ CQE)
 * is integration-level and covered by vmtests.
 */
TEST_F(VerbsTest, PostZeroLengthReadIsAccepted) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  char dummy[1] = {};
  ibv_mr* mr = verbs_.ibv_reg_mr_internal(pd_, dummy, sizeof(dummy), 0);
  ASSERT_NE(mr, nullptr);

  struct ibv_sge sge{
      .addr = (uint64_t)dummy,
      .length = 0,
      .lkey = mr->lkey,
  };
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_READ,
      .send_flags = IBV_SEND_SIGNALED,
      .wr = {.rdma = {.remote_addr = 0x1000, .rkey = 1}},
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "zero-length IBV_WR_RDMA_READ should be accepted as a flush "
         "request per spec 8.2.3";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";

  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
}

/*
 * Tests that ibv_post_send accepts a zero-length IBV_WR_RDMA_READ posted
 * with num_sge=0 (no SGE list) — the natural spelling of a flush request,
 * since no local buffer is needed when no bytes will be returned.
 */
TEST_F(VerbsTest, PostZeroLengthReadWithNoSgeIsAccepted) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_RDMA_READ,
      .send_flags = IBV_SEND_SIGNALED,
      .wr = {.rdma = {.remote_addr = 0x0, .rkey = 0}},
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "zero-length IBV_WR_RDMA_READ with num_sge=0 should be accepted";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";
}

TEST_F(VerbsTest, PostReadWithImmediateViaDriver1IsAccepted) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  char dummy[1] = {};
  ibv_mr* mr = verbs_.ibv_reg_mr_internal(pd_, dummy, sizeof(dummy), 0);
  ASSERT_NE(mr, nullptr);

  struct ibv_sge sge{
      .addr = (uint64_t)dummy,
      .length = sizeof(dummy),
      .lkey = mr->lkey,
  };
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_DRIVER1,
      .send_flags = IBV_SEND_SIGNALED,
      .imm_data = 0xA11CE,
      .wr = {.rdma = {.remote_addr = 0x1000, .rkey = 1}},
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "IBV_WR_DRIVER1 carrying a read-with-immediate should be accepted";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";

  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
}

/*
 * Tests that ibv_post_send accepts a zero-length IBV_WR_RDMA_WRITE_WITH_IMM.
 */
TEST_F(VerbsTest, PostZeroLengthWriteWithImmIsAccepted) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  // No SGE is required for zero-length WRITE_WITH_IMM; pass an empty list.
  struct ibv_send_wr send_wr{
      .wr_id = 1,
      .sg_list = nullptr,
      .num_sge = 0,
      .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
      .send_flags = IBV_SEND_SIGNALED,
      .imm_data = 0xCAFEBABE,
      .wr = {.rdma = {.remote_addr = 0x0, .rkey = 0}},
  };
  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &send_wr, &bad_wr), 0)
      << "zero-length IBV_WR_RDMA_WRITE_WITH_IMM should be accepted";
  EXPECT_EQ(bad_wr, nullptr)
      << "no bad_wr should be reported on a successful post";
}

TEST(
    MetaRoceVerbsTest,
    AnAlternatePortAnotherProcessHoldsStillLeavesAReceiver) {
  libsoftmetaroce::MetaRoceVerbs verbs(alternatePortBindFailing(EADDRINUSE));
  EXPECT_NE(verbs.getReceiver(folly::IPAddressV6("fc00::1")), nullptr)
      << "the alternate port is a second way in, so losing it to another "
         "process leaves the address served rather than unreachable";
}

TEST(MetaRoceVerbsTest, AnAlternatePortFailingToBindForAnyOtherReasonThrows) {
  libsoftmetaroce::MetaRoceVerbs verbs(alternatePortBindFailing(EACCES));
  EXPECT_THROW(
      (void)verbs.getReceiver(folly::IPAddressV6("fc00::1")),
      folly::AsyncSocketException)
      << "only a port already in use is expected there, so any other bind "
         "failure reaches the caller rather than leaving a receiver that "
         "looks healthy";
}

TEST(MetaRoceVerbsTest, StandaloneDestructor) {
  // ensures that a verbs instance can be destroyed even
  // if there are qp's created.
  libsoftmetaroce::MetaRoceVerbs* verbs =
      new libsoftmetaroce::MetaRoceVerbs(mockUDPSocketMaker);

  int num_devices;
  struct ibv_device** dev_list = verbs->ibv_get_device_list(&num_devices);
  std::string dev_name = verbs->ibv_get_device_name(dev_list[0]);
  struct ibv_context* context = verbs->ibv_open_device(dev_list[0]);
  struct ibv_pd* pd = verbs->ibv_alloc_pd(context);
  ASSERT_NE(pd, nullptr);
  struct ibv_cq* cq = verbs->ibv_create_cq(context, 20, nullptr, nullptr, 0);
  struct ibv_qp_init_attr init_attr = {
      .send_cq = cq,
      .recv_cq = cq,
      .qp_type = IBV_QPT_RC,
  };
  struct ibv_qp* qp = verbs->ibv_create_qp(pd, &init_attr);
  EXPECT_NE(qp, nullptr);

  // this test is to not crash and not hang at this point.
  delete verbs;

  // in this test, free the qp memory to satisfy the leak checker; the ibv_qp
  // points to memory we were supposed to ibv_destroy_qp, but the verbs is gone.
  free(qp);
  free(cq);
  free(pd);
  free(context);
}

// Flushing the queue pair's unacked send stages a completion, so the
// completion queue has to outlive it.
TEST(MetaRoceVerbsTest, AQueuePairFlushedByTheVerbsDestructorFindsItsQueue) {
  auto verbs =
      std::make_unique<libsoftmetaroce::MetaRoceVerbs>(mockUDPSocketMaker);
  struct ibv_context context{};
  struct ibv_cq* cq = verbs->ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(cq, nullptr);
  struct ibv_pd* pd = verbs->ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_qp* ibvqp =
      setup_qp(*verbs, cq, pd, folly::IPAddressV6("fc00::1"), 10, IBV_MTU_2048);
  ASSERT_NE(ibvqp, nullptr);

  constexpr uint32_t kPayloadBytes = 2048;
  auto payload = std::make_unique<char[]>(kPayloadBytes);
  ibv_mr* mr = verbs->ibv_reg_mr_internal(
      pd, payload.get(), kPayloadBytes, IBV_ACCESS_LOCAL_WRITE);
  ASSERT_NE(mr, nullptr);
  struct ibv_sge sge = {
      .addr = reinterpret_cast<uint64_t>(payload.get()),
      .length = kPayloadBytes,
      .lkey = mr->lkey};
  struct ibv_send_wr sendWr = {
      .wr_id = 1,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_WRITE,
      .wr = {.rdma = {.remote_addr = 1, .rkey = 1}}};
  struct ibv_send_wr* badWr = nullptr;
  ASSERT_EQ(verbs->ibv_post_send(ibvqp, &sendWr, &badWr), 0)
      << "precondition: the post is accepted, or the destroy below has no work "
         "to flush";
  auto* qp = verbs->getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  ASSERT_TRUE(qp->waitForWqesToBePosted(1))
      << "precondition: the send reaches the tag, or the destroy flushes an "
         "empty tag and never stages a completion";

  verbs.reset();

  free(ibvqp);
  free(cq);
  free(pd);
}

/*
 * Tests multiport functionality in ibv_modify_qp.
 * This test verifies that multiple addresses can be added incrementally
 * using IBV_QP_AV, and then the final setup is triggered with
 * the transition to RTR state.
 */
TEST_F(VerbsTest, MultiportIbvModifyQp) {
  struct ibv_qp_init_attr qp_init_attr{
      .send_cq = cq_,
      .recv_cq = cq_,
  };
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qp_init_attr);

  const auto qp = verbs_.getQpByNum(ibvqp->qp_num);

  // Transition to INIT state
  struct ibv_qp_attr init_attr{};
  init_attr.qp_state = IBV_QPS_INIT;
  verbs_.ibv_modify_qp(ibvqp, &init_attr, IBV_QP_STATE);

  // Define test addresses
  const auto remoteAddr1 = folly::IPAddressV6("fc00::100");
  const auto remoteAddr2 = folly::IPAddressV6("fc00::200");
  const auto remoteAddr3 = folly::IPAddressV6("fc00::300");

  // Step 1: Add first remote address using IBV_QP_AV
  struct ibv_qp_attr attr{};
  memcpy(attr.ah_attr.grh.dgid.raw, remoteAddr1.toBinary().data(), 16);
  attr.ah_attr.grh.sgid_index = 0; // Use first available GID

  verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_AV);
  // Verify first address is added but paths are not set up yet
  EXPECT_EQ(qp->getRemoteAddrs().size(), 1);
  EXPECT_TRUE(qp->getRemoteAddrs().contains(remoteAddr1));
  EXPECT_EQ(qp->getLocalAddrs().size(), 1);
  EXPECT_EQ(qp->getNumTxFlows(), 0); // Paths not set up yet

  // Step 2: Add second remote address using IBV_QP_AV
  memcpy(attr.ah_attr.grh.dgid.raw, remoteAddr2.toBinary().data(), 16);
  attr.ah_attr.grh.sgid_index = 0; // Use same GID for simplicity

  verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_AV);

  // Verify second address is added
  EXPECT_EQ(qp->getRemoteAddrs().size(), 2);
  EXPECT_TRUE(qp->getRemoteAddrs().contains(remoteAddr2));
  EXPECT_EQ(qp->getLocalAddrs().size(), 1);
  EXPECT_EQ(qp->getNumTxFlows(), 0); // Paths still not set up

  // Step 3: Add third address with duplicate local address (to test
  // deduplication)
  memcpy(attr.ah_attr.grh.dgid.raw, remoteAddr3.toBinary().data(), 16);
  attr.ah_attr.grh.sgid_index = 0; // Reuse same GID (duplicate local)

  verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_AV);

  // Verify third address is added
  EXPECT_GE(qp->getRemoteAddrs().size(), 2);
  EXPECT_TRUE(qp->getRemoteAddrs().contains(remoteAddr3));
  EXPECT_EQ(qp->getLocalAddrs().size(), 1);
  EXPECT_EQ(qp->getNumTxFlows(), 0); // Paths still not set up

  // Add third address again
  verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_AV);

  // Verify third address is not added again
  EXPECT_EQ(qp->getRemoteAddrs().size(), 3);

  // Step 4: Final setup. Transition to RTR state
  const auto destQpNum = 100;
  struct ibv_qp_attr final_attr{};
  memcpy(final_attr.ah_attr.grh.dgid.raw, remoteAddr1.toBinary().data(), 16);
  final_attr.ah_attr.grh.sgid_index = 0;
  final_attr.dest_qp_num = destQpNum;
  final_attr.path_mtu = IBV_MTU_4096;
  final_attr.qp_state = IBV_QPS_RTR;

  verbs_.ibv_modify_qp(
      ibvqp,
      &final_attr,
      IBV_QP_AV | IBV_QP_DEST_QPN | IBV_QP_PATH_MTU | IBV_QP_STATE);

  // Verify final setup: addresses are deduplicated and paths are created
  EXPECT_EQ(qp->getRemoteAddrs().size(), 3)
      << "All unique remote addresses should appear";
  EXPECT_EQ(qp->getLocalAddrs().size(), 1)
      << "Deduplicated local addresses (only one unique)";
  EXPECT_EQ(qp->getRemoteQpNum(), destQpNum);

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

class MetaRoceVerbsAtomicTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cq = verbs.ibv_create_cq(&context, 128, nullptr, nullptr, 0);
    ASSERT_NE(cq, nullptr);
    pd = verbs.ibv_alloc_pd(&context);
    ASSERT_NE(pd, nullptr);
    ibvqp = setup_qp(verbs, cq, pd, remoteAddr, destQpNum, IBV_MTU_4096);
    ASSERT_NE(ibvqp, nullptr);
    mr = verbs.ibv_reg_mr_internal(
        pd,
        &localBuffer,
        sizeof(localBuffer),
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
            IBV_ACCESS_LOCAL_WRITE);
    ASSERT_NE(mr, nullptr);
    sge = {
        .addr = reinterpret_cast<uint64_t>(&localBuffer),
        .length = 8,
        .lkey = mr->lkey};
  }

  void TearDown() override {
    // gtest runs this even where SetUp stopped partway, so release only what
    // SetUp reached.
    if (mr != nullptr) {
      EXPECT_EQ(verbs.ibv_dereg_mr(mr), 0);
    }
    if (ibvqp != nullptr) {
      EXPECT_EQ(verbs.ibv_destroy_qp(ibvqp), 0);
    }
    if (cq != nullptr) {
      EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
    }
    if (pd != nullptr) {
      EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
    }
  }

  ibv_send_wr
  makeAtomicWr(uint64_t wrId, ibv_wr_opcode opcode, uint64_t atomicRemoteAddr) {
    struct ibv_send_wr wr{};
    wr.wr_id = wrId;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = opcode;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = atomicRemoteAddr;
    wr.wr.atomic.rkey = mr->rkey;
    return wr;
  }

  MetaRoceVerbs verbs{mockUDPSocketMaker};
  struct ibv_context context{};
  folly::IPAddressV6 remoteAddr{"fc00::101"};
  uint32_t destQpNum = 20;
  struct ibv_cq* cq = nullptr;
  struct ibv_qp* ibvqp = nullptr;
  struct ibv_pd* pd = nullptr;
  ibv_mr* mr = nullptr;
  uint64_t localBuffer = 0;
  uint64_t remoteBuffer = 0;
  uint64_t remoteAddress = reinterpret_cast<uint64_t>(&remoteBuffer);
  struct ibv_sge sge{};
};

TEST_F(MetaRoceVerbsAtomicTest, PostAtomicFetchAndAdd) {
  auto wr = makeAtomicWr(100, IBV_WR_ATOMIC_FETCH_AND_ADD, remoteAddress);
  wr.wr.atomic.compare_add = 42;

  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs.ibv_post_send(ibvqp, &wr, &bad_wr), 0)
      << "ibv_post_send should succeed with a valid atomic fetch-and-add request";
  EXPECT_EQ(bad_wr, nullptr);
}

TEST_F(MetaRoceVerbsAtomicTest, PostAtomicCmpAndSwp) {
  auto wr = makeAtomicWr(101, IBV_WR_ATOMIC_CMP_AND_SWP, remoteAddress);
  wr.wr.atomic.compare_add = 0xDEAD;
  wr.wr.atomic.swap = 0xBEEF;

  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs.ibv_post_send(ibvqp, &wr, &bad_wr), 0)
      << "ibv_post_send should succeed with a valid atomic compare-and-swap request";
  EXPECT_EQ(bad_wr, nullptr);
}

TEST_F(MetaRoceVerbsAtomicTest, PostAtomicUnalignedAddressIsAccepted) {
  auto wr = makeAtomicWr(102, IBV_WR_ATOMIC_FETCH_AND_ADD, 7);
  wr.wr.atomic.compare_add = 42;

  struct ibv_send_wr* bad_wr = nullptr;
  EXPECT_EQ(verbs.ibv_post_send(ibvqp, &wr, &bad_wr), 0)
      << "an unaligned atomic remote address is accepted at post; the responder "
         "detects the misalignment and NAKs it as an invalid request";
}

TEST_F(MetaRoceVerbsAtomicTest, PostAtomicWrongSizeCompletesWithLenError) {
  constexpr uint64_t kWrId = 103;
  sge.length = 9; // atomics operate on exactly 8 bytes
  auto wr = makeAtomicWr(kWrId, IBV_WR_ATOMIC_FETCH_AND_ADD, remoteAddress);
  wr.wr.atomic.compare_add = 42;

  struct ibv_send_wr* bad_wr = nullptr;
  ASSERT_EQ(verbs.ibv_post_send(ibvqp, &wr, &bad_wr), 0)
      << "a wrong atomic operand size is completed in error, not rejected at post";

  struct ibv_wc wc{};
  ASSERT_TRUE(pollOneCompletion(verbs, cq, wc))
      << "the length error must surface as a completion";
  EXPECT_EQ(wc.status, IBV_WC_LOC_LEN_ERR);
  EXPECT_EQ(wc.wr_id, kWrId);
  EXPECT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "a local length error must move the QP to the error state";
}

// =====================================================================
// Tests for the comma-separated --libsoftmetaroce_gid filter behavior.
//
// These tests verify that getLocalAddrAndDevicesList():
//   1. Accepts a comma-separated list of IPs (not just a single IP).
//   2. Returns only the requested IPs in the user-specified order, so that
//      gid indices are stable across hosts.
//   3. Trims whitespace and ignores empty list entries.
//   4. Skips IPs that aren't present locally rather than failing.
//   5. Throws when the entire requested list is missing locally.
// =====================================================================

namespace {

// Re-encode an IP string to ibv_gid using the same scheme as
// MetaRoceVerbs::ibv_query_gid (IPv6 -> raw, IPv4 -> v4-in-v6 mapped).
ibv_gid encodeIpToIbvGid(const std::string& ip) {
  ibv_gid gid{};
  if (inet_pton(AF_INET6, ip.c_str(), gid.raw) == 1) {
    return gid;
  }
  struct in_addr v4{};
  if (inet_pton(AF_INET, ip.c_str(), &v4) == 1) {
    gid.raw[10] = 0xff;
    gid.raw[11] = 0xff;
    memcpy(&gid.raw[12], &v4, sizeof(v4));
  }
  return gid;
}

bool gidsEqual(const ibv_gid& a, const ibv_gid& b) {
  return memcmp(a.raw, b.raw, sizeof(a.raw)) == 0;
}

// Discover the same set of local IPs that
// MetaRoceVerbs::getLocalAddrAndDevicesList would observe (non-loopback,
// IFF_UP, global-scope IPv6 or any IPv4).
std::vector<std::string> discoverEligibleLocalIps() {
  std::vector<std::string> ips;
  struct ifaddrs* ifAddrs = nullptr;
  if (getifaddrs(&ifAddrs) == -1) {
    return ips;
  }
  std::set<std::string> seen;
  char buf[INET6_ADDRSTRLEN];
  for (auto ifa = ifAddrs; ifa != nullptr; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == nullptr || !(ifa->ifa_flags & IFF_UP) ||
        (ifa->ifa_flags & IFF_LOOPBACK)) {
      continue;
    }
    std::optional<std::string> addr;
    if (ifa->ifa_addr->sa_family == AF_INET6) {
      auto* in6 = reinterpret_cast<sockaddr_in6*>(ifa->ifa_addr);
      if (in6->sin6_scope_id == 0 &&
          inet_ntop(AF_INET6, &in6->sin6_addr, buf, sizeof(buf)) != nullptr) {
        addr = std::string(buf);
      }
    } else if (ifa->ifa_addr->sa_family == AF_INET) {
      auto* in4 = reinterpret_cast<sockaddr_in*>(ifa->ifa_addr);
      if (inet_ntop(AF_INET, &in4->sin_addr, buf, sizeof(buf)) != nullptr) {
        addr = std::string(buf);
      }
    }
    if (addr && seen.insert(*addr).second) {
      ips.push_back(*addr);
    }
  }
  if (ifAddrs != nullptr) {
    freeifaddrs(ifAddrs);
  }
  return ips;
}

// Build the gid_table_ that MetaRoceVerbs ended up with by querying it
// through its public verbs API (ibv_query_port + ibv_query_gid). Returns
// the GIDs in their gid_index order, decoded back to the encoded ibv_gid
// representation so they can be compared with encodeIpToIbvGid().
std::vector<ibv_gid> readGidTableViaApi(MetaRoceVerbs& verbs) {
  std::vector<ibv_gid> result;
  ibv_port_attr port_attr{};
  if (verbs.ibv_query_port(nullptr, 1, &port_attr) != 0) {
    return result;
  }
  for (int i = 0; i < port_attr.gid_tbl_len; i++) {
    ibv_gid gid{};
    if (verbs.ibv_query_gid(nullptr, 1, i, &gid) == 0) {
      result.push_back(gid);
    }
  }
  return result;
}

class MetaRoceVerbsGidFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedFlag_ = FLAGS_libsoftmetaroce_gid;
    localIps_ = discoverEligibleLocalIps();
  }

  void TearDown() override {
    FLAGS_libsoftmetaroce_gid = savedFlag_;
  }

  std::string savedFlag_;
  std::vector<std::string> localIps_;
};

} // namespace

TEST(MetaRoceVerbsTest, ThePortAdvertisesTheLongestMessageItsMtuCarries) {
  MetaRoceVerbs verbs(mockUDPSocketMaker);

  ibv_port_attr port_attr{};
  ASSERT_EQ(verbs.ibv_query_port(nullptr, 1, &port_attr), 0)
      << "precondition: the port query succeeds";
  EXPECT_EQ(port_attr.max_msg_sz, 65536u * 8192u)
      << "a message carries at most 2^16 packets of the port's 8192-byte mtu";
  EXPECT_GT(port_attr.max_msg_sz, 0x1000u)
      << "rdma-core's tests/test_device.py requires more than one page";
}

TEST(MetaRoceVerbsTest, AGidIndexPastTheTableReportsThroughErrno) {
  MetaRoceVerbs verbs(mockUDPSocketMaker);

  ibv_port_attr port_attr{};
  ASSERT_EQ(verbs.ibv_query_port(nullptr, 1, &port_attr), 0)
      << "precondition: the port query succeeds, or there is no table length "
         "to index past";

  ibv_gid gid{};
  errno = 0;
  EXPECT_EQ(verbs.ibv_query_gid(nullptr, 1, port_attr.gid_tbl_len, &gid), -1)
      << "ibv_query_gid reports failure as -1 rather than as the code itself, "
         "which is the convention its callers read";
  EXPECT_EQ(errno, EINVAL)
      << "ibv_query_gid must leave EINVAL in errno for an index past the "
         "table lsmr advertises, since the -1 it returns carries no reason";
}

TEST(MetaRoceVerbsTest, ANullGidOutParameterIsRefused) {
  MetaRoceVerbs verbs(mockUDPSocketMaker);

  errno = 0;
  EXPECT_EQ(verbs.ibv_query_gid(nullptr, 1, 0, nullptr), -1)
      << "ibv_query_gid must refuse a null gid before writing to it rather "
         "than dereference the caller's null";
  EXPECT_EQ(errno, EINVAL)
      << "and must leave EINVAL in errno for a caller that reads it";

  errno = 0;
  EXPECT_EQ(verbs.ibv_query_gid_ex(nullptr, 1, 0, nullptr, 0), EINVAL)
      << "ibv_query_gid_ex answers with the code itself, and must refuse a "
         "null entry the same way";
  EXPECT_EQ(errno, EINVAL)
      << "and must leave EINVAL in errno as ibv_query_gid does, so a caller "
         "reading errno after either sees the same thing";
}

class ErrnoClobberingObserver final : public ConnectionObserver {
 public:
  void onTagConfigured(const AppliedTagConfig& /*config*/) noexcept override {
    ++timesObserved_;
    errno = ERANGE;
  }
  void onQpRetiring(uint32_t /*localQpNum*/) noexcept override {}

  [[nodiscard]] int timesObserved() const {
    return timesObserved_;
  }

 private:
  int timesObserved_{0};
};

TEST_F(VerbsTest, ARefusedPostLeavesItsCodeInErrno) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_send_wr malformedSend{};
  malformedSend.num_sge = 2;
  struct ibv_send_wr* badSendWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &malformedSend, &badSendWr), EINVAL)
      << "ibv_post_send must refuse two scatter-gather entries with no list";
  EXPECT_EQ(errno, EINVAL)
      << "and must leave that code in errno, or a caller reading it after the "
         "refusal sees what an earlier, unrelated call left behind";

  struct ibv_recv_wr malformedRecv{};
  malformedRecv.num_sge = 2;
  struct ibv_recv_wr* badRecvWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &malformedRecv, &badRecvWr), EINVAL)
      << "ibv_post_recv must refuse it the same way";
  EXPECT_EQ(errno, EINVAL)
      << "and must leave that code in errno as its sibling does";
}

TEST_F(VerbsTest, ARefusedReadAtomicDepthLeavesItsCodeInErrno) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the depth is refused only past the bring-up states";

  struct ibv_qp_attr attr{};
  attr.max_rd_atomic = 4;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &attr, IBV_QP_MAX_QP_RD_ATOMIC), EINVAL)
      << "ibv_modify_qp must refuse the read-atomic depth once the queue pair "
         "is running, since the responder table is negotiated once";
  EXPECT_EQ(errno, EINVAL)
      << "and must leave that code in errno, or a caller reading it after the "
         "refusal sees what an earlier, unrelated call left behind";
}

TEST_F(VerbsTest, ARefusedReadPostLeavesItsCodeInErrno) {
  struct ibv_qp* ibvqp = setup_qp(
      verbs_,
      cq_,
      pd_,
      folly::IPAddressV6("fc00::1"),
      10,
      IBV_MTU_2048,
      true,
      /*maxRdAtomic=*/0);
  ASSERT_NE(ibvqp, nullptr);

  std::array<char, 8> payload{};
  struct ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(payload.data()),
      .length = (uint32_t)payload.size(),
      .lkey = 0,
  };
  struct ibv_send_wr wr{};
  wr.opcode = IBV_WR_RDMA_READ;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  struct ibv_send_wr* bad_wr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &wr, &bad_wr), EINVAL)
      << "a depth of none permits no outstanding read, so the post is refused "
         "rather than queued against a table with no room";
  EXPECT_EQ(errno, EINVAL)
      << "and must leave that code in errno, or a caller reading it after the "
         "refusal sees what an earlier, unrelated call left behind";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST(MetaRoceVerbsTest, ARefusalSurvivesWhatPublishingDoes) {
  ErrnoClobberingObserver observer;
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);
  verbs.registerConnectionObserver(observer);

  struct ibv_context context{};
  struct ibv_cq* cq = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(cq, nullptr);
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_qp* ibvqp =
      setup_qp(verbs, cq, pd, folly::IPAddressV6("fc00::1"), 10, IBV_MTU_2048);
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the send sequence number is refused only from RTS";

  // The destination number applies, so publishing has a config to report, and
  // the sequence number is then refused from this state.
  struct ibv_qp_attr attr{};
  attr.dest_qp_num = 11;
  attr.sq_psn = 1;
  const int publishedBeforeTheRefusal = observer.timesObserved();
  errno = 0;
  EXPECT_EQ(
      verbs.ibv_modify_qp(ibvqp, &attr, IBV_QP_DEST_QPN | IBV_QP_SQ_PSN),
      EINVAL)
      << "precondition: the modify has to refuse before there is a refusal's "
         "errno for the check below to read";
  EXPECT_GT(observer.timesObserved(), publishedBeforeTheRefusal)
      << "precondition: the refusal has to publish, or the errno check below "
         "passes because nothing tried to overwrite it";
  EXPECT_EQ(errno, EINVAL)
      << "the refusal's code has to reach a caller reading errno, and what an "
         "observer does while the applied config is published must not stand "
         "in for it";

  EXPECT_EQ(verbs.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

TEST(MetaRoceVerbsTest, ARefusedQueryLeavesItsCodeInErrno) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  errno = 0;
  EXPECT_EQ(verbs.ibv_query_device(nullptr, nullptr), EINVAL)
      << "a verb of this shape answers with the code itself";
  EXPECT_EQ(errno, EINVAL)
      << "and sets it too, or a caller reading errno after the refusal sees "
         "what some earlier, unrelated call left behind";

  errno = 0;
  EXPECT_EQ(verbs.ibv_query_port(nullptr, 1, nullptr), EINVAL)
      << "the same contract binds every verb that returns its errno";
  EXPECT_EQ(errno, EINVAL) << "so this one sets errno as well";

  ibv_port_attr port_attr{};
  ASSERT_EQ(verbs.ibv_query_port(nullptr, 1, &port_attr), 0)
      << "precondition: the port query succeeds, or there is no table length "
         "to index past";
  ibv_gid_entry entry{};
  errno = 0;
  EXPECT_EQ(
      verbs.ibv_query_gid_ex(nullptr, 1, port_attr.gid_tbl_len, &entry, 0),
      EINVAL)
      << "an index past the table lsmr advertises is refused here as it is by "
         "ibv_query_gid, which differs only in reporting through -1";
  EXPECT_EQ(errno, EINVAL) << "and this sibling sets errno as well";
}

TEST_F(MetaRoceVerbsGidFilterTest, EmptyFlagExposesAllDiscoveredIps) {
  if (localIps_.empty()) {
    GTEST_SKIP() << "Test host has no eligible non-loopback IPs to discover";
  }
  FLAGS_libsoftmetaroce_gid = "";
  MetaRoceVerbs verbs(mockUDPSocketMaker);

  auto gids = readGidTableViaApi(verbs);
  EXPECT_EQ(gids.size(), localIps_.size())
      << "With an empty filter the gid table must contain every eligible IP "
         "that getifaddrs() reported, with no de-duplication beyond what "
         "ifaddrs already yields";
}

TEST_F(MetaRoceVerbsGidFilterTest, SingleIpFiltersToOnlyThatIp) {
  if (localIps_.empty()) {
    GTEST_SKIP() << "Test host has no eligible non-loopback IPs to discover";
  }
  const std::string& chosen = localIps_.front();
  FLAGS_libsoftmetaroce_gid = chosen;
  MetaRoceVerbs verbs(mockUDPSocketMaker);

  auto gids = readGidTableViaApi(verbs);
  ASSERT_EQ(gids.size(), 1u)
      << "A single-IP filter must reduce the gid table to exactly that IP, "
         "regardless of how many IPs are present on the host";
  EXPECT_TRUE(gidsEqual(gids[0], encodeIpToIbvGid(chosen)))
      << "The single retained gid must be the one we asked for (" << chosen
      << ")";
}

TEST_F(MetaRoceVerbsGidFilterTest, MultipleIpsAppearInUserSpecifiedOrder) {
  if (localIps_.empty()) {
    GTEST_SKIP() << "Test host has no eligible non-loopback IPs to discover";
  }
  // Use the discovered IPs in REVERSE order so that we'd notice if the
  // implementation silently sorted by getifaddrs() order instead.
  // With a single IP, reversing is a no-op and the test degenerates to
  // verifying that one IP appears at gid_index 0.
  std::vector<std::string> requested(localIps_.rbegin(), localIps_.rend());
  FLAGS_libsoftmetaroce_gid = folly::join(",", requested);
  MetaRoceVerbs verbs(mockUDPSocketMaker);

  auto gids = readGidTableViaApi(verbs);
  ASSERT_EQ(gids.size(), requested.size())
      << "Every requested IP that exists locally must appear in the gid table";
  for (size_t i = 0; i < requested.size(); i++) {
    EXPECT_TRUE(gidsEqual(gids[i], encodeIpToIbvGid(requested[i])))
        << "gid index " << i
        << " must correspond to the IP at the same position in the "
           "--libsoftmetaroce_gid list (this is the whole point of the "
           "feature: deterministic gid indices). Expected "
        << requested[i];
  }
}

TEST_F(MetaRoceVerbsGidFilterTest, WhitespaceAndEmptyEntriesAreTolerated) {
  if (localIps_.empty()) {
    GTEST_SKIP() << "Test host has no eligible non-loopback IPs to discover";
  }
  const std::string& chosen = localIps_.front();
  // Pad with whitespace and sprinkle in empty entries from stray commas.
  FLAGS_libsoftmetaroce_gid = "  , " + chosen + "  ,,  ";
  MetaRoceVerbs verbs(mockUDPSocketMaker);

  auto gids = readGidTableViaApi(verbs);
  ASSERT_EQ(gids.size(), 1u)
      << "Whitespace-only and empty comma-separated entries must be ignored, "
         "leaving only the one real IP in the gid table";
  EXPECT_TRUE(gidsEqual(gids[0], encodeIpToIbvGid(chosen)))
      << "The surviving entry must match the trimmed IP " << chosen;
}

TEST_F(MetaRoceVerbsGidFilterTest, MissingIpIsSkippedNotFatal) {
  if (localIps_.empty()) {
    GTEST_SKIP() << "Test host has no eligible non-loopback IPs to discover";
  }
  const std::string& chosen = localIps_.front();
  // 2001:db8::/32 is reserved for documentation (RFC 3849) and never
  // appears as a real interface address, so it cannot exist locally.
  const std::string missing = "2001:db8::dead:beef";
  FLAGS_libsoftmetaroce_gid = missing + "," + chosen;

  // Constructor must not throw -- one valid IP is enough.
  std::unique_ptr<MetaRoceVerbs> verbs;
  ASSERT_NO_THROW(verbs = std::make_unique<MetaRoceVerbs>(mockUDPSocketMaker))
      << "Missing IPs in the filter should be skipped (with a warning), not "
         "abort construction, as long as at least one requested IP exists "
         "locally";

  auto gids = readGidTableViaApi(*verbs);
  ASSERT_EQ(gids.size(), 1u)
      << "Only the locally-present IP should be in the gid table; the "
         "missing one must be skipped";
  EXPECT_TRUE(gidsEqual(gids[0], encodeIpToIbvGid(chosen)))
      << "The single surviving gid must be the locally-present " << chosen
      << " (not the missing " << missing << ")";
}

TEST_F(MetaRoceVerbsGidFilterTest, AllMissingIpsThrows) {
  // Two RFC 3849 documentation addresses that cannot exist on a real host.
  FLAGS_libsoftmetaroce_gid = "2001:db8::1,2001:db8::2";

  EXPECT_THROW(
      { MetaRoceVerbs verbs(mockUDPSocketMaker); }, std::runtime_error)
      << "If none of the requested IPs are found locally the gid table is "
         "empty and the constructor must surface that as a runtime_error so "
         "the operator notices the misconfiguration";
}

/*
 * The receive queue is bounded by the same advertised depth as the send queue,
 * and a post past it is refused where the client can see it.
 */
TEST(
    MetaRoceVerbsTest,
    PostRecvPastTheAdvertisedDepthIsRefusedWithNoCompletion) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_context context{};
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_cq* cq = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  struct ibv_qp* ibvqp = setup_qp(
      verbs,
      cq,
      pd,
      folly::IPAddressV6("fc00::1"),
      10,
      IBV_MTU_2048,
      true,
      std::nullopt,
      kMaxQpWorkRequests);
  auto* qp = verbs.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);

  // Filled through the tag rather than by posting, so the depth is reached
  // without waiting on the sender to take up what was posted.
  auto tag = qp->getTag(MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num));
  for (uint32_t i = 0; i < kMaxQpWorkRequests; ++i) {
    ASSERT_TRUE(tag->tryReserveReceiveQueueEntry())
        << "precondition: the tag fills to the depth it advertises; it "
           "refused at "
        << i;
  }

  std::unique_ptr<char[]> payload(new char[1024]);
  ibv_mr* mr = verbs.ibv_reg_mr_internal(
      pd, payload.get(), 1024, IBV_ACCESS_LOCAL_WRITE);
  ASSERT_NE(mr, nullptr);
  struct ibv_sge sge[1] = {
      {.addr = (uint64_t)payload.get(), .length = 1024, .lkey = mr->lkey}};
  struct ibv_recv_wr recv_wr = {.wr_id = 9, .sg_list = sge, .num_sge = 1};
  struct ibv_recv_wr* bad_wr = nullptr;

  EXPECT_EQ(verbs.ibv_post_recv(ibvqp, &recv_wr, &bad_wr), ENOMEM)
      << "a receive posted past the advertised depth must fail, or the client "
         "believes it has a buffer waiting that the queue pair has no room "
         "for; a full queue is a shortage of room, not an invalid request";
  EXPECT_EQ(bad_wr, &recv_wr)
      << "the caller is told which work request was not posted";
  struct ibv_wc wc{};
  EXPECT_EQ(verbs.ibv_poll_cq(cq, 1, &wc), 0)
      << "a refused post must enqueue no completion; reported here it would "
         "arrive ahead of every work request posted before it";

  EXPECT_EQ(verbs.ibv_dereg_mr(mr), 0);
  EXPECT_EQ(verbs.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

TEST(
    MetaRoceVerbsTest,
    PostSendPastTheAdvertisedDepthIsRefusedWithNoCompletion) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_context context{};
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_cq* cq = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  struct ibv_qp* ibvqp = setup_qp(
      verbs,
      cq,
      pd,
      folly::IPAddressV6("fc00::1"),
      10,
      IBV_MTU_2048,
      true,
      std::nullopt,
      kMaxQpWorkRequests);
  auto* qp = verbs.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);

  // Filled through the tag rather than by posting, so the depth is reached
  // without waiting on the sender to drain what was posted.
  auto tag = qp->getTag(MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num));
  for (uint32_t i = 0; i < kMaxQpWorkRequests; ++i) {
    ASSERT_TRUE(tag->tryReserveSendQueueEntry())
        << "precondition: the tag fills to the depth it advertises; it "
           "refused at "
        << i;
  }

  std::unique_ptr<char[]> payload(new char[1024]);
  ibv_mr* mr = verbs.ibv_reg_mr_internal(
      pd, payload.get(), 1024, IBV_ACCESS_LOCAL_WRITE);
  ASSERT_NE(mr, nullptr);
  struct ibv_sge sge[1] = {
      {.addr = (uint64_t)payload.get(), .length = 1024, .lkey = mr->lkey}};
  struct ibv_send_wr send_wr = {
      .wr_id = 7, .sg_list = sge, .num_sge = 1, .opcode = IBV_WR_SEND};
  struct ibv_send_wr* bad_wr = nullptr;

  EXPECT_EQ(verbs.ibv_post_send(ibvqp, &send_wr, &bad_wr), ENOMEM)
      << "a post past the advertised depth must fail, or the client believes "
         "work was taken on that the queue pair has no room for; a full queue "
         "is a shortage of room, not an invalid request";
  EXPECT_EQ(bad_wr, &send_wr)
      << "the caller is told which work request was not posted";
  struct ibv_wc wc{};
  EXPECT_EQ(verbs.ibv_poll_cq(cq, 1, &wc), 0)
      << "a refused post must enqueue no completion; reported here it would "
         "arrive ahead of every work request posted before it";

  EXPECT_EQ(verbs.ibv_dereg_mr(mr), 0);
  EXPECT_EQ(verbs.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

TEST_F(VerbsTest, AProtectionDomainIsFreedOnlyOnceEveryObjectOfItIsGone) {
  struct ibv_pd* ownPd = verbs_.ibv_alloc_pd(&context_);
  ASSERT_NE(ownPd, nullptr);
  std::array<char, 4096> buffer{};
  struct ibv_mr* mr = verbs_.ibv_reg_mr_internal(
      ownPd, buffer.data(), buffer.size(), IBV_ACCESS_LOCAL_WRITE);
  ASSERT_NE(mr, nullptr);
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 1}};
  struct ibv_srq* srq = verbs_.ibv_create_srq(ownPd, &srqInitAttr);
  ASSERT_NE(srq, nullptr);
  struct ibv_qp_init_attr qpInitAttr{.send_cq = cq_, .recv_cq = cq_};
  struct ibv_qp* qp = verbs_.ibv_create_qp(ownPd, &qpInitAttr);
  ASSERT_NE(qp, nullptr);

  EXPECT_EQ(verbs_.ibv_dealloc_pd(ownPd), EBUSY)
      << "a queue pair, a shared receive queue and a memory region were all "
         "created against this domain, so freeing it would leave every key "
         "they validate against pointing into released memory";

  EXPECT_EQ(verbs_.ibv_destroy_qp(qp), 0);
  EXPECT_EQ(verbs_.ibv_dealloc_pd(ownPd), EBUSY)
      << "the shared receive queue and the region are still alive, so one "
         "object going is not the domain going";

  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0);
  EXPECT_EQ(verbs_.ibv_dealloc_pd(ownPd), EBUSY)
      << "the region outlives both, and a region is reached by key from any "
         "queue pair of the domain rather than held by one";

  EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
  EXPECT_EQ(verbs_.ibv_dealloc_pd(ownPd), 0)
      << "the last object of the domain is gone, so a client that tidied up in "
         "any order gets its domain back rather than leaking it";
}

TEST_F(VerbsTest, ASharedReceiveQueuePastAFullTableIsRefused) {
  std::vector<ibv_srq*> srqs;
  SCOPE_EXIT {
    for (auto* srq : srqs) {
      EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0);
    }
  };
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 1}};
  for (uint32_t i = 0; i < kMaxSharedReceiveQueues; ++i) {
    struct ibv_srq* srq = verbs_.ibv_create_srq(pd_, &srqInitAttr);
    ASSERT_NE(srq, nullptr)
        << "precondition: queue " << i
        << " failing before kMaxSharedReceiveQueues would underfill "
           "the handle table";
    srqs.push_back(srq);
  }
  errno = 0;
  ASSERT_EQ(verbs_.ibv_create_srq(pd_, &srqInitAttr), nullptr)
      << "precondition: returning a wrapper would give the caller an SRQ "
         "that was not registered";
  EXPECT_EQ(errno, ENOSPC)
      << "ENOMEM would misreport handle-table exhaustion as allocation "
         "failure";
}

TEST_F(VerbsTest, ASharedReceiveQueueInUseIsNotDestroyed) {
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 1}};
  struct ibv_srq* srq = verbs_.ibv_create_srq(pd_, &srqInitAttr);
  ASSERT_NE(srq, nullptr);
  struct ibv_qp_init_attr qpInitAttr{
      .send_cq = cq_, .recv_cq = cq_, .srq = srq};
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qpInitAttr);
  ASSERT_NE(ibvqp, nullptr);

  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), EBUSY)
      << "a shared receive queue its queue pair can still draw from must "
         "survive the destroy, or that queue pair reads freed memory looking "
         "for a receive";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0)
      << "the queue pair drawing from it is gone, so nothing is left to draw "
         "from this shared receive queue";
}

static struct ibv_qp* setupQpOnSharedQueue(
    libsoftmetaroce::MetaRoceVerbs& verbs,
    struct ibv_cq* cq,
    struct ibv_pd* pd,
    struct ibv_srq* srq,
    const folly::IPAddressV6& remoteAddr,
    uint32_t destQpNum) {
  struct ibv_qp_init_attr initAttr{
      .send_cq = cq, .recv_cq = cq, .srq = srq, .sq_sig_all = 1};
  struct ibv_qp* ibvQp = verbs.ibv_create_qp(pd, &initAttr);
  if (ibvQp == nullptr) {
    return nullptr;
  }
  struct ibv_qp_attr attr{};
  memset(&attr, 0, sizeof(attr));
  memcpy(attr.ah_attr.grh.dgid.raw, remoteAddr.toBinary().data(), 16);
  attr.dest_qp_num =
      MetaRoceVerbs::combineQpAndTag(destQpNum, kMetaRoceInitialTag);
  attr.path_mtu = IBV_MTU_1024;
  attr.qp_state = IBV_QPS_INIT;
  verbs.ibv_modify_qp(
      ibvQp,
      &attr,
      IBV_QP_DEST_QPN | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_STATE);
  attr.qp_state = IBV_QPS_RTR;
  verbs.ibv_modify_qp(ibvQp, &attr, IBV_QP_STATE);
  attr.qp_state = IBV_QPS_RTS;
  verbs.ibv_modify_qp(ibvQp, &attr, IBV_QP_STATE);
  return ibvQp;
}

struct FirstRnrCancelOnTheWire {
  folly::Baton<> sent;
  std::atomic_flag posted = ATOMIC_FLAG_INIT;
  std::atomic<uint32_t> srcQp{0};

  auto socketMaker() {
    return [this](folly::EventBase* evb) -> SocketWithModifier {
      auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
      ON_CALL(*socket, write(_, _))
          .WillByDefault([this](
                             const folly::SocketAddress&,
                             const std::unique_ptr<folly::IOBuf>& buf) {
            auto wire = CHECK_NOTNULL(buf.get())->clone();
            wire->coalesce();
            folly::io::Cursor cursor(wire.get());
            const BaseHeader header{cursor};
            if (header.getOpcode().operationType() ==
                    BaseHeader::Opcode::OperationType::RNR_CANCEL &&
                !posted.test_and_set(std::memory_order_relaxed)) {
              srcQp.store(header.getSrcQp(), std::memory_order_release);
              sent.post();
            }
            return wire->computeChainDataLength();
          });
      return std::make_pair(std::move(socket), nullptr);
    };
  }
};

// The responder refuses with BRNR and records the tag on its queue.
static void
refuseOneMessage(libsoftmetaroce::Qp* qp, uint16_t csn, uint16_t msn) {
  const std::vector<char> payload(qp->getMtu());
  const auto dataHeader = DataHeader::DataHeaderBuilder()
                              .setFsn(0)
                              .setUna(0)
                              .setAckRequest(true)
                              .setPosn(0)
                              .setMsn(msn)
                              .setCsn(csn)
                              .setTimestamp(0)
                              .build();
  const auto baseHeader =
      BaseHeader::BaseHeaderBuilder()
          .setSrcQp(qp->getRemoteQpNum())
          .setDstQp(qp->getLocalQpNum())
          .setOpcode(
              BaseHeader::Opcode(BaseHeader::Opcode::OperationType::SEND))
          .setVersion(0)
          .setFlowId(0)
          .build();
  const auto dataPacket =
      DataPacket(baseHeader, dataHeader, payload.data(), qp->getMtu());
  qp->getRxFlowById(0)->processDataPacket(&dataPacket, kEcnECT0);
}

void VerbsTest::runErroredTagLeavesTheReceive(
    bool erroredRefusedFirst,
    bool errorTheFirstCreated) {
  SCOPED_TRACE(
      fmt::format(
          "erroredRefusedFirst={} errorTheFirstCreated={}",
          erroredRefusedFirst,
          errorTheFirstCreated));
  // The sender owns the drain, so the cancel is read off the wire.
  FirstRnrCancelOnTheWire cancel;
  libsoftmetaroce::MetaRoceVerbs verbs{cancel.socketMaker()};
  struct ibv_context context{};
  struct ibv_cq* const cq =
      verbs.ibv_create_cq(&context, kCqEntries, nullptr, nullptr, 0);
  ASSERT_NE(cq, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
  };
  struct ibv_pd* const pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
  };

  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 1}};
  struct ibv_srq* srq = verbs.ibv_create_srq(pd, &srqInitAttr);
  ASSERT_NE(srq, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs.ibv_destroy_srq(srq), 0);
  };

  struct ibv_qp* firstIbvQp = setupQpOnSharedQueue(
      verbs, cq, pd, srq, folly::IPAddressV6("fc00::1"), kDestQpNum);
  ASSERT_NE(firstIbvQp, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs.ibv_destroy_qp(firstIbvQp), 0);
  };
  struct ibv_qp* secondIbvQp = setupQpOnSharedQueue(
      verbs, cq, pd, srq, folly::IPAddressV6("fc00::1"), kDestQpNum + 1);
  ASSERT_NE(secondIbvQp, nullptr);
  SCOPE_EXIT {
    EXPECT_EQ(verbs.ibv_destroy_qp(secondIbvQp), 0);
  };
  Qp* const first = verbs.getQpByNum(firstIbvQp->qp_num);
  Qp* const second = verbs.getQpByNum(secondIbvQp->qp_num);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);

  struct ibv_qp* const erroredIbvQp =
      errorTheFirstCreated ? firstIbvQp : secondIbvQp;
  Qp* const errored = errorTheFirstCreated ? first : second;
  Qp* const live = errorTheFirstCreated ? second : first;

  // The queue holds no receive, so each message is refused and its tag
  // recorded as waiting.
  Qp* const refusedFirst = erroredRefusedFirst ? errored : live;
  Qp* const refusedSecond = erroredRefusedFirst ? live : errored;
  refuseOneMessage(refusedFirst, /*csn=*/0, /*msn=*/1);
  refuseOneMessage(refusedSecond, /*csn=*/0, /*msn=*/1);

  struct ibv_qp_attr toError{.qp_state = IBV_QPS_ERR};
  ASSERT_EQ(verbs.ibv_modify_qp(erroredIbvQp, &toError, IBV_QP_STATE), 0)
      << "precondition: one of the two waiting tags stops being able to "
         "resume";

  char buffer[128] = {};
  struct ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(buffer),
      .length = sizeof(buffer),
      .lkey = 0};
  struct ibv_recv_wr recvWr{
      .wr_id = 1, .next = nullptr, .sg_list = &sge, .num_sge = 1};
  struct ibv_recv_wr* badWr = nullptr;
  ASSERT_EQ(verbs.ibv_post_srq_recv(srq, &recvWr, &badWr), 0)
      << "precondition: one receive arrives for the two tags waiting";

  ASSERT_TRUE(cancel.sent.try_wait_for(std::chrono::seconds(30)))
      << "the one receive posted sends a cancel, rather than being spent on "
         "the tag that cannot use it";
  EXPECT_EQ(cancel.srcQp.load(std::memory_order_acquire), live->getLocalQpNum())
      << "the cancel goes to the tag that can still resume, whichever of the "
         "two was refused first";
}

TEST_F(VerbsTest, AnErroredTagLeavesTheReceiveForTheTagBehindIt) {
  // The errored tag ahead of the live one is the arrangement that strands it;
  // the other three hold the result steady across how the two were made.
  for (const bool erroredRefusedFirst : {true, false}) {
    for (const bool errorTheFirstCreated : {true, false}) {
      runErroredTagLeavesTheReceive(erroredRefusedFirst, errorTheFirstCreated);
    }
  }
}

TEST_F(VerbsTest, ASharedReceiveQueueRefusesALimitPastItsDepth) {
  constexpr uint32_t kDepth = 4;
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = kDepth, .max_sge = 1}};
  struct ibv_srq* srq = verbs_.ibv_create_srq(pd_, &srqInitAttr);
  ASSERT_NE(srq, nullptr);

  struct ibv_srq_attr past{.srq_limit = kDepth + 1};
  EXPECT_EQ(verbs_.ibv_modify_srq(srq, &past, IBV_SRQ_LIMIT), EINVAL)
      << "a queue is always below a limit past its depth, so arming one picks "
         "out no moment to report and a client would be told at the first take";

  struct ibv_srq_attr atDepth{.srq_limit = kDepth};
  EXPECT_EQ(verbs_.ibv_modify_srq(srq, &atDepth, IBV_SRQ_LIMIT), 0)
      << "a limit of the depth itself is reachable: the queue is below it as "
         "soon as one receive is taken from a full queue";

  struct ibv_srq_attr queried{};
  ASSERT_EQ(verbs_.ibv_query_srq(srq, &queried), 0)
      << "precondition: the query succeeds, or there is no armed limit to "
         "read back";
  EXPECT_EQ(queried.srq_limit, kDepth)
      << "the arming a client asked for is what it reads back, or it cannot "
         "tell which of its requests took";

  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0);
}

TEST_F(VerbsTest, ASharedReceiveQueueHoldsAPostToItsOwnSgeBound) {
  // Below what the device allows, which is what tells the two bounds apart.
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 0}};
  struct ibv_srq* srq = verbs_.ibv_create_srq(pd_, &srqInitAttr);
  ASSERT_NE(srq, nullptr);

  struct ibv_srq_attr queried{};
  ASSERT_EQ(verbs_.ibv_query_srq(srq, &queried), 0)
      << "precondition: the query answers, or the bound below is the value "
         "`queried` was initialized with rather than the queue's";
  ASSERT_EQ(queried.max_sge, 0u)
      << "precondition: the queue answers with the bound it was created with, "
         "which the post below is checked against";

  struct ibv_sge sge{};
  struct ibv_recv_wr recvWr{.sg_list = &sge, .num_sge = 1};
  struct ibv_recv_wr* badWr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_srq_recv(srq, &recvWr, &badWr), EINVAL)
      << "the queue takes a post against the bound it advertises, not the "
         "wider one the device allows, or ibv_query_srq answers with a bound "
         "the queue does not keep";
  EXPECT_EQ(badWr, &recvWr)
      << "a client walks its chain from bad_wr, so a refusal points at the "
         "work request it stopped at";

  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0);
}

TEST_F(VerbsTest, AQueuePairWithASharedReceiveQueueTakesNoReceive) {
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 1}};
  struct ibv_srq* srq = verbs_.ibv_create_srq(pd_, &srqInitAttr);
  ASSERT_NE(srq, nullptr);
  struct ibv_qp_init_attr qpInitAttr{
      .send_cq = cq_, .recv_cq = cq_, .srq = srq};
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qpInitAttr);
  ASSERT_NE(ibvqp, nullptr);
  ibvqp->state = IBV_QPS_INIT;

  struct ibv_recv_wr recvWr{};
  struct ibv_recv_wr* badWr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &recvWr, &badWr), EINVAL)
      << "the receive belongs on the shared queue, and taking it here would "
         "leave it where nothing draws from";
  EXPECT_EQ(badWr, &recvWr)
      << "a client walks its chain from bad_wr, so a refusal points at the "
         "work request it stopped at";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0);
}

TEST(MetaRoceVerbsTest, ACompletionQueueInUseIsNotDestroyed) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_context context{};
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_cq* cq = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(cq, nullptr);
  struct ibv_qp_init_attr qpInitAttr{.send_cq = cq, .recv_cq = cq};
  struct ibv_qp* ibvqp = verbs.ibv_create_qp(pd, &qpInitAttr);
  ASSERT_NE(ibvqp, nullptr);

  ASSERT_EQ(verbs.ibv_destroy_cq(cq), EBUSY)
      << "a completion queue its queue pair can still report into must survive "
         "the destroy, or that report lands in freed memory";

  ASSERT_EQ(verbs.ibv_destroy_qp(ibvqp), 0)
      << "precondition: the queue pair goes, or the destroy below answers "
         "EBUSY for one still holding the queue rather than for the property "
         "under test";
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0)
      << "the queue pair using it is gone, so no work queue is left to "
         "report into this completion queue";
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

TEST(MetaRoceVerbsTest, QueryDeviceRejectsANullAttrRatherThanCrashing) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  EXPECT_EQ(verbs.ibv_query_device(nullptr, nullptr), EINVAL)
      << "a verbs entry point must reject a null out-parameter with EINVAL, "
         "since a client passing one is a caller error and not ours to abort on";
}

TEST(MetaRoceVerbsTest, QueryPortRejectsANullAttrRatherThanCrashing) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  EXPECT_EQ(verbs.ibv_query_port(nullptr, 1, nullptr), EINVAL)
      << "a verbs entry point must reject a null out-parameter with EINVAL, "
         "since a client passing one is a caller error and not ours to abort on";
}

TEST_F(VerbsTest, QueryQpRejectsANullArgumentRatherThanCrashing) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr initAttr{};
  errno = 0;
  EXPECT_EQ(verbs_.ibv_query_qp(ibvqp, nullptr, IBV_QP_CAP, &initAttr), EINVAL)
      << "every branch of the query writes through the out-parameter, so a "
         "client passing none is a caller error to refuse rather than one to "
         "abort the process on";
  EXPECT_EQ(errno, EINVAL)
      << "the refusal has to reach a caller reading errno rather than the "
         "return";

  errno = 0;
  EXPECT_EQ(verbs_.ibv_query_qp(nullptr, &attr, IBV_QP_CAP, &initAttr), EINVAL)
      << "the queue pair this device is asked about is read off the handle, so "
         "a client passing none leaves no queue pair to answer for";
  EXPECT_EQ(errno, EINVAL)
      << "the refusal has to reach a caller reading errno rather than the "
         "return";

  errno = 0;
  EXPECT_EQ(verbs_.ibv_query_qp(ibvqp, &attr, IBV_QP_CAP, nullptr), EINVAL)
      << "the initial attributes are an out-parameter of this verb as much as "
         "the attributes are, so a client passing none is refused alike";
  EXPECT_EQ(errno, EINVAL)
      << "the refusal has to reach a caller reading errno rather than the "
         "return";
}

/*
 * The handle survives a qp_num pointed elsewhere, so the query gets past the
 * handle check and has only the number left to resolve a Tag through.
 */
TEST_F(VerbsTest, AQueryMatchingNoQueuePairIsRefused) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  const uint32_t allocatedQpNum = ibvqp->qp_num;

  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr initAttr{};
  ibvqp->qp_num = allocatedQpNum + 0x1000;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_query_qp(ibvqp, &attr, IBV_QP_CAP, &initAttr), ENOENT)
      << "a query for a queue pair this device does not hold must be "
         "refused; answering it would read the cap through a null Tag";
  EXPECT_EQ(errno, ENOENT)
      << "and the same code reaches a caller reading errno";

  errno = 0;
  EXPECT_EQ(
      verbs_.ibv_query_qp(ibvqp, &attr, IBV_QP_TIMEOUT, &initAttr), ENOENT)
      << "the timeout branch reads both the Tag and the queue pair's own "
         "interval, so a mask asking for only it has to be refused rather than "
         "reaching through a queue pair this device does not hold";
  EXPECT_EQ(errno, ENOENT)
      << "the refusal has to reach a caller reading errno for this mask too";

  errno = 0;
  EXPECT_EQ(
      verbs_.ibv_query_qp(ibvqp, &attr, IBV_QP_RETRY_CNT, &initAttr), ENOENT)
      << "the retry count is read through the same two, so it has to be "
         "refused on the same grounds";
  EXPECT_EQ(errno, ENOENT)
      << "the refusal has to reach a caller reading errno for this mask too";

  // The fixture destroys through this handle, so it has to point to the
  // queue pair again.
  ibvqp->qp_num = allocatedQpNum;
}

// The handle survives a qp_num pointed elsewhere, so the post gets past the
// handle check with only the number left to resolve a queue pair through.
TEST_F(VerbsTest, APostMatchingNoQueuePairIsRefused) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  const uint32_t allocatedQpNum = ibvqp->qp_num;
  ibvqp->qp_num = allocatedQpNum + 0x1000;

  struct ibv_send_wr sendWr{};
  sendWr.opcode = IBV_WR_SEND;
  struct ibv_send_wr* badSendWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &sendWr, &badSendWr), EINVAL)
      << "a post for a queue pair this device does not hold must be "
         "refused; posting it would read the mtu through a null queue pair";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badSendWr, &sendWr)
      << "a refused post reports the request it refused, which is how the "
         "caller finds where its chain stopped";

  struct ibv_recv_wr recvWr{};
  struct ibv_recv_wr* badRecvWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &recvWr, &badRecvWr), EINVAL)
      << "the receive path resolves the same queue pair and posts a work "
         "queue entry through it, so it needs the same refusal";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badRecvWr, &recvWr)
      << "a refused post reports the request it refused, which is how the "
         "caller finds where its chain stopped";

  // The fixture destroys through this handle, so it has to point to the
  // queue pair again.
  ibvqp->qp_num = allocatedQpNum;
}

// The number in the struct still resolves, so the handle is the only field
// left to tell the struct apart from one lsmr issued.
TEST_F(VerbsTest, APostThroughAForeignHandleIsRefused) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  const uint32_t allocatedHandle = ibvqp->handle;
  ibvqp->handle = allocatedHandle + 1;

  struct ibv_send_wr sendWr{};
  sendWr.opcode = IBV_WR_SEND;
  struct ibv_send_wr* badSendWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &sendWr, &badSendWr), EINVAL)
      << "a struct lsmr did not issue is not a queue pair to post through, "
         "whatever its qp_num resolves to";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badSendWr, &sendWr)
      << "a refused post reports the request it refused, which is how the "
         "caller finds where its chain stopped";

  struct ibv_recv_wr recvWr{};
  struct ibv_recv_wr* badRecvWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_recv(ibvqp, &recvWr, &badRecvWr), EINVAL)
      << "the receive path takes the same struct, so it needs the same "
         "refusal";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badRecvWr, &recvWr)
      << "a refused post reports the request it refused, which is how the "
         "caller finds where its chain stopped";

  // The fixture destroys through this handle, so it has to point to the
  // queue pair again.
  ibvqp->handle = allocatedHandle;
}

/*
 * A tag index the queue pair never created leaves the queue pair lookup
 * answering and only the Tag missing, which is the state every attribute read
 * through the Tag has to survive.
 */
TEST(
    MetaRoceVerbsTest,
    AQueryMatchingNoTagIsRefusedForEveryAttributeTheTagHolds) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_context context{};
  struct ibv_cq* cq = verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(cq, nullptr);
  struct ibv_pd* pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_qp* ibvqp =
      setup_qp(verbs, cq, pd, folly::IPAddressV6("fc00::1"), 10, IBV_MTU_2048);
  ASSERT_NE(ibvqp, nullptr);
  const uint32_t allocatedQpNum = ibvqp->qp_num;
  ibvqp->qp_num = MetaRoceVerbs::combineQpAndTag(
      MetaRoceVerbs::qpAndTagToQp(allocatedQpNum), 3);

  struct ibv_qp_attr attr{};
  struct ibv_qp_init_attr initAttr{};
  for (const int attrMask :
       {IBV_QP_CAP,
        IBV_QP_RNR_RETRY,
        IBV_QP_TIMEOUT,
        IBV_QP_RETRY_CNT,
        IBV_QP_MAX_QP_RD_ATOMIC,
        IBV_QP_MAX_DEST_RD_ATOMIC}) {
    errno = 0;
    EXPECT_EQ(verbs.ibv_query_qp(ibvqp, &attr, attrMask, &initAttr), ENOENT)
        << "an attribute the Tag holds must be refused where the queue pair "
           "holds no Tag under this number; answering it would read through a "
           "null Tag, for mask "
        << attrMask;
    EXPECT_EQ(errno, ENOENT)
        << "and the same code reaches a caller reading errno, for mask "
        << attrMask;
  }
  EXPECT_EQ(verbs.ibv_query_qp(ibvqp, &attr, IBV_QP_STATE, &initAttr), ENOENT)
      << "a successful query fills init_attr, so with no Tag to fill its cap "
         "and signaling from, a state-only query fails too";

  ibvqp->qp_num = allocatedQpNum;
  EXPECT_EQ(verbs.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
}

// The inline branch reads the cap off the Tag the qp_num resolves to, so a
// tag index the queue pair never created leaves it reading through a null Tag.
TEST_F(VerbsTest, PostingInlineOnATagTheQueuePairNeverHadIsRefused) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: a post is refused on state alone outside RTS and ERR, "
         "which would pass this check without ever reaching the Tag";
  const uint32_t allocatedQpNum = ibvqp->qp_num;
  ibvqp->qp_num = MetaRoceVerbs::combineQpAndTag(
      MetaRoceVerbs::qpAndTagToQp(allocatedQpNum), 3);

  char payload[8] = {};
  struct ibv_sge sge{
      .addr = (uint64_t)payload,
      .length = sizeof(payload),
      .lkey = 0,
  };
  struct ibv_send_wr sendWr{
      .wr_id = 1,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_INLINE,
  };
  struct ibv_send_wr* badSendWr = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &sendWr, &badSendWr), EINVAL)
      << "the inline cap comes off the Tag the qp_num resolves to, so a post "
         "for a tag the queue pair never had must be refused rather than read "
         "through a null Tag";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";
  EXPECT_EQ(badSendWr, &sendWr)
      << "a refused post reports the work request it stopped at, which is how "
         "the caller finds where its chain stopped";

  // The fixture destroys through this handle, so it has to point to the
  // queue pair again.
  ibvqp->qp_num = allocatedQpNum;
}

/*
 * A client that reads no atomic capability emulates fetch-add and compare-swap
 * over send and read rather than issuing them, which is slower and loses the
 * atomicity it was emulating.
 */
TEST(MetaRoceVerbsTest, QueryDeviceAdvertisesTheAtomicCapabilityItImplements) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_device_attr device_attr{};
  ASSERT_EQ(verbs.ibv_query_device(nullptr, &device_attr), 0)
      << "precondition: the device query must succeed, or the capability read "
         "below is whatever the attr struct held";
  EXPECT_EQ(device_attr.atomic_cap, IBV_ATOMIC_HCA)
      << "atomics execute on the responder's own memory, so the device is "
         "atomic against what reaches it and must say so";
}

/*
 * A client enumerating ports walks 1 through phys_port_cnt, so a device
 * reporting none offers it no port to bring up.
 */
TEST(MetaRoceVerbsTest, QueryDeviceCountsAPort) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_device_attr device_attr{};
  ASSERT_EQ(verbs.ibv_query_device(nullptr, &device_attr), 0)
      << "precondition: the device query must succeed, or the count read below "
         "is whatever the attr struct held";
  EXPECT_GT(device_attr.phys_port_cnt, 0)
      << "a device reporting no port offers a client none to bring up, and "
         "enumeration that walks the count finds nothing to drive";
}

TEST(MetaRoceVerbsTest, QueryDeviceAdvertisesAnOutstandingReadAtomicDepth) {
  libsoftmetaroce::MetaRoceVerbs verbs(mockUDPSocketMaker);

  struct ibv_device_attr device_attr{};
  ASSERT_EQ(verbs.ibv_query_device(nullptr, &device_attr), 0)
      << "precondition: the device query must succeed, or the caps compared "
         "below are whatever the attr struct was initialized to";
  EXPECT_GT(device_attr.max_qp_init_rd_atom, 0)
      << "a client sizing its reads and atomics from the advertised initiator "
         "depth would issue none at all against a zero";
  EXPECT_GT(device_attr.max_qp_rd_atom, 0)
      << "the same for the responder depth a client may ask a queue pair for";
  EXPECT_GT(device_attr.max_res_rd_atom, 0)
      << "and for the target resources the device claims across queue pairs";
}

// Linux permits IB_QP_PATH_MTU on the INIT to RTR transition alone.
TEST_F(VerbsTest, ThePathMtuIsRefusedOnceTheQueuePairIsPastInit) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the queue pair must be past INIT, or the refusal below "
         "is not the one under test";
  auto* const qp =
      verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  const uint32_t mtuInUse = qp->getMtu();

  struct ibv_qp_attr requested{};
  requested.path_mtu = IBV_MTU_256;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &requested, IBV_QP_PATH_MTU), EINVAL)
      << "a running queue pair must refuse a new path mtu; its work requests "
         "already carry posn counts sized from the one in use";
  EXPECT_EQ(qp->getMtu(), mtuInUse)
      << "and must still be carrying the mtu it was running with, since a "
         "refusal that changed it anyway would leave those counts wrong";

  struct ibv_qp_attr toError{};
  toError.qp_state = IBV_QPS_ERR;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &toError, IBV_QP_STATE), 0)
      << "precondition: the transition to the error state must be accepted";
  ASSERT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "precondition: the queue pair must have reached the error state, or "
         "the refusal below is the running one already covered above";

  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &requested, IBV_QP_PATH_MTU), EINVAL)
      << "an errored queue pair must refuse a new path mtu by its return code "
         "too, the same answer Linux gives outside the INIT to RTR transition";
}

TEST_F(VerbsTest, TheSendPsnIsRefusedOnceTheQueuePairIsRunning) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the queue pair must already be running, or the refusal "
         "below is not the one under test";

  struct ibv_qp_attr requested{};
  requested.sq_psn = 0x123456;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &requested, IBV_QP_SQ_PSN), EINVAL)
      << "a running queue pair must refuse a new send psn";

  struct ibv_qp_attr toError{};
  toError.qp_state = IBV_QPS_ERR;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &toError, IBV_QP_STATE), 0)
      << "precondition: the transition to the error state must be accepted";
  ASSERT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "precondition: the queue pair must have reached the error state, or "
         "the refusal below is the running one already covered above";

  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &requested, IBV_QP_SQ_PSN), EINVAL)
      << "an errored queue pair must refuse a new send psn; the old guard "
         "matched only RTS, so every other state including this one was taken";
}

// IBTA sets rq_psn on the INIT to RTR transition.
TEST_F(VerbsTest, TheReceivePsnIsRefusedOnceTheQueuePairIsPastInit) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the queue pair must be past INIT, or the refusal below "
         "is not the one under test";

  struct ibv_qp_attr requested{};
  requested.rq_psn = 0x123456;
  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &requested, IBV_QP_RQ_PSN), EINVAL)
      << "a running queue pair must refuse a new receive psn";

  struct ibv_qp_attr toError{};
  toError.qp_state = IBV_QPS_ERR;
  ASSERT_EQ(verbs_.ibv_modify_qp(ibvqp, &toError, IBV_QP_STATE), 0)
      << "precondition: the transition to the error state must be accepted";
  ASSERT_EQ(ibvqp->state, IBV_QPS_ERR)
      << "precondition: the queue pair must have reached the error state, or "
         "the refusal below is the running one already covered above";

  EXPECT_EQ(verbs_.ibv_modify_qp(ibvqp, &requested, IBV_QP_RQ_PSN), EINVAL)
      << "an errored queue pair must refuse a new receive psn by its return "
         "code; the tag drops what it cannot take, so a client left untold "
         "would never learn its sequence was ignored";
}

/*
 * The depth has to be settled before the queue pair carries anything: the
 * peer's responder table is negotiated once and does not resize to match, so
 * rewriting it under running traffic would not move the limit it sets.
 */
TEST_F(VerbsTest, TheReadAtomicDepthIsRefusedOnceTheQueuePairIsRunning) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  ASSERT_EQ(ibvqp->state, IBV_QPS_RTS)
      << "precondition: the queue pair must be running, or the refusal below "
         "is not the one under test";

  struct ibv_qp_attr requested{};
  requested.max_rd_atomic = 4;
  EXPECT_EQ(
      verbs_.ibv_modify_qp(ibvqp, &requested, IBV_QP_MAX_QP_RD_ATOMIC), EINVAL)
      << "a depth the peer negotiated once must not be rewritten under "
         "running traffic";
}

/*
 * A client that negotiated no read or atomic depth is told so at the post
 * rather than having the work admitted a slot at a time, which is what a tag
 * holding none would otherwise allow.
 */
TEST_F(VerbsTest, ARequestIsRefusedAtADepthOfZero) {
  struct ibv_qp* ibvqp = setup_qp(
      verbs_,
      cq_,
      pd_,
      folly::IPAddressV6("fc00::1"),
      10,
      IBV_MTU_2048,
      /*sqSigAll=*/true,
      /*maxRdAtomic=*/0);
  auto* qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  ASSERT_EQ(
      qp->getTag(MetaRoceVerbs::qpAndTagToTag(ibvqp->qp_num))->getMaxRdAtomic(),
      0)
      << "precondition: the depth must have been taken on the way up, or the "
         "post below is refused for some reason other than the one under test";

  std::array<char, 64> buffer{};
  struct ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(buffer.data()),
      .length = (uint32_t)buffer.size(),
      .lkey = 0,
  };
  struct ibv_send_wr wr{};
  wr.wr_id = 1;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = 0x1000;
  wr.wr.rdma.rkey = 1;
  struct ibv_send_wr* bad_wr = nullptr;

  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &wr, &bad_wr), EINVAL)
      << "a read on a queue pair permitted none outstanding must be refused "
         "where the client can still act on it";
  EXPECT_EQ(bad_wr, &wr)
      << "the refusal must report the work request it refused, which is how a "
         "client posting a chain finds the one to fix";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

/*
 * A qp_num matching no queue pair resolves no Tag, so the post is refused
 * before the depth check reads one. A read is never inline, so this is the
 * first place the verb reaches through the queue pair.
 */
TEST_F(VerbsTest, APostOfAReadMatchingNoQueuePairIsRefused) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  const uint32_t allocatedQpNum = ibvqp->qp_num;

  std::array<char, 64> buffer{};
  struct ibv_sge sge{
      .addr = reinterpret_cast<uint64_t>(buffer.data()),
      .length = (uint32_t)buffer.size(),
      .lkey = 0,
  };
  struct ibv_send_wr wr{};
  wr.wr_id = 1;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = 0x1000;
  wr.wr.rdma.rkey = 1;
  struct ibv_send_wr* bad_wr = nullptr;

  // The queue pair id is the low bytes of qp_num, so stepping it points to one
  // this device never created.
  ibvqp->qp_num = allocatedQpNum + 1;
  EXPECT_EQ(verbs_.ibv_post_send(ibvqp, &wr, &bad_wr), EINVAL)
      << "a read for a queue pair this device does not hold must be "
         "refused; the depth check would otherwise read it through a null Tag";
  EXPECT_EQ(bad_wr, &wr)
      << "the refusal must report the work request it refused, as the sibling "
         "refusals in this verb do";

  // The fixture destroys through this handle, so it has to point to the
  // queue pair again.
  ibvqp->qp_num = allocatedQpNum;
}

TEST_F(VerbsTest, AQueuePairReportsTheReadAtomicDepthItWasGiven) {
  struct ibv_qp_init_attr qpInitAttr{};
  qpInitAttr.send_cq = cq_;
  qpInitAttr.recv_cq = cq_;
  struct ibv_qp* ibvqp = verbs_.ibv_create_qp(pd_, &qpInitAttr);
  ASSERT_NE(ibvqp, nullptr);

  // Set before the queue pair leaves reset: the depth is a bring-up attribute
  // and ibv_modify_qp refuses it once the queue pair is carrying traffic.
  struct ibv_qp_attr requested{};
  requested.max_rd_atomic = 4;
  requested.max_dest_rd_atomic = 6;
  ASSERT_EQ(
      verbs_.ibv_modify_qp(
          ibvqp,
          &requested,
          IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_MAX_DEST_RD_ATOMIC),
      0)
      << "precondition: the depths must be accepted, or the query below reads "
         "back a default rather than what this test set";

  struct ibv_qp_attr reported{};
  struct ibv_qp_init_attr init{};
  ASSERT_EQ(
      verbs_.ibv_query_qp(
          ibvqp,
          &reported,
          IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_MAX_DEST_RD_ATOMIC,
          &init),
      0)
      << "precondition: the query must succeed, or the depths compared below "
         "are whatever the attr struct was initialized to";
  EXPECT_EQ(reported.max_rd_atomic, 4)
      << "a depth the client negotiated must read back, or it cannot tell what "
         "the queue pair agreed to carry";
  EXPECT_EQ(reported.max_dest_rd_atomic, 6)
      << "and the responder depth the same";

  verbs_.ibv_destroy_qp(ibvqp);
}

TEST_F(VerbsTest, AQueuePairNeverGivenADepthReportsTheAdvertisedCeiling) {
  struct ibv_qp* ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_qp_attr reported{};
  struct ibv_qp_init_attr init{};
  ASSERT_EQ(
      verbs_.ibv_query_qp(
          ibvqp,
          &reported,
          IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_MAX_DEST_RD_ATOMIC,
          &init),
      0)
      << "precondition: the query must succeed, or the depths compared below "
         "are whatever the attr struct was initialized to";
  struct ibv_device_attr device_attr{};
  ASSERT_EQ(verbs_.ibv_query_device(nullptr, &device_attr), 0)
      << "precondition: the device query must succeed, or the caps compared "
         "below are whatever the attr struct was initialized to";
  // ibv_query_device sets both caps from kMaxOutstandingReadAtomic, so either
  // comparison below would hold against either cap.
  EXPECT_EQ(reported.max_rd_atomic, device_attr.max_qp_init_rd_atom)
      << "a client that never set the attribute must read back what the "
         "device advertised, not the zero that permits no read or atomic";
  EXPECT_EQ(reported.max_dest_rd_atomic, device_attr.max_qp_rd_atom)
      << "and the responder depth must read back an advertised ceiling too";
}

TEST_F(VerbsTest, AnUndeclaredDomainFieldIsNotReadOnTheTagPath) {
  struct ibv_qp_init_attr initAttr{};
  initAttr.send_cq = cq_;
  initAttr.recv_cq = cq_;
  struct ibv_qp* parent = verbs_.ibv_create_qp(pd_, &initAttr);
  ASSERT_NE(parent, nullptr);

  struct ibv_qp_init_attr_ex exAttr{};
  exAttr.send_cq = cq_;
  exAttr.recv_cq = cq_;
  exAttr.qp_type = IBV_QPT_RC;
  exAttr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS;
  exAttr.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  exAttr.source_qpn = MetaRoceVerbs::combineQpAndTag(
      MetaRoceVerbs::qpAndTagToQp(parent->qp_num), 1);
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  exAttr.pd = reinterpret_cast<struct ibv_pd*>(0xdeadbeef);

  struct ibv_qp* tagQp = verbs_.ibv_create_qp_ex(&context_, &exAttr);
  EXPECT_NE(tagQp, nullptr)
      << "judging the field would dereference that address; the tag path has "
         "to reach the caller's answer without reading what was never set";

  if (tagQp != nullptr) {
    EXPECT_EQ(verbs_.ibv_destroy_qp(tagQp), 0);
  }
  EXPECT_EQ(verbs_.ibv_destroy_qp(parent), 0);
}

TEST_F(VerbsTest, AFallbackCreateTakesTheDomainTheFieldHolds) {
  // No IBV_QP_CREATE_SOURCE_QPN, so this carries no tag and falls through.
  struct ibv_qp_init_attr_ex exAttr{};
  exAttr.send_cq = cq_;
  exAttr.recv_cq = cq_;
  exAttr.qp_type = IBV_QPT_RC;
  exAttr.pd = pd_;
  struct ibv_qp* undeclared = verbs_.ibv_create_qp_ex(&context_, &exAttr);
  EXPECT_NE(undeclared, nullptr)
      << "the field holds a domain lsmr allocated, and the plain path reads "
         "it without asking comp_mask to declare it with IBV_QP_INIT_ATTR_PD";

  exAttr.pd = nullptr;
  errno = 0;
  EXPECT_EQ(verbs_.ibv_create_qp_ex(&context_, &exAttr), nullptr)
      << "passing no domain at all is refused where the plain path judges it, "
         "which is the same answer ibv_create_qp gives directly";
  EXPECT_EQ(errno, EINVAL)
      << "and the same code reaches a caller reading errno";

  if (undeclared != nullptr) {
    EXPECT_EQ(verbs_.ibv_destroy_qp(undeclared), 0);
  }
}

static std::vector<char> countingBytes(uint32_t count, char first) {
  std::vector<char> bytes(count);
  std::iota(bytes.begin(), bytes.end(), first);
  return bytes;
}

// The headers of a message of opType carried whole in one packet to qp's
// initial Tag.
static std::pair<BaseHeader, DataHeader> singlePacketHeaders(
    const Qp& qp,
    BaseHeader::Opcode::OperationType opType,
    uint16_t fsn,
    uint16_t msn,
    uint16_t csn) {
  return {
      BaseHeader::BaseHeaderBuilder()
          .setSrcQp(qp.getRemoteQpNum())
          .setDstQp(qp.getLocalQpNum())
          .setOpcode(BaseHeader::Opcode(opType))
          .setVersion(0)
          .setFlowId(0)
          .build(),
      DataHeader::DataHeaderBuilder()
          .setFsn(fsn)
          .setUna(0)
          .setTag(kMetaRoceInitialTag)
          .setAckRequest(true)
          .setPosn(0)
          .setMsn(msn)
          .setCsn(csn)
          .setTimestamp(0)
          .build()};
}

TEST_F(VerbsTest, AMultiSgeSharedReceiveScattersAnArrivingSend) {
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 3}};
  struct ibv_srq* const srq = verbs_.ibv_create_srq(pd_, &srqInitAttr);
  ASSERT_NE(srq, nullptr);
  struct ibv_qp* const ibvqp = setupQpOnSharedQueue(
      verbs_, cq_, pd_, srq, folly::IPAddressV6("fc00::1"), kDestQpNum);
  ASSERT_NE(ibvqp, nullptr);
  Qp* const qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);

  std::array<char, 20> first{};
  std::array<char, 20> second{};
  std::array<char, 24> third{};
  const std::array<std::span<char>, 3> segments{first, second, third};
  std::array<ibv_sge, 3> sges{};
  std::vector<ibv_mr*> mrs;
  for (size_t i = 0; i < segments.size(); ++i) {
    ibv_mr* const mr = verbs_.ibv_reg_mr_internal(
        pd_, segments[i].data(), segments[i].size(), IBV_ACCESS_LOCAL_WRITE);
    ASSERT_NE(mr, nullptr);
    mrs.push_back(mr);
    sges[i] = {
        .addr = reinterpret_cast<uint64_t>(segments[i].data()),
        .length = static_cast<uint32_t>(segments[i].size()),
        .lkey = mr->lkey,
    };
  }
  struct ibv_recv_wr recvWr{
      .wr_id = 300,
      .sg_list = sges.data(),
      .num_sge = static_cast<int>(sges.size()),
  };
  struct ibv_recv_wr* badWr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_srq_recv(srq, &recvWr, &badWr), 0)
      << "precondition: a shared receive of three registered sges is accepted";

  const auto payload = countingBytes(64, 1);
  const auto [baseHeader, dataHeader] = singlePacketHeaders(
      *qp,
      BaseHeader::Opcode::OperationType::SEND,
      /*fsn=*/0,
      qp->getTag(kMetaRoceInitialTag)->getNextMsn(),
      /*csn=*/0);
  const DataPacket send(baseHeader, dataHeader, payload.data(), payload.size());
  qp->getRxFlowById(0)->processDataPacket(&send, kEcnECT0);

  std::vector<char> scattered;
  for (const std::span<char> segment : segments) {
    scattered.insert(scattered.end(), segment.begin(), segment.end());
  }
  EXPECT_EQ(scattered, payload)
      << "a shared receive takes a message across its sges in order";
  struct ibv_wc wc{};
  ASSERT_TRUE(pollOneCompletion(verbs_, cq_, wc))
      << "the whole message arrived, so the receive must complete";
  EXPECT_EQ(wc.status, IBV_WC_SUCCESS)
      << "every sge is registered for local write";
  EXPECT_EQ(wc.wr_id, 300u)
      << "the completion reports the shared receive the message took";
  EXPECT_EQ(wc.byte_len, payload.size())
      << "the completion reports the bytes across every sge";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0);
  for (ibv_mr* mr : mrs) {
    EXPECT_EQ(verbs_.ibv_dereg_mr(mr), 0);
  }
}

TEST_F(VerbsTest, ASharedReceiveQueueTakesEverySgeTheDeviceReports) {
  struct ibv_srq_init_attr pastTheLimit{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = kMaxSgesPerWorkRequest + 1}};
  EXPECT_EQ(verbs_.ibv_create_srq(pd_, &pastTheLimit), nullptr)
      << "a queue may not ask for more sges than max_srq_sge";

  struct ibv_srq_init_attr atTheLimit{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = kMaxSgesPerWorkRequest}};
  struct ibv_srq* const srq = verbs_.ibv_create_srq(pd_, &atTheLimit);
  ASSERT_NE(srq, nullptr) << "a queue may ask for every sge max_srq_sge allows";
  std::array<ibv_sge, kMaxSgesPerWorkRequest> sges{};
  struct ibv_recv_wr recvWr{
      .sg_list = sges.data(), .num_sge = static_cast<int>(sges.size())};
  struct ibv_recv_wr* badWr = nullptr;
  EXPECT_EQ(verbs_.ibv_post_srq_recv(srq, &recvWr, &badWr), 0)
      << "a post may use every sge its queue was created with";

  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0);
}

// Each case posts at a nonzero offset into a sentinel-filled buffer and
// compares the whole buffer, so bytes landing anywhere but at the offset show.
class ZeroBasedLocalBufferTest : public VerbsTest {
 protected:
  static constexpr char kSentinel = 0x5a;
  static constexpr uint32_t kMessageBytes = 64;
  static constexpr uint64_t kOffset = 32;

  void SetUp() override {
    VerbsTest::SetUp();
    mr_ = verbs_.ibv_reg_mr_internal(
        pd_,
        buffer_.data(),
        buffer_.size(),
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_ZERO_BASED);
    ASSERT_NE(mr_, nullptr);
  }

  void TearDown() override {
    if (mr_ != nullptr) {
      EXPECT_EQ(verbs_.ibv_dereg_mr(mr_), 0);
    }
    VerbsTest::TearDown();
  }

  [[nodiscard]] std::vector<char> withBytesAtOffset(
      const std::vector<char>& bytes) const {
    std::vector<char> expected(buffer_.size(), kSentinel);
    std::copy(bytes.begin(), bytes.end(), expected.begin() + kOffset);
    return expected;
  }

  [[nodiscard]] struct ibv_sge sgeAtOffset(uint64_t offset, uint32_t length)
      const {
    return {.addr = offset, .length = length, .lkey = mr_->lkey};
  }

  std::vector<char> buffer_ = std::vector<char>(256, kSentinel);
  struct ibv_mr* mr_{nullptr};
};

TEST_F(ZeroBasedLocalBufferTest, AReceiveScattersAtItsOffset) {
  struct ibv_qp* const ibvqp = fixtureQp();
  ASSERT_NE(ibvqp, nullptr);
  Qp* const qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  struct ibv_sge sge = sgeAtOffset(kOffset, kMessageBytes);
  struct ibv_recv_wr recvWr{.wr_id = 1, .sg_list = &sge, .num_sge = 1};
  struct ibv_recv_wr* badWr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_recv(ibvqp, &recvWr, &badWr), 0)
      << "precondition: the receive is posted";
  ASSERT_TRUE(qp->waitForRWqesToBePosted(1, std::chrono::milliseconds(100)))
      << "precondition: the receive is pending when the message arrives";

  const auto payload = countingBytes(kMessageBytes, 1);
  const auto [baseHeader, dataHeader] = singlePacketHeaders(
      *qp,
      BaseHeader::Opcode::OperationType::SEND,
      /*fsn=*/0,
      qp->getTag(kMetaRoceInitialTag)->getNextMsn(),
      /*csn=*/0);
  const DataPacket send(baseHeader, dataHeader, payload.data(), payload.size());
  qp->getRxFlowById(0)->processDataPacket(&send, kEcnECT0);

  EXPECT_EQ(buffer_, withBytesAtOffset(payload))
      << "ibv_post_recv(3) gives a zero-based region's SGE address as an "
         "offset from the start of the region";
}

TEST_F(ZeroBasedLocalBufferTest, ASharedQueueReceiveScattersAtItsOffset) {
  struct ibv_srq_init_attr srqInitAttr{
      .attr = ibv_srq_attr{.max_wr = 4, .max_sge = 1}};
  struct ibv_srq* const srq = verbs_.ibv_create_srq(pd_, &srqInitAttr);
  ASSERT_NE(srq, nullptr);
  struct ibv_qp* const ibvqp = setupQpOnSharedQueue(
      verbs_, cq_, pd_, srq, folly::IPAddressV6("fc00::1"), kDestQpNum);
  ASSERT_NE(ibvqp, nullptr);
  Qp* const qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  struct ibv_sge sge = sgeAtOffset(kOffset, kMessageBytes);
  struct ibv_recv_wr recvWr{.wr_id = 1, .sg_list = &sge, .num_sge = 1};
  struct ibv_recv_wr* badWr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_srq_recv(srq, &recvWr, &badWr), 0)
      << "precondition: the receive is posted to the shared queue";

  const auto payload = countingBytes(kMessageBytes, 1);
  const auto [baseHeader, dataHeader] = singlePacketHeaders(
      *qp,
      BaseHeader::Opcode::OperationType::SEND,
      /*fsn=*/0,
      qp->getTag(kMetaRoceInitialTag)->getNextMsn(),
      /*csn=*/0);
  const DataPacket send(baseHeader, dataHeader, payload.data(), payload.size());
  qp->getRxFlowById(0)->processDataPacket(&send, kEcnECT0);

  EXPECT_EQ(buffer_, withBytesAtOffset(payload))
      << "ibv_post_srq_recv(3) gives a zero-based region's SGE address as an "
         "offset from the start of the region";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs_.ibv_destroy_srq(srq), 0);
}

TEST_F(ZeroBasedLocalBufferTest, AReadResponseLandsAtItsOffset) {
  struct ibv_qp* const ibvqp = setup_qp(
      verbs_,
      cq_,
      pd_,
      folly::IPAddressV6("fc00::1"),
      kDestQpNum,
      IBV_MTU_2048,
      /*sqSigAll=*/true,
      /*maxRdAtomic=*/1);
  ASSERT_NE(ibvqp, nullptr);
  Qp* const qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  const uint16_t readMsn = qp->getTag(kMetaRoceInitialTag)->getNextMsn();
  struct ibv_sge sge = sgeAtOffset(kOffset, kMessageBytes);
  struct ibv_send_wr readWr{
      .wr_id = 1,
      .sg_list = &sge,
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_READ,
      .wr = {.rdma = {.remote_addr = 0x1000, .rkey = 1}}};
  struct ibv_send_wr* badWr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &readWr, &badWr), 0)
      << "precondition: the read is posted";
  ASSERT_TRUE(qp->waitForWqesToBePosted(1, std::chrono::milliseconds(100)))
      << "precondition: the read is pending when its response arrives";

  const auto payload = countingBytes(kMessageBytes, 1);
  const auto [baseHeader, dataHeader] = singlePacketHeaders(
      *qp,
      BaseHeader::Opcode::OperationType::READ_RESP,
      /*fsn=*/0,
      readMsn,
      /*csn=*/0);
  const DataPacket response(
      baseHeader, dataHeader, payload.data(), payload.size());
  qp->getRxFlowById(0)->processDataPacket(&response, kEcnECT0);

  EXPECT_EQ(buffer_, withBytesAtOffset(payload))
      << "ibv_post_send(3) gives a zero-based region's SGE address as an "
         "offset from the start of the region, for a read's destination too";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

TEST_F(ZeroBasedLocalBufferTest, AtomicResultsLandAtTheirOffsets) {
  struct ibv_qp* const ibvqp = setup_qp(
      verbs_,
      cq_,
      pd_,
      folly::IPAddressV6("fc00::1"),
      kDestQpNum,
      IBV_MTU_2048,
      /*sqSigAll=*/true,
      /*maxRdAtomic=*/2);
  ASSERT_NE(ibvqp, nullptr);
  Qp* const qp = verbs_.getQpByNum(MetaRoceVerbs::qpAndTagToQp(ibvqp->qp_num));
  ASSERT_NE(qp, nullptr);
  const uint16_t firstMsn = qp->getTag(kMetaRoceInitialTag)->getNextMsn();
  struct ibv_sge fetchAddSge = sgeAtOffset(kOffset, kAtomicOperationSizeBytes);
  struct ibv_sge compareSwapSge = sgeAtOffset(
      kOffset + kAtomicOperationSizeBytes, kAtomicOperationSizeBytes);
  struct ibv_send_wr compareSwap{
      .wr_id = 2,
      .sg_list = &compareSwapSge,
      .num_sge = 1,
      .opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
      .wr = {.atomic = {.remote_addr = 0x1008, .rkey = 1}}};
  struct ibv_send_wr fetchAdd{
      .wr_id = 1,
      .next = &compareSwap,
      .sg_list = &fetchAddSge,
      .num_sge = 1,
      .opcode = IBV_WR_ATOMIC_FETCH_AND_ADD,
      .wr = {.atomic = {.remote_addr = 0x1000, .rkey = 1}}};
  struct ibv_send_wr* badWr = nullptr;
  ASSERT_EQ(verbs_.ibv_post_send(ibvqp, &fetchAdd, &badWr), 0)
      << "precondition: both atomics are posted";
  ASSERT_TRUE(qp->waitForWqesToBePosted(2, std::chrono::milliseconds(100)))
      << "precondition: both atomics are pending when their acks arrive";

  const std::array<uint64_t, 2> originals{
      0x0102030405060708ULL, 0x1112131415161718ULL};
  for (uint16_t i = 0; i < originals.size(); ++i) {
    const auto [baseHeader, dataHeader] = singlePacketHeaders(
        *qp,
        BaseHeader::Opcode::OperationType::ATOMIC_ACK_RESP,
        /*fsn=*/i,
        static_cast<uint16_t>(firstMsn + i),
        /*csn=*/0);
    const AtomicAckPacket ack(
        baseHeader,
        dataHeader,
        AtomicAckHeader::AtomicAckHeaderBuilder()
            .setOrigRemoteData(originals.at(i))
            .build());
    qp->getRxFlowById(0)->processDataPacket(&ack, kEcnECT0);
  }

  std::vector<char> results(sizeof(originals));
  std::memcpy(results.data(), originals.data(), sizeof(originals));
  EXPECT_EQ(buffer_, withBytesAtOffset(results))
      << "ibv_post_send(3) gives a zero-based region's SGE address as an "
         "offset from the start of the region, for an atomic's result too";

  EXPECT_EQ(verbs_.ibv_destroy_qp(ibvqp), 0);
}

// Watches the wire for a send and a write each carrying a known payload.
struct PayloadsOnTheWire {
  std::vector<char> sendPayload;
  std::vector<char> writePayload;
  folly::Baton<> sendSeen;
  folly::Baton<> writeSeen;
  std::atomic_flag sendPosted = ATOMIC_FLAG_INIT;
  std::atomic_flag writePosted = ATOMIC_FLAG_INIT;

  auto socketMaker() {
    return [this](folly::EventBase* evb) -> SocketWithModifier {
      auto socket = std::make_unique<NiceMock<MockUDPSocket>>(evb);
      ON_CALL(*socket, write(_, _))
          .WillByDefault([this](
                             const folly::SocketAddress&,
                             const std::unique_ptr<folly::IOBuf>& buf) {
            auto wire = CHECK_NOTNULL(buf.get())->clone();
            wire->coalesce();
            if (carries(*wire, sendPayload) &&
                !sendPosted.test_and_set(std::memory_order_relaxed)) {
              sendSeen.post();
            }
            if (carries(*wire, writePayload) &&
                !writePosted.test_and_set(std::memory_order_relaxed)) {
              writeSeen.post();
            }
            return wire->computeChainDataLength();
          });
      return std::make_pair(std::move(socket), nullptr);
    };
  }

  static bool carries(
      const folly::IOBuf& wire,
      const std::vector<char>& bytes) {
    const auto* const begin = reinterpret_cast<const char*>(wire.data());
    const auto* const end = begin + wire.length();
    return std::search(begin, end, bytes.begin(), bytes.end()) != end;
  }
};

TEST(MetaRoceVerbsTest, ASendAndAWriteReadTheirSourceAtAZeroBasedOffset) {
  constexpr uint32_t kMessageBytes = 64;
  constexpr uint64_t kSendOffset = 32;
  constexpr uint64_t kWriteOffset = 96;
  PayloadsOnTheWire wire{
      .sendPayload = countingBytes(kMessageBytes, 1),
      .writePayload = countingBytes(kMessageBytes, 0x41)};
  std::vector<char> buffer(256, 0x5a);
  std::copy(
      wire.sendPayload.begin(),
      wire.sendPayload.end(),
      buffer.begin() + kSendOffset);
  std::copy(
      wire.writePayload.begin(),
      wire.writePayload.end(),
      buffer.begin() + kWriteOffset);

  libsoftmetaroce::MetaRoceVerbs verbs(wire.socketMaker());
  struct ibv_context context{};
  struct ibv_cq* const cq =
      verbs.ibv_create_cq(&context, 10, nullptr, nullptr, 0);
  ASSERT_NE(cq, nullptr);
  struct ibv_pd* const pd = verbs.ibv_alloc_pd(&context);
  ASSERT_NE(pd, nullptr);
  struct ibv_mr* const mr = verbs.ibv_reg_mr_internal(
      pd, buffer.data(), buffer.size(), IBV_ACCESS_ZERO_BASED);
  ASSERT_NE(mr, nullptr);
  struct ibv_qp* const ibvqp =
      setup_qp(verbs, cq, pd, folly::IPAddressV6("fc00::1"), 10, IBV_MTU_2048);
  ASSERT_NE(ibvqp, nullptr);

  struct ibv_sge sendSge{
      .addr = kSendOffset, .length = kMessageBytes, .lkey = mr->lkey};
  struct ibv_sge writeSge{
      .addr = kWriteOffset, .length = kMessageBytes, .lkey = mr->lkey};
  struct ibv_send_wr write{
      .wr_id = 2,
      .sg_list = &writeSge,
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_WRITE,
      .wr = {.rdma = {.remote_addr = 0x1000, .rkey = 1}}};
  struct ibv_send_wr send{
      .wr_id = 1,
      .next = &write,
      .sg_list = &sendSge,
      .num_sge = 1,
      .opcode = IBV_WR_SEND};
  struct ibv_send_wr* badWr = nullptr;
  ASSERT_EQ(verbs.ibv_post_send(ibvqp, &send, &badWr), 0)
      << "precondition: both requests are posted";

  EXPECT_TRUE(wire.sendSeen.try_wait_for(std::chrono::seconds(5)))
      << "ibv_post_send(3) gives a zero-based region's SGE address as an "
         "offset from the start of the region, for a send's source";
  EXPECT_TRUE(wire.writeSeen.try_wait_for(std::chrono::seconds(5)))
      << "ibv_post_send(3) gives a zero-based region's SGE address as an "
         "offset from the start of the region, for a write's source";

  EXPECT_EQ(verbs.ibv_destroy_qp(ibvqp), 0);
  EXPECT_EQ(verbs.ibv_dereg_mr(mr), 0);
  EXPECT_EQ(verbs.ibv_dealloc_pd(pd), 0);
  EXPECT_EQ(verbs.ibv_destroy_cq(cq), 0);
}

} // namespace libsoftmetaroce
