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

#include <fmt/format.h>
#include <folly/IPAddressV6.h>
#include <folly/init/Init.h>
#include <folly/portability/GMock.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <libsoftmetaroce/MetaRoceVerbs.h>
#include <libsoftmetaroce/test/MockUDPSocket.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <ctime>
#include <memory>
#include <vector>

DEFINE_uint32(qps, 16, "Queue pairs to bring up, one after another");
DEFINE_bool(
    mock_sockets,
    false,
    "Use mock UDP sockets, which never call bind(2)");
DEFINE_bool(each, false, "Also print the RTR time of every queue pair");

using libsoftmetaroce::MetaRoceVerbs;
using libsoftmetaroce::Modifier;
using ::testing::NiceMock;

namespace {

constexpr int kCqEntries = 10;
constexpr uint32_t kRemoteQpNum = 10;

enum Step { kCreate, kInit, kRtr, kRts, kSteps };
constexpr const char* kStepNames[kSteps] = {"create", "INIT", "RTR", "RTS"};

struct Sample {
  double wallUs;
  double cpuUs;
};

double threadCpuUs() {
  struct timespec ts{};
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

template <typename F>
Sample timed(F&& step) {
  const auto wall0 = std::chrono::steady_clock::now();
  const double cpu0 = threadCpuUs();
  step();
  const double cpuUs = threadCpuUs() - cpu0;
  const std::chrono::duration<double, std::micro> wall =
      std::chrono::steady_clock::now() - wall0;
  return {wall.count(), cpuUs};
}

std::pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>
mockUDPSocketMaker(folly::EventBase* evb) {
  return std::make_pair(
      std::make_unique<NiceMock<libsoftmetaroce::MockUDPSocket>>(evb), nullptr);
}

std::unique_ptr<MetaRoceVerbs> makeVerbs() {
  if (FLAGS_mock_sockets) {
    return std::make_unique<MetaRoceVerbs>(mockUDPSocketMaker);
  }
  return std::make_unique<MetaRoceVerbs>();
}

struct Device {
  explicit Device(MetaRoceVerbs& verbs) : verbs(verbs) {
    int numDevices = 0;
    struct ibv_device** deviceList = verbs.ibv_get_device_list(&numDevices);
    CHECK_GT(numDevices, 0);
    context = verbs.ibv_open_device(deviceList[0]);
    verbs.ibv_free_device_list(deviceList);
    CHECK(context != nullptr);
    cq = verbs.ibv_create_cq(context, kCqEntries, nullptr, nullptr, 0);
    CHECK(cq != nullptr) << "create_cq failed, errno " << errno;
    pd = verbs.ibv_alloc_pd(context);
    CHECK(pd != nullptr) << "alloc_pd failed, errno " << errno;
  }

  ~Device() {
    for (struct ibv_qp* qp : qps) {
      CHECK_EQ(verbs.ibv_destroy_qp(qp), 0);
    }
    CHECK_EQ(verbs.ibv_dealloc_pd(pd), 0);
    CHECK_EQ(verbs.ibv_destroy_cq(cq), 0);
    CHECK_EQ(verbs.ibv_close_device(context), 0);
  }

  MetaRoceVerbs& verbs;
  struct ibv_context* context = nullptr;
  struct ibv_cq* cq = nullptr;
  struct ibv_pd* pd = nullptr;
  std::vector<struct ibv_qp*> qps;
};

std::array<Sample, kSteps> bringUpOne(Device& dev) {
  std::array<Sample, kSteps> samples{};
  struct ibv_qp* qp = nullptr;
  samples[kCreate] = timed([&] {
    struct ibv_qp_init_attr initAttr{};
    initAttr.send_cq = dev.cq;
    initAttr.recv_cq = dev.cq;
    initAttr.qp_type = IBV_QPT_RC;
    qp = dev.verbs.ibv_create_qp(dev.pd, &initAttr);
  });
  CHECK(qp != nullptr) << "create_qp failed, errno " << errno;
  dev.qps.push_back(qp);

  struct ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_INIT;
  samples[kInit] = timed(
      [&] { CHECK_EQ(dev.verbs.ibv_modify_qp(qp, &attr, IBV_QP_STATE), 0); });

  attr = {};
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_4096;
  attr.dest_qp_num = MetaRoceVerbs::combineQpAndTag(kRemoteQpNum, 0);
  attr.ah_attr.is_global = 1;
  attr.ah_attr.grh.sgid_index = 0;
  memcpy(
      attr.ah_attr.grh.dgid.raw,
      folly::IPAddressV6("fc00::1").toBinary().data(),
      sizeof(attr.ah_attr.grh.dgid.raw));
  samples[kRtr] = timed([&] {
    CHECK_EQ(
        dev.verbs.ibv_modify_qp(
            qp,
            &attr,
            IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN),
        0);
  });

  attr = {};
  attr.qp_state = IBV_QPS_RTS;
  samples[kRts] = timed([&] {
    CHECK_EQ(
        dev.verbs.ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN), 0);
  });
  return samples;
}

double median(std::vector<double> values) {
  std::sort(values.begin(), values.end());
  return values[values.size() / 2];
}

void report(const std::vector<std::array<Sample, kSteps>>& runs) {
  if (FLAGS_each) {
    for (size_t i = 0; i < runs.size(); ++i) {
      fmt::print(
          "qp {:3} RTR {:10.1f} us wall {:10.1f} us cpu\n",
          i,
          runs[i][kRtr].wallUs,
          runs[i][kRtr].cpuUs);
    }
  }
  for (int step = 0; step < kSteps; ++step) {
    const Sample& first = runs.front()[step];
    fmt::print(
        "{:6} first {:10.1f} us wall {:10.1f} us cpu",
        kStepNames[step],
        first.wallUs,
        first.cpuUs);
    std::vector<double> wall;
    std::vector<double> cpu;
    for (size_t i = 1; i < runs.size(); ++i) {
      wall.push_back(runs[i][step].wallUs);
      cpu.push_back(runs[i][step].cpuUs);
    }
    if (!wall.empty()) {
      fmt::print(
          " | later median {:8.1f} us wall {:8.1f} us cpu, max {:8.1f} us wall",
          median(wall),
          median(cpu),
          *std::max_element(wall.begin(), wall.end()));
    }
    fmt::print("\n");
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  CHECK_GT(FLAGS_qps, 0);
  // Owned here, so its sender and receiver threads stop before folly's
  // singletons shut down.
  std::unique_ptr<MetaRoceVerbs> verbs;
  const Sample construction = timed([&] { verbs = makeVerbs(); });
  Device dev(*verbs);
  std::vector<std::array<Sample, kSteps>> runs;
  runs.reserve(FLAGS_qps);
  for (uint32_t i = 0; i < FLAGS_qps; ++i) {
    runs.push_back(bringUpOne(dev));
  }
  fmt::print(
      "verbs  construct {:6.1f} us wall {:10.1f} us cpu\n",
      construction.wallUs,
      construction.cpuUs);
  report(runs);
  return 0;
}
