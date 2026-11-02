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

#include "rdma_gen/memory/GpuMemory.h"
#include <fmt/format.h>

namespace facebook::neteng::ai::rdma_gen {

void GpuMemory::initGpu() {
#ifndef NO_CUDA
  int cuda_pci_bus_id;
  int cuda_pci_device_id;
  int index;
  CUdevice cu_device;

  int res = cuInit(0);
  if (res == 10000) {
    LOG(ERROR)
        << "Failed to cuInit with error code consistent with testing on a machine without GPU";
    return;
  }
  CUresult error = (CUresult)res;
  CHECK_EQ(error, CUDA_SUCCESS)
      << fmt::format("cuInit(0) returned: {}", fmt::underlying(error));

  int deviceCount = 0;
  error = cuDeviceGetCount(&deviceCount);

  CHECK_EQ(error, CUDA_SUCCESS)
      << fmt::format("cuDeviceGetCount() returned: {}", fmt::underlying(error));
  CHECK_NE(deviceCount, 0)
      << "There are no available device(s) that support CUDA";
  CHECK_LT(params_.gpu_idx, deviceCount) << fmt::format(
      "No such device ID ({}) exists in system", params_.gpu_idx);
  LOG(INFO) << "Listing all CUDA devices in system";

  for (index = 0; index < deviceCount; index++) {
    error = cuDeviceGet(&cu_device, index);
    CHECK_EQ(error, CUDA_SUCCESS)
        << fmt::format("cuDeviceGet() returned: {}", fmt::underlying(error));

    error = cuDeviceGetAttribute(
        &cuda_pci_bus_id, CU_DEVICE_ATTRIBUTE_PCI_BUS_ID, cu_device);
    CHECK_EQ(error, CUDA_SUCCESS) << fmt::format(
        "cuDeviceGetAttribute(CU_DEVICE_ATTRIBUTE_PCI_BUS_ID) returned: {} for cu_device={}",
        fmt::underlying(error),
        cu_device);

    error = cuDeviceGetAttribute(
        &cuda_pci_device_id, CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID, cu_device);
    CHECK_EQ(error, CUDA_SUCCESS) << fmt::format(
        "cuDeviceGetAttribute(CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID) returned: {} for cuda_pci_device_id={}",
        fmt::underlying(error),
        cuda_pci_device_id);
    LOG(INFO) << fmt::format(
        "CUDA device: {} PCIe address is {}:{}",
        index,
        cuda_pci_bus_id,
        cuda_pci_device_id);
  }

  LOG(INFO) << fmt::format("Picking device: {}", params_.gpu_idx);
  error = cuDeviceGet(&cuDevice_, params_.gpu_idx);
  CHECK_EQ(error, CUDA_SUCCESS)
      << fmt::format("cuDeviceGet() returned: {}", fmt::underlying(error));

  std::array<char, 128> name{};
  error = cuDeviceGetName(std::data(name), sizeof(name), params_.gpu_idx);
  CHECK_EQ(error, CUDA_SUCCESS)
      << fmt::format("cuDeviceGetName() returned: {}", fmt::underlying(error));

  LOG(INFO) << fmt::format("Picking device: {}", params_.gpu_idx);
  LOG(INFO) << fmt::format(
      "[pid: {}, dev: {}], device name: [{}]",
      getpid(),
      cuDevice_,
      std::data(name));

#if CUDA_VERSION >= 13000
  // CUDA 13 redirects cuCtxCreate to cuCtxCreate_v4, which takes an extra
  // CUctxCreateParams*; null selects the same behavior as the v2 signature.
  error = cuCtxCreate(&ctx()->cuContext, nullptr, CU_CTX_MAP_HOST, cuDevice_);
#else
  error = cuCtxCreate(&ctx()->cuContext, CU_CTX_MAP_HOST, cuDevice_);
#endif
  CHECK_EQ(error, CUDA_SUCCESS)
      << fmt::format("cuCtxCreate() returned: {}", fmt::underlying(error));

  error = cuCtxSetCurrent(ctx()->cuContext);
  CHECK_EQ(error, CUDA_SUCCESS)
      << fmt::format("cuCtxCSetCurrent() returned: {}", fmt::underlying(error));
  return;
#else
  throw std::runtime_error("rdma_gen compiled without CUDA");
#endif // ifndef NO_CUDA
}

void GpuMemory::pushBlock(int buffer_cap) {
#ifndef NO_CUDA
  auto len = getLen(buffer_cap);

  CUdeviceptr d_A;
  int error;
  const size_t gpu_page_size = 64 * 1024;
  size_t size = (len + gpu_page_size - 1) & ~(gpu_page_size - 1);
  error = cuda()->cuMemAlloc(&d_A, size);
  if (error != CUDA_SUCCESS) {
    LOG(ERROR) << fmt::format("cuMemAlloc error: {}", error);
    return;
  }
  LOG(INFO) << fmt::format(
      "Allocated GPU buffer address at: {}, pointer: {}", d_A, (void*)d_A);
  auto b = (void*)d_A;
  unsigned long base_offset = 0;
  for (int i = 0; i < params_.qp_cap; i++) {
    for (int j = 0; j < buffer_cap; j++) {
      auto c = (char*)b;
      ctx()->gpu_buffers.push_back(
          (void*)(c + base_offset + (j * params_.message_size[i])));
    }
    base_offset += buffer_cap * params_.message_size[i];
  }

  ctx()->freeBuffers = [](RcQpContext* ctx) {
    // from the block, only the [0] was an address allocated by cuMemAlloc
    cuMemFree((CUdeviceptr)(ctx->gpu_buffers[0]));
    if (ctx->cuContext) {
      CHECK_EQ(CUDA_SUCCESS, cuCtxDestroy(ctx->cuContext));
    }
  };
#else
  throw std::runtime_error("rdma_gen compiled without CUDA");
#endif // ifndef NO_CUDA
}

void GpuMemory::pushBuffers(int buffer_cap) {
#ifndef NO_CUDA
  for (int i = 0; i < params_.qp_cap; i++) {
    for (int j = 0; j < buffer_cap; j++) {
      const auto len = params_.message_size[i];
      CUdeviceptr d_A;
      int error;
      const size_t gpu_page_size = 64 * 1024;
      size_t size = (len + gpu_page_size - 1) & ~(gpu_page_size - 1);
      error = cuda()->cuMemAlloc(&d_A, size);
      if (error != CUDA_SUCCESS) {
        LOG(ERROR) << fmt::format("cuMemAlloc error: {}", error);
        return;
      }
      LOG(INFO) << fmt::format(
          "Allocated GPU buffer address at: {}, pointer: {}", d_A, (void*)d_A);
      auto b = (void*)d_A;
      ctx()->gpu_buffers.push_back(b);
    }
  }
  ctx()->freeBuffers = [](RcQpContext* ctx) {
    for (auto* b : ctx->gpu_buffers) {
      auto d_A = (CUdeviceptr)b;
      cuMemFree(d_A);
    }
    if (ctx->cuContext) {
      CHECK_EQ(CUDA_SUCCESS, cuCtxDestroy(ctx->cuContext));
    }
  };
#else
  throw std::runtime_error("rdma_gen compiled without CUDA");
#endif // ifndef NO_CUDA
}

void GpuMemory::regBlock(int buffer_cap) {
  auto len = getLen(buffer_cap);

  auto mr = ctx()->pd_adapter->registerMemory(
      ctx()->gpu_buffers[0], len, accessFlags_);
  ctx()->mr_list.emplace_back(std::move(mr));
}

void GpuMemory::regBuffers(int buffer_cap) {
  for (int i = 0; i < params_.qp_cap; i++) {
    for (int j = 0; j < buffer_cap; j++) {
      auto idx = i * buffer_cap + j;
      auto mr = ctx()->pd_adapter->registerMemory(
          ctx()->gpu_buffers[idx], params_.message_size[i], accessFlags_);
      ctx()->mr_list.emplace_back(std::move(mr));
    }
  }
}
} // namespace facebook::neteng::ai::rdma_gen
