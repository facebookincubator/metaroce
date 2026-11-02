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

#include "rdma_gen/transport/ITransportProvider.h"

#include <fmt/format.h>
#include <glog/logging.h>

namespace facebook::neteng::ai::rdma_gen {

SendFlag convertVerbsSendFlags(uint32_t verbsFlags) {
  constexpr uint32_t kKnownVerbsFlags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
  uint32_t unknown = verbsFlags & ~kKnownVerbsFlags;
  if (unknown != 0) {
    LOG(FATAL) << fmt::format("Unrecognized verbs send flags: 0x{:x}", unknown);
  }

  uint32_t result = kNone;
  if (verbsFlags & IBV_SEND_SIGNALED) {
    result |= kSignaled;
  }
  if (verbsFlags & IBV_SEND_INLINE) {
    result |= kInline;
  }
  return static_cast<SendFlag>(result);
}

ITransportProvider::~ITransportProvider() {}
ITransportDevice::~ITransportDevice() {}
ITransportContext::~ITransportContext() {}
ITransportDomain::~ITransportDomain() {}
ITransportCq::~ITransportCq() {}
ITransportMr::~ITransportMr() {}
ITransportPeer::~ITransportPeer() {}

}; // namespace facebook::neteng::ai::rdma_gen
