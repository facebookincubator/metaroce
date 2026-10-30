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

/**
 * @file metaroce_config_tutorial.cpp
 * @brief MetaRoCE Configuration Tutorial
 *
 * This file demonstrates how to program a host using MetaRoCE through the
 * standard RDMA verbs API. MetaRoCE reuses the existing
 * RDMA verbs API semantics with augmented functionality, avoiding the need to
 * modify the default libibverbs header files.
 *
 * This example covers:
 *   1. Creating a MetaRoCE QP (using IBV_QPT_RC)
 *   2. Creating and managing Tags
 *   3. Configuring multiple ports/flows
 *   4. Setting initial MSN/CSN values
 *   5. Posting send/receive operations
 *   6. Polling for completions
 */

#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include <infiniband/verbs.h>

// ============================================================================
// MetaRoCE-Specific Constants and Encoding
// ============================================================================

namespace metaroce {

/**
 * Maximum number of Tags per QP.
 * Tags are encoded in the upper 8 bits of the QP number field.
 */
[[maybe_unused]] constexpr uint32_t kMaxTagsPerQp = 256;

/**
 * QP number bit width (24 bits for base QPN).
 */
constexpr uint32_t kQpNumBits = 24;

/**
 * Mask for extracting the QP number from combined QP+Tag value (24 bits).
 */
constexpr uint32_t kQpNumMask = (1 << kQpNumBits) - 1; // 0x00FFFFFF

} // namespace metaroce

// ============================================================================
// MetaRoCE Wrapper Functions
// ============================================================================

/**
 * @brief Combines a QP number and Tag number into a single 32-bit value.
 *
 * MetaRoCE encodes Tags within the QP number field. The format is:
 *   (tag << 24) | QPN
 *
 * The tag occupies the upper 8 bits, and the QP number occupies the lower
 * 24 bits. This allows up to 256 Tags per QP while maintaining compatibility
 * with the standard ibv_qp structure.
 *
 * @param qpNum The 24-bit QP number.
 * @param tagNum The 8-bit Tag number.
 * @return Combined 32-bit value representing QP and Tag.
 */
inline uint32_t metaroce_combine_qp_and_tag(uint32_t qpNum, uint8_t tagNum) {
  return (static_cast<uint32_t>(tagNum) << metaroce::kQpNumBits) |
      (qpNum & metaroce::kQpNumMask);
}

/**
 * @brief Extracts the Tag number from a combined QP+Tag value.
 *
 * The tag is stored in the upper 8 bits of the 32-bit value.
 *
 * @param qpAndTag Combined 32-bit value from ibv_qp->qp_num.
 * @return The 8-bit Tag number.
 */
inline uint8_t metaroce_extract_tag(uint32_t qpAndTag) {
  return static_cast<uint8_t>(qpAndTag >> metaroce::kQpNumBits);
}

/**
 * @brief Extracts the QP number from a combined QP+Tag value.
 *
 * The QP number is stored in the lower 24 bits of the 32-bit value.
 *
 * @param qpAndTag Combined 32-bit value from ibv_qp->qp_num.
 * @return The 24-bit QP number.
 */
inline uint32_t metaroce_extract_qp(uint32_t qpAndTag) {
  return qpAndTag & metaroce::kQpNumMask;
}

/**
 * @brief Creates initial CSN/MSN encoding for ibv_modify_qp.
 *
 * MetaRoCE reuses the 32-bit sq_psn and rq_psn fields in ibv_qp_attr to
 * set the initial CSN and MSN values:
 *   (CSN << 16) | MSN
 *
 * The CSN occupies the upper 16 bits, and the MSN occupies the lower 16 bits.
 *
 * @param csn Initial Completion Sequence Number (16-bit).
 * @param msn Initial Message Sequence Number (16-bit).
 * @return Combined 32-bit value for sq_psn/rq_psn fields.
 */
inline uint32_t metaroce_encode_csn_msn(uint16_t csn, uint16_t msn) {
  return (static_cast<uint32_t>(csn) << 16) | static_cast<uint32_t>(msn);
}

/**
 * @brief Converts an IPv6 address string to a GID structure.
 *
 * @param ipv6Str IPv6 address as a string (e.g., "fc00::1").
 * @param gid Pointer to the ibv_gid structure to fill.
 * @return 0 on success, -1 on failure.
 */
inline int ipv6_to_gid(const char* ipv6Str, union ibv_gid* gid) {
  return inet_pton(AF_INET6, ipv6Str, gid->raw) == 1 ? 0 : -1;
}

/**
 * @brief Creates a MetaRoCE QP.
 *
 * MetaRoCE QPs use IBV_QPT_RC (Reliable Connection). The base QP includes
 * Tag 0 by default.
 *
 * @param pd Protection Domain.
 * @param sendCq Completion Queue for send operations.
 * @param recvCq Completion Queue for receive operations.
 * @return Pointer to the created ibv_qp, or nullptr on failure.
 */
struct ibv_qp* metaroce_create_qp(
    struct ibv_pd* pd,
    struct ibv_cq* sendCq,
    struct ibv_cq* recvCq) {
  struct ibv_qp_init_attr initAttr = {};
  initAttr.send_cq = sendCq;
  initAttr.recv_cq = recvCq;
  initAttr.cap.max_send_wr = 128;
  initAttr.cap.max_recv_wr = 128;
  initAttr.cap.max_send_sge = 1;
  initAttr.cap.max_recv_sge = 1;

  initAttr.qp_type = IBV_QPT_RC;

  return ibv_create_qp(pd, &initAttr);
}

/**
 * @brief Creates a Tag on an existing MetaRoCE QP.
 *
 * Tags allow multiple independent streams
 * within a single QP. This uses ibv_create_qp_ex with IBV_QP_CREATE_SOURCE_QPN
 * flag, where source_qpn encodes both the base QP number and the Tag number.
 *
 * The encoding format is: (tagNum << 24) | baseQpNum
 *
 * @param context Device context.
 * @param pd Protection Domain.
 * @param sendCq Completion Queue for send operations.
 * @param recvCq Completion Queue for receive operations.
 * @param baseQp Pointer to the base QP.
 * @param tagNum The Tag number to create (1-255, Tag 0 is the base).
 * @return Pointer to the created ibv_qp for this Tag, or nullptr on failure.
 */
struct ibv_qp* metaroce_create_tag(
    struct ibv_context* context,
    struct ibv_pd* pd,
    struct ibv_cq* sendCq,
    struct ibv_cq* recvCq,
    struct ibv_qp* baseQp,
    uint8_t tagNum) {
  if (tagNum == 0) {
    std::cerr << "Error: Tag 0 is created with the base QP. Use "
                 "metaroce_create_qp instead."
              << std::endl;
    return nullptr;
  }

  // Extract the base QP number from the provided QP pointer
  uint32_t baseQpNum = metaroce_extract_qp(baseQp->qp_num);

  struct ibv_qp_init_attr_ex initAttrEx = {};
  initAttrEx.send_cq = sendCq;
  initAttrEx.recv_cq = recvCq;
  initAttrEx.cap.max_send_wr = 128;
  initAttrEx.cap.max_recv_wr = 128;
  initAttrEx.cap.max_send_sge = 1;
  initAttrEx.cap.max_recv_sge = 1;
  initAttrEx.qp_type = IBV_QPT_RC;
  initAttrEx.pd = pd;

  // IBV_QP_CREATE_SOURCE_QPN flag indicates we're specifying the source QPN.
  // The source_qpn field contains the combined (tagNum << 24 | baseQpNum).
  initAttrEx.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS | IBV_QP_INIT_ATTR_PD;
  initAttrEx.create_flags = IBV_QP_CREATE_SOURCE_QPN;
  initAttrEx.source_qpn = metaroce_combine_qp_and_tag(baseQpNum, tagNum);

  return ibv_create_qp_ex(context, &initAttrEx);
}

/**
 * @brief Configures the initial CSN and MSN for a QP or Tag.
 *
 * This function sets up the initial sequence numbers for reliable delivery.
 * MetaRoCE reuses the sq_psn and rq_psn fields in ibv_qp_attr to encode
 * both CSN and MSN values using the format: (CSN << 16) | MSN.
 *
 *   - sq_psn: Local (send-side) sequence numbers
 *   - rq_psn: Remote (receive-side) sequence numbers
 *
 * This should be called after metaroce_qp_init() and before
 * metaroce_qp_rtr() for each QP and Tag.
 *
 * @param qp Pointer to the ibv_qp (QP or Tag).
 * @param localCsn Local Completion Sequence Number (send-side).
 * @param localMsn Local Message Sequence Number (send-side).
 * @param remoteCsn Remote Completion Sequence Number (receive-side).
 * @param remoteMsn Remote Message Sequence Number (receive-side).
 * @return 0 on success, non-zero on failure.
 */
int metaroce_configure_sequence_numbers(
    struct ibv_qp* qp,
    uint16_t localCsn,
    uint16_t localMsn,
    uint16_t remoteCsn,
    uint16_t remoteMsn) {
  struct ibv_qp_attr attr = {};
  attr.sq_psn = metaroce_encode_csn_msn(localCsn, localMsn);
  attr.rq_psn = metaroce_encode_csn_msn(remoteCsn, remoteMsn);

  return ibv_modify_qp(qp, &attr, IBV_QP_RQ_PSN | IBV_QP_SQ_PSN);
}

/**
 * @brief Adds a GID pair (SGID + DGID) to a MetaRoCE QP.
 *
 * MetaRoCE uses append semantics for ibv_modify_qp with IBV_QP_AV.
 * Each successive call adds a new GID pair instead of overwriting.
 * Duplicate pairs are ignored.
 *
 * This should be called after metaroce_qp_init() and before
 * metaroce_qp_rtr().
 *
 * @param qp Pointer to the ibv_qp.
 * @param localPort Local port number.
 * @param localSgidIndex Local SGID index.
 * @param remoteGid Remote GID (destination).
 * @return 0 on success, non-zero on failure.
 */
int metaroce_add_gid_pair(
    struct ibv_qp* qp,
    uint8_t localPort,
    uint8_t localSgidIndex,
    const union ibv_gid* remoteGid) {
  struct ibv_qp_attr attr = {};

  attr.ah_attr.port_num = localPort;
  attr.ah_attr.grh.sgid_index = localSgidIndex;
  std::memcpy(attr.ah_attr.grh.dgid.raw, remoteGid->raw, 16);
  attr.ah_attr.is_global = 1;

  return ibv_modify_qp(qp, &attr, IBV_QP_AV);
}

/**
 * @brief Transitions a MetaRoCE QP to INIT state.
 *
 * This is the first step in configuring a QP for communication.
 * After calling this function, you should:
 *   1. Add all GID pairs using metaroce_add_gid_pair()
 *   2. Create all Tags using metaroce_create_tag()
 *   3. Call metaroce_qp_rtr() to transition to RTR
 *   4. Call metaroce_qp_rts() to transition to RTS
 *
 * @param qp Pointer to the ibv_qp to initialize.
 * @param localPort Local port number.
 * @return 0 on success, non-zero on failure.
 */
int metaroce_qp_init(struct ibv_qp* qp, uint8_t localPort) {
  struct ibv_qp_attr attr = {};

  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = localPort;
  attr.qp_access_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

  int ret = ibv_modify_qp(
      qp,
      &attr,
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
  if (ret != 0) {
    std::cerr << "Failed to transition QP to INIT state: " << strerror(errno)
              << std::endl;
  }
  return ret;
}

/**
 * @brief Transitions a MetaRoCE QP to RTR (Ready to Receive) state.
 *
 * This should be called after all configuration is complete:
 *   - All GID pairs added via metaroce_add_gid_pair()
 *   - All Tags created via metaroce_create_tag()
 *
 * @param qp Pointer to the ibv_qp.
 * @param remoteQpNum Remote QP number.
 * @param remoteCsn Remote (receive-side) initial CSN.
 * @param remoteMsn Remote (receive-side) initial MSN.
 * @return 0 on success, non-zero on failure.
 */
int metaroce_qp_rtr(
    struct ibv_qp* qp,
    uint32_t remoteQpNum,
    uint16_t remoteCsn,
    uint16_t remoteMsn) {
  struct ibv_qp_attr attr = {};

  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_4096;
  attr.dest_qp_num = remoteQpNum;
  // rq_psn: Remote (receive-side) sequence numbers
  // Format: (CSN << 16) | MSN
  attr.rq_psn = metaroce_encode_csn_msn(remoteCsn, remoteMsn);
  attr.max_dest_rd_atomic = 16;
  attr.min_rnr_timer = 12;

  int ret = ibv_modify_qp(
      qp,
      &attr,
      IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
          IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
  if (ret != 0) {
    std::cerr << "Failed to transition QP to RTR state: " << strerror(errno)
              << std::endl;
  }
  return ret;
}

/**
 * @brief Transitions a MetaRoCE QP to RTS (Ready to Send) state.
 *
 * This is the final step before the QP can be used for communication.
 * Must be called after metaroce_qp_rtr().
 *
 * @param qp Pointer to the ibv_qp.
 * @param localCsn Local (send-side) initial CSN.
 * @param localMsn Local (send-side) initial MSN.
 * @return 0 on success, non-zero on failure.
 */
int metaroce_qp_rts(struct ibv_qp* qp, uint16_t localCsn, uint16_t localMsn) {
  struct ibv_qp_attr attr = {};

  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = metaroce_encode_csn_msn(localCsn, localMsn);
  attr.max_rd_atomic = 16;

  int ret = ibv_modify_qp(
      qp,
      &attr,
      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
  if (ret != 0) {
    std::cerr << "Failed to transition QP to RTS state: " << strerror(errno)
              << std::endl;
  }
  return ret;
}

/**
 * @brief Registers a memory region for RDMA operations.
 *
 * @param pd Protection Domain.
 * @param buffer Pointer to the memory buffer.
 * @param length Size of the buffer in bytes.
 * @return Pointer to the registered ibv_mr, or nullptr on failure.
 */
struct ibv_mr*
metaroce_register_memory(struct ibv_pd* pd, void* buffer, size_t length) {
  return ibv_reg_mr(
      pd,
      buffer,
      length,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
          IBV_ACCESS_REMOTE_READ);
}

/**
 * @brief Posts a send operation on a specific Tag.
 *
 * @param tagQp Pointer to the ibv_qp for this Tag.
 * @param sge Scatter-gather entry describing the data to send.
 * @param wrId Work request ID for completion tracking.
 * @return 0 on success, non-zero on failure.
 */
int metaroce_post_send(
    struct ibv_qp* tagQp,
    struct ibv_sge* sge,
    uint64_t wrId) {
  struct ibv_send_wr* badWr = nullptr;
  struct ibv_send_wr sendWr = {};
  sendWr.wr_id = wrId;
  sendWr.sg_list = sge;
  sendWr.num_sge = 1;
  sendWr.opcode = IBV_WR_SEND;
  sendWr.send_flags = IBV_SEND_SIGNALED;

  return ibv_post_send(tagQp, &sendWr, &badWr);
}

/**
 * @brief Posts a receive operation on a specific Tag.
 *
 * @param tagQp Pointer to the ibv_qp for this Tag.
 * @param sge Scatter-gather entry describing the receive buffer.
 * @param wrId Work request ID for completion tracking.
 * @return 0 on success, non-zero on failure.
 */
int metaroce_post_recv(
    struct ibv_qp* tagQp,
    struct ibv_sge* sge,
    uint64_t wrId) {
  struct ibv_recv_wr* badWr = nullptr;
  struct ibv_recv_wr recvWr = {};
  recvWr.wr_id = wrId;
  recvWr.sg_list = sge;
  recvWr.num_sge = 1;

  return ibv_post_recv(tagQp, &recvWr, &badWr);
}

/**
 * @brief Polls for completions on a CQ.
 *
 * @param cq Completion Queue to poll.
 * @param maxCompletions Maximum number of completions to retrieve.
 * @param completions Vector to store the work completions.
 * @return Number of completions retrieved.
 */
int metaroce_poll_cq(
    struct ibv_cq* cq,
    int maxCompletions,
    std::vector<struct ibv_wc>& completions) {
  completions.resize(maxCompletions);
  int numCompletions = ibv_poll_cq(cq, maxCompletions, completions.data());
  if (numCompletions > 0) {
    completions.resize(numCompletions);
  } else {
    completions.clear();
  }
  return numCompletions;
}

// ============================================================================
// Tag Management Structure
// ============================================================================
namespace {
/**
 * @brief Holds metadata for a single Tag including its QP handle and buffers.
 */
struct TagContext {
  struct ibv_qp* qp = nullptr;
  uint8_t tagNum = 0;
  std::vector<char> sendBuffer;
  std::vector<char> recvBuffer;
  struct ibv_mr* sendMr = nullptr;
  struct ibv_mr* recvMr = nullptr;
  struct ibv_sge sendSge = {};
  struct ibv_sge recvSge = {};
};

/**
 * @brief Holds all MetaRoCE resources for cleanup.
 *
 * This structure centralizes resource management.
 */
struct MetaRoceResources {
  struct ibv_context* context = nullptr;
  struct ibv_pd* pd = nullptr;
  struct ibv_cq* cq = nullptr;
  std::vector<TagContext>* tagContexts = nullptr;

  /**
   * @brief Cleans up all allocated RDMA resources.
   *
   * Resources are freed in reverse order of allocation:
   * memory regions -> QPs -> CQ -> PD -> context
   */
  void cleanup() {
    if (tagContexts) {
      for (auto& ctx : *tagContexts) {
        if (ctx.sendMr) {
          ibv_dereg_mr(ctx.sendMr);
        }
        if (ctx.recvMr) {
          ibv_dereg_mr(ctx.recvMr);
        }
        if (ctx.qp) {
          ibv_destroy_qp(ctx.qp);
        }
      }
    }
    if (cq) {
      ibv_destroy_cq(cq);
    }
    if (pd) {
      ibv_dealloc_pd(pd);
    }
    if (context) {
      ibv_close_device(context);
    }
  }
};
} // namespace
// ============================================================================
// Configuration
// ============================================================================

// An application would take these from command-line arguments or a
// configuration file; this tutorial fixes them as constants.
const char* const kDeviceName = nullptr; // Use first device if nullptr
const char* const kRemoteAddr = "fc00::2";
const char* const kAlternateRemoteAddr = "fc00::4";
constexpr uint32_t kNumTags = 2;
constexpr uint32_t kBufferSize = 4096;
constexpr uint16_t kLocalCsn = 0xface; // Local (send-side) initial CSN
constexpr uint16_t kLocalMsn = 0xb00c; // Local (send-side) initial MSN
constexpr uint16_t kRemoteCsn = 0xfeed; // Remote (receive-side) initial CSN
constexpr uint16_t kRemoteMsn = 0xabed; // Remote (receive-side) initial MSN
constexpr bool kEnableMultiport = false;
constexpr uint32_t kRemoteQpNum = 2; // Remote QP number to connect to

// ============================================================================
// Main Tutorial
// ============================================================================

int main(int /* argc */, char* /* argv */[]) {
  std::cout << "=== MetaRoCE Programming Tutorial ===" << std::endl;
  std::cout << std::endl;

  // Validate number of Tags
  if (kNumTags < 1 || kNumTags > metaroce::kMaxTagsPerQp) {
    std::cerr << "Error: num_tags must be between 1 and "
              << metaroce::kMaxTagsPerQp << std::endl;
    return 1;
  }

  // -------------------------------------------------------------------------
  // Step 1: Get RDMA device list and open device
  // -------------------------------------------------------------------------
  std::cout << "Step 1: Opening RDMA device..." << std::endl;

  int numDevices = 0;
  struct ibv_device** deviceList = ibv_get_device_list(&numDevices);
  if (!deviceList || numDevices == 0) {
    std::cerr << "Error: No RDMA devices found" << std::endl;
    return 1;
  }

  // Find the requested device or use the first one
  struct ibv_device* device = nullptr;
  if (kDeviceName) {
    for (int i = 0; i < numDevices; ++i) {
      if (strcmp(ibv_get_device_name(deviceList[i]), kDeviceName) == 0) {
        device = deviceList[i];
        break;
      }
    }
    if (!device) {
      std::cerr << "Error: Device '" << kDeviceName << "' not found"
                << std::endl;
      ibv_free_device_list(deviceList);
      return 1;
    }
  } else {
    device = deviceList[0];
  }

  std::cout << "  Using device: " << ibv_get_device_name(device) << std::endl;

  struct ibv_context* context = ibv_open_device(device);
  if (!context) {
    std::cerr << "Error: Failed to open device" << std::endl;
    ibv_free_device_list(deviceList);
    return 1;
  }

  ibv_free_device_list(deviceList);

  // Initialize resources struct for centralized cleanup
  MetaRoceResources resources;
  resources.context = context;

  // -------------------------------------------------------------------------
  // Step 2: Allocate Protection Domain and Completion Queue
  // -------------------------------------------------------------------------
  std::cout << "Step 2: Allocating RDMA resources..." << std::endl;

  struct ibv_pd* pd = ibv_alloc_pd(context);
  if (!pd) {
    std::cerr << "Error: Failed to allocate Protection Domain" << std::endl;
    resources.cleanup();
    return 1;
  }
  resources.pd = pd;

  struct ibv_cq* cq = ibv_create_cq(
      context,
      128, // CQ size
      nullptr, // CQ context
      nullptr, // Completion channel
      0 // Completion vector
  );
  if (!cq) {
    std::cerr << "Error: Failed to create Completion Queue" << std::endl;
    resources.cleanup();
    return 1;
  }
  resources.cq = cq;

  // -------------------------------------------------------------------------
  // Step 3: Create MetaRoCE QP (with base Tag 0)
  // -------------------------------------------------------------------------
  std::cout << "Step 3: Creating MetaRoCE QP with IBV_QPT_RC..." << std::endl;

  struct ibv_qp* baseQp = metaroce_create_qp(pd, cq, cq);
  if (!baseQp) {
    std::cerr << "Error: Failed to create MetaRoCE QP" << std::endl;
    resources.cleanup();
    return 1;
  }

  uint32_t baseQpNum = metaroce_extract_qp(baseQp->qp_num);
  std::cout << "  Base QP number: " << baseQpNum << std::endl;
  std::cout << "  Tag 0 is included with base QP" << std::endl;

  // -------------------------------------------------------------------------
  // Step 4: Create additional Tags using ibv_create_qp_ex
  // -------------------------------------------------------------------------
  std::cout << "Step 4: Creating " << kNumTags - 1 << " additional Tags..."
            << std::endl;

  std::vector<TagContext> tagContexts(kNumTags);
  resources.tagContexts = &tagContexts;

  // Tag 0 is the base QP
  tagContexts[0].qp = baseQp;
  tagContexts[0].tagNum = 0;

  // Create Tags 1 through num_tags-1
  for (uint32_t tagNum = 1; tagNum < kNumTags; ++tagNum) {
    std::cout << "  Creating Tag " << tagNum << "..." << std::endl;

    struct ibv_qp* tagQp = metaroce_create_tag(
        context, pd, cq, cq, baseQp, static_cast<uint8_t>(tagNum));

    if (!tagQp) {
      std::cerr << "Error: Failed to create Tag " << tagNum << std::endl;
      resources.cleanup();
      return 1;
    }

    tagContexts[tagNum].qp = tagQp;
    tagContexts[tagNum].tagNum = static_cast<uint8_t>(tagNum);
  }

  // -------------------------------------------------------------------------
  // Step 5: Configure QP - transition to INIT and add GID pairs
  // -------------------------------------------------------------------------
  std::cout << "Step 5: Configuring QP..." << std::endl;

  // Step 5a: Transition QP to INIT state
  std::cout << "  Transitioning QP to INIT state..." << std::endl;
  int ret = metaroce_qp_init(baseQp, 1);
  if (ret != 0) {
    std::cerr << "Error: Failed to transition QP to INIT" << std::endl;
    resources.cleanup();
    return 1;
  }

  // Step 5b: Add GID pairs (all configuration must be done before RTR)
  union ibv_gid remoteGid{};
  if (ipv6_to_gid(kRemoteAddr, &remoteGid) != 0) {
    std::cerr << "Error: Invalid remote address: " << kRemoteAddr << std::endl;
    resources.cleanup();
    return 1;
  }

  // Add GID pairs for multi-port:
  //   port 1 -> kRemoteAddr
  //   port 2 -> kAlternateRemoteAddr (only if multi-port enabled)
  std::cout << "  Adding GID pair: port 1, sgid_index 0 -> " << kRemoteAddr
            << std::endl;
  ret = metaroce_add_gid_pair(baseQp, 1, 0, &remoteGid);
  if (ret != 0) {
    std::cerr << "Warning: Failed to add GID pair" << std::endl;
  }

  if (kEnableMultiport) {
    std::cout << "  Multi-port enabled - adding second GID pair..."
              << std::endl;

    union ibv_gid altRemoteGid{};
    if (ipv6_to_gid(kAlternateRemoteAddr, &altRemoteGid) != 0) {
      std::cerr << "Error: Invalid alternate remote address: "
                << kAlternateRemoteAddr << std::endl;
      resources.cleanup();
      return 1;
    }

    std::cout << "    Adding GID pair: port 2, sgid_index 0 -> "
              << kAlternateRemoteAddr << std::endl;
    ret = metaroce_add_gid_pair(baseQp, 2, 0, &altRemoteGid);
    if (ret != 0) {
      std::cerr << "Warning: Failed to add GID pair on port 2" << std::endl;
    }
  }

  // Step 5c: Configure sequence numbers for all Tags (including base QP)
  std::cout << "  Configuring sequence numbers for all Tags..." << std::endl;
  for (auto& ctx : tagContexts) {
    ret = metaroce_configure_sequence_numbers(
        ctx.qp, kLocalCsn, kLocalMsn, kRemoteCsn, kRemoteMsn);
    if (ret != 0) {
      std::cerr << "Warning: Failed to configure sequence numbers for Tag "
                << static_cast<int>(ctx.tagNum) << std::endl;
    }
  }

  // -------------------------------------------------------------------------
  // Step 6: Transition QP to RTR and RTS
  // -------------------------------------------------------------------------
  std::cout << "Step 6: Transitioning QP to RTR and RTS..." << std::endl;

  std::cout << "  Transitioning to RTR..." << std::endl;
  ret = metaroce_qp_rtr(baseQp, kRemoteQpNum, kRemoteCsn, kRemoteMsn);
  if (ret != 0) {
    std::cerr << "Error: Failed to transition QP to RTR" << std::endl;
    resources.cleanup();
    return 1;
  }

  std::cout << "  Transitioning to RTS..." << std::endl;
  ret = metaroce_qp_rts(baseQp, kLocalCsn, kLocalMsn);
  if (ret != 0) {
    std::cerr << "Error: Failed to transition QP to RTS" << std::endl;
    resources.cleanup();
    return 1;
  }

  // -------------------------------------------------------------------------
  // Step 7: Register memory and prepare SGEs for each Tag
  // -------------------------------------------------------------------------
  std::cout << "Step 7: Registering memory regions..." << std::endl;

  for (auto& ctx : tagContexts) {
    ctx.sendBuffer.resize(kBufferSize);
    ctx.recvBuffer.resize(kBufferSize);

    // Fill send buffer with pattern for this Tag
    std::memset(
        ctx.sendBuffer.data(), static_cast<int>(ctx.tagNum), kBufferSize);

    // Register send buffer
    ctx.sendMr =
        metaroce_register_memory(pd, ctx.sendBuffer.data(), kBufferSize);
    if (!ctx.sendMr) {
      std::cerr << "Error: Failed to register send MR for Tag "
                << static_cast<int>(ctx.tagNum) << std::endl;
      resources.cleanup();
      return 1;
    }

    // Register receive buffer
    ctx.recvMr =
        metaroce_register_memory(pd, ctx.recvBuffer.data(), kBufferSize);
    if (!ctx.recvMr) {
      std::cerr << "Error: Failed to register receive MR for Tag "
                << static_cast<int>(ctx.tagNum) << std::endl;
      resources.cleanup();
      return 1;
    }

    // Prepare SGEs
    ctx.sendSge.addr = reinterpret_cast<uint64_t>(ctx.sendBuffer.data());
    ctx.sendSge.length = kBufferSize;
    ctx.sendSge.lkey = ctx.sendMr->lkey;

    ctx.recvSge.addr = reinterpret_cast<uint64_t>(ctx.recvBuffer.data());
    ctx.recvSge.length = kBufferSize;
    ctx.recvSge.lkey = ctx.recvMr->lkey;

    std::cout << "  Tag " << static_cast<int>(ctx.tagNum)
              << ": send_lkey=" << ctx.sendMr->lkey
              << ", recv_lkey=" << ctx.recvMr->lkey << std::endl;
  }

  // -------------------------------------------------------------------------
  // Step 8: Post receive operations (pre-post receives before sends)
  // -------------------------------------------------------------------------
  std::cout << "Step 8: Posting receive operations..." << std::endl;

  for (auto& ctx : tagContexts) {
    // Work request ID encodes the Tag number
    uint64_t wrId = 1000 + ctx.tagNum;

    ret = metaroce_post_recv(ctx.qp, &ctx.recvSge, wrId);
    if (ret != 0) {
      std::cerr << "Error: Failed to post receive for Tag "
                << static_cast<int>(ctx.tagNum) << std::endl;
      resources.cleanup();
      return 1;
    }
    std::cout << "  Posted receive on Tag " << static_cast<int>(ctx.tagNum)
              << ", wr_id=" << wrId << std::endl;
  }

  // -------------------------------------------------------------------------
  // Step 9: Post send operations on each Tag
  // -------------------------------------------------------------------------
  std::cout << "Step 9: Posting send operations..." << std::endl;

  for (auto& ctx : tagContexts) {
    // Work request ID encodes the Tag number for easy identification
    uint64_t wrId = 2000 + ctx.tagNum;

    ret = metaroce_post_send(ctx.qp, &ctx.sendSge, wrId);
    if (ret != 0) {
      std::cerr << "Error: Failed to post send for Tag "
                << static_cast<int>(ctx.tagNum) << std::endl;
      resources.cleanup();
      return 1;
    }
    std::cout << "  Posted send on Tag " << static_cast<int>(ctx.tagNum)
              << ", wr_id=" << wrId << std::endl;
  }

  // -------------------------------------------------------------------------
  // Step 10: Poll for completions
  // -------------------------------------------------------------------------
  std::cout << "Step 10: Polling for completions..." << std::endl;

  int expectedCompletions = kNumTags * 2; // 1 send + 1 recv per Tag
  int totalCompletions = 0;
  int pollRetries = 0;
  constexpr int kMaxPollRetries = 100;

  while (totalCompletions < expectedCompletions &&
         pollRetries < kMaxPollRetries) {
    std::vector<struct ibv_wc> completions;
    int numCompletions = metaroce_poll_cq(cq, 16, completions);

    for (const auto& wc : completions) {
      const char* opType = (wc.wr_id >= 2000) ? "SEND" : "RECV";
      uint8_t tagFromWrId = static_cast<uint8_t>(wc.wr_id % 1000);

      if (wc.status == IBV_WC_SUCCESS) {
        std::cout << "  Completion: Tag=" << static_cast<int>(tagFromWrId)
                  << ", op=" << opType << ", status=SUCCESS" << std::endl;
      } else {
        std::cerr << "  Completion: Tag=" << static_cast<int>(tagFromWrId)
                  << ", op=" << opType << ", status=" << wc.status << " (ERROR)"
                  << std::endl;
      }
      ++totalCompletions;
    }

    if (numCompletions == 0) {
      // No completions yet, wait a bit
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      ++pollRetries;
    }
  }

  if (totalCompletions < expectedCompletions) {
    std::cout << "Warning: Only received " << totalCompletions << " of "
              << expectedCompletions << " expected completions" << std::endl;
  } else {
    std::cout << "All " << totalCompletions << " completions received!"
              << std::endl;
  }

  // -------------------------------------------------------------------------
  // Step 11: Cleanup
  // -------------------------------------------------------------------------
  std::cout << "Step 11: Cleaning up..." << std::endl;

  resources.cleanup();

  std::cout << std::endl;
  std::cout << "=== MetaRoCE Configuration Tutorial Complete ===" << std::endl;

  return 0;
}
