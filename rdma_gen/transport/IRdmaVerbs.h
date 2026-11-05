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

#include <infiniband/verbs.h>

namespace facebook::neteng::ai {

/**
 * IRdmaVerbs is an abstract interface for Infiniband Verbs
 */
class IRdmaVerbs {
 public:
  virtual ~IRdmaVerbs() = default;

  virtual struct ibv_context* ibv_open_device(struct ibv_device* device) = 0;

  virtual struct ibv_comp_channel* ibv_create_comp_channel(
      struct ibv_context* context) = 0;

  virtual struct ibv_cq* ibv_create_cq(
      struct ibv_context* context,
      int cqe,
      void* cq_context,
      struct ibv_comp_channel* channel,
      int comp_vector) = 0;

  virtual struct ibv_qp* ibv_create_qp(
      struct ibv_pd* pd,
      struct ibv_qp_init_attr* qp_init_attr) = 0;

  virtual struct ibv_qp* ibv_create_qp_ex(
      struct ibv_context* context,
      struct ibv_qp_init_attr_ex* qp_init_attr) = 0;

  virtual struct ibv_pd* ibv_alloc_pd(struct ibv_context* context) = 0;

  virtual struct ibv_mr* ibv_reg_mr_internal(
      struct ibv_pd* pd,
      void* addr,
      size_t length,
      int access) = 0;

  virtual struct ibv_mr* ibv_reg_mr_iova2(
      struct ibv_pd* pd,
      void* addr,
      size_t length,
      uint64_t iova,
      int access) = 0;

  virtual struct ibv_mr* ibv_reg_dmabuf_internal(
      struct ibv_pd* pd,
      uint64_t offset,
      size_t length,
      uint64_t iova,
      int fd,
      int access) = 0;

#ifndef IBVERBS_LACKS_DMAH
  virtual struct ibv_mr* ibv_reg_mr_ex(
      struct ibv_pd* pd,
      struct ibv_mr_init_attr* mr_init_attr) = 0;

  virtual struct ibv_dmah* ibv_alloc_dmah(
      struct ibv_context* context,
      struct ibv_dmah_init_attr* dmah_init_attr) = 0;
#endif

  virtual int ibv_query_device(
      struct ibv_context* context,
      struct ibv_device_attr* deviceAttr) = 0;

  virtual int ibv_query_port(
      struct ibv_context* context,
      uint8_t port_num,
      struct ibv_port_attr* port_attr) = 0;

  virtual int ibv_query_qp(
      struct ibv_qp* qp,
      struct ibv_qp_attr* attr,
      int attr_mask,
      struct ibv_qp_init_attr* init_attr) = 0;

  virtual int
  ibv_modify_qp(struct ibv_qp* qp, struct ibv_qp_attr* attr, int attr_mask) = 0;

  virtual int ibv_modify_qp_rate_limit(
      struct ibv_qp* qp,
      struct ibv_qp_rate_limit_attr* attr) = 0;

  virtual int ibv_query_gid(
      struct ibv_context* context,
      uint8_t port_num,
      int index,
      union ibv_gid* gid) = 0;

  virtual int ibv_query_gid_ex(
      struct ibv_context* context,
      uint8_t portNum,
      int index,
      struct ibv_gid_entry* entry,
      uint32_t flags) = 0;

  virtual int ibv_post_send(
      struct ibv_qp* qp,
      struct ibv_send_wr* wr,
      struct ibv_send_wr** bad_wr) = 0;

  virtual int ibv_post_recv(
      struct ibv_qp* qp,
      struct ibv_recv_wr* wr,
      struct ibv_recv_wr** bad_wr) = 0;

  virtual const char* ibv_wc_status_str(enum ibv_wc_status status) = 0;

  virtual int
  ibv_poll_cq(struct ibv_cq* cq, int num_entries, struct ibv_wc* wc) = 0;

  virtual struct ibv_device** ibv_get_device_list(int* num_devices) = 0;

  virtual const char* ibv_get_device_name(struct ibv_device* device) = 0;

  virtual int ibv_get_device_index(struct ibv_device* device) = 0;

  virtual int ibv_get_cq_event(
      struct ibv_comp_channel* channel,
      struct ibv_cq** cq,
      void** cq_context) = 0;

  virtual void ibv_ack_cq_events(struct ibv_cq* cq, unsigned int nevents) = 0;

  virtual int ibv_req_notify_cq(struct ibv_cq* cq, int solicited_only) = 0;

  virtual int ibv_destroy_qp(struct ibv_qp* qp) = 0;

  virtual int ibv_destroy_cq(struct ibv_cq* cq) = 0;

  virtual int ibv_dereg_mr(struct ibv_mr* mr) = 0;

#ifndef IBVERBS_LACKS_DMAH
  virtual int ibv_dealloc_dmah(struct ibv_dmah* dmah) = 0;
#endif

  virtual int ibv_dealloc_pd(struct ibv_pd* pd) = 0;

  virtual int ibv_destroy_comp_channel(struct ibv_comp_channel* channel) = 0;

  virtual int ibv_close_device(struct ibv_context* context) = 0;

  virtual void ibv_free_device_list(struct ibv_device** list) = 0;

  virtual int ibv_get_async_event(
      struct ibv_context* context,
      struct ibv_async_event* event) = 0;

  virtual void ibv_ack_async_event(struct ibv_async_event* event) = 0;

  virtual const char* ibv_event_type_str(enum ibv_event_type event) = 0;
};

} // namespace facebook::neteng::ai
