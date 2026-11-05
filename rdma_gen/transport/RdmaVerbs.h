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
#include <cstdint>

#include "rdma_gen/transport/IRdmaVerbs.h"

namespace facebook::neteng::ai {

/**
 * RdmaVerbs is a wrapper class. Every function in this class
 * is a wrapper function around infiniband/verbs functions. These
 * wrapper functions will enable us to mock these functions and write
 * unit tests
 */
class RdmaVerbs : public IRdmaVerbs {
 public:
  ~RdmaVerbs() override = default;

  struct ibv_context* ibv_open_device(struct ibv_device* device) override {
    return ::ibv_open_device(device);
  }

  struct ibv_comp_channel* ibv_create_comp_channel(
      struct ibv_context* context) override {
    return ::ibv_create_comp_channel(context);
  }

  struct ibv_cq* ibv_create_cq(
      struct ibv_context* context,
      int cqe,
      void* cq_context,
      struct ibv_comp_channel* channel,
      int comp_vector) override {
    return ::ibv_create_cq(context, cqe, cq_context, channel, comp_vector);
  }

  struct ibv_qp* ibv_create_qp(
      struct ibv_pd* pd,
      struct ibv_qp_init_attr* qp_init_attr) override {
    return ::ibv_create_qp(pd, qp_init_attr);
  }

  struct ibv_qp* ibv_create_qp_ex(
      struct ibv_context* context,
      struct ibv_qp_init_attr_ex* qp_init_attr) override {
    return ::ibv_create_qp_ex(context, qp_init_attr);
  }

  struct ibv_pd* ibv_alloc_pd(struct ibv_context* context) override {
    return ::ibv_alloc_pd(context);
  }

  struct ibv_mr* ibv_reg_mr_internal(
      struct ibv_pd* pd,
      void* addr,
      size_t length,
      int access) override {
    return ::ibv_reg_mr(pd, addr, length, access);
  }

  struct ibv_mr* ibv_reg_mr_iova2(
      struct ibv_pd* pd,
      void* addr,
      size_t length,
      uint64_t iova,
      int access) override {
    return ::ibv_reg_mr_iova2(pd, addr, length, iova, access);
  }

  struct ibv_mr* ibv_reg_dmabuf_internal(
      struct ibv_pd* pd,
      uint64_t offset,
      size_t length,
      uint64_t iova,
      int fd,
      int access) override {
    return ::ibv_reg_dmabuf_mr(pd, offset, length, iova, fd, access);
  }

#ifndef IBVERBS_LACKS_DMAH
  struct ibv_mr* ibv_reg_mr_ex(
      struct ibv_pd* pd,
      struct ibv_mr_init_attr* mr_init_attr) override {
    return ::ibv_reg_mr_ex(pd, mr_init_attr);
  }

  struct ibv_dmah* ibv_alloc_dmah(
      struct ibv_context* context,
      struct ibv_dmah_init_attr* dmah_init_attr) override {
    return ::ibv_alloc_dmah(context, dmah_init_attr);
  }
#endif

  int ibv_query_device(
      struct ibv_context* context,
      struct ibv_device_attr* deviceAttr) override {
    return ::ibv_query_device(context, deviceAttr);
  }

  int ibv_query_port(
      struct ibv_context* context,
      uint8_t port_num,
      struct ibv_port_attr* port_attr) override {
    return ::ibv_query_port(context, port_num, port_attr);
  }

  int ibv_query_qp(
      struct ibv_qp* qp,
      struct ibv_qp_attr* attr,
      int attr_mask,
      struct ibv_qp_init_attr* init_attr) override {
    return ::ibv_query_qp(qp, attr, attr_mask, init_attr);
  }

  int ibv_modify_qp(struct ibv_qp* qp, struct ibv_qp_attr* attr, int attr_mask)
      override {
    return ::ibv_modify_qp(qp, attr, attr_mask);
  }

  int ibv_modify_qp_rate_limit(
      struct ibv_qp* qp,
      struct ibv_qp_rate_limit_attr* attr) override {
    return ::ibv_modify_qp_rate_limit(qp, attr);
  }

  int ibv_query_gid(
      struct ibv_context* context,
      uint8_t port_num,
      int index,
      union ibv_gid* gid) override {
    return ::ibv_query_gid(context, port_num, index, gid);
  }

  int ibv_query_gid_ex(
      struct ibv_context* context,
      uint8_t portNum,
      int index,
      struct ibv_gid_entry* entry,
      uint32_t flags) override {
    return ::ibv_query_gid_ex(context, portNum, index, entry, flags);
  }

  int ibv_post_send(
      struct ibv_qp* qp,
      struct ibv_send_wr* wr,
      struct ibv_send_wr** bad_wr) override {
    return ::ibv_post_send(qp, wr, bad_wr);
  }

  int ibv_post_recv(
      struct ibv_qp* qp,
      struct ibv_recv_wr* wr,
      struct ibv_recv_wr** bad_wr) override {
    return ::ibv_post_recv(qp, wr, bad_wr);
  }

  const char* ibv_wc_status_str(enum ibv_wc_status status) override {
    return ::ibv_wc_status_str(status);
  }

  int ibv_poll_cq(struct ibv_cq* cq, int num_entries, struct ibv_wc* wc)
      override {
    return ::ibv_poll_cq(cq, num_entries, wc);
  }

  struct ibv_device** ibv_get_device_list(int* num_devices) override {
    return ::ibv_get_device_list(num_devices);
  }

  const char* ibv_get_device_name(struct ibv_device* device) override {
    return ::ibv_get_device_name(device);
  }

  virtual int ibv_get_device_index(struct ibv_device* device) override {
    return ::ibv_get_device_index(device);
  }

  int ibv_get_cq_event(
      struct ibv_comp_channel* channel,
      struct ibv_cq** cq,
      void** cq_context) override {
    return ::ibv_get_cq_event(channel, cq, cq_context);
  }

  void ibv_ack_cq_events(struct ibv_cq* cq, unsigned int nevents) override {
    return ::ibv_ack_cq_events(cq, nevents);
  }

  int ibv_req_notify_cq(struct ibv_cq* cq, int solicited_only) override {
    return ::ibv_req_notify_cq(cq, solicited_only);
  }

  int ibv_destroy_qp(struct ibv_qp* qp) override {
    return ::ibv_destroy_qp(qp);
  }

  int ibv_destroy_cq(struct ibv_cq* cq) override {
    return ::ibv_destroy_cq(cq);
  }

  int ibv_dereg_mr(struct ibv_mr* mr) override {
    return ::ibv_dereg_mr(mr);
  }

#ifndef IBVERBS_LACKS_DMAH
  int ibv_dealloc_dmah(struct ibv_dmah* dmah) override {
    return ::ibv_dealloc_dmah(dmah);
  }
#endif

  int ibv_dealloc_pd(struct ibv_pd* pd) override {
    return ::ibv_dealloc_pd(pd);
  }

  int ibv_destroy_comp_channel(struct ibv_comp_channel* channel) override {
    return ::ibv_destroy_comp_channel(channel);
  }

  int ibv_close_device(struct ibv_context* context) override {
    return ::ibv_close_device(context);
  }

  void ibv_free_device_list(struct ibv_device** list) override {
    ::ibv_free_device_list(list);
  }

  int ibv_get_async_event(
      struct ibv_context* context,
      struct ibv_async_event* event) override {
    return ::ibv_get_async_event(context, event);
  }

  void ibv_ack_async_event(struct ibv_async_event* event) override {
    ::ibv_ack_async_event(event);
  }

  const char* ibv_event_type_str(enum ibv_event_type event) override {
    return ::ibv_event_type_str(event);
  }
};

} // namespace facebook::neteng::ai
