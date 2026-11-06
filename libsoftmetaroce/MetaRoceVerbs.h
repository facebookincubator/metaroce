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

#include <folly/Synchronized.h>
#include <folly/Utility.h>
#include <folly/container/F14Set.h>
#include <folly/io/async/AsyncSocketException.h>
#include <cerrno>
#ifdef RDMA_GEN_OSS
#include "rdma_gen/transport/RdmaVerbs.h"
#else
#include "neteng/ai/common/RdmaVerbs.h"
#endif
#include <libsoftmetaroce/ConnectionObserver.h>
#include <libsoftmetaroce/EgressProfile.h>
#include <libsoftmetaroce/MemoryRegions.h>
#include <libsoftmetaroce/NextFreeId.h>
#include <libsoftmetaroce/PingController.h>
#include <libsoftmetaroce/Qp.h>
#include <libsoftmetaroce/QpArbiter.h>
#include <libsoftmetaroce/RcnRateHint.h>
#include <libsoftmetaroce/Receiver.h>
#include <libsoftmetaroce/Sender.h>
#include <libsoftmetaroce/SharedReceiveQueue.h>
#include <libsoftmetaroce/congestion_control/CongestionControllerFactory.h>
#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

DECLARE_bool(libsoftmetaroce_ignore_modify_psn);
DECLARE_int32(libsoftmetaroce_bind_to_port);
DECLARE_int32(libsoftmetaroce_send_to_port);

namespace libsoftmetaroce {

// Forward declaration so that we do not need to explicitly include
// CompletionQueue.h, which is already included by Qp.h.
class CompletionQueue;

struct DiscoveryTables;

inline constexpr uint32_t kMetaRoceInitialQpId = 2;

using facebook::neteng::ai::RdmaVerbs;

using SocketWithModifier = std::
    pair<std::unique_ptr<folly::AsyncUDPSocket>, std::unique_ptr<Modifier>>;

class MetaRoceVerbs : public RdmaVerbs, public folly::NonCopyableNonMovable {
 public:
  /*
   * Fetches a list of all the interfaces and assigns the gid, if unassigned, to
   * the ipv6 address of the first interface that is not loopback and has
   * sin6_scope_id == 0. Note that this approach doesn't work when MetaRoCE is
   * expected to use multiple interfaces.
   */
  void getLocalAddrAndDevicesList();

  static const std::string readModifierConfigFile(
      const std::string& modifierConfigFilePath);

  // Returns a socketMaker that shares one appliedCount across per-path
  // Modifiers.
  static std::function<SocketWithModifier(folly::EventBase*)>
  makeSocketMakerFromConfigFile(const std::string& modifierConfigFilePath);

  explicit MetaRoceVerbs(
      const std::function<SocketWithModifier(folly::EventBase*)>& socketMaker);

  explicit MetaRoceVerbs(const std::string& modifierConfigFilePath);
  explicit MetaRoceVerbs();

  ~MetaRoceVerbs() override {
    receivers_.clear();
    alternateReceivers_.clear();
    for (ibv_device* device : devices_) {
      delete (device);
    }
  }

  struct ibv_context* FOLLY_NULLABLE
  ibv_open_device(struct ibv_device* device) override;
  int ibv_close_device(struct ibv_context* context) override;
  // Answers the oldest event raised against this context, or -1 with `EAGAIN`
  // where none waits.
  int ibv_get_async_event(
      struct ibv_context* context,
      struct ibv_async_event* event) override;
  void ibv_ack_async_event(struct ibv_async_event* event) override;
  int ibv_query_device(struct ibv_context*, struct ibv_device_attr* device_attr)
      override;
  int ibv_query_port(
      struct ibv_context* context,
      uint8_t port_num,
      struct ibv_port_attr* port_attr) override;
  struct ibv_device** ibv_get_device_list(int* num_devices) override;
  void ibv_free_device_list(struct ibv_device** list) override;
  int ibv_query_gid(
      struct ibv_context* context,
      uint8_t port_num,
      int index,
      union ibv_gid* gid) override;
  int ibv_query_gid_ex(
      struct ibv_context* context,
      uint8_t portNum,
      int index,
      struct ibv_gid_entry* entry,
      uint32_t flags) override;
  struct ibv_pd* FOLLY_NULLABLE
  ibv_alloc_pd(struct ibv_context* context) override;
  struct ibv_comp_channel* ibv_create_comp_channel(
      struct ibv_context* context) override;
  struct ibv_cq* FOLLY_NULLABLE ibv_create_cq(
      struct ibv_context* context,
      int sizeOfQueue,
      void* cq_context,
      struct ibv_comp_channel* channel,
      int comp_vector) override;
  struct ibv_qp* ibv_create_qp(
      struct ibv_pd* pd,
      struct ibv_qp_init_attr* qp_init_attr) override;
  struct ibv_qp* ibv_create_qp_ex(
      struct ibv_context* context,
      struct ibv_qp_init_attr_ex* qp_init_attr) override;
  // The shared receive queue verbs. `RdmaVerbs` declares none of them, since
  // no other client of that interface asks for a shared receive queue.
  struct ibv_srq* FOLLY_NULLABLE
  ibv_create_srq(struct ibv_pd* pd, struct ibv_srq_init_attr* srq_init_attr);
  int ibv_modify_srq(
      struct ibv_srq* srq,
      struct ibv_srq_attr* srq_attr,
      int srq_attr_mask);
  int ibv_query_srq(struct ibv_srq* srq, struct ibv_srq_attr* srq_attr);
  int ibv_destroy_srq(struct ibv_srq* srq);
  int ibv_post_srq_recv(
      struct ibv_srq* srq,
      struct ibv_recv_wr* recv_wr,
      struct ibv_recv_wr** bad_recv_wr);
  struct ibv_mr* ibv_reg_mr_internal(
      struct ibv_pd* pd,
      void* addr,
      size_t length,
      int access) override;
  int ibv_modify_qp(struct ibv_qp* qp, struct ibv_qp_attr* attr, int attr_mask)
      override;
  int ibv_modify_qp_rate_limit(
      struct ibv_qp* qp,
      struct ibv_qp_rate_limit_attr* attr) override;
  // The lowest rate limit installed; a nonzero request below it gets it.
  [[nodiscard]] static uint64_t minRateLimitBytesPerSec();
  int ibv_query_qp(
      struct ibv_qp* qp,
      struct ibv_qp_attr* attr,
      int attr_mask,
      struct ibv_qp_init_attr* init_attr) override;
  int ibv_post_send(
      struct ibv_qp* qp,
      struct ibv_send_wr* wr,
      struct ibv_send_wr** bad_wr) override;
  int ibv_post_recv(
      struct ibv_qp* qp,
      struct ibv_recv_wr* wr,
      struct ibv_recv_wr** bad_wr) override;
  int ibv_poll_cq(struct ibv_cq* cq, int num_entries, struct ibv_wc* wc)
      override;
  int ibv_destroy_qp(struct ibv_qp* qp) override;
  int ibv_destroy_cq(struct ibv_cq* cq) override;
  int ibv_dereg_mr(struct ibv_mr* mr) override;
  int ibv_dealloc_pd(struct ibv_pd* pd) override;
  int ibv_destroy_comp_channel(struct ibv_comp_channel* channel) override;
  int ibv_req_notify_cq(struct ibv_cq* cq, int solicited_only) override;
  // Blocks unless channel->fd has O_NONBLOCK. A poll/epoll client should set
  // O_NONBLOCK and handle EAGAIN: the fd can be readable with no event left.
  int ibv_get_cq_event(
      struct ibv_comp_channel* channel,
      struct ibv_cq** cq,
      void** cq_context) override;
  void ibv_ack_cq_events(struct ibv_cq* cq, unsigned int nevents) override;

  // The caller keeps the observer alive for the rest of this instance's life.
  void registerConnectionObserver(ConnectionObserver& observer);

#ifndef IBVERBS_LACKS_DMAH
  struct ibv_dmah* ibv_alloc_dmah(
      struct ibv_context* /* context */,
      struct ibv_dmah_init_attr* /* dmah_init_attr */) override {
    return nullptr;
  }

  struct ibv_mr* ibv_reg_mr_ex(
      struct ibv_pd* /* pd */,
      struct ibv_mr_init_attr* /* mr_init_attr */) override {
    return nullptr;
  }

  int ibv_dealloc_dmah(struct ibv_dmah* /* dmah */) override {
    return 0;
  }
#endif

  [[nodiscard]] uint16_t getActivePortSet() const {
    return activePortSet_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] size_t getPortCount() const {
    return portInterfaces_.size();
  }

  [[nodiscard]] Qp* getQpByNum(uint32_t qpNum) {
    auto locked = qpNumToQp_.rlock();
    if (auto it = locked->find(qpNum); it != locked->end()) {
      return it->second.get();
    }
    return nullptr;
  }

  // cq is a non-owning alias; handleToCq_ owns the CompletionQueue.
  struct AllocatedCq {
    uint32_t handle;
    CompletionQueue* cq;
  };
  [[nodiscard]] std::optional<AllocatedCq> allocateCompletionQueue(
      uint32_t depth,
      std::function<void()> reportOverflow = {},
      std::function<void()> raiseCompletionEvent = {}) {
    auto locked = handleToCq_.wlock();
    if (locked->size() >= kMaxCompletionQueues) {
      return std::nullopt;
    }
    const std::optional<uint32_t> maybeHandle = nextFreeId(
        nextCqHandleToAssign_,
        kFirstCompletionQueueHandle,
        std::numeric_limits<uint32_t>::max(),
        [&](uint32_t handle) { return locked->contains(handle); });
    static_assert(
        kMaxCompletionQueues <= std::numeric_limits<uint32_t>::max() -
                kFirstCompletionQueueHandle + 1,
        "the handle range must hold kMaxCompletionQueues completion queues");
    CHECK(maybeHandle.has_value())
        << "the count checked above leaves handles free, so nextFreeId finds "
           "one";
    auto [it, inserted] = locked->insert(
        {*maybeHandle,
         std::make_unique<CompletionQueue>(
             depth,
             std::move(reportOverflow),
             std::move(raiseCompletionEvent))});
    CHECK(inserted)
        << "nextFreeId skips a handle handleToCq_ already holds, under the "
           "write lock this insert takes";
    return AllocatedCq{*maybeHandle, it->second.get()};
  }
  // Extract folly::IPAddress from the binary GID in an ibv_gid_entry.
  static folly::IPAddress gidEntryToIPAddress(const ibv_gid_entry& entry);

  [[nodiscard]] Receiver* getReceiver(
      folly::IPAddress receiverAddr,
      const ibv_gid_entry& entry = ibv_gid_entry{}) {
    std::lock_guard<std::mutex> lock(receiversMutex_);
    // Check if any existing receiver already handles this address.
    for (auto& [_, receiver] : receivers_) {
      if (receiver->hasLocalAddress(receiverAddr)) {
        return receiver.get();
      }
    }

    // If we have a primary receiver, add this address to it.
    // This ensures all GIDs share the same thread for thread-safe
    // processing of packets that may belong to the same QP across flows.
    if (!receivers_.empty()) {
      Receiver& primaryReceiver =
          *CHECK_NOTNULL(receivers_.begin()->second.get());
      LOG(INFO) << "Adding receiver address " << receiverAddr
                << " to existing receiver";
      primaryReceiver.addLocalAddress(
          receiverAddr, socketMaker_, metaRoceBindToPort_, entry);
    } else {
      LOG(INFO) << "Creating receiver on " << receiverAddr;
      receivers_[receiverAddr] = std::make_unique<Receiver>(
          receiverAddr,
          qpNumToQp_,
          socketMaker_,
          metaRoceBindToPort_,
          metaRoceSendToPort_,
          *pingController_,
          entry,
          &activePortSet_);
    }

    if (defaultEgressRegistry()
            .getProfile(entry.gid_type)
            .supportsAlternateReceiver) {
      // The primary listener carries the traffic, so the receiver stands
      // without this second one. Only a port another process holds is
      // tolerable here; any other failure is the caller's to see.
      try {
        if (!alternateReceivers_.empty()) {
          Receiver& alternateReceiver =
              *CHECK_NOTNULL(alternateReceivers_.begin()->second.get());
          alternateReceiver.addLocalAddress(
              receiverAddr, socketMaker_, kMetaRoceBindToPortAlternate, entry);
        } else {
          auto alternateReceiver = std::make_unique<Receiver>(
              receiverAddr,
              qpNumToQp_,
              socketMaker_,
              kMetaRoceBindToPortAlternate,
              metaRoceSendToPort_,
              *pingController_,
              entry,
              &activePortSet_);
          alternateReceivers_.emplace(
              receiverAddr, std::move(alternateReceiver));
        }
      } catch (const folly::AsyncSocketException& e) {
        if (e.getErrno() != EADDRINUSE) {
          throw;
        }
        LOG(WARNING) << "Not listening on alternate port "
                     << kMetaRoceBindToPortAlternate << " for " << receiverAddr
                     << ": " << e.what();
      }
    }
    return receivers_.begin()->second.get();
  }
  int sendPing(
      uint8_t sgidIndex,
      folly::IPAddress remoteAddr,
      uint32_t sourceQpNum,
      uint32_t remoteQpNum,
      uint8_t tagNum,
      uint16_t flowId,
      uint16_t supportedFlowCount,
      int timeout_ms = 1000);

  // Manipulate Tag and QP numbers.
  static constexpr uint8_t qpAndTagToTag(uint32_t qpNum) {
    return static_cast<uint8_t>((qpNum & kQpTagMask) >> kQpTagShift);
  }
  static constexpr uint32_t qpAndTagToQp(uint32_t qpNum) {
    return qpNum & kQpIdMask;
  }
  static constexpr uint32_t combineQpAndTag(uint32_t qpNum, uint8_t tagNum) {
    return (static_cast<uint32_t>(tagNum) << kQpTagShift) | qpNum;
  }

 private:
  // What one context holds for ibv_get_async_event. The id tells the context
  // open under this address now from one closed at it earlier.
  struct AsyncEventQueue {
    uint64_t contextId;
    std::deque<struct ibv_async_event> events;
  };

  // Queues an event for ibv_get_cq_event on `cq`'s channel and wakes a poll of
  // the channel's fd.
  void raiseCompletionEvent(struct ibv_cq* cq);
  // Queues an event for ibv_get_async_event and wakes a poll of async_fd.
  // Dropped unless `context` is open and its id still matches `contextId`.
  // True where the event is queued for a client to take.
  bool raiseAsyncEvent(
      struct ibv_context* context,
      uint64_t contextId,
      struct ibv_async_event event);
  // Drops every event `matches` selects, and the wakeup byte each of them
  // wrote, so a client polling afterward is handed neither.
  // Each returns how many it dropped, which a caller that waits gives back.
  [[nodiscard]] size_t purgeAsyncEvents(
      const std::function<bool(const struct ibv_async_event&)>& matches);
  [[nodiscard]] size_t purgeAsyncEventsForCq(const struct ibv_cq* cq);
  [[nodiscard]] size_t purgeAsyncEventsForSrq(const struct ibv_srq* srq);
  // The id of the queue for `context`, or zero for one this object did
  // not open. Found by lookup: a caller's context may not be dereferenced.
  [[nodiscard]] uint64_t asyncEventIdOf(struct ibv_context* context) const;
  // The queue the wrapper behind `cq` holds, without a map lookup. Valid for
  // as long as `cq` is, which `ibv_destroy_cq` frees last.
  [[nodiscard]] static CompletionQueue& completionQueueOf(struct ibv_cq& cq);
  // The queue the wrapper behind `srq` holds, without a map lookup. Valid for
  // as long as `srq` is, which `ibv_destroy_srq` frees last.
  [[nodiscard]] static SharedReceiveQueue& sharedReceiveQueueOf(
      struct ibv_srq& srq);
  // True while a memory region, queue pair or shared receive queue created
  // against `pd` is still alive.
  [[nodiscard]] bool isProtectionDomainInUse(const struct ibv_pd* pd) const;

  using SrqsByHandle =
      folly::F14FastMap<uint32_t, std::unique_ptr<SharedReceiveQueue>>;
  // The caller's lock on `srqs` is what keeps `ibv_destroy_srq` from freeing
  // the queue it answers with.
  [[nodiscard]] static SharedReceiveQueue* FOLLY_NULLABLE
  sharedReceiveQueueIn(const SrqsByHandle& srqs, const struct ibv_srq& srq);
  // The same, taking the reference under the lock the lookup holds, so an
  // `ibv_destroy_srq` between the two cannot free what is returned.
  [[nodiscard]] SharedReceiveQueue* FOLLY_NULLABLE
  referenceSharedReceiveQueue(struct ibv_srq& srq);

  // The same lookup with a reference taken under its lock, which the caller
  // gives back; an `ibv_destroy_cq` between the two cannot free it.
  [[nodiscard]] CompletionQueue* FOLLY_NULLABLE
  referenceCompletionQueue(struct ibv_cq& cq);

  // Sends the RNR-Cancel that lets a tag refused at `postedCsn` retry. False
  // where no tag is left for it to reach.
  [[nodiscard]] bool cancelRnrForRefusedTag(
      SharedReceiveTaker refused,
      uint16_t postedCsn);

  // Posts one work request of a chain: 0, or the errno that refuses it.
  int postSendWr(struct ibv_qp& ibvqp, Qp& qp, const struct ibv_send_wr& wr);
  int postRecvWr(struct ibv_qp& ibvqp, Qp& qp, const struct ibv_recv_wr& wr);

  // Drain receives, run ~Qp, then free the QP's Tags' ibv_qp handles.
  void tearDownQp(std::unique_ptr<Qp> qp);

  void publishTagConfig(const AppliedTagConfig& config);
  void publishQpRetirement(uint32_t localQpNum);

  // Injected dependencies.
  std::function<SocketWithModifier(folly::EventBase*)> socketMaker_;
  std::vector<ConnectionObserver*> connectionObservers_;

  // Configuration.
  const uint16_t metaRoceSendToPort_{};
  const uint16_t metaRoceBindToPort_{};

  // Runtime state.
  std::vector<ibv_device*> devices_; // owned; freed in the destructor
  std::vector<ibv_gid_entry> gid_table_;
  uint32_t nextQpNumToAssign_{kMetaRoceInitialQpId};
  uint32_t nextCqHandleToAssign_{kFirstCompletionQueueHandle};
  uint32_t nextSrqHandleToAssign_{kFirstSharedReceiveQueueHandle};

  // Owned subsystems and connection state; teardown depends on declaration
  // order (see arbiter_), so do not reorder.
  HighResTimer timer_{"lsmrRetx"};
  // QpArbiter or ConcurrentQpArbiter, chosen by
  // FLAGS_libsoftmetaroce_sending_threads. ~Qp calls arbiter_->removeQp(), so
  // the arbiter must outlive the QPs.
  std::unique_ptr<QpArbiterIface> arbiter_;
  // Events waiting for ibv_get_async_event. Only a context ibv_open_device
  // returned has an entry here, so a miss is one this object did not open.
  folly::Synchronized<folly::F14FastMap<struct ibv_context*, AsyncEventQueue>>
      contextEvents_;
  // Completion events waiting for ibv_get_cq_event, oldest first, and how many
  // completion queues report to the channel. Only a channel
  // ibv_create_comp_channel returned has an entry here.
  struct CompChannelState {
    std::deque<struct ibv_cq*> events;
    size_t completionQueues{0};
  };
  folly::Synchronized<
      folly::F14FastMap<struct ibv_comp_channel*, CompChannelState>>
      compChannels_;
  std::atomic<uint64_t> nextContextId_{1};
  // A create holds this read lock from the membership test through registering
  // its object, so a deallocate cannot free the domain between the two.
  folly::Synchronized<folly::F14FastSet<const struct ibv_pd*>>
      protectionDomains_;
  // ~Qp flushes the work a destroy left outstanding, which stages a completion
  // on the queue the Tag holds, so the queues must outlive the QPs.
  folly::Synchronized<
      folly::F14FastMap<uint32_t, std::unique_ptr<CompletionQueue>>>
      handleToCq_;
  folly::Synchronized<SrqsByHandle> handleToSrq_;
  // ~Qp deactivates through this hint.
  std::unique_ptr<RcnRateHint> rcnRateHint_;
  // Synchronized is required because this is accessed by the main thread and
  // sender thread.
  folly::Synchronized<folly::F14FastMap<uint32_t, std::unique_ptr<Qp>>>
      qpNumToQp_;
  MemoryRegions memoryRegions_;
  // one sender has many sockets each with one address
  std::unique_ptr<Sender> sender_;
  // dedicated controller for sending/receiving pings
  std::unique_ptr<PingController> pingController_;
  // to receive, place a socket on both the metaroce port
  // (receivers_) and the alternate debug port (alternateReceivers_) that
  // allows receipt on hardware that would collect metaroce port traffic.
  std::unordered_map<folly::IPAddress, std::unique_ptr<Receiver>> receivers_;
  std::unordered_map<folly::IPAddress, std::unique_ptr<Receiver>>
      alternateReceivers_;
  std::mutex receiversMutex_;
  CongestionControllerFactory ccFactory_;

  bool tryDiscoverMultiport(
      const std::vector<std::string>& requestedGidOrder,
      DiscoveryTables& tables);

  // Multiport state. When macvlan children are detected on the interface
  // owning the --libsoftmetaroce_gid address, portInterfaces_ stores the
  // child interface names (sorted) and activePortSet_ tracks which ports
  // are operationally up. Atomic because Receiver and PingController threads
  // read it concurrently via pointer while the main thread may update it
  // when local port state changes.
  std::vector<std::string> portInterfaces_;
  static_assert(kMaxPorts <= 16, "activePortSet_ is uint16_t");
  std::atomic<uint16_t> activePortSet_{0};
};

} // namespace libsoftmetaroce
