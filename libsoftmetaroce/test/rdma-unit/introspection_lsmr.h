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

// rdma-unit-test introspection for the libsoftmetaroce (lsmr) device: the
// MetaRoCE capability profile lsmr emulates plus its deviation map, keyed on
// the "lsmr" device prefix. See DESIGN.md.

#pragma once

#include <string>

#include "absl/container/flat_hash_map.h"
#include "infiniband/verbs.h"
#include "internal/introspection_registrar.h"
#include "public/introspection.h"

namespace rdma_unit_test {

class IntrospectionLsmr : public NicIntrospection {
 public:
  static void Register() {
    IntrospectionRegistrar::GetInstance().Register(
        "lsmr", [](const std::string& name, const ibv_device_attr& attr) {
          return new IntrospectionLsmr(name, attr);
        });
  }

  bool SupportsRcQp() const override {
    return true;
  }

  // A work request whose completion the depth refuses still finishes, so the
  // queue pair keeps working.
  bool FullCqIdlesQp() const override {
    return false;
  }

  bool IsSlowNic() const override {
    return true;
  }

  bool SupportsUdQp() const override {
    return false;
  }

  bool SupportsExtendedCqs() const override {
    return false;
  }

  bool SupportsType1() const override {
    return false;
  }

  bool SupportsType2() const override {
    return false;
  }

 protected:
  const absl::flat_hash_map<TestcaseKey, std::string>& GetDeviations()
      const override {
    // MetaRoCE-protocol-level gaps (apply to any MetaRoCE impl).
    // Hardware-specific deviations are omitted -- e.g. lsmr validates
    // atomic operand length.
    static const absl::flat_hash_map<TestcaseKey, std::string> deviations{
        {{"SrqTest", "QpFlush"}, "Provider does not support UD."},
        {{"SrqTest", "OverflowSrq"}, "Provider does not support SRQ."},
        {{"SrqTest", "ModifyMaxWr"}, "Provider does not support SRQ resizing."},

        // UD is not supported.
        {{"LoopbackUdQpTest", "Send"}, "Provider does not support UD."},
        {{"LoopbackUdQpTest", "SendLargerThanMtu"},
         "Provider does not support UD."},
        {{"LoopbackUdQpTest", "SendRnr"}, "Provider does not support UD."},
        {{"LoopbackUdQpTest", "SendTrafficClass"},
         "Provider does not support UD."},
        {{"LoopbackUdQpTest", "SendWithTooSmallRecv"},
         "Provider does not support UD."},
        {{"LoopbackUdQpTest", "SendInvalidAh"},
         "Provider does not support UD."},
        {{"LoopbackUdQpTest", "SendInvalidQpn"},
         "Provider does not support UD."},
        {{"LoopbackUdQpTest", "SendInvalidQKey"},
         "Provider does not support UD."},
        {{"LoopbackUdQpTest", "Read"}, "Provider does not support UD."},
        {{"LoopbackUdQpTest", "PollMultipleCqe"},
         "Provider does not support UD."},
        {{"LoopbackUdQpTest", "Write"}, "Provider does not support UD."},
        {{"LoopbackUdQpTest", "FetchAdd"}, "Provider does not support UD."},
        {{"LoopbackUdQpTest", "CompareSwap"}, "Provider does not support UD."},
        {{"LoopbackUdQpTest", "MultiThreadedAhLifecycle"},
         "Provider does not support UD."},
        {{"AdvancedLoopbackTest", "RcSendToUd"},
         "Provider does not support UD."},
        {{"AdvancedLoopbackTest", "UdSendToRc"},
         "Provider does not support UD."},
        {{"UdConstantOpTest", "OneToOne"}, "Provider does not support UD."},
        {{"UdMixedOpTest", "OneToOne"}, "Provider does not support UD."},
        {{"MultiplexedUdTest", "FixedOpSize"}, "Provider does not support UD."},
        {{"UdMultiplexedMixedOpsTest", "MultiplexedMixedOpsShared"},
         "Provider does not support UD."},
        {{"UdBurstTest", "TrafficOkAfterBurst"},
         "Provider does not support UD."},
        {{"QpTest", "CreateUd"}, "Provider does not support UD."},
        {{"QpTest", "UdModify"}, "Provider does not support UD."},
        {{"QpTest", "ModifyBadResetToInitStateTransition"},
         "Provider defaults the attributes a modify omits."},
        {{"QpTest", "ModifyBadInitToRtrStateTransition"},
         "Provider defaults the attributes a modify omits, other than the "
         "address vector."},
        {{"PdUdLoopbackTest", "SendAhOnOtherPd"},
         "Provider does not support UD."},

        // AH is not supported.
        {{"AhTest", "CreateAndDestroy"}, "Provider does not support AH."},
        {{"AhTest", "DestroyWithInvalidHandle"},
         "Provider does not support AH."},
        {{"PdTest", "AhRefCount"}, "Provider does not support AH."},
        {{"ThreadedTest", "CreateDestroyAh"}, "Provider does not support AH."},
        {{"ThreadedTest", "CreateAh"}, "Provider does not support AH."},
        {{"DeviceLimitTest", "MaxAh"}, "Provider does not support AH."},

        // Memory windows are not supported.
        {{"PdBindTest", "MwOnOtherPd"}, "Provider does not support MW."},
        {{"PdBindTest", "MrOnOtherPd"}, "Provider does not support MW."},
        {{"PdBindTest", "MrMwOnOtherPd"}, "Provider does not support MW."},
        {{"PdTest", "AllocMwWithInvalidPd"}, "Provider does not support MW."},
        {{"PdTest", "MwRefCount"}, "Provider does not support MW."},
        {{"DeviceLimitTest", "MaxMw"}, "Provider does not support MW."},
        {{"BufferMwTest", "BindExceedFront"}, "Provider does not support MW."},
        {{"BufferMwTest", "BindExceedRear"}, "Provider does not support MW."},
        {{"BufferMwTest", "ReadExceedFront"}, "Provider does not support MW."},
        {{"BufferMwTest", "ReadExceedRear"}, "Provider does not support MW."},
        {{"BufferMwTest", "ReadZeroByte"}, "Provider does not support MW."},
        {{"BufferMwTest", "ReadZeroByteOutsideMw"},
         "Provider does not support MW."},
        {{"BufferMwTest", "WriteExceedFront"}, "Provider does not support MW."},
        {{"BufferMwTest", "WriteExceedRear"}, "Provider does not support MW."},
        {{"BufferMwTest", "WriteZeroByte"}, "Provider does not support MW."},
        {{"BufferMwTest", "WriteZeroByteOutsideMw"},
         "Provider does not support MW."},
        {{"MwGeneralTest", "Alloc"}, "Provider does not support MW."},
        {{"MwGeneralTest", "DeallocInvalidMw"},
         "Provider does not support MW."},
        {{"MwGeneralTest", "Bind"}, "Provider does not support MW."},
        {{"MwGeneralTest", "Read"}, "Provider does not support MW."},
        {{"MwGeneralTest", "ReadZeroBased"}, "Provider does not support MW."},
        {{"MwGeneralTest", "BindReadDiffQp"}, "Provider does not support MW."},
        {{"MwGeneralTest", "BindInvalidMr"}, "Provider does not support MW."},
        {{"MwGeneralTest", "BindInvalidMw"}, "Provider does not support MW."},
        {{"MwGeneralTest", "BindInvalidQp"}, "Provider does not support MW."},
        {{"MwGeneralTest", "DeregMrWhenBound"},
         "Provider does not support MW."},
        {{"MwType1Test", "BindZeroLengthMw"}, "Provider does not support MW."},
        {{"MwType1Test", "UnsignaledBindError"},
         "Provider does not support MW."},
        {{"MwType1Test", "DestroyQpWithType1Bound"},
         "Provider does not support MW."},
        {{"MwType1Test", "CrossQpManagement"}, "Provider does not support MW."},
        {{"MwType1Test", "DeallocPdWithOutstandingMw"},
         "Provider does not support MW."},
        {{"MwType1Test", "BindType1WhenQpError"},
         "Provider does not support MW."},
        {{"MwType2Test", "Bind"}, "Provider does not support MW."},
        {{"MwType2Test", "UnsignaledBind"}, "Provider does not support MW."},
        {{"MwType2Test", "BindZeroLengthMw"}, "Provider does not support MW."},
        {{"MwType2Test", "BindRKeyReuse"}, "Provider does not support MW."},
        {{"MwType2Test", "DistinctRKeys"}, "Provider does not support MW."},
        {{"MwType2Test", "UnsignaledBindError"},
         "Provider does not support MW."},
        {{"MwType2Test", "LocalInvalidate"}, "Provider does not support MW."},
        {{"MwType2Test", "UnsignaledLocalInvalidate"},
         "Provider does not support MW."},
        {{"MwType2Test", "UnsignaledLocalInvalidateError"},
         "Provider does not support MW."},
        {{"MwType2Test", "Type1BindOnType2"}, "Provider does not support MW."},
        {{"MwType2Test", "DestroyQpWithType2Bound"},
         "Provider does not support MW."},
        {{"MwType2Test", "DeregMrWhenBound"}, "Provider does not support MW."},
        {{"MwType2Test", "RebindToDifferentQp"},
         "Provider does not support MW."},
        {{"MwType2Test", "RebindToDifferentRKey"},
         "Provider does not support MW."},
        {{"MwType2Test", "CrossQpInvalidate"}, "Provider does not support MW."},
        {{"MwType2Test", "CrossQpRead"}, "Provider does not support MW."},
        {{"MwType2Test", "BindType1WhenQpError"},
         "Provider does not support MW."},
        {{"MwType2AdvancedTest", "OnlyReads"}, "Provider does not support MW."},
        {{"MwType2AdvancedTest", "Rebind"}, "Provider does not support MW."},
        {{"MwType2AdvancedTest", "Invalidate"},
         "Provider does not support MW."},
        {{"MwType2AdvancedTest", "Dealloc"}, "Provider does not support MW."},
        {{"MwBindAccessTest", "MwBindAccessTests"},
         "Provider does not support MW."},
        {{"PdType1MwTest", "ReadMwOtherPd"}, "Provider does not support MW."},
        {{"PdType1MwTest", "WriteMwOtherPd"}, "Provider does not support MW."},
        {{"PdType1MwTest", "FetchAddMwOtherPd"},
         "Provider does not support MW."},
        {{"PdType1MwTest", "CompSwapMwOtherPd"},
         "Provider does not support MW."},
        {{"ThreadedTest", "AllocDeallocMw"}, "Provider does not support MW."},
        {{"ThreadedTest", "AllocMw"}, "Provider does not support MW."},
        {{"LoopbackRcQpTest", "Type1MWRead"}, "Provider does not support MW."},
        {{"LoopbackRcQpTest", "Type2MWRead"}, "Provider does not support MW."},
        {{"LoopbackRcQpTest", "Type1MWUnbind"},
         "Provider does not support MW."},
        {{"LoopbackRcQpTest", "Type1MWWrite"}, "Provider does not support MW."},
        {{"LoopbackRcQpTest", "Type2MWWrite"}, "Provider does not support MW."},
        {{"LoopbackRcQpTest", "SendWithInvalidateType1Rkey"},
         "Provider does not support MW."},
        {{"CompChannelTest", "Bind"}, "Provider does not support MW."},

        // Solicited completion notifications are not supported.
        {{"CompChannelTest", "RecvSolicitedNofityAny"},
         "Provider does not support solicited completion notifications."},
        {{"CompChannelTest", "RecvSolicitedNofitySolicited"},
         "Provider does not support solicited completion notifications."},
        {{"CompChannelTest", "RecvUnsolicitedNofitySolicited"},
         "Provider does not support solicited completion notifications."},

        // Only a single SGE per WR is supported.
        {{"HugePageTest", "SendMultipleSge"},
         "Provider does not support multiple SGEs."},

        // Posting to a QP outside RTS is rejected rather than queued.
        {{"QpStateTest", "PostSendReset"},
         "lsmr does not allow posting to SQ in Reset state."},
        {{"QpStateTest", "PostRecvReset"},
         "lsmr does not allow posting to RQ in Reset state."},
        {{"QpStateTest", "PostSendInit"},
         "lsmr does not allow posting to SQ in Init state."},
        {{"QpStateTest", "PostSendRtr"},
         "lsmr does not allow posting to SQ in Rtr state."},

        // A receive overflow is indistinguishable from wire alignment padding
        // when the payload and the receive length fall within the same
        // four-byte alignment bucket, so lsmr cannot emit the length error.
        {{"LoopbackRcQpTest", "SendWithTooSmallRecv"},
         "lsmr cannot distinguish a receive overflow from padding when the "
         "payload and the receive length share a four-byte alignment bucket."},

        {{"BufferMrTest", "WriteZeroByte"},
         "MetaRoCE forbids a zero-byte RDMA Write; lsmr refuses it at post."},
        {{"BufferMrTest", "WriteZeroByteOutsideMr"},
         "MetaRoCE forbids a zero-byte RDMA Write; lsmr refuses it at post."},
        {{"BufferMrTest", "WriteZeroByteToZeroByteMr"},
         "MetaRoCE forbids a zero-byte RDMA Write; lsmr refuses it at post."},
        {{"BufferMrTest", "WriteZeroByteOutsideZeroByteMr"},
         "MetaRoCE forbids a zero-byte RDMA Write; lsmr refuses it at post."},
        {{"BufferMrTest", "WriteZeroByteInvalidRKey"},
         "MetaRoCE forbids a zero-byte RDMA Write; lsmr refuses it at post."},

        {{"DeviceLimitTest", "MaxQp"},
         "Creating max_qp emulated queue pairs takes minutes."},
        {{"DeviceLimitTest", "MaxCqMixed"},
         "Creating max_cq emulated completion queues takes minutes."},
        {{"QpStateTest", "QpIdRollover"},
         "Creating and destroying max_qp emulated queue pairs takes minutes."},
    };
    return deviations;
  }

 private:
  IntrospectionLsmr() = delete;
  ~IntrospectionLsmr() override = default;
  IntrospectionLsmr(const std::string& name, const ibv_device_attr& attr)
      : NicIntrospection(name, attr) {}
};

} // namespace rdma_unit_test
