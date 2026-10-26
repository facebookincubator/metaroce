# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# rdma-unit-test conformance suite driven against lsmr, for the standalone
# libsoftmetaroce build. The test filter runs as one CTest entry.

include(FetchContent)

# The patch step and check_green_names.py both need the interpreter.
find_package(Python3 REQUIRED COMPONENTS Interpreter)

find_package(absl REQUIRED)

find_package(magic_enum REQUIRED)

# Google upstream at a pinned hash. Populate-only: this project compiles the
# sources it needs directly, and the suite has no CMake build.
# Apply completion-wait, retry-count, and magic_enum include-path adjustments.
# Use a fresh build directory after changing the patches.
FetchContent_Declare(
  rdma_unit_test
  URL https://github.com/google/rdma-unit-test/archive/beb9daaecf73ac0c904a1be9e635d5447b0819f8.tar.gz)
FetchContent_GetProperties(rdma_unit_test)
if(NOT rdma_unit_test_POPULATED)
  FetchContent_Populate(rdma_unit_test)
endif()
execute_process(
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/apply_local_patch.py
          ${CMAKE_CURRENT_SOURCE_DIR}/patches ${rdma_unit_test_SOURCE_DIR}
  RESULT_VARIABLE LSMR_RDMA_UNIT_PATCH_RESULT)
if(LSMR_RDMA_UNIT_PATCH_RESULT)
  message(
    FATAL_ERROR
    "patch series did not apply (result ${LSMR_RDMA_UNIT_PATCH_RESULT})")
endif()

find_package(GTest REQUIRED)

# Suite sources relative to the fetched root.
set(LSMR_RDMA_UNIT_SUITE_SRCS
    internal/introspection_registrar.cc
    internal/handle_garble.cc
    internal/verbs_attribute.cc
    internal/verbs_cleanup.cc
    internal/verbs_extension.cc
    public/flags.cc
    public/basic_fixture.cc
    public/introspection.cc
    public/rdma_memblock.cc
    public/status_matchers.cc
    public/verbs_helper_suite.cc
    public/verbs_util.cc
    unit/access_test.cc
    unit/ah_test.cc
    unit/async_event_test.cc
    unit/batch_op_fixture.cc
    unit/buffer_test.cc
    unit/comp_channel_test.cc
    unit/cq_ex_test.cc
    unit/cq_test.cc
    unit/device_test.cc
    unit/hugepage_test.cc
    unit/loopback_fixture.cc
    unit/mr_test.cc
    unit/mw_test.cc
    unit/pd_test.cc
    unit/qp_test.cc
    unit/rc_test.cc
    unit/rdma_verbs_fixture.cc
    unit/srq_test.cc
    unit/stress_test.cc
    unit/threaded_test.cc
    unit/ud_test.cc
)

foreach(SUITE_SRC ${LSMR_RDMA_UNIT_SUITE_SRCS})
  list(
    APPEND LSMR_RDMA_UNIT_SUITE_ABS_SRCS
    "${rdma_unit_test_SOURCE_DIR}/${SUITE_SRC}")
endforeach()

# The suite is pinned upstream Google code, fetched, not written here: its
# absl::MutexLock(pointer) calls predate the reference-only deprecation in
# the pinned absl, and three mw_test.cc helpers fall off the end. Silence
# both warning classes on the fetched sources only; the shims below keep
# the default warnings.
set_source_files_properties(
  ${LSMR_RDMA_UNIT_SUITE_ABS_SRCS}
  PROPERTIES COMPILE_OPTIONS "-Wno-deprecated-declarations;-Wno-return-type")
add_executable(
  lsmr_run_unit_test ${LSMR_RDMA_UNIT_SUITE_ABS_SRCS} lsmr_ibv_shim.cpp
  lsmr_gunit_main.cc)
target_compile_features(lsmr_run_unit_test PRIVATE cxx_std_20)
# Suite headers include by root-relative path (public/flags.h).
target_include_directories(
  lsmr_run_unit_test PRIVATE ${rdma_unit_test_SOURCE_DIR})
target_link_libraries(
  lsmr_run_unit_test
  PRIVATE libsoftmetaroce::softmetaroce
          GTest::gtest
          GTest::gmock
          magic_enum::magic_enum
          absl::cleanup
          absl::failure_signal_handler
          absl::flags
          absl::flags_parse
          absl::flat_hash_map
          absl::flat_hash_set
          absl::log
          absl::log_flags
          absl::log_initialize
          absl::log_internal_check_op
          absl::status
          absl::statusor
          absl::str_format
          absl::strings
          absl::synchronization
          absl::time)

# The libibverbs symbols the shim intercepts; datapath
# verbs dispatch through context->ops trampolines instead.
set(LSMR_RDMA_UNIT_WRAP_SYMS
    mmap
    ibv_get_device_list
    ibv_free_device_list
    ibv_open_device
    ibv_close_device
    ibv_query_device
    ibv_query_port
    ibv_query_gid
    ibv_alloc_pd
    ibv_dealloc_pd
    ibv_reg_mr
    ibv_reg_mr_iova2
    ibv_reg_mr_iova
    ibv_dereg_mr
    ibv_create_comp_channel
    ibv_destroy_comp_channel
    ibv_get_cq_event
    ibv_ack_cq_events
    ibv_create_cq
    ibv_destroy_cq
    ibv_create_qp
    ibv_destroy_qp
    ibv_modify_qp
    ibv_query_qp
    ibv_create_srq
    ibv_modify_srq
    ibv_query_srq
    ibv_destroy_srq
    ibv_get_async_event
    ibv_ack_async_event
)
foreach(WRAP_SYM ${LSMR_RDMA_UNIT_WRAP_SYMS})
  target_link_options(lsmr_run_unit_test PRIVATE
    "-Wl,--wrap,${WRAP_SYM}")
endforeach()

# Tests known to pass against lsmr.
set(LSMR_RDMA_UNIT_GREEN
    LoopbackRcQpTest.Send
    LoopbackRcQpTest.SendLargeChunk
    LoopbackRcQpTest.BasicRead
    LoopbackRcQpTest.BasicReadLargePayload
    LoopbackRcQpTest.BasicWrite
    LoopbackRcQpTest.BasicWriteLargePayload
    LoopbackRcQpTest.QpSigAll
    QpPostTest.OverflowSendWr
    CqTest.AboveMaxCompVector
    CqTest.Basic
    CqTest.DestroyInvalidCq
    CqTest.LargeCompVector
    CqTest.MaxCqe
    CqTest.NegativeCompVector
    CqTest.NegativeCqe
    CqTest.ShareChannel
    CqTest.WithChannel
    CqTest.ZeroCompVector
    CqTest.ZeroCqe
    CompChannelTest.Atomic
    CompChannelTest.CreateDestroy
    CompChannelTest.DeleteWithUnacked
    CompChannelTest.DestroyChannelWithCqRef
    CompChannelTest.DowngradeRequest
    CompChannelTest.ManyOutstanding
    CompChannelTest.MuxOntoSingleChannel
    CompChannelTest.RecvUnsolicitedNofityAny
    CompChannelTest.RequestNotificationInvalidCq
    CompChannelTest.RequestNotificationOnCqWithoutCompChannel
    CompChannelTest.SameQueueMultipleOutstanding
    CompChannelTest.UpgradeRequest
    CompChannelTest.Write
    LoopbackRcQpTest.SendManyWithOneOutstanding
    LoopbackRcQpTest.SendEmptySgl
    LoopbackRcQpTest.WriteZeroByteWithImmData
    LoopbackRcQpTest.WriteImmData
    LoopbackRcQpTest.FetchAddNoOp
    LoopbackRcQpTest.FetchAddIncrementBy1
    LoopbackRcQpTest.FetchAddLargeIncrement
    LoopbackRcQpTest.CompareSwapNotEqualNoSwap
    LoopbackRcQpTest.CompareSwapEqualWithSwap
    LoopbackRcQpTest.SgePointerChase
    LoopbackRcQpTest.QueryQpInitialState
    QpTest.MaxSge
    QpTest.UnknownType
    MrTest.DeregInvalidMr
    PdTest.DeleteInvalidPd
    PdTest.AllocQpWithInvalidPd
    PdTest.AllocMrWithInvalidPd
    PdTest.MrRefCount
    PdTest.QpRefCount
    PdTest.SrqRefCount
    QpTest.DeallocPdWithOutstandingQp
    QpTest.CreateWithInvalidSendCq
    QpTest.CreateWithInvalidRecvCq
    SrqTest.Create
    SrqTest.DestroyInvalidSrq
    SrqTest.CreateWithInvalidPd
    SrqTest.CreateQpWithSrq
    SrqTest.PostWrongApi
    SrqTest.MaxWr
    SrqTest.ExceedMaxWr
    SrqTest.MaxSge
    SrqTest.ExceedMaxSge
    SrqTest.DeviceCap
    SrqTest.ExceedDeviceMaxWr
    SrqTest.ExceedDeviceMaxSge
    SrqTest.ExceedMaxWrInfinitChain
    QpTest.CreateWithInvalidSrq
    DeviceLimitTest.MaxSrq
    PdSrqTest.CreateSrq
    ThreadedTest.CreateDestroySrq
    ThreadedTest.CreateSrq
    SrqTest.Send
    SrqTest.SendRnr
    SrqRcMultiplexTest.Send
    SrqRcMultiplexTest.WriteWithImmediate
    SrqMultiThreadTest.MultiThreadedSrqLoopback
    SrqMultiThreadTest.MultiThreadedMultiQpSingleSrq
    PdSrqTest.SrqRecvMrSrqMatch
    PdSrqTest.SrqRecvMrSrqMismatch
    PdSrqTest.QpSrqPdMismatchWriteWithImm
    SrqTest.SrqLimit
    QpTest.QueueRefs
    QpTest.ModifyInvalidQp
    QpTest.QueryInvalidQp
    QpTest.DestroyInvalidQp
    LoopbackRcQpTest.ReadInvalidRkey
    LoopbackRcQpTest.ReadUnregisteredRemoteAddress
    LoopbackRcQpTest.WriteInvalidRkey
    LoopbackRcQpTest.UnsignaledWriteInvalidRkey
    LoopbackRcQpTest.WriteUnregisteredAddress
    LoopbackRcQpTest.FetchAddInvalidRKey
    LoopbackRcQpTest.CompareSwapInvalidRKey
    LoopbackRcQpTest.SendRnrInfiniteRetries
    LoopbackRcQpTest.FetchAddSplitSgl
    LoopbackRcQpTest.SendInvalidLkey
    LoopbackRcQpTest.UnsignaledSendError
    LoopbackRcQpTest.ReadInvalidLkey
    LoopbackRcQpTest.UnsignaledReadError
    LoopbackRcQpTest.WriteInvalidLkey
    LoopbackRcQpTest.WriteInvalidRKeyAndInvalidLKey
    LoopbackRcQpTest.FetchAddInvalidLKey
    LoopbackRcQpTest.UnsignaledFetchAddError
    LoopbackRcQpTest.BadRecvLkey
    LoopbackRcQpTest.BadRecvAddr
    LoopbackRcQpTest.RecvOnDeregisteredRegion
    LoopbackRcQpTest.RecvPayloadExceedMr
    LoopbackRcQpTest.RecvBufferExceedMr
    LoopbackRcQpTest.WriteImmDataInvalidRKey
    LoopbackRcQpTest.WriteInlineData
    LoopbackRcQpTest.SendInlineData
    LoopbackRcQpTest.SendExceedMaxInlineData
    LoopbackRcQpTest.SendInlineDataInvalidOp
    LoopbackRcQpTest.SendRnr
    LoopbackRcQpTest.WriteImmDataRnR
    LoopbackRcQpTest.FetchAddInvalidSize
    LoopbackRcQpTest.CompareSwapInvalidSize
    LoopbackRcQpTest.FetchAddUnaligned
    LoopbackRcQpTest.CompareSwapUnaligned
    LoopbackRcQpTest.CompareSwapInvalidLKey
    LoopbackRcQpTest.UnsignaledCompareSwapError
    LoopbackRcQpTest.FetchAddInvalidLKeyAndInvalidRKey
    LoopbackRcQpTest.CompareSwapInvalidRKeyAndInvalidLKey
    LoopbackRcQpTest.FetchAddUnalignedInvalidLKey
    LoopbackRcQpTest.FetchAddUnalignedInvalidRKey
    LoopbackRcQpTest.CompareSwapUnalignedInvalidRKey
    LoopbackRcQpTest.CompareSwapUnalignedInvalidLKey
    LoopbackRcQpTest.UnsignaledSend
    LoopbackRcQpTest.UnsignaledRead
    LoopbackRcQpTest.UnsignaledWrite
    LoopbackRcQpTest.UnsignaledFetchAdd
    LoopbackRcQpTest.UnsignaledCompareSwap
    MessagingAccessTest.AllAccess
    MessagingAccessTest.MissingDstRemoteWrite
    MessagingAccessTest.MissingSrcLocalWrite
    LoopbackRcQpTest.FlushErrorPollTogether
    LoopbackRcQpTest.RemoteFatalError
    LoopbackRcQpTest.SendImmData
    Rdma/RdmaAccessTest.MissingDstRemoteWrite/MemoryRegion*
    Rdma/RdmaAccessTest.MissingDstRemoteRead/MemoryRegion*
    LoopbackRcQpTest.ReadInvalidRKeyAndInvalidLKey
    Rdma/RdmaAccessTest.AllAccess/MemoryRegion*
    Rdma/RdmaAccessTest.MissingSrcLocalWrite/MemoryRegion*
    Rdma/RdmaAccessTest.MissingDstRemoteAtomic/MemoryRegion*
    Rdma/RdmaAccessTest.ZeroBasedAccess/MemoryRegion*
    DeviceTest.GetDeviceList
    DeviceTest.Open
    DeviceTest.OpenMany
    DeviceTest.OpenInAnotherThread
    DeviceTest.OpenInManyThreads
    DeviceTest.ContextTomfoolery
    DeviceTest.QueryDevice
    DeviceLimitTest.MaxPd
    PdTest.AllocPd
    MrTest.RegisterMemory
    BufferTest.RegisterZeroByteMr
    ThreadedTest.AllocDeallocPd
    ThreadedTest.AllocPd
    ThreadedTest.CreateCq
    ThreadedTest.CreateQp
    ThreadedTest.CreateDestroyQp
    ThreadedTest.RegMr
    ThreadedTest.RegDeregMr
    ThreadedTest.CreateDestroyCq
    BufferMrTest.ReadExceedFront
    BufferMrTest.ReadExceedRear
    BufferMrTest.WriteExceedFront
    BufferMrTest.WriteExceedRear
    BufferMrTest.ReadZeroByte
    BufferMrTest.ReadZeroByteFromZeroByteMr
    BufferMrTest.ReadZeroByteOutsideZeroByteMr
    BufferMrTest.ReadZeroByteOutsideMr
    BufferMrTest.ReadZeroByteInvalidRKey
    BufferMrTest.SendZeroByte
    BufferMrTest.SendZeroByteOutsideMr
    BufferMrTest.SendZeroByteFromZeroByteMr
    MrLoopbackTest.OutstandingRecv
    QpTest.Create
    QpTest.CreateWithInlineMax
    QpTest.ZeroSendWr
    QpTest.MaxQpWr
    QpTest.Modify
    QpTest.ModifyInvalidResetToRtrTransition
    QpTest.ModifyInvalidInitToRtsTransition
    QpTest.ModifyInvalidRtsToRtrTransition
    QpStateTest.DestroyFromReset
    QpStateTest.DestroyFromInit
    QpStateTest.PostRecvRtr
    QpStateTest.PostRecvInit
    QpStateTest.RtsSendToRtr
    QpStateTest.ModRtsToError
    QpStateTest.PostSendErr
    QpStateTest.PostSendErrConcurrent
    QpStateTest.PostRecvErr
    ThreadedWorkloadTest.QpLifecycle
    Atomic/AtomicAccessTest.AllAccess/MemoryRegion*
    Atomic/AtomicAccessTest.MissingSrcLocalWrite/MemoryRegion*
    Atomic/AtomicAccessTest.MissingDstRemoteRead/MemoryRegion*
    Atomic/AtomicAccessTest.MissingDstRemoteWrite/MemoryRegion*
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpRtrReadTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpRtrWriteTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpRtrFetchAndAddTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpRtrCompareAndSwapTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpRtsReadTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpRtsWriteTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpRtsFetchAndAddTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpRtsCompareAndSwapTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpErrReadTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpErrWriteTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpErrFetchAndAddTest
    RemoteRcQpStateTest/RemoteRcQpStateTest.RemoteRcQpStateTests/RemoteQpErrCompareAndSwapTest
    ResponderQpStateTest/ResponderQpStateTest.ResponderNotReadyToReceive/ServiceReadInResetState
    ResponderQpStateTest/ResponderQpStateTest.ResponderNotReadyToReceive/ServiceWriteInResetState
    ResponderQpStateTest/ResponderQpStateTest.ResponderNotReadyToReceive/ServiceReadInInitState
    ResponderQpStateTest/ResponderQpStateTest.ResponderNotReadyToReceive/ServiceWriteInInitState
    MessagingAccessTest.MissingDstLocalWrite
    MrTest.RemoteWriteWithoutLocalWrite
    MrTest.RemoteAtomicWithoutLocalWrite
    PdRcLoopbackMrTest.SendMrOtherPdLocal
    PdRcLoopbackMrTest.SendMrOtherPdRemote
    PdRcLoopbackMrTest.BasicReadMrOtherPdLocal
    PdRcLoopbackMrTest.BasicReadMrOtherPdRemote
    PdRcLoopbackMrTest.BasicWriteMrOtherPdLocal
    PdRcLoopbackMrTest.BasicWriteMrOtherPdRemote
    PdRcLoopbackMrTest.BasicFetchAddMrOtherPdLocal
    PdRcLoopbackMrTest.BasicFetchAddMrOtherPdRemote
    PdRcLoopbackMrTest.BasicCompSwapMrOtherPdLocal
    PdRcLoopbackMrTest.BasicCompSwapMrOtherPdRemote
    Atomic/AtomicAccessTest.MissingDstRemoteAtomic/MemoryRegion*
    CqOverflowTest.SendCqOverflow
    CqOverflowTest.SendSharedCqOverflow
    CqOverflowTest.RecvCqOverflow
    CqOverflowTest.RecvSharedCqOverflow
    CqBatchOpTest.SendSharedCq
    CqBatchOpTest.RecvSharedCq
)
string(JOIN ":" LSMR_RDMA_UNIT_FILTER ${LSMR_RDMA_UNIT_GREEN})
# Reject filter entries whose test definition is absent from the fetched suite.
execute_process(
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/check_green_names.py
          ${rdma_unit_test_SOURCE_DIR} ${LSMR_RDMA_UNIT_GREEN}
  RESULT_VARIABLE LSMR_RDMA_UNIT_NAMES_RESULT)
if(LSMR_RDMA_UNIT_NAMES_RESULT)
  message(
    FATAL_ERROR
      "green entries match no test definition in the fetched suite (result ${LSMR_RDMA_UNIT_NAMES_RESULT})"
  )
endif()

# These tests are absent from the pinned upstream suite.
# Fail configuration if their definitions appear.
set(LSMR_RDMA_UNIT_PRISTINE_SKIP
    LoopbackRcQpTest.SendRnrPreservesRequestOrder
)

# Reject skips whose test definition appears in the fetched suite: a skip that
# exists upstream belongs in the test filter, not here.
execute_process(
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/check_green_names.py
          --absent ${rdma_unit_test_SOURCE_DIR} ${LSMR_RDMA_UNIT_PRISTINE_SKIP}
  RESULT_VARIABLE LSMR_RDMA_UNIT_SKIP_RESULT)
if(LSMR_RDMA_UNIT_SKIP_RESULT)
  message(
    FATAL_ERROR
    "skip entries match test definitions in the fetched suite (result ${LSMR_RDMA_UNIT_SKIP_RESULT})"
  )
endif()
# Pass the filter on the command line: runners that invoke the binary
# directly never see CTest ENVIRONMENT properties.
add_test(
  NAME lsmr_run_unit_test
  COMMAND lsmr_run_unit_test "--gtest_filter=${LSMR_RDMA_UNIT_FILTER}")
