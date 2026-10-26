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

# Standalone CMake build for libsoftmetaroce as exported in the rdma_gen open
# source repository (the libsoftmetaroce/ subdirectory). Builds the static
# library and, unless disabled, the basic (unprivileged, no-VM) unit tests,
# the send/receive/ping sample tools, and the LD_PRELOAD interposer. The
# rdma-unit-test suite is opt-in because configuration downloads sources.
#
# From an rdma_gen OSS checkout, configure with this directory as the
# source, build, then run ctest in the build directory: cmake -S
# <repo>/libsoftmetaroce -B build-lsmr && cmake --build build-lsmr &&
# cd build-lsmr && ctest. The rdma_gen/ sibling directory must be present
# (MetaRoceVerbs.h includes rdma_gen/transport/RdmaVerbs.h). Also works via
# add_subdirectory() from the repo root; tests default off there so the
# root build is unchanged. A parent that enables the tests must also call
# enable_testing() for root-level ctest discovery.
#
# LSMR_LIB_SRCS mirrors the libsoftmetaroce entries of the repo-root CMakeLists.
# The test list in test/CMakeLists.txt includes the *Test targets except the
# benches, the test subdirectories, and tests without a target. A configure
# without test/ skips the tests with a warning.

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  cmake_minimum_required(VERSION 3.18.4)
  project(softmetaroce LANGUAGES CXX)
  set(LSMR_STANDALONE TRUE)
else()
  set(LSMR_STANDALONE FALSE)
endif()

# Same toolchain floor as the repo-root build.
if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  message(FATAL_ERROR "This project requires GCC compiler. Current compiler: "
                      "${CMAKE_CXX_COMPILER_ID}")
endif()
if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "13.0")
  message(
    FATAL_ERROR
      "This project requires GCC version 13 or higher. Current version: "
      "${CMAKE_CXX_COMPILER_VERSION}")
endif()

# getdeps settings (repo-root relative, so prefixed with the parent here).
set(CMAKE_MODULE_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/../build/fbcode_builder/CMake"
    "${CMAKE_CURRENT_SOURCE_DIR}/.." ${CMAKE_MODULE_PATH})

find_package(Boost 1.78.0 REQUIRED)
find_package(Gflags REQUIRED)
find_package(Glog REQUIRED)
find_package(fmt REQUIRED)
find_package(folly REQUIRED)
find_package(ibverbs REQUIRED)
# will not get here if ibverbs is not found
message("-- ibverbs found ${IBVERBS_INCLUDE_DIRS} (version ${IBVERBS_VERSION})")

# Check if ibverbs supports the extended API (ibv_reg_mr_ex, ibv_alloc_dmah,
# ibv_dealloc_dmah)
include(CheckCXXSourceCompiles)
include(CMakePushCheckState)
cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_INCLUDES ${IBVERBS_INCLUDE_DIRS})
set(CMAKE_REQUIRED_LIBRARIES ${IBVERBS_LIBRARIES})

check_cxx_source_compiles(
  "
  #include <infiniband/verbs.h>
  int main() {
    ibv_mr_init_attr attr;
    ibv_reg_mr_ex(nullptr, &attr);
    ibv_dmah_init_attr dmah_attr;
    ibv_alloc_dmah(nullptr, &dmah_attr);
    ibv_dealloc_dmah(nullptr);
    return 0;
  }
"
  IBVERBS_HAS_DMAH)
cmake_pop_check_state()

if(NOT IBVERBS_HAS_DMAH)
  message(WARNING "ibverbs library found but does not support dmah functions "
                  "(ibv_reg_mr_ex, ibv_alloc_dmah, ibv_dealloc_dmah). "
                  "Defining IBVERBS_LACKS_DMAH preprocessor variable.")
else()
  message("-- ibverbs supports dmah functions "
          "(ibv_reg_mr_ex, ibv_alloc_dmah, ibv_dealloc_dmah)")
endif()

find_package(Threads REQUIRED)

# Sources using neteng/ai/... include paths need a binary-dir shim. The
# GitHub export and the getdeps CI tree are both full ShipIt exports with
# rewritten includes (the rdma_gen manifest sets use_shipit=true), so only
# a raw fbcode checkout still resolves neteng/ai/... through this shim.
set(LSMR_SHIM_DIR "${CMAKE_CURRENT_BINARY_DIR}/lsmr_shim")
file(MAKE_DIRECTORY "${LSMR_SHIM_DIR}/neteng/ai")
# Recreated on every configure so a reused binary dir never keeps a stale target
# if the source directory moved. rm -rf clears a stale real directory too,
# not just a symlink.
execute_process(COMMAND ${CMAKE_COMMAND} -E rm -rf
                        "${LSMR_SHIM_DIR}/neteng/ai/libsoftmetaroce"
                RESULT_VARIABLE LSMR_RM_RESULT
                ERROR_VARIABLE LSMR_RM_ERROR)
if(LSMR_RM_RESULT)
  message(
    FATAL_ERROR
      "could not remove stale ${LSMR_SHIM_DIR}/neteng/ai/libsoftmetaroce "
      "(result ${LSMR_RM_RESULT}): ${LSMR_RM_ERROR}")
endif()
execute_process(
  COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}"
          "${LSMR_SHIM_DIR}/neteng/ai/libsoftmetaroce"
  RESULT_VARIABLE LSMR_SYMLINK_RESULT)
if(LSMR_SYMLINK_RESULT)
  message(
    FATAL_ERROR
      "could not create ${LSMR_SHIM_DIR}/neteng/ai/libsoftmetaroce symlink "
      "(result ${LSMR_SYMLINK_RESULT}): the build directory must be writable "
      "and the filesystem must support symlinks.")
endif()
set(LSMR_RDMA_GEN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../rdma_gen")
if(NOT EXISTS "${LSMR_RDMA_GEN_DIR}/transport/RdmaVerbs.h")
  message(
    FATAL_ERROR
      "rdma_gen sibling directory not found at ${LSMR_RDMA_GEN_DIR}: "
      "MetaRoceVerbs.h includes rdma_gen/transport/RdmaVerbs.h. Configure from an "
      "rdma_gen OSS checkout so libsoftmetaroce/ and rdma_gen/ are siblings.")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} -E rm -rf
                        "${LSMR_SHIM_DIR}/neteng/ai/rdma_gen"
                RESULT_VARIABLE LSMR_RM_RESULT
                ERROR_VARIABLE LSMR_RM_ERROR)
if(LSMR_RM_RESULT)
  message(
    FATAL_ERROR
      "could not remove stale ${LSMR_SHIM_DIR}/neteng/ai/rdma_gen "
      "(result ${LSMR_RM_RESULT}): ${LSMR_RM_ERROR}")
endif()
execute_process(
  COMMAND ${CMAKE_COMMAND} -E create_symlink "${LSMR_RDMA_GEN_DIR}"
          "${LSMR_SHIM_DIR}/neteng/ai/rdma_gen"
  RESULT_VARIABLE LSMR_SYMLINK_RESULT)
if(LSMR_SYMLINK_RESULT)
  message(
    FATAL_ERROR
      "could not create ${LSMR_SHIM_DIR}/neteng/ai/rdma_gen symlink "
      "(result ${LSMR_SYMLINK_RESULT}): the build directory must be writable "
      "and the filesystem must support symlinks.")
endif()

# Library sources: .cpps first, then headers.
set(LSMR_LIB_SRCS
    codec/AckHeader.cpp
    codec/AtomicHeader.cpp
    codec/BaseHeader.cpp
    codec/ControlHeader.cpp
    codec/DataHeader.cpp
    codec/MetaRoceCodec.cpp
    codec/PingHeader.cpp
    codec/ReadWriteHeader.cpp
    codec/RnrCancelHeader.cpp
    congestion_control/Flags.cpp
    modifier/ModifierConfig.cpp
    modifier/CorruptModifier.cpp
    modifier/DuplicateModifier.cpp
    modifier/HeaderEditModifier.cpp
    modifier/MarkEcnModifier.cpp
    modifier/Modifier.cpp
    modifier/PacingModifier.cpp
    modifier/ReorderModifier.cpp
    modifier/StripedMessageWaveModifier.cpp
    modifier/TruncateModifier.cpp
    modifier/WriteHeaderEditModifier.cpp
    ClockSource.cpp
    CompletionQueue.cpp
    ConcurrentQpArbiter.cpp
    DeadlineQueue.cpp
    EgressProfile.cpp
    EsunEncapsulation.cpp
    EsunProfile.cpp
    EthernetEncapsulation.cpp
    GidDiscovery.cpp
    HighResTimer.cpp
    MemoryRegions.cpp
    MetaRoceVerbs.cpp
    PacketModifierSocket.cpp
    PacketSocketFactory.cpp
    PingController.cpp
    ProtocolCompatibility.cpp
    Qp.cpp
    QpArbiter.cpp
    RawEthernetSocket.cpp
    RawPacketSocket.cpp
    RcnRateHint.cpp
    ReceiveSequence.cpp
    Receiver.cpp
    RenamedFlag.cpp
    RnrState.cpp
    RxFlow.cpp
    Sender.cpp
    SgeList.cpp
    SharedReceiveQueue.cpp
    SourcePortCounter.cpp
    Tag.cpp
    TagSet.cpp
    TxFlow.cpp
    UdpEncapsulation.cpp
    UdpPacketSocket.cpp
    WatchdogTimer.cpp
    WireProfiles.cpp
    Wqe.cpp
    codec/AckHeader.h
    codec/AtomicHeader.h
    codec/BaseHeader.h
    codec/BitVector.h
    codec/Codec.h
    codec/ControlHeader.h
    codec/DataHeader.h
    codec/Header.h
    codec/MetaRoceCodec.h
    codec/Packet.h
    codec/PingHeader.h
    codec/ReadWriteHeader.h
    codec/RnrCancelHeader.h
    congestion_control/Baseline.h
    congestion_control/BaselineWithRCN.h
    congestion_control/CongestionController.h
    congestion_control/CongestionControllerFactory.h
    congestion_control/FastStart.h
    congestion_control/Flags.h
    congestion_control/StaticQwnd.h
    congestion_control/SynchronizedCongestionController.h
    modifier/Modifier.h
    modifier/ModifierConfig.h
    modifier/ReorderModifier.h
    modifier/StripedMessageWaveModifier.h
    modifier/CorruptModifier.h
    modifier/DuplicateModifier.h
    modifier/HeaderEditModifier.h
    modifier/MarkEcnModifier.h
    modifier/PacingModifier.h
    modifier/TruncateModifier.h
    modifier/WriteHeaderEditModifier.h
    BlockingCoroLock.h
    ClockSource.h
    CompletionQueue.h
    ConcurrentQpArbiter.h
    ConnectionObserver.h
    Constants.h
    Cqe.h
    DeadlineQueue.h
    Ecn.h
    EgressPacket.h
    EgressProfile.h
    Encapsulation.h
    EsunEncapsulation.h
    EsunHeader.h
    EsunProfile.h
    EthernetEncapsulation.h
    Flow.h
    GidDiscovery.h
    HighResTimer.h
    Icrc.h
    IngressPacket.h
    MemoryRegions.h
    MetaRoceVerbs.h
    MultiUnboundedQueue.h
    NextFreeId.h
    PacketId.h
    PacketModifierSocket.h
    PacketSocket.h
    PacketSocketFactory.h
    PingController.h
    PosnAuditor.h
    ProtocolCompatibility.h
    Qp.h
    QpArbiter.h
    QpArbiterIface.h
    QpState.h
    ReceiveSequence.h
    RawEncapsulation.h
    RawEthernetSocket.h
    RawPacketSocket.h
    RcnRateHint.h
    TagSet.h
    Receiver.h
    RejectedPosnSet.h
    RenamedFlag.h
    RnrState.h
    RxFlow.h
    Sender.h
    SequenceNumbers.h
    SgeList.h
    SharedReceiveQueue.h
    ShiftableBitset.h
    SourcePortCounter.h
    Tag.h
    TxFlow.h
    UdpEncapsulation.h
    UdpPacketSocket.h
    WatchdogTimer.h
    WireProfiles.h
    Wqe.h
    WrappingCompare.h
    WrappingMapIterator.h
    WrrScheduler.h)

add_library(softmetaroce STATIC ${LSMR_LIB_SRCS})
# The LD_PRELOAD interposer links this archive into a shared object, so its
# objects need to be position-independent even in static builds.
set_target_properties(softmetaroce PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_library(libsoftmetaroce::softmetaroce ALIAS softmetaroce)
target_compile_features(softmetaroce PUBLIC cxx_std_20)
# -fcoroutines is PUBLIC: consumers include coroutine headers through Qp.h.
target_compile_options(
  softmetaroce
  PUBLIC -fcoroutines
  PRIVATE -Wall)
# Both macros change what consumers compile from the public headers (base class,
# virtual interface), so both propagate.
target_compile_definitions(
  softmetaroce PUBLIC RDMA_GEN_OSS
                      $<$<NOT:$<BOOL:${IBVERBS_HAS_DMAH}>>:IBVERBS_LACKS_DMAH>)
# Includes arrive in export-root form (libsoftmetaroce/..., rdma_gen/...);
# the parent is the export root both standalone and via add_subdirectory().
target_include_directories(
  softmetaroce
  PUBLIC ${LSMR_SHIM_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..
         ${Boost_INCLUDE_DIR} ${FOLLY_INCLUDE_DIR} ${gflags_INCLUDE_DIR}
         ${IBVERBS_INCLUDE_DIRS})
# These are needed because folly headers depend on them.
get_target_property(FMT_INCLUDE_DIRS fmt::fmt INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(GLOG_INCLUDE_DIRS glog::glog INTERFACE_INCLUDE_DIRECTORIES)
if(FMT_INCLUDE_DIRS)
  target_include_directories(softmetaroce PUBLIC ${FMT_INCLUDE_DIRS})
endif()
if(GLOG_INCLUDE_DIRS)
  target_include_directories(softmetaroce PUBLIC ${GLOG_INCLUDE_DIRS})
endif()
target_link_libraries(
  softmetaroce PUBLIC Folly::folly fmt::fmt gflags glog::glog
                      ${IBVERBS_LIBRARIES} Threads::Threads)

# The interposer needs nothing beyond the library, so it defaults with the
# samples: on standalone, off as a subdirectory.
option(LSMR_BUILD_PRELOAD "Build the LD_PRELOAD verbs interposer"
       ${LSMR_STANDALONE})
if(LSMR_BUILD_PRELOAD)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/preload/CMakeLists.txt")
    add_subdirectory(preload)
  else()
    message(
      WARNING "LSMR_BUILD_PRELOAD is on but preload/CMakeLists.txt is absent; "
              "skipping the interposer.")
  endif()
endif()

# Tests honor the getdeps BUILD_TESTS define when set, else default on
# standalone and off as a subdirectory. A set BUILD_TESTS wins on every
# configure (FORCE): option() would keep the first-seen value in the cache
# and silently ignore a -DBUILD_TESTS flip on reconfigure.
if(DEFINED BUILD_TESTS)
  set(LSMR_BUILD_TESTS
      ${BUILD_TESTS}
      CACHE BOOL "Build the basic (unprivileged) unit tests" FORCE)
else()
  option(LSMR_BUILD_TESTS "Build the basic (unprivileged) unit tests"
         ${LSMR_STANDALONE})
endif()

if(LSMR_BUILD_TESTS)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test/CMakeLists.txt")
    # enable_testing lives here, not in test/: ctest discovers test/ only when
    # testing is enabled in an ancestor of the directory it runs from.
    enable_testing()
    add_subdirectory(test)
  else()
    message(
      WARNING "LSMR_BUILD_TESTS is on but test/CMakeLists.txt is absent; "
              "skipping tests.")
  endif()
endif()

# Samples default on standalone and off as a subdirectory, like the tests.
option(LSMR_BUILD_SAMPLES "Build the send/receive/ping sample tools"
       ${LSMR_STANDALONE})
# The conformance suite is never built by default: it downloads its sources
# (and Abseil) at configure time, so it builds only when requested.
option(LSMR_BUILD_RDMA_UNIT "Build the rdma-unit-test conformance suite" OFF)
if(LSMR_BUILD_SAMPLES)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/samples/CMakeLists.txt")
    add_subdirectory(samples)
  else()
    # Fatal, not a warning: a standalone leaf configure has no root-level
    # existence check to catch a partial export, so silently skipping the
    # requested samples would run CI with zero sample coverage.
    message(
      FATAL_ERROR
        "LSMR_BUILD_SAMPLES is on but samples/CMakeLists.txt is absent; "
        "the export is missing the samples tree.")
  endif()
endif()

if(LSMR_BUILD_RDMA_UNIT)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test/rdma-unit/CMakeLists.txt")
    # Enable conformance tests even when the basic unit tests are disabled.
    enable_testing()
    add_subdirectory(test/rdma-unit)
  else()
    message(
      WARNING "LSMR_BUILD_RDMA_UNIT is on but test/rdma-unit/CMakeLists.txt "
              "is absent; skipping the conformance suite.")
  endif()
endif()

# The namespace suite is never built by default: it needs user/network
# namespaces, so it runs only when requested.
option(LSMR_BUILD_NETNS_TESTS "Build the namespace tests" OFF)
if(LSMR_BUILD_NETNS_TESTS)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test/netns/CMakeLists.txt")
    # Enable namespace tests even when the basic unit tests are disabled.
    enable_testing()
    add_subdirectory(test/netns)
  else()
    # Fatal: silently skipping requested coverage would report success
    # with zero namespace tests.
    message(
      FATAL_ERROR
        "LSMR_BUILD_NETNS_TESTS is on but test/netns/CMakeLists.txt is absent; "
        "the export is missing the namespace test tree.")
  endif()
endif()
