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

cmake_minimum_required(VERSION 3.18.4)
project(rdma_gen LANGUAGES CXX)
enable_language(CXX)

# Check that we're using GCC with version at least 13
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

message(STATUS "Using GCC ${CMAKE_CXX_COMPILER_VERSION}")

# General CMake settings
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

# CXX Settings
set(CMAKE_CXX_STANDARD 20)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fcoroutines>)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_DEBUG "-Wall -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-Wall -O2")
add_definitions(-DNO_CUDA -DNO_ROCM)

# Optionally build with LTO
option(BUILD_WITH_LTO "BUILD_WITH_LTO" OFF)
if(BUILD_WITH_LTO)
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto")
endif()

# OSS FLag
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DRDMA_GEN_OSS")

# getdeps settings
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/build/fbcode_builder/CMake"
                      "${CMAKE_SOURCE_DIR}" ${CMAKE_MODULE_PATH})

# Dependencies
find_package(ibverbs REQUIRED)
# will not get here if ibverbs is not found
message("-- ibverbs found ${IBVERBS_INCLUDE_DIRS} (version ${IBVERBS_VERSION})")

# Check if ibverbs supports the extended API
# (ibv_reg_mr_ex, ibv_alloc_dmah, ibv_dealloc_dmah)
include(CheckCXXSourceCompiles)
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

if(NOT IBVERBS_HAS_DMAH)
  message(
    WARNING
      "ibverbs library found but does not support dmah functions "
      "(ibv_reg_mr_ex, ibv_alloc_dmah, ibv_dealloc_dmah). "
      "Defining IBVERBS_LACKS_DMAH preprocessor variable."
  )
  add_definitions(-DIBVERBS_LACKS_DMAH)
else()
  message(
    "-- ibverbs supports dmah functions "
    "(ibv_reg_mr_ex, ibv_alloc_dmah, ibv_dealloc_dmah)"
  )
endif()

find_package(Boost 1.78.0 REQUIRED)
find_package(Gflags REQUIRED)
find_package(Glog REQUIRED)
#find_package(LibEvent REQUIRED)
#find_package(fizz REQUIRED)
find_package(fmt REQUIRED)
find_package(folly REQUIRED)
find_package(Re2 REQUIRED)
find_library(RE2 re2)
find_package(thrift CONFIG REQUIRED)
find_package(wangle REQUIRED)

# Add include directories for fmt and glog
# These are needed because folly headers depend on them
get_target_property(FMT_INCLUDE_DIRS fmt::fmt INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(GLOG_INCLUDE_DIRS glog::glog INTERFACE_INCLUDE_DIRECTORIES)
if(FMT_INCLUDE_DIRS)
  include_directories(${FMT_INCLUDE_DIRS})
endif()
if(GLOG_INCLUDE_DIRS)
  include_directories(${GLOG_INCLUDE_DIRS})
endif()

# Find CUDA
find_package(CUDA 9.0 QUIET)
if(CUDA_FOUND)
  message("-- CUDA found, adding CUDA support")
  enable_language(CUDA)
  set(SRC_LIB_GPU rdma_gen/cuda.h)
  set(LIBS_COMMON_GPU -lcuda)
else()
  message("-- CUDA not found, skipping CUDA support")
endif()

# Find HIP
# set(CMAKE_PREFIX_PATH "${ROCM_PATH};/opt/rocm;$CMAKE_PREFIX_PATH")
# find_package(HIP)
# if(HIP_FOUND)
#   message("-- HIP found, adding HIP support")
#   enable_language(HIP)
#   set(CMAKE_CXX_COMPILER ${HIP_HIPCC_EXECUTABLE})
#   set(CMAKE_CXX_LINKER   ${HIP_HIPCC_EXECUTABLE})
#   set(SRC_LIB_GPU
#     cuda_to_hip.h
#     ${ROCM_PATH}/include/hip/hip_runtime.h
#   )
#   set (LIBS_COMMON_GPU
#     -lamdhip64
#   )
# else()
#   message("-- HIP not found, skipping HIP support")
# endif()

# Includes
include_directories(${Boost_INCLUDE_DIR} ${FOLLY_INCLUDE_DIR}
                    ${gflags_INCLUDE_DIR} ${IBVERBS_INCLUDE_DIRS})
find_path(RE2_INCLUDE_DIR re2/re2.h)
include_directories(${RE2_INCLUDE_DIR})

# Sources
# The list of sources is kept in two alphabetically ordered lists,
# starting with .cpp files, then .h files.
set(SRC_LIB
    rdma_gen/Common.cpp
    rdma_gen/CqeLogEntry.cpp
    rdma_gen/GFlagValidators.cpp
    rdma_gen/PayloadPattern.cpp
    rdma_gen/RdmaGenAtomicRequester.cpp
    rdma_gen/RdmaGenClientServerMain.cpp
    rdma_gen/RdmaGenClientStatsRecorder.cpp
    rdma_gen/RdmaGenControlFactory.cpp
    rdma_gen/RdmaGenEndpoint.cpp
    rdma_gen/RdmaGenFlags.cpp
    rdma_gen/RdmaGenMain.cpp
    rdma_gen/RdmaGenMtu.cpp
    rdma_gen/RdmaGenPoolManager.cpp
    rdma_gen/RdmaGenReadRequester.cpp
    rdma_gen/RdmaGenReadResponder.cpp
    rdma_gen/RdmaGenReceiver.cpp
    rdma_gen/RdmaGenRequester.cpp
    rdma_gen/RdmaGenRequesterFactory.cpp
    rdma_gen/RdmaGenResponder.cpp
    rdma_gen/RdmaGenResponderFactory.cpp
    rdma_gen/RdmaGenSender.cpp
    rdma_gen/RdmaGenServerStatsRecorder.cpp
    rdma_gen/RdmaGenThriftControlClient.cpp
    rdma_gen/RdmaGenThriftControlCommon.cpp
    rdma_gen/RdmaGenThriftControlServer.cpp
    rdma_gen/RdmaGenUtils.cpp
    rdma_gen/RdmaGenWriteRequester.cpp
    rdma_gen/RdmaGenWriteResponder.cpp
    rdma_gen/memory/CpuMemory.cpp
    rdma_gen/memory/GpuMemory.cpp
    rdma_gen/memory/RdmaGenMemory.cpp
    rdma_gen/memory/RdmaGenMemoryFactory.cpp
    rdma_gen/memory/RdmaGenRdmaMode.cpp
    rdma_gen/memory/RocmMemory.cpp
    rdma_gen/memory/RocmMemoryRegistration.cpp
    rdma_gen/parsers/OperationTypeParser.cpp
    rdma_gen/transport/GetTransportProvider.cpp
    rdma_gen/transport/ITransportProvider.cpp
    rdma_gen/transport/RdmaGenVerbsMode.cpp
    rdma_gen/transport/VerbsProvider.cpp
    rdma_gen/Common.h
    rdma_gen/CqeLogEntry.h
    rdma_gen/GFlagValidators.h
    rdma_gen/PayloadPattern.h
    rdma_gen/RdmaGenAtomicRequester.h
    rdma_gen/RdmaGenClientServerMain.h
    rdma_gen/RdmaGenClientStatsRecorder.h
    rdma_gen/RdmaGenControlFactory.h
    rdma_gen/RdmaGenEndpoint.h
    rdma_gen/RdmaGenFlags.h
    rdma_gen/RdmaGenMain.h
    rdma_gen/RdmaGenMtu.h
    rdma_gen/RdmaGenPoolManager.h
    rdma_gen/RdmaGenReadRequester.h
    rdma_gen/RdmaGenReadResponder.h
    rdma_gen/RdmaGenReceiver.h
    rdma_gen/RdmaGenRequesterFactory.h
    rdma_gen/RdmaGenRequester.h
    rdma_gen/RdmaGenResponderFactory.h
    rdma_gen/RdmaGenResponder.h
    rdma_gen/RdmaGenSender.h
    rdma_gen/RdmaGenServerStatsRecorder.h
    rdma_gen/RdmaGenStallWatchdog.h
    rdma_gen/RdmaGenStatsRecorder.h
    rdma_gen/RdmaGenThriftControlClient.h
    rdma_gen/RdmaGenThriftControlCommon.h
    rdma_gen/RdmaGenThriftControlServer.h
    rdma_gen/RdmaGenUtils.h
    rdma_gen/RdmaGenWriteRequester.h
    rdma_gen/RdmaGenWriteResponder.h
    rdma_gen/memory/CpuMemory.h
    rdma_gen/memory/CudaWrapper.h
    rdma_gen/memory/GpuMemory.h
    rdma_gen/memory/RdmaGenMemory.h
    rdma_gen/memory/RdmaGenMemoryFactory.h
    rdma_gen/memory/RdmaGenRdmaMode.h
    rdma_gen/memory/RocmMemory.h
    rdma_gen/memory/RocmMemoryRegistration.h
    rdma_gen/parsers/OperationTypeParser.h
    rdma_gen/transport/GetTransportProvider.h
    rdma_gen/transport/IRdmaVerbs.h
    rdma_gen/transport/ITransportProvider.h
    rdma_gen/transport/RdmaGenVerbsMode.h
    rdma_gen/transport/RdmaVerbs.h
    rdma_gen/transport/VerbsProvider.h)

set(SRC_MAIN rdma_gen/RdmaGen.cpp)

# no need to explicitly include Boost_LIBRARIES; it will be brought in.
set(LIBS_COMMON
    ${ASYNC}
    ${CONCURRENCY}
    Folly::folly
    ${FOLLY_EXCEPTION_TRACER}
    fmt::fmt
    gflags
    glog::glog
    ${RE2}
    ${IBVERBS_LIBRARIES}
    -lpthread)

# Generate thrift interface
add_library(rdma_info_thrift)
thrift_generate(
  rdma_info_thrift rdma_gen/if/RdmaInfo.thrift
  LANGUAGE cpp2
  SERVICES RdmaGenControlService
  OPTIONS json stack_arguments
  INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR} ${THRIFT_INCLUDE_DIR})
target_include_directories(rdma_info_thrift
                           PUBLIC $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>)
target_link_libraries(rdma_info_thrift PUBLIC thrift::rpc)

# The generated gen-cpp2 sources call deprecated thrift and folly APIs
# while fbthrift migrates its codegen; the warnings are not actionable
# here, so silence them on this generated-only target.
target_compile_options(rdma_info_thrift PRIVATE -Wno-deprecated-declarations)

# rdma_gen_lib links softmetaroce instead of compiling the libsoftmetaroce
# sources itself; the leaf owns that list (LSMR_LIB_SRCS), so the two can
# never drift and test builds compile each source once.
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/libsoftmetaroce/CMakeLists.txt")
  message(
    FATAL_ERROR
      "libsoftmetaroce/CMakeLists.txt is absent; "
      "the export is missing the library tree.")
endif()
add_subdirectory(libsoftmetaroce)

add_library(rdma_gen_lib ${SRC_LIB} ${SRC_LIB_GPU})
target_include_directories(rdma_gen_lib PUBLIC ${RE2_INCLUDE_DIR})
target_compile_definitions(rdma_gen_lib PUBLIC)
target_link_libraries(rdma_gen_lib ${LIBS_COMMON} ${LIBS_COMMON_GPU}
                      rdma_info_thrift)
# Plain signature to match the call above: keyword and plain signatures
# cannot mix on one target.
target_link_libraries(rdma_gen_lib libsoftmetaroce::softmetaroce)
add_executable(rdma_gen ${SRC_MAIN})
set_target_properties(rdma_gen PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
# no need to include the LIBS_COMMON and rdma_info_thrift while they
# are in rdma_gen_lib
target_link_libraries(rdma_gen rdma_gen_lib)
install(TARGETS rdma_gen)

# Leaf tests for OSS CI. getdeps passes -DBUILD_TESTS=ON for the test variant
# (see build/fbcode_builder/manifests/metaroce); a plain configure leaves it
# off and builds only rdma_gen. Strict by design: when tests are requested,
# the leaf test tree must be present, so a partial export fails configure
# loudly instead of running zero tests.
option(BUILD_TESTS "Build the libsoftmetaroce leaf tests" OFF)
if(BUILD_TESTS)
  enable_testing()
  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/libsoftmetaroce/test/CMakeLists.txt")
    message(
      FATAL_ERROR
        "BUILD_TESTS is on but libsoftmetaroce/test/CMakeLists.txt is absent; "
        "the export is missing the leaf test tree.")
  endif()
  if(LSMR_BUILD_SAMPLES AND NOT EXISTS
     "${CMAKE_CURRENT_SOURCE_DIR}/libsoftmetaroce/samples/CMakeLists.txt")
    message(
      FATAL_ERROR
        "LSMR_BUILD_SAMPLES is on but "
        "libsoftmetaroce/samples/CMakeLists.txt is absent; "
        "the export is missing the samples tree.")
  endif()
  if(LSMR_BUILD_RDMA_UNIT AND NOT EXISTS
     "${CMAKE_CURRENT_SOURCE_DIR}/libsoftmetaroce/test/rdma-unit/CMakeLists.txt")
    message(
      FATAL_ERROR
        "LSMR_BUILD_RDMA_UNIT is on but "
        "libsoftmetaroce/test/rdma-unit/CMakeLists.txt is absent; "
        "the export is missing the conformance tree.")
  endif()
endif()

if(LSMR_BUILD_PRELOAD AND NOT EXISTS
   "${CMAKE_CURRENT_SOURCE_DIR}/libsoftmetaroce/preload/CMakeLists.txt")
  message(
    FATAL_ERROR
      "LSMR_BUILD_PRELOAD is on but "
      "libsoftmetaroce/preload/CMakeLists.txt is absent; "
      "the export is missing the interposer tree.")
endif()
