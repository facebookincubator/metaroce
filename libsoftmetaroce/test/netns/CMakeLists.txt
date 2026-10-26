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

# Rootless or privileged namespace suite. Added by the leaf directly, not
# through test/, so it runs without the C++ unit tests. The mode is
# configuration, here or -DLSMR_NETNS_MODE=privileged; the scripts never fall
# back between modes at runtime.
set(LSMR_NETNS_MODE "rootless" CACHE STRING "Namespace execution mode")
set_property(CACHE LSMR_NETNS_MODE PROPERTY STRINGS rootless privileged)
if(NOT LSMR_NETNS_MODE MATCHES "^(rootless|privileged)$")
  message(FATAL_ERROR "LSMR_NETNS_MODE must be rootless or privileged, not "
                      "${LSMR_NETNS_MODE}.")
endif()

find_package(Python3 COMPONENTS Interpreter REQUIRED)
# iproute2 lives outside PATH on some systems; hints keep the failure a clear
# missing-tool error instead of a PATH accident.
find_program(IP_EXECUTABLE NAMES ip HINTS /usr/sbin /sbin REQUIRED)
if(LSMR_NETNS_MODE STREQUAL "privileged")
  # No configure-time sudo check: on some systems sudo is execute-only, which
  # find_program rejects, and presence alone says nothing about passwordless
  # use. The probe gates `sudo -n` at runtime and fails visibly without it.
  set(NETNS_LAUNCHER ${Python3_EXECUTABLE})
else()
  find_program(UNSHARE_EXECUTABLE NAMES unshare REQUIRED)
  set(NETNS_LAUNCHER ${UNSHARE_EXECUTABLE} -U -r -n -m ${Python3_EXECUTABLE})
endif()

add_test(NAME netns_probe
         COMMAND ${NETNS_LAUNCHER} ${CMAKE_CURRENT_SOURCE_DIR}/netns_probe.py
                 --mode=${LSMR_NETNS_MODE})
set_tests_properties(netns_probe PROPERTIES LABELS "netns" TIMEOUT 120)

# The SEND/RECV conversation needs a built rdma_gen. The root build defines
# the rdma_gen target after this directory is configured, so that path
# resolves at generation time; standalone builds must point at a binary.
# Requesting conversation coverage without an executable fails configuration;
# -DLSMR_NETNS_SMOKE=OFF selects the explicit probe-only configuration.
option(LSMR_NETNS_SMOKE "Run the SEND/RECV conversation test" ON)
if(LSMR_NETNS_SMOKE)
  set(RDMA_GEN_EXECUTABLE "" CACHE FILEPATH "rdma_gen binary for netns tests")
  if(RDMA_GEN_EXECUTABLE)
    set(NETNS_RDMA_GEN ${RDMA_GEN_EXECUTABLE})
  elseif(DEFINED LSMR_STANDALONE AND NOT LSMR_STANDALONE)
    set(NETNS_RDMA_GEN $<TARGET_FILE:rdma_gen>)
  else()
    message(FATAL_ERROR "LSMR_NETNS_SMOKE is on but no rdma_gen executable: "
      "set RDMA_GEN_EXECUTABLE, or -DLSMR_NETNS_SMOKE=OFF for probe-only.")
  endif()
  add_test(NAME netns_send_recv_smoke
           COMMAND ${NETNS_LAUNCHER}
                   ${CMAKE_CURRENT_SOURCE_DIR}/send_recv_smoke.py
                   --rdma-gen=${NETNS_RDMA_GEN}
                   --artifact-dir=${CMAKE_CURRENT_BINARY_DIR}/send_recv_smoke_artifacts
                   --mode=${LSMR_NETNS_MODE})
  set_tests_properties(netns_send_recv_smoke PROPERTIES LABELS "netns"
                       TIMEOUT 600)
  # Interrupting a conversation stops both endpoints and still writes a
  # summary; needs the same executable.
  add_test(NAME netns_interruption
           COMMAND ${NETNS_LAUNCHER}
                   ${CMAKE_CURRENT_SOURCE_DIR}/interruption_test.py
                   --rdma-gen=${NETNS_RDMA_GEN}
                   --artifact-dir=${CMAKE_CURRENT_BINARY_DIR}/interruption_artifacts
                   --mode=${LSMR_NETNS_MODE})
  set_tests_properties(netns_interruption PROPERTIES LABELS "netns"
                       TIMEOUT 300)
else()
  message(STATUS "LSMR_NETNS_SMOKE is off; probe-only namespace coverage")
endif()

# Output-format checks: no namespaces, identical in both modes.
add_test(NAME netns_output_checks
         COMMAND ${Python3_EXECUTABLE}
                 ${CMAKE_CURRENT_SOURCE_DIR}/rdmagen_output_test.py)
set_tests_properties(netns_output_checks PROPERTIES LABELS "netns" TIMEOUT 120)

# Pipe-marker regression: coalesced producer output must not strand the
# readiness marker in a buffer. No namespaces either.
add_test(NAME netns_await_marker
         COMMAND ${Python3_EXECUTABLE}
                 ${CMAKE_CURRENT_SOURCE_DIR}/await_marker_test.py)
set_tests_properties(netns_await_marker PROPERTIES LABELS "netns" TIMEOUT 120)

# Endpoint B lifecycle: sequential launches, one-at-a-time, teardown.
add_test(NAME netns_repeat_launch
         COMMAND ${NETNS_LAUNCHER}
                 ${CMAKE_CURRENT_SOURCE_DIR}/repeat_launch_test.py
                 --mode=${LSMR_NETNS_MODE})
set_tests_properties(netns_repeat_launch PROPERTIES LABELS "netns" TIMEOUT 120)
