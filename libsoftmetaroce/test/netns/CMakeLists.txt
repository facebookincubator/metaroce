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
# through test/, so it runs without the C++ unit tests. No rdma_gen needed yet.
# The mode is configuration, here or -DLSMR_NETNS_MODE=privileged; the probe
# never falls back between modes at runtime.
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
