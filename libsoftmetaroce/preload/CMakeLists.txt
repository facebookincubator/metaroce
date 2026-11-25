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

# LD_PRELOAD interposer presenting libsoftmetaroce as the verbs
# implementation to unmodified binaries. Added by the parent only when
# requested and present.
add_library(lsmr_preload SHARED lsmr_preload.cpp)
target_link_libraries(lsmr_preload PRIVATE libsoftmetaroce::softmetaroce)
# Only the interposed C API is callable from outside; the version script
# hides the statically linked implementation behind it.
target_link_options(
  lsmr_preload
  PRIVATE "LINKER:--version-script,${CMAKE_CURRENT_SOURCE_DIR}/lsmr_preload.map")
set_target_properties(
  lsmr_preload PROPERTIES LINK_DEPENDS
                          ${CMAKE_CURRENT_SOURCE_DIR}/lsmr_preload.map)
