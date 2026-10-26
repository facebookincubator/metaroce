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

# Test executables for the standalone libsoftmetaroce build. Added by the parent
# only when tests are requested and this tree is present.
find_package(GTest REQUIRED)
include(GoogleTest)

# One main for all tests: folly must initialize before gtest runs. Links
# folly, glog, and gtest directly (mirroring test/BUCK's folly_gtest_main)
# rather than the library, which the main never uses.
add_library(lsmr_test_main STATIC FollyGTestMain.cpp)
target_link_libraries(lsmr_test_main PRIVATE Folly::folly glog::glog
                                              GTest::gtest)

# QpTestBase.cpp compiles into the tests that use the Qp fixture (mirroring
# test/BUCK's :qp_test_base users); the mock-socket and sender helpers the
# concurrency test also uses are header-only, so they need no wiring here.
set(LSMR_QP_BASE_TESTS QpTest QpWithTagsTest RtoRepathTest CongestionControllerConcurrencyTest)

set(LSMR_TEST_SRCS
    AckedSendTest.cpp
    BaselineTest.cpp
    BaselineWithRCNTest.cpp
    BlockingCoroLockTest.cpp
    ClockSourceTest.cpp
    CodecTest.cpp
    CompletionQueueTest.cpp
    ConcurrentQpArbiterTest.cpp
    ConcurrentSendTest.cpp
    CongestionControllerConcurrencyTest.cpp
    CongestionControllerFactoryTest.cpp
    DeadlineQueueTest.cpp
    EgressProfileTest.cpp
    EsunEncapsulationTest.cpp
    EsunProfileTest.cpp
    EthernetEncapsulationTest.cpp
    FastStartTest.cpp
    FlowCountFlagsTest.cpp
    GidDiscoveryTest.cpp
    HighResTimerTest.cpp
    IcrcTest.cpp
    MemoryRegionsTest.cpp
    MetaRoceVerbsTest.cpp
    MetaRoceVerbsWithTagsTest.cpp
    ModifierTest.cpp
    MultiUnboundedQueueTest.cpp
    NakTest.cpp
    PacketModifierSocketTest.cpp
    PacketSocketFactoryTest.cpp
    PingControllerTest.cpp
    PingReceiverTest.cpp
    QpArbiterTest.cpp
    QpTest.cpp
    QpWithTagsTest.cpp
    RcnRateHintTest.cpp
    ReceiverTest.cpp
    ReceiveSequenceTest.cpp
    RenamedFlagTest.cpp
    RoceV2iCRCTest.cpp
    RtoRepathTest.cpp
    RttEstimatorTest.cpp
    SenderTest.cpp
    SgeListTest.cpp
    SharedReceiveQueueTest.cpp
    ShiftableBitsetTest.cpp
    SourcePortCounterTest.cpp
    StaticQwndTest.cpp
    StripedMessageWaveModifierTest.cpp
    TagSetTest.cpp
    TagTest.cpp
    UdpEncapsulationTest.cpp
    WatchdogTest.cpp
    WatchdogTimerTest.cpp
    WireProfilesTest.cpp
    WqeTest.cpp
    WrappingMapIteratorTest.cpp
    WrrSchedulerTest.cpp)

foreach(TEST_SRC ${LSMR_TEST_SRCS})
  get_filename_component(TEST_NAME ${TEST_SRC} NAME_WE)
  # Target names are global; prefix so add_subdirectory() cannot collide
  # with sibling targets. Ctest names still come from the gtest suites.
  set(LSMR_TEST_TARGET lsmr_${TEST_NAME})
  add_executable(${LSMR_TEST_TARGET} ${TEST_SRC})
  if(TEST_NAME IN_LIST LSMR_QP_BASE_TESTS)
    target_sources(${LSMR_TEST_TARGET} PRIVATE QpTestBase.cpp)
  endif()
  target_link_libraries(
    ${LSMR_TEST_TARGET} PRIVATE libsoftmetaroce::softmetaroce GTest::gtest
                                GTest::gmock lsmr_test_main)
  # One ctest entry per TEST_F, each in its own process: some tests
  # (ClockSourceTest) depend on fresh process state per case.
  gtest_discover_tests(${LSMR_TEST_TARGET})
endforeach()

# The interposer ABI test links the preload library directly: the test
# binary references ibv_* without linking libibverbs, so the loader binds
# every call to the interposer, the same binding LD_PRELOAD produces. It
# uses the shared plain gtest main (PreloadAbiMain.cpp, also serving the
# re-executed child mode): folly initializes through the interposer's
# own lazy path, as with a real C consumer.
if(TARGET lsmr_preload)
  add_executable(lsmr_PreloadAbiTest PreloadAbiTest.cpp PreloadAbiMain.cpp)
  target_include_directories(lsmr_PreloadAbiTest
                             PRIVATE ${IBVERBS_INCLUDE_DIRS})
  target_link_libraries(lsmr_PreloadAbiTest PRIVATE lsmr_preload
                                                    GTest::gtest)
  gtest_discover_tests(lsmr_PreloadAbiTest)
endif()
