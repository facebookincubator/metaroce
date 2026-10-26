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

#include <chrono>

#include <cstddef>
#include <cstdint>

namespace libsoftmetaroce {

// --- Protocol Version ---
// MetaRoCE protocol version included in all packet base headers.
inline constexpr uint8_t kMetaRoceProtocolVersion = 0;

// The sequence numbers a tag hands out before any rq_psn sets where its
// peer's numbering starts.
inline constexpr uint16_t kUnconfiguredStartingCsn = 0;

// --- QP/Tag Bit Layout ---
// A 32-bit QP number encodes an 8-bit tag in the upper byte and a 24-bit QP ID
// in the lower three bytes:  [tag(8) | qpId(24)]
inline constexpr uint32_t kQpTagMask = 0xFF000000;
inline constexpr uint32_t kQpIdMask = 0x00FFFFFF;
inline constexpr uint32_t kQpTagShift = 24;

// --- PSN Encoding (MSN / CSN) ---
// A 32-bit PSN (Packet Sequence Number) packs a 16-bit MSN in the lower half
// and a 16-bit CSN in the upper half:  [csn(16) | msn(16)]
inline constexpr uint32_t kPsnMsnMask = 0xFFFF;
inline constexpr uint32_t kPsnCsnShift = 16;

// --- MTU ---
// Default MTU size in bytes when no MTU is explicitly configured.
inline constexpr uint16_t kDefaultMtu = 1024;

// --- QP Attribute Field Widths ---
// IBTA gives ibv_qp_attr::timeout five bits for the exponent.
inline constexpr uint8_t kMaxLocalAckTimeoutExponent = 31;
// IBTA gives ibv_qp_attr::ah_attr.sl four bits for the service level.
inline constexpr uint8_t kMaxServiceLevel = 15;

// --- Local Ack Timeout Derivation ---
// IBTA's local ack timeout wait is 4.096us * 2^exponent.
inline constexpr uint64_t kLocalAckTimeoutBaseNs = 4096;
// IBTA reserves the zero exponent for "no timeout": rxe arms no retransmit
// timer at all for it, and rdma-unit-test calls it infinite.
inline constexpr uint8_t kDisabledLocalAckTimeoutExponent = 0;
// The watchdog a QP gets where its client asked for no timeout. Long enough to
// be no timeout in practice, short enough that a silent QP still ends.
inline constexpr std::chrono::milliseconds kDisabledTimeoutWatchdogMs{30000};
// IBTA gives ibv_qp_attr::retry_cnt three bits, and a count past it is not
// portable: rxe collapses it to 7 and retries forever, rdmavt refuses it.
inline constexpr uint8_t kMaxTransportRetryCount = 7;
// A wider exponent has its wait cut to this one's, bounding what a single
// attempt waits.
inline constexpr uint8_t kLocalAckTimeoutExponentCeiling = 19;

// --- Device Capability Limits ---
// Reported via ibv_query_device to emulate hardware limits.
inline constexpr uint8_t kPhysicalPortCount = 1;
inline constexpr uint32_t kMaxMemoryRegions = 16777216;
inline constexpr uint32_t kMaxCqEntries = 32768;
inline constexpr uint32_t kMaxCompletionQueues = 1048576;
inline constexpr int kCompletionVectorCount = 1;
inline constexpr uint32_t kMaxQueuePairs = 1048576;
inline constexpr uint32_t kMaxQpWorkRequests = 32768;
inline constexpr uint32_t kMaxSgesPerWorkRequest = 8;
// Outstanding RDMA reads and atomics per queue pair: the ceiling of the uint8_t
// field the verbs attributes use, since lsmr keeps no responder table.
inline constexpr uint8_t kMaxOutstandingReadAtomic = 255;

// --- Queue Pair Creation ---
// Applied by ibv_create_qp to its attributes, and reported by no device field.
inline constexpr uint32_t kMaxInlineDataBytes = 512;
inline constexpr uint32_t kDefaultQpWorkRequests = 32;

// --- Completion Queue ---
// Zero is left unissued, so a zeroed struct matches no queue.
inline constexpr uint32_t kFirstCompletionQueueHandle = 1;

// --- Shared Receive Queue ---
inline constexpr uint32_t kFirstSharedReceiveQueueHandle = 1;
inline constexpr uint32_t kMaxSrqWorkRequests = kMaxQpWorkRequests;
inline constexpr uint32_t kMaxSharedReceiveQueues = 1024;

// --- Protection Domain ---
inline constexpr uint32_t kMaxProtectionDomains = 65536;
// Sit far from the values a zeroed or foreign struct is likely to hold.
inline constexpr uint32_t kProtectionDomainMagic = 0xFFFF0001;
inline constexpr uint32_t kQueuePairMagic = 0xFFFF0002;

// --- Timing / RTO ---
// Initial mean deviation of RTT estimate (1ms in nanoseconds).
inline constexpr uint32_t kInitialMdevRttNs = 1'000'000;
// A tag's failures draw one NAK per interval: longer than an in-flight window
// takes to arrive, shorter than a requester's initial RTO.
inline constexpr uint64_t kErrorNakIntervalNs = 1'000'000;
// Slow-receiver mode: inflated mdev to produce ~1s initial RTO.
inline constexpr uint32_t kSlowReceiverMdevRttNs = 250'000'000;
// Maximum RTO for slow receivers (2s in nanoseconds).
inline constexpr unsigned long kSlowReceiverMaxRtoNs = 2'000'000'000UL;
// Cap on the RTO exponential backoff exponent.
inline constexpr unsigned int kMaxRtoBackoffExponent = 16;
// Nanoseconds per microsecond, used for RTO unit conversion.
inline constexpr uint32_t kNsPerUs = 1000;

// --- Window / Transmission Limits ---
// If the in-flight window (sndNxt - sndUna) is below this threshold and the CC
// window has room, skip RTO retransmission and send new data instead.
inline constexpr uint16_t kRtoSkipWindowThreshold = 128;
// Sanity-check upper bound on tracker offset during retransmission.
inline constexpr uint16_t kMaxTrackerSize = 1024;
// An ACK whose measured RTT exceeds estRtt * this multiplier is considered
// stale and discarded to protect against wrapped sequence numbers.
inline constexpr uint32_t kStaleAckRttMultiplier = 64;

// --- Congestion Notification ---
// The largest CE-marked count an ack's four-bit CNP field can express.
inline constexpr uint8_t kMaxCnpCeCount = 15;
// The count at which the receiver enqueues an ack reporting and clearing it,
// leaving the field headroom for marks landing before that ack goes out.
inline constexpr uint8_t kCeCountAckThreshold = 8;

// --- Payload Alignment ---
// MetaRoCE payloads are padded to this byte boundary on the last packet.
inline constexpr uint32_t kPayloadAlignmentBytes = 4;

// --- Wire sizes shared by the framings ---
inline constexpr size_t kEthernetHeaderSize = 14;
// Ethernet's 64-byte minimum includes the 4-byte FCS, which is absent from
// buffers passed to an AF_PACKET socket.
inline constexpr size_t kMinimumEthernetFrameSizeBytes = 60;

// --- Networking ---
// Maximum receive buffer size for any socket type (UDP, raw Ethernet).
inline constexpr size_t kMaxPacketSizeBytes = 9000;
inline constexpr int kFastRetransmitHopLimit = 192;
// Conservative cap on segments coalesced into one writeGSO (the historical
// UDP_MAX_SEGMENTS; newer kernels allow more).
inline constexpr uint16_t kGsoMaxSegments = 64;
// A UDP GSO call's pre-segmentation payload must fit one IPv6 datagram
// (<= 65535 bytes); 60000 leaves margin for framing and the UDP header.
inline constexpr size_t kMaxGsoBytes = 60000;
// Default MetaRoCE ethertype for direct L2 framing.
// 0x88B6 is an IEEE 802 "Local Experimental" ethertype (IEEE Std 802-2014,
// Table 9-1) reserved for prototype and vendor-specific use.
inline constexpr uint16_t kDefaultMetaRoceEthertype = 0x88B6;
// Provisional ESUN EtherType used until an IEEE assignment is available.
inline constexpr uint16_t kProvisionalEsunEthertype = 0xFC0E;

// --- Class of Service / DSCP ---
// Default DSCP code points (6-bit, 0-63) for the per-CoS sender sockets.
// Data uses CS3 (24); ACKs use EF (46). Override via gflags.
inline constexpr uint8_t kDefaultDataDscp = 24; // CS3
inline constexpr uint8_t kDefaultAckDscp = 46; // EF

// ESUN CoS values (3-bit, 0-7) are independent of IP DSCP.
inline constexpr uint8_t kDefaultEsunDataCos = 3;
inline constexpr uint8_t kDefaultEsunAckCos = 5;

// --- Unit Conversions ---
inline constexpr uint64_t kNsPerSecond = 1'000'000'000ULL;

// --- MTU ---
// IBV MTU enum to bytes: mtu_bytes = 1 << (enum_value + kMtuEnumShift).
// Enum 1 = 256 bytes, Enum 5 = 4096 bytes.
inline constexpr uint8_t kMtuEnumShift = 7;

// --- RNR Backoff ---
// Base time unit (microseconds) for RNR backoff: backoff_us = kRnrBackoffBaseUs
// << exponent.
inline constexpr uint32_t kRnrBackoffBaseUs = 5;
// This is stored as the number of times to left-shift 5 usec, which here is 10.
// 0x1f is a mask to restrict this to the five least significant bits.
// Requested backoff is 5*2^10 usec = 5.120ms
inline constexpr uint8_t kDefaultRnrBackoffTimeExp = 10 & 0x1f;

// --- Atomic Operations ---
// All atomic operations (fetch-add, compare-swap) operate on exactly this many
// bytes.
inline constexpr uint32_t kAtomicOperationSizeBytes = 8;

// --- GID Transport Types ---
// MetaRoCE transport types stored as gid_type in ibv_gid_entry.
// Values are intentional literals to avoid coupling Constants.h to ibverbs.
inline constexpr uint32_t kGidTypeMetaRoceUdp =
    2; // equal to IBV_GID_TYPE_ROCE_V2
inline constexpr uint32_t kGidTypeMetaRoceEthernet = 100;

inline constexpr size_t kMaxPorts = 16;

// A tag number is a uint8_t, so this bounds any per-tag set.
inline constexpr size_t kMetaRoceTagCount = 256;

} // namespace libsoftmetaroce
