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

#include <folly/IPAddress.h>
#include <libsoftmetaroce/codec/Packet.h>
#include <memory>

namespace libsoftmetaroce {

class PacketModifierSocket;

// A buffer and the metadata to send one packet, including its resolved send
// socket. Every field but keepBuildOrder is set by the constructor.
struct EgressPacket {
  enum class RxmitType { None, Fast, Rto };
  // Data and ACK packets travel on independent sockets so the TOS byte is set
  // once at socket creation and never per-packet.
  enum class Kind { Data, Ack };

  EgressPacket(
      std::unique_ptr<Packet> packet,
      PacketModifierSocket* socketToSendOn,
      folly::IPAddress remoteAddr,
      uint32_t qpNum,
      RxmitType rxmitType)
      : packet(std::move(packet)),
        socketToSendOn(socketToSendOn),
        remoteAddr(std::move(remoteAddr)),
        qpNum(qpNum),
        rxmitType(rxmitType) {}

  std::unique_ptr<Packet> packet;
  // Non-owning; the socket outlives the packet (owned by the Sender).
  PacketModifierSocket* const socketToSendOn;
  const folly::IPAddress remoteAddr;
  const uint32_t qpNum;
  const RxmitType rxmitType;
  // The congestion controller chose the order of flows this packet was built
  // in, so a batch holding it is sent in build order.
  bool keepBuildOrder{false};
};

} // namespace libsoftmetaroce
