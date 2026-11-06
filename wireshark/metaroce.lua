-- Copyright (c) Meta Platforms, Inc. and affiliates.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-----------------------------------------------------------------------
-- MetaRoCE protocol dissector for Wireshark
-- See wireshark-metaroce-readme.md for more information
--
-- WARNING: A C dissector reimplementation lives in c_dissector/.
-- When adding or modifying protocol fields here, please also update
-- c_dissector/packet-metaroce.c so both dissectors stay in sync.
-- The CI test c_dissector:field_parity_test will FAIL if the C
-- dissector is missing fields registered by this Lua plugin.
------------------------------------------------------------------------
-- Ignore init errors related to Wireshark
--  @lint-ignore-every LUA_LUAJIT

local debug_level = {
   DISABLED = 0,
   ENABLED  = 1,
}

local default_settings = {
   debug_level  = debug_level.DISABLED,
   -- port number "0xface" == 64206 has been used for testing so far
   port_debug   = 64206,
   -- port number "0x0ace" == 2766 is the default MetaRoCE udp port
   port_real    = 2766,
   heur_enabled = false,
}

local dprint = function() end

local function reset_debug_level()
   if default_settings.debug_level ~= debug_level.DISABLED then
      dprint = function(...)
         print(table.concat({"Lua:", ...}," "))
      end
   end
end

reset_debug_level()
dprint("Wireshark version = ", get_version())
dprint("Lua version = ", _VERSION)

----------------------------------------
-- Verify wireshark version
local major, minor, micro = get_version():match("(%d+)%.(%d+)%.(%d+)")
if major and tonumber(major) <= 1
   and ((tonumber(minor) <= 10)
      or (tonumber(minor) == 11 and tonumber(micro) < 3))
then
   error(  "Sorry, but your Wireshark/Tshark version ("
           .. get_version()
           .. ") is too old for this script!\n"
           .. "This script needs Wireshark/Tshark version 1.11.3 or higher.\n")
end

-- Verify we have the ProtoExpert class in wireshark, as that's the newest
-- thing this file uses
assert(ProtoExpert.new, "Wireshark does not have the ProtoExpert class,"
       .. "so it's too old - get the latest 1.11.3 or higher")
----------------------------------------
-- MetaRoCE protocol constants
-- Field lengths in 8-bit bytes
local U8_LEN  = 1
local U16_LEN = 2
local U32_LEN = 4
local U64_LEN = 8

-- Packet header lengths (in number of bytes) for mandatory data fields
local mr_header = {
   BASE_LEN = 12,
   DATA_LEN = 20,
   IMMD_LEN = 4,
   RDWR_LEN = 16,
   CTRL_LEN = 8,
   CTRL_ECHO_LEN = 8,
   SAETH_LEN     = 20,
   SAETH_FSN_LEN = 32,
   SAETH_RNR_LEN = 32,
   RNRC_LEN      = 8,
   ATOMICETH_LEN = 28,
   ATOMICACKETH_LEN = 8,
}

-- Base header field offsets
local mr_base_offset = {
   OPCODE  = 0,
   RSVD1   = 1,
   VERSION = 2,
   PATHID  = 2,
   DEST_QP = 4,
   SRC_QP  = 8,
}

-- Data header (METH) field offsets in bytes
local mr_meth_offset = {
   PATH_FSN   = 0,
   PATH_UNA   = 2,
   TAG        = 4,
   A          = 5,
   R          = 5,
   T          = 5,
   RESERVED1  = 5,
   PADDING    = 5,
   POSN       = 6,
   CSN        = 8,
   MSN        = 10,
   RATE_HINTS = 12,
   TIMESTAMP  = 16,
}

-- Immediate header for Send or Read/Write
local MR_IMMD           = 0

-- Read/Write header (RETH) field offsets in bytes
local mr_reth_offset = {
   MSG_ADDR = 0,
   R_KEY    = 8,
   MSG_LEN  = 12,
}

-- Control header field offsets in bytes
local mr_ctrl_offset = {
   TYPE       = 0,
   RESERVED1  = 2,
   TIMESTAMP  = 4,
   PKT_DATA   = 8,
}
local mr_ctrl_echo_offset = {
   TAG           = 0,
   ERROR_STATE   = 1,
   SUPP_PATH_CNT = 2,
   RSVD1         = 4,
   ACTIVE_PORT_SET = 6,
}
--- Control header constants
local CTRL_TYPE_ECHO          = 0
local CTRL_TYPE_ECHO_RESPONSE = 1

--- Ack header field offsets in bytes
local mr_saeth_offset = {
   RESERVED1  = 0,
   CNP        = 0,
   TAG        = 1,
   C_FSN      = 2,
   MAX_R_FSN  = 4,
   STATUS     = 6,
   RESERVED2  = 7,
   CRD_MSN    = 8,
   ACTIVE_PORT_SET = 10,
   RATE_HINTS = 12,
   ECHO_TIMESTAMP = 16,
}

local mr_saeth_fsn_offset = {
   VEC0   = 0, -- [31:0]
   VEC1   = 4, -- [63:32]
   VEC2   = 8, -- [95:64]
   VEC3   = 12, -- [127:96]
   VEC4   = 16, -- [159:128]
   VEC5   = 20, -- [191:160]
   VEC6   = 24, -- [223:192]
   VEC7   = 28, -- [255:224]
}

local mr_saeth_rnr_offset = {
   RNR0   = 0,
   RNR1   = 4,
   RNR2   = 8,
   RNR3   = 12,
   RNR4   = 16,
   RNR5   = 20,
   RNR6   = 24,
   RNR7   = 28,
}

-- Interpretation of field values
local MR_SAETH_STATUS_ACK      = 0
local MR_SAETH_STATUS_HRNR_NAK = 1
local MR_SAETH_STATUS_BRNR_NAK = 2
local MR_SAETH_STATUS_NAK      = 3

-- RNRCancel header field offsets in bytes
local mr_rnrc_offset = {
   PATHS_AVAIL    = 0,
   TAG            = 1,
   PCSN           = 2,
   RATE_HINTS     = 4,
}

-- AtomicETH header
local mr_atomiceth_offset = {
   VADDR     = 0,
   R_KEY     = 8,
   SWAP_DATA = 12,
   COMP_DATA = 20,
}

-- AtomicEthAck header
local MR_ATOMICACKETH_ORIG_REMOTE_DATA = 0

----------------------------------------
-- Creates the MetaRoCE Proto object.
-- Use pcall to detect if the C dissector plugin (metaroce.so) is already
-- loaded.  When the C plugin is present it registers the same protocol name
-- and description, causing Proto.new() to fail.  In that case, tell the
-- developer how to bypass the C plugin so this Lua script takes over.
local ok, metaroce = pcall(Proto.new, "metaroce", "MetaRoCE Protocol v0.95")
if not ok then
   local script_path = debug.getinfo(1, "S").source:sub(2)
   error("\n\nThe MetaRoCE C dissector (metaroce.so) is already loaded and "
         .. "registers the same protocol/field names.\nTo use this Lua "
         .. "dissector for development, prevent the C plugin from loading "
         .. "by running:\n\n"
         .. "  mkdir -p /tmp/ws-noplugins && WIRESHARK_PLUGIN_DIR="
         .. "/tmp/ws-noplugins \\\n"
         .. "    tshark -X lua_script:" .. script_path .. " ...\n\n"
         .. "Or use the wrapper: metaroce-parse-lua.sh -r capture.pcap\n")
end
metaroce.fields = {}

-- Base header
local pf_mr_base = ProtoField.new("Base", "metaroce.base",
                                  ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_base)
-- Data header (METH)
local pf_mr_data = ProtoField.new("Data", "metaroce.data",
                                  ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_data)
-- Read/Write header (RETH)
local pf_mr_rdwr = ProtoField.new("Read/Write", "metaroce.rdwr",
                                  ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_rdwr)
-- Immediate header (ImmDt)
local pf_mr_immd = ProtoField.new("Immediate", "metaroce.immd",
                                 ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_immd)
-- Ack header (SAETH)
local pf_mr_saeth = ProtoField.new("SelAck", "metaroce.saeth",
                                 ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_saeth)
-- Control header (Control)
local pf_mr_ctrl = ProtoField.new("Control", "metaroce.ctrl",
                                  ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_ctrl)
-- Echo header (Echo)
local pf_mr_ctrl_echo = ProtoField.new("Echo", "metaroce.echo",
                                  ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_ctrl_echo)
-- RNR Cancel (RNR-Cancel)
local pf_mr_rnrc = ProtoField.new("RNR-Cancel", "metaroce.rnr",
                                  ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_rnrc)
-- Payload data
local pf_mr_payload = ProtoField.new("Payload", "metaroce.payload",
                                  ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_payload)
-- AtomicETH
local pf_mr_atomiceth = ProtoField.new("AtomicETH", "metaroce.atomiceth",
                                       ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_atomiceth)
-- AtomicAckETH
local pf_mr_atomicacketh = ProtoField.new("AtomicAckETH",
                                          "metaroce.atomicacketh",
                                          ftypes.BYTES, nil, base.NONE)
table.insert(metaroce.fields, pf_mr_atomicacketh)
-- CRC
local pf_mr_crc = ProtoField.new("CRC", "metaroce.crc",
                                    ftypes.UINT32, nil, base.HEX)
table.insert(metaroce.fields, pf_mr_crc)

-- CRC Status
local crc_status_name = {
   [0] = "Good",
   [1] = "Bad",
}
local pf_mr_crc_status = ProtoField.new("CRC Status", "metaroce.crc.status",
                                       ftypes.UINT8, crc_status_name, base.DEC)
table.insert(metaroce.fields, pf_mr_crc_status)

-- Expert info for CRC validation
local ef_crc_bad = ProtoExpert.new("metaroce.crc.bad", "Bad CRC",
                                   expert.group.CHECKSUM, expert.severity.ERROR)
local ef_crc_good = ProtoExpert.new("metaroce.crc.good", "Good CRC",
                                    expert.group.CHECKSUM, expert.severity.CHAT)
metaroce.experts = {ef_crc_bad, ef_crc_good}

-- Conversation tracking fields
local pf_mr_conv_id = ProtoField.uint32("metaroce.conv_id",
                                        "Conversation ID", base.DEC)
table.insert(metaroce.fields, pf_mr_conv_id)

local pf_mr_conv = ProtoField.string("metaroce.conv", "Conversation")
table.insert(metaroce.fields, pf_mr_conv)

-- Conversation tracking state
-- Maps canonical key -> conversation ID
local conv_table = {}
-- Maps packet number -> {id, str} for stable re-dissection
local pkt_conv_cache = {}
-- Next conversation ID to assign
local conv_next_id = 0

-- The two endpoints are sorted to ensure A->B and B->A yield
-- the same conversation.  Sorting raw byte ordering to avoid
-- locale-dependent string collation.
local function conv_endpoint_less(ip_a, ip_b)
   local len = math.min(#ip_a, #ip_b)
   for i = 1, len do
      local ba = string.byte(ip_a, i)
      local bb = string.byte(ip_b, i)
      if ba ~= bb then
         return ba < bb
      end
   end
   return #ip_a < #ip_b
end

-- Build a direction-independent str from IP addresses for both key & display.
-- QP numbers. The key is used to look up the conversation ID.
local function conv_make_str(src_ip, dst_ip, src_qp, dst_qp)
   local ip_lo, qp_lo, ip_hi, qp_hi
   if conv_endpoint_less(src_ip, dst_ip) then
      ip_lo, qp_lo, ip_hi, qp_hi = src_ip, src_qp, dst_ip, dst_qp
   else
      ip_lo, qp_lo, ip_hi, qp_hi = dst_ip, dst_qp, src_ip, src_qp
   end
   local a = ip_lo .. "#qp:" .. qp_lo
   local b = ip_hi .. "#qp:" .. qp_hi
   return a .. "<->" .. b
end

-- Reset conversation state when a capture file is (re)loaded
function metaroce.init()
   conv_table = {}
   pkt_conv_cache = {}
   conv_next_id = 0
end

-- CRC-32 calculation function (IEEE 802.3 polynomial)
-- table-based implementation
local crc32_table = {}

-- using bitop library for compatibility with lua 5.2 and 5.4 documented
-- https://www.wireshark.org/docs/wsdg_html_chunked/_bitwise_operations.html

-- Initialize CRC-32 lookup table
local function init_crc32_table()
   local poly = 0xEDB88320
   for i = 0, 255 do
      local crc = i
      for j = 0, 7 do
         if bit.band(crc, 1) == 1 then
            crc = bit.bxor(bit.rshift(crc, 1), poly)
         else
            crc = bit.rshift(crc, 1)
         end
      end
      crc32_table[i] = crc
   end
end

-- Fast CRC-32 calculation using precomputed table
local function crc32(data)
   if #crc32_table == 0 then
      init_crc32_table()
   end

   local crc = 0xFFFFFFFF

   for i = 0, data:len() - 1 do
      local byte = data:get_index(i)
      local table_index = bit.band(bit.bxor(crc, byte), 0xFF)
      crc = bit.bxor(bit.rshift(crc, 8), crc32_table[table_index])
   end

   return bit.bxor(crc, 0xFFFFFFFF)
end

-- Wrapping subtraction in the 16-bit FSN/MSN sequence space: b - a.
local function seq16_sub(b, a)
   return bit.band(b - a, 0xFFFF)
end

-- Base header fields (v0.92)
-- Note! Changing any of these field names and indexes will impact
-- decoding below
local opcode_name = {
   [0x00] = "RoCEv2 RC", [0x01] = "RoCEv2 RC", [0x02] = "RoCEv2 RC",
   [0x03] = "RoCEv2 RC", [0x04] = "RoCEv2 RC", [0x05] = "RoCEv2 RC",
   [0x06] = "RoCEv2 RC", [0x07] = "RoCEv2 RC", [0x08] = "RoCEv2 RC",
   [0x09] = "RoCEv2 RC", [0x0a] = "RoCEv2 RC", [0x0b] = "RoCEv2 RC",
   [0x0c] = "RoCEv2 RC", [0x0d] = "RoCEv2 RC", [0x0e] = "RoCEv2 RC",
   [0x0f] = "RoCEv2 RC", [0x10] = "RoCEv2 RC", [0x11] = "RoCEv2 RC",
   [0x12] = "RoCEv2 RC", [0x13] = "RoCEv2 RC", [0x14] = "RoCEv2 RC",
   [0x15] = "RoCEv2 RC", [0x16] = "RoCEv2 RC", [0x17] = "RoCEv2 RC",
   [0x18] = "RoCEv2 RC", [0x19] = "RoCEv2 RC", [0x1a] = "RoCEv2 RC",
   [0x1b] = "RoCEv2 RC", [0x1c] = "RoCEv2 RC", [0x1d] = "RoCEv2 RC",
   [0x1e] = "RoCEv2 RC", [0x1f] = "RoCEv2 RC", [0x20] = "RoCEv2 RC",
   [0x21] = "RoCEv2 RC", [0x22] = "RoCEv2 RC", [0x23] = "RoCEv2 RC",
   [0x24] = "RoCEv2 RC", [0x25] = "RoCEv2 RC", [0x26] = "RoCEv2 RC",
   [0x27] = "RoCEv2 RC", [0x28] = "RoCEv2 RC", [0x29] = "RoCEv2 RC",
   [0x2a] = "RoCEv2 RC", [0x2b] = "RoCEv2 RC", [0x2c] = "RoCEv2 RC",
   [0x2d] = "RoCEv2 RC", [0x2e] = "RoCEv2 RC", [0x2f] = "RoCEv2 RC",
   [0x30] = "RoCEv2 RC", [0x31] = "RoCEv2 RC", [0x32] = "RoCEv2 RC",
   [0x33] = "RoCEv2 RC", [0x34] = "RoCEv2 RC", [0x35] = "RoCEv2 RC",
   [0x36] = "RoCEv2 RC", [0x37] = "RoCEv2 RC", [0x38] = "RoCEv2 RC",
   [0x39] = "RoCEv2 RC", [0x3a] = "RoCEv2 RC", [0x3b] = "RoCEv2 RC",
   [0x3c] = "RoCEv2 RC", [0x3d] = "RoCEv2 RC", [0x3e] = "RoCEv2 RC",
   [0x3f] = "RoCEv2 RC", [0x40] = "RoCEv2 RC", [0x41] = "RoCEv2 RC",
   [0x42] = "RoCEv2 RC", [0x43] = "RoCEv2 RC", [0x44] = "RoCEv2 RC",
   [0x45] = "RoCEv2 RC", [0x46] = "RoCEv2 RC", [0x47] = "RoCEv2 RC",
   [0x48] = "RoCEv2 RC", [0x49] = "RoCEv2 RC", [0x4a] = "RoCEv2 RC",
   [0x4b] = "RoCEv2 RC", [0x4c] = "RoCEv2 RC", [0x4d] = "RoCEv2 RC",
   [0x4e] = "RoCEv2 RC", [0x4f] = "RoCEv2 RC", [0x50] = "RoCEv2 RC",
   [0x51] = "RoCEv2 RC", [0x52] = "RoCEv2 RC", [0x53] = "RoCEv2 RC",
   [0x54] = "RoCEv2 RC", [0x55] = "RoCEv2 RC", [0x56] = "RoCEv2 RC",
   [0x57] = "RoCEv2 RC", [0x58] = "RoCEv2 RC", [0x59] = "RoCEv2 RC",
   [0x5a] = "RoCEv2 RC", [0x5b] = "RoCEv2 RC", [0x5c] = "RoCEv2 RC",
   [0x5d] = "RoCEv2 RC", [0x5e] = "RoCEv2 RC", [0x5f] = "RoCEv2 RC",
   [0x60] = "RoCEv2 RC", [0x61] = "RoCEv2 RC", [0x62] = "RoCEv2 RC",
   [0x63] = "RoCEv2 RC", [0x64] = "RoCEv2 RC", [0x65] = "RoCEv2 RC",
   [0x66] = "RoCEv2 RC", [0x67] = "RoCEv2 RC", [0x68] = "RoCEv2 RC",
   [0x69] = "RoCEv2 RC", [0x6a] = "RoCEv2 RC", [0x6b] = "RoCEv2 RC",
   [0x6c] = "RoCEv2 RC", [0x6d] = "RoCEv2 RC", [0x6e] = "RoCEv2 RC",
   [0x6f] = "RoCEv2 RC", [0x70] = "RoCEv2 RC", [0x71] = "RoCEv2 RC",
   [0x72] = "RoCEv2 RC", [0x73] = "RoCEv2 RC", [0x74] = "RoCEv2 RC",
   [0x75] = "RoCEv2 RC", [0x76] = "RoCEv2 RC", [0x77] = "RoCEv2 RC",
   [0x78] = "RoCEv2 RC", [0x79] = "RoCEv2 RC", [0x7a] = "RoCEv2 RC",
   [0x7b] = "RoCEv2 RC", [0x7c] = "RoCEv2 RC", [0x7d] = "RoCEv2 RC",
   [0x7e] = "RoCEv2 RC", [0x7f] = "RoCEv2 RC", [0x80] = "RoCEv2 UD",
   [0x81] = "RoCEv2 UD", [0x82] = "RoCEv2 UD", [0x83] = "RoCEv2 UD",
   [0x84] = "RoCEv2 UD", [0x85] = "RoCEv2 UD", [0x86] = "RoCEv2 UD",
   [0x87] = "RoCEv2 UD", [0x88] = "RoCEv2 UD", [0x89] = "RoCEv2 UD",
   [0x8a] = "RoCEv2 UD", [0x8b] = "RoCEv2 UD", [0x8c] = "RoCEv2 UD",
   [0x8d] = "RoCEv2 UD", [0x8e] = "RoCEv2 UD", [0x8f] = "RoCEv2 UD",
   [0x90] = "RoCEv2 UD", [0x91] = "RoCEv2 UD", [0x92] = "RoCEv2 UD",
   [0x93] = "RoCEv2 UD", [0x94] = "RoCEv2 UD", [0x95] = "RoCEv2 UD",
   [0x96] = "RoCEv2 UD", [0x97] = "RoCEv2 UD", [0x98] = "RoCEv2 UD",
   [0x99] = "RoCEv2 UD", [0x9a] = "RoCEv2 UD", [0x9b] = "RoCEv2 UD",
   [0x9c] = "RoCEv2 UD", [0x9d] = "RoCEv2 UD", [0x9e] = "RoCEv2 UD",
   [0x9f] = "RoCEv2 UD", [0xa0] = "RoCEv2 UD", [0xa1] = "RoCEv2 UD",
   [0xa2] = "RoCEv2 UD", [0xa3] = "RoCEv2 UD", [0xa4] = "RoCEv2 UD",
   [0xa5] = "RoCEv2 UD", [0xa6] = "RoCEv2 UD", [0xa7] = "RoCEv2 UD",
   [0xa8] = "RoCEv2 UD", [0xa9] = "RoCEv2 UD", [0xaa] = "RoCEv2 UD",
   [0xab] = "RoCEv2 UD", [0xac] = "RoCEv2 UD", [0xad] = "RoCEv2 UD",
   [0xae] = "RoCEv2 UD", [0xaf] = "RoCEv2 UD", [0xb0] = "RoCEv2 UD",
   [0xb1] = "RoCEv2 UD", [0xb2] = "RoCEv2 UD", [0xb3] = "RoCEv2 UD",
   [0xb4] = "RoCEv2 UD", [0xb5] = "RoCEv2 UD", [0xb6] = "RoCEv2 UD",
   [0xb7] = "RoCEv2 UD", [0xb8] = "RoCEv2 UD", [0xb9] = "RoCEv2 UD",
   [0xba] = "RoCEv2 UD", [0xbb] = "RoCEv2 UD", [0xbc] = "RoCEv2 UD",
   [0xbd] = "RoCEv2 UD", [0xbe] = "RoCEv2 UD", [0xbf] = "RoCEv2 UD",
   [0xc0] = "Send",
   [0xc1] = "Send with Immediate Present",
   [0xc2] = "RDMA Write with Immediate Present",
   [0xc3] = "Reserved", [0xc4] = "Send Last", [0xc5] = "Reserved",
   [0xc6] = "RDMA Write",
   [0xc7] = "RDMA Write with Immediate",
   [0xc8] = "Reserved", [0xc9] = "Reserved", [0xca] = "Reserved",
   [0xcb] = "Reserved",
   [0xcc] = "RDMA Read Request",
   [0xcd] = "RDMA Read Request with Immediate Present",
   [0xce] = "Reserved",
   [0xcf] = "RDMA Read Response",
   [0xd0] = "RNR-Cancel, Receiver is Ready",
   [0xd1] = "Selective Ack",
   [0xd2] = "Atomic Ack (Response)",
   [0xd3] = "CmpSWap",
   [0xd4] = "FetchAdd",
   [0xd5] = "CmpSWap",
   [0xd6] = "Reserved for Atomics 2",
   [0xd7] = "Reserved for Atomics 3",
   [0xd8] = "Reserved", [0xd9] = "Reserved", [0xda] = "Reserved",
   [0xdb] = "Reserved", [0xdc] = "Reserved", [0xdd] = "Reserved",
   [0xde] = "Reliable Control",
   [0xdf] = "Unreliable Control",
   [0xe0] = "Reserved", [0xe1] = "Reserved", [0xe2] = "Reserved",
   [0xe3] = "Reserved", [0xe4] = "Reserved", [0xe5] = "Reserved",
   [0xe6] = "Reserved", [0xe7] = "Reserved", [0xe8] = "Reserved",
   [0xe9] = "Reserved", [0xea] = "Reserved", [0xeb] = "Reserved",
   [0xec] = "Reserved", [0xed] = "Reserved", [0xee] = "Reserved",
   [0xef] = "Reserved", [0xf0] = "Reserved", [0xf1] = "Reserved",
   [0xf2] = "Reserved", [0xf3] = "Reserved", [0xf4] = "Reserved",
   [0xf5] = "Reserved", [0xf6] = "Reserved", [0xf7] = "Reserved",
   [0xf8] = "Reserved", [0xf9] = "Reserved", [0xfa] = "Reserved",
   [0xfb] = "Reserved", [0xfc] = "Reserved", [0xfd] = "Reserved",
   [0xfe] = "Reserved", [0xff] = "Reserved",
}
local opcode_abbr = {
   [0x00] = "Rv2", [0x01] = "Rv2", [0x02] = "Rv2",
   [0x03] = "Rv2", [0x04] = "Rv2", [0x05] = "Rv2",
   [0x06] = "Rv2", [0x07] = "Rv2", [0x08] = "Rv2",
   [0x09] = "Rv2", [0x0a] = "Rv2", [0x0b] = "Rv2",
   [0x0c] = "Rv2", [0x0d] = "Rv2", [0x0e] = "Rv2",
   [0x0f] = "Rv2", [0x10] = "Rv2", [0x11] = "Rv2",
   [0x12] = "Rv2", [0x13] = "Rv2", [0x14] = "Rv2",
   [0x15] = "Rv2", [0x16] = "Rv2", [0x17] = "Rv2",
   [0x18] = "Rv2", [0x19] = "Rv2", [0x1a] = "Rv2",
   [0x1b] = "Rv2", [0x1c] = "Rv2", [0x1d] = "Rv2",
   [0x1e] = "Rv2", [0x1f] = "Rv2", [0x20] = "Rv2",
   [0x21] = "Rv2", [0x22] = "Rv2", [0x23] = "Rv2",
   [0x24] = "Rv2", [0x25] = "Rv2", [0x26] = "Rv2",
   [0x27] = "Rv2", [0x28] = "Rv2", [0x29] = "Rv2",
   [0x2a] = "Rv2", [0x2b] = "Rv2", [0x2c] = "Rv2",
   [0x2d] = "Rv2", [0x2e] = "Rv2", [0x2f] = "Rv2",
   [0x30] = "Rv2", [0x31] = "Rv2", [0x32] = "Rv2",
   [0x33] = "Rv2", [0x34] = "Rv2", [0x35] = "Rv2",
   [0x36] = "Rv2", [0x37] = "Rv2", [0x38] = "Rv2",
   [0x39] = "Rv2", [0x3a] = "Rv2", [0x3b] = "Rv2",
   [0x3c] = "Rv2", [0x3d] = "Rv2", [0x3e] = "Rv2",
   [0x3f] = "Rv2", [0x40] = "Rv2", [0x41] = "Rv2",
   [0x42] = "Rv2", [0x43] = "Rv2", [0x44] = "Rv2",
   [0x45] = "Rv2", [0x46] = "Rv2", [0x47] = "Rv2",
   [0x48] = "Rv2", [0x49] = "Rv2", [0x4a] = "Rv2",
   [0x4b] = "Rv2", [0x4c] = "Rv2", [0x4d] = "Rv2",
   [0x4e] = "Rv2", [0x4f] = "Rv2", [0x50] = "Rv2",
   [0x51] = "Rv2", [0x52] = "Rv2", [0x53] = "Rv2",
   [0x54] = "Rv2", [0x55] = "Rv2", [0x56] = "Rv2",
   [0x57] = "Rv2", [0x58] = "Rv2", [0x59] = "Rv2",
   [0x5a] = "Rv2", [0x5b] = "Rv2", [0x5c] = "Rv2",
   [0x5d] = "Rv2", [0x5e] = "Rv2", [0x5f] = "Rv2",
   [0x60] = "Rv2", [0x61] = "Rv2", [0x62] = "Rv2",
   [0x63] = "Rv2", [0x64] = "Rv2", [0x65] = "Rv2",
   [0x66] = "Rv2", [0x67] = "Rv2", [0x68] = "Rv2",
   [0x69] = "Rv2", [0x6a] = "Rv2", [0x6b] = "Rv2",
   [0x6c] = "Rv2", [0x6d] = "Rv2", [0x6e] = "Rv2",
   [0x6f] = "Rv2", [0x70] = "Rv2", [0x71] = "Rv2",
   [0x72] = "Rv2", [0x73] = "Rv2", [0x74] = "Rv2",
   [0x75] = "Rv2", [0x76] = "Rv2", [0x77] = "Rv2",
   [0x78] = "Rv2", [0x79] = "Rv2", [0x7a] = "Rv2",
   [0x7b] = "Rv2", [0x7c] = "Rv2", [0x7d] = "Rv2",
   [0x7e] = "Rv2", [0x7f] = "Rv2", [0x80] = "Ru2",
   [0x81] = "Ru2", [0x82] = "Ru2", [0x83] = "Ru2",
   [0x84] = "Ru2", [0x85] = "Ru2", [0x86] = "Ru2",
   [0x87] = "Ru2", [0x88] = "Ru2", [0x89] = "Ru2",
   [0x8a] = "Ru2", [0x8b] = "Ru2", [0x8c] = "Ru2",
   [0x8d] = "Ru2", [0x8e] = "Ru2", [0x8f] = "Ru2",
   [0x90] = "Ru2", [0x91] = "Ru2", [0x92] = "Ru2",
   [0x93] = "Ru2", [0x94] = "Ru2", [0x95] = "Ru2",
   [0x96] = "Ru2", [0x97] = "Ru2", [0x98] = "Ru2",
   [0x99] = "Ru2", [0x9a] = "Ru2", [0x9b] = "Ru2",
   [0x9c] = "Ru2", [0x9d] = "Ru2", [0x9e] = "Ru2",
   [0x9f] = "Ru2", [0xa0] = "Ru2", [0xa1] = "Ru2",
   [0xa2] = "Ru2", [0xa3] = "Ru2", [0xa4] = "Ru2",
   [0xa5] = "Ru2", [0xa6] = "Ru2", [0xa7] = "Ru2",
   [0xa8] = "Ru2", [0xa9] = "Ru2", [0xaa] = "Ru2",
   [0xab] = "Ru2", [0xac] = "Ru2", [0xad] = "Ru2",
   [0xae] = "Ru2", [0xaf] = "Ru2", [0xb0] = "Ru2",
   [0xb1] = "Ru2", [0xb2] = "Ru2", [0xb3] = "Ru2",
   [0xb4] = "Ru2", [0xb5] = "Ru2", [0xb6] = "Ru2",
   [0xb7] = "Ru2", [0xb8] = "Ru2", [0xb9] = "Ru2",
   [0xba] = "Ru2", [0xbb] = "Ru2", [0xbc] = "Ru2",
   [0xbd] = "Ru2", [0xbe] = "Ru2", [0xbf] = "Ru2",
   [0xc0] = "Snd",
   [0xc1] = "SnI",
   [0xc2] = "WIP",
   [0xc3] = "Rsv", [0xc4] = "SnL", [0xc5] = "Rsv",
   [0xc6] = "Wr ",
   [0xc7] = "WrI",
   [0xc8] = "Rsv", [0xc9] = "Rsv", [0xca] = "Rsv",
   [0xcb] = "Rsv",
   [0xcc] = "RdQ",
   [0xcd] = "RdI",
   [0xce] = "Rsv",
   [0xcf] = "RdD",
   [0xd0] = "RNC",
   [0xd1] = "Ack",
   [0xd2] = "AtA",
   [0xd3] = "C&S",
   [0xd4] = "F&A",
   [0xd5] = "C&S",
   [0xd6] = "Res",
   [0xd7] = "Res",
   [0xd8] = "Rsv", [0xd9] = "Rsv", [0xda] = "Rsv",
   [0xdb] = "Rsv", [0xdc] = "Rsv", [0xdd] = "Rsv",
   [0xde] = "Ctl",
   [0xdf] = "UCl",
   [0xe0] = "Rsv", [0xe1] = "Rsv", [0xe2] = "Rsv",
   [0xe3] = "Rsv", [0xe4] = "Rsv", [0xe5] = "Rsv",
   [0xe6] = "Rsv", [0xe7] = "Rsv", [0xe8] = "Rsv",
   [0xe9] = "Rsv", [0xea] = "Rsv", [0xeb] = "Rsv",
   [0xec] = "Rsv", [0xed] = "Rsv", [0xee] = "Rsv",
   [0xef] = "Rsv", [0xf0] = "Rsv", [0xf1] = "Rsv",
   [0xf2] = "Rsv", [0xf3] = "Rsv", [0xf4] = "Rsv",
   [0xf5] = "Rsv", [0xf6] = "Rsv", [0xf7] = "Rsv",
   [0xf8] = "Rsv", [0xf9] = "Rsv", [0xfa] = "Rsv",
   [0xfb] = "Rsv", [0xfc] = "Rsv", [0xfd] = "Rsv",
   [0xfe] = "Rsv", [0xff] = "Rsv",
}

local opcode_value = {
   SEND = 0xc0,
   SEND_IMMD_P = 0xc1,
   RDMA_WRITE_IMMD_P = 0xc2,
   SEND_LAST = 0xc4,
   RDMA_WRITE = 0xc6,
   RDMA_WRITE_IMMD = 0xc7,
   RDMA_READ = 0xcc,
   RDMA_READ_IMMD_P = 0xcd,
   RDMA_READ_RESP = 0xcf,
   RNR_CANCEL = 0xd0,
   SEL_ACK = 0xd1,
   ATOMIC_ACK = 0xd2,
   CMP_SWAP = 0xd5,
   CMP_SWAP_ALT = 0xd3,
   FETCH_ADD = 0xd4,
   RELIABLE_CTRL= 0xde,
   UNRELIABLE_CTRL = 0xdf,
}

local pf_mr_base_opcode = ProtoField.new(
   "OpCode",
   "metaroce.base.opcode",
   ftypes.UINT8,
   opcode_name,
   base.HEX)
table.insert(metaroce.fields, pf_mr_base_opcode)

local pf_mr_base_reserved1 = ProtoField.new(
   "Reserved",
   "metaroce.base.reserved1",
   ftypes.UINT8,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_base_reserved1)

local pf_mr_base_version = ProtoField.new(
   "Version Number",
   "metaroce.base.version",
   ftypes.UINT16,
   nil,
   base.DEC,
   0xf000)
table.insert(metaroce.fields, pf_mr_base_version)

-- One field under two names: metaroce.base.flowid is the spelling to use,
-- metaroce.base.pathid the one it replaces. Both read the same bits.
local pf_mr_base_pathid = ProtoField.new(
   "PathID", "metaroce.base.pathid",
   ftypes.UINT16,
   nil,
   base.DEC,
   0x0fff)
table.insert(metaroce.fields, pf_mr_base_pathid)
local pf_mr_base_flowid = ProtoField.new(
   "FlowID", "metaroce.base.flowid",
   ftypes.UINT16,
   nil,
   base.DEC,
   0x0fff)
table.insert(metaroce.fields, pf_mr_base_flowid)

local pf_mr_base_destqp = ProtoField.new(
   "Destination QP", "metaroce.base.destqp",
   ftypes.UINT32,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_base_destqp)

local pf_mr_base_srcqp = ProtoField.new(
   "Source QP",
   "metaroce.base.sourceqp",
   ftypes.UINT32,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_base_srcqp)

-- Data header fields - METH
local pf_mr_data_path_fsn = ProtoField.uint16(
   "metaroce.data.fsn",
   "Flow Sequence Number (FSN)",
   base.DEC,
   nil)
table.insert(metaroce.fields, pf_mr_data_path_fsn)

local pf_mr_data_path_una = ProtoField.uint16(
   "metaroce.data.una",
   "Flow Sequence Number Clear (UNA)",
   base.DEC,
   nil)
table.insert(metaroce.fields, pf_mr_data_path_una)

local pf_mr_data_tag = ProtoField.uint8(
   "metaroce.data.tag",
   "Tag",
   base.DEC,
   nil)
table.insert(metaroce.fields, pf_mr_data_tag)

local ack_request_name = {
   [0] = "No Ack requested",
   [1] = "Immediate Ack requested",
}
local pf_mr_data_a = ProtoField.new(
   "A",
   "metaroce.data.a",
   ftypes.UINT8,
   ack_request_name,
   base.HEX,
   0x80)
table.insert(metaroce.fields, pf_mr_data_a)

local is_retrasmit_name = {
   [0] = "Not a retransmission",
   [1] = "Retransmitted due to inferred loss",
}
local pf_mr_data_r = ProtoField.new(
   "R",
   "metaroce.data.r",
   ftypes.UINT8,
   is_retrasmit_name,
   base.HEX,
   0x40)
table.insert(metaroce.fields, pf_mr_data_r)

local is_timeout_name = {
   [0] = "Not a timeout retransmission",
   [1] = "Retransmitted due to timeout",
}
local pf_mr_data_t = ProtoField.new(
   "T",
   "metaroce.data.t",
   ftypes.UINT8,
   is_timeout_name,
   base.HEX,
   0x20)
table.insert(metaroce.fields, pf_mr_data_t)

local pf_mr_data_reserved1 = ProtoField.new(
   "Reserved",
   "metaroce.data.reserved1",
   ftypes.UINT8,
   nil,
   base.HEX,
   0x10)
table.insert(metaroce.fields, pf_mr_data_reserved1)

local pf_mr_data_padding = ProtoField.new(
   "Padding",
   "metaroce.data.padding",
   ftypes.UINT8,
   nil,
   base.DEC,
   0x0f)
table.insert(metaroce.fields, pf_mr_data_padding)

local pf_mr_data_posn = ProtoField.uint16(
   "metaroce.data.posn",
   "Packet Offset Sequence Number (POSN)",
   base.DEC,
   nil)
table.insert(metaroce.fields, pf_mr_data_posn)

local pf_mr_data_csn = ProtoField.uint16(
   "metaroce.data.csn",
   "Completion Sequence Number (CSN)",
   base.DEC,
   nil)
table.insert(metaroce.fields, pf_mr_data_csn)

local pf_mr_data_msn = ProtoField.uint16(
   "metaroce.data.msn",
   "Message Sequence Number (MSN)",
   base.DEC,
   nil)
table.insert(metaroce.fields, pf_mr_data_msn)

local pf_mr_data_rate_hints = ProtoField.new(
   "Rate Hints (Gbps)",
   "metaroce.data.rate_hints",
   ftypes.UINT32,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_data_rate_hints)

local pf_mr_data_timestamp = ProtoField.new(
   "Timestamp",
   "metaroce.data.timestamp",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_data_timestamp)

-- Immediate header
local pf_mr_immd_immd = ProtoField.new(
   "IMMD",
   "metaroce.immd.immd",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_immd_immd)

-- Read/Write header fields
local pf_mr_rdwr_message_length = ProtoField.new(
   "Message Length",
   "metaroce.rdwr.message_length",
   ftypes.UINT32,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_rdwr_message_length)

local pf_mr_rdwr_r_key = ProtoField.new(
   "R_Key",
   "metaroce.rdwr.r_key",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_rdwr_r_key)

local pf_mr_rdwr_msg_address = ProtoField.new(
   "Message Address",
   "metaroce.rdwr.msg_address",
   ftypes.UINT64,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_rdwr_msg_address)

-- SAETH (Selective Ack) header fields
local pf_mr_saeth_reserved1 = ProtoField.uint8(
   "metaroce.saeth.reserved1",
   "Reserved",
   base.HEX,
   nil,
   0xf0)
table.insert(metaroce.fields, pf_mr_saeth_reserved1)

local pf_mr_saeth_cnp_ce_count = ProtoField.uint8(
   "metaroce.saeth.cnp_ce_count",
   "CNP CE count",
   base.DEC,
   nil,
   0x0f)
table.insert(metaroce.fields, pf_mr_saeth_cnp_ce_count)

local pf_mr_saeth_tag = ProtoField.new(
   "Tag",
   "metaroce.saeth.tag",
   ftypes.UINT8,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_saeth_tag)

local pf_mr_saeth_c_fsn = ProtoField.new(
   "Cumulative FSN",
   "metaroce.saeth.c_fsn",
   ftypes.UINT16,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_saeth_c_fsn)

local pf_mr_saeth_max_r_fsn = ProtoField.new(
   "Max Received FSN",
   "metaroce.saeth.max_r_fsn",
   ftypes.UINT16,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_saeth_max_r_fsn)

local ack_type_name = {
   [MR_SAETH_STATUS_ACK] = "ACK",
   [MR_SAETH_STATUS_HRNR_NAK] = "HRNR-NAK",
   [MR_SAETH_STATUS_BRNR_NAK] = "BRNR-NAK",
   [MR_SAETH_STATUS_NAK] = "NAK",
   [4] = "Reserved4",
   [5] = "Reserved5",
   [6] = "Reserved6",
   [7] = "Reserved7",
}
local pf_mr_saeth_status_type = ProtoField.uint8(
   "metaroce.saeth.ack_status",
   "Ack Status",
   base.DEC,
   ack_type_name,
   0xe0)
table.insert(metaroce.fields, pf_mr_saeth_status_type)

local ack_subtype_name = {
   [0] = "All data in sequence, ACK bitset optional",
   [1] = "Some data received out-of-order",
   [2] = "Reserved2", [3] = "Reserved3",
   [4] = "Reserved4", [5] = "Reserved5",
   [6] = "Reserved6", [7] = "Reserved7",
   [8] = "Reserved8", [9] = "Reserved9",
   [10] = "Reserved10", [11] = "Reserved11",
   [12] = "Reserved12", [13] = "Reserved13",
   [14] = "Reserved14", [15] = "Reserved15",
   [16] = "Reserved16", [17] = "Reserved17",
   [18] = "Reserved18", [19] = "Reserved19",
   [20] = "Reserved20", [21] = "Reserved21",
   [22] = "Reserved22", [23] = "Reserved23",
   [24] = "Reserved24", [25] = "Reserved25",
   [26] = "Reserved26", [27] = "Reserved27",
   [28] = "Reserved28", [29] = "Reserved29",
   [30] = "Reserved30", [31] = "Reserved31",
}
local pf_mr_saeth_status_ack = ProtoField.uint8(
   "metaroce.saeth.ack",
   "ACK",
   base.DEC,
   ack_subtype_name,
   0x1f)
table.insert(metaroce.fields, pf_mr_saeth_status_ack)

local pf_mr_saeth_status_hrnr = ProtoField.uint8(
   "metaroce.saeth.hrnr",
   "HRNR Backoff time",
   base.DEC,
   nil,
   0x1f)
table.insert(metaroce.fields, pf_mr_saeth_status_hrnr)

local pf_mr_saeth_status_brnr = ProtoField.uint8(
   "metaroce.saeth.brnr",
   "BRNR Backoff time",
   base.DEC,
   nil,
   0x1f)
table.insert(metaroce.fields, pf_mr_saeth_status_brnr)

local ack_nak_name = {
   [0] = "Path ID unsupported / Too large",
   [1] = "Path ID unsupported (0.94); Invalid Request (0.95)",
   [2] = "Address invalid / Access error",
   [3] = "Operational Error",
   [4] = "Reserved4",
   [5] = "Reserved5",
   [6] = "Reserved6",
   [7] = "Reserved7",
}
local pf_mr_saeth_status_nak = ProtoField.uint8(
   "metaroce.saeth.nak",
   "NAK",
   base.DEC,
   ack_nak_name,
   0x1f)
table.insert(metaroce.fields, pf_mr_saeth_status_nak)

local pf_mr_saeth_reserved2 = ProtoField.new(
   "Reserved",
   "metaroce.saeth.reserved2",
   ftypes.UINT8,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_reserved2)

local pf_mr_saeth_crd_msn = ProtoField.new(
   "Cumulative Responder Delivered MSN",
   "metaroce.saeth.crd_msn",
   ftypes.UINT16,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_saeth_crd_msn)

local pf_mr_saeth_active_port_set = ProtoField.new(
   "Active Port Set",
   "metaroce.saeth.active_port_set",
   ftypes.UINT16,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_active_port_set)

local pf_mr_saeth_rate_hints = ProtoField.new(
   "Rate hints (Gbps)",
   "metaroce.saeth.rate_hints",
   ftypes.UINT32,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_saeth_rate_hints)

local pf_mr_saeth_echo_timestamp = ProtoField.new(
   "Echo Timestamp",
   "metaroce.saeth.echo_timestamp",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_echo_timestamp)

local pf_mr_saeth_fsn_bit_vec0 = ProtoField.new(
   "FSN Bit Vector0",
   "metaroce.saeth.fsn_bit_vec0",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_bit_vec0)

local pf_mr_saeth_fsn_bit_vec1 = ProtoField.new(
   "FSN Bit Vector1",
   "metaroce.saeth.fsn_bit_vec1",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_bit_vec1)

local pf_mr_saeth_fsn_bit_vec2 = ProtoField.new(
   "FSN Bit Vector2",
   "metaroce.saeth.fsn_bit_vec2",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_bit_vec2)

local pf_mr_saeth_fsn_bit_vec3 = ProtoField.new(
   "FSN Bit Vector3",
   "metaroce.saeth.fsn_bit_vec3",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_bit_vec3)

local pf_mr_saeth_fsn_bit_vec4 = ProtoField.new(
   "FSN Bit Vector4",
   "metaroce.saeth.fsn_bit_vec4",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_bit_vec4)

local pf_mr_saeth_fsn_bit_vec5 = ProtoField.new(
   "FSN Bit Vector5",
   "metaroce.saeth.fsn_bit_vec5",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_bit_vec5)

local pf_mr_saeth_fsn_bit_vec6 = ProtoField.new(
   "FSN Bit Vector6",
   "metaroce.saeth.fsn_bit_vec6",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_bit_vec6)

local pf_mr_saeth_fsn_bit_vec7 = ProtoField.new(
   "FSN Bit Vector7",
   "metaroce.saeth.fsn_bit_vec7",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_bit_vec7)

local pf_mr_saeth_fsn_rnr_vec0 = ProtoField.new(
   "FSN RNR Bit Vector0",
   "metaroce.saeth.fsn_rnr_vec0",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_rnr_vec0)

local pf_mr_saeth_fsn_rnr_vec1 = ProtoField.new(
   "FSN RNR Bit Vector1",
   "metaroce.saeth.fsn_rnr_vec1",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_rnr_vec1)

local pf_mr_saeth_fsn_rnr_vec2 = ProtoField.new(
   "FSN RNR Bit Vector2",
   "metaroce.saeth.fsn_rnr_vec2",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_rnr_vec2)

local pf_mr_saeth_fsn_rnr_vec3 = ProtoField.new(
   "FSN RNR Bit Vector3",
   "metaroce.saeth.fsn_rnr_vec3",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_rnr_vec3)

local pf_mr_saeth_fsn_rnr_vec4 = ProtoField.new(
   "FSN RNR Bit Vector4",
   "metaroce.saeth.fsn_rnr_vec4",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_rnr_vec4)

local pf_mr_saeth_fsn_rnr_vec5 = ProtoField.new(
   "FSN RNR Bit Vector5",
   "metaroce.saeth.fsn_rnr_vec5",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_rnr_vec5)

local pf_mr_saeth_fsn_rnr_vec6 = ProtoField.new(
   "FSN RNR Bit Vector6",
   "metaroce.saeth.fsn_rnr_vec6",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_rnr_vec6)

local pf_mr_saeth_fsn_rnr_vec7 = ProtoField.new(
   "FSN RNR Bit Vector7",
   "metaroce.saeth.fsn_rnr_vec7",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_saeth_fsn_rnr_vec7)

-- Control header fields (v0.63)
local ctrl_type_name = {
   [0] = "Echo",
   [1] = "Response",
}
local pf_mr_ctrl_type = ProtoField.new(
   "Type",
   "metaroce.ctrl.type",
   ftypes.UINT16,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_ctrl_type)
-- For known ctlr packet types, currently only echo defined
local pf_mr_ctrl_type_named = ProtoField.new(
   "Type",
   "metaroce.ctrl.type",
   ftypes.UINT16,
   ctrl_type_name,
   base.HEX)
table.insert(metaroce.fields, pf_mr_ctrl_type_named)

local pf_mr_ctrl_reserved1 = ProtoField.new(
   "Reserved",
   "metaroce.ctrl.reserved1",
   ftypes.UINT16,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_ctrl_reserved1)

local pf_mr_ctrl_timestamp = ProtoField.new(
   "Timestamp",
   "metaroce.ctrl.timestamp",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_ctrl_timestamp)

local pf_mr_ctrl_data = ProtoField.new(
   "Control Data",
   "metaroce.ctrl.data",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_ctrl_data)

-- Echo header
local echo_error_state_name = {
   [0] = "No reported error",
   [1] = "QP not present / unknown",
   [2] = "Tag not present / unknown",
   [3] = "Tag not associated with QP",
   [4] = "Source address not associated with QP",
   [5] = "Destination address not associated with path",
   [6] = "Path ID unsupported / too large",
   [7] = "Reserved",
}

local pf_mr_ctrl_echo_id = ProtoField.new(
   "Id",
   "metaroce.echo.id",
   ftypes.UINT16,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_ctrl_echo_id)

local pf_mr_ctrl_echo_timestamp = ProtoField.new(
   "Echo timestamp",
   "metaroce.echo.timestamp",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_ctrl_echo_timestamp)

local pf_mr_ctrl_echo_tag = ProtoField.new(
   "Tag",
   "metaroce.echo.tag",
   ftypes.UINT8,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_ctrl_echo_tag)

local pf_mr_ctrl_echo_error_state = ProtoField.uint8(
   "metaroce.echo.error_state",
   "Error State",
   base.DEC,
   echo_error_state_name,
   0xff)
table.insert(metaroce.fields, pf_mr_ctrl_echo_error_state)

-- One field under two names: metaroce.echo.flow_cnt is the spelling to use,
-- metaroce.echo.path_cnt the one it replaces. Both read the same bits.
local pf_mr_ctrl_echo_supp_path_cnt = ProtoField.new(
   "Supported Path Count",
   "metaroce.echo.path_cnt",
   ftypes.UINT16,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_ctrl_echo_supp_path_cnt)
local pf_mr_ctrl_echo_supp_flow_cnt = ProtoField.new(
   "Supported Flow Count",
   "metaroce.echo.flow_cnt",
   ftypes.UINT16,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_ctrl_echo_supp_flow_cnt)

local pf_mr_ctrl_echo_rsvd1 = ProtoField.new(
   "Reserved",
   "metaroce.echo.rsvd1",
   ftypes.UINT16,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_ctrl_echo_rsvd1)

local pf_mr_ctrl_echo_active_port_set = ProtoField.new(
   "Active Port Set",
   "metaroce.echo.active_port_set",
   ftypes.UINT16,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_ctrl_echo_active_port_set)

-- RNRCancel header fields (v0.63)
-- One field under two names: metaroce.rnrc.flows is the spelling to use,
-- metaroce.rnrc.paths the one it replaces. Both read the same bits.
local pf_mr_rnrc_paths_avail = ProtoField.new(
   "Paths Available",
   "metaroce.rnrc.paths",
   ftypes.UINT8,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_rnrc_paths_avail)
local pf_mr_rnrc_flows_avail = ProtoField.new(
   "Flows Available",
   "metaroce.rnrc.flows",
   ftypes.UINT8,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_rnrc_flows_avail)

local pf_mr_rnrc_tag = ProtoField.new(
   "Tag",
   "metaroce.rnrc.tag",
   ftypes.UINT8,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_rnrc_tag)

local pf_mr_rnrc_pcsn = ProtoField.new(
   "Posted CSN",
   "metaroce.rnrc.pcsn",
   ftypes.UINT16,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_rnrc_pcsn)

local pf_mr_rnrc_rate_hints = ProtoField.new(
   "Rate Hints (Gbps)",
   "metaroce.rnrc.rate_hints",
   ftypes.UINT32,
   nil,
   base.DEC)
table.insert(metaroce.fields, pf_mr_rnrc_rate_hints)

-- AtomicETH header fields (v0.63)
local pf_mr_atomiceth_virt_addr = ProtoField.new(
   "Virtual Address",
   "metaroce.atomiceth.virt_addr",
   ftypes.UINT64,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_atomiceth_virt_addr)

local pf_mr_atomiceth_r_key = ProtoField.new(
   "R Key",
   "metaroce.atomiceth.r_key",
   ftypes.UINT32,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_atomiceth_r_key)

local pf_mr_atomiceth_swap_data = ProtoField.new(
   "Swap (or Add) Data",
   "metaroce.atomiceth.swap_data",
   ftypes.UINT64,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_atomiceth_swap_data)

local pf_mr_atomiceth_comp_data = ProtoField.new(
   "Compare Data",
   "metaroce.atomiceth.comp_data",
   ftypes.UINT64,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_atomiceth_comp_data)

-- AtomicAckETH header fields
local pf_mr_atomicacketh_orig_remote_data = ProtoField.new(
   "Original Remote Data",
   "metaroce.atomicacketh.orig_remote_data",
   ftypes.UINT64,
   nil,
   base.HEX)
table.insert(metaroce.fields, pf_mr_atomicacketh_orig_remote_data)

-- handle preferences
local debug_pref_enum = {
   { 1,  "Disabled", debug_level.DISABLED },
   { 2,  "Enabled",  debug_level.ENABLED  },
}

metaroce.prefs.debug = Pref.enum("Debug", default_settings.debug_level,
                                 "The debug state", debug_pref_enum)

metaroce.prefs.port_debug = Pref.uint(
   "Port number",
   default_settings.port_debug,
   "The debug UDP port number for MetaRoCE")

metaroce.prefs.port_real = Pref.uint(
   "Port number", default_settings.port_real,
   "The real UDP port number for MetaRoCE")

metaroce.prefs.heur = Pref.bool(
   "Heuristic enabled", default_settings.heur_enabled,
   "Whether heuristic dissection is enabled or not")

-- if triggered on the command line, arguments to change preferences will be
-- parsed here
function metaroce.prefs_changed()
   dprint("prefs_changed called")

   default_settings.debug_level  = metaroce.prefs.debug
   reset_debug_level()

   default_settings.heur_enabled = metaroce.prefs.heur

   if default_settings.port_debug ~= metaroce.prefs.port_debug then
      -- remove old one, if not 0
      if default_settings.port_debug ~= 0 then
         dprint("removing MetaRoCE from port",default_settings.port_debug)
         DissectorTable.get("udp.port"):remove(default_settings.port_debug,
                                               metaroce)
      end
      -- set our new default
      default_settings.port_debug = metaroce.prefs.port_debug
      -- add new one, if not 0
      if default_settings.port_debug ~= 0 then
         dprint("adding MetaRoCE to port",default_settings.port_debug)
         DissectorTable.get("udp.port"):add(default_settings.port_debug,
                                            metaroce)
      end
   end

   if default_settings.port_real ~= metaroce.prefs.port_real then
      -- remove old one, if not 0
      if default_settings.port_real ~= 0 then
         dprint("removing MetaRoCE from port",default_settings.port_real)
         DissectorTable.get("udp.port"):remove(default_settings.port_real,
                                               metaroce)
      end
      -- set our new default
      default_settings.port_real = metaroce.prefs.port_real
      -- add new one, if not 0
      if default_settings.port_real ~= 0 then
         dprint("adding MetaRoCE to port",default_settings.port_real)
         DissectorTable.get("udp.port"):add(default_settings.port_real,
                                            metaroce)
      end
   end

end

dprint("MetaRoCE Prefs registered")

-- Main dissector function
function metaroce.dissector(tvbuf, pktinfo, root)
   dprint("metaroce.dissector called")
   local info = ""
   local pos = 0
   local pi_error_type = PI_ERROR
   local pi_error_message = "Invalid length, cannot decode "
   -- initialize type of header
   local headers = {}
   headers.BASE = true
   headers.usesCSN = false

   -- show protocol name
   pktinfo.cols.protocol:set("MetaRoCE")

   local pktlen = tvbuf:captured_len()
   local wire_len = tvbuf:reported_len()

   if pktlen < wire_len then
      -- if packet capture length is truncated, do not report as invalid
      pi_error_type = PI_NOTE
      pi_error_message = "Capture truncated, cannot decode "
   end

   -- Add to dissection display tree
   local tree = root:add(metaroce, tvbuf:range(0, pktlen))

   -- too short to even process header?
   if pktlen < (pos + mr_header.BASE_LEN) then
      -- short packet
      tree:add_expert_info(PI_MALFORMED, pi_error_type,
                           pi_error_message ..
                           pktlen .. "/" .. (pos + mr_header.BASE_LEN))
      pktinfo.cols.info:set(info .. " [Base] Short packet!")
      return pos
   end

   -- figure out which headers we have
   local opcode_type = tvbuf:range(mr_base_offset.OPCODE, U8_LEN):uint()

   -- Based on packet type, establish headers to decode
   if opcode_type == opcode_value.SEND or
      opcode_type == opcode_value.SEND_LAST then
      headers.METH = true
      headers.Pyld = true
      headers.usesCSN = true
   elseif opcode_type == opcode_value.SEND_IMMD_P then
      headers.METH = true
      headers.ImmDt = true
      headers.Pyld = true
      headers.usesCSN = true
   elseif opcode_type == opcode_value.RDMA_WRITE then
      headers.METH = true
      headers.RETH = true
      headers.Pyld = true
   elseif opcode_type == opcode_value.RDMA_WRITE_IMMD then
      headers.METH = true
      headers.RETH = true
      headers.ImmDt = false
      headers.Pyld = true
      headers.usesCSN = true
   elseif opcode_type == opcode_value.RDMA_WRITE_IMMD_P then
      headers.METH = true
      headers.RETH = true
      headers.ImmDt = true
      headers.Pyld = true
      headers.usesCSN = true
   elseif opcode_type == opcode_value.RDMA_READ then
      headers.METH = true
      headers.RETH = true
   elseif opcode_type == opcode_value.RDMA_READ_IMMD_P then
      headers.METH = true
      headers.RETH = true
      headers.ImmDt = true
      headers.usesCSN = true
   elseif opcode_type == opcode_value.RDMA_READ_RESP then
      headers.METH = true
      headers.Pyld = true
   elseif opcode_type == opcode_value.RNR_CANCEL then
      headers.RNRCancel = true
   elseif opcode_type == opcode_value.SEL_ACK then
      headers.SAETH = true
   elseif opcode_type == opcode_value.ATOMIC_ACK then
      headers.METH = true
      headers.AtomicAckETH = true
   elseif opcode_type == opcode_value.CMP_SWAP or
          opcode_type == opcode_value.CMP_SWAP_ALT then
      headers.METH = true
      headers.AtomicETH = true
   elseif opcode_type == opcode_value.FETCH_ADD then
      headers.METH = true
      headers.AtomicETH = true
   elseif opcode_type >= 0xd6 and opcode_type <= 0xd7 then
      -- Atomics Reserved in the range 0xd6 - 0xd7
      headers.METH = true
      headers.AtomicETH = true
   elseif opcode_type == opcode_value.RELIABLE_CTRL then
      headers.METH = true
      headers.Control = true
      headers.Pyld = true
      -- End of packet assumed to be control data payload
   elseif opcode_type == opcode_value.UNRELIABLE_CTRL then
      headers.Control = true
      headers.Pyld = true
      -- End of packet assumed to be control data payload
   else
      -- Treat a metaroce packet with an invalid opcode as
      -- if it is entirely payload, allowing checking the
      -- terminal mCRC.
      headers.Pyld = true
   end

   -- Base header
   if headers.BASE then
      -- length of headers has been verified above
      local base_tree = tree:add(pf_mr_base, tvbuf:range(pos,
                                                         mr_header.BASE_LEN))

      local opcode = tvbuf:range(mr_base_offset.OPCODE, U8_LEN)
      base_tree:add(pf_mr_base_opcode, opcode)
      base_tree:add(pf_mr_base_reserved1, tvbuf:range(mr_base_offset.RSVD1,
                                                      U8_LEN))
      base_tree:add(pf_mr_base_version, tvbuf:range(mr_base_offset.VERSION,
                                                    U16_LEN))

      local pathid = tvbuf:range(mr_base_offset.PATHID, U16_LEN)
      base_tree:add(pf_mr_base_flowid, pathid)
      base_tree:add(pf_mr_base_pathid, pathid):set_hidden()
      local destqp = tvbuf:range(mr_base_offset.DEST_QP, U32_LEN)
      local srcqp = tvbuf:range(mr_base_offset.SRC_QP, U32_LEN)
      base_tree:add(pf_mr_base_destqp, destqp)
      base_tree:add(pf_mr_base_srcqp, srcqp)
      base_tree:set_text(
         string.format("Base, QP-Source: %d, QP-Dest: %d, Path: %d, OpCode: %s",
                       srcqp:uint(), destqp:uint(), pathid:bitfield(4,12),
                       opcode_name[tvbuf:range(mr_base_offset.OPCODE,
                                               U8_LEN):uint()]))
      -- magic symbols in the middle here is the right arrow ( -> )
      info = info .. string.format(" QP %d \226\158\148 %d %d [%s]",
         srcqp:uint(),
         destqp:uint(),
         pathid:bitfield(4, 12),
         opcode_abbr[opcode:uint()])
      pktinfo.cols.info:set(info)

      -- Conversation tracking: assign a stable ID to each unique QP pair
      local pkt_num = pktinfo.number
      local cached = pkt_conv_cache[pkt_num]
      if cached then
         tree:add(pf_mr_conv_id, cached.id):set_generated()
         tree:add(pf_mr_conv, cached.str):set_generated()
      else
         local src_ip = tostring(pktinfo.src)
         local dst_ip = tostring(pktinfo.dst)
         local sq = srcqp:uint()
         local dq = destqp:uint()
         local conv_str = conv_make_str(src_ip, dst_ip, sq, dq)
         local cid = conv_table[conv_str]
         if not cid then
            cid = conv_next_id
            conv_table[conv_str] = cid
            conv_next_id = conv_next_id + 1
         end
         pkt_conv_cache[pkt_num] = {id = cid, str = conv_str}
         tree:add(pf_mr_conv_id, cid):set_generated()
         tree:add(pf_mr_conv, conv_str):set_generated()
      end

      pos = pos + mr_header.BASE_LEN
   end

   -- Data header
   if headers.METH then
      if pktlen < (pos + mr_header.DATA_LEN) then
         -- short packet
         tree:add_expert_info(PI_MALFORMED, pi_error_type,
                              pi_error_message ..
                              pktlen .. "/" .. (pos + mr_header.DATA_LEN))
         pktinfo.cols.info:set(info .. " Short packet!")
         return pos
      end
      local data_tree = tree:add(pf_mr_data, tvbuf:range(pos,
                                                         mr_header.DATA_LEN))

      data_tree:add(pf_mr_data_path_fsn, tvbuf:range(
                       pos + mr_meth_offset.PATH_FSN, U16_LEN))
      data_tree:add(pf_mr_data_path_una, tvbuf:range(
                       pos + mr_meth_offset.PATH_UNA, U16_LEN))
      local fsn =  tvbuf:range(pos + mr_meth_offset.PATH_FSN,
                               U16_LEN):uint()

      local tag = tvbuf:range(pos + mr_meth_offset.TAG, U8_LEN):uint()
      data_tree:add(pf_mr_data_tag, tvbuf:range(
                       pos + mr_meth_offset.TAG, U8_LEN))
      data_tree:add(pf_mr_data_a, tvbuf:range(
                       pos + mr_meth_offset.A, U8_LEN))
      data_tree:add(pf_mr_data_r, tvbuf:range(
                       pos + mr_meth_offset.R, U8_LEN))
      data_tree:add(pf_mr_data_t, tvbuf:range(
                       pos + mr_meth_offset.T, U8_LEN))
      data_tree:add(pf_mr_data_reserved1, tvbuf:range(
                       pos + mr_meth_offset.RESERVED1, U8_LEN))
      data_tree:add(pf_mr_data_padding, tvbuf:range(
                       pos + mr_meth_offset.PADDING, U8_LEN))
      data_tree:add(pf_mr_data_posn, tvbuf:range(
                       pos + mr_meth_offset.POSN, U16_LEN))
      local posn = tvbuf:range(pos + mr_meth_offset.POSN, U16_LEN):uint()

      data_tree:add(pf_mr_data_csn, tvbuf:range(
                       pos + mr_meth_offset.CSN, U16_LEN))
      data_tree:add(pf_mr_data_msn, tvbuf:range(
                       pos + mr_meth_offset.MSN, U16_LEN))
      local msn = tvbuf:range(pos + mr_meth_offset.MSN, U16_LEN):uint()
      -- Format MSN with tag prefix if tag is nonzero
      if tag ~= 0 then
         info = info .. string.format(" FSN=%d MSN=%d.%d", fsn, tag, msn)
      else
         info = info .. string.format(" FSN=%d MSN=%d", fsn, msn)
      end
      if headers.usesCSN  then
         local csn = tvbuf:range(pos + mr_meth_offset.CSN, U16_LEN):uint()
         -- Format CSN with tag prefix if tag is nonzero
         if tag ~= 0 then
            info = info .. string.format(" CSN=%d.%d", tag, csn)
         else
            info = info .. string.format(" CSN=%d", csn)
         end
      end
      info = info .. string.format(" POSN=%d", posn)

      data_tree:add(pf_mr_data_rate_hints, tvbuf:range(
                       pos + mr_meth_offset.RATE_HINTS, U32_LEN))
      data_tree:add(pf_mr_data_timestamp, tvbuf:range(
                       pos + mr_meth_offset.TIMESTAMP, U32_LEN))
      data_tree:set_text(
         string.format("Data, FSN: %d, MSN: %d, POSN: %d",
                       fsn, msn, posn))

      pktinfo.cols.info:set(info)
      pos = pos + mr_header.DATA_LEN
   end

   -- Read/Write header
   if headers.RETH then
      if (pktlen < (pos + mr_header.RDWR_LEN))
      then
         -- short packet
         tree:add_expert_info(PI_MALFORMED, pi_error_type,
                              pi_error_message ..
                              pktlen .. "/" .. (pos + mr_header.RDWR_LEN))
         pktinfo.cols.info:set(info .. " Short packet!")
         return pos
      end

      local rdwr_tree = tree:add(pf_mr_rdwr,
                                 tvbuf:range(pos,
                                             mr_header.RDWR_LEN))
      rdwr_tree:add(pf_mr_rdwr_msg_address,
                    tvbuf:range(pos
                                + mr_reth_offset.MSG_ADDR,
                                U64_LEN))

      rdwr_tree:add(pf_mr_rdwr_r_key,
                    tvbuf:range(pos
                                + mr_reth_offset.R_KEY,
                                U32_LEN))

      rdwr_tree:add(pf_mr_rdwr_message_length,
                    tvbuf:range(pos
                                + mr_reth_offset.MSG_LEN,
                                U32_LEN))
      rdwr_tree:set_text("Read/Write")
      pktinfo.cols.info:set(info)
      pos = pos + mr_header.RDWR_LEN
   end

   -- Immediate header
   if headers.ImmDt then
      info = info .. " ImmDt"
      if (pktlen < (pos + mr_header.IMMD_LEN))
      then
         tree:add_expert_info(PI_MALFORMED, pi_error_type,
                              pi_error_message ..
                              pktlen .. "/" .. (pos + mr_header.IMMD_LEN))
         pktinfo.cols.info:set(info .. " Short packet!")
         return pos
      end

      local immd_tree = tree:add(pf_mr_immd, tvbuf:range(pos,
                                                         mr_header.IMMD_LEN))
      immd_tree:add(pf_mr_immd_immd, tvbuf:range(pos, U32_LEN))
      immd_tree:set_text("Immediate")
      pktinfo.cols.info:set(info)
      pos = pos + mr_header.IMMD_LEN
   end

   -- RNRCancel header
   if headers.RNRCancel then
      if pktlen < (pos + mr_header.RNRC_LEN) then
         tree:add_expert_info(PI_MALFORMED, pi_error_type,
                              pi_error_message ..
                              pktlen .. "/" .. (pos + mr_header.RNRC_LEN))
         pktinfo.cols.info:set(info .. " Short packet!")
         return pos
      end

      local rnrc_tree = tree:add(pf_mr_rnrc,
                                 tvbuf:range(pos,
                                             mr_header.RNRC_LEN))
      local paths_avail = tvbuf:range(pos + mr_rnrc_offset.PATHS_AVAIL,
                                      U8_LEN)
      rnrc_tree:add(pf_mr_rnrc_flows_avail, paths_avail)
      rnrc_tree:add(pf_mr_rnrc_paths_avail, paths_avail):set_hidden()
      rnrc_tree:add(pf_mr_rnrc_tag,
                    tvbuf:range(pos + mr_rnrc_offset.TAG,
                                U8_LEN))
      rnrc_tree:add(pf_mr_rnrc_pcsn,
                    tvbuf:range(pos + mr_rnrc_offset.PCSN,
                                U16_LEN))
      rnrc_tree:add(pf_mr_rnrc_rate_hints,
                    tvbuf:range(pos + mr_rnrc_offset.RATE_HINTS,
                                U32_LEN))
      pktinfo.cols.info:set(info)
      pos = pos + mr_header.RNRC_LEN
   end

   -- Selective Ack header
   if headers.SAETH then
      if pktlen < (pos + mr_header.SAETH_LEN) then
         -- short packet
         tree:add_expert_info(PI_MALFORMED, pi_error_type,
                              pi_error_message ..
                              pktlen .. "/" .. (pos + mr_header.SAETH_LEN))
         pktinfo.cols.info:set(info .. " Short packet!")
         return pos
      end
      local ack_tree = tree:add(pf_mr_saeth,
                                tvbuf:range(pos,
                                            mr_header.SAETH_LEN ))

      ack_tree:add(pf_mr_saeth_reserved1,
                   tvbuf:range(pos + mr_saeth_offset.RESERVED1,
                               U8_LEN))
      ack_tree:add(pf_mr_saeth_cnp_ce_count,
                   tvbuf:range(pos + mr_saeth_offset.CNP,
                               U8_LEN))
      local saeth_tag = tvbuf:range(pos + mr_saeth_offset.TAG, U8_LEN):uint()
      ack_tree:add(pf_mr_saeth_tag, tvbuf:range(pos
                                                + mr_saeth_offset.TAG,
                                                U8_LEN))

      local c_fsn = tvbuf:range(pos + mr_saeth_offset.C_FSN,
                                     U16_LEN)
      ack_tree:add(pf_mr_saeth_c_fsn, c_fsn)

      local max_r_fsn = tvbuf:range(pos + mr_saeth_offset.MAX_R_FSN,
                                    U16_LEN)
      ack_tree:add(pf_mr_saeth_max_r_fsn, max_r_fsn)
      -- only print max-received-fsn if worth the space; print it relative.
      local ooo = seq16_sub(max_r_fsn:uint(), c_fsn:uint())
      local ack_type = tvbuf:range(pos + mr_saeth_offset.STATUS, U8_LEN)
      ack_tree:add(pf_mr_saeth_status_type, ack_type)

      if ack_type:bitfield(0, 3) == MR_SAETH_STATUS_ACK then
         if ack_type:bitfield(3, 5) == 1 then
           info = info .. " SACK"
         end
         ack_tree:add(pf_mr_saeth_status_ack,
                             ack_type);
      end
      if ack_type:bitfield(0, 3) == MR_SAETH_STATUS_HRNR_NAK then
         info = info .. " HRNR"
         ack_tree:add(pf_mr_saeth_status_hrnr,
                             ack_type);
      end
      local has_brnr = ack_type:bitfield(0, 3) == MR_SAETH_STATUS_BRNR_NAK
      if has_brnr then
         info = info .. " BRNR"
         ack_tree:add(pf_mr_saeth_status_brnr,
                             ack_type);
      end
      if ack_type:bitfield(0, 3) == MR_SAETH_STATUS_NAK then
         info = info .. " NAK"
         ack_tree:add(pf_mr_saeth_status_nak,
                             ack_type);
      end
      ack_tree:add(pf_mr_saeth_reserved2, tvbuf:range(
                      pos + mr_saeth_offset.RESERVED2, U8_LEN))

      info = info .. string.format(" C-FSN=%d", c_fsn:uint())

      if ooo > 0 then
         info = info .. string.format(" Max-R-FSN=+%d", ooo)
      end

      local cdmsn = tvbuf:range(pos + mr_saeth_offset.CRD_MSN, U16_LEN)
      ack_tree:add(pf_mr_saeth_crd_msn, cdmsn)
      -- Format CDMSN with tag prefix if tag is nonzero
      if saeth_tag ~= 0 then
         info = info .. string.format(" CDMSN=%d.%d", saeth_tag, cdmsn:uint())
      else
         info = info .. string.format(" CDMSN=%d", cdmsn:uint())
      end

      local ack_aps = tvbuf:range(
                      pos + mr_saeth_offset.ACTIVE_PORT_SET, U16_LEN)
      ack_tree:add(pf_mr_saeth_active_port_set, ack_aps)
      local aps_val = ack_aps:uint()
      if aps_val ~= 0 and bit.band(aps_val, aps_val + 1) ~= 0 then
         info = info .. string.format(" APS=0x%x", aps_val)
      end
      ack_tree:add(pf_mr_saeth_rate_hints, tvbuf:range(
                      pos + mr_saeth_offset.RATE_HINTS, U32_LEN))
      ack_tree:add(pf_mr_saeth_echo_timestamp, tvbuf:range(
                      pos + mr_saeth_offset.ECHO_TIMESTAMP, U32_LEN))

      pos = pos + mr_header.SAETH_LEN

      local crc_len = pktlen == wire_len and U32_LEN or 0
      if pktlen >= (pos + mr_header.SAETH_FSN_LEN + crc_len) then
         -- FSN bit vectors
         ack_tree:add(pf_mr_saeth_fsn_bit_vec0, tvbuf:range(
                         pos + mr_saeth_fsn_offset.VEC0, U32_LEN))
         ack_tree:add(pf_mr_saeth_fsn_bit_vec1, tvbuf:range(
                         pos + mr_saeth_fsn_offset.VEC1, U32_LEN))
         ack_tree:add(pf_mr_saeth_fsn_bit_vec2, tvbuf:range(
                         pos + mr_saeth_fsn_offset.VEC2, U32_LEN))
         ack_tree:add(pf_mr_saeth_fsn_bit_vec3, tvbuf:range(
                         pos + mr_saeth_fsn_offset.VEC3, U32_LEN))
         ack_tree:add(pf_mr_saeth_fsn_bit_vec4, tvbuf:range(
                         pos + mr_saeth_fsn_offset.VEC4, U32_LEN))
         ack_tree:add(pf_mr_saeth_fsn_bit_vec5, tvbuf:range(
                         pos + mr_saeth_fsn_offset.VEC5, U32_LEN))
         ack_tree:add(pf_mr_saeth_fsn_bit_vec6, tvbuf:range(
                         pos + mr_saeth_fsn_offset.VEC6, U32_LEN))
         ack_tree:add(pf_mr_saeth_fsn_bit_vec7, tvbuf:range(
                         pos + mr_saeth_fsn_offset.VEC7, U32_LEN))

         pos = pos + mr_header.SAETH_FSN_LEN
      elseif ack_type:uint() == 1 and pktlen == wire_len then
         -- Selective ACKs may carry fewer than eight FSN words. The last
         -- word is the mCRC, so leave it for the common CRC check below.
         local vector_len = pktlen - pos - U32_LEN
         if vector_len >= U32_LEN and vector_len % U32_LEN == 0 then
            local vector_fields = {
               pf_mr_saeth_fsn_bit_vec0, pf_mr_saeth_fsn_bit_vec1,
               pf_mr_saeth_fsn_bit_vec2, pf_mr_saeth_fsn_bit_vec3,
               pf_mr_saeth_fsn_bit_vec4, pf_mr_saeth_fsn_bit_vec5,
               pf_mr_saeth_fsn_bit_vec6, pf_mr_saeth_fsn_bit_vec7,
            }
            for word = 1, vector_len / U32_LEN do
               ack_tree:add(vector_fields[word], tvbuf:range(pos, U32_LEN))
               pos = pos + U32_LEN
            end
         end
      end

      if has_brnr then
         if pktlen >= (pos + mr_header.SAETH_RNR_LEN + crc_len) then
            -- RNR bit vectors
            ack_tree:add(pf_mr_saeth_fsn_rnr_vec0, tvbuf:range(
                           pos + mr_saeth_rnr_offset.RNR0, U32_LEN))
            ack_tree:add(pf_mr_saeth_fsn_rnr_vec1, tvbuf:range(
                           pos + mr_saeth_rnr_offset.RNR1, U32_LEN))
            ack_tree:add(pf_mr_saeth_fsn_rnr_vec2, tvbuf:range(
                           pos + mr_saeth_rnr_offset.RNR2, U32_LEN))
            ack_tree:add(pf_mr_saeth_fsn_rnr_vec3, tvbuf:range(
                           pos + mr_saeth_rnr_offset.RNR3, U32_LEN))
            ack_tree:add(pf_mr_saeth_fsn_rnr_vec4, tvbuf:range(
                           pos + mr_saeth_rnr_offset.RNR4, U32_LEN))
            ack_tree:add(pf_mr_saeth_fsn_rnr_vec5, tvbuf:range(
                           pos + mr_saeth_rnr_offset.RNR5, U32_LEN))
            ack_tree:add(pf_mr_saeth_fsn_rnr_vec6, tvbuf:range(
                           pos + mr_saeth_rnr_offset.RNR6, U32_LEN))
            ack_tree:add(pf_mr_saeth_fsn_rnr_vec7, tvbuf:range(
                           pos + mr_saeth_rnr_offset.RNR7, U32_LEN))

            pos = pos + mr_header.SAETH_RNR_LEN
         elseif pktlen >= (pos + U32_LEN + crc_len) then
            -- RNR bit vectors, special case with one word only.
            ack_tree:add(pf_mr_saeth_fsn_rnr_vec0, tvbuf:range(
                           pos + mr_saeth_rnr_offset.RNR0, U32_LEN))
            pos = pos + U32_LEN
         end
      end

      ack_tree:set_text(
         string.format("SelAck, C-FSN: %d, Max-R-FSN: %d",
                       c_fsn:uint(), max_r_fsn:uint()))
      pktinfo.cols.info:set(info)
   end

   -- Atomic ETH header
   if headers.AtomicETH then
      if pktlen < (pos + mr_header.ATOMICETH_LEN) then
         -- short packet
         tree:add_expert_info(PI_MALFORMED, pi_error_type,
                              pi_error_message ..
                              pktlen .. "/" ..
                              (pos + mr_header.ATOMICETH_LEN))
         pktinfo.cols.info:set(info .. " Short packet!")
         return pos
      end

      local atomic_eth = tree:add(pf_mr_atomiceth, tvbuf:range(pos,
                                  mr_header.ATOMICETH_LEN))
      atomic_eth:add(pf_mr_atomiceth_virt_addr, tvbuf:range(pos
                     + mr_atomiceth_offset.VADDR, U64_LEN))
      atomic_eth:add(pf_mr_atomiceth_r_key, tvbuf:range(pos
                     + mr_atomiceth_offset.R_KEY, U32_LEN))
      atomic_eth:add(pf_mr_atomiceth_swap_data, tvbuf:range(pos
                     + mr_atomiceth_offset.SWAP_DATA, U64_LEN))
      atomic_eth:add(pf_mr_atomiceth_comp_data, tvbuf:range(pos
                     + mr_atomiceth_offset.COMP_DATA, U64_LEN))
      atomic_eth:set_text("AtomicEth")
      pktinfo.cols.info:set(info)
      pos = pos + mr_header.ATOMICETH_LEN
   end

   -- Atomic Ack ETH header
   if headers.AtomicAckETH then
      if pktlen < (pos + mr_header.ATOMICACKETH_LEN) then
         -- short packet
         tree:add_expert_info(PI_MALFORMED, pi_error_type,
                              pi_error_message ..
                              pktlen .. "/" ..
                              (pos + mr_header.ATOMICACKETH_LEN))
         pktinfo.cols.info:set(info .. " Short packet!")
         return pos
      end

      local atomic_ack_eth = tree:add(pf_mr_atomicacketh,
                                      tvbuf:range(pos,
                                      mr_header.ATOMICACKETH_LEN))
      atomic_ack_eth:add(pf_mr_atomicacketh_orig_remote_data,
                         tvbuf:range(pos + MR_ATOMICACKETH_ORIG_REMOTE_DATA,
                         U64_LEN))
      atomic_ack_eth:set_text("AtomicAckEth")
      pktinfo.cols.info:set(info)
      pos = pos + mr_header.ATOMICACKETH_LEN
   end

   -- Control header
   if headers.Control then
      -- dissect control packet
      info = info .. " [Control]"
      if pktlen < (pos + mr_header.CTRL_LEN) then
         -- short packet
         tree:add_expert_info(PI_MALFORMED, pi_error_type,
                              pi_error_message ..
                              pktlen .. "/" .. (pos + mr_header.CTRL_LEN))
         pktinfo.cols.info:set(info .. " Short packet")
         return pos
      end

      local ctrl_tree = tree:add(pf_mr_ctrl, tvbuf:range(pos,
                                                         mr_header.CTRL_LEN))

      -- Add ctrl packet type name if known
      local ctrl_type = tvbuf:range(pos + mr_ctrl_offset.TYPE, U16_LEN):uint()
      if ctrl_type <= 1 then
         ctrl_tree:add(pf_mr_ctrl_type_named,
                       tvbuf:range(pos + mr_ctrl_offset.TYPE,
                                   U16_LEN))
      else
         ctrl_tree:add(pf_mr_ctrl_type,
                       tvbuf:range(pos + mr_ctrl_offset.TYPE,
                                   U16_LEN))
      end

      -- check for Ping (Echo) and Echo response packets
      if ctrl_type == CTRL_TYPE_ECHO
         or ctrl_type == CTRL_TYPE_ECHO_RESPONSE then
         -- normal ctrl packet, decode control header first
         ctrl_tree:add(pf_mr_ctrl_echo_id,
                       tvbuf:range(pos
                                   + mr_ctrl_offset.RESERVED1,
                                   U16_LEN))
         ctrl_tree:add(pf_mr_ctrl_echo_timestamp,
                       tvbuf:range(pos
                                   + mr_ctrl_offset.TIMESTAMP,
                                   U32_LEN))
         pos = pos + mr_header.CTRL_LEN
         -- echo packet contents
         local echo_tree = tree:add(pf_mr_ctrl_echo,
                                    tvbuf:range(pos, mr_header.CTRL_ECHO_LEN))
         echo_tree:add(pf_mr_ctrl_echo_tag,
                       tvbuf:range(pos + mr_ctrl_echo_offset.TAG, U8_LEN))
         echo_tree:add(pf_mr_ctrl_echo_error_state,
                       tvbuf:range(pos + mr_ctrl_echo_offset.ERROR_STATE,
                                   U8_LEN))
         local supp_path_cnt =
            tvbuf:range(pos + mr_ctrl_echo_offset.SUPP_PATH_CNT, U16_LEN)
         echo_tree:add(pf_mr_ctrl_echo_supp_flow_cnt, supp_path_cnt)
         echo_tree:add(pf_mr_ctrl_echo_supp_path_cnt,
                       supp_path_cnt):set_hidden()
         echo_tree:add(pf_mr_ctrl_echo_rsvd1,
                       tvbuf:range(pos + mr_ctrl_echo_offset.RSVD1, U16_LEN))
         local echo_aps = tvbuf:range(
                       pos + mr_ctrl_echo_offset.ACTIVE_PORT_SET, U16_LEN)
         echo_tree:add(pf_mr_ctrl_echo_active_port_set, echo_aps)
         pos = pos + mr_header.CTRL_ECHO_LEN
      else
         -- normal ctrl packet
         ctrl_tree:add(pf_mr_ctrl_reserved1,
                       tvbuf:range(pos
                                   + mr_ctrl_offset.RESERVED1,
                                   U16_LEN))
         ctrl_tree:add(pf_mr_ctrl_timestamp,
                       tvbuf:range(pos
                                   + mr_ctrl_offset.TIMESTAMP,
                                   U32_LEN))
         pos = pos + mr_header.CTRL_LEN
      end
      -- decode additional 32-bit words for total length of packet.
      -- The last word of a complete capture is the mCRC, which the spec
      -- requires on control packets too; leave it to the CRC check.
      local ctrl_end = pktlen
      if pktlen == wire_len and (pktlen - pos) >= U32_LEN then
         ctrl_end = pktlen - U32_LEN
      end
      while (ctrl_end - pos) >= U32_LEN do
         ctrl_tree:add(pf_mr_ctrl_data, tvbuf:range(pos,
                                                    U32_LEN))
         pos = pos + U32_LEN
      end
      pktinfo.cols.info:set(info)
   end

   -- Rest of packet _should_ be payload. A control packet's payload is its
   -- control-data words, added above.
   if headers.Pyld and not headers.Control then
      pktinfo.cols.info:set(info)
      local payload_length
      if pktlen == wire_len and pktlen - pos >= 4
      then
         payload_length = pktlen - pos - 4
      else
         payload_length = pktlen - pos
      end
      local payload_tree = tree:add(pf_mr_payload,
                                    tvbuf:range(pos, payload_length))
      local payload_truncated = ""

      if pktlen < wire_len then
         payload_truncated = " [truncated]"
      end

      payload_tree:set_text(
         string.format("Payload, Length: %d %s",
                       payload_length, payload_truncated))
      pos = pos + payload_length
   end

   -- Expect the last 4 bytes to be CRC, if the capture was not truncated.
   if pktlen == wire_len and pos + U32_LEN == pktlen then
      -- Our computation returns a little-endian crc, so read
      -- as a little endian value.

      -- bitop uses signed values, so we load a signed value (le_int,
      -- not le_uint) for comparison; see https://bitop.luajit.org/api.html
      local crc_received = tvbuf:range(pos, U32_LEN):le_int()
      local crc_tree = tree:add(pf_mr_crc, tvbuf:range(pos, U32_LEN))

      -- Calculate CRC-32 over the packet data (excluding the CRC field itself)
      local packet_data = tvbuf:range(0, pos)
      local crc_calculated = crc32(packet_data:bytes())

      -- Compare calculated vs received CRC
      local crc_status = (crc_calculated == crc_received) and 0 or 1
      local status_item = crc_tree:add(pf_mr_crc_status, crc_status)

      if crc_status == 0 then
         crc_tree:set_text(string.format("CRC: 0x%s [correct]",
                                         bit.tohex(crc_received)))
         crc_tree:add_proto_expert_info(ef_crc_good)
      else
         local crc_verify = crc32(tvbuf:range(0,pos+U32_LEN):bytes())
         crc_tree:set_text(string.format(
                     "CRC: 0x%s [incorrect, should be 0x%s;"
                     .. " verify: 0x%s should be 0x2144df1c]",
                     bit.tohex(crc_received),
                     bit.tohex(crc_calculated),
                     bit.tohex(crc_verify)))
         crc_tree:add_proto_expert_info(ef_crc_bad)
         info = info .. " [CRC ERROR]"
      end

      pos = pos + U32_LEN
   end

   -- Unexpected data at end of packet
   if pktlen > pos then
      info = info .. " [?]"

      local payload_tree = tree:add(pf_mr_payload,
                                    tvbuf:range(pos, pktlen - pos))
      payload_tree:set_text(
        string.format("Payload, Unknown: %d", (pktlen - pos)))
      pktinfo.cols.info:set(info)
      pos = pktlen
   end

   pktinfo.cols.info:set(info)
   return pos
end

-- we want to have our protocol dissection invoked for a specific UDP ports
DissectorTable.get("udp.port"):add(default_settings.port_debug, metaroce)
DissectorTable.get("udp.port"):add(default_settings.port_real, metaroce)

local function heur_dissect_metaroce(tvbuf, pktinfo, root)
   dprint("heur_dissect_metaroce called")

   -- if our preferences tell us not to do this, return false
   if not default_settings.heur_enabled then
      return false
   end

   if tvbuf:len() < mr_header.BASE_LEN then
      dprint("heur_dissect_metaroce: tvb shorter than mr_header.BASE_LEN of:",
             mr_header.BASE_LEN)
      return false
   end

   metaroce.dissector(tvbuf, pktinfo, root)
   pktinfo.conversation = metaroce

   return true
end

-- register heuristic dissector
metaroce:register_heuristic("udp", heur_dissect_metaroce)
