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

#include <folly/CppAttributes.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/coro/BlockingWait.h>
#include <folly/synchronization/AsymmetricThreadFence.h>
#include <folly/system/HardwareConcurrency.h>

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <libsoftmetaroce/ConcurrentQpArbiter.h>
#include <libsoftmetaroce/Constants.h>
#include <libsoftmetaroce/EgressProfile.h>
#include <libsoftmetaroce/EthernetEncapsulation.h>
#include <libsoftmetaroce/GidDiscovery.h>
#include <libsoftmetaroce/MetaRoceVerbs.h>
#include <libsoftmetaroce/PacketSocketFactory.h>
#include <libsoftmetaroce/QpState.h>
#include <libsoftmetaroce/RenamedFlag.h>
#include <libsoftmetaroce/SequenceNumbers.h>
#include <libsoftmetaroce/SgeList.h>
#include <libsoftmetaroce/WireProfiles.h>
#include <libsoftmetaroce/Wqe.h>
#include <libsoftmetaroce/congestion_control/Flags.h>
#include <libsoftmetaroce/modifier/CorruptModifier.h>
#include <libsoftmetaroce/modifier/DuplicateModifier.h>
#include <libsoftmetaroce/modifier/HeaderEditModifier.h>
#include <libsoftmetaroce/modifier/MarkEcnModifier.h>
#include <libsoftmetaroce/modifier/Modifier.h>
#include <libsoftmetaroce/modifier/PacingModifier.h>
#include <libsoftmetaroce/modifier/ReorderModifier.h>
#include <libsoftmetaroce/modifier/StripedMessageWaveModifier.h>
#include <libsoftmetaroce/modifier/TruncateModifier.h>
#include <libsoftmetaroce/modifier/WriteHeaderEditModifier.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <sstream>
#include <type_traits>
#include <utility>

#ifndef IBV_MTU_8192
constexpr ibv_mtu IBV_MTU_8192 = static_cast<ibv_mtu>(6);
#endif

namespace {

constexpr uint64_t maxMessageBytes(uint32_t mtuBytes) {
  return uint64_t{
             libsoftmetaroce::OversizeWqeException::kMaxPacketsPerMessage} *
      mtuBytes;
}

} // namespace

DEFINE_string(
    libsoftmetaroce_modifier_config,
    "",
    "Path to the libsoftmetaroce modifier config JSON file");
DEFINE_string(
    modifier_config,
    "",
    "Former name of --libsoftmetaroce_modifier_config, still accepted. Set "
    "the new name instead.");
DEFINE_string(
    libsoftmetaroce_gid,
    "",
    "Comma-separated local IP addresses to select as GIDs for sending and "
    "receiving. Outside macvlan multiport discovery, built-in entries follow this "
    "address order and registered types are appended. Selecting one macvlan parent "
    "may also expose child-interface GIDs. Empty uses eligible non-loopback "
    "addresses in getifaddrs() order.");
DEFINE_bool(
    libsoftmetaroce_ignore_modify_psn,
    false,
    "Start all QP with MSN=1 (or --libsoftmetaroce_initialmsn), CSN=0");
static bool validateFlowCount(const char*, int32_t value) {
  return value >= 1 && value <= 4096;
}
LSMR_DEFINE_RENAMED_FLAG(
    int32,
    libsoftmetaroce_txflows,
    libsoftmetaroce_txpaths,
    8,
    &validateFlowCount,
    "Number of flows to use when transmitting");
LSMR_DEFINE_RENAMED_FLAG(
    int32,
    libsoftmetaroce_rxflows,
    libsoftmetaroce_rxpaths,
    64,
    &validateFlowCount,
    "Number of flows to accept when receiving");

DEFINE_uint32(
    libsoftmetaroce_sending_threads,
    1,
    "Number of send consumer threads. 1 uses the single-consumer QpArbiter; "
    ">= 2 uses ConcurrentQpArbiter with that many parallel send consumers.");
static bool validateSendingThreads(const char*, uint32_t value) {
  // Cap at the cgroup-aware CPU count (available_concurrency), floored at 8 so
  // a 0/low-CPU/CI count still admits the default and typical values -- at the
  // cost of mild oversubscription there.
  const uint32_t maxThreads = std::max(folly::available_concurrency(), 8u);
  return value >= 1 && value <= maxThreads;
}
DEFINE_validator(libsoftmetaroce_sending_threads, &validateSendingThreads);

DEFINE_uint32(
    libsoftmetaroce_send_batch,
    1,
    "Max packets a send consumer drains from one QP per arbiter turn. 1 is one "
    "packet per claim; >1 amortizes the ready-queue round-trip and improves "
    "cache locality while the QP is owned. Only effective with sending_threads "
    ">= 2; the single-consumer arbiter does not batch.");
static bool validateSendBatch(const char*, uint32_t value) {
  return value >= 1;
}
DEFINE_validator(libsoftmetaroce_send_batch, &validateSendBatch);

DEFINE_string(
    libsoftmetaroce_sack_retransmission_mode,
    "immediate",
    "SACK retransmission mode: immediate, never, count_based_pwnd, distance_based_pwnd");
DEFINE_double(
    libsoftmetaroce_sack_retransmission_fraction,
    1.0,
    "Fraction of fwnd used as threshold for COUNT/DISTANCE modes");
static bool validateSackRetransmissionMode(
    const char*,
    const std::string& value) {
  if (libsoftmetaroce::parseSackRetransmissionMode(value).has_value()) {
    return true;
  }
  LOG(ERROR) << "Invalid sack_retransmission_mode '" << value
             << "'; valid values: immediate, never, count_based_pwnd, "
                "distance_based_pwnd";
  return false;
}
DEFINE_validator(
    libsoftmetaroce_sack_retransmission_mode,
    &validateSackRetransmissionMode);
static bool validateSackRetransmissionFraction(const char*, double value) {
  if (value >= 0.0) {
    return true;
  }
  LOG(ERROR) << "sack_retransmission_fraction must be >= 0.0, got " << value;
  return false;
}
DEFINE_validator(
    libsoftmetaroce_sack_retransmission_fraction,
    &validateSackRetransmissionFraction);

DEFINE_int32(
    libsoftmetaroce_send_to_port,
    0xace,
    "Destination port for MetaRoce traffic (default: 0xace = 2766)");
DEFINE_int32(
    libsoftmetaroce_bind_to_port,
    0xace,
    "Port the receiver binds to (default: 0xace = 2766)");

namespace libsoftmetaroce {

namespace {

std::unique_ptr<Modifier> createModifierFromConfig(
    std::unique_ptr<ModifierConfig> modifierConfig) {
  const ModifierConfig& config = *CHECK_NOTNULL(modifierConfig.get());
  if (config.type == ModifierConfig::ModifierType::Drop) {
    return std::make_unique<DropModifier>(std::move(modifierConfig));
  } else if (config.type == ModifierConfig::ModifierType::Reorder) {
    return std::make_unique<ReorderModifier>(std::move(modifierConfig));
  } else if (config.type == ModifierConfig::ModifierType::StripedMessageWave) {
    return std::make_unique<StripedMessageWaveModifier>(
        std::move(modifierConfig));
  } else if (config.type == ModifierConfig::ModifierType::Duplicate) {
    return std::make_unique<DuplicateModifier>(std::move(modifierConfig));
  } else if (config.type == ModifierConfig::ModifierType::Corrupt) {
    return std::make_unique<CorruptModifier>(std::move(modifierConfig));
  } else if (config.type == ModifierConfig::ModifierType::WriteEdit) {
    return std::make_unique<WriteHeaderEditModifier>(std::move(modifierConfig));
  } else if (config.type == ModifierConfig::ModifierType::HeaderEdit) {
    return std::make_unique<HeaderEditModifier>(std::move(modifierConfig));
  } else if (config.type == ModifierConfig::ModifierType::MarkEcn) {
    return std::make_unique<MarkEcnModifier>(std::move(modifierConfig));
  } else if (config.type == ModifierConfig::ModifierType::Pacing) {
    return std::make_unique<PacingModifier>(std::move(modifierConfig));
  } else if (config.type == ModifierConfig::ModifierType::Truncate) {
    return std::make_unique<TruncateModifier>(std::move(modifierConfig));
  } else {
    return nullptr;
  }
}

std::vector<std::string> enumerateMacvlanChildren(
    const std::string& parentIface) {
  std::vector<std::string> children;
  unsigned int parentIfindex = if_nametoindex(parentIface.c_str());
  if (parentIfindex == 0) {
    return children;
  }
  std::error_code ec;
  std::filesystem::path sysDir = "/sys/class/net/" + parentIface;
  constexpr std::string_view kUpperPrefix = "upper_";
  for (auto it = std::filesystem::directory_iterator(sysDir, ec);
       !ec && it != std::filesystem::directory_iterator();
       it.increment(ec)) {
    // filename() returns a temporary whose native() ref would dangle
    const auto name = folly::copy(it->path().filename().native());
    if (!name.starts_with(kUpperPrefix)) {
      continue;
    }
    std::string childName(name.substr(kUpperPrefix.size()));
    std::ifstream iflinkFile("/sys/class/net/" + childName + "/iflink");
    unsigned int childIflink = 0;
    if (iflinkFile >> childIflink && childIflink == parentIfindex) {
      children.push_back(std::move(childName));
    }
  }
  if (ec) {
    LOG(WARNING) << "error enumerating " << sysDir << ": " << ec.message();
  }
  std::ranges::sort(children);
  return children;
}

bool isInterfaceUp(const std::string& ifname) {
  std::ifstream file("/sys/class/net/" + ifname + "/operstate");
  std::string state;
  return (file >> state) && state == "up";
}

int32_t txFlows() {
  static const int32_t value = LSMR_RESOLVE_RENAMED_FLAG(
      libsoftmetaroce_txflows, libsoftmetaroce_txpaths);
  return value;
}

int32_t rxFlows() {
  static const int32_t value = LSMR_RESOLVE_RENAMED_FLAG(
      libsoftmetaroce_rxflows, libsoftmetaroce_rxpaths);
  return value;
}

const std::string& modifierConfigFilePath() {
  static const std::string value = LSMR_RESOLVE_RENAMED_FLAG(
      libsoftmetaroce_modifier_config, modifier_config);
  return value;
}

// An int-returning verb must both set and return errno.
int withErrno(int err) {
  errno = err;
  return err;
}

// A verb that reports -1 leaves the reason to errno.
int minusOneSettingErrno(int err) {
  errno = err;
  return -1;
}

// A pointer return has no room for the failure reason, so errno takes it.
std::nullptr_t nullSettingErrno(int err) {
  errno = err;
  return nullptr;
}

// Diagnostic logging is best-effort: a logging failure must not replace the
// error return the log annotates.
template <typename Fn>
void containLogFailure(Fn&& fn) noexcept {
  try {
    std::forward<Fn>(fn)();
  } catch (...) {
  }
}

// Maps an unexpected implementation failure without throwing through a verb.
int logUnexpectedFailure(const char* site) noexcept {
  containLogFailure([&] {
    LOG(ERROR) << "libsoftmetaroce: unexpected failure in " << site
               << "; reporting EIO";
  });
  return EIO;
}

// An ibv_pd with the context it was allocated on, out of reach of a caller
// writing the ibv_pd's own context field.
struct LsmrPd {
  struct ibv_pd ibvPd;
  const struct ibv_context* allocatingContext;
};
static_assert(
    std::is_standard_layout_v<LsmrPd> && offsetof(LsmrPd, ibvPd) == 0,
    "an ibv_pd* lsmr hands out must convert back to its LsmrPd*");

bool holdsProtectionDomain(
    const folly::F14FastSet<const struct ibv_pd*>& domains,
    const struct ibv_pd* pd) {
  return pd != nullptr && domains.contains(pd) &&
      pd->handle == kProtectionDomainMagic &&
      reinterpret_cast<const LsmrPd*>(pd)->allocatingContext == pd->context;
}

// Zero is served too, for callers that zero-initialize the attributes and
// leave the type unset.
bool isServedQueuePairType(enum ibv_qp_type type) {
  return static_cast<int>(type) == 0 || type == IBV_QPT_RC ||
      type == IBV_QPT_DRIVER;
}

bool isQueueDepthPastTheMaximum(uint32_t requested) {
  return requested > kMaxQpWorkRequests;
}

// A request of zero specifies no depth and takes the default.
uint32_t resolvedQueueDepth(uint32_t requested) {
  return requested == 0 ? kDefaultQpWorkRequests : requested;
}

bool everyPageIsMapped(void* addr, size_t length) {
  const auto base = reinterpret_cast<uint64_t>(addr);
  const uint64_t bytesUntilEnd = std::numeric_limits<uint64_t>::max() - base;
  if (length > bytesUntilEnd) {
    return false;
  }
  const auto pageSize = static_cast<uint64_t>(::sysconf(_SC_PAGESIZE));
  const uint64_t alignedStart = base & ~(pageSize - 1);
  // msync answers a span of none without reading it, so a region of no length
  // is asked for the page it starts on.
  const uint64_t spanned =
      std::max<uint64_t>(length + (base - alignedStart), 1);
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  void* const start = reinterpret_cast<void*>(alignedStart);
  return ::msync(start, spanned, MS_ASYNC) == 0;
}

} // namespace

MetaRoceVerbs::MetaRoceVerbs(const std::string& modifierConfigFilePath)
    : MetaRoceVerbs(makeSocketMakerFromConfigFile(modifierConfigFilePath)) {}

std::function<SocketWithModifier(folly::EventBase*)>
MetaRoceVerbs::makeSocketMakerFromConfigFile(
    const std::string& modifierConfigFilePath) {
  if (modifierConfigFilePath.empty()) {
    return [](folly::EventBase* evb) -> SocketWithModifier {
      return std::make_pair(
          std::make_unique<folly::AsyncUDPSocket>(evb), nullptr /*modifier*/);
    };
  }
  LOG(INFO) << "Using modifier config file: " << modifierConfigFilePath;

  auto initialConfigs = ModifierConfig::buildModifierConfigsFromJson(
      readModifierConfigFile(modifierConfigFilePath));
  const size_t numModifiers = initialConfigs.size();
  std::vector<std::shared_ptr<std::atomic<uint32_t>>> sharedCounters;
  std::vector<std::shared_ptr<StripedMessageWaveState>> sharedWaveStates;
  sharedCounters.reserve(numModifiers);
  sharedWaveStates.reserve(numModifiers);
  for (const auto& config : initialConfigs) {
    sharedCounters.push_back(std::make_shared<std::atomic<uint32_t>>(0));
    const auto* waveConfig =
        dynamic_cast<const StripedMessageWaveModifierConfig*>(config.get());
    sharedWaveStates.push_back(
        waveConfig ? std::make_shared<StripedMessageWaveState>() : nullptr);
  }

  return [modifierConfigFilePath,
          sharedCounters = std::move(sharedCounters),
          sharedWaveStates = std::move(sharedWaveStates)](
             folly::EventBase* evb) -> SocketWithModifier {
    auto modifierConfigs = ModifierConfig::buildModifierConfigsFromJson(
        readModifierConfigFile(modifierConfigFilePath));
    if (modifierConfigs.size() != sharedCounters.size() ||
        modifierConfigs.size() != sharedWaveStates.size()) {
      throw std::runtime_error(
          fmt::format(
              "Modifier config file changed between MetaRoceVerbs construction "
              "and per-path socket creation: expected {} configs, got {}",
              sharedCounters.size(),
              modifierConfigs.size()));
    }
    std::vector<std::unique_ptr<Modifier>> modifiers;
    for (size_t i = 0; i < modifierConfigs.size(); ++i) {
      modifierConfigs[i]->appliedCount = sharedCounters[i];
      if (auto* waveConfig = dynamic_cast<StripedMessageWaveModifierConfig*>(
              modifierConfigs[i].get())) {
        waveConfig->state = sharedWaveStates[i];
      }
      auto modifier = createModifierFromConfig(std::move(modifierConfigs[i]));
      if (modifier) {
        modifiers.push_back(std::move(modifier));
      }
    }

    auto modifierPtr = modifiers.empty()
        ? nullptr
        : (modifiers.size() == 1
               ? std::move(modifiers[0])
               : std::make_unique<CompositeModifier>(std::move(modifiers)));
    return std::make_pair(
        std::make_unique<folly::AsyncUDPSocket>(evb), std::move(modifierPtr));
  };
}

MetaRoceVerbs::MetaRoceVerbs(
    const std::function<SocketWithModifier(folly::EventBase*)>& socketMaker)
    : socketMaker_(socketMaker),
      metaRoceSendToPort_(
          static_cast<uint16_t>(FLAGS_libsoftmetaroce_send_to_port)),
      metaRoceBindToPort_(
          static_cast<uint16_t>(FLAGS_libsoftmetaroce_bind_to_port)) {
  // Pays the slow first heavy fence eagerly, before any traffic.
  folly::asymmetric_thread_fence_heavy(std::memory_order_seq_cst);
  installWireProfiles();
  attachConnectionObservers([this](ConnectionObserver& observer) {
    registerConnectionObserver(observer);
  });
  // Populates devices_ and gid_table_.
  getLocalAddrAndDevicesList();

  if (gid_table_.empty()) {
    // Clean up devices_ allocated by getLocalAddrAndDevicesList() before
    // throwing -- the destructor only runs on fully-constructed objects, so
    // any new ibv_device the helper allocated would otherwise leak.
    for (ibv_device* device : devices_) {
      delete device;
    }
    devices_.clear();
    if (!FLAGS_libsoftmetaroce_gid.empty()) {
      throw std::runtime_error(
          fmt::format(
              "None of the requested libsoftmetaroce_gid ip addresses ({}) were "
              "found on host",
              FLAGS_libsoftmetaroce_gid));
    } else {
      throw std::runtime_error("Failed to find any ip addresses");
    }
  }
  const uint32_t sendingThreads = FLAGS_libsoftmetaroce_sending_threads;
  if (sendingThreads >= 2) {
    arbiter_ = std::make_unique<ConcurrentQpArbiter>();
  } else {
    arbiter_ = std::make_unique<QpArbiter>();
  }
  sender_ = std::make_unique<Sender>(
      *arbiter_,
      socketMaker,
      metaRoceSendToPort_,
      sendingThreads,
      FLAGS_libsoftmetaroce_send_batch);
  rcnRateHint_ =
      std::make_unique<RcnRateHint>(FLAGS_libsoftmetaroce_nic_rate_gbps);
  pingController_ = std::make_unique<PingController>(
      socketMaker_, metaRoceSendToPort_, &activePortSet_);
}

MetaRoceVerbs::MetaRoceVerbs() : MetaRoceVerbs(modifierConfigFilePath()) {}

void MetaRoceVerbs::publishTagConfig(const AppliedTagConfig& config) {
  if (!config.hasAppliedValue()) {
    return;
  }
  for (const auto& observer : connectionObservers_) {
    observer->onTagConfigured(config);
  }
}

void MetaRoceVerbs::publishQpRetirement(uint32_t localQpNum) {
  for (const auto& observer : connectionObservers_) {
    observer->onQpRetiring(localQpNum);
  }
}

void MetaRoceVerbs::registerConnectionObserver(ConnectionObserver& observer) {
  auto locked = qpNumToQp_.wlock();
  CHECK(locked->empty()) << "observers must be registered before any QP exists";
  connectionObservers_.push_back(&observer);
}

folly::IPAddress MetaRoceVerbs::gidEntryToIPAddress(
    const ibv_gid_entry& entry) {
  auto v6 = folly::IPAddressV6::fromBinary(
      folly::ByteRange(entry.gid.raw, sizeof(entry.gid.raw)));
  if (v6.isIPv4Mapped()) {
    return v6.createIPv4();
  }
  return v6;
}

void MetaRoceVerbs::getLocalAddrAndDevicesList() {
  struct ifaddrs* ifAddrs = nullptr;
  if (getifaddrs(&ifAddrs) == -1) {
    LOG(ERROR) << "getifaddrs failed, errno: " << errno;
    return;
  }

  // Probe whether we can open raw sockets (CAP_NET_RAW). If so, we
  // can offer link-local addresses as Ethernet GIDs.
  bool hasRawCapability = false;
  int probeFd = socket(AF_PACKET, SOCK_RAW, 0);
  if (probeFd >= 0) {
    hasRawCapability = true;
    close(probeFd);
  }

  // Parse and deduplicate --libsoftmetaroce_gid while preserving first-seen
  // order.
  std::vector<std::string> requestedGidOrder;
  std::set<std::string> requestedGidSet;
  if (!FLAGS_libsoftmetaroce_gid.empty()) {
    std::vector<folly::StringPiece> parts;
    folly::split(',', FLAGS_libsoftmetaroce_gid, parts);
    for (auto p : parts) {
      auto trimmed = folly::trimWhitespace(p).str();
      if (!trimmed.empty() && requestedGidSet.insert(trimmed).second) {
        requestedGidOrder.push_back(std::move(trimmed));
      }
    }
  }

  std::set<std::string> deviceNames;

  ibv_device* dev = new ibv_device();
  strcpy(dev->name, "lsmr0");
  devices_.clear();
  devices_.push_back(dev);

  gid_table_.clear();
  // Collect every eligible local IP. Also build a map from interface name to
  // addresses and from address to interface name, used for macvlan child
  // discovery.
  DiscoveryTables tables;
  const ScanConditions conditions{
      .explicitGidsSelected = !requestedGidSet.empty(),
      .hasRawCapability = hasRawCapability,
  };
  const auto ruleChain = defaultGidRuleRegistry().makeChain(conditions);
  for (auto ifa = ifAddrs; ifa != nullptr; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == nullptr) {
      continue;
    }
    if (ifa->ifa_flags & IFF_LOOPBACK) {
      continue;
    }
    deviceNames.insert(ifa->ifa_name);

    const auto candidate = makeDiscoveredAddress(*ifa);
    if (!candidate) {
      continue;
    }
    const auto claims = ruleChain.classify(*candidate);
    if (claims.empty()) {
      continue;
    }

    const auto addrStr = candidate->scopelessAddress.str();
    const auto requestedByUser =
        requestedGidSet.empty() || requestedGidSet.contains(addrStr);
    const auto record = [&](uint32_t gidType, bool fromBuiltInRule) {
      recordGid(
          *candidate,
          gidType,
          requestedByUser,
          defaultEgressRegistry().getProfile(gidType).socketKind ==
              SocketKind::RAW,
          fromBuiltInRule,
          tables);
    };
    for (const auto gidType : claims.builtIn) {
      record(gidType, /*fromBuiltInRule=*/true);
    }
    for (const auto gidType : claims.registered) {
      record(gidType, /*fromBuiltInRule=*/false);
    }
  }

  if (!tryDiscoverMultiport(requestedGidOrder, tables)) {
    gid_table_ = orderGidTable(
        requestedGidOrder.empty() ? tables.discoveredOrder : requestedGidOrder,
        tables.discovered);
  }

  LOG(INFO) << "Gids: ";
  for (const auto& entry : gid_table_) {
    LOG(INFO) << gidEntryToIPAddress(entry).str();
  }
  LOG(INFO) << "Devices: " << devices_.size();
  if (ifAddrs != nullptr) {
    freeifaddrs(ifAddrs);
  }
}

bool MetaRoceVerbs::tryDiscoverMultiport(
    const std::vector<std::string>& requestedGidOrder,
    DiscoveryTables& tables) {
  if (requestedGidOrder.size() != 1) {
    return false;
  }
  auto addrIt = tables.interfaceNameByAddress.find(requestedGidOrder[0]);
  if (addrIt == tables.interfaceNameByAddress.end()) {
    return false;
  }
  auto children = enumerateMacvlanChildren(addrIt->second);
  if (children.empty()) {
    return false;
  }

  if (auto parentIt = tables.all.find(requestedGidOrder[0]);
      parentIt != tables.all.end()) {
    parentIt->second.gid_index = static_cast<uint32_t>(gid_table_.size());
    gid_table_.push_back(parentIt->second);
  }
  if (children.size() > kMaxPorts) {
    children.resize(kMaxPorts);
  }
  portInterfaces_ = children;
  uint16_t portSet = 0;
  for (size_t i = 0; i < children.size(); i++) {
    if (auto childIt = tables.byInterfaceName.find(children[i]);
        childIt != tables.byInterfaceName.end()) {
      if (isInterfaceUp(children[i])) {
        portSet |= static_cast<uint16_t>(1u << i);
      }
      childIt->second.gid_index = static_cast<uint32_t>(gid_table_.size());
      gid_table_.push_back(childIt->second);
    } else {
      LOG(WARNING) << "macvlan child " << children[i]
                   << " has no usable address; port " << i
                   << " gets placeholder GID entry";
      ibv_gid_entry placeholder{};
      placeholder.gid_index = static_cast<uint32_t>(gid_table_.size());
      gid_table_.push_back(placeholder);
    }
  }
  activePortSet_.store(portSet, std::memory_order_relaxed);
  LOG(INFO) << "Multiport: " << children.size() << " ports on "
            << addrIt->second << ", active port set: 0x" << std::hex << portSet
            << std::dec;
  return true;
}

const std::string MetaRoceVerbs::readModifierConfigFile(
    const std::string& modifierConfigFilePath) {
  std::ifstream file(modifierConfigFilePath);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file: " + modifierConfigFilePath);
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  file.close();
  return buffer.str();
}

namespace {
// Events a client may leave unread before a raise starts dropping them, well
// under what the pipe behind async_fd holds.
constexpr size_t kMaxPendingAsyncEvents = 64;

struct LsmrContext {
  struct ibv_context ibvContext;
  // The write end of the pipe whose read end the client polls as `async_fd`.
  int asyncWriteFd;
};
static_assert(
    std::is_standard_layout_v<LsmrContext>,
    "LsmrContext must be standard-layout for the ibv_context <-> LsmrContext "
    "pointer interconvertibility below to be well-defined");

// An ibv_comp_channel with the write end of the pipe whose read end the client
// reads as `fd`.
struct LsmrCompChannel {
  struct ibv_comp_channel ibvChannel;
  int writeFd;
};
static_assert(
    std::is_standard_layout_v<LsmrCompChannel> &&
        offsetof(LsmrCompChannel, ibvChannel) == 0,
    "an ibv_comp_channel* lsmr hands out must convert back to its wrapper");
static_assert(
    offsetof(LsmrContext, ibvContext) == 0,
    "ibv_context must be the first member so an ibv_context* aliases the "
    "LsmrContext* (reinterpret_cast and free below depend on this)");

// Logs a dropped async event without allowing logging exceptions to escape.
void logDroppedAsyncEvent(uint64_t contextId) noexcept {
  try {
    LOG_FIRST_N(ERROR, 20)
        << "context " << contextId
        << " failed to queue an async event; dropping the event";
  } catch (...) {
  }
}

// Writes a wakeup byte to `channel`. Returns false only where no byte may be
// waiting: a full pipe already holds bytes that wake a reader.
// Caller holds `compChannels_` for writing.
bool wakeCompChannel(const LsmrCompChannel& channel) {
  const char raised = 1;
  ssize_t wrote = 0;
  do {
    wrote = write(channel.writeFd, &raised, sizeof(raised));
  } while (wrote < 0 && errno == EINTR);
  return wrote == sizeof(raised) || (wrote < 0 && errno == EAGAIN);
}
} // namespace

struct ibv_context* FOLLY_NULLABLE
MetaRoceVerbs::ibv_open_device(struct ibv_device* device) {
  // The read end is the `async_fd` a client polls; the write end stays here so
  // an event raised against this context wakes that poll.
  // Neither end may block: a raise writes to one where a client leaves what it
  // wrote unread, and a get reads the other where a client drained it first.
  int pipeFds[2];
  if (pipe2(pipeFds, O_NONBLOCK | O_CLOEXEC) < 0) {
    return nullptr;
  }
  auto closePipe = folly::makeGuard([&pipeFds] {
    const int saved = errno;
    close(pipeFds[0]);
    close(pipeFds[1]);
    errno = saved;
  });
  auto* wrapper = static_cast<LsmrContext*>(malloc(sizeof(LsmrContext)));
  if (wrapper == nullptr) {
    return nullSettingErrno(ENOMEM);
  }
  wrapper->asyncWriteFd = pipeFds[1];
  wrapper->ibvContext = {
      device,
      ibv_context_ops(),
      0 /*cmd_fd*/,
      pipeFds[0] /*async_fd*/,
      kCompletionVectorCount /*num_comp_vectors*/,
      pthread_mutex_t(),
      nullptr /*abi_compat*/};
  try {
    contextEvents_.wlock()->emplace(
        &wrapper->ibvContext,
        AsyncEventQueue{
            .contextId =
                nextContextId_.fetch_add(1, std::memory_order_relaxed)});
  } catch (const std::bad_alloc&) {
    free(wrapper);
    return nullSettingErrno(ENOMEM);
  } catch (...) {
    free(wrapper);
    return nullSettingErrno(logUnexpectedFailure("ibv_open_device"));
  }
  closePipe.dismiss();
  return &wrapper->ibvContext;
}

int MetaRoceVerbs::ibv_close_device(struct ibv_context* context) {
  if (context == nullptr) {
    return minusOneSettingErrno(EINVAL);
  }
  if (contextEvents_.wlock()->erase(context) == 0) {
    return minusOneSettingErrno(ENOENT);
  }
  auto* wrapper = reinterpret_cast<LsmrContext*>(context);
  close(wrapper->ibvContext.async_fd);
  close(wrapper->asyncWriteFd);
  free(wrapper);
  return 0;
}

uint64_t MetaRoceVerbs::asyncEventIdOf(struct ibv_context* context) const {
  auto lockedEvents = contextEvents_.rlock();
  const auto it = lockedEvents->find(context);
  return it == lockedEvents->end() ? 0 : it->second.contextId;
}

bool MetaRoceVerbs::raiseAsyncEvent(
    struct ibv_context* context,
    uint64_t contextId,
    struct ibv_async_event event) {
  if (context == nullptr) {
    return false;
  }
  // Async events can originate in destructors and sender loops, so any
  // failure below drops the event.
  try {
    auto lockedEvents = contextEvents_.wlock();
    const auto it = lockedEvents->find(context);
    // An address a close gives up can be handed to a later open, which gets
    // its own id. Both guard the dereference below, valid only while open.
    if (it == lockedEvents->end() || it->second.contextId != contextId) {
      return false;
    }
    if (it->second.events.size() >= kMaxPendingAsyncEvents) {
      LOG_FIRST_N(ERROR, 20)
          << "context " << contextId << " holds " << it->second.events.size()
          << " async events a client has not taken, so this one is dropped";
      return false;
    }
    it->second.events.push_back(event);
    // A byte apiece, so a client's poll of async_fd reports ready for exactly
    // the events queued above and ibv_get_async_event takes one of each.
    const char raised = 1;
    const auto* wrapper = reinterpret_cast<const LsmrContext*>(context);
    if (write(wrapper->asyncWriteFd, &raised, sizeof(raised)) !=
        sizeof(raised)) {
      it->second.events.pop_back();
      LOG_FIRST_N(ERROR, 20)
          << "context " << context
          << " could not be woken for an async event, which is dropped rather "
             "than left for a poll that will not report it";
      return false;
    }
    return true;
  } catch (...) {
    logDroppedAsyncEvent(contextId);
    return false;
  }
}

size_t MetaRoceVerbs::purgeAsyncEventsForCq(const struct ibv_cq* cq) {
  return purgeAsyncEvents([cq](const struct ibv_async_event& event) {
    return event.event_type == IBV_EVENT_CQ_ERR && event.element.cq == cq;
  });
}

size_t MetaRoceVerbs::purgeAsyncEventsForSrq(const struct ibv_srq* srq) {
  return purgeAsyncEvents([srq](const struct ibv_async_event& event) {
    return event.event_type == IBV_EVENT_SRQ_LIMIT_REACHED &&
        event.element.srq == srq;
  });
}

size_t MetaRoceVerbs::purgeAsyncEvents(
    const std::function<bool(const struct ibv_async_event&)>& matches) {
  size_t purged = 0;
  auto lockedEvents = contextEvents_.wlock();
  for (auto& [context, queue] : *lockedEvents) {
    const size_t held = queue.events.size();
    purged += std::erase_if(queue.events, matches);
    // The byte each dropped event wrote goes with it, so the descriptor is
    // readable once per event that remains.
    size_t bytesMissing = 0;
    for (size_t taken = queue.events.size(); taken < held; ++taken) {
      char raised = 0;
      if (read(context->async_fd, &raised, sizeof(raised)) != sizeof(raised)) {
        ++bytesMissing;
      }
    }
    if (bytesMissing > 0) {
      LOG_FIRST_N(ERROR, 20)
          << "context " << context << " had no wakeup byte for " << bytesMissing
          << " events dropped with the object they were raised for, so the "
             "descriptor stays readable with no event behind it";
    }
  }
  return purged;
}

int MetaRoceVerbs::ibv_get_async_event(
    struct ibv_context* context,
    struct ibv_async_event* event) {
  if (context == nullptr || event == nullptr) {
    return minusOneSettingErrno(EINVAL);
  }
  auto lockedEvents = contextEvents_.wlock();
  const auto it = lockedEvents->find(context);
  if (it == lockedEvents->end()) {
    return minusOneSettingErrno(ENOENT);
  }
  if (it->second.events.empty()) {
    return minusOneSettingErrno(EAGAIN);
  }
  *event = it->second.events.front();
  it->second.events.pop_front();
  // The byte the raise wrote for this event, under the lock the entry was
  // found under: a close cannot take the descriptor between the two.
  char raised = 0;
  if (read(context->async_fd, &raised, sizeof(raised)) != sizeof(raised)) {
    LOG_FIRST_N(ERROR, 20)
        << "context " << context
        << " had no wakeup byte for an event it held, so a later poll may "
           "report a readiness no event answers";
  }
  return 0;
}

void MetaRoceVerbs::ibv_ack_async_event(struct ibv_async_event* event) {
  if (event == nullptr) {
    return;
  }
  if (event->event_type == IBV_EVENT_CQ_ERR) {
    if (event->element.cq == nullptr) {
      LOG_FIRST_N(ERROR, 20)
          << "an event acked with no completion queue was not one this library "
             "handed out, and this verb has no return to refuse it with";
      return;
    }
    completionQueueOf(*event->element.cq).releaseEventReferences(1);
  } else if (event->event_type == IBV_EVENT_SRQ_LIMIT_REACHED) {
    if (event->element.srq == nullptr) {
      LOG_FIRST_N(ERROR, 20)
          << "an event acked with no shared receive queue was not one this "
             "library handed out, and this verb has no return to refuse it "
             "with";
      return;
    }
    sharedReceiveQueueOf(*event->element.srq).releaseEventReferences(1);
  }
}

int MetaRoceVerbs::ibv_query_device(
    struct ibv_context*,
    struct ibv_device_attr* device_attr) {
  if (device_attr == nullptr) {
    return withErrno(EINVAL);
  }
  memset(device_attr, 0, sizeof(struct ibv_device_attr));
  device_attr->max_mr = kMaxMemoryRegions;
  device_attr->max_cq = kMaxCompletionQueues;
  device_attr->max_cqe = kMaxCqEntries;
  device_attr->max_qp = kMaxQueuePairs;
  device_attr->max_qp_wr = kMaxQpWorkRequests;
  device_attr->max_sge = kMaxSgesPerWorkRequest;
  device_attr->max_srq = kMaxSharedReceiveQueues;
  device_attr->max_pd = kMaxProtectionDomains;
  device_attr->max_srq_wr = kMaxSrqWorkRequests;
  device_attr->max_srq_sge = kMaxSgesPerWorkRequest;
  device_attr->max_qp_rd_atom = kMaxOutstandingReadAtomic;
  device_attr->max_qp_init_rd_atom = kMaxOutstandingReadAtomic;
  device_attr->max_res_rd_atom = kMaxQpWorkRequests;
  device_attr->phys_port_cnt = kPhysicalPortCount;
  // Atomics execute on the responder's own memory, so they are atomic against
  // the other operations reaching it through this device.
  device_attr->atomic_cap = IBV_ATOMIC_HCA;
  return 0;
}

int MetaRoceVerbs::ibv_query_port(
    struct ibv_context* /*context*/,
    uint8_t /*port_num*/,
    struct ibv_port_attr* port_attr) {
  if (port_attr == nullptr) {
    return withErrno(EINVAL);
  }
  memset(port_attr, 0, sizeof(*port_attr));
  port_attr->state = IBV_PORT_ACTIVE;
  port_attr->max_mtu = IBV_MTU_8192;
  port_attr->active_mtu = IBV_MTU_8192;
  port_attr->max_msg_sz = static_cast<uint32_t>(
      maxMessageBytes(1u << (IBV_MTU_8192 + kMtuEnumShift)));
  port_attr->link_layer = IBV_LINK_LAYER_ETHERNET;
  // Fixed verbs width; the GID count always fits.
  // NOLINTNEXTLINE(clang-diagnostic-shorten-64-to-32)
  port_attr->gid_tbl_len = gid_table_.size();
  return 0;
}

int MetaRoceVerbs::ibv_query_gid(
    struct ibv_context* /*context*/,
    uint8_t /*port_num*/,
    int index,
    union ibv_gid* gid) {
  if (gid == nullptr) {
    return minusOneSettingErrno(EINVAL);
  }
  if (static_cast<size_t>(index) >= gid_table_.size()) {
    return minusOneSettingErrno(EINVAL);
  }
  memcpy(gid->raw, gid_table_[index].gid.raw, sizeof(gid->raw));
  return 0;
}

int MetaRoceVerbs::ibv_query_gid_ex(
    struct ibv_context* /*context*/,
    uint8_t /*portNum*/,
    int index,
    struct ibv_gid_entry* entry,
    uint32_t /*flags*/) {
  if (entry == nullptr) {
    return withErrno(EINVAL);
  }
  if (static_cast<size_t>(index) >= gid_table_.size()) {
    return withErrno(EINVAL);
  }
  *entry = gid_table_[index];
  return 0;
}

// there is a single lsmr device.
struct ibv_device** MetaRoceVerbs::ibv_get_device_list(int* num_devices) {
  if (num_devices != nullptr) {
    // The count width is fixed by the verbs interface; one device always fits.
    // NOLINTNEXTLINE(clang-diagnostic-shorten-64-to-32)
    *num_devices = devices_.size();
  }
  return devices_.data();
}

void MetaRoceVerbs::ibv_free_device_list(struct ibv_device** /*list*/) {
  // free is the reflection of get_device_list in case we provided a copy,
  // which we did not.
}

struct ibv_pd* FOLLY_NULLABLE
MetaRoceVerbs::ibv_alloc_pd(struct ibv_context* context) {
  auto* wrapper = static_cast<LsmrPd*>(malloc(sizeof(LsmrPd)));
  if (wrapper == nullptr) {
    return nullSettingErrno(ENOMEM);
  }
  wrapper->ibvPd = {context, kProtectionDomainMagic};
  wrapper->allocatingContext = context;
  struct ibv_pd* pd = &wrapper->ibvPd;
  try {
    auto lockedProtectionDomains = protectionDomains_.wlock();
    if (lockedProtectionDomains->size() >= kMaxProtectionDomains) {
      free(pd);
      return nullSettingErrno(ENOSPC);
    }
    lockedProtectionDomains->insert(pd);
  } catch (const std::bad_alloc&) {
    free(pd);
    return nullSettingErrno(ENOMEM);
  } catch (...) {
    free(pd);
    return nullSettingErrno(logUnexpectedFailure("ibv_alloc_pd"));
  }
  return pd;
}

bool MetaRoceVerbs::isProtectionDomainInUse(const struct ibv_pd* pd) const {
  if (memoryRegions_.holdsRegionOf(pd)) {
    return true;
  }
  {
    const auto lockedSrqs = handleToSrq_.rlock();
    for (const auto& [_, srq] : *lockedSrqs) {
      if (srq->getProtectionDomain() == pd) {
        return true;
      }
    }
  }
  const auto lockedQps = qpNumToQp_.rlock();
  for (const auto& [_, qp] : *lockedQps) {
    if (qp->getProtectionDomain() == pd) {
      return true;
    }
  }
  return false;
}

int MetaRoceVerbs::ibv_dealloc_pd(struct ibv_pd* pd) {
  if (pd == nullptr) {
    return withErrno(EINVAL);
  }
  {
    // A create takes the read lock this excludes from its membership test
    // through registering its object, so an object of this domain is found.
    auto lockedProtectionDomains = protectionDomains_.wlock();
    if (!holdsProtectionDomain(*lockedProtectionDomains, pd)) {
      return withErrno(ENOENT);
    }
    if (isProtectionDomainInUse(pd)) {
      return withErrno(EBUSY);
    }
    lockedProtectionDomains->erase(pd);
  }
  free(reinterpret_cast<LsmrPd*>(pd));
  return 0;
}

struct ibv_comp_channel* FOLLY_NULLABLE
MetaRoceVerbs::ibv_create_comp_channel(struct ibv_context* context) {
  if (context == nullptr) {
    return nullSettingErrno(EINVAL);
  }
  // The read end blocks, as ibv_get_cq_event(3) does on a fresh channel; the
  // write end does not, so a raise never waits on a client.
  int pipeFds[2];
  if (pipe2(pipeFds, O_CLOEXEC) < 0) {
    return nullptr;
  }
  auto closePipe = folly::makeGuard([&pipeFds] {
    const int saved = errno;
    close(pipeFds[0]);
    close(pipeFds[1]);
    errno = saved;
  });
  if (fcntl(pipeFds[1], F_SETFL, O_NONBLOCK) < 0) {
    return nullptr;
  }
  auto* wrapper =
      static_cast<LsmrCompChannel*>(malloc(sizeof(LsmrCompChannel)));
  if (wrapper == nullptr) {
    return nullSettingErrno(ENOMEM);
  }
  wrapper->ibvChannel = {context, pipeFds[0], 0 /*refcnt*/};
  wrapper->writeFd = pipeFds[1];
  try {
    compChannels_.wlock()->emplace(&wrapper->ibvChannel, CompChannelState{});
  } catch (...) {
    free(wrapper);
    return nullSettingErrno(ENOMEM);
  }
  closePipe.dismiss();
  return &wrapper->ibvChannel;
}

int MetaRoceVerbs::ibv_destroy_comp_channel(struct ibv_comp_channel* channel) {
  if (channel == nullptr) {
    return withErrno(EINVAL);
  }
  {
    auto lockedChannels = compChannels_.wlock();
    const auto it = lockedChannels->find(channel);
    if (it == lockedChannels->end()) {
      return withErrno(ENOENT);
    }
    if (it->second.completionQueues > 0) {
      return withErrno(EBUSY);
    }
    lockedChannels->erase(it);
  }
  auto* wrapper = reinterpret_cast<LsmrCompChannel*>(channel);
  close(wrapper->ibvChannel.fd);
  close(wrapper->writeFd);
  free(wrapper);
  return 0;
}

void MetaRoceVerbs::raiseCompletionEvent(struct ibv_cq* cq) {
  try {
    auto lockedChannels = compChannels_.wlock();
    const auto it = lockedChannels->find(cq->channel);
    if (it == lockedChannels->end()) {
      return;
    }
    auto& events = it->second.events;
    const bool wasEmpty = events.empty();
    events.push_back(cq);
    // Events already waiting have a byte written for them, or a reader that
    // took it and replaces it under this lock.
    if (wasEmpty &&
        !wakeCompChannel(*reinterpret_cast<LsmrCompChannel*>(cq->channel))) {
      LOG_FIRST_N(ERROR, 20)
          << "completion channel " << cq->channel
          << " could not be woken; its event waits for the next reader";
    }
  } catch (...) {
    // Only the push can throw. Linux uverbs also drops a notification it cannot
    // allocate; the queue stays disarmed until the client re-arms it.
    LOG_FIRST_N(ERROR, 20) << "a completion event could not be queued";
  }
}

int MetaRoceVerbs::ibv_req_notify_cq(
    struct ibv_cq* cq,
    int /*solicited_only*/) {
  // MetaRoCE carries no solicited bit, so a solicited-only request notifies on
  // any completion: a spurious wakeup is safe where a missed one is not.
  if (cq == nullptr) {
    return withErrno(EINVAL);
  }
  const auto lockedCqs = handleToCq_.rlock();
  const auto it = lockedCqs->find(cq->handle);
  if (it == lockedCqs->end() || it->second == nullptr) {
    return withErrno(ENOENT);
  }
  if (cq->channel == nullptr) {
    return withErrno(EINVAL);
  }
  it->second->requestNotification();
  return 0;
}

int MetaRoceVerbs::ibv_get_cq_event(
    struct ibv_comp_channel* channel,
    struct ibv_cq** cq,
    void** cq_context) {
  if (channel == nullptr || cq == nullptr || cq_context == nullptr) {
    return minusOneSettingErrno(EINVAL);
  }
  if (!compChannels_.rlock()->contains(channel)) {
    return minusOneSettingErrno(ENOENT);
  }
  struct ibv_cq* taken = nullptr;
  // ibv_destroy_cq removes a queue's events but leaves the channel's wakeup
  // byte; reading it, the loop waits for the next event, as Linux uverbs does.
  do {
    char raised = 0;
    const ssize_t got = read(channel->fd, &raised, sizeof(raised));
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }
    if (got == 0) {
      return minusOneSettingErrno(EIO);
    }
    auto lockedChannels = compChannels_.wlock();
    const auto it = lockedChannels->find(channel);
    if (it == lockedChannels->end()) {
      return minusOneSettingErrno(ENOENT);
    }
    if (!it->second.events.empty()) {
      taken = it->second.events.front();
      it->second.events.pop_front();
      // The channel lock also covers `ibv_destroy_cq`'s unacked-event check.
      completionQueueOf(*taken).takeRetrievedCompletionEvent();
      if (!it->second.events.empty() &&
          !wakeCompChannel(*reinterpret_cast<LsmrCompChannel*>(channel))) {
        LOG_FIRST_N(ERROR, 20)
            << "completion channel " << channel
            << " could not be woken for its remaining events";
      }
    }
  } while (taken == nullptr);
  *cq = taken;
  *cq_context = taken->cq_context;
  return 0;
}

void MetaRoceVerbs::ibv_ack_cq_events(struct ibv_cq* cq, unsigned int nevents) {
  if (cq == nullptr || nevents == 0) {
    return;
  }
  completionQueueOf(*cq).releaseRetrievedCompletionEvents(nevents);
}

namespace {
// An ibv_cq with an lsmr-private trailing field. ibv_cq is the first member, so
// an ibv_cq* returned to callers aliases the LsmrCq* and ibv_poll_cq can
// recover the internal CompletionQueue* from the cq pointer alone -- no
// handleToCq_ rwlock on the busy-poll hot path -- while cq_context stays the
// caller's field (the libibverbs contract: an opaque pointer returned via
// completion events). This is the intrusive container_of pattern; lsmr owns
// alloc/free of the struct (ibv_create_cq / ibv_destroy_cq), so the larger
// allocation is safe.
struct LsmrCq {
  struct ibv_cq ibvCq;
  CompletionQueue* completionQueue;
};
// Standard-layout + ibvCq at offset 0 make an ibv_cq* and the enclosing LsmrCq*
// pointer-interconvertible, so the reinterpret_cast in completionQueueOf and
// the free in ibv_destroy_cq are well-defined regardless of compiler
// padding/alignment. These asserts fail the build if a future member breaks
// that assumption.
static_assert(
    std::is_standard_layout_v<LsmrCq>,
    "LsmrCq must be standard-layout for the ibv_cq <-> LsmrCq pointer "
    "interconvertibility below to be well-defined");
static_assert(
    offsetof(LsmrCq, ibvCq) == 0,
    "ibv_cq must be the first member so an ibv_cq* aliases the LsmrCq* "
    "(reinterpret_cast and free below depend on this)");

struct LsmrSrq {
  struct ibv_srq ibvSrq;
  SharedReceiveQueue* sharedReceiveQueue;
};
static_assert(
    std::is_standard_layout_v<LsmrSrq>,
    "LsmrSrq must be standard-layout for the ibv_srq <-> LsmrSrq pointer "
    "interconvertibility below to be well-defined");
static_assert(
    offsetof(LsmrSrq, ibvSrq) == 0,
    "ibv_srq must be the first member so an ibv_srq* aliases the LsmrSrq* "
    "(reinterpret_cast and free below depend on this)");

} // namespace

CompletionQueue& MetaRoceVerbs::completionQueueOf(struct ibv_cq& cq) {
  return *CHECK_NOTNULL(reinterpret_cast<LsmrCq*>(&cq)->completionQueue);
}

SharedReceiveQueue& MetaRoceVerbs::sharedReceiveQueueOf(struct ibv_srq& srq) {
  return *CHECK_NOTNULL(reinterpret_cast<LsmrSrq*>(&srq)->sharedReceiveQueue);
}

struct ibv_cq* FOLLY_NULLABLE MetaRoceVerbs::ibv_create_cq(
    struct ibv_context* context,
    int sizeOfQueue,
    void* cq_context,
    struct ibv_comp_channel* channel,
    int comp_vector) {
  if (sizeOfQueue <= 0 || static_cast<uint32_t>(sizeOfQueue) > kMaxCqEntries) {
    return nullSettingErrno(EINVAL);
  }
  if (comp_vector < 0 || comp_vector >= kCompletionVectorCount) {
    return nullSettingErrno(EINVAL);
  }
  // Counted before the queue exists, so a destroy of the channel racing this
  // create finds it in use.
  if (channel != nullptr) {
    auto lockedChannels = compChannels_.wlock();
    const auto it = lockedChannels->find(channel);
    if (it == lockedChannels->end()) {
      return nullSettingErrno(EINVAL);
    }
    ++it->second.completionQueues;
  }
  auto releaseChannel = folly::makeGuard([this, channel] {
    if (channel != nullptr) {
      auto lockedChannels = compChannels_.wlock();
      const auto it = lockedChannels->find(channel);
      if (it != lockedChannels->end()) {
        --it->second.completionQueues;
      }
    }
  });
  auto* wrapper = static_cast<LsmrCq*>(malloc(sizeof(LsmrCq)));
  if (wrapper == nullptr) {
    return nullSettingErrno(ENOMEM);
  }
  auto freeWrapper = folly::makeGuard([wrapper] { free(wrapper); });
  // Allocate the CQ and cache its CompletionQueue* in our trailing field so the
  // busy-polled ibv_poll_cq hot path resolves it directly instead of taking the
  // handleToCq_ rwlock on every call. allocateCompletionQueue hands back the
  // inserted pointer under the write lock it already holds, so there is no
  // second (read-locked) lookup. handleToCq_ remains the owner.
  std::optional<AllocatedCq> allocated;
  try {
    // Safe to capture: each QP holds a reference on cq until its teardown stops
    // completing into it, and ibv_destroy_cq returns EBUSY until then.
    allocated = allocateCompletionQueue(
        static_cast<uint32_t>(sizeOfQueue),
        [this,
         context,
         contextId = asyncEventIdOf(context),
         cq = &wrapper->ibvCq] {
          struct ibv_async_event event{};
          event.element.cq = cq;
          event.event_type = IBV_EVENT_CQ_ERR;
          // Taken before the raise publishes the event and wakes the client.
          CompletionQueue& queue = completionQueueOf(*cq);
          queue.takeEventReference();
          if (!raiseAsyncEvent(context, contextId, event)) {
            queue.releaseEventReferences(1);
          }
        },
        channel == nullptr
            ? std::function<void()>{}
            : std::function<void()>{
                  [this, cq = &wrapper->ibvCq] { raiseCompletionEvent(cq); }});
  } catch (const std::bad_alloc&) {
    return nullSettingErrno(ENOMEM);
  } catch (...) {
    return nullSettingErrno(logUnexpectedFailure("ibv_create_cq"));
  }
  if (!allocated) {
    // Past the advertised max_cq: a caller mistake; errno only, no log.
    return nullSettingErrno(ENOSPC);
  }
  wrapper->completionQueue = allocated->cq;
  wrapper->ibvCq = {
      .context = context,
      .channel = channel, // nullptr from rdma_gen
      .cq_context = cq_context, // opaque caller pointer; left untouched
      .handle = allocated->handle, // allows finding the Completion Queue object
      .cqe = sizeOfQueue,
      .mutex = pthread_mutex_t(),
      .cond = pthread_cond_t(),
      .comp_events_completed = 0,
      .async_events_completed = 0};
  freeWrapper.dismiss();
  releaseChannel.dismiss();
  return &wrapper->ibvCq;
}

CompletionQueue* FOLLY_NULLABLE
MetaRoceVerbs::referenceCompletionQueue(struct ibv_cq& cq) {
  const auto lockedCqs = handleToCq_.rlock();
  const auto it = lockedCqs->find(cq.handle);
  if (it == lockedCqs->end()) {
    return nullptr;
  }
  // ibv_destroy_cq erases under the write lock this read lock excludes, so it
  // either ran before the find above or finds the reference already held.
  it->second->takeReference();
  return it->second.get();
}

int MetaRoceVerbs::ibv_destroy_cq(struct ibv_cq* cq) {
  if (cq == nullptr) {
    return withErrno(EINVAL);
  }
  // Held past the erase, so an ack arriving during the wait below reaches a
  // queue this still owns.
  std::unique_ptr<CompletionQueue> queue;
  {
    auto lockedCqs = handleToCq_.wlock();
    const auto it = lockedCqs->find(cq->handle);
    if (it == lockedCqs->end()) {
      return withErrno(ENOENT);
    }
    if (it->second == nullptr) {
      return withErrno(ENOENT);
    }
    // A completion queue a work queue can still report into is kept.
    if (it->second->isReferenced()) {
      return withErrno(EBUSY);
    }
    if (cq->channel != nullptr) {
      auto lockedChannels = compChannels_.wlock();
      // A client holding an event it has not acked keeps the queue.
      if (it->second->hasUnackedCompletionEvents()) {
        return withErrno(EBUSY);
      }
      const auto channelIt = lockedChannels->find(cq->channel);
      if (channelIt != lockedChannels->end()) {
        auto& events = channelIt->second.events;
        events.erase(
            std::remove(events.begin(), events.end(), cq), events.end());
        --channelIt->second.completionQueues;
      }
    }
    queue = std::move(it->second);
    lockedCqs->erase(it);
  }
  CompletionQueue& owned = *CHECK_NOTNULL(queue.get());
  // The purge abandons the events still pending, resolving them here.
  owned.releaseEventReferences(purgeAsyncEventsForCq(cq));
  // What stays outstanding is with a client, and resolves when it acks.
  owned.waitForEventsAcked();
  free(reinterpret_cast<LsmrCq*>(cq));
  return 0;
}

SharedReceiveQueue* FOLLY_NULLABLE MetaRoceVerbs::sharedReceiveQueueIn(
    const SrqsByHandle& srqs,
    const struct ibv_srq& srq) {
  const auto it = srqs.find(srq.handle);
  return it == srqs.end() ? nullptr : it->second.get();
}

SharedReceiveQueue* FOLLY_NULLABLE
MetaRoceVerbs::referenceSharedReceiveQueue(struct ibv_srq& srq) {
  const auto lockedSrqs = handleToSrq_.rlock();
  const auto it = lockedSrqs->find(srq.handle);
  if (it == lockedSrqs->end()) {
    return nullptr;
  }
  // ibv_destroy_srq erases under the write lock this read lock excludes, so it
  // either ran before the find above or finds the reference already held.
  it->second->takeReference();
  return it->second.get();
}

bool MetaRoceVerbs::cancelRnrForRefusedTag(
    SharedReceiveTaker refused,
    uint16_t postedCsn) {
  // Holding this lock across the schedule keeps the queue pair alive through
  // the call.
  const auto lockedQps = qpNumToQp_.rlock();
  const auto qpEntry = lockedQps->find(refused.qpNum);
  if (qpEntry == lockedQps->end()) {
    return false;
  }
  // A tag whose posts will be flushed cannot use the receive.
  const auto tag = qpEntry->second->getTag(refused.tagNum);
  if (tag == nullptr || tag->postWillBeFlushed()) {
    return false;
  }
  qpEntry->second->scheduleRnrCancelPacket(refused.tagNum, postedCsn);
  return true;
}

struct ibv_srq* FOLLY_NULLABLE MetaRoceVerbs::ibv_create_srq(
    struct ibv_pd* pd,
    struct ibv_srq_init_attr* srq_init_attr) {
  if (pd == nullptr || srq_init_attr == nullptr) {
    return nullSettingErrno(EINVAL);
  }
  const uint32_t maxWorkRequests = srq_init_attr->attr.max_wr;
  const uint32_t maxSge = srq_init_attr->attr.max_sge;
  // A queue that can hold no receive serves no queue pair.
  if (maxWorkRequests == 0 || maxWorkRequests > kMaxSrqWorkRequests ||
      maxSge > kMaxSgesPerWorkRequest) {
    return nullSettingErrno(EINVAL);
  }
  const auto lockedProtectionDomains = protectionDomains_.rlock();
  if (!holdsProtectionDomain(*lockedProtectionDomains, pd)) {
    return nullSettingErrno(EINVAL);
  }

  auto* wrapper = static_cast<LsmrSrq*>(malloc(sizeof(LsmrSrq)));
  if (wrapper == nullptr) {
    return nullSettingErrno(ENOMEM);
  }
  auto freeWrapper = folly::makeGuard([wrapper] { free(wrapper); });
  struct ibv_srq* ibvSrq = &wrapper->ibvSrq;
  {
    auto lockedSrqs = handleToSrq_.wlock();
    if (lockedSrqs->size() >= kMaxSharedReceiveQueues) {
      containLogFailure([&] {
        LOG(ERROR) << "libsoftmetaroce: every shared receive queue is in use; "
                      "refusing to create SRQ";
      });
      return nullSettingErrno(ENOSPC);
    }
    const std::optional<uint32_t> maybeHandle = nextFreeId(
        nextSrqHandleToAssign_,
        kFirstSharedReceiveQueueHandle,
        std::numeric_limits<uint32_t>::max(),
        [&](uint32_t handle) { return lockedSrqs->contains(handle); });
    CHECK(maybeHandle.has_value())
        << "the count checked above leaves handles free, so nextFreeId finds "
           "one; exhausting four billion of them would mean the map holds far "
           "more queues than the count allows";
    std::pair<SrqsByHandle::iterator, bool> placed{lockedSrqs->end(), false};
    try {
      placed = lockedSrqs->insert(
          {*maybeHandle,
           std::make_unique<SharedReceiveQueue>(
               maxWorkRequests,
               maxSge,
               *pd,
               [this,
                context = pd->context,
                contextId = asyncEventIdOf(pd->context),
                srq = ibvSrq] {
                 struct ibv_async_event event{};
                 event.element.srq = srq;
                 event.event_type = IBV_EVENT_SRQ_LIMIT_REACHED;
                 return raiseAsyncEvent(context, contextId, event);
               },
               [this](SharedReceiveTaker refused, uint16_t postedCsn) {
                 return cancelRnrForRefusedTag(refused, postedCsn);
               })});
    } catch (const std::bad_alloc&) {
      return nullSettingErrno(ENOMEM);
    } catch (...) {
      return nullSettingErrno(logUnexpectedFailure("ibv_create_srq"));
    }
    const auto [it, inserted] = placed;
    CHECK(inserted)
        << "nextFreeId skips a handle handleToSrq_ already holds, under the "
           "write lock this insert takes";
    wrapper->sharedReceiveQueue = it->second.get();
    *ibvSrq = {
        // ibv_post_srq_recv dispatches through this context's ops table, so a
        // queue reached from a protection domain must hold that domain's.
        .context = pd->context,
        .srq_context = srq_init_attr->srq_context,
        .pd = pd,
        .handle = *maybeHandle,
        .mutex = pthread_mutex_t(),
        .cond = pthread_cond_t(),
        .events_completed = 0};
  }
  freeWrapper.dismiss();
  return ibvSrq;
}

int MetaRoceVerbs::ibv_query_srq(
    struct ibv_srq* srq,
    struct ibv_srq_attr* srq_attr) {
  if (srq == nullptr || srq_attr == nullptr) {
    return withErrno(EINVAL);
  }
  const auto lockedSrqs = handleToSrq_.rlock();
  const SharedReceiveQueue* const queue =
      sharedReceiveQueueIn(*lockedSrqs, *srq);
  if (queue == nullptr) {
    return withErrno(ENOENT);
  }
  *srq_attr = {
      .max_wr = queue->getMaxWorkRequests(),
      .max_sge = queue->getMaxSge(),
      .srq_limit = queue->getArmedLimit()};
  return 0;
}

int MetaRoceVerbs::ibv_modify_srq(
    struct ibv_srq* srq,
    struct ibv_srq_attr* srq_attr,
    int srq_attr_mask) {
  if (srq == nullptr || srq_attr == nullptr) {
    return withErrno(EINVAL);
  }
  const auto lockedSrqs = handleToSrq_.rlock();
  SharedReceiveQueue* const queue = sharedReceiveQueueIn(*lockedSrqs, *srq);
  if (queue == nullptr) {
    return withErrno(ENOENT);
  }
  if ((srq_attr_mask & IBV_SRQ_MAX_WR) != 0) {
    LOG_FIRST_N(ERROR, 10)
        << "libsoftmetaroce: ibv_modify_srq cannot resize a shared receive "
           "queue; refusing mask "
        << srq_attr_mask;
    return withErrno(ENOSYS);
  }
  if ((srq_attr_mask & IBV_SRQ_LIMIT) == 0) {
    return withErrno(EINVAL);
  }
  // A limit past the depth is one the queue is always below, so it describes
  // no moment to report.
  if (srq_attr->srq_limit > queue->getMaxWorkRequests()) {
    return withErrno(EINVAL);
  }
  queue->armLimit(srq_attr->srq_limit);
  return 0;
}

int MetaRoceVerbs::ibv_destroy_srq(struct ibv_srq* srq) {
  if (srq == nullptr) {
    return withErrno(EINVAL);
  }
  // Held past the erase, so an ack arriving during the wait below reaches a
  // queue this still owns.
  std::unique_ptr<SharedReceiveQueue> queue;
  {
    auto lockedSrqs = handleToSrq_.wlock();
    const auto it = lockedSrqs->find(srq->handle);
    if (it == lockedSrqs->end()) {
      return withErrno(ENOENT);
    }
    if (it->second == nullptr) {
      return withErrno(ENOENT);
    }
    // A queue a queue pair can still draw from is kept.
    if (it->second->isReferenced()) {
      return withErrno(EBUSY);
    }
    queue = std::move(it->second);
    lockedSrqs->erase(it);
  }
  SharedReceiveQueue& owned = *CHECK_NOTNULL(queue.get());
  // The purge abandons the events still pending, resolving them here.
  owned.releaseEventReferences(purgeAsyncEventsForSrq(srq));
  // What stays outstanding is with a client, and resolves when it acks.
  owned.waitForEventsAcked();
  free(reinterpret_cast<LsmrSrq*>(srq));
  return 0;
}

namespace {

// Drops zero-length SGEs, which require no local memory validation.
LocalSges resolveLocalSges(
    const MemoryRegions& memoryRegions,
    const ibv_pd* pd,
    std::span<const ibv_sge> sges) {
  LocalSges result;
  for (const ibv_sge& sge : sges) {
    if (sge.length != 0) {
      result.push_back({
          .address = memoryRegions.localAddressOf(pd, sge.addr, sge.lkey),
          .length = sge.length,
          .lkey = sge.lkey,
      });
    }
  }
  return result;
}

} // namespace

int MetaRoceVerbs::ibv_post_srq_recv(
    struct ibv_srq* srq,
    struct ibv_recv_wr* recv_wr,
    struct ibv_recv_wr** bad_recv_wr) {
  // Checked first: every refusal below writes through bad_recv_wr.
  if (bad_recv_wr == nullptr) {
    return withErrno(EINVAL);
  }
  if (recv_wr == nullptr || srq == nullptr) {
    *bad_recv_wr = recv_wr;
    return withErrno(EINVAL);
  }
  const auto lockedSrqs = handleToSrq_.rlock();
  SharedReceiveQueue* const queue = sharedReceiveQueueIn(*lockedSrqs, *srq);
  if (queue == nullptr) {
    *bad_recv_wr = recv_wr;
    return withErrno(EINVAL);
  }

  // A chain that loops back on itself ends at the depth: tryPost refuses once
  // the queue is full, and the refusal points at the work request it looped
  // to.
  for (struct ibv_recv_wr* wr = recv_wr; wr != nullptr; wr = wr->next) {
    const std::optional<ValidatedSgeList> sgeList =
        validatedSgeList(wr->sg_list, wr->num_sge);
    if (!sgeList || sgeList->sges.size() > queue->getMaxSge()) {
      *bad_recv_wr = wr;
      return withErrno(EINVAL);
    }
    SharedReceive receive{
        .workRequestId = wr->wr_id,
        .sges = resolveLocalSges(
            memoryRegions_, queue->getProtectionDomain(), sgeList->sges),
    };
    if (!queue->tryPost(std::move(receive))) {
      *bad_recv_wr = wr;
      return withErrno(ENOMEM);
    }
  }

  *bad_recv_wr = nullptr;
  return 0;
}

struct ibv_mr* FOLLY_NULLABLE MetaRoceVerbs::ibv_reg_mr_internal(
    struct ibv_pd* pd,
    void* addr,
    size_t length,
    int access) {
  if (addr == nullptr) {
    return nullSettingErrno(EINVAL);
  }
  const auto lockedProtectionDomains = protectionDomains_.rlock();
  if (!holdsProtectionDomain(*lockedProtectionDomains, pd)) {
    return nullSettingErrno(EINVAL);
  }
  if (!everyPageIsMapped(addr, length)) {
    return nullSettingErrno(EFAULT);
  }
  struct ibv_mr* const mr = memoryRegions_.reg_mr(pd, addr, length, access);
  if (mr == nullptr) {
    return nullSettingErrno(EINVAL);
  }
  return mr;
}

int MetaRoceVerbs::ibv_dereg_mr(struct ibv_mr* mr) {
  if (mr == nullptr) {
    return withErrno(EINVAL);
  }
  if (!memoryRegions_.dereg_mr(*mr)) {
    return withErrno(ENOENT);
  }
  return 0;
}

// Applies supported attributes in source order and performs the transitions
// classifyQpTransition accepts. A refused transition keeps and publishes
// fields applied before state validation. Unsupported attributes are ignored.
int MetaRoceVerbs::ibv_modify_qp(
    struct ibv_qp* ibvqp,
    struct ibv_qp_attr* attr,
    int attr_mask) {
  if (ibvqp == nullptr || attr == nullptr) {
    return withErrno(EINVAL);
  }
  if (ibvqp->handle != kQueuePairMagic) {
    return withErrno(ENOENT);
  }
  auto qpNum = qpAndTagToQp(ibvqp->qp_num);
  auto qp = getQpByNum(qpNum);
  if (qp == nullptr) {
    return withErrno(ENOENT);
  }
  auto tagNum = qpAndTagToTag(ibvqp->qp_num);
  AppliedTagConfig tagConfig{.localQpNum = qpNum, .tag = tagNum};
  if (!qp->containsTag(tagNum)) {
    return withErrno(ENOENT);
  }
  auto tag = qp->getTag(tagNum);
  auto publishAppliedConfig = folly::makeGuard([&] {
    const int saved = errno;
    publishTagConfig(tagConfig);
    errno = saved;
  });
  // Attributes below are applied unguarded: a caller driving this queue pair to
  // ERR from another thread mid-setup is a race accepted here, not prevented.
  if (attr_mask & IBV_QP_AV) {
    const uint8_t serviceLevel = attr->ah_attr.sl;
    if (serviceLevel > kMaxServiceLevel) {
      return withErrno(EINVAL);
    }
    // The service level freezes at RTR entry; an update repeating the stored
    // level configures only addresses.
    if (!isBeforeRtr(loadQpState(*ibvqp)) &&
        serviceLevel != tag->getServiceLevel()) {
      return withErrno(EINVAL);
    }

    // Extract remote address (IPv4 or IPv6) from dgid
    folly::IPAddress remoteAddr;

    // First try to interpret as IPv6
    try {
      remoteAddr = folly::IPAddressV6::fromBinary(
          folly::ByteRange(
              reinterpret_cast<const unsigned char*>(
                  attr->ah_attr.grh.dgid.raw),
              sizeof(attr->ah_attr.grh.dgid)));

      // Check if it's an IPv4-mapped IPv6 address
      if (remoteAddr.isV6() && remoteAddr.asV6().isIPv4Mapped()) {
        remoteAddr = remoteAddr.asV6().createIPv4();
      }
    } catch (const std::exception&) {
      remoteAddr = folly::IPAddressV4::fromLongHBO(ntohl(
          *reinterpret_cast<const uint32_t*>(attr->ah_attr.grh.dgid.raw)));
    }

    // Extract local address from sgid_index
    uint8_t sgid_index = attr->ah_attr.grh.sgid_index;
    if (sgid_index >= gid_table_.size()) {
      return withErrno(EINVAL);
    }
    const auto& entry = gid_table_[sgid_index];
    auto localAddr = gidEntryToIPAddress(entry);

    // Make sure we have a receiver for this local address
    (void)getReceiver(localAddr, entry);

    try {
      qp->addPortPair(localAddr, entry, remoteAddr);
    } catch (const std::length_error&) {
      // Active-port state has no bit for another logical port.
      LOG(ERROR) << "libsoftmetaroce: QP " << qpNum
                 << " has no free port id; refusing to add a port pair";
      return withErrno(ENOSPC);
    } catch (const std::bad_alloc&) {
      return withErrno(ENOMEM);
    } catch (const std::runtime_error&) {
      // Size-derived port ids are unique; a conflict is an internal error.
      return withErrno(logUnexpectedFailure("ibv_modify_qp addPortPair"));
    }

    LOG(INFO) << "Added port pair with remote addr " << remoteAddr.str()
              << " and local gid idx " << (int)sgid_index << " ("
              << localAddr.str() << ") to QP " << qpNum;
    tag->setServiceLevel(serviceLevel);
  }
  if (attr_mask & IBV_QP_DEST_QPN) {
    // Both IBV_QP_DEST_QPN and IBV_QP_AV are set - do full setup
    qp->setRemoteQpNum(attr->dest_qp_num);
    tagConfig.remoteQpNum = attr->dest_qp_num;
  }
  const ibv_qp_state state = loadQpState(*ibvqp);
  if (attr_mask & IBV_QP_SQ_PSN) {
    // sq_psn is expected only on RTR -> RTS.
    if (!isBeforeRts(state)) {
      return withErrno(EINVAL);
    }
    if (!FLAGS_libsoftmetaroce_ignore_modify_psn &&
        !tag->setSendSequenceNumbers(SequenceNumbers::fromPsn(attr->sq_psn))) {
      return withErrno(EINVAL);
    }
    tagConfig.sendCsn = tag->getNextCsn();
    tagConfig.sendMsn = tag->getNextMsn();
  }
  if (attr_mask & IBV_QP_RQ_PSN) {
    // rq_psn is expected only on INIT -> RTR.
    if (FLAGS_libsoftmetaroce_ignore_modify_psn) {
      if (!isBeforeRtr(state)) {
        return withErrno(EINVAL);
      }
    } else if (!tag->setReceiveSequenceNumbers(
                   SequenceNumbers::fromPsn(attr->rq_psn))) {
      return withErrno(EINVAL);
    }
    tagConfig.receiveCsn = tag->getNextRxCsn();
    tagConfig.receiveMsn = tag->getCDMSN();
  }
  if (attr_mask & IBV_QP_RNR_RETRY) {
    // Accepted only on the way up, so the BRNR timer never sees it change: a
    // tag entering error leaves RTS for ERR and the timer reads it there.
    if (!isBeforeRts(state)) {
      return withErrno(EINVAL);
    }
    // The attribute was set on this ibv_qp, which is one Tag of the QP, and it
    // is that Tag's retries it bounds.
    tag->setRnrRetryLimit(attr->rnr_retry);
  }
  if (attr_mask & (IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT)) {
    // Accepted only on the way up, as rnr_retry is, so every Tag's
    // configuration is frozen by the time the QP carries traffic.
    if (!isBeforeRts(state)) {
      return withErrno(EINVAL);
    }
    if (attr_mask & IBV_QP_TIMEOUT) {
      // rxe refuses the same value rather than interpreting it.
      if (attr->timeout > kMaxLocalAckTimeoutExponent) {
        return withErrno(EINVAL);
      }
      tag->setLocalAckTimeoutExponent(attr->timeout);
    }
    if (attr_mask & IBV_QP_RETRY_CNT) {
      // A count past three bits is not portable: rxe collapses it to 7 and
      // retries forever, rdmavt refuses the modify.
      tag->setTransportRetryCount(attr->retry_cnt);
    }
  }
  if (attr_mask & (IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_MAX_DEST_RD_ATOMIC)) {
    // Accepted only on the way up, as rnr_retry is: the peer's responder table
    // is negotiated once and does not resize.
    if (!isBeforeRts(state)) {
      return withErrno(EINVAL);
    }
  }
  if (attr_mask & IBV_QP_MAX_QP_RD_ATOMIC) {
    tag->setMaxRdAtomic(attr->max_rd_atomic);
  }
  if (attr_mask & IBV_QP_MAX_DEST_RD_ATOMIC) {
    tag->setMaxDestRdAtomic(attr->max_dest_rd_atomic);
  }
  if (attr_mask & IBV_QP_PATH_MTU) {
    // path_mtu is expected on INIT -> RTR.
    if (!isBeforeRtr(state)) {
      return withErrno(EINVAL);
    }
    // 8192 has no name in `ibv_mtu`, and this library frames work requests at
    // that size, so a client asks for it as one past the enum.
    constexpr auto kLargestPathMtu =
        static_cast<enum ibv_mtu>(IBV_MTU_4096 + 1);
    if (attr->path_mtu < IBV_MTU_256 || attr->path_mtu > kLargestPathMtu) {
      return withErrno(EINVAL);
    }
    uint16_t mtu = 1 << (attr->path_mtu + kMtuEnumShift);
    if (attr_mask & IBV_QP_AV) {
      uint8_t si = attr->ah_attr.grh.sgid_index;
      if (si < gid_table_.size() && !validateMtuForEntry(gid_table_[si], mtu)) {
        return withErrno(EINVAL);
      }
    }
    qp->setMtu(mtu);
  }
  if (attr_mask & IBV_QP_STATE) {
    const auto transition = classifyQpTransition(state, attr->qp_state);
    if (transition == QpTransition::INVALID) {
      return withErrno(EINVAL);
    }
    if (transition == QpTransition::LEGAL_BUT_UNSUPPORTED) {
      LOG(ERROR) << "libsoftmetaroce: QP " << qpNum << " transition from state "
                 << (int)state << " to " << (int)attr->qp_state
                 << " is legal but unimplemented";
      return withErrno(ENOSYS);
    }
    if (state == attr->qp_state) {
      // no-op
    } else if (attr->qp_state == IBV_QPS_ERR) {
      qp->setIbvQpState(IBV_QPS_ERR);
    } else {
      // Forward bring-up is the only supported state change.
      if (attr->qp_state == IBV_QPS_RTR) {
        if (qp->getNumPortPairs() == 0) {
          // RTR needs a port pair from IBV_QP_AV to build flows.
          return withErrno(EINVAL);
        }
        auto cc = ccFactory_.makeCongestionController(
            FLAGS_libsoftmetaroce_cc_algorithm);
        if (cc == nullptr) {
          LOG(ERROR) << "invalid congestion control algorithm: "
                     << FLAGS_libsoftmetaroce_cc_algorithm;
          return withErrno(EINVAL);
        }
        cc->setMTU(qp->getMtu());
        qp->setCongestionController(std::move(cc));

        qp->setupFlows(
            txFlows(),
            rxFlows(),
            sender_.get(),
            SackRetransmissionConfig{
                .mode = parseSackRetransmissionMode(
                            FLAGS_libsoftmetaroce_sack_retransmission_mode)
                            .value(),
                .fraction = FLAGS_libsoftmetaroce_sack_retransmission_fraction,
            });

        LOG(INFO) << "Setup flows for QP " << qpAndTagToQp(ibvqp->qp_num)
                  << " with " << qp->getLocalAddrs().size()
                  << " local addresses";
        qp->startProcessingWqes();
      }
      // Ordered before the state commits, since a Tag can post a send as soon
      // as it does.
      if (attr->qp_state == IBV_QPS_RTS) {
        qp->onEnteringRts();
      }
      qp->setIbvQpState(attr->qp_state);
      if (loadQpState(*ibvqp) != attr->qp_state) {
        LOG(ERROR) << "Failed to change QP " << qpNum << " state to "
                   << (int)attr->qp_state;
        return withErrno(EINVAL);
      }
    }
  }
  return 0;
}

int MetaRoceVerbs::ibv_query_qp(
    struct ibv_qp* ibvqp,
    struct ibv_qp_attr* attr,
    int attr_mask,
    struct ibv_qp_init_attr* init_attr) {
  if (ibvqp == nullptr || attr == nullptr || init_attr == nullptr) {
    return withErrno(EINVAL);
  }
  if (ibvqp->handle != kQueuePairMagic) {
    return withErrno(ENOENT);
  }
  const Qp* const qp = getQpByNum(qpAndTagToQp(ibvqp->qp_num));
  if (qp == nullptr) {
    return withErrno(ENOENT);
  }
  if (attr_mask & IBV_QP_STATE) {
    const ibv_qp_state state = loadQpState(*ibvqp);
    if (state > IBV_QPS_UNKNOWN) {
      return withErrno(EINVAL);
    }
    attr->qp_state = state;
  }
  // A state-only query answers from the caller's own struct, so resolve a Tag
  // only for a mask asking for an attribute the Tag holds.
  constexpr int kAttrsBackedByTag = IBV_QP_CAP | IBV_QP_RNR_RETRY |
      IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_MAX_QP_RD_ATOMIC |
      IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_AV;
  if ((attr_mask & kAttrsBackedByTag) == 0) {
    return 0;
  }

  const std::shared_ptr<const Tag> tag =
      qp->getTag(qpAndTagToTag(ibvqp->qp_num));
  if (tag == nullptr) {
    return withErrno(ENOENT);
  }
  if (attr_mask & IBV_QP_CAP) {
    // Populate every cap field so a caller passing an uninitialized attr does
    // not read indeterminate values. The work request depths are the ones this
    // Tag's create asked for.
    attr->cap.max_send_wr = tag->getSendQueueDepth();
    attr->cap.max_recv_wr = tag->getReceiveQueueDepth();
    attr->cap.max_send_sge = kMaxSgesPerWorkRequest;
    attr->cap.max_recv_sge = kMaxSgesPerWorkRequest;
    // Each Tag negotiated its own cap, so report the one this ibv_qp addresses
    // rather than a value shared across the tags of the underlying QP.
    attr->cap.max_inline_data = tag->getMaxInlineData();
  }
  if (attr_mask & IBV_QP_RNR_RETRY) {
    attr->rnr_retry = tag->getRnrRetryLimit();
  }
  if (attr_mask & IBV_QP_TIMEOUT) {
    // Zero means "no timeout", so an unconfigured Tag reports instead the pair
    // deriving the deadline it is held to.
    const auto configured = tag->getLocalAckTimeoutExponent();
    attr->timeout =
        configured.value_or(qpAttributesForInterval(qp->getWatchdogIntervalMs())
                                .localAckTimeoutExponent);
  }
  if (attr_mask & IBV_QP_RETRY_CNT) {
    // A count set without a timeout never reached the deadline, so the
    // derived one is what the QP will actually spend its retries against.
    attr->retry_cnt = tag->getLocalAckTimeoutExponent().has_value()
        ? tag->getTransportRetryCount()
        : qpAttributesForInterval(qp->getWatchdogIntervalMs()).retryCount;
  }
  if (attr_mask & IBV_QP_MAX_QP_RD_ATOMIC) {
    attr->max_rd_atomic = tag->getMaxRdAtomic();
  }
  if (attr_mask & IBV_QP_MAX_DEST_RD_ATOMIC) {
    attr->max_dest_rd_atomic = tag->getMaxDestRdAtomic();
  }
  if (attr_mask & IBV_QP_AV) {
    // Reports only the tag's service level; the addresses are port pairs
    // on the QP, so no single address vector exists to return.
    attr->ah_attr = {};
    attr->ah_attr.sl = tag->getServiceLevel();
  }
  return 0;
}

// create roce socket and mock qp
struct ibv_qp* FOLLY_NULLABLE MetaRoceVerbs::ibv_create_qp(
    struct ibv_pd* pd,
    struct ibv_qp_init_attr* qp_init_attr) {
  if (qp_init_attr == nullptr) {
    return nullSettingErrno(EINVAL);
  }
  if (!isServedQueuePairType(qp_init_attr->qp_type)) {
    return nullSettingErrno(EINVAL);
  }
  if (qp_init_attr->cap.max_inline_data > kMaxInlineDataBytes) {
    // libibverbs reports create_qp failure via errno; callers inspect it.
    return nullSettingErrno(EINVAL);
  }
  if (qp_init_attr->cap.max_send_sge > kMaxSgesPerWorkRequest ||
      qp_init_attr->cap.max_recv_sge > kMaxSgesPerWorkRequest) {
    return nullSettingErrno(EINVAL);
  }
  if (isQueueDepthPastTheMaximum(qp_init_attr->cap.max_send_wr) ||
      isQueueDepthPastTheMaximum(qp_init_attr->cap.max_recv_wr)) {
    return nullSettingErrno(EINVAL);
  }
  // ibv_create_qp(3) answers the depths only on a create that succeeds.
  const uint32_t sendQueueDepth =
      resolvedQueueDepth(qp_init_attr->cap.max_send_wr);
  const uint32_t receiveQueueDepth =
      resolvedQueueDepth(qp_init_attr->cap.max_recv_wr);

  if (qp_init_attr->send_cq == nullptr || qp_init_attr->recv_cq == nullptr) {
    return nullSettingErrno(EINVAL);
  }

  auto lockedProtectionDomains = protectionDomains_.rlock();
  if (!holdsProtectionDomain(*lockedProtectionDomains, pd)) {
    return nullSettingErrno(EINVAL);
  }
  CompletionQueue* const sendCq =
      referenceCompletionQueue(*qp_init_attr->send_cq);
  CompletionQueue* const recvCq =
      referenceCompletionQueue(*qp_init_attr->recv_cq);
  // Each Tag takes its own reference as it is constructed; these hold the
  // queues only until it does, so they go back however this call ends.
  auto releaseCompletionQueues = folly::makeGuard([sendCq, recvCq] {
    if (sendCq != nullptr) {
      sendCq->dropReference();
    }
    if (recvCq != nullptr) {
      recvCq->dropReference();
    }
  });
  if (sendCq == nullptr || recvCq == nullptr) {
    return nullSettingErrno(ENOENT);
  }

  SharedReceiveQueue* sharedReceiveQueue = nullptr;
  if (qp_init_attr->srq != nullptr) {
    sharedReceiveQueue = referenceSharedReceiveQueue(*qp_init_attr->srq);
    if (sharedReceiveQueue == nullptr) {
      return nullSettingErrno(EINVAL);
    }
  }
  // Gives the reference back where a refusal below leaves no queue pair to
  // hold it.
  auto releaseSharedReceiveQueue = folly::makeGuard([sharedReceiveQueue] {
    if (sharedReceiveQueue != nullptr) {
      sharedReceiveQueue->dropReference();
    }
  });

  struct ibv_qp* ibvQp = (struct ibv_qp*)malloc(sizeof(struct ibv_qp));
  if (ibvQp == nullptr) {
    return nullSettingErrno(ENOMEM);
  }
  auto freeIbvQp = folly::makeGuard([ibvQp] { free(ibvQp); });

  AppliedTagConfig initialTagConfig{};
  {
    // Hold the map lock across allocate-and-insert so two creators cannot claim
    // the same number and a number freed by a concurrent destroy is seen here.
    auto lockedQpMap = qpNumToQp_.wlock();
    if (lockedQpMap->size() >= kMaxQueuePairs) {
      // Past the advertised max_qp: a caller mistake; errno only, no log.
      return nullSettingErrno(ENOSPC);
    }
    const std::optional<uint32_t> maybeQpn = nextFreeId(
        nextQpNumToAssign_, kMetaRoceInitialQpId, kQpIdMask, [&](uint32_t n) {
          return lockedQpMap->contains(n);
        });
    static_assert(
        kMaxQueuePairs <= kQpIdMask - kMetaRoceInitialQpId + 1,
        "the QP number range must hold kMaxQueuePairs queue pairs");
    CHECK(maybeQpn.has_value())
        << "the count checked above leaves QP numbers free, so nextFreeId finds "
           "one";
    const uint32_t thisQpn = *maybeQpn;
    *ibvQp = {
        .context = nullptr,
        .qp_context = qp_init_attr->qp_context,
        .pd = pd,
        .send_cq = qp_init_attr->send_cq,
        .recv_cq = qp_init_attr->recv_cq,
        .srq = qp_init_attr->srq,
        .handle = kQueuePairMagic,
        .qp_num = combineQpAndTag(thisQpn, kMetaRoceInitialTag),
        .state = IBV_QPS_RESET,
        .qp_type = qp_init_attr->qp_type,
        .mutex = pthread_mutex_t(),
        .cond = pthread_cond_t(),
        .events_completed = 0};
    try {
      auto qp = std::make_unique<Qp>(
          thisQpn,
          timer_,
          *arbiter_,
          memoryRegions_,
          sendCq,
          recvCq,
          ibvQp,
          rcnRateHint_.get(),
          qp_init_attr->sq_sig_all != 0,
          qp_init_attr->cap.max_inline_data,
          sendQueueDepth,
          receiveQueueDepth,
          sharedReceiveQueue);
      const auto tag = qp->getTag(kMetaRoceInitialTag);
      initialTagConfig = {
          .localQpNum = thisQpn,
          .tag = kMetaRoceInitialTag,
          .sendCsn = tag->getNextCsn(),
          .receiveCsn = tag->getNextRxCsn(),
          .sendMsn = tag->getNextMsn()};
      lockedQpMap->insert({thisQpn, std::move(qp)});
    } catch (const std::bad_alloc&) {
      return nullSettingErrno(ENOMEM);
    } catch (...) {
      return nullSettingErrno(logUnexpectedFailure("ibv_create_qp"));
    }
    freeIbvQp.dismiss();
    containLogFailure([&] {
      VLOG(1) << "Created QP: " << thisQpn
              << " with initial Tag: " << (uint32_t)kMetaRoceInitialTag;
    });
  }
  releaseSharedReceiveQueue.dismiss();
  // Observers may call back into verbs, so the lock is released here: the
  // queue pair is registered, the last thing it guards.
  lockedProtectionDomains.unlock();
  publishTagConfig(initialTagConfig);
  qp_init_attr->cap.max_send_wr = sendQueueDepth;
  qp_init_attr->cap.max_recv_wr = receiveQueueDepth;
  return ibvQp;
}

struct ibv_qp* FOLLY_NULLABLE MetaRoceVerbs::ibv_create_qp_ex(
    struct ibv_context* context,
    struct ibv_qp_init_attr_ex* qp_init_attr) {
  if (qp_init_attr == nullptr) {
    return nullSettingErrno(EINVAL);
  }
  if (!(qp_init_attr->qp_type == IBV_QPT_RC &&
        qp_init_attr->comp_mask & IBV_QP_INIT_ATTR_CREATE_FLAGS &&
        qp_init_attr->create_flags & IBV_QP_CREATE_SOURCE_QPN)) {
    // If this is not an RC QP, or if we do not have create_flags, or
    // IBV_QP_CREATE_SOURCE_QPN is not set, then fall back to creating a new QP
    // using ibv_create_qp().
    return ibv_create_qp(
        qp_init_attr->pd, (struct ibv_qp_init_attr*)qp_init_attr);
  }

  auto qpNum = qpAndTagToQp(qp_init_attr->source_qpn);
  Qp* const found = getQpByNum(qpNum);
  if (found == nullptr) {
    LOG(ERROR) << "QP num '" << qpNum << "' not found. "
               << "Cannot specify 'IBV_QP_CREATE_SOURCE_QPN' for RC QPs "
               << "unless creating a Tag on an existing QP.";
    return nullSettingErrno(EINVAL);
  }
  Qp& qp = *found;

  if (qp_init_attr->cap.max_inline_data > kMaxInlineDataBytes) {
    // A Tag added to an existing QP negotiates its own cap, so it is bounded
    // here too rather than inheriting the cap the first Tag was granted.
    return nullSettingErrno(EINVAL);
  }
  if (qp_init_attr->cap.max_send_sge > kMaxSgesPerWorkRequest ||
      qp_init_attr->cap.max_recv_sge > kMaxSgesPerWorkRequest) {
    return nullSettingErrno(EINVAL);
  }
  if (isQueueDepthPastTheMaximum(qp_init_attr->cap.max_send_wr) ||
      isQueueDepthPastTheMaximum(qp_init_attr->cap.max_recv_wr)) {
    return nullSettingErrno(EINVAL);
  }
  // ibv_create_qp(3) answers the depths only on a create that succeeds.
  const uint32_t sendQueueDepth =
      resolvedQueueDepth(qp_init_attr->cap.max_send_wr);
  const uint32_t receiveQueueDepth =
      resolvedQueueDepth(qp_init_attr->cap.max_recv_wr);
  if (qp_init_attr->send_cq == nullptr || qp_init_attr->recv_cq == nullptr) {
    return nullSettingErrno(EINVAL);
  }
  CompletionQueue* const sendCq =
      referenceCompletionQueue(*qp_init_attr->send_cq);
  CompletionQueue* const recvCq =
      referenceCompletionQueue(*qp_init_attr->recv_cq);
  // Each Tag takes its own reference as it is constructed; these hold the
  // queues only until it does, so they go back however this call ends.
  auto releaseCompletionQueues = folly::makeGuard([sendCq, recvCq] {
    if (sendCq != nullptr) {
      sendCq->dropReference();
    }
    if (recvCq != nullptr) {
      recvCq->dropReference();
    }
  });
  if (sendCq == nullptr || recvCq == nullptr) {
    return nullSettingErrno(ENOENT);
  }
  SharedReceiveQueue* sharedReceiveQueue = nullptr;
  if (qp_init_attr->srq != nullptr) {
    sharedReceiveQueue = referenceSharedReceiveQueue(*qp_init_attr->srq);
    if (sharedReceiveQueue == nullptr) {
      return nullSettingErrno(EINVAL);
    }
  }
  // Gives the reference back where a refusal below leaves no queue pair to
  // hold it.
  auto releaseSharedReceiveQueue = folly::makeGuard([sharedReceiveQueue] {
    if (sharedReceiveQueue != nullptr) {
      sharedReceiveQueue->dropReference();
    }
  });
  // A Tag posts against the queue pair's regions, so it takes that pair's
  // domain; IBV_QP_INIT_ATTR_PD is what makes the caller's field a claim.
  struct ibv_pd* const parentPd = qp.getProtectionDomain();
  if ((qp_init_attr->comp_mask & IBV_QP_INIT_ATTR_PD) != 0 &&
      qp_init_attr->pd != parentPd) {
    return nullSettingErrno(EINVAL);
  }

  auto tagNum = qpAndTagToTag(qp_init_attr->source_qpn);
  struct ibv_qp* ibvQp = (struct ibv_qp*)malloc(sizeof(struct ibv_qp));
  if (ibvQp == nullptr) {
    return nullSettingErrno(ENOMEM);
  }
  auto freeIbvQp = folly::makeGuard([ibvQp] { free(ibvQp); });
  struct ibv_qp& addedQp = *ibvQp;
  addedQp = {
      .context = context,
      .qp_context = qp_init_attr->qp_context,
      .pd = parentPd,
      .send_cq = qp_init_attr->send_cq,
      .recv_cq = qp_init_attr->recv_cq,
      .srq = qp_init_attr->srq,
      .handle = kQueuePairMagic,
      .qp_num = qp_init_attr->source_qpn,
      .state = IBV_QPS_RESET,
      .qp_type = qp_init_attr->qp_type,
      .mutex = pthread_mutex_t(),
      .cond = pthread_cond_t(),
      .events_completed = 0};
  std::shared_ptr<Tag> addedTag;
  try {
    addedTag = qp.addTagIfAllInReset(
        tagNum,
        sendCq,
        recvCq,
        &addedQp,
        qp_init_attr->sq_sig_all != 0,
        qp_init_attr->cap.max_inline_data,
        sendQueueDepth,
        receiveQueueDepth,
        sharedReceiveQueue);
    if (!addedTag) {
      LOG(ERROR)
          << "Every Tag on a QP must be in ibv_qp_state IBV_QPS_RESET to add "
             "another, but QP:"
          << qpNum << " has at least one that is not";
      return nullSettingErrno(EINVAL);
    }
  } catch (const std::invalid_argument&) {
    // The pair already holds this tag number.
    return nullSettingErrno(EINVAL);
  } catch (const std::bad_alloc&) {
    return nullSettingErrno(ENOMEM);
  } catch (...) {
    return nullSettingErrno(logUnexpectedFailure("ibv_create_qp_ex"));
  }
  freeIbvQp.dismiss();
  releaseSharedReceiveQueue.dismiss();
  containLogFailure([&] {
    LOG(INFO) << "Added Tag: " << (uint32_t)qpAndTagToTag(addedQp.qp_num)
              << " to QP: " << qpAndTagToQp(addedQp.qp_num);
  });
  publishTagConfig(
      {.localQpNum = qpNum,
       .tag = tagNum,
       .sendCsn = addedTag->getNextCsn(),
       .receiveCsn = addedTag->getNextRxCsn(),
       .sendMsn = addedTag->getNextMsn()});
  qp_init_attr->cap.max_send_wr = sendQueueDepth;
  qp_init_attr->cap.max_recv_wr = receiveQueueDepth;
  return &addedQp;
}

int MetaRoceVerbs::ibv_destroy_qp(struct ibv_qp* qp) {
  if (qp == nullptr) {
    return withErrno(EINVAL);
  }
  if (qp->handle != kQueuePairMagic) {
    return withErrno(ENOENT);
  }
  auto qpNum = qpAndTagToQp(qp->qp_num);
  auto tagNum = qpAndTagToTag(qp->qp_num);
  // Held from before the erase through the teardown: a queue pair the map no
  // longer holds still validates against its domain while it is destroyed.
  const auto lockedProtectionDomains = protectionDomains_.rlock();
  std::unique_ptr<Qp> qpToTearDown;
  {
    auto locked = qpNumToQp_.wlock();
    auto qpIt = locked->find(qpNum);
    if (qpIt == locked->end()) {
      return withErrno(ENOENT);
    }
    // An ibv_qp is really a Tag. Mark it, but leave it (and its ibv_qp handle)
    // in the table until the whole QP is destroyed -- readers may still ack its
    // WQEs, and the Tag dereferences the handle on its error paths.
    auto tag = qpIt->second->getTag(tagNum);
    if (!tag) {
      return withErrno(ENOENT);
    }
    // Already marked: this is a double destroy of the ibv_qp.
    if (tag->isMarkedForDeletion()) {
      return withErrno(ENOENT);
    }
    tag->markForDeletion();
    // A destroyed Tag serves no further work, so the sender reconsiders at its
    // next visit whether the QP has any Tag left to keep its flows up for.
    qpIt->second->notifyTagLeftService();
    // Tearing down on the last Tag lets ~Qp() flush any still-outstanding WQEs
    // in error -- safe because teardown quiesces the readers below.
    if (qpIt->second->allTagsMarkedForDeletion()) {
      publishQpRetirement(qpNum);
      qpToTearDown = std::move(qpIt->second);
      locked->erase(qpIt);
    }
  }
  // Set only by the destroy that marks the last live Tag.
  if (qpToTearDown) {
    tearDownQp(std::move(qpToTearDown));
  }
  return 0;
}

// The Tags' ibv_qp handles must outlive drainInFlightReceives() and ~Qp()'s
// in-error flush (both dereference getIbvQp()), so snapshot them first and free
// them only afterward.
void MetaRoceVerbs::tearDownQp(std::unique_ptr<Qp> qp) {
  Qp& queuePair = *CHECK_NOTNULL(qp.get());
  std::vector<std::reference_wrapper<struct ibv_qp>> ibvQps;
  for (auto t : queuePair.getTagNums()) {
    ibvQps.emplace_back(queuePair.getTag(t)->getIbvQp());
  }
  // Wait out any reader still holding a Qp* read before the erase. Drain off
  // receiversMutex_ (drainInFlightReceives() blocks cross-thread); the
  // snapshot is safe as receivers are only added, never removed.
  std::vector<Receiver*> receivers;
  {
    std::lock_guard<std::mutex> lock(receiversMutex_);
    receivers.reserve(receivers_.size() + alternateReceivers_.size());
    for (auto& [_, receiver] : receivers_) {
      receivers.push_back(receiver.get());
    }
    for (auto& [_, receiver] : alternateReceivers_) {
      receivers.push_back(receiver.get());
    }
  }
  for (auto* receiver : receivers) {
    CHECK_NOTNULL(receiver)->drainInFlightReceives();
  }
  const uint32_t releasedQpNum = queuePair.getLocalQpNum();
  qp.reset();
  for (struct ibv_qp& ibvQp : ibvQps) {
    if (ibvQp.srq != nullptr) {
      const auto lockedSrqs = handleToSrq_.rlock();
      if (SharedReceiveQueue* const queue =
              sharedReceiveQueueIn(*lockedSrqs, *ibvQp.srq)) {
        // `nextFreeId` reissues a number only after every other, so a
        // record matched on this one cannot belong to a later queue pair.
        queue->forgetRefusedTags(releasedQpNum);
        queue->dropReference();
      }
    }
    free(&ibvQp);
  }
}

namespace {

LocalSges makeInlineSourceSges(std::span<const ibv_sge> sges) {
  LocalSges result;
  for (const ibv_sge& sge : sges) {
    if (sge.length != 0) {
      result.push_back({
          // Inline addr is a virtual address, independent of the lkey.
          // NOLINTNEXTLINE(performance-no-int-to-ptr)
          .address = reinterpret_cast<void*>(sge.addr),
          .length = sge.length,
          .lkey = sge.lkey,
      });
    }
  }
  return result;
}

} // namespace

int MetaRoceVerbs::postSendWr(
    struct ibv_qp& ibvqp,
    Qp& qp,
    const struct ibv_send_wr& wr) {
  // An errored QP flushes a WR rather than diagnosing it, so this precedes the
  // local-buffer validation below.
  const ibv_qp_state state = loadQpState(ibvqp);
  if (state != IBV_QPS_RTS && state != IBV_QPS_ERR) {
    return EINVAL;
  }
  const std::optional<ValidatedSgeList> sgeList =
      validatedSgeList(wr.sg_list, wr.num_sge);
  if (!sgeList) {
    return EINVAL;
  }
  const std::span<const struct ibv_sge> sges = sgeList->sges;
  const uint32_t length = sgeList->totalLength;
  // A path mtu below the port's mtu carries fewer bytes than max_msg_sz.
  if (length > maxMessageBytes(qp.getMtu())) {
    return EINVAL;
  }
  const bool isInline = wr.send_flags & IBV_SEND_INLINE;
  if (isInline) {
    // Inline data is copied into the WQE, so it applies only to operations that
    // carry a send payload and must fit the cap this Tag was granted; any
    // other opcode, or a payload over that cap, is a posting error.
    const bool inlineOpcodeOk = wr.opcode == IBV_WR_SEND ||
        wr.opcode == IBV_WR_SEND_WITH_IMM || wr.opcode == IBV_WR_RDMA_WRITE ||
        wr.opcode == IBV_WR_RDMA_WRITE_WITH_IMM;
    const std::shared_ptr<const Tag> tag =
        qp.getTag(qpAndTagToTag(ibvqp.qp_num));
    if (tag == nullptr) {
      return EINVAL;
    }
    const uint32_t maxInlineData = tag->getMaxInlineData();
    if (!inlineOpcodeOk || length > maxInlineData) {
      return EINVAL;
    }
    // Reject a null source before copying a nonempty inline payload.
    const bool hasNullInlineSource =
        std::ranges::any_of(sges, [](const struct ibv_sge& sge) {
          return sge.length != 0 && sge.addr == 0;
        });
    if (hasNullInlineSource) {
      return EINVAL;
    }
  }
  const LocalSges localSges = isInline
      ? makeInlineSourceSges(sges)
      : resolveLocalSges(memoryRegions_, ibvqp.pd, sges);

  const bool isReadWithImm = wr.opcode == IBV_WR_DRIVER1;
  const bool isRead = wr.opcode == IBV_WR_RDMA_READ;
  const bool isAtomic = wr.opcode == IBV_WR_ATOMIC_FETCH_AND_ADD ||
      wr.opcode == IBV_WR_ATOMIC_CMP_AND_SWP;
  const bool permitsZeroLength = wr.opcode == IBV_WR_RDMA_WRITE_WITH_IMM ||
      wr.opcode == IBV_WR_SEND || wr.opcode == IBV_WR_SEND_WITH_IMM || isRead ||
      isReadWithImm;
  if (isAtomic && sges.size() != 1) {
    LOG(ERROR) << "opcode " << wr.opcode << " requires exactly one sge";
    return EINVAL;
  }
  if (!permitsZeroLength && sges.empty()) {
    LOG(ERROR) << "opcode " << wr.opcode
               << " requires an sge with data; an empty sge list is only "
               << "permitted for RDMA_WRITE_WITH_IMM and zero-length SEND / "
               << "SEND_WITH_IMM / RDMA_READ";
    return EINVAL;
  }

  const bool countsTowardMaxRdAtomic = isAtomic || isRead || isReadWithImm;
  if (countsTowardMaxRdAtomic) {
    const std::shared_ptr<const Tag> tag =
        qp.getTag(qpAndTagToTag(ibvqp.qp_num));
    if (tag == nullptr) {
      return EINVAL;
    }
    // A max_rd_atomic of zero permits this queue pair no outstanding read
    // or atomic. An errored tag skips this check: its work is flushed.
    if (!tag->isInErrorState() && tag->getMaxRdAtomic() == 0) {
      return EINVAL;
    }
  }
  // rdma-unit-test requires a read or atomic that is invalid both locally and
  // remotely to complete with the remote-access error; the spec permits either
  // order, so that expectation is the tiebreak. Reads and atomics defer to the
  // response/ack writeback (see RxFlow) to report the remote error, so they do
  // not validate a local key here. Inline data is copied from the source here,
  // so its local key is likewise never validated -- neither reaches the check.
  // A bad local key is a terminal local protection error. The WQE is still
  // posted so the completion is reported when it reaches its turn in MSN order,
  // behind the WRs posted before it.
  std::optional<WorkCompletionStatus> postError;
  const bool wrNeedsValidSge =
      !isAtomic && !isInline && !isRead && !isReadWithImm;
  // A send reads the local buffer, which needs no registered permission.
  if (wrNeedsValidSge &&
      !memoryRegions_.isValidLocal(ibvqp.pd, localSges, length, 0)) {
    LOG(ERROR)
        << "post_send with an unregistered local address; completing with "
           "local protection error";
    postError = kLocalProtErr;
  }

  // A payload-carrying opcode with a zero-length buffer is rejected gracefully
  // (atomics validate their own operand size below).
  if (!postError && !isAtomic && !permitsZeroLength && length == 0) {
    LOG(ERROR) << "opcode " << wr.opcode
               << " requires a non-empty local buffer";
    return EINVAL;
  }
  uint8_t tagNum = qpAndTagToTag(ibvqp.qp_num);

  // Built here and posted once below, so a WR that failed local validation
  // still occupies its MSN and completes in order.
  std::unique_ptr<Wqe> wqe;
  if (wr.opcode == IBV_WR_SEND) {
    wqe = std::make_unique<SendWqe>(
        wr.wr_id, localSges, qp.getMtu(), wr.send_flags);
  } else if (wr.opcode == IBV_WR_SEND_WITH_IMM) {
    wqe = std::make_unique<SendWithImmWqe>(
        wr.wr_id, localSges, qp.getMtu(), wr.imm_data, wr.send_flags);
  } else if (wr.opcode == IBV_WR_RDMA_WRITE) {
    wqe = std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kWrite,
        wr.wr_id,
        localSges,
        qp.getMtu(),
        wr.wr.rdma.remote_addr,
        wr.wr.rdma.rkey,
        wr.send_flags);
  } else if (wr.opcode == IBV_WR_RDMA_WRITE_WITH_IMM) {
    wqe = std::make_unique<WriteWithImmWqe>(
        wr.wr_id,
        localSges,
        qp.getMtu(),
        wr.wr.rdma.remote_addr,
        wr.wr.rdma.rkey,
        wr.imm_data,
        wr.send_flags);
  } else if (wr.opcode == IBV_WR_RDMA_READ) {
    wqe = std::make_unique<ReadWriteWqe>(
        Wqe::WorkType::kRead,
        wr.wr_id,
        localSges,
        qp.getMtu(),
        wr.wr.rdma.remote_addr,
        wr.wr.rdma.rkey,
        wr.send_flags);
  } else if (isReadWithImm) {
    wqe = std::make_unique<ReadWithImmWqe>(
        wr.wr_id,
        localSges,
        qp.getMtu(),
        wr.wr.rdma.remote_addr,
        wr.wr.rdma.rkey,
        wr.imm_data,
        wr.send_flags);
  } else if (isAtomic) {
    CHECK_EQ(sges.size(), 1u)
        << "an atomic with 0 or >1 sge is rejected before this point, so "
           "sg_list must be a single valid entry to read its lkey";
    const bool isFetchAdd = (wr.opcode == IBV_WR_ATOMIC_FETCH_AND_ADD);
    if (length != kAtomicOperationSizeBytes) {
      // The operand size is a local property, so a wrong size is a local length
      // error on the send CQ. The rejected length reaches no further: AtomicWqe
      // fixes its own size.
      LOG(ERROR) << "Atomic operations must operate on exactly "
                 << kAtomicOperationSizeBytes << " bytes, got " << length;
      postError = kLocalLenErr;
    }
    // Remote-address alignment is validated by the responder, which NAKs a
    // misaligned request; do not reject it here.
    auto workType = isFetchAdd ? Wqe::WorkType::kAtomicFetchAdd
                               : Wqe::WorkType::kAtomicCmpSwp;
    uint64_t swapOrAddData =
        isFetchAdd ? wr.wr.atomic.compare_add : wr.wr.atomic.swap;
    uint64_t compareData = isFetchAdd ? 0 : wr.wr.atomic.compare_add;
    wqe = std::make_unique<AtomicWqe>(
        workType,
        wr.wr_id,
        localSges.empty() ? LocalSge{} : localSges.front(),
        qp.getMtu(),
        wr.wr.atomic.remote_addr,
        wr.wr.atomic.rkey,
        swapOrAddData,
        compareData,
        wr.send_flags);
  } else {
    LOG(ERROR) << "MetaRoceVerbs does not support post_send opcode "
               << (int)wr.opcode;
    return EINVAL;
  }

  // We know this WQE is invalid, but the validation is expected to happen at
  // processing time and complete in error in sequence.
  if (postError) {
    wqe->setCompletionStatusOverride(*postError);
  }
  if (!qp.postWqe(tagNum, std::move(wqe))) {
    LOG_FIRST_N(WARNING, 10)
        << "post_send past max_qp_wr on qp: " << ibvqp.qp_num;
    return ENOMEM;
  }

  return 0;
}

int MetaRoceVerbs::ibv_post_send(
    struct ibv_qp* ibvqp,
    struct ibv_send_wr* wr,
    struct ibv_send_wr** bad_wr) {
  // Checked first: every refusal below writes through bad_wr.
  if (bad_wr == nullptr) {
    return withErrno(EINVAL);
  }
  if (wr == nullptr) {
    *bad_wr = nullptr;
    return withErrno(EINVAL);
  }
  if (ibvqp == nullptr) {
    *bad_wr = wr;
    return withErrno(EINVAL);
  }
  if (ibvqp->handle != kQueuePairMagic) {
    *bad_wr = wr;
    return withErrno(EINVAL);
  }
  auto qp = getQpByNum(qpAndTagToQp(ibvqp->qp_num));
  if (qp == nullptr || !qp->containsTag(qpAndTagToTag(ibvqp->qp_num))) {
    *bad_wr = wr;
    return withErrno(EINVAL);
  }

  for (; wr != nullptr; wr = wr->next) {
    if (const int refusal = postSendWr(*ibvqp, *qp, *wr); refusal != 0) {
      *bad_wr = wr;
      return withErrno(refusal);
    }
  }
  *bad_wr = nullptr;
  return 0;
}

int MetaRoceVerbs::postRecvWr(
    struct ibv_qp& ibvqp,
    Qp& qp,
    const struct ibv_recv_wr& wr) {
  const std::optional<ValidatedSgeList> sgeList =
      validatedSgeList(wr.sg_list, wr.num_sge);
  if (!sgeList) {
    return EINVAL;
  }
  if (loadQpState(ibvqp) == IBV_QPS_RESET) {
    return EINVAL;
  }

  // Each sge keeps its lkey, for validating the buffer when data arrives.
  const LocalSges localSges =
      resolveLocalSges(memoryRegions_, ibvqp.pd, sgeList->sges);
  auto tagNum = qpAndTagToTag(ibvqp.qp_num);
  const bool accepted = qp.postWqe(
      tagNum, std::make_unique<ReceiveWqe>(wr.wr_id, localSges, qp.getMtu()));

  if (!accepted) {
    LOG_FIRST_N(WARNING, 10)
        << "post_recv past max_qp_wr on qp: " << ibvqp.qp_num;
    return ENOMEM;
  }

  return 0;
}

int MetaRoceVerbs::ibv_post_recv(
    struct ibv_qp* ibvqp,
    struct ibv_recv_wr* wr,
    struct ibv_recv_wr** bad_wr) {
  // Checked first: every refusal below writes through bad_wr.
  if (bad_wr == nullptr) {
    return withErrno(EINVAL);
  }
  if (wr == nullptr) {
    *bad_wr = nullptr;
    return withErrno(EINVAL);
  }
  if (ibvqp == nullptr) {
    *bad_wr = wr;
    return withErrno(EINVAL);
  }
  if (ibvqp->handle != kQueuePairMagic) {
    *bad_wr = wr;
    return withErrno(EINVAL);
  }
  auto qp = getQpByNum(qpAndTagToQp(ibvqp->qp_num));
  if (qp == nullptr || !qp->containsTag(qpAndTagToTag(ibvqp->qp_num))) {
    *bad_wr = wr;
    return withErrno(EINVAL);
  }
  // A queue pair that draws its receives from a shared queue has none of its
  // own to post to; the receive belongs on that queue.
  if (ibvqp->srq != nullptr) {
    *bad_wr = wr;
    return withErrno(EINVAL);
  }

  for (; wr != nullptr; wr = wr->next) {
    if (const int refusal = postRecvWr(*ibvqp, *qp, *wr); refusal != 0) {
      *bad_wr = wr;
      return withErrno(refusal);
    }
  }
  *bad_wr = nullptr;
  return 0;
}

int MetaRoceVerbs::ibv_poll_cq(
    struct ibv_cq* cq,
    int num_entries,
    struct ibv_wc* wc) {
  if (cq == nullptr || wc == nullptr) {
    return minusOneSettingErrno(EINVAL);
  }
  if (num_entries < 0) {
    return minusOneSettingErrno(EINVAL);
  }
  CompletionQueue& queue = completionQueueOf(*cq);
  int num_entries_found = std::min(num_entries, queue.getNumCompletedWqes());
  for (int i = 0; i < num_entries_found; i++) {
    struct ibv_wc& slot = wc[i];
    folly::coro::blockingWait([&]() -> folly::coro::Task<> {
      Cqe cqe = co_await queue.pollCq();
      slot = ibv_wc{
          .wr_id = cqe.workReqId,
          .status = static_cast<ibv_wc_status>(cqe.status),
          .opcode = cqe.opcode,
          .vendor_err = 0, // error detail is conveyed via status
          .byte_len = cqe.sizeBytes,
          .imm_data = cqe.maybeImmData.value_or(0),
          .qp_num = combineQpAndTag(cqe.qpn, cqe.tag),
          .src_qp = 0, // used only for UD QPs
          .wc_flags = cqe.maybeImmData.has_value()
              ? static_cast<unsigned int>(IBV_WC_WITH_IMM)
              : 0u,
          .pkey_index = 0, // used only for GSI QPs
          .slid = 0, // used only for UD QPs
          .sl = 0, // used only for UD QPs
          .dlid_path_bits = 0, // used only for UD QPs
      };
    }());
  }
  return num_entries_found;
}

int MetaRoceVerbs::sendPing(
    uint8_t sgidIndex,
    folly::IPAddress remoteAddr,
    uint32_t sourceQpNum,
    uint32_t remoteQpNum,
    uint8_t tagNum,
    uint16_t flowId,
    uint16_t supportedFlowCount,
    int timeout_ms) {
  if (sgidIndex >= gid_table_.size()) {
    LOG(ERROR) << "Invalid sgidIndex: " << (int)sgidIndex;
    return -1;
  }
  const auto& entry = gid_table_[sgidIndex];
  auto localAddr = gidEntryToIPAddress(entry);

  // Ensure a receiver exists for this local address so we can receive
  // the PING_RESPONSE from the remote side.
  (void)getReceiver(localAddr, entry);

  try {
    auto result = folly::coro::blockingWait(pingController_->co_sendPing(
        localAddr,
        remoteAddr,
        entry,
        sourceQpNum,
        remoteQpNum,
        tagNum,
        flowId,
        supportedFlowCount,
        std::chrono::milliseconds{timeout_ms}));
    if (result.outcome == PingOutcome::COMPLETED) {
      VLOG(1) << "Ping completed: Error State: " << result.errorState
              << ", Round-Trip Time: "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     result.rtt)
                     .count()
              << "us";
      return static_cast<uint8_t>(result.errorState);
    }
    LOG(ERROR) << "Ping failed: " << result.outcome;
    return -1;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Ping exception: " << e.what();
    return -1;
  }
}

} // namespace libsoftmetaroce
