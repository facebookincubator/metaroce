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

#include "rdma_gen/RdmaGenThriftControlClient.h"

#include <folly/coro/Sleep.h>
#include <folly/io/async/AsyncSocket.h>
#include <glog/logging.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>

#include "rdma_gen/RdmaGenFlags.h"
#include "rdma_gen/RdmaGenThriftControlCommon.h"

namespace facebook::neteng::ai::rdma_gen {

RdmaGenThriftControlClient::RdmaGenThriftControlClient(
    std::shared_ptr<RdmaGenEndpoint> endpoint,
    std::shared_ptr<std::vector<RcQpInfo>> remoteRcQpInfo,
    std::shared_ptr<std::vector<void*>> remoteAddrs,
    std::shared_ptr<std::vector<uint32_t>> rkeys)
    : endpoint_(std::move(endpoint)),
      remoteRcQpInfo_(std::move(remoteRcQpInfo)),
      remoteAddrs_(std::move(remoteAddrs)),
      rkeys_(std::move(rkeys)),
      evbThread_("thrift-ctrl") {}

folly::coro::Task<bool> RdmaGenThriftControlClient::signalServer(
    const std::string& servername,
    int port,
    uint32_t qpIdx,
    ControlMessage controlMsg) {
  VLOG(1) << "ThriftControlClient signaling server " << servername << ":"
          << port << " qpIdx: " << qpIdx << " msg: " << int(controlMsg);

  try {
    switch (controlMsg) {
      case ControlMessage::GetRemoteInfo:
        co_return co_await exchangeQpInfo(servername, port, qpIdx);
      case ControlMessage::Terminate:
        co_return co_await terminate(servername, port, qpIdx);
      case ControlMessage::Write:
        co_return co_await write(servername, port, qpIdx);
      case ControlMessage::Read:
        co_return co_await read(servername, port, qpIdx);
      case ControlMessage::ExchangeMetadata:
        LOG(ERROR) << "ExchangeMetadata should not be sent via signalServer";
        co_return false;
      default:
        LOG(ERROR) << "Unknown control message type: " << int(controlMsg);
        co_return false;
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Error signaling server: " << ex.what();
    co_return false;
  }
}

folly::coro::Task<bool> RdmaGenThriftControlClient::exchangeQpInfo(
    const std::string& servername,
    int port,
    uint32_t qpIdx) {
  VLOG(1) << "Exchanging QP info for qpIdx: " << qpIdx;

  try {
    auto client = co_await getClient(servername, port);

    thrift::ExchangeQpInfoRequest request{};
    request.qpInfo() = RdmaGenThriftConversions::convertToThriftQpInfo(
        endpoint_->getLocalRcQpInfo(qpIdx));

    auto response = co_await client->co_exchangeQpInfo(request);

    if (*response.status()) {
      RcQpInfo remoteQpInfo =
          RdmaGenThriftConversions::convertFromThriftQpInfo(*response.qpInfo());
      remoteRcQpInfo_->push_back(remoteQpInfo);

      VLOG(1) << "Successfully exchanged QP info";
      co_return true;
    } else {
      LOG(ERROR) << "Server returned failure for exchange QP info";
      co_return false;
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Error exchanging QP info: " << ex.what();
    co_return false;
  }
}

folly::coro::Task<bool> RdmaGenThriftControlClient::terminate(
    const std::string& servername,
    int port,
    uint32_t qpIdx) {
  VLOG(1) << "Sending termination for qpIdx: " << qpIdx;

  try {
    auto client = co_await getClient(servername, port);

    thrift::TerminationRequest request{};
    request.qpn() = endpoint_->getLocalRcQpInfo(qpIdx).qpn;

    auto response = co_await client->co_terminate(request);

    if (*response.status()) {
      VLOG(1) << "Successfully sent termination";
      co_return true;
    } else {
      LOG(ERROR) << "Server returned failure for termination";
      co_return false;
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Error sending termination: " << ex.what();
    co_return false;
  }
}

folly::coro::Task<bool> RdmaGenThriftControlClient::write(
    const std::string& servername,
    int port,
    uint32_t qpIdx) {
  VLOG(1) << "Requesting write for qpIdx: " << qpIdx;

  try {
    auto client = co_await getClient(servername, port);

    thrift::WriteRequest request{};
    request.qpInfo() = RdmaGenThriftConversions::convertToThriftQpInfo(
        endpoint_->getLocalRcQpInfo(qpIdx));

    if (qpIdx < endpoint_->params().message_size.size()) {
      request.writeLength() = endpoint_->params().message_size[qpIdx];
    } else {
      LOG(ERROR) << "Invalid qpIdx for write request: " << qpIdx;
      co_return false;
    }

    auto response = co_await client->co_write(request);

    if (*response.status()) {
      RcQpInfo remoteQpInfo =
          RdmaGenThriftConversions::convertFromThriftQpInfo(*response.qpInfo());
      remoteRcQpInfo_->push_back(remoteQpInfo);

      // Store remote address and key
      // NOLINTNEXTLINE(performance-no-int-to-ptr)
      remoteAddrs_->push_back(reinterpret_cast<void*>(*response.writeAddr()));
      rkeys_->push_back(*response.rkey());

      VLOG(1) << "Successfully requested write, addr: " << *response.writeAddr()
              << ", rkey: " << *response.rkey();
      co_return true;
    } else {
      LOG(ERROR) << "Server returned failure for write request";
      co_return false;
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Error requesting write: " << ex.what();
    co_return false;
  }
}

folly::coro::Task<bool> RdmaGenThriftControlClient::read(
    const std::string& servername,
    int port,
    uint32_t qpIdx) {
  VLOG(1) << "Requesting read for qpIdx: " << qpIdx;

  try {
    auto client = co_await getClient(servername, port);

    thrift::ReadRequest request{};
    request.qpInfo() = RdmaGenThriftConversions::convertToThriftQpInfo(
        endpoint_->getLocalRcQpInfo(qpIdx));

    if (qpIdx < endpoint_->params().message_size.size()) {
      request.readLength() = endpoint_->params().message_size[qpIdx];
    } else {
      LOG(ERROR) << "Invalid qpIdx for read request: " << qpIdx;
      co_return false;
    }

    auto response = co_await client->co_read(request);

    if (*response.status()) {
      RcQpInfo remoteQpInfo =
          RdmaGenThriftConversions::convertFromThriftQpInfo(*response.qpInfo());
      remoteRcQpInfo_->push_back(remoteQpInfo);

      // Store remote address and key
      // NOLINTNEXTLINE(performance-no-int-to-ptr)
      remoteAddrs_->push_back(reinterpret_cast<void*>(*response.readAddr()));
      rkeys_->push_back(*response.rkey());

      VLOG(1) << "Successfully requested read, addr: " << *response.readAddr()
              << ", rkey: " << *response.rkey();
      co_return true;
    } else {
      LOG(ERROR) << "Server returned failure for read request";
      co_return false;
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Error requesting read: " << ex.what();
    co_return false;
  }
}

folly::coro::Task<bool> RdmaGenThriftControlClient::exchangeMetadata(
    const std::string& servername,
    int port,
    const std::map<std::string, std::string>& localMetadata,
    std::map<std::string, std::string>& remoteMetadata) {
  VLOG(1) << "Exchanging metadata with " << servername << ":" << port;

  try {
    auto client = co_await getClient(servername, port);

    thrift::MetadataExchangeRequest request{};
    request.metadata() = localMetadata;

    auto response = co_await client->co_exchangeMetadata(request);

    if (*response.status()) {
      remoteMetadata = *response.metadata();
      VLOG(1) << "Successfully exchanged metadata: sent "
              << localMetadata.size() << " entries, received "
              << remoteMetadata.size() << " entries";
      co_return true;
    } else {
      LOG(ERROR) << "Server returned failure for metadata exchange";
      co_return false;
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Error exchanging metadata: " << ex.what();
    co_return false;
  }
}

folly::coro::Task<bool> RdmaGenThriftControlClient::completionBarrier(
    const std::string& servername,
    int port,
    int qpIdx,
    uint64_t clientCompleted) {
  VLOG(1) << "Sending completion barrier for qpIdx: " << qpIdx;

  try {
    auto client = co_await getClient(servername, port);

    thrift::CompletionBarrierRequest request{};
    request.qpn() = endpoint_->getLocalRcQpInfo(qpIdx).qpn;
    request.clientCompleted() = clientCompleted;

    auto response = co_await client->co_completionBarrier(request);

    if (*response.status()) {
      VLOG(1) << "Completion barrier satisfied by server (serverCompleted="
              << *response.serverCompleted() << ")";
      co_return true;
    } else {
      LOG(WARNING) << "Server returned failure for completion barrier";
      co_return false;
    }
  } catch (const std::exception& ex) {
    LOG(WARNING) << "Error sending completion barrier: " << ex.what();
    co_return false;
  }
}

namespace {
// Connection callback to properly handle async connections
class ConnectionCallback : public folly::AsyncSocket::ConnectCallback {
 public:
  enum class State { CONNECTING, SUCCEEDED, FAILED };

  ConnectionCallback() : state_(State::CONNECTING) {}

  void connectSuccess() noexcept override {
    state_ = State::SUCCEEDED;
    promise_.setValue(true);
  }

  void connectErr(const folly::AsyncSocketException& ex) noexcept override {
    state_ = State::FAILED;
    exception_ = ex;
    promise_.setValue(false);
  }

  folly::coro::Task<bool> waitForConnection() {
    co_return co_await promise_.getFuture();
  }

  State getState() const {
    return state_;
  }

  const folly::AsyncSocketException& getException() const {
    return exception_;
  }

 private:
  State state_;
  folly::AsyncSocketException exception_{
      folly::AsyncSocketException::UNKNOWN,
      "none"};
  folly::Promise<bool> promise_;
};
} // namespace

folly::coro::Task<
    std::unique_ptr<apache::thrift::Client<thrift::RdmaGenControlService>>>
RdmaGenThriftControlClient::createClient(
    const std::string& servername,
    int port) {
  LOG(INFO) << "Creating Thrift client connection to " << servername << ":"
            << port;
  const std::chrono::milliseconds absoluteTimeout{FLAGS_rc_connect_timeout_ms};
  const std::chrono::milliseconds firstRetryDelayMs{1}; // 1 millisecond
  const std::chrono::milliseconds maxRetryDelayMs{10000}; // 10 seconds
  const int connectTimeoutMs = 5000; // 5 seconds

  auto startTime = std::chrono::steady_clock::now();
  std::string lastErrorMessage;
  auto lastWarningLogTime = std::chrono::steady_clock::time_point{};
  auto lastInfoLogTime = std::chrono::steady_clock::time_point{};

  for (int attempt = 1;; ++attempt) {
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - startTime);
    if (elapsed >= absoluteTimeout) {
      std::string errorMsg = "Failed to create Thrift client connection to " +
          servername + ":" + std::to_string(port) + " after " +
          std::to_string(attempt - 1) + " attempts (" +
          std::to_string(elapsed.count()) + "ms elapsed, absolute timeout " +
          std::to_string(absoluteTimeout.count()) + "ms exceeded)";
      LOG(ERROR) << errorMsg;
      throw std::runtime_error(errorMsg);
    }

    folly::SocketAddress addr(servername, port, true /* allowNameLookup */);
    auto now = std::chrono::steady_clock::now();
    if (now - lastInfoLogTime >= std::chrono::seconds{1}) {
      LOG(INFO) << "Attempt " << attempt << ": Connecting to "
                << addr.describe() << " (" << elapsed.count() << "ms elapsed)";
      lastInfoLogTime = now;
    }

    // Create socket without connecting
    auto socket = folly::AsyncSocket::newSocket(&getEventBase());

    // Create connection callback
    auto connectCallback = std::make_unique<ConnectionCallback>();
    auto* callbackPtr = connectCallback.get();

    std::string errorMessage;

    try {
      // Start async connection
      socket->connect(callbackPtr, addr, connectTimeoutMs);

      // Wait for connection to complete
      bool connected = co_await callbackPtr->waitForConnection();

      if (connected &&
          callbackPtr->getState() == ConnectionCallback::State::SUCCEEDED) {
        auto totalElapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - startTime);
        LOG(INFO) << "Successfully connected to " << servername << ":" << port
                  << " on attempt " << attempt << " (" << totalElapsed.count()
                  << "ms elapsed)";

        // Create Rocket client channel
        auto channel =
            apache::thrift::RocketClientChannel::newChannel(std::move(socket));
        if (FLAGS_rc_rpc_timeout_ms > 0) {
          channel->setTimeout(FLAGS_rc_rpc_timeout_ms);
        }

        // Create and return client
        co_return std::make_unique<
            apache::thrift::Client<thrift::RdmaGenControlService>>(
            std::move(channel));
      } else {
        errorMessage = callbackPtr->getException().what();
      }
    } catch (const std::exception& ex) {
      errorMessage = ex.what();
    }

    // Log warnings: always if message changed, otherwise at most once per
    // second
    now = std::chrono::steady_clock::now();
    if (errorMessage != lastErrorMessage ||
        now - lastWarningLogTime >= std::chrono::seconds{1}) {
      LOG(WARNING) << "Connection attempt " << attempt << " failed to "
                   << servername << ":" << port << " - " << errorMessage;
      lastWarningLogTime = now;
      lastErrorMessage = errorMessage;
    }

    auto remaining = absoluteTimeout -
        std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - startTime);
    if (remaining <= std::chrono::milliseconds{0}) {
      continue; // will hit the timeout check at the top of the loop
    }
    // Exponential backoff: 1ms, 2ms, 4ms, ..., capped at 10s
    co_await folly::coro::sleep(
        std::min(
            {firstRetryDelayMs * (1 << std::min(attempt - 1, 20)),
             maxRetryDelayMs,
             remaining}));
  }
}

folly::coro::Task<apache::thrift::Client<thrift::RdmaGenControlService>*>
RdmaGenThriftControlClient::getClient(const std::string& servername, int port) {
  std::string key = servername + ":" + std::to_string(port);

  // Check if we already have a cached client
  auto it = clientCache_.find(key);
  if (it != clientCache_.end()) {
    VLOG(2) << "Using cached Thrift client for " << servername << ":" << port;
    co_return it->second.get();
  }

  // Create new client and cache it
  VLOG(1) << "Creating new Thrift client for " << servername << ":" << port;
  auto uniqueClient = co_await createClient(servername, port);

  auto* rawPtr = uniqueClient.get();
  clientCache_[key] = std::move(uniqueClient);

  co_return rawPtr;
}

folly::EventBase& RdmaGenThriftControlClient::getEventBase() {
  return *evbThread_.getEventBase();
}

} // namespace facebook::neteng::ai::rdma_gen
