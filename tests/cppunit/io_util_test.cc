/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
#include "common/io_util.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <string>
#include <thread>

#include "common/scope_exit.h"
#include "common/unique_fd.h"

TEST(IOUtil, MatchListeningIP) {
  // bind 0.0.0.0 should at least match 127.0.0.1
  std::vector<std::string> binds{"0.0.0.0"};
  ASSERT_TRUE(util::MatchListeningIP(binds, "127.0.0.1"));
}

class SockSendWithTimeoutTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    int sockets[2] = {-1, -1};
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    sender_.Reset(sockets[0]);
    receiver_.Reset(sockets[1]);

    int buffer_size = 4096;
    ASSERT_EQ(setsockopt(sender_.Get(), SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size)), 0);
    // Bound a broken blocking send as well, so the regression cannot hang the test suite.
    timeval failsafe{3, 0};
    ASSERT_EQ(setsockopt(sender_.Get(), SOL_SOCKET, SO_SNDTIMEO, &failsafe, sizeof(failsafe)), 0);
    ASSERT_EQ(setsockopt(receiver_.Get(), SOL_SOCKET, SO_RCVTIMEO, &failsafe, sizeof(failsafe)), 0);
    ASSERT_TRUE(util::SockSetBlocking(sender_.Get(), GetParam() ? 0 : 1).IsOK());
    original_flags_ = fcntl(sender_.Get(), F_GETFL);
    ASSERT_NE(original_flags_, -1);
  }

  void expectBlockingModeUnchanged() {
    int flags = fcntl(sender_.Get(), F_GETFL);
    ASSERT_NE(flags, -1);
    // Darwin can update internal bookkeeping bits after write; compare the public mode bit.
    EXPECT_EQ(flags & O_NONBLOCK, original_flags_ & O_NONBLOCK);
  }

  UniqueFD sender_;
  UniqueFD receiver_;
  int original_flags_ = 0;
};

TEST_P(SockSendWithTimeoutTest, StalledPeerTimesOut) {
  const std::string data(1024 * 1024, 'x');
  auto start = std::chrono::steady_clock::now();
  auto status = util::SockSendWithTimeout(sender_.Get(), data, 100);
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_FALSE(status.IsOK());
  EXPECT_NE(status.Msg().find("send timeout"), std::string::npos) << status.Msg();
  // Allow scheduling slack while distinguishing the helper deadline from the 3s failsafe.
  EXPECT_LT(elapsed, std::chrono::seconds(1));
  expectBlockingModeUnchanged();
}

TEST_P(SockSendWithTimeoutTest, SendsCompletePayload) {
  std::string data(1024 * 1024, '\0');
  for (size_t index = 0; index < data.size(); ++index) {
    data[index] = static_cast<char>(index % 256);
  }
  std::string received;
  int read_error = 0;
  Status status;
  {
    std::thread reader([&] {
      char buffer[4096];
      while (received.size() < data.size()) {
        auto count = read(receiver_.Get(), buffer, sizeof(buffer));
        if (count > 0) {
          received.append(buffer, static_cast<size_t>(count));
        } else if (count == 0) {
          break;
        } else if (errno != EINTR) {
          read_error = errno;
          break;
        }
      }
    });
    auto cleanup = MakeScopeExit([&] {
      shutdown(sender_.Get(), SHUT_WR);
      reader.join();
    });
    status = util::SockSendWithTimeout(sender_.Get(), data, 2000);
  }

  EXPECT_TRUE(status.IsOK()) << status.Msg();
  EXPECT_EQ(read_error, 0);
  EXPECT_EQ(received.size(), data.size());
  EXPECT_TRUE(received == data);
  expectBlockingModeUnchanged();
}

TEST_P(SockSendWithTimeoutTest, NonPositiveTimeoutSendsNormally) {
  const std::string data("hello\0world", 11);
  for (int timeout : {0, -1}) {
    SCOPED_TRACE(timeout);
    auto status = util::SockSendWithTimeout(sender_.Get(), data, timeout);
    ASSERT_TRUE(status.IsOK()) << status.Msg();
    std::string received(data.size(), '\0');
    ASSERT_EQ(recv(receiver_.Get(), received.data(), received.size(), MSG_WAITALL), static_cast<ssize_t>(data.size()));
    EXPECT_EQ(received, data);
    expectBlockingModeUnchanged();
  }
}

INSTANTIATE_TEST_SUITE_P(SocketModes, SockSendWithTimeoutTest, testing::Bool(),
                         [](const testing::TestParamInfo<bool> &info) {
                           return info.param ? "NonBlocking" : "Blocking";
                         });

TEST(IOUtil, SockSendWithTimeoutEmptyData) {
  for (int timeout : {100, 0, -1}) {
    SCOPED_TRACE(timeout);
    EXPECT_TRUE(util::SockSendWithTimeout(-1, "", timeout).IsOK());
  }
}

TEST(IOUtil, SockSendWithTimeoutInvalidFD) {
  auto status = util::SockSendWithTimeout(-1, "data", 100);
  EXPECT_FALSE(status.IsOK());
  EXPECT_NE(status.Msg().find("fcntl(F_GETFL)"), std::string::npos) << status.Msg();
}
