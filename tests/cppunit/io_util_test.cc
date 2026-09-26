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

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <string_view>
#include <thread>

TEST(IOUtil, MatchListeningIP) {
  // bind 0.0.0.0 should at least match 127.0.0.1
  std::vector<std::string> binds{"0.0.0.0"};
  ASSERT_TRUE(util::MatchListeningIP(binds, "127.0.0.1"));
}

TEST(IOUtil, SockReadLineWaitsForConfiguredReceiveTimeout) {
  int sockets[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
  ASSERT_TRUE(util::SockSetReceiveTimeout(sockets[0], 2000).IsOK());

  std::thread writer([fd = sockets[1]] {
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    constexpr std::string_view response = "+OK\r\n";
    (void)write(fd, response.data(), response.size());
    close(fd);
  });
  auto response = util::SockReadLine(sockets[0]);
  writer.join();
  close(sockets[0]);

  ASSERT_TRUE(response.IsOK());
  EXPECT_EQ(*response, "+OK");
}
