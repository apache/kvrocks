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

#include "cli/pid_util.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <string>

TEST(PidUtil, CreatePidFileTruncatesExistingContent) {
  // Simulate a stale PID file whose content is longer than the current PID.
  // Without O_TRUNC, CreatePidFile would overwrite only the first N bytes,
  // leaving trailing characters from the old content and producing an
  // incorrect PID string (e.g. old "12345678" + new "999" => "99945678").
  const std::string path = "/tmp/kvrocks_pid_util_test.pid";

  // Write a fake old PID that is guaranteed to be longer than any real PID.
  {
    auto fd = UniqueFD(open(path.data(), O_RDWR | O_CREAT | O_TRUNC, 0660));
    ASSERT_TRUE(fd);
    const std::string old_pid = "99999999";  // 8 chars
    ASSERT_TRUE(util::Write(*fd, old_pid).IsOK());
  }

  // Now call CreatePidFile which should overwrite with the real (shorter) PID.
  auto status = CreatePidFile(path);
  ASSERT_TRUE(status.IsOK());

  // Read back and verify the content is exactly the current PID.
  std::ifstream ifs(path);
  std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
  ifs.close();

  std::string expected = std::to_string(getpid());
  EXPECT_EQ(content, expected);
  EXPECT_EQ(content.size(), expected.size()) << "PID file should not contain leftover bytes from previous content";

  RemovePidFile(path);
}
