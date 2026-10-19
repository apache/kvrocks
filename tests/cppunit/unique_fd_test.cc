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
 */

#include "common/unique_fd.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cerrno>
#include <utility>

TEST(UniqueFD, MoveAssignmentClosesPreviousDescriptor) {
  int first_pipe[2];
  int second_pipe[2];
  ASSERT_EQ(pipe(first_pipe), 0);
  ASSERT_EQ(pipe(second_pipe), 0);

  const int previous_fd = first_pipe[0];
  UniqueFD target(previous_fd);
  UniqueFD source(second_pipe[0]);
  target = std::move(source);

  errno = 0;
  EXPECT_EQ(fcntl(previous_fd, F_GETFD), -1);
  EXPECT_EQ(errno, EBADF);
  EXPECT_EQ(target.Get(), second_pipe[0]);
  EXPECT_FALSE(source);  // NOLINT(bugprone-use-after-move): moved-from state is under test.

  close(first_pipe[1]);
  close(second_pipe[1]);
}

TEST(UniqueFD, SelfMovePreservesDescriptor) {
  int pipe_fds[2];
  ASSERT_EQ(pipe(pipe_fds), 0);

  UniqueFD fd(pipe_fds[0]);
  fd = std::move(fd);

  EXPECT_EQ(fcntl(fd.Get(), F_GETFD), 0);
  close(pipe_fds[1]);
}

TEST(UniqueFD, ResetWithSameDescriptorDoesNotCloseIt) {
  int pipe_fds[2];
  ASSERT_EQ(pipe(pipe_fds), 0);

  UniqueFD fd(pipe_fds[0]);
  fd.Reset(fd.Get());

  EXPECT_EQ(fcntl(fd.Get(), F_GETFD), 0);
  close(pipe_fds[1]);
}

TEST(UniqueFD, ReleaseTransfersOwnership) {
  int pipe_fds[2];
  ASSERT_EQ(pipe(pipe_fds), 0);

  UniqueFD fd(pipe_fds[0]);
  const int released_fd = fd.Release();

  EXPECT_FALSE(fd);
  EXPECT_EQ(fcntl(released_fd, F_GETFD), 0);
  EXPECT_EQ(close(released_fd), 0);
  close(pipe_fds[1]);
}
