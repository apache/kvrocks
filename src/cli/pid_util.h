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

#pragma once

#include <fcntl.h>

#include "io_util.h"
#include "status.h"
#include "unique_fd.h"

inline Status CreatePidFile(const std::string &path) {
  auto fd = UniqueFD(open(path.data(), O_RDWR | O_CREAT, 0660));
  if (!fd) {
    return Status::FromErrno();
  }

  std::string pid_str = std::to_string(getpid());
  auto s = util::Write(*fd, pid_str);
  if (!s.IsOK()) {
    return s.Prefixed("failed to write to PID-file");
  }

  return Status::OK();
}

inline void RemovePidFile(const std::string &path) { std::remove(path.data()); }
