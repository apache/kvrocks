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

#include "writer.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstring>

#include "io_util.h"

Writer::~Writer() {
  for (const auto &iter : aof_fds_) {
    close(iter.second);
  }
}

Status Writer::Write(const std::string &ns, const std::vector<std::string> &aofs) {
  GET_OR_RET(GetAofFd(ns));
  for (const auto &aof : aofs) {
    GET_OR_RET(Util::Write(aof_fds_[ns], aof));
  }

  return Status::OK();
}

Status Writer::FlushDB(const std::string &ns) {
  GET_OR_RET(GetAofFd(ns, true));

  return Status::OK();
}

Status Writer::GetAofFd(const std::string &ns, bool truncate) {
  auto aof_fd = aof_fds_.find(ns);
  if (aof_fd == aof_fds_.end()) {
    return OpenAofFile(ns, truncate);
  } else if (truncate) {
    close(aof_fds_[ns]);
    return OpenAofFile(ns, truncate);
  }
  if (aof_fds_[ns] < 0) {
    return Status::FromErrno("Failed to open aof file:");
  }
  return Status::OK();
}

Status Writer::OpenAofFile(const std::string &ns, bool truncate) {
  int openmode = O_RDWR | O_CREAT | O_APPEND;
  if (truncate) {
    openmode |= O_TRUNC;
  }
  aof_fds_[ns] = open(GetAofFilePath(ns).data(), openmode, 0666);
  if (aof_fds_[ns] < 0) {
    return Status::FromErrno("Failed to open aof file:");
  }

  return Status::OK();
}

std::string Writer::GetAofFilePath(const std::string &ns) {
  return config_->output_dir_ + ns + "_" + config_->aof_file_name_;
}
