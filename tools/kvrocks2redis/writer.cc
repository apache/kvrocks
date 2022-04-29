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

Writer::~Writer() {
  for (const auto &iter : aof_fds_) {
    close(iter.second);
  }
}

Status Writer::Write(const std::string &ns, const std::vector<std::string> &aofs) {
  auto s = GetAofFd(ns);
  if (!s.IsOK()) {
    return Status(Status::NotOK, s.Msg());
  }
  for (size_t i = 0; i < aofs.size(); i++) {
    write(aof_fds_[ns], aofs[i].data(), aofs[i].size());
  }

  return Status::OK();
}

Status Writer::FlushDB(const std::string &ns) {
  auto s = GetAofFd(ns, true);
  if (!s.IsOK()) {
    return Status(Status::NotOK, s.Msg());
  }

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
    return Status(Status::NotOK, std::string("Failed to open aof file :") + strerror(errno));
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
    return Status(Status::NotOK, std::string("Failed to open aof file :") + strerror(errno));
  }

  return Status::OK();
}

std::string Writer::GetAofFilePath(const std::string &ns) {
  return config_->output_dir + ns + "_" + config_->aof_file_name;
}
