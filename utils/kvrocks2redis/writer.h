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

#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "config.h"
#include "status.h"

class Writer {
 public:
  explicit Writer(kvrocks2redis::Config *config) : config_(config) {}

  Writer(const Writer &) = delete;
  Writer &operator=(const Writer &) = delete;

  ~Writer();
  virtual Status Write(const std::string &ns, const std::vector<std::string> &aofs);
  virtual Status FlushDB(const std::string &ns);
  virtual void Stop() {}
  Status OpenAofFile(const std::string &ns, bool truncate);
  Status GetAofFd(const std::string &ns, bool truncate = false);
  std::string GetAofFilePath(const std::string &ns);

 protected:
  kvrocks2redis::Config *config_ = nullptr;
  std::map<std::string, int> aof_fds_;
};
