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

#include <map>
#include <string>
#include <vector>

#include "status.h"

namespace Kvrocks2redis {

struct redis_server {
  std::string host;
  uint32_t port;
  std::string auth;
  int db_number;
};

struct Config {
 public:
  int loglevel = 0;
  bool daemonize = false;

  std::string data_dir = "./data";
  std::string output_dir = "./";
  std::string db_dir = data_dir + "/db";
  std::string pidfile = output_dir + "/kvrocks2redis2.pid";
  std::string aof_file_name = "appendonly.aof";
  std::string next_offset_file_name = "last_next_offset.txt";
  std::string next_seq_file_path = output_dir + "/last_next_seq.txt";

  std::string kvrocks_auth;
  std::string kvrocks_host;
  int kvrocks_port = 0;
  std::map<std::string, redis_server> tokens;
  bool cluster_enable = false;

  Status Load(std::string path);
  Config() = default;
  ~Config() = default;

 private:
  std::string path_;
  static StatusOr<bool> yesnotoi(const std::string &input);
  Status parseConfigFromString(const std::string &input);
};

}  // namespace Kvrocks2redis
