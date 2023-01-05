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

#include "config.h"

#include <fmt/format.h>
#include <rocksdb/env.h>
#include <strings.h>

#include <fstream>
#include <iostream>
#include <utility>
#include <vector>

#include "config/config.h"

namespace Kvrocks2redis {

static const char *kLogLevels[] = {"info", "warning", "error", "fatal"};
static const size_t kNumLogLevel = sizeof(kLogLevels) / sizeof(kLogLevels[0]);

int Config::yesnotoi(std::string input) {
  if (strcasecmp(input.data(), "yes") == 0) {
    return 1;
  } else if (strcasecmp(input.data(), "no") == 0) {
    return 0;
  }
  return -1;
}

Status Config::parseConfigFromString(const std::string &input) {
  std::vector<std::string> args = Util::Split(input, " \t\r\n");
  // omit empty line and comment
  if (args.empty() || args[0].front() == '#') return Status::OK();

  args[0] = Util::ToLower(args[0]);
  size_t size = args.size();
  if (size == 2 && args[0] == "daemonize") {
    if (int i = yesnotoi(args[1]); i == -1) {
      return {Status::NotOK, "the value of 'daemonize' must be 'yes' or 'no'"};
    } else {
      daemonize = (i == 1);
    }
  } else if (size == 2 && args[0] == "data-dir") {
    data_dir = args[1];
    if (data_dir.empty()) {
      return {Status::NotOK, "'data-dir' was not specified"};
    }

    if (data_dir.back() != '/') {
      data_dir += "/";
    }
    db_dir = data_dir + "db";
  } else if (size == 2 && args[0] == "output-dir") {
    output_dir = args[1];
    if (output_dir.empty()) {
      return {Status::NotOK, "'output-dir' was not specified"};
    }

    if (output_dir.back() != '/') {
      output_dir += "/";
    }
    pidfile = output_dir + "kvrocks2redis.pid";
    next_seq_file_path = output_dir + "last_next_seq.txt";
  } else if (size == 2 && args[0] == "loglevel") {
    for (size_t i = 0; i < kNumLogLevel; i++) {
      if (Util::ToLower(args[1]) == kLogLevels[i]) {
        loglevel = static_cast<int>(i);
        break;
      }
    }
  } else if (size == 2 && args[0] == "pidfile") {
    pidfile = args[1];
  } else if (size >= 3 && args[0] == "kvrocks") {
    kvrocks_host = args[1];
    // In new versions, we don't use extra port to implement replication
    kvrocks_port = std::stoi(args[2]);
    if (kvrocks_port <= 0 || kvrocks_port > 65535) {
      return {Status::NotOK, "Kvrocks port value should be between 0 and 65535"};
    }

    if (size == 4) {
      kvrocks_auth = args[3];
    }
  } else if (size == 2 && args[0] == "cluster-enable") {
    if (int i = yesnotoi(args[1]); i == -1) {
      return {Status::NotOK, "the value of 'cluster-enable' must be 'yes' or 'no'"};
    } else {
      cluster_enable = (i == 1);
    }
  } else if (size >= 3 && strncasecmp(args[0].data(), "namespace.", 10) == 0) {
    std::string ns = args[0].substr(10, args.size() - 10);
    if (ns.size() > INT8_MAX) {
      return {Status::NotOK, std::string("namespace size exceed limit ") + std::to_string(INT8_MAX)};
    }

    tokens[ns].host = args[1];
    tokens[ns].port = std::stoi(args[2]);
    if (tokens[ns].port <= 0 || tokens[ns].port > 65535) {
      return {Status::NotOK, "Redis port value should be between 0 and 65535"};
    }

    if (size >= 4) {
      tokens[ns].auth = args[3];
    }
    tokens[ns].db_number = size == 5 ? std::atoi(args[4].c_str()) : 0;
  } else {
    return {Status::NotOK, "unknown configuration directive or wrong number of arguments"};
  }

  return Status::OK();
}

Status Config::Load(std::string path) {
  path_ = std::move(path);
  std::ifstream file(path_);
  if (!file.is_open()) {
    return {Status::NotOK, fmt::format("failed to open file '{}': {}", path_, strerror(errno))};
  }

  std::string line;
  int line_num = 1;
  while (!file.eof()) {
    std::getline(file, line);
    Status s = parseConfigFromString(line);
    if (!s.IsOK()) {
      return s.Prefixed(fmt::format("at line #L{}", line_num));
    }

    line_num++;
  }

  auto s = rocksdb::Env::Default()->FileExists(data_dir);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return {Status::NotOK, fmt::format("the specified Kvrocks working directory '{}' doesn't exist", data_dir)};
    }
    return {Status::NotOK, s.ToString()};
  }

  s = rocksdb::Env::Default()->FileExists(output_dir);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return {Status::NotOK,
              fmt::format("the specified directory '{}' for intermediate files doesn't exist", output_dir)};
    }
    return {Status::NotOK, s.ToString()};
  }

  return Status::OK();
}

}  // namespace Kvrocks2redis
