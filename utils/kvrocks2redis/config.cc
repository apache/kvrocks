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
#include "config/config_util.h"
#include "string_util.h"

namespace kvrocks2redis {

static constexpr const char *kLogLevels[] = {"info", "warning", "error", "fatal"};
static constexpr size_t kNumLogLevel = std::size(kLogLevels);

StatusOr<bool> Config::yesnotoi(const std::string &input) {
  if (util::EqualICase(input, "yes")) {
    return true;
  } else if (util::EqualICase(input, "no")) {
    return false;
  }
  return {Status::NotOK, "value must be 'yes' or 'no'"};
}

Status Config::parseConfigFromString(const std::string &input) {
  auto [original_key, value] = GET_OR_RET(ParseConfigLine(input));
  if (original_key.empty()) return Status::OK();

  std::vector<std::string> args = util::Split(value, " \t\r\n");
  auto key = util::ToLower(original_key);
  size_t size = args.size();

  if (size == 1 && key == "daemonize") {
    daemonize = GET_OR_RET(yesnotoi(args[0]).Prefixed("key 'daemonize'"));
  } else if (size == 1 && key == "data-dir") {
    data_dir = args[0];
    if (data_dir.empty()) {
      return {Status::NotOK, "'data-dir' was not specified"};
    }

    if (data_dir.back() != '/') {
      data_dir += "/";
    }
    db_dir = data_dir + "db";
  } else if (size == 1 && key == "output-dir") {
    output_dir = args[0];
    if (output_dir.empty()) {
      return {Status::NotOK, "'output-dir' was not specified"};
    }

    if (output_dir.back() != '/') {
      output_dir += "/";
    }
    pidfile = output_dir + "kvrocks2redis.pid";
    next_seq_file_path = output_dir + "last_next_seq.txt";
  } else if (size == 1 && key == "log-level") {
    for (size_t i = 0; i < kNumLogLevel; i++) {
      if (util::ToLower(args[0]) == kLogLevels[i]) {
        loglevel = static_cast<int>(i);
        break;
      }
    }
  } else if (size == 1 && key == "pidfile") {
    pidfile = args[0];
  } else if (size >= 2 && key == "kvrocks") {
    kvrocks_host = args[0];
    // In new versions, we don't use extra port to implement replication
    kvrocks_port = GET_OR_RET(ParseInt<std::uint16_t>(args[1]).Prefixed("kvrocks port number"));

    if (size == 3) {
      kvrocks_auth = args[2];
    }
  } else if (size == 1 && key == "cluster-enable") {
    cluster_enable = GET_OR_RET(yesnotoi(args[0]).Prefixed("key 'cluster-enable'"));
  } else if (size >= 2 && strncasecmp(key.data(), "namespace.", 10) == 0) {
    std::string ns = original_key.substr(10);
    if (ns.size() > INT8_MAX) {
      return {Status::NotOK, fmt::format("namespace size exceed limit {}", INT8_MAX)};
    }

    tokens[ns].host = args[0];
    tokens[ns].port = GET_OR_RET(ParseInt<std::uint16_t>(args[1]).Prefixed("kvrocks port number"));

    if (size >= 3) {
      tokens[ns].auth = args[2];
    }
    tokens[ns].db_number = size == 4 ? std::atoi(args[3].c_str()) : 0;
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

}  // namespace kvrocks2redis
