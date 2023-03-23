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

#include <time.h>

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

class SlowEntry {
 public:
  uint64_t id;
  time_t time;
  uint64_t duration;
  std::vector<std::string> args;

  std::string ToRedisString() const;
};

class PerfEntry {
 public:
  uint64_t id;
  time_t time;
  uint64_t duration;
  std::string cmd_name;
  std::string perf_context;
  std::string iostats_context;

  std::string ToRedisString() const;
};

template <class T>
class LogCollector {
 public:
  LogCollector() = default;
  LogCollector(const LogCollector &) = delete;
  LogCollector &operator=(const LogCollector &) = delete;
  ~LogCollector();
  ssize_t Size();
  void Reset();
  void SetMaxEntries(int64_t max_entries);
  void PushEntry(std::unique_ptr<T> &&entry);
  std::string GetLatestEntries(int64_t cnt);

 private:
  std::mutex mu_;
  uint64_t id_ = 0;
  int64_t max_entries_ = 128;
  std::deque<std::unique_ptr<T>> entries_;
};
