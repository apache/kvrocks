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

#include <deque>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "status.h"

struct HotkeyEntry {
  uint32_t count = 0;
  uint32_t threshold = 0;
  uint64_t timestamp_ms = 0;
  std::string redis_type;
  std::string key;
};

struct KeyCounter {
  bool enqueued = false;
  uint32_t counter = 0;
  uint64_t last_access_timestamp_ms = 0;
  std::string redis_type;
  std::string key;
};

class Hotkey {
 public:
  Hotkey() = default;
  ~Hotkey();
  Hotkey(const Hotkey &) = delete;
  Hotkey &operator=(const Hotkey &) = delete;
  Hotkey(Hotkey &&) = delete;
  Hotkey &operator=(Hotkey &&) = delete;

  Status Enable(uint32_t capacity, uint32_t deque_size, uint32_t threshold);
  Status Disable();
  std::string SearchByTimeRange(uint32_t max_fetch_entries, uint64_t begin_timestamp_ms, uint64_t end_timestamp_ms);
  std::string GetByKeyOrThreshold(uint32_t max_fetch_entries, const std::string &key, uint32_t threshold,
                                  uint64_t begin_timestamp_ms, uint64_t end_timestamp_ms);
  std::string GetStats();
  void SetThreshold(uint32_t threshold);
  void UpdateCounter(const std::string &key, const std::string &redis_type);
  void SetDumpToLogfileLevel(spdlog::level::level_enum level);

  bool enable_analyze = false;  // guard by mutext_

 private:
  void clean();
  static inline bool isInSameSecond(uint64_t timestamp_ms1, uint64_t timestamp_ms2) {
    return timestamp_ms1 / 1000 == timestamp_ms2 / 1000;
  }
  void dumpToLogFile(const HotkeyEntry &entry) const;

  mutable std::mutex mutex_;
  uint32_t capacity_ = 0;
  uint32_t deque_size_ = 0;
  uint32_t threshold_ = 0;
  spdlog::level::level_enum dump_to_logfile_level_ = spdlog::level::off;
  std::list<KeyCounter> list_;
  std::unordered_map<std::string, std::list<KeyCounter>::iterator> map_;
  std::deque<HotkeyEntry> deque_;

  uint64_t cached_timestamp_ms_ = 0;
  std::thread refresh_ts_thread_;
};