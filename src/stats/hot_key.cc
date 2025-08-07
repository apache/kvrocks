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

#include "hot_key.h"

#include "common/time_util.h"
#include "server/redis_reply.h"
#include "thread_util.h"

Hotkey::~Hotkey() {
  if (enable_analyze) {
    enable_analyze = false;
    if (auto s = util::ThreadJoin(refresh_ts_thread_); !s) {
      warn("[hotkey] refresh timestamp thread operation failed: {}", s.Msg());
    }
  }
}

void Hotkey::UpdateCounter(const std::string& key, const std::string& redis_type) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (!enable_analyze) {
    return;
  }

  auto now = cached_timestamp_ms_;

  if (auto it = map_.find(key); it != map_.end()) {
    auto key_counter = it->second;
    uint32_t current = key_counter->counter;
    if (isInSameSecond(now, key_counter->last_access_timestamp_ms)) {
      ++key_counter->counter;
    } else {
      if (current >= threshold_) {
        deque_.emplace_back(HotkeyEntry{current, threshold_, key_counter->last_access_timestamp_ms, redis_type, key});
        if (deque_.size() > deque_size_) {
          deque_.pop_front();
        }
        key_counter->enqueued = true;
        dumpToLogFile(deque_.back());
      }
      // reset the counter
      key_counter->counter = 1;
    }
    key_counter->last_access_timestamp_ms = now;
    // move to list head
    list_.splice(list_.begin(), list_, key_counter);
    return;
  }

  // insert new element at list head
  list_.emplace_front(KeyCounter{false, 1, now, redis_type, key});
  map_[key] = list_.begin();

  if (list_.size() > capacity_) {
    auto key_counter = list_.back();
    if (key_counter.counter >= threshold_ && !key_counter.enqueued) {
      deque_.emplace_back(HotkeyEntry{key_counter.counter, threshold_, key_counter.last_access_timestamp_ms, redis_type,
                                      key_counter.key});
      if (deque_.size() > deque_size_) {
        deque_.pop_front();
      }
      dumpToLogFile(deque_.back());
    }
    map_.erase(key_counter.key);
    list_.pop_back();
  }
}

void Hotkey::SetThreshold(uint32_t threshold) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (!enable_analyze) {
    return;
  }

  threshold_ = threshold;
}

void Hotkey::SetDumpToLogfileLevel(spdlog::level::level_enum level) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (!enable_analyze) {
    return;
  }

  dump_to_logfile_level_ = level;
}

void Hotkey::dumpToLogFile(const HotkeyEntry& entry) const {
  if (dump_to_logfile_level_ == spdlog::level::off) {
    return;
  }

  log(dump_to_logfile_level_, "[hotkey] key: {}, redis_type: {}, count: {}, threshold: {}, timestamp: {}", entry.key,
      entry.redis_type, entry.count, entry.threshold, entry.timestamp_ms);
}

Status Hotkey::Enable(uint32_t capacity, uint32_t deque_size, uint32_t threshold) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (enable_analyze) {
    return {Status::NotOK, "please disable hotkey analyze at first"};
  }

  capacity_ = capacity;
  deque_size_ = deque_size;
  threshold_ = threshold;
  cached_timestamp_ms_ = util::GetTimeStampMS();
  clean();
  enable_analyze = true;
  refresh_ts_thread_ = GET_OR_RET(util::CreateThread("hotkey-ts", [this] {
    while (enable_analyze) {
      cached_timestamp_ms_ = util::GetTimeStampMS();
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }));

  return Status::OK();
}

Status Hotkey::Disable() {
  std::lock_guard<std::mutex> lg(mutex_);
  if (enable_analyze) {
    enable_analyze = false;
    clean();
    if (auto s = util::ThreadJoin(refresh_ts_thread_); !s) {
      warn("[hotkey] timestamp refresh thread operation failed: {}", s.Msg());
      return s;
    }
  }

  return Status::OK();
}

std::string Hotkey::SearchByTimeRange(uint32_t max_fetch_entries, uint64_t begin_timestamp_ms,
                                      uint64_t end_timestamp_ms) {
  std::string output;

  if (max_fetch_entries == 0 || begin_timestamp_ms >= end_timestamp_ms) {
    return redis::MultiLen(0);
  }

  auto begin = std::chrono::high_resolution_clock::now();
  std::unique_lock<std::mutex> lock(mutex_);
  if (!enable_analyze) {
    return redis::MultiLen(0);
  }
  if (deque_.empty()) {
    return redis::MultiLen(0);
  }

  if (deque_.size() < max_fetch_entries) {
    max_fetch_entries = deque_.size();
  }
  std::vector<HotkeyEntry> hotkeys;
  hotkeys.reserve(max_fetch_entries);
  uint32_t numbers = 0;
  for (auto& it : deque_) {
    if (it.timestamp_ms >= begin_timestamp_ms && it.timestamp_ms < end_timestamp_ms) {
      hotkeys.emplace_back(it);
      ++numbers;
      if (numbers == max_fetch_entries) {
        break;
      }
    }
  }
  lock.unlock();
  auto elapsed1 =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
  info("[hotkey] deque size {} filter between {} and {} cost {} us", deque_.size(), begin_timestamp_ms,
       end_timestamp_ms, elapsed1);

  if (hotkeys.empty()) {
    return redis::MultiLen(0);
  }

  std::sort(hotkeys.begin(), hotkeys.end(),
            [](const HotkeyEntry& a, const HotkeyEntry& b) { return a.timestamp_ms < b.timestamp_ms; });

  std::string entries;
  for (auto& it : hotkeys) {
    entries.append(redis::MultiLen(8));
    entries.append(redis::SimpleString("key"));
    entries.append(redis::SimpleString(it.key));
    entries.append(redis::SimpleString("redis_type"));
    entries.append(redis::SimpleString(it.redis_type));
    entries.append(redis::SimpleString("count"));
    entries.append(redis::Integer(it.count));
    entries.append(redis::SimpleString("timestamp"));
    entries.append(redis::Integer(it.timestamp_ms));
  }
  output.append(redis::MultiLen(hotkeys.size()));
  output.append(entries);
  auto elapsed2 =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
  info("[hotkey] deque size {} filter and serialize between {} and {} cost {} us", deque_.size(), begin_timestamp_ms,
       end_timestamp_ms, elapsed2);

  return output;
}

std::string Hotkey::GetByKeyOrThreshold(uint32_t max_fetch_entries, const std::string& key, uint32_t threshold,
                                        uint64_t begin_timestamp_ms, uint64_t end_timestamp_ms) {
  std::string output;

  if (max_fetch_entries == 0) {
    return redis::MultiLen(0);
  }
  if (begin_timestamp_ms != 0 && end_timestamp_ms != 0 && begin_timestamp_ms >= end_timestamp_ms) {
    return redis::MultiLen(0);
  }

  bool by_threshold = false, by_key = false;
  if (key != "" && threshold == 0) {
    by_key = true;
  } else if (key == "" && threshold > 0) {
    by_threshold = true;
  } else {
    return redis::MultiLen(0);
  }

  auto begin = std::chrono::high_resolution_clock::now();
  std::unique_lock<std::mutex> lock(mutex_);
  if (!enable_analyze) {
    return redis::MultiLen(0);
  }
  if (deque_.empty()) {
    return redis::MultiLen(0);
  }

  if (deque_.size() < max_fetch_entries) {
    max_fetch_entries = deque_.size();
  }
  std::vector<HotkeyEntry> hotkeys;
  hotkeys.reserve(max_fetch_entries);
  uint32_t numbers = 0;
  if (by_threshold) {
    if (begin_timestamp_ms != 0 && end_timestamp_ms != 0) {
      for (auto& it : deque_) {
        if (it.count >= threshold && it.timestamp_ms >= begin_timestamp_ms && it.timestamp_ms < end_timestamp_ms) {
          hotkeys.emplace_back(it);
          ++numbers;
          if (numbers == max_fetch_entries) {
            break;
          }
        }
      }
    } else {
      for (auto& it : deque_) {
        if (it.count >= threshold) {
          hotkeys.emplace_back(it);
          ++numbers;
          if (numbers == max_fetch_entries) {
            break;
          }
        }
      }
    }
  } else {
    if (begin_timestamp_ms != 0 && end_timestamp_ms != 0) {
      for (auto& it : deque_) {
        if (it.key == key && it.timestamp_ms >= begin_timestamp_ms && it.timestamp_ms < end_timestamp_ms) {
          hotkeys.emplace_back(it);
          ++numbers;
          if (numbers == max_fetch_entries) {
            break;
          }
        }
      }
    } else {
      for (auto& it : deque_) {
        if (it.key == key) {
          hotkeys.emplace_back(it);
          ++numbers;
          if (numbers == max_fetch_entries) {
            break;
          }
        }
      }
    }
  }
  lock.unlock();
  auto elapsed1 =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
  if (by_key) {
    if (begin_timestamp_ms != 0 && end_timestamp_ms != 0) {
      info("[hotkey] deque size {} filter by key {} between {} and {} cost {} us", deque_.size(), key,
           begin_timestamp_ms, end_timestamp_ms, elapsed1);
    } else {
      info("[hotkey] deque size {} filter by key {} cost {} us", deque_.size(), key, elapsed1);
    }
  } else {
    if (begin_timestamp_ms != 0 && end_timestamp_ms != 0) {
      info("[hotkey] deque size {} filter by threshold {} between {} and {} cost {} us", deque_.size(), threshold,
           begin_timestamp_ms, end_timestamp_ms, elapsed1);
    } else {
      info("[hotkey] deque size {} filter by threshold {} cost {} us", deque_.size(), threshold, elapsed1);
    }
  }

  if (hotkeys.empty()) {
    return redis::MultiLen(0);
  }

  std::sort(hotkeys.begin(), hotkeys.end(),
            [](const HotkeyEntry& a, const HotkeyEntry& b) { return a.timestamp_ms < b.timestamp_ms; });

  std::string entries;
  if (by_threshold) {
    for (auto& it : hotkeys) {
      entries.append(redis::MultiLen(8));
      entries.append(redis::SimpleString("key"));
      entries.append(redis::SimpleString(it.key));
      entries.append(redis::SimpleString("redis_type"));
      entries.append(redis::SimpleString(it.redis_type));
      entries.append(redis::SimpleString("count"));
      entries.append(redis::Integer(it.count));
      entries.append(redis::SimpleString("timestamp"));
      entries.append(redis::Integer(it.timestamp_ms));
    }
  } else {
    for (auto& it : hotkeys) {
      entries.append(redis::MultiLen(6));
      entries.append(redis::SimpleString("redis_type"));
      entries.append(redis::SimpleString(it.redis_type));
      entries.append(redis::SimpleString("count"));
      entries.append(redis::Integer(it.count));
      entries.append(redis::SimpleString("timestamp"));
      entries.append(redis::Integer(it.timestamp_ms));
    }
  }
  output.append(redis::MultiLen(hotkeys.size()));
  output.append(entries);
  auto elapsed2 =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
  if (by_key) {
    if (begin_timestamp_ms != 0 && end_timestamp_ms != 0) {
      info("[hotkey] deque size {} filter by key {} between {} and {} and serialize cost {} us", deque_.size(), key,
           begin_timestamp_ms, end_timestamp_ms, elapsed2);
    } else {
      info("[hotkey] deque size {} filter by key {} and serialize cost {} us", deque_.size(), key, elapsed2);
    }
  } else {
    if (begin_timestamp_ms != 0 && end_timestamp_ms != 0) {
      info("[hotkey] deque size {} filter by threshold {} between {} and {} and serialize cost {} us", deque_.size(),
           threshold, begin_timestamp_ms, end_timestamp_ms, elapsed2);
    } else {
      info("[hotkey] deque size {} filter by threshold {} and serialize cost {} us", deque_.size(), threshold,
           elapsed2);
    }
  }

  return output;
}

std::string Hotkey::GetStats() {
  std::lock_guard<std::mutex> lg(mutex_);
  if (!enable_analyze) {
    return redis::MultiLen(0);
  }

  std::string output;
  output.append(redis::MultiLen(12));
  output.append(redis::SimpleString("lru_cache_capacity"));
  output.append(redis::Integer(capacity_));
  output.append(redis::SimpleString("lru_cache_keys"));
  output.append(redis::Integer(list_.size()));
  output.append(redis::SimpleString("threshold"));
  output.append(redis::Integer(threshold_));
  output.append(redis::SimpleString("hotkey_deque_size"));
  output.append(redis::Integer(deque_size_));
  output.append(redis::SimpleString("hotkey_entries"));
  output.append(redis::Integer(deque_.size()));
  output.append(redis::SimpleString("hotkey_first_entry_timestamp"));
  if (deque_.empty()) {
    output.append(redis::Integer(0));
  } else {
    output.append(redis::Integer(deque_.front().timestamp_ms));
  }

  return output;
}

void Hotkey::clean() {
  list_.clear();
  map_.clear();
  deque_.clear();
}