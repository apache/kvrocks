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

#include "log_collector.h"

#include <algorithm>

#include "server/redis_reply.h"
#include "time_util.h"

std::string SlowEntry::ToRedisString() const {
  std::string output;
  output.append(Redis::MultiLen(4));
  output.append(Redis::Integer(id));
  output.append(Redis::Integer(time));
  output.append(Redis::Integer(duration));
  output.append(Redis::MultiBulkString(args));
  return output;
}

std::string PerfEntry::ToRedisString() const {
  std::string output;
  output.append(Redis::MultiLen(6));
  output.append(Redis::Integer(id));
  output.append(Redis::Integer(time));
  output.append(Redis::BulkString(cmd_name));
  output.append(Redis::Integer(duration));
  output.append(Redis::BulkString(perf_context));
  output.append(Redis::BulkString(iostats_context));
  return output;
}

template <class T>
LogCollector<T>::~LogCollector() {
  Reset();
}

template <class T>
ssize_t LogCollector<T>::Size() {
  std::lock_guard<std::mutex> guard(mu_);
  ssize_t n = entries_.size();
  return n;
}

template <class T>
void LogCollector<T>::Reset() {
  std::lock_guard<std::mutex> guard(mu_);
  while (!entries_.empty()) {
    entries_.pop_front();
  }
}

template <class T>
void LogCollector<T>::SetMaxEntries(int64_t max_entries) {
  std::lock_guard<std::mutex> guard(mu_);
  while (max_entries > 0 && static_cast<int64_t>(entries_.size()) > max_entries) {
    entries_.pop_back();
  }
  max_entries_ = max_entries;
}

template <class T>
void LogCollector<T>::PushEntry(std::unique_ptr<T> &&entry) {
  std::lock_guard<std::mutex> guard(mu_);
  entry->id = ++id_;
  entry->time = Util::GetTimeStamp();
  if (max_entries_ > 0 && !entries_.empty() && entries_.size() >= static_cast<size_t>(max_entries_)) {
    entries_.pop_back();
  }
  entries_.push_front(std::move(entry));
}

template <class T>
std::string LogCollector<T>::GetLatestEntries(int64_t cnt) {
  size_t n = 0;
  std::string output;

  std::lock_guard<std::mutex> guard(mu_);
  if (cnt > 0) {
    n = std::min(entries_.size(), static_cast<size_t>(cnt));
  } else {
    n = entries_.size();
  }
  output.append(Redis::MultiLen(n));
  for (const auto &entry : entries_) {
    output.append(entry->ToRedisString());
    if (--n == 0) break;
  }
  return output;
}

template class LogCollector<SlowEntry>;
template class LogCollector<PerfEntry>;
