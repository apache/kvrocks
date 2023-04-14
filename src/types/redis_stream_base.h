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

#include <rocksdb/status.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "status.h"

namespace Redis {

enum class StreamTrimStrategy {
  None = 0,
  MaxLen = 1,
  MinID = 2,
};

struct StreamEntryID {
  uint64_t ms_ = 0;
  uint64_t seq_ = 0;

  StreamEntryID() = default;
  StreamEntryID(uint64_t ms, uint64_t seq) : ms_(ms), seq_(seq) {}

  void Clear() {
    ms_ = 0;
    seq_ = 0;
  }

  bool IsMaximum() const { return ms_ == UINT64_MAX && seq_ == UINT64_MAX; }
  bool IsMinimum() const { return ms_ == 0 && seq_ == 0; }

  bool operator<(const StreamEntryID &rhs) const {
    if (ms_ < rhs.ms_) return true;
    if (ms_ == rhs.ms_) return seq_ < rhs.seq_;
    return false;
  }

  bool operator>=(const StreamEntryID &rhs) const { return !(*this < rhs); }

  bool operator>(const StreamEntryID &rhs) const { return rhs < *this; }

  bool operator<=(const StreamEntryID &rhs) const { return !(rhs < *this); }

  bool operator==(const StreamEntryID &rhs) const { return ms_ == rhs.ms_ && seq_ == rhs.seq_; }

  std::string ToString() const { return fmt::format("{}-{}", ms_, seq_); }

  static StreamEntryID Minimum() { return StreamEntryID{0, 0}; }
  static StreamEntryID Maximum() { return StreamEntryID{UINT64_MAX, UINT64_MAX}; }
};

struct NewStreamEntryID {
  uint64_t ms_ = 0;
  uint64_t seq_ = 0;
  bool any_seq_number_ = false;

  NewStreamEntryID() = default;
  explicit NewStreamEntryID(uint64_t ms) : ms_(ms), any_seq_number_(true) {}
  NewStreamEntryID(uint64_t ms, uint64_t seq) : ms_(ms), seq_(seq) {}
};

struct StreamEntry {
  std::string key_;
  std::vector<std::string> values_;

  StreamEntry(std::string k, std::vector<std::string> vv) : key_(std::move(k)), values_(std::move(vv)) {}
};

struct StreamTrimOptions {
  uint64_t max_len_;
  StreamEntryID min_id_;
  StreamTrimStrategy strategy_ = StreamTrimStrategy::None;
};

struct StreamAddOptions {
  NewStreamEntryID entry_id_;
  StreamTrimOptions trim_options_;
  bool nomkstream_ = false;
  bool with_entry_id_ = false;
};

struct StreamRangeOptions {
  StreamEntryID start_;
  StreamEntryID end_;
  uint64_t count_;
  bool with_count_ = false;
  bool reverse_ = false;
  bool exclude_start_ = false;
  bool exclude_end_ = false;
};

struct StreamLenOptions {
  StreamEntryID entry_id_;
  bool with_entry_id_ = false;
  bool to_first_ = false;
};

struct StreamInfo {
  uint64_t size_;
  uint64_t entries_added_;
  StreamEntryID last_generated_id_;
  StreamEntryID max_deleted_entry_id_;
  StreamEntryID recorded_first_entry_id_;
  std::unique_ptr<StreamEntry> first_entry_;
  std::unique_ptr<StreamEntry> last_entry_;
  std::vector<StreamEntry> entries_;
};

struct StreamReadResult {
  std::string name_;
  std::vector<StreamEntry> entries_;

  StreamReadResult(std::string name, std::vector<StreamEntry> result)
      : name_(std::move(name)), entries_(std::move(result)) {}
};

rocksdb::Status IncrementStreamEntryID(StreamEntryID *id);
rocksdb::Status GetNextStreamEntryID(const StreamEntryID &last_id, StreamEntryID *new_id);
Status ParseStreamEntryID(const std::string &input, StreamEntryID *id);
Status ParseNewStreamEntryID(const std::string &input, NewStreamEntryID *id);
Status ParseRangeStart(const std::string &input, StreamEntryID *id);
Status ParseRangeEnd(const std::string &input, StreamEntryID *id);
std::string EncodeStreamEntryValue(const std::vector<std::string> &args);
Status DecodeRawStreamEntryValue(const std::string &value, std::vector<std::string> *result);

}  // namespace Redis
