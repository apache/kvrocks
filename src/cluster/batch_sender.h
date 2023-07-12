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

#include <rocksdb/write_batch.h>

#include "status.h"

struct redisContext;

class BatchSender {
 public:
  BatchSender() = default;
  BatchSender(int16_t slot, redisContext *dst_redis_context, uint32_t max_bytes)
      : slot_(slot), dst_redis_context_(dst_redis_context), max_bytes_(max_bytes) {}

  ~BatchSender() = default;

  Status Put(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key, const rocksdb::Slice &value);
  Status Delete(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key);
  Status PutLogData(const rocksdb::Slice &blob);
  void SetPrefixLogData(const std::string &prefix_logdata);
  Status Send();

  void SetMaxBytes(uint32_t max_bytes) {
    if (max_bytes_ != max_bytes) max_bytes_ = max_bytes;
  }
  bool IsFull() const { return write_batch_.GetDataSize() >= max_bytes_; }
  size_t GetDataSize() const { return write_batch_.GetDataSize(); }
  uint64_t GetSentBytes() const { return sent_bytes_; }
  uint32_t GetSentBatchesNum() const { return sent_batches_num_; }
  uint32_t GetEntriesNum() const { return entries_num_; }

 private:
  static Status sendBatchSetCmd(int16_t slot, redisContext *redis_context, const rocksdb::WriteBatch &write_batch);

  rocksdb::WriteBatch write_batch_{};
  std::string prefix_logdata_{};
  uint64_t sent_bytes_ = 0;
  uint32_t sent_batches_num_ = 0;
  uint32_t entries_num_ = 0;
  uint32_t pending_entries_ = 0;

  int16_t slot_;
  redisContext *dst_redis_context_;
  uint32_t max_bytes_;
};
