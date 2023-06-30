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

#include "migrate_batch.h"

#include "hiredis.h"
#include "scope_exit.h"

Status MigrateBatch::Put(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key, const rocksdb::Slice &value) {
  if (pending_entries_ == 0 && !prefix_logdata_.empty()) {
    // add prefix_logdata_ when the entry is first putted
    auto s = PutLogData(prefix_logdata_);
    if (!s.IsOK()) {
      return s;
    }
  }
  auto s = write_batch_.Put(cf, key, value);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("put key value to migrate batch failed, {}", s.ToString())};
  }

  pending_entries_++;
  entries_num_++;
  return Status::OK();
}

Status MigrateBatch::Delete(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key) {
  auto s = write_batch_.Delete(cf, key);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("delete key from migrate batch failed, {}", s.ToString())};
  }
  pending_entries_++;
  entries_num_++;
  return Status::OK();
}

Status MigrateBatch::PutLogData(const rocksdb::Slice &blob) {
  auto s = write_batch_.PutLogData(blob);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("put log data to migrate batch failed, {}", s.ToString())};
  }
  pending_entries_++;
  entries_num_++;
  return Status::OK();
}

void MigrateBatch::SetPrefixLogData(const std::string &prefix_logdata) { prefix_logdata_ = prefix_logdata; }

Status MigrateBatch::Send() {
  if (pending_entries_ == 0) {
    return Status::OK();
  }

  auto s = sendBatchSetCmd(slot_, dst_redis_context_, write_batch_);
  if (!s.IsOK()) {
    return {Status::NotOK, fmt::format("BATCHSET command failed, {}", s.Msg())};
  }

  sent_bytes_ += write_batch_.GetDataSize();
  sent_batches_num_++;
  pending_entries_ = 0;
  write_batch_.Clear();
  return Status::OK();
}

Status MigrateBatch::sendBatchSetCmd(int16_t slot, redisContext *redis_context,
                                     const rocksdb::WriteBatch &write_batch) {
  if (redis_context == nullptr) {
    return {Status::NotOK, "redis context is null"};
  }

  auto *reply = static_cast<redisReply *>(
      redisCommand(redis_context, "BATCHSET %d %b", slot, write_batch.Data().c_str(), write_batch.GetDataSize()));
  auto exit = MakeScopeExit([reply] {
    if (reply != nullptr) {
      freeReplyObject(reply);
    }
  });

  if (redis_context->err != 0) {
    return {Status::NotOK, std::string(redis_context->errstr)};
  }

  if (reply == nullptr) {
    return {Status::NotOK, "get null reply"};
  }

  if (reply->type == REDIS_REPLY_ERROR) {
    auto error_str = std::string(reply->str);
    return {Status::NotOK, error_str};
  }
  return Status::OK();
}
