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

#include "batch_sender.h"

#include "io_util.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "storage/redis_db.h"
#include "time_util.h"

Status BatchSender::Put(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key, const rocksdb::Slice &value) {
  GET_OR_RET(ensureLogData());
  auto s = write_batch_.Put(cf, key, value);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to put key value to migration batch, {}", s.ToString())};
  }

  pending_entries_++;
  entries_num_++;
  return Status::OK();
}

Status BatchSender::Delete(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key) {
  GET_OR_RET(ensureLogData());
  auto s = write_batch_.Delete(cf, key);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to delete key from migration batch, {}", s.ToString())};
  }
  pending_entries_++;
  entries_num_++;
  return Status::OK();
}
Status BatchSender::DeleteRange(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &begin,
                                const rocksdb::Slice &end) {
  GET_OR_RET(ensureLogData());
  auto s = write_batch_.DeleteRange(cf, begin, end);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to delete range from migration batch, {}", s.ToString())};
  }
  pending_entries_++;
  entries_num_++;
  return Status::OK();
}

Status BatchSender::PutLogData(const rocksdb::Slice &blob) {
  GET_OR_RET(appendLogData(blob));
  if (!blob.empty() && !ServerLogData::IsServerLogData(blob.data())) {
    // Hash records are decoded per subkey. Other log data may represent a whole command
    // (e.g. LTRIM), which must not be replayed again in each split batch.
    redis::WriteBatchLogData log_data;
    prefix_logdata_.clear();
    if (log_data.Decode(blob).IsOK() && log_data.GetRedisType() == kRedisHash) {
      prefix_logdata_ = blob.ToString();
    }
    prefix_logdata_pending_ = false;
  }
  return Status::OK();
}

Status BatchSender::appendLogData(const rocksdb::Slice &blob) {
  auto s = write_batch_.PutLogData(blob);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to put log data to migration batch, {}", s.ToString())};
  }
  pending_entries_++;
  entries_num_++;
  return Status::OK();
}

void BatchSender::SetPrefixLogData(const std::string &prefix_logdata) {
  prefix_logdata_ = prefix_logdata;
  prefix_logdata_pending_ = true;
}

Status BatchSender::ensureLogData() {
  // Each batch must describe its own subkeys, including after a split or a change of key/encoding.
  if (prefix_logdata_pending_ && !prefix_logdata_.empty()) {
    GET_OR_RET(appendLogData(prefix_logdata_));
    prefix_logdata_pending_ = false;
  }
  return Status::OK();
}

Status BatchSender::Send() {
  if (pending_entries_ == 0) {
    return Status::OK();
  }

  // rate limit
  if (bytes_per_sec_ > 0) {
    auto single_burst = rate_limiter_->GetSingleBurstBytes();
    auto left = static_cast<int64_t>(write_batch_.GetDataSize());
    while (left > 0) {
      auto request_size = std::min(left, single_burst);
      rate_limiter_->Request(request_size, rocksdb::Env::IOPriority::IO_HIGH, nullptr);
      left -= request_size;
    }
  }

  auto s = sendApplyBatchCmd(dst_fd_, write_batch_);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send APPLYBATCH command");
  }

  sent_bytes_ += write_batch_.GetDataSize();
  sent_batches_num_++;
  pending_entries_ = 0;
  write_batch_.Clear();
  prefix_logdata_pending_ = true;
  return Status::OK();
}

Status BatchSender::sendApplyBatchCmd(int fd, const rocksdb::WriteBatch &write_batch) {
  if (fd <= 0) {
    return {Status::NotOK, "invalid fd"};
  }

  GET_OR_RET(util::SockSend(fd, redis::ArrayOfBulkStrings({"APPLYBATCH", write_batch.Data()})));

  std::string line = GET_OR_RET(util::SockReadLine(fd));

  if (line.compare(0, 1, "-") == 0) {
    return {Status::NotOK, line};
  }

  return Status::OK();
}

void BatchSender::SetBytesPerSecond(size_t bytes_per_sec) {
  if (bytes_per_sec_ == bytes_per_sec) {
    return;
  }
  bytes_per_sec_ = bytes_per_sec;
  if (bytes_per_sec > 0) {
    rate_limiter_->SetBytesPerSecond(static_cast<int64_t>(bytes_per_sec));
  }
}

double BatchSender::GetRate(uint64_t since) const {
  auto t = util::GetTimeStampMS();
  if (t <= since) {
    return 0;
  }

  return ((static_cast<double>(sent_bytes_) / 1024.0) / (static_cast<double>(t - since) / 1000.0));
}
