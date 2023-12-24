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

#include "sync.h"

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <rocksdb/write_batch.h>
#include <unistd.h>

#include <fstream>
#include <string>

#include "event_util.h"
#include "io_util.h"
#include "server/redis_reply.h"

void SendStringToEvent(bufferevent *bev, const std::string &data) {
  auto output = bufferevent_get_output(bev);
  evbuffer_add(output, data.c_str(), data.length());
}

Sync::Sync(engine::Storage *storage, Writer *writer, Parser *parser, kvrocks2redis::Config *config)
    : storage_(storage), writer_(writer), parser_(parser), config_(config) {}

Sync::~Sync() {
  if (next_seq_fd_) close(next_seq_fd_);
  writer_->Stop();
}

/*
 * 1. Attempt to directly parse the wal.
 * 2. If the attempt fails, then it is necessary to parse the current snapshot,
 *    After completion, repeat the steps of the first phase.
*/
void Sync::Start() {
  auto s = readNextSeqFromFile(&next_seq_);
  if (!s.IsOK()) {
    LOG(ERROR) << s.Msg();
    return;
  }
  LOG(INFO) << "[kvrocks2redis] Start sync the data from kvrocks to redis";
  while (!IsStopped()) {
    s = checkWalBoundary();
    if (!s.IsOK()) {
      parseKVFromLocalStorage();
    }
    s = incrementBatchLoop();
    if (!s.IsOK()) {
      LOG(ERROR) << s.Msg();
    }
  }
}

void Sync::Stop() {
  if (stop_flag_) return;

  stop_flag_ = true;  // Stopping procedure is asynchronous,
  LOG(INFO) << "[kvrocks2redis] Stopped";
}


Status Sync::tryCatchUpWithPrimary() {
  auto s = storage_->GetDB()->TryCatchUpWithPrimary();
  return s.ok() ? Status() : Status::NotOK;
}

Status Sync::checkWalBoundary() {
  if (next_seq_ == storage_->LatestSeqNumber() + 1) {
    return Status::OK();
  }

  // Upper bound
  if (next_seq_ > storage_->LatestSeqNumber() + 1) {
    return {Status::NotOK};
  }

  // Lower bound
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto s = storage_->GetWALIter(next_seq_, &iter);
  if (s.IsOK() && iter->Valid()) {
    auto batch = iter->GetBatch();
    if (next_seq_ != batch.sequence) {
      if (next_seq_ > batch.sequence) {
        LOG(ERROR) << "checkWALBoundary with sequence: " << next_seq_
                   << ", but GetWALIter return older sequence: " << batch.sequence;
      }
      return {Status::NotOK};
    }
    return Status::OK();
  }
  return {Status::NotOK};
}

Status Sync::incrementBatchLoop() {
  LOG(INFO) << "[kvrocks2redis] Start parsing increment data";
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  while (!IsStopped()) {
    if (!tryCatchUpWithPrimary().IsOK()) {
      return {Status::NotOK};
    }
    if (next_seq_ < storage_->LatestSeqNumber()) {
      storage_->GetDB()->GetUpdatesSince(next_seq_, &iter);
      for (; iter->Valid(); iter->Next()) {
        auto batch = iter->GetBatch();
        if (batch.sequence != next_seq_) {
          if (next_seq_ > batch.sequence) {
            LOG(ERROR) << "checkWALBoundary with sequence: " << next_seq_
                        << ", but GetWALIter return older sequence: " << batch.sequence;
          }
          return {Status::NotOK};
        }
        auto s = parser_->ParseWriteBatch(batch.writeBatchPtr->Data());
        if (!s.IsOK()) {
          return s.Prefixed(fmt::format("failed to parse write batch '{}'", util::StringToHex(batch.writeBatchPtr->Data())));
        }
        s = updateNextSeq(next_seq_ + batch.writeBatchPtr->Count());
        if (!s.IsOK()) {
          return s.Prefixed("failed to update next sequence");
        }
      }
    } else {
      usleep(10000);
    }
  }
  return Status::OK();
}

void Sync::parseKVFromLocalStorage() {
  LOG(INFO) << "[kvrocks2redis] Start parsing kv from the local storage";
  for (const auto &iter : config_->tokens) {
    auto s = writer_->FlushDB(iter.first);
    if (!s.IsOK()) {
      LOG(ERROR) << "[kvrocks2redis] Failed to flush target redis db in namespace: " << iter.first
                 << ", encounter error: " << s.Msg();
      return;
    }
  }

  Status s = parser_->ParseFullDB();
  if (!s.IsOK()) {
    LOG(ERROR) << "[kvrocks2redis] Failed to parse full db, encounter error: " << s.Msg();
    return;
  }
  auto last_seq = storage_->GetDB()->GetLatestSequenceNumber();
  s = updateNextSeq(last_seq + 1);
  if (!s.IsOK()) {
    LOG(ERROR) << "[kvrocks2redis] Failed to update next sequence: " << s.Msg();
  }
}

Status Sync::updateNextSeq(rocksdb::SequenceNumber seq) {
  next_seq_ = seq;
  return writeNextSeqToFile(seq);
}

Status Sync::readNextSeqFromFile(rocksdb::SequenceNumber *seq) {
  next_seq_fd_ = open(config_->next_seq_file_path.data(), O_RDWR | O_CREAT, 0666);
  if (next_seq_fd_ < 0) {
    return {Status::NotOK, std::string("Failed to open next seq file :") + strerror(errno)};
  }

  *seq = 0;
  // 21 + 1 byte, extra one byte for the ending \0
  char buf[22];
  memset(buf, '\0', sizeof(buf));
  if (read(next_seq_fd_, buf, sizeof(buf)) > 0) {
    *seq = static_cast<rocksdb::SequenceNumber>(std::stoi(buf));
  }

  return Status::OK();
}

Status Sync::writeNextSeqToFile(rocksdb::SequenceNumber seq) const {
  std::string seq_string = std::to_string(seq);
  // append to 21 byte (overwrite entire first 21 byte, aka the largest SequenceNumber size )
  int append_byte = 21 - static_cast<int>(seq_string.size());
  while (append_byte-- > 0) {
    seq_string += " ";
  }
  seq_string += '\0';
  return util::Pwrite(next_seq_fd_, seq_string, 0);
}
