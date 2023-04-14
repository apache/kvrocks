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

void send_string_to_event(bufferevent *bev, const std::string &data) {
  auto output = bufferevent_get_output(bev);
  evbuffer_add(output, data.c_str(), data.length());
}

Sync::Sync(Engine::Storage *storage, Writer *writer, Parser *parser, Kvrocks2redis::Config *config)
    : storage_(storage), writer_(writer), parser_(parser), config_(config) {}

Sync::~Sync() {
  if (sock_fd_) close(sock_fd_);
  if (next_seq_fd_) close(next_seq_fd_);
  writer_->Stop();
}

/*
 * Run connect to kvrocks, and start the following steps
 * asynchronously
 *  - TryPsync
 *  - - if ok, IncrementBatchLoop
 *  - - not, parseAllLocalStorage and restart TryPsync when done
 */
void Sync::Start() {
  auto s = readNextSeqFromFile(&next_seq_);
  if (!s.IsOK()) {
    LOG(ERROR) << s.Msg();
    return;
  }

  LOG(INFO) << "[kvrocks2redis] Start sync the data from kvrocks to redis";
  while (!IsStopped()) {
    auto sock_fd = Util::SockConnect(config_->kvrocks_host, config_->kvrocks_port);
    if (!sock_fd) {
      LOG(ERROR) << fmt::format("Failed to connect to Kvrocks on {}:{}. Error: {}", config_->kvrocks_host,
                                config_->kvrocks_port, sock_fd.Msg());
      usleep(10000);
      continue;
    }

    sock_fd_ = *sock_fd;
    s = auth();
    if (!s.IsOK()) {
      LOG(ERROR) << s.Msg();
      usleep(10000);
      continue;
    }

    while (!IsStopped()) {
      s = tryPSync();
      if (!s.IsOK()) {
        LOG(ERROR) << s.Msg();
        break;
      }
      LOG(INFO) << "[kvrocks2redis] PSync is ok, start increment batch loop";
      s = incrementBatchLoop();
      if (!s.IsOK()) {
        LOG(ERROR) << s.Msg();
        continue;
      }
    }
    close(sock_fd_);
  }
}

void Sync::Stop() {
  if (stop_flag_) return;

  stop_flag_ = true;  // Stopping procedure is asynchronous,
  LOG(INFO) << "[kvrocks2redis] Stopped";
}

Status Sync::auth() {
  // Send auth when needed
  if (!config_->kvrocks_auth.empty()) {
    const auto auth_command = Redis::MultiBulkString({"AUTH", config_->kvrocks_auth});
    auto s = Util::SockSend(sock_fd_, auth_command);
    if (!s) return s.Prefixed("send auth command err");
    std::string line = GET_OR_RET(Util::SockReadLine(sock_fd_).Prefixed("read auth response err"));
    if (line.compare(0, 3, "+OK") != 0) {
      return {Status::NotOK, "auth got invalid response"};
    }
  }
  LOG(INFO) << "[kvrocks2redis] Auth succ, continue...";
  return Status::OK();
}

Status Sync::tryPSync() {
  const auto seq_str = std::to_string(next_seq_);
  const auto seq_len_str = std::to_string(seq_str.length());
  const auto cmd_str = "*2" CRLF "$5" CRLF "PSYNC" CRLF "$" + seq_len_str + CRLF + seq_str + CRLF;
  auto s = Util::SockSend(sock_fd_, cmd_str);
  LOG(INFO) << "[kvrocks2redis] Try to use psync, next seq: " << next_seq_;
  if (!s) return s.Prefixed("send psync command err");
  std::string line = GET_OR_RET(Util::SockReadLine(sock_fd_).Prefixed("read psync response err"));

  if (line.compare(0, 3, "+OK") != 0) {
    if (next_seq_ > 0) {
      // Ooops, Failed to psync , sync process has been terminated, administrator should be notified
      // when full sync is needed, please remove last_next_seq config file, and restart kvrocks2redis
      auto error_msg =
          "[kvrocks2redis] CRITICAL - Failed to psync , please remove"
          " last_next_seq config file, and restart kvrocks2redis, redis reply: " +
          std::string(line);
      stop_flag_ = true;
      return {Status::NotOK, error_msg};
    }
    // PSYNC isn't OK, we should use parseAllLocalStorage
    // Switch to parseAllLocalStorage
    LOG(INFO) << "[kvrocks2redis] Failed to psync, redis reply: " << std::string(line);
    parseKVFromLocalStorage();
    // Restart tryPSync
    return tryPSync();
  }
  return Status::OK();
}

Status Sync::incrementBatchLoop() {
  std::cout << "Start parse increment batch ..." << std::endl;
  evbuffer *evbuf = evbuffer_new();
  while (!IsStopped()) {
    if (evbuffer_read(evbuf, sock_fd_, -1) <= 0) {
      evbuffer_free(evbuf);
      return {Status::NotOK, std::string("[kvrocks2redis] read increment batch err: ") + strerror(errno)};
    }
    if (incr_state_ == IncrementBatchLoopState::Incr_batch_size) {
      // Read bulk length
      UniqueEvbufReadln line(evbuf, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) {
        usleep(10000);
        continue;
      }
      incr_bulk_len_ = line.length > 0 ? std::strtoull(line.get() + 1, nullptr, 10) : 0;
      if (incr_bulk_len_ == 0) {
        return {Status::NotOK, "[kvrocks2redis] Invalid increment data size"};
      }
      incr_state_ = Incr_batch_data;
    }

    if (incr_state_ == IncrementBatchLoopState::Incr_batch_data) {
      // Read bulk data (batch data)
      if (incr_bulk_len_ + 2 <= evbuffer_get_length(evbuf)) {  // We got enough data
        char *bulk_data = reinterpret_cast<char *>(evbuffer_pullup(evbuf, static_cast<ssize_t>(incr_bulk_len_) + 2));
        std::string bulk_data_str = std::string(bulk_data, incr_bulk_len_);
        // Skip the ping packet
        if (bulk_data_str != "ping") {
          auto bat = rocksdb::WriteBatch(bulk_data_str);
          int count = static_cast<int>(bat.Count());
          auto s = parser_->ParseWriteBatch(bulk_data_str);
          if (!s.IsOK()) {
            return s.Prefixed(fmt::format("failed to parse write batch '{}'", Util::StringToHex(bulk_data_str)));
          }

          s = updateNextSeq(next_seq_ + count);
          if (!s.IsOK()) {
            return s.Prefixed("failed to update next sequence");
          }
        }
        evbuffer_drain(evbuf, incr_bulk_len_ + 2);
        incr_state_ = Incr_batch_size;
      } else {
        usleep(10000);
        continue;
      }
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

  s = updateNextSeq(storage_->LatestSeqNumber() + 1);
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
  return Util::Pwrite(next_seq_fd_, seq_string, 0);
}
