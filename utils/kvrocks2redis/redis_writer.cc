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

#include "redis_writer.h"

#include <assert.h>
#include <fcntl.h>
#include <unistd.h>

#include <system_error>

#include "io_util.h"
#include "server/redis_reply.h"
#include "thread_util.h"

RedisWriter::RedisWriter(kvrocks2redis::Config *config) : Writer(config) {
  try {
    t_ = std::thread([this]() {
      util::ThreadSetName("redis-writer");
      this->sync();
      assert(stop_flag_);
    });
  } catch (const std::system_error &e) {
    LOG(ERROR) << "[kvrocks2redis] Failed to create thread: " << e.what();
    return;
  }
}

RedisWriter::~RedisWriter() {
  for (const auto &iter : next_offset_fds_) {
    close(iter.second);
  }
  for (const auto &iter : redis_fds_) {
    close(iter.second);
  }
}

Status RedisWriter::Write(const std::string &ns, const std::vector<std::string> &aofs) {
  auto s = Writer::Write(ns, aofs);
  if (!s.IsOK()) {
    return s;
  }

  return Status::OK();
}

Status RedisWriter::FlushDB(const std::string &ns) {
  auto s = Writer::FlushDB(ns);
  if (!s.IsOK()) {
    return s;
  }

  s = updateNextOffset(ns, 0);
  if (!s.IsOK()) {
    return s;
  }

  s = Write(ns, {redis::ArrayOfBulkStrings({"FLUSHDB"})});
  if (!s.IsOK()) return s;

  return Status::OK();
}

void RedisWriter::Stop() {
  if (stop_flag_) return;

  stop_flag_ = true;  // Stopping procedure is asynchronous,

  if (t_.joinable()) t_.join();
  // handled by sync func
  LOG(INFO) << "[kvrocks2redis] redis_writer Stopped";
}

void RedisWriter::sync() {
  for (const auto &iter : config_->tokens) {
    Status s = readNextOffsetFromFile(iter.first, &next_offsets_[iter.first]);
    if (!s.IsOK()) {
      LOG(ERROR) << s.Msg();
      return;
    }
  }

  size_t chunk_size = 4 * 1024 * 1024;
  char *buffer = new char[chunk_size];
  while (!stop_flag_) {
    for (const auto &iter : config_->tokens) {
      Status s = GetAofFd(iter.first);
      if (!s.IsOK()) {
        LOG(ERROR) << s.Msg();
        continue;
      }

      s = getRedisConn(iter.first, iter.second.host, iter.second.port, iter.second.auth, iter.second.db_number);
      if (!s.IsOK()) {
        LOG(ERROR) << s.Msg();
        continue;
      }

      while (true) {
        auto getted_line_leng = pread(aof_fds_[iter.first], buffer, chunk_size, next_offsets_[iter.first]);
        if (getted_line_leng <= 0) {
          if (getted_line_leng < 0) {
            LOG(ERROR) << "ERR read aof file : " << strerror(errno);
          }
          break;
        }

        std::string con = std::string(buffer, getted_line_leng);
        s = util::SockSend(redis_fds_[iter.first], std::string(buffer, getted_line_leng));
        if (!s.IsOK()) {
          LOG(ERROR) << "ERR send data to redis err: " << s.Msg();
          break;
        }

        auto line_state = util::SockReadLine(redis_fds_[iter.first]);
        if (!line_state) {
          LOG(ERROR) << "read redis response err: " << s.Msg();
          break;
        }

        std::string line = *line_state;
        if (line.compare(0, 1, "-") == 0) {
          // Ooops, something went wrong , sync process has been terminated, administrator should be notified
          // when full sync is needed, please remove last_next_seq config file, and restart kvrocks2redis
          LOG(ERROR) << "[kvrocks2redis] CRITICAL - redis sync return error , administrator confirm needed : " << line;
          delete[] buffer;
          Stop();
          return;
        }

        s = updateNextOffset(iter.first, next_offsets_[iter.first] + getted_line_leng);
        if (!s.IsOK()) {
          LOG(ERROR) << "ERR updating next offset: " << s.Msg();
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  delete[] buffer;
}

Status RedisWriter::getRedisConn(const std::string &ns, const std::string &host, uint32_t port, const std::string &auth,
                                 int db_index) {
  auto iter = redis_fds_.find(ns);
  if (iter == redis_fds_.end()) {
    redis_fds_[ns] = GET_OR_RET(util::SockConnect(host, port).Prefixed("Failed to connect to redis"));

    if (!auth.empty()) {
      auto s = authRedis(ns, auth);
      if (!s.IsOK()) {
        close(redis_fds_[ns]);
        redis_fds_.erase(ns);
        return s;
      }
    }

    if (db_index != 0) {
      auto s = selectDB(ns, db_index);
      if (!s.IsOK()) {
        close(redis_fds_[ns]);
        redis_fds_.erase(ns);
        return s;
      }
    }
  }

  return Status::OK();
}

Status RedisWriter::authRedis(const std::string &ns, const std::string &auth) {
  const auto auth_len_str = std::to_string(auth.length());
  auto s = util::SockSend(redis_fds_[ns], "*2" CRLF "$4" CRLF "auth" CRLF "$" + auth_len_str + CRLF + auth + CRLF);
  if (!s.IsOK()) {
    return s.Prefixed("[kvrocks2redis] failed to send AUTH command");
  }

  std::string line = GET_OR_RET(util::SockReadLine(redis_fds_[ns]).Prefixed("read redis auth response err"));
  if (line.compare(0, 3, "+OK") != 0) {
    return {Status::NotOK, "[kvrocks2redis] redis Auth failed: " + line};
  }

  return Status::OK();
}

Status RedisWriter::selectDB(const std::string &ns, int db_number) {
  const auto db_number_str = std::to_string(db_number);
  const auto db_number_str_len = std::to_string(db_number_str.length());
  auto s = util::SockSend(redis_fds_[ns],
                          "*2" CRLF "$6" CRLF "select" CRLF "$" + db_number_str_len + CRLF + db_number_str + CRLF);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send SELECT command to socket");
  }

  LOG(INFO) << "[kvrocks2redis] select db request was sent, waiting for response";
  std::string line = GET_OR_RET(util::SockReadLine(redis_fds_[ns]).Prefixed("read select db response err"));
  if (line.compare(0, 3, "+OK") != 0) {
    return {Status::NotOK, "[kvrocks2redis] redis select db failed: " + line};
  }

  return Status::OK();
}

Status RedisWriter::updateNextOffset(const std::string &ns, std::istream::off_type offset) {
  next_offsets_[ns] = offset;
  return writeNextOffsetToFile(ns, offset);
}

Status RedisWriter::readNextOffsetFromFile(const std::string &ns, std::istream::off_type *offset) {
  next_offset_fds_[ns] = open(getNextOffsetFilePath(ns).data(), O_RDWR | O_CREAT, 0666);
  if (next_offset_fds_[ns] < 0) {
    return Status::FromErrno("Failed to open next offset file");
  }

  *offset = 0;
  // 256 + 1 byte, extra one byte for the ending \0
  char buf[257];
  memset(buf, '\0', sizeof(buf));
  if (read(next_offset_fds_[ns], buf, sizeof(buf)) > 0) {
    *offset = std::stoll(buf);
  }

  return Status::OK();
}

Status RedisWriter::writeNextOffsetToFile(const std::string &ns, std::istream::off_type offset) {
  std::string offset_string = std::to_string(offset);
  // append to 256 byte (overwrite entire first 21 byte, aka the largest SequenceNumber size )
  int append_byte = 256 - static_cast<int>(offset_string.size());
  while (append_byte-- > 0) {
    offset_string += " ";
  }
  offset_string += '\0';
  return util::Pwrite(next_offset_fds_[ns], offset_string, 0);
}

std::string RedisWriter::getNextOffsetFilePath(const std::string &ns) {
  return config_->output_dir + ns + "_" + config_->next_offset_file_name;
}
