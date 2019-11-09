#include "sync.h"
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <rocksdb/write_batch.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <fstream>

#include "../../src/redis_reply.h"
#include "../../src/util.h"

#include "util.h"

void send_string_to_event(bufferevent *bev, const std::string &data) {
  auto output = bufferevent_get_output(bev);
  evbuffer_add(output, data.c_str(), data.length());
}

Sync::Sync(Engine::Storage *storage, Writer *writer, Parser *parser, Kvrocks2redis::Config *config)
    : storage_(storage),
      writer_(writer),
      parser_(parser),
      config_(config) {
}

Sync::~Sync() {
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
    s = Util::SockConnect(config_->kvrocks_host, config_->kvrocks_port, &sock_fd_);
    if (!s.IsOK()) {
      LOG(ERROR) << s.Msg();
      usleep(10000);
      continue;
    }
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
    if (!s.IsOK()) return Status(Status::NotOK, "send auth command err:" + s.Msg());
    std::string line;
    s = Util::SockReadLine(sock_fd_, &line);
    if (!s.IsOK()) {
      return Status(Status::NotOK, std::string("read auth response err: ") + s.Msg());
    }
    if (line.compare(0, 3, "+OK") != 0) {
      return Status(Status::NotOK, "auth got invalid response");
    }
  }
  LOG(INFO) << "[kvrocks2redis] Auth succ, continue...";
  return Status::OK();
}

Status Sync::tryPSync() {
  const auto seq_str = std::to_string(next_seq_);
  const auto seq_len_str = std::to_string(seq_str.length());
  const auto cmd_str = "*2" CRLF "$5" CRLF "PSYNC" CRLF "$" + seq_len_str +
      CRLF + seq_str + CRLF;
  auto s = Util::SockSend(sock_fd_, cmd_str);
  LOG(INFO) << "[kvrocks2redis] Try to use psync, next seq: " << next_seq_;
  if (!s.IsOK()) return Status(Status::NotOK, "send psync command err:" + s.Msg());
  std::string line;
  s = Util::SockReadLine(sock_fd_, &line);
  if (!s.IsOK()) {
    return Status(Status::NotOK, std::string("read psync response err: ") + s.Msg());
  }

  if (line.compare(0, 3, "+OK") != 0) {
    if (next_seq_ > 0) {
      // Ooops, Failed to psync , sync process has been terminated, administrator should be notified
      // when full sync is needed, please remove last_next_seq config file, and restart kvrocks2redis
      auto error_msg =
          "[kvrocks2redis] CRITICAL - Failed to psync , administrator confirm needed : " + std::string(line);
      stop_flag_ = true;
      return Status(Status::NotOK, error_msg);
    }
    // PSYNC isn't OK, we should use parseAllLocalStorage
    // Switch to parseAllLocalStorage
    parseKVFromLocalStorage();
    LOG(INFO) << "[kvrocks2redis] Failed to psync, switch to parseAllLocalStorage : " << std::string(line);
    // Restart tryPSync
    return tryPSync();
  }
  LOG(INFO) << "[kvrocks2redis] PSync is ok, start increment batch loop";
  return Status::OK();
}

Status Sync::incrementBatchLoop() {
  char *line = nullptr;
  size_t line_len = 0;
  char *bulk_data = nullptr;
  evbuffer *evbuf = evbuffer_new();
  while (!IsStopped()) {
    if (evbuffer_read(evbuf, sock_fd_, -1) <= 0) {
      evbuffer_free(evbuf);
      return Status(Status::NotOK,
                    std::string("[kvrocks2redis] read psync response err: ") + strerror(errno));
    }
    switch (incr_state_) {
      case Incr_batch_size:
        // Read bulk length
        line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) {
          usleep(10000);
          continue;
        }
        incr_bulk_len_ = line_len > 0 ? std::strtoull(line + 1, nullptr, 10) : 0;
        free(line);
        if (incr_bulk_len_ == 0) {
          return Status(Status::NotOK, "[kvrocks2redis] Invalid increment data size");
        }
        incr_state_ = Incr_batch_data;
      case Incr_batch_data:
        // Read bulk data (batch data)
        if (incr_bulk_len_ + 2 <= evbuffer_get_length(evbuf)) {  // We got enough data
          bulk_data = reinterpret_cast<char *>(evbuffer_pullup(evbuf, incr_bulk_len_ + 2));
          auto bat = rocksdb::WriteBatch(std::string(bulk_data, incr_bulk_len_));
          int count = bat.Count();

          parser_->ParseWriteBatch(std::string(bulk_data, incr_bulk_len_));

          updateNextSeq(next_seq_ + count);

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
    auto s = writer_->FlushAll(iter.first);
    if (!s.IsOK()) {
      LOG(ERROR) << "[kvrocks2redis] Failed to flush all in namespace: " << iter.first
                 << ", encounter error: " << s.Msg();
      return;
    }
  }

  Status s = parser_->ParseFullDB();
  if (!s.IsOK()) {
    LOG(ERROR) << "[kvrocks2redis] Failed to parse full db, encounter error: " << s.Msg();
    return;
  }
  updateNextSeq(storage_->LatestSeq() + 1);
}

Status Sync::updateNextSeq(rocksdb::SequenceNumber seq) {
  next_seq_ = seq;
  return writeNextSeqToFile(seq);
}

Status Sync::readNextSeqFromFile(rocksdb::SequenceNumber *seq) {
  next_seq_fd_ = open(config_->next_seq_file_path.data(), O_RDWR | O_CREAT, 0666);
  if (next_seq_fd_ < 0) {
    return Status(Status::NotOK, std::string("Failed to open next seq file :") + strerror(errno));
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

Status Sync::writeNextSeqToFile(rocksdb::SequenceNumber seq) {
  std::string seq_string = std::to_string(seq);
  // append to 21 byte (overwrite entire first 21 byte, aka the largest SequenceNumber size )
  int append_byte = 21 - seq_string.size();
  while (append_byte-- > 0) {
    seq_string += " ";
  }
  seq_string += '\0';
  pwrite(next_seq_fd_, seq_string.data(), seq_string.size(), 0);
  return Status::OK();
}
