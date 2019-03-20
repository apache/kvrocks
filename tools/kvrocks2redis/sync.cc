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

#include "util.h"

void send_string_to_event(bufferevent *bev, const std::string &data) {
  auto output = bufferevent_get_output(bev);
  evbuffer_add(output, data.c_str(), data.length());
}

Sync::Sync(Engine::Storage *storage, Writer *writer, Parser *parser, Kvrocks2redis::Config *config)
    : ReplicationThread(config->kvrocks_host, config->kvrocks_port, storage, config->kvrocks_auth),
      storage_(storage),
      writer_(writer),
      parser_(parser),
      config_(config),
      sync_state_(kReplConnecting),
      psync_steps_(this,
                   CallbacksStateMachine::CallbackList{
                       CallbacksStateMachine::CallbackType{
                           CallbacksStateMachine::WRITE, "psync write", tryPSyncWriteCB
                       },
                       CallbacksStateMachine::CallbackType{
                           CallbacksStateMachine::READ, "psync read", tryPSyncReadCB
                       },
                       CallbacksStateMachine::CallbackType{
                           CallbacksStateMachine::READ, "batch loop", incrementBatchLoopCB
                       }
                   }) {
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

  base_ = event_base_new();
  if (base_ == nullptr) {
    LOG(ERROR) << "[kvrocks2redis] Failed to create new ev base";
    return;
  }

  LOG(INFO) << "[kvrocks2redis] Start sync the data from kvrocks to redis";

  psync_steps_.Start();

  auto timer = event_new(base_, -1, EV_PERSIST, EventTimerCB, this);
  timeval tmo{1, 0};  // 1 sec
  evtimer_add(timer, &tmo);

  event_base_dispatch(base_);
  event_free(timer);
  event_base_free(base_);
}

void Sync::Stop() {
  if (stop_flag_) return;

  stop_flag_ = true;  // Stopping procedure is asynchronous,
  // handled by timer
  LOG(INFO) << "[kvrocks2redis] Stopped";
}

Sync::CBState Sync::tryPSyncWriteCB(
    bufferevent *bev, void *ctx) {
  auto self = static_cast<Sync *>(ctx);

  const auto seq_str = std::to_string(self->next_seq_);
  const auto seq_len_str = std::to_string(seq_str.length());
  const auto cmd_str = "*2" CRLF "$5" CRLF "PSYNC" CRLF "$" + seq_len_str +
      CRLF + seq_str + CRLF;
  send_string_to_event(bev, cmd_str);
  self->sync_state_ = kReplSendPSync;
  LOG(INFO) << "[kvrocks2redis] Try to use psync, next seq: " << self->next_seq_;
  return CBState::NEXT;
}

Sync::CBState Sync::tryPSyncReadCB(bufferevent *bev,
                                   void *ctx) {
  char *line;
  size_t line_len;
  auto self = static_cast<Sync *>(ctx);
  auto input = bufferevent_get_input(bev);
  line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;

  if (strncmp(line, "+OK", 3) != 0) {
    if (self->next_seq_ > 0) {
      // Ooops, Failed to psync , sync process has been terminated, administrator should be notified
      // when full sync is needed, please remove last_next_seq config file, and restart kvrocks2redis
      LOG(ERROR) << "[kvrocks2redis] CRITICAL - Failed to psync , administrator confirm needed : ";
      self->stop_flag_ = true;
      return CBState::QUIT;
    }
    // PSYNC isn't OK, we should use parseAllLocalStorage
    // Switch to parseAllLocalStorage
    self->parseKVFromLocalStorage();
    LOG(INFO) << "[kvrocks2redis] Failed to psync, switch to parseAllLocalStorage";
    LOG(INFO) << line;
    free(line);
    // Restart psync state machine
    return CBState::RESTART;
  } else {
    // PSYNC is OK, use IncrementBatchLoop
    free(line);
    LOG(INFO) << "[kvrocks2redis] PSync is ok, start increment batch loop";
    return CBState::NEXT;
  }
}

Sync::CBState Sync::incrementBatchLoopCB(
    bufferevent *bev, void *ctx) {
  char *line = nullptr;
  size_t line_len = 0;
  char *bulk_data = nullptr;
  auto self = static_cast<Sync *>(ctx);
  self->sync_state_ = kReplConnected;
  auto input = bufferevent_get_input(bev);
  while (true) {
    switch (self->incr_state_) {
      case Incr_batch_size:
        // Read bulk length
        line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return CBState::AGAIN;
        self->incr_bulk_len_ = line_len > 0 ? std::strtoull(line + 1, nullptr, 10) : 0;
        free(line);
        if (self->incr_bulk_len_ == 0) {
          LOG(ERROR) << "[kvrocks2redis] Invalid increment data size";
          return CBState::RESTART;
        }
        self->incr_state_ = Incr_batch_data;
      case Incr_batch_data:
        // Read bulk data (batch data)
        if (self->incr_bulk_len_ + 2 <= evbuffer_get_length(input)) {  // We got enough data
          bulk_data = reinterpret_cast<char *>(evbuffer_pullup(input, self->incr_bulk_len_ + 2));
          auto bat = rocksdb::WriteBatch(std::string(bulk_data, self->incr_bulk_len_));
          int count = bat.Count();

          self->parser_->ParseWriteBatch(std::string(bulk_data, self->incr_bulk_len_));

          self->updateNextSeq(self->next_seq_ + count);

          evbuffer_drain(input, self->incr_bulk_len_ + 2);
          self->incr_state_ = Incr_batch_size;
        } else {
          return CBState::AGAIN;
        }
    }
  }
}

// Check if stop_flag_ is set, when do, tear down kvrocks2redis
void Sync::EventTimerCB(int, int16_t, void *ctx) {
  // DLOG(INFO) << "[kvrocks2redis] timer";
  auto self = static_cast<Sync *>(ctx);
  if (self->stop_flag_) {
    LOG(INFO) << "[kvrocks2redis] Stop ev loop";
    event_base_loopbreak(self->base_);
    self->psync_steps_.Stop();
    self->writer_->Stop();
    // stop parseAllLocalStorage ?
  }
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
  char buf[next_seq_string_size_+1];
  memset(buf, '\0', sizeof(buf));
  if (read(next_seq_fd_, buf, sizeof(buf)) > 0) {
    *seq = static_cast<rocksdb::SequenceNumber>(std::stoi(buf));
  }

  return Status::OK();
}

Status Sync::writeNextSeqToFile(rocksdb::SequenceNumber seq) {
  std::string seq_string = std::to_string(seq);
  // append to 21 byte (overwrite entire first 21 byte, aka the largest SequenceNumber size )
  int append_byte = next_seq_string_size_ - seq_string.size();
  while (append_byte-- > 0) {
    seq_string += " ";
  }
  seq_string += '\0';
  pwrite(next_seq_fd_, seq_string.data(), seq_string.size(), 0);
  return Status::OK();
}
