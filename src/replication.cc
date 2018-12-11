#include <arpa/inet.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <netinet/tcp.h>
#include <string>
#include <thread>

#include "redis_reply.h"
#include "replication.h"
#include "rocksdb_crc32c.h"
#include "sock_util.h"
#include "status.h"

void send_string(bufferevent *bev, const std::string &data) {
  auto output = bufferevent_get_output(bev);
  evbuffer_add(output, data.c_str(), data.length());
}

void conn_event_cb(bufferevent *bev, short events, void *ctx) {
  if (events & BEV_EVENT_CONNECTED) {
    // call write_cb when connected
    bufferevent_data_cb write_cb;
    bufferevent_getcb(bev, nullptr, &write_cb, nullptr, nullptr);
    write_cb(bev, ctx);
    return;
  }
  if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
    LOG(ERROR) << "[replication] connection error/eof";
    // TODO: restart replication procedure
  }
}

inline void set_read_cb(bufferevent *bev, bufferevent_data_cb cb, void *ctx) {
  bufferevent_enable(bev, EV_READ);
  bufferevent_setcb(bev, cb, nullptr, conn_event_cb, ctx);
}

void set_write_cb(bufferevent *bev, bufferevent_data_cb cb, void *ctx) {
  bufferevent_enable(bev, EV_WRITE);
  bufferevent_setcb(bev, nullptr, cb, conn_event_cb, ctx);
}

ReplicationThread::CallbacksStateMachine::CallbacksStateMachine(
    ReplicationThread *repl,
    ReplicationThread::CallbacksStateMachine::CallbackList &&handlers)
    : repl_(repl), handlers_(std::move(handlers)) {
  if (!repl_->auth_.empty()) {
    handlers_.emplace_front(CallbacksStateMachine::READ, Auth_read_cb);
    handlers_.emplace_front(CallbacksStateMachine::WRITE, Auth_write_cb);
  }
}

void ReplicationThread::CallbacksStateMachine::evCallback(bufferevent *bev,
                                                          void *ctx) {
  auto self = static_cast<CallbacksStateMachine *>(ctx);
LOOP_LABEL:
  assert(self->handler_idx_ <= self->handlers_.size());
  auto st = self->handlers_[self->handler_idx_].second(bev, self->repl_);
  DLOG(INFO) << "[replication] Execute handler[" << self->handler_idx_ << "]";
  switch (st) {
    case NEXT:
      ++self->handler_idx_;
      if (self->handlers_[self->handler_idx_].first == WRITE) {
        set_write_cb(bev, evCallback, ctx);
      } else {
        set_read_cb(bev, evCallback, ctx);
      }
      // invoke the read handler (of next step) directly, as the bev might
      // have the data already.
      goto LOOP_LABEL;
    case AGAIN:
      break;
    case QUIT:
      bufferevent_free(bev);
      self->bev_ = nullptr;
      self->repl_->repl_state_ = kReplError;
      break;
  }
}

void ReplicationThread::CallbacksStateMachine::Start() {
  if (handlers_.empty()) {
    return;
  }
  auto sockaddr_inet = new_sockaddr_inet(repl_->host_, repl_->port_);
  auto bev = bufferevent_socket_new(repl_->base_, -1, BEV_OPT_CLOSE_ON_FREE);
  if (bufferevent_socket_connect(bev,
                                 reinterpret_cast<sockaddr *>(&sockaddr_inet),
                                 sizeof(sockaddr_inet)) != 0) {
    LOG(ERROR) << "[replication] Failed to connect";
  }
  handler_idx_ = 0;
  if (handlers_.front().first == WRITE) {
    set_write_cb(bev, evCallback, this);
  } else {
    set_read_cb(bev, evCallback, this);
  }
  bev_ = bev;
}

void ReplicationThread::CallbacksStateMachine::Stop() {
  if (bev_) {
    bufferevent_free(bev_);
    bev_ = nullptr;
  }
}

ReplicationThread::ReplicationThread(std::string host, uint32_t port,
                                     Engine::Storage *storage, std::string auth)
    : host_(std::move(host)),
      port_(port),
      auth_(std::move(auth)),
      storage_(storage),
      repl_state_(kReplConnecting),
      psync_steps_(this,
                   CallbacksStateMachine::CallbackList{
                       {CallbacksStateMachine::WRITE, CheckDBName_write_cb},
                       {CallbacksStateMachine::READ, CheckDBName_read_cb},
                       {CallbacksStateMachine::WRITE, TryPsync_write_cb},
                       {CallbacksStateMachine::READ, TryPsync_read_cb},
                       {CallbacksStateMachine::READ, IncrementBatchLoop_cb}}),
      fullsync_steps_(this,
                      CallbacksStateMachine::CallbackList{
                          {CallbacksStateMachine::WRITE, FullSync_write_cb},
                          {CallbacksStateMachine::READ, FullSync_read_cb}}) {}

void ReplicationThread::Start(std::function<void()> &&pre_fullsync_cb,
                              std::function<void()> &&post_fullsync_cb) {
  pre_fullsync_cb_ = std::move(pre_fullsync_cb);
  post_fullsync_cb_ = std::move(post_fullsync_cb);
  try {
    t_ = std::thread([this]() {
      this->Run();
      assert(stop_flag_);
    });
  } catch (const std::system_error &e) {
    LOG(ERROR) << "[replication] Failed to create thread: " << e.what();
    return;
  }
  LOG(INFO) << "[replication] Start";
}

void ReplicationThread::Stop() {
  stop_flag_ = true;  // Stopping procedure is asynchronous,
                      // handled by timer
  t_.join();
  LOG(INFO) << "[replication] Stopped";
}

/*
 * Run connect to master, and start the following steps
 * asynchronously
 *  - CheckDBName
 *  - TryPsync
 *  - - if ok, IncrementBatchLoop
 *  - - not, FullSync and restart TryPsync when done
 */
void ReplicationThread::Run() {
  base_ = event_base_new();
  if (base_ == nullptr) {
    LOG(ERROR) << "[replication] Failed to create new ev base";
    return;
  }
  psync_steps_.Start();

  auto timer = event_new(base_, -1, EV_PERSIST, Timer_cb, this);
  timeval tmo{1, 0};  // 1 sec
  evtimer_add(timer, &tmo);

  event_base_dispatch(base_);
  event_base_free(base_);
}

ReplicationThread::CBState ReplicationThread::Auth_write_cb(bufferevent *bev,
                                                            void *ctx) {
  auto self = static_cast<ReplicationThread *>(ctx);
  const auto auth_len_str = std::to_string(self->auth_.length());
  send_string(bev, "*2" CRLF "$4" CRLF "auth" CRLF "$" + auth_len_str + CRLF +
                       self->auth_ + CRLF);
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::Auth_read_cb(bufferevent *bev,
                                                           void *ctx) {
  char *line;
  size_t line_len;
  auto input = bufferevent_get_input(bev);
  line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;
  if (strncmp(line, "+OK", 3) != 0) {
    // Auth failed
    LOG(ERROR) << "[replication] Auth failed";
    return CBState::QUIT;
  }
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::CheckDBName_write_cb(
    bufferevent *bev, void *ctx) {
  send_string(bev, "*1" CRLF "$8" CRLF "_db_name" CRLF);
  auto self = static_cast<ReplicationThread *>(ctx);
  self->repl_state_ = kReplCheckDBName;
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::CheckDBName_read_cb(
    bufferevent *bev, void *ctx) {
  char *line;
  size_t line_len;
  auto input = bufferevent_get_input(bev);
  line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;

  auto self = static_cast<ReplicationThread *>(ctx);
  std::string db_name = self->storage_->GetName();
  if (line_len == db_name.size() && !strncmp(line, db_name.data(), line_len)) {
    // DB name match, we should continue to next step: TryPsync
    free(line);
    return CBState::NEXT;
  }
  free(line);
  LOG(ERROR) << "[replication] db-name mismatched";
  return CBState::QUIT;
}

ReplicationThread::CBState ReplicationThread::TryPsync_write_cb(
    bufferevent *bev, void *ctx) {
  auto self = static_cast<ReplicationThread *>(ctx);
  const auto seq_str = std::to_string(self->seq_);
  const auto seq_len_str = std::to_string(seq_str.length());
  const auto cmd_str = "*2" CRLF "$5" CRLF "PSYNC" CRLF "$" + seq_len_str +
                       CRLF + seq_str + CRLF;
  send_string(bev, cmd_str);
  self->repl_state_ = kReplSendPSync;
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::TryPsync_read_cb(bufferevent *bev,
                                                               void *ctx) {
  char *line;
  size_t line_len;
  auto self = static_cast<ReplicationThread *>(ctx);
  auto input = bufferevent_get_input(bev);
  line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;

  if (strncmp(line, "+OK", 3) != 0) {
    // PSYNC isn't OK, we should use FullSync
    // Switch to fullsync state machine
    self->fullsync_steps_.Start();
    return CBState::QUIT;
  } else {
    // PSYNC is OK, use IncrementBatchLoop
    return CBState::NEXT;
  }
}

ReplicationThread::CBState ReplicationThread::IncrementBatchLoop_cb(
    bufferevent *bev, void *ctx) {
  char *line = nullptr;
  size_t line_len = 0;
  char *bulk_data = nullptr;
  auto self = static_cast<ReplicationThread *>(ctx);
  self->repl_state_ = kReplConnected;
  auto input = bufferevent_get_input(bev);
  while (true) {
    switch (self->incr_state_) {
      case Incr_batch_size:
        // Read bulk length
        line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return CBState::AGAIN;
        self->incr_bulk_len_ =
            line_len > 0 ? std::strtoull(line + 1, nullptr, 10) : 0;
        free(line);
        if (self->incr_bulk_len_ == 0) {
          LOG(ERROR) << "[replication] Invalid increment data size";
          self->stop_flag_ = true;
          return CBState::QUIT;
        }
        self->incr_state_ = Incr_batch_data;
      case Incr_batch_data:
        // Read bulk data (batch data)
        auto delta = (self->incr_bulk_len_ + 2) - evbuffer_get_length(input);
        if (delta <= 0) {  // We got enough data
          bulk_data = reinterpret_cast<char *>(
              evbuffer_pullup(input, self->incr_bulk_len_ + 2));
          self->storage_->WriteBatch(
              std::string(bulk_data, self->incr_bulk_len_));
          evbuffer_drain(input, self->incr_bulk_len_ + 2);
          self->incr_state_ = Incr_batch_size;
        } else {
          return CBState::AGAIN;
        }
    }
  }
}

ReplicationThread::CBState ReplicationThread::FullSync_write_cb(
    bufferevent *bev, void *ctx) {
  send_string(bev, "*1" CRLF "$11" CRLF "_fetch_meta" CRLF);
  auto self = static_cast<ReplicationThread *>(ctx);
  self->repl_state_ = kReplFetchMeta;
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::FullSync_read_cb(bufferevent *bev,
                                                               void *ctx) {
  char *line;
  size_t line_len;
  auto self = static_cast<ReplicationThread *>(ctx);
  auto input = bufferevent_get_input(bev);
  switch (self->fullsync_state_) {
    case Fetch_meta_id:
      line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) return CBState::AGAIN;
      self->fullsync_meta_id_ = static_cast<rocksdb::BackupID>(
          line_len > 0 ? std::strtoul(line, nullptr, 10) : 0);
      free(line);
      if (self->fullsync_meta_id_ == 0) {
        LOG(ERROR) << "[replication] Invalid meta id received";
        self->stop_flag_ = true;
        return CBState::QUIT;
      }
      self->fullsync_state_ = Fetch_meta_size;
    case Fetch_meta_size:
      line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) return CBState::AGAIN;
      self->fullsync_filesize_ =
          line_len > 0 ? std::strtoull(line, nullptr, 10) : 0;
      free(line);
      if (self->fullsync_filesize_ == 0) {
        LOG(ERROR) << "[replication] Invalid meta file size received";
        self->stop_flag_ = true;
        return CBState::QUIT;
      }
      self->fullsync_state_ = Fetch_meta_content;
    case Fetch_meta_content:
      if (evbuffer_get_length(input) < self->fullsync_filesize_) {
        return CBState::AGAIN;
      }
      auto meta = Engine::Storage::BackupManager::ParseMetaAndSave(
          self->storage_, self->fullsync_meta_id_, input);
      assert(evbuffer_get_length(input) == 0);
      self->fullsync_state_ = Fetch_meta_id;

      // TODO: this loop is still a synchronized operation
      // without event base. we should considering some
      // concurrent file fetching methods in the future.
      self->repl_state_ = kReplFetchSST;
      for (auto f : meta.files) {
        DLOG(INFO) << "> " << f.first << " " << f.second;
        int fd2;
        // Don't fetch existing files
        if (Engine::Storage::BackupManager::FileExists(self->storage_,
                                                       f.first)) {
          continue;
        }
        // FIXME: don't connect every time, see _fetch_file cmd
        // implementation
        if (sock_connect(self->host_, self->port_, &fd2) < 0) {
          LOG(ERROR) << "[replication] Failed to connect";
          continue;
        }
        self->FetchFile(fd2, f.first, f.second);
        close(fd2);
      }

      // Restore DB from backup
      self->pre_fullsync_cb_();
      if (!self->storage_->RestoreFromBackup(&self->seq_).IsOK()) {
        LOG(ERROR) << "[replication] Failed to restore backup";
        self->post_fullsync_cb_();
        self->stop_flag_ = true;
        return CBState::QUIT;
      }
      self->post_fullsync_cb_();
      ++self->seq_;

      // Switch to psync state machine again
      self->psync_steps_.Start();
      return CBState::QUIT;
  }
}

Status ReplicationThread::FetchFile(int sock_fd, std::string path,
                                    uint32_t crc) {
  char *line;
  size_t line_len, file_size;
  evbuffer *evbuf = evbuffer_new();

  // Send auth when needed
  if (!auth_.empty()) {
    const auto auth_len_str = std::to_string(auth_.length());
    sock_send(sock_fd, "*2" CRLF "$4" CRLF "auth" CRLF "$" + auth_len_str +
                           CRLF + auth_ + CRLF);
    while(true) {
      if (evbuffer_read(evbuf, sock_fd, -1) < 0) {
        LOG(ERROR) << "[replication] Failed to auth resp";
        return Status(Status::NotOK);
      }
      line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) continue;
      if (strncmp(line, "+OK", 3) != 0) {
        LOG(ERROR) << "[replication] Auth failed";
        return Status(Status::NotOK);
      }
      break;
    }
  }
  const auto cmd_str = "*2" CRLF "$11" CRLF "_fetch_file" CRLF "$" +
                       std::to_string(path.length()) + CRLF + path + CRLF;
  if (sock_send(sock_fd, cmd_str) < 0) {
    return Status(Status::NotOK);
  }

  // Read file size line
  while (true) {
    // TODO: test stop_flag_
    if (evbuffer_read(evbuf, sock_fd, -1) < 0) {
      LOG(ERROR) << "[replication] Failed to read size line";
      return Status(Status::NotOK);
    }
    line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) continue;
    if (*line == '-') {
      LOG(ERROR) << "[replication] Failed to send _fetch_file cmd: " << line;
    }
    file_size = line_len > 0 ? std::strtoull(line, nullptr, 10) : 0;
    free(line);
    break;
  }
  // Write to tmp file
  auto tmp_file = Engine::Storage::BackupManager::NewTmpFile(storage_, path);
  if (!tmp_file) {
    LOG(ERROR) << "[replication] Failed to create tmp file";
    return Status(Status::NotOK);
  }

  size_t seen_bytes = 0;
  uint32_t tmp_crc = 0;
  char data[1024];
  while (seen_bytes < file_size) {
    if (evbuffer_get_length(evbuf) > 0) {
      auto data_len = evbuffer_remove(evbuf, data, 1024);
      if (data_len == 0) continue;
      if (data_len < 0) {
        LOG(ERROR) << "[replication] Failed to read data";
      }
      tmp_file->Append(rocksdb::Slice(data, data_len));
      tmp_crc = rocksdb::crc32c::Extend(tmp_crc, data, data_len);
      seen_bytes += data_len;
    } else {
      if (evbuffer_read(evbuf, sock_fd, -1) < 0) {
        LOG(ERROR) << "[replication] Failed to read data file";
        return Status(Status::NotOK);
      }
    }
  }
  if (crc != tmp_crc) {
    LOG(ERROR) << "[replication] CRC mismatch";
    return Status(Status::NotOK);
  }
  // File is OK, rename to formal name
  return Engine::Storage::BackupManager::SwapTmpFile(storage_, path);
}

// Check if stop_flag_ is set, when do, tear down replication
void ReplicationThread::Timer_cb(int, short, void *ctx) {
  // DLOG(INFO) << "[replication] timer";
  auto self = static_cast<ReplicationThread *>(ctx);
  if (self->stop_flag_) {
    LOG(INFO) << "[replication] Stop ev loop";
    event_base_loopbreak(self->base_);
    self->psync_steps_.Stop();
    self->fullsync_steps_.Stop();
  }
}
