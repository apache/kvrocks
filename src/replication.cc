#include <arpa/inet.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <netinet/tcp.h>
#include <string>
#include <thread>

#include "replication.h"
#include "redis_reply.h"
#include "rocksdb_crc32c.h"
#include "sock_util.h"
#include "status.h"

void send_string(bufferevent *bev, const std::string& data) {
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

ReplicationThread::ReplicationThread(std::string host, uint32_t port,
                                     Engine::Storage *storage)
    : host_(std::move(host)), port_(port), storage_(storage), repl_state_(kReplConnecting) {
}

void ReplicationThread::Start(std::function<void()>&& pre_fullsync_cb, std::function<void()>&& post_fullsync_cb) {
  pre_fullsync_cb_ = std::move(pre_fullsync_cb);
  post_fullsync_cb_ = std::move(post_fullsync_cb);
  try {
    t_ = std::thread([this]() {
      this->Run();
      stop_flag_ = true;
    });  // there might be exception here
  } catch (const std::system_error &e) {
    LOG(ERROR) << "[replication] Failed to create replication thread: "
        << e.what();
    return;
  }
  LOG(INFO) << "[replication] Start";
}

void ReplicationThread::Stop() {
  stop_flag_ = true; // Stopping procedure is asynchronous, handled by timer
  t_.join();
  LOG(INFO) << "[replication] Stopped";
}

/*
 * Run connect to master, and start the following steps asynchronously
 *  - CheckDBName
 *  - TryPsync
 *  - - if ok, IncrementBatchLoop
 *  - - not, FullSync and restart TryPsync when done
 */
void ReplicationThread::Run() {
  base_ = event_base_new();
  auto sockaddr_inet = new_sockaddr_inet(host_, port_);
  upstream_bev_ = bufferevent_socket_new(base_, -1, BEV_OPT_CLOSE_ON_FREE);
  set_write_cb(upstream_bev_, CheckDBName_write_cb, this);
  if (bufferevent_socket_connect(upstream_bev_, reinterpret_cast<sockaddr *>(&sockaddr_inet),
                             sizeof(sockaddr_inet)) != 0) {
    LOG(ERROR) << "Failed to connect";
  }

  auto timer = event_new(base_, -1, EV_PERSIST, Timer_cb, this);
  timeval tmo{1, 0}; // 1 sec
  evtimer_add(timer, &tmo);

  event_base_dispatch(base_);
  event_base_free(base_);
}

void ReplicationThread::CheckDBName_write_cb(bufferevent *bev, void *ctx) {
  send_string(bev, "*1" CRLF "$8" CRLF "_db_name" CRLF);
  set_read_cb(bev, CheckDBName_read_cb, ctx);
}

void ReplicationThread::CheckDBName_read_cb(bufferevent *bev, void *ctx) {
  char *line;
  size_t line_len;
  auto input = bufferevent_get_input(bev);
  line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return; // Wait for more data

  auto self = static_cast<ReplicationThread*>(ctx);
  if (!strncmp(line, self->storage_->GetName().c_str(), line_len)) {
    // DB name match, we should continue to next step: TryPsync
    set_write_cb(bev, TryPsync_write_cb, ctx);
  }
  free(line);
}

void ReplicationThread::TryPsync_write_cb(bufferevent *bev, void *ctx) {
  auto self = static_cast<ReplicationThread*>(ctx);
  const auto seq_str = std::to_string(self->seq_);
  const auto seq_len_len = std::to_string(seq_str.length());
  const auto cmd_str = "*2" CRLF "$5" CRLF "PSYNC" CRLF "$" + seq_len_len + CRLF +
      seq_str + CRLF;
  send_string(bev, cmd_str);
  set_read_cb(bev, TryPsync_read_cb, ctx);
}

void ReplicationThread::TryPsync_read_cb(bufferevent *bev, void *ctx) {
  char *line;
  size_t line_len;
  auto self = static_cast<ReplicationThread*>(ctx);
  auto input = bufferevent_get_input(bev);
  line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return;

  if (strncmp(line, "+OK", 3) != 0) {
    // PSYNC isn't OK, we should use FullSync
    // Reconnect so the _full_sync cmd can be processed (the server won't
    // process other cmd once entering SidecarCommandThread
    auto base = bufferevent_get_base(bev);
    bufferevent_free(bev); // Close previous conn
    auto sockaddr_inet = new_sockaddr_inet(self->host_, self->port_);
    self->upstream_bev_ = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE);
    set_write_cb(self->upstream_bev_, FullSync_write_cb, ctx);
    bufferevent_socket_connect(self->upstream_bev_,
                               reinterpret_cast<sockaddr *>(&sockaddr_inet),
                               sizeof(sockaddr_inet));
  } else {
    // PSYNC is OK, use IncrementBatchLoop
    set_read_cb(bev, IncrementBatchLoop_cb, ctx);
  }
}

void ReplicationThread::IncrementBatchLoop_cb(bufferevent *bev, void *ctx) {
  char *line = nullptr;
  size_t line_len = 0;
  char *bulk_data = nullptr;
  auto self = static_cast<ReplicationThread*>(ctx);
  auto input = bufferevent_get_input(bev);
  while(true) {
    switch (self->incr_state_) {
      case Incr_batch_size:
        // Read bulk length
        line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return; // Wait for more data
        self->incr_bulk_len_ = line_len > 0 ? std::strtoull(line + 1, nullptr, 10) : 0;
        free(line);
        if (self->incr_bulk_len_ == 0) {
          LOG(ERROR) << "[replication] Invalid increment data size";
          self->stop_flag_ = true;
          return;
        }
        self->incr_state_ = Incr_batch_data;
      case Incr_batch_data:
        // Read bulk data (batch data)
        auto delta = (self->incr_bulk_len_ + 2) - evbuffer_get_length(input);
        if (delta <= 0) {  // We got enough data
          bulk_data = reinterpret_cast<char *>(evbuffer_pullup(input, self->incr_bulk_len_ + 2));
          self->storage_->WriteBatch(std::string(bulk_data, self->incr_bulk_len_));
          evbuffer_drain(input, self->incr_bulk_len_ + 2);
          self->incr_state_ = Incr_batch_size;
        } else {
          return; // Wait for more data
        }
    }
  }
}

void ReplicationThread::FullSync_write_cb(bufferevent *bev, void *ctx) {
  send_string(bev, "*1" CRLF "$11" CRLF "_fetch_meta" CRLF);
  set_read_cb(bev, FullSync_read_cb, ctx);
}

void ReplicationThread::FullSync_read_cb(bufferevent *bev, void *ctx) {
  char *line;
  size_t line_len;
  auto self = static_cast<ReplicationThread*>(ctx);
  auto input = bufferevent_get_input(bev);
  switch (self->fullsync_state_) {
    case Fetch_meta_id:
      line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) return; // Wait for more data
      self->fullsync_meta_id_ = static_cast<rocksdb::BackupID>(
          line_len > 0 ? std::strtoul(line, nullptr, 10) : 0);
      free(line);
      if (self->fullsync_meta_id_ == 0) {
        LOG(ERROR) << "[replication] Invalid meta id received";
        self->stop_flag_ = true;
        return;
      }
      self->fullsync_state_ = Fetch_meta_size;
    case Fetch_meta_size:
      line = evbuffer_readln(input, &line_len, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) return; // Wait for more data
      self->fullsync_filesize_ = line_len > 0 ? std::strtoull(line, nullptr, 10) : 0;
      free(line);
      if (self->fullsync_filesize_ == 0) {
        LOG(ERROR) << "[replication] Invalid meta file size received";
        self->stop_flag_ = true;
      }
      self->fullsync_state_ = Fetch_meta_content;
    case Fetch_meta_content:
      if (evbuffer_get_length(input) < self->fullsync_filesize_) {
        return; // Wait for more data
      }
      auto meta = Engine::Storage::BackupManager::ParseMetaAndSave(self->storage_,
                                                                   self->fullsync_meta_id_,
                                                                   input);
      assert(evbuffer_get_length(input) == 0);
      self->fullsync_state_ = Fetch_meta_id;

      // TODO: this loop is still a synchronized operation without event base.
      // we should considering some concurrent file fetching methods in the future.
      for (auto f : meta.files) {
        DLOG(INFO) << "> " << f.first << " " << f.second;
        int fd2;
        // Don't fetch existing files
        if (Engine::Storage::BackupManager::FileExists(self->storage_, f.first)) {
          continue;
        }
        // FIXME: don't connect every time, see _fetch_file cmd implementation
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
        self->post_fullsync_cb_();
        self->stop_flag_ = true;
        return;
      }
      self->post_fullsync_cb_();
      ++self->seq_;

      // Reconnect to try Psync again
      auto base = bufferevent_get_base(bev);
      bufferevent_free(bev); // Close previous conn
      auto sockaddr_inet = new_sockaddr_inet(self->host_, self->port_);
      self->upstream_bev_ = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE);
      set_write_cb(self->upstream_bev_, TryPsync_write_cb, ctx);
      bufferevent_socket_connect(self->upstream_bev_,
                                 reinterpret_cast<sockaddr *>(&sockaddr_inet),
                                 sizeof(sockaddr_inet));
  }
}

Status ReplicationThread::FetchFile(int sock_fd, std::string path,
                                    uint32_t crc) {
  const auto cmd_str = "*2" CRLF "$11" CRLF "_fetch_file" CRLF "$" +
                 std::to_string(path.length()) + CRLF + path + CRLF;
  if (sock_send(sock_fd, cmd_str) < 0) {
    return Status(Status::NotOK);
  }

  char *line;
  size_t line_len, file_size;
  evbuffer *evbuf = evbuffer_new();
  // Read file size line
  while (true) {
    // TODO: test stop_flag_
    if (evbuffer_read(evbuf, sock_fd, -1) < 0) {
      LOG(ERROR) << "[replication] Failed to read data file size line";
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
        LOG(ERROR) << "[replication] Failed to read data from socket buffer";
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
  DLOG(INFO) << "[replication] timer";
  auto self = static_cast<ReplicationThread*>(ctx);
  if (self->stop_flag_) {
    LOG(INFO) << "[replication] Stop ev loop";
    bufferevent_free(self->upstream_bev_);
    self->upstream_bev_ = nullptr;
    event_base_loopbreak(self->base_);
  }
}
