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

ReplicationThread::ReplicationThread(std::string host, uint32_t port,
                                     Engine::Storage *storage)
    : host_(std::move(host)), port_(port), storage_(storage), repl_state_(REPL_CONNECTING) {
  // TODO:
  // 1. check if the DB has the same base of master's, if not, we should erase
  // the DB.
  //    NOTE: how can we know the current slave's DB can be used to do increment
  //    replication?
  // 2. set the `seq_` to the latest of DB
  // 3. how to stop the replication when `slave of no one`? maybe use `select` to break the increment syncing loop
}

void ReplicationThread::Start(std::function<void()> pre_fullsync_cb, std::function<void()> post_fullsync_cb) {
  LOG(INFO) << "[replication] Start";
  t_ = std::thread([this, pre_fullsync_cb, post_fullsync_cb]() {
    this->Run(pre_fullsync_cb, post_fullsync_cb);
    stop_flag_ = true;
  });  // there might be exception here
}

void ReplicationThread::Stop() {
  stop_flag_ = true;
  t_.join();
  LOG(INFO) << "[replication] Stop";
}

void ReplicationThread::Run(std::function<void()> pre_fullsync_cb, std::function<void()> post_fullsync_cb) {
  int fd;
  while(true) {
    // Connect to master
    if (sock_connect(host_, port_, &fd) < 0) {
      LOG(ERROR) << "[replication] Failed to contact master, wait";
      std::this_thread::sleep_for(std::chrono::seconds(5));
      continue;
    }

    // Check master's db_name
    if (!CheckDBName(fd).IsOK()) {
      close(fd);
      LOG(ERROR) << "[replication] Mismatched DB name";
      last_status = Status(Status::DBMismatched);
      return;
    }

    // Update seq_ to the latest
    seq_ = storage_->LatestSeq();

    // Send PSYNC
    repl_state_ = REPL_SEND_PSYNC;
    if (!TryPsync(fd).IsOK()) {
      // FullSync, reconnect
      close(fd);
      if (sock_connect(host_, port_, &fd) < 0) {
        LOG(ERROR) << "[replication] Failed to contact master, wait";
        std::this_thread::sleep_for(std::chrono::seconds(5));
        continue;
      }
      pre_fullsync_cb();
      if (!FullSync(fd).IsOK()) {
        LOG(ERROR) << "[replication] Failed to full-sync";
        post_fullsync_cb();
        continue;
      }
      if (!storage_->RestoreFromBackup(&seq_).IsOK()) {
        LOG(ERROR) << "[replication] Failed to apply backup";
        return;
      }
      post_fullsync_cb();
      seq_++;
    } else {
      // Keep receiving batch data and apply to DB
      IncrementBatchLoop(fd);
    }
    close(fd);
  }
}

Status ReplicationThread::CheckDBName(int sock_fd) {
  const std::string cmd_str = "*1" CRLF "$8" CRLF "_db_name" CRLF;
  if (sock_send(sock_fd, cmd_str) < 0) {
    return Status(Status::NotOK);
  }
  evbuffer *evbuf = evbuffer_new();
  char *line;
  size_t line_len;
  while (true) {
    if (evbuffer_read(evbuf, sock_fd, -1) < 0) {
      LOG(ERROR) << "Failed to read db name";
      break;
    }
    line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) continue;

    if (!strncmp(line, storage_->GetName().c_str(), line_len)) {
      free(line);
      return Status::OK();
    }
    free(line);
    break;
  }
  return Status(Status::NotOK);
}

Status ReplicationThread::TryPsync(int sock_fd) {
  const auto seq_str = std::to_string(seq_);
  const auto seq_len_len = std::to_string(seq_str.length());
  const auto cmd_str = "*2" CRLF "$5" CRLF "PSYNC" CRLF "$" + seq_len_len + CRLF +
                 seq_str + CRLF;
  if (sock_send(sock_fd, cmd_str) < 0) {
    return Status(Status::NotOK);
  }
  char buf[5];  // "+OK\r\n"
  if (recv(sock_fd, buf, 5, 0) < 0) {
    LOG(ERROR) << "Failed to recv: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    return Status(Status::NotOK);
  }
  if (strncmp(buf, "+OK", 3) != 0) {
    // PSYNC Not OK, update backup
    LOG(INFO) << "PSYNC not OK";
    return Status(Status::NotOK);
  }
  return Status::OK();
}

Status ReplicationThread::IncrementBatchLoop(int sock_fd) {
  evbuffer *evbuf = evbuffer_new();
  char *line = nullptr;
  size_t line_len = 0;
  char *bulk_data = nullptr;
  size_t bulk_len = 0;
  repl_state_ = REPL_CONNECTED;
  while (true) {
    // TODO: test stop_flag_
    // Read bulk length
    if (evbuffer_read(evbuf, sock_fd, -1) < 0) {
      LOG(ERROR) << "Failed to batch data";
      break;
    }
    line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) continue;
    // Bulk string length received
    bulk_len = line_len > 0 ? std::strtoull(line + 1, nullptr, 10) : 0;
    free(line);
    // Read bulk data (batch data)
    while (true) {
      auto delta = (bulk_len + 2) - evbuffer_get_length(evbuf);
      if (delta <= 0) {  // We got enough data
        bulk_data =
            reinterpret_cast<char *>(evbuffer_pullup(evbuf, bulk_len + 2));
        LOG(INFO) << "Data received: " << std::string(bulk_data, bulk_len);
        // TODO: log the batch metadata if any
        storage_->WriteBatch(std::string(bulk_data, bulk_len));
        evbuffer_drain(evbuf, bulk_len + 2);
        break;
      } else {  // Not enough data, keep receiving
        evbuffer_read(evbuf, sock_fd, delta);
      }
    }
  }

  evbuffer_free(evbuf);
  return Status::OK();
}

Status ReplicationThread::FullSync(int sock_fd) {
  repl_state_ = REPL_FETCH_META;
  const std::string cmd_str = "*1" CRLF "$11" CRLF "_fetch_meta" CRLF;
  if (sock_send(sock_fd, cmd_str) < 0) {
    return Status(Status::NotOK);
  }

  char *line;
  size_t line_len, file_size;
  rocksdb::BackupID meta_id = 0;
  evbuffer *evbuf = evbuffer_new();
  // Read meta id
  while (true) {
    if (evbuffer_read(evbuf, sock_fd, -1) < 0) {
      LOG(ERROR) << "Failed to read meta id line";
      return Status(Status::NotOK);
    }
    line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) continue;
    meta_id = static_cast<rocksdb::BackupID>(
        line_len > 0 ? std::strtoul(line, nullptr, 10) : 0);
    free(line);
    break;
  }
  if (meta_id == 0) {
    LOG(ERROR) << "Invalid meta id received";
  }
  // Read meta file size line
  while (true) {
    line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) {
      if (evbuffer_read(evbuf, sock_fd, -1) < 0) {
        LOG(ERROR) << "Failed to read meta file size line";
        return Status(Status::NotOK);
      }
      continue;
    }
    file_size = line_len > 0 ? std::strtoull(line, nullptr, 10) : 0;
    free(line);
    break;
  }
  // Read meta file content
  while (evbuffer_get_length(evbuf) < file_size) {
    if (evbuffer_read(evbuf, sock_fd, -1) < 0) break;
  }
  auto meta = Engine::Storage::BackupManager::ParseMetaAndSave(storage_,
                                                               meta_id, evbuf);
  evbuffer_free(evbuf);
  repl_state_ = REPL_FETCH_SST;
  for (auto f : meta.files) {
    LOG(INFO) << "> " << f.first << " " << f.second;
    int fd2;
    // Don't fetch existing files
    if (Engine::Storage::BackupManager::FileExists(storage_, f.first)) {
      continue;
    }
    // FIXME: don't connect every time, see _fetch_file cmd implementation
    if (sock_connect(host_, port_, &fd2) < 0) {
      return Status(Status::NotOK);
    }
    FetchFile(fd2, f.first, f.second);
    close(fd2);
  }
  return Status::OK();
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
      LOG(ERROR) << "Failed to read data file size line";
      return Status(Status::NotOK);
    }
    line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) continue;
    if (*line == '-') {
      LOG(ERROR) << "Failed to send _fetch_file cmd: " << line;
    }
    file_size = line_len > 0 ? std::strtoull(line, nullptr, 10) : 0;
    free(line);
    break;
  }
  // Write to tmp file
  auto tmp_file = Engine::Storage::BackupManager::NewTmpFile(storage_, path);
  if (!tmp_file) {
    LOG(ERROR) << "Failed to create tmp file";
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
        LOG(ERROR) << "Failed to read data from socket buffer";
      }
      tmp_file->Append(rocksdb::Slice(data, data_len));
      tmp_crc = rocksdb::crc32c::Extend(tmp_crc, data, data_len);
      seen_bytes += data_len;
    } else {
      if (evbuffer_read(evbuf, sock_fd, -1) < 0) {
        LOG(ERROR) << "Failed to read data file";
        return Status(Status::NotOK);
      }
    }
  }
  if (crc != tmp_crc) {
    LOG(ERROR) << "CRC mismatch";
    return Status(Status::NotOK);
  }
  // File is OK, rename to formal name
  return Engine::Storage::BackupManager::SwapTmpFile(storage_, path);
}
