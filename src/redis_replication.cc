#include <arpa/inet.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <netinet/tcp.h>
#include <string>
#include <thread>

#include "redis_replication.h"
#include "redis_reply.h"
#include "status.h"
#include "sock_util.h"

namespace Redis {

ReplicationThread::ReplicationThread(std::string host, uint32_t port, Engine::Storage *storage)
    : host_(std::move(host)), port_(port), storage_(storage) {
  // TODO:
  // 1. check if the DB has the same base of master's, if not, we should erase
  // the DB.
  //    NOTE: how can we know the current slave's DB can be used to do increment
  //    replication?
  // 2. set the `seq_` to the latest of DB
}

void ReplicationThread::Start() {
  t_ = std::thread([this]() {
    this->Run();
    stop_flag_ = true;
  });  // there might be exception here
}

void ReplicationThread::Stop() {
  stop_flag_ = true;
  t_.join();
}

void ReplicationThread::Run() {
  // 1. Connect to master
  int fd;
  if (sock_connect(host_, port_, &fd) < 0) {
    return;
  }

  // 2. Send PSYNC
  // if PSYNC failed, send _fetch_bk cmd, restore the backup, repeat psync.
  // if OK, just apply the write batch stream.
  auto seq_str = std::to_string(seq_);
  auto seq_len_len = std::to_string(seq_str.length());
  auto cmd_str = "*2" CRLF
                 "$5" CRLF
                 "PSYNC" CRLF
                 "$" + seq_len_len + CRLF
                 + seq_str + CRLF;
  if (sock_send(fd, cmd_str) < 0) {
    return;
  }
  char buf[5]; // "+OK\r\n"
  if (recv(fd, buf, 5, 0) < 0) {
    LOG(ERROR) << "Failed to recv: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    return;
  }
  if (strncmp(buf, "+OK", 3) != 0) {
    // PSYNC Not OK, update backup
    LOG(INFO) << "PSYNC not OK";
    return;
  }

  // 3. Keep receiving batch data and apply to DB
  evbuffer *evbuf = evbuffer_new();
  char *line = nullptr;
  size_t line_len = 0;
  char *bulk_data = nullptr;
  size_t bulk_len = 0;
  while(true) {
    if (evbuffer_read(evbuf, fd, -1) < 0) {
      break;
    }
    line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) continue;
    // Bulk string length received
    bulk_len = line_len > 0 ? std::strtoull(line + 1, nullptr, 10) : 0;
    free(line);
    while(true) {
      auto delta = (bulk_len + 2) - evbuffer_get_length(evbuf);
      if (delta <= 0) { // We got enough data
        bulk_data = reinterpret_cast<char *>(evbuffer_pullup(evbuf, bulk_len + 2));
        LOG(INFO) << "Data received: " << std::string(bulk_data, bulk_len);
        storage_->WriteBatch(std::string(bulk_data, bulk_len));
        evbuffer_drain(evbuf, bulk_len + 2);
        break;
      } else { // Not enough data, keep receiving
        evbuffer_read(evbuf, fd, delta);
      }
    }
  }

  evbuffer_free(evbuf);
}

}  // namespace Redis
