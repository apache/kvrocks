#pragma once

#include <thread>
#include <event2/bufferevent.h>

#include "status.h"
#include "storage.h"
typedef enum {
  REPL_CONNECTING = 1,
  REPL_SEND_PSYNC,
  REPL_FETCH_META,
  REPL_FETCH_SST,
  REPL_CONNECTED
}ReplState;

class ReplicationThread {
 public:
  explicit ReplicationThread(std::string host, uint32_t port, Engine::Storage *storage);
  void Start(std::function<void()> pre_fullsync_cb, std::function<void()> post_fullsync_cb);
  void Stop();
  ReplState State() { return repl_state_; }
  rocksdb::SequenceNumber Offset() { return seq_; }

 private:
  std::thread t_;
  bool stop_flag_ = false;
  std::string host_;
  uint32_t port_;
  Engine::Storage *storage_;
  rocksdb::SequenceNumber seq_ = 0;
  ReplState repl_state_;
  Status last_status = Status::OK(); // Use to indicate some fatal errors

  void Run(std::function<void()> pre_fullsync_cb, std::function<void()> post_fullsync_cb);
  Status CheckDBName(int sock_fd);
  Status TryPsync(int sock_fd);
  Status IncrementBatchLoop(int sock_fd);
  Status FullSync(int sock_fd);
  Status FetchFile(int sock_fd, std::string path, uint32_t crc);
};
