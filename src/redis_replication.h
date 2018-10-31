#pragma once

#include <thread>
#include <event2/bufferevent.h>

#include "status.h"
#include "storage.h"

namespace Redis {

class ReplicationThread {
 public:
  explicit ReplicationThread(std::string host, uint32_t port, Engine::Storage *storage);
  void Start();
  void Run();
  void Stop();

 private:
  std::thread t_;
  bool stop_flag_ = false;
  std::string host_;
  uint32_t port_;
  Engine::Storage *storage_;
  rocksdb::SequenceNumber seq_ = 0;

  Status TryPsync(int sock_fd);
  Status IncrementBatchLoop(int sock_fd);
  Status FullSync(int sock_fd);
  Status FetchFile(int sock_fd, std::string path, uint32_t crc);
};

}  // namespace Redis
