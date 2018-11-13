#ifndef KVROCKS_SERVER_H
#define KVROCKS_SERVER_H

#include <string>
#include <vector>

#include "storage.h"
#include "replication.h"

class WorkerThread;
class Server {
 public:
  explicit Server(Engine::Storage *storage, int port, int workers);
  void Start();
  void Stop();
  void Join();

  Status AddMaster(std::string host, uint32_t port);
  void RemoveMaster();
  bool IsLockDown() {return is_locked_;}

  Engine::Storage *storage_;
 private:
  bool is_locked_;
  std::string master_host_;
  uint32_t master_port_;

  std::vector<WorkerThread*> workers_;
  int listen_port_;
  std::unique_ptr<ReplicationThread> replication_thread_;
};

#endif //KVROCKS_SERVER_H
