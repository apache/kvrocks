#ifndef KVROCKS_SERVER_H
#define KVROCKS_SERVER_H

#include <list>
#include <string>
#include <vector>

#include "stats.h"
#include "storage.h"
#include "replication.h"

namespace Redis {
class Connection;
}

class WorkerThread;
class Server {
 public:
  explicit Server(Engine::Storage *storage, Config *config);
  void Start();
  void Stop();
  void Join();

  Status AddMaster(std::string host, uint32_t port);
  void RemoveMaster();
  bool IsLockDown() {return is_locked_;}
  int PublishMessage(std::string &channel, std::string &msg);
  void SubscribeChannel(std::string &channel, Redis::Connection *conn);
  void UnSubscribeChannel(std::string &channel, Redis::Connection *conn);
  Status IncrConnections();
  void DecrConnections();

  Stats stats_;
  Engine::Storage *storage_;
 private:
  bool is_locked_;
  std::string master_host_;
  uint32_t master_port_;
  std::atomic<int> connections_{0};

  Config *config_;
  std::vector<WorkerThread*> workers_;
  std::unique_ptr<ReplicationThread> replication_thread_;

  // TODO: locked before modify
  std::map<std::string, std::list<Redis::Connection*>> pubsub_channels_;
};

#endif //KVROCKS_SERVER_H
