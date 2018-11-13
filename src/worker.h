#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <thread>

#include "storage.h"
#include "replication.h"

class Server;
// forward declare
namespace Redis {
class Request;
class Connection;
}

class Worker{
  friend Redis::Request;

 public:
  Worker(Server *svr, uint32_t port);
  Worker(const Worker &) = delete;
  Worker(Worker &&) = delete;
  void Run(std::thread::id tid);
  void Stop();

  Status AddConnection(Redis::Connection *c);
  void RemoveConnection(int fd);

  // Lock down the server(slave) so we can start the replication
  bool IsLockDown() {
    return locked_;
  }

  Server *svr_;
  Engine::Storage *storage_;

 private:
  static void NewConnection(evconnlistener *listener, evutil_socket_t fd,
                            sockaddr *address, int socklen, void *ctx);

  event_base *base_;
  sockaddr_in sin_{};
  evutil_socket_t fd_ = 0;
  std::thread::id tid_;

  // Replication related
  bool is_slave_ = false;
  bool locked_ = false;
  std::string master_host_;
  uint32_t master_port_ = 0;
  std::unique_ptr<ReplicationThread> replication_thread_;
  std::map<int, Redis::Connection*> conns_;
};

class WorkerThread {
 public:
  explicit WorkerThread(Worker *worker) : worker_(worker) {}
  WorkerThread(const WorkerThread&) = delete;
  WorkerThread(WorkerThread&&) = delete;
  void Start();
  void Stop();
  void Join();

 private:
  std::thread t_;
  Worker *worker_;
};
