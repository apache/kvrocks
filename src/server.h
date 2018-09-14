#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <cstring>
#include <iostream>
#include <memory>
#include <thread>

#include "storage.h"
#include "redis_replication.h"

// forward declare
namespace Redis {
class Request;
}

class Server {
  friend Redis::Request;

 public:
  Server(Engine::Storage &storage, uint32_t port);
  void Run(std::thread::id tid);
  void Stop();
  // TODO: callbacks for psync
  void AddSlave();
  void RemoveSlave();

  // TODO: callbacks for slaveof
  Status AddMaster(std::string host, uint32_t port);
  void RemoveMaster();

  Engine::Storage &storage_;

 private:
  static void NewConnection(evconnlistener *listener, evutil_socket_t fd,
                            sockaddr *address, int socklen, void *ctx);

  event_base *base_;
  sockaddr_in sin_{};
  evutil_socket_t fd_ = 0;
  std::thread::id tid_;

  // Replication related
  bool is_slave_ = false;
  std::string master_host_;
  uint32_t master_port_ = 0;
  std::unique_ptr<Redis::ReplicationThread> replication_thread_;
};

class ServerThread {
 public:
  explicit ServerThread(Server &svr) : svr_(svr) {}
  void Start();
  void Join();

 private:
  std::thread t_;
  Server &svr_;
};
