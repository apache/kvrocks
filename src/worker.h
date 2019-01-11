#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <event2/util.h>
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

class Worker {
  friend Redis::Request;

 public:
  Worker(Server *svr, Config *config, bool repl = false);
  ~Worker();
  Worker(const Worker &) = delete;
  Worker(Worker &&) = delete;
  void Stop();
  void Run(std::thread::id tid);
  void RemoveConnection(int fd);
  void RemoveConnectionByID(int fd, uint64_t id);
  Status AddConnection(Redis::Connection *c);
  bool IsRepl() { // Whether if worker is replication worker
    return repl_;
  }

  std::string GetClientsStr();
  void KillClient(int64_t *killed, std::string addr, uint64_t id, bool skipme, Redis::Connection *conn);
  void KickoutIdleClients(int timeout);

  Server *svr_;
 private:
  Status listen(const std::string &host, int port, int backlog);
  static void newConnection(evconnlistener *listener, evutil_socket_t fd,
                            sockaddr *address, int socklen, void *ctx);
  void removeConnection(std::map<int, Redis::Connection*>::iterator iter);

  event_base *base_;
  std::thread::id tid_;
  std::vector<evconnlistener*> listen_events_;
  std::map<int, Redis::Connection*> conns_;
  int last_iter_conn_fd = 0;   // fd of last processed connection in previous cron
  std::mutex conns_mu_;

  bool repl_;
};

class WorkerThread {
 public:
  explicit WorkerThread(Worker *worker) : worker_(worker) {}
  ~WorkerThread() { delete worker_; }
  WorkerThread(const WorkerThread&) = delete;
  WorkerThread(WorkerThread&&) = delete;
  void Start();
  void Stop();
  void Join();

  std::string GetClientsStr();
  void KillClient(int64_t *killed, std::string addr, uint64_t id, bool skipme, Redis::Connection *conn);
  void KickoutIdleClients(int timeout);


 private:
  std::thread t_;
  Worker *worker_;
};
