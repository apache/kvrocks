/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <thread>
#include <string>
#include <utility>
#include <vector>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <event2/util.h>
#include "storage.h"

#include "lua.hpp"
#include "redis_connection.h"

class Server;

class Worker {
 public:
  Worker(Server *svr, Config *config, bool repl = false);
  ~Worker();
  Worker(const Worker &) = delete;
  Worker(Worker &&) = delete;
  void Stop();
  void Run(std::thread::id tid);

  void DetachConnection(Redis::Connection *conn);
  void FreeConnection(Redis::Connection *conn);
  void FreeConnectionByID(int fd, uint64_t id);
  Status AddConnection(Redis::Connection *c);
  Status EnableWriteEvent(int fd);
  Status Reply(int fd, const std::string &reply);
  void BecomeMonitorConn(Redis::Connection *conn);
  void FeedMonitorConns(Redis::Connection *conn, const std::vector<std::string> &tokens);

  std::string GetClientsStr();
  void KillClient(Redis::Connection *self, uint64_t id, std::string addr,
                  uint64_t type, bool skipme, int64_t *killed);
  void KickoutIdleClients(int timeout);

  Status ListenUnixSocket(const std::string &path, int perm, int backlog);

  lua_State *Lua() { return lua_; }
  Server *svr_;

 private:
  Status listenTCP(const std::string &host, int port, int backlog);
  static void newTCPConnection(evconnlistener *listener, evutil_socket_t fd,
                               sockaddr *address, int socklen, void *ctx);
  static void newUnixSocketConnection(evconnlistener *listener, evutil_socket_t fd,
                                      sockaddr *address, int socklen, void *ctx);
  static void TimerCB(int, int16_t events, void *ctx);
  Redis::Connection *removeConnection(int fd);


  event_base *base_;
  event *timer_;
  std::thread::id tid_;
  std::vector<evconnlistener*> listen_events_;
  std::mutex conns_mu_;
  std::map<int, Redis::Connection*> conns_;
  std::map<int, Redis::Connection*> monitor_conns_;
  int last_iter_conn_fd = 0;   // fd of last processed connection in previous cron

  struct bufferevent_rate_limit_group *rate_limit_group_ = nullptr;
  struct ev_token_bucket_cfg *rate_limit_group_cfg_ = nullptr;
  lua_State* lua_;
};

class WorkerThread {
 public:
  explicit WorkerThread(std::unique_ptr<Worker> worker) : worker_(std::move(worker)) {}
  ~WorkerThread() = default;
  WorkerThread(const WorkerThread&) = delete;
  WorkerThread(WorkerThread&&) = delete;
  Worker *GetWorker() { return worker_.get(); }
  void Start();
  void Stop();
  void Join();

 private:
  std::thread t_;
  std::unique_ptr<Worker> worker_;
};
