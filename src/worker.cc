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

#include "worker.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <list>
#include <cctype>
#include <cstring>
#include <utility>
#include <algorithm>
#include <glog/logging.h>
#include <event2/util.h>

#ifdef ENABLE_OPENSSL
#include <event2/bufferevent_ssl.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#endif

#include "redis_connection.h"
#include "redis_request.h"
#include "scripting.h"
#include "server.h"
#include "util.h"

Worker::Worker(Server *svr, Config *config, bool repl) : svr_(svr) {
  base_ = event_base_new();
  if (!base_) throw std::exception();

  timer_ = event_new(base_, -1, EV_PERSIST, TimerCB, this);
  timeval tm = {10, 0};
  evtimer_add(timer_, &tm);

  int ports[3] = {config->port, config->tls_port, 0};
  auto binds = config->binds;

  for (int* port = ports; *port; ++port) {
    for (const auto &bind : binds) {
      Status s = listenTCP(bind, *port, config->backlog);
      if (!s.IsOK()) {
        LOG(ERROR) << "[worker] Failed to listen on: "<< bind << ":" << *port
                  << ", encounter error: " << s.Msg();
        exit(1);
      }
      LOG(INFO) << "[worker] Listening on: " << bind << ":" << *port;
    }
  }
  lua_ = Lua::CreateState(true);
}

Worker::~Worker() {
  std::list<Redis::Connection*> conns;
  for (const auto &iter : conns_) {
    conns.emplace_back(iter.second);
  }
  for (const auto &iter : monitor_conns_) {
    conns.emplace_back(iter.second);
  }
  for (const auto &iter : conns) {
    iter->Close();
  }
  event_free(timer_);
  if (rate_limit_group_ != nullptr) {
    bufferevent_rate_limit_group_free(rate_limit_group_);
  }
  if (rate_limit_group_cfg_ != nullptr) {
    ev_token_bucket_cfg_free(rate_limit_group_cfg_);
  }
  event_base_free(base_);
  Lua::DestroyState(lua_);
}

void Worker::TimerCB(int, int16_t events, void *ctx) {
  auto worker = static_cast<Worker*>(ctx);
  auto config = worker->svr_->GetConfig();
  if (config->timeout == 0) return;
  worker->KickoutIdleClients(config->timeout);
}

void Worker::newTCPConnection(evconnlistener *listener, evutil_socket_t fd,
                              sockaddr *address, int socklen, void *ctx) {
  auto worker = static_cast<Worker *>(ctx);
  int local_port = Util::GetLocalPort(fd);
  DLOG(INFO) << "[worker] New connection: fd=" << fd
              << " from port: " << local_port << " thread #"
              << worker->tid_;
  auto s = Util::SockSetTcpKeepalive(fd, 120);
  if (!s.IsOK()) {
    LOG(ERROR) << "[worker] Failed to set tcp-keepalive, err:" << s.Msg();
    evutil_closesocket(fd);
    return;
  }
  s = Util::SockSetTcpNoDelay(fd, 1);
  if (!s.IsOK()) {
    LOG(ERROR) << "[worker] Failed to set tcp-nodelay, err:" << s.Msg();
    evutil_closesocket(fd);
    return;
  }
  event_base *base = evconnlistener_get_base(listener);
  auto evThreadSafeFlags = BEV_OPT_THREADSAFE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_UNLOCK_CALLBACKS;

  bufferevent *bev;
#ifdef ENABLE_OPENSSL
  SSL *ssl = nullptr;
  if (local_port == worker->svr_->GetConfig()->tls_port) {
    ssl = SSL_new(worker->svr_->ssl_ctx_.get());
    if (!ssl) {
      LOG(ERROR) << "Failed to construct SSL structure for new connection: " << SSLErrors{};
      evutil_closesocket(fd);
      return;
    }
    bev = bufferevent_openssl_socket_new(base, fd, ssl, BUFFEREVENT_SSL_ACCEPTING, evThreadSafeFlags);
  } else {
    bev = bufferevent_socket_new(base, fd, evThreadSafeFlags);
  }
#else
  bev = bufferevent_socket_new(base, fd, evThreadSafeFlags);
#endif
  if (!bev) {
    auto socket_err = evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
#ifdef ENABLE_OPENSSL
    LOG(ERROR) << "Failed to construct socket for new connection: " << socket_err << ", SSL error: " << SSLErrors{};
    if (ssl) SSL_free(ssl);
#else
    LOG(ERROR) << "Failed to construct socket for new connection: " << socket_err;
#endif
    evutil_closesocket(fd);
    return;
  }
#ifdef ENABLE_OPENSSL
  if (local_port == worker->svr_->GetConfig()->tls_port) {
    bufferevent_openssl_set_allow_dirty_shutdown(bev, 1);
  }
#endif
  auto conn = new Redis::Connection(bev, worker);
  bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite,
                    Redis::Connection::OnEvent, conn);
  bufferevent_enable(bev, EV_READ);
  Status status = worker->AddConnection(conn);
  if (!status.IsOK()) {
    std::string err_msg = Redis::Error("ERR " + status.Msg());
    write(fd, err_msg.data(), err_msg.size());
    conn->Close();
    return;
  }
  std::string ip;
  uint32_t port;
  if (Util::GetPeerAddr(fd, &ip, &port) == 0) {
    conn->SetAddr(ip, port);
  }
  if (worker->rate_limit_group_ != nullptr) {
    bufferevent_add_to_rate_limit_group(bev, worker->rate_limit_group_);
  }
}

void Worker::newUnixSocketConnection(evconnlistener *listener, evutil_socket_t fd,
                                     sockaddr *address, int socklen, void *ctx) {
  auto worker = static_cast<Worker *>(ctx);
  DLOG(INFO) << "[worker] New connection: fd=" << fd
              << " from unixsocket: " << worker->svr_->GetConfig()->unixsocket << " thread #"
              << worker->tid_;
  event_base *base = evconnlistener_get_base(listener);
  auto evThreadSafeFlags = BEV_OPT_THREADSAFE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_UNLOCK_CALLBACKS;
  bufferevent *bev = bufferevent_socket_new(base,
                                            fd,
                                            evThreadSafeFlags);
  auto conn = new Redis::Connection(bev, worker);
  bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite,
                    Redis::Connection::OnEvent, conn);
  bufferevent_enable(bev, EV_READ);
  Status status = worker->AddConnection(conn);
  if (!status.IsOK()) {
    std::string err_msg = Redis::Error("ERR " + status.Msg());
    write(fd, err_msg.data(), err_msg.size());
    conn->Close();
    return;
  }
  conn->SetAddr(worker->svr_->GetConfig()->unixsocket, 0);
  if (worker->rate_limit_group_ != nullptr) {
    bufferevent_add_to_rate_limit_group(bev, worker->rate_limit_group_);
  }
}

Status Worker::listenTCP(const std::string &host, int port, int backlog) {
  char _port[6];
  int af, rv, fd, sock_opt = 1;

  if (strchr(host.data(), ':')) {
    af = AF_INET6;
  } else {
    af = AF_INET;
  }
  snprintf(_port, sizeof(_port), "%d", port);
  struct addrinfo hints, *srv_info, *p;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = af;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  if ((rv = getaddrinfo(host.data(), _port, &hints, &srv_info)) != 0) {
    return Status(Status::NotOK, gai_strerror(rv));
  }

  for (p = srv_info; p != nullptr; p = p->ai_next) {
    if ((fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
      continue;
    if (af == AF_INET6 && setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &sock_opt, sizeof(sock_opt)) == -1) {
      goto error;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sock_opt, sizeof(sock_opt)) < 0) {
      goto error;
    }
    // to support multi-thread binding on macOS
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &sock_opt, sizeof(sock_opt)) < 0) {
      goto error;
    }
    if (bind(fd, p->ai_addr, p->ai_addrlen)) {
      goto error;
    }
    evutil_make_socket_nonblocking(fd);
    auto lev = evconnlistener_new(base_, newTCPConnection, this,
                                  LEV_OPT_CLOSE_ON_FREE, backlog, fd);
    listen_events_.emplace_back(lev);
  }

  freeaddrinfo(srv_info);
  return Status::OK();

error:
  freeaddrinfo(srv_info);
  return Status(Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
}

Status Worker::ListenUnixSocket(const std::string &path, int perm, int backlog) {
  unlink(path.c_str());
  sockaddr_un sa{};
  if (path.size() > sizeof(sa.sun_path) - 1) {
    return Status(Status::NotOK, "unix socket path too long");
  }
  sa.sun_family = AF_LOCAL;
  strncpy(sa.sun_path, path.c_str(), sizeof(sa.sun_path) - 1);
  int fd = socket(AF_LOCAL, SOCK_STREAM, 0);
  if (fd == -1) {
    return Status(Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
  }
  if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
    return Status(Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
  }
  evutil_make_socket_nonblocking(fd);
  auto lev = evconnlistener_new(base_, newUnixSocketConnection, this,
                                LEV_OPT_CLOSE_ON_FREE, backlog, fd);
  listen_events_.emplace_back(lev);
  if (perm != 0) {
    chmod(sa.sun_path, (mode_t)perm);
  }
  return Status::OK();
}

void Worker::Run(std::thread::id tid) {
  tid_ = tid;
  if (event_base_dispatch(base_) != 0) {
    LOG(ERROR) << "[worker] Failed to run server, err: " << strerror(errno);
  }
}

void Worker::Stop() {
  event_base_loopbreak(base_);
  for (const auto &lev : listen_events_) {
    evutil_socket_t fd = evconnlistener_get_fd(lev);
    if (fd > 0) close(fd);
    evconnlistener_free(lev);
  }
}

Status Worker::AddConnection(Redis::Connection *c) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(c->GetFD());
  if (iter != conns_.end()) {
    return Status(Status::NotOK, "connection was exists");
  }
  int max_clients = svr_->GetConfig()->maxclients;
  if (svr_->IncrClientNum() >= max_clients) {
    svr_->DecrClientNum();
    return Status(Status::NotOK, "max number of clients reached");
  }
  conns_.insert(std::pair<int, Redis::Connection*>(c->GetFD(), c));
  uint64_t id = svr_->GetClientID()->fetch_add(1, std::memory_order_relaxed);
  c->SetID(id);
  return Status::OK();
}


Redis::Connection *Worker::removeConnection(int fd) {
  Redis::Connection *conn = nullptr;
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    conn = iter->second;
    conns_.erase(iter);
    svr_->DecrClientNum();
  }
  iter = monitor_conns_.find(fd);
  if (iter != monitor_conns_.end()) {
    conn = iter->second;
    monitor_conns_.erase(iter);
    svr_->DecrClientNum();
    svr_->DecrMonitorClientNum();
  }
  return conn;
}

void Worker::DetachConnection(Redis::Connection *conn) {
  if (!conn) return;
  removeConnection(conn->GetFD());
  if (rate_limit_group_ != nullptr) {
    bufferevent_remove_from_rate_limit_group(conn->GetBufferEvent());
  }
  auto bev = conn->GetBufferEvent();
  bufferevent_disable(bev, EV_READ|EV_WRITE);
  bufferevent_setcb(bev, nullptr, nullptr, nullptr, nullptr);
}

void Worker::FreeConnection(Redis::Connection *conn) {
  if (!conn) return;
  removeConnection(conn->GetFD());
  if (rate_limit_group_ != nullptr) {
    bufferevent_remove_from_rate_limit_group(conn->GetBufferEvent());
  }
#ifdef ENABLE_OPENSSL
  if (SSL *ssl = bufferevent_openssl_get_ssl(conn->GetBufferEvent())) {
    SSL_free(ssl);
  }
#endif
  delete conn;
}

void Worker::FreeConnectionByID(int fd, uint64_t id) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto conn_iter = conns_.find(fd);
  if (conn_iter != conns_.end() && conn_iter->second->GetID() == id) {
    if (rate_limit_group_ != nullptr) {
      bufferevent_remove_from_rate_limit_group(conn_iter->second->GetBufferEvent());
    }
    delete conn_iter->second;
    conns_.erase(conn_iter);
    svr_->DecrClientNum();
  }
  auto monitor_conn_iter = monitor_conns_.find(fd);
  if (monitor_conn_iter != monitor_conns_.end() && monitor_conn_iter->second->GetID() == id) {
    delete monitor_conn_iter->second;
    monitor_conns_.erase(monitor_conn_iter);
    svr_->DecrClientNum();
    svr_->DecrMonitorClientNum();
  }
}

Status Worker::EnableWriteEvent(int fd) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    auto bev = iter->second->GetBufferEvent();
    bufferevent_enable(bev, EV_WRITE);
    return Status::OK();
  }
  return Status(Status::NotOK);
}

Status Worker::Reply(int fd, const std::string &reply) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    iter->second->SetLastInteraction();
    Redis::Reply(iter->second->Output(), reply);
    return Status::OK();
  }
  return Status(Status::NotOK, "connection doesn't exist");
}

void Worker::BecomeMonitorConn(Redis::Connection *conn) {
  {
    std::lock_guard<std::mutex> guard(conns_mu_);
    conns_.erase(conn->GetFD());
    monitor_conns_[conn->GetFD()] = conn;
  }
  svr_->IncrMonitorClientNum();
  conn->EnableFlag(Redis::Connection::kMonitor);
}

void Worker::FeedMonitorConns(Redis::Connection *conn, const std::vector<std::string> &tokens) {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  std::string output;
  output += std::to_string(tv.tv_sec) + "." + std::to_string(tv.tv_usec);
  output += " [" + conn->GetNamespace() + " " + conn->GetAddr() + "]";
  for (const auto &token : tokens) {
    output += " \"" + token + "\"";
  }
  std::unique_lock<std::mutex> lock(conns_mu_);
  for (const auto &iter : monitor_conns_) {
    if (conn == iter.second) continue;  // skip the monitor command
    if (conn->GetNamespace() == iter.second->GetNamespace()
        || iter.second->GetNamespace() == kDefaultNamespace) {
      iter.second->Reply(Redis::SimpleString(output));
    }
  }
}

std::string Worker::GetClientsStr() {
  std::unique_lock<std::mutex> lock(conns_mu_);
  std::string output;
  for (const auto &iter : conns_) {
    Redis::Connection *conn = iter.second;
    output.append(conn->ToString());
  }
  return output;
}

void Worker::KillClient(Redis::Connection *self, uint64_t id, std::string addr,
                        uint64_t type, bool skipme, int64_t *killed) {
  std::lock_guard<std::mutex> guard(conns_mu_);
  for (const auto &iter : conns_) {
    Redis::Connection* conn = iter.second;
    if (skipme && self == conn) continue;
    if ((type & conn->GetClientType()) ||
        (!addr.empty() && conn->GetAddr() == addr) ||
        (id != 0 && conn->GetID() == id)) {
      conn->EnableFlag(Redis::Connection::kCloseAfterReply);
      // enable write event to notify worker wake up ASAP, and remove the connection
      if (!conn->IsFlagEnabled(Redis::Connection::kSlave)) {  // don't enable any event in slave connection
        auto bev = conn->GetBufferEvent();
        bufferevent_enable(bev, EV_WRITE);
      }
      (*killed)++;
    }
  }
}

void Worker::KickoutIdleClients(int timeout) {
  std::list<std::pair<int, uint64_t>> to_be_killed_conns;
  {
    std::lock_guard<std::mutex> guard(conns_mu_);
    if (conns_.empty()) {
      return;
    }
    int iterations = std::min(static_cast<int>(conns_.size()), 50);
    auto iter = conns_.upper_bound(last_iter_conn_fd);
    while (iterations--) {
      if (iter == conns_.end())
        iter = conns_.begin();
      if (static_cast<int>(iter->second->GetIdleTime()) >= timeout) {
        to_be_killed_conns.emplace_back(
            std::make_pair(iter->first, iter->second->GetID()));
      }
      iter++;
    }
    iter--;
    last_iter_conn_fd = iter->first;
  }
  for (const auto &conn : to_be_killed_conns) {
    FreeConnectionByID(conn.first, conn.second);
  }
}

void WorkerThread::Start() {
  try {
    t_ = std::thread([this]() {
      Util::ThreadSetName("worker");
      this->worker_->Run(std::this_thread::get_id());
    });
  } catch (const std::system_error &e) {
    LOG(ERROR) << "[worker] Failed to start worker thread, err: " << e.what();
    return;
  }
  LOG(INFO) << "[worker] Thread #" << t_.get_id() << " started";
}

void WorkerThread::Stop() {
  worker_->Stop();
}

void WorkerThread::Join() {
  if (t_.joinable()) t_.join();
}
