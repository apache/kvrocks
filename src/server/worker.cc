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

#include <event2/util.h>
#include <glog/logging.h>

#include <stdexcept>
#include <string>

#include "event2/bufferevent.h"
#include "io_util.h"
#include "scope_exit.h"
#include "thread_util.h"
#include "time_util.h"

#ifdef ENABLE_OPENSSL
#include <event2/bufferevent_ssl.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>

#include <algorithm>
#include <list>
#include <utility>

#include "oneapi/tbb/parallel_for.h"
#include "oneapi/tbb/parallel_reduce.h"
#include "redis_connection.h"
#include "redis_request.h"
#include "server.h"
#include "storage/scripting.h"

Worker::Worker(Server *srv, Config *config) : srv(srv), base_(event_base_new()) {
  if (!base_) throw std::runtime_error{"event base failed to be created"};

  timer_.reset(NewEvent(base_, -1, EV_PERSIST));
  timeval tm = {10, 0};
  evtimer_add(timer_.get(), &tm);

  uint32_t ports[3] = {config->port, config->tls_port, 0};
  auto binds = config->binds;

  for (uint32_t *port = ports; *port; ++port) {
    for (const auto &bind : binds) {
      Status s = listenTCP(bind, *port, config->backlog);
      if (!s.IsOK()) {
        LOG(ERROR) << "[worker] Failed to listen on: " << bind << ":" << *port << ". Error: " << s.Msg();
        exit(1);
      }
      LOG(INFO) << "[worker] Listening on: " << bind << ":" << *port;
    }
  }
  lua_ = lua::CreateState(srv, true);
}

Worker::~Worker() {
  auto conns = tbb::parallel_reduce(
      conns_.range(), std::vector<redis::Connection *>{},
      [](const ConnMap::range_type &range, std::vector<redis::Connection *> &&result) {
        for (auto &it : range) {
          result.push_back(it.second);
        }
        return result;
      },
      [](const std::vector<redis::Connection *> &lhs, const std::vector<redis::Connection *> &rhs) {
        std::vector<redis::Connection *> result = lhs;
        result.insert(result.end(), rhs.begin(), rhs.end());
        return result;
      });

  auto monitor_conns = tbb::parallel_reduce(
      monitor_conns_.range(), std::vector<redis::Connection *>{},
      [](const ConnMap::range_type &range, std::vector<redis::Connection *> &&result) {
        for (auto &it : range) {
          result.push_back(it.second);
        }
        return result;
      },
      [](const std::vector<redis::Connection *> &lhs, const std::vector<redis::Connection *> &rhs) {
        std::vector<redis::Connection *> result = lhs;
        result.insert(result.end(), rhs.begin(), rhs.end());
        return result;
      });

  for (auto conn : conns) {
    conn->Close();
  }

  for (auto conn : monitor_conns) {
    conn->Close();
  }

  timer_.reset();
  if (rate_limit_group_) {
    bufferevent_rate_limit_group_free(rate_limit_group_);
  }
  if (rate_limit_group_cfg_) {
    ev_token_bucket_cfg_free(rate_limit_group_cfg_);
  }
  event_base_free(base_);
  lua::DestroyState(lua_);
}

void Worker::TimerCB(int, int16_t events) {
  auto config = srv->GetConfig();
  if (config->timeout == 0) return;
  KickoutIdleClients(config->timeout);
}

void Worker::newTCPConnection(evconnlistener *listener, evutil_socket_t fd, sockaddr *address, int socklen) {
  int local_port = util::GetLocalPort(fd);  // NOLINT
  DLOG(INFO) << "[worker] New connection: fd=" << fd << " from port: " << local_port << " thread #" << tid_;

  auto s = util::SockSetTcpKeepalive(fd, 120);
  if (!s.IsOK()) {
    LOG(ERROR) << "[worker] Failed to set tcp-keepalive on socket. Error: " << s.Msg();
    evutil_closesocket(fd);
    return;
  }

  s = util::SockSetTcpNoDelay(fd, 1);
  if (!s.IsOK()) {
    LOG(ERROR) << "[worker] Failed to set tcp-nodelay on socket. Error: " << s.Msg();
    evutil_closesocket(fd);
    return;
  }

  event_base *base = evconnlistener_get_base(listener);
  auto ev_thread_safe_flags =
      BEV_OPT_THREADSAFE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_UNLOCK_CALLBACKS | BEV_OPT_CLOSE_ON_FREE;

  bufferevent *bev = nullptr;
  ssl_st *ssl = nullptr;
#ifdef ENABLE_OPENSSL
  if (uint32_t(local_port) == srv->GetConfig()->tls_port) {
    ssl = SSL_new(srv->ssl_ctx.get());
    if (!ssl) {
      LOG(ERROR) << "Failed to construct SSL structure for new connection: " << SSLErrors{};
      evutil_closesocket(fd);
      return;
    }
    bev = bufferevent_openssl_socket_new(base, fd, ssl, BUFFEREVENT_SSL_ACCEPTING, ev_thread_safe_flags);
  } else {
    bev = bufferevent_socket_new(base, fd, ev_thread_safe_flags);
  }
#else
  bev = bufferevent_socket_new(base, fd, ev_thread_safe_flags);
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
  if (uint32_t(local_port) == srv->GetConfig()->tls_port) {
    bufferevent_openssl_set_allow_dirty_shutdown(bev, 1);
  }
#endif
  auto conn = new redis::Connection(bev, this);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ);

  s = AddConnection(conn);
  if (!s.IsOK()) {
    std::string err_msg = redis::Error({Status::NotOK, s.Msg()});
    s = util::SockSend(fd, err_msg, ssl);
    if (!s.IsOK()) {
      LOG(WARNING) << "Failed to send error response to socket: " << s.Msg();
    }
    conn->Close();
    return;
  }

  if (auto s = util::GetPeerAddr(fd)) {
    auto [ip, port] = std::move(*s);
    conn->SetAddr(ip, port);
  }

  if (rate_limit_group_) {
    bufferevent_add_to_rate_limit_group(bev, rate_limit_group_);
  }
}

void Worker::newUnixSocketConnection(evconnlistener *listener, evutil_socket_t fd, sockaddr *address, int socklen) {
  DLOG(INFO) << "[worker] New connection: fd=" << fd << " from unixsocket: " << srv->GetConfig()->unixsocket
             << " thread #" << tid_;
  event_base *base = evconnlistener_get_base(listener);
  auto ev_thread_safe_flags =
      BEV_OPT_THREADSAFE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_UNLOCK_CALLBACKS | BEV_OPT_CLOSE_ON_FREE;
  bufferevent *bev = bufferevent_socket_new(base, fd, ev_thread_safe_flags);

  auto conn = new redis::Connection(bev, this);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ);

  auto s = AddConnection(conn);
  if (!s.IsOK()) {
    s = util::SockSend(fd, redis::Error(s));
    if (!s.IsOK()) {
      LOG(WARNING) << "Failed to send error response to socket: " << s.Msg();
    }
    conn->Close();
    return;
  }

  conn->SetAddr(srv->GetConfig()->unixsocket, 0);
  if (rate_limit_group_) {
    bufferevent_add_to_rate_limit_group(bev, rate_limit_group_);
  }
}

Status Worker::listenTCP(const std::string &host, uint32_t port, int backlog) {
  bool ipv6_used = strchr(host.data(), ':');

  addrinfo hints = {};
  hints.ai_family = ipv6_used ? AF_INET6 : AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  addrinfo *srv_info = nullptr;
  if (int rv = getaddrinfo(host.data(), std::to_string(port).c_str(), &hints, &srv_info); rv != 0) {
    return {Status::NotOK, gai_strerror(rv)};
  }
  auto exit = MakeScopeExit([srv_info] { freeaddrinfo(srv_info); });

  for (auto p = srv_info; p != nullptr; p = p->ai_next) {
    int fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd == -1) continue;

    int sock_opt = 1;
    if (ipv6_used && setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &sock_opt, sizeof(sock_opt)) == -1) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sock_opt, sizeof(sock_opt)) < 0) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    // to support multi-thread binding on macOS
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &sock_opt, sizeof(sock_opt)) < 0) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    if (bind(fd, p->ai_addr, p->ai_addrlen)) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    evutil_make_socket_nonblocking(fd);
    auto lev =
        NewEvconnlistener<&Worker::newTCPConnection>(base_, LEV_OPT_THREADSAFE | LEV_OPT_CLOSE_ON_FREE, backlog, fd);
    listen_events_.emplace_back(lev);
  }

  return Status::OK();
}

Status Worker::ListenUnixSocket(const std::string &path, int perm, int backlog) {
  unlink(path.c_str());
  sockaddr_un sa{};
  if (path.size() > sizeof(sa.sun_path) - 1) {
    return {Status::NotOK, "unix socket path too long"};
  }

  sa.sun_family = AF_LOCAL;
  strncpy(sa.sun_path, path.c_str(), sizeof(sa.sun_path) - 1);
  int fd = socket(AF_LOCAL, SOCK_STREAM, 0);
  if (fd == -1) {
    return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
  }

  if (bind(fd, (sockaddr *)&sa, sizeof(sa)) < 0) {
    return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
  }

  evutil_make_socket_nonblocking(fd);
  auto lev = NewEvconnlistener<&Worker::newUnixSocketConnection>(base_, LEV_OPT_CLOSE_ON_FREE, backlog, fd);
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
  is_terminated_ = true;
}

void Worker::Stop(uint32_t wait_seconds) {
  for (const auto &lev : listen_events_) {
    // It's unnecessary to close the listener fd since we have set the LEV_OPT_CLOSE_ON_FREE flag
    evconnlistener_free(lev);
  }
  // wait_seconds == 0 means stop immediately, or it will wait N seconds
  // for the worker to process the remaining requests before stopping.
  if (wait_seconds > 0) {
    timeval tv = {wait_seconds, 0};
    event_base_loopexit(base_, &tv);
  } else {
    event_base_loopbreak(base_);
  }
}

Status Worker::AddConnection(redis::Connection *c) {
  if (ConnMap::const_accessor accessor; conns_.find(accessor, c->GetFD())) {
    return {Status::NotOK, "connection was exists"};
  }

  int max_clients = srv->GetConfig()->maxclients;
  if (srv->IncrClientNum() >= max_clients) {
    srv->DecrClientNum();
    return {Status::NotOK, "max number of clients reached"};
  }

  ConnMap::accessor accessor;
  conns_.insert(accessor, std::make_pair(c->GetFD(), c));
  uint64_t id = srv->GetClientID();
  c->SetID(id);

  return Status::OK();
}

redis::Connection *Worker::removeConnection(int fd) {
  redis::Connection *conn = nullptr;

  if (ConnMap::accessor accessor; conns_.find(accessor, fd)) {
    {
      conn = accessor->second;
      conns_.erase(accessor);
    }
    srv->DecrClientNum();
  }

  if (ConnMap::accessor accessor; monitor_conns_.find(accessor, fd)) {
    conn = accessor->second;
    monitor_conns_.erase(accessor);
    srv->DecrClientNum();
    srv->DecrMonitorClientNum();
  }

  return conn;
}

// MigrateConnection moves the connection to another worker
// when reducing the number of workers.
//
// To make it simple, we would close the connection if it's
// blocked on a key or stream.
void Worker::MigrateConnection(Worker *target, redis::Connection *conn) {
  if (!target || !conn) return;

  auto bev = conn->GetBufferEvent();
  // disable read/write event to prevent the connection from being processed during migration
  bufferevent_disable(bev, EV_READ | EV_WRITE);
  // We cannot migrate the connection if it has a running command
  // since it will cause data race since the old worker may still process the command.
  if (!conn->CanMigrate()) {
    // Need to enable read/write event again since we disabled them before
    bufferevent_enable(bev, EV_READ | EV_WRITE);
    return;
  }

  // remove the connection from current worker
  DetachConnection(conn);
  if (!target->AddConnection(conn).IsOK()) {
    conn->Close();
    return;
  }
  bufferevent_base_set(target->base_, bev);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ | EV_WRITE);
  conn->SetOwner(target);
}

void Worker::DetachConnection(redis::Connection *conn) {
  if (!conn) return;

  removeConnection(conn->GetFD());

  if (rate_limit_group_) {
    bufferevent_remove_from_rate_limit_group(conn->GetBufferEvent());
  }

  auto bev = conn->GetBufferEvent();
  bufferevent_disable(bev, EV_READ | EV_WRITE);
  bufferevent_setcb(bev, nullptr, nullptr, nullptr, nullptr);
}

void Worker::FreeConnection(redis::Connection *conn) {
  if (!conn) return;

  removeConnection(conn->GetFD());
  srv->ResetWatchedKeys(conn);
  if (rate_limit_group_) {
    bufferevent_remove_from_rate_limit_group(conn->GetBufferEvent());
  }
  delete conn;
}

void Worker::FreeConnectionByID(int fd, uint64_t id) {
  if (ConnMap::accessor accessor; conns_.find(accessor, fd)) {
    if (rate_limit_group_ != nullptr) {
      bufferevent_remove_from_rate_limit_group(accessor->second->GetBufferEvent());
    }

    delete accessor->second;
    conns_.erase(accessor);

    srv->DecrClientNum();
  }
  if (ConnMap::accessor accessor; monitor_conns_.find(accessor, fd)) {
    delete accessor->second;
    monitor_conns_.erase(accessor);
    srv->DecrClientNum();
    srv->DecrMonitorClientNum();
  }
}

Status Worker::EnableWriteEvent(int fd) {
  if (ConnMap::const_accessor accessor; conns_.find(accessor, fd)) {
    auto bev = accessor->second->GetBufferEvent();
    bufferevent_enable(bev, EV_WRITE);
    return Status::OK();
  }

  return {Status::NotOK, "connection doesn't exist"};
}

Status Worker::Reply(int fd, const std::string &reply) {
  if (ConnMap::accessor accessor; conns_.find(accessor, fd)) {
    accessor->second->SetLastInteraction();
    redis::Reply(accessor->second->Output(), reply);
    return Status::OK();
  }

  return {Status::NotOK, "connection doesn't exist"};
}

void Worker::BecomeMonitorConn(redis::Connection *conn) {
  if (ConnMap::accessor accessor; conns_.find(accessor, conn->GetFD())) {
    conns_.erase(accessor);
    accessor.release();

    if (ConnMap::accessor accessor; monitor_conns_.find(accessor, conn->GetFD())) {
      accessor->second = conn;
    } else {
      monitor_conns_.insert(accessor, std::make_pair(conn->GetFD(), conn));
    }
  }
  srv->IncrMonitorClientNum();
  conn->EnableFlag(redis::Connection::kMonitor);
}

void Worker::QuitMonitorConn(redis::Connection *conn) {
  if (ConnMap::accessor accessor; monitor_conns_.find(accessor, conn->GetFD())) {
    {
      monitor_conns_.erase(accessor);
      accessor.release();
    }
    if (ConnMap::accessor accessor; conns_.find(accessor, conn->GetFD())) {
      accessor->second = conn;
    } else {
      conns_.insert(accessor, std::make_pair(conn->GetFD(), conn));
    }
  }
  srv->DecrMonitorClientNum();
  conn->DisableFlag(redis::Connection::kMonitor);
}

void Worker::FeedMonitorConns(redis::Connection *conn, const std::string &response) {
  tbb::parallel_for(monitor_conns_.range(), [conn, response](const ConnMap::range_type &range) {
    for (auto &it : range) {
      const auto &value = it.second;
      if (conn == value) continue;  // skip the monitor command
      if (conn->GetNamespace() == value->GetNamespace() || value->GetNamespace() == kDefaultNamespace) {
        value->Reply(response);
      }
    }
  });
}

std::string Worker::GetClientsStr() {
  return tbb::parallel_reduce(
      conns_.range(), std::string{},
      [](const ConnMap::range_type &range, std::string &&result) {
        for (auto &it : range) {
          result.append(it.second->ToString());
        }
        return result;
      },
      [](const std::string &lhs, const std::string &rhs) {
        std::string result = lhs;
        result.append(rhs);
        return result;
      });
}

void Worker::KillClient(redis::Connection *self, uint64_t id, const std::string &addr, uint64_t type, bool skipme,
                        int64_t *killed) {
  for (const auto key : getConnFds()) {
    if (ConnMap::accessor accessor; conns_.find(accessor, key)) {
      auto conn = accessor->second;
      if (skipme && self == conn) continue;

      // no need to kill the client again if the kCloseAfterReply flag is set
      if (conn->IsFlagEnabled(redis::Connection::kCloseAfterReply)) {
        continue;
      }
      if ((type & conn->GetClientType()) ||
          (!addr.empty() && (conn->GetAddr() == addr || conn->GetAnnounceAddr() == addr)) ||
          (id != 0 && conn->GetID() == id)) {
        conn->EnableFlag(redis::Connection::kCloseAfterReply);
        // enable write event to notify worker wake up ASAP, and remove the connection
        if (!conn->IsFlagEnabled(redis::Connection::kSlave)) {  // don't enable any event in slave connection
          auto bev = conn->GetBufferEvent();
          bufferevent_enable(bev, EV_WRITE);
        }
        (*killed)++;
      }
    }
  }
}

void Worker::KickoutIdleClients(int timeout) {
  std::vector<std::pair<int, uint64_t>> to_be_killed_conns;

  auto fd_list = getConnFds();
  if (fd_list.empty()) {
    return;
  }

  std::set<int> fds(fd_list.cbegin(), fd_list.cend());

  int iterations = std::min(static_cast<int>(conns_.size()), 50);
  auto iter = fds.upper_bound(last_iter_conn_fd_);
  while (iterations--) {
    if (iter == fds.end()) {
      iter = fds.begin();
    }
    if (ConnMap::const_accessor accessor;
        conns_.find(accessor, *iter) && static_cast<int>(accessor->second->GetIdleTime()) >= timeout) {
      to_be_killed_conns.emplace_back(accessor->first, accessor->second->GetID());
    }
    iter++;
  }
  iter--;
  last_iter_conn_fd_ = *iter;

  for (const auto &conn : to_be_killed_conns) {
    FreeConnectionByID(conn.first, conn.second);
  }
}

std::vector<int> Worker::getConnFds() const {
  return tbb::parallel_reduce(
      conns_.range(), std::vector<int>{},
      [](const ConnMap::const_range_type &range, std::vector<int> result) {
        for (const auto &fd : range) {
          result.emplace_back(fd.first);
        }
        return result;
      },
      [](const std::vector<int> &lhs, const std::vector<int> &rhs) {
        std::vector<int> result = lhs;
        result.insert(result.end(), rhs.begin(), rhs.end());
        return result;
      });
}

void WorkerThread::Start() {
  auto s = util::CreateThread("worker", [this] { this->worker_->Run(std::this_thread::get_id()); });

  if (s) {
    t_ = std::move(*s);
  } else {
    LOG(ERROR) << "[worker] Failed to start worker thread, err: " << s.Msg();
    return;
  }

  LOG(INFO) << "[worker] Thread #" << t_.get_id() << " started";
}

std::map<int, redis::Connection *> Worker::GetConnections() const {
  std::map<int, redis::Connection *> result;
  result = tbb::parallel_reduce(
      conns_.range(), result,
      [](const ConnMap::const_range_type &range, std::map<int, redis::Connection *> &&tmp_result) {
        // std::map<int, redis::Connection *> tmp_result;
        for (auto &it : range) {
          tmp_result.emplace(it.first, it.second);
        }
        return tmp_result;
      },
      [](const std::map<int, redis::Connection *> &lhs, const std::map<int, redis::Connection *> &rhs) {
        std::map<int, redis::Connection *> result = lhs;
        result.insert(rhs.cbegin(), rhs.cend());
        return result;
      });
  return result;
}

void WorkerThread::Stop(uint32_t wait_seconds) { worker_->Stop(wait_seconds); }

void WorkerThread::Join() {
  if (auto s = util::ThreadJoin(t_); !s) {
    LOG(WARNING) << "[worker] " << s.Msg();
  }
}
