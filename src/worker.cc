#include "worker.h"

#include <glog/logging.h>
#include <list>
#include <cctype>
#include <utility>
#include <algorithm>

#include "redis_request.h"
#include "server.h"
#include "util.h"

Worker::Worker(Server *svr, Config *config, bool repl) : svr_(svr), repl_(repl) {
  base_ = event_base_new();
  if (!base_) throw std::exception();

  int port = repl ? config->repl_port : config->port;
  auto binds = repl ? config->repl_binds : config->binds;
  for (const auto &bind : binds) {
    Status s = listen(bind, port, config->backlog);
    if (!s.IsOK()) {
      LOG(ERROR) << "[worker] Failed to listen on: "<< bind << ":" << port
                 << ", encounter error: " << s.Msg();
      exit(1);
    }
  }
}

Worker::~Worker() {
  std::list<Redis::Connection*> conns;
  for (const auto iter : conns_) {
    conns.emplace_back(iter.second);
  }

  for (auto iter : conns) {
    RemoveConnection(iter->GetFD());
  }
  event_base_free(base_);
}

void Worker::newConnection(evconnlistener *listener, evutil_socket_t fd,
                           sockaddr *address, int socklen, void *ctx) {
  auto worker = static_cast<Worker *>(ctx);
  if (worker->IsRepl()) {
    DLOG(INFO) << "[worker] New connection: fd=" << fd
               << " from port: " << worker->svr_->GetConfig()->repl_port << " thread #"
               << worker->tid_;
  } else {
    DLOG(INFO) << "[worker] New connection: fd=" << fd
               << " from port: " << worker->svr_->GetConfig()->port << " thread #"
               << worker->tid_;
  }
  int enable = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<void*>(&enable), sizeof(enable)) < 0) {
    LOG(ERROR) << "[worker] Failed to set tcp-keepalive, err:" << evutil_socket_geterror(fd);
    evutil_closesocket(fd);
    return;
  }
  event_base *base = evconnlistener_get_base(listener);
  bufferevent *bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
  auto conn = new Redis::Connection(bev, worker);
  bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite,
                    Redis::Connection::OnEvent, conn);
  bufferevent_enable(bev, EV_READ);
  Status status = worker->AddConnection(conn);
  std::string host;
  uint32_t port;
  if (Util::GetPeerAddr(fd, &host, &port) == 0) {
    conn->SetAddr(host+":"+std::to_string(port));
  }
  if (!status.IsOK()) {
    std::string err_msg = Redis::Error("ERR " + status.Msg());
    write(fd, err_msg.data(), err_msg.size());
    worker->RemoveConnection(conn->GetFD());
  }
}

Status Worker::listen(const std::string &host, int port, int backlog) {
  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  evutil_inet_pton(AF_INET, host.data(), &(sin.sin_addr));
  sin.sin_port = htons(port);
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  int sock_opt = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sock_opt, sizeof(sock_opt)) < 0) {
    return Status(Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
  }
  // to support multi-thread binding on macOS
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &sock_opt, sizeof(sock_opt)) < 0) {
    return Status(Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
  }
  if (bind(fd, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
    return Status(Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
  }
  evutil_make_socket_nonblocking(fd);
  auto lev = evconnlistener_new(base_, newConnection, this,
                                LEV_OPT_CLOSE_ON_FREE, backlog, fd);
  listen_events_.emplace_back(lev);
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
  Status status = svr_->IncrClients();
  if (!status.IsOK()) return status;
  conns_.insert(std::pair<int, Redis::Connection*>(c->GetFD(), c));
  uint64_t id = svr_->GetClientID()->fetch_add(1, std::memory_order_relaxed);
  c->SetID(id);
  return Status::OK();
}

void Worker::removeConnection(std::map<int, Redis::Connection *>::iterator iter) {
  // unscribe all channels if exists
  iter->second->UnSubscribeAll();
  delete iter->second;
  conns_.erase(iter);
  svr_->DecrClients();
}

void Worker::RemoveConnection(int fd) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end())
    removeConnection(iter);
}

void Worker::RemoveConnectionByID(int fd, uint64_t id) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end() && iter->second->GetID() == id)
    removeConnection(iter);
}

std::string Worker::GetClientsStr() {
  std::unique_lock<std::mutex> lock(conns_mu_);
  std::string clients;
  for (const auto iter : conns_) {
    int fd = iter.first;
    Redis::Connection *c = iter.second;
    std::ostringstream s;
    s << "id=" << c->GetID()
      << " addr=" << c->GetAddr()
      << " fd=" << fd
      << " name=" << c->GetName()
      << " age=" << c->GetAge()
      << " idle=" << c->GetIdleTime()
      << " flags="
      << " namespace=" << c->GetNamespace()
      << " qbuf=" << evbuffer_get_length(c->Input())
      << " obuf=" << evbuffer_get_length(c->Output())
      << " cmd=" << c->GetLastCmd()
      << "\n";
    clients.append(s.str());
  }
  return clients;
}

void Worker::KillClient(Redis::Connection *self, uint64_t id, std::string addr, bool skipme, int64_t *killed) {
  std::list<std::pair<int, uint64_t>> to_be_killed_conns;

  conns_mu_.lock();
  for (const auto iter : conns_) {
    Redis::Connection* conn = iter.second;
    if (skipme && self == conn) continue;
    if (!addr.empty() && conn->GetAddr() == addr) {
      to_be_killed_conns.emplace_back(std::make_pair(conn->GetFD(), conn->GetID()));
    } else if (id != 0 && conn->GetID() == id) {
      to_be_killed_conns.emplace_back(std::make_pair(conn->GetFD(), conn->GetID()));
    }
  }
  conns_mu_.unlock();

  for (const auto iter : to_be_killed_conns) {
    if (iter.first == self->GetFD() && iter.second == self->GetID()) {
      self->SetFlag(Redis::Connection::kCloseAfterReply);
    } else {
      RemoveConnectionByID(iter.first, iter.second);
    }
    (*killed)++;
  }
}

void Worker::KickoutIdleClients(int timeout) {
  conns_mu_.lock();
  std::list<std::pair<int, uint64_t>> to_be_killed_conns;
  if (conns_.empty()) {
    conns_mu_.unlock();
    return;
  }
  int iterations = std::min(static_cast<int>(conns_.size()), 50);
  auto iter = conns_.upper_bound(last_iter_conn_fd);
  while (iterations--) {
    if (iter == conns_.end()) iter = conns_.begin();
    if (static_cast<int>(iter->second->GetIdleTime()) >= timeout) {
      to_be_killed_conns.emplace_back(std::make_pair(iter->first, iter->second->GetID()));
    }
    iter++;
  }
  iter--;
  last_iter_conn_fd = iter->first;
  conns_mu_.unlock();

  for (const auto conn : to_be_killed_conns) {
    RemoveConnectionByID(conn.first, conn.second);
  }
}

void WorkerThread::Start() {
  try {
    t_ = std::thread([this]() {
      if (this->worker_->IsRepl()) {
        Util::ThreadSetName("repl-worker");
      } else {
        Util::ThreadSetName("worker");
      }
      this->worker_->Run(t_.get_id());
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

std::string WorkerThread::GetClientsStr() {
  return worker_->GetClientsStr();
}

void WorkerThread::KillClient(int64_t *killed, std::string addr, uint64_t id, bool skipme, Redis::Connection *conn) {
  worker_->KillClient(conn, id, addr, skipme, killed);
}

void WorkerThread::KickoutIdleClients(int timeout) {
  worker_->KickoutIdleClients(timeout);
}
