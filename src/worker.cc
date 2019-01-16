#include <glog/logging.h>
#include <cctype>
#include <utility>
#include <algorithm>

#include "redis_request.h"
#include "server.h"
#include "sock_util.h"

Worker::Worker(Server *svr, Config *config, bool repl) : svr_(svr), repl_(repl) {
  base_ = event_base_new();
  if (!base_) throw std::exception();
  if (repl_) {
    for (const auto &host : config->repl_binds) {
      Status s = listen(host, config->repl_port, config->backlog);
      if (!s.IsOK()) {
        LOG(ERROR) << "Failed to listen the replication port "<< config->repl_port << ", err: " << s.Msg();
        exit(1);
      }
    }
  } else {
    for (const auto &host : config->binds) {
      Status s = listen(host, config->port, config->backlog);
      if (!s.IsOK()) {
        LOG(ERROR) << "Failed to listen port " << config->port << ", err: " << s.Msg();
        exit(1);
      }
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
    DLOG(INFO) << "new connection: fd=" << fd
               << " from port: " << worker->svr_->GetConfig()->repl_port << " thread #"
               << worker->tid_;
  } else {
    DLOG(INFO) << "new connection: fd=" << fd
               << " from port: " << worker->svr_->GetConfig()->port << " thread #"
               << worker->tid_;
  }
  int enable = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void*)&enable, sizeof(enable)) < 0) {
    LOG(ERROR) << "Failed to set tcp-keepalive, err:" << evutil_socket_geterror(fd);
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
  if (GetPeerAddr(fd, &host, &port)==0) {
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
  if (event_base_dispatch(base_) != 0) LOG(ERROR) << "failed to run server";
}

void Worker::Stop() {
  event_base_loopbreak(base_);
  for (const auto &lev: listen_events_) {
    evutil_socket_t fd = evconnlistener_get_fd(lev);
    if (fd > 0) close(fd);
    evconnlistener_free(lev);
  }
}

Status Worker::AddConnection(Redis::Connection *c) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(c->GetFD());
  if (iter != conns_.end()) {
    // TODO: Connection exists
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
      << " idle=" << c->GetIdle()
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

void Worker::KillClient(int64_t *killed, std::string addr, uint64_t id, bool skipme, Redis::Connection *conn) {
  std::list<std::pair<int, uint64_t>> clients;
  conns_mu_.lock();
  for (const auto iter : conns_) {
    Redis::Connection* c = iter.second;
    if (addr != "" && c->GetAddr() != addr) continue;
    if (id != 0 && c->GetID() != id) continue;
    if (skipme && conn == c) continue;
    clients.emplace_back(std::make_pair(c->GetFD(), c->GetID()));
  }
  conns_mu_.unlock();
  for (const auto iter : clients) {
    if (iter.first == conn->GetFD() && iter.second == conn->GetID()) {
      conn->AddFlag(Redis::Connection::kCloseAfterReply);
    } else {
      RemoveConnectionByID(iter.first, iter.second);
    }
    (*killed) ++;
  }
}

void Worker::KickoutIdleClients(int timeout) {
  conns_mu_.lock();

  std::list<std::pair<int, uint64_t>> clients;
  if (conns_.empty()) {
    conns_mu_.unlock();
    return;
  }
  int iterations = std::min(static_cast<int>(conns_.size()), 50);
  auto iter = conns_.upper_bound(last_iter_conn_fd);
  while (iterations--) {
    if (iter == conns_.end()) iter = conns_.begin();
    if (static_cast<int>(iter->second->GetIdle()) >= timeout) {
      clients.emplace_back(std::make_pair(iter->first, iter->second->GetID()));
    }
    iter++;
  }
  iter--;
  last_iter_conn_fd = iter->first;
  conns_mu_.unlock();

  for (const auto client : clients) {
    RemoveConnectionByID(client.first, client.second);
  }
}

void WorkerThread::Start() {
  try {
    t_ = std::thread([this]() { this->worker_->Run(t_.get_id()); });
  } catch (const std::system_error &e) {
    LOG(ERROR) << "Failed to start worker thread, err: " << e.what();
    return;
  }
  LOG(INFO) << "Worker thread #" << t_.get_id() << " started";
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
  worker_->KillClient(killed, addr, id, skipme, conn);
}

void WorkerThread::KickoutIdleClients(int timeout) {
  worker_->KickoutIdleClients(timeout);
}
