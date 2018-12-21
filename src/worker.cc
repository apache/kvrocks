#include <glog/logging.h>
#include <cctype>
#include <utility>

#include "redis_request.h"
#include "server.h"

Worker::Worker(Server *svr, Config *config) : svr_(svr){
  base_ = event_base_new();
  if (!base_) throw std::exception();
  for (const auto &host : config->binds) {
    listen(host, config->port, config->backlog);
  }
}

Worker::~Worker() {
  for (const auto &conn_iter:conns_) {
    delete conn_iter.second;
  }
  event_base_free(base_);
}

void Worker::newConnection(evconnlistener *listener, evutil_socket_t fd,
                           sockaddr *address, int socklen, void *ctx) {
  auto worker = static_cast<Worker *>(ctx);
  DLOG(INFO) << "new connection: fd=" << fd
             << " from port: " << worker->svr_->GetConfig()->port << " thread #"
             << worker->tid_;
  int enable = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void*)&enable, sizeof(enable)) < 0) {
    LOG(ERROR) << "Failed to set tcp-keepalive, err:" << evutil_socket_geterror(fd);
    evutil_closesocket(fd);
    return;
  }
  event_base *base = evconnlistener_get_base(listener);
  bufferevent *bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
  auto conn = new Redis::Connection(bev, worker);
  bufferevent_setcb(bev, Redis::Connection::OnRead, nullptr,
                    Redis::Connection::OnEvent, conn);
  timeval tmo = {30, 0};  // TODO: timeout configs
  bufferevent_set_timeouts(bev, &tmo, &tmo);
  bufferevent_enable(bev, EV_READ);
  Status status = worker->AddConnection(conn);
  if (!status.IsOK()) {
    std::string err_msg = Redis::Error(status.Msg());
    write(fd, err_msg.data(), err_msg.size());
    delete conn;
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
  LOG(INFO) << "Listening on: " << evconnlistener_get_fd(lev);
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
  auto iter = conns_.find(c->GetFD());
  if (iter != conns_.end()) {
    // TODO: Connection exists
    return Status(Status::NotOK, "connection was exists");
  }
  Status status = svr_->IncrClients();
  if (!status.IsOK()) return status;
  conns_.insert(std::pair<int, Redis::Connection*>(c->GetFD(), c));
  return Status::OK();
}

void Worker::RemoveConnection(int fd) {
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    // unscribe all channels if exists
    iter->second->UnSubscribeAll();
    conns_.erase(fd);
    svr_->DecrClients();
  }
}

void WorkerThread::Start() {
  try {
    t_ = std::thread([this]() { this->worker_->Run(t_.get_id()); });
  } catch (const std::system_error &e) {
    LOG(ERROR) << "Failed to start worker thread";
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
