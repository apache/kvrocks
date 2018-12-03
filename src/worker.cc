#include <glog/logging.h>
#include <cctype>
#include <utility>

#include "redis_request.h"
#include "server.h"

Worker::Worker(Server *svr, Config *config) : svr_(svr), storage_(svr->storage_) {
  base_ = event_base_new();
  if (!base_) throw std::exception();
  for (const auto &host : config->binds) {
    Listen(host, config->port, config->backlog);
  }
}

void Worker::newConnection(evconnlistener *listener, evutil_socket_t fd,
                           sockaddr *address, int socklen, void *ctx) {
  auto worker = static_cast<Worker *>(ctx);
  DLOG(INFO) << "new connection: fd=" << fd
             << " from port: " << worker->svr_->GetConfig()->port << " thread #"
             << worker->tid_;
  event_base *base = evconnlistener_get_base(listener);
  // TODO: set tcp-keepliave
  bufferevent *bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
  auto conn = new Redis::Connection(bev, worker);
  bufferevent_setcb(bev, Redis::Connection::OnRead, nullptr,
                    Redis::Connection::OnEvent, conn);
  timeval tmo = {30, 0};  // TODO: timeout configs
  bufferevent_set_timeouts(bev, &tmo, &tmo);
  bufferevent_enable(bev, EV_READ);
  // TODO: set tcp-keepalive
  Status status = worker->AddConnection(conn);
  if (!status.IsOK()) {
    std::string err_msg = Redis::Error(status.msg());
    write(fd, err_msg.data(), err_msg.size());
    delete conn;
  }
}

Status Worker::Listen(const std::string &host, int port, int backlog) {
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
  evutil_socket_t listen_fd = evconnlistener_get_fd(lev);
  listen_fds_.emplace_back(listen_fd);
  LOG(INFO) << "Listening on: " << listen_fd;
  return Status::OK();
}

void Worker::Run(std::thread::id tid) {
  tid_ = tid;
  if (event_base_dispatch(base_) != 0) LOG(ERROR) << "failed to run server";
}

void Worker::Stop() {
  event_base_loopbreak(base_);
  for (const auto &fd : listen_fds_) {
    if (fd > 0) close(fd);
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
