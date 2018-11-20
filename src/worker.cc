#include <glog/logging.h>
#include <cctype>
#include <utility>

#include "redis_request.h"
#include "server.h"

Worker::Worker(Server *svr, Config *config) : svr_(svr), storage_(svr->storage_) {
  base_ = event_base_new();
  if (!base_) throw std::exception();
  sin_.sin_family = AF_INET;
  sin_.sin_addr.s_addr = htonl(0);
  sin_.sin_port = htons(config->port);
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  int sock_opt = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sock_opt, sizeof(sock_opt)) <
      0) {
    LOG(ERROR) << "Failed to set REUSEADDR: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    exit(1);
  }
  // to support multi-thread binding on macOS
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &sock_opt, sizeof(sock_opt)) <
      0) {
    LOG(ERROR) << "Failed to set REUSEPORT: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    exit(1);
  }
  if (bind(fd, (struct sockaddr *)&sin_, sizeof(sin_)) < 0) {
    LOG(ERROR) << "Failed to bind: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    exit(1);
  }
  evutil_make_socket_nonblocking(fd);
  auto lev = evconnlistener_new(base_, newConnection, this,
                                LEV_OPT_CLOSE_ON_FREE, config->backlog, fd);
  fd_ = evconnlistener_get_fd(lev);
  LOG(INFO) << "Listening on: " << fd_;
}

void Worker::newConnection(evconnlistener *listener, evutil_socket_t fd,
                           sockaddr *address, int socklen, void *ctx) {
  auto worker = static_cast<Worker *>(ctx);
  DLOG(INFO) << "new connection: fd=" << fd
             << " from port: " << ntohs(worker->sin_.sin_port) << " thread #"
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

void Worker::Run(std::thread::id tid) {
  tid_ = tid;
  if (event_base_dispatch(base_) != 0) LOG(ERROR) << "failed to run server";
}

void Worker::Stop() {
  event_base_loopbreak(base_);
  if (fd_ > 0) close(fd_);
}

Status Worker::AddConnection(Redis::Connection *c) {
  auto iter = conns_.find(c->GetFD());
  if (iter != conns_.end()) {
    // TODO: Connection exists
    return Status(Status::NotOK, "connection was exists");
  }
  Status status = svr_->IncrConnections();
  if (!status.IsOK()) return status;
  conns_.insert(std::pair<int, Redis::Connection*>(c->GetFD(), c));
  return Status::OK();
}

void Worker::RemoveConnection(int fd) {
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    // unscribe all channels if exists
    iter->second->UnSubscribeAll();
    delete iter->second;
    conns_.erase(fd);
    svr_->DecrConnections();
  }
}

void WorkerThread::Start() {
  t_ = std::thread([this]() { this->worker_->Run(t_.get_id()); });
  LOG(INFO) << "worker thread #" << t_.get_id() <<" started";
}

void WorkerThread::Stop() {
  worker_->Stop();
}

void WorkerThread::Join() {
  if (t_.joinable()) t_.join();
}