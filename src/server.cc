#include <glog/logging.h>
#include <cctype>
#include <utility>

#include "redis_request.h"
#include "server.h"

Server::Server(Engine::Storage *storage, uint32_t port, std::vector<Server*> *all_servers) : storage_(storage) {
  all_servers_ = all_servers;
  base_ = event_base_new();
  if (!base_) throw std::exception();
  sin_.sin_family = AF_INET;
  sin_.sin_addr.s_addr = htonl(0);
  sin_.sin_port = htons(port);
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
  auto lev = evconnlistener_new(base_, NewConnection, this,
                                LEV_OPT_CLOSE_ON_FREE, -1, fd);
  fd_ = evconnlistener_get_fd(lev);
  LOG(INFO) << "Listening on: " << fd_;
}

void Server::NewConnection(evconnlistener *listener, evutil_socket_t fd,
                           sockaddr *address, int socklen, void *ctx) {
  auto svr = static_cast<Server *>(ctx);
  DLOG(INFO) << "new connection: fd=" << fd
             << " from port: " << ntohs(svr->sin_.sin_port) << " thread #"
             << svr->tid_;
  event_base *base = evconnlistener_get_base(listener);

  bufferevent *bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
  auto conn = new Redis::Connection(bev, svr);
  bufferevent_setcb(bev, Redis::Connection::OnRead, nullptr,
                    Redis::Connection::OnEvent, conn);
  timeval tmo = {30, 0};  // TODO: timeout configs
  bufferevent_set_timeouts(bev, &tmo, &tmo);
  bufferevent_enable(bev, EV_READ);
}

void Server::Run(std::thread::id tid) {
  tid_ = tid;
  if (event_base_dispatch(base_) != 0) LOG(ERROR) << "failed to run server";
}

void Server::Stop() {
  event_base_loopbreak(base_);
  if (fd_ > 0) close(fd_);
}

Status Server::AddMaster(std::string host, uint32_t port) {
  // TODO: need mutex to avoid racing, so to make sure only one replication thread is running
  if (is_slave_) {
    LOG(INFO) << "Master already configured";
    return Status(Status::RedisReplicationConflict, "replication in progress");
  }
  for (auto svr: *all_servers_) {
    svr->is_slave_ = true;
  }
  master_host_ = std::move(host);
  master_port_ = port;
  replication_thread_ = std::unique_ptr<ReplicationThread>(
      new ReplicationThread(master_host_, master_port_, storage_));
  replication_thread_->Start([this]() { this->LockDownAllServers(); },
                             [this]() { this->UnlockAllServers(); });
  return Status::OK();
}

void Server::RemoveMaster() {
  for (auto svr: *all_servers_) {
    if (svr->is_slave_) {
      master_host_ = "no one";
      master_port_ = 0;
      if (svr->replication_thread_) replication_thread_->Stop();
    }
  }
}

void ServerThread::Start() {
  t_ = std::thread([this]() { this->svr_->Run(t_.get_id()); });
  LOG(INFO) << "thread #" << t_.get_id();
}

void ServerThread::Join() {
  if (t_.joinable()) t_.join();
}
