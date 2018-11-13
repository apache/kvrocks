#include "server.h"
#include "worker.h"

#include <glog/logging.h>

Server::Server(Engine::Storage *storage, int port, int workers):
  storage_(storage),
  listen_port_(port){
  for (int i = 0; i < workers; i++) {
    auto worker = new Worker(this, static_cast<uint32_t>(port));
    workers_.emplace_back(new WorkerThread(worker));
  }
}

void Server::Start() {
  for (const auto worker : workers_) {
    worker->Start();
  }
}

void Server::Stop() {
  for (const auto worker : workers_) {
    worker->Stop();
  }
}

void Server::Join() {
  for (const auto worker : workers_) {
    worker->Join();
  }
}

Status Server::AddMaster(std::string host, uint32_t port) {
  // TODO: need mutex to avoid racing, so to make sure only one replication thread is running
  if (!master_host_.empty()) {
    LOG(INFO) << "Master already configured";
    return Status(Status::RedisReplicationConflict, "replication in progress");
  }
  master_host_ = std::move(host);
  master_port_ = port;
  replication_thread_ = std::unique_ptr<ReplicationThread>(
      new ReplicationThread(master_host_, master_port_, storage_));
  replication_thread_->Start([this]() { this->is_locked_ = true; },
                             [this]() { this->is_locked_ = false; });
  return Status::OK();
}

void Server::RemoveMaster() {
  if (master_host_.empty()) {
    master_host_.clear();
    master_port_ = 0;
    if (replication_thread_) replication_thread_->Stop();
  }
}

