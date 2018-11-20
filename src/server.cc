#include "server.h"
#include "worker.h"
#include "redis_request.h"

#include <glog/logging.h>

Server::Server(Engine::Storage *storage, Config *config) :
  storage_(storage), config_(config) {
  for (int i = 0; i < config->workers; i++) {
    auto worker = new Worker(this, config);
    worker_threads_.emplace_back(new WorkerThread(worker));
  }
}

void Server::Start() {
  for (const auto worker : worker_threads_) {
    worker->Start();
  }
  // setup server cron thread
  cron_thread_ = std::thread([this]() { this->cron(); });
}

void Server::Stop() {
  for (const auto worker : worker_threads_) {
    worker->Stop();
  }
  cron_thread_.join();
}

void Server::Join() {
  for (const auto worker : worker_threads_) {
    worker->Join();
  }
  if (cron_thread_.joinable()) cron_thread_.join();
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
  if (!master_host_.empty()) {
    master_host_.clear();
    master_port_ = 0;
    if (replication_thread_) replication_thread_->Stop();
  }
}

int Server::PublishMessage(std::string &channel, std::string &msg) {
  int cnt = 0;

  auto iter = pubsub_channels_.find(channel);
  if (iter == pubsub_channels_.end()) {
    return 0;
  }
  std::string reply;
  reply.append(Redis::MultiLen(3));
  reply.append(Redis::BulkString("message"));
  reply.append(Redis::BulkString(channel));
  reply.append(Redis::BulkString(msg));
  for (const auto conn : iter->second) {
    Redis::Reply(conn->Output(), reply);
    cnt++;
  }
  return cnt;
}

void Server::SubscribeChannel(std::string &channel, Redis::Connection *conn) {
  auto iter = pubsub_channels_.find(channel);
  if (iter == pubsub_channels_.end()) {
    std::list<Redis::Connection*> conns;
    conns.emplace_back(conn);
    pubsub_channels_.insert(std::pair<std::string, std::list<Redis::Connection*>>(channel, conns));
  } else {
    iter->second.emplace_back(conn);
  }
}

void Server::UnSubscribeChannel(std::string &channel, Redis::Connection *conn) {
  auto iter = pubsub_channels_.find(channel);
  if (iter == pubsub_channels_.end()) {
    return;
  }
  for (const auto c: iter->second) {
    if (conn == c) {
      iter->second.remove(c);
      break;
    }
  }
}

Status Server::IncrConnections() {
  auto connections = connections_.fetch_add(1, std::memory_order_relaxed);
  if (config_->maxclients > 0 && connections >= config_->maxclients) {
    connections_.fetch_sub(1, std::memory_order_relaxed);
    return Status(Status::NotOK, "max number of clients reached");
  }
  return Status::OK();
}

void Server::DecrConnections() {
  connections_.fetch_sub(1, std::memory_order_relaxed);
}

void Server::clientsCron() {
  if (config_->timeout <= 0) return;
}

void Server::cron() {
  static uint64_t counter = 0;
  if (counter != 0 && counter % 10000) {
    clientsCron();
  }
  // wake up every millisecond
  counter++;
  std::this_thread::sleep_for(std::chrono::microseconds(1));
}
