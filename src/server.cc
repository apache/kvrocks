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

#include "server.h"

#include <fcntl.h>
#include <sys/statvfs.h>
#include <sys/utsname.h>
#include <sys/resource.h>
#include <memory>
#include <utility>
#include <glog/logging.h>
#include <rocksdb/convenience.h>

#include <tls_util.h>
#include "util.h"
#include "worker.h"
#include "version.h"
#include "redis_db.h"
#include "redis_request.h"
#include "redis_connection.h"
#include "compaction_checker.h"
#include "config.h"
#include "scripting.h"

std::atomic<int>Server::unix_time_ = {0};

Server::Server(Engine::Storage *storage, Config *config) :
  storage_(storage), config_(config) {
  // init commands stats here to prevent concurrent insert, and cause core
  auto commands = Redis::GetOriginalCommands();
  for (const auto &iter : *commands) {
    stats_.commands_stats[iter.first].calls = 0;
    stats_.commands_stats[iter.first].latency = 0;
  }

#ifdef ENABLE_OPENSSL
  // init ssl context
  if (config->tls_port) {
    ssl_ctx_ = CreateSSLContext(config);
    if (!ssl_ctx_) {
      exit(1);
    }
  }
#endif

  // Init cluster
  cluster_ = Util::MakeUnique<Cluster>(this, config_->binds, config_->port);

  for (int i = 0; i < config->workers; i++) {
    auto worker = Util::MakeUnique<Worker>(this, config);
    // multiple workers can't listen to the same unix socket, so
    // listen unix socket only from a single worker - the first one
    if (!config->unixsocket.empty() && i == 0) {
      Status s = worker->ListenUnixSocket(config->unixsocket, config->unixsocketperm, config->backlog);
      if (!s.IsOK()) {
        LOG(ERROR) << "[server] Failed to listen on unix socket: " << config->unixsocket
                   << ", encounter error: " << s.Msg();
        exit(1);
      }
      LOG(INFO) << "[server] Listening on unix socket: " << config->unixsocket;
    }
    worker_threads_.emplace_back(Util::MakeUnique<WorkerThread>(std::move(worker)));
  }
  AdjustOpenFilesLimit();
  slow_log_.SetMaxEntries(config->slowlog_max_len);
  perf_log_.SetMaxEntries(config->profiling_sample_record_max_len);
  lua_ = Lua::CreateState();
  fetch_file_threads_num_ = 0;
  time(&start_time_);
  stop_ = false;
  is_loading_ = false;
}

Server::~Server() {
  for (const auto &iter : conn_ctxs_) {
    delete iter.first;
  }

  // Wait for all fetch file threads stop and exit and force destroy
  // the server after 60s.
  int counter = 0;
  while (GetFetchFileThreadNum() != 0) {
    usleep(100000);
    if (++counter == 600) {
      LOG(WARNING) << "Will force destroy the server after waiting 60s, leave " << GetFetchFileThreadNum()
                   << " fetch file threads are still running";
      break;
    }
  }
  Lua::DestroyState(lua_);
}

// Kvrocks threads list:
// - Work-thread: process client's connections and requests
// - Task-runner: one thread pool, handle some jobs that may freeze server if run directly
// - Cron-thread: server's crontab, clean backups, resize sst and memtable size
// - Compaction-checker: active compaction according to collected statistics
// - Replication-thread: replicate incremental stream from master if in slave role, there
//   are some dynamic threads to fetch files when full sync.
//     - fetch-file-thread: fetch SST files from master
// - Feed-slave-thread: feed data to slaves if having slaves, but there also are some dynamic
//   threads when full sync, TODO(@shooterit) we should manage this threads uniformly.
//     - feed-replica-data-info: generate checkpoint and send files list when full sync
//     - feed-replica-file: send SST files when slaves ask for full sync
Status Server::Start() {
  if (!config_->master_host.empty()) {
    Status s = AddMaster(config_->master_host, static_cast<uint32_t>(config_->master_port), false);
    if (!s.IsOK()) return s;
  } else {
    // Generate new replication id if not a replica
    storage_->ShiftReplId();
  }

  if (config_->cluster_enabled) {
    // Create objects used for slot migration
    slot_migrate_ = Util::MakeUnique<SlotMigrate>(this, config_->migrate_speed,
                                    config_->pipeline_size, config_->sequence_gap);
    slot_import_ = new SlotImport(this);
    // Create migrating thread
    auto s = slot_migrate_->CreateMigrateHandleThread();
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed to create migration thread, Err: " << s.Msg();
      return Status(Status::NotOK);
    }
  }

  for (const auto &worker : worker_threads_) {
    worker->Start();
  }
  task_runner_.Start();
  // setup server cron thread
  cron_thread_ = std::thread([this]() {
    Util::ThreadSetName("server-cron");
    this->cron();
  });

  compaction_checker_thread_ = std::thread([this]() {
    uint64_t counter = 0;
    int32_t last_compact_date = 0;
    Util::ThreadSetName("compact-check");
    CompactionChecker compaction_checker(this->storage_);
    while (!stop_) {
      // Sleep first
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      // To guarantee accessing DB safely
      auto guard = storage_->ReadLockGuard();
      if (storage_->IsClosing()) continue;

      if (is_loading_ == false && ++counter % 600 == 0  // check every minute
          && config_->compaction_checker_range.Enabled()) {
        auto now = std::time(nullptr);
        std::tm local_time{};
        localtime_r(&now, &local_time);
        if (local_time.tm_hour >= config_->compaction_checker_range.Start
        && local_time.tm_hour <= config_->compaction_checker_range.Stop) {
          std::vector<std::string> cf_names = {Engine::kMetadataColumnFamilyName,
                                               Engine::kSubkeyColumnFamilyName,
                                               Engine::kZSetScoreColumnFamilyName,
                                               Engine::kStreamColumnFamilyName};
          for (const auto &cf_name : cf_names) {
            compaction_checker.PickCompactionFiles(cf_name);
          }
        }
        // compact once per day
        if (now != 0 && last_compact_date != now/86400) {
          last_compact_date = now/86400;
          compaction_checker.CompactPropagateAndPubSubFiles();
        }
      }
    }
  });

  LOG(INFO) << "Ready to accept connections";

  return Status::OK();
}

void Server::Stop() {
  stop_ = true;
  if (replication_thread_) replication_thread_->Stop();
  for (const auto &worker : worker_threads_) {
    worker->Stop();
  }
  DisconnectSlaves();
  rocksdb::CancelAllBackgroundWork(storage_->GetDB(), true);
  task_runner_.Stop();
}

void Server::Join() {
  for (const auto &worker : worker_threads_) {
    worker->Join();
  }
  task_runner_.Join();
  if (cron_thread_.joinable()) cron_thread_.join();
  if (compaction_checker_thread_.joinable()) compaction_checker_thread_.join();
}

Status Server::AddMaster(std::string host, uint32_t port, bool force_reconnect) {
  std::lock_guard<std::mutex> guard(slaveof_mu_);

  // Don't check host and port if 'force_reconnect' argument is set to true
  if (!force_reconnect &&
      !master_host_.empty() && master_host_ == host &&
      master_port_ == port) {
    return Status::OK();
  }

  // Master is changed
  if (!master_host_.empty()) {
    if (replication_thread_) replication_thread_->Stop();
    replication_thread_ = nullptr;
  }

  // For master using old version, it uses replication thread to implement
  // replication, and uses 'listen-port + 1' as thread listening port.
  uint32_t master_listen_port = port;
  if (GetConfig()->master_use_repl_port)  master_listen_port += 1;
  replication_thread_ = std::unique_ptr<ReplicationThread>(
      new ReplicationThread(host, master_listen_port, this));
  auto s = replication_thread_->Start(
      [this]() {
        PrepareRestoreDB();
      },
      [this]() {
        this->is_loading_ = false;
        task_runner_.Start();
      });
  if (s.IsOK()) {
    master_host_ = host;
    master_port_ = port;
    config_->SetMaster(host, port);
  } else {
    replication_thread_ = nullptr;
  }
  return s;
}

Status Server::RemoveMaster() {
  std::lock_guard<std::mutex> guard(slaveof_mu_);
  if (!master_host_.empty()) {
    master_host_.clear();
    master_port_ = 0;
    config_->ClearMaster();
    if (replication_thread_) replication_thread_->Stop();
    replication_thread_ = nullptr;
    storage_->ShiftReplId();
  }
  return Status::OK();
}

Status Server::AddSlave(Redis::Connection *conn, rocksdb::SequenceNumber next_repl_seq) {
  auto t = new FeedSlaveThread(this, conn, next_repl_seq);
  auto s = t->Start();
  if (!s.IsOK()) {
    delete t;
    return s;
  }

  std::lock_guard<std::mutex> lg(slave_threads_mu_);
  slave_threads_.emplace_back(t);
  return Status::OK();
}

void Server::DisconnectSlaves() {
  std::lock_guard<std::mutex> guard(slaveof_mu_);
  for (const auto &slave_thread : slave_threads_) {
    if (!slave_thread->IsStopped()) slave_thread->Stop();
  }
  while (!slave_threads_.empty()) {
    auto slave_thread = slave_threads_.front();
    slave_threads_.pop_front();
    slave_thread->Join();
    delete slave_thread;
  }
}

void Server::cleanupExitedSlaves() {
  std::list<FeedSlaveThread *> exited_slave_threads;
  std::lock_guard<std::mutex> guard(slaveof_mu_);
  for (const auto &slave_thread : slave_threads_) {
    if (slave_thread->IsStopped())
      exited_slave_threads.emplace_back(slave_thread);
  }
  while (!exited_slave_threads.empty()) {
    auto t = exited_slave_threads.front();
    exited_slave_threads.pop_front();
    slave_threads_.remove(t);
    t->Join();
    delete t;
  }
}

void Server::FeedMonitorConns(Redis::Connection *conn, const std::vector<std::string> &tokens) {
  if (monitor_clients_ <= 0) return;
  for (const auto &worker_thread : worker_threads_) {
    auto worker = worker_thread->GetWorker();
    worker->FeedMonitorConns(conn, tokens);
  }
}

int Server::PublishMessage(const std::string &channel, const std::string &msg) {
  int cnt = 0;
  int index = 0;

  pubsub_channels_mu_.lock();
  std::vector<ConnContext> to_publish_conn_ctxs;
  auto iter = pubsub_channels_.find(channel);
  if (iter != pubsub_channels_.end()) {
    for (const auto &conn_ctx : iter->second) {
      to_publish_conn_ctxs.emplace_back(*conn_ctx);
    }
  }

  // The patterns variable records the pattern of connections
  std::vector<std::string> patterns;
  std::vector<ConnContext> to_publish_patterns_conn_ctxs;
  for (const auto &iter : pubsub_patterns_) {
    if (Util::StringMatch(iter.first, channel, 0)) {
      for (const auto &conn_ctx : iter.second) {
        to_publish_patterns_conn_ctxs.emplace_back(*conn_ctx);
        patterns.emplace_back(iter.first);
      }
    }
  }
  pubsub_channels_mu_.unlock();

  std::string channel_reply;
  channel_reply.append(Redis::MultiLen(3));
  channel_reply.append(Redis::BulkString("message"));
  channel_reply.append(Redis::BulkString(channel));
  channel_reply.append(Redis::BulkString(msg));
  for (const auto &conn_ctx : to_publish_conn_ctxs) {
    auto s = conn_ctx.owner->Reply(conn_ctx.fd, channel_reply);
    if (s.IsOK()) {
      cnt++;
    }
  }

  // We should publish corresponding pattern and message for connections
  for (const auto &conn_ctx : to_publish_patterns_conn_ctxs) {
    std::string pattern_reply;
    pattern_reply.append(Redis::MultiLen(4));
    pattern_reply.append(Redis::BulkString("pmessage"));
    pattern_reply.append(Redis::BulkString(patterns[index++]));
    pattern_reply.append(Redis::BulkString(channel));
    pattern_reply.append(Redis::BulkString(msg));
    auto s = conn_ctx.owner->Reply(conn_ctx.fd, pattern_reply);
    if (s.IsOK()) {
      cnt++;
    }
  }
  return cnt;
}

void Server::SubscribeChannel(const std::string &channel, Redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);
  auto conn_ctx = new ConnContext(conn->Owner(), conn->GetFD());
  conn_ctxs_[conn_ctx] = true;
  auto iter = pubsub_channels_.find(channel);
  if (iter == pubsub_channels_.end()) {
    std::list<ConnContext *> conn_ctxs;
    conn_ctxs.emplace_back(conn_ctx);
    pubsub_channels_.insert(std::pair<std::string, std::list<ConnContext *>>(channel, conn_ctxs));
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::UnSubscribeChannel(const std::string &channel, Redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);
  auto iter = pubsub_channels_.find(channel);
  if (iter == pubsub_channels_.end()) {
    return;
  }
  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx->fd && conn->Owner() == conn_ctx->owner) {
      delConnContext(conn_ctx);
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        pubsub_channels_.erase(iter);
      }
      break;
    }
  }
}

void Server::GetChannelsByPattern(const std::string &pattern, std::vector<std::string> *channels) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);
  for (const auto &iter : pubsub_channels_) {
    if (pattern.empty() || Util::StringMatch(pattern, iter.first, 0)) {
      channels->emplace_back(iter.first);
    }
  }
}

void Server::ListChannelSubscribeNum(std::vector<std::string> channels,
                                     std::vector<ChannelSubscribeNum> *channel_subscribe_nums) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);
  for (const auto &chan : channels) {
    auto iter = pubsub_channels_.find(chan);
    if (iter != pubsub_channels_.end()) {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{iter->first, iter->second.size()});
    } else {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{chan, 0});
    }
  }
}

void Server::PSubscribeChannel(const std::string &pattern, Redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);
  auto conn_ctx = new ConnContext(conn->Owner(), conn->GetFD());
  conn_ctxs_[conn_ctx] = true;
  auto iter = pubsub_patterns_.find(pattern);
  if (iter == pubsub_patterns_.end()) {
    std::list<ConnContext *> conn_ctxs;
    conn_ctxs.emplace_back(conn_ctx);
    pubsub_patterns_.insert(std::pair<std::string, std::list<ConnContext *>>(pattern, conn_ctxs));
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::PUnSubscribeChannel(const std::string &pattern, Redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);
  auto iter = pubsub_patterns_.find(pattern);
  if (iter == pubsub_patterns_.end()) {
    return;
  }
  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx->fd && conn->Owner() == conn_ctx->owner) {
      delConnContext(conn_ctx);
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        pubsub_patterns_.erase(iter);
      }
      break;
    }
  }
}

void Server::AddBlockingKey(const std::string &key, Redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);
  auto iter = blocking_keys_.find(key);
  auto conn_ctx = new ConnContext(conn->Owner(), conn->GetFD());
  conn_ctxs_[conn_ctx] = true;
  if (iter == blocking_keys_.end()) {
    std::list<ConnContext *> conn_ctxs;
    conn_ctxs.emplace_back(conn_ctx);
    blocking_keys_.insert(std::pair<std::string, std::list<ConnContext *>>(key, conn_ctxs));
  } else {
    iter->second.emplace_back(conn_ctx);
  }
  IncrBlockedClientNum();
}

void Server::UnBlockingKey(const std::string &key, Redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);
  auto iter = blocking_keys_.find(key);
  if (iter == blocking_keys_.end()) {
    return;
  }
  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx->fd && conn->Owner() == conn_ctx->owner) {
      delConnContext(conn_ctx);
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        blocking_keys_.erase(iter);
      }
      break;
    }
  }
  DecrBlockedClientNum();
}

void Server::BlockOnStreams(const std::vector<std::string> &keys,
                            const std::vector<Redis::StreamEntryID> &entry_ids, Redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);
  IncrBlockedClientNum();
  for (size_t i = 0; i < keys.size(); ++i) {
    auto consumer = std::make_shared<StreamConsumer>(
          conn->Owner(), conn->GetFD(), conn->GetNamespace(), entry_ids[i]);
    auto iter = blocked_stream_consumers_.find(keys[i]);
    if (iter == blocked_stream_consumers_.end()) {
      std::set<std::shared_ptr<StreamConsumer>> consumers;
      consumers.insert(consumer);
      blocked_stream_consumers_.insert(std::make_pair(keys[i], consumers));
    } else {
      iter->second.insert(consumer);
    }
  }
}

void Server::UnblockOnStreams(const std::vector<std::string> &keys, Redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);
  DecrBlockedClientNum();
  for (const auto &key : keys) {
    auto iter = blocked_stream_consumers_.find(key);
    if (iter == blocked_stream_consumers_.end()) {
      continue;
    }

    for (auto it = iter->second.begin(); it != iter->second.end(); ) {
      auto consumer = *it;
      if (conn->GetFD() == consumer->fd && conn->Owner() == consumer->owner) {
        iter->second.erase(it);
        if (iter->second.empty()) {
          blocked_stream_consumers_.erase(iter);
        }
        break;
      }
    }
  }
}

Status Server::WakeupBlockingConns(const std::string &key, size_t n_conns) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);
  auto iter = blocking_keys_.find(key);
  if (iter == blocking_keys_.end() || iter->second.empty()) {
    return Status(Status::NotOK);
  }
  while (n_conns-- && !iter->second.empty()) {
    auto conn_ctx = iter->second.front();
    conn_ctx->owner->EnableWriteEvent(conn_ctx->fd);
    delConnContext(conn_ctx);
    iter->second.pop_front();
  }
  return Status::OK();
}

Status Server::OnEntryAddedToStream(const std::string &ns, const std::string &key,
                                    const Redis::StreamEntryID &entry_id) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);
  auto iter = blocked_stream_consumers_.find(key);
  if (iter == blocked_stream_consumers_.end() || iter->second.empty()) {
    return Status(Status::NotOK);
  }

  for (auto it = iter->second.begin(); it != iter->second.end(); ) {
    auto consumer = *it;
    if (consumer->ns == ns && entry_id > consumer->last_consumed_id) {
      consumer->owner->EnableWriteEvent(consumer->fd);
      it = iter->second.erase(it);
    } else {
      ++it;
    }
  }

  return Status::OK();
}

void Server::delConnContext(ConnContext *c) {
  auto conn_ctx_iter = conn_ctxs_.find(c);
  if (conn_ctx_iter != conn_ctxs_.end()) {
    delete conn_ctx_iter->first;
    conn_ctxs_.erase(conn_ctx_iter);
  }
}

void Server::updateCachedTime() {
  time_t ret = time(nullptr);
  if (ret == -1) return;
  unix_time_.store(static_cast<int>(ret));
}

int Server::IncrClientNum() {
  total_clients_.fetch_add(1, std::memory_order::memory_order_relaxed);
  return connected_clients_.fetch_add(1, std::memory_order_relaxed);
}

int Server::DecrClientNum() {
  return connected_clients_.fetch_sub(1, std::memory_order_relaxed);
}

int Server::IncrMonitorClientNum() {
  return monitor_clients_.fetch_add(1, std::memory_order_relaxed);
}

int Server::DecrMonitorClientNum() {
  return monitor_clients_.fetch_sub(1, std::memory_order_relaxed);
}

int Server::IncrBlockedClientNum() {
  return blocked_clients_.fetch_add(1, std::memory_order_relaxed);
}

int Server::DecrBlockedClientNum() {
  return blocked_clients_.fetch_sub(1, std::memory_order_relaxed);
}

std::unique_ptr<RWLock::ReadLock> Server::WorkConcurrencyGuard() {
  return Util::MakeUnique<RWLock::ReadLock>(works_concurrency_rw_lock_);
}

std::unique_ptr<RWLock::WriteLock> Server::WorkExclusivityGuard() {
  return Util::MakeUnique<RWLock::WriteLock>(works_concurrency_rw_lock_);
}

std::atomic<uint64_t> *Server::GetClientID() {
  return &client_id_;
}

void Server::recordInstantaneousMetrics() {
  auto rocksdb_stats = storage_->GetDB()->GetDBOptions().statistics;
  stats_.TrackInstantaneousMetric(STATS_METRIC_COMMAND, stats_.total_calls);
  stats_.TrackInstantaneousMetric(STATS_METRIC_NET_INPUT, stats_.in_bytes);
  stats_.TrackInstantaneousMetric(STATS_METRIC_NET_OUTPUT, stats_.out_bytes);
  stats_.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_PUT,
    rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_WRITTEN));
  stats_.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_GET,
    rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_READ));
  stats_.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_MULTIGET,
    rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_MULTIGET_KEYS_READ));
  stats_.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_SEEK,
    rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_SEEK));
  stats_.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_NEXT,
    rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_NEXT));
  stats_.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_PREV,
    rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_PREV));
}

void Server::cron() {
  uint64_t counter = 0;
  while (!stop_) {
    // Sleep first
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // To guarantee accessing DB safely
    auto guard = storage_->ReadLockGuard();
    if (storage_->IsClosing()) continue;

    updateCachedTime();
    counter++;
    if (is_loading_) {
      // We need to skip the cron operations since `is_loading_` means the db is restoring,
      // and the db pointer will be modified after that. It will panic if we use the db pointer
      // before the new db was reopened.
      continue;
    }
    // check every 20s (use 20s instead of 60s so that cron will execute in critical condition)
    if (counter != 0 && counter % 200 == 0) {
      auto t = std::time(nullptr);
      std::tm now{};
      localtime_r(&t, &now);
      // disable compaction cron when the compaction checker was enabled
      if (!config_->compaction_checker_range.Enabled()
          && config_->compact_cron.IsEnabled()
          && config_->compact_cron.IsTimeMatch(&now)) {
        Status s = AsyncCompactDB();
        LOG(INFO) << "[server] Schedule to compact the db, result: " << s.Msg();
      }
      if (config_->bgsave_cron.IsEnabled() && config_->bgsave_cron.IsTimeMatch(&now)) {
        Status s = AsyncBgsaveDB();
        LOG(INFO) << "[server] Schedule to bgsave the db, result: " << s.Msg();
      }
    }
    // check every 10s
    if (counter != 0 && counter % 100 == 0) {
      Status s = AsyncPurgeOldBackups(config_->max_backup_to_keep, config_->max_backup_keep_hours);

      // Purge backup if needed, it will cost much disk space if we keep backup and full sync
      // checkpoints at the same time
      if (config_->purge_backup_on_fullsync &&
          (storage_->ExistCheckpoint() || storage_->ExistSyncCheckpoint())) {
        AsyncPurgeOldBackups(0, 0);
      }
    }

    // No replica uses this checkpoint, we can remove it.
    if (counter != 0 && counter % 100 == 0) {
      time_t create_time = storage_->GetCheckpointCreateTime();
      time_t access_time = storage_->GetCheckpointAccessTime();

      if (storage_->ExistCheckpoint()) {
        // TODO(shooterit): support to config the alive time of checkpoint
        if ((GetFetchFileThreadNum() == 0 && std::time(nullptr) - access_time > 30) ||
            (std::time(nullptr) - create_time > 24 * 60 * 60)) {
          auto s = rocksdb::DestroyDB(config_->checkpoint_dir, rocksdb::Options());
          if (!s.ok()) {
            LOG(WARNING) << "[server] Fail to clean checkpoint, error: " << s.ToString();
          } else {
            LOG(INFO) << "[server] Clean checkpoint successfully";
          }
        }
      }
    }
    // check if DB need to be resumed every minute
    // Rocksdb has auto resume feature after retryable io error, earlier version(before v6.22.1) had
    // bug when encounter no space error. The current version fixes the no space error issue, but it
    // does not completely resolve, which still exists when encountered disk quota exceeded error.
    // In order to properly handle all possible situations on rocksdb, we manually resume here
    // when encountering no space error and disk quota exceeded error.
    if (counter != 0 && counter % 600 == 0 && storage_->IsDBInRetryableIOError()) {
      storage_->GetDB()->Resume();
      LOG(INFO) << "[server] Schedule to resume DB after retryable io error";
      storage_->SetDBInRetryableIOError(false);
    }

    cleanupExitedSlaves();
    recordInstantaneousMetrics();
  }
}

void Server::GetRocksDBInfo(std::string *info) {
  std::ostringstream string_stream;
  rocksdb::DB *db = storage_->GetDB();

  uint64_t memtable_sizes, cur_memtable_sizes, num_snapshots, num_running_flushes;
  uint64_t num_immutable_tables, memtable_flush_pending, compaction_pending;
  uint64_t num_running_compaction, num_live_versions, num_superversion, num_backgroud_errors;

  db->GetAggregatedIntProperty("rocksdb.num-snapshots", &num_snapshots);
  db->GetAggregatedIntProperty("rocksdb.size-all-mem-tables", &memtable_sizes);
  db->GetAggregatedIntProperty("rocksdb.cur-size-all-mem-tables", &cur_memtable_sizes);
  db->GetAggregatedIntProperty("rocksdb.num-running-flushes", &num_running_flushes);
  db->GetAggregatedIntProperty("rocksdb.num-immutable-mem-table", &num_immutable_tables);
  db->GetAggregatedIntProperty("rocksdb.mem-table-flush-pending", &memtable_flush_pending);
  db->GetAggregatedIntProperty("rocksdb.num-running-compactions", &num_running_compaction);
  db->GetAggregatedIntProperty("rocksdb.current-super-version-number", &num_superversion);
  db->GetAggregatedIntProperty("rocksdb.background-errors", &num_backgroud_errors);
  db->GetAggregatedIntProperty("rocksdb.compaction-pending", &compaction_pending);
  db->GetAggregatedIntProperty("rocksdb.num-live-versions", &num_live_versions);

  string_stream << "# RocksDB\r\n";
  for (const auto &cf_handle : *storage_->GetCFHandles()) {
    uint64_t estimate_keys, block_cache_usage, block_cache_pinned_usage, index_and_filter_cache_usage;
    std::map<std::string, std::string> cf_stats_map;
    db->GetIntProperty(cf_handle, "rocksdb.estimate-num-keys", &estimate_keys);
    string_stream << "estimate_keys[" << cf_handle->GetName() << "]:" << estimate_keys << "\r\n";
    db->GetIntProperty(cf_handle, "rocksdb.block-cache-usage", &block_cache_usage);
    string_stream << "block_cache_usage[" << cf_handle->GetName() << "]:" << block_cache_usage << "\r\n";
    db->GetIntProperty(cf_handle, "rocksdb.block-cache-pinned-usage", &block_cache_pinned_usage);
    string_stream << "block_cache_pinned_usage[" << cf_handle->GetName() << "]:" << block_cache_pinned_usage << "\r\n";
    db->GetIntProperty(cf_handle, "rocksdb.estimate-table-readers-mem", &index_and_filter_cache_usage);
    string_stream << "index_and_filter_cache_usage[" << cf_handle->GetName() << "]:" << index_and_filter_cache_usage
                  << "\r\n";
    db->GetMapProperty(cf_handle, rocksdb::DB::Properties::kCFStats, &cf_stats_map);
    string_stream << "level0_file_limit_slowdown[" << cf_handle->GetName() << "]:"
                  << cf_stats_map["io_stalls.level0_slowdown"] << "\r\n";
    string_stream << "level0_file_limit_stop[" << cf_handle->GetName() << "]:"
                  << cf_stats_map["io_stalls.level0_numfiles"] << "\r\n";
    string_stream << "pending_compaction_bytes_slowdown[" << cf_handle->GetName() << "]:"
                  << cf_stats_map["io_stalls.slowdown_for_pending_compaction_bytes"] << "\r\n";
    string_stream << "pending_compaction_bytes_stop[" << cf_handle->GetName() << "]:"
                  << cf_stats_map["io_stalls.stop_for_pending_compaction_bytes"] << "\r\n";
    string_stream << "memtable_count_limit_slowdown[" << cf_handle->GetName() << "]:"
                  << cf_stats_map["io_stalls.memtable_slowdown"] << "\r\n";
    string_stream << "memtable_count_limit_stop[" << cf_handle->GetName() << "]:"
                  << cf_stats_map["io_stalls.memtable_compaction"] << "\r\n";
  }
  string_stream << "all_mem_tables:" << memtable_sizes << "\r\n";
  string_stream << "cur_mem_tables:" << cur_memtable_sizes << "\r\n";
  string_stream << "snapshots:" << num_snapshots << "\r\n";
  string_stream << "num_immutable_tables:" << num_immutable_tables << "\r\n";
  string_stream << "num_running_flushes:" << num_running_flushes << "\r\n";
  string_stream << "memtable_flush_pending:" << memtable_flush_pending << "\r\n";
  string_stream << "compaction_pending:" << compaction_pending << "\r\n";
  string_stream << "num_running_compactions:" << num_running_compaction << "\r\n";
  string_stream << "num_live_versions:" << num_live_versions << "\r\n";
  string_stream << "num_superversion:" << num_superversion << "\r\n";
  string_stream << "num_background_errors:" << num_backgroud_errors << "\r\n";
  string_stream << "flush_count:" << storage_->GetFlushCount()<< "\r\n";
  string_stream << "compaction_count:" << storage_->GetCompactionCount()<< "\r\n";
  string_stream << "put_per_sec:" << stats_.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_PUT) << "\r\n";
  string_stream << "get_per_sec:" << stats_.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_GET) +
                                      stats_.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_MULTIGET) << "\r\n";
  string_stream << "seek_per_sec:" << stats_.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_SEEK) << "\r\n";
  string_stream << "next_per_sec:" << stats_.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_NEXT) << "\r\n";
  string_stream << "prev_per_sec:" << stats_.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_PREV) << "\r\n";
  string_stream << "is_bgsaving:" << (is_bgsave_in_progress_ ? "yes" : "no") << "\r\n";
  string_stream << "is_compacting:" << (db_compacting_ ? "yes" : "no") << "\r\n";
  *info = string_stream.str();
}

void Server::GetServerInfo(std::string *info) {
  time_t now;
  std::ostringstream string_stream;
  static int call_uname = 1;
  static utsname name;
  if (call_uname) {
    /* Uname can be slow and is always the same output. Cache it. */
    uname(&name);
    call_uname = 0;
  }
  time(&now);
  string_stream << "# Server\r\n";
  string_stream << "version:" << VERSION << "\r\n";
  string_stream << "git_sha1:" << GIT_COMMIT << "\r\n";
  string_stream << "os:" << name.sysname << " " << name.release << " " << name.machine << "\r\n";
#ifdef __GNUC__
  string_stream << "gcc_version:" << __GNUC__ << "." << __GNUC_MINOR__ << "." << __GNUC_PATCHLEVEL__ << "\r\n";
#else
  string_stream << "gcc_version:0,0,0\r\n";
#endif
  string_stream << "arch_bits:" << sizeof(void *) * 8 << "\r\n";
  string_stream << "process_id:" << getpid() << "\r\n";
  string_stream << "tcp_port:" << config_->port << "\r\n";
  string_stream << "uptime_in_seconds:" << now-start_time_ << "\r\n";
  string_stream << "uptime_in_days:" << (now-start_time_)/86400 << "\r\n";
  *info = string_stream.str();
}

void Server::GetClientsInfo(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# Clients\r\n";
  string_stream << "maxclients:" << config_->maxclients << "\r\n";
  string_stream << "connected_clients:" << connected_clients_ << "\r\n";
  string_stream << "monitor_clients:" << monitor_clients_ << "\r\n";
  string_stream << "blocked_clients:" << blocked_clients_ << "\r\n";
  *info = string_stream.str();
}

void Server::GetMemoryInfo(std::string *info) {
  std::ostringstream string_stream;
  char used_memory_rss_human[16], used_memory_lua_human[16];
  int64_t rss = Stats::GetMemoryRSS();
  Util::BytesToHuman(used_memory_rss_human, 16, static_cast<uint64_t>(rss));
  int memory_lua = lua_gc(lua_, LUA_GCCOUNT, 0)*1024;
  Util::BytesToHuman(used_memory_lua_human, 16, static_cast<uint64_t>(memory_lua));
  string_stream << "# Memory\r\n";
  string_stream << "used_memory_rss:" << rss <<"\r\n";
  string_stream << "used_memory_human:" << used_memory_rss_human << "\r\n";
  string_stream << "used_memory_lua:" << memory_lua << "\r\n";
  string_stream << "used_memory_lua_human:" << used_memory_lua_human << "\r\n";
  *info = string_stream.str();
}

void Server::GetReplicationInfo(std::string *info) {
  time_t now;
  std::ostringstream string_stream;
  string_stream << "# Replication\r\n";
  string_stream << "role:" << (IsSlave() ? "slave":"master") << "\r\n";
  if (IsSlave()) {
    time(&now);
    string_stream << "master_host:" << master_host_ << "\r\n";
    string_stream << "master_port:" << master_port_ << "\r\n";
    ReplState state = GetReplicationState();
    string_stream << "master_link_status:" << (state == kReplConnected? "up":"down") << "\r\n";
    string_stream << "master_sync_unrecoverable_error:" << (state == kReplError ? "yes" : "no") << "\r\n";
    string_stream << "master_sync_in_progress:" << (state == kReplFetchMeta || state == kReplFetchSST) << "\r\n";
    string_stream << "master_last_io_seconds_ago:" << now-replication_thread_->LastIOTime() << "\r\n";
    string_stream << "slave_repl_offset:" << storage_->LatestSeq() << "\r\n";
    string_stream << "slave_priority:" << config_->slave_priority << "\r\n";
  }

  int idx = 0;
  rocksdb::SequenceNumber latest_seq = storage_->LatestSeq();
  slave_threads_mu_.lock();
  string_stream << "connected_slaves:" << slave_threads_.size() << "\r\n";
  for (const auto &slave : slave_threads_) {
    if (slave->IsStopped()) continue;
    string_stream << "slave" << std::to_string(idx) << ":";
    string_stream << "ip=" << slave->GetConn()->GetIP()
                  << ",port=" << slave->GetConn()->GetListeningPort()
                  << ",offset=" << slave->GetCurrentReplSeq()
                  << ",lag=" << latest_seq - slave->GetCurrentReplSeq() << "\r\n";
    ++idx;
  }
  slave_threads_mu_.unlock();
  string_stream << "master_repl_offset:" << latest_seq  << "\r\n";

  *info = string_stream.str();
}

void Server::GetRoleInfo(std::string *info) {
  if (IsSlave()) {
    std::vector<std::string> roles;
    roles.emplace_back("slave");
    roles.emplace_back(master_host_);
    roles.emplace_back(std::to_string(master_port_));
    auto state = GetReplicationState();
    if (state == kReplConnected) {
      roles.emplace_back("connected");
    } else if (state == kReplFetchMeta || state == kReplFetchSST) {
      roles.emplace_back("sync");
    } else {
      roles.emplace_back("connecting");
    }
    roles.emplace_back(std::to_string(storage_->LatestSeq()));
    *info = Redis::MultiBulkString(roles);
  } else {
    std::vector<std::string> list;
    slave_threads_mu_.lock();
    for (const auto &slave : slave_threads_) {
      if (slave->IsStopped()) continue;
      list.emplace_back(Redis::MultiBulkString({
                                                   slave->GetConn()->GetIP(),
                                                   std::to_string(slave->GetConn()->GetListeningPort()),
                                                   std::to_string(slave->GetCurrentReplSeq()),
                                               }));
    }
    slave_threads_mu_.unlock();
    auto multi_len = 2;
    if (list.size() > 0) {
      multi_len = 3;
    }
    info->append(Redis::MultiLen(multi_len));
    info->append(Redis::BulkString("master"));
    info->append(Redis::BulkString(std::to_string(storage_->LatestSeq())));
    if (list.size() > 0) {
      info->append(Redis::Array(list));
    }
  }
}

std::string Server::GetLastRandomKeyCursor() {
  std::string cursor;
  std::lock_guard<std::mutex> guard(last_random_key_cursor_mu_);
  cursor = last_random_key_cursor_;
  return cursor;
}
void Server::SetLastRandomKeyCursor(const std::string &cursor) {
  std::lock_guard<std::mutex> guard(last_random_key_cursor_mu_);
  last_random_key_cursor_ = cursor;
}

int Server::GetUnixTime() {
  if (unix_time_.load() == 0) {
    time_t ret = time(nullptr);
    unix_time_.store(static_cast<int>(ret));
  }
  return unix_time_.load();
}

void Server::GetStatsInfo(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# Stats\r\n";
  string_stream << "total_connections_received:" << total_clients_ <<"\r\n";
  string_stream << "total_commands_processed:" << stats_.total_calls <<"\r\n";
  string_stream << "instantaneous_ops_per_sec:" << \
  stats_.GetInstantaneousMetric(STATS_METRIC_COMMAND) <<"\r\n";
  string_stream << "total_net_input_bytes:" << stats_.in_bytes <<"\r\n";
  string_stream << "total_net_output_bytes:" << stats_.out_bytes <<"\r\n";
  string_stream << "instantaneous_input_kbps:" << \
  static_cast<float>(stats_.GetInstantaneousMetric(STATS_METRIC_NET_INPUT)/1024) <<"\r\n";
  string_stream << "instantaneous_output_kbps:" << \
  static_cast<float>(stats_.GetInstantaneousMetric(STATS_METRIC_NET_OUTPUT)/1024) <<"\r\n";
  string_stream << "sync_full:" << stats_.fullsync_counter <<"\r\n";
  string_stream << "sync_partial_ok:" << stats_.psync_ok_counter <<"\r\n";
  string_stream << "sync_partial_err:" << stats_.psync_err_counter <<"\r\n";
  {
    std::lock_guard<std::mutex> lg(pubsub_channels_mu_);
    string_stream << "pubsub_channels:" << pubsub_channels_.size() <<"\r\n";
    string_stream << "pubsub_patterns:" << pubsub_patterns_.size() <<"\r\n";
  }
  *info = string_stream.str();
}

void Server::GetCommandsStatsInfo(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# Commandstats\r\n";

  for (const auto &cmd_stat : stats_.commands_stats) {
    auto calls = cmd_stat.second.calls.load();
    auto latency = cmd_stat.second.latency.load();
    if (calls == 0) continue;
    string_stream << "cmdstat_" << cmd_stat.first << ":calls=" << calls
                  << ",usec=" << latency << ",usec_per_call="
                  << ((calls == 0) ? 0 : static_cast<float>(latency/calls))
                  << "\r\n";
  }
  *info = string_stream.str();
}

// WARNING: we must not access DB(i.e.RocksDB) when server is loading since
// DB is closed and the pointer is invalid. Server may crash if we access DB
// during loading.
// If you add new fields which access DB into INFO command output, make sure
// this section cant't be shown when loading(i.e. !is_loading_).
void Server::GetInfo(const std::string &ns, const std::string &section, std::string *info) {
  info->clear();
  std::ostringstream string_stream;
  bool all = section == "all";

  if (all || section == "server") {
    std::string server_info;
    GetServerInfo(&server_info);
    string_stream << server_info;
  }
  if (all || section == "clients") {
    std::string clients_info;
    GetClientsInfo(&clients_info);
    string_stream << clients_info;
  }
  if (all || section == "memory") {
    std::string memory_info;
    GetMemoryInfo(&memory_info);
    string_stream << memory_info;
  }
  if (all || section == "persistence") {
    string_stream << "# Persistence\r\n";
    string_stream << "loading:" << is_loading_ <<"\r\n";

    std::lock_guard<std::mutex> lg(db_job_mu_);
    string_stream << "bgsave_in_progress:" << (is_bgsave_in_progress_? 1 : 0) << "\r\n";
    string_stream << "last_bgsave_time:" << last_bgsave_time_ << "\r\n";
    string_stream << "last_bgsave_status:" << last_bgsave_status_ << "\r\n";
    string_stream << "last_bgsave_time_sec:" << last_bgsave_time_sec_ << "\r\n";
  }
  if (all || section == "stats") {
    std::string stats_info;
    GetStatsInfo(&stats_info);
    string_stream << stats_info;
  }

  // In replication section, we access DB, so we can't do that when loading
  if (!is_loading_ && (all || section == "replication")) {
    std::string replication_info;
    GetReplicationInfo(&replication_info);
    string_stream << replication_info;
  }
  if (all || section == "cpu") {
    struct rusage self_ru;
    getrusage(RUSAGE_SELF, &self_ru);
    string_stream << "# CPU\r\n";
    string_stream << "used_cpu_sys:"
                  << static_cast<float>(self_ru.ru_stime.tv_sec)+static_cast<float>(self_ru.ru_stime.tv_usec/1000000)
                  << "\r\n";
    string_stream << "used_cpu_user:"
                  << static_cast<float>(self_ru.ru_utime.tv_sec)+static_cast<float>(self_ru.ru_utime.tv_usec/1000000)
                  << "\r\n";
  }
  if (all || section == "commandstats") {
    std::string commands_stats_info;
    GetCommandsStatsInfo(&commands_stats_info);
    string_stream << commands_stats_info;
  }

  // In keyspace section, we access DB, so we can't do that when loading
  if (!is_loading_ && (all || section == "keyspace")) {
    KeyNumStats stats;
    GetLastestKeyNumStats(ns, &stats);
    time_t last_scan_time = GetLastScanTime(ns);
    string_stream << "# Keyspace\r\n";
    string_stream << "# Last scan db time: " << std::asctime(std::localtime(&last_scan_time));
    string_stream << "db0:keys=" << stats.n_key << ",expires=" << stats.n_expires
                  << ",avg_ttl=" << stats.avg_ttl << ",expired=" << stats.n_expired << "\r\n";
    string_stream << "sequence:" << storage_->GetDB()->GetLatestSequenceNumber() << "\r\n";
    string_stream << "used_db_size:" << storage_->GetTotalSize(ns) << "\r\n";
    string_stream << "max_db_size:" << config_->max_db_size * GiB << "\r\n";
    double used_percent = config_->max_db_size ?
                          storage_->GetTotalSize() * 100 / (config_->max_db_size * GiB) : 0;
    string_stream << "used_percent: " << used_percent << "%\r\n";
    struct statvfs stat;
    if (statvfs(config_->db_dir.c_str(), &stat) == 0) {
      auto disk_capacity = stat.f_blocks * stat.f_frsize;
      auto used_disk_size = (stat.f_blocks - stat.f_bavail) * stat.f_frsize;
      string_stream << "disk_capacity:" << disk_capacity << "\r\n";
      string_stream << "used_disk_size:" << used_disk_size << "\r\n";
      double used_disk_percent = used_disk_size * 100 / disk_capacity;
      string_stream << "used_disk_percent: " << used_disk_percent << "%\r\n";
    }
  }

  // In rocksdb section, we access DB, so we can't do that when loading
  if (!is_loading_ && (all || section == "rocksdb")) {
    std::string rocksdb_info;
    GetRocksDBInfo(&rocksdb_info);
    string_stream << rocksdb_info;
  }
  *info = string_stream.str();
}

std::string Server::GetRocksDBStatsJson() {
  char buf[256];
  std::string output;

  output.reserve(8*1024);
  output.append("{");
  auto stats = storage_->GetDB()->GetDBOptions().statistics;
  for (const auto &iter : rocksdb::TickersNameMap) {
    snprintf(buf, sizeof(buf), "\"%s\":%" PRIu64 ",",
             iter.second.c_str(), stats->getTickerCount(iter.first));
    output.append(buf);
  }
  for (const auto &iter : rocksdb::HistogramsNameMap) {
    rocksdb::HistogramData hist_data;
    stats->histogramData(iter.first, &hist_data);
    /* P50 P95 P99 P100 COUNT SUM */
    snprintf(buf, sizeof(buf), "\"%s\":[%f,%f,%f,%f,%" PRIu64 ",%" PRIu64 "],",
             iter.second.c_str(),
             hist_data.median, hist_data.percentile95, hist_data.percentile99,
             hist_data.max, hist_data.count, hist_data.sum);
    output.append(buf);
  }
  output.pop_back();
  output.append("}");
  output.shrink_to_fit();
  return output;
}

// This function is called by replication thread when finished fetching
// all files from its master.
// Before restoring the db from backup or checkpoint, we should
// guarantee other threads don't acess DB and its column families,
// then close db.
void Server::PrepareRestoreDB() {
  // Stop freeding slaves thread
  LOG(INFO) << "Disconnecting slaves...";
  DisconnectSlaves();

  // Stop task runner
  LOG(INFO) << "Stopping the task runner and clear task queue...";
  task_runner_.Stop();
  task_runner_.Join();
  task_runner_.Purge();

  // If the DB is retored, the object 'db_' will be destroyed, but
  // 'db_' will be accessed in data migration task. To avoid wrong
  // accessing, data migration task should be stopped before restoring DB
  WaitNoMigrateProcessing();

  // To guarantee work theads don't access DB, we should relase 'ExclusivityGuard'
  // ASAP to avoid user can't recieve respones for long time, becasue the following
  // 'CloseDB' may cost much time to acquire DB mutex.
  LOG(INFO) << "Waiting workers for finishing executing commands...";
  {
    auto exclusivity = WorkExclusivityGuard();
    is_loading_ = true;
  }

  // Cron thread, compaction checker thread, full synchronization thread
  // may always run in the backgroud, we need to close db, so they don't
  // actually work.
  LOG(INFO) << "Waiting for closing DB...";
  storage_->CloseDB();
}

void Server::WaitNoMigrateProcessing() {
  if (config_->cluster_enabled) {
    LOG(INFO) << "Waiting until no migration task is running...";
    slot_migrate_->SetMigrateStopFlag(true);
    while (slot_migrate_->GetMigrateStateMachine() != MigrateStateMachine::kSlotMigrateNone) {
      usleep(500);
    }
  }
}

Status Server::AsyncCompactDB(const std::string &begin_key, const std::string &end_key) {
  if (is_loading_) {
    return Status(Status::NotOK, "loading in-progress");
  }
  std::lock_guard<std::mutex> lg(db_job_mu_);
  if (db_compacting_) {
    return Status(Status::NotOK, "compact in-progress");
  }
  db_compacting_ = true;

  Task task = [begin_key, end_key, this] {
    Slice *begin = nullptr, *end = nullptr;
    if (!begin_key.empty()) begin = new Slice(begin_key);
    if (!end_key.empty()) end = new Slice(end_key);
    storage_->Compact(begin, end);
    std::lock_guard<std::mutex> lg(db_job_mu_);
    db_compacting_ = false;
    delete begin;
    delete end;
  };
  return task_runner_.Publish(task);
}

Status Server::AsyncBgsaveDB() {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  if (is_bgsave_in_progress_) {
    return Status(Status::NotOK, "bgsave in-progress");
  }
  is_bgsave_in_progress_ = true;

  Task task = [this] {
    auto start_bgsave_time = std::time(nullptr);
    Status s = storage_->CreateBackup();
    auto stop_bgsave_time = std::time(nullptr);

    std::lock_guard<std::mutex> lg(db_job_mu_);
    is_bgsave_in_progress_ = false;
    last_bgsave_time_ = static_cast<int>(start_bgsave_time);
    last_bgsave_status_ = s.IsOK() ? "ok" : "err";
    last_bgsave_time_sec_ = static_cast<int>(stop_bgsave_time-start_bgsave_time);
  };
  return task_runner_.Publish(task);
}

Status Server::AsyncPurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours) {
  Task task = [num_backups_to_keep, backup_max_keep_hours, this] {
    storage_->PurgeOldBackups(num_backups_to_keep, backup_max_keep_hours);
  };
  return task_runner_.Publish(task);
}

Status Server::AsyncScanDBSize(const std::string &ns) {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  auto iter = db_scan_infos_.find(ns);
  if (iter == db_scan_infos_.end()) {
    db_scan_infos_[ns] = DBScanInfo{};
  }
  if (db_scan_infos_[ns].is_scanning) {
    return Status(Status::NotOK, "scanning the db now");
  }
  db_scan_infos_[ns].is_scanning = true;

  Task task = [ns, this] {
    Redis::Database db(storage_, ns);
    KeyNumStats stats;
    db.GetKeyNumStats("", &stats);

    std::lock_guard<std::mutex> lg(db_job_mu_);
    db_scan_infos_[ns].key_num_stats = stats;
    time(&db_scan_infos_[ns].last_scan_time);
    db_scan_infos_[ns].is_scanning = false;
  };
  return task_runner_.Publish(task);
}

Status Server::autoResizeBlockAndSST() {
  auto total_size = storage_->GetTotalSize(kDefaultNamespace);
  uint64_t total_keys = 0, estimate_keys = 0;
  for (const auto &cf_handle : *storage_->GetCFHandles()) {
    storage_->GetDB()->GetIntProperty(cf_handle, "rocksdb.estimate-num-keys", &estimate_keys);
    total_keys += estimate_keys;
  }
  if (total_size == 0 || total_keys == 0) {
    return Status::OK();
  }
  auto average_kv_size = total_size / total_keys;
  int target_file_size_base = 0;
  int block_size = 0;
  if (average_kv_size > 512 * KiB) {
    target_file_size_base = 1024;
    block_size = 1 * MiB;
  } else if (average_kv_size > 256 * KiB) {
    target_file_size_base = 512;
    block_size = 512 * KiB;
  } else if (average_kv_size > 32 * KiB) {
    target_file_size_base = 256;
    block_size = 256 * KiB;
  } else if (average_kv_size > 1 * KiB) {
    target_file_size_base = 128;
    block_size = 32 * KiB;
  } else if (average_kv_size > 128) {
    target_file_size_base = 64;
    block_size = 8 * KiB;
  } else {
    target_file_size_base = 16;
    block_size = 2 * KiB;
  }
  if (target_file_size_base == config_->RocksDB.target_file_size_base
      && target_file_size_base == config_->RocksDB.write_buffer_size
      && block_size == config_->RocksDB.block_size) {
    return Status::OK();
  }
  if (target_file_size_base != config_->RocksDB.target_file_size_base) {
    auto old_target_file_size_base = config_->RocksDB.target_file_size_base;
    auto s = config_->Set(this, "rocksdb.target_file_size_base", std::to_string(target_file_size_base));
    LOG(INFO) << "[server] Resize rocksdb.target_file_size_base from "
              << old_target_file_size_base
              << " to " << target_file_size_base
              << ", average_kv_size: " << average_kv_size
              << ", total_size: " << total_size
              << ", total_keys: " << total_keys
              << ", result: " << s.Msg();
    if (!s.IsOK()) {
      return s;
    }
  }
  if (target_file_size_base != config_->RocksDB.write_buffer_size) {
    auto old_write_buffer_size = config_->RocksDB.write_buffer_size;
    auto s = config_->Set(this, "rocksdb.write_buffer_size", std::to_string(target_file_size_base));
    LOG(INFO) << "[server] Resize rocksdb.write_buffer_size from "
              << old_write_buffer_size
              << " to " << target_file_size_base
              << ", average_kv_size: " << average_kv_size
              << ", total_size: " << total_size
              << ", total_keys: " << total_keys
              << ", result: " << s.Msg();
    if (!s.IsOK()) {
      return s;
    }
  }
  if (block_size != config_->RocksDB.block_size) {
    auto s = storage_->SetColumnFamilyOption("table_factory.block_size", std::to_string(block_size));
    LOG(INFO) << "[server] Resize rocksdb.block_size from "
              << config_->RocksDB.block_size
              << " to " << block_size
              << ", average_kv_size: " << average_kv_size
              << ", total_size: " << total_size
              << ", total_keys: " << total_keys
              << ", result: " << s.Msg();
    if (!s.IsOK()) {
      return s;
    }
    config_->RocksDB.block_size = block_size;
  }
  auto s = config_->Rewrite();
  LOG(INFO) << "[server] rewrite config, result: " << s.Msg();
  return Status::OK();
}

void Server::GetLastestKeyNumStats(const std::string &ns, KeyNumStats *stats) {
  auto iter = db_scan_infos_.find(ns);
  if (iter != db_scan_infos_.end()) {
    *stats = iter->second.key_num_stats;
  }
}

time_t Server::GetLastScanTime(const std::string &ns) {
  auto iter = db_scan_infos_.find(ns);
  if (iter != db_scan_infos_.end()) {
    return iter->second.last_scan_time;
  }
  return 0;
}

void Server::SlowlogPushEntryIfNeeded(const std::vector<std::string>* args, uint64_t duration) {
  int64_t threshold = config_->slowlog_log_slower_than;
  if (threshold < 0 || static_cast<int64_t>(duration) < threshold) return;
  auto entry = new SlowEntry();
  size_t argc = args->size() > kSlowLogMaxArgc ? kSlowLogMaxArgc : args->size();
  for (size_t i = 0; i < argc; i++) {
    if (argc != args->size() && i == argc-1) {
      entry->args.emplace_back("... (" +
        std::to_string(args->size()-argc+1) + " more arguments)");
      break;
    }
    if (args->data()[i].length() <= kSlowLogMaxString) {
      entry->args.emplace_back(args->data()[i]);
    } else {
      entry->args.emplace_back(args->data()[i].substr(0, kSlowLogMaxString) + "... (" +
          std::to_string(args->data()[i].length() - kSlowLogMaxString) + " more bytes)");
    }
  }
  entry->duration = duration;
  slow_log_.PushEntry(entry);
}

std::string Server::GetClientsStr() {
  std::string clients;
  for (const auto &t : worker_threads_) {
    clients.append(t->GetWorker()->GetClientsStr());
  }
  std::lock_guard<std::mutex> guard(slave_threads_mu_);
  for (const auto &st : slave_threads_) {
    clients.append(st->GetConn()->ToString());
  }
  return clients;
}

void Server::KillClient(int64_t *killed, std::string addr, uint64_t id,
                         uint64_t type, bool skipme, Redis::Connection *conn) {
  *killed = 0;

  // Normal clients and pubsub clients
  for (const auto &t : worker_threads_) {
    int64_t killed_in_worker = 0;
    t->GetWorker()->KillClient(conn, id, addr, type, skipme, &killed_in_worker);
    *killed += killed_in_worker;
  }

  // Slave clients
  slave_threads_mu_.lock();
  for (const auto &st : slave_threads_) {
    if ((type & kTypeSlave) ||
        (!addr.empty() && st->GetConn()->GetAddr() == addr) ||
        (id != 0 && st->GetConn()->GetID() == id)) {
      st->Stop();
      (*killed)++;
    }
  }
  slave_threads_mu_.unlock();

  // Master client
  if (IsSlave() &&
      (type & kTypeMaster ||
       (!addr.empty() && addr == master_host_+":"+std::to_string(master_port_)))) {
    // Stop replication thread and start a new one to replicate
    AddMaster(master_host_, master_port_, true);
    (*killed)++;
  }
}

ReplState Server::GetReplicationState() {
  if (IsSlave() && replication_thread_) {
    return replication_thread_->State();
  }
  return kReplConnecting;
}

Status Server::LookupAndCreateCommand(const std::string &cmd_name,
                                      std::unique_ptr<Redis::Commander> *cmd) {
  auto commands = Redis::GetCommands();
  if (cmd_name.empty()) return Status(Status::RedisUnknownCmd);
  auto cmd_iter = commands->find(Util::ToLower(cmd_name));
  if (cmd_iter == commands->end()) {
    return Status(Status::RedisUnknownCmd);
  }
  auto redisCmd = cmd_iter->second;
  *cmd = redisCmd->factory();
  (*cmd)->SetAttributes(redisCmd);
  return Status::OK();
}

Status Server::ScriptExists(const std::string &sha) {
  std::string body;
  return ScriptGet(sha, &body);
}

Status Server::ScriptGet(const std::string &sha, std::string *body) {
  std::string funcname = Engine::kLuaFunctionPrefix + sha;
  auto cf = storage_->GetCFHandle(Engine::kPropagateColumnFamilyName);
  auto s = storage_->GetDB()->Get(rocksdb::ReadOptions(), cf, funcname, body);
  if (!s.ok()) {
    if (s.IsNotFound()) return Status(Status::NotFound);
    return Status(Status::NotOK, s.ToString());
  }
  return Status::OK();
}

void Server::ScriptSet(const std::string &sha, const std::string &body) {
  std::string funcname = Engine::kLuaFunctionPrefix + sha;
  storage_->WriteToPropagateCF(funcname, body);
}

void Server::ScriptReset() {
  Lua::DestroyState(lua_);
  lua_ = Lua::CreateState();
}

void Server::ScriptFlush() {
  auto cf = storage_->GetCFHandle(Engine::kPropagateColumnFamilyName);
  storage_->FlushScripts(storage_->DefaultWriteOptions(), cf);
  ScriptReset();
}

// Generally, we store data into rocksdb and just replicate WAL instead of propagating
// commands. But sometimes, we need to update inner states or do special operations
// for specific commands, such as `script flush`.
// channel: we put the same function commands into one channel to handle uniformly
// tokens: the serialized commands
Status Server::Propagate(const std::string &channel, const std::vector<std::string> &tokens) {
  std::string value = Redis::MultiLen(tokens.size());
  for (const auto &iter : tokens) {
    value += Redis::BulkString(iter);
  }
  return storage_->WriteToPropagateCF(channel, value);
}

Status Server::ExecPropagateScriptCommand(const std::vector<std::string> &tokens) {
  auto subcommand = Util::ToLower(tokens[1]);
  if (subcommand == "flush") {
    ScriptReset();
  }
  return Status::OK();
}

Status Server::ExecPropagatedCommand(const std::vector<std::string> &tokens) {
  if (tokens.empty()) return Status::OK();

  auto command = Util::ToLower(tokens[0]);
  if (command == "script" && tokens.size() >= 2) {
    return ExecPropagateScriptCommand(tokens);
  }
  return Status::OK();
}

// AdjustOpenFilesLimit only try best to raise the max open files according to
// the max clients and rocksdb open file configuration. It also reserves a number
// of file descriptors(128) for extra operations of persistence, listening sockets,
// log files and so forth.
void Server::AdjustOpenFilesLimit() {
  const int min_reserved_fds = 128;
  auto rocksdb_max_open_file = static_cast<rlim_t>(config_->RocksDB.max_open_files);
  auto max_clients = static_cast<rlim_t>(config_->maxclients);
  auto max_files = max_clients + rocksdb_max_open_file + min_reserved_fds;

  struct rlimit limit;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    return;
  }
  rlim_t old_limit = limit.rlim_cur;
  // Set the max number of files only if the current limit is not enough
  if (old_limit >= max_files) {
    return;
  }

  int setrlimit_error = 0;
  rlim_t best_limit = max_files;
  while (best_limit > old_limit) {
    limit.rlim_cur = best_limit;
    limit.rlim_max = best_limit;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) break;
    setrlimit_error = errno;

    rlim_t decr_step = 16;
    if (best_limit < decr_step) {
      best_limit = old_limit;
      break;
    }
    best_limit -= decr_step;
  }

  if (best_limit < old_limit) best_limit = old_limit;
  if (best_limit < max_files) {
    if (best_limit <= static_cast<int>(min_reserved_fds)) {
      LOG(WARNING) << "[server] Your current 'ulimit -n' of " << old_limit << " is not enough for the server to start."
                   << "Please increase your open file limit to at least " << max_files << ". Exiting.";
      exit(1);
    }
    LOG(WARNING) << "[server] You requested max clients of " << max_clients << " and rocksdb max open files of "
                 << rocksdb_max_open_file <<" requiring at least " << max_files << " max file descriptors.";
    LOG(WARNING) << "[server] Server can't set maximum open files to " << max_files
                 << " because of OS error: " << strerror(setrlimit_error);
  } else {
    LOG(WARNING) << "[server] Increased maximum number of open files to " << max_files
                 << " (it's originally set to " << old_limit << ")";
  }
}

std::string ServerLogData::Encode() {
  if (type_ == kReplIdLog) {
    return std::string(1, kReplIdTag) + " " + content_;
  } else {
    return content_;
  }
}

Status ServerLogData::Decode(const rocksdb::Slice &blob) {
  if (blob.size() == 0) {
    return Status(Status::NotOK);
  }

  const char *header = blob.data();
  // Only support `kReplIdTag` now
  if (*header == kReplIdTag && blob.size() == 2 + kReplIdLength) {
    type_ = kReplIdLog;
    content_ = std::string(blob.data()+2, blob.size()-2);
    return Status::OK();
  }
  return Status(Status::NotOK);
}
