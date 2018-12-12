#include "server.h"
#include "worker.h"
#include "redis_request.h"
#include "version.h"
#include "string_util.h"

#include <sys/utsname.h>
#include <sys/resource.h>
#include <glog/logging.h>

Server::Server(Engine::Storage *storage, Config *config) :
  storage_(storage), config_(config) {

  for (int i = 0; i < config->workers; i++) {
    auto worker = new Worker(this, config);
    worker_threads_.emplace_back(new WorkerThread(worker));
  }
  task_runner_ = new TaskRunner(2, 1024);
  time(&start_time_);
}

Server::~Server() {
  delete task_runner_;
}

Status Server::Start() {
  if (!config_->master_host.empty()) {
    Status s = AddMaster(config_->master_host, static_cast<uint32_t>(config_->master_port));
    if (!s.IsOK()) return s;
  }

  for (const auto worker : worker_threads_) {
    worker->Start();
  }
  task_runner_->Start();
  // setup server cron thread
  cron_thread_ = std::thread([this]() { this->cron(); });
  return Status::OK();
}

void Server::Stop() {
  if (replication_thread_) replication_thread_->Stop();
  for (const auto worker : worker_threads_) {
    worker->Stop();
  }
  task_runner_->Stop();
  if(cron_thread_.joinable()) cron_thread_.join();
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
      new ReplicationThread(master_host_, master_port_, storage_, config_->master_auth));
  replication_thread_->Start([this]() { this->is_loading_ = true; },
                             [this]() { this->is_loading_ = false; });
  return Status::OK();
}

Status Server::RemoveMaster() {
  if (!master_host_.empty()) {
    master_host_.clear();
    master_port_ = 0;
    if (replication_thread_) replication_thread_->Stop();
    replication_thread_ = nullptr;
  }
  return Status::OK();
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

Status Server::IncrClients() {
  auto connections = connected_clients_.fetch_add(1, std::memory_order_relaxed);
  if (config_->maxclients > 0 && connections >= config_->maxclients) {
    connected_clients_.fetch_sub(1, std::memory_order_relaxed);
    return Status(Status::NotOK, "max number of clients reached");
  }
  total_clients_.fetch_add(1, std::memory_order_relaxed);
  return Status::OK();
}

void Server::DecrClients() {
  connected_clients_.fetch_sub(1, std::memory_order_relaxed);
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

void Server::GetRocksDBInfo(std::string &info) {
  std::ostringstream string_stream;
  rocksdb::DB *db = storage_->GetDB();

  uint64_t estimate_keys, memtable_sizes, num_snapshots, num_running_flushes;
  uint64_t num_immutable_tables, memtable_flush_pending, compaction_pending;
  uint64_t num_running_compaction, num_live_versions, num_superversion, num_backgroud_errors;
  db->GetAggregatedIntProperty("rocksdb.estimate-num-keys", &estimate_keys);
  db->GetAggregatedIntProperty("rocksdb.size-all-mem-tables", &memtable_sizes);
  db->GetAggregatedIntProperty("rocksdb.num-snapshots", &num_snapshots);
  db->GetAggregatedIntProperty("rocksdb.num-immutable-mem-table", &num_immutable_tables);
  db->GetAggregatedIntProperty("rocksdb.num-running-flushes", &num_running_flushes);
  db->GetAggregatedIntProperty("rocksdb.mem-table-flush-pending", &memtable_flush_pending);
  db->GetAggregatedIntProperty("rocksdb.compaction-pending", &compaction_pending);
  db->GetAggregatedIntProperty("rocksdb.num-running-compactions", &num_running_compaction);
  db->GetAggregatedIntProperty("rocksdb.num-live-versions", &num_live_versions);
  db->GetAggregatedIntProperty("rocksdb.current-super-version-number", &num_superversion);
  db->GetAggregatedIntProperty("rocksdb.background-errors", &num_backgroud_errors);

  string_stream << "# RocksDB\r\n";
  string_stream << "estimate_keys:" << estimate_keys << "\r\n";
  string_stream << "all_mem_tables:" << memtable_sizes << "\r\n";
  string_stream << "snapshots:" << num_snapshots << "\r\n";
  string_stream << "num_immutable_tables:" << num_immutable_tables << "\r\n";
  string_stream << "num_running_flushes:" << num_running_flushes << "\r\n";
  string_stream << "memtable_flush_pending:" << memtable_flush_pending << "\r\n";
  string_stream << "compaction_pending:" << compaction_pending << "\r\n";
  string_stream << "num_running_compactions:" << num_running_compaction << "\r\n";
  string_stream << "num_live_versions:" << num_live_versions << "\r\n";
  string_stream << "num_superversion:" << num_superversion << "\r\n";
  string_stream << "num_background_errors:" << num_backgroud_errors << "\r\n";
  info = string_stream.str();
}

void Server::GetServerInfo(std::string &info) {
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
  string_stream << "arch_bits:" << sizeof(long)*8 << "\r\n";
  string_stream << "process_id:" << getpid() << "\r\n";
  string_stream << "tcp_port:" << config_->port << "\r\n";
  string_stream << "uptime_in_seconds:" << now-start_time_ << "\r\n";
  string_stream << "uptime_in_days:" << (now-start_time_)/86400 << "\r\n";
  info = string_stream.str();
}

void Server::GetClientsInfo(std::string &info) {
  std::ostringstream string_stream;
  string_stream << "# Clients\r\n";
  string_stream << "connected_clients:" << connected_clients_ << "\r\n";
  // TODO: blocked clients
  info = string_stream.str();
}

void Server::GetMemoryInfo(std::string &info) {
  std::ostringstream string_stream;
  char buf[16];
  long rss = Stats::GetMemoryRSS();
  Util::BytesToHuman(buf, static_cast<unsigned long long>(rss));
  string_stream << "# Memory\r\n";
  string_stream << "used_memory_rss:" << rss <<"\r\n";
  string_stream << "used_memory_human:" << buf <<"\r\n";
  info = string_stream.str();
}

void Server::GetReplicationInfo(std::string &info) {
  std::ostringstream string_stream;
  string_stream << "# Replication\r\n";
  string_stream << "role:"<< (master_host_.empty()?"master":"slave") << "\r\n";
  if (!master_host_.empty()) {
    string_stream << "master_host:" << master_host_ << "\r\n";
    string_stream << "master_port:" << master_port_ << "\r\n";
    ReplState state = replication_thread_->State();
    string_stream << "master_link_status:" << (state == kReplConnected? "up":"down") << "\r\n";
    string_stream << "master_sync_unrecoverable_error:" << (state == kReplError? "yes" : "no") << "\r\n";
    string_stream << "master_sync_in_progress:" << (state==kReplFetchMeta||state==kReplFetchSST) << "\r\n";
    // TODO: last io time, 主从同步目前修改可能比较多，后面再加
    string_stream << "master_last_io_seconds_ago:" << 0 << "\r\n";
    string_stream << "slave_repl_offset:" << replication_thread_->Offset() << "\r\n";
  }
  // TODO: slave priority/readonly
  // TODO: slaves
  info = string_stream.str();
}

void Server::GetStatsInfo(std::string &info) {
  std::ostringstream string_stream;
  string_stream << "# Stats\r\n";
  string_stream << "total_connections_received:" << total_clients_ <<"\r\n";
  string_stream << "total_commands_processed:" << stats_.total_calls <<"\r\n";
  string_stream << "total_net_input_bytes:" << stats_.in_bytes <<"\r\n";
  string_stream << "total_net_output_bytes:" << stats_.out_bytes <<"\r\n";
  string_stream << "sync_full:" << stats_.fullsync_counter <<"\r\n";
  string_stream << "sync_partial_ok:" << stats_.psync_ok_counter <<"\r\n";
  string_stream << "sync_partial_err:" << stats_.psync_err_counter <<"\r\n";
  string_stream << "pubsub_channels:" << pubsub_channels_.size() <<"\r\n";
  info = string_stream.str();
}

void Server::GetInfo(std::string ns, std::string section, std::string &info) {
  info.clear();
  std::ostringstream string_stream;
  bool all = section == "all";

  if (all || section == "server") {
    std::string server_info;
    GetServerInfo(server_info);
    string_stream << server_info;
  }
  if (all || section == "clients") {
    std::string clients_info;
    GetClientsInfo(clients_info);
    string_stream << clients_info;
  }
  if (all || section == "memory") {
    std::string memory_info;
    GetMemoryInfo(memory_info);
    string_stream << memory_info;
  }
  if (all || section == "persistence") {
    string_stream << "# Persistence\r\n";
    string_stream << "loading:" << is_loading_ <<"\r\n";
    // TODO: db size
  }
  if (all || section == "stats") {
    std::string stats_info;
    GetStatsInfo(stats_info);
    string_stream << stats_info;
  }
  if (all || section == "replication") {
    std::string replication_info;
    GetReplicationInfo(replication_info);
    string_stream << replication_info;
  }
  if (all || section == "cpu") {
    struct rusage self_ru;
    getrusage(RUSAGE_SELF, &self_ru);
    string_stream << "# CPU\r\n";
    string_stream << "used_cpu_sys:"
                  << (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000 << "\r\n";
    string_stream << "used_cpu_user:"
                  << (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000 << "\r\n";
  }
  if (all || section == "commandstats") {
  }
  if (all || section == "keyspace") {
    string_stream << "# Keyspace\r\n";
    string_stream << "dbsize: " << GetLastKeyNum(ns) << "\r\n";
  }
  if (all || section == "rocksdb") {
    std::string rocksdb_info;
    GetRocksDBInfo(rocksdb_info);
    string_stream << rocksdb_info;
  }
  info = string_stream.str();
}

Status Server::AsyncScanDBSize(std::string &ns) {
  // data race is ok, needn't lock the db_scan_infos_
  auto iter = db_scan_infos_.find(ns);
  if(iter == db_scan_infos_.end()) {
    db_scan_infos_[ns] = DBScanInfo{};
  }
  if (db_scan_infos_[ns].is_scanning) {
    return Status(Status::NotOK, "scanning the db now");
  }
  db_scan_infos_[ns].is_scanning = true;
  Task task;
  task.arg = this;
  task.callback = [ns](void *arg) {
    Server *svr = static_cast<Server*>(arg);
    RedisDB db(svr->storage_, ns);
    svr->db_scan_infos_[ns].n_key = db.GetKeyNum();
    svr->db_scan_infos_[ns].is_scanning = false;
  };
  return task_runner_->Publish(task);
}

uint64_t Server::GetLastKeyNum(std::string &ns) {
  auto iter = db_scan_infos_.find(ns);
  if(iter != db_scan_infos_.end()) {
    return iter->second.n_key;
  }
  return 0;
}

time_t Server::GetLastScanTime(std::string &ns) {
  auto iter = db_scan_infos_.find(ns);
  if(iter != db_scan_infos_.end()) {
    return iter->second.last_scan_time;
  }
  return 0;
}