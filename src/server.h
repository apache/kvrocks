#pragma once

#include <list>
#include <string>
#include <vector>

#include "replication.h"
#include "stats.h"
#include "storage.h"
#include "task_runner.h"
#include "redis_metadata.h"

namespace Redis {
class Connection;
}

class WorkerThread;

struct DBScanInfo {
  time_t last_scan_time = 0;
  uint64_t n_key = 0;
  bool is_scanning = false;
};

struct SlowlogEntry {
  std::vector<std::string> args;
  uint64_t id;
  uint64_t duration;
  time_t time;
};

class Server {
 public:
  explicit Server(Engine::Storage *storage, Config *config);
  ~Server();
  Status Start();
  void Stop();
  void Join();

  Status AddMaster(std::string host, uint32_t port);
  Status RemoveMaster();
  bool IsLoading() { return is_loading_; }
  int PublishMessage(std::string &channel, std::string &msg);
  void SubscribeChannel(std::string &channel, Redis::Connection *conn);
  void UnSubscribeChannel(std::string &channel, Redis::Connection *conn);
  Config *GetConfig() { return config_; }
  bool IsSlave() { return !master_host_.empty(); }

 private:
  struct SlaveInfo;

 public:
  using SlaveInfoPos = std::list<std::shared_ptr<SlaveInfo>>::iterator;
  SlaveInfoPos AddSlave(const std::string &addr, uint32_t port) {
    std::lock_guard<std::mutex> guard(slaves_info_mu_);
    slaves_info_.push_back(
        std::shared_ptr<SlaveInfo>(new SlaveInfo(addr, port)));
    return --(slaves_info_.end());
  }
  void RemoveSlave(SlaveInfoPos &pos) {
    std::lock_guard<std::mutex> guard(slaves_info_mu_);
    slaves_info_.erase(pos);
  }
  void UpdateSlaveStats(SlaveInfoPos &pos, rocksdb::SequenceNumber seq) {
    (*pos)->seq = seq;
  }

  Status IncrClients();
  void DecrClients();
  std::atomic<uint64_t> *GetClientID();
  void GetInfo(std::string ns, std::string section, std::string &info);
  void GetStatsInfo(std::string &info);
  void GetCommandsStatsInfo(std::string &info);
  void GetServerInfo(std::string &info);
  void GetRocksDBInfo(std::string &info);
  void GetReplicationInfo(std::string &info);
  void GetClientsInfo(std::string &info);
  void GetMemoryInfo(std::string &info);

  Status AsyncCompactDB();
  Status AsyncBgsaveDB();
  Status AsyncScanDBSize(std::string &ns);
  uint64_t GetLastKeyNum(std::string &ns);
  time_t GetLastScanTime(std::string &ns);

  void SlowlogReset();
  uint32_t SlowlogLen();
  void CreateSlowlogReply(std::string *output, uint32_t count);
  void SlowlogPushEntryIfNeeded(const std::vector<std::string>* args, uint64_t duration);

  std::string GetClientsStr();
  void KillClient(int64_t *killed, std::string addr, uint64_t id, bool skipme, Redis::Connection *conn);
  void KickoutIdleClients();

  Stats stats_;
  Engine::Storage *storage_;

 private:
  bool stop_ = false;
  bool is_loading_ = false;
  time_t start_time_ = 0;
  std::string master_host_;
  uint32_t master_port_ = 0;

  std::atomic<int> connected_clients_{0};
  std::atomic<uint64_t> total_clients_{0};
  std::atomic<uint64_t> client_id_{1};

  Config *config_;
  std::vector<WorkerThread *> worker_threads_;
  std::unique_ptr<ReplicationThread> replication_thread_;
  std::thread cron_thread_;
  TaskRunner *task_runner_;

  std::mutex db_mutex_;
  bool db_compacting_ = false;
  bool db_bgsave_ = false;
  std::map<std::string, DBScanInfo> db_scan_infos_;
  // TODO: locked before modify
  std::map<std::string, std::list<Redis::Connection *>> pubsub_channels_;

  // Used by master role, tracking slaves' info
  struct SlaveInfo {
    std::string addr;
    uint32_t port;
    rocksdb::SequenceNumber seq = 0;

    SlaveInfo(std::string a, uint32_t p) : addr(std::move(a)), port(p) {}
  };
  std::mutex slaves_info_mu_;
  std::list<std::shared_ptr<SlaveInfo>> slaves_info_;
  using slaves_info_iter_ = std::list<std::shared_ptr<SlaveInfo>>::iterator;

  struct SlowLog {
    std::list<SlowlogEntry> entry_list;
    uint64_t id = 0;
    std::mutex mu;
  } slowlog_;

  void cron();
  void clientsCron();
  Status compactCron();
  Status bgsaveCron();
};
