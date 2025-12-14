#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include "status.h"

class Server;

enum class FailoverState { kNone = 0, kStarted, kCheck, kPause, kSyncWait, kSwitch, kSuccess, kFailed };

class ClusterFailover {
 public:
  explicit ClusterFailover(Server *srv);
  ~ClusterFailover();

  Status Run(std::string slave_node_id, int timeout_ms);
  bool IsWriteForbidden() {
    auto s = state_.load();
    return s == FailoverState::kPause || s == FailoverState::kSyncWait || s == FailoverState::kSwitch;
  }
  std::string GetSlaveNodeId() { return slave_node_id_; }
  void GetFailoverInfo(std::string *info);
  void ResetFailoverState() { state_ = FailoverState::kNone; }

 private:
  void loop();
  void runFailoverProcess();

  Status checkSlaveStatus();
  Status checkSlaveLag();
  Status waitReplicationSync();
  Status sendTakeoverCmd();
  void abortFailover(const std::string &reason);

  Server *srv_;
  std::atomic<FailoverState> state_{FailoverState::kNone};
  std::string slave_node_id_;
  std::string node_ip_port_;
  std::string node_ip_;
  int node_port_ = 0;
  int timeout_ms_ = 0;
  uint64_t target_seq_ = 0;
  int64_t start_time_ms_ = 0;

  std::thread t_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stop_thread_ = false;
  std::atomic<bool> failover_job_triggered_{false};
};
