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

#include "cluster_failover.h"

#include <unistd.h>

#include "cluster/cluster.h"
#include "common/io_util.h"
#include "common/time_util.h"
#include "logging.h"
#include "server/redis_reply.h"
#include "server/server.h"

ClusterFailover::ClusterFailover(Server *srv) : srv_(srv) {
  t_ = std::thread([this]() { loop(); });
}

ClusterFailover::~ClusterFailover() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_thread_ = true;
    cv_.notify_all();
  }
  if (t_.joinable()) t_.join();
}

Status ClusterFailover::Run(std::string slave_node_id, int timeout_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (state_ != FailoverState::kNone && state_ != FailoverState::kFailed) {
    return {Status::NotOK, "Failover is already in progress"};
  }
 
  if (srv_->IsSlave()) {
    return {Status::NotOK, "Current node is a slave, can't failover"};
  }

  slave_node_id_ = std::move(slave_node_id);
  timeout_ms_ = timeout_ms;
  state_ = FailoverState::kStarted;
  failover_job_triggered_ = true;
  cv_.notify_all();
  return Status::OK();
}

void ClusterFailover::loop() {
  while (true) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this]() { return stop_thread_ || failover_job_triggered_; });

    if (stop_thread_) return;

    if (failover_job_triggered_) {
      failover_job_triggered_ = false;
      lock.unlock();
      runFailoverProcess();
    }
  }
}

void ClusterFailover::runFailoverProcess() {
  auto ip_port = srv_->cluster->GetNodeIPPort(slave_node_id_);
  if (!ip_port.IsOK()) {
    error("[Failover] slave node not found in cluster {}", slave_node_id_);
    abortFailover("Slave node not found in cluster");
    return;
  }
  node_ip_port_ = ip_port.GetValue().first + ":" + std::to_string(ip_port.GetValue().second);
  node_ip_ = ip_port.GetValue().first;
  node_port_ = ip_port.GetValue().second;
  info("[Failover] slave node {} {} failover state: {}", slave_node_id_, node_ip_port_, static_cast<int>(state_.load()));
  state_ = FailoverState::kCheck;

  auto s = checkSlaveStatus();
  if (!s.IsOK()) {
    abortFailover(s.Msg());
    return;
  }

  s = checkSlaveLag();
  if (!s.IsOK()) {
    abortFailover("Slave lag check failed: " + s.Msg());
    return;
  }

  info("[Failover] slave node {} {} check slave status success, enter pause state", slave_node_id_, node_ip_port_);
  start_time_ms_ = util::GetTimeStampMS();
  // Enter Pause state (Stop writing)
  state_ = FailoverState::kPause;
  // Get current sequence
  target_seq_ = srv_->storage->LatestSeqNumber();
  info("[Failover] slave node {} {} target sequence {}", slave_node_id_, node_ip_port_, target_seq_);

  state_ = FailoverState::kSyncWait;
  s = waitReplicationSync();
  if (!s.IsOK()) {
    abortFailover(s.Msg());
    return;
  }
  info("[Failover] slave node {} {} wait replication sync success, enter switch state, cost {} ms", slave_node_id_,
       node_ip_port_, util::GetTimeStampMS() - start_time_ms_);

  state_ = FailoverState::kSwitch;
  s = sendTakeoverCmd();
  if (!s.IsOK()) {
    abortFailover(s.Msg());
    return;
  }

  // Redirect slots
  srv_->cluster->SetMySlotsMigrated(node_ip_port_);

  state_ = FailoverState::kSuccess;
  info("[Failover] success {} {}", slave_node_id_, node_ip_port_);
}

Status ClusterFailover::checkSlaveLag() {
  auto start_offset_status = srv_->GetSlaveReplicationOffset(node_ip_port_);
  if (!start_offset_status.IsOK()) {
    return {Status::NotOK, "Failed to get slave offset: " + start_offset_status.Msg()};
  }
  uint64_t start_offset = *start_offset_status;
  int64_t start_sampling_ms = util::GetTimeStampMS();

  // Wait 3s or half of timeout, but at least a bit to measure speed
  int64_t wait_time = std::max(100, std::min(3000, timeout_ms_ / 2));
  std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));

  auto end_offset_status = srv_->GetSlaveReplicationOffset(node_ip_port_);
  if (!end_offset_status.IsOK()) {
    return {Status::NotOK, "Failed to get slave offset: " + end_offset_status.Msg()};
  }
  uint64_t end_offset = *end_offset_status;
  int64_t end_sampling_ms = util::GetTimeStampMS();

  double elapsed_sec = (end_sampling_ms - start_sampling_ms) / 1000.0;
  if (elapsed_sec <= 0) elapsed_sec = 0.001;

  uint64_t bytes = 0;
  if (end_offset > start_offset) bytes = end_offset - start_offset;
  double speed = bytes / elapsed_sec;

  uint64_t master_seq = srv_->storage->LatestSeqNumber();
  uint64_t lag = 0;
  if (master_seq > end_offset) lag = master_seq - end_offset;

  if (lag == 0) return Status::OK();

  if (speed <= 0.1) {  // Basically 0
    return {Status::NotOK, fmt::format("Slave is not replicating (lag: {})", lag)};
  }

  double required_sec = lag / speed;
  int64_t required_ms = static_cast<int64_t>(required_sec * 1000);

  int64_t elapsed_total = end_sampling_ms - start_sampling_ms;
  int64_t remaining = timeout_ms_ - elapsed_total;

  if (required_ms > remaining) {
    return {Status::NotOK, fmt::format("Estimated catchup time {}ms > remaining time {}ms (lag: {}, speed: {:.2f}/s)",
                                       required_ms, remaining, lag, speed)};
  }

  info("[Failover] check: lag={}, speed={:.2f}/s, estimated_time={}ms, remaining={}ms", lag, speed, required_ms,
       remaining);
  return Status::OK();
}

Status ClusterFailover::checkSlaveStatus() {
  // We could try to connect, but GetSlaveReplicationOffset checks connection.
  auto offset = srv_->GetSlaveReplicationOffset(node_ip_port_);
  if (!offset.IsOK()) {
    error("[Failover] slave node {} {} not connected or not syncing", slave_node_id_, node_ip_port_);
    return {Status::NotOK, "Slave not connected or not syncing"};
  }
  info("[Failover] slave node {} {} is connected and syncing offset {}", slave_node_id_, node_ip_port_, offset.Msg());
  return Status::OK();
}

Status ClusterFailover::waitReplicationSync() {
  while (true) {
    if (util::GetTimeStampMS() - start_time_ms_ > static_cast<uint64_t>(timeout_ms_)) {
      return {Status::NotOK, "Timeout waiting for replication sync"};
    }

    auto offset_status = srv_->GetSlaveReplicationOffset(node_ip_port_);
    if (!offset_status.IsOK()) {
      return {Status::NotOK, "Failed to get slave offset: " + offset_status.Msg()};
    }

    if (*offset_status >= target_seq_) {
      return Status::OK();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

Status ClusterFailover::sendTakeoverCmd() {
  auto s = util::SockConnect(node_ip_, node_port_);
  if (!s.IsOK()) {
    return {Status::NotOK, "Failed to connect to slave: " + s.Msg()};
  }
  int fd = *s;

  std::string pass = srv_->GetConfig()->requirepass;
  if (!pass.empty()) {
    std::string auth_cmd = redis::ArrayOfBulkStrings({"AUTH", pass});
    auto s_auth = util::SockSend(fd, auth_cmd);
    if (!s_auth.IsOK()) {
      close(fd);
      return {Status::NotOK, "Failed to send AUTH: " + s_auth.Msg()};
    }
    auto s_line = util::SockReadLine(fd);
    if (!s_line.IsOK() || s_line.GetValue().substr(0, 3) != "+OK") {
      close(fd);
      return {Status::NotOK, "AUTH failed"};
    }
  }

  std::string cmd = redis::ArrayOfBulkStrings({"CLUSTERX", "TAKEOVER"});
  auto s_send = util::SockSend(fd, cmd);
  if (!s_send.IsOK()) {
    close(fd);
    return {Status::NotOK, "Failed to send TAKEOVER: " + s_send.Msg()};
  }

  auto s_resp = util::SockReadLine(fd);
  close(fd);

  if (!s_resp.IsOK()) {
    return {Status::NotOK, "Failed to read TAKEOVER response: " + s_resp.Msg()};
  }

  if (s_resp.GetValue().substr(0, 3) != "+OK") {
    return {Status::NotOK, "TAKEOVER failed: " + s_resp.GetValue()};
  }

  return Status::OK();
}

void ClusterFailover::abortFailover(const std::string &reason) {
  error("[Failover] node {} {} failover failed: {}", slave_node_id_, node_ip_port_, reason);
  state_ = FailoverState::kFailed;
}

void ClusterFailover::GetFailoverInfo(std::string *info) {
  *info = "cluster_failover_state:";
  switch (state_.load()) {
    case FailoverState::kNone:
      *info += "none";
      break;
    case FailoverState::kStarted:
      *info += "started";
      break;
    case FailoverState::kCheck:
      *info += "check_slave";
      break;
    case FailoverState::kPause:
      *info += "pause_write";
      break;
    case FailoverState::kSyncWait:
      *info += "wait_sync";
      break;
    case FailoverState::kSwitch:
      *info += "switching";
      break;
    case FailoverState::kSuccess:
      *info += "success";
      break;
    case FailoverState::kFailed:
      *info += "failed";
      break;
  }
  *info += "\r\n";
}
