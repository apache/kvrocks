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

#include <glog/logging.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>

#include <mutex>
#include <shared_mutex>

#include "commands/commander.h"
#include "fmt/format.h"
#ifdef ENABLE_OPENSSL
#include <event2/bufferevent_ssl.h>
#endif

#include "redis_connection.h"
#include "server.h"
#include "time_util.h"
#include "tls_util.h"
#include "worker.h"

namespace Redis {

Connection::Connection(bufferevent *bev, Worker *owner)
    : need_free_bev_(true), bev_(bev), req_(owner->svr_), owner_(owner), svr_(owner->svr_) {
  int64_t now = Util::GetTimeStamp();
  create_time_ = now;
  last_interaction_ = now;
}

Connection::~Connection() {
  if (bev_) {
    if (need_free_bev_) {
      bufferevent_free(bev_);
    } else {
      // cleanup event callbacks here to prevent using Connection's resource
      bufferevent_setcb(bev_, nullptr, nullptr, nullptr, nullptr);
    }
  }
  // unsubscribe all channels and patterns if exists
  UnsubscribeAll();
  PUnsubscribeAll();
}

std::string Connection::ToString() {
  return fmt::format("id={} addr={} fd={} name={} age={} idle={} flags={} namespace={} qbuf={} obuf={} cmd={}\n", id_,
                     addr_, bufferevent_getfd(bev_), name_, GetAge(), GetIdleTime(), GetFlags(), ns_,
                     evbuffer_get_length(Input()), evbuffer_get_length(Output()), last_cmd_);
}

void Connection::Close() {
  if (close_cb_) close_cb_(GetFD());
  owner_->FreeConnection(this);
}

void Connection::Detach() { owner_->DetachConnection(this); }

void Connection::OnRead(struct bufferevent *bev, void *ctx) {
  DLOG(INFO) << "[connection] on read: " << bufferevent_getfd(bev);
  auto conn = static_cast<Connection *>(ctx);

  conn->SetLastInteraction();
  auto s = conn->req_.Tokenize(conn->Input());
  if (!s.IsOK()) {
    conn->EnableFlag(Redis::Connection::kCloseAfterReply);
    conn->Reply(Redis::Error(s.Msg()));
    LOG(INFO) << "[connection] Failed to tokenize the request. Error: " << s.Msg();
    return;
  }

  conn->ExecuteCommands(conn->req_.GetCommands());
  if (conn->IsFlagEnabled(kCloseAsync)) {
    conn->Close();
  }
}

void Connection::OnWrite(struct bufferevent *bev, void *ctx) {
  auto conn = static_cast<Connection *>(ctx);
  if (conn->IsFlagEnabled(kCloseAfterReply) || conn->IsFlagEnabled(kCloseAsync)) {
    conn->Close();
  }
}

void Connection::OnEvent(bufferevent *bev, int16_t events, void *ctx) {
  auto conn = static_cast<Connection *>(ctx);
  if (events & BEV_EVENT_ERROR) {
    LOG(ERROR) << "[connection] Going to remove the client: " << conn->GetAddr()
               << ", while encounter error: " << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())
#ifdef ENABLE_OPENSSL
               << ", SSL Error: " << SSLError(bufferevent_get_openssl_error(bev))  // NOLINT
#endif
        ;  // NOLINT
    conn->Close();
    return;
  }

  if (events & BEV_EVENT_EOF) {
    DLOG(INFO) << "[connection] Going to remove the client: " << conn->GetAddr() << ", while closed by client";
    conn->Close();
    return;
  }

  if (events & BEV_EVENT_TIMEOUT) {
    DLOG(INFO) << "[connection] The client: " << conn->GetAddr() << "] reached timeout";
    bufferevent_enable(bev, EV_READ | EV_WRITE);
  }
}

void Connection::Reply(const std::string &msg) {
  owner_->svr_->stats_.IncrOutbondBytes(msg.size());
  Redis::Reply(bufferevent_get_output(bev_), msg);
}

void Connection::SendFile(int fd) {
  // NOTE: we don't need to close the fd, the libevent will do that
  auto output = bufferevent_get_output(bev_);
  evbuffer_add_file(output, fd, 0, -1);
}

void Connection::SetAddr(std::string ip, uint32_t port) {
  ip_ = std::move(ip);
  port_ = port;
  addr_ = ip_ + ":" + std::to_string(port_);
}

uint64_t Connection::GetAge() const { return static_cast<uint64_t>(Util::GetTimeStamp() - create_time_); }

void Connection::SetLastInteraction() { last_interaction_ = Util::GetTimeStamp(); }

uint64_t Connection::GetIdleTime() const { return static_cast<uint64_t>(Util::GetTimeStamp() - last_interaction_); }

// Currently, master connection is not handled in connection
// but in replication thread.
//
// The function will return one of the following:
//  kTypeSlave  -> Slave
//  kTypeNormal -> Normal client
//  kTypePubsub -> Client subscribed to Pub/Sub channels
uint64_t Connection::GetClientType() {
  if (IsFlagEnabled(kSlave)) return kTypeSlave;

  if (!subscribe_channels_.empty() || !subscribe_patterns_.empty()) return kTypePubsub;

  return kTypeNormal;
}

std::string Connection::GetFlags() {
  std::string flags;
  if (IsFlagEnabled(kSlave)) flags.append("S");
  if (IsFlagEnabled(kCloseAfterReply)) flags.append("c");
  if (IsFlagEnabled(kMonitor)) flags.append("M");
  if (!subscribe_channels_.empty() || !subscribe_patterns_.empty()) flags.append("P");
  if (flags.empty()) flags = "N";
  return flags;
}

void Connection::EnableFlag(Flag flag) { flags_ |= flag; }

void Connection::DisableFlag(Flag flag) { flags_ &= (~flag); }

bool Connection::IsFlagEnabled(Flag flag) { return (flags_ & flag) > 0; }

void Connection::SubscribeChannel(const std::string &channel) {
  for (const auto &chan : subscribe_channels_) {
    if (channel == chan) return;
  }

  subscribe_channels_.emplace_back(channel);
  owner_->svr_->SubscribeChannel(channel, this);
}

void Connection::UnsubscribeChannel(const std::string &channel) {
  for (auto iter = subscribe_channels_.begin(); iter != subscribe_channels_.end(); iter++) {
    if (*iter == channel) {
      subscribe_channels_.erase(iter);
      owner_->svr_->UnsubscribeChannel(channel, this);
      return;
    }
  }
}

void Connection::UnsubscribeAll(const unsubscribe_callback &reply) {
  if (subscribe_channels_.empty()) {
    if (reply) reply("", static_cast<int>(subscribe_patterns_.size()));
    return;
  }

  int removed = 0;
  for (const auto &chan : subscribe_channels_) {
    owner_->svr_->UnsubscribeChannel(chan, this);
    removed++;
    if (reply) {
      reply(chan, static_cast<int>(subscribe_channels_.size() - removed + subscribe_patterns_.size()));
    }
  }
  subscribe_channels_.clear();
}

int Connection::SubscriptionsCount() { return static_cast<int>(subscribe_channels_.size()); }

void Connection::PSubscribeChannel(const std::string &pattern) {
  for (const auto &p : subscribe_patterns_) {
    if (pattern == p) return;
  }
  subscribe_patterns_.emplace_back(pattern);
  owner_->svr_->PSubscribeChannel(pattern, this);
}

void Connection::PUnsubscribeChannel(const std::string &pattern) {
  for (auto iter = subscribe_patterns_.begin(); iter != subscribe_patterns_.end(); iter++) {
    if (*iter == pattern) {
      subscribe_patterns_.erase(iter);
      owner_->svr_->PUnsubscribeChannel(pattern, this);
      return;
    }
  }
}

void Connection::PUnsubscribeAll(const unsubscribe_callback &reply) {
  if (subscribe_patterns_.empty()) {
    if (reply) reply("", static_cast<int>(subscribe_channels_.size()));
    return;
  }

  int removed = 0;
  for (const auto &pattern : subscribe_patterns_) {
    owner_->svr_->PUnsubscribeChannel(pattern, this);
    removed++;
    if (reply) {
      reply(pattern, static_cast<int>(subscribe_patterns_.size() - removed + subscribe_channels_.size()));
    }
  }
  subscribe_patterns_.clear();
}

int Connection::PSubscriptionsCount() { return static_cast<int>(subscribe_patterns_.size()); }

bool Connection::isProfilingEnabled(const std::string &cmd) {
  auto config = svr_->GetConfig();
  if (config->profiling_sample_ratio == 0) return false;

  if (!config->profiling_sample_all_commands &&
      config->profiling_sample_commands.find(cmd) == config->profiling_sample_commands.end()) {
    return false;
  }

  if (config->profiling_sample_ratio == 100 || std::rand() % 100 <= config->profiling_sample_ratio) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();
    return true;
  }

  return false;
}

void Connection::recordProfilingSampleIfNeed(const std::string &cmd, uint64_t duration) {
  int threshold = svr_->GetConfig()->profiling_sample_record_threshold_ms;
  if (threshold > 0 && static_cast<int>(duration / 1000) < threshold) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    return;
  }

  std::string perf_context = rocksdb::get_perf_context()->ToString(true);
  std::string iostats_context = rocksdb::get_iostats_context()->ToString(true);
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  if (perf_context.empty()) return;  // request without db operation

  auto entry = std::unique_ptr<PerfEntry>();
  entry->cmd_name = cmd;
  entry->duration = duration;
  entry->iostats_context = std::move(iostats_context);
  entry->perf_context = std::move(perf_context);
  svr_->GetPerfLog()->PushEntry(std::move(entry));
}

void Connection::ExecuteCommands(std::deque<CommandTokens> *to_process_cmds) {
  Config *config = svr_->GetConfig();
  std::string reply, password = config->requirepass;

  while (!to_process_cmds->empty()) {
    auto cmd_tokens = to_process_cmds->front();
    to_process_cmds->pop_front();

    if (IsFlagEnabled(Redis::Connection::kCloseAfterReply) && !IsFlagEnabled(Connection::kMultiExec)) break;

    auto s = svr_->LookupAndCreateCommand(cmd_tokens.front(), &current_cmd_);
    if (!s.IsOK()) {
      if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
      Reply(Redis::Error("ERR unknown command " + cmd_tokens.front()));
      continue;
    }

    if (GetNamespace().empty()) {
      if (!password.empty() && Util::ToLower(cmd_tokens.front()) != "auth" &&
          Util::ToLower(cmd_tokens.front()) != "hello") {
        Reply(Redis::Error("NOAUTH Authentication required."));
        continue;
      }

      if (password.empty()) {
        BecomeAdmin();
        SetNamespace(kDefaultNamespace);
      }
    }

    const auto attributes = current_cmd_->GetAttributes();
    auto cmd_name = attributes->name;

    std::shared_lock<std::shared_mutex> concurrency;  // Allow concurrency
    std::unique_lock<std::shared_mutex> exclusivity;  // Need exclusivity
    // If the command needs to process exclusively, we need to get 'ExclusivityGuard'
    // that can guarantee other threads can't come into critical zone, such as DEBUG,
    // CLUSTER subcommand, CONFIG SET, MULTI, LUA (in the immediate future).
    // Otherwise, we just use 'ConcurrencyGuard' to allow all workers to execute commands at the same time.
    if (IsFlagEnabled(Connection::kMultiExec) && attributes->name != "exec") {
      // No lock guard, because 'exec' command has acquired 'WorkExclusivityGuard'
    } else if (attributes->is_exclusive() ||
               (cmd_name == "config" && cmd_tokens.size() == 2 && !strcasecmp(cmd_tokens[1].c_str(), "set")) ||
               (config->cluster_enabled && (cmd_name == "clusterx" || cmd_name == "cluster") &&
                cmd_tokens.size() >= 2 && Cluster::SubCommandIsExecExclusive(cmd_tokens[1]))) {
      exclusivity = svr_->WorkExclusivityGuard();

      // When executing lua script commands that have "exclusive" attribute, we need to know current connection,
      // but we should set current connection after acquiring the WorkExclusivityGuard to make it thread-safe
      svr_->SetCurrentConnection(this);
    } else {
      concurrency = svr_->WorkConcurrencyGuard();
    }

    if (attributes->flags & kCmdROScript) {
      // if executing read only lua script commands, set current connection.
      svr_->SetCurrentConnection(this);
    }

    if (svr_->IsLoading() && !attributes->is_ok_loading()) {
      Reply(Redis::Error("LOADING kvrocks is restoring the db from backup"));
      if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
      continue;
    }

    int arity = attributes->arity;
    int tokens = static_cast<int>(cmd_tokens.size());
    if ((arity > 0 && tokens != arity) || (arity < 0 && tokens < -arity)) {
      if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
      Reply(Redis::Error("ERR wrong number of arguments"));
      continue;
    }

    current_cmd_->SetArgs(cmd_tokens);
    s = current_cmd_->Parse();
    if (!s.IsOK()) {
      if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
      Reply(Redis::Error("ERR " + s.Msg()));
      continue;
    }

    if (IsFlagEnabled(Connection::kMultiExec) && attributes->is_no_multi()) {
      std::string no_multi_err = "Err Can't execute " + attributes->name + " in MULTI";
      Reply(Redis::Error(no_multi_err));
      multi_error_ = true;
      continue;
    }

    if (config->cluster_enabled) {
      s = svr_->cluster_->CanExecByMySelf(attributes, cmd_tokens, this);
      if (!s.IsOK()) {
        if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
        Reply(Redis::Error(s.Msg()));
        continue;
      }
    }

    // We don't execute commands, but queue them, ant then execute in EXEC command
    if (IsFlagEnabled(Connection::kMultiExec) && !in_exec_ && !attributes->is_multi()) {
      multi_cmds_.emplace_back(cmd_tokens);
      Reply(Redis::SimpleString("QUEUED"));
      continue;
    }

    if (config->slave_readonly && svr_->IsSlave() && attributes->is_write()) {
      Reply(Redis::Error("READONLY You can't write against a read only slave."));
      continue;
    }

    if (!config->slave_serve_stale_data && svr_->IsSlave() && cmd_name != "info" && cmd_name != "slaveof" &&
        svr_->GetReplicationState() != kReplConnected) {
      Reply(
          Redis::Error("MASTERDOWN Link with MASTER is down "
                       "and slave-serve-stale-data is set to 'no'."));
      continue;
    }

    SetLastCmd(cmd_name);
    svr_->stats_.IncrCalls(cmd_name);

    auto start = std::chrono::high_resolution_clock::now();
    bool is_profiling = isProfilingEnabled(cmd_name);
    s = current_cmd_->Execute(svr_, this, &reply);
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    if (is_profiling) recordProfilingSampleIfNeed(cmd_name, duration);

    svr_->SlowlogPushEntryIfNeeded(&cmd_tokens, duration);
    svr_->stats_.IncrLatency(static_cast<uint64_t>(duration), cmd_name);
    svr_->FeedMonitorConns(this, cmd_tokens);

    // Break the execution loop when occurring the blocking command like BLPOP or BRPOP,
    // it will suspend the connection and wait for the wakeup signal.
    if (s.Is<Status::BlockingCmd>()) {
      break;
    }

    // Reply for MULTI
    if (!s.IsOK()) {
      Reply(Redis::Error("ERR " + s.Msg()));
      continue;
    }

    svr_->UpdateWatchedKeysFromArgs(cmd_tokens, *attributes);

    if (!reply.empty()) Reply(reply);
    reply.clear();
  }
}

void Connection::ResetMultiExec() {
  in_exec_ = false;
  multi_error_ = false;
  multi_cmds_.clear();
  DisableFlag(Connection::kMultiExec);
}

}  // namespace Redis
