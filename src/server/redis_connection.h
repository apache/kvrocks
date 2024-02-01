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

#pragma once

#include <event2/buffer.h>

#include <deque>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "commands/commander.h"
#include "event_util.h"
#include "redis_request.h"

class Worker;

namespace redis {

class Connection : public EvbufCallbackBase<Connection> {
 public:
  enum Flag {
    kSlave = 1 << 4,
    kMonitor = 1 << 5,
    kCloseAfterReply = 1 << 6,
    kCloseAsync = 1 << 7,
    kMultiExec = 1 << 8,
  };

  explicit Connection(bufferevent *bev, Worker *owner);
  ~Connection();

  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;

  void Close();
  void Detach();
  void OnRead(bufferevent *bev);
  void OnWrite(bufferevent *bev);
  void OnEvent(bufferevent *bev, int16_t events);
  void SendFile(int fd);
  std::string ToString();

  void Reply(const std::string &msg);
  RESP GetProtocolVersion() const { return protocol_version_; }
  void SetProtocolVersion(RESP version) { protocol_version_ = version; }
  std::string Bool(bool b) const;
  std::string BigNumber(const std::string &n) const {
    return protocol_version_ == RESP::v3 ? "(" + n + CRLF : BulkString(n);
  }
  std::string Double(double d) const {
    return protocol_version_ == RESP::v3 ? "," + util::Float2String(d) + CRLF : BulkString(util::Float2String(d));
  }
  std::string NilString() const { return redis::NilString(protocol_version_); }
  std::string NilArray() const { return protocol_version_ == RESP::v3 ? "_" CRLF : "*-1" CRLF; }
  std::string MultiBulkString(const std::vector<std::string> &values) const;
  std::string MultiBulkString(const std::vector<std::string> &values,
                              const std::vector<rocksdb::Status> &statuses) const;
  template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
  std::string HeaderOfSet(T len) const {
    return protocol_version_ == RESP::v3 ? "~" + std::to_string(len) + CRLF : MultiLen(len);
  }
  std::string SetOfBulkStrings(const std::vector<std::string> &elems) const;
  template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
  std::string HeaderOfMap(T len) const {
    return protocol_version_ == RESP::v3 ? "%" + std::to_string(len) + CRLF : MultiLen(len * 2);
  }
  std::string MapOfBulkStrings(const std::vector<std::string> &elems) const;

  using UnsubscribeCallback = std::function<void(std::string, int)>;
  void SubscribeChannel(const std::string &channel);
  void UnsubscribeChannel(const std::string &channel);
  void UnsubscribeAll(const UnsubscribeCallback &reply = nullptr);
  int SubscriptionsCount();
  void PSubscribeChannel(const std::string &pattern);
  void PUnsubscribeChannel(const std::string &pattern);
  void PUnsubscribeAll(const UnsubscribeCallback &reply = nullptr);
  int PSubscriptionsCount();
  void SSubscribeChannel(const std::string &channel, uint16_t slot);
  void SUnsubscribeChannel(const std::string &channel, uint16_t slot);
  void SUnsubscribeAll(const UnsubscribeCallback &reply = nullptr);
  int SSubscriptionsCount();

  uint64_t GetAge() const;
  uint64_t GetIdleTime() const;
  void SetLastInteraction();
  std::string GetFlags() const;
  void EnableFlag(Flag flag);
  void DisableFlag(Flag flag);
  bool IsFlagEnabled(Flag flag) const;

  uint64_t GetID() const { return id_; }
  void SetID(uint64_t id) { id_ = id; }
  std::string GetName() const { return name_; }
  void SetName(std::string name) { name_ = std::move(name); }
  std::string GetAddr() const { return addr_; }
  void SetAddr(std::string ip, uint32_t port);
  void SetLastCmd(std::string cmd) { last_cmd_ = std::move(cmd); }
  std::string GetIP() const { return ip_; }
  uint32_t GetPort() const { return port_; }
  void SetListeningPort(int port) { listening_port_ = port; }
  int GetListeningPort() const { return listening_port_; }
  void SetAnnounceIP(std::string ip) { announce_ip_ = std::move(ip); }
  std::string GetAnnounceIP() const { return !announce_ip_.empty() ? announce_ip_ : ip_; }
  uint32_t GetAnnouncePort() const { return listening_port_ != 0 ? listening_port_ : port_; }
  std::string GetAnnounceAddr() const { return GetAnnounceIP() + ":" + std::to_string(GetAnnouncePort()); }
  uint64_t GetClientType() const;
  Server *GetServer() { return srv_; }

  bool IsAdmin() const { return is_admin_; }
  void BecomeAdmin() { is_admin_ = true; }
  void BecomeUser() { is_admin_ = false; }
  std::string GetNamespace() const { return ns_; }
  void SetNamespace(std::string ns) { ns_ = std::move(ns); }

  void NeedFreeBufferEvent(bool need_free = true) { need_free_bev_ = need_free; }
  void NeedNotFreeBufferEvent() { NeedFreeBufferEvent(false); }
  bool IsNeedFreeBufferEvent() const { return need_free_bev_; }

  Worker *Owner() { return owner_; }
  void SetOwner(Worker *new_owner) { owner_ = new_owner; };
  int GetFD() { return bufferevent_getfd(bev_); }
  evbuffer *Input() { return bufferevent_get_input(bev_); }
  evbuffer *Output() { return bufferevent_get_output(bev_); }
  bufferevent *GetBufferEvent() { return bev_; }
  void ExecuteCommands(std::deque<CommandTokens> *to_process_cmds);
  bool IsProfilingEnabled(const std::string &cmd);
  void RecordProfilingSampleIfNeed(const std::string &cmd, uint64_t duration);
  void SetImporting() { importing_ = true; }
  bool IsImporting() const { return importing_; }
  bool CanMigrate() const;

  // Multi exec
  void SetInExec() { in_exec_ = true; }
  bool IsInExec() const { return in_exec_; }
  bool IsMultiError() const { return multi_error_; }
  void ResetMultiExec();
  std::deque<redis::CommandTokens> *GetMultiExecCommands() { return &multi_cmds_; }

  std::function<void(int)> close_cb = nullptr;

  std::set<std::string> watched_keys;
  std::atomic<bool> watched_keys_modified = false;

 private:
  uint64_t id_ = 0;
  std::atomic<int> flags_ = 0;
  std::string ns_;
  std::string name_;
  std::string ip_;
  std::string announce_ip_;
  uint32_t port_ = 0;
  std::string addr_;
  int listening_port_ = 0;
  bool is_admin_ = false;
  bool need_free_bev_ = true;
  std::string last_cmd_;
  int64_t create_time_;
  int64_t last_interaction_;

  bufferevent *bev_;
  Request req_;
  Worker *owner_;
  std::unique_ptr<Commander> saved_current_command_;

  std::vector<std::string> subscribe_channels_;
  std::vector<std::string> subscribe_patterns_;
  std::vector<std::string> subscribe_shard_channels_;

  Server *srv_;
  bool in_exec_ = false;
  bool multi_error_ = false;
  std::atomic<bool> is_running_ = false;
  std::deque<redis::CommandTokens> multi_cmds_;

  bool importing_ = false;
  RESP protocol_version_ = RESP::v2;
};

}  // namespace redis
