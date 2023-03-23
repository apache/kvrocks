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

#include <algorithm>
#include <bitset>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "commands/commander.h"
#include "redis_slot.h"
#include "server/redis_connection.h"
#include "status.h"

enum {
  kClusterMaster = 1,
  kClusterSlave = 2,
  kClusterNodeIdLen = 40,
  kClusterPortIncr = 10000,
  kClusterSlots = HASH_SLOTS_SIZE,
};

class ClusterNode {
 public:
  explicit ClusterNode(std::string id, std::string host, int port, int role, std::string master_id,
                       std::bitset<kClusterSlots> slots);
  std::string id_;
  std::string host_;
  int port_;
  int role_;
  std::string master_id_;
  std::string slots_info_;
  std::bitset<kClusterSlots> slots_;
  std::vector<std::string> replicas;
  int importing_slot_ = -1;
};

struct SlotInfo {
  int start;
  int end;
  struct NodeInfo {
    std::string host;
    int port;
    std::string id;
  };
  std::vector<NodeInfo> nodes;
};

using ClusterNodes = std::unordered_map<std::string, std::shared_ptr<ClusterNode>>;

class Server;

class Cluster {
 public:
  explicit Cluster(Server *svr, std::vector<std::string> binds, int port);
  Status SetClusterNodes(const std::string &nodes_str, int64_t version, bool force);
  Status GetClusterNodes(std::string *nodes_str);
  Status SetNodeId(const std::string &node_id);
  Status SetSlot(int slot, const std::string &node_id, int64_t version);
  Status SetSlotMigrated(int slot, const std::string &ip_port);
  Status SetSlotImported(int slot);
  Status GetSlotsInfo(std::vector<SlotInfo> *slot_infos);
  Status GetClusterInfo(std::string *cluster_infos);
  int64_t GetVersion() const { return version_; }
  static bool IsValidSlot(int slot) { return slot >= 0 && slot < kClusterSlots; }
  bool IsNotMaster();
  bool IsWriteForbiddenSlot(int slot);
  Status CanExecByMySelf(const Redis::CommandAttributes *attributes, const std::vector<std::string> &cmd_tokens,
                         Redis::Connection *conn);
  Status SetMasterSlaveRepl();
  Status MigrateSlot(int slot, const std::string &dst_node_id);
  Status ImportSlot(Redis::Connection *conn, int slot, int state);
  std::string GetMyId() const { return myid_; }
  Status DumpClusterNodes(const std::string &file);
  Status LoadClusterNodes(const std::string &file_path);

  static bool SubCommandIsExecExclusive(const std::string &subcommand);

 private:
  std::string GenNodesDescription();
  std::string GenNodesInfo();
  void UpdateSlotsInfo();
  SlotInfo GenSlotNodeInfo(int start, int end, const std::shared_ptr<ClusterNode> &n);
  static Status ParseClusterNodes(const std::string &nodes_str, ClusterNodes *nodes,
                                  std::unordered_map<int, std::string> *slots_nodes);
  Server *svr_;
  std::vector<std::string> binds_;
  int port_;
  int size_;
  int64_t version_;
  std::string myid_;
  std::shared_ptr<ClusterNode> myself_;
  ClusterNodes nodes_;
  std::shared_ptr<ClusterNode> slots_nodes_[kClusterSlots];

  std::map<int, std::string> migrated_slots_;
  std::set<int> imported_slots_;
};
