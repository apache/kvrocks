#pragma once

#include <string>
#include <vector>
#include <bitset>
#include <memory>
#include <algorithm>
#include <unordered_map>
#include <set>

#include "status.h"
#include "rw_lock.h"
#include "redis_cmd.h"
#include "redis_slot.h"
#include "redis_connection.h"

enum {
  kClusterMaster    = 1,
  kClusterSlave     = 2,
  kClusterNodeIdLen = 40,
  kClusterPortIncr  = 10000,
  kClusterSlots     = HASH_SLOTS_SIZE,
};

class ClusterNode {
 public:
  explicit ClusterNode(std::string id, std::string host, int port,
        int role, std::string master_id, std::bitset<kClusterSlots> slots);
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

typedef std::unordered_map<std::string, std::shared_ptr<ClusterNode>> ClusterNodes;

class Server;

class Cluster {
 public:
  explicit Cluster(Server *svr, std::vector<std::string> binds, int port);
  Status SetClusterNodes(const std::string &nodes_str, int64_t version, bool force);
  Status GetClusterNodes(std::string *nodes_str);
  Status SetNodeId(std::string node_id);
  Status SetSlot(int slot, std::string node_id, int64_t version);
  Status SetSlotMigrated(int slot, const std::string &ip_port);
  Status SetSlotImported(int slot);
  Status GetSlotsInfo(std::vector<SlotInfo> *slot_infos);
  Status GetClusterInfo(std::string *cluster_infos);
  uint64_t GetVersion() { return version_; }
  static bool IsValidSlot(int slot) { return slot >= 0 && slot < kClusterSlots; }
  bool IsNotMaster();
  Status CanExecByMySelf(const Redis::CommandAttributes *attributes,
                         const std::vector<std::string> &cmd_tokens,
                         Redis::Connection *conn);
  void SetMasterSlaveRepl();
  Status MigrateSlot(int slot, const std::string &dst_node_id);
  Status ImportSlot(Redis::Connection *conn, int slot, int state);
  Status GetMigrateInfo(int slot, std::vector<std::string> *info);
  Status GetImportInfo(int slot, std::vector<std::string> *info);
  Status GetSlotKeys(Redis::Connection *conn, int slot, int count, std::string *output);
  std::string GetMyId() const { return myid_; }

  static bool SubCommandIsExecExclusive(const std::string &subcommand);

 private:
  std::string GenNodesDescription();
  SlotInfo GenSlotNodeInfo(int start, int end, std::shared_ptr<ClusterNode> n);
  Status ParseClusterNodes(const std::string &nodes_str, ClusterNodes *nodes,
                    std::unordered_map<int, std::string> *slots_nodes);
  Server *svr_;
  std::vector<std::string> binds_;
  int port_;
  int size_;
  int64_t version_;
  std::string  myid_;
  std::shared_ptr<ClusterNode> myself_;
  ClusterNodes nodes_;
  std::shared_ptr<ClusterNode> slots_nodes_[kClusterSlots];

  std::map<int, std::string> migrated_slots_;
  std::set<int> imported_slots_;
};
