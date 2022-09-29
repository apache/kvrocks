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

#include <cassert>
#include <cstring>
#include <algorithm>
#include <memory>

#include "util.h"
#include "server.h"
#include "cluster.h"
#include "redis_cmd.h"
#include "replication.h"

const char *errInvalidNodeID = "Invalid cluster node id";
const char *errInvalidSlotID = "Invalid slot id";
const char *errSlotOutOfRange = "Slot is out of range";
const char *errInvalidClusterVersion = "Invalid cluster version";
const char *errSlotOverlapped = "Slot distribution is overlapped";
const char *errNoMasterNode = "The node isn't a master";
const char *errClusterNoInitialized = "CLUSTERDOWN The cluster is not initialized";
const char *errInvalidClusterNodeInfo = "Invalid cluster nodes info";
const char *errInvalidImportState = "Invalid import state";

ClusterNode::ClusterNode(std::string id, std::string host, int port,
    int role, std::string master_id, std::bitset<kClusterSlots> slots):
    id_(id), host_(host), port_(port), role_(role),
    master_id_(master_id), slots_(slots) { }

Cluster::Cluster(Server *svr, std::vector<std::string> binds, int port) :
    svr_(svr), binds_(binds), port_(port), size_(0), version_(-1), myself_(nullptr) {
  for (unsigned i = 0; i < kClusterSlots; i++) {
    slots_nodes_[i] = nullptr;
  }
}

// We access cluster without lock, actually we guarantee data-safe by work threads
// ReadWriteLockGuard, CLUSTER command doesn't have 'exclusive' attribute, i.e.
// CLUSTER command can be executed concurrently, but some subcommand may change
// cluster data, so these commands should be executed exclusively, and ReadWriteLock
// also can guarantee accessing data is safe.
bool Cluster::SubCommandIsExecExclusive(const std::string &subcommand) {
  if (strcasecmp("setnodes", subcommand.c_str()) == 0) {
    return true;
  } else if (strcasecmp("setnodeid", subcommand.c_str()) == 0) {
    return true;
  }  else if (strcasecmp("setslot", subcommand.c_str()) == 0) {
    return true;
  } else if (strcasecmp("import", subcommand.c_str()) == 0) {
    return true;
  }
  return false;
}

Status Cluster::SetNodeId(std::string node_id) {
  if (node_id.size() != kClusterNodeIdLen) {
    return Status(Status::ClusterInvalidInfo, errInvalidNodeID);
  }

  myid_ = node_id;
  // Already has cluster topology
  if (version_ >= 0 && nodes_.find(node_id) != nodes_.end()) {
    myself_ = nodes_[myid_];
  } else {
    myself_ = nullptr;
  }

  // Set replication relationship
  if (myself_ != nullptr) SetMasterSlaveRepl();

  return Status::OK();
}

// Set the slot to the node if new version is current version +1. It is useful
// when we scale cluster avoid too many big messages, since we only update one
// slot distribution and there are 16384 slot in our design.
//
// The reason why the new version MUST be +1 of current version is that,
// the command changes topology based on specific topology (also means specific
// version), we must guarantee current topology is exactly expected, otherwise,
// this update may make topology corrupt, so base topology version is very important.
// This is different with CLUSTERX SETNODES commands because it uses new version
// topology to cover current version, it allows kvrocks nodes lost some topology
// updates since of network failure, it is state instead of operation.
Status Cluster::SetSlot(int slot, std::string node_id, int64_t new_version) {
  // Parameters check
  if (new_version <= 0 || new_version != version_ + 1) {
    return Status(Status::NotOK, errInvalidClusterVersion);
  }
  if (!IsValidSlot(slot)) {
    return Status(Status::NotOK, errInvalidSlotID);
  }
  if (node_id.size() != kClusterNodeIdLen)  {
    return Status(Status::NotOK, errInvalidNodeID);
  }

  // Get the node which we want to assign a slot into it
  std::shared_ptr<ClusterNode> to_assign_node = nodes_[node_id];
  if (to_assign_node == nullptr) {
    return Status(Status::NotOK, "No this node in the cluster");
  }
  if (to_assign_node->role_ != kClusterMaster) {
    return Status(Status::NotOK, errNoMasterNode);
  }

  // Update version
  version_ = new_version;

  // Update topology
  //  1. Remove the slot from old node if existing
  //  2. Add the slot into to-assign node
  //  3. Update the map of slots to nodes.
  std::shared_ptr<ClusterNode> old_node = slots_nodes_[slot];
  if (old_node != nullptr) {
    old_node->slots_[slot] = 0;
  }
  to_assign_node->slots_[slot] = 1;
  slots_nodes_[slot] = to_assign_node;

  // Clear data of migrated slot or record of imported slot
  if (old_node == myself_ && old_node != to_assign_node) {
    // If slot is migrated from this node
    if (migrated_slots_.count(slot)) {
      svr_->slot_migrate_->ClearKeysOfSlot(kDefaultNamespace, slot);
      migrated_slots_.erase(slot);
    }
    // If slot is imported into this node
    if (imported_slots_.count(slot)) {
      imported_slots_.erase(slot);
    }
  }

  return Status::OK();
}

// cluster setnodes $all_nodes_info $version $force
// one line of $all_nodes: $node_id $host $port $role $master_node_id $slot_range
Status Cluster::SetClusterNodes(const std::string &nodes_str, int64_t version, bool force) {
  if (version < 0) return Status(Status::NotOK, errInvalidClusterVersion);

  if (force == false) {
    // Low version wants to reset current version
    if (version_ > version) {
      return Status(Status::NotOK, errInvalidClusterVersion);
    }
    // The same version, it is not needed to update
    if (version_ == version) return Status::OK();
  }

  ClusterNodes nodes;
  std::unordered_map<int, std::string> slots_nodes;
  Status s = ParseClusterNodes(nodes_str, &nodes, &slots_nodes);
  if (!s.IsOK()) return s;

  // Update version and cluster topology
  version_ = version;
  nodes_ = nodes;
  size_ = 0;

  // Update slots to nodes
  for (const auto &n : slots_nodes) {
    slots_nodes_[n.first] = nodes_[n.second];
  }

  // Update replicas info and size
  for (auto &n : nodes_) {
    if (n.second->role_ == kClusterSlave) {
      if (nodes_.find(n.second->master_id_) != nodes_.end()) {
        nodes_[n.second->master_id_]->replicas.push_back(n.first);
      }
    }
    if (n.second->role_ == kClusterMaster && n.second->slots_.count() > 0) {
      size_++;
    }
  }

  // Find myself
  if (myid_.empty() || force) {
    for (auto &n : nodes_) {
      if (n.second->port_ == port_ &&
          std::find(binds_.begin(), binds_.end(), n.second->host_)
            != binds_.end()) {
        myid_ = n.first;
        break;
      }
    }
  }
  myself_ = nullptr;
  if (!myid_.empty() && nodes_.find(myid_) != nodes_.end()) {
    myself_ = nodes_[myid_];
  }

  // Set replication relationship
  if (myself_ != nullptr) SetMasterSlaveRepl();

  // Clear data of migrated slots
  if (!migrated_slots_.empty()) {
    for (auto &it : migrated_slots_) {
      if (slots_nodes_[it.first] != myself_) {
        svr_->slot_migrate_->ClearKeysOfSlot(kDefaultNamespace, it.first);
      }
    }
  }
  // Clear migrated and imported slot info
  migrated_slots_.clear();
  imported_slots_.clear();

  return Status::OK();
}

// Set replication relationship by cluster topology setting
void Cluster::SetMasterSlaveRepl() {
  if (svr_ == nullptr) return;
  if (myself_ == nullptr) return;

  if (myself_->role_ == kClusterMaster) {
    // Master mode
    svr_->RemoveMaster();
    LOG(INFO) << "MASTER MODE enabled by cluster topology setting";
  } else if (nodes_.find(myself_->master_id_) != nodes_.end()) {
    // Slave mode and master node is existing
    std::shared_ptr<ClusterNode> master = nodes_[myself_->master_id_];
    Status s = svr_->AddMaster(master->host_, master->port_, 0);
    if (s.IsOK()) {
      LOG(INFO) << "SLAVE OF " << master->host_ << ":" << master->port_
                << " enabled by cluster topology setting";
    } else {
      LOG(WARNING) << "SLAVE OF " << master->host_ << ":" << master->port_
                    << " enabled by cluster topology setting, encounter error: "
                    << s.Msg();
    }
  }
}

bool Cluster::IsNotMaster() {
  return myself_ == nullptr || myself_->role_ != kClusterMaster || svr_->IsSlave();
}

Status Cluster::SetSlotMigrated(int slot, const std::string &ip_port) {
  if (!IsValidSlot(slot)) {
    return Status(Status::NotOK, errSlotOutOfRange);
  }
  // It is called by slot-migrating thread which is an asynchronous thread.
  // Therefore, it should be locked when a record is added to 'migrated_slots_'
  // which will be accessed when executing commands.
  auto exclusivity = svr_->WorkExclusivityGuard();
  migrated_slots_[slot] = ip_port;
  return Status::OK();
}

Status Cluster::SetSlotImported(int slot) {
  if (!IsValidSlot(slot)) {
    return Status(Status::NotOK, errSlotOutOfRange);
  }
  // It is called by command 'cluster import'. When executing the command, the
  // exclusive lock has been locked. Therefore, it can't be locked again.
  imported_slots_.insert(slot);
  return Status::OK();
}

Status Cluster::MigrateSlot(int slot, const std::string &dst_node_id) {
  if (nodes_.find(dst_node_id) == nodes_.end()) {
    return Status(Status::NotOK, "Can't find the destination node id");
  }
  if (!IsValidSlot(slot)) {
    return Status(Status::NotOK, errSlotOutOfRange);
  }
  if (slots_nodes_[slot] != myself_) {
    return Status(Status::NotOK, "Can't migrate slot which doesn't belong to me");
  }
  if (IsNotMaster()) {
    return Status(Status::NotOK, "Slave can't migrate slot");
  }
  if (nodes_[dst_node_id]->role_ != kClusterMaster) {
    return Status(Status::NotOK, "Can't migrate slot to a slave");
  }
  if (nodes_[dst_node_id] == myself_) {
    return Status(Status::NotOK, "Can't migrate slot to myself");
  }

  const auto dst = nodes_[dst_node_id];
  Status s = svr_->slot_migrate_->MigrateStart(svr_, dst_node_id, dst->host_,
                                               dst->port_, slot,
                                               svr_->GetConfig()->migrate_speed,
                                               svr_->GetConfig()->pipeline_size,
                                               svr_->GetConfig()->sequence_gap);
  return s;
}

Status Cluster::ImportSlot(Redis::Connection *conn, int slot, int state) {
  if (IsNotMaster()) {
    return Status(Status::NotOK, "Slave can't import slot");
  }
  if (!IsValidSlot(slot)) {
    return Status(Status::NotOK, errSlotOutOfRange);
  }

  switch (state) {
    case kImportStart:
      if (!svr_->slot_import_->Start(conn->GetFD(), slot)) {
        return Status(Status::NotOK, "Can't start importing slot " + std::to_string(slot));
      }
      // Set link importing
      conn->SetImporting();
      myself_->importing_slot_ = slot;
      // Set link error callback
      conn->close_cb_ = std::bind(&SlotImport::StopForLinkError, svr_->slot_import_, conn->GetFD());
      // Stop forbidding writing slot to accept write commands
      if (slot == svr_->slot_migrate_->GetForbiddenSlot()) svr_->slot_migrate_->ReleaseForbiddenSlot();
      LOG(INFO) << "[import] Start importing slot " << slot;
      break;
    case kImportSuccess:
      if (!svr_->slot_import_->Success(slot)) {
        LOG(ERROR) << "[import] Failed to set slot importing success, maybe slot is wrong"
                  << ", received slot: " << slot
                  << ", current slot: " << svr_->slot_import_->GetSlot();
        return Status(Status::NotOK, "Failed to set slot " + std::to_string(slot) + " importing success");
      }
      LOG(INFO) << "[import] Succeed to import slot " << slot;
      break;
    case kImportFailed:
      if (!svr_->slot_import_->Fail(slot)) {
        LOG(ERROR) << "[import] Failed to set slot importing error, maybe slot is wrong"
                  << ", received slot: " << slot
                  << ", current slot: " << svr_->slot_import_->GetSlot();
        return Status(Status::NotOK, "Failed to set slot " + std::to_string(slot) + " importing error");
      }
      LOG(INFO) << "[import] Failed to import slot " << slot;
      break;
    default:
      return Status(Status::NotOK, errInvalidImportState);
  }
  return Status::OK();
}

Status Cluster::GetClusterInfo(std::string *cluster_infos) {
  if (version_ < 0) {
    return Status(Status::ClusterDown, errClusterNoInitialized);
  }
  cluster_infos->clear();

  int ok_slot = 0;
  for (int i = 0; i < kClusterSlots; i++) {
    if (slots_nodes_[i] != nullptr) ok_slot++;
  }

  *cluster_infos =
    "cluster_state:ok\r\n"
    "cluster_slots_assigned:" + std::to_string(ok_slot) + "\r\n"
    "cluster_slots_ok:" + std::to_string(ok_slot) + "\r\n"
    "cluster_slots_pfail:0\r\n"
    "cluster_slots_fail:0\r\n"
    "cluster_known_nodes:" + std::to_string(nodes_.size()) +"\r\n"
    "cluster_size:" + std::to_string(size_) + "\r\n"
    "cluster_current_epoch:" +  std::to_string(version_) + "\r\n"
    "cluster_my_epoch:" +  std::to_string(version_) + "\r\n";

  if (myself_ != nullptr && myself_->role_ == kClusterMaster && !svr_->IsSlave()) {
    // Get migrating status
    std::string migrate_infos;
    svr_->slot_migrate_->GetMigrateInfo(&migrate_infos);
    *cluster_infos += migrate_infos;

    // Get importing status
    std::string import_infos;
    svr_->slot_import_->GetImportInfo(&import_infos);
    *cluster_infos += import_infos;
  }

  return Status::OK();
}

// Format: 1) 1) start slot
//            2) end slot
//            3) 1) master IP
//               2) master port
//               3) node ID
//            4) 1) replica IP
//               2) replica port
//               3) node ID
//          ... continued until done
Status Cluster::GetSlotsInfo(std::vector<SlotInfo> *slots_infos) {
  if (version_ < 0) {
    return Status(Status::ClusterDown, errClusterNoInitialized);
  }
  slots_infos->clear();

  int start = -1;
  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      slots_infos->emplace_back(GenSlotNodeInfo(start, i-1, n));
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }
  return Status::OK();
}

SlotInfo Cluster::GenSlotNodeInfo(int start, int end, std::shared_ptr<ClusterNode> n) {
  std::vector<SlotInfo::NodeInfo> vn;
  vn.push_back({n->host_, n->port_, n->id_});  // itself

  for (const auto &id : n->replicas) {         // replicas
    if (nodes_.find(id) == nodes_.end()) continue;
    vn.push_back({nodes_[id]->host_, nodes_[id]->port_, nodes_[id]->id_});
  }
  return {start, end, vn};
}

// $node $host:$port@$cport $role $master_id/$- $ping_sent $ping_received
// $version $connected $slot_range
Status Cluster::GetClusterNodes(std::string *nodes_str) {
  if (version_ < 0) {
    return Status(Status::ClusterDown, errClusterNoInitialized);
  }

  *nodes_str = GenNodesDescription();
  return Status::OK();
}

std::string Cluster::GenNodesDescription() {
  // Generate slots info firstly
  int start = -1;
  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      if (start == i-1) {
        n->slots_info_ += std::to_string(start) + " ";
      } else {
        n->slots_info_ += std::to_string(start)+ "-" + std::to_string(i-1) + " ";
      }
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }

  std::string nodes_desc;
  for (const auto &item : nodes_) {
    const std::shared_ptr<ClusterNode> n = item.second;

    std::string node_str;
    // ID, host, port
    node_str.append(n->id_ + " ");
    node_str.append(n->host_ + ":" + std::to_string(n->port_) +
        "@" + std::to_string(n->port_+kClusterPortIncr) + " ");

    // Flags
    if (n->id_ == myid_) node_str.append("myself,");
    if (n->role_ == kClusterMaster) {
      node_str.append("master - ");
    } else {
      node_str.append("slave " + n->master_id_ + " ");
    }

    // Ping sent, pong received, config epoch, link status
    node_str.append(std::to_string(std::time(nullptr)*1000-1) + " " +
      std::to_string(std::time(nullptr)*1000) + " " +
      std::to_string(version_) + " " + "connected");

    // Slots
    if (n->slots_info_.size() > 0) n->slots_info_.pop_back();  // Trim space
    if (n->role_ == kClusterMaster && n->slots_info_.size() > 0) {
      node_str.append(" " + n->slots_info_);
    }
    n->slots_info_.clear();  // Reset

    nodes_desc.append(node_str + "\n");
  }
  return nodes_desc;
}

Status Cluster::ParseClusterNodes(const std::string &nodes_str, ClusterNodes *nodes,
                                  std::unordered_map<int, std::string> *slots_nodes) {
  std::vector<std::string> nodes_info = Util::Split(nodes_str, "\n");
  if (nodes_info.size() == 0) {
    return Status(Status::ClusterInvalidInfo, errInvalidClusterNodeInfo);
  }
  nodes->clear();

  // Parse all nodes
  for (const auto &node_str : nodes_info) {
    std::vector<std::string> fields = Util::Split(node_str, " ");
    if (fields.size() < 5) {
      return Status(Status::ClusterInvalidInfo, errInvalidClusterNodeInfo);
    }

    // 1) node id
    if (fields[0].size() != kClusterNodeIdLen) {
      return Status(Status::ClusterInvalidInfo, errInvalidNodeID);
    }
    std::string id = fields[0];

    // 2) host, TODO(@shooterit): check host is valid
    std::string host = fields[1];

    // 3) port
    int port = std::atoi(fields[2].c_str());
    if (port <= 0 || port >= (65535-kClusterPortIncr)) {
      return Status(Status::ClusterInvalidInfo, "Invalid cluster node port");
    }

    // 4) role
    int role;
    if (strcasecmp(fields[3].c_str(), "master") == 0) {
      role = kClusterMaster;
    } else if (strcasecmp(fields[3].c_str(), "slave") == 0 ||
               strcasecmp(fields[3].c_str(), "replica") == 0) {
      role = kClusterSlave;
    } else {
      return Status(Status::ClusterInvalidInfo, "Invalid cluster node role");
    }

    // 5) master id
    std::string master_id = fields[4];
    if ((role == kClusterMaster && master_id != "-") ||
        (role == kClusterSlave && master_id.size() != kClusterNodeIdLen)) {
      return Status(Status::ClusterInvalidInfo, errInvalidNodeID);
    }

    std::bitset<kClusterSlots> slots;
    if (role == kClusterSlave) {
      if (fields.size() != 5) {
        return Status(Status::ClusterInvalidInfo, errInvalidClusterNodeInfo);
      } else {
        // Create slave node
        (*nodes)[id] = Util::MakeShared<ClusterNode>(
          id, host, port, role, master_id, slots);
        continue;
      }
    }

    // 6) slot info
    for (unsigned i = 5; i < fields.size(); i++) {
      int start, stop;
      std::vector<std::string> ranges = Util::Split(fields[i], "-");
      if (ranges.size() == 1) {
        start = std::atoi(ranges[0].c_str());
        if (IsValidSlot(start) == false) {
          return Status(Status::ClusterInvalidInfo, errSlotOutOfRange);
        }
        slots.set(start, 1);
        if (role == kClusterMaster) {
          if (slots_nodes->find(start) != slots_nodes->end()) {
            return Status(Status::ClusterInvalidInfo, errSlotOverlapped);
          } else {
            (*slots_nodes)[start] = id;
          }
        }
      } else if (ranges.size() == 2) {
        start = std::atoi(ranges[0].c_str());
        stop = std::atoi(ranges[1].c_str());
        if (start >= stop || start < 0 || stop >= kClusterSlots) {
          return Status(Status::ClusterInvalidInfo, errSlotOutOfRange);
        }
        for (int j = start; j <= stop; j++) {
          slots.set(j, 1);
          if (role == kClusterMaster) {
            if (slots_nodes->find(j) != slots_nodes->end()) {
              return Status(Status::ClusterInvalidInfo, errSlotOverlapped);
            } else {
              (*slots_nodes)[j] = id;
            }
          }
        }
      } else {
        return Status(Status::ClusterInvalidInfo, errSlotOutOfRange);
      }
    }

    // Create master node
    (*nodes)[id] = Util::MakeShared<ClusterNode>(
        id, host, port, role, master_id, slots);
  }
  return Status::OK();
}

bool Cluster::IsWriteForbiddenSlot(int slot) {
  if (svr_->slot_migrate_->GetForbiddenSlot() == slot) {
    return true;
  }
  return false;
}

Status Cluster::CanExecByMySelf(const Redis::CommandAttributes *attributes,
                                const std::vector<std::string> &cmd_tokens,
                                Redis::Connection *conn) {
  std::vector<int> keys_indexes;
  auto s = Redis::GetKeysFromCommand(attributes->name, cmd_tokens.size(), &keys_indexes);
  // No keys
  if (!s.IsOK()) return Status::OK();
  if (keys_indexes.size() == 0) return Status::OK();

  int slot = -1;
  for (auto i : keys_indexes) {
    if (i >= static_cast<int>(cmd_tokens.size())) break;
    int cur_slot = GetSlotNumFromKey(cmd_tokens[i]);
    if (slot == -1) slot = cur_slot;
    if (slot != cur_slot) {
      return Status(Status::RedisExecErr,
        "CROSSSLOT Attempted to access keys that don't hash to the same slot");
    }
  }
  if (slot == -1) return Status::OK();

  if (slots_nodes_[slot] == nullptr) {
    return Status(Status::ClusterDown, "CLUSTERDOWN Hash slot not served");
  } else if (myself_ && myself_ == slots_nodes_[slot]) {
    // We use central controller to manage the topology of the cluster.
    // Server can't change the topology directly, so we record the migrated slots
    // to move the requests of the migrated slots to the destination node.
    if (migrated_slots_.count(slot)) {  // I'm not serving the migrated slot
      return Status(Status::RedisExecErr, "MOVED " + std::to_string(slot) + " " + migrated_slots_[slot]);
    }
    // To keep data consistency, slot will be forbidden write while sending the last incremental data.
    // During this phase, the requests of the migrating slot has to be rejected.
    if (attributes->is_write() && IsWriteForbiddenSlot(slot)) {
      return Status(Status::RedisExecErr, "Can't write to slot being migrated which is in write forbidden phase");
    }
    return Status::OK();  // I'm serving this slot
  } else if (myself_ && myself_->importing_slot_ == slot && conn->IsImporting()) {
    // While data migrating, the topology of the destination node has not been changed.
    // The destination node has to serve the requests from the migrating slot,
    // although the slot is not belong to itself. Therefore, we record the importing slot
    // and mark the importing connection to accept the importing data.
    return Status::OK();  // I'm serving the importing connection
  } else if (myself_ && imported_slots_.count(slot)) {
    // After the slot is migrated, new requests of the migrated slot will be moved to
    // the destination server. Before the central controller change the topology, the destination
    // server should record the imported slots to accept new data of the imported slots.
    return Status::OK();  // I'm serving the imported slot
  } else if (myself_ && myself_->role_ == kClusterSlave
     && attributes->is_write() == false
     && nodes_.find(myself_->master_id_) != nodes_.end()
     && nodes_[myself_->master_id_] == slots_nodes_[slot]) {
    return Status::OK();  // My mater is serving this slot
  } else {
    std::string ip_port = slots_nodes_[slot]->host_ + ":" +
                          std::to_string(slots_nodes_[slot]->port_);
    return Status(Status::RedisExecErr, "MOVED " + std::to_string(slot) + " " + ip_port);
  }
}
