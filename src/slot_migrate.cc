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

#include "slot_migrate.h"

#include <memory>
#include <utility>

#include "batch_extractor.h"
#include "event_util.h"


static std::map<RedisType, std::string> type_to_cmd = {
  {kRedisString, "set"},
  {kRedisList, "rpush"},
  {kRedisHash, "hmset"},
  {kRedisSet, "sadd"},
  {kRedisZSet, "zadd"},
  {kRedisBitmap, "setbit"},
  {kRedisSortedint, "siadd"},
};

SlotMigrate::SlotMigrate(Server *svr, int speed, int pipeline_size, int seq_gap)
                        : Database(svr->storage_, kDefaultNamespace), svr_(svr),
                          state_machine_(kSlotMigrateNone), current_pipeline_size_(0),
                          last_send_time_(0), slot_job_(nullptr), slot_snapshot_time_(0),
                          wal_begin_seq_(0), wal_increment_seq_(0) {
  // Let db_ and metadata_cf_handle_ be nullptr, and get them in real time to avoid accessing invalid pointer,
  // because metadata_cf_handle_ and db_ will be destroyed if DB is reopened.
  // [Situation]:
  // 1. Start an empty slave server.
  // 2. Connect to master which has amount of data, and trigger full sychronization.
  // 3. After replication, change slave to master and start slot migrate.
  // 4. It will occur segment fault when using metadata_cf_handle_ to create iterator of rocksdb.
  // [Reason]:
  // After full sychronization, DB will be reopened, db_ and metadata_cf_handle_ will be released.
  // Then, if we create rocksdb iterator with metadata_cf_handle_, it will go wrong.
  // [Solution]:
  // db_ and metadata_cf_handle_ will be replaced by storage_->GetDB() and storage_->GetCFHandle("metadata")
  // in all functions used in migration process.
  // [Note]:
  // This problem may exist in all functions of Database called in slot migration process.
  db_ = nullptr;
  metadata_cf_handle_ = nullptr;

  if (speed >= 0) {
    migrate_speed_ = speed;
  }
  if (pipeline_size > 0) {
    pipeline_size_limit_ = pipeline_size;
  }
  if (seq_gap > 0) {
    seq_gap_limit_ = seq_gap;
  }

  dst_port_ = -1;
  forbidden_slot_ = -1;
  migrate_slot_ = -1;
  migrate_failed_slot_ = -1;
  migrate_state_ = kMigrateNone;
  stop_ = false;
  slot_snapshot_ = nullptr;

  if (svr->IsSlave()) {
    SetMigrateStopFlag(true);
  }
}

Status SlotMigrate::MigrateStart(Server *svr, const std::string &node_id, const std::string dst_ip,
                                 int dst_port, int slot, int speed, int pipeline_size, int seq_gap) {
  // Only one slot migration job at the same time
  int16_t no_slot = -1;
  if (migrate_slot_.compare_exchange_strong(no_slot, (int16_t)slot) == false) {
    return Status(Status::NotOK, "There is already a migrating slot");
  }
  if (forbidden_slot_ == slot) {
    // Have to release migrate slot set above
    migrate_slot_ = -1;
    return Status(Status::NotOK, "Can't migrate slot which has been migrated");
  }

  migrate_state_ = kMigrateStart;
  if (speed <= 0) {
    speed = 0;
  }
  if (pipeline_size <= 0) {
    pipeline_size = kPipelineSize;
  }
  if (seq_gap <= 0) {
    seq_gap = kSeqGapLimit;
  }
  dst_node_ = node_id;

  // Create migration job
  auto job = Util::MakeUnique<SlotMigrateJob>(slot, dst_ip, dst_port,
                                           speed, pipeline_size, seq_gap);
  {
    std::lock_guard<std::mutex> guard(job_mutex_);
    slot_job_ = std::move(job);
    job_cv_.notify_one();
  }
  LOG(INFO) << "[migrate] Start migrating slot " << slot
            << " to " << dst_ip << ":" << dst_port;
  return Status::OK();
}

Status SlotMigrate::CreateMigrateHandleThread(void) {
  try {
    t_ = std::thread([this]() {
      Util::ThreadSetName("slot-migrate");
      this->Loop(static_cast<void*>(this));
    });
  } catch(const std::exception &e) {
    return Status(Status::NotOK, std::string(e.what()));
  }
  return Status::OK();
}

void *SlotMigrate::Loop(void *arg) {
  while (true) {
    std::unique_lock<std::mutex> ul(this->job_mutex_);
    while (this->slot_job_ == nullptr) {
      this->job_cv_.wait(ul);
    }
    ul.unlock();

    LOG(INFO) << "[migrate] migrate_slot: " << slot_job_->migrate_slot_
              << ", dst_ip: " << slot_job_->dst_ip_
              << ", dst_port: " << slot_job_->dst_port_
              << ", speed_limit: " << slot_job_->speed_limit_
              << ", pipeline_size_limit: " << slot_job_->pipeline_size_;

    dst_ip_ = slot_job_->dst_ip_;
    dst_port_ = slot_job_->dst_port_;
    migrate_speed_ = slot_job_->speed_limit_;
    pipeline_size_limit_ = slot_job_->pipeline_size_;
    seq_gap_limit_ = slot_job_->seq_gap_;

    StateMachine();
  }
  return nullptr;
}

void SlotMigrate::StateMachine(void) {
  state_machine_ = kSlotMigrateStart;
  while (true) {
    switch (state_machine_) {
      case kSlotMigrateStart: {
        auto s = Start();
        if (s.IsOK()) {
          LOG(INFO) << "[migrate] Succeed to start migrating";
          state_machine_ = kSlotMigrateSnapshot;
        } else {
          LOG(ERROR) << "[migrate] Failed to start migrating";
          state_machine_ = kSlotMigrateFailed;
        }
        break;
      }
      case kSlotMigrateSnapshot: {
        auto s = SendSnapshot();
        if (s.IsOK()) {
          LOG(INFO) << "[migrate] Succeed to send snapshot";
          state_machine_ = kSlotMigrateWal;
        } else {
          LOG(ERROR) << "[migrate] Failed to send snapshot";
          state_machine_ = kSlotMigrateFailed;
        }
        break;
      }
      case kSlotMigrateWal: {
        auto s = SyncWal();
        if (s.IsOK()) {
          LOG(INFO) << "[migrate] Succeed to sync WAL";
          state_machine_ = kSlotMigrateSuccess;
        } else {
          LOG(ERROR) << "[migrate] Failed to sync Wal";
          state_machine_ = kSlotMigrateFailed;
        }
        break;
      }
      case kSlotMigrateSuccess: {
        auto s = Success();
        if (s.IsOK()) {
          LOG(INFO) << "[migrate] Succeed to migrate slot " << migrate_slot_;
          state_machine_ = kSlotMigrateClean;
          migrate_state_ = kMigrateSuccess;
        } else {
          state_machine_ = kSlotMigrateFailed;
        }
        break;
      }
      case kSlotMigrateFailed:
        Fail();
        LOG(INFO) << "[migrate] Failed to migrate slot" << migrate_slot_;
        migrate_state_ = kMigrateFailed;
        state_machine_ = kSlotMigrateClean;
        break;
      case kSlotMigrateClean:
        Clean();
        return;
      default:
        LOG(ERROR) << "[migrate] Wrong state for state machine";
        Clean();
        return;
    }
  }
}

Status SlotMigrate::Start(void) {
  // Get snapshot and sequence
  slot_snapshot_ = storage_->GetDB()->GetSnapshot();
  if (slot_snapshot_ == nullptr) {
    LOG(INFO) << "[migrate] Failed to create snapshot";
    return Status(Status::NotOK);
  }
  wal_begin_seq_ = slot_snapshot_->GetSequenceNumber();

  slot_snapshot_time_ = Util::GetTimeStampUS();
  current_pipeline_size_ = 0;
  last_send_time_ = 0;

  // Connect to dst node
  auto s = Util::SockConnect(dst_ip_, dst_port_, &slot_job_->slot_fd_);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to connect destination server";
    return Status(Status::NotOK);
  }

  // Auth first
  std::string pass = svr_->GetConfig()->requirepass;
  if (!pass.empty()) {
    bool st = AuthDstServer(slot_job_->slot_fd_, pass);
    if (!st) {
      return Status(Status::NotOK, "Failed to auth destination server");
    }
  }

  // Set dst node importing START
  if (!SetDstImportStatus(slot_job_->slot_fd_, kImportStart)) {
    LOG(ERROR) << "[migrate] Failed to notify the destination to prepare to import data";
    return Status(Status::NotOK);
  }

  LOG(INFO) << "[migrate] Start migrating slot " << migrate_slot_
            << ", connect destination fd " << slot_job_->slot_fd_;
  return Status::OK();
}

Status SlotMigrate::SendSnapshot(void) {
  // Create DB iter of snapshot
  uint64_t migratedkey_cnt = 0, expiredkey_cnt = 0, emptykey_cnt = 0;
  std::string restore_cmds;
  int16_t slot = migrate_slot_;
  LOG(INFO) << "[migrate] Start migrating snapshot of slot " << slot;

  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  read_options.fill_cache = false;
  rocksdb::ColumnFamilyHandle *cf_handle = storage_->GetCFHandle("metadata");
  std::unique_ptr<rocksdb::Iterator> iter(storage_->GetDB()->NewIterator(read_options, cf_handle));

  // Construct key prefix to iterate the keys belong to the target slot
  std::string prefix;
  ComposeSlotKeyPrefix(namespace_, slot, &prefix);
  LOG(INFO) << "[migrate] Iterate keys of slot, key's prefix: " << prefix;

  // Seek to the begining of keys start with 'prefix' and iterate all these keys
  for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    // The migrating task has to be stopped, if server role is changed from master to slave
    // or flush command (flushdb or flushall) is executed
    if (stop_) {
      LOG(ERROR) << "[migrate] Stop migrating snapshot due to the thread stoped";
      return Status(Status::NotOK);
    }

    // Iteration is out of range
    if (!iter->key().starts_with(prefix)) {
      break;
    }

    // Get user key
    std::string ns, user_key;
    ExtractNamespaceKey(iter->key(), &ns, &user_key, true);
    current_migrate_key_ = user_key;

    // Add key's constructed cmd to restore_cmds, send pipeline
    // or not according to current_pipeline_size_
    auto stat = MigrateOneKey(user_key, iter->value(), &restore_cmds);
    if (stat.IsOK()) {
      if (stat.Msg() == "ok") migratedkey_cnt++;
      if (stat.Msg() == "expired") expiredkey_cnt++;
      if (stat.Msg() == "empty") emptykey_cnt++;
    } else {
      LOG(ERROR) << "[migrate] Failed to migrate key: " << user_key;
      return Status(Status::NotOK);
    }
  }

  // Send the rest data in pipeline. This operation is necessary,
  // the final pipeline may not be sent while iterating keys,
  // because its size may less than pipeline_size_limit_.
  if (!SendCmdsPipelineIfNeed(&restore_cmds, true)) {
    LOG(ERROR) << "[migrate] Failed to send left data in pipeline";
    return Status(Status::NotOK);
  }

  LOG(INFO) << "[migrate] Succeed to migrate slot snapshot, slot: " << slot
            << ", Migrated keys: " << migratedkey_cnt
            << ", Expired keys: " << expiredkey_cnt
            << ", Emtpy keys: " << emptykey_cnt;
  return Status::OK();
}

Status SlotMigrate::SyncWal(void) {
  // Send incremental data in wal circularly until new increment less than a certain amount
  auto s = SyncWalBeforeForbidSlot();
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to sync WAL before forbidding slot";
    return Status(Status::NotOK);
  }

  // Set forbidden slot, and send last incremental data
  s = SyncWalAfterForbidSlot();
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to sync WAL after forbidding slot";
    return Status(Status::NotOK);
  }
  return Status::OK();
}

Status SlotMigrate::Success(void) {
  if (stop_) {
    LOG(ERROR) << "[migrate] Stop migrating slot " << migrate_slot_;
    return Status(Status::NotOK);
  }
  // Set destination status SUCCESS
  if (!SetDstImportStatus(slot_job_->slot_fd_, kImportSuccess)) {
    LOG(ERROR) << "[migrate] Failed to notify the destination that data migration succeeded";
    return Status(Status::NotOK);
  }
  std::string dst_ip_port = dst_ip_ + ":" + std::to_string(dst_port_);
  Status st = svr_->cluster_->SetSlotMigrated(migrate_slot_, dst_ip_port);
  if (!st.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to set slot, Err:" << st.Msg();
    return Status(Status::NotOK);
  }
  migrate_failed_slot_ = -1;
  return Status::OK();
}

Status SlotMigrate::Fail(void) {
  // Set destination status
  if (!SetDstImportStatus(slot_job_->slot_fd_, kImportFailed)) {
    LOG(INFO) << "[migrate] Failed to notify the destination that data migration failed";
  }
  // Stop slot forbiding writing
  migrate_failed_slot_ = migrate_slot_;
  forbidden_slot_ = -1;
  return Status::OK();
}

Status SlotMigrate::Clean(void) {
  LOG(INFO) << "[migrate] Clean resources of migrating slot " << migrate_slot_;
  if (slot_snapshot_) {
    storage_->GetDB()->ReleaseSnapshot(slot_snapshot_);
    slot_snapshot_ = nullptr;
  }

  state_machine_ = kSlotMigrateNone;
  current_pipeline_size_ = 0;
  wal_begin_seq_ = 0;
  wal_increment_seq_ = 0;
  std::lock_guard<std::mutex> guard(job_mutex_);
  slot_job_ = nullptr;
  migrate_slot_ = -1;
  SetMigrateStopFlag(false);
  return Status::OK();
}

bool SlotMigrate::AuthDstServer(int sock_fd, std::string password) {
  std::string cmd = Redis::MultiBulkString({"auth", password}, false);
  auto s = Util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to send auth command to destination, slot: "
               << migrate_slot_ << ", error: " << s.Msg();
    return false;
  }

  if (!CheckResponseOnce(sock_fd)) {
    LOG(ERROR) << "[migrate] Failed to auth destination server with '" << password
               << "', stop migrating slot " << migrate_slot_;
    return false;
  }
  return true;
}

bool SlotMigrate::SetDstImportStatus(int sock_fd, int status) {
  if (sock_fd <= 0) return false;

  int slot = migrate_slot_;
  std::string cmd = Redis::MultiBulkString({"cluster", "import", std::to_string(slot), std::to_string(status)});
  auto s = Util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to send import command to destination, slot: " << slot << ", error: " << s.Msg();
    return false;
  }

  return CheckResponseOnce(sock_fd);
}

bool SlotMigrate::CheckResponseOnce(int sock_fd) {
  return CheckResponseWithCounts(sock_fd, 1);
}

// Commands  |  Response            |  Instance
// ++++++++++++++++++++++++++++++++++++++++
// set          Redis::Integer         :1/r/n
// hset         Redis::SimpleString    +OK/r/n
// sadd         Redis::Integer
// zadd         Redis::Integer
// siadd        Redis::Integer
// setbit       Redis::Integer
// expire       Redis::Integer
// lpush        Redis::Integer
// rpush        Redis::Integer
// ltrim        Redis::SimpleString    -Err\r\n
// linsert      Redis::Integer
// lset         Redis::SimpleString
// hdel         Redis::Intege
// srem         Redis::Integer
// zrem         Redis::Integer
// lpop         Redis::NilString       $-1\r\n
//          or  Redis::BulkString      $1\r\n1\r\n
// rpop         Redis::NilString
//          or  Redis::BulkString
// lrem         Redis::Integer
// sirem        Redis::Integer
// del          Redis::Integer
bool SlotMigrate::CheckResponseWithCounts(int sock_fd, int total) {
  if (sock_fd < 0 || total <= 0) {
    LOG(INFO) << "[migrate] Invalid args, sock_fd: " << sock_fd
              << ", count: " << total;
    return false;
  }

  // Set socket recieve timeout first
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  // Start checking response
  size_t bulk_len = 0;
  int cnt = 0;
  stat_ = ArrayLen;
  UniqueEvbuf evbuf;
  while (true) {
    // Read response data from socket buffer to event buffer
    if (evbuffer_read(evbuf.get(), sock_fd, -1) <= 0) {
      LOG(ERROR) << "[migrate] Failed to read response, Err: " + std::string(strerror(errno));
      return false;
    }

    // Parse response data in event buffer
    bool run = true;
    while (run) {
      switch (stat_) {
        // Handle single string response
        case ArrayLen: {
          UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
          if (!line) {
            LOG(INFO) << "[migrate] Event buffer is empty, read socket again";
            run = false;
            break;
          }

          if (line[0] == '-') {
            LOG(ERROR) << "[migrate] Got invalid response: " + std::string(line.get())
                << ", line length: " << line.length;
            stat_ = Error;
          } else if (line[0] == '$') {
            try {
              bulk_len = std::stoull(std::string(line.get() + 1, line.length - 1));
              stat_ = bulk_len > 0 ? BulkData : OneRspEnd;
            } catch (const std::exception &e) {
              LOG(ERROR) << "[migrate] Protocol Err: expect integer";
              stat_ = Error;
            }
          } else if (line[0] == '+' || line[0] == ':') {
              stat_ = OneRspEnd;
          } else {
            LOG(ERROR) << "[migrate] Unexpected response: " << line.get();
            stat_ = Error;
          }

          break;
        }
        // Handle bulk string response
        case BulkData: {
          if (evbuffer_get_length(evbuf.get()) < bulk_len + 2) {
            LOG(INFO) << "[migrate] Bulk data in event buffer is not complete, read socket again";
            run = false;
            break;
          }
          // TODO(chrisZMF): Check tail '\r\n'
          evbuffer_drain(evbuf.get(), bulk_len + 2);
          bulk_len = 0;
          stat_ = OneRspEnd;
          break;
        }
        case OneRspEnd: {
          cnt++;
          if (cnt >= total) {
            return true;
          }
          stat_ = ArrayLen;
          break;
        }
        case Error: {
          return false;
        }
        default: break;
      }
    }
  }
  return true;  // Can't reach here
}

Status SlotMigrate::MigrateOneKey(const rocksdb::Slice &key, const rocksdb::Slice &value, std::string *restore_cmds) {
  std::string prefix_key;
  AppendNamespacePrefix(key, &prefix_key);
  std::string bytes = value.ToString();
  Metadata metadata(kRedisNone, false);
  metadata.Decode(bytes);
  if (metadata.Type() != kRedisString && metadata.size == 0) {
    LOG(INFO) << "[migrate] No elements of key: " << prefix_key;
    return Status(Status::cOK, "empty");
  }

  if (metadata.Expired()) {
    return Status(Status::cOK, "expired");
  }

  // Construct command according to type of the key
  switch (metadata.Type()) {
    case kRedisString: {
      bool s = MigrateSimpleKey(key, metadata, bytes, restore_cmds);
      if (!s) {
        LOG(ERROR) << "[migrate] Failed to migrate simple key: " << key.ToString();
        return Status(Status::NotOK);
      }
      break;
    }
    case kRedisList:
    case kRedisZSet:
    case kRedisBitmap:
    case kRedisHash:
    case kRedisSet:
    case kRedisSortedint: {
      bool s = MigrateComplexKey(key, metadata, restore_cmds);
      if (!s) {
        LOG(ERROR) << "[migrate] Failed to migrate complex key: " << key.ToString();
        return Status(Status::NotOK);
      }
      break;
    }
    default:
      break;
  }
  return Status::OK();
}

bool SlotMigrate::MigrateSimpleKey(const rocksdb::Slice &key, const Metadata &metadata,
                                   const std::string &bytes, std::string *restore_cmds) {
  std::vector<std::string> command = {"set", key.ToString(), bytes.substr(5, bytes.size() - 5)};
  if (metadata.expire > 0) {
    command.emplace_back("EXAT");
    command.emplace_back(std::to_string(metadata.expire));
  }
  *restore_cmds += Redis::MultiBulkString(command, false);
  current_pipeline_size_++;

  // Check whether pipeline needs to be sent
  // TODO(chrisZMF): Resend data if failed to send data
  if (!SendCmdsPipelineIfNeed(restore_cmds, false)) {
    LOG(ERROR) << "[migrate] Failed to send simple key";
    return false;
  }
  return true;
}

bool SlotMigrate::MigrateComplexKey(const rocksdb::Slice &key, const Metadata &metadata, std::string *restore_cmds) {
  std::string cmd;
  cmd = type_to_cmd[metadata.Type()];

  // Construct key prefix to iterate values of the complex type user key
  std::vector<std::string> user_cmd = {cmd, key.ToString()};
  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  read_options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> iter(storage_->GetDB()->NewIterator(read_options));

  std::string slot_key, prefix_subkey;
  AppendNamespacePrefix(key, &slot_key);
  InternalKey(slot_key, "", metadata.version, true).Encode(&prefix_subkey);
  int itermscount = 0;
  for (iter->Seek(prefix_subkey); iter->Valid(); iter->Next()) {
    if (stop_) {
      LOG(ERROR) << "[migrate] Stop migrating complex key due to task stopped";
      return false;
    }

    if (!iter->key().starts_with(prefix_subkey)) {
      break;
    }

    // Parse values of the complex key
    // InternalKey is adopt to get compex key's value
    // from the formatted key return by iterator of rocksdb
    InternalKey inkey(iter->key(), true);
    switch (metadata.Type()) {
      case kRedisSet: {
        user_cmd.emplace_back(inkey.GetSubKey().ToString());
        break;
      }
      case kRedisSortedint: {
        auto id = DecodeFixed64(inkey.GetSubKey().ToString().data());
        user_cmd.emplace_back(std::to_string(id));
        break;
      }
      case kRedisZSet: {
        auto score = DecodeDouble(iter->value().ToString().data());
        user_cmd.emplace_back(Util::Float2String(score));
        user_cmd.emplace_back(inkey.GetSubKey().ToString());
        break;
      }
      case kRedisBitmap: {
        if (!MigrateBitmapKey(inkey, &iter, &user_cmd, restore_cmds)) return false;
        break;
      }
      case kRedisHash: {
        user_cmd.emplace_back(inkey.GetSubKey().ToString());
        user_cmd.emplace_back(iter->value().ToString());
        break;
      }
      case kRedisList: {
        user_cmd.emplace_back(iter->value().ToString());
        break;
      }
      default:
        break;
    }

    // Check iterms count
    // Exclude bitmap because it does not have hmset-like command
    if (metadata.Type() != kRedisBitmap) {
      itermscount++;
      if (itermscount >= kMaxItemsInCommand) {
        *restore_cmds += Redis::MultiBulkString(user_cmd, false);
        current_pipeline_size_++;
        itermscount = 0;
        // Have to clear saved iterms
        user_cmd.erase(user_cmd.begin() + 2, user_cmd.end());

        // Maybe key has amout of elements, have to check pipeline here
        if (!SendCmdsPipelineIfNeed(restore_cmds, false)) {
          LOG(INFO) << "[migrate] Failed to send complex key part";
          return false;
        }
      }
    }
  }

  // Have to check the iterm count of the last command list
  if (itermscount % kMaxItemsInCommand) {
    *restore_cmds += Redis::MultiBulkString(user_cmd, false);
    current_pipeline_size_++;
  }

  // Add ttl for complex key
  if (metadata.expire) {
    *restore_cmds += Redis::MultiBulkString({"EXPIREAT", key.ToString(), std::to_string(metadata.expire)}, false);
    current_pipeline_size_++;
  }

  // Check whether pipeline needs to send
  if (!SendCmdsPipelineIfNeed(restore_cmds, false)) {
    LOG(INFO) << "[migrate] Failed to send complex key";
    return false;
  }
  return true;
}

bool SlotMigrate::MigrateBitmapKey(const InternalKey &inkey,
                                   std::unique_ptr<rocksdb::Iterator> *iter,
                                   std::vector<std::string> *user_cmd,
                                   std::string *restore_cmds) {
  uint32_t index, offset;
  std::string index_str = inkey.GetSubKey().ToString();
  std::string fragment = (*iter)->value().ToString();
  try {
    index = std::stoi(index_str);
  } catch (std::exception &e) {
    LOG(ERROR) << "[migrate] Parse bitmap index error, Err: " << strerror(errno);
    return false;
  }

  // Bitmap does not have hmset-like command
  // TODO(chrisZMF): Use hmset-like command for efficiency
  for (int byte_idx = 0; byte_idx < static_cast<int>(fragment.size()); byte_idx++) {
    if (fragment[byte_idx] & 0xff) {
      for (int bit_idx = 0; bit_idx < 8; bit_idx++) {
        if (fragment[byte_idx] & (1 << bit_idx)) {
          offset = (index * 8) + (byte_idx * 8) + bit_idx;
          user_cmd->emplace_back(std::to_string(offset));
          user_cmd->emplace_back("1");
          *restore_cmds += Redis::MultiBulkString(*user_cmd, false);
          current_pipeline_size_++;
          user_cmd->erase(user_cmd->begin() + 2, user_cmd->end());
        }
      }

      if (!SendCmdsPipelineIfNeed(restore_cmds, false)) {
        LOG(ERROR) << "[migrate] Failed to send commands of bitmap!";
        return false;
      }
    }
  }
  return true;
}

bool SlotMigrate::SendCmdsPipelineIfNeed(std::string *commands, bool need) {
  // Stop migrating or not
  if (stop_) {
    LOG(ERROR) << "[migrate] Stop sending data due to migrating thread stopped"
               << ", current migrating slot: " << migrate_slot_;
    return false;
  }

  // Check pipeline
  if (need == false && current_pipeline_size_ < pipeline_size_limit_) {
    return true;
  }
  if (current_pipeline_size_ == 0) {
    LOG(INFO) << "[migrate] No data to send";
    return true;
  }

  // Migrate speed limit
  MigrateSpeedLimit();

  // Send pipeline
  auto s = Util::SockSend(slot_job_->slot_fd_, *commands);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to send commands, Err: " << s.Msg();
    return false;
  }
  last_send_time_ = Util::GetTimeStampUS();

  // Check response
  bool st = CheckResponseWithCounts(slot_job_->slot_fd_, current_pipeline_size_);
  if (!st) {
    LOG(ERROR) << "[migrate] Wrong response";
    return false;
  }

  // Clear commands and currentpipeline
  commands->clear();
  current_pipeline_size_ = 0;
  return true;
}

void SlotMigrate::SetForbiddenSlot(int16_t slot) {
  LOG(INFO) << "[migrate] Set forbidden slot " << slot;
  forbidden_slot_ = slot;
}

void SlotMigrate::ReleaseForbiddenSlot() {
  LOG(INFO) << "[migrate] Release forbidden slot " << forbidden_slot_;
  forbidden_slot_ = -1;
}

void SlotMigrate::MigrateSpeedLimit(void) {
  if (migrate_speed_ > 0) {
    uint64_t current_time = Util::GetTimeStampUS();
    uint64_t per_request_time = 1000000 * pipeline_size_limit_ / migrate_speed_;
    if (per_request_time == 0) {
      per_request_time = 1;
    }
    if (last_send_time_ + per_request_time > current_time) {
      uint64_t during = last_send_time_ + per_request_time - current_time;
      LOG(INFO) << "[migrate] Sleep for migrating speed limit, sleep duration: " << during;
      std::this_thread::sleep_for(std::chrono::microseconds(during));
    }
  }
}

Status SlotMigrate::GenerateCmdsFromBatch(rocksdb::BatchResult *batch, std::string *commands) {
  // Iterate batch to get keys and construct commands for keys
  WriteBatchExtractor write_batch_extractor(storage_->IsSlotIdEncoded(), migrate_slot_, false);
  rocksdb::Status status = batch->writeBatchPtr->Iterate(&write_batch_extractor);
  if (!status.ok()) {
    LOG(ERROR) << "[migrate] Failed to parse write batch, Err: " << status.ToString();
    return Status(Status::NotOK);
  }

  // Get all constructed commands
  auto resp_commands = write_batch_extractor.GetRESPCommands();
  for (const auto &iter : *resp_commands) {
    for (const auto &it : iter.second) {
      *commands += it;
      current_pipeline_size_++;
    }
  }
  return Status::OK();
}

Status SlotMigrate::MigrateIncrementData(std::unique_ptr<rocksdb::TransactionLogIterator> *iter, uint64_t endseq) {
  if (!(*iter) || !(*iter)->Valid()) {
    LOG(ERROR) << "[migrate] WAL iterator is invalid";
    return Status(Status::NotOK);
  }

  uint64_t next_seq = wal_begin_seq_ + 1;
  std::string commands;
  commands.clear();
  while (true) {
    if (stop_) {
      LOG(ERROR) << "[migrate] Migration task end during migrating WAL data";
      return Status(Status::NotOK);
    }
    auto batch = (*iter)->GetBatch();
    if (batch.sequence != next_seq) {
      LOG(ERROR) << "[migrate] WAL iterator is discrete, some seq might be lost"
                 << ", expectd sequence: " << next_seq << ", but got sequence: " << batch.sequence;
      return Status(Status::NotOK);
    }

    // Generate commands by iterating write bacth
    auto s = GenerateCmdsFromBatch(&batch, &commands);
    if (!s.IsOK()) {
      LOG(ERROR) << "[migrate] Failed to generate commands from wirte batch";
      return Status(Status::NotOK);
    }

    // Check whether command pipeline should be sent
    if (!SendCmdsPipelineIfNeed(&commands, false)) {
      LOG(ERROR) << "[migrate] Failed to send WAL commands pipeline";
      return Status(Status::NotOK);
    }

    next_seq = batch.sequence + batch.writeBatchPtr->Count();
    if (next_seq > endseq) {
      LOG(INFO) << "[migrate] Migrate incremental data an epoch OK, seq from " << wal_begin_seq_
                << ", to " << endseq;
      break;
    }
    (*iter)->Next();
    if (!(*iter)->Valid()) {
      LOG(ERROR) << "[migrate] WAL iterator is invalid, expected end seq: " << endseq
                 << ", next seq: " << next_seq;
      return Status(Status::NotOK);
    }
  }

  // Send the left data of this epoch
  if (!SendCmdsPipelineIfNeed(&commands, true)) {
    LOG(ERROR) << "[migrate] Failed to send WAL last commands in pipeline";
    return Status(Status::NotOK);
  }
  return Status::OK();
}

Status SlotMigrate::SyncWalBeforeForbidSlot(void) {
  uint32_t count = 0;
  while (count < kMaxLoopTimes) {
    wal_increment_seq_ = storage_->GetDB()->GetLatestSequenceNumber();
    uint64_t gap = wal_increment_seq_ - wal_begin_seq_;
    if (gap <= static_cast<uint64_t>(seq_gap_limit_)) {
      LOG(INFO) << "[migrate] Incremental data sequence: " << gap
                << ", less than limit: " << seq_gap_limit_
                << ", go to set forbidden slot";
      break;
    }

    std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
    auto s = storage_->GetWALIter(wal_begin_seq_ + 1, &iter);
    if (!s.IsOK()) {
      LOG(ERROR) << "[migrate] Failed to generate WAL iterator before setting forbidden slot"
                 << ", Err: " << s.Msg();
      return Status(Status::NotOK);
    }

    // Iterate wal and migrate data
    s = MigrateIncrementData(&iter, wal_increment_seq_);
    if (!s.IsOK()) {
      LOG(ERROR) << "[migrate] Failed to migrate WAL data before setting forbidden slot";
      return Status(Status::NotOK);
    }

    wal_begin_seq_ = wal_increment_seq_;
    count++;
  }
  LOG(INFO) << "[migrate] Succeed to migrate incremental data before setting forbidden slot, end epoch: " << count;
  return Status::OK();
}

Status SlotMigrate::SyncWalAfterForbidSlot() {
  // Block server to set forbidden slot
  uint64_t during = Util::GetTimeStampUS();
  {
    auto exclusivity = svr_->WorkExclusivityGuard();
    SetForbiddenSlot(migrate_slot_);
  }
  wal_increment_seq_ = storage_->GetDB()->GetLatestSequenceNumber();
  during = Util::GetTimeStampUS() - during;
  LOG(INFO) << "[migrate] To set forbidden slot, server is blocked for " << during << "us";

  // No incremental data
  if (wal_increment_seq_ <= wal_begin_seq_) return Status::OK();

  // Get WAL iter
  std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
  auto s = storage_->GetWALIter(wal_begin_seq_ + 1, &iter);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to generate WAL iterator after setting forbidden slot"
               << ", Err: " << s.Msg();
    return Status(Status::NotOK);
  }

  // Send incremental data
  s = MigrateIncrementData(&iter, wal_increment_seq_);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to migrate WAL data after setting forbidden slot";
    return Status(Status::NotOK);
  }
  return Status::OK();
}

void SlotMigrate::GetMigrateInfo(std::string *info) {
  info->clear();
  if (migrate_slot_ < 0 && forbidden_slot_ < 0 && migrate_failed_slot_ < 0) {
    return;
  }

  int16_t slot = -1;
  std::string task_state;
  switch (migrate_state_.load()) {
    case kMigrateNone:
      task_state = "none";
      break;
    case kMigrateStart:
      task_state = "start";
      slot = migrate_slot_;
      break;
    case kMigrateSuccess:
      task_state = "success";
      slot = forbidden_slot_;
      break;
    case kMigrateFailed:
      task_state = "fail";
      slot = migrate_failed_slot_;
      break;
    default:
      break;
  }

  *info = "migrating_slot: " + std::to_string(slot) + "\r\n"
          + "destination_node: " + dst_node_ + "\r\n"
          + "migrating_state: " + task_state + "\r\n";
}
