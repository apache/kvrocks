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

#include "db_util.h"
#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "storage/batch_extractor.h"
#include "thread_util.h"
#include "time_util.h"
#include "types/redis_stream_base.h"
#include "types/redis_string.h"

static std::map<RedisType, std::string> type_to_cmd = {
    {kRedisString, "set"}, {kRedisList, "rpush"},    {kRedisHash, "hmset"},      {kRedisSet, "sadd"},
    {kRedisZSet, "zadd"},  {kRedisBitmap, "setbit"}, {kRedisSortedint, "siadd"}, {kRedisStream, "xadd"},
};

SlotMigrate::SlotMigrate(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap)
    : Database(svr->storage_, kDefaultNamespace), svr_(svr) {
  // Let db_ and metadata_cf_handle_ be nullptr, and get them in real time to avoid accessing invalid pointer,
  // because metadata_cf_handle_ and db_ will be destroyed if DB is reopened.
  // [Situation]:
  // 1. Start an empty slave server.
  // 2. Connect to master which has amounted of data, and trigger full synchronization.
  // 3. After replication, change slave to master and start slot migrate.
  // 4. It will occur segment fault when using metadata_cf_handle_ to create iterator of rocksdb.
  // [Reason]:
  // After full synchronization, DB will be reopened, db_ and metadata_cf_handle_ will be released.
  // Then, if we create rocksdb iterator with metadata_cf_handle_, it will go wrong.
  // [Solution]:
  // db_ and metadata_cf_handle_ will be replaced by storage_->GetDB() and storage_->GetCFHandle("metadata")
  // in all functions used in migration process.
  // [Note]:
  // This problem may exist in all functions of Database called in slot migration process.
  db_ = nullptr;
  metadata_cf_handle_ = nullptr;

  if (migration_speed >= 0) {
    migration_speed_ = migration_speed;
  }
  if (pipeline_size_limit > 0) {
    pipeline_size_limit_ = pipeline_size_limit;
  }
  if (seq_gap > 0) {
    seq_gap_limit_ = seq_gap;
  }

  dst_port_ = -1;
  forbidden_slot_ = -1;
  migrate_slot_ = -1;
  migrate_failed_slot_ = -1;
  migrate_state_ = kMigrateNone;
  stop_migrate_ = false;
  slot_snapshot_ = nullptr;

  if (svr->IsSlave()) {
    SetMigrateStopFlag(true);
  }
}

Status SlotMigrate::MigrateStart(Server *svr, const std::string &node_id, const std::string &dst_ip, int dst_port,
                                 int slot, int speed, int pipeline_size, int seq_gap) {
  // Only one slot migration job at the same time
  int16_t no_slot = -1;
  if (!migrate_slot_.compare_exchange_strong(no_slot, (int16_t)slot)) {
    return {Status::NotOK, "There is already a migrating slot"};
  }

  if (forbidden_slot_ == slot) {
    // Have to release migrate slot set above
    migrate_slot_ = -1;
    return {Status::NotOK, "Can't migrate slot which has been migrated"};
  }

  migrate_state_ = kMigrateStarted;
  if (speed <= 0) {
    speed = 0;
  }
  if (pipeline_size <= 0) {
    pipeline_size = kDefaultPipelineSizeLimit;
  }
  if (seq_gap <= 0) {
    seq_gap = kDefaultSeqGapLimit;
  }
  dst_node_ = node_id;

  // Create migration job
  auto job = std::make_unique<SlotMigrateJob>(slot, dst_ip, dst_port, speed, pipeline_size, seq_gap);
  {
    std::lock_guard<std::mutex> guard(job_mutex_);
    slot_job_ = std::move(job);
    job_cv_.notify_one();
  }
  LOG(INFO) << "[migrate] Start migrating slot " << slot << " to " << dst_ip << ":" << dst_port;
  return Status::OK();
}

SlotMigrate::~SlotMigrate() {
  if (thread_state_ == ThreadState::Running) {
    stop_migrate_ = true;
    thread_state_ = ThreadState::Terminated;
    job_cv_.notify_all();
    if (t_.joinable()) t_.join();
  }
}

Status SlotMigrate::CreateMigrateHandleThread() {
  try {
    t_ = std::thread([this]() {
      Util::ThreadSetName("slot-migrate");
      thread_state_ = ThreadState::Running;
      this->Loop();
    });
  } catch (const std::exception &e) {
    return {Status::NotOK, std::string(e.what())};
  }

  return Status::OK();
}

void SlotMigrate::Loop() {
  while (true) {
    std::unique_lock<std::mutex> ul(job_mutex_);
    while (!IsTerminated() && !slot_job_) {
      job_cv_.wait(ul);
    }
    ul.unlock();

    if (IsTerminated()) {
      return;
    }

    LOG(INFO) << "[migrate] migrate_slot: " << slot_job_->migrate_slot_ << ", dst_ip: " << slot_job_->dst_ip_
              << ", dst_port: " << slot_job_->dst_port_ << ", speed_limit: " << slot_job_->speed_limit_
              << ", pipeline_size_limit: " << slot_job_->pipeline_size_;

    dst_ip_ = slot_job_->dst_ip_;
    dst_port_ = slot_job_->dst_port_;
    migration_speed_ = slot_job_->speed_limit_;
    pipeline_size_limit_ = slot_job_->pipeline_size_;
    seq_gap_limit_ = slot_job_->seq_gap_;

    StateMachine();
  }
}

void SlotMigrate::StateMachine() {
  state_machine_ = kSlotMigrateStart;
  while (true) {
    if (IsTerminated()) {
      LOG(WARNING) << "[migrate] Will stop state machine, because the thread was terminated";
      return;
    }

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

Status SlotMigrate::Start() {
  // Get snapshot and sequence
  slot_snapshot_ = storage_->GetDB()->GetSnapshot();
  if (!slot_snapshot_) {
    LOG(INFO) << "[migrate] Failed to create snapshot";
    return {Status::NotOK};
  }

  wal_begin_seq_ = slot_snapshot_->GetSequenceNumber();
  slot_snapshot_time_ = Util::GetTimeStampUS();
  current_pipeline_size_ = 0;
  last_send_time_ = 0;

  // Connect to dst node
  auto s = Util::SockConnect(dst_ip_, dst_port_, &slot_job_->slot_fd_);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to connect to destination server: " << s.Msg();
    return {Status::NotOK};
  }

  // Auth first
  std::string pass = svr_->GetConfig()->requirepass;
  if (!pass.empty()) {
    bool ok = AuthDstServer(slot_job_->slot_fd_, pass);
    if (!ok) {
      return {Status::NotOK, "Failed to authenticate on destination server"};
    }
  }

  // Set dst node importing START
  if (!SetDstImportStatus(slot_job_->slot_fd_, kImportStart)) {
    LOG(ERROR) << "[migrate] Failed to notify the destination to prepare to import data";
    return {Status::NotOK};
  }

  LOG(INFO) << "[migrate] Start migrating slot " << migrate_slot_ << ", connect destination fd " << slot_job_->slot_fd_;
  return Status::OK();
}

Status SlotMigrate::SendSnapshot() {
  // Create DB iter of snapshot
  uint64_t migrated_key_cnt = 0;
  uint64_t expired_key_cnt = 0;
  uint64_t empty_key_cnt = 0;
  std::string restore_cmds;
  int16_t slot = migrate_slot_;
  LOG(INFO) << "[migrate] Start migrating snapshot of slot " << slot;

  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  read_options.fill_cache = false;
  rocksdb::ColumnFamilyHandle *cf_handle = storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);
  std::unique_ptr<rocksdb::Iterator> iter(storage_->GetDB()->NewIterator(read_options, cf_handle));

  // Construct key prefix to iterate the keys belong to the target slot
  std::string prefix;
  ComposeSlotKeyPrefix(namespace_, slot, &prefix);
  LOG(INFO) << "[migrate] Iterate keys of slot, key's prefix: " << prefix;

  // Seek to the beginning of keys start with 'prefix' and iterate all these keys
  for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    // The migrating task has to be stopped, if server role is changed from master to slave
    // or flush command (flushdb or flushall) is executed
    if (stop_migrate_) {
      LOG(ERROR) << "[migrate] Stop migrating snapshot due to the thread stopped";
      return {Status::NotOK};
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
    auto result = MigrateOneKey(user_key, iter->value(), &restore_cmds);
    if (result == KeyMigrationResult::kMigrated) {
      migrated_key_cnt++;
    } else if (result == KeyMigrationResult::kExpired) {
      expired_key_cnt++;
    } else if (result == KeyMigrationResult::kUnderlyingStructEmpty) {
      empty_key_cnt++;
    } else {
      LOG(ERROR) << "[migrate] Failed to migrate key: " << user_key;
      return {Status::NotOK};
    }
  }

  // Send the rest data in pipeline. This operation is necessary,
  // the final pipeline may not be sent while iterating keys,
  // because its size may less than pipeline_size_limit_.
  if (!SendCmdsPipelineIfNeed(&restore_cmds, true)) {
    LOG(ERROR) << "[migrate] Failed to send left data in pipeline";
    return {Status::NotOK};
  }

  LOG(INFO) << "[migrate] Succeed to migrate slot snapshot, slot: " << slot << ", Migrated keys: " << migrated_key_cnt
            << ", Expired keys: " << expired_key_cnt << ", Emtpy keys: " << empty_key_cnt;
  return Status::OK();
}

Status SlotMigrate::SyncWal() {
  // Send incremental data in wal circularly until new increment less than a certain amount
  auto s = SyncWalBeforeForbidSlot();
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to sync WAL before forbidding slot";
    return {Status::NotOK};
  }

  // Set forbidden slot, and send last incremental data
  s = SyncWalAfterForbidSlot();
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to sync WAL after forbidding slot";
    return {Status::NotOK};
  }

  return Status::OK();
}

Status SlotMigrate::Success() {
  if (stop_migrate_) {
    LOG(ERROR) << "[migrate] Stop migrating slot " << migrate_slot_;
    return {Status::NotOK};
  }

  // Set destination status SUCCESS
  if (!SetDstImportStatus(slot_job_->slot_fd_, kImportSuccess)) {
    LOG(ERROR) << "[migrate] Failed to notify the destination that data migration succeeded";
    return {Status::NotOK};
  }

  std::string dst_ip_port = dst_ip_ + ":" + std::to_string(dst_port_);
  auto s = svr_->cluster_->SetSlotMigrated(migrate_slot_, dst_ip_port);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to set slot, Err:" << s.Msg();
    return {Status::NotOK};
  }

  migrate_failed_slot_ = -1;

  return Status::OK();
}

Status SlotMigrate::Fail() {
  // Set destination status
  if (!SetDstImportStatus(slot_job_->slot_fd_, kImportFailed)) {
    LOG(INFO) << "[migrate] Failed to notify the destination that data migration failed";
  }
  // Stop slot will forbid writing
  migrate_failed_slot_ = migrate_slot_;
  forbidden_slot_ = -1;

  return Status::OK();
}

Status SlotMigrate::Clean() {
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
  slot_job_.reset();
  migrate_slot_ = -1;
  SetMigrateStopFlag(false);

  return Status::OK();
}

bool SlotMigrate::AuthDstServer(int sock_fd, const std::string &password) {
  std::string cmd = Redis::MultiBulkString({"auth", password}, false);
  auto s = Util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to send auth command to destination, slot: " << migrate_slot_
               << ", error: " << s.Msg();
    return false;
  }

  if (!CheckResponseOnce(sock_fd)) {
    LOG(ERROR) << "[migrate] Failed to auth destination server with '" << password << "', stop migrating slot "
               << migrate_slot_;
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

bool SlotMigrate::CheckResponseOnce(int sock_fd) { return CheckResponseWithCounts(sock_fd, 1); }

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
// hdel         Redis::Integer
// srem         Redis::Integer
// zrem         Redis::Integer
// lpop         Redis::NilString       $-1\r\n
//          or  Redis::BulkString      $1\r\n1\r\n
// rpop         Redis::NilString
//          or  Redis::BulkString
// lrem         Redis::Integer
// sirem        Redis::Integer
// del          Redis::Integer
// xadd         Redis::BulkString
bool SlotMigrate::CheckResponseWithCounts(int sock_fd, int total) {
  if (sock_fd < 0 || total <= 0) {
    LOG(INFO) << "[migrate] Invalid args, sock_fd: " << sock_fd << ", count: " << total;
    return false;
  }

  // Set socket receive timeout first
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  // Start checking response
  size_t bulk_len = 0;
  int cnt = 0;
  parser_state_ = ParserState::ArrayLen;
  UniqueEvbuf evbuf;
  while (true) {
    // Read response data from socket buffer to the event buffer
    if (evbuffer_read(evbuf.get(), sock_fd, -1) <= 0) {
      LOG(ERROR) << "[migrate] Failed to read response, Err: " << strerror(errno);
      return false;
    }

    // Parse response data in event buffer
    bool run = true;
    while (run) {
      switch (parser_state_) {
        // Handle single string response
        case ParserState::ArrayLen: {
          UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
          if (!line) {
            LOG(INFO) << "[migrate] Event buffer is empty, read socket again";
            run = false;
            break;
          }

          if (line[0] == '-') {
            LOG(ERROR) << "[migrate] Got invalid response: " << line.get() << ", line length: " << line.length;
            parser_state_ = ParserState::Error;
          } else if (line[0] == '$') {
            auto parse_result = ParseInt<uint64_t>(std::string(line.get() + 1, line.length - 1), 10);
            if (!parse_result) {
              LOG(ERROR) << "[migrate] Protocol Err: expect integer";
              parser_state_ = ParserState::Error;
            } else {
              bulk_len = *parse_result;
              parser_state_ = bulk_len > 0 ? ParserState::BulkData : ParserState::OneRspEnd;
            }
          } else if (line[0] == '+' || line[0] == ':') {
            parser_state_ = ParserState::OneRspEnd;
          } else {
            LOG(ERROR) << "[migrate] Unexpected response: " << line.get();
            parser_state_ = ParserState::Error;
          }

          break;
        }
        // Handle bulk string response
        case ParserState::BulkData: {
          if (evbuffer_get_length(evbuf.get()) < bulk_len + 2) {
            LOG(INFO) << "[migrate] Bulk data in event buffer is not complete, read socket again";
            run = false;
            break;
          }
          // TODO(chrisZMF): Check tail '\r\n'
          evbuffer_drain(evbuf.get(), bulk_len + 2);
          bulk_len = 0;
          parser_state_ = ParserState::OneRspEnd;
          break;
        }
        case ParserState::OneRspEnd: {
          cnt++;
          if (cnt >= total) {
            return true;
          }
          parser_state_ = ParserState::ArrayLen;
          break;
        }
        case ParserState::Error: {
          return false;
        }
        default:
          break;
      }
    }
  }
}

KeyMigrationResult SlotMigrate::MigrateOneKey(const rocksdb::Slice &key, const rocksdb::Slice &encoded_metadata,
                                              std::string *restore_cmds) {
  std::string prefix_key;
  AppendNamespacePrefix(key, &prefix_key);
  std::string bytes = encoded_metadata.ToString();
  Metadata metadata(kRedisNone, false);
  metadata.Decode(bytes);
  if (metadata.Type() != kRedisString && metadata.Type() != kRedisStream && metadata.size == 0) {
    LOG(INFO) << "[migrate] No elements of key: " << prefix_key;
    return KeyMigrationResult::kUnderlyingStructEmpty;
  }

  if (metadata.Expired()) {
    return KeyMigrationResult::kExpired;
  }

  // Construct command according to type of the key
  switch (metadata.Type()) {
    case kRedisString: {
      bool ok = MigrateSimpleKey(key, metadata, bytes, restore_cmds);
      if (!ok) {
        LOG(ERROR) << "[migrate] Failed to migrate simple key: " << key.ToString();
        return KeyMigrationResult::kError;
      }
      break;
    }
    case kRedisList:
    case kRedisZSet:
    case kRedisBitmap:
    case kRedisHash:
    case kRedisSet:
    case kRedisSortedint: {
      bool ok = MigrateComplexKey(key, metadata, restore_cmds);
      if (!ok) {
        LOG(ERROR) << "[migrate] Failed to migrate complex key: " << key.ToString();
        return KeyMigrationResult::kError;
      }
      break;
    }
    case kRedisStream: {
      StreamMetadata stream_md(false);
      stream_md.Decode(bytes);

      bool ok = MigrateStream(key, stream_md, restore_cmds);
      if (!ok) {
        LOG(ERROR) << "[migrate] Failed to migrate stream: " << key.ToString();
        return KeyMigrationResult::kError;
      }
      break;
    }
    default:
      break;
  }

  return KeyMigrationResult::kMigrated;
}

bool SlotMigrate::MigrateSimpleKey(const rocksdb::Slice &key, const Metadata &metadata, const std::string &bytes,
                                   std::string *restore_cmds) {
  std::vector<std::string> command = {"set", key.ToString(),
                                      bytes.substr(Redis::STRING_HDR_SIZE, bytes.size() - Redis::STRING_HDR_SIZE)};
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

  std::vector<std::string> user_cmd = {cmd, key.ToString()};
  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  read_options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> iter(storage_->GetDB()->NewIterator(read_options));

  // Construct key prefix to iterate values of the complex type user key
  std::string slot_key, prefix_subkey;
  AppendNamespacePrefix(key, &slot_key);
  InternalKey(slot_key, "", metadata.version, true).Encode(&prefix_subkey);
  int item_count = 0;

  for (iter->Seek(prefix_subkey); iter->Valid(); iter->Next()) {
    if (stop_migrate_) {
      LOG(ERROR) << "[migrate] Stop migrating complex key due to task stopped";
      return false;
    }

    if (!iter->key().starts_with(prefix_subkey)) {
      break;
    }

    // Parse values of the complex key
    // InternalKey is adopted to get complex key's value
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

    // Check item count
    // Exclude bitmap because it does not have hmset-like command
    if (metadata.Type() != kRedisBitmap) {
      item_count++;
      if (item_count >= kMaxItemsInCommand) {
        *restore_cmds += Redis::MultiBulkString(user_cmd, false);
        current_pipeline_size_++;
        item_count = 0;
        // Have to clear saved items
        user_cmd.erase(user_cmd.begin() + 2, user_cmd.end());

        // Maybe key has amounted of elements, have to check pipeline here
        if (!SendCmdsPipelineIfNeed(restore_cmds, false)) {
          LOG(INFO) << "[migrate] Failed to send complex key part";
          return false;
        }
      }
    }
  }

  // Have to check the item count of the last command list
  if (item_count % kMaxItemsInCommand != 0) {
    *restore_cmds += Redis::MultiBulkString(user_cmd, false);
    current_pipeline_size_++;
  }

  // Add TTL for complex key
  if (metadata.expire > 0) {
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

bool SlotMigrate::MigrateStream(const Slice &key, const StreamMetadata &metadata, std::string *restore_cmds) {
  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  read_options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> iter(
      storage_->GetDB()->NewIterator(read_options, storage_->GetCFHandle(Engine::kStreamColumnFamilyName)));

  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  // Construct key prefix to iterate values of the stream
  std::string prefix_key;
  InternalKey(ns_key, "", metadata.version, true).Encode(&prefix_key);

  std::vector<std::string> user_cmd = {type_to_cmd[metadata.Type()], key.ToString()};

  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (stop_migrate_) {
      LOG(ERROR) << "[migrate] Stop migrating stream due to task cancellation";
      return false;
    }

    if (!iter->key().starts_with(prefix_key)) {
      break;
    }

    // Parse values of the complex key
    // InternalKey is adopted to get complex key's value from the formatted key returned by iterator of rocksdb
    InternalKey inkey(iter->key(), true);
    std::vector<std::string> values;
    auto s = Redis::DecodeRawStreamEntryValue(iter->value().ToString(), &values);
    if (!s.IsOK()) {
      LOG(ERROR) << "[migrate] Failed to decode stream values: " << s.Msg();
      return false;
    }

    Slice encoded_id = inkey.GetSubKey();
    Redis::StreamEntryID entry_id;
    GetFixed64(&encoded_id, &entry_id.ms);
    GetFixed64(&encoded_id, &entry_id.seq);

    user_cmd.emplace_back(entry_id.ToString());
    user_cmd.insert(user_cmd.end(), values.begin(), values.end());

    *restore_cmds += Redis::MultiBulkString(user_cmd, false);
    current_pipeline_size_++;

    user_cmd.erase(user_cmd.begin() + 2, user_cmd.end());

    if (!SendCmdsPipelineIfNeed(restore_cmds, false)) {
      LOG(INFO) << "[migrate] Failed to send the part of stream restoring commands";
      return false;
    }
  }

  // commands like XTRIM and XDEL affect stream's metadata, but we use only XADD for a slot migration
  // XSETID is used to adjust stream's info on the destination node according to the current values on the source
  *restore_cmds += Redis::MultiBulkString(
      {"XSETID", key.ToString(), metadata.last_generated_id.ToString(), "ENTRIESADDED",
       std::to_string(metadata.entries_added), "MAXDELETEDID", metadata.max_deleted_entry_id.ToString()},
      false);
  current_pipeline_size_++;

  // Add TTL
  if (metadata.expire > 0) {
    *restore_cmds += Redis::MultiBulkString({"EXPIREAT", key.ToString(), std::to_string(metadata.expire)}, false);
    current_pipeline_size_++;
  }

  // Check whether commands from the pipeline need to be sent
  if (!SendCmdsPipelineIfNeed(restore_cmds, false)) {
    LOG(INFO) << "[migrate] Failed to send stream commands";
    return false;
  }

  return true;
}

bool SlotMigrate::MigrateBitmapKey(const InternalKey &inkey, std::unique_ptr<rocksdb::Iterator> *iter,
                                   std::vector<std::string> *user_cmd, std::string *restore_cmds) {
  uint32_t index = 0;
  uint32_t offset = 0;
  std::string index_str = inkey.GetSubKey().ToString();
  std::string fragment = (*iter)->value().ToString();
  auto parse_result = ParseInt<int>(index_str, 10);
  if (!parse_result) {
    LOG(ERROR) << "[migrate] Parse bitmap index error, Err: " << strerror(errno);
    return false;
  }

  index = *parse_result;

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
  if (stop_migrate_) {
    LOG(ERROR) << "[migrate] Stop sending data due to migrating thread stopped"
               << ", current migrating slot: " << migrate_slot_;
    return false;
  }

  // Check pipeline
  if (!need && current_pipeline_size_ < pipeline_size_limit_) {
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
  bool ok = CheckResponseWithCounts(slot_job_->slot_fd_, current_pipeline_size_);
  if (!ok) {
    LOG(ERROR) << "[migrate] Wrong response";
    return false;
  }

  // Clear commands and running pipeline
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

void SlotMigrate::MigrateSpeedLimit() {
  if (migration_speed_ > 0) {
    uint64_t current_time = Util::GetTimeStampUS();
    uint64_t per_request_time = 1000000 * pipeline_size_limit_ / migration_speed_;
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
    return {Status::NotOK};
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

Status SlotMigrate::MigrateIncrementData(std::unique_ptr<rocksdb::TransactionLogIterator> *iter, uint64_t end_seq) {
  if (!(*iter) || !(*iter)->Valid()) {
    LOG(ERROR) << "[migrate] WAL iterator is invalid";
    return {Status::NotOK};
  }

  uint64_t next_seq = wal_begin_seq_ + 1;
  std::string commands;

  while (true) {
    if (stop_migrate_) {
      LOG(ERROR) << "[migrate] Migration task end during migrating WAL data";
      return {Status::NotOK};
    }

    auto batch = (*iter)->GetBatch();
    if (batch.sequence != next_seq) {
      LOG(ERROR) << "[migrate] WAL iterator is discrete, some seq might be lost"
                 << ", expected sequence: " << next_seq << ", but got sequence: " << batch.sequence;
      return {Status::NotOK};
    }

    // Generate commands by iterating write batch
    auto s = GenerateCmdsFromBatch(&batch, &commands);
    if (!s.IsOK()) {
      LOG(ERROR) << "[migrate] Failed to generate commands from write batch";
      return {Status::NotOK};
    }

    // Check whether command pipeline should be sent
    if (!SendCmdsPipelineIfNeed(&commands, false)) {
      LOG(ERROR) << "[migrate] Failed to send WAL commands pipeline";
      return {Status::NotOK};
    }

    next_seq = batch.sequence + batch.writeBatchPtr->Count();
    if (next_seq > end_seq) {
      LOG(INFO) << "[migrate] Migrate incremental data an epoch OK, seq from " << wal_begin_seq_ << ", to " << end_seq;
      break;
    }

    (*iter)->Next();
    if (!(*iter)->Valid()) {
      LOG(ERROR) << "[migrate] WAL iterator is invalid, expected end seq: " << end_seq << ", next seq: " << next_seq;
      return {Status::NotOK};
    }
  }

  // Send the left data of this epoch
  if (!SendCmdsPipelineIfNeed(&commands, true)) {
    LOG(ERROR) << "[migrate] Failed to send WAL last commands in pipeline";
    return {Status::NotOK};
  }

  return Status::OK();
}

Status SlotMigrate::SyncWalBeforeForbidSlot() {
  uint32_t count = 0;

  while (count < kMaxLoopTimes) {
    wal_increment_seq_ = storage_->GetDB()->GetLatestSequenceNumber();
    uint64_t gap = wal_increment_seq_ - wal_begin_seq_;
    if (gap <= static_cast<uint64_t>(seq_gap_limit_)) {
      LOG(INFO) << "[migrate] Incremental data sequence: " << gap << ", less than limit: " << seq_gap_limit_
                << ", go to set forbidden slot";
      break;
    }

    std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
    auto s = storage_->GetWALIter(wal_begin_seq_ + 1, &iter);
    if (!s.IsOK()) {
      LOG(ERROR) << "[migrate] Failed to generate WAL iterator before setting forbidden slot"
                 << ", Err: " << s.Msg();
      return {Status::NotOK};
    }

    // Iterate wal and migrate data
    s = MigrateIncrementData(&iter, wal_increment_seq_);
    if (!s.IsOK()) {
      LOG(ERROR) << "[migrate] Failed to migrate WAL data before setting forbidden slot";
      return {Status::NotOK};
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
  LOG(INFO) << "[migrate] To set forbidden slot, server was blocked for " << during << "us";

  // No incremental data
  if (wal_increment_seq_ <= wal_begin_seq_) return Status::OK();

  // Get WAL iter
  std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
  auto s = storage_->GetWALIter(wal_begin_seq_ + 1, &iter);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to generate WAL iterator after setting forbidden slot"
               << ", Err: " << s.Msg();
    return {Status::NotOK};
  }

  // Send incremental data
  s = MigrateIncrementData(&iter, wal_increment_seq_);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to migrate WAL data after setting forbidden slot";
    return {Status::NotOK};
  }

  return Status::OK();
}

void SlotMigrate::GetMigrateInfo(std::string *info) const {
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
    case kMigrateStarted:
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

  *info =
      fmt::format("migrating_slot: {}\r\ndestination_node: {}\r\nmigrating_state: {}\r\n", slot, dst_node_, task_state);
}
