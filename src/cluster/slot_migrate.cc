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

#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "storage/iterator.h"
#include "storage/redis_metadata.h"
#include "sync_migrate_context.h"
#include "thread_util.h"
#include "time_util.h"

constexpr std::string_view errMigrationTaskCanceled = "key migration stopped due to a task cancellation";
constexpr std::string_view errFailedToSetImportStatus = "failed to set import status on destination node";

SlotMigrator::SlotMigrator(Server *srv)
    : Database(srv->storage, kDefaultNamespace),
      srv_(srv),
      seq_gap_limit_(srv->GetConfig()->sequence_gap),
      migrate_batch_bytes_per_sec_(srv->GetConfig()->migrate_batch_rate_limit_mb * MiB),
      migrate_batch_size_bytes_(srv->GetConfig()->migrate_batch_size_kb * KiB) {
  // Let metadata_cf_handle_ be nullptr, and get them in real time to avoid accessing invalid pointer,
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
  metadata_cf_handle_ = nullptr;

  if (srv->IsSlave()) {
    SetStopMigrationFlag(true);
  }
}

Status SlotMigrator::PerformSlotRangeMigration(const std::string &node_id, std::string &dst_ip, int dst_port,
                                               const SlotRange &slot_range, SyncMigrateContext *blocking_ctx) {
  // TODO: concurrent migration, multiple migration jobs
  // Only one slot migration job at the same time
  SlotRange empty_slot_range = {-1, -1};
  if (!slot_range_.compare_exchange_strong(empty_slot_range, slot_range)) {
    return {Status::NotOK, "There is already a migrating job"};
  }

  if (slot_range.HasOverlap(forbidden_slot_range_)) {
    // Have to release migrate slot set above
    slot_range_ = empty_slot_range;
    return {Status::NotOK, "Can't migrate slot which has been migrated"};
  }
  migration_state_ = MigrationState::kStarted;

  auto seq_gap = srv_->GetConfig()->sequence_gap;
  if (seq_gap <= 0) {
    seq_gap = kDefaultSequenceGapLimit;
  }

  if (blocking_ctx) {
    std::unique_lock<std::mutex> lock(blocking_mutex_);
    blocking_context_ = blocking_ctx;
    blocking_context_->Suspend();
  }

  dst_node_ = node_id;

  // Create migration job
  auto job = std::make_unique<SlotMigrationJob>(slot_range, dst_ip, dst_port, seq_gap);
  {
    std::lock_guard<std::mutex> guard(job_mutex_);
    migration_job_ = std::move(job);
    job_cv_.notify_one();
  }
  INFO("[migrate] Start migrating slot(s) {} to {}:{}", slot_range.String(), dst_ip, dst_port);

  return Status::OK();
}

SlotMigrator::~SlotMigrator() {
  if (thread_state_ == ThreadState::Running) {
    stop_migration_ = true;
    thread_state_ = ThreadState::Terminated;
    job_cv_.notify_all();
    if (auto s = util::ThreadJoin(t_); !s) {
      WARN("Slot migrating thread operation failed: {}", s.Msg());
    }
  }
}

Status SlotMigrator::CreateMigrationThread() {
  t_ = GET_OR_RET(util::CreateThread("slot-migrate", [this] {
    thread_state_ = ThreadState::Running;
    this->loop();
  }));

  return Status::OK();
}

void SlotMigrator::loop() {
  while (true) {
    {
      std::unique_lock<std::mutex> ul(job_mutex_);
      job_cv_.wait(ul, [&] { return isTerminated() || migration_job_; });
    }

    if (isTerminated()) {
      clean();
      return;
    }
    INFO("[migrate] Migrating slot(s): {}, dst_ip: {}, dst_port: {}", migration_job_->slot_range.String(),
         migration_job_->dst_ip, migration_job_->dst_port);

    dst_ip_ = migration_job_->dst_ip;
    dst_port_ = migration_job_->dst_port;
    seq_gap_limit_ = migration_job_->seq_gap_limit;

    runMigrationProcess();
  }
}

void SlotMigrator::runMigrationProcess() {
  current_stage_ = SlotMigrationStage::kStart;

  while (true) {
    if (isTerminated()) {
      WARN("[migrate] Will stop state machine, because the thread was terminated");
      clean();
      return;
    }

    switch (current_stage_) {
      case SlotMigrationStage::kStart: {
        auto s = startMigration();
        if (s.IsOK()) {
          INFO("[migrate] Succeed to start migrating slot(s) {}", slot_range_.load().String());
          current_stage_ = SlotMigrationStage::kSnapshot;
        } else {
          ERROR("[migrate] Failed to start migrating slot(s) {}. Error: {}", slot_range_.load().String(), s.Msg());
          current_stage_ = SlotMigrationStage::kFailed;
          resumeSyncCtx(s);
        }
        break;
      }
      case SlotMigrationStage::kSnapshot: {
        auto s = sendSnapshot();
        if (s.IsOK()) {
          current_stage_ = SlotMigrationStage::kWAL;
        } else {
          ERROR("[migrate] Failed to send snapshot of slot(s) {}. Error: {}", slot_range_.load().String(), s.Msg());
          current_stage_ = SlotMigrationStage::kFailed;
          resumeSyncCtx(s);
        }
        break;
      }
      case SlotMigrationStage::kWAL: {
        auto s = syncWAL();
        if (s.IsOK()) {
          INFO("[migrate] Succeed to sync from WAL for slot(s) {}", slot_range_.load().String());
          current_stage_ = SlotMigrationStage::kSuccess;
        } else {
          ERROR("[migrate] Failed to sync from WAL for slot(s) {}. Error: {}", slot_range_.load().String(), s.Msg());
          current_stage_ = SlotMigrationStage::kFailed;
          resumeSyncCtx(s);
        }
        break;
      }
      case SlotMigrationStage::kSuccess: {
        auto s = finishSuccessfulMigration();
        if (s.IsOK()) {
          INFO("[migrate] Succeed to migrate slot(s) {}", slot_range_.load().String());
          current_stage_ = SlotMigrationStage::kClean;
          migration_state_ = MigrationState::kSuccess;
          resumeSyncCtx(s);
        } else {
          ERROR("[migrate] Failed to finish a successful migration of slot(s) {}. Error: {}",
                slot_range_.load().String(), s.Msg());
          current_stage_ = SlotMigrationStage::kFailed;
          resumeSyncCtx(s);
        }
        break;
      }
      case SlotMigrationStage::kFailed: {
        auto s = finishFailedMigration();
        if (!s.IsOK()) {
          ERROR("[migrate] Failed to finish a failed migration of slot(s) {}. Error: {}", slot_range_.load().String(),
                s.Msg());
        }
        INFO("[migrate] Failed to migrate a slot(s) {}", slot_range_.load().String());
        migration_state_ = MigrationState::kFailed;
        current_stage_ = SlotMigrationStage::kClean;
        break;
      }
      case SlotMigrationStage::kClean: {
        clean();
        return;
      }
      default:
        ERROR("[migrate] Unexpected state for the state machine: {}", static_cast<int>(current_stage_));
        clean();
        return;
    }
  }
}

Status SlotMigrator::startMigration() {
  // Get snapshot and sequence
  slot_snapshot_ = storage_->GetDB()->GetSnapshot();
  if (!slot_snapshot_) {
    return {Status::NotOK, "failed to create snapshot"};
  }

  wal_begin_seq_ = slot_snapshot_->GetSequenceNumber();

  // Connect to the destination node
  auto result = util::SockConnect(dst_ip_, dst_port_);
  if (!result.IsOK()) {
    return {Status::NotOK, fmt::format("failed to connect to the destination node: {}", result.Msg())};
  }

  dst_fd_.Reset(*result);

  // Auth first
  std::string pass = srv_->GetConfig()->requirepass;
  if (!pass.empty()) {
    auto s = authOnDstNode(*dst_fd_, pass);
    if (!s.IsOK()) {
      return s.Prefixed("failed to authenticate on destination node");
    }
  }

  // Set destination node import status to START
  auto s = setImportStatusOnDstNode(*dst_fd_, kImportStart);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSetImportStatus);
  }

  // The migration relies on the APPLYBATCH command on the destination node
  // to apply the raw key-value batches, so abort the migration if it's unavailable.
  bool supported = GET_OR_RET(supportedApplyBatchCommandOnDstNode(*dst_fd_));
  if (!supported) {
    return {Status::NotOK, "destination node doesn't support the APPLYBATCH command"};
  }
  INFO("[migrate] Start migrating slot(s) {}, connect destination fd {}", slot_range_.load().String(), *dst_fd_);

  return Status::OK();
}

Status SlotMigrator::finishSuccessfulMigration() {
  if (stop_migration_) {
    return {Status::NotOK, std::string(errMigrationTaskCanceled)};
  }

  // Set import status on the destination node to SUCCESS
  auto s = setImportStatusOnDstNode(*dst_fd_, kImportSuccess);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSetImportStatus);
  }

  std::string dst_ip_port = dst_ip_ + ":" + std::to_string(dst_port_);
  s = srv_->cluster->SetSlotRangeMigrated(slot_range_, dst_ip_port);
  if (!s.IsOK()) {
    return s.Prefixed(
        fmt::format("failed to set slot(s) {} as migrated to {}", slot_range_.load().String(), dst_ip_port));
  }

  migrate_failed_slot_range_ = {-1, -1};

  return Status::OK();
}

Status SlotMigrator::finishFailedMigration() {
  // Stop slot will forbid writing
  migrate_failed_slot_range_ = slot_range_.load();
  forbidden_slot_range_ = {-1, -1};

  // Set import status on the destination node to FAILED
  auto s = setImportStatusOnDstNode(*dst_fd_, kImportFailed);
  if (!s.IsOK()) {
    return s.Prefixed(errFailedToSetImportStatus);
  }

  return Status::OK();
}

void SlotMigrator::clean() {
  INFO("[migrate] Clean resources of migrating slot(s) {}", slot_range_.load().String());
  if (slot_snapshot_) {
    storage_->GetDB()->ReleaseSnapshot(slot_snapshot_);
    slot_snapshot_ = nullptr;
  }

  current_stage_ = SlotMigrationStage::kNone;
  wal_begin_seq_ = 0;
  std::lock_guard<std::mutex> guard(job_mutex_);
  migration_job_.reset();
  dst_fd_.Reset();
  slot_range_ = {-1, -1};
  SetStopMigrationFlag(false);
}

Status SlotMigrator::authOnDstNode(int sock_fd, const std::string &password) {
  std::string cmd = redis::ArrayOfBulkStrings({"auth", password});
  auto s = util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send AUTH command");
  }

  s = checkSingleResponse(sock_fd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to check the response of AUTH command");
  }

  return Status::OK();
}

Status SlotMigrator::setImportStatusOnDstNode(int sock_fd, int status) {
  if (sock_fd <= 0) return {Status::NotOK, "invalid socket descriptor"};

  std::string cmd =
      redis::ArrayOfBulkStrings({"cluster", "import", slot_range_.load().String(), std::to_string(status)});
  auto s = util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send command to the destination node");
  }

  s = checkSingleResponse(sock_fd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to check the response from the destination node");
  }

  return Status::OK();
}

StatusOr<bool> SlotMigrator::supportedApplyBatchCommandOnDstNode(int sock_fd) {
  std::string cmd = redis::ArrayOfBulkStrings({"command", "info", "applybatch"});
  auto s = util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send command info to the destination node");
  }

  UniqueEvbuf evbuf;
  if (evbuffer_read(evbuf.get(), sock_fd, -1) <= 0) {
    return Status::FromErrno("read response error");
  }

  UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
  if (!line) {
    return Status::FromErrno("read empty response");
  }

  if (line[0] == '*') {
    line = UniqueEvbufReadln(evbuf.get(), EVBUFFER_EOL_LF);
    if (line && line[0] == '*') {
      return true;
    }
  }

  return false;
}

Status SlotMigrator::checkSingleResponse(int sock_fd) { return checkMultipleResponses(sock_fd, 1); }

Status SlotMigrator::checkMultipleResponses(int sock_fd, int total) {
  if (sock_fd < 0 || total <= 0) {
    return {Status::NotOK, fmt::format("invalid arguments: sock_fd={}, count={}", sock_fd, total)};
  }

  // Set socket receive timeout first
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  // Start checking response
  size_t bulk_or_array_len = 0;
  int cnt = 0;
  parser_state_ = ParserState::ArrayLen;
  UniqueEvbuf evbuf;
  while (true) {
    // Read response data from socket buffer to the event buffer
    if (evbuffer_read(evbuf.get(), sock_fd, -1) <= 0) {
      return {Status::NotOK, fmt::format("failed to read response: {}", strerror(errno))};
    }

    // Parse response data in event buffer
    bool run = true;
    while (run) {
      switch (parser_state_) {
        // Handle single string response
        case ParserState::ArrayLen: {
          UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
          if (!line) {
            INFO("[migrate] Event buffer is empty, read socket again");
            run = false;
            break;
          }

          if (line[0] == '-') {
            return {Status::NotOK, fmt::format("got invalid response of length {}: {}", line.length, line.get())};
          } else if (line[0] == '$' || line[0] == '*') {
            auto parse_result = ParseInt<uint64_t>(std::string(line.get() + 1, line.length - 1), 10);
            if (!parse_result) {
              return {Status::NotOK, "protocol error: expected integer value"};
            }

            bulk_or_array_len = *parse_result;
            if (bulk_or_array_len <= 0) {
              parser_state_ = ParserState::OneRspEnd;
            } else if (line[0] == '$') {
              parser_state_ = ParserState::BulkData;
            } else {
              parser_state_ = ParserState::ArrayData;
            }
          } else if (line[0] == '+' || line[0] == ':') {
            parser_state_ = ParserState::OneRspEnd;
          } else {
            return {Status::NotOK, fmt::format("got unexpected response of length {}: {}", line.length, line.get())};
          }

          break;
        }
        // Handle bulk string response
        case ParserState::BulkData: {
          if (evbuffer_get_length(evbuf.get()) < bulk_or_array_len + 2) {
            INFO("[migrate] Bulk data in event buffer is not complete, read socket again");
            run = false;
            break;
          }
          // TODO(chrisZMF): Check tail '\r\n'
          evbuffer_drain(evbuf.get(), bulk_or_array_len + 2);
          bulk_or_array_len = 0;
          parser_state_ = ParserState::OneRspEnd;
          break;
        }
        case ParserState::ArrayData: {
          while (run && bulk_or_array_len > 0) {
            evbuffer_ptr ptr = evbuffer_search_eol(evbuf.get(), nullptr, nullptr, EVBUFFER_EOL_CRLF_STRICT);
            if (ptr.pos < 0) {
              INFO("[migrate] Array data in event buffer is not complete, read socket again");
              run = false;
              break;
            }
            evbuffer_drain(evbuf.get(), ptr.pos + 2);
            --bulk_or_array_len;
          }
          if (run) {
            parser_state_ = ParserState::OneRspEnd;
          }
          break;
        }
        case ParserState::OneRspEnd: {
          cnt++;
          if (cnt >= total) {
            return Status::OK();
          }

          parser_state_ = ParserState::ArrayLen;
          break;
        }
        default:
          break;
      }
    }
  }
}

void SlotMigrator::setForbiddenSlotRange(const SlotRange &slot_range) {
  INFO("[migrate] Setting forbidden slot(s) {}", slot_range.String());
  // Block server to set forbidden slot
  uint64_t during = util::GetTimeStampUS();
  {
    auto exclusivity = srv_->WorkExclusivityGuard();
    forbidden_slot_range_ = slot_range;
  }
  during = util::GetTimeStampUS() - during;
  INFO("[migrate] To set forbidden slot, server was blocked for {} us", during);
}

void SlotMigrator::ReleaseForbiddenSlotRange() {
  INFO("[migrate] Release forbidden slot(s) {}", forbidden_slot_range_.load().String());
  forbidden_slot_range_ = {-1, -1};
}

void SlotMigrator::GetMigrationInfo(std::string *info) const {
  info->clear();
  if (!slot_range_.load().IsValid() && !forbidden_slot_range_.load().IsValid() &&
      !migrate_failed_slot_range_.load().IsValid()) {
    return;
  }

  SlotRange slot_range;
  std::string task_state;

  switch (migration_state_.load()) {
    case MigrationState::kNone:
      task_state = "none";
      break;
    case MigrationState::kStarted:
      task_state = "start";
      slot_range = slot_range_;
      break;
    case MigrationState::kSuccess:
      task_state = "success";
      slot_range = forbidden_slot_range_;
      break;
    case MigrationState::kFailed:
      task_state = "fail";
      slot_range = migrate_failed_slot_range_;
      break;
    default:
      break;
  }

  *info = fmt::format("migrating_slot(s): {}\r\ndestination_node: {}\r\nmigrating_state: {}\r\n", slot_range.String(),
                      dst_node_, task_state);
}

void SlotMigrator::CancelSyncCtx() {
  std::unique_lock<std::mutex> lock(blocking_mutex_);
  blocking_context_ = nullptr;
}

void SlotMigrator::resumeSyncCtx(const Status &migrate_result) {
  std::unique_lock<std::mutex> lock(blocking_mutex_);
  if (blocking_context_) {
    blocking_context_->Resume(migrate_result);

    blocking_context_ = nullptr;
  }
}

Status SlotMigrator::sendMigrationBatch(BatchSender *batch) {
  // user may dynamically change some configs, apply it when send data
  batch->SetMaxBytes(migrate_batch_size_bytes_);
  batch->SetBytesPerSecond(migrate_batch_bytes_per_sec_);
  return batch->Send();
}

Status SlotMigrator::sendSnapshot() {
  uint64_t start_ts = util::GetTimeStampMS();
  auto slot_range = slot_range_.load();
  INFO("[migrate] Migrating snapshot of slot(s) {} by raw key value", slot_range.String());

  auto prefix = ComposeSlotKeyPrefix(namespace_, slot_range.start);
  auto upper_bound = ComposeSlotKeyUpperBound(namespace_, slot_range.end);

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = slot_snapshot_;
  rocksdb::Slice prefix_slice(prefix);
  rocksdb::Slice upper_bound_slice(upper_bound);
  read_options.iterate_lower_bound = &prefix_slice;
  read_options.iterate_upper_bound = &upper_bound_slice;
  auto no_txn_ctx = engine::Context::NoTransactionContext(storage_);
  engine::DBIterator iter(no_txn_ctx, read_options);

  BatchSender batch_sender(*dst_fd_, migrate_batch_size_bytes_, migrate_batch_bytes_per_sec_);

  for (iter.Seek(prefix); iter.Valid(); iter.Next()) {
    // Iteration is out of range
    auto key_slot_id = ExtractSlotId(iter.Key());
    if (!slot_range.Contains(key_slot_id)) {
      break;
    }

    auto redis_type = iter.Type();
    std::string log_data;
    if (redis_type == RedisType::kRedisList) {
      redis::WriteBatchLogData batch_log_data(redis_type, {std::to_string(RedisCommand::kRedisCmdRPush)});
      log_data = batch_log_data.Encode();
    } else {
      redis::WriteBatchLogData batch_log_data(redis_type);
      log_data = batch_log_data.Encode();
    }
    batch_sender.SetPrefixLogData(log_data);

    GET_OR_RET(batch_sender.Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), iter.Key(), iter.Value()));
    if (batch_sender.IsFull()) {
      GET_OR_RET(sendMigrationBatch(&batch_sender));
    }

    auto subkey_iter = iter.GetSubKeyIterator();
    if (!subkey_iter) {
      continue;
    }

    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      GET_OR_RET(batch_sender.Put(subkey_iter->ColumnFamilyHandle(), subkey_iter->Key(), subkey_iter->Value()));
      if (batch_sender.IsFull()) {
        GET_OR_RET(sendMigrationBatch(&batch_sender));
      }

      if (redis_type == RedisType::kRedisZSet) {
        InternalKey internal_key(subkey_iter->Key(), storage_->IsSlotIdEncoded());
        auto score_key = subkey_iter->Value().ToString();
        score_key.append(subkey_iter->UserKey().ToString());
        auto score_key_bytes =
            InternalKey(iter.Key(), score_key, internal_key.GetVersion(), storage_->IsSlotIdEncoded()).Encode();
        GET_OR_RET(batch_sender.Put(storage_->GetCFHandle(ColumnFamilyID::SecondarySubkey), score_key_bytes, Slice()));
      }

      if (batch_sender.IsFull()) {
        GET_OR_RET(sendMigrationBatch(&batch_sender));
      }
    }
  }

  GET_OR_RET(sendMigrationBatch(&batch_sender));

  auto elapsed = util::GetTimeStampMS() - start_ts;
  INFO(
      "[migrate] Succeed to migrate snapshot range, slot(s): {}, elapsed: {} ms, sent: {} bytes, rate: {:.2f} kb/s, "
      "batches: {}, entries: {}",
      slot_range.String(), elapsed, batch_sender.GetSentBytes(), batch_sender.GetRate(start_ts),
      batch_sender.GetSentBatchesNum(), batch_sender.GetEntriesNum());

  return Status::OK();
}

Status SlotMigrator::syncWAL() {
  uint64_t start_ts = util::GetTimeStampMS();
  INFO("[migrate] Syncing WAL of slot(s) {} by raw key value", slot_range_.load().String());
  BatchSender batch_sender(*dst_fd_, migrate_batch_size_bytes_, migrate_batch_bytes_per_sec_);

  int epoch = 1;
  uint64_t wal_incremental_seq = 0;

  while (epoch <= kMaxLoopTimes) {
    if (catchUpIncrementalWAL()) {
      break;
    }
    wal_incremental_seq = storage_->GetDB()->GetLatestSequenceNumber();
    auto s = migrateIncrementalDataByRawKV(wal_incremental_seq, &batch_sender);
    if (!s.IsOK()) {
      return {Status::NotOK, fmt::format("migrate incremental data failed, {}", s.Msg())};
    }
    INFO("[migrate] Migrated incremental data, epoch: {}, seq from {} to {}", epoch, wal_begin_seq_,
         wal_incremental_seq);
    wal_begin_seq_ = wal_incremental_seq;
    epoch++;
  }

  setForbiddenSlotRange(slot_range_);

  wal_incremental_seq = storage_->GetDB()->GetLatestSequenceNumber();
  if (wal_incremental_seq > wal_begin_seq_) {
    auto s = migrateIncrementalDataByRawKV(wal_incremental_seq, &batch_sender);
    if (!s.IsOK()) {
      return {Status::NotOK, fmt::format("migrate last incremental data failed, {}", s.Msg())};
    }
    INFO("[migrate] Migrated last incremental data after set forbidden slot, seq from {} to {}", wal_begin_seq_,
         wal_incremental_seq);
  }

  auto elapsed = util::GetTimeStampMS() - start_ts;
  INFO(
      "[migrate] Succeed to migrate incremental data, slot(s): {}, elapsed: {} ms, "
      "sent: {} bytes, rate: {:.2f} kb/s, batches: {}, entries: {}",
      slot_range_.load().String(), elapsed, batch_sender.GetSentBytes(), batch_sender.GetRate(start_ts),
      batch_sender.GetSentBatchesNum(), batch_sender.GetEntriesNum());

  return Status::OK();
}

bool SlotMigrator::catchUpIncrementalWAL() {
  uint64_t gap = storage_->GetDB()->GetLatestSequenceNumber() - wal_begin_seq_;
  if (gap <= seq_gap_limit_) {
    INFO("[migrate] Incremental data sequence gap: {}, less than limit: {}, set forbidden slot(s): {}", gap,
         seq_gap_limit_, slot_range_.load().String());
    return true;
  }
  return false;
}

Status SlotMigrator::migrateIncrementalDataByRawKV(uint64_t end_seq, BatchSender *batch_sender) {
  engine::WALIterator wal_iter(storage_, slot_range_);
  uint64_t start_seq = wal_begin_seq_ + 1;
  for (wal_iter.Seek(start_seq); wal_iter.Valid(); wal_iter.Next()) {
    if (wal_iter.NextSequenceNumber() > end_seq + 1) {
      break;
    }
    auto item = wal_iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypeLogData: {
        GET_OR_RET(batch_sender->PutLogData(item.key));
        break;
      }
      case engine::WALItem::Type::kTypePut: {
        if (item.column_family_id > kMaxColumnFamilyID) {
          INFO("[migrate] Invalid put column family id: {}", item.column_family_id);
          continue;
        }
        GET_OR_RET(batch_sender->Put(storage_->GetCFHandle(static_cast<ColumnFamilyID>(item.column_family_id)),
                                     item.key, item.value));
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        if (item.column_family_id > kMaxColumnFamilyID) {
          INFO("[migrate] Invalid delete column family id: {}", item.column_family_id);
          continue;
        }
        GET_OR_RET(
            batch_sender->Delete(storage_->GetCFHandle(static_cast<ColumnFamilyID>(item.column_family_id)), item.key));
        break;
      }
      case engine::WALItem::Type::kTypeDeleteRange: {
        // Do nothing in DeleteRange due to it might cross multiple slots. It's only used in
        // FLUSHDB/FLUSHALL commands for now and maybe we can disable them while migrating.
      }
      default:
        break;
    }
    if (batch_sender->IsFull()) {
      GET_OR_RET(sendMigrationBatch(batch_sender));
    }
  }

  // send the remaining data
  return sendMigrationBatch(batch_sender);
}
