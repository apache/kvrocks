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

#include <glog/logging.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <rocksdb/transaction_log.h>
#include <rocksdb/write_batch.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "config.h"
#include "encoding.h"
#include "parse_util.h"
#include "redis_slot.h"
#include "server/server.h"
#include "slot_import.h"
#include "stats/stats.h"
#include "status.h"
#include "storage/redis_db.h"
#include "unique_fd.h"

enum class MigrationState { kNone = 0, kStarted, kSuccess, kFailed };

enum class SlotMigrationStage { kNone, kStart, kSnapshot, kWAL, kSuccess, kFailed, kClean };

enum class KeyMigrationResult { kMigrated, kExpired, kUnderlyingStructEmpty };

struct SlotMigrationJob {
  SlotMigrationJob(int slot_id, std::string dst_ip, int dst_port, int speed, int pipeline_size, int seq_gap)
      : slot_id(static_cast<int16_t>(slot_id)),
        dst_ip(std::move(dst_ip)),
        dst_port(dst_port),
        max_speed(speed),
        max_pipeline_size(pipeline_size),
        seq_gap_limit(seq_gap) {}
  SlotMigrationJob(const SlotMigrationJob &other) = delete;
  SlotMigrationJob &operator=(const SlotMigrationJob &other) = delete;
  ~SlotMigrationJob() = default;

  int16_t slot_id;
  std::string dst_ip;
  int dst_port;
  int max_speed;
  int max_pipeline_size;
  int seq_gap_limit;
};

class SlotMigrator : public redis::Database {
 public:
  explicit SlotMigrator(Server *svr, int max_migration_speed = kDefaultMaxMigrationSpeed,
                        int max_pipeline_size = kDefaultMaxPipelineSize, int seq_gap_limit = kDefaultSequenceGapLimit);
  SlotMigrator(const SlotMigrator &other) = delete;
  SlotMigrator &operator=(const SlotMigrator &other) = delete;
  ~SlotMigrator();

  Status CreateMigrationThread();
  Status PerformSlotMigration(const std::string &node_id, std::string &dst_ip, int dst_port, int slot_id, int speed,
                              int pipeline_size, int seq_gap);
  void ReleaseForbiddenSlot();
  void SetMaxMigrationSpeed(int value) {
    if (value >= 0) max_migration_speed_ = value;
  }
  void SetMaxPipelineSize(int value) {
    if (value > 0) max_pipeline_size_ = value;
  }
  void SetSequenceGapLimit(int value) {
    if (value > 0) seq_gap_limit_ = value;
  }
  void SetStopMigrationFlag(bool value) { stop_migration_ = value; }
  bool IsMigrationInProgress() const { return migration_state_ == MigrationState::kStarted; }
  SlotMigrationStage GetCurrentSlotMigrationStage() const { return current_stage_; }
  int16_t GetForbiddenSlot() const { return forbidden_slot_; }
  int16_t GetMigratingSlot() const { return migrating_slot_; }
  void GetMigrationInfo(std::string *info) const;

 private:
  void loop();
  void runMigrationProcess();
  bool isTerminated() { return thread_state_ == ThreadState::Terminated; }
  Status startMigration();
  Status sendSnapshot();
  Status syncWal();
  Status finishSuccessfulMigration();
  Status finishFailedMigration();
  void clean();

  Status authOnDstNode(int sock_fd, const std::string &password);
  Status setImportStatusOnDstNode(int sock_fd, int status);
  Status checkSingleResponse(int sock_fd);
  Status checkMultipleResponses(int sock_fd, int total);

  StatusOr<KeyMigrationResult> migrateOneKey(const rocksdb::Slice &key, const rocksdb::Slice &encoded_metadata,
                                             std::string *restore_cmds);
  Status migrateSimpleKey(const rocksdb::Slice &key, const Metadata &metadata, const std::string &bytes,
                          std::string *restore_cmds);
  Status migrateComplexKey(const rocksdb::Slice &key, const Metadata &metadata, std::string *restore_cmds);
  Status migrateStream(const rocksdb::Slice &key, const StreamMetadata &metadata, std::string *restore_cmds);
  Status migrateBitmapKey(const InternalKey &inkey, std::unique_ptr<rocksdb::Iterator> *iter,
                          std::vector<std::string> *user_cmd, std::string *restore_cmds);

  Status sendCmdsPipelineIfNeed(std::string *commands, bool need);
  void applyMigrationSpeedLimit() const;
  Status generateCmdsFromBatch(rocksdb::BatchResult *batch, std::string *commands);
  Status migrateIncrementData(std::unique_ptr<rocksdb::TransactionLogIterator> *iter, uint64_t end_seq);
  Status syncWalBeforeForbiddingSlot();
  Status syncWalAfterForbiddingSlot();
  void setForbiddenSlot(int16_t slot);

  enum class ParserState { ArrayLen, BulkLen, BulkData, OneRspEnd };
  enum class ThreadState { Uninitialized, Running, Terminated };

  static const int kDefaultMaxPipelineSize = 16;
  static const int kDefaultMaxMigrationSpeed = 4096;
  static const int kDefaultSequenceGapLimit = 10000;
  static const int kMaxItemsInCommand = 16;  // number of items in every write command of complex keys
  static const int kMaxLoopTimes = 10;

  Server *svr_;
  int max_migration_speed_;
  int max_pipeline_size_;
  int seq_gap_limit_;

  SlotMigrationStage current_stage_ = SlotMigrationStage::kNone;
  ParserState parser_state_ = ParserState::ArrayLen;
  std::atomic<ThreadState> thread_state_ = ThreadState::Uninitialized;
  std::atomic<MigrationState> migration_state_ = MigrationState::kNone;

  int current_pipeline_size_ = 0;
  uint64_t last_send_time_ = 0;

  std::thread t_;
  std::mutex job_mutex_;
  std::condition_variable job_cv_;
  std::unique_ptr<SlotMigrationJob> migration_job_;

  std::string dst_node_;
  std::string dst_ip_;
  int dst_port_ = -1;
  UniqueFD dst_fd_;

  std::atomic<int16_t> forbidden_slot_ = -1;
  std::atomic<int16_t> migrating_slot_ = -1;
  int16_t migrate_failed_slot_ = -1;
  std::atomic<bool> stop_migration_ = false;  // if is true migration will be stopped but the thread won't be destroyed
  const rocksdb::Snapshot *slot_snapshot_ = nullptr;
  uint64_t wal_begin_seq_ = 0;
};
