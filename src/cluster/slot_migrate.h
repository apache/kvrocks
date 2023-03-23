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

constexpr const auto CLUSTER_SLOTS = HASH_SLOTS_SIZE;

enum MigrateTaskState { kMigrateNone = 0, kMigrateStarted, kMigrateSuccess, kMigrateFailed };

enum MigrateStateMachine {
  kSlotMigrateNone,
  kSlotMigrateStart,
  kSlotMigrateSnapshot,
  kSlotMigrateWal,
  kSlotMigrateSuccess,
  kSlotMigrateFailed,
  kSlotMigrateClean
};

enum class KeyMigrationResult { kMigrated, kExpired, kUnderlyingStructEmpty };

struct SlotMigrateJob {
  SlotMigrateJob(int slot, std::string dst_ip, int port, int speed, int pipeline_size, int seq_gap)
      : migrate_slot_(slot),
        dst_ip_(std::move(dst_ip)),
        dst_port_(port),
        speed_limit_(speed),
        pipeline_size_(pipeline_size),
        seq_gap_(seq_gap) {}
  SlotMigrateJob(const SlotMigrateJob &other) = delete;
  SlotMigrateJob &operator=(const SlotMigrateJob &other) = delete;
  ~SlotMigrateJob() { close(slot_fd_); }

  int slot_fd_ = -1;  // fd to send data to dst during migrate job
  int migrate_slot_;
  std::string dst_ip_;
  int dst_port_;
  int speed_limit_;
  int pipeline_size_;
  int seq_gap_;
};

class SlotMigrate : public Redis::Database {
 public:
  explicit SlotMigrate(Server *svr, int migration_speed = kDefaultMigrationSpeed,
                       int pipeline_size_limit = kDefaultPipelineSizeLimit, int seq_gap = kDefaultSeqGapLimit);
  SlotMigrate(const SlotMigrate &other) = delete;
  SlotMigrate &operator=(const SlotMigrate &other) = delete;
  ~SlotMigrate();

  Status CreateMigrateHandleThread();
  void Loop();
  Status MigrateStart(Server *svr, const std::string &node_id, const std::string &dst_ip, int dst_port, int slot,
                      int speed, int pipeline_size, int seq_gap);
  void ReleaseForbiddenSlot();
  void SetMigrateSpeedLimit(int speed) {
    if (speed >= 0) migration_speed_ = speed;
  }
  void SetPipelineSize(int value) {
    if (value > 0) pipeline_size_limit_ = value;
  }
  void SetSequenceGapSize(int size) {
    if (size > 0) seq_gap_limit_ = size;
  }
  void SetMigrateStopFlag(bool state) { stop_migrate_ = state; }
  bool IsMigrationInProgress() const { return migrate_state_ == kMigrateStarted; }
  int16_t GetMigrateStateMachine() const { return state_machine_; }
  int16_t GetForbiddenSlot() const { return forbidden_slot_; }
  int16_t GetMigratingSlot() const { return migrate_slot_; }
  void GetMigrateInfo(std::string *info) const;
  bool IsTerminated() { return thread_state_ == ThreadState::Terminated; }

 private:
  void RunStateMachine();
  Status Start();
  Status SendSnapshot();
  Status SyncWal();
  Status Success();
  Status Fail();
  void Clean();

  Status AuthDstServer(int sock_fd, const std::string &password);
  Status SetDstImportStatus(int sock_fd, int status);
  Status CheckResponseOnce(int sock_fd);
  Status CheckResponseWithCounts(int sock_fd, int total);

  StatusOr<KeyMigrationResult> MigrateOneKey(const rocksdb::Slice &key, const rocksdb::Slice &encoded_metadata,
                                             std::string *restore_cmds);
  Status MigrateSimpleKey(const rocksdb::Slice &key, const Metadata &metadata, const std::string &bytes,
                          std::string *restore_cmds);
  Status MigrateComplexKey(const rocksdb::Slice &key, const Metadata &metadata, std::string *restore_cmds);
  Status MigrateStream(const rocksdb::Slice &key, const StreamMetadata &metadata, std::string *restore_cmds);
  Status MigrateBitmapKey(const InternalKey &inkey, std::unique_ptr<rocksdb::Iterator> *iter,
                          std::vector<std::string> *user_cmd, std::string *restore_cmds);
  Status SendCmdsPipelineIfNeed(std::string *commands, bool need);
  void ApplyMigrationSpeedLimit() const;
  Status GenerateCmdsFromBatch(rocksdb::BatchResult *batch, std::string *commands);
  Status MigrateIncrementData(std::unique_ptr<rocksdb::TransactionLogIterator> *iter, uint64_t end_seq);
  Status SyncWalBeforeForbidSlot();
  Status SyncWalAfterForbidSlot();
  void SetForbiddenSlot(int16_t slot);

  enum class ParserState { ArrayLen, BulkLen, BulkData, OneRspEnd };
  enum class ThreadState { Uninitialized, Running, Terminated };

  static const size_t kProtoInlineMaxSize = 16 * 1024L;
  static const size_t kProtoBulkMaxSize = 512 * 1024L * 1024L;
  static const int kMaxNotifyRetryTimes = 3;
  static const int kDefaultPipelineSizeLimit = 16;
  static const int kDefaultMigrationSpeed = 4096;
  static const int kMaxItemsInCommand = 16;  // number of items in every write command of complex keys
  static const int kDefaultSeqGapLimit = 10000;
  static const int kMaxLoopTimes = 10;

  Server *svr_;
  MigrateStateMachine state_machine_ = kSlotMigrateNone;
  ParserState parser_state_ = ParserState::ArrayLen;
  std::atomic<ThreadState> thread_state_ = ThreadState::Uninitialized;

  int migration_speed_ = kDefaultMigrationSpeed;
  int pipeline_size_limit_ = kDefaultPipelineSizeLimit;
  int seq_gap_limit_ = kDefaultSeqGapLimit;
  int current_pipeline_size_ = 0;
  uint64_t last_send_time_ = 0;

  std::thread t_;
  std::mutex job_mutex_;
  std::condition_variable job_cv_;
  std::unique_ptr<SlotMigrateJob> slot_job_;
  std::string dst_node_;
  std::string dst_ip_;
  int dst_port_ = -1;
  std::atomic<int16_t> forbidden_slot_ = -1;
  std::atomic<int16_t> migrate_slot_ = -1;
  int16_t migrate_failed_slot_ = -1;
  std::atomic<MigrateTaskState> migrate_state_ = kMigrateNone;
  std::atomic<bool> stop_migrate_ = false;  // if is true migration will be stopped but the thread won't be destroyed
  std::string current_migrate_key_;
  uint64_t slot_snapshot_time_ = 0;
  const rocksdb::Snapshot *slot_snapshot_ = nullptr;
  uint64_t wal_begin_seq_ = 0;
  uint64_t wal_increment_seq_ = 0;
};
