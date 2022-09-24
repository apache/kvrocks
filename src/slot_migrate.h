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

#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <map>
#include <memory>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/db.h>
#include <rocksdb/transaction_log.h>
#include <glog/logging.h>

#include "redis_db.h"
#include "config.h"
#include "server.h"
#include "stats.h"
#include "util.h"
#include "slot_import.h"
#include "encoding.h"
#include "status.h"
#include "redis_slot.h"

#define CLUSTER_SLOTS HASH_SLOTS_SIZE

enum MigrateTaskState {
  kMigrateNone = 0,
  kMigrateStart,
  kMigrateSuccess,
  kMigrateFailed
};

enum MigrateStateMachine {
  kSlotMigrateNone,
  kSlotMigrateStart,
  kSlotMigrateSnapshot,
  kSlotMigrateWal,
  kSlotMigrateSuccess,
  kSlotMigrateFailed,
  kSlotMigrateClean
};

struct SlotMigrateJob {
  SlotMigrateJob(int slot, std::string dst_ip, int port,
                 int speed, int pipeline_size, int seq_gap) :
                 migrate_slot_(slot), dst_ip_(dst_ip), dst_port_(port),
                 speed_limit_(speed), pipeline_size_(pipeline_size), seq_gap_(seq_gap) {}
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
  explicit SlotMigrate(Server *svr, int speed = kMigrateSpeed,
                       int pipeline_size = kPipelineSize, int seq_gap = kSeqGapLimit);
  ~SlotMigrate() {}

  Status CreateMigrateHandleThread(void);
  void *Loop(void *arg);
  Status MigrateStart(Server *svr, const std::string &node_id, const std::string dst_ip,
                      int dst_port, int slot, int speed, int pipeline_size, int seq_gap);
  void ReleaseForbiddenSlot();
  void SetMigrateSpeedLimit(int speed) { if (speed >= 0) migrate_speed_ = speed; }
  void SetPipelineSize(uint32_t size) { if (size > 0) pipeline_size_limit_ = size; }
  void SetSequenceGapSize(int size) { if (size > 0) seq_gap_limit_ = size; }
  void SetMigrateStopFlag(bool state) { stop_ = state; }
  int16_t GetMigrateState() { return migrate_state_; }
  int16_t GetMigrateStateMachine() { return state_machine_; }
  int16_t GetForbiddenSlot(void) { return forbidden_slot_; }
  int16_t GetMigratingSlot(void) { return migrate_slot_; }
  void GetMigrateInfo(std::string *info);

 private:
  void StateMachine(void);
  Status Start(void);
  Status SendSnapshot(void);
  Status SyncWal(void);
  Status Success(void);
  Status Fail(void);
  Status Clean(void);

  bool AuthDstServer(int sock_fd, std::string password);
  bool SetDstImportStatus(int sock_fd, int status);
  bool CheckResponseOnce(int sock_fd);
  bool CheckResponseWithCounts(int sock_fd, int total);

  Status MigrateOneKey(const rocksdb::Slice &key, const rocksdb::Slice &value, std::string *restore_cmds);
  bool MigrateSimpleKey(const rocksdb::Slice &key, const Metadata &metadata,
                        const std::string &bytes, std::string *restore_cmds);
  bool MigrateComplexKey(const rocksdb::Slice &key, const Metadata &metadata, std::string *restore_cmds);
  bool MigrateBitmapKey(const InternalKey &inkey,
                        std::unique_ptr<rocksdb::Iterator> *iter,
                        std::vector<std::string> *user_cmd,
                        std::string *restore_cmds);
  bool SendCmdsPipelineIfNeed(std::string *commands, bool need);
  void MigrateSpeedLimit(void);
  Status GenerateCmdsFromBatch(rocksdb::BatchResult *batch, std::string *commands);
  Status MigrateIncrementData(std::unique_ptr<rocksdb::TransactionLogIterator> *iter, uint64_t endseq);
  Status SyncWalBeforeForbidSlot(void);
  Status SyncWalAfterForbidSlot(void);
  void MigrateWaitCmmdsFinish(void);
  void SetForbiddenSlot(int16_t slot);

 private:
  Server *svr_;

  MigrateStateMachine state_machine_;

  enum ParserState {
    ArrayLen,
    BulkLen,
    BulkData,
    Error,
    OneRspEnd
  };
  ParserState stat_ = ArrayLen;

  static const size_t kProtoInlineMaxSize = 16 * 1024L;
  static const size_t kProtoBulkMaxSize = 512 * 1024L * 1024L;
  static const int kMaxNotifyRetryTimes = 3;
  static const int kPipelineSize = 16;
  static const int kMigrateSpeed = 4096;
  static const int kMaxItemsInCommand = 16;   // Iterms in every write commmand of complex keys
  static const int kSeqGapLimit = 10000;
  static const int kMaxLoopTimes = 10;

  int current_pipeline_size_;
  int migrate_speed_ = kMigrateSpeed;
  uint64_t last_send_time_;

  std::thread t_;
  std::mutex job_mutex_;
  std::condition_variable job_cv_;
  std::unique_ptr<SlotMigrateJob> slot_job_ = nullptr;
  std::string dst_node_;
  std::string dst_ip_;
  int dst_port_;
  std::atomic<int16_t> forbidden_slot_;
  std::atomic<int16_t> migrate_slot_;
  int16_t migrate_failed_slot_;
  std::atomic<MigrateTaskState> migrate_state_;
  std::atomic<bool> stop_;
  std::string current_migrate_key_;
  uint64_t slot_snapshot_time_;
  const rocksdb::Snapshot *slot_snapshot_;
  uint64_t wal_begin_seq_;
  uint64_t wal_increment_seq_;

  int pipeline_size_limit_ = kPipelineSize;
  int seq_gap_limit_ = kSeqGapLimit;
};


