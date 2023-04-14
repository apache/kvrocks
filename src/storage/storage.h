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

#include <event2/bufferevent.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/backup_engine.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <atomic>
#include <cinttypes>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "config/config.h"
#include "lock_manager.h"
#include "observer_or_unique.h"
#include "status.h"

const int kReplIdLength = 16;

enum ColumnFamilyID {
  kColumnFamilyIDDefault,
  kColumnFamilyIDMetadata,
  kColumnFamilyIDZSetScore,
  kColumnFamilyIDPubSub,
  kColumnFamilyIDPropagate,
  kColumnFamilyIDStream,
};

namespace Engine {

constexpr const char *kPubSubColumnFamilyName = "pubsub";
constexpr const char *kZSetScoreColumnFamilyName = "zset_score";
constexpr const char *kMetadataColumnFamilyName = "metadata";
constexpr const char *kSubkeyColumnFamilyName = "default";
constexpr const char *kPropagateColumnFamilyName = "propagate";
constexpr const char *kStreamColumnFamilyName = "stream";

constexpr const char *kPropagateScriptCommand = "script";

constexpr const char *kLuaFunctionPrefix = "lua_f_";

class Storage {
 public:
  explicit Storage(Config *config);
  ~Storage();

  void SetWriteOptions(const Config::RocksDB::WriteOptions &config);
  void SetReadOptions(rocksdb::ReadOptions &read_options);
  Status Open(bool read_only = false);
  void CloseDB();
  void EmptyDB();
  rocksdb::BlockBasedTableOptions InitTableOptions();
  void SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options);
  rocksdb::Options InitRocksDBOptions();
  Status SetOptionForAllColumnFamilies(const std::string &key, const std::string &value);
  Status SetOption(const std::string &key, const std::string &value);
  Status SetDBOption(const std::string &key, const std::string &value);
  Status CreateColumnFamilies(const rocksdb::Options &options);
  Status CreateBackup();
  void DestroyBackup();
  Status RestoreFromBackup();
  Status RestoreFromCheckpoint();
  Status GetWALIter(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator> *iter);
  Status ReplicaApplyWriteBatch(std::string &&raw_batch);
  rocksdb::SequenceNumber LatestSeqNumber();

  rocksdb::Status Get(const rocksdb::ReadOptions &options, const rocksdb::Slice &key, std::string *value);
  rocksdb::Status Get(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                      const rocksdb::Slice &key, std::string *value);
  void MultiGet(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family, size_t num_keys,
                const rocksdb::Slice *keys, rocksdb::PinnableSlice *values, rocksdb::Status *statuses);
  rocksdb::Iterator *NewIterator(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family);
  rocksdb::Iterator *NewIterator(const rocksdb::ReadOptions &options);

  rocksdb::Status Write(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates);
  const rocksdb::WriteOptions &DefaultWriteOptions() { return write_opts_; }
  rocksdb::Status Delete(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle,
                         const rocksdb::Slice &key);
  rocksdb::Status DeleteRange(const std::string &first_key, const std::string &last_key);
  rocksdb::Status FlushScripts(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle);
  bool WALHasNewData(rocksdb::SequenceNumber seq) { return seq <= LatestSeqNumber(); }
  Status InWALBoundary(rocksdb::SequenceNumber seq);
  Status WriteToPropagateCF(const std::string &key, const std::string &value);

  rocksdb::Status Compact(const rocksdb::Slice *begin, const rocksdb::Slice *end);
  rocksdb::DB *GetDB();
  bool IsClosing() const { return db_closing_; }
  std::string GetName() { return config_->db_name; }
  rocksdb::ColumnFamilyHandle *GetCFHandle(const std::string &name);
  std::vector<rocksdb::ColumnFamilyHandle *> *GetCFHandles() { return &cf_handles_; }
  LockManager *GetLockManager() { return &lock_mgr_; }
  void PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours);
  uint64_t GetTotalSize(const std::string &ns = kDefaultNamespace);
  void CheckDBSizeLimit();
  void SetIORateLimit(int64_t max_io_mb);

  std::shared_lock<std::shared_mutex> ReadLockGuard();
  std::unique_lock<std::shared_mutex> WriteLockGuard();

  uint64_t GetFlushCount() { return flush_count_; }
  void IncrFlushCount(uint64_t n) { flush_count_.fetch_add(n); }
  uint64_t GetCompactionCount() { return compaction_count_; }
  void IncrCompactionCount(uint64_t n) { compaction_count_.fetch_add(n); }
  bool IsSlotIdEncoded() { return config_->slot_id_encoded; }
  const Config *GetConfig() { return config_; }

  Status BeginTxn();
  Status CommitTxn();
  ObserverOrUniquePtr<rocksdb::WriteBatchBase> GetWriteBatchBase();

  Storage(const Storage &) = delete;
  Storage &operator=(const Storage &) = delete;

  // Full replication data files manager
  class ReplDataManager {
   public:
    // Master side
    static Status GetFullReplDataInfo(Storage *storage, std::string *files);
    static int OpenDataFile(Storage *storage, const std::string &rel_file, uint64_t *file_size);
    static Status CleanInvalidFiles(Storage *storage, const std::string &dir, std::vector<std::string> valid_files);
    struct CheckpointInfo {
      std::atomic<time_t> create_time = 0;
      std::atomic<time_t> access_time = 0;
      uint64_t latest_seq = 0;
    };

    // Slave side
    struct MetaInfo {
      int64_t timestamp;
      rocksdb::SequenceNumber seq;
      std::string meta_data;
      // [[filename, checksum]...]
      std::vector<std::pair<std::string, uint32_t>> files;
    };
    static Status ParseMetaAndSave(Storage *storage, rocksdb::BackupID meta_id, evbuffer *evbuf,
                                   Storage::ReplDataManager::MetaInfo *meta);
    static std::unique_ptr<rocksdb::WritableFile> NewTmpFile(Storage *storage, const std::string &dir,
                                                             const std::string &repl_file);
    static Status SwapTmpFile(Storage *storage, const std::string &dir, const std::string &repl_file);
    static bool FileExists(Storage *storage, const std::string &dir, const std::string &repl_file, uint32_t crc);
  };

  bool ExistCheckpoint();
  bool ExistSyncCheckpoint();
  time_t GetCheckpointCreateTime() { return checkpoint_info_.create_time; }
  void SetCheckpointAccessTime(time_t t) { checkpoint_info_.access_time = t; }
  time_t GetCheckpointAccessTime() { return checkpoint_info_.access_time; }
  void SetDBInRetryableIOError(bool yes_or_no) { db_in_retryable_io_error_ = yes_or_no; }
  bool IsDBInRetryableIOError() { return db_in_retryable_io_error_; }

  Status ShiftReplId();
  std::string GetReplIdFromWalBySeq(rocksdb::SequenceNumber seq);
  std::string GetReplIdFromDbEngine();

 private:
  rocksdb::DB *db_ = nullptr;
  std::string replid_;
  time_t backup_creating_time_;
  rocksdb::BackupEngine *backup_ = nullptr;
  rocksdb::Env *env_;
  std::shared_ptr<rocksdb::SstFileManager> sst_file_manager_;
  std::shared_ptr<rocksdb::RateLimiter> rate_limiter_;
  ReplDataManager::CheckpointInfo checkpoint_info_;
  std::mutex checkpoint_mu_;
  Config *config_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  LockManager lock_mgr_;
  bool db_size_limit_reached_ = false;
  std::atomic<uint64_t> flush_count_{0};
  std::atomic<uint64_t> compaction_count_{0};

  std::shared_mutex db_rw_lock_;
  bool db_closing_ = true;

  std::atomic<bool> db_in_retryable_io_error_{false};

  std::atomic<bool> is_txn_mode_ = false;
  // txn_write_batch_ is used as the global write batch for the transaction mode,
  // all writes will be grouped in this write batch when entering the transaction mode,
  // then write it at once when committing.
  //
  // Notice: the reason why we can use the global transaction? because the EXEC is an exclusive
  // command, so it won't have multi transactions to be executed at the same time.
  std::unique_ptr<rocksdb::WriteBatchWithIndex> txn_write_batch_;

  rocksdb::WriteOptions write_opts_ = rocksdb::WriteOptions();

  rocksdb::Status writeToDB(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates);
};

}  // namespace Engine
