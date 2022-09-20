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

#include <inttypes.h>
#include <utility>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/backup_engine.h>
#include <event2/bufferevent.h>

#include "status.h"
#include "lock_manager.h"
#include "config.h"
#include "rw_lock.h"

const int kReplIdLength = 16;

enum ColumnFamilyID{
  kColumnFamilyIDDefault,
  kColumnFamilyIDMetadata,
  kColumnFamilyIDZSetScore,
  kColumnFamilyIDPubSub,
  kColumnFamilyIDPropagate,
  kColumnFamilyIDStream,
};

namespace Engine {
extern const char *kPubSubColumnFamilyName;
extern const char *kZSetScoreColumnFamilyName;
extern const char *kMetadataColumnFamilyName;
extern const char *kSubkeyColumnFamilyName;
extern const char *kPropagateColumnFamilyName;
extern const char *kStreamColumnFamilyName;

extern const char *kPropagateScriptCommand;

extern const char *kLuaFunctionPrefix;

class Storage {
 public:
  explicit Storage(Config *config);
  ~Storage();

  void SetWriteOptions(const Config::RocksDB::WriteOptions& config);
  Status Open(bool read_only);
  Status Open();
  Status OpenForReadOnly();
  void CloseDB();
  void EmptyDB();
  rocksdb::BlockBasedTableOptions InitTableOptions();
  void SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options);
  rocksdb::Options InitOptions();
  Status SetColumnFamilyOption(const std::string &key, const std::string &value);
  Status SetOption(const std::string &key, const std::string &value);
  Status SetDBOption(const std::string &key, const std::string &value);
  Status CreateColumnFamilies(const rocksdb::Options &options);
  Status CreateBackup();
  Status DestroyBackup();
  Status RestoreFromBackup();
  Status RestoreFromCheckpoint();
  Status GetWALIter(rocksdb::SequenceNumber seq,
                    std::unique_ptr<rocksdb::TransactionLogIterator> *iter);
  Status ReplicaApplyWriteBatch(std::string &&raw_batch);
  rocksdb::SequenceNumber LatestSeq();
  rocksdb::Status Write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates);
  const rocksdb::WriteOptions& DefaultWriteOptions() { return write_opts_; }
  rocksdb::Status Delete(const rocksdb::WriteOptions &options,
                         rocksdb::ColumnFamilyHandle *cf_handle,
                         const rocksdb::Slice &key);
  rocksdb::Status DeleteRange(const std::string &first_key, const std::string &last_key);
  rocksdb::Status FlushScripts(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle);
  bool WALHasNewData(rocksdb::SequenceNumber seq) { return seq <= LatestSeq(); }
  Status WriteToPropagateCF(const std::string &key, const std::string &value);

  rocksdb::Status Compact(const rocksdb::Slice *begin, const rocksdb::Slice *end);
  rocksdb::DB *GetDB();
  bool IsClosing() { return db_closing_; }
  const std::string GetName() {return config_->db_name; }
  rocksdb::ColumnFamilyHandle *GetCFHandle(const std::string &name);
  std::vector<rocksdb::ColumnFamilyHandle *>* GetCFHandles() { return &cf_handles_; }
  LockManager *GetLockManager() { return &lock_mgr_; }
  void PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours);
  uint64_t GetTotalSize(const std::string &ns = kDefaultNamespace);
  Status CheckDBSizeLimit();
  void SetIORateLimit(uint64_t max_io_mb);

  std::unique_ptr<RWLock::ReadLock> ReadLockGuard();
  std::unique_ptr<RWLock::WriteLock> WriteLockGuard();

  uint64_t GetFlushCount() { return flush_count_; }
  void IncrFlushCount(uint64_t n) { flush_count_.fetch_add(n); }
  uint64_t GetCompactionCount() { return compaction_count_; }
  void IncrCompactionCount(uint64_t n) { compaction_count_.fetch_add(n); }
  bool IsSlotIdEncoded() { return config_->slot_id_encoded; }

  Storage(const Storage &) = delete;
  Storage &operator=(const Storage &) = delete;

  // Full replication data files manager
  class ReplDataManager {
   public:
    // Master side
    static Status GetFullReplDataInfo(Storage *storage, std::string *files);
    static int OpenDataFile(Storage *storage, const std::string &rel_file,
                            uint64_t *file_size);
    static Status CleanInvalidFiles(Storage *storage,
      const std::string &dir, std::vector<std::string> valid_files);
    struct CheckpointInfo {
      std::atomic<bool>   is_creating;
      std::atomic<time_t> create_time;
      std::atomic<time_t> access_time;
    };

    // Slave side
    struct MetaInfo {
      int64_t timestamp;
      rocksdb::SequenceNumber seq;
      std::string meta_data;
      // [[filename, checksum]...]
      std::vector<std::pair<std::string, uint32_t>> files;
    };
    static MetaInfo ParseMetaAndSave(Storage *storage,
                                     rocksdb::BackupID meta_id,
                                     evbuffer *evbuf);
    static std::unique_ptr<rocksdb::WritableFile> NewTmpFile(
        Storage *storage, const std::string &dir, const std::string &repl_file);
    static Status SwapTmpFile(Storage *storage, const std::string &dir,
        const std::string &repl_file);
    static bool FileExists(Storage *storage, const std::string &dir,
        const std::string &repl_file, uint32_t crc);
  };

  bool ExistCheckpoint(void);
  bool ExistSyncCheckpoint(void);
  void SetCheckpointCreateTime(time_t t)  { checkpoint_info_.create_time = t; }
  time_t GetCheckpointCreateTime()  { return checkpoint_info_.create_time; }
  void SetCheckpointAccessTime(time_t t)  { checkpoint_info_.access_time = t; }
  time_t GetCheckpointAccessTime()  { return checkpoint_info_.access_time; }
  void SetDBInRetryableIOError(bool yes_or_no) { db_in_retryable_io_error_ = yes_or_no; }
  bool IsDBInRetryableIOError() { return db_in_retryable_io_error_; }

  bool ShiftReplId(void);
  std::string GetReplIdFromWalBySeq(rocksdb::SequenceNumber seq);
  std::string GetReplIdFromDbEngine(void);

 private:
  rocksdb::DB *db_ = nullptr;
  std::string replid_;
  std::mutex backup_mu_;
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
  bool reach_db_size_limit_ = false;
  std::atomic<uint64_t> flush_count_{0};
  std::atomic<uint64_t> compaction_count_{0};

  RWLock::ReadWriteLock db_rw_lock_;
  bool db_closing_ = true;

  std::atomic<bool> db_in_retryable_io_error_{false};

  rocksdb::WriteOptions write_opts_ = rocksdb::WriteOptions();
};

}  // namespace Engine
