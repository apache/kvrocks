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
#include <cstddef>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/port.h"
#include "config/config.h"
#include "lock_manager.h"
#include "observer_or_unique.h"
#include "status.h"

#if defined(__sparc__) || defined(__arm__)
#define USE_ALIGNED_ACCESS
#endif

enum class StorageEngineType : uint16_t {
  RocksDB,
  Speedb,
};

inline constexpr StorageEngineType STORAGE_ENGINE_TYPE = StorageEngineType::KVROCKS_STORAGE_ENGINE;

const int kReplIdLength = 16;

enum ColumnFamilyID {
  kColumnFamilyIDDefault = 0,
  kColumnFamilyIDMetadata,
  kColumnFamilyIDZSetScore,
  kColumnFamilyIDPubSub,
  kColumnFamilyIDPropagate,
  kColumnFamilyIDStream,
  kColumnFamilyIDSearch,
};

enum DBOpenMode {
  kDBOpenModeDefault,
  kDBOpenModeForReadOnly,
  kDBOpenModeAsSecondaryInstance,
};

namespace engine {

constexpr const char *kPubSubColumnFamilyName = "pubsub";
constexpr const char *kZSetScoreColumnFamilyName = "zset_score";
constexpr const char *kMetadataColumnFamilyName = "metadata";
constexpr const char *kSubkeyColumnFamilyName = "default";
constexpr const char *kPropagateColumnFamilyName = "propagate";
constexpr const char *kStreamColumnFamilyName = "stream";
constexpr const char *kSearchColumnFamilyName = "search";

constexpr const char *kPropagateScriptCommand = "script";

constexpr const char *kLuaFuncSHAPrefix = "lua_f_";
constexpr const char *kLuaFuncLibPrefix = "lua_func_lib_";
constexpr const char *kLuaLibCodePrefix = "lua_lib_code_";

struct CompressionOption {
  rocksdb::CompressionType type;
  const std::string name;
  const std::string val;
};

inline const std::vector<CompressionOption> CompressionOptions = {
    {rocksdb::kNoCompression, "no", "kNoCompression"},
    {rocksdb::kSnappyCompression, "snappy", "kSnappyCompression"},
    {rocksdb::kZlibCompression, "zlib", "kZlibCompression"},
    {rocksdb::kLZ4Compression, "lz4", "kLZ4Compression"},
    {rocksdb::kZSTD, "zstd", "kZSTD"},
};

struct CacheOption {
  BlockCacheType type;
  const std::string name;
  const std::string val;
};

inline const std::vector<CacheOption> CacheOptions = {
    {BlockCacheType::kCacheTypeLRU, "lru", "kCacheTypeLRU"},
    {BlockCacheType::kCacheTypeHCC, "hcc", "kCacheTypeHCC"},
};

enum class StatType : uint_fast8_t {
  CompactionCount,
  FlushCount,
  KeyspaceHits,
  KeyspaceMisses,
};

struct DBStats {
  alignas(CACHE_LINE_SIZE) std::atomic<uint_fast64_t> compaction_count = 0;
  alignas(CACHE_LINE_SIZE) std::atomic<uint_fast64_t> flush_count = 0;
  alignas(CACHE_LINE_SIZE) std::atomic<uint_fast64_t> keyspace_hits = 0;
  alignas(CACHE_LINE_SIZE) std::atomic<uint_fast64_t> keyspace_misses = 0;
};

class Storage {
 public:
  explicit Storage(Config *config);
  ~Storage();

  void SetWriteOptions(const Config::RocksDB::WriteOptions &config);
  Status Open(DBOpenMode mode = kDBOpenModeDefault);
  void CloseDB();
  bool IsEmptyDB();
  void EmptyDB();
  rocksdb::BlockBasedTableOptions InitTableOptions();
  void SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options);
  rocksdb::Options InitRocksDBOptions();
  Status SetOptionForAllColumnFamilies(const std::string &key, const std::string &value);
  Status SetDBOption(const std::string &key, const std::string &value);
  Status CreateColumnFamilies(const rocksdb::Options &options);
  // The sequence_number will be pointed to the value of the sequence number in range of DB,
  // but can't promise it's the latest sequence number. So you must check it by yourself before
  // using it.
  Status CreateBackup(uint64_t *sequence_number = nullptr);
  void DestroyBackup();
  Status RestoreFromBackup();
  Status RestoreFromCheckpoint();
  Status GetWALIter(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator> *iter);
  Status ReplicaApplyWriteBatch(std::string &&raw_batch);
  Status ApplyWriteBatch(const rocksdb::WriteOptions &options, std::string &&raw_batch);
  rocksdb::SequenceNumber LatestSeqNumber();

  [[nodiscard]] rocksdb::Status Get(const rocksdb::ReadOptions &options, const rocksdb::Slice &key, std::string *value);
  [[nodiscard]] rocksdb::Status Get(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                                    const rocksdb::Slice &key, std::string *value);
  [[nodiscard]] rocksdb::Status Get(const rocksdb::ReadOptions &options, const rocksdb::Slice &key,
                                    rocksdb::PinnableSlice *value);
  [[nodiscard]] rocksdb::Status Get(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                                    const rocksdb::Slice &key, rocksdb::PinnableSlice *value);
  void MultiGet(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family, size_t num_keys,
                const rocksdb::Slice *keys, rocksdb::PinnableSlice *values, rocksdb::Status *statuses);
  rocksdb::Iterator *NewIterator(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family);
  rocksdb::Iterator *NewIterator(const rocksdb::ReadOptions &options);

  [[nodiscard]] rocksdb::Status Write(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates);
  const rocksdb::WriteOptions &DefaultWriteOptions() { return default_write_opts_; }
  rocksdb::ReadOptions DefaultScanOptions() const;
  rocksdb::ReadOptions DefaultMultiGetOptions() const;
  [[nodiscard]] rocksdb::Status Delete(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle,
                                       const rocksdb::Slice &key);
  [[nodiscard]] rocksdb::Status DeleteRange(const std::string &first_key, const std::string &last_key);
  [[nodiscard]] rocksdb::Status FlushScripts(const rocksdb::WriteOptions &options,
                                             rocksdb::ColumnFamilyHandle *cf_handle);
  bool WALHasNewData(rocksdb::SequenceNumber seq) { return seq <= LatestSeqNumber(); }
  Status InWALBoundary(rocksdb::SequenceNumber seq);
  Status WriteToPropagateCF(const std::string &key, const std::string &value);

  [[nodiscard]] rocksdb::Status Compact(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice *begin,
                                        const rocksdb::Slice *end);
  rocksdb::DB *GetDB();
  bool IsClosing() const { return db_closing_; }
  std::string GetName() const { return config_->db_name; }
  rocksdb::ColumnFamilyHandle *GetCFHandle(const std::string &name);
  rocksdb::ColumnFamilyHandle *GetCFHandle(ColumnFamilyID id);
  std::vector<rocksdb::ColumnFamilyHandle *> *GetCFHandles() { return &cf_handles_; }
  LockManager *GetLockManager() { return &lock_mgr_; }
  void PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours);
  uint64_t GetTotalSize(const std::string &ns = kDefaultNamespace);
  void CheckDBSizeLimit();
  bool ReachedDBSizeLimit() { return db_size_limit_reached_; }
  void SetDBSizeLimit(bool limit) { db_size_limit_reached_ = limit; }
  void SetIORateLimit(int64_t max_io_mb);

  std::shared_lock<std::shared_mutex> ReadLockGuard();
  std::unique_lock<std::shared_mutex> WriteLockGuard();

  bool IsSlotIdEncoded() const { return config_->slot_id_encoded; }
  Config *GetConfig() const { return config_; }

  const DBStats *GetDBStats() const { return db_stats_.get(); }
  void RecordStat(StatType type, uint64_t v);

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
      // System clock time when the checkpoint was created.
      std::atomic<int64_t> create_time_secs = 0;
      // System clock time when the checkpoint was last accessed.
      std::atomic<int64_t> access_time_secs = 0;
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
  int64_t GetCheckpointCreateTimeSecs() const { return checkpoint_info_.create_time_secs; }
  void SetCheckpointAccessTimeSecs(int64_t t) { checkpoint_info_.access_time_secs = t; }
  int64_t GetCheckpointAccessTimeSecs() const { return checkpoint_info_.access_time_secs; }
  void SetDBInRetryableIOError(bool yes_or_no) { db_in_retryable_io_error_ = yes_or_no; }
  bool IsDBInRetryableIOError() const { return db_in_retryable_io_error_; }

  Status ShiftReplId();
  std::string GetReplIdFromWalBySeq(rocksdb::SequenceNumber seq);
  std::string GetReplIdFromDbEngine();

 private:
  std::unique_ptr<rocksdb::DB> db_ = nullptr;
  std::string replid_;
  // The system clock time when the backup was created.
  int64_t backup_creating_time_secs_;
  std::unique_ptr<rocksdb::BackupEngine> backup_ = nullptr;
  rocksdb::Env *env_;
  std::shared_ptr<rocksdb::SstFileManager> sst_file_manager_;
  std::shared_ptr<rocksdb::RateLimiter> rate_limiter_;
  ReplDataManager::CheckpointInfo checkpoint_info_;
  std::mutex checkpoint_mu_;
  Config *config_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  LockManager lock_mgr_;
  std::atomic<bool> db_size_limit_reached_{false};

  std::unique_ptr<DBStats> db_stats_;

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

  rocksdb::WriteOptions default_write_opts_ = rocksdb::WriteOptions();

  rocksdb::Status writeToDB(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates);
  void recordKeyspaceStat(const rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Status &s);
};

}  // namespace engine
