#pragma once

#include <inttypes.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/backupable_db.h>
#include <event2/bufferevent.h>
#include <utility>
#include <memory>
#include <string>
#include <vector>
#include <atomic>

#include "status.h"
#include "lock_manager.h"
#include "config.h"

enum ColumnFamilyID{
  kColumnFamilyIDDefault,
  kColumnFamilyIDMetadata,
  kColumnFamilyIDZSetScore,
  kColumnFamilyIDPubSub,
};

namespace Engine {

class Storage {
 public:
  explicit Storage(Config *config)
      :backup_env_(rocksdb::Env::Default()),
       config_(config),
       lock_mgr_(16) {}
  ~Storage();

  Status Open(bool read_only);
  Status Open();
  Status OpenForReadOnly();
  void InitOptions(rocksdb::Options *options);
  Status CreateColumnFamiles(const rocksdb::Options &options);
  Status CreateBackup();
  Status DestroyBackup();
  Status RestoreFromBackup();
  Status GetWALIter(rocksdb::SequenceNumber seq,
                    std::unique_ptr<rocksdb::TransactionLogIterator> *iter);
  Status WriteBatch(std::string &&raw_batch);
  rocksdb::SequenceNumber LatestSeq();
  rocksdb::Status Write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates);
  bool WALHasNewData(rocksdb::SequenceNumber seq) { return seq <= LatestSeq(); }

  rocksdb::Status Compact(const rocksdb::Slice *begin, const rocksdb::Slice *end);
  rocksdb::DB *GetDB();
  const std::string GetName() {return config_->db_name; }
  rocksdb::ColumnFamilyHandle *GetCFHandle(const std::string &name);
  std::vector<rocksdb::ColumnFamilyHandle *> GetCFHandles() { return cf_handles_; }
  LockManager *GetLockManager() { return &lock_mgr_; }
  void PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours);
  uint64_t GetTotalSize();
  Status CheckDBSizeLimit();

  uint64_t GetFlushCount() { return flush_count_; }
  void IncrFlushCount(uint64_t n) { flush_count_.fetch_add(n); }
  uint64_t GetCompactionCount() { return compaction_count_; }
  void IncrCompactionCount(uint64_t n) { compaction_count_.fetch_add(n); }

  Storage(const Storage &) = delete;
  Storage &operator=(const Storage &) = delete;

  class BackupManager {
   public:
    // Master side
    static Status OpenLatestMeta(Storage *storage,
                                 int *fd,
                                 rocksdb::BackupID *meta_id,
                                 uint64_t *file_size);
    static int OpenDataFile(Storage *storage, const std::string &rel_path,
                            uint64_t *file_size);

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
        Storage *storage, const std::string &rel_path);
    static Status SwapTmpFile(Storage *storage, const std::string &rel_path);
    static bool FileExists(Storage *storage, const std::string &rel_path);
    static Status PurgeBackup(Storage *storage);
  };

 private:
  rocksdb::DB *db_ = nullptr;
  rocksdb::BackupEngine *backup_ = nullptr;
  rocksdb::Env *backup_env_;
  std::shared_ptr<rocksdb::SstFileManager> sst_file_manager_;
  Config *config_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  LockManager lock_mgr_;
  bool reach_db_size_limit_ = false;
  std::atomic<uint64_t> flush_count_{0};
  std::atomic<uint64_t> compaction_count_{0};
};

}  // namespace Engine
