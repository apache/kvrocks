#pragma once

#include <event2/bufferevent.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/backupable_db.h>
#include <utility>

#include "status.h"

namespace Engine {

class Storage {
 public:
  explicit Storage(std::string db_dir, std::string backup_dir)
      : db_dir_(std::move(db_dir)),
        backup_dir_(std::move(backup_dir)),
        backup_env_(rocksdb::Env::Default()) {}

  Status Open();
  Status CreateBackup();
  Status DestroyBackup();
  Status RestoreFromBackup();
  Status GetWALIter(rocksdb::SequenceNumber seq,
                    std::unique_ptr<rocksdb::TransactionLogIterator> *iter);
  Status Set(const std::string &k, const std::string &v);
  std::string Get(const std::string &k);
  Status WriteBatch(std::string &&raw_batch);
  rocksdb::SequenceNumber LatestSeq();

  rocksdb::Status Compact();
  rocksdb::DB *GetDB();
  rocksdb::ColumnFamilyHandle *GetCFHandle(std::string name);

  ~Storage() { delete db_; }

  Storage(const Storage &) = delete;
  Storage &operator=(const Storage &) = delete;

  class BackupManager {
   public:
    // Master side
    static int OpenMetaFileByID(Storage *storage, rocksdb::BackupID meta_id,
                                uint64_t *file_size);
    static int OpenDataFile(Storage *storage, std::string rel_path,
                            uint64_t *file_size);

    // Slave side
    struct MetaInfo {
      int64_t timestamp;
      rocksdb::SequenceNumber seq;
      std::string meta_data;
      // [[filename, checksum]...]
      std::vector<std::pair<std::string, uint32_t>> files;
    };
    static MetaInfo ParseMeta(evbuffer *evbuf);
    static std::unique_ptr<rocksdb::WritableFile> NewTmpFile(Storage *storage, std::string rel_path);
    static Status SwapTmpFile(Storage *storage, std::string rel_path);
  };

 private:
  rocksdb::DB *db_ = nullptr;
  rocksdb::BackupEngine *backup_ = nullptr;
  std::string db_dir_;
  std::string backup_dir_;
  rocksdb::Env *backup_env_;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
};

}  // namespace Engine
