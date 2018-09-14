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
      : db_dir_(std::move(db_dir)), backup_dir_(std::move(backup_dir)) {}

  Status Open();
  Status CreateBackup();
  Status DestroyBackup();
  Status RestoreFromBackup();
  Status GetWALIter(rocksdb::SequenceNumber seq,
                    std::unique_ptr<rocksdb::TransactionLogIterator> *iter);
  Status Set(const std::string &k, const std::string &v);
  std::string Get(const std::string &k);
  rocksdb::SequenceNumber LatestSeq();

  rocksdb::Status Compact();
  rocksdb::DB* GetDB();
  rocksdb::ColumnFamilyHandle *GetCFHandle(std::string name);

  ~Storage() { delete db_; }

  Storage(const Storage &) = delete;
  Storage &operator=(const Storage &) = delete;

 private:
  rocksdb::DB *db_ = nullptr;
  rocksdb::BackupEngine *backup_ = nullptr;
  std::string db_dir_;
  std::string backup_dir_;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
};
}  // namespace Engine
