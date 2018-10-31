#include <glog/logging.h>
#include <iostream>

#include <rocksdb/compaction_filter.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>

#include "storage.h"
#include "t_metadata.h"

namespace Engine {

const char *kZSetScoreColumnFamilyName = "zset_score";
const char *kMetadataColumnFamilyName = "metadata";
const char *kScanColumnFamilyName = "scan";
using rocksdb::Slice;

class MetadataFilter : public rocksdb::CompactionFilter {
 public:
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override {
    std::string bytes = value.ToString();
    Metadata metadata(kRedisNone);
    rocksdb::Status s = metadata.Decode(bytes);
    if (s.ok()) {
      // TODO： log error message while encounter coruption metadata
      return false;
    }
    return metadata.Expired();
  }
  const char *Name() const override { return "MetadataFilter"; }
};

class MetadataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  MetadataFilterFactory() = default;
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new MetadataFilter());
  }
  const char *Name() const override { return "MetadataFilterFactory"; }
};

class SubKeyFilter : public rocksdb::CompactionFilter {
 public:
  SubKeyFilter(rocksdb::DB **db,
               std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles)
      : cached_key_(""),
        cached_metadata_(""),
        db_(db),
        cf_handles_(cf_handles) {}
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override {
    InternalKey ikey(key);
    Slice metadata_key = ikey.GetKey();
    if (cached_key_.empty() || metadata_key != cached_key_) {
      std::string bytes;
      rocksdb::Status s = (*db_)->Get(rocksdb::ReadOptions(), (*cf_handles_)[1],
                                      metadata_key, &bytes);
      cached_key_ = metadata_key;
      if (s.ok()) {
        cached_metadata_ = bytes;
      } else if (s.IsNotFound()) {
        // metadata was deleted(perhaps compaction or manual)
        // clear the metadata
        cached_metadata_.clear();
        return true;
      } else {
        // failed to get metadata, clear the cached key and reserve
        cached_key_.clear();
        cached_metadata_.clear();
        return false;
      }
    }
    // the metadata was not found
    if (cached_metadata_.empty()) return true;
    // the metadata is cached
    Metadata metadata(kRedisNone);
    rocksdb::Status s = metadata.Decode(cached_metadata_);
    if (!s.ok()) {
      cached_key_.clear();
      return false;
    }
    if (metadata.Expired() || ikey.GetVersion() < metadata.version) {
      cached_metadata_.clear();
      return true;
    }
    return false;
  }
  const char *Name() const override { return "SubkeyFilter"; }

 protected:
  mutable Slice cached_key_;
  mutable std::string cached_metadata_;
  rocksdb::DB **db_;
  std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles_;
};

class SubKeyFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  explicit SubKeyFilterFactory(
      rocksdb::DB **db,
      std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles) {
    db_ = db;
    cf_handles_ = cf_handles;
  }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new SubKeyFilter(db_, cf_handles_));
  }
  const char *Name() const override { return "SubKeyFilterFactory"; }

 private:
  rocksdb::DB **db_;
  std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles_;
};

Status Engine::Storage::Open() {
  rocksdb::Options options;
  options.create_if_missing = true;
  //  options.IncreaseParallelism(2);
  options.OptimizeLevelStyleCompaction();
  options.WAL_ttl_seconds = 7 * 24 * 60 * 60;
  options.WAL_size_limit_MB = 3 * 1024;
  {
    rocksdb::DB *tmp_db;
    rocksdb::Status s = rocksdb::DB::Open(options, db_dir_, &tmp_db);
    if (s.ok()) {  // open will be failed, if the column family was exists
      std::vector<std::string> cf_names = {kMetadataColumnFamilyName,
                                           kZSetScoreColumnFamilyName};
      std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
      s = tmp_db->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(), cf_names,
                                       &cf_handles);
      if (!s.ok()) return Status(Status::DBOpenErr, s.ToString());
      for (auto handle : cf_handles) delete handle;
      delete tmp_db;
    }
  }
  rocksdb::BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  rocksdb::ColumnFamilyOptions metadata_opts;
  metadata_opts.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_opts));
  metadata_opts.compaction_filter_factory =
      std::make_shared<MetadataFilterFactory>();
  rocksdb::ColumnFamilyOptions subkey_opts;
  subkey_opts.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_opts));
  subkey_opts.compaction_filter_factory =
      std::make_shared<SubKeyFilterFactory>(&db_, &cf_handles_);
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Caution: don't change the order of column family, or the handle will be
  // mismatched
  column_families.emplace_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, subkey_opts));
  column_families.emplace_back(rocksdb::ColumnFamilyDescriptor(
      kMetadataColumnFamilyName, metadata_opts));
  column_families.emplace_back(
      rocksdb::ColumnFamilyDescriptor(kZSetScoreColumnFamilyName, subkey_opts));
  rocksdb::Status s =
      rocksdb::DB::Open(options, db_dir_, column_families, &cf_handles_, &db_);
  if (!s.ok()) {
    return Status(Status::DBOpenErr, s.ToString());
  }
  return Status::OK();
}

Status Storage::Set(const std::string &k, const std::string &v) {
  auto s = db_->Put(rocksdb::WriteOptions(), k, v);
  return Status::OK();
}

std::string Storage::Get(const std::string &k) {
  std::string v;
  auto s = db_->Get(rocksdb::ReadOptions(), k, &v);
  if (s.ok()) {
    return v;
  }
  return "not found";
}

Status Storage::CreateBackup() {
  rocksdb::BackupableDBOptions bk_option(backup_dir_);
  auto s =
      rocksdb::BackupEngine::Open(rocksdb::Env::Default(), bk_option, &backup_);
  if (!s.ok()) return Status(Status::DBOpenErr, s.ToString());

  auto tm = std::time(nullptr);
  s = backup_->CreateNewBackupWithMetadata(db_,
                                           std::asctime(std::localtime(&tm)));
  if (!s.ok()) return Status(Status::DBBackupErr, s.ToString());
  return Status::OK();
}

Status Storage::DestroyBackup() {
  backup_->StopBackup();
  auto env = rocksdb::Env::Default();
  env->DeleteDir(backup_dir_);
  delete backup_;
  return Status();
}

Status Storage::RestoreFromBackup() {
  // backup_->RestoreDBFromLatestBackup();
  return Status();
}

Status Storage::GetWALIter(
    rocksdb::SequenceNumber seq,
    std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
  auto s = db_->GetUpdatesSince(seq, iter);
  if (!s.ok()) return Status(Status::DBGetWALErr, s.ToString());
  return Status::OK();
}

rocksdb::SequenceNumber Storage::LatestSeq() {
  return db_->GetLatestSequenceNumber();
}

Status Storage::WriteBatch(std::string &&raw_batch) {
  auto bat = rocksdb::WriteBatch(std::move(raw_batch));
  db_->Write(rocksdb::WriteOptions(), &bat);
  return Status::OK();
}

rocksdb::ColumnFamilyHandle *Storage::GetCFHandle(std::string name) {
  if (name == kMetadataColumnFamilyName) {
    return cf_handles_[1];
  } else if (name == kZSetScoreColumnFamilyName) {
    return cf_handles_[2];
  }
  return cf_handles_[0];
}

rocksdb::Status Storage::Compact() {
  rocksdb::CompactRangeOptions compact_opts;
  compact_opts.change_level = true;
  for (auto cf_handle : cf_handles_) {
    rocksdb::Status s = db_->CompactRange(compact_opts, cf_handle, nullptr, nullptr);
    if (!s.ok()) return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::DB* Storage::GetDB() {
  return db_;
}
}  // namespace Engine
