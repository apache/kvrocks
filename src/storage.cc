#include <event2/buffer.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <iostream>

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

class DebuggingLogger : public rocksdb::Logger {
 public:
  explicit DebuggingLogger(
      const rocksdb::InfoLogLevel log_level = rocksdb::InfoLogLevel::INFO_LEVEL)
      : Logger(log_level) {}

  // Brings overloaded Logv()s into scope so they're not hidden when we override
  // a subset of them.
  using Logger::Logv;

  virtual void Logv(const char *format, va_list ap) override {
    vfprintf(stderr, format, ap);
    fprintf(stderr, "\n");
  }
};

Status Storage::CreateBackup() {
  // TODO: assert role to be master. slaves never create backup, they sync
  rocksdb::BackupableDBOptions bk_option(backup_dir_);
  if (!backup_) {
    auto s = rocksdb::BackupEngine::Open(db_->GetEnv(), bk_option, &backup_);
    if (!s.ok()) return Status(Status::DBBackupErr, s.ToString());
  }

  auto tm = std::time(nullptr);
  auto s = backup_->CreateNewBackupWithMetadata(
      db_, std::asctime(std::localtime(&tm)));
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

Status Storage::RestoreFromBackup(rocksdb::SequenceNumber *seq) {
  // TODO: assert role to be slave
  // We must reopen the backup engine every time, as the files is changed
  rocksdb::BackupableDBOptions bk_option(backup_dir_);
#ifndef NDEBUG
  bk_option.info_log = new DebuggingLogger;
#endif
  auto s = rocksdb::BackupEngine::Open(db_->GetEnv(), bk_option, &backup_);
  if (!s.ok()) return Status(Status::DBBackupErr, s.ToString());

  // Reopen db. TODO: fix the race, use shared_ptr to hold the db ptr
  delete db_;

  s = backup_->RestoreDBFromLatestBackup(db_dir_, db_dir_);
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Failed to restore: " << s.ToString();
    return Status(Status::DBBackupErr, s.ToString());
  }
  LOG(INFO) << "[storage] Restore from backup";
  // FIXME: when delete db_ after restoring backup, SST files aren't
  // copied, causing db Open error. I haven't figure out why. :(
  //delete db_;
  auto s2 = Open();
  if (!s2.IsOK()) {
    LOG(ERROR) << "Failed to reopen db: " << s2.msg();
    return Status(Status::DBOpenErr);
  }
  *seq = LatestSeq();
  return Status::OK();
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
    rocksdb::Status s =
        db_->CompactRange(compact_opts, cf_handle, nullptr, nullptr);
    if (!s.ok()) return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::DB *Storage::GetDB() { return db_; }

// TODO: if meta_id == 0, return the latest metafile.
int Storage::BackupManager::OpenLatestMeta(Storage *storage,
                                           rocksdb::BackupID *meta_id,
                                           uint64_t *file_size) {
  if (!storage->CreateBackup().IsOK()) {
    LOG(ERROR) << "Failed to create new backup";
    return -1;
  }
  std::vector<rocksdb::BackupInfo> backup_infos;
  storage->backup_->GetBackupInfo(&backup_infos);
  auto latest_backup = backup_infos.back();
  if (!storage->backup_->VerifyBackup(latest_backup.backup_id).ok()) {
    LOG(ERROR) << "Backup verification failed";
    return -1;
  }
  *meta_id = latest_backup.backup_id;
  std::string meta_file =
      storage->backup_dir_ + "/meta/" + std::to_string(*meta_id);
  auto s = storage->backup_env_->FileExists(meta_file);
  storage->backup_env_->GetFileSize(meta_file, file_size);
  // NOTE: here we use the system's open instead of using rocksdb::Env to open
  // a sequential file, because we want to use sendfile syscall.
  auto rv = open(meta_file.c_str(), O_RDONLY);
  if (rv < 0) {
    LOG(ERROR) << "Failed to open file: " << strerror(errno);
  }
  return rv;
}

int Storage::BackupManager::OpenDataFile(Storage *storage, std::string rel_path,
                                         uint64_t *file_size) {
  std::string abs_path = storage->backup_dir_ + "/" + rel_path;
  auto s = storage->backup_env_->FileExists(abs_path);
  if (!s.ok()) {
    LOG(ERROR) << "Data file [" << abs_path << "] not found";
    return -1;
  }
  storage->backup_env_->GetFileSize(abs_path, file_size);
  auto rv = open(abs_path.c_str(), O_RDONLY);
  if (rv < 0) {
    LOG(ERROR) << "Failed to open file: " << strerror(errno);
  }
  return rv;
}

Storage::BackupManager::MetaInfo Storage::BackupManager::ParseMetaAndSave(
    Storage *storage, rocksdb::BackupID meta_id, evbuffer *evbuf) {
  char *line;
  size_t len;
  Storage::BackupManager::MetaInfo meta;
  auto meta_file = "meta/" + std::to_string(meta_id);
  DLOG(INFO) << "[meta] id: " << meta_id;

  // Save the meta to tmp file
  auto wf = NewTmpFile(storage, meta_file);
  auto data = evbuffer_pullup(evbuf, -1);
  wf->Append(rocksdb::Slice(reinterpret_cast<char *>(data),
                            evbuffer_get_length(evbuf)));
  wf->Close();

  // timestamp;
  line = evbuffer_readln(evbuf, &len, EVBUFFER_EOL_LF);
  DLOG(INFO) << "[meta] timestamp: " << line;
  meta.timestamp = std::strtoll(line, nullptr, 10);
  free(line);
  // sequence
  line = evbuffer_readln(evbuf, &len, EVBUFFER_EOL_LF);
  DLOG(INFO) << "[meta] seq:" << line;
  meta.seq = std::strtoull(line, nullptr, 10);
  free(line);
  // optional metadata
  line = evbuffer_readln(evbuf, &len, EVBUFFER_EOL_LF);
  if (strncmp(line, "metadata", 8) == 0) {
    DLOG(INFO) << "[meta] meta: " << line;
    meta.meta_data = std::string(line, len);
    free(line);
    line = evbuffer_readln(evbuf, &len, EVBUFFER_EOL_LF);
  }
  DLOG(INFO) << "[meta] file count: " << line;
  free(line);
  // file list
  while (true) {
    line = evbuffer_readln(evbuf, &len, EVBUFFER_EOL_LF);
    if (!line) {
      break;
    }
    DLOG(INFO) << "[meta] file info: " << line;
    auto cptr = line;
    while (*(cptr++) != ' ')
      ;
    auto filename = std::string(line, cptr - line - 1);
    while (*(cptr++) != ' ')
      ;
    auto crc32 = std::strtoul(cptr, nullptr, 10);
    meta.files.emplace_back(filename, crc32);
    free(line);
  }
  SwapTmpFile(storage, meta_file);
  return meta;
}

Status MkdirRecursively(rocksdb::Env *env, const std::string &dir) {
  if (env->CreateDirIfMissing(dir).ok()) return Status::OK();

  std::string parent;
  for (auto pos = dir.find('/', 1); pos != std::string::npos;
       pos = dir.find('/', pos + 1)) {
    parent = dir.substr(0, pos);
    if (!env->CreateDirIfMissing(parent).ok()) {
      LOG(ERROR) << "Failed to create directory recursively";
      return Status(Status::NotOK);
    }
  }
  if (env->CreateDirIfMissing(dir).ok()) return Status::OK();
  return Status::NotOK;
}

std::unique_ptr<rocksdb::WritableFile> Storage::BackupManager::NewTmpFile(
    Storage *storage, std::string rel_path) {
  std::string tmp_path = storage->backup_dir_ + "/" + rel_path + ".tmp";
  auto s = storage->backup_env_->FileExists(tmp_path);
  if (s.ok()) {
    LOG(ERROR) << "Data file exists, override";
    storage->backup_env_->DeleteFile(tmp_path);
  }
  // Create directory if missing
  auto abs_dir = tmp_path.substr(0, tmp_path.rfind('/'));
  if (!MkdirRecursively(storage->backup_env_, abs_dir).IsOK()) {
    return nullptr;
  }
  std::unique_ptr<rocksdb::WritableFile> wf;
  s = storage->backup_env_->NewWritableFile(tmp_path, &wf,
                                            rocksdb::EnvOptions());
  if (!s.ok()) {
    LOG(ERROR) << "Failed to create data file: " << s.ToString();
    return nullptr;
  }
  return wf;
}

Status Storage::BackupManager::SwapTmpFile(Storage *storage,
                                           std::string rel_path) {
  std::string tmp_path = storage->backup_dir_ + "/" + rel_path + ".tmp";
  std::string orig_path = storage->backup_dir_ + "/" + rel_path;
  if (!storage->backup_env_->RenameFile(tmp_path, orig_path).ok()) {
    LOG(ERROR) << "Failed to rename: " << tmp_path;
    return Status(Status::NotOK);
  }
  return Status::OK();
}

}  // namespace Engine
