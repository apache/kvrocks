#include <event2/buffer.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <iostream>

#include "config.h"
#include "storage.h"
#include "t_metadata.h"
#include "event_listener.h"

namespace Engine {

const char *kZSetScoreColumnFamilyName = "zset_score";
const char *kMetadataColumnFamilyName = "metadata";
using rocksdb::Slice;

class MetadataFilter : public rocksdb::CompactionFilter {
 public:
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override {
    std::string ns, real_key, bytes = value.ToString();
    Metadata metadata(kRedisNone);
    rocksdb::Status s = metadata.Decode(bytes);
    ExtractNamespaceKey(key, &ns, &real_key);
    if (!s.ok()) {
      LOG(WARNING) << "[Compacting metadata key] Failed to decode,"
                   << "namespace: " << ns
                   << "key: " << real_key
                   << ", err: " << s.ToString();
      return false;
    }
    DLOG(INFO) << "[Compacting metadata key]"
               << " namespace: " << ns
               << ", key: " << real_key
               << ", result: " << (metadata.Expired() ? "deleted":"reserved");
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

  bool IsKeyExpired(InternalKey &ikey) const {
    std::string metadata_key;

    ComposeNamespaceKey(ikey.GetNamespace(), ikey.GetKey(), &metadata_key);
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

  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override {
    InternalKey ikey(key);
    bool result = IsKeyExpired(ikey);
    DLOG(INFO) << "[Compacting subkey]"
               << " namespace: " << ikey.GetNamespace().ToString()
               << ", metadata key: "<< ikey.GetKey().ToString()
               << ", subkey: "<< ikey.GetSubKey().ToString()
               << ", verison: " << ikey.GetVersion()
               << ", result: " << (result ? "deleted":"reserved");
    return result;
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

void Engine::Storage::InitOptions(rocksdb::Options *options) {
  options->create_if_missing = true;
  // options.IncreaseParallelism(2);
  // NOTE: the overhead of statistics is 5%-10%, so it should be configurable in prod env
  // See: https://github.com/facebook/rocksdb/wiki/Statistics
  options->statistics = rocksdb::CreateDBStatistics();
  options->OptimizeLevelStyleCompaction();
  options->max_open_files = config_->rocksdb_options.max_open_files;
  options->max_subcompactions = config_->rocksdb_options.max_sub_compactions;
  options->max_background_flushes = config_->rocksdb_options.max_background_flushes;
  options->max_background_compactions = config_->rocksdb_options.max_background_compactions;
  options->max_write_buffer_number = config_->rocksdb_options.max_write_buffer_number;
  options->write_buffer_size =  config_->rocksdb_options.write_buffer_size;
  options->target_file_size_base = 256 * 1048576;
  options->max_manifest_file_size = 64 * 1024 * 1024;
  options->max_log_file_size = 512 * 1024 * 1024;
  options->WAL_ttl_seconds = 7 * 24 * 60 * 60;
  options->WAL_size_limit_MB = 3 * 1024;
  options->listeners.emplace_back(new CompactionEventListener());
}

Status Engine::Storage::CreateColumnFamiles(rocksdb::Options &options) {
  rocksdb::DB *tmp_db;
  rocksdb::ColumnFamilyOptions cf_options(options);
  rocksdb::Status s = rocksdb::DB::Open(options, config_->db_dir, &tmp_db);
  if (s.ok()) {
    std::vector<std::string> cf_names = {kMetadataColumnFamilyName,
                                         kZSetScoreColumnFamilyName};
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    s = tmp_db->CreateColumnFamilies(cf_options, cf_names, &cf_handles);
    if (!s.ok()) {
      delete tmp_db;
      return Status(Status::DBOpenErr, s.ToString());
    }
    for (auto handle : cf_handles) delete handle;
    delete tmp_db;
  }
  // Open db would be failed if the column families have already exists,
  // so we return ok here.
  return Status::OK();
}

Status Engine::Storage::Open() {
  rocksdb::Options options;
  InitOptions(&options);
  CreateColumnFamiles(options);
  rocksdb::BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));

  rocksdb::ColumnFamilyOptions metadata_opts(options);
  metadata_opts.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_opts));
  metadata_opts.compaction_filter_factory =
      std::make_shared<MetadataFilterFactory>();

  rocksdb::ColumnFamilyOptions subkey_opts(options);
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
      rocksdb::DB::Open(options, config_->db_dir, column_families, &cf_handles_, &db_);
  if (!s.ok()) {
    return Status(Status::DBOpenErr, s.ToString());
  }
  return Status::OK();
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
  LOG(INFO) << "Start to create new backup";
  rocksdb::BackupableDBOptions bk_option(config_->backup_dir);
  if (!backup_) {
    auto s = rocksdb::BackupEngine::Open(db_->GetEnv(), bk_option, &backup_);
    if (!s.ok()) return Status(Status::DBBackupErr, s.ToString());
  }

  auto tm = std::time(nullptr);
  auto s = backup_->CreateNewBackupWithMetadata(
      db_, std::asctime(std::localtime(&tm)));
  if (!s.ok()) return Status(Status::DBBackupErr, s.ToString());
  LOG(INFO) << "Success to create new backup";
  return Status::OK();
}

Status Storage::DestroyBackup() {
  backup_->StopBackup();
  auto env = rocksdb::Env::Default();
  env->DeleteDir(config_->backup_dir);
  delete backup_;
  return Status();
}

Status Storage::RestoreFromBackup(rocksdb::SequenceNumber *seq) {
  // TODO: assert role to be slave
  // We must reopen the backup engine every time, as the files is changed
  rocksdb::BackupableDBOptions bk_option(config_->backup_dir);
#ifndef NDEBUG
  bk_option.info_log = new DebuggingLogger;
#endif
  auto s = rocksdb::BackupEngine::Open(db_->GetEnv(), bk_option, &backup_);
  if (!s.ok()) return Status(Status::DBBackupErr, s.ToString());

  // Close DB;
  for (auto handle : cf_handles_) delete handle;
  delete db_;

  s = backup_->RestoreDBFromLatestBackup(config_->db_dir, config_->db_dir);
  if (!s.ok()) {
    LOG(ERROR) << "[storage_] Failed to restore: " << s.ToString();
    return Status(Status::DBBackupErr, s.ToString());
  }
  LOG(INFO) << "[storage_] Restore from backup";

  // Reopen DB
  auto s2 = Open();
  if (!s2.IsOK()) {
    LOG(ERROR) << "Failed to reopen db: " << s2.Msg();
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

// Get the timestamp data in the WriteBatch.
// we can use this info to indicate the progress of syncing.
class TimestampLogHandler : public rocksdb::WriteBatch::Handler {
 public:
  TimestampLogHandler() : rocksdb::WriteBatch::Handler() {}
  void LogData(const Slice& blob) override {
    LOG(INFO) << "[batch] Log data: " << blob.ToString();
  }
};

rocksdb::Status Storage::Write(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates) {
  // TODO: hook write op here.
  return db_->Write(options, updates);
}

Status Storage::WriteBatch(std::string &&raw_batch) {
  auto bat = rocksdb::WriteBatch(std::move(raw_batch));
  db_->Write(rocksdb::WriteOptions(), &bat);
  TimestampLogHandler handler;
  bat.Iterate(&handler);
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

std::vector<rocksdb::ColumnFamilyHandle *> * Storage::GetCFHandles() {
  return &cf_handles_;
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
  Status status = storage->CreateBackup();
  if (!status.IsOK()) {
    LOG(ERROR) << "Failed to create new backup, err:" << status.Msg();
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
      storage->config_->backup_dir + "/meta/" + std::to_string(*meta_id);
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
  std::string abs_path = storage->config_->backup_dir + "/" + rel_path;
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
  std::string tmp_path = storage->config_->backup_dir + "/" + rel_path + ".tmp";
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
  std::string tmp_path = storage->config_->backup_dir + "/" + rel_path + ".tmp";
  std::string orig_path = storage->config_->backup_dir + "/" + rel_path;
  if (!storage->backup_env_->RenameFile(tmp_path, orig_path).ok()) {
    LOG(ERROR) << "Failed to rename: " << tmp_path;
    return Status(Status::NotOK);
  }
  return Status::OK();
}

bool Storage::BackupManager::FileExists(Storage *storage, std::string rel_path) {
  auto s = storage->backup_env_->FileExists(storage->config_->backup_dir + "/" + rel_path);
  if (s.ok()) return true;
  return false;
}

}  // namespace Engine