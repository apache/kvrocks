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

#include "storage.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <iostream>
#include <memory>
#include <algorithm>
#include <event2/buffer.h>
#include <glog/logging.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/utilities/table_properties_collectors.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/env.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/convenience.h>

#include "config.h"
#include "redis_db.h"
#include "rocksdb_crc32c.h"
#include "redis_metadata.h"
#include "event_listener.h"
#include "compact_filter.h"
#include "table_properties_collector.h"

namespace Engine {

const char *kPubSubColumnFamilyName = "pubsub";
const char *kZSetScoreColumnFamilyName = "zset_score";
const char *kMetadataColumnFamilyName = "metadata";
const char *kSubkeyColumnFamilyName = "default";
const char *kPropagateColumnFamilyName = "propagate";

const char *kPropagateScriptCommand = "script";

const char *kLuaFunctionPrefix = "lua_f_";

const uint64_t kIORateLimitMaxMb = 1024000;

using rocksdb::Slice;

Storage::Storage(Config *config)
    : env_(rocksdb::Env::Default()),
      config_(config),
      lock_mgr_(16) {
  Metadata::InitVersionCounter();
  SetCheckpointCreateTime(0);
  SetCheckpointAccessTime(0);
  backup_creating_time_ = std::time(nullptr);
}

Storage::~Storage() {
  if (backup_ != nullptr) {
    DestroyBackup();
  }
  CloseDB();
}

void Storage::CloseDB() {
  auto guard = WriteLockGuard();
  if (db_ == nullptr) return;

  db_closing_ = true;
  db_->SyncWAL();
  rocksdb::CancelAllBackgroundWork(db_, true);
  for (auto handle : cf_handles_) db_->DestroyColumnFamilyHandle(handle);
  delete db_;
  db_ = nullptr;
}

void Storage::InitTableOptions(rocksdb::BlockBasedTableOptions *table_options) {
  table_options->format_version = 5;
  table_options->index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_options->partition_filters = true;
  table_options->optimize_filters_for_memory = true;
  table_options->metadata_block_size = 4096;
  table_options->data_block_index_type =
   rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash;
  table_options->data_block_hash_table_util_ratio = 0.75;
  table_options->block_size = static_cast<size_t>(config_->RocksDB.block_size);
}

void Storage::SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options) {
  cf_options->enable_blob_files = config_->RocksDB.enable_blob_files;
  cf_options->min_blob_size = config_->RocksDB.min_blob_size;
  cf_options->blob_file_size = config_->RocksDB.blob_file_size * MiB;
  cf_options->blob_compression_type = static_cast<rocksdb::CompressionType>(config_->RocksDB.compression);
  cf_options->enable_blob_garbage_collection = config_->RocksDB.enable_blob_garbage_collection;
  // Use 100.0 to force converting blob_garbage_collection_age_cutoff to double
  cf_options->blob_garbage_collection_age_cutoff = config_->RocksDB.blob_garbage_collection_age_cutoff / 100.0;
}

void Storage::InitOptions(rocksdb::Options *options) {
  options->create_if_missing = true;
  options->create_missing_column_families = true;
  // options.IncreaseParallelism(2);
  // NOTE: the overhead of statistics is 5%-10%, so it should be configurable in prod env
  // See: https://github.com/facebook/rocksdb/wiki/Statistics
  options->statistics = rocksdb::CreateDBStatistics();
  options->stats_dump_period_sec = config_->RocksDB.stats_dump_period_sec;
  options->max_open_files = config_->RocksDB.max_open_files;
  options->compaction_style = rocksdb::CompactionStyle::kCompactionStyleLevel;
  options->max_subcompactions = static_cast<uint32_t>(config_->RocksDB.max_sub_compactions);
  options->max_background_flushes = config_->RocksDB.max_background_flushes;
  options->max_background_compactions = config_->RocksDB.max_background_compactions;
  options->max_write_buffer_number = config_->RocksDB.max_write_buffer_number;
  options->min_write_buffer_number_to_merge = 2;
  options->write_buffer_size =  config_->RocksDB.write_buffer_size * MiB;
  options->num_levels = 7;
  options->compression_per_level.resize(options->num_levels);
  // only compress levels >= 2
  for (int i = 0; i < options->num_levels; ++i) {
    if (i < 2) {
      options->compression_per_level[i] = rocksdb::CompressionType::kNoCompression;
    } else {
      options->compression_per_level[i] = static_cast<rocksdb::CompressionType>(config_->RocksDB.compression);
    }
  }
  if (config_->RocksDB.row_cache_size) {
    options->row_cache = rocksdb::NewLRUCache(config_->RocksDB.row_cache_size * MiB);
  }
  options->enable_pipelined_write = config_->RocksDB.enable_pipelined_write;
  options->target_file_size_base = config_->RocksDB.target_file_size_base * MiB;
  options->max_manifest_file_size = 64 * MiB;
  options->max_log_file_size = 256 * MiB;
  options->keep_log_file_num = 12;
  options->WAL_ttl_seconds = static_cast<uint64_t>(config_->RocksDB.WAL_ttl_seconds);
  options->WAL_size_limit_MB = static_cast<uint64_t>(config_->RocksDB.WAL_size_limit_MB);
  options->max_total_wal_size = static_cast<uint64_t>(config_->RocksDB.max_total_wal_size * MiB);
  options->listeners.emplace_back(new EventListener(this));
  options->dump_malloc_stats = true;
  sst_file_manager_ = std::shared_ptr<rocksdb::SstFileManager>(rocksdb::NewSstFileManager(rocksdb::Env::Default()));
  options->sst_file_manager = sst_file_manager_;
  uint64_t max_io_mb = kIORateLimitMaxMb;
  if (config_->max_io_mb > 0) max_io_mb = static_cast<uint64_t>(config_->max_io_mb);
  rate_limiter_ = std::shared_ptr<rocksdb::RateLimiter>(rocksdb::NewGenericRateLimiter(max_io_mb * MiB));
  options->rate_limiter = rate_limiter_;
  options->delayed_write_rate = static_cast<uint64_t>(config_->RocksDB.delayed_write_rate);
  options->compaction_readahead_size = static_cast<size_t>(config_->RocksDB.compaction_readahead_size);
  options->level0_slowdown_writes_trigger = config_->RocksDB.level0_slowdown_writes_trigger;
  options->level0_stop_writes_trigger = config_->RocksDB.level0_stop_writes_trigger;
  options->level0_file_num_compaction_trigger = config_->RocksDB.level0_file_num_compaction_trigger;
  options->max_bytes_for_level_base = config_->RocksDB.max_bytes_for_level_base * MiB;
  options->max_bytes_for_level_multiplier = config_->RocksDB.max_bytes_for_level_multiplier;
  options->level_compaction_dynamic_level_bytes = config_->RocksDB.level_compaction_dynamic_level_bytes;
}

Status Storage::SetColumnFamilyOption(const std::string &key, const std::string &value) {
  for (auto &cf_handle : cf_handles_) {
    auto s = db_->SetOptions(cf_handle, {{key, value}});
    if (!s.ok()) return Status(Status::NotOK, s.ToString());
  }
  return Status::OK();
}

Status Storage::SetOption(const std::string &key, const std::string &value) {
  auto s = db_->SetOptions({{key, value}});
  if (!s.ok()) return Status(Status::NotOK, s.ToString());
  return Status::OK();
}

Status Storage::SetDBOption(const std::string &key, const std::string &value) {
  auto s = db_->SetDBOptions({{key, value}});
  if (!s.ok()) return Status(Status::NotOK, s.ToString());
  return Status::OK();
}

Status Storage::CreateColumnFamilies(const rocksdb::Options &options) {
  rocksdb::DB *tmp_db;
  rocksdb::ColumnFamilyOptions cf_options(options);
  rocksdb::Status s = rocksdb::DB::Open(options, config_->db_dir, &tmp_db);
  if (s.ok()) {
    std::vector<std::string> cf_names = {kMetadataColumnFamilyName,
                                         kZSetScoreColumnFamilyName,
                                         kPubSubColumnFamilyName,
                                         kPropagateColumnFamilyName};
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    s = tmp_db->CreateColumnFamilies(cf_options, cf_names, &cf_handles);
    if (!s.ok()) {
      delete tmp_db;
      return Status(Status::DBOpenErr, s.ToString());
    }
    for (auto handle : cf_handles) tmp_db->DestroyColumnFamilyHandle(handle);
    tmp_db->Close();
    delete tmp_db;
  }
  // Open db would be failed if the column families have already exists,
  // so we return ok here.
  return Status::OK();
}

Status Storage::Open(bool read_only) {
  auto guard = WriteLockGuard();
  db_closing_ = false;

  bool cache_index_and_filter_blocks = config_->RocksDB.cache_index_and_filter_blocks;
  size_t metadata_block_cache_size = config_->RocksDB.metadata_block_cache_size*MiB;
  size_t subkey_block_cache_size = config_->RocksDB.subkey_block_cache_size*MiB;

  rocksdb::Options options;
  InitOptions(&options);
  CreateColumnFamilies(options);

  std::shared_ptr<rocksdb::Cache> shared_block_cache;
  if (config_->RocksDB.share_metadata_and_subkey_block_cache) {
    size_t shared_block_cache_size = metadata_block_cache_size + subkey_block_cache_size;
    shared_block_cache = rocksdb::NewLRUCache(shared_block_cache_size, -1, false, 0.75);
  }

  rocksdb::BlockBasedTableOptions metadata_table_opts;
  InitTableOptions(&metadata_table_opts);
  metadata_table_opts.block_cache = shared_block_cache ?
    shared_block_cache : rocksdb::NewLRUCache(metadata_block_cache_size, -1, false, 0.75);
  metadata_table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
  metadata_table_opts.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
  metadata_table_opts.cache_index_and_filter_blocks_with_high_priority = true;

  rocksdb::ColumnFamilyOptions metadata_opts(options);
  metadata_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(metadata_table_opts));
  metadata_opts.compaction_filter_factory = std::make_shared<MetadataFilterFactory>(this);
  metadata_opts.disable_auto_compactions = config_->RocksDB.disable_auto_compactions;
  // Enable whole key bloom filter in memtable
  metadata_opts.memtable_whole_key_filtering = true;
  metadata_opts.memtable_prefix_bloom_size_ratio = 0.1;
  metadata_opts.table_properties_collector_factories.emplace_back(
      NewCompactOnExpiredTableCollectorFactory(kMetadataColumnFamilyName, 0.3));
  SetBlobDB(&metadata_opts);

  rocksdb::BlockBasedTableOptions subkey_table_opts;
  InitTableOptions(&subkey_table_opts);
  subkey_table_opts.block_cache = shared_block_cache ?
    shared_block_cache : rocksdb::NewLRUCache(subkey_block_cache_size, -1, false, 0.75);
  subkey_table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
  subkey_table_opts.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
  subkey_table_opts.cache_index_and_filter_blocks_with_high_priority = true;
  rocksdb::ColumnFamilyOptions subkey_opts(options);
  subkey_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(subkey_table_opts));
  subkey_opts.compaction_filter_factory = std::make_shared<SubKeyFilterFactory>(this);
  subkey_opts.disable_auto_compactions = config_->RocksDB.disable_auto_compactions;
  subkey_opts.table_properties_collector_factories.emplace_back(
      NewCompactOnExpiredTableCollectorFactory(kSubkeyColumnFamilyName, 0.3));
  SetBlobDB(&subkey_opts);

  rocksdb::BlockBasedTableOptions pubsub_table_opts;
  InitTableOptions(&pubsub_table_opts);
  rocksdb::ColumnFamilyOptions pubsub_opts(options);
  pubsub_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(pubsub_table_opts));
  pubsub_opts.compaction_filter_factory = std::make_shared<PubSubFilterFactory>();
  pubsub_opts.disable_auto_compactions = config_->RocksDB.disable_auto_compactions;
  SetBlobDB(&pubsub_opts);

  rocksdb::BlockBasedTableOptions propagate_table_opts;
  InitTableOptions(&propagate_table_opts);
  rocksdb::ColumnFamilyOptions propagate_opts(options);
  propagate_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(propagate_table_opts));
  propagate_opts.compaction_filter_factory = std::make_shared<PropagateFilterFactory>();
  propagate_opts.disable_auto_compactions = config_->RocksDB.disable_auto_compactions;
  SetBlobDB(&propagate_opts);

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Caution: don't change the order of column family, or the handle will be mismatched
  column_families.emplace_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, subkey_opts));
  column_families.emplace_back(rocksdb::ColumnFamilyDescriptor(kMetadataColumnFamilyName, metadata_opts));
  column_families.emplace_back(rocksdb::ColumnFamilyDescriptor(kZSetScoreColumnFamilyName, subkey_opts));
  column_families.emplace_back(rocksdb::ColumnFamilyDescriptor(kPubSubColumnFamilyName, pubsub_opts));
  column_families.emplace_back(rocksdb::ColumnFamilyDescriptor(kPropagateColumnFamilyName, propagate_opts));
  std::vector<std::string> old_column_families;
  auto s = rocksdb::DB::ListColumnFamilies(options, config_->db_dir, &old_column_families);
  if (!s.ok()) return Status(Status::NotOK, s.ToString());
  auto start = std::chrono::high_resolution_clock::now();
  if (read_only) {
    s = rocksdb::DB::OpenForReadOnly(options, config_->db_dir, column_families, &cf_handles_, &db_);
  } else {
    s = rocksdb::DB::Open(options, config_->db_dir, column_families, &cf_handles_, &db_);
  }
  auto end = std::chrono::high_resolution_clock::now();
  int64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (!s.ok()) {
    LOG(INFO) << "[storage] Failed to load the data from disk: " << duration << " ms";
    return Status(Status::DBOpenErr, s.ToString());
  }
  LOG(INFO) << "[storage] Success to load the data from disk: " << duration << " ms";
  return Status::OK();
}

Status Storage::Open() {
  return Open(false);
}

Status Storage::OpenForReadOnly() {
  return Open(true);
}

Status Storage::CreateBackup() {
  LOG(INFO) << "[storage] Start to create new backup";
  std::lock_guard<std::mutex> lg(backup_mu_);

  std::string tmpdir = config_->backup_dir + ".tmp";
  // Maybe there is a dirty tmp checkpoint, try to clean it
  rocksdb::DestroyDB(tmpdir, rocksdb::Options());

  // 1) Create checkpoint of rocksdb for backup
  rocksdb::Checkpoint* checkpoint = NULL;
  rocksdb::Status s = rocksdb::Checkpoint::Create(db_, &checkpoint);
  if (!s.ok()) {
    LOG(WARNING) << "Fail to create checkpoint for backup, error:" << s.ToString();
    return Status(Status::NotOK, s.ToString());
  }
  std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
  s = checkpoint->CreateCheckpoint(tmpdir, config_->RocksDB.write_buffer_size*MiB);
  if (!s.ok()) {
    LOG(WARNING) << "Fail to create checkpoint for backup, error:" << s.ToString();
    return Status(Status::DBBackupErr, s.ToString());
  }

  // 2) Rename tmp backup to real backup dir
  if (!(s = rocksdb::DestroyDB(config_->backup_dir, rocksdb::Options())).ok()) {
    LOG(WARNING) << "[storage] Fail to clean old backup, error:" << s.ToString();
    return Status(Status::NotOK, s.ToString());
  }
  if (!(s = env_->RenameFile(tmpdir, config_->backup_dir)).ok()) {
    LOG(WARNING) << "[storage] Fail to rename tmp backup, error:" << s.ToString();
    // Just try best effort
    if (!(s = rocksdb::DestroyDB(tmpdir, rocksdb::Options())).ok()) {
      LOG(WARNING) << "[storage] Fail to clean tmp backup, error:" << s.ToString();
    }
    return Status(Status::NotOK, s.ToString());
  }
  // 'backup_mu_' can guarantee 'backup_creating_time_' is thread-safe
  backup_creating_time_ = std::time(nullptr);

  LOG(INFO) << "[storage] Success to create new backup";
  return Status::OK();
}

Status Storage::DestroyBackup() {
  backup_->StopBackup();
  delete backup_;
  return Status();
}

Status Storage::RestoreFromBackup() {
  // TODO(@ruoshan): assert role to be slave
  // We must reopen the backup engine every time, as the files is changed
  rocksdb::BackupableDBOptions bk_option(config_->backup_sync_dir);
  auto s = rocksdb::BackupEngine::Open(db_->GetEnv(), bk_option, &backup_);
  if (!s.ok()) return Status(Status::DBBackupErr, s.ToString());

  s = backup_->RestoreDBFromLatestBackup(config_->db_dir, config_->db_dir);
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Failed to restore: " << s.ToString();
  } else {
    LOG(INFO) << "[storage] Restore from backup";
  }
  // Reopen DB （should always try to reopen db even if restore failed , replication sst file crc check may use it）
  auto s2 = Open();
  if (!s2.IsOK()) {
    LOG(ERROR) << "[storage] Failed to reopen db: " << s2.Msg();
    return Status(Status::DBOpenErr, s2.Msg());
  }

  // Clean up backup engine
  backup_->PurgeOldBackups(0);
  DestroyBackup();
  backup_ = nullptr;

  return s.ok() ? Status::OK() : Status(Status::DBBackupErr, s.ToString());
}

Status Storage::RestoreFromCheckpoint() {
  std::string dir = config_->sync_checkpoint_dir;
  std::string tmp_dir = config_->db_dir + ".tmp";

  // Clean old backups and checkpoints because server will work on the new db
  PurgeOldBackups(0, 0);
  rocksdb::DestroyDB(config_->checkpoint_dir, rocksdb::Options());

  // Maybe there is no db dir
  auto s = env_->CreateDirIfMissing(config_->db_dir);
  if (!s.ok()) {
    return Status(Status::NotOK, "Fail to create db dir, error: " + s.ToString());
  }

  // Rename db dir to tmp, so we can restore if replica fails to load
  // the checkpoint from master.
  // But only try best effort to make data safe
  s = env_->RenameFile(config_->db_dir, tmp_dir);
  if (!s.ok()) {
    if (!Open().IsOK()) LOG(ERROR) << "[storage] Fail to reopen db";
    return Status(Status::NotOK, "Fail to rename db dir, error: " + s.ToString());
  }

  // Rename checkpoint dir to db dir
  if (!(s = env_->RenameFile(dir, config_->db_dir)).ok()) {
    env_->RenameFile(tmp_dir, config_->db_dir);
    if (!Open().IsOK()) LOG(ERROR) << "[storage] Fail to reopen db";
    return Status(Status::NotOK, "Fail to rename checkpoint dir, error: " + s.ToString());
  }

  // Open the new db, restore if replica fails to open db
  auto s2 = Open();
  if (!s2.IsOK()) {
    LOG(WARNING) << "[storage] Fail to open master checkpoint, error: " << s2.Msg();
    rocksdb::DestroyDB(config_->db_dir, rocksdb::Options());
    env_->RenameFile(tmp_dir, config_->db_dir);
    if (!Open().IsOK()) LOG(ERROR) << "[storage] Fail to reopen db";
    return Status(Status::DBOpenErr,
              "Fail to open master checkpoint, error: " + s2.Msg());
  }

  // Destory origin db
  if (!(s = rocksdb::DestroyDB(tmp_dir, rocksdb::Options())).ok()) {
    LOG(WARNING) << "[storage] Fail to destroy " << tmp_dir << ", error:" << s.ToString();
  }
  return Status::OK();
}

void Storage::EmptyDB() {
  // Clean old backups and checkpoints
  PurgeOldBackups(0, 0);
  rocksdb::DestroyDB(config_->checkpoint_dir, rocksdb::Options());

  auto s = rocksdb::DestroyDB(config_->db_dir, rocksdb::Options());
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Failed to destroy db, error: " << s.ToString();
  }
}

void Storage::PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours) {
  time_t now = time(nullptr);
  std::lock_guard<std::mutex> lg(backup_mu_);

  // Return if there is no backup
  auto s = env_->FileExists(config_->backup_dir);
  if (!s.ok()) return;

  // No backup is needed to keep or the backup is expired, we will clean it.
  if (num_backups_to_keep == 0 || (backup_max_keep_hours != 0 &&
        backup_creating_time_ + backup_max_keep_hours*3600 < now)) {
    s = rocksdb::DestroyDB(config_->backup_dir, rocksdb::Options());
    if (s.ok()) {
      LOG(INFO) << "[storage] Succeeded cleaning old backup that was born at "
                << backup_creating_time_;
    } else {
      LOG(INFO) << "[storage] Failed cleaning old backup that was born at "
                << backup_creating_time_;
    }
  }
}

Status Storage::GetWALIter(
    rocksdb::SequenceNumber seq,
    std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
  auto s = db_->GetUpdatesSince(seq, iter);
  if (!s.ok()) return Status(Status::DBGetWALErr, s.ToString());
  if (!(*iter)->Valid()) return Status(Status::DBGetWALErr, "iterator not valid");
  return Status::OK();
}

rocksdb::SequenceNumber Storage::LatestSeq() {
  return db_->GetLatestSequenceNumber();
}

rocksdb::Status Storage::Write(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates) {
  if (reach_db_size_limit_) {
    return rocksdb::Status::SpaceLimit();
  }

  auto s = db_->Write(options, updates);
  if (!s.ok()) return s;

  return s;
}

rocksdb::Status Storage::Delete(const rocksdb::WriteOptions &options,
                                rocksdb::ColumnFamilyHandle *cf_handle,
                                const rocksdb::Slice &key) {
  rocksdb::WriteBatch batch;
  batch.Delete(cf_handle, key);
  return db_->Write(options, &batch);
}

rocksdb::Status Storage::DeleteRange(const std::string &first_key, const std::string &last_key) {
  auto s = db_->DeleteRange(rocksdb::WriteOptions(), GetCFHandle("metadata"), first_key, last_key);
  if (!s.ok()) {
    return s;
  }
  s = Delete(rocksdb::WriteOptions(), GetCFHandle("metadata"), last_key);
  if (!s.ok()) {
    return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Storage::FlushScripts(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle) {
  std::string begin_key = kLuaFunctionPrefix, end_key = begin_key;
  // we need to increase one here since the DeleteRange api
  // didn't contain the end key.
  end_key[end_key.size()-1] += 1;
  return db_->DeleteRange(options, cf_handle, begin_key, end_key);
}

Status Storage::WriteBatch(std::string &&raw_batch) {
  if (reach_db_size_limit_) {
    return Status(Status::NotOK, "reach space limit");
  }
  auto bat = rocksdb::WriteBatch(std::move(raw_batch));
  auto s = db_->Write(rocksdb::WriteOptions(), &bat);
  if (!s.ok()) {
    return Status(Status::NotOK, s.ToString());
  }
  return Status::OK();
}

rocksdb::ColumnFamilyHandle *Storage::GetCFHandle(const std::string &name) {
  if (name == kMetadataColumnFamilyName) {
    return cf_handles_[1];
  } else if (name == kZSetScoreColumnFamilyName) {
    return cf_handles_[2];
  } else if (name == kPubSubColumnFamilyName) {
    return cf_handles_[3];
  } else if (name == kPropagateColumnFamilyName) {
    return cf_handles_[4];
  }
  return cf_handles_[0];
}

rocksdb::Status Storage::Compact(const Slice *begin, const Slice *end) {
  rocksdb::CompactRangeOptions compact_opts;
  compact_opts.change_level = true;
  for (const auto &cf_handle : cf_handles_) {
    rocksdb::Status s = db_->CompactRange(compact_opts, cf_handle, begin, end);
    if (!s.ok()) return s;
  }
  return rocksdb::Status::OK();
}

uint64_t Storage::GetTotalSize(const std::string &ns) {
  if (ns == kDefaultNamespace) {
    return sst_file_manager_->GetTotalSize();
  }
  std::string prefix, begin_key, end_key;
  ComposeNamespaceKey(ns, "", &prefix, false);

  Redis::Database db(this, ns);
  uint64_t size, total_size = 0;
  uint8_t include_both = rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES |
      rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES;
  for (auto cf_handle : cf_handles_) {
    if (cf_handle == GetCFHandle(kPubSubColumnFamilyName) ||
        cf_handle == GetCFHandle(kPropagateColumnFamilyName)) {
      continue;
    }
    auto s = db.FindKeyRangeWithPrefix(prefix,  std::string(), &begin_key, &end_key, cf_handle);
    if (!s.ok()) continue;

    rocksdb::Range r(begin_key, end_key);
    db_->GetApproximateSizes(cf_handle, &r, 1, &size, include_both);
    total_size += size;
  }
  return total_size;
}

Status Storage::CheckDBSizeLimit() {
  bool reach_db_size_limit;
  if (config_->max_db_size == 0) {
    reach_db_size_limit = false;
  } else {
    reach_db_size_limit = GetTotalSize() >= config_->max_db_size * GiB;
  }
  if (reach_db_size_limit_ == reach_db_size_limit) {
    return Status::OK();
  }
  reach_db_size_limit_ = reach_db_size_limit;
  if (reach_db_size_limit_) {
    LOG(WARNING) << "[storage] ENABLE db_size limit " << config_->max_db_size << " GB"
                 << "set kvrocks to read-only mode";
  } else {
    LOG(WARNING) << "[storage] DISABLE db_size limit, set kvrocks to read-write mode ";
  }
  return Status::OK();
}

void Storage::SetIORateLimit(uint64_t max_io_mb) {
  if (max_io_mb == 0) {
    max_io_mb = kIORateLimitMaxMb;
  }
  rate_limiter_->SetBytesPerSecond(max_io_mb * MiB);
}

rocksdb::DB *Storage::GetDB() { return db_; }

std::unique_ptr<RWLock::ReadLock> Storage::ReadLockGuard() {
  return std::unique_ptr<RWLock::ReadLock>(new RWLock::ReadLock(db_rw_lock_));
}

std::unique_ptr<RWLock::WriteLock> Storage::WriteLockGuard() {
  return std::unique_ptr<RWLock::WriteLock>(new RWLock::WriteLock(db_rw_lock_));
}

Status Storage::ReplDataManager::GetFullReplDataInfo(Storage *storage, std::string *files) {
  auto guard = storage->ReadLockGuard();
  if (storage->IsClosing()) return Status(Status::NotOK, "DB is closing");

  std::string data_files_dir = storage->config_->checkpoint_dir;
  std::unique_lock<std::mutex> ulm(storage->checkpoint_mu_);

  // Create checkpoint if not exist
  if (!storage->env_->FileExists(data_files_dir).ok()) {
    rocksdb::Checkpoint* checkpoint = NULL;
    rocksdb::Status s = rocksdb::Checkpoint::Create(storage->db_, &checkpoint);
    if (!s.ok()) {
      LOG(WARNING) << "Fail to create checkpoint, error:" << s.ToString();
      return Status(Status::NotOK, s.ToString());
    }
    std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);

    // Create checkpoint of rocksdb
    s = checkpoint->CreateCheckpoint(data_files_dir,
          storage->config_->RocksDB.write_buffer_size*MiB);
    storage->SetCheckpointCreateTime(std::time(nullptr));
    storage->SetCheckpointAccessTime(std::time(nullptr));
    if (!s.ok()) {
      LOG(WARNING) << "[storage] Fail to create checkpoint, error:" << s.ToString();
      return Status(Status::NotOK, s.ToString());
    }
    LOG(INFO) << "[storage] Create checkpoint successfully";
  } else {
    // Replicas can share checkpiont to replication if the checkpoint existing
    // time is less half of WAL ttl.
    int64_t can_shared_time = storage->config_->RocksDB.WAL_ttl_seconds / 2;
    if (can_shared_time > 60 * 60) can_shared_time = 60 * 60;
    if (can_shared_time < 10 * 60) can_shared_time = 10 * 60;
    if (std::time(nullptr) - storage->GetCheckpointCreateTime() > can_shared_time) {
      LOG(WARNING) << "[storage] Can't use current checkpoint, waiting next checkpoint";
      return Status(Status::NotOK, "Can't use current checkpoint, waiting for next checkpoint");
    }
    LOG(INFO) << "[storage] Use current existing checkpoint";
  }
  ulm.unlock();

  // Get checkpoint file list
  std::vector<std::string> result;
  storage->env_->GetChildren(data_files_dir, &result);
  for (auto f : result) {
    if (f == "." || f == "..") continue;
    files->append(f);
    files->push_back(',');
  }
  files->pop_back();
  return Status::OK();
}

bool Storage::ExistCheckpoint(void) {
  std::lock_guard<std::mutex> lg(checkpoint_mu_);
  return env_->FileExists(config_->checkpoint_dir).ok();
}

bool Storage::ExistSyncCheckpoint(void) {
  return env_->FileExists(config_->sync_checkpoint_dir).ok();
}

Status Storage::ReplDataManager::CleanInvalidFiles(Storage *storage,
    const std::string &dir, std::vector<std::string> valid_files) {
  if (!storage->env_->FileExists(dir).ok()) {
    return Status::OK();
  }

  std::vector<std::string> tmp_files, files;
  storage->env_->GetChildren(dir, &tmp_files);
  for (auto file : tmp_files) {
    if (file == "." || file == "..") continue;
    files.push_back(file);
  }

  // Find invalid files
  std::sort(files.begin(), files.end());
  std::sort(valid_files.begin(), valid_files.end());
  std::vector<std::string> invalid_files(files.size() + valid_files.size());
  auto it = std::set_difference(files.begin(), files.end(),
                valid_files.begin(), valid_files.end(), invalid_files.begin());

  // Delete invalid files
  Status ret;
  invalid_files.resize(it - invalid_files.begin());
  for (it = invalid_files.begin(); it != invalid_files.end(); ++it) {
    auto s = storage->env_->DeleteFile(dir + "/" + *it);
    if (!s.ok()) {
      ret = Status(Status::NotOK, s.ToString());
      LOG(INFO) << "[storage] Fail to delete invalid file "
                << *it << " of master checkpoint";
    } else {
      LOG(INFO) << "[storage] Succeed deleting invalid file "
                << *it << " of master checkpoint";
    }
  }
  return ret;
}

int Storage::ReplDataManager::OpenDataFile(Storage *storage,
            const std::string &repl_file, uint64_t *file_size) {
  std::string abs_path = storage->config_->checkpoint_dir + "/" + repl_file;
  auto s = storage->env_->FileExists(abs_path);
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Data file [" << abs_path << "] not found";
    return -1;
  }
  storage->env_->GetFileSize(abs_path, file_size);
  auto rv = open(abs_path.c_str(), O_RDONLY);
  if (rv < 0) {
    LOG(ERROR) << "[storage] Failed to open file: " << strerror(errno);
  }
  return rv;
}

Storage::ReplDataManager::MetaInfo Storage::ReplDataManager::ParseMetaAndSave(
    Storage *storage, rocksdb::BackupID meta_id, evbuffer *evbuf) {
  char *line;
  size_t len;
  Storage::ReplDataManager::MetaInfo meta;
  auto meta_file = "meta/" + std::to_string(meta_id);
  DLOG(INFO) << "[meta] id: " << meta_id;

  // Save the meta to tmp file
  auto wf = NewTmpFile(storage, storage->config_->backup_sync_dir, meta_file);
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
    while (*(cptr++) != ' ') {}
    auto filename = std::string(line, cptr - line - 1);
    while (*(cptr++) != ' ') {}
    auto crc32 = std::strtoul(cptr, nullptr, 10);
    meta.files.emplace_back(filename, crc32);
    free(line);
  }
  SwapTmpFile(storage, storage->config_->backup_sync_dir, meta_file);
  return meta;
}

Status MkdirRecursively(rocksdb::Env *env, const std::string &dir) {
  if (env->CreateDirIfMissing(dir).ok()) return Status::OK();

  std::string parent;
  for (auto pos = dir.find('/', 1); pos != std::string::npos;
       pos = dir.find('/', pos + 1)) {
    parent = dir.substr(0, pos);
    if (!env->CreateDirIfMissing(parent).ok()) {
      LOG(ERROR) << "[storage] Failed to create directory recursively";
      return Status(Status::NotOK);
    }
  }
  if (env->CreateDirIfMissing(dir).ok()) return Status::OK();
  return Status(Status::NotOK);
}

std::unique_ptr<rocksdb::WritableFile> Storage::ReplDataManager::NewTmpFile(
          Storage *storage, const std::string &dir, const std::string &repl_file) {
  std::string tmp_file = dir + "/" + repl_file + ".tmp";
  auto s = storage->env_->FileExists(tmp_file);
  if (s.ok()) {
    LOG(ERROR) << "[storage] Data file exists, override";
    storage->env_->DeleteFile(tmp_file);
  }
  // Create directory if missing
  auto abs_dir = tmp_file.substr(0, tmp_file.rfind('/'));
  if (!MkdirRecursively(storage->env_, abs_dir).IsOK()) {
    return nullptr;
  }
  std::unique_ptr<rocksdb::WritableFile> wf;
  s = storage->env_->NewWritableFile(tmp_file, &wf, rocksdb::EnvOptions());
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Failed to create data file: " << s.ToString();
    return nullptr;
  }
  return wf;
}

Status Storage::ReplDataManager::SwapTmpFile(Storage *storage,
                  const std::string &dir, const std::string &repl_file) {
  std::string tmp_file = dir + "/" + repl_file + ".tmp";
  std::string orig_file = dir + "/" + repl_file;
  if (!storage->env_->RenameFile(tmp_file, orig_file).ok()) {
    return Status(Status::NotOK, "unable to rename: "+tmp_file);
  }
  return Status::OK();
}

bool Storage::ReplDataManager::FileExists(Storage *storage, const std::string &dir,
        const std::string &repl_file, uint32_t crc) {
  if (storage->IsClosing()) return false;

  auto file_path = dir + "/" + repl_file;
  auto s = storage->env_->FileExists(file_path);
  if (!s.ok()) return false;

  // If crc is 0, we needn't verify, return true directly.
  if (crc == 0) return true;

  std::unique_ptr<rocksdb::SequentialFile> src_file;
  const rocksdb::EnvOptions soptions;
  s = storage->env_->NewSequentialFile(file_path, &src_file, soptions);
  if (!s.ok()) return false;

  uint64_t size;
  s = storage->env_->GetFileSize(file_path, &size);
  if (!s.ok()) return false;
  std::unique_ptr<rocksdb::SequentialFileWrapper> src_reader;
  src_reader.reset(new rocksdb::SequentialFileWrapper(src_file.get()));

  char buffer[4096];
  Slice slice;
  uint32_t tmp_crc = 0;
  while (size > 0) {
    size_t bytes_to_read = std::min(sizeof(buffer), static_cast<size_t>(size));
    s = src_reader->Read(bytes_to_read, &slice, buffer);
    if (!s.ok()) return false;
    if (slice.size() == 0) return false;
    tmp_crc = rocksdb::crc32c::Extend(0, slice.ToString().c_str(), slice.size());
    size -= slice.size();
  }
  return crc == tmp_crc;
}

}  // namespace Engine
