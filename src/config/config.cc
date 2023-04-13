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

#include "config.h"

#include <fmt/format.h>
#include <rocksdb/env.h>
#include <strings.h>

#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "config_type.h"
#include "config_util.h"
#include "parse_util.h"
#include "server/server.h"
#include "status.h"

constexpr const char *kDefaultBindAddress = "127.0.0.1";

constexpr const char *errBlobDbNotEnabled = "Must set rocksdb.enable_blob_files to yes first.";
constexpr const char *errLevelCompactionDynamicLevelBytesNotSet =
    "Must set rocksdb.level_compaction_dynamic_level_bytes yes first.";

ConfigEnum compression_types[] = {
    {"no", rocksdb::CompressionType::kNoCompression},     {"snappy", rocksdb::CompressionType::kSnappyCompression},
    {"lz4", rocksdb::CompressionType::kLZ4Compression},   {"zstd", rocksdb::CompressionType::kZSTD},
    {"zlib", rocksdb::CompressionType::kZlibCompression}, {nullptr, 0}};

ConfigEnum supervised_modes[] = {{"no", kSupervisedNone},
                                 {"auto", kSupervisedAutoDetect},
                                 {"upstart", kSupervisedUpStart},
                                 {"systemd", kSupervisedSystemd},
                                 {nullptr, 0}};

ConfigEnum log_levels[] = {{"info", google::INFO},
                           {"warning", google::WARNING},
                           {"error", google::ERROR},
                           {"fatal", google::FATAL},
                           {nullptr, 0}};

std::string trimRocksDBPrefix(std::string s) {
  if (strncasecmp(s.data(), "rocksdb.", 8) != 0) return s;
  return s.substr(8, s.size() - 8);
}

int configEnumGetValue(ConfigEnum *ce, const char *name) {
  while (ce->name_ != nullptr) {
    if (strcasecmp(ce->name_, name) == 0) return ce->val_;
    ce++;
  }
  return INT_MIN;
}

const char *configEnumGetName(ConfigEnum *ce, int val) {
  while (ce->name_ != nullptr) {
    if (ce->val_ == val) return ce->name_;
    ce++;
  }
  return nullptr;
}

Config::Config() {
  struct FieldWrapper {
    std::string name_;
    bool readonly_;
    std::unique_ptr<ConfigField> field_;

    FieldWrapper(std::string name, bool readonly, ConfigField *field)
        : name_(std::move(name)), readonly_(readonly), field_(field) {}
  };
  FieldWrapper fields[] = {
      {"daemonize", true, new YesNoField(&daemonize_, false)},
      {"bind", true, new StringField(&binds_str_, "")},
      {"port", true, new UInt32Field(&port_, kDefaultPort, 1, PORT_LIMIT)},
#ifdef ENABLE_OPENSSL
      {"tls-port", true, new UInt32Field(&tls_port_, 0, 0, PORT_LIMIT)},
      {"tls-cert-file", false, new StringField(&tls_cert_file_, "")},
      {"tls-key-file", false, new StringField(&tls_key_file_, "")},
      {"tls-key-file-pass", false, new StringField(&tls_key_file_pass_, "")},
      {"tls-ca-cert-file", false, new StringField(&tls_ca_cert_file_, "")},
      {"tls-ca-cert-dir", false, new StringField(&tls_ca_cert_dir_, "")},
      {"tls-protocols", false, new StringField(&tls_protocols_, "")},
      {"tls-auth-clients", false, new StringField(&tls_auth_clients_, "")},
      {"tls-ciphers", false, new StringField(&tls_ciphers_, "")},
      {"tls-ciphersuites", false, new StringField(&tls_ciphersuites_, "")},
      {"tls-prefer-server-ciphers", false, new YesNoField(&tls_prefer_server_ciphers_, false)},
      {"tls-session-caching", false, new YesNoField(&tls_session_caching_, true)},
      {"tls-session-cache-size", false, new IntField(&tls_session_cache_size_, 1024 * 20, 0, INT_MAX)},
      {"tls-session-cache-timeout", false, new IntField(&tls_session_cache_timeout_, 300, 0, INT_MAX)},
#endif
      {"workers", true, new IntField(&workers_, 8, 1, 256)},
      {"timeout", false, new IntField(&timeout_, 0, 0, INT_MAX)},
      {"tcp-backlog", true, new IntField(&backlog_, 511, 0, INT_MAX)},
      {"maxclients", false, new IntField(&maxclients_, 10240, 0, INT_MAX)},
      {"max-backup-to-keep", false, new IntField(&max_backup_to_keep_, 1, 0, 1)},
      {"max-backup-keep-hours", false, new IntField(&max_backup_keep_hours_, 0, 0, INT_MAX)},
      {"master-use-repl-port", false, new YesNoField(&master_use_repl_port_, false)},
      {"requirepass", false, new StringField(&requirepass_, "")},
      {"masterauth", false, new StringField(&masterauth_, "")},
      {"slaveof", true, new StringField(&slaveof_, "")},
      {"compact-cron", false, new StringField(&compact_cron_str_, "")},
      {"bgsave-cron", false, new StringField(&bgsave_cron_str_, "")},
      {"replica-announce-ip", false, new StringField(&replica_announce_ip_, "")},
      {"replica-announce-port", false, new UInt32Field(&replica_announce_port_, 0, 0, PORT_LIMIT)},
      {"compaction-checker-range", false, new StringField(&compaction_checker_range_str_, "")},
      {"force-compact-file-age", false, new Int64Field(&force_compact_file_age_, 2 * 24 * 3600, 60, INT64_MAX)},
      {"force-compact-file-min-deleted-percentage", false,
       new IntField(&force_compact_file_min_deleted_percentage_, 10, 1, 100)},
      {"db-name", true, new StringField(&db_name_, "change.me.db")},
      {"dir", true, new StringField(&dir_, "/tmp/kvrocks")},
      {"backup-dir", false, new StringField(&backup_dir_, "")},
      {"log-dir", true, new StringField(&log_dir_, "")},
      {"log-level", true, new EnumField(&log_level_, log_levels, google::INFO)},
      {"pidfile", true, new StringField(&pidfile_, "")},
      {"max-io-mb", false, new IntField(&max_io_mb_, 500, 0, INT_MAX)},
      {"max-bitmap-to-string-mb", false, new IntField(&max_bitmap_to_string_mb_, 16, 0, INT_MAX)},
      {"max-db-size", false, new IntField(&max_db_size_, 0, 0, INT_MAX)},
      {"max-replication-mb", false, new IntField(&max_replication_mb_, 0, 0, INT_MAX)},
      {"supervised", true, new EnumField(&supervised_mode_, supervised_modes, kSupervisedNone)},
      {"slave-serve-stale-data", false, new YesNoField(&slave_serve_stale_data_, true)},
      {"slave-empty-db-before-fullsync", false, new YesNoField(&slave_empty_db_before_fullsync_, false)},
      {"slave-priority", false, new IntField(&slave_priority_, 100, 0, INT_MAX)},
      {"slave-read-only", false, new YesNoField(&slave_readonly_, true)},
      {"use-rsid-psync", true, new YesNoField(&use_rsid_psync_, false)},
      {"profiling-sample-ratio", false, new IntField(&profiling_sample_ratio_, 0, 0, 100)},
      {"profiling-sample-record-max-len", false, new IntField(&profiling_sample_record_max_len_, 256, 0, INT_MAX)},
      {"profiling-sample-record-threshold-ms", false,
       new IntField(&profiling_sample_record_threshold_ms_, 100, 0, INT_MAX)},
      {"slowlog-log-slower-than", false, new IntField(&slowlog_log_slower_than_, 200000, -1, INT_MAX)},
      {"profiling-sample-commands", false, new StringField(&profiling_sample_commands_str_, "")},
      {"slowlog-max-len", false, new IntField(&slowlog_max_len_, 128, 0, INT_MAX)},
      {"purge-backup-on-fullsync", false, new YesNoField(&purge_backup_on_fullsync_, false)},
      {"rename-command", true, new MultiStringField(&rename_command_, std::vector<std::string>{})},
      {"auto-resize-block-and-sst", false, new YesNoField(&auto_resize_block_and_sst_, true)},
      {"fullsync-recv-file-delay", false, new IntField(&fullsync_recv_file_delay_, 0, 0, INT_MAX)},
      {"cluster-enabled", true, new YesNoField(&cluster_enabled_, false)},
      {"migrate-speed", false, new IntField(&migrate_speed_, 4096, 0, INT_MAX)},
      {"migrate-pipeline-size", false, new IntField(&pipeline_size_, 16, 1, INT_MAX)},
      {"migrate-sequence-gap", false, new IntField(&sequence_gap_, 10000, 1, INT_MAX)},
      {"unixsocket", true, new StringField(&unixsocket_, "")},
      {"unixsocketperm", true, new OctalField(&unixsocketperm_, 0777, 1, INT_MAX)},
      {"log-retention-days", false, new IntField(&log_retention_days_, -1, -1, INT_MAX)},
      {"persist-cluster-nodes-enabled", false, new YesNoField(&persist_cluster_nodes_enabled_, true)},

      /* rocksdb options */
      {"rocksdb.compression", false,
       new EnumField(&rocks_db_.compression_, compression_types, rocksdb::CompressionType::kNoCompression)},
      {"rocksdb.block_size", true, new IntField(&rocks_db_.block_size_, 4096, 0, INT_MAX)},
      {"rocksdb.max_open_files", false, new IntField(&rocks_db_.max_open_files_, 4096, -1, INT_MAX)},
      {"rocksdb.write_buffer_size", false, new IntField(&rocks_db_.write_buffer_size_, 64, 0, 4096)},
      {"rocksdb.max_write_buffer_number", false, new IntField(&rocks_db_.max_write_buffer_number_, 4, 0, 256)},
      {"rocksdb.target_file_size_base", false, new IntField(&rocks_db_.target_file_size_base_, 128, 1, 1024)},
      {"rocksdb.max_background_compactions", false, new IntField(&rocks_db_.max_background_compactions_, 2, -1, 32)},
      {"rocksdb.max_background_flushes", true, new IntField(&rocks_db_.max_background_flushes_, 2, -1, 32)},
      {"rocksdb.max_sub_compactions", false, new IntField(&rocks_db_.max_sub_compactions_, 1, 0, 16)},
      {"rocksdb.delayed_write_rate", false, new Int64Field(&rocks_db_.delayed_write_rate_, 0, 0, INT64_MAX)},
      {"rocksdb.wal_ttl_seconds", true, new IntField(&rocks_db_.wal_ttl_seconds_, 3 * 3600, 0, INT_MAX)},
      {"rocksdb.wal_size_limit_mb", true, new IntField(&rocks_db_.wal_size_limit_mb_, 16384, 0, INT_MAX)},
      {"rocksdb.max_total_wal_size", false, new IntField(&rocks_db_.max_total_wal_size_, 64 * 4 * 2, 0, INT_MAX)},
      {"rocksdb.disable_auto_compactions", false, new YesNoField(&rocks_db_.disable_auto_compactions_, false)},
      {"rocksdb.enable_pipelined_write", true, new YesNoField(&rocks_db_.enable_pipelined_write_, false)},
      {"rocksdb.stats_dump_period_sec", false, new IntField(&rocks_db_.stats_dump_period_sec_, 0, 0, INT_MAX)},
      {"rocksdb.cache_index_and_filter_blocks", true, new YesNoField(&rocks_db_.cache_index_and_filter_blocks_, false)},
      {"rocksdb.subkey_block_cache_size", true, new IntField(&rocks_db_.subkey_block_cache_size_, 2048, 0, INT_MAX)},
      {"rocksdb.metadata_block_cache_size", true,
       new IntField(&rocks_db_.metadata_block_cache_size_, 2048, 0, INT_MAX)},
      {"rocksdb.share_metadata_and_subkey_block_cache", true,
       new YesNoField(&rocks_db_.share_metadata_and_subkey_block_cache_, true)},
      {"rocksdb.row_cache_size", true, new IntField(&rocks_db_.row_cache_size_, 0, 0, INT_MAX)},
      {"rocksdb.compaction_readahead_size", false,
       new IntField(&rocks_db_.compaction_readahead_size_, 2 * MiB, 0, 64 * MiB)},
      {"rocksdb.level0_slowdown_writes_trigger", false,
       new IntField(&rocks_db_.level0_slowdown_writes_trigger_, 20, 1, 1024)},
      {"rocksdb.level0_stop_writes_trigger", false, new IntField(&rocks_db_.level0_stop_writes_trigger_, 40, 1, 1024)},
      {"rocksdb.level0_file_num_compaction_trigger", false,
       new IntField(&rocks_db_.level0_file_num_compaction_trigger_, 4, 1, 1024)},
      {"rocksdb.enable_blob_files", false, new YesNoField(&rocks_db_.enable_blob_files_, false)},
      {"rocksdb.min_blob_size", false, new IntField(&rocks_db_.min_blob_size_, 4096, 0, INT_MAX)},
      {"rocksdb.blob_file_size", false, new IntField(&rocks_db_.blob_file_size_, 268435456, 0, INT_MAX)},
      {"rocksdb.enable_blob_garbage_collection", false,
       new YesNoField(&rocks_db_.enable_blob_garbage_collection_, true)},
      {"rocksdb.blob_garbage_collection_age_cutoff", false,
       new IntField(&rocks_db_.blob_garbage_collection_age_cutoff_, 25, 0, 100)},
      {"rocksdb.max_bytes_for_level_base", false,
       new IntField(&rocks_db_.max_bytes_for_level_base_, 268435456, 0, INT_MAX)},
      {"rocksdb.max_bytes_for_level_multiplier", false,
       new IntField(&rocks_db_.max_bytes_for_level_multiplier_, 10, 1, 100)},
      {"rocksdb.level_compaction_dynamic_level_bytes", false,
       new YesNoField(&rocks_db_.level_compaction_dynamic_level_bytes_, false)},
      {"rocksdb.max_background_jobs", false, new IntField(&rocks_db_.max_background_jobs_, 4, 0, 32)},

      /* rocksdb write options */
      {"rocksdb.write_options.sync", true, new YesNoField(&rocks_db_.write_options_.sync_, false)},
      {"rocksdb.write_options.disable_wal", true, new YesNoField(&rocks_db_.write_options_.disable_wal_, false)},
      {"rocksdb.write_options.no_slowdown", true, new YesNoField(&rocks_db_.write_options_.no_slowdown_, false)},
      {"rocksdb.write_options.low_pri", true, new YesNoField(&rocks_db_.write_options_.low_pri_, false)},
      {"rocksdb.write_options.memtable_insert_hint_per_batch", true,
       new YesNoField(&rocks_db_.write_options_.memtable_insert_hint_per_batch_, false)},

      /* rocksdb read options */
      {"rocksdb.read_options.async_io", false, new YesNoField(&rocks_db_.read_options_.async_io_, false)},
  };
  for (auto &wrapper : fields) {
    auto &field = wrapper.field_;
    field->readonly_ = wrapper.readonly_;
    fields_.emplace(std::move(wrapper.name_), std::move(field));
  }
  initFieldValidator();
  initFieldCallback();
}

// The validate function would be invoked before the field was set,
// to make sure that new value is valid.
void Config::initFieldValidator() {
  std::map<std::string, validate_fn> validators = {
      {"requirepass",
       [this](const std::string &k, const std::string &v) -> Status {
         if (v.empty() && !tokens_.empty()) {
           return {Status::NotOK, "requirepass empty not allowed while the namespace exists"};
         }
         if (tokens_.find(v) != tokens_.end()) {
           return {Status::NotOK, "requirepass is duplicated with namespace tokens"};
         }
         return Status::OK();
       }},
      {"masterauth",
       [this](const std::string &k, const std::string &v) -> Status {
         if (tokens_.find(v) != tokens_.end()) {
           return {Status::NotOK, "masterauth is duplicated with namespace tokens"};
         }
         return Status::OK();
       }},
      {"compact-cron",
       [this](const std::string &k, const std::string &v) -> Status {
         std::vector<std::string> args = Util::Split(v, " \t");
         return compact_cron_.SetScheduleTime(args);
       }},
      {"bgsave-cron",
       [this](const std::string &k, const std::string &v) -> Status {
         std::vector<std::string> args = Util::Split(v, " \t");
         return bgsave_cron_.SetScheduleTime(args);
       }},
      {"compaction-checker-range",
       [this](const std::string &k, const std::string &v) -> Status {
         if (v.empty()) {
           compaction_checker_range_.start_ = -1;
           compaction_checker_range_.stop_ = -1;
           return Status::OK();
         }
         std::vector<std::string> args = Util::Split(v, "-");
         if (args.size() != 2) {
           return {Status::NotOK, "invalid range format, the range should be between 0 and 24"};
         }
         auto start = GET_OR_RET(ParseInt<int>(args[0], {0, 24}, 10)),
              stop = GET_OR_RET(ParseInt<int>(args[1], {0, 24}, 10));
         if (start > stop) return {Status::NotOK, "invalid range format, start should be smaller than stop"};
         compaction_checker_range_.start_ = start;
         compaction_checker_range_.stop_ = stop;
         return Status::OK();
       }},
      {"rename-command",
       [](const std::string &k, const std::string &v) -> Status {
         std::vector<std::string> all_args = Util::Split(v, "\n");
         for (auto &p : all_args) {
           std::vector<std::string> args = Util::Split(p, " \t");
           if (args.size() != 2) {
             return {Status::NotOK, "Invalid rename-command format"};
           }
           auto commands = Redis::GetCommands();
           auto cmd_iter = commands->find(Util::ToLower(args[0]));
           if (cmd_iter == commands->end()) {
             return {Status::NotOK, "No such command in rename-command"};
           }
           if (args[1] != "\"\"") {
             auto new_command_name = Util::ToLower(args[1]);
             if (commands->find(new_command_name) != commands->end()) {
               return {Status::NotOK, "Target command name already exists"};
             }
             (*commands)[new_command_name] = cmd_iter->second;
           }
           commands->erase(cmd_iter);
         }
         return Status::OK();
       }},
  };
  for (const auto &iter : validators) {
    auto field_iter = fields_.find(iter.first);
    if (field_iter != fields_.end()) {
      field_iter->second->validate_ = iter.second;
    }
  }
}

// The callback function would be invoked after the field was set,
// it may change related fields or re-format the field. for example,
// when the 'dir' was set, the db-dir or backup-dir should be reset as well.
void Config::initFieldCallback() {
  auto set_db_option_cb = [](Server *srv, const std::string &k, const std::string &v) -> Status {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    return srv->storage_->SetDBOption(trimRocksDBPrefix(k), v);
  };
  auto set_cf_option_cb = [](Server *srv, const std::string &k, const std::string &v) -> Status {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k), v);
  };
#ifdef ENABLE_OPENSSL
  auto set_tls_option = [](Server *srv, const std::string &k, const std::string &v) {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    auto new_ctx = CreateSSLContext(srv->GetConfig());
    if (!new_ctx) {
      return Status(Status::NotOK, "Failed to configure SSL context, check server log for more details");
    }
    srv->ssl_ctx_ = std::move(new_ctx);
    return Status::OK();
  };
#endif

  std::map<std::string, callback_fn> callbacks = {
      {"dir",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         db_dir_ = dir_ + "/db";
         {
           std::lock_guard<std::mutex> lg(this->backup_mu_);
           if (backup_dir_.empty()) {
             backup_dir_ = dir_ + "/backup";
           }
         }
         if (log_dir_.empty()) log_dir_ = dir_;
         checkpoint_dir_ = dir_ + "/checkpoint";
         sync_checkpoint_dir_ = dir_ + "/sync_checkpoint";
         backup_sync_dir_ = dir_ + "/backup_for_sync";
         return Status::OK();
       }},
      {"backup-dir",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         std::string previous_backup;
         {
           // Note: currently, backup_mu_ may block by backing up or purging,
           //  the command may wait for seconds.
           std::lock_guard<std::mutex> lg(this->backup_mu_);
           previous_backup = std::move(backup_dir_);
           backup_dir_ = v;
         }
         if (!previous_backup.empty() && srv != nullptr && !srv->IsLoading()) {
           // LOG(INFO) should be called after log is initialized and server is loaded.
           LOG(INFO) << "change backup dir from " << previous_backup << " to " << v;
         }
         return Status::OK();
       }},
      {"cluster-enabled",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (cluster_enabled_) slot_id_encoded_ = true;
         return Status::OK();
       }},
      {"bind",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         std::vector<std::string> args = Util::Split(v, " \t");
         binds_ = std::move(args);
         return Status::OK();
       }},
      {"maxclients",
       [](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         srv->AdjustOpenFilesLimit();
         return Status::OK();
       }},
      {"slaveof",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (v.empty()) {
           return Status::OK();
         }
         std::vector<std::string> args = Util::Split(v, " \t");
         if (args.size() != 2) return {Status::NotOK, "wrong number of arguments"};
         if (args[0] != "no" && args[1] != "one") {
           master_host_ = args[0];
           auto parse_result = ParseInt<int>(args[1], NumericRange<int>{1, PORT_LIMIT - 1}, 10);
           if (!parse_result) {
             return {Status::NotOK, "should be between 0 and 65535"};
           }
           master_port_ = *parse_result;
         }
         return Status::OK();
       }},
      {"profiling-sample-commands",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         std::vector<std::string> cmds = Util::Split(v, ",");
         profiling_sample_all_commands_ = false;
         profiling_sample_commands_.clear();
         for (auto const &cmd : cmds) {
           if (cmd == "*") {
             profiling_sample_all_commands_ = true;
             profiling_sample_commands_.clear();
             return Status::OK();
           }
           if (!Redis::IsCommandExists(cmd)) {
             return {Status::NotOK, cmd + " is not Kvrocks supported command"};
           }
           // profiling_sample_commands use command's original name, regardless of rename-command directive
           profiling_sample_commands_.insert(cmd);
         }
         return Status::OK();
       }},
      {"slowlog-max-len",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         srv->GetSlowLog()->SetMaxEntries(slowlog_max_len_);
         return Status::OK();
       }},
      {"max-db-size",
       [](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         srv->storage_->CheckDBSizeLimit();
         return Status::OK();
       }},
      {"max-io-mb",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         srv->storage_->SetIORateLimit(max_io_mb_);
         return Status::OK();
       }},
      {"profiling-sample-record-max-len",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         srv->GetPerfLog()->SetMaxEntries(profiling_sample_record_max_len_);
         return Status::OK();
       }},
      {"migrate-speed",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (cluster_enabled_) srv->slot_migrator_->SetMaxMigrationSpeed(migrate_speed_);
         return Status::OK();
       }},
      {"migrate-pipeline-size",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (cluster_enabled_) srv->slot_migrator_->SetMaxPipelineSize(pipeline_size_);
         return Status::OK();
       }},
      {"migrate-sequence-gap",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (cluster_enabled_) srv->slot_migrator_->SetSequenceGapLimit(sequence_gap_);
         return Status::OK();
       }},
      {"log-retention-days",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (Util::ToLower(log_dir_) == "stdout") {
           return {Status::NotOK, "can't set the 'log-retention-days' when the log dir is stdout"};
         }

         if (log_retention_days_ != -1) {
           google::EnableLogCleaner(log_retention_days_);
         } else {
           google::DisableLogCleaner();
         }
         return Status::OK();
       }},
      {"persist-cluster-nodes-enabled",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv || !cluster_enabled_) return Status::OK();
         auto nodes_file_path = NodesFilePath();
         if (v == "yes") {
           return srv->cluster_->DumpClusterNodes(nodes_file_path);
         }
         // Remove the cluster nodes file to avoid stale cluster nodes info
         remove(nodes_file_path.data());
         return Status::OK();
       }},
      {"rocksdb.target_file_size_base",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k),
                                                             std::to_string(rocks_db_.target_file_size_base_ * MiB));
       }},
      {"rocksdb.write_buffer_size",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k),
                                                             std::to_string(rocks_db_.write_buffer_size_ * MiB));
       }},
      {"rocksdb.disable_auto_compactions",
       [](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         std::string disable_auto_compactions = v == "yes" ? "true" : "false";
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k), disable_auto_compactions);
       }},
      {"rocksdb.max_total_wal_size",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         return srv->storage_->SetDBOption(trimRocksDBPrefix(k), std::to_string(rocks_db_.max_total_wal_size_ * MiB));
       }},
      {"rocksdb.enable_blob_files",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         std::string enable_blob_files = rocks_db_.enable_blob_files_ ? "true" : "false";
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k), enable_blob_files);
       }},
      {"rocksdb.min_blob_size",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (!rocks_db_.enable_blob_files_) {
           return {Status::NotOK, errBlobDbNotEnabled};
         }
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k), v);
       }},
      {"rocksdb.blob_file_size",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (!rocks_db_.enable_blob_files_) {
           return {Status::NotOK, errBlobDbNotEnabled};
         }
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k),
                                                             std::to_string(rocks_db_.blob_file_size_));
       }},
      {"rocksdb.enable_blob_garbage_collection",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (!rocks_db_.enable_blob_files_) {
           return {Status::NotOK, errBlobDbNotEnabled};
         }
         std::string enable_blob_garbage_collection = v == "yes" ? "true" : "false";
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k), enable_blob_garbage_collection);
       }},
      {"rocksdb.blob_garbage_collection_age_cutoff",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (!rocks_db_.enable_blob_files_) {
           return {Status::NotOK, errBlobDbNotEnabled};
         }
         int val = 0;
         auto parse_result = ParseInt<int>(v, 10);
         if (!parse_result) {
           return {Status::NotOK, "Illegal blob_garbage_collection_age_cutoff value."};
         }
         val = *parse_result;
         if (val < 0 || val > 100) {
           return {Status::NotOK, "blob_garbage_collection_age_cutoff must >= 0 and <= 100."};
         }

         double cutoff = val / 100.0;
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k), std::to_string(cutoff));
       }},
      {"rocksdb.level_compaction_dynamic_level_bytes",
       [](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         std::string level_compaction_dynamic_level_bytes = v == "yes" ? "true" : "false";
         return srv->storage_->SetDBOption(trimRocksDBPrefix(k), level_compaction_dynamic_level_bytes);
       }},
      {"rocksdb.max_bytes_for_level_base",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (!rocks_db_.level_compaction_dynamic_level_bytes_) {
           return {Status::NotOK, errLevelCompactionDynamicLevelBytesNotSet};
         }
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k),
                                                             std::to_string(rocks_db_.max_bytes_for_level_base_));
       }},
      {"rocksdb.max_bytes_for_level_multiplier",
       [this](Server *srv, const std::string &k, const std::string &v) -> Status {
         if (!srv) return Status::OK();
         if (!rocks_db_.level_compaction_dynamic_level_bytes_) {
           return {Status::NotOK, errLevelCompactionDynamicLevelBytesNotSet};
         }
         return srv->storage_->SetOptionForAllColumnFamilies(trimRocksDBPrefix(k), v);
       }},
      {"rocksdb.max_open_files", set_db_option_cb},
      {"rocksdb.stats_dump_period_sec", set_db_option_cb},
      {"rocksdb.delayed_write_rate", set_db_option_cb},
      {"rocksdb.max_background_compactions", set_db_option_cb},
      {"rocksdb.max_background_flushes", set_db_option_cb},
      {"rocksdb.compaction_readahead_size", set_db_option_cb},
      {"rocksdb.max_background_jobs", set_db_option_cb},

      {"rocksdb.max_write_buffer_number", set_cf_option_cb},
      {"rocksdb.level0_slowdown_writes_trigger", set_cf_option_cb},
      {"rocksdb.level0_stop_writes_trigger", set_cf_option_cb},
      {"rocksdb.level0_file_num_compaction_trigger", set_cf_option_cb},
#ifdef ENABLE_OPENSSL
      {"tls-cert-file", set_tls_option},
      {"tls-key-file", set_tls_option},
      {"tls-key-file-pass", set_tls_option},
      {"tls-ca-cert-file", set_tls_option},
      {"tls-ca-cert-dir", set_tls_option},
      {"tls-protocols", set_tls_option},
      {"tls-auth-clients", set_tls_option},
      {"tls-ciphers", set_tls_option},
      {"tls-ciphersuites", set_tls_option},
      {"tls-prefer-server-ciphers", set_tls_option},
      {"tls-session-caching", set_tls_option},
      {"tls-session-cache-size", set_tls_option},
      {"tls-session-cache-timeout", set_tls_option},
#endif
  };
  for (const auto &iter : callbacks) {
    auto field_iter = fields_.find(iter.first);
    if (field_iter != fields_.end()) {
      field_iter->second->callback_ = iter.second;
    }
  }
}

std::string Config::NodesFilePath() const { return dir_ + "/nodes.conf"; }

void Config::SetMaster(const std::string &host, uint32_t port) {
  master_host_ = host;
  master_port_ = port;
  auto iter = fields_.find("slaveof");
  if (iter != fields_.end()) {
    auto s = iter->second->Set(master_host_ + " " + std::to_string(master_port_));
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed to set the value of 'slaveof' setting: " << s.Msg();
    }
  }
}

void Config::ClearMaster() {
  master_host_.clear();
  master_port_ = 0;
  auto iter = fields_.find("slaveof");
  if (iter != fields_.end()) {
    auto s = iter->second->Set("no one");
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed to clear the value of 'slaveof' setting: " << s.Msg();
    }
  }
}

Status Config::parseConfigFromPair(const std::pair<std::string, std::string> &input, int line_number) {
  std::string field_key = Util::ToLower(input.first);
  const char ns_str[] = "namespace.";
  size_t ns_str_size = sizeof(ns_str) - 1;
  if (strncasecmp(input.first.data(), ns_str, ns_str_size) == 0) {
    // namespace should keep key case-sensitive
    field_key = input.first;
    tokens_[input.second] = input.first.substr(ns_str_size);
  }
  auto iter = fields_.find(field_key);
  if (iter != fields_.end()) {
    auto &field = iter->second;
    field->line_number_ = line_number;
    auto s = field->Set(input.second);
    if (!s.IsOK()) return s.Prefixed(fmt::format("failed to set value of field '{}'", field_key));
  }
  return Status::OK();
}

Status Config::parseConfigFromString(const std::string &input, int line_number) {
  auto parsed = ParseConfigLine(input);
  if (!parsed) return parsed.ToStatus().Prefixed("malformed line");

  auto kv = std::move(*parsed);

  if (kv.first.empty() || kv.second.empty()) return Status::OK();

  return parseConfigFromPair(kv, line_number);
}

Status Config::finish() {
  if (requirepass_.empty() && !tokens_.empty()) {
    return {Status::NotOK, "requirepass empty wasn't allowed while the namespace exists"};
  }
  if ((cluster_enabled_) && !tokens_.empty()) {
    return {Status::NotOK, "enabled cluster mode wasn't allowed while the namespace exists"};
  }
  if (unixsocket_.empty() && binds_.size() == 0) {
    binds_.emplace_back(kDefaultBindAddress);
  }
  if (cluster_enabled_ && binds_.size() == 0) {
    return {Status::NotOK,
            "node is in cluster mode, but TCP listen address "
            "wasn't specified via configuration file"};
  }
  if (master_port_ != 0 && binds_.size() == 0) {
    return {Status::NotOK, "replication doesn't support unix socket"};
  }
  if (db_dir_.empty()) db_dir_ = dir_ + "/db";
  if (backup_dir_.empty()) backup_dir_ = dir_ + "/backup";
  if (log_dir_.empty()) log_dir_ = dir_;
  if (pidfile_.empty()) pidfile_ = dir_ + "/kvrocks.pid";
  std::vector<std::string> create_dirs = {dir_};
  for (const auto &name : create_dirs) {
    auto s = rocksdb::Env::Default()->CreateDirIfMissing(name);
    if (!s.ok()) return {Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Config::Load(const CLIOptions &opts) {
  if (!opts.conf_file_.empty()) {
    std::ifstream file;
    std::istream *in = nullptr;
    if (opts.conf_file_ == "-") {
      in = &std::cin;
    } else {
      path_ = opts.conf_file_;
      file.open(path_);
      if (!file.is_open()) {
        return {Status::NotOK, fmt::format("failed to open file '{}': {}", path_, strerror(errno))};
      }

      in = &file;
    }

    std::string line;
    int line_num = 1;
    while (!in->eof()) {
      std::getline(*in, line);
      if (auto s = parseConfigFromString(line, line_num); !s.IsOK()) {
        return s.Prefixed(fmt::format("at line #L{}", line_num));
      }

      line_num++;
    }
  } else {
    std::cout << "WARNING: No config file specified, using the default configuration. "
              << "In order to specify a config file use 'kvrocks -c /path/to/kvrocks.conf'" << std::endl;
  }

  for (const auto &opt : opts.cli_options_) {
    GET_OR_RET(parseConfigFromPair(opt, -1).Prefixed("CLI config option error"));
  }

  for (const auto &iter : fields_) {
    // line_number = 0 means the user didn't specify the field value
    // on config file and would use default value, so won't validate here.
    if (iter.second->line_number_ != 0 && iter.second->validate_) {
      auto s = iter.second->validate_(iter.first, iter.second->ToString());
      if (!s.IsOK()) {
        return s.Prefixed(fmt::format("at line #L{}: {} is invalid", iter.second->line_number_, iter.first));
      }
    }
  }

  for (const auto &iter : fields_) {
    if (iter.second->callback_) {
      auto s = iter.second->callback_(nullptr, iter.first, iter.second->ToString());
      if (!s.IsOK()) {
        return s.Prefixed(fmt::format("while changing key '{}'", iter.first));
      }
    }
  }
  return finish();
}

void Config::Get(const std::string &key, std::vector<std::string> *values) {
  values->clear();
  for (const auto &iter : fields_) {
    if (key == "*" || Util::ToLower(key) == iter.first) {
      if (iter.second->IsMultiConfig()) {
        for (const auto &p : Util::Split(iter.second->ToString(), "\n")) {
          values->emplace_back(iter.first);
          values->emplace_back(p);
        }
      } else {
        values->emplace_back(iter.first);
        values->emplace_back(iter.second->ToString());
      }
    }
  }
}

Status Config::Set(Server *svr, std::string key, const std::string &value) {
  key = Util::ToLower(key);
  auto iter = fields_.find(key);
  if (iter == fields_.end() || iter->second->readonly_) {
    return {Status::NotOK, "Unsupported CONFIG parameter: " + key};
  }

  auto &field = iter->second;
  if (field->validate_) {
    auto s = field->validate_(key, value);
    if (!s.IsOK()) return s.Prefixed("invalid value");
  }

  auto s = field->Set(value);
  if (!s.IsOK()) return s.Prefixed("failed to set new value");

  if (field->callback_) {
    return field->callback_(svr, key, value);
  }

  return Status::OK();
}

Status Config::Rewrite() {
  if (path_.empty()) {
    return {Status::NotOK, "the server is running without a config file"};
  }

  std::vector<std::string> lines;
  std::map<std::string, std::string> new_config;
  for (const auto &iter : fields_) {
    if (iter.second->IsMultiConfig()) {
      // We should NOT overwrite the commands which are MultiConfig since it cannot be rewritten in-flight,
      // so skip it here to avoid rewriting it as new item.
      continue;
    }
    new_config[iter.first] = iter.second->ToString();
  }

  std::string namespace_prefix = "namespace.";
  for (const auto &iter : tokens_) {
    new_config[namespace_prefix + iter.second] = iter.first;
  }

  std::ifstream file(path_);
  if (file.is_open()) {
    std::string raw_line;
    while (!file.eof()) {
      std::getline(file, raw_line);
      auto parsed = ParseConfigLine(raw_line);
      if (!parsed || parsed->first.empty()) {
        lines.emplace_back(raw_line);
        continue;
      }
      auto kv = std::move(*parsed);
      if (Util::HasPrefix(kv.first, namespace_prefix)) {
        // Ignore namespace fields here since we would always rewrite them
        continue;
      }
      auto iter = new_config.find(Util::ToLower(kv.first));
      if (iter != new_config.end()) {
        if (!iter->second.empty()) lines.emplace_back(DumpConfigLine({iter->first, iter->second}));
        new_config.erase(iter);
      } else {
        lines.emplace_back(raw_line);
      }
    }
  }
  file.close();

  std::string out_buf;
  for (const auto &line : lines) {
    fmt::format_to(std::back_inserter(out_buf), "{}\n", line);
  }
  for (const auto &remain : new_config) {
    if (remain.second.empty()) continue;
    fmt::format_to(std::back_inserter(out_buf), "{} {}\n", remain.first, remain.second);
  }
  std::string tmp_path = path_ + ".tmp";
  remove(tmp_path.data());
  std::ofstream output_file(tmp_path, std::ios::out);
  output_file << out_buf;
  output_file.close();
  if (rename(tmp_path.data(), path_.data()) < 0) {
    return {Status::NotOK, fmt::format("rename file encounter error: {}", strerror(errno))};
  }
  return Status::OK();
}

Status Config::GetNamespace(const std::string &ns, std::string *token) {
  token->clear();
  for (const auto &iter : tokens_) {
    if (iter.second == ns) {
      *token = iter.first;
      return Status::OK();
    }
  }
  return {Status::NotFound};
}

Status Config::SetNamespace(const std::string &ns, const std::string &token) {
  if (ns == kDefaultNamespace) {
    return {Status::NotOK, "forbidden to update the default namespace"};
  }
  if (tokens_.find(token) != tokens_.end()) {
    return {Status::NotOK, "the token has already exists"};
  }

  if (token == requirepass_ || token == masterauth_) {
    return {Status::NotOK, "the token is duplicated with requirepass or masterauth"};
  }

  for (const auto &iter : tokens_) {
    if (iter.second == ns) {
      tokens_.erase(iter.first);
      tokens_[token] = ns;
      auto s = Rewrite();
      if (!s.IsOK()) {
        // Need to roll back the old token if fails to rewrite the config
        tokens_.erase(token);
        tokens_[iter.first] = ns;
      }
      return s;
    }
  }
  return {Status::NotOK, "the namespace was not found"};
}

Status Config::AddNamespace(const std::string &ns, const std::string &token) {
  if (requirepass_.empty()) {
    return {Status::NotOK, "forbidden to add namespace when requirepass was empty"};
  }
  if (cluster_enabled_) {
    return {Status::NotOK, "forbidden to add namespace when cluster mode was enabled"};
  }
  if (ns == kDefaultNamespace) {
    return {Status::NotOK, "forbidden to add the default namespace"};
  }
  auto s = isNamespaceLegal(ns);
  if (!s.IsOK()) return s;
  if (tokens_.find(token) != tokens_.end()) {
    return {Status::NotOK, "the token has already exists"};
  }

  if (token == requirepass_ || token == masterauth_) {
    return {Status::NotOK, "the token is duplicated with requirepass or masterauth"};
  }

  for (const auto &iter : tokens_) {
    if (iter.second == ns) {
      return {Status::NotOK, "the namespace has already exists"};
    }
  }
  tokens_[token] = ns;

  s = Rewrite();
  if (!s.IsOK()) {
    tokens_.erase(token);
  }
  return s;
}

Status Config::DelNamespace(const std::string &ns) {
  if (ns == kDefaultNamespace) {
    return {Status::NotOK, "forbidden to delete the default namespace"};
  }
  for (const auto &iter : tokens_) {
    if (iter.second == ns) {
      tokens_.erase(iter.first);
      auto s = Rewrite();
      if (!s.IsOK()) {
        tokens_[iter.first] = ns;
      }
      return s;
    }
  }
  return {Status::NotOK, "the namespace was not found"};
}

Status Config::isNamespaceLegal(const std::string &ns) {
  if (ns.size() > UINT8_MAX) {
    return {Status::NotOK, fmt::format("size exceed limit {}", UINT8_MAX)};
  }
  char last_char = ns.back();
  if (last_char == std::numeric_limits<char>::max()) {
    return {Status::NotOK, "namespace contain illegal letter"};
  }
  return Status::OK();
}
