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

#include <fcntl.h>
#include <string.h>
#include <strings.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>
#include <limits>
#include <algorithm>
#include <cctype>
#include <glog/logging.h>
#include <rocksdb/env.h>

#include "config.h"
#include "config_type.h"
#include "tls_util.h"
#include "util.h"
#include "status.h"
#include "cron.h"
#include "server.h"
#include "log_collector.h"
#include "config_util.h"

const char *kDefaultNamespace = "__namespace";

const char *errNotEnableBlobDB = "Must set rocksdb.enable_blob_files to yes first.";

const char *errNotSetLevelCompactionDynamicLevelBytes =
            "Must set rocksdb.level_compaction_dynamic_level_bytes yes first.";

const char *kDefaultBindAddress = "127.0.0.1";

configEnum compression_type_enum[] = {
    {"no", rocksdb::CompressionType::kNoCompression},
    {"snappy", rocksdb::CompressionType::kSnappyCompression},
    {"lz4", rocksdb::CompressionType::kLZ4Compression},
    {"zstd", rocksdb::CompressionType::kZSTD},
    {nullptr, 0}
};

configEnum supervised_mode_enum[] = {
    {"no", SUPERVISED_NONE},
    {"auto", SUPERVISED_AUTODETECT},
    {"upstart", SUPERVISED_UPSTART},
    {"systemd", SUPERVISED_SYSTEMD},
    {nullptr, 0}
};

std::string trimRocksDBPrefix(std::string s) {
  if (strncasecmp(s.data(), "rocksdb.", 8)) return s;
  return s.substr(8, s.size()-8);
}

int configEnumGetValue(configEnum *ce, const char *name) {
  while (ce->name != nullptr) {
    if (!strcasecmp(ce->name, name)) return ce->val;
    ce++;
  }
  return INT_MIN;
}

const char *configEnumGetName(configEnum *ce, int val) {
  while (ce->name != nullptr) {
    if (ce->val == val) return ce->name;
    ce++;
  }
  return nullptr;
}

Config::Config() {
  struct FieldWrapper {
    std::string name;
    bool readonly;
    std::unique_ptr<ConfigField> field;

    FieldWrapper(std::string name, bool readonly, ConfigField *field)
        : name(std::move(name)), readonly(readonly), field(field) {}
  };
  FieldWrapper fields[] = {
      {"daemonize", true, new YesNoField(&daemonize, false)},
      {"bind", true, new StringField(&binds_, "")},
      {"port", true, new IntField(&port, kDefaultPort, 1, 65535)},
#ifdef ENABLE_OPENSSL
      {"tls-port", true, new IntField(&tls_port, 0, 0, 65535)},
      {"tls-cert-file", false, new StringField(&tls_cert_file, "")},
      {"tls-key-file", false, new StringField(&tls_key_file, "")},
      {"tls-key-file-pass", false, new StringField(&tls_key_file_pass, "")},
      {"tls-ca-cert-file", false, new StringField(&tls_ca_cert_file, "")},
      {"tls-ca-cert-dir", false, new StringField(&tls_ca_cert_dir, "")},
      {"tls-protocols", false, new StringField(&tls_protocols, "")},
      {"tls-auth-clients", false, new StringField(&tls_auth_clients, "")},
      {"tls-ciphers", false, new StringField(&tls_ciphers, "")},
      {"tls-ciphersuites", false, new StringField(&tls_ciphersuites, "")},
      {"tls-prefer-server-ciphers", false, new YesNoField(&tls_prefer_server_ciphers, false)},
      {"tls-session-caching", false, new YesNoField(&tls_session_caching, true)},
      {"tls-session-cache-size", false,
        new IntField(&tls_session_cache_size, 1024 * 20, 0, INT_MAX)},
      {"tls-session-cache-timeout", false, new IntField(&tls_session_cache_timeout, 300, 0, INT_MAX)},
#endif
      {"workers", true, new IntField(&workers, 8, 1, 256)},
      {"timeout", false, new IntField(&timeout, 0, 0, INT_MAX)},
      {"tcp-backlog", true, new IntField(&backlog, 511, 0, INT_MAX)},
      {"maxclients", false, new IntField(&maxclients, 10240, 0, INT_MAX)},
      {"max-backup-to-keep", false, new IntField(&max_backup_to_keep, 1, 0, 1)},
      {"max-backup-keep-hours", false, new IntField(&max_backup_keep_hours, 0, 0, INT_MAX)},
      {"master-use-repl-port", false, new YesNoField(&master_use_repl_port, false)},
      {"requirepass", false, new StringField(&requirepass, "")},
      {"masterauth", false, new StringField(&masterauth, "")},
      {"slaveof", true, new StringField(&slaveof_, "")},
      {"compact-cron", false, new StringField(&compact_cron_, "")},
      {"bgsave-cron", false, new StringField(&bgsave_cron_, "")},
      {"compaction-checker-range", false, new StringField(&compaction_checker_range_, "")},
      {"db-name", true, new StringField(&db_name, "change.me.db")},
      {"dir", true, new StringField(&dir, "/tmp/kvrocks")},
      {"backup-dir", true, new StringField(&backup_dir, "")},
      {"log-dir", true, new StringField(&log_dir, "")},
      {"pidfile", true, new StringField(&pidfile, "")},
      {"max-io-mb", false, new IntField(&max_io_mb, 500, 0, INT_MAX)},
      {"max-bitmap-to-string-mb", false, new IntField(&max_bitmap_to_string_mb, 16, 0, INT_MAX)},
      {"max-db-size", false, new IntField(&max_db_size, 0, 0, INT_MAX)},
      {"max-replication-mb", false, new IntField(&max_replication_mb, 0, 0, INT_MAX)},
      {"supervised", true, new EnumField(&supervised_mode, supervised_mode_enum, SUPERVISED_NONE)},
      {"slave-serve-stale-data", false, new YesNoField(&slave_serve_stale_data, true)},
      {"slave-empty-db-before-fullsync", false, new YesNoField(&slave_empty_db_before_fullsync, false)},
      {"slave-priority", false, new IntField(&slave_priority, 100, 0, INT_MAX)},
      {"slave-read-only", false, new YesNoField(&slave_readonly, true)},
      {"use-rsid-psync", true, new YesNoField(&use_rsid_psync, false)},
      {"profiling-sample-ratio", false, new IntField(&profiling_sample_ratio, 0, 0, 100)},
      {"profiling-sample-record-max-len", false, new IntField(&profiling_sample_record_max_len, 256, 0, INT_MAX)},
      {"profiling-sample-record-threshold-ms",
       false, new IntField(&profiling_sample_record_threshold_ms, 100, 0, INT_MAX)},
      {"slowlog-log-slower-than", false, new IntField(&slowlog_log_slower_than, 200000, -1, INT_MAX)},
      {"profiling-sample-commands", false, new StringField(&profiling_sample_commands_, "")},
      {"slowlog-max-len", false, new IntField(&slowlog_max_len, 128, 0, INT_MAX)},
      {"purge-backup-on-fullsync", false, new YesNoField(&purge_backup_on_fullsync, false)},
      {"rename-command", true, new StringField(&rename_command_, "")},
      {"auto-resize-block-and-sst", false, new YesNoField(&auto_resize_block_and_sst, true)},
      {"fullsync-recv-file-delay", false, new IntField(&fullsync_recv_file_delay, 0, 0, INT_MAX)},
      {"cluster-enabled", true, new YesNoField(&cluster_enabled, false)},
      {"migrate-speed", false, new IntField(&migrate_speed, 4096, 0, INT_MAX)},
      {"migrate-pipeline-size", false, new IntField(&pipeline_size, 16, 1, INT_MAX)},
      {"migrate-sequence-gap", false, new IntField(&sequence_gap, 10000, 1, INT_MAX)},
      {"unixsocket", true, new StringField(&unixsocket, "")},
      {"unixsocketperm", true, new OctalField(&unixsocketperm, 0777, 1, INT_MAX)},

      /* rocksdb options */
      {"rocksdb.compression", false, new EnumField(&RocksDB.compression, compression_type_enum, 0)},
      {"rocksdb.block_size", true, new IntField(&RocksDB.block_size, 4096, 0, INT_MAX)},
      {"rocksdb.max_open_files", false, new IntField(&RocksDB.max_open_files, 4096, -1, INT_MAX)},
      {"rocksdb.write_buffer_size", false, new IntField(&RocksDB.write_buffer_size, 64, 0, 4096)},
      {"rocksdb.max_write_buffer_number", false, new IntField(&RocksDB.max_write_buffer_number, 4, 0, 256)},
      {"rocksdb.target_file_size_base", false, new IntField(&RocksDB.target_file_size_base, 128, 1, 1024)},
      {"rocksdb.max_background_compactions", false, new IntField(&RocksDB.max_background_compactions, 2, 0, 32)},
      {"rocksdb.max_background_flushes", true, new IntField(&RocksDB.max_background_flushes, 2, 0, 32)},
      {"rocksdb.max_sub_compactions", false, new IntField(&RocksDB.max_sub_compactions, 1, 0, 16)},
      {"rocksdb.delayed_write_rate", false, new Int64Field(&RocksDB.delayed_write_rate, 0, 0, INT64_MAX)},
      {"rocksdb.wal_ttl_seconds", true, new IntField(&RocksDB.WAL_ttl_seconds, 3*3600, 0, INT_MAX)},
      {"rocksdb.wal_size_limit_mb", true, new IntField(&RocksDB.WAL_size_limit_MB, 16384, 0, INT_MAX)},
      {"rocksdb.max_total_wal_size", false, new IntField(&RocksDB.max_total_wal_size, 64*4*2, 0, INT_MAX)},
      {"rocksdb.disable_auto_compactions", false, new YesNoField(&RocksDB.disable_auto_compactions, false)},
      {"rocksdb.enable_pipelined_write", true, new YesNoField(&RocksDB.enable_pipelined_write, false)},
      {"rocksdb.stats_dump_period_sec", false, new IntField(&RocksDB.stats_dump_period_sec, 0, 0, INT_MAX)},
      {"rocksdb.cache_index_and_filter_blocks", true, new YesNoField(&RocksDB.cache_index_and_filter_blocks, false)},
      {"rocksdb.subkey_block_cache_size", true, new IntField(&RocksDB.subkey_block_cache_size, 2048, 0, INT_MAX)},
      {"rocksdb.metadata_block_cache_size", true, new IntField(&RocksDB.metadata_block_cache_size, 2048, 0, INT_MAX)},
      {"rocksdb.share_metadata_and_subkey_block_cache",
       true, new YesNoField(&RocksDB.share_metadata_and_subkey_block_cache, true)},
      {"rocksdb.row_cache_size", true, new IntField(&RocksDB.row_cache_size, 0, 0, INT_MAX)},
      {"rocksdb.compaction_readahead_size", false, new IntField(&RocksDB.compaction_readahead_size, 2*MiB, 0, 64*MiB)},
      {"rocksdb.level0_slowdown_writes_trigger",
       false, new IntField(&RocksDB.level0_slowdown_writes_trigger, 20, 1, 1024)},
      {"rocksdb.level0_stop_writes_trigger",
       false, new IntField(&RocksDB.level0_stop_writes_trigger, 40, 1, 1024)},
      {"rocksdb.level0_file_num_compaction_trigger",
       false, new IntField(&RocksDB.level0_file_num_compaction_trigger, 4, 1, 1024)},
      {"rocksdb.enable_blob_files", false, new YesNoField(&RocksDB.enable_blob_files, false)},
      {"rocksdb.min_blob_size", false, new IntField(&RocksDB.min_blob_size, 4096, 0, INT_MAX)},
      {"rocksdb.blob_file_size", false, new IntField(&RocksDB.blob_file_size, 268435456, 0, INT_MAX)},
      {"rocksdb.enable_blob_garbage_collection", false, new YesNoField(&RocksDB.enable_blob_garbage_collection, true)},
      {"rocksdb.blob_garbage_collection_age_cutoff",
       false, new IntField(&RocksDB.blob_garbage_collection_age_cutoff, 25, 0, 100)},
      {"rocksdb.max_bytes_for_level_base",
        false, new IntField(&RocksDB.max_bytes_for_level_base, 268435456, 0, INT_MAX)},
      {"rocksdb.max_bytes_for_level_multiplier",
        false, new IntField(&RocksDB.max_bytes_for_level_multiplier, 10, 1, 100)},
      {"rocksdb.level_compaction_dynamic_level_bytes",
        false, new YesNoField(&RocksDB.level_compaction_dynamic_level_bytes, false)},

      /* rocksdb write options */
      {"rocksdb.write_options.sync", true, new YesNoField(&RocksDB.write_options.sync, false)},
      {"rocksdb.write_options.disable_wal", true, new YesNoField(&RocksDB.write_options.disable_WAL, false)},
      {"rocksdb.write_options.no_slowdown", true, new YesNoField(&RocksDB.write_options.no_slowdown, false)},
      {"rocksdb.write_options.low_pri", true, new YesNoField(&RocksDB.write_options.low_pri, false)},
      {"rocksdb.write_options.memtable_insert_hint_per_batch",
        true, new YesNoField(&RocksDB.write_options.memtable_insert_hint_per_batch, false)},
  };
  for (auto &wrapper : fields) {
    auto &field = wrapper.field;
    field->readonly = wrapper.readonly;
    fields_.emplace(std::move(wrapper.name), std::move(field));
  }
  initFieldValidator();
  initFieldCallback();
}

// The validate function would be invoked before the field was set,
// to make sure that new value is valid.
void Config::initFieldValidator() {
  std::map<std::string, validate_fn> validators = {
      {"requirepass", [this](const std::string& k, const std::string& v)->Status {
        if (v.empty() && !tokens.empty()) {
          return Status(Status::NotOK, "requirepass empty not allowed while the namespace exists");
        }
        if (tokens.find(v) != tokens.end()) {
          return Status(Status::NotOK, "requirepass is duplicated with namespace tokens");
        }
        return Status::OK();
      }},
      {"masterauth", [this](const std::string& k, const std::string& v)->Status {
        if (tokens.find(v) != tokens.end()) {
          return Status(Status::NotOK, "masterauth is duplicated with namespace tokens");
        }
        return Status::OK();
      }},
      {"compact-cron", [this](const std::string& k, const std::string& v)->Status {
        std::vector<std::string> args = Util::Split(v, " \t");
        return compact_cron.SetScheduleTime(args);
      }},
      {"bgsave-cron", [this](const std::string& k, const std::string& v)->Status {
        std::vector<std::string> args = Util::Split(v, " \t");
        return bgsave_cron.SetScheduleTime(args);
      }},
      {"compaction-checker-range", [this](const std::string& k, const std::string& v)->Status {
        if (v.empty()) {
          compaction_checker_range.Start = -1;
          compaction_checker_range.Stop = -1;
          return Status::OK();
        }
        std::vector<std::string> args = Util::Split(v, "-");
        if (args.size() != 2) {
          return Status(Status::NotOK, "invalid range format, the range should be between 0 and 24");
        }
        int64_t start, stop;
        Status s = Util::DecimalStringToNum(args[0], &start, 0, 24);
        if (!s.IsOK()) return s;
        s = Util::DecimalStringToNum(args[1], &stop, 0, 24);
        if (!s.IsOK()) return s;
        if (start > stop)  return Status(Status::NotOK, "invalid range format, start should be smaller than stop");
        compaction_checker_range.Start = start;
        compaction_checker_range.Stop = stop;
        return Status::OK();
      }},
      {"rename-command", [](const std::string &k, const std::string &v) -> Status {
        std::vector<std::string> args = Util::Split(v, " \t");
        if (args.size() != 2) {
          return Status(Status::NotOK, "Invalid rename-command format");
        }
        auto commands = Redis::GetCommands();
        auto cmd_iter = commands->find(Util::ToLower(args[0]));
        if (cmd_iter == commands->end()) {
          return Status(Status::NotOK, "No such command in rename-command");
        }
        if (args[1] != "\"\"") {
          auto new_command_name = Util::ToLower(args[1]);
          if (commands->find(new_command_name) != commands->end()) {
            return Status(Status::NotOK, "Target command name already exists");
          }
          (*commands)[new_command_name] = cmd_iter->second;
        }
        commands->erase(cmd_iter);
        return Status::OK();
      }},
  };
  for (const auto &iter : validators) {
    auto field_iter = fields_.find(iter.first);
    if (field_iter != fields_.end()) {
      field_iter->second->validate = iter.second;
    }
  }
}

// The callback function would be invoked after the field was set,
// it may change related fileds or re-format the field. for example,
// when the 'dir' was set, the db-dir or backup-dir should be reset as well.
void Config::initFieldCallback() {
  auto set_db_option_cb = [](Server* srv,  const std::string &k, const std::string& v)->Status {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    return srv->storage_->SetDBOption(trimRocksDBPrefix(k), v);
  };
  auto set_cf_option_cb = [](Server* srv,  const std::string &k, const std::string& v)->Status {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k), v);
  };
#ifdef ENABLE_OPENSSL
  auto set_tls_option = [](Server* srv,  const std::string &k, const std::string& v) {
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
      {"dir", [this](Server* srv,  const std::string &k, const std::string& v)->Status {
        db_dir = dir + "/db";
        if (backup_dir.empty()) backup_dir = dir + "/backup";
        if (log_dir.empty()) log_dir = dir;
        checkpoint_dir = dir + "/checkpoint";
        sync_checkpoint_dir = dir + "/sync_checkpoint";
        backup_sync_dir = dir + "/backup_for_sync";
        return Status::OK();
      }},
      {"cluster-enabled", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (cluster_enabled) slot_id_encoded = true;
        return Status::OK();
      }},
      {"bind", [this](Server* srv,  const std::string &k,  const std::string& v)->Status {
        trimRocksDBPrefix(k);
        std::vector<std::string> args = Util::Split(v, " \t");
        binds = std::move(args);
        return Status::OK();
      }},
      { "maxclients", [](Server* srv, const std::string &k, const std::string& v) -> Status {
        if (!srv) return Status::OK();
        srv->AdjustOpenFilesLimit();
        return Status::OK();
      }},
      {"slaveof", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (v.empty()) {
          return Status::OK();
        }
        std::vector<std::string> args = Util::Split(v, " \t");
        if (args.size() != 2) return Status(Status::NotOK, "wrong number of arguments");
        if (args[0] != "no" && args[1] != "one") {
          master_host = args[0];
          master_port = std::atoi(args[1].c_str());
          if (master_port <= 0 || master_port >= 65535) {
            return Status(Status::NotOK, "should be between 0 and 65535");
          }
        }
        return Status::OK();
      }},
      {"profiling-sample-commands", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        std::vector<std::string> cmds = Util::Split(v, ",");
        profiling_sample_all_commands = false;
        profiling_sample_commands.clear();
        for (auto const &cmd : cmds) {
          if (cmd == "*") {
            profiling_sample_all_commands = true;
            profiling_sample_commands.clear();
            return Status::OK();
          }
          if (!Redis::IsCommandExists(cmd)) {
            return Status(Status::NotOK, cmd + " is not Kvrocks supported command");
          }
          // profiling_sample_commands use command's original name, regardless of rename-command directive
          profiling_sample_commands.insert(cmd);
        }
        return Status::OK();
      }},
      {"slowlog-max-len", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        srv->GetSlowLog()->SetMaxEntries(slowlog_max_len);
        return Status::OK();
      }},
      {"max-db-size", [](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        srv->storage_->CheckDBSizeLimit();
        return Status::OK();
      }},
      {"max-io-mb", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        srv->storage_->SetIORateLimit(static_cast<uint64_t>(max_io_mb));
        return Status::OK();
      }},
      {"profiling-sample-record-max-len", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        srv->GetPerfLog()->SetMaxEntries(profiling_sample_record_max_len);
        return Status::OK();
      }},
      {"migrate-speed", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (cluster_enabled) srv->slot_migrate_->SetMigrateSpeedLimit(migrate_speed);
        return Status::OK();
      }},
      {"migrate-pipeline-size", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (cluster_enabled) srv->slot_migrate_->SetPipelineSize(pipeline_size);
        return Status::OK();
      }},
      {"migrate-sequence-gap", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (cluster_enabled) srv->slot_migrate_->SetSequenceGapSize(sequence_gap);
        return Status::OK();
      }},
      {"rocksdb.target_file_size_base", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k),
                                                    std::to_string(RocksDB.target_file_size_base * MiB));
      }},
      {"rocksdb.write_buffer_size", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k),
                                                    std::to_string(RocksDB.write_buffer_size * MiB));
      }},
      {"rocksdb.disable_auto_compactions", [](Server* srv,
                                                        const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        std::string disable_auto_compactions = v == "yes" ? "true" : "false";
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k), disable_auto_compactions);
      }},
      {"rocksdb.max_total_wal_size", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        return srv->storage_->SetDBOption(trimRocksDBPrefix(k),
                                          std::to_string(RocksDB.max_total_wal_size * MiB));
      }},
      {"rocksdb.enable_blob_files", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        std::string enable_blob_files = RocksDB.enable_blob_files ? "true" : "false";
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k), enable_blob_files);
      }},
      {"rocksdb.min_blob_size", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (!RocksDB.enable_blob_files) {
          return Status(Status::NotOK, errNotEnableBlobDB);
        }
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k), v);
      }},
      {"rocksdb.blob_file_size", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (!RocksDB.enable_blob_files) {
          return Status(Status::NotOK, errNotEnableBlobDB);
        }
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k),
                                                    std::to_string(RocksDB.blob_file_size));
      }},
      {"rocksdb.enable_blob_garbage_collection", [this](Server* srv, const std::string &k,
                                                        const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (!RocksDB.enable_blob_files) {
          return Status(Status::NotOK, errNotEnableBlobDB);
        }
        std::string enable_blob_garbage_collection = v == "yes" ? "true" : "false";
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k), enable_blob_garbage_collection);
      }},
      {"rocksdb.blob_garbage_collection_age_cutoff", [this](Server* srv, const std::string &k,
                                                            const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (!RocksDB.enable_blob_files) {
          return Status(Status::NotOK, errNotEnableBlobDB);
        }
        int val;
        try {
          val = std::stoi(v);
        } catch (std::exception &e) {
          return Status(Status::NotOK, "Illegal blob_garbage_collection_age_cutoff value.");
        }
        if (val < 0 || val > 100) {
          return Status(Status::NotOK, "blob_garbage_collection_age_cutoff must >= 0 and <= 100.");
        }

        double cutoff = val / 100.0;
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k), std::to_string(cutoff));
      }},
      {"rocksdb.level_compaction_dynamic_level_bytes", [](Server* srv, const std::string &k,
                                                        const std::string& v)->Status {
        if (!srv) return Status::OK();
        std::string level_compaction_dynamic_level_bytes = v == "yes" ? "true" : "false";
        return srv->storage_->SetDBOption(trimRocksDBPrefix(k), level_compaction_dynamic_level_bytes);
      }},
      {"rocksdb.max_bytes_for_level_base", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (!RocksDB.level_compaction_dynamic_level_bytes) {
          return Status(Status::NotOK, errNotSetLevelCompactionDynamicLevelBytes);
        }
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k),
                                                    std::to_string(RocksDB.max_bytes_for_level_base));
      }},
      {"rocksdb.max_bytes_for_level_multiplier", [this](Server* srv, const std::string &k,
                                                   const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (!RocksDB.level_compaction_dynamic_level_bytes) {
          return Status(Status::NotOK, errNotSetLevelCompactionDynamicLevelBytes);
        }
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k), v);
      }},
      {"rocksdb.max_open_files", set_db_option_cb},
      {"rocksdb.stats_dump_period_sec", set_db_option_cb},
      {"rocksdb.delayed_write_rate", set_db_option_cb},
      {"rocksdb.max_background_compactions", set_db_option_cb},
      {"rocksdb.max_background_flushes", set_db_option_cb},
      {"rocksdb.compaction_readahead_size", set_db_option_cb},

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
      field_iter->second->callback = iter.second;
    }
  }
}

void Config::SetMaster(const std::string &host, int port) {
  master_host = host;
  master_port = port;
  auto iter = fields_.find("slaveof");
  if (iter != fields_.end()) {
    iter->second->Set(master_host+" "+std::to_string(master_port));
  }
}

void Config::ClearMaster() {
  master_host.clear();
  master_port = 0;
  auto iter = fields_.find("slaveof");
  if (iter != fields_.end()) {
    iter->second->Set("no one");
  }
}

Status Config::parseConfigFromString(const std::string &input, int line_number) {
  auto parsed = ParseConfigLine(input);
  if (!parsed) return parsed.ToStatus();

  auto kv = std::move(*parsed);

  if (kv.first.empty() || kv.second.empty()) return Status::OK();

  std::string field_key = Util::ToLower(kv.first);
  const char ns_str[] = "namespace.";
  size_t ns_str_size = sizeof(ns_str) - 1;
  if (!strncasecmp(kv.first.data(), ns_str, ns_str_size)) {
      // namespace should keep key case-sensitive
      field_key = kv.first;
      tokens[kv.second] = kv.first.substr(ns_str_size);
  }
  auto iter = fields_.find(field_key);
  if (iter != fields_.end()) {
    auto& field = iter->second;
    field->line_number = line_number;
    auto s = field->Set(kv.second);
    if (!s.IsOK()) return s;
  }
  return Status::OK();
}

Status Config::finish() {
  if (requirepass.empty() && !tokens.empty()) {
    return Status(Status::NotOK, "requirepass empty wasn't allowed while the namespace exists");
  }
  if ((cluster_enabled) && !tokens.empty()) {
    return Status(Status::NotOK, "enabled cluster mode wasn't allowed while the namespace exists");
  }
  if (unixsocket.empty() && binds.size() == 0) {
    binds.emplace_back(kDefaultBindAddress);
  }
  if (cluster_enabled && binds.size() == 0) {
    return Status(Status::NotOK, "node is in cluster mode, but TCP listen address "
                                 "wasn't specified via configuration file");
  }
  if (master_port != 0 && binds.size() == 0) {
    return Status(Status::NotOK, "replication doesn't supports unix socket");
  }
  if (db_dir.empty()) db_dir = dir + "/db";
  if (backup_dir.empty()) backup_dir = dir + "/backup";
  if (log_dir.empty()) log_dir = dir;
  if (pidfile.empty()) pidfile = dir + "/kvrocks.pid";
  std::vector<std::string> createDirs = {dir};
  for (const auto &name : createDirs) {
    auto s = rocksdb::Env::Default()->CreateDirIfMissing(name);
    if (!s.ok()) return Status(Status::NotOK, s.ToString());
  }
  return Status::OK();
}

Status Config::Load(const std::string &path) {
  if (!path.empty()) {
    path_ = path;
    std::ifstream file(path_);
    if (!file.is_open()) return Status(Status::NotOK, strerror(errno));

    std::string line;
    int line_num = 1;
    while (!file.eof()) {
      std::getline(file, line);
      Status s = parseConfigFromString(line, line_num);
      if (!s.IsOK()) {
        file.close();
        return Status(Status::NotOK, "at line: #L" + std::to_string(line_num) + ", err: " + s.Msg());
      }
      line_num++;
    }
    file.close();
  } else {
    std::cout << "Warn: no config file specified, using the default config. "
                    "In order to specify a config file use kvrocks -c /path/to/kvrocks.conf" << std::endl;
  }

  for (const auto &iter : fields_) {
    // line_number = 0 means the user didn't specify the field value
    // on config file and would use default value, so won't validate here.
    if (iter.second->line_number != 0 && iter.second->validate) {
      auto s = iter.second->validate(iter.first, iter.second->ToString());
      if (!s.IsOK()) {
      return Status(Status::NotOK, "at line: #L" + std::to_string(iter.second->line_number)
                    + ", " + iter.first + " is invalid: " + s.Msg());
      }
    }
  }

  for (const auto &iter : fields_) {
    if (iter.second->callback) {
      auto s = iter.second->callback(nullptr, iter.first, iter.second->ToString());
      if (!s.IsOK()) {
        return Status(Status::NotOK, s.Msg()+" in key '"+iter.first+"'");
      }
    }
  }
  return finish();
}

void Config::Get(std::string key, std::vector<std::string> *values) {
  values->clear();
  for (const auto &iter : fields_) {
    if (key == "*" || Util::ToLower(key) == iter.first) {
      values->emplace_back(iter.first);
      values->emplace_back(iter.second->ToString());
    }
  }
}

Status Config::Set(Server *svr, std::string key, const std::string &value) {
  key = Util::ToLower(key);
  auto iter = fields_.find(key);
  if (iter == fields_.end() || iter->second->readonly) {
    return Status(Status::NotOK, "Unsupported CONFIG parameter: "+key);
  }
  auto& field = iter->second;
  if (field->validate) {
    auto s = field->validate(key, value);
    if (!s.IsOK()) return s;
  }
  auto s = field->Set(value);
  if (!s.IsOK()) return s;
  if (field->callback) {
    return field->callback(svr, key, value);
  }
  return Status::OK();
}

Status Config::Rewrite() {
  if (path_.empty()) {
    return Status(Status::NotOK, "the server is running without a config file");
  }
  std::vector<std::string> lines;
  std::map<std::string, std::string> new_config;
  for (const auto &iter : fields_) {
    if (iter.first == "rename-command") {
      // We should NOT overwrite the rename command since it cannot be rewritten in-flight,
      // so skip it here to avoid rewriting it as new item.
      continue;
    }
    new_config[iter.first] = iter.second->ToString();
  }

  std::string namespacePrefix = "namespace.";
  for (const auto &iter : tokens) {
    new_config[namespacePrefix + iter.second] = iter.first;
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
      if (Util::HasPrefix(kv.first, namespacePrefix)) {
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

  std::ostringstream string_stream;
  for (const auto &line : lines) {
    string_stream << line << "\n";
  }
  for (const auto &remain : new_config) {
    if (remain.second.empty()) continue;
    string_stream << remain.first << " " << remain.second << "\n";
  }
  std::string tmp_path = path_+".tmp";
  remove(tmp_path.data());
  std::ofstream output_file(tmp_path, std::ios::out);
  output_file.write(string_stream.str().c_str(), string_stream.str().size());
  output_file.close();
  if (rename(tmp_path.data(), path_.data()) < 0) {
    return Status(Status::NotOK, std::string("rename file encounter error: ")+strerror(errno));
  }
  return Status::OK();
}

Status Config::GetNamespace(const std::string &ns, std::string *token) {
  token->clear();
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      *token = iter.first;
      return Status::OK();
    }
  }
  return Status(Status::NotFound);
}

Status Config::SetNamespace(const std::string &ns, const std::string &token) {
  if (ns == kDefaultNamespace) {
    return Status(Status::NotOK, "forbidden to update the default namespace");
  }
  if (tokens.find(token) != tokens.end()) {
    return Status(Status::NotOK, "the token has already exists");
  }

  if (token == requirepass || token == masterauth) {
    return Status(Status::NotOK, "the token is duplicated with requirepass or masterauth");
  }

  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      tokens.erase(iter.first);
      tokens[token] = ns;
      auto s = Rewrite();
      if (!s.IsOK()) {
        // Need to roll back the old token if fails to rewrite the config
        tokens.erase(token);
        tokens[iter.first] = ns;
      }
      return s;
    }
  }
  return Status(Status::NotOK, "the namespace was not found");
}

Status Config::AddNamespace(const std::string &ns, const std::string &token) {
  if (requirepass.empty()) {
    return Status(Status::NotOK, "forbidden to add namespace when requirepass was empty");
  }
  if (cluster_enabled) {
    return Status(Status::NotOK, "forbidden to add namespace when cluster mode was enabled");
  }
  if (ns == kDefaultNamespace) {
    return Status(Status::NotOK, "forbidden to add the default namespace");
  }
  auto s = isNamespaceLegal(ns);
  if (!s.IsOK()) return s;
  if (tokens.find(token) != tokens.end()) {
    return Status(Status::NotOK, "the token has already exists");
  }

  if (token == requirepass || token == masterauth) {
    return Status(Status::NotOK, "the token is duplicated with requirepass or masterauth");
  }

  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      return Status(Status::NotOK, "the namespace has already exists");
    }
  }
  tokens[token] = ns;

  s = Rewrite();
  if (!s.IsOK()) {
    tokens.erase(token);
  }
  return s;
}

Status Config::DelNamespace(const std::string &ns) {
  if (ns == kDefaultNamespace) {
    return Status(Status::NotOK, "forbidden to delete the default namespace");
  }
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      tokens.erase(iter.first);
      auto s = Rewrite();
      if (!s.IsOK()) {
        tokens[iter.first] = ns;
      }
      return s;
    }
  }
  return Status(Status::NotOK, "the namespace was not found");
}

Status Config::isNamespaceLegal(const std::string &ns) {
  if (ns.size() > UINT8_MAX) {
    return Status(Status::NotOK, std::string("size exceed limit ") + std::to_string(UINT8_MAX));
  }
  char last_char = ns.back();
  if (last_char == std::numeric_limits<char>::max()) {
    return Status(Status::NotOK, std::string("namespace contain illegal letter"));
  }
  return Status::OK();
}
