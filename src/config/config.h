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

#include <rocksdb/options.h>
#include <sys/resource.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "config_type.h"
#include "cron.h"
#include "status.h"

// forward declaration
class Server;
namespace Engine {
class Storage;
}

constexpr const uint32_t PORT_LIMIT = 65535;

enum SupervisedMode { kSupervisedNone = 0, kSupervisedAutoDetect, kSupervisedSystemd, kSupervisedUpStart };

constexpr const char *TLS_AUTH_CLIENTS_NO = "no";
constexpr const char *TLS_AUTH_CLIENTS_OPTIONAL = "optional";

constexpr const size_t KiB = 1024L;
constexpr const size_t MiB = 1024L * KiB;
constexpr const size_t GiB = 1024L * MiB;
constexpr const uint32_t kDefaultPort = 6666;

constexpr const char *kDefaultNamespace = "__namespace";

struct CompactionCheckerRange {
 public:
  int start_;
  int stop_;

  bool Enabled() const { return start_ != -1 || stop_ != -1; }
};

struct CLIOptions {
  std::string conf_file_;
  std::vector<std::pair<std::string, std::string>> cli_options_;

  CLIOptions() = default;
  explicit CLIOptions(std::string_view file) : conf_file_(file) {}
};

struct Config {
 public:
  Config();
  ~Config() = default;
  uint32_t port_ = 0;
  uint32_t tls_port_ = 0;
  std::string tls_cert_file_;
  std::string tls_key_file_;
  std::string tls_key_file_pass_;
  std::string tls_ca_cert_file_;
  std::string tls_ca_cert_dir_;
  std::string tls_auth_clients_;
  bool tls_prefer_server_ciphers_ = false;
  std::string tls_ciphers_;
  std::string tls_ciphersuites_;
  std::string tls_protocols_;
  bool tls_session_caching_ = true;
  int tls_session_cache_size_ = 1024 * 20;
  int tls_session_cache_timeout_ = 300;
  int workers_ = 0;
  int timeout_ = 0;
  int log_level_ = 0;
  int backlog_ = 511;
  int maxclients_ = 10000;
  int max_backup_to_keep_ = 1;
  int max_backup_keep_hours_ = 24;
  int slowlog_log_slower_than_ = 100000;
  int slowlog_max_len_ = 128;
  bool daemonize_ = false;
  int supervised_mode_ = kSupervisedNone;
  bool slave_readonly_ = true;
  bool slave_serve_stale_data_ = true;
  bool slave_empty_db_before_fullsync_ = false;
  int slave_priority_ = 100;
  int max_db_size_ = 0;
  int max_replication_mb_ = 0;
  int max_io_mb_ = 0;
  int max_bitmap_to_string_mb_ = 16;
  bool master_use_repl_port_ = false;
  bool purge_backup_on_fullsync_ = false;
  bool auto_resize_block_and_sst_ = true;
  int fullsync_recv_file_delay_ = 0;
  bool use_rsid_psync_ = false;
  std::vector<std::string> binds_;
  std::string dir_;
  std::string db_dir_;
  std::string backup_dir_;  // GUARD_BY(backup_mu_)
  std::string backup_sync_dir_;
  std::string checkpoint_dir_;
  std::string sync_checkpoint_dir_;
  std::string log_dir_;
  std::string pidfile_;
  std::string db_name_;
  std::string masterauth_;
  std::string requirepass_;
  std::string master_host_;
  std::string unixsocket_;
  int unixsocketperm_ = 0777;
  uint32_t master_port_ = 0;
  Cron compact_cron_;
  Cron bgsave_cron_;
  CompactionCheckerRange compaction_checker_range_{-1, -1};
  int64_t force_compact_file_age_;
  int force_compact_file_min_deleted_percentage_;
  std::map<std::string, std::string> tokens_;
  std::string replica_announce_ip_;
  uint32_t replica_announce_port_ = 0;

  bool persist_cluster_nodes_enabled_ = true;
  bool slot_id_encoded_ = false;
  bool cluster_enabled_ = false;
  int migrate_speed_;
  int pipeline_size_;
  int sequence_gap_;

  int log_retention_days_;
  // profiling
  int profiling_sample_ratio_ = 0;
  int profiling_sample_record_threshold_ms_ = 0;
  int profiling_sample_record_max_len_ = 128;
  std::set<std::string> profiling_sample_commands_;
  bool profiling_sample_all_commands_ = false;

  struct RocksDB {
    int block_size_;
    bool cache_index_and_filter_blocks_;
    int metadata_block_cache_size_;
    int subkey_block_cache_size_;
    bool share_metadata_and_subkey_block_cache_;
    int row_cache_size_;
    int max_open_files_;
    int write_buffer_size_;
    int max_write_buffer_number_;
    int max_background_compactions_;
    int max_background_flushes_;
    int max_sub_compactions_;
    int stats_dump_period_sec_;
    bool enable_pipelined_write_;
    int64_t delayed_write_rate_;
    int compaction_readahead_size_;
    int target_file_size_base_;
    int wal_ttl_seconds_;
    int wal_size_limit_mb_;
    int max_total_wal_size_;
    int level0_slowdown_writes_trigger_;
    int level0_stop_writes_trigger_;
    int level0_file_num_compaction_trigger_;
    int compression_;
    bool disable_auto_compactions_;
    bool enable_blob_files_;
    int min_blob_size_;
    int blob_file_size_;
    bool enable_blob_garbage_collection_;
    int blob_garbage_collection_age_cutoff_;
    int max_bytes_for_level_base_;
    int max_bytes_for_level_multiplier_;
    bool level_compaction_dynamic_level_bytes_;
    int max_background_jobs_;

    struct WriteOptions {
      bool sync_;
      bool disable_wal_;
      bool no_slowdown_;
      bool low_pri_;
      bool memtable_insert_hint_per_batch_;
    } write_options_;

    struct ReadOptions {
      bool async_io_;
    } read_options_;
  } rocks_db_;

  mutable std::mutex backup_mu_;

  std::string NodesFilePath() const;
  Status Rewrite();
  Status Load(const CLIOptions &path);
  void Get(const std::string &key, std::vector<std::string> *values);
  Status Set(Server *svr, std::string key, const std::string &value);
  void SetMaster(const std::string &host, uint32_t port);
  void ClearMaster();
  Status GetNamespace(const std::string &ns, std::string *token);
  Status AddNamespace(const std::string &ns, const std::string &token);
  Status SetNamespace(const std::string &ns, const std::string &token);
  Status DelNamespace(const std::string &ns);

 private:
  std::string path_;
  std::string binds_str_;
  std::string slaveof_;
  std::string compact_cron_str_;
  std::string bgsave_cron_str_;
  std::string compaction_checker_range_str_;
  std::string profiling_sample_commands_str_;
  std::map<std::string, std::unique_ptr<ConfigField>> fields_;
  std::vector<std::string> rename_command_;

  void initFieldValidator();
  void initFieldCallback();
  Status parseConfigFromPair(const std::pair<std::string, std::string> &input, int line_number);
  Status parseConfigFromString(const std::string &input, int line_number);
  Status finish();
  static Status isNamespaceLegal(const std::string &ns);
};
