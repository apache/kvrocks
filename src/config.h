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
#include <sys/resource.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <rocksdb/options.h>

#include "config_type.h"
#include "status.h"
#include "cron.h"

// forward declaration
class Server;
namespace Engine {
class Storage;
}

#define SUPERVISED_NONE 0
#define SUPERVISED_AUTODETECT 1
#define SUPERVISED_SYSTEMD 2
#define SUPERVISED_UPSTART 3

#define TLS_AUTH_CLIENTS_NO "no"
#define TLS_AUTH_CLIENTS_OPTIONAL "optional"

const size_t KiB = 1024L;
const size_t MiB = 1024L * KiB;
const size_t GiB = 1024L * MiB;
const int kDefaultPort = 6666;

extern const char *kDefaultNamespace;

struct CompactionCheckerRange {
 public:
  int Start;
  int Stop;

  bool Enabled() {
    return Start != -1 || Stop != -1;
  }
};

struct Config{
 public:
  Config();
  ~Config() = default;
  int port = 0;
  int tls_port = 0;
  std::string tls_cert_file;
  std::string tls_key_file;
  std::string tls_key_file_pass;
  std::string tls_ca_cert_file;
  std::string tls_ca_cert_dir;
  std::string tls_auth_clients;
  bool tls_prefer_server_ciphers = false;
  std::string tls_ciphers;
  std::string tls_ciphersuites;
  std::string tls_protocols;
  bool tls_session_caching = true;
  int tls_session_cache_size = 1024 * 20;
  int tls_session_cache_timeout = 300;
  int workers = 0;
  int timeout = 0;
  int loglevel = 0;
  int backlog = 511;
  int maxclients = 10000;
  int max_backup_to_keep = 1;
  int max_backup_keep_hours = 24;
  int slowlog_log_slower_than = 100000;
  int slowlog_max_len = 128;
  bool daemonize = false;
  int supervised_mode = SUPERVISED_NONE;
  bool slave_readonly = true;
  bool slave_serve_stale_data = true;
  bool slave_empty_db_before_fullsync = false;
  int slave_priority = 100;
  int max_db_size = 0;
  int max_replication_mb = 0;
  int max_io_mb = 0;
  int max_bitmap_to_string_mb = 16;
  bool master_use_repl_port = false;
  bool purge_backup_on_fullsync = false;
  bool auto_resize_block_and_sst = true;
  int fullsync_recv_file_delay = 0;
  bool use_rsid_psync = false;
  std::vector<std::string> binds;
  std::string dir;
  std::string db_dir;
  std::string backup_dir;
  std::string backup_sync_dir;
  std::string checkpoint_dir;
  std::string sync_checkpoint_dir;
  std::string log_dir;
  std::string pidfile;
  std::string db_name;
  std::string masterauth;
  std::string requirepass;
  std::string master_host;
  std::string unixsocket;
  int unixsocketperm = 0777;
  int master_port = 0;
  Cron compact_cron;
  Cron bgsave_cron;
  CompactionCheckerRange compaction_checker_range{-1, -1};
  std::map<std::string, std::string> tokens;

  bool slot_id_encoded = false;
  bool cluster_enabled = false;
  int migrate_speed;
  int pipeline_size;
  int sequence_gap;

  // profiling
  int profiling_sample_ratio = 0;
  int profiling_sample_record_threshold_ms = 0;
  int profiling_sample_record_max_len = 128;
  std::set<std::string> profiling_sample_commands;
  bool profiling_sample_all_commands = false;

  struct RocksDB {
    int block_size;
    bool cache_index_and_filter_blocks;
    int metadata_block_cache_size;
    int subkey_block_cache_size;
    bool share_metadata_and_subkey_block_cache;
    int row_cache_size;
    int max_open_files;
    int write_buffer_size;
    int max_write_buffer_number;
    int max_background_compactions;
    int max_background_flushes;
    int max_sub_compactions;
    int stats_dump_period_sec;
    bool enable_pipelined_write;
    int64_t delayed_write_rate;
    int compaction_readahead_size;
    int target_file_size_base;
    int WAL_ttl_seconds;
    int WAL_size_limit_MB;
    int max_total_wal_size;
    int level0_slowdown_writes_trigger;
    int level0_stop_writes_trigger;
    int level0_file_num_compaction_trigger;
    int compression;
    bool disable_auto_compactions;
    bool enable_blob_files;
    int min_blob_size;
    int blob_file_size;
    bool enable_blob_garbage_collection;
    int blob_garbage_collection_age_cutoff;
    int max_bytes_for_level_base;
    int max_bytes_for_level_multiplier;
    bool level_compaction_dynamic_level_bytes;

    struct WriteOptions {
      bool sync;
      bool disable_WAL;
      bool no_slowdown;
      bool low_pri;
      bool memtable_insert_hint_per_batch;
    } write_options;
  } RocksDB;

 public:
  Status Rewrite();
  Status Load(const std::string &path);
  void Get(std::string key, std::vector<std::string> *values);
  Status Set(Server *svr, std::string key, const std::string &value);
  void SetMaster(const std::string &host, int port);
  void ClearMaster();
  Status GetNamespace(const std::string &ns, std::string *token);
  Status AddNamespace(const std::string &ns, const std::string &token);
  Status SetNamespace(const std::string &ns, const std::string &token);
  Status DelNamespace(const std::string &ns);

 private:
  std::string path_;
  std::string binds_;
  std::string slaveof_;
  std::string compact_cron_;
  std::string bgsave_cron_;
  std::string compaction_checker_range_;
  std::string profiling_sample_commands_;
  std::map<std::string, std::unique_ptr<ConfigField>> fields_;
  std::string rename_command_;

  void initFieldValidator();
  void initFieldCallback();
  Status parseConfigFromString(const std::string &input, int line_number);
  Status finish();
  Status isNamespaceLegal(const std::string &ns);
};
