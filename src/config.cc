#include <fcntl.h>
#include <string.h>
#include <strings.h>
#include <glog/logging.h>
#include <rocksdb/env.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>
#include <limits>

#include "config.h"
#include "util.h"
#include "status.h"
#include "cron.h"
#include "server.h"
#include "log_collector.h"

const char *kDefaultNamespace = "__namespace";
static const char *kLogLevels[] = {"info", "warning", "error", "fatal"};
static const size_t kNumLogLevel = sizeof(kLogLevels)/ sizeof(kLogLevels[0]);
static const char *kCompressionType[] = {"no", "snappy"};
static const size_t kNumCompressionType = sizeof(kCompressionType) / sizeof(kCompressionType[0]);

typedef struct configEnum {
  const char *name;
  const int val;
} configEnum;

configEnum supervised_mode_enum[] = {
    {"no", SUPERVISED_NONE},
    {"auto", SUPERVISED_AUTODETECT},
    {"upstart", SUPERVISED_UPSTART},
    {"systemd", SUPERVISED_SYSTEMD},
    {nullptr, 0}
};

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

void Config::incrOpenFilesLimit(rlim_t maxfiles) {
  struct rlimit limit;

  rlim_t old_limit, best_limit = maxfiles, decr_step = 16;
  if (getrlimit(RLIMIT_NOFILE, &limit) < 0 || best_limit <= limit.rlim_cur) {
    return;
  }
  old_limit = limit.rlim_cur;
  while (best_limit > old_limit) {
    limit.rlim_cur = best_limit;
    limit.rlim_max = best_limit;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) break;
    /* We failed to set file limit to 'bestlimit'. Try with a
     * smaller limit decrementing by a few FDs per iteration. */
    if (best_limit < decr_step) break;
    best_limit -= decr_step;
  }
}

void Config::array2String(const std::vector<std::string> &array,
                          const std::string &delim, std::string *output) {
  output->clear();
  for (size_t i = 0; i < array.size(); i++) {
    output->append(array[i]);
    if (i != array.size()-1) output->append(delim);
  }
}

int Config::yesnotoi(std::string input) {
  if (strcasecmp(input.data(), "yes") == 0) {
    return 1;
  } else if (strcasecmp(input.data(), "no") == 0) {
    return 0;
  }
  return -1;
}

Status Config::parseRocksdbOption(const std::string &key, std::string value) {
  if (key == "compression") {
    for (size_t i = 0; i < kNumCompressionType; i++) {
      if (Util::ToLower(value) == kCompressionType[i]) {
        rocksdb_options.compression = static_cast<rocksdb::CompressionType >(i);
        break;
      }
    }
  } else if (key == "enable_pipelined_write")  {
    rocksdb_options.enable_pipelined_write = value == "yes";
  } else if (key == "cache_index_and_filter_blocks")  {
    rocksdb_options.cache_index_and_filter_blocks = value == "yes";
  } else {
    return parseRocksdbIntOption(key, value);
  }
  return Status::OK();
}

Status Config::parseRocksdbIntOption(std::string key, std::string value) {
  int64_t n;
  auto s = Util::StringToNum(value, &n);
  if (key == "max_open_files") {
    rocksdb_options.max_open_files = static_cast<int>(n);
  }  else if (key == "block_size") {
    rocksdb_options.block_size = static_cast<size_t>(n);
  } else if (!strncasecmp(key.data(), "write_buffer_size" , strlen("write_buffer_size"))) {
    rocksdb_options.write_buffer_size = static_cast<size_t>(n) * MiB;
  }  else if (key == "max_write_buffer_number") {
    rocksdb_options.max_write_buffer_number = static_cast<int>(n);
  }  else if (key == "write_buffer_size") {
    rocksdb_options.write_buffer_size = static_cast<uint64_t>(n);
  }  else if (key == "target_file_size_base") {
    rocksdb_options.target_file_size_base = static_cast<uint64_t>(n);
  }  else if (key == "max_background_compactions") {
    rocksdb_options.max_background_compactions = static_cast<int>(n);
  }  else if (key == "max_background_flushes") {
    rocksdb_options.max_background_flushes = static_cast<int>(n);
  }  else if (key == "max_sub_compactions") {
    rocksdb_options.max_sub_compactions = static_cast<uint32_t>(n);
  } else if (key == "metadata_block_cache_size") {
    rocksdb_options.metadata_block_cache_size = static_cast<size_t>(n) * MiB;
  } else if (key == "subkey_block_cache_size") {
    rocksdb_options.subkey_block_cache_size = static_cast<size_t>(n) * MiB;
  } else if (key == "delayed_write_rate") {
    rocksdb_options.delayed_write_rate = static_cast<uint64_t>(n);
  } else if (key == "compaction_readahead_size") {
    rocksdb_options.compaction_readahead_size = static_cast<size_t>(n);
  } else if (key == "wal_ttl_seconds") {
    rocksdb_options.WAL_ttl_seconds = static_cast<uint64_t>(n);
  } else if (key == "wal_size_limit_mb") {
    rocksdb_options.WAL_size_limit_MB = static_cast<uint64_t>(n);
  } else if (key == "level0_slowdown_writes_trigger") {
    rocksdb_options.level0_slowdown_writes_trigger = static_cast<int>(n);
    rocksdb_options.level0_stop_writes_trigger = static_cast<int>(n*2);
  } else {
    return Status(Status::NotOK, "Bad directive or wrong number of arguments");
  }
  return Status::OK();
}

Status Config::parseConfigFromString(std::string input) {
  std::vector<std::string> args;
  Util::Split(input, " \t\r\n", &args);
  // omit empty line and comment
  if (args.empty() || args[0].front() == '#') return Status::OK();

  args[0] = Util::ToLower(args[0]);
  size_t size = args.size();
  if (size == 2 && args[0] == "port") {
    port = std::atoi(args[1].c_str());
    repl_port = port + 1;
  } else if (size == 2 && args[0] == "timeout") {
    timeout = std::atoi(args[1].c_str());
  } else if (size == 2 && args[0] == "workers") {
    workers = std::atoi(args[1].c_str());
    if (workers < 1 || workers > 1024) {
      return Status(Status::NotOK, "too many worker threads");
    }
  } else if (size == 2 && args[0] == "repl-workers") {
    repl_workers = std::atoi(args[1].c_str());
    if (workers < 1 || workers > 1024) {
      return Status(Status::NotOK, "too many replication worker threads");
    }
  } else if (size >= 2 && args[0] == "bind") {
    binds.clear();
    for (unsigned i = 1; i < args.size(); i++) {
      binds.emplace_back(args[i]);
    }
  } else if (size >= 2 && args[0] == "repl-bind") {
    repl_binds.clear();
    for (unsigned i = 1; i < args.size(); i++) {
      repl_binds.emplace_back(args[i]);
    }
  } else if (size == 2 && args[0] == "daemonize") {
    int i;
    if ((i = yesnotoi(args[1])) == -1) {
      return Status(Status::NotOK, "argument must be 'yes' or 'no'");
    }
    daemonize = (i == 1);
  } else if (size == 2 && args[0] == "slave-read-only") {
    int i;
    if ((i = yesnotoi(args[1])) == -1) {
      return Status(Status::NotOK, "argument must be 'yes' or 'no'");
    }
    slave_readonly = (i == 1);
  } else if (size == 2 && args[0] == "slave-serve-stale-data") {
    int i;
    if ((i = yesnotoi(args[1])) == -1) {
      return Status(Status::NotOK, "argument must be 'yes' or 'no'");
    }
    slave_serve_stale_data = (i == 1);
  } else if (size == 2 && args[0] == "slave-priority") {
    slave_priority = std::atoi(args[1].c_str());
  } else if (size == 2 && args[0] == "tcp-backlog") {
    backlog = std::atoi(args[1].c_str());
  } else if (size == 2 && args[0] == "dir") {
    dir = args[1];
    db_dir = dir + "/db";
    pidfile = dir + "/kvrocks.pid";
  } else if (size == 2 && args[0] == "backup-dir") {
    backup_dir = args[1];
  } else if (size == 2 && args[0] == "maxclients") {
    maxclients = std::atoi(args[1].c_str());
    if (maxclients > 0) incrOpenFilesLimit(static_cast<rlim_t >(maxclients));
  } else if (size == 2 && args[0] == "db-name") {
    db_name = args[1];
  } else if (size == 2 && args[0] == "masterauth") {
    masterauth = args[1];
  } else if (size == 2 && args[0] == "max-backup-to-keep") {
    max_backup_to_keep = static_cast<uint32_t>(std::atoi(args[1].c_str()));
  } else if (size == 2 && args[0] == "max-backup-keep-hours") {
    max_backup_keep_hours = static_cast<uint32_t>(std::atoi(args[1].c_str()));
  } else if (size == 2 && args[0] == "codis-enabled") {
    int i;
    if ((i = yesnotoi(args[1])) == -1) {
      return Status(Status::NotOK, "argument must be 'yes' or 'no'");
    }
    codis_enabled = (i == 1);
  } else if (size == 2 && args[0] == "requirepass") {
    requirepass = args[1];
  } else if (size == 2 && args[0] == "pidfile") {
    pidfile = args[1];
  } else if (size == 2 && args[0] == "loglevel") {
    for (size_t i = 0; i < kNumLogLevel; i++) {
      if (Util::ToLower(args[1]) == kLogLevels[i]) {
        loglevel = static_cast<int>(i);
        break;
      }
    }
  } else if (size == 3 && args[0] == "slaveof") {
    if (args[1] != "no" && args[2] != "one") {
      master_host = args[1];
      master_port = std::atoi(args[2].c_str());
      if (master_port <= 0 || master_port >= 65535) {
        return Status(Status::NotOK, "master port range should be between 0 and 65535");
      }
    }
  } else if (size == 2 && args[0] == "max-db-size") {
    max_db_size = static_cast<uint32_t>(std::atoi(args[1].c_str()));
  } else if (size == 2 && args[0] == "max-replication-mb") {
    max_replication_mb = static_cast<uint64_t>(std::atoi(args[1].c_str()));
  } else if (size == 2 && args[0] == "max-io-mb") {
    max_io_mb = static_cast<uint64_t>(std::atoi(args[1].c_str()));
  } else if (size >= 2 && args[0] == "compact-cron") {
    args.erase(args.begin());
    Status s = compact_cron.SetScheduleTime(args);
    if (!s.IsOK()) {
      return Status(Status::NotOK, "compact-cron time expression format error : "+s.Msg());
    }
  } else if (size >=2 && args[0] == "bgsave-cron") {
    args.erase(args.begin());
    Status s = bgsave_cron.SetScheduleTime(args);
    if (!s.IsOK()) {
      return Status(Status::NotOK, "bgsave-cron time expression format error : " + s.Msg());
    }
  } else if (size == 2 && args[0] == "profiling-sample-ratio") {
    profiling_sample_ratio = std::atoi(args[1].c_str());
    if (profiling_sample_ratio < 0 || profiling_sample_ratio > 100) {
      return Status(Status::NotOK, "profiling_sample_ratio value should between 0 and 100");
    }
  } else if (size == 2 && args[0] == "profiling-sample-record-max-len") {
    profiling_sample_record_max_len = std::atoi(args[1].c_str());
  } else if (size == 2 && args[0] == "profiling-sample-record-threshold-ms") {
    profiling_sample_record_threshold_ms = std::atoi(args[1].c_str());
  } else if (size == 2 && args[0] == "profiling-sample-commands") {
    std::vector<std::string> cmds;
    Util::Split(args[1], ",", &cmds);
    for (auto const &cmd : cmds) {
      if (cmd == "*") {
        profiling_sample_all_commands = true;
        profiling_sample_commands.clear();
        break;
      }
      if (!Redis::IsCommandExists(cmd)) {
        return Status(Status::NotOK, "invalid command: "+cmd+" in profiling-sample-commands");
      }
      profiling_sample_commands.insert(cmd);
    }
  } else if (size == 2 && !strncasecmp(args[0].data(), "rocksdb.", 8)) {
    return parseRocksdbOption(args[0].substr(8, args[0].size() - 8), args[1]);
  } else if (size == 2 && !strncasecmp(args[0].data(), "namespace.", 10)) {
    std::string ns = args[0].substr(10, args.size()-10);
    auto s = isNamespaceLegal(ns);
    if (!s.IsOK()) {
      return s;
    }
    tokens[args[1]] = ns;
  } else if (size == 2 && !strcasecmp(args[0].data(), "slowlog-log-slower-than")) {
    slowlog_log_slower_than = std::atoll(args[1].c_str());
  } else if (size == 2 && !strcasecmp(args[0].data(), "slowlog-max-len")) {
    slowlog_max_len = std::atoi(args[1].c_str());
  } else if (size == 2 && args[0] == "supervised") {
    supervised_mode = configEnumGetValue(supervised_mode_enum, args[1].c_str());
    if (supervised_mode == INT_MIN) {
      return Status(Status::NotOK, "Invalid option for 'supervised'."
                                   " Allowed values: 'upstart', 'systemd', 'auto', 'no'");
    }
  } else {
    return Status(Status::NotOK, "Bad directive or wrong number of arguments");
  }
  return Status::OK();
}

Status Config::Load(std::string path) {
  path_ = std::move(path);
  std::ifstream file(path_);
  if (!file.is_open()) {
    return Status(Status::NotOK, strerror(errno));
  }

  std::string line;
  int line_num = 1;
  while (!file.eof()) {
    std::getline(file, line);
    Status s = parseConfigFromString(line);
    if (!s.IsOK()) {
      file.close();
      return Status(Status::NotOK, "at line: #L" + std::to_string(line_num) + ", err: " + s.Msg());
    }
    line_num++;
  }
  if (backup_dir.empty()) {  // backup-dir was not assigned in config file
    backup_dir = dir+"/backup";
  }
  if (!tokens.empty()) {
    if (requirepass.empty()) {
      file.close();
      return Status(Status::NotOK, "requirepass was required when namespace isn't empty");
    }
    if (codis_enabled) {
      file.close();
      return Status(Status::NotOK, "namespace wasn't allowed when the codis mode enabled");
    }
  }
  auto s = rocksdb::Env::Default()->CreateDirIfMissing(dir);
  if (!s.ok()) {
    file.close();
    return Status(Status::NotOK, s.ToString());
  }
  s = rocksdb::Env::Default()->CreateDirIfMissing(backup_dir);
  if (!s.ok()) {
    file.close();
    return Status(Status::NotOK, s.ToString());
  }
  file.close();
  return Status::OK();
}

void Config::Get(std::string key, std::vector<std::string> *values) {
  key = Util::ToLower(key);
  values->clear();
  bool is_all = key == "*";

#define PUSH_IF_MATCH(name, value) do { \
  if ((is_all) || (key) == (name)) { \
    values->emplace_back((name)); \
    values->emplace_back((value)); \
  } \
} while (0);

  std::string master_str;
  if (!master_host.empty()) {
    master_str = master_host+" "+ std::to_string(master_port);
  }
  std::string binds_str;
  array2String(binds, ",", &binds_str);
  std::string sample_commands_str;
  if (profiling_sample_all_commands) {
    sample_commands_str = "*";
  } else {
    for (const auto &cmd : profiling_sample_commands) {
      sample_commands_str.append(cmd);
      sample_commands_str.append(",");
    }
    if (!sample_commands_str.empty()) sample_commands_str.pop_back();
  }
  PUSH_IF_MATCH("dir", dir);
  PUSH_IF_MATCH("db-dir", db_dir);
  PUSH_IF_MATCH("backup-dir", backup_dir);
  PUSH_IF_MATCH("port", std::to_string(port));
  PUSH_IF_MATCH("workers", std::to_string(workers));
  PUSH_IF_MATCH("timeout", std::to_string(timeout));
  PUSH_IF_MATCH("tcp-backlog", std::to_string(backlog));
  PUSH_IF_MATCH("daemonize", (daemonize ? "yes" : "no"));
  PUSH_IF_MATCH("supervised", configEnumGetName(supervised_mode_enum, supervised_mode));
  PUSH_IF_MATCH("maxclients", std::to_string(maxclients));
  PUSH_IF_MATCH("slave-read-only", (slave_readonly ? "yes" : "no"));
  PUSH_IF_MATCH("slave-serve-stale-data", (slave_serve_stale_data ? "yes" : "no"));
  PUSH_IF_MATCH("slave-priority", std::to_string(slave_priority));
  PUSH_IF_MATCH("max-backup-to-keep", std::to_string(max_backup_to_keep));
  PUSH_IF_MATCH("max-backup-keep-hours", std::to_string(max_backup_keep_hours));
  PUSH_IF_MATCH("codis-enabled", (codis_enabled ? "yes" : "no"));
  PUSH_IF_MATCH("compact-cron", compact_cron.ToString());
  PUSH_IF_MATCH("bgsave-cron", bgsave_cron.ToString());
  PUSH_IF_MATCH("loglevel", kLogLevels[loglevel]);
  PUSH_IF_MATCH("requirepass", requirepass);
  PUSH_IF_MATCH("masterauth", masterauth);
  PUSH_IF_MATCH("slaveof", master_str);
  PUSH_IF_MATCH("pidfile", pidfile);
  PUSH_IF_MATCH("db-name", db_name);
  PUSH_IF_MATCH("binds", binds_str);
  PUSH_IF_MATCH("max-io-mb", std::to_string(max_io_mb));
  PUSH_IF_MATCH("max-db-size", std::to_string(max_db_size));
  PUSH_IF_MATCH("slowlog-max-len", std::to_string(slowlog_max_len));
  PUSH_IF_MATCH("max-replication-mb", std::to_string(max_replication_mb));
  PUSH_IF_MATCH("profiling-sample-commands", sample_commands_str);
  PUSH_IF_MATCH("profiling-sample-ratio", std::to_string(profiling_sample_ratio));
  PUSH_IF_MATCH("profiling-sample-record-max-len", std::to_string(profiling_sample_record_max_len));
  PUSH_IF_MATCH("profiling-sample-record-threshold-ms", std::to_string(profiling_sample_record_threshold_ms));
  PUSH_IF_MATCH("slowlog-log-slower-than", std::to_string(slowlog_log_slower_than));
  PUSH_IF_MATCH("rocksdb.max_open_files", std::to_string(rocksdb_options.max_open_files));
  PUSH_IF_MATCH("rocksdb.block_size", std::to_string(rocksdb_options.block_size));
  PUSH_IF_MATCH("rocksdb.write_buffer_size", std::to_string(rocksdb_options.write_buffer_size/MiB));
  PUSH_IF_MATCH("rocksdb.max_write_buffer_number", std::to_string(rocksdb_options.max_write_buffer_number));
  PUSH_IF_MATCH("rocksdb.max_background_compactions", std::to_string(rocksdb_options.max_background_compactions));
  PUSH_IF_MATCH("rocksdb.metadata_block_cache_size", std::to_string(rocksdb_options.metadata_block_cache_size/MiB));
  PUSH_IF_MATCH("rocksdb.subkey_block_cache_size", std::to_string(rocksdb_options.subkey_block_cache_size/MiB));
  PUSH_IF_MATCH("rocksdb.compaction_readahead_size", std::to_string(rocksdb_options.compaction_readahead_size));
  PUSH_IF_MATCH("rocksdb.max_background_flushes", std::to_string(rocksdb_options.max_background_flushes));
  PUSH_IF_MATCH("rocksdb.enable_pipelined_write", (rocksdb_options.enable_pipelined_write ? "yes": "no"))
  PUSH_IF_MATCH("rocksdb.cache_index_and_filter_blocks", (rocksdb_options.cache_index_and_filter_blocks? "yes": "no"))
  PUSH_IF_MATCH("rocksdb.stats_dump_period_sec", std::to_string(rocksdb_options.stats_dump_period_sec));
  PUSH_IF_MATCH("rocksdb.max_sub_compactions", std::to_string(rocksdb_options.max_sub_compactions));
  PUSH_IF_MATCH("rocksdb.delayed_write_rate", std::to_string(rocksdb_options.delayed_write_rate));
  PUSH_IF_MATCH("rocksdb.wal_ttl_seconds", std::to_string(rocksdb_options.WAL_ttl_seconds));
  PUSH_IF_MATCH("rocksdb.wal_size_limit_mb", std::to_string(rocksdb_options.WAL_size_limit_MB));
  PUSH_IF_MATCH("rocksdb.target_file_size_base", std::to_string(rocksdb_options.target_file_size_base));
  PUSH_IF_MATCH("rocksdb.level0_slowdown_writes_trigger",
                std::to_string(rocksdb_options.level0_slowdown_writes_trigger));
  PUSH_IF_MATCH("rocksdb.level0_stop_writes_trigger", std::to_string(rocksdb_options.level0_stop_writes_trigger));
  PUSH_IF_MATCH("rocksdb.compression", kCompressionType[rocksdb_options.compression]);
}

Status Config::setRocksdbOption(Engine::Storage *storage, const std::string &key, const std::string &value) {
  int64_t i;
  bool is_cf_mutal_option = false;
  auto db = storage->GetDB();
  auto s = Util::StringToNum(value, &i, 0);
  if (!s.IsOK()) return s;
  if (key == "stats_dump_period_sec") {
    rocksdb_options.stats_dump_period_sec = static_cast<int>(i);
  } else if (key == "max_open_files") {
    rocksdb_options.max_open_files = static_cast<int>(i);
  } else if (key == "delayed_write_rate") {
    rocksdb_options.delayed_write_rate = static_cast<uint64_t>(i);
  } else if (key == "max_background_compactions") {
    rocksdb_options.max_background_compactions = static_cast<int>(i);
  } else if (key == "max_background_flushes") {
    rocksdb_options.max_background_flushes = static_cast<int>(i);
  } else if (key == "compaction_readahead_size") {
    rocksdb_options.compaction_readahead_size = static_cast<size_t>(i);
  } else if (key == "target_file_size_base") {
    is_cf_mutal_option = true;
    rocksdb_options.target_file_size_base = static_cast<uint64_t>(i);
  } else if (key == "write_buffer_size") {
    is_cf_mutal_option = true;
    rocksdb_options.write_buffer_size = static_cast<uint64_t>(i*MiB);
  } else if (key == "max_write_buffer_number") {
    is_cf_mutal_option = true;
    rocksdb_options.max_write_buffer_number =  static_cast<int>(i);
  } else if (key == "level0_slowdown_writes_trigger") {
    is_cf_mutal_option = true;
    rocksdb_options.level0_slowdown_writes_trigger = static_cast<int>(i);
    rocksdb_options.level0_stop_writes_trigger = static_cast<int>(i * 2);
  } else {
    return Status(Status::NotOK, "option can't be set in-flight");
  }
  rocksdb::Status r_status;
  if (!is_cf_mutal_option) {
    r_status = db->SetDBOptions({{key, value}});
  } else {
    auto cf_handles = storage->GetCFHandles();
    for (auto & cf_handle : cf_handles) {
      r_status = db->SetOptions(cf_handle, {{key, value}});
      if (!r_status.ok()) break;
    }
  }
  if (r_status.ok()) return Status::OK();
  return Status(Status::NotOK, r_status.ToString());
}

Status Config::Set(std::string key, const std::string &value, Server *svr) {
  key = Util::ToLower(key);
  if (key == "timeout") {
    timeout = std::atoi(value.c_str());
    return Status::OK();
  }
  if (key == "backup-dir") {
    auto s = rocksdb::Env::Default()->CreateDirIfMissing(value);
    if (!s.ok()) return Status(Status::NotOK, s.ToString());
    backup_dir = value;
    return Status::OK();
  }
  if (key == "maxclients") {
    maxclients = std::atoi(value.c_str());
    return Status::OK();
  }
  if (key == "max-backup-to-keep") {
    max_backup_to_keep = static_cast<uint32_t>(std::atoi(value.c_str()));
    return Status::OK();
  }
  if (key == "max-backup-keep-hours") {
    max_backup_keep_hours = static_cast<uint32_t>(std::atoi(value.c_str()));
    return Status::OK();
  }
  if (key == "masterauth") {
    masterauth = value;
    return Status::OK();
  }
  if (key == "requirepass") {
    if (requirepass.empty() && !tokens.empty()) {
      return Status(Status::NotOK, "don't clear the requirepass while the namespace wasn't empty");
    }
    requirepass = value;
    return Status::OK();
  }
  if (key == "slave-read-only") {
    int i;
    if ((i = yesnotoi(value)) == -1) {
      return Status(Status::NotOK, "argument must be 'yes' or 'no'");
    }
    slave_readonly = (i == 1);
    return Status::OK();
  }
  if (key == "slave-serve-stale-data") {
    int i;
    if ((i = yesnotoi(value)) == -1) {
      return Status(Status::NotOK, "argument must be 'yes' or 'no'");
    }
    slave_serve_stale_data = (i == 1);
    return Status::OK();
  }
  if (key == "slave-priority") {
    slave_priority = std::atoi(value.c_str());
    return Status::OK();
  }
  if (key == "loglevel") {
    for (size_t i = 0; i < kNumLogLevel; i++) {
      if (Util::ToLower(value) == kLogLevels[i]) {
        loglevel = static_cast<int>(i);
        break;
      }
    }
    return Status(Status::NotOK, "loglevel should be info,warning,error,fatal");
  }
  if (key == "compact-cron") {
    std::vector<std::string> args;
    Util::Split(value, " ", &args);
    return compact_cron.SetScheduleTime(args);
  }
  if (key == "bgsave-cron") {
    std::vector<std::string> args;
    Util::Split(value, " ", &args);
    return bgsave_cron.SetScheduleTime(args);
  }
  if (key == "slowlog-log-slower-than") {
    slowlog_log_slower_than = std::atoll(value.c_str());
    return Status::OK();
  }
  if (key == "slowlog-max-len") {
    slowlog_max_len = std::atoi(value.c_str());
    svr->GetSlowLog()->SetMaxEntries(slowlog_max_len);
    return Status::OK();
  }
  if (key == "max-db-size") {
    try {
      int32_t i = std::atoi(value.c_str());
      if (i < 0) {
        return Status(Status::RedisParseErr, "value should be >= 0");
      }
      max_db_size = static_cast<uint32_t>(i);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
    }
    svr->storage_->CheckDBSizeLimit();
    return Status::OK();
  }
  if (key == "max-replication-mb") {
    int64_t i;
    auto s = Util::StringToNum(value, &i, 0);
    if (!s.IsOK()) return s;
    svr->SetReplicationRateLimit(static_cast<uint64_t>(i));
    return Status::OK();
  }
  if (key == "max-io-mb") {
    int64_t i;
    auto s = Util::StringToNum(value, &i, 0);
    if (!s.IsOK()) return s;
    max_io_mb = i;
    svr->storage_->SetIORateLimit(static_cast<uint64_t>(i));
    return Status::OK();
  }
  if (key == "profiling-sample-ratio") {
    int64_t i;
    auto s = Util::StringToNum(value, &i, 0, 100);
    if (!s.IsOK()) return s;
    profiling_sample_ratio = static_cast<int>(i);
    return Status::OK();
  }
  if (key == "profiling-sample-record-threshold-ms") {
    int64_t i;
    auto s = Util::StringToNum(value, &i, 0, INT_MAX);
    if (!s.IsOK()) return s;
    profiling_sample_record_threshold_ms = static_cast<int>(i);
    return Status::OK();
  }
  if (key == "profiling-sample-record-max-len") {
    int64_t i;
    auto s = Util::StringToNum(value, &i, 0, INT_MAX);
    if (!s.IsOK()) return s;
    profiling_sample_record_max_len = static_cast<int>(i);
    svr->GetPerfLog()->SetMaxEntries(profiling_sample_record_max_len);
    return Status::OK();
  }
  if (key == "profiling-sample-commands") {
    std::vector<std::string> cmds;
    Util::Split(value, ",", &cmds);
    for (auto const &cmd : cmds) {
      if (!Redis::IsCommandExists(cmd) && cmd != "*") {
        return Status(Status::NotOK, "invalid command: "+cmd+" in profiling-sample-commands");
      }
    }
    profiling_sample_all_commands = false;
    profiling_sample_commands.clear();
    for (auto const &cmd : cmds) {
      if (cmd == "*") {
        profiling_sample_all_commands = true;
        profiling_sample_commands.clear();
        break;
      }
      profiling_sample_commands.insert(cmd);
    }
    return Status::OK();
  }
  if (!strncasecmp(key.c_str(), "rocksdb.", 8)) {
    return setRocksdbOption(svr->storage_, key.substr(8, key.size()-8), value);
  }
  return Status(Status::NotOK, "Unsupported CONFIG parameter");
}

Status Config::Rewrite() {
  std::string tmp_path = path_+".tmp";
  remove(tmp_path.data());
  std::ofstream output_file(tmp_path, std::ios::out);

  std::ostringstream string_stream;
#define WRITE_TO_FILE(key, value) do { \
  string_stream << (key) << " " << (value) <<  "\n"; \
} while (0)

  std::string binds_str, repl_binds_str, sample_commands_str;
  array2String(binds, ",", &binds_str);
  array2String(repl_binds, ",", &repl_binds_str);
  if (profiling_sample_all_commands) {
    sample_commands_str = "*";
  } else {
    for (const auto &cmd : profiling_sample_commands) {
      sample_commands_str.append(cmd);
      sample_commands_str.append(",");
    }
    if (!sample_commands_str.empty()) sample_commands_str.pop_back();
  }
  string_stream << "################################ GERNERAL #####################################\n";
  WRITE_TO_FILE("bind", binds_str);
  WRITE_TO_FILE("port", port);
  WRITE_TO_FILE("repl-bind", repl_binds_str);
  WRITE_TO_FILE("timeout", timeout);
  WRITE_TO_FILE("workers", workers);
  WRITE_TO_FILE("maxclients", maxclients);
  WRITE_TO_FILE("repl-workers", repl_workers);
  WRITE_TO_FILE("loglevel", kLogLevels[loglevel]);
  WRITE_TO_FILE("daemonize", (daemonize?"yes":"no"));
  WRITE_TO_FILE("supervised", (configEnumGetName(supervised_mode_enum, supervised_mode)));
  WRITE_TO_FILE("db-name", db_name);
  WRITE_TO_FILE("dir", dir);
  WRITE_TO_FILE("backup-dir", backup_dir);
  WRITE_TO_FILE("tcp-backlog", backlog);
  WRITE_TO_FILE("slave-read-only", (slave_readonly? "yes":"no"));
  WRITE_TO_FILE("slave-serve-stale-data", (slave_serve_stale_data? "yes":"no"));
  WRITE_TO_FILE("slave-priority", slave_priority);
  WRITE_TO_FILE("slowlog-max-len", slowlog_max_len);
  WRITE_TO_FILE("slowlog-log-slower-than", slowlog_log_slower_than);
  WRITE_TO_FILE("max-backup-to-keep", max_backup_to_keep);
  WRITE_TO_FILE("max-backup-keep-hours", max_backup_keep_hours);
  WRITE_TO_FILE("max-db-size", max_db_size);
  WRITE_TO_FILE("max-replication-mb", max_replication_mb);
  WRITE_TO_FILE("max-io-mb", max_io_mb);
  WRITE_TO_FILE("codis-enabled", (codis_enabled? "yes":"no"));
  if (!requirepass.empty()) WRITE_TO_FILE("requirepass", requirepass);
  if (!masterauth.empty()) WRITE_TO_FILE("masterauth", masterauth);
  if (!master_host.empty())  WRITE_TO_FILE("slaveof", master_host+" "+std::to_string(master_port));
  if (compact_cron.IsEnabled()) WRITE_TO_FILE("compact-cron", compact_cron.ToString());
  if (bgsave_cron.IsEnabled()) WRITE_TO_FILE("bgave-cron", bgsave_cron.ToString());
  WRITE_TO_FILE("profiling-sample-ratio", profiling_sample_ratio);
  if (!sample_commands_str.empty()) WRITE_TO_FILE("profiling-sample-commands", sample_commands_str);
  WRITE_TO_FILE("profiling-sample-record-max-len", profiling_sample_record_max_len);
  WRITE_TO_FILE("profiling-sample-record-threshold-ms", profiling_sample_record_threshold_ms);

  string_stream << "\n################################ ROCKSDB #####################################\n";
  WRITE_TO_FILE("rocksdb.max_open_files", rocksdb_options.max_open_files);
  WRITE_TO_FILE("rocksdb.block_size", rocksdb_options.block_size);
  WRITE_TO_FILE("rocksdb.write_buffer_size", rocksdb_options.write_buffer_size/MiB);
  WRITE_TO_FILE("rocksdb.max_write_buffer_number", rocksdb_options.max_write_buffer_number);
  WRITE_TO_FILE("rocksdb.max_background_compactions", rocksdb_options.max_background_compactions);
  WRITE_TO_FILE("rocksdb.metadata_block_cache_size", rocksdb_options.metadata_block_cache_size/MiB);
  WRITE_TO_FILE("rocksdb.subkey_block_cache_size", rocksdb_options.subkey_block_cache_size/MiB);
  WRITE_TO_FILE("rocksdb.max_background_flushes", rocksdb_options.max_background_flushes);
  WRITE_TO_FILE("rocksdb.max_sub_compactions", rocksdb_options.max_sub_compactions);
  WRITE_TO_FILE("rocksdb.compression", kCompressionType[rocksdb_options.compression]);
  WRITE_TO_FILE("rocksdb.enable_pipelined_write", (rocksdb_options.enable_pipelined_write ? "yes" : "no"));
  WRITE_TO_FILE("rocksdb.cache_index_and_filter_blocks", (rocksdb_options.cache_index_and_filter_blocks? "yes" : "no"));
  WRITE_TO_FILE("rocksdb.delayed_write_rate", rocksdb_options.delayed_write_rate);
  WRITE_TO_FILE("rocksdb.compaction_readahead_size", rocksdb_options.compaction_readahead_size);
  WRITE_TO_FILE("rocksdb.target_file_size_base", rocksdb_options.target_file_size_base);
  WRITE_TO_FILE("rocksdb.level0_slowdown_writes_trigger", rocksdb_options.level0_slowdown_writes_trigger);
  WRITE_TO_FILE("rocksdb.wal_ttl_seconds", rocksdb_options.WAL_ttl_seconds);
  WRITE_TO_FILE("rocksdb.wal_size_limit_mb", rocksdb_options.WAL_size_limit_MB);

  string_stream << "\n################################ Namespace #####################################\n";
  for (const auto &iter : tokens) {
    WRITE_TO_FILE("namespace."+iter.second, iter.first);
  }
  output_file.write(string_stream.str().c_str(), string_stream.str().size());
  output_file.close();
  if (rename(tmp_path.data(), path_.data()) < 0) {
    return Status(Status::NotOK, std::string("unable to rename config file, err: ")+strerror(errno));
  }
  return Status::OK();
}

void Config::GetNamespace(const std::string &ns, std::string *token) {
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      *token = iter.first;
    }
  }
}

Status Config::SetNamespace(const std::string &ns, const std::string &token) {
  if (ns == kDefaultNamespace) {
    return Status(Status::NotOK, "can't set the default namespace");
  }
  if (tokens.find(token) != tokens.end()) {
    return Status(Status::NotOK, "the token has already exists");
  }
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      tokens.erase(iter.first);
      tokens[token] = ns;
      return Status::OK();
    }
  }
  return Status(Status::NotOK, "the namespace was not found");
}

Status Config::AddNamespace(const std::string &ns, const std::string &token) {
  if (requirepass.empty()) {
    return Status(Status::NotOK, "forbid to add new namespace while the requirepass is empty");
  }
  if (codis_enabled) {
    return Status(Status::NotOK, "forbid to add new namespace while codis support is enabled");
  }
  auto s = isNamespaceLegal(ns);
  if (!s.IsOK()) {
    return s;
  }
  if (tokens.find(token) != tokens.end()) {
    return Status(Status::NotOK, "the token has already exists");
  }
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      return Status(Status::NotOK, "the namespace has already exists");
    }
  }
  tokens[token] = ns;
  return Status::OK();
}

Status Config::DelNamespace(const std::string &ns) {
  if (ns == kDefaultNamespace) {
    return Status(Status::NotOK, "can't del the default namespace");
  }
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      tokens.erase(iter.first);
      return Status::OK();
    }
  }
  return Status(Status::NotOK, "the namespace was not found");
}

Status Config::isNamespaceLegal(const std::string &ns) {
  if (ns.size() > UINT8_MAX) {
    return Status(Status::NotOK, std::string("namespace size exceed limit ") + std::to_string(UINT8_MAX));
  }
  char last_char = ns.back();
  if (last_char == std::numeric_limits<char>::max()) {
    return Status(Status::NotOK, std::string("namespace contain ilegal letter"));
  }
  return Status::OK();
}
