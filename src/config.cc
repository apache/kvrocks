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

#include "config.h"
#include "util.h"
#include "status.h"
#include "cron.h"
#include "server.h"

const char *kDefaultNamespace = "__namespace";
const uint64_t kIORateLimitMinMb = 128;
static const char *kLogLevels[] = {"info", "warning", "error", "fatal"};
static const size_t kNumLogLevel = sizeof(kLogLevels)/ sizeof(kLogLevels[0]);
static const char *kCompressionType[] = {"no", "snappy"};
static const size_t kNumCompressionType = sizeof(kCompressionType) / sizeof(kCompressionType[0]);

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
  } else {
    return parseRocksdbIntOption(key, value);
  }
  return Status::OK();
}

Status Config::parseRocksdbIntOption(std::string key, std::string value) {
  int32_t n;
  try {
    n = std::stoi(value);
  } catch (std::exception &e) {
    return Status(Status::NotOK, e.what());
  }
  if (key == "max_open_files") {
    rocksdb_options.max_open_files = n;
  } else if (!strncasecmp(key.data(), "write_buffer_size" , strlen("write_buffer_size"))) {
    if (n < 16 || n > 4096) {
      return Status(Status::NotOK, "write_buffer_size should be between 16MB and 4GB");
    }
    rocksdb_options.write_buffer_size = static_cast<size_t>(n) * MiB;
  }  else if (key == "max_write_buffer_number") {
    if (n < 1 || n > 64) {
      return Status(Status::NotOK, "max_write_buffer_number should be between 1 and 64");
    }
    rocksdb_options.max_write_buffer_number = n;
  }  else if (key == "max_background_compactions") {
    if (n < 1 || n > 16) {
      return Status(Status::NotOK, "max_background_compactions should be between 1 and 16");
    }
    rocksdb_options.max_background_compactions = n;
  }  else if (key == "max_background_flushes") {
    if (n < 1 || n > 16) {
      return Status(Status::NotOK, "max_background_flushes should be between 1 and 16");
    }
    rocksdb_options.max_background_flushes = n;
  }  else if (key == "max_sub_compactions") {
    if (n < 1 || n > 8) {
      return Status(Status::NotOK, "max_sub_compactions should be between 1 and 8");
    }
    rocksdb_options.max_sub_compactions = static_cast<uint32_t>(n);
  } else if (key == "metadata_block_cache_size") {
    rocksdb_options.metadata_block_cache_size = n * MiB;
  } else if (key == "subkey_block_cache_size") {
    rocksdb_options.subkey_block_cache_size = n * MiB;
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
    port = std::stoi(args[1]);
    repl_port = port + 1;
  } else if (size == 2 && args[0] == "timeout") {
    timeout = std::stoi(args[1]);
  } else if (size == 2 && args[0] == "workers") {
    workers = std::stoi(args[1]);
    if (workers < 1 || workers > 1024) {
      return Status(Status::NotOK, "too many worker threads");
    }
  } else if (size == 2 && args[0] == "repl-workers") {
    repl_workers = std::stoi(args[1]);
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
  } else if (size == 2 && args[0] == "slave-priority") {
    slave_priority = std::stoi(args[1]);
  } else if (size == 2 && args[0] == "tcp-backlog") {
    backlog = std::stoi(args[1]);
  } else if (size == 2 && args[0] == "dir") {
    dir = args[1];
    db_dir = dir + "/db";
    pidfile = dir + "/kvrocks.pid";
  } else if (size == 2 && args[0] == "backup-dir") {
    backup_dir = args[1];
  } else if (size == 2 && args[0] == "maxclients") {
    maxclients = std::stoi(args[1]);
    if (maxclients > 0) incrOpenFilesLimit(static_cast<rlim_t >(maxclients));
  } else if (size == 2 && args[0] == "db-name") {
    db_name = args[1];
  } else if (size == 2 && args[0] == "masterauth") {
    masterauth = args[1];
  } else if (size == 2 && args[0] == "max-backup-to-keep") {
    max_backup_to_keep = static_cast<uint32_t>(std::stoi(args[1]));
  } else if (size == 2 && args[0] == "max-backup-keep-hours") {
    max_backup_keep_hours = static_cast<uint32_t>(std::stoi(args[1]));
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
      master_port = std::stoi(args[2]);
      if (master_port <= 0 || master_port >= 65535) {
        return Status(Status::NotOK, "master port range should be between 0 and 65535");
      }
    }
  } else if (size == 2 && args[0] == "max-db-size") {
    max_db_size = static_cast<uint32_t>(std::stoi(args[1]));
  } else if (size == 2 && args[0] == "max-replication-mb") {
    max_replication_mb = static_cast<uint64_t>(std::stoi(args[1]));
  } else if (size == 2 && args[0] == "max-io-mb") {
    max_io_mb = static_cast<uint64_t>(std::stoi(args[1]));
    if (max_io_mb > 0 && max_io_mb < kIORateLimitMinMb) {
      return Status(Status::NotOK, std::string("max_io_mb should be >= ") + std::to_string(kIORateLimitMinMb));
    }
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
  } else if (size == 2 && !strncasecmp(args[0].data(), "rocksdb.", 8)) {
    return parseRocksdbOption(args[0].substr(8, args[0].size() - 8), args[1]);
  } else if (size == 2 && !strncasecmp(args[0].data(), "namespace.", 10)) {
    std::string ns = args[0].substr(10, args.size()-10);
    if (ns.size() > INT8_MAX) {
      return Status(Status::NotOK, std::string("namespace size exceed limit ")+std::to_string(INT8_MAX));
    }
    tokens[args[1]] = ns;
  } else if (size == 2 && !strcasecmp(args[0].data(), "slowlog-log-slower-than")) {
    slowlog_log_slower_than = std::stoll(args[1]);
  } else if (size == 2 && !strcasecmp(args[0].data(), "slowlog-max-len")) {
    slowlog_max_len = std::stoi(args[1]);
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

  if (requirepass.empty()) {
    file.close();
    return Status(Status::NotOK, "requirepass cannot be empty");
  }
  tokens[requirepass] = kDefaultNamespace;
  file.close();
  return Status::OK();
}

void Config::Get(std::string key, std::vector<std::string> *values) {
  key = Util::ToLower(key);
  values->clear();
  bool is_all = key == "*";
  bool is_rocksdb_all = (key == "rocksdb.*" || is_all);

#define PUSH_IF_MATCH(force, k1, k2, value) do { \
  if ((force) || (k1) == (k2)) { \
    values->emplace_back((k2)); \
    values->emplace_back((value)); \
  } \
} while (0);

  std::string master_str;
  if (!master_host.empty()) {
    master_str = master_host+" "+ std::to_string(master_port);
  }
  std::string binds_str;
  for (const auto &bind : binds) {
    binds_str.append(bind);
    binds_str.append(",");
  }
  binds_str = binds_str.substr(0, binds_str.size()-1);

  PUSH_IF_MATCH(is_all, key, "dir", dir);
  PUSH_IF_MATCH(is_all, key, "db-dir", db_dir);
  PUSH_IF_MATCH(is_all, key, "backup-dir", backup_dir);
  PUSH_IF_MATCH(is_all, key, "port", std::to_string(port));
  PUSH_IF_MATCH(is_all, key, "workers", std::to_string(workers));
  PUSH_IF_MATCH(is_all, key, "timeout", std::to_string(timeout));
  PUSH_IF_MATCH(is_all, key, "tcp-backlog", std::to_string(backlog));
  PUSH_IF_MATCH(is_all, key, "daemonize", (daemonize ? "yes" : "no"));
  PUSH_IF_MATCH(is_all, key, "maxclients", std::to_string(maxclients));
  PUSH_IF_MATCH(is_all, key, "slave-read-only", (slave_readonly ? "yes" : "no"));
  PUSH_IF_MATCH(is_all, key, "slave-priority", std::to_string(slave_priority));
  PUSH_IF_MATCH(is_all, key, "max-backup-to-keep", std::to_string(max_backup_to_keep));
  PUSH_IF_MATCH(is_all, key, "max-backup-keep-hours", std::to_string(max_backup_keep_hours));
  PUSH_IF_MATCH(is_all, key, "compact-cron", compact_cron.ToString());
  PUSH_IF_MATCH(is_all, key, "bgsave-cron", bgsave_cron.ToString());
  PUSH_IF_MATCH(is_all, key, "loglevel", kLogLevels[loglevel]);
  PUSH_IF_MATCH(is_all, key, "requirepass", requirepass);
  PUSH_IF_MATCH(is_all, key, "masterauth", masterauth);
  PUSH_IF_MATCH(is_all, key, "slaveof", master_str);
  PUSH_IF_MATCH(is_all, key, "pidfile", pidfile);
  PUSH_IF_MATCH(is_all, key, "db-name", db_name);
  PUSH_IF_MATCH(is_all, key, "binds", binds_str);
  PUSH_IF_MATCH(is_all, key, "max-db-size", std::to_string(max_db_size));
  PUSH_IF_MATCH(is_all, key, "max-replication-mb", std::to_string(max_replication_mb));
  PUSH_IF_MATCH(is_all, key, "max-io-mb", std::to_string(max_io_mb));
  PUSH_IF_MATCH(is_all, key, "slowlog-max-len", std::to_string(slowlog_max_len));
  PUSH_IF_MATCH(is_all, key, "slowlog-log-slower-than", std::to_string(slowlog_log_slower_than));

  PUSH_IF_MATCH(is_rocksdb_all, key,
      "rocksdb.max_open_files", std::to_string(rocksdb_options.max_open_files));
  PUSH_IF_MATCH(is_rocksdb_all, key,
      "rocksdb.write_buffer_size", std::to_string(rocksdb_options.write_buffer_size/MiB));
  PUSH_IF_MATCH(is_rocksdb_all, key,
      "rocksdb.max_write_buffer_number", std::to_string(rocksdb_options.max_write_buffer_number));
  PUSH_IF_MATCH(is_rocksdb_all, key,
      "rocksdb.max_background_compactions", std::to_string(rocksdb_options.max_background_compactions));
  PUSH_IF_MATCH(is_rocksdb_all, key,
                "rocksdb.metadata_block_cache_size", std::to_string(rocksdb_options.metadata_block_cache_size/MiB));
  PUSH_IF_MATCH(is_rocksdb_all, key,
                "rocksdb.subkey_block_cache_size", std::to_string(rocksdb_options.subkey_block_cache_size/MiB));
  PUSH_IF_MATCH(is_rocksdb_all, key,
      "rocksdb.max_background_flushes", std::to_string(rocksdb_options.max_background_flushes));
  PUSH_IF_MATCH(is_rocksdb_all, key,
      "rocksdb.max_sub_compactions", std::to_string(rocksdb_options.max_sub_compactions));
  PUSH_IF_MATCH(is_rocksdb_all, key,
                "rocksdb.compression", kCompressionType[rocksdb_options.compression]);
  PUSH_IF_MATCH(is_rocksdb_all, key,
                "rocksdb.stats_dump_period_sec", std::to_string(rocksdb_options.stats_dump_period_sec));
  PUSH_IF_MATCH(is_rocksdb_all, key,
                "rocksdb.enable_pipelined_write", (rocksdb_options.enable_pipelined_write ? "yes": "no"))
}

Status Config::Set(std::string key, const std::string &value, Server *svr) {
  key = Util::ToLower(key);
  if (key == "timeout") {
    timeout = std::stoi(value);
    return Status::OK();
  }
  if (key == "backup-dir") {
    auto s = rocksdb::Env::Default()->CreateDirIfMissing(value);
    if (!s.ok()) return Status(Status::NotOK, s.ToString());
    backup_dir = value;
    return Status::OK();
  }
  if (key == "maxclients") {
    maxclients = std::stoi(value);
    return Status::OK();
  }
  if (key == "max-backup-to-keep") {
    max_backup_to_keep = static_cast<uint32_t>(std::stoi(value));
    return Status::OK();
  }
  if (key == "max-backup-keep-hours") {
    max_backup_keep_hours = static_cast<uint32_t>(std::stoi(value));
    return Status::OK();
  }
  if (key == "masterauth") {
    masterauth = value;
    return Status::OK();
  }
  if (key == "requirepass") {
    if (value.empty()) {
      return Status(Status::NotOK, "requirepass cannot be empty");
    }
    tokens.erase(requirepass);
    requirepass = value;
    tokens[requirepass] = kDefaultNamespace;
    LOG(WARNING) << "Updated requirepass,  new requirepass: " << value;
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
  if (key == "slave-priority") {
    slave_priority = std::stoi(value);
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
    slowlog_log_slower_than = std::stoll(value);
    return Status::OK();
  }
  if (key == "slowlog-max-len") {
    slowlog_max_len = std::stoi(value);
    return Status::OK();
  }
  if (key == "max-db-size") {
    try {
      int32_t i = std::stoi(value);
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
    try {
      int64_t i = std::stoi(value);
      if (i < 0) {
        return Status(Status::RedisParseErr, "value should be >= 0");
      }
      max_replication_mb = static_cast<uint64_t>(i);
      svr->SetReplicationRateLimit(max_replication_mb);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
    }
    return Status::OK();
  }
  if (key == "max-io-mb") {
    try {
      int64_t i = std::stoi(value);
      if (i > 0 && i < static_cast<int64_t>(kIORateLimitMinMb)) {
        return Status(Status::RedisParseErr, "value should be >= " + std::to_string(kIORateLimitMinMb));
      }
      max_io_mb = static_cast<uint64_t>(i);
      svr->storage_->SetIORateLimit(max_io_mb);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
    }
    return Status::OK();
  }
  if (key == "rocksdb.stats_dump_period_sec") {
    try {
      int i = std::stoi(value);
      if (i != 0 && i < 60) {
        return Status(Status::RedisParseErr, "value should be >= 60");
      }
      rocksdb_options.stats_dump_period_sec = i;
      auto db = svr->storage_->GetDB();
      auto s = db->SetDBOptions({{"stats_dump_period_sec", value}});
      if (s.ok()) return Status::OK();
      return Status(Status::NotOK, s.ToString());
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
    }
  }
  if (key == "rocksdb.enable_pipelined_write") {
    int i;
    if ((i = yesnotoi(value)) == -1) {
      return Status(Status::NotOK, "value must be 'yes' or 'no'");
    }
    rocksdb_options.enable_pipelined_write = i == 1;
    auto db = svr->storage_->GetDB();
    auto s = db->SetDBOptions({{"enable_pipelined_write", (i==1?"true":"false")}});
    return Status::OK();
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
  std::string binds_str, repl_binds_str;
  array2String(binds, ",", &binds_str);
  array2String(repl_binds, ",", &repl_binds_str);

  string_stream << "################################ GERNERAL #####################################\n";
  WRITE_TO_FILE("bind", binds_str);
  WRITE_TO_FILE("port", std::to_string(port));
  WRITE_TO_FILE("repl-bind", repl_binds_str);
  WRITE_TO_FILE("timeout", std::to_string(timeout));
  WRITE_TO_FILE("workers", std::to_string(workers));
  WRITE_TO_FILE("maxclients", std::to_string(maxclients));
  WRITE_TO_FILE("repl-workers", std::to_string(repl_workers));
  WRITE_TO_FILE("loglevel", kLogLevels[loglevel]);
  WRITE_TO_FILE("daemonize", (daemonize?"yes":"no"));
  WRITE_TO_FILE("requirepass", requirepass);
  WRITE_TO_FILE("db-name", db_name);
  WRITE_TO_FILE("dir", dir);
  WRITE_TO_FILE("backup-dir", backup_dir);
  WRITE_TO_FILE("tcp-backlog", std::to_string(backlog));
  WRITE_TO_FILE("slave-read-only", (slave_readonly? "yes":"no"));
  WRITE_TO_FILE("slave-priority", std::to_string(slave_priority));
  WRITE_TO_FILE("slowlog-max-len", std::to_string(slowlog_max_len));
  WRITE_TO_FILE("slowlog-log-slower-than", std::to_string(slowlog_log_slower_than));
  WRITE_TO_FILE("max-backup-to-keep", std::to_string(max_backup_to_keep));
  WRITE_TO_FILE("max-backup-keep-hours", std::to_string(max_backup_keep_hours));
  WRITE_TO_FILE("max-db-size", std::to_string(max_db_size));
  WRITE_TO_FILE("max-replication-mb", std::to_string(max_replication_mb));
  WRITE_TO_FILE("max-io-mb", std::to_string(max_io_mb));
  if (!masterauth.empty()) WRITE_TO_FILE("masterauth", masterauth);
  if (!master_host.empty())  WRITE_TO_FILE("slaveof", master_host+" "+std::to_string(master_port));
  if (compact_cron.IsEnabled()) WRITE_TO_FILE("compact-cron", compact_cron.ToString());
  if (bgsave_cron.IsEnabled()) WRITE_TO_FILE("bgave-cron", bgsave_cron.ToString());

  string_stream << "\n################################ ROCKSDB #####################################\n";
  WRITE_TO_FILE("rocksdb.max_open_files", std::to_string(rocksdb_options.max_open_files));
  WRITE_TO_FILE("rocksdb.write_buffer_size", std::to_string(rocksdb_options.write_buffer_size/MiB));
  WRITE_TO_FILE("rocksdb.max_write_buffer_number", std::to_string(rocksdb_options.max_write_buffer_number));
  WRITE_TO_FILE("rocksdb.max_background_compactions", std::to_string(rocksdb_options.max_background_compactions));
  WRITE_TO_FILE("rocksdb.metadata_block_cache_size", std::to_string(rocksdb_options.metadata_block_cache_size/MiB));
  WRITE_TO_FILE("rocksdb.subkey_block_cache_size", std::to_string(rocksdb_options.subkey_block_cache_size/MiB));
  WRITE_TO_FILE("rocksdb.max_background_flushes", std::to_string(rocksdb_options.max_background_flushes));
  WRITE_TO_FILE("rocksdb.max_sub_compactions", std::to_string(rocksdb_options.max_sub_compactions));
  WRITE_TO_FILE("rocksdb.compression", kCompressionType[rocksdb_options.compression]);
  WRITE_TO_FILE("rocksdb.enable_pipelined_write", (rocksdb_options.enable_pipelined_write ? "yes" : "no"));

  string_stream << "\n################################ Namespace #####################################\n";
  std::string ns_prefix = "namespace.";
  for (const auto &iter : tokens) {
    if (iter.second == kDefaultNamespace) continue;
    WRITE_TO_FILE(ns_prefix+iter.second, iter.first);
  }
  output_file.write(string_stream.str().c_str(), string_stream.str().size());
  output_file.close();
  if (rename(tmp_path.data(), path_.data()) < 0) {
    return Status(Status::NotOK, std::string("unable to rename, err: ")+strerror(errno));
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
  if (ns.size() > 255) {
    return Status(Status::NotOK, "the namespace size exceed limit " + std::to_string(INT8_MAX));
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
