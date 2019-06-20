#pragma once

#include <sys/resource.h>
#include <rocksdb/options.h>

#include <string>
#include <map>
#include <vector>

#include "status.h"
#include "cron.h"

// forward declaration
class Server;

extern const char *kDefaultNamespace;

const uint32_t KiB = 1024;
const uint32_t MiB = 1024 * KiB;
const uint32_t GiB = 1024 * MiB;

struct Config{
 public:
  int port = 6666;
  int repl_port = port + 1;
  int workers = 4;
  int repl_workers = 1;
  int timeout = 0;
  int loglevel = 0;
  int backlog = 1024;
  int maxclients = 10240;
  uint32_t max_backup_to_keep = 1;
  uint32_t max_backup_keep_hours = 0;
  int64_t slowlog_log_slower_than = 200000;  // 200ms
  unsigned int slowlog_max_len = 0;
  bool daemonize = false;
  bool slave_readonly = true;
  uint32_t slave_priority = 100;
  uint32_t max_db_size = 0;  // unit is GB
  uint64_t max_replication_mb = 0;  // unit is MB
  uint64_t max_io_mb = 500;  // unit is MB

  std::vector<std::string> binds{"127.0.0.1"};
  std::vector<std::string> repl_binds{"127.0.0.1"};
  std::string dir = "/tmp/ev";
  std::string db_dir = dir+"/db";
  std::string backup_dir;
  std::string pidfile = dir+"/kvrocks.pid";
  std::string db_name = "changeme.name";
  std::string masterauth;
  std::string requirepass;
  std::string master_host;
  int master_port = 0;
  Cron compact_cron;
  Cron bgsave_cron;
  std::map<std::string, std::string> tokens;

  struct {
    size_t metadata_block_cache_size = 4 * GiB;
    size_t subkey_block_cache_size = 8 * GiB;
    int max_open_files = 4096;
    size_t write_buffer_size = 256 * MiB;
    int max_write_buffer_number = 2;
    int max_background_compactions = 2;
    int max_background_flushes = 2;
    uint32_t max_sub_compactions = 1;
    rocksdb::CompressionType compression = rocksdb::kSnappyCompression;  // default: snappy
    int stats_dump_period_sec = 0;
    bool enable_pipelined_write = true;
  } rocksdb_options;

 public:
  Status Rewrite();
  Status Load(std::string path);
  void Get(std::string key, std::vector<std::string> *values);
  Status Set(std::string key, const std::string &value, Server *svr);
  void GetNamespace(const std::string &ns, std::string *token);
  Status AddNamespace(const std::string &ns, const std::string &token);
  Status SetNamespace(const std::string &ns, const std::string &token);
  Status DelNamespace(const std::string &ns);
  Config() = default;
  ~Config() = default;

 private:
  std::string path_;
  int yesnotoi(std::string input);
  void incrOpenFilesLimit(rlim_t maxfiles);
  Status parseConfigFromString(std::string input);
  Status parseRocksdbOption(const std::string &key, std::string value);
  Status parseRocksdbIntOption(std::string key, std::string value);
  void array2String(const std::vector<std::string> &array, const std::string &delim, std::string *output);
};
