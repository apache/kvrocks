#pragma once

#include <sys/resource.h>

#include <string>
#include <map>
#include <vector>

#include "status.h"
#include "cron.h"

struct Config{
 public:
  int port = 6666;
  int repl_port = 6668;
  int workers = 4;
  int repl_workers = 1;
  int timeout = 0 ;
  int loglevel = 0;
  int backlog = 1024;
  int maxclients = 10240;
  int64_t slowlog_log_slower_than = -1;
  unsigned int slowlog_max_len = 0;
  bool daemonize = false;
  bool slave_readonly = true;

  std::vector<std::string> binds{"127.0.0.1"};
  std::vector<std::string> repl_binds{"127.0.0.1"};
  std::string pidfile = "/var/log/kvrocks.pid";
  std::string db_dir = "/tmp/ev";
  std::string db_name = "changeme.name";
  std::string backup_dir = "/tmp/ev_bak";
  std::string masterauth;
  std::string requirepass;
  std::string master_host;
  Cron compact_cron;
  Cron bgsave_cron;
  int master_port = 0;
  std::map<std::string, std::string> tokens;

  struct {
    int max_open_files = 4096;
    size_t write_buffer_size = 256 * 1048576; // unit is MB
    int max_write_buffer_number = 2;
    int max_background_compactions = 2;
    int max_background_flushes = 2;
    uint32_t max_sub_compactions = 1;
    uint64_t block_cache_size = 1048576; // unit is MB
  } rocksdb_options;

 public:
  Status Rewrite();
  Status Load(std::string path);
  void Get(std::string &key, std::vector<std::string> *values);
  Status Set(std::string &key, std::string &value);
  void GetNamespace(std::string &ns, std::string *token);
  Status DelNamespace(std::string &ns);
  Status SetNamepsace(std::string &ns, std::string token);
  Status AddNamespace(const std::string &ns, const std::string &token);
  Config() = default;
  ~Config() = default;

 private:
  std::string path_;
  int yesnotoi(std::string input);
  void incrOpenFilesLimit(rlim_t maxfiles);
  Status parseRocksdbOption(std::string key, std::string value);
  Status parseConfigFromString(std::string input);
  bool rewriteConfigValue(std::vector<std::string> &args);
};
