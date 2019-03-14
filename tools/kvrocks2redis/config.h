#pragma once

#include <string>
#include <map>
#include <vector>

#include "../../src/status.h"

namespace Kvrocks2redis {

struct redis_server {
  std::string host;
  uint32_t port;
  std::string auth;
};
struct Config {
 public:
  int workers = 4;
  int loglevel = 0;
  bool daemonize = false;

  std::string pidfile = "/var/log/kvrocksredis.pid";
  std::string dir = "/tmp/ev";
  std::string db_dir = dir + "/db";
  std::string aof_file_name = "appendonly.aof";
  std::string next_offset_file_name = "last_next_offset.txt";
  std::string next_seq_file_path = dir + "/last_next_seq.txt";

  std::string db_name = "changeme.name";
  std::string kvrocks_auth;
  std::string kvrocks_host;
  int kvrocks_port = 0;
  std::map<std::string, redis_server> tokens;

  struct {
    int max_open_files = 4096;
  } rocksdb_options;

 public:
  Status Load(std::string path);
  void Get(std::string key, std::vector<std::string> *values);
  Status Set(std::string key, const std::string &value);
  Config() = default;
  ~Config() = default;

 private:
  std::string path_;
  int yesnotoi(std::string input);
  Status parseConfigFromString(std::string input);
  Status parseRocksdbOption(std::string key, std::string value);
};

}  // namespace Kvrocks2redis

