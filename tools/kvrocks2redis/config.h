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
  int db_number;
};
struct Config {
 public:
  int loglevel = 0;
  bool daemonize = false;

  std::string dir = "/tmp/ev";
  std::string db_dir = dir + "/db";
  std::string pidfile = dir+"/kvrocks2redis.pid";
  std::string aof_file_name = "appendonly.aof";
  std::string next_offset_file_name = "last_next_offset.txt";
  std::string next_seq_file_path = dir + "/last_next_seq.txt";

  std::string kvrocks_auth;
  std::string kvrocks_host;
  int kvrocks_port = 0;
  std::map<std::string, redis_server> tokens;

 public:
  Status Load(std::string path);
  Config() = default;
  ~Config() = default;

 private:
  std::string path_;
  int yesnotoi(std::string input);
  Status parseConfigFromString(std::string input);
};

}  // namespace Kvrocks2redis

