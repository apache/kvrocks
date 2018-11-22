#ifndef KVROCKS_CONFIG_H
#define KVROCKS_CONFIG_H

typedef struct Config {
 public:
  int port = 6666;
  int workers = 4;
  int timeout = 0 ;
  int loglevel = 0;
  int backlog = 1024;
  int maxclients = 10240;
  bool daemonize = false;

  std::vector<std::string> binds{"127.0.0.1"};
  std::string pidfile = "/var/log/kvrocks.pid";
  std::string db_dir = "/tmp/ev";
  std::string backup_dir = "/tmp/ev_bak";

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
  bool Load(std::string path, std::string *err);

  int Rewrite();
  Config() = default;
  ~Config() = default;

 private:
  std::string path_;
  int yesnotoi(std::string input);
  void incrOpenFilesLimit(rlim_t maxfiles);
  bool parseRocksdbOption(std::string key, std::string value, std::string *err);
  bool parseConfigFromString(std::string input, std::string *err);
} Config;

#endif //KVROCKS_CONFIG_H
