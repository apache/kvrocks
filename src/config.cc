#include <fcntl.h>
#include <string.h>
#include <strings.h>
#include <glog/logging.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

#include "config.h"
#include "util.h"
#include "status.h"
#include "cron.h"

static const std::vector<std::string> loglevels {"info", "warning", "error", "fatal"};
static const char *default_namespace = "__namespace";

void Config::incrOpenFilesLimit(rlim_t maxfiles) {
  struct rlimit limit;

  rlim_t old_limit, best_limit = maxfiles, decr_step = 16;
  if (getrlimit(RLIMIT_NOFILE, &limit) < 0 || best_limit <= limit.rlim_cur) {
    return;
  }
  old_limit = limit.rlim_cur;
  while(best_limit > old_limit) {
    limit.rlim_cur = best_limit;
    limit.rlim_max = best_limit;
    if (setrlimit(RLIMIT_NOFILE,&limit) != -1) break;
    /* We failed to set file limit to 'bestlimit'. Try with a
     * smaller limit decrementing by a few FDs per iteration. */
    if (best_limit < decr_step) break;
    best_limit -= decr_step;
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

Status Config::parseRocksdbOption(std::string key, std::string value) {
  int32_t n;
  try {
    n = std::stoi(value);
  } catch (std::exception &e) {
    return Status(Status::NotOK, e.what());
  }
  if (key == "max_open_files" ) {
    rocksdb_options.max_open_files = n;
  } else if (!strncasecmp(key.data(), "write_buffer_size" , strlen("write_buffer_size"))) {
    if (n < 16 || n > 4096) {
      return Status(Status::NotOK, "write_buffer_size should be between 16MB and 4GB");
    }
    rocksdb_options.write_buffer_size = static_cast<size_t>(n) * 1048576;
  }  else if (key == "max_write_buffer_number" ) {
    if (n < 1 || n > 64) {
      return Status(Status::NotOK, "max_write_buffer_number should be between 1 and 64");
    }
    rocksdb_options.max_write_buffer_number = n;
  }  else if (key == "max_background_compactions" ) {
    if (n < 1 || n > 16) {
      return Status(Status::NotOK, "max_background_compactions should be between 1 and 16");
    }
    rocksdb_options.max_background_compactions = n;
  }  else if (key == "max_background_flushes" ) {
    if (n < 1 || n > 16) {
      return Status(Status::NotOK, "max_background_flushes should be between 1 and 16");
    }
    rocksdb_options.max_background_flushes = n;
  }  else if (key == "max_sub_compactions" ) {
    if (n < 1 || n > 8) {
      return Status(Status::NotOK, "max_sub_compactions should be between 1 and 8");
    }
    rocksdb_options.max_sub_compactions = static_cast<uint32_t>(n);
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
  } else if (size == 2 && args[0] == "repl-workers" ){
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
  }else if (size == 2 && args[0] == "daemonize") {
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
  } else if (size == 2 && args[0] == "tcp-backlog") {
    backlog = std::stoi(args[1]);
  } else if (size == 2 && args[0] == "dir") {
    dir = args[1];
    db_dir = dir + "/db";
  } else if (size == 2 && args[0] == "backup-dir") {
    backup_dir = args[1];
  } else if (size == 2 && args[0] == "maxclients") {
    maxclients = std::stoi(args[1]);
    if (maxclients > 0) incrOpenFilesLimit(static_cast<rlim_t >(maxclients));
  } else if (size == 2 && args[0] == "db-name") {
    db_name = args[1];
  } else if (size == 2 && args[0] == "masterauth") {
    masterauth = args[1];
  } else if (size == 2 && args[0] == "requirepass") {
    requirepass = args[1];
  } else if (size == 2 && args[0] == "pidfile") {
    pidfile = args[1];
  } else if (size == 2 && args[0] == "loglevel") {
    for (size_t i = 0; i < loglevels.size(); i++) {
      if (Util::ToLower(args[1]) == loglevels[i]) {
        loglevel = static_cast<int>(i);
        break;
      }
    }
  } else if (size == 3 && args[0] == "slaveof") {
    if (args[1] != "no" && args[2] != "one") {
      master_host = args[1];
      // we use port + 1 as repl port, so incr the slaveof port here
      master_port = std::stoi(args[2]) + 1;
      if (master_port <= 0 || master_port >= 65535) {
        return Status(Status::NotOK, "master port range should be between 0 and 65535");
      }
    }
  } else if (size >=2 && args[0] == "compact-cron") {
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
    if(ns.size() > INT8_MAX) {
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

  std::string line, parse_err;
  int line_num = 1;
  while (!file.eof()) {
    std::getline(file, line);
    line = Util::ToLower(line);
    Status s = parseConfigFromString(line);
    if (!s.IsOK()) {
      file.close();
      return Status(Status::NotOK, "at line: #L" + std::to_string(line_num) + ", err: " + s.Msg());
    }
    line_num++;
  }
  if (backup_dir.empty()) { // backup-dir was not assigned in config file
    backup_dir = dir+"/backup";
  }
  if (requirepass.empty()) {
    file.close();
    return Status(Status::NotOK, "requirepass cannot be empty");
  }
  tokens[requirepass] = default_namespace;
  file.close();
  return Status::OK();
}

bool Config::rewriteConfigValue(std::vector<std::string> &args) {
#define REWRITE_IF_MATCH(argc, k1, k2, value) do { \
  if((argc) == 2 && (k1) == (k2)) { \
    if (args[1] != (value)) {       \
      args[1] = (value);            \
      return true;                  \
    }                               \
    return false;                   \
  }                                 \
} while(0);

  size_t size = args.size();
  REWRITE_IF_MATCH(size, args[0], "masterauth", masterauth);
  REWRITE_IF_MATCH(size, args[0], "requirepass", requirepass);
  REWRITE_IF_MATCH(size, args[0], "maxclients", std::to_string(maxclients));
  REWRITE_IF_MATCH(size, args[0], "slave-read-only", (slave_readonly? "yes":"no"));
  REWRITE_IF_MATCH(size, args[0], "timeout", std::to_string(timeout));
  REWRITE_IF_MATCH(size, args[0], "backup-dir", backup_dir);
  REWRITE_IF_MATCH(size, args[0], "loglevel", loglevels[loglevel]);

  if (size >= 2 && args[0] == "compact-cron") {
    std::vector<std::string> new_args = compact_cron.ToConfParamVector();
    return rewriteCronConfigValue(new_args, args);
  }
  if (size >= 2 && args[0] == "bgsave-cron") {
    std::vector<std::string> new_args = bgsave_cron.ToConfParamVector();
    return rewriteCronConfigValue(new_args, args);
  }
  return false;
}

bool Config::rewriteCronConfigValue(const std::vector<std::string> &new_args, std::vector<std::string> &args) {
  size_t args_size = args.size();
  size_t new_args_size = new_args.size();
  if (new_args_size == args_size - 1 &&
      std::equal(new_args.begin(), new_args.end(), args.begin() + 1)
      ) {
    return false;
  }
  args.erase(args.begin() + 1, args.end());
  for (unsigned long i = 0; i < new_args.size(); i++) {
    args.push_back(new_args[i]);
  }
  return true;
}

void Config::Get(std::string &key, std::vector<std::string> *values) {
  key = Util::ToLower(key);
  values->clear();
  bool is_all = key == "*";
  bool is_rocksdb_all = (key == "rocksdb.*" || is_all);

#define PUSH_IF_MATCH(force, k1, k2, value) do { \
  if ((force) || (k1) == (k2)) { \
    values->emplace_back((k2)); \
    values->emplace_back((value)); \
  } \
} while(0);

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
  PUSH_IF_MATCH(is_all, key, "compact-cron", compact_cron.ToString());
  PUSH_IF_MATCH(is_all, key, "bgsave-cron", bgsave_cron.ToString());
  PUSH_IF_MATCH(is_all, key, "loglevel", loglevels[loglevel]);
  PUSH_IF_MATCH(is_all, key, "requirepass", requirepass);
  PUSH_IF_MATCH(is_all, key, "masterauth", masterauth);
  PUSH_IF_MATCH(is_all, key, "slaveof", master_str);
  PUSH_IF_MATCH(is_all, key, "pidfile", pidfile);
  PUSH_IF_MATCH(is_all, key, "db-name", db_name);
  PUSH_IF_MATCH(is_all, key, "binds", binds_str);

  PUSH_IF_MATCH(is_rocksdb_all, key, "rocksdb.max_open_files", std::to_string(rocksdb_options.max_open_files));
  PUSH_IF_MATCH(is_rocksdb_all, key, "rocksdb.block_cache_size", std::to_string(rocksdb_options.block_cache_size));
  PUSH_IF_MATCH(is_rocksdb_all, key, "rocksdb.write_buffer_size", std::to_string(rocksdb_options.write_buffer_size));
  PUSH_IF_MATCH(is_rocksdb_all, key, "rocksdb.max_write_buffer_number", std::to_string(rocksdb_options.max_write_buffer_number));
  PUSH_IF_MATCH(is_rocksdb_all, key, "rocksdb.max_background_compactions", std::to_string(rocksdb_options.max_background_compactions));
  PUSH_IF_MATCH(is_rocksdb_all, key, "rocksdb.max_background_flushes", std::to_string(rocksdb_options.max_background_flushes));
  PUSH_IF_MATCH(is_rocksdb_all, key, "rocksdb.max_sub_compactions", std::to_string(rocksdb_options.max_sub_compactions));
}

Status Config::Set(std::string &key, std::string &value) {
  key = Util::ToLower(key);
  if (key == "timeout") {
    timeout = std::stoi(value);
    return Status::OK();
  }
  if (key == "backup-dir") {
    backup_dir = value;
    return Status::OK();
  }
  if (key == "maxclients") {
    timeout = std::stoi(value);
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
    tokens[requirepass] = default_namespace;
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
  if (key == "loglevel") {
    for (size_t i = 0; i < loglevels.size(); i++) {
      if (Util::ToLower(value) == loglevels[i]) {
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
  return Status(Status::NotOK, "Unsupported CONFIG parameter");
}

Status Config::Rewrite() {
  std::string tmp_path = path_+".tmp";
  std::ostringstream string_stream;

  remove(tmp_path.data());
  std::ifstream input_file(path_, std::ios::in);
  std::ofstream output_file(tmp_path, std::ios::out);
  if (!input_file.is_open() || !output_file.is_open()) {
    if (input_file.is_open()) input_file.close();
    return Status(Status::NotOK, strerror(errno));
  }

  std::string line, new_line, buffer;
  std::vector<std::string> args;
  while (!input_file.eof()) {
    std::getline(input_file, line);
    Util::Split(line, " \t\r\n", &args);
    if (args.empty() || args[0].front() == '#' || !rewriteConfigValue(args)) {
      if (!strncasecmp(args[0].data(), "namespace.", 10)) {
        // skip the namespace, append at the end
        continue;
      }
      buffer.append(line);
      buffer.append("\n");
    } else {
      string_stream.str(std::string());
      string_stream.clear();
      for (const auto &arg : args) {
        string_stream << arg << " ";
      }
      buffer.append(string_stream.str());
      buffer.append("\n");
    }
  }
  string_stream.str(std::string());
  string_stream.clear();
  for (auto iter = tokens.begin(); iter != tokens.end(); ++iter) {
    if (iter->first != requirepass) {
      string_stream << "namespace." << iter->second << " " << iter->first << "\n";
    }
  }
  buffer.append(string_stream.str());

  output_file.write(buffer.data(), buffer.size());
  input_file.close();
  output_file.close();
  if (rename(tmp_path.data(), path_.data()) < 0) {
    return Status(Status::NotOK, std::string("unable to rename, err: ")+strerror(errno));
  }
  return Status::OK();
}

void Config::GetNamespace(std::string &ns, std::string *token) {
  for (auto iter = tokens.begin(); iter != tokens.end(); iter++) {
    if (iter->second == ns) {
      *token = iter->first;
    }
  }
}

Status Config::SetNamepsace(std::string &ns, std::string token) {
  if (ns == default_namespace) {
    return Status(Status::NotOK, "can't set the default namespace");
  }
  if (tokens.find(token) != tokens.end()) {
    return Status(Status::NotOK, "the token has already exists");
  }
  for (auto iter = tokens.begin(); iter != tokens.end(); iter++) {
    if (iter->second == ns) {
      tokens.erase(iter);
      tokens[token] = ns;
      LOG(WARNING) << "Updated namespace: " << ns << ", new token: " << token;
      return Status::OK();
    }
  }
  return Status(Status::NotOK, "namespace was not found");
}

Status Config::AddNamespace(const std::string &ns, const std::string &token) {
  if (ns.size() > 255) {
    return Status(Status::NotOK, "namespace size exceed limit " + std::to_string(INT8_MAX));
  }
  if (tokens.find(token) != tokens.end()) {
    return Status(Status::NotOK, "the token has already exists");
  }
  for (auto iter = tokens.begin(); iter != tokens.end(); iter++) {
    if (iter->second == ns) {
      return Status(Status::NotOK, "namespace has already exists");
    }
  }
  tokens[token] = ns;
  LOG(WARNING) << "Create new namespace: " << ns << ", token: " << token;
  return Status::OK();
}

Status Config::DelNamespace(std::string &ns) {
  if (ns == default_namespace) {
    return Status(Status::NotOK, "can't del the default namespace");
  }
  for (auto iter = tokens.begin(); iter != tokens.end(); iter++) {
    if (iter->second == ns) {
      tokens.erase(iter);
      LOG(WARNING) << "Deleted namespace: " << ns << ", token: " << iter->first;
      return Status::OK();
    }
  }
  return Status(Status::NotOK, "namespace was not found");
}
