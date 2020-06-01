#pragma once

#include <list>
#include <map>
#include <string>
#include <vector>
#include <thread>
#include <utility>
#include <memory>

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <rocksdb/types.h>
#include <rocksdb/utilities/backupable_db.h>

#include "redis_reply.h"
#include "status.h"

class Server;
namespace Redis {

class Connection;

class Commander {
 public:
  // @name: cmd name
  // @sidecar: whether cmd will be executed in sidecar thread, eg. psync.
  explicit Commander(std::string name, int arity, bool is_write = false)
      : name_(std::move(name)), arity_(arity), is_write_(is_write) {}
  std::string Name() { return name_; }
  int GetArity() { return arity_; }
  bool IsWrite() { return is_write_; }

  void SetArgs(const std::vector<std::string> &args) { args_ = args; }
  const std::vector<std::string>* Args() {
    return &args_;
  }
  virtual Status Parse(const std::vector<std::string> &args) {
    return Status::OK();
  }
  virtual Status Execute(Server *svr, Connection *conn, std::string *output) {
    return Status(Status::RedisExecErr, "not implemented");
  }

  virtual ~Commander() = default;

 protected:
  std::vector<std::string> args_;
  std::string name_;
  int arity_;
  bool is_write_;
};

bool IsCommandExists(const std::string &cmd);
void GetCommandList(std::vector<std::string> *cmds);
Status LookupCommand(const std::string &cmd_name,
                     std::unique_ptr<Commander> *cmd, bool is_repl);
}  // namespace Redis
