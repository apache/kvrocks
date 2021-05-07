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
struct CommandAttributes;

class Commander {
 public:
  std::string Name();
  int GetArity() const;
  bool IsWrite() const;

  void SetAttributes(const CommandAttributes *attributes) { attributes_ = attributes; }
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
  const CommandAttributes *attributes_;
};

using CommanderFactory = std::function<std::unique_ptr<Commander>()>;

struct CommandAttributes {
  std::string name;
  int arity;
  bool is_write;
  int first_key;
  int last_key;
  int key_step;
  CommanderFactory factory;
};

void PopulateCommands();
bool IsCommandExists(const std::string &cmd);
void GetCommandList(std::vector<std::string> *cmds);
Status LookupAndCreateCommand(const std::string &cmd_name,
                              std::unique_ptr<Commander> *cmd);
}  // namespace Redis
