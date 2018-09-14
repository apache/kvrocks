#pragma once

#include <event2/event.h>
#include <event2/bufferevent.h>
#include <glog/logging.h>
#include <rocksdb/types.h>
#include <list>
#include <map>
#include <string>
#include <thread>

#include "redis_reply.h"
#include "status.h"

// forward declare
class Server;

namespace Redis {
class Commander {
 public:
  // @name: cmd name
  // @sidecar: whether cmd will be executed in sidecar thread, eg. psync.
  explicit Commander(std::string name, bool sidecar = false)
      : name_(name), is_sidecar_(sidecar) {}
  std::string Name() { return name_; }

  virtual Status Parse(const std::list<std::string> &args) = 0;
  virtual bool IsSidecar() { return is_sidecar_; }
  virtual Status Execute(Server *svr, std::string *output) {
    return Status(Status::RedisExecErr, "not implemented");
  }
  virtual Status SidecarExecute(Server *svr, int out_fd) {
    return Status(Status::RedisExecErr, "not implemented");
  }

  virtual ~Commander() = default;

 protected:
  std::string name_;
  bool is_sidecar_;
};

using CommanderFactory = std::function<std::unique_ptr<Commander>()>;
extern std::map<std::string, CommanderFactory> command_table;
Status LookupCommand(const std::string &cmd_name,
                     std::unique_ptr<Commander> *cmd);

class CommandGet : public Commander {
 public:
  explicit CommandGet() : Commander("get") {}
  Status Parse(const std::list<std::string> &args) override;
  Status Execute(Server *svr, std::string *) override;

 private:
  std::string key_;
};

class CommandSet : public Commander {
 public:
  explicit CommandSet() : Commander("set") {}
  Status Parse(const std::list<std::string> &args) override;
  Status Execute(Server *svr, std::string *) override;

 private:
  std::string key_;
  std::string value_;
};

class CommandSlaveOf : public Commander {
 public:
  explicit CommandSlaveOf() : Commander("slaveof") {}
  Status Parse(const std::list<std::string> &args) override;
  Status Execute(Server *svr, std::string *) override;

 private:
  std::string host_;
  uint32_t port_;
};

class CommandPSync : public Commander {
 public:
  explicit CommandPSync() : Commander("psync", true) {}
  Status Parse(const std::list<std::string> &args) override;
  Status SidecarExecute(Server *svr, int) override;

 private:
  rocksdb::SequenceNumber seq_;
};

void TakeOverBufferEvent(bufferevent *bev);

class SidecarCommandThread {
 public:
  explicit SidecarCommandThread(std::unique_ptr<Commander> cmd,
                                bufferevent *bev, Server *svr)
      : cmd_(std::move(cmd)), svr_(svr), bev_(bev) {}
  Status Start();
  void Stop() {
    if (bev_) bufferevent_free(bev_);
  }

 private:
  std::unique_ptr<Commander> cmd_;
  Server *svr_;
  std::thread t_;
  bufferevent *bev_;

  void Run();
};

}  // namespace Redis
