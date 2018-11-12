#pragma once

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <rocksdb/types.h>
#include <rocksdb/utilities/backupable_db.h>
#include <list>
#include <map>
#include <string>
#include <thread>

#include "redis_reply.h"
#include "status.h"

// forward declare
class Worker;

namespace Redis {
class Commander {
 public:
  // @name: cmd name
  // @sidecar: whether cmd will be executed in sidecar thread, eg. psync.
  explicit Commander(std::string name, int arity, bool sidecar = false)
      : name_(name), arity_(arity), is_sidecar_(sidecar) {}
  std::string Name() { return name_; }
  int GetArity() { return arity_; }

  void SetArgs(const std::vector<std::string> args) { args_ = args; }
  virtual Status Parse(const std::vector<std::string> &args) {
    return Status::OK();
  };
  virtual bool IsSidecar() { return is_sidecar_; }
  virtual Status Execute(Worker *svr, std::string *output) {
    return Status(Status::RedisExecErr, "not implemented");
  }
  virtual Status SidecarExecute(Worker *svr, int sock_fd) {
    return Status(Status::RedisExecErr, "not implemented");
  }

  virtual ~Commander() = default;

 protected:
  std::vector<std::string> args_;
  std::string name_;
  int arity_;
  int64_t calls_;
  int64_t microseconds;
  bool is_sidecar_;
};

Status LookupCommand(const std::string &cmd_name,
                     std::unique_ptr<Commander> *cmd);

void TakeOverBufferEvent(bufferevent *bev);

class SidecarCommandThread {
 public:
  explicit SidecarCommandThread(std::unique_ptr<Commander> cmd,
                                bufferevent *bev, Worker *svr)
      : cmd_(std::move(cmd)), svr_(svr), bev_(bev) {}
  Status Start();
  void Stop() {
    if (bev_) bufferevent_free(bev_);
  }

 private:
  std::unique_ptr<Commander> cmd_;
  Worker *svr_;
  std::thread t_;
  bufferevent *bev_;

  void Run();
};

}  // namespace Redis
