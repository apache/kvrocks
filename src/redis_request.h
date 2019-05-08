#pragma once

#include <event2/buffer.h>
#include <list>
#include <vector>
#include <string>

#include "redis_cmd.h"
#include "worker.h"
#include "status.h"
#include "storage.h"
#include "server.h"

namespace Redis {

class Connection;

class Request {
 public:
  explicit Request(Server *svr) : svr_(svr) {}
  // Not copyable
  Request(const Request &) = delete;
  Request &operator=(const Request &) = delete;

  // Parse the redis requests (bulk string array format)
  void Tokenize(evbuffer *input);
  // Exec return true when command finished
  void ExecuteCommands(Connection *conn);

 private:
  // internal states related to parsing

  enum ParserState { ArrayLen, BulkLen, BulkData };
  ParserState state_ = ArrayLen;
  size_t multi_bulk_len_ = 0;
  size_t bulk_len_ = 0;
  using CommandTokens = std::vector<std::string>;
  CommandTokens tokens_;
  std::vector<CommandTokens> commands_;

  Server *svr_;
};

class Connection {
 public:
  enum Flag {
    kMonitor         = 1<<5,
    kCloseAfterReply = 1<<6,
  };

  explicit Connection(bufferevent *bev, Worker *owner);
  ~Connection();

  static void OnRead(struct bufferevent *bev, void *ctx);
  static void OnWrite(struct bufferevent *bev, void *ctx);
  static void OnEvent(bufferevent *bev, int16_t events, void *ctx);
  void Reply(const std::string &msg);
  void SendFile(int fd);

  void SubscribeChannel(const std::string &channel);
  void UnSubscribeChannel(const std::string &channel);
  void UnSubscribeAll();
  int SubscriptionsCount();

  uint64_t GetAge();
  uint64_t GetIdleTime();
  void SetLastInteraction();
  std::string GetFlags();
  void EnableFlag(Flag flag);
  bool IsFlagEnabled(Flag flag);
  bool IsRepl() { return this->owner_->IsRepl();}

  uint64_t GetID() { return id_; }
  void SetID(uint64_t id) { id_ = id; }
  std::string GetName() { return name_; }
  void SetName(std::string name) { name_ = std::move(name); }
  std::string GetAddr() { return addr_; }
  void SetAddr(std::string addr) { addr_ = std::move(addr); }
  std::string GetLastCmd() { return last_cmd_; }
  void SetLastCmd(std::string cmd) { last_cmd_ = std::move(cmd); }

  bool IsAdmin() { return is_admin_; }
  void BecomeAdmin() { is_admin_ = true; }
  void BecomeUser() { is_admin_ = false; }
  std::string GetNamespace() { return ns_; }
  void SetNamespace(std::string ns) { ns_ = std::move(ns); }

  Worker *Owner() { return owner_; }
  int GetFD() { return bufferevent_getfd(bev_); }
  evbuffer *Input() { return bufferevent_get_input(bev_); }
  evbuffer *Output() { return bufferevent_get_output(bev_); }
  bufferevent *GetBufferEvent() { return bev_; }

  std::unique_ptr<Commander> current_cmd_;

 private:
  uint64_t id_ = 0;
  int flags_ = 0;
  std::string ns_;
  std::string name_;
  std::string addr_;
  bool is_admin_ = false;
  std::string last_cmd_;
  time_t create_time_;
  time_t last_interaction_;

  bufferevent *bev_;
  Request req_;
  Worker *owner_;
  std::vector<std::string> subscribe_channels_;
};

}  // namespace Redis
