#pragma once

#include <event2/buffer.h>
#include <list>
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
    kCloseAfterReply = 1<<6,
  };
  explicit Connection(bufferevent *bev, Worker *owner) : bev_(bev), req_(owner->svr_), owner_(owner) {
    create_time_ = std::chrono::system_clock::now();
    last_interaction_ = create_time_;
  }
  ~Connection() {
    if (bev_) {
      bufferevent_free(bev_);
    }
  }

  static void OnRead(struct bufferevent *bev, void *ctx);
  static void OnWrite(struct bufferevent *bev, void *ctx);
  static void OnEvent(bufferevent *bev, short events, void *ctx);
  void Reply(const std::string &msg);
  void SendFile(int fd);

  void SubscribeChannel(std::string &channel);
  void UnSubscribeChannel(std::string &channel);
  void UnSubscribeAll();
  int SubscriptionsCount();

  uint64_t GetAge();
  void SetLastInteraction();
  uint64_t GetIdle();
  void AddFlag(Flag flag);
  void DelFlag(Flag flag);
  bool ExistFlag(Flag flag);

  bool IsAdmin() { return is_admin_; }
  void BecomeAdmin() { is_admin_ = true; }
  void BecomeUser() { is_admin_ = false; }
  std::string GetNamespace() { return ns_; }
  void SetNamespace(std::string ns) { ns_ = ns; }
  void SetID(uint64_t id) { id_ = id; }
  uint64_t GetID() { return id_; }
  void SetLastCmd(std::string cmd) { last_cmd_ = cmd; }
  std::string GetLastCmd() { return last_cmd_; }
  void SetName(std::string name) { name_ = name; }
  std::string GetName() { return name_; };
  void SetAddr(std::string addr) { addr_ = addr; }
  std::string GetAddr() { return addr_; }
  Worker *Owner() { return owner_; }

  int GetFD();
  evbuffer *Input();
  evbuffer *Output();
  bufferevent *GetBufferEvent() { return bev_; }
  bool IsRepl() {
    return this->owner_->IsRepl();
  }
  std::unique_ptr<Commander> current_cmd_;

 private:
  bool is_admin_;
  std::string ns_;
  bufferevent *bev_;
  Request req_;
  Worker *owner_;
  std::vector<std::string> subscribe_channels_;
  uint64_t id_;
  std::string last_cmd_;
  std::string name_ = "";
  std::string addr_ = "";
  std::chrono::time_point<std::chrono::system_clock> create_time_;
  std::chrono::time_point<std::chrono::system_clock> last_interaction_;
  int flags_ = 0;
};

}  // namespace Redis
