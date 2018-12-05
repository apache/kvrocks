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
  explicit Connection(bufferevent *bev, Worker *owner) : bev_(bev), req_(owner->svr_), owner_(owner) {}
  ~Connection() {
    if (bev_) {
      owner_->RemoveConnection(bufferevent_getfd(bev_));
      bufferevent_free(bev_);
    }
  }

  static void OnRead(struct bufferevent *bev, void *ctx);
  static void OnEvent(bufferevent *bev, short events, void *ctx);
  void Reply(const std::string &msg);

  void SubscribeChannel(std::string &channel);
  void UnSubscribeChannel(std::string &channel);
  void UnSubscribeAll();
  int SubscriptionsCount();

  bool IsAdmin() { return is_admin_; }
  void BecomeAdmin() { is_admin_ = true; }
  void BecomeUser() { is_admin_ = false; }
  std::string GetNamespace() { return ns_; };
  void SetNamespace(std::string ns) { ns_ = ns; };
  int GetFD();
  evbuffer *Input();
  evbuffer *Output();
  bufferevent *GetBufferEvent() { return bev_; }

 private:
  bool is_admin_;
  std::string ns_;
  bufferevent *bev_;
  Request req_;
  Worker *owner_;
  std::vector<std::string> subscribe_channels_;
};

}  // namespace Redis
