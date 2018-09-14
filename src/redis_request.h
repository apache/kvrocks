#pragma once

#include <event2/buffer.h>
#include <list>
#include <string>

#include "redis_cmd.h"
#include "storage.h"
#include "status.h"
#include "server.h"

namespace Redis {

class Connection {
 public:
  explicit Connection(bufferevent *bev) : bev_(bev) {}
  ~Connection() { if (bev_) bufferevent_free(bev_); }

  static void onRead(struct bufferevent *bev, void *ctx);
  static void onWrite(struct bufferevent *bev, void *ctx);
  static void onEvent(bufferevent *bev, short events, void *ctx);

  evbuffer *Input();
  evbuffer *Output();
  bufferevent *DetachBufferEvent() {
    auto tmp = bev_;
    bev_ = nullptr;
    return tmp;
  }

 private:
  bufferevent *bev_;
  int GetFD();
};

class Request {
  friend Connection;

 public:
  explicit Request(Server *svr, std::unique_ptr<Connection> &&conn)
      : svr_(svr), conn_(std::move(conn)) {}
  // Not copyable
  Request(const Request &) = delete;
  Request &operator=(const Request &) = delete;

  // Parse the redis requests (bulk string array format)
  void Tokenize(evbuffer *input);
  // Exec return true when command finished
  void ExecuteCommands(evbuffer *output);

 private:
  // internal states related to parsing

  enum ParserState {
    ArrayLen,
    BulkLen,
    BulkData
  };
  ParserState state_ = ArrayLen;
  size_t array_len_ = 0;
  size_t buck_len_ = 0;
  using CommandTokens = std::list<std::string>;
  CommandTokens tokens_;
  std::vector<CommandTokens> commands_;

  Server *svr_;
  std::unique_ptr<Connection> conn_;
};

}  // namespace Redis
