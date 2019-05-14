#pragma once

#include <event2/buffer.h>
#include <vector>
#include <string>

#include "worker.h"
#include "redis_request.h"

namespace Redis {
class Request;
class Connection {
 public:
  enum Flag {
    kMonitor = 1 << 5,
    kCloseAfterReply = 1 << 6,
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
  void PSubscribeChannel(const std::string &pattern);
  void PUnSubscribeChannel(const std::string &pattern);
  void PUnSubscribeAll();
  int PSubscriptionsCount();

  uint64_t GetAge();
  uint64_t GetIdleTime();
  void SetLastInteraction();
  std::string GetFlags();
  void EnableFlag(Flag flag);
  bool IsFlagEnabled(Flag flag);
  bool IsRepl() { return this->owner_->IsRepl(); }

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
  std::vector<std::string> subcribe_patterns_;
};
}  // namespace Redis
