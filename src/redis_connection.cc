#include "redis_connection.h"

#include <glog/logging.h>
#include "worker.h"
#include "server.h"

namespace Redis {

Connection::Connection(bufferevent *bev, Worker *owner)
    : bev_(bev), req_(owner->svr_), owner_(owner) {
  time_t now;
  time(&now);
  create_time_ = now;
  last_interaction_ = now;
}

Connection::~Connection() {
  if (bev_) { bufferevent_free(bev_); }
  // unscribe all channels and patterns if exists
  UnSubscribeAll();
  PUnSubscribeAll();
}

std::string Connection::ToString() {
  std::ostringstream stream;
  stream << "id=" << id_
    << " addr=" << addr_
    << " fd=" << bufferevent_getfd(bev_)
    << " name=" << name_
    << " age=" << GetAge()
    << " idle=" << GetIdleTime()
    << " flags=" << GetFlags()
    << " namespace=" << ns_
    << " qbuf=" << evbuffer_get_length(Input())
    << " obuf=" << evbuffer_get_length(Output())
    << " cmd=" << last_cmd_
    << "\n";
  return stream.str();
}

void Connection::Close() {
  owner_->FreeConnection(this);
}

void Connection::Detach() {
  owner_->DetachConnection(this);
}

void Connection::OnRead(struct bufferevent *bev, void *ctx) {
  DLOG(INFO) << "[connection] on read: " << bufferevent_getfd(bev);
  auto conn = static_cast<Connection *>(ctx);

  conn->SetLastInteraction();
  auto s = conn->req_.Tokenize(conn->Input());
  if (!s.IsOK()) {
    conn->EnableFlag(Redis::Connection::kCloseAfterReply);
    conn->Reply(Redis::Error(s.Msg()));
    LOG(INFO) << "Failed to tokenize the request, encounter error: " << s.Msg();
    return;
  }
  conn->req_.ExecuteCommands(conn);
}

void Connection::OnWrite(struct bufferevent *bev, void *ctx) {
  auto conn = static_cast<Connection *>(ctx);
  if (conn->IsFlagEnabled(kCloseAfterReply)) {
    conn->Close();
  }
}

void Connection::OnEvent(bufferevent *bev, int16_t events, void *ctx) {
  auto conn = static_cast<Connection *>(ctx);
  if (events & BEV_EVENT_ERROR) {
    LOG(ERROR) << "[connection] Going to remove the client: " << conn->GetAddr()
               << ", while encounter error: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    conn->Close();
    return;
  }
  if (events & BEV_EVENT_EOF) {
    DLOG(INFO) << "[connection] Going to remove the client: " << conn->GetAddr()
               << ", while closed by client";
    conn->Close();
    return;
  }
  if (events & BEV_EVENT_TIMEOUT) {
    DLOG(INFO) << "[connection] The client: " << conn->GetAddr()  << "] reached timeout";
    bufferevent_enable(bev, EV_READ | EV_WRITE);
  }
}

void Connection::Reply(const std::string &msg) {
  owner_->svr_->stats_.IncrOutbondBytes(msg.size());
  Redis::Reply(bufferevent_get_output(bev_), msg);
}

void Connection::SendFile(int fd) {
  // NOTE: we don't need to close the fd, the libevent will do that
  auto output = bufferevent_get_output(bev_);
  evbuffer_add_file(output, fd, 0, -1);
}

void Connection::SetAddr(std::string ip, int port) {
  ip_ = std::move(ip);
  port_ = port;
  addr_ = ip_ +":"+ std::to_string(port_);
}

uint64_t Connection::GetAge() {
  time_t now;
  time(&now);
  return static_cast<uint64_t>(now-create_time_);
}

void Connection::SetLastInteraction() {
  time(&last_interaction_);
}

uint64_t Connection::GetIdleTime() {
  time_t now;
  time(&now);
  return static_cast<uint64_t>(now-last_interaction_);
}

std::string Connection::GetFlags() {
  std::string flags;
  if (owner_->IsRepl()) flags.append("R");
  if (IsFlagEnabled(kSlave)) flags.append("S");
  if (IsFlagEnabled(kCloseAfterReply)) flags.append("c");
  if (IsFlagEnabled(kMonitor)) flags.append("M");
  if (!subscribe_channels_.empty()) flags.append("P");
  if (flags.empty()) flags = "N";
  return flags;
}

void Connection::EnableFlag(Flag flag) {
  flags_ |= flag;
}

bool Connection::IsFlagEnabled(Flag flag) {
  return (flags_ & flag) > 0;
}

bool Connection::IsRepl() {
  return owner_->IsRepl();
}

void Connection::SubscribeChannel(const std::string &channel) {
  for (const auto &chan : subscribe_channels_) {
    if (channel == chan) return;
  }
  subscribe_channels_.emplace_back(channel);
  owner_->svr_->SubscribeChannel(channel, this);
}

void Connection::UnSubscribeChannel(const std::string &channel) {
  auto iter = subscribe_channels_.begin();
  for (; iter != subscribe_channels_.end(); iter++) {
    if (*iter == channel) {
      subscribe_channels_.erase(iter);
      owner_->svr_->UnSubscribeChannel(channel, this);
      return;
    }
  }
}

void Connection::UnSubscribeAll(unsubscribe_callback reply) {
  if (subscribe_channels_.empty()) {
    if (reply != nullptr) reply("", subcribe_patterns_.size());
    return;
  }
  int removed = 0;
  for (const auto &chan : subscribe_channels_) {
    owner_->svr_->UnSubscribeChannel(chan, this);
    removed++;
    if (reply != nullptr) {
      reply(chan, static_cast<int>(subscribe_channels_.size() -
                                   removed + subcribe_patterns_.size()));
    }
  }
  subscribe_channels_.clear();
}

int Connection::SubscriptionsCount() {
  return static_cast<int>(subscribe_channels_.size());
}

void Connection::PSubscribeChannel(const std::string &pattern) {
  for (const auto &p : subcribe_patterns_) {
    if (pattern == p) return;
  }
  subcribe_patterns_.emplace_back(pattern);
  owner_->svr_->PSubscribeChannel(pattern, this);
}

void Connection::PUnSubscribeChannel(const std::string &pattern) {
  auto iter = subcribe_patterns_.begin();
  for (; iter != subcribe_patterns_.end(); iter++) {
    if (*iter == pattern) {
      subcribe_patterns_.erase(iter);
      owner_->svr_->PUnSubscribeChannel(pattern, this);
      return;
    }
  }
}

void Connection::PUnSubscribeAll(unsubscribe_callback reply) {
  if (subcribe_patterns_.empty()) {
    if (reply != nullptr) reply("", subscribe_channels_.size());
    return;
  }

  int removed = 0;
  for (const auto &pattern : subcribe_patterns_) {
    owner_->svr_->PUnSubscribeChannel(pattern, this);
    removed++;
    if (reply != nullptr) {
      reply(pattern, static_cast<int>(subcribe_patterns_.size() -
                                      removed + subscribe_channels_.size()));
    }
  }
  subcribe_patterns_.clear();
}

int Connection::PSubscriptionsCount() {
  return static_cast<int>(subcribe_patterns_.size());
}
}  // namespace Redis
