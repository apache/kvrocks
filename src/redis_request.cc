#include <glog/logging.h>
#include <iostream>

#include "string_util.h"
#include "redis_cmd.h"
#include "redis_reply.h"
#include "redis_request.h"
#include "storage.h"

namespace Redis {

void Connection::OnRead(struct bufferevent *bev, void *ctx) {
  DLOG(INFO) << "on read: " << bufferevent_getfd(bev);
  auto conn = static_cast<Connection *>(ctx);

  conn->req_.Tokenize(conn->Input());
  conn->req_.ExecuteCommands(conn);
}

void Connection::OnEvent(bufferevent *bev, short events, void *ctx) {
  auto conn = static_cast<Connection *>(ctx);
  if (events & BEV_EVENT_ERROR) {
    LOG(ERROR) << "bev error: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
  }
  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    DLOG(INFO) << "deleted: fd=" << conn->GetFD();
    delete conn;
    return;
  }
  if (events & BEV_EVENT_TIMEOUT) {
    LOG(INFO) << "timeout, fd=" << conn->GetFD();
    bufferevent_enable(bev, EV_READ | EV_WRITE);
  }
}

void Connection::Reply(const std::string &msg) {
  owner_->svr_->stats_.AddOutbondBytes(msg.size());
  Redis::Reply(bufferevent_get_output(bev_), msg);
}

int Connection::GetFD() { return bufferevent_getfd(bev_); }

evbuffer *Connection::Input() { return bufferevent_get_input(bev_); }

evbuffer *Connection::Output() { return bufferevent_get_output(bev_); }

void Connection::SubscribeChannel(std::string &channel) {
  for (const auto &chan : subscribe_channels_) {
    if (channel == chan) return;
  }
  subscribe_channels_.emplace_back(channel);
  owner_->svr_->SubscribeChannel(channel, this);
}

void Connection::UnSubscribeChannel(std::string &channel) {
  auto iter = subscribe_channels_.begin();
  for (; iter != subscribe_channels_.end(); iter++) {
    if (*iter == channel) {
      subscribe_channels_.erase(iter);
      owner_->svr_->UnSubscribeChannel(channel, this);
    }
  }
}

void Connection::UnSubscribeAll() {
  if (subscribe_channels_.empty()) return;
  for (auto chan : subscribe_channels_) {
    owner_->svr_->UnSubscribeChannel(chan, this);
  }
  subscribe_channels_.clear();
}

int Connection::SubscriptionsCount() {
  return static_cast<int>(subscribe_channels_.size());
}

void Request::Tokenize(evbuffer *input) {
  char *line;
  size_t len;
  while (true) {
    switch (state_) {
      case ArrayLen:
        line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return;
        svr_->stats_.AddInbondBytes(len);
        multi_bulk_len_ = len > 0 ? std::strtoull(line + 1, nullptr, 10) : 0;
        free(line);
        state_ = BulkLen;
        break;
      case BulkLen:
        line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return;
        svr_->stats_.AddInbondBytes(len);
        bulk_len_ = std::strtoull(line + 1, nullptr, 10);
        free(line);
        state_ = BulkData;
        break;
      case BulkData:
        if (evbuffer_get_length(input) < bulk_len_ + 2) return;
        char *data =
            reinterpret_cast<char *>(evbuffer_pullup(input, bulk_len_ + 2));
        tokens_.emplace_back(data, bulk_len_);
        evbuffer_drain(input, bulk_len_ + 2);
        svr_->stats_.AddInbondBytes(bulk_len_ + 2);
        --multi_bulk_len_;
        if (multi_bulk_len_ <= 0) {
          state_ = ArrayLen;
          commands_.push_back(std::move(tokens_));
          tokens_.clear();
        } else {
          state_ = BulkLen;
        }
        break;
    }
  }
}

void Request::ExecuteCommands(Connection *conn) {
  if (commands_.empty()) return;

  if (svr_->IsLoading()) {
    conn->Reply(Redis::Error("replication in progress"));
    return;
  }

  Config *config = svr_->GetConfig();
  std::unique_ptr<Commander> cmd;
  std::string reply;
  for (auto &cmd_tokens : commands_) {
    if (conn->GetNamespace().empty()
        && Util::ToLower(cmd_tokens.front()) != "auth") {
      conn->Reply(Redis::Error("NOAUTH Authentication required."));
      continue;
    }
    auto s = LookupCommand(cmd_tokens.front(), &cmd);
    if (!s.IsOK()) {
      // FIXME: change the err string
      conn->Reply(Redis::Error("unknown command"));
      continue;
    }
    int arity = cmd->GetArity();
    int tokens = static_cast<int>(cmd_tokens.size());
    if ((arity > 0 && tokens != arity)
        || (arity < 0 && tokens < -arity)) {
      conn->Reply(Redis::Error("wrong number of arguments"));
      continue;
    }
    cmd->SetArgs(cmd_tokens);
    s = cmd->Parse(cmd_tokens);
    if (!s.IsOK()) {
      conn->Reply(Redis::Error(s.Msg()));
      continue;
    }
    if (config->slave_readonly && svr_->IsSlave() && cmd->IsWrite()) {
      conn->Reply(Redis::Error("READONLY You can't write against a read only slave."));
      continue;
    }

    svr_->stats_.IncrCalls();
    if (!cmd->IsSidecar()) {
      s = cmd->Execute(svr_, conn, &reply);
      if (!s.IsOK()) {
        conn->Reply(Redis::Error(s.Msg()));
        LOG(ERROR) << "Failed to execute redis command: " << cmd->Name()
                   << ", err: " << s.Msg();
        continue;
      }
      if (!reply.empty()) conn->Reply(reply);
    } else {
      auto t = new SidecarCommandThread(std::move(cmd), svr_, conn);
      t->Start();
      // TODO: track this thread in Worker class; delete the req and sidecar obj
      // when done.
      return;  // NOTE: we break out the pipeline, even some commands left
    }
  }
  commands_.clear();
}

}  // namespace Redis
