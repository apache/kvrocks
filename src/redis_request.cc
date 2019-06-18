#include <glog/logging.h>
#include <chrono>
#include <utility>

#include "util.h"
#include "redis_cmd.h"
#include "redis_reply.h"
#include "redis_request.h"
#include "redis_connection.h"
#include "server.h"

namespace Redis {
void Request::Tokenize(evbuffer *input) {
  char *line;
  size_t len;
  while (true) {
    switch (state_) {
      case ArrayLen:
        line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return;
        svr_->stats_.IncrInbondBytes(len);
        multi_bulk_len_ = len > 0 ? std::strtoull(line + 1, nullptr, 10) : 0;
        free(line);
        state_ = BulkLen;
        break;
      case BulkLen:
        line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return;
        svr_->stats_.IncrInbondBytes(len);
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
        svr_->stats_.IncrInbondBytes(bulk_len_ + 2);
        --multi_bulk_len_;
        if (multi_bulk_len_ == 0) {
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

bool Request::inCommandWhitelist(const std::string &command) {
  std::vector<std::string> whitelist = {"auth"};
  for (const auto &allow_command : whitelist) {
    if (allow_command == command) return true;
  }
  return false;
}

void Request::ExecuteCommands(Connection *conn) {
  if (commands_.empty()) return;

  Config *config = svr_->GetConfig();
  std::string reply;
  for (auto &cmd_tokens : commands_) {
    if (conn->IsFlagEnabled(Redis::Connection::kCloseAfterReply)) break;
    if (conn->GetNamespace().empty()
        && Util::ToLower(cmd_tokens.front()) != "auth") {
      conn->Reply(Redis::Error("NOAUTH Authentication required."));
      continue;
    }
    auto s = LookupCommand(cmd_tokens.front(), &conn->current_cmd_, conn->IsRepl());
    if (!s.IsOK()) {
      conn->Reply(Redis::Error("ERR unknown command"));
      continue;
    }
    if (svr_->IsLoading() && !inCommandWhitelist(conn->current_cmd_->Name())) {
      conn->Reply(Redis::Error("ERR restoring the db from backup"));
      break;
    }
    int arity = conn->current_cmd_->GetArity();
    int tokens = static_cast<int>(cmd_tokens.size());
    if ((arity > 0 && tokens != arity)
        || (arity < 0 && tokens < -arity)) {
      conn->Reply(Redis::Error("ERR wrong number of arguments"));
      continue;
    }
    conn->current_cmd_->SetArgs(cmd_tokens);
    s = conn->current_cmd_->Parse(cmd_tokens);
    if (!s.IsOK()) {
      conn->Reply(Redis::Error(s.Msg()));
      continue;
    }
    if (config->slave_readonly && svr_->IsSlave() && conn->current_cmd_->IsWrite()) {
      conn->Reply(Redis::Error("READONLY You can't write against a read only slave."));
      continue;
    }
    conn->SetLastCmd(conn->current_cmd_->Name());

    svr_->stats_.IncrCalls(conn->current_cmd_->Name());
    auto start = std::chrono::high_resolution_clock::now();
    svr_->IncrExecutingCommandNum();
    s = conn->current_cmd_->Execute(svr_, conn, &reply);
    svr_->DecrExecutingCommandNum();
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
    svr_->SlowlogPushEntryIfNeeded(conn->current_cmd_->Args(), static_cast<uint64_t>(duration));
    svr_->stats_.IncrLatency(static_cast<uint64_t>(duration), conn->current_cmd_->Name());
    svr_->FeedMonitorConns(conn, cmd_tokens);
    if (!s.IsOK()) {
      conn->Reply(Redis::Error("ERR " + s.Msg()));
      LOG(ERROR) << "[request] Failed to execute command: " << conn->current_cmd_->Name()
                 << ", encounter err: " << s.Msg();
      continue;
    }
    if (!reply.empty()) conn->Reply(reply);
  }
  commands_.clear();
}

}  // namespace Redis
