#include <chrono>
#include <utility>
#include <glog/logging.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/iostats_context.h>

#include "util.h"
#include "redis_cmd.h"
#include "redis_reply.h"
#include "redis_request.h"
#include "redis_connection.h"
#include "server.h"

namespace Redis {
const size_t PROTO_INLINE_MAX_SIZE = 16 * 1024L;
const size_t PROTO_BULK_MAX_SIZE = 512 * 1024L * 1024L;
const size_t PROTO_MULTI_MAX_SIZE = 1024 * 1024L;

Status Request::Tokenize(evbuffer *input) {
  char *line;
  size_t len;
  size_t pipeline_size = 0;
  while (true) {
    switch (state_) {
      case ArrayLen:
        line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line || len <= 0) {
          if (pipeline_size > 128) {
            LOG(INFO) << "Large pipeline detected: " << pipeline_size;
          }
          if (line) {
            free(line);
            continue;
          }
          return Status::OK();
        }
        pipeline_size++;
        svr_->stats_.IncrInbondBytes(len);
        if (line[0] == '*') {
          try {
            multi_bulk_len_ = std::stoull(std::string(line + 1, len-1));
          } catch (std::exception &e) {
            free(line);
            return Status(Status::NotOK, "Protocol error: invalid multibulk length");
          }
          if (multi_bulk_len_ > PROTO_MULTI_MAX_SIZE) {
            free(line);
            return Status(Status::NotOK, "Protocol error: invalid multibulk length");
          }
          state_ = BulkLen;
        } else {
          if (len > PROTO_INLINE_MAX_SIZE) {
            free(line);
            return Status(Status::NotOK, "Protocol error: invalid bulk length");
          }
          Util::Split(std::string(line, len), " \t", &tokens_);
          commands_.push_back(std::move(tokens_));
          state_ = ArrayLen;
        }
        free(line);
        break;
      case BulkLen:
        line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line || len <= 0) return Status::OK();
        svr_->stats_.IncrInbondBytes(len);
        if (line[0] != '$') {
          free(line);
          return Status(Status::NotOK, "Protocol error: expected '$'");
        }
        try {
          bulk_len_ = std::stoull(std::string(line + 1, len-1));
        } catch (std::exception &e) {
          free(line);
          return Status(Status::NotOK, "Protocol error: invalid bulk length");
        }
        if (bulk_len_ > PROTO_BULK_MAX_SIZE) {
          free(line);
          return Status(Status::NotOK, "Protocol error: invalid bulk length");
        }
        free(line);
        state_ = BulkData;
        break;
      case BulkData:
        if (evbuffer_get_length(input) < bulk_len_ + 2) return Status::OK();
        char *data = reinterpret_cast<char *>(evbuffer_pullup(input, bulk_len_ + 2));
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

bool Request::isProfilingEnabled(const std::string &cmd) {
  auto config = svr_->GetConfig();
  if (config->profiling_sample_ratio == 0) return false;
  if (!config->profiling_sample_all_commands &&
      config->profiling_sample_commands.find(cmd) == config->profiling_sample_commands.end()) {
    return false;
  }
  if (config->profiling_sample_ratio == 100 ||
      std::rand() % 100 <= config->profiling_sample_ratio) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();
    return true;
  }
  return false;
}

void Request::recordProfilingSampleIfNeed(const std::string &cmd, uint64_t duration) {
  int threshold = svr_->GetConfig()->profiling_sample_record_threshold_ms;
  if (threshold > 0 && static_cast<int>(duration/1000) < threshold) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    return;
  }

  std::string perf_context = rocksdb::get_perf_context()->ToString(true);
  std::string iostats_context = rocksdb::get_iostats_context()->ToString(true);
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  if (perf_context.empty()) return;  // request without db operation
  auto entry = new PerfEntry();
  entry->cmd_name = cmd;
  entry->duration = duration;
  entry->iostats_context = std::move(iostats_context);
  entry->perf_context = std::move(perf_context);
  svr_->GetPerfLog()->PushEntry(entry);
}

void Request::ExecuteCommands(Connection *conn) {
  if (commands_.empty()) return;

  Config *config = svr_->GetConfig();
  std::string reply, password;
  password = conn->IsRepl() ? config->masterauth : config->requirepass;
  for (auto &cmd_tokens : commands_) {
    if (conn->IsFlagEnabled(Redis::Connection::kCloseAfterReply)) break;
    if (conn->GetNamespace().empty()) {
      if (!password.empty() && Util::ToLower(cmd_tokens.front()) != "auth") {
        conn->Reply(Redis::Error("NOAUTH Authentication required."));
        continue;
      }
      if (password.empty()) {
        conn->BecomeAdmin();
        conn->SetNamespace(kDefaultNamespace);
      }
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
    auto cmd_name = conn->current_cmd_->Name();
    if (!config->slave_serve_stale_data && svr_->IsSlave()
        && cmd_name != "info" && cmd_name != "slaveof"
        && svr_->GetReplicationState() != kReplConnected) {
      conn->Reply(Redis::Error("MASTERDOWN Link with MASTER is down "
                               "and slave-serve-stale-data is set to 'no'."));
      continue;
    }
    conn->SetLastCmd(cmd_name);
    svr_->stats_.IncrCalls(cmd_name);
    auto start = std::chrono::high_resolution_clock::now();
    bool is_profiling = isProfilingEnabled(cmd_name);
    svr_->IncrExecutingCommandNum();
    s = conn->current_cmd_->Execute(svr_, conn, &reply);
    svr_->DecrExecutingCommandNum();
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
    if (is_profiling) recordProfilingSampleIfNeed(cmd_name, duration);
    svr_->SlowlogPushEntryIfNeeded(conn->current_cmd_->Args(), duration);
    svr_->stats_.IncrLatency(static_cast<uint64_t>(duration), cmd_name);
    svr_->FeedMonitorConns(conn, cmd_tokens);
    if (!s.IsOK()) {
      conn->Reply(Redis::Error("ERR " + s.Msg()));
      continue;
    }
    if (!reply.empty()) conn->Reply(reply);
    reply.clear();
  }
  commands_.clear();
}

}  // namespace Redis
