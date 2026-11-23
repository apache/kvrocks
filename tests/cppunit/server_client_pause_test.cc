/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <gtest/gtest.h>

#include <chrono>
#include <deque>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "commands/commander.h"
#include "common/time_util.h"
#include "server/redis_connection.h"
#include "server/server.h"
#include "server/worker.h"
#include "test_base.h"
#include "types/redis_string.h"

class ServerClientPauseTest : public TestBase {
 protected:
  void SetUp() override {
    server_ = std::make_unique<Server>(storage_.get(), storage_->GetConfig());
    server_->Stop();
    server_->Join();

    // Create a Worker (port=0 means no TCP listening happens)
    worker_ = std::make_unique<Worker>(server_.get(), storage_->GetConfig());

    // Create a bufferevent pair so we can construct a Connection without a real socket
    base_ = event_base_new();
    ASSERT_NE(base_, nullptr);
    int rc = bufferevent_pair_new(base_, 0, bev_pair_);
    ASSERT_EQ(rc, 0);

    // Connection takes ownership of bev_pair_[0] by default; disable that so
    // we can free the pair ourselves in TearDown.
    conn_ = std::make_unique<redis::Connection>(bev_pair_[0], worker_.get());
    conn_->NeedNotFreeBufferEvent();
  }

  void TearDown() override {
    conn_.reset();
    bufferevent_free(bev_pair_[0]);
    bufferevent_free(bev_pair_[1]);
    event_base_free(base_);
    worker_.reset();
    server_.reset();
  }

  // Returns true if the command is allowed through (not suspended by CLIENT PAUSE).
  bool commandPassesThrough(const std::string &cmd_name, uint64_t cmd_flags) {
    bool suspended = server_->PauseConnIfNeeded(conn_.get(), cmd_name, cmd_flags);
    if (suspended) {
      // clean up: resume and remove from paused list so the test stays isolated
      server_->RemovePausedConn(conn_.get());
      conn_->Unpause();
    }
    return !suspended;
  }

  std::unique_ptr<Server> server_;
  std::unique_ptr<Worker> worker_;
  event_base *base_ = nullptr;
  bufferevent *bev_pair_[2] = {nullptr, nullptr};
  std::unique_ptr<redis::Connection> conn_;
};

// ---------------------------------------------------------------------------
// Group 1: PauseConns / UnpauseConns state management
// ---------------------------------------------------------------------------

TEST_F(ServerClientPauseTest, SetPauseAllMode) {
  uint64_t future_ms = util::GetTimeStampMS() + 60000;
  server_->PauseConns(future_ms, PauseMode::kAll);

  // Write command should be suspended (not pass through).
  EXPECT_FALSE(commandPassesThrough("set", redis::kCmdWrite));
  server_->UnpauseConns();
}

TEST_F(ServerClientPauseTest, UnpauseClearsState) {
  uint64_t future_ms = util::GetTimeStampMS() + 60000;
  server_->PauseConns(future_ms, PauseMode::kAll);
  server_->UnpauseConns();

  // After unpause, commands should pass through immediately.
  EXPECT_TRUE(commandPassesThrough("set", redis::kCmdWrite));
  EXPECT_TRUE(commandPassesThrough("get", redis::kCmdReadOnly));
}

TEST_F(ServerClientPauseTest, SetPauseOverwrite) {
  uint64_t future_ms1 = util::GetTimeStampMS() + 60000;
  server_->PauseConns(future_ms1, PauseMode::kAll);

  // Overwrite with a new pause – command should still be suspended.
  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kAll);
  EXPECT_FALSE(commandPassesThrough("set", redis::kCmdWrite));
  server_->UnpauseConns();
}

// ---------------------------------------------------------------------------
// Group 2: PauseConnIfNeeded exemption rules
// ---------------------------------------------------------------------------

TEST_F(ServerClientPauseTest, NotPausedWhenEndTimeIsZero) {
  // No pause set – any command should pass through immediately.
  EXPECT_TRUE(commandPassesThrough("set", redis::kCmdWrite));
  EXPECT_TRUE(commandPassesThrough("get", redis::kCmdReadOnly));
}

TEST_F(ServerClientPauseTest, ClientCommandIsExempt) {
  // "client" subcommands (PAUSE/UNPAUSE) are exempt from pausing to avoid deadlock.
  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kAll);
  EXPECT_TRUE(commandPassesThrough("client", 0));
  server_->UnpauseConns();
}

TEST_F(ServerClientPauseTest, ReadCommandNotBlockedInWriteMode) {
  // WRITE mode: read-only commands must not be suspended.
  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kWrite);
  EXPECT_TRUE(commandPassesThrough("get", redis::kCmdReadOnly));
  server_->UnpauseConns();
}

TEST_F(ServerClientPauseTest, WriteCommandBlockedInWriteMode) {
  // WRITE mode: write commands should be suspended.
  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kWrite);
  EXPECT_FALSE(commandPassesThrough("set", redis::kCmdWrite));
  server_->UnpauseConns();
}

TEST_F(ServerClientPauseTest, SpecialCommandsBlockedInWriteMode) {
  // WRITE mode: publish/pfcount/wait are also suspended.
  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kWrite);
  for (const auto &cmd : {"publish", "pfcount", "wait"}) {
    EXPECT_FALSE(commandPassesThrough(cmd, 0 /* no write flag, but special */))
        << "Command '" << cmd << "' should be suspended in WRITE mode";
  }
  server_->UnpauseConns();
}

TEST_F(ServerClientPauseTest, AllCommandsBlockedInAllMode) {
  // ALL mode: even read-only commands are suspended.
  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kAll);
  EXPECT_FALSE(commandPassesThrough("get", redis::kCmdReadOnly));
  EXPECT_FALSE(commandPassesThrough("set", redis::kCmdWrite));
  server_->UnpauseConns();
}

// ---------------------------------------------------------------------------
// Group 3: CommandClient::Parse tests
// ---------------------------------------------------------------------------

// Helper: look up the "client" command from the global table and invoke Parse.
static Status ParseClientCommand(const std::vector<std::string> &args) {
  auto *cmd_table = redis::CommandTable::Get();
  auto it = cmd_table->find("client");
  if (it == cmd_table->end()) {
    return {Status::NotOK, "client command not found in table"};
  }
  auto cmd = it->second->factory();
  cmd->SetArgs(args);
  return cmd->Parse(args);
}

TEST_F(ServerClientPauseTest, ParsePauseBasic) {
  auto s = ParseClientCommand({"client", "pause", "1000"});
  EXPECT_TRUE(s.IsOK()) << s.Msg();
}

TEST_F(ServerClientPauseTest, ParsePauseWithAllMode) {
  auto s = ParseClientCommand({"client", "pause", "1000", "ALL"});
  EXPECT_TRUE(s.IsOK()) << s.Msg();
}

TEST_F(ServerClientPauseTest, ParsePauseWithWriteMode) {
  auto s = ParseClientCommand({"client", "pause", "1000", "WRITE"});
  EXPECT_TRUE(s.IsOK()) << s.Msg();
}

TEST_F(ServerClientPauseTest, ParsePauseInvalidMode) {
  auto s = ParseClientCommand({"client", "pause", "1000", "READ"});
  EXPECT_EQ(s.GetCode(), Status::RedisParseErr) << "Expected RedisParseErr for unsupported mode READ";
}

TEST_F(ServerClientPauseTest, ParsePauseMissingTimeout) {
  auto s = ParseClientCommand({"client", "pause"});
  EXPECT_EQ(s.GetCode(), Status::RedisParseErr) << "Expected RedisParseErr when timeout is missing";
}

TEST_F(ServerClientPauseTest, ParsePauseNonIntegerTimeout) {
  auto s = ParseClientCommand({"client", "pause", "abc"});
  EXPECT_EQ(s.GetCode(), Status::RedisParseErr) << "Expected RedisParseErr for non-integer timeout";
}

TEST_F(ServerClientPauseTest, ParsePauseTooManyArgs) {
  auto s = ParseClientCommand({"client", "pause", "1000", "ALL", "EXTRA"});
  EXPECT_EQ(s.GetCode(), Status::RedisParseErr) << "Expected RedisParseErr for too many arguments";
}

TEST_F(ServerClientPauseTest, ParseUnpauseBasic) {
  auto s = ParseClientCommand({"client", "unpause"});
  EXPECT_TRUE(s.IsOK()) << s.Msg();
}

TEST_F(ServerClientPauseTest, ParseUnpauseTooManyArgs) {
  auto s = ParseClientCommand({"client", "unpause", "extra"});
  EXPECT_EQ(s.GetCode(), Status::RedisParseErr) << "Expected RedisParseErr for extra argument to unpause";
}

namespace {
std::string ReadPeerWireReplies(event_base *base, bufferevent *peer_bev) {
  bufferevent_enable(peer_bev, EV_READ);
  event_base_loop(base, EVLOOP_NONBLOCK);
  evbuffer *in_buf = bufferevent_get_input(peer_bev);
  const size_t in_len = evbuffer_get_length(in_buf);
  std::string out(in_len, '\0');
  if (in_len > 0) {
    bufferevent_read(peer_bev, out.data(), in_len);
  }
  return out;
}
}  // namespace

TEST_F(ServerClientPauseTest, ReadOnlyAndUnqueuedExecPassInWriteMode) {
  redis::CommandTable::Reset();
  constexpr uint64_t exec_flags = redis::kCmdExclusive | redis::kCmdBypassMulti | redis::kCmdSlow;

  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kWrite);
  EXPECT_TRUE(commandPassesThrough("exec", exec_flags));

  conn_->EnableFlag(redis::Connection::kMultiExec);
  *conn_->GetMultiExecCommands() = {
      {"GET", "k1"},         {"SCRIPT", "EXISTS", "0123456789abcdef0123456789abcdef01234567"},
      {"HLEN", "h1"},        {"EVAL_RO", "return 1", "0"},
      {"CLIENT", "GETNAME"},
  };
  EXPECT_TRUE(commandPassesThrough("exec", exec_flags));

  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kAll);
  EXPECT_FALSE(commandPassesThrough("exec", exec_flags));

  conn_->ResetMultiExec();
  server_->UnpauseConns();
}

TEST_F(ServerClientPauseTest, WriteDynamicAndConfigPromotedMultiExecBlockedInWriteMode) {
  redis::CommandTable::Reset();
  constexpr uint64_t exec_flags = redis::kCmdExclusive | redis::kCmdBypassMulti | redis::kCmdSlow;
  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kWrite);

  const std::vector<redis::CommandTokens> blocked_cases = {
      {"SET", "k1", "v1"},       {"SCRIPT", "LOAD", "return 1"}, {"HLEN", "h1", "REPAIR"},
      {"EVAL", "return 1", "0"}, {"PUBLISH", "ch", "msg"},       {"PFCOUNT", "hll"},
  };
  for (const auto &tokens : blocked_cases) {
    conn_->ResetMultiExec();
    conn_->EnableFlag(redis::Connection::kMultiExec);
    *conn_->GetMultiExecCommands() = {{"GET", "k0"}, tokens};
    EXPECT_FALSE(commandPassesThrough("exec", exec_flags)) << tokens.front();
    EXPECT_FALSE(commandPassesThrough("EXEC", exec_flags)) << tokens.front();
  }

  auto *cmd_table = redis::CommandTable::Get();
  (*cmd_table)["mypublish"] = (*cmd_table)["publish"];
  cmd_table->erase("publish");
  conn_->ResetMultiExec();
  conn_->EnableFlag(redis::Connection::kMultiExec);
  *conn_->GetMultiExecCommands() = {{"MYPUBLISH", "ch", "msg"}};
  EXPECT_FALSE(commandPassesThrough("exec", exec_flags));
  redis::CommandTable::Reset();

  auto *cfg = storage_->GetConfig();
  const auto orig_encoding = cfg->hash_encoding_mode;
  const auto orig_len_mode = cfg->hash_length_mode;
  cfg->hash_encoding_mode = HashSubkeyEncodingMode::kFieldExpiration;
  cfg->hash_length_mode = HashLengthMode::kAccurate;
  conn_->ResetMultiExec();
  conn_->EnableFlag(redis::Connection::kMultiExec);
  *conn_->GetMultiExecCommands() = {{"HLEN", "h1"}};
  EXPECT_FALSE(commandPassesThrough("exec", exec_flags));
  cfg->hash_length_mode = HashLengthMode::kApproximate;
  EXPECT_TRUE(commandPassesThrough("exec", exec_flags));

  *conn_->GetMultiExecCommands() = {{"CONFIG", "SET", "hash-length-mode", "accurate"}, {"HLEN", "h1"}};
  EXPECT_FALSE(commandPassesThrough("exec", exec_flags));
  *conn_->GetMultiExecCommands() = {{"HLEN", "h1"}, {"CONFIG", "SET", "hash-length-mode", "accurate"}};
  EXPECT_TRUE(commandPassesThrough("exec", exec_flags));

  cfg->hash_encoding_mode = HashSubkeyEncodingMode::kLegacy;
  *conn_->GetMultiExecCommands() = {
      {"CONFIG", "SET", "hash-encoding-mode", "field-expiration"},
      {"CONFIG", "SET", "hash-length-mode", "accurate"},
      {"HLEN", "h1"},
  };
  EXPECT_FALSE(commandPassesThrough("exec", exec_flags));

  cfg->hash_encoding_mode = HashSubkeyEncodingMode::kFieldExpiration;
  cfg->hash_length_mode = HashLengthMode::kAccurate;
  *conn_->GetMultiExecCommands() = {{"CONFIG", "SET", "hash-encoding-mode", "legacy"}, {"HLEN", "h1"}};
  EXPECT_FALSE(commandPassesThrough("exec", exec_flags));
  *conn_->GetMultiExecCommands() = {
      {"HELLO", "3", "AUTH", "default", "pw"},
      {"CONFIG", "SET", "hash-length-mode", "approximate"},
      {"HLEN", "h1"},
  };
  EXPECT_FALSE(commandPassesThrough("exec", exec_flags));

  cfg->hash_encoding_mode = orig_encoding;
  cfg->hash_length_mode = orig_len_mode;
  conn_->ResetMultiExec();

  // In-flight EXEC connections finish atomically without mid-transaction suspension.
  conn_->SetInExec();
  EXPECT_TRUE(commandPassesThrough("set", redis::kCmdWrite));
  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kAll);
  EXPECT_TRUE(commandPassesThrough("get", redis::kCmdReadOnly));
  conn_->ResetMultiExec();
  EXPECT_FALSE(commandPassesThrough("set", redis::kCmdWrite));
  server_->UnpauseConns();
}

TEST_F(ServerClientPauseTest, ExecWaitingOnExclusivityHonorsConcurrentWritePause) {
  redis::CommandTable::Reset();
  conn_->BecomeAdmin();
  conn_->SetNamespace(kDefaultNamespace);
  bufferevent_enable(bev_pair_[1], EV_READ);

  conn_->EnableFlag(redis::Connection::kMultiExec);
  *conn_->GetMultiExecCommands() = {{"SET", "race_pause_k", "v1"}};

  std::deque<redis::CommandTokens> to_process = {{"EXEC"}};
  auto shared_guard = server_->WorkConcurrencyGuard();

  std::promise<void> started_promise;
  auto started_future = started_promise.get_future();
  std::thread exec_worker([&] {
    started_promise.set_value();
    (void)conn_->ExecuteCommands(&to_process);
  });

  started_future.wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  server_->PauseConns(util::GetTimeStampMS() + 60000, PauseMode::kWrite);
  shared_guard.unlock();
  exec_worker.join();

  EXPECT_EQ(ReadPeerWireReplies(base_, bev_pair_[1]), "");
  EXPECT_TRUE(conn_->IsFlagEnabled(redis::Connection::kMultiExec));
  EXPECT_TRUE(conn_->IsPaused());
  ASSERT_EQ(to_process.size(), 1U);
  EXPECT_EQ(to_process.front(), redis::CommandTokens({"EXEC"}));

  redis::String string_db(storage_.get(), kDefaultNamespace);
  engine::Context pre_ctx(storage_.get());
  std::string got_val;
  EXPECT_TRUE(string_db.Get(pre_ctx, "race_pause_k", &got_val).IsNotFound());

  server_->RemovePausedConn(conn_.get());
  server_->UnpauseConns();
  conn_->Unpause();
  (void)conn_->ExecuteCommands(&to_process);
  EXPECT_TRUE(to_process.empty());
  EXPECT_EQ(ReadPeerWireReplies(base_, bev_pair_[1]), "*1\r\n+OK\r\n");

  engine::Context post_ctx(storage_.get());
  ASSERT_TRUE(string_db.Get(post_ctx, "race_pause_k", &got_val).ok());
  EXPECT_EQ(got_val, "v1");
}
