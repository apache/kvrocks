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

#include <event2/bufferevent.h>
#include <gtest/gtest.h>

#include <memory>

#include "commands/commander.h"
#include "common/time_util.h"
#include "server/redis_connection.h"
#include "server/server.h"
#include "server/worker.h"
#include "test_base.h"

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
