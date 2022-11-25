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

#pragma once

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <rocksdb/types.h>
#include <rocksdb/utilities/backup_engine.h>

#include <deque>
#include <initializer_list>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "server/redis_reply.h"
#include "status.h"
#include "util.h"

class Server;

namespace Redis {

class Connection;
struct CommandAttributes;

enum CommandFlags {
  kCmdWrite = (1ULL << 0),        // "write" flag
  kCmdReadOnly = (1ULL << 1),     // "read-only" flag
  kCmdReplication = (1ULL << 2),  // "replication" flag
  kCmdPubSub = (1ULL << 3),       // "pub-sub" flag
  kCmdScript = (1ULL << 4),       // "script" flag
  kCmdLoading = (1ULL << 5),      // "ok-loading" flag
  kCmdMulti = (1ULL << 6),        // "multi" flag
  kCmdExclusive = (1ULL << 7),    // "exclusive" flag
  kCmdNoMulti = (1ULL << 8),      // "no-multi" flag
  kCmdNoScript = (1ULL << 9),     // "noscript" flag
};

class Commander {
 public:
  void SetAttributes(const CommandAttributes *attributes) { attributes_ = attributes; }
  const CommandAttributes *GetAttributes() { return attributes_; }
  void SetArgs(const std::vector<std::string> &args) { args_ = args; }
  virtual Status Parse() { return Parse(args_); }
  virtual Status Parse(const std::vector<std::string> &args) { return Status::OK(); }
  virtual Status Execute(Server *svr, Connection *conn, std::string *output) {
    return {Status::RedisExecErr, "not implemented"};
  }

  virtual ~Commander() = default;

 protected:
  std::vector<std::string> args_;
  const CommandAttributes *attributes_ = nullptr;
};

class CommanderWithParseMove : Commander {
 public:
  Status Parse() override { return ParseMove(std::move(args_)); }
  virtual Status ParseMove(std::vector<std::string> &&args) { return Status::OK(); }
};

using CommanderFactory = std::function<std::unique_ptr<Commander>()>;

struct CommandAttributes {
  std::string name;
  int arity;
  std::string description;
  uint64_t flags;
  int first_key;
  int last_key;
  int key_step;
  CommanderFactory factory;

  bool is_write() const { return (flags & kCmdWrite) != 0; }
  bool is_ok_loading() const { return (flags & kCmdLoading) != 0; }
  bool is_exclusive() const { return (flags & kCmdExclusive) != 0; }
  bool is_multi() const { return (flags & kCmdMulti) != 0; }
  bool is_no_multi() const { return (flags & kCmdNoMulti) != 0; }
};

using CommandMap = std::map<std::string, const CommandAttributes *>;

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, int first_key, int last_key,
                 int key_step) {
  CommandAttributes attr{
      name,        arity,
      description, 0,
      first_key,   last_key,
      key_step,    []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  for (const auto &flag : Util::Split(attr.description, " ")) {
    if (flag == "write") attr.flags |= kCmdWrite;
    if (flag == "read-only") attr.flags |= kCmdReadOnly;
    if (flag == "replication") attr.flags |= kCmdReplication;
    if (flag == "pub-sub") attr.flags |= kCmdPubSub;
    if (flag == "ok-loading") attr.flags |= kCmdLoading;
    if (flag == "exclusive") attr.flags |= kCmdExclusive;
    if (flag == "multi") attr.flags |= kCmdMulti;
    if (flag == "no-multi") attr.flags |= kCmdNoMulti;
    if (flag == "no-script") attr.flags |= kCmdNoScript;
  }

  return attr;
}

struct RegisterToCommandTable {
  RegisterToCommandTable(std::initializer_list<CommandAttributes> list);
};

// these variables cannot be put into source files (to ensure init order for multiple TUs)
namespace command_details {
inline std::deque<CommandAttributes> redis_command_table;

// Original Command table before rename-command directive
inline CommandMap original_commands;

// Command table after rename-command directive
inline CommandMap commands;
}  // namespace command_details

// NOLINTNEXTLINE
#define REGISTER_COMMANDS(...) static RegisterToCommandTable register_to_command_table_##__LINE__{__VA_ARGS__};

int GetCommandNum();
CommandMap *GetCommands();
void ResetCommands();
const CommandMap *GetOriginalCommands();
void GetAllCommandsInfo(std::string *info);
void GetCommandsInfo(std::string *info, const std::vector<std::string> &cmd_names);
std::string GetCommandInfo(const CommandAttributes *command_attributes);
Status GetKeysFromCommand(const std::string &name, int argc, std::vector<int> *keys_indexes);
bool IsCommandExists(const std::string &name);

}  // namespace Redis
