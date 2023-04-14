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
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "server/redis_reply.h"
#include "status.h"
#include "string_util.h"

class Server;

namespace Redis {

class Connection;
struct CommandAttributes;

enum CommandFlags {
  kCmdWrite = 1ULL << 0,        // "write" flag
  kCmdReadOnly = 1ULL << 1,     // "read-only" flag
  kCmdReplication = 1ULL << 2,  // "replication" flag
  kCmdPubSub = 1ULL << 3,       // "pub-sub" flag
  kCmdScript = 1ULL << 4,       // "script" flag
  kCmdLoading = 1ULL << 5,      // "ok-loading" flag
  kCmdMulti = 1ULL << 6,        // "multi" flag
  kCmdExclusive = 1ULL << 7,    // "exclusive" flag
  kCmdNoMulti = 1ULL << 8,      // "no-multi" flag
  kCmdNoScript = 1ULL << 9,     // "no-script" flag
  kCmdROScript = 1ULL << 10,    // "ro-script" flag for read-only script commands
  kCmdCluster = 1ULL << 11,     // "cluster" flag
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

struct CommandKeyRange {
  // index of the first key in command tokens
  // 0 stands for no key, since the first index of command arguments is command name
  int first_key_;

  // index of the last key in command tokens
  // in normal one-key commands, first key and last key index are both 1
  // -n stands for the n-th last index of the sequence, i.e. args.size() - n
  int last_key_;

  // step length of key position
  // e.g. key step 2 means "key other key other ..." sequence
  int key_step_;
};

using CommandKeyRangeGen = std::function<CommandKeyRange(const std::vector<std::string> &)>;

struct CommandAttributes {
  std::string name_;

  // number of command arguments
  // positive number n means number of arguments is equal to n
  // negative number -n means number of arguments is equal to or large than n
  int arity_;

  std::string description_;
  uint64_t flags_;

  CommandKeyRange key_range_;

  // if key_range.first_key == -1, key_range_gen is used instead
  CommandKeyRangeGen key_range_gen_;

  CommanderFactory factory_;

  bool is_write() const { return (flags_ & kCmdWrite) != 0; }
  bool is_ok_loading() const { return (flags_ & kCmdLoading) != 0; }
  bool is_exclusive() const { return (flags_ & kCmdExclusive) != 0; }
  bool is_multi() const { return (flags_ & kCmdMulti) != 0; }
  bool is_no_multi() const { return (flags_ & kCmdNoMulti) != 0; }
};

using CommandMap = std::map<std::string, const CommandAttributes *>;

inline uint64_t ParseCommandFlags(const std::string &description, const std::string &cmd_name) {
  uint64_t flags = 0;

  for (const auto &flag : Util::Split(description, " ")) {
    if (flag == "write")
      flags |= kCmdWrite;
    else if (flag == "read-only")
      flags |= kCmdReadOnly;
    else if (flag == "replication")
      flags |= kCmdReplication;
    else if (flag == "pub-sub")
      flags |= kCmdPubSub;
    else if (flag == "ok-loading")
      flags |= kCmdLoading;
    else if (flag == "exclusive")
      flags |= kCmdExclusive;
    else if (flag == "multi")
      flags |= kCmdMulti;
    else if (flag == "no-multi")
      flags |= kCmdNoMulti;
    else if (flag == "no-script")
      flags |= kCmdNoScript;
    else if (flag == "ro-script")
      flags |= kCmdROScript;
    else if (flag == "cluster")
      flags |= kCmdCluster;
    else {
      std::cout << fmt::format("Encountered non-existent flag '{}' in command {} in command attribute parsing", flag,
                               cmd_name)
                << std::endl;
      std::abort();
    }
  }

  return flags;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, int first_key, int last_key,
                 int key_step) {
  CommandAttributes attr{name,
                         arity,
                         description,
                         ParseCommandFlags(description, name),
                         {first_key, last_key, key_step},
                         {},
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  if ((first_key > 0 && key_step <= 0) || (first_key > 0 && last_key >= 0 && last_key < first_key)) {
    std::cout << fmt::format("Encountered invalid key range in command {}", name) << std::endl;
    std::abort();
  }

  return attr;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, const CommandKeyRangeGen &gen) {
  CommandAttributes attr{name,
                         arity,
                         description,
                         ParseCommandFlags(description, name),
                         {-1, 0, 0},
                         gen,
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

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

#define KVROCKS_CONCAT(a, b) a##b                   // NOLINT
#define KVROCKS_CONCAT2(a, b) KVROCKS_CONCAT(a, b)  // NOLINT

// NOLINTNEXTLINE
#define REDIS_REGISTER_COMMANDS(...) \
  static RegisterToCommandTable KVROCKS_CONCAT2(register_to_command_table_, __LINE__){__VA_ARGS__};

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
