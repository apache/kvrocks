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

#include <list>
#include <map>
#include <string>
#include <vector>
#include <thread>
#include <utility>
#include <memory>

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <rocksdb/types.h>
#include <rocksdb/utilities/backup_engine.h>

#include "redis_reply.h"
#include "status.h"

class Server;
namespace Redis {

class Connection;
struct CommandAttributes;

enum CommandFlags {
  kCmdWrite         = (1ULL<<0),  // "write" flag
  kCmdReadOnly      = (1ULL<<1),  // "read-only" flag
  kCmdReplication   = (1ULL<<2),  // "replication" flag
  kCmdPubSub        = (1ULL<<3),  // "pub-sub" flag
  kCmdScript        = (1ULL<<4),  // "script" flag
  kCmdLoading       = (1ULL<<5),  // "ok-loading" flag
  kCmdMulti         = (1ULL<<6),  // "multi" flag
  kCmdExclusive     = (1ULL<<7),  // "exclusive" flag
  kCmdNoMulti       = (1ULL<<8),  // "no-multi" flag
  kCmdNoScript      = (1ULL<<9),  // "noscript" flag
};

class Commander {
 public:
  void SetAttributes(const CommandAttributes *attributes) { attributes_ = attributes; }
  const CommandAttributes* GetAttributes() { return attributes_; }
  void SetArgs(const std::vector<std::string> &args) { args_ = args; }
  const std::vector<std::string>* Args() {
    return &args_;
  }
  virtual Status Parse(const std::vector<std::string> &args) {
    return Status::OK();
  }
  virtual Status Execute(Server *svr, Connection *conn, std::string *output) {
    return Status(Status::RedisExecErr, "not implemented");
  }

  virtual ~Commander() = default;

 protected:
  std::vector<std::string> args_;
  const CommandAttributes *attributes_ = nullptr;
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

int GetCommandNum();
std::map<std::string, CommandAttributes *> *GetCommands();
std::map<std::string, CommandAttributes *> *GetOriginalCommands();
void InitCommandsTable();
void PopulateCommands();
void GetAllCommandsInfo(std::string *info);
void GetCommandsInfo(std::string *info, const std::vector<std::string> &cmd_names);
std::string GetCommandInfo(const CommandAttributes *command_attributes);
Status GetKeysFromCommand(const std::string &name, int argc, std::vector<int> *keys_indexes);
bool IsCommandExists(const std::string &name);
}  // namespace Redis
