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

#include "commander.h"
#include "commands/command_parser.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/json.h"
#include "types/redis_json.h"

namespace redis {

///
/// JSON.GET <key>
///         [path ...]
///
class CommandJsonGet final : public Commander {
 public:
  inline static constexpr std::string_view CMD_ARG_NOESCAPE = "noescape";
  inline static constexpr std::string_view CMD_ARG_INDENT = "indent";
  inline static constexpr std::string_view CMD_ARG_NEWLINE = "newline";
  inline static constexpr std::string_view CMD_ARG_SPACE = "space";
  inline static constexpr std::string_view CMD_ARG_FORMAT = "format";

  inline static constexpr size_t JSONGET_SUBCOMMANDS_MAXSTRLEN = 8;

  inline static const std::unordered_set<std::string_view> INTERNAL_COMMANDS = {
      CMD_ARG_NOESCAPE, CMD_ARG_INDENT, CMD_ARG_NEWLINE, CMD_ARG_SPACE, CMD_ARG_FORMAT};

  Status Parse(const std::vector<std::string> &args) override {
    // RedisJson support JSON.GET like:
    // ```
    // JSON.GET <key>
    //         [INDENT indentation-string]
    //         [NEWLINE line-break-string]
    //         [SPACE space-string]
    //         [path ...]
    // ```
    // However, we don't support FORMAT, INDENT, NEWLINE and SPACE here.
    for (size_t i = 2; i < args_.size(); ++i) {
      if (args_[i].size() < JSONGET_SUBCOMMANDS_MAXSTRLEN) {
        auto lower = util::ToLower(args_[i]);
        if (INTERNAL_COMMANDS.find(lower) != INTERNAL_COMMANDS.end()) {
          if (lower == CMD_ARG_NOESCAPE) {
            continue;
          }
          return {Status::RedisParseErr, fmt::format("{} not support", args_[i])};
        }
      }
      auto json_path = JsonPath::BuildJsonPath(args_[i]);
      if (!json_path.IsOK()) {
        return json_path.ToStatus();
      }
      json_paths_.push_back(std::move(json_path.GetValue()));
    }
    // JSON.GET <key> is regard as get "$".
    if (json_paths_.empty()) {
      json_paths_.push_back(JsonPath::BuildJsonRootPath());
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    const auto &key = args_[1];

    redis::RedisJson redis_json(svr->storage, conn->GetNamespace());
    rocksdb::Status s = redis_json.JsonGet(key, json_paths_, output);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    return Status::OK();
  }

 private:
  std::vector<JsonPath> json_paths_;
};

///
/// JSON.SET <key> <path> <json> [NX | XX | FORMAT <format>]
///
class CommandJsonSet final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 4);
    while (parser.Good()) {
      if (parser.EatEqICase("NX")) {
        if (set_flags_ != JsonSetFlags::kNone) {
          return Status::RedisParseErr;
        }
        set_flags_ = JsonSetFlags::kJsonSetNX;
      } else if (parser.EatEqICase("XX")) {
        if (set_flags_ != JsonSetFlags::kNone) {
          return Status::RedisParseErr;
        }
        set_flags_ = JsonSetFlags::kJsonSetXX;
      } else if (parser.EatEqICase("FORMAT")) {
        return {Status::RedisParseErr, fmt::format("FORMAT not support")};
      } else {
        // "ERR syntax error"
        return parser.InvalidSyntax();
      }
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    auto json_path = JsonPath::BuildJsonPath(args_[2]);
    if (!json_path.IsOK()) {
      return json_path.ToStatus();
    }
    redis::RedisJson redis_json(svr->storage, conn->GetNamespace());
    bool set_success = false;
    rocksdb::Status s = redis_json.JsonSet(args_[1], json_path.GetValue(), args_[3], set_flags_, &set_success);
    if (!s.ok()) {
      return Status::FromErrno(s.ToString());
    }
    if (!set_success) {
      *output = redis::NilString();
    } else {
      *output = redis::SimpleString("OK");
    }
    return Status::OK();
  }

 private:
  JsonSetFlags set_flags_ = JsonSetFlags::kNone;
};

///
/// JSON.DEL <key> [path]
///
class CommandJsonDel final : public Commander {
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string_view json_path_string;
    if (args_.size() < 2) {
      json_path_string = JsonPath::ROOT_PATH;
    } else {
      json_path_string = args_[2];
    }

    auto json_path = JsonPath::BuildJsonPath(std::string(json_path_string));
    if (!json_path.IsOK()) {
      return {Status::RedisParseErr, fmt::format("PARSE {} failed", json_path_string)};
    }

    redis::RedisJson redis_json(svr->storage, conn->GetNamespace());
    rocksdb::Status s = redis_json.JsonDel(args_[1], json_path.GetValue());
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    return Status::OK();
  }
};

class CommandJsonType final : public Commander {
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    // NYI
    __builtin_unreachable();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandJsonDel>("json.del", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonGet>("json.get", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandJsonSet>("json.set", -4, "write", 1, -2, 1),
                        MakeCmdAttr<CommandJsonType>("json.type", 3, "read-only", 1, 1, 1), )

}  // namespace redis