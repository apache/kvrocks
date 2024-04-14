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

#include "commander.h"
#include "commands/command_parser.h"
#include "error_constants.h"
#include "parse_util.h"
#include "server/server.h"

namespace redis {

inline constexpr const char *kCursorPrefix = "_";

class CommandScanBase : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);

    PutCursor(GET_OR_RET(parser.TakeStr()));

    return ParseAdditionalFlags<true>(parser);
  }

  template <bool IsScan, typename Parser>
  Status ParseAdditionalFlags(Parser &parser) {
    while (parser.Good()) {
      if (parser.EatEqICase("match")) {
        prefix_ = GET_OR_RET(parser.TakeStr());
        if (!prefix_.empty() && prefix_.back() == '*') {
          prefix_ = prefix_.substr(0, prefix_.size() - 1);
        } else {
          return {Status::RedisParseErr, "currently only key prefix matching is supported"};
        }
      } else if (parser.EatEqICase("count")) {
        limit_ = GET_OR_RET(parser.TakeInt());
        if (limit_ <= 0) {
          return {Status::RedisParseErr, "limit should be a positive integer"};
        }
      } else if (IsScan && parser.EatEqICase("type")) {
        return {Status::RedisParseErr, "TYPE flag is currently not supported"};
      } else {
        return parser.InvalidSyntax();
      }
    }

    return Status::OK();
  }

  void PutCursor(const std::string &param) {
    cursor_ = param;
    if (cursor_ == "0") {
      cursor_ = std::string();
    } else {
      cursor_ = cursor_.find(kCursorPrefix) == 0 ? cursor_.substr(strlen(kCursorPrefix)) : cursor_;
    }
  }

  std::string GenerateOutput(Server *srv, const Connection *conn, const std::vector<std::string> &keys,
                             CursorType cursor_type) const {
    std::vector<std::string> list;
    if (keys.size() == static_cast<size_t>(limit_)) {
      auto end_cursor = srv->GenerateCursorFromKeyName(keys.back(), cursor_type);
      list.emplace_back(redis::BulkString(end_cursor));
    } else {
      list.emplace_back(redis::BulkString("0"));
    }

    list.emplace_back(ArrayOfBulkStrings(keys));

    return redis::Array(list);
  }

 protected:
  std::string cursor_;
  std::string prefix_;
  int limit_ = 20;
};

class CommandSubkeyScanBase : public CommandScanBase {
 public:
  CommandSubkeyScanBase() : CommandScanBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);

    key_ = GET_OR_RET(parser.TakeStr());

    PutCursor(GET_OR_RET(parser.TakeStr()));

    return ParseAdditionalFlags<false>(parser);
  }

  std::string GetNextCursor(Server *srv, std::vector<std::string> &fields, CursorType cursor_type) const {
    if (fields.size() == static_cast<size_t>(limit_)) {
      return srv->GenerateCursorFromKeyName(fields.back(), cursor_type);
    }
    return "0";
  }

 protected:
  std::string key_;
};

}  // namespace redis
