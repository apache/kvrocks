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
#include "error_constants.h"
#include "parse_util.h"
#include "server/server.h"

namespace redis {

inline constexpr const char *kCursorPrefix = "_";

class CommandScanBase : public Commander {
 public:
  Status ParseMatchAndCountParam(const std::string &type, std::string value) {
    if (type == "match") {
      prefix_ = std::move(value);
      if (!prefix_.empty() && prefix_[prefix_.size() - 1] == '*') {
        prefix_ = prefix_.substr(0, prefix_.size() - 1);
        return Status::OK();
      }

      return {Status::RedisParseErr, "only keys prefix match was supported"};
    } else if (type == "count") {
      auto parse_result = ParseInt<int>(value, 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "count param should be type int"};
      }

      limit_ = *parse_result;
      if (limit_ <= 0) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Status::OK();
  }

  void ParseCursor(const std::string &param) {
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

    list.emplace_back(conn->MultiBulkString(keys, false));

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
    if (args.size() % 2 == 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    key_ = args[1];
    ParseCursor(args[2]);
    if (args.size() >= 5) {
      Status s = ParseMatchAndCountParam(util::ToLower(args[3]), args_[4]);
      if (!s.IsOK()) {
        return s;
      }
    }

    if (args.size() >= 7) {
      Status s = ParseMatchAndCountParam(util::ToLower(args[5]), args_[6]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }

  std::string GenerateOutput(Server *srv, const Connection *conn, const std::vector<std::string> &fields,
                             const std::vector<std::string> &values, CursorType cursor_type) {
    std::vector<std::string> list;
    auto items_count = fields.size();
    if (items_count == static_cast<size_t>(limit_)) {
      auto end_cursor = srv->GenerateCursorFromKeyName(fields.back(), cursor_type);
      list.emplace_back(redis::BulkString(end_cursor));
    } else {
      list.emplace_back(redis::BulkString("0"));
    }
    std::vector<std::string> fvs;
    if (items_count > 0) {
      for (size_t i = 0; i < items_count; i++) {
        fvs.emplace_back(fields[i]);
        fvs.emplace_back(values[i]);
      }
    }
    list.emplace_back(conn->MultiBulkString(fvs, false));
    return redis::Array(list);
  }

 protected:
  std::string key_;
};

}  // namespace redis
