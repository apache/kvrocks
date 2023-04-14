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
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_sortedint.h"

namespace Redis {

class CommandSortedintAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); i++) {
      auto parse_result = ParseInt<uint64_t>(args[i], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      ids_.emplace_back(*parse_result);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    int ret = 0;
    auto s = sortedint_db.Add(args_[1], ids_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<uint64_t> ids_;
};

class CommandSortedintRem : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); i++) {
      auto parse_result = ParseInt<uint64_t>(args[i], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      ids_.emplace_back(*parse_result);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    int ret = 0;
    auto s = sortedint_db.Remove(args_[1], ids_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<uint64_t> ids_;
};

class CommandSortedintCard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    int ret = 0;
    auto s = sortedint_db.Card(args_[1], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSortedintExists : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    std::vector<uint64_t> ids;
    for (size_t i = 2; i < args_.size(); i++) {
      auto parse_result = ParseInt<uint64_t>(args_[i], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      ids.emplace_back(*parse_result);
    }

    std::vector<int> exists;
    auto s = sortedint_db.MExist(args_[1], ids, &exists);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      exists.resize(ids.size(), 0);
    }
    output->append(Redis::MultiLen(exists.size()));
    for (auto exist : exists) {
      output->append(Redis::Integer(exist));
    }

    return Status::OK();
  }
};

class CommandSortedintRange : public Commander {
 public:
  explicit CommandSortedintRange(bool reversed = false) : reversed_(reversed) {}

  Status Parse(const std::vector<std::string> &args) override {
    auto parse_offset = ParseInt<uint64_t>(args[2], 10);
    auto parse_limit = ParseInt<uint64_t>(args[3], 10);
    if (!parse_offset || !parse_limit) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    offset_ = *parse_offset;
    limit_ = *parse_limit;

    if (args.size() == 6) {
      if (Util::ToLower(args[4]) != "cursor") {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      auto parse_result = ParseInt<uint64_t>(args[5], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      cursor_id_ = *parse_result;
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    std::vector<uint64_t> ids;
    auto s = sortedint_db.Range(args_[1], cursor_id_, offset_, limit_, reversed_, &ids);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(Redis::MultiLen(ids.size()));
    for (const auto id : ids) {
      output->append(Redis::BulkString(std::to_string(id)));
    }

    return Status::OK();
  }

 private:
  uint64_t cursor_id_ = 0;
  uint64_t offset_ = 0;
  uint64_t limit_ = 20;
  bool reversed_ = false;
};

class CommandSortedintRevRange : public CommandSortedintRange {
 public:
  CommandSortedintRevRange() : CommandSortedintRange(true) {}
};

class CommandSortedintRangeByValue : public Commander {
 public:
  explicit CommandSortedintRangeByValue(bool reversed = false) { spec_.reversed_ = reversed; }

  Status Parse(const std::vector<std::string> &args) override {
    Status s;
    if (spec_.reversed_) {
      s = Redis::Sortedint::ParseRangeSpec(args[3], args[2], &spec_);
    } else {
      s = Redis::Sortedint::ParseRangeSpec(args[2], args[3], &spec_);
    }
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }

    if (args.size() == 7) {
      if (Util::ToLower(args[4]) != "limit") {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      auto parse_offset = ParseInt<int>(args[5], 10);
      auto parse_count = ParseInt<int>(args[6], 10);
      if (!parse_offset || !parse_count) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      spec_.offset_ = *parse_offset;
      spec_.count_ = *parse_count;
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<uint64_t> ids;
    int size = 0;
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    auto s = sortedint_db.RangeByValue(args_[1], spec_, &ids, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(Redis::MultiLen(ids.size()));
    for (auto id : ids) {
      output->append(Redis::BulkString(std::to_string(id)));
    }

    return Status::OK();
  }

 private:
  SortedintRangeSpec spec_;
};

class CommandSortedintRevRangeByValue : public CommandSortedintRangeByValue {
 public:
  CommandSortedintRevRangeByValue() : CommandSortedintRangeByValue(true) {}
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandSortedintAdd>("siadd", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandSortedintRem>("sirem", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandSortedintCard>("sicard", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSortedintExists>("siexists", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSortedintRange>("sirange", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSortedintRevRange>("sirevrange", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSortedintRangeByValue>("sirangebyvalue", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSortedintRevRangeByValue>("sirevrangebyvalue", -4, "read-only", 1, 1, 1), )

}  // namespace Redis
