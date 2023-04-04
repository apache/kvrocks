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
#include "commands/scan_base.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_set.h"

namespace Redis {

class CommandSAdd : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> members;
    for (unsigned int i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }

    int ret = 0;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.Add(args_[1], members, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSRem : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }

    int ret = 0;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.Remove(args_[1], members, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSCard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    int ret = 0;
    auto s = set_db.Card(args_[1], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSMembers : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    auto s = set_db.Members(args_[1], &members);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }
};

class CommandSIsMember : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    int ret = 0;
    auto s = set_db.IsMember(args_[1], args_[2], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSMIsMember : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }

    std::vector<int> exists;
    auto s = set_db.MIsMember(args_[1], members, &exists);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      exists.resize(members.size(), 0);
    }

    output->append(Redis::MultiLen(exists.size()));
    for (const auto &exist : exists) {
      output->append(Redis::Integer(exist));
    }

    return Status::OK();
  }
};

class CommandSPop : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 3) {
      auto parse_result = ParseInt<int>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
      with_count_ = true;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    auto s = set_db.Take(args_[1], &members, count_, true);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (with_count_) {
      *output = Redis::MultiBulkString(members, false);
    } else {
      if (members.size() > 0) {
        *output = Redis::BulkString(members.front());
      } else {
        *output = Redis::NilString();
      }
    }
    return Status::OK();
  }

 private:
  int count_ = 1;
  bool with_count_ = false;
};

class CommandSRandMember : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 3) {
      auto parse_result = ParseInt<int>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    auto s = set_db.Take(args_[1], &members, count_, false);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }

 private:
  int count_ = 1;
};

class CommandSMove : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    int ret = 0;
    auto s = set_db.Move(args_[1], args_[2], args_[3], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSDiff : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    std::vector<std::string> members;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.Diff(keys, &members);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }
};

class CommandSUnion : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    std::vector<std::string> members;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.Union(keys, &members);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }
};

class CommandSInter : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    std::vector<std::string> members;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.Inter(keys, &members);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }
};

class CommandSDiffStore : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 2; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    int ret = 0;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.DiffStore(args_[1], keys, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSUnionStore : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 2; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    int ret = 0;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.UnionStore(args_[1], keys, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSInterStore : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 2; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    int ret = 0;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.InterStore(args_[1], keys, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSScan : public CommandSubkeyScanBase {
 public:
  CommandSScan() = default;
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    auto s = set_db.Scan(key, cursor, limit, prefix, &members);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = CommandScanBase::GenerateOutput(members);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandSAdd>("sadd", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandSRem>("srem", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandSCard>("scard", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSMembers>("smembers", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSIsMember>("sismember", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSMIsMember>("smismember", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSPop>("spop", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandSRandMember>("srandmember", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSMove>("smove", 4, "write", 1, 2, 1),
                        MakeCmdAttr<CommandSDiff>("sdiff", -2, "read-only", 1, -1, 1),
                        MakeCmdAttr<CommandSUnion>("sunion", -2, "read-only", 1, -1, 1),
                        MakeCmdAttr<CommandSInter>("sinter", -2, "read-only", 1, -1, 1),
                        MakeCmdAttr<CommandSDiffStore>("sdiffstore", -3, "write", 1, -1, 1),
                        MakeCmdAttr<CommandSUnionStore>("sunionstore", -3, "write", 1, -1, 1),
                        MakeCmdAttr<CommandSInterStore>("sinterstore", -3, "write", 1, -1, 1),
                        MakeCmdAttr<CommandSScan>("sscan", -3, "read-only", 1, 1, 1), )

}  // namespace Redis
