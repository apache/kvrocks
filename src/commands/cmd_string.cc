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

#include <cstdint>
#include <optional>

#include "commander.h"
#include "commands/command_parser.h"
#include "error_constants.h"
#include "server/server.h"
#include "time_util.h"
#include "ttl_util.h"
#include "types/redis_bitmap.h"
#include "types/redis_string.h"

namespace Redis {

class CommandGet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string value;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.Get(args_[1], &value);
    // The IsInvalidArgument error means the key type maybe a bitmap
    // which we need to fall back to the bitmap's GetString according
    // to the `max-bitmap-to-string-mb` configuration.
    if (s.IsInvalidArgument()) {
      Config *config = svr->GetConfig();
      uint32_t max_btos_size = static_cast<uint32_t>(config->max_bitmap_to_string_mb) * MiB;
      Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
      s = bitmap_db.GetString(args_[1], max_btos_size, &value);
    }
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(value);
    return Status::OK();
  }
};

class CommandGetEx : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    std::string_view ttl_flag;
    while (parser.Good()) {
      if (auto v = GET_OR_RET(ParseTTL(parser, ttl_flag))) {
        ttl_ = *v;
      } else if (parser.EatEqICaseFlag("PERSIST", ttl_flag)) {
        persist_ = true;
      } else {
        return parser.InvalidSyntax();
      }
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string value;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.GetEx(args_[1], &value, ttl_);

    // The IsInvalidArgument error means the key type maybe a bitmap
    // which we need to fall back to the bitmap's GetString according
    // to the `max-bitmap-to-string-mb` configuration.
    if (s.IsInvalidArgument()) {
      Config *config = svr->GetConfig();
      uint32_t max_btos_size = static_cast<uint32_t>(config->max_bitmap_to_string_mb) * MiB;
      Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
      s = bitmap_db.GetString(args_[1], max_btos_size, &value);
    }
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(value);
    return Status::OK();
  }

 private:
  uint64_t ttl_ = 0;
  bool persist_ = false;
};

class CommandStrlen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string value;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.Get(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::Integer(0);
    } else {
      *output = Redis::Integer(value.size());
    }
    return Status::OK();
  }
};

class CommandGetSet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    std::string old_value;
    auto s = string_db.GetSet(args_[1], args_[2], &old_value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(old_value);
    }
    return Status::OK();
  }
};

class CommandGetDel : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    std::string value;
    auto s = string_db.GetDel(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(value);
    }
    return Status::OK();
  }
};

class CommandGetRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    start_ = *parse_start;
    stop_ = *parse_stop;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string value;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.Get(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::NilString();
      return Status::OK();
    }

    if (start_ < 0) start_ = static_cast<int>(value.size()) + start_;
    if (stop_ < 0) stop_ = static_cast<int>(value.size()) + stop_;
    if (start_ < 0) start_ = 0;
    if (stop_ > static_cast<int>(value.size())) stop_ = static_cast<int>(value.size());
    if (start_ > stop_) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(value.substr(start_, stop_ - start_ + 1));
    }
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandSubStr : public CommandGetRange {
 public:
  CommandSubStr() : CommandGetRange() {}
};

class CommandSetRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    offset_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.SetRange(args_[1], offset_, args_[3], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int offset_ = 0;
};

class CommandMGet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    std::vector<std::string> values;
    // always return OK
    auto statuses = string_db.MGet(keys, &values);
    *output = Redis::MultiBulkString(values, statuses);
    return Status::OK();
  }
};

class CommandAppend : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.Append(args_[1], args_[2], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 3);
    std::string_view ttl_flag, set_flag;
    while (parser.Good()) {
      if (auto v = GET_OR_RET(ParseTTL(parser, ttl_flag))) {
        ttl_ = *v;
      } else if (parser.EatEqICaseFlag("NX", set_flag)) {
        set_flag_ = NX;
      } else if (parser.EatEqICaseFlag("XX", set_flag)) {
        set_flag_ = XX;
      } else {
        return parser.InvalidSyntax();
      }
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s;

    if (ttl_ < 0) {
      string_db.Del(args_[1]);
      *output = Redis::SimpleString("OK");
      return Status::OK();
    }

    if (set_flag_ == NX) {
      s = string_db.SetNX(args_[1], args_[2], ttl_, &ret);
    } else if (set_flag_ == XX) {
      s = string_db.SetXX(args_[1], args_[2], ttl_, &ret);
    } else {
      s = string_db.SetEX(args_[1], args_[2], ttl_);
    }

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (set_flag_ != NONE && !ret) {
      *output = Redis::NilString();
    } else {
      *output = Redis::SimpleString("OK");
    }
    return Status::OK();
  }

 private:
  uint64_t ttl_ = 0;
  enum { NONE, NX, XX } set_flag_ = NONE;
};

class CommandSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*parse_result <= 0) return {Status::RedisParseErr, errInvalidExpireTime};

    ttl_ = *parse_result;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.SetEX(args_[1], args_[3], ttl_ * 1000);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  uint64_t ttl_ = 0;
};

class CommandPSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto ttl_ms = ParseInt<int64_t>(args[2], 10);
    if (!ttl_ms) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*ttl_ms <= 0) return {Status::RedisParseErr, errInvalidExpireTime};

    ttl_ = *ttl_ms;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.SetEX(args_[1], args_[3], ttl_);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int64_t ttl_ = 0;
};

class CommandMSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 1) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    std::vector<StringPair> kvs;
    for (size_t i = 1; i < args_.size(); i += 2) {
      kvs.emplace_back(StringPair{args_[i], args_[i + 1]});
    }

    auto s = string_db.MSet(kvs);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandSetNX : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.SetNX(args_[1], args_[2], 0, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandMSetNX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 1) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    std::vector<StringPair> kvs;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    for (size_t i = 1; i < args_.size(); i += 2) {
      kvs.emplace_back(StringPair{args_[i], args_[i + 1]});
    }

    auto s = string_db.MSetNX(kvs, 0, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandIncr : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.IncrBy(args_[1], 1, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandDecr : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.IncrBy(args_[1], -1, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.IncrBy(args_[1], increment_, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandIncrByFloat : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[2]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    increment_ = *increment;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double ret = 0;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.IncrByFloat(args_[1], increment_, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::BulkString(Util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandDecrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.IncrBy(args_[1], -1 * increment_, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandCAS : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 4);
    std::string_view flag;
    while (parser.Good()) {
      if (parser.EatEqICaseFlag("EX", flag)) {
        ttl_ = GET_OR_RET(parser.TakeInt<int64_t>(TTL_RANGE<int64_t>)) * 1000;
      } else if (parser.EatEqICaseFlag("PX", flag)) {
        ttl_ = GET_OR_RET(parser.TakeInt<int64_t>(TTL_RANGE<int64_t>));
      } else {
        return parser.InvalidSyntax();
      }
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    int ret = 0;
    auto s = string_db.CAS(args_[1], args_[2], args_[3], ttl_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  uint64_t ttl_ = 0;
};

class CommandCAD : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    int ret = 0;
    auto s = string_db.CAD(args_[1], args_[2], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(
    MakeCmdAttr<CommandGet>("get", 2, "read-only", 1, 1, 1), MakeCmdAttr<CommandGetEx>("getex", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandStrlen>("strlen", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGetSet>("getset", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandGetRange>("getrange", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandSubStr>("substr", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGetDel>("getdel", 2, "write", 1, 1, 1),
    MakeCmdAttr<CommandSetRange>("setrange", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandMGet>("mget", -2, "read-only", 1, -1, 1),
    MakeCmdAttr<CommandAppend>("append", 3, "write", 1, 1, 1), MakeCmdAttr<CommandSet>("set", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandSetEX>("setex", 4, "write", 1, 1, 1), MakeCmdAttr<CommandPSetEX>("psetex", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandSetNX>("setnx", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandMSetNX>("msetnx", -3, "write exclusive", 1, -1, 2),
    MakeCmdAttr<CommandMSet>("mset", -3, "write", 1, -1, 2), MakeCmdAttr<CommandIncrBy>("incrby", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandIncrByFloat>("incrbyfloat", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandIncr>("incr", 2, "write", 1, 1, 1), MakeCmdAttr<CommandDecrBy>("decrby", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandDecr>("decr", 2, "write", 1, 1, 1), MakeCmdAttr<CommandCAS>("cas", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandCAD>("cad", 3, "write", 1, 1, 1), )

}  // namespace Redis
