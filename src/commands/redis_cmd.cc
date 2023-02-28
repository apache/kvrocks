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

#include "redis_cmd.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <chrono>
#include <climits>
#include <cmath>
#include <limits>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>

#include "cluster/cluster.h"
#include "cluster/redis_slot.h"
#include "cluster/slot_import.h"
#include "cluster/slot_migrate.h"
#include "command_parser.h"
#include "fd_util.h"
#include "io_util.h"
#include "parse_util.h"
#include "server/redis_connection.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "stats/disk_stats.h"
#include "stats/log_collector.h"
#include "status.h"
#include "storage/redis_db.h"
#include "storage/redis_pubsub.h"
#include "storage/scripting.h"
#include "storage/storage.h"
#include "thread_util.h"
#include "time_util.h"
#include "types/redis_bitmap.h"
#include "types/redis_geo.h"
#include "types/redis_hash.h"
#include "types/redis_list.h"
#include "types/redis_set.h"
#include "types/redis_sortedint.h"
#include "types/redis_stream.h"
#include "types/redis_string.h"
#include "types/redis_zset.h"

namespace Redis {

constexpr const char *kCursorPrefix = "_";

constexpr const char *errInvalidSyntax = "syntax error";
constexpr const char *errInvalidExpireTime = "invalid expire time";
constexpr const char *errWrongNumOfArguments = "wrong number of arguments";
constexpr const char *errValueNotInteger = "value is not an integer or out of range";
constexpr const char *errAdministorPermissionRequired = "administor permission required to perform the command";
constexpr const char *errValueMustBePositive = "value is out of range, must be positive";
constexpr const char *errNoSuchKey = "no such key";
constexpr const char *errUnbalancedStreamList =
    "Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.";
constexpr const char *errTimeoutIsNegative = "timeout is negative";
constexpr const char *errLimitOptionNotAllowed = "syntax error, LIMIT cannot be used without the special ~ option";
constexpr const char *errZSetLTGTNX = "GT, LT, and/or NX options at the same time are not compatible";
constexpr const char *errScoreIsNotValidFloat = "score is not a valid float";
constexpr const char *errValueIsNotFloat = "value is not a valid float";
constexpr const char *errNoMatchingScript = "NOSCRIPT No matching script. Please use EVAL";

enum class AuthResult {
  OK,
  INVALID_PASSWORD,
  NO_REQUIRE_PASS,
};

AuthResult AuthenticateUser(Connection *conn, Config *config, const std::string &user_password) {
  auto iter = config->tokens.find(user_password);
  if (iter != config->tokens.end()) {
    conn->SetNamespace(iter->second);
    conn->BecomeUser();
    return AuthResult::OK;
  }

  const auto &requirepass = config->requirepass;
  if (!requirepass.empty() && user_password != requirepass) {
    return AuthResult::INVALID_PASSWORD;
  }

  conn->SetNamespace(kDefaultNamespace);
  conn->BecomeAdmin();
  if (requirepass.empty()) {
    return AuthResult::NO_REQUIRE_PASS;
  }

  return AuthResult::OK;
}

template <typename T>
T TTLMsToS(T ttl) {
  if (ttl <= 0) {
    return ttl;
  } else if (ttl < 1000) {
    return 1;
  } else {
    return ttl / 1000;
  }
}

int ExpireToTTL(int64_t expire) {
  int64_t now = Util::GetTimeStamp();
  return static_cast<int>(expire - now);
}

template <typename T>
constexpr auto TTL_RANGE = NumericRange<T>{1, std::numeric_limits<T>::max()};

template <typename T>
StatusOr<std::optional<int>> ParseTTL(CommandParser<T> &parser, std::string_view &curr_flag) {
  if (parser.EatEqICaseFlag("EX", curr_flag)) {
    return GET_OR_RET(parser.template TakeInt<int>(TTL_RANGE<int>));
  } else if (parser.EatEqICaseFlag("EXAT", curr_flag)) {
    return ExpireToTTL(GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>)));
  } else if (parser.EatEqICaseFlag("PX", curr_flag)) {
    return TTLMsToS(GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>)));
  } else if (parser.EatEqICaseFlag("PXAT", curr_flag)) {
    return ExpireToTTL(TTLMsToS(GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>))));
  } else {
    return std::nullopt;
  }
}

class CommandAuth : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Config *config = svr->GetConfig();
    auto &user_password = args_[1];
    AuthResult result = AuthenticateUser(conn, config, user_password);
    switch (result) {
      case AuthResult::OK:
        *output = Redis::SimpleString("OK");
        break;
      case AuthResult::INVALID_PASSWORD:
        *output = Redis::Error("ERR invalid password");
        break;
      case AuthResult::NO_REQUIRE_PASS:
        *output = Redis::Error("ERR Client sent AUTH, but no password is set");
        break;
    }
    return Status::OK();
  }
};

class CommandNamespace : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    Config *config = svr->GetConfig();
    std::string sub_command = Util::ToLower(args_[1]);
    if (args_.size() == 3 && sub_command == "get") {
      if (args_[2] == "*") {
        std::vector<std::string> namespaces;
        auto tokens = config->tokens;
        for (auto &token : tokens) {
          namespaces.emplace_back(token.second);  // namespace
          namespaces.emplace_back(token.first);   // token
        }
        namespaces.emplace_back(kDefaultNamespace);
        namespaces.emplace_back(config->requirepass);
        *output = Redis::MultiBulkString(namespaces, false);
      } else {
        std::string token;
        auto s = config->GetNamespace(args_[2], &token);
        if (s.Is<Status::NotFound>()) {
          *output = Redis::NilString();
        } else {
          *output = Redis::BulkString(token);
        }
      }
    } else if (args_.size() == 4 && sub_command == "set") {
      Status s = config->SetNamespace(args_[2], args_[3]);
      *output = s.IsOK() ? Redis::SimpleString("OK") : Redis::Error(s.Msg());
      LOG(WARNING) << "Updated namespace: " << args_[2] << " with token: " << args_[3] << ", addr: " << conn->GetAddr()
                   << ", result: " << s.Msg();
    } else if (args_.size() == 4 && sub_command == "add") {
      Status s = config->AddNamespace(args_[2], args_[3]);
      *output = s.IsOK() ? Redis::SimpleString("OK") : Redis::Error(s.Msg());
      LOG(WARNING) << "New namespace: " << args_[2] << " with token: " << args_[3] << ", addr: " << conn->GetAddr()
                   << ", result: " << s.Msg();
    } else if (args_.size() == 3 && sub_command == "del") {
      Status s = config->DelNamespace(args_[2]);
      *output = s.IsOK() ? Redis::SimpleString("OK") : Redis::Error(s.Msg());
      LOG(WARNING) << "Deleted namespace: " << args_[2] << ", addr: " << conn->GetAddr() << ", result: " << s.Msg();
    } else {
      *output = Redis::Error("NAMESPACE subcommand must be one of GET, SET, DEL, ADD");
    }
    return Status::OK();
  }
};

class CommandKeys : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string prefix = args_[1];
    std::vector<std::string> keys;
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    if (prefix == "*") {
      redis.Keys(std::string(), &keys);
    } else {
      if (prefix[prefix.size() - 1] != '*') {
        *output = Redis::Error("ERR only keys prefix match was supported");
        return Status::OK();
      }

      redis.Keys(prefix.substr(0, prefix.size() - 1), &keys);
    }
    *output = Redis::MultiBulkString(keys);
    return Status::OK();
  }
};

class CommandFlushDB : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (svr->GetConfig()->cluster_enabled) {
      if (svr->slot_migrate_->IsMigrationInProgress()) {
        svr->slot_migrate_->SetMigrateStopFlag(true);
        LOG(INFO) << "Stop migration task for flushdb";
      }
    }
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    auto s = redis.FlushDB();
    LOG(WARNING) << "DB keys in namespace: " << conn->GetNamespace() << " was flushed, addr: " << conn->GetAddr();
    if (s.ok()) {
      *output = Redis::SimpleString("OK");
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandFlushAll : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    if (svr->GetConfig()->cluster_enabled) {
      if (svr->slot_migrate_->IsMigrationInProgress()) {
        svr->slot_migrate_->SetMigrateStopFlag(true);
        LOG(INFO) << "Stop migration task for flushall";
      }
    }

    Redis::Database redis(svr->storage_, conn->GetNamespace());
    auto s = redis.FlushAll();
    if (s.ok()) {
      LOG(WARNING) << "All DB keys was flushed, addr: " << conn->GetAddr();
      *output = Redis::SimpleString("OK");
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandPing : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      *output = Redis::SimpleString("PONG");
    } else if (args_.size() == 2) {
      *output = Redis::BulkString(args_[1]);
    } else {
      return {Status::NotOK, errWrongNumOfArguments};
    }
    return Status::OK();
  }
};

class CommandSelect : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandConfig : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    Config *config = svr->GetConfig();
    std::string sub_command = Util::ToLower(args_[1]);
    if ((sub_command == "rewrite" && args_.size() != 2) || (sub_command == "get" && args_.size() != 3) ||
        (sub_command == "set" && args_.size() != 4)) {
      *output = Redis::Error(errWrongNumOfArguments);
      return Status::OK();
    }

    if (args_.size() == 2 && sub_command == "rewrite") {
      Status s = config->Rewrite();
      if (!s.IsOK()) return {Status::RedisExecErr, s.Msg()};

      *output = Redis::SimpleString("OK");
      LOG(INFO) << "# CONFIG REWRITE executed with success";
    } else if (args_.size() == 3 && sub_command == "get") {
      std::vector<std::string> values;
      config->Get(args_[2], &values);
      *output = Redis::MultiBulkString(values);
    } else if (args_.size() == 4 && sub_command == "set") {
      Status s = config->Set(svr, args_[2], args_[3]);
      if (!s.IsOK()) {
        *output = Redis::Error("CONFIG SET '" + args_[2] + "' error: " + s.Msg());
      } else {
        *output = Redis::SimpleString("OK");
      }
    } else {
      *output = Redis::Error("CONFIG subcommand must be one of GET, SET, REWRITE");
    }
    return Status::OK();
  }
};

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
  int ttl_ = 0;
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
  int ttl_ = 0;
  enum { NONE, NX, XX } set_flag_ = NONE;
};

class CommandSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*parse_result <= 0) return {Status::RedisParseErr, errInvalidExpireTime};

    ttl_ = *parse_result;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.SetEX(args_[1], args_[3], ttl_);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int ttl_ = 0;
};

class CommandPSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto ttl_ms = ParseInt<int64_t>(args[2], 10);
    if (!ttl_ms) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*ttl_ms <= 0) return {Status::RedisParseErr, errInvalidExpireTime};

    if (*ttl_ms < 1000) {
      ttl_ = 1;
    } else {
      ttl_ = static_cast<int>(*ttl_ms / 1000);
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    auto s = string_db.SetEX(args_[1], args_[3], ttl_);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int ttl_ = 0;
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
        ttl_ = GET_OR_RET(parser.TakeInt<int>(TTL_RANGE<int>));
      } else if (parser.EatEqICaseFlag("PX", flag)) {
        ttl_ = static_cast<int>(TTLMsToS(GET_OR_RET(parser.TakeInt<int64_t>(TTL_RANGE<int64_t>))));
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
  int ttl_ = 0;
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

class CommandDel : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int cnt = 0;
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    for (size_t i = 1; i < args_.size(); i++) {
      auto s = redis.Del(args_[i]);
      if (s.ok()) cnt++;
    }
    *output = Redis::Integer(cnt);
    return Status::OK();
  }
};

Status getBitOffsetFromArgument(const std::string &arg, uint32_t *offset) {
  auto parse_result = ParseInt<uint32_t>(arg, 10);
  if (!parse_result) {
    return parse_result.ToStatus();
  }

  *offset = *parse_result;
  return Status::OK();
}

class CommandGetBit : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = getBitOffsetFromArgument(args[2], &offset_);
    if (!s.IsOK()) return s;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    bool bit = false;
    Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
    auto s = bitmap_db.GetBit(args_[1], offset_, &bit);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(bit ? 1 : 0);
    return Status::OK();
  }

 private:
  uint32_t offset_ = 0;
};

class CommandSetBit : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = getBitOffsetFromArgument(args[2], &offset_);
    if (!s.IsOK()) return s;

    if (args[3] == "0") {
      bit_ = false;
    } else if (args[3] == "1") {
      bit_ = true;
    } else {
      return {Status::RedisParseErr, "bit is out of range"};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    bool old_bit = false;
    Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
    auto s = bitmap_db.SetBit(args_[1], offset_, bit_, &old_bit);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(old_bit ? 1 : 0);
    return Status::OK();
  }

 private:
  uint32_t offset_ = 0;
  bool bit_ = false;
};

class CommandBitCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 3) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if (args.size() == 4) {
      auto parse_start = ParseInt<int64_t>(args[2], 10);
      if (!parse_start) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      start_ = *parse_start;
      auto parse_stop = ParseInt<int64_t>(args[3], 10);
      if (!parse_stop) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      stop_ = *parse_stop;
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    uint32_t cnt = 0;
    Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
    auto s = bitmap_db.BitCount(args_[1], start_, stop_, &cnt);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(cnt);
    return Status::OK();
  }

 private:
  int64_t start_ = 0;
  int64_t stop_ = -1;
};

class CommandBitPos : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() >= 4) {
      auto parse_start = ParseInt<int64_t>(args[3], 10);
      if (!parse_start) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      start_ = *parse_start;
    }

    if (args.size() >= 5) {
      auto parse_stop = ParseInt<int64_t>(args[4], 10);
      if (!parse_stop) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      stop_given_ = true;
      stop_ = *parse_stop;
    }

    if (args[2] == "0") {
      bit_ = false;
    } else if (args[2] == "1") {
      bit_ = true;
    } else {
      return {Status::RedisParseErr, "bit should be 0 or 1"};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t pos = 0;
    Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
    auto s = bitmap_db.BitPos(args_[1], bit_, start_, stop_, stop_given_, &pos);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(pos);
    return Status::OK();
  }

 private:
  int64_t start_ = 0;
  int64_t stop_ = -1;
  bool bit_ = false;
  bool stop_given_ = false;
};

class CommandBitOp : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    std::string opname = Util::ToLower(args[1]);
    if (opname == "and")
      op_flag_ = kBitOpAnd;
    else if (opname == "or")
      op_flag_ = kBitOpOr;
    else if (opname == "xor")
      op_flag_ = kBitOpXor;
    else if (opname == "not")
      op_flag_ = kBitOpNot;
    else
      return {Status::RedisInvalidCmd, "Unknown bit operation"};
    if (op_flag_ == kBitOpNot && args.size() != 4) {
      return {Status::RedisInvalidCmd, "BITOP NOT must be called with a single source key."};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> op_keys;
    for (uint64_t i = 3; i < args_.size(); i++) {
      op_keys.emplace_back(args_[i]);
    }

    int64_t dest_key_len = 0;
    Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
    auto s = bitmap_db.BitOp(op_flag_, args_[1], args_[2], op_keys, &dest_key_len);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(dest_key_len);
    return Status::OK();
  }

 private:
  BitOpFlags op_flag_;
};

class CommandType : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    RedisType type = kRedisNone;
    auto s = redis.Type(args_[1], &type);
    if (s.ok()) {
      *output = Redis::SimpleString(RedisTypeNames[type]);
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandObject : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (Util::ToLower(args_[1]) == "dump") {
      Redis::Database redis(svr->storage_, conn->GetNamespace());
      std::vector<std::string> infos;
      auto s = redis.Dump(args_[2], &infos);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      output->append(Redis::MultiLen(infos.size()));
      for (const auto &info : infos) {
        output->append(Redis::BulkString(info));
      }
    } else {
      *output = Redis::Error("object subcommand must be dump");
    }
    return Status::OK();
  }
};

class CommandTTL : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    int ttl = 0;
    auto s = redis.TTL(args_[1], &ttl);
    if (s.ok()) {
      *output = Redis::Integer(ttl);
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandPTTL : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    int ttl = 0;
    auto s = redis.TTL(args_[1], &ttl);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    if (ttl > 0) {
      *output = Redis::Integer(ttl * 1000);
    } else {
      *output = Redis::Integer(ttl);
    }

    return Status::OK();
  }
};

class CommandExists : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<rocksdb::Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    int cnt = 0;
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    redis.Exists(keys, &cnt);
    *output = Redis::Integer(cnt);

    return Status::OK();
  }
};

StatusOr<int> TTLToTimestamp(int ttl) {
  int64_t now = Util::GetTimeStamp();
  if (ttl >= INT32_MAX - now) {
    return {Status::RedisParseErr, "the expire time was overflow"};
  }

  return ttl + now;
}

class CommandExpire : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    seconds_ = GET_OR_RET(TTLToTimestamp(GET_OR_RET(ParseInt<int>(args[2], 10))));
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    auto s = redis.Expire(args_[1], seconds_);
    if (s.ok()) {
      *output = Redis::Integer(1);
    } else {
      *output = Redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  int seconds_ = 0;
};

class CommandPExpire : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    seconds_ = GET_OR_RET(TTLToTimestamp(TTLMsToS(GET_OR_RET(ParseInt<int64_t>(args[2], 10)))));
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    auto s = redis.Expire(args_[1], seconds_);
    if (s.ok()) {
      *output = Redis::Integer(1);
    } else {
      *output = Redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  int seconds_ = 0;
};

class CommandExpireAt : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*parse_result >= INT32_MAX) {
      return {Status::RedisParseErr, "the expire time was overflow"};
    }

    timestamp_ = static_cast<int>(*parse_result);

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    auto s = redis.Expire(args_[1], timestamp_);
    if (s.ok()) {
      *output = Redis::Integer(1);
    } else {
      *output = Redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  int timestamp_ = 0;
};

class CommandPExpireAt : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*parse_result / 1000 >= INT32_MAX) {
      return {Status::RedisParseErr, "the expire time was overflow"};
    }

    timestamp_ = static_cast<int>(*parse_result / 1000);

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    auto s = redis.Expire(args_[1], timestamp_);
    if (s.ok()) {
      *output = Redis::Integer(1);
    } else {
      *output = Redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  int timestamp_ = 0;
};

class CommandPersist : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ttl = 0;
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    auto s = redis.TTL(args_[1], &ttl);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    if (ttl == -1 || ttl == -2) {
      *output = Redis::Integer(0);
      return Status::OK();
    }

    s = redis.Expire(args_[1], 0);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(1);
    return Status::OK();
  }
};

class CommandHGet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::string value;
    auto s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(value);
    return Status::OK();
  }
};

class CommandHSetNX : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    auto s = hash_db.SetNX(args_[1], args_[2], args_[3], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHStrlen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::string value;
    auto s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(static_cast<int>(value.size()));
    return Status::OK();
  }
};

class CommandHDel : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }

    int ret = 0;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    auto s = hash_db.Delete(args_[1], fields, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHExists : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::string value;
    auto s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? Redis::Integer(0) : Redis::Integer(1);
    return Status::OK();
  }
};

class CommandHLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    uint32_t count = 0;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    auto s = hash_db.Size(args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? Redis::Integer(0) : Redis::Integer(count);
    return Status::OK();
  }
};

class CommandHIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[3], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    auto s = hash_db.IncrBy(args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandHIncrByFloat : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[3]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    increment_ = *increment;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double ret = 0;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    auto s = hash_db.IncrByFloat(args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::BulkString(Util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandHMGet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }

    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    auto s = hash_db.MGet(args_[1], fields, &values, &statuses);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      values.resize(fields.size(), "");
      *output = Redis::MultiBulkString(values);
    } else {
      *output = Redis::MultiBulkString(values, statuses);
    }
    return Status::OK();
  }
};

class CommandHMSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<FieldValue> field_values;
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values.emplace_back(FieldValue{args_[i], args_[i + 1]});
    }

    int ret = 0;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    auto s = hash_db.MSet(args_[1], field_values, false, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (GetAttributes()->name == "hset") {
      *output = Redis::Integer(ret);
    } else {
      *output = Redis::SimpleString("OK");
    }
    return Status::OK();
  }
};

class CommandHKeys : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    auto s = hash_db.GetAll(args_[1], &field_values, HashFetchType::kOnlyKey);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> keys;
    keys.reserve(field_values.size());
    for (const auto &fv : field_values) {
      keys.emplace_back(fv.field);
    }
    *output = Redis::MultiBulkString(keys);

    return Status::OK();
  }
};

class CommandHVals : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    auto s = hash_db.GetAll(args_[1], &field_values, HashFetchType::kOnlyValue);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> values;
    values.reserve(field_values.size());
    for (const auto &p : field_values) {
      values.emplace_back(p.value);
    }
    *output = MultiBulkString(values);

    return Status::OK();
  }
};

class CommandHGetAll : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    auto s = hash_db.GetAll(args_[1], &field_values);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> kv_pairs;
    kv_pairs.reserve(field_values.size());
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = MultiBulkString(kv_pairs);

    return Status::OK();
  }
};

class CommandHRangeByLex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 4);
    while (parser.Good()) {
      if (parser.EatEqICase("REV")) {
        spec_.reversed = true;
      } else if (parser.EatEqICase("LIMIT")) {
        spec_.offset = GET_OR_RET(parser.TakeInt());
        spec_.count = GET_OR_RET(parser.TakeInt());
      } else {
        return parser.InvalidSyntax();
      }
    }
    Status s;
    if (spec_.reversed) {
      s = ParseRangeLexSpec(args[3], args[2], &spec_);
    } else {
      s = ParseRangeLexSpec(args[2], args[3], &spec_);
    }
    if (!s.IsOK()) {
      return Status(Status::RedisParseErr, s.Msg());
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    rocksdb::Status s = hash_db.RangeByLex(args_[1], spec_, &field_values);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    std::vector<std::string> kv_pairs;
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = MultiBulkString(kv_pairs);

    return Status::OK();
  }

 private:
  CommonRangeLexSpec spec_;
};

class CommandPush : public Commander {
 public:
  CommandPush(bool create_if_missing, bool left) : left_(left), create_if_missing_(create_if_missing) {}

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> elems;
    for (size_t i = 2; i < args_.size(); i++) {
      elems.emplace_back(args_[i]);
    }

    int ret = 0;
    rocksdb::Status s;
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    if (create_if_missing_) {
      s = list_db.Push(args_[1], elems, left_, &ret);
    } else {
      s = list_db.PushX(args_[1], elems, left_, &ret);
    }
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    svr->WakeupBlockingConns(args_[1], elems.size());

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  bool left_;
  bool create_if_missing_;
};

class CommandLPush : public CommandPush {
 public:
  CommandLPush() : CommandPush(true, true) {}
};

class CommandRPush : public CommandPush {
 public:
  CommandRPush() : CommandPush(true, false) {}
};

class CommandLPushX : public CommandPush {
 public:
  CommandLPushX() : CommandPush(false, true) {}
};

class CommandRPushX : public CommandPush {
 public:
  CommandRPushX() : CommandPush(false, false) {}
};

class CommandPop : public Commander {
 public:
  explicit CommandPop(bool left) : left_(left) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    if (args.size() == 2) {
      return Status::OK();
    }

    auto v = ParseInt<int32_t>(args[2], 10);
    if (!v) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*v < 0) {
      return {Status::RedisParseErr, errValueMustBePositive};
    }

    count_ = *v;
    with_count_ = true;

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    if (with_count_) {
      std::vector<std::string> elems;
      auto s = list_db.PopMulti(args_[1], left_, count_, &elems);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (s.IsNotFound()) {
        *output = Redis::MultiLen(-1);
      } else {
        *output = Redis::MultiBulkString(elems);
      }
    } else {
      std::string elem;
      auto s = list_db.Pop(args_[1], left_, &elem);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (s.IsNotFound()) {
        *output = Redis::NilString();
      } else {
        *output = Redis::BulkString(elem);
      }
    }

    return Status::OK();
  }

 private:
  bool left_;
  bool with_count_ = false;
  uint32_t count_ = 1;
};

class CommandLPop : public CommandPop {
 public:
  CommandLPop() : CommandPop(true) {}
};

class CommandRPop : public CommandPop {
 public:
  CommandRPop() : CommandPop(false) {}
};

class CommandBPop : public Commander {
 public:
  explicit CommandBPop(bool left) : left_(left) {}

  CommandBPop(const CommandBPop &) = delete;
  CommandBPop &operator=(const CommandBPop &) = delete;

  ~CommandBPop() override {
    if (timer_) {
      event_free(timer_);
      timer_ = nullptr;
    }
  }

  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[args.size() - 1], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "timeout is not an integer or out of range"};
    }

    if (*parse_result < 0) {
      return {Status::RedisParseErr, "timeout should not be negative"};
    }

    timeout_ = *parse_result;

    keys_ = std::vector<std::string>(args.begin() + 1, args.end() - 1);
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    svr_ = svr;
    conn_ = conn;

    auto bev = conn->GetBufferEvent();
    auto s = TryPopFromList();
    if (s.ok() || !s.IsNotFound()) {
      return Status::OK();  // error has already output in TryPopFromList
    }

    if (conn->IsInExec()) {
      *output = Redis::MultiLen(-1);
      return Status::OK();  // No blocking in multi-exec
    }

    for (const auto &key : keys_) {
      svr_->AddBlockingKey(key, conn_);
    }

    bufferevent_setcb(bev, nullptr, WriteCB, EventCB, this);

    if (timeout_) {
      timer_ = evtimer_new(bufferevent_get_base(bev), TimerCB, this);
      timeval tm = {timeout_, 0};
      evtimer_add(timer_, &tm);
    }

    return {Status::BlockingCmd};
  }

  rocksdb::Status TryPopFromList() {
    Redis::List list_db(svr_->storage_, conn_->GetNamespace());
    std::string elem;
    const std::string *last_key_ptr = nullptr;
    rocksdb::Status s;
    for (const auto &key : keys_) {
      last_key_ptr = &key;
      s = list_db.Pop(key, left_, &elem);
      if (s.ok() || !s.IsNotFound()) {
        break;
      }
    }

    if (s.ok()) {
      if (!last_key_ptr) {
        conn_->Reply(Redis::MultiBulkString({"", std::move(elem)}));
      } else {
        conn_->Reply(Redis::MultiBulkString({*last_key_ptr, std::move(elem)}));
      }
    } else if (!s.IsNotFound()) {
      conn_->Reply(Redis::Error("ERR " + s.ToString()));
      LOG(ERROR) << "Failed to execute redis command: " << conn_->current_cmd_->GetAttributes()->name
                 << ", err: " << s.ToString();
    }

    return s;
  }

  static void WriteCB(bufferevent *bev, void *ctx) {
    auto self = reinterpret_cast<CommandBPop *>(ctx);
    auto s = self->TryPopFromList();
    if (s.IsNotFound()) {
      // The connection may be waked up but can't pop from list. For example,
      // connection A is blocking on list and connection B push a new element
      // then wake up the connection A, but this element may be token by other connection C.
      // So we need to wait for the wake event again by disabling the WRITE event.
      bufferevent_disable(bev, EV_WRITE);
      return;
    }

    if (self->timer_) {
      event_free(self->timer_);
      self->timer_ = nullptr;
    }

    self->unBlockingAll();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite, Redis::Connection::OnEvent,
                      self->conn_);
    bufferevent_enable(bev, EV_READ);
    // We need to manually trigger the read event since we will stop processing commands
    // in connection after the blocking command, so there may have some commands to be processed.
    // Related issue: https://github.com/apache/incubator-kvrocks/issues/831
    bufferevent_trigger(bev, EV_READ, BEV_TRIG_IGNORE_WATERMARKS);
  }

  static void EventCB(bufferevent *bev, int16_t events, void *ctx) {
    auto self = static_cast<CommandBPop *>(ctx);
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (self->timer_ != nullptr) {
        event_free(self->timer_);
        self->timer_ = nullptr;
      }
      self->unBlockingAll();
    }
    Redis::Connection::OnEvent(bev, events, self->conn_);
  }

  static void TimerCB(int, int16_t events, void *ctx) {
    auto self = reinterpret_cast<CommandBPop *>(ctx);
    self->conn_->Reply(Redis::NilString());
    event_free(self->timer_);
    self->timer_ = nullptr;
    self->unBlockingAll();
    auto bev = self->conn_->GetBufferEvent();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite, Redis::Connection::OnEvent,
                      self->conn_);
    bufferevent_enable(bev, EV_READ);
  }

 private:
  bool left_ = false;
  int timeout_ = 0;  // seconds
  std::vector<std::string> keys_;
  Server *svr_ = nullptr;
  Connection *conn_ = nullptr;
  event *timer_ = nullptr;

  void unBlockingAll() {
    for (const auto &key : keys_) {
      svr_->UnBlockingKey(key, conn_);
    }
  }
};

class CommandBLPop : public CommandBPop {
 public:
  CommandBLPop() : CommandBPop(true) {}
};

class CommandBRPop : public CommandBPop {
 public:
  CommandBRPop() : CommandBPop(false) {}
};

class CommandLRem : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    count_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    auto s = list_db.Rem(args_[1], count_, args_[3], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int count_ = 0;
};

class CommandLInsert : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if ((Util::ToLower(args[2]) == "before")) {
      before_ = true;
    } else if ((Util::ToLower(args[2]) == "after")) {
      before_ = false;
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    auto s = list_db.Insert(args_[1], args_[3], args_[4], before_, &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  bool before_ = false;
};

class CommandLRange : public Commander {
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
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> elems;
    auto s = list_db.Range(args_[1], start_, stop_, &elems);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(elems, false);
    return Status::OK();
  }

 private:
  int start_ = 0, stop_ = 0;
};

class CommandLLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    uint32_t count = 0;
    auto s = list_db.Size(args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(count);
    return Status::OK();
  }
};

class CommandLIndex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    index_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    auto s = list_db.Index(args_[1], index_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(elem);
    }
    return Status::OK();
  }

 private:
  int index_ = 0;
};

class CommandLSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    index_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    auto s = list_db.Set(args_[1], index_, args_[3]);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int index_ = 0;
};

class CommandLTrim : public Commander {
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
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    auto s = list_db.Trim(args_[1], start_, stop_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandRPopLPUSH : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    auto s = list_db.RPopLPush(args_[1], args_[2], &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(elem);
    return Status::OK();
  }
};

class CommandLMove : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto arg_val = Util::ToLower(args_[3]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    src_left_ = arg_val == "left";
    arg_val = Util::ToLower(args_[4]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    dst_left_ = arg_val == "left";
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    auto s = list_db.LMove(args_[1], args_[2], src_left_, dst_left_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(elem);
    return Status::OK();
  }

 private:
  bool src_left_;
  bool dst_left_;
};

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

class CommandZAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t index = 2;
    parseFlags(args, index);
    if (auto s = validateFlags(); !s.IsOK()) {
      return s;
    }

    if (auto left = (args.size() - index); left >= 0) {
      if (flags_.HasIncr() && left != 2) {
        return {Status::RedisParseErr, "INCR option supports a single increment-element pair"};
      }

      if (left % 2 != 0 || left == 0) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    for (size_t i = index; i < args.size(); i += 2) {
      auto score = ParseFloat(args[i]);
      if (!score) {
        return {Status::RedisParseErr, errValueIsNotFloat};
      }
      if (std::isnan(*score)) {
        return {Status::RedisParseErr, errScoreIsNotValidFloat};
      }

      member_scores_.emplace_back(MemberScore{args[i + 1], *score});
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    double old_score = member_scores_[0].score;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Add(args_[1], flags_, &member_scores_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (flags_.HasIncr()) {
      auto new_score = member_scores_[0].score;
      if ((flags_.HasNX() || flags_.HasXX() || flags_.HasLT() || flags_.HasGT()) && old_score == new_score &&
          ret == 0) {  // not the first time using incr && score not changed
        *output = Redis::NilString();
        return Status::OK();
      }

      *output = Redis::BulkString(Util::Float2String(new_score));
    } else {
      *output = Redis::Integer(ret);
    }
    return Status::OK();
  }

 private:
  std::vector<MemberScore> member_scores_;
  ZAddFlags flags_{0};

  void parseFlags(const std::vector<std::string> &args, size_t &index);
  Status validateFlags() const;
};

void CommandZAdd::parseFlags(const std::vector<std::string> &args, size_t &index) {
  std::unordered_map<std::string, ZSetFlags> options = {{"xx", kZSetXX}, {"nx", kZSetNX}, {"ch", kZSetCH},
                                                        {"lt", kZSetLT}, {"gt", kZSetGT}, {"incr", kZSetIncr}};
  for (size_t i = 2; i < args.size(); i++) {
    auto option = Util::ToLower(args[i]);
    if (auto it = options.find(option); it != options.end()) {
      flags_.SetFlag(it->second);
      index++;
    } else {
      break;
    }
  }
}

Status CommandZAdd::validateFlags() const {
  if (!flags_.HasAnyFlags()) {
    return Status::OK();
  }

  if (flags_.HasNX() && flags_.HasXX()) {
    return {Status::RedisParseErr, "XX and NX options at the same time are not compatible"};
  }

  if ((flags_.HasLT() && flags_.HasGT()) || (flags_.HasLT() && flags_.HasNX()) || (flags_.HasGT() && flags_.HasNX())) {
    return {Status::RedisParseErr, errZSetLTGTNX};
  }

  return Status::OK();
}

class CommandZCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = Redis::ZSet::ParseRangeSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Count(args_[1], spec_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
};

class CommandZCard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Card(args_[1], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandZIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[2]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    incr_ = *increment;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double score = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.IncrBy(args_[1], args_[3], incr_, &score);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::BulkString(Util::Float2String(score));
    return Status::OK();
  }

 private:
  double incr_ = 0.0;
};

class CommandZLexCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = ParseRangeLexSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.RangeByLex(args_[1], spec_, nullptr, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  CommonRangeLexSpec spec_;
};

class CommandZPop : public Commander {
 public:
  explicit CommandZPop(bool min) : min_(min) {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 2) {
      auto parse_result = ParseInt<int>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> member_scores;
    auto s = zset_db.Pop(args_[1], count_, min_, &member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(Redis::MultiLen(member_scores.size() * 2));
    for (const auto &ms : member_scores) {
      output->append(Redis::BulkString(ms.member));
      output->append(Redis::BulkString(Util::Float2String(ms.score)));
    }

    return Status::OK();
  }

 private:
  bool min_;
  int count_ = 1;
};

class CommandZPopMin : public CommandZPop {
 public:
  CommandZPopMin() : CommandZPop(true) {}
};

class CommandZPopMax : public CommandZPop {
 public:
  CommandZPopMax() : CommandZPop(false) {}
};

class CommandZRange : public Commander {
 public:
  explicit CommandZRange(bool reversed = false) : reversed_(reversed) {}

  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    start_ = *parse_start;
    stop_ = *parse_stop;
    if (args.size() > 4 && (Util::ToLower(args[4]) == "withscores")) {
      with_scores_ = true;
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> member_scores;
    uint8_t flags = !reversed_ ? 0 : kZSetReversed;
    auto s = zset_db.Range(args_[1], start_, stop_, flags, &member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!with_scores_) {
      output->append(Redis::MultiLen(member_scores.size()));
    } else {
      output->append(Redis::MultiLen(member_scores.size() * 2));
    }

    for (const auto &ms : member_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_) output->append(Redis::BulkString(Util::Float2String(ms.score)));
    }

    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
  bool reversed_;
  bool with_scores_ = false;
};

class CommandZRevRange : public CommandZRange {
 public:
  CommandZRevRange() : CommandZRange(true) {}
};

class CommandZRangeByLex : public Commander {
 public:
  explicit CommandZRangeByLex(bool reversed = false) { spec_.reversed = reversed; }

  Status Parse(const std::vector<std::string> &args) override {
    Status s;
    if (spec_.reversed) {
      s = ParseRangeLexSpec(args[3], args[2], &spec_);
    } else {
      s = ParseRangeLexSpec(args[2], args[3], &spec_);
    }

    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }

    if (args.size() == 7 && Util::ToLower(args[4]) == "limit") {
      auto parse_offset = ParseInt<int>(args[5], 10);
      auto parse_count = ParseInt<int>(args[6], 10);
      if (!parse_offset || !parse_count) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      spec_.offset = *parse_offset;
      spec_.count = *parse_count;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    auto s = zset_db.RangeByLex(args_[1], spec_, &members, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }

 private:
  CommonRangeLexSpec spec_;
};

class CommandZRangeByScore : public Commander {
 public:
  explicit CommandZRangeByScore(bool reversed = false) { spec_.reversed = reversed; }
  Status Parse(const std::vector<std::string> &args) override {
    Status s;
    if (spec_.reversed) {
      s = Redis::ZSet::ParseRangeSpec(args[3], args[2], &spec_);
    } else {
      s = Redis::ZSet::ParseRangeSpec(args[2], args[3], &spec_);
    }

    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }

    size_t i = 4;
    while (i < args.size()) {
      if (Util::ToLower(args[i]) == "withscores") {
        with_scores_ = true;
        i++;
      } else if (Util::ToLower(args[i]) == "limit" && i + 2 < args.size()) {
        auto parse_offset = ParseInt<int>(args[i + 1], 10);
        auto parse_count = ParseInt<int>(args[i + 2], 10);
        if (!parse_offset || !parse_count) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        spec_.offset = *parse_offset;
        spec_.count = *parse_count;
        i += 3;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> member_scores;
    auto s = zset_db.RangeByScore(args_[1], spec_, &member_scores, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!with_scores_) {
      output->append(Redis::MultiLen(member_scores.size()));
    } else {
      output->append(Redis::MultiLen(member_scores.size() * 2));
    }

    for (const auto &ms : member_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_) output->append(Redis::BulkString(Util::Float2String(ms.score)));
    }

    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
  bool with_scores_ = false;
};

class CommandZRank : public Commander {
 public:
  explicit CommandZRank(bool reversed = false) : reversed_(reversed) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int rank = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Rank(args_[1], args_[2], reversed_, &rank);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (rank == -1) {
      *output = Redis::NilString();
    } else {
      *output = Redis::Integer(rank);
    }
    return Status::OK();
  }

 private:
  bool reversed_;
};

class CommandZRevRank : public CommandZRank {
 public:
  CommandZRevRank() : CommandZRank(true) {}
};

class CommandZRevRangeByScore : public CommandZRangeByScore {
 public:
  CommandZRevRangeByScore() : CommandZRangeByScore(true) {}
};

class CommandZRevRangeByLex : public CommandZRangeByLex {
 public:
  CommandZRevRangeByLex() : CommandZRangeByLex(true) {}
};

class CommandZRem : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<rocksdb::Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }

    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Remove(args_[1], members, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }
};

class CommandZRemRangeByRank : public Commander {
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
    int ret = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.RemoveRangeByRank(args_[1], start_, stop_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandZRemRangeByScore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = Redis::ZSet::ParseRangeSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.RemoveRangeByScore(args_[1], spec_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
};

class CommandZRemRangeByLex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = ParseRangeLexSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.RemoveRangeByLex(args_[1], spec_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  CommonRangeLexSpec spec_;
};

class CommandZScore : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double score = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Score(args_[1], args_[2], &score);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(Util::Float2String(score));
    }
    return Status::OK();
  }
};

class CommandZMScore : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }
    std::map<std::string, double> mscores;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.MGet(args_[1], members, &mscores);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> values;
    if (s.IsNotFound()) {
      values.resize(members.size(), "");
    } else {
      for (const auto &member : members) {
        auto iter = mscores.find(member.ToString());
        if (iter == mscores.end()) {
          values.emplace_back("");
        } else {
          values.emplace_back(Util::Float2String(iter->second));
        }
      }
    }
    *output = Redis::MultiBulkString(values);
    return Status::OK();
  }
};

class CommandZUnionStore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    numkeys_ = *parse_result;
    if (numkeys_ > args.size() - 3) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    size_t j = 0;
    while (j < numkeys_) {
      keys_weights_.emplace_back(KeyWeight{args[j + 3], 1});
      j++;
    }

    size_t i = 3 + numkeys_;
    while (i < args.size()) {
      if (Util::ToLower(args[i]) == "aggregate" && i + 1 < args.size()) {
        if (Util::ToLower(args[i + 1]) == "sum") {
          aggregate_method_ = kAggregateSum;
        } else if (Util::ToLower(args[i + 1]) == "min") {
          aggregate_method_ = kAggregateMin;
        } else if (Util::ToLower(args[i + 1]) == "max") {
          aggregate_method_ = kAggregateMax;
        } else {
          return {Status::RedisParseErr, "aggregate param error"};
        }
        i += 2;
      } else if (Util::ToLower(args[i]) == "weights" && i + numkeys_ < args.size()) {
        size_t k = 0;
        while (k < numkeys_) {
          auto weight = ParseFloat(args[i + k + 1]);
          if (!weight || std::isnan(*weight)) {
            return {Status::RedisParseErr, "weight is not a double or out of range"};
          }
          keys_weights_[k].weight = *weight;

          k++;
        }
        i += numkeys_ + 1;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.UnionStore(args_[1], keys_weights_, aggregate_method_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }

 protected:
  size_t numkeys_ = 0;
  std::vector<KeyWeight> keys_weights_;
  AggregateMethod aggregate_method_ = kAggregateSum;
};

class CommandZInterStore : public CommandZUnionStore {
 public:
  CommandZInterStore() : CommandZUnionStore() {}

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.InterStore(args_[1], keys_weights_, aggregate_method_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }
};

class CommandGeoBase : public Commander {
 public:
  Status ParseDistanceUnit(const std::string &param) {
    if (Util::ToLower(param) == "m") {
      distance_unit_ = kDistanceMeter;
    } else if (Util::ToLower(param) == "km") {
      distance_unit_ = kDistanceKilometers;
    } else if (Util::ToLower(param) == "ft") {
      distance_unit_ = kDistanceFeet;
    } else if (Util::ToLower(param) == "mi") {
      distance_unit_ = kDistanceMiles;
    } else {
      return {Status::RedisParseErr, "unsupported unit provided. please use M, KM, FT, MI"};
    }
    return Status::OK();
  }

  Status ParseLongLat(const std::string &longitude_para, const std::string &latitude_para, double *longitude,
                      double *latitude) {
    auto long_stat = ParseFloat(longitude_para);
    auto lat_stat = ParseFloat(latitude_para);
    if (!long_stat || !lat_stat) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    *longitude = *long_stat;
    *latitude = *lat_stat;

    if (*longitude < GEO_LONG_MIN || *longitude > GEO_LONG_MAX || *latitude < GEO_LAT_MIN || *latitude > GEO_LAT_MAX) {
      return {Status::RedisParseErr, "invalid longitude,latitude pair " + longitude_para + "," + latitude_para};
    }

    return Status::OK();
  }

  double GetDistanceByUnit(double distance) { return distance / GetUnitConversion(); }

  double GetRadiusMeters(double radius) { return radius * GetUnitConversion(); }

  double GetUnitConversion() {
    double conversion = 0;
    switch (distance_unit_) {
      case kDistanceMeter:
        conversion = 1;
        break;
      case kDistanceKilometers:
        conversion = 1000;
        break;
      case kDistanceFeet:
        conversion = 0.3048;
        break;
      case kDistanceMiles:
        conversion = 1609.34;
        break;
    }
    return conversion;
  }

 protected:
  DistanceUnit distance_unit_ = kDistanceMeter;
};

class CommandGeoAdd : public CommandGeoBase {
 public:
  CommandGeoAdd() : CommandGeoBase() {}
  Status Parse(const std::vector<std::string> &args) override {
    if ((args.size() - 5) % 3 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    for (size_t i = 2; i < args.size(); i += 3) {
      double longitude = 0;
      double latitude = 0;
      auto s = ParseLongLat(args[i], args[i + 1], &longitude, &latitude);
      if (!s.IsOK()) return s;

      geo_points_.emplace_back(GeoPoint{longitude, latitude, args[i + 2]});
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Add(args_[1], &geo_points_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<GeoPoint> geo_points_;
};

class CommandGeoDist : public CommandGeoBase {
 public:
  CommandGeoDist() : CommandGeoBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 5) {
      auto s = ParseDistanceUnit(args[4]);
      if (!s.IsOK()) return s;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double distance = 0;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Dist(args_[1], args_[2], args_[3], &distance);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(Util::Float2String(GetDistanceByUnit(distance)));
    }
    return Status::OK();
  }
};

class CommandGeoHash : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); i++) {
      members_.emplace_back(args[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<std::string> hashes;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Hash(args_[1], members_, &hashes);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(hashes);
    return Status::OK();
  }

 private:
  std::vector<Slice> members_;
};

class CommandGeoPos : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); i++) {
      members_.emplace_back(args[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::map<std::string, GeoPoint> geo_points;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Pos(args_[1], members_, &geo_points);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> list;
    for (const auto &member : members_) {
      auto iter = geo_points.find(member.ToString());
      if (iter == geo_points.end()) {
        list.emplace_back(Redis::NilString());
      } else {
        list.emplace_back(Redis::MultiBulkString(
            {Util::Float2String(iter->second.longitude), Util::Float2String(iter->second.latitude)}));
      }
    }
    *output = Redis::Array(list);
    return Status::OK();
  }

 private:
  std::vector<Slice> members_;
};

class CommandGeoRadius : public CommandGeoBase {
 public:
  CommandGeoRadius() : CommandGeoBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    auto s = ParseLongLat(args[2], args[3], &longitude_, &latitude_);
    if (!s.IsOK()) return s;

    auto radius = ParseFloat(args[4]);
    if (!radius) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    radius_ = *radius;

    s = ParseDistanceUnit(args[5]);
    if (!s.IsOK()) return s;

    s = ParseRadiusExtraOption();
    if (!s.IsOK()) return s;

    return Commander::Parse(args);
  }

  Status ParseRadiusExtraOption(size_t i = 6) {
    while (i < args_.size()) {
      if (Util::ToLower(args_[i]) == "withcoord") {
        with_coord_ = true;
        i++;
      } else if (Util::ToLower(args_[i]) == "withdist") {
        with_dist_ = true;
        i++;
      } else if (Util::ToLower(args_[i]) == "withhash") {
        with_hash_ = true;
        i++;
      } else if (Util::ToLower(args_[i]) == "asc") {
        sort_ = kSortASC;
        i++;
      } else if (Util::ToLower(args_[i]) == "desc") {
        sort_ = kSortDESC;
        i++;
      } else if (Util::ToLower(args_[i]) == "count" && i + 1 < args_.size()) {
        auto parse_result = ParseInt<int>(args_[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
        i += 2;
      } else if (attributes_->is_write() &&
                 (Util::ToLower(args_[i]) == "store" || Util::ToLower(args_[i]) == "storedist") &&
                 i + 1 < args_.size()) {
        store_key_ = args_[i + 1];
        if (Util::ToLower(args_[i]) == "storedist") {
          store_distance_ = true;
        }
        i += 2;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    /* Trap options not compatible with STORE and STOREDIST. */
    if (!store_key_.empty() && (with_dist_ || with_hash_ || with_coord_)) {
      return {Status::RedisParseErr,
              "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options"};
    }

    /* COUNT without ordering does not make much sense, force ASC
     * ordering if COUNT was specified but no sorting was requested.
     * */
    if (count_ != 0 && sort_ == kSortNone) {
      sort_ = kSortASC;
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Radius(args_[1], longitude_, latitude_, GetRadiusMeters(radius_), count_, sort_, store_key_,
                           store_distance_, GetUnitConversion(), &geo_points);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (store_key_.size() != 0) {
      *output = Redis::Integer(geo_points.size());
    } else {
      *output = GenerateOutput(geo_points);
    }
    return Status::OK();
  }

  std::string GenerateOutput(const std::vector<GeoPoint> &geo_points) {
    int result_length = static_cast<int>(geo_points.size());
    int returned_items_count = (count_ == 0 || result_length < count_) ? result_length : count_;
    std::vector<std::string> list;
    for (int i = 0; i < returned_items_count; i++) {
      auto geo_point = geo_points[i];
      if (!with_coord_ && !with_hash_ && !with_dist_) {
        list.emplace_back(Redis::BulkString(geo_point.member));
      } else {
        std::vector<std::string> one;
        one.emplace_back(Redis::BulkString(geo_point.member));
        if (with_dist_) {
          one.emplace_back(Redis::BulkString(Util::Float2String(GetDistanceByUnit(geo_point.dist))));
        }
        if (with_hash_) {
          one.emplace_back(Redis::BulkString(Util::Float2String(geo_point.score)));
        }
        if (with_coord_) {
          one.emplace_back(Redis::MultiBulkString(
              {Util::Float2String(geo_point.longitude), Util::Float2String(geo_point.latitude)}));
        }
        list.emplace_back(Redis::Array(one));
      }
    }
    return Redis::Array(list);
  }

 protected:
  double radius_ = 0;
  bool with_coord_ = false;
  bool with_dist_ = false;
  bool with_hash_ = false;
  int count_ = 0;
  DistanceSort sort_ = kSortNone;
  std::string store_key_;
  bool store_distance_ = false;

 private:
  double longitude_ = 0;
  double latitude_ = 0;
};

class CommandGeoRadiusByMember : public CommandGeoRadius {
 public:
  CommandGeoRadiusByMember() : CommandGeoRadius() {}

  Status Parse(const std::vector<std::string> &args) override {
    auto radius = ParseFloat(args[3]);
    if (!radius) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    radius_ = *radius;

    auto s = ParseDistanceUnit(args[4]);
    if (!s.IsOK()) return s;

    s = ParseRadiusExtraOption(5);
    if (!s.IsOK()) return s;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.RadiusByMember(args_[1], args_[2], GetRadiusMeters(radius_), count_, sort_, store_key_,
                                   store_distance_, GetUnitConversion(), &geo_points);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = GenerateOutput(geo_points);
    return Status::OK();
  }
};

class CommandGeoRadiusReadonly : public CommandGeoRadius {
 public:
  CommandGeoRadiusReadonly() : CommandGeoRadius() {}
};

class CommandGeoRadiusByMemberReadonly : public CommandGeoRadiusByMember {
 public:
  CommandGeoRadiusByMemberReadonly() : CommandGeoRadiusByMember() {}
};

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
  explicit CommandSortedintRangeByValue(bool reversed = false) { spec_.reversed = reversed; }

  Status Parse(const std::vector<std::string> &args) override {
    Status s;
    if (spec_.reversed) {
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

      spec_.offset = *parse_offset;
      spec_.count = *parse_count;
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

class CommandInfo : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string section = "all";
    if (args_.size() == 2) {
      section = Util::ToLower(args_[1]);
    }
    std::string info;
    svr->GetInfo(conn->GetNamespace(), section, &info);
    *output = Redis::BulkString(info);
    return Status::OK();
  }
};

class CommandDisk : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    std::string opname = Util::ToLower(args[1]);
    if (opname != "usage") return {Status::RedisInvalidCmd, "Unknown operation"};
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisType type = kRedisNone;
    Redis::Disk disk_db(svr->storage_, conn->GetNamespace());
    auto s = disk_db.Type(args_[2], &type);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    uint64_t result = 0;
    s = disk_db.GetKeySize(args_[2], type, &result);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = Redis::Integer(result);
    return Status::OK();
  }
};

class CommandRole : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    svr->GetRoleInfo(output);
    return Status::OK();
  }
};

class CommandMulti : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = Redis::Error("ERR MULTI calls can not be nested");
      return Status::OK();
    }
    conn->ResetMultiExec();
    // Client starts into MULTI-EXEC
    conn->EnableFlag(Connection::kMultiExec);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandDiscard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = Redis::Error("ERR DISCARD without MULTI");
      return Status::OK();
    }

    conn->ResetMultiExec();
    *output = Redis::SimpleString("OK");

    return Status::OK();
  }
};

class CommandExec : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = Redis::Error("ERR EXEC without MULTI");
      return Status::OK();
    }

    if (conn->IsMultiError()) {
      conn->ResetMultiExec();
      *output = Redis::Error("EXECABORT Transaction discarded");
      return Status::OK();
    }

    // Reply multi length first
    conn->Reply(Redis::MultiLen(conn->GetMultiExecCommands()->size()));
    // Execute multi-exec commands
    conn->SetInExec();
    conn->ExecuteCommands(conn->GetMultiExecCommands());
    conn->ResetMultiExec();
    return Status::OK();
  }
};

class CommandCompact : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string begin_key, end_key;
    auto ns = conn->GetNamespace();

    if (ns != kDefaultNamespace) {
      std::string prefix;
      ComposeNamespaceKey(ns, "", &prefix, false);

      Redis::Database redis_db(svr->storage_, conn->GetNamespace());
      auto s = redis_db.FindKeyRangeWithPrefix(prefix, std::string(), &begin_key, &end_key);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          *output = Redis::SimpleString("OK");
          return Status::OK();
        }

        return {Status::RedisExecErr, s.ToString()};
      }
    }

    Status s = svr->AsyncCompactDB(begin_key, end_key);
    if (!s.IsOK()) return s;

    *output = Redis::SimpleString("OK");
    LOG(INFO) << "Compact was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandBGSave : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    Status s = svr->AsyncBgsaveDB();
    if (!s.IsOK()) return s;

    *output = Redis::SimpleString("OK");
    LOG(INFO) << "BGSave was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandFlushBackup : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    Status s = svr->AsyncPurgeOldBackups(0, 0);
    if (!s.IsOK()) return s;

    *output = Redis::SimpleString("OK");
    LOG(INFO) << "flushbackup was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandDBSize : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string ns = conn->GetNamespace();
    if (args_.size() == 1) {
      KeyNumStats stats;
      svr->GetLastestKeyNumStats(ns, &stats);
      *output = Redis::Integer(stats.n_key);
    } else if (args_.size() == 2 && args_[1] == "scan") {
      Status s = svr->AsyncScanDBSize(ns);
      if (s.IsOK()) {
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else {
      *output = Redis::Error("DBSIZE subcommand only supports scan");
    }
    return Status::OK();
  }
};

class CommandPublish : public Commander {
 public:
  // mark is_write as false here because slave should be able to execute publish command
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!svr->IsSlave()) {
      // Compromise: can't replicate message to sub-replicas in a cascading-like structure.
      // Replication relies on WAL seq, increase the seq on slave will break the replication, hence the compromise
      Redis::PubSub pubsub_db(svr->storage_);
      auto s = pubsub_db.Publish(args_[1], args_[2]);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
    }

    int receivers = svr->PublishMessage(args_[1], args_[2]);
    *output = Redis::Integer(receivers);
    return Status::OK();
  }
};

void SubscribeCommandReply(std::string *output, const std::string &name, const std::string &sub_name, int num) {
  output->append(Redis::MultiLen(3));
  output->append(Redis::BulkString(name));
  output->append(sub_name.empty() ? Redis::NilString() : Redis::BulkString(sub_name));
  output->append(Redis::Integer(num));
}

class CommandSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    for (unsigned i = 1; i < args_.size(); i++) {
      conn->SubscribeChannel(args_[i]);
      SubscribeCommandReply(output, "subscribe", args_[i], conn->SubscriptionsCount() + conn->PSubscriptionsCount());
    }
    return Status::OK();
  }
};

class CommandUnSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      conn->UnSubscribeAll([output](const std::string &sub_name, int num) {
        SubscribeCommandReply(output, "unsubscribe", sub_name, num);
      });
    } else {
      for (size_t i = 1; i < args_.size(); i++) {
        conn->UnSubscribeChannel(args_[i]);
        SubscribeCommandReply(output, "unsubscribe", args_[i],
                              conn->SubscriptionsCount() + conn->PSubscriptionsCount());
      }
    }
    return Status::OK();
  }
};

class CommandPSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    for (size_t i = 1; i < args_.size(); i++) {
      conn->PSubscribeChannel(args_[i]);
      SubscribeCommandReply(output, "psubscribe", args_[i], conn->SubscriptionsCount() + conn->PSubscriptionsCount());
    }
    return Status::OK();
  }
};

class CommandPUnSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      conn->PUnSubscribeAll([output](const std::string &sub_name, int num) {
        SubscribeCommandReply(output, "punsubscribe", sub_name, num);
      });
    } else {
      for (size_t i = 1; i < args_.size(); i++) {
        conn->PUnSubscribeChannel(args_[i]);
        SubscribeCommandReply(output, "punsubscribe", args_[i],
                              conn->SubscriptionsCount() + conn->PSubscriptionsCount());
      }
    }
    return Status::OK();
  }
};

class CommandPubSub : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if (subcommand_ == "numpat" && args.size() == 2) {
      return Status::OK();
    }

    if ((subcommand_ == "numsub") && args.size() >= 2) {
      if (args.size() > 2) {
        channels_ = std::vector<std::string>(args.begin() + 2, args.end());
      }
      return Status::OK();
    }

    if ((subcommand_ == "channels") && args.size() <= 3) {
      if (args.size() == 3) {
        pattern_ = args[2];
      }
      return Status::OK();
    }

    return {Status::RedisInvalidCmd, "Unknown subcommand or wrong number of arguments"};
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "numpat") {
      *output = Redis::Integer(srv->GetPubSubPatternSize());
      return Status::OK();
    }

    if (subcommand_ == "numsub") {
      std::vector<ChannelSubscribeNum> channel_subscribe_nums;
      srv->ListChannelSubscribeNum(channels_, &channel_subscribe_nums);

      output->append(Redis::MultiLen(channel_subscribe_nums.size() * 2));
      for (const auto &chan_subscribe_num : channel_subscribe_nums) {
        output->append(Redis::BulkString(chan_subscribe_num.channel));
        output->append(Redis::Integer(chan_subscribe_num.subscribe_num));
      }

      return Status::OK();
    }

    if (subcommand_ == "channels") {
      std::vector<std::string> channels;
      srv->GetChannelsByPattern(pattern_, &channels);
      *output = Redis::MultiBulkString(channels);
      return Status::OK();
    }

    return {Status::RedisInvalidCmd, "Unknown subcommand or wrong number of arguments"};
  }

 private:
  std::string pattern_;
  std::vector<std::string> channels_;
  std::string subcommand_;
};

class CommandSlaveOf : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    host_ = args[1];
    const auto &port = args[2];
    if (Util::ToLower(host_) == "no" && Util::ToLower(port) == "one") {
      host_.clear();
      return Status::OK();
    }

    auto parse_result = ParseInt<uint32_t>(port, 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "port should be number"};
    }

    port_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (svr->GetConfig()->cluster_enabled) {
      return {Status::RedisExecErr, "can't change to slave in cluster mode"};
    }

    if (svr->GetConfig()->RocksDB.write_options.disable_WAL) {
      return {Status::RedisExecErr, "slaveof doesn't work with disable_wal option"};
    }

    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    if (host_.empty()) {
      auto s = svr->RemoveMaster();
      if (!s.IsOK()) {
        return s.Prefixed("failed to remove master");
      }

      *output = Redis::SimpleString("OK");
      LOG(WARNING) << "MASTER MODE enabled (user request from '" << conn->GetAddr() << "')";
      if (svr->GetConfig()->cluster_enabled) {
        svr->slot_migrate_->SetMigrateStopFlag(false);
        LOG(INFO) << "Change server role to master, restart migration task";
      }

      return Status::OK();
    }

    auto s = svr->AddMaster(host_, port_, false);
    if (s.IsOK()) {
      *output = Redis::SimpleString("OK");
      LOG(WARNING) << "SLAVE OF " << host_ << ":" << port_ << " enabled (user request from '" << conn->GetAddr()
                   << "')";
      if (svr->GetConfig()->cluster_enabled) {
        svr->slot_migrate_->SetMigrateStopFlag(true);
        LOG(INFO) << "Change server role to slave, stop migration task";
      }
    } else {
      LOG(ERROR) << "SLAVE OF " << host_ << ":" << port_ << " (user request from '" << conn->GetAddr()
                 << "') encounter error: " << s.Msg();
    }

    return s;
  }

 private:
  std::string host_;
  uint32_t port_ = 0;
};

class CommandStats : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string stats_json = svr->GetRocksDBStatsJson();
    *output = Redis::BulkString(stats_json);
    return Status::OK();
  }
};

class CommandPSync : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t seq_arg = 1;
    if (args.size() == 3) {
      seq_arg = 2;
      new_psync = true;
    }

    auto parse_result = ParseInt<uint64_t>(args[seq_arg], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "value is not an unsigned long long or out of range"};
    }

    next_repl_seq = static_cast<rocksdb::SequenceNumber>(*parse_result);
    if (new_psync) {
      assert(args.size() == 3);
      replica_replid = args[1];
      if (replica_replid.size() != kReplIdLength) {
        return {Status::RedisParseErr, "Wrong replication id length"};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    LOG(INFO) << "Slave " << conn->GetAddr() << ", listening port: " << conn->GetListeningPort()
              << " asks for synchronization"
              << " with next sequence: " << next_repl_seq
              << " replication id: " << (replica_replid.length() ? replica_replid : "not supported")
              << ", and local sequence: " << svr->storage_->LatestSeq();

    bool need_full_sync = false;

    // Check replication id of the last sequence log
    if (new_psync && svr->GetConfig()->use_rsid_psync) {
      std::string replid_in_wal = svr->storage_->GetReplIdFromWalBySeq(next_repl_seq - 1);
      LOG(INFO) << "Replication id in WAL: " << replid_in_wal;

      // We check replication id only when WAL has this sequence, since there may be no WAL,
      // Or WAL may have nothing when starting from db of old version kvrocks.
      if (replid_in_wal.length() == kReplIdLength && replid_in_wal != replica_replid) {
        *output = "wrong replication id of the last log";
        need_full_sync = true;
      }
    }

    // Check Log sequence
    if (!need_full_sync && !checkWALBoundary(svr->storage_, next_repl_seq).IsOK()) {
      *output = "sequence out of range, please use fullsync";
      need_full_sync = true;
    }

    if (need_full_sync) {
      svr->stats_.IncrPSyncErrCounter();
      return {Status::RedisExecErr, *output};
    }

    // Server would spawn a new thread to sync the batch, and connection would
    // be taken over, so should never trigger any event in worker thread.
    conn->Detach();
    conn->EnableFlag(Redis::Connection::kSlave);
    auto s = Util::SockSetBlocking(conn->GetFD(), 1);
    if (!s.IsOK()) {
      conn->EnableFlag(Redis::Connection::kCloseAsync);
      return s.Prefixed("failed to set blocking mode on socket");
    }

    svr->stats_.IncrPSyncOKCounter();
    s = svr->AddSlave(conn, next_repl_seq);
    if (!s.IsOK()) {
      std::string err = "-ERR " + s.Msg() + "\r\n";
      s = Util::SockSend(conn->GetFD(), err);
      if (!s.IsOK()) {
        LOG(WARNING) << "failed to send error message to the replica: " << s.Msg();
      }
      conn->EnableFlag(Redis::Connection::kCloseAsync);
      LOG(WARNING) << "Failed to add replica: " << conn->GetAddr() << " to start incremental syncing";
    } else {
      LOG(INFO) << "New replica: " << conn->GetAddr() << " was added, start incremental syncing";
    }
    return s;
  }

 private:
  rocksdb::SequenceNumber next_repl_seq = 0;
  bool new_psync = false;
  std::string replica_replid;

  // Return OK if the seq is in the range of the current WAL
  Status checkWALBoundary(Engine::Storage *storage, rocksdb::SequenceNumber seq) {
    if (seq == storage->LatestSeq() + 1) {
      return Status::OK();
    }

    // Upper bound
    if (seq > storage->LatestSeq() + 1) {
      return {Status::NotOK};
    }

    // Lower bound
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto s = storage->GetWALIter(seq, &iter);
    if (s.IsOK() && iter->Valid()) {
      auto batch = iter->GetBatch();
      if (seq != batch.sequence) {
        if (seq > batch.sequence) {
          LOG(ERROR) << "checkWALBoundary with sequence: " << seq
                     << ", but GetWALIter return older sequence: " << batch.sequence;
        }
        return {Status::NotOK};
      }
      return Status::OK();
    }
    return {Status::NotOK};
  }
};

class CommandPerfLog : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if (subcommand_ != "reset" && subcommand_ != "get" && subcommand_ != "len") {
      return {Status::NotOK, "PERFLOG subcommand must be one of RESET, LEN, GET"};
    }

    if (subcommand_ == "get" && args.size() >= 3) {
      if (args[2] == "*") {
        cnt_ = 0;
      } else {
        cnt_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
      }
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    auto perf_log = srv->GetPerfLog();
    if (subcommand_ == "len") {
      *output = Redis::Integer(static_cast<int64_t>(perf_log->Size()));
    } else if (subcommand_ == "reset") {
      perf_log->Reset();
      *output = Redis::SimpleString("OK");
    } else if (subcommand_ == "get") {
      *output = perf_log->GetLatestEntries(cnt_);
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  int64_t cnt_ = 10;
};

class CommandSlowlog : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if (subcommand_ != "reset" && subcommand_ != "get" && subcommand_ != "len") {
      return {Status::NotOK, "SLOWLOG subcommand must be one of RESET, LEN, GET"};
    }

    if (subcommand_ == "get" && args.size() >= 3) {
      if (args[2] == "*") {
        cnt_ = 0;
      } else {
        cnt_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
      }
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    auto slowlog = srv->GetSlowLog();
    if (subcommand_ == "reset") {
      slowlog->Reset();
      *output = Redis::SimpleString("OK");
      return Status::OK();
    } else if (subcommand_ == "len") {
      *output = Redis::Integer(static_cast<int64_t>(slowlog->Size()));
      return Status::OK();
    } else if (subcommand_ == "get") {
      *output = slowlog->GetLatestEntries(cnt_);
      return Status::OK();
    }
    return {Status::NotOK, "SLOWLOG subcommand must be one of RESET, LEN, GET"};
  }

 private:
  std::string subcommand_;
  int64_t cnt_ = 10;
};

class CommandClient : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    // subcommand: getname id kill list setname
    if ((subcommand_ == "id" || subcommand_ == "getname" || subcommand_ == "list") && args.size() == 2) {
      return Status::OK();
    }

    if ((subcommand_ == "setname") && args.size() == 3) {
      // Check if the charset is ok. We need to do this otherwise
      // CLIENT LIST format will break. You should always be able to
      // split by space to get the different fields.
      for (auto ch : args[2]) {
        if (ch < '!' || ch > '~') {
          return {Status::RedisInvalidCmd, "Client names cannot contain spaces, newlines or special characters"};
        }
      }

      conn_name_ = args[2];
      return Status::OK();
    }

    if ((subcommand_ == "kill")) {
      if (args.size() == 2) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      if (args.size() == 3) {
        addr_ = args[2];
        new_format_ = false;
        return Status::OK();
      }

      size_t i = 2;
      new_format_ = true;

      while (i < args.size()) {
        bool more_args = i < args.size();
        if (!strcasecmp(args[i].c_str(), "addr") && more_args) {
          addr_ = args[i + 1];
        } else if (!strcasecmp(args[i].c_str(), "id") && more_args) {
          auto parse_result = ParseInt<uint64_t>(args[i + 1], 10);
          if (!parse_result) {
            return {Status::RedisParseErr, errValueNotInteger};
          }

          id_ = *parse_result;
        } else if (!strcasecmp(args[i].c_str(), "skipme") && more_args) {
          if (!strcasecmp(args[i + 1].c_str(), "yes")) {
            skipme_ = true;
          } else if (!strcasecmp(args[i + 1].c_str(), "no")) {
            skipme_ = false;
          } else {
            return {Status::RedisParseErr, errInvalidSyntax};
          }
        } else if (!strcasecmp(args[i].c_str(), "type") && more_args) {
          if (!strcasecmp(args[i + 1].c_str(), "normal")) {
            kill_type_ |= kTypeNormal;
          } else if (!strcasecmp(args[i + 1].c_str(), "pubsub")) {
            kill_type_ |= kTypePubsub;
          } else if (!strcasecmp(args[i + 1].c_str(), "master")) {
            kill_type_ |= kTypeMaster;
          } else if (!strcasecmp(args[i + 1].c_str(), "replica") || !strcasecmp(args[i + 1].c_str(), "slave")) {
            kill_type_ |= kTypeSlave;
          } else {
            return {Status::RedisParseErr, errInvalidSyntax};
          }
        } else {
          return {Status::RedisParseErr, errInvalidSyntax};
        }
        i += 2;
      }
      return Status::OK();
    }
    return {Status::RedisInvalidCmd, "Syntax error, try CLIENT LIST|KILL ip:port|GETNAME|SETNAME"};
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "list") {
      *output = Redis::BulkString(srv->GetClientsStr());
      return Status::OK();
    } else if (subcommand_ == "setname") {
      conn->SetName(conn_name_);
      *output = Redis::SimpleString("OK");
      return Status::OK();
    } else if (subcommand_ == "getname") {
      std::string name = conn->GetName();
      *output = name == "" ? Redis::NilString() : Redis::BulkString(name);
      return Status::OK();
    } else if (subcommand_ == "id") {
      *output = Redis::Integer(conn->GetID());
      return Status::OK();
    } else if (subcommand_ == "kill") {
      int64_t killed = 0;
      srv->KillClient(&killed, addr_, id_, kill_type_, skipme_, conn);
      if (new_format_) {
        *output = Redis::Integer(killed);
      } else {
        if (killed == 0)
          *output = Redis::Error("No such client");
        else
          *output = Redis::SimpleString("OK");
      }
      return Status::OK();
    }

    return {Status::RedisInvalidCmd, "Syntax error, try CLIENT LIST|KILL ip:port|GETNAME|SETNAME"};
  }

 private:
  std::string addr_;
  std::string conn_name_;
  std::string subcommand_;
  bool skipme_ = false;
  int64_t kill_type_ = 0;
  uint64_t id_ = 0;
  bool new_format_ = true;
};

class CommandMonitor : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    conn->Owner()->BecomeMonitorConn(conn);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandShutdown : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    if (!srv->IsStopped()) {
      LOG(INFO) << "bye bye";
      srv->Stop();
    }
    return Status::OK();
  }
};

class CommandQuit : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    conn->EnableFlag(Redis::Connection::kCloseAfterReply);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandDebug : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if ((subcommand_ == "sleep") && args.size() == 3) {
      auto second = ParseFloat(args[2]);
      if (!second) {
        return {Status::RedisParseErr, "invalid debug sleep time"};
      }

      microsecond_ = static_cast<uint64_t>(*second * 1000 * 1000);
      return Status::OK();
    }
    return {Status::RedisInvalidCmd, "Syntax error, DEBUG SLEEP <seconds>"};
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "sleep") {
      usleep(microsecond_);
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  std::string subcommand_;
  uint64_t microsecond_ = 0;
};

class CommandCommand : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      GetAllCommandsInfo(output);
    } else {
      std::string sub_command = Util::ToLower(args_[1]);
      if ((sub_command == "count" && args_.size() != 2) || (sub_command == "getkeys" && args_.size() < 3) ||
          (sub_command == "info" && args_.size() < 3)) {
        *output = Redis::Error(errWrongNumOfArguments);
        return Status::OK();
      }

      if (sub_command == "count") {
        *output = Redis::Integer(GetCommandNum());
      } else if (sub_command == "info") {
        GetCommandsInfo(output, std::vector<std::string>(args_.begin() + 2, args_.end()));
      } else if (sub_command == "getkeys") {
        std::vector<int> keys_indexes;
        auto s = GetKeysFromCommand(args_[2], static_cast<int>(args_.size()) - 2, &keys_indexes);
        if (!s.IsOK()) return s;

        if (keys_indexes.size() == 0) {
          *output = Redis::Error("Invalid arguments specified for command");
          return Status::OK();
        }

        std::vector<std::string> keys;
        keys.reserve(keys_indexes.size());
        for (const auto &key_index : keys_indexes) {
          keys.emplace_back(args_[key_index + 2]);
        }
        *output = Redis::MultiBulkString(keys);
      } else {
        *output = Redis::Error("Command subcommand must be one of COUNT, GETKEYS, INFO");
      }
    }
    return Status::OK();
  }
};

class CommandEcho : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    *output = Redis::BulkString(args_[1]);
    return Status::OK();
  }
};

/* HELLO [<protocol-version> [AUTH <password>] [SETNAME <name>] ] */
class CommandHello final : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    size_t next_arg = 1;
    if (args_.size() >= 2) {
      int64_t protocol = 0;
      auto parse_result = ParseInt<int64_t>(args_[next_arg], 10);
      ++next_arg;
      if (!parse_result) {
        return {Status::NotOK, "Protocol version is not an integer or out of range"};
      }

      protocol = *parse_result;

      // In redis, it will check protocol < 2 or protocol > 3,
      // kvrocks only supports REPL2 by now, but for supporting some
      // `hello 3`, it will not report error when using 3.
      if (protocol < 2 || protocol > 3) {
        return {Status::NotOK, "-NOPROTO unsupported protocol version"};
      }
    }

    // Handling AUTH and SETNAME
    for (; next_arg < args_.size(); ++next_arg) {
      size_t more_args = args_.size() - next_arg - 1;
      const std::string &opt = args_[next_arg];
      if (opt == "AUTH" && more_args != 0) {
        const auto &user_password = args_[next_arg + 1];
        auto authResult = AuthenticateUser(conn, svr->GetConfig(), user_password);
        switch (authResult) {
          case AuthResult::INVALID_PASSWORD:
            return {Status::NotOK, "invalid password"};
          case AuthResult::NO_REQUIRE_PASS:
            return {Status::NotOK, "Client sent AUTH, but no password is set"};
          case AuthResult::OK:
            break;
        }
        next_arg += 1;
      } else if (opt == "SETNAME" && more_args != 0) {
        const std::string &name = args_[next_arg + 1];
        conn->SetName(name);
        next_arg += 1;
      } else {
        *output = Redis::Error("Syntax error in HELLO option " + opt);
        return Status::OK();
      }
    }

    std::vector<std::string> output_list;
    output_list.push_back(Redis::BulkString("server"));
    output_list.push_back(Redis::BulkString("redis"));
    output_list.push_back(Redis::BulkString("proto"));
    output_list.push_back(Redis::Integer(2));

    output_list.push_back(Redis::BulkString("mode"));
    // Note: sentinel is not supported in kvrocks.
    if (svr->GetConfig()->cluster_enabled) {
      output_list.push_back(Redis::BulkString("cluster"));
    } else {
      output_list.push_back(Redis::BulkString("standalone"));
    }
    *output = Redis::Array(output_list);
    return Status::OK();
  }
};

class CommandScanBase : public Commander {
 public:
  Status ParseMatchAndCountParam(const std::string &type, std::string value) {
    if (type == "match") {
      prefix = std::move(value);
      if (!prefix.empty() && prefix[prefix.size() - 1] == '*') {
        prefix = prefix.substr(0, prefix.size() - 1);
        return Status::OK();
      }

      return {Status::RedisParseErr, "only keys prefix match was supported"};
    } else if (type == "count") {
      auto parse_result = ParseInt<int>(value, 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "count param should be type int"};
      }

      limit = *parse_result;
      if (limit <= 0) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Status::OK();
  }

  void ParseCursor(const std::string &param) {
    cursor = param;
    if (cursor == "0") {
      cursor = std::string();
    } else {
      cursor = cursor.find(kCursorPrefix) == 0 ? cursor.substr(strlen(kCursorPrefix)) : cursor;
    }
  }

  std::string GenerateOutput(const std::vector<std::string> &keys) {
    std::vector<std::string> list;
    if (keys.size() == static_cast<size_t>(limit)) {
      list.emplace_back(Redis::BulkString(keys.back()));
    } else {
      list.emplace_back(Redis::BulkString("0"));
    }

    list.emplace_back(Redis::MultiBulkString(keys));

    return Redis::Array(list);
  }

 protected:
  std::string cursor;
  std::string prefix;
  int limit = 20;
};

class CommandSubkeyScanBase : public CommandScanBase {
 public:
  CommandSubkeyScanBase() : CommandScanBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 == 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    key = args[1];
    ParseCursor(args[2]);
    if (args.size() >= 5) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[3]), args_[4]);
      if (!s.IsOK()) {
        return s;
      }
    }

    if (args.size() >= 7) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[5]), args_[6]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }

  std::string GenerateOutput(const std::vector<std::string> &fields, const std::vector<std::string> &values) {
    std::vector<std::string> list;
    auto items_count = fields.size();
    if (items_count == static_cast<size_t>(limit)) {
      list.emplace_back(Redis::BulkString(fields.back()));
    } else {
      list.emplace_back(Redis::BulkString("0"));
    }
    std::vector<std::string> fvs;
    if (items_count > 0) {
      for (size_t i = 0; i < items_count; i++) {
        fvs.emplace_back(fields[i]);
        fvs.emplace_back(values[i]);
      }
    }
    list.emplace_back(Redis::MultiBulkString(fvs, false));
    return Redis::Array(list);
  }

 protected:
  std::string key;
};

class CommandScan : public CommandScanBase {
 public:
  CommandScan() : CommandScanBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    ParseCursor(args[1]);
    if (args.size() >= 4) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[2]), args_[3]);
      if (!s.IsOK()) {
        return s;
      }
    }

    if (args.size() >= 6) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[4]), args_[5]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }

  std::string GenerateOutput(const std::vector<std::string> &keys, std::string end_cursor) {
    std::vector<std::string> list;
    if (!end_cursor.empty()) {
      end_cursor = kCursorPrefix + end_cursor;
      list.emplace_back(Redis::BulkString(end_cursor));
    } else {
      list.emplace_back(Redis::BulkString("0"));
    }

    list.emplace_back(Redis::MultiBulkString(keys));

    return Redis::Array(list);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> keys;
    std::string end_cursor;
    auto s = redis_db.Scan(cursor, limit, prefix, &keys, &end_cursor);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = GenerateOutput(keys, end_cursor);
    return Status::OK();
  }
};

class CommandHScan : public CommandSubkeyScanBase {
 public:
  CommandHScan() : CommandSubkeyScanBase() {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> fields;
    std::vector<std::string> values;
    auto s = hash_db.Scan(key, cursor, limit, prefix, &fields, &values);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = GenerateOutput(fields, values);
    return Status::OK();
  }
};

class CommandSScan : public CommandSubkeyScanBase {
 public:
  CommandSScan() : CommandSubkeyScanBase() {}
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

class CommandZScan : public CommandSubkeyScanBase {
 public:
  CommandZScan() : CommandSubkeyScanBase() {}

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    std::vector<double> scores;
    auto s = zset_db.Scan(key, cursor, limit, prefix, &members, &scores);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> score_strings;
    score_strings.reserve(scores.size());
    for (const auto &score : scores) {
      score_strings.emplace_back(Util::Float2String(score));
    }
    *output = GenerateOutput(members, score_strings);
    return Status::OK();
  }
};

class CommandRandomKey : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string key;
    auto cursor = svr->GetLastRandomKeyCursor();
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    redis.RandomKey(cursor, &key);
    svr->SetLastRandomKeyCursor(key);
    *output = Redis::BulkString(key);
    return Status::OK();
  }
};

class CommandReplConf : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 == 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    if (args.size() >= 3) {
      Status s = ParseParam(Util::ToLower(args[1]), args_[2]);
      if (!s.IsOK()) {
        return s;
      }
    }

    if (args.size() >= 5) {
      Status s = ParseParam(Util::ToLower(args[3]), args_[4]);
      if (!s.IsOK()) {
        return s;
      }
    }

    return Commander::Parse(args);
  }

  Status ParseParam(const std::string &option, const std::string &value) {
    if (option == "listening-port") {
      auto parse_result = ParseInt<int>(value, NumericRange<int>{1, PORT_LIMIT - 1}, 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "listening-port should be number or out of range"};
      }

      port_ = *parse_result;
    } else {
      return {Status::RedisParseErr, "unknown option"};
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (port_ != 0) {
      conn->SetListeningPort(port_);
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int port_ = 0;
};

class CommandFetchMeta : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int repl_fd = conn->GetFD();
    std::string ip = conn->GetIP();

    auto s = Util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotClose();
    conn->EnableFlag(Redis::Connection::kCloseAsync);
    svr->stats_.IncrFullSyncCounter();

    // Feed-replica-meta thread
    std::thread t = std::thread([svr, repl_fd, ip]() {
      Util::ThreadSetName("feed-repl-info");
      UniqueFD unique_fd{repl_fd};

      std::string files;
      auto s = Engine::Storage::ReplDataManager::GetFullReplDataInfo(svr->storage_, &files);
      if (!s.IsOK()) {
        s = Util::SockSend(repl_fd, "-ERR can't create db checkpoint");
        if (!s.IsOK()) {
          LOG(WARNING) << "[replication] Failed to send error response: " << s.Msg();
        }
        LOG(WARNING) << "[replication] Failed to get full data file info: " << s.Msg();
        return;
      }
      // Send full data file info
      if (Util::SockSend(repl_fd, files + CRLF).IsOK()) {
        LOG(INFO) << "[replication] Succeed sending full data file info to " << ip;
      } else {
        LOG(WARNING) << "[replication] Fail to send full data file info " << ip << ", error: " << strerror(errno);
      }
      auto now = static_cast<time_t>(Util::GetTimeStamp());
      svr->storage_->SetCheckpointAccessTime(now);
    });
    t.detach();

    return Status::OK();
  }
};

class CommandFetchFile : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    files_str_ = args[1];
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<std::string> files = Util::Split(files_str_, ",");

    int repl_fd = conn->GetFD();
    std::string ip = conn->GetIP();

    auto s = Util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotClose();  // Feed-replica-file thread will close the replica fd
    conn->EnableFlag(Redis::Connection::kCloseAsync);

    std::thread t = std::thread([svr, repl_fd, ip, files]() {
      Util::ThreadSetName("feed-repl-file");
      UniqueFD unique_fd{repl_fd};
      svr->IncrFetchFileThread();

      for (const auto &file : files) {
        if (svr->IsStopped()) break;

        uint64_t file_size = 0, max_replication_bytes = 0;
        if (svr->GetConfig()->max_replication_mb > 0) {
          max_replication_bytes = (svr->GetConfig()->max_replication_mb * MiB) / svr->GetFetchFileThreadNum();
        }
        auto start = std::chrono::high_resolution_clock::now();
        auto fd = UniqueFD(Engine::Storage::ReplDataManager::OpenDataFile(svr->storage_, file, &file_size));
        if (!fd) break;

        // Send file size and content
        if (Util::SockSend(repl_fd, std::to_string(file_size) + CRLF).IsOK() &&
            Util::SockSendFile(repl_fd, *fd, file_size).IsOK()) {
          LOG(INFO) << "[replication] Succeed sending file " << file << " to " << ip;
        } else {
          LOG(WARNING) << "[replication] Fail to send file " << file << " to " << ip << ", error: " << strerror(errno);
          break;
        }
        fd.Close();

        // Sleep if the speed of sending file is more than replication speed limit
        auto end = std::chrono::high_resolution_clock::now();
        uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        auto shortest = static_cast<uint64_t>(static_cast<double>(file_size) /
                                              static_cast<double>(max_replication_bytes) * (1000 * 1000));
        if (max_replication_bytes > 0 && duration < shortest) {
          LOG(INFO) << "[replication] Need to sleep " << (shortest - duration) / 1000
                    << " ms since of sending files too quickly";
          usleep(shortest - duration);
        }
      }
      auto now = static_cast<time_t>(Util::GetTimeStamp());
      svr->storage_->SetCheckpointAccessTime(now);
      svr->DecrFetchFileThread();
    });
    t.detach();

    return Status::OK();
  }

 private:
  std::string files_str_;
};

class CommandDBName : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    conn->Reply(svr->storage_->GetName() + CRLF);
    return Status::OK();
  }
};

class CommandCluster : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);

    if (args.size() == 2 && (subcommand_ == "nodes" || subcommand_ == "slots" || subcommand_ == "info"))
      return Status::OK();

    if (subcommand_ == "keyslot" && args_.size() == 3) return Status::OK();

    if (subcommand_ == "import") {
      if (args.size() != 4) return {Status::RedisParseErr, errWrongNumOfArguments};
      slot_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));

      auto state = ParseInt<unsigned>(args[3], {kImportStart, kImportNone}, 10);
      if (!state) return {Status::NotOK, "Invalid import state"};

      state_ = static_cast<ImportStatus>(*state);
      return Status::OK();
    }

    return {Status::RedisParseErr, "CLUSTER command, CLUSTER INFO|NODES|SLOTS|KEYSLOT"};
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!svr->GetConfig()->cluster_enabled) {
      *output = Redis::Error("Cluster mode is not enabled");
      return Status::OK();
    }

    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    if (subcommand_ == "keyslot") {
      auto slot_id = GetSlotNumFromKey(args_[2]);
      *output = Redis::Integer(slot_id);
    } else if (subcommand_ == "slots") {
      std::vector<SlotInfo> infos;
      Status s = svr->cluster_->GetSlotsInfo(&infos);
      if (s.IsOK()) {
        output->append(Redis::MultiLen(infos.size()));
        for (const auto &info : infos) {
          output->append(Redis::MultiLen(info.nodes.size() + 2));
          output->append(Redis::Integer(info.start));
          output->append(Redis::Integer(info.end));
          for (const auto &n : info.nodes) {
            output->append(Redis::MultiLen(3));
            output->append(Redis::BulkString(n.host));
            output->append(Redis::Integer(n.port));
            output->append(Redis::BulkString(n.id));
          }
        }
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "nodes") {
      std::string nodes_desc;
      Status s = svr->cluster_->GetClusterNodes(&nodes_desc);
      if (s.IsOK()) {
        *output = Redis::BulkString(nodes_desc);
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "info") {
      std::string cluster_info;
      Status s = svr->cluster_->GetClusterInfo(&cluster_info);
      if (s.IsOK()) {
        *output = Redis::BulkString(cluster_info);
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "import") {
      Status s = svr->cluster_->ImportSlot(conn, static_cast<int>(slot_), state_);
      if (s.IsOK()) {
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else {
      *output = Redis::Error("Invalid cluster command options");
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  int64_t slot_ = -1;
  ImportStatus state_ = kImportNone;
};

class CommandClusterX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);

    if (args.size() == 2 && (subcommand_ == "version")) return Status::OK();

    if (subcommand_ == "setnodeid" && args_.size() == 3 && args_[2].size() == kClusterNodeIdLen) return Status::OK();

    if (subcommand_ == "migrate") {
      if (args.size() != 4) return {Status::RedisParseErr, errWrongNumOfArguments};

      slot_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));

      dst_node_id_ = args[3];
      return Status::OK();
    }

    if (subcommand_ == "setnodes" && args_.size() >= 4) {
      nodes_str_ = args_[2];

      auto parse_result = ParseInt<int64_t>(args[3].c_str(), 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "Invalid version"};
      }

      set_version_ = *parse_result;

      if (args_.size() == 4) return Status::OK();

      if (args_.size() == 5 && strcasecmp(args_[4].c_str(), "force") == 0) {
        force_ = true;
        return Status::OK();
      }

      return {Status::RedisParseErr, "Invalid setnodes options"};
    }

    // CLUSTERX SETSLOT $SLOT_ID NODE $NODE_ID $VERSION
    if (subcommand_ == "setslot" && args_.size() == 6) {
      auto parse_id = ParseInt<int>(args[2].c_str(), 10);
      if (!parse_id) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      slot_id_ = *parse_id;

      if (!Cluster::IsValidSlot(slot_id_)) {
        return {Status::RedisParseErr, "Invalid slot id"};
      }

      if (strcasecmp(args_[3].c_str(), "node") != 0) {
        return {Status::RedisParseErr, "Invalid setslot options"};
      }

      if (args_[4].size() != kClusterNodeIdLen) {
        return {Status::RedisParseErr, "Invalid node id"};
      }

      auto parse_version = ParseInt<int64_t>(args[5].c_str(), 10);
      if (!parse_version) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      if (*parse_version < 0) return {Status::RedisParseErr, "Invalid version"};

      set_version_ = *parse_version;

      return Status::OK();
    }

    return {Status::RedisParseErr, "CLUSTERX command, CLUSTERX VERSION|SETNODEID|SETNODES|SETSLOT|MIGRATE"};
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!svr->GetConfig()->cluster_enabled) {
      *output = Redis::Error("Cluster mode is not enabled");
      return Status::OK();
    }

    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    bool need_persist_nodes_info = false;
    if (subcommand_ == "setnodes") {
      Status s = svr->cluster_->SetClusterNodes(nodes_str_, set_version_, force_);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "setnodeid") {
      Status s = svr->cluster_->SetNodeId(args_[2]);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "setslot") {
      Status s = svr->cluster_->SetSlot(slot_id_, args_[4], set_version_);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "version") {
      int64_t v = svr->cluster_->GetVersion();
      *output = Redis::BulkString(std::to_string(v));
    } else if (subcommand_ == "migrate") {
      Status s = svr->cluster_->MigrateSlot(static_cast<int>(slot_), dst_node_id_);
      if (s.IsOK()) {
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else {
      *output = Redis::Error("Invalid cluster command options");
    }
    if (need_persist_nodes_info && svr->GetConfig()->persist_cluster_nodes_enabled) {
      return svr->cluster_->DumpClusterNodes(svr->GetConfig()->NodesFilePath());
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  std::string nodes_str_;
  std::string dst_node_id_;
  int64_t set_version_ = 0;
  int64_t slot_ = -1;
  int slot_id_ = -1;
  bool force_ = false;
};

class CommandEval : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    return Lua::evalGenericCommand(conn, args_, false, output);
  }
};

class CommandEvalSHA : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_[1].size() != 40) {
      *output = Redis::Error(errNoMatchingScript);
      return Status::OK();
    }
    return Lua::evalGenericCommand(conn, args_, true, output);
  }
};

class CommandEvalRO : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    return Lua::evalGenericCommand(conn, args_, false, output, true);
  }
};

class CommandEvalSHARO : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_[1].size() != 40) {
      *output = Redis::Error(errNoMatchingScript);
      return Status::OK();
    }
    return Lua::evalGenericCommand(conn, args_, true, output, true);
  }
};

class CommandScript : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    // There's a little tricky here since the script command was the write type
    // command but some subcommands like `exists` were readonly, so we want to allow
    // executing on slave here. Maybe we should find other way to do this.
    if (svr->IsSlave() && subcommand_ != "exists") {
      return {Status::NotOK, "READONLY You can't write against a read only slave"};
    }

    if (args_.size() == 2 && subcommand_ == "flush") {
      svr->ScriptFlush();
      auto s = svr->Propagate(Engine::kPropagateScriptCommand, args_);
      if (!s.IsOK()) {
        LOG(ERROR) << "Failed to propagate script command: " << s.Msg();
        return s;
      }
      *output = Redis::SimpleString("OK");
    } else if (args_.size() >= 2 && subcommand_ == "exists") {
      *output = Redis::MultiLen(args_.size() - 2);
      for (size_t j = 2; j < args_.size(); j++) {
        if (svr->ScriptExists(args_[j]).IsOK()) {
          *output += Redis::Integer(1);
        } else {
          *output += Redis::Integer(0);
        }
      }
    } else if (args_.size() == 3 && subcommand_ == "load") {
      std::string sha;
      auto s = Lua::createFunction(svr, args_[2], &sha, svr->Lua());
      if (!s.IsOK()) {
        return s;
      }

      *output = Redis::BulkString(sha);
    } else {
      return {Status::NotOK, "Unknown SCRIPT subcommand or wrong # of args"};
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
};

class CommandXAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    bool entry_id_found = false;
    stream_name_ = args[1];

    for (size_t i = 2; i < args.size();) {
      auto val = entry_id_found ? args[i] : Util::ToLower(args[i]);

      if (val == "nomkstream" && !entry_id_found) {
        nomkstream_ = true;
        ++i;
        continue;
      }

      if (val == "maxlen" && !entry_id_found) {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        size_t max_len_idx = 0;
        bool eq_sign_found = false;
        if (args[i + 1] == "=") {
          max_len_idx = i + 2;
          eq_sign_found = true;
        } else {
          max_len_idx = i + 1;
        }

        if (max_len_idx >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        auto parse_result = ParseInt<uint64_t>(args[max_len_idx], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        max_len_ = *parse_result;
        with_max_len_ = true;

        i += eq_sign_found ? 3 : 2;
        continue;
      }

      if (val == "minid" && !entry_id_found) {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        size_t min_id_idx = 0;
        bool eq_sign_found = false;
        if (args[i + 1] == "=") {
          min_id_idx = i + 2;
          eq_sign_found = true;
        } else {
          min_id_idx = i + 1;
        }

        if (min_id_idx >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        auto s = ParseStreamEntryID(args[min_id_idx], &min_id_);
        if (!s.IsOK()) {
          return {Status::RedisParseErr, s.Msg()};
        }

        with_min_id_ = true;
        i += eq_sign_found ? 3 : 2;
        continue;
      }

      if (val == "limit" && !entry_id_found) {
        return {Status::RedisParseErr, errLimitOptionNotAllowed};
      }

      if (val == "*" && !entry_id_found) {
        entry_id_found = true;
        ++i;
        continue;
      } else if (!entry_id_found) {
        auto s = ParseNewStreamEntryID(val, &entry_id_);
        if (!s.IsOK()) {
          return {Status::RedisParseErr, s.Msg()};
        }

        entry_id_found = true;
        with_entry_id_ = true;
        ++i;
        continue;
      }

      if (entry_id_found) {
        name_value_pairs_.push_back(val);
        ++i;
      }
    }

    if (name_value_pairs_.empty() || name_value_pairs_.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::StreamAddOptions options;
    options.nomkstream = nomkstream_;
    if (with_max_len_) {
      options.trim_options.strategy = StreamTrimStrategy::MaxLen;
      options.trim_options.max_len = max_len_;
    }
    if (with_min_id_) {
      options.trim_options.strategy = StreamTrimStrategy::MinID;
      options.trim_options.min_id = min_id_;
    }
    if (with_entry_id_) {
      options.with_entry_id = true;
      options.entry_id = entry_id_;
    }

    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());
    StreamEntryID entry_id;
    auto s = stream_db.Add(stream_name_, options, name_value_pairs_, &entry_id);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound() && nomkstream_) {
      *output = Redis::NilString();
      return Status::OK();
    }

    *output = Redis::BulkString(entry_id.ToString());

    svr->OnEntryAddedToStream(conn->GetNamespace(), stream_name_, entry_id);

    return Status::OK();
  }

 private:
  std::string stream_name_;
  uint64_t max_len_ = 0;
  Redis::StreamEntryID min_id_;
  Redis::NewStreamEntryID entry_id_;
  std::vector<std::string> name_value_pairs_;
  bool nomkstream_ = false;
  bool with_max_len_ = false;
  bool with_min_id_ = false;
  bool with_entry_id_ = false;
};

class CommandXDel : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); ++i) {
      Redis::StreamEntryID id;
      auto s = ParseStreamEntryID(args[i], &id);
      if (!s.IsOK()) {
        return s;
      }

      ids_.push_back(id);
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());
    uint64_t deleted = 0;
    auto s = stream_db.DeleteEntries(args_[1], ids_, &deleted);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(deleted);

    return Status::OK();
  }

 private:
  std::vector<Redis::StreamEntryID> ids_;
};

class CommandXLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());
    uint64_t len = 0;
    auto s = stream_db.Len(args_[1], &len);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(len);

    return Status::OK();
  }
};

class CommandXInfo : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto val = Util::ToLower(args[1]);
    if (val == "stream" && args.size() >= 2) {
      stream_ = true;

      if (args.size() > 3 && Util::ToLower(args[3]) == "full") {
        full_ = true;
      }

      if (args.size() > 5 && Util::ToLower(args[4]) == "count") {
        auto parse_result = ParseInt<uint64_t>(args[5], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
      }
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (stream_) {
      return getStreamInfo(svr, conn, output);
    }
    return Status::OK();
  }

 private:
  uint64_t count_ = 10;  // default Redis value
  bool stream_ = false;
  bool full_ = false;

  Status getStreamInfo(Server *svr, Connection *conn, std::string *output) {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());
    Redis::StreamInfo info;
    auto s = stream_db.GetStreamInfo(args_[2], full_, count_, &info);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, errNoSuchKey};
    }

    if (!full_) {
      output->append(Redis::MultiLen(14));
    } else {
      output->append(Redis::MultiLen(12));
    }
    output->append(Redis::BulkString("length"));
    output->append(Redis::Integer(info.size));
    output->append(Redis::BulkString("last-generated-id"));
    output->append(Redis::BulkString(info.last_generated_id.ToString()));
    output->append(Redis::BulkString("max-deleted-entry-id"));
    output->append(Redis::BulkString(info.max_deleted_entry_id.ToString()));
    output->append(Redis::BulkString("entries-added"));
    output->append(Redis::Integer(info.entries_added));
    output->append(Redis::BulkString("recorded-first-entry-id"));
    output->append(Redis::BulkString(info.recorded_first_entry_id.ToString()));
    if (!full_) {
      output->append(Redis::BulkString("first-entry"));
      if (info.first_entry) {
        output->append(Redis::MultiLen(2));
        output->append(Redis::BulkString(info.first_entry->key));
        output->append(Redis::MultiBulkString(info.first_entry->values));
      } else {
        output->append(Redis::NilString());
      }
      output->append(Redis::BulkString("last-entry"));
      if (info.last_entry) {
        output->append(Redis::MultiLen(2));
        output->append(Redis::BulkString(info.last_entry->key));
        output->append(Redis::MultiBulkString(info.last_entry->values));
      } else {
        output->append(Redis::NilString());
      }
    } else {
      output->append(Redis::BulkString("entries"));
      output->append(Redis::MultiLen(info.entries.size()));
      for (const auto &e : info.entries) {
        output->append(Redis::MultiLen(2));
        output->append(Redis::BulkString(e.key));
        output->append(Redis::MultiBulkString(e.values));
      }
    }

    return Status::OK();
  }
};

class CommandXRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];

    if (args[2] == "-") {
      start_ = Redis::StreamEntryID::Minimum();
    } else if (args[2][0] == '(') {
      exclude_start_ = true;
      auto s = ParseRangeStart(args[2].substr(1), &start_);
      if (!s.IsOK()) return s;
    } else if (args[2] == "+") {
      start_ = Redis::StreamEntryID::Maximum();
    } else {
      auto s = ParseRangeStart(args[2], &start_);
      if (!s.IsOK()) return s;
    }

    if (args[3] == "+") {
      end_ = Redis::StreamEntryID::Maximum();
    } else if (args[3][0] == '(') {
      exclude_end_ = true;
      auto s = ParseRangeEnd(args[3].substr(1), &end_);
      if (!s.IsOK()) return s;
    } else if (args[3] == "-") {
      end_ = Redis::StreamEntryID::Minimum();
    } else {
      auto s = ParseRangeEnd(args[3], &end_);
      if (!s.IsOK()) return s;
    }

    if (args.size() >= 5 && Util::ToLower(args[4]) == "count") {
      if (args.size() != 6) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      with_count_ = true;

      auto parse_result = ParseInt<uint64_t>(args[5], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (with_count_ && count_ == 0) {
      *output = Redis::NilString();
      return Status::OK();
    }

    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    Redis::StreamRangeOptions options;
    options.reverse = false;
    options.start = start_;
    options.end = end_;
    options.with_count = with_count_;
    options.count = count_;
    options.exclude_start = exclude_start_;
    options.exclude_end = exclude_end_;

    std::vector<StreamEntry> result;
    auto s = stream_db.Range(stream_name_, options, &result);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(Redis::MultiLen(result.size()));

    for (const auto &e : result) {
      output->append(Redis::MultiLen(2));
      output->append(Redis::BulkString(e.key));
      output->append(Redis::MultiBulkString(e.values));
    }

    return Status::OK();
  }

 private:
  std::string stream_name_;
  StreamEntryID start_;
  StreamEntryID end_;
  uint64_t count_ = 0;
  bool exclude_start_ = false;
  bool exclude_end_ = false;
  bool with_count_ = false;
};

class CommandXRevRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];

    if (args[2] == "+") {
      start_ = Redis::StreamEntryID::Maximum();
    } else if (args[2][0] == '(') {
      exclude_start_ = true;
      auto s = ParseRangeEnd(args[2].substr(1), &start_);
      if (!s.IsOK()) return s;
    } else if (args[2] == "-") {
      start_ = Redis::StreamEntryID::Minimum();
    } else {
      auto s = ParseRangeEnd(args[2], &start_);
      if (!s.IsOK()) return s;
    }

    if (args[3] == "-") {
      end_ = Redis::StreamEntryID::Minimum();
    } else if (args[3][0] == '(') {
      exclude_end_ = true;
      auto s = ParseRangeStart(args[3].substr(1), &end_);
      if (!s.IsOK()) return s;
    } else if (args[3] == "+") {
      end_ = Redis::StreamEntryID::Maximum();
    } else {
      auto s = ParseRangeStart(args[3], &end_);
      if (!s.IsOK()) return s;
    }

    if (args.size() >= 5 && Util::ToLower(args[4]) == "count") {
      if (args.size() != 6) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      with_count_ = true;

      auto parse_result = ParseInt<uint64_t>(args[5]);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (with_count_ && count_ == 0) {
      *output = Redis::NilString();
      return Status::OK();
    }

    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    Redis::StreamRangeOptions options;
    options.reverse = true;
    options.start = start_;
    options.end = end_;
    options.with_count = with_count_;
    options.count = count_;
    options.exclude_start = exclude_start_;
    options.exclude_end = exclude_end_;

    std::vector<StreamEntry> result;
    auto s = stream_db.Range(stream_name_, options, &result);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(Redis::MultiLen(result.size()));

    for (const auto &e : result) {
      output->append(Redis::MultiLen(2));
      output->append(Redis::BulkString(e.key));
      output->append(Redis::MultiBulkString(e.values));
    }

    return Status::OK();
  }

 private:
  std::string stream_name_;
  StreamEntryID start_;
  StreamEntryID end_;
  uint64_t count_ = 0;
  bool exclude_start_ = false;
  bool exclude_end_ = false;
  bool with_count_ = false;
};

class CommandXRead : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t streams_word_idx = 0;

    for (size_t i = 1; i < args.size();) {
      auto arg = Util::ToLower(args[i]);

      if (arg == "streams") {
        streams_word_idx = i;
        break;
      }

      if (arg == "count") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        with_count_ = true;

        auto parse_result = ParseInt<uint64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
        i += 2;
        continue;
      }

      if (arg == "block") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        block_ = true;

        auto parse_result = ParseInt<int64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        if (*parse_result < 0) {
          return {Status::RedisParseErr, errTimeoutIsNegative};
        }

        block_timeout_ = *parse_result;
        i += 2;
        continue;
      }

      ++i;
    }

    if (streams_word_idx == 0) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if ((args.size() - streams_word_idx - 1) % 2 != 0) {
      return {Status::RedisParseErr, errUnbalancedStreamList};
    }

    size_t number_of_streams = (args.size() - streams_word_idx - 1) / 2;

    for (size_t i = streams_word_idx + 1; i <= streams_word_idx + number_of_streams; ++i) {
      streams_.push_back(args[i]);
      const auto &id_str = args[i + number_of_streams];
      bool get_latest = id_str == "$";
      latest_marks_.push_back(get_latest);
      StreamEntryID id;
      if (!get_latest) {
        auto s = ParseStreamEntryID(id_str, &id);
        if (!s.IsOK()) {
          return s;
        }
      }
      ids_.push_back(id);
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    std::vector<Redis::StreamReadResult> results;

    for (size_t i = 0; i < streams_.size(); ++i) {
      if (latest_marks_[i]) {
        continue;
      }

      Redis::StreamRangeOptions options;
      options.reverse = false;
      options.start = ids_[i];
      options.end = StreamEntryID{UINT64_MAX, UINT64_MAX};
      options.with_count = with_count_;
      options.count = count_;
      options.exclude_start = true;
      options.exclude_end = false;

      std::vector<StreamEntry> result;
      auto s = stream_db.Range(streams_[i], options, &result);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (result.size() > 0) {
        results.emplace_back(streams_[i], result);
      }
    }

    if (block_ && results.empty()) {
      if (conn->IsInExec()) {
        *output = Redis::MultiLen(-1);
        return Status::OK();  // No blocking in multi-exec
      }

      return blockingRead(svr, conn, &stream_db);
    }

    if (!block_ && results.empty()) {
      *output = Redis::MultiLen(-1);
      return Status::OK();
    }

    return sendResults(output, results);
  }

  Status sendResults(std::string *output, const std::vector<StreamReadResult> &results) {
    output->append(Redis::MultiLen(results.size()));

    for (const auto &result : results) {
      output->append(Redis::MultiLen(2));
      output->append(Redis::BulkString(result.name));
      output->append(Redis::MultiLen(result.entries.size()));
      for (const auto &entry : result.entries) {
        output->append(Redis::MultiLen(2));
        output->append(Redis::BulkString(entry.key));
        output->append(Redis::MultiBulkString(entry.values));
      }
    }

    return Status::OK();
  }

  Status blockingRead(Server *svr, Connection *conn, Redis::Stream *stream_db) {
    if (!with_count_) {
      with_count_ = true;
      count_ = blocked_default_count_;
    }

    for (size_t i = 0; i < streams_.size(); ++i) {
      if (latest_marks_[i]) {
        StreamEntryID last_generated_id;
        auto s = stream_db->GetLastGeneratedID(streams_[i], &last_generated_id);
        if (!s.ok()) {
          return {Status::RedisExecErr, s.ToString()};
        }

        ids_[i] = last_generated_id;
      }
    }

    svr_ = svr;
    conn_ = conn;

    svr_->BlockOnStreams(streams_, ids_, conn_);

    auto bev = conn->GetBufferEvent();
    bufferevent_setcb(bev, nullptr, WriteCB, EventCB, this);

    if (block_timeout_ > 0) {
      timer_ = evtimer_new(bufferevent_get_base(bev), TimerCB, this);
      timeval tm;
      if (block_timeout_ > 1000) {
        tm.tv_sec = block_timeout_ / 1000;
        tm.tv_usec = (block_timeout_ % 1000) * 1000;
      } else {
        tm.tv_sec = 0;
        tm.tv_usec = block_timeout_ * 1000;
      }

      evtimer_add(timer_, &tm);
    }

    return {Status::BlockingCmd};
  }

  static void WriteCB(bufferevent *bev, void *ctx) {
    auto command = reinterpret_cast<CommandXRead *>(ctx);

    if (command->timer_ != nullptr) {
      event_free(command->timer_);
      command->timer_ = nullptr;
    }

    command->unblockAll();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite, Redis::Connection::OnEvent,
                      command->conn_);
    bufferevent_enable(bev, EV_READ);

    Redis::Stream stream_db(command->svr_->storage_, command->conn_->GetNamespace());

    std::vector<StreamReadResult> results;

    for (size_t i = 0; i < command->streams_.size(); ++i) {
      Redis::StreamRangeOptions options;
      options.reverse = false;
      options.start = command->ids_[i];
      options.end = StreamEntryID{UINT64_MAX, UINT64_MAX};
      options.with_count = command->with_count_;
      options.count = command->count_;
      options.exclude_start = true;
      options.exclude_end = false;

      std::vector<StreamEntry> result;
      auto s = stream_db.Range(command->streams_[i], options, &result);
      if (!s.ok()) {
        command->conn_->Reply(Redis::MultiLen(-1));
        LOG(ERROR) << "ERR executing XRANGE for stream " << command->streams_[i] << " from "
                   << command->ids_[i].ToString() << " to " << options.end.ToString() << " with count "
                   << command->count_ << ": " << s.ToString();
      }

      if (result.size() > 0) {
        results.emplace_back(command->streams_[i], result);
      }
    }

    if (results.empty()) {
      command->conn_->Reply(Redis::MultiLen(-1));
    }

    command->sendReply(results);
  }

  void sendReply(const std::vector<StreamReadResult> &results) {
    std::string output;

    output.append(Redis::MultiLen(results.size()));

    for (const auto &result : results) {
      output.append(Redis::MultiLen(2));
      output.append(Redis::BulkString(result.name));
      output.append(Redis::MultiLen(result.entries.size()));
      for (const auto &entry : result.entries) {
        output.append(Redis::MultiLen(2));
        output.append(Redis::BulkString(entry.key));
        output.append(Redis::MultiBulkString(entry.values));
      }
    }

    conn_->Reply(output);
  }

  static void EventCB(bufferevent *bev, int16_t events, void *ctx) {
    auto command = static_cast<CommandXRead *>(ctx);

    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (command->timer_ != nullptr) {
        event_free(command->timer_);
        command->timer_ = nullptr;
      }
      command->unblockAll();
    }
    Redis::Connection::OnEvent(bev, events, command->conn_);
  }

  static void TimerCB(int, int16_t events, void *ctx) {
    auto command = reinterpret_cast<CommandXRead *>(ctx);

    command->conn_->Reply(Redis::NilString());

    event_free(command->timer_);
    command->timer_ = nullptr;

    command->unblockAll();

    auto bev = command->conn_->GetBufferEvent();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite, Redis::Connection::OnEvent,
                      command->conn_);
    bufferevent_enable(bev, EV_READ);
  }

 private:
  std::vector<std::string> streams_;
  std::vector<StreamEntryID> ids_;
  std::vector<bool> latest_marks_;
  Server *svr_ = nullptr;
  Connection *conn_ = nullptr;
  event *timer_ = nullptr;
  uint64_t count_ = 0;
  int64_t block_timeout_ = 0;
  int blocked_default_count_ = 1000;
  bool with_count_ = false;
  bool block_ = false;

  void unblockAll() { svr_->UnblockOnStreams(streams_, conn_); }
};

class CommandXTrim : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    bool eq_sign_found = false;

    auto trim_strategy = Util::ToLower(args[2]);
    if (trim_strategy == "maxlen") {
      strategy_ = StreamTrimStrategy::MaxLen;

      size_t max_len_idx = 0;
      if (args[3] != "=") {
        max_len_idx = 3;
      } else {
        max_len_idx = 4;
        eq_sign_found = true;
      }

      if (max_len_idx >= args.size()) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      auto parse_result = ParseInt<uint64_t>(args[max_len_idx], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      max_len_ = *parse_result;
    } else if (trim_strategy == "minid") {
      strategy_ = StreamTrimStrategy::MinID;

      size_t min_id_idx = 0;
      if (args[3] != "=") {
        min_id_idx = 3;
      } else {
        min_id_idx = 4;
        eq_sign_found = true;
      }

      if (min_id_idx >= args.size()) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      auto s = ParseStreamEntryID(args[min_id_idx], &min_id_);
      if (!s.IsOK()) {
        return s;
      }
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    bool limit_option_found = false;
    if (eq_sign_found) {
      if (args.size() > 6 && Util::ToLower(args[5]) == "limit") {
        limit_option_found = true;
      }
    } else {
      if (args.size() > 5 && Util::ToLower(args[4]) == "limit") {
        limit_option_found = true;
      }
    }

    if (limit_option_found) {
      return {Status::RedisParseErr, errLimitOptionNotAllowed};
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    StreamTrimOptions options;
    options.strategy = strategy_;
    options.max_len = max_len_;
    options.min_id = min_id_;

    uint64_t removed = 0;
    auto s = stream_db.Trim(args_[1], options, &removed);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(removed);

    return Status::OK();
  }

 private:
  uint64_t max_len_ = 0;
  StreamEntryID min_id_;
  StreamTrimStrategy strategy_ = StreamTrimStrategy::None;
};

class CommandXSetId : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];

    auto s = Redis::ParseStreamEntryID(args[2], &last_id_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }

    if (args.size() == 3) {
      return Status::OK();
    }

    for (size_t i = 3; i < args.size(); /* manual increment */) {
      if (Util::EqualICase(args[i], "entriesadded") && i + 1 < args.size()) {
        auto parse_result = ParseInt<uint64_t>(args[i + 1]);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        entries_added_ = *parse_result;
        i += 2;
      } else if (Util::EqualICase(args[i], "maxdeletedid") && i + 1 < args.size()) {
        StreamEntryID id;
        s = Redis::ParseStreamEntryID(args[i + 1], &id);
        if (!s.IsOK()) {
          return {Status::RedisParseErr, s.Msg()};
        }

        max_deleted_id_ = std::make_optional<StreamEntryID>(id.ms, id.seq);
        i += 2;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    auto s = stream_db.SetId(stream_name_, last_id_, entries_added_, max_deleted_id_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::SimpleString("OK");

    return Status::OK();
  }

 private:
  std::string stream_name_;
  StreamEntryID last_id_;
  std::optional<StreamEntryID> max_deleted_id_;
  std::optional<uint64_t> entries_added_;
};

REDIS_REGISTER_COMMANDS(
    MakeCmdAttr<CommandAuth>("auth", 2, "read-only ok-loading", 0, 0, 0),
    MakeCmdAttr<CommandPing>("ping", -1, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandSelect>("select", 2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandInfo>("info", -1, "read-only ok-loading", 0, 0, 0),
    MakeCmdAttr<CommandRole>("role", 1, "read-only ok-loading", 0, 0, 0),
    MakeCmdAttr<CommandConfig>("config", -2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandNamespace>("namespace", -3, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandKeys>("keys", 2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandFlushDB>("flushdb", 1, "write", 0, 0, 0),
    MakeCmdAttr<CommandFlushAll>("flushall", 1, "write", 0, 0, 0),
    MakeCmdAttr<CommandDBSize>("dbsize", -1, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandSlowlog>("slowlog", -2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandPerfLog>("perflog", -2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandClient>("client", -2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandMonitor>("monitor", 1, "read-only no-multi", 0, 0, 0),
    MakeCmdAttr<CommandShutdown>("shutdown", 1, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandQuit>("quit", 1, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandScan>("scan", -2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandRandomKey>("randomkey", 1, "read-only no-script", 0, 0, 0),
    MakeCmdAttr<CommandDebug>("debug", -2, "read-only exclusive", 0, 0, 0),
    MakeCmdAttr<CommandCommand>("command", -1, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandEcho>("echo", 2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandDisk>("disk", 3, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandHello>("hello", -1, "read-only ok-loading", 0, 0, 0),

    MakeCmdAttr<CommandTTL>("ttl", 2, "read-only", 1, 1, 1), MakeCmdAttr<CommandPTTL>("pttl", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandType>("type", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandObject>("object", 3, "read-only", 2, 2, 1),
    MakeCmdAttr<CommandExists>("exists", -2, "read-only", 1, -1, 1),
    MakeCmdAttr<CommandPersist>("persist", 2, "write", 1, 1, 1),
    MakeCmdAttr<CommandExpire>("expire", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandPExpire>("pexpire", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandExpireAt>("expireat", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandPExpireAt>("pexpireat", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandDel>("del", -2, "write", 1, -1, 1), MakeCmdAttr<CommandDel>("unlink", -2, "write", 1, -1, 1),

    MakeCmdAttr<CommandGet>("get", 2, "read-only", 1, 1, 1), MakeCmdAttr<CommandGetEx>("getex", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandStrlen>("strlen", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGetSet>("getset", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandGetRange>("getrange", 4, "read-only", 1, 1, 1),
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
    MakeCmdAttr<CommandCAD>("cad", 3, "write", 1, 1, 1),

    MakeCmdAttr<CommandGetBit>("getbit", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandSetBit>("setbit", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandBitCount>("bitcount", -2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandBitPos>("bitpos", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandBitOp>("bitop", -4, "write", 2, -1, 1),

    MakeCmdAttr<CommandHGet>("hget", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHIncrBy>("hincrby", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandHIncrByFloat>("hincrbyfloat", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandHMSet>("hset", -4, "write", 1, 1, 1), MakeCmdAttr<CommandHSetNX>("hsetnx", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandHDel>("hdel", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandHStrlen>("hstrlen", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHExists>("hexists", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHLen>("hlen", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHMGet>("hmget", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHMSet>("hmset", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandHKeys>("hkeys", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHVals>("hvals", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHGetAll>("hgetall", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHScan>("hscan", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHRangeByLex>("hrangebylex", -4, "read-only", 1, 1, 1),

    MakeCmdAttr<CommandLPush>("lpush", -3, "write", 1, 1, 1), MakeCmdAttr<CommandRPush>("rpush", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandLPushX>("lpushx", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandRPushX>("rpushx", -3, "write", 1, 1, 1), MakeCmdAttr<CommandLPop>("lpop", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandRPop>("rpop", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandBLPop>("blpop", -3, "write no-script", 1, -2, 1),
    MakeCmdAttr<CommandBRPop>("brpop", -3, "write no-script", 1, -2, 1),
    MakeCmdAttr<CommandLRem>("lrem", 4, "write", 1, 1, 1), MakeCmdAttr<CommandLInsert>("linsert", 5, "write", 1, 1, 1),
    MakeCmdAttr<CommandLRange>("lrange", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandLIndex>("lindex", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandLTrim>("ltrim", 4, "write", 1, 1, 1), MakeCmdAttr<CommandLLen>("llen", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandLSet>("lset", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandRPopLPUSH>("rpoplpush", 3, "write", 1, 2, 1),
    MakeCmdAttr<CommandLMove>("lmove", 5, "write", 1, 2, 1),

    MakeCmdAttr<CommandSAdd>("sadd", -3, "write", 1, 1, 1), MakeCmdAttr<CommandSRem>("srem", -3, "write", 1, 1, 1),
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
    MakeCmdAttr<CommandSScan>("sscan", -3, "read-only", 1, 1, 1),

    MakeCmdAttr<CommandZAdd>("zadd", -4, "write", 1, 1, 1), MakeCmdAttr<CommandZCard>("zcard", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZCount>("zcount", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZIncrBy>("zincrby", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandZInterStore>("zinterstore", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandZLexCount>("zlexcount", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZPopMax>("zpopmax", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandZPopMin>("zpopmin", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandZRange>("zrange", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZRevRange>("zrevrange", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZRangeByLex>("zrangebylex", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZRevRangeByLex>("zrevrangebylex", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZRangeByScore>("zrangebyscore", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZRank>("zrank", 3, "read-only", 1, 1, 1), MakeCmdAttr<CommandZRem>("zrem", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandZRemRangeByRank>("zremrangebyrank", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandZRemRangeByScore>("zremrangebyscore", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandZRemRangeByLex>("zremrangebylex", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandZRevRangeByScore>("zrevrangebyscore", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZRevRank>("zrevrank", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZScore>("zscore", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZMScore>("zmscore", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZScan>("zscan", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandZUnionStore>("zunionstore", -4, "write", 1, 1, 1),

    MakeCmdAttr<CommandGeoAdd>("geoadd", -5, "write", 1, 1, 1),
    MakeCmdAttr<CommandGeoDist>("geodist", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGeoHash>("geohash", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGeoPos>("geopos", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGeoRadius>("georadius", -6, "write", 1, 1, 1),
    MakeCmdAttr<CommandGeoRadiusByMember>("georadiusbymember", -5, "write", 1, 1, 1),
    MakeCmdAttr<CommandGeoRadiusReadonly>("georadius_ro", -6, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGeoRadiusByMemberReadonly>("georadiusbymember_ro", -5, "read-only", 1, 1, 1),

    MakeCmdAttr<CommandPublish>("publish", 3, "read-only pub-sub", 0, 0, 0),
    MakeCmdAttr<CommandSubscribe>("subscribe", -2, "read-only pub-sub no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandUnSubscribe>("unsubscribe", -1, "read-only pub-sub no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandPSubscribe>("psubscribe", -2, "read-only pub-sub no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandPUnSubscribe>("punsubscribe", -1, "read-only pub-sub no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandPubSub>("pubsub", -2, "read-only pub-sub no-script", 0, 0, 0),

    MakeCmdAttr<CommandMulti>("multi", 1, "multi", 0, 0, 0),
    MakeCmdAttr<CommandDiscard>("discard", 1, "multi", 0, 0, 0),
    MakeCmdAttr<CommandExec>("exec", 1, "exclusive multi", 0, 0, 0),

    MakeCmdAttr<CommandSortedintAdd>("siadd", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandSortedintRem>("sirem", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandSortedintCard>("sicard", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandSortedintExists>("siexists", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandSortedintRange>("sirange", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandSortedintRevRange>("sirevrange", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandSortedintRangeByValue>("sirangebyvalue", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandSortedintRevRangeByValue>("sirevrangebyvalue", -4, "read-only", 1, 1, 1),

    MakeCmdAttr<CommandCluster>("cluster", -2, "cluster no-script", 0, 0, 0),
    MakeCmdAttr<CommandClusterX>("clusterx", -2, "cluster no-script", 0, 0, 0),

    MakeCmdAttr<CommandEval>("eval", -3, "exclusive write no-script", 0, 0, 0),
    MakeCmdAttr<CommandEvalSHA>("evalsha", -3, "exclusive write no-script", 0, 0, 0),
    MakeCmdAttr<CommandEvalRO>("eval_ro", -3, "read-only no-script", 0, 0, 0),
    MakeCmdAttr<CommandEvalSHARO>("evalsha_ro", -3, "read-only no-script", 0, 0, 0),
    MakeCmdAttr<CommandScript>("script", -2, "exclusive no-script", 0, 0, 0),

    MakeCmdAttr<CommandCompact>("compact", 1, "read-only no-script", 0, 0, 0),
    MakeCmdAttr<CommandBGSave>("bgsave", 1, "read-only no-script", 0, 0, 0),
    MakeCmdAttr<CommandFlushBackup>("flushbackup", 1, "read-only no-script", 0, 0, 0),
    MakeCmdAttr<CommandSlaveOf>("slaveof", 3, "read-only exclusive no-script", 0, 0, 0),
    MakeCmdAttr<CommandStats>("stats", 1, "read-only", 0, 0, 0),

    MakeCmdAttr<CommandReplConf>("replconf", -3, "read-only replication no-script", 0, 0, 0),
    MakeCmdAttr<CommandPSync>("psync", -2, "read-only replication no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandFetchMeta>("_fetch_meta", 1, "read-only replication no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandFetchFile>("_fetch_file", 2, "read-only replication no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandDBName>("_db_name", 1, "read-only replication no-multi", 0, 0, 0),

    MakeCmdAttr<CommandXAdd>("xadd", -5, "write", 1, 1, 1), MakeCmdAttr<CommandXDel>("xdel", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandXLen>("xlen", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandXInfo>("xinfo", -2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandXRange>("xrange", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandXRevRange>("xrevrange", -2, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandXRead>("xread", -4, "read-only", 0, 0, 0),
    MakeCmdAttr<CommandXTrim>("xtrim", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandXSetId>("xsetid", -3, "write", 1, 1, 1))

RegisterToCommandTable::RegisterToCommandTable(std::initializer_list<CommandAttributes> list) {
  for (const auto &attr : list) {
    command_details::redis_command_table.emplace_back(attr);
    command_details::original_commands[attr.name] = &command_details::redis_command_table.back();
    command_details::commands[attr.name] = &command_details::redis_command_table.back();
  }
}

int GetCommandNum() { return (int)command_details::redis_command_table.size(); }

const CommandMap *GetOriginalCommands() { return &command_details::original_commands; }

CommandMap *GetCommands() { return &command_details::commands; }

void ResetCommands() { command_details::commands = command_details::original_commands; }

std::string GetCommandInfo(const CommandAttributes *command_attributes) {
  std::string command, command_flags;
  command.append(Redis::MultiLen(6));
  command.append(Redis::BulkString(command_attributes->name));
  command.append(Redis::Integer(command_attributes->arity));
  command_flags.append(Redis::MultiLen(1));
  command_flags.append(Redis::BulkString(command_attributes->is_write() ? "write" : "readonly"));
  command.append(command_flags);
  command.append(Redis::Integer(command_attributes->first_key));
  command.append(Redis::Integer(command_attributes->last_key));
  command.append(Redis::Integer(command_attributes->key_step));
  return command;
}

void GetAllCommandsInfo(std::string *info) {
  info->append(Redis::MultiLen(command_details::original_commands.size()));
  for (const auto &iter : command_details::original_commands) {
    auto command_attribute = iter.second;
    auto command_info = GetCommandInfo(command_attribute);
    info->append(command_info);
  }
}

void GetCommandsInfo(std::string *info, const std::vector<std::string> &cmd_names) {
  info->append(Redis::MultiLen(cmd_names.size()));
  for (const auto &cmd_name : cmd_names) {
    auto cmd_iter = command_details::original_commands.find(Util::ToLower(cmd_name));
    if (cmd_iter == command_details::original_commands.end()) {
      info->append(Redis::NilString());
    } else {
      auto command_attribute = cmd_iter->second;
      auto command_info = GetCommandInfo(command_attribute);
      info->append(command_info);
    }
  }
}

Status GetKeysFromCommand(const std::string &cmd_name, int argc, std::vector<int> *keys_indexes) {
  auto cmd_iter = command_details::original_commands.find(Util::ToLower(cmd_name));
  if (cmd_iter == command_details::original_commands.end()) {
    return {Status::RedisUnknownCmd, "Invalid command specified"};
  }

  auto command_attribute = cmd_iter->second;
  if (command_attribute->first_key == 0) {
    return {Status::NotOK, "The command has no key arguments"};
  }

  if ((command_attribute->arity > 0 && command_attribute->arity != argc) || argc < -command_attribute->arity) {
    return {Status::NotOK, "Invalid number of arguments specified for command"};
  }

  auto last = command_attribute->last_key;
  if (last < 0) last = argc + last;

  for (int j = command_attribute->first_key; j <= last; j += command_attribute->key_step) {
    keys_indexes->emplace_back(j);
  }

  return Status::OK();
}

bool IsCommandExists(const std::string &name) {
  return command_details::original_commands.find(Util::ToLower(name)) != command_details::original_commands.end();
}

}  // namespace Redis
