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

#include <algorithm>

#include "commander.h"
#include "commands/command_parser.h"
#include "commands/error_constants.h"
#include "error_constants.h"
#include "parse_util.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "storage/redis_metadata.h"
#include "types/redis_json.h"

namespace redis {

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string OptionalsToString(Optionals<T> &opts) {
  std::string str = MultiLen(opts.size());
  for (const auto &opt : opts) {
    if (opt.has_value()) {
      str += redis::Integer(opt.value());
    } else {
      str += redis::NilString();
    }
  }
  return str;
}

class CommandJsonSet : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Json json(srv->storage, conn->GetNamespace());

    auto s = json.Set(args_[1], args_[2], args_[3]);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandJsonGet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);

    while (parser.Good()) {
      if (parser.EatEqICase("INDENT")) {
        auto indent = GET_OR_RET(parser.TakeStr());

        if (std::any_of(indent.begin(), indent.end(), [](char v) { return v != ' '; })) {
          return {Status::RedisParseErr, "Currently only all-space INDENT is supported"};
        }

        indent_size_ = indent.size();
      } else if (parser.EatEqICase("NEWLINE")) {
        new_line_chars_ = GET_OR_RET(parser.TakeStr());
      } else if (parser.EatEqICase("SPACE")) {
        auto space = GET_OR_RET(parser.TakeStr());

        if (space != "" && space != " ") {
          return {Status::RedisParseErr, "Currently only SPACE ' ' is supported"};
        }

        spaces_after_colon_ = !space.empty();
      } else {
        break;
      }
    }

    while (parser.Good()) {
      paths_.push_back(GET_OR_RET(parser.TakeStr()));
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Json json(srv->storage, conn->GetNamespace());

    JsonValue result;
    auto s = json.Get(args_[1], paths_, &result);
    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::BulkString(GET_OR_RET(result.Print(indent_size_, spaces_after_colon_, new_line_chars_)));
    return Status::OK();
  }

 private:
  uint8_t indent_size_ = 0;
  bool spaces_after_colon_ = false;
  std::string new_line_chars_;

  std::vector<std::string> paths_;
};

class CommandJsonInfo : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Json json(srv->storage, conn->GetNamespace());

    auto storage_format = JsonStorageFormat::JSON;
    auto s = json.Info(args_[1], &storage_format);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    auto format_str = storage_format == JsonStorageFormat::JSON   ? "json"
                      : storage_format == JsonStorageFormat::CBOR ? "cbor"
                                                                  : "unknown";
    output->append(redis::MultiBulkString({"storage_format", format_str}));
    return Status::OK();
  }
};

class CommandJsonArrAppend : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Json json(srv->storage, conn->GetNamespace());

    Optionals<size_t> results;

    auto s = json.ArrAppend(args_[1], args_[2], {args_.begin() + 3, args_.end()}, &results);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = OptionalsToString(results);
    return Status::OK();
  }
};

class CommandJsonArrInsert : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[3], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    index_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Json json(srv->storage, conn->GetNamespace());

    Optionals<uint64_t> results;
    auto parse_result = ParseInt<int>(args_[3], 10);

    auto s = json.ArrInsert(args_[1], args_[2], index_, {args_.begin() + 4, args_.end()}, &results);
    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = OptionalsToString(results);
    return Status::OK();
  }

 private:
  int index_;
};

class CommandJsonType : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Json json(srv->storage, conn->GetNamespace());

    std::vector<std::string> types;

    std::string path = "$";
    if (args_.size() == 3) {
      path = args_[2];
    } else if (args_.size() > 3) {
      return {Status::RedisExecErr, "The number of arguments is more than expected"};
    }
    auto s = json.Type(args_[1], path, &types);
    if (!s.ok() && !s.IsNotFound()) return {Status::RedisExecErr, s.ToString()};
    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }

    *output = redis::MultiBulkString(types);
    return Status::OK();
  }
};

class CommandJsonObjkeys : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Json json(srv->storage, conn->GetNamespace());

    Optionals<std::vector<std::string>> results;

    // If path not specified set it to $
    std::string path = (args_.size() > 2) ? args_[2] : "$";
    auto s = json.ObjKeys(args_[1], path, &results);
    if (!s.ok() && !s.IsNotFound()) return {Status::RedisExecErr, s.ToString()};
    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }

    *output = redis::MultiLen(results.size());
    for (const auto &item : results) {
      if (item.has_value()) {
        *output += redis::MultiBulkString(item.value(), false);
      } else {
        *output += redis::NilString();
      }
    }

    return Status::OK();
  }
};

class CommandJsonClear : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    size_t result = 0;

    // If path not specified set it to $
    std::string path = (args_.size() > 2) ? args_[2] : "$";
    auto s = json.Clear(args_[1], path, &result);

    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }

    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(result);
    return Status::OK();
  }
};

class CommandJsonToggle : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    std::string path = (args_.size() > 2) ? args_[2] : "$";
    Optionals<bool> results;
    auto s = json.Toggle(args_[1], path, &results);
    if (!s.ok() && !s.IsNotFound()) return {Status::RedisExecErr, s.ToString()};
    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }

    *output = OptionalsToString(results);
    return Status::OK();
  }
};

class CommandJsonArrLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    std::string path = "$";
    if (args_.size() == 3) {
      path = args_[2];
    } else if (args_.size() > 3) {
      return {Status::RedisExecErr, "The number of arguments is more than expected"};
    }

    Optionals<uint64_t> results;
    auto s = json.ArrLen(args_[1], path, &results);
    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = OptionalsToString(results);
    return Status::OK();
  }
};

class CommandJsonMerge : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    std::string key = args_[1];
    std::string path = args_[2];
    std::string value = args_[3];
    bool result = false;

    auto s = json.Merge(key, path, value, result);

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!result) {
      *output = redis::NilString();
    } else {
      *output = redis::SimpleString("OK");
    }

    return Status::OK();
  }
};

class CommandJsonArrPop : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    path_ = (args_.size() > 2) ? args_[2] : "$";

    if (args_.size() == 4) {
      index_ = GET_OR_RET(ParseInt<int64_t>(args_[3], 10));
    } else if (args_.size() > 4) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Json json(srv->storage, conn->GetNamespace());

    Optionals<JsonValue> results;

    auto s = json.ArrPop(args_[1], path_, index_, &results);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(results.size());
    for (const auto &data : results) {
      if (data.has_value()) {
        *output += redis::BulkString(GET_OR_RET(data->Print()));
      } else {
        *output += redis::NilString();
      }
    }

    return Status::OK();
  }

 private:
  std::string path_;
  int64_t index_ = -1;
};

class CommandJsonObjLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    std::string path = "$";
    if (args_.size() == 3) {
      path = args_[2];
    } else if (args_.size() > 3) {
      return {Status::RedisExecErr, "The number of arguments is more than expected"};
    }

    Optionals<uint64_t> results;
    auto s = json.ObjLen(args_[1], path, &results);
    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = OptionalsToString(results);
    return Status::OK();
  }
};

class CommandJsonArrTrim : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    path_ = args_[2];
    start_ = GET_OR_RET(ParseInt<int64_t>(args_[3], 10));
    stop_ = GET_OR_RET(ParseInt<int64_t>(args_[4], 10));

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Json json(srv->storage, conn->GetNamespace());

    Optionals<uint64_t> results;

    auto s = json.ArrTrim(args_[1], path_, start_, stop_, &results);

    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = OptionalsToString(results);
    return Status::OK();
  }

 private:
  std::string path_;
  int64_t start_ = 0;
  int64_t stop_ = 0;
};

class CommanderJsonArrIndex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 6) {
      return {Status::RedisExecErr, errWrongNumOfArguments};
    }
    start_ = 0;
    end_ = std::numeric_limits<ssize_t>::max();

    if (args.size() > 4) {
      start_ = GET_OR_RET(ParseInt<ssize_t>(args[4], 10));
    }
    if (args.size() > 5) {
      end_ = GET_OR_RET(ParseInt<ssize_t>(args[5], 10));
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    Optionals<ssize_t> results;

    auto s = json.ArrIndex(args_[1], args_[2], args_[3], start_, end_, &results);

    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }

    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = OptionalsToString(results);
    return Status::OK();
  }

 private:
  ssize_t start_;
  ssize_t end_;
};

class CommandJsonDel : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());
    size_t result = 0;
    std::string path = "$";
    if (args_.size() == 3) {
      path = args_[2];
    } else if (args_.size() > 3) {
      return {Status::RedisExecErr, "The number of arguments is more than expected"};
    }
    auto s = json.Del(args_[1], path, &result);
    if (s.IsNotFound()) {
      *output = redis::NilString();
      return Status::OK();
    }
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    *output = redis::Integer(result);
    return Status::OK();
  }
};

class CommandJsonNumIncrBy : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    JsonValue result = JsonValue::FromString("[]").GetValue();
    auto s = json.NumIncrBy(args_[1], args_[2], args_[3], &result);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::BulkString(result.value.to_string());

    return Status::OK();
  }
};

class CommandJsonNumMultBy : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    JsonValue result = JsonValue::FromString("[]").GetValue();
    auto s = json.NumMultBy(args_[1], args_[2], args_[3], &result);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::BulkString(result.value.to_string());

    return Status::OK();
  }
};

class CommandJsonStrAppend : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    // path default, if not provided
    std::string path = "$";
    if (args_.size() == 4) {
      path = args_[2];
    } else if (args_.size() > 4) {
      return {Status::RedisExecErr, "The number of arguments is more than expected"};
    }

    Optionals<uint64_t> results;
    auto s = json.StrAppend(args_[1], path, args_[3], &results);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = OptionalsToString(results);
    return Status::OK();
  }
};

class CommandJsonStrLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    std::string path = "$";
    if (args_.size() == 3) {
      path = args_[2];
    } else if (args_.size() > 3) {
      return {Status::RedisExecErr, "The number of arguments is more than expected"};
    }

    Optionals<uint64_t> results;
    auto s = json.StrLen(args_[1], path, &results);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = OptionalsToString(results);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandJsonSet>("json.set", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonGet>("json.get", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandJsonInfo>("json.info", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandJsonType>("json.type", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandJsonArrAppend>("json.arrappend", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonArrInsert>("json.arrinsert", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonArrTrim>("json.arrtrim", 5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonClear>("json.clear", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonToggle>("json.toggle", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonArrLen>("json.arrlen", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandJsonMerge>("json.merge", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonObjkeys>("json.objkeys", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandJsonArrPop>("json.arrpop", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommanderJsonArrIndex>("json.arrindex", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandJsonDel>("json.del", -2, "write", 1, 1, 1),
                        // JSON.FORGET is an alias for JSON.DEL, refer: https://redis.io/commands/json.forget/
                        MakeCmdAttr<CommandJsonDel>("json.forget", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonNumIncrBy>("json.numincrby", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonNumMultBy>("json.nummultby", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonObjLen>("json.objlen", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandJsonStrAppend>("json.strappend", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonStrLen>("json.strlen", -2, "read-only", 1, 1, 1), );

}  // namespace redis
