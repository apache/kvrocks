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
#include "command_parser.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_topk.h"

namespace {
constexpr const char *errBadK = "Bad K";
constexpr const char *errBadWidth = "Bad width";
constexpr const char *errBadDepth = "Bad depth";
constexpr const char *errBadDecay = "Bad decay";
constexpr const char *errInvalidDecay = "Decay must be between 0 and 1";
}

namespace redis {

class CommandTopKReserve final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    auto parse_k = ParseInt<uint32_t>(args[2], 10);
    if (!parse_k) {
      return {Status::RedisParseErr, errBadK};
    }
    k_ = *parse_k;
    if (args_.size() >= 4) {
      auto parse_width = ParseInt<uint32_t>(args[3], 10);
      if (!parse_width) {
        return {Status::RedisParseErr, errBadWidth};
      }
      width_ = *parse_width;
    }
    if (args_.size() >= 5) {
      auto parse_depth = ParseInt<uint32_t>(args[4], 10);
      if (!parse_depth) {
        return {Status::RedisParseErr, errBadDepth};
      }
      depth_ = *parse_depth;
    }
    if (args_.size() >= 6) {
      auto parse_decay = ParseFloat<double>(args[5]);
      if (!parse_decay) {
        return {Status::RedisParseErr, errBadDecay};
      }
      decay_ = *parse_decay;
      if (decay_ <= 0.0 || decay_ >= 1.0) {
        return {Status::RedisParseErr, errInvalidDecay};
      }
    }
    if (args_.size() > 6) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::TopK topk(srv->storage, conn->GetNamespace());

    auto s = topk.Reserve(ctx, args_[1], k_, width_, depth_, decay_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::RESP_OK;
    return Status::OK();
  }
 private:
  uint32_t k_;
  uint32_t width_ = 7;
  uint32_t depth_ = 8;
  double decay_ = 0.9;
};

class CommandTopKAdd final : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::TopK topk(srv->storage, conn->GetNamespace());
    CHECK(args_.size() == 3);

    auto s = topk.Add(ctx, args_[1], args_[2]);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandTopKIncrBy final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args_.size() != 4) {
      return {Status::InvalidArgument, "invalid argument"};
    }
    auto parse_incr = ParseInt<uint32_t>(args[3], 10);
    if (!parse_incr) {
      return {Status::InvalidArgument, "invalid argument"};
    }
    incr_ = *parse_incr;
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::TopK topk(srv->storage, conn->GetNamespace());
    CHECK(args_.size() == 4);

    auto s = topk.IncrBy(ctx, args_[1], args_[2], incr_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::RESP_OK;
    return Status::OK();
  }
 private:
  uint32_t incr_;
};

class CommandTopKList final : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::TopK topk(srv->storage, conn->GetNamespace());
    CHECK(args_.size() == 2);

    std::vector<std::string> items;
    auto s = topk.List(ctx, args_[1], items);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = MultiBulkString(redis::RESP::v2, items);
    return Status::OK();
  }
};

class CommandTopKInfo final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 3) {
      return {Status::InvalidArgument, errWrongNumOfArguments};
    }

    CommandParser parser(args, 2);
    if (parser.Good()) {
      if (args.size() == 3) {
        std::string type_str = args[2];
        if (type_str == "topk") {
          type_ = TopKInfoType::kTopK;
        } else if (type_str == "width") {
          type_ = TopKInfoType::kWidth;
        } else if (type_str == "depth") {
          type_ = TopKInfoType::kDepth;
        } else if (type_str == "decay") {
          type_ = TopKInfoType::kDecay;
        } else {
          return {Status::InvalidArgument, "Invalid info type"};
        }
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, [[maybe_unused]]std::string *output) override {
    redis::TopK topk_db(srv->storage, conn->GetNamespace());
    TopKInfo info;

    auto s = topk_db.Info(ctx, args_[1], &info);
    if (s.IsNotFound()) return {Status::RedisExecErr, "key does not exist"};
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    switch (type_) {
      case TopKInfoType::kAll:
        *output = redis::MultiLen(2 * 4);
        *output += redis::SimpleString("K");
        *output += redis::Integer(info.k);
        *output += redis::SimpleString("Width");
        *output += redis::Integer(info.width);
        *output += redis::SimpleString("Depth");
        *output += redis::Integer(info.depth);
        *output += redis::SimpleString("Decay");
        *output += redis::Double(redis::RESP::v2, info.decay);
        break;
      case TopKInfoType::kTopK:
        *output = redis::Integer(info.k);
        break;
      case TopKInfoType::kWidth:
        *output = redis::Integer(info.width);
        break;
      case TopKInfoType::kDepth:
        *output = redis::Integer(info.depth);
        break;
      case TopKInfoType::kDecay:
        *output = redis::Double(redis::RESP::v2, info.decay);
        break;
    }
    return Status::OK();
  }
 private:
  TopKInfoType type_ = TopKInfoType::kAll;
};

class CommandTopKQuery final : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::TopK topk(srv->storage, conn->GetNamespace());
    CHECK(args_.size() == 3);

    bool is_exists_;
    auto s = topk.Query(ctx, args_[1], args_[2], &is_exists_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    } 
    *output = redis::Bool(redis::RESP::v2, is_exists_);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(TopK, MakeCmdAttr<CommandTopKAdd>("topk.add", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTopKList>("topk.list", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTopKInfo>("topk.info", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTopKQuery>("topk.query", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTopKReserve>("topk.reserve", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTopKIncrBy>("topk.incrby", 4, "write", 1, 1, 1));

} // namespace redis