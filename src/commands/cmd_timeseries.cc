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

#include "command_parser.h"
#include "commander.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_timeseries.h"

namespace {
constexpr const char *errBadRetention = "Couldn't parse RETENTION";
constexpr const char *errBadChunkSize = "invalid CHUNK_SIZE";
constexpr const char *errBadEncoding = "unknown ENCODING parameter";
constexpr const char *errDuplicatePolicy = "Unknown DUPLICATE_POLICY";
constexpr const char *errInvalidTimestamp = "invalid timestamp";
constexpr const char *errInvalidValue = "invalid value";
constexpr const char *errOldTimestamp = "Timestamp is older than retention";
constexpr const char *errDupBlock =
    "Error at upsert, update is not supported when DUPLICATE_POLICY is set to BLOCK mode";

}  // namespace

namespace redis {

class KeywordCommandBase : public Commander {
 public:
  KeywordCommandBase(size_t skip_num, size_t tail_skip_num) : skip_num_(skip_num), tail_skip_num_(tail_skip_num) {}
  virtual ~KeywordCommandBase() = default;

  Status Parse(const std::vector<std::string> &args) override {
    TSOptionsParser parser(args.begin() + skip_num_, args.end() - tail_skip_num_);

    while (parser.Good()) {
      bool handled = false;
      for (const auto &handler : handlers_) {
        if (parser.EatEqICase(handler.first)) {
          Status s = handler.second(parser);
          if (!s.IsOK()) return s;
          handled = true;
          break;
        }
      }

      if (!handled) {
        parser.Skip(1);
      }
    }

    return Commander::Parse(args);
  }

 protected:
  using TSOptionsParser = CommandParser<CommandTokens::const_iterator>;

  template <typename Handler>
  void RegisterHandler(const std::string &keyword, Handler &&handler) {
    handlers_.emplace_back(keyword, std::forward<Handler>(handler));
  }

 private:
  size_t skip_num_ = 0;
  size_t tail_skip_num_ = 0;

  std::vector<std::pair<std::string, std::function<Status(TSOptionsParser &)>>> handlers_;
};

class CommandTSCreateBase : public KeywordCommandBase {
 public:
  CommandTSCreateBase(size_t skip_num, size_t tail_skip_num) : KeywordCommandBase(skip_num, tail_skip_num) {
    RegisterHandler("RETENTION", [this](TSOptionsParser &parser) { return HandleRetention(parser); });
    RegisterHandler("CHUNK_SIZE", [this](TSOptionsParser &parser) { return HandleChunkSize(parser); });
    RegisterHandler("ENCODING", [this](TSOptionsParser &parser) { return HandleEncoding(parser); });
    RegisterHandler("DUPLICATE_POLICY", [this](TSOptionsParser &parser) { return HandleDuplicatePolicy(parser); });
    RegisterHandler("LABELS", [this](TSOptionsParser &parser) { return HandleLabels(parser); });
  }

 protected:
  using DuplicatePolicy = TimeSeriesMetadata::DuplicatePolicy;

  const TSCreateOption &getCreateOption() const { return create_option_; }

 private:
  TSCreateOption create_option_;

  Status HandleRetention(TSOptionsParser &parser) {
    auto parse_retention = parser.TakeInt<uint64_t>();
    if (!parse_retention.IsOK()) {
      return {Status::RedisParseErr, errBadRetention};
    }
    create_option_.retention_time = parse_retention.GetValue();
    return Status::OK();
  }

  Status HandleChunkSize(TSOptionsParser &parser) {
    auto parse_chunk_size = parser.TakeInt<uint64_t>();
    if (!parse_chunk_size.IsOK()) {
      return {Status::RedisParseErr, errBadChunkSize};
    }
    create_option_.chunk_size = parse_chunk_size.GetValue();
    return Status::OK();
  }

  Status HandleEncoding(TSOptionsParser &parser) {
    using ChunkType = TimeSeriesMetadata::ChunkType;
    if (parser.EatEqICase("UNCOMPRESSED")) {
      create_option_.chunk_type = ChunkType::UNCOMPRESSED;
    } else if (parser.EatEqICase("COMPRESSED")) {
      create_option_.chunk_type = ChunkType::COMPRESSED;
    } else {
      return {Status::RedisParseErr, errBadEncoding};
    }
    return Status::OK();
  }

  Status HandleDuplicatePolicy(TSOptionsParser &parser) {
    if (parser.EatEqICase("BLOCK")) {
      create_option_.duplicate_policy = DuplicatePolicy::BLOCK;
    } else if (parser.EatEqICase("FIRST")) {
      create_option_.duplicate_policy = DuplicatePolicy::FIRST;
    } else if (parser.EatEqICase("LAST")) {
      create_option_.duplicate_policy = DuplicatePolicy::LAST;
    } else if (parser.EatEqICase("MAX")) {
      create_option_.duplicate_policy = DuplicatePolicy::MAX;
    } else if (parser.EatEqICase("MIN")) {
      create_option_.duplicate_policy = DuplicatePolicy::MIN;
    } else if (parser.EatEqICase("SUM")) {
      create_option_.duplicate_policy = DuplicatePolicy::SUM;
    } else {
      return {Status::RedisParseErr, errDuplicatePolicy};
    }
    return Status::OK();
  }

  Status HandleLabels(TSOptionsParser &parser) {
    while (parser.Good()) {
      auto parse_key = parser.TakeStr();
      auto parse_value = parser.TakeStr();
      if (!parse_key.IsOK() || !parse_value.IsOK()) {
        break;
      }
      create_option_.labels.push_back({parse_key.GetValue(), parse_value.GetValue()});
    }
    return Status::OK();
  }
};

class CommandTSCreate : public CommandTSCreateBase {
 public:
  CommandTSCreate() : CommandTSCreateBase(2, 0) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 2) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    return CommandTSCreateBase::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    auto s = timeseries_db.Create(ctx, args_[1], getCreateOption());
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandTSAdd : public CommandTSCreateBase {
 public:
  CommandTSAdd() : CommandTSCreateBase(4, 0) {
    RegisterHandler("ON_DUPLICATE", [this](TSOptionsParser &parser) { return HandleOnDuplicatePolicy(parser); });
  }
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 4) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    user_key_ = args[1];
    CommandParser parser(args, 2);
    auto ts_parse = parser.TakeInt<uint64_t>();
    if (!ts_parse.IsOK()) {
      return {Status::RedisParseErr, errInvalidTimestamp};
    }
    auto value_parse = parser.TakeFloat<double>();
    if (!value_parse.IsOK()) {
      return {Status::RedisParseErr, errInvalidValue};
    }
    ts_ = ts_parse.GetValue();
    value_ = value_parse.GetValue();
    return CommandTSCreateBase::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    std::vector<TSSample> sample{{ts_, value_}};
    auto option = getCreateOption();
    TSChunk::SampleBatch batch(std::move(sample),
                               is_on_duplicate_policy_set_ ? on_duplicate_policy_ : option.duplicate_policy);
    auto s = timeseries_db.MAdd(ctx, user_key_, batch, option);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    auto results = batch.GetFinalResults();
    using AddResult = TSChunk::AddResult;
    for (auto res : results) {
      switch (res.first) {
        case AddResult::kOk:
          *output += redis::Integer(res.second);
          break;
        case AddResult::kOld:
          *output += redis::Error({Status::NotOK, errOldTimestamp});
          break;
        case AddResult::kBlock:
          *output += redis::Error({Status::NotOK, errDupBlock});
          break;
        default:
          unreachable();
      }
    }
    return Status::OK();
  }

 private:
  DuplicatePolicy on_duplicate_policy_ = DuplicatePolicy::BLOCK;
  bool is_on_duplicate_policy_set_ = false;
  std::string user_key_;
  uint64_t ts_ = 0;
  double value_ = 0;

  Status HandleOnDuplicatePolicy(TSOptionsParser &parser) {
    if (parser.EatEqICase("BLOCK")) {
      on_duplicate_policy_ = DuplicatePolicy::BLOCK;
    } else if (parser.EatEqICase("FIRST")) {
      on_duplicate_policy_ = DuplicatePolicy::FIRST;
    } else if (parser.EatEqICase("LAST")) {
      on_duplicate_policy_ = DuplicatePolicy::LAST;
    } else if (parser.EatEqICase("MAX")) {
      on_duplicate_policy_ = DuplicatePolicy::MAX;
    } else if (parser.EatEqICase("MIN")) {
      on_duplicate_policy_ = DuplicatePolicy::MIN;
    } else if (parser.EatEqICase("SUM")) {
      on_duplicate_policy_ = DuplicatePolicy::SUM;
    } else {
      return {Status::RedisParseErr, errDuplicatePolicy};
    }
    is_on_duplicate_policy_set_ = true;
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(Timeseries, MakeCmdAttr<CommandTSCreate>("ts.create", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTSAdd>("ts.add", -4, "write", 1, 1, 1), );

}  // namespace redis
