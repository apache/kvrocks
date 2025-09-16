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
constexpr const char *errTSKeyNotFound = "the key is not a TSDB key";
constexpr const char *errTSInvalidAlign = "unknown ALIGN parameter";
constexpr const char *errTSMRangeArgsNum = "wrong number of arguments for 'ts.mrange' command";

using ChunkType = TimeSeriesMetadata::ChunkType;
using DuplicatePolicy = TimeSeriesMetadata::DuplicatePolicy;
using TSAggregatorType = redis::TSAggregatorType;
using TSCreateRuleResult = redis::TSCreateRuleResult;
using GroupReducerType = redis::TSMRangeOption::GroupReducerType;

const std::unordered_map<ChunkType, std::string_view> kChunkTypeMap = {
    {ChunkType::COMPRESSED, "compressed"},
    {ChunkType::UNCOMPRESSED, "uncompressed"},
};
const std::unordered_map<DuplicatePolicy, std::string_view> kDuplicatePolicyMap = {
    {DuplicatePolicy::BLOCK, "block"}, {DuplicatePolicy::FIRST, "first"}, {DuplicatePolicy::LAST, "last"},
    {DuplicatePolicy::MIN, "min"},     {DuplicatePolicy::MAX, "max"},     {DuplicatePolicy::SUM, "sum"},
};
const std::unordered_map<TSAggregatorType, std::string_view> kAggregatorTypeMap = {
    {TSAggregatorType::AVG, "avg"},     {TSAggregatorType::SUM, "sum"},     {TSAggregatorType::MIN, "min"},
    {TSAggregatorType::MAX, "max"},     {TSAggregatorType::RANGE, "range"}, {TSAggregatorType::COUNT, "count"},
    {TSAggregatorType::FIRST, "first"}, {TSAggregatorType::LAST, "last"},   {TSAggregatorType::STD_P, "std.p"},
    {TSAggregatorType::STD_S, "std.s"}, {TSAggregatorType::VAR_P, "var.p"}, {TSAggregatorType::VAR_S, "var.s"},
};
const std::unordered_map<GroupReducerType, std::string_view> kGroupReducerTypeMap = {
    {GroupReducerType::AVG, "avg"},     {GroupReducerType::SUM, "sum"},     {GroupReducerType::MIN, "min"},
    {GroupReducerType::MAX, "max"},     {GroupReducerType::RANGE, "range"}, {GroupReducerType::COUNT, "count"},
    {GroupReducerType::STD_P, "std.p"}, {GroupReducerType::STD_S, "std.s"}, {GroupReducerType::VAR_P, "var.p"},
    {GroupReducerType::VAR_S, "var.s"},
};

std::string FormatAddResultAsRedisReply(TSChunk::AddResult res) {
  using AddResultType = TSChunk::AddResultType;
  switch (res.type) {
    case AddResultType::kInsert:
    case AddResultType::kUpdate:
    case AddResultType::kSkip:
      return redis::Integer(res.sample.ts);
    case AddResultType::kOld:
      return redis::Error({Status::NotOK, errOldTimestamp});
    case AddResultType::kBlock:
      return redis::Error({Status::NotOK, errDupBlock});
    default:
      unreachable();
  }
  return "";
}

std::string FormatTSSampleAsRedisReply(TSSample sample) {
  std::string res = redis::MultiLen(2);
  res += redis::Integer(sample.ts);
  res += redis::Double(redis::RESP::v3, sample.v);
  return res;
}

std::string_view FormatChunkTypeAsRedisReply(ChunkType chunk_type) {
  auto it = kChunkTypeMap.find(chunk_type);
  if (it == kChunkTypeMap.end()) {
    unreachable();
  }
  return it->second;
}

std::string_view FormatDuplicatePolicyAsRedisReply(DuplicatePolicy policy) {
  auto it = kDuplicatePolicyMap.find(policy);
  if (it == kDuplicatePolicyMap.end()) {
    unreachable();
  }
  return it->second;
}

std::string_view FormatAggregatorTypeAsRedisReply(TSAggregatorType aggregator) {
  auto it = kAggregatorTypeMap.find(aggregator);
  if (it == kAggregatorTypeMap.end()) {
    unreachable();
  }
  return it->second;
}

std::string_view GroupReducerTypeToString(GroupReducerType reducer) {
  auto it = kGroupReducerTypeMap.find(reducer);
  if (it == kGroupReducerTypeMap.end()) {
    unreachable();
  }
  return it->second;
}

std::string GroupSourceToString(const std::vector<std::string> &sources) {
  std::string res;
  size_t total_size = 0;
  for (auto &src : sources) {
    total_size += src.size();
  }
  res.reserve(total_size + sources.size());
  for (size_t i = 0; i < sources.size(); i++) {
    res += sources[i];
    if (i != sources.size() - 1) {
      res += ',';
    }
  }
  return res;
}

std::string FormatCreateRuleResAsRedisReply(TSCreateRuleResult res) {
  switch (res) {
    case TSCreateRuleResult::kOK:
      return redis::RESP_OK;
    case TSCreateRuleResult::kSrcNotExist:
    case TSCreateRuleResult::kDstNotExist:
      return redis::Error({Status::NotOK, errTSKeyNotFound});
    case TSCreateRuleResult::kSrcEqDst:
      return redis::Error({Status::NotOK, "the source key and destination key should be different"});
    case TSCreateRuleResult::kSrcHasSourceRule:
      return redis::Error({Status::NotOK, "the source key already has a source rule"});
    case TSCreateRuleResult::kDstHasSourceRule:
      return redis::Error({Status::NotOK, "the destination key already has a src rule"});
    case TSCreateRuleResult::kDstHasDestRule:
      return redis::Error({Status::NotOK, "the destination key already has a dst rule"});
    default:
      unreachable();
  }
  return "";
}

std::string FormatTSLabelListAsRedisReply(const redis::LabelKVList &labels) {
  std::vector<std::string> labels_str;
  labels_str.reserve(labels.size());
  for (const auto &label : labels) {
    auto str = redis::Array(
        {redis::BulkString(label.k), label.v.size() ? redis::BulkString(label.v) : redis::NilString(redis::RESP::v3)});
    labels_str.push_back(str);
  }
  return redis::Array(labels_str);
}

}  // namespace

namespace redis {

class KeywordCommandBase : public Commander {
 public:
  KeywordCommandBase() = default;

  Status Parse(const std::vector<std::string> &args) override {
    TSOptionsParser parser(std::next(args.begin(), static_cast<std::ptrdiff_t>(skip_num_)),
                           std::prev(args.end(), static_cast<std::ptrdiff_t>(tail_skip_num_)));
    while (parser.Good()) {
      auto &value = parser.RawTake();
      auto value_upper = util::ToUpper(value);
      if (containsKeyword(value_upper, true)) {
        Status s = handlers_[value_upper](parser);
        if (!s.IsOK()) return s;
      }
    }
    return Commander::Parse(args);
  }

 protected:
  using TSOptionsParser = CommandParser<CommandTokens::const_iterator>;

  template <typename Handler>
  void registerHandler(const std::string &keyword, Handler &&handler) {
    handlers_.emplace(util::ToUpper(keyword), std::forward<Handler>(handler));
  }
  virtual void registerDefaultHandlers() = 0;

  void setSkipNum(size_t num) { skip_num_ = num; }
  void setTailSkipNum(size_t num) { tail_skip_num_ = num; }
  bool containsKeyword(const std::string &keyword, bool is_upper = false) const {
    if (is_upper) {
      return handlers_.count(keyword);
    } else {
      return handlers_.count(util::ToUpper(keyword));
    }
  }

 private:
  size_t skip_num_ = 0;
  size_t tail_skip_num_ = 0;
  std::unordered_map<std::string, std::function<Status(TSOptionsParser &)>> handlers_;
};

class CommandTSCreateBase : public KeywordCommandBase {
 protected:
  const TSCreateOption &getCreateOption() const { return create_option_; }

  void registerDefaultHandlers() override {
    registerHandler("RETENTION",
                    [this](TSOptionsParser &parser) { return handleRetention(parser, create_option_.retention_time); });
    registerHandler("CHUNK_SIZE",
                    [this](TSOptionsParser &parser) { return handleChunkSize(parser, create_option_.chunk_size); });
    registerHandler("ENCODING",
                    [this](TSOptionsParser &parser) { return handleEncoding(parser, create_option_.chunk_type); });
    registerHandler("DUPLICATE_POLICY", [this](TSOptionsParser &parser) {
      return handleDuplicatePolicy(parser, create_option_.duplicate_policy);
    });
    registerHandler("LABELS", [this](TSOptionsParser &parser) { return handleLabels(parser, create_option_.labels); });
  }

  static Status handleRetention(TSOptionsParser &parser, uint64_t &retention_time) {
    auto parse_retention = parser.TakeInt<uint64_t>();
    if (!parse_retention.IsOK()) {
      return {Status::RedisParseErr, errBadRetention};
    }
    retention_time = parse_retention.GetValue();
    return Status::OK();
  }

  static Status handleChunkSize(TSOptionsParser &parser, uint64_t &chunk_size) {
    auto parse_chunk_size = parser.TakeInt<uint64_t>();
    if (!parse_chunk_size.IsOK()) {
      return {Status::RedisParseErr, errBadChunkSize};
    }
    chunk_size = parse_chunk_size.GetValue();
    return Status::OK();
  }

  static Status handleEncoding(TSOptionsParser &parser, ChunkType &chunk_type) {
    if (parser.EatEqICase("UNCOMPRESSED")) {
      chunk_type = ChunkType::UNCOMPRESSED;
    } else if (parser.EatEqICase("COMPRESSED")) {
      chunk_type = ChunkType::COMPRESSED;
    } else {
      return {Status::RedisParseErr, errBadEncoding};
    }
    return Status::OK();
  }

  static Status handleDuplicatePolicy(TSOptionsParser &parser, DuplicatePolicy &duplicate_policy) {
    if (parser.EatEqICase("BLOCK")) {
      duplicate_policy = DuplicatePolicy::BLOCK;
    } else if (parser.EatEqICase("FIRST")) {
      duplicate_policy = DuplicatePolicy::FIRST;
    } else if (parser.EatEqICase("LAST")) {
      duplicate_policy = DuplicatePolicy::LAST;
    } else if (parser.EatEqICase("MAX")) {
      duplicate_policy = DuplicatePolicy::MAX;
    } else if (parser.EatEqICase("MIN")) {
      duplicate_policy = DuplicatePolicy::MIN;
    } else if (parser.EatEqICase("SUM")) {
      duplicate_policy = DuplicatePolicy::SUM;
    } else {
      return {Status::RedisParseErr, errDuplicatePolicy};
    }
    return Status::OK();
  }

  static Status handleLabels(TSOptionsParser &parser, LabelKVList &labels) {
    while (parser.Good()) {
      auto parse_key = parser.TakeStr();
      auto parse_value = parser.TakeStr();
      if (!parse_key.IsOK() || !parse_value.IsOK()) {
        break;
      }
      labels.push_back({parse_key.GetValue(), parse_value.GetValue()});
    }
    return Status::OK();
  }

 private:
  TSCreateOption create_option_;
};

class CommandTSCreate : public CommandTSCreateBase {
 public:
  CommandTSCreate() { registerDefaultHandlers(); }
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 2) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    CommandTSCreateBase::setSkipNum(2);
    return CommandTSCreateBase::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    auto s = timeseries_db.Create(ctx, args_[1], getCreateOption());
    if (!s.ok() && s.IsInvalidArgument()) return {Status::RedisExecErr, errKeyAlreadyExists};
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandTSInfo : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override { return Commander::Parse(args); }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    TSInfoResult info;
    auto s = timeseries_db.Info(ctx, args_[1], &info);
    if (s.IsNotFound()) return {Status::RedisExecErr, errTSKeyNotFound};
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    *output = redis::MultiLen(24);
    *output += redis::SimpleString("totalSamples");
    *output += redis::Integer(info.total_samples);
    *output += redis::SimpleString("memoryUsage");
    *output += redis::Integer(info.memory_usage);
    *output += redis::SimpleString("firstTimestamp");
    *output += redis::Integer(info.first_timestamp);
    *output += redis::SimpleString("lastTimestamp");
    *output += redis::Integer(info.last_timestamp);
    *output += redis::SimpleString("retentionTime");
    *output += redis::Integer(info.metadata.retention_time);
    *output += redis::SimpleString("chunkCount");
    *output += redis::Integer(info.metadata.size);
    *output += redis::SimpleString("chunkSize");
    *output += redis::Integer(info.metadata.chunk_size);
    *output += redis::SimpleString("chunkType");
    *output += redis::SimpleString(FormatChunkTypeAsRedisReply(info.metadata.chunk_type));
    *output += redis::SimpleString("duplicatePolicy");
    *output += redis::SimpleString(FormatDuplicatePolicyAsRedisReply(info.metadata.duplicate_policy));
    *output += redis::SimpleString("labels");
    *output += FormatTSLabelListAsRedisReply(info.labels);
    *output += redis::SimpleString("sourceKey");
    *output += info.metadata.source_key.empty() ? redis::NilString(redis::RESP::v3)
                                                : redis::BulkString(info.metadata.source_key);
    *output += redis::SimpleString("rules");
    std::vector<std::string> rules_str;
    rules_str.reserve(info.downstream_rules.size());
    for (const auto &[key, rule] : info.downstream_rules) {
      const auto &aggregator = rule.aggregator;
      auto str = redis::Array({redis::BulkString(key), redis::Integer(aggregator.bucket_duration),
                               redis::SimpleString(FormatAggregatorTypeAsRedisReply(aggregator.type)),
                               redis::Integer(aggregator.alignment)});
      rules_str.push_back(str);
    }
    *output += redis::Array(rules_str);
    return Status::OK();
  }
};

class CommandTSAdd : public CommandTSCreateBase {
 public:
  CommandTSAdd() { registerDefaultHandlers(); }
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 4) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
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
    CommandTSCreateBase::setSkipNum(4);
    return CommandTSCreateBase::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    const auto &option = getCreateOption();

    TSChunk::AddResult res;
    auto s = timeseries_db.Add(ctx, args_[1], {ts_, value_}, option, &res,
                               is_on_dup_policy_set_ ? &on_dup_policy_ : nullptr);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output += FormatAddResultAsRedisReply(res);

    return Status::OK();
  }

 protected:
  void registerDefaultHandlers() override {
    CommandTSCreateBase::registerDefaultHandlers();
    registerHandler("ON_DUPLICATE", [this](TSOptionsParser &parser) { return handleOnDuplicatePolicy(parser); });
  }

 private:
  DuplicatePolicy on_dup_policy_ = DuplicatePolicy::BLOCK;
  bool is_on_dup_policy_set_ = false;
  uint64_t ts_ = 0;
  double value_ = 0;

  Status handleOnDuplicatePolicy(TSOptionsParser &parser) {
    if (parser.EatEqICase("BLOCK")) {
      on_dup_policy_ = DuplicatePolicy::BLOCK;
    } else if (parser.EatEqICase("FIRST")) {
      on_dup_policy_ = DuplicatePolicy::FIRST;
    } else if (parser.EatEqICase("LAST")) {
      on_dup_policy_ = DuplicatePolicy::LAST;
    } else if (parser.EatEqICase("MAX")) {
      on_dup_policy_ = DuplicatePolicy::MAX;
    } else if (parser.EatEqICase("MIN")) {
      on_dup_policy_ = DuplicatePolicy::MIN;
    } else if (parser.EatEqICase("SUM")) {
      on_dup_policy_ = DuplicatePolicy::SUM;
    } else {
      return {Status::RedisParseErr, errDuplicatePolicy};
    }
    is_on_dup_policy_set_ = true;
    return Status::OK();
  }
};

class CommandTSMAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 4 || (args.size() - 1) % 3 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    CommandParser parser(args, 1);
    for (size_t i = 1; i < args.size(); i += 3) {
      const auto &user_key = args[i];
      parser.Skip(1);
      auto ts_parse = parser.TakeInt<uint64_t>();
      if (!ts_parse.IsOK()) {
        return {Status::RedisParseErr, errInvalidTimestamp};
      }
      auto value_parse = parser.TakeFloat<double>();
      if (!value_parse.IsOK()) {
        return Status{Status::RedisParseErr, errInvalidValue};
      }
      userkey_samples_map_[user_key].push_back({ts_parse.GetValue(), value_parse.GetValue()});
      userkey_indexes_map_[user_key].push_back(i / 3);
      samples_count_ += 1;
    }
    return Commander::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());

    auto replies = std::vector<std::string>(samples_count_);
    for (auto &[user_key, samples] : userkey_samples_map_) {
      std::vector<TSChunk::AddResult> res;
      auto count = samples.size();
      auto s = timeseries_db.MAdd(ctx, user_key, std::move(samples), &res);
      std::string err_reply;
      if (!s.ok()) {
        err_reply = s.IsNotFound() ? redis::Error({Status::NotOK, errTSKeyNotFound})
                                   : redis::Error({Status::NotOK, s.ToString()});
      }
      for (size_t i = 0; i < count; i++) {
        size_t idx = userkey_indexes_map_[user_key][i];
        replies[idx] = s.ok() ? FormatAddResultAsRedisReply(res[i]) : err_reply;
      }
    }
    *output = redis::MultiLen(samples_count_);
    for (auto &reply : replies) {
      if (reply.empty()) continue;
      *output += reply;
    }
    return Status::OK();
  }

 private:
  size_t samples_count_ = 0;
  std::unordered_map<std::string_view, std::vector<TSSample>> userkey_samples_map_;
  std::unordered_map<std::string_view, std::vector<size_t>> userkey_indexes_map_;
};

class CommandTSAggregatorBase : public KeywordCommandBase {
 protected:
  const TSAggregator &getAggregator() const { return aggregator_; }

  void registerDefaultHandlers() override {
    registerHandler("AGGREGATION", [this](TSOptionsParser &parser) { return handleAggregation(parser, aggregator_); });
    registerHandler("ALIGN", [this](TSOptionsParser &parser) { return handleAlignCommon(parser, aggregator_); });
  }

  static Status handleAggregation(TSOptionsParser &parser, TSAggregator &aggregator) {
    auto &type = aggregator.type;
    if (parser.EatEqICase("AVG")) {
      type = TSAggregatorType::AVG;
    } else if (parser.EatEqICase("SUM")) {
      type = TSAggregatorType::SUM;
    } else if (parser.EatEqICase("MIN")) {
      type = TSAggregatorType::MIN;
    } else if (parser.EatEqICase("MAX")) {
      type = TSAggregatorType::MAX;
    } else if (parser.EatEqICase("RANGE")) {
      type = TSAggregatorType::RANGE;
    } else if (parser.EatEqICase("COUNT")) {
      type = TSAggregatorType::COUNT;
    } else if (parser.EatEqICase("FIRST")) {
      type = TSAggregatorType::FIRST;
    } else if (parser.EatEqICase("LAST")) {
      type = TSAggregatorType::LAST;
    } else if (parser.EatEqICase("STD.P")) {
      type = TSAggregatorType::STD_P;
    } else if (parser.EatEqICase("STD.S")) {
      type = TSAggregatorType::STD_S;
    } else if (parser.EatEqICase("VAR.P")) {
      type = TSAggregatorType::VAR_P;
    } else if (parser.EatEqICase("VAR.S")) {
      type = TSAggregatorType::VAR_S;
    } else {
      return {Status::RedisParseErr, "Invalid aggregator type"};
    }

    auto duration = parser.TakeInt<uint64_t>();
    if (!duration.IsOK()) {
      return {Status::RedisParseErr, "Couldn't parse AGGREGATION"};
    }
    aggregator.bucket_duration = duration.GetValue();
    if (aggregator.bucket_duration == 0) {
      return {Status::RedisParseErr, "bucketDuration must be greater than zero"};
    }
    return Status::OK();
  }
  static Status handleAlignCommon(TSOptionsParser &parser, TSAggregator &aggregator) {
    auto align = parser.TakeInt<uint64_t>();
    if (!align.IsOK()) {
      return {Status::RedisParseErr, errTSInvalidAlign};
    }
    aggregator.alignment = align.GetValue();
    return Status::OK();
  }
  static Status handleLatest([[maybe_unused]] TSOptionsParser &parser, bool &is_return_latest) {
    is_return_latest = true;
    return Status::OK();
  }

 private:
  TSAggregator aggregator_;
};

class CommandTSRangeBase : virtual public CommandTSAggregatorBase {
 public:
  explicit CommandTSRangeBase(size_t skip_num) : skip_num_(skip_num) {}

  Status Parse(const std::vector<std::string> &args) override {
    TSOptionsParser parser(std::next(args.begin(), static_cast<std::ptrdiff_t>(skip_num_)), args.end());
    // Parse start timestamp
    auto start_ts = parser.TakeInt<uint64_t>();
    if (!start_ts.IsOK()) {
      auto start_ts_str = parser.TakeStr();
      if (!start_ts_str.IsOK() || start_ts_str.GetValue() != "-") {
        return {Status::RedisParseErr, "wrong fromTimestamp"};
      }
      // "-" means use default start timestamp: 0
    } else {
      is_start_explicit_set_ = true;
      option_.start_ts = start_ts.GetValue();
    }

    // Parse end timestamp
    auto end_ts = parser.TakeInt<uint64_t>();
    if (!end_ts.IsOK()) {
      auto end_ts_str = parser.TakeStr();
      if (!end_ts_str.IsOK() || end_ts_str.GetValue() != "+") {
        return {Status::RedisParseErr, "wrong toTimestamp"};
      }
      // "+" means use default end timestamp: MAX_TIMESTAMP
    } else {
      is_end_explicit_set_ = true;
      option_.end_ts = end_ts.GetValue();
    }
    KeywordCommandBase::setSkipNum(skip_num_ + 2);
    auto s = KeywordCommandBase::Parse(args);
    if (!s.IsOK()) return s;
    if (is_alignment_explicit_set_ && option_.aggregator.type == TSAggregatorType::NONE) {
      return {Status::RedisParseErr, "ALIGN parameter can only be used with AGGREGATION"};
    }
    return s;
  }

 protected:
  const TSRangeOption &getRangeOption() const { return option_; }

  void registerDefaultHandlers() override {
    registerHandler("LATEST",
                    [this](TSOptionsParser &parser) { return handleLatest(parser, option_.is_return_latest); });
    registerHandler("FILTER_BY_TS",
                    [this](TSOptionsParser &parser) { return handleFilterByTS(parser, option_.filter_by_ts); });
    registerHandler("FILTER_BY_VALUE",
                    [this](TSOptionsParser &parser) { return handleFilterByValue(parser, option_.filter_by_value); });
    registerHandler("COUNT", [this](TSOptionsParser &parser) { return handleCount(parser, option_.count_limit); });
    registerHandler("ALIGN", [this](TSOptionsParser &parser) { return handleAlign(parser); });
    registerHandler("AGGREGATION",
                    [this](TSOptionsParser &parser) { return handleAggregation(parser, option_.aggregator); });
    registerHandler("BUCKETTIMESTAMP",
                    [this](TSOptionsParser &parser) { return handleBucketTimestamp(parser, option_); });
    registerHandler("EMPTY", [this](TSOptionsParser &parser) { return handleEmpty(parser, option_); });
  }

  static Status handleFilterByTS(TSOptionsParser &parser, std::set<uint64_t> &filter_by_ts) {
    filter_by_ts.clear();
    while (parser.Good()) {
      auto ts = parser.TakeInt<uint64_t>();
      if (!ts.IsOK()) break;
      filter_by_ts.insert(ts.GetValue());
    }
    return Status::OK();
  }

  static Status handleFilterByValue(TSOptionsParser &parser,
                                    std::optional<std::pair<double, double>> &filter_by_value) {
    auto min = parser.TakeFloat<double>();
    auto max = parser.TakeFloat<double>();
    if (!min.IsOK() || !max.IsOK()) {
      return {Status::RedisParseErr, "Invalid min or max value"};
    }
    filter_by_value = std::make_optional(std::make_pair(min.GetValue(), max.GetValue()));
    return Status::OK();
  }

  static Status handleCount(TSOptionsParser &parser, uint64_t &count_limit) {
    auto count = parser.TakeInt<uint64_t>();
    if (!count.IsOK()) {
      return {Status::RedisParseErr, "Couldn't parse COUNT"};
    }
    count_limit = count.GetValue();
    if (count_limit == 0) {
      return {Status::RedisParseErr, "Invalid COUNT value"};
    }
    return Status::OK();
  }

  static Status handleBucketTimestamp(TSOptionsParser &parser, TSRangeOption &option) {
    if (option.aggregator.type == TSAggregatorType::NONE) {
      return {Status::RedisParseErr, "BUCKETTIMESTAMP flag should be the 3rd or 4th flag after AGGREGATION flag"};
    }
    using BucketTimestampType = TSRangeOption::BucketTimestampType;
    if (parser.EatEqICase("START")) {
      option.bucket_timestamp_type = BucketTimestampType::Start;
    } else if (parser.EatEqICase("END")) {
      option.bucket_timestamp_type = BucketTimestampType::End;
    } else if (parser.EatEqICase("MID")) {
      option.bucket_timestamp_type = BucketTimestampType::Mid;
    } else {
      return {Status::RedisParseErr, "unknown BUCKETTIMESTAMP parameter"};
    }
    return Status::OK();
  }

  static Status handleEmpty([[maybe_unused]] TSOptionsParser &parser, TSRangeOption &option) {
    if (option.aggregator.type == TSAggregatorType::NONE) {
      return {Status::RedisParseErr, "EMPTY flag should be the 3rd or 5th flag after AGGREGATION flag"};
    }
    option.is_return_empty = true;
    return Status::OK();
  }

 private:
  TSRangeOption option_;
  size_t skip_num_;
  bool is_start_explicit_set_ = false;
  bool is_end_explicit_set_ = false;
  bool is_alignment_explicit_set_ = false;

  Status handleAlign(TSOptionsParser &parser) {
    auto s = handleAlignCommon(parser, option_.aggregator);
    if (s.IsOK()) {
      is_alignment_explicit_set_ = true;
      return Status::OK();
    }

    auto align_str = parser.TakeStr();
    if (!align_str.IsOK()) {
      return {Status::RedisParseErr, errTSInvalidAlign};
    }

    const auto &value = align_str.GetValue();
    if (value == "-" || value == "+") {
      bool is_explicit_set = value == "-" ? is_start_explicit_set_ : is_end_explicit_set_;
      auto err_msg = value == "-" ? "start alignment can only be used with explicit start timestamp"
                                  : "end alignment can only be used with explicit end timestamp";

      if (!is_explicit_set) {
        return {Status::RedisParseErr, err_msg};
      }

      option_.aggregator.alignment = value == "-" ? option_.start_ts : option_.end_ts;
    } else {
      return {Status::RedisParseErr, errTSInvalidAlign};
    }
    is_alignment_explicit_set_ = true;
    return Status::OK();
  }
};

class CommandTSRange : public CommandTSRangeBase {
 public:
  CommandTSRange() : CommandTSRangeBase(2) { registerDefaultHandlers(); }
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 4) {
      return {Status::RedisParseErr, "wrong number of arguments for 'ts.range' command"};
    }

    return CommandTSRangeBase::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    std::vector<TSSample> res;
    auto s = timeseries_db.Range(ctx, args_[1], getRangeOption(), &res);
    if (!s.ok()) return {Status::RedisExecErr, errKeyNotFound};
    std::vector<std::string> reply;
    reply.reserve(res.size());
    for (auto &sample : res) {
      reply.push_back(FormatTSSampleAsRedisReply(sample));
    }
    *output = redis::Array(reply);
    return Status::OK();
  }
};

class CommandTSCreateRule : public CommandTSAggregatorBase {
 public:
  explicit CommandTSCreateRule() { registerDefaultHandlers(); }
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 6) {
      return {Status::NotOK, "wrong number of arguments for 'TS.CREATERULE' command"};
    }
    src_key_ = args[1];
    dst_key_ = args[2];
    CommandTSAggregatorBase::setSkipNum(3);
    return CommandTSAggregatorBase::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    auto res = TSCreateRuleResult::kOK;
    auto s = timeseries_db.CreateRule(ctx, src_key_, dst_key_, getAggregator(), &res);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    *output = FormatCreateRuleResAsRedisReply(res);
    return Status::OK();
  }

 private:
  std::string src_key_;
  std::string dst_key_;
};

class CommandTSGet : public CommandTSAggregatorBase {
 public:
  CommandTSGet() { registerDefaultHandlers(); }
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 2) {
      return {Status::RedisParseErr, "wrong number of arguments for 'ts.get' command"};
    }
    CommandTSAggregatorBase::setSkipNum(2);
    return CommandTSAggregatorBase::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    std::vector<TSSample> res;
    auto s = timeseries_db.Get(ctx, args_[1], is_return_latest_, &res);
    if (!s.ok()) return {Status::RedisExecErr, errKeyNotFound};

    std::vector<std::string> reply;
    reply.reserve(res.size());
    for (auto &sample : res) {
      reply.push_back(FormatTSSampleAsRedisReply(sample));
    }
    *output = redis::Array(reply);
    return Status::OK();
  }

 protected:
  void registerDefaultHandlers() override {
    registerHandler("LATEST", [this](TSOptionsParser &parser) { return handleLatest(parser, is_return_latest_); });
  }

 private:
  bool is_return_latest_ = false;
};

class CommandTSMGetBase : virtual public CommandTSAggregatorBase {
 protected:
  const TSMGetOption &getMGetOption() const { return option_; }

  void registerDefaultHandlers() override {
    registerHandler("WITHLABELS",
                    [this](TSOptionsParser &parser) { return handleWithLabels(parser, option_.with_labels); });
    registerHandler("SELECTED_LABELS",
                    [this](TSOptionsParser &parser) { return handleSelectedLabels(parser, option_.selected_labels); });
    registerHandler("FILTER", [this](TSOptionsParser &parser) { return handleFilterExpr(parser, option_.filter); });
  }

  static Status handleWithLabels([[maybe_unused]] TSOptionsParser &parser, bool &with_labels) {
    with_labels = true;
    return Status::OK();
  }
  Status handleSelectedLabels(TSOptionsParser &parser, std::set<std::string> &selected_labels) {
    while (parser.Good()) {
      auto &value = parser.RawPeek();
      if (containsKeyword(value)) {
        break;
      }
      selected_labels.emplace(parser.TakeStr().GetValue());
    }
    return Status::OK();
  }
  Status handleFilterExpr(TSOptionsParser &parser, TSMGetOption::FilterOption &filter_option) {
    auto filter_parser = TSMQueryFilterParser(filter_option);
    while (parser.Good()) {
      auto &value = parser.RawPeek();
      if (containsKeyword(value)) {
        break;
      }
      auto s = filter_parser.Parse(parser.TakeStr().GetValue());
      if (!s.IsOK()) return s;
    }
    return filter_parser.Check();
  }

 private:
  TSMGetOption option_;
};

class CommandTSMGet : public CommandTSMGetBase {
 public:
  CommandTSMGet() { registerDefaultHandlers(); }
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 3) {
      return {Status::RedisParseErr, "wrong number of arguments for 'ts.mget' command"};
    }
    CommandTSMGetBase::setSkipNum(1);
    return CommandTSMGetBase::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    std::vector<TSMGetResult> results;
    auto s = timeseries_db.MGet(ctx, getMGetOption(), is_return_latest_, &results);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    std::vector<std::string> reply;
    reply.reserve(results.size());
    for (auto &result : results) {
      std::vector<std::string> entry(3);
      entry[0] = redis::BulkString(result.name);
      entry[1] = FormatTSLabelListAsRedisReply(result.labels);
      std::vector<std::string> temp;
      for (auto &sample : result.samples) {
        temp.push_back(FormatTSSampleAsRedisReply(sample));
      }
      entry[2] = redis::Array(temp);
      reply.push_back(redis::Array(entry));
    }
    *output = redis::Array(reply);
    return Status::OK();
  }

 protected:
  void registerDefaultHandlers() override {
    CommandTSMGetBase::registerDefaultHandlers();
    registerHandler("LATEST", [this](TSOptionsParser &parser) { return handleLatest(parser, is_return_latest_); });
  }

 private:
  bool is_return_latest_ = false;
};

class CommandTSMRange : public CommandTSRangeBase, public CommandTSMGetBase {
 public:
  CommandTSMRange() : CommandTSRangeBase(1) { registerDefaultHandlers(); }
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 5) {
      return {Status::RedisParseErr, errTSMRangeArgsNum};
    }
    auto s = CommandTSRangeBase::Parse(args);
    if (!s.IsOK()) return s;
    // Combine MGet and Range options
    static_cast<TSRangeOption &>(option_) = getRangeOption();
    static_cast<TSMGetOption &>(option_) = getMGetOption();

    return Status::OK();
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    std::vector<TSMRangeResult> results;
    auto s = timeseries_db.MRange(ctx, option_, &results);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    std::vector<std::string> reply;
    reply.reserve(results.size());
    for (auto &result : results) {
      std::vector<std::string> entry(3);
      entry[0] =
          redis::BulkString(option_.group_by_label.empty() ? result.name : option_.group_by_label + "=" + result.name);
      if (option_.group_by_label.size() && option_.with_labels) {
        result.labels.reserve(result.labels.size() + 2);
        result.labels.push_back(LabelKVPair{"__reducer__", std::string(GroupReducerTypeToString(option_.reducer))});
        result.labels.push_back(LabelKVPair{"__source__", GroupSourceToString(result.source_keys)});
      }
      entry[1] = FormatTSLabelListAsRedisReply(result.labels);
      std::vector<std::string> temp;
      for (auto &sample : result.samples) {
        temp.push_back(FormatTSSampleAsRedisReply(sample));
      }
      entry[2] = redis::Array(temp);
      reply.push_back(redis::Array(entry));
    }
    *output = redis::Array(reply);
    return Status::OK();
  }

 protected:
  void registerDefaultHandlers() override {
    CommandTSMGetBase::registerDefaultHandlers();
    CommandTSRangeBase::registerDefaultHandlers();
    registerHandler("GROUPBY", [this](TSOptionsParser &parser) { return handleGroupBy(parser, option_); });
  }

  static Status handleGroupBy(TSOptionsParser &parser, TSMRangeOption &option) {
    auto group_value_parse = parser.TakeStr();
    if (group_value_parse.IsOK()) {
      option.group_by_label = std::move(group_value_parse.GetValue());
    } else {
      return {Status::RedisParseErr, errTSMRangeArgsNum};
    }
    auto reduce_keyword_parse = parser.TakeStr();
    if (!reduce_keyword_parse.IsOK() || reduce_keyword_parse.GetValue() != "REDUCE") {
      return {Status::RedisParseErr, errTSMRangeArgsNum};
    }
    auto &type = option.reducer;
    using GroupReducerType = TSMRangeOption::GroupReducerType;
    if (parser.EatEqICase("AVG")) {
      type = GroupReducerType::AVG;
    } else if (parser.EatEqICase("SUM")) {
      type = GroupReducerType::SUM;
    } else if (parser.EatEqICase("MIN")) {
      type = GroupReducerType::MIN;
    } else if (parser.EatEqICase("MAX")) {
      type = GroupReducerType::MAX;
    } else if (parser.EatEqICase("RANGE")) {
      type = GroupReducerType::RANGE;
    } else if (parser.EatEqICase("COUNT")) {
      type = GroupReducerType::COUNT;
    } else if (parser.EatEqICase("STD.P")) {
      type = GroupReducerType::STD_P;
    } else if (parser.EatEqICase("STD.S")) {
      type = GroupReducerType::STD_S;
    } else if (parser.EatEqICase("VAR.P")) {
      type = GroupReducerType::VAR_P;
    } else if (parser.EatEqICase("VAR.S")) {
      type = GroupReducerType::VAR_S;
    } else if (parser.Good()) {
      return {Status::RedisParseErr, "Invalid reducer type"};
    } else {
      return {Status::RedisParseErr, errTSMRangeArgsNum};
    }
    return Status::OK();
  }

 private:
  TSMRangeOption option_;
};

class CommandTSIncrByDecrBy : public CommandTSCreateBase {
 public:
  CommandTSIncrByDecrBy() { registerDefaultHandlers(); }
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    auto value_parse = parser.TakeFloat<double>();
    if (!value_parse.IsOK()) {
      return {Status::RedisParseErr, errInvalidValue};
    }
    value_ = value_parse.GetValue();
    if (util::ToUpper(args[0]) == "TS.DECRBY") {
      value_ = -value_;
    }
    CommandTSCreateBase::setSkipNum(3);
    return CommandTSCreateBase::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    const auto &option = getCreateOption();

    if (!is_ts_set_) {
      // TODO: Should modify function `Add` and `IncrBy` to add a sample with current time
    }
    TSChunk::AddResult res;
    auto s = timeseries_db.IncrBy(ctx, args_[1], {ts_, value_}, option, &res);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    if (res.type == TSChunk::AddResultType::kOld) {
      *output +=
          redis::Error({Status::NotOK, "timestamp must be equal to or higher than the maximum existing timestamp"});
    } else {
      *output += FormatAddResultAsRedisReply(res);
    }
    return Status::OK();
  }

 protected:
  void registerDefaultHandlers() override {
    CommandTSCreateBase::registerDefaultHandlers();
    registerHandler("TIMESTAMP", [this](TSOptionsParser &parser) {
      auto s = handleTimeStamp(parser, ts_);
      if (!s.IsOK()) return s;
      is_ts_set_ = true;
      return Status::OK();
    });
  }
  static Status handleTimeStamp(TSOptionsParser &parser, uint64_t &ts) {
    auto parse_timestamp = parser.TakeInt<uint64_t>();
    if (!parse_timestamp.IsOK()) {
      return {Status::RedisParseErr, errInvalidTimestamp};
    }
    ts = parse_timestamp.GetValue();
    return Status::OK();
  }

 private:
  bool is_ts_set_ = false;
  uint64_t ts_ = 0;
  double value_ = 0;
};

class CommandTSDel : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 4) {
      return {Status::RedisParseErr, "wrong number of arguments for 'ts.del' command"};
    }
    CommandParser parser(args, 2);
    // Parse start timestamp
    auto start_parse = parser.TakeInt<uint64_t>();
    if (!start_parse.IsOK()) {
      auto start_ts_str = parser.TakeStr();
      if (!start_ts_str.IsOK() || start_ts_str.GetValue() != "-") {
        return {Status::RedisParseErr, "wrong fromTimestamp"};
      }
      // "-" means use default start timestamp: 0
    } else {
      start_ts_ = start_parse.GetValue();
    }
    // Parse end timestamp
    auto end_parse = parser.TakeInt<uint64_t>();
    if (!end_parse.IsOK()) {
      auto end_ts_str = parser.TakeStr();
      if (!end_ts_str.IsOK() || end_ts_str.GetValue() != "+") {
        return {Status::RedisParseErr, "wrong toTimestamp"};
      }
      // "+" means use default end timestamp: MAX_TIMESTAMP
    } else {
      end_ts_ = end_parse.GetValue();
    }
    return Commander::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    uint64_t deleted_count = 0;
    auto s = timeseries_db.Del(ctx, args_[1], start_ts_, end_ts_, &deleted_count);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::Integer(deleted_count);
    return Status::OK();
  }

 private:
  uint64_t start_ts_ = 0;
  uint64_t end_ts_ = TSSample::MAX_TIMESTAMP;
};

REDIS_REGISTER_COMMANDS(Timeseries, MakeCmdAttr<CommandTSCreate>("ts.create", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTSAdd>("ts.add", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTSMAdd>("ts.madd", -4, "write", 1, -3, 1),
                        MakeCmdAttr<CommandTSRange>("ts.range", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTSInfo>("ts.info", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTSGet>("ts.get", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTSCreateRule>("ts.createrule", -6, "write", 1, 2, 1),
                        MakeCmdAttr<CommandTSMGet>("ts.mget", -3, "read-only", NO_KEY),
                        MakeCmdAttr<CommandTSMRange>("ts.mrange", -5, "read-only", NO_KEY),
                        MakeCmdAttr<CommandTSIncrByDecrBy>("ts.incrby", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTSIncrByDecrBy>("ts.decrby", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTSDel>("ts.del", -4, "write", 1, 1, 1), );

}  // namespace redis
