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

#include <limits>
#include <optional>
#include <string_view>

#include "commander.h"
#include "commands/command_parser.h"
#include "error_constants.h"
#include "scan_base.h"
#include "server/server.h"
#include "time_util.h"
#include "types/redis_hash.h"

namespace redis {
namespace {

constexpr uint64_t kMaxHashFieldExpireAtMs = (static_cast<uint64_t>(1) << 46) - 1;

enum class HashFieldExpireTimeMode {
  kRelativeSeconds,
  kRelativeMilliseconds,
  kAbsoluteSeconds,
  kAbsoluteMilliseconds,
};

std::vector<Slice> ToSlices(const std::vector<std::string> &values) {
  std::vector<Slice> slices;
  slices.reserve(values.size());
  for (const auto &value : values) {
    slices.emplace_back(value);
  }
  return slices;
}

std::string IntegerArray(const std::vector<int64_t> &results) {
  std::vector<std::string> entries;
  entries.reserve(results.size());
  for (auto result : results) {
    entries.emplace_back(redis::Integer(result));
  }
  return redis::Array(entries);
}

uint64_t CeilDiv1000(uint64_t value) { return value / 1000 + (value % 1000 != 0 ? 1 : 0); }

Status ParseHashFieldExpireArgument(std::string_view raw, int64_t *expire_arg) {
  auto parsed = ParseInt<int64_t>(raw, 10);
  if (!parsed) {
    return {Status::RedisParseErr, errValueNotInteger};
  }
  if (*parsed < 0) {
    return {Status::RedisParseErr, "invalid expire time, must be >= 0"};
  }
  *expire_arg = *parsed;
  return Status::OK();
}

Status ConvertHashFieldExpireAtMs(int64_t expire_arg, HashFieldExpireTimeMode time_mode, uint64_t now_ms,
                                  uint64_t *expire_at_ms) {
  auto value = static_cast<uint64_t>(expire_arg);
  bool seconds =
      time_mode == HashFieldExpireTimeMode::kRelativeSeconds || time_mode == HashFieldExpireTimeMode::kAbsoluteSeconds;
  bool relative = time_mode == HashFieldExpireTimeMode::kRelativeSeconds ||
                  time_mode == HashFieldExpireTimeMode::kRelativeMilliseconds;

  if (seconds) {
    if (value > kMaxHashFieldExpireAtMs / 1000) {
      return {Status::RedisExecErr, "expire time overflow"};
    }
    value *= 1000;
  }
  if (value > kMaxHashFieldExpireAtMs) {
    return {Status::RedisExecErr, "expire time overflow"};
  }
  if (relative) {
    if (value > kMaxHashFieldExpireAtMs - now_ms) {
      return {Status::RedisExecErr, "expire time overflow"};
    }
    value += now_ms;
  }

  *expire_at_ms = value;
  return Status::OK();
}

std::optional<HashFieldExpireCondition> ParseHashExpireCondition(std::string_view token) {
  if (util::EqualICase(token, "NX")) return HashFieldExpireCondition::kNX;
  if (util::EqualICase(token, "XX")) return HashFieldExpireCondition::kXX;
  if (util::EqualICase(token, "GT")) return HashFieldExpireCondition::kGT;
  if (util::EqualICase(token, "LT")) return HashFieldExpireCondition::kLT;
  return std::nullopt;
}

template <typename Parser>
Status ParseHashFieldListTail(Parser &parser, std::vector<std::string> *fields) {
  if (!parser.Good()) {
    return {Status::RedisParseErr, errWrongNumOfArguments};
  }

  auto num_fields = parser.template TakeInt<int64_t>(NumericRange<int64_t>{1, std::numeric_limits<int64_t>::max()}, 10);
  if (!num_fields) {
    return {Status::RedisParseErr, errValueNotInteger};
  }
  if (static_cast<size_t>(*num_fields) != parser.Remains()) {
    return {Status::RedisParseErr, errWrongNumOfArguments};
  }

  fields->clear();
  fields->reserve(static_cast<size_t>(*num_fields));
  while (parser.Good()) {
    fields->emplace_back(GET_OR_RET(parser.TakeStr()));
  }
  return Status::OK();
}

Status ParseHashFixedFields(const std::vector<std::string> &args, std::vector<std::string> *fields) {
  CommandParser parser(args, 2);
  if (!parser.EatEqICase("FIELDS")) {
    return {Status::RedisParseErr, errInvalidSyntax};
  }
  return ParseHashFieldListTail(parser, fields);
}

Status ParseHashExpireFields(const std::vector<std::string> &args, size_t start,
                             HashFieldExpireCondition *condition_out, std::vector<std::string> *fields) {
  *condition_out = HashFieldExpireCondition::kNone;
  fields->clear();
  bool fields_seen = false;

  for (size_t i = start; i < args.size();) {
    if (util::EqualICase(args[i], "FIELDS")) {
      if (fields_seen) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
      fields_seen = true;
      if (i + 1 >= args.size()) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }

      auto num_fields = ParseInt<int64_t>(args[i + 1], 10);
      if (!num_fields || *num_fields < 1) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      size_t first_field = i + 2;
      auto field_count = static_cast<size_t>(*num_fields);
      if (field_count > args.size() - first_field) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }

      fields->clear();
      fields->reserve(field_count);
      for (size_t j = 0; j < field_count; j++) {
        fields->emplace_back(args[first_field + j]);
      }
      i = first_field + field_count;
      continue;
    }

    auto condition = ParseHashExpireCondition(args[i]);
    if (!condition) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    if (*condition_out != HashFieldExpireCondition::kNone && *condition_out != *condition) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    *condition_out = *condition;
    i++;
  }

  if (!fields_seen) {
    return {Status::RedisParseErr, errInvalidSyntax};
  }
  return Status::OK();
}

int64_t FormatHashFieldExpireResult(int64_t expire_at, uint64_t now, HashFieldExpireTimeMode time_mode) {
  if (expire_at < 0) {
    return expire_at;
  }

  auto expire = static_cast<uint64_t>(expire_at);
  switch (time_mode) {
    case HashFieldExpireTimeMode::kRelativeSeconds:
      return static_cast<int64_t>(CeilDiv1000(expire > now ? expire - now : 0));
    case HashFieldExpireTimeMode::kRelativeMilliseconds:
      return static_cast<int64_t>(expire > now ? expire - now : 0);
    case HashFieldExpireTimeMode::kAbsoluteSeconds:
      return static_cast<int64_t>(CeilDiv1000(expire));
    case HashFieldExpireTimeMode::kAbsoluteMilliseconds:
      return expire_at;
  }
  return -2;
}

uint64_t GenerateHLenFlags(uint64_t flags, const std::vector<std::string> &args, const Config &config) {
  bool needs_repair = false;
  if (args.size() == 2) {
    needs_repair = config.hash_encoding_mode == HashSubkeyEncodingMode::kFieldExpiration &&
                   config.hash_length_mode == HashLengthMode::kAccurate;
  } else if (args.size() == 3) {
    needs_repair = util::EqualICase(args[2], "REPAIR");
  }

  if (!needs_repair) {
    return flags;
  }

  return (flags | kCmdWrite | kCmdNoDBSizeCheck) & ~kCmdReadOnly;
}

}  // namespace

class CommandHGet : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = hash_db.Get(ctx, args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(value);
    return Status::OK();
  }
};

class CommandHSetNX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MSet(ctx, args_[1], field_values_, true, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
};

class CommandHStrlen : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = hash_db.Get(ctx, args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(static_cast<int>(value.size()));
    return Status::OK();
  }
};

class CommandHDel : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.Delete(ctx, args_[1], fields, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHExists : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = hash_db.Get(ctx, args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(1);
    return Status::OK();
  }
};

class CommandHLen : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 2) {
      length_mode_ = std::nullopt;
      return Commander::Parse(args);
    }
    if (args.size() != 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    if (util::EqualICase(args[2], "APPROX")) {
      length_mode_ = HashLengthMode::kApproximate;
    } else if (util::EqualICase(args[2], "REPAIR")) {
      length_mode_ = HashLengthMode::kAccurate;
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t count = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = length_mode_ ? hash_db.Size(ctx, args_[1], &count, *length_mode_) : hash_db.Size(ctx, args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(count);
    return Status::OK();
  }

 private:
  std::optional<HashLengthMode> length_mode_;
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

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.IncrBy(ctx, args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
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

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    double ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.IncrByFloat(ctx, args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::BulkString(util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandHMGet : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }

    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MGet(ctx, args_[1], fields, &values, &statuses);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      values.resize(fields.size(), "");
      *output = conn->MultiBulkString(values);
    } else {
      *output = conn->MultiBulkString(values, statuses);
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
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MSet(ctx, args_[1], field_values_, false, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (GetAttributes()->name == "hset") {
      *output = redis::Integer(ret);
    } else {
      *output = redis::RESP_OK;
    }
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
};

class CommandHSetExpire : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    ttl_ = GET_OR_RET(ParseInt<uint64_t>(args[2], 10));
    if ((args.size() - 3) % 2 != 0) {
      return {Status::RedisParseErr, "Invalid number of arguments: field-value pairs must be complete"};
    }
    for (size_t i = 3; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MSet(ctx, args_[1], field_values_, false, &ret, ttl_ * 1000 + util::GetTimeStampMS());
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
  uint64_t ttl_ = 0;
};

class CommandHKeys : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.GetAll(ctx, args_[1], &field_values, HashFetchType::kOnlyKey);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> keys;
    keys.reserve(field_values.size());
    for (const auto &fv : field_values) {
      keys.emplace_back(fv.field);
    }
    *output = conn->MultiBulkString(keys);

    return Status::OK();
  }
};

class CommandHVals : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.GetAll(ctx, args_[1], &field_values, HashFetchType::kOnlyValue);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> values;
    values.reserve(field_values.size());
    for (const auto &p : field_values) {
      values.emplace_back(p.value);
    }
    *output = ArrayOfBulkStrings(values);

    return Status::OK();
  }
};

class CommandHGetAll : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.GetAll(ctx, args_[1], &field_values);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> kv_pairs;
    kv_pairs.reserve(field_values.size());
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = conn->MapOfBulkStrings(kv_pairs);

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
    if (spec_.reversed) {
      return ParseRangeLexSpec(args[3], args[2], &spec_);
    } else {
      return ParseRangeLexSpec(args[2], args[3], &spec_);
    }
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    rocksdb::Status s = hash_db.RangeByLex(ctx, args_[1], spec_, &field_values);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    std::vector<std::string> kv_pairs;
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = ArrayOfBulkStrings(kv_pairs);

    return Status::OK();
  }

 private:
  RangeLexSpec spec_;
};

class CommandHScan : public CommandSubkeyScanBase {
 public:
  CommandHScan() = default;
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<std::string> fields;
    std::vector<std::string> values;
    auto key_name = srv->GetKeyNameFromCursor(cursor_, CursorType::kTypeHash);

    auto s = hash_db.Scan(ctx, key_, key_name, limit_, prefix_, &fields, no_values_ ? nullptr : &values);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    auto cursor = GetNextCursor(srv, fields, CursorType::kTypeHash);
    std::vector<std::string> entries;
    if (no_values_) {
      entries.reserve(fields.size());
    } else {
      entries.reserve(2 * fields.size());
    }
    for (size_t i = 0; i < fields.size(); i++) {
      entries.emplace_back(redis::BulkString(fields[i]));
      if (!no_values_) {
        entries.emplace_back(redis::BulkString(values[i]));
      }
    }
    *output = redis::Array({redis::BulkString(cursor), redis::Array(entries)});
    return Status::OK();
  }
};

class CommandHRandField : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() >= 3) {
      no_parameters_ = false;
      auto parse_result = ParseInt<int64_t>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      command_count_ = *parse_result;

      if (args.size() > 4 || (args.size() == 4 && !util::EqualICase(args[3], "withvalues"))) {
        return {Status::RedisParseErr, errInvalidSyntax};
      } else if (args.size() == 4) {
        withvalues_ = true;
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.RandField(ctx, args_[1], command_count_, &field_values,
                               withvalues_ ? HashFetchType::kAll : HashFetchType::kOnlyKey);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> result_entries;
    result_entries.reserve(field_values.size());
    for (const auto &p : field_values) {
      result_entries.emplace_back(p.field);
      if (withvalues_) result_entries.emplace_back(p.value);
    }

    if (no_parameters_)
      *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(result_entries[0]);
    else
      *output = ArrayOfBulkStrings(result_entries);
    return Status::OK();
  }

 private:
  bool withvalues_ = false;
  int64_t command_count_ = 1;
  bool no_parameters_ = true;
};

template <HashFieldExpireTimeMode kTimeMode>
class CommandHExpireGeneric : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    GET_OR_RET(ParseHashFieldExpireArgument(args[2], &expire_arg_));

    GET_OR_RET(ParseHashExpireFields(args, 3, &condition_, &fields_));
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t now = util::GetTimeStampMS();
    uint64_t expire_at = 0;
    GET_OR_RET(ConvertHashFieldExpireAtMs(expire_arg_, kTimeMode, now, &expire_at));

    std::vector<int64_t> results;
    std::vector<Slice> fields = ToSlices(fields_);
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    auto s = hash_db.ExpireFields(ctx, args_[1], fields, expire_at, condition_, &results, now);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = IntegerArray(results);
    return Status::OK();
  }

 private:
  int64_t expire_arg_ = 0;
  HashFieldExpireCondition condition_ = HashFieldExpireCondition::kNone;
  std::vector<std::string> fields_;
};

class CommandHPersist : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    GET_OR_RET(ParseHashFixedFields(args, &fields_));
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<int64_t> results;
    std::vector<Slice> fields = ToSlices(fields_);
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    auto s = hash_db.PersistFields(ctx, args_[1], fields, &results);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = IntegerArray(results);
    return Status::OK();
  }

 private:
  std::vector<std::string> fields_;
};

template <HashFieldExpireTimeMode kTimeMode>
class CommandHExpireInfo : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    GET_OR_RET(ParseHashFixedFields(args, &fields_));
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t now = util::GetTimeStampMS();
    std::vector<int64_t> results;
    std::vector<Slice> fields = ToSlices(fields_);
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    auto s = hash_db.GetFieldsExpireTime(ctx, args_[1], fields, &results, now);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    for (auto &result : results) {
      result = FormatHashFieldExpireResult(result, now, kTimeMode);
    }
    *output = IntegerArray(results);
    return Status::OK();
  }

 private:
  std::vector<std::string> fields_;
};

REDIS_REGISTER_COMMANDS(
    Hash, MakeCmdAttr<CommandHGet>("hget", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHIncrBy>("hincrby", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandHIncrByFloat>("hincrbyfloat", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandHMSet>("hset", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandHSetExpire>("hsetexpire", -5, "write", 1, 1, 1),
    MakeCmdAttr<CommandHSetNX>("hsetnx", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandHDel>("hdel", -3, "write no-dbsize-check", 1, 1, 1),
    MakeCmdAttr<CommandHStrlen>("hstrlen", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHExists>("hexists", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHLen>("hlen", -2, "read-only", 1, 1, 1, GenerateHLenFlags),
    MakeCmdAttr<CommandHMGet>("hmget", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHMSet>("hmset", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandHKeys>("hkeys", 2, "read-only slow", 1, 1, 1),
    MakeCmdAttr<CommandHVals>("hvals", 2, "read-only slow", 1, 1, 1),
    MakeCmdAttr<CommandHGetAll>("hgetall", 2, "read-only slow", 1, 1, 1),
    MakeCmdAttr<CommandHScan>("hscan", -3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHRangeByLex>("hrangebylex", -4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHRandField>("hrandfield", -2, "read-only slow", 1, 1, 1),
    MakeCmdAttr<CommandHExpireGeneric<HashFieldExpireTimeMode::kRelativeSeconds>>("hexpire", -6, "write", 1, 1, 1),
    MakeCmdAttr<CommandHExpireGeneric<HashFieldExpireTimeMode::kRelativeMilliseconds>>("hpexpire", -6, "write", 1, 1,
                                                                                       1),
    MakeCmdAttr<CommandHExpireGeneric<HashFieldExpireTimeMode::kAbsoluteSeconds>>("hexpireat", -6, "write", 1, 1, 1),
    MakeCmdAttr<CommandHExpireGeneric<HashFieldExpireTimeMode::kAbsoluteMilliseconds>>("hpexpireat", -6, "write", 1, 1,
                                                                                       1),
    MakeCmdAttr<CommandHPersist>("hpersist", -5, "write", 1, 1, 1),
    MakeCmdAttr<CommandHExpireInfo<HashFieldExpireTimeMode::kRelativeSeconds>>("httl", -5, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHExpireInfo<HashFieldExpireTimeMode::kRelativeMilliseconds>>("hpttl", -5, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHExpireInfo<HashFieldExpireTimeMode::kAbsoluteSeconds>>("hexpiretime", -5, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandHExpireInfo<HashFieldExpireTimeMode::kAbsoluteMilliseconds>>("hpexpiretime", -5, "read-only", 1,
                                                                                    1, 1), )

}  // namespace redis
