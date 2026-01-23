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
#include "commands/command_parser.h"
#include "error_constants.h"
#include "scan_base.h"
#include "server/server.h"
#include "string_util.h"
#include "time_util.h"
#include "types/redis_hash.h"

namespace redis {

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
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t count = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.Size(ctx, args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(count);
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

auto ParseFieldsArgs = [](const std::vector<std::string> &args, size_t fields_keyword_index,
                          std::vector<Slice> &fields) -> Status {
  if (!util::EqualICase(args[fields_keyword_index], "FIELDS")) {
    return {Status::RedisParseErr, "mandatory argument FIELDS is missing or not in the right position"};
  }

  auto num_fields_result = ParseInt<int64_t>(args[fields_keyword_index + 1], 10);
  if (!num_fields_result) {
    return {Status::RedisParseErr, errValueNotInteger};
  }
  if (*num_fields_result <= 0) {
    return {Status::RedisParseErr, "numfields must be a positive integer"};
  }
  auto num_fields = static_cast<size_t>(*num_fields_result);

  // Check we have the right number of fields
  if (args.size() != fields_keyword_index + 2 + num_fields) {
    return {Status::RedisParseErr, "number of fields does not match numfields"};
  }

  for (size_t i = fields_keyword_index + 2; i < args.size(); i++) {
    fields.emplace_back(args[i]);
  }

  return Status::OK();
};

class CommandHExpire : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // HEXPIRE key seconds FIELDS numfields field [field ...]
    // HPEXPIRE key ms FIELDS numfields field [field ...]
    // Minimum: HEXPIRE key seconds FIELDS 1 field = 6 args
    if (args.size() < 6) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    auto input_time = ParseInt<int64_t>(args[2], 10);
    if (!input_time) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    if (*input_time < 0) {
      return {Status::RedisParseErr, "invalid time, must be >= 0"};
    }
    if (util::EqualICase(args[0], "hexpire")) {
      expire_ms_ = static_cast<uint64_t>(*input_time) * 1000 + util::GetTimeStampMS();
    } else if (util::EqualICase(args[0], "hpexpire")) {
      expire_ms_ = static_cast<uint64_t>(*input_time) + util::GetTimeStampMS();
    } else if (util::EqualICase(args[0], "hexpireat")) {
      expire_ms_ = static_cast<uint64_t>(*input_time) * 1000;
    } else if (util::EqualICase(args[0], "hpexpireat")) {
      expire_ms_ = static_cast<uint64_t>(*input_time);
    }

    if (util::EqualICase(args[3], "FIELDS")) {
      GET_OR_RET(ParseFieldsArgs(args, 3, fields_));
      return Commander::Parse(args);
    }

    if (util::EqualICase(args[3], "NX")) {
      condition_ = FieldExpireCondition::kFieldExpireTimeNotExists;
    } else if (util::EqualICase(args[3], "XX")) {
      condition_ = FieldExpireCondition::kFieldExpireTimeExists;
    } else if (util::EqualICase(args[3], "GT")) {
      condition_ = FieldExpireCondition::kFieldExpireTimeGreaterThanInput;
    } else if (util::EqualICase(args[3], "LT")) {
      condition_ = FieldExpireCondition::kFieldExpireTimeLessThanInput;
    } else {
      return {Status::RedisParseErr, "expect argument FIELDS or [NX|XX|GT|LT] is missing or not in the right position"};
    }

    if (util::EqualICase(args[4], "FIELDS")) {
      GET_OR_RET(ParseFieldsArgs(args, 4, fields_));
      return Commander::Parse(args);
    }
    return {Status::RedisParseErr, "mandatory argument FIELDS is missing or not in the right position"};
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldExpireResult> results;

    auto s = hash_db.ExpireFields(ctx, args_[1], expire_ms_, fields_, &results, condition_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    // Return array of results
    std::vector<std::string> result_strings;
    result_strings.reserve(results.size());
    for (const auto &r : results) {
      result_strings.emplace_back(redis::Integer(static_cast<int64_t>(r)));
    }
    *output = redis::Array(result_strings);
    return Status::OK();
  }

 private:
  uint64_t expire_ms_ = 0;
  std::vector<Slice> fields_;
  FieldExpireCondition condition_ = FieldExpireCondition::kFieldNoExpireCondition;
};

class CommandHPersist : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // HPERSIST key FIELDS numfields field [field ...]
    // Minimum: HPERSIST key FIELDS 1 field = 5 args
    if (args.size() < 5) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    GET_OR_RET(ParseFieldsArgs(args, 2, fields_));
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldPersistResult> results;

    auto s = hash_db.PersistFields(ctx, args_[1], fields_, &results);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    // Return array of results
    std::vector<std::string> result_strings;
    result_strings.reserve(results.size());
    for (const auto &r : results) {
      result_strings.emplace_back(redis::Integer(static_cast<int64_t>(r)));
    }
    *output = redis::Array(result_strings);
    return Status::OK();
  }

 private:
  std::vector<Slice> fields_;
};

class CommandHTTL : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // HTTL key FIELDS numfields field [field ...]
    // Minimum: HTTL key FIELDS 1 field = 5 args
    if (args.size() < 5) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    GET_OR_RET(ParseFieldsArgs(args, 2, fields_));
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<int64_t> results;

    auto s = hash_db.TTLFields(ctx, args_[1], fields_, &results);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    auto current_time_ms = static_cast<int64_t>(util::GetTimeStampMS());

    // httl returns time in seconds, hpttl returns time in milliseconds as TTLFields
    // hexpiretime returns expire_time in seconds, hpexpiretime returns expire_time in milliseconds as TTLFields already
    // does
    for (auto &r : results) {
      if (r > 0) {
        if (util::EqualICase(args_[0], "httl")) {
          r = ((r - current_time_ms) / 1000);
        } else if (util::EqualICase(args_[0], "hpttl")) {
          r = (r - current_time_ms);
        } else if (util::EqualICase(args_[0], "hexpiretime")) {
          r = r / 1000;
        } else if (util::EqualICase(args_[0], "hpexpiretime")) {
          // do nothing as TTLFields already returns expire_time in milliseconds
        }
      }
    }
    std::vector<std::string> result_strings;
    result_strings.reserve(results.size());
    for (const auto &r : results) {
      result_strings.emplace_back(redis::Integer(r));
    }
    *output = redis::Array(result_strings);
    return Status::OK();
  }

 private:
  std::vector<Slice> fields_;
};

class CommandHMSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 7) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    size_t pos = 2;

    // Lambda to parse expiration value with validation
    auto parse_expire_value = [](const std::string &value_str) -> uint64_t {
      auto result = ParseInt<uint64_t>(value_str, 10);
      if (!result || *result <= 0) {
        return 0;
      }
      return result.GetValue();
    };

    if (pos < args.size() && util::EqualICase(args[pos], std::string_view("FIELDS"))) {
      return {Status::RedisParseErr, "ERR Missing expiration option"};
    }

    // Parse expiration option - optional
    params_.expire_params.option = SetEXExpireOption::kNoExpire;
    while (pos < args.size()) {
      const auto &opt = args[pos];
      if (util::EqualICase(opt, "KEEPTTL")) {
        params_.expire_params.option = SetEXExpireOption::kKEEPTTL;
        pos++;
      } else if (util::EqualICase(opt, "FNX")) {
        params_.condition = SetEXFieldCondition::kFNX;
        pos++;
      } else if (util::EqualICase(opt, "FXX")) {
        params_.condition = SetEXFieldCondition::kFXX;
        pos++;
      } else if (util::EqualICase(opt, "FIELDS")) {
        if (params_.expire_params.option == SetEXExpireOption::kNoExpire) {
          return {Status::RedisParseErr, "Invalid syntax: at least one expiration option is required before FIELDS"};
        } else {
          // FIELDS is a special case and should not be treated as an expiration option
          break;
        }
      } else {
        // got next must be a integer
        auto value = parse_expire_value(args[pos + 1]);
        params_.expire_params.value = value;
        if (value == 0) {
          return {Status::RedisParseErr, "Invalid expire time value"};
        }
        if (util::EqualICase(opt, "EX")) {
          params_.expire_params.option = SetEXExpireOption::kEX;
        } else if (util::EqualICase(opt, "PX")) {
          params_.expire_params.option = SetEXExpireOption::kPX;
        } else if (util::EqualICase(opt, "EXAT")) {
          params_.expire_params.option = SetEXExpireOption::kEXAT;
        } else if (util::EqualICase(opt, "PXAT")) {
          params_.expire_params.option = SetEXExpireOption::kPXAT;
        } else {
          return {Status::RedisParseErr, "Invalid syntax: expected EX, PX, EXAT, PXAT, KEEPTTL, FNX or FXX"};
        }
        pos += 2;
      }
    }
    // Parse FIELDS and field-value pairs
    if (pos >= args.size() || !util::EqualICase(args[pos], "FIELDS")) {
      return {Status::RedisParseErr, "mandatory argument FIELDS is missing"};
    }
    pos++;
    if (pos >= args.size()) {
      return {Status::RedisParseErr, "FIELDS requires numfields argument"};
    }

    auto num_fields_result = ParseInt<uint64_t>(args[pos], 10);
    if (!num_fields_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    if (*num_fields_result <= 0) {
      return {Status::RedisParseErr, "numfields must be a positive integer"};
    }
    auto num_fields = *num_fields_result;
    pos++;

    // Parse field-value pairs
    if (args.size() != pos + 2 * num_fields) {
      return {Status::RedisParseErr, "number of field-value pairs does not match numfields"};
    }

    for (size_t i = 0; i < num_fields; i++) {
      field_values_.emplace_back(args[pos], args[pos + 1]);
      pos += 2;
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MSetEx(ctx, args_[1], field_values_, params_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    if (ret == 0) {
      *output = redis::Integer(0);
    } else {
      *output = redis::Integer(1);
    }
    return Status::OK();
  }

 private:
  HSetExParams params_;
  std::vector<FieldValue> field_values_;
};

REDIS_REGISTER_COMMANDS(Hash, MakeCmdAttr<CommandHGet>("hget", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHIncrBy>("hincrby", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHIncrByFloat>("hincrbyfloat", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHMSet>("hset", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHSetExpire>("hsetexpire", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHSetNX>("hsetnx", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHDel>("hdel", -3, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandHStrlen>("hstrlen", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHExists>("hexists", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHLen>("hlen", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHMGet>("hmget", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHMSet>("hmset", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHKeys>("hkeys", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHVals>("hvals", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHGetAll>("hgetall", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHScan>("hscan", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHRangeByLex>("hrangebylex", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHRandField>("hrandfield", -2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHExpire>("hexpire", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHExpire>("hpexpire", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHExpire>("hexpireat", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHExpire>("hpexpireat", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHTTL>("httl", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHTTL>("hpttl", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHTTL>("hexpiretime", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHTTL>("hpexpiretime", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHPersist>("hpersist", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHMSetEX>("hsetex", -7, "write", 1, 1, 1), )

}  // namespace redis
