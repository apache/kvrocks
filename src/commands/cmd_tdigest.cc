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

#include <range/v3/range/conversion.hpp>
#include <range/v3/view/transform.hpp>

#include "command_parser.h"
#include "commander.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "status.h"
#include "string_util.h"
#include "types/redis_tdigest.h"

namespace redis {
namespace {
constexpr auto kCompressionArg = "compression";

constexpr auto kInfoCompression = "Compression";
constexpr auto kInfoCapacity = "Capacity";
constexpr auto kInfoMergedNodes = "Merged nodes";
constexpr auto kInfoUnmergedNodes = "Unmerged nodes";
constexpr auto kInfoMergedWeight = "Merged weight";
constexpr auto kInfoUnmergedWeight = "Unmerged weight";
constexpr auto kInfoObservations = "Observations";
constexpr auto kInfoTotalCompressions = "Total compressions";
constexpr auto kNan = "nan";
}  // namespace

class CommandTDigestCreate : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    key_name_ = GET_OR_RET(parser.TakeStr());
    options_.compression = 100;
    if (parser.EatEqICase(kCompressionArg)) {
      if (!parser.Good()) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }
      auto status = parser.TakeInt<int32_t>();
      if (!status) {
        return {Status::RedisParseErr, errParseCompression};
      }
      auto compression = *status;
      if (compression <= 0) {
        return {Status::RedisParseErr, errCompressionMustBePositive};
      }
      if (compression < 1 || compression > static_cast<int32_t>(kTDigestMaxCompression)) {
        return {Status::RedisParseErr, errCompressionOutOfRange};
      }
      options_.compression = static_cast<uint32_t>(compression);
    }
    if (parser.Good()) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    bool exists = false;
    auto s = tdigest.Create(ctx, key_name_, options_, &exists);
    if (!s.ok()) {
      if (exists) {
        return {Status::RedisExecErr, errKeyAlreadyExists};
      }
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  std::string key_name_;
  TDigestCreateOptions options_;
};

class CommandTDigestInfo : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    key_name_ = args[1];
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    TDigestMetadata metadata;
    auto s = tdigest.GetMetaData(ctx, key_name_, &metadata);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return {Status::RedisExecErr, errKeyNotFound};
      }
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(conn->HeaderOfMap(8));
    output->append(redis::BulkString(kInfoCompression));
    output->append(redis::Integer(metadata.compression));
    output->append(redis::BulkString(kInfoCapacity));
    output->append(redis::Integer(metadata.capacity));
    output->append(redis::BulkString(kInfoMergedNodes));
    output->append(redis::Integer(metadata.merged_nodes));
    output->append(redis::BulkString(kInfoUnmergedNodes));
    output->append(redis::Integer(metadata.unmerged_nodes));
    output->append(redis::BulkString(kInfoMergedWeight));
    output->append(redis::Integer(metadata.merged_weight));
    output->append(redis::BulkString(kInfoUnmergedWeight));
    output->append(redis::Integer(metadata.total_weight - metadata.merged_weight));
    output->append(redis::BulkString(kInfoObservations));
    output->append(redis::Integer(metadata.total_observations));
    output->append(redis::BulkString(kInfoTotalCompressions));
    output->append(redis::Integer(metadata.merge_times));
    // "Memory usage" is not meaningful for kvrocks, so we don't provide it here.
    return Status::OK();
  }

 private:
  std::string key_name_;
};

class CommandTDigestAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    key_name_ = args[1];

    values_.reserve(args.size() - 2);
    for (size_t i = 2; i < args.size(); i++) {
      auto value = ParseFloat(args[i]);
      if (!value) {
        return {Status::RedisParseErr, errValueIsNotFloat};
      }
      values_.push_back(*value);
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());

    auto s = tdigest.Add(ctx, key_name_, values_);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return {Status::RedisExecErr, errKeyNotFound};
      }
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  std::string key_name_;
  std::vector<double> values_;
};

class CommandTDigestMinMax : public Commander {
 public:
  explicit CommandTDigestMinMax(bool is_min) : is_min_(is_min) {}

  Status Parse(const std::vector<std::string> &args) override {
    key_name_ = args[1];
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    TDigestMetadata metadata;
    auto s = tdigest.GetMetaData(ctx, key_name_, &metadata);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return {Status::RedisExecErr, errKeyNotFound};
      }
      return {Status::RedisExecErr, s.ToString()};
    }

    if (metadata.total_observations == 0) {
      *output = redis::BulkString("nan");
      return Status::OK();
    }

    double value = is_min_ ? metadata.minimum : metadata.maximum;
    *output = redis::BulkString(fmt::format("{}", value));
    return Status::OK();
  }

 private:
  std::string key_name_;
  bool is_min_;
};
class CommandTDigestReset : public Commander {
  Status Parse(const std::vector<std::string> &args) override {
    key_name_ = args[1];
    return Status::OK();
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    TDigestMetadata metadata;
    auto s = tdigest.GetMetaData(ctx, key_name_, &metadata);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return {Status::RedisExecErr, errKeyNotFound};
      }
      return {Status::RedisExecErr, s.ToString()};
    }
    if (metadata.total_observations == 0) {
      *output = redis::RESP_OK;
      return Status::OK();
    }
    s = tdigest.Reset(ctx, key_name_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  std::string key_name_;
};
// Then replace the existing template implementation and type aliases with:
class CommandTDigestMin : public CommandTDigestMinMax {
 public:
  CommandTDigestMin() : CommandTDigestMinMax(true) {}
};

class CommandTDigestMax : public CommandTDigestMinMax {
 public:
  CommandTDigestMax() : CommandTDigestMinMax(false) {}
};
class CommandTDigestQuantile : public Commander {
  Status Parse(const std::vector<std::string> &args) override {
    key_name_ = args[1];
    quantiles_.reserve(args.size() - 2);
    for (size_t i = 2; i < args.size(); i++) {
      auto value = ParseFloat(args[i]);
      if (!value) {
        return {Status::RedisParseErr, errValueIsNotFloat};
      }
      quantiles_.push_back(*value);
    }
    return Status::OK();
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    TDigestQuantitleResult result;
    auto s = tdigest.Quantile(ctx, key_name_, quantiles_, &result);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return {Status::RedisExecErr, errKeyNotFound};
      }
      return {Status::RedisExecErr, s.ToString()};
    }
    auto quantile_strings = result.quantiles
                                ? (ranges::views::transform(*result.quantiles, util::Float2String) | ranges::to_vector)
                                : std::vector<std::string>(quantiles_.size(), kNan);
    *output = conn->MultiBulkString(quantile_strings);
    return Status::OK();
  }

 private:
  std::string key_name_;
  std::vector<double> quantiles_;
};
REDIS_REGISTER_COMMANDS(TDigest, MakeCmdAttr<CommandTDigestCreate>("tdigest.create", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTDigestInfo>("tdigest.info", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTDigestAdd>("tdigest.add", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTDigestMax>("tdigest.max", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTDigestMin>("tdigest.min", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTDigestQuantile>("tdigest.quantile", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandTDigestReset>("tdigest.reset", 2, "write", 1, 1, 1));
}  // namespace redis
