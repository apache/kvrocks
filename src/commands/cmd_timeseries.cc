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

}  // namespace

namespace redis {
class CommandTSOptions : public Commander {
 protected:
  Status parseOptions(const std::vector<std::string> &args, size_t skip_num) {
    CommandParser parser(args, skip_num);
    while (parser.Good()) {
      if (parser.EatEqICase("RETENTION")) {
        auto parse_retention = parser.TakeInt<uint64_t>();
        if (!parse_retention.IsOK()) {
          return {Status::RedisParseErr, errBadRetention};
        }
        metadata_.retention_time = parse_retention.GetValue();
      } else if (parser.EatEqICase("CHUNK_SIZE")) {
        // TODO: should limit chunk_size to a reasonable range
        auto parse_chunk_size = parser.TakeInt<uint64_t>();
        if (!parse_chunk_size.IsOK()) {
          return {Status::RedisParseErr, errBadChunkSize};
        }
        metadata_.chunk_size = parse_chunk_size.GetValue();
      } else if (parser.EatEqICase("ENCODING")) {
        using ChunkType = TimeSeriesMetadata::ChunkType;
        if (parser.EatEqICase("UNCOMPRESSED")) {
          metadata_.chunk_type = ChunkType::UNCOMPRESSED;
        } else if (parser.EatEqICase("COMPRESSED")) {
          metadata_.chunk_type = ChunkType::COMPRESSED;
        } else {
          return {Status::RedisParseErr, errBadEncoding};
        }
      } else if (parser.EatEqICase("DUPLICATE_POLICY")) {
        using DuplicatePolicy = TimeSeriesMetadata::DuplicatePolicy;
        if (parser.EatEqICase("BLOCK")) {
          metadata_.duplicate_policy = DuplicatePolicy::BLOCK;
        } else if (parser.EatEqICase("FIRST")) {
          metadata_.duplicate_policy = DuplicatePolicy::FIRST;
        } else if (parser.EatEqICase("LAST")) {
          metadata_.duplicate_policy = DuplicatePolicy::LAST;
        } else if (parser.EatEqICase("MAX")) {
          metadata_.duplicate_policy = DuplicatePolicy::MAX;
        } else if (parser.EatEqICase("MIN")) {
          metadata_.duplicate_policy = DuplicatePolicy::MIN;
        } else if (parser.EatEqICase("SUM")) {
          metadata_.duplicate_policy = DuplicatePolicy::SUM;
        } else {
          return {Status::RedisParseErr, errDuplicatePolicy};
        }
      } else if (parser.EatEqICase("LABELS")) {
        while (parser.Good()) {
          auto parse_key = parser.TakeStr();
          auto parse_value = parser.TakeStr();
          if (!parse_key.IsOK() || !parse_value.IsOK()) {
            break;
          }
          labels_.push_back({parse_key.GetValue(), parse_value.GetValue()});
        }
      } else {
        parser.Skip(1);
      }
    }
    return Commander::Parse(args);
  }
  const TimeSeriesMetadata &getMetadata() { return metadata_; }
  const std::vector<LabelKVPair> &getLabels() { return labels_; }

 private:
  TimeSeriesMetadata metadata_;
  LabelKVList labels_;
};

class CommandTSCreate : public CommandTSOptions {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 2) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    return CommandTSOptions::parseOptions(args, 2);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto timeseries_db = TimeSeries(srv->storage, conn->GetNamespace());
    auto s = timeseries_db.Create(ctx, args_[1], getMetadata(), getLabels());
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

// class CommandTSMAdd : public Commander {
//  public:
//   Status Parse(const std::vector<std::string> &args) override {}
//   Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {}
// };

REDIS_REGISTER_COMMANDS(Timeseries, MakeCmdAttr<CommandTSCreate>("ts.create", -2, "write", 1, 1, 1), );

}  // namespace redis
