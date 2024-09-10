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

#include <types/cms.h>
#include <types/redis_cms.h>

#include "parse_util.h"
#include "commander.h"
#include "commands/command_parser.h"
#include "server/redis_reply.h"
#include "server/server.h"

namespace redis {

/// CMS.INCRBY key item increment [item increment ...]
class CommandCMSIncrBy final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if ((args_.size() - 2) % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);
    rocksdb::Status s;
    std::unordered_map<std::string, uint64_t> elements;
    for (size_t i = 2; i < args_.size(); i += 2) {
      std::string key = args_[i];
      auto parse_result = ParseInt<uint64_t>(args_[i + 1]);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      uint64_t value = *parse_result;
      elements[key] = value;
    }

    s = cms.IncrBy(ctx, args_[1], elements);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

/// CMS.INFO key
class CommandCMSInfo final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);
    rocksdb::Status s;
    CMSketch::CMSInfo ret{};

    s = cms.Info(ctx, args_[1], &ret);

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Array({
      redis::BulkString("width"), redis::Integer(ret.width), 
      redis::BulkString("depth"), redis::Integer(ret.depth), 
      redis::BulkString("count"), redis::Integer(ret.count)
    });

    return Status::OK();
  }
};

/// CMS.INITBYDIM key width depth
class CommandCMSInitByDim final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);
    rocksdb::Status s;
    auto width_result = ParseInt<uint32_t>(this->args_[2]);
    if (!width_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    uint32_t width = *width_result;

    auto depth_result = ParseInt<uint32_t>(this->args_[3]);
    if (!depth_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    uint32_t depth = *depth_result; 

    s = cms.InitByDim(ctx, args_[1], width, depth);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

/// CMS.INITBYPROB key error probability
class CommandCMSInitByProb final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);
    rocksdb::Status s;

    auto error_result = ParseFloat<double>(args_[2]);
    if (!error_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    double error = *error_result; 

    auto delta_result = ParseFloat<double>(args_[3]);
    if (!delta_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    double delta = *delta_result;

    s = cms.InitByProb(ctx, args_[1], error, delta);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

/// CMS.QUERY key item [item ...]
class CommandCMSQuery final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);
    rocksdb::Status s;

    std::vector<uint32_t> counters{};
    std::vector<std::string> elements;

    for (size_t i = 2; i < args_.size(); ++i) {
      elements.emplace_back(args_[i]);
    }

    s = cms.Query(ctx, args_[1], elements, counters);

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> output_values;
    output_values.reserve(counters.size());
    for (const auto &counter : counters) {
      output_values.push_back(std::to_string(counter));
    }

    *output = redis::ArrayOfBulkStrings(output_values);

    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(CMS, MakeCmdAttr<CommandCMSIncrBy>("cms.incrby", -4, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCMSInfo>("cms.info", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandCMSInitByDim>("cms.initbydim", 4, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCMSInitByProb>("cms.initbyprob", 4, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCMSQuery>("cms.query", -3, "read-only", 0, 0, 0), );
}  // namespace redis