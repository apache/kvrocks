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
      return Status::RedisTryAgain;
    }
    redis::CMS cms(srv->storage, conn->GetNamespace());
    rocksdb::Status s;
    std::unordered_map<std::string, uint64_t> elements;
    for (size_t i = 2; i < args_.size(); i += 2) {
      std::string key = args_[i];
      uint64_t value = 0;
      try {
        value = std::stoull(args_[i + 1]);
      } catch (const std::exception &e) {
        return Status::InvalidArgument;
      }
      elements[key] = value;
    }

    s = cms.IncrBy(args_[1], elements);
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
    rocksdb::Status s;
    std::unordered_map<std::string, uint64_t> elements;
    std::vector<uint64_t> ret{};

    s = cms.Info(args_[1], &ret);

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Array({redis::BulkString("width"), redis::Integer(ret[0]), redis::BulkString("depth"),
                            redis::Integer(ret[1]), redis::BulkString("count"), redis::Integer(ret[2])});

    return Status::OK();
  }
};

/// CMS.INITBYDIM key width depth
class CommandCMSInitByDim final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    rocksdb::Status s;
    uint64_t width = std::stoull(args_[2]);
    uint64_t depth = std::stoull(args_[3]);

    s = cms.InitByDim(args_[1], width, depth);
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
    rocksdb::Status s;
    double error = std::stod(args_[2]);
    double delta = std::stod(args_[3]);

    s = cms.InitByProb(args_[1], error, delta);
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
    rocksdb::Status s;

    std::vector<uint32_t> counters{};
    std::vector<std::string> elements;

    for (size_t i = 2; i < args_.size(); ++i) {
      elements.emplace_back(args_[i]);
    }

    s = cms.Query(args_[1], elements, counters);

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