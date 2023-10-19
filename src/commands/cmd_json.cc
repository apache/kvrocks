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
#include "server/redis_reply.h"
#include "server/server.h"
#include "types/redis_json.h"

namespace redis {

class CommandJsonSet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    auto s = json.Set(args_[1], args_[2], args_[3]);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandJsonGet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    JsonValue result;
    auto s = json.Get(args_[1], {args_.begin() + 2, args_.end()}, &result);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::BulkString(result.Dump());
    return Status::OK();
  }
};

class CommandJsonArrAppend : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Json json(svr->storage, conn->GetNamespace());

    std::vector<uint64_t> result_count;

    auto s = json.ArrAppend(args_[1], args_[2], {args_.begin() + 3, args_.end()}, result_count);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    if(result_count.empty()) {
      result_count.emplace_back(0);
    }

    std::vector<std::string> result_values;
    result_values.reserve(result_count.size());
    for(auto c : result_count) {
      if(c != 0) {
        result_values.emplace_back("(integer) " + std::to_string(c));
      } else {
        result_values.emplace_back();
      }
    }

    *output = redis::MultiBulkString(result_values, true);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandJsonSet>("json.set", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandJsonGet>("json.get", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandJsonArrAppend>("json.arrappend", -4, "write", 1, 1, 1), );

}  // namespace redis
