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
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_bloomfilter.h"

namespace redis {

class CommandBFReserve : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_error_rate = ParseFloat<double>(args[2]);
    if (!parse_error_rate) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    error_rate_ = *parse_error_rate;
    if (error_rate_ >= 1 || error_rate_ <= 0) {
      return {Status::RedisParseErr, "0 < error rate range < 1"};
    }

    auto parse_capacity = ParseInt<uint64_t>(args[3], 10);
    if (!parse_capacity) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    capacity_ = *parse_capacity;
    if (capacity_ <= 0) {
      return {Status::RedisParseErr, "capacity should be larger than 0"};
    }

    if (args.size() == 5) {
      if (args[4] == "NONSCALING") {
        scaling_ = 0;
      } else if (args[4] == "EXPANSION") {
        return {Status::RedisParseErr, "no expansion"};
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    if (args.size() == 6) {
      if (args[4] == "EXPANSION") {
        auto parse_expansion = ParseInt<uint16_t>(args[5], 10);
        if (!parse_expansion) {
          return {Status::RedisParseErr, errValueNotInteger};
        }
        expansion_ = *parse_expansion;
        if (expansion_ < 1) {
          return {Status::RedisParseErr, "expansion should be greater or equal to 1"};
        }
      }
    }

    if (args.size() > 6) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::BloomFilter bloomfilter_db(svr->storage, conn->GetNamespace());
    auto s = bloomfilter_db.Reserve(args_[1], error_rate_, capacity_, expansion_, scaling_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  double error_rate_;
  uint64_t capacity_;
  uint16_t expansion_ = 2;
  uint16_t scaling_ = 1;
};

class CommandBFADD : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::BloomFilter bloom_db(svr->storage, conn->GetNamespace());
    int ret = 0;
    auto s = bloom_db.Add(args_[1], args_[2], ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandBFMADD : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args_.size(); ++i) {
      items_.emplace_back(args_[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::BloomFilter bloom_db(svr->storage, conn->GetNamespace());
    std::vector<int> rets(items_.size(), 0);
    auto s = bloom_db.MAdd(args_[1], items_, rets);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiInteger(rets);
    return Status::OK();
  }

 private:
  std::vector<Slice> items_;
};

class CommandBFEXIST : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::BloomFilter bloom_db(svr->storage, conn->GetNamespace());
    int ret = 0;
    auto s = bloom_db.Exist(args_[1], args_[2], ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandBFMEXIST : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args_.size(); ++i) {
      items_.emplace_back(args_[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::BloomFilter bloom_db(svr->storage, conn->GetNamespace());
    std::vector<int> rets(items_.size(), 0);
    auto s = bloom_db.MExist(args_[1], items_, rets);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiInteger(rets);
    return Status::OK();
  }

 private:
  std::vector<Slice> items_;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandBFReserve>("bf.reserve", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFADD>("bf.add", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFMADD>("bf.madd", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFEXIST>("bf.exist", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBFMEXIST>("bf.mexist", -3, "read-only", 1, 1, 1), )

}  // namespace redis