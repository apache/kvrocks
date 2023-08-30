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
#include "types/redis_bloom_chain.h"

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
      return {Status::RedisParseErr, "error rate should be between 0 and 1"};
    }

    auto parse_capacity = ParseInt<uint32_t>(args[3], 10);
    if (!parse_capacity) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    capacity_ = *parse_capacity;
    if (capacity_ <= 0) {
      return {Status::RedisParseErr, "capacity should be larger than 0"};
    }

    CommandParser parser(args, 4);
    bool is_nonscaling = false;
    bool has_expansion = false;
    while (parser.Good()) {
      if (parser.EatEqICase("nonscaling")) {
        is_nonscaling = true;
      } else if (parser.EatEqICase("expansion")) {
        has_expansion = true;
        expansion_ = GET_OR_RET(parser.TakeInt<uint16_t>());
        if (expansion_ < 1) {
          return {Status::RedisParseErr, "expansion should be greater or equal to 1"};
        }
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    if (is_nonscaling && has_expansion) {
      return {Status::RedisParseErr, "nonscaling filters cannot expand"};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::BloomChain bloomfilter_db(svr->storage, conn->GetNamespace());
    auto s = bloomfilter_db.Reserve(args_[1], capacity_, error_rate_, expansion_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  double error_rate_;
  uint32_t capacity_;
  uint16_t expansion_ = kBFDefaultExpansion;
};

class CommandBFAdd : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(svr->storage, conn->GetNamespace());
    int ret = 0;
    auto s = bloom_db.Add(args_[1], args_[2], &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandBFExists : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(svr->storage, conn->GetNamespace());
    int ret = 0;
    auto s = bloom_db.Exist(args_[1], args_[2], &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandBFInfo : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    while (parser.Good()) {
      if (parser.EatEqICase("capacity")) {
        type_ = BloomInfoType::CAPACITY;
      } else if (parser.EatEqICase("size")) {
        type_ = BloomInfoType::SIZE;
      } else if (parser.EatEqICase("filters")) {
        type_ = BloomInfoType::FILTERS;
      } else if (parser.EatEqICase("items")) {
        type_ = BloomInfoType::ITEMS;
      } else if (parser.EatEqICase("expansion")) {
        type_ = BloomInfoType::EXPANSION;
      } else {
        return {Status::RedisParseErr, "Invalid info argument"};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(svr->storage, conn->GetNamespace());
    BloomFilterInfo info;
    auto s = bloom_db.Info(args_[1], &info);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    switch (type_) {
      case BloomInfoType::ALL:
        *output = "*" + std::to_string(2 * 5) + CRLF;
        *output += redis::SimpleString("Capacity");
        *output += redis::Integer(info.capacity);
        *output += redis::SimpleString("Size");
        *output += redis::Integer(info.bloom_bytes);
        *output += redis::SimpleString("Number of filters");
        *output += redis::Integer(info.n_filters);
        *output += redis::SimpleString("Number of items inserted");
        *output += redis::Integer(info.size);
        *output += redis::SimpleString("Expansion rate");
        *output += redis::Integer(info.expansion);
        break;
      case BloomInfoType::CAPACITY:
        *output = redis::Integer(info.capacity);
        break;
      case BloomInfoType::SIZE:
        *output = redis::Integer(info.bloom_bytes);
        break;
      case BloomInfoType::FILTERS:
        *output = redis::Integer(info.n_filters);
        break;
      case BloomInfoType::ITEMS:
        *output = redis::Integer(info.size);
        break;
      case BloomInfoType::EXPANSION:
        *output = redis::Integer(info.expansion);
        break;
      default:
        LOG(ERROR) << "Failed to parse the type of BF.INFO command";
    }

    return Status::OK();
  }

 private:
  BloomInfoType type_ = BloomInfoType::ALL;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandBFReserve>("bf.reserve", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFAdd>("bf.add", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFExists>("bf.exists", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBFInfo>("bf.info", -2, "read-only", 1, 1, 1), )
}  // namespace redis
