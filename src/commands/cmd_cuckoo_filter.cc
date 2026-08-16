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

#include <cstdlib>
#include <vector>

#include "command_parser.h"
#include "commander.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_bloom_chain.h"
#include "types/redis_cuckoo_chain.h"

namespace redis {

class CommandCFReserve : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // CF.RESERVE key capacity [BUCKETSIZE bs] [MAXITERATIONS mi] [EXPANSION ex]
    if (args.size() < 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    // Parse capacity (required)
    auto parse_capacity = ParseInt<uint64_t>(args[2], 10);
    if (!parse_capacity) {
      return {Status::RedisParseErr, "invalid capacity"};
    }
    capacity_ = *parse_capacity;
    if (capacity_ <= 0) {
      return {Status::RedisParseErr, "capacity must be larger than 0"};
    }

    // Parse optional parameters
    CommandParser parser(args, 3);
    while (parser.Good()) {
      if (parser.EatEqICase("BUCKETSIZE")) {
        auto parse_bucket_size = parser.TakeInt<uint8_t>();
        if (!parse_bucket_size.IsOK()) {
          return {Status::RedisParseErr, "invalid bucket size"};
        }
        bucket_size_ = parse_bucket_size.GetValue();
        if (bucket_size_ == 0 || bucket_size_ > 255) {
          return {Status::RedisParseErr, "bucket size must be between 1 and 255"};
        }
      } else if (parser.EatEqICase("MAXITERATIONS")) {
        auto parse_max_iterations = parser.TakeInt<uint16_t>();
        if (!parse_max_iterations.IsOK()) {
          return {Status::RedisParseErr, "invalid max iterations"};
        }
        max_iterations_ = parse_max_iterations.GetValue();
        if (max_iterations_ == 0) {
          return {Status::RedisParseErr, "max iterations must be larger than 0"};
        }
      } else if (parser.EatEqICase("EXPANSION")) {
        auto parse_expansion = parser.TakeInt<uint16_t>();
        if (!parse_expansion.IsOK()) {
          return {Status::RedisParseErr, "invalid expansion factor"};
        }
        expansion_ = parse_expansion.GetValue();
        if (expansion_ > kCFMaxExpansion) {
          return {Status::RedisParseErr, "expansion must be between 0 and 32768"};
        }
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    auto s = cuckoo_db.Reserve(ctx, args_[1], capacity_, bucket_size_, max_iterations_, expansion_,
                               kCuckooFilterDefaultPageSize);

    if (!s.ok()) {
      if (s.IsInvalidArgument()) {
        // Return error message to client
        return {Status::RedisExecErr, s.ToString()};
      }
      return {Status::RedisExecErr, "failed to create cuckoo filter"};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  uint64_t capacity_ = kCFDefaultCapacity;
  uint8_t bucket_size_ = kCFDefaultBucketSize;
  uint16_t max_iterations_ = kCFDefaultMaxIterations;
  uint16_t expansion_ = kCFDefaultExpansion;
};

class CommandCFAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // CF.ADD key item
    if (args.size() != 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    redis::CuckooFilterInsertResult ret = CuckooFilterInsertResult::kOk;
    auto s = cuckoo_db.Add(ctx, args_[1], args_[2], ret);

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    // Duplicate items are allowed, so successful insertions return 1.
    switch (ret) {
      case CuckooFilterInsertResult::kOk:
        *output = redis::Integer(1);
        break;
      case CuckooFilterInsertResult::kExist:
        std::cout << "Item exists result in add command is not possible" << std::endl;
        std::abort();
      case CuckooFilterInsertResult::kFull:
        *output = redis::Error({Status::NotOK, "filter is full"});
        break;
    }
    return Status::OK();
  }
};

class CommandCFInsert : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // CF.INSERT key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
    if (args.size() < 4) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    CommandParser parser(args, 2);
    while (parser.Good()) {
      if (parser.EatEqICase("CAPACITY")) {
        auto parse_capacity = parser.TakeInt<uint64_t>();
        if (!parse_capacity.IsOK()) {
          return {Status::RedisParseErr, "invalid capacity"};
        }
        insert_options_.capacity = parse_capacity.GetValue();
        if (insert_options_.capacity <= 0) {
          return {Status::RedisParseErr, "capacity must be larger than 0"};
        }
      } else if (parser.EatEqICase("NOCREATE")) {
        insert_options_.auto_create = false;
      } else if (parser.EatEqICase("ITEMS")) {
        has_items_ = true;
        break;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    if (!has_items_) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    while (parser.Good()) {
      items_.emplace_back(GET_OR_RET(parser.TakeStr()));
    }

    if (items_.empty()) {
      return {Status::RedisParseErr, "num of items should be greater than 0"};
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    std::vector<CuckooFilterInsertResult> rets(items_.size(), CuckooFilterInsertResult::kOk);

    auto s = cuckoo_db.Insert(ctx, args_[1], items_, insert_options_, rets);

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::MultiLen(items_.size());
    for (const auto &ret : rets) {
      switch (ret) {
        case CuckooFilterInsertResult::kOk:
          *output += redis::Integer(1);
          break;
        case CuckooFilterInsertResult::kExist:
          std::cout << "Item exists result in add command is not possible" << std::endl;
          std::abort();
        case CuckooFilterInsertResult::kFull:
          *output += redis::Integer(-1);
          break;
      }
    }

    return Status::OK();
  }

 private:
  CuckooFilterInsertOptions insert_options_;
  bool has_items_ = false;
  std::vector<std::string> items_;
};

// Register the CF.RESERVE and CF.ADD commands
REDIS_REGISTER_COMMANDS(CuckooFilter, MakeCmdAttr<CommandCFReserve>("cf.reserve", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCFAdd>("cf.add", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCFInsert>("cf.insert", -4, "write", 1, 1, 1))

}  // namespace redis
