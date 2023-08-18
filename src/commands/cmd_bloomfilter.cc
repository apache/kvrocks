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

    size_t expect_size = 4;
    auto iter = find(args.begin(), args.end(), "NONSCALING");
    if (iter != args.end()) {
      scaling_ = 0;
      expect_size += 1;
    }
    iter = find(args.begin(), args.end(), "EXPANSION");
    if (iter != args.end()) {
      if (scaling_ == 0) {
        return {Status::RedisParseErr, "nonscaling filters cannot expand"};
      } else if (++iter == args.end()) {
        return {Status::RedisParseErr, "no expansion"};
      } else {
        auto parse_expansion = ParseInt<uint16_t>(*iter, 10);
        if (!parse_expansion) {
          return {Status::RedisParseErr, "ERR bad expansion"};
        }
        expansion_ = *parse_expansion;
        if (expansion_ < 1) {
          return {Status::RedisParseErr, "expansion should be greater or equal to 1"};
        }
        expect_size += 2;
      }
    }

    if (args.size() != expect_size) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::SBChain bloomfilter_db(svr->storage, conn->GetNamespace());
    auto s = bloomfilter_db.Reserve(args_[1], error_rate_, capacity_, expansion_, scaling_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  double error_rate_;
  uint64_t capacity_;
  uint16_t expansion_ = kBFDefaultExpansion;
  uint16_t scaling_ = 1;
};

class CommandBFADD : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::SBChain bloom_db(svr->storage, conn->GetNamespace());
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
    redis::SBChain bloom_db(svr->storage, conn->GetNamespace());
    std::vector<int> rets(items_.size(), 0);
    auto s = bloom_db.MAdd(args_[1], items_, rets);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiInteger(rets);
    return Status::OK();
  }

 private:
  std::vector<Slice> items_;
};

class CommandBFINSERT : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    insert_options_ = {.capacity = kBFDefaultInitCapacity,
                       .error_rate = kBFDefaultErrorRate,
                       .autocreate = 1,
                       .expansion = kBFDefaultExpansion,
                       .scaling = 1};

    size_t expect_size = 2;

    auto items_begin = find(args.begin(), args.end(), "ITEMS");
    if (items_begin == args.end()) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    expect_size += 1;
    for (auto item_iter = items_begin + 1; item_iter < args.end(); ++item_iter) {
      items_.emplace_back(*item_iter);
      expect_size += 1;
    }
    if (items_.size() == 0) {
      return {Status::RedisParseErr, "num of items should > 0"};
    }

    auto iter = find(args.begin(), items_begin, "NOCREATE");
    if (iter != items_begin) {
      insert_options_.autocreate = 0;
      expect_size += 1;
    }

    iter = find(args.begin(), items_begin, "CAPACITY");
    if (iter != items_begin) {
      if (insert_options_.autocreate == 0) {
        return {Status::RedisParseErr, "specify NOCREATE together with CAPACITY"};
      } else {
        auto parse_capacity = ParseInt<uint64_t>(*(++iter), 10);
        if (!parse_capacity) {
          return {Status::RedisParseErr, errValueNotInteger};
        }
        insert_options_.capacity = *parse_capacity;
        if (insert_options_.capacity <= 0) {
          return {Status::RedisParseErr, "capacity should be larger than 0"};
        }
        expect_size += 2;
      }
    }

    iter = find(args.begin(), items_begin, "ERROR");
    if (iter != items_begin) {
      if (insert_options_.autocreate == 0) {
        return {Status::RedisParseErr, "specify NOCREATE together with ERROR"};
      } else {
        auto parse_error_rate = ParseFloat<double>(*(++iter));
        if (!parse_error_rate) {
          return {Status::RedisParseErr, errValueIsNotFloat};
        }
        insert_options_.error_rate = *parse_error_rate;
        if (insert_options_.error_rate >= 1 || insert_options_.error_rate <= 0) {
          return {Status::RedisParseErr, "0 < error rate range < 1"};
        }
        expect_size += 2;
      }
    }

    iter = find(args.begin(), items_begin, "NONSCALING");
    if (iter != items_begin) {
      insert_options_.scaling = 0;
      expect_size += 1;
    }
    iter = find(args.begin(), items_begin, "EXPANSION");
    if (iter != items_begin) {
      if (insert_options_.scaling == 0) {
        return {Status::RedisParseErr, "nonscaling filters cannot expand"};
      } else if (++iter == args.end()) {
        return {Status::RedisParseErr, "no expansion"};
      } else {
        auto parse_expansion = ParseInt<uint16_t>(*iter, 10);
        if (!parse_expansion) {
          return {Status::RedisParseErr, "ERR bad expansion"};
        }
        insert_options_.expansion = *parse_expansion;
        if (insert_options_.expansion < 1) {
          return {Status::RedisParseErr, "expansion should be greater or equal to 1"};
        }
        expect_size += 2;
      }
    }

    if (args.size() != expect_size) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::SBChain bloom_db(svr->storage, conn->GetNamespace());
    std::vector<int> rets(items_.size(), 0);
    auto s = bloom_db.InsertCommon(args_[1], items_, &insert_options_, rets);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiInteger(rets);
    return Status::OK();
  }

 private:
  std::vector<Slice> items_;
  BFInsertOptions insert_options_;
};

class CommandBFEXIST : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::SBChain bloom_db(svr->storage, conn->GetNamespace());
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
    redis::SBChain bloom_db(svr->storage, conn->GetNamespace());
    std::vector<int> rets(items_.size(), 0);
    auto s = bloom_db.MExist(args_[1], items_, rets);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiInteger(rets);
    return Status::OK();
  }

 private:
  std::vector<Slice> items_;
};

class CommandBFINFO : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 3) {
      const std::string &info = args[2];
      if (info != "CAPACITY" && info != "SIZE" && info != "FILTERS" && info != "ITEMS" && info != "EXPANSION") {
        return {Status::RedisParseErr, "Invalid info argument"};
      }
      info_ = info;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::SBChain bloom_db(svr->storage, conn->GetNamespace());
    std::vector<int> rets;
    if (info_ == "ALL") {
      rets.resize(5, 0);
    } else {
      rets.resize(1, 0);
    }
    auto s = bloom_db.Info(args_[1], info_, rets);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    if (info_ == "ALL") {
      *output = "*" + std::to_string(2 * all_nums_) + CRLF;  // todo: this reply only used in here, whether should I place it in redis_reply.h
      for (int i = 0; i < all_nums_; ++i) {
        *output += redis::SimpleString(all_info_rets_[i]);
        *output += redis::Integer(rets[i]);
      }
    } else {
      *output = redis::Integer(rets[0]);
    }

    return Status::OK();
  }

 private:
  std::string info_ = "ALL";
  const int all_nums_ = 5;
  const std::vector<std::string> all_info_rets_ = {"Capacity", "Size", "Number of filters", "Number of items inserted",
                                                   "Expansion rate"};
};

class CommandBFCARD : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::SBChain bloom_db(svr->storage, conn->GetNamespace());
    std::vector<int> rets(1, 0);
    auto s = bloom_db.Info(args_[1], info_, rets);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(rets[0]);
    return Status::OK();
  }

 private:
  std::string info_ = "ITEMS";
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandBFReserve>("bf.reserve", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFADD>("bf.add", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFMADD>("bf.madd", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFINSERT>("bf.insert", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFEXIST>("bf.exist", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBFMEXIST>("bf.mexist", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBFINFO>("bf.info", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBFCARD>("bf.card", 2, "read-only", 1, 1, 1), )

}  // namespace redis