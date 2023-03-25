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
#include "commands/scan_base.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_zset.h"

namespace Redis {

class CommandZAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t index = 2;
    parseFlags(args, index);
    if (auto s = validateFlags(); !s.IsOK()) {
      return s;
    }

    if (auto left = (args.size() - index); left >= 0) {
      if (flags_.HasIncr() && left != 2) {
        return {Status::RedisParseErr, "INCR option supports a single increment-element pair"};
      }

      if (left % 2 != 0 || left == 0) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    for (size_t i = index; i < args.size(); i += 2) {
      auto score = ParseFloat(args[i]);
      if (!score) {
        return {Status::RedisParseErr, errValueIsNotFloat};
      }
      if (std::isnan(*score)) {
        return {Status::RedisParseErr, errScoreIsNotValidFloat};
      }

      member_scores_.emplace_back(MemberScore{args[i + 1], *score});
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    double old_score = member_scores_[0].score;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Add(args_[1], flags_, &member_scores_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (flags_.HasIncr()) {
      auto new_score = member_scores_[0].score;
      if ((flags_.HasNX() || flags_.HasXX() || flags_.HasLT() || flags_.HasGT()) && old_score == new_score &&
          ret == 0) {  // not the first time using incr && score not changed
        *output = Redis::NilString();
        return Status::OK();
      }

      *output = Redis::BulkString(Util::Float2String(new_score));
    } else {
      *output = Redis::Integer(ret);
    }
    return Status::OK();
  }

 private:
  std::vector<MemberScore> member_scores_;
  ZAddFlags flags_{0};

  void parseFlags(const std::vector<std::string> &args, size_t &index);
  Status validateFlags() const;
};

void CommandZAdd::parseFlags(const std::vector<std::string> &args, size_t &index) {
  std::unordered_map<std::string, ZSetFlags> options = {{"xx", kZSetXX}, {"nx", kZSetNX}, {"ch", kZSetCH},
                                                        {"lt", kZSetLT}, {"gt", kZSetGT}, {"incr", kZSetIncr}};
  for (size_t i = 2; i < args.size(); i++) {
    auto option = Util::ToLower(args[i]);
    if (auto it = options.find(option); it != options.end()) {
      flags_.SetFlag(it->second);
      index++;
    } else {
      break;
    }
  }
}

Status CommandZAdd::validateFlags() const {
  if (!flags_.HasAnyFlags()) {
    return Status::OK();
  }

  if (flags_.HasNX() && flags_.HasXX()) {
    return {Status::RedisParseErr, "XX and NX options at the same time are not compatible"};
  }

  if ((flags_.HasLT() && flags_.HasGT()) || (flags_.HasLT() && flags_.HasNX()) || (flags_.HasGT() && flags_.HasNX())) {
    return {Status::RedisParseErr, errZSetLTGTNX};
  }

  return Status::OK();
}

class CommandZCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = Redis::ZSet::ParseRangeSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Count(args_[1], spec_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
};

class CommandZCard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Card(args_[1], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandZIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[2]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    incr_ = *increment;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double score = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.IncrBy(args_[1], args_[3], incr_, &score);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::BulkString(Util::Float2String(score));
    return Status::OK();
  }

 private:
  double incr_ = 0.0;
};

class CommandZLexCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = ParseRangeLexSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.RangeByLex(args_[1], spec_, nullptr, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  CommonRangeLexSpec spec_;
};

class CommandZPop : public Commander {
 public:
  explicit CommandZPop(bool min) : min_(min) {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 2) {
      auto parse_result = ParseInt<int>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> member_scores;
    auto s = zset_db.Pop(args_[1], count_, min_, &member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(Redis::MultiLen(member_scores.size() * 2));
    for (const auto &ms : member_scores) {
      output->append(Redis::BulkString(ms.member));
      output->append(Redis::BulkString(Util::Float2String(ms.score)));
    }

    return Status::OK();
  }

 private:
  bool min_;
  int count_ = 1;
};

class CommandZPopMin : public CommandZPop {
 public:
  CommandZPopMin() : CommandZPop(true) {}
};

class CommandZPopMax : public CommandZPop {
 public:
  CommandZPopMax() : CommandZPop(false) {}
};

class CommandZRange : public Commander {
 public:
  explicit CommandZRange(bool reversed = false) : reversed_(reversed) {}

  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    start_ = *parse_start;
    stop_ = *parse_stop;
    if (args.size() > 4 && (Util::ToLower(args[4]) == "withscores")) {
      with_scores_ = true;
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> member_scores;
    uint8_t flags = !reversed_ ? 0 : kZSetReversed;
    auto s = zset_db.Range(args_[1], start_, stop_, flags, &member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!with_scores_) {
      output->append(Redis::MultiLen(member_scores.size()));
    } else {
      output->append(Redis::MultiLen(member_scores.size() * 2));
    }

    for (const auto &ms : member_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_) output->append(Redis::BulkString(Util::Float2String(ms.score)));
    }

    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
  bool reversed_;
  bool with_scores_ = false;
};

class CommandZRevRange : public CommandZRange {
 public:
  CommandZRevRange() : CommandZRange(true) {}
};

class CommandZRangeByLex : public Commander {
 public:
  explicit CommandZRangeByLex(bool reversed = false) { spec_.reversed = reversed; }

  Status Parse(const std::vector<std::string> &args) override {
    Status s;
    if (spec_.reversed) {
      s = ParseRangeLexSpec(args[3], args[2], &spec_);
    } else {
      s = ParseRangeLexSpec(args[2], args[3], &spec_);
    }

    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }

    if (args.size() == 7 && Util::ToLower(args[4]) == "limit") {
      auto parse_offset = ParseInt<int>(args[5], 10);
      auto parse_count = ParseInt<int>(args[6], 10);
      if (!parse_offset || !parse_count) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      spec_.offset = *parse_offset;
      spec_.count = *parse_count;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    auto s = zset_db.RangeByLex(args_[1], spec_, &members, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }

 private:
  CommonRangeLexSpec spec_;
};

class CommandZRangeByScore : public Commander {
 public:
  explicit CommandZRangeByScore(bool reversed = false) { spec_.reversed = reversed; }
  Status Parse(const std::vector<std::string> &args) override {
    Status s;
    if (spec_.reversed) {
      s = Redis::ZSet::ParseRangeSpec(args[3], args[2], &spec_);
    } else {
      s = Redis::ZSet::ParseRangeSpec(args[2], args[3], &spec_);
    }

    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }

    size_t i = 4;
    while (i < args.size()) {
      if (Util::ToLower(args[i]) == "withscores") {
        with_scores_ = true;
        i++;
      } else if (Util::ToLower(args[i]) == "limit" && i + 2 < args.size()) {
        auto parse_offset = ParseInt<int>(args[i + 1], 10);
        auto parse_count = ParseInt<int>(args[i + 2], 10);
        if (!parse_offset || !parse_count) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        spec_.offset = *parse_offset;
        spec_.count = *parse_count;
        i += 3;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> member_scores;
    auto s = zset_db.RangeByScore(args_[1], spec_, &member_scores, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!with_scores_) {
      output->append(Redis::MultiLen(member_scores.size()));
    } else {
      output->append(Redis::MultiLen(member_scores.size() * 2));
    }

    for (const auto &ms : member_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_) output->append(Redis::BulkString(Util::Float2String(ms.score)));
    }

    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
  bool with_scores_ = false;
};

class CommandZRank : public Commander {
 public:
  explicit CommandZRank(bool reversed = false) : reversed_(reversed) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int rank = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Rank(args_[1], args_[2], reversed_, &rank);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (rank == -1) {
      *output = Redis::NilString();
    } else {
      *output = Redis::Integer(rank);
    }
    return Status::OK();
  }

 private:
  bool reversed_;
};

class CommandZRevRank : public CommandZRank {
 public:
  CommandZRevRank() : CommandZRank(true) {}
};

class CommandZRevRangeByScore : public CommandZRangeByScore {
 public:
  CommandZRevRangeByScore() : CommandZRangeByScore(true) {}
};

class CommandZRevRangeByLex : public CommandZRangeByLex {
 public:
  CommandZRevRangeByLex() : CommandZRangeByLex(true) {}
};

class CommandZRem : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<rocksdb::Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }

    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Remove(args_[1], members, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }
};

class CommandZRemRangeByRank : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    start_ = *parse_start;
    stop_ = *parse_stop;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.RemoveRangeByRank(args_[1], start_, stop_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandZRemRangeByScore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = Redis::ZSet::ParseRangeSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.RemoveRangeByScore(args_[1], spec_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
};

class CommandZRemRangeByLex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = ParseRangeLexSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.RemoveRangeByLex(args_[1], spec_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  CommonRangeLexSpec spec_;
};

class CommandZScore : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double score = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.Score(args_[1], args_[2], &score);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(Util::Float2String(score));
    }
    return Status::OK();
  }
};

class CommandZMScore : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }
    std::map<std::string, double> mscores;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.MGet(args_[1], members, &mscores);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> values;
    if (s.IsNotFound()) {
      values.resize(members.size(), "");
    } else {
      for (const auto &member : members) {
        auto iter = mscores.find(member.ToString());
        if (iter == mscores.end()) {
          values.emplace_back("");
        } else {
          values.emplace_back(Util::Float2String(iter->second));
        }
      }
    }
    *output = Redis::MultiBulkString(values);
    return Status::OK();
  }
};

class CommandZUnionStore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    numkeys_ = *parse_result;
    if (numkeys_ > args.size() - 3) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    size_t j = 0;
    while (j < numkeys_) {
      keys_weights_.emplace_back(KeyWeight{args[j + 3], 1});
      j++;
    }

    size_t i = 3 + numkeys_;
    while (i < args.size()) {
      if (Util::ToLower(args[i]) == "aggregate" && i + 1 < args.size()) {
        if (Util::ToLower(args[i + 1]) == "sum") {
          aggregate_method_ = kAggregateSum;
        } else if (Util::ToLower(args[i + 1]) == "min") {
          aggregate_method_ = kAggregateMin;
        } else if (Util::ToLower(args[i + 1]) == "max") {
          aggregate_method_ = kAggregateMax;
        } else {
          return {Status::RedisParseErr, "aggregate param error"};
        }
        i += 2;
      } else if (Util::ToLower(args[i]) == "weights" && i + numkeys_ < args.size()) {
        size_t k = 0;
        while (k < numkeys_) {
          auto weight = ParseFloat(args[i + k + 1]);
          if (!weight || std::isnan(*weight)) {
            return {Status::RedisParseErr, "weight is not a double or out of range"};
          }
          keys_weights_[k].weight = *weight;

          k++;
        }
        i += numkeys_ + 1;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.UnionStore(args_[1], keys_weights_, aggregate_method_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }

 protected:
  size_t numkeys_ = 0;
  std::vector<KeyWeight> keys_weights_;
  AggregateMethod aggregate_method_ = kAggregateSum;
};

class CommandZInterStore : public CommandZUnionStore {
 public:
  CommandZInterStore() : CommandZUnionStore() {}

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size = 0;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    auto s = zset_db.InterStore(args_[1], keys_weights_, aggregate_method_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(size);
    return Status::OK();
  }
};

class CommandZScan : public CommandSubkeyScanBase {
 public:
  CommandZScan() = default;

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    std::vector<double> scores;
    auto s = zset_db.Scan(key, cursor, limit, prefix, &members, &scores);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> score_strings;
    score_strings.reserve(scores.size());
    for (const auto &score : scores) {
      score_strings.emplace_back(Util::Float2String(score));
    }
    *output = GenerateOutput(members, score_strings);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandZAdd>("zadd", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZCard>("zcard", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZCount>("zcount", 4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZIncrBy>("zincrby", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZInterStore>("zinterstore", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZLexCount>("zlexcount", 4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZPopMax>("zpopmax", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZPopMin>("zpopmin", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZRange>("zrange", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRevRange>("zrevrange", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRangeByLex>("zrangebylex", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRevRangeByLex>("zrevrangebylex", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRangeByScore>("zrangebyscore", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRank>("zrank", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRem>("zrem", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZRemRangeByRank>("zremrangebyrank", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZRemRangeByScore>("zremrangebyscore", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZRemRangeByLex>("zremrangebylex", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZRevRangeByScore>("zrevrangebyscore", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRevRank>("zrevrank", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZScore>("zscore", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZMScore>("zmscore", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZScan>("zscan", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZUnionStore>("zunionstore", -4, "write", 1, 1, 1), )

}  // namespace Redis
