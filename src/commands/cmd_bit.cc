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
#include "types/redis_bitmap.h"

namespace redis {

Status GetBitOffsetFromArgument(const std::string &arg, uint32_t *offset) {
  auto parse_result = ParseInt<uint32_t>(arg, 10);
  if (!parse_result) {
    return parse_result.ToStatus();
  }

  *offset = *parse_result;
  return Status::OK();
}

class CommandGetBit : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = GetBitOffsetFromArgument(args[2], &offset_);
    if (!s.IsOK()) return s;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    bool bit = false;
    redis::Bitmap bitmap_db(svr->storage, conn->GetNamespace());
    auto s = bitmap_db.GetBit(args_[1], offset_, &bit);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(bit ? 1 : 0);
    return Status::OK();
  }

 private:
  uint32_t offset_ = 0;
};

class CommandSetBit : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = GetBitOffsetFromArgument(args[2], &offset_);
    if (!s.IsOK()) return s;

    if (args[3] == "0") {
      bit_ = false;
    } else if (args[3] == "1") {
      bit_ = true;
    } else {
      return {Status::RedisParseErr, "bit is not an integer or out of range"};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    bool old_bit = false;
    redis::Bitmap bitmap_db(svr->storage, conn->GetNamespace());
    auto s = bitmap_db.SetBit(args_[1], offset_, bit_, &old_bit);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(old_bit ? 1 : 0);
    return Status::OK();
  }

 private:
  uint32_t offset_ = 0;
  bool bit_ = false;
};

class CommandBitCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 3) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if (args.size() == 4) {
      auto parse_start = ParseInt<int64_t>(args[2], 10);
      if (!parse_start) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      start_ = *parse_start;
      auto parse_stop = ParseInt<int64_t>(args[3], 10);
      if (!parse_stop) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      stop_ = *parse_stop;
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    uint32_t cnt = 0;
    redis::Bitmap bitmap_db(svr->storage, conn->GetNamespace());
    auto s = bitmap_db.BitCount(args_[1], start_, stop_, &cnt);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(cnt);
    return Status::OK();
  }

 private:
  int64_t start_ = 0;
  int64_t stop_ = -1;
};

class CommandBitPos : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() >= 4) {
      auto parse_start = ParseInt<int64_t>(args[3], 10);
      if (!parse_start) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      start_ = *parse_start;
    }

    if (args.size() >= 5) {
      auto parse_stop = ParseInt<int64_t>(args[4], 10);
      if (!parse_stop) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      stop_given_ = true;
      stop_ = *parse_stop;
    }

    if (args[2] == "0") {
      bit_ = false;
    } else if (args[2] == "1") {
      bit_ = true;
    } else {
      return {Status::RedisParseErr, "bit should be 0 or 1"};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t pos = 0;
    redis::Bitmap bitmap_db(svr->storage, conn->GetNamespace());
    auto s = bitmap_db.BitPos(args_[1], bit_, start_, stop_, stop_given_, &pos);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(pos);
    return Status::OK();
  }

 private:
  int64_t start_ = 0;
  int64_t stop_ = -1;
  bool bit_ = false;
  bool stop_given_ = false;
};

class CommandBitOp : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    std::string opname = util::ToLower(args[1]);
    if (opname == "and")
      op_flag_ = kBitOpAnd;
    else if (opname == "or")
      op_flag_ = kBitOpOr;
    else if (opname == "xor")
      op_flag_ = kBitOpXor;
    else if (opname == "not")
      op_flag_ = kBitOpNot;
    else
      return {Status::RedisInvalidCmd, "Unknown bit operation"};
    if (op_flag_ == kBitOpNot && args.size() != 4) {
      return {Status::RedisInvalidCmd, "BITOP NOT must be called with a single source key."};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> op_keys;
    op_keys.reserve(args_.size() - 2);
    for (uint64_t i = 3; i < args_.size(); i++) {
      op_keys.emplace_back(args_[i]);
    }

    int64_t dest_key_len = 0;
    redis::Bitmap bitmap_db(svr->storage, conn->GetNamespace());
    auto s = bitmap_db.BitOp(op_flag_, args_[1], args_[2], op_keys, &dest_key_len);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(dest_key_len);
    return Status::OK();
  }

 private:
  BitOpFlags op_flag_;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandGetBit>("getbit", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSetBit>("setbit", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBitCount>("bitcount", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBitPos>("bitpos", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBitOp>("bitop", -4, "write", 2, -1, 1), )

}  // namespace redis
