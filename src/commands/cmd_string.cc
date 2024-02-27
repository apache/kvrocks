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

#include <cstdint>
#include <optional>
#include <string>

#include "commander.h"
#include "commands/command_parser.h"
#include "error_constants.h"
#include "server/redis_reply.h"
#include "server/redis_request.h"
#include "server/server.h"
#include "storage/redis_db.h"
#include "time_util.h"
#include "ttl_util.h"
#include "types/redis_bitmap.h"
#include "types/redis_string.h"

namespace redis {

class CommandGet : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    std::string value;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.Get(args_[1], &value);
    // The IsInvalidArgument error means the key type maybe a bitmap
    // which we need to fall back to the bitmap's GetString according
    // to the `max-bitmap-to-string-mb` configuration.
    if (s.IsInvalidArgument()) {
      Config *config = srv->GetConfig();
      uint32_t max_btos_size = static_cast<uint32_t>(config->max_bitmap_to_string_mb) * MiB;
      redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());
      s = bitmap_db.GetString(args_[1], max_btos_size, &value);
    }
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(value);
    return Status::OK();
  }
};

class CommandGetEx : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    std::string_view ttl_flag;
    while (parser.Good()) {
      if (auto v = GET_OR_RET(ParseTTL(parser, ttl_flag))) {
        ttl_ = *v;
      } else if (parser.EatEqICaseFlag("PERSIST", ttl_flag)) {
        persist_ = true;
      } else {
        return parser.InvalidSyntax();
      }
    }
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    std::string value;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.GetEx(args_[1], &value, ttl_, persist_);

    // The IsInvalidArgument error means the key type maybe a bitmap
    // which we need to fall back to the bitmap's GetString according
    // to the `max-bitmap-to-string-mb` configuration.
    if (s.IsInvalidArgument()) {
      Config *config = srv->GetConfig();
      uint32_t max_btos_size = static_cast<uint32_t>(config->max_bitmap_to_string_mb) * MiB;
      redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());
      s = bitmap_db.GetString(args_[1], max_btos_size, &value);
      if (s.ok()) {
        if (ttl_ > 0) {
          s = bitmap_db.Expire(args_[1], ttl_ + util::GetTimeStampMS());
        } else if (persist_) {
          s = bitmap_db.Expire(args_[1], 0);
        }
      }
    }
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(value);
    return Status::OK();
  }

 private:
  uint64_t ttl_ = 0;
  bool persist_ = false;
};

class CommandStrlen : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    std::string value;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.Get(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = redis::Integer(0);
    } else {
      *output = redis::Integer(value.size());
    }
    return Status::OK();
  }
};

class CommandGetSet : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    std::optional<std::string> old_value;
    auto s = string_db.GetSet(args_[1], args_[2], old_value);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (old_value.has_value()) {
      *output = redis::BulkString(old_value.value());
    } else {
      *output = conn->NilString();
    }
    return Status::OK();
  }
};

class CommandGetDel : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    std::string value;
    auto s = string_db.GetDel(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = conn->NilString();
    } else {
      *output = redis::BulkString(value);
    }
    return Status::OK();
  }
};

class CommandGetRange : public Commander {
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

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    std::string value;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.Get(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = conn->NilString();
      return Status::OK();
    }

    if (start_ < 0) start_ = static_cast<int>(value.size()) + start_;
    if (stop_ < 0) stop_ = static_cast<int>(value.size()) + stop_;
    if (start_ < 0) start_ = 0;
    if (stop_ > static_cast<int>(value.size())) stop_ = static_cast<int>(value.size());
    if (start_ > stop_) {
      *output = conn->NilString();
    } else {
      *output = redis::BulkString(value.substr(start_, stop_ - start_ + 1));
    }
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandSubStr : public CommandGetRange {
 public:
  CommandSubStr() : CommandGetRange() {}
};

class CommandSetRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    offset_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.SetRange(args_[1], offset_, args_[3], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int offset_ = 0;
};

class CommandMGet : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    std::vector<std::string> values;
    // always return OK
    auto statuses = string_db.MGet(keys, &values);
    *output = conn->MultiBulkString(values, statuses);
    return Status::OK();
  }
};

class CommandAppend : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.Append(args_[1], args_[2], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 3);
    std::string_view ttl_flag, set_flag;
    while (parser.Good()) {
      if (auto v = GET_OR_RET(ParseTTL(parser, ttl_flag))) {
        ttl_ = *v;
      } else if (parser.EatEqICaseFlag("KEEPTTL", ttl_flag)) {
        keep_ttl_ = true;
      } else if (parser.EatEqICaseFlag("NX", set_flag)) {
        set_flag_ = StringSetType::NX;
      } else if (parser.EatEqICaseFlag("XX", set_flag)) {
        set_flag_ = StringSetType::XX;
      } else if (parser.EatEqICase("GET")) {
        get_ = true;
      } else {
        return parser.InvalidSyntax();
      }
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    std::optional<std::string> ret;
    redis::String string_db(srv->storage, conn->GetNamespace());

    if (ttl_ < 0) {
      auto s = string_db.Del(args_[1]);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
      *output = redis::SimpleString("OK");
      return Status::OK();
    }

    rocksdb::Status s;
    s = string_db.Set(args_[1], args_[2], {ttl_, set_flag_, get_, keep_ttl_}, ret);

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (get_) {
      if (ret.has_value()) {
        *output = redis::BulkString(ret.value());
      } else {
        *output = conn->NilString();
      }
    } else {
      if (ret.has_value()) {
        *output = redis::SimpleString("OK");
      } else {
        *output = conn->NilString();
      }
    }
    return Status::OK();
  }

 private:
  uint64_t ttl_ = 0;
  bool get_ = false;
  bool keep_ttl_ = false;
  StringSetType set_flag_ = StringSetType::NONE;
};

class CommandSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*parse_result <= 0) return {Status::RedisParseErr, errInvalidExpireTime};

    ttl_ = *parse_result;

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.SetEX(args_[1], args_[3], ttl_ * 1000);
    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  uint64_t ttl_ = 0;
};

class CommandPSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto ttl_ms = ParseInt<int64_t>(args[2], 10);
    if (!ttl_ms) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*ttl_ms <= 0) return {Status::RedisParseErr, errInvalidExpireTime};

    ttl_ = *ttl_ms;

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.SetEX(args_[1], args_[3], ttl_);
    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int64_t ttl_ = 0;
};

class CommandMSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 1) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    std::vector<StringPair> kvs;
    for (size_t i = 1; i < args_.size(); i += 2) {
      kvs.emplace_back(StringPair{args_[i], args_[i + 1]});
    }

    auto s = string_db.MSet(kvs);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandSetNX : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    bool ret = false;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.SetNX(args_[1], args_[2], 0, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret ? 1 : 0);
    return Status::OK();
  }
};

class CommandMSetNX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 1) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    bool ret = false;
    std::vector<StringPair> kvs;
    redis::String string_db(srv->storage, conn->GetNamespace());
    for (size_t i = 1; i < args_.size(); i += 2) {
      kvs.emplace_back(StringPair{args_[i], args_[i + 1]});
    }

    auto s = string_db.MSetNX(kvs, 0, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret ? 1 : 0);
    return Status::OK();
  }
};

class CommandIncr : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.IncrBy(args_[1], 1, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandDecr : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.IncrBy(args_[1], -1, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.IncrBy(args_[1], increment_, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandIncrByFloat : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[2]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    increment_ = *increment;

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    double ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.IncrByFloat(args_[1], increment_, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::BulkString(util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandDecrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;

    // Negating LLONG_MIN will cause an overflow.
    if (increment_ == LLONG_MIN) {
      return {Status::RedisParseErr, "decrement would overflow"};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.IncrBy(args_[1], -1 * increment_, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandCAS : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 4);
    std::string_view flag;
    while (parser.Good()) {
      if (auto v = GET_OR_RET(ParseTTL(parser, flag))) {
        ttl_ = *v;
      } else {
        return parser.InvalidSyntax();
      }
    }
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    int ret = 0;
    auto s = string_db.CAS(args_[1], args_[2], args_[3], ttl_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  uint64_t ttl_ = 0;
};

class CommandCAD : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    int ret = 0;
    auto s = string_db.CAD(args_[1], args_[2], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandLCS : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 3);
    while (parser.Good()) {
      if (parser.EatEqICase("IDX")) {
        get_idx_ = true;
      } else if (parser.EatEqICase("LEN")) {
        get_len_ = true;
      } else if (parser.EatEqICase("WITHMATCHLEN")) {
        with_match_len_ = true;
      } else if (parser.EatEqICase("MINMATCHLEN")) {
        min_match_len_ = GET_OR_RET(parser.TakeInt<int64_t>());
        if (min_match_len_ < 0) {
          min_match_len_ = 0;
        }
      } else {
        return parser.InvalidSyntax();
      }
    }

    // Complain if the user passed ambiguous parameters.
    if (get_idx_ && get_len_) {
      return {Status::RedisParseErr,
              "If you want both the length and indexes, "
              "please just use IDX."};
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());

    std::string a;
    std::string b;
    auto s1 = string_db.Get(args_[1], &a);
    auto s2 = string_db.Get(args_[2], &b);

    if (!s1.ok() && !s1.IsNotFound()) {
      return {Status::RedisExecErr, s1.ToString()};
    }
    if (!s2.ok() && !s2.IsNotFound()) {
      return {Status::RedisExecErr, s2.ToString()};
    }
    if (s1.IsNotFound()) a = "";
    if (s2.IsNotFound()) b = "";

    // Detect string truncation or later overflows.
    if (a.length() >= UINT32_MAX - 1 || b.length() >= UINT32_MAX - 1) {
      return {Status::RedisExecErr, "String too long for LCS"};
    }

    // Compute the LCS using the vanilla dynamic programming technique of
    // building a table of LCS(x, y) substrings.
    auto alen = static_cast<uint32_t>(a.length());
    auto blen = static_cast<uint32_t>(b.length());

    // Allocate the LCS table.
    uint64_t dp_size = (alen + 1) * (blen + 1);
    uint64_t bulk_size = dp_size * sizeof(uint32_t);
    if (bulk_size >= SIZE_MAX || bulk_size / dp_size != sizeof(uint32_t)) {
      return {Status::RedisExecErr, "Insufficient memory, failed allocating transient memory for LCS"};
    }
    if (bulk_size > PROTO_BULK_MAX_SIZE) {
      return {Status::RedisExecErr, "Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len"};
    }
    std::vector<uint32_t> dp(dp_size, 0);
    auto lcs = [&](const uint32_t i, const uint32_t j) -> uint32_t & { return dp[i * (blen + 1) + j]; };

    // Start building the LCS table.
    for (uint32_t i = 1; i <= alen; i++) {
      for (uint32_t j = 1; j <= blen; j++) {
        if (a[i - 1] == b[j - 1]) {
          // The len LCS (and the LCS itself) of two
          // sequences with the same final character, is the
          // LCS of the two sequences without the last char
          // plus that last char.
          lcs(i, j) = lcs(i - 1, j - 1) + 1;
        } else {
          // If the last character is different, take the longest
          // between the LCS of the first string and the second
          // minus the last char, and the reverse.
          lcs(i, j) = std::max(lcs(i - 1, j), lcs(i, j - 1));
        }
      }
    }

    // Store the actual LCS string if needed.
    std::string result;
    uint32_t idx = lcs(alen, blen);

    // Allocate when we need to compute the actual LCS string.
    bool compute_lcs = get_idx_ || !get_len_;
    if (compute_lcs) result.resize(idx);

    // Build a array if we have to emit the matched ranges.
    std::string matches;
    uint32_t matches_len = 0;

    uint32_t i = alen;
    uint32_t j = blen;
    uint32_t arange_start = alen;  // alen signals that values are not set.
    uint32_t arange_end = 0;
    uint32_t brange_start = 0;
    uint32_t brange_end = 0;
    while (compute_lcs && i > 0 && j > 0) {
      bool emit_range = false;
      if (a[i - 1] == b[j - 1]) {
        // If there is a match, store the character and reduce
        // the indexes to look for a new match.
        result.at(idx - 1) = a[i - 1];

        // Track the current range.
        if (arange_start == alen) {
          arange_start = i - 1;
          arange_end = i - 1;
          brange_start = j - 1;
          brange_end = j - 1;
        }
        // Let's see if we can extend the range backward since
        // it is contiguous.
        else if (arange_start == i && brange_start == j) {
          arange_start--;
          brange_start--;
        } else {
          emit_range = true;
        }

        // Emit the range if we matched with the first byte of
        // one of the two strings. We'll exit the loop ASAP.
        if (arange_start == 0 || brange_start == 0) {
          emit_range = true;
        }
        idx--;
        i--;
        j--;
      } else {
        // Otherwise reduce i and j depending on the largest
        // LCS between, to understand what direction we need to go.
        uint32_t lcs1 = lcs(i - 1, j);
        uint32_t lcs2 = lcs(i, j - 1);
        if (lcs1 > lcs2)
          i--;
        else
          j--;
        if (arange_start != alen) emit_range = true;
      }

      // Emit the current range if needed.
      uint32_t match_len = arange_end - arange_start + 1;
      if (emit_range) {
        if (get_idx_ && (min_match_len_ == 0 || match_len >= min_match_len_)) {
          matches += redis::MultiLen(with_match_len_ ? 3 : 2);
          matches += redis::MultiLen(2);
          matches += redis::Integer(arange_start);
          matches += redis::Integer(arange_end);
          matches += redis::MultiLen(2);
          matches += redis::Integer(brange_start);
          matches += redis::Integer(brange_end);
          if (with_match_len_) {
            matches += redis::Integer(match_len);
          }
          matches_len++;
        }

        // Restart at the next match.
        arange_start = alen;
      }
    }

    // Build output by the given options.
    if (get_idx_) {
      *output = conn->HeaderOfMap(2);
      *output += redis::BulkString("matches");
      *output += redis::MultiLen(matches_len);
      *output += matches;
      *output += redis::BulkString("len");
      *output += redis::Integer(lcs(alen, blen));
    } else if (get_len_) {
      *output = redis::Integer(lcs(alen, blen));
    } else {
      *output = redis::BulkString(result);
    }

    return Status::OK();
  }

 private:
  bool get_idx_ = false;
  bool get_len_ = false;
  bool with_match_len_ = false;
  int64_t min_match_len_ = 0;
};

REDIS_REGISTER_COMMANDS(
    MakeCmdAttr<CommandGet>("get", 2, "read-only", 1, 1, 1), MakeCmdAttr<CommandGetEx>("getex", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandStrlen>("strlen", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGetSet>("getset", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandGetRange>("getrange", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandSubStr>("substr", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGetDel>("getdel", 2, "write no-dbsize-check", 1, 1, 1),
    MakeCmdAttr<CommandSetRange>("setrange", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandMGet>("mget", -2, "read-only", 1, -1, 1),
    MakeCmdAttr<CommandAppend>("append", 3, "write", 1, 1, 1), MakeCmdAttr<CommandSet>("set", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandSetEX>("setex", 4, "write", 1, 1, 1), MakeCmdAttr<CommandPSetEX>("psetex", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandSetNX>("setnx", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandMSetNX>("msetnx", -3, "write", 1, -1, 2),
    MakeCmdAttr<CommandMSet>("mset", -3, "write", 1, -1, 2), MakeCmdAttr<CommandIncrBy>("incrby", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandIncrByFloat>("incrbyfloat", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandIncr>("incr", 2, "write", 1, 1, 1), MakeCmdAttr<CommandDecrBy>("decrby", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandDecr>("decr", 2, "write", 1, 1, 1), MakeCmdAttr<CommandCAS>("cas", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandCAD>("cad", 3, "write", 1, 1, 1), MakeCmdAttr<CommandLCS>("lcs", -3, "read-only", 1, 2, 1), )
}  // namespace redis
