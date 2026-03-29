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

#include <types/redis_cms.h>

#include "commander.h"
#include "commands/command_parser.h"
#include "server/redis_reply.h"
#include "server/server.h"

namespace redis {

/// CMS.INITBYDIM - Initialize a Count-Min Sketch with specified dimensions
///
/// Redis command: CMS.INITBYDIM key width depth
/// Documentation: https://redis.io/docs/latest/commands/cms.initbydim/
///
/// Parameters:
///   - key: The name of the sketch
///   - width: Number of counters in each array (reduces error size)
///   - depth: Number of counter-arrays (reduces error probability)
///
/// Time complexity: O(1)
/// ACL categories: @cms, @write, @fast
class CommandCMSInitByDim final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_width = ParseInt<uint32_t>(args[2], 10);
    if (!parse_width) {
      return {Status::RedisParseErr, "invalid width"};
    }
    width_ = *parse_width;

    auto parse_depth = ParseInt<uint32_t>(args[3], 10);
    if (!parse_depth) {
      return {Status::RedisParseErr, "invalid depth"};
    }
    depth_ = *parse_depth;

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());

    auto s = cms.InitByDim(ctx, args_[1], width_, depth_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  uint32_t width_;
  uint32_t depth_;
};

/// CMS.INITBYPROB - Initialize a Count-Min Sketch with specified error rate and probability
///
/// Redis command: CMS.INITBYPROB key error probability
/// Documentation: https://redis.io/docs/latest/commands/cms.initbyprob/
///
/// Parameters:
///   - key: The name of the sketch
///   - error: Estimate size of error (as percent of total counted items)
///   - probability: Desired probability for inflated count (failure probability)
///
/// Time complexity: O(1)
/// ACL categories: @cms, @write, @fast
class CommandCMSInitByProb final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_error = ParseFloat<double>(args[2]);
    if (!parse_error) {
      return {Status::RedisParseErr, "invalid error rate"};
    }
    error_rate_ = *parse_error;

    auto parse_prob = ParseFloat<double>(args[3]);
    if (!parse_prob) {
      return {Status::RedisParseErr, "invalid probability"};
    }
    probability_ = *parse_prob;

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());

    auto s = cms.InitByProb(ctx, args_[1], error_rate_, probability_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  double error_rate_;
  double probability_;
};

/// CMS.INCRBY - Increment the count of one or more items
///
/// Redis command: CMS.INCRBY key item increment [item increment ...]
/// Documentation: https://redis.io/docs/latest/commands/cms.incrby/
///
/// Parameters:
///   - key: The name of the sketch
///   - item: The item to increment
///   - increment: Amount to increment (must be non-negative)
///
/// Time complexity: O(n) where n is the number of items
/// ACL categories: @cms, @write, @fast
///
/// Returns: Array of estimated counts for each item after increment
/// Errors: invalid arguments, missing key, overflow (saturates at UINT32_MAX), wrong key type
class CommandCMSIncrBy final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
      return {Status::RedisParseErr, "wrong number of arguments"};
    }

    for (size_t i = 2; i < args.size(); i += 2) {
      auto parse_increment = ParseInt<int64_t>(args[i + 1], 10);
      if (!parse_increment) {
        return {Status::RedisParseErr, "invalid increment"};
      }
      if (*parse_increment < 0) {
        return {Status::RedisParseErr, "increment must be non-negative"};
      }
      items_.emplace_back(args[i], *parse_increment);
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    std::vector<uint64_t> counts;

    auto s = cms.IncrBy(ctx, args_[1], items_, &counts);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(counts.size());
    for (auto count : counts) {
      *output += redis::Integer(count);
    }
    return Status::OK();
  }

 private:
  std::vector<std::pair<std::string, int64_t>> items_;
};

/// CMS.QUERY - Return the estimated count of one or more items
///
/// Redis command: CMS.QUERY key item [item ...]
/// Documentation: https://redis.io/docs/latest/commands/cms.query/
///
/// Parameters:
///   - key: The name of the sketch
///   - item: One or more items to query
///
/// Time complexity: O(n) where n is the number of items
/// ACL categories: @cms, @read, @fast
///
/// Returns: Array of estimated counts (min-counts across all layers)
/// Errors: invalid arguments, missing key, wrong key type
class CommandCMSQuery final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    items_.reserve(args.size() - 2);
    for (size_t i = 2; i < args.size(); ++i) {
      items_.push_back(args[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    std::vector<uint64_t> counts;

    auto s = cms.Query(ctx, args_[1], items_, &counts);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(counts.size());
    for (auto count : counts) {
      *output += redis::Integer(count);
    }
    return Status::OK();
  }

 private:
  std::vector<std::string> items_;
};

/// CMS.MERGE - Merge multiple Count-Min Sketches into one
///
/// Redis command: CMS.MERGE destination numKeys source [source ...] [WEIGHTS weight [weight ...]]
/// Documentation: https://redis.io/docs/latest/commands/cms.merge/
///
/// Parameters:
///   - destination: Name of destination sketch (must be initialized)
///   - numKeys: Number of sketches to merge
///   - source: Names of source sketches
///   - weight: Multiplier for each sketch (can be negative, default = 1)
///
/// Time complexity: O(n) where n is the number of sketches
/// ACL categories: @cms, @write
///
/// Requirements:
///   - All sketches must have identical width and depth
///   - Destination must already exist
///
/// Returns: OK on success
/// Errors: invalid arguments, overflow, dimension mismatch, missing key
class CommandCMSMerge final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_numkeys = ParseInt<size_t>(args[2], 10);
    if (!parse_numkeys) {
      return {Status::RedisParseErr, "invalid numkeys"};
    }
    numkeys_ = *parse_numkeys;

    if (args.size() < 3 + numkeys_) {
      return {Status::RedisParseErr, "wrong number of arguments"};
    }

    // Parse source keys
    for (size_t i = 0; i < numkeys_; ++i) {
      src_keys_.push_back(args[3 + i]);
    }

    // Parse optional WEIGHTS
    size_t next_arg = 3 + numkeys_;
    if (next_arg < args.size() && strcasecmp(args[next_arg].c_str(), "WEIGHTS") == 0) {
      next_arg++;
      if (args.size() < next_arg + numkeys_) {
        return {Status::RedisParseErr, "wrong number of weights"};
      }
      for (size_t i = 0; i < numkeys_; ++i) {
        auto parse_weight = ParseInt<int64_t>(args[next_arg + i], 10);
        if (!parse_weight) {
          return {Status::RedisParseErr, "invalid weight"};
        }
        weights_.push_back(*parse_weight);
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());

    auto s = cms.Merge(ctx, args_[1], src_keys_, weights_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  size_t numkeys_;
  std::vector<std::string> src_keys_;
  std::vector<int64_t> weights_;
};

/// CMS.INFO - Return information about a Count-Min Sketch
///
/// Redis command: CMS.INFO key
/// Documentation: https://redis.io/docs/latest/commands/cms.info/
///
/// Parameters:
///   - key: The name of the sketch
///
/// Time complexity: O(1)
/// ACL categories: @cms, @read, @fast
///
/// Returns: Array of key-value pairs:
///   - width: Number of counters per layer
///   - depth: Number of layers
///   - count: Total count of all items
///   - size: Total number of buckets (Kvrocks extension)
///
/// Errors: missing key, wrong key type
class CommandCMSInfo final : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    CMSInfo info;

    auto s = cms.Info(ctx, args_[1], &info);
    if (s.IsNotFound()) return {Status::RedisExecErr, "key not found"};
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(8);
    *output += redis::SimpleString("width");
    *output += redis::Integer(info.width);
    *output += redis::SimpleString("depth");
    *output += redis::Integer(info.depth);
    *output += redis::SimpleString("count");
    *output += redis::Integer(info.total_count);
    *output += redis::SimpleString("size");
    *output += redis::Integer(info.size);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(CMS, MakeCmdAttr<CommandCMSInitByDim>("cms.initbydim", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCMSInitByProb>("cms.initbyprob", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCMSIncrBy>("cms.incrby", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCMSQuery>("cms.query", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandCMSMerge>("cms.merge", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCMSInfo>("cms.info", 2, "read-only", 1, 1, 1), )

}  // namespace redis