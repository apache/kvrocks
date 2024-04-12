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

#include <algorithm>
#include <memory>

#include "command_parser.h"
#include "commander.h"
#include "server/server.h"
#include "storage/redis_db.h"
#include "types/redis_hash.h"
#include "types/redis_list.h"
#include "types/redis_set.h"
#include "types/redis_string.h"
#include "types/redis_zset.h"

namespace redis {

template<bool ReadOnly>
class CommandSort : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    while (parser.Good()) {
      if (parser.EatEqICase("BY")) {
        if (parser.Remains() < 1) {
          return parser.InvalidSyntax();
        }
        sortby_ = GET_OR_RET(parser.TakeStr());

        if (sortby_.find('*') == std::string::npos) {
          dontsort_ = true;
        } else {
          // TODO Check
          /* If BY is specified with a real pattern, we can't accept it in cluster mode,
           * unless we can make sure the keys formed by the pattern are in the same slot
           * as the key to sort. */

          /* If BY is specified with a real pattern, we can't accept
           * it if no full ACL key access is applied for this command. */
        }
      } else if (parser.EatEqICase("LIMIT")) {
        if (parser.Remains() < 2) {
          return parser.InvalidSyntax();
        }
        offset_ = GET_OR_RET(parser.template TakeInt<long>());
        count_ = GET_OR_RET(parser.template TakeInt<long>());
      } else if (parser.EatEqICase("GET") && parser.Remains() >= 1) {
        if (parser.Remains() < 1) {
          return parser.InvalidSyntax();
        }
        // TODO Check
        /* If GET is specified with a real pattern, we can't accept it in cluster mode,
         * unless we can make sure the keys formed by the pattern are in the same slot
         * as the key to sort. */

        getpatterns_.push_back(GET_OR_RET(parser.TakeStr()));
      } else if (parser.EatEqICase("ASC")) {
        desc_ = false;
      } else if (parser.EatEqICase("DESC")) {
        desc_ = true;
      } else if (parser.EatEqICase("ALPHA")) {
        alpha_ = true;
      } else if (parser.EatEqICase("STORE")) {
        if constexpr (ReadOnly) {
          return parser.InvalidSyntax();
        }
        if (parser.Remains() < 1) {
          return parser.InvalidSyntax();
        }
        storekey_ = GET_OR_RET(parser.TakeStr());
      } else {
        return parser.InvalidSyntax();
      }
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    // Get Key Type
    redis::Database redis(srv->storage, conn->GetNamespace());
    RedisType type = kRedisNone;
    auto s = redis.Type(args_[1], &type);
    if (s.ok()) {
      if (type >= RedisTypeNames.size()) {
        return {Status::RedisExecErr, "Invalid type"};
      } else if (type != RedisType::kRedisList && type != RedisType::kRedisSet && type != RedisType::kRedisZSet) {
        *output = Error("WRONGTYPE Operation against a key holding the wrong kind of value");
        return Status::OK();
      }
    } else {
      return {Status::RedisExecErr, s.ToString()};
    }

    /* When sorting a set with no sort specified, we must sort the output
     * so the result is consistent across scripting and replication.
     *
     * The other types (list, sorted set) will retain their native order
     * even if no sort order is requested, so they remain stable across
     * scripting and replication. */

    // TODO c->flags & CLIENT_SCRIPT ???
    // if (dontsort_ && type == RedisType::kRedisZSet && (!storekey_.empty() || c->flags & CLIENT_SCRIPT))
    if (dontsort_ && type == RedisType::kRedisZSet && (!storekey_.empty())) {
      /* Force ALPHA sorting */
      dontsort_ = false;
      alpha_ = true;
      sortby_ = "";
    }

    // Obtain the length of the object to sort.
    uint64_t vec_count = 0;
    auto list_db = redis::List(srv->storage, conn->GetNamespace());
    auto set_db = redis::Set(srv->storage, conn->GetNamespace());
    auto zset_db = redis::ZSet(srv->storage, conn->GetNamespace());

    switch (type) {
      case RedisType::kRedisList: {
        s = list_db.Size(args_[1], &vec_count);
        if (!s.ok() && !s.IsNotFound()) {
          return {Status::RedisExecErr, s.ToString()};
        }

        break;
      }

      case RedisType::kRedisSet: {
        s = set_db.Card(args_[1], &vec_count);
        if (!s.ok() && !s.IsNotFound()) {
          return {Status::RedisExecErr, s.ToString()};
        }

        break;
      }

      case RedisType::kRedisZSet: {
        s = zset_db.Card(args_[1], &vec_count);
        if (!s.ok() && !s.IsNotFound()) {
          return {Status::RedisExecErr, s.ToString()};
        }
        break;
      }

      default:
        vec_count = 0;
        return {Status::RedisExecErr, "Bad SORT type"};
    }

    long vectorlen = (long)vec_count;

    // Adjust the offset and count of the limit
    long offset = offset_ >= vectorlen ? 0 : std::clamp(offset_, 0L, vectorlen - 1);
    long count = offset_ >= vectorlen ? 0 : std::clamp(count_, -1L, vectorlen - offset);
    if (count == -1L) count = vectorlen - offset;

    // Get the elements that need to be sorted
    std::vector<std::string> str_vec;
    if (count != 0) {
      if (type == RedisType::kRedisList && dontsort_) {
        if (desc_) {
          list_db.Range(args_[1], -count - offset, -1 - offset, &str_vec);
          std::reverse(str_vec.begin(), str_vec.end());
        } else {
          list_db.Range(args_[1], offset, offset + count - 1, &str_vec);
        }
      } else if (type == RedisType::kRedisList) {
        list_db.Range(args_[1], 0, -1, &str_vec);
      } else if (type == RedisType::kRedisSet) {
        set_db.Members(args_[1], &str_vec);
        if (dontsort_) {
          str_vec = std::vector(str_vec.begin() + offset, str_vec.begin() + offset + count);
        }
      } else if (type == RedisType::kRedisZSet && dontsort_) {
        std::vector<MemberScore> member_scores;
        RangeRankSpec spec;
        spec.start = (int)offset;
        spec.stop = (int)(offset + count - 1);
        spec.reversed = desc_;
        zset_db.RangeByRank(args_[1], spec, &member_scores, nullptr);
        for (size_t i = 0; i < member_scores.size(); ++i) {
          str_vec.emplace_back(member_scores[i].member);
        }
      } else if (type == RedisType::kRedisZSet) {
        std::vector<MemberScore> member_scores;
        zset_db.GetAllMemberScores(args_[1], &member_scores);
        for (size_t i = 0; i < member_scores.size(); ++i) {
          str_vec.emplace_back(member_scores[i].member);
        }
      } else {
        return {Status::RedisExecErr, "Unknown type"};
      }
    }

    std::vector<RedisSortObject> sort_vec(str_vec.size());
    for (size_t i = 0; i < str_vec.size(); ++i) {
      sort_vec[i].obj = str_vec[i];
    }

    // Sort by BY, ALPHA, ASC/DESC
    if (!dontsort_) {
      for (size_t i = 0; i < sort_vec.size(); ++i) {
        std::string byval;
        if (!sortby_.empty()) {
          byval = lookupKeyByPattern(srv, conn, sortby_, str_vec[i]);
          if (byval.empty()) continue;
        } else {
          byval = str_vec[i];
        }

        if (alpha_) {
          if (!sortby_.empty()) {
            sort_vec[i].v = byval;
          }
        } else {
          try {
            sort_vec[i].v = std::stod(byval);
          } catch (const std::exception &e) {
            *output = redis::Error("One or more scores can't be converted into double");
            return Status::OK();
          }
        }
      }

      std::sort(sort_vec.begin(), sort_vec.end(),
                [this](const RedisSortObject &a, const RedisSortObject &b) { return sortCompare(a, b); });

      // Gets the element specified by Limit
      if (offset != 0 || count != vectorlen) {
        sort_vec = std::vector(sort_vec.begin() + offset, sort_vec.begin() + offset + count);
      }
    }

    // Get the output and perform storage
    std::vector<std::string> output_vec;

    for (size_t i = 0; i < sort_vec.size(); ++i) {
      if (getpatterns_.empty()) {
        output_vec.emplace_back(sort_vec[i].obj);
      }
      for (const std::string &pattern : getpatterns_) {
        std::string val = lookupKeyByPattern(srv, conn, pattern, sort_vec[i].obj);
        if (val.empty()) {
          output_vec.emplace_back(conn->NilString());
        } else {
          output_vec.emplace_back(val);
        }
      }
    }

    if (storekey_.empty()) {
      *output = ArrayOfBulkStrings(output_vec);
    } else {
      std::vector<Slice> elems(output_vec.begin(), output_vec.end());
      list_db.Trim(storekey_, 0, -1);
      uint64_t new_size = 0;
      list_db.Push(storekey_, elems, false, &new_size);
      *output = Integer(new_size);
    }

    return Status::OK();
  }

 private:
  struct RedisSortObject {
    std::string obj;
    std::variant<double, std::string> v;
  };

  bool sortCompare(const RedisSortObject &a, const RedisSortObject &b) const {
    if (!alpha_) {
      double score_a = std::get<double>(a.v);
      double score_b = std::get<double>(b.v);
      return !desc_ ? score_a < score_b : score_a > score_b;
    } else {
      if (!sortby_.empty()) {
        std::string cmp_a = std::get<std::string>(a.v);
        std::string cmp_b = std::get<std::string>(b.v);
        return !desc_ ? cmp_a < cmp_b : cmp_a > cmp_b;
      } else {
        return !desc_ ? a.obj < b.obj : a.obj > b.obj;
      }
    }
  }

  static std::string lookupKeyByPattern(Server *srv, Connection *conn, const std::string &pattern,
                                        const std::string &subst) {
    if (pattern == "#") {
      return subst;
    }

    auto match_pos = pattern.find('*');
    if (match_pos == std::string::npos) {
      return "";
    }

    // hash field
    std::string field;
    auto arrow_pos = pattern.find("->", match_pos + 1);
    if (arrow_pos != std::string::npos && arrow_pos + 2 < pattern.size()) {
      field = pattern.substr(arrow_pos + 2);
    }

    std::string key = pattern.substr(0, match_pos + 1);
    key.replace(match_pos, 1, subst);

    std::string value;
    if (!field.empty()) {
      auto hash_db = redis::Hash(srv->storage, conn->GetNamespace());
      RedisType type = RedisType::kRedisNone;
      if (auto s = hash_db.Type(key, &type); !s.ok() || type >= RedisTypeNames.size()) {
        return "";
      }

      hash_db.Get(key, field, &value);
    } else {
      auto string_db = redis::String(srv->storage, conn->GetNamespace());
      RedisType type = RedisType::kRedisNone;
      if (auto s = string_db.Type(key, &type); !s.ok() || type >= RedisTypeNames.size()) {
        return "";
      }
      string_db.Get(key, &value);
    }
    return value;
  }

  std::string sortby_;                    // BY
  bool dontsort_ = false;                 // DONT SORT
  long offset_ = 0;                       // LIMIT OFFSET
  long count_ = -1;                       // LIMIT COUNT
  std::vector<std::string> getpatterns_;  // GET
  bool desc_ = false;                     // ASC/DESC
  bool alpha_ = false;                    // ALPHA
  std::string storekey_;                  // STORE
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandSort<false>>("sort", -2, "write deny-oom movable-keys", 1, 1, 1),
                        MakeCmdAttr<CommandSort<true>>("sort_ro", -2, "read-only movable-keys", 1, 1, 1))

}  // namespace redis