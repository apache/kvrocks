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

#pragma once

#include <limits>
#include <map>
#include <string>
#include <vector>

#include "common/range_spec.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

enum AggregateMethod { kAggregateSum, kAggregateMin, kAggregateMax };

struct KeyWeight {
  std::string key;
  double weight;
};

struct MemberScore {
  std::string member;
  double score;
};

enum ZRangeType {
  kZRangeAuto,
  kZRangeRank,
  kZRangeScore,
  kZRangeLex,
};

enum ZRangeDirection {
  kZRangeDirectionAuto,
  kZRangeDirectionForward,
  kZRangeDirectionReverse,
};

enum ZSetFlags {
  kZSetIncr = 1,
  kZSetNX = 1 << 1,
  kZSetXX = 1 << 2,
  kZSetGT = 1 << 3,
  kZSetLT = 1 << 4,
  kZSetCH = 1 << 5,
};

class ZAddFlags {
 public:
  explicit ZAddFlags(uint8_t flags = 0) : flags_(flags) {}

  bool HasNX() const { return (flags_ & kZSetNX) != 0; }
  bool HasXX() const { return (flags_ & kZSetXX) != 0; }
  bool HasLT() const { return (flags_ & kZSetLT) != 0; }
  bool HasGT() const { return (flags_ & kZSetGT) != 0; }
  bool HasCH() const { return (flags_ & kZSetCH) != 0; }
  bool HasIncr() const { return (flags_ & kZSetIncr) != 0; }
  bool HasAnyFlags() const { return flags_ != 0; }

  void SetFlag(ZSetFlags set_flags) { flags_ |= set_flags; }

  static ZAddFlags Incr() { return ZAddFlags{kZSetIncr}; }

  static ZAddFlags Default() { return ZAddFlags{0}; }

 private:
  uint8_t flags_ = 0;
};

namespace redis {

class ZSet : public SubKeyScanner {
 public:
  explicit ZSet(engine::Storage *storage, const std::string &ns)
      : SubKeyScanner(storage, ns), score_cf_handle_(storage->GetCFHandle(ColumnFamilyID::SecondarySubkey)) {}

  using Members = std::vector<std::string>;
  using MemberScores = std::vector<MemberScore>;

  rocksdb::Status Add(const Slice &user_key, ZAddFlags flags, MemberScores *mscores, uint64_t *added_cnt);
  rocksdb::Status Card(const Slice &user_key, uint64_t *size);
  rocksdb::Status IncrBy(const Slice &user_key, const Slice &member, double increment, double *score);
  rocksdb::Status Rank(const Slice &user_key, const Slice &member, bool reversed, int *member_rank,
                       double *member_score);
  rocksdb::Status Remove(const Slice &user_key, const std::vector<Slice> &members, uint64_t *removed_cnt);
  rocksdb::Status Pop(const Slice &user_key, int count, bool min, MemberScores *mscores);
  rocksdb::Status Score(const Slice &user_key, const Slice &member, double *score);
  rocksdb::Status Scan(const Slice &user_key, const std::string &cursor, uint64_t limit,
                       const std::string &member_prefix, std::vector<std::string> *members,
                       std::vector<double> *scores = nullptr);
  rocksdb::Status Overwrite(const Slice &user_key, const MemberScores &mscores);
  rocksdb::Status InterStore(const Slice &dst, const std::vector<KeyWeight> &keys_weights,
                             AggregateMethod aggregate_method, uint64_t *saved_cnt);
  rocksdb::Status Inter(const std::vector<KeyWeight> &keys_weights, AggregateMethod aggregate_method,
                        std::vector<MemberScore> *members);
  rocksdb::Status InterCard(const std::vector<std::string> &user_keys, uint64_t limit, uint64_t *inter_cnt);
  rocksdb::Status UnionStore(const Slice &dst, const std::vector<KeyWeight> &keys_weights,
                             AggregateMethod aggregate_method, uint64_t *saved_cnt);
  rocksdb::Status Union(const std::vector<KeyWeight> &keys_weights, AggregateMethod aggregate_method,
                        std::vector<MemberScore> *members);
  rocksdb::Status Diff(const std::vector<Slice> &keys, MemberScores *members);
  rocksdb::Status DiffStore(const Slice &dst, const std::vector<Slice> &keys, uint64_t *stored_count);
  rocksdb::Status MGet(const Slice &user_key, const std::vector<Slice> &members, std::map<std::string, double> *scores);
  rocksdb::Status GetMetadata(Database::GetOptions get_options, const Slice &ns_key, ZSetMetadata *metadata);

  rocksdb::Status Count(const Slice &user_key, const RangeScoreSpec &spec, uint64_t *size);
  rocksdb::Status RangeByRank(const Slice &user_key, const RangeRankSpec &spec, MemberScores *mscores,
                              uint64_t *removed_cnt);
  rocksdb::Status RangeByScore(const Slice &user_key, const RangeScoreSpec &spec, MemberScores *mscores,
                               uint64_t *removed_cnt);
  rocksdb::Status RangeByLex(const Slice &user_key, const RangeLexSpec &spec, MemberScores *mscores,
                             uint64_t *removed_cnt);
  rocksdb::Status GetAllMemberScores(const Slice &user_key, std::vector<MemberScore> *member_scores);
  rocksdb::Status RandMember(const Slice &user_key, int64_t command_count, std::vector<MemberScore> *member_scores);

 private:
  rocksdb::ColumnFamilyHandle *score_cf_handle_;
};

}  // namespace redis
