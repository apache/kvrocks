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

const double kMinScore = (std::numeric_limits<float>::is_iec559 ? -std::numeric_limits<double>::infinity()
                                                                : std::numeric_limits<double>::lowest());
const double kMaxScore = (std::numeric_limits<float>::is_iec559 ? std::numeric_limits<double>::infinity()
                                                                : std::numeric_limits<double>::max());

struct ZRangeSpec {
  double min = kMinScore, max = kMaxScore;
  bool minex = false, maxex = false; /* are min or max exclusive */
  int offset = -1, count = -1;
  bool removed = false, reversed = false;
  ZRangeSpec() = default;
};

struct KeyWeight {
  std::string key;
  double weight;
};

struct MemberScore {
  std::string member;
  double score;
};

enum ZSetFlags {
  kZSetIncr = 1,
  kZSetNX = 1 << 1,
  kZSetXX = 1 << 2,
  kZSetReversed = 1 << 3,
  kZSetRemoved = 1 << 4,
  kZSetGT = 1 << 5,
  kZSetLT = 1 << 6,
  kZSetCH = 1 << 7,
};

class ZAddFlags {
 public:
  explicit ZAddFlags(uint8_t flags = 0) : flags(flags) {}

  bool HasNX() const { return (flags & kZSetNX) != 0; }
  bool HasXX() const { return (flags & kZSetXX) != 0; }
  bool HasLT() const { return (flags & kZSetLT) != 0; }
  bool HasGT() const { return (flags & kZSetGT) != 0; }
  bool HasCH() const { return (flags & kZSetCH) != 0; }
  bool HasIncr() const { return (flags & kZSetIncr) != 0; }
  bool HasAnyFlags() const { return flags != 0; }

  void SetFlag(ZSetFlags setFlags) { flags |= setFlags; }

  static ZAddFlags Incr() { return ZAddFlags{kZSetIncr}; }

  static ZAddFlags Default() { return ZAddFlags{0}; }

 private:
  uint8_t flags = 0;
};

namespace Redis {

class ZSet : public SubKeyScanner {
 public:
  explicit ZSet(Engine::Storage *storage, const std::string &ns)
      : SubKeyScanner(storage, ns), score_cf_handle_(storage->GetCFHandle("zset_score")) {}
  rocksdb::Status Add(const Slice &user_key, ZAddFlags flags, std::vector<MemberScore> *mscores, int *ret);
  rocksdb::Status Card(const Slice &user_key, int *ret);
  rocksdb::Status Count(const Slice &user_key, const ZRangeSpec &spec, int *ret);
  rocksdb::Status IncrBy(const Slice &user_key, const Slice &member, double increment, double *score);
  rocksdb::Status Range(const Slice &user_key, int start, int stop, uint8_t flags, std::vector<MemberScore> *mscores);
  rocksdb::Status RangeByScore(const Slice &user_key, ZRangeSpec spec, std::vector<MemberScore> *mscores, int *size);
  rocksdb::Status RangeByLex(const Slice &user_key, const CommonRangeLexSpec &spec, std::vector<std::string> *members,
                             int *size);
  rocksdb::Status Rank(const Slice &user_key, const Slice &member, bool reversed, int *ret);
  rocksdb::Status Remove(const Slice &user_key, const std::vector<Slice> &members, int *ret);
  rocksdb::Status RemoveRangeByScore(const Slice &user_key, ZRangeSpec spec, int *ret);
  rocksdb::Status RemoveRangeByLex(const Slice &user_key, CommonRangeLexSpec spec, int *ret);
  rocksdb::Status RemoveRangeByRank(const Slice &user_key, int start, int stop, int *ret);
  rocksdb::Status Pop(const Slice &user_key, int count, bool min, std::vector<MemberScore> *mscores);
  rocksdb::Status Score(const Slice &user_key, const Slice &member, double *score);
  static Status ParseRangeSpec(const std::string &min, const std::string &max, ZRangeSpec *spec);
  rocksdb::Status Scan(const Slice &user_key, const std::string &cursor, uint64_t limit,
                       const std::string &member_prefix, std::vector<std::string> *members,
                       std::vector<double> *scores = nullptr);
  rocksdb::Status Overwrite(const Slice &user_key, const std::vector<MemberScore> &mscores);
  rocksdb::Status InterStore(const Slice &dst, const std::vector<KeyWeight> &keys_weights,
                             AggregateMethod aggregate_method, int *size);
  rocksdb::Status UnionStore(const Slice &dst, const std::vector<KeyWeight> &keys_weights,
                             AggregateMethod aggregate_method, int *size);
  rocksdb::Status MGet(const Slice &user_key, const std::vector<Slice> &members,
                       std::map<std::string, double> *mscores);

  rocksdb::Status GetMetadata(const Slice &ns_key, ZSetMetadata *metadata);

 private:
  rocksdb::ColumnFamilyHandle *score_cf_handle_;
};

}  // namespace Redis
