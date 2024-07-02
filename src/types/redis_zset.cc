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

#include "redis_zset.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>

#include "db_util.h"
#include "sample_helper.h"
#include "storage/redis_metadata.h"

namespace redis {

rocksdb::Status ZSet::GetMetadata(Database::GetOptions get_options, const Slice &ns_key, ZSetMetadata *metadata) {
  return Database::GetMetadata(get_options, {kRedisZSet}, ns_key, metadata);
}

rocksdb::Status ZSet::Add(const Slice &user_key, ZAddFlags flags, MemberScores *mscores, uint64_t *added_cnt) {
  *added_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(Database::GetOptions{}, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  int added = 0;
  int changed = 0;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());
  std::unordered_set<std::string_view> added_member_keys;
  for (auto it = mscores->rbegin(); it != mscores->rend(); ++it) {
    if (!added_member_keys.insert(it->member).second) {
      continue;
    }
    std::string member_key = InternalKey(ns_key, it->member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    if (metadata.size > 0) {
      std::string old_score_bytes;
      s = storage_->Get(rocksdb::ReadOptions(), member_key, &old_score_bytes);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (s.ok()) {
        if (!s.IsNotFound() && flags.HasNX()) {
          continue;
        }
        double old_score = DecodeDouble(old_score_bytes.data());
        if (flags.HasIncr()) {
          if ((flags.HasLT() && it->score >= 0) || (flags.HasGT() && it->score <= 0)) {
            continue;
          }
          it->score += old_score;
          if (std::isnan(it->score)) {
            return rocksdb::Status::InvalidArgument("resulting score is not a number (NaN)");
          }
        }
        if (it->score != old_score) {
          if ((flags.HasLT() && it->score >= old_score) || (flags.HasGT() && it->score <= old_score)) {
            continue;
          }
          old_score_bytes.append(it->member);
          std::string old_score_key =
              InternalKey(ns_key, old_score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
          batch->Delete(score_cf_handle_, old_score_key);
          std::string new_score_bytes;
          PutDouble(&new_score_bytes, it->score);
          batch->Put(member_key, new_score_bytes);
          new_score_bytes.append(it->member);
          std::string new_score_key =
              InternalKey(ns_key, new_score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
          batch->Put(score_cf_handle_, new_score_key, Slice());
          changed++;
        }
        continue;
      }
    }
    if (flags.HasXX()) {
      continue;
    }
    std::string score_bytes;
    PutDouble(&score_bytes, it->score);
    batch->Put(member_key, score_bytes);
    score_bytes.append(it->member);
    std::string score_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    batch->Put(score_cf_handle_, score_key, Slice());
    added++;
  }
  if (added > 0) {
    *added_cnt = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  if (flags.HasCH()) {
    *added_cnt += changed;
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status ZSet::Card(const Slice &user_key, uint64_t *size) {
  *size = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(GetOptions{}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  *size = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Count(const Slice &user_key, const RangeScoreSpec &spec, uint64_t *size) {
  return RangeByScore(user_key, spec, nullptr, size);
}

rocksdb::Status ZSet::IncrBy(const Slice &user_key, const Slice &member, double increment, double *score) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  mscores.emplace_back(MemberScore{member.ToString(), increment});
  rocksdb::Status s = Add(user_key, ZAddFlags::Incr(), &mscores, &ret);
  if (!s.ok()) return s;
  *score = mscores[0].score;
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Pop(const Slice &user_key, int count, bool min, MemberScores *mscores) {
  mscores->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(GetOptions{}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  if (count <= 0) return rocksdb::Status::OK();
  if (count > static_cast<int>(metadata.size)) count = static_cast<int>(metadata.size);

  std::string score_bytes;
  double score = min ? kMinScore : kMaxScore;
  PutDouble(&score_bytes, score);
  std::string start_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(storage_, read_options, score_cf_handle_);
  iter->Seek(start_key);
  // see comment in RangeByScore()
  if (!min && (!iter->Valid() || !iter->key().starts_with(prefix_key))) {
    iter->SeekForPrev(start_key);
  }
  for (; iter->Valid() && iter->key().starts_with(prefix_key); min ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice score_key = ikey.GetSubKey();
    GetDouble(&score_key, &score);
    mscores->emplace_back(MemberScore{score_key.ToString(), score});
    std::string default_cf_key = InternalKey(ns_key, score_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    batch->Delete(default_cf_key);
    batch->Delete(score_cf_handle_, iter->key());
    if (mscores->size() >= static_cast<unsigned>(count)) break;
  }

  if (!mscores->empty()) {
    metadata.size -= mscores->size();
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status ZSet::RangeByRank(const Slice &user_key, const RangeRankSpec &spec, MemberScores *mscores,
                                  uint64_t *removed_cnt) {
  if (mscores) mscores->clear();

  uint64_t cnt = 0;
  if (!removed_cnt) removed_cnt = &cnt;
  *removed_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  std::optional<LockGuard> lock_guard;
  if (spec.with_deletion) lock_guard.emplace(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata(false);
  LatestSnapShot ss(storage_);
  rocksdb::Status s = GetMetadata(GetOptions{ss.GetSnapShot()}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  int start = spec.start;
  int stop = spec.stop;

  if (start < 0) start += static_cast<int>(metadata.size);
  if (stop < 0) stop += static_cast<int>(metadata.size);
  if (start < 0) start = 0;
  if (stop < 0 || start > stop) {
    return rocksdb::Status::OK();
  }

  std::string score_bytes;
  double score = !(spec.reversed) ? kMinScore : kMaxScore;
  PutDouble(&score_bytes, score);
  std::string start_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  int removed_subkey = 0;
  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto batch = storage_->GetWriteBatchBase();
  auto iter = util::UniqueIterator(storage_, read_options, score_cf_handle_);
  iter->Seek(start_key);
  // see comment in RangeByScore()
  if (spec.reversed && (!iter->Valid() || !iter->key().starts_with(prefix_key))) {
    iter->SeekForPrev(start_key);
  }

  int count = 0;
  for (; iter->Valid() && iter->key().starts_with(prefix_key); !(spec.reversed) ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice score_key = ikey.GetSubKey();
    GetDouble(&score_key, &score);
    if (count >= start) {
      if (spec.with_deletion) {
        std::string sub_key = InternalKey(ns_key, score_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
        batch->Delete(sub_key);
        batch->Delete(score_cf_handle_, iter->key());
        removed_subkey++;
      } else {
        if (mscores) mscores->emplace_back(MemberScore{score_key.ToString(), score});
      }
      *removed_cnt += 1;
    }
    if (count++ >= stop) break;
  }

  if (removed_subkey) {
    metadata.size -= removed_subkey;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::RangeByScore(const Slice &user_key, const RangeScoreSpec &spec, MemberScores *mscores,
                                   uint64_t *removed_cnt) {
  if (mscores) mscores->clear();

  uint64_t cnt = 0;
  if (!removed_cnt) removed_cnt = &cnt;
  *removed_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  std::optional<LockGuard> lock_guard;
  if (spec.with_deletion) lock_guard.emplace(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(GetOptions{}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  // let's get familiar with score first:
  //    a. score of zset's member is represented by double and it takes up 8 bytes in rocksdb
  //    b. to make positive double greater than native double in lexicographical order, score
  //       is required encoding before stored in rocksdb. encoding details see PutDouble()
  //    c. for convenience, user_score and inner_score respectively represent before and after encoding
  //
  // next lexicographical ordered inner_score of max:
  //    a. we can think of inner_score as a fixed 8-byte string. logically, the next lexicographical
  //       ordered inner_score of max_inner_score is 'max_inner_score + 1' if we assume no overflow.
  //       'max_inner_score + 1' means binary increment.
  //    b. realize binary increment 'max_inner_score + 1'
  //       use PutDouble() encoding max(max_user_score) to max_inner_score
  //       memcpy max_inner_score to u64(uint64_t)
  //       incr u64
  //       memcpy u64 to max_next_inner_score
  //    it may not be hard to understand about how to get max_next_inner_score
  //
  // directly generate max_next_user_score of max_next_inner_score:
  //    a. give a key argument first:
  //       for positive score, user_score is positively correlated with inner_score in lexicographical order
  //       for negative score, user_score is negatively correlated with inner_score in lexicographical order
  //       more details see PutDouble()
  //    b. get max_next_user_score of max_next_inner_score:
  //       for positive max_user_score, max_next_user_score is 'max_user_score + 1'
  //       for negative max_user_score, max_next_user_score is 'max_user_score - 1'
  // Note: fortunately, there is no overflow in fact. more details see binary encoding of double
  // binary encoding of double: https://en.wikipedia.org/wiki/Double-precision_floating-point_format

  // generate next possible score of max
  int64_t i64 = 0;
  double max_next_score = 0;
  if (spec.reversed && !spec.maxex) {
    memcpy(&i64, &spec.max, sizeof(spec.max));
    i64 = i64 >= 0 ? i64 + 1 : i64 - 1;
    memcpy(&max_next_score, &i64, sizeof(i64));
  }

  std::string start_score_bytes;
  PutDouble(&start_score_bytes, spec.reversed ? (spec.maxex ? spec.max : max_next_score) : spec.min);
  std::string start_key =
      InternalKey(ns_key, start_score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  int pos = 0;
  auto iter = util::UniqueIterator(storage_, read_options, score_cf_handle_);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());
  if (!spec.reversed) {
    iter->Seek(start_key);
  } else {
    iter->SeekForPrev(start_key);
    if (iter->Valid() && iter->key().starts_with(start_key)) {
      iter->Prev();
    }
  }

  for (; iter->Valid() && iter->key().starts_with(prefix_key); !spec.reversed ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice score_key = ikey.GetSubKey();
    double score = NAN;
    GetDouble(&score_key, &score);
    if (spec.reversed) {
      if ((spec.minex && score == spec.min) || score < spec.min) break;
      if ((spec.maxex && score == spec.max) || score > spec.max) continue;
    } else {
      if ((spec.minex && score == spec.min) || score < spec.min) continue;
      if ((spec.maxex && score == spec.max) || score > spec.max) break;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;
    if (spec.with_deletion) {
      std::string sub_key = InternalKey(ns_key, score_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
      batch->Delete(sub_key);
      batch->Delete(score_cf_handle_, iter->key());
    } else {
      if (mscores) mscores->emplace_back(MemberScore{score_key.ToString(), score});
    }
    *removed_cnt += 1;
    if (spec.count > 0 && mscores && mscores->size() >= static_cast<unsigned>(spec.count)) break;
  }

  if (spec.with_deletion && *removed_cnt > 0) {
    metadata.size -= *removed_cnt;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::RangeByLex(const Slice &user_key, const RangeLexSpec &spec, MemberScores *mscores,
                                 uint64_t *removed_cnt) {
  if (mscores) mscores->clear();

  uint64_t cnt = 0;
  if (!removed_cnt) removed_cnt = &cnt;
  *removed_cnt = 0;

  if (spec.offset > -1 && spec.count == 0) {
    return rocksdb::Status::OK();
  }

  std::string ns_key = AppendNamespacePrefix(user_key);

  std::optional<LockGuard> lock_guard;
  if (spec.with_deletion) {
    lock_guard.emplace(storage_->GetLockManager(), ns_key);
  }
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(GetOptions{}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string start_member = spec.reversed ? spec.max : spec.min;
  std::string start_key = InternalKey(ns_key, start_member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  int pos = 0;
  auto iter = util::UniqueIterator(storage_, read_options);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());

  if (!spec.reversed) {
    iter->Seek(start_key);
  } else {
    if (spec.max_infinite) {
      iter->SeekToLast();
    } else {
      iter->SeekForPrev(start_key);
    }
  }

  for (; iter->Valid() && iter->key().starts_with(prefix_key); (!spec.reversed ? iter->Next() : iter->Prev())) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice member = ikey.GetSubKey();
    if (spec.reversed) {
      if (member.ToString() < spec.min || (spec.minex && member == spec.min)) {
        break;
      }
      if ((spec.maxex && member == spec.max) || (!spec.max_infinite && member.ToString() > spec.max)) {
        continue;
      }
    } else {
      if (spec.minex && member == spec.min) continue;  // the min member was exclusive
      if ((spec.maxex && member == spec.max) || (!spec.max_infinite && member.ToString() > spec.max)) break;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;
    if (spec.with_deletion) {
      std::string score_bytes = iter->value().ToString();
      score_bytes.append(member.data(), member.size());
      std::string score_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
      batch->Delete(score_cf_handle_, score_key);
      batch->Delete(iter->key());
    } else {
      if (mscores) mscores->emplace_back(MemberScore{member.ToString(), DecodeDouble(iter->value().data())});
    }
    *removed_cnt += 1;
    if (spec.count > 0 && mscores && mscores->size() >= static_cast<unsigned>(spec.count)) break;
  }

  if (spec.with_deletion && *removed_cnt > 0) {
    metadata.size -= *removed_cnt;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Score(const Slice &user_key, const Slice &member, double *score) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(GetOptions{}, ns_key, &metadata);
  if (!s.ok()) return s;

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();

  std::string score_bytes;
  std::string member_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(read_options, member_key, &score_bytes);
  if (!s.ok()) return s;
  *score = DecodeDouble(score_bytes.data());
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Remove(const Slice &user_key, const std::vector<Slice> &members, uint64_t *removed_cnt) {
  *removed_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(GetOptions{}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());
  int removed = 0;
  std::unordered_set<std::string_view> mset;
  for (const auto &member : members) {
    if (!mset.insert(member.ToStringView()).second) {
      continue;
    }
    std::string member_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    std::string score_bytes;
    s = storage_->Get(rocksdb::ReadOptions(), member_key, &score_bytes);
    if (s.ok()) {
      score_bytes.append(member.data(), member.size());
      std::string score_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
      batch->Delete(member_key);
      batch->Delete(score_cf_handle_, score_key);
      removed++;
    }
  }
  if (removed > 0) {
    *removed_cnt = removed;
    metadata.size -= removed;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status ZSet::Rank(const Slice &user_key, const Slice &member, bool reversed, int *member_rank,
                           double *member_score) {
  *member_rank = -1;
  *member_score = 0.0;

  std::string ns_key = AppendNamespacePrefix(user_key);
  ZSetMetadata metadata(false);
  LatestSnapShot ss(storage_);
  rocksdb::Status s = GetMetadata(GetOptions{ss.GetSnapShot()}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = ss.GetSnapShot();
  std::string score_bytes;
  std::string member_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(read_options, member_key, &score_bytes);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  double target_score = DecodeDouble(score_bytes.data());
  std::string start_score_bytes;
  double start_score = !reversed ? kMinScore : kMaxScore;
  PutDouble(&start_score_bytes, start_score);
  std::string start_key =
      InternalKey(ns_key, start_score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  int rank = 0;
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(storage_, read_options, score_cf_handle_);
  iter->Seek(start_key);
  // see comment in RangeByScore()
  if (reversed && (!iter->Valid() || !iter->key().starts_with(prefix_key))) {
    iter->SeekForPrev(start_key);
  }
  for (; iter->Valid() && iter->key().starts_with(prefix_key); !reversed ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice score_key = ikey.GetSubKey();
    double score = NAN;
    GetDouble(&score_key, &score);
    if (score == target_score && score_key == member) break;
    rank++;
  }

  *member_rank = rank;
  *member_score = target_score;
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Overwrite(const Slice &user_key, const MemberScores &mscores) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());
  for (const auto &ms : mscores) {
    std::string score_bytes;
    std::string member_key = InternalKey(ns_key, ms.member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    PutDouble(&score_bytes, ms.score);
    batch->Put(member_key, score_bytes);
    score_bytes.append(ms.member);
    std::string score_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    batch->Put(score_cf_handle_, score_key, Slice());
  }
  metadata.size = static_cast<uint32_t>(mscores.size());
  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status ZSet::InterStore(const Slice &dst, const std::vector<KeyWeight> &keys_weights,
                                 AggregateMethod aggregate_method, uint64_t *saved_cnt) {
  *saved_cnt = 0;
  std::vector<MemberScore> members;
  auto s = Inter(keys_weights, aggregate_method, &members);
  if (!s.ok()) return s;
  *saved_cnt = members.size();
  return Overwrite(dst, members);
}

rocksdb::Status ZSet::Inter(const std::vector<KeyWeight> &keys_weights, AggregateMethod aggregate_method,
                            std::vector<MemberScore> *members) {
  std::vector<std::string> lock_keys;
  lock_keys.reserve(keys_weights.size());
  for (const auto &key_weight : keys_weights) {
    std::string ns_key = AppendNamespacePrefix(key_weight.key);
    lock_keys.emplace_back(std::move(ns_key));
  }
  MultiLockGuard guard(storage_->GetLockManager(), lock_keys);

  std::map<std::string, double> dst_zset;
  std::map<std::string, size_t> member_counters;
  std::vector<MemberScore> target_mscores;
  uint64_t target_size = 0;
  RangeScoreSpec spec;
  auto s = RangeByScore(keys_weights[0].key, spec, &target_mscores, &target_size);
  if (!s.ok() || target_mscores.empty()) return s;

  for (const auto &ms : target_mscores) {
    double score = ms.score * keys_weights[0].weight;
    if (std::isnan(score)) score = 0;
    dst_zset[ms.member] = score;
    member_counters[ms.member] = 1;
  }

  for (size_t i = 1; i < keys_weights.size(); i++) {
    s = RangeByScore(keys_weights[i].key, spec, &target_mscores, &target_size);
    if (!s.ok() || target_mscores.empty()) return s;

    for (const auto &ms : target_mscores) {
      if (dst_zset.find(ms.member) == dst_zset.end()) continue;
      member_counters[ms.member]++;
      double score = ms.score * keys_weights[i].weight;
      if (std::isnan(score)) score = 0;
      switch (aggregate_method) {
        case kAggregateSum:
          dst_zset[ms.member] += score;
          if (std::isnan(dst_zset[ms.member])) {
            dst_zset[ms.member] = 0;
          }
          break;
        case kAggregateMin:
          if (dst_zset[ms.member] > score) {
            dst_zset[ms.member] = score;
          }
          break;
        case kAggregateMax:
          if (dst_zset[ms.member] < score) {
            dst_zset[ms.member] = score;
          }
          break;
      }
    }
  }
  if (members && !dst_zset.empty()) {
    members->reserve(dst_zset.size());
    for (const auto &iter : dst_zset) {
      if (member_counters[iter.first] != keys_weights.size()) continue;
      members->emplace_back(MemberScore{iter.first, iter.second});
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::InterCard(const std::vector<std::string> &user_keys, uint64_t limit, uint64_t *inter_cnt) {
  std::vector<std::string> lock_keys;
  lock_keys.reserve(user_keys.size());
  for (const auto &user_key : user_keys) {
    std::string ns_key = AppendNamespacePrefix(user_key);
    lock_keys.emplace_back(std::move(ns_key));
  }
  MultiLockGuard guard(storage_->GetLockManager(), lock_keys);

  std::vector<MemberScores> mscores_list;
  mscores_list.reserve(user_keys.size());
  RangeScoreSpec spec;
  for (const auto &user_key : user_keys) {
    MemberScores mscores;
    auto s = RangeByScore(user_key, spec, &mscores, nullptr);
    if (!s.ok() || mscores.empty()) return s;
    mscores_list.emplace_back(std::move(mscores));
  }
  std::sort(mscores_list.begin(), mscores_list.end(),
            [](const MemberScores &v1, const MemberScores &v2) { return v1.size() < v2.size(); });

  auto base_mscores = mscores_list[0];
  std::map<std::string, size_t> member_counters;
  uint64_t cardinality = 0;
  for (const auto &base_ms : base_mscores) {
    member_counters[base_ms.member] = 1;
    for (size_t i = 1; i < mscores_list.size(); i++) {
      for (const auto &ms : mscores_list[i]) {
        if (base_ms.member == ms.member) {
          member_counters[ms.member]++;
          break;
        }
      }
    }
    if (member_counters[base_ms.member] == mscores_list.size()) {
      cardinality++;
      if (limit > 0 && cardinality >= limit) {
        *inter_cnt = limit;
        return rocksdb::Status::OK();
      };
    }
  }
  *inter_cnt = cardinality;
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::UnionStore(const Slice &dst, const std::vector<KeyWeight> &keys_weights,
                                 AggregateMethod aggregate_method, uint64_t *saved_cnt) {
  *saved_cnt = 0;
  std::vector<MemberScore> members;
  auto s = Union(keys_weights, aggregate_method, &members);
  if (!s.ok()) return s;
  *saved_cnt = members.size();
  return Overwrite(dst, members);
}

rocksdb::Status ZSet::Union(const std::vector<KeyWeight> &keys_weights, AggregateMethod aggregate_method,
                            std::vector<MemberScore> *members) {
  std::vector<std::string> lock_keys;
  lock_keys.reserve(keys_weights.size());
  for (const auto &key_weight : keys_weights) {
    std::string ns_key = AppendNamespacePrefix(key_weight.key);
    lock_keys.emplace_back(std::move(ns_key));
  }
  MultiLockGuard guard(storage_->GetLockManager(), lock_keys);

  std::map<std::string, double> dst_zset;
  std::vector<MemberScore> target_mscores;
  uint64_t target_size = 0;
  RangeScoreSpec spec;
  for (const auto &key_weight : keys_weights) {
    // get all member
    auto s = RangeByScore(key_weight.key, spec, &target_mscores, &target_size);
    if (!s.ok() && !s.IsNotFound()) return s;
    for (const auto &ms : target_mscores) {
      double score = ms.score * key_weight.weight;
      if (std::isnan(score)) score = 0;
      if (dst_zset.find(ms.member) == dst_zset.end()) {
        dst_zset[ms.member] = score;
      } else {
        switch (aggregate_method) {
          case kAggregateSum:
            dst_zset[ms.member] += score;
            if (std::isnan(dst_zset[ms.member])) dst_zset[ms.member] = 0;
            break;
          case kAggregateMin:
            if (dst_zset[ms.member] > score) {
              dst_zset[ms.member] = score;
            }
            break;
          case kAggregateMax:
            if (dst_zset[ms.member] < score) {
              dst_zset[ms.member] = score;
            }
            break;
        }
      }
    }
  }
  if (members && !dst_zset.empty()) {
    members->reserve(dst_zset.size());
    for (const auto &iter : dst_zset) {
      members->emplace_back(MemberScore{iter.first, iter.second});
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Scan(const Slice &user_key, const std::string &cursor, std::vector<std::string> *members,
                           const ScanConfig &scan_config, const BaseMatchType &match_mode,
                           std::vector<double> *scores) {
  if (scores != nullptr) {
    std::vector<std::string> values;
    auto s = SubKeyScanner::Scan(kRedisZSet, user_key, cursor, members, scan_config, match_mode, &values);
    if (!s.ok()) return s;

    for (const auto &value : values) {
      double target_score = DecodeDouble(value.data());
      scores->emplace_back(target_score);
    }
    return s;
  }
  return SubKeyScanner::Scan(kRedisZSet, user_key, cursor, members, scan_config, match_mode, nullptr);
}

rocksdb::Status ZSet::MGet(const Slice &user_key, const std::vector<Slice> &members,
                           std::map<std::string, double> *mscores) {
  mscores->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  ZSetMetadata metadata(false);
  LatestSnapShot ss(storage_);
  rocksdb::Status s = GetMetadata(GetOptions{ss.GetSnapShot()}, ns_key, &metadata);
  if (!s.ok()) return s;

  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string score_bytes;
  for (const auto &member : members) {
    std::string member_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    score_bytes.clear();
    s = storage_->Get(read_options, member_key, &score_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      continue;
    }
    double target_score = DecodeDouble(score_bytes.data());
    (*mscores)[member.ToString()] = target_score;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::GetAllMemberScores(const Slice &user_key, std::vector<MemberScore> *member_scores) {
  member_scores->clear();
  std::string ns_key = AppendNamespacePrefix(user_key);
  ZSetMetadata metadata(false);
  LatestSnapShot ss(storage_);
  rocksdb::Status s = GetMetadata(GetOptions{ss.GetSnapShot()}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = ss.GetSnapShot();

  rocksdb::Slice upper_bound(next_version_prefix_key);
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(storage_, read_options, score_cf_handle_);

  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice score_key = ikey.GetSubKey();
    double score = NAN;
    GetDouble(&score_key, &score);
    member_scores->emplace_back(MemberScore{score_key.ToString(), score});
  }

  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::RandMember(const Slice &user_key, int64_t command_count,
                                 std::vector<MemberScore> *member_scores) {
  if (command_count == 0) {
    return rocksdb::Status::OK();
  }

  uint64_t count = command_count > 0 ? static_cast<uint64_t>(command_count) : static_cast<uint64_t>(-command_count);
  bool unique = (command_count >= 0);

  std::string ns_key = AppendNamespacePrefix(user_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(GetOptions{}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  if (metadata.size == 0) return rocksdb::Status::OK();

  return ExtractRandMemberFromSet<MemberScore>(
      unique, count,
      [this, user_key](std::vector<MemberScore> *scores) -> rocksdb::Status {
        return this->GetAllMemberScores(user_key, scores);
      },
      member_scores);
}

rocksdb::Status ZSet::Diff(const std::vector<Slice> &keys, MemberScores *members) {
  members->clear();
  MemberScores source_member_scores;
  RangeScoreSpec spec;
  uint64_t first_element_size = 0;
  auto s = RangeByScore(keys[0], spec, &source_member_scores, &first_element_size);
  if (!s.ok()) return s;

  if (first_element_size == 0) {
    return rocksdb::Status::OK();
  }

  std::set<std::string> exclude_members;
  MemberScores target_member_scores;
  for (size_t i = 1; i < keys.size(); i++) {
    uint64_t size = 0;
    s = RangeByScore(keys[i], spec, &target_member_scores, &size);
    if (!s.ok()) return s;
    for (auto &member_score : target_member_scores) {
      exclude_members.emplace(std::move(member_score.member));
    }
    target_member_scores.clear();
  }
  for (const auto &member_score : source_member_scores) {
    if (exclude_members.find(member_score.member) == exclude_members.end()) {
      members->push_back(member_score);
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::DiffStore(const Slice &dst, const std::vector<Slice> &keys, uint64_t *stored_count) {
  MemberScores mscores;
  auto s = Diff(keys, &mscores);
  if (!s.ok()) return s;
  *stored_count = mscores.size();
  return Overwrite(dst, mscores);
}

}  // namespace redis
