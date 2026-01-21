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

#include "redis_hash.h"

#include <rocksdb/status.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <random>
#include <utility>

#include "_deps/rocksdb-src/util/string_util.h"
#include "db_util.h"
#include "encoding.h"
#include "parse_util.h"
#include "rocksdb/slice.h"
#include "sample_helper.h"
#include "storage/redis_metadata.h"
#include "time_util.h"

namespace redis {

rocksdb::Status Hash::GetMetadata(engine::Context &ctx, const Slice &ns_key, HashMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisHash}, ns_key, metadata);
}

rocksdb::Status Hash::Size(engine::Context &ctx, const Slice &user_key, uint64_t *size) {
  *size = 0;
  std::vector<FieldValue> fields;
  auto s = this->GetAll(ctx, user_key, &fields, HashFetchType::kOnlyKey);
  if (!s.ok()) {
    return s;
  }
  *size = fields.size();
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Get(engine::Context &ctx, const Slice &user_key, const Slice &field, std::string *value) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;
  rocksdb::ReadOptions read_options;
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, value);
  if (!s.ok()) return s;
  uint64_t expire_at = NoExpireTime;
  s = GetSubKeyExpireTimestampMS(ctx, user_key, field, metadata.version, &expire_at);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (expire_at != NoExpireTime && expire_at < util::GetTimeStampMS()) {
    return rocksdb::Status::NotFound();
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::IncrBy(engine::Context &ctx, const Slice &user_key, const Slice &field, int64_t increment,
                             int64_t *new_value) {
  bool exists = false;
  int64_t old_value = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    std::string value_bytes;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      uint64_t expire_at = NoExpireTime;
      s = GetSubKeyExpireTimestampMS(ctx, user_key, field, metadata.version, &expire_at);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (expire_at != NoExpireTime && expire_at < util::GetTimeStampMS()) {
        old_value = 0;
      } else {
        auto parse_result = ParseInt<int64_t>(value_bytes, 10);
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument(parse_result.Msg());
        }
        if (isspace(value_bytes[0])) {
          return rocksdb::Status::InvalidArgument("value is not an integer");
        }
        old_value = *parse_result;
        exists = true;
      }
    }
  }
  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN - old_value)) ||
      (increment > 0 && old_value > 0 && increment > (LLONG_MAX - old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *new_value = old_value + increment;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  s = batch->Put(sub_key, std::to_string(*new_value));
  if (!s.ok()) return s;
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::IncrByFloat(engine::Context &ctx, const Slice &user_key, const Slice &field, double increment,
                                  double *new_value) {
  bool exists = false;
  double old_value = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    std::string value_bytes;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      uint64_t expire_at = NoExpireTime;
      s = GetSubKeyExpireTimestampMS(ctx, user_key, field, metadata.version, &expire_at);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (expire_at != NoExpireTime && expire_at < util::GetTimeStampMS()) {
        old_value = 0;
      } else {
        auto value_stat = ParseFloat(value_bytes);
        if (!value_stat || isspace(value_bytes[0])) {
          return rocksdb::Status::InvalidArgument("value is not a number");
        }
        old_value = *value_stat;
        exists = true;
      }
    }
  }
  double n = old_value + increment;
  if (std::isinf(n) || std::isnan(n)) {
    return rocksdb::Status::InvalidArgument("increment would produce NaN or Infinity");
  }

  *new_value = n;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  s = batch->Put(sub_key, std::to_string(*new_value));
  if (!s.ok()) return s;
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::MGet(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                           std::vector<std::string> *values, std::vector<rocksdb::Status> *statuses) {
  values->clear();
  statuses->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }

  rocksdb::ReadOptions read_options = ctx.DefaultMultiGetOptions();
  std::vector<rocksdb::Slice> keys;

  keys.reserve(fields.size());
  std::vector<std::string> sub_keys;
  sub_keys.resize(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    auto &field = fields[i];
    sub_keys[i] = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    keys.emplace_back(sub_keys[i]);
  }

  std::vector<uint64_t> expire_ats(fields.size(), NoExpireTime);
  std::vector<rocksdb::Status> expire_statuses(fields.size(), rocksdb::Status::OK());
  MGetSubKeyExpireTimestampMS(ctx, user_key, fields, metadata.version, &expire_ats, &expire_statuses);
  std::vector<rocksdb::PinnableSlice> values_vector;
  values_vector.resize(keys.size());
  std::vector<rocksdb::Status> statuses_vector;
  statuses_vector.resize(keys.size());
  storage_->MultiGet(ctx, read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                     values_vector.data(), statuses_vector.data());
  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses_vector[i].ok() && !statuses_vector[i].IsNotFound()) return statuses_vector[i];
    if (!expire_statuses[i].ok() && !expire_statuses[i].IsNotFound()) return expire_statuses[i];
    if (statuses_vector[i].ok() && expire_ats[i] != 0 && expire_ats[i] < util::GetTimeStampMS()) {
      values->emplace_back("");
      statuses->emplace_back(rocksdb::Status::NotFound());
      continue;
    }
    values->emplace_back(values_vector[i].ToString());
    statuses->emplace_back(statuses_vector[i]);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Set(engine::Context &ctx, const Slice &user_key, const Slice &field, const Slice &value,
                          uint64_t *added_cnt) {
  return MSet(ctx, user_key, {{field.ToString(), value.ToString()}}, false, added_cnt);
}

rocksdb::Status Hash::Delete(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                             uint64_t *deleted_cnt) {
  *deleted_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata(false);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string value;
  std::unordered_set<std::string_view> field_set;
  for (const auto &field : fields) {
    if (!field_set.emplace(field.ToStringView()).second) {
      continue;
    }
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    if (s.ok()) {
      std::string sub_key_expire =
          InternalKey(ns_key, field, metadata.version + ExpireVersionOffset, storage_->IsSlotIdEncoded()).Encode();
      uint64_t old_expired_timestamp = NoExpireTime;
      s = GetSubKeyExpireTimestampMS(ctx, user_key, field, metadata.version, &old_expired_timestamp);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (old_expired_timestamp == NoExpireTime || old_expired_timestamp > util::GetTimeStampMS()) {
        *deleted_cnt += 1;
      }
      if (s.ok()) {
        s = batch->Delete(sub_key_expire);
        if (!s.ok()) return s;
      }
      s = batch->Delete(sub_key);
      if (!s.ok()) return s;
    }
  }
  if (*deleted_cnt == 0) {
    return rocksdb::Status::OK();
  }
  metadata.size -= *deleted_cnt;
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::MSet(engine::Context &ctx, const Slice &user_key, const std::vector<FieldValue> &field_values,
                           bool nx, uint64_t *added_cnt, uint64_t expire) {
  *added_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  bool ttl_updated = false;
  if (expire > 0 && metadata.expire != expire) {
    metadata.expire = expire;
    ttl_updated = true;
  }
  int added = 0;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  std::unordered_set<std::string_view> field_set;

  std::vector<rocksdb::Slice> keys;
  std::vector<std::string> keys_encoded;
  std::vector<std::string_view> values;
  keys.reserve(field_values.size());
  values.reserve(field_values.size());
  for (auto it = field_values.rbegin(); it != field_values.rend(); it++) {
    if (!field_set.insert(it->field).second) {
      continue;
    }

    keys_encoded.push_back(InternalKey(ns_key, it->field, metadata.version, storage_->IsSlotIdEncoded()).Encode());
    keys.emplace_back(keys_encoded.back());
    values.emplace_back(it->value);
  }

  std::vector<rocksdb::PinnableSlice> values_vector(keys.size());
  std::vector<rocksdb::Status> statuses_vector(keys.size());
  if (metadata.size > 0) {
    rocksdb::ReadOptions read_options = ctx.DefaultMultiGetOptions();
    storage_->MultiGet(ctx, read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                       values_vector.data(), statuses_vector.data());
  }

  std::vector<uint64_t> expire_ats(field_values.size(), NoExpireTime);
  std::vector<rocksdb::Status> expire_statuses(field_values.size(), rocksdb::Status::OK());
  MGetSubKeyExpireTimestampMS(ctx, user_key, keys, metadata.version, &expire_ats, &expire_statuses);

  for (size_t field_index = 0; field_index < keys.size(); field_index++) {
    const rocksdb::Slice field_key = keys[field_index];
    bool exists = false;

    if (metadata.size > 0) {
      rocksdb::Status &field_status = statuses_vector[field_index];
      if (!field_status.ok() && !field_status.IsNotFound()) {
        return field_status;
      }
      if (field_status.ok()) {
        if (nx || values_vector[field_index] == values[field_index]) {
          continue;
        }
        exists = true;
      }
    }

    if (!exists) {
      added++;
    }

    s = batch->Put(field_key, values[field_index]);
    if (!s.ok()) return s;
    uint64_t expire_at = expire_ats[field_index];
    rocksdb::Status &expire_status = expire_statuses[field_index];
    if (!expire_status.ok() && !expire_status.IsNotFound()) {
      return expire_status;
    }
    // this key was once expired but the expire time exists may block the read
    if (expire_at != NoExpireTime && expire_at < util::GetTimeStampMS()) {
      auto sub_key_expire = GetSubKeyExpireInternalKey(user_key, field_key, metadata.version);
      s = batch->Delete(sub_key_expire);
      if (!s.ok()) return s;
    }
  }

  if (added > 0 || ttl_updated) {
    *added_cnt = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::RangeByLex(engine::Context &ctx, const Slice &user_key, const RangeLexSpec &spec,
                                 std::vector<FieldValue> *field_values) {
  field_values->clear();
  if (spec.count == 0) {
    return rocksdb::Status::OK();
  }
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string start_member = spec.reversed ? spec.max : spec.min;
  std::string start_key = InternalKey(ns_key, start_member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  if (!spec.reversed) {
    iter->Seek(start_key);
  } else {
    if (spec.max_infinite) {
      iter->SeekToLast();
    } else {
      iter->SeekForPrev(start_key);
    }
  }
  int64_t pos = 0;
  for (; iter->Valid() && iter->key().starts_with(prefix_key); (!spec.reversed ? iter->Next() : iter->Prev())) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    if (spec.reversed) {
      if (ikey.GetSubKey().ToString() < spec.min || (spec.minex && ikey.GetSubKey().ToString() == spec.min)) {
        break;
      }
      if ((spec.maxex && ikey.GetSubKey().ToString() == spec.max) ||
          (!spec.max_infinite && ikey.GetSubKey().ToString() > spec.max)) {
        continue;
      }
    } else {
      if (spec.minex && ikey.GetSubKey().ToString() == spec.min) continue;  // the min member was exclusive
      if ((spec.maxex && ikey.GetSubKey().ToString() == spec.max) ||
          (!spec.max_infinite && ikey.GetSubKey().ToString() > spec.max))
        break;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;

    field_values->emplace_back(ikey.GetSubKey().ToString(), iter->value().ToString());
    if (spec.count > 0 && field_values->size() >= static_cast<unsigned>(spec.count)) break;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::GetAll(engine::Context &ctx, const Slice &user_key, std::vector<FieldValue> *field_values,
                             HashFetchType type) {
  field_values->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    uint64_t expire_at = NoExpireTime;
    s = GetSubKeyExpireTimestampMS(ctx, user_key, ikey.GetSubKey(), metadata.version, &expire_at);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    if (expire_at != NoExpireTime && expire_at < util::GetTimeStampMS()) {
      continue;
    }
    if (type == HashFetchType::kOnlyKey) {
      field_values->emplace_back(ikey.GetSubKey().ToString(), "");
    } else if (type == HashFetchType::kOnlyValue) {
      field_values->emplace_back("", iter->value().ToString());
    } else {
      field_values->emplace_back(ikey.GetSubKey().ToString(), iter->value().ToString());
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Scan(engine::Context &ctx, const Slice &user_key, const std::string &cursor, uint64_t limit,
                           const std::string &field_prefix, std::vector<std::string> *fields,
                           std::vector<std::string> *values) {
  return SubKeyScanner::Scan(ctx, kRedisHash, user_key, cursor, limit, field_prefix, fields, values);
}

rocksdb::Status Hash::RandField(engine::Context &ctx, const Slice &user_key, int64_t command_count,
                                std::vector<FieldValue> *field_values, HashFetchType type) {
  uint64_t count = (command_count >= 0) ? static_cast<uint64_t>(command_count) : static_cast<uint64_t>(-command_count);
  bool unique = (command_count >= 0);

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(/*generate_version=*/false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::vector<FieldValue> samples;
  // TODO: Getting all values in Hash might be heavy, consider lazy-loading these values later
  if (count == 0) return rocksdb::Status::OK();
  s = ExtractRandMemberFromSet<FieldValue>(
      unique, count,
      [this, user_key, type, &ctx](std::vector<FieldValue> *elements) {
        return this->GetAll(ctx, user_key, elements, type);
      },
      field_values);
  if (!s.ok()) {
    return s;
  }
  switch (type) {
    case HashFetchType::kAll:
      break;
    case HashFetchType::kOnlyKey: {
      // GetAll should only fetching the key, checking all the values is empty
      for (const FieldValue &value : *field_values) {
        CHECK(value.value.empty());
      }
      break;
    }
    case HashFetchType::kOnlyValue:
      UNREACHABLE();
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::ExpireFields(engine::Context &ctx, const Slice &user_key, uint64_t expireat_ms,
                                   const std::vector<Slice> &fields, std::vector<FieldExpireResult> *results,
                                   FieldExpireCondition condition) {
  results->clear();
  results->reserve(fields.size());

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    // Key doesn't exist - all fields don't exist
    results->assign(fields.size(), FieldExpireResult::kFieldNotFound);
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  auto current_time_ms = util::GetTimeStampMS();
  for (const auto &field : fields) {
    if (current_time_ms >= expireat_ms) {
      results->push_back(FieldExpireResult::kExpireWithPastTime);
      continue;
    }
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    std::string value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);

    if (s.IsNotFound()) {
      results->push_back(FieldExpireResult::kFieldNotFound);  // Field doesn't exist
      continue;
    }
    if (!s.ok()) return s;

    std::string sub_key_expire =
        InternalKey(ns_key, field, metadata.version + ExpireVersionOffset, storage_->IsSlotIdEncoded()).Encode();
    uint64_t old_expired_timestamp = NoExpireTime;
    s = GetSubKeyExpireTimestampMS(ctx, user_key, field, metadata.version, &old_expired_timestamp);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (old_expired_timestamp != NoExpireTime && old_expired_timestamp < util::GetTimeStampMS()) {
      results->push_back(FieldExpireResult::kFieldNotFound);  // Treat as non-existent
      continue;
    }

    switch (condition) {
      case FieldExpireCondition::kFieldNoExpireCondition:
        break;
      case FieldExpireCondition::kFieldExpireTimeNotExists:
        if (old_expired_timestamp != NoExpireTime) {
          // NX set expiration only when the field has no expiration.
          results->push_back(FieldExpireResult::kExpireNotSet);
          continue;
        }
        break;
      case FieldExpireCondition::kFieldExpireTimeExists:
        if (old_expired_timestamp == NoExpireTime) {
          // XX set expiration only when the field has an existing expiration.
          results->push_back(FieldExpireResult::kExpireNotSet);  // No existing expiration
          continue;
        }
        break;
      case FieldExpireCondition::kFieldExpireTimeGreaterThanInput:
        if (old_expired_timestamp != NoExpireTime && expireat_ms < old_expired_timestamp) {
          // GT set expiration only when the new expiration is
          // greater than current one.
          results->push_back(FieldExpireResult::kExpireNotSet);
          continue;
        }
        break;
      case FieldExpireCondition::kFieldExpireTimeLessThanInput:
        if (old_expired_timestamp != NoExpireTime && expireat_ms > old_expired_timestamp) {
          // LT set expiration only when the new expiration is
          // less than current one.
          results->push_back(FieldExpireResult::kExpireNotSet);
          continue;
        }
        break;
    }
    // set new expired time
    std::string expire_value;
    PutFixed64(&expire_value, expireat_ms);
    s = batch->Put(sub_key_expire, expire_value);
    if (!s.ok()) return s;
    results->push_back(FieldExpireResult::kExpireSet);  // Expiration set successfully
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::TTLFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                std::vector<int64_t> *results) {
  results->clear();
  results->reserve(fields.size());

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    // Key doesn't exist - all fields don't exist
    results->assign(fields.size(), -2);
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }
  for (const auto &field : fields) {
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    std::string value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    if (s.IsNotFound()) {
      results->push_back(-2);  // Field doesn't exist
      continue;
    }
    if (!s.ok()) return s;

    std::string sub_key_expire =
        InternalKey(ns_key, field, metadata.version + ExpireVersionOffset, storage_->IsSlotIdEncoded()).Encode();
    uint64_t expired_time = 0;
    s = GetSubKeyExpireTimestampMS(ctx, user_key, field, metadata.version, &expired_time);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (expired_time == NoExpireTime) {
      results->push_back(-1);
      continue;
    }
    auto ttl_ms = static_cast<int64_t>(expired_time);
    results->push_back(ttl_ms > 0 ? ttl_ms : -2);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::PersistFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                    std::vector<FieldPersistResult> *results) {
  results->clear();
  results->reserve(fields.size());

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    // Key doesn't exist - all fields don't exist
    results->assign(fields.size(), FieldPersistResult::kFieldNotFound);
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  bool has_updates = false;
  for (const auto &field : fields) {
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    std::string raw_value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &raw_value);
    if (s.IsNotFound()) {
      results->push_back(FieldPersistResult::kFieldNotFound);  // Field doesn't exist
      continue;
    }
    if (!s.ok()) return s;
    std::string sub_key_expire =
        InternalKey(ns_key, field, metadata.version + ExpireVersionOffset, storage_->IsSlotIdEncoded()).Encode();
    uint64_t old_expired_timestamp = 0;
    s = GetSubKeyExpireTimestampMS(ctx, user_key, field, metadata.version, &old_expired_timestamp);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      s = batch->Delete(sub_key_expire);
      if (!s.ok()) return s;
    }
    has_updates = true;
    if (old_expired_timestamp == NoExpireTime) {
      results->push_back(FieldPersistResult::kNotVolatile);
      continue;
    } else if (old_expired_timestamp < util::GetTimeStampMS()) {
      results->push_back(FieldPersistResult::kFieldNotFound);  // Expired treat as non-existent
      continue;
    }
    results->push_back(FieldPersistResult::kPersisted);  // Expiration removed successfully
  }

  if (has_updates) {
    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}
}  // namespace redis
