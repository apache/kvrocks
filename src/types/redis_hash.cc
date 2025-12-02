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
#include <random>
#include <utility>

#include "db_util.h"
#include "parse_util.h"
#include "sample_helper.h"

namespace redis {

rocksdb::Status Hash::GetMetadata(engine::Context &ctx, const Slice &ns_key, HashMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisHash}, ns_key, metadata);
}

rocksdb::Status Hash::Size(engine::Context &ctx, const Slice &user_key, uint64_t *size) {
  *size = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;
  *size = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Get(engine::Context &ctx, const Slice &user_key, const Slice &field, std::string *value) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;
  rocksdb::ReadOptions read_options;
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string raw_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &raw_value);
  if (!s.ok()) return s;

  // Decode field value and check expiration
  HashFieldValue field_value;
  if (!HashFieldValue::Decode(raw_value, &field_value)) {
    return rocksdb::Status::Corruption("failed to decode hash field value");
  }
  if (field_value.IsExpired()) {
    return rocksdb::Status::NotFound();
  }
  *value = field_value.value.ToString();
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::IncrBy(engine::Context &ctx, const Slice &user_key, const Slice &field, int64_t increment,
                             int64_t *new_value) {
  bool exists = false;
  int64_t old_value = 0;
  uint64_t field_expire = 0;  // Preserve field expiration if any

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
      // Decode field value with potential expiration
      HashFieldValue field_value;
      if (!HashFieldValue::Decode(value_bytes, &field_value)) {
        return rocksdb::Status::Corruption("failed to decode hash field value");
      }
      // Treat expired field as non-existent
      if (field_value.IsExpired()) {
        s = rocksdb::Status::NotFound();
      } else {
        auto parse_result = ParseInt<int64_t>(field_value.value.ToString(), 10);
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument(parse_result.Msg());
        }
        if (!field_value.value.empty() && isspace(field_value.value[0])) {
          return rocksdb::Status::InvalidArgument("value is not an integer");
        }
        old_value = *parse_result;
        field_expire = field_value.expire;  // Preserve expiration
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

  // Encode new value preserving expiration
  HashFieldValue new_field_value(std::to_string(*new_value), field_expire);
  std::string encoded_value;
  new_field_value.Encode(&encoded_value);
  s = batch->Put(sub_key, encoded_value);
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
  uint64_t field_expire = 0;  // Preserve field expiration if any

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
      // Decode field value with potential expiration
      HashFieldValue field_value;
      if (!HashFieldValue::Decode(value_bytes, &field_value)) {
        return rocksdb::Status::Corruption("failed to decode hash field value");
      }
      // Treat expired field as non-existent
      if (field_value.IsExpired()) {
        s = rocksdb::Status::NotFound();
      } else {
        auto value_stat = ParseFloat(field_value.value.ToString());
        if (!value_stat || (!field_value.value.empty() && isspace(field_value.value[0]))) {
          return rocksdb::Status::InvalidArgument("value is not a number");
        }
        old_value = *value_stat;
        field_expire = field_value.expire;  // Preserve expiration
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

  // Encode new value preserving expiration
  HashFieldValue new_field_value(std::to_string(*new_value), field_expire);
  std::string encoded_value;
  new_field_value.Encode(&encoded_value);
  s = batch->Put(sub_key, encoded_value);
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

  std::vector<rocksdb::PinnableSlice> values_vector;
  values_vector.resize(keys.size());
  std::vector<rocksdb::Status> statuses_vector;
  statuses_vector.resize(keys.size());
  storage_->MultiGet(ctx, read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                     values_vector.data(), statuses_vector.data());
  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses_vector[i].ok() && !statuses_vector[i].IsNotFound()) return statuses_vector[i];

    if (statuses_vector[i].ok()) {
      // Decode field value and check expiration
      HashFieldValue field_value;
      if (!HashFieldValue::Decode(values_vector[i], &field_value)) {
        return rocksdb::Status::Corruption("failed to decode hash field value");
      }
      if (field_value.IsExpired()) {
        values->emplace_back("");
        statuses->emplace_back(rocksdb::Status::NotFound());
      } else {
        values->emplace_back(field_value.value.data(), field_value.value.size());
        statuses->emplace_back(statuses_vector[i]);
      }
    } else {
      values->emplace_back("");
      statuses->emplace_back(statuses_vector[i]);
    }
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
      *deleted_cnt += 1;
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
  for (auto it = field_values.rbegin(); it != field_values.rend(); it++) {
    if (!field_set.insert(it->field).second) {
      continue;
    }

    bool exists = false;
    std::string sub_key = InternalKey(ns_key, it->field, metadata.version, storage_->IsSlotIdEncoded()).Encode();

    if (metadata.size > 0) {
      std::string field_value;
      s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &field_value);
      if (!s.ok() && !s.IsNotFound()) return s;

      if (s.ok()) {
        if (nx || field_value == it->value) continue;

        exists = true;
      }
    }

    if (!exists) added++;

    s = batch->Put(sub_key, it->value);
    if (!s.ok()) return s;
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
    // Decode and check expiration
    HashFieldValue field_value;
    if (!HashFieldValue::Decode(iter->value().ToString(), &field_value)) {
      continue;  // Skip corrupted values
    }
    if (field_value.IsExpired()) {
      continue;  // Skip expired fields
    }

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

    field_values->emplace_back(ikey.GetSubKey().ToString(), std::move(field_value.value));
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
    // Decode and check expiration for all fetch types
    HashFieldValue field_value;
    if (!HashFieldValue::Decode(iter->value(), &field_value)) {
      continue;  // Skip corrupted values
    }
    if (field_value.IsExpired()) {
      continue;  // Skip expired fields
    }

    if (type == HashFetchType::kOnlyKey) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), "");
    } else if (type == HashFetchType::kOnlyValue) {
      field_values->emplace_back("", field_value.value.ToString());
    } else {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), field_value.value.ToString());
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
      unreachable();
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::ExpireFields(engine::Context &ctx, const Slice &user_key, uint64_t expire_ms,
                                   const std::vector<Slice> &fields, std::vector<int64_t> *results) {
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

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  for (const auto &field : fields) {
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    std::string raw_value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &raw_value);

    if (s.IsNotFound()) {
      results->push_back(-2);  // Field doesn't exist
      continue;
    }
    if (!s.ok()) return s;

    // Decode existing value
    HashFieldValue field_value;
    if (!HashFieldValue::Decode(raw_value, &field_value)) {
      return rocksdb::Status::Corruption("failed to decode hash field value");
    }

    // Check if field is already expired
    if (field_value.IsExpired()) {
      results->push_back(-2);  // Treat as non-existent
      continue;
    }

    // Set new expiration - create a new HashFieldValue with the value and new expiration
    HashFieldValue new_field_value(field_value.value, expire_ms);
    std::string encoded_value;
    new_field_value.Encode(&encoded_value);
    s = batch->Put(sub_key, encoded_value);
    if (!s.ok()) return s;
    results->push_back(1);  // Expiration set successfully
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
    std::string raw_value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &raw_value);

    if (s.IsNotFound()) {
      results->push_back(-2);  // Field doesn't exist
      continue;
    }
    if (!s.ok()) return s;

    // Decode existing value
    HashFieldValue field_value;
    if (!HashFieldValue::Decode(raw_value, &field_value)) {
      return rocksdb::Status::Corruption("failed to decode hash field value");
    }

    // Check if field is already expired
    if (field_value.IsExpired()) {
      results->push_back(-2);  // Treat as non-existent
      continue;
    }

    // Return TTL
    results->push_back(field_value.TTL());
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Hash::PersistFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
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
      results->push_back(-2);  // Field doesn't exist
      continue;
    }
    if (!s.ok()) return s;

    // Decode existing value
    HashFieldValue field_value;
    if (!HashFieldValue::Decode(raw_value, &field_value)) {
      return rocksdb::Status::Corruption("failed to decode hash field value");
    }

    // Check if field is already expired
    if (field_value.IsExpired()) {
      results->push_back(-2);  // Treat as non-existent
      continue;
    }

    // Check if field has expiration
    if (field_value.expire == 0) {
      results->push_back(-1);  // Field exists but has no TTL
      continue;
    }

    // Remove expiration - create a new HashFieldValue with the value and no expiration
    HashFieldValue new_field_value(field_value.value, 0);
    std::string encoded_value;
    new_field_value.Encode(&encoded_value);
    s = batch->Put(sub_key, encoded_value);
    if (!s.ok()) return s;
    has_updates = true;
    results->push_back(1);  // Expiration removed successfully
  }

  if (has_updates) {
    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
