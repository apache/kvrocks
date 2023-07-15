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

namespace redis {

rocksdb::Status Hash::GetMetadata(const Slice &ns_key, HashMetadata *metadata) {
  return Database::GetMetadata(kRedisHash, ns_key, metadata);
}

rocksdb::Status Hash::Size(const Slice &user_key, uint64_t *size) {
  *size = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;
  *size = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Get(const Slice &user_key, const Slice &field, std::string *value) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key;
  InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  return storage_->Get(read_options, sub_key, value);
}

rocksdb::Status Hash::IncrBy(const Slice &user_key, const Slice &field, int64_t increment, int64_t *new_value) {
  bool exists = false;
  int64_t old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key;
  InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  if (s.ok()) {
    std::string value_bytes;
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
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
  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN - old_value)) ||
      (increment > 0 && old_value > 0 && increment > (LLONG_MAX - old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *new_value = old_value + increment;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  batch->PutLogData(log_data.Encode());
  batch->Put(sub_key, std::to_string(*new_value));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::IncrByFloat(const Slice &user_key, const Slice &field, double increment, double *new_value) {
  bool exists = false;
  double old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key;
  InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  if (s.ok()) {
    std::string value_bytes;
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      auto value_stat = ParseFloat(value_bytes);
      if (!value_stat || isspace(value_bytes[0])) {
        return rocksdb::Status::InvalidArgument("value is not a number");
      }
      old_value = *value_stat;
      exists = true;
    }
  }
  double n = old_value + increment;
  if (std::isinf(n) || std::isnan(n)) {
    return rocksdb::Status::InvalidArgument("increment would produce NaN or Infinity");
  }

  *new_value = n;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  batch->PutLogData(log_data.Encode());
  batch->Put(sub_key, std::to_string(*new_value));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::MGet(const Slice &user_key, const std::vector<Slice> &fields, std::vector<std::string> *values,
                           std::vector<rocksdb::Status> *statuses) {
  values->clear();
  statuses->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  storage_->SetReadOptions(read_options);
  std::vector<rocksdb::Slice> keys;

  keys.reserve(fields.size());
  std::vector<std::string> sub_keys;
  sub_keys.resize(fields.size());
  int i = 0;
  for (const auto &field : fields) {
    InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&(sub_keys[i]));
    keys.emplace_back(sub_keys[i++]);
  }

  std::vector<rocksdb::PinnableSlice> values_vector;
  values_vector.resize(keys.size());
  std::vector<rocksdb::Status> statuses_vector;
  statuses_vector.resize(keys.size());
  storage_->MultiGet(read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                     values_vector.data(), statuses_vector.data());
  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses_vector[i].ok() && !statuses_vector[i].IsNotFound()) return statuses_vector[i];
    values->emplace_back(values_vector[i].ToString());
    statuses->emplace_back(statuses_vector[i]);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Set(const Slice &user_key, const Slice &field, const Slice &value, uint64_t *added_cnt) {
  return MSet(user_key, {{field.ToString(), value.ToString()}}, false, added_cnt);
}

rocksdb::Status Hash::Delete(const Slice &user_key, const std::vector<Slice> &fields, uint64_t *deleted_cnt) {
  *deleted_cnt = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  HashMetadata metadata(false);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  batch->PutLogData(log_data.Encode());
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string sub_key, value;
  for (const auto &field : fields) {
    InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (s.ok()) {
      *deleted_cnt += 1;
      batch->Delete(sub_key);
    }
  }
  if (*deleted_cnt == 0) {
    return rocksdb::Status::OK();
  }
  metadata.size -= *deleted_cnt;
  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::MSet(const Slice &user_key, const std::vector<FieldValue> &field_values, bool nx,
                           uint64_t *added_cnt) {
  *added_cnt = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  int added = 0;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  batch->PutLogData(log_data.Encode());

  for (const auto &fv : field_values) {
    bool exists = false;

    std::string sub_key;
    InternalKey(ns_key, fv.field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);

    if (metadata.size > 0) {
      std::string field_value;
      s = storage_->Get(rocksdb::ReadOptions(), sub_key, &field_value);
      if (!s.ok() && !s.IsNotFound()) return s;

      if (s.ok()) {
        if (nx || field_value == fv.value) continue;

        exists = true;
      }
    }

    if (!exists) added++;

    batch->Put(sub_key, fv.value);
  }

  if (added > 0) {
    *added_cnt = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::RangeByLex(const Slice &user_key, const RangeLexSpec &spec,
                                 std::vector<FieldValue> *field_values) {
  field_values->clear();
  if (spec.count == 0) {
    return rocksdb::Status::OK();
  }
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string start_member = spec.reversed ? spec.max : spec.min;
  std::string start_key, prefix_key, next_version_prefix_key;
  InternalKey(ns_key, start_member, metadata.version, storage_->IsSlotIdEncoded()).Encode(&start_key);
  InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode(&prefix_key);
  InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode(&next_version_prefix_key);
  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;
  storage_->SetReadOptions(read_options);

  auto iter = util::UniqueIterator(storage_, read_options);
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

rocksdb::Status Hash::GetAll(const Slice &user_key, std::vector<FieldValue> *field_values, HashFetchType type) {
  field_values->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix_key, next_version_prefix_key;
  InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode(&prefix_key);
  InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode(&next_version_prefix_key);

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  storage_->SetReadOptions(read_options);

  auto iter = util::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    if (type == HashFetchType::kOnlyKey) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), "");
    } else if (type == HashFetchType::kOnlyValue) {
      field_values->emplace_back("", iter->value().ToString());
    } else {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), iter->value().ToString());
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Scan(const Slice &user_key, const std::string &cursor, uint64_t limit,
                           const std::string &field_prefix, std::vector<std::string> *fields,
                           std::vector<std::string> *values) {
  return SubKeyScanner::Scan(kRedisHash, user_key, cursor, limit, field_prefix, fields, values);
}

rocksdb::Status Hash::RandField(const Slice &user_key, int64_t command_count, std::vector<FieldValue> *field_values,
                                HashFetchType type) {
  uint64_t count = (command_count >= 0) ? static_cast<uint64_t>(command_count) : static_cast<uint64_t>(-command_count);
  bool unique = (command_count >= 0);

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata(/*generate_version=*/false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;

  uint64_t size = metadata.size;
  std::vector<FieldValue> samples;
  // TODO: Getting all values in Hash might be heavy, consider lazy-loading these values later
  if (count == 0) return rocksdb::Status::OK();
  s = GetAll(user_key, &samples, type);
  if (!s.ok()) return s;
  auto append_field_with_index = [field_values, &samples, type](uint64_t index) {
    if (type == HashFetchType::kAll) {
      field_values->emplace_back(samples[index].field, samples[index].value);
    } else {
      field_values->emplace_back(samples[index].field, "");
    }
  };
  field_values->reserve(std::min(size, count));
  if (!unique || count == 1) {
    // Case 1: Negative count, randomly select elements or without parameter
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(0, size - 1);
    for (uint64_t i = 0; i < count; i++) {
      uint64_t index = dis(gen);
      append_field_with_index(index);
    }
  } else if (size <= count) {
    // Case 2: Requested count is greater than or equal to the number of elements inside the hash
    for (uint64_t i = 0; i < size; i++) {
      append_field_with_index(i);
    }
  } else {
    // Case 3: Requested count is less than the number of elements inside the hash
    std::vector<uint64_t> indices(size);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(),
                 std::random_device{});  // use Fisher-Yates shuffle algorithm to randomize the order
    for (uint64_t i = 0; i < count; i++) {
      uint64_t index = indices[i];
      append_field_with_index(index);
    }
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
