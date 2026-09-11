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
#include <limits>
#include <optional>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/string_util.h"
#include "db_util.h"
#include "parse_util.h"
#include "sample_helper.h"
#include "time_util.h"

namespace redis {
namespace {

enum class HashFieldStateKind {
  kMissing,
  kPersistent,
  kLiveTTL,
  kExpiredTTLPhysical,
};

struct HashFieldState {
  HashFieldStateKind kind = HashFieldStateKind::kMissing;
  std::string value;
  uint64_t expire = 0;
};

constexpr const char *kHashFieldExpirationLegacyEncodingError =
    "hash field expiration is not supported by legacy hash encoding";

bool IsFieldExpired(uint64_t expire, uint64_t now) { return expire != 0 && expire < now; }

bool IsImmediateExpire(uint64_t expire_at, uint64_t now) { return expire_at <= now; }

uint64_t SaturatingSub(uint64_t lhs, uint64_t rhs) { return rhs > lhs ? 0 : lhs - rhs; }

void ClearBoundsIfNoTtlCandidates(HashMetadata *metadata) {
  if (metadata->IsFieldExpirationEncoding() && metadata->size == metadata->persist) {
    metadata->lower = 0;
    metadata->upper = 0;
  }
}

void ExpandExpireBounds(HashMetadata *metadata, uint64_t expire_at) {
  if (!metadata->IsFieldExpirationEncoding()) return;
  if (metadata->size == metadata->persist) {
    metadata->lower = expire_at;
    metadata->upper = expire_at;
    return;
  }
  metadata->lower = std::min(metadata->lower, expire_at);
  metadata->upper = std::max(metadata->upper, expire_at);
}

void ApplyMissingToPersistent(HashMetadata *metadata) {
  metadata->size += 1;
  if (metadata->IsFieldExpirationEncoding()) {
    metadata->persist += 1;
    ClearBoundsIfNoTtlCandidates(metadata);
  }
}

void ApplyMissingToTTL(HashMetadata *metadata, uint64_t expire_at) {
  ExpandExpireBounds(metadata, expire_at);
  metadata->size += 1;
}

void ApplyPersistentToTTL(HashMetadata *metadata, uint64_t expire_at) {
  ExpandExpireBounds(metadata, expire_at);
  metadata->persist = SaturatingSub(metadata->persist, 1);
}

void ApplyTTLToTTL(HashMetadata *metadata, uint64_t expire_at) { ExpandExpireBounds(metadata, expire_at); }

void ApplyTTLToPersistent(HashMetadata *metadata) {
  metadata->persist += 1;
  if (metadata->persist > metadata->size) {
    metadata->persist = metadata->size;
  }
  ClearBoundsIfNoTtlCandidates(metadata);
}

void ApplyPersistentToDeleted(HashMetadata *metadata) {
  metadata->size = SaturatingSub(metadata->size, 1);
  metadata->persist = SaturatingSub(metadata->persist, 1);
  ClearBoundsIfNoTtlCandidates(metadata);
}

void ApplyTTLToDeleted(HashMetadata *metadata) {
  metadata->size = SaturatingSub(metadata->size, 1);
  if (metadata->persist > metadata->size) {
    metadata->persist = metadata->size;
  }
  ClearBoundsIfNoTtlCandidates(metadata);
}

rocksdb::Status DecodeFieldState(const HashMetadata &metadata, Slice raw_value, uint64_t now, HashFieldState *state) {
  state->kind = HashFieldStateKind::kMissing;
  state->value.clear();
  state->expire = 0;

  uint64_t expire = 0;
  auto s = metadata.DecodeSubkeyValue(&raw_value, &expire);
  if (!s.ok()) return s;

  state->value.assign(raw_value.data(), raw_value.size());
  state->expire = expire;
  if (expire == 0) {
    state->kind = HashFieldStateKind::kPersistent;
  } else if (IsFieldExpired(expire, now)) {
    state->kind = HashFieldStateKind::kExpiredTTLPhysical;
  } else {
    state->kind = HashFieldStateKind::kLiveTTL;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status LoadFieldState(engine::Storage *storage, engine::Context &ctx, const HashMetadata &metadata,
                               const std::string &sub_key, uint64_t now, HashFieldState *state) {
  std::string raw_value;
  auto s = storage->Get(ctx, ctx.GetReadOptions(), sub_key, &raw_value);
  if (s.IsNotFound()) {
    *state = HashFieldState{};
    return rocksdb::Status::OK();
  }
  if (!s.ok()) return s;
  return DecodeFieldState(metadata, Slice(raw_value), now, state);
}

bool HExpireConditionPasses(HashFieldExpireCondition condition, const HashFieldState &state, uint64_t expire_at) {
  switch (state.kind) {
    case HashFieldStateKind::kMissing:
    case HashFieldStateKind::kExpiredTTLPhysical:
      return false;
    case HashFieldStateKind::kPersistent:
      return condition == HashFieldExpireCondition::kNone || condition == HashFieldExpireCondition::kNX ||
             condition == HashFieldExpireCondition::kLT;
    case HashFieldStateKind::kLiveTTL:
      switch (condition) {
        case HashFieldExpireCondition::kNone:
        case HashFieldExpireCondition::kXX:
          return true;
        case HashFieldExpireCondition::kNX:
          return false;
        case HashFieldExpireCondition::kGT:
          return expire_at > state.expire;
        case HashFieldExpireCondition::kLT:
          return expire_at < state.expire;
      }
  }
  return false;
}

rocksdb::Status ValidateHashFieldExpirationMetadata(const HashMetadata &metadata) {
  if (metadata.persist > metadata.size) {
    return rocksdb::Status::Corruption("invalid hash field expiration metadata: persist exceeds size");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ValidateMissingFieldTransition(const HashMetadata &metadata) {
  auto s = ValidateHashFieldExpirationMetadata(metadata);
  if (!s.ok()) return s;
  if (metadata.size == std::numeric_limits<uint64_t>::max()) {
    return rocksdb::Status::Corruption("invalid hash field expiration metadata: size overflow");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ValidatePersistentFieldTransition(const HashMetadata &metadata) {
  auto s = ValidateHashFieldExpirationMetadata(metadata);
  if (!s.ok()) return s;
  if (metadata.size == 0 || metadata.persist == 0) {
    return rocksdb::Status::Corruption("invalid hash field expiration metadata: no persistent field to update");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ValidateTTLFieldTransition(const HashMetadata &metadata) {
  auto s = ValidateHashFieldExpirationMetadata(metadata);
  if (!s.ok()) return s;
  if (metadata.size == 0 || metadata.persist == metadata.size) {
    return rocksdb::Status::Corruption("invalid hash field expiration metadata: no TTL field to update");
  }
  return rocksdb::Status::OK();
}

bool HashMetadataEqual(const HashMetadata &lhs, const HashMetadata &rhs) {
  return static_cast<const Metadata &>(lhs) == static_cast<const Metadata &>(rhs) && lhs.mode == rhs.mode &&
         lhs.persist == rhs.persist && lhs.lower == rhs.lower && lhs.upper == rhs.upper;
}

class HashBatchWriter {
 public:
  explicit HashBatchWriter(rocksdb::WriteBatchBase *batch) : batch_(batch) {}

  rocksdb::Status Put(const Slice &key, const Slice &value) {
    auto s = ensureLogData();
    if (!s.ok()) return s;
    s = batch_->Put(key, value);
    if (s.ok()) storage_mutated_ = true;
    return s;
  }

  rocksdb::Status Put(rocksdb::ColumnFamilyHandle *column_family, const Slice &key, const Slice &value) {
    auto s = ensureLogData();
    if (!s.ok()) return s;
    s = batch_->Put(column_family, key, value);
    if (s.ok()) storage_mutated_ = true;
    return s;
  }

  rocksdb::Status Delete(const Slice &key) {
    auto s = ensureLogData();
    if (!s.ok()) return s;
    s = batch_->Delete(key);
    if (s.ok()) storage_mutated_ = true;
    return s;
  }

  rocksdb::Status Delete(rocksdb::ColumnFamilyHandle *column_family, const Slice &key) {
    auto s = ensureLogData();
    if (!s.ok()) return s;
    s = batch_->Delete(column_family, key);
    if (s.ok()) storage_mutated_ = true;
    return s;
  }

  bool HasStorageMutation() const { return storage_mutated_; }

 private:
  rocksdb::Status ensureLogData() {
    if (has_log_data_) return rocksdb::Status::OK();
    WriteBatchLogData log_data(kRedisHash);
    auto s = batch_->PutLogData(log_data.Encode());
    if (s.ok()) has_log_data_ = true;
    return s;
  }

  rocksdb::WriteBatchBase *batch_;
  bool has_log_data_ = false;
  bool storage_mutated_ = false;
};

rocksdb::Status LoadFieldStates(engine::Storage *storage, engine::Context &ctx, const HashMetadata &metadata,
                                const std::string &ns_key, const std::vector<Slice> &fields, uint64_t now,
                                bool read_existing, std::unordered_map<std::string, HashFieldState> *states) {
  states->clear();
  states->reserve(fields.size());

  std::vector<std::string> unique_fields;
  unique_fields.reserve(fields.size());
  for (const auto &field : fields) {
    bool inserted = states->emplace(field.ToString(), HashFieldState{}).second;
    if (inserted) unique_fields.emplace_back(field.ToString());
  }

  if (!read_existing || unique_fields.empty()) return rocksdb::Status::OK();

  std::vector<std::string> encoded_keys;
  encoded_keys.reserve(unique_fields.size());
  for (const auto &field : unique_fields) {
    encoded_keys.emplace_back(InternalKey(ns_key, field, metadata.version, storage->IsSlotIdEncoded()).Encode());
  }

  std::vector<rocksdb::Slice> keys;
  keys.reserve(encoded_keys.size());
  for (const auto &key : encoded_keys) keys.emplace_back(key);

  std::vector<rocksdb::PinnableSlice> raw_values(keys.size());
  std::vector<rocksdb::Status> statuses(keys.size());
  storage->MultiGet(ctx, ctx.DefaultMultiGetOptions(), storage->GetDB()->DefaultColumnFamily(), keys.size(),
                    keys.data(), raw_values.data(), statuses.data());

  for (size_t i = 0; i < unique_fields.size(); ++i) {
    if (statuses[i].IsNotFound()) continue;
    if (!statuses[i].ok()) return statuses[i];
    auto s = DecodeFieldState(metadata, Slice(raw_values[i]), now, &states->at(unique_fields[i]));
    if (!s.ok()) return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status FinalizeHashMetadata(const HashMetadata &original_metadata, bool metadata_existed,
                                     HashMetadata *metadata, rocksdb::ColumnFamilyHandle *metadata_cf_handle,
                                     const std::string &ns_key, HashBatchWriter *writer) {
  auto s = ValidateHashFieldExpirationMetadata(*metadata);
  if (!s.ok()) return s;

  ClearBoundsIfNoTtlCandidates(metadata);
  if (metadata_existed && metadata->size == 0) {
    return writer->Delete(metadata_cf_handle, ns_key);
  }
  if (metadata->size == 0 || (metadata_existed && HashMetadataEqual(original_metadata, *metadata))) {
    return rocksdb::Status::OK();
  }

  std::string encoded_metadata;
  metadata->Encode(&encoded_metadata);
  return writer->Put(metadata_cf_handle, ns_key, encoded_metadata);
}

}  // namespace

HashMetadata Hash::createMetadataForWrite(bool generate_version) const {
  return HashMetadata(generate_version, storage_->GetConfig()->hash_encoding_mode);
}

rocksdb::Status Hash::getMetadata(engine::Context &ctx, const Slice &ns_key, HashMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisHash}, ns_key, metadata);
}

rocksdb::Status Hash::getRawValue(engine::Context &ctx, const std::string &sub_key, std::string *value) {
  return storage_->Get(ctx, ctx.GetReadOptions(), sub_key, value);
}

rocksdb::Status Hash::decodeValue(const HashMetadata &metadata, Slice *value, uint64_t *expire) {
  return metadata.DecodeSubkeyValue(value, expire);
}

rocksdb::Status Hash::Size(engine::Context &ctx, const Slice &user_key, uint64_t *size) {
  return Size(ctx, user_key, size, storage_->GetConfig()->hash_length_mode);
}

rocksdb::Status Hash::Size(engine::Context &ctx, const Slice &user_key, uint64_t *size, HashLengthMode length_mode) {
  *size = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  if (metadata.IsLegacySubkeyEncoding() || length_mode == HashLengthMode::kApproximate) {
    *size = metadata.size;
    return rocksdb::Status::OK();
  }

  if (metadata.persist > metadata.size) {
    return scanAndRepair(ctx, ns_key, &metadata, util::GetTimeStampMS(), size);
  }

  uint64_t ttl_candidates = metadata.size - metadata.persist;
  if (ttl_candidates == 0) {
    *size = metadata.size;
    return rocksdb::Status::OK();
  }

  uint64_t now = util::GetTimeStampMS();
  if (metadata.lower != 0 && now < metadata.lower) {
    *size = metadata.size;
    return rocksdb::Status::OK();
  }
  if (metadata.upper != 0 && now > metadata.upper && metadata.persist == 0) {
    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisHash);
    s = batch->PutLogData(log_data.Encode());
    if (!s.ok()) return s;
    s = batch->Delete(metadata_cf_handle_, ns_key);
    if (!s.ok()) return s;
    s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) return s;
    *size = 0;
    return rocksdb::Status::OK();
  }

  return scanAndRepair(ctx, ns_key, &metadata, now, size);
}

rocksdb::Status Hash::scanAndRepair(engine::Context &ctx, const Slice &ns_key, HashMetadata *metadata, uint64_t now,
                                    uint64_t *size) {
  *size = metadata->size;
  if (!metadata->IsFieldExpirationEncoding()) {
    return rocksdb::Status::OK();
  }

  HashMetadata repaired_metadata = *metadata;
  repaired_metadata.size = 0;
  repaired_metadata.persist = 0;
  repaired_metadata.lower = 0;
  repaired_metadata.upper = 0;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string prefix_key = InternalKey(ns_key, "", metadata->version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata->version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice lower_bound(prefix_key);
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_lower_bound = &lower_bound;
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    HashFieldState state;
    s = DecodeFieldState(*metadata, Slice(iter->value()), now, &state);
    if (!s.ok()) return s;
    if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
      s = batch->Delete(iter->key());
      if (!s.ok()) return s;
      continue;
    }
    repaired_metadata.size += 1;
    if (state.kind == HashFieldStateKind::kPersistent) {
      repaired_metadata.persist += 1;
    } else if (state.kind == HashFieldStateKind::kLiveTTL) {
      if (repaired_metadata.lower == 0 || state.expire < repaired_metadata.lower) {
        repaired_metadata.lower = state.expire;
      }
      repaired_metadata.upper = std::max(repaired_metadata.upper, state.expire);
    }
  }
  s = iter->status();
  if (!s.ok()) return s;

  *size = repaired_metadata.size;
  if (repaired_metadata.size == 0) {
    s = batch->Delete(metadata_cf_handle_, ns_key);
  } else {
    std::string bytes;
    repaired_metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  if (!s.ok()) return s;

  s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  if (!s.ok()) return s;
  *metadata = repaired_metadata;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Get(engine::Context &ctx, const Slice &user_key, const Slice &field, std::string *value) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string raw_value;
  s = getRawValue(ctx, sub_key, &raw_value);
  if (!s.ok()) return s;
  HashFieldState state;
  s = DecodeFieldState(metadata, Slice(raw_value), util::GetTimeStampMS(), &state);
  if (!s.ok()) return s;
  if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
    return rocksdb::Status::NotFound();
  }
  value->assign(state.value);
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::IncrBy(engine::Context &ctx, const Slice &user_key, const Slice &field, int64_t increment,
                             int64_t *new_value) {
  bool exists = false;
  bool expired_ttl_to_persistent = false;
  uint64_t keep_expire = 0;
  int64_t old_value = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata = createMetadataForWrite();
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    HashFieldState state;
    s = LoadFieldState(storage_, ctx, metadata, sub_key, util::GetTimeStampMS(), &state);
    if (!s.ok()) return s;
    if (state.kind == HashFieldStateKind::kPersistent || state.kind == HashFieldStateKind::kLiveTTL) {
      auto parse_result = ParseInt<int64_t>(state.value, 10);
      if (!parse_result) {
        return rocksdb::Status::InvalidArgument(parse_result.Msg());
      }
      if (!state.value.empty() && isspace(state.value[0])) {
        return rocksdb::Status::InvalidArgument("value is not an integer");
      }
      old_value = *parse_result;
      exists = true;
      keep_expire = state.expire;
    } else if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
      exists = true;
      expired_ttl_to_persistent = true;
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
  std::string encoded_value = metadata.EncodeSubkeyValue(std::to_string(*new_value), keep_expire);
  s = batch->Put(sub_key, encoded_value);
  if (!s.ok()) return s;
  if (!exists) {
    ApplyMissingToPersistent(&metadata);
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  } else if (metadata.IsFieldExpirationEncoding() && expired_ttl_to_persistent) {
    ApplyTTLToPersistent(&metadata);
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
  bool expired_ttl_to_persistent = false;
  uint64_t keep_expire = 0;
  double old_value = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata = createMetadataForWrite();
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    HashFieldState state;
    s = LoadFieldState(storage_, ctx, metadata, sub_key, util::GetTimeStampMS(), &state);
    if (!s.ok()) return s;
    if (state.kind == HashFieldStateKind::kPersistent || state.kind == HashFieldStateKind::kLiveTTL) {
      auto value_stat = ParseFloat(state.value);
      if (!value_stat || (!state.value.empty() && isspace(state.value[0]))) {
        return rocksdb::Status::InvalidArgument("value is not a number");
      }
      old_value = *value_stat;
      exists = true;
      keep_expire = state.expire;
    } else if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
      exists = true;
      expired_ttl_to_persistent = true;
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
  std::string encoded_value = metadata.EncodeSubkeyValue(util::Float2String(*new_value), keep_expire);
  s = batch->Put(sub_key, encoded_value);
  if (!s.ok()) return s;
  if (!exists) {
    ApplyMissingToPersistent(&metadata);
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  } else if (metadata.IsFieldExpirationEncoding() && expired_ttl_to_persistent) {
    ApplyTTLToPersistent(&metadata);
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
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
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
      HashFieldState state;
      s = DecodeFieldState(metadata, Slice(values_vector[i]), util::GetTimeStampMS(), &state);
      if (!s.ok()) return s;
      if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
        values->emplace_back("");
        statuses->emplace_back(rocksdb::Status::NotFound());
        continue;
      }
      values->emplace_back(std::move(state.value));
    } else {
      values->emplace_back("");
    }
    statuses->emplace_back(statuses_vector[i]);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::SetFieldsWithExpire(engine::Context &ctx, const Slice &user_key,
                                          const std::vector<FieldValue> &field_values, const HashSetExOptions &options,
                                          bool *applied, std::optional<uint64_t> now_ms) {
  *applied = false;

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  auto s = getMetadata(ctx, ns_key, &metadata);
  bool metadata_existed = s.ok();
  if (s.IsNotFound()) {
    if (options.condition == HashFieldSetCondition::kFXX) return rocksdb::Status::OK();
    metadata = createMetadataForWrite();
    if (metadata.IsLegacySubkeyEncoding()) {
      return rocksdb::Status::InvalidArgument(kHashFieldExpirationLegacyEncodingError);
    }
  } else if (!s.ok()) {
    return s;
  } else if (metadata.IsLegacySubkeyEncoding()) {
    return rocksdb::Status::InvalidArgument(kHashFieldExpirationLegacyEncodingError);
  }

  s = ValidateHashFieldExpirationMetadata(metadata);
  if (!s.ok()) return s;
  HashMetadata original_metadata = metadata;
  uint64_t now = now_ms.value_or(util::GetTimeStampMS());

  std::vector<Slice> fields;
  fields.reserve(field_values.size());
  for (const auto &field_value : field_values) fields.emplace_back(field_value.field);

  std::unordered_map<std::string, HashFieldState> state_cache;
  s = LoadFieldStates(storage_, ctx, metadata, ns_key, fields, now, metadata_existed, &state_cache);
  if (!s.ok()) return s;

  auto make_sub_key = [&](const std::string &field) {
    return InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  };

  std::vector<std::string> expired_cleanup_keys;
  bool condition_passed = true;
  if (options.condition != HashFieldSetCondition::kNone) {
    for (const auto &field_value : field_values) {
      HashFieldState &state = state_cache.at(field_value.field);
      if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
        s = ValidateTTLFieldTransition(metadata);
        if (!s.ok()) return s;
        ApplyTTLToDeleted(&metadata);
        expired_cleanup_keys.emplace_back(make_sub_key(field_value.field));
        state = HashFieldState{};
      }

      bool exists = state.kind == HashFieldStateKind::kPersistent || state.kind == HashFieldStateKind::kLiveTTL;
      if ((options.condition == HashFieldSetCondition::kFNX && exists) ||
          (options.condition == HashFieldSetCondition::kFXX && !exists)) {
        condition_passed = false;
        break;
      }
    }
  }

  auto batch = storage_->GetWriteBatchBase();
  HashBatchWriter writer(batch.Get());
  for (const auto &sub_key : expired_cleanup_keys) {
    s = writer.Delete(sub_key);
    if (!s.ok()) return s;
  }

  if (!condition_passed) {
    s = FinalizeHashMetadata(original_metadata, metadata_existed, &metadata, metadata_cf_handle_, ns_key, &writer);
    if (!s.ok()) return s;
    if (writer.HasStorageMutation()) {
      s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
      if (!s.ok()) return s;
    }
    return rocksdb::Status::OK();
  }

  std::unordered_set<std::string_view> seen_fields;
  std::vector<const FieldValue *> unique_field_values;
  seen_fields.reserve(field_values.size());
  unique_field_values.reserve(field_values.size());
  for (auto iter = field_values.rbegin(); iter != field_values.rend(); ++iter) {
    if (seen_fields.emplace(iter->field).second) unique_field_values.emplace_back(&*iter);
  }
  std::reverse(unique_field_values.begin(), unique_field_values.end());

  bool immediate =
      options.ttl_action == HashSetExOptions::TTLAction::kSet && IsImmediateExpire(options.expire_at_ms, now);
  for (const auto *field_value : unique_field_values) {
    HashFieldState &state = state_cache.at(field_value->field);
    std::string sub_key = make_sub_key(field_value->field);

    if (immediate) {
      if (state.kind == HashFieldStateKind::kMissing) continue;
      if (state.kind == HashFieldStateKind::kPersistent) {
        s = ValidatePersistentFieldTransition(metadata);
        if (!s.ok()) return s;
        ApplyPersistentToDeleted(&metadata);
      } else {
        s = ValidateTTLFieldTransition(metadata);
        if (!s.ok()) return s;
        ApplyTTLToDeleted(&metadata);
      }
      s = writer.Delete(sub_key);
      if (!s.ok()) return s;
      state = HashFieldState{};
      continue;
    }

    uint64_t target_expire = 0;
    switch (options.ttl_action) {
      case HashSetExOptions::TTLAction::kDiscard:
        break;
      case HashSetExOptions::TTLAction::kKeep:
        if (state.kind == HashFieldStateKind::kLiveTTL || state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
          target_expire = state.expire;
        }
        break;
      case HashSetExOptions::TTLAction::kSet:
        target_expire = options.expire_at_ms;
        break;
    }

    switch (state.kind) {
      case HashFieldStateKind::kMissing:
        s = ValidateMissingFieldTransition(metadata);
        if (!s.ok()) return s;
        if (target_expire == 0) {
          ApplyMissingToPersistent(&metadata);
        } else {
          ApplyMissingToTTL(&metadata, target_expire);
        }
        break;
      case HashFieldStateKind::kPersistent:
        if (target_expire != 0) {
          s = ValidatePersistentFieldTransition(metadata);
          if (!s.ok()) return s;
          ApplyPersistentToTTL(&metadata, target_expire);
        }
        break;
      case HashFieldStateKind::kLiveTTL:
      case HashFieldStateKind::kExpiredTTLPhysical:
        s = ValidateTTLFieldTransition(metadata);
        if (!s.ok()) return s;
        if (target_expire == 0) {
          ApplyTTLToPersistent(&metadata);
        } else {
          ApplyTTLToTTL(&metadata, target_expire);
        }
        break;
    }

    s = writer.Put(sub_key, metadata.EncodeSubkeyValue(field_value->value, target_expire));
    if (!s.ok()) return s;
    state.value = field_value->value;
    state.expire = target_expire;
    if (target_expire == 0) {
      state.kind = HashFieldStateKind::kPersistent;
    } else if (IsFieldExpired(target_expire, now)) {
      state.kind = HashFieldStateKind::kExpiredTTLPhysical;
    } else {
      state.kind = HashFieldStateKind::kLiveTTL;
    }
  }

  s = FinalizeHashMetadata(original_metadata, metadata_existed, &metadata, metadata_cf_handle_, ns_key, &writer);
  if (!s.ok()) return s;
  if (writer.HasStorageMutation()) {
    s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) return s;
  }
  *applied = true;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::GetFieldsWithExpire(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                          const HashGetExOptions &options, std::vector<std::string> *values,
                                          std::vector<rocksdb::Status> *statuses, std::optional<uint64_t> now_ms) {
  values->clear();
  statuses->clear();
  values->reserve(fields.size());
  statuses->reserve(fields.size());

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  auto s = getMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) {
    values->resize(fields.size());
    statuses->resize(fields.size(), rocksdb::Status::NotFound());
    return rocksdb::Status::OK();
  }
  if (!s.ok()) return s;
  if (metadata.IsLegacySubkeyEncoding()) {
    return rocksdb::Status::InvalidArgument(kHashFieldExpirationLegacyEncodingError);
  }

  s = ValidateHashFieldExpirationMetadata(metadata);
  if (!s.ok()) return s;
  HashMetadata original_metadata = metadata;
  uint64_t now = now_ms.value_or(util::GetTimeStampMS());

  std::unordered_map<std::string, HashFieldState> state_cache;
  s = LoadFieldStates(storage_, ctx, metadata, ns_key, fields, now, true, &state_cache);
  if (!s.ok()) return s;

  auto batch = storage_->GetWriteBatchBase();
  HashBatchWriter writer(batch.Get());
  auto make_sub_key = [&](const std::string &field) {
    return InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  };
  bool immediate =
      options.ttl_action == HashGetExOptions::TTLAction::kSet && IsImmediateExpire(options.expire_at_ms, now);

  for (const auto &field_slice : fields) {
    std::string field = field_slice.ToString();
    HashFieldState &state = state_cache.at(field);
    std::string sub_key = make_sub_key(field);

    if (state.kind == HashFieldStateKind::kMissing) {
      values->emplace_back();
      statuses->emplace_back(rocksdb::Status::NotFound());
      continue;
    }
    if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
      values->emplace_back();
      statuses->emplace_back(rocksdb::Status::NotFound());
      s = ValidateTTLFieldTransition(metadata);
      if (!s.ok()) return s;
      ApplyTTLToDeleted(&metadata);
      s = writer.Delete(sub_key);
      if (!s.ok()) return s;
      state = HashFieldState{};
      continue;
    }

    values->emplace_back(state.value);
    statuses->emplace_back(rocksdb::Status::OK());

    if (options.ttl_action == HashGetExOptions::TTLAction::kNone) continue;
    if (options.ttl_action == HashGetExOptions::TTLAction::kPersist) {
      if (state.kind == HashFieldStateKind::kPersistent) continue;
      s = ValidateTTLFieldTransition(metadata);
      if (!s.ok()) return s;
      ApplyTTLToPersistent(&metadata);
      s = writer.Put(sub_key, metadata.EncodeSubkeyValue(state.value));
      if (!s.ok()) return s;
      state.kind = HashFieldStateKind::kPersistent;
      state.expire = 0;
      continue;
    }

    if (immediate) {
      if (state.kind == HashFieldStateKind::kPersistent) {
        s = ValidatePersistentFieldTransition(metadata);
        if (!s.ok()) return s;
        ApplyPersistentToDeleted(&metadata);
      } else {
        s = ValidateTTLFieldTransition(metadata);
        if (!s.ok()) return s;
        ApplyTTLToDeleted(&metadata);
      }
      s = writer.Delete(sub_key);
      if (!s.ok()) return s;
      state = HashFieldState{};
      continue;
    }

    if (state.kind == HashFieldStateKind::kLiveTTL && state.expire == options.expire_at_ms) continue;
    if (state.kind == HashFieldStateKind::kPersistent) {
      s = ValidatePersistentFieldTransition(metadata);
      if (!s.ok()) return s;
      ApplyPersistentToTTL(&metadata, options.expire_at_ms);
    } else {
      s = ValidateTTLFieldTransition(metadata);
      if (!s.ok()) return s;
      ApplyTTLToTTL(&metadata, options.expire_at_ms);
    }
    s = writer.Put(sub_key, metadata.EncodeSubkeyValue(state.value, options.expire_at_ms));
    if (!s.ok()) return s;
    state.kind = HashFieldStateKind::kLiveTTL;
    state.expire = options.expire_at_ms;
  }

  s = FinalizeHashMetadata(original_metadata, true, &metadata, metadata_cf_handle_, ns_key, &writer);
  if (!s.ok()) return s;
  if (writer.HasStorageMutation()) {
    s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) return s;
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

  s = getMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  uint64_t physical_removed = 0;
  uint64_t persistent_removed = 0;
  std::unordered_set<std::string_view> field_set;
  uint64_t now = util::GetTimeStampMS();
  for (const auto &field : fields) {
    if (!field_set.emplace(field.ToStringView()).second) {
      continue;
    }
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    HashFieldState state;
    s = LoadFieldState(storage_, ctx, metadata, sub_key, now, &state);
    if (!s.ok()) return s;
    switch (state.kind) {
      case HashFieldStateKind::kMissing:
        break;
      case HashFieldStateKind::kPersistent:
        persistent_removed += 1;
        physical_removed += 1;
        *deleted_cnt += 1;
        s = batch->Delete(sub_key);
        if (!s.ok()) return s;
        break;
      case HashFieldStateKind::kLiveTTL:
        physical_removed += 1;
        *deleted_cnt += 1;
        s = batch->Delete(sub_key);
        if (!s.ok()) return s;
        break;
      case HashFieldStateKind::kExpiredTTLPhysical:
        physical_removed += 1;
        s = batch->Delete(sub_key);
        if (!s.ok()) return s;
        break;
    }
  }
  if (physical_removed == 0) {
    return rocksdb::Status::OK();
  }
  metadata.size = SaturatingSub(metadata.size, physical_removed);
  if (metadata.IsFieldExpirationEncoding()) {
    metadata.persist = SaturatingSub(metadata.persist, persistent_removed);
    ClearBoundsIfNoTtlCandidates(&metadata);
  }
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

  HashMetadata metadata = createMetadataForWrite();
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  bool had_existing_fields = s.ok() && metadata.size > 0;
  bool ttl_updated = false;
  if (expire > 0 && metadata.expire != expire) {
    metadata.expire = expire;
    ttl_updated = true;
  }
  int added = 0;
  bool metadata_changed = ttl_updated;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  std::unordered_set<std::string_view> field_set;

  std::vector<rocksdb::Slice> keys;
  std::vector<std::string> keys_encoded;
  std::vector<std::string_view> values;
  keys.reserve(field_values.size());
  keys_encoded.reserve(field_values.size());
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
  if (had_existing_fields) {
    rocksdb::ReadOptions read_options = ctx.DefaultMultiGetOptions();
    storage_->MultiGet(ctx, read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                       values_vector.data(), statuses_vector.data());
  }

  for (size_t field_index = 0; field_index < keys.size(); field_index++) {
    const rocksdb::Slice field_key = keys[field_index];
    HashFieldStateKind state_kind = HashFieldStateKind::kMissing;

    if (had_existing_fields) {
      rocksdb::Status &field_status = statuses_vector[field_index];
      if (!field_status.ok() && !field_status.IsNotFound()) {
        return field_status;
      }
      if (field_status.ok()) {
        HashFieldState state;
        s = DecodeFieldState(metadata, Slice(values_vector[field_index]), util::GetTimeStampMS(), &state);
        if (!s.ok()) return s;
        state_kind = state.kind;
        if (nx && state.kind != HashFieldStateKind::kExpiredTTLPhysical) {
          continue;
        }
        if (state.kind == HashFieldStateKind::kPersistent && state.value == values[field_index]) {
          continue;
        }
      }
    }

    switch (state_kind) {
      case HashFieldStateKind::kMissing:
        ApplyMissingToPersistent(&metadata);
        added++;
        metadata_changed = true;
        break;
      case HashFieldStateKind::kPersistent:
        break;
      case HashFieldStateKind::kLiveTTL:
        ApplyTTLToPersistent(&metadata);
        metadata_changed = true;
        break;
      case HashFieldStateKind::kExpiredTTLPhysical:
        ApplyTTLToPersistent(&metadata);
        added++;
        metadata_changed = true;
        break;
    }

    std::string encoded_value = metadata.EncodeSubkeyValue(values[field_index]);
    s = batch->Put(field_key, encoded_value);
    if (!s.ok()) return s;
  }

  if (metadata_changed) {
    *added_cnt = added;
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
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
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
    HashFieldState state;
    s = DecodeFieldState(metadata, Slice(iter->value()), util::GetTimeStampMS(), &state);
    if (!s.ok()) return s;
    if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
      continue;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;
    field_values->emplace_back(ikey.GetSubKey().ToString(), std::move(state.value));
    if (spec.count > 0 && field_values->size() >= static_cast<unsigned>(spec.count)) break;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::GetAll(engine::Context &ctx, const Slice &user_key, std::vector<FieldValue> *field_values,
                             HashFetchType type) {
  field_values->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    HashFieldState state;
    s = DecodeFieldState(metadata, Slice(iter->value()), util::GetTimeStampMS(), &state);
    if (!s.ok()) return s;
    if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
      continue;
    }
    if (type == HashFetchType::kOnlyKey) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), "");
    } else if (type == HashFetchType::kOnlyValue) {
      field_values->emplace_back("", std::move(state.value));
    } else {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), std::move(state.value));
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Scan(engine::Context &ctx, const Slice &user_key, const std::string &cursor, uint64_t limit,
                           const std::string &field_prefix, std::vector<std::string> *fields,
                           std::vector<std::string> *values) {
  fields->clear();
  if (values != nullptr) values->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  auto iter = util::UniqueIterator(ctx, ctx.DefaultScanOptions());
  std::string match_prefix_key =
      InternalKey(ns_key, field_prefix, metadata.version, storage_->IsSlotIdEncoded()).Encode();

  std::string start_key;
  if (!cursor.empty()) {
    start_key = InternalKey(ns_key, cursor, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  } else {
    start_key = match_prefix_key;
  }

  uint64_t live_count = 0;
  uint64_t now = util::GetTimeStampMS();
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    if (!cursor.empty() && iter->key() == start_key) {
      continue;
    }
    if (!iter->key().starts_with(match_prefix_key)) {
      break;
    }
    HashFieldState state;
    s = DecodeFieldState(metadata, Slice(iter->value()), now, &state);
    if (!s.ok()) return s;
    if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
      continue;
    }
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    fields->emplace_back(ikey.GetSubKey().ToString());
    if (values != nullptr) {
      values->emplace_back(std::move(state.value));
    }
    live_count++;
    if (limit > 0 && live_count >= limit) {
      break;
    }
  }
  return iter->status();
}

rocksdb::Status Hash::RandField(engine::Context &ctx, const Slice &user_key, int64_t command_count,
                                std::vector<FieldValue> *field_values, HashFetchType type) {
  uint64_t count = (command_count >= 0) ? static_cast<uint64_t>(command_count) : static_cast<uint64_t>(-command_count);
  bool unique = (command_count >= 0);

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(/*generate_version=*/false);
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
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
  if (field_values->empty()) {
    return rocksdb::Status::NotFound();
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

rocksdb::Status Hash::GetFieldsExpireTime(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                          std::vector<int64_t> *results, std::optional<uint64_t> now_ms) {
  results->clear();
  results->resize(fields.size(), -2);

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!s.ok()) return s;
  if (!metadata.IsFieldExpirationEncoding()) {
    return rocksdb::Status::InvalidArgument("hash field expiration is not supported by legacy hash encoding");
  }

  uint64_t now = now_ms.value_or(util::GetTimeStampMS());
  std::unordered_map<std::string, int64_t> result_cache;
  for (size_t i = 0; i < fields.size(); i++) {
    std::string field = fields[i].ToString();
    auto cache_iter = result_cache.find(field);
    if (cache_iter != result_cache.end()) {
      (*results)[i] = cache_iter->second;
      continue;
    }

    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    HashFieldState state;
    s = LoadFieldState(storage_, ctx, metadata, sub_key, now, &state);
    if (!s.ok()) return s;

    int64_t result = -2;
    if (state.kind == HashFieldStateKind::kPersistent) {
      result = -1;
    } else if (state.kind == HashFieldStateKind::kLiveTTL) {
      result = static_cast<int64_t>(state.expire);
    }
    result_cache.emplace(std::move(field), result);
    (*results)[i] = result;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::ExpireFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                   uint64_t expire_at_ms, HashFieldExpireCondition condition,
                                   std::vector<int64_t> *results, std::optional<uint64_t> now_ms) {
  results->clear();
  results->resize(fields.size(), -2);

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!s.ok()) return s;
  if (!metadata.IsFieldExpirationEncoding()) {
    return rocksdb::Status::InvalidArgument("hash field expiration is not supported by legacy hash encoding");
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  bool metadata_changed = false;
  uint64_t now = now_ms.value_or(util::GetTimeStampMS());
  bool immediate = IsImmediateExpire(expire_at_ms, now);
  std::unordered_map<std::string, HashFieldState> state_cache;

  for (size_t i = 0; i < fields.size(); i++) {
    std::string field = fields[i].ToString();
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();

    auto cache_iter = state_cache.find(field);
    if (cache_iter == state_cache.end()) {
      HashFieldState state;
      s = LoadFieldState(storage_, ctx, metadata, sub_key, now, &state);
      if (!s.ok()) return s;
      cache_iter = state_cache.emplace(field, std::move(state)).first;
    }
    HashFieldState &state = cache_iter->second;

    if (state.kind == HashFieldStateKind::kMissing) {
      (*results)[i] = -2;
      continue;
    }
    if (state.kind == HashFieldStateKind::kExpiredTTLPhysical) {
      s = batch->Delete(sub_key);
      if (!s.ok()) return s;
      ApplyTTLToDeleted(&metadata);
      metadata_changed = true;
      state = HashFieldState{};
      (*results)[i] = -2;
      continue;
    }
    if (!HExpireConditionPasses(condition, state, expire_at_ms)) {
      (*results)[i] = 0;
      continue;
    }

    if (immediate) {
      s = batch->Delete(sub_key);
      if (!s.ok()) return s;
      if (state.kind == HashFieldStateKind::kPersistent) {
        ApplyPersistentToDeleted(&metadata);
      } else {
        ApplyTTLToDeleted(&metadata);
      }
      metadata_changed = true;
      state = HashFieldState{};
      (*results)[i] = 2;
      continue;
    }

    if (state.kind == HashFieldStateKind::kPersistent) {
      ApplyPersistentToTTL(&metadata, expire_at_ms);
    } else {
      ApplyTTLToTTL(&metadata, expire_at_ms);
    }
    metadata_changed = true;
    s = batch->Put(sub_key, metadata.EncodeSubkeyValue(state.value, expire_at_ms));
    if (!s.ok()) return s;
    state.kind = HashFieldStateKind::kLiveTTL;
    state.expire = expire_at_ms;
    (*results)[i] = 1;
  }

  if (metadata_changed) {
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::PersistFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                    std::vector<int64_t> *results) {
  results->clear();
  results->resize(fields.size(), -2);

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = getMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!s.ok()) return s;
  if (!metadata.IsFieldExpirationEncoding()) {
    return rocksdb::Status::InvalidArgument("hash field expiration is not supported by legacy hash encoding");
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  bool metadata_changed = false;
  uint64_t now = util::GetTimeStampMS();
  std::unordered_map<std::string, HashFieldState> state_cache;

  for (size_t i = 0; i < fields.size(); i++) {
    std::string field = fields[i].ToString();
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();

    auto cache_iter = state_cache.find(field);
    if (cache_iter == state_cache.end()) {
      HashFieldState state;
      s = LoadFieldState(storage_, ctx, metadata, sub_key, now, &state);
      if (!s.ok()) return s;
      cache_iter = state_cache.emplace(field, std::move(state)).first;
    }
    HashFieldState &state = cache_iter->second;

    switch (state.kind) {
      case HashFieldStateKind::kMissing:
        (*results)[i] = -2;
        break;
      case HashFieldStateKind::kPersistent:
        (*results)[i] = -1;
        break;
      case HashFieldStateKind::kExpiredTTLPhysical:
        s = batch->Delete(sub_key);
        if (!s.ok()) return s;
        ApplyTTLToDeleted(&metadata);
        metadata_changed = true;
        state = HashFieldState{};
        (*results)[i] = -2;
        break;
      case HashFieldStateKind::kLiveTTL:
        ApplyTTLToPersistent(&metadata);
        metadata_changed = true;
        s = batch->Put(sub_key, metadata.EncodeSubkeyValue(state.value));
        if (!s.ok()) return s;
        state.kind = HashFieldStateKind::kPersistent;
        state.expire = 0;
        (*results)[i] = 1;
        break;
    }
  }

  if (metadata_changed) {
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
