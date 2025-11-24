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

#include <rocksdb/status.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/range_spec.h"
#include "encoding.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "time_util.h"

struct FieldValue {
  std::string field;
  std::string value;

  FieldValue(std::string f, std::string v) : field(std::move(f)), value(std::move(v)) {}
};

enum class HashFetchType { kAll = 0, kOnlyKey = 1, kOnlyValue = 2 };

// Hash field value encoding flags
// Bit 0: has expiration timestamp
constexpr uint8_t HASH_FIELD_FLAG_EXPIRE = 0x01;
// Magic byte to identify new encoding format (must not conflict with typical value first bytes)
constexpr uint8_t HASH_FIELD_ENCODING_VERSION = 0xFF;

// HashFieldValue handles encoding/decoding of hash field values with optional expiration
// Legacy format (backward compatible): [raw value]
// New format: [1-byte version=0xFF][1-byte flags][8-byte expire timestamp if flag set][value]
struct HashFieldValue {
  std::string value;
  uint64_t expire = 0;  // 0 means no expiration, otherwise millisecond timestamp

  HashFieldValue() = default;
  explicit HashFieldValue(std::string v, uint64_t exp = 0) : value(std::move(v)), expire(exp) {}

  // Encode the field value with optional expiration
  void Encode(std::string *dst) const {
    if (expire == 0) {
      // No expiration - store as raw value for backward compatibility
      dst->assign(value);
    } else {
      // Has expiration - use new format
      dst->clear();
      PutFixed8(dst, HASH_FIELD_ENCODING_VERSION);
      PutFixed8(dst, HASH_FIELD_FLAG_EXPIRE);
      PutFixed64(dst, expire);
      dst->append(value);
    }
  }

  // Decode the field value, extracting expiration if present
  // Returns true if decoding succeeded
  static bool Decode(const std::string &input, HashFieldValue *out) {
    if (input.empty()) {
      out->value.clear();
      out->expire = 0;
      return true;
    }

    // Check for new encoding format
    if (static_cast<uint8_t>(input[0]) == HASH_FIELD_ENCODING_VERSION && input.size() >= 2) {
      rocksdb::Slice slice(input);
      slice.remove_prefix(1);  // Skip version byte

      uint8_t flags = 0;
      if (!GetFixed8(&slice, &flags)) return false;

      if (flags & HASH_FIELD_FLAG_EXPIRE) {
        if (!GetFixed64(&slice, &out->expire)) return false;
      } else {
        out->expire = 0;
      }
      out->value = slice.ToString();
    } else {
      // Legacy format - raw value, no expiration
      out->value = input;
      out->expire = 0;
    }
    return true;
  }

  // Check if the field has expired
  bool IsExpired() const {
    if (expire == 0) return false;
    return expire <= util::GetTimeStampMS();
  }

  // Get TTL in milliseconds, -1 if no expiration, -2 should be used by caller if field doesn't exist
  int64_t TTLMS() const {
    if (expire == 0) return -1;
    auto now = static_cast<int64_t>(util::GetTimeStampMS());
    auto ttl = static_cast<int64_t>(expire) - now;
    return ttl > 0 ? ttl : -2;  // -2 indicates expired
  }

  // Get TTL in seconds
  int64_t TTL() const {
    int64_t ttl_ms = TTLMS();
    if (ttl_ms < 0) return ttl_ms;
    return (ttl_ms + 999) / 1000;  // Round up to seconds
  }
};

namespace redis {

class Hash : public SubKeyScanner {
 public:
  Hash(engine::Storage *storage, const std::string &ns) : SubKeyScanner(storage, ns) {}

  rocksdb::Status Size(engine::Context &ctx, const Slice &user_key, uint64_t *size);
  rocksdb::Status Get(engine::Context &ctx, const Slice &user_key, const Slice &field, std::string *value);
  rocksdb::Status Set(engine::Context &ctx, const Slice &user_key, const Slice &field, const Slice &value,
                      uint64_t *added_cnt);
  rocksdb::Status Delete(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                         uint64_t *deleted_cnt);
  rocksdb::Status IncrBy(engine::Context &ctx, const Slice &user_key, const Slice &field, int64_t increment,
                         int64_t *new_value);
  rocksdb::Status IncrByFloat(engine::Context &ctx, const Slice &user_key, const Slice &field, double increment,
                              double *new_value);
  rocksdb::Status MSet(engine::Context &ctx, const Slice &user_key, const std::vector<FieldValue> &field_values,
                       bool nx, uint64_t *added_cnt, uint64_t expire = 0);
  rocksdb::Status RangeByLex(engine::Context &ctx, const Slice &user_key, const RangeLexSpec &spec,
                             std::vector<FieldValue> *field_values);
  rocksdb::Status MGet(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                       std::vector<std::string> *values, std::vector<rocksdb::Status> *statuses);
  rocksdb::Status GetAll(engine::Context &ctx, const Slice &user_key, std::vector<FieldValue> *field_values,
                         HashFetchType type = HashFetchType::kAll);
  rocksdb::Status Scan(engine::Context &ctx, const Slice &user_key, const std::string &cursor, uint64_t limit,
                       const std::string &field_prefix, std::vector<std::string> *fields,
                       std::vector<std::string> *values = nullptr);
  rocksdb::Status RandField(engine::Context &ctx, const Slice &user_key, int64_t command_count,
                            std::vector<FieldValue> *field_values, HashFetchType type = HashFetchType::kOnlyKey);

  // Per-field expiration methods
  // Set expiration on fields, returns result codes per field:
  // -2 = field doesn't exist, 1 = expiration set, 0 = expiration not set (e.g., invalid expire time)
  rocksdb::Status ExpireFields(engine::Context &ctx, const Slice &user_key, uint64_t expire_ms,
                               const std::vector<Slice> &fields, std::vector<int64_t> *results);

  // Get TTL for fields in seconds, returns per field:
  // -2 = field doesn't exist, -1 = field exists but no TTL, >= 0 = TTL in seconds
  rocksdb::Status TTLFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                            std::vector<int64_t> *results);

  // Remove expiration from fields, returns result codes per field:
  // -2 = field doesn't exist, -1 = field exists but no TTL, 1 = expiration removed
  rocksdb::Status PersistFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                std::vector<int64_t> *results);

 private:
  rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &ns_key, HashMetadata *metadata);

  friend struct FieldValueRetriever;
};

}  // namespace redis
