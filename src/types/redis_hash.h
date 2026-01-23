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

enum class FieldExpireResult : int64_t {
  kFieldNotFound = -2,
  kExpireNotSet = 0,
  kExpireSet = 1,
  kExpireWithPastTime = 2,
};

enum class FieldExpireCondition : int64_t {
  kFieldNoExpireCondition = 0,
  kFieldExpireTimeNotExists = 1,
  kFieldExpireTimeExists = 2,
  kFieldExpireTimeGreaterThanInput = 3,
  kFieldExpireTimeLessThanInput = 4,
};

enum class FieldPersistResult : int64_t {
  kFieldNotFound = -2,
  kNotVolatile = -1,
  kPersisted = 1,
};

enum class SetEXFieldCondition : int64_t {
  kNoCondition = 0,
  kFNX = 1,
  kFXX = 2,
};

enum class SetEXExpireOption : int64_t {
  kNoExpire = 0,
  kEX = 1,
  kPX = 2,
  kEXAT = 3,
  kPXAT = 4,
  kKEEPTTL = 5,
};

struct ExpireParams {
  SetEXExpireOption option = SetEXExpireOption::kNoExpire;
  uint64_t value = 0;
};

struct HSetExParams {
  SetEXFieldCondition condition = SetEXFieldCondition::kNoCondition;
  ExpireParams expire_params;
};

const uint64_t NoExpireTime = 0;
const uint64_t ExpireVersionOffset = 8;

struct FieldValue {
  std::string field;
  std::string value;

  FieldValue(std::string f, std::string v) : field(std::move(f)), value(std::move(v)) {}
};

enum class HashFetchType { kAll = 0, kOnlyKey = 1, kOnlyValue = 2 };

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
  rocksdb::Status ExpireFields(engine::Context &ctx, const Slice &user_key, uint64_t expireat_ms,
                               const std::vector<Slice> &fields, std::vector<FieldExpireResult> *results,
                               FieldExpireCondition condition = FieldExpireCondition::kFieldNoExpireCondition);
  // Get TTL for fields in milliseconds.
  // For results:
  // -2 if the field does not exist.
  // -1 if the field exists but has no associated expiration.
  // A non-negative value representing the expire_time in milliseconds.
  rocksdb::Status TTLFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                            std::vector<int64_t> *results);

  rocksdb::Status PersistFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                std::vector<FieldPersistResult> *results);

  rocksdb::Status MSetEx(engine::Context &ctx, const Slice &user_key, const std::vector<FieldValue> &field_values,
                         const HSetExParams &params, uint64_t *added_cnt);

 private:
  rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &ns_key, HashMetadata *metadata);

  friend struct FieldValueRetriever;
};

}  // namespace redis
