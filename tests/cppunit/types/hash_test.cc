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

#include <gtest/gtest.h>

#include <algorithm>
#include <climits>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "parse_util.h"
#include "test_base.h"
#include "time_util.h"
#include "types/redis_hash.h"

class RedisHashTest : public TestBase {
 protected:
  explicit RedisHashTest() { hash_ = std::make_unique<redis::Hash>(storage_.get(), "hash_ns"); }
  ~RedisHashTest() override = default;

  void SetUp() override {
    key_ = "test_hash->key";
    fields_ = {"test-hash-key-1", "test-hash-key-2", "test-hash-key-3"};
    values_ = {"hash-test-value-1", "hash-test-value-2", "hash-test-value-3"};
  }
  void TearDown() override {}

  std::unique_ptr<redis::Hash> hash_;
};

TEST_F(RedisHashTest, GetAndSet) {
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(*ctx_, key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string got;
    auto s = hash_->Get(*ctx_, key_, fields_[i], &got);
    EXPECT_EQ(s.ToString(), "OK");
    EXPECT_EQ(values_[i], got);
  }
  auto s = hash_->Delete(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, MGetAndMSet) {
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(fields_[i].ToString(), values_[i].ToString());
  }
  auto s = hash_->MSet(*ctx_, key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && fvs.size() == ret);
  s = hash_->MSet(*ctx_, key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 0);
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  s = hash_->MGet(*ctx_, key_, fields_, &values, &statuses);
  EXPECT_TRUE(s.ok());
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(values[i], values_[i].ToString());
  }
  s = hash_->Delete(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, MSetAndDeleteRepeated) {
  std::vector<std::string> fields{"f1", "f1", "f2", "f3"};
  std::vector<std::string> values{"v1", "v11", "v2", "v3"};
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields.size(); i++) {
    fvs.emplace_back(fields[i], values[i]);
  }

  uint64_t ret = 0;
  rocksdb::Status s = hash_->MSet(*ctx_, key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<uint64_t>(fvs.size() - 1) == ret);
  std::string got;
  s = hash_->Get(*ctx_, key_, "f1", &got);
  EXPECT_EQ("v11", got);

  s = hash_->Size(*ctx_, key_, &ret);
  EXPECT_TRUE(s.ok() && ret == static_cast<uint64_t>(fvs.size() - 1));

  std::vector<rocksdb::Slice> fields_to_delete{"f1", "f2", "f2"};
  s = hash_->Delete(*ctx_, key_, fields_to_delete, &ret);
  EXPECT_TRUE(s.ok() && ret == static_cast<uint64_t>(fields_to_delete.size() - 1));
  s = hash_->Size(*ctx_, key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  s = hash_->Get(*ctx_, key_, "f3", &got);
  EXPECT_EQ("v3", got);

  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, MSetSingleFieldAndNX) {
  uint64_t ret = 0;
  std::vector<FieldValue> values = {{"field-one", "value-one"}};
  auto s = hash_->MSet(*ctx_, key_, values, true, &ret);
  EXPECT_TRUE(s.ok() && ret == 1);

  std::string field2 = "field-two";
  std::string initial_value = "value-two";
  s = hash_->Set(*ctx_, key_, field2, initial_value, &ret);
  EXPECT_TRUE(s.ok() && ret == 1);

  values = {{field2, "value-two-changed"}};
  s = hash_->MSet(*ctx_, key_, values, true, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);

  std::string final_value;
  s = hash_->Get(*ctx_, key_, field2, &final_value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(initial_value, final_value);

  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, MSetMultipleFieldsAndNX) {
  uint64_t ret = 0;
  std::vector<FieldValue> values = {{"field-one", "value-one"}, {"field-two", "value-two"}};
  auto s = hash_->MSet(*ctx_, key_, values, true, &ret);
  EXPECT_TRUE(s.ok() && ret == 2);

  values = {{"field-one", "value-one"}, {"field-two", "value-two-changed"}, {"field-three", "value-three"}};
  s = hash_->MSet(*ctx_, key_, values, true, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 1);

  std::string value;
  s = hash_->Get(*ctx_, key_, "field-one", &value);
  EXPECT_TRUE(s.ok() && value == "value-one");

  s = hash_->Get(*ctx_, key_, "field-two", &value);
  EXPECT_TRUE(s.ok() && value == "value-two");

  s = hash_->Get(*ctx_, key_, "field-three", &value);
  EXPECT_TRUE(s.ok() && value == "value-three");

  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, HGetAll) {
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(*ctx_, key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  std::vector<FieldValue> fvs;
  auto s = hash_->GetAll(*ctx_, key_, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == fields_.size());
  s = hash_->Delete(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, HIncr) {
  int64_t value = 0;
  Slice field("hash-incrby-invalid-field");
  for (int i = 0; i < 32; i++) {
    auto s = hash_->IncrBy(*ctx_, key_, field, 1, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  hash_->Get(*ctx_, key_, field, &bytes);
  auto parse_result = ParseInt<int64_t>(bytes, 10);
  if (!parse_result) {
    FAIL();
  }
  EXPECT_EQ(32, *parse_result);
  auto s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, HIncrInvalid) {
  uint64_t ret = 0;
  int64_t value = 0;
  Slice field("hash-incrby-invalid-field");
  auto s = hash_->IncrBy(*ctx_, key_, field, 1, &value);
  EXPECT_TRUE(s.ok() && value == 1);

  s = hash_->IncrBy(*ctx_, key_, field, LLONG_MAX, &value);
  EXPECT_TRUE(s.IsInvalidArgument());
  hash_->Set(*ctx_, key_, field, "abc", &ret);
  s = hash_->IncrBy(*ctx_, key_, field, 1, &value);
  EXPECT_TRUE(s.IsInvalidArgument());

  hash_->Set(*ctx_, key_, field, "-1", &ret);
  s = hash_->IncrBy(*ctx_, key_, field, -1, &value);
  EXPECT_TRUE(s.ok());
  s = hash_->IncrBy(*ctx_, key_, field, LLONG_MIN, &value);
  EXPECT_TRUE(s.IsInvalidArgument());

  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, HIncrByFloat) {
  double value = 0.0;
  Slice field("hash-incrbyfloat-invalid-field");
  for (int i = 0; i < 32; i++) {
    auto s = hash_->IncrByFloat(*ctx_, key_, field, 1.2, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  hash_->Get(*ctx_, key_, field, &bytes);
  value = std::stof(bytes);
  EXPECT_FLOAT_EQ(32 * 1.2, value);
  auto s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, HRangeByLex) {
  uint64_t ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < 4; i++) {
    fvs.emplace_back("key" + std::to_string(i), "value" + std::to_string(i));
  }
  for (size_t i = 0; i < 26; i++) {
    fvs.emplace_back(std::to_string(char(i + 'a')), std::to_string(char(i + 'a')));
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::vector<FieldValue> tmp(fvs);
  for (size_t i = 0; i < 100; i++) {
    std::shuffle(tmp.begin(), tmp.end(), g);
    auto s = hash_->MSet(*ctx_, key_, tmp, false, &ret);
    EXPECT_TRUE(s.ok() && tmp.size() == ret);
    s = hash_->MSet(*ctx_, key_, fvs, false, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 0);
    std::vector<FieldValue> result;
    RangeLexSpec spec;
    spec.offset = 0;
    spec.count = INT_MAX;
    spec.min = "key0";
    spec.max = "key3";
    s = hash_->RangeByLex(*ctx_, key_, spec, &result);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(4, result.size());
    EXPECT_EQ("key0", result[0].field);
    EXPECT_EQ("key1", result[1].field);
    EXPECT_EQ("key2", result[2].field);
    EXPECT_EQ("key3", result[3].field);
    s = hash_->Del(*ctx_, key_);
  }

  auto s = hash_->MSet(*ctx_, key_, tmp, false, &ret);
  EXPECT_TRUE(s.ok() && tmp.size() == ret);
  // use offset and count
  std::vector<FieldValue> result;
  RangeLexSpec spec;
  spec.offset = 0;
  spec.count = INT_MAX;
  spec.min = "key0";
  spec.max = "key3";
  spec.offset = 1;
  s = hash_->RangeByLex(*ctx_, key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("key1", result[0].field);
  EXPECT_EQ("key2", result[1].field);
  EXPECT_EQ("key3", result[2].field);

  spec.offset = 1;
  spec.count = 1;
  s = hash_->RangeByLex(*ctx_, key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, result.size());
  EXPECT_EQ("key1", result[0].field);

  spec.offset = 0;
  spec.count = 0;
  s = hash_->RangeByLex(*ctx_, key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());

  spec.offset = 1000;
  spec.count = 1000;
  s = hash_->RangeByLex(*ctx_, key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());
  // exclusive range
  spec.offset = 0;
  spec.count = -1;
  spec.minex = true;
  s = hash_->RangeByLex(*ctx_, key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("key1", result[0].field);
  EXPECT_EQ("key2", result[1].field);
  EXPECT_EQ("key3", result[2].field);

  spec.offset = 0;
  spec.count = -1;
  spec.maxex = true;
  spec.minex = false;
  s = hash_->RangeByLex(*ctx_, key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("key0", result[0].field);
  EXPECT_EQ("key1", result[1].field);
  EXPECT_EQ("key2", result[2].field);

  spec.offset = 0;
  spec.count = -1;
  spec.maxex = true;
  spec.minex = true;
  s = hash_->RangeByLex(*ctx_, key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("key1", result[0].field);
  EXPECT_EQ("key2", result[1].field);

  // inf and reversed
  spec.minex = false;
  spec.maxex = false;
  spec.min = "-";
  spec.max = "+";
  spec.max_infinite = true;
  spec.reversed = true;
  s = hash_->RangeByLex(*ctx_, key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(4 + 26, result.size());
  EXPECT_EQ("key3", result[0].field);
  EXPECT_EQ("key2", result[1].field);
  EXPECT_EQ("key1", result[2].field);
  EXPECT_EQ("key0", result[3].field);
  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, HRangeByLexNonExistingKey) {
  std::vector<FieldValue> result;
  RangeLexSpec spec;
  spec.offset = 0;
  spec.count = INT_MAX;
  spec.min = "any-start-key";
  spec.max = "any-end-key";
  auto s = hash_->RangeByLex(*ctx_, "non-existing-key", spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(result.size(), 0);
}

TEST_F(RedisHashTest, HRandField) {
  uint64_t ret = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(*ctx_, key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  auto size = static_cast<int64_t>(fields_.size());
  std::vector<FieldValue> fvs;
  // Case 1: Negative count, randomly select elements
  fvs.clear();
  auto s = hash_->RandField(*ctx_, key_, -(size + 10), &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == (fields_.size() + 10));

  // Case 2: Requested count is greater than or equal to the number of elements inside the hash
  fvs.clear();
  s = hash_->RandField(*ctx_, key_, size + 1, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == fields_.size());

  // Case 3: Requested count is less than the number of elements inside the hash
  fvs.clear();
  s = hash_->RandField(*ctx_, key_, size - 1, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == fields_.size() - 1);

  // hrandfield key 0
  fvs.clear();
  s = hash_->RandField(*ctx_, key_, 0, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == 0);

  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, ExpireFields) {
  uint64_t ret = 0;
  // Set up some fields
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(*ctx_, key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }

  // Expire two fields
  std::vector<Slice> fields_to_expire = {fields_[0], fields_[1]};
  std::vector<FieldExpireResult> results;
  uint64_t expire_time = util::GetTimeStampMS() + 1 * 1000;  // 2000 ms
  auto s = hash_->ExpireFields(*ctx_, key_, expire_time, fields_to_expire, &results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], FieldExpireResult::kExpireSet);  // First field expired successfully
  EXPECT_EQ(results[1], FieldExpireResult::kExpireSet);  // Second field expired successfully

  // Try to expire non-existent field
  std::vector<Slice> non_existent = {Slice("non_existent_field")};
  results.clear();
  s = hash_->ExpireFields(*ctx_, key_, expire_time, non_existent, &results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], FieldExpireResult::kFieldNotFound);  // Field doesn't exist

  // Wait for 2 keys expired
  std::this_thread::sleep_for(std::chrono::seconds(2));
  std::string value;
  s = hash_->Get(*ctx_, key_, fields_to_expire[0], &value);
  EXPECT_TRUE(s.IsNotFound());

  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, TTLFields) {
  uint64_t ret = 0;
  // Set up some fields
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(*ctx_, key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }

  // Get TTL for fields without expiration
  std::vector<int64_t> results;
  auto s = hash_->TTLFields(*ctx_, key_, fields_, &results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(results.size(), fields_.size());
  for (const auto &ttl : results) {
    EXPECT_EQ(ttl, -1);  // No TTL set
  }

  // Set expiration on first field
  std::vector<Slice> first_field = {fields_[0]};
  uint64_t expire_time = util::GetTimeStampMS() + 30000;  // 30 seconds
  std::vector<FieldExpireResult> expire_results;
  s = hash_->ExpireFields(*ctx_, key_, expire_time, first_field, &expire_results);
  EXPECT_TRUE(s.ok());

  // Get TTL again
  results.clear();
  s = hash_->TTLFields(*ctx_, key_, fields_, &results);
  EXPECT_TRUE(s.ok());
  EXPECT_GE(results[0], 29000);  // Should be around 30 seconds
  EXPECT_LE(results[0], 31000);
  EXPECT_EQ(results[1], -1);  // No TTL
  EXPECT_EQ(results[2], -1);  // No TTL

  // Get TTL for non-existent field
  std::vector<Slice> non_existent = {Slice("non_existent_field")};
  results.clear();
  s = hash_->TTLFields(*ctx_, key_, non_existent, &results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], -2);  // Field doesn't exist

  s = hash_->Del(*ctx_, key_);
}

TEST_F(RedisHashTest, PersistFields) {
  uint64_t ret = 0;
  // Set up some fields
  for (size_t i = 0; i < fields_.size(); i++) {
    auto s = hash_->Set(*ctx_, key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }

  // Set expiration on first two fields
  std::vector<Slice> fields_to_expire = {fields_[0], fields_[1]};
  std::vector<FieldExpireResult> expire_results;
  uint64_t expire_time = util::GetTimeStampMS() + 60000;
  auto s = hash_->ExpireFields(*ctx_, key_, expire_time, fields_to_expire, &expire_results);
  EXPECT_TRUE(s.ok());

  // Persist the first field
  std::vector<Slice> first_field = {fields_[0]};
  std::vector<FieldPersistResult> persist_results;
  s = hash_->PersistFields(*ctx_, key_, first_field, &persist_results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(persist_results.size(), 1);
  EXPECT_EQ(persist_results[0], FieldPersistResult::kPersisted);  // Expiration removed

  // Check TTL - should be -1 now
  std::vector<int64_t> ttl_results;
  s = hash_->TTLFields(*ctx_, key_, first_field, &ttl_results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ttl_results[0], -1);

  // Try to persist a field without TTL
  std::vector<Slice> third_field = {fields_[2]};
  persist_results.clear();
  s = hash_->PersistFields(*ctx_, key_, third_field, &persist_results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(persist_results[0], FieldPersistResult::kNotVolatile);  // Field exists but has no TTL

  // Try to persist non-existent field
  std::vector<Slice> non_existent = {Slice("non_existent_field")};
  persist_results.clear();
  s = hash_->PersistFields(*ctx_, key_, non_existent, &persist_results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(persist_results[0], FieldPersistResult::kFieldNotFound);  // Field doesn't exist

  s = hash_->Del(*ctx_, key_);
}