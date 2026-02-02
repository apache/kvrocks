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
#include <chrono>
#include <climits>
#include <memory>
#include <random>
#include <set>
#include <string>

#include "parse_util.h"
#include "test_base.h"
#include "time_util.h"
#include "types/redis_hash.h"

class RedisHashTTLTest : public TestBase {
 protected:
  explicit RedisHashTTLTest() { hash_ = std::make_unique<redis::Hash>(storage_.get(), "hash_ns"); }
  ~RedisHashTTLTest() override = default;

  void SetUp() override {
    key_ = "test_hash_ttl_key";
    non_expired_fields_ = {"field1", "field2", "field3"};
    non_expired_values_ = {"value1", "value2", "value3"};
    expired_fields_ = {"expired_field1", "expired_field2"};
    expired_values_ = {"expired_value1", "expired_value2"};
  }
  void TearDown() override {}

  // Helper to set up initial data with some fields having TTL
  void setupHashWithMixedFields() {
    uint64_t ret = 0;

    // Set non-expired fields
    for (size_t i = 0; i < non_expired_fields_.size(); i++) {
      auto s = hash_->Set(*ctx_, key_, non_expired_fields_[i], non_expired_values_[i], &ret);
      EXPECT_TRUE(s.ok() && ret == 1);
    }

    // Set expired fields with very short TTL
    for (size_t i = 0; i < expired_fields_.size(); i++) {
      auto s = hash_->Set(*ctx_, key_, expired_fields_[i], expired_values_[i], &ret);
      EXPECT_TRUE(s.ok() && ret == 1);

      // Set expiration for 100ms
      std::vector<Slice> field_to_expire = {expired_fields_[i]};
      std::vector<FieldExpireResult> expire_results;
      uint64_t expire_time = util::GetTimeStampMS() + 100;  // 100ms
      s = hash_->ExpireFields(*ctx_, key_, expire_time, field_to_expire, &expire_results);
      EXPECT_TRUE(s.ok());
      EXPECT_EQ(expire_results[0], FieldExpireResult::kExpireSet);
    }

    // Wait for expired fields to actually expire
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  std::unique_ptr<redis::Hash> hash_;
  std::vector<std::string> non_expired_fields_;
  std::vector<std::string> non_expired_values_;
  std::vector<std::string> expired_fields_;
  std::vector<std::string> expired_values_;
};

// Test Size command with mixed expired and non-expired fields
TEST_F(RedisHashTTLTest, SizeWithExpiredFields) {
  setupHashWithMixedFields();

  uint64_t size = 0;
  auto s = hash_->Size(*ctx_, key_, &size);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(size, non_expired_fields_.size());  // Should only count non-expired fields

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test Get command with expired field
TEST_F(RedisHashTTLTest, GetExpiredField) {
  setupHashWithMixedFields();

  // Try to get expired field
  std::string value;
  auto s = hash_->Get(*ctx_, key_, expired_fields_[0], &value);
  EXPECT_TRUE(s.IsNotFound());  // Should return NotFound for expired field

  // Get non-expired field
  s = hash_->Get(*ctx_, key_, non_expired_fields_[0], &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, non_expired_values_[0]);

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test Delete command with mixed fields
TEST_F(RedisHashTTLTest, DeleteMixedFields) {
  uint64_t ret = 0;

  // Set up fields with some having TTL
  for (size_t i = 0; i < non_expired_fields_.size(); i++) {
    auto s = hash_->Set(*ctx_, key_, non_expired_fields_[i], non_expired_values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }

  // Set expired field
  auto s = hash_->Set(*ctx_, key_, "expired_field", "value", &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  std::vector<Slice> field_to_expire = {"expired_field"};
  std::vector<FieldExpireResult> expire_results;
  uint64_t expire_time = util::GetTimeStampMS() + 100;
  s = hash_->ExpireFields(*ctx_, key_, expire_time, field_to_expire, &expire_results);
  EXPECT_TRUE(s.ok());

  // Wait for expiration
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Delete mix of expired and non-expired fields
  std::vector<Slice> fields_to_delete;
  fields_to_delete.push_back("expired_field");         // expired
  fields_to_delete.push_back(non_expired_fields_[0]);  // non-expired

  s = hash_->Delete(*ctx_, key_, fields_to_delete, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 1);  // Should only delete 1 non-expired field

  // Verify non-expired field is deleted
  std::string value;
  s = hash_->Get(*ctx_, key_, non_expired_fields_[0], &value);
  EXPECT_TRUE(s.IsNotFound());

  // Verify size is correct
  uint64_t size = 0;
  s = hash_->Size(*ctx_, key_, &size);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(size, non_expired_fields_.size() - 1);  // One less

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test IncrBy with expired field
TEST_F(RedisHashTTLTest, IncrByExpiredField) {
  setupHashWithMixedFields();

  // Increment expired field (should work as if field doesn't exist)
  int64_t value = 0;
  auto s = hash_->IncrBy(*ctx_, key_, expired_fields_[0], 5, &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, 5);  // Should start from 0 and increment

  // Verify the field now exists with the incremented value
  std::string result;
  s = hash_->Get(*ctx_, key_, expired_fields_[0], &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(result, "5");

  // Try increment on non-expired field
  s = hash_->IncrBy(*ctx_, key_, non_expired_fields_[0], 10, &value);
  EXPECT_FALSE(s.ok());
  // Should fail because non_expired_fields_[0] has a non-numeric value
  // Let's test with a numeric field instead

  uint64_t added = 0;
  s = hash_->Set(*ctx_, key_, "numeric_field", "100", &added);
  EXPECT_TRUE(s.ok());
  s = hash_->IncrBy(*ctx_, key_, "numeric_field", 5, &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, 105);

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test IncrByFloat with expired field
TEST_F(RedisHashTTLTest, IncrByFloatExpiredField) {
  setupHashWithMixedFields();

  // Increment expired field with float (should work as if field doesn't exist)
  double value = 0.0;
  auto s = hash_->IncrByFloat(*ctx_, key_, expired_fields_[0], 3.14, &value);
  EXPECT_TRUE(s.ok());
  EXPECT_DOUBLE_EQ(value, 3.14);  // Should start from 0 and increment

  // Verify the field now exists with the incremented value
  std::string result;
  s = hash_->Get(*ctx_, key_, expired_fields_[0], &result);
  EXPECT_TRUE(s.ok());
  EXPECT_DOUBLE_EQ(std::stod(result), 3.14);

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test MSet with expired fields
TEST_F(RedisHashTTLTest, MSetWithExpiredFields) {
  setupHashWithMixedFields();

  uint64_t ret = 0;
  std::vector<FieldValue> fvs;

  // Mix of new fields, expired fields, and existing non-expired fields
  fvs.emplace_back(non_expired_fields_[0], "updated_value1");  // update non-expired
  fvs.emplace_back(expired_fields_[0], "new_value_expired1");  // expired field treated as new
  fvs.emplace_back("brand_new_field", "brand_new_value");      // completely new field

  auto s = hash_->MSet(*ctx_, key_, fvs, false, &ret);  // false = don't use NX
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 2);  // only expired and brand new was added

  // Verify all fields were set correctly
  std::string value;

  // Check updated non-expired field
  s = hash_->Get(*ctx_, key_, non_expired_fields_[0], &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "updated_value1");

  // Check field that was expired
  s = hash_->Get(*ctx_, key_, expired_fields_[0], &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "new_value_expired1");

  // Check new field
  s = hash_->Get(*ctx_, key_, "brand_new_field", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "brand_new_value");

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test MGet with mixed expired and non-expired fields
TEST_F(RedisHashTTLTest, MGetWithExpiredFields) {
  setupHashWithMixedFields();

  std::vector<Slice> fields_to_get;
  fields_to_get.push_back(non_expired_fields_[0]);  // non-expired
  fields_to_get.push_back(expired_fields_[0]);      // expired
  fields_to_get.push_back("non_existent_field");    // never existed
  fields_to_get.push_back(non_expired_fields_[1]);  // non-expired

  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  auto s = hash_->MGet(*ctx_, key_, fields_to_get, &values, &statuses);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(values.size(), 4);
  EXPECT_EQ(statuses.size(), 4);

  // Check non-expired field
  EXPECT_TRUE(statuses[0].ok());
  EXPECT_EQ(values[0], non_expired_values_[0]);

  // Check expired field - should return empty string with NotFound status
  EXPECT_TRUE(statuses[1].IsNotFound());
  EXPECT_EQ(values[1], "");

  // Check non-existent field - should return empty string with NotFound status
  EXPECT_TRUE(statuses[2].IsNotFound());
  EXPECT_EQ(values[2], "");

  // Check another non-expired field
  EXPECT_TRUE(statuses[3].ok());
  EXPECT_EQ(values[3], non_expired_values_[1]);

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test GetAll with expired fields
TEST_F(RedisHashTTLTest, GetAllWithExpiredFields) {
  setupHashWithMixedFields();

  std::vector<FieldValue> fvs;
  auto s = hash_->GetAll(*ctx_, key_, &fvs);
  EXPECT_TRUE(s.ok());

  // Should only get non-expired fields
  EXPECT_EQ(fvs.size(), non_expired_fields_.size());

  // Verify all returned fields are non-expired
  std::set<std::string> returned_fields;
  for (const auto& fv : fvs) {
    returned_fields.insert(fv.field);
  }

  for (const auto& field : non_expired_fields_) {
    EXPECT_TRUE(returned_fields.find(field) != returned_fields.end());
  }

  for (const auto& field : expired_fields_) {
    EXPECT_TRUE(returned_fields.find(field) == returned_fields.end());
  }

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test RangeByLex with expired fields
TEST_F(RedisHashTTLTest, RangeByLexWithExpiredFields) {
  setupHashWithMixedFields();

  // Add some lexically ordered fields for better testing
  std::vector<FieldValue> lex_fields = {
      {"alpha", "value_alpha"}, {"beta", "value_beta"}, {"gamma", "value_gamma"}, {"delta", "value_delta"}};

  uint64_t ret = 0;
  for (const auto& fv : lex_fields) {
    auto s = hash_->Set(*ctx_, key_, fv.field, fv.value, &ret);
    EXPECT_TRUE(s.ok());
  }

  // Set one of the lex fields to expire
  std::vector<Slice> field_to_expire = {Slice("beta")};
  std::vector<FieldExpireResult> expire_results;
  uint64_t expire_time = util::GetTimeStampMS() + 100;
  auto s = hash_->ExpireFields(*ctx_, key_, expire_time, field_to_expire, &expire_results);
  EXPECT_TRUE(s.ok());

  // Wait for expiration
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Test RangeByLex - should not include expired field
  std::vector<FieldValue> result;
  RangeLexSpec spec;
  spec.offset = 0;
  spec.count = INT_MAX;
  spec.min = "alpha";
  spec.max = "gamma";

  s = hash_->RangeByLex(*ctx_, key_, spec, &result);
  EXPECT_TRUE(s.ok());

  // Should have alpha, gamma (beta is expired), plus our non-expired fields
  // Non-expired fields are: field1, field2, field3
  // Lex fields in range: alpha, delta gamma
  // Expected: 6 fields total
  EXPECT_EQ(result.size(), 6);

  // Check that beta is not in results
  for (const auto& fv : result) {
    EXPECT_NE(fv.field, "beta");
  }

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test Scan with expired fields
TEST_F(RedisHashTTLTest, ScanWithExpiredFields) {
  setupHashWithMixedFields();

  std::string cursor = "0";
  std::vector<std::string> fvs;
  int limit = 10;

  auto s = hash_->Scan(*ctx_, key_, cursor, limit, "", &fvs);
  EXPECT_TRUE(s.ok());

  // Scan should only return non-expired fields
  EXPECT_EQ(fvs.size(), non_expired_fields_.size());

  // Verify no expired fields are returned
  for (const auto& fv : fvs) {
    for (const auto& expired_field : expired_fields_) {
      EXPECT_NE(fv, expired_field);
    }
  }

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test RandField with expired fields
TEST_F(RedisHashTTLTest, RandFieldWithExpiredFields) {
  setupHashWithMixedFields();

  auto size = static_cast<int64_t>(non_expired_fields_.size());
  std::vector<FieldValue> fvs;

  // Request random fields - should only return non-expired ones
  auto s = hash_->RandField(*ctx_, key_, size + 5, &fvs);  // Request more than available
  EXPECT_TRUE(s.ok());

  // Should only get non-expired fields (no duplicates since we request exact count)
  EXPECT_EQ(fvs.size(), non_expired_fields_.size());

  // Verify no expired fields are returned
  for (const auto& fv : fvs) {
    for (const auto& expired_field : expired_fields_) {
      EXPECT_NE(fv.field, expired_field);
    }
  }

  // Test with negative count (allows duplicates)
  fvs.clear();
  s = hash_->RandField(*ctx_, key_, -10, &fvs);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs.size(), 10);  // Should return exactly 10

  // Again verify no expired fields
  for (const auto& fv : fvs) {
    for (const auto& expired_field : expired_fields_) {
      EXPECT_NE(fv.field, expired_field);
    }
  }

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test edge case: all fields expired
TEST_F(RedisHashTTLTest, AllFieldsExpired) {
  setupHashWithMixedFields();

  // Expire all non-expired fields
  std::vector<Slice> fields_to_expire;
  for (const auto& field : non_expired_fields_) {
    fields_to_expire.push_back(field);
  }

  std::vector<FieldExpireResult> expire_results;
  uint64_t expire_time = util::GetTimeStampMS() + 100;
  auto s = hash_->ExpireFields(*ctx_, key_, expire_time, fields_to_expire, &expire_results);
  EXPECT_TRUE(s.ok());

  // Wait for all to expire
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Test various commands on hash with all expired fields

  // Size should be 0
  uint64_t size = 0;
  s = hash_->Size(*ctx_, key_, &size);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(size, 0);

  // Get on any field should return NotFound
  std::string value;
  s = hash_->Get(*ctx_, key_, non_expired_fields_[0], &value);
  EXPECT_TRUE(s.IsNotFound());

  // GetAll should return empty
  std::vector<FieldValue> fvs;
  s = hash_->GetAll(*ctx_, key_, &fvs);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs.size(), 0);

  // Scan should return empty
  std::string cursor = "0";
  std::vector<std::string> scan_keys;
  s = hash_->Scan(*ctx_, key_, cursor, 10, "", &scan_keys);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs.size(), 0);

  // RandField should return empty
  s = hash_->RandField(*ctx_, key_, 5, &fvs);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs.size(), 0);

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}

// Test that operations work correctly when field expires during operation
TEST_F(RedisHashTTLTest, FieldExpiresDuringOperations) {
  uint64_t ret = 0;

  // Set a field with short TTL
  auto s = hash_->Set(*ctx_, key_, "temp_field", "temp_value", &ret);
  EXPECT_TRUE(s.ok() && ret == 1);

  std::vector<Slice> field_to_expire = {Slice("temp_field")};
  std::vector<FieldExpireResult> expire_results;
  uint64_t expire_time = util::GetTimeStampMS() + 100;
  s = hash_->ExpireFields(*ctx_, key_, expire_time, field_to_expire, &expire_results);
  EXPECT_TRUE(s.ok());

  // Set non-expired field
  s = hash_->Set(*ctx_, key_, "permanent_field", "permanent_value", &ret);
  EXPECT_TRUE(s.ok() && ret == 1);

  // Wait for expiration
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Verify the field that expired is treated as if it doesn't exist
  std::string value;
  s = hash_->Get(*ctx_, key_, "temp_field", &value);
  EXPECT_TRUE(s.IsNotFound());

  // But permanent field still works
  s = hash_->Get(*ctx_, key_, "permanent_field", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "permanent_value");

  // Size should only count permanent field
  uint64_t size = 0;
  s = hash_->Size(*ctx_, key_, &size);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(size, 1);

  auto cleanup_s = hash_->Del(*ctx_, key_);
  EXPECT_TRUE(cleanup_s.ok());
}
