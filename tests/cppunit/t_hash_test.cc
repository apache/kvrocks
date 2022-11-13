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

#include "parse_util.h"
#include "test_base.h"
#include "types/redis_hash.h"
class RedisHashTest : public TestBase {
 protected:
  explicit RedisHashTest() : TestBase() { hash = std::make_unique<Redis::Hash>(storage_, "hash_ns"); }
  ~RedisHashTest() = default;
  void SetUp() override {
    key_ = "test_hash->key";
    fields_ = {"test-hash-key-1", "test-hash-key-2", "test-hash-key-3"};
    values_ = {"hash-test-value-1", "hash-test-value-2", "hash-test-value-3"};
  }
  void TearDown() override {}

 protected:
  std::unique_ptr<Redis::Hash> hash;
};

TEST_F(RedisHashTest, GetAndSet) {
  int ret;
  for (size_t i = 0; i < fields_.size(); i++) {
    rocksdb::Status s = hash->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string got;
    rocksdb::Status s = hash->Get(key_, fields_[i], &got);
    EXPECT_EQ(values_[i], got);
  }
  rocksdb::Status s = hash->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  hash->Del(key_);
}

TEST_F(RedisHashTest, MGetAndMSet) {
  int ret;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(FieldValue{fields_[i].ToString(), values_[i].ToString()});
  }
  rocksdb::Status s = hash->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fvs.size()) == ret);
  s = hash->MSet(key_, fvs, false, &ret);
  EXPECT_EQ(ret, 0);
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  s = hash->MGet(key_, fields_, &values, &statuses);
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(values[i], values_[i].ToString());
  }
  s = hash->Delete(key_, fields_, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  hash->Del(key_);
}

TEST_F(RedisHashTest, SetNX) {
  int ret;
  Slice field("foo");
  rocksdb::Status s = hash->Set(key_, field, "bar", &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  s = hash->Set(key_, field, "bar", &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  std::vector<Slice> fields = {field};
  s = hash->Delete(key_, fields, &ret);
  EXPECT_EQ(fields.size(), ret);
  hash->Del(key_);
}

TEST_F(RedisHashTest, HGetAll) {
  int ret;
  for (size_t i = 0; i < fields_.size(); i++) {
    rocksdb::Status s = hash->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  std::vector<FieldValue> fvs;
  rocksdb::Status s = hash->GetAll(key_, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == fields_.size());
  s = hash->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  hash->Del(key_);
}

TEST_F(RedisHashTest, HIncr) {
  int64_t value;
  Slice field("hash-incrby-invalid-field");
  for (int i = 0; i < 32; i++) {
    rocksdb::Status s = hash->IncrBy(key_, field, 1, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  hash->Get(key_, field, &bytes);
  auto parseResult = ParseInt<int64_t>(bytes, 10);
  if (!parseResult) {
    FAIL();
  }
  EXPECT_EQ(32, *parseResult);
  hash->Del(key_);
}

TEST_F(RedisHashTest, HIncrInvalid) {
  int ret;
  int64_t value;
  Slice field("hash-incrby-invalid-field");
  rocksdb::Status s = hash->IncrBy(key_, field, 1, &value);
  EXPECT_TRUE(s.ok() && value == 1);

  s = hash->IncrBy(key_, field, LLONG_MAX, &value);
  EXPECT_TRUE(s.IsInvalidArgument());
  hash->Set(key_, field, "abc", &ret);
  s = hash->IncrBy(key_, field, 1, &value);
  EXPECT_TRUE(s.IsInvalidArgument());

  hash->Set(key_, field, "-1", &ret);
  s = hash->IncrBy(key_, field, -1, &value);
  EXPECT_TRUE(s.ok());
  s = hash->IncrBy(key_, field, LLONG_MIN, &value);
  EXPECT_TRUE(s.IsInvalidArgument());

  hash->Del(key_);
}

TEST_F(RedisHashTest, HIncrByFloat) {
  double value;
  Slice field("hash-incrbyfloat-invalid-field");
  for (int i = 0; i < 32; i++) {
    rocksdb::Status s = hash->IncrByFloat(key_, field, 1.2, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  hash->Get(key_, field, &bytes);
  value = std::stof(bytes);
  EXPECT_FLOAT_EQ(32 * 1.2, value);
  hash->Del(key_);
}

TEST_F(RedisHashTest, HRangeByLex) {
  int ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < 4; i++) {
    fvs.emplace_back(FieldValue{"key" + std::to_string(i), "value" + std::to_string(i)});
  }
  for (size_t i = 0; i < 26; i++) {
    fvs.emplace_back(FieldValue{std::to_string(char(i + 'a')), std::to_string(char(i + 'a'))});
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::vector<FieldValue> tmp(fvs);
  for (size_t i = 0; i < 100; i++) {
    std::shuffle(tmp.begin(), tmp.end(), g);
    rocksdb::Status s = hash->MSet(key_, tmp, false, &ret);
    EXPECT_TRUE(s.ok() && static_cast<int>(tmp.size()) == ret);
    s = hash->MSet(key_, fvs, false, &ret);
    EXPECT_EQ(ret, 0);
    std::vector<FieldValue> result;
    HashSpec spec;
    spec.offset = 0;
    spec.count = INT_MAX;
    spec.min = "key0";
    spec.max = "key3";
    s = hash->RangeByLex(key_, spec, &result);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(4, result.size());
    EXPECT_EQ("key0", result[0].field);
    EXPECT_EQ("key1", result[1].field);
    EXPECT_EQ("key2", result[2].field);
    EXPECT_EQ("key3", result[3].field);
    hash->Del(key_);
  }

  rocksdb::Status s = hash->MSet(key_, tmp, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(tmp.size()) == ret);
  // use offset and count
  std::vector<FieldValue> result;
  HashSpec spec;
  spec.offset = 0;
  spec.count = INT_MAX;
  spec.min = "key0";
  spec.max = "key3";
  spec.offset = 1;
  s = hash->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("key1", result[0].field);
  EXPECT_EQ("key2", result[1].field);
  EXPECT_EQ("key3", result[2].field);

  spec.offset = 1;
  spec.count = 1;
  s = hash->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, result.size());
  EXPECT_EQ("key1", result[0].field);

  spec.offset = 0;
  spec.count = 0;
  s = hash->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());

  spec.offset = 1000;
  spec.count = 1000;
  s = hash->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());
  // exclusive range
  spec.offset = 0;
  spec.count = -1;
  spec.minex = true;
  s = hash->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("key1", result[0].field);
  EXPECT_EQ("key2", result[1].field);
  EXPECT_EQ("key3", result[2].field);

  spec.offset = 0;
  spec.count = -1;
  spec.maxex = true;
  spec.minex = false;
  s = hash->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, result.size());
  EXPECT_EQ("key0", result[0].field);
  EXPECT_EQ("key1", result[1].field);
  EXPECT_EQ("key2", result[2].field);

  spec.offset = 0;
  spec.count = -1;
  spec.maxex = true;
  spec.minex = true;
  s = hash->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("key1", result[0].field);
  EXPECT_EQ("key2", result[1].field);

  // inf and revered
  spec.minex = false;
  spec.maxex = false;
  spec.min = "-";
  spec.max = "+";
  spec.max_infinite = true;
  spec.reversed = true;
  s = hash->RangeByLex(key_, spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(4 + 26, result.size());
  EXPECT_EQ("key3", result[0].field);
  EXPECT_EQ("key2", result[1].field);
  EXPECT_EQ("key1", result[2].field);
  EXPECT_EQ("key0", result[3].field);
  hash->Del(key_);
}

TEST_F(RedisHashTest, HRangeUseIndex) {
  hash->Del(key_);
  int ret = 0;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < 10; i++) {
    fvs.emplace_back(FieldValue{std::string("key") + static_cast<char>(i + 'a'), std::to_string(i)});
  }
  std::random_device rd;
  std::mt19937 g(rd());
  std::vector<FieldValue> tmp(fvs);
  std::vector<FieldValue> result;
  for (size_t i = 0; i < 100; i++) {
    std::shuffle(tmp.begin(), tmp.end(), g);
    rocksdb::Status s = hash->MSet(key_, tmp, false, &ret);
    EXPECT_TRUE(s.ok() && static_cast<int>(tmp.size()) == ret);
    s = hash->MSet(key_, fvs, false, &ret);
    EXPECT_EQ(ret, 0);
    s = hash->Range(key_, 0, -1, 0, -1, false, &result);
    EXPECT_TRUE(s.ok());
    for (size_t j = 0; j < fvs.size(); j++) {
      EXPECT_EQ(fvs[j].field, result[j].field);
      EXPECT_EQ(fvs[j].value, result[j].value);
    }
    hash->Del(key_);
  }
  rocksdb::Status s = hash->MSet(key_, tmp, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(tmp.size()) == ret);

  // test limit and rev
  s = hash->Range(key_, 0, -1, 0, 2, false, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("keya", result[0].field);
  EXPECT_EQ("keyb", result[1].field);

  s = hash->Range(key_, 0, -1, 1, 2, false, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("keyb", result[0].field);
  EXPECT_EQ("keyc", result[1].field);

  s = hash->Range(key_, 0, -1, 1000, -1, false, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());

  s = hash->Range(key_, 0, -1, 0, 0, false, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());

  s = hash->Range(key_, 1, 2, 0, -1, false, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("keyb", result[0].field);
  EXPECT_EQ("keyc", result[1].field);

  s = hash->Range(key_, 1, 2, 1, 2, false, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("keyc", result[0].field);
  EXPECT_EQ("keyd", result[1].field);

  s = hash->Range(key_, 5, 10, 6, 2, false, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());

  s = hash->Range(key_, 10, 1, 0, -1, false, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, result.size());

  s = hash->Range(key_, 0, 10, 0, 2, true, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("keyj", result[0].field);
  EXPECT_EQ("keyi", result[1].field);

  s = hash->Range(key_, 0, 10, 1, 2, true, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, result.size());
  EXPECT_EQ("keyi", result[0].field);
  EXPECT_EQ("keyh", result[1].field);
}

TEST_F(RedisHashTest, HRangeByLexNonExistingKey) {
  std::vector<FieldValue> result;
  HashSpec spec;
  spec.offset = 0;
  spec.count = INT_MAX;
  spec.min = "any-start-key";
  spec.max = "any-end-key";
  auto s = hash->RangeByLex("non-existing-key", spec, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(result.size(), 0);
}
