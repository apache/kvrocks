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
#include <cassert>
#include <climits>
#include <cstdint>
#include <filesystem>
#include <fstream>
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

class RedisHashFieldExpirationEncodingTest : public ::testing::Test {
 public:
  RedisHashFieldExpirationEncodingTest(const RedisHashFieldExpirationEncodingTest &) = delete;
  RedisHashFieldExpirationEncodingTest &operator=(const RedisHashFieldExpirationEncodingTest &) = delete;

 protected:
  RedisHashFieldExpirationEncodingTest() {
    const char *path = "test_hash_field_expiration.conf";
    unlink(path);
    std::ofstream output_file(path, std::ios::out);
    output_file << "hash-encoding-mode field-expiration\n";
    output_file.close();

    auto s = config_.Load(CLIOptions(path));
    assert(s.IsOK());
    config_.db_dir = "testdb_hash_field_expiration";
    config_.rocks_db.compression = rocksdb::CompressionType::kNoCompression;
    config_.rocks_db.write_buffer_size = 1;
    config_.rocks_db.block_size = 100;

    storage_ = std::make_unique<engine::Storage>(&config_);
    s = storage_->Open();
    assert(s.IsOK());

    ctx_ = std::make_unique<engine::Context>(storage_.get());
    hash_ = std::make_unique<redis::Hash>(storage_.get(), "hash_ns");
    db_ = std::make_unique<redis::Database>(storage_.get(), "hash_ns");
  }

  ~RedisHashFieldExpirationEncodingTest() override {
    ctx_.reset();
    hash_.reset();
    db_.reset();
    storage_.reset();

    std::error_code ec;
    std::filesystem::remove_all(config_.db_dir, ec);
    unlink("test_hash_field_expiration.conf");
  }

  std::string rawHashValue(const std::string &key, const std::string &field, HashMetadata *metadata) {
    std::string ns_key = db_->AppendNamespacePrefix(key);
    auto s = db_->GetMetadata(*ctx_, {kRedisHash}, ns_key, metadata);
    assert(s.ok());
    std::string sub_key = InternalKey(ns_key, field, metadata->version, storage_->IsSlotIdEncoded()).Encode();
    std::string raw_value;
    s = storage_->Get(*ctx_, ctx_->GetReadOptions(), sub_key, &raw_value);
    assert(s.ok());
    return raw_value;
  }

  HashMetadata hashMetadata(const std::string &key) {
    HashMetadata metadata(false);
    std::string ns_key = db_->AppendNamespacePrefix(key);
    auto s = db_->GetMetadata(*ctx_, {kRedisHash}, ns_key, &metadata);
    assert(s.ok());
    return metadata;
  }

  rocksdb::Status getHashMetadata(const std::string &key, HashMetadata *metadata) {
    std::string ns_key = db_->AppendNamespacePrefix(key);
    return db_->GetMetadata(*ctx_, {kRedisHash}, ns_key, metadata);
  }

  std::string hashSubKey(const std::string &key, const std::string &field, const HashMetadata &metadata) {
    std::string ns_key = db_->AppendNamespacePrefix(key);
    return InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  }

  rocksdb::Status putRawHashValue(const std::string &key, const std::string &field, uint64_t expire,
                                  const std::string &value) {
    HashMetadata metadata = hashMetadata(key);
    auto batch = storage_->GetWriteBatchBase();
    auto s = batch->Put(hashSubKey(key, field, metadata), metadata.EncodeSubkeyValue(value, expire));
    if (!s.ok()) return s;
    return storage_->Write(*ctx_, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }

  rocksdb::Status putHashMetadata(const std::string &key, const HashMetadata &metadata) {
    std::string bytes;
    metadata.Encode(&bytes);
    auto batch = storage_->GetWriteBatchBase();
    auto s = batch->Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), db_->AppendNamespacePrefix(key), bytes);
    if (!s.ok()) return s;
    return storage_->Write(*ctx_, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }

  rocksdb::Status deleteRawHashValue(const std::string &key, const std::string &field) {
    HashMetadata metadata = hashMetadata(key);
    auto batch = storage_->GetWriteBatchBase();
    auto s = batch->Delete(hashSubKey(key, field, metadata));
    if (!s.ok()) return s;
    return storage_->Write(*ctx_, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }

  Config config_;
  std::unique_ptr<engine::Storage> storage_;
  std::unique_ptr<engine::Context> ctx_;
  std::unique_ptr<redis::Hash> hash_;
  std::unique_ptr<redis::Database> db_;
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

TEST_F(RedisHashFieldExpirationEncodingTest, StoreAndScanValuesWithModeOneEncoding) {
  const Slice key = "mode-one-hash";
  const Slice field = "field-1";
  const Slice value = "value-1";

  uint64_t added = 0;
  auto s = hash_->Set(*ctx_, key, field, value, &added);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(added, 1);

  HashMetadata metadata(false);
  std::string raw_value = rawHashValue(key.ToString(), field.ToString(), &metadata);
  EXPECT_EQ(metadata.mode, HashSubkeyEncodingMode::kFieldExpiration);
  EXPECT_EQ(metadata.size, 1);
  EXPECT_EQ(metadata.persist, 1);
  EXPECT_EQ(raw_value.size(), HashMetadata::kFieldExpirationPrefixSize + value.size());

  Slice decoded_value(raw_value);
  uint64_t field_expire = UINT64_MAX;
  ASSERT_TRUE(metadata.DecodeSubkeyValue(&decoded_value, &field_expire).ok());
  EXPECT_EQ(decoded_value.ToStringView(), value.ToStringView());
  EXPECT_EQ(field_expire, 0);

  std::string got;
  s = hash_->Get(*ctx_, key, field, &got);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(got, value.ToString());

  std::vector<std::string> fields;
  std::vector<std::string> values;
  s = hash_->Scan(*ctx_, key, "", 10, "", &fields, &values);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(fields, std::vector<std::string>({"field-1"}));
  ASSERT_EQ(values, std::vector<std::string>({"value-1"}));

  std::vector<FieldValue> field_values;
  s = hash_->GetAll(*ctx_, key, &field_values);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_values.size(), 1);
  EXPECT_EQ(field_values[0].field, "field-1");
  EXPECT_EQ(field_values[0].value, "value-1");
}

TEST_F(RedisHashFieldExpirationEncodingTest, PersistentCountTracksPersistentFieldWrites) {
  const Slice key = "mode-one-persist-count";

  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"field-1", "1"}, {"field-2", "2"}}, false, &ret);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(ret, 2);

  HashMetadata metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.mode, HashSubkeyEncodingMode::kFieldExpiration);
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 2);
  EXPECT_EQ(metadata.lower, 0);
  EXPECT_EQ(metadata.upper, 0);

  int64_t new_int = 0;
  s = hash_->IncrBy(*ctx_, key, "field-3", 3, &new_int);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(new_int, 3);

  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 3);
  EXPECT_EQ(metadata.persist, 3);

  double new_float = 0;
  s = hash_->IncrByFloat(*ctx_, key, "field-4", 1.5, &new_float);
  ASSERT_TRUE(s.ok());
  EXPECT_DOUBLE_EQ(new_float, 1.5);

  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 4);
  EXPECT_EQ(metadata.persist, 4);

  s = hash_->Delete(*ctx_, key, {"field-1", "field-2"}, &ret);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(ret, 2);

  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 2);
  EXPECT_EQ(metadata.lower, 0);
  EXPECT_EQ(metadata.upper, 0);
}

TEST_F(RedisHashFieldExpirationEncodingTest, ExpireFieldsMaintainsPersistentToTTLAndTTLToTTLMetadata) {
  const Slice key = "hfe-expire-fields-metadata";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"a", "1"}, {"b", "2"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 2);

  HashMetadata metadata = hashMetadata(key.ToString());
  ASSERT_EQ(metadata.size, 2);
  ASSERT_EQ(metadata.persist, 2);
  ASSERT_EQ(metadata.lower, 0);
  ASSERT_EQ(metadata.upper, 0);

  std::vector<int64_t> results;
  uint64_t now = util::GetTimeStampMS();
  uint64_t t10 = now + 10'000;
  uint64_t t20 = now + 20'000;
  uint64_t t5 = now + 5'000;
  uint64_t t30 = now + 30'000;

  s = hash_->ExpireFields(*ctx_, key, {"a"}, t10, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(results, std::vector<int64_t>({1}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 1);
  EXPECT_EQ(metadata.lower, t10);
  EXPECT_EQ(metadata.upper, t10);

  s = hash_->ExpireFields(*ctx_, key, {"b"}, t20, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(results, std::vector<int64_t>({1}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 0);
  EXPECT_EQ(metadata.lower, t10);
  EXPECT_EQ(metadata.upper, t20);

  s = hash_->ExpireFields(*ctx_, key, {"b"}, t5, HashFieldExpireCondition::kLT, &results);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(results, std::vector<int64_t>({1}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 0);
  EXPECT_EQ(metadata.lower, t5);
  EXPECT_EQ(metadata.upper, t20);

  s = hash_->ExpireFields(*ctx_, key, {"a"}, t30, HashFieldExpireCondition::kGT, &results);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(results, std::vector<int64_t>({1}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 0);
  EXPECT_EQ(metadata.lower, t5);
  EXPECT_EQ(metadata.upper, t30);
}

TEST_F(RedisHashFieldExpirationEncodingTest, PersistFieldsMaintainsTTLToPersistentMetadata) {
  const Slice key = "hfe-persist-fields-metadata";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"a", "1"}, {"b", "2"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 2);

  std::vector<int64_t> results;
  uint64_t now = util::GetTimeStampMS();
  uint64_t t10 = now + 10'000;
  uint64_t t20 = now + 20'000;
  s = hash_->ExpireFields(*ctx_, key, {"a"}, t10, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());
  s = hash_->ExpireFields(*ctx_, key, {"b"}, t20, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());

  HashMetadata metadata = hashMetadata(key.ToString());
  ASSERT_EQ(metadata.size, 2);
  ASSERT_EQ(metadata.persist, 0);
  ASSERT_EQ(metadata.lower, t10);
  ASSERT_EQ(metadata.upper, t20);

  s = hash_->PersistFields(*ctx_, key, {"a"}, &results);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(results, std::vector<int64_t>({1}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 1);
  EXPECT_EQ(metadata.lower, t10);
  EXPECT_EQ(metadata.upper, t20);

  s = hash_->PersistFields(*ctx_, key, {"b"}, &results);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(results, std::vector<int64_t>({1}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 2);
  EXPECT_EQ(metadata.lower, 0);
  EXPECT_EQ(metadata.upper, 0);

  s = hash_->PersistFields(*ctx_, key, {"a"}, &results);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(results, std::vector<int64_t>({-1}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 2);
  EXPECT_EQ(metadata.lower, 0);
  EXPECT_EQ(metadata.upper, 0);
}

TEST_F(RedisHashFieldExpirationEncodingTest, ExpiredTTLPhysicalIsMissingForReadsAndDoesNotMutateMetadata) {
  const Slice key = "hfe-expired-ttl-read";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"a", "1"}, {"b", "2"}, {"c", "3"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 3);

  HashMetadata metadata = hashMetadata(key.ToString());
  metadata.persist = 2;
  metadata.lower = util::GetTimeStampMS() - 1000;
  metadata.upper = metadata.lower;
  s = putHashMetadata(key.ToString(), metadata);
  ASSERT_TRUE(s.ok());
  s = putRawHashValue(key.ToString(), "a", metadata.lower, "1");
  ASSERT_TRUE(s.ok());

  HashMetadata before = hashMetadata(key.ToString());
  std::string got;
  s = hash_->Get(*ctx_, key, "a", &got);
  EXPECT_TRUE(s.IsNotFound());

  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  s = hash_->MGet(*ctx_, key, {"a", "b"}, &values, &statuses);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(statuses[0].IsNotFound());
  EXPECT_EQ(values[1], "2");

  std::vector<FieldValue> field_values;
  s = hash_->GetAll(*ctx_, key, &field_values);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(field_values.size(), 2);

  RangeLexSpec spec;
  spec.min = "a";
  spec.max = "z";
  spec.count = INT_MAX;
  s = hash_->RangeByLex(*ctx_, key, spec, &field_values);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(field_values.size(), 2);

  std::vector<std::string> fields;
  s = hash_->Scan(*ctx_, key, "", 10, "", &fields, &values);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(fields.size(), 2);

  HashMetadata after = hashMetadata(key.ToString());
  EXPECT_EQ(after.size, before.size);
  EXPECT_EQ(after.persist, before.persist);
  EXPECT_EQ(after.lower, before.lower);
  EXPECT_EQ(after.upper, before.upper);
}

TEST_F(RedisHashFieldExpirationEncodingTest, DuplicateFieldsUseCommandLocalState) {
  const Slice key = "hfe-duplicate-fields";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"a", "1"}, {"b", "2"}, {"c", "3"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 3);

  std::vector<int64_t> results;
  uint64_t future = util::GetTimeStampMS() + 60'000;
  s = hash_->ExpireFields(*ctx_, key, {"a", "a"}, future, HashFieldExpireCondition::kNX, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(results, std::vector<int64_t>({1, 0}));
  HashMetadata metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 3);
  EXPECT_EQ(metadata.persist, 2);

  s = hash_->PersistFields(*ctx_, key, {"a", "a"}, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(results, std::vector<int64_t>({1, -1}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 3);
  EXPECT_EQ(metadata.persist, 3);
  EXPECT_EQ(metadata.lower, 0);
  EXPECT_EQ(metadata.upper, 0);

  s = hash_->ExpireFields(*ctx_, key, {"b", "b"}, util::GetTimeStampMS(), HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(results, std::vector<int64_t>({2, -2}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 2);
}

TEST_F(RedisHashFieldExpirationEncodingTest, CompactionGhostDoesNotDecrementMetadataOnMissingSubkey) {
  const Slice key = "hfe-compaction-ghost";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"field1", "1"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 1);

  std::vector<int64_t> results;
  uint64_t future = util::GetTimeStampMS() + 60'000;
  s = hash_->ExpireFields(*ctx_, key, {"field1"}, future, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(results, std::vector<int64_t>({1}));

  HashMetadata before = hashMetadata(key.ToString());
  ASSERT_EQ(before.size, 1);
  ASSERT_EQ(before.persist, 0);
  s = deleteRawHashValue(key.ToString(), "field1");
  ASSERT_TRUE(s.ok()) << s.ToString();

  s = hash_->PersistFields(*ctx_, key, {"field1"}, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(results, std::vector<int64_t>({-2}));
  HashMetadata metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, before.size);
  EXPECT_EQ(metadata.persist, before.persist);
  EXPECT_EQ(metadata.lower, before.lower);
  EXPECT_EQ(metadata.upper, before.upper);

  s = hash_->ExpireFields(*ctx_, key, {"field1"}, future + 10'000, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(results, std::vector<int64_t>({-2}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, before.size);
  EXPECT_EQ(metadata.persist, before.persist);
  EXPECT_EQ(metadata.lower, before.lower);
  EXPECT_EQ(metadata.upper, before.upper);

  s = hash_->Set(*ctx_, key, "field1", "new", &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(ret, 1);
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 1);
  EXPECT_EQ(metadata.lower, before.lower);
  EXPECT_EQ(metadata.upper, before.upper);
}

TEST_F(RedisHashFieldExpirationEncodingTest, GetFieldsExpireTimeReturnsMissingForMissingKey) {
  std::vector<int64_t> results;
  auto s = hash_->GetFieldsExpireTime(*ctx_, "hfe-expire-info-missing-key", {"a", "b"}, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(results, (std::vector<int64_t>{-2, -2}));
}

TEST_F(RedisHashFieldExpirationEncodingTest, GetFieldsExpireTimeCoversPersistentLiveExpiredMissingAndDuplicates) {
  const Slice key = "hfe-expire-info-states";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"persist", "1"}, {"live", "2"}, {"expired", "3"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 3);

  std::vector<int64_t> expire_results;
  uint64_t now = util::GetTimeStampMS();
  uint64_t live_expire = now + 60'000;
  uint64_t expired_rewrite_expire = now + 120'000;
  s = hash_->ExpireFields(*ctx_, key, {"live"}, live_expire, HashFieldExpireCondition::kNone, &expire_results, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = hash_->ExpireFields(*ctx_, key, {"expired"}, expired_rewrite_expire, HashFieldExpireCondition::kNone,
                          &expire_results, now);
  ASSERT_TRUE(s.ok()) << s.ToString();

  HashMetadata metadata = hashMetadata(key.ToString());
  ASSERT_EQ(metadata.size, 3);
  ASSERT_EQ(metadata.persist, 1);
  uint64_t expired_at = now - 1;
  s = putRawHashValue(key.ToString(), "expired", expired_at, "3");
  ASSERT_TRUE(s.ok()) << s.ToString();
  metadata.lower = expired_at;
  s = putHashMetadata(key.ToString(), metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();
  HashMetadata before = hashMetadata(key.ToString());

  std::vector<int64_t> results;
  s = hash_->GetFieldsExpireTime(*ctx_, key, {"persist", "live", "expired", "missing", "live"}, &results, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(results,
            (std::vector<int64_t>{-1, static_cast<int64_t>(live_expire), -2, -2, static_cast<int64_t>(live_expire)}));

  HashMetadata after = hashMetadata(key.ToString());
  EXPECT_EQ(after.size, before.size);
  EXPECT_EQ(after.persist, before.persist);
  EXPECT_EQ(after.lower, before.lower);
  EXPECT_EQ(after.upper, before.upper);
}

TEST_F(RedisHashFieldExpirationEncodingTest, GetFieldsExpireTimeReturnsAbsoluteMilliseconds) {
  const Slice key = "hfe-expire-info-format";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"field", "1"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 1);

  uint64_t expire_at = util::GetTimeStampMS() + 60'123;
  std::vector<int64_t> expire_results;
  s = hash_->ExpireFields(*ctx_, key, {"field"}, expire_at, HashFieldExpireCondition::kNone, &expire_results);
  ASSERT_TRUE(s.ok()) << s.ToString();

  std::vector<int64_t> results;
  s = hash_->GetFieldsExpireTime(*ctx_, key, {"field"}, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(results, (std::vector<int64_t>{static_cast<int64_t>(expire_at)}));
}

TEST_F(RedisHashFieldExpirationEncodingTest, GetFieldsExpireTimeDoesNotRepairCompactionGhost) {
  const Slice key = "hfe-expire-info-ghost";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"ghost", "1"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 1);

  std::vector<int64_t> expire_results;
  uint64_t future = util::GetTimeStampMS() + 60'000;
  s = hash_->ExpireFields(*ctx_, key, {"ghost"}, future, HashFieldExpireCondition::kNone, &expire_results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(expire_results, std::vector<int64_t>({1}));

  HashMetadata before = hashMetadata(key.ToString());
  ASSERT_EQ(before.size, 1);
  ASSERT_EQ(before.persist, 0);
  s = deleteRawHashValue(key.ToString(), "ghost");
  ASSERT_TRUE(s.ok()) << s.ToString();

  std::vector<int64_t> results;
  s = hash_->GetFieldsExpireTime(*ctx_, key, {"ghost"}, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(results, (std::vector<int64_t>{-2}));

  HashMetadata after = hashMetadata(key.ToString());
  EXPECT_EQ(after.size, before.size);
  EXPECT_EQ(after.persist, before.persist);
  EXPECT_EQ(after.lower, before.lower);
  EXPECT_EQ(after.upper, before.upper);
}

TEST_F(RedisHashFieldExpirationEncodingTest, SizeRepairsExpiredPhysicalAndGhostMetadata) {
  const Slice key = "hfe-size-repair";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"persistent", "1"}, {"live", "2"}, {"expired", "3"}, {"ghost", "4"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 4);

  std::vector<int64_t> results;
  uint64_t now = util::GetTimeStampMS();
  uint64_t live_expire = now + 60'000;
  uint64_t expired_at = now - 1;
  s = hash_->ExpireFields(*ctx_, key, {"live"}, live_expire, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = hash_->ExpireFields(*ctx_, key, {"ghost"}, live_expire + 60'000, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();

  HashMetadata metadata = hashMetadata(key.ToString());
  s = putRawHashValue(key.ToString(), "expired", expired_at, "3");
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = deleteRawHashValue(key.ToString(), "ghost");
  ASSERT_TRUE(s.ok()) << s.ToString();

  metadata.size = 4;
  metadata.persist = 1;
  metadata.lower = expired_at;
  metadata.upper = live_expire + 60'000;
  s = putHashMetadata(key.ToString(), metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();

  s = hash_->Size(*ctx_, key, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(ret, 2);

  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 1);
  EXPECT_EQ(metadata.lower, live_expire);
  EXPECT_EQ(metadata.upper, live_expire);

  std::string value;
  s = hash_->Get(*ctx_, key, "expired", &value);
  EXPECT_TRUE(s.IsNotFound());
  s = hash_->Get(*ctx_, key, "ghost", &value);
  EXPECT_TRUE(s.IsNotFound());
  s = hash_->Get(*ctx_, key, "persistent", &value);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(value, "1");
  s = hash_->Get(*ctx_, key, "live", &value);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(value, "2");
}

TEST_F(RedisHashFieldExpirationEncodingTest, SizeDeletesHashWhenAllTtlCandidatesExpired) {
  const Slice key = "hfe-size-delete-all-expired";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"a", "1"}, {"b", "2"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 2);

  uint64_t now = util::GetTimeStampMS();
  uint64_t lower = now - 2'000;
  uint64_t upper = now - 1'000;
  s = putRawHashValue(key.ToString(), "a", lower, "1");
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = putRawHashValue(key.ToString(), "b", upper, "2");
  ASSERT_TRUE(s.ok()) << s.ToString();

  HashMetadata metadata = hashMetadata(key.ToString());
  metadata.size = 2;
  metadata.persist = 0;
  metadata.lower = lower;
  metadata.upper = upper;
  s = putHashMetadata(key.ToString(), metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();

  s = hash_->Size(*ctx_, key, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(ret, 0);

  metadata = HashMetadata(false);
  s = getHashMetadata(key.ToString(), &metadata);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(RedisHashFieldExpirationEncodingTest, DeleteHandlesPersistentLiveExpiredMissingAndDuplicateFields) {
  const Slice key = "hfe-delete-state-matrix";
  uint64_t ret = 0;
  auto s =
      hash_->MSet(*ctx_, key, {{"persistent", "1"}, {"live", "2"}, {"expired", "3"}, {"keeper", "4"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 4);

  std::vector<int64_t> results;
  uint64_t now = util::GetTimeStampMS();
  s = hash_->ExpireFields(*ctx_, key, {"live"}, now + 60'000, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = hash_->ExpireFields(*ctx_, key, {"expired"}, now, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(results, std::vector<int64_t>({2}));

  HashMetadata metadata = hashMetadata(key.ToString());
  ASSERT_EQ(metadata.size, 3);
  ASSERT_EQ(metadata.persist, 2);

  s = putRawHashValue(key.ToString(), "expired", now - 1, "3");
  ASSERT_TRUE(s.ok()) << s.ToString();
  metadata.size = 4;
  metadata.persist = 2;
  metadata.lower = now - 1;
  metadata.upper = now + 60'000;
  s = putHashMetadata(key.ToString(), metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();

  s = hash_->Delete(*ctx_, key, {"persistent", "live", "expired", "missing", "persistent"}, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(ret, 2);
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 1);
  EXPECT_EQ(metadata.persist, 1);
  EXPECT_EQ(metadata.lower, 0);
  EXPECT_EQ(metadata.upper, 0);

  std::vector<FieldValue> fields;
  s = hash_->GetAll(*ctx_, key, &fields);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(fields.size(), 1);
  EXPECT_EQ(fields[0].field, "keeper");
  EXPECT_EQ(fields[0].value, "4");
}

TEST_F(RedisHashFieldExpirationEncodingTest, MSetHandlesPersistentLiveExpiredAndGhostFields) {
  const Slice key = "hfe-mset-state-matrix";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"persistent", "1"}, {"live", "2"}, {"expired", "3"}, {"ghost", "4"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 4);

  std::vector<int64_t> results;
  uint64_t now = util::GetTimeStampMS();
  s = hash_->ExpireFields(*ctx_, key, {"live"}, now + 60'000, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = hash_->ExpireFields(*ctx_, key, {"expired"}, now, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = hash_->ExpireFields(*ctx_, key, {"ghost"}, now + 120'000, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();

  HashMetadata before = hashMetadata(key.ToString());
  ASSERT_EQ(before.size, 3);
  ASSERT_EQ(before.persist, 1);
  s = putRawHashValue(key.ToString(), "expired", now - 1, "3");
  ASSERT_TRUE(s.ok()) << s.ToString();
  before.size = 4;
  before.lower = now - 1;
  before.upper = now + 120'000;
  s = putHashMetadata(key.ToString(), before);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = deleteRawHashValue(key.ToString(), "ghost");
  ASSERT_TRUE(s.ok()) << s.ToString();

  s = hash_->MSet(*ctx_, key,
                  {{"persistent", "11"}, {"live", "22"}, {"expired", "33"}, {"ghost", "44"}, {"missing", "55"}}, false,
                  &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(ret, 3);
  HashMetadata metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 6);
  EXPECT_EQ(metadata.persist, 5);
  EXPECT_EQ(metadata.lower, before.lower);
  EXPECT_EQ(metadata.upper, before.upper);

  std::string value;
  s = hash_->Get(*ctx_, key, "persistent", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "11");
  s = hash_->Get(*ctx_, key, "live", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "22");
  s = hash_->Get(*ctx_, key, "expired", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "33");
  s = hash_->Get(*ctx_, key, "ghost", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "44");
  s = hash_->Get(*ctx_, key, "missing", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "55");

  s = hash_->PersistFields(*ctx_, key, {"ghost"}, &results);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(results, std::vector<int64_t>({-1}));
  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 6);
  EXPECT_EQ(metadata.persist, 5);
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetNXHandlesPersistentLiveExpiredAndGhostFields) {
  const Slice key = "hfe-msetnx-state-matrix";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"persistent", "1"}, {"live", "2"}, {"expired", "3"}, {"ghost", "4"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 4);

  std::vector<int64_t> results;
  uint64_t now = util::GetTimeStampMS();
  s = hash_->ExpireFields(*ctx_, key, {"live"}, now + 60'000, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());
  s = hash_->ExpireFields(*ctx_, key, {"expired"}, now, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());
  s = hash_->ExpireFields(*ctx_, key, {"ghost"}, now + 120'000, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());

  HashMetadata before = hashMetadata(key.ToString());
  s = putRawHashValue(key.ToString(), "expired", now - 1, "3");
  ASSERT_TRUE(s.ok()) << s.ToString();
  before.size = 4;
  before.lower = now - 1;
  before.upper = now + 120'000;
  s = putHashMetadata(key.ToString(), before);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = deleteRawHashValue(key.ToString(), "ghost");
  ASSERT_TRUE(s.ok()) << s.ToString();

  s = hash_->MSet(*ctx_, key,
                  {{"persistent", "11"}, {"live", "22"}, {"expired", "33"}, {"ghost", "44"}, {"missing", "55"}}, true,
                  &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(ret, 3);
  HashMetadata metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 6);
  EXPECT_EQ(metadata.persist, 4);
  EXPECT_EQ(metadata.lower, before.lower);
  EXPECT_EQ(metadata.upper, before.upper);

  std::string value;
  s = hash_->Get(*ctx_, key, "persistent", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "1");
  s = hash_->Get(*ctx_, key, "live", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "2");
  s = hash_->Get(*ctx_, key, "expired", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "33");
  s = hash_->Get(*ctx_, key, "ghost", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "44");
  s = hash_->Get(*ctx_, key, "missing", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "55");
}

TEST_F(RedisHashFieldExpirationEncodingTest, IncrementsKeepLiveTTLAndTreatExpiredPhysicalAndGhostAsZero) {
  const Slice key = "hfe-incr-state-matrix";
  uint64_t ret = 0;
  auto s =
      hash_->MSet(*ctx_, key, {{"persistent", "10"}, {"live", "20"}, {"expired", "30"}, {"ghost", "40"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 4);

  std::vector<int64_t> results;
  uint64_t now = util::GetTimeStampMS();
  s = hash_->ExpireFields(*ctx_, key, {"live"}, now + 60'000, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());
  s = hash_->ExpireFields(*ctx_, key, {"expired"}, now, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());
  s = hash_->ExpireFields(*ctx_, key, {"ghost"}, now + 120'000, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok());

  HashMetadata metadata = hashMetadata(key.ToString());
  s = putRawHashValue(key.ToString(), "expired", now - 1, "30");
  ASSERT_TRUE(s.ok()) << s.ToString();
  metadata.size = 4;
  metadata.lower = now - 1;
  metadata.upper = now + 120'000;
  s = putHashMetadata(key.ToString(), metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = deleteRawHashValue(key.ToString(), "ghost");
  ASSERT_TRUE(s.ok()) << s.ToString();

  int64_t int_value = 0;
  s = hash_->IncrBy(*ctx_, key, "persistent", 1, &int_value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(int_value, 11);
  s = hash_->IncrBy(*ctx_, key, "live", 1, &int_value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(int_value, 21);
  metadata = hashMetadata(key.ToString());
  std::string raw_value = rawHashValue(key.ToString(), "live", &metadata);
  Slice decoded_value(raw_value);
  uint64_t live_expire = 0;
  ASSERT_TRUE(metadata.DecodeSubkeyValue(&decoded_value, &live_expire).ok());
  EXPECT_EQ(decoded_value.ToStringView(), "21");
  EXPECT_EQ(live_expire, now + 60'000);
  s = hash_->IncrBy(*ctx_, key, "expired", 1, &int_value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(int_value, 1);
  s = hash_->IncrBy(*ctx_, key, "ghost", 1, &int_value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(int_value, 1);
  s = hash_->IncrBy(*ctx_, key, "missing", 1, &int_value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(int_value, 1);

  metadata = hashMetadata(key.ToString());
  EXPECT_EQ(metadata.size, 6);
  EXPECT_EQ(metadata.persist, 4);
  EXPECT_EQ(metadata.lower, now - 1);
  EXPECT_EQ(metadata.upper, now + 120'000);

  double float_value = 0;
  s = hash_->IncrByFloat(*ctx_, key, "live", 0.5, &float_value);
  ASSERT_TRUE(s.ok());
  EXPECT_DOUBLE_EQ(float_value, 21.5);
  metadata = hashMetadata(key.ToString());
  raw_value = rawHashValue(key.ToString(), "live", &metadata);
  decoded_value = Slice(raw_value);
  live_expire = 0;
  ASSERT_TRUE(metadata.DecodeSubkeyValue(&decoded_value, &live_expire).ok());
  EXPECT_EQ(decoded_value.ToStringView(), "21.5");
  EXPECT_EQ(live_expire, now + 60'000);
  EXPECT_EQ(metadata.size, 6);
  EXPECT_EQ(metadata.persist, 4);
  EXPECT_EQ(metadata.lower, now - 1);
  EXPECT_EQ(metadata.upper, now + 120'000);
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

TEST_F(RedisHashTest, HIncrByFloatStoredFormat) {
  double value = 0.0;
  Slice field("hash-incrbyfloat-format-field");

  // Stored value should use compact format without trailing zeros
  auto s = hash_->IncrByFloat(*ctx_, key_, field, 10.5, &value);
  EXPECT_TRUE(s.ok());
  EXPECT_DOUBLE_EQ(10.5, value);
  std::string bytes;
  s = hash_->Get(*ctx_, key_, field, &bytes);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ("10.5", bytes);

  // Subsequent IncrByFloat should parse the stored compact format correctly
  s = hash_->IncrByFloat(*ctx_, key_, field, 1.5, &value);
  EXPECT_TRUE(s.ok());
  EXPECT_DOUBLE_EQ(12.0, value);
  s = hash_->Get(*ctx_, key_, field, &bytes);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ("12", bytes);

  s = hash_->Del(*ctx_, key_);
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
