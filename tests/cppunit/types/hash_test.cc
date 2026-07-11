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
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <utility>
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

  rocksdb::Status getRawHashValue(const std::string &key, const std::string &field, std::string *raw_value) {
    HashMetadata metadata = hashMetadata(key);
    return storage_->Get(*ctx_, ctx_->GetReadOptions(), hashSubKey(key, field, metadata), raw_value);
  }

  std::pair<std::string, uint64_t> decodedHashValue(const std::string &key, const std::string &field) {
    HashMetadata metadata(false);
    std::string raw_value = rawHashValue(key, field, &metadata);
    Slice decoded_value(raw_value);
    uint64_t expire = 0;
    auto s = metadata.DecodeSubkeyValue(&decoded_value, &expire);
    assert(s.ok());
    return {decoded_value.ToString(), expire};
  }

  void expectHashMetadata(const std::string &key, uint64_t size, uint64_t persist, uint64_t lower, uint64_t upper) {
    HashMetadata metadata = hashMetadata(key);
    EXPECT_EQ(metadata.mode, HashSubkeyEncodingMode::kFieldExpiration);
    EXPECT_EQ(metadata.size, size);
    EXPECT_EQ(metadata.persist, persist);
    EXPECT_EQ(metadata.lower, lower);
    EXPECT_EQ(metadata.upper, upper);
    EXPECT_LE(metadata.persist, metadata.size);
    if (metadata.size == metadata.persist) {
      EXPECT_EQ(metadata.lower, 0);
      EXPECT_EQ(metadata.upper, 0);
    } else {
      EXPECT_GT(metadata.lower, 0);
      EXPECT_GE(metadata.upper, metadata.lower);
    }
  }

  void createFourStateHash(const std::string &key, uint64_t expired_at, uint64_t live_expire) {
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"P", "p"}, {"L", "l"}, {"X", "x"}, {"K", "k"}}, false, &ret);
    assert(s.ok());
    assert(ret == 4);
    s = putRawHashValue(key, "L", live_expire, "l");
    assert(s.ok());
    s = putRawHashValue(key, "X", expired_at, "x");
    assert(s.ok());

    HashMetadata metadata = hashMetadata(key);
    metadata.persist = 2;
    metadata.lower = expired_at;
    metadata.upper = live_expire;
    s = putHashMetadata(key, metadata);
    assert(s.ok());
  }

  void createKeeperAndGhost(const std::string &key, uint64_t ghost_expire) {
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"K", "k"}, {"G", "g"}}, false, &ret);
    assert(s.ok());
    assert(ret == 2);
    s = putRawHashValue(key, "G", ghost_expire, "g");
    assert(s.ok());

    HashMetadata metadata = hashMetadata(key);
    metadata.persist = 1;
    metadata.lower = ghost_expire;
    metadata.upper = ghost_expire;
    s = putHashMetadata(key, metadata);
    assert(s.ok());
    s = deleteRawHashValue(key, "G");
    assert(s.ok());
  }

  static void expectGetResults(const std::vector<std::string> &values, const std::vector<rocksdb::Status> &statuses,
                               const std::vector<std::optional<std::string>> &expected) {
    ASSERT_EQ(values.size(), expected.size());
    ASSERT_EQ(statuses.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      if (expected[i]) {
        EXPECT_TRUE(statuses[i].ok()) << statuses[i].ToString();
        EXPECT_EQ(values[i], *expected[i]);
      } else {
        EXPECT_TRUE(statuses[i].IsNotFound()) << statuses[i].ToString();
      }
    }
  }

  static HashSetExOptions setExOptions(HashSetExOptions::TTLAction action, uint64_t expire_at = 0,
                                       HashFieldSetCondition condition = HashFieldSetCondition::kNone) {
    HashSetExOptions options;
    options.ttl_action = action;
    options.expire_at_ms = expire_at;
    options.condition = condition;
    return options;
  }

  static HashGetExOptions getExOptions(HashGetExOptions::TTLAction action, uint64_t expire_at = 0) {
    HashGetExOptions options;
    options.ttl_action = action;
    options.expire_at_ms = expire_at;
    return options;
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

TEST_F(RedisHashFieldExpirationEncodingTest, SetFieldsWithExpireCoversAllPhysicalStatesAndTTLChanges) {
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t expired_at = now - 1'000;
  const uint64_t live_expire = now + 60'000;
  const uint64_t new_expire = now + 120'000;
  const std::vector<FieldValue> updates = {{"P", "new-p"}, {"L", "new-l"}, {"X", "new-x"}, {"M", "new-m"}};

  {
    const std::string key = "hsetex-state-future";
    createFourStateHash(key, expired_at, live_expire);
    HashMetadata before = hashMetadata(key);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kSet, new_expire);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, updates, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 5, 1, expired_at, new_expire);
    HashMetadata after = hashMetadata(key);
    EXPECT_EQ(after.flags, before.flags);
    EXPECT_EQ(after.version, before.version);
    EXPECT_EQ(after.expire, before.expire);
    for (const auto &field : {"P", "L", "X", "M"}) {
      EXPECT_EQ(decodedHashValue(key, field).second, new_expire);
    }
    EXPECT_EQ(decodedHashValue(key, "P").first, "new-p");
    EXPECT_EQ(decodedHashValue(key, "L").first, "new-l");
    EXPECT_EQ(decodedHashValue(key, "X").first, "new-x");
    EXPECT_EQ(decodedHashValue(key, "M").first, "new-m");
    EXPECT_EQ(decodedHashValue(key, "K"), (std::pair<std::string, uint64_t>{"k", 0}));
  }

  {
    const std::string key = "hsetex-state-discard";
    createFourStateHash(key, expired_at, live_expire);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, updates, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 5, 5, 0, 0);
    for (const auto &field : {"P", "L", "X", "M", "K"}) {
      EXPECT_EQ(decodedHashValue(key, field).second, 0);
    }
  }

  {
    const std::string key = "hsetex-state-keep";
    createFourStateHash(key, expired_at, live_expire);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kKeep);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, updates, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 5, 3, expired_at, live_expire);
    EXPECT_EQ(decodedHashValue(key, "P"), (std::pair<std::string, uint64_t>{"new-p", 0}));
    EXPECT_EQ(decodedHashValue(key, "L"), (std::pair<std::string, uint64_t>{"new-l", live_expire}));
    EXPECT_EQ(decodedHashValue(key, "X"), (std::pair<std::string, uint64_t>{"new-x", expired_at}));
    EXPECT_EQ(decodedHashValue(key, "M"), (std::pair<std::string, uint64_t>{"new-m", 0}));
    std::string value;
    EXPECT_TRUE(hash_->Get(*ctx_, key, "X", &value).IsNotFound());
  }

  {
    const std::string key = "hsetex-state-immediate";
    createFourStateHash(key, expired_at, live_expire);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kSet, now);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, updates, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 1, 1, 0, 0);
    std::string raw_value;
    for (const auto &field : {"P", "L", "X", "M"}) {
      EXPECT_TRUE(getRawHashValue(key, field, &raw_value).IsNotFound());
    }
    EXPECT_EQ(decodedHashValue(key, "K"), (std::pair<std::string, uint64_t>{"k", 0}));
  }
}

TEST_F(RedisHashFieldExpirationEncodingTest, GetFieldsWithExpireCoversAllPhysicalStatesAndTTLChanges) {
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t expired_at = now - 1'000;
  const uint64_t live_expire = now + 60'000;
  const uint64_t new_expire = now + 120'000;
  const std::vector<Slice> fields = {"P", "L", "X", "M"};
  const std::vector<std::optional<std::string>> expected = {"p", "l", std::nullopt, std::nullopt};

  {
    const std::string key = "hgetex-state-none";
    createFourStateHash(key, expired_at, live_expire);
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    auto options = getExOptions(HashGetExOptions::TTLAction::kNone);
    auto s = hash_->GetFieldsWithExpire(*ctx_, key, fields, options, &values, &statuses, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    expectGetResults(values, statuses, expected);
    expectHashMetadata(key, 3, 2, expired_at, live_expire);
    EXPECT_EQ(decodedHashValue(key, "P"), (std::pair<std::string, uint64_t>{"p", 0}));
    EXPECT_EQ(decodedHashValue(key, "L"), (std::pair<std::string, uint64_t>{"l", live_expire}));
  }

  {
    const std::string key = "hgetex-state-future";
    createFourStateHash(key, expired_at, live_expire);
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    auto options = getExOptions(HashGetExOptions::TTLAction::kSet, new_expire);
    auto s = hash_->GetFieldsWithExpire(*ctx_, key, fields, options, &values, &statuses, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    expectGetResults(values, statuses, expected);
    expectHashMetadata(key, 3, 1, expired_at, new_expire);
    EXPECT_EQ(decodedHashValue(key, "P").second, new_expire);
    EXPECT_EQ(decodedHashValue(key, "L").second, new_expire);
  }

  {
    const std::string key = "hgetex-state-persist";
    createFourStateHash(key, expired_at, live_expire);
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    auto options = getExOptions(HashGetExOptions::TTLAction::kPersist);
    auto s = hash_->GetFieldsWithExpire(*ctx_, key, fields, options, &values, &statuses, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    expectGetResults(values, statuses, expected);
    expectHashMetadata(key, 3, 3, 0, 0);
    EXPECT_EQ(decodedHashValue(key, "P").second, 0);
    EXPECT_EQ(decodedHashValue(key, "L").second, 0);
  }

  {
    const std::string key = "hgetex-state-immediate";
    createFourStateHash(key, expired_at, live_expire);
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    auto options = getExOptions(HashGetExOptions::TTLAction::kSet, now);
    auto s = hash_->GetFieldsWithExpire(*ctx_, key, fields, options, &values, &statuses, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    expectGetResults(values, statuses, expected);
    expectHashMetadata(key, 1, 1, 0, 0);
    std::string raw_value;
    for (const auto &field : {"P", "L", "X"}) {
      EXPECT_TRUE(getRawHashValue(key, field, &raw_value).IsNotFound());
    }
  }
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetAndGetFieldsWithExpireHandleMissingAndDeadlineBoundaries) {
  const uint64_t now = util::GetTimeStampMS();
  bool applied = false;
  auto immediate_set = setExOptions(HashSetExOptions::TTLAction::kSet, now);
  auto s =
      hash_->SetFieldsWithExpire(*ctx_, "hsetex-missing-immediate", {{"M", "value"}}, immediate_set, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_TRUE(applied);
  HashMetadata metadata(false);
  EXPECT_TRUE(getHashMetadata("hsetex-missing-immediate", &metadata).IsNotFound());

  applied = false;
  auto fnx_immediate = setExOptions(HashSetExOptions::TTLAction::kSet, now, HashFieldSetCondition::kFNX);
  s = hash_->SetFieldsWithExpire(*ctx_, "hsetex-missing-fnx-immediate", {{"M", "value"}}, fnx_immediate, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_TRUE(applied);
  EXPECT_TRUE(getHashMetadata("hsetex-missing-fnx-immediate", &metadata).IsNotFound());

  const std::string key = "hgetex-equal-now";
  uint64_t ret = 0;
  s = hash_->MSet(*ctx_, key, {{"f", "value"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 1);
  s = putRawHashValue(key, "f", now, "value");
  ASSERT_TRUE(s.ok()) << s.ToString();
  metadata = hashMetadata(key);
  metadata.persist = 0;
  metadata.lower = now;
  metadata.upper = now;
  s = putHashMetadata(key, metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();

  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  auto immediate_get = getExOptions(HashGetExOptions::TTLAction::kSet, now);
  s = hash_->GetFieldsWithExpire(*ctx_, key, {"f"}, immediate_get, &values, &statuses, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expectGetResults(values, statuses, {"value"});
  EXPECT_TRUE(getHashMetadata(key, &metadata).IsNotFound());

  const std::string expired_key = "hgetex-only-expired";
  s = hash_->MSet(*ctx_, expired_key, {{"X", "x"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = putRawHashValue(expired_key, "X", now - 1, "x");
  ASSERT_TRUE(s.ok()) << s.ToString();
  metadata = hashMetadata(expired_key);
  metadata.persist = 0;
  metadata.lower = now - 1;
  metadata.upper = now - 1;
  s = putHashMetadata(expired_key, metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();
  values.clear();
  statuses.clear();
  auto no_change = getExOptions(HashGetExOptions::TTLAction::kNone);
  s = hash_->GetFieldsWithExpire(*ctx_, expired_key, {"X"}, no_change, &values, &statuses, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expectGetResults(values, statuses, {std::nullopt});
  EXPECT_TRUE(getHashMetadata(expired_key, &metadata).IsNotFound());
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetFieldsWithExpireConditionsAreAtomicAndCleanupInRequestOrder) {
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t expired_at = now - 1'000;
  const uint64_t live_expire = now + 60'000;

  {
    const std::string key = "hsetex-fxx-success";
    createFourStateHash(key, expired_at, live_expire);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard, 0, HashFieldSetCondition::kFXX);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"P", "new-p"}, {"L", "new-l"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 4, 3, expired_at, live_expire);
    EXPECT_EQ(decodedHashValue(key, "P"), (std::pair<std::string, uint64_t>{"new-p", 0}));
    EXPECT_EQ(decodedHashValue(key, "L"), (std::pair<std::string, uint64_t>{"new-l", 0}));
  }

  {
    const std::string key = "hsetex-fxx-missing-failure";
    createFourStateHash(key, expired_at, live_expire);
    HashMetadata before = hashMetadata(key);
    bool applied = true;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard, 0, HashFieldSetCondition::kFXX);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"P", "new-p"}, {"M", "new-m"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_FALSE(applied);
    EXPECT_EQ(hashMetadata(key), before);
    EXPECT_EQ(decodedHashValue(key, "P").first, "p");
    std::string raw_value;
    EXPECT_TRUE(getRawHashValue(key, "M", &raw_value).IsNotFound());
  }

  {
    const std::string key = "hsetex-fnx-expired-success";
    createFourStateHash(key, expired_at, live_expire);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard, 0, HashFieldSetCondition::kFNX);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"X", "new-x"}, {"M", "new-m"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 5, 4, expired_at, live_expire);
    EXPECT_EQ(decodedHashValue(key, "X"), (std::pair<std::string, uint64_t>{"new-x", 0}));
    EXPECT_EQ(decodedHashValue(key, "M"), (std::pair<std::string, uint64_t>{"new-m", 0}));
  }

  {
    const std::string key = "hsetex-fxx-expired-failure";
    createFourStateHash(key, expired_at, live_expire);
    bool applied = true;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard, 0, HashFieldSetCondition::kFXX);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"P", "new-p"}, {"X", "new-x"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_FALSE(applied);
    expectHashMetadata(key, 3, 2, expired_at, live_expire);
    EXPECT_EQ(decodedHashValue(key, "P").first, "p");
    std::string raw_value;
    EXPECT_TRUE(getRawHashValue(key, "X", &raw_value).IsNotFound());
  }

  for (const auto &[name, condition, fields] :
       std::vector<std::tuple<std::string, HashFieldSetCondition, std::vector<FieldValue>>>{
           {"fxx", HashFieldSetCondition::kFXX, {{"M", "new-m"}, {"X", "new-x"}}},
           {"fnx", HashFieldSetCondition::kFNX, {{"P", "new-p"}, {"X", "new-x"}}},
       }) {
    const std::string key = "hsetex-ordered-unvisited-x-" + name;
    createFourStateHash(key, expired_at, live_expire);
    HashMetadata before = hashMetadata(key);
    bool applied = true;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard, 0, condition);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, fields, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_FALSE(applied);
    EXPECT_EQ(hashMetadata(key), before);
    EXPECT_EQ(decodedHashValue(key, "X"), (std::pair<std::string, uint64_t>{"x", expired_at}));
  }

  {
    const std::string key = "hsetex-ordered-cleaned-x-before-failure";
    createFourStateHash(key, expired_at, live_expire);
    bool applied = true;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard, 0, HashFieldSetCondition::kFNX);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"X", "new-x"}, {"P", "new-p"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_FALSE(applied);
    expectHashMetadata(key, 3, 2, expired_at, live_expire);
    EXPECT_EQ(decodedHashValue(key, "P").first, "p");
    std::string raw_value;
    EXPECT_TRUE(getRawHashValue(key, "X", &raw_value).IsNotFound());
  }

  {
    const std::string key = "hsetex-missing-fxx";
    bool applied = true;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard, 0, HashFieldSetCondition::kFXX);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"M", "value"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_FALSE(applied);
    HashMetadata metadata(false);
    EXPECT_TRUE(getHashMetadata(key, &metadata).IsNotFound());
  }
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetFieldsWithExpireConditionChangesExpiredKeepTTLTransition) {
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t expired_at = now - 1'000;
  const uint64_t future = now + 60'000;

  auto create_keeper_and_expired = [&](const std::string &key) {
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"K", "k"}, {"X", "x"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(ret, 2);
    s = putRawHashValue(key, "X", expired_at, "x");
    ASSERT_TRUE(s.ok()) << s.ToString();
    HashMetadata metadata = hashMetadata(key);
    metadata.persist = 1;
    metadata.lower = expired_at;
    metadata.upper = expired_at;
    s = putHashMetadata(key, metadata);
    ASSERT_TRUE(s.ok()) << s.ToString();
  };

  {
    const std::string key = "hsetex-x-keep-without-condition";
    create_keeper_and_expired(key);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kKeep);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"X", "unconditional"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 2, 1, expired_at, expired_at);
    EXPECT_EQ(decodedHashValue(key, "X"), (std::pair<std::string, uint64_t>{"unconditional", expired_at}));
  }

  {
    const std::string key = "hsetex-x-keep-with-fnx";
    create_keeper_and_expired(key);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kKeep, 0, HashFieldSetCondition::kFNX);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"X", "conditional"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 2, 2, 0, 0);
    EXPECT_EQ(decodedHashValue(key, "X"), (std::pair<std::string, uint64_t>{"conditional", 0}));
  }

  {
    const std::string key = "hsetex-x-future-with-fnx";
    create_keeper_and_expired(key);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kSet, future, HashFieldSetCondition::kFNX);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"X", "conditional"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 2, 1, future, future);
    EXPECT_EQ(decodedHashValue(key, "X"), (std::pair<std::string, uint64_t>{"conditional", future}));
  }
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetFieldsWithExpireDuplicatesUsePreWriteStateAndLastValue) {
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t expired_at = now - 1'000;
  const uint64_t future = now + 60'000;

  {
    const std::string key = "hsetex-duplicate-missing-fnx";
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard, 0, HashFieldSetCondition::kFNX);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"f", "first"}, {"f", "last"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 1, 1, 0, 0);
    EXPECT_EQ(decodedHashValue(key, "f"), (std::pair<std::string, uint64_t>{"last", 0}));
  }

  {
    const std::string key = "hsetex-duplicate-existing-fxx";
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"f", "old"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kSet, future, HashFieldSetCondition::kFXX);
    s = hash_->SetFieldsWithExpire(*ctx_, key, {{"f", "first"}, {"f", "last"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 1, 0, future, future);
    EXPECT_EQ(decodedHashValue(key, "f"), (std::pair<std::string, uint64_t>{"last", future}));
  }

  {
    const std::string key = "hsetex-duplicate-expired-keep";
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"K", "k"}, {"X", "x"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = putRawHashValue(key, "X", expired_at, "x");
    ASSERT_TRUE(s.ok()) << s.ToString();
    HashMetadata metadata = hashMetadata(key);
    metadata.persist = 1;
    metadata.lower = expired_at;
    metadata.upper = expired_at;
    s = putHashMetadata(key, metadata);
    ASSERT_TRUE(s.ok()) << s.ToString();
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kKeep);
    s = hash_->SetFieldsWithExpire(*ctx_, key, {{"X", "first"}, {"X", "last"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 2, 1, expired_at, expired_at);
    EXPECT_EQ(decodedHashValue(key, "X"), (std::pair<std::string, uint64_t>{"last", expired_at}));
  }

  {
    const std::string key = "hsetex-duplicate-immediate";
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"f", "old"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kSet, now);
    s = hash_->SetFieldsWithExpire(*ctx_, key, {{"f", "first"}, {"f", "last"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    HashMetadata metadata(false);
    EXPECT_TRUE(getHashMetadata(key, &metadata).IsNotFound());
  }
}

TEST_F(RedisHashFieldExpirationEncodingTest, GetFieldsWithExpireDuplicatesUseCommandLocalState) {
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t expired_at = now - 1'000;
  const uint64_t future = now + 60'000;

  {
    const std::string key = "hgetex-duplicate-immediate";
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"f", "value"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    auto options = getExOptions(HashGetExOptions::TTLAction::kSet, now);
    s = hash_->GetFieldsWithExpire(*ctx_, key, {"f", "f"}, options, &values, &statuses, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    expectGetResults(values, statuses, {"value", std::nullopt});
    HashMetadata metadata(false);
    EXPECT_TRUE(getHashMetadata(key, &metadata).IsNotFound());
  }

  {
    const std::string key = "hgetex-duplicate-persist";
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"f", "value"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = putRawHashValue(key, "f", future, "value");
    ASSERT_TRUE(s.ok()) << s.ToString();
    HashMetadata metadata = hashMetadata(key);
    metadata.persist = 0;
    metadata.lower = future;
    metadata.upper = future;
    s = putHashMetadata(key, metadata);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    auto options = getExOptions(HashGetExOptions::TTLAction::kPersist);
    s = hash_->GetFieldsWithExpire(*ctx_, key, {"f", "f"}, options, &values, &statuses, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    expectGetResults(values, statuses, {"value", "value"});
    expectHashMetadata(key, 1, 1, 0, 0);
  }

  {
    const std::string key = "hgetex-duplicate-future";
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"f", "value"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    auto options = getExOptions(HashGetExOptions::TTLAction::kSet, future);
    s = hash_->GetFieldsWithExpire(*ctx_, key, {"f", "f"}, options, &values, &statuses, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    expectGetResults(values, statuses, {"value", "value"});
    expectHashMetadata(key, 1, 0, future, future);
  }

  {
    const std::string key = "hgetex-duplicate-expired";
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"X", "x"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = putRawHashValue(key, "X", expired_at, "x");
    ASSERT_TRUE(s.ok()) << s.ToString();
    HashMetadata metadata = hashMetadata(key);
    metadata.persist = 0;
    metadata.lower = expired_at;
    metadata.upper = expired_at;
    s = putHashMetadata(key, metadata);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    auto options = getExOptions(HashGetExOptions::TTLAction::kNone);
    s = hash_->GetFieldsWithExpire(*ctx_, key, {"X", "X"}, options, &values, &statuses, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    expectGetResults(values, statuses, {std::nullopt, std::nullopt});
    EXPECT_TRUE(getHashMetadata(key, &metadata).IsNotFound());
  }

  {
    const std::string key = "hgetex-duplicate-ghost";
    createKeeperAndGhost(key, expired_at);
    HashMetadata before = hashMetadata(key);
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    auto options = getExOptions(HashGetExOptions::TTLAction::kNone);
    auto s = hash_->GetFieldsWithExpire(*ctx_, key, {"G", "G"}, options, &values, &statuses, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    expectGetResults(values, statuses, {std::nullopt, std::nullopt});
    EXPECT_EQ(hashMetadata(key), before);
  }
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetAndGetFieldsWithExpireKeepConservativeBoundsUntilLastTTL) {
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t t10 = now + 10'000;
  const uint64_t t20 = now + 20'000;
  const uint64_t t25 = now + 25'000;
  const uint64_t t30 = now + 30'000;
  bool applied = false;
  auto options = setExOptions(HashSetExOptions::TTLAction::kSet, t20);
  auto s = hash_->SetFieldsWithExpire(*ctx_, "hsetex-bounds", {{"a", "1"}, {"b", "2"}}, options, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_TRUE(applied);
  expectHashMetadata("hsetex-bounds", 2, 0, t20, t20);

  options.expire_at_ms = t30;
  s = hash_->SetFieldsWithExpire(*ctx_, "hsetex-bounds", {{"a", "3"}}, options, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expectHashMetadata("hsetex-bounds", 2, 0, t20, t30);

  options.expire_at_ms = t25;
  s = hash_->SetFieldsWithExpire(*ctx_, "hsetex-bounds", {{"b", "4"}}, options, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expectHashMetadata("hsetex-bounds", 2, 0, t20, t30);

  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  auto get_options = getExOptions(HashGetExOptions::TTLAction::kSet, t10);
  s = hash_->GetFieldsWithExpire(*ctx_, "hsetex-bounds", {"a"}, get_options, &values, &statuses, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expectHashMetadata("hsetex-bounds", 2, 0, t10, t30);

  get_options.expire_at_ms = t20;
  s = hash_->GetFieldsWithExpire(*ctx_, "hsetex-bounds", {"a"}, get_options, &values, &statuses, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expectHashMetadata("hsetex-bounds", 2, 0, t10, t30);

  get_options = getExOptions(HashGetExOptions::TTLAction::kPersist);
  s = hash_->GetFieldsWithExpire(*ctx_, "hsetex-bounds", {"a"}, get_options, &values, &statuses, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expectHashMetadata("hsetex-bounds", 2, 1, t10, t30);

  options = setExOptions(HashSetExOptions::TTLAction::kDiscard);
  s = hash_->SetFieldsWithExpire(*ctx_, "hsetex-bounds", {{"b", "persistent"}}, options, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expectHashMetadata("hsetex-bounds", 2, 2, 0, 0);
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetFieldsWithExpireDoesNotConsumeCompactionGhosts) {
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t ghost_expire = now - 1'000;
  const uint64_t future = now + 60'000;

  {
    const std::string key = "hsetex-ghost-persistent";
    createKeeperAndGhost(key, ghost_expire);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"G", "new-g"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 3, 2, ghost_expire, ghost_expire);
    EXPECT_EQ(decodedHashValue(key, "G"), (std::pair<std::string, uint64_t>{"new-g", 0}));

    uint64_t size = 0;
    s = hash_->Size(*ctx_, key, &size, HashLengthMode::kAccurate);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_EQ(size, 2);
    expectHashMetadata(key, 2, 2, 0, 0);
  }

  {
    const std::string key = "hsetex-ghost-future";
    createKeeperAndGhost(key, ghost_expire);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kSet, future);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"G", "new-g"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 3, 1, ghost_expire, future);
    EXPECT_EQ(decodedHashValue(key, "G"), (std::pair<std::string, uint64_t>{"new-g", future}));

    uint64_t size = 0;
    s = hash_->Size(*ctx_, key, &size, HashLengthMode::kAccurate);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_EQ(size, 2);
    expectHashMetadata(key, 2, 1, future, future);
  }

  {
    const std::string key = "hsetex-ghost-keep";
    createKeeperAndGhost(key, ghost_expire);
    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kKeep);
    auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"G", "new-g"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    expectHashMetadata(key, 3, 2, ghost_expire, ghost_expire);
    EXPECT_EQ(decodedHashValue(key, "G"), (std::pair<std::string, uint64_t>{"new-g", 0}));
  }
}

TEST_F(RedisHashFieldExpirationEncodingTest, GetFieldsWithExpireDistinguishesExpiredPhysicalFromGhost) {
  const std::string key = "hgetex-expired-and-ghost";
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t expired_at = now - 2'000;
  const uint64_t ghost_expire = now - 1'000;
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"X", "x"}, {"G", "g"}, {"K", "k"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 3);
  s = putRawHashValue(key, "X", expired_at, "x");
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = putRawHashValue(key, "G", ghost_expire, "g");
  ASSERT_TRUE(s.ok()) << s.ToString();
  HashMetadata metadata = hashMetadata(key);
  metadata.persist = 1;
  metadata.lower = expired_at;
  metadata.upper = ghost_expire;
  s = putHashMetadata(key, metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = deleteRawHashValue(key, "G");
  ASSERT_TRUE(s.ok()) << s.ToString();

  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  auto options = getExOptions(HashGetExOptions::TTLAction::kPersist);
  s = hash_->GetFieldsWithExpire(*ctx_, key, {"X", "G", "K"}, options, &values, &statuses, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expectGetResults(values, statuses, {std::nullopt, std::nullopt, "k"});
  expectHashMetadata(key, 2, 1, expired_at, ghost_expire);
  std::string raw_value;
  EXPECT_TRUE(getRawHashValue(key, "X", &raw_value).IsNotFound());
  EXPECT_TRUE(getRawHashValue(key, "G", &raw_value).IsNotFound());

  uint64_t size = 0;
  s = hash_->Size(*ctx_, key, &size, HashLengthMode::kAccurate);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(size, 1);
  expectHashMetadata(key, 1, 1, 0, 0);
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetAndGetFieldsWithExpirePreserveExistingKeyMetadataIdentity) {
  const std::string key = "hsetex-key-metadata-identity";
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t expired_at = now - 1'000;
  const uint64_t live_expire = now + 60'000;
  const uint64_t new_expire = now + 120'000;
  const uint64_t key_expire = now + 600'000;
  createFourStateHash(key, expired_at, live_expire);
  HashMetadata metadata = hashMetadata(key);
  metadata.expire = key_expire;
  auto s = putHashMetadata(key, metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();
  const HashMetadata identity = hashMetadata(key);

  auto expect_identity = [&]() {
    HashMetadata current = hashMetadata(key);
    EXPECT_EQ(current.flags, identity.flags);
    EXPECT_EQ(current.mode, identity.mode);
    EXPECT_EQ(current.version, identity.version);
    EXPECT_EQ(current.expire, identity.expire);
  };

  bool applied = false;
  auto set_options = setExOptions(HashSetExOptions::TTLAction::kSet, new_expire);
  s = hash_->SetFieldsWithExpire(*ctx_, key, {{"P", "new-p"}}, set_options, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_TRUE(applied);
  expect_identity();

  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  auto get_options = getExOptions(HashGetExOptions::TTLAction::kSet, new_expire);
  s = hash_->GetFieldsWithExpire(*ctx_, key, {"L"}, get_options, &values, &statuses, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expect_identity();

  values.clear();
  statuses.clear();
  get_options = getExOptions(HashGetExOptions::TTLAction::kPersist);
  s = hash_->GetFieldsWithExpire(*ctx_, key, {"L"}, get_options, &values, &statuses, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expect_identity();

  set_options = setExOptions(HashSetExOptions::TTLAction::kKeep);
  s = hash_->SetFieldsWithExpire(*ctx_, key, {{"X", "kept-x"}}, set_options, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  expect_identity();
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetFieldsWithExpireRetainsConservativeKeyTTLCornerState) {
  const std::string key = "hsetex-key-ttl-conservative-upper";
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t short_expire = now - 1'000;
  const uint64_t long_expire = now + 120'000;
  const uint64_t key_expire = now + 60'000;
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"X", "x"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = putRawHashValue(key, "X", short_expire, "x");
  ASSERT_TRUE(s.ok()) << s.ToString();
  HashMetadata metadata = hashMetadata(key);
  metadata.persist = 0;
  metadata.lower = short_expire;
  metadata.upper = long_expire;
  metadata.expire = key_expire;
  s = putHashMetadata(key, metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();
  const HashMetadata original_metadata = hashMetadata(key);

  bool applied = false;
  auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard);
  s = hash_->SetFieldsWithExpire(*ctx_, key, {{"new", "value"}}, options, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_TRUE(applied);
  metadata = hashMetadata(key);
  EXPECT_EQ(metadata.size, 2);
  EXPECT_EQ(metadata.persist, 1);
  EXPECT_EQ(metadata.lower, short_expire);
  EXPECT_EQ(metadata.upper, long_expire);
  EXPECT_EQ(metadata.expire, original_metadata.expire);
  EXPECT_EQ(metadata.version, original_metadata.version);
  EXPECT_EQ(metadata.flags, original_metadata.flags);
  EXPECT_EQ(decodedHashValue(key, "new"), (std::pair<std::string, uint64_t>{"value", 0}));
}

TEST_F(RedisHashFieldExpirationEncodingTest, SetFieldsWithExpireRecreatesExpiredKeyOrLeavesFXXMissing) {
  const uint64_t now = util::GetTimeStampMS();
  const uint64_t key_expired_at = now - 1'000;

  {
    const std::string key = "hsetex-recreate-expired-key";
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"old", "value"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    HashMetadata metadata = hashMetadata(key);
    const uint64_t old_version = metadata.version;
    metadata.expire = key_expired_at;
    s = putHashMetadata(key, metadata);
    ASSERT_TRUE(s.ok()) << s.ToString();

    bool applied = false;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard);
    s = hash_->SetFieldsWithExpire(*ctx_, key, {{"new", "new-value"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(applied);
    metadata = hashMetadata(key);
    EXPECT_NE(metadata.version, old_version);
    EXPECT_EQ(metadata.expire, 0);
    EXPECT_EQ(metadata.size, 1);
    EXPECT_EQ(metadata.persist, 1);
    EXPECT_EQ(metadata.lower, 0);
    EXPECT_EQ(metadata.upper, 0);
    EXPECT_EQ(decodedHashValue(key, "new"), (std::pair<std::string, uint64_t>{"new-value", 0}));
  }

  {
    const std::string key = "hsetex-fxx-expired-key";
    uint64_t ret = 0;
    auto s = hash_->MSet(*ctx_, key, {{"old", "value"}}, false, &ret);
    ASSERT_TRUE(s.ok()) << s.ToString();
    HashMetadata metadata = hashMetadata(key);
    metadata.expire = key_expired_at;
    s = putHashMetadata(key, metadata);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::string raw_before;
    s = db_->GetRawMetadata(*ctx_, db_->AppendNamespacePrefix(key), &raw_before);
    ASSERT_TRUE(s.ok()) << s.ToString();

    bool applied = true;
    auto options = setExOptions(HashSetExOptions::TTLAction::kDiscard, 0, HashFieldSetCondition::kFXX);
    s = hash_->SetFieldsWithExpire(*ctx_, key, {{"old", "new-value"}}, options, &applied, now);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_FALSE(applied);
    std::string raw_after;
    s = db_->GetRawMetadata(*ctx_, db_->AppendNamespacePrefix(key), &raw_after);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_EQ(raw_after, raw_before);
  }
}

TEST_F(RedisHashTest, SetAndGetFieldsWithExpireRejectExistingLegacyHashWithoutMutation) {
  redis::Database db(storage_.get(), "hash_ns");
  const std::string key = "hsetex-legacy-existing";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"f", "value"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 1);

  HashMetadata metadata(false);
  s = db.GetMetadata(*ctx_, {kRedisHash}, db.AppendNamespacePrefix(key), &metadata);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(metadata.mode, HashSubkeyEncodingMode::kLegacy);
  const std::string sub_key =
      InternalKey(db.AppendNamespacePrefix(key), "f", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string raw_metadata_before;
  std::string raw_value_before;
  s = db.GetRawMetadata(*ctx_, db.AppendNamespacePrefix(key), &raw_metadata_before);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = storage_->Get(*ctx_, ctx_->GetReadOptions(), sub_key, &raw_value_before);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(raw_value_before, "value");

  std::vector<HashSetExOptions> set_options;
  for (auto action : {HashSetExOptions::TTLAction::kDiscard, HashSetExOptions::TTLAction::kKeep}) {
    HashSetExOptions options;
    options.ttl_action = action;
    set_options.push_back(options);
  }
  for (uint64_t expire_at : {uint64_t{1}, util::GetTimeStampMS() + 60'000}) {
    HashSetExOptions options;
    options.ttl_action = HashSetExOptions::TTLAction::kSet;
    options.expire_at_ms = expire_at;
    set_options.push_back(options);
  }
  HashSetExOptions conditional;
  conditional.ttl_action = HashSetExOptions::TTLAction::kDiscard;
  conditional.condition = HashFieldSetCondition::kFXX;
  set_options.push_back(conditional);

  for (const auto &options : set_options) {
    bool applied = false;
    s = hash_->SetFieldsWithExpire(*ctx_, key, {{"f", "changed"}}, options, &applied, util::GetTimeStampMS());
    EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }

  std::vector<HashGetExOptions> get_options;
  for (auto action : {HashGetExOptions::TTLAction::kNone, HashGetExOptions::TTLAction::kPersist}) {
    HashGetExOptions options;
    options.ttl_action = action;
    get_options.push_back(options);
  }
  for (uint64_t expire_at : {uint64_t{1}, util::GetTimeStampMS() + 60'000}) {
    HashGetExOptions options;
    options.ttl_action = HashGetExOptions::TTLAction::kSet;
    options.expire_at_ms = expire_at;
    get_options.push_back(options);
  }
  for (const auto &options : get_options) {
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    s = hash_->GetFieldsWithExpire(*ctx_, key, {"f"}, options, &values, &statuses, util::GetTimeStampMS());
    EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }

  std::string raw_metadata_after;
  std::string raw_value_after;
  s = db.GetRawMetadata(*ctx_, db.AppendNamespacePrefix(key), &raw_metadata_after);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = storage_->Get(*ctx_, ctx_->GetReadOptions(), sub_key, &raw_value_after);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(raw_metadata_after, raw_metadata_before);
  EXPECT_EQ(raw_value_after, raw_value_before);
}

TEST_F(RedisHashTest, SetAndGetFieldsWithExpireHandleMissingKeyUnderLegacyConfig) {
  redis::Database db(storage_.get(), "hash_ns");
  const uint64_t now = util::GetTimeStampMS();
  const std::string key = "hsetex-legacy-missing";

  for (auto condition : {HashFieldSetCondition::kNone, HashFieldSetCondition::kFNX}) {
    for (auto action : {HashSetExOptions::TTLAction::kDiscard, HashSetExOptions::TTLAction::kKeep,
                        HashSetExOptions::TTLAction::kSet}) {
      HashSetExOptions options;
      options.ttl_action = action;
      options.expire_at_ms = action == HashSetExOptions::TTLAction::kSet ? now : 0;
      options.condition = condition;
      bool applied = false;
      auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"f", "value"}}, options, &applied, now);
      EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
      std::string raw_metadata;
      EXPECT_TRUE(db.GetRawMetadata(*ctx_, db.AppendNamespacePrefix(key), &raw_metadata).IsNotFound());
    }
  }

  HashSetExOptions fxx;
  fxx.ttl_action = HashSetExOptions::TTLAction::kDiscard;
  fxx.condition = HashFieldSetCondition::kFXX;
  bool applied = true;
  auto s = hash_->SetFieldsWithExpire(*ctx_, key, {{"f", "value"}}, fxx, &applied, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_FALSE(applied);

  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  HashGetExOptions get_options;
  get_options.ttl_action = HashGetExOptions::TTLAction::kSet;
  get_options.expire_at_ms = now + 60'000;
  s = hash_->GetFieldsWithExpire(*ctx_, key, {"a", "b"}, get_options, &values, &statuses, now);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(values.size(), 2);
  ASSERT_EQ(statuses.size(), 2);
  EXPECT_TRUE(statuses[0].IsNotFound());
  EXPECT_TRUE(statuses[1].IsNotFound());
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
