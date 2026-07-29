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
#include <limits>
#include <memory>
#include <vector>

#include "common/encoding.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "types/cuckoo_filter_page.h"
#include "types/cuckoo_filter_sub_filter.h"
#include "types/redis_cuckoo_chain.h"

class RedisCuckooFilterTest : public TestBase {
 public:
  RedisCuckooFilterTest(const RedisCuckooFilterTest &) = delete;
  RedisCuckooFilterTest &operator=(const RedisCuckooFilterTest &) = delete;
  RedisCuckooFilterTest(RedisCuckooFilterTest &&) = delete;
  RedisCuckooFilterTest &operator=(RedisCuckooFilterTest &&) = delete;

 protected:
  explicit RedisCuckooFilterTest() : TestBase() {
    cuckoo_ = std::make_unique<redis::CuckooChain>(storage_.get(), "cuckoo_ns");
    db_ = std::make_unique<redis::Database>(storage_.get(), "cuckoo_ns");
  }
  ~RedisCuckooFilterTest() override {
    // Ensure cuckoo_ is destroyed before storage_
    cuckoo_.reset();
    db_.reset();
  }

  void SetUp() override {
    const ::testing::TestInfo *const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    key_ = std::string("cf_test_") + test_info->name();
  }

  void verifyMetadata(const std::string &key, uint64_t capacity, uint8_t bucket_size, uint16_t max_iterations,
                      uint16_t expansion, uint64_t size, uint16_t n_filters, uint64_t num_deleted_items = 0,
                      uint32_t page_size = kCuckooFilterDefaultPageSize) {
    std::string ns_key = db_->AppendNamespacePrefix(key);
    CuckooChainMetadata metadata(false);
    auto s = db_->GetMetadata(*ctx_, {kRedisCuckooFilter}, ns_key, &metadata);
    ASSERT_TRUE(s.ok()) << key << ": metadata not found";
    EXPECT_EQ(metadata.Type(), kRedisCuckooFilter) << key;
    EXPECT_EQ(metadata.base_capacity, capacity) << key;
    EXPECT_EQ(metadata.bucket_size, bucket_size) << key;
    EXPECT_EQ(metadata.max_iterations, max_iterations) << key;
    EXPECT_EQ(metadata.expansion, expansion) << key;
    EXPECT_EQ(metadata.size, size) << key;
    EXPECT_EQ(metadata.n_filters, n_filters) << key;
    EXPECT_EQ(metadata.num_deleted_items, num_deleted_items) << key;
    EXPECT_EQ(metadata.page_size, page_size) << key;
  }

  void reserveAndVerify(const std::string &key, uint64_t capacity, uint8_t bucket_size, uint16_t max_iterations,
                        uint16_t expansion, uint32_t page_size = kCuckooFilterDefaultPageSize) {
    auto s = cuckoo_->Reserve(*ctx_, key, capacity, bucket_size, max_iterations, expansion, page_size);
    ASSERT_TRUE(s.ok()) << key << ": " << s.ToString();
    verifyMetadata(key, capacity, bucket_size, max_iterations, expansion, 0, 1, 0, page_size);
  }

  void addAndVerify(const std::string &key, const std::string &item, uint64_t capacity, uint8_t bucket_size,
                    uint16_t max_iterations, uint16_t expansion, uint64_t expected_size, uint16_t n_filters = 1) {
    bool added = false;
    auto s = cuckoo_->Add(*ctx_, key, item, &added);
    ASSERT_TRUE(s.ok()) << key << ": add '" << item << "' failed: " << s.ToString();
    ASSERT_TRUE(added) << key << ": item '" << item << "' should have been added";
    verifyMetadata(key, capacity, bucket_size, max_iterations, expansion, expected_size, n_filters, 0);
  }

  CuckooChainMetadata getMetadata(const std::string &key) {
    std::string ns_key = db_->AppendNamespacePrefix(key);
    CuckooChainMetadata metadata(false);
    auto s = db_->GetMetadata(*ctx_, {kRedisCuckooFilter}, ns_key, &metadata);
    EXPECT_TRUE(s.ok()) << key << ": metadata not found";
    return metadata;
  }

  std::string makePageKey(const std::string &key, const CuckooChainMetadata &metadata, uint16_t filter_index,
                          uint32_t page_index) {
    std::string sub_key;
    PutFixed16(&sub_key, filter_index);
    PutFixed32(&sub_key, page_index);
    return InternalKey(db_->AppendNamespacePrefix(key), sub_key, metadata.version, storage_->IsSlotIdEncoded())
        .Encode();
  }

  rocksdb::Status readPage(const std::string &page_key, std::string *value) {
    return storage_->Get(*ctx_, ctx_->GetReadOptions(), storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), page_key,
                         value);
  }

  void writePage(const std::string &page_key, const std::string &value) {
    auto batch = storage_->GetWriteBatchBase();
    auto s = batch->Put(page_key, value);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = storage_->Write(*ctx_, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  void writeMetadata(const std::string &key, const CuckooChainMetadata &metadata) {
    std::string metadata_bytes;
    metadata.Encode(&metadata_bytes);
    auto batch = storage_->GetWriteBatchBase();
    auto s =
        batch->Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), db_->AppendNamespacePrefix(key), metadata_bytes);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = storage_->Write(*ctx_, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  std::unique_ptr<redis::CuckooChain> cuckoo_;
  std::unique_ptr<redis::Database> db_;
  std::string key_;
};

TEST_F(RedisCuckooFilterTest, ReserveInvalidParams) {
  struct InvalidTestCase {
    std::string key;
    uint64_t capacity;
    uint8_t bucket_size;
    uint16_t max_iterations;
    uint16_t expansion;
    std::string err;
  };

  std::vector<InvalidTestCase> invalid_test_cases = {
      {"zero_capacity", 0, 4, 500, 2, "capacity must be larger than 0"},
      {"capacity_too_small", 1, 4, 500, 2, "capacity must be at least 2"},
      {"zero_bucket_size", 1000, 0, 500, 2, "bucket_size must be between 1 and 255"},
      {"zero_max_iterations", 1000, 4, 0, 2, "max_iterations must be larger than 0"},
      {"capacity_too_large", std::numeric_limits<uint64_t>::max(), 4, 500, 2, "capacity is too large"},
      {"expansion_too_large", 1000, 4, 500, redis::kCFMaxExpansion + 1, "expansion must be between 0 and 32768"},
      {"zero_page_size", 1000, 4, 500, 2, "page_size must be larger than 0"},
      {"page_size_smaller_than_bucket", 1000, 4, 500, 2, "page_size must be at least bucket_size"},
  };

  for (const auto &test_case : invalid_test_cases) {
    uint32_t page_size = kCuckooFilterDefaultPageSize;
    if (test_case.key == "zero_page_size") {
      page_size = 0;
    } else if (test_case.key == "page_size_smaller_than_bucket") {
      page_size = test_case.bucket_size - 1;
    }
    auto s = cuckoo_->Reserve(*ctx_, test_case.key, test_case.capacity, test_case.bucket_size, test_case.max_iterations,
                              test_case.expansion, page_size);
    ASSERT_FALSE(s.ok()) << test_case.key;
    ASSERT_TRUE(s.IsInvalidArgument()) << test_case.key << ": " << s.ToString();
    ASSERT_NE(s.ToString().find(test_case.err), std::string::npos) << test_case.key << ": " << s.ToString();
  }
}

TEST_F(RedisCuckooFilterTest, ReserveValidParams) {
  struct TestCase {
    std::string key;
    uint64_t capacity;
    uint8_t bucket_size;
    uint16_t max_iterations;
    uint16_t expansion;
  };

  std::vector<TestCase> test_cases = {
      {"min_capacity", 2, 4, 500, 2},
      {"min_bucket_size", 1000, 1, 500, 2},
      {"max_bucket_size", 1000, 255, 500, 2},
      {"min_max_iterations", 1000, 4, 1, 2},
      {"max_max_iterations", 1000, 4, 65535, 2},
      // RedisBloom allows expansion=0; it disables creating additional sub-filters.
      {"no_auto_expansion", 2, 1, 1, 0},
      {"capacity_3", 3, 4, 500, 2},
      {"capacity_10", 10, 4, 500, 2},
      {"capacity_100", 100, 4, 500, 2},
      {"capacity_10000", 10000, 4, 500, 2},
      {"capacity_100000", 100000, 4, 500, 2},
      {"bucket_size_2", 1000, 2, 500, 2},
      {"bucket_size_8", 1000, 8, 500, 2},
      {"bucket_size_16", 1000, 16, 500, 2},
      {"bucket_size_128", 1000, 128, 500, 2},
      {"no_auto_expansion_regular", 1000, 4, 500, 0},
      // expansion=1 is valid and creates additional sub-filters with the same capacity.
      {"expansion_1", 1000, 4, 500, 1},
      {"expansion_8", 1000, 4, 500, 8},
      {"expansion_256", 1000, 4, 500, 256},
      {"max_expansion", 1000, 4, 500, redis::kCFMaxExpansion},
      {"capacity_10M", 10000000, 4, 500, 2},
      {"capacity_100M", 100000000, 4, 500, 2},
      {"max_params", 100000, 255, 65535, redis::kCFMaxExpansion},
      {"mixed_params", 50000, 8, 1000, 4},
  };

  for (const auto &test_case : test_cases) {
    reserveAndVerify(test_case.key, test_case.capacity, test_case.bucket_size, test_case.max_iterations,
                     test_case.expansion);
  }
}

TEST_F(RedisCuckooFilterTest, ReserveRoundsExpansionToPowerOfTwo) {
  auto s = cuckoo_->Reserve(*ctx_, key_, 1000, 4, 500, 3, kCuckooFilterDefaultPageSize);
  ASSERT_TRUE(s.ok()) << s.ToString();
  verifyMetadata(key_, 1000, 4, 500, 4, 0, 1);

  EXPECT_EQ(redis::CuckooFilterHelper::NormalizeExpansion(0), 0);
  EXPECT_EQ(redis::CuckooFilterHelper::NormalizeExpansion(1), 1);
  EXPECT_EQ(redis::CuckooFilterHelper::NormalizeExpansion(3), 4);
  EXPECT_EQ(redis::CuckooFilterHelper::NormalizeExpansion(5), 8);
  EXPECT_EQ(redis::CuckooFilterHelper::NormalizeExpansion(32767), 32768);
}

TEST_F(RedisCuckooFilterTest, ReserveKeepsZeroExpansionNonScaling) {
  reserveAndVerify(key_, 2, 1, 1, 0);

  uint64_t added_count = 0;
  bool full = false;
  for (int i = 0; i < 100; ++i) {
    bool added = false;
    auto s = cuckoo_->Add(*ctx_, key_, "item_" + std::to_string(i), &added);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsAborted()) << s.ToString();
      full = true;
      break;
    }
    ASSERT_TRUE(added);
    ++added_count;
  }

  ASSERT_TRUE(full);
  verifyMetadata(key_, 2, 1, 1, 0, added_count, 1, 0);
}

TEST_F(RedisCuckooFilterTest, ReserveDuplicate) {
  reserveAndVerify(key_, 1000, 4, 500, 2);

  // Second reserve with same key should fail
  auto s = cuckoo_->Reserve(*ctx_, key_, 2000, 4, 500, 2, kCuckooFilterDefaultPageSize);
  ASSERT_FALSE(s.ok());
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_NE(s.ToString().find("already exists"), std::string::npos);
}

TEST_F(RedisCuckooFilterTest, CalculateRequiredBucketsCalculation) {
  struct TestCase {
    uint64_t capacity;
    uint8_t bucket_size;
    uint32_t expected_num_buckets;
  };

  std::vector<TestCase> test_cases = {
      {0, 4, 1},     {1, 4, 1},     {2, 1, 4},      {4, 4, 2},      {488, 4, 128},   {489, 4, 256},
      {977, 4, 256}, {978, 4, 512}, {1000, 4, 512}, {1024, 4, 512}, {1000, 1, 2048}, {1000, 16, 128},
  };

  for (const auto &test_case : test_cases) {
    uint32_t num_buckets = 0;
    auto s =
        redis::CuckooFilterHelper::CalculateRequiredBuckets(test_case.capacity, test_case.bucket_size, &num_buckets);
    ASSERT_TRUE(s.ok()) << "capacity=" << test_case.capacity
                        << ", bucket_size=" << static_cast<int>(test_case.bucket_size) << ": " << s.ToString();
    ASSERT_EQ(num_buckets, test_case.expected_num_buckets)
        << "capacity=" << test_case.capacity << ", bucket_size=" << static_cast<int>(test_case.bucket_size);
    ASSERT_EQ(num_buckets & (num_buckets - 1), 0) << "Number of buckets should be power of 2";

    auto expected_min =
        static_cast<uint32_t>(static_cast<long double>(test_case.capacity) / test_case.bucket_size / 0.955L);
    ASSERT_GE(num_buckets, expected_min) << "Number of buckets too small for capacity";
  }
}

TEST_F(RedisCuckooFilterTest, FingerprintGeneration) {
  // Test fingerprint generation ensures non-zero values in range [1, 255]
  // Following RedisBloom: fp = hash % 255 + 1
  for (uint64_t hash = 0; hash < 1000; ++hash) {
    uint8_t fp = redis::CuckooFilterHelper::GenerateFingerprint(hash);
    ASSERT_GE(fp, 1) << "Fingerprint should be at least 1";
    ASSERT_LE(fp, 255) << "Fingerprint should be at most 255";
  }

  // Verify the formula: fp = hash % 255 + 1
  ASSERT_EQ(redis::CuckooFilterHelper::GenerateFingerprint(0), 1);
  ASSERT_EQ(redis::CuckooFilterHelper::GenerateFingerprint(254), 255);
  ASSERT_EQ(redis::CuckooFilterHelper::GenerateFingerprint(255), 1);
  ASSERT_EQ(redis::CuckooFilterHelper::GenerateFingerprint(256), 2);
}

TEST_F(RedisCuckooFilterTest, AlternateBucketCalculation) {
  std::vector<uint32_t> num_buckets_cases = {1, 2, 128, 256, 512, 1024, 2048};

  // Test GetAltHash symmetry at hash level (following RedisBloom design)
  // h2 = GetAltHash(fp, h1)
  // h1 = GetAltHash(fp, h2)  <- this is the symmetry property
  for (auto num_buckets : num_buckets_cases) {
    for (uint64_t hash = 0; hash < 100; ++hash) {
      for (uint16_t fp = 1; fp <= 255; ++fp) {
        auto fingerprint = static_cast<uint8_t>(fp);
        uint64_t alt_hash = redis::CuckooFilterHelper::GetAltHash(fingerprint, hash);

        // Applying GetAltHash twice should return original hash
        uint64_t double_alt_hash = redis::CuckooFilterHelper::GetAltHash(fingerprint, alt_hash);
        ASSERT_EQ(double_alt_hash, hash) << "Double alternate hash should give original hash";

        // Both hashes should map to valid bucket indices
        uint32_t bucket1 = hash % num_buckets;
        uint32_t bucket2 = alt_hash % num_buckets;
        ASSERT_LT(bucket1, num_buckets) << "Bucket 1 out of range";
        ASSERT_LT(bucket2, num_buckets) << "Bucket 2 out of range";
      }
    }
  }
}

TEST_F(RedisCuckooFilterTest, HashFunction) {
  // Test that Hash function produces consistent 64-bit values
  std::string test_item = "hello";
  uint64_t hash1 = redis::CuckooFilterHelper::Hash(test_item);
  uint64_t hash2 = redis::CuckooFilterHelper::Hash(test_item.data(), test_item.size());

  // Both methods should produce the same result
  ASSERT_EQ(hash1, hash2) << "Hash methods should be consistent";

  // Hash should be deterministic
  uint64_t hash3 = redis::CuckooFilterHelper::Hash(test_item);
  ASSERT_EQ(hash1, hash3) << "Hash should be deterministic";

  // Different items should produce different hashes (with high probability)
  uint64_t hash_world = redis::CuckooFilterHelper::Hash("world");
  ASSERT_NE(hash1, hash_world) << "Different items should have different hashes";

  // Empty string produces hash value 0 (this is expected with MurmurHash)
  uint64_t hash_empty = redis::CuckooFilterHelper::Hash("");
  ASSERT_EQ(hash_empty, 0) << "Empty string should produce hash value 0 with MurmurHash";

  // Even with hash=0, fingerprint should be non-zero
  uint8_t fp_empty = redis::CuckooFilterHelper::GenerateFingerprint(hash_empty);
  ASSERT_EQ(fp_empty, 1) << "Fingerprint of hash=0 should be 1 (0 % 255 + 1)";

  // Test that hash can be used with fingerprint generation
  uint8_t fp = redis::CuckooFilterHelper::GenerateFingerprint(hash1);
  ASSERT_GE(fp, 1) << "Fingerprint should be at least 1";
  ASSERT_LE(fp, 255) << "Fingerprint should be at most 255";
}

TEST_F(RedisCuckooFilterTest, MetadataEncodeDecodeRoundTrip) {
  CuckooChainMetadata metadata(false);
  metadata.expire = 2000;
  metadata.version = 5678;
  metadata.size = 42;
  metadata.n_filters = 3;
  metadata.expansion = 4;
  metadata.base_capacity = 1000;
  metadata.bucket_size = 7;
  metadata.max_iterations = 20;
  metadata.num_deleted_items = 5;
  metadata.page_size = 4096;

  std::string encoded;
  metadata.Encode(&encoded);

  Slice input(encoded);
  CuckooChainMetadata decoded(false);
  auto s = decoded.Decode(&input);
  ASSERT_TRUE(s.ok()) << s.ToString();

  EXPECT_EQ(decoded.Type(), kRedisCuckooFilter);
  EXPECT_EQ(decoded.expire, metadata.expire);
  EXPECT_EQ(decoded.version, metadata.version);
  EXPECT_EQ(decoded.size, metadata.size);
  EXPECT_EQ(decoded.n_filters, metadata.n_filters);
  EXPECT_EQ(decoded.expansion, metadata.expansion);
  EXPECT_EQ(decoded.base_capacity, metadata.base_capacity);
  EXPECT_EQ(decoded.bucket_size, metadata.bucket_size);
  EXPECT_EQ(decoded.max_iterations, metadata.max_iterations);
  EXPECT_EQ(decoded.num_deleted_items, metadata.num_deleted_items);
  EXPECT_EQ(decoded.page_size, metadata.page_size);
}

TEST_F(RedisCuckooFilterTest, ReserveVerifyMetadata) {
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint16_t expansion = 2;

  // Create the filter
  reserveAndVerify(key_, capacity, bucket_size, max_iterations, expansion);

  // Verify metadata was stored by trying to reserve again with same key
  // This should fail with "already exists" error
  auto s =
      cuckoo_->Reserve(*ctx_, key_, capacity * 2, bucket_size, max_iterations, expansion, kCuckooFilterDefaultPageSize);
  ASSERT_FALSE(s.ok()) << "Second reserve with same key should fail";
  ASSERT_TRUE(s.IsInvalidArgument()) << "Should return InvalidArgument error";
  ASSERT_NE(s.ToString().find("already exists"), std::string::npos) << "Error message should mention 'already exists'";

  // Verify we can still create filters with different keys
  reserveAndVerify("different_key", capacity, bucket_size, max_iterations, expansion);

  // Verify the original key still exists (can't create it again)
  s = cuckoo_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion, kCuckooFilterDefaultPageSize);
  ASSERT_FALSE(s.ok()) << "Original key should still exist";
  ASSERT_NE(s.ToString().find("already exists"), std::string::npos);
}

TEST_F(RedisCuckooFilterTest, ReservePersistsPageSize) {
  constexpr uint32_t page_size = 4096;
  reserveAndVerify(key_, 1000, 4, 500, 2, page_size);
}

TEST_F(RedisCuckooFilterTest, AddBasic) {
  reserveAndVerify(key_, 1000, 4, 500, 2);
  addAndVerify(key_, "item1", 1000, 4, 500, 2, 1);
}

TEST_F(RedisCuckooFilterTest, ReserveDoesNotPreallocatePages) {
  reserveAndVerify(key_, 1000, 4, 500, 2);

  auto metadata = getMetadata(key_);
  std::string page;
  auto s = readPage(makePageKey(key_, metadata, 0, 0), &page);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(RedisCuckooFilterTest, AddWritesPagedLayout) {
  constexpr uint64_t capacity = 1000;
  constexpr uint8_t bucket_size = 4;
  reserveAndVerify(key_, capacity, bucket_size, 500, 2);

  const std::string item = "item1";
  addAndVerify(key_, item, capacity, bucket_size, 500, 2, 1);

  auto metadata = getMetadata(key_);
  uint32_t num_buckets = 0;
  auto s = redis::CuckooFilterHelper::CalculateRequiredBuckets(capacity, bucket_size, &num_buckets);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto hash = redis::CuckooFilterHelper::Hash(item);
  auto fingerprint = redis::CuckooFilterHelper::GenerateFingerprint(hash);
  auto bucket1_idx = static_cast<uint32_t>(hash % num_buckets);
  auto bucket2_idx = static_cast<uint32_t>(redis::CuckooFilterHelper::GetAltHash(fingerprint, hash) % num_buckets);
  auto buckets_per_page = metadata.page_size / bucket_size;
  auto page_index = bucket1_idx / buckets_per_page;
  auto page_size = std::min(buckets_per_page, num_buckets - page_index * buckets_per_page) * bucket_size;

  std::string page;
  s = readPage(makePageKey(key_, metadata, 0, page_index), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(page.size(), page_size);

  auto bucket1_offset = (bucket1_idx % buckets_per_page) * bucket_size;
  auto bucket2_offset = (bucket2_idx % buckets_per_page) * bucket_size;
  bool found = false;
  for (uint32_t i = 0; i < bucket_size; ++i) {
    found = found || static_cast<uint8_t>(page[bucket1_offset + i]) == fingerprint;
    if (bucket2_idx / buckets_per_page == page_index) {
      found = found || static_cast<uint8_t>(page[bucket2_offset + i]) == fingerprint;
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(RedisCuckooFilterTest, AddToNonExistentFilter) {
  std::string key = "nonexistent_key";
  addAndVerify(key, "item1", redis::kCFDefaultCapacity, redis::kCFDefaultBucketSize, redis::kCFDefaultMaxIterations,
               redis::kCFDefaultExpansion, 1);

  auto s = cuckoo_->Reserve(*ctx_, key, 1000, 4, 500, 2, kCuckooFilterDefaultPageSize);
  ASSERT_FALSE(s.ok());
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_NE(s.ToString().find("already exists"), std::string::npos);
}

TEST_F(RedisCuckooFilterTest, AddWithDifferentBucketSizes) {
  std::vector<uint8_t> bucket_sizes = {1, 2, 4, 8, 16};

  for (auto bs : bucket_sizes) {
    std::string test_key = key_ + "_bucket_" + std::to_string(bs);
    reserveAndVerify(test_key, 100, bs, 500, 2);
    for (int j = 0; j < 10; ++j) {
      addAndVerify(test_key, "item_" + std::to_string(j), 100, bs, 500, 2, j + 1);
    }
  }
}

TEST_F(RedisCuckooFilterTest, AddDuplicateItems) {
  reserveAndVerify(key_, 1000, 4, 500, 2);
  for (int i = 0; i < 5; ++i) {
    addAndVerify(key_, "duplicate_item", 1000, 4, 500, 2, i + 1);
  }
}

TEST_F(RedisCuckooFilterTest, AddPrioritizesNewestFilter) {
  CuckooChainMetadata metadata;
  metadata.size = 0;
  metadata.base_capacity = 1000;
  metadata.bucket_size = 4;
  metadata.max_iterations = 500;
  metadata.expansion = 2;
  metadata.n_filters = 2;
  metadata.num_deleted_items = 0;
  metadata.page_size = kCuckooFilterDefaultPageSize;
  writeMetadata(key_, metadata);

  const std::string item = "item";
  addAndVerify(key_, item, metadata.base_capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion, 1,
               metadata.n_filters);

  auto stored_metadata = getMetadata(key_);
  ASSERT_EQ(stored_metadata.n_filters, 2);
  ASSERT_EQ(stored_metadata.size, 1);

  auto hash = redis::CuckooFilterHelper::Hash(item);
  uint32_t old_num_buckets = 0;
  auto s = redis::CuckooFilterHelper::CalculateRequiredBuckets(metadata.base_capacity, metadata.bucket_size,
                                                               &old_num_buckets);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto buckets_per_page = stored_metadata.page_size / stored_metadata.bucket_size;
  auto old_page_idx = static_cast<uint32_t>(hash % old_num_buckets) / buckets_per_page;

  std::string page;
  s = readPage(makePageKey(key_, stored_metadata, 0, old_page_idx), &page);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();

  uint32_t num_buckets = 0;
  s = redis::CuckooFilterHelper::CalculateRequiredBuckets(metadata.base_capacity * metadata.expansion,
                                                          metadata.bucket_size, &num_buckets);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto bucket1_idx = static_cast<uint32_t>(hash % num_buckets);
  auto expected_page_idx = bucket1_idx / buckets_per_page;

  s = readPage(makePageKey(key_, stored_metadata, 1, expected_page_idx), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(RedisCuckooFilterTest, AddManyItems) {
  reserveAndVerify(key_, 1000, 4, 500, 2);
  for (int i = 0; i < 100; ++i) {
    addAndVerify(key_, "item_" + std::to_string(i), 1000, 4, 500, 2, i + 1);
  }
}

TEST_F(RedisCuckooFilterTest, AddSmallFilterCapacity) {
  uint64_t small_capacity = 10;
  reserveAndVerify(key_, small_capacity, 2, 500, 0);

  uint64_t added_count = 0;
  bool full = false;
  for (int i = 0; i < 100; ++i) {
    std::string item = "item_" + std::to_string(i);
    bool added = false;
    auto s = cuckoo_->Add(*ctx_, key_, item, &added);

    if (!s.ok()) {
      ASSERT_TRUE(s.IsAborted()) << "Should be Aborted status when full";
      full = true;
      break;
    }

    ASSERT_TRUE(added) << "Item should have been added before the filter is full";
    ++added_count;
  }

  ASSERT_TRUE(full) << "Small filter should eventually become full";
  verifyMetadata(key_, small_capacity, 2, 500, 0, added_count, 1, 0);
}

TEST_F(RedisCuckooFilterTest, AddEdgeCaseItems) {
  reserveAndVerify(key_, 1000, 4, 500, 2);
  std::vector<std::string> items = {
      "",
      std::string(10000, 'x'),
      std::string("\x00\x01\x02\xFF\xFE", 5),
  };
  for (size_t i = 0; i < items.size(); ++i) {
    addAndVerify(key_, items[i], 1000, 4, 500, 2, i + 1);
  }
}

TEST_F(RedisCuckooFilterTest, KickOutSuccessWritesDirtyPages) {
  constexpr uint64_t capacity = 2;
  constexpr uint8_t bucket_size = 1;
  constexpr uint32_t num_buckets = 4;
  reserveAndVerify(key_, capacity, bucket_size, 1, 0);

  struct Candidate {
    std::string item;
    uint8_t fingerprint = 0;
    uint32_t bucket1 = 0;
    uint32_t bucket2 = 0;
  };

  std::vector<Candidate> candidates;
  for (int i = 0; i < 10000; ++i) {
    std::string item = "kick_item_" + std::to_string(i);
    auto hash = redis::CuckooFilterHelper::Hash(item);
    auto fingerprint = redis::CuckooFilterHelper::GenerateFingerprint(hash);
    candidates.push_back(
        {item, fingerprint, static_cast<uint32_t>(hash % num_buckets),
         static_cast<uint32_t>(redis::CuckooFilterHelper::GetAltHash(fingerprint, hash) % num_buckets)});
  }

  Candidate first;
  Candidate second;
  Candidate kicked;
  uint32_t evicted_bucket = 0;
  bool found = false;
  for (const auto &candidate : candidates) {
    if (candidate.bucket1 == candidate.bucket2) continue;
    for (const auto &first_candidate : candidates) {
      if (first_candidate.item == candidate.item || first_candidate.bucket1 != candidate.bucket1) continue;
      auto alt_for_victim =
          redis::CuckooFilterHelper::GetAltBucketIndex(candidate.bucket1, first_candidate.fingerprint, num_buckets);
      if (alt_for_victim == candidate.bucket1 || alt_for_victim == candidate.bucket2) continue;

      for (const auto &second_candidate : candidates) {
        if (second_candidate.item == candidate.item || second_candidate.item == first_candidate.item ||
            second_candidate.bucket1 != candidate.bucket2) {
          continue;
        }
        first = first_candidate;
        second = second_candidate;
        kicked = candidate;
        evicted_bucket = alt_for_victim;
        found = true;
        break;
      }
      if (found) break;
    }
    if (found) break;
  }
  ASSERT_TRUE(found);

  addAndVerify(key_, first.item, capacity, bucket_size, 1, 0, 1);
  addAndVerify(key_, second.item, capacity, bucket_size, 1, 0, 2);
  addAndVerify(key_, kicked.item, capacity, bucket_size, 1, 0, 3);

  auto metadata = getMetadata(key_);
  ASSERT_EQ(metadata.n_filters, 1);
  ASSERT_EQ(metadata.size, 3);

  std::string page;
  auto s = readPage(makePageKey(key_, metadata, 0, 0), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(page.size(), num_buckets * bucket_size);
  EXPECT_EQ(static_cast<uint8_t>(page[kicked.bucket1]), kicked.fingerprint);
  EXPECT_EQ(static_cast<uint8_t>(page[kicked.bucket2]), second.fingerprint);
  EXPECT_EQ(static_cast<uint8_t>(page[evicted_bucket]), first.fingerprint);
}

TEST_F(RedisCuckooFilterTest, KickOutErrorDiscardsDirtyPages) {
  constexpr uint8_t bucket_size = 1;
  constexpr uint32_t page_size = 1;
  constexpr uint32_t num_buckets = 2;
  CuckooChainMetadata metadata(false);
  metadata.version = 1;
  metadata.size = 0;
  metadata.base_capacity = 1;
  metadata.bucket_size = bucket_size;
  metadata.max_iterations = 2;
  metadata.expansion = 0;
  metadata.n_filters = 1;
  metadata.num_deleted_items = 0;
  metadata.page_size = page_size;
  writeMetadata(key_, metadata);

  constexpr uint64_t hash = 0;
  constexpr uint8_t fingerprint = 2;
  constexpr uint8_t existing_fingerprint = 1;
  ASSERT_EQ(redis::CuckooFilterHelper::GetAltBucketIndex(0, existing_fingerprint, num_buckets), 1);
  std::string original_page{static_cast<char>(existing_fingerprint)};
  writePage(makePageKey(key_, metadata, 0, 0), original_page);
  writePage(makePageKey(key_, metadata, 0, 1), std::string(2, static_cast<char>(9)));

  redis::CuckooSubFilter sub_filter(storage_.get(), *ctx_, db_->AppendNamespacePrefix(key_),
                                    storage_->IsSlotIdEncoded(), metadata.version, metadata.bucket_size,
                                    metadata.page_size, 0, num_buckets);
  bool inserted = true;
  auto s = sub_filter.TryKickOutInsert(hash, fingerprint, metadata.max_iterations, &inserted);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_FALSE(inserted);

  auto batch = storage_->GetWriteBatchBase();
  s = sub_filter.WriteToBatch(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = storage_->Write(*ctx_, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  ASSERT_TRUE(s.ok()) << s.ToString();

  std::string page;
  s = readPage(makePageKey(key_, metadata, 0, 0), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(page, original_page);
}

TEST_F(RedisCuckooFilterTest, ExpansionWritesNewFilterIndexPage) {
  constexpr uint64_t capacity = 2;
  constexpr uint8_t bucket_size = 1;
  reserveAndVerify(key_, capacity, bucket_size, 1, 2);

  CuckooChainMetadata metadata(false);
  uint64_t added_count = 0;
  for (int i = 0; i < 100; ++i) {
    bool added = false;
    auto s = cuckoo_->Add(*ctx_, key_, "item_" + std::to_string(i), &added);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(added);
    ++added_count;

    metadata = getMetadata(key_);
    if (metadata.n_filters > 1) break;
  }

  ASSERT_GT(metadata.n_filters, 1);
  EXPECT_EQ(metadata.size, added_count);

  uint32_t num_buckets = 0;
  uint64_t new_filter_capacity = capacity;
  for (uint16_t i = 0; i < metadata.n_filters - 1; ++i) {
    new_filter_capacity *= metadata.expansion;
  }
  auto s = redis::CuckooFilterHelper::CalculateRequiredBuckets(new_filter_capacity, bucket_size, &num_buckets);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto expected_page_size = std::min(metadata.page_size / bucket_size, num_buckets) * bucket_size;

  std::string page;
  s = readPage(makePageKey(key_, metadata, metadata.n_filters - 1, 0), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(page.size(), expected_page_size);
}
