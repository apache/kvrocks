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
#include <memory>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "types/redis_cuckoo_chain.h"

class RedisCuckooFilterTest : public TestBase {
 protected:
  explicit RedisCuckooFilterTest() : TestBase() {
    cuckoo_ = std::make_unique<redis::CuckooChain>(storage_.get(), "cuckoo_ns");
  }
  ~RedisCuckooFilterTest() override = default;

  void SetUp() override {
    key_ = "test_cuckoo_filter_key";
  }

  void TearDown() override {
    [[maybe_unused]] auto s = cuckoo_->Del(*ctx_, key_);
  }

  std::unique_ptr<redis::CuckooChain> cuckoo_;
  std::string key_;
};

TEST_F(RedisCuckooFilterTest, ReserveBasic) {
  // Test basic reserve operation
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint8_t expansion = 2;

  auto s = cuckoo_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  ASSERT_TRUE(s.ok()) << "Failed to reserve cuckoo filter: " << s.ToString();
}

TEST_F(RedisCuckooFilterTest, ReserveDuplicate) {
  // First reserve should succeed
  auto s = cuckoo_->Reserve(*ctx_, key_, 1000, 4, 500, 2);
  ASSERT_TRUE(s.ok());

  // Second reserve with same key should fail
  s = cuckoo_->Reserve(*ctx_, key_, 2000, 4, 500, 2);
  ASSERT_FALSE(s.ok());
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_NE(s.ToString().find("already exists"), std::string::npos);
}

TEST_F(RedisCuckooFilterTest, ReserveInvalidParams) {
  // Test with zero capacity
  auto s = cuckoo_->Reserve(*ctx_, key_, 0, 4, 500, 2);
  ASSERT_FALSE(s.ok());
  ASSERT_TRUE(s.IsInvalidArgument());

  // Test with zero bucket size
  s = cuckoo_->Reserve(*ctx_, "key2", 1000, 0, 500, 2);
  ASSERT_FALSE(s.ok());
  ASSERT_TRUE(s.IsInvalidArgument());

  // Test with zero max iterations
  s = cuckoo_->Reserve(*ctx_, "key3", 1000, 4, 0, 2);
  ASSERT_FALSE(s.ok());
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_F(RedisCuckooFilterTest, ReserveVariousCapacities) {
  // Test with different capacities
  std::vector<uint64_t> capacities = {100, 1000, 10000, 100000};

  for (size_t i = 0; i < capacities.size(); ++i) {
    std::string test_key = key_ + "_" + std::to_string(i);
    auto s = cuckoo_->Reserve(*ctx_, test_key, capacities[i], 4, 500, 2);
    ASSERT_TRUE(s.ok()) << "Failed for capacity " << capacities[i];
  }
}

TEST_F(RedisCuckooFilterTest, ReserveWithDifferentBucketSizes) {
  // Test with different valid bucket sizes
  std::vector<uint8_t> bucket_sizes = {1, 2, 4, 8, 16};

  for (size_t i = 0; i < bucket_sizes.size(); ++i) {
    std::string test_key = key_ + "_bucket_" + std::to_string(i);
    auto s = cuckoo_->Reserve(*ctx_, test_key, 1000, bucket_sizes[i], 500, 2);
    ASSERT_TRUE(s.ok()) << "Failed for bucket size " << static_cast<int>(bucket_sizes[i]);
  }
}

TEST_F(RedisCuckooFilterTest, OptimalNumBucketsCalculation) {
  // Test the static helper function
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;

  uint32_t num_buckets = redis::CuckooFilter::OptimalNumBuckets(capacity, bucket_size);

  // Should be a power of 2
  ASSERT_EQ(num_buckets & (num_buckets - 1), 0) << "Number of buckets should be power of 2";

  // Should be able to hold the capacity with 95.5% load factor
  uint32_t expected_min = static_cast<uint32_t>(capacity / bucket_size / 0.955);
  ASSERT_GE(num_buckets, expected_min) << "Number of buckets too small for capacity";
}

TEST_F(RedisCuckooFilterTest, FingerprintGeneration) {
  // Test fingerprint generation ensures non-zero values in range [1, 255]
  // Following RedisBloom: fp = hash % 255 + 1
  for (uint64_t hash = 0; hash < 1000; ++hash) {
    uint8_t fp = redis::CuckooFilter::GenerateFingerprint(hash);
    ASSERT_GE(fp, 1) << "Fingerprint should be at least 1";
    ASSERT_LE(fp, 255) << "Fingerprint should be at most 255";
  }

  // Verify the formula: fp = hash % 255 + 1
  ASSERT_EQ(redis::CuckooFilter::GenerateFingerprint(0), 1);
  ASSERT_EQ(redis::CuckooFilter::GenerateFingerprint(254), 255);
  ASSERT_EQ(redis::CuckooFilter::GenerateFingerprint(255), 1);
  ASSERT_EQ(redis::CuckooFilter::GenerateFingerprint(256), 2);
}

TEST_F(RedisCuckooFilterTest, AlternateBucketCalculation) {
  uint32_t num_buckets = 1024;

  // Test GetAltHash symmetry at hash level (following RedisBloom design)
  // h2 = GetAltHash(fp, h1)
  // h1 = GetAltHash(fp, h2)  <- this is the symmetry property
  for (uint64_t hash = 0; hash < 100; ++hash) {
    for (uint8_t fp = 1; fp < 10; ++fp) {
      uint64_t alt_hash = redis::CuckooFilter::GetAltHash(fp, hash);

      // Applying GetAltHash twice should return original hash
      uint64_t double_alt_hash = redis::CuckooFilter::GetAltHash(fp, alt_hash);
      ASSERT_EQ(double_alt_hash, hash) << "Double alternate hash should give original hash";

      // Both hashes should map to valid bucket indices
      uint32_t bucket1 = hash % num_buckets;
      uint32_t bucket2 = alt_hash % num_buckets;
      ASSERT_LT(bucket1, num_buckets) << "Bucket 1 out of range";
      ASSERT_LT(bucket2, num_buckets) << "Bucket 2 out of range";
    }
  }
}

TEST_F(RedisCuckooFilterTest, HashFunction) {
  // Test that Hash function produces consistent 64-bit values
  std::string test_item = "hello";
  uint64_t hash1 = redis::CuckooFilter::Hash(test_item);
  uint64_t hash2 = redis::CuckooFilter::Hash(test_item.data(), test_item.size());

  // Both methods should produce the same result
  ASSERT_EQ(hash1, hash2) << "Hash methods should be consistent";

  // Hash should be deterministic
  uint64_t hash3 = redis::CuckooFilter::Hash(test_item);
  ASSERT_EQ(hash1, hash3) << "Hash should be deterministic";

  // Different items should produce different hashes (with high probability)
  uint64_t hash_world = redis::CuckooFilter::Hash("world");
  ASSERT_NE(hash1, hash_world) << "Different items should have different hashes";

  // Empty string produces hash value 0 (this is expected with MurmurHash)
  uint64_t hash_empty = redis::CuckooFilter::Hash("");
  ASSERT_EQ(hash_empty, 0) << "Empty string should produce hash value 0 with MurmurHash";

  // Even with hash=0, fingerprint should be non-zero
  uint8_t fp_empty = redis::CuckooFilter::GenerateFingerprint(hash_empty);
  ASSERT_EQ(fp_empty, 1) << "Fingerprint of hash=0 should be 1 (0 % 255 + 1)";

  // Test that hash can be used with fingerprint generation
  uint8_t fp = redis::CuckooFilter::GenerateFingerprint(hash1);
  ASSERT_GE(fp, 1) << "Fingerprint should be at least 1";
  ASSERT_LE(fp, 255) << "Fingerprint should be at most 255";
}