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
  ~RedisCuckooFilterTest() override {
    // Ensure cuckoo_ is destroyed before storage_
    cuckoo_.reset();
  }

  void SetUp() override {
    // Use a unique key for each test to avoid conflicts
    // Include test name to make debugging easier
    const ::testing::TestInfo* const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    key_ = std::string("cf_test_") + test_info->name();
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


TEST_F(RedisCuckooFilterTest, ReserveTooSmallCapacity) {
  // Test capacity = 1 (too small, following RedisBloom behavior)
  // With load factor 0.955 and bucket_size=4, this would result in 0 buckets
  auto s = cuckoo_->Reserve(*ctx_, key_, 1, 4, 500, 2);
  ASSERT_FALSE(s.ok()) << "Should reject capacity = 1";
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_NE(s.ToString().find("at least 2"), std::string::npos) << "Error message should mention minimum capacity";
}

TEST_F(RedisCuckooFilterTest, ReserveBucketSizeBoundary) {
  // Test valid upper boundary (bucket_size is uint8_t, max = 255)
  auto s = cuckoo_->Reserve(*ctx_, key_, 1000, 255, 500, 2);
  ASSERT_TRUE(s.ok()) << "bucket_size=255 should be valid";

  // Test bucket_size = 1 (minimum valid value)
  s = cuckoo_->Reserve(*ctx_, "key_bs1", 1000, 1, 500, 2);
  ASSERT_TRUE(s.ok()) << "bucket_size=1 should be valid";

  // Test common power-of-2 bucket sizes
  std::vector<uint8_t> valid_sizes = {2, 4, 8, 16, 32, 64, 128};
  for (size_t i = 0; i < valid_sizes.size(); ++i) {
    std::string test_key = "key_bs_" + std::to_string(valid_sizes[i]);
    s = cuckoo_->Reserve(*ctx_, test_key, 1000, valid_sizes[i], 500, 2);
    ASSERT_TRUE(s.ok()) << "bucket_size=" << static_cast<int>(valid_sizes[i]) << " should be valid";
  }
}

TEST_F(RedisCuckooFilterTest, ReserveVerifyMetadata) {
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint8_t expansion = 2;

  // Create the filter
  auto s = cuckoo_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  ASSERT_TRUE(s.ok()) << "First reserve should succeed";

  // Verify metadata was stored by trying to reserve again with same key
  // This should fail with "already exists" error
  s = cuckoo_->Reserve(*ctx_, key_, capacity * 2, bucket_size, max_iterations, expansion);
  ASSERT_FALSE(s.ok()) << "Second reserve with same key should fail";
  ASSERT_TRUE(s.IsInvalidArgument()) << "Should return InvalidArgument error";
  ASSERT_NE(s.ToString().find("already exists"), std::string::npos)
      << "Error message should mention 'already exists'";

  // Verify we can still create filters with different keys
  s = cuckoo_->Reserve(*ctx_, "different_key", capacity, bucket_size, max_iterations, expansion);
  ASSERT_TRUE(s.ok()) << "Should be able to create filter with different key";

  // Verify the original key still exists (can't create it again)
  s = cuckoo_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  ASSERT_FALSE(s.ok()) << "Original key should still exist";
  ASSERT_NE(s.ToString().find("already exists"), std::string::npos);
}


TEST_F(RedisCuckooFilterTest, ReserveNoExpansion) {
  // expansion=0 means no auto-expansion when filter is full
  auto s = cuckoo_->Reserve(*ctx_, key_, 1000, 4, 500, 0);
  ASSERT_TRUE(s.ok()) << "expansion=0 should be valid (no auto-growth)";

  // Verify different expansion values
  std::vector<uint8_t> expansions = {0, 1, 2, 4, 8};
  for (size_t i = 0; i < expansions.size(); ++i) {
    std::string test_key = "key_exp_" + std::to_string(expansions[i]);
    s = cuckoo_->Reserve(*ctx_, test_key, 1000, 4, 500, expansions[i]);
    ASSERT_TRUE(s.ok()) << "expansion=" << static_cast<int>(expansions[i]) << " should be valid";
  }
}

TEST_F(RedisCuckooFilterTest, ReserveLargeCapacity) {
  // Test with very large capacity
  uint64_t large_capacity = 10000000;  // 10 million
  auto s = cuckoo_->Reserve(*ctx_, key_, large_capacity, 4, 500, 2);
  ASSERT_TRUE(s.ok()) << "Should handle large capacity";

  // Verify num_buckets calculation doesn't overflow
  uint32_t num_buckets = redis::CuckooFilter::OptimalNumBuckets(large_capacity, 4);
  ASSERT_GT(num_buckets, 0) << "Should not overflow to 0";
  ASSERT_EQ(num_buckets & (num_buckets - 1), 0) << "Should be power of 2";

  // Test even larger capacity (100 million)
  uint64_t huge_capacity = 100000000;
  s = cuckoo_->Reserve(*ctx_, "huge_key", huge_capacity, 4, 500, 2);
  ASSERT_TRUE(s.ok()) << "Should handle 100M capacity";

  num_buckets = redis::CuckooFilter::OptimalNumBuckets(huge_capacity, 4);
  ASSERT_GT(num_buckets, 0) << "Should not overflow with 100M capacity";
}

TEST_F(RedisCuckooFilterTest, ReserveMaxIterationsBoundary) {
  // Test different max_iterations values
  std::vector<uint16_t> iterations = {1, 10, 100, 500, 1000, 5000, 65535};

  for (size_t i = 0; i < iterations.size(); ++i) {
    std::string test_key = "key_iter_" + std::to_string(iterations[i]);
    auto s = cuckoo_->Reserve(*ctx_, test_key, 1000, 4, iterations[i], 2);
    ASSERT_TRUE(s.ok()) << "max_iterations=" << iterations[i] << " should be valid";
  }

  // Test max_iterations = 0 (should fail)
  auto s = cuckoo_->Reserve(*ctx_, "iter_zero", 1000, 4, 0, 2);
  ASSERT_FALSE(s.ok()) << "max_iterations=0 should fail";
}

TEST_F(RedisCuckooFilterTest, ReserveEdgeCaseCapacities) {
  // Test minimum valid capacity
  auto s = cuckoo_->Reserve(*ctx_, "min_cap", 2, 4, 500, 2);
  ASSERT_TRUE(s.ok()) << "capacity=2 should be valid (minimum)";

  // Test small but valid capacities
  std::vector<uint64_t> small_capacities = {2, 3, 4, 5, 10, 50, 100};
  for (size_t i = 0; i < small_capacities.size(); ++i) {
    std::string test_key = "small_" + std::to_string(small_capacities[i]);
    s = cuckoo_->Reserve(*ctx_, test_key, small_capacities[i], 4, 500, 2);
    ASSERT_TRUE(s.ok()) << "capacity=" << small_capacities[i] << " should be valid";

    // Verify at least one bucket is created
    uint32_t num_buckets = redis::CuckooFilter::OptimalNumBuckets(small_capacities[i], 4);
    ASSERT_GE(num_buckets, 1) << "Should have at least 1 bucket for capacity=" << small_capacities[i];
  }
}

TEST_F(RedisCuckooFilterTest, ReserveParameterCombinations) {
  // Test various parameter combinations to ensure robustness
  struct TestCase {
    uint64_t capacity;
    uint8_t bucket_size;
    uint16_t max_iterations;
    uint8_t expansion;
    bool should_succeed;
    std::string description;
  };

  std::vector<TestCase> test_cases = {
      {1000, 4, 500, 2, true, "Standard parameters"},
      {2, 1, 1, 0, true, "Minimum all parameters"},
      {100000, 255, 65535, 255, true, "Maximum all parameters"},
      {1000, 2, 100, 1, true, "Small bucket, moderate iterations"},
      {50000, 8, 1000, 4, true, "Large capacity, large bucket"},
      {10, 16, 50, 0, true, "Small capacity, large bucket, no expansion"},
  };

  for (size_t i = 0; i < test_cases.size(); ++i) {
    const auto& tc = test_cases[i];
    std::string test_key = "combo_" + std::to_string(i);
    auto s = cuckoo_->Reserve(*ctx_, test_key, tc.capacity, tc.bucket_size, tc.max_iterations, tc.expansion);

    if (tc.should_succeed) {
      ASSERT_TRUE(s.ok()) << "Test case failed: " << tc.description;
    } else {
      ASSERT_FALSE(s.ok()) << "Test case should have failed: " << tc.description;
    }
  }
}