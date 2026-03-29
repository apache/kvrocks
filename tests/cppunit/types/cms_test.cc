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
#include <vector>

#include "test_base.h"
#include "types/redis_cms.h"

class RedisCMSTest : public TestBase {
 protected:
  explicit RedisCMSTest() : TestBase() {
    cms_ = std::make_unique<redis::CMS>(storage_.get(), "cms_ns");
  }
  ~RedisCMSTest() override = default;

  void SetUp() override {
    TestBase::SetUp();
    [[maybe_unused]] auto s = cms_->Del(*ctx_, "cms");
    for (int x = 1; x <= 3; x++) {
      s = cms_->Del(*ctx_, "cms" + std::to_string(x));
    }
  }

  void TearDown() override {
    TestBase::TearDown();
    [[maybe_unused]] auto s = cms_->Del(*ctx_, "cms");
    for (int x = 1; x <= 3; x++) {
      s = cms_->Del(*ctx_, "cms" + std::to_string(x));
    }
  }

  std::unique_ptr<redis::CMS> cms_;
};

TEST_F(RedisCMSTest, InitByDim) {
  // Test basic initialization
  auto s = cms_->InitByDim(*ctx_, "cms", 1000, 5);
  ASSERT_TRUE(s.ok());

  // Test get info
  redis::CMSInfo info;
  s = cms_->Info(*ctx_, "cms", &info);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(1000, info.width);
  EXPECT_EQ(5, info.depth);
  EXPECT_EQ(0, info.total_count);
  EXPECT_EQ(5000, info.size);

  // Test duplicate key
  s = cms_->InitByDim(*ctx_, "cms", 100, 3);
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST_F(RedisCMSTest, InitByProb) {
  // Test initialization with error rate and probability
  // Per Redis documentation, 'probability' is the failure probability (probability of inflated count)
  // For a 1% failure rate, set probability = 0.01
  auto s = cms_->InitByProb(*ctx_, "cms", 0.01, 0.01);
  ASSERT_TRUE(s.ok());

  redis::CMSInfo info;
  s = cms_->Info(*ctx_, "cms", &info);
  ASSERT_TRUE(s.ok());
  // RedisBloom formula:
  //   width = ceil(2 / error_rate) = ceil(2 / 0.01) = 200
  //   depth = ceil(log10(probability) / log10(0.5)) = ceil(log10(0.01) / log10(0.5)) = 7
  EXPECT_EQ(200, info.width);
  EXPECT_EQ(7, info.depth);
}

TEST_F(RedisCMSTest, IncrBy) {
  // Initialize CMS
  auto s = cms_->InitByDim(*ctx_, "cms", 100, 5);
  ASSERT_TRUE(s.ok());

  // Test single item increment
  std::vector<uint64_t> counts;
  std::vector<std::pair<std::string, int64_t>> items = {{"foo", 10}};
  s = cms_->IncrBy(*ctx_, "cms", items, &counts);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, counts.size());
  EXPECT_EQ(10, counts[0]);

  // Test multiple items
  items = {{"foo", 5}, {"bar", 20}};
  s = cms_->IncrBy(*ctx_, "cms", items, &counts);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(2, counts.size());
  EXPECT_EQ(15, counts[0]);  // 10 + 5
  EXPECT_EQ(20, counts[1]);

  // Check total count
  redis::CMSInfo info;
  s = cms_->Info(*ctx_, "cms", &info);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(35, info.total_count);  // 10 + 5 + 20
}

TEST_F(RedisCMSTest, Query) {
  // Initialize CMS
  auto s = cms_->InitByDim(*ctx_, "cms", 100, 5);
  ASSERT_TRUE(s.ok());

  // Query non-existent item
  std::vector<uint64_t> counts;
  std::vector<std::string> items = {"foo"};
  s = cms_->Query(*ctx_, "cms", items, &counts);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, counts.size());
  EXPECT_EQ(0, counts[0]);

  // Increment and query
  std::vector<std::pair<std::string, int64_t>> incr_items = {{"foo", 10}, {"bar", 20}};
  s = cms_->IncrBy(*ctx_, "cms", incr_items, &counts);
  ASSERT_TRUE(s.ok());

  // Query multiple items
  items = {"foo", "bar", "baz"};
  s = cms_->Query(*ctx_, "cms", items, &counts);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, counts.size());
  EXPECT_EQ(10, counts[0]);
  EXPECT_EQ(20, counts[1]);
  EXPECT_EQ(0, counts[2]);  // baz was never incremented
}

TEST_F(RedisCMSTest, Merge) {
  // Initialize three CMS with same dimensions (destination must be initialized)
  auto s = cms_->InitByDim(*ctx_, "cms1", 100, 5);
  ASSERT_TRUE(s.ok());
  s = cms_->InitByDim(*ctx_, "cms2", 100, 5);
  ASSERT_TRUE(s.ok());
  s = cms_->InitByDim(*ctx_, "cms_merge", 100, 5);
  ASSERT_TRUE(s.ok());

  // Add data to both
  std::vector<uint64_t> counts;
  std::vector<std::pair<std::string, int64_t>> items1 = {{"item1", 100}};
  s = cms_->IncrBy(*ctx_, "cms1", items1, &counts);
  ASSERT_TRUE(s.ok());

  std::vector<std::pair<std::string, int64_t>> items2 = {{"item1", 50}};
  s = cms_->IncrBy(*ctx_, "cms2", items2, &counts);
  ASSERT_TRUE(s.ok());

  // Merge without weights (SUM)
  s = cms_->Merge(*ctx_, "cms_merge", {"cms1", "cms2"}, {}, redis::CMSMergeMethod::SUM);
  ASSERT_TRUE(s.ok());

  // Query merged CMS
  std::vector<std::string> query_items = {"item1"};
  s = cms_->Query(*ctx_, "cms_merge", query_items, &counts);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(150, counts[0]);  // 100 + 50
}

TEST_F(RedisCMSTest, MergeWithWeights) {
  // Initialize three CMS with same dimensions (destination must be initialized)
  auto s = cms_->InitByDim(*ctx_, "cms1", 100, 5);
  ASSERT_TRUE(s.ok());
  s = cms_->InitByDim(*ctx_, "cms2", 100, 5);
  ASSERT_TRUE(s.ok());
  s = cms_->InitByDim(*ctx_, "cms_merge", 100, 5);
  ASSERT_TRUE(s.ok());

  // Add data
  std::vector<uint64_t> counts;
  std::vector<std::pair<std::string, int64_t>> items1 = {{"item1", 100}};
  s = cms_->IncrBy(*ctx_, "cms1", items1, &counts);
  ASSERT_TRUE(s.ok());

  std::vector<std::pair<std::string, int64_t>> items2 = {{"item1", 50}};
  s = cms_->IncrBy(*ctx_, "cms2", items2, &counts);
  ASSERT_TRUE(s.ok());

  // Merge with weights
  s = cms_->Merge(*ctx_, "cms_merge", {"cms1", "cms2"}, {2, 3}, redis::CMSMergeMethod::SUM);
  ASSERT_TRUE(s.ok());

  // Query merged CMS
  std::vector<std::string> query_items = {"item1"};
  s = cms_->Query(*ctx_, "cms_merge", query_items, &counts);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(350, counts[0]);  // 100 * 2 + 50 * 3 = 350
}

TEST_F(RedisCMSTest, MergeInvalidDimensions) {
  // Initialize CMS with different dimensions
  auto s = cms_->InitByDim(*ctx_, "cms1", 100, 5);
  ASSERT_TRUE(s.ok());
  s = cms_->InitByDim(*ctx_, "cms2", 200, 5);  // Different width
  ASSERT_TRUE(s.ok());

  // Merge should fail
  s = cms_->Merge(*ctx_, "cms_merge", {"cms1", "cms2"}, {}, redis::CMSMergeMethod::SUM);
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST_F(RedisCMSTest, NonExistentKey) {
  // Query non-existent key
  std::vector<uint64_t> counts;
  std::vector<std::string> items = {"foo"};
  auto s = cms_->Query(*ctx_, "nonexistent", items, &counts);
  EXPECT_TRUE(s.IsNotFound());

  // IncrBy non-existent key
  std::vector<std::pair<std::string, int64_t>> incr_items = {{"foo", 10}};
  s = cms_->IncrBy(*ctx_, "nonexistent", incr_items, &counts);
  EXPECT_TRUE(s.IsNotFound());

  // Info non-existent key
  redis::CMSInfo info;
  s = cms_->Info(*ctx_, "nonexistent", &info);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(RedisCMSTest, AccuracyTest) {
  // Test CMS accuracy with known values
  const uint32_t width = 1000;
  const uint32_t depth = 10;
  auto s = cms_->InitByDim(*ctx_, "cms", width, depth);
  ASSERT_TRUE(s.ok());

  // Add items with known counts
  std::vector<uint64_t> counts;
  for (int i = 0; i < 100; i++) {
    std::vector<std::pair<std::string, int64_t>> items = {{"item" + std::to_string(i), i + 1}};
    s = cms_->IncrBy(*ctx_, "cms", items, &counts);
    ASSERT_TRUE(s.ok());
  }

  // Query and verify accuracy (CMS may overestimate but never underestimate)
  for (int i = 0; i < 100; i++) {
    std::vector<std::string> items = {"item" + std::to_string(i)};
    s = cms_->Query(*ctx_, "cms", items, &counts);
    ASSERT_TRUE(s.ok());
    // CMS should return count >= actual count (never underestimate)
    EXPECT_GE(counts[0], static_cast<uint64_t>(i + 1));
  }
}