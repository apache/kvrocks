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

#include "test_base.h"
#include "types/redis_topk.h"

static constexpr uint32_t k = 5;
static constexpr uint32_t width = 7;
static constexpr uint32_t depth = 8;
static constexpr double decay = 0.9;

class RedisTopKTest : public TestBase {
 protected:
  explicit RedisTopKTest() : TestBase() {
    top_k_ = std::make_unique<redis::TopK>(storage_.get(), "topk_ns");
  }
  ~RedisTopKTest() override = default;

  void SetUp() override {
    key_ = "test_topk->key";
    top_k_->Reserve(*ctx_, key_, k, width, depth, decay);
  }

  void TearDown() override {}

  std::unique_ptr<redis::TopK> top_k_;
};

TEST_F(RedisTopKTest, TestTopKInfo) {
  // test exist key
  redis::TopKInfo info1;
  top_k_->Info(*ctx_, key_, &info1);
  ASSERT_EQ(info1.k, k);
  ASSERT_EQ(info1.width, width);
  ASSERT_EQ(info1.depth, depth);
  ASSERT_EQ(info1.decay, decay);

  // test not exist key
  redis::TopKInfo info2;
  auto s = top_k_->Info(*ctx_, "not_exist_key", &info2);
  ASSERT_FALSE(s.ok());
}

TEST_F(RedisTopKTest, TestTopKAddAndQuery) {
  // test not exist key
  std::string no_exist_key = "no_exist_key";
  auto s = top_k_->Add(*ctx_, no_exist_key, "1");
  ASSERT_FALSE(s.ok());

  bool exist;
  s = top_k_->Query(*ctx_, no_exist_key, "1", &exist);
  ASSERT_FALSE(s.ok());

  std::vector<std::string> list;
  s = top_k_->List(*ctx_, no_exist_key, list);
  ASSERT_FALSE(s.ok());

  // test exist key
  std::vector<std::string> values1 = {"1", "2", "3", "4", "5"};
  std::vector<std::string> values2 = {"6", "7", "8", "9", "10"};
  std::unordered_set<std::string> values_set1(values1.begin(), values1.end());
  std::unordered_set<std::string> values_set2(values2.begin(), values2.end());

  // found not exist values1
  for (size_t i = 0; i < values1.size(); ++i) {
    bool found = true;
    top_k_->Query(*ctx_, key_, values1[i], &found);
    ASSERT_FALSE(found);
  }
  // add values1, and query values1.
  for (size_t i = 0; i < values1.size(); ++i) {
    top_k_->Add(*ctx_, key_, values1[i]);
    bool found = false;
    top_k_->Query(*ctx_, key_, values1[i], &found);
    ASSERT_TRUE(found);
  }
  for (size_t i = 0; i < values1.size(); ++i) {
    bool found = false;
    top_k_->Query(*ctx_, key_, values1[i], &found);
    ASSERT_TRUE(found);
  }

  // found topk list.
  std::vector<std::string> top_k_list;
  top_k_->List(*ctx_, key_, top_k_list);
  ASSERT_EQ(top_k_list.size(), values1.size());
  for (size_t i = 0; i < k; ++i) {
    ASSERT_TRUE(values_set1.find(top_k_list[i]) != values_set1.end());
  }

  // heap is full, need remove values1.
  for (size_t i = 0; i < values2.size(); ++i) {
    bool found = false;
    // due to decay, topk is possiable to remove values1.
    while (!found) {
      top_k_->Add(*ctx_, key_, values2[i]);
      top_k_->Query(*ctx_, key_, values2[i], &found);
    }
    top_k_->Add(*ctx_, key_, values2[i]);
  }

  // values1 is removed.
  for (size_t i = 0; i < values1.size(); ++i) {
    bool found = true;
    top_k_->Query(*ctx_, key_, values1[i], &found);
    ASSERT_FALSE(found);
  }

  // found topk list.
  top_k_list.clear();
  top_k_->List(*ctx_, key_, top_k_list);
  for (size_t i = 0; i < top_k_list.size(); ++i) {
    ASSERT_TRUE(values_set2.find(top_k_list[i]) != values_set2.end());
  }
}