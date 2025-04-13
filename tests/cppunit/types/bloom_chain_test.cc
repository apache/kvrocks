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
#include <random>

#include "test_base.h"
#include "types/redis_bloom_chain.h"

class RedisBloomChainTest : public TestBase {
 protected:
  explicit RedisBloomChainTest() = default;
  ~RedisBloomChainTest() override = default;

  void SetUp() override {
    key_ = "test_sb_chain_key";
    sb_chain_ = std::make_unique<redis::BloomChain>(storage_.get(), "sb_chain_ns");
  }
  void TearDown() override {}

  std::unique_ptr<redis::BloomChain> sb_chain_;
};

TEST_F(RedisBloomChainTest, Reserve) {
  uint32_t capacity = 1000;
  double error_rate = 0.02;
  uint16_t expansion = 0;

  auto s = sb_chain_->Reserve(*ctx_, key_, capacity, error_rate, expansion);
  EXPECT_TRUE(s.ok());

  // return false because the key is already exists;
  s = sb_chain_->Reserve(*ctx_, key_, capacity, error_rate, expansion);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "Invalid argument: the key already exists");
}

TEST_F(RedisBloomChainTest, BasicAddAndTest) {
  redis::BloomFilterAddResult ret = redis::BloomFilterAddResult::kOk;
  bool exist = false;

  auto s = sb_chain_->Exists(*ctx_, "no_exist_key", "test_item", &exist);
  EXPECT_EQ(exist, 0);
  s = sb_chain_->Del(*ctx_, "no_exist_key");

  std::string insert_items[] = {"item1", "item2", "item3", "item101", "item202", "303"};
  for (const auto& insert_item : insert_items) {
    s = sb_chain_->Add(*ctx_, key_, insert_item, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, redis::BloomFilterAddResult::kOk);
  }

  for (const auto& insert_item : insert_items) {
    s = sb_chain_->Exists(*ctx_, key_, insert_item, &exist);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(exist, true);
  }

  std::string no_insert_items[] = {"item303", "item404", "1", "2", "3"};
  for (const auto& no_insert_item : no_insert_items) {
    s = sb_chain_->Exists(*ctx_, key_, no_insert_item, &exist);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(exist, false);
  }
}

TEST_F(RedisBloomChainTest, DuplicateInsert) {
  std::string insert_items[] = {"item1", "item2", "item3", "item101", "item202", "303"};
  for (const auto& insert_item : insert_items) {
    redis::BloomFilterAddResult ret = redis::BloomFilterAddResult::kOk;
    auto s = sb_chain_->Add(*ctx_, key_, insert_item, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, redis::BloomFilterAddResult::kOk);
  }
  std::vector<std::string> arrays;
  for (int64_t idx = 0; idx < 1000; ++idx) {
    arrays.push_back("itemx" + std::to_string(idx));
  }
  std::vector<redis::BloomFilterAddResult> results;
  results.resize(arrays.size());
  auto s = sb_chain_->MAdd(*ctx_, key_, arrays, &results);
  EXPECT_TRUE(s.ok());
  arrays.clear();
  for (int64_t idx = 1000; idx < 2000; ++idx) {
    arrays.push_back("itemx" + std::to_string(idx));
  }
  s = sb_chain_->MAdd(*ctx_, key_, arrays, &results);
  EXPECT_TRUE(s.ok());
  arrays.clear();
  for (int64_t idx = 2000; idx < 3000; ++idx) {
    arrays.push_back("itemx" + std::to_string(idx));
  }
  s = sb_chain_->MAdd(*ctx_, key_, arrays, &results);
  EXPECT_TRUE(s.ok());
}

TEST_F(RedisBloomChainTest, MultiThreadInsert) {
  // Concurrent insert, every task would insert 100 random values
  std::mutex mu;

  std::default_random_engine gen(42);
  std::uniform_int_distribution<uint32_t> dist;
  auto insert_task = [&]() {
    // Generate 100 random keys
    std::vector<std::string> arrays;
    for (size_t idx = 0; idx < 100; ++idx) {
      arrays.push_back("itemx" + std::to_string(dist(gen)));
    }
    engine::Context ctx(storage_.get());
    // Call madd
    std::vector<redis::BloomFilterAddResult> results;
    results.resize(arrays.size());
    auto s = sb_chain_->MAdd(ctx, key_, arrays, &results);
    EXPECT_TRUE(s.ok());
  };

  std::vector<std::thread> threads;
  threads.reserve(10);
  for (int32_t thread_idx = 0; thread_idx < 10; ++thread_idx) {
    threads.emplace_back([&]() {
      std::lock_guard<std::mutex> lock(mu);
      insert_task();
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
