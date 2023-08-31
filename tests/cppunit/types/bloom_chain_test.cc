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
#include "types/redis_bloom_chain.h"

class RedisBloomChainTest : public TestBase {
 protected:
  explicit RedisBloomChainTest() { sb_chain_ = std::make_unique<redis::BloomChain>(storage_, "sb_chain_ns"); }
  ~RedisBloomChainTest() override = default;

  void SetUp() override { key_ = "test_sb_chain_key"; }
  void TearDown() override {}

  std::unique_ptr<redis::BloomChain> sb_chain_;
};

TEST_F(RedisBloomChainTest, Reserve) {
  uint32_t capacity = 1000;
  double error_rate = 0.02;
  uint16_t expansion = 0;

  auto s = sb_chain_->Reserve(key_, capacity, error_rate, expansion);
  EXPECT_TRUE(s.ok());

  // return false because the key is already exists;
  s = sb_chain_->Reserve(key_, capacity, error_rate, expansion);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "Invalid argument: the key already exists");

  sb_chain_->Del(key_);
}

TEST_F(RedisBloomChainTest, BasicAddAndTest) {
  int ret = 0;

  auto s = sb_chain_->Exist("no_exist_key", "test_item", &ret);
  EXPECT_EQ(ret, 0);
  sb_chain_->Del("no_exist_key");

  std::string insert_items[] = {"item1", "item2", "item3", "item101", "item202", "303"};
  for (const auto& insert_item : insert_items) {
    s = sb_chain_->Add(key_, insert_item, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 1);
  }

  for (const auto& insert_item : insert_items) {
    s = sb_chain_->Exist(key_, insert_item, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 1);
  }

  std::string no_insert_items[] = {"item303", "item404", "1", "2", "3"};
  for (const auto& no_insert_item : no_insert_items) {
    s = sb_chain_->Exist(key_, no_insert_item, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 0);
  }
  sb_chain_->Del(key_);
}
