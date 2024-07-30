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
#include "types/redis_hyperloglog.h"

class RedisHyperLogLogTest : public TestBase {
 protected:
  explicit RedisHyperLogLogTest() : TestBase() {
    hll_ = std::make_unique<redis::HyperLogLog>(storage_.get(), "hll_ns");
  }
  ~RedisHyperLogLogTest() override = default;

  std::unique_ptr<redis::HyperLogLog> hll_;

  static std::vector<uint64_t> computeHashes(const std::vector<std::string_view> &elements) {
    std::vector<uint64_t> hashes;
    hashes.reserve(elements.size());
    for (const auto &element : elements) {
      hashes.push_back(redis::HyperLogLog::HllHash(element));
    }
    return hashes;
  }
};

TEST_F(RedisHyperLogLogTest, PFADD) {
  uint64_t ret = 0;
  ASSERT_TRUE(hll_->Add("hll", {}, &ret).ok() && ret == 0);
  // Approximated cardinality after creation is zero
  ASSERT_TRUE(hll_->Count("hll", &ret).ok() && ret == 0);
  // PFADD returns 1 when at least 1 reg was modified
  ASSERT_TRUE(hll_->Add("hll", computeHashes({"a", "b", "c"}), &ret).ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(hll_->Count("hll", &ret).ok());
  ASSERT_EQ(3, ret);
  // PFADD returns 0 when no reg was modified
  ASSERT_TRUE(hll_->Add("hll", computeHashes({"a", "b", "c"}), &ret).ok() && ret == 0);
  // PFADD works with empty string
  ASSERT_TRUE(hll_->Add("hll", computeHashes({""}), &ret).ok() && ret == 1);
  // PFADD works with similiar hash, which is likely to be in the same bucket
  ASSERT_TRUE(hll_->Add("hll", {1, 2, 3, 2, 1}, &ret).ok() && ret == 1);
  ASSERT_TRUE(hll_->Count("hll", &ret).ok());
  ASSERT_EQ(7, ret);
}

TEST_F(RedisHyperLogLogTest, PFCOUNT_returns_approximated_cardinality_of_set) {
  uint64_t ret = 0;
  // pf add "1" to "5"
  ASSERT_TRUE(hll_->Add("hll", computeHashes({"1", "2", "3", "4", "5"}), &ret).ok() && ret == 1);
  // pf count is 5
  ASSERT_TRUE(hll_->Count("hll", &ret).ok() && ret == 5);
  // pf add "6" to "10"
  ASSERT_TRUE(hll_->Add("hll", computeHashes({"6", "7", "8", "8", "9", "10"}), &ret).ok() && ret == 1);
  // pf count is 10
  ASSERT_TRUE(hll_->Count("hll", &ret).ok() && ret == 10);
}
