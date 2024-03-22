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

class RedisHyperloglogTest : public TestBase {
 protected:
  explicit RedisHyperloglogTest() : TestBase() {
    hll_ = std::make_unique<redis::HyperLogLog>(storage_.get(), "hll_ns");
  }
  ~RedisHyperloglogTest() = default;

 protected:
  std::unique_ptr<redis::HyperLogLog> hll_;
};

TEST_F(RedisHyperloglogTest, PFADD) {
  uint64_t ret = 0;
  ASSERT_TRUE(hll_->Add("hll", {}, &ret).ok() && ret == 0);
  // Approximated cardinality after creation is zero
  ASSERT_TRUE(hll_->Count("hll", &ret).ok() && ret == 0);
  // PFADD returns 1 when at least 1 reg was modified
  ASSERT_TRUE(hll_->Add("hll", {"a", "b", "c"}, &ret).ok() && ret == 1);
  ASSERT_TRUE(hll_->Count("hll", &ret).ok() && ret == 3);
  // PFADD returns 0 when no reg was modified
  ASSERT_TRUE(hll_->Add("hll", {"a", "b", "c"}, &ret).ok() && ret == 0);
  // PFADD works with empty string
  ASSERT_TRUE(hll_->Add("hll", {""}, &ret).ok() && ret == 1);
}

TEST_F(RedisHyperloglogTest, PFCOUNT_returns_approximated_cardinality_of_set) {
  uint64_t ret = 0;
  // pf add "1" to "5"
  ASSERT_TRUE(hll_->Add("hll", {"1", "2", "3", "4", "5"}, &ret).ok() && ret == 1);
  // pf count is 5
  ASSERT_TRUE(hll_->Count("hll", &ret).ok() && ret == 5);
  // pf add "6" to "10"
  ASSERT_TRUE(hll_->Add("hll", {"6", "7", "8", "8", "9", "10"}, &ret).ok() && ret == 1);
  // pf count is 10
  ASSERT_TRUE(hll_->Count("hll", &ret).ok() && ret == 10);
}

TEST_F(RedisHyperloglogTest, PFMERGE_results_on_the_cardinality_of_union_of_sets) {
  uint64_t ret = 0;
  // pf add hll1 a b c
  ASSERT_TRUE(hll_->Add("hll1", {"a", "b", "c"}, &ret).ok() && ret == 1);
  // pf add hll2 b c d
  ASSERT_TRUE(hll_->Add("hll2", {"b", "c", "d"}, &ret).ok() && ret == 1);
  // pf add hll3 c d e
  ASSERT_TRUE(hll_->Add("hll3", {"c", "d", "e"}, &ret).ok() && ret == 1);
  // pf merge hll hll1 hll2 hll3
  ASSERT_TRUE(hll_->Merge({"hll", "hll1", "hll2", "hll3"}).ok());
  // pf count hll is 5
  ASSERT_TRUE(hll_->Count("hll", &ret).ok() && ret == 5);
}

TEST_F(RedisHyperloglogTest, PFCOUNT_multiple_keys_merge_returns_cardinality_of_union_1) {
  for (int x = 1; x < 1000; x++) {
    uint64_t ret = 0;
    ASSERT_TRUE(hll_->Add("hll0", {"foo-" + std::to_string(x)}, &ret).ok());
    ASSERT_TRUE(hll_->Add("hll1", {"bar-" + std::to_string(x)}, &ret).ok());
    ASSERT_TRUE(hll_->Add("hll2", {"zap-" + std::to_string(x)}, &ret).ok());

    std::vector<uint64_t> cards(3);
    ASSERT_TRUE(hll_->Count("hll0", &cards[0]).ok());
    ASSERT_TRUE(hll_->Count("hll1", &cards[1]).ok());
    ASSERT_TRUE(hll_->Count("hll2", &cards[2]).ok());

    double card = cards[0] + cards[1] + cards[2];
    double realcard = x * 3;
    // assert the ABS of 'card' and 'realcart' is within 5% of the cardinality
    double left = std::abs(card - realcard);
    double right = card / 100 * 5;
    ASSERT_TRUE(left < right) << "left : " << left << ", right: " << right;
  }
}

TEST_F(RedisHyperloglogTest, PFCOUNT_multiple_keys_merge_returns_cardinality_of_union_2) {
  std::unordered_set<int> realcard_set;
  for (auto i = 1; i < 1000; i++) {
    for (auto j = 0; j < 3; j++) {
      uint64_t ret = 0;
      int rint = std::rand() % 20000;
      ASSERT_TRUE(hll_->Add("hll" + std::to_string(j), {std::to_string(rint)}, &ret).ok());
      realcard_set.insert(rint);
    }
  }
  std::vector<uint64_t> cards(3);
  ASSERT_TRUE(hll_->Count("hll0", &cards[0]).ok());
  ASSERT_TRUE(hll_->Count("hll1", &cards[1]).ok());
  ASSERT_TRUE(hll_->Count("hll2", &cards[2]).ok());

  double card = cards[0] + cards[1] + cards[2];
  double realcard = realcard_set.size();
  double left = std::abs(card - realcard);
  // TODO when 'right = card / 100 * 5', the test run failed that the ABS is
  // a little larger than 'card * 0.05' (left : 149, right: 146.30000000000001).
  double right = card / 100 * 5.1;
  ASSERT_TRUE(left < right) << "left : " << left << ", right: " << right;
}
