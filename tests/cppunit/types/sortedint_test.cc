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
#include "types/redis_sortedint.h"

class RedisSortedintTest : public TestBase {
 protected:
  explicit RedisSortedintTest() { sortedint = std::make_unique<Redis::Sortedint>(storage_, "sortedint_ns"); }
  ~RedisSortedintTest() override = default;

  void SetUp() override {
    key_ = "test-sortedint-key";
    ids_ = {1, 2, 3, 4};
  }

  std::unique_ptr<Redis::Sortedint> sortedint;
  std::vector<uint64_t> ids_;
};

TEST_F(RedisSortedintTest, AddAndRemove) {
  int ret = 0;
  rocksdb::Status s = sortedint->Add(key_, ids_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
  s = sortedint->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
  s = sortedint->Remove(key_, ids_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
  s = sortedint->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  sortedint->Del(key_);
}

TEST_F(RedisSortedintTest, Range) {
  int ret = 0;
  rocksdb::Status s = sortedint->Add(key_, ids_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
  std::vector<uint64_t> ids;
  s = sortedint->Range(key_, 0, 0, 20, false, &ids);
  EXPECT_TRUE(s.ok() && ids_.size() == ids.size());
  for (size_t i = 0; i < ids_.size(); i++) {
    EXPECT_EQ(ids_[i], ids[i]);
  }
  s = sortedint->Remove(key_, ids_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
  sortedint->Del(key_);
}
