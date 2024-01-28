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
#include "types/redis_set.h"

class RedisSetTest : public TestBase {
 protected:
  explicit RedisSetTest() { set_ = std::make_unique<redis::Set>(storage_.get(), "set_ns"); }
  ~RedisSetTest() override = default;

  void SetUp() override {
    key_ = "test-set-key";
    fields_ = {"set-key-1", "set-key-2", "set-key-3", "set-key-4"};
  }

  std::unique_ptr<redis::Set> set_;
};

TEST_F(RedisSetTest, AddAndRemove) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  s = set_->Del(key_);
}

TEST_F(RedisSetTest, AddAndRemoveRepeated) {
  std::vector<rocksdb::Slice> allmembers{"m1", "m1", "m2", "m3"};
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(key_, allmembers, &ret);
  EXPECT_TRUE(s.ok() && (allmembers.size() - 1) == ret);
  uint64_t card = 0;
  set_->Card(key_, &card);
  EXPECT_EQ(card, allmembers.size() - 1);

  std::vector<rocksdb::Slice> remembers{"m1", "m2", "m2"};
  s = set_->Remove(key_, remembers, &ret);
  EXPECT_TRUE(s.ok() && (remembers.size() - 1) == ret);
  set_->Card(key_, &card);
  EXPECT_EQ(card, allmembers.size() - 1 - ret);

  s = set_->Del(key_);
}

TEST_F(RedisSetTest, Members) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  std::vector<std::string> members;
  s = set_->Members(key_, &members);
  EXPECT_TRUE(s.ok() && fields_.size() == members.size());
  // Note: the members was fetched by iterator, so the order should be asec
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(fields_[i], members[i]);
  }
  s = set_->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Del(key_);
}

TEST_F(RedisSetTest, IsMember) {
  uint64_t ret = 0;
  bool flag = false;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  for (auto &field : fields_) {
    s = set_->IsMember(key_, field, &flag);
    EXPECT_TRUE(s.ok() && flag);
  }
  set_->IsMember(key_, "foo", &flag);
  EXPECT_TRUE(s.ok() && !flag);
  s = set_->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Del(key_);
}

TEST_F(RedisSetTest, MIsMember) {
  uint64_t ret = 0;
  std::vector<int> exists;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->MIsMember(key_, fields_, &exists);
  EXPECT_TRUE(s.ok());
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_TRUE(exists[i] == 1);
  }
  s = set_->Remove(key_, {fields_[0]}, &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  s = set_->MIsMember(key_, fields_, &exists);
  EXPECT_TRUE(s.ok() && exists[0] == 0);
  for (size_t i = 1; i < fields_.size(); i++) {
    EXPECT_TRUE(exists[i] == 1);
  }
  s = set_->Del(key_);
}

TEST_F(RedisSetTest, Move) {
  uint64_t ret = 0;
  bool flag = false;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  Slice dst("set-test-move-key");
  for (auto &field : fields_) {
    s = set_->Move(key_, dst, field, &flag);
    EXPECT_TRUE(s.ok() && flag);
  }
  s = set_->Move(key_, dst, "set-no-exists-key", &flag);
  EXPECT_TRUE(s.ok() && !flag);
  s = set_->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  s = set_->Card(dst, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Remove(dst, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Del(key_);
  s = set_->Del(dst);
}

TEST_F(RedisSetTest, TakeWithPop) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  std::vector<std::string> members;
  s = set_->Take(key_, &members, 3, true);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), 3);
  s = set_->Take(key_, &members, 2, true);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), 1);
  s = set_->Take(key_, &members, 1, true);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(s.ok() && members.size() == 0);
  s = set_->Del(key_);
}

TEST_F(RedisSetTest, Diff) {
  uint64_t ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3";
  rocksdb::Status s = set_->Add(k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(k2, {"c"}, &ret);
  EXPECT_EQ(ret, 1);
  set_->Add(k3, {"a", "c", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  std::vector<std::string> members;
  set_->Diff({k1, k2, k3}, &members);
  EXPECT_EQ(2, members.size());
  s = set_->Del(k1);
  s = set_->Del(k2);
  s = set_->Del(k3);
}

TEST_F(RedisSetTest, Union) {
  uint64_t ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3";
  rocksdb::Status s = set_->Add(k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(k2, {"c"}, &ret);
  EXPECT_EQ(ret, 1);
  set_->Add(k3, {"a", "c", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  std::vector<std::string> members;
  set_->Union({k1, k2, k3}, &members);
  EXPECT_EQ(5, members.size());
  s = set_->Del(k1);
  s = set_->Del(k2);
  s = set_->Del(k3);
}

TEST_F(RedisSetTest, Inter) {
  uint64_t ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3", k4 = "key4", k5 = "key5";
  rocksdb::Status s = set_->Add(k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(k2, {"c"}, &ret);
  EXPECT_EQ(ret, 1);
  set_->Add(k3, {"a", "c", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  set_->Add(k5, {"a"}, &ret);
  EXPECT_EQ(ret, 1);
  std::vector<std::string> members;
  set_->Inter({k1, k2, k3}, &members);
  EXPECT_EQ(1, members.size());
  members.clear();
  set_->Inter({k1, k2, k4}, &members);
  EXPECT_EQ(0, members.size());
  set_->Inter({k1, k4, k5}, &members);
  EXPECT_EQ(0, members.size());
  s = set_->Del(k1);
  s = set_->Del(k2);
  s = set_->Del(k3);
  s = set_->Del(k4);
  s = set_->Del(k5);
}

TEST_F(RedisSetTest, InterCard) {
  uint64_t ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3", k4 = "key4";
  rocksdb::Status s = set_->Add(k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(k2, {"c", "d", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  set_->Add(k3, {"e", "f"}, &ret);
  EXPECT_EQ(ret, 2);
  set_->InterCard({k1, k2}, 0, &ret);
  EXPECT_EQ(ret, 2);
  set_->InterCard({k1, k2}, 1, &ret);
  EXPECT_EQ(ret, 1);
  set_->InterCard({k1, k2}, 3, &ret);
  EXPECT_EQ(ret, 2);
  set_->InterCard({k2, k3}, 1, &ret);
  EXPECT_EQ(ret, 1);
  set_->InterCard({k1, k3}, 5, &ret);
  EXPECT_EQ(ret, 0);
  set_->InterCard({k1, k4}, 5, &ret);
  EXPECT_EQ(ret, 0);
  set_->InterCard({k1}, 0, &ret);
  EXPECT_EQ(ret, 4);
  for (uint32_t i = 1; i < 20; i++) {
    set_->InterCard({k1}, i, &ret);
    uint64_t val = (i >= 4) ? 4 : i;
    EXPECT_EQ(ret, val);
  }
  s = set_->Del(k1);
  s = set_->Del(k2);
  s = set_->Del(k3);
  s = set_->Del(k4);
}

TEST_F(RedisSetTest, Overwrite) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  set_->Overwrite(key_, {"a"});
  set_->Card(key_, &ret);
  EXPECT_EQ(ret, 1);
  s = set_->Del(key_);
}

TEST_F(RedisSetTest, TakeWithoutPop) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  std::vector<std::string> members;
  s = set_->Take(key_, &members, int(fields_.size() + 1), false);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), fields_.size());
  s = set_->Take(key_, &members, int(fields_.size() - 1), false);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), fields_.size() - 1);
  s = set_->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Del(key_);
}
