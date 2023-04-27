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
  explicit RedisSetTest() { set_ = std::make_unique<redis::Set>(storage_, "set_ns"); }
  ~RedisSetTest() override = default;

  void SetUp() override {
    key_ = "test-set-key";
    fields_ = {"set-key-1", "set-key-2", "set-key-3", "set-key-4"};
  }

  std::unique_ptr<redis::Set> set_;
};

TEST_F(RedisSetTest, AddAndRemove) {
  int ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  s = set_->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  s = set_->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  s = set_->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  set_->Del(key_);
}

TEST_F(RedisSetTest, Members) {
  int ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  std::vector<std::string> members;
  s = set_->Members(key_, &members);
  EXPECT_TRUE(s.ok() && fields_.size() == members.size());
  // Note: the members was fetched by iterator, so the order should be asec
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(fields_[i], members[i]);
  }
  s = set_->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  set_->Del(key_);
}

TEST_F(RedisSetTest, IsMember) {
  int ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  for (auto &field : fields_) {
    s = set_->IsMember(key_, field, &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  set_->IsMember(key_, "foo", &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  s = set_->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  set_->Del(key_);
}

TEST_F(RedisSetTest, MIsMember) {
  int ret = 0;
  std::vector<int> exists;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
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
  set_->Del(key_);
}

TEST_F(RedisSetTest, Move) {
  int ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  Slice dst("set-test-move-key");
  for (auto &field : fields_) {
    s = set_->Move(key_, dst, field, &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  s = set_->Move(key_, dst, "set-no-exists-key", &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  s = set_->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  s = set_->Card(dst, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  s = set_->Remove(dst, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  set_->Del(key_);
  set_->Del(dst);
}

TEST_F(RedisSetTest, TakeWithPop) {
  int ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
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
  set_->Del(key_);
}

TEST_F(RedisSetTest, Diff) {
  int ret = 0;
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
  set_->Del(k1);
  set_->Del(k2);
  set_->Del(k3);
}

TEST_F(RedisSetTest, Union) {
  int ret = 0;
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
  set_->Del(k1);
  set_->Del(k2);
  set_->Del(k3);
}

TEST_F(RedisSetTest, Inter) {
  int ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3";
  rocksdb::Status s = set_->Add(k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(k2, {"c"}, &ret);
  EXPECT_EQ(ret, 1);
  set_->Add(k3, {"a", "c", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  std::vector<std::string> members;
  set_->Inter({k1, k2, k3}, &members);
  EXPECT_EQ(1, members.size());
  set_->Del(k1);
  set_->Del(k2);
  set_->Del(k3);
}

TEST_F(RedisSetTest, Overwrite) {
  int ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  set_->Overwrite(key_, {"a"});
  int count = 0;
  set_->Card(key_, &count);
  EXPECT_EQ(count, 1);
  set_->Del(key_);
}

TEST_F(RedisSetTest, TakeWithoutPop) {
  int ret = 0;
  rocksdb::Status s = set_->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  std::vector<std::string> members;
  s = set_->Take(key_, &members, int(fields_.size() + 1), false);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), fields_.size());
  s = set_->Take(key_, &members, int(fields_.size() - 1), false);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), fields_.size() - 1);
  s = set_->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  set_->Del(key_);
}
