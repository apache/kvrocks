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
#include <rocksdb/perf_context.h>
#include <rocksdb/perf_level.h>

#include <memory>

#include "test_base.h"
#include "types/redis_hyperloglog.h"
#include "types/redis_set.h"
#include "types/redis_string.h"

class RedisSetTest : public TestBase {
 protected:
  explicit RedisSetTest() { set_ = std::make_unique<redis::Set>(storage_.get(), "set_ns"); }
  ~RedisSetTest() override = default;

  void SetUp() override {
    key_ = "test-set-key";
    fields_ = {"set-key-1", "set-key-2", "set-key-3", "set-key-4"};
  }

  void TearDown() override { [[maybe_unused]] auto s = set_->Del(*ctx_, key_); }

  std::unique_ptr<redis::Set> set_;
};

TEST_F(RedisSetTest, AddAndRemove) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Card(*ctx_, key_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Remove(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Card(*ctx_, key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
}

TEST_F(RedisSetTest, AddAndRemoveRepeated) {
  std::vector<rocksdb::Slice> allmembers{"m1", "m1", "m2", "m3"};
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(*ctx_, key_, allmembers, &ret);
  EXPECT_TRUE(s.ok() && (allmembers.size() - 1) == ret);
  uint64_t card = 0;
  set_->Card(*ctx_, key_, &card);
  EXPECT_EQ(card, allmembers.size() - 1);

  std::vector<rocksdb::Slice> remembers{"m1", "m2", "m2"};
  s = set_->Remove(*ctx_, key_, remembers, &ret);
  EXPECT_TRUE(s.ok() && (remembers.size() - 1) == ret);
  set_->Card(*ctx_, key_, &card);
  EXPECT_EQ(card, allmembers.size() - 1 - ret);
}

TEST_F(RedisSetTest, Members) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  std::vector<std::string> members;
  s = set_->Members(*ctx_, key_, &members);
  EXPECT_TRUE(s.ok() && fields_.size() == members.size());
  // Note: the members was fetched by iterator, so the order should be asec
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(fields_[i], members[i]);
  }
  s = set_->Remove(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
}

TEST_F(RedisSetTest, IsMember) {
  uint64_t ret = 0;
  bool flag = false;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  for (auto &field : fields_) {
    s = set_->IsMember(*ctx_, key_, field, &flag);
    EXPECT_TRUE(s.ok() && flag);
  }
  set_->IsMember(*ctx_, key_, "foo", &flag);
  EXPECT_TRUE(s.ok() && !flag);
  s = set_->Remove(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
}

TEST_F(RedisSetTest, MIsMember) {
  uint64_t ret = 0;
  std::vector<int> exists;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->MIsMember(*ctx_, key_, fields_, &exists);
  EXPECT_TRUE(s.ok());
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_TRUE(exists[i] == 1);
  }
  s = set_->Remove(*ctx_, key_, {fields_[0]}, &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  s = set_->MIsMember(*ctx_, key_, fields_, &exists);
  EXPECT_TRUE(s.ok() && exists[0] == 0);
  for (size_t i = 1; i < fields_.size(); i++) {
    EXPECT_TRUE(exists[i] == 1);
  }
}

TEST_F(RedisSetTest, Move) {
  uint64_t ret = 0;
  bool flag = false;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  Slice dst("set-test-move-key");
  for (auto &field : fields_) {
    s = set_->Move(*ctx_, key_, dst, field, &flag);
    EXPECT_TRUE(s.ok() && flag);
  }
  s = set_->Move(*ctx_, key_, dst, "set-no-exists-key", &flag);
  EXPECT_TRUE(s.ok() && !flag);
  s = set_->Card(*ctx_, key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  s = set_->Card(*ctx_, dst, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Remove(*ctx_, dst, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set_->Del(*ctx_, dst);
}

TEST_F(RedisSetTest, TakeWithPop) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  std::vector<std::string> members;
  s = set_->Take(*ctx_, key_, &members, 3, true);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), 3);
  s = set_->Take(*ctx_, key_, &members, 2, true);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), 1);
  s = set_->Take(*ctx_, key_, &members, 1, true);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(s.ok() && members.size() == 0);
}

TEST_F(RedisSetTest, Diff) {
  uint64_t ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3";
  rocksdb::Status s = set_->Add(*ctx_, k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(*ctx_, k2, {"c"}, &ret);
  EXPECT_EQ(ret, 1);
  set_->Add(*ctx_, k3, {"a", "c", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  std::vector<std::string> members;
  set_->Diff(*ctx_, {k1, k2, k3}, &members);
  EXPECT_EQ(2, members.size());
  s = set_->Del(*ctx_, k1);
  s = set_->Del(*ctx_, k2);
  s = set_->Del(*ctx_, k3);
}

TEST_F(RedisSetTest, DiffCard) {
  uint64_t ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3";
  rocksdb::Status s = set_->Add(*ctx_, k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(*ctx_, k2, {"c"}, &ret);
  EXPECT_EQ(ret, 1);
  set_->Add(*ctx_, k3, {"a", "c", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  set_->DiffCard(*ctx_, {k1, k2, k3}, 0, &ret);
  EXPECT_EQ(ret, 2);
  set_->DiffCard(*ctx_, {k1, k2, k3}, 1, &ret);
  EXPECT_EQ(ret, 1);
  s = set_->Del(*ctx_, k1);
  s = set_->Del(*ctx_, k2);
  s = set_->Del(*ctx_, k3);
}

TEST_F(RedisSetTest, Union) {
  uint64_t ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3";
  rocksdb::Status s = set_->Add(*ctx_, k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(*ctx_, k2, {"c"}, &ret);
  EXPECT_EQ(ret, 1);
  set_->Add(*ctx_, k3, {"a", "c", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  std::vector<std::string> members;
  set_->Union(*ctx_, {k1, k2, k3}, &members);
  EXPECT_EQ(5, members.size());
  s = set_->Del(*ctx_, k1);
  s = set_->Del(*ctx_, k2);
  s = set_->Del(*ctx_, k3);
}

TEST_F(RedisSetTest, UnionCard) {
  uint64_t ret = 0;
  ASSERT_TRUE(set_->Add(*ctx_, key_, {"a", "b", "c"}, &ret).ok());
  ASSERT_TRUE(set_->Add(*ctx_, "other", {"b", "d"}, &ret).ok());
  for (bool approx : {false, true}) {
    for (uint64_t limit : {0, 1, 3, 4, 10}) {
      ASSERT_TRUE(set_->UnionCard(*ctx_, {key_, "missing", "other", key_}, approx, limit, &ret).ok());
      EXPECT_EQ(ret, limit == 0 ? 4 : std::min<uint64_t>(4, limit));
    }
    ret = 42;
    ASSERT_TRUE(set_->UnionCard(*ctx_, {"missing", "missing"}, approx, 0, &ret).ok());
    EXPECT_EQ(ret, 0);
    ASSERT_TRUE(set_->UnionCard(*ctx_, {}, approx, 0, &ret).ok());
    EXPECT_EQ(ret, 0);
  }
}

TEST_F(RedisSetTest, UnionCardWrongTypeAfterLimit) {
  uint64_t ret = 0;
  ASSERT_TRUE(set_->Add(*ctx_, key_, fields_, &ret).ok());
  redis::String strings(storage_.get(), "set_ns");
  ASSERT_TRUE(strings.Set(*ctx_, "wrongtype", "value").ok());
  for (bool approx : {false, true}) {
    auto s = set_->UnionCard(*ctx_, {key_, "missing", "wrongtype"}, approx, 1, &ret);
    EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }
}

TEST_F(RedisSetTest, UnionCardApproxUsesHyperLogLog) {
  std::vector<std::string> first, second;
  std::vector<uint64_t> hashes;
  for (int i = 0; i < 20000; ++i) {
    auto member = std::to_string(i);
    if (i < 15000) first.push_back(member);
    if (i >= 5000) second.push_back(member);
    hashes.push_back(redis::HyperLogLog::HllHash(member));
  }
  ASSERT_TRUE(set_->Overwrite(*ctx_, key_, first).ok());
  ASSERT_TRUE(set_->Overwrite(*ctx_, "other", second).ok());
  redis::HyperLogLog hll(storage_.get(), "set_ns");
  uint64_t ret = 0, expected = 0;
  ASSERT_TRUE(hll.Add(*ctx_, "hll", hashes, &ret).ok());
  ASSERT_TRUE(hll.Count(*ctx_, "hll", &expected).ok());
  EXPECT_NE(expected, 20000);
  EXPECT_NEAR(expected, 20000, 1000);
  ASSERT_TRUE(set_->UnionCard(*ctx_, {key_, "other", key_}, true, 0, &ret).ok());
  EXPECT_EQ(ret, expected);
  ASSERT_TRUE(set_->UnionCard(*ctx_, {key_, "other"}, true, 30000, &ret).ok());
  EXPECT_EQ(ret, expected);
  ASSERT_TRUE(set_->UnionCard(*ctx_, {key_, "other"}, true, 1000, &ret).ok());
  EXPECT_EQ(ret, 1000);
  ASSERT_TRUE(set_->UnionCard(*ctx_, {key_, "other"}, false, 0, &ret).ok());
  EXPECT_EQ(ret, 20000);
}

TEST_F(RedisSetTest, UnionCardLimitStopsIteration) {
  std::vector<std::string> members;
  members.reserve(4096);
  for (int i = 0; i < 4096; ++i) members.push_back(std::to_string(i));
  ASSERT_TRUE(set_->Overwrite(*ctx_, key_, members).ok());
  auto previous_level = rocksdb::GetPerfLevel();
  auto restore_level = MakeScopeExit([previous_level] { rocksdb::SetPerfLevel(previous_level); });
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableCount);
  for (bool approx : {false, true}) {
    uint64_t ret = 0;
    auto *perf = rocksdb::get_perf_context();
    perf->Reset();
    ASSERT_TRUE(set_->UnionCard(*ctx_, {key_}, approx, 0, &ret).ok());
    auto full_read_bytes = perf->iter_read_bytes;
    ASSERT_GT(full_read_bytes, 0);
    perf->Reset();
    ASSERT_TRUE(set_->UnionCard(*ctx_, {key_}, approx, 1, &ret).ok());
    EXPECT_EQ(ret, 1);
    EXPECT_LT(perf->iter_read_bytes, full_read_bytes / 10);
  }
}

TEST_F(RedisSetTest, Inter) {
  uint64_t ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3", k4 = "key4", k5 = "key5";
  rocksdb::Status s = set_->Add(*ctx_, k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(*ctx_, k2, {"c"}, &ret);
  EXPECT_EQ(ret, 1);
  set_->Add(*ctx_, k3, {"a", "c", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  set_->Add(*ctx_, k5, {"a"}, &ret);
  EXPECT_EQ(ret, 1);
  std::vector<std::string> members;
  set_->Inter(*ctx_, {k1, k2, k3}, &members);
  EXPECT_EQ(1, members.size());
  members.clear();
  set_->Inter(*ctx_, {k1, k2, k4}, &members);
  EXPECT_EQ(0, members.size());
  set_->Inter(*ctx_, {k1, k4, k5}, &members);
  EXPECT_EQ(0, members.size());
  s = set_->Del(*ctx_, k1);
  s = set_->Del(*ctx_, k2);
  s = set_->Del(*ctx_, k3);
  s = set_->Del(*ctx_, k4);
  s = set_->Del(*ctx_, k5);
}

TEST_F(RedisSetTest, InterCard) {
  uint64_t ret = 0;
  std::string k1 = "key1", k2 = "key2", k3 = "key3", k4 = "key4";
  rocksdb::Status s = set_->Add(*ctx_, k1, {"a", "b", "c", "d"}, &ret);
  EXPECT_EQ(ret, 4);
  set_->Add(*ctx_, k2, {"c", "d", "e"}, &ret);
  EXPECT_EQ(ret, 3);
  set_->Add(*ctx_, k3, {"e", "f"}, &ret);
  EXPECT_EQ(ret, 2);
  set_->InterCard(*ctx_, {k1, k2}, 0, &ret);
  EXPECT_EQ(ret, 2);
  set_->InterCard(*ctx_, {k1, k2}, 1, &ret);
  EXPECT_EQ(ret, 1);
  set_->InterCard(*ctx_, {k1, k2}, 3, &ret);
  EXPECT_EQ(ret, 2);
  set_->InterCard(*ctx_, {k2, k3}, 1, &ret);
  EXPECT_EQ(ret, 1);
  set_->InterCard(*ctx_, {k1, k3}, 5, &ret);
  EXPECT_EQ(ret, 0);
  set_->InterCard(*ctx_, {k1, k4}, 5, &ret);
  EXPECT_EQ(ret, 0);
  set_->InterCard(*ctx_, {k1}, 0, &ret);
  EXPECT_EQ(ret, 4);
  for (uint32_t i = 1; i < 20; i++) {
    set_->InterCard(*ctx_, {k1}, i, &ret);
    uint64_t val = (i >= 4) ? 4 : i;
    EXPECT_EQ(ret, val);
  }
  s = set_->Del(*ctx_, k1);
  s = set_->Del(*ctx_, k2);
  s = set_->Del(*ctx_, k3);
  s = set_->Del(*ctx_, k4);
}

TEST_F(RedisSetTest, AddExistingMembers) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fields_.size(), ret);

  s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, ret);

  uint64_t card = 0;
  s = set_->Card(*ctx_, key_, &card);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fields_.size(), card);

  std::vector<Slice> mixed_members = {"set-key-1", "set-key-new-1", "set-key-new-2"};
  s = set_->Add(*ctx_, key_, mixed_members, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, ret);

  s = set_->Card(*ctx_, key_, &card);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fields_.size() + 2, card);
}

TEST_F(RedisSetTest, RemoveNonExistingMembers) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fields_.size(), ret);

  std::vector<Slice> non_existing = {"non-exist-1", "non-exist-2"};
  s = set_->Remove(*ctx_, key_, non_existing, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, ret);

  uint64_t card = 0;
  s = set_->Card(*ctx_, key_, &card);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fields_.size(), card);

  std::vector<Slice> mixed = {"set-key-1", "non-exist-1"};
  s = set_->Remove(*ctx_, key_, mixed, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, ret);

  s = set_->Card(*ctx_, key_, &card);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fields_.size() - 1, card);
}

TEST_F(RedisSetTest, Overwrite) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  set_->Overwrite(*ctx_, key_, {"a"});
  set_->Card(*ctx_, key_, &ret);
  EXPECT_EQ(ret, 1);
}

TEST_F(RedisSetTest, TakeWithoutPop) {
  uint64_t ret = 0;
  rocksdb::Status s = set_->Add(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  std::vector<std::string> members;
  s = set_->Take(*ctx_, key_, &members, int(fields_.size() + 1), false);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), fields_.size());
  s = set_->Take(*ctx_, key_, &members, int(fields_.size() - 1), false);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), fields_.size() - 1);
  s = set_->Take(*ctx_, key_, &members, -int(fields_.size() - 1), false);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(members.size(), fields_.size() - 1);
  s = set_->Remove(*ctx_, key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
}
