#include <gtest/gtest.h>
#include "t_set.h"
#include "test_base.h"

class RedisSetTest : public TestBase {
protected:
  explicit RedisSetTest() : TestBase() {
    set = new RedisSet(storage_);
  }
  ~RedisSetTest() {
    delete set;
  }
  void SetUp() override {
    key_ = "test-set-key";
    fields_ = {"set-key-1", "set-key-2", "set-key-3", "set-key-4"};
  }

protected:
  RedisSet *set;
};

TEST_F(RedisSetTest, AddAndRemove) {
  int ret;
   rocksdb::Status s = set->Add(key_, fields_, &ret);
   EXPECT_TRUE(s.ok() && fields_.size() == ret);
   s = set->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  set->Del(key_);
}

TEST_F(RedisSetTest, Members) {
  int ret;
  rocksdb::Status s = set->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  std::vector<std::string> members;
  s = set->Members(key_, &members);
  EXPECT_TRUE(s.ok() && fields_.size() == members.size());
  // Note: the members was fetched by iterator, so the order should be asec
  for (int i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(fields_[i], members[i]);
  }
  s = set->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  set->Del(key_);
}

TEST_F(RedisSetTest, IsMember) {
  int ret;
  rocksdb::Status s = set->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  for (int i = 0; i < fields_.size(); i++) {
    s = set->IsMember(key_, fields_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  set->IsMember(key_, "foo", &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  s = set->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  set->Del(key_);
}

TEST_F(RedisSetTest, Move) {
  int ret;
  rocksdb::Status s = set->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  Slice dst("set-test-move-key");
  for (int i = 0; i < fields_.size(); i++) {
    s = set->Move(key_, dst, fields_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  s = set->Move(key_, dst, "set-no-exists-key", &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  s = set->Card(key_, &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  s = set->Card(dst, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  s = set->Remove(dst, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  set->Del(key_);
  set->Del(dst);
}

TEST_F(RedisSetTest, TakeWithPop) {
  int ret;
  rocksdb::Status s = set->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  std::vector<std::string> members;
  s = set->Take(key_, &members, 3, true);
  EXPECT_EQ(members.size(),3);
  s = set->Take(key_, &members, 2, true);
  EXPECT_EQ(members.size(),1);
  s = set->Take(key_, &members, 1, true);
  EXPECT_TRUE(s.ok() && members.size() == 0);
  set->Del(key_);
}

TEST_F(RedisSetTest, TakeWithoutPop) {
  int ret;
  rocksdb::Status s = set->Add(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  std::vector<std::string> members;
  s = set->Take(key_, &members, int(fields_.size()+1), false);
  EXPECT_EQ(members.size(), fields_.size());
  s = set->Take(key_, &members, int(fields_.size()-1), false);
  EXPECT_EQ(members.size(), fields_.size()-1);
  s = set->Remove(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && fields_.size() == ret);
  set->Del(key_);
}
