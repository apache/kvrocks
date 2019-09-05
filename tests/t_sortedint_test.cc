#include <gtest/gtest.h>
#include "redis_sortedint.h"
#include "test_base.h"

class RedisSortedintTest : public TestBase {
protected:
  explicit RedisSortedintTest() : TestBase() {
    sortedint = new Redis::Sortedint(storage_, "sortedint_ns");
  }
  ~RedisSortedintTest() {
    delete sortedint;
  }
  void SetUp() override {
    key_ = "test-sortedint-key";
    ids_ = {1, 2, 3, 4};
  }

protected:
  Redis::Sortedint *sortedint;
  std::vector<uint64_t > ids_;
};

TEST_F(RedisSortedintTest, AddAndRemove) {
  int ret;
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
  int ret;
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
