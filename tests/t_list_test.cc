#include "test_base.h"
#include "redis_list.h"
#include <gtest/gtest.h>

class RedisListTest : public TestBase {
protected:
  explicit RedisListTest():TestBase() {
    list = new Redis::List(storage_, "list_ns");
  }
  ~RedisListTest() {
    delete list;
  }
  void SetUp() override {
    key_ = "test-list-key";
    fields_ = {"list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5"};
  }

protected:
  Redis::List *list;
};

TEST_F(RedisListTest, PushAndPop) {
  int ret;
  list->Push(key_, fields_, true, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, false);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Pushx) {
  int ret;
  Slice pushx_key("test-pushx-key");
  rocksdb::Status s = list->PushX(pushx_key, fields_, true, &ret);
  EXPECT_TRUE(s.ok());
  list->Push(pushx_key, fields_, true, &ret);
  EXPECT_EQ(fields_.size(), ret);
  s = list->PushX(pushx_key, fields_, true, &ret);
  EXPECT_EQ(ret, fields_.size()*2);
  list->Del(pushx_key);
}

TEST_F(RedisListTest, Index) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::string elem;
  for (size_t i = 0; i < fields_.size(); i++) {
    list->Index(key_,i, &elem);
    EXPECT_EQ(fields_[i].ToString(), elem);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  rocksdb::Status s = list->Index(key_,-1, &elem);
  EXPECT_TRUE(s.IsNotFound());
  list->Del(key_);
}

TEST_F(RedisListTest, Set) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice new_elem("new_elem");
  list->Set(key_, -1, new_elem);
  std::string elem;
  list->Index(key_, -1, &elem);
  EXPECT_EQ(new_elem.ToString(), elem);
  for (size_t i = 0; i < fields_.size(); i++) {
    list->Pop(key_, &elem, true);
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Range) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::vector<std::string> elems;
  list->Range(key_, 0, int(elems.size()-1), &elems);
  EXPECT_EQ(elems.size(), fields_.size());
  for (size_t i = 0; i < elems.size(); i++) {
    EXPECT_EQ(fields_[i].ToString(), elems[i]);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Trim) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Trim(key_, 1, 2000);
  uint32_t len;
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size()-1, len);
  for (size_t i = 1; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, RPopLPush) {
  int ret;
  list->Push(key_, fields_, true, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice dst("test-list-rpoplpush-key");
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->RPopLPush(key_, dst, &elem);
    EXPECT_EQ(fields_[i].ToString(), elem);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(dst, &elem, false);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
  list->Del(dst);
}