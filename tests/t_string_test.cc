#include <redis_string.h>
#include "test_base.h"
#include "redis_string.h"

class RedisStringTest : public TestBase {
protected:
  explicit RedisStringTest() : TestBase() {
    string = new Redis::String(storage_, "string_ns");
  }
  ~RedisStringTest() {
    delete string;
  }
  void SetUp() override {
    key_ = "test-string-key";
    pairs_ = {
            {"test-string-key1", "test-strings-value1"},
            {"test-string-key2", "test-strings-value2"},
            {"test-string-key3", "test-strings-value3"},
            {"test-string-key4", "test-strings-value4"},
            {"test-string-key5", "test-strings-value5"},
            {"test-string-key6", "test-strings-value6"},
    };
  }

protected:
  Redis::String *string;
  std::vector<StringPair> pairs_;
};

TEST_F(RedisStringTest, Append) {
  int ret;
  for (size_t i = 0; i < 32; i++) {
    rocksdb::Status s = string->Append(key_, "a", &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(static_cast<int>(i+1), ret);
  }
  string->Del(key_);
}

TEST_F(RedisStringTest, GetAndSet) {
  for (size_t i = 0; i < pairs_.size(); i++) {
    string->Set(pairs_[i].key, pairs_[i].value);
  }
  for (size_t i = 0; i < pairs_.size(); i++) {
    std::string got_value;
    string->Get(pairs_[i].key, &got_value);
    EXPECT_EQ(pairs_[i].value, got_value);
  }
  for (size_t i = 0; i < pairs_.size(); i++) {
    string->Del(pairs_[i].key);
  }
}

TEST_F(RedisStringTest, MGetAndMSet) {
  string->MSet(pairs_);
  std::vector<Slice> keys;
  std::vector<std::string> values;
  for (const auto pair : pairs_) {
    keys.emplace_back(pair.key);
  }
  string->MGet(keys, &values);
  for (size_t i = 0; i < pairs_.size(); i++) {
    EXPECT_EQ(pairs_[i].value.ToString(), values[i]);
  }
  for (size_t i = 0; i < pairs_.size(); i++) {
    string->Del(pairs_[i].key);
  }
}

TEST_F(RedisStringTest, IncrBy) {
  int64_t ret;
  string->IncrBy(key_, 1, &ret);
  EXPECT_EQ(1, ret);
  string->IncrBy(key_, INT64_MAX-1, &ret);
  EXPECT_EQ(INT64_MAX, ret);
  rocksdb::Status s = string->IncrBy(key_, 1, &ret);
  EXPECT_TRUE(s.IsInvalidArgument());
  string->IncrBy(key_, INT64_MIN+1, &ret);
  EXPECT_EQ(0, ret);
  string->IncrBy(key_, INT64_MIN, &ret);
  EXPECT_EQ(INT64_MIN, ret);
  s = string->IncrBy(key_, -1, &ret);
  EXPECT_TRUE(s.IsInvalidArgument());
  // key hold value is not the number
  string->Set(key_, "abc");
  s = string->IncrBy(key_, 1, &ret);
  EXPECT_TRUE(s.IsInvalidArgument());
  string->Del(key_);

}

TEST_F(RedisStringTest, GetSet) {
  std::vector<Slice> values = {"a", "b", "c", "d"};
  for(size_t i = 0; i < values.size(); i++) {
    std::string old_value;
    string->GetSet(key_, values[i], &old_value);
    if (i != 0) {
      EXPECT_EQ(values[i - 1], old_value);
    } else {
      EXPECT_TRUE(old_value.empty());
    }
  }
  string->Del(key_);
}

TEST_F(RedisStringTest, MSetXX) {
  int ret;
  string->SetXX(key_, "test-value", 3, &ret);
  EXPECT_EQ(ret, 0);
  string->Set(key_, "test-value");
  string->SetXX(key_, "test-value", 3, &ret);
  EXPECT_EQ(ret, 1);
  int ttl;
  string->TTL(key_, &ttl);
  EXPECT_TRUE(ttl >= 2 && ttl <= 3);
  string->Del(key_);
}

TEST_F(RedisStringTest, MSetNX) {
  int ret;
  string->MSetNX(pairs_, 0, &ret);
  EXPECT_EQ(1, ret);
  std::vector<Slice> keys;
  std::vector<std::string> values;
  for (const auto pair : pairs_) {
    keys.emplace_back(pair.key);
  }
  string->MGet(keys, &values);
  for (size_t i = 0; i < pairs_.size(); i++) {
    EXPECT_EQ(pairs_[i].value.ToString(), values[i]);
  }

  std::vector<StringPair> new_pairs{
          {"a", "1"},
          {"b", "2"},
          {"c", "3"},
          {pairs_[0].key, pairs_[0].value},
          {"d", "4"},
  };
  string->MSetNX(pairs_, 0, &ret);
  EXPECT_EQ(0, ret);

  for (size_t i = 0; i < pairs_.size(); i++) {
    string->Del(pairs_[i].key);
  }
}

TEST_F(RedisStringTest, MSetNXWithTTL) {
  int ret;
  string->SetNX(key_, "test-value", 3, &ret);
  int ttl;
  string->TTL(key_, &ttl);
  EXPECT_TRUE(ttl >= 2 && ttl <= 3);
  string->Del(key_);
}

TEST_F(RedisStringTest, SetEX) {
  string->SetEX(key_, "test-value", 3);
  int ttl;
  string->TTL(key_, &ttl);
  EXPECT_TRUE(ttl >= 2 && ttl <= 3);
  string->Del(key_);
}

TEST_F(RedisStringTest, SetRange) {
  int ret;
  string->Set(key_, "hello,world");
  string->SetRange(key_, 6, "redis", &ret);
  EXPECT_EQ(11, ret);
  std::string value;
  string->Get(key_, &value);
  EXPECT_EQ("hello,redis", value);
  string->SetRange(key_, 6, "redis-1", &ret);
  EXPECT_EQ(13, ret);
  string->Get(key_, &value);
  EXPECT_EQ("hello,redis-1", value);
  string->SetRange(key_, 15, "1", &ret);
  EXPECT_EQ(16, ret);
  string->Get(key_, &value);
  EXPECT_EQ(16, value.size());
  string->Del(key_);
}