#include <gtest/gtest.h>

#include "test_base.h"
#include "redis_hash.h"

class RedisExHashTest : public TestBase {
 protected:
  RedisExHashTest() : TestBase() {
    exhash = new Redis::Hash(storage_, "exhash_ns", true);
  }
  ~RedisExHashTest() {
    delete exhash;
  }
  void SetUp() override {
    key_ = "test_exhash->key";
    fields_ = {"test-exhash-key-1", "test-exhash-key-2", "test-exhash-key-3"};
    values_  = {"exhash-test-value-1", "exhash-test-value-2", "exhash-test-value-3"};
    ttls_ = {0, 1, 10};
  }
  void TearDown() override {
  }

 protected:
  Redis::Hash *exhash;
};

TEST_F(RedisExHashTest, GetAndSet) {
  int ret;
  for (size_t i = 0; i < fields_.size(); i++) {
    rocksdb::Status s = exhash->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string got;
    rocksdb::Status s = exhash->Get(key_, fields_[i], &got);
    EXPECT_EQ(values_[i], got);
  }
  rocksdb::Status s = exhash->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  exhash->Del(key_);
}

TEST_F(RedisExHashTest, GetAndSetWithTTL) {
  int ret;
  int64_t now;
  std::string got;
  int32_t ttl = 0;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  for (size_t i = 0; i < fields_.size(); i++) {
    rocksdb::Status s = exhash->Set(key_, fields_[i], values_[i], ttls_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }

  rocksdb::Status s = exhash->Get(key_, fields_[0], &got, &ttl);
  EXPECT_TRUE(s.ok() && got == values_[0] && ttl == -1);

  sleep(2);
  s = exhash->Get(key_, fields_[1], &got, &ttl);
  EXPECT_TRUE(s == rocksdb::Status::NotFound(kErrMsgFieldExpired) && ttl == -2);

  s = exhash->Get(key_, fields_[2], &got, &ttl);
  EXPECT_TRUE(s.ok() && got == values_[2]);
  // The TTL should be reduced by at least 2s
  EXPECT_TRUE(ttls_[2] - ttl >= 2 && ttls_[2] - ttl <= 3);

  uint32_t size;
  exhash->Size(key_, &size, false);
  EXPECT_EQ(size, 3);
  exhash->Size(key_, &size, true);
  EXPECT_EQ(size, 2);

  s = exhash->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  exhash->Del(key_);
}

TEST_F(RedisExHashTest, MGetAndMSet) {
  int ret;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(FieldValue{fields_[i].ToString(), values_[i].ToString(), 0});
  }
  rocksdb::Status s = exhash->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fvs.size()) == ret);
  s = exhash->MSet(key_, fvs, false, &ret);
  EXPECT_EQ(ret, 0);
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  s = exhash->MGet(key_, fields_, &values, &statuses);
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(values[i], values_[i].ToString());
  }
  s = exhash->Delete(key_, fields_, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  exhash->Del(key_);
}

TEST_F(RedisExHashTest, MGetAndMSetWithTTL) {
  int ret;
  std::vector<FieldValue> fvs;
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(FieldValue{fields_[i].ToString(), values_[i].ToString(), 2});
  }
  rocksdb::Status s = exhash->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fvs.size()) == ret);
  s = exhash->MSet(key_, fvs, false, &ret);
  EXPECT_EQ(ret, 0);
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  std::vector<int32_t> field_ttls;
  s = exhash->MGet(key_, fields_, &values, &field_ttls, &statuses);
  for (size_t i = 0; i < fields_.size(); i++) {
    if (statuses[i].ok()) {
      EXPECT_EQ(values[i], values_[i].ToString());
      EXPECT_TRUE(field_ttls[i] >= 2 && field_ttls[i] <= 3);
    } else {
      EXPECT_TRUE(statuses[i] == rocksdb::Status::NotFound(kErrMsgFieldExpired) && field_ttls[i] == -2);
    }
  }
  s = exhash->Delete(key_, fields_, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  exhash->Del(key_);
}

TEST_F(RedisExHashTest, SetNX) {
  int ret;
  Slice field("foo");
  rocksdb::Status s = exhash->Set(key_, field, "bar", &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  s = exhash->Set(key_, field, "bar", &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  std::vector<Slice> fields = {field};
  s = exhash->Delete(key_, fields, &ret);
  EXPECT_EQ(fields.size(), (size_t)ret);
  exhash->Del(key_);
}

TEST_F(RedisExHashTest, HGetAll) {
  int ret;
  for (size_t i = 0; i < fields_.size(); i++) {
    rocksdb::Status s = exhash->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  std::vector<FieldValue> fvs;
  rocksdb::Status s = exhash->GetAll(key_, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == fields_.size());
  s = exhash->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  exhash->Del(key_);
}

TEST_F(RedisExHashTest, HGetAllWithTTL) {
  int ret;
  for (size_t i = 0; i < fields_.size(); i++) {
    rocksdb::Status s = exhash->Set(key_, fields_[i], values_[i], ttls_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  std::vector<FieldValue> fvs;
  sleep(2);  // fields_[1] will expired
  rocksdb::Status s = exhash->GetAll(key_, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == 2);
  s = exhash->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  exhash->Del(key_);
}

TEST_F(RedisExHashTest, HIncr) {
  int64_t value;
  Slice field("exhash-incrby-invalid-field");
  for (int i = 0; i < 32; i++) {
    rocksdb::Status s = exhash->IncrBy(key_, field, 1, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  exhash->Get(key_, field, &bytes);
  value = std::stoll(bytes);
  EXPECT_EQ(32, value);
  exhash->Del(key_);
}

TEST_F(RedisExHashTest, HIncrInvalid) {
  int ret;
  int64_t value;
  Slice field("hash-incrby-invalid-field");
  rocksdb::Status s = exhash->IncrBy(key_, field, 1, &value);
  EXPECT_TRUE(s.ok() && value == 1);

  s = exhash->IncrBy(key_, field, LLONG_MAX, &value);
  EXPECT_TRUE(s.IsInvalidArgument());
  exhash->Set(key_, field, "abc", &ret);
  s = exhash->IncrBy(key_, field, 1, &value);
  EXPECT_TRUE(s.IsInvalidArgument());

  exhash->Set(key_, field, "-1", &ret);
  s = exhash->IncrBy(key_, field, -1, &value);
  EXPECT_TRUE(s.ok());
  s = exhash->IncrBy(key_, field, LLONG_MIN, &value);
  EXPECT_TRUE(s.IsInvalidArgument());

  exhash->Del(key_);
}

TEST_F(RedisExHashTest, HIncrByFloat) {
  double value;
  Slice field("hash-incrbyfloat-invalid-field");
  for (int i = 0; i < 32; i++) {
    rocksdb::Status s = exhash->IncrByFloat(key_, field, 1.2, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  exhash->Get(key_, field, &bytes);
  value = std::stof(bytes);
  EXPECT_FLOAT_EQ(32*1.2, value);
  exhash->Del(key_);
}
