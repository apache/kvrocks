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

#include "redis_metadata.h"
#include "redis_hash.h"
#include "test_base.h"
#include <gtest/gtest.h>
#include <memory>

TEST(InternalKey, EncodeAndDecode) {
  Slice key = "test-metadata-key";
  Slice sub_key = "test-metadata-sub-key";
  Slice ns = "namespace";
  uint64_t version = 12;
  std::string ns_key;

  ComposeNamespaceKey(ns, key, &ns_key, false);
  InternalKey ikey(ns_key, sub_key, version, false);
  ASSERT_EQ(ikey.GetKey(), key);
  ASSERT_EQ(ikey.GetSubKey(), sub_key);
  ASSERT_EQ(ikey.GetVersion(), version);
  std::string bytes;
  ikey.Encode(&bytes);
  InternalKey ikey1(bytes, false);
  EXPECT_EQ(ikey, ikey1);
}

TEST(Metadata, EncodeAndDeocde) {
  std::string string_bytes;
  Metadata string_md(kRedisString);
  string_md.expire = 123;
  string_md.Encode(&string_bytes);
  Metadata string_md1(kRedisNone);
  string_md1.Decode(string_bytes);
  ASSERT_EQ(string_md, string_md1);
  ListMetadata list_md;
  list_md.flags = 13;
  list_md.expire = 123;
  list_md.version = 2;
  list_md.size = 1234;
  list_md.head = 123;
  list_md.tail = 321;
  ListMetadata list_md1;
  std::string list_bytes;
  list_md.Encode(&list_bytes);
  list_md1.Decode(list_bytes);
  ASSERT_EQ(list_md, list_md1);
}

class RedisTypeTest : public TestBase {
public:
  RedisTypeTest() :TestBase() {
    redis = Util::MakeUnique<Redis::Database>(storage_, "default_ns");
    hash = Util::MakeUnique<Redis::Hash>(storage_, "default_ns");
    key_ = "test-redis-type";
    fields_ = {"test-hash-key-1", "test-hash-key-2", "test-hash-key-3"};
    values_  = {"hash-test-value-1", "hash-test-value-2", "hash-test-value-3"};
  }
  ~RedisTypeTest() = default;
protected:
  std::unique_ptr<Redis::Database> redis;
  std::unique_ptr<Redis::Hash> hash;
};

TEST_F(RedisTypeTest, GetMetadata) {
  int ret;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(FieldValue{fields_[i].ToString(), values_[i].ToString()});
  }
  rocksdb::Status s = hash->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fvs.size())==ret);
  HashMetadata metadata;
  std::string ns_key;
  redis->AppendNamespacePrefix(key_, &ns_key);
  redis->GetMetadata(kRedisHash, ns_key, &metadata);
  EXPECT_EQ(fvs.size(), metadata.size);
  s = redis->Del(key_);
  EXPECT_TRUE(s.ok());
}

TEST_F(RedisTypeTest, Expire) {
  int ret;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(FieldValue{fields_[i].ToString(), values_[i].ToString()});
  }
  rocksdb::Status s = hash->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fvs.size())==ret);
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  redis->Expire(key_,int(now+2));
  int ttl;
  redis->TTL(key_, &ttl);
  ASSERT_TRUE(ttl >= 1 && ttl <= 2);
  sleep(2);
}