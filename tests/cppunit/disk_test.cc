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

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "stats/disk_stats.h"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "types/redis_bitmap.h"
#include "types/redis_list.h"
#include "types/redis_set.h"
#include "types/redis_sortedint.h"
#include "types/redis_stream.h"
#include "types/redis_string.h"
#include "types/redis_zset.h"

class RedisDiskTest : public TestBase {
 protected:
  explicit RedisDiskTest() = default;
  ~RedisDiskTest() override = default;

  double estimation_factor_ = 0.1;
};

TEST_F(RedisDiskTest, StringDisk) {
  key_ = "stringdisk_key";
  std::unique_ptr<redis::String> string = std::make_unique<redis::String>(storage_, "disk_ns_string");
  std::unique_ptr<redis::Disk> disk = std::make_unique<redis::Disk>(storage_, "disk_ns_string");
  std::vector<int> value_size{1024 * 1024};
  EXPECT_TRUE(string->Set(key_, std::string(value_size[0], 'a')).ok());
  uint64_t result = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisString, &result).ok());
  EXPECT_GE(result, value_size[0] * estimation_factor_);
  EXPECT_LE(result, value_size[0] / estimation_factor_);
  string->Del(key_);
}

TEST_F(RedisDiskTest, HashDisk) {
  std::unique_ptr<redis::Hash> hash = std::make_unique<redis::Hash>(storage_, "disk_ns_hash");
  std::unique_ptr<redis::Disk> disk = std::make_unique<redis::Disk>(storage_, "disk_ns_hash");
  key_ = "hashdisk_key";
  fields_ = {"hashdisk_kkey1", "hashdisk_kkey2", "hashdisk_kkey3", "hashdisk_kkey4", "hashdisk_kkey5"};
  values_.resize(5);
  uint64_t approximate_size = 0;
  uint64_t ret = 0;
  std::vector<int> value_size{1024, 1024, 1024, 1024, 1024};
  std::vector<std::string> values(value_size.size());
  for (int i = 0; i < int(fields_.size()); i++) {
    values[i] = std::string(value_size[i], static_cast<char>('a' + i));
    values_[i] = values[i];
    approximate_size += key_.size() + 8 + fields_[i].size() + values_[i].size();
    rocksdb::Status s = hash->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisHash, &key_size).ok());
  EXPECT_GE(key_size, approximate_size * estimation_factor_);
  EXPECT_LE(key_size, approximate_size / estimation_factor_);
  hash->Del(key_);
}

TEST_F(RedisDiskTest, SetDisk) {
  std::unique_ptr<redis::Set> set = std::make_unique<redis::Set>(storage_, "disk_ns_set");
  std::unique_ptr<redis::Disk> disk = std::make_unique<redis::Disk>(storage_, "disk_ns_set");
  key_ = "setdisk_key";
  values_.resize(5);
  uint64_t approximate_size = 0;
  uint64_t ret = 0;
  std::vector<int> value_size{1024, 1024, 1024, 1024, 1024};
  std::vector<std::string> values(value_size.size());
  for (int i = 0; i < int(values_.size()); i++) {
    values[i] = std::string(value_size[i], static_cast<char>(i + 'a'));
    values_[i] = values[i];
    approximate_size += key_.size() + values_[i].size() + 8;
  }
  rocksdb::Status s = set->Add(key_, values_, &ret);
  EXPECT_TRUE(s.ok() && ret == 5);

  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisSet, &key_size).ok());
  EXPECT_GE(key_size, approximate_size * estimation_factor_);
  EXPECT_LE(key_size, approximate_size / estimation_factor_);

  set->Del(key_);
}

TEST_F(RedisDiskTest, ListDisk) {
  std::unique_ptr<redis::List> list = std::make_unique<redis::List>(storage_, "disk_ns_list");
  std::unique_ptr<redis::Disk> disk = std::make_unique<redis::Disk>(storage_, "disk_ns_list");
  key_ = "listdisk_key";
  values_.resize(5);
  std::vector<int> value_size{1024, 1024, 1024, 1024, 1024};
  std::vector<std::string> values(value_size.size());
  uint64_t approximate_size = 0;
  for (int i = 0; i < int(values_.size()); i++) {
    values[i] = std::string(value_size[i], static_cast<char>('a' + i));
    values_[i] = values[i];
    approximate_size += key_.size() + values_[i].size() + 8 + 8;
  }
  uint64_t ret = 0;
  rocksdb::Status s = list->Push(key_, values_, false, &ret);
  EXPECT_TRUE(s.ok() && ret == 5);
  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisList, &key_size).ok());
  EXPECT_GE(key_size, approximate_size * estimation_factor_);
  EXPECT_LE(key_size, approximate_size / estimation_factor_);
  list->Del(key_);
}

TEST_F(RedisDiskTest, ZsetDisk) {
  std::unique_ptr<redis::ZSet> zset = std::make_unique<redis::ZSet>(storage_, "disk_ns_zet");
  std::unique_ptr<redis::Disk> disk = std::make_unique<redis::Disk>(storage_, "disk_ns_zet");
  key_ = "zsetdisk_key";
  uint64_t ret = 0;
  uint64_t approximate_size = 0;
  std::vector<MemberScore> mscores(5);
  std::vector<int> value_size{1024, 1024, 1024, 1024, 1024};
  for (int i = 0; i < int(value_size.size()); i++) {
    mscores[i].member = std::string(value_size[i], static_cast<char>('a' + i));
    mscores[i].score = 1.0;
    approximate_size += (key_.size() + 8 + mscores[i].member.size() + 8) * 2;
  }
  rocksdb::Status s = zset->Add(key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_TRUE(s.ok() && ret == 5);
  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisZSet, &key_size).ok());
  EXPECT_GE(key_size, approximate_size * estimation_factor_);
  EXPECT_LE(key_size, approximate_size / estimation_factor_);
  zset->Del(key_);
}

TEST_F(RedisDiskTest, BitmapDisk) {
  std::unique_ptr<redis::Bitmap> bitmap = std::make_unique<redis::Bitmap>(storage_, "disk_ns_bitmap");
  std::unique_ptr<redis::Disk> disk = std::make_unique<redis::Disk>(storage_, "disk_ns_bitmap");
  key_ = "bitmapdisk_key";
  bool bit = false;
  uint64_t approximate_size = 0;
  for (int i = 0; i < 1024 * 8 * 100000; i += 1024 * 8) {
    EXPECT_TRUE(bitmap->SetBit(key_, i, true, &bit).ok());
    approximate_size += key_.size() + 8 + std::to_string(i / 1024 / 8).size();
  }
  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisBitmap, &key_size).ok());
  EXPECT_GE(key_size, approximate_size * estimation_factor_);
  EXPECT_LE(key_size, approximate_size / estimation_factor_);
  bitmap->Del(key_);
}

TEST_F(RedisDiskTest, BitmapDisk2) {
  for (bool set_op : {false, true}) {
    std::unique_ptr<redis::Bitmap> bitmap = std::make_unique<redis::Bitmap>(storage_, "disk_ns_bitmap2");
    std::unique_ptr<redis::Disk> disk = std::make_unique<redis::Disk>(storage_, "disk_ns_bitmap2");
    key_ = "bitmapdisk_key2";
    bool bit = false;
    EXPECT_TRUE(bitmap->SetBit(key_, 0, !set_op, &bit).ok());
    EXPECT_TRUE(bitmap->SetBit(key_, 8191, set_op, &bit).ok());
    bool result = false;
    EXPECT_TRUE(bitmap->GetBit(key_, 8191, &result).ok());
    EXPECT_EQ(set_op, result);
    auto not_dest_key = "bit_op_not_dest_key";

    int64_t len = 0;
    bitmap->BitOp(BitOpFlags::kBitOpNot, "NOT", not_dest_key, {key_}, &len);

    EXPECT_TRUE(bitmap->GetBit(not_dest_key, 8191, &result).ok());
    EXPECT_EQ(!set_op, result);
    EXPECT_TRUE(bitmap->GetBit(not_dest_key, 0, &result).ok());
    EXPECT_EQ(set_op, result);

    for (int i = 1; i < 8191; ++i) {
      EXPECT_TRUE(bitmap->GetBit(not_dest_key, i, &result).ok());
      EXPECT_TRUE(result);
    }
  }
}

TEST_F(RedisDiskTest, SortedintDisk) {
  std::unique_ptr<redis::Sortedint> sortedint = std::make_unique<redis::Sortedint>(storage_, "disk_ns_sortedint");
  std::unique_ptr<redis::Disk> disk = std::make_unique<redis::Disk>(storage_, "disk_ns_sortedint");
  key_ = "sortedintdisk_key";
  uint64_t ret = 0;
  uint64_t approximate_size = 0;
  for (int i = 0; i < 100000; i++) {
    EXPECT_TRUE(sortedint->Add(key_, std::vector<uint64_t>{uint64_t(i)}, &ret).ok() && ret == 1);
    approximate_size += key_.size() + 8 + 8;
  }
  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisSortedint, &key_size).ok());
  EXPECT_GE(key_size, approximate_size * estimation_factor_);
  EXPECT_LE(key_size, approximate_size / estimation_factor_);
  sortedint->Del(key_);
}

TEST_F(RedisDiskTest, StreamDisk) {
  std::unique_ptr<redis::Stream> stream = std::make_unique<redis::Stream>(storage_, "disk_ns_stream");
  std::unique_ptr<redis::Disk> disk = std::make_unique<redis::Disk>(storage_, "disk_ns_stream");
  key_ = "streamdisk_key";
  redis::StreamAddOptions options;
  options.next_id_strategy = *redis::ParseNextStreamEntryIDStrategy("*");
  redis::StreamEntryID id;
  uint64_t approximate_size = 0;
  for (int i = 0; i < 100000; i++) {
    std::vector<std::string> values = {"key" + std::to_string(i), "val" + std::to_string(i)};
    auto s = stream->Add(key_, options, values, &id);
    EXPECT_TRUE(s.ok());
    approximate_size += key_.size() + 8 + 8 + values[0].size() + values[1].size();
  }
  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisStream, &key_size).ok());
  EXPECT_GE(key_size, approximate_size * estimation_factor_);
  EXPECT_LE(key_size, approximate_size / estimation_factor_);
  stream->Del(key_);
}