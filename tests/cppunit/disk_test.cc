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

#include <chrono>
#include <memory>
#include <vector>
#include <gtest/gtest.h>
#include "redis_metadata.h"
#include "test_base.h"
#include "redis_string.h"
#include "redis_disk.h"
#include "redis_set.h"
#include "redis_list.h"
#include "redis_zset.h"
#include "redis_bitmap.h"
#include "redis_sortedint.h"

class RedisDiskTest : public TestBase {
protected:
  explicit RedisDiskTest() : TestBase() {
    delete storage_;
    config_->RocksDB.compression = rocksdb::CompressionType::kNoCompression;
    config_->RocksDB.write_buffer_size = 1;
    storage_ = new Engine::Storage(config_);
    Status s = storage_->Open();
    if (!s.IsOK()) {
      std::cout << "Failed to open the storage, encounter error: " << s.Msg() << std::endl;
      assert(s.IsOK());
    }
  }
  ~RedisDiskTest() = default;

protected:
  double estimation_factor_ = 0.5;
};

TEST_F(RedisDiskTest, StringDisk) {
  key_ = "stringdisk_key";
  std::unique_ptr<Redis::String> string = Util::MakeUnique<Redis::String>(storage_, "disk_ns_string");
  std::unique_ptr<Redis::Disk> disk = Util::MakeUnique<Redis::Disk>(storage_, "disk_ns_string");
  std::vector<int> value_size{1024*1024};
  for(auto &p : value_size){
    EXPECT_TRUE(string->Set(key_, std::string(p, 'a')).ok());
    std::string got_value;
    EXPECT_TRUE(string->Get(key_,  &got_value).ok());
    EXPECT_EQ(got_value, std::string(p, 'a'));
    uint64_t result = 0;
    EXPECT_TRUE(disk->GetKeySize(key_, kRedisString, &result).ok());
    EXPECT_GE(result, p * estimation_factor_);
  }
  string->Del(key_);
}

TEST_F(RedisDiskTest, HashDisk) {
  std::unique_ptr<Redis::Hash> hash = Util::MakeUnique<Redis::Hash>(storage_, "disk_ns_hash");
  std::unique_ptr<Redis::Disk> disk = Util::MakeUnique<Redis::Disk>(storage_, "disk_ns_hash");
  key_ = "hashdisk_key";
  fields_ = {"hashdisk_kkey1", "hashdisk_kkey2", "hashdisk_kkey3", "hashdisk_kkey4", "hashdisk_kkey5"};
  values_.resize(5);
  uint64_t sum = 0;
  int ret = 0;
  std::vector<int>value_size{1024, 1024, 1024, 1024, 1024};
  for(int i = 0; i < int(fields_.size()); i++){
    values_[i] = std::string(value_size[i], static_cast<char>('a' + i));
    sum += uint64_t(fields_[i].size()) + values_[i].size();
    rocksdb::Status s = hash->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisHash, &key_size).ok());
  EXPECT_GE(key_size, sum * estimation_factor_);
  hash->Del(key_);
}

TEST_F(RedisDiskTest, SetDisk) {
  std::unique_ptr<Redis::Set> set = Util::MakeUnique<Redis::Set>(storage_, "disk_ns_set");
  std::unique_ptr<Redis::Disk> disk = Util::MakeUnique<Redis::Disk>(storage_, "disk_ns_set");
  key_ = "setdisk_key";
  values_.resize(5);
  uint64_t sum = 0;
  int ret =0 ;
  std::vector<int>value_size{1024, 1024, 1024, 1024, 1024};
  for(int i = 0; i < int(values_.size()); i++){
    values_[i] = std::string(value_size[i], static_cast<char>(i+ 'a'));
    sum += values_[i].size();
  }
  rocksdb::Status s = set->Add(key_, values_, &ret);
  EXPECT_TRUE(s.ok() && ret == 5);
  
  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisSet, &key_size).ok());
  EXPECT_GE(key_size, sum * estimation_factor_);

  set->Del(key_);
}


TEST_F(RedisDiskTest, ListDisk) {
  std::unique_ptr<Redis::List> list = Util::MakeUnique<Redis::List>(storage_, "disk_ns_list");
  std::unique_ptr<Redis::Disk> disk = Util::MakeUnique<Redis::Disk>(storage_, "disk_ns_list");
  key_ = "listdisk_key";
  values_.resize(5);
  std::vector<int>value_size{1024, 1024, 1024, 1024, 1024};
  uint64_t sum = 0;
  for(int i = 0; i < int(values_.size()); i++){
    values_[i] = std::string(value_size[i], static_cast<char>('a' + i));
    sum += values_[i].size();
  }
  int ret = 0;
  rocksdb::Status s = list->Push(key_, values_, false, &ret);
  EXPECT_TRUE(s.ok() && ret == 5);
  uint64_t key_size = 0;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisList, &key_size).ok());
  EXPECT_GE(key_size, sum * estimation_factor_);
  list->Del(key_);
}

TEST_F(RedisDiskTest, ZsetDisk) {
  std::unique_ptr<Redis::ZSet> zset = Util::MakeUnique<Redis::ZSet>(storage_, "disk_ns_zet");
  std::unique_ptr<Redis::Disk> disk = Util::MakeUnique<Redis::Disk>(storage_, "disk_ns_zet");
  key_ = "zsetdisk_key";
  int ret = 0;
  uint64_t sum = 0;
  std::vector<MemberScore> mscores(5);
  std::vector<int>value_size{1024, 1024, 1024, 1024, 1024};
  for(int i = 0; i < int(value_size.size()); i++){
    mscores[i].member = std::string(value_size[i], static_cast<char>('a' + i));
    mscores[i].score = 1.0 * value_size[int(values_.size()) - i - 1];
    sum += mscores[i].member.size();
  }
  rocksdb::Status s = zset->Add(key_, 0, &mscores, &ret);
  EXPECT_TRUE(s.ok() && ret == 5);
  uint64_t key_size = 0; 
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisZSet, &key_size).ok());
  EXPECT_GE(key_size, sum * estimation_factor_);
  zset->Del(key_);
}

TEST_F(RedisDiskTest, BitmapDisk) {
  std::unique_ptr<Redis::Bitmap> bitmap = Util::MakeUnique<Redis::Bitmap>(storage_, "disk_ns_bitmap");
  std::unique_ptr<Redis::Disk> disk = Util::MakeUnique<Redis::Disk>(storage_, "disk_ns_bitmap");
  key_ = "bitmapdisk_key";
  bool bit = false;
  for (int i= 0; i < 1024*8*1000; i += 1024 * 8){
    EXPECT_TRUE(bitmap->SetBit(key_, i, true, &bit).ok());
  }
  uint64_t key_size;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisBitmap, &key_size).ok());
  EXPECT_GE(key_size, 800 * estimation_factor_);
  bitmap->Del(key_);
}

TEST_F(RedisDiskTest, SortedintDisk) {
  std::unique_ptr<Redis::Sortedint> sortedint = Util::MakeUnique<Redis::Sortedint>(storage_, "disk_ns_sortedint");
  std::unique_ptr<Redis::Disk> disk = Util::MakeUnique<Redis::Disk>(storage_, "disk_ns_sortedint");
  key_ = "sortedintdisk_key";
  int ret;
  for(int i = 0; i < 100000; i++){
    EXPECT_TRUE(sortedint->Add(key_, std::vector<uint64_t>{uint64_t(i)}, &ret).ok() && ret==1);
  }
  uint64_t key_size;
  EXPECT_TRUE(disk->GetKeySize(key_, kRedisSortedint, &key_size).ok());
  EXPECT_GE(key_size, 10000 * estimation_factor_);
  sortedint->Del(key_);
}
