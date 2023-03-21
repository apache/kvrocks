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

#include <filesystem>

#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "types/redis_hash.h"
#include "types/redis_zset.h"

TEST(Compact, Filter) {
  Config config;
  config.db_dir = "compactdb";
  config.backup_dir = "compactdb/backup";
  config.slot_id_encoded = false;

  auto storage_ = std::make_unique<Engine::Storage>(&config);
  Status s = storage_->Open();
  assert(s.IsOK());

  int ret = 0;
  std::string ns = "test_compact";
  auto hash = std::make_unique<Redis::Hash>(storage_.get(), ns);
  std::string expired_hash_key = "expire_hash_key";
  std::string live_hash_key = "live_hash_key";
  hash->Set(expired_hash_key, "f1", "v1", &ret);
  hash->Set(expired_hash_key, "f2", "v2", &ret);
  hash->Expire(expired_hash_key, 1);  // expired
  usleep(10000);
  hash->Set(live_hash_key, "f1", "v1", &ret);
  hash->Set(live_hash_key, "f2", "v2", &ret);
  auto status = storage_->Compact(nullptr, nullptr);
  assert(status.ok());
  rocksdb::DB* db = storage_->GetDB();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = db->GetSnapshot();
  read_options.fill_cache = false;

  auto NewIterator = [db, read_options, &storage_](const std::string& name) {
    return std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options, storage_->GetCFHandle(name)));
  };

  auto iter = NewIterator("metadata");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string user_key, user_ns;
    ExtractNamespaceKey(iter->key(), &user_ns, &user_key, storage_->IsSlotIdEncoded());
    EXPECT_EQ(user_key, live_hash_key);
  }

  iter = NewIterator("subkey");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }

  auto zset = std::make_unique<Redis::ZSet>(storage_.get(), ns);
  std::string expired_zset_key = "expire_zset_key";
  std::vector<MemberScore> member_scores = {MemberScore{"z1", 1.1}, MemberScore{"z2", 0.4}};
  zset->Add(expired_zset_key, ZAddFlags::Default(), &member_scores, &ret);
  zset->Expire(expired_zset_key, 1);  // expired
  usleep(10000);

  status = storage_->Compact(nullptr, nullptr);
  assert(status.ok());

  iter = NewIterator("default");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }

  iter = NewIterator("zset_score");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    EXPECT_TRUE(false);  // never reach here
  }

  Slice mk_with_ttl = "mk_with_ttl";
  hash->Set(mk_with_ttl, "f1", "v1", &ret);
  hash->Set(mk_with_ttl, "f2", "v2", &ret);

  int retry = 2;
  while (retry-- > 0) {
    status = storage_->Compact(nullptr, nullptr);
    assert(status.ok());
    std::vector<FieldValue> fieldvalues;
    auto getRes = hash->GetAll(mk_with_ttl, &fieldvalues);
    auto sExpire = hash->Expire(mk_with_ttl, 1);  // expired immediately..

    if (retry == 1) {
      ASSERT_TRUE(getRes.ok());  // not expired first time
      ASSERT_TRUE(sExpire.ok());
    } else {
      ASSERT_TRUE(getRes.ok());  // expired but still return ok....
      ASSERT_EQ(0, fieldvalues.size());
      ASSERT_TRUE(sExpire.IsNotFound());
    }
    usleep(10000);
  }

  db->ReleaseSnapshot(read_options.snapshot);
  std::error_code ec;
  std::filesystem::remove_all(config.db_dir, ec);
  if (ec) {
    std::cout << "Encounter filesystem error: " << ec << std::endl;
  }
}
