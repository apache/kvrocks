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

  auto storage = std::make_unique<engine::Storage>(&config);
  Status s = storage->Open();
  assert(s.IsOK());

  uint64_t ret = 0;
  std::string ns = "test_compact";
  auto hash = std::make_unique<redis::Hash>(storage.get(), ns);
  std::string expired_hash_key = "expire_hash_key";
  std::string live_hash_key = "live_hash_key";
  hash->Set(expired_hash_key, "f1", "v1", &ret);
  hash->Set(expired_hash_key, "f2", "v2", &ret);
  auto st = hash->Expire(expired_hash_key, 1);  // expired
  usleep(10000);
  hash->Set(live_hash_key, "f1", "v1", &ret);
  hash->Set(live_hash_key, "f2", "v2", &ret);

  auto status = storage->Compact(nullptr, nullptr);
  assert(status.ok());
  // Compact twice to workaround issue fixed by: https://github.com/facebook/rocksdb/pull/11468
  // before rocksdb/speedb 8.1.1. This line can be removed after speedb upgraded above 8.1.1.
  status = storage->Compact(nullptr, nullptr);
  assert(status.ok());

  rocksdb::DB* db = storage->GetDB();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = db->GetSnapshot();
  read_options.fill_cache = false;

  auto new_iterator = [db, read_options, &storage](const std::string& name) {
    return std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options, storage->GetCFHandle(name)));
  };

  auto iter = new_iterator("metadata");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    auto [user_ns, user_key] = ExtractNamespaceKey(iter->key(), storage->IsSlotIdEncoded());
    EXPECT_EQ(user_key.ToString(), live_hash_key);
  }

  iter = new_iterator("subkey");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }

  auto zset = std::make_unique<redis::ZSet>(storage.get(), ns);
  std::string expired_zset_key = "expire_zset_key";
  std::vector<MemberScore> member_scores = {MemberScore{"z1", 1.1}, MemberScore{"z2", 0.4}};
  zset->Add(expired_zset_key, ZAddFlags::Default(), &member_scores, &ret);
  st = zset->Expire(expired_zset_key, 1);  // expired
  usleep(10000);

  // Same as the above compact, need to compact twice here
  status = storage->Compact(nullptr, nullptr);
  assert(status.ok());
  status = storage->Compact(nullptr, nullptr);
  assert(status.ok());

  iter = new_iterator("default");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }

  iter = new_iterator("zset_score");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    EXPECT_TRUE(false);  // never reach here
  }

  Slice mk_with_ttl = "mk_with_ttl";
  hash->Set(mk_with_ttl, "f1", "v1", &ret);
  hash->Set(mk_with_ttl, "f2", "v2", &ret);

  int retry = 2;
  while (retry-- > 0) {
    status = storage->Compact(nullptr, nullptr);
    assert(status.ok());
    std::vector<FieldValue> fieldvalues;
    auto get_res = hash->GetAll(mk_with_ttl, &fieldvalues);
    auto s_expire = hash->Expire(mk_with_ttl, 1);  // expired immediately..

    if (retry == 1) {
      ASSERT_TRUE(get_res.ok());  // not expired first time
      ASSERT_TRUE(s_expire.ok());
    } else {
      ASSERT_TRUE(get_res.ok());  // expired but still return ok....
      ASSERT_EQ(0, fieldvalues.size());
      ASSERT_TRUE(s_expire.IsNotFound());
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
