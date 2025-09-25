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

#include <cstdint>
#include <filesystem>

#include "search/index_info.h"
#include "search/indexer.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "types/redis_hash.h"
#include "types/redis_timeseries.h"
#include "types/redis_zset.h"

TEST(Compact, Filter) {
  Config config;
  config.db_dir = "compactdb";
  config.slot_id_encoded = false;

  auto storage = std::make_unique<engine::Storage>(&config);
  Status s = storage->Open();
  assert(s.IsOK());

  uint64_t ret = 0;
  std::string ns = "test_compact";
  auto hash = std::make_unique<redis::Hash>(storage.get(), ns);
  std::string expired_hash_key = "expire_hash_key";
  std::string live_hash_key = "live_hash_key";

  engine::Context ctx(storage.get());

  hash->Set(ctx, expired_hash_key, "f1", "v1", &ret);
  hash->Set(ctx, expired_hash_key, "f2", "v2", &ret);
  auto st = hash->Expire(ctx, expired_hash_key, 1);  // expired
  usleep(10000);
  hash->Set(ctx, live_hash_key, "f1", "v1", &ret);
  hash->Set(ctx, live_hash_key, "f2", "v2", &ret);

  auto status = storage->Compact(nullptr, nullptr, nullptr);
  assert(status.ok());
  // Compact twice to workaround issue fixed by: https://github.com/facebook/rocksdb/pull/11468
  status = storage->Compact(nullptr, nullptr, nullptr);
  assert(status.ok());

  rocksdb::DB* db = storage->GetDB();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = db->GetSnapshot();
  read_options.fill_cache = false;

  auto new_iterator = [db, read_options, &storage](ColumnFamilyID column_family_id) {
    return std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options, storage->GetCFHandle(column_family_id)));
  };

  auto iter = new_iterator(ColumnFamilyID::Metadata);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    auto [user_ns, user_key] = ExtractNamespaceKey(iter->key(), storage->IsSlotIdEncoded());
    EXPECT_EQ(user_key.ToString(), live_hash_key);
  }

  iter = new_iterator(ColumnFamilyID::PrimarySubkey);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }

  auto zset = std::make_unique<redis::ZSet>(storage.get(), ns);
  std::string expired_zset_key = "expire_zset_key";
  std::vector<MemberScore> member_scores = {MemberScore{"z1", 1.1}, MemberScore{"z2", 0.4}};
  zset->Add(ctx, expired_zset_key, ZAddFlags::Default(), &member_scores, &ret);
  st = zset->Expire(ctx, expired_zset_key, 1);  // expired
  usleep(10000);

  // Same as the above compact, need to compact twice here
  status = storage->Compact(nullptr, nullptr, nullptr);
  EXPECT_TRUE(status.ok());
  status = storage->Compact(nullptr, nullptr, nullptr);
  EXPECT_TRUE(status.ok());

  iter = new_iterator(ColumnFamilyID::PrimarySubkey);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }

  iter = new_iterator(ColumnFamilyID::SecondarySubkey);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    EXPECT_TRUE(false);  // never reach here
  }

  Slice mk_with_ttl = "mk_with_ttl";
  hash->Set(ctx, mk_with_ttl, "f1", "v1", &ret);
  hash->Set(ctx, mk_with_ttl, "f2", "v2", &ret);

  int retry = 2;
  while (retry-- > 0) {
    status = storage->Compact(nullptr, nullptr, nullptr);
    ASSERT_TRUE(status.ok());
    std::vector<FieldValue> fieldvalues;
    auto get_res = hash->GetAll(ctx, mk_with_ttl, &fieldvalues);
    auto s_expire = hash->Expire(ctx, mk_with_ttl, 1);  // expired immediately..

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

TEST(Compact, SearchFilter) {
  Config config;
  config.db_dir = "compactdb";
  config.slot_id_encoded = false;

  auto storage = std::make_unique<engine::Storage>(&config);
  auto s = storage->Open();
  assert(s.IsOK());

  uint64_t ret = 0;
  std::string ns = "test_compact_search";
  auto hash = std::make_unique<redis::Hash>(storage.get(), ns);

  redis::IndexMetadata hash_field_meta;
  hash_field_meta.on_data_type = redis::IndexOnDataType::HASH;

  auto hash_info = std::make_unique<kqir::IndexInfo>("hashtest", hash_field_meta, ns);
  hash_info->Add(kqir::FieldInfo("f1", std::make_unique<redis::TagFieldMetadata>()));
  hash_info->Add(kqir::FieldInfo("f2", std::make_unique<redis::NumericFieldMetadata>()));

  redis::GlobalIndexer indexer(storage.get());
  kqir::IndexMap map;
  map.Insert(std::move(hash_info));

  auto hash_updater = std::make_unique<redis::IndexUpdater>(map.at(ComposeNamespaceKey(ns, "hashtest", false)).get());
  indexer.Add(std::move(hash_updater));

  engine::Context ctx(storage.get());
  std::string hash_key = "hash_key";

  auto sr = indexer.Record(ctx, hash_key, ns);
  ASSERT_EQ(sr.Msg(), Status::ok_msg);
  auto record = *sr;

  hash->Set(ctx, hash_key, "f1", "hello", &ret);
  hash->Set(ctx, hash_key, "f2", "233", &ret);

  auto su = indexer.Update(ctx, record);
  ASSERT_TRUE(su);

  auto tag_search_key = redis::SearchKey(ns, "hashtest", "f1").ConstructTagFieldData("hello", hash_key);
  std::string search_value;
  auto sg = storage->Get(ctx, rocksdb::ReadOptions(), storage->GetCFHandle(ColumnFamilyID::Search), tag_search_key,
                         &search_value);
  ASSERT_TRUE(sg.ok());

  auto num_search_key = redis::SearchKey(ns, "hashtest", "f2").ConstructNumericFieldData(233, hash_key);
  sg = storage->Get(ctx, rocksdb::ReadOptions(), storage->GetCFHandle(ColumnFamilyID::Search), num_search_key,
                    &search_value);
  ASSERT_TRUE(sg.ok());

  auto st = hash->Expire(ctx, hash_key, 1);

  ASSERT_TRUE(storage->Compact(nullptr, nullptr, nullptr).ok());

  sg = storage->Get(ctx, rocksdb::ReadOptions(), storage->GetCFHandle(ColumnFamilyID::Search), tag_search_key,
                    &search_value);
  ASSERT_TRUE(sg.IsNotFound());

  sg = storage->Get(ctx, rocksdb::ReadOptions(), storage->GetCFHandle(ColumnFamilyID::Search), num_search_key,
                    &search_value);
  ASSERT_TRUE(sg.IsNotFound());

  std::error_code ec;
  std::filesystem::remove_all(config.db_dir, ec);
  if (ec) {
    std::cout << "Encounter filesystem error: " << ec << std::endl;
  }
}

TEST(Compact, IndexFilter) {
  Config config;
  config.db_dir = "compactdb";
  config.slot_id_encoded = false;

  auto storage = std::make_unique<engine::Storage>(&config);
  auto s = storage->Open();
  assert(s.IsOK());

  std::string ns = "test_compact_index";
  auto timeseries = std::make_unique<redis::TimeSeries>(storage.get(), ns);
  engine::Context ctx(storage.get());

  std::string ts_del_key = "ts_del_key";
  std::string ts_expire_key = "ts_expire_key";
  std::string ts_keep_key = "ts_keep_key";
  auto create_option = redis::TSCreateOption();
  create_option.labels.push_back({"flag", "temp"});
  ASSERT_TRUE(timeseries->Create(ctx, ts_del_key, create_option).ok());
  ASSERT_TRUE(timeseries->Create(ctx, ts_expire_key, create_option).ok());
  ASSERT_TRUE(timeseries->Create(ctx, ts_keep_key, create_option).ok());

  redis::TSMGetOption mget_option;
  mget_option.filter.labels_equals["flag"].insert("temp");
  std::vector<redis::TSMGetResult> mget_result;
  ASSERT_TRUE(timeseries->MGet(ctx, mget_option, false, &mget_result).ok());
  ASSERT_EQ(mget_result.size(), 3);
  ASSERT_EQ(mget_result[0].name, ts_del_key);
  ASSERT_EQ(mget_result[1].name, ts_expire_key);
  ASSERT_EQ(mget_result[2].name, ts_keep_key);

  std::string ns_del_key = ComposeNamespaceKey(ns, ts_del_key, false);
  ASSERT_TRUE(
      storage->Delete(ctx, storage->DefaultWriteOptions(), storage->GetCFHandle(ColumnFamilyID::Metadata), ns_del_key)
          .ok());
  ASSERT_TRUE(timeseries->Expire(ctx, ts_expire_key, 1).ok());

  ASSERT_TRUE(storage->Compact(nullptr, nullptr, nullptr).ok());

  ASSERT_TRUE(timeseries->MGet(ctx, mget_option, false, &mget_result).ok());
  ASSERT_EQ(mget_result.size(), 1);
  ASSERT_EQ(mget_result[0].name, ts_keep_key);

  std::error_code ec;
  std::filesystem::remove_all(config.db_dir, ec);
  if (ec) {
    std::cout << "Encounter filesystem error: " << ec << std::endl;
  }
}

TEST(Compact, TSRetention) {
  Config config;
  config.db_dir = "compactdb_tsretention";
  config.slot_id_encoded = false;

  auto storage = std::make_unique<engine::Storage>(&config);
  auto s = storage->Open();
  assert(s.IsOK());

  std::string ns = "test_compact_tsretention";
  auto timeseries = std::make_unique<redis::TimeSeries>(storage.get(), ns);
  engine::Context ctx(storage.get());

  std::string ts_key = "ts_key";
  redis::TSCreateOption create_option;
  create_option.chunk_size = 3;
  create_option.retention_time = 100;
  ASSERT_TRUE(timeseries->Create(ctx, ts_key, create_option).ok());

  rocksdb::DB* db = storage->GetDB();
  rocksdb::ReadOptions read_options;
  read_options.fill_cache = false;
  auto get_all_chunks = [&]() {
    auto iter = std::unique_ptr<rocksdb::Iterator>(
        db->NewIterator(read_options, storage->GetCFHandle(ColumnFamilyID::PrimarySubkey)));
    std::vector<uint64_t> chunk_ids;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Slice slice(iter->key());
      slice.remove_prefix(slice.size() - sizeof(uint64_t));
      uint64_t chunk_id = 0;
      GetFixed64(&slice, &chunk_id);
      chunk_ids.push_back(chunk_id);
    }
    return chunk_ids;
  };

  // Add two chunk
  std::vector<TSSample> samples = {{1, 1.0}, {2, 2.0}, {3, 3.0}, {4, 4.0}, {5, 5.0}, {10, 10.0}};
  std::vector<TSChunk::AddResult> add_results;
  ASSERT_TRUE(timeseries->MAdd(ctx, ts_key, samples, &add_results).ok());

  // There should be two chunk key
  auto chunk_ids = get_all_chunks();
  ASSERT_EQ(chunk_ids.size(), 2);
  ASSERT_EQ(chunk_ids[0], 1);
  ASSERT_EQ(chunk_ids[1], 4);

  // Add a sample to make last_timestamp = 110, then the first chunk is expired
  samples = {{110, 110.0}};
  ASSERT_TRUE(timeseries->MAdd(ctx, ts_key, samples, &add_results).ok());
  ASSERT_TRUE(storage->Compact(nullptr, nullptr, nullptr).ok());

  // Check the first chunk is deleted
  chunk_ids = get_all_chunks();
  ASSERT_EQ(chunk_ids.size(), 2);
  ASSERT_EQ(chunk_ids[0], 4);
  ASSERT_EQ(chunk_ids[1], 110);

  // Check samples should be kept
  redis::TSRangeOption range_option;
  std::vector<TSSample> range_result;
  ASSERT_TRUE(timeseries->Range(ctx, ts_key, range_option, &range_result).ok());
  ASSERT_EQ(range_result.size(), 2);
  ASSERT_EQ(range_result[0].ts, 10);
  ASSERT_EQ(range_result[1].ts, 110);

  std::error_code ec;
  std::filesystem::remove_all(config.db_dir, ec);
  if (ec) {
    std::cout << "Encounter filesystem error: " << ec << std::endl;
  }
}
