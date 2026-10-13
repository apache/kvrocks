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
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>

#include "encoding.h"
#include "search/index_info.h"
#include "search/indexer.h"
#include "storage/compact_filter.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "time_util.h"
#include "types/redis_hash.h"
#include "types/redis_timeseries.h"
#include "types/redis_zset.h"

namespace {

class CompactHashFieldExpirationTest : public ::testing::Test {
 public:
  CompactHashFieldExpirationTest(const CompactHashFieldExpirationTest &) = delete;
  CompactHashFieldExpirationTest &operator=(const CompactHashFieldExpirationTest &) = delete;
  CompactHashFieldExpirationTest(CompactHashFieldExpirationTest &&) = delete;
  CompactHashFieldExpirationTest &operator=(CompactHashFieldExpirationTest &&) = delete;

 protected:
  CompactHashFieldExpirationTest() {
    const char *path = "compact_hash_field_expiration.conf";
    unlink(path);
    std::ofstream output_file(path, std::ios::out);
    output_file << "hash-encoding-mode field-expiration\n";
    output_file.close();

    auto s = config_.Load(CLIOptions(path));
    assert(s.IsOK());
    config_.db_dir = "compactdb_hash_field_expiration";
    config_.slot_id_encoded = false;
    config_.rocks_db.compression = rocksdb::CompressionType::kNoCompression;
    config_.rocks_db.write_buffer_size = 1;
    config_.rocks_db.block_size = 100;

    storage_ = std::make_unique<engine::Storage>(&config_);
    s = storage_->Open();
    assert(s.IsOK());

    ctx_ = std::make_unique<engine::Context>(storage_.get());
    db_ = std::make_unique<redis::Database>(storage_.get(), ns_);
    hash_ = std::make_unique<redis::Hash>(storage_.get(), ns_);
  }

  ~CompactHashFieldExpirationTest() override {
    ctx_.reset();
    db_.reset();
    hash_.reset();
    storage_.reset();

    std::error_code ec;
    std::filesystem::remove_all(config_.db_dir, ec);
    unlink("compact_hash_field_expiration.conf");
  }

  HashMetadata hashMetadataOf(const std::string &key) {
    HashMetadata metadata(false);
    auto s = db_->GetMetadata(*ctx_, {kRedisHash}, db_->AppendNamespacePrefix(key), &metadata);
    assert(s.ok());
    return metadata;
  }

  std::string hashSubKey(const std::string &key, const std::string &field) {
    HashMetadata metadata = hashMetadataOf(key);
    return InternalKey(db_->AppendNamespacePrefix(key), field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  }

  rocksdb::Status getRawHashValue(const std::string &key, const std::string &field, std::string *value) {
    return storage_->Get(*ctx_, ctx_->GetReadOptions(), hashSubKey(key, field), value);
  }

  rocksdb::Status getRawSubKeyValue(const std::string &sub_key, std::string *value) {
    return storage_->Get(*ctx_, ctx_->GetReadOptions(), sub_key, value);
  }

  rocksdb::Status putRawHashValue(const std::string &key, const std::string &field, const std::string &value) {
    auto batch = storage_->GetWriteBatchBase();
    auto s = batch->Put(hashSubKey(key, field), value);
    if (!s.ok()) return s;
    return storage_->Write(*ctx_, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }

  rocksdb::Status getRawMetadata(const std::string &key, std::string *value) {
    return storage_->Get(*ctx_, ctx_->GetReadOptions(), storage_->GetCFHandle(ColumnFamilyID::Metadata),
                         db_->AppendNamespacePrefix(key), value);
  }

  void compactTwice() {
    auto s = storage_->Compact(nullptr, nullptr, nullptr);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = storage_->Compact(nullptr, nullptr, nullptr);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  Config config_;
  std::string ns_ = "test_compact_hfe";
  std::unique_ptr<engine::Storage> storage_;
  std::unique_ptr<engine::Context> ctx_;
  std::unique_ptr<redis::Database> db_;
  std::unique_ptr<redis::Hash> hash_;
};

class SlotCompactionTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    config_.db_dir = "compactdb_slot_protection";
    config_.slot_id_encoded = true;
    config_.rocks_db.disable_auto_compactions = true;
    config_.rocks_db.enable_blob_files = GetParam();
    config_.rocks_db.min_blob_size = 0;
    storage_ = std::make_unique<engine::Storage>(&config_);
    ASSERT_TRUE(storage_->Open().IsOK());
  }

  void TearDown() override {
    storage_.reset();
    std::error_code ec;
    std::filesystem::remove_all(config_.db_dir, ec);
  }

  static std::string metadataKey(const std::string &key) { return ComposeNamespaceKey("ns", key, true); }

  void putMetadata(const std::string &key, const Metadata &metadata) {
    std::string value;
    metadata.Encode(&value);
    ASSERT_TRUE(
        storage_->GetDB()
            ->Put(rocksdb::WriteOptions(), storage_->GetCFHandle(ColumnFamilyID::Metadata), metadataKey(key), value)
            .ok());
  }

  void put(ColumnFamilyID cf, const std::string &key, const std::string &value) {
    ASSERT_TRUE(storage_->GetDB()->Put(rocksdb::WriteOptions(), storage_->GetCFHandle(cf), key, value).ok());
  }

  rocksdb::Status get(ColumnFamilyID cf, const std::string &key, const rocksdb::Snapshot *snapshot = nullptr) {
    rocksdb::ReadOptions options;
    options.snapshot = snapshot;
    std::string value;
    return storage_->GetDB()->Get(options, storage_->GetCFHandle(cf), key, &value);
  }

  void compact() {
    // Compact metadata first to also exercise orphan subkey cleanup.
    ASSERT_TRUE(storage_->Compact(storage_->GetCFHandle(ColumnFamilyID::Metadata), nullptr, nullptr).ok());
    ASSERT_TRUE(storage_->Compact(nullptr, nullptr, nullptr).ok());
  }

  Config config_;
  std::unique_ptr<engine::Storage> storage_;
};

}  // namespace

TEST_P(SlotCompactionTest, ProtectedSlotsKeepExpiredRecords) {
  const std::string protected_key = "{protected}:list";
  const std::string other_key = "{other}:list";
  const auto slot = ExtractSlotId(metadataKey(protected_key));
  ASSERT_NE(slot, ExtractSlotId(metadataKey(other_key)));
  auto guard = storage_->ProtectSlotRange(SlotRange::GetPoint(slot));
  ListMetadata metadata;
  metadata.expire = 1;
  metadata.size = 1;
  const std::array subkey_cfs = {ColumnFamilyID::PrimarySubkey, ColumnFamilyID::SecondarySubkey,
                                 ColumnFamilyID::Stream};
  for (const auto &key : {protected_key, other_key}) {
    putMetadata(key, metadata);
    auto subkey = InternalKey(metadataKey(key), "element", metadata.version, true).Encode();
    for (auto cf : subkey_cfs) put(cf, subkey, "value");
    put(ColumnFamilyID::Index, redis::TSRevLabelKey("ns", "label", "value", key).Encode(), "");
  }

  compact();
  for (const auto &key : {protected_key, other_key}) {
    const bool kept = key == protected_key;
    EXPECT_EQ(get(ColumnFamilyID::Metadata, metadataKey(key)).ok(), kept);
    auto subkey = InternalKey(metadataKey(key), "element", metadata.version, true).Encode();
    for (auto cf : subkey_cfs) EXPECT_EQ(get(cf, subkey).ok(), kept);
    EXPECT_EQ(get(ColumnFamilyID::Index, redis::TSRevLabelKey("ns", "label", "value", key).Encode()).ok(), kept);
  }

  guard.reset();
  compact();
  EXPECT_TRUE(get(ColumnFamilyID::Metadata, metadataKey(protected_key)).IsNotFound());
  auto subkey = InternalKey(metadataKey(protected_key), "element", metadata.version, true).Encode();
  for (auto cf : subkey_cfs) EXPECT_TRUE(get(cf, subkey).IsNotFound());
  EXPECT_TRUE(
      get(ColumnFamilyID::Index, redis::TSRevLabelKey("ns", "label", "value", protected_key).Encode()).IsNotFound());
}

TEST_P(SlotCompactionTest, SourceSnapshotKeepsReplacedSubkeys) {
  const std::string key = "list";
  auto guard = storage_->ProtectSlotRange(SlotRange::GetPoint(ExtractSlotId(metadataKey(key))));
  ASSERT_TRUE(guard->WaitForCompactions().IsOK());
  ListMetadata metadata;
  metadata.size = 1;
  putMetadata(key, metadata);
  auto subkey = InternalKey(metadataKey(key), "element", metadata.version, true).Encode();
  put(ColumnFamilyID::PrimarySubkey, subkey, "snapshot value");
  auto snapshot = std::shared_ptr<const rocksdb::Snapshot>(
      storage_->GetDB()->GetSnapshot(), [this](const auto *snapshot) { storage_->GetDB()->ReleaseSnapshot(snapshot); });

  // Replacing a key makes its previous subkeys obsolete for current reads, but
  // migration still needs them when it opens a subkey iterator at the snapshot.
  ++metadata.version;
  putMetadata(key, metadata);
  compact();
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.get();
  auto iter = std::unique_ptr<rocksdb::Iterator>(
      storage_->GetDB()->NewIterator(options, storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey)));
  iter->Seek(subkey);
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key().ToString(), subkey);
  EXPECT_EQ(iter->value().ToString(), "snapshot value");
  iter.reset();
  snapshot.reset();
  guard.reset();
  compact();
  EXPECT_TRUE(get(ColumnFamilyID::PrimarySubkey, subkey).IsNotFound());
}

TEST_P(SlotCompactionTest, ImportKeepsBaselineUntilMetadataTTLIsExtended) {
  const std::string key = "list";
  auto guard = storage_->ProtectSlotRange(SlotRange::GetPoint(ExtractSlotId(metadataKey(key))));
  ListMetadata metadata;
  metadata.expire = 1;
  metadata.size = 1;
  // The snapshot arrives with its old TTL, before the WAL extends it. No user
  // command can recreate the missing elements if compaction removes them here.
  putMetadata(key, metadata);
  auto subkey = InternalKey(metadataKey(key), "element", metadata.version, true).Encode();
  put(ColumnFamilyID::PrimarySubkey, subkey, "snapshot value");
  compact();
  metadata.expire = 0;
  putMetadata(key, metadata);
  guard.reset();
  compact();
  EXPECT_TRUE(get(ColumnFamilyID::Metadata, metadataKey(key)).ok());
  EXPECT_TRUE(get(ColumnFamilyID::PrimarySubkey, subkey).ok());
}

TEST_P(SlotCompactionTest, GuardsAreReferenceCounted) {
  auto first_slot = storage_->ProtectSlotRange(SlotRange::GetPoint(0));
  auto last_slot = storage_->ProtectSlotRange(SlotRange::GetPoint(kClusterSlots - 1));
  EXPECT_TRUE(storage_->IsSlotCompactionProtected(0));
  EXPECT_TRUE(storage_->IsSlotCompactionProtected(kClusterSlots - 1));
  auto first = storage_->ProtectSlotRange({10, 12});
  auto second = storage_->ProtectSlotRange({12, 14});
  EXPECT_FALSE(storage_->IsSlotCompactionProtected(9));
  EXPECT_TRUE(storage_->IsSlotCompactionProtected(10));
  EXPECT_TRUE(storage_->IsSlotCompactionProtected(14));
  EXPECT_FALSE(storage_->IsSlotCompactionProtected(kClusterSlots));
  first.reset();
  EXPECT_FALSE(storage_->IsSlotCompactionProtected(10));
  EXPECT_TRUE(storage_->IsSlotCompactionProtected(12));
  second.reset();
  EXPECT_FALSE(storage_->IsSlotCompactionProtected(12));
}

TEST_P(SlotCompactionTest, WaitsOnlyForPreexistingCompactions) {
  storage_->OnCompactionBegin(1, 1);
  auto guard = storage_->ProtectSlotRange({10, 12});
  // Job IDs are only unique within a thread.
  storage_->OnCompactionBegin(2, 1);
  std::atomic<bool> cancel = false;
  std::promise<void> waiting;
  auto entered = waiting.get_future();
  auto result = std::async(std::launch::async, [&] {
    bool notified = false;
    return guard->WaitForCompactions([&] {
      if (!notified) {
        waiting.set_value();
        notified = true;
      }
      return cancel.load();
    });
  });
  EXPECT_EQ(entered.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(result.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
  storage_->OnCompactionCompleted(1, 1);
  auto status = result.wait_for(std::chrono::seconds(5));
  cancel = true;
  EXPECT_EQ(status, std::future_status::ready);
  EXPECT_TRUE(result.get().IsOK());
  storage_->OnCompactionCompleted(2, 1);
}

TEST_P(SlotCompactionTest, WaitingCanBeCancelledAndDoesNotSurviveReopen) {
  storage_->OnCompactionBegin(1, 1);
  auto guard = storage_->ProtectSlotRange({10, 12});
  std::atomic<bool> cancel = false;
  auto result =
      std::async(std::launch::async, [&] { return guard->WaitForCompactions([&] { return cancel.load(); }); });
  EXPECT_EQ(result.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
  cancel = true;
  EXPECT_EQ(result.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_FALSE(result.get().IsOK());
  storage_->CloseDB();
  EXPECT_FALSE(guard->WaitForCompactions().IsOK());
  ASSERT_TRUE(storage_->Open().IsOK());
  // A waiter that did not observe CloseDB must not succeed on a different DB.
  EXPECT_FALSE(guard->WaitForCompactions().IsOK());
  EXPECT_FALSE(storage_->IsSlotCompactionProtected(10));
  auto reopened_guard = storage_->ProtectSlotRange({10, 12});
  guard.reset();
  EXPECT_TRUE(storage_->IsSlotCompactionProtected(10));
  EXPECT_TRUE(reopened_guard->WaitForCompactions().IsOK());
}

INSTANTIATE_TEST_SUITE_P(InlineAndBlobValues, SlotCompactionTest, ::testing::Bool());

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

  rocksdb::DB *db = storage->GetDB();
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

TEST_F(CompactHashFieldExpirationTest, DropsExpiredTTLSubkeyWithoutChangingMetadata) {
  const std::string key = "hfe_compact_hash";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"persistent", "1"}, {"live", "2"}, {"expired", "3"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 3);

  uint64_t now = util::GetTimeStampMS();
  uint64_t expired_at = now + 200;
  uint64_t live_expire_at = now + 60'000;
  std::vector<int64_t> results;
  s = hash_->ExpireFields(*ctx_, key, {"live"}, live_expire_at, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = hash_->ExpireFields(*ctx_, key, {"expired"}, expired_at, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();

  HashMetadata before = hashMetadataOf(key);
  ASSERT_EQ(before.size, 3);
  ASSERT_EQ(before.persist, 1);
  ASSERT_LT(before.lower, before.upper);
  usleep(250 * 1000);
  ASSERT_LT(expired_at, util::GetTimeStampMS());

  std::string raw_value;
  ASSERT_TRUE(getRawHashValue(key, "persistent", &raw_value).ok());
  ASSERT_TRUE(getRawHashValue(key, "live", &raw_value).ok());
  ASSERT_TRUE(getRawHashValue(key, "expired", &raw_value).ok());

  engine::SubKeyFilter filter(storage_.get());
  EXPECT_EQ(filter.FilterBlobByKey(0, hashSubKey(key, "expired"), nullptr, nullptr),
            rocksdb::CompactionFilter::Decision::kUndetermined);

  compactTwice();

  EXPECT_TRUE(getRawHashValue(key, "persistent", &raw_value).ok());
  EXPECT_TRUE(getRawHashValue(key, "live", &raw_value).ok());
  EXPECT_TRUE(getRawHashValue(key, "expired", &raw_value).IsNotFound());

  HashMetadata after = hashMetadataOf(key);
  EXPECT_EQ(after.size, before.size);
  EXPECT_EQ(after.persist, before.persist);
  EXPECT_EQ(after.lower, before.lower);
  EXPECT_EQ(after.upper, before.upper);
  EXPECT_EQ(after.version, before.version);
}

TEST_F(CompactHashFieldExpirationTest, DropsWholeHashWhenAllTTLFieldsExpiredByBounds) {
  const std::string key = "hfe_compact_whole_hash";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"first", "1"}, {"second", "2"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 2);

  uint64_t expire_at = util::GetTimeStampMS() + 200;
  std::vector<int64_t> results;
  s = hash_->ExpireFields(*ctx_, key, {"first", "second"}, expire_at, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();

  HashMetadata before = hashMetadataOf(key);
  ASSERT_TRUE(before.IsFieldExpirationEncoding());
  ASSERT_EQ(before.size, 2);
  ASSERT_EQ(before.persist, 0);
  ASSERT_EQ(before.lower, expire_at);
  ASSERT_EQ(before.upper, expire_at);

  usleep(250 * 1000);
  ASSERT_LT(before.upper, util::GetTimeStampMS());

  std::string raw_value;
  std::string first_sub_key = hashSubKey(key, "first");
  std::string second_sub_key = hashSubKey(key, "second");
  ASSERT_TRUE(getRawMetadata(key, &raw_value).ok());
  ASSERT_TRUE(getRawSubKeyValue(first_sub_key, &raw_value).ok());
  ASSERT_TRUE(getRawSubKeyValue(second_sub_key, &raw_value).ok());

  compactTwice();

  EXPECT_TRUE(getRawMetadata(key, &raw_value).IsNotFound());
  EXPECT_TRUE(getRawSubKeyValue(first_sub_key, &raw_value).IsNotFound());
  EXPECT_TRUE(getRawSubKeyValue(second_sub_key, &raw_value).IsNotFound());
}

TEST_F(CompactHashFieldExpirationTest, KeepsHashMetadataWhenPersistentFieldExistsPastUpperBound) {
  const std::string key = "hfe_compact_persistent_survivor";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"persistent", "1"}, {"expired", "2"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 2);

  uint64_t expire_at = util::GetTimeStampMS() + 200;
  std::vector<int64_t> results;
  s = hash_->ExpireFields(*ctx_, key, {"expired"}, expire_at, HashFieldExpireCondition::kNone, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();

  HashMetadata before = hashMetadataOf(key);
  ASSERT_TRUE(before.IsFieldExpirationEncoding());
  ASSERT_EQ(before.size, 2);
  ASSERT_EQ(before.persist, 1);
  ASSERT_EQ(before.lower, expire_at);
  ASSERT_EQ(before.upper, expire_at);

  usleep(250 * 1000);
  ASSERT_LT(before.upper, util::GetTimeStampMS());

  compactTwice();

  std::string raw_value;
  EXPECT_TRUE(getRawMetadata(key, &raw_value).ok());
  EXPECT_TRUE(getRawHashValue(key, "persistent", &raw_value).ok());
  EXPECT_TRUE(getRawHashValue(key, "expired", &raw_value).IsNotFound());

  HashMetadata after = hashMetadataOf(key);
  EXPECT_EQ(after.size, before.size);
  EXPECT_EQ(after.persist, before.persist);
  EXPECT_EQ(after.lower, before.lower);
  EXPECT_EQ(after.upper, before.upper);
  EXPECT_EQ(after.version, before.version);
}

TEST(Compact, KeepsLegacyHashSubkeyWithTTLLikePrefix) {
  Config config;
  config.db_dir = "compactdb_legacy_hash_ttl_like_prefix";
  config.slot_id_encoded = false;
  config.rocks_db.compression = rocksdb::CompressionType::kNoCompression;
  config.rocks_db.write_buffer_size = 1;
  config.rocks_db.block_size = 100;

  auto storage = std::make_unique<engine::Storage>(&config);
  Status s = storage->Open();
  ASSERT_TRUE(s.IsOK()) << s.Msg();

  std::string ns = "test_compact_legacy_hash";
  engine::Context ctx(storage.get());
  redis::Database db(storage.get(), ns);
  redis::Hash hash(storage.get(), ns);

  std::string key = "legacy_hash";
  std::string field = "field";
  std::string value;
  PutFixed64(&value, 1);
  value.append("legacy-value");
  uint64_t ret = 0;
  auto rs = hash.Set(ctx, key, field, value, &ret);
  ASSERT_TRUE(rs.ok()) << rs.ToString();

  HashMetadata metadata(false);
  rs = db.GetMetadata(ctx, {kRedisHash}, db.AppendNamespacePrefix(key), &metadata);
  ASSERT_TRUE(rs.ok()) << rs.ToString();
  ASSERT_TRUE(metadata.IsLegacySubkeyEncoding());
  std::string sub_key =
      InternalKey(db.AppendNamespacePrefix(key), field, metadata.version, storage->IsSlotIdEncoded()).Encode();

  engine::SubKeyFilter filter(storage.get());
  EXPECT_EQ(filter.FilterBlobByKey(0, sub_key, nullptr, nullptr), rocksdb::CompactionFilter::Decision::kKeep);

  auto status = storage->Compact(nullptr, nullptr, nullptr);
  ASSERT_TRUE(status.ok()) << status.ToString();
  status = storage->Compact(nullptr, nullptr, nullptr);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::string raw_value;
  rs = storage->Get(ctx, ctx.GetReadOptions(), sub_key, &raw_value);
  EXPECT_TRUE(rs.ok()) << rs.ToString();
  EXPECT_EQ(raw_value, value);

  storage.reset();
  std::error_code ec;
  std::filesystem::remove_all(config.db_dir, ec);
  if (ec) {
    std::cout << "Encounter filesystem error: " << ec << std::endl;
  }
}

TEST_F(CompactHashFieldExpirationTest, KeepsMalformedFieldExpirationSubkeyValue) {
  const std::string key = "hfe_compact_malformed";
  uint64_t ret = 0;
  auto s = hash_->MSet(*ctx_, key, {{"field", "value"}}, false, &ret);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, 1);
  std::vector<int64_t> results;
  s = hash_->ExpireFields(*ctx_, key, {"field"}, util::GetTimeStampMS() + 60'000, HashFieldExpireCondition::kNone,
                          &results);
  ASSERT_TRUE(s.ok()) << s.ToString();

  HashMetadata before = hashMetadataOf(key);
  ASSERT_TRUE(before.IsFieldExpirationEncoding());
  ASSERT_EQ(before.persist, 0);

  s = putRawHashValue(key, "field", "short");
  ASSERT_TRUE(s.ok()) << s.ToString();

  compactTwice();

  std::string raw_value;
  EXPECT_TRUE(getRawHashValue(key, "field", &raw_value).ok());
  EXPECT_EQ(raw_value, "short");

  HashMetadata after = hashMetadataOf(key);
  EXPECT_EQ(after.size, before.size);
  EXPECT_EQ(after.persist, before.persist);
  EXPECT_EQ(after.lower, before.lower);
  EXPECT_EQ(after.upper, before.upper);
  EXPECT_EQ(after.version, before.version);
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

  rocksdb::DB *db = storage->GetDB();
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

TEST(Compact, TSDownstreamSubKey) {
  Config config;
  config.db_dir = "compactdb_tsdownstream";
  config.slot_id_encoded = false;

  auto storage = std::make_unique<engine::Storage>(&config);
  auto s = storage->Open();
  assert(s.IsOK());

  std::string ns = "test_compact_tsdownstream";
  auto timeseries = std::make_unique<redis::TimeSeries>(storage.get(), ns);
  engine::Context ctx(storage.get());

  rocksdb::DB *db = storage->GetDB();
  rocksdb::ReadOptions read_options;
  read_options.fill_cache = false;
  auto get_all_ds_key = [&]() {
    auto iter = std::unique_ptr<rocksdb::Iterator>(
        db->NewIterator(read_options, storage->GetCFHandle(ColumnFamilyID::PrimarySubkey)));
    std::vector<std::string> ds_keys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Slice slice(iter->key());
      slice.remove_prefix(slice.size() - sizeof(uint64_t));
      ds_keys.push_back(slice.ToString());
    }
    return ds_keys;
  };

  std::string ts_key = "ts_key";
  std::string dst_key1 = "dst_key1";
  std::string dst_key2 = "dst_key2";
  redis::TSCreateOption create_option;
  ASSERT_TRUE(timeseries->Create(ctx, ts_key, create_option).ok());
  ASSERT_TRUE(timeseries->Create(ctx, dst_key1, create_option).ok());
  ASSERT_TRUE(timeseries->Create(ctx, dst_key2, create_option).ok());

  // Create two downstream rule
  redis::TSAggregator agg;
  agg.type = redis::TSAggregatorType::AVG;
  agg.bucket_duration = 100;
  auto rule_res = redis::TSCreateRuleResult::kOK;
  ASSERT_TRUE(timeseries->CreateRule(ctx, ts_key, dst_key1, agg, &rule_res).ok());
  ASSERT_TRUE(timeseries->CreateRule(ctx, ts_key, dst_key2, agg, &rule_res).ok());

  auto ds_keys = get_all_ds_key();
  ASSERT_EQ(ds_keys.size(), 2);
  ASSERT_EQ(ds_keys[0], dst_key1);
  ASSERT_EQ(ds_keys[1], dst_key2);

  // Recreate the downstream key
  ASSERT_TRUE(static_cast<redis::Database *>(timeseries.get())->Del(ctx, dst_key1).ok());
  ASSERT_TRUE(timeseries->Create(ctx, dst_key1, create_option).ok());
  ASSERT_TRUE(storage->Compact(nullptr, nullptr, nullptr).ok());
  ds_keys = get_all_ds_key();
  ASSERT_EQ(ds_keys.size(), 1);
  ASSERT_EQ(ds_keys[0], dst_key2);

  std::error_code ec;
  std::filesystem::remove_all(config.db_dir, ec);
  if (ec) {
    std::cout << "Encounter filesystem error: " << ec << std::endl;
  }
}
