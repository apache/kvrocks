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
 */

#include <rocksdb/snapshot.h>

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <string>

#include "cluster/replication.h"
#include "encoding.h"
#include "server/redis_reply.h"
#include "storage/redis_metadata.h"
#include "test_base.h"

class ReplicationWriteBatchTest : public TestBase {
 protected:
  std::string metadataKey(const std::string &ns, const std::string &key) {
    return ComposeNamespaceKey(ns, key, storage_->IsSlotIdEncoded());
  }

  std::string entryKey(const std::string &ns, const std::string &key, uint64_t version = 42, uint64_t ms = 2) {
    std::string id;
    PutFixed64(&id, ms);
    PutFixed64(&id, 0);
    return InternalKey(metadataKey(ns, key), id, version, storage_->IsSlotIdEncoded()).Encode();
  }

  rocksdb::Status putMetadata(const std::string &ns, const std::string &key, const Metadata &metadata) {
    std::string bytes;
    metadata.Encode(&bytes);
    return storage_->GetDB()->Put(storage_->DefaultWriteOptions(), storage_->GetCFHandle(ColumnFamilyID::Metadata),
                                  metadataKey(ns, key), bytes);
  }

  rocksdb::Status putStream(const std::string &ns, const std::string &key, uint64_t version = 42) {
    StreamMetadata metadata(false);
    metadata.version = version;
    metadata.size = 1;
    std::string bytes;
    metadata.Encode(&bytes);
    rocksdb::WriteBatch batch;
    auto s = batch.Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), metadataKey(ns, key), bytes);
    if (!s.ok()) return s;
    s = batch.Put(storage_->GetCFHandle(ColumnFamilyID::Stream), entryKey(ns, key, version), "entry-value");
    if (!s.ok()) return s;
    return storage_->GetDB()->Write(storage_->DefaultWriteOptions(), &batch);
  }

  StatusOr<bool> visibility(const std::string &encoded, const rocksdb::ReadOptions &options) {
    InternalKey key(encoded, storage_->IsSlotIdEncoded());
    return ReplicationThread::isStreamEntryVisible(storage_.get(), storage_->GetDB(), options, key);
  }

  void expectVisible(const std::string &encoded, const rocksdb::ReadOptions &options, bool expected) {
    auto result = visibility(encoded, options);
    ASSERT_TRUE(result.IsOK()) << result.Msg();
    EXPECT_EQ(*result, expected);
  }

  void expectVisible(const std::string &encoded, bool expected) {
    rocksdb::ManagedSnapshot snapshot(storage_->GetDB());
    ASSERT_NE(snapshot.snapshot(), nullptr);
    auto options = storage_->DefaultScanOptions();
    options.snapshot = snapshot.snapshot();
    expectVisible(encoded, options, expected);
  }

  static void expectUpdates(const WriteBatchHandler &handler,
                            std::initializer_list<WriteBatchHandler::Update> expected) {
    const auto &updates = handler.GetUpdates();
    ASSERT_EQ(updates.size(), expected.size());
    size_t index = 0;
    for (const auto &update : expected) {
      SCOPED_TRACE(index);
      EXPECT_EQ(updates[index].type, update.type);
      EXPECT_EQ(updates[index].key, update.key);
      EXPECT_EQ(updates[index].value, update.value);
      ++index;
    }
  }
};

TEST_F(ReplicationWriteBatchTest, RetainsRepeatedPublicationsInOrder) {
  rocksdb::WriteBatch batch;
  auto *cf = storage_->GetCFHandle(ColumnFamilyID::PubSub);
  ASSERT_TRUE(batch.Put(cf, "channel", "same").ok());
  ASSERT_TRUE(batch.Put(cf, "channel", "same").ok());
  ASSERT_TRUE(batch.Put(cf, "other", "third").ok());
  ASSERT_TRUE(batch.Put(cf, "channel", "fourth").ok());
  WriteBatchHandler handler;
  ASSERT_TRUE(batch.Iterate(&handler).ok());
  expectUpdates(handler, {{kBatchTypePublish, "channel", "same"},
                          {kBatchTypePublish, "channel", "same"},
                          {kBatchTypePublish, "other", "third"},
                          {kBatchTypePublish, "channel", "fourth"}});
}

TEST_F(ReplicationWriteBatchTest, MixedRecordsDoNotClearEarlierEvents) {
  rocksdb::WriteBatch batch;
  auto *ordinary = storage_->GetCFHandle(ColumnFamilyID::Metadata);
  auto *pubsub = storage_->GetCFHandle(ColumnFamilyID::PubSub);
  auto *propagate = storage_->GetCFHandle(ColumnFamilyID::Propagate);
  const auto stream_key = entryKey("ns", "stream");
  const auto flush = redis::ArrayOfBulkStrings({"script", "flush"});
  ASSERT_TRUE(batch.Put(ordinary, "before", "value").ok());
  ASSERT_TRUE(batch.Put(pubsub, "channel", "first").ok());
  ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::Stream), stream_key, "entry-value").ok());
  ASSERT_TRUE(batch.Put(ordinary, "stream-metadata", "value").ok());
  ASSERT_TRUE(batch.Put(propagate, engine::kPropagateScriptCommand, flush).ok());
  ASSERT_TRUE(batch.Put(pubsub, "channel", "second").ok());
  ASSERT_TRUE(batch.Put(propagate, "unknown-propagate-key", "opaque").ok());
  ASSERT_TRUE(batch.Delete(pubsub, "channel").ok());
  ASSERT_TRUE(batch.DeleteRange(ordinary, "a", "z").ok());
  ASSERT_TRUE(batch.PutLogData("log-data").ok());
  ASSERT_TRUE(batch.Put(ordinary, "after", "value").ok());
  WriteBatchHandler handler;
  ASSERT_TRUE(batch.Iterate(&handler).ok());
  expectUpdates(handler, {{kBatchTypePublish, "channel", "first"},
                          {kBatchTypeStream, stream_key, ""},
                          {kBatchTypePropagate, engine::kPropagateScriptCommand, flush},
                          {kBatchTypePublish, "channel", "second"},
                          {kBatchTypePropagate, "unknown-propagate-key", "opaque"}});
}

TEST_F(ReplicationWriteBatchTest, EmptyOrdinaryBatchesAndFreshHandlersHaveNoEvents) {
  rocksdb::WriteBatch empty;
  WriteBatchHandler empty_handler;
  ASSERT_TRUE(empty.Iterate(&empty_handler).ok());
  EXPECT_TRUE(empty_handler.GetUpdates().empty());

  rocksdb::WriteBatch publication;
  ASSERT_TRUE(publication.Put(storage_->GetCFHandle(ColumnFamilyID::PubSub), "channel", "value").ok());
  WriteBatchHandler previous_handler;
  ASSERT_TRUE(publication.Iterate(&previous_handler).ok());
  ASSERT_EQ(previous_handler.GetUpdates().size(), 1);

  rocksdb::WriteBatch ordinary;
  auto *cf = storage_->GetCFHandle(ColumnFamilyID::Metadata);
  ASSERT_TRUE(ordinary.Put(cf, "key", "value").ok());
  ASSERT_TRUE(ordinary.Delete(cf, "key").ok());
  ASSERT_TRUE(ordinary.DeleteRange(cf, "a", "z").ok());
  ASSERT_TRUE(ordinary.PutLogData("log-data").ok());
  WriteBatchHandler next_handler;
  ASSERT_TRUE(ordinary.Iterate(&next_handler).ok());
  EXPECT_TRUE(next_handler.GetUpdates().empty());
}

TEST_F(ReplicationWriteBatchTest, OwnsFieldsWithoutRetainingLargeStreamValues) {
  const std::string channel(128, 'c');
  const std::string message("payload\0tail", 12);
  const std::string propagated(4096, 'p');
  const auto stream_key = entryKey("ns", "stream");
  WriteBatchHandler handler;
  {
    rocksdb::WriteBatch batch;
    ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::PubSub), channel, message).ok());
    ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::Propagate), "owned-key", propagated).ok());
    ASSERT_TRUE(
        batch.Put(storage_->GetCFHandle(ColumnFamilyID::Stream), stream_key, std::string(1024 * 1024, 's')).ok());
    ASSERT_TRUE(batch.Iterate(&handler).ok());
  }
  expectUpdates(handler, {{kBatchTypePublish, channel, message},
                          {kBatchTypePropagate, "owned-key", propagated},
                          {kBatchTypeStream, stream_key, ""}});
}

TEST_F(ReplicationWriteBatchTest, ManySmallEventsKeepTheirOrder) {
  rocksdb::WriteBatch batch;
  constexpr size_t count = 256;
  for (size_t i = 0; i < count; ++i) {
    ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::PubSub), "channel", std::to_string(i)).ok());
  }
  WriteBatchHandler handler;
  ASSERT_TRUE(batch.Iterate(&handler).ok());
  const auto &updates = handler.GetUpdates();
  ASSERT_EQ(updates.size(), count);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(updates[i].type, kBatchTypePublish);
    EXPECT_EQ(updates[i].key, "channel");
    EXPECT_EQ(updates[i].value, std::to_string(i));
  }
}

TEST_F(ReplicationWriteBatchTest, MalformedBatchFailsAfterCollectingPrefix) {
  rocksdb::WriteBatch batch;
  ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::PubSub), "channel", "first").ok());
  ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), "key", "truncated").ok());
  std::string bytes = batch.Data();
  bytes.pop_back();
  rocksdb::WriteBatch malformed(bytes);
  WriteBatchHandler handler;
  EXPECT_FALSE(malformed.Iterate(&handler).ok());
  // A failed Iterate can leave a prefix: the production dispatcher must check its status first.
  expectUpdates(handler, {{kBatchTypePublish, "channel", "first"}});
}

TEST_F(ReplicationWriteBatchTest, RequiresLiveMatchingMetadata) {
  const auto encoded = entryKey("ns", "stream");
  auto *db = storage_->GetDB();
  auto *cf = storage_->GetCFHandle(ColumnFamilyID::Stream);
  ASSERT_TRUE(db->Put(storage_->DefaultWriteOptions(), cf, encoded, "entry-value").ok());
  expectVisible(encoded, false);

  StreamMetadata metadata(false);
  metadata.version = 42;
  metadata.size = 1;
  ASSERT_TRUE(putMetadata("ns", "stream", metadata).ok());
  expectVisible(encoded, true);

  metadata.size = 0;
  ASSERT_TRUE(putMetadata("ns", "stream", metadata).ok());
  expectVisible(encoded, false);

  metadata.size = 1;
  metadata.expire = 1000;
  ASSERT_TRUE(putMetadata("ns", "stream", metadata).ok());
  expectVisible(encoded, false);

  metadata.expire = 0;
  metadata.version = 43;
  ASSERT_TRUE(putMetadata("ns", "stream", metadata).ok());
  std::string physical_value;
  ASSERT_TRUE(db->Get(storage_->DefaultScanOptions(), cf, encoded, &physical_value).ok());
  expectVisible(encoded, false);

  Metadata replacement(kRedisString, false);
  ASSERT_TRUE(putMetadata("ns", "stream", replacement).ok());
  expectVisible(encoded, false);
}

TEST_F(ReplicationWriteBatchTest, PointAndRangeDeletedEntriesAreNotVisible) {
  for (bool range_delete : {false, true}) {
    SCOPED_TRACE(range_delete);
    const std::string key = range_delete ? "range" : "point";
    ASSERT_TRUE(putStream("ns", key).ok());
    const auto encoded = entryKey("ns", key);
    auto *cf = storage_->GetCFHandle(ColumnFamilyID::Stream);
    auto *db = storage_->GetDB();
    ASSERT_TRUE(db->Put(storage_->DefaultWriteOptions(), cf, entryKey("ns", key, 42, 1), "seed").ok());
    if (range_delete) {
      ASSERT_TRUE(db->DeleteRange(storage_->DefaultWriteOptions(), cf, encoded, entryKey("ns", key, 42, 3)).ok());
    } else {
      ASSERT_TRUE(db->Delete(storage_->DefaultWriteOptions(), cf, encoded).ok());
    }
    expectVisible(encoded, false);
    expectVisible(entryKey("ns", key, 42, 1), true);
  }
}

TEST_F(ReplicationWriteBatchTest, OnlySixteenByteSubkeysAreEntryCandidates) {
  ASSERT_TRUE(putStream("ns", "stream").ok());
  for (size_t length : {size_t{0}, size_t{15}, size_t{17}, size_t{25}, size_t{33}}) {
    SCOPED_TRACE(length);
    std::string subkey(length, '\xff');
    auto encoded = InternalKey(metadataKey("ns", "stream"), subkey, 42, storage_->IsSlotIdEncoded()).Encode();
    ASSERT_TRUE(storage_->GetDB()
                    ->Put(storage_->DefaultWriteOptions(), storage_->GetCFHandle(ColumnFamilyID::Stream), encoded,
                          "bookkeeping")
                    .ok());
    expectVisible(encoded, false);
  }
  const auto maximum_ms = entryKey("ns", "stream", 42, std::numeric_limits<uint64_t>::max());
  ASSERT_TRUE(storage_->GetDB()
                  ->Put(storage_->DefaultWriteOptions(), storage_->GetCFHandle(ColumnFamilyID::Stream), maximum_ms,
                        "entry-value")
                  .ok());
  expectVisible(maximum_ms, true);
}

TEST_F(ReplicationWriteBatchTest, PreservesNamespaceAndSlotEncoding) {
  for (bool slot_encoded : {false, true}) {
    SCOPED_TRACE(slot_encoded);
    config_.slot_id_encoded = slot_encoded;
    ASSERT_TRUE(putStream("first", "{stream}").ok());
    ASSERT_TRUE(putStream("second", "{stream}", 43).ok());
    expectVisible(entryKey("first", "{stream}"), true);
    expectVisible(entryKey("second", "{stream}"), false);
    expectVisible(entryKey("second", "{stream}", 43), true);
    expectVisible(entryKey("missing", "{stream}"), false);
  }
}

TEST_F(ReplicationWriteBatchTest, BothReadsUseTheSuppliedSnapshot) {
  ASSERT_TRUE(putStream("ns", "stream").ok());
  const auto original = entryKey("ns", "stream");
  rocksdb::ManagedSnapshot snapshot(storage_->GetDB());
  ASSERT_NE(snapshot.snapshot(), nullptr);
  auto options = storage_->DefaultScanOptions();
  options.snapshot = snapshot.snapshot();

  ASSERT_TRUE(putStream("ns", "stream", 43).ok());
  ASSERT_TRUE(storage_->GetDB()
                  ->Delete(storage_->DefaultWriteOptions(), storage_->GetCFHandle(ColumnFamilyID::Stream), original)
                  .ok());
  const auto replacement = entryKey("ns", "stream", 43);
  // The old metadata and deleted entry must both remain visible through the same snapshot.
  expectVisible(original, options, true);
  expectVisible(replacement, options, false);
  expectVisible(original, false);
  expectVisible(replacement, true);
}

TEST_F(ReplicationWriteBatchTest, IgnoresUncommittedStorageTransactionOverlay) {
  ASSERT_TRUE(putStream("ns", "committed").ok());
  const auto committed = entryKey("ns", "committed");
  const auto pending = entryKey("ns", "pending");
  {
    ASSERT_TRUE(storage_->BeginTxn().IsOK());
    auto cleanup = MakeScopeExit([this] { EXPECT_TRUE(storage_->CommitTxn().IsOK()); });
    auto batch = storage_->GetWriteBatchBase();
    ASSERT_TRUE(batch->Delete(storage_->GetCFHandle(ColumnFamilyID::Metadata), metadataKey("ns", "committed")).ok());
    ASSERT_TRUE(batch->Delete(storage_->GetCFHandle(ColumnFamilyID::Stream), committed).ok());
    StreamMetadata metadata(false);
    metadata.version = 42;
    metadata.size = 1;
    std::string bytes;
    metadata.Encode(&bytes);
    ASSERT_TRUE(batch->Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), metadataKey("ns", "pending"), bytes).ok());
    ASSERT_TRUE(batch->Put(storage_->GetCFHandle(ColumnFamilyID::Stream), pending, "entry-value").ok());
    expectVisible(committed, true);
    expectVisible(pending, false);
  }
  expectVisible(committed, false);
  expectVisible(pending, true);
}

TEST_F(ReplicationWriteBatchTest, MalformedMetadataAndMissingSnapshotAreErrors) {
  ASSERT_TRUE(putStream("ns", "stream").ok());
  const auto encoded = entryKey("ns", "stream");
  auto options = storage_->DefaultScanOptions();
  EXPECT_FALSE(visibility(encoded, options).IsOK());

  ASSERT_TRUE(storage_->GetDB()
                  ->Put(storage_->DefaultWriteOptions(), storage_->GetCFHandle(ColumnFamilyID::Metadata),
                        metadataKey("ns", "stream"), std::string(1, '\0'))
                  .ok());
  rocksdb::ManagedSnapshot snapshot(storage_->GetDB());
  ASSERT_NE(snapshot.snapshot(), nullptr);
  options.snapshot = snapshot.snapshot();
  EXPECT_FALSE(visibility(encoded, options).IsOK());
}

TEST_F(ReplicationWriteBatchTest, DatabaseReadErrorsAreNotTreatedAsMissingEntries) {
  ASSERT_TRUE(putStream("ns", "stream").ok());
  rocksdb::ManagedSnapshot snapshot(storage_->GetDB());
  ASSERT_NE(snapshot.snapshot(), nullptr);
  auto options = storage_->DefaultScanOptions();
  options.snapshot = snapshot.snapshot();
  const std::string timestamp_bytes(8, '\0');
  const rocksdb::Slice timestamp(timestamp_bytes);
  // Kvrocks column families do not use RocksDB user-defined timestamps.
  options.timestamp = &timestamp;
  std::string bytes;
  auto status = storage_->GetDB()->Get(options, storage_->GetCFHandle(ColumnFamilyID::Metadata),
                                       metadataKey("ns", "stream"), &bytes);
  ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
  EXPECT_FALSE(visibility(entryKey("ns", "stream"), options).IsOK());
}
