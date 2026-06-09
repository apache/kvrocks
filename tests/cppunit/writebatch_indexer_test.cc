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
#include <rocksdb/merge_operator.h>
#include <rocksdb/write_batch.h>

#include <string>

#include "encoding.h"
#include "server/redis_reply.h"
#include "storage/batch_extractor.h"
#include "storage/batch_indexer.h"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "types/redis_stream_base.h"

class WriteBatchIndexerTest : public TestBase {
 protected:
  explicit WriteBatchIndexerTest() = default;
  ~WriteBatchIndexerTest() override = default;
};

TEST_F(WriteBatchIndexerTest, PutDelete) {
  rocksdb::WriteBatch batch;
  auto s = batch.Put("key0", "value0");
  EXPECT_TRUE(s.ok()) << s.ToString();
  for (int i = 1; i < 4; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    s = batch.Put(storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), key, value);
    EXPECT_TRUE(s.ok()) << s.ToString();
  }

  ctx_->batch = std::make_unique<rocksdb::WriteBatchWithIndex>();
  WriteBatchIndexer handle1(*ctx_);
  s = batch.Iterate(&handle1);
  EXPECT_TRUE(s.ok()) << s.ToString();

  rocksdb::Options options;
  std::string value;
  for (int i = 0; i < 4; i++) {
    std::string key = "key" + std::to_string(i);
    std::string expect_value = "value" + std::to_string(i);
    s = ctx_->batch->GetFromBatch(options, key, &value);
    EXPECT_TRUE(s.ok()) << s.ToString();
    EXPECT_EQ(expect_value, value);
  }

  s = storage_->GetDB()->Write(rocksdb::WriteOptions(), ctx_->batch->GetWriteBatch());
  EXPECT_TRUE(s.ok()) << s.ToString();

  batch.Clear();
  s = batch.Delete("key0");
  EXPECT_TRUE(s.ok()) << s.ToString();

  s = batch.DeleteRange(storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), "key1", "key3");
  EXPECT_TRUE(s.ok()) << s.ToString();

  WriteBatchIndexer handle2(*ctx_);
  s = batch.Iterate(&handle2);
  s = batch.Iterate(&handle2);
  EXPECT_TRUE(s.ok()) << s.ToString();

  for (int i = 0; i < 3; i++) {
    std::string key = "key" + std::to_string(i);
    s = ctx_->batch->GetFromBatchAndDB(storage_->GetDB(), rocksdb::ReadOptions(), key, &value);
    EXPECT_TRUE(s.IsNotFound());
  }

  s = ctx_->batch->GetFromBatchAndDB(storage_->GetDB(), rocksdb::ReadOptions(), "key3", &value);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ("value3", value);
}

TEST_F(WriteBatchIndexerTest, SingleDelete) {
  auto s = storage_->GetDB()->Put(rocksdb::WriteOptions(), "key", "value");
  EXPECT_TRUE(s.ok()) << s.ToString();

  std::string value;
  s = storage_->GetDB()->Get(rocksdb::ReadOptions(), "key", &value);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ("value", value);

  rocksdb::WriteBatch batch;
  s = batch.SingleDelete("key");
  EXPECT_TRUE(s.ok()) << s.ToString();

  ctx_->batch = std::make_unique<rocksdb::WriteBatchWithIndex>();
  WriteBatchIndexer handle(*ctx_);
  s = batch.Iterate(&handle);
  EXPECT_TRUE(s.ok()) << s.ToString();

  s = ctx_->batch->GetFromBatchAndDB(storage_->GetDB(), rocksdb::ReadOptions(), "key", &value);
  EXPECT_TRUE(s.IsNotFound());
}

namespace {

std::string EncodeStreamEntryKeyForBatchExtractorTest(const std::string &ns, const std::string &user_key,
                                                      const redis::StreamEntryID &id) {
  std::string sub_key;
  PutFixed64(&sub_key, id.ms);
  PutFixed64(&sub_key, id.seq);
  auto ns_key = ComposeNamespaceKey(ns, user_key, false);
  return InternalKey(ns_key, sub_key, 1, false).Encode();
}

}  // namespace

class WriteBatchExtractorTest : public TestBase {};

TEST_F(WriteBatchExtractorTest, InvalidLogDataResetsXDelExDedupState) {
  rocksdb::WriteBatch batch;
  auto stream_cf = storage_->GetCFHandle(ColumnFamilyID::Stream);
  auto entry_key = EncodeStreamEntryKeyForBatchExtractorTest("stream_ns", "stream", redis::StreamEntryID{1, 0});

  redis::WriteBatchLogData xdelex_log_data(kRedisStream, {"XDELEX", "KEEPREF"});
  ASSERT_TRUE(batch.PutLogData(xdelex_log_data.Encode()).ok());
  ASSERT_TRUE(batch.Delete(stream_cf, entry_key).ok());
  ASSERT_TRUE(batch.Delete(stream_cf, entry_key).ok());
  ASSERT_TRUE(batch.PutLogData("bad-log-data").ok());
  ASSERT_TRUE(batch.Delete(stream_cf, entry_key).ok());

  WriteBatchExtractor extractor(false);
  auto s = batch.Iterate(&extractor);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto commands = extractor.GetRESPCommands();
  auto iter = commands->find("stream_ns");
  ASSERT_NE(commands->end(), iter);
  const auto &stream_commands = iter->second;
  ASSERT_EQ(2, stream_commands.size());

  EXPECT_EQ(redis::ArrayOfBulkStrings({"XDELEX", "stream", "KEEPREF", "IDS", "1", "1-0"}), stream_commands[0]);
  EXPECT_EQ(redis::ArrayOfBulkStrings({"XDEL", "stream", "1-0"}), stream_commands[1]);
}
