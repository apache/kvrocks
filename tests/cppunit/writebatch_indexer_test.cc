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
#include <rocksdb/transaction_log.h>
#include <rocksdb/write_batch.h>

#include "storage/batch_extractor.h"
#include "storage/batch_indexer.h"
#include "test_base.h"
#include "types/redis_stream.h"

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

std::unique_ptr<rocksdb::WriteBatch> GetOnlyWalBatchAfter(engine::Storage *storage, rocksdb::SequenceNumber seq) {
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto s = storage->GetWALIter(seq + 1, &iter);
  if (!s.IsOK() || !iter || !iter->Valid()) {
    return nullptr;
  }

  auto batch = iter->GetBatch();
  auto copy = std::make_unique<rocksdb::WriteBatch>(batch.writeBatchPtr->Data());

  iter->Next();
  EXPECT_FALSE(iter->Valid());
  return copy;
}

struct DeleteOp {
  uint32_t column_family_id = 0;
  std::string key;
};

class XAckDelBatchCollector : public rocksdb::WriteBatch::Handler {
 public:
  rocksdb::Status PutCF(uint32_t, const rocksdb::Slice &, const rocksdb::Slice &) override {
    return rocksdb::Status::OK();
  }

  void LogData(const rocksdb::Slice &blob) override {
    redis::WriteBatchLogData log_data;
    if (log_data.Decode(blob).IsOK()) {
      auto *args = log_data.GetArguments();
      if (!args->empty() && (*args)[0] == "XACKDEL") {
        log_data_ = blob.ToString();
      }
    }
  }

  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice &key) override {
    deletes_.push_back({column_family_id, key.ToString()});
    return rocksdb::Status::OK();
  }

  const std::string &LogDataBlob() const { return log_data_; }
  const std::vector<DeleteOp> &Deletes() const { return deletes_; }

 private:
  std::string log_data_;
  std::vector<DeleteOp> deletes_;
};

void AddPendingStreamEntry(redis::Stream *stream, engine::Context *ctx, const std::string &stream_name,
                           const std::string &group_name) {
  redis::StreamAddOptions add_options;
  add_options.next_id_strategy = *redis::ParseNextStreamEntryIDStrategy("1-0");
  redis::StreamEntryID id;
  auto s = stream->Add(*ctx, stream_name, add_options, {"field", "value"}, &id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  redis::StreamXGroupCreateOptions create_options = {false, 0, "0-0"};
  s = stream->CreateGroup(*ctx, stream_name, create_options, group_name);
  ASSERT_TRUE(s.ok()) << s.ToString();

  redis::StreamRangeOptions range_options;
  range_options.start = redis::StreamEntryID::Minimum();
  range_options.end = redis::StreamEntryID::Maximum();
  range_options.count = 1;
  range_options.with_count = true;
  range_options.exclude_start = true;
  std::vector<redis::StreamEntry> entries;
  std::string pending_group_name = group_name;
  std::string consumer_name = "c1";
  s = stream->RangeWithPending(*ctx, stream_name, range_options, &entries, pending_group_name, consumer_name, false,
                               true);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(entries.size(), 1);
}

}  // namespace

TEST_F(WriteBatchIndexerTest, MalformedLogDataResetsXAckDelExtractorState) {
  const std::string stream_name = "stream";
  const std::string group_name = "group";
  redis::Stream stream(storage_.get(), "test_ns");

  AddPendingStreamEntry(&stream, ctx_.get(), stream_name, group_name);
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  std::vector<int> results;
  auto s = stream.DeleteEntriesAndAck(*ctx_, stream_name, group_name, {redis::StreamEntryID{1, 0}},
                                      redis::StreamDeleteOption::KeepRef, &results);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(results, std::vector<int>({1}));
  auto first_batch = GetOnlyWalBatchAfter(storage_.get(), start_seq);
  ASSERT_NE(first_batch, nullptr);

  XAckDelBatchCollector collector;
  auto collect_status = first_batch->Iterate(&collector);
  ASSERT_TRUE(collect_status.ok()) << collect_status.ToString();
  ASSERT_FALSE(collector.LogDataBlob().empty());
  ASSERT_GE(collector.Deletes().size(), 2);

  rocksdb::WriteBatch combined;
  combined.PutLogData(collector.LogDataBlob());
  for (const auto &op : collector.Deletes()) {
    auto status = combined.Delete(storage_->GetCFHandle(static_cast<ColumnFamilyID>(op.column_family_id)), op.key);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  combined.PutLogData("malformed-log-data");
  for (const auto &op : collector.Deletes()) {
    auto status = combined.Delete(storage_->GetCFHandle(static_cast<ColumnFamilyID>(op.column_family_id)), op.key);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  WriteBatchExtractor extractor(storage_->IsSlotIdEncoded());
  auto iterate_status = combined.Iterate(&extractor);
  ASSERT_TRUE(iterate_status.ok()) << iterate_status.ToString();

  auto *resp_commands = extractor.GetRESPCommands();
  auto it = resp_commands->find("test_ns");
  ASSERT_NE(it, resp_commands->end());

  int xack_count = 0;
  int xdel_count = 0;
  for (const auto &encoded : it->second) {
    if (encoded.find("$4\r\nXACK\r\n") != std::string::npos) xack_count++;
    if (encoded.find("$4\r\nXDEL\r\n") != std::string::npos) xdel_count++;
  }

  EXPECT_EQ(xack_count, 1);
  EXPECT_EQ(xdel_count, 2);
}
