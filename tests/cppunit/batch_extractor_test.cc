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

#include "storage/batch_extractor.h"

#include <gtest/gtest.h>
#include <rocksdb/write_batch.h>

#include "common/string_util.h"
#include "config/config.h"
#include "server/redis_reply.h"
#include "test_base.h"
#include "types/redis_string.h"

namespace {

std::vector<std::string> GetCommands(WriteBatchExtractor *extractor, const std::string &ns) {
  auto *commands = extractor->GetRESPCommands();
  if (auto it = commands->find(ns); it != commands->end()) {
    return it->second;
  }
  return {};
}

}  // namespace

TEST(WriteBatchExtractorTest, ExtractFlushDBFromNamespaceDeleteRange) {
  WriteBatchExtractor extractor(false, -1, true);
  auto begin_key = ComposeNamespaceKey(kDefaultNamespace, "", false);
  auto end_key = util::StringNext(begin_key);

  auto s = extractor.DeleteRangeCF(static_cast<uint32_t>(ColumnFamilyID::Metadata), begin_key, end_key);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto commands = GetCommands(&extractor, kDefaultNamespace);
  ASSERT_EQ(commands.size(), 1);
  EXPECT_EQ(commands[0], redis::ArrayOfBulkStrings({"FLUSHDB"}));
}

TEST(WriteBatchExtractorTest, ExtractFlushDBFromNonDefaultNamespaceDeleteRange) {
  std::string ns = "test-ns";
  WriteBatchExtractor extractor(false, -1, true);
  auto begin_key = ComposeNamespaceKey(ns, "", false);
  auto end_key = util::StringNext(begin_key);

  auto s = extractor.DeleteRangeCF(static_cast<uint32_t>(ColumnFamilyID::Metadata), begin_key, end_key);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto commands = GetCommands(&extractor, ns);
  ASSERT_EQ(commands.size(), 1);
  EXPECT_EQ(commands[0], redis::ArrayOfBulkStrings({"FLUSHDB"}));
}

TEST(WriteBatchExtractorTest, RejectNonFlushDBDeleteRange) {
  std::string ns = "test-ns";
  WriteBatchExtractor extractor(false, -1, true);
  auto begin_key = ComposeNamespaceKey(ns, "key", false);
  auto end_key = util::StringNext(begin_key);

  auto s = extractor.DeleteRangeCF(static_cast<uint32_t>(ColumnFamilyID::Metadata), begin_key, end_key);
  ASSERT_TRUE(s.ok()) << s.ToString();

  EXPECT_TRUE(extractor.GetRESPCommands()->empty());
}

TEST(WriteBatchExtractorTest, RejectDeleteRangeWithMismatchedEndKey) {
  std::string ns = "test-ns";
  WriteBatchExtractor extractor(false, -1, true);
  auto begin_key = ComposeNamespaceKey(ns, "", false);
  // end_key is not StringNext(begin_key) - deliberately tampered.
  std::string end_key = begin_key + std::string(1, '\x02');

  auto s = extractor.DeleteRangeCF(static_cast<uint32_t>(ColumnFamilyID::Metadata), begin_key, end_key);
  ASSERT_TRUE(s.ok()) << s.ToString();

  EXPECT_TRUE(extractor.GetRESPCommands()->empty());
}

TEST(WriteBatchExtractorTest, RejectDeleteRangeWithEmptyBeginKey) {
  WriteBatchExtractor extractor(false, -1, true);

  auto s = extractor.DeleteRangeCF(static_cast<uint32_t>(ColumnFamilyID::Metadata), "", "");
  ASSERT_TRUE(s.ok()) << s.ToString();

  EXPECT_TRUE(extractor.GetRESPCommands()->empty());
}

TEST(WriteBatchExtractorTest, IgnoreDeleteRangeOutsideMetadataColumnFamily) {
  WriteBatchExtractor extractor(false, -1, true);
  auto begin_key = ComposeNamespaceKey(kDefaultNamespace, "", false);
  auto end_key = util::StringNext(begin_key);

  auto s = extractor.DeleteRangeCF(static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey), begin_key, end_key);
  ASSERT_TRUE(s.ok()) << s.ToString();

  EXPECT_TRUE(extractor.GetRESPCommands()->empty());
}

class WriteBatchExtractorFlushAllTest : public TestBase {};

TEST_F(WriteBatchExtractorFlushAllTest, ExtractFlushAllAsNamespaceFlushDBCommands) {
  std::string ns = "test-ns";
  redis::String default_db(storage_.get(), kDefaultNamespace);
  redis::String non_default_db(storage_.get(), ns);

  ASSERT_TRUE(default_db.Set(*ctx_, "key1", "value1").ok());
  ASSERT_TRUE(non_default_db.Set(*ctx_, "key2", "value2").ok());

  auto flush_all_seq = storage_->LatestSeqNumber() + 1;
  redis::Database db(storage_.get(), kDefaultNamespace);
  auto s = db.FlushAll(*ctx_);
  ASSERT_TRUE(s.ok()) << s.ToString();

  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto status = storage_->GetWALIter(flush_all_seq, &iter);
  ASSERT_TRUE(status.IsOK()) << status.Msg();
  ASSERT_TRUE(iter->Valid());

  auto batch = iter->GetBatch();
  ASSERT_EQ(batch.sequence, flush_all_seq);

  rocksdb::WriteBatch write_batch(batch.writeBatchPtr->Data());
  WriteBatchExtractor extractor(storage_->IsSlotIdEncoded(), -1, true);
  auto db_status = write_batch.Iterate(&extractor);
  ASSERT_TRUE(db_status.ok()) << db_status.ToString();

  auto default_commands = GetCommands(&extractor, kDefaultNamespace);
  ASSERT_EQ(default_commands.size(), 1);
  EXPECT_EQ(default_commands[0], redis::ArrayOfBulkStrings({"FLUSHDB"}));

  auto non_default_commands = GetCommands(&extractor, ns);
  ASSERT_EQ(non_default_commands.size(), 1);
  EXPECT_EQ(non_default_commands[0], redis::ArrayOfBulkStrings({"FLUSHDB"}));
}
