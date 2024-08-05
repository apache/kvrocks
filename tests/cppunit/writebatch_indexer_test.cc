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

#include "storage/batch_indexer.h"
#include "test_base.h"

class WriteBatchIndexerTest : public TestBase {
 protected:
  explicit WriteBatchIndexerTest() = default;
  ~WriteBatchIndexerTest() override = default;
};

TEST_F(WriteBatchIndexerTest, PutDelete) {
  rocksdb::WriteBatch batch;
  batch.Put("key0", "value0");
  for (int i = 1; i < 4; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    batch.Put(storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), key, value);
  }

  rocksdb::WriteBatchWithIndex dest_batch;
  WriteBatchIndexer handle(storage_.get(), &dest_batch);
  batch.Iterate(&handle);

  rocksdb::Options options;
  std::string value;
  for (int i = 0; i < 4; i++) {
    std::string key = "key" + std::to_string(i);
    std::string expect_value = "value" + std::to_string(i);
    dest_batch.GetFromBatch(options, key, &value);
    EXPECT_EQ(expect_value, value);
  }

  storage_->GetDB()->Write(rocksdb::WriteOptions(), dest_batch.GetWriteBatch());

  batch.Clear();
  dest_batch.Clear();
  batch.Delete("key0");
  batch.DeleteRange(storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), "key1", "key3");

  batch.Iterate(&handle);
  for (int i = 0; i < 3; i++) {
    std::string key = "key" + std::to_string(i);
    dest_batch.GetFromBatchAndDB(storage_->GetDB(), rocksdb::ReadOptions(), key, &value);
    EXPECT_EQ("", value);
  }

  dest_batch.GetFromBatchAndDB(storage_->GetDB(), rocksdb::ReadOptions(), "key3", &value);
  EXPECT_EQ("value3", value);
}

TEST_F(WriteBatchIndexerTest, SingleDelete) {
  storage_->GetDB()->Put(rocksdb::WriteOptions(), storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), "key", "value");
  std::string value;
  storage_->GetDB()->Get(rocksdb::ReadOptions(), storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), "key", &value);
  EXPECT_EQ("value", value);

  rocksdb::WriteBatch batch;
  rocksdb::WriteBatchWithIndex dest_batch;
  batch.SingleDelete("key");
  WriteBatchIndexer handle(storage_.get(), &dest_batch);
  batch.Iterate(&handle);

  dest_batch.GetFromBatchAndDB(storage_->GetDB(), rocksdb::ReadOptions(), "key", &value);
  EXPECT_EQ("", value);
}
