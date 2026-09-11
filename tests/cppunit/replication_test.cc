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

#include "cluster/replication.h"

#include <gtest/gtest.h>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "test_base.h"

class ReplicationWriteBatchTest : public TestBase {};

TEST_F(ReplicationWriteBatchTest, ExtractKeyspaceEvents) {
  rocksdb::WriteBatch batch;
  auto *metadata_cf = storage_->GetCFHandle(ColumnFamilyID::Metadata);

  redis::WriteBatchLogData set_log_data(kRedisString, {std::to_string(kRedisCmdSet)});
  ASSERT_TRUE(batch.PutLogData(set_log_data.Encode()).ok());
  std::string string_value;
  Metadata(kRedisString, false).Encode(&string_value);
  string_value.append("value");
  ASSERT_TRUE(batch.Put(metadata_cf, ComposeNamespaceKey("tenant", "set-key", false), string_value).ok());

  redis::WriteBatchLogData legacy_log_data(kRedisString);
  ASSERT_TRUE(batch.PutLogData(legacy_log_data.Encode()).ok());
  ASSERT_TRUE(batch.Put(metadata_cf, ComposeNamespaceKey("tenant", "legacy-key", false), string_value).ok());

  redis::WriteBatchLogData del_log_data(kRedisNone, {std::to_string(kRedisCmdDel)});
  ASSERT_TRUE(batch.PutLogData(del_log_data.Encode()).ok());
  ASSERT_TRUE(batch.Delete(metadata_cf, ComposeNamespaceKey("tenant", "del-key", false)).ok());

  WriteBatchHandler detector(true);
  ASSERT_TRUE(batch.Iterate(&detector).ok());
  ASSERT_TRUE(detector.HasKeyspaceEvents());

  KeyspaceEventBatchHandler handler(false);
  ASSERT_TRUE(batch.Iterate(&handler).ok());
  const auto &events = handler.Events();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].type_flag, kNotifyString);
  EXPECT_EQ(events[0].event, "set");
  EXPECT_EQ(events[0].ns, "tenant");
  EXPECT_EQ(events[0].key, "set-key");
  EXPECT_EQ(events[1].type_flag, kNotifyGeneric);
  EXPECT_EQ(events[1].event, "del");
  EXPECT_EQ(events[1].ns, "tenant");
  EXPECT_EQ(events[1].key, "del-key");
}

TEST_F(ReplicationWriteBatchTest, IgnoreLegacyLogData) {
  rocksdb::WriteBatch batch;
  redis::WriteBatchLogData legacy_log_data(kRedisString);
  ASSERT_TRUE(batch.PutLogData(legacy_log_data.Encode()).ok());

  std::string string_value;
  Metadata(kRedisString, false).Encode(&string_value);
  string_value.append("value");
  ASSERT_TRUE(batch
                  .Put(storage_->GetCFHandle(ColumnFamilyID::Metadata),
                       ComposeNamespaceKey(kDefaultNamespace, "key", false), string_value)
                  .ok());

  WriteBatchHandler detector(true);
  ASSERT_TRUE(batch.Iterate(&detector).ok());
  EXPECT_FALSE(detector.HasKeyspaceEvents());

  KeyspaceEventBatchHandler handler(false);
  ASSERT_TRUE(batch.Iterate(&handler).ok());
  EXPECT_TRUE(handler.Events().empty());
}
