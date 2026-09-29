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

#include "storage/batch_extractor.h"

#include <gtest/gtest.h>

#include "server/redis_reply.h"

TEST(WriteBatchLogData, HashEncoding) {
  redis::WriteBatchLogData legacy(HashSubkeyEncodingMode::kLegacy);
  redis::WriteBatchLogData hfe(HashSubkeyEncodingMode::kFieldExpiration);
  EXPECT_EQ(legacy.Encode(), "2");
  EXPECT_EQ(hfe.Encode(), "2 13 1");

  redis::WriteBatchLogData decoded;
  for (const auto &log : {hfe, legacy}) {
    ASSERT_TRUE(decoded.Decode(log.Encode()).IsOK());
    auto mode = decoded.GetHashSubkeyEncodingMode();
    ASSERT_TRUE(mode.IsOK());
    EXPECT_EQ(*mode, *log.GetHashSubkeyEncodingMode());
  }
}

class HashBatchExtractorTest : public ::testing::TestWithParam<bool> {};

TEST_P(HashBatchExtractorTest, MixedEncodingsWithoutMetadata) {
  bool slot_encoded = GetParam();
  HashMetadata hfe(false, HashSubkeyEncodingMode::kFieldExpiration);
  auto ns_key = ComposeNamespaceKey("ns", "hash", slot_encoded);
  auto key = InternalKey(ns_key, "field", 1, slot_encoded).Encode();
  const std::string binary_value("\0value\xff", 7);
  const auto legacy_value = hfe.EncodeSubkeyValue(binary_value, 123);
  rocksdb::WriteBatch batch;
  std::vector<std::string> expected;

  // Include an already-expired timestamp: replay must remove any older destination value.
  for (uint64_t expire : {0ULL, 1ULL, 4000000000000ULL}) {
    ASSERT_TRUE(batch.PutLogData(redis::WriteBatchLogData(hfe.mode).Encode()).ok());
    ASSERT_TRUE(batch.Put(key, hfe.EncodeSubkeyValue(binary_value, expire)).ok());
    expected.emplace_back(redis::ArrayOfBulkStrings({"HSET", "hash", "field", binary_value}));
    if (expire != 0) {
      expected.emplace_back(
          redis::ArrayOfBulkStrings({"HPEXPIREAT", "hash", std::to_string(expire), "FIELDS", "1", "field"}));
    }

    // An old-format log must reset the mode, even if its value looks like HFE data.
    ASSERT_TRUE(batch.PutLogData(redis::WriteBatchLogData(kRedisHash).Encode()).ok());
    ASSERT_TRUE(batch.Put(key, legacy_value).ok());
    expected.emplace_back(redis::ArrayOfBulkStrings({"HSET", "hash", "field", legacy_value}));
  }
  ASSERT_TRUE(batch.PutLogData(redis::WriteBatchLogData(hfe.mode).Encode()).ok());
  ASSERT_TRUE(batch.Put(key, hfe.EncodeSubkeyValue("")).ok());
  expected.emplace_back(redis::ArrayOfBulkStrings({"HSET", "hash", "field", ""}));
  ASSERT_TRUE(batch.Delete(key).ok());
  expected.emplace_back(redis::ArrayOfBulkStrings({"HDEL", "hash", "field"}));

  WriteBatchExtractor extractor(slot_encoded, -1, true);
  ASSERT_TRUE(batch.Iterate(&extractor).ok());
  ASSERT_EQ(extractor.GetRESPCommands()->size(), 1);
  EXPECT_EQ(extractor.GetRESPCommands()->at("ns"), expected);
}

TEST_P(HashBatchExtractorTest, InvalidHashEncoding) {
  auto ns_key = ComposeNamespaceKey("ns", "hash", GetParam());
  auto key = InternalKey(ns_key, "field", 1, GetParam()).Encode();
  for (const auto &log : {"2 13", "2 13 2", "2 13 -1", "2 13 invalid", "2 13 1 extra"}) {
    SCOPED_TRACE(log);
    rocksdb::WriteBatch batch;
    ASSERT_TRUE(batch.PutLogData(log).ok());
    ASSERT_TRUE(batch.Put(key, std::string(8, '\0')).ok());
    WriteBatchExtractor extractor(GetParam(), -1, true);
    EXPECT_TRUE(batch.Iterate(&extractor).IsInvalidArgument());
    EXPECT_TRUE(extractor.GetRESPCommands()->empty());
  }

  rocksdb::WriteBatch batch;
  ASSERT_TRUE(batch.PutLogData(redis::WriteBatchLogData(HashSubkeyEncodingMode::kFieldExpiration).Encode()).ok());
  ASSERT_TRUE(batch.Put(key, std::string(7, '\0')).ok());
  WriteBatchExtractor extractor(GetParam(), -1, true);
  EXPECT_TRUE(batch.Iterate(&extractor).IsInvalidArgument());
  EXPECT_TRUE(extractor.GetRESPCommands()->empty());
}

INSTANTIATE_TEST_SUITE_P(SlotEncoding, HashBatchExtractorTest, ::testing::Bool());
