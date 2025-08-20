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

#include <memory>
#include <random>

#include "test_base.h"
#include "types/redis_timeseries.h"

class TimeSeriesTest : public TestBase {
 protected:
  explicit TimeSeriesTest() = default;
  ~TimeSeriesTest() override = default;

  void SetUp() override {
    key_ = "test_ts_key";
    ts_db_ = std::make_unique<redis::TimeSeries>(storage_.get(), "ts_namespace");
  }

  std::unique_ptr<redis::TimeSeries> ts_db_;
};

TEST_F(TimeSeriesTest, Create) {
  redis::TSCreateOption option;
  option.retention_time = 3600;
  option.chunk_size = 1024;
  option.chunk_type = TimeSeriesMetadata::ChunkType::COMPRESSED;

  auto s = ts_db_->Create(*ctx_, key_, option);
  EXPECT_TRUE(s.ok());

  s = ts_db_->Create(*ctx_, key_, option);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "Invalid argument: key already exists");
}

TEST_F(TimeSeriesTest, Add) {
  redis::TSCreateOption option;
  option.retention_time = 3600;
  option.chunk_size = 3;
  auto s = ts_db_->Create(*ctx_, key_, option);
  EXPECT_TRUE(s.ok());

  TSSample sample{1620000000, 123.45};
  TSChunk::AddResultWithTS result;
  s = ts_db_->Add(*ctx_, key_, sample, option, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(result.first, TSChunk::AddResult::kOk);
  EXPECT_EQ(result.second, sample.ts);
}

TEST_F(TimeSeriesTest, MAdd) {
  redis::TSCreateOption option;
  option.retention_time = 10;
  auto s = ts_db_->Create(*ctx_, key_, option);
  EXPECT_TRUE(s.ok());

  std::vector<TSSample> samples = {{1, 10}, {3, 10}, {2, 20}, {3, 20}, {4, 20}, {13, 20}, {1, 20}, {14, 20}};
  std::vector<TSChunk::AddResultWithTS> results;
  results.resize(samples.size());

  s = ts_db_->MAdd(*ctx_, key_, samples, &results);
  EXPECT_TRUE(s.ok());

  // Expected results: kOk/kBlock/kOld verification
  std::vector<TSChunk::AddResult> expected_results = {TSChunk::AddResult::kOk,     // 1
                                                      TSChunk::AddResult::kOk,     // 3
                                                      TSChunk::AddResult::kOk,     // 2
                                                      TSChunk::AddResult::kBlock,  // duplicate 3
                                                      TSChunk::AddResult::kOk,     // 4
                                                      TSChunk::AddResult::kOk,     // 13
                                                      TSChunk::AddResult::kOld,    // 1 (older than retention)
                                                      TSChunk::AddResult::kOk};    // 14

  std::vector<uint64_t> expected_ts = {1, 3, 2, 0, 4, 13, 0, 14};

  for (size_t i = 0; i < results.size(); ++i) {
    EXPECT_EQ(results[i].first, expected_results[i]) << "Result mismatch at index " << i;
    if (expected_results[i] == TSChunk::AddResult::kOk) {
      EXPECT_EQ(results[i].second, expected_ts[i]) << "Timestamp mismatch at index " << i;
    }
  }
  s = ts_db_->MAdd(*ctx_, key_, {{14, 0}}, &results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].first, TSChunk::AddResult::kBlock);
}
