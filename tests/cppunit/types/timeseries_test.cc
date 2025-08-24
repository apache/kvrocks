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

TEST_F(TimeSeriesTest, Range) {
  redis::TSCreateOption option;
  option.labels = {{"type", "stock"}, {"name", "A"}};
  auto s = ts_db_->Create(*ctx_, key_, option);
  EXPECT_TRUE(s.ok());

  // Add three batches of samples
  std::vector<TSSample> samples1 = {{1000, 100}, {1010, 110}, {1020, 120}};
  std::vector<TSChunk::AddResultWithTS> results1;
  results1.resize(samples1.size());
  s = ts_db_->MAdd(*ctx_, key_, samples1, &results1);
  EXPECT_TRUE(s.ok());

  std::vector<TSSample> samples2 = {{2000, 200}, {2010, 210}, {2020, 220}};
  std::vector<TSChunk::AddResultWithTS> results2;
  results2.resize(samples2.size());
  s = ts_db_->MAdd(*ctx_, key_, samples2, &results2);
  EXPECT_TRUE(s.ok());

  std::vector<TSSample> samples3 = {{3000, 300}, {3010, 310}, {3020, 320}};
  std::vector<TSChunk::AddResultWithTS> results3;
  results3.resize(samples3.size());
  s = ts_db_->MAdd(*ctx_, key_, samples3, &results3);
  EXPECT_TRUE(s.ok());

  // Test basic range query without aggregation
  std::vector<TSSample> res;
  redis::TSRangeOption range_opt;
  range_opt.start_ts = 0;
  range_opt.end_ts = TSSample::MAX_TIMESTAMP;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 9);
  for (size_t i = 0; i < samples1.size(); ++i) {
    EXPECT_EQ(res[i], samples1[i]);
  }
  for (size_t i = 0; i < samples2.size(); ++i) {
    EXPECT_EQ(res[i + samples1.size()], samples2[i]);
  }
  for (size_t i = 0; i < samples3.size(); ++i) {
    EXPECT_EQ(res[i + samples1.size() + samples2.size()], samples3[i]);
  }

  // Test aggregation with min
  res.clear();
  range_opt.aggregator.type = redis::TSAggregatorType::MIN;
  range_opt.aggregator.bucket_duration = 20;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 6);
  EXPECT_EQ(res[0].ts, 1000);
  EXPECT_EQ(res[0].v, 100);
  EXPECT_EQ(res[1].ts, 1020);
  EXPECT_EQ(res[1].v, 120);
  EXPECT_EQ(res[2].ts, 2000);
  EXPECT_EQ(res[2].v, 200);
  EXPECT_EQ(res[3].ts, 2020);
  EXPECT_EQ(res[3].v, 220);
  EXPECT_EQ(res[4].ts, 3000);
  EXPECT_EQ(res[4].v, 300);
  EXPECT_EQ(res[5].ts, 3020);
  EXPECT_EQ(res[5].v, 320);

  // Test different aggregators
  res.clear();
  range_opt.aggregator.type = redis::TSAggregatorType::AVG;
  range_opt.aggregator.bucket_duration = 1000;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res[0].v, 110);
  EXPECT_EQ(res[1].v, 210);
  EXPECT_EQ(res[2].v, 310);

  range_opt.aggregator.type = redis::TSAggregatorType::SUM;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res[0].v, 330);
  EXPECT_EQ(res[1].v, 630);
  EXPECT_EQ(res[2].v, 930);

  range_opt.aggregator.type = redis::TSAggregatorType::COUNT;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res[0].v, 3);
  EXPECT_EQ(res[1].v, 3);
  EXPECT_EQ(res[2].v, 3);

  range_opt.aggregator.type = redis::TSAggregatorType::RANGE;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res[0].v, 20);
  EXPECT_EQ(res[1].v, 20);
  EXPECT_EQ(res[2].v, 20);

  range_opt.aggregator.type = redis::TSAggregatorType::FIRST;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res[0].v, 100);
  EXPECT_EQ(res[1].v, 200);
  EXPECT_EQ(res[2].v, 300);

  range_opt.aggregator.type = redis::TSAggregatorType::STD_P;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_NEAR(res[0].v, 8.16496580927726, 1e-6);
  EXPECT_NEAR(res[1].v, 8.16496580927726, 1e-6);
  EXPECT_NEAR(res[2].v, 8.16496580927726, 1e-6);

  range_opt.aggregator.type = redis::TSAggregatorType::STD_S;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_NEAR(res[0].v, 10.0, 1e-6);
  EXPECT_NEAR(res[1].v, 10.0, 1e-6);
  EXPECT_NEAR(res[2].v, 10.0, 1e-6);

  range_opt.aggregator.type = redis::TSAggregatorType::VAR_P;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_NEAR(res[0].v, 66.666666, 1e-6);
  EXPECT_NEAR(res[1].v, 66.666666, 1e-6);
  EXPECT_NEAR(res[2].v, 66.666666, 1e-6);

  range_opt.aggregator.type = redis::TSAggregatorType::VAR_S;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_NEAR(res[0].v, 100.0, 1e-6);
  EXPECT_NEAR(res[1].v, 100.0, 1e-6);
  EXPECT_NEAR(res[2].v, 100.0, 1e-6);

  // Test alignment
  res.clear();
  range_opt.aggregator.type = redis::TSAggregatorType::MIN;
  range_opt.aggregator.alignment = 10;
  range_opt.aggregator.bucket_duration = 20;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 6);
  EXPECT_EQ(res[0].ts, 990);
  EXPECT_EQ(res[0].v, 100);
  EXPECT_EQ(res[1].ts, 1010);
  EXPECT_EQ(res[1].v, 110);
  EXPECT_EQ(res[2].ts, 1990);
  EXPECT_EQ(res[2].v, 200);
  EXPECT_EQ(res[3].ts, 2010);
  EXPECT_EQ(res[3].v, 210);
  EXPECT_EQ(res[4].ts, 2990);
  EXPECT_EQ(res[4].v, 300);
  EXPECT_EQ(res[5].ts, 3010);
  EXPECT_EQ(res[5].v, 310);

  // Test alignment with irregular bucket
  res.clear();
  range_opt.aggregator.bucket_duration = 4000;
  range_opt.aggregator.alignment = 2000;
  range_opt.aggregator.type = redis::TSAggregatorType::SUM;
  range_opt.start_ts = 1000;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 2);
  EXPECT_EQ(res[0].ts, 0);
  EXPECT_EQ(res[0].v, 330);
  EXPECT_EQ(res[1].ts, 2000);
  EXPECT_EQ(res[1].v, 1560);

  // Test bucket timestamp type
  res.clear();
  range_opt.aggregator.type = redis::TSAggregatorType::MIN;
  range_opt.aggregator.alignment = 10;
  range_opt.aggregator.bucket_duration = 20;
  range_opt.start_ts = 0;
  range_opt.bucket_timestamp_type = redis::TSRangeOption::BucketTimestampType::Mid;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 6);
  EXPECT_EQ(res[0].ts, 1000);
  EXPECT_EQ(res[0].v, 100);
  EXPECT_EQ(res[1].ts, 1020);
  EXPECT_EQ(res[1].v, 110);
  EXPECT_EQ(res[2].ts, 2000);
  EXPECT_EQ(res[2].v, 200);
  EXPECT_EQ(res[3].ts, 2020);
  EXPECT_EQ(res[3].v, 210);
  EXPECT_EQ(res[4].ts, 3000);
  EXPECT_EQ(res[4].v, 300);
  EXPECT_EQ(res[5].ts, 3020);
  EXPECT_EQ(res[5].v, 310);

  // Test empty buckets
  res.clear();
  range_opt.is_return_empty = true;
  range_opt.start_ts = 1500;
  range_opt.end_ts = 2500;
  range_opt.aggregator.bucket_duration = 5;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 5);
  EXPECT_EQ(res[0].ts, 2002);
  EXPECT_EQ(res[0].v, 200);
  EXPECT_EQ(res[1].ts, 2007);
  EXPECT_TRUE(std::isnan(res[1].v));
  EXPECT_EQ(res[2].ts, 2012);
  EXPECT_EQ(res[2].v, 210);
  EXPECT_EQ(res[3].ts, 2017);
  EXPECT_TRUE(std::isnan(res[3].v));
  EXPECT_EQ(res[4].ts, 2022);
  EXPECT_EQ(res[4].v, 220);

  // Test filter by value
  res.clear();
  range_opt.aggregator.bucket_duration = 20;
  range_opt.is_return_empty = false;
  range_opt.filter_by_value = std::make_optional(std::make_pair(200.0, 300.0));
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 2);
  for (const auto& sample : res) {
    EXPECT_GE(sample.v, 200.0);
    EXPECT_LE(sample.v, 300.0);
  }

  // Test count limit
  res.clear();
  range_opt.count_limit = 1;
  s = ts_db_->Range(*ctx_, key_, range_opt, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 1);
}
