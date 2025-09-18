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
  TSChunk::AddResult result;
  s = ts_db_->Add(*ctx_, key_, sample, option, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(result.type, TSChunk::AddResultType::kInsert);
  EXPECT_EQ(result.sample.ts, sample.ts);
}

TEST_F(TimeSeriesTest, MAdd) {
  redis::TSCreateOption option;
  option.retention_time = 10;
  auto s = ts_db_->Create(*ctx_, key_, option);
  EXPECT_TRUE(s.ok());

  std::vector<TSSample> samples = {{1, 10}, {3, 10}, {2, 20}, {3, 20}, {4, 20}, {13, 20}, {1, 20}, {14, 20}};
  std::vector<TSChunk::AddResult> results;
  results.resize(samples.size());

  s = ts_db_->MAdd(*ctx_, key_, samples, &results);
  EXPECT_TRUE(s.ok());

  // Expected results: kOk/kBlock/kOld verification
  std::vector<TSChunk::AddResultType> expected_results = {TSChunk::AddResultType::kInsert,   // 1
                                                          TSChunk::AddResultType::kInsert,   // 3
                                                          TSChunk::AddResultType::kInsert,   // 2
                                                          TSChunk::AddResultType::kBlock,    // duplicate 3
                                                          TSChunk::AddResultType::kInsert,   // 4
                                                          TSChunk::AddResultType::kInsert,   // 13
                                                          TSChunk::AddResultType::kOld,      // 1 (older than retention)
                                                          TSChunk::AddResultType::kInsert};  // 14

  std::vector<uint64_t> expected_ts = {1, 3, 2, 0, 4, 13, 0, 14};

  for (size_t i = 0; i < results.size(); ++i) {
    EXPECT_EQ(results[i].type, expected_results[i]) << "Result mismatch at index " << i;
    if (expected_results[i] == TSChunk::AddResultType::kInsert) {
      EXPECT_EQ(results[i].sample.ts, expected_ts[i]) << "Timestamp mismatch at index " << i;
    }
  }
  s = ts_db_->MAdd(*ctx_, key_, {{14, 0}}, &results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].type, TSChunk::AddResultType::kBlock);
}

TEST_F(TimeSeriesTest, Range) {
  redis::TSCreateOption option;
  option.labels = {{"type", "stock"}, {"name", "A"}};
  auto s = ts_db_->Create(*ctx_, key_, option);
  EXPECT_TRUE(s.ok());

  // Add three batches of samples
  std::vector<TSSample> samples1 = {{1000, 100}, {1010, 110}, {1020, 120}};
  std::vector<TSChunk::AddResult> results1;
  results1.resize(samples1.size());
  s = ts_db_->MAdd(*ctx_, key_, samples1, &results1);
  EXPECT_TRUE(s.ok());

  std::vector<TSSample> samples2 = {{2000, 200}, {2010, 210}, {2020, 220}};
  std::vector<TSChunk::AddResult> results2;
  results2.resize(samples2.size());
  s = ts_db_->MAdd(*ctx_, key_, samples2, &results2);
  EXPECT_TRUE(s.ok());

  std::vector<TSSample> samples3 = {{3000, 300}, {3010, 310}, {3020, 320}};
  std::vector<TSChunk::AddResult> results3;
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
  for (const auto &sample : res) {
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

TEST_F(TimeSeriesTest, Get) {
  redis::TSCreateOption option;
  auto s = ts_db_->Create(*ctx_, key_, option);
  EXPECT_TRUE(s.ok());

  std::vector<TSSample> res;
  // Test empty timeseries
  s = ts_db_->Get(*ctx_, key_, false, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 0);

  // Add multiple samples
  std::vector<TSSample> samples = {{1, 10}, {2, 20}, {3, 30}};
  std::vector<TSChunk::AddResult> results;
  results.resize(samples.size());

  s = ts_db_->MAdd(*ctx_, key_, samples, &results);
  EXPECT_TRUE(s.ok());

  // Test basic GET (returns latest sample)
  s = ts_db_->Get(*ctx_, key_, false, &res);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(res.size(), 1);
  EXPECT_EQ(res[0].ts, 3);
  EXPECT_EQ(res[0].v, 30);

  // Test GET with empty timeseries
  std::vector<TSSample> empty_res;
  s = ts_db_->Get(*ctx_, "nonexistent_key", false, &empty_res);
  EXPECT_FALSE(s.ok());
}

TEST_F(TimeSeriesTest, CreateRuleErrorCases) {
  std::string src_key = "error_src";
  std::string dst_key = "error_dst";
  std::string another_key = "another_dst";
  std::string another_src = "another_src";
  std::string src_of_src = "src_of_src";
  redis::TSAggregator aggregator;
  aggregator.type = redis::TSAggregatorType::AVG;
  aggregator.bucket_duration = 1000;
  aggregator.alignment = 0;

  // 1. Source key equals destination key
  {
    redis::TSCreateRuleResult res = redis::TSCreateRuleResult::kOK;
    auto s = ts_db_->CreateRule(*ctx_, src_key, src_key, aggregator, &res);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(res, redis::TSCreateRuleResult::kSrcEqDst);
  }

  // 2. Source key does not exist
  {
    redis::TSCreateRuleResult res = redis::TSCreateRuleResult::kOK;
    auto s = ts_db_->CreateRule(*ctx_, src_key, dst_key, aggregator, &res);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(res, redis::TSCreateRuleResult::kSrcNotExist);
  }

  // Create source key
  redis::TSCreateOption option;
  auto s = ts_db_->Create(*ctx_, src_key, option);
  EXPECT_TRUE(s.ok());

  // 3. Destination key does not exist
  {
    redis::TSCreateRuleResult res = redis::TSCreateRuleResult::kOK;
    auto s = ts_db_->CreateRule(*ctx_, src_key, dst_key, aggregator, &res);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(res, redis::TSCreateRuleResult::kDstNotExist);
  }

  // Create destination key
  s = ts_db_->Create(*ctx_, dst_key, option);
  EXPECT_TRUE(s.ok());

  // 4. Source key already has a source rule
  {
    s = ts_db_->Create(*ctx_, src_of_src, option);
    EXPECT_TRUE(s.ok());

    redis::TSCreateRuleResult res = redis::TSCreateRuleResult::kOK;
    redis::TSAggregator aggregator2;
    aggregator2.type = redis::TSAggregatorType::AVG;
    aggregator2.bucket_duration = 1000;
    s = ts_db_->CreateRule(*ctx_, src_of_src, src_key, aggregator2, &res);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(res, redis::TSCreateRuleResult::kOK);

    ts_db_->Create(*ctx_, another_key, option);
    redis::TSCreateRuleResult res2 = redis::TSCreateRuleResult::kOK;
    auto s2 = ts_db_->CreateRule(*ctx_, src_key, another_key, aggregator, &res2);
    EXPECT_TRUE(s2.ok());
    EXPECT_EQ(res2, redis::TSCreateRuleResult::kSrcHasSourceRule);
  }

  // 5. Destination key already has a source rule
  {
    std::string src_for_dst = "src_for_dst";
    s = ts_db_->Create(*ctx_, src_for_dst, option);
    EXPECT_TRUE(s.ok());

    redis::TSCreateRuleResult res = redis::TSCreateRuleResult::kOK;
    redis::TSAggregator aggregator2;
    aggregator2.type = redis::TSAggregatorType::AVG;
    aggregator2.bucket_duration = 1000;
    s = ts_db_->CreateRule(*ctx_, src_for_dst, dst_key, aggregator2, &res);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(res, redis::TSCreateRuleResult::kOK);

    redis::TSCreateRuleResult res2 = redis::TSCreateRuleResult::kOK;
    s = ts_db_->Create(*ctx_, another_src, option);
    EXPECT_TRUE(s.ok());
    auto s2 = ts_db_->CreateRule(*ctx_, another_src, dst_key, aggregator, &res2);
    EXPECT_TRUE(s2.ok());
    EXPECT_EQ(res2, redis::TSCreateRuleResult::kDstHasSourceRule);
  }

  // 6. Destination key already has downstream rules
  {
    redis::TSCreateRuleResult res = redis::TSCreateRuleResult::kOK;
    redis::TSAggregator aggregator2;
    aggregator2.type = redis::TSAggregatorType::AVG;
    aggregator2.bucket_duration = 1000;
    s = ts_db_->CreateRule(*ctx_, another_src, another_key, aggregator2, &res);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(res, redis::TSCreateRuleResult::kOK);

    redis::TSCreateRuleResult res2 = redis::TSCreateRuleResult::kOK;
    auto s2 = ts_db_->CreateRule(*ctx_, another_src, src_of_src, aggregator, &res2);
    EXPECT_TRUE(s2.ok());
    EXPECT_EQ(res2, redis::TSCreateRuleResult::kDstHasDestRule);
  }
}

TEST_F(TimeSeriesTest, AggregationMultiple) {
  redis::TSCreateOption option;
  option.chunk_size = 3;
  const std::string key_src = "agg_test_multi";

  auto s = ts_db_->Create(*ctx_, key_src, option);
  EXPECT_TRUE(s.ok());

  // Define all aggregation types and their expected results
  struct AggregationTest {
    std::string suffix;
    redis::TSAggregatorType type;
    std::vector<std::pair<int64_t, double>> expected_results;
  };

  std::vector<AggregationTest> tests = {
      {"avg", redis::TSAggregatorType::AVG, {{0, 6.2}, {10, 27.666666666666668}}},
      {"sum", redis::TSAggregatorType::SUM, {{0, 31.0}, {10, 83.0}}},
      {"min", redis::TSAggregatorType::MIN, {{0, 1}, {10, 11}}},
      {"max", redis::TSAggregatorType::MAX, {{0, 15}, {10, 55}}},
      {"range", redis::TSAggregatorType::RANGE, {{0, 14}, {10, 44}}},
      {"count", redis::TSAggregatorType::COUNT, {{0, 5}, {10, 3}}},
      {"first", redis::TSAggregatorType::FIRST, {{0, 1}, {10, 11}}},
      {"last", redis::TSAggregatorType::LAST, {{0, 7}, {10, 55}}},
      {"std_p", redis::TSAggregatorType::STD_P, {{0, 4.955804677345548}, {10, 19.48218559493661}}},
      {"std_s", redis::TSAggregatorType::STD_S, {{0, 5.540758070878028}, {10, 23.860706890897706}}},
      {"var_p", redis::TSAggregatorType::VAR_P, {{0, 24.56000000000001}, {10, 379.5555555555555}}},
      {"var_s", redis::TSAggregatorType::VAR_S, {{0, 30.70000000000001}, {10, 569.3333333333333}}}};

  // Create all destination time series and aggregation rules
  redis::TSAggregator aggregator;
  aggregator.bucket_duration = 10;
  aggregator.alignment = 0;

  for (const auto &test : tests) {
    std::string dst_key = key_src + "_dst_" + test.suffix;
    s = ts_db_->Create(*ctx_, dst_key, option);
    EXPECT_TRUE(s.ok());

    redis::TSCreateRuleResult result = redis::TSCreateRuleResult::kOK;
    aggregator.type = test.type;
    s = ts_db_->CreateRule(*ctx_, key_src, dst_key, aggregator, &result);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(result, redis::TSCreateRuleResult::kOK);
  }

  // Add sample data
  std::vector<TSSample> samples = {{1, 1}, {2, 2}, {3, 6}, {5, 7}, {10, 11}, {11, 17}};
  std::vector<TSChunk::AddResult> add_results(samples.size());
  s = ts_db_->MAdd(*ctx_, key_src, samples, &add_results);
  EXPECT_TRUE(s.ok());

  samples = {{4, 15}, {12, 55}, {20, 65}};
  add_results.resize(samples.size());
  s = ts_db_->MAdd(*ctx_, key_src, samples, &add_results);
  EXPECT_TRUE(s.ok());

  // Test each aggregation type
  redis::TSRangeOption range_opt;
  range_opt.start_ts = 0;
  range_opt.end_ts = TSSample::MAX_TIMESTAMP;

  for (const auto &test : tests) {
    std::string dst_key = key_src + "_dst_" + test.suffix;

    std::vector<TSSample> res;
    s = ts_db_->Range(*ctx_, dst_key, range_opt, &res);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(res.size(), test.expected_results.size());

    for (size_t i = 0; i < res.size(); ++i) {
      EXPECT_EQ(res[i].ts, test.expected_results[i].first);
      EXPECT_NEAR(res[i].v, test.expected_results[i].second, 1e-5)
          << "Test failed for " << test.suffix << " at index " << i;
    }
  }
}

TEST_F(TimeSeriesTest, MGetFilterExprParse) {
  using TSMGetOption = redis::TSMGetOption;
  using TSMQueryFilterParser = redis::TSMQueryFilterParser;
  // Test 1: Valid single equality
  {
    TSMGetOption::FilterOption filter;
    TSMQueryFilterParser parser(filter);
    EXPECT_TRUE(parser.Parse("label=value").IsOK());
    EXPECT_EQ(filter.labels_equals.size(), 1);
    EXPECT_EQ(filter.labels_equals["label"], std::set<std::string>{"value"});
    EXPECT_TRUE(filter.labels_not_equals.empty());
    EXPECT_TRUE(parser.Check().IsOK());
  }

  // Test 2: Valid single not-equals
  {
    TSMGetOption::FilterOption filter;
    TSMQueryFilterParser parser(filter);
    EXPECT_TRUE(parser.Parse("label!=value").IsOK());
    EXPECT_TRUE(filter.labels_equals.empty());
    EXPECT_EQ(filter.labels_not_equals.size(), 1);
    EXPECT_EQ(filter.labels_not_equals["label"], std::set<std::string>{"value"});
    EXPECT_FALSE(parser.Check().IsOK());  // Fails because no matcher
  }

  // Test 3: Empty equality (label not exists)
  {
    TSMGetOption::FilterOption filter;
    TSMQueryFilterParser parser(filter);
    EXPECT_TRUE(parser.Parse("label=").IsOK());
    EXPECT_EQ(filter.labels_equals.size(), 1);
    EXPECT_TRUE(filter.labels_equals["label"].empty());
    EXPECT_FALSE(parser.Check().IsOK());  // Fails because no matcher
  }

  // Test 4: Empty not-equals (label exists)
  {
    TSMGetOption::FilterOption filter;
    TSMQueryFilterParser parser(filter);
    EXPECT_TRUE(parser.Parse("label!=").IsOK());
    EXPECT_EQ(filter.labels_not_equals.size(), 1);
    EXPECT_EQ(filter.labels_not_equals["label"], std::set<std::string>{""});
    EXPECT_FALSE(parser.Check().IsOK());  // Fails because no matcher
  }

  // Test 5: Multi-value equality
  {
    TSMGetOption::FilterOption filter;
    TSMQueryFilterParser parser(filter);
    EXPECT_TRUE(parser.Parse("label=('v1','v2',v3)").IsOK());
    std::set<std::string> expected{"v1", "v2", "v3"};
    EXPECT_EQ(filter.labels_equals["label"], expected);
    EXPECT_TRUE(parser.Check().IsOK());
  }

  // Test 6: Multi-value not-equals
  {
    TSMGetOption::FilterOption filter;
    TSMQueryFilterParser parser(filter);
    EXPECT_TRUE(parser.Parse("label!=(v1,\"v2\",'v3')").IsOK());
    std::set<std::string> expected{"v1", "v2", "v3"};
    EXPECT_EQ(filter.labels_not_equals["label"], expected);
    EXPECT_FALSE(parser.Check().IsOK());
  }

  // Test 7: Invalid expression (no operator)
  {
    TSMGetOption::FilterOption filter;
    TSMQueryFilterParser parser(filter);
    auto s = parser.Parse("label value");
    EXPECT_FALSE(s.IsOK());
    EXPECT_EQ(s.Msg(), "failed parsing labels");
  }

  // Test 8: Check failure conditions
  {
    // No conditions
    TSMGetOption::FilterOption filter1;
    TSMQueryFilterParser parser1(filter1);
    EXPECT_FALSE(parser1.Check().IsOK());
  }

  // Test 9: Label existence precedence
  {
    TSMGetOption::FilterOption filter;
    TSMQueryFilterParser parser1(filter);
    auto s = parser1.Parse("label=");  // Label not exists
    EXPECT_TRUE(s.IsOK());
    EXPECT_TRUE(filter.labels_equals["label"].empty());

    // Adding value to same label - should be ignored
    TSMQueryFilterParser parser2(filter);
    s = parser2.Parse("label=value");
    EXPECT_TRUE(s.IsOK());
    EXPECT_TRUE(filter.labels_equals["label"].empty());
    EXPECT_TRUE(parser2.Check().IsOK());
  }
}

TEST_F(TimeSeriesTest, MGetFilterExpression) {
  using TSCreateOption = redis::TSCreateOption;
  using TSMGetOption = redis::TSMGetOption;
  using TSMGetResult = redis::TSMGetResult;
  // Create time series with various labels
  {
    TSCreateOption option;
    option.labels = {{"type", "temperature"}, {"room", "study"}, {"id", "1"}};
    auto s = ts_db_->Create(*ctx_, "ts1", option);
    EXPECT_TRUE(s.ok());
  }
  {
    TSCreateOption option;
    option.labels = {{"type", "humidity"}, {"location", "home"}, {"id", "2"}};
    auto s = ts_db_->Create(*ctx_, "ts2", option);
    EXPECT_TRUE(s.ok());
  }
  {
    TSCreateOption option;
    option.labels = {{"type", "temperature"}, {"location", "office"}, {"id", "3"}};
    auto s = ts_db_->Create(*ctx_, "ts3", option);
    EXPECT_TRUE(s.ok());
  }
  {
    TSCreateOption option;
    option.labels = {{"room", "dining"}, {"id", "4"}, {"location", "new york"}};
    auto s = ts_db_->Create(*ctx_, "ts4", option);
    EXPECT_TRUE(s.ok());
  }

  // Test case: Exact value match (label=value)
  {
    TSMGetOption option;
    option.filter.labels_equals = {{"type", {"temperature"}}};
    std::vector<TSMGetResult> results;
    auto s = ts_db_->MGet(*ctx_, option, false, &results);
    EXPECT_TRUE(s.ok());
    std::set<std::string> expected{"ts1", "ts3"};
    std::set<std::string> actual;
    for (auto &res : results) {
      actual.insert(res.name);
    }
    EXPECT_EQ(actual, expected);
  }

  // Test case: Multiple conditions (label=value1 label=value2)
  {
    TSMGetOption option;
    option.filter.labels_equals = {{"type", {"temperature"}}, {"room", {"study"}}};
    std::vector<TSMGetResult> results;
    auto s = ts_db_->MGet(*ctx_, option, false, &results);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(results.size(), 1);
    if (!results.empty()) {
      EXPECT_EQ(results[0].name, "ts1");
    }
  }

  // Test case: List match (label=(v1,v2))
  {
    TSMGetOption option;
    option.filter.labels_equals = {{"type", {"temperature", "humidity"}}};
    std::vector<TSMGetResult> results;
    auto s = ts_db_->MGet(*ctx_, option, false, &results);
    EXPECT_TRUE(s.ok());
    std::set<std::string> expected{"ts1", "ts2", "ts3"};
    std::set<std::string> actual;
    for (auto &res : results) {
      actual.insert(res.name);
    }
    EXPECT_EQ(actual, expected);
  }

  // Test case: Negation match (label!=value)
  {
    TSMGetOption option;
    option.filter.labels_equals = {{"type", {"temperature"}}};           // type=temperature
    option.filter.labels_not_equals = {{"room", {"study", "bedroom"}}};  // room not in study or bedroom
    std::vector<TSMGetResult> results;
    auto s = ts_db_->MGet(*ctx_, option, false, &results);
    EXPECT_TRUE(s.ok());
    std::set<std::string> expected{"ts3"};
    std::set<std::string> actual;
    for (auto &res : results) {
      actual.insert(res.name);
    }
    EXPECT_EQ(actual, expected);
  }

  // Test case: Existence check (label!=)
  {
    TSMGetOption option;
    option.filter.labels_not_equals = {{"location", {""}}};     // location!=
    option.filter.labels_equals = {{"type", {"temperature"}}};  // type=temperature
    std::vector<TSMGetResult> results;
    auto s = ts_db_->MGet(*ctx_, option, false, &results);
    EXPECT_TRUE(s.ok());
    std::set<std::string> expected{"ts3"};
    std::set<std::string> actual;
    for (auto &res : results) {
      actual.insert(res.name);
    }
    EXPECT_EQ(actual, expected);
  }

  // Test case: List negation (label!=(v1,v2))
  {
    TSMGetOption option;
    option.filter.labels_equals = {{"type", {"temperature"}}};           // type=temperature
    option.filter.labels_not_equals = {{"room", {"study", "bedroom"}}};  // room!= (study,bedroom)
    std::vector<TSMGetResult> results;
    auto s = ts_db_->MGet(*ctx_, option, false, &results);
    EXPECT_TRUE(s.ok());
    std::set<std::string> expected{"ts3"};
    std::set<std::string> actual;
    for (auto &res : results) {
      actual.insert(res.name);
    }
    EXPECT_EQ(actual, expected);
  }

  // Test case: Quoted value with space
  {
    TSMGetOption mget_opt;
    mget_opt.filter.labels_equals = {{"location", {"new york"}}};
    std::vector<TSMGetResult> results;
    auto s = ts_db_->MGet(*ctx_, mget_opt, false, &results);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(results.size(), 1);
    if (!results.empty()) {
      EXPECT_EQ(results[0].name, "ts4");
    }
  }
}

TEST_F(TimeSeriesTest, MGet) {
  using TSCreateOption = redis::TSCreateOption;
  using TSMGetOption = redis::TSMGetOption;
  using TSMGetResult = redis::TSMGetResult;

  // Create two time series with temperature data for different locations
  TSCreateOption tlv_opt;
  tlv_opt.labels = {{"type", "temp"}, {"location", "TLV"}};
  auto s = ts_db_->Create(*ctx_, "temp:TLV", tlv_opt);
  EXPECT_TRUE(s.ok());

  TSCreateOption jlm_opt;
  jlm_opt.labels = {{"type", "temp"}, {"location", "JLM"}};
  s = ts_db_->Create(*ctx_, "temp:JLM", jlm_opt);
  EXPECT_TRUE(s.ok());

  // Add data points to TLV time series
  std::vector<TSSample> tlv_samples = {{1000, 30}, {1010, 35}, {1020, 9999}, {1030, 40}};
  std::vector<TSChunk::AddResult> tlv_results(tlv_samples.size());
  s = ts_db_->MAdd(*ctx_, "temp:TLV", tlv_samples, &tlv_results);
  EXPECT_TRUE(s.ok());

  // Add data points to JLM time series
  std::vector<TSSample> jlm_samples = {{1005, 30}, {1015, 35}, {1025, 9999}, {1035, 40}};
  std::vector<TSChunk::AddResult> jlm_results(jlm_samples.size());
  s = ts_db_->MAdd(*ctx_, "temp:JLM", jlm_samples, &jlm_results);
  EXPECT_TRUE(s.ok());

  // Test MGET with WITHLABELS
  {
    TSMGetOption mget_opt;
    mget_opt.filter.labels_equals = {{"type", {"temp"}}};
    mget_opt.with_labels = true;
    std::vector<TSMGetResult> results;
    s = ts_db_->MGet(*ctx_, mget_opt, false, &results);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(results.size(), 2);

    // Sort results to ensure consistent order for testing
    std::sort(results.begin(), results.end(),
              [](const TSMGetResult &a, const TSMGetResult &b) { return a.name < b.name; });

    // Check JLM result
    EXPECT_EQ(results[0].name, "temp:JLM");
    EXPECT_EQ(results[0].labels.size(), 2);
    EXPECT_EQ(results[0].labels[0].k, "location");
    EXPECT_EQ(results[0].labels[0].v, "JLM");
    EXPECT_EQ(results[0].labels[1].k, "type");
    EXPECT_EQ(results[0].labels[1].v, "temp");
    EXPECT_EQ(results[0].samples[0].ts, 1035);
    EXPECT_EQ(results[0].samples[0].v, 40);

    // Check TLV result
    EXPECT_EQ(results[1].name, "temp:TLV");
    EXPECT_EQ(results[1].labels.size(), 2);
    EXPECT_EQ(results[1].labels[0].k, "location");
    EXPECT_EQ(results[1].labels[0].v, "TLV");
    EXPECT_EQ(results[1].labels[1].k, "type");
    EXPECT_EQ(results[1].labels[1].v, "temp");
    EXPECT_EQ(results[1].samples[0].ts, 1030);
    EXPECT_EQ(results[1].samples[0].v, 40);
  }

  // Test MGET with SELECTED_LABELS
  {
    TSMGetOption mget_opt;
    mget_opt.filter.labels_equals = {{"type", {"temp"}}};
    mget_opt.selected_labels = {"location"};
    std::vector<TSMGetResult> results;
    s = ts_db_->MGet(*ctx_, mget_opt, true, &results);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(results.size(), 2);

    // Sort results to ensure consistent order for testing
    std::sort(results.begin(), results.end(),
              [](const TSMGetResult &a, const TSMGetResult &b) { return a.name < b.name; });

    // Check JLM result
    EXPECT_EQ(results[0].name, "temp:JLM");
    EXPECT_EQ(results[0].labels.size(), 1);  // Only location should be present
    EXPECT_EQ(results[0].labels[0].k, "location");
    EXPECT_EQ(results[0].labels[0].v, "JLM");
    EXPECT_EQ(results[0].samples[0].ts, 1035);
    EXPECT_EQ(results[0].samples[0].v, 40);

    // Check TLV result
    EXPECT_EQ(results[1].name, "temp:TLV");
    EXPECT_EQ(results[1].labels.size(), 1);  // Only location should be present
    EXPECT_EQ(results[1].labels[0].k, "location");
    EXPECT_EQ(results[1].labels[0].v, "TLV");
    EXPECT_EQ(results[1].samples[0].ts, 1030);
    EXPECT_EQ(results[1].samples[0].v, 40);
  }
}

TEST_F(TimeSeriesTest, MRangeGroupSamplesAndReduce) {
  using TSMRangeOption = redis::TSMRangeOption;
  // Test Case: SUM and AVG Reducer
  {
    std::vector<std::vector<TSSample>> all_samples;
    all_samples.push_back({{1, 2.0}, {2, 3.0}});
    all_samples.push_back({{1, 3.0}, {3, 3.0}});
    all_samples.push_back({{1, 1.0}, {4, 2.0}});

    std::vector<TSSample> expected = {{1, 6.0}, {2, 3.0}, {3, 3.0}, {4, 2.0}};
    auto actual = GroupSamplesAndReduce(all_samples, TSMRangeOption::GroupReducerType::SUM);
    EXPECT_EQ(actual, expected);

    expected = {{1, 3.0}, {2, 3.0}, {3, 3.0}, {4, 2.0}};
    actual = GroupSamplesAndReduce(all_samples, TSMRangeOption::GroupReducerType::AVG);
    EXPECT_EQ(actual, expected);
  }

  // Test Case: MIN and MAX Reducers
  {
    std::vector<std::vector<TSSample>> all_samples;
    all_samples.push_back({{1, 10.0}, {5, 100.0}});
    all_samples.push_back({{1, -5.0}, {5, 200.0}});
    all_samples.push_back({{1, 20.0}, {5, -50.0}});

    std::vector<TSSample> expected_min = {{1, -5.0}, {5, -50.0}};
    std::vector<TSSample> expected_max = {{1, 20.0}, {5, 200.0}};

    auto actual_min =
        GroupSamplesAndReduce(std::vector<std::vector<TSSample>>(all_samples), TSMRangeOption::GroupReducerType::MIN);
    EXPECT_EQ(actual_min, expected_min);

    auto actual_max =
        GroupSamplesAndReduce(std::vector<std::vector<TSSample>>(all_samples), TSMRangeOption::GroupReducerType::MAX);
    EXPECT_EQ(actual_max, expected_max);
  }

  // Test Case: COUNT Reducer
  {
    std::vector<std::vector<TSSample>> all_samples;
    all_samples.push_back({{1, 2.0}, {2, 3.0}});
    all_samples.push_back({{1, 3.0}, {3, 3.0}});
    all_samples.push_back({{1, 1.0}, {4, 2.0}});
    all_samples.push_back({{2, 9.0}});  // Add another sample at ts=2

    // ts=1 has 3 samples, ts=2 has 2 samples, ts=3 has 1, ts=4 has 1
    std::vector<TSSample> expected = {{1, 3.0}, {2, 2.0}, {3, 1.0}, {4, 1.0}};
    auto actual = GroupSamplesAndReduce(all_samples, TSMRangeOption::GroupReducerType::COUNT);
    EXPECT_EQ(actual, expected);
  }

  // Test Case : Edge Case - Empty Input Vector
  {
    std::vector<std::vector<TSSample>> all_samples;

    auto actual = GroupSamplesAndReduce(all_samples, TSMRangeOption::GroupReducerType::SUM);
    EXPECT_TRUE(actual.empty());
  }

  // Test Case : Edge Case - Input with Empty Sub-Vectors
  {
    std::vector<std::vector<TSSample>> all_samples;
    all_samples.push_back({{1, 10.0}});
    all_samples.emplace_back();  // Empty vector
    all_samples.push_back({{1, 20.0}});
    all_samples.emplace_back();  // Another empty vector

    const std::vector<TSSample> &expected = {{1, 30.0}};
    auto actual = GroupSamplesAndReduce(all_samples, TSMRangeOption::GroupReducerType::SUM);
    EXPECT_EQ(actual, expected);
  }

  // Test Case : Edge Case - Single Input Vector
  {
    std::vector<std::vector<TSSample>> all_samples;
    std::vector<TSSample> single_vector = {{10, 1.0}, {20, 2.0}};
    all_samples.push_back(single_vector);

    // Expected is the same as input
    std::vector<TSSample> &expected = single_vector;
    auto actual = GroupSamplesAndReduce(all_samples, TSMRangeOption::GroupReducerType::SUM);
    EXPECT_EQ(actual, expected);
  }

  // Test Case : Edge Case - No Overlapping Timestamps
  {
    std::vector<std::vector<TSSample>> all_samples;
    all_samples.push_back({{1, 1.0}, {4, 4.0}});
    all_samples.push_back({{2, 2.0}, {5, 5.0}});
    all_samples.push_back({{3, 3.0}, {6, 6.0}});

    // Expected is a simple sorted merge
    std::vector<TSSample> expected = {{1, 1.0}, {2, 2.0}, {3, 3.0}, {4, 4.0}, {5, 5.0}, {6, 6.0}};
    auto actual = GroupSamplesAndReduce(all_samples, TSMRangeOption::GroupReducerType::SUM);
    EXPECT_EQ(actual, expected);
  }

  // Test Case: Edge Case - ReducerType is NONE
  {
    std::vector<std::vector<TSSample>> all_samples;
    all_samples.push_back({{1, 2.0}, {2, 3.0}});
    all_samples.push_back({{1, 3.0}, {3, 3.0}});

    auto actual = GroupSamplesAndReduce(all_samples, TSMRangeOption::GroupReducerType::NONE);
    EXPECT_TRUE(actual.empty());
  }
}

TEST_F(TimeSeriesTest, DelComprehensive) {
  using TSCreateOption = redis::TSCreateOption;
  using TSRangeOption = redis::TSRangeOption;
  // Create time series
  auto s = ts_db_->Create(*ctx_, "test1", TSCreateOption());
  EXPECT_TRUE(s.ok());
  s = ts_db_->Create(*ctx_, "test2", TSCreateOption());
  EXPECT_TRUE(s.ok());
  s = ts_db_->Create(*ctx_, "test3", TSCreateOption());
  EXPECT_TRUE(s.ok());
  s = ts_db_->Create(*ctx_, "test4", TSCreateOption());
  EXPECT_TRUE(s.ok());

  // Create rules
  redis::TSAggregator aggregator;
  redis::TSCreateRuleResult res = redis::TSCreateRuleResult::kOK;

  aggregator.type = redis::TSAggregatorType::SUM;
  aggregator.bucket_duration = 10;
  s = ts_db_->CreateRule(*ctx_, "test1", "test2", aggregator, &res);
  EXPECT_TRUE(s.ok());
  aggregator.bucket_duration = 200;
  s = ts_db_->CreateRule(*ctx_, "test1", "test4", aggregator, &res);
  EXPECT_TRUE(s.ok());
  aggregator.bucket_duration = 20;
  aggregator.alignment = 10;
  s = ts_db_->CreateRule(*ctx_, "test1", "test3", aggregator, &res);
  EXPECT_TRUE(s.ok());

  // Add samples
  std::vector<TSSample> samples = {{1, 1},   {2, 2},   {11, 11}, {15, 15}, {16, 16}, {21, 21},
                                   {24, 24}, {31, 31}, {35, 35}, {39, 39}, {42, 42}, {49, 49}};
  std::vector<TSChunk::AddResult> results;
  results.resize(samples.size());
  s = ts_db_->MAdd(*ctx_, "test1", samples, &results);
  EXPECT_TRUE(s.ok());

  // Delete samples between timestamps 10 and 40
  uint64_t deleted = 0;
  s = ts_db_->Del(*ctx_, "test1", 12, 40, &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 7);

  // Validate downstream ranges
  std::vector<std::string> keys = {"test2", "test3", "test4"};
  auto check = [&](const std::vector<std::vector<TSSample>> &expected_samples) {
    for (size_t i = 0; i < keys.size(); ++i) {
      std::vector<TSSample> res;
      TSRangeOption option;
      option.start_ts = 0;
      option.end_ts = TSSample::MAX_TIMESTAMP;
      s = ts_db_->Range(*ctx_, keys[i], option, &res);
      EXPECT_TRUE(s.ok());
      EXPECT_EQ(res, expected_samples[i]);
    }
  };
  std::vector<std::vector<TSSample>> expected_samples = {
      {{0, 3}, {10, 11}},  // test2
      {{0, 3}, {10, 11}},  // test3
      {}                   // test4
  };
  check(expected_samples);

  // Add new samples
  TSSample new_sample{50, 50};
  TSChunk::AddResult add_result;
  s = ts_db_->Add(*ctx_, "test1", new_sample, TSCreateOption(), &add_result);
  EXPECT_TRUE(s.ok());
  // Validate updated ranges
  expected_samples = {
      {{0, 3}, {10, 11}, {40, 91}},  // test2
      {{0, 3}, {10, 11}, {30, 91}},  // test3
      {}                             // test4
  };
  check(expected_samples);

  // Add final sample
  TSSample final_sample{200, 200};
  s = ts_db_->Add(*ctx_, "test1", final_sample, TSCreateOption(), &add_result);
  EXPECT_TRUE(s.ok());
  // Validate final ranges
  expected_samples = {
      {{0, 3}, {10, 11}, {40, 91}, {50, 50}},  // test2
      {{0, 3}, {10, 11}, {30, 91}, {50, 50}},  // test3
      {{0, 155}}                               // test4
  };
  check(expected_samples);
}
