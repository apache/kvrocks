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

#include "types/tdigest.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <random>
#include <range/v3/algorithm/shuffle.hpp>
#include <range/v3/range.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/transform.hpp>
#include <string>
#include <vector>

#include "logging.h"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "time_util.h"
#include "types/redis_tdigest.h"

namespace {
constexpr std::random_device::result_type kSeed = 14863;  // fixed seed for reproducibility

std::vector<double> QuantileOf(const std::vector<double> &samples, const std::vector<double> &qs) {
  std::vector<double> result;
  result.reserve(qs.size());
  std::vector<double> sorted_samples = samples;
  std::sort(sorted_samples.begin(), sorted_samples.end());
  for (auto q : qs) {
    auto index = q * static_cast<double>(sorted_samples.size());
    if (index <= 1) {
      result.push_back(sorted_samples.front());
    } else if (index >= static_cast<double>(sorted_samples.size() - 1)) {
      result.push_back(sorted_samples.back());
    } else {
      auto left = sorted_samples[static_cast<int>(index)];
      auto right = sorted_samples[static_cast<int>(index) + 1];
      auto diff = index - static_cast<int>(index);
      result.push_back(left + (right - left) * diff);
    }
  }
  return result;
}

std::vector<std::pair<double, double>> QuantileIntervalOf(const std::vector<double> &samples,
                                                          const std::vector<double> &qs) {
  std::vector<std::pair<double, double>> result;
  result.reserve(qs.size());
  std::vector<double> sorted_samples = samples;
  std::sort(sorted_samples.begin(), sorted_samples.end());
  for (auto q : qs) {
    auto index = q * static_cast<double>(sorted_samples.size());
    if (index <= 1) {
      result.emplace_back(sorted_samples.front(), sorted_samples.front());
    } else if (index >= static_cast<double>(sorted_samples.size() - 1)) {
      result.emplace_back(sorted_samples.back(), sorted_samples.back());
    } else {
      auto left = sorted_samples[static_cast<int>(index)];
      auto right = sorted_samples[static_cast<int>(index) + 1];
      result.emplace_back(left, right);
    }
  }
  return result;
}

std::vector<double> GenerateSamples(int count, double from, double to) {
  std::vector<double> samples;
  samples.reserve(count);
  for (int i = 0; i < count; i++) {
    samples.push_back(from + static_cast<double>(i) * (to - from) / static_cast<double>(count));
  }
  return samples;
}

std::vector<double> GenerateQuantiles(int count, bool with_head = false, bool with_tail = false) {
  std::vector<double> qs;
  qs.reserve(count);
  for (int i = 1; i <= count; i++) {
    qs.push_back(static_cast<double>(i) / static_cast<double>(count));
  }
  if (with_head) {
    qs.insert(qs.begin(), 0);
  }
  if (with_tail) {
    qs.push_back(1);
  }
  return qs;
}

}  // namespace

class RedisTDigestTest : public TestBase {
 protected:
  RedisTDigestTest() : name_("tdigest_test") {
    tdigest_ = std::make_unique<redis::TDigest>(storage_.get(), "tdigest_ns");
  }

  std::string name_;
  std::unique_ptr<redis::TDigest> tdigest_;
};

TEST_F(RedisTDigestTest, CentroidTest) {
  Centroid c1{
      2.,
      3.,
  };
  Centroid c2{
      3.,
      4.,
  };

  c1.Merge(c2);

  EXPECT_NEAR(c1.weight, 7., 0.01);
  EXPECT_NEAR(c1.mean, 2.57, 0.01);
}

TEST_F(RedisTDigestTest, Create) {
  std::string test_digest_name = "test_digest_create" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_TRUE(exists);
  ASSERT_TRUE(status.IsInvalidArgument());

  TDigestMetadata metadata;
  auto get_status = tdigest_->GetMetaData(*ctx_, test_digest_name, &metadata);
  ASSERT_TRUE(get_status.ok()) << get_status.ToString();
  ASSERT_EQ(metadata.compression, 100) << metadata.compression;
}

TEST_F(RedisTDigestTest, Quantile) {
  std::string test_digest_name = "test_digest_quantile" + std::to_string(util::GetTimeStampMS());

  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());
  std::vector<double> samples = ranges::views::iota(1, 101) | ranges::views::transform([](int i) { return i; }) |
                                ranges::to<std::vector<double>>();

  status = tdigest_->Add(*ctx_, test_digest_name, samples);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<double> qs = {0.5, 0.9, 0.99};
  redis::TDigestQuantitleResult result;
  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(result.quantiles);
  ASSERT_EQ(result.quantiles->size(), qs.size());
  EXPECT_NEAR((*result.quantiles)[0], 50.5, 0.01);
  EXPECT_NEAR((*result.quantiles)[1], 90.5, 0.01);
  EXPECT_NEAR((*result.quantiles)[2], 100, 0.01);
}

TEST_F(RedisTDigestTest, PlentyQuantile_10000_144) {
  std::string test_digest_name = "test_digest_quantile" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  int sample_count = 10000;
  int quantile_count = 144;
  double from = -100;
  double to = 100;
  auto error_double = (to - from) / sample_count;
  auto samples = GenerateSamples(sample_count, -100, 100);
  status = tdigest_->Add(*ctx_, test_digest_name, samples);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto qs = GenerateQuantiles(quantile_count);
  auto result = QuantileOf(samples, qs);

  redis::TDigestQuantitleResult tdigest_result;
  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &tdigest_result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(tdigest_result.quantiles);

  for (int i = 0; i < quantile_count; i++) {
    EXPECT_NEAR((*tdigest_result.quantiles)[i], result[i], error_double) << "quantile is: " << qs[i];
  }
}

TEST_F(RedisTDigestTest, Add_2_times) {
  std::string test_digest_name = "test_digest_quantile" + std::to_string(util::GetTimeStampMS());

  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  int sample_count = 17;
  int quantile_count = 7;
  auto samples = GenerateSamples(sample_count, -100, 100);
  auto qs = GenerateQuantiles(quantile_count);
  auto expect_result = QuantileIntervalOf(samples, qs);
  std::shuffle(samples.begin(), samples.end(), std::mt19937(kSeed));

  int group_count = 4;
  auto samples_sub_group =
      samples | ranges::views::chunk(sample_count / group_count) | ranges::to<std::vector<std::vector<double>>>();

  for (const auto &s : samples_sub_group) {
    status = tdigest_->Add(*ctx_, test_digest_name, s);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  redis::TDigestQuantitleResult tdigest_result;
  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &tdigest_result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(tdigest_result.quantiles);

  for (int i = 0; i < quantile_count; i++) {
    auto &[expect_down, expect_upper] = expect_result[i];
    auto got = (*tdigest_result.quantiles)[i];
    EXPECT_GE(got, expect_down) << fmt::format("quantile is {}, should in interval [{}, {}]", qs[i], expect_down,
                                               expect_upper);
    EXPECT_LE(got, expect_upper) << fmt::format("quantile is {}, should in interval [{}, {}]", qs[i], expect_down,
                                                expect_upper);
  }
}

TEST_F(RedisTDigestTest, Add_100_times_same_value) {
  std::string test_digest_name = "test_digest_quantile" + std::to_string(util::GetTimeStampMS());

  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  auto samples = std::vector<double>{-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
  auto qs = std::vector<double>{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99};

  auto repeat_times = 100;

  for (auto i = 0; i < repeat_times; ++i) {
    std::shuffle(samples.begin(), samples.end(), std::mt19937(kSeed));
    status = tdigest_->Add(*ctx_, test_digest_name, samples);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  redis::TDigestQuantitleResult tdigest_result;
  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &tdigest_result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(tdigest_result.quantiles);

  auto expect_result = std::vector<double>{
      -10, -9, -8, -5, 1, 7, 10, 11, 12,
  };

  EXPECT_EQ(tdigest_result.quantiles->size(), qs.size());

  for (size_t i = 0; i < qs.size(); i++) {
    auto got = (*tdigest_result.quantiles)[i];
    EXPECT_NEAR(got, expect_result[i], 0.5) << fmt::format("quantile is {}, should be {}", qs[i], expect_result[i]);
  }
}
TEST_F(RedisTDigestTest, Quantile_returns_nan_on_empty_tdigest) {
  std::string test_digest_name = "test_digest_nan" + std::to_string(util::GetTimeStampMS());

  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  std::vector<double> qs = {0.3, 0.1, 0.2, 0.56, 0.44, 0.12, 0.11};
  redis::TDigestQuantitleResult result;

  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_FALSE(result.quantiles) << "should not have quantiles with empty tdigest";
}

TEST_F(RedisTDigestTest, RevRank_and_Rank_on_the_set_containing_different_elements) {
  std::string test_digest_name = "test_digest_revrank" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());
  std::vector<double> input{10, 20, 30, 40, 50, 60};
  status = tdigest_->Add(*ctx_, test_digest_name, input);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<int> result;
  result.reserve(input.size());
  const std::vector<double> value = {0, 10, 20, 30, 40, 50, 60, 70};
  status = tdigest_->RevRank(*ctx_, test_digest_name, value, &result);
  const auto expect_result_revrank = std::vector<double>{6, 5, 4, 3, 2, 1, 0, -1};

  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_revrank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  result.clear();
  result.reserve(input.size());
  status = tdigest_->Rank(*ctx_, test_digest_name, value, &result);
  const auto expect_result_rank = std::vector<double>{-1, 0, 1, 2, 3, 4, 5, 6};
  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_rank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(RedisTDigestTest, RevRank_and_Rank_on_the_set_containing_several_identical_elements) {
  std::string test_digest_name = "test_digest_revrank_and_rank" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());
  std::vector<double> input{10, 10, 10, 20, 20};
  status = tdigest_->Add(*ctx_, test_digest_name, input);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<int> result;
  const std::vector<double> value = {10, 20};
  result.reserve(value.size());
  status = tdigest_->RevRank(*ctx_, test_digest_name, value, &result);
  const auto expect_result_revrank = std::vector<double>{3, 1};
  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_revrank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  result.clear();
  result.reserve(value.size());
  status = tdigest_->Rank(*ctx_, test_digest_name, value, &result);
  const auto expect_result_rank = std::vector<double>{1, 4};
  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_rank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  status = tdigest_->Add(*ctx_, test_digest_name, std::vector<double>{10});
  ASSERT_TRUE(status.ok()) << status.ToString();

  result.clear();
  result.reserve(value.size());
  status = tdigest_->RevRank(*ctx_, test_digest_name, value, &result);
  const auto expect_result_new_revrank = std::vector<double>{4, 1};
  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_new_revrank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  result.clear();
  result.reserve(value.size());
  status = tdigest_->Rank(*ctx_, test_digest_name, value, &result);
  const auto expect_result_new_rank = std::vector<double>{2, 5};
  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_new_rank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(RedisTDigestTest, RevRank_and_Rank_on_empty_tdigest) {
  std::string test_digest_name = "test_digest_revrank_and_rank" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  std::vector<int> result;
  result.reserve(2);
  const std::vector<double> value = {10, 20};
  status = tdigest_->RevRank(*ctx_, test_digest_name, value, &result);
  const auto expect_result_revrank = std::vector<double>{-2, -2};
  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_revrank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  result.clear();
  result.reserve(2);
  status = tdigest_->Rank(*ctx_, test_digest_name, value, &result);
  const auto expect_result_rank = std::vector<double>{-2, -2};
  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_rank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(RedisTDigestTest, RevRank_and_Rank_on_different_or_same_and_unordered_inputs_tdigest) {
  std::string test_digest_name = "test_digest_revrank_and_rank" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  std::vector<double> input{12, 100, 50, 36, 75, 81, 35.5, 46, 36, 8.8, 15, 4, 32.5, 12, 8.8, 7, 99, 0};
  status = tdigest_->Add(*ctx_, test_digest_name, input);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<int> result;
  const std::vector<double> value = {50, 36, 4, 99, 8.8};
  result.reserve(value.size());
  status = tdigest_->Rank(*ctx_, test_digest_name, value, &result);
  const auto expect_result_rank = std::vector<double>{13, 11, 1, 16, 4};
  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_rank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  const std::vector<double> value_new = {50, 36, 4, 99, 8.8, 12};
  result.clear();
  result.reserve(value_new.size());
  status = tdigest_->RevRank(*ctx_, test_digest_name, value_new, &result);
  const auto expect_result_revrank = std::vector<double>{4, 7, 16, 1, 14, 12};
  for (size_t i = 0; i < result.size(); i++) {
    auto got = result[i];
    EXPECT_EQ(got, expect_result_revrank[i]);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(RedisTDigestTest, ByRank_And_ByRevRank) {
  std::string test_digest_name = "test_digest_byrank_and_byrevrank" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  // Test 1: Empty TDigest should return NaN
  std::vector<double> result;
  std::vector<int> value = {1, 2};
  result.reserve(value.size());
  status = tdigest_->ByRank(*ctx_, test_digest_name, value, &result);
  for (size_t i = 0; i < result.size(); i++) {
    EXPECT_TRUE(std::isnan(result[i])) << "Expected NaN at index " << i << ", got " << result[i];
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  result.clear();
  result.reserve(value.size());
  status = tdigest_->ByRevRank(*ctx_, test_digest_name, value, &result);
  for (size_t i = 0; i < result.size(); i++) {
    EXPECT_TRUE(std::isnan(result[i])) << "Expected NaN at index " << i << ", got " << result[i];
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Test 2: Add values and test ByRank
  // Add values: 1 2 2 3 3 3 4 4 4 4 5 5 5 5 5 (15 values)
  std::vector<double> values = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5};
  status = tdigest_->Add(*ctx_, test_digest_name, values);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Test ByRank: rank 0 should be min, increasing ranks should give increasing values
  std::vector<int> ranks = {0, 1, 2, 3, 6, 9, 10, 14, 15};
  std::vector<double> expected_values = {
      1.0, 2.0, 2.0, 3.0, 4.0, 4.0, 5.0, 5.0, std::numeric_limits<double>::infinity()};
  result.clear();
  status = tdigest_->ByRank(*ctx_, test_digest_name, ranks, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(result.size(), ranks.size());

  for (size_t i = 0; i < result.size(); i++) {
    if (std::isinf(expected_values[i])) {
      EXPECT_TRUE(std::isinf(result[i])) << "Expected inf at rank " << ranks[i] << ", got " << result[i];
    } else {
      EXPECT_DOUBLE_EQ(result[i], expected_values[i])
          << "ByRank mismatch at rank " << ranks[i] << ": expected " << expected_values[i] << ", got " << result[i];
    }
  }

  // Test ByRevRank: rank 0 should be max, increasing ranks should give decreasing values
  std::vector<double> expected_revvalues = {
      5.0, 5.0, 5.0, 5.0, 4.0, 3.0, 3.0, 1.0, -std::numeric_limits<double>::infinity()};
  result.clear();
  status = tdigest_->ByRevRank(*ctx_, test_digest_name, ranks, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(result.size(), ranks.size());

  for (size_t i = 0; i < result.size(); i++) {
    if (std::isinf(expected_revvalues[i])) {
      EXPECT_TRUE(std::isinf(result[i])) << "Expected inf at revrank " << ranks[i] << ", got " << result[i];
    } else {
      EXPECT_DOUBLE_EQ(result[i], expected_revvalues[i]) << "ByRevRank mismatch at rank " << ranks[i] << ": expected "
                                                         << expected_revvalues[i] << ", got " << result[i];
    }
  }

  // Test 3: Test boundary conditions
  std::vector<int> boundary_ranks = {0, 7, 14, 100};
  result.clear();
  status = tdigest_->ByRank(*ctx_, test_digest_name, boundary_ranks, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(result[0], 1.0) << "Rank 0 should be minimum";
  EXPECT_TRUE(std::isinf(result[3])) << "Rank >= total_weight should be infinity";
}

TEST_F(RedisTDigestTest, TrimmedMean) {
  std::string test_digest_name = "test_digest_trimmed_mean" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  std::vector<double> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  status = tdigest_->Add(*ctx_, test_digest_name, values);
  ASSERT_TRUE(status.ok()) << status.ToString();

  redis::TDigestTrimmedMeanResult result;
  status = tdigest_->TrimmedMean(*ctx_, test_digest_name, 0.1, 0.9, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(result.mean.has_value());
  EXPECT_NEAR(*result.mean, 5.5, 0.01);

  status = tdigest_->TrimmedMean(*ctx_, test_digest_name, 0.0, 1.0, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(result.mean.has_value());
  EXPECT_NEAR(*result.mean, 5.5, 0.01);

  status = tdigest_->TrimmedMean(*ctx_, test_digest_name, 0.25, 0.75, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(result.mean.has_value());
  EXPECT_NEAR(*result.mean, 5.5, 0.01);
}

TEST_F(RedisTDigestTest, TrimmedMeanEmptyDigest) {
  std::string test_digest_name = "test_digest_trimmed_mean_empty" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  redis::TDigestTrimmedMeanResult result;
  status = tdigest_->TrimmedMean(*ctx_, test_digest_name, 0.1, 0.9, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_FALSE(result.mean.has_value());
}

TEST_F(RedisTDigestTest, TrimmedMeanUnorderedInput) {
  std::string test_digest_name = "test_digest_trimmed_mean_unordered" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  std::vector<double> values = {5, 2, 8, 1, 9, 3, 7, 4, 6, 10};
  status = tdigest_->Add(*ctx_, test_digest_name, values);
  ASSERT_TRUE(status.ok()) << status.ToString();

  redis::TDigestTrimmedMeanResult result;
  status = tdigest_->TrimmedMean(*ctx_, test_digest_name, 0.1, 0.9, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(result.mean.has_value());
  EXPECT_NEAR(*result.mean, 5.5, 0.01);
}

TEST_F(RedisTDigestTest, TrimmedMeanComplexInput) {
  std::string test_digest_name = "test_digest_trimmed_mean_complex" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  std::vector<double> values = {-10, 5, -3, 5, 0, 5, 3, -5, 10, -10};
  status = tdigest_->Add(*ctx_, test_digest_name, values);
  ASSERT_TRUE(status.ok()) << status.ToString();

  redis::TDigestTrimmedMeanResult result;
  status = tdigest_->TrimmedMean(*ctx_, test_digest_name, 0.2, 0.8, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(result.mean.has_value());
  ASSERT_FALSE(std::isnan(*result.mean));
  EXPECT_NEAR(*result.mean, 5.0 / 6.0, 0.01);
}

TEST_F(RedisTDigestTest, MergeIntoExistingDestWithoutOverride) {
  // Test: When dest exists without OVERRIDE flag, merge dest + sources together (Redis behavior)
  std::string ts = std::to_string(util::GetTimeStampMS());
  std::string src1 = "tdigest_merge_src1_" + ts;
  std::string src2 = "tdigest_merge_src2_" + ts;
  std::string dest = "tdigest_merge_dest_" + ts;

  bool exists = false;
  // Create source1 with values: 1, 2, 3
  ASSERT_TRUE(tdigest_->Create(*ctx_, src1, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src1, {1.0, 2.0, 3.0}).ok());

  // Create source2 with values: 4, 5, 6, 100, -200
  ASSERT_TRUE(tdigest_->Create(*ctx_, src2, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src2, {4.0, 5.0, 6.0, 100.0, -200.0}).ok());

  // Create dest with values: 7, 8, 9
  ASSERT_TRUE(tdigest_->Create(*ctx_, dest, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, dest, {7.0, 8.0, 9.0}).ok());

  // Merge sources into existing dest without OVERRIDE
  // Should merge dest(3) + src1(3) + src2(5) = 11 observations
  redis::TDigestMergeOptions options;
  options.override_flag = false;
  auto status = tdigest_->Merge(*ctx_, dest, {src1, src2}, options);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Verify total observations: 3 + 3 + 5 = 11
  TDigestMetadata metadata;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata).ok());
  EXPECT_EQ(metadata.total_observations, 11);

  // Verify min: -200
  std::vector<double> qs = {0.0};
  redis::TDigestQuantitleResult result;
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], -200.0, 1.0);

  // Verify max: 100 (quantile 1.0)
  qs = {1.0};
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], 100.0, 1.0);
}

TEST_F(RedisTDigestTest, MergeIntoExistingDestWithOverride) {
  // Test: When dest exists with OVERRIDE flag, overwrite dest data
  std::string ts = std::to_string(util::GetTimeStampMS());
  std::string src1 = "tdigest_merge_override_src1_" + ts;
  std::string src2 = "tdigest_merge_override_src2_" + ts;
  std::string dest = "tdigest_merge_override_dest_" + ts;

  bool exists = false;
  // Create source1 with values: 1, 2, 3
  ASSERT_TRUE(tdigest_->Create(*ctx_, src1, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src1, {1.0, 2.0, 3.0}).ok());

  // Create source2 with values: 4, 5, 6, 100, -200
  ASSERT_TRUE(tdigest_->Create(*ctx_, src2, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src2, {4.0, 5.0, 6.0, 100.0, -200.0}).ok());

  // Create dest with values: 7, 8, 9 (will be overwritten)
  ASSERT_TRUE(tdigest_->Create(*ctx_, dest, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, dest, {7.0, 8.0, 9.0}).ok());

  // Merge sources into existing dest WITH OVERRIDE
  // Should only have src1(3) + src2(5) = 8 observations (dest data overwritten)
  redis::TDigestMergeOptions options;
  options.override_flag = true;
  auto status = tdigest_->Merge(*ctx_, dest, {src1, src2}, options);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Verify total observations: 3 + 5 = 8 (dest data was overwritten)
  TDigestMetadata metadata;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata).ok());
  EXPECT_EQ(metadata.total_observations, 8);

  // Verify min: -200
  std::vector<double> qs = {0.0};
  redis::TDigestQuantitleResult result;
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], -200.0, 1.0);

  // Verify max: 100
  qs = {1.0};
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], 100.0, 1.0);
}

TEST_F(RedisTDigestTest, MergeDestInSourceListWithoutOverride) {
  // Test: dest in source list without OVERRIDE - dest data is merged twice (Redis behavior)
  std::string ts = std::to_string(util::GetTimeStampMS());
  std::string dest = "tdigest_dest_in_src_" + ts;
  std::string src = "tdigest_src_for_dest_" + ts;

  bool exists = false;
  // Create dest with values: 1, 2, 3
  ASSERT_TRUE(tdigest_->Create(*ctx_, dest, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, dest, {1.0, 2.0, 3.0}).ok());

  // Create src with values: 10, 20
  ASSERT_TRUE(tdigest_->Create(*ctx_, src, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src, {10.0, 20.0}).ok());

  // Verify dest has 3 observations before merge
  TDigestMetadata metadata_before;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata_before).ok());
  EXPECT_EQ(metadata_before.total_observations, 3);

  // Merge: TDIGEST.MERGE dest 2 dest src
  // dest is both the destination AND in the source list
  // Redis behavior: dest's existing data + dest(in source) + src = 3+3+2 = 8
  redis::TDigestMergeOptions options;
  options.override_flag = false;
  auto status = tdigest_->Merge(*ctx_, dest, {dest, src}, options);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Verify: should have dest(3) + dest(3) + src(2) = 8 observations
  // (dest is double-counted when in source list without OVERRIDE)
  TDigestMetadata metadata_after;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata_after).ok());
  EXPECT_EQ(metadata_after.total_observations, 8);

  // Verify min: 1 (from dest)
  std::vector<double> qs = {0.0};
  redis::TDigestQuantitleResult result;
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], 1.0, 0.1);

  // Verify max: 20 (from src)
  qs = {1.0};
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], 20.0, 0.1);
}

TEST_F(RedisTDigestTest, MergeDestInSourceListWithOverride) {
  // Test: dest in source list WITH OVERRIDE - dest in source counted once
  std::string ts = std::to_string(util::GetTimeStampMS());
  std::string dest = "tdigest_dest_in_src_override_" + ts;
  std::string src = "tdigest_src_for_override_" + ts;

  bool exists = false;
  // Create dest with values: 1, 2, 3
  ASSERT_TRUE(tdigest_->Create(*ctx_, dest, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, dest, {1.0, 2.0, 3.0}).ok());

  // Create src with values: 10, 20
  ASSERT_TRUE(tdigest_->Create(*ctx_, src, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src, {10.0, 20.0}).ok());

  // Merge with OVERRIDE: dest in source list should be counted once
  // Result: dest(3) + src(2) = 5 observations
  redis::TDigestMergeOptions options;
  options.override_flag = true;
  auto status = tdigest_->Merge(*ctx_, dest, {dest, src}, options);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Verify: should have dest(3) + src(2) = 5 observations
  TDigestMetadata metadata;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata).ok());
  EXPECT_EQ(metadata.total_observations, 5);

  // Verify min: 1 (from dest in source list)
  std::vector<double> qs = {0.0};
  redis::TDigestQuantitleResult result;
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], 1.0, 0.1);

  // Verify max: 20 (from src)
  qs = {1.0};
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], 20.0, 0.1);
}

TEST_F(RedisTDigestTest, MergeIntoNewDest) {
  // Test: Merge into a new (non-existing) destination
  std::string ts = std::to_string(util::GetTimeStampMS());
  std::string src1 = "tdigest_new_dest_src1_" + ts;
  std::string src2 = "tdigest_new_dest_src2_" + ts;
  std::string dest = "tdigest_new_dest_" + ts;

  bool exists = false;
  // Create source1 with values: 1, 2, 3
  ASSERT_TRUE(tdigest_->Create(*ctx_, src1, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src1, {1.0, 2.0, 3.0}).ok());

  // Create source2 with values: 4, 5, 6
  ASSERT_TRUE(tdigest_->Create(*ctx_, src2, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src2, {4.0, 5.0, 6.0}).ok());

  // Merge into new dest (dest does not exist)
  // Result: src1(3) + src2(3) = 6 observations
  redis::TDigestMergeOptions options;
  options.override_flag = false;
  auto status = tdigest_->Merge(*ctx_, dest, {src1, src2}, options);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Verify total observations: 3 + 3 = 6
  TDigestMetadata metadata;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata).ok());
  EXPECT_EQ(metadata.total_observations, 6);

  // Verify min: 1
  std::vector<double> qs = {0.0};
  redis::TDigestQuantitleResult result;
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], 1.0, 0.1);

  // Verify max: 6
  qs = {1.0};
  ASSERT_TRUE(tdigest_->Quantile(*ctx_, dest, qs, &result).ok());
  ASSERT_TRUE(result.quantiles.has_value());
  EXPECT_NEAR((*result.quantiles)[0], 6.0, 0.1);
}

TEST_F(RedisTDigestTest, MergeIntoExistingDestKeepsCompression) {
  // Test: When merging into existing dest without OVERRIDE, dest's compression should be preserved
  // (Redis behavior: compression is not overwritten by source's compression)
  std::string ts = std::to_string(util::GetTimeStampMS());
  std::string src = "tdigest_compression_src_" + ts;
  std::string dest = "tdigest_compression_dest_" + ts;

  bool exists = false;
  // Create source with COMPRESSION 200
  ASSERT_TRUE(tdigest_->Create(*ctx_, src, {200}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src, {1.0}).ok());

  // Create dest with COMPRESSION 100
  ASSERT_TRUE(tdigest_->Create(*ctx_, dest, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, dest, {2.0}).ok());

  // Verify dest compression before merge
  TDigestMetadata metadata_before;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata_before).ok());
  EXPECT_EQ(metadata_before.compression, 100);

  // Merge source into dest without OVERRIDE
  // dest's compression (100) should be preserved, not overwritten by source's compression (200)
  redis::TDigestMergeOptions options;
  options.override_flag = false;
  auto status = tdigest_->Merge(*ctx_, dest, {src}, options);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Verify dest compression is still 100 (not 200)
  TDigestMetadata metadata_after;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata_after).ok());
  EXPECT_EQ(metadata_after.compression, 100) << "dest compression should be preserved when merging without OVERRIDE";

  // Verify total observations: dest(1) + src(1) = 2
  EXPECT_EQ(metadata_after.total_observations, 2);
}

TEST_F(RedisTDigestTest, MergeWithOverrideTakesMaxCompression) {
  // Test: When merging with OVERRIDE, compression should be max of all sources
  std::string ts = std::to_string(util::GetTimeStampMS());
  std::string src1 = "tdigest_override_compression_src1_" + ts;
  std::string src2 = "tdigest_override_compression_src2_" + ts;
  std::string dest = "tdigest_override_compression_dest_" + ts;

  bool exists = false;
  // Create source1 with COMPRESSION 200
  ASSERT_TRUE(tdigest_->Create(*ctx_, src1, {200}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src1, {1.0}).ok());

  // Create source2 with COMPRESSION 300
  ASSERT_TRUE(tdigest_->Create(*ctx_, src2, {300}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src2, {2.0}).ok());

  // Create dest with COMPRESSION 100
  ASSERT_TRUE(tdigest_->Create(*ctx_, dest, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, dest, {3.0}).ok());

  // Merge with OVERRIDE - compression should be max(src1, src2) = 300
  redis::TDigestMergeOptions options;
  options.override_flag = true;
  auto status = tdigest_->Merge(*ctx_, dest, {src1, src2}, options);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Verify dest compression is 300 (max of sources)
  TDigestMetadata metadata;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata).ok());
  EXPECT_EQ(metadata.compression, 300) << "with OVERRIDE, compression should be max of sources";

  // Verify total observations: src1(1) + src2(1) = 2 (dest data was overwritten)
  EXPECT_EQ(metadata.total_observations, 2);
}

TEST_F(RedisTDigestTest, MergeWithUserSpecifiedCompression) {
  // Test: User-specified compression overrides all other compression values
  std::string ts = std::to_string(util::GetTimeStampMS());
  std::string src = "tdigest_user_compression_src_" + ts;
  std::string dest = "tdigest_user_compression_dest_" + ts;

  bool exists = false;
  // Create source with COMPRESSION 200
  ASSERT_TRUE(tdigest_->Create(*ctx_, src, {200}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, src, {1.0}).ok());

  // Create dest with COMPRESSION 100
  ASSERT_TRUE(tdigest_->Create(*ctx_, dest, {100}, &exists).ok());
  ASSERT_FALSE(exists);
  ASSERT_TRUE(tdigest_->Add(*ctx_, dest, {2.0}).ok());

  // Merge with user-specified COMPRESSION 50
  // User-specified compression should override both dest and source compression
  redis::TDigestMergeOptions options;
  options.override_flag = false;
  options.compression = 50;
  auto status = tdigest_->Merge(*ctx_, dest, {src}, options);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Verify dest compression is 50 (user-specified)
  TDigestMetadata metadata;
  ASSERT_TRUE(tdigest_->GetMetaData(*ctx_, dest, &metadata).ok());
  EXPECT_EQ(metadata.compression, 50) << "user-specified compression should override all";

  // Verify total observations: dest(1) + src(1) = 2
  EXPECT_EQ(metadata.total_observations, 2);
}
