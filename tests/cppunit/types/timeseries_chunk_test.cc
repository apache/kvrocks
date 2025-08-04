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

#include <random>
#include <vector>

#include "test_base.h"
#include "types/timeseries.h"

namespace test {

using SampleBatch = TSChunk::SampleBatch;
using SampleBatchSlice = TSChunk::SampleBatchSlice;
using DuplicatePolicy = TSChunk::DuplicatePolicy;
using AddResult = TSChunk::AddResult;

// Helper function to generate TSSample with specific timestamp and value
TSSample MakeSample(uint64_t timestamp, double value) {
  TSSample sample;
  sample.ts = timestamp;
  sample.v = value;
  return sample;
}

// Test different duplicate policies
TEST(RedisTimeSeriesChunkTest, PolicyBehaviors) {
  TSSample original = MakeSample(100, 1.0);
  TSSample duplicate = MakeSample(100, 2.0);

  // Test BLOCK policy
  EXPECT_EQ(TSChunk::MergeSamplesValue(original, duplicate, DuplicatePolicy::BLOCK), AddResult::kBlock);
  EXPECT_EQ(original.v, 1.0);

  // Test LAST policy
  EXPECT_EQ(TSChunk::MergeSamplesValue(original, duplicate, DuplicatePolicy::LAST), AddResult::kOk);
  EXPECT_EQ(original.v, 2.0);

  // Reset and test MAX policy
  original.v = 1.0;
  EXPECT_EQ(TSChunk::MergeSamplesValue(original, duplicate, DuplicatePolicy::MAX), AddResult::kOk);
  EXPECT_EQ(original.v, 2.0);

  // Reset and test MIN policy
  original.v = 3.0;
  EXPECT_EQ(TSChunk::MergeSamplesValue(original, duplicate, DuplicatePolicy::MIN), AddResult::kOk);
  EXPECT_EQ(original.v, 2.0);

  // Reset and test SUM policy
  original.v = 1.0;
  EXPECT_EQ(TSChunk::MergeSamplesValue(original, duplicate, DuplicatePolicy::SUM), AddResult::kOk);
  EXPECT_EQ(original.v, 3.0);
}

// Test timestamp-based slicing operations
TEST(RedisTimeSeriesChunkTest, TimestampSlicing) {
  std::vector<TSSample> samples = {MakeSample(100, 1.0), MakeSample(200, 2.0), MakeSample(300, 3.0),
                                   MakeSample(400, 4.0)};
  SampleBatch batch(std::move(samples), DuplicatePolicy::LAST);
  SampleBatchSlice slice = batch.AsSlice();

  // Test SliceByTimestamps with contain_last=true
  auto result_slice = slice.SliceByTimestamps(150, 300, true);
  EXPECT_EQ(result_slice.GetValidCount(), 2);
  EXPECT_EQ(result_slice.GetFirstTimestamp(), 200);
  EXPECT_EQ(result_slice.GetLastTimestamp(), 300);

  // Test SliceByTimestamps with contain_last=false
  result_slice = slice.SliceByTimestamps(150, 300, false);
  EXPECT_EQ(result_slice.GetValidCount(), 1);
  EXPECT_EQ(result_slice.GetFirstTimestamp(), 200);
  EXPECT_EQ(result_slice.GetLastTimestamp(), 200);

  // Test SliceByCount
  uint64_t last_ts = 0;
  result_slice = slice.SliceByCount(200, 2, &last_ts);
  EXPECT_EQ(result_slice.GetValidCount(), 2);
  EXPECT_EQ(last_ts, 300);
  EXPECT_EQ(result_slice.GetFirstTimestamp(), 200);
  EXPECT_EQ(result_slice.GetLastTimestamp(), 300);
}

// Test expiration logic
TEST(RedisTimeSeriesChunkTest, ExpirationLogic) {
  std::vector<TSSample> samples = {MakeSample(200, 1.0), MakeSample(400, 2.0), MakeSample(100, 3.0),
                                   MakeSample(150, 4.0)};
  SampleBatch batch(samples, DuplicatePolicy::LAST);

  // Set retention to 150, last_ts = 300
  batch.Expire(300, 150);
  auto results = batch.GetFinalResults();

  // Only samples with ts >= 150 should be kept
  EXPECT_EQ(results[0], AddResult::kNone);
  EXPECT_EQ(results[1], AddResult::kNone);
  EXPECT_EQ(results[2], AddResult::kOld);
  EXPECT_EQ(results[3], AddResult::kOld);
}

// Test SampleBatch construction and sorting
TEST(RedisTimeSeriesChunkTest, BatchSortingAndDeduplication) {
  std::vector<TSSample> samples = {
      MakeSample(300, 3.0), MakeSample(100, 1.0), MakeSample(200, 2.0), MakeSample(100, 4.0),  // Duplicate timestamp
      MakeSample(200, 5.0)                                                                     // Duplicate timestamp
  };

  // Test with BLOCK policy
  SampleBatch batch(samples, DuplicatePolicy::BLOCK);
  SampleBatchSlice slice = batch.AsSlice();

  // Verify sorting
  EXPECT_EQ(slice.GetFirstTimestamp(), 100);
  EXPECT_EQ(slice.GetLastTimestamp(), 300);

  // Verify deduplication
  EXPECT_EQ(slice.GetValidCount(), 3);
  auto results = batch.GetFinalResults();
  EXPECT_EQ(results[0], AddResult::kNone);
  EXPECT_EQ(results[1], AddResult::kNone);
  EXPECT_EQ(results[2], AddResult::kNone);
  EXPECT_EQ(results[3], AddResult::kBlock);
  EXPECT_EQ(results[4], AddResult::kBlock);
}

// Test MAddSample merging logic with additional samples and content validation
TEST(RedisTimeSeriesChunkTest, UcompChunkMAddSampleLogic) {
  // Create base chunk
  auto [chunk, data] = CreateEmptyOwnedTSChunk(false);

  // Create test samples with multiple duplicates and new timestamps
  std::vector<TSSample> new_samples = {
      MakeSample(300, 3.0), MakeSample(100, 1.0), MakeSample(200, 2.0), MakeSample(100, 4.0),
      MakeSample(200, 5.0), MakeSample(400, 4.0), MakeSample(100, 6.0)  // Additional duplicates and new timestamp
  };
  SampleBatch batch(new_samples, DuplicatePolicy::LAST);
  SampleBatchSlice slice = batch.AsSlice();

  // Merge samples into chunk
  std::string result = chunk->UpsertSamples(slice);
  EXPECT_FALSE(result.empty());

  // Verify merged chunk metadata
  auto new_chunk = CreateTSChunkFromData(result);
  EXPECT_EQ(new_chunk->GetCount(), 4);  // 100, 200, 300, 400 (with duplicates removed)
  EXPECT_EQ(new_chunk->GetFirstTimestamp(), 100);
  EXPECT_EQ(new_chunk->GetLastTimestamp(), 400);

  // Validate content of merged chunk
  auto iter = new_chunk->CreateIterator();
  auto* sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 100);
  EXPECT_EQ(sample->v, 6.0);  // Latest value for 100
  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 200);
  EXPECT_EQ(sample->v, 5.0);  // Latest value for 200
  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 300);
  EXPECT_EQ(sample->v, 3.0);
  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 400);
  EXPECT_EQ(sample->v, 4.0);
  EXPECT_EQ(iter->HasNext(), false);
  EXPECT_EQ(iter->Next(), std::nullopt);
}

// Test UpsertSamples with a chunk that has existing samples
TEST(RedisTimeSeriesChunkTest, UcompChunkMAddSampleWithExistingSamples) {
  // Create empty chunk
  auto [chunk, data] = CreateEmptyOwnedTSChunk(false);

  // Initialize chunk with samples: 100, 200, 300
  std::vector<TSSample> initial_samples = {MakeSample(100, 1.0), MakeSample(200, 2.0), MakeSample(300, 3.0)};
  SampleBatch initial_batch(initial_samples, DuplicatePolicy::LAST);
  SampleBatchSlice initial_slice = initial_batch.AsSlice();
  std::string merged_data = chunk->UpsertSamples(initial_slice);
  ASSERT_FALSE(merged_data.empty());

  // New samples to add: 150, 200(update), 400
  std::vector<TSSample> new_samples = {MakeSample(50, 0.5), MakeSample(150, 1.5), MakeSample(200, 2.5),
                                       MakeSample(300, 3.5), MakeSample(400, 4.0)};
  SampleBatch new_batch(new_samples, DuplicatePolicy::LAST);
  SampleBatchSlice new_slice = new_batch.AsSlice();

  // Perform merge
  merged_data = CreateTSChunkFromData(merged_data)->UpsertSamples(new_slice);
  ASSERT_FALSE(merged_data.empty());

  // Validate final state
  TSChunkPtr final_chunk = CreateTSChunkFromData(merged_data);
  EXPECT_EQ(final_chunk->GetCount(), 6);
  EXPECT_EQ(final_chunk->GetFirstTimestamp(), 50);
  EXPECT_EQ(final_chunk->GetLastTimestamp(), 400);

  // Verify content through iterator
  auto iter = final_chunk->CreateIterator();
  auto* sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 50);
  EXPECT_EQ(sample->v, 0.5);

  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 100);
  EXPECT_EQ(sample->v, 1.0);

  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 150);
  EXPECT_EQ(sample->v, 1.5);

  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 200);
  EXPECT_EQ(sample->v, 2.5);

  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 300);
  EXPECT_EQ(sample->v, 3.5);

  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 400);
  EXPECT_EQ(sample->v, 4.0);

  EXPECT_FALSE(iter->HasNext());
}

// Test RemoveSamplesBetween with different deletion ranges and content validation
TEST(RedisTimeSeriesChunkTest, UcompChunkDeletionRange) {
  // Create base chunk with samples: 100, 200, 300, 400, 500
  auto [chunk, data] = CreateEmptyOwnedTSChunk(false);
  std::vector<TSSample> initial_samples = {MakeSample(100, 1.0), MakeSample(200, 2.0), MakeSample(300, 3.0),
                                           MakeSample(400, 4.0), MakeSample(500, 5.0)};
  SampleBatch initial_batch(initial_samples, DuplicatePolicy::LAST);
  SampleBatchSlice initial_slice = initial_batch.AsSlice();
  std::string merged_data = chunk->UpsertSamples(initial_slice);
  ASSERT_FALSE(merged_data.empty());
  TSChunkPtr test_chunk = CreateTSChunkFromData(merged_data);

  // Test 1: Delete middle range (200-400 inclusive)
  std::string deleted_data = test_chunk->RemoveSamplesBetween(200, 400);
  ASSERT_FALSE(deleted_data.empty());
  TSChunkPtr result_chunk = CreateTSChunkFromData(deleted_data);
  EXPECT_EQ(result_chunk->GetCount(), 2);

  auto iter = result_chunk->CreateIterator();
  auto* sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 100);
  EXPECT_EQ(sample->v, 1.0);
  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 500);
  EXPECT_EQ(sample->v, 5.0);
  EXPECT_FALSE(iter->HasNext());

  // Test 2: Delete range (300,1000)
  deleted_data = test_chunk->RemoveSamplesBetween(300, 1000);
  ASSERT_FALSE(deleted_data.empty());
  result_chunk = CreateTSChunkFromData(deleted_data);
  EXPECT_EQ(result_chunk->GetCount(), 2);
  EXPECT_EQ(result_chunk->GetFirstTimestamp(), 100);
  EXPECT_EQ(result_chunk->GetLastTimestamp(), 200);

  // Test 3: Delete range (0, 300)
  deleted_data = test_chunk->RemoveSamplesBetween(0, 300);
  ASSERT_FALSE(deleted_data.empty());
  result_chunk = CreateTSChunkFromData(deleted_data);
  EXPECT_EQ(result_chunk->GetCount(), 2);
  EXPECT_EQ(result_chunk->GetFirstTimestamp(), 400);
  EXPECT_EQ(result_chunk->GetLastTimestamp(), 500);

  // Test 4: Delete entire range (100-500)
  deleted_data = test_chunk->RemoveSamplesBetween(0, 1000);
  ASSERT_FALSE(deleted_data.empty());
  result_chunk = CreateTSChunkFromData(deleted_data);
  EXPECT_EQ(result_chunk->GetCount(), 0);
  EXPECT_EQ(result_chunk->GetFirstTimestamp(), 0);
  EXPECT_EQ(result_chunk->GetLastTimestamp(), 0);

  // Test 5: Delete from > to (should return original data)
  deleted_data = test_chunk->RemoveSamplesBetween(500, 100);
  EXPECT_TRUE(deleted_data.empty());

  // Test 6: Delete single timestamp (300)
  deleted_data = test_chunk->RemoveSamplesBetween(300, 300);
  ASSERT_FALSE(deleted_data.empty());
  result_chunk = CreateTSChunkFromData(deleted_data);
  EXPECT_EQ(result_chunk->GetCount(), 4);

  iter = result_chunk->CreateIterator();
  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 100);
  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 200);
  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 400);
  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 500);
  EXPECT_FALSE(iter->HasNext());
}

// Test UpdateSampleValue with different update scenarios and validation
TEST(RedisTimeSeriesChunkTest, UpdateSampleBehavior) {
  // Initialize chunk with samples: 100, 200, 300
  auto [chunk, data] = CreateEmptyOwnedTSChunk(false);
  std::vector<TSSample> initial_samples = {MakeSample(100, 1.0), MakeSample(200, 2.0), MakeSample(300, 3.0)};
  SampleBatch initial_batch(initial_samples, DuplicatePolicy::LAST);
  SampleBatchSlice initial_slice = initial_batch.AsSlice();
  std::string merged_data = chunk->UpsertSamples(initial_slice);
  ASSERT_FALSE(merged_data.empty());
  TSChunkPtr test_chunk = CreateTSChunkFromData(merged_data);

  // Test 1: Update existing sample with replace (is_add_on = false)
  std::string updated_data = test_chunk->UpdateSampleValue(200, 5.0, false);
  ASSERT_FALSE(updated_data.empty());
  TSChunkPtr result_chunk = CreateTSChunkFromData(updated_data);
  EXPECT_EQ(result_chunk->GetCount(), 3);

  auto iter = result_chunk->CreateIterator();
  auto* sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 100);
  EXPECT_EQ(sample->v, 1.0);

  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 200);
  EXPECT_EQ(sample->v, 5.0);  // Value should be replaced

  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 300);
  EXPECT_EQ(sample->v, 3.0);

  // Test 2: Update existing sample with add-on (is_add_on = true)
  updated_data = test_chunk->UpdateSampleValue(200, 1.5, true);
  ASSERT_FALSE(updated_data.empty());
  result_chunk = CreateTSChunkFromData(updated_data);

  iter = result_chunk->CreateIterator();
  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 100);
  EXPECT_EQ(sample->v, 1.0);

  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 200);
  EXPECT_EQ(sample->v, 3.5);  // 2.0 + 1.5

  sample = iter->Next().value();
  EXPECT_EQ(sample->ts, 300);
  EXPECT_EQ(sample->v, 3.0);

  // Test 3: Update non-existent sample
  updated_data = test_chunk->UpdateSampleValue(400, 5.0, false);
  EXPECT_TRUE(updated_data.empty());  // Should return empty buffer

  // Test 4: Update sample out of range (before first)
  updated_data = test_chunk->UpdateSampleValue(50, 0.5, false);
  EXPECT_TRUE(updated_data.empty());

  // Test 5: Update sample out of range (after last)
  updated_data = test_chunk->UpdateSampleValue(500, 5.0, false);
  EXPECT_TRUE(updated_data.empty());
}

}  // namespace test
