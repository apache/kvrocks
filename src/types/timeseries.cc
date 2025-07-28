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

#include "timeseries.h"

#include <algorithm>

#include "encoding.h"

using AddResult = TSChunk::AddResult;
using SampleBatch = TSChunk::SampleBatch;
using SampleBatchSlice = TSChunk::SampleBatchSlice;

TSChunk::SampleBatch::SampleBatch(size_t size, DuplicatePolicy policy)
    : policy_(policy), unique_count_(0), is_sorted_(false) {
  samples_.reserve(size);
}

void TSChunk::SampleBatch::Push(const TSSample& sample) {
  is_sorted_ = false;
  samples_.push_back(sample);
}

void TSChunk::SampleBatch::SortAndOrganize() {
  if (is_sorted_) return;
  auto count = samples_.size();
  if (0 == count) return;

  add_results_.resize(count, AddResult::kNone);
  indexes_.resize(count);
  for (size_t i = 0; i < count; ++i) {
    indexes_[i] = i;
  }

  // should be stable sort
  std::stable_sort(indexes_.begin(), indexes_.end(), [this](size_t a, size_t b) { return samples_[a] < samples_[b]; });
  std::vector<TSSample> samples_sorted;
  samples_sorted.reserve(indexes_.size());
  for (size_t i = 0; i < count; ++i) {
    samples_sorted.push_back(samples_[indexes_[i]]);
  }
  samples_ = std::move(samples_sorted);

  size_t prev_idx = 0;
  add_results_[0] = AddResult::kNone;
  unique_count_ = 1;
  for (size_t i = 1; i < count; ++i) {
    TSSample* cur = &samples_[i];
    auto result = MergeSamplesValue(samples_[prev_idx], *cur, policy_);
    if (result == AddResult::kNone) {
      unique_count_++;
      prev_idx = i;
    }
    add_results_[i] = result;
  }
  is_sorted_ = true;
}

std::vector<SampleBatchSlice> TSChunk::SampleBatch::SliceByTimestamps(const std::vector<uint64_t>& timestamps) {
  EnsureSorted();
  std::vector<SampleBatchSlice> slices(timestamps.size());
  if (samples_.empty()) return slices;

  // Precompute timestamps for binary search
  std::vector<uint64_t> s_ts;
  s_ts.reserve(samples_.size());
  for (const auto& sample : samples_) {
    s_ts.push_back(sample.ts);
  }

  // Calculate insertion points for each timestamp
  std::vector<size_t> pos;
  pos.reserve(timestamps.size());
  for (auto t : timestamps) {
    auto it = std::lower_bound(s_ts.begin(), s_ts.end(), t);
    pos.push_back(std::distance(s_ts.begin(), it));
  }

  // Generate slices based on calculated positions
  for (size_t i = 0; i < timestamps.size(); ++i) {
    size_t start_idx = pos[i];
    size_t end_idx = (i == timestamps.size() - 1) ? samples_.size() : pos[i + 1];

    if (start_idx < end_idx) {
      size_t count = end_idx - start_idx;
      slices[i] = SampleBatchSlice(nonstd::span<TSSample>(&samples_[start_idx], count),
                                   nonstd::span<AddResult>(&add_results_[start_idx], count), policy_);
    }
  }
  return slices;
}

SampleBatchSlice TSChunk::SampleBatch::AsSlice() {
  EnsureSorted();
  return {samples_, add_results_, policy_};
}

void TSChunk::SampleBatch::EnsureSorted() {
  if (!is_sorted_) {
    SortAndOrganize();
  }
}

AddResult TSChunk::MergeSamplesValue(TSSample& to, const TSSample& from, DuplicatePolicy policy) {
  if (to.ts != from.ts) {
    return AddResult::kNone;
  }

  switch (policy) {
    case DuplicatePolicy::BLOCK:
      return AddResult::kBlock;
    case DuplicatePolicy::FIRST:
      return AddResult::kOk;
    case DuplicatePolicy::LAST:
      to.v = from.v;
      return AddResult::kOk;
    case DuplicatePolicy::MAX:
      to.v = std::max(to.v, from.v);
      return AddResult::kOk;
    case DuplicatePolicy::MIN:
      to.v = std::min(to.v, from.v);
      return AddResult::kOk;
    case DuplicatePolicy::SUM:
      to.v += from.v;
      return AddResult::kOk;
  }

  return AddResult::kNone;
}

uint64_t TSChunk::SampleBatchSlice::GetFirstTimestamp() {
  if (sample_span_.size() == 0) return 0;
  return sample_span_[0].ts;
}

uint64_t TSChunk::SampleBatchSlice::GetLastTimestamp() {
  if (sample_span_.size() == 0) return 0;
  return sample_span_[sample_span_.size() - 1].ts;
}

size_t TSChunk::SampleBatchSlice::GetUniqueCount() const {
  size_t count = 0;
  for (auto res : add_result_span_) {
    if (res == AddResult::kNone) {
      count++;
    }
  }
  return count;
}
