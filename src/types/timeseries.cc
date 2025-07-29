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

size_t TSChunk::SampleBatchSlice::GetValidCount() const {
  size_t count = 0;
  for (auto res : add_result_span_) {
    if (res == AddResult::kNone) {
      count++;
    }
  }
  return count;
}

std::string TSChunk::MetaData::Encode() {
  std::string ret;
  // Reserved some bits for future
  uint32_t flag = 0;
  flag |= (is_compressed ? uint32_t(1) : 0);
  PutFixed32(&ret, flag);
  PutFixed32(&ret, count);
  return ret;
}

void TSChunk::MetaData::Decode(Slice* input) {
  uint32_t flag = 0;
  GetFixed32(input, &flag);
  is_compressed = flag & 1;
  GetFixed32(input, &count);
}

TSChunk::TSChunk(nonstd::span<char> data) : data_(data) {
  Slice input(data_.data(), data_.size());
  metadata_.Decode(&input);
}

class UncompTSChunkIterator : public TSChunkIterator {
 public:
  explicit UncompTSChunkIterator(nonstd::span<TSSample> data, uint64_t count) : TSChunkIterator(count), data_(data) {}
  std::optional<TSSample*> next() override {
    if (idx_ >= count_) return std::nullopt;
    return &data_[idx_++];
  }

 private:
  nonstd::span<TSSample> data_;
};

UncompTSChunk::UncompTSChunk(nonstd::span<char> data) : TSChunk(data) {
  auto data_ptr = reinterpret_cast<char*>(data.data()) + TSChunk::MetaData::kEncodedSize;
  samples_ = nonstd::span<TSSample>(reinterpret_cast<TSSample*>(data_ptr), metadata_.count);
}

std::unique_ptr<TSChunkIterator> UncompTSChunk::create_iterator() const {
  return std::make_unique<UncompTSChunkIterator>(samples_, metadata_.count);
}

std::string UncompTSChunk::MAddSample(SampleBatchSlice batch) {
  const auto new_valid_count = batch.GetValidCount();
  if (new_valid_count == 0) {
    return "";
  }

  auto new_samples = batch.GetSampleSpan();
  auto add_results = batch.GetAddResultSpan();
  DuplicatePolicy policy = batch.GetPolicy();
  const size_t existing_count = metadata_.count;

  // Calculate buffer size: header + existing samples + unique new samples
  const size_t header_size = TSChunk::MetaData::kEncodedSize;
  const size_t required_size = header_size + (existing_count + new_valid_count) * sizeof(TSSample);

  // Prepare new buffer
  std::string new_buffer;
  new_buffer.resize(required_size);
  TSSample* merged_data = reinterpret_cast<TSSample*>(new_buffer.data() + header_size);

  // Prepare iterators for merging
  size_t new_sample_idx = 0;
  auto existing_sample_iter = std::upper_bound(samples_.begin(), samples_.end(), new_samples[0]);

  // Copy existing samples that are before the first new sample
  const size_t preserved_count = std::distance(samples_.begin(), existing_sample_iter);
  size_t current_index = preserved_count;
  if (preserved_count > 0) {
    std::memcpy(merged_data, samples_.data(), preserved_count * sizeof(TSSample));
    current_index--;  // Point to last copied sample
  } else {
    current_index = -1;  // Special case: no preserved samples
  }

  // Merge samples from both sources
  while (new_valid_count != new_samples.size() && existing_sample_iter != samples_.end()) {
    const TSSample* candidate;
    bool from_new_batch = false;

    // Select next sample by earliest timestamp
    if (existing_sample_iter->ts <= new_samples[new_sample_idx].ts) {
      candidate = &(*existing_sample_iter);
    } else {
      candidate = &new_samples[new_sample_idx];
      from_new_batch = true;
    }
    if (from_new_batch && add_results[new_sample_idx] != AddResult::kNone) {
      new_sample_idx++;
      continue;
    }

    // Handle first sample case
    if (current_index == static_cast<size_t>(-1)) {
      merged_data[0] = *candidate;
      current_index = 0;
      continue;
    }

    // Append or merge based on timestamp
    if (candidate->ts > merged_data[current_index].ts) {
      merged_data[++current_index] = *candidate;
    } else {
      if (from_new_batch) {
        auto add_res = MergeSamplesValue(merged_data[current_index], *candidate, policy);
        add_results[new_sample_idx] = add_res;
      }
    }

    // Update the index
    if (from_new_batch) {
      new_sample_idx++;
    } else {
      existing_sample_iter++;
    }
  }

  // Copy remaining existing samples
  if (existing_sample_iter != samples_.end()) {
    const size_t remaining_count = std::distance(existing_sample_iter, samples_.end());
    std::memcpy(&merged_data[current_index + 1], &(*existing_sample_iter), remaining_count * sizeof(TSSample));
    current_index += remaining_count;
  }

  // Process remaining new samples
  while (new_sample_idx != new_samples.size()) {
    if (add_results[new_sample_idx] != AddResult::kNone) {
      ++new_sample_idx;
      continue;
    }
    if (new_samples[new_sample_idx].ts > merged_data[current_index].ts) {
      merged_data[++current_index] = new_samples[new_sample_idx];
    } else {
      auto add_res = MergeSamplesValue(merged_data[current_index], new_samples[new_sample_idx], policy);
      add_results[new_sample_idx] = add_res;
    }
    ++new_sample_idx;
  }

  // Update metadata in buffer header
  const size_t final_count = current_index + 1;
  metadata_.count = final_count;
  auto str = metadata_.Encode();
  EncodeBuffer(new_buffer.data(), str);

  return new_buffer;
}
