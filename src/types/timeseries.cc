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

TSChunkPtr CreateTSChunkFromData(nonstd::span<char> data) {
  auto chunk_meta = TSChunk::MetaData();
  Slice input(data.data(), TSChunk::MetaData::kEncodedSize);
  chunk_meta.Decode(&input);
  if (!chunk_meta.is_compressed) {
    return std::make_unique<UncompTSChunk>(std::move(data));
  } else {
    // TODO: compressed chunk
    unreachable();
  }
}

OwnedTSChunk CreateEmptyOwnedTSChunk(bool is_compressed) {
  auto metadata = TSChunk::MetaData(is_compressed, 0);
  std::string data = metadata.Encode();
  return {CreateTSChunkFromData(data), std::move(data)};
}

TSChunk::SampleBatch::SampleBatch(std::vector<TSSample> samples, DuplicatePolicy policy)
    : samples_(std::move(samples)), policy_(policy) {
  size_t count = samples_.size();
  add_results_.resize(count, AddResult::kNone);
  indexes_.resize(count);
  for (size_t i = 0; i < count; ++i) {
    indexes_[i] = i;
  }
  sortAndOrganize();
}

void TSChunk::SampleBatch::Expire(uint64_t last_ts, uint64_t retention) {
  if (retention == 0) return;
  for (auto idx : indexes_) {
    if (samples_[idx].ts + retention < last_ts) {
      add_results_[idx] = AddResult::kOld;
    } else if (samples_[idx].ts > last_ts) {
      last_ts = samples_[idx].ts;
    }
  }
}

void TSChunk::SampleBatch::sortAndOrganize() {
  auto count = samples_.size();
  if (0 == count) return;

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
  for (size_t i = 1; i < count; ++i) {
    TSSample* cur = &samples_[i];
    auto result = MergeSamplesValue(samples_[prev_idx], *cur, policy_);
    if (result == AddResult::kNone) {
      prev_idx = i;
    }
    add_results_[i] = result;
  }
}

SampleBatchSlice TSChunk::SampleBatchSlice::SliceByCount(uint64_t first, int count, uint64_t* last_ts) {
  if (sample_span_.empty()) {
    return {};
  }

  auto start_it = std::lower_bound(sample_span_.begin(), sample_span_.end(), TSSample{first, 0.0});
  if (start_it == sample_span_.end()) {
    return {};
  }

  size_t start_idx = start_it - sample_span_.begin();

  if (count < 0) {
    if (last_ts) {
      *last_ts = sample_span_.back().ts;
    }
    return createSampleSlice(start_idx, sample_span_.size());
  }

  size_t end_idx = start_idx;
  while (end_idx < sample_span_.size() && count > 0) {
    if (add_result_span_[end_idx] == AddResult::kNone) {
      if (last_ts) {
        *last_ts = sample_span_[end_idx].ts;
      }
      count--;
    }
    end_idx++;
  }

  return createSampleSlice(start_idx, end_idx);
}

SampleBatchSlice TSChunk::SampleBatchSlice::SliceByTimestamps(uint64_t first, uint64_t last, bool contain_last) {
  if (sample_span_.empty()) {
    return {};
  }

  auto start_it = std::lower_bound(sample_span_.begin(), sample_span_.end(), TSSample{first, 0.0});
  auto end_it = contain_last ? std::upper_bound(sample_span_.begin(), sample_span_.end(), TSSample{last, 0.0})
                             : std::lower_bound(sample_span_.begin(), sample_span_.end(), TSSample{last, 0.0});

  size_t start_idx = start_it - sample_span_.begin();
  size_t end_idx = end_it - sample_span_.begin();

  if (start_idx < end_idx) {
    return createSampleSlice(start_idx, end_idx);
  }
  return {};
}

SampleBatchSlice TSChunk::SampleBatchSlice::createSampleSlice(size_t start_idx, size_t end_idx) {
  if (end_idx > sample_span_.size()) {
    end_idx = sample_span_.size();
  }
  if (end_idx - start_idx == 0) {
    return {};
  }
  return {nonstd::span<const TSSample>(&sample_span_[start_idx], end_idx - start_idx),
          nonstd::span<AddResult>(&add_result_span_[start_idx], end_idx - start_idx), policy_};
}

SampleBatchSlice TSChunk::SampleBatch::AsSlice() { return {samples_, add_results_, policy_}; }

std::vector<AddResult> TSChunk::SampleBatch::GetFinalResults() const {
  std::vector<AddResult> res;
  res.resize(add_results_.size());
  for (size_t idx = 0; idx < add_results_.size(); idx++) {
    res[indexes_[idx]] = add_results_[idx];
  }
  return res;
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

uint32_t TSChunk::GetCount() const { return metadata_.count; }

uint64_t TSChunk::SampleBatchSlice::GetFirstTimestamp() {
  if (sample_span_.size() == 0) return 0;
  for (size_t i = 0; i < sample_span_.size(); i++) {
    if (add_result_span_[i] == AddResult::kNone) {
      return sample_span_[i].ts;
    }
  }
  return 0;
}

uint64_t TSChunk::SampleBatchSlice::GetLastTimestamp() {
  if (sample_span_.size() == 0) return 0;
  for (size_t i = sample_span_.size() - 1; i >= 0; i--) {
    if (add_result_span_[i] == AddResult::kNone) {
      return sample_span_[i].ts;
    }
  }
  return 0;
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

std::string TSChunk::MetaData::Encode() const {
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
  is_compressed = flag & uint32_t(1);
  GetFixed32(input, &count);
}

TSChunk::TSChunk(nonstd::span<char> data) : data_(data) {
  Slice input(data_.data(), data_.size());
  metadata_.Decode(&input);
}

class UncompTSChunkIterator : public TSChunkIterator {
 public:
  explicit UncompTSChunkIterator(nonstd::span<TSSample> data, uint64_t count) : TSChunkIterator(count), data_(data) {}
  std::optional<TSSample*> Next() override {
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

std::unique_ptr<TSChunkIterator> UncompTSChunk::CreateIterator() const {
  return std::make_unique<UncompTSChunkIterator>(samples_, metadata_.count);
}

uint64_t UncompTSChunk::GetFirstTimestamp() const {
  if (metadata_.count == 0) {
    return 0;
  }
  return samples_[0].ts;
}

uint64_t UncompTSChunk::GetLastTimestamp() const {
  if (metadata_.count == 0) {
    return 0;
  }
  return samples_[metadata_.count - 1].ts;
}

std::string UncompTSChunk::UpsertSamples(SampleBatchSlice batch) const {
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
  auto* merged_data = reinterpret_cast<TSSample*>(new_buffer.data() + header_size);

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
  while (new_sample_idx != new_samples.size() && existing_sample_iter != samples_.end()) {
    const TSSample* candidate = nullptr;
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
    if (current_index == static_cast<size_t>(-1)) {
      current_index = 0;
      merged_data[current_index] = new_samples[new_sample_idx];
    } else if (new_samples[new_sample_idx].ts > merged_data[current_index].ts) {
      merged_data[++current_index] = new_samples[new_sample_idx];
    } else {
      auto add_res = MergeSamplesValue(merged_data[current_index], new_samples[new_sample_idx], policy);
      add_results[new_sample_idx] = add_res;
    }
    ++new_sample_idx;
  }

  // Update metadata in buffer header
  const size_t final_count = current_index + 1;
  auto metadata = TSChunk::MetaData(false, 0);
  metadata.count = final_count;
  auto str = metadata.Encode();
  EncodeBuffer(new_buffer.data(), str);

  return new_buffer;
}

std::string UncompTSChunk::RemoveSamplesBetween(uint64_t from, uint64_t to) const {
  if (from > to) {
    return "";
  }

  // Find the range of samples to delete using binary search
  auto start_it = std::lower_bound(samples_.begin(), samples_.end(), TSSample{from, 0.0});
  if (start_it == samples_.end()) {
    return "";
  }
  auto end_it = std::upper_bound(samples_.begin(), samples_.end(), TSSample{to, 0.0});

  size_t start_idx = std::distance(samples_.begin(), start_it);
  size_t end_idx = std::distance(samples_.begin(), end_it);

  // Calculate buffer size: header + remaining samples
  const size_t header_size = TSChunk::MetaData::kEncodedSize;
  const size_t remaining_count = metadata_.count - (end_idx - start_idx);
  const size_t required_size = header_size + remaining_count * sizeof(TSSample);

  // Prepare new buffer
  std::string new_buffer;
  new_buffer.resize(required_size);

  // Copy header + samples before deletion range
  size_t part_size = header_size + start_idx * sizeof(TSSample);
  std::memcpy(new_buffer.data(), data_.data(), part_size);

  // Copy samples after deletion range
  if (end_idx < metadata_.count) {
    std::memcpy(new_buffer.data() + part_size,
                reinterpret_cast<const char*>(samples_.data()) + end_idx * sizeof(TSSample),
                (metadata_.count - end_idx) * sizeof(TSSample));
  }

  // Update metadata in buffer header
  auto metadata = TSChunk::MetaData(false, remaining_count);
  auto str = metadata.Encode();
  EncodeBuffer(new_buffer.data(), str);

  return new_buffer;
}

std::string UncompTSChunk::UpdateSampleValue(uint64_t ts, double value, bool is_add_on) const {
  if (ts < GetFirstTimestamp() || ts > GetLastTimestamp()) {
    return "";
  }

  // Find the position of the sample to update
  auto it = std::lower_bound(samples_.begin(), samples_.end(), TSSample{ts, 0.0});
  if (it == samples_.end() || it->ts != ts) {
    return "";  // Sample not found
  }
  auto cur_value = it->v;

  std::string new_buffer = std::string(data_.data(), data_.size());
  size_t header_size = TSChunk::MetaData::kEncodedSize;
  auto* new_samples = reinterpret_cast<TSSample*>(new_buffer.data() + header_size);
  auto idx = std::distance(samples_.begin(), it);
  double new_value = is_add_on ? cur_value + value : value;
  new_samples[idx] = TSSample{ts, new_value};

  return new_buffer;
}
