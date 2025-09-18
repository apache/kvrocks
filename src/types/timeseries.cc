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

TSChunkPtr CreateTSChunkFromData(nonstd::span<const char> data) {
  auto chunk_meta = TSChunk::MetaData();
  Slice input(data.data(), TSChunk::MetaData::kEncodedSize);
  chunk_meta.Decode(&input);
  if (!chunk_meta.is_compressed) {
    return std::make_unique<UncompTSChunk>(data);
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
  add_results_.resize(count);
  indexes_.resize(count);
  for (size_t i = 0; i < count; ++i) {
    indexes_[i] = i;
  }
  sortAndOrganize();
}

void TSChunk::SampleBatch::Expire(uint64_t last_ts, uint64_t retention) {
  if (retention == 0) return;
  std::vector<size_t> inverse(indexes_.size());
  for (size_t i = 0; i < indexes_.size(); ++i) {
    inverse[indexes_[i]] = i;
  }
  for (auto idx : inverse) {
    if (samples_[idx].ts + retention < last_ts) {
      add_results_[idx].type = AddResultType::kOld;
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
  for (size_t i = 1; i < count; ++i) {
    TSSample& cur = samples_[i];
    auto result = MergeSamplesValue(samples_[prev_idx], cur, policy_, true);
    if (result.type == AddResultType::kNone) {
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
    if (add_result_span_[end_idx].type == AddResultType::kNone) {
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

SampleBatchSlice TSChunk::SampleBatchSlice::createSampleSlice(size_t start_idx, size_t end_idx) const {
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

std::vector<TSChunk::AddResult> TSChunk::SampleBatch::GetFinalResults() const {
  std::vector<AddResult> res;
  res.resize(add_results_.size());
  for (size_t idx = 0; idx < add_results_.size(); idx++) {
    res[indexes_[idx]] = add_results_[idx];
    res[indexes_[idx]].sample.ts = samples_[idx].ts;
  }
  return res;
}

AddResult TSChunk::MergeSamplesValue(TSSample& to, const TSSample& from, DuplicatePolicy policy,
                                     bool is_batch_process) {
  AddResult res;
  if (to.ts != from.ts) {
    return res;
  }
  res.sample.ts = from.ts;
  double old_value = to.v;
  switch (policy) {
    case DuplicatePolicy::BLOCK:
      res.type = AddResultType::kBlock;
      break;
    case DuplicatePolicy::FIRST:
      res.type = AddResultType::kSkip;
      break;
    case DuplicatePolicy::LAST:
      res.type = to.v == from.v ? AddResultType::kSkip : AddResultType::kUpdate;
      to.v = from.v;
      break;
    case DuplicatePolicy::MAX:
      res.type = from.v > to.v ? AddResultType::kUpdate : AddResultType::kSkip;
      to.v = std::max(to.v, from.v);
      break;
    case DuplicatePolicy::MIN:
      res.type = from.v < to.v ? AddResultType::kUpdate : AddResultType::kSkip;
      to.v = std::min(to.v, from.v);
      break;
    case DuplicatePolicy::SUM:
      // Since 'from.v' comes directly from user input,
      // we can safely use exact comparison (== 0.0) to check for zero.
      res.type = from.v == 0.0 ? AddResultType::kSkip : AddResultType::kUpdate;
      to.v += from.v;
      break;
  }
  // For batch preprocessing, merged sample should be treated as Skip, except for BLOCK
  if (is_batch_process && res.type != AddResultType::kBlock) {
    res.type = AddResultType::kSkip;
  }
  res.sample.v = to.v - old_value;
  return res;
}

uint32_t TSChunk::GetCount() const { return metadata_.count; }

uint64_t TSChunk::SampleBatchSlice::GetFirstTimestamp() const {
  if (sample_span_.size() == 0) return 0;
  for (size_t i = 0; i < sample_span_.size(); i++) {
    if (add_result_span_[i].type == AddResultType::kNone) {
      return sample_span_[i].ts;
    }
  }
  return 0;
}

uint64_t TSChunk::SampleBatchSlice::GetLastTimestamp() const {
  if (sample_span_.size() == 0) return 0;
  for (size_t i = 0; i < sample_span_.size(); i++) {
    auto index = sample_span_.size() - i - 1;
    if (add_result_span_[index].type == AddResultType::kNone) {
      return sample_span_[index].ts;
    }
  }
  return 0;
}

size_t TSChunk::SampleBatchSlice::GetValidCount() const {
  size_t count = 0;
  for (auto res : add_result_span_) {
    if (res.type == AddResultType::kNone) {
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

TSChunk::TSChunk(nonstd::span<const char> data) : data_(data) {
  Slice input(data_.data(), data_.size());
  metadata_.Decode(&input);
}

class UncompTSChunkIterator : public TSChunkIterator {
 public:
  explicit UncompTSChunkIterator(nonstd::span<const TSSample> data, uint64_t count)
      : TSChunkIterator(count), data_(data) {}
  std::optional<const TSSample*> Next() override {
    if (idx_ >= count_) return std::nullopt;
    return &data_[idx_++];
  }

 private:
  nonstd::span<const TSSample> data_;
};

UncompTSChunk::UncompTSChunk(nonstd::span<const char> data) : TSChunk(data) {
  auto data_ptr = reinterpret_cast<const char*>(data.data()) + TSChunk::MetaData::kEncodedSize;
  samples_ = nonstd::span<const TSSample>(reinterpret_cast<const TSSample*>(data_ptr), metadata_.count);
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
      candidate = existing_sample_iter;
    } else {
      candidate = &new_samples[new_sample_idx];
      from_new_batch = true;
    }
    if (from_new_batch && add_results[new_sample_idx].type != AddResultType::kNone) {
      new_sample_idx++;
      continue;
    }

    // Handle first sample case
    if (current_index == static_cast<size_t>(-1)) {
      merged_data[0] = *candidate;
      current_index = 0;
      if (from_new_batch) {
        add_results[new_sample_idx] = AddResult::CreateInsert(*candidate);
      }
      continue;
    }

    // Append or merge based on timestamp
    bool is_append = false;
    if (candidate->ts > merged_data[current_index].ts) {
      merged_data[++current_index] = *candidate;
      is_append = true;
    }
    if (from_new_batch) {
      add_results[new_sample_idx] = is_append ? AddResult::CreateInsert(*candidate)
                                              : MergeSamplesValue(merged_data[current_index], *candidate, policy);
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
    std::memcpy(&merged_data[current_index + 1], existing_sample_iter, remaining_count * sizeof(TSSample));
    current_index += remaining_count;
  }

  // Process remaining new samples
  while (new_sample_idx != new_samples.size()) {
    if (add_results[new_sample_idx].type != AddResultType::kNone) {
      ++new_sample_idx;
      continue;
    }
    const auto& new_sample = new_samples[new_sample_idx];
    if (current_index == static_cast<size_t>(-1)) {
      current_index = 0;
      merged_data[current_index] = new_sample;
      add_results[new_sample_idx] = AddResult::CreateInsert(new_sample);
    } else if (new_sample.ts > merged_data[current_index].ts) {
      merged_data[++current_index] = new_sample;
      add_results[new_sample_idx] = AddResult::CreateInsert(new_sample);
    } else {
      auto add_res = MergeSamplesValue(merged_data[current_index], new_sample, policy);
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

std::vector<std::string> UncompTSChunk::UpsertSampleAndSplit(SampleBatchSlice batch, uint64_t preferred_chunk_size,
                                                             bool is_fix_split_mode) const {
  auto whole_chunk_data = UpsertSamples(batch);
  // Return empty if no changes
  if (whole_chunk_data.empty()) {
    return {};
  }
  auto whole_chunk = CreateTSChunkFromData(whole_chunk_data);

  // Split
  std::vector<size_t> split_size;
  auto total_count = whole_chunk->GetCount();
  if (is_fix_split_mode) {
    // Fixed split
    size_t remaining = total_count;
    while (remaining > 0) {
      auto size = std::min<size_t>(remaining, preferred_chunk_size);
      split_size.push_back(size);
      remaining -= size;
    }
  } else if (total_count > 2 * preferred_chunk_size) {
    // Equal split
    auto split_count = total_count / preferred_chunk_size;
    auto chunk_size = total_count / split_count;
    auto remainder = total_count % split_count;
    split_size.resize(split_count);
    std::fill(split_size.begin(), split_size.end(), chunk_size);
    for (uint32_t i = 0; i < remainder; ++i) {
      split_size[i] += 1;
    }
  }
  if (split_size.empty()) {
    split_size.push_back(total_count);
  }
  // Return if only one chunk
  if (split_size.size() == 1) {
    return {std::move(whole_chunk_data)};
  }

  constexpr size_t header_size = TSChunk::MetaData::kEncodedSize;
  const char* data_ptr = whole_chunk_data.data() + header_size;
  std::vector<std::string> res;
  for (auto size : split_size) {
    auto sample_bytes = size * sizeof(TSSample);
    const size_t required_size = header_size + sample_bytes;
    std::string buffer;
    buffer.resize(required_size);
    auto metadata = TSChunk::MetaData(false, size);
    auto str = metadata.Encode();
    EncodeBuffer(buffer.data(), str);
    std::memcpy(buffer.data() + header_size, data_ptr, sample_bytes);
    data_ptr += sample_bytes;
    res.push_back(std::move(buffer));
  }
  return res;
}

std::string TSChunk::RemoveSamplesBetween(uint64_t from, uint64_t to, uint64_t* deleted, bool inclusive_to) const {
  uint64_t temp = 0;
  if (deleted == nullptr) deleted = &temp;
  return doRemoveSamplesBetween(from, to, deleted, inclusive_to);
}

std::string UncompTSChunk::doRemoveSamplesBetween(uint64_t from, uint64_t to, uint64_t* deleted,
                                                  bool inclusive_to) const {
  if (from > to) {
    *deleted = 0;
    return "";
  }

  // Find the range of samples to delete using binary search
  auto start_it = std::lower_bound(samples_.begin(), samples_.end(), TSSample{from, 0.0});
  if (start_it == samples_.end()) {
    *deleted = 0;
    return "";
  }

  auto end_it = inclusive_to ? std::upper_bound(start_it, samples_.end(), TSSample{to, 0.0})
                             : std::lower_bound(start_it, samples_.end(), TSSample{to, 0.0});

  size_t start_idx = std::distance(samples_.begin(), start_it);
  size_t end_idx = std::distance(samples_.begin(), end_it);

  *deleted = end_idx - start_idx;
  if (*deleted == 0) {
    return "";
  }
  // Calculate buffer size: header + remaining samples
  const size_t header_size = TSChunk::MetaData::kEncodedSize;
  const size_t remaining_count = metadata_.count - *deleted;
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

TSSample UncompTSChunk::GetLatestSample(uint32_t idx) const {
  if (metadata_.count == 0 || idx >= metadata_.count) {
    unreachable();
  }
  return samples_[metadata_.count - 1 - idx];
}
