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

#pragma once

#include <nonstd/span.hpp>
#include <optional>

#include "storage/redis_metadata.h"

class TSChunk;
class UncompTSChunk;

using TSChunkPtr = std::unique_ptr<TSChunk>;
using OwnedTSChunk = std::tuple<TSChunkPtr, std::string>;

// Creates a TSChunk from the provided raw data buffer.
TSChunkPtr CreateTSChunkFromData(nonstd::span<const char> data);

// Creates an empty owned time series chunk with specified compression option.
OwnedTSChunk CreateEmptyOwnedTSChunk(bool is_compressed = false);

struct TSSample {
  uint64_t ts;
  double v;

  static constexpr uint64_t MAX_TIMESTAMP = std::numeric_limits<uint64_t>::max();
  static constexpr double NAN_VALUE = std::numeric_limits<double>::quiet_NaN();

  // Custom comparison operator for sorting by ts
  bool operator<(const TSSample& other) const { return ts < other.ts; }
  bool operator==(const TSSample& other) const { return ts == other.ts; }
};

// Simple TSChunk iterator base class providing basic traversal functionality
class TSChunkIterator {
 public:
  explicit TSChunkIterator(uint64_t count) : count_(count), idx_(0) {}
  virtual ~TSChunkIterator() = default;

  virtual std::optional<const TSSample*> Next() = 0;
  virtual bool HasNext() const { return idx_ < count_; }

 protected:
  uint64_t count_;
  uint64_t idx_;
};

class TSChunk {
 public:
  using DuplicatePolicy = TimeSeriesMetadata::DuplicatePolicy;

  enum class AddResultType : uint8_t {
    kNone,
    kInsert,
    kUpdate,
    kSkip,
    kBlock,
    kOld,
  };
  struct AddResult {
    AddResultType type = AddResultType::kNone;
    TSSample sample = {0, 0.0};

    static inline AddResult CreateInsert(const TSSample& sample) {
      AddResult result;
      result.type = AddResultType::kInsert;
      result.sample = sample;
      return result;
    }
  };

  class SampleBatch;
  class SampleBatchSlice {
   public:
    nonstd::span<const TSSample> GetSampleSpan() const { return sample_span_; }
    nonstd::span<AddResult> GetAddResultSpan() { return add_result_span_; }
    nonstd::span<const AddResult> GetAddResultSpan() const { return add_result_span_; }

    // Slice samples by count. Returns count valid samples starting from first timestamp
    // e.g., samples: {100,200,300}, first=200, count=2 -> {200,300}
    SampleBatchSlice SliceByCount(uint64_t first, int count, uint64_t* last_ts = nullptr);

    // Slice samples by timestamp range [first, last)
    // e.g., samples: {10,20,30,40}, first=20, last=40 -> {20,30}
    SampleBatchSlice SliceByTimestamps(uint64_t first, uint64_t last, bool contain_last = false);

    uint64_t GetFirstTimestamp() const;
    uint64_t GetLastTimestamp() const;

    // Get number of valid samples (excluding duplicates and expired entries)
    size_t GetValidCount() const;

    DuplicatePolicy GetPolicy() const { return policy_; }
    size_t Size() const { return sample_span_.size(); }
    bool Empty() const { return sample_span_.empty(); }

    friend class TSChunk::SampleBatch;

   private:
    nonstd::span<const TSSample> sample_span_;
    nonstd::span<AddResult> add_result_span_;
    DuplicatePolicy policy_;

    SampleBatchSlice() = default;
    SampleBatchSlice(nonstd::span<const TSSample> samples, nonstd::span<AddResult> results, DuplicatePolicy policy)
        : sample_span_(samples), add_result_span_(results), policy_(policy) {}

    SampleBatchSlice createSampleSlice(size_t start_idx, size_t end_idx) const;
  };

  class SampleBatch {
   public:
    // Construct a batch of samples with duplicate policy
    // Samples will be sorted and deduplicated according to policy
    SampleBatch(std::vector<TSSample> samples, DuplicatePolicy policy);

    // Mark samples as expired if ts + retention < last_ts
    // e.g., retention=3600, last_ts=5000 -> samples before 5000-3600 are expired
    void Expire(uint64_t last_ts, uint64_t retention);

    SampleBatchSlice AsSlice();

    // Return add results by samples' order
    std::vector<AddResult> GetFinalResults() const;

   private:
    std::vector<TSSample> samples_;
    std::vector<size_t> indexes_;  // Record original index cause of sorting
    std::vector<AddResult> add_results_;
    DuplicatePolicy policy_;

    void sortAndOrganize();
  };

  struct MetaData {
    constexpr static size_t kEncodedSize = 2 * sizeof(uint32_t);

    bool is_compressed;
    uint32_t count;

    MetaData() = default;
    MetaData(bool is_compressed, uint32_t count) : is_compressed(is_compressed), count(count) {}
    std::string Encode() const;
    void Decode(Slice* input);
  };

  explicit TSChunk(nonstd::span<const char> data);

  virtual ~TSChunk() = default;

  // Merge samples with duplicate policy handling
  // Returns result status, updates 'to' value according to policy
  static AddResult MergeSamplesValue(TSSample& to, const TSSample& from, DuplicatePolicy policy,
                                     bool is_batch_process = false);

  virtual std::unique_ptr<TSChunkIterator> CreateIterator() const = 0;

  uint32_t GetCount() const;
  virtual uint64_t GetFirstTimestamp() const = 0;
  virtual uint64_t GetLastTimestamp() const = 0;

  // Add new samples to the chunk according to duplicate policy
  // Returns new chunk data with merged samples. Returns empty string if no changes
  virtual std::string UpsertSamples(SampleBatchSlice samples) const = 0;

  // Add new samples to the chunk according to duplicate policy
  // Split chunk and return new chunk. There two split modes:
  // 1. Fix split mode: used for unsealed chunk. 2. Equal split mode: used for sealed chunk.
  // Returns empty if no changes
  virtual std::vector<std::string> UpsertSampleAndSplit(SampleBatchSlice batch, uint64_t preferred_chunk_size,
                                                        bool is_fix_split_mode) const = 0;

  // Delete samples in [from, to] timestamp range
  // Returns new chunk data without deleted samples. Returns empty string if no changes
  std::string RemoveSamplesBetween(uint64_t from, uint64_t to, uint64_t* deleted = nullptr,
                                   bool inclusive_to = true) const;

  // Update sample value at specified timestamp
  // is_add_on controls whether to add to existing value or replace it
  // Returns empty string if no changes
  virtual std::string UpdateSampleValue(uint64_t ts, double value, bool is_add_on) const = 0;

  // Get idx-th latest sample, idx=0 means latest sample
  virtual TSSample GetLatestSample(uint32_t idx) const = 0;

  // Get all samples as a span
  virtual nonstd::span<const TSSample> GetSamplesSpan() const = 0;

 protected:
  nonstd::span<const char> data_;
  MetaData metadata_;

  virtual std::string doRemoveSamplesBetween(uint64_t from, uint64_t to, uint64_t* deleted,
                                             bool inclusive_to) const = 0;
};

class UncompTSChunk : public TSChunk {
 public:
  explicit UncompTSChunk(nonstd::span<const char> data);
  std::unique_ptr<TSChunkIterator> CreateIterator() const override;

  uint64_t GetFirstTimestamp() const override;
  uint64_t GetLastTimestamp() const override;

  std::string UpsertSamples(SampleBatchSlice samples) const override;
  std::vector<std::string> UpsertSampleAndSplit(SampleBatchSlice batch, uint64_t preferred_chunk_size,
                                                bool is_fix_split_mode) const override;
  std::string UpdateSampleValue(uint64_t ts, double value, bool is_add_on) const override;
  TSSample GetLatestSample(uint32_t idx) const override;

  nonstd::span<const TSSample> GetSamplesSpan() const override { return samples_; }

 protected:
  std::string doRemoveSamplesBetween(uint64_t from, uint64_t to, uint64_t* deleted, bool inclusive_to) const override;

 private:
  nonstd::span<const TSSample> samples_;
};
