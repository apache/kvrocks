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

struct TSSample {
  uint64_t ts;
  double v;

  // Custom comparison operator for sorting by ts
  bool operator<(const TSSample& other) const { return ts < other.ts; }
  bool operator==(const TSSample& other) const { return ts == other.ts; }
};

class TSChunkIterator {
 public:
  TSChunkIterator(uint64_t count) : count_(count), idx_(0) {}
  virtual ~TSChunkIterator() = default;

  virtual std::optional<TSSample*> next() = 0;
  virtual bool has_next() const { return idx_ < count_; }

 protected:
  uint64_t count_;
  uint64_t idx_;
};

class TSChunk {
 public:
  using DuplicatePolicy = TimeSeriesMetadata::DuplicatePolicy;

  enum class AddResult : uint8_t {
    kNone,
    kOk,
    kBlock,
  };

  class SampleBatchSlice {
   public:
    SampleBatchSlice() = default;
    SampleBatchSlice(nonstd::span<const TSSample> samples, nonstd::span<AddResult> results, DuplicatePolicy policy)
        : sample_span_(samples), add_result_span_(results), policy_(policy) {}

    nonstd::span<const TSSample> GetSampleSpan() const { return sample_span_; }
    nonstd::span<AddResult> GetAddResultSpan() { return add_result_span_; }
    nonstd::span<const AddResult> GetAddResultSpan() const { return add_result_span_; }

    uint64_t GetFirstTimestamp();
    uint64_t GetLastTimestamp();

    size_t GetUniqueCount() const;

    DuplicatePolicy GetPolicy() const { return policy_; }
    size_t Size() const { return sample_span_.size(); }
    bool Empty() const { return sample_span_.empty(); }

   private:
    nonstd::span<const TSSample> sample_span_;
    nonstd::span<AddResult> add_result_span_;
    DuplicatePolicy policy_;
  };

  class SampleBatch {
   public:
    SampleBatch(size_t size, DuplicatePolicy policy);

    void Push(const TSSample& sample);

    void SortAndOrganize();

    // Slice samples by timestamp.
    // Note: timestamps must be sorted and timestamp[0] <= this->GetfirstTimestamp()
    // e.g. samples: {10,20,30,40}, timestamps: {5,15,30} -> Slice1:{10}, Slice2:{20},Sl
    std::vector<SampleBatchSlice> SliceByTimestamps(const std::vector<uint64_t>& timestamps);

    SampleBatchSlice AsSlice();

   private:
    std::vector<TSSample> samples_;
    std::vector<size_t> indexes_;  // Record original index cause of sorting
    std::vector<AddResult> add_results_;
    DuplicatePolicy policy_;
    size_t unique_count_;  // unique samples

    bool is_sorted_;

    void EnsureSorted();
  };

  explicit TSChunk(std::string* data) : data_(data), count_(0) {}

  virtual ~TSChunk() = default;

  static AddResult MergeSamplesValue(TSSample& a, const TSSample& b, DuplicatePolicy policy);

  virtual std::unique_ptr<TSChunkIterator> create_iterator() const = 0;

  virtual void MAddSample(SampleBatchSlice samples) = 0;

 protected:
  std::string* data_;
  uint64_t count_;
};
