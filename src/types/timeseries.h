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

using TSChunkPtr = std::shared_ptr<TSChunk>;
using OwnedTSChunk = std::tuple<TSChunkPtr, std::string>;

TSChunkPtr createTSChunkFromData(nonstd::span<char> data);

OwnedTSChunk createEmptyOwnedTSChunk(bool is_compressed = false);

struct TSSample {
  uint64_t ts;
  double v;

  static constexpr uint64_t MAX_TIMESTAMP = std::numeric_limits<uint64_t>::max();

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
    kOld,
  };

  class SampleBatch;
  class SampleBatchSlice {
   public:
    nonstd::span<const TSSample> GetSampleSpan() const { return sample_span_; }
    nonstd::span<AddResult> GetAddResultSpan() { return add_result_span_; }
    nonstd::span<const AddResult> GetAddResultSpan() const { return add_result_span_; }

    SampleBatchSlice SliceByCount(uint64_t first, int count, uint64_t* last_ts = nullptr);

    // Slice samples by timestamp.
    // e.g. samples: {10,20,30,40}, first=20, last=40 -> Slice:{20,30}
    SampleBatchSlice SliceByTimestamps(uint64_t first, uint64_t last, bool contain_last = false);

    uint64_t GetFirstTimestamp();
    uint64_t GetLastTimestamp();

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

    SampleBatchSlice createSampleSlice(size_t start_idx, size_t end_idx);
  };

  class SampleBatch {
   public:
    SampleBatch(std::vector<TSSample> samples, DuplicatePolicy policy);

    void Expire(uint64_t last_ts, uint64_t retention);

    SampleBatchSlice AsSlice();

    std::vector<AddResult> GetFinalResults() const;

   private:
    std::vector<TSSample> samples_;
    std::vector<size_t> indexes_;  // Record original index cause of sorting
    std::vector<AddResult> add_results_;
    DuplicatePolicy policy_;

    void SortAndOrganize();
  };

  struct MetaData {
    constexpr static size_t kEncodedSize = 2 * sizeof(uint32_t);

    bool is_compressed;
    uint32_t count;

    MetaData() = default;
    MetaData(bool is_compressed, uint32_t count) : is_compressed(is_compressed), count(count) {}
    std::string Encode();
    void Decode(Slice* input);
  };

  explicit TSChunk(nonstd::span<char> data);

  virtual ~TSChunk() = default;

  static AddResult MergeSamplesValue(TSSample& a, const TSSample& b, DuplicatePolicy policy);

  virtual std::unique_ptr<TSChunkIterator> CreateIterator() const = 0;

  uint32_t GetCount() const;
  virtual uint64_t GetFirstTimestamp() const = 0;
  virtual uint64_t GetLastTimestamp() const = 0;

  virtual std::string MAddSample(SampleBatchSlice samples) const = 0;
  virtual std::string DelSampleInRange(uint64_t from, uint64_t to) const = 0;
  virtual std::string UpdateSample(uint64_t ts, double value, bool is_add_on) const = 0;

 protected:
  nonstd::span<char> data_;
  MetaData metadata_;
};

class UncompTSChunk : public TSChunk {
 public:
  explicit UncompTSChunk(nonstd::span<char> data);
  std::unique_ptr<TSChunkIterator> CreateIterator() const override;

  uint64_t GetFirstTimestamp() const override;
  uint64_t GetLastTimestamp() const override;

  std::string MAddSample(SampleBatchSlice samples) const override;
  std::string DelSampleInRange(uint64_t from, uint64_t to) const override;
  std::string UpdateSample(uint64_t ts, double value, bool is_add_on) const override;

 private:
  nonstd::span<TSSample> samples_;
};
