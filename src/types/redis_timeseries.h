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

#include <cstdint>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "types/timeseries.h"

namespace redis {

enum class TSSubkeyType : uint8_t {
  CHUNK = 0,
  LABEL = 1,
  DOWNSTREAM = 2,
};

// Enum prefix for new CF.
enum class IndexKeyType : uint8_t {
  TS_LABEL = 0,
};

enum class TSAggregatorType : uint8_t {
  NONE = 0,
  AVG = 1,
  SUM = 2,
  MIN = 3,
  MAX = 4,
  RANGE = 5,
  COUNT = 6,
  FIRST = 7,
  LAST = 8,
  STD_P = 9,
  STD_S = 10,
  VAR_P = 11,
  VAR_S = 12,
};

struct TSAggregator {
  TSAggregatorType type = TSAggregatorType::NONE;
  uint64_t bucket_duration = 0;
  uint64_t alignment = 0;

  TSAggregator() = default;
  TSAggregator(TSAggregatorType type, uint64_t bucket_duration, uint64_t alignment)
      : type(type), bucket_duration(bucket_duration), alignment(alignment) {}

  // Calculates the start timestamp of the aligned bucket that contains the given timestamp.
  // E.g. `ts`=100, `duration`=30, `alignment`=20.
  // The bucket containing `ts=100` starts at `80` (since 80 ≤ 100 < 110). Returns `80`.
  uint64_t CalculateAlignedBucketLeft(uint64_t ts) const;

  // Calculates the end timestamp of the aligned bucket that contains the given timestamp.
  uint64_t CalculateAlignedBucketRight(uint64_t ts) const;

  // Calculates the aggregated value of the given samples according to the aggregator type
  double AggregateSamplesValue(nonstd::span<const TSSample> samples) const;
};

struct TSDownStreamMeta {
  TSAggregator aggregator;
  uint64_t latest_bucket_idx;

  // store auxiliary info for each aggregator.
  // e.g. for avg, need to store sum and count: u64_auxs={count}, f64_auxs={sum}
  std::vector<uint64_t> u64_auxs;
  std::vector<double> f64_auxs;

  TSDownStreamMeta() = default;
  TSDownStreamMeta(TSAggregatorType agg_type, uint64_t bucket_duration, uint64_t alignment, uint64_t latest_bucket_idx)
      : aggregator(agg_type, bucket_duration, alignment), latest_bucket_idx(latest_bucket_idx) {}

  void Encode(std::string *dst) const;
  rocksdb::Status Decode(Slice *input);
};

struct TSRevLabelKey {
  Slice ns;
  Slice label_key;
  Slice label_value;
  Slice user_key;

  TSRevLabelKey(Slice ns, Slice label_key, Slice label_value, Slice user_key = Slice())
      : ns(ns), label_key(label_key), label_value(label_value), user_key(user_key) {}

  [[nodiscard]] std::string Encode() const;
};

struct LabelKVPair {
  std::string k;
  std::string v;
};
using LabelKVList = std::vector<LabelKVPair>;

struct TSCreateOption {
  uint64_t retention_time;
  uint64_t chunk_size;
  TimeSeriesMetadata::ChunkType chunk_type;
  TimeSeriesMetadata::DuplicatePolicy duplicate_policy;
  std::string source_key;
  LabelKVList labels;

  TSCreateOption();
};

struct TSInfoResult {
  TimeSeriesMetadata metadata;
  uint64_t total_samples;
  uint64_t memory_usage;
  uint64_t first_timestamp;
  uint64_t last_timestamp;
  std::vector<std::pair<std::string, TSDownStreamMeta>> downstream_rules;
  LabelKVList labels;
};

struct TSRangeOption {
  enum class BucketTimestampType : uint8_t {
    Start = 0,
    End = 1,
    Mid = 2,
  };
  uint64_t start_ts = 0;
  uint64_t end_ts = TSSample::MAX_TIMESTAMP;
  uint64_t count_limit = 0;
  std::set<uint64_t> filter_by_ts;
  std::optional<std::pair<double, double>> filter_by_value;

  // Used for comapction
  TSAggregator aggregator;
  bool is_return_latest = false;
  bool is_return_empty = false;
  BucketTimestampType bucket_timestamp_type = BucketTimestampType::Start;
};

TimeSeriesMetadata CreateMetadataFromOption(const TSCreateOption &option);

class TimeSeries : public SubKeyScanner {
 public:
  using SampleBatch = TSChunk::SampleBatch;
  using AddResultWithTS = TSChunk::AddResultWithTS;
  using DuplicatePolicy = TimeSeriesMetadata::DuplicatePolicy;

  TimeSeries(engine::Storage *storage, const std::string &ns) : SubKeyScanner(storage, ns) {}
  rocksdb::Status Create(engine::Context &ctx, const Slice &user_key, const TSCreateOption &option);
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, TSSample sample, const TSCreateOption &option,
                      AddResultWithTS *res, const DuplicatePolicy *on_dup_policy = nullptr);
  rocksdb::Status MAdd(engine::Context &ctx, const Slice &user_key, std::vector<TSSample> samples,
                       std::vector<AddResultWithTS> *res);
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, TSInfoResult *res);
  rocksdb::Status Range(engine::Context &ctx, const Slice &user_key, const TSRangeOption &option,
                        std::vector<TSSample> *res);

 private:
  rocksdb::Status getTimeSeriesMetadata(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata *metadata);
  rocksdb::Status createTimeSeries(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata *metadata_out,
                                   const TSCreateOption *options);
  rocksdb::Status getOrCreateTimeSeries(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata *metadata_out,
                                        const TSCreateOption *option = nullptr);
  rocksdb::Status getLabelKVList(engine::Context &ctx, const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                 LabelKVList *labels);
  rocksdb::Status upsertCommon(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                               SampleBatch &sample_batch);
  rocksdb::Status createLabelIndexInBatch(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                          ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                          const LabelKVList &labels);
  std::string internalKeyFromChunkID(const Slice &ns_key, const TimeSeriesMetadata &metadata, uint64_t id) const;
  std::string internalKeyFromLabelKey(const Slice &ns_key, const TimeSeriesMetadata &metadata, Slice label_key) const;
  std::string internalKeyFromDownstreamKey(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                           Slice downstream_key) const;
  std::string labelKeyFromInternalKey(Slice internal_key) const;
  static uint64_t chunkIDFromInternalKey(Slice internal_key);
};

}  // namespace redis
