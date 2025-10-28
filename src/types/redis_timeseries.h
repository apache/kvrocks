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
  SUM = 1,
  MIN = 2,
  MAX = 3,
  COUNT = 4,
  FIRST = 5,
  LAST = 6,
  AVG = 7,
  RANGE = 8,
  STD_P = 9,
  STD_S = 10,
  VAR_P = 11,
  VAR_S = 12,
};

inline bool IsIncrementalAggregatorType(TSAggregatorType type) {
  auto type_num = static_cast<uint8_t>(type);
  return type_num >= 1 && type_num <= 4;
}

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

  // Splits the given samples into buckets.
  std::vector<nonstd::span<const TSSample>> SplitSamplesToBuckets(nonstd::span<const TSSample> samples) const;

  // Returns the samples earlier than `less_than` in the bucket that contains `ts`.
  nonstd::span<const TSSample> GetBucketByTimestamp(nonstd::span<const TSSample> samples, uint64_t ts,
                                                    uint64_t less_than = TSSample::MAX_TIMESTAMP) const;

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

  // Aggregate samples and update the auxiliary info and latest_bucket_idx if needed.
  // Returns the aggregated samples if there are new buckets.
  // Note: Samples must be sorted by timestamp.
  std::vector<TSSample> AggregateMultiBuckets(const std::vector<nonstd::span<const TSSample>> &bucket_spans,
                                              bool skip_last_bucket = false);

  // Aggregate the samples to the latest bucket, update the auxiliary info.
  void AggregateLatestBucket(nonstd::span<const TSSample> samples);

  // Reset auxiliary info.
  void ResetAuxs();

  void Encode(std::string *dst) const;
  rocksdb::Status Decode(Slice *input);
};

struct IndexInternalKey {
  Slice ns;
  IndexKeyType type;
  IndexInternalKey(Slice ns, IndexKeyType type) : ns(ns), type(type) {}
  explicit IndexInternalKey(Slice input);
};

struct TSRevLabelKey : public IndexInternalKey {
  Slice label_key;
  Slice label_value;
  Slice user_key;

  TSRevLabelKey(Slice ns, Slice label_key, Slice label_value, Slice user_key = Slice())
      : IndexInternalKey(ns, IndexKeyType::TS_LABEL),
        label_key(label_key),
        label_value(label_value),
        user_key(user_key) {}
  explicit TSRevLabelKey(Slice input);

  [[nodiscard]] std::string Encode() const;
  static std::string UpperBound(Slice ns);
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
  std::vector<std::pair<std::string, TSAggregator>> downstream_rules;
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

struct TSMGetOption {
  struct FilterOption {
    std::unordered_map<std::string, std::set<std::string>> labels_equals;
    std::unordered_map<std::string, std::set<std::string>> labels_not_equals;
  };

  bool with_labels = false;
  std::set<std::string> selected_labels;
  FilterOption filter;
};

struct TSMGetResult {
  std::string name;  // name of the source key or the group
  LabelKVList labels;
  std::vector<TSSample> samples;
};

class TSMQueryFilterParser {
 public:
  explicit TSMQueryFilterParser(TSMGetOption::FilterOption &option) : option_(option) {}
  Status Parse(std::string_view expr);
  Status Check() const;

 private:
  TSMGetOption::FilterOption &option_;
  bool has_matcher_ = false;
  static std::pair<size_t, size_t> findOperator(std::string_view expr);
  static std::string_view trim(std::string_view s);
  static std::string_view unquote(std::string_view s);
  static std::vector<std::string_view> splitValueList(std::string_view list);
  void handleEquals(std::string_view label, std::string_view value_str);
  void handleNotEquals(std::string_view label, std::string_view value_str);
};

struct TSMRangeOption : TSMGetOption, TSRangeOption {
  enum class GroupReducerType : uint8_t {
    NONE = 0,
    AVG = 1,
    SUM = 2,
    MIN = 3,
    MAX = 4,
    RANGE = 5,
    COUNT = 6,
    STD_P = 7,
    STD_S = 8,
    VAR_P = 9,
    VAR_S = 10,
  };

  GroupReducerType reducer = GroupReducerType::NONE;
  std::string group_by_label;
};

struct TSMRangeResult : TSMGetResult {
  std::vector<std::string> source_keys;
};

enum class TSCreateRuleResult : uint8_t {
  kOK = 0,
  kSrcNotExist = 1,
  kDstNotExist = 2,
  kSrcHasSourceRule = 3,
  kDstHasSourceRule = 4,
  kDstHasDestRule = 5,
  kSrcEqDst = 6,
};

std::vector<TSSample> GroupSamplesAndReduce(const std::vector<std::vector<TSSample>> &all_samples,
                                            TSMRangeOption::GroupReducerType reducer_type);

TimeSeriesMetadata CreateMetadataFromOption(const TSCreateOption &option);

class TimeSeries : public SubKeyScanner {
 public:
  using SampleBatch = TSChunk::SampleBatch;
  using AddResult = TSChunk::AddResult;
  using DuplicatePolicy = TimeSeriesMetadata::DuplicatePolicy;

  TimeSeries(engine::Storage *storage, const std::string &ns)
      : SubKeyScanner(storage, ns), index_cf_handle_(storage->GetCFHandle(ColumnFamilyID::Index)) {}
  rocksdb::Status Create(engine::Context &ctx, const Slice &user_key, const TSCreateOption &option);
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, TSSample sample, const TSCreateOption &option,
                      AddResult *res, const DuplicatePolicy *on_dup_policy = nullptr);
  rocksdb::Status MAdd(engine::Context &ctx, const Slice &user_key, std::vector<TSSample> samples,
                       std::vector<AddResult> *res);
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, TSInfoResult *res);
  rocksdb::Status Range(engine::Context &ctx, const Slice &user_key, const TSRangeOption &option,
                        std::vector<TSSample> *res);
  rocksdb::Status RevRange(engine::Context &ctx, const Slice &user_key, const TSRangeOption &option,
                           std::vector<TSSample> *res);
  rocksdb::Status Get(engine::Context &ctx, const Slice &user_key, bool is_return_latest, std::vector<TSSample> *res);
  rocksdb::Status CreateRule(engine::Context &ctx, const Slice &src_key, const Slice &dst_key,
                             const TSAggregator &aggregator, TSCreateRuleResult *res);
  rocksdb::Status MGet(engine::Context &ctx, const TSMGetOption &option, bool is_return_latest,
                       std::vector<TSMGetResult> *res);
  rocksdb::Status MRange(engine::Context &ctx, const TSMRangeOption &option, std::vector<TSMRangeResult> *res);
  rocksdb::Status IncrBy(engine::Context &ctx, const Slice &user_key, TSSample sample, const TSCreateOption &option,
                         AddResult *res);
  rocksdb::Status Del(engine::Context &ctx, const Slice &user_key, uint64_t from, uint64_t to, uint64_t *deleted);
  rocksdb::Status IsTSSubKeyExpired(const TimeSeriesMetadata &metadata, const Slice &key, const Slice &value,
                                    bool &expired);

  static bool ExtractTSSubType(const InternalKey &ikey, TSSubkeyType *type);

 private:
  // Bundles the arguments for a downstream upsert operation
  struct DownstreamUpsertArgs {
    std::vector<std::string> new_chunks;  // Newly created chunks
    bool was_source_empty = false;        // Whether the source time series was empty before upsert
    SampleBatch *sample_batch = nullptr;  // The sample batch that has been upserted to source
  };

  rocksdb::ColumnFamilyHandle *index_cf_handle_;

  rocksdb::Status getTimeSeriesMetadata(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata *metadata);
  rocksdb::Status createTimeSeries(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata *metadata_out,
                                   const TSCreateOption *options);
  rocksdb::Status getOrCreateTimeSeries(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata *metadata_out,
                                        const TSCreateOption *option = nullptr);
  rocksdb::Status getLabelKVList(engine::Context &ctx, const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                 LabelKVList *labels);
  rocksdb::Status upsertCommon(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                               SampleBatch &sample_batch, DownstreamUpsertArgs *ds_args = nullptr);
  rocksdb::Status upsertCommonInBatch(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                                      SampleBatch &sample_batch, ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                      DownstreamUpsertArgs *ds_args = nullptr);
  rocksdb::Status rangeCommon(engine::Context &ctx, const Slice &ns_key, const TimeSeriesMetadata &metadata,
                              const TSRangeOption &option, std::vector<TSSample> *res, bool apply_retention = true);
  rocksdb::Status upsertDownStream(engine::Context &ctx, const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                   DownstreamUpsertArgs &ds_args);
  rocksdb::Status getCommon(engine::Context &ctx, const Slice &ns_key, const TimeSeriesMetadata &metadata,
                            bool is_return_latest, std::vector<TSSample> *res);
  rocksdb::Status delRangeCommon(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata, uint64_t from,
                                 uint64_t to, uint64_t *deleted, bool inclusive_to = true);
  rocksdb::Status delRangeCommonInBatch(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                                        uint64_t from, uint64_t to, ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                        uint64_t *deleted, bool inclusive_to = true);
  rocksdb::Status delRangeDownStream(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                                     std::vector<std::string> &ds_user_keys, std::vector<TSDownStreamMeta> &ds_metas,
                                     std::vector<TimeSeriesMetadata> &ds_series_metas, uint64_t from, uint64_t to);
  rocksdb::Status createLabelIndexInBatch(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                          ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                          const LabelKVList &labels);
  rocksdb::Status createDownStreamMetadataInBatch(engine::Context &ctx, const Slice &ns_src_key, const Slice &dst_key,
                                                  const TimeSeriesMetadata &src_metadata,
                                                  const TSAggregator &aggregator,
                                                  ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                                  TSDownStreamMeta *ds_metadata);
  // Get downstream rules of the source time series.
  // - `ds_user_keys`: the user keys of the downstream time series.
  // - `ds_metas`: (optional) the downstream rule meta.
  // - `ds_series_metadatas`: (optional) the metadata of the downstream time series.
  rocksdb::Status getDownStreamRules(engine::Context &ctx, const Slice &ns_src_key,
                                     const TimeSeriesMetadata &src_metadata, std::vector<std::string> *ds_user_keys,
                                     std::vector<TSDownStreamMeta> *ds_metas = nullptr,
                                     std::vector<TimeSeriesMetadata> *ds_series_metadatas = nullptr);
  rocksdb::Status getTSKeyByFilter(engine::Context &ctx, const TSMGetOption::FilterOption &filter,
                                   std::vector<std::string> *user_keys, std::vector<LabelKVList> *labels_vec = nullptr,
                                   std::vector<TimeSeriesMetadata> *metas = nullptr);
  rocksdb::Status checkTSMetadataSourceExists(engine::Context &ctx, const TimeSeriesMetadata &metadata, bool &exists,
                                              TimeSeriesMetadata *src_metadata = nullptr);

  std::string internalKeyFromChunkID(const Slice &ns_key, const TimeSeriesMetadata &metadata, uint64_t id) const;
  std::string internalKeyFromLabelKey(const Slice &ns_key, const TimeSeriesMetadata &metadata, Slice label_key) const;
  std::string internalKeyFromDownstreamKey(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                           Slice downstream_key) const;
  std::string labelKeyFromInternalKey(Slice internal_key) const;
  std::string downstreamKeyFromInternalKey(Slice internal_key) const;
  static uint64_t chunkIDFromInternalKey(Slice internal_key);
  static bool isChunkExpired(const TimeSeriesMetadata &metadata, const Slice &chunk_value);
};

}  // namespace redis
