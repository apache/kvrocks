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

#include "redis_timeseries.h"

#include "commands/error_constants.h"
#include "db_util.h"

namespace redis {

// TODO: make it configurable
constexpr uint64_t kDefaultRetentionTime = 0;
constexpr uint64_t kDefaultChunkSize = 1024;
constexpr auto kDefaultChunkType = TimeSeriesMetadata::ChunkType::UNCOMPRESSED;
constexpr auto kDefaultDuplicatePolicy = TimeSeriesMetadata::DuplicatePolicy::BLOCK;

struct Reducer {
  static inline double Sum(nonstd::span<const TSSample> samples) {
    return std::accumulate(samples.begin(), samples.end(), 0.0,
                           [](double acc, const TSSample &sample) { return acc + sample.v; });
  }
  static inline double SquareSum(nonstd::span<const TSSample> samples) {
    return std::accumulate(samples.begin(), samples.end(), 0.0,
                           [](double acc, const TSSample &sample) { return acc + sample.v * sample.v; });
  }
  static inline double Min(nonstd::span<const TSSample> samples) {
    return std::min_element(samples.begin(), samples.end(),
                            [](const TSSample &a, const TSSample &b) { return a.v < b.v; })
        ->v;
  }
  static inline double Max(nonstd::span<const TSSample> samples) {
    return std::max_element(samples.begin(), samples.end(),
                            [](const TSSample &a, const TSSample &b) { return a.v < b.v; })
        ->v;
  }
  static inline double VarP(nonstd::span<const TSSample> samples) {
    auto sample_size = static_cast<double>(samples.size());
    double sum = Sum(samples);
    double square_sum = SquareSum(samples);
    return (square_sum - sum * sum / sample_size) / sample_size;
  }
  static inline double VarS(nonstd::span<const TSSample> samples) {
    if (samples.size() <= 1) return 0.0;
    auto sample_size = static_cast<double>(samples.size());
    return VarP(samples) * sample_size / (sample_size - 1);
  }
  static inline double StdP(nonstd::span<const TSSample> samples) { return std::sqrt(VarP(samples)); }

  static inline double StdS(nonstd::span<const TSSample> samples) { return std::sqrt(VarS(samples)); }
  static inline double Range(nonstd::span<const TSSample> samples) {
    if (samples.empty()) return 0.0;
    auto [min, max] = std::minmax_element(samples.begin(), samples.end(),
                                          [](const TSSample &a, const TSSample &b) { return a.v < b.v; });
    return max->v - min->v;
  }
};

std::vector<TSSample> AggregateSamplesByRangeOption(std::vector<TSSample> samples, const TSRangeOption &option) {
  const auto &aggregator = option.aggregator;
  std::vector<TSSample> res;
  if (aggregator.type == TSAggregatorType::NONE || samples.empty()) {
    res = std::move(samples);
    return res;
  }
  auto spans = aggregator.SplitSamplesToBuckets(samples);

  auto get_bucket_ts = [&](uint64_t left) -> uint64_t {
    using BucketTimestampType = TSRangeOption::BucketTimestampType;
    switch (option.bucket_timestamp_type) {
      case BucketTimestampType::Start:
        return left;
      case BucketTimestampType::End:
        return left + aggregator.bucket_duration;
      case BucketTimestampType::Mid:
        return left + aggregator.bucket_duration / 2;
      default:
        unreachable();
    }
    return 0;
  };
  res.reserve(spans.size());
  uint64_t bucket_left = aggregator.CalculateAlignedBucketLeft(samples.front().ts);
  for (size_t i = 0; i < spans.size(); i++) {
    if (option.count_limit && res.size() >= option.count_limit) {
      break;
    }
    TSSample sample;
    if (i != 0) {
      bucket_left = aggregator.CalculateAlignedBucketRight(bucket_left);
    }
    sample.ts = get_bucket_ts(bucket_left);
    if (option.is_return_empty && spans[i].empty()) {
      switch (aggregator.type) {
        case TSAggregatorType::SUM:
        case TSAggregatorType::COUNT:
          sample.v = 0;
          break;
        case TSAggregatorType::LAST:
          if (i == 0 || spans[i - 1].empty()) {
            sample.v = TSSample::NAN_VALUE;
          } else {
            sample.v = spans[i].back().v;
          }
          break;
        default:
          sample.v = TSSample::NAN_VALUE;
      }
    } else if (!spans[i].empty()) {
      sample.v = aggregator.AggregateSamplesValue(spans[i]);
    } else {
      continue;
    }
    res.emplace_back(sample);
  }
  return res;
}

std::vector<TSSample> TSDownStreamMeta::AggregateMultiBuckets(nonstd::span<const TSSample> samples,
                                                              bool skip_last_bucket) {
  std::vector<TSSample> res;
  auto bucket_spans = aggregator.SplitSamplesToBuckets(samples);
  for (size_t i = 0; i < bucket_spans.size(); i++) {
    const auto &span = bucket_spans[i];
    if (span.empty()) {
      continue;
    }
    auto bucket_idx = aggregator.CalculateAlignedBucketLeft(span.front().ts);
    if (bucket_idx < latest_bucket_idx) {
      continue;
    }
    if (bucket_idx > latest_bucket_idx) {
      // Aggregate the previous bucket from aux info and push to result
      TSSample sample;
      sample.ts = latest_bucket_idx;
      double v = 0.0;
      double temp_n = 0.0;
      switch (aggregator.type) {
        case TSAggregatorType::SUM:
        case TSAggregatorType::MIN:
        case TSAggregatorType::MAX:
        case TSAggregatorType::COUNT:
        case TSAggregatorType::FIRST:
        case TSAggregatorType::LAST:
          sample.v = f64_auxs[0];
          break;
        case TSAggregatorType::AVG:
          temp_n = static_cast<double>(u64_auxs[0]);
          sample.v = f64_auxs[0] / temp_n;
          break;
        case TSAggregatorType::STD_P:
        case TSAggregatorType::STD_S:
        case TSAggregatorType::VAR_P:
        case TSAggregatorType::VAR_S:
          temp_n = static_cast<double>(u64_auxs[0]);
          v = f64_auxs[1] - f64_auxs[0] * f64_auxs[0] / temp_n;
          if (aggregator.type == TSAggregatorType::STD_S || aggregator.type == TSAggregatorType::VAR_S) {
            if (u64_auxs[0] > 1) {
              v = v / (temp_n - 1);
            } else {
              v = 0.0;
            }
          } else {
            v = v / temp_n;
          }
          if (aggregator.type == TSAggregatorType::STD_P || aggregator.type == TSAggregatorType::STD_S) {
            sample.v = std::sqrt(v);
          } else {
            sample.v = v;
          }
          break;
        case TSAggregatorType::RANGE:
          sample.v = f64_auxs[1] - f64_auxs[0];
          break;
        default:
          unreachable();
      }
      res.push_back(sample);
      // Reset aux info for the new bucket
      ResetAuxs();
      latest_bucket_idx = bucket_idx;
    }
    if (skip_last_bucket && i == bucket_spans.size() - 1) {
      // Skip updating aux info for the last bucket
      break;
    }
    AggregateLatestBucket(span);
  }

  return res;
}

void TSDownStreamMeta::AggregateLatestBucket(nonstd::span<const TSSample> samples) {
  double temp_v = 0.0;
  switch (aggregator.type) {
    case TSAggregatorType::SUM:
      f64_auxs[0] += Reducer::Sum(samples);
      break;
    case TSAggregatorType::MIN:
      temp_v = Reducer::Min(samples);
      f64_auxs[0] = std::isnan(f64_auxs[0]) ? temp_v : std::min(f64_auxs[0], temp_v);
      break;
    case TSAggregatorType::MAX:
      temp_v = Reducer::Max(samples);
      f64_auxs[0] = std::isnan(f64_auxs[0]) ? temp_v : std::max(f64_auxs[0], temp_v);
      break;
    case TSAggregatorType::COUNT:
      f64_auxs[0] += static_cast<double>(samples.size());
      break;
    case TSAggregatorType::FIRST:
      if (std::isnan(f64_auxs[0]) || samples.front().ts < u64_auxs[0]) {
        f64_auxs[0] = samples.front().v;
        u64_auxs[0] = samples.front().ts;
      }
      break;
    case TSAggregatorType::LAST:
      if (std::isnan(f64_auxs[0]) || samples.back().ts > u64_auxs[0]) {
        f64_auxs[0] = samples.back().v;
        u64_auxs[0] = samples.back().ts;
      }
      break;
    case TSAggregatorType::AVG:
      u64_auxs[0] += static_cast<uint64_t>(samples.size());
      f64_auxs[0] += Reducer::Sum(samples);
      break;
    case TSAggregatorType::STD_P:
    case TSAggregatorType::STD_S:
    case TSAggregatorType::VAR_P:
    case TSAggregatorType::VAR_S:
      u64_auxs[0] += static_cast<uint64_t>(samples.size());
      f64_auxs[0] += Reducer::Sum(samples);
      f64_auxs[1] += Reducer::SquareSum(samples);
      break;
    case TSAggregatorType::RANGE:
      if (std::isnan(f64_auxs[0])) {
        f64_auxs[0] = Reducer::Min(samples);
        f64_auxs[1] = Reducer::Max(samples);
      } else {
        f64_auxs[0] = std::min(f64_auxs[0], Reducer::Min(samples));
        f64_auxs[1] = std::max(f64_auxs[1], Reducer::Max(samples));
      }
      break;
    default:
      unreachable();
  }
}

void TSDownStreamMeta::ResetAuxs() {
  auto type = aggregator.type;
  switch (type) {
    case TSAggregatorType::SUM:
      f64_auxs = {0.0};
      break;
    case TSAggregatorType::MIN:
    case TSAggregatorType::MAX:
      f64_auxs = {TSSample::NAN_VALUE};
      break;
    case TSAggregatorType::COUNT:
      f64_auxs = {0};
      break;
    case TSAggregatorType::FIRST:
      u64_auxs = {TSSample::MAX_TIMESTAMP};
      f64_auxs = {TSSample::NAN_VALUE};
      break;
    case TSAggregatorType::LAST:
      u64_auxs = {0};
      f64_auxs = {TSSample::NAN_VALUE};
      break;
    case TSAggregatorType::AVG:
      u64_auxs = {0};
      f64_auxs = {0.0};
      break;
    case TSAggregatorType::STD_P:
    case TSAggregatorType::STD_S:
    case TSAggregatorType::VAR_P:
    case TSAggregatorType::VAR_S:
      u64_auxs = {0};
      f64_auxs = {0.0, 0.0};
      break;
    case TSAggregatorType::RANGE:
      f64_auxs = {TSSample::NAN_VALUE, TSSample::NAN_VALUE};
      break;
    default:
      unreachable();
  }
}

void TSDownStreamMeta::Encode(std::string *dst) const {
  PutFixed8(dst, static_cast<uint8_t>(aggregator.type));
  PutFixed64(dst, aggregator.bucket_duration);
  PutFixed64(dst, aggregator.alignment);
  PutFixed64(dst, latest_bucket_idx);
  PutFixed8(dst, static_cast<uint8_t>(u64_auxs.size()));
  PutFixed8(dst, static_cast<uint8_t>(f64_auxs.size()));
  for (const auto &aux : u64_auxs) {
    PutFixed64(dst, aux);
  }
  for (const auto &aux : f64_auxs) {
    PutDouble(dst, aux);
  }
}

rocksdb::Status TSDownStreamMeta::Decode(Slice *input) {
  if (input->size() < sizeof(uint8_t) * 3 + sizeof(uint64_t) * 3) {
    return rocksdb::Status::InvalidArgument("TSDownStreamMeta size is too short");
  }

  GetFixed8(input, reinterpret_cast<uint8_t *>(&aggregator.type));
  GetFixed64(input, &aggregator.bucket_duration);
  GetFixed64(input, &aggregator.alignment);
  GetFixed64(input, &latest_bucket_idx);
  uint8_t u64_auxs_size = 0;
  GetFixed8(input, &u64_auxs_size);
  uint8_t f64_auxs_size = 0;
  GetFixed8(input, &f64_auxs_size);

  // Strict checking to prevent accidental overwrites
  if (input->size() != sizeof(uint64_t) * u64_auxs_size + sizeof(double) * f64_auxs_size) {
    return rocksdb::Status::InvalidArgument("Invalid auxinfo size");
  }

  for (uint8_t i = 0; i < u64_auxs_size; i++) {
    uint64_t aux = 0;
    GetFixed64(input, &aux);
    u64_auxs.push_back(aux);
  }
  for (uint8_t i = 0; i < f64_auxs_size; i++) {
    double aux = 0;
    GetDouble(input, &aux);
    f64_auxs.push_back(aux);
  }

  return rocksdb::Status::OK();
}

std::string TSRevLabelKey::Encode() const {
  std::string encoded;
  size_t total = 1 + ns.size() + 1 + 4 + label_key.size() + 4 + label_value.size() + user_key.size();

  encoded.resize(total);
  auto buf = encoded.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(ns.size()));
  buf = EncodeBuffer(buf, ns);
  buf = EncodeFixed8(buf, static_cast<uint8_t>(IndexKeyType::TS_LABEL));
  buf = EncodeFixed32(buf, static_cast<uint32_t>(label_key.size()));
  buf = EncodeBuffer(buf, label_key);
  buf = EncodeFixed32(buf, static_cast<uint32_t>(label_value.size()));
  buf = EncodeBuffer(buf, label_value);
  EncodeBuffer(buf, user_key);

  return encoded;
}

TSCreateOption::TSCreateOption()
    : retention_time(kDefaultRetentionTime),
      chunk_size(kDefaultChunkSize),
      chunk_type(kDefaultChunkType),
      duplicate_policy(kDefaultDuplicatePolicy) {}

TimeSeriesMetadata CreateMetadataFromOption(const TSCreateOption &option) {
  TimeSeriesMetadata metadata;
  metadata.retention_time = option.retention_time;
  metadata.chunk_size = option.chunk_size;
  metadata.chunk_type = option.chunk_type;
  metadata.duplicate_policy = option.duplicate_policy;
  metadata.SetSourceKey(option.source_key);

  return metadata;
}

TSDownStreamMeta CreateDownStreamMetaFromAgg(const TSAggregator &aggregator) {
  TSDownStreamMeta meta;
  meta.aggregator = aggregator;
  meta.latest_bucket_idx = 0;
  meta.ResetAuxs();
  return meta;
}

uint64_t TSAggregator::CalculateAlignedBucketLeft(uint64_t ts) const {
  uint64_t x = 0;

  if (ts >= alignment) {
    uint64_t diff = ts - alignment;
    uint64_t k = diff / bucket_duration;
    x = alignment + k * bucket_duration;
  } else {
    uint64_t diff = alignment - ts;
    uint64_t m0 = diff / bucket_duration + (diff % bucket_duration == 0 ? 0 : 1);
    if (m0 <= alignment / bucket_duration) {
      x = alignment - m0 * bucket_duration;
    }
  }

  return x;
}

uint64_t TSAggregator::CalculateAlignedBucketRight(uint64_t ts) const {
  uint64_t x = TSSample::MAX_TIMESTAMP;
  if (ts < alignment) {
    uint64_t diff = alignment - ts;
    uint64_t k = diff / bucket_duration;
    x = alignment - k * bucket_duration;
  } else {
    uint64_t diff = ts - alignment;
    uint64_t m0 = diff / bucket_duration + 1;
    if (m0 <= (TSSample::MAX_TIMESTAMP - alignment) / bucket_duration) {
      x = alignment + m0 * bucket_duration;
    }
  }

  return x;
}

std::vector<nonstd::span<const TSSample>> TSAggregator::SplitSamplesToBuckets(
    nonstd::span<const TSSample> samples) const {
  std::vector<nonstd::span<const TSSample>> spans;
  if (type == TSAggregatorType::NONE || samples.empty()) {
    return spans;
  }
  uint64_t start_bucket = CalculateAlignedBucketLeft(samples.front().ts);
  uint64_t end_bucket = CalculateAlignedBucketLeft(samples.back().ts);
  uint64_t bucket_count = (end_bucket - start_bucket) / bucket_duration + 1;

  spans.reserve(bucket_count);
  auto it = samples.begin();
  const auto end = samples.end();
  uint64_t bucket_left = start_bucket;
  while (it != end) {
    uint64_t bucket_right = CalculateAlignedBucketRight(bucket_left);
    auto lower = std::lower_bound(it, end, TSSample{bucket_left, 0.0});
    auto upper = std::lower_bound(lower, end, TSSample{bucket_right, 0.0});
    spans.emplace_back(lower, upper);
    it = upper;

    bucket_left = bucket_right;
  }
  return spans;
}

nonstd::span<const TSSample> TSAggregator::GetBucketByTimestamp(nonstd::span<const TSSample> samples,
                                                                uint64_t ts) const {
  if (type == TSAggregatorType::NONE || samples.empty()) {
    return {};
  }
  uint64_t start_bucket = CalculateAlignedBucketLeft(ts);
  uint64_t end_bucket = CalculateAlignedBucketRight(ts);
  auto lower = std::lower_bound(samples.begin(), samples.end(), TSSample{start_bucket, 0.0});
  auto upper = std::lower_bound(lower, samples.end(), TSSample{end_bucket, 0.0});
  return {lower, upper};
}

double TSAggregator::AggregateSamplesValue(nonstd::span<const TSSample> samples) const {
  double res = TSSample::NAN_VALUE;
  if (samples.empty()) {
    return res;
  }
  auto sample_size = static_cast<double>(samples.size());
  switch (type) {
    case TSAggregatorType::AVG:
      res = Reducer::Sum(samples) / sample_size;
      break;
    case TSAggregatorType::SUM:
      res = Reducer::Sum(samples);
      break;
    case TSAggregatorType::MIN:
      res = Reducer::Min(samples);
      break;
    case TSAggregatorType::MAX:
      res = Reducer::Max(samples);
      break;
    case TSAggregatorType::RANGE:
      res = Reducer::Range(samples);
      break;
    case TSAggregatorType::COUNT:
      res = sample_size;
      break;
    case TSAggregatorType::FIRST:
      res = samples.front().v;
      break;
    case TSAggregatorType::LAST:
      res = samples.back().v;
      break;
    case TSAggregatorType::STD_P:
      res = Reducer::StdP(samples);
      break;
    case TSAggregatorType::STD_S:
      res = Reducer::StdS(samples);
      break;
    case TSAggregatorType::VAR_P:
      res = Reducer::VarP(samples);
      break;
    case TSAggregatorType::VAR_S:
      res = Reducer::VarS(samples);
      break;
    default:
      unreachable();
  }

  return res;
}

rocksdb::Status TimeSeries::getTimeSeriesMetadata(engine::Context &ctx, const Slice &ns_key,
                                                  TimeSeriesMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisTimeSeries}, ns_key, metadata);
}

rocksdb::Status TimeSeries::createTimeSeries(engine::Context &ctx, const Slice &ns_key,
                                             TimeSeriesMetadata *metadata_out, const TSCreateOption *option) {
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTimeSeries, {"createTimeSeries"});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  *metadata_out = CreateMetadataFromOption(option ? *option : TSCreateOption{});
  std::string bytes;
  metadata_out->Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;

  if (option && !option->labels.empty()) {
    createLabelIndexInBatch(ns_key, *metadata_out, batch, option->labels);
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::getOrCreateTimeSeries(engine::Context &ctx, const Slice &ns_key,
                                                  TimeSeriesMetadata *metadata_out, const TSCreateOption *option) {
  auto s = getTimeSeriesMetadata(ctx, ns_key, metadata_out);
  if (s.ok()) {
    return s;
  }
  return createTimeSeries(ctx, ns_key, metadata_out, option);
}

rocksdb::Status TimeSeries::upsertCommon(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                                         SampleBatch &sample_batch, std::vector<std::string> *new_chunks) {
  auto all_batch_slice = sample_batch.AsSlice();

  // In the emun `TSSubkeyType`, `LABEL` is the next of `CHUNK`
  std::string chunk_upper_bound = internalKeyFromLabelKey(ns_key, metadata, "");
  std::string end_key = internalKeyFromChunkID(ns_key, metadata, TSSample::MAX_TIMESTAMP);
  std::string prefix = end_key.substr(0, end_key.size() - sizeof(uint64_t));

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(chunk_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  uint64_t chunk_count = metadata.size;

  // Get the latest chunk
  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(end_key);
  TSChunkPtr latest_chunk;
  std::string latest_chunk_key, latest_chunk_value;
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    // Create a new empty chunk if there is no chunk
    auto [chunk_ptr_, data_] = CreateEmptyOwnedTSChunk();
    latest_chunk_value = std::move(data_);
    latest_chunk = std::move(chunk_ptr_);
  } else {
    latest_chunk_key = iter->key().ToString();
    latest_chunk_value = iter->value().ToString();
    latest_chunk = CreateTSChunkFromData(latest_chunk_value);
  }

  // Filter out samples older than retention time
  sample_batch.Expire(latest_chunk->GetLastTimestamp(), metadata.retention_time);
  if (all_batch_slice.GetValidCount() == 0) {
    return rocksdb::Status::OK();
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTimeSeries);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // Get the first chunk
  auto start_key = internalKeyFromChunkID(ns_key, metadata, all_batch_slice.GetFirstTimestamp());
  iter->SeekForPrev(start_key);
  if (!iter->Valid()) {
    iter->Seek(start_key);
  } else if (!iter->key().starts_with(prefix)) {
    iter->Next();
  }

  // Process samples added to sealed chunks
  uint64_t start_ts = 0;
  uint64_t end_ts = TSSample::MAX_TIMESTAMP;
  bool is_chunk = (iter->Valid() && iter->key().starts_with(prefix));
  while (is_chunk) {
    auto cur_chunk_data = iter->value().ToString();
    auto cur_chunk_key = iter->key().ToString();
    iter->Next();
    is_chunk = (iter->Valid() && iter->key().starts_with(prefix));
    if (!is_chunk) {
      // Process last chunk
      break;
    }
    end_ts = chunkIDFromInternalKey(iter->key());

    auto chunk = CreateTSChunkFromData(cur_chunk_data);
    auto sample_slice = all_batch_slice.SliceByTimestamps(start_ts, end_ts);
    if (sample_slice.GetValidCount() == 0) {
      continue;
    }
    auto new_data_list = chunk->UpsertSampleAndSplit(sample_slice, metadata.chunk_size, false);
    for (size_t i = 0; i < new_data_list.size(); i++) {
      auto &new_data = new_data_list[i];
      auto new_chunk = CreateTSChunkFromData(new_data);
      auto new_key = internalKeyFromChunkID(ns_key, metadata, new_chunk->GetFirstTimestamp());
      // Process samples older than the first chunk, should update the key
      if (i == 0 && new_key != cur_chunk_key) {
        s = batch->Delete(cur_chunk_key);
        if (!s.ok()) return s;
      }
      s = batch->Put(new_key, new_data);
      if (!s.ok()) return s;
    }
    if (!new_data_list.empty()) {
      chunk_count += new_data_list.size() - 1;
    }
  }

  // Process samples added to latest chunk(unseal)
  auto remained_samples = all_batch_slice.SliceByTimestamps(start_ts, TSSample::MAX_TIMESTAMP, true);

  auto new_data_list = latest_chunk->UpsertSampleAndSplit(remained_samples, metadata.chunk_size, true);
  for (size_t i = 0; i < new_data_list.size(); i++) {
    auto &new_data = new_data_list[i];
    auto new_chunk = CreateTSChunkFromData(new_data);
    auto new_key = internalKeyFromChunkID(ns_key, metadata, new_chunk->GetFirstTimestamp());
    if (i == 0 && new_key != latest_chunk_key) {
      s = batch->Delete(latest_chunk_key);
      if (!s.ok()) return s;
    }
    s = batch->Put(new_key, new_data);
    if (!s.ok()) return s;
  }
  if (!new_data_list.empty()) {
    chunk_count += new_data_list.size() - (metadata.size == 0 ? 0 : 1);
  }
  if (chunk_count != metadata.size) {
    metadata.size = chunk_count;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  if (new_chunks) {
    if (new_data_list.size()) {
      *new_chunks = std::move(new_data_list);
    } else {
      *new_chunks = {std::move(latest_chunk_value)};
    }
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::rangeCommon(engine::Context &ctx, const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                        const TSRangeOption &option, std::vector<TSSample> *res, bool apply_retention) {
  if (option.end_ts < option.start_ts) {
    return rocksdb::Status::OK();
  }

  // In the emun `TSSubkeyType`, `LABEL` is the next of `CHUNK`
  std::string chunk_upper_bound = internalKeyFromLabelKey(ns_key, metadata, "");
  std::string end_key = internalKeyFromChunkID(ns_key, metadata, TSSample::MAX_TIMESTAMP);
  std::string prefix = end_key.substr(0, end_key.size() - sizeof(uint64_t));

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(chunk_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  // Get the latest chunk
  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(end_key);
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    return rocksdb::Status::OK();
  }
  auto chunk = CreateTSChunkFromData(iter->value());
  uint64_t last_timestamp = chunk->GetLastTimestamp();
  uint64_t retention_bound =
      (apply_retention && metadata.retention_time != 0 && last_timestamp > metadata.retention_time)
          ? last_timestamp - metadata.retention_time
          : 0;
  uint64_t start_timestamp = std::max(retention_bound, option.start_ts);
  uint64_t end_timestamp = std::min(last_timestamp, option.end_ts);

  // Update iterator options
  auto start_key = internalKeyFromChunkID(ns_key, metadata, start_timestamp);
  if (end_timestamp != TSSample::MAX_TIMESTAMP) {
    end_key = internalKeyFromChunkID(ns_key, metadata, end_timestamp + 1);
  }
  upper_bound = Slice(end_key);
  read_options.iterate_upper_bound = &upper_bound;
  iter = util::UniqueIterator(ctx, read_options);

  iter->SeekForPrev(start_key);
  if (!iter->Valid()) {
    iter->Seek(start_key);
  } else if (!iter->key().starts_with(prefix)) {
    iter->Next();
  }
  // Prepare to store results
  std::vector<TSSample> temp_results;
  const auto &aggregator = option.aggregator;
  bool has_aggregator = aggregator.type != TSAggregatorType::NONE;
  if (iter->Valid()) {
    if (option.count_limit != 0 && !has_aggregator) {
      temp_results.reserve(option.count_limit);
    } else {
      chunk = CreateTSChunkFromData(iter->value());
      auto range = chunk->GetLastTimestamp() - chunk->GetFirstTimestamp() + 1;
      auto estimate_chunks = std::min((end_timestamp - start_timestamp) / range, uint64_t(32));
      temp_results.reserve(estimate_chunks * metadata.chunk_size);
    }
  }
  // Get samples from chunks
  uint64_t bucket_count = 0;
  uint64_t last_bucket = 0;
  bool is_not_enough = true;
  for (; iter->Valid() && is_not_enough; iter->Next()) {
    chunk = CreateTSChunkFromData(iter->value());
    auto it = chunk->CreateIterator();
    while (it->HasNext()) {
      auto sample = it->Next().value();
      // Early termination check
      if (!has_aggregator && option.count_limit && temp_results.size() >= option.count_limit) {
        is_not_enough = false;
        break;
      }
      const bool in_time_range = sample->ts >= start_timestamp && sample->ts <= end_timestamp;
      const bool not_time_filtered = option.filter_by_ts.empty() || option.filter_by_ts.count(sample->ts);
      const bool value_in_range = !option.filter_by_value || (sample->v >= option.filter_by_value->first &&
                                                              sample->v <= option.filter_by_value->second);

      if (!in_time_range || !not_time_filtered || !value_in_range) {
        continue;
      }

      // Do checks for early termination when `count_limit` is set.
      if (has_aggregator && option.count_limit > 0) {
        const auto bucket = aggregator.CalculateAlignedBucketRight(sample->ts);
        const bool is_empty_count = (last_bucket > 0 && option.is_return_empty);
        const size_t increment = is_empty_count ? (bucket - last_bucket) / aggregator.bucket_duration : 1;
        bucket_count += increment;
        last_bucket = bucket;
        if (bucket_count > option.count_limit) {
          is_not_enough = false;
          temp_results.push_back(*sample);  // Ensure empty bucket is reported
          break;
        }
      }
      temp_results.push_back(*sample);
    }
  }

  // Process compaction logic
  *res = AggregateSamplesByRangeOption(std::move(temp_results), option);

  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::upsertDownStream(engine::Context &ctx, const Slice &ns_key,
                                             const TimeSeriesMetadata &metadata,
                                             const std::vector<std::string> &new_chunks, SampleBatch &sample_batch) {
  // If no valid written
  if (new_chunks.empty()) return rocksdb::Status::OK();
  std::vector<std::string> downstream_keys;
  std::vector<TSDownStreamMeta> downstream_metas;
  auto s = getDownStreamRules(ctx, ns_key, metadata, &downstream_keys, &downstream_metas);
  if (!s.ok()) return s;
  if (downstream_keys.empty()) return rocksdb::Status::OK();

  auto all_batch_slice = sample_batch.AsSlice();
  uint64_t new_chunk_first_ts = CreateTSChunkFromData(new_chunks[0])->GetFirstTimestamp();

  nonstd::span<const AddResult> add_results = all_batch_slice.GetAddResultSpan();
  auto samples_span = all_batch_slice.GetSampleSpan();
  std::vector<std::vector<TSSample>> all_agg_samples(downstream_metas.size());
  std::vector<std::vector<TSSample>> all_agg_samples_inc(downstream_metas.size());
  std::vector<uint64_t> last_buckets(downstream_metas.size());
  std::vector<bool> is_meta_updates(downstream_metas.size(), false);

  using AddResultType = TSChunk::AddResultType;
  struct ProcessingInfo {
    uint64_t start_ts;
    uint64_t end_ts;
    size_t sample_idx;
    std::vector<size_t> downstream_indices;
  };
  std::vector<ProcessingInfo> processing_infos;
  processing_infos.reserve(add_results.size());

  for (size_t i = 0; i < add_results.size(); i++) {
    const auto &add_result = add_results[i];
    auto sample_ts = add_result.sample.ts;
    const auto type = add_result.type;
    if (type != AddResultType::kInsert && type != AddResultType::kUpdate) {
      continue;
    }

    // Prepare  info for samples added to sealed chunks
    ProcessingInfo info;
    info.sample_idx = i;
    info.start_ts = TSSample::MAX_TIMESTAMP;
    info.end_ts = 0;

    for (size_t j = 0; j < downstream_metas.size(); j++) {
      const auto &agg = downstream_metas[j].aggregator;
      uint64_t latest_bucket_idx = downstream_metas[j].latest_bucket_idx;
      uint64_t bkt_left = agg.CalculateAlignedBucketLeft(sample_ts);

      // Skip samples with timestamps beyond the retrieval boundary
      // Boundary is defined as the later of:
      //   - New chunk start time (new_chunk_first_ts)
      //   - Latest bucket index (latest_bucket_idx)
      auto boundary = std::max(new_chunk_first_ts, latest_bucket_idx);
      if (sample_ts >= boundary) {
        continue;
      }
      // For these type, no need retrieve source samples
      if (IsIncrementalAggregatorType(agg.type)) {
        info.downstream_indices.push_back(j);
        continue;
      }
      if ((i > 0 && bkt_left == last_buckets[j])) {
        continue;
      }

      info.downstream_indices.push_back(j);
      uint64_t bkt_right = agg.CalculateAlignedBucketRight(sample_ts);
      info.start_ts = std::min(info.start_ts, bkt_left);
      info.end_ts = std::max(info.end_ts, bkt_right);
      info.end_ts = std::min(info.end_ts, boundary - 1);  // Exclusive. Boundary > 0
    }

    if (info.downstream_indices.size()) {
      processing_infos.push_back(info);
    }
  }

  // Process samples added to sealed chunks
  for (const auto &info : processing_infos) {
    const auto &add_result = add_results[info.sample_idx];
    const auto &sample = samples_span[info.sample_idx];

    TSRangeOption option;
    option.start_ts = info.start_ts;
    option.end_ts = info.end_ts;
    std::vector<TSSample> retrieve_samples;
    s = rangeCommon(ctx, ns_key, metadata, option, &retrieve_samples, false);
    if (!s.ok()) return s;

    for (size_t j : info.downstream_indices) {
      auto &meta = downstream_metas[j];
      const auto &agg = meta.aggregator;
      uint64_t bkt_left = agg.CalculateAlignedBucketLeft(add_result.sample.ts);

      if (IsIncrementalAggregatorType(agg.type)) {
        std::vector<TSSample> sample_temp = {{bkt_left, add_result.sample.v}};
        switch (agg.type) {
          case TSAggregatorType::MIN:
          case TSAggregatorType::MAX:
            sample_temp[0].v = sample.v;
            break;
          case TSAggregatorType::COUNT:
            sample_temp[0].v = 1.0;
            break;
          default:
            break;
        }
        if (bkt_left == meta.latest_bucket_idx) {
          meta.AggregateLatestBucket(sample_temp);
          is_meta_updates[j] = true;
        } else {
          all_agg_samples_inc[j].push_back({bkt_left, sample_temp[0].v});
        }
      } else {
        auto span = agg.GetBucketByTimestamp(retrieve_samples, bkt_left);
        CHECK(!span.empty());
        last_buckets[j] = bkt_left;
        if (bkt_left == meta.latest_bucket_idx) {
          meta.ResetAuxs();
          meta.AggregateLatestBucket(span);
          is_meta_updates[j] = true;
        } else {
          all_agg_samples[j].push_back({bkt_left, agg.AggregateSamplesValue(span)});
        }
      }
    }
  }

  // Process samples added to the latest chunk
  for (size_t i = 0; i < downstream_metas.size(); i++) {
    auto &agg_samples = all_agg_samples[i];
    auto &meta = downstream_metas[i];
    const auto &agg = meta.aggregator;
    if (new_chunks.size() > 1) {
      is_meta_updates[i] = true;
    }
    // For chunk except the last chunk(sealed)
    for (size_t j = 0; j < new_chunks.size() - 1; j++) {
      auto chunk = CreateTSChunkFromData(new_chunks[j]);
      auto samples = meta.AggregateMultiBuckets(chunk->GetSamplesSpan());
      agg_samples.insert(agg_samples.end(), samples.begin(), samples.end());
    }
    // For last chunk(unsealed)
    auto chunk = CreateTSChunkFromData(new_chunks.back());
    auto newest_bucket_idx = agg.CalculateAlignedBucketLeft(chunk->GetLastTimestamp());
    if (meta.latest_bucket_idx < newest_bucket_idx) {
      auto samples = meta.AggregateMultiBuckets(chunk->GetSamplesSpan(), true);
      agg_samples.insert(agg_samples.end(), samples.begin(), samples.end());
      is_meta_updates[i] = true;
    }
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTimeSeries, {"upsertDownStream"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // Write downstream metadata
  for (size_t i = 0; i < downstream_metas.size(); i++) {
    if (!is_meta_updates[i]) {
      continue;
    }
    const auto &meta = downstream_metas[i];
    const auto &key = downstream_keys[i];
    std::string bytes;
    meta.Encode(&bytes);
    s = batch->Put(key, bytes);
    if (!s.ok()) return s;
  }
  // Write aggregated samples
  for (size_t i = 0; i < downstream_metas.size(); i++) {
    const auto &ds_key = downstream_keys[i];
    auto key = downstreamKeyFromInternalKey(ds_key);
    auto ns_key = AppendNamespacePrefix(key);
    auto &agg_samples = all_agg_samples[i];
    auto &agg_samples_inc = all_agg_samples_inc[i];

    if (agg_samples.empty() && agg_samples_inc.empty()) {
      continue;
    }
    TimeSeriesMetadata metadata;
    s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
    if (!s.ok()) return s;

    if (agg_samples.size()) {
      auto sample_batch_t = SampleBatch(std::move(agg_samples), DuplicatePolicy::LAST);
      s = upsertCommon(ctx, ns_key, metadata, sample_batch_t);
      if (!s.ok()) return s;
    }

    if (agg_samples_inc.size()) {
      const auto &agg = downstream_metas[i].aggregator;
      DuplicatePolicy policy = DuplicatePolicy::LAST;
      if (agg.type == TSAggregatorType::SUM || agg.type == TSAggregatorType::COUNT) {
        policy = DuplicatePolicy::SUM;
      } else if (agg.type == TSAggregatorType::MIN) {
        policy = DuplicatePolicy::MIN;
      } else if (agg.type == TSAggregatorType::MAX) {
        policy = DuplicatePolicy::MAX;
      }
      auto sample_batch_t = SampleBatch(std::move(agg_samples_inc), policy);
      s = upsertCommon(ctx, ns_key, metadata, sample_batch_t);
      if (!s.ok()) return s;
    }
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::createLabelIndexInBatch(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                                    ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                                    const LabelKVList &labels) {
  for (auto &label : labels) {
    auto internal_key = internalKeyFromLabelKey(ns_key, metadata, label.k);
    auto s = batch->Put(internal_key, label.v);
    if (!s.ok()) return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::getLabelKVList(engine::Context &ctx, const Slice &ns_key,
                                           const TimeSeriesMetadata &metadata, LabelKVList *labels) {
  // In the emun `TSSubkeyType`, `DOWNSTREAM` is the next of `LABEL`
  std::string label_upper_bound = internalKeyFromDownstreamKey(ns_key, metadata, "");
  std::string prefix = internalKeyFromLabelKey(ns_key, metadata, "");

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(label_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  labels->clear();
  for (iter->Seek(lower_bound); iter->Valid(); iter->Next()) {
    labels->push_back({labelKeyFromInternalKey(iter->key()), iter->value().ToString()});
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::createDownStreamMetadataInBatch(engine::Context &ctx, const Slice &ns_src_key,
                                                            const Slice &dst_key,
                                                            const TimeSeriesMetadata &src_metadata,
                                                            const TSAggregator &aggregator,
                                                            ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                                            TSDownStreamMeta *ds_metadata) {
  WriteBatchLogData log_data(kRedisTimeSeries, {"createDownStreamMetadata"});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  *ds_metadata = CreateDownStreamMetaFromAgg(aggregator);
  std::string bytes;
  ds_metadata->Encode(&bytes);
  auto ikey = internalKeyFromDownstreamKey(ns_src_key, src_metadata, dst_key);
  s = batch->Put(ikey, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::getDownStreamRules(engine::Context &ctx, const Slice &ns_src_key,
                                               const TimeSeriesMetadata &src_metadata, std::vector<std::string> *keys,
                                               std::vector<TSDownStreamMeta> *metas) {
  std::string prefix = internalKeyFromDownstreamKey(ns_src_key, src_metadata, "");
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  keys->clear();
  if (metas != nullptr) {
    metas->clear();
  }
  for (iter->Seek(lower_bound); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    keys->push_back(iter->key().ToString());
    if (metas != nullptr) {
      TSDownStreamMeta meta;
      Slice slice = iter->value().ToStringView();
      meta.Decode(&slice);
      metas->push_back(meta);
    }
  }
  return rocksdb::Status::OK();
}

std::string TimeSeries::internalKeyFromChunkID(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                               uint64_t id) const {
  std::string sub_key;
  PutFixed8(&sub_key, static_cast<uint8_t>(TSSubkeyType::CHUNK));
  PutFixed64(&sub_key, id);

  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TimeSeries::internalKeyFromLabelKey(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                                Slice label_key) const {
  std::string sub_key;
  sub_key.resize(1 + label_key.size());
  auto buf = sub_key.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(TSSubkeyType::LABEL));
  EncodeBuffer(buf, label_key);

  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TimeSeries::internalKeyFromDownstreamKey(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                                     Slice downstream_key) const {
  std::string sub_key;
  sub_key.resize(1 + downstream_key.size());
  auto buf = sub_key.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(TSSubkeyType::DOWNSTREAM));
  EncodeBuffer(buf, downstream_key);

  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

uint64_t TimeSeries::chunkIDFromInternalKey(Slice internal_key) {
  auto size = internal_key.size();
  internal_key.remove_prefix(size - sizeof(uint64_t));
  return DecodeFixed64(internal_key.data());
}

std::string TimeSeries::labelKeyFromInternalKey(Slice internal_key) const {
  auto key = InternalKey(internal_key, storage_->IsSlotIdEncoded());
  auto label_key = key.GetSubKey();
  label_key.remove_prefix(sizeof(TSSubkeyType));
  return label_key.ToString();
}

std::string TimeSeries::downstreamKeyFromInternalKey(Slice internal_key) const {
  auto key = InternalKey(internal_key, storage_->IsSlotIdEncoded());
  auto ds_key = key.GetSubKey();
  ds_key.remove_prefix(sizeof(TSSubkeyType));
  return ds_key.ToString();
}

rocksdb::Status TimeSeries::Create(engine::Context &ctx, const Slice &user_key, const TSCreateOption &option) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata;
  auto s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (s.ok()) {
    return rocksdb::Status::InvalidArgument("key already exists");
  }
  return createTimeSeries(ctx, ns_key, &metadata, &option);
}

rocksdb::Status TimeSeries::Add(engine::Context &ctx, const Slice &user_key, TSSample sample,
                                const TSCreateOption &option, AddResult *res, const DuplicatePolicy *on_dup_policy) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getOrCreateTimeSeries(ctx, ns_key, &metadata, &option);
  if (!s.ok()) return s;
  auto sample_batch = SampleBatch({sample}, on_dup_policy ? *on_dup_policy : metadata.duplicate_policy);

  std::vector<std::string> new_chunks;
  s = upsertCommon(ctx, ns_key, metadata, sample_batch, &new_chunks);
  if (!s.ok()) return s;
  s = upsertDownStream(ctx, ns_key, metadata, new_chunks, sample_batch);
  if (!s.ok()) return s;
  *res = sample_batch.GetFinalResults()[0];
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::MAdd(engine::Context &ctx, const Slice &user_key, std::vector<TSSample> samples,
                                 std::vector<AddResult> *res) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }
  auto sample_batch = SampleBatch(std::move(samples), metadata.duplicate_policy);
  std::vector<std::string> new_chunks;
  s = upsertCommon(ctx, ns_key, metadata, sample_batch, &new_chunks);
  if (!s.ok()) return s;
  s = upsertDownStream(ctx, ns_key, metadata, new_chunks, sample_batch);
  if (!s.ok()) return s;
  *res = sample_batch.GetFinalResults();
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::Info(engine::Context &ctx, const Slice &user_key, TSInfoResult *res) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &res->metadata);
  if (!s.ok()) {
    return s;
  }
  auto chunk_count = res->metadata.size;
  auto &metadata = res->metadata;
  // Approximate total samples
  res->total_samples = chunk_count * res->metadata.chunk_size;
  // TODO: Estimate disk usage for the field `memoryUsage`
  res->memory_usage = 0;
  // Retrieve the first and last timestamp
  std::string chunk_upper_bound = internalKeyFromLabelKey(ns_key, metadata, "");
  std::string end_key = internalKeyFromChunkID(ns_key, metadata, TSSample::MAX_TIMESTAMP);
  std::string prefix = end_key.substr(0, end_key.size() - sizeof(uint64_t));

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(chunk_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(end_key);
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    // no chunk
    res->first_timestamp = 0;
    res->last_timestamp = 0;
  } else {
    auto chunk = CreateTSChunkFromData(iter->value());
    res->last_timestamp = chunk->GetLastTimestamp();
    // Get the first timestamp
    TSRangeOption range_option;
    range_option.count_limit = 1;
    std::vector<TSSample> samples;
    s = rangeCommon(ctx, ns_key, metadata, range_option, &samples);
    if (!s.ok()) return s;
    CHECK(samples.size() == 1);
    res->first_timestamp = samples[0].ts;
  }
  getLabelKVList(ctx, ns_key, metadata, &res->labels);

  // Retrieve downstream downstream_rules
  std::vector<std::string> downstream_keys;
  std::vector<TSDownStreamMeta> downstream_rules;
  getDownStreamRules(ctx, ns_key, metadata, &downstream_keys, &downstream_rules);
  for (size_t i = 0; i < downstream_keys.size(); i++) {
    auto key = downstreamKeyFromInternalKey(downstream_keys[i]);
    res->downstream_rules.emplace_back(std::move(key), std::move(downstream_rules[i]));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::Range(engine::Context &ctx, const Slice &user_key, const TSRangeOption &option,
                                  std::vector<TSSample> *res) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }
  s = rangeCommon(ctx, ns_key, metadata, option, res);
  return s;
}

rocksdb::Status TimeSeries::Get(engine::Context &ctx, const Slice &user_key, bool is_return_latest,
                                std::vector<TSSample> *res) {
  res->clear();
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }

  // In the emun `TSSubkeyType`, `LABEL` is the next of `CHUNK`
  std::string chunk_upper_bound = internalKeyFromLabelKey(ns_key, metadata, "");
  std::string end_key = internalKeyFromChunkID(ns_key, metadata, TSSample::MAX_TIMESTAMP);
  std::string prefix = end_key.substr(0, end_key.size() - sizeof(uint64_t));

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(chunk_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  // Get the latest chunk
  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(end_key);
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    return rocksdb::Status::OK();
  }
  auto chunk = CreateTSChunkFromData(iter->value());

  if (is_return_latest) {
    // TODO: need process `latest` option
  }
  res->push_back(chunk->GetLatestSample(0));
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::CreateRule(engine::Context &ctx, const Slice &src_key, const Slice &dst_key,
                                       const TSAggregator &aggregator, TSCreateRuleResult *res) {
  if (src_key == dst_key) {
    *res = TSCreateRuleResult::kSrcEqDst;
    return rocksdb::Status::OK();
  }
  std::string ns_src_key = AppendNamespacePrefix(src_key);
  TimeSeriesMetadata src_metadata;
  auto s = getTimeSeriesMetadata(ctx, ns_src_key, &src_metadata);
  if (!s.ok()) {
    *res = TSCreateRuleResult::kSrcNotExist;
    return rocksdb::Status::OK();
  }
  TimeSeriesMetadata dst_metadata;
  std::string ns_dst_key = AppendNamespacePrefix(dst_key);
  s = getTimeSeriesMetadata(ctx, ns_dst_key, &dst_metadata);
  if (!s.ok()) {
    *res = TSCreateRuleResult::kDstNotExist;
    return rocksdb::Status::OK();
  }

  if (src_metadata.source_key.size()) {
    *res = TSCreateRuleResult::kSrcHasSourceRule;
    return rocksdb::Status::OK();
  }
  if (dst_metadata.source_key.size()) {
    *res = TSCreateRuleResult::kDstHasSourceRule;
    return rocksdb::Status::OK();
  }
  std::vector<std::string> dst_ds_keys;
  s = getDownStreamRules(ctx, ns_dst_key, dst_metadata, &dst_ds_keys);
  if (!s.ok()) return s;
  if (dst_ds_keys.size()) {
    *res = TSCreateRuleResult::kDstHasDestRule;
    return rocksdb::Status::OK();
  }

  // Create downstream metadata
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTimeSeries);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  TSDownStreamMeta downstream_metadata;
  s = createDownStreamMetadataInBatch(ctx, ns_src_key, dst_key, src_metadata, aggregator, batch, &downstream_metadata);
  if (!s.ok()) return s;
  dst_metadata.SetSourceKey(src_key);

  std::string bytes;
  dst_metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_dst_key, bytes);
  if (!s.ok()) return s;

  *res = TSCreateRuleResult::kOK;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

}  // namespace redis
