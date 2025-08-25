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

std::vector<TSSample> AggregateSamplesByRangeOption(std::vector<TSSample> samples, const TSRangeOption &option) {
  const auto &aggregator = option.aggregator;
  std::vector<TSSample> res;
  if (aggregator.type == TSAggregatorType::NONE || samples.empty()) {
    res = std::move(samples);
    return res;
  }
  uint64_t start_bucket = aggregator.CalculateAlignedBucketLeft(samples.front().ts);
  uint64_t end_bucket = aggregator.CalculateAlignedBucketLeft(samples.back().ts);
  uint64_t bucket_count = (end_bucket - start_bucket) / aggregator.bucket_duration + 1;

  std::vector<nonstd::span<const TSSample>> spans;
  spans.reserve(bucket_count);
  auto it = samples.begin();
  const auto end = samples.end();
  uint64_t bucket_left = start_bucket;
  while (it != end) {
    uint64_t bucket_right = aggregator.CalculateAlignedBucketRight(bucket_left);
    auto lower = std::lower_bound(it, end, TSSample{bucket_left, 0.0});
    auto upper = std::lower_bound(lower, end, TSSample{bucket_right, 0.0});
    spans.emplace_back(lower, upper);
    it = upper;

    bucket_left = bucket_right;
  }

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
  bucket_left = start_bucket;
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

double TSAggregator::AggregateSamplesValue(nonstd::span<const TSSample> samples) const {
  double res = TSSample::NAN_VALUE;
  if (samples.empty()) {
    return res;
  }
  auto sample_size = static_cast<double>(samples.size());
  switch (type) {
    case TSAggregatorType::AVG: {
      res = std::accumulate(samples.begin(), samples.end(), 0.0,
                            [](double sum, const TSSample &sample) { return sum + sample.v; }) /
            sample_size;
      break;
    }
    case TSAggregatorType::SUM: {
      res = std::accumulate(samples.begin(), samples.end(), 0.0,
                            [](double sum, const TSSample &sample) { return sum + sample.v; });
      break;
    }
    case TSAggregatorType::MIN: {
      res = std::min_element(samples.begin(), samples.end(), [](const TSSample &a, const TSSample &b) {
              return a.v < b.v;
            })->v;
      break;
    }
    case TSAggregatorType::MAX: {
      res = std::max_element(samples.begin(), samples.end(), [](const TSSample &a, const TSSample &b) {
              return a.v < b.v;
            })->v;
      break;
    }
    case TSAggregatorType::RANGE: {
      auto [min_it, max_it] = std::minmax_element(samples.begin(), samples.end(),
                                                  [](const TSSample &a, const TSSample &b) { return a.v < b.v; });
      res = max_it->v - min_it->v;
      break;
    }
    case TSAggregatorType::COUNT: {
      res = sample_size;
      break;
    }
    case TSAggregatorType::FIRST: {
      res = samples.front().v;
      break;
    }
    case TSAggregatorType::LAST: {
      res = samples.back().v;
      break;
    }
    case TSAggregatorType::STD_P: {
      double mean = std::accumulate(samples.begin(), samples.end(), 0.0,
                                    [](double sum, const TSSample &sample) { return sum + sample.v; }) /
                    sample_size;
      double variance =
          std::accumulate(samples.begin(), samples.end(), 0.0,
                          [mean](double sum, const TSSample &sample) { return sum + std::pow(sample.v - mean, 2); }) /
          sample_size;
      res = std::sqrt(variance);
      break;
    }
    case TSAggregatorType::STD_S: {
      if (samples.size() <= 1) {
        res = 0.0;
        break;
      }
      double mean = std::accumulate(samples.begin(), samples.end(), 0.0,
                                    [](double sum, const TSSample &sample) { return sum + sample.v; }) /
                    sample_size;
      double variance =
          std::accumulate(samples.begin(), samples.end(), 0.0,
                          [mean](double sum, const TSSample &sample) { return sum + std::pow(sample.v - mean, 2); }) /
          (sample_size - 1.0);
      res = std::sqrt(variance);
      break;
    }
    case TSAggregatorType::VAR_P: {
      double mean = std::accumulate(samples.begin(), samples.end(), 0.0,
                                    [](double sum, const TSSample &sample) { return sum + sample.v; }) /
                    sample_size;
      res = std::accumulate(samples.begin(), samples.end(), 0.0,
                            [mean](double sum, const TSSample &sample) { return sum + std::pow(sample.v - mean, 2); }) /
            sample_size;
      break;
    }
    case TSAggregatorType::VAR_S: {
      if (samples.size() <= 1) {
        res = 0.0;
        break;
      }
      double mean = std::accumulate(samples.begin(), samples.end(), 0.0,
                                    [](double sum, const TSSample &sample) { return sum + sample.v; }) /
                    sample_size;
      res = std::accumulate(samples.begin(), samples.end(), 0.0,
                            [mean](double sum, const TSSample &sample) { return sum + std::pow(sample.v - mean, 2); }) /
            (sample_size - 1.0);
      break;
    }
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
                                         SampleBatch &sample_batch) {
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
                                const TSCreateOption &option, AddResultWithTS *res,
                                const DuplicatePolicy *on_dup_policy) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getOrCreateTimeSeries(ctx, ns_key, &metadata, &option);
  if (!s.ok()) return s;
  auto sample_batch = SampleBatch({sample}, on_dup_policy ? *on_dup_policy : metadata.duplicate_policy);

  s = upsertCommon(ctx, ns_key, metadata, sample_batch);
  if (!s.ok()) {
    return s;
  }
  *res = sample_batch.GetFinalResults()[0];
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::MAdd(engine::Context &ctx, const Slice &user_key, std::vector<TSSample> samples,
                                 std::vector<AddResultWithTS> *res) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }
  auto sample_batch = SampleBatch(std::move(samples), metadata.duplicate_policy);
  s = upsertCommon(ctx, ns_key, metadata, sample_batch);
  if (!s.ok()) {
    return s;
  }
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
    uint64_t retention_bound = (metadata.retention_time > 0 && res->last_timestamp > metadata.retention_time)
                                   ? res->last_timestamp - metadata.retention_time
                                   : 0;
    auto bound_key = internalKeyFromChunkID(ns_key, metadata, retention_bound);
    iter->SeekForPrev(bound_key);
    if (!iter->Valid() || !iter->key().starts_with(prefix)) {
      if (!iter->Valid()) {
        iter->Seek(bound_key);
      } else {
        iter->Next();
      }
      chunk = CreateTSChunkFromData(iter->value());
      res->first_timestamp = chunk->GetFirstTimestamp();
    } else {
      chunk = CreateTSChunkFromData(iter->value());
      auto chunk_it = chunk->CreateIterator();
      while (chunk_it->HasNext()) {
        auto sample = chunk_it->Next().value();
        if (sample->ts >= retention_bound) {
          res->first_timestamp = sample->ts;
          break;
        }
      }
    }
  }
  getLabelKVList(ctx, ns_key, metadata, &res->labels);
  // TODO: Retrieve downstream downstream_rules

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
  uint64_t retention_bound = (metadata.retention_time == 0 || last_timestamp <= metadata.retention_time)
                                 ? 0
                                 : last_timestamp - metadata.retention_time;
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

}  // namespace redis
