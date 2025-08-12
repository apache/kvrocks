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

void TSDownStreamMeta::Encode(std::string *dst) const {
  PutFixed8(dst, static_cast<uint8_t>(aggregator));
  PutFixed64(dst, bucket_duration);
  PutFixed64(dst, alignment);
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

  GetFixed8(input, reinterpret_cast<uint8_t *>(&aggregator));
  GetFixed64(input, &bucket_duration);
  GetFixed64(input, &alignment);
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

  if (!option && !option->labels.empty()) {
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
    chunk_count += new_data_list.size() - 1;
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
  chunk_count += new_data_list.size() - (metadata.size == 0 ? 0 : 1);
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

}  // namespace redis
