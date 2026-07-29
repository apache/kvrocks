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

#include "redis_cuckoo_chain.h"

#include "cuckoo_filter.h"
#include "cuckoo_filter_sub_filter.h"
#include "logging.h"

namespace redis {

rocksdb::Status CuckooChain::getCuckooChainMetadata(engine::Context &ctx, const Slice &ns_key,
                                                    CuckooChainMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCuckooFilter}, ns_key, metadata);
}

rocksdb::Status CuckooChain::validateMetadata(const CuckooChainMetadata &metadata) {
  if (metadata.n_filters == 0) {
    return rocksdb::Status::Corruption("invalid metadata: n_filters is 0");
  }
  if (metadata.base_capacity == 0) {
    return rocksdb::Status::Corruption("invalid metadata: base_capacity is 0");
  }
  if (metadata.bucket_size == 0) {
    return rocksdb::Status::Corruption("invalid metadata: bucket_size is 0");
  }
  if (metadata.max_iterations == 0) {
    return rocksdb::Status::Corruption("invalid metadata: max_iterations is 0");
  }
  if (metadata.page_size < metadata.bucket_size) {
    return rocksdb::Status::Corruption("invalid metadata: page_size is smaller than bucket_size");
  }
  if (!CuckooFilterHelper::IsCapacitySupported(metadata.base_capacity, metadata.bucket_size)) {
    return rocksdb::Status::Corruption("invalid metadata: base_capacity is too large");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooChain::Reserve(engine::Context &ctx, const Slice &user_key, uint64_t capacity,
                                     uint8_t bucket_size, uint16_t max_iterations, uint16_t expansion,
                                     uint32_t page_size) {
  if (capacity == 0) {
    return rocksdb::Status::InvalidArgument("capacity must be larger than 0");
  }

  // RedisBloom requires minimum capacity to ensure at least one bucket can be created
  // With load factor 0.955, capacity=1 and bucket_size=4 results in 0 buckets
  if (capacity < 2) {
    return rocksdb::Status::InvalidArgument("capacity must be at least 2");
  }

  if (bucket_size == 0 || bucket_size > 255) {
    return rocksdb::Status::InvalidArgument("bucket_size must be between 1 and 255");
  }

  if (max_iterations == 0) {
    return rocksdb::Status::InvalidArgument("max_iterations must be larger than 0");
  }
  if (page_size == 0) {
    return rocksdb::Status::InvalidArgument("page_size must be larger than 0");
  }
  if (page_size < bucket_size) {
    return rocksdb::Status::InvalidArgument("page_size must be at least bucket_size");
  }
  if (expansion > kCFMaxExpansion) {
    return rocksdb::Status::InvalidArgument("expansion must be between 0 and 32768");
  }
  if (!CuckooFilterHelper::IsCapacitySupported(capacity, bucket_size)) {
    return rocksdb::Status::InvalidArgument("capacity is too large");
  }
  uint16_t normalized_expansion = CuckooFilterHelper::NormalizeExpansion(expansion);

  std::string ns_key = AppendNamespacePrefix(user_key);

  CuckooChainMetadata existing_metadata;
  auto s = getCuckooChainMetadata(ctx, ns_key, &existing_metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (!s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument("the key already exists");
  }

  CuckooChainMetadata metadata;

  metadata.size = 0;
  metadata.base_capacity = capacity;
  metadata.bucket_size = bucket_size;
  metadata.max_iterations = max_iterations;
  metadata.expansion = normalized_expansion;
  metadata.n_filters = 1;
  metadata.num_deleted_items = 0;
  metadata.page_size = page_size;

  // Create a write batch for atomic operation
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, std::vector<std::string>{"reserve", user_key.ToString()});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!s.ok()) return s;

  // Pages are created lazily on first write. Reserve only persists metadata so sparse filters don't preallocate page
  // values that may never be used.

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CuckooChain::Add(engine::Context &ctx, const Slice &user_key, const Slice &item, bool *added) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  CuckooChainMetadata metadata(false);
  auto s = getCuckooChainMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) {
    // RedisBloom CF.ADD auto-creates the filter when the key does not exist:
    // https://redis.io/docs/latest/commands/cf.add/
    metadata = CuckooChainMetadata();
    metadata.size = 0;
    metadata.base_capacity = kCFDefaultCapacity;
    metadata.bucket_size = kCFDefaultBucketSize;
    metadata.max_iterations = kCFDefaultMaxIterations;
    metadata.expansion = kCFDefaultExpansion;
    metadata.n_filters = 1;
    metadata.num_deleted_items = 0;
    metadata.page_size = kCuckooFilterDefaultPageSize;
  } else if (!s.ok()) {
    return s;
  }

  s = validateMetadata(metadata);
  if (!s.ok()) return s;

  // Calculate hash and fingerprint for the item
  uint64_t hash = CuckooFilterHelper::Hash(item.data(), item.size());
  uint8_t fingerprint = CuckooFilterHelper::GenerateFingerprint(hash);

  bool inserted = false;
  s = tryCuckooInsert(ctx, user_key, ns_key, &metadata, hash, fingerprint, &inserted);
  if (!s.ok()) return s;
  if (inserted) {
    *added = true;
    return rocksdb::Status::OK();
  }

  s = tryCuckooKickOut(ctx, user_key, ns_key, &metadata, hash, fingerprint, &inserted);
  if (!s.ok()) return s;
  if (inserted) {
    *added = true;
    return rocksdb::Status::OK();
  }

  s = expandAndInsertCuckooChain(ctx, user_key, ns_key, &metadata, hash, fingerprint, &inserted);
  if (!s.ok()) return s;
  if (inserted) {
    *added = true;
    return rocksdb::Status::OK();
  }

  // No expansion allowed and filter is full
  *added = false;
  return rocksdb::Status::Aborted("filter is full");
}

rocksdb::Status CuckooChain::tryCuckooInsert(engine::Context &ctx, const Slice &user_key, const std::string &ns_key,
                                             CuckooChainMetadata *metadata, uint64_t hash, uint8_t fingerprint,
                                             bool *inserted) {
  *inserted = false;

  // RedisBloom prioritizes the newest sub-filter to avoid repeatedly probing older, fuller filters.
  for (int filter_idx = static_cast<int>(metadata->n_filters) - 1; filter_idx >= 0; --filter_idx) {
    auto current_filter_idx = static_cast<uint16_t>(filter_idx);
    uint32_t num_buckets = 0;
    auto s = CuckooFilterHelper::GetFilterNumBuckets(metadata->base_capacity, metadata->expansion,
                                                     metadata->bucket_size, current_filter_idx, &num_buckets);
    if (!s.ok()) return s;

    CuckooSubFilter sub_filter(storage_, ctx, ns_key, storage_->IsSlotIdEncoded(), metadata->version,
                               metadata->bucket_size, metadata->page_size, current_filter_idx, num_buckets);
    bool current_inserted = false;
    s = sub_filter.TryInsert(hash, fingerprint, &current_inserted);
    if (!s.ok()) return s;

    if (current_inserted) {
      s = commitSubFilterAndMetadata(ctx, user_key, ns_key, metadata, &sub_filter);
      if (!s.ok()) return s;
      *inserted = true;
      return rocksdb::Status::OK();
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status CuckooChain::tryCuckooKickOut(engine::Context &ctx, const Slice &user_key, const std::string &ns_key,
                                              CuckooChainMetadata *metadata, uint64_t hash, uint8_t fingerprint,
                                              bool *inserted) {
  *inserted = false;

  // No space found in any filter, try kick-out on the last filter
  uint16_t last_filter_idx = metadata->n_filters - 1;
  uint32_t num_buckets = 0;
  auto s = CuckooFilterHelper::GetFilterNumBuckets(metadata->base_capacity, metadata->expansion, metadata->bucket_size,
                                                   last_filter_idx, &num_buckets);
  if (!s.ok()) return s;

  CuckooSubFilter last_filter(storage_, ctx, ns_key, storage_->IsSlotIdEncoded(), metadata->version,
                              metadata->bucket_size, metadata->page_size, last_filter_idx, num_buckets);
  bool kickout_inserted = false;
  s = last_filter.TryKickOutInsert(hash, fingerprint, metadata->max_iterations, &kickout_inserted);
  if (!s.ok()) return s;
  if (kickout_inserted) {
    s = commitSubFilterAndMetadata(ctx, user_key, ns_key, metadata, &last_filter);
    if (!s.ok()) return s;
    *inserted = true;
    return rocksdb::Status::OK();
  }

  return rocksdb::Status::OK();
}

rocksdb::Status CuckooChain::expandAndInsertCuckooChain(engine::Context &ctx, const Slice &user_key,
                                                        const std::string &ns_key, CuckooChainMetadata *metadata,
                                                        uint64_t hash, uint8_t fingerprint, bool *inserted) {
  *inserted = false;

  // Kick-out failed, try to expand if allowed
  if (metadata->expansion == 0) return rocksdb::Status::OK();

  if (metadata->n_filters >= UINT16_MAX) return rocksdb::Status::Aborted("maximum number of filters reached");

  // Retry insertion in the new expanded filter
  uint16_t new_filter_idx = metadata->n_filters;
  uint32_t new_num_buckets = 0;
  auto s = CuckooFilterHelper::GetFilterNumBuckets(metadata->base_capacity, metadata->expansion, metadata->bucket_size,
                                                   new_filter_idx, &new_num_buckets);
  if (s.IsCorruption()) {
    return rocksdb::Status::Aborted("maximum filter capacity reached");
  }
  if (!s.ok()) return s;

  CuckooSubFilter new_filter(storage_, ctx, ns_key, storage_->IsSlotIdEncoded(), metadata->version,
                             metadata->bucket_size, metadata->page_size, new_filter_idx, new_num_buckets);
  bool new_filter_inserted = false;
  s = new_filter.TryInsert(hash, fingerprint, &new_filter_inserted);
  if (!s.ok()) return s;
  if (!new_filter_inserted) return rocksdb::Status::Corruption("failed to insert into new cuckoo filter");

  metadata->n_filters++;
  s = commitSubFilterAndMetadata(ctx, user_key, ns_key, metadata, &new_filter);
  if (!s.ok()) return s;

  *inserted = true;
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooChain::commitSubFilterAndMetadata(engine::Context &ctx, const Slice &user_key,
                                                        const std::string &ns_key, CuckooChainMetadata *metadata,
                                                        CuckooSubFilter *sub_filter) {
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, std::vector<std::string>{"add", user_key.ToString()});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  s = sub_filter->WriteToBatch(batch.Get());
  if (!s.ok()) return s;

  metadata->size++;
  std::string metadata_bytes;
  metadata->Encode(&metadata_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

}  // namespace redis
