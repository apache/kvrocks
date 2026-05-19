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

#include <limits>

#include "cuckoo_filter.h"
#include "cuckoo_filter_page.h"
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
  if (!CuckooFilter::IsCapacitySupported(metadata.base_capacity, metadata.bucket_size)) {
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
  if (!CuckooFilter::IsCapacitySupported(capacity, bucket_size)) {
    return rocksdb::Status::InvalidArgument("capacity is too large");
  }

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
  metadata.expansion = expansion;
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

static bool CalculateFilterCapacity(uint64_t base_capacity, uint16_t expansion, uint16_t filter_index,
                                    uint64_t *filter_capacity) {
  uint64_t capacity = base_capacity;
  for (uint16_t i = 0; i < filter_index; ++i) {
    if (expansion != 0 && capacity > std::numeric_limits<uint64_t>::max() / expansion) return false;
    capacity *= expansion;
  }
  *filter_capacity = capacity;
  return true;
}

static rocksdb::Status GetFilterNumBuckets(const CuckooChainMetadata &metadata, uint16_t filter_index,
                                           uint32_t *num_buckets) {
  uint64_t filter_capacity = 0;
  if (!CalculateFilterCapacity(metadata.base_capacity, metadata.expansion, filter_index, &filter_capacity) ||
      !CuckooFilter::IsCapacitySupported(filter_capacity, metadata.bucket_size)) {
    return rocksdb::Status::Corruption("invalid metadata: filter capacity is too large");
  }
  return CuckooFilter::CalculateRequiredBuckets(filter_capacity, metadata.bucket_size, num_buckets);
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
  }
  if (!s.ok() && !s.IsNotFound()) return s;

  s = validateMetadata(metadata);
  if (!s.ok()) return s;

  // Calculate hash and fingerprint for the item
  uint64_t hash = CuckooFilter::Hash(item.data(), item.size());
  uint8_t fingerprint = CuckooFilter::GenerateFingerprint(hash);

  // RedisBloom prioritizes the newest sub-filter to avoid repeatedly probing older, fuller filters.
  for (int filter_idx = static_cast<int>(metadata.n_filters) - 1; filter_idx >= 0; --filter_idx) {
    auto current_filter_idx = static_cast<uint16_t>(filter_idx);
    uint32_t num_buckets = 0;
    s = GetFilterNumBuckets(metadata, current_filter_idx, &num_buckets);
    if (!s.ok()) return s;

    // Calculate bucket indices
    uint32_t bucket1_idx = hash % num_buckets;
    uint64_t alt_hash = CuckooFilter::GetAltHash(fingerprint, hash);
    uint32_t bucket2_idx = alt_hash % num_buckets;

    CuckooPageSet pages(storage_, ctx, ns_key, metadata, storage_->IsSlotIdEncoded());
    bool inserted = false;
    s = pages.TryInsertInCandidateBuckets(current_filter_idx, num_buckets, bucket1_idx, bucket2_idx, fingerprint,
                                          &inserted);
    if (!s.ok()) return s;

    if (inserted) {
      // Successfully inserted, write to storage atomically
      auto batch = storage_->GetWriteBatchBase();
      WriteBatchLogData log_data(kRedisCuckooFilter, std::vector<std::string>{"add", user_key.ToString()});
      s = batch->PutLogData(log_data.Encode());
      if (!s.ok()) return s;
      s = pages.WriteBackDirtyPages(batch.Get());
      if (!s.ok()) return s;

      metadata.size++;
      std::string metadata_bytes;
      metadata.Encode(&metadata_bytes);
      s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
      if (!s.ok()) return s;

      s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
      if (!s.ok()) return s;

      *added = true;
      return rocksdb::Status::OK();
    }
  }

  // No space found in any filter, try kick-out on the last filter
  uint16_t last_filter_idx = metadata.n_filters - 1;
  uint32_t num_buckets = 0;
  s = GetFilterNumBuckets(metadata, last_filter_idx, &num_buckets);
  if (!s.ok()) return s;

  bool inserted = false;
  auto batch = storage_->GetWriteBatchBase();
  s = kickOutInsert(ctx, ns_key, metadata, last_filter_idx, num_buckets, fingerprint, hash, &inserted, batch.Get());
  if (s.ok() && inserted) {
    WriteBatchLogData log_data(kRedisCuckooFilter, std::vector<std::string>{"add", user_key.ToString()});
    s = batch->PutLogData(log_data.Encode());
    if (!s.ok()) return s;

    metadata.size++;
    std::string metadata_bytes;
    metadata.Encode(&metadata_bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
    if (!s.ok()) return s;

    s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) return s;

    *added = true;
    return rocksdb::Status::OK();
  }

  // Kick-out failed, try to expand if allowed
  if (metadata.expansion > 0) {
    if (metadata.n_filters >= UINT16_MAX) return rocksdb::Status::Aborted("maximum number of filters reached");

    metadata.n_filters++;
    INFO("add expanded to {} filters", metadata.n_filters);

    // Retry insertion in the new expanded filter
    uint16_t new_filter_idx = metadata.n_filters - 1;
    uint32_t new_num_buckets = 0;
    s = GetFilterNumBuckets(metadata, new_filter_idx, &new_num_buckets);
    if (s.IsCorruption()) {
      return rocksdb::Status::Aborted("maximum filter capacity reached");
    }
    if (!s.ok()) return s;

    uint32_t bucket1_idx = hash % new_num_buckets;
    CuckooPageSet pages(storage_, ctx, ns_key, metadata, storage_->IsSlotIdEncoded());
    s = pages.TryInsertInBucket(new_filter_idx, new_num_buckets, bucket1_idx, fingerprint, &inserted);
    if (!s.ok()) return s;
    if (!inserted) return rocksdb::Status::Corruption("failed to insert into new cuckoo filter");

    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisCuckooFilter, std::vector<std::string>{"add", user_key.ToString()});
    s = batch->PutLogData(log_data.Encode());
    if (!s.ok()) return s;
    s = pages.WriteBackDirtyPages(batch.Get());
    if (!s.ok()) return s;

    metadata.size++;
    std::string metadata_bytes;
    metadata.Encode(&metadata_bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
    if (!s.ok()) return s;

    s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) return s;

    *added = true;
    return rocksdb::Status::OK();
  }

  // No expansion allowed and filter is full
  *added = false;
  return rocksdb::Status::Aborted("filter is full");
}

rocksdb::Status CuckooChain::kickOutInsert(engine::Context &ctx, const Slice &ns_key,
                                           const CuckooChainMetadata &metadata, uint16_t filter_index,
                                           uint32_t num_buckets, uint8_t fingerprint, uint64_t hash, bool *inserted,
                                           rocksdb::WriteBatchBase *batch) {
  *inserted = false;
  CuckooPageSet pages(storage_, ctx, ns_key, metadata, storage_->IsSlotIdEncoded());

  // Start from bucket1
  uint32_t current_bucket_idx = hash % num_buckets;
  uint8_t current_fp = fingerprint;
  uint32_t victim_slot = 0;

  // Try to kick out existing fingerprints (all operations in memory)
  for (uint16_t iteration = 0; iteration < metadata.max_iterations; ++iteration) {
    // Swap fingerprint with victim slot
    uint8_t old_fp = 0;
    auto s = pages.GetBucketSlot(filter_index, num_buckets, current_bucket_idx, victim_slot, &old_fp);
    if (!s.ok()) return s;
    s = pages.SetBucketSlot(filter_index, num_buckets, current_bucket_idx, victim_slot, current_fp);
    if (!s.ok()) return s;
    current_fp = old_fp;

    // If kicked-out fingerprint is 0 (empty), we successfully inserted
    if (current_fp == 0) {
      *inserted = true;
      break;
    }

    uint32_t alt_bucket_idx = CuckooFilter::GetAltBucketIndex(current_bucket_idx, current_fp, num_buckets);

    bool inserted_in_alt_bucket = false;
    s = pages.TryInsertInBucket(filter_index, num_buckets, alt_bucket_idx, current_fp, &inserted_in_alt_bucket);
    if (!s.ok()) return s;
    if (inserted_in_alt_bucket) {
      *inserted = true;
      break;
    }

    // Move to alternate bucket and try next victim slot
    current_bucket_idx = alt_bucket_idx;
    victim_slot = (victim_slot + 1) % metadata.bucket_size;
  }

  if (*inserted) return pages.WriteBackDirtyPages(batch);
  return rocksdb::Status::OK();
}

}  // namespace redis
