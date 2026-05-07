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
#include <unordered_map>

#include "cuckoo_filter.h"
#include "logging.h"

namespace redis {

rocksdb::Status CuckooChain::getCuckooChainMetadata(engine::Context &ctx, const Slice &ns_key,
                                                    CuckooChainMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCuckooFilter}, ns_key, metadata);
}

rocksdb::Status CuckooChain::ValidateMetadata(const CuckooChainMetadata &metadata) {
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
  if (!CuckooFilter::IsCapacitySupported(metadata.base_capacity, metadata.bucket_size)) {
    return rocksdb::Status::Corruption("invalid metadata: base_capacity is too large");
  }
  return rocksdb::Status::OK();
}

std::string CuckooChain::getBucketKey(const Slice &ns_key, const CuckooChainMetadata &metadata, uint16_t filter_index,
                                      uint32_t bucket_index) {
  // Create a sub-key that includes both filter index and bucket index
  std::string sub_key;
  PutFixed16(&sub_key, filter_index);
  PutFixed32(&sub_key, bucket_index);

  // Create the internal key using the storage encoding
  std::string bucket_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return bucket_key;
}

rocksdb::Status CuckooChain::Reserve(engine::Context &ctx, const Slice &user_key, uint64_t capacity,
                                     uint8_t bucket_size, uint16_t max_iterations, uint16_t expansion) {
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

  // Calculate the number of buckets needed for this filter
  uint32_t num_buckets = 0;
  s = CuckooFilter::OptimalNumBuckets(capacity, bucket_size, &num_buckets);
  if (!s.ok()) return s;

  INFO("Creating cuckoo filter with capacity={}, bucket_size={}, num_buckets={}, max_iterations={}, expansion={}",
       capacity, bucket_size, num_buckets, max_iterations, static_cast<int>(expansion));

  // Create a write batch for atomic operation
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, std::vector<std::string>{"CF.RESERVE", user_key.ToString()});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!s.ok()) return s;

  // Note: With bucket-based storage, we don't pre-allocate all buckets
  // Buckets will be created lazily on first write
  // This saves memory for sparse filters

  // Optionally, we could create the first few buckets to ensure the filter is ready
  // But for now, we'll keep it fully lazy for maximum memory efficiency

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

// Helper function: try to find empty slot in a bucket and insert fingerprint
static bool TryInsertInBucket(std::string &bucket_data, uint8_t bucket_size, uint8_t fingerprint, size_t *slot_idx) {
  for (size_t i = 0; i < bucket_size; ++i) {
    if (static_cast<uint8_t>(bucket_data[i]) == 0) {
      bucket_data[i] = static_cast<char>(fingerprint);
      *slot_idx = i;
      return true;
    }
  }
  return false;
}

// Helper function: read bucket from storage and ensure correct size
static rocksdb::Status ReadBucket(engine::Storage *storage, engine::Context &ctx, const std::string &bucket_key,
                                  uint8_t bucket_size, std::string *bucket_data) {
  auto s = storage->Get(ctx, ctx.GetReadOptions(), bucket_key, bucket_data);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.IsNotFound()) {
    bucket_data->clear();
  }
  if (bucket_data->size() < bucket_size) {
    bucket_data->resize(bucket_size, 0);
  }
  return rocksdb::Status::OK();
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
  }
  if (!s.ok() && !s.IsNotFound()) return s;

  s = ValidateMetadata(metadata);
  if (!s.ok()) return s;

  // Calculate hash and fingerprint for the item
  uint64_t hash = CuckooFilter::Hash(item.data(), item.size());
  uint8_t fingerprint = CuckooFilter::GenerateFingerprint(hash);

  // Try to insert in each sub-filter (starting from the first/smallest one)
  // This follows RedisBloom's behavior and is more efficient
  for (uint16_t filter_idx = 0; filter_idx < metadata.n_filters; ++filter_idx) {
    uint64_t filter_capacity = 0;
    if (!CalculateFilterCapacity(metadata.base_capacity, metadata.expansion, filter_idx, &filter_capacity) ||
        !CuckooFilter::IsCapacitySupported(filter_capacity, metadata.bucket_size)) {
      return rocksdb::Status::Corruption("invalid metadata: filter capacity is too large");
    }
    uint32_t num_buckets = 0;
    s = CuckooFilter::OptimalNumBuckets(filter_capacity, metadata.bucket_size, &num_buckets);
    if (!s.ok()) return s;

    // Calculate bucket indices
    uint32_t bucket1_idx = hash % num_buckets;
    uint64_t alt_hash = CuckooFilter::GetAltHash(fingerprint, hash);
    uint32_t bucket2_idx = alt_hash % num_buckets;

    // Read both buckets using helper function
    std::string bucket1_key = getBucketKey(ns_key, metadata, filter_idx, bucket1_idx);
    std::string bucket2_key = getBucketKey(ns_key, metadata, filter_idx, bucket2_idx);

    std::string bucket1_data, bucket2_data;
    s = ReadBucket(storage_, ctx, bucket1_key, metadata.bucket_size, &bucket1_data);
    if (!s.ok()) return s;

    s = ReadBucket(storage_, ctx, bucket2_key, metadata.bucket_size, &bucket2_data);
    if (!s.ok()) return s;

    // Try simple insertion in bucket1 or bucket2
    size_t slot_idx = 0;
    std::string *target_bucket_data = nullptr;
    std::string target_bucket_key;

    if (TryInsertInBucket(bucket1_data, metadata.bucket_size, fingerprint, &slot_idx)) {
      target_bucket_data = &bucket1_data;
      target_bucket_key = bucket1_key;
    } else if (TryInsertInBucket(bucket2_data, metadata.bucket_size, fingerprint, &slot_idx)) {
      target_bucket_data = &bucket2_data;
      target_bucket_key = bucket2_key;
    }

    if (target_bucket_data != nullptr) {
      // Successfully inserted, write to storage atomically
      auto batch = storage_->GetWriteBatchBase();
      WriteBatchLogData log_data(kRedisCuckooFilter, std::vector<std::string>{"CF.ADD", user_key.ToString()});
      s = batch->PutLogData(log_data.Encode());
      if (!s.ok()) return s;
      s = batch->Put(target_bucket_key, *target_bucket_data);
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
  uint64_t filter_capacity = 0;
  if (!CalculateFilterCapacity(metadata.base_capacity, metadata.expansion, last_filter_idx, &filter_capacity) ||
      !CuckooFilter::IsCapacitySupported(filter_capacity, metadata.bucket_size)) {
    return rocksdb::Status::Corruption("invalid metadata: filter capacity is too large");
  }
  uint32_t num_buckets = 0;
  s = CuckooFilter::OptimalNumBuckets(filter_capacity, metadata.bucket_size, &num_buckets);
  if (!s.ok()) return s;

  bool inserted = false;
  std::unordered_map<std::string, std::string> modified_buckets;
  s = kickOutInsert(ctx, ns_key, metadata, last_filter_idx, num_buckets, fingerprint, hash, &inserted,
                    &modified_buckets);
  if (s.ok() && inserted) {
    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisCuckooFilter, std::vector<std::string>{"CF.ADD", user_key.ToString()});
    s = batch->PutLogData(log_data.Encode());
    if (!s.ok()) return s;

    for (const auto &entry : modified_buckets) {
      s = batch->Put(entry.first, entry.second);
      if (!s.ok()) return s;
    }

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
    INFO("CF.ADD: Expanded to {} filters", metadata.n_filters);

    // Retry insertion in the new expanded filter
    uint16_t new_filter_idx = metadata.n_filters - 1;
    uint64_t new_filter_capacity = 0;
    if (!CalculateFilterCapacity(metadata.base_capacity, metadata.expansion, new_filter_idx, &new_filter_capacity) ||
        !CuckooFilter::IsCapacitySupported(new_filter_capacity, metadata.bucket_size)) {
      return rocksdb::Status::Aborted("maximum filter capacity reached");
    }
    uint32_t new_num_buckets = 0;
    s = CuckooFilter::OptimalNumBuckets(new_filter_capacity, metadata.bucket_size, &new_num_buckets);
    if (!s.ok()) return s;

    uint32_t bucket1_idx = hash % new_num_buckets;
    std::string bucket1_key = getBucketKey(ns_key, metadata, new_filter_idx, bucket1_idx);

    // Insert into first slot of the new filter's first bucket
    std::string bucket1_data(metadata.bucket_size, 0);
    bucket1_data[0] = static_cast<char>(fingerprint);

    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisCuckooFilter, std::vector<std::string>{"CF.ADD", user_key.ToString()});
    s = batch->PutLogData(log_data.Encode());
    if (!s.ok()) return s;
    s = batch->Put(bucket1_key, bucket1_data);
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
                                           std::unordered_map<std::string, std::string> *modified_buckets) {
  *inserted = false;
  modified_buckets->clear();

  // Start from bucket1
  uint32_t current_bucket_idx = hash % num_buckets;
  uint8_t current_fp = fingerprint;
  uint32_t victim_slot = 0;

  // Try to kick out existing fingerprints (all operations in memory)
  for (uint16_t iteration = 0; iteration < metadata.max_iterations; ++iteration) {
    // Read current bucket (check modified_buckets cache first)
    std::string bucket_key = getBucketKey(ns_key, metadata, filter_index, current_bucket_idx);
    std::string bucket_data;

    auto cached = modified_buckets->find(bucket_key);
    if (cached != modified_buckets->end()) {
      bucket_data = cached->second;
    } else {
      auto s = ReadBucket(storage_, ctx, bucket_key, metadata.bucket_size, &bucket_data);
      if (!s.ok()) return s;
    }

    // Swap fingerprint with victim slot
    auto old_fp = static_cast<uint8_t>(bucket_data[victim_slot]);
    bucket_data[victim_slot] = static_cast<char>(current_fp);
    (*modified_buckets)[bucket_key] = bucket_data;
    current_fp = old_fp;

    // If kicked-out fingerprint is 0 (empty), we successfully inserted
    if (current_fp == 0) {
      *inserted = true;
      break;
    }

    // Calculate alternate bucket for the kicked-out fingerprint
    // CRITICAL FIX: Use XOR hash calculation correctly
    // We need to reconstruct the hash for the alternate bucket
    // Since h2 = h1 ^ (fp * 0x5bd1e995), and we know bucket_idx = h1 % num_buckets
    // We calculate alt_hash using the fingerprint and current bucket index
    uint64_t current_hash = current_bucket_idx;  // Approximate hash from bucket index
    uint64_t alt_hash = CuckooFilter::GetAltHash(current_fp, current_hash);
    uint32_t alt_bucket_idx = alt_hash % num_buckets;

    // Check if alternate bucket has empty slot
    std::string alt_bucket_key = getBucketKey(ns_key, metadata, filter_index, alt_bucket_idx);
    std::string alt_bucket_data;

    cached = modified_buckets->find(alt_bucket_key);
    if (cached != modified_buckets->end()) {
      alt_bucket_data = cached->second;
    } else {
      auto s = ReadBucket(storage_, ctx, alt_bucket_key, metadata.bucket_size, &alt_bucket_data);
      if (!s.ok()) return s;
    }

    size_t empty_slot = 0;
    if (TryInsertInBucket(alt_bucket_data, metadata.bucket_size, current_fp, &empty_slot)) {
      (*modified_buckets)[alt_bucket_key] = alt_bucket_data;
      *inserted = true;
      break;
    }

    // Move to alternate bucket and try next victim slot
    current_bucket_idx = alt_bucket_idx;
    victim_slot = (victim_slot + 1) % metadata.bucket_size;
  }

  return rocksdb::Status::OK();
}

}  // namespace redis
