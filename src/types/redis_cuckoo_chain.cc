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

#include <cmath>

#include "logging.h"

#include "cuckoo_filter.h"

namespace redis {

rocksdb::Status CuckooChain::getCuckooChainMetadata(engine::Context &ctx, const Slice &ns_key,
                                                    CuckooChainMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCuckooFilter}, ns_key, metadata);
}

std::string CuckooChain::getBucketKey(const Slice &ns_key, const CuckooChainMetadata &metadata,
                                      uint16_t filter_index, uint32_t bucket_index) {
  // Create a sub-key that includes both filter index and bucket index
  std::string sub_key;
  PutFixed16(&sub_key, filter_index);
  PutFixed32(&sub_key, bucket_index);

  // Create the internal key using the storage encoding
  std::string bucket_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return bucket_key;
}

rocksdb::Status CuckooChain::Reserve(engine::Context &ctx, const Slice &user_key, uint64_t capacity,
                                     uint8_t bucket_size, uint16_t max_iterations, uint8_t expansion) {
  // Validate parameters
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

  std::string ns_key = AppendNamespacePrefix(user_key);

  // Check if the key already exists
  // Read without snapshot to ensure we see any committed data
  std::string raw_value;
  rocksdb::ReadOptions read_options;  // No snapshot
  auto s = storage_->Get(ctx, read_options, metadata_cf_handle_, ns_key, &raw_value);
  if (s.ok()) {
    return rocksdb::Status::InvalidArgument("the key already exists");
  }
  if (!s.IsNotFound()) {
    return s;  // Return other errors
  }

  // Initialize metadata for the new cuckoo filter
  CuckooChainMetadata metadata;

  // Initialize metadata for the new cuckoo filter
  metadata.size = 0;
  metadata.base_capacity = capacity;
  metadata.bucket_size = bucket_size;
  metadata.max_iterations = max_iterations;
  metadata.expansion = expansion;
  metadata.n_filters = 1;
  metadata.num_deleted_items = 0;

  // Calculate the number of buckets needed for this filter
  uint32_t num_buckets = CuckooFilter::OptimalNumBuckets(capacity, bucket_size);

  info("Creating cuckoo filter with capacity={}, bucket_size={}, num_buckets={}, max_iterations={}, expansion={}",
       capacity, bucket_size, num_buckets, max_iterations, static_cast<int>(expansion));

  // Create a write batch for atomic operation
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, {"CF.RESERVE", user_key.ToString()});
  batch->PutLogData(log_data.Encode());

  // Store the metadata
  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);

  // Note: With bucket-based storage, we don't pre-allocate all buckets
  // Buckets will be created lazily on first write
  // This saves memory for sparse filters

  // Optionally, we could create the first few buckets to ensure the filter is ready
  // But for now, we'll keep it fully lazy for maximum memory efficiency

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

}  // namespace redis