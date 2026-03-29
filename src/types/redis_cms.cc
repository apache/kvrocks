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

#include "redis_cms.h"

#include <algorithm>
#include <cmath>

#include "vendor/murmurhash2.h"

namespace redis {

// ============================================================================
// Private helper methods
// ============================================================================

rocksdb::Status CMS::getCMSMetadata(engine::Context &ctx, const Slice &ns_key, CMSMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCMS}, ns_key, metadata);
}

std::string CMS::getBucketKey(const Slice &ns_key, uint64_t version, uint32_t bucket_id) {
  std::string sub_key;
  PutFixed32(&sub_key, bucket_id);
  return InternalKey(ns_key, sub_key, version, storage_->IsSlotIdEncoded()).Encode();
}

uint64_t CMS::hashItem(const Slice &item, uint32_t layer) {
  // Use MurmurHash64 with layer as seed
  // This ensures different hash functions for each layer
  return HllMurMurHash64A(item.data(), static_cast<int>(item.size()), layer);
}

uint32_t CMS::getCol(const Slice &item, uint32_t layer, uint32_t width) {
  uint64_t hash = hashItem(item, layer);
  return static_cast<uint32_t>(hash % width);
}

std::pair<uint32_t, uint32_t> CMS::calcDimFromProb(double error_rate, double probability) {
  // Formula from RedisBloom implementation:
  //   width = ceil(2 / error_rate)
  //   depth = ceil(log10(probability) / log10(0.5))
  //
  // Note: RedisBloom uses simplified approximations of the CMS paper formulas.
  // This differs from the theoretical formulas (width = e/error, depth = ln(1/probability)).
  //
  // Per Redis documentation (https://redis.io/docs/latest/commands/cms.initbyprob/):
  //   - error: Estimate size of error, as a percent of total counted items
  //   - probability: The desired probability for inflated count (i.e., the probability
  //                   that the estimate exceeds the true count by more than error_rate).
  auto width = static_cast<uint32_t>(std::ceil(2.0 / error_rate));
  auto depth = static_cast<uint32_t>(std::ceil(std::log10(probability) / std::log10(0.5)));

  // Clamp to allowed range
  width = std::max(1u, std::min(width, kCMSMaxWidth));
  depth = std::max(1u, std::min(depth, kCMSMaxDepth));

  return {width, depth};
}

// ============================================================================
// Public API methods
// ============================================================================

/// CMS.INITBYDIM - Initialize CMS with given dimensions
///
/// Redis command: CMS.INITBYDIM key width depth
/// Documentation: https://redis.io/docs/latest/commands/cms.initbydim/
///
/// Parameters:
///   - key: The name of the sketch
///   - width: Number of counters in each array. Reduces the error size.
///   - depth: Number of counter-arrays. Reduces the probability for an error.
///
/// Time complexity: O(1)
/// Returns: OK on success
///
/// Note: Buckets are lazily initialized (not pre-allocated). Missing buckets
/// are treated as 0, reducing write amplification on CMS creation.
rocksdb::Status CMS::InitByDim(engine::Context &ctx, const Slice &key, uint32_t width, uint32_t depth) {
  if (width == 0) {
    return rocksdb::Status::InvalidArgument("width must be positive");
  }
  if (depth == 0) {
    return rocksdb::Status::InvalidArgument("depth must be positive");
  }
  if (width > kCMSMaxWidth) {
    return rocksdb::Status::InvalidArgument("width exceeds maximum limit (" + std::to_string(kCMSMaxWidth) + ")");
  }
  if (depth > kCMSMaxDepth) {
    return rocksdb::Status::InvalidArgument("depth exceeds maximum limit (" + std::to_string(kCMSMaxDepth) + ")");
  }

  // Check total size
  uint64_t total_size = static_cast<uint64_t>(width) * depth * 4;
  if (total_size > kCMSMaxSize) {
    return rocksdb::Status::InvalidArgument("matrix size exceeds maximum limit (max " +
                                            std::to_string(kCMSMaxSize / 1024 / 1024) + "MB)");
  }

  std::string ns_key = ComposeNamespaceKey(namespace_, key, storage_->IsSlotIdEncoded());

  // Check if key already exists
  CMSMetadata existing_metadata;
  rocksdb::Status s = getCMSMetadata(ctx, ns_key, &existing_metadata);
  if (!s.IsNotFound()) {
    if (s.ok()) {
      return rocksdb::Status::InvalidArgument("key already exists");
    }
    return s;
  }

  // Create new CMS metadata
  CMSMetadata metadata;
  metadata.width = width;
  metadata.depth = depth;
  metadata.total_count = 0;
  metadata.storage_mode = CMSMetadata::StorageMode::PER_BUCKET;

  // Write metadata and initialize all buckets to 0
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCMS, {"InitByDim"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // Write metadata
  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!s.ok()) return s;

  // Note: Buckets are not pre-initialized. Missing buckets are treated as 0.
  // This lazy initialization reduces write amplification on CMS creation.
  // Query and IncrBy correctly handle missing buckets by treating them as 0.

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

/// CMS.INITBYPROB - Initialize CMS with given error rate and probability
///
/// Redis command: CMS.INITBYPROB key error probability
/// Documentation: https://redis.io/docs/latest/commands/cms.initbyprob/
///
/// Parameters:
///   - key: The name of the sketch
///   - error: Estimate size of error, as a percent of total counted items
///   - probability: The desired probability for inflated count (failure probability)
///                  For example, for 0.1% failure rate, set probability = 0.001
///
/// Time complexity: O(1)
/// Returns: OK on success
///
/// Formula (from RedisBloom):
///   width = ceil(2 / error)
///   depth = ceil(log10(probability) / log10(0.5))
rocksdb::Status CMS::InitByProb(engine::Context &ctx, const Slice &key, double error_rate, double probability) {
  if (error_rate <= 0 || error_rate >= 1) {
    return rocksdb::Status::InvalidArgument("error rate must be between 0 and 1");
  }
  if (probability <= 0 || probability >= 1) {
    return rocksdb::Status::InvalidArgument("probability must be between 0 and 1");
  }

  auto [width, depth] = calcDimFromProb(error_rate, probability);
  return InitByDim(ctx, key, width, depth);
}

/// CMS.INCRBY - Increment counters for given items
///
/// Redis command: CMS.INCRBY key item increment [item increment ...]
/// Documentation: https://redis.io/docs/latest/commands/cms.incrby/
///
/// Parameters:
///   - key: The name of the sketch
///   - item: The item to increment
///   - increment: Amount to increment (must be non-negative)
///
/// Time complexity: O(depth) for each item
/// Returns: Array of estimated counts for each item after increment
///
/// Overflow behavior: Counters saturate at UINT32_MAX (~4.3 billion).
/// This matches RedisBloom behavior (silent saturation, no error).
rocksdb::Status CMS::IncrBy(engine::Context &ctx, const Slice &key,
                            const std::vector<std::pair<std::string, int64_t>> &items, std::vector<uint64_t> *counts) {
  if (items.empty()) {
    return rocksdb::Status::InvalidArgument("no items provided");
  }

  std::string ns_key = ComposeNamespaceKey(namespace_, key, storage_->IsSlotIdEncoded());

  CMSMetadata metadata;
  rocksdb::Status s = getCMSMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  counts->resize(items.size(), 0);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCMS, {"IncrBy"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  uint64_t total_increment = 0;

  for (size_t i = 0; i < items.size(); ++i) {
    const auto &[item, increment] = items[i];
    Slice item_slice(item);

    if (increment < 0) {
      return rocksdb::Status::InvalidArgument("increment must be non-negative");
    }
    if (increment > UINT32_MAX) {
      return rocksdb::Status::InvalidArgument("increment exceeds maximum value");
    }

    uint64_t min_count = UINT64_MAX;
    std::vector<std::pair<uint32_t, uint32_t>> bucket_updates;  // (bucket_id, new_count)

    // Read and update depth buckets (one per layer) for this item
    for (uint32_t layer = 0; layer < metadata.depth; ++layer) {
      uint32_t col = getCol(item_slice, layer, metadata.width);
      uint32_t bucket_id = layer * metadata.width + col;
      std::string bucket_key = getBucketKey(ns_key, metadata.version, bucket_id);

      // Read current count
      std::string count_str;
      s = storage_->Get(ctx, ctx.GetReadOptions(), bucket_key, &count_str);
      uint32_t current_count = 0;
      if (s.ok() && count_str.size() >= 4) {
        Slice count_slice(count_str);
        GetFixed32(&count_slice, &current_count);
      } else if (!s.IsNotFound()) {
        return s;
      }

      // Check for overflow
      auto inc_val = static_cast<uint32_t>(increment);
      uint32_t new_count = 0;
      if (__builtin_add_overflow(current_count, inc_val, &new_count)) {
        new_count = UINT32_MAX;  // Saturate at max value
      }
      bucket_updates.emplace_back(bucket_id, new_count);

      if (new_count < min_count) {
        min_count = new_count;
      }
    }

    // Write all updated buckets
    for (const auto &[bucket_id, new_count] : bucket_updates) {
      std::string bucket_key = getBucketKey(ns_key, metadata.version, bucket_id);
      std::string count_value;
      PutFixed32(&count_value, new_count);
      s = batch->Put(bucket_key, count_value);
      if (!s.ok()) return s;
    }

    (*counts)[i] = min_count;

    // Check total_increment overflow
    if (__builtin_add_overflow(total_increment, static_cast<uint64_t>(increment), &total_increment)) {
      total_increment = UINT64_MAX;  // Saturate at max value
    }
  }

  // Update metadata: increment total_count with overflow check
  uint64_t new_total = 0;
  if (__builtin_add_overflow(metadata.total_count, total_increment, &new_total)) {
    metadata.total_count = UINT64_MAX;
  } else {
    metadata.total_count = new_total;
  }
  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

/// CMS.QUERY - Query estimated counts for given items
///
/// Redis command: CMS.QUERY key item [item ...]
/// Documentation: https://redis.io/docs/latest/commands/cms.query/
///
/// Parameters:
///   - key: The name of the sketch
///   - item: One or more items to query
///
/// Time complexity: O(depth) for each item
/// Returns: Array of estimated counts (minimum count across all layers)
///
/// Note: CMS never underestimates the true count (returns count >= actual).
rocksdb::Status CMS::Query(engine::Context &ctx, const Slice &key, const std::vector<std::string> &items,
                           std::vector<uint64_t> *counts) {
  if (items.empty()) {
    return rocksdb::Status::InvalidArgument("no items provided");
  }

  std::string ns_key = ComposeNamespaceKey(namespace_, key, storage_->IsSlotIdEncoded());

  CMSMetadata metadata;
  rocksdb::Status s = getCMSMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  counts->resize(items.size(), 0);

  for (size_t i = 0; i < items.size(); ++i) {
    Slice item_slice(items[i]);
    uint64_t min_count = UINT64_MAX;

    for (uint32_t layer = 0; layer < metadata.depth; ++layer) {
      uint32_t col = getCol(item_slice, layer, metadata.width);
      uint32_t bucket_id = layer * metadata.width + col;
      std::string bucket_key = getBucketKey(ns_key, metadata.version, bucket_id);

      std::string count_str;
      s = storage_->Get(ctx, ctx.GetReadOptions(), bucket_key, &count_str);

      uint32_t count = 0;
      if (s.ok() && count_str.size() >= 4) {
        Slice count_slice(count_str);
        GetFixed32(&count_slice, &count);
      } else if (!s.IsNotFound()) {
        return s;
      }

      if (count < min_count) {
        min_count = count;
      }
    }

    (*counts)[i] = min_count;
  }

  return rocksdb::Status::OK();
}

/// CMS.MERGE - Merge multiple CMS sketches into one
///
/// Redis command: CMS.MERGE destination numKeys source [source ...] [WEIGHTS weight [weight ...]]
/// Documentation: https://redis.io/docs/latest/commands/cms.merge/
///
/// Parameters:
///   - destination: The name of destination sketch (must be initialized)
///   - numKeys: Number of sketches to merge
///   - source: Names of source sketches to merge
///   - weight: Multiplier for each sketch (can be negative, default = 1)
///
/// Time complexity: O(width * depth * numKeys)
/// Returns: OK on success, error if overflow detected
///
/// Requirements:
///   - All sketches must have identical width and depth
///   - Destination must already exist
///
/// Overflow behavior: Returns error if any bucket would overflow UINT32_MAX or
/// become negative after applying weights. This matches RedisBloom behavior.
rocksdb::Status CMS::Merge(engine::Context &ctx, const Slice &dest_key, const std::vector<std::string> &src_keys,
                           const std::vector<int64_t> &weights) {
  if (src_keys.empty()) {
    return rocksdb::Status::InvalidArgument("no source keys provided");
  }
  if (!weights.empty() && weights.size() != src_keys.size()) {
    return rocksdb::Status::InvalidArgument("number of weights must match number of source keys");
  }

  // Get all source metadata and validate dimensions match
  std::vector<CMSMetadata> src_metadata(src_keys.size());
  std::vector<std::string> src_ns_keys(src_keys.size());

  for (size_t i = 0; i < src_keys.size(); ++i) {
    src_ns_keys[i] = ComposeNamespaceKey(namespace_, src_keys[i], storage_->IsSlotIdEncoded());
    rocksdb::Status s = getCMSMetadata(ctx, src_ns_keys[i], &src_metadata[i]);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return rocksdb::Status::InvalidArgument("source key not found: " + src_keys[i]);
      }
      return s;
    }

    // Validate dimensions match
    if (i > 0) {
      if (src_metadata[i].width != src_metadata[0].width || src_metadata[i].depth != src_metadata[0].depth) {
        return rocksdb::Status::InvalidArgument("CMS dimensions do not match");
      }
    }
  }

  // Create destination CMS
  std::string dest_ns_key = ComposeNamespaceKey(namespace_, dest_key, storage_->IsSlotIdEncoded());

  // Check if destination exists
  // Per Redis documentation: destination must be initialized
  CMSMetadata dest_metadata;
  rocksdb::Status s = getCMSMetadata(ctx, dest_ns_key, &dest_metadata);
  if (s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument("destination key not found: " + dest_key.ToString());
  }
  if (!s.ok()) {
    return s;
  }

  // Check if destination dimensions match source
  if (dest_metadata.width != src_metadata[0].width || dest_metadata.depth != src_metadata[0].depth) {
    return rocksdb::Status::InvalidArgument("destination dimensions do not match source");
  }

  uint32_t width = dest_metadata.width;
  uint32_t depth = dest_metadata.depth;
  uint32_t total_buckets = width * depth;

  // Phase 1: Pre-check for overflow (following RedisBloom behavior)
  // Read all buckets and validate no overflow will occur
  std::vector<std::vector<uint32_t>> src_buckets(src_keys.size());
  for (size_t k = 0; k < src_keys.size(); ++k) {
    src_buckets[k].resize(total_buckets, 0);
    for (uint32_t bucket_id = 0; bucket_id < total_buckets; ++bucket_id) {
      std::string bucket_key = getBucketKey(src_ns_keys[k], src_metadata[k].version, bucket_id);
      std::string count_str;
      s = storage_->Get(ctx, ctx.GetReadOptions(), bucket_key, &count_str);
      if (s.ok() && count_str.size() >= 4) {
        Slice count_slice(count_str);
        GetFixed32(&count_slice, &src_buckets[k][bucket_id]);
      } else if (!s.IsNotFound()) {
        return s;
      }
    }
  }

  // Check for overflow in all buckets
  for (uint32_t bucket_id = 0; bucket_id < total_buckets; ++bucket_id) {
    int64_t item_count = 0;
    for (size_t k = 0; k < src_keys.size(); ++k) {
      int64_t weight = weights.empty() ? 1 : weights[k];
      int64_t count = src_buckets[k][bucket_id];
      int64_t mul = 0;

      // Check for multiplication and addition overflow
      if (__builtin_mul_overflow(count, weight, &mul) || __builtin_add_overflow(item_count, mul, &item_count)) {
        return rocksdb::Status::InvalidArgument("overflow detected in merge operation");
      }
    }

    // Validate result is within valid range for uint32_t
    if (item_count < 0 || item_count > UINT32_MAX) {
      return rocksdb::Status::InvalidArgument("overflow detected in merge operation");
    }
  }

  // Check total_count overflow
  int64_t cms_count = 0;
  for (size_t k = 0; k < src_keys.size(); ++k) {
    int64_t weight = weights.empty() ? 1 : weights[k];
    int64_t mul = 0;
    if (__builtin_mul_overflow(static_cast<int64_t>(src_metadata[k].total_count), weight, &mul) ||
        __builtin_add_overflow(cms_count, mul, &cms_count)) {
      return rocksdb::Status::InvalidArgument("overflow detected in merge operation");
    }
  }
  if (cms_count < 0) {
    return rocksdb::Status::InvalidArgument("overflow detected in merge operation");
  }

  // Phase 2: Execute merge (pre-check passed, no overflow will occur)
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCMS, {"Merge"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  dest_metadata.total_count = 0;

  for (uint32_t bucket_id = 0; bucket_id < total_buckets; ++bucket_id) {
    int64_t item_count = 0;

    for (size_t k = 0; k < src_keys.size(); ++k) {
      int64_t weight = weights.empty() ? 1 : weights[k];
      item_count += static_cast<int64_t>(src_buckets[k][bucket_id]) * weight;
    }

    // Write merged bucket
    std::string dest_bucket_key = getBucketKey(dest_ns_key, dest_metadata.version, bucket_id);
    std::string count_value;
    PutFixed32(&count_value, static_cast<uint32_t>(item_count));
    s = batch->Put(dest_bucket_key, count_value);
    if (!s.ok()) return s;

    dest_metadata.total_count += item_count;
  }

  // Write destination metadata
  std::string metadata_bytes;
  dest_metadata.Encode(&metadata_bytes);
  s = batch->Put(metadata_cf_handle_, dest_ns_key, metadata_bytes);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

/// CMS.INFO - Get CMS information
///
/// Redis command: CMS.INFO key
/// Documentation: https://redis.io/docs/latest/commands/cms.info/
///
/// Parameters:
///   - key: The name of the sketch
///
/// Time complexity: O(1)
/// Returns: width, depth, count (total count), size (number of buckets)
///
/// Note: 'size' field is a Kvrocks extension (not in Redis).
rocksdb::Status CMS::Info(engine::Context &ctx, const Slice &key, CMSInfo *info) {
  std::string ns_key = ComposeNamespaceKey(namespace_, key, storage_->IsSlotIdEncoded());

  CMSMetadata metadata;
  rocksdb::Status s = getCMSMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  info->width = metadata.width;
  info->depth = metadata.depth;
  info->total_count = metadata.total_count;
  info->size = static_cast<uint64_t>(metadata.width) * metadata.depth;

  return rocksdb::Status::OK();
}

}  // namespace redis