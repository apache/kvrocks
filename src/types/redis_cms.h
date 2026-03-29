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

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

/// Default width for CMS (number of buckets per layer)
constexpr uint32_t kCMSDefaultWidth = 2000;

/// Default depth for CMS (number of layers)
constexpr uint32_t kCMSDefaultDepth = 9;

/// Maximum width allowed
constexpr uint32_t kCMSMaxWidth = 100000;

/// Maximum depth allowed
constexpr uint32_t kCMSMaxDepth = 100;

/// Maximum total size in bytes (16MB)
constexpr uint64_t kCMSMaxSize = 16 * 1024 * 1024;

/// Info fields for CMS.INFO command
enum class CMSInfoField {
  kAll,
  kWidth,
  kDepth,
  kTotalCount,
};

/// CMS information structure
struct CMSInfo {
  uint32_t width;
  uint32_t depth;
  uint64_t total_count;
  uint64_t size;  // Number of buckets (width * depth)
};

/// Count-Min Sketch probabilistic data structure
class CMS : public Database {
 public:
  explicit CMS(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}

  /// Initialize CMS with given dimensions
  /// @param ctx Engine context
  /// @param key User key
  /// @param width Number of buckets per layer
  /// @param depth Number of layers
  rocksdb::Status InitByDim(engine::Context &ctx, const Slice &key, uint32_t width, uint32_t depth);

  /// Initialize CMS with given error rate and probability
  /// @param ctx Engine context
  /// @param key User key
  /// @param error_rate Desired error rate (0 < error_rate < 1)
  /// @param probability Desired probability (0 < probability < 1)
  rocksdb::Status InitByProb(engine::Context &ctx, const Slice &key, double error_rate, double probability);

  /// Increment counters for given items
  /// @param ctx Engine context
  /// @param key User key
  /// @param items Vector of (item, increment) pairs
  /// @param counts Output: estimated counts for each item after increment
  rocksdb::Status IncrBy(engine::Context &ctx, const Slice &key,
                         const std::vector<std::pair<std::string, int64_t>> &items,
                         std::vector<uint64_t> *counts);

  /// Query estimated counts for given items
  /// @param ctx Engine context
  /// @param key User key
  /// @param items Vector of items to query
  /// @param counts Output: estimated counts for each item
  rocksdb::Status Query(engine::Context &ctx, const Slice &key, const std::vector<std::string> &items,
                        std::vector<uint64_t> *counts);

  /// Merge multiple CMS sketches into one
  /// @param ctx Engine context
  /// @param dest_key Destination key
  /// @param src_keys Source CMS keys
  /// @param weights Weights for each source (can be negative, optional, default all 1)
  rocksdb::Status Merge(engine::Context &ctx, const Slice &dest_key, const std::vector<std::string> &src_keys,
                        const std::vector<int64_t> &weights);

  /// Get CMS information
  /// @param ctx Engine context
  /// @param key User key
  /// @param info Output: CMS information
  rocksdb::Status Info(engine::Context &ctx, const Slice &key, CMSInfo *info);

 private:
  /// Get CMS metadata from storage
  rocksdb::Status getCMSMetadata(engine::Context &ctx, const Slice &ns_key, CMSMetadata *metadata);

  /// Build bucket key for given bucket_id
  /// @param ns_key Namespace key
  /// @param version Metadata version
  /// @param bucket_id Bucket identifier (layer * width + col)
  /// @return Encoded InternalKey for the bucket
  std::string getBucketKey(const Slice &ns_key, uint64_t version, uint32_t bucket_id);

  /// Hash an item for a specific layer
  /// @param item Item to hash
  /// @param layer Layer index (0 to depth-1)
  /// @return Hash value for the layer
  static uint64_t hashItem(const Slice &item, uint32_t layer);

  /// Calculate bucket_id for an item at a given layer
  /// @param item Item to calculate bucket for
  /// @param layer Layer index
  /// @param width Width of the CMS
  /// @return Column index (hash % width)
  static uint32_t getCol(const Slice &item, uint32_t layer, uint32_t width);

  /// Calculate width and depth from error rate and probability
  /// @param error_rate Desired error rate
  /// @param probability Desired probability
  /// @return Pair of (width, depth)
  static std::pair<uint32_t, uint32_t> calcDimFromProb(double error_rate, double probability);
};

}  // namespace redis