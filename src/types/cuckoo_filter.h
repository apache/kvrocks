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

#include <rocksdb/status.h>

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "vendor/murmurhash2.h"

namespace redis {

constexpr long double kCuckooFilterLoadFactor = 0.955L;
constexpr uint64_t kCuckooFilterMaxSupportedBuckets = std::numeric_limits<uint32_t>::max() / 2 + 1ULL;
constexpr uint64_t kCuckooFilterFingerprintModulus = 255;
constexpr uint64_t kCuckooFilterAltHashMultiplier = 0x5bd1e995ULL;

// Cuckoo filter implementation from the paper:
// "Cuckoo Filter: Practically Better Than Bloom" by Fan et al.
// Buckets are grouped into page values in RocksDB. The Cuckoo algorithm still
// works with logical bucket indexes, while the storage layer maps buckets to pages.
//
// Hash calculation follows RedisBloom's design:
// - fp = hash % 255 + 1 (fingerprint, non-zero, range: 1-255)
// - h1 = hash (primary hash)
// - h2 = h1 ^ (fp * 0x5bd1e995) (alternate hash via XOR)
// - bucket_index = hash % num_buckets (only apply modulo when indexing)
class CuckooFilterHelper {
 public:
  static bool IsCapacitySupported(uint64_t capacity, uint8_t bucket_size) {
    uint32_t num_buckets = 0;
    return CalculateRequiredBuckets(capacity, bucket_size, &num_buckets).ok();
  }

  static uint16_t NormalizeExpansion(uint16_t expansion) {
    if (expansion <= 1) return expansion;

    uint32_t normalized = 1;
    while (normalized < expansion) normalized <<= 1;
    return static_cast<uint16_t>(normalized);
  }

  // Returns the power-of-two bucket count required for the requested capacity.
  static rocksdb::Status CalculateRequiredBuckets(uint64_t capacity, uint8_t bucket_size, uint32_t *num_buckets) {
    if (bucket_size == 0) {
      return rocksdb::Status::InvalidArgument("bucket_size must be larger than 0");
    }

    auto max_supported_capacity =
        static_cast<uint64_t>(kCuckooFilterMaxSupportedBuckets * bucket_size * kCuckooFilterLoadFactor);
    if (capacity > max_supported_capacity) {
      return rocksdb::Status::InvalidArgument("capacity is too large");
    }

    auto exact_buckets = static_cast<long double>(capacity) / bucket_size / kCuckooFilterLoadFactor;
    auto required_buckets = static_cast<uint64_t>(exact_buckets);
    if (static_cast<long double>(required_buckets) < exact_buckets) required_buckets++;
    if (required_buckets == 0) required_buckets = 1;

    // Round up to next power of 2 for better hash distribution.
    uint32_t power = 1;
    while (power < required_buckets) power <<= 1;
    *num_buckets = power;
    return rocksdb::Status::OK();
  }

  // Following RedisBloom: fp = hash % 255 + 1.
  static uint8_t GenerateFingerprint(uint64_t hash) {
    return static_cast<uint8_t>(hash % kCuckooFilterFingerprintModulus + 1);
  }

  // Calculate alternate hash using XOR (following RedisBloom)
  // h2 = h1 ^ (fp * kCuckooFilterAltHashMultiplier)
  // This preserves symmetry: GetAltHash(fp, GetAltHash(fp, h)) == h
  static uint64_t GetAltHash(uint8_t fingerprint, uint64_t hash) {
    return hash ^ (static_cast<uint64_t>(fingerprint) * kCuckooFilterAltHashMultiplier);
  }

  // Calculate an alternate bucket from a bucket index and fingerprint.
  static uint32_t GetAltBucketIndex(uint32_t bucket_idx, uint8_t fingerprint, uint32_t num_buckets) {
    uint64_t hash = bucket_idx;
    uint64_t alt_hash = GetAltHash(fingerprint, hash);
    return static_cast<uint32_t>(alt_hash % num_buckets);
  }

  // Compute hash for a given item using MurmurHash2 (compatible with RedisBloom).
  static uint64_t Hash(const char *data, size_t length) { return HllMurMurHash64A(data, static_cast<int>(length), 0); }

  // Convenience overload for std::string
  static uint64_t Hash(const std::string &item) { return Hash(item.data(), item.size()); }

  // Calculate the capacity of a sub-filter at a given index in the chain.
  // Returns false if overflow would occur.
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

  // Calculate the number of buckets for a sub-filter at a given index.
  static rocksdb::Status GetFilterNumBuckets(uint64_t base_capacity, uint16_t expansion, uint8_t bucket_size,
                                             uint16_t filter_index, uint32_t *num_buckets) {
    uint64_t filter_capacity = 0;
    if (!CalculateFilterCapacity(base_capacity, expansion, filter_index, &filter_capacity)) {
      return rocksdb::Status::Corruption("invalid metadata: filter capacity is too large");
    }

    auto s = CalculateRequiredBuckets(filter_capacity, bucket_size, num_buckets);
    if (!s.ok()) return rocksdb::Status::Corruption("invalid metadata: filter capacity is too large");
    return rocksdb::Status::OK();
  }
};

}  // namespace redis
