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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "vendor/murmurhash2.h"

namespace redis {

// Cuckoo filter implementation from the paper:
// "Cuckoo Filter: Practically Better Than Bloom" by Fan et al.
// This is a bucket-based storage implementation where each bucket is stored
// as an independent key-value pair in RocksDB
//
// Hash calculation follows RedisBloom's design:
// - fp = hash % 255 + 1 (fingerprint, non-zero, range: 1-255)
// - h1 = hash (primary hash)
// - h2 = h1 ^ (fp * 0x5bd1e995) (alternate hash via XOR)
// - bucket_index = hash % num_buckets (only apply modulo when indexing)
class CuckooFilter {
 public:
  // Calculate the optimal number of buckets for the filter
  static uint32_t OptimalNumBuckets(uint64_t capacity, uint8_t bucket_size) {
    // A load factor of 95.5% is chosen for the cuckoo filter
    auto num_buckets = static_cast<uint32_t>(static_cast<long double>(capacity) / bucket_size / 0.955L);
    // Round up to next power of 2 for better hash distribution
    if (num_buckets == 0) num_buckets = 1;
    uint32_t power = 1;
    while (power < num_buckets) power <<= 1;
    return power;
  }

  // Generate fingerprint from hash (8-bit fingerprint, non-zero, range: 1-255)
  // Following RedisBloom: fp = hash % 255 + 1
  static uint8_t GenerateFingerprint(uint64_t hash) { return static_cast<uint8_t>(hash % 255 + 1); }

  // Calculate alternate hash using XOR (following RedisBloom)
  // h2 = h1 ^ (fp * 0x5bd1e995)
  // This preserves symmetry: GetAltHash(fp, GetAltHash(fp, h)) == h
  static uint64_t GetAltHash(uint8_t fingerprint, uint64_t hash) {
    return hash ^ (static_cast<uint64_t>(fingerprint) * 0x5bd1e995);
  }

  // Legacy function for backward compatibility with tests
  // Converts bucket index to hash, applies GetAltHash, then converts back to bucket index
  static uint32_t GetAltBucketIndex(uint32_t bucket_idx, uint8_t fingerprint, uint32_t num_buckets) {
    // Treat bucket_idx as a hash value for the calculation
    uint64_t hash = bucket_idx;
    uint64_t alt_hash = GetAltHash(fingerprint, hash);
    // Convert back to bucket index
    return static_cast<uint32_t>(alt_hash % num_buckets);
  }

  // Compute hash for a given item using MurmurHash2 (compatible with RedisBloom)
  // This is the entry point for hashing items before they are inserted/checked in the filter
  static uint64_t Hash(const char* data, size_t length) { return HllMurMurHash64A(data, static_cast<int>(length), 0); }

  // Convenience overload for std::string
  static uint64_t Hash(const std::string& item) { return Hash(item.data(), item.size()); }
};

}  // namespace redis
