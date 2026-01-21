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

namespace redis {

// Cuckoo filter implementation from the paper:
// "Cuckoo Filter: Practically Better Than Bloom" by Fan et al.
// This is a bucket-based storage implementation where each bucket is stored
// as an independent key-value pair in RocksDB
class CuckooFilter {
 public:
  // Calculate the optimal number of buckets for the filter
  static uint32_t OptimalNumBuckets(uint64_t capacity, uint8_t bucket_size) {
    // A load factor of 95.5% is chosen for the cuckoo filter
    uint32_t num_buckets = static_cast<uint32_t>(capacity / bucket_size / 0.955);
    // Round up to next power of 2 for better hash distribution
    if (num_buckets == 0) num_buckets = 1;
    uint32_t power = 1;
    while (power < num_buckets) power <<= 1;
    return power;
  }

  // Generate fingerprint from hash (8-bit fingerprint, non-zero)
  static uint8_t GenerateFingerprint(uint64_t hash) {
    uint8_t fp = hash & 0xFF;
    return fp == 0 ? 1 : fp;  // Ensure non-zero fingerprint
  }

  // Calculate alternate bucket index using XOR
  static uint32_t GetAltBucketIndex(uint32_t bucket_idx, uint8_t fingerprint, uint32_t num_buckets) {
    // Use a simple hash of the fingerprint for the XOR operation
    uint32_t fp_hash = fingerprint * 0x5bd1e995;  // MurmurHash2 constant
    return (bucket_idx ^ fp_hash) % num_buckets;
  }
};

}  // namespace redis