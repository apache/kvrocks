// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/*
This project uses Redis, which is licensed under your choice of the
Redis Source Available License 2.0 (RSALv2) or the Server Side Public License v1 (SSPLv1).

For more information on Redis licensing, visit:
https://redis.io/docs/about/license/
*/

#include "cuckoo.h"

#include <vendor/murmurhash2.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

bool CuckooFilter::isPowerOf2(uint64_t num) { return (num & (num - 1)) == 0 && num != 0; }

uint64_t CuckooFilter::getNextPowerOf2(uint64_t n) {
  if (n == 0) return 1;
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  n++;
  return n;
}

uint64_t CuckooFilter::getAltHash(uint8_t fingerprint, uint64_t index) {
  uint64_t combined = (static_cast<uint64_t>(fingerprint) << 32) | index;
  return HllMurMurHash64A(reinterpret_cast<const void *>(&combined), sizeof(combined), 0x9747b28c);
}

// generate lookup parameters: fingerprint, h1, h2
void CuckooFilter::getLookupParams(uint64_t hash, uint8_t &fingerprint, uint64_t &h1, uint64_t &h2) {
  fingerprint = static_cast<uint8_t>((hash >> 16) & 0xFF) + 1;

  h1 = hash;
  h2 = getAltHash(fingerprint, h1);
}

CuckooFilter::CuckooFilter(uint64_t capacity, uint16_t bucket_size, uint16_t max_iterations, uint16_t expansion,
                           uint64_t num_buckets, uint16_t num_filters, uint64_t num_items, uint64_t num_deletes,
                           const std::vector<SubCF> &filters)
    : capacity_(capacity),
      bucket_size_(bucket_size),
      max_iterations_(max_iterations),
      expansion_(getNextPowerOf2(expansion)),
      num_buckets_(getNextPowerOf2(num_buckets)),
      num_filters_(num_filters),
      num_items_(num_items),
      num_deletes_(num_deletes),
      filters_(filters) {
  if (filters_.empty()) {
    SubCF initial_filter;
    initial_filter.bucket_size = bucket_size_;
    initial_filter.num_buckets = num_buckets_;
    initial_filter.data.resize(num_buckets_ * bucket_size_, CUCKOO_NULLFP);
    filters_.push_back(initial_filter);
    num_filters_ = 1;
  }
}

// grow the filter by adding a new SubCF with increased number of buckets
bool CuckooFilter::grow() {
  // calculate new number of buckets using linear growth
  uint64_t growth = expansion_;
  if (num_buckets_ > (std::numeric_limits<uint64_t>::max() / growth)) {
    return false;
  }
  uint64_t new_num_buckets = num_buckets_ * growth;

  // initialize the new SubCF
  SubCF new_filter;
  new_filter.bucket_size = bucket_size_;
  new_filter.num_buckets = new_num_buckets;
  try {
    new_filter.data.resize(new_num_buckets * bucket_size_, CUCKOO_NULLFP);
  } catch (const std::bad_alloc &) {
    return false;
  }

  // add the new filter
  filters_.push_back(new_filter);
  num_filters_++;
  num_buckets_ = new_num_buckets;
  return true;
}

// find if a fingerprint exists in a given filter
bool CuckooFilter::filterFind(const SubCF &filter, uint8_t fingerprint, uint64_t h1, uint64_t h2) const {
  uint64_t index1 = (h1 % filter.num_buckets) * filter.bucket_size;
  uint64_t index2 = (h2 % filter.num_buckets) * filter.bucket_size;

  const uint8_t *bucket1 = &filter.data[index1];
  const uint8_t *bucket2 = &filter.data[index2];

  // Search in both buckets
  if (std::find(bucket1, bucket1 + bucket_size_, fingerprint) != bucket1 + bucket_size_) {
    return true;
  }
  if (std::find(bucket2, bucket2 + bucket_size_, fingerprint) != bucket2 + bucket_size_) {
    return true;
  }
  return false;
}

// Insert a fingerprint into a specific filter
bool CuckooFilter::filterInsert(SubCF &filter, uint8_t fingerprint, uint64_t h1, uint64_t h2) const {
  uint64_t index1 = (h1 % filter.num_buckets) * filter.bucket_size;
  uint64_t index2 = (h2 % filter.num_buckets) * filter.bucket_size;

  uint8_t *bucket1 = &filter.data[index1];
  uint8_t *bucket2 = &filter.data[index2];

  // Try to insert into the first bucket
  for (uint16_t i = 0; i < bucket_size_; ++i) {
    if (bucket1[i] == CUCKOO_NULLFP) {
      bucket1[i] = fingerprint;
      return true;
    }
  }

  // Try to insert into the second bucket
  for (uint16_t i = 0; i < bucket_size_; ++i) {
    if (bucket2[i] == CUCKOO_NULLFP) {
      bucket2[i] = fingerprint;
      return true;
    }
  }

  // No space available
  return false;
}

// Delete a fingerprint from a specific bucket
bool CuckooFilter::bucketDelete(uint8_t *bucket_start, uint8_t fingerprint) const {
  for (uint16_t i = 0; i < bucket_size_; ++i) {
    if (*(bucket_start + i) == fingerprint) {
      *(bucket_start + i) = CUCKOO_NULLFP;
      return true;
    }
  }
  return false;
}

// delete a fingerprint from a specific filter
bool CuckooFilter::filterDelete(SubCF &filter, uint8_t fingerprint, uint64_t h1, uint64_t h2) {
  uint64_t index1 = (h1 % filter.num_buckets) * filter.bucket_size;
  uint64_t index2 = (h2 % filter.num_buckets) * filter.bucket_size;

  uint8_t *bucket1 = &filter.data[index1];
  uint8_t *bucket2 = &filter.data[index2];

  return (bucketDelete(bucket1, fingerprint) || bucketDelete(bucket2, fingerprint));
}

// insert with kickout mechanism
CuckooFilter::CuckooInsertStatus CuckooFilter::filterKickoutInsert(SubCF &filter, uint8_t fingerprint,
                                                                   uint64_t h1) const {
  uint16_t counter = 0;
  uint64_t i = h1 % filter.num_buckets;

  while (counter++ < max_iterations_) {
    size_t index = i * bucket_size_;
    // randomly select a victim slot
    uint16_t victim_index = rand() % bucket_size_;  // You can replace rand() with a better RNG if needed
    std::swap(fingerprint, filter.data[index + victim_index]);

    // Calculate the alternative index for the displaced fingerprint
    i = getAltHash(fingerprint, i) % filter.num_buckets;

    size_t new_index = i * bucket_size_;
    // Find an empty slot in the new bucket
    bool inserted = false;
    for (uint16_t j = 0; j < bucket_size_; ++j) {
      if (filter.data[new_index + j] == CUCKOO_NULLFP) {
        filter.data[new_index + j] = fingerprint;
        inserted = true;
        break;
      }
    }

    if (inserted) {
      return CuckooInsertStatus::Inserted;
    }
  }

  // if unable to insert after max_iterations_, return NoSpace
  return CuckooInsertStatus::NoSpace;
}

// insert a hash into the Cuckoo Filter
CuckooFilter::CuckooInsertStatus CuckooFilter::Insert(uint64_t hash) {
  uint8_t fingerprint{};
  uint64_t h1{}, h2{};
  getLookupParams(hash, fingerprint, h1, h2);

  // Attempt to insert into existing filters
  for (int i = num_filters_ - 1; i >= 0; --i) {
    if (filterInsert(filters_[i], fingerprint, h1, h2)) {
      num_items_++;
      return CuckooInsertStatus::Inserted;
    }
  }

  // attempt to kick out and insert into the last filter
  CuckooInsertStatus status = filterKickoutInsert(filters_.back(), fingerprint, h1);
  if (status == CuckooInsertStatus::Inserted) {
    num_items_++;
    return CuckooInsertStatus::Inserted;
  }

  // Grow the filter if possible
  if (expansion_ == 0 || !grow()) {
    return CuckooInsertStatus::MemAllocFailed;
  }

  // Retry insertion after growing
  return Insert(hash);
}

// Insert a unique hash into the Cuckoo Filter (only if it doesn't already exist)
CuckooFilter::CuckooInsertStatus CuckooFilter::InsertUnique(uint64_t hash) {
  if (Contains(hash)) {
    return CuckooInsertStatus::Exists;
  }
  return Insert(hash);
}

// Check if a hash exists in the Cuckoo Filter
bool CuckooFilter::Contains(uint64_t hash) const {
  uint8_t fingerprint{};
  uint64_t h1{}, h2{};
  getLookupParams(hash, fingerprint, h1, h2);

  for (const auto &filter : filters_) {
    if (filterFind(filter, fingerprint, h1, h2)) {
      return true;
    }
  }
  return false;
}

// Remove a hash from the Cuckoo Filter
bool CuckooFilter::Remove(uint64_t hash) {
  uint8_t fingerprint{};
  uint64_t h1{}, h2{};
  getLookupParams(hash, fingerprint, h1, h2);

  // Iterate from the latest filter to the earliest
  for (int i = num_filters_ - 1; i >= 0; --i) {
    if (filterDelete(filters_[i], fingerprint, h1, h2)) {
      num_items_--;
      num_deletes_++;
      // Trigger compaction if necessary
      if (num_filters_ > 1 && static_cast<double>(num_deletes_) > static_cast<double>(num_items_) * 0.10) {
        Compact(false);
      }
      return true;
    }
  }
  return false;
}

// Count the number of occurrences of a hash in the Cuckoo Filter
uint64_t CuckooFilter::Count(uint64_t hash) const {
  uint8_t fingerprint{};
  uint64_t h1{}, h2{};
  getLookupParams(hash, fingerprint, h1, h2);
  uint64_t total = 0;

  for (const auto &filter : filters_) {
    uint64_t index1 = (h1 % filter.num_buckets) * filter.bucket_size;
    uint64_t index2 = (h2 % filter.num_buckets) * filter.bucket_size;

    const uint8_t *bucket1 = &filter.data[index1];
    const uint8_t *bucket2 = &filter.data[index2];

    // Count in both buckets
    total += std::count(bucket1, bucket1 + bucket_size_, fingerprint);
    total += std::count(bucket2, bucket2 + bucket_size_, fingerprint);
  }

  return total;
}

// Relocate a fingerprint to an older filter
bool CuckooFilter::relocateSlot(std::vector<uint8_t> &bucket, uint16_t filter_index, uint64_t bucket_index,
                                uint16_t slot_index) {
  uint8_t fingerprint = bucket[slot_index];

  if (fingerprint == CUCKOO_NULLFP) {
    return true;
  }

  uint64_t h1 = bucket_index;
  uint64_t h2 = getAltHash(fingerprint, h1);

  // Attempt to relocate to older filters
  for (int i = 0; i < filter_index; ++i) {
    SubCF &target_filter = filters_[i];
    uint64_t target_loc1 = (h1 % target_filter.num_buckets) * target_filter.bucket_size;
    uint64_t target_loc2 = (h2 % target_filter.num_buckets) * target_filter.bucket_size;

    // Attempt to find an empty slot in the first bucket
    for (uint16_t j = 0; j < bucket_size_; ++j) {
      if (target_filter.data[target_loc1 + j] == CUCKOO_NULLFP) {
        target_filter.data[target_loc1 + j] = fingerprint;
        bucket[slot_index] = CUCKOO_NULLFP;
        return true;
      }
    }

    // Attempt to find an empty slot in the second bucket
    for (uint16_t j = 0; j < bucket_size_; ++j) {
      if (target_filter.data[target_loc2 + j] == CUCKOO_NULLFP) {
        target_filter.data[target_loc2 + j] = fingerprint;
        bucket[slot_index] = CUCKOO_NULLFP;
        return true;
      }
    }
  }

  // Unable to relocate
  return false;
}

// Compact a single filter by relocating all its fingerprints to older filters
bool CuckooFilter::compactSingle(uint16_t filter_index) {
  if (filter_index >= num_filters_) {
    return false;  // Invalid filter index
  }

  SubCF &current_filter = filters_[filter_index];
  bool success = true;

  for (uint64_t bucket_ix = 0; bucket_ix < current_filter.num_buckets; ++bucket_ix) {
    for (uint16_t slot_ix = 0; slot_ix < bucket_size_; ++slot_ix) {
      if (!relocateSlot(current_filter.data, filter_index, bucket_ix, slot_ix)) {
        success = false;
      }
    }
  }

  // Remove the filter if all relocations were successful and it's the latest filter
  if (success && filter_index == num_filters_ - 1) {
    filters_.pop_back();
    num_filters_--;
  }

  return success;
}

// Compact the Cuckoo Filter by relocating fingerprints from higher filters to lower ones
void CuckooFilter::Compact(bool cont) {
  // Iterate from the latest filter downwards
  for (int i = num_filters_ - 1; i > 0; --i) {
    if (!compactSingle(i) && !cont) {
      break;  // Stop if compacting failed and not continuing
    }
  }
  num_deletes_ = 0;
}

// Validates the cuckoo filter
bool CuckooFilter::ValidateIntegrity() const {
  return (bucket_size_ != 0 && bucket_size_ <= 128 &&           // CF_MAX_BUCKET_SIZE
          num_buckets_ != 0 && num_buckets_ <= (1ULL << 48) &&  // CF_MAX_NUM_BUCKETS
          num_filters_ != 0 && num_filters_ <= 64 &&            // CF_MAX_NUM_FILTERS
          max_iterations_ != 0 && isPowerOf2(num_buckets_));
}