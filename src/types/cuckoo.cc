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

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <vector>
#include "storage/redis_metadata.h"

CuckooFilter::CuckooFilter(uint64_t capacity,
                           uint16_t bucket_size,
                           uint16_t max_iterations,
                           uint16_t expansion,
                           uint64_t num_buckets = 0,
                           uint16_t num_filters = 0,
                           uint64_t num_items = 0,
                           uint64_t num_deletes = 0,
                           const std::vector<SubCF>& filters = std::vector<SubCF>())
    : capacity_(capacity), 
    bucket_size_(bucket_size), 
    max_iterations_(max_iterations), 
    num_buckets_(num_buckets),
    expansion_(expansion),
    num_filters_(num_filters),
    num_items_(num_items),
    num_deletes_(num_deletes),
    filters_(filters)
{
    assert(isPowerOf2(num_buckets_));
    
    if (num_filters_ == 0) {
        grow();
    }
}

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
  return index ^ (static_cast<uint64_t>(fingerprint) * 0x5bd1e995);
}

void CuckooFilter::getLookupParams(uint64_t hash, uint8_t &fingerprint, uint64_t &h1, uint64_t &h2) {
  fingerprint = hash % 255 + 1;
  h1 = hash;
  h2 = getAltHash(fingerprint, h1);
}

bool CuckooFilter::grow() {
  SubCF new_filter;
  auto growth = static_cast<size_t>(std::pow(expansion_, num_filters_));
  new_filter.bucket_size = bucket_size_;
  new_filter.num_buckets = num_buckets_ * growth;
  new_filter.data.resize(new_filter.num_buckets * bucket_size_, 0);

  filters_.push_back(new_filter);
  num_filters_++;
  return true;
}

bool CuckooFilter::filterFind(const SubCF &filter, uint8_t fingerprint, uint64_t h1, uint64_t h2) const {
  auto index1 = static_cast<std::vector<uint8_t>::difference_type>((h1 % filter.num_buckets) * filter.bucket_size);
  auto index2 = static_cast<std::vector<uint8_t>::difference_type>((h2 % filter.num_buckets) * filter.bucket_size);

  const auto &bucket1 = filter.data.begin() + index1;
  const auto &bucket2 = filter.data.begin() + index2;

  if (std::find(bucket1, bucket1 + bucket_size_, fingerprint) != bucket1 + bucket_size_) {
    return true;
  }
  if (std::find(bucket2, bucket2 + bucket_size_, fingerprint) != bucket2 + bucket_size_) {
    return true;
  }
  return false;
}

bool CuckooFilter::Contains(uint64_t hash) const {
  uint8_t fingerprint {};
  uint64_t h1 {}, h2 {};
  getLookupParams(hash, fingerprint, h1, h2);

  for (const auto &filter : filters_) {
    if (filterFind(filter, fingerprint, h1, h2)) {
      return true;
    }
  }
  return false;
}

bool CuckooFilter::bucketDelete(std::vector<uint8_t>::iterator bucket_start, uint8_t fingerprint) const {
  auto it = std::find(bucket_start, bucket_start + bucket_size_, fingerprint);
  if (it != bucket_start + bucket_size_) {
    *it = 0;  // Set the slot to 0 to indicate deletion
    return true;
  }
  return false;
}

bool CuckooFilter::filterDelete(SubCF &filter, uint8_t fingerprint, uint64_t h1, uint64_t h2) {
  auto index1 = static_cast<std::vector<uint8_t>::difference_type>((h1 % filter.num_buckets) * filter.bucket_size);
  auto index2 = static_cast<std::vector<uint8_t>::difference_type>((h2 % filter.num_buckets) * filter.bucket_size);

  // Get iterators for the start of each bucket
  auto bucket1 = filter.data.begin() + index1;
  auto bucket2 = filter.data.begin() + index2;

  return bucketDelete(bucket1, fingerprint) || bucketDelete(bucket2, fingerprint);
}

bool CuckooFilter::Remove(uint64_t hash) {
  uint8_t fingerprint {};
  uint64_t h1 {}, h2 {};
  getLookupParams(hash, fingerprint, h1, h2);

  for (int i = num_filters_ - 1; i >= 0; --i) {
    if (filterDelete(filters_[i], fingerprint, h1, h2)) {
      num_items_--;
      num_deletes_++;
      if (num_filters_ > 1 && static_cast<double>(num_deletes_) > static_cast<double>(num_items_) * 0.10) {
        Compact(false);
      }
      return true;
    }
  }
  return false;
}

bool CuckooFilter::filterInsert(SubCF &filter, uint8_t fingerprint, uint64_t h1, uint64_t h2) const {
  auto index1 = static_cast<std::vector<uint8_t>::difference_type>((h1 % filter.num_buckets) * filter.bucket_size);
  auto index2 = static_cast<std::vector<uint8_t>::difference_type>((h2 % filter.num_buckets) * filter.bucket_size);

  auto bucket1 = filter.data.begin() + index1;
  auto bucket2 = filter.data.begin() + index2;

  auto slot1 = std::find(bucket1, bucket1 + bucket_size_, 0);
  if (slot1 != bucket1 + bucket_size_) {
    *slot1 = fingerprint;
    return true;
  }

  auto slot2 = std::find(bucket2, bucket2 + bucket_size_, 0);
  if (slot2 != bucket2 + bucket_size_) {
    *slot2 = fingerprint;
    return true;
  }

  return false;
}

CuckooFilter::CuckooInsertStatus CuckooFilter::filterKickoutInsert(SubCF &filter, uint8_t fingerprint, uint64_t h1) const {
  uint16_t counter = 0;
  uint32_t num_buckets = filter.num_buckets;
  uint16_t victim_index = 0;
  uint64_t i = h1 % num_buckets;

  while (counter++ < max_iterations_) {
    size_t index = i * bucket_size_;
    auto bucket = filter.data.begin() + static_cast<std::vector<uint8_t>::difference_type>(index);

    std::swap(fingerprint, bucket[victim_index]);

    i = getAltHash(fingerprint, i) % num_buckets;
    size_t new_index = i * bucket_size_;
    auto new_bucket = filter.data.begin() + static_cast<std::vector<uint8_t>::difference_type>(new_index);
    auto empty_slot = std::find(new_bucket, new_bucket + bucket_size_, 0);
    if (empty_slot != new_bucket + bucket_size_) {
      *empty_slot = fingerprint;
      return Inserted;
    }
    victim_index = (victim_index + 1) % bucket_size_;
  }

  return NoSpace;
}

CuckooFilter::CuckooInsertStatus CuckooFilter::Insert(uint64_t hash) {
  uint8_t fingerprint {};
  uint64_t h1 {}, h2 {};
  getLookupParams(hash, fingerprint, h1, h2);

  for (int i = num_filters_ - 1; i >= 0; --i) {
    if (filterInsert(filters_[i], fingerprint, h1, h2)) {
      num_items_++;
      return Inserted;
    }
  }

  auto status = filterKickoutInsert(filters_.back(), fingerprint, h1);
  if (status == Inserted) {
    num_items_++;
    return Inserted;
  }

  if (expansion_ == 0) {
    return NoSpace;
  }

  if (!grow()) {
    return MemAllocFailed;
  }

  return Insert(hash);
}

CuckooFilter::CuckooInsertStatus CuckooFilter::InsertUnique(uint64_t hash) {
  if (Contains(hash)) {
    return Exists;
  }
  return Insert(hash);
}

uint64_t CuckooFilter::Count(uint64_t hash) const {
  uint8_t fingerprint {};
  uint64_t h1 {}, h2 {};
  getLookupParams(hash, fingerprint, h1, h2);

  uint64_t total = 0;
  for (const auto &filter : filters_) {
    auto index1 = static_cast<std::vector<uint8_t>::difference_type>((h1 % filter.num_buckets) * filter.bucket_size);
    auto index2 = static_cast<std::vector<uint8_t>::difference_type>((h2 % filter.num_buckets) * filter.bucket_size);

    auto bucket1 = filter.data.begin() + index1;
    auto bucket2 = filter.data.begin() + index2;

    total += std::count(bucket1, bucket1 + bucket_size_, fingerprint);
    total += std::count(bucket2, bucket2 + bucket_size_, fingerprint);
  }
  return total;
}

void CuckooFilter::Compact(bool cont) {
  for (int i = num_filters_ - 1; i > 0; --i) {
    if (!compactSingle(i) && !cont) {
      break;
    }
  }
  num_deletes_ = 0;
}

bool CuckooFilter::compactSingle(uint16_t filter_index) {
  auto &current_filter = filters_[filter_index];
  bool success = true;

  for (uint64_t bucket_ix = 0; bucket_ix < current_filter.num_buckets; ++bucket_ix) {
    for (uint16_t slot_ix = 0; slot_ix < bucket_size_; ++slot_ix) {
      if (!relocateSlot(current_filter.data, filter_index, bucket_ix, slot_ix)) {
        success = false;
      }
    }
  }

  if (success && filter_index == num_filters_ - 1) {
    filters_.pop_back();
    num_filters_--;
  }

  return success;
}

bool CuckooFilter::relocateSlot(std::vector<uint8_t> &bucket_data, uint16_t filter_index, uint64_t bucket_index,
                                uint16_t slot_index) {
  uint64_t index = bucket_index * bucket_size_ + slot_index;
  uint8_t fingerprint = bucket_data[index];

  if (fingerprint == 0) {
    return true;
  }

  uint64_t h1 = bucket_index;
  uint64_t h2 = getAltHash(fingerprint, h1);

  for (int i = 0; i < filter_index; ++i) {
    if (filterInsert(filters_[i], fingerprint, h1, h2)) {
      bucket_data[index] = 0;
      return true;
    }
  }
  return false;
}

bool CuckooFilter::ValidateIntegrity() const {
  return bucket_size_ != 0 && bucket_size_ <= 128 && num_buckets_ != 0 && num_buckets_ <= (1ULL << 48) &&
         num_filters_ != 0 && num_filters_ <= 64 && max_iterations_ != 0 && isPowerOf2(num_buckets_);
}
