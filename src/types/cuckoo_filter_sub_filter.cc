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

#include "cuckoo_filter_sub_filter.h"

#include <vector>

#include "cuckoo_filter.h"
#include "cuckoo_filter_page.h"

namespace redis {

CuckooSubFilter::CuckooSubFilter(CuckooPageCache &pages, uint16_t filter_index, uint32_t num_buckets)
    : bucket_size_(pages.BucketSize()), filter_index_(filter_index), num_buckets_(num_buckets), pages_(pages) {}

rocksdb::Status CuckooSubFilter::TryInsert(uint64_t hash, uint8_t fingerprint, bool *inserted) {
  *inserted = false;
  uint32_t bucket1_idx = getPrimaryBucketIndex(hash);
  uint32_t bucket2_idx = getSecondaryBucketIndex(hash, fingerprint);
  auto s = pages_.PrefetchBuckets(filter_index_, num_buckets_, bucket1_idx, bucket2_idx);
  if (!s.ok()) return s;

  s = pages_.TryInsertInBucket(filter_index_, num_buckets_, bucket1_idx, fingerprint, inserted);
  if (!s.ok() || *inserted || bucket1_idx == bucket2_idx) return s;

  return pages_.TryInsertInBucket(filter_index_, num_buckets_, bucket2_idx, fingerprint, inserted);
}

rocksdb::Status CuckooSubFilter::TryKickOutInsert(uint64_t hash, uint8_t fingerprint, uint16_t max_iterations,
                                                  bool *inserted) {
  *inserted = false;

  uint32_t current_bucket_idx = getPrimaryBucketIndex(hash);
  uint8_t current_fp = fingerprint;
  uint32_t victim_slot = 0;
  std::vector<CuckooPageCache::SlotMutation> mutations;
  mutations.reserve(max_iterations);

  auto rollback = [&]() {
    for (auto it = mutations.rbegin(); it != mutations.rend(); ++it) {
      auto s = pages_.RestoreBucketSlot(*it);
      if (!s.ok()) return s;
    }
    return rocksdb::Status::OK();
  };

  auto rollbackAndReturn = [&](const rocksdb::Status &status) {
    auto rollback_status = rollback();
    return rollback_status.ok() ? status : rollback_status;
  };

  for (uint16_t iteration = 0; iteration < max_iterations; ++iteration) {
    CuckooPageCache::SlotMutation mutation;
    auto s = pages_.SetBucketSlotWithUndo(filter_index_, num_buckets_, current_bucket_idx, victim_slot, current_fp,
                                          &mutation);
    if (!s.ok()) return rollbackAndReturn(s);
    current_fp = mutation.OldFingerprint();
    mutations.push_back(mutation);

    if (current_fp == 0) {
      *inserted = true;
      return rocksdb::Status::OK();
    }

    uint32_t alt_bucket_idx = CuckooFilterHelper::GetAltBucketIndex(current_bucket_idx, current_fp, num_buckets_);

    bool inserted_in_alt_bucket = false;
    s = pages_.TryInsertInBucket(filter_index_, num_buckets_, alt_bucket_idx, current_fp, &inserted_in_alt_bucket);
    if (!s.ok()) return rollbackAndReturn(s);
    if (inserted_in_alt_bucket) {
      *inserted = true;
      return rocksdb::Status::OK();
    }

    current_bucket_idx = alt_bucket_idx;
    victim_slot = (victim_slot + 1) % bucket_size_;
  }

  return rollback();
}

uint32_t CuckooSubFilter::getPrimaryBucketIndex(uint64_t hash) const { return hash % num_buckets_; }

uint32_t CuckooSubFilter::getSecondaryBucketIndex(uint64_t hash, uint8_t fingerprint) const {
  return CuckooFilterHelper::GetAltHash(fingerprint, hash) % num_buckets_;
}

}  // namespace redis
