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

#include "cuckoo_filter.h"

namespace redis {

CuckooSubFilter::CuckooSubFilter(engine::Storage *storage, engine::Context &ctx, const Slice &ns_key,
                                 bool slot_id_encoded, uint64_t version, uint8_t bucket_size, uint32_t page_size,
                                 uint16_t filter_index, uint32_t num_buckets)
    : bucket_size_(bucket_size),
      filter_index_(filter_index),
      num_buckets_(num_buckets),
      pages_(storage, ctx, ns_key, slot_id_encoded, version, bucket_size, page_size) {}

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

  for (uint16_t iteration = 0; iteration < max_iterations; ++iteration) {
    uint8_t old_fp = 0;
    auto s = pages_.GetBucketSlot(filter_index_, num_buckets_, current_bucket_idx, victim_slot, &old_fp);
    if (!s.ok()) {
      pages_.DiscardCachedPages();
      return s;
    }
    s = pages_.SetBucketSlot(filter_index_, num_buckets_, current_bucket_idx, victim_slot, current_fp);
    if (!s.ok()) {
      pages_.DiscardCachedPages();
      return s;
    }
    current_fp = old_fp;

    if (current_fp == 0) {
      *inserted = true;
      return rocksdb::Status::OK();
    }

    uint32_t alt_bucket_idx = CuckooFilterHelper::GetAltBucketIndex(current_bucket_idx, current_fp, num_buckets_);

    bool inserted_in_alt_bucket = false;
    s = pages_.TryInsertInBucket(filter_index_, num_buckets_, alt_bucket_idx, current_fp, &inserted_in_alt_bucket);
    if (!s.ok()) {
      pages_.DiscardCachedPages();
      return s;
    }
    if (inserted_in_alt_bucket) {
      *inserted = true;
      return rocksdb::Status::OK();
    }

    current_bucket_idx = alt_bucket_idx;
    victim_slot = (victim_slot + 1) % bucket_size_;
  }

  pages_.DiscardCachedPages();
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooSubFilter::WriteToBatch(rocksdb::WriteBatchBase *batch) {
  return pages_.WriteBackDirtyPages(batch);
}

uint32_t CuckooSubFilter::getPrimaryBucketIndex(uint64_t hash) const { return hash % num_buckets_; }

uint32_t CuckooSubFilter::getSecondaryBucketIndex(uint64_t hash, uint8_t fingerprint) const {
  return CuckooFilterHelper::GetAltHash(fingerprint, hash) % num_buckets_;
}

}  // namespace redis
