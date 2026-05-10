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
#include <rocksdb/write_batch.h>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/redis_metadata.h"
#include "storage/storage.h"

namespace redis {

class CuckooPageSet {
 public:
  CuckooPageSet(engine::Storage *storage, engine::Context &ctx, const Slice &ns_key,
                const CuckooChainMetadata &metadata, bool slot_id_encoded);

  rocksdb::Status TryInsertInCandidateBuckets(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket1_index,
                                              uint32_t bucket2_index, uint8_t fingerprint, bool *inserted);
  rocksdb::Status TryInsertInBucket(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                    uint8_t fingerprint, bool *inserted);
  rocksdb::Status GetBucketSlot(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index, uint32_t slot_idx,
                                uint8_t *fingerprint);
  rocksdb::Status SetBucketSlot(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index, uint32_t slot_idx,
                                uint8_t fingerprint);
  rocksdb::Status WriteBackDirtyPages(rocksdb::WriteBatchBase *batch);

 private:
  struct PageEntry {
    std::string data;
    bool is_dirty = false;
  };

  struct BucketRef {
    PageEntry *page = nullptr;
    uint32_t offset = 0;
    uint8_t size = 0;
  };

  struct BucketLocation {
    std::string page_key;
    uint32_t offset = 0;
    uint32_t expected_page_size = 0;
  };

  rocksdb::Status ResolveBucketLocation(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                        BucketLocation *location) const;
  rocksdb::Status EnsureBucketLoaded(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                     BucketRef *bucket);
  rocksdb::Status EnsureCandidateBucketsLoaded(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket1_index,
                                               uint32_t bucket2_index, BucketRef *bucket1, BucketRef *bucket2);
  rocksdb::Status LoadPage(const BucketLocation &location, PageEntry **page);
  rocksdb::Status LoadPages(const std::vector<BucketLocation> &locations);
  rocksdb::Status NormalizePage(const rocksdb::Status &status, uint32_t expected_size, PageEntry *page) const;

  bool TryInsertInBucketRef(const BucketRef &bucket, uint8_t fingerprint, size_t *slot_idx);
  uint8_t GetBucketRefSlot(const BucketRef &bucket, uint32_t slot_idx) const;
  void SetBucketRefSlot(const BucketRef &bucket, uint32_t slot_idx, uint8_t fingerprint);

  engine::Storage *storage_ = nullptr;
  engine::Context &ctx_;
  std::string ns_key_;
  const CuckooChainMetadata &metadata_;
  bool slot_id_encoded_ = false;
  std::unordered_map<std::string, PageEntry> pages_;
};

}  // namespace redis
