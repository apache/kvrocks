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

#include "storage/storage.h"

namespace redis {

class CuckooPageCache {
 public:
  CuckooPageCache(engine::Storage *storage, engine::Context &ctx, const Slice &ns_key, bool slot_id_encoded,
                  uint64_t version, uint8_t bucket_size, uint32_t page_size);

  rocksdb::Status PrefetchBuckets(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket1_index,
                                  uint32_t bucket2_index);
  rocksdb::Status TryInsertInBucket(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                    uint8_t fingerprint, bool *inserted);
  rocksdb::Status GetBucketSlot(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index, uint32_t slot_idx,
                                uint8_t *fingerprint);
  rocksdb::Status SetBucketSlot(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index, uint32_t slot_idx,
                                uint8_t fingerprint);
  rocksdb::Status WriteBackDirtyPages(rocksdb::WriteBatchBase *batch);

  void DiscardCachedPages();

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

  rocksdb::Status resolveBucketLocation(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                        BucketLocation *location) const;
  rocksdb::Status ensureBucketLoaded(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                     BucketRef *bucket);
  rocksdb::Status loadPage(const BucketLocation &location, PageEntry **page);
  rocksdb::Status loadPages(const std::vector<BucketLocation> &locations);
  static rocksdb::Status normalizePage(const rocksdb::Status &status, uint32_t expected_size, PageEntry *page);

  static bool tryInsertInBucketRef(const BucketRef &bucket, uint8_t fingerprint, size_t *slot_idx);
  static uint8_t getBucketRefSlot(const BucketRef &bucket, uint32_t slot_idx);
  static void setBucketRefSlot(const BucketRef &bucket, uint32_t slot_idx, uint8_t fingerprint);

  engine::Storage *storage_ = nullptr;
  engine::Context &ctx_;
  std::string ns_key_;
  bool slot_id_encoded_ = false;
  uint64_t version_ = 0;
  uint8_t bucket_size_ = 0;
  uint32_t page_size_ = 0;
  // Maps encoded cuckoo page keys to cached page entries.
  std::unordered_map<std::string, PageEntry> pages_;
};

}  // namespace redis
