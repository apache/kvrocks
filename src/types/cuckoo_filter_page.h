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
#include <list>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/storage.h"

namespace redis {

class CuckooPageCache {
 public:
  struct SlotMutation {
    uint8_t OldFingerprint() const { return old_fingerprint; }

    uint16_t filter_index = 0;
    uint32_t num_buckets = 0;
    uint32_t bucket_index = 0;
    uint32_t slot_idx = 0;
    uint8_t old_fingerprint = 0;
    bool page_was_dirty = false;
  };

  CuckooPageCache(engine::Storage *storage, engine::Context &ctx, const Slice &ns_key, bool slot_id_encoded,
                  uint64_t version, uint8_t bucket_size, uint32_t page_size);

  CuckooPageCache(const CuckooPageCache &) = delete;
  CuckooPageCache &operator=(const CuckooPageCache &) = delete;
  ~CuckooPageCache() = default;

  uint8_t BucketSize() const { return bucket_size_; }

  rocksdb::Status PrefetchBuckets(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket1_index,
                                  uint32_t bucket2_index);
  rocksdb::Status TryInsertInBucket(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                    uint8_t fingerprint, bool *inserted);
  rocksdb::Status GetBucketSlot(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index, uint32_t slot_idx,
                                uint8_t *fingerprint);
  rocksdb::Status SetBucketSlot(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index, uint32_t slot_idx,
                                uint8_t fingerprint);
  rocksdb::Status SetBucketSlotWithUndo(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                        uint32_t slot_idx, uint8_t fingerprint, SlotMutation *mutation);
  rocksdb::Status RestoreBucketSlot(const SlotMutation &mutation);
  rocksdb::Status WriteBackDirtyPages(rocksdb::WriteBatchBase *batch);

  void DiscardCleanPages();
  void DiscardCachedPages();

  // Counts how many slots in the bucket contain the given fingerprint.
  rocksdb::Status CountFingerprintInBucket(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                           uint8_t fingerprint, uint32_t *count);

 private:
  struct PageEntry {
    std::string data;
    bool exists = false;
    std::optional<std::list<std::string>::iterator> clean_list_iter;

    bool IsDirty() const { return !clean_list_iter.has_value(); }
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
  PageEntry *cachePage(const std::string &page_key, PageEntry &&page);
  void markDirty(PageEntry *page);
  static rocksdb::Status normalizePage(const rocksdb::Status &status, uint32_t expected_size, PageEntry *page);

  bool tryInsertInBucketRef(const BucketRef &bucket, uint8_t fingerprint, size_t *slot_idx);
  static uint8_t getBucketRefSlot(const BucketRef &bucket, uint32_t slot_idx);
  void setBucketRefSlot(const BucketRef &bucket, uint32_t slot_idx, uint8_t fingerprint);

  engine::Storage *storage_ = nullptr;
  engine::Context &ctx_;
  std::string ns_key_;
  bool slot_id_encoded_ = false;
  uint64_t version_ = 0;
  uint8_t bucket_size_ = 0;
  uint32_t page_size_ = 0;
  // Maps encoded cuckoo page keys to cached page entries.
  std::unordered_map<std::string, PageEntry> pages_;
  // Avoid rescanning dirty pages accumulated by earlier items in a bulk insertion.
  std::list<std::string> clean_page_keys_;
};

}  // namespace redis
