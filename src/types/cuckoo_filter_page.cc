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

#include "cuckoo_filter_page.h"

#include <algorithm>

#include "common/encoding.h"
#include "storage/redis_db.h"

namespace redis {

namespace {

uint32_t GetBucketsPerPage(uint32_t page_size, uint8_t bucket_size) {
  return std::max<uint32_t>(1, page_size / bucket_size);
}

uint32_t GetPageIndex(uint32_t bucket_index, uint32_t buckets_per_page) { return bucket_index / buckets_per_page; }

uint32_t GetBucketOffset(uint32_t bucket_index, uint32_t buckets_per_page, uint8_t bucket_size) {
  return (bucket_index % buckets_per_page) * bucket_size;
}

uint32_t GetExpectedPageSize(uint32_t page_index, uint32_t num_buckets, uint32_t buckets_per_page,
                             uint8_t bucket_size) {
  uint32_t first_bucket = page_index * buckets_per_page;
  uint32_t page_bucket_count = std::min(buckets_per_page, num_buckets - first_bucket);
  return page_bucket_count * bucket_size;
}

std::string GetCuckooPageKey(const Slice &ns_key, uint64_t version, bool slot_id_encoded, uint16_t filter_index,
                             uint32_t page_index) {
  std::string sub_key;
  PutFixed16(&sub_key, filter_index);
  PutFixed32(&sub_key, page_index);
  return InternalKey(ns_key, sub_key, version, slot_id_encoded).Encode();
}

}  // namespace

CuckooPageCache::CuckooPageCache(engine::Storage *storage, engine::Context &ctx, const Slice &ns_key,
                                 bool slot_id_encoded, uint64_t version, uint8_t bucket_size, uint32_t page_size)
    : storage_(storage),
      ctx_(ctx),
      ns_key_(ns_key.ToString()),
      slot_id_encoded_(slot_id_encoded),
      version_(version),
      bucket_size_(bucket_size),
      page_size_(page_size) {}

rocksdb::Status CuckooPageCache::PrefetchBuckets(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket1_index,
                                                 uint32_t bucket2_index) {
  BucketLocation location1, location2;
  auto s = resolveBucketLocation(filter_index, num_buckets, bucket1_index, &location1);
  if (!s.ok()) return s;
  s = resolveBucketLocation(filter_index, num_buckets, bucket2_index, &location2);
  if (!s.ok()) return s;

  std::vector<BucketLocation> missing_locations;
  if (pages_.find(location1.page_key) == pages_.end()) missing_locations.push_back(location1);
  if (location2.page_key != location1.page_key && pages_.find(location2.page_key) == pages_.end()) {
    missing_locations.push_back(location2);
  }
  return loadPages(missing_locations);
}

rocksdb::Status CuckooPageCache::TryInsertInBucket(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                                   uint8_t fingerprint, bool *inserted) {
  *inserted = false;
  BucketRef bucket;
  auto s = ensureBucketLoaded(filter_index, num_buckets, bucket_index, &bucket);
  if (!s.ok()) return s;

  size_t slot_idx = 0;
  *inserted = tryInsertInBucketRef(bucket, fingerprint, &slot_idx);
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooPageCache::GetBucketSlot(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                               uint32_t slot_idx, uint8_t *fingerprint) {
  if (slot_idx >= bucket_size_) return rocksdb::Status::InvalidArgument("invalid cuckoo filter bucket slot");

  BucketRef bucket;
  auto s = ensureBucketLoaded(filter_index, num_buckets, bucket_index, &bucket);
  if (!s.ok()) return s;
  *fingerprint = getBucketRefSlot(bucket, slot_idx);
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooPageCache::SetBucketSlot(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                               uint32_t slot_idx, uint8_t fingerprint) {
  if (slot_idx >= bucket_size_) return rocksdb::Status::InvalidArgument("invalid cuckoo filter bucket slot");

  BucketRef bucket;
  auto s = ensureBucketLoaded(filter_index, num_buckets, bucket_index, &bucket);
  if (!s.ok()) return s;
  setBucketRefSlot(bucket, slot_idx, fingerprint);
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooPageCache::WriteBackDirtyPages(rocksdb::WriteBatchBase *batch) {
  for (const auto &entry : pages_) {
    if (!entry.second.is_dirty) continue;
    auto s = batch->Put(entry.first, entry.second.data);
    if (!s.ok()) return s;
  }
  return rocksdb::Status::OK();
}

void CuckooPageCache::DiscardCachedPages() { pages_.clear(); }

rocksdb::Status CuckooPageCache::resolveBucketLocation(uint16_t filter_index, uint32_t num_buckets,
                                                       uint32_t bucket_index, BucketLocation *location) const {
  if (bucket_size_ == 0 || num_buckets == 0 || bucket_index >= num_buckets) {
    return rocksdb::Status::Corruption("invalid cuckoo filter bucket location");
  }

  uint32_t buckets_per_page = GetBucketsPerPage(page_size_, bucket_size_);
  uint32_t page_index = GetPageIndex(bucket_index, buckets_per_page);
  location->page_key = GetCuckooPageKey(ns_key_, version_, slot_id_encoded_, filter_index, page_index);
  location->offset = GetBucketOffset(bucket_index, buckets_per_page, bucket_size_);
  location->expected_page_size = GetExpectedPageSize(page_index, num_buckets, buckets_per_page, bucket_size_);
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooPageCache::ensureBucketLoaded(uint16_t filter_index, uint32_t num_buckets, uint32_t bucket_index,
                                                    BucketRef *bucket) {
  BucketLocation location;
  auto s = resolveBucketLocation(filter_index, num_buckets, bucket_index, &location);
  if (!s.ok()) return s;

  PageEntry *page = nullptr;
  s = loadPage(location, &page);
  if (!s.ok()) return s;

  bucket->page = page;
  bucket->offset = location.offset;
  bucket->size = bucket_size_;
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooPageCache::loadPage(const BucketLocation &location, PageEntry **page) {
  auto iter = pages_.find(location.page_key);
  if (iter != pages_.end()) {
    *page = &iter->second;
    return rocksdb::Status::OK();
  }

  PageEntry page_entry;
  auto s = storage_->Get(ctx_, ctx_.GetReadOptions(), location.page_key, &page_entry.data);
  if (!s.ok() && !s.IsNotFound()) return s;
  s = normalizePage(s, location.expected_page_size, &page_entry);
  if (!s.ok()) return s;

  auto result = pages_.emplace(location.page_key, std::move(page_entry));
  *page = &result.first->second;
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooPageCache::loadPages(const std::vector<BucketLocation> &locations) {
  if (locations.empty()) return rocksdb::Status::OK();
  if (locations.size() == 1) {
    PageEntry *page = nullptr;
    return loadPage(locations[0], &page);
  }

  std::vector<rocksdb::Slice> keys;
  keys.reserve(locations.size());
  for (const auto &location : locations) keys.emplace_back(location.page_key);

  std::vector<rocksdb::PinnableSlice> values(locations.size());
  std::vector<rocksdb::Status> statuses(locations.size());
  storage_->MultiGet(ctx_, ctx_.DefaultMultiGetOptions(), storage_->GetDB()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());

  for (size_t i = 0; i < locations.size(); ++i) {
    PageEntry page_entry;
    if (statuses[i].ok()) page_entry.data.assign(values[i].data(), values[i].size());
    auto s = normalizePage(statuses[i], locations[i].expected_page_size, &page_entry);
    if (!s.ok()) return s;
    pages_.emplace(locations[i].page_key, std::move(page_entry));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooPageCache::normalizePage(const rocksdb::Status &status, uint32_t expected_size, PageEntry *page) {
  if (!status.ok() && !status.IsNotFound()) return status;
  if (status.IsNotFound()) {
    page->data.assign(expected_size, 0);
    return rocksdb::Status::OK();
  }
  if (page->data.size() > expected_size) return rocksdb::Status::Corruption("invalid cuckoo filter page size");
  if (page->data.size() < expected_size) return rocksdb::Status::Corruption("invalid cuckoo filter page size");
  return rocksdb::Status::OK();
}

bool CuckooPageCache::tryInsertInBucketRef(const BucketRef &bucket, uint8_t fingerprint, size_t *slot_idx) {
  for (size_t i = 0; i < bucket.size; ++i) {
    size_t offset = bucket.offset + i;
    if (static_cast<uint8_t>(bucket.page->data[offset]) == 0) {
      bucket.page->data[offset] = static_cast<char>(fingerprint);
      bucket.page->is_dirty = true;
      *slot_idx = i;
      return true;
    }
  }
  return false;
}

uint8_t CuckooPageCache::getBucketRefSlot(const BucketRef &bucket, uint32_t slot_idx) {
  return static_cast<uint8_t>(bucket.page->data[bucket.offset + slot_idx]);
}

void CuckooPageCache::setBucketRefSlot(const BucketRef &bucket, uint32_t slot_idx, uint8_t fingerprint) {
  bucket.page->data[bucket.offset + slot_idx] = static_cast<char>(fingerprint);
  bucket.page->is_dirty = true;
}

}  // namespace redis
