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

#include "redis_hyperloglog.h"

#include <db_util.h>
#include <stdint.h>

#include "hyperloglog.h"
#include "vendor/murmurhash2.h"

namespace redis {

class HllSegmentCache {
 public:
  struct SegmentEntry {
    std::string data;
    bool dirty;
  };
  std::map<uint32_t, SegmentEntry> segments;

  rocksdb::Status Get(uint32_t segment_index,
                      const std::function<rocksdb::Status(uint32_t, std::string *)> &get_segment,
                      SegmentEntry **entry) {
    auto iter = segments.find(segment_index);
    if (iter == segments.end()) {
      std::string segment_data;
      auto s = get_segment(segment_index, &segment_data);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          iter = segments.emplace(segment_index, SegmentEntry{std::move(segment_data), false}).first;
          // Initialize the segment with 0
          iter->second.data.resize(kHyperLogLogRegisterBytesPerSegment, 0);
          *entry = &iter->second;
          return rocksdb::Status::OK();
        }
        return s;
      }
      iter = segments.emplace(segment_index, SegmentEntry{std::move(segment_data), false}).first;
    }
    if (iter->second.data.size() != kHyperLogLogRegisterBytesPerSegment) {
      return rocksdb::Status::Corruption(
          "invalid segment size: expect=" + std::to_string(kHyperLogLogRegisterBytesPerSegment) +
          ", actual=" + std::to_string(iter->second.data.size()));
    }
    *entry = &iter->second;
    return rocksdb::Status::OK();
  }
};

rocksdb::Status HyperLogLog::GetMetadata(Database::GetOptions get_options, const Slice &ns_key,
                                         HyperLogLogMetadata *metadata) {
  return Database::GetMetadata(get_options, {kRedisHyperLogLog}, ns_key, metadata);
}

uint64_t HyperLogLog::HllHash(std::string_view element) {
  DCHECK(element.size() <= std::numeric_limits<int32_t>::max());
  return HllMurMurHash64A(element.data(), static_cast<int32_t>(element.size()), kHyperLogLogHashSeed);
}

/* the max 0 pattern counter of the subset the element belongs to is incremented if needed */
rocksdb::Status HyperLogLog::Add(const Slice &user_key, const std::vector<uint64_t> &element_hashes, uint64_t *ret) {
  *ret = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HyperLogLogMetadata metadata;
  rocksdb::Status s = GetMetadata(GetOptions(), ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHyperLogLog);
  batch->PutLogData(log_data.Encode());

  HllSegmentCache cache;
  for (uint64_t element_hash : element_hashes) {
    DenseHllResult dense_hll_result = ExtractDenseHllResult(element_hash);
    uint32_t segment_index = dense_hll_result.register_index / kHyperLogLogRegisterCountPerSegment;
    uint32_t register_index_in_segment = dense_hll_result.register_index % kHyperLogLogRegisterCountPerSegment;

    HllSegmentCache::SegmentEntry *entry{nullptr};
    s = cache.Get(
        segment_index,
        [this, &ns_key](uint32_t segment_index, std::string *segment) -> rocksdb::Status {
          return this->getSubKey(Database::GetOptions{}, ns_key, segment_index, segment);
        },
        &entry);
    if (!s.ok()) return s;
    DCHECK(entry != nullptr);
    DCHECK_EQ(kHyperLogLogRegisterBytesPerSegment, entry->data.size());
    auto *segment_data = reinterpret_cast<uint8_t *>(entry->data.data());
    uint8_t old_count = HllDenseGetRegister(segment_data, register_index_in_segment);
    if (dense_hll_result.hll_trailing_zero > old_count) {
      HllDenseSetRegister(segment_data, register_index_in_segment, dense_hll_result.hll_trailing_zero);
      entry->dirty = true;
      *ret = 1;
    }
  }
  // Flush dirty segments
  for (const auto &[segment_index, entry] : cache.segments) {
    if (entry.dirty) {
      std::string sub_key = InternalKey(ns_key, "FIXME", metadata.version, storage_->IsSlotIdEncoded()).Encode();
      batch->Put(sub_key, entry.data);
    }
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status HyperLogLog::Count(const Slice &user_key, uint64_t *ret) {
  *ret = 0;
  std::vector<uint8_t> registers(kHyperLogLogRegisterBytes);
  auto s = getRegisters(user_key, &registers);
  if (!s.ok()) return s;
  *ret = HllCount(registers);
  return rocksdb::Status::OK();
}

rocksdb::Status HyperLogLog::getRegisters(const Slice &user_key, std::vector<uint8_t> *registers) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  HyperLogLogMetadata metadata;
  LatestSnapShot ss(storage_);

  rocksdb::Status s = GetMetadata(Database::GetOptions{ss.GetSnapShot()}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = ss.GetSnapShot();
  const rocksdb::Slice upper_bound(next_version_prefix);
  const rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    auto subkey = ikey.GetSubKey();
    auto register_index = ParseInt<uint32_t>(subkey.data(), 10);
    if (!register_index) {
      return rocksdb::Status::Corruption("parse subkey index failed: sub=" + subkey.ToString());
    }
    if (*register_index / kHyperLogLogRegisterCountPerSegment < 0 ||
        *register_index / kHyperLogLogRegisterCountPerSegment >= kHyperLogLogSegmentCount ||
        *register_index % kHyperLogLogRegisterCountPerSegment != 0) {
      return rocksdb::Status::Corruption("invalid subkey index: idx=" + subkey.ToString());
    }
    auto val = iter->value().ToStringView();
    if (val.size() != kHyperLogLogRegisterBytesPerSegment) {
      return rocksdb::Status::Corruption(
          "insufficient length subkey value size: expect=" + std::to_string(kHyperLogLogRegisterBytesPerSegment) +
          ", actual=" + std::to_string(val.size()));
    }

    auto register_byte_offset = *register_index / 8 * kHyperLogLogRegisterBits;
    std::copy(val.begin(), val.end(), registers->data() + register_byte_offset);
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
