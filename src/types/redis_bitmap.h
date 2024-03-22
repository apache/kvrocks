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

#include <optional>
#include <string>
#include <vector>

#include "common/bitfield_util.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

enum BitOpFlags {
  kBitOpAnd,
  kBitOpOr,
  kBitOpXor,
  kBitOpNot,
};

namespace redis {

constexpr uint32_t kBitmapSegmentBits = 1024 * 8;
constexpr uint32_t kBitmapSegmentBytes = 1024;

// We use least-significant bit (LSB) numbering (also known as bit-endianness).
// This means that within a group of 8 bits, we read right-to-left.
// This is different from applying "bit" commands to string, which uses MSB.
class Bitmap : public Database {
 public:
  class SegmentCacheStore;

  Bitmap(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status GetBit(const Slice &user_key, uint32_t bit_offset, bool *bit);
  rocksdb::Status GetString(const Slice &user_key, uint32_t max_btos_size, std::string *value);
  rocksdb::Status SetBit(const Slice &user_key, uint32_t bit_offset, bool new_bit, bool *old_bit);
  rocksdb::Status BitCount(const Slice &user_key, int64_t start, int64_t stop, bool is_bit_index, uint32_t *cnt);
  rocksdb::Status BitPos(const Slice &user_key, bool bit, int64_t start, int64_t stop, bool stop_given, int64_t *pos);
  rocksdb::Status BitOp(BitOpFlags op_flag, const std::string &op_name, const Slice &user_key,
                        const std::vector<Slice> &op_keys, int64_t *len);
  rocksdb::Status Bitfield(const Slice &user_key, const std::vector<BitfieldOperation> &ops,
                           std::vector<std::optional<BitfieldValue>> *rets) {
    return bitfield<false>(user_key, ops, rets);
  }
  // read-only version for Bitfield(), if there is a write operation in ops, the function will return with failed
  // status.
  rocksdb::Status BitfieldReadOnly(const Slice &user_key, const std::vector<BitfieldOperation> &ops,
                                   std::vector<std::optional<BitfieldValue>> *rets) {
    return bitfield<true>(user_key, ops, rets);
  }
  static bool GetBitFromValueAndOffset(std::string_view value, uint32_t bit_offset);
  static bool IsEmptySegment(const Slice &segment);

 private:
  template <bool ReadOnly>
  rocksdb::Status bitfield(const Slice &user_key, const std::vector<BitfieldOperation> &ops,
                           std::vector<std::optional<BitfieldValue>> *rets);
  static bool bitfieldWriteAheadLog(const ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                    const std::vector<BitfieldOperation> &ops);
  rocksdb::Status GetMetadata(const Slice &ns_key, BitmapMetadata *metadata, std::string *raw_value);

  template <bool ReadOnly>
  static rocksdb::Status runBitfieldOperationsWithCache(SegmentCacheStore &cache,
                                                        const std::vector<BitfieldOperation> &ops,
                                                        std::vector<std::optional<BitfieldValue>> *rets);
};

// SegmentCacheStore is used to read segments from storage.
class Bitmap::SegmentCacheStore {
 public:
  SegmentCacheStore(engine::Storage *storage, rocksdb::ColumnFamilyHandle *metadata_cf_handle,
                    std::string namespace_key, const Metadata &bitmap_metadata)
      : storage_(storage),
        metadata_cf_handle_(metadata_cf_handle),
        ns_key_(std::move(namespace_key)),
        metadata_(bitmap_metadata) {}

  // Set a segment by given index
  void Set(uint32_t index, const std::string &segment) {
    auto [seg_itor, _] = cache_.try_emplace(index);
    auto &[__, str] = seg_itor->second;
    str = segment;
  }

  // Get a read-only segment by given index
  rocksdb::Status Get(uint32_t index, const std::string **cache) {
    std::string *res = nullptr;
    auto s = get(index, /*set_dirty=*/false, &res);
    if (s.ok()) {
      *cache = res;
    }
    return s;
  }

  // Get a segment by given index, and mark it dirty.
  rocksdb::Status GetMut(uint32_t index, std::string **cache) { return get(index, /*set_dirty=*/true, cache); }

  // Add all dirty segments into write batch.
  void BatchForFlush(ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch) {
    uint64_t used_size = 0;
    for (auto &[index, content] : cache_) {
      if (content.first) {
        std::string sub_key =
            InternalKey(ns_key_, getSegmentSubKey(index), metadata_.version, storage_->IsSlotIdEncoded()).Encode();
        batch->Put(sub_key, content.second);
        used_size = std::max(used_size, static_cast<uint64_t>(index) * kBitmapSegmentBytes + content.second.size());
      }
    }
    if (used_size > metadata_.size) {
      metadata_.size = used_size;
      std::string bytes;
      metadata_.Encode(&bytes);
      batch->Put(metadata_cf_handle_, ns_key_, bytes);
    }
  }

 private:
  rocksdb::Status get(uint32_t index, bool set_dirty, std::string **cache) {
    auto [seg_itor, no_cache] = cache_.try_emplace(index);
    auto &[is_dirty, str] = seg_itor->second;

    if (no_cache) {
      is_dirty = false;
      std::string sub_key =
          InternalKey(ns_key_, getSegmentSubKey(index), metadata_.version, storage_->IsSlotIdEncoded()).Encode();
      rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), sub_key, &str);
      if (!s.ok() && !s.IsNotFound()) {
        return s;
      }
    }

    is_dirty |= set_dirty;
    *cache = &str;
    return rocksdb::Status::OK();
  }

  static std::string getSegmentSubKey(uint32_t index) { return std::to_string(index * kBitmapSegmentBytes); }

  engine::Storage *storage_;
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_;
  std::string ns_key_;
  Metadata metadata_;
  // Segment index -> [is_dirty, segment_cache_string]
  std::unordered_map<uint32_t, std::pair<bool, std::string>> cache_;
};

}  // namespace redis
