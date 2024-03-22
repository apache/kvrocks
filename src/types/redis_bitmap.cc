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

#include "redis_bitmap.h"

#include <algorithm>
#include <memory>
#include <vector>

#include "common/bit_util.h"
#include "db_util.h"
#include "parse_util.h"
#include "redis_bitmap_string.h"

namespace redis {

constexpr char kErrBitmapStringOutOfRange[] =
    "The size of the bitmap string exceeds the "
    "configuration item max-bitmap-to-string-mb";

/*
 * If you setbit bit 0 1, the value is stored as 0x01 in Kvrocks but 0x80 in Redis.
 * So we need to swap bits is to keep the same return value as Redis.
 * This swap table is generated according to the following mapping definition.
 * kBitSwapTable(x) =  ((x & 0x80) >> 7)| ((x & 0x40) >> 5)|\
 *                  ((x & 0x20) >> 3)| ((x & 0x10) >> 1)|\
 *                  ((x & 0x08) << 1)| ((x & 0x04) << 3)|\
 *                  ((x & 0x02) << 5)| ((x & 0x01) << 7);
 */
static const uint8_t kBitSwapTable[256] = {
    0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0, 0x08, 0x88, 0x48,
    0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8, 0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4,
    0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4, 0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C,
    0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC, 0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52, 0xD2,
    0x32, 0xB2, 0x72, 0xF2, 0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A,
    0xFA, 0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6, 0x0E, 0x8E,
    0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE, 0x01, 0x81, 0x41, 0xC1, 0x21,
    0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1, 0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9,
    0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9, 0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55,
    0xD5, 0x35, 0xB5, 0x75, 0xF5, 0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD,
    0x7D, 0xFD, 0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3, 0x0B,
    0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB, 0x07, 0x87, 0x47, 0xC7,
    0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7, 0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F,
    0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF};

// Resize the segment to makes its new length at least min_bytes, new bytes will be set to 0.
// min_bytes can not more than kBitmapSegmentBytes
void ExpandBitmapSegment(std::string *segment, size_t min_bytes) {
  assert(min_bytes <= kBitmapSegmentBytes);

  auto old_size = segment->size();
  if (min_bytes > old_size) {
    size_t new_size = 0;
    if (min_bytes > old_size * 2) {
      new_size = min_bytes;
    } else if (old_size * 2 > kBitmapSegmentBytes) {
      new_size = kBitmapSegmentBytes;
    } else {
      new_size = old_size * 2;
    }
    segment->resize(new_size, 0);
  }
}

// Constructing sub-key index, see:
// https://kvrocks.apache.org/community/data-structure-on-rocksdb#bitmap-sub-keys-values
// The value is also equal to the offset of the bytes in the bitmap.
uint32_t SegmentSubKeyIndexForBit(uint32_t bit_offset) {
  return (bit_offset / kBitmapSegmentBits) * kBitmapSegmentBytes;
}

rocksdb::Status Bitmap::GetMetadata(const Slice &ns_key, BitmapMetadata *metadata, std::string *raw_value) {
  auto s = GetRawMetadata(ns_key, raw_value);
  if (!s.ok()) return s;

  Slice slice = *raw_value;
  return ParseMetadata({kRedisBitmap, kRedisString}, &slice, metadata);
}

rocksdb::Status Bitmap::GetBit(const Slice &user_key, uint32_t bit_offset, bool *bit) {
  *bit = false;
  std::string raw_value;
  std::string ns_key = AppendNamespacePrefix(user_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (metadata.Type() == kRedisString) {
    redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.GetBit(raw_value, bit_offset, bit);
  }

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::PinnableSlice value;
  std::string sub_key = InternalKey(ns_key, std::to_string(SegmentSubKeyIndexForBit(bit_offset)), metadata.version,
                                    storage_->IsSlotIdEncoded())
                            .Encode();
  s = storage_->Get(read_options, sub_key, &value);
  // If s.IsNotFound(), it means all bits in this segment are 0,
  // so we can return with *bit == false directly.
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  uint32_t bit_offset_in_segment = bit_offset % kBitmapSegmentBits;
  if (bit_offset_in_segment / 8 < value.size() &&
      util::lsb::GetBit(reinterpret_cast<const uint8_t *>(value.data()), bit_offset_in_segment)) {
    *bit = true;
  }
  return rocksdb::Status::OK();
}

// Use this function after careful estimation, and reserve enough memory
// according to the max size of the bitmap string to prevent OOM.
rocksdb::Status Bitmap::GetString(const Slice &user_key, const uint32_t max_btos_size, std::string *value) {
  value->clear();
  std::string raw_value;
  std::string ns_key = AppendNamespacePrefix(user_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok()) return s;
  if (metadata.size > max_btos_size) {
    return rocksdb::Status::Aborted(kErrBitmapStringOutOfRange);
  }
  value->assign(metadata.size, 0);

  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();

  auto iter = util::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    auto parse_result = ParseInt<uint32_t>(ikey.GetSubKey().ToString(), 10);
    if (!parse_result) {
      return rocksdb::Status::InvalidArgument(parse_result.Msg());
    }
    uint32_t frag_index = *parse_result;
    std::string fragment = iter->value().ToString();
    // To be compatible with data written before the commit d603b0e(#338)
    // and avoid returning extra null char after expansion.
    uint32_t valid_size = std::min(
        {fragment.size(), static_cast<size_t>(kBitmapSegmentBytes), static_cast<size_t>(metadata.size - frag_index)});

    for (uint32_t i = 0; i < valid_size; i++) {
      if (!fragment[i]) continue;
      fragment[i] = static_cast<char>(kBitSwapTable[static_cast<uint8_t>(fragment[i])]);
    }
    value->replace(frag_index, valid_size, fragment.data(), valid_size);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Bitmap::SetBit(const Slice &user_key, uint32_t bit_offset, bool new_bit, bool *old_bit) {
  std::string raw_value;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  BitmapMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;

  if (metadata.Type() == kRedisString) {
    redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.SetBit(ns_key, &raw_value, bit_offset, new_bit, old_bit);
  }

  std::string value;
  uint32_t segment_index = SegmentSubKeyIndexForBit(bit_offset);
  std::string sub_key =
      InternalKey(ns_key, std::to_string(segment_index), metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
  }
  uint32_t bit_offset_in_segment = bit_offset % kBitmapSegmentBits;
  uint32_t byte_index = (bit_offset / 8) % kBitmapSegmentBytes;
  uint64_t used_size = segment_index + byte_index + 1;
  uint64_t bitmap_size = std::max(used_size, metadata.size);
  // NOTE: value.size() might be greater than metadata.size.
  ExpandBitmapSegment(&value, byte_index + 1);
  auto *data_ptr = reinterpret_cast<uint8_t *>(value.data());
  *old_bit = util::lsb::GetBit(data_ptr, bit_offset_in_segment);
  util::lsb::SetBitTo(data_ptr, bit_offset_in_segment, new_bit);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBitmap, {std::to_string(kRedisCmdSetBit), std::to_string(bit_offset)});
  batch->PutLogData(log_data.Encode());
  batch->Put(sub_key, value);
  if (metadata.size != bitmap_size) {
    metadata.size = bitmap_size;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Bitmap::BitCount(const Slice &user_key, int64_t start, int64_t stop, bool is_bit_index, uint32_t *cnt) {
  *cnt = 0;
  std::string raw_value;
  std::string ns_key = AppendNamespacePrefix(user_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  /* Convert negative indexes */
  if (start < 0 && stop < 0 && start > stop) {
    return rocksdb::Status::OK();
  }

  if (metadata.Type() == kRedisString) {
    redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.BitCount(raw_value, start, stop, is_bit_index, cnt);
  }

  auto totlen = static_cast<int64_t>(metadata.size);
  if (is_bit_index) totlen <<= 3;
  // Counting bits in byte [start, stop].
  std::tie(start, stop) = BitmapString::NormalizeRange(start, stop, totlen);
  // Always return 0 if start is greater than stop after normalization.
  if (start > stop) return rocksdb::Status::OK();

  int64_t start_byte = start;
  int64_t stop_byte = stop;
  uint8_t first_byte_neg_mask = 0, last_byte_neg_mask = 0;
  std::tie(start_byte, stop_byte) = BitmapString::NormalizeToByteRangeWithPaddingMask(
      is_bit_index, start, stop, &first_byte_neg_mask, &last_byte_neg_mask);

  auto u_start = static_cast<uint32_t>(start_byte);
  auto u_stop = static_cast<uint32_t>(stop_byte);

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  uint32_t start_index = u_start / kBitmapSegmentBytes;
  uint32_t stop_index = u_stop / kBitmapSegmentBytes;
  // Don't use multi get to prevent large range query, and take too much memory
  uint32_t mask_cnt = 0;
  for (uint32_t i = start_index; i <= stop_index; i++) {
    rocksdb::PinnableSlice pin_value;
    std::string sub_key =
        InternalKey(ns_key, std::to_string(i * kBitmapSegmentBytes), metadata.version, storage_->IsSlotIdEncoded())
            .Encode();
    s = storage_->Get(read_options, sub_key, &pin_value);
    if (!s.ok() && !s.IsNotFound()) return s;
    // NotFound means all bits in this segment are 0.
    if (s.IsNotFound()) continue;
    // Counting bits in [start_in_segment, stop_in_segment]
    int64_t start_in_segment = 0;                                                // start_index in 1024 bytes segment
    auto readable_stop_in_segment = static_cast<int64_t>(pin_value.size() - 1);  // stop_index  in 1024 bytes segment
    auto stop_in_segment = readable_stop_in_segment;
    if (i == start_index) {
      start_in_segment = u_start % kBitmapSegmentBytes;
      if (is_bit_index && start_in_segment <= readable_stop_in_segment && first_byte_neg_mask != 0) {
        uint8_t first_mask_byte =
            kBitSwapTable[static_cast<uint8_t>(pin_value[start_in_segment])] & first_byte_neg_mask;
        mask_cnt += util::RawPopcount(&first_mask_byte, 1);
      }
    }
    if (i == stop_index) {
      stop_in_segment = u_stop % kBitmapSegmentBytes;
      if (is_bit_index && stop_in_segment <= readable_stop_in_segment && last_byte_neg_mask != 0) {
        uint8_t last_mask_byte = kBitSwapTable[static_cast<uint8_t>(pin_value[stop_in_segment])] & last_byte_neg_mask;
        mask_cnt += util::RawPopcount(&last_mask_byte, 1);
      }
    }
    if (stop_in_segment >= start_in_segment && readable_stop_in_segment >= start_in_segment) {
      int64_t bytes = 0;
      bytes = std::min(stop_in_segment, readable_stop_in_segment) - start_in_segment + 1;
      *cnt += util::RawPopcount(reinterpret_cast<const uint8_t *>(pin_value.data()) + start_in_segment, bytes);
    }
  }
  *cnt -= mask_cnt;
  return rocksdb::Status::OK();
}

rocksdb::Status Bitmap::BitPos(const Slice &user_key, bool bit, int64_t start, int64_t stop, bool stop_given,
                               int64_t *pos) {
  std::string raw_value;
  std::string ns_key = AppendNamespacePrefix(user_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    *pos = bit ? -1 : 0;
    return rocksdb::Status::OK();
  }

  if (metadata.Type() == kRedisString) {
    redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.BitPos(raw_value, bit, start, stop, stop_given, pos);
  }
  std::tie(start, stop) = BitmapString::NormalizeRange(start, stop, static_cast<int64_t>(metadata.size));
  auto u_start = static_cast<uint32_t>(start);
  auto u_stop = static_cast<uint32_t>(stop);
  if (u_start > u_stop) {
    *pos = -1;
    return rocksdb::Status::OK();
  }

  auto bit_pos_in_byte = [](char byte, bool bit) -> int {
    for (int i = 0; i < 8; i++) {
      if (bit && (byte & (1 << i)) != 0) return i;
      if (!bit && (byte & (1 << i)) == 0) return i;
    }
    return -1;
  };

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  uint32_t start_index = u_start / kBitmapSegmentBytes;
  uint32_t stop_index = u_stop / kBitmapSegmentBytes;
  // Don't use multi get to prevent large range query, and take too much memory
  // Searching bits in segments [start_index, stop_index].
  for (uint32_t i = start_index; i <= stop_index; i++) {
    rocksdb::PinnableSlice pin_value;
    std::string sub_key =
        InternalKey(ns_key, std::to_string(i * kBitmapSegmentBytes), metadata.version, storage_->IsSlotIdEncoded())
            .Encode();
    s = storage_->Get(read_options, sub_key, &pin_value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      if (!bit) {
        // Note: even if stop is given, we can return immediately when bit is 0.
        // because bit_pos will always be greater.
        *pos = i * kBitmapSegmentBits;
        return rocksdb::Status::OK();
      }
      continue;
    }
    size_t byte_pos_in_segment = 0;
    if (i == start_index) byte_pos_in_segment = u_start % kBitmapSegmentBytes;
    size_t stop_byte_in_segment = pin_value.size();
    if (i == stop_index) {
      DCHECK_LE(u_stop % kBitmapSegmentBytes + 1, pin_value.size());
      stop_byte_in_segment = u_stop % kBitmapSegmentBytes + 1;
    }
    // Invariant:
    // 1. pin_value.size() <= kBitmapSegmentBytes.
    // 2. If it's the last segment, metadata.size % kBitmapSegmentBytes <= pin_value.size().
    for (; byte_pos_in_segment < stop_byte_in_segment; byte_pos_in_segment++) {
      int bit_pos_in_byte_value = bit_pos_in_byte(pin_value[byte_pos_in_segment], bit);
      if (bit_pos_in_byte_value != -1) {
        *pos = static_cast<int64_t>(i * kBitmapSegmentBits + byte_pos_in_segment * 8 + bit_pos_in_byte_value);
        return rocksdb::Status::OK();
      }
    }
    if (bit) {
      continue;
    }
    // There're two cases that `pin_value.size() < kBitmapSegmentBytes`:
    // 1. If it's the last segment, we've done searching in the above loop.
    // 2. If it's not the last segment, we can check if the segment is all 0.
    if (pin_value.size() < kBitmapSegmentBytes) {
      if (i == stop_index) {
        continue;
      }
      *pos = static_cast<int64_t>(i * kBitmapSegmentBits + pin_value.size() * 8);
      return rocksdb::Status::OK();
    }
  }
  // bit was not found
  /* If we are looking for clear bits, and the user specified an exact
   * range with start-end, we can't consider the right of the range as
   * zero padded (as we do when no explicit end is given).
   *
   * So if redisBitpos() returns the first bit outside the range,
   * we return -1 to the caller, to mean, in the specified range there
   * is not a single "0" bit. */
  if (stop_given && bit == 0) {
    *pos = -1;
    return rocksdb::Status::OK();
  }
  *pos = bit ? -1 : static_cast<int64_t>(metadata.size * 8);
  return rocksdb::Status::OK();
}

rocksdb::Status Bitmap::BitOp(BitOpFlags op_flag, const std::string &op_name, const Slice &user_key,
                              const std::vector<Slice> &op_keys, int64_t *len) {
  std::string raw_value;
  std::string ns_key = AppendNamespacePrefix(user_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  std::vector<std::pair<std::string, BitmapMetadata>> meta_pairs;
  uint64_t max_size = 0, num_keys = op_keys.size();

  for (const auto &op_key : op_keys) {
    BitmapMetadata metadata(false);
    std::string ns_op_key = AppendNamespacePrefix(op_key);
    auto s = GetMetadata(ns_op_key, &metadata, &raw_value);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        num_keys--;
        continue;
      }
      return s;
    }
    if (metadata.Type() == kRedisString) {
      return rocksdb::Status::InvalidArgument(kErrMsgWrongType);
    }
    if (metadata.size > max_size) max_size = metadata.size;
    meta_pairs.emplace_back(std::move(ns_op_key), metadata);
  }

  auto batch = storage_->GetWriteBatchBase();
  if (max_size == 0) {
    batch->Delete(metadata_cf_handle_, ns_key);
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  std::vector<std::string> log_args = {std::to_string(kRedisCmdBitOp), op_name};
  for (const auto &op_key : op_keys) {
    log_args.emplace_back(op_key.ToString());
  }
  WriteBatchLogData log_data(kRedisBitmap, std::move(log_args));
  batch->PutLogData(log_data.Encode());

  BitmapMetadata res_metadata;
  if (num_keys == op_keys.size() || op_flag != kBitOpAnd) {
    uint64_t frag_numkeys = num_keys;
    uint64_t stop_index = (max_size - 1) / kBitmapSegmentBytes;
    std::unique_ptr<unsigned char[]> frag_res(new unsigned char[kBitmapSegmentBytes]);
    uint16_t frag_maxlen = 0, frag_minlen = 0;
    std::string fragment;
    unsigned char output = 0, byte = 0;
    std::vector<std::string> fragments;

    LatestSnapShot ss(storage_);
    rocksdb::ReadOptions read_options;
    read_options.snapshot = ss.GetSnapShot();
    for (uint64_t frag_index = 0; frag_index <= stop_index; frag_index++) {
      for (const auto &meta_pair : meta_pairs) {
        std::string sub_key = InternalKey(meta_pair.first, std::to_string(frag_index * kBitmapSegmentBytes),
                                          meta_pair.second.version, storage_->IsSlotIdEncoded())
                                  .Encode();
        auto s = storage_->Get(read_options, sub_key, &fragment);
        if (!s.ok() && !s.IsNotFound()) {
          return s;
        }
        if (s.IsNotFound()) {
          frag_numkeys--;
          if (op_flag == kBitOpAnd) {
            frag_maxlen = 0;
            break;
          }
        } else {
          if (frag_maxlen < fragment.size()) frag_maxlen = fragment.size();
          if (fragment.size() < frag_minlen || frag_minlen == 0) frag_minlen = fragment.size();
          fragments.emplace_back(std::move(fragment));
        }
      }

      if (frag_maxlen != 0 || op_flag == kBitOpNot) {
        uint16_t j = 0;
        if (op_flag == kBitOpNot) {
          memset(frag_res.get(), UCHAR_MAX, kBitmapSegmentBytes);
        } else {
          memset(frag_res.get(), 0, frag_maxlen);
        }

#ifndef USE_ALIGNED_ACCESS
        if (frag_minlen >= sizeof(uint64_t) * 4 && frag_numkeys <= 16) {
          auto *lres = reinterpret_cast<uint64_t *>(frag_res.get());
          const uint64_t *lp[16];
          for (uint64_t i = 0; i < frag_numkeys; i++) {
            lp[i] = reinterpret_cast<const uint64_t *>(fragments[i].data());
          }
          memcpy(frag_res.get(), fragments[0].data(), frag_minlen);

          if (op_flag == kBitOpAnd) {
            while (frag_minlen >= sizeof(uint64_t) * 4) {
              for (uint64_t i = 1; i < frag_numkeys; i++) {
                lres[0] &= lp[i][0];
                lres[1] &= lp[i][1];
                lres[2] &= lp[i][2];
                lres[3] &= lp[i][3];
                lp[i] += 4;
              }
              lres += 4;
              j += sizeof(uint64_t) * 4;
              frag_minlen -= sizeof(uint64_t) * 4;
            }
          } else if (op_flag == kBitOpOr) {
            while (frag_minlen >= sizeof(uint64_t) * 4) {
              for (uint64_t i = 1; i < frag_numkeys; i++) {
                lres[0] |= lp[i][0];
                lres[1] |= lp[i][1];
                lres[2] |= lp[i][2];
                lres[3] |= lp[i][3];
                lp[i] += 4;
              }
              lres += 4;
              j += sizeof(uint64_t) * 4;
              frag_minlen -= sizeof(uint64_t) * 4;
            }
          } else if (op_flag == kBitOpXor) {
            while (frag_minlen >= sizeof(uint64_t) * 4) {
              for (uint64_t i = 1; i < frag_numkeys; i++) {
                lres[0] ^= lp[i][0];
                lres[1] ^= lp[i][1];
                lres[2] ^= lp[i][2];
                lres[3] ^= lp[i][3];
                lp[i] += 4;
              }
              lres += 4;
              j += sizeof(uint64_t) * 4;
              frag_minlen -= sizeof(uint64_t) * 4;
            }
          } else if (op_flag == kBitOpNot) {
            while (frag_minlen >= sizeof(uint64_t) * 4) {
              lres[0] = ~lres[0];
              lres[1] = ~lres[1];
              lres[2] = ~lres[2];
              lres[3] = ~lres[3];
              lres += 4;
              j += sizeof(uint64_t) * 4;
              frag_minlen -= sizeof(uint64_t) * 4;
            }
          }
        }
#endif

        for (; j < frag_maxlen; j++) {
          output = (fragments[0].size() <= j) ? 0 : fragments[0][j];
          if (op_flag == kBitOpNot) output = ~output;
          for (uint64_t i = 1; i < frag_numkeys; i++) {
            byte = (fragments[i].size() <= j) ? 0 : fragments[i][j];
            switch (op_flag) {
              case kBitOpAnd:
                output &= byte;
                break;
              case kBitOpOr:
                output |= byte;
                break;
              case kBitOpXor:
                output ^= byte;
                break;
              default:
                break;
            }
          }
          frag_res[j] = output;
        }

        if (op_flag == kBitOpNot) {
          if (frag_index == stop_index) {
            if (max_size == (frag_index + 1) * kBitmapSegmentBytes) {
              // If the last fragment is full, `max_size % kBitmapSegmentBytes`
              // would be 0. In this case, we should set `frag_maxlen` to
              // `kBitmapSegmentBytes` to avoid writing an empty fragment.
              frag_maxlen = kBitmapSegmentBytes;
            } else {
              frag_maxlen = max_size % kBitmapSegmentBytes;
            }
          } else {
            frag_maxlen = kBitmapSegmentBytes;
          }
        }
        std::string sub_key = InternalKey(ns_key, std::to_string(frag_index * kBitmapSegmentBytes),
                                          res_metadata.version, storage_->IsSlotIdEncoded())
                                  .Encode();
        batch->Put(sub_key, Slice(reinterpret_cast<char *>(frag_res.get()), frag_maxlen));
      }

      frag_maxlen = 0;
      frag_minlen = 0;
      frag_numkeys = num_keys;
      fragments.clear();
    }
  }

  std::string bytes;
  res_metadata.size = max_size;
  res_metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);
  *len = static_cast<int64_t>(max_size);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

// Copy a range of bytes from entire bitmap and store them into ArrayBitfieldBitmap.
static rocksdb::Status CopySegmentsBytesToBitfield(Bitmap::SegmentCacheStore &store, uint32_t byte_offset,
                                                   uint32_t bytes, ArrayBitfieldBitmap *bitfield) {
  bitfield->SetByteOffset(byte_offset);
  bitfield->Reset();

  uint32_t segment_index = byte_offset / kBitmapSegmentBytes;
  int64_t remain_bytes = bytes;
  // the byte_offset in current segment.
  auto segment_byte_offset = static_cast<int>(byte_offset % kBitmapSegmentBytes);
  for (; remain_bytes > 0; ++segment_index) {
    const std::string *cache = nullptr;
    auto cache_status = store.Get(segment_index, &cache);
    if (!cache_status.ok()) {
      return cache_status;
    }

    auto cache_size = static_cast<int>(cache->size());
    auto copyable = std::max(0, cache_size - segment_byte_offset);
    auto copy_count = std::min(static_cast<int>(remain_bytes), copyable);
    auto src = reinterpret_cast<const uint8_t *>(cache->data() + segment_byte_offset);
    auto status = bitfield->Set(byte_offset, copy_count, src);
    if (!status) {
      return rocksdb::Status::InvalidArgument();
    }

    // next segment will copy from its front.
    byte_offset = (segment_index + 1) * kBitmapSegmentBytes;
    // maybe negative, but still correct.
    remain_bytes -= kBitmapSegmentBytes - segment_byte_offset;
    segment_byte_offset = 0;
  }

  return rocksdb::Status::OK();
}

static rocksdb::Status GetBitfieldInteger(const ArrayBitfieldBitmap &bitfield, uint32_t bit_offset,
                                          BitfieldEncoding enc, uint64_t *res) {
  if (enc.IsSigned()) {
    auto status = bitfield.GetSignedBitfield(bit_offset, enc.Bits());
    if (!status) {
      return rocksdb::Status::InvalidArgument();
    }
    *res = status.GetValue();
  } else {
    auto status = bitfield.GetUnsignedBitfield(bit_offset, enc.Bits());
    if (!status) {
      return rocksdb::Status::InvalidArgument();
    }
    *res = status.GetValue();
  }
  return rocksdb::Status::OK();
}

static rocksdb::Status CopyBitfieldBytesToSegments(Bitmap::SegmentCacheStore &store,
                                                   const ArrayBitfieldBitmap &bitfield, uint32_t byte_offset,
                                                   uint32_t bytes) {
  uint32_t segment_index = byte_offset / kBitmapSegmentBytes;
  auto segment_byte_offset = static_cast<int>(byte_offset % kBitmapSegmentBytes);
  auto remain_bytes = static_cast<int32_t>(bytes);
  for (; remain_bytes > 0; ++segment_index) {
    std::string *cache = nullptr;
    auto cache_status = store.GetMut(segment_index, &cache);
    if (!cache_status.ok()) {
      return cache_status;
    }

    auto copy_count = std::min(remain_bytes, static_cast<int32_t>(kBitmapSegmentBytes - segment_byte_offset));
    if (static_cast<int>(cache->size()) < segment_byte_offset + copy_count) {
      cache->resize(segment_byte_offset + copy_count);
    }

    auto dst = reinterpret_cast<uint8_t *>(cache->data()) + segment_byte_offset;
    auto status = bitfield.Get(byte_offset, copy_count, dst);
    if (!status) {
      return rocksdb::Status::InvalidArgument();
    }

    // next segment will copy from its front.
    byte_offset = (segment_index + 1) * kBitmapSegmentBytes;
    // maybe negative, but still correct.
    remain_bytes -= static_cast<int32_t>(kBitmapSegmentBytes - segment_byte_offset);
    segment_byte_offset = 0;
  }
  return rocksdb::Status::OK();
}

template <bool ReadOnly>
rocksdb::Status Bitmap::bitfield(const Slice &user_key, const std::vector<BitfieldOperation> &ops,
                                 std::vector<std::optional<BitfieldValue>> *rets) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  std::optional<LockGuard> guard;
  if constexpr (!ReadOnly) {
    guard = LockGuard(storage_->GetLockManager(), ns_key);
  }

  BitmapMetadata metadata;
  std::string raw_value;
  auto s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (metadata.Type() == RedisType::kRedisString) {
    if constexpr (ReadOnly) {
      s = BitmapString::BitfieldReadOnly(ns_key, raw_value, ops, rets);
    } else {
      s = BitmapString(storage_, namespace_).Bitfield(ns_key, &raw_value, ops, rets);
    }
    return s;
  }

  if (metadata.Type() != RedisType::kRedisBitmap) {
    return rocksdb::Status::InvalidArgument("The value is not a bitmap or string.");
  }

  // We firstly do the bitfield operation by fetching segments into memory.
  // Use SegmentCacheStore to record dirty segments. (if not read-only mode)
  SegmentCacheStore cache(storage_, metadata_cf_handle_, ns_key, metadata);
  runBitfieldOperationsWithCache<ReadOnly>(cache, ops, rets);

  if constexpr (!ReadOnly) {
    // Write changes into storage.
    auto batch = storage_->GetWriteBatchBase();
    if (bitfieldWriteAheadLog(batch, ops)) {
      cache.BatchForFlush(batch);
      return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    }
  }
  return rocksdb::Status::OK();
}

template <bool ReadOnly>
rocksdb::Status Bitmap::runBitfieldOperationsWithCache(SegmentCacheStore &cache,
                                                       const std::vector<BitfieldOperation> &ops,
                                                       std::vector<std::optional<BitfieldValue>> *rets) {
  ArrayBitfieldBitmap bitfield;
  for (BitfieldOperation op : ops) {
    // found all bytes that contents the bitfield.
    uint32_t first_byte = op.offset / 8;
    uint32_t last_bytes = (op.offset + op.encoding.Bits() - 1) / 8 + 1;
    uint32_t bytes = last_bytes - first_byte;

    auto segment_status = CopySegmentsBytesToBitfield(cache, first_byte, bytes, &bitfield);
    if (!segment_status.ok()) {
      return segment_status;
    }

    // Covert the bitfield from a buffer to an integer.
    uint64_t unsigned_old_value = 0;
    auto s = GetBitfieldInteger(bitfield, op.offset, op.encoding, &unsigned_old_value);
    if (!s.ok()) {
      return s;
    }

    if constexpr (ReadOnly) {
      rets->emplace_back() = {op.encoding, unsigned_old_value};
      continue;
    }

    auto &ret = rets->emplace_back();
    uint64_t unsigned_new_value = 0;
    // BitfieldOp failed only when the length or bits illegal.
    // BitfieldOperation already check above case in construction function.
    if (BitfieldOp(op, unsigned_old_value, &unsigned_new_value).GetValue()) {
      if (op.type != BitfieldOperation::Type::kGet) {
        Status _ = bitfield.SetBitfield(op.offset, op.encoding.Bits(), unsigned_new_value);
        s = CopyBitfieldBytesToSegments(cache, bitfield, first_byte, bytes);
        if (!s.ok()) {
          return s;
        }
      }

      if (op.type == BitfieldOperation::Type::kSet) {
        unsigned_new_value = unsigned_old_value;
      }

      ret = {op.encoding, unsigned_new_value};
    }
  }

  return rocksdb::Status::OK();
}

template rocksdb::Status Bitmap::bitfield<false>(const Slice &, const std::vector<BitfieldOperation> &,
                                                 std::vector<std::optional<BitfieldValue>> *);
template rocksdb::Status Bitmap::bitfield<true>(const Slice &, const std::vector<BitfieldOperation> &,
                                                std::vector<std::optional<BitfieldValue>> *);

// Return true if there are any write operation to bitmap. Otherwise return false.
bool Bitmap::bitfieldWriteAheadLog(const ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                   const std::vector<BitfieldOperation> &ops) {
  std::vector<std::string> cmd_args{std::to_string(kRedisCmdBitfield)};
  auto current_overflow = BitfieldOverflowBehavior::kWrap;
  for (BitfieldOperation op : ops) {
    if (op.type == BitfieldOperation::Type::kGet) {
      continue;
    }
    if (current_overflow != op.overflow) {
      current_overflow = op.overflow;
      std::string overflow_str;
      switch (op.overflow) {
        case BitfieldOverflowBehavior::kWrap:
          overflow_str = "WRAP";
          break;
        case BitfieldOverflowBehavior::kSat:
          overflow_str = "SAT";
          break;
        case BitfieldOverflowBehavior::kFail:
          overflow_str = "FAIL";
          break;
      }
      cmd_args.emplace_back("OVERFLOW");
      cmd_args.emplace_back(std::move(overflow_str));
    }

    if (op.type == BitfieldOperation::Type::kSet) {
      cmd_args.emplace_back("SET");
    } else {
      cmd_args.emplace_back("INCRBY");
    }
    cmd_args.push_back(op.encoding.ToString());
    cmd_args.push_back(std::to_string(op.offset));
    if (op.type == BitfieldOperation::Type::kSet) {
      if (op.encoding.IsSigned()) {
        cmd_args.push_back(std::to_string(op.value));
      } else {
        cmd_args.push_back(std::to_string(static_cast<uint64_t>(op.value)));
      }
    } else {
      cmd_args.push_back(std::to_string(op.value));
    }
  }

  if (cmd_args.size() > 1) {
    WriteBatchLogData log_data(kRedisBitmap, std::move(cmd_args));
    batch->PutLogData(log_data.Encode());
    return true;
  }
  return false;
}

bool Bitmap::GetBitFromValueAndOffset(std::string_view value, uint32_t bit_offset) {
  bool bit = false;
  uint32_t byte_index = (bit_offset / 8) % kBitmapSegmentBytes;
  if (byte_index < value.size() &&
      util::lsb::GetBit(reinterpret_cast<const uint8_t *>(value.data()), bit_offset % kBitmapSegmentBits)) {
    bit = true;
  }
  return bit;
}

bool Bitmap::IsEmptySegment(const Slice &segment) {
  static const char zero_byte_segment[kBitmapSegmentBytes] = {0};
  return !memcmp(zero_byte_segment, segment.data(), segment.size());
}
}  // namespace redis
