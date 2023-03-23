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
#include <utility>
#include <vector>

#include "db_util.h"
#include "parse_util.h"
#include "redis_bitmap_string.h"

namespace Redis {

const uint32_t kBitmapSegmentBits = 1024 * 8;
const uint32_t kBitmapSegmentBytes = 1024;

const char kErrBitmapStringOutOfRange[] =
    "The size of the bitmap string exceeds the "
    "configuration item max-bitmap-to-string-mb";

extern const uint8_t kNum2Bits[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2,
    3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3,
    3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5,
    6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
    3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4,
    5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
    6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

rocksdb::Status Bitmap::GetMetadata(const Slice &ns_key, BitmapMetadata *metadata, std::string *raw_value) {
  std::string old_metadata;
  metadata->Encode(&old_metadata);
  auto s = GetRawMetadata(ns_key, raw_value);
  if (!s.ok()) return s;
  metadata->Decode(*raw_value);

  if (metadata->Expired()) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound(kErrMsgKeyExpired);
  }
  if (metadata->Type() == kRedisString) return s;
  if (metadata->Type() != kRedisBitmap && metadata->size > 0) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::InvalidArgument(kErrMsgWrongType);
  }
  if (metadata->size == 0) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound("no elements");
  }
  return s;
}

rocksdb::Status Bitmap::GetBit(const Slice &user_key, uint32_t offset, bool *bit) {
  *bit = false;
  std::string ns_key, raw_value;
  AppendNamespacePrefix(user_key, &ns_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (metadata.Type() == kRedisString) {
    Redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.GetBit(raw_value, offset, bit);
  }

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  uint32_t index = (offset / kBitmapSegmentBits) * kBitmapSegmentBytes;
  std::string sub_key, value;
  InternalKey(ns_key, std::to_string(index), metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  s = storage_->Get(read_options, sub_key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  uint32_t byte_index = (offset / 8) % kBitmapSegmentBytes;
  if ((byte_index < value.size() && (value[byte_index] & (1 << (offset % 8))))) {
    *bit = true;
  }
  return rocksdb::Status::OK();
}

// Use this function after careful estimation, and reserve enough memory
// according to the max size of the bitmap string to prevent OOM.
rocksdb::Status Bitmap::GetString(const Slice &user_key, const uint32_t max_btos_size, std::string *value) {
  value->clear();
  std::string ns_key, raw_value;
  AppendNamespacePrefix(user_key, &ns_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok()) return s;
  if (metadata.size > max_btos_size) {
    return rocksdb::Status::Aborted(kErrBitmapStringOutOfRange);
  }
  value->assign(metadata.size, 0);

  std::string fragment, prefix_key;
  fragment.reserve(kBitmapSegmentBytes * 2);
  InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode(&prefix_key);

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  storage_->SetReadOptions(read_options);

  auto iter = DBUtil::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    auto parse_result = ParseInt<uint32_t>(ikey.GetSubKey().ToString(), 10);
    if (!parse_result) {
      return rocksdb::Status::InvalidArgument(parse_result.Msg());
    }
    uint32_t frag_index = *parse_result;
    fragment = iter->value().ToString();
    // To be compatible with data written before the commit d603b0e(#338)
    // and avoid returning extra null char after expansion.
    uint32_t valid_size = std::min(
        {fragment.size(), static_cast<size_t>(kBitmapSegmentBytes), static_cast<size_t>(metadata.size - frag_index)});

    /*
     * If you setbit bit 0 1, the value is stored as 0x01 in Kvrocks but 0x80 in Redis.
     * So we need to swap bits is to keep the same return value as Redis.
     * This swap table is generated according to the following mapping definition.
     * swap_table(x) =  ((x & 0x80) >> 7)| ((x & 0x40) >> 5)|\
     *                  ((x & 0x20) >> 3)| ((x & 0x10) >> 1)|\
     *                  ((x & 0x08) << 1)| ((x & 0x04) << 3)|\
     *                  ((x & 0x02) << 5)| ((x & 0x01) << 7);
     */
    static const uint8_t swap_table[256] = {
        0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0, 0x08, 0x88,
        0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8, 0x04, 0x84, 0x44, 0xC4,
        0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4, 0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC,
        0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC, 0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2,
        0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2, 0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A,
        0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA, 0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6,
        0x36, 0xB6, 0x76, 0xF6, 0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE,
        0x7E, 0xFE, 0x01, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
        0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9, 0x05, 0x85,
        0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5, 0x0D, 0x8D, 0x4D, 0xCD,
        0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD, 0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3,
        0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3, 0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB,
        0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB, 0x07, 0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97,
        0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7, 0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF,
        0x3F, 0xBF, 0x7F, 0xFF};
    for (uint32_t i = 0; i < valid_size; i++) {
      if (!fragment[i]) continue;
      fragment[i] = static_cast<char>(swap_table[static_cast<uint8_t>(fragment[i])]);
    }
    value->replace(frag_index, valid_size, fragment.data(), valid_size);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Bitmap::SetBit(const Slice &user_key, uint32_t offset, bool new_bit, bool *old_bit) {
  std::string ns_key, raw_value;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  BitmapMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;

  if (metadata.Type() == kRedisString) {
    Redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.SetBit(ns_key, &raw_value, offset, new_bit, old_bit);
  }

  std::string sub_key, value;
  uint32_t index = (offset / kBitmapSegmentBits) * kBitmapSegmentBytes;
  InternalKey(ns_key, std::to_string(index), metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  if (s.ok()) {
    s = storage_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
  }
  uint32_t byte_index = (offset / 8) % kBitmapSegmentBytes;
  uint64_t used_size = index + byte_index + 1;
  uint64_t bitmap_size = std::max(used_size, metadata.size);
  if (byte_index >= value.size()) {  // expand the bitmap
    size_t expand_size = 0;
    if (byte_index >= value.size() * 2) {
      expand_size = byte_index - value.size() + 1;
    } else if (value.size() * 2 > kBitmapSegmentBytes) {
      expand_size = kBitmapSegmentBytes - value.size();
    } else {
      expand_size = value.size();
    }
    value.append(expand_size, 0);
  }
  uint32_t bit_offset = offset % 8;
  *old_bit = (value[byte_index] & (1 << bit_offset)) != 0;
  if (new_bit) {
    value[byte_index] = static_cast<char>(value[byte_index] | (1 << bit_offset));
  } else {
    value[byte_index] = static_cast<char>(value[byte_index] & (~(1 << bit_offset)));
  }
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBitmap, {std::to_string(kRedisCmdSetBit), std::to_string(offset)});
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

rocksdb::Status Bitmap::BitCount(const Slice &user_key, int64_t start, int64_t stop, uint32_t *cnt) {
  *cnt = 0;
  std::string ns_key, raw_value;
  AppendNamespacePrefix(user_key, &ns_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (metadata.Type() == kRedisString) {
    Redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.BitCount(raw_value, start, stop, cnt);
  }

  if (start < 0) start += static_cast<int64_t>(metadata.size) + 1;
  if (stop < 0) stop += static_cast<int64_t>(metadata.size) + 1;
  if (stop > static_cast<int64_t>(metadata.size)) stop = static_cast<int64_t>(metadata.size);
  if (start < 0 || stop <= 0 || start >= stop) return rocksdb::Status::OK();

  auto u_start = static_cast<uint32_t>(start);
  auto u_stop = static_cast<uint32_t>(stop);

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  uint32_t start_index = u_start / kBitmapSegmentBytes;
  uint32_t stop_index = u_stop / kBitmapSegmentBytes;
  // Don't use multi get to prevent large range query, and take too much memory
  std::string sub_key, value;
  for (uint32_t i = start_index; i <= stop_index; i++) {
    InternalKey(ns_key, std::to_string(i * kBitmapSegmentBytes), metadata.version, storage_->IsSlotIdEncoded())
        .Encode(&sub_key);
    s = storage_->Get(read_options, sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) continue;
    size_t j = 0;
    if (i == start_index) j = u_start % kBitmapSegmentBytes;
    for (; j < value.size(); j++) {
      if (i == stop_index && j > (u_stop % kBitmapSegmentBytes)) break;
      *cnt += kNum2Bits[static_cast<uint8_t>(value[j])];
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Bitmap::BitPos(const Slice &user_key, bool bit, int64_t start, int64_t stop, bool stop_given,
                               int64_t *pos) {
  std::string ns_key, raw_value;
  AppendNamespacePrefix(user_key, &ns_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    *pos = bit ? -1 : 0;
    return rocksdb::Status::OK();
  }

  if (metadata.Type() == kRedisString) {
    Redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.BitPos(raw_value, bit, start, stop, stop_given, pos);
  }

  if (start < 0) start += static_cast<int64_t>(metadata.size) + 1;
  if (stop < 0) stop += static_cast<int64_t>(metadata.size) + 1;
  if (start < 0 || stop < 0 || start > stop) {
    *pos = -1;
    return rocksdb::Status::OK();
  }
  auto u_start = static_cast<uint32_t>(start);
  auto u_stop = static_cast<uint32_t>(stop);

  auto bitPosInByte = [](char byte, bool bit) -> int {
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
  std::string sub_key, value;
  for (uint32_t i = start_index; i <= stop_index; i++) {
    InternalKey(ns_key, std::to_string(i * kBitmapSegmentBytes), metadata.version, storage_->IsSlotIdEncoded())
        .Encode(&sub_key);
    s = storage_->Get(read_options, sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      if (!bit) {
        *pos = i * kBitmapSegmentBits;
        return rocksdb::Status::OK();
      }
      continue;
    }
    size_t j = 0;
    if (i == start_index) j = u_start % kBitmapSegmentBytes;
    for (; j < value.size(); j++) {
      if (i == stop_index && j > (u_stop % kBitmapSegmentBytes)) break;
      if (bitPosInByte(value[j], bit) != -1) {
        *pos = static_cast<int64_t>(i * kBitmapSegmentBits + j * 8 + bitPosInByte(value[j], bit));
        return rocksdb::Status::OK();
      }
    }
    if (!bit && value.size() < kBitmapSegmentBytes) {
      *pos = static_cast<int64_t>(i * kBitmapSegmentBits + value.size() * 8);
      return rocksdb::Status::OK();
    }
  }
  // bit was not found
  *pos = bit ? -1 : static_cast<int64_t>(metadata.size * 8);
  return rocksdb::Status::OK();
}

rocksdb::Status Bitmap::BitOp(BitOpFlags op_flag, const std::string &op_name, const Slice &user_key,
                              const std::vector<Slice> &op_keys, int64_t *len) {
  std::string ns_key, raw_value, ns_op_key;
  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  std::vector<std::pair<std::string, BitmapMetadata>> meta_pairs;
  uint64_t max_size = 0, num_keys = op_keys.size();

  for (const auto &op_key : op_keys) {
    BitmapMetadata metadata(false);
    AppendNamespacePrefix(op_key, &ns_op_key);
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
    meta_pairs.emplace_back(ns_op_key, metadata);
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
    uint64_t frag_numkeys = num_keys, stop_index = (max_size - 1) / kBitmapSegmentBytes;
    std::unique_ptr<unsigned char[]> frag_res(new unsigned char[kBitmapSegmentBytes]);
    uint16_t frag_maxlen = 0, frag_minlen = 0;
    std::string sub_key, fragment;
    unsigned char output = 0, byte = 0;
    std::vector<std::string> fragments;

    LatestSnapShot ss(storage_);
    rocksdb::ReadOptions read_options;
    read_options.snapshot = ss.GetSnapShot();
    for (uint64_t frag_index = 0; frag_index <= stop_index; frag_index++) {
      for (const auto &meta_pair : meta_pairs) {
        InternalKey(meta_pair.first, std::to_string(frag_index * kBitmapSegmentBytes), meta_pair.second.version,
                    storage_->IsSlotIdEncoded())
            .Encode(&sub_key);
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
          fragments.emplace_back(fragment);
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
            frag_maxlen = max_size % kBitmapSegmentBytes;
          } else {
            frag_maxlen = kBitmapSegmentBytes;
          }
        }
        InternalKey(ns_key, std::to_string(frag_index * kBitmapSegmentBytes), res_metadata.version,
                    storage_->IsSlotIdEncoded())
            .Encode(&sub_key);
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

bool Bitmap::GetBitFromValueAndOffset(const std::string &value, uint32_t offset) {
  bool bit = false;
  uint32_t byte_index = (offset / 8) % kBitmapSegmentBytes;
  if ((byte_index < value.size() && (value[byte_index] & (1 << (offset % 8))))) {
    bit = true;
  }
  return bit;
}

bool Bitmap::IsEmptySegment(const Slice &segment) {
  static const char zero_byte_segment[kBitmapSegmentBytes] = {0};
  return !memcmp(zero_byte_segment, segment.data(), segment.size());
}
}  // namespace Redis
