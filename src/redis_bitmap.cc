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
#include <vector>
#include <memory>
#include <utility>
#include <algorithm>

#include "redis_bitmap_string.h"

namespace Redis {

const uint32_t kBitmapSegmentBits = 1024 * 8;
const uint32_t kBitmapSegmentBytes = 1024;

const char kErrBitmapStringOutOfRange[] = "The size of the bitmap string exceeds the "
                                          "configuration item max-bitmap-to-string-mb";

uint32_t kNum2Bits[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};

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

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  uint32_t index = (offset / kBitmapSegmentBits) * kBitmapSegmentBytes;
  std::string sub_key, value;
  InternalKey(ns_key, std::to_string(index), metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  s = db_->Get(read_options, sub_key, &value);
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
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  uint32_t frag_index, valid_size;

  auto iter = db_->NewIterator(read_options);
  for (iter->Seek(prefix_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    frag_index = std::stoul(ikey.GetSubKey().ToString());
    fragment = iter->value().ToString();
    // To be compatible with data written before the commit d603b0e(#338)
    // and avoid returning extra null char after expansion.
    valid_size = std::min({fragment.size(),
                          static_cast<size_t>(kBitmapSegmentBytes),
                          static_cast<size_t>(metadata.size - frag_index)});

    //  If you setbit bit 0 1, the value is stored as 0x01 in Kvocks but 0x80 in Redis.
    // So we need to swap bits is to keep the same return value as Redis.
    for (uint32_t i = 0; i < valid_size; i++) {
        if (!fragment[i]) continue;
        fragment[i] = ((fragment[i] & 0x80) >> 7)|\
                      ((fragment[i] & 0x40) >> 5)|\
                      ((fragment[i] & 0x20) >> 3)|\
                      ((fragment[i] & 0x10) >> 1)|\
                      ((fragment[i] & 0x08) << 1)|\
                      ((fragment[i] & 0x04) << 3)|\
                      ((fragment[i] & 0x02) << 5)|\
                      ((fragment[i] & 0x01) << 7);
    }
    value->replace(frag_index, valid_size, fragment.data(), valid_size);
  }
  delete iter;
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
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
  }
  uint32_t byte_index = (offset / 8) % kBitmapSegmentBytes;
  uint32_t used_size = index + byte_index + 1;
  uint32_t bitmap_size = std::max(used_size, metadata.size);
  if (byte_index >= value.size()) {  // expand the bitmap
    size_t expand_size;
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
    value[byte_index] |= 1 << bit_offset;
  } else {
    value[byte_index] &= ~(1 << bit_offset);
  }
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisBitmap, {std::to_string(kRedisCmdSetBit), std::to_string(offset)});
  batch.PutLogData(log_data.Encode());
  batch.Put(sub_key, value);
  if (metadata.size != bitmap_size) {
    metadata.size = bitmap_size;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
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

  if (start < 0) start += metadata.size + 1;
  if (stop < 0) stop += metadata.size + 1;
  if (stop > static_cast<int64_t>(metadata.size)) stop = metadata.size;
  if (start < 0 || stop <= 0 || start >= stop) return rocksdb::Status::OK();

  uint32_t u_start = static_cast<uint32_t>(start);
  uint32_t u_stop = static_cast<uint32_t>(stop);

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  uint32_t start_index = u_start / kBitmapSegmentBytes;
  uint32_t stop_index = u_stop / kBitmapSegmentBytes;
  // Don't use multi get to prevent large range query, and take too much memory
  std::string sub_key, value;
  for (uint32_t i = start_index; i <= stop_index; i++) {
    InternalKey(ns_key, std::to_string(i * kBitmapSegmentBytes), metadata.version,
                  storage_->IsSlotIdEncoded()).Encode(&sub_key);
    s = db_->Get(read_options, sub_key, &value);
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

rocksdb::Status Bitmap::BitPos(const Slice &user_key, bool bit, int64_t start,
                                  int64_t stop, bool stop_given, int64_t *pos) {
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

  if (start < 0) start += metadata.size + 1;
  if (stop < 0) stop += metadata.size + 1;
  if (start < 0 || stop < 0 || start > stop) {
    *pos = -1;
    return rocksdb::Status::OK();
  }
  uint32_t u_start = static_cast<uint32_t>(start);
  uint32_t u_stop = static_cast<uint32_t>(stop);

  auto bitPosInByte = [](char byte, bool bit) -> int {
    for (int i = 0; i < 8; i++) {
      if (bit && (byte & (1 << i)) != 0) return i;
      if (!bit && (byte & (1 << i)) == 0) return i;
    }
    return -1;
  };

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  uint32_t start_index = u_start / kBitmapSegmentBytes;
  uint32_t stop_index = u_stop / kBitmapSegmentBytes;
  // Don't use multi get to prevent large range query, and take too much memory
  std::string sub_key, value;
  for (uint32_t i = start_index; i <= stop_index; i++) {
    InternalKey(ns_key, std::to_string(i * kBitmapSegmentBytes), metadata.version,
                  storage_->IsSlotIdEncoded()).Encode(&sub_key);
    s = db_->Get(read_options, sub_key, &value);
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

rocksdb::Status Bitmap::BitOp(BitOpFlags op_flag, const std::string &op_name,
                              const Slice &user_key, const std::vector<Slice> &op_keys, int64_t *len) {
  std::string ns_key, raw_value, ns_op_key;
  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  rocksdb::Status s;
  std::vector<std::pair<std::string, BitmapMetadata>> meta_pairs;
  uint64_t max_size = 0, num_keys = op_keys.size();

  for (const auto &op_key : op_keys) {
    BitmapMetadata metadata(false);
    AppendNamespacePrefix(op_key, &ns_op_key);
    s = GetMetadata(ns_op_key, &metadata, &raw_value);
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
    meta_pairs.emplace_back(std::make_pair(ns_op_key, metadata));
  }

  rocksdb::WriteBatch batch;
  if (max_size == 0) {
    batch.Delete(metadata_cf_handle_, ns_key);
    return storage_->Write(rocksdb::WriteOptions(), &batch);
  }
  std::vector<std::string> log_args = {std::to_string(kRedisCmdBitOp), op_name};
  for (const auto &op_key : op_keys) {
    log_args.emplace_back(op_key.ToString());
  }
  WriteBatchLogData log_data(kRedisBitmap, std::move(log_args));
  batch.PutLogData(log_data.Encode());

  BitmapMetadata res_metadata;
  if (num_keys == op_keys.size() || op_flag != kBitOpAnd) {
    uint64_t i, frag_numkeys = num_keys, stop_index = (max_size -1)/kBitmapSegmentBytes;
    std::unique_ptr<unsigned char[]> frag_res(new unsigned char[kBitmapSegmentBytes]);
    uint16_t frag_maxlen = 0, frag_minlen = 0;
    std::string sub_key, fragment;
    unsigned char output, byte;
    std::vector<std::string> fragments;

    LatestSnapShot ss(db_);
    rocksdb::ReadOptions read_options;
    read_options.snapshot = ss.GetSnapShot();
    for (uint64_t frag_index = 0; frag_index <= stop_index; frag_index++) {
      for (const auto &meta_pair : meta_pairs) {
        InternalKey(meta_pair.first, std::to_string(frag_index * kBitmapSegmentBytes),
                  meta_pair.second.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
        auto s = db_->Get(read_options, sub_key, &fragment);
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
        if (frag_minlen >= sizeof(uint64_t)*4 && frag_numkeys <= 16) {
          uint64_t *lres = reinterpret_cast<uint64_t*>(frag_res.get());
          const uint64_t *lp[16];
          for (i = 0; i < frag_numkeys; i++) {
            lp[i] = reinterpret_cast<const uint64_t*>(fragments[i].data());
          }
          memcpy(frag_res.get(), fragments[0].data(), frag_minlen);

          if (op_flag == kBitOpAnd) {
              while (frag_minlen >= sizeof(uint64_t)*4) {
                for (i = 1; i < frag_numkeys; i++) {
                    lres[0] &= lp[i][0];
                    lres[1] &= lp[i][1];
                    lres[2] &= lp[i][2];
                    lres[3] &= lp[i][3];
                    lp[i]+=4;
                }
                lres+=4;
                j += sizeof(uint64_t)*4;
                frag_minlen -= sizeof(uint64_t)*4;
              }
          } else if (op_flag == kBitOpOr) {
              while (frag_minlen >= sizeof(uint64_t)*4) {
                for (i = 1; i < frag_numkeys; i++) {
                    lres[0] |= lp[i][0];
                    lres[1] |= lp[i][1];
                    lres[2] |= lp[i][2];
                    lres[3] |= lp[i][3];
                    lp[i]+=4;
                }
                lres+=4;
                j += sizeof(uint64_t)*4;
                frag_minlen -= sizeof(uint64_t)*4;
              }
          } else if (op_flag == kBitOpXor) {
              while (frag_minlen >= sizeof(uint64_t)*4) {
                for (i = 1; i < frag_numkeys; i++) {
                    lres[0] ^= lp[i][0];
                    lres[1] ^= lp[i][1];
                    lres[2] ^= lp[i][2];
                    lres[3] ^= lp[i][3];
                    lp[i]+=4;
                }
                lres+=4;
                j += sizeof(uint64_t)*4;
                frag_minlen -= sizeof(uint64_t)*4;
              }
          } else if (op_flag == kBitOpNot) {
              while (frag_minlen >= sizeof(uint64_t)*4) {
                  lres[0] = ~lres[0];
                  lres[1] = ~lres[1];
                  lres[2] = ~lres[2];
                  lres[3] = ~lres[3];
                  lres+=4;
                  j += sizeof(uint64_t)*4;
                  frag_minlen -= sizeof(uint64_t)*4;
              }
          }
       }
       #endif

        for (; j < frag_maxlen; j++) {
          output = (fragments[0].size() <= j) ? 0 : fragments[0][j];
          if (op_flag == kBitOpNot) output = ~output;
          for (i = 1; i < frag_numkeys; i++) {
            byte = (fragments[i].size() <= j) ? 0 : fragments[i][j];
            switch (op_flag) {
              case kBitOpAnd: output &= byte; break;
              case kBitOpOr:  output |= byte; break;
              case kBitOpXor: output ^= byte; break;
              default: break;
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
        InternalKey(ns_key, std::to_string(frag_index * kBitmapSegmentBytes),
                    res_metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
        batch.Put(sub_key, Slice(reinterpret_cast<char*>(frag_res.get()), frag_maxlen));
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
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  *len = max_size;
  return storage_->Write(rocksdb::WriteOptions(), &batch);
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
  std::string value = segment.ToString();
  return !memcmp(zero_byte_segment, value.c_str(), value.size());
}
}  // namespace Redis
