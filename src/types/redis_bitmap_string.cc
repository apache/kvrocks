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

#include "redis_bitmap_string.h"

#include <endian.h>
#include <glog/logging.h>

#include <cstdint>

#include "redis_string.h"
#include "storage/redis_metadata.h"
#include "type_util.h"

namespace Redis {

rocksdb::Status BitmapString::GetBit(const std::string &raw_value, uint32_t offset, bool *bit) {
  auto string_value = raw_value.substr(Metadata::GetOffsetAfterExpire(raw_value[0]));
  uint32_t byte_index = offset >> 3;
  uint32_t bit_val = 0;
  uint32_t bit_offset = 7 - (offset & 0x7);
  if (byte_index < string_value.size()) {
    bit_val = string_value[byte_index] & (1 << bit_offset);
  }
  *bit = bit_val != 0;
  return rocksdb::Status::OK();
}

rocksdb::Status BitmapString::SetBit(const Slice &ns_key, std::string *raw_value, uint32_t offset, bool new_bit,
                                     bool *old_bit) {
  size_t header_offset = Metadata::GetOffsetAfterExpire((*raw_value)[0]);
  auto string_value = raw_value->substr(header_offset);
  uint32_t byte_index = offset >> 3;
  if (byte_index >= string_value.size()) {  // expand the bitmap
    string_value.append(byte_index - string_value.size() + 1, 0);
  }
  uint32_t bit_offset = 7 - (offset & 0x7);
  auto byteval = string_value[byte_index];
  *old_bit = (byteval & (1 << bit_offset)) != 0;

  byteval = static_cast<char>(byteval & (~(1 << bit_offset)));
  byteval = static_cast<char>(byteval | ((new_bit & 0x1) << bit_offset));
  string_value[byte_index] = byteval;

  *raw_value = raw_value->substr(0, header_offset);
  raw_value->append(string_value);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisString);
  batch->PutLogData(log_data.Encode());
  batch->Put(metadata_cf_handle_, ns_key, *raw_value);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BitmapString::BitCount(const std::string &raw_value, int64_t start, int64_t stop, uint32_t *cnt) {
  *cnt = 0;
  auto string_value = raw_value.substr(Metadata::GetOffsetAfterExpire(raw_value[0]));
  /* Convert negative indexes */
  if (start < 0 && stop < 0 && start > stop) {
    return rocksdb::Status::OK();
  }
  auto strlen = static_cast<int64_t>(string_value.size());
  if (start < 0) start = strlen + start;
  if (stop < 0) stop = strlen + stop;
  if (start < 0) start = 0;
  if (stop < 0) stop = 0;
  if (stop >= strlen) stop = strlen - 1;

  /* Precondition: end >= 0 && end < strlen, so the only condition where
   * zero can be returned is: start > stop. */
  if (start <= stop) {
    int64_t bytes = stop - start + 1;
    *cnt = redisPopcount(reinterpret_cast<const uint8_t *>(string_value.data()) + start, bytes);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status BitmapString::BitPos(const std::string &raw_value, bool bit, int64_t start, int64_t stop,
                                     bool stop_given, int64_t *pos) {
  auto string_value = raw_value.substr(Metadata::GetOffsetAfterExpire(raw_value[0]));
  auto strlen = static_cast<int64_t>(string_value.size());
  /* Convert negative indexes */
  if (start < 0) start = strlen + start;
  if (stop < 0) stop = strlen + stop;
  if (start < 0) start = 0;
  if (stop < 0) stop = 0;
  if (stop >= strlen) stop = strlen - 1;

  if (start > stop) {
    *pos = -1;
  } else {
    int64_t bytes = stop - start + 1;
    *pos = redisBitpos(reinterpret_cast<const uint8_t *>(string_value.data()) + start, bytes, bit);

    /* If we are looking for clear bits, and the user specified an exact
     * range with start-end, we can't consider the right of the range as
     * zero padded (as we do when no explicit end is given).
     *
     * So if redisBitpos() returns the first bit outside the range,
     * we return -1 to the caller, to mean, in the specified range there
     * is not a single "0" bit. */
    if (stop_given && bit == 0 && *pos == bytes * 8) {
      *pos = -1;
      return rocksdb::Status::OK();
    }
    if (*pos != -1) *pos += start * 8; /* Adjust for the bytes we skipped. */
  }
  return rocksdb::Status::OK();
}

/* Count number of bits set in the binary array pointed by 's' and long
 * 'count' bytes. The implementation of this function is required to
 * work with a input string length up to 512 MB.
 * */
size_t BitmapString::redisPopcount(const uint8_t *p, int64_t count) {
  size_t bits = 0;

  for (; count >= 8; p += 8, count -= 8) {
    bits += __builtin_popcountll(*reinterpret_cast<const uint64_t *>(p));
  }

  if (count > 0) {
    uint64_t v = 0;
    __builtin_memcpy(&v, p, count);
    bits += __builtin_popcountll(v);
  }

  return bits;
}

template <typename T = void>
inline int clzllWithEndian(uint64_t x) {
  if constexpr (IsLittleEndian()) {
    return __builtin_clzll(__builtin_bswap64(x));
  } else if constexpr (IsBigEndian()) {
    return __builtin_clzll(x);
  } else {
    static_assert(AlwaysFalse<T>);
  }
}

/* Return the position of the first bit set to one (if 'bit' is 1) or
 * zero (if 'bit' is 0) in the bitmap starting at 's' and long 'count' bytes.
 *
 * The function is guaranteed to return a value >= 0 if 'bit' is 0 since if
 * no zero bit is found, it returns count*8 assuming the string is zero
 * padded on the right. However if 'bit' is 1 it is possible that there is
 * not a single set bit in the bitmap. In this special case -1 is returned.
 * */
int64_t BitmapString::redisBitpos(const uint8_t *c, int64_t count, bool bit) {
  int64_t res = 0;

  if (bit) {
    int64_t ct = count;

    for (; count >= 8; c += 8, count -= 8) {
      uint64_t x = *reinterpret_cast<const uint64_t *>(c);
      if (x != 0) {
        return res + clzllWithEndian(x);
      }
      res += 64;
    }

    if (count > 0) {
      uint64_t v = 0;
      __builtin_memcpy(&v, c, count);
      res += v == 0 ? count * 8 : clzllWithEndian(v);
    }

    if (res == ct * 8) {
      return -1;
    }
  } else {
    for (; count >= 8; c += 8, count -= 8) {
      uint64_t x = *reinterpret_cast<const uint64_t *>(c);
      if (x != (uint64_t)-1) {
        return res + clzllWithEndian(~x);
      }
      res += 64;
    }

    if (count > 0) {
      uint64_t v = -1;
      __builtin_memcpy(&v, c, count);
      res += v == (uint64_t)-1 ? count * 8 : clzllWithEndian(~v);
    }
  }

  return res;
}

}  // namespace Redis
