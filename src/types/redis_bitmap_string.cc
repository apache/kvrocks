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

#include <glog/logging.h>

#include "redis_string.h"
#include "storage/redis_metadata.h"

namespace Redis {

extern const uint8_t kNum2Bits[256];

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
    *cnt = redisPopcount((unsigned char *)(&string_value[0] + start), bytes);
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
    *pos = redisBitpos((unsigned char *)(&string_value[0] + start), bytes, bit);

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
 *
 * This is a function from the redis project.
 * This function started out as:
 * https://github.com/antirez/redis/blob/94f2e7f/src/bitops.c#L40
 * */
size_t BitmapString::redisPopcount(unsigned char *p, int64_t count) {
  size_t bits = 0;

  /* Count initial bytes not aligned to 32 bit. */
  while (reinterpret_cast<uint64_t>(p) & 3 && count) {
    bits += kNum2Bits[*p++];
    count--;
  }

  /* Count bits 28 bytes at a time */
  auto p4 = reinterpret_cast<uint32_t *>(p);
  while (count >= 28) {
    uint32_t aux1 = *p4++;
    uint32_t aux2 = *p4++;
    uint32_t aux3 = *p4++;
    uint32_t aux4 = *p4++;
    uint32_t aux5 = *p4++;
    uint32_t aux6 = *p4++;
    uint32_t aux7 = *p4++;
    count -= 28;

    aux1 = aux1 - ((aux1 >> 1) & 0x55555555);
    aux1 = (aux1 & 0x33333333) + ((aux1 >> 2) & 0x33333333);
    aux2 = aux2 - ((aux2 >> 1) & 0x55555555);
    aux2 = (aux2 & 0x33333333) + ((aux2 >> 2) & 0x33333333);
    aux3 = aux3 - ((aux3 >> 1) & 0x55555555);
    aux3 = (aux3 & 0x33333333) + ((aux3 >> 2) & 0x33333333);
    aux4 = aux4 - ((aux4 >> 1) & 0x55555555);
    aux4 = (aux4 & 0x33333333) + ((aux4 >> 2) & 0x33333333);
    aux5 = aux5 - ((aux5 >> 1) & 0x55555555);
    aux5 = (aux5 & 0x33333333) + ((aux5 >> 2) & 0x33333333);
    aux6 = aux6 - ((aux6 >> 1) & 0x55555555);
    aux6 = (aux6 & 0x33333333) + ((aux6 >> 2) & 0x33333333);
    aux7 = aux7 - ((aux7 >> 1) & 0x55555555);
    aux7 = (aux7 & 0x33333333) + ((aux7 >> 2) & 0x33333333);
    bits += ((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) + ((aux2 + (aux2 >> 4)) & 0x0F0F0F0F) +
              ((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) + ((aux4 + (aux4 >> 4)) & 0x0F0F0F0F) +
              ((aux5 + (aux5 >> 4)) & 0x0F0F0F0F) + ((aux6 + (aux6 >> 4)) & 0x0F0F0F0F) +
              ((aux7 + (aux7 >> 4)) & 0x0F0F0F0F)) *
             0x01010101) >>
            24;
  }
  /* Count the remaining bytes. */
  p = (unsigned char *)p4;
  while (count--) bits += kNum2Bits[*p++];
  return bits;
}

/* Return the position of the first bit set to one (if 'bit' is 1) or
 * zero (if 'bit' is 0) in the bitmap starting at 's' and long 'count' bytes.
 *
 * The function is guaranteed to return a value >= 0 if 'bit' is 0 since if
 * no zero bit is found, it returns count*8 assuming the string is zero
 * padded on the right. However if 'bit' is 1 it is possible that there is
 * not a single set bit in the bitmap. In this special case -1 is returned.
 *
 * This is a function from the redis project.
 * This function started out as:
 * https://github.com/antirez/redis/blob/94f2e7f/src/bitops.c#L101
 * */
int64_t BitmapString::redisBitpos(unsigned char *c, int64_t count, int bit) {
  uint64_t *l = nullptr;
  uint64_t skipval = 0, word = 0, one = 0;
  int64_t pos = 0; /* Position of bit, to return to the caller. */
  uint64_t j = 0;
  int found = 0;

  /* Process whole words first, seeking for first word that is not
   * all ones or all zeros respectively if we are lookig for zeros
   * or ones. This is much faster with large strings having contiguous
   * blocks of 1 or 0 bits compared to the vanilla bit per bit processing.
   *
   * Note that if we start from an address that is not aligned
   * to sizeof(unsigned long) we consume it byte by byte until it is
   * aligned. */

  /* Skip initial bits not aligned to sizeof(unsigned long) byte by byte. */
  skipval = bit ? 0 : UCHAR_MAX;
  found = 0;
  while (reinterpret_cast<uint64_t>(c) & (sizeof(*l) - 1) && count) {
    if (*c != skipval) {
      found = 1;
      break;
    }
    c++;
    count--;
    pos += 8;
  }

  /* Skip bits with full word step. */
  l = reinterpret_cast<uint64_t *>(c);
  if (!found) {
    skipval = bit ? 0 : UINT64_MAX;
    while (count >= static_cast<int>(sizeof(*l))) {
      if (*l != skipval) break;
      l++;
      count -= sizeof(*l);
      pos += sizeof(*l) * 8;
    }
  }

  /* Load bytes into "word" considering the first byte as the most significant
   * (we basically consider it as written in big endian, since we consider the
   * string as a set of bits from left to right, with the first bit at position
   * zero.
   *
   * Note that the loading is designed to work even when the bytes left
   * (count) are less than a full word. We pad it with zero on the right. */
  c = reinterpret_cast<unsigned char *>(l);
  for (j = 0; j < sizeof(*l); j++) {
    word <<= 8;
    if (count) {
      word |= *c;
      c++;
      count--;
    }
  }

  /* Special case:
   * If bits in the string are all zero and we are looking for one,
   * return -1 to signal that there is not a single "1" in the whole
   * string. This can't happen when we are looking for "0" as we assume
   * that the right of the string is zero padded. */
  if (bit == 1 && word == 0) return -1;

  /* Last word left, scan bit by bit. The first thing we need is to
   * have a single "1" set in the most significant position in an
   * unsigned long. We don't know the size of the long so we use a
   * simple trick. */
  one = UINT64_MAX; /* All bits set to 1.*/
  one >>= 1;        /* All bits set to 1 but the MSB. */
  one = ~one;       /* All bits set to 0 but the MSB. */

  while (one) {
    if (((one & word) != 0) == bit) return pos;
    pos++;
    one >>= 1;
  }

  /* If we reached this point, there is a bug in the algorithm, since
   * the case of no match is handled as a special case before. */
  LOG(ERROR) << "End of redisBitpos() reached.";
  return 0; /* Just to avoid warnings. */
}

}  // namespace Redis
