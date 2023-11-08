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

#include "rdb_ziplist.h"

#include <string>
#include <vector>

#include "vendor/endianconv.h"

constexpr const uint8_t ZipListBigLen = 0xFE;
constexpr const uint8_t zlEnd = 0xFF;

constexpr const uint8_t ZIP_STR_MASK = 0xC0;
constexpr const uint8_t ZIP_STR_06B = (0 << 6);
constexpr const uint8_t ZIP_STR_14B = (1 << 6);
constexpr const uint8_t ZIP_STR_32B = (2 << 6);
constexpr const uint8_t ZIP_INT_16B = (0xC0 | 0 << 4);
constexpr const uint8_t ZIP_INT_32B = (0xC0 | 1 << 4);
constexpr const uint8_t ZIP_INT_64B = (0xC0 | 2 << 4);
constexpr const uint8_t ZIP_INT_24B = (0xC0 | 3 << 4);
constexpr const uint8_t ZIP_INT_8B = 0xFE;

constexpr const uint8_t ZIP_INT_IMM_MIN = 0xF1; /* 11110001 */
constexpr const uint8_t ZIP_INT_IMM_MAX = 0xFD; /* 11111101 */
constexpr const uint8_t ZIP_INT_IMM_MASK = 0x0F;

/* Copy from Redis codebase in file ziplist.c
 *
 * The ziplist is a specially encoded dually linked list that is designed
 * to be very memory efficient. It stores both strings and integer values,
 * where integers are encoded as actual integers instead of a series of
 * characters. It allows push and pop operations on either side of the list
 * in O(1) time. However, because every operation requires a reallocation of
 * the memory used by the ziplist, the actual complexity is related to the
 * amount of memory used by the ziplist.
 *
 * ----------------------------------------------------------------------------
 *
 * ZIPLIST OVERALL LAYOUT
 * ======================
 *
 * The general layout of the ziplist is as follows:
 *
 * <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 *
 * NOTE: all fields are stored in little endian, if not specified otherwise.
 *
 * <uint32_t zlbytes> is an unsigned integer to hold the number of bytes that
 * the ziplist occupies, including the four bytes of the zlbytes field itself.
 * This value needs to be stored to be able to resize the entire structure
 * without the need to traverse it first.
 *
 * <uint32_t zltail> is the offset to the last entry in the list. This allows
 * a pop operation on the far side of the list without the need for full
 * traversal.
 *
 * <uint16_t zllen> is the number of entries. When there are more than
 * 2^16-2 entries, this value is set to 2^16-1 and we need to traverse the
 * entire list to know how many items it holds.
 *
 * <uint8_t zlend> is a special entry representing the end of the ziplist.
 * Is encoded as a single byte equal to 255. No other normal entry starts
 * with a byte set to the value of 255.
 *
 * ZIPLIST ENTRIES
 * ===============
 *
 * Every entry in the ziplist is prefixed by metadata that contains two pieces
 * of information. First, the length of the previous entry is stored to be
 * able to traverse the list from back to front. Second, the entry encoding is
 * provided. It represents the entry type, integer or string, and in the case
 * of strings it also represents the length of the string payload.
 * So a complete entry is stored like this:
 *
 * <prevlen> <encoding> <entry-data>
 *
 * Sometimes the encoding represents the entry itself, like for small integers
 * as we'll see later. In such a case the <entry-data> part is missing, and we
 * could have just:
 *
 * <prevlen> <encoding>
 *
 * The length of the previous entry, <prevlen>, is encoded in the following way:
 * If this length is smaller than 254 bytes, it will only consume a single
 * byte representing the length as an unsigned 8 bit integer. When the length
 * is greater than or equal to 254, it will consume 5 bytes. The first byte is
 * set to 254 (FE) to indicate a larger value is following. The remaining 4
 * bytes take the length of the previous entry as value.
 *
 * So practically an entry is encoded in the following way:
 *
 * <prevlen from 0 to 253> <encoding> <entry>
 *
 * Or alternatively if the previous entry length is greater than 253 bytes
 * the following encoding is used:
 *
 * 0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>
 *
 * The encoding field of the entry depends on the content of the
 * entry. When the entry is a string, the first 2 bits of the encoding first
 * byte will hold the type of encoding used to store the length of the string,
 * followed by the actual length of the string. When the entry is an integer
 * the first 2 bits are both set to 1. The following 2 bits are used to specify
 * what kind of integer will be stored after this header. An overview of the
 * different types and encodings is as follows. The first byte is always enough
 * to determine the kind of entry.
 *
 * |00pppppp| - 1 byte
 *      String value with length less than or equal to 63 bytes (6 bits).
 *      "pppppp" represents the unsigned 6 bit length.
 * |01pppppp|qqqqqqqq| - 2 bytes
 *      String value with length less than or equal to 16383 bytes (14 bits).
 *      IMPORTANT: The 14 bit number is stored in big endian.
 * |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
 *      String value with length greater than or equal to 16384 bytes.
 *      Only the 4 bytes following the first byte represents the length
 *      up to 2^32-1. The 6 lower bits of the first byte are not used and
 *      are set to zero.
 *      IMPORTANT: The 32 bit number is stored in big endian.
 * |11000000| - 3 bytes
 *      Integer encoded as int16_t (2 bytes).
 * |11010000| - 5 bytes
 *      Integer encoded as int32_t (4 bytes).
 * |11100000| - 9 bytes
 *      Integer encoded as int64_t (8 bytes).
 * |11110000| - 4 bytes
 *      Integer encoded as 24 bit signed (3 bytes).
 * |11111110| - 2 bytes
 *      Integer encoded as 8 bit signed (1 byte).
 * |1111xxxx| - (with xxxx between 0001 and 1101) immediate 4 bit integer.
 *      Unsigned integer from 0 to 12. The encoded value is actually from
 *      1 to 13 because 0000 and 1111 can not be used, so 1 should be
 *      subtracted from the encoded 4 bit value to obtain the right value.
 * |11111111| - End of ziplist special entry.
 *
 * Like for the ziplist header, all the integers are represented in little
 * endian byte order, even when this code is compiled in big endian systems.
 *
 * EXAMPLES OF ACTUAL ZIPLISTS
 * ===========================
 *
 * The following is a ziplist containing the two elements representing
 * the strings "2" and "5". It is composed of 15 bytes, that we visually
 * split into sections:
 *
 *  [0f 00 00 00] [0c 00 00 00] [02 00] [00 f3] [02 f6] [ff]
 *        |             |          |       |       |     |
 *     zlbytes        zltail     zllen    "2"     "5"   end
 *
 * The first 4 bytes represent the number 15, that is the number of bytes
 * the whole ziplist is composed of. The second 4 bytes are the offset
 * at which the last ziplist entry is found, that is 12, in fact the
 * last entry, that is "5", is at offset 12 inside the ziplist.
 * The next 16 bit integer represents the number of elements inside the
 * ziplist, its value is 2 since there are just two elements inside.
 * Finally "00 f3" is the first entry representing the number 2. It is
 * composed of the previous entry length, which is zero because this is
 * our first entry, and the byte F3 which corresponds to the encoding
 * |1111xxxx| with xxxx between 0001 and 1101. We need to remove the "F"
 * higher order bits 1111, and subtract 1 from the "3", so the entry value
 * is "2". The next entry has a prevlen of 02, since the first entry is
 * composed of exactly two bytes. The entry itself, F6, is encoded exactly
 * like the first entry, and 6-1 = 5, so the value of the entry is 5.
 * Finally the special entry FF signals the end of the ziplist.
 *
 * Adding another element to the above string with the value "Hello World"
 * allows us to show how the ziplist encodes small strings. We'll just show
 * the hex dump of the entry itself. Imagine the bytes as following the
 * entry that stores "5" in the ziplist above:
 *
 * [02] [0b] [48 65 6c 6c 6f 20 57 6f 72 6c 64]
 *
 * The first byte, 02, is the length of the previous entry. The next
 * byte represents the encoding in the pattern |00pppppp| that means
 * that the entry is a string of length <pppppp>, so 0B means that
 * an 11 bytes string follows. From the third byte (48) to the last (64)
 * there are just the ASCII characters for "Hello World".
 */

std::string ZipList::next() {
  auto prev_len = stream_.Read<uint8_t>();
  if (prev_len == ZipListBigLen) {
    stream_.Read<uint32_t>();  // real prev_len
  }

  auto encoding = stream_.Read<uint8_t>();
  uint32_t len = 0;
  uint64_t num = 0;
  std::string value;

  if ((encoding & ZIP_STR_MASK) == ZIP_STR_06B) {
    len = encoding & 0x3F;
    value = stream_.Read(len);
  } else if ((encoding & ZIP_STR_MASK) == ZIP_STR_14B) {
    len = (encoding & 0x3F) << 8 | stream_.Read<uint8_t>();
    value = stream_.Read(len);
  } else if ((encoding & ZIP_STR_MASK) == ZIP_STR_32B) {
    len = stream_.Read<uint32_t>();
    len = ntohl(len);  // string length is saved in big endian
    value = stream_.Read(len);
  } else if (encoding == ZIP_INT_16B) {
    num = stream_.Read<int16_t>();
    value = std::to_string(num);
  } else if (encoding == ZIP_INT_32B) {
    num = stream_.Read<int32_t>();
    value = std::to_string(num);
  } else if (encoding == ZIP_INT_64B) {
    num = stream_.Read<int64_t>();
    value = std::to_string(num);
  } else if (encoding == ZIP_INT_24B) {
    uint32_t u32 = stream_.Read<uint8_t>() << 8 | stream_.Read<uint8_t>() << 16 | stream_.Read<uint8_t>() << 24;
    num = static_cast<int32_t>(u32) >> 8;
    value = std::to_string(num);
  } else if (encoding == ZIP_INT_8B) {
    num = stream_.Read<uint8_t>();
    value = std::to_string(num);
  } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
    num = (encoding & ZIP_INT_IMM_MASK) - 1;
    value = std::to_string(num);
  } else {
    throw std::runtime_error{"invalid ziplist encoding"};
  }

  return value;
}

StatusOr<std::vector<std::string>> ZipList::Entries() {
  try {
    stream_.Consume(4);  // skip zlbytes
    stream_.Consume(4);  // skip zltail

    std::vector<std::string> entries;
    auto zllen = stream_.Read<uint16_t>();
    for (uint16_t i = 0; i < zllen; i++) {
      entries.emplace_back(next());
    }
    auto end = stream_.Read<uint8_t>();
    if (end != zlEnd) {
      return {Status::NotOK, "invalid ziplist encoding"};
    }
    return entries;
  } catch (...) {
    return {Status::NotOK, "invalid ziplist encoding"};
  }
}
