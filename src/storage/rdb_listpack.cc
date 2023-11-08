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

/*
 * ListPack implements the reading of list-pack binary format
 * according to: https://github.com/antirez/listpack/blob/master/listpack.md.
 *
 * A list-pack is encoded into a single linear chunk of memory.
 * It has a fixed length header of six bytes which contains the total bytes and number of elements:
 *
 *     <total-bytes> <num-elements> <element-1> ... <element-N> <end-byte>
 *          |              |
 *        4byte          2byte
 * Each element is encoded as follows:
 *
 *     <encoding-type><element-data><element-total-bytes>
 *
 * encoding-type is used to indicate what type of data is stored in the element-data:
 *   - [0xxxxxxx] represents 7bit unsigned int
 *   - [10xxxxxx] represents 6bit string, the first byte is the length of the string
 *   - [110xxxxx] represents 13bit signed integer, the first byte is the sign bit
 *   - [1110|xxxx yyyyyyyy] represents 12bit string which can up to 4095 bytes
 *   - [1111|0000] represents 32bit string and the next 4 bytes are the length of the string
 *   - [1111|0001] represents 16 bits signed integer
 *   - [1111|0010] represents 24 bits signed integer
 *   - [1111|0011] represents 32 bits signed integer
 *   - [1111|0100] represents 64 bits signed integer
 *   - [1111|0101] not used for now
 *   - [1111|1111] represents the end of the list-pack
 *
 * element-data was determined by the encoding-type and element-total-bytes is the total bytes of the element.
 * The total bytes of the element is used to traverse the list-pack from the end to the beginning.
 */
#include "rdb_listpack.h"
#include <string>
#include <vector>
#include <stdexcept>

constexpr const int ListPack7BitUIntMask = 0x80;
constexpr const int ListPack7BitUInt = 0;

constexpr const int ListPack6BitStringMask = 0xC0;
constexpr const int ListPack6BitString = 0x80;

constexpr const int ListPack13BitIntMask = 0xE0;
constexpr const int ListPack13BitInt = 0xC0;

constexpr const int ListPack12BitStringMask = 0xF0;
constexpr const int ListPack12BitString = 0xE0;

constexpr const int ListPack16BitIntMask = 0xFF;
constexpr const int ListPack16BitInt = 0xF1;

constexpr const int ListPack24BitIntMask = 0xFF;
constexpr const int ListPack24BitInt = 0xF2;

constexpr const int ListPack32BitIntMask = 0xFF;
constexpr const int ListPack32BitInt = 0xF3;

constexpr const int ListPack64BitIntMask = 0xFF;
constexpr const int ListPack64BitInt = 0xF4;

constexpr const int ListPack32BitStringMask = 0xFF;
constexpr const int ListPack32BitString = 0xF0;

StatusOr<std::vector<std::string>> ListPack::Entries() {
  try {
    std::vector<std::string> elements;
    auto len = length();
    while (len-- != 0) {
      elements.emplace_back(next());
    }
    return elements;
  } catch (const std::runtime_error &e) {
    return Status{Status::NotOK, e.what()};
  } catch (...) {
    return Status{Status::NotOK, "invalid listpack encoding"};
  }
}

uint32_t ListPack::length() {
  try {
    stream_.Consume(4);  // skip total-bytes
    auto entries = stream_.Read<uint16_t>();
    return entries;
  } catch (...) {
    throw std::runtime_error("invalid listpack length");
  }
}

// encodeBackLen returns the number of bytes required to encode the length of the element.
uint32_t ListPack::encodeBackLen(uint32_t len) {
  if (len <= 127) {
    return 1;
  } else if (len < 16383) {
    return 2;
  } else if (len < 2097151) {
    return 3;
  } else if (len < 268435455) {
    return 4;
  } else {
    return 5;
  }
}

std::string ListPack::next() {
  uint32_t len = 0;
  int64_t num = 0;
  std::string value;
  uint32_t entry_bytes = 0;

  auto encoding = stream_.Read<uint8_t>();
  if ((encoding & ListPack7BitUIntMask) == ListPack7BitUInt) {
    // 0|xxxxxxx - 7bit unsigned integer
    num = encoding & 0x7F;
    value = std::to_string(num);
    entry_bytes = 1;
  } else if ((encoding & ListPack6BitStringMask) == ListPack6BitString) {
    // 10xxxxxx - 6bit string
    len = (encoding & 0x3F);
    value = stream_.Read(len);
    entry_bytes = 1 + len;
  } else if ((encoding & ListPack13BitIntMask) == ListPack13BitInt) {
    // 110|xxxxx yyyyyyyy -- 13 bit signed integer
    uint16_t u16 = (encoding & 0x1F) << 8 | stream_.Read<uint8_t>();
    num = static_cast<int16_t>(u16 << 3) >> 3;
    value = std::to_string(num);
    entry_bytes = 1 + 1;
  } else if ((encoding & ListPack16BitIntMask) == ListPack16BitInt) {
    // 1111|0001 <16 bits signed integer>
    num = stream_.Read<int16_t>();
    value = std::to_string(num);
    entry_bytes = 1 + sizeof(int16_t);
  } else if ((encoding & ListPack24BitIntMask) == ListPack24BitInt) {
    // 1111|0010 <24 bits signed integer>
    uint32_t u32 = stream_.Read<uint8_t>() << 8 | stream_.Read<uint8_t>() << 16 | stream_.Read<uint8_t>() << 24;
    num = static_cast<int32_t>(u32) >> 8;
    value = std::to_string(num);
    entry_bytes = 1 + 3;
  } else if ((encoding & ListPack32BitIntMask) == ListPack32BitInt) {
    // 1111|0011 <32 bits signed integer>
    num = stream_.Read<int32_t>();
    value = std::to_string(num);
    entry_bytes = 1 + sizeof(int32_t);
  } else if ((encoding & ListPack64BitIntMask) == ListPack64BitInt) {
    // 1111|0100 <64 bits signed integer>
    num = stream_.Read<int64_t>();
    value = std::to_string(num);
    entry_bytes = 1 + sizeof(int64_t);
  } else if ((encoding & ListPack12BitStringMask) == ListPack12BitString) {
    // 1110|xxxx yyyyyyyy -- string with length up to 4095
    len = (encoding & 0xF) << 8 | stream_.Read<uint8_t>();
    value = stream_.Read(len);
    entry_bytes = 2 + len;
  } else if ((encoding & ListPack32BitStringMask) == ListPack32BitString) {
    // 1111|0000 <4 bytes len> <large string>
    len = stream_.Read<uint32_t>();
    value = stream_.Read(len);
    entry_bytes += 1 + sizeof(uint32_t) + len;
  } else {
    throw std::runtime_error("invalid listpack entry");
  }

  len = encodeBackLen(entry_bytes);
  stream_.Consume(len);  // skip tail len

  return value;
}
