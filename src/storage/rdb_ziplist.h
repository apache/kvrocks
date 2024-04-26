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

#include <map>
#include <string_view>

#include "common/status.h"
#include "vendor/endianconv.h"

constexpr const int zlHeaderSize = 10;
constexpr const int zlEndSize = 1;
constexpr const int zlTailSize = 4;
constexpr const int zlLenSize = 2;
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

#define ZIPLIST_BYTES(zl) (*((uint32_t *)(zl)))
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t *)((zl) + sizeof(uint32_t))))
#define ZIPLIST_LENGTH(zl) (*((uint16_t *)((zl) + sizeof(uint32_t) * 2)))
#define ZIPLIST_ENTRY_HEAD(zl) ((zl) + zlHeaderSize)

class ZipList {
 public:
  explicit ZipList(std::string_view input) : input_(input){};
  ~ZipList() = default;

  StatusOr<std::string> Next();
  StatusOr<std::vector<std::string>> Entries();
  static uint32_t zipStorePrevEntryLengthLarge(unsigned char *p, unsigned int len);
  static uint32_t zipStorePrevEntryLength(unsigned char *p, unsigned int len);
  static uint32_t zipStoreEntryEncoding(unsigned char *p, unsigned int rawlen);

 private:
  std::string_view input_;
  uint64_t pos_ = 0;
  uint32_t pre_entry_len_ = 0;

  Status peekOK(size_t n);
  void setPreEntryLen(uint32_t len) { pre_entry_len_ = len; }
  static uint32_t getEncodedLengthSize(uint32_t len);
};