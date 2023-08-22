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

#include "rdb.h"

#include "common/encoding.h"
#include "list_pack.h"
#include "vendor/crc64.h"
#include "vendor/endianconv.h"
#include "vendor/lzf.h"

// Redis object encoding length
constexpr const int RDB6BitLen = 0;
constexpr const int RDB14BitLen = 1;
constexpr const int RDBEncVal = 3;
constexpr const int RDB32BitLen = 0x08;
constexpr const int RDB64BitLen = 0x81;
constexpr const int RDBEncInt8 = 0;
constexpr const int RDBEncInt16 = 1;
constexpr const int RDBEncInt32 = 2;
constexpr const int RDBEncLzf = 3;

Status RDB::peekOk(size_t n) {
  if (pos_ + n > input_.size()) {
    return {Status::NotOK, "unexpected EOF"};
  }
  return Status::OK();
}

Status RDB::VerifyPayloadChecksum() {
  if (input_.size() < 10) {
    return {Status::NotOK, "invalid payload length"};
  }
  auto footer = input_.substr(input_.size() - 10);
  auto rdb_version = (footer[1] << 8) | footer[0];
  // For now, the max redis rdb version is 10
  if (rdb_version > 11) {
    return {Status::NotOK, fmt::format("invalid version: {}", rdb_version)};
  }
  uint64_t crc = crc64(0, reinterpret_cast<const unsigned char*>(input_.data()), input_.size() - 8);
  memrev64ifbe(&crc);
  if (memcmp(&crc, footer.data() + 2, 8)) {
    return {Status::NotOK, "incorrect checksum"};
  }
  return Status::OK();
}

StatusOr<int> RDB::LoadObjectType() {
  GET_OR_RET(peekOk(1));
  auto type = input_[pos_++] & 0xFF;
  // 0-5 is the basic type of Redis objects and 9-21 is the encoding type of Redis objects.
  // Redis allow basic is 0-7 and 6/7 is for the module type which we don't support here.
  if ((type >= 0 && type < 5) || (type >= 9 && type <= 21)) {
    return type;
  }
  return {Status::NotOK, fmt::format("invalid object type: {}", type)};
}

StatusOr<uint64_t> RDB::loadObjectLen(bool* is_encoded) {
  GET_OR_RET(peekOk(1));
  uint64_t len = 0;
  auto c = input_[pos_++];
  auto type = (c & 0xC0) >> 6;
  switch (type) {
    case RDBEncVal:
      if (is_encoded) *is_encoded = true;
      return c & 0x3F;
    case RDB6BitLen:
      return c & 0x3F;
    case RDB14BitLen:
      len = c & 0x3F;
      GET_OR_RET(peekOk(1));
      return (len << 8) | input_[pos_++];
    case RDB32BitLen:
      GET_OR_RET(peekOk(4));
      __builtin_memcpy(&len, input_.data() + pos_, 4);
      pos_ += 4;
      return len;
    case RDB64BitLen:
      GET_OR_RET(peekOk(8));
      __builtin_memcpy(&len, input_.data() + pos_, 8);
      pos_ += 8;
      return len;
    default:
      return {Status::NotOK, fmt::format("Unknown length encoding {} in loadObjectLen()", type)};
  }
}

StatusOr<std::string> RDB::LoadStringObject() { return loadEncodedString(); }

// Load the LZF compression string
StatusOr<std::string> RDB::loadLzfString() {
  auto compression_len = GET_OR_RET(loadObjectLen(nullptr));
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  GET_OR_RET(peekOk(static_cast<size_t>(compression_len)));

  auto out_buf = make_unique<char[]>(len);
  if (lzf_decompress(input_.data() + pos_, compression_len, out_buf.get(), len) != len) {
    return {Status::NotOK, "Invalid LZF compressed string"};
  }
  pos_ += compression_len;
  return std::string().assign(out_buf.get(), len);
}

StatusOr<std::string> RDB::loadEncodedString() {
  bool is_encoded = false;
  std::string value;
  auto len = GET_OR_RET(loadObjectLen(&is_encoded));
  if (is_encoded) {
    switch (len) {
      case RDBEncInt8:
        GET_OR_RET(peekOk(1));
        return std::to_string(input_[pos_++]);
      case RDBEncInt16:
        GET_OR_RET(peekOk(2));
        value = std::to_string(input_[pos_] | (input_[pos_ + 1] << 8));
        pos_ += 2;
        return value;
      case RDBEncInt32:
        GET_OR_RET(peekOk(4));
        value = std::to_string(input_[pos_] | (input_[pos_ + 1] << 8) | (input_[pos_ + 2] << 16) |
                               (input_[pos_ + 3] << 24));
        pos_ += 4;
        return value;
      case RDBEncLzf:
        return loadLzfString();
      default:
        return {Status::NotOK, fmt::format("Unknown RDB string encoding type {}", len)};
    }
  }

  // Normal string
  GET_OR_RET(peekOk(static_cast<size_t>(len)));
  value = std::string(input_.data() + pos_, len);
  pos_ += len;
  return value;
}

StatusOr<std::vector<std::string>> RDB::LoadQuickListObject(int rdb_type) {
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  std::vector<std::string> list;
  if (len == 0) {
    return list;
  }

  uint64_t container = QuickListNodeContainerPacked;
  for (size_t i = 0; i < len; i++) {
    if (rdb_type == RDBTypeListQuickList2) {
      container = GET_OR_RET(loadObjectLen(nullptr));
      if (container != QuickListNodeContainerPlain && container != QuickListNodeContainerPacked) {
        return {Status::NotOK, fmt::format("Unknown quicklist node container type {}", container)};
      }
    }
    auto list_pack_string = GET_OR_RET(loadEncodedString());
    ListPack lp(list_pack_string);
    auto elements = GET_OR_RET(lp.Entries());
    list.insert(list.end(), elements.begin(), elements.end());
  }
  return list;
}

StatusOr<std::vector<std::string>> RDB::LoadListObject() {
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  std::vector<std::string> list;
  if (len == 0) {
    return list;
  }
  for (size_t i = 0; i < len; i++) {
    auto type = GET_OR_RET(LoadObjectType());
    if (type != RDBTypeString) {
      return {Status::NotOK, fmt::format("Unknown object type {} in loadList()", type)};
    }
    auto element = GET_OR_RET(loadEncodedString());
    list.push_back(element);
  }
  return list;
}

StatusOr<std::vector<std::string>> RDB::LoadSetObject() {
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  std::vector<std::string> set;
  if (len == 0) {
    return set;
  }
  for (size_t i = 0; i < len; i++) {
    auto type = GET_OR_RET(LoadObjectType());
    if (type != RDBTypeString) {
      return {Status::NotOK, fmt::format("Unknown object type {} in loadSet()", type)};
    }
    auto element = GET_OR_RET(loadEncodedString());
    set.push_back(element);
  }
  return set;
}
