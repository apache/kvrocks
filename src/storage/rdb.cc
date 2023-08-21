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

#include "common/crc64.h"
#include "common/encoding.h"
#include "common/lzf.h"

// Redis object encoding length
constexpr const int RDB_6BITLEN = 0;
constexpr const int RDB_14BITLEN = 1;
constexpr const int RDB_ENCVAL = 3;
constexpr const int RDB_32BITLEN = 0x08;
constexpr const int RDB_64BITLEN = 0x81;

constexpr const int RDB_ENC_INT8 = 0;
constexpr const int RDB_ENC_INT16 = 1;
constexpr const int RDB_ENC_INT32 = 2;
constexpr const int RDB_ENC_LZF = 3;

class ListPack {
 public:
  explicit ListPack(std::string_view input) : input_(input){};
  ~ListPack() = default;

  StatusOr<std::vector<std::string>> Elements() {
    auto len = GET_OR_RET(length());
    std::vector<std::string> elements;
    while (len-- != 0) {
      auto element = GET_OR_RET(next());
      elements.emplace_back(element);
    }
    return elements;
  }

 private:
  std::string_view input_;
  uint64_t pos_ = 0;

  StatusOr<uint32_t> length() {
    constexpr const int listPackHeaderSize = 6;
    if (input_.size() < listPackHeaderSize) {
      return {Status::NotOK, "invalid listpack length"};
    }
    uint32_t total_bytes = (static_cast<uint32_t>(input_[0])) | (static_cast<uint32_t>(input_[1]) << 8) |
                           (static_cast<uint32_t>(input_[2]) << 16) | (static_cast<uint32_t>(input_[3]) << 24);
    uint32_t len = (static_cast<uint32_t>(input_[4])) | (static_cast<uint32_t>(input_[5]) << 8);
    pos_ += listPackHeaderSize;
    if (total_bytes != input_.size()) {
      return {Status::NotOK, "invalid list pack length"};
    }
    return len;
  }

  static uint32_t encodeBackLen(uint32_t len) {
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

  Status peekOK(size_t n) {
    if (pos_ + n > input_.size()) {
      return {Status::NotOK, "invalid list pack entry"};
    }
    return Status::OK();
  }

  StatusOr<std::string> next() {
    GET_OR_RET(peekOK(1));

    uint32_t value_len = 0;
    uint64_t int_value = 0;
    std::string value;
    unsigned char c = input_[pos_];
    if ((c & 0x80) == 0) {  // 7bit unsigned int
      GET_OR_RET(peekOK(2));
      int_value = c & 0x7F;
      value = std::to_string(int_value);
      pos_ += 2;
    } else if ((c & 0xC0) == 0x80) {  // 6bit string
      value_len = (c & 0x3F);
      pos_ += 1;
      GET_OR_RET(peekOK(value_len));
      value = input_.substr(pos_, value_len);
      pos_ += value_len + encodeBackLen(value_len + 1);
    } else if ((c & 0xE0) == 0xC0) {  // 13bit int
      GET_OR_RET(peekOK(3));
      int_value = ((c & 0x1F) << 8) | input_[pos_];
      value = std::to_string(int_value);
      pos_ += 3;
    } else if ((c & 0xFF) == 0xF1) {  // 16bit int
      GET_OR_RET(peekOK(4));
      int_value = (static_cast<uint64_t>(input_[pos_ + 1])) | (static_cast<uint64_t>(input_[pos_ + 2]) << 8);
      value = std::to_string(int_value);
      pos_ += 4;
    } else if ((c & 0xFF) == 0xF2) {  // 24bit int
      GET_OR_RET(peekOK(5));
      int_value = (static_cast<uint64_t>(input_[pos_ + 1])) | (static_cast<uint64_t>(input_[pos_ + 2]) << 8) |
                  (static_cast<uint64_t>(input_[pos_ + 3]) << 16);
      value = std::to_string(int_value);
      pos_ += 5;
    } else if ((c & 0xFF) == 0xF3) {  // 32bit int
      GET_OR_RET(peekOK(6));
      int_value = (static_cast<uint64_t>(input_[pos_ + 1])) | (static_cast<uint64_t>(input_[pos_ + 2]) << 8) |
                  (static_cast<uint64_t>(input_[pos_ + 3]) << 16) | (static_cast<uint64_t>(input_[pos_ + 4]) << 24);
      value = std::to_string(int_value);
      pos_ += 6;
    } else if ((c & 0xFF) == 0xF4) {  // 64bit int
      GET_OR_RET(peekOK(10));
      int_value = (static_cast<uint64_t>(input_[pos_ + 1])) | (static_cast<uint64_t>(input_[pos_ + 2]) << 8) |
                  (static_cast<uint64_t>(input_[pos_ + 3]) << 16) | (static_cast<uint64_t>(input_[pos_ + 4]) << 24) |
                  (static_cast<uint64_t>(input_[pos_ + 5]) << 32) | (static_cast<uint64_t>(input_[pos_ + 6]) << 40) |
                  (static_cast<uint64_t>(input_[pos_ + 7]) << 48) | (static_cast<uint64_t>(input_[pos_ + 8]) << 56);
      value = std::to_string(int_value);
      pos_ += 10;
    } else if ((c & 0xF0) == 0xE0) {  // 12bit string
      GET_OR_RET(peekOK(2));
      value_len = ((input_[pos_] & 0xF) << 8) | input_[pos_ + 1];
      pos_ += 2;
      GET_OR_RET(peekOK(value_len));
      value = input_.substr(pos_, value_len);
      pos_ += value_len + encodeBackLen(value_len + 2);
    } else if ((c & 0xFF) == 0xF0) {  // 32bit string
      GET_OR_RET(peekOK(5));
      value_len = (static_cast<uint32_t>(input_[pos_])) | (static_cast<uint32_t>(input_[pos_ + 1]) << 8) |
                  (static_cast<uint32_t>(input_[pos_ + 2]) << 16) | (static_cast<uint32_t>(input_[pos_ + 3]) << 24);
      pos_ += 5;
      GET_OR_RET(peekOK(value_len));
      value = input_.substr(pos_, value_len);
      pos_ += value_len + encodeBackLen(value_len + 5);
    } else if (c == 0xFF) {
      ++pos_;
    } else {
      return {Status::NotOK, "invalid list pack entry"};
    }
    return value;
  }
};

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
    case RDB_ENCVAL:
      if (is_encoded) *is_encoded = true;
      return c & 0x3F;
    case RDB_6BITLEN:
      return c & 0x3F;
    case RDB_14BITLEN:
      len = c & 0x3F;
      GET_OR_RET(peekOk(1));
      return (len << 8) | input_[pos_++];
    case RDB_32BITLEN:
      GET_OR_RET(peekOk(4));
      __builtin_memcpy(&len, input_.data() + pos_, 4);
      pos_ += 4;
      return len;
    case RDB_64BITLEN:
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

  auto out_buf = make_unique<char*>(new char[len]);
  if (lzf_decompress(input_.data() + pos_, compression_len, *out_buf, len) != len) {
    return {Status::NotOK, "Invalid LZF compressed string"};
  }
  pos_ += compression_len;
  return std::string().assign(*out_buf, len);
}

StatusOr<std::string> RDB::loadEncodedString() {
  bool is_encoded = false;
  std::string value;
  auto len = GET_OR_RET(loadObjectLen(&is_encoded));
  if (is_encoded) {
    switch (len) {
      case RDB_ENC_INT8:
        GET_OR_RET(peekOk(1));
        return std::to_string(input_[pos_++]);
      case RDB_ENC_INT16:
        GET_OR_RET(peekOk(2));
        value = std::to_string(input_[pos_] | (input_[pos_ + 1] << 8));
        pos_ += 2;
        return value;
      case RDB_ENC_INT32:
        GET_OR_RET(peekOk(4));
        value = std::to_string(input_[pos_] | (input_[pos_ + 1] << 8) | (input_[pos_ + 2] << 16) |
                               (input_[pos_ + 3] << 24));
        pos_ += 4;
        return value;
      case RDB_ENC_LZF:
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

  uint64_t container = QUICKLIST_NODE_CONTAINER_PACKED;
  for (size_t i = 0; i < len; i++) {
    if (rdb_type == RDB_TYPE_LIST_QUICKLIST_2) {
      container = GET_OR_RET(loadObjectLen(nullptr));
      if (container != QUICKLIST_NODE_CONTAINER_PLAIN && container != QUICKLIST_NODE_CONTAINER_PACKED) {
        return {Status::NotOK, fmt::format("Unknown quicklist node container type {}", container)};
      }
    }
    auto list_pack_string = GET_OR_RET(loadEncodedString());
    ListPack lp(list_pack_string);
    auto elements = GET_OR_RET(lp.Elements());
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
    if (type != RDB_TYPE_STRING) {
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
    if (type != RDB_TYPE_STRING) {
      return {Status::NotOK, fmt::format("Unknown object type {} in loadSet()", type)};
    }
    auto element = GET_OR_RET(loadEncodedString());
    set.push_back(element);
  }
  return set;
}

StatusOr<std::vector<MemberScore>>  RDB::LoadZSetObject() {
  std::vector<MemberScore> member_scores;
  return member_scores;
}
