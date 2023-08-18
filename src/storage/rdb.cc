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

Status RDB::peekOk(size_t n) {
  if (pos_ + n > buffer_.size()) {
    return {Status::NotOK, "unexpected EOF"};
  }
  return Status::OK();
}

Status RDB::VerifyPayloadChecksum() {
  if (buffer_.size() < 10) {
    return {Status::NotOK, "invalid payload length"};
  }
  auto footer = buffer_.substr(buffer_.size() - 10);
  auto rdb_version = (footer[1] << 8) | footer[0];
  // For now, the max redis rdb version is 10
  if (rdb_version > 11) {
    return {Status::NotOK, fmt::format("invalid version: {}", rdb_version)};
  }
  uint64_t crc = crc64(0, reinterpret_cast<const unsigned char*>(buffer_.data()), buffer_.size() - 8);
  memrev64ifbe(&crc);
  if (memcmp(&crc, footer.data() + 2, 8)) {
    return {Status::NotOK, "incorrect checksum"};
  }
  return Status::OK();
}

StatusOr<int> RDB::LoadObjectType() {
  if (auto status = peekOk(1); !status.OK()) {
    return status;
  }
  auto type = buffer_[pos_++] & 0xFF;
  // 0-5 is the basic type of Redis objects and 9-21 is the encoding type of Redis objects.
  // Redis allow basic is 0-7 and 6/7 is for the module type which we don't support here.
  if ((type >= 0 && type < 5) || (type >= 9 && type <= 21)) {
    return type;
  }
  return {Status::NotOK, fmt::format("invalid object type: {}", type)};
}

StatusOr<uint64_t> RDB::loadObjectLen(bool* is_encoded) {
  if (auto status = peekOk(1); !status.OK()) {
    return status;
  }
  uint64_t len = 0;
  auto c = buffer_[pos_++];
  auto type = (c & 0xC0) >> 6;
  switch (type) {
    case RDB_ENCVAL:
      if (is_encoded) *is_encoded = true;
      return c & 0x3F;
    case RDB_6BITLEN:
      return c & 0x3F;
    case RDB_14BITLEN:
      len = c & 0x3F;
      if (auto status = peekOk(1); !status.OK()) {
        return status;
      }
      return (len << 8) | buffer_[pos_++];
    case RDB_32BITLEN:
      if (auto status = peekOk(4); !status.OK()) {
        return status;
      }
      __builtin_memcpy(&len, buffer_.data() + pos_, 4);
      pos_ += 4;
      return len;
    case RDB_64BITLEN:
      if (auto status = peekOk(8); !status.OK()) {
        return status;
      }
      __builtin_memcpy(&len, buffer_.data() + pos_, 8);
      pos_ += 8;
      return len;
    default:
      return {Status::NotOK, fmt::format("Unknown length encoding {} in loadObjectLen()", type)};
  }
}

StatusOr<std::string> RDB::LoadObject(int type) {
  switch (type) {
    case RDB_TYPE_STRING:
      return loadEncodedString();
    default:
      return {Status::NotOK, fmt::format("Unknown object type {} in LoadObject()", type)};
  }
}

// Load the LZF compression string
StatusOr<std::string> RDB::loadLzfString() {
  auto compression_len = GET_OR_RET(loadObjectLen(nullptr));
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  if (auto status = peekOk(static_cast<size_t>(compression_len)); !status.OK()) {
    return status;
  }

  auto out_buf = std::make_unique<char*>(new char[len]);
  if (lzf_decompress(buffer_.data() + pos_, compression_len, out_buf.get(), len) != len) {
    return {Status::NotOK, "Invalid LZF compressed string"};
  }
  pos_ += compression_len;
  return std::string(reinterpret_cast<const char*>(out_buf.get()), len);
}

StatusOr<std::string> RDB::loadEncodedString() {
  bool is_encoded = false;
  std::string value;
  auto len = GET_OR_RET(loadObjectLen(&is_encoded));
  if (is_encoded) {
    switch (len) {
      case RDB_ENC_INT8:
        if (auto status = peekOk(1); !status.OK()) {
          return status;
        }
        return std::to_string(buffer_[pos_++]);
      case RDB_ENC_INT16:
        if (auto status = peekOk(2); !status.OK()) {
          return status;
        }
        value = std::to_string(buffer_[pos_] | (buffer_[pos_ + 1] << 8));
        pos_ += 2;
        return value;
      case RDB_ENC_INT32:
        if (auto status = peekOk(4); !status.OK()) {
          return status;
        }
        value = std::to_string(buffer_[pos_] | (buffer_[pos_ + 1] << 8) | (buffer_[pos_ + 2] << 16) |
                               (buffer_[pos_ + 3] << 24));
        pos_ += 4;
        return value;
      case RDB_ENC_LZF:
        return loadLzfString();
      default:
        return {Status::NotOK, fmt::format("Unknown RDB string encoding type {}", len)};
    }
  }

  // Normal string
  if (auto status = peekOk(len); !status.OK()) {
    return status;
  }
  value = std::string(buffer_.data() + pos_, len);
  pos_ += len;
  return value;
}
