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

#include <encoding.h>
#include <storage/redis_metadata.h>

namespace redis {

inline constexpr auto kErrorInsufficientLength = "insufficient length while decoding metadata";

enum class SearchSubkeyType : uint8_t {
  // search global metadata
  PREFIXES = 1,

  // field metadata for different types
  TAG_FIELD_META = 64 + 1,
  NUMERIC_FIELD_META = 64 + 2,

  // field indexing for different types
  TAG_FIELD = 128 + 1,
  NUMERIC_FIELD = 128 + 2,
};

inline std::string ConstructSearchPrefixesSubkey() { return {(char)SearchSubkeyType::PREFIXES}; }

struct SearchPrefixesMetadata {
  std::vector<std::string> prefixes;

  void Encode(std::string *dst) const {
    for (const auto &prefix : prefixes) {
      PutFixed32(dst, prefix.size());
      dst->append(prefix);
    }
  }

  rocksdb::Status Decode(Slice *input) {
    uint32_t size = 0;

    while (GetFixed32(input, &size)) {
      if (input->size() < size) return rocksdb::Status::Corruption(kErrorInsufficientLength);
      prefixes.emplace_back(input->data(), size);
      input->remove_prefix(size);
    }

    return rocksdb::Status::OK();
  }
};

struct SearchFieldMetadata {
  bool noindex = false;

  // flag: <noindex: 1 bit> <reserved: 7 bit>
  uint8_t MakeFlag() const { return noindex; }

  void DecodeFlag(uint8_t flag) { noindex = flag & 1; }

  virtual ~SearchFieldMetadata() = default;

  virtual void Encode(std::string *dst) const { PutFixed8(dst, MakeFlag()); }

  virtual rocksdb::Status Decode(Slice *input) {
    uint8_t flag = 0;
    if (!GetFixed8(input, &flag)) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    DecodeFlag(flag);
    return rocksdb::Status::OK();
  }
};

inline std::string ConstructTagFieldMetadataSubkey(std::string_view field_name) {
  std::string res = {(char)SearchSubkeyType::TAG_FIELD_META};
  res.append(field_name);
  return res;
}

struct SearchTagFieldMetadata : SearchFieldMetadata {
  char separator = ',';
  bool case_sensitive = false;

  void Encode(std::string *dst) const override {
    SearchFieldMetadata::Encode(dst);
    PutFixed8(dst, separator);
    PutFixed8(dst, case_sensitive);
  }

  rocksdb::Status Decode(Slice *input) override {
    if (auto s = SearchFieldMetadata::Decode(input); !s.ok()) {
      return s;
    }

    if (input->size() < 8 + 8) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    GetFixed8(input, (uint8_t *)&separator);
    GetFixed8(input, (uint8_t *)&case_sensitive);
    return rocksdb::Status::OK();
  }
};

inline std::string ConstructNumericFieldMetadataSubkey(std::string_view field_name) {
  std::string res = {(char)SearchSubkeyType::NUMERIC_FIELD_META};
  res.append(field_name);
  return res;
}

struct SearchSortableFieldMetadata : SearchFieldMetadata {};

struct SearchNumericFieldMetadata : SearchSortableFieldMetadata {};

inline std::string ConstructTagFieldSubkey(std::string_view field_name, std::string_view tag, std::string_view key) {
  std::string res = {(char)SearchSubkeyType::TAG_FIELD};
  PutFixed32(&res, field_name.size());
  res.append(field_name);
  PutFixed32(&res, tag.size());
  res.append(tag);
  PutFixed32(&res, key.size());
  res.append(key);
  return res;
}

inline std::string ConstructNumericFieldSubkey(std::string_view field_name, double number, std::string_view key) {
  std::string res = {(char)SearchSubkeyType::NUMERIC_FIELD};
  PutFixed32(&res, field_name.size());
  res.append(field_name);
  PutDouble(&res, number);
  PutFixed32(&res, key.size());
  res.append(key);
  return res;
}

}  // namespace redis
