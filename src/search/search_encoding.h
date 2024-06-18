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

#include <memory>

namespace redis {

enum class IndexOnDataType : uint8_t {
  HASH = kRedisHash,
  JSON = kRedisJson,
};

inline constexpr auto kErrorInsufficientLength = "insufficient length while decoding metadata";

class IndexMetadata {
 public:
  uint8_t flag = 0;  // all reserved
  IndexOnDataType on_data_type;

  void Encode(std::string *dst) const {
    PutFixed8(dst, flag);
    PutFixed8(dst, uint8_t(on_data_type));
  }

  rocksdb::Status Decode(Slice *input) {
    if (!GetFixed8(input, &flag)) {
      return rocksdb::Status::InvalidArgument(kErrorInsufficientLength);
    }

    if (!GetFixed8(input, reinterpret_cast<uint8_t *>(&on_data_type))) {
      return rocksdb::Status::InvalidArgument(kErrorInsufficientLength);
    }

    return rocksdb::Status::OK();
  }
};

enum class SearchSubkeyType : uint8_t {
  INDEX_META = 0,

  PREFIXES = 1,

  // field metadata
  FIELD_META = 2,

  // field indexing data
  FIELD = 3,

  // field alias
  FIELD_ALIAS = 4,
};

enum class IndexFieldType : uint8_t {
  TAG = 1,

  NUMERIC = 2,

  VECTOR = 3,
};

enum class VectorType : uint8_t {
  FLOAT32 = 0,
  FLOAT64 = 1,
};

enum class DistanceMetric : uint8_t {
  L2 = 0,
  IP = 1,
  COSINE = 2,
};

enum class HnswLevelType : uint8_t {
  NODE = 1,
  EDGE = 2,
};

struct SearchKey {
  std::string_view ns;
  std::string_view index;
  std::string_view field;

  SearchKey(std::string_view ns, std::string_view index) : ns(ns), index(index) {}
  SearchKey(std::string_view ns, std::string_view index, std::string_view field) : ns(ns), index(index), field(field) {}

  void PutNamespace(std::string *dst) const {
    PutFixed8(dst, ns.size());
    dst->append(ns);
  }

  static void PutType(std::string *dst, SearchSubkeyType type) { PutFixed8(dst, uint8_t(type)); }

  void PutIndex(std::string *dst) const { PutSizedString(dst, index); }

  void PutHnswLevelType(std::string *dst, HnswLevelType type) const { PutFixed8(dst, uint8_t(type)); }

  void PutHnswLevelPrefix(std::string *dst, uint16_t level) const {
    PutNamespace(dst);
    PutType(dst, SearchSubkeyType::FIELD);
    PutIndex(dst);
    PutSizedString(dst, field);
    PutFixed16(dst, level);
  }

  void PutHnswLevelNodePrefix(std::string *dst, uint16_t level) const {
    PutHnswLevelPrefix(dst, level);
    PutHnswLevelType(dst, HnswLevelType::NODE);
  }

  void PutHnswLevelEdgePrefix(std::string *dst, uint16_t level) const {
    PutHnswLevelPrefix(dst, level);
    PutHnswLevelType(dst, HnswLevelType::EDGE);
  }

  std::string ConstructIndexMeta() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::INDEX_META);
    PutIndex(&dst);
    return dst;
  }

  std::string ConstructIndexPrefixes() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::PREFIXES);
    PutIndex(&dst);
    return dst;
  }

  std::string ConstructFieldMeta() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD_META);
    PutIndex(&dst);
    PutSizedString(&dst, field);
    return dst;
  }

  std::string ConstructAllFieldMetaBegin() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD_META);
    PutIndex(&dst);
    PutFixed32(&dst, 0);
    return dst;
  }

  std::string ConstructAllFieldMetaEnd() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD_META);
    PutIndex(&dst);
    PutFixed32(&dst, (uint32_t)(-1));
    return dst;
  }

  std::string ConstructAllFieldDataBegin() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD);
    PutIndex(&dst);
    PutFixed32(&dst, 0);
    return dst;
  }

  std::string ConstructAllFieldDataEnd() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD);
    PutIndex(&dst);
    PutFixed32(&dst, (uint32_t)(-1));
    return dst;
  }

  std::string ConstructTagFieldData(std::string_view tag, std::string_view key) const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD);
    PutIndex(&dst);
    PutSizedString(&dst, field);
    PutSizedString(&dst, tag);
    PutSizedString(&dst, key);
    return dst;
  }

  std::string ConstructNumericFieldData(double num, std::string_view key) const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst, SearchSubkeyType::FIELD);
    PutIndex(&dst);
    PutSizedString(&dst, field);
    PutDouble(&dst, num);
    PutSizedString(&dst, key);
    return dst;
  }

  std::string ConstructHnswLevelNodePrefix(uint16_t level) const {
    std::string dst;
    PutHnswLevelNodePrefix(&dst, level);
    return dst;
  }

  std::string ConstructHnswNode(uint16_t level, std::string_view key) const {
    std::string dst;
    PutHnswLevelNodePrefix(&dst, level);
    PutSizedString(&dst, key);
    return dst;
  }

  std::string ConstructHnswEdgeWithSingleEnd(uint16_t level, std::string_view key) const {
    std::string dst;
    PutHnswLevelEdgePrefix(&dst, level);
    PutSizedString(&dst, key);
    return dst;
  }

  std::string ConstructHnswEdge(uint16_t level, std::string_view key1, std::string_view key2) const {
    std::string dst;
    PutHnswLevelEdgePrefix(&dst, level);
    PutSizedString(&dst, key1);
    PutSizedString(&dst, key2);
    return dst;
  }
};

struct IndexPrefixes {
  std::vector<std::string> prefixes;

  static inline const std::string all[] = {""};

  auto begin() const {  // NOLINT
    return prefixes.empty() ? std::begin(all) : prefixes.data();
  }

  auto end() const {  // NOLINT
    return prefixes.empty() ? std::end(all) : prefixes.data() + prefixes.size();
  }

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

struct IndexFieldMetadata {
  bool noindex = false;
  IndexFieldType type;

  explicit IndexFieldMetadata(IndexFieldType type) : type(type) {}

  // flag: <noindex: 1 bit> <type: 4 bit> <reserved: 3 bit>
  uint8_t MakeFlag() const { return noindex | (uint8_t)type << 1; }

  void DecodeFlag(uint8_t flag) {
    noindex = flag & 1;
    type = DecodeType(flag);
  }

  static IndexFieldType DecodeType(uint8_t flag) { return IndexFieldType(flag >> 1); }

  virtual ~IndexFieldMetadata() = default;

  std::string_view Type() const {
    switch (type) {
      case IndexFieldType::TAG:
        return "tag";
      case IndexFieldType::NUMERIC:
        return "numeric";
      case IndexFieldType::VECTOR:
        return "vector";
      default:
        return "unknown";
    }
  }

  virtual void Encode(std::string *dst) const { PutFixed8(dst, MakeFlag()); }

  virtual rocksdb::Status Decode(Slice *input) {
    uint8_t flag = 0;
    if (!GetFixed8(input, &flag)) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    DecodeFlag(flag);
    return rocksdb::Status::OK();
  }

  virtual bool IsSortable() const { return false; }

  static inline rocksdb::Status Decode(Slice *input, std::unique_ptr<IndexFieldMetadata> &ptr);
};

struct TagFieldMetadata : IndexFieldMetadata {
  char separator = ',';
  bool case_sensitive = false;

  TagFieldMetadata() : IndexFieldMetadata(IndexFieldType::TAG) {}

  void Encode(std::string *dst) const override {
    IndexFieldMetadata::Encode(dst);
    PutFixed8(dst, separator);
    PutFixed8(dst, case_sensitive);
  }

  rocksdb::Status Decode(Slice *input) override {
    if (auto s = IndexFieldMetadata::Decode(input); !s.ok()) {
      return s;
    }

    if (input->size() < 2) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    GetFixed8(input, (uint8_t *)&separator);
    GetFixed8(input, (uint8_t *)&case_sensitive);
    return rocksdb::Status::OK();
  }
};

struct NumericFieldMetadata : IndexFieldMetadata {
  NumericFieldMetadata() : IndexFieldMetadata(IndexFieldType::NUMERIC) {}

  bool IsSortable() const override { return true; }
};

struct HnswVectorFieldMetadata : IndexFieldMetadata {
  VectorType vector_type;
  uint16_t dim;
  DistanceMetric distance_metric;

  uint32_t initial_cap = 500000;
  uint16_t m = 16;
  uint32_t ef_construction = 200;
  uint32_t ef_runtime = 10;
  double epsilon = 0.01;
  uint16_t num_levels = 10;

  HnswVectorFieldMetadata() : IndexFieldMetadata(IndexFieldType::VECTOR) {}

  void Encode(std::string *dst) const override {
    IndexFieldMetadata::Encode(dst);
    PutFixed8(dst, uint8_t(vector_type));
    PutFixed16(dst, dim);
    PutFixed8(dst, uint8_t(distance_metric));
    PutFixed32(dst, initial_cap);
    PutFixed16(dst, m);
    PutFixed32(dst, ef_construction);
    PutFixed32(dst, ef_runtime);
    PutDouble(dst, epsilon);
    PutFixed16(dst, num_levels);
  }

  rocksdb::Status Decode(Slice *input) override {
    if (auto s = IndexFieldMetadata::Decode(input); !s.ok()) {
      return s;
    }

    if (input->size() < 1 + 2 + 1 + 4 + 2 + 4 + 4 + 8 + 2) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    GetFixed8(input, (uint8_t *)(&vector_type));
    GetFixed16(input, &dim);
    GetFixed8(input, (uint8_t *)(&distance_metric));
    GetFixed32(input, &initial_cap);
    GetFixed16(input, &m);
    GetFixed32(input, &ef_construction);
    GetFixed32(input, &ef_runtime);
    GetDouble(input, (double *)(&epsilon));
    GetFixed16(input, &num_levels);
    return rocksdb::Status::OK();
  }
};

struct HnswNodeFieldMetadata {
  uint16_t num_neighbours;
  std::string vector;

  HnswNodeFieldMetadata() {}
  HnswNodeFieldMetadata(uint16_t num_neighbours, std::string_view vector) : num_neighbours(num_neighbours), vector(vector) {}

  void Encode(std::string *dst) const {
    PutFixed16(dst, uint16_t(num_neighbours));
    PutSizedString(dst, vector);
  }

  rocksdb::Status Decode(Slice *input) {
    if (input->size() < 2 + 4) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }
    GetFixed16(input, (uint16_t *)(&num_neighbours));
    Slice value;
    GetSizedString(input, &value);
    vector = value.ToString();
    return rocksdb::Status::OK();
  }
};

inline rocksdb::Status IndexFieldMetadata::Decode(Slice *input, std::unique_ptr<IndexFieldMetadata> &ptr) {
  if (input->size() < 1) {
    return rocksdb::Status::Corruption(kErrorInsufficientLength);
  }

  switch (DecodeType((*input)[0])) {
    case IndexFieldType::TAG:
      ptr = std::make_unique<TagFieldMetadata>();
      break;
    case IndexFieldType::NUMERIC:
      ptr = std::make_unique<NumericFieldMetadata>();
      break;
    case IndexFieldType::VECTOR:
      ptr = std::make_unique<HnswVectorFieldMetadata>();
      break;
    default:
      return rocksdb::Status::Corruption("encountered unknown field type");
  }

  return ptr->Decode(input);
}

}  // namespace redis
