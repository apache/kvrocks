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
#include "rdb_intset.h"
#include "rdb_listpack.h"
#include "rdb_ziplist.h"
#include "rdb_zipmap.h"
#include "time_util.h"
#include "types/redis_hash.h"
#include "types/redis_list.h"
#include "types/redis_set.h"
#include "types/redis_string.h"
#include "types/redis_zset.h"
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
  uint64_t crc = crc64(0, reinterpret_cast<const unsigned char *>(input_.data()), input_.size() - 8);
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

StatusOr<uint64_t> RDB::loadObjectLen(bool *is_encoded) {
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

  std::string out_buf(len, 0);
  if (lzf_decompress(input_.data() + pos_, compression_len, out_buf.data(), len) != len) {
    return {Status::NotOK, "Invalid LZF compressed string"};
  }
  pos_ += compression_len;
  return out_buf;
}

StatusOr<std::string> RDB::loadEncodedString() {
  bool is_encoded = false;
  auto len = GET_OR_RET(loadObjectLen(&is_encoded));
  if (is_encoded) {
    // For integer type, needs to convert to uint8_t* to avoid signed extension
    auto data = reinterpret_cast<const uint8_t *>(input_.data());
    if (len == RDBEncInt8) {
      GET_OR_RET(peekOk(1));
      return std::to_string(data[pos_++]);
    } else if (len == RDBEncInt16) {
      GET_OR_RET(peekOk(2));
      auto value = static_cast<uint16_t>(data[pos_]) | (static_cast<uint16_t>(data[pos_ + 1]) << 8);
      pos_ += 2;
      return std::to_string(static_cast<int16_t>(value));
    } else if (len == RDBEncInt32) {
      GET_OR_RET(peekOk(4));
      auto value = static_cast<uint32_t>(data[pos_]) | (static_cast<uint32_t>(data[pos_ + 1]) << 8) |
                   (static_cast<uint32_t>(data[pos_ + 2]) << 16) | (static_cast<uint32_t>(data[pos_ + 3]) << 24);
      pos_ += 4;
      return std::to_string(static_cast<int32_t>(value));
    } else if (len == RDBEncLzf) {
      return loadLzfString();
    } else {
      return {Status::NotOK, fmt::format("Unknown RDB string encoding type {}", len)};
    }
  }

  // Normal string
  GET_OR_RET(peekOk(static_cast<size_t>(len)));
  auto value = std::string(input_.data() + pos_, len);
  pos_ += len;
  return value;
}

StatusOr<std::vector<std::string>> RDB::LoadListWithQuickList(int type) {
  std::vector<std::string> list;
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  if (len == 0) {
    return list;
  }

  uint64_t container = QuickListNodeContainerPacked;
  for (size_t i = 0; i < len; i++) {
    if (type == RDBTypeListQuickList2) {
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
    auto element = GET_OR_RET(loadEncodedString());
    list.push_back(element);
  }
  return list;
}

StatusOr<std::vector<std::string>> RDB::LoadListWithZipList() {
  auto encoded_string = GET_OR_RET(LoadStringObject());
  ZipList zip_list(encoded_string);
  return zip_list.Entries();
}

StatusOr<std::vector<std::string>> RDB::LoadSetObject() {
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  std::vector<std::string> set;
  if (len == 0) {
    return set;
  }
  for (size_t i = 0; i < len; i++) {
    auto element = GET_OR_RET(LoadStringObject());
    set.push_back(element);
  }
  return set;
}

StatusOr<std::vector<std::string>> RDB::LoadSetWithListPack() {
  auto encoded_string = GET_OR_RET(LoadStringObject());
  ListPack lp(encoded_string);
  return lp.Entries();
}

StatusOr<std::vector<std::string>> RDB::LoadSetWithIntSet() {
  auto encoded_string = GET_OR_RET(LoadStringObject());
  IntSet intset(encoded_string);
  return intset.Entries();
}

StatusOr<std::map<std::string, std::string>> RDB::LoadHashObject() {
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  std::map<std::string, std::string> hash;
  if (len == 0) {
    return hash;
  }

  for (size_t i = 0; i < len; i++) {
    auto field = GET_OR_RET(LoadStringObject());
    auto value = GET_OR_RET(LoadStringObject());
    hash[field] = value;
  }
  return hash;
}

StatusOr<std::map<std::string, std::string>> RDB::LoadHashWithZipMap() {
  auto encoded_string = GET_OR_RET(LoadStringObject());
  ZipMap zip_map(encoded_string);
  return zip_map.Entries();
}

StatusOr<std::map<std::string, std::string>> RDB::LoadHashWithListPack() {
  auto encoded_string = GET_OR_RET(LoadStringObject());
  ListPack lp(encoded_string);
  auto entries = GET_OR_RET(lp.Entries());
  if (entries.size() % 2 != 0) {
    return {Status::NotOK, "invalid list pack length"};
  }
  std::map<std::string, std::string> hash;
  for (size_t i = 0; i < entries.size(); i += 2) {
    hash[entries[i]] = entries[i + 1];
  }
  return hash;
}

StatusOr<std::map<std::string, std::string>> RDB::LoadHashWithZipList() {
  auto encoded_string = GET_OR_RET(LoadStringObject());
  ZipList zip_list(encoded_string);
  auto entries = GET_OR_RET(zip_list.Entries());
  if (entries.size() % 2 != 0) {
    return {Status::NotOK, "invalid zip list length"};
  }
  std::map<std::string, std::string> hash;
  for (size_t i = 0; i < entries.size(); i += 2) {
    hash[entries[i]] = entries[i + 1];
  }
  return hash;
}

StatusOr<double> RDB::loadBinaryDouble() {
  GET_OR_RET(peekOk(8));
  double value = 0;
  memcpy(&value, input_.data() + pos_, 8);
  memrev64ifbe(&value);
  pos_ += 8;
  return value;
}

StatusOr<double> RDB::loadDouble() {
  char buf[256];
  GET_OR_RET(peekOk(1));
  auto len = static_cast<uint8_t>(input_[pos_++]);
  switch (len) {
    case 255:
      return -INFINITY; /* Negative inf */
    case 254:
      return INFINITY; /* Positive inf */
    case 253:
      return NAN; /* NaN */
  }
  GET_OR_RET(peekOk(len));
  memcpy(buf, input_.data() + pos_, len);
  buf[len] = '\0';
  pos_ += len;
  return ParseFloat<double>(std::string(buf, len));
}

StatusOr<std::vector<MemberScore>> RDB::LoadZSetObject(int type) {
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  std::vector<MemberScore> zset;
  if (len == 0) {
    return zset;
  }

  for (size_t i = 0; i < len; i++) {
    auto member = GET_OR_RET(LoadStringObject());
    double score = 0;
    if (type == RDBTypeZSet2) {
      score = GET_OR_RET(loadBinaryDouble());
    } else {
      score = GET_OR_RET(loadDouble());
    }
    zset.emplace_back(MemberScore{member, score});
  }
  return zset;
}

StatusOr<std::vector<MemberScore>> RDB::LoadZSetWithListPack() {
  auto encoded_string = GET_OR_RET(LoadStringObject());
  ListPack lp(encoded_string);
  auto entries = GET_OR_RET(lp.Entries());
  if (entries.size() % 2 != 0) {
    return {Status::NotOK, "invalid list pack length"};
  }
  std::vector<MemberScore> zset;
  for (size_t i = 0; i < entries.size(); i += 2) {
    zset.emplace_back(MemberScore{entries[i], std::stod(entries[i + 1])});
  }
  return zset;
}

StatusOr<std::vector<MemberScore>> RDB::LoadZSetWithZipList() {
  auto encoded_string = GET_OR_RET(LoadStringObject());
  ZipList zip_list(encoded_string);
  auto entries = GET_OR_RET(zip_list.Entries());
  if (entries.size() % 2 != 0) {
    return {Status::NotOK, "invalid zip list length"};
  }
  std::vector<MemberScore> zset;
  for (size_t i = 0; i < entries.size(); i += 2) {
    zset.emplace_back(MemberScore{entries[i], std::stod(entries[i + 1])});
  }
  return zset;
}

Status RDB::Restore(const std::string &key, uint64_t ttl_ms) {
  rocksdb::Status db_status;

  // Check the checksum of the payload
  GET_OR_RET(VerifyPayloadChecksum());

  auto type = GET_OR_RET(LoadObjectType());
  if (type == RDBTypeString) {
    auto value = GET_OR_RET(LoadStringObject());
    redis::String string_db(storage_, ns_);
    db_status = string_db.SetEX(key, value, ttl_ms);
  } else if (type == RDBTypeSet || type == RDBTypeSetIntSet || type == RDBTypeSetListPack) {
    std::vector<std::string> members;
    if (type == RDBTypeSet) {
      members = GET_OR_RET(LoadSetObject());
    } else if (type == RDBTypeSetListPack) {
      members = GET_OR_RET(LoadSetWithListPack());
    } else {
      members = GET_OR_RET(LoadSetWithIntSet());
    }
    redis::Set set_db(storage_, ns_);
    uint64_t count = 0;
    std::vector<Slice> insert_members;
    insert_members.reserve(members.size());
    for (const auto &member : members) {
      insert_members.emplace_back(member);
    }
    db_status = set_db.Add(key, insert_members, &count);
  } else if (type == RDBTypeZSet || type == RDBTypeZSet2 || type == RDBTypeZSetListPack || type == RDBTypeZSetZipList) {
    std::vector<MemberScore> member_scores;
    if (type == RDBTypeZSet || type == RDBTypeZSet2) {
      member_scores = GET_OR_RET(LoadZSetObject(type));
    } else if (type == RDBTypeZSetListPack) {
      member_scores = GET_OR_RET(LoadZSetWithListPack());
    } else {
      member_scores = GET_OR_RET(LoadZSetWithZipList());
    }
    redis::ZSet zset_db(storage_, ns_);
    uint64_t count = 0;
    db_status = zset_db.Add(key, ZAddFlags(0), (redis::ZSet::MemberScores *)&member_scores, &count);
  } else if (type == RDBTypeHash || type == RDBTypeHashListPack || type == RDBTypeHashZipList ||
             type == RDBTypeHashZipMap) {
    std::map<std::string, std::string> entries;
    if (type == RDBTypeHash) {
      entries = GET_OR_RET(LoadHashObject());
    } else if (type == RDBTypeHashListPack) {
      entries = GET_OR_RET(LoadHashWithListPack());
    } else if (type == RDBTypeHashZipList) {
      entries = GET_OR_RET(LoadHashWithZipList());
    } else {
      entries = GET_OR_RET(LoadHashWithZipMap());
    }
    std::vector<FieldValue> filed_values;
    filed_values.reserve(entries.size());
    for (const auto &entry : entries) {
      filed_values.emplace_back(entry.first, entry.second);
    }
    redis::Hash hash_db(storage_, ns_);
    uint64_t count = 0;
    db_status = hash_db.MSet(key, filed_values, false /*nx*/, &count);
  } else if (type == RDBTypeList || type == RDBTypeListZipList || type == RDBTypeListQuickList ||
             type == RDBTypeListQuickList2) {
    std::vector<std::string> elements;
    if (type == RDBTypeList) {
      elements = GET_OR_RET(LoadListObject());
    } else if (type == RDBTypeListZipList) {
      elements = GET_OR_RET(LoadListWithZipList());
    } else {
      elements = GET_OR_RET(LoadListWithQuickList(type));
    }
    if (!elements.empty()) {
      std::vector<Slice> insert_elements;
      insert_elements.reserve(elements.size());
      for (const auto &element : elements) {
        insert_elements.emplace_back(element);
      }
      redis::List list_db(storage_, ns_);
      uint64_t list_size = 0;
      db_status = list_db.Push(key, insert_elements, false, &list_size);
    }
  } else {
    return {Status::RedisExecErr, fmt::format("unsupported restore type: {}", type)};
  }
  if (!db_status.ok()) {
    return {Status::RedisExecErr, db_status.ToString()};
  }
  // String type will use the SETEX, so just only set the ttl for other types
  if (ttl_ms > 0 && type != RDBTypeString) {
    redis::Database db(storage_, ns_);
    db_status = db.Expire(key, ttl_ms + util::GetTimeStampMS());
  }
  return db_status.ok() ? Status::OK() : Status{Status::RedisExecErr, db_status.ToString()};
}