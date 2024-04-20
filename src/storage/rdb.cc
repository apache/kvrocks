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

#include <glog/logging.h>

#include "common/encoding.h"
#include "common/rdb_stream.h"
#include "common/time_util.h"
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
constexpr const int RDB32BitLen = 0x80;
constexpr const int RDB64BitLen = 0x81;
constexpr const int RDBEncInt8 = 0;
constexpr const int RDBEncInt16 = 1;
constexpr const int RDBEncInt32 = 2;
constexpr const int RDBEncLzf = 3;

/* Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType). */
constexpr const int RDBOpcodeFunction2 = 245;    /* function library data */
constexpr const int RDBOpcodeFunction = 246;     /* old function library data for 7.0 rc1 and rc2 */
constexpr const int RDBOpcodeModuleAux = 247;    /* Module auxiliary data. */
constexpr const int RDBOpcodeIdle = 248;         /* LRU idle time. */
constexpr const int RDBOpcodeFreq = 249;         /* LFU frequency. */
constexpr const int RDBOpcodeAux = 250;          /* RDB aux field. */
constexpr const int RDBOpcodeResizeDB = 251;     /* Hash table resize hint. */
constexpr const int RDBOpcodeExpireTimeMs = 252; /* Expire time in milliseconds. */
constexpr const int RDBOpcodeExpireTime = 253;   /* Old expire time in seconds. */
constexpr const int RDBOpcodeSelectDB = 254;     /* DB number of the following keys. */
constexpr const int RDBOpcodeEof = 255;          /* End of the RDB file. */

constexpr const int SupportedRDBVersion = 10;  // not been tested for version 11, so use this version with caution.

constexpr const int RDBCheckSumLen = 8;                                        // rdb check sum length
constexpr const int RestoreRdbVersionLen = 2;                                  // rdb version len in restore string
constexpr const int RestoreFooterLen = RestoreRdbVersionLen + RDBCheckSumLen;  // 10 = ver len  + checksum len
constexpr const int MinRdbVersionToVerifyChecksum = 5;

template <typename T>
T LogWhenError(T &&s) {
  if (!s) {
    LOG(WARNING) << "Short read or unsupported type loading DB. Unrecoverable error, aborting now.";
    LOG(ERROR) << "Unexpected EOF reading RDB file";
  }
  return std::forward<T>(s);
}

Status RDB::VerifyPayloadChecksum(const std::string_view &payload) {
  if (payload.size() < RestoreFooterLen) {  // at least has rdb version and checksum
    return {Status::NotOK, "invalid payload length"};
  }
  auto footer = payload.substr(payload.size() - RestoreFooterLen);
  auto rdb_version = (footer[1] << 8) | footer[0];
  // For now, the max redis rdb version is 12
  if (rdb_version > MaxRDBVersion) {
    return {Status::NotOK, fmt::format("invalid or unsupported rdb version: {}", rdb_version)};
  }
  auto crc = GET_OR_RET(stream_->GetCheckSum());
  if (memcmp(&crc, footer.data() + RestoreRdbVersionLen, RDBCheckSumLen)) {
    return {Status::NotOK, "incorrect checksum"};
  }
  return Status::OK();
}

StatusOr<int> RDB::LoadObjectType() {
  auto type = GET_OR_RET(stream_->ReadByte());
  if (isObjectType(type)) {
    return type;
  }
  return {Status::NotOK, fmt::format("invalid or unsupported object type: {}", type)};
}

StatusOr<uint64_t> RDB::loadObjectLen(bool *is_encoded) {
  uint64_t len = 0;
  auto c = GET_OR_RET(stream_->ReadByte());
  auto type = (c & 0xC0) >> 6;
  switch (type) {
    case RDBEncVal:
      if (is_encoded) *is_encoded = true;
      return c & 0x3F;
    case RDB6BitLen:
      return c & 0x3F;
    case RDB14BitLen:
      len = c & 0x3F;
      return (len << 8) | GET_OR_RET(stream_->ReadByte());
    default:
      if (c == RDB32BitLen) {
        GET_OR_RET(stream_->Read(reinterpret_cast<char *>(&len), sizeof(uint32_t)));
        return ntohl(len);
      } else if (c == RDB64BitLen) {
        GET_OR_RET(stream_->Read(reinterpret_cast<char *>(&len), sizeof(uint64_t)));
        return ntohu64(len);
      } else {
        return {Status::NotOK, fmt::format("Unknown RDB string encoding type {} byte {}", type, c)};
      }
  }
}

StatusOr<std::string> RDB::LoadStringObject() { return loadEncodedString(); }

// Load the LZF compression string
StatusOr<std::string> RDB::loadLzfString() {
  auto compression_len = GET_OR_RET(loadObjectLen(nullptr));
  auto len = GET_OR_RET(loadObjectLen(nullptr));
  std::string out_buf(len, 0);
  std::vector<char> vec(compression_len);
  GET_OR_RET(stream_->Read(vec.data(), compression_len));

  if (lzf_decompress(vec.data(), compression_len, out_buf.data(), len) != len) {
    return {Status::NotOK, "Invalid LZF compressed string"};
  }
  return out_buf;
}

StatusOr<std::string> RDB::loadEncodedString() {
  bool is_encoded = false;
  auto len = GET_OR_RET(loadObjectLen(&is_encoded));
  if (is_encoded) {
    // For integer type, needs to convert to uint8_t* to avoid signed extension
    unsigned char buf[4] = {0};
    if (len == RDBEncInt8) {
      auto next = GET_OR_RET(stream_->ReadByte());
      return std::to_string(static_cast<int>(next));
    } else if (len == RDBEncInt16) {
      GET_OR_RET(stream_->Read(reinterpret_cast<char *>(buf), 2));
      auto value = static_cast<uint16_t>(buf[0]) | (static_cast<uint16_t>(buf[1]) << 8);
      return std::to_string(static_cast<int16_t>(value));
    } else if (len == RDBEncInt32) {
      GET_OR_RET(stream_->Read(reinterpret_cast<char *>(buf), 4));
      auto value = static_cast<uint32_t>(buf[0]) | (static_cast<uint32_t>(buf[1]) << 8) |
                   (static_cast<uint32_t>(buf[2]) << 16) | (static_cast<uint32_t>(buf[3]) << 24);
      return std::to_string(static_cast<int32_t>(value));
    } else if (len == RDBEncLzf) {
      return loadLzfString();
    } else {
      return {Status::NotOK, fmt::format("Unknown RDB string encoding type {}", len)};
    }
  }

  // Normal string
  if (len == 0) {
    return "";
  }
  std::string read_string;
  read_string.resize(len);
  GET_OR_RET(stream_->Read(read_string.data(), len));
  return read_string;
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

    if (container == QuickListNodeContainerPlain) {
      auto element = GET_OR_RET(loadEncodedString());
      list.push_back(std::move(element));
      continue;
    }

    auto encoded_string = GET_OR_RET(loadEncodedString());
    if (type == RDBTypeListQuickList2) {
      ListPack lp(encoded_string);
      auto elements = GET_OR_RET(lp.Entries());
      list.insert(list.end(), std::make_move_iterator(elements.begin()), std::make_move_iterator(elements.end()));
    } else {
      ZipList zip_list(encoded_string);
      auto elements = GET_OR_RET(zip_list.Entries());
      list.insert(list.end(), std::make_move_iterator(elements.begin()), std::make_move_iterator(elements.end()));
    }
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
    list.push_back(std::move(element));
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
    set.push_back(std::move(element));
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
    hash[field] = std::move(value);
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
  double value = 0;
  GET_OR_RET(stream_->Read(reinterpret_cast<char *>(&value), 8));
  memrev64ifbe(&value);
  return value;
}

StatusOr<double> RDB::loadDouble() {
  char buf[256];
  auto len = GET_OR_RET(stream_->ReadByte());
  switch (len) {
    case 255:
      return -INFINITY; /* Negative inf */
    case 254:
      return INFINITY; /* Positive inf */
    case 253:
      return NAN; /* NaN */
  }
  GET_OR_RET(stream_->Read(buf, len));
  buf[len] = '\0';
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
    auto score = GET_OR_RET(ParseFloat<double>(entries[i + 1]));
    zset.emplace_back(MemberScore{entries[i], score});
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
    auto score = GET_OR_RET(ParseFloat<double>(entries[i + 1]));
    zset.emplace_back(MemberScore{entries[i], score});
  }
  return zset;
}

Status RDB::Restore(const std::string &key, std::string_view payload, uint64_t ttl_ms) {
  rocksdb::Status db_status;

  // Check the checksum of the payload
  GET_OR_RET(VerifyPayloadChecksum(payload));

  auto type = GET_OR_RET(LoadObjectType());

  auto value = GET_OR_RET(loadRdbObject(type, key));

  return saveRdbObject(type, key, value, ttl_ms);  // NOLINT
}

StatusOr<int> RDB::loadRdbType() {
  auto type = GET_OR_RET(stream_->ReadByte());
  return type;
}

StatusOr<RedisObjValue> RDB::loadRdbObject(int type, const std::string &key) {
  if (type == RDBTypeString) {
    auto value = GET_OR_RET(LoadStringObject());
    return value;
  } else if (type == RDBTypeSet || type == RDBTypeSetIntSet || type == RDBTypeSetListPack) {
    std::vector<std::string> members;
    if (type == RDBTypeSet) {
      members = GET_OR_RET(LoadSetObject());
    } else if (type == RDBTypeSetListPack) {
      members = GET_OR_RET(LoadSetWithListPack());
    } else {
      members = GET_OR_RET(LoadSetWithIntSet());
    }
    return members;
  } else if (type == RDBTypeZSet || type == RDBTypeZSet2 || type == RDBTypeZSetListPack || type == RDBTypeZSetZipList) {
    std::vector<MemberScore> member_scores;
    if (type == RDBTypeZSet || type == RDBTypeZSet2) {
      member_scores = GET_OR_RET(LoadZSetObject(type));
    } else if (type == RDBTypeZSetListPack) {
      member_scores = GET_OR_RET(LoadZSetWithListPack());
    } else {
      member_scores = GET_OR_RET(LoadZSetWithZipList());
    }
    return member_scores;
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
    return entries;
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
    return elements;
  }

  return {Status::RedisParseErr, fmt::format("unsupported type: {}", type)};
}

Status RDB::saveRdbObject(int type, const std::string &key, const RedisObjValue &obj, uint64_t ttl_ms) {
  rocksdb::Status db_status;
  if (type == RDBTypeString) {
    const auto &value = std::get<std::string>(obj);
    redis::String string_db(storage_, ns_);
    uint64_t expire = 0;
    if (ttl_ms > 0) {
      expire = ttl_ms + util::GetTimeStampMS();
    }
    db_status = string_db.SetEX(key, value, expire);
  } else if (type == RDBTypeSet || type == RDBTypeSetIntSet || type == RDBTypeSetListPack) {
    const auto &members = std::get<std::vector<std::string>>(obj);
    redis::Set set_db(storage_, ns_);
    uint64_t count = 0;
    std::vector<Slice> insert_members;
    insert_members.reserve(members.size());
    for (const auto &member : members) {
      insert_members.emplace_back(member);
    }
    db_status = set_db.Add(key, insert_members, &count);
  } else if (type == RDBTypeZSet || type == RDBTypeZSet2 || type == RDBTypeZSetListPack || type == RDBTypeZSetZipList) {
    const auto &member_scores = std::get<std::vector<MemberScore>>(obj);
    redis::ZSet zset_db(storage_, ns_);
    uint64_t count = 0;
    db_status = zset_db.Add(key, ZAddFlags(0), const_cast<std::vector<MemberScore> *>(&member_scores), &count);
  } else if (type == RDBTypeHash || type == RDBTypeHashListPack || type == RDBTypeHashZipList ||
             type == RDBTypeHashZipMap) {
    const auto &entries = std::get<std::map<std::string, std::string>>(obj);
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
    const auto &elements = std::get<std::vector<std::string>>(obj);
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
    return {Status::RedisExecErr, fmt::format("unsupported save type: {}", type)};
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

StatusOr<uint32_t> RDB::loadExpiredTimeSeconds() {
  uint32_t t32 = 0;
  GET_OR_RET(stream_->Read(reinterpret_cast<char *>(&t32), 4));
  return t32;
}

StatusOr<uint64_t> RDB::loadExpiredTimeMilliseconds(int rdb_version) {
  uint64_t t64 = 0;
  GET_OR_RET(stream_->Read(reinterpret_cast<char *>(&t64), 8));
  /* before Redis 5 (RDB version 9), the function
   * failed to convert data to/from little endian, so RDB files with keys having
   * expires could not be shared between big endian and little endian systems
   * (because the expire time will be totally wrong). comment from src/rdb.c: rdbLoadMillisecondTime*/
  if (rdb_version >= 9) {
    memrev64ifbe(&t64);
  }
  return t64;
}

bool RDB::isEmptyRedisObject(const RedisObjValue &value) {
  if (auto vec_str_ptr = std::get_if<std::vector<std::string>>(&value)) {
    return vec_str_ptr->size() == 0;
  }
  if (auto vec_mem_ptr = std::get_if<std::vector<MemberScore>>(&value)) {
    return vec_mem_ptr->size() == 0;
  }
  if (auto map_ptr = std::get_if<std::map<std::string, std::string>>(&value)) {
    return map_ptr->size() == 0;
  }

  return false;
}

// Load RDB file: copy from redis/src/rdb.c:branch 7.0, 76b9c13d.
Status RDB::LoadRdb(uint32_t db_index, bool overwrite_exist_key) {
  char buf[1024] = {0};
  GET_OR_RET(LogWhenError(stream_->Read(buf, 9)));
  buf[9] = '\0';

  if (memcmp(buf, "REDIS", 5) != 0) {
    LOG(WARNING) << "Wrong signature trying to load DB from file";
    return {Status::NotOK, "Wrong signature trying to load DB from file"};
  }

  auto rdb_ver = std::atoi(buf + 5);
  if (rdb_ver < 1 || rdb_ver > SupportedRDBVersion) {
    LOG(WARNING) << "Can't handle RDB format version " << rdb_ver;
    return {Status::NotOK, fmt::format("Can't handle RDB format version {}", rdb_ver)};
  }

  uint64_t expire_time = 0;
  int64_t expire_keys = 0;
  int64_t load_keys = 0;
  int64_t empty_keys_skipped = 0;
  auto now = util::GetTimeStampMS();
  uint32_t db_id = 0;
  uint64_t skip_exist_keys = 0;
  while (true) {
    auto type = GET_OR_RET(LogWhenError(loadRdbType()));
    if (type == RDBOpcodeExpireTime) {
      expire_time = static_cast<uint64_t>(GET_OR_RET(LogWhenError(loadExpiredTimeSeconds())));
      expire_time *= 1000;
      continue;
    } else if (type == RDBOpcodeExpireTimeMs) {
      expire_time = GET_OR_RET(LogWhenError(loadExpiredTimeMilliseconds(rdb_ver)));
      continue;
    } else if (type == RDBOpcodeFreq) {               // LFU frequency: not use in kvrocks
      GET_OR_RET(LogWhenError(stream_->ReadByte()));  // discard the value
      continue;
    } else if (type == RDBOpcodeIdle) {  // LRU idle time: not use in kvrocks
      uint64_t discard = 0;
      GET_OR_RET(LogWhenError(stream_->Read(reinterpret_cast<char *>(&discard), sizeof(uint64_t))));
      continue;
    } else if (type == RDBOpcodeEof) {
      break;
    } else if (type == RDBOpcodeSelectDB) {
      db_id = GET_OR_RET(LogWhenError(loadObjectLen(nullptr)));
      continue;
    } else if (type == RDBOpcodeResizeDB) {              // not use in kvrocks, hint redis for hash table resize
      GET_OR_RET(LogWhenError(loadObjectLen(nullptr)));  // db_size
      GET_OR_RET(LogWhenError(loadObjectLen(nullptr)));  // expires_size
      continue;
    } else if (type == RDBOpcodeAux) {
      /* AUX: generic string-string fields. Use to add state to RDB
       * which is backward compatible. Implementations of RDB loading
       * are required to skip AUX fields they don't understand.
       *
       * An AUX field is composed of two strings: key and value. */
      auto key = GET_OR_RET(LogWhenError(LoadStringObject()));
      auto value = GET_OR_RET(LogWhenError(LoadStringObject()));
      continue;
    } else if (type == RDBOpcodeModuleAux) {
      LOG(WARNING) << "RDB module not supported";
      return {Status::NotOK, "RDB module not supported"};
    } else if (type == RDBOpcodeFunction || type == RDBOpcodeFunction2) {
      LOG(WARNING) << "RDB function not supported";
      return {Status::NotOK, "RDB function not supported"};
    } else {
      if (!isObjectType(type)) {
        LOG(WARNING) << "Invalid or Not supported object type: " << type;
        return {Status::NotOK, fmt::format("Invalid or Not supported object type {}", type)};
      }
    }

    auto key = GET_OR_RET(LogWhenError(LoadStringObject()));
    auto value = GET_OR_RET(LogWhenError(loadRdbObject(type, key)));

    if (db_index != db_id) {  // skip db not match
      continue;
    }

    if (isEmptyRedisObject(value)) {  // compatible with empty value
      /* Since we used to have bug that could lead to empty keys
       * (See #8453), we rather not fail when empty key is encountered
       * in an RDB file, instead we will silently discard it and
       * continue loading. */
      if (empty_keys_skipped++ < 10) {  // only log 10 empty keys, just as redis does.
        LOG(WARNING) << "skipping empty key: " << key;
      }
      continue;
    } else if (expire_time != 0 &&
               expire_time < now) {  // in redis this used to feed this deletion to any connected replicas
      expire_keys++;
      continue;
    }

    if (!overwrite_exist_key) {  // only load not exist key
      redis::Database redis(storage_, ns_);
      auto s = redis.KeyExist(key);
      if (!s.IsNotFound()) {
        skip_exist_keys++;  // skip it even it's not okay
        if (!s.ok()) {
          LOG(ERROR) << "check key " << key << " exist failed: " << s.ToString();
        }
        continue;
      }
    }

    auto ret = saveRdbObject(type, key, value, expire_time);
    if (!ret.IsOK()) {
      LOG(WARNING) << "save rdb object key " << key << " failed: " << ret.Msg();
    } else {
      load_keys++;
    }
  }

  // Verify the checksum if RDB version is >= 5
  if (rdb_ver >= MinRdbVersionToVerifyChecksum) {
    uint64_t chk_sum = 0;
    auto expected = GET_OR_RET(LogWhenError(stream_->GetCheckSum()));
    GET_OR_RET(LogWhenError(stream_->Read(reinterpret_cast<char *>(&chk_sum), RDBCheckSumLen)));
    if (chk_sum == 0) {
      LOG(WARNING) << "RDB file was saved with checksum disabled: no check performed.";
    } else if (chk_sum != expected) {
      LOG(WARNING) << "Wrong RDB checksum expected: " << chk_sum << " got: " << expected;
      return {Status::NotOK, "All objects were processed and loaded but the checksum is unexpected!"};
    }
  }

  std::string skip_info = (overwrite_exist_key ? ", exist keys skipped: " + std::to_string(skip_exist_keys) : "");

  LOG(INFO) << "Done loading RDB,  keys loaded: " << load_keys << ", keys expired:" << expire_keys
            << ", empty keys skipped: " << empty_keys_skipped << skip_info;

  return Status::OK();
}

Status RDB::Dump(const std::string &key, const RedisType type) {
  unsigned char buf[2];
  /* Serialize the object in an RDB-like format. It consist of an object type
   * byte followed by the serialized object. This is understood by RESTORE. */
  auto s = SaveObjectType(type);
  if (!s.IsOK()) return {Status::RedisExecErr, s.Msg()};
  s = SaveObject(key, type);
  if (!s.IsOK()) return {Status::RedisExecErr, s.Msg()};

  /* Write the footer, this is how it looks like:
   * ----------------+---------------------+---------------+
   * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
   * ----------------+---------------------+---------------+
   * RDB version and CRC are both in little endian.
   */

  // We should choose the minimum RDB version for compatibility consideration.
  // For the current DUMP implementation, it was supported since from the Redis 2.6,
  // so we choose the RDB version of Redis 2.6 as the minimum version.
  buf[0] = MinRDBVersion & 0xff;
  buf[1] = (MinRDBVersion >> 8) & 0xff;
  s = stream_->Write((const char *)buf, 2);
  if (!s.IsOK()) {
    return {Status::RedisExecErr, s.Msg()};
  }

  /* CRC64 */
  CHECK(dynamic_cast<RdbStringStream *>(stream_.get()) != nullptr);
  std::string &output = static_cast<RdbStringStream *>(stream_.get())->GetInput();
  uint64_t crc = crc64(0, (unsigned char *)(output.c_str()), output.length());
  memrev64ifbe(&crc);
  s = stream_->Write((const char *)(&crc), 8);
  if (!s.IsOK()) {
    return {Status::RedisExecErr, s.Msg()};
  }

  return Status::OK();
}

Status RDB::SaveObjectType(const RedisType type) {
  int robj_type = -1;
  if (type == kRedisString) {
    robj_type = RDBTypeString;
  } else if (type == kRedisHash) {
    robj_type = RDBTypeHash;
  } else if (type == kRedisList) {
    robj_type = RDBTypeListQuickList2;
  } else if (type == kRedisSet) {
    robj_type = RDBTypeSet;
  } else if (type == kRedisZSet) {
    robj_type = RDBTypeZSet2;
  } else {
    LOG(WARNING) << "Invalid or Not supported object type: " << type;
    return {Status::NotOK, "Invalid or Not supported object type"};
  }
  return stream_->Write((const char *)(&robj_type), 1);
}

Status RDB::SaveObject(const std::string &key, const RedisType type) {
  if (type == kRedisString) {
    std::string value;
    redis::String string_db(storage_, ns_);
    auto s = string_db.Get(key, &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    return SaveStringObject(value);
  } else if (type == kRedisList) {
    std::vector<std::string> elems;
    redis::List list_db(storage_, ns_);
    auto s = list_db.Range(key, 0, -1, &elems);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    return SaveListObject(elems);
  } else if (type == kRedisSet) {
    redis::Set set_db(storage_, ns_);
    std::vector<std::string> members;
    auto s = set_db.Members(key, &members);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    return SaveSetObject(members);
  } else if (type == kRedisZSet) {
    redis::ZSet zset_db(storage_, ns_);
    std::vector<MemberScore> member_scores;
    RangeScoreSpec spec;
    auto s = zset_db.RangeByScore(key, spec, &member_scores, nullptr);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    std::sort(member_scores.begin(), member_scores.end(),
              [](const MemberScore &v1, const MemberScore &v2) { return v1.score > v2.score; });
    return SaveZSetObject(member_scores);
  } else if (type == kRedisHash) {
    redis::Hash hash_db(storage_, ns_);
    std::vector<FieldValue> field_values;
    auto s = hash_db.GetAll(key, &field_values);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    return SaveHashObject(field_values);
  } else {
    LOG(WARNING) << "Invalid or Not supported object type: " << type;
    return {Status::NotOK, "Invalid or Not supported object type"};
  }
}

Status RDB::RdbSaveLen(uint64_t len) {
  unsigned char buf[2];
  if (len < (1 << 6)) {
    /* Save a 6 bit len */
    buf[0] = (len & 0xFF) | (RDB6BitLen << 6);
    auto status = stream_->Write((const char *)buf, 1);
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }
    return Status::OK();
  } else if (len < (1 << 14)) {
    /* Save a 14 bit len */
    buf[0] = ((len >> 8) & 0xFF) | (RDB14BitLen << 6);
    buf[1] = len & 0xFF;
    auto status = stream_->Write((const char *)buf, 2);
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }
    return Status::OK();
  } else if (len <= UINT32_MAX) {
    /* Save a 32 bit len */
    buf[0] = RDB32BitLen;
    auto status = stream_->Write((const char *)buf, 1);
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }

    uint32_t len32 = htonl(len);
    status = stream_->Write((const char *)(&len32), 4);
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }
    return Status::OK();
  } else {
    /* Save a 64 bit len */
    buf[0] = RDB64BitLen;
    auto status = stream_->Write((const char *)buf, 1);
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }

    len = htonu64(len);
    status = stream_->Write((const char *)(&len), 8);
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }
    return Status::OK();
  }
}

Status RDB::SaveStringObject(const std::string &value) {
  const size_t len = value.length();
  int enclen = 0;

  // When the length is less than 11, value may be an integer,
  // so special encoding is performed.
  if (len <= 11) {
    unsigned char buf[5];
    // convert string to long long
    auto parse_result = ParseInt<long long>(value, 10);
    if (parse_result) {
      long long integer_value = *parse_result;
      // encode integer
      enclen = rdbEncodeInteger(integer_value, buf);
      if (enclen > 0) {
        auto status = stream_->Write((const char *)buf, enclen);
        if (!status.IsOK()) {
          return {Status::RedisExecErr, status.Msg()};
        }
        return Status::OK();
      }
    }
  }

  // Since we do not support rdb compression,
  // the lzf encoding method has not been implemented yet.

  /* Store verbatim */
  auto status = RdbSaveLen(value.length());
  if (!status.IsOK()) {
    return {Status::RedisExecErr, status.Msg()};
  }
  if (value.length() > 0) {
    status = stream_->Write(value.c_str(), value.length());
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }
  }
  return Status::OK();
}

Status RDB::SaveListObject(const std::vector<std::string> &elems) {
  if (elems.size() > 0) {
    auto status = RdbSaveLen(elems.size());
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }

    for (const auto &elem : elems) {
      status = RdbSaveLen(1 /*plain container mode */);
      if (!status.IsOK()) {
        return {Status::RedisExecErr, status.Msg()};
      }

      status = SaveStringObject(elem);
      if (!status.IsOK()) {
        return {Status::RedisExecErr, status.Msg()};
      }
    }
  } else {
    LOG(WARNING) << "the size of elems is zero";
    return {Status::NotOK, "the size of elems is zero"};
  }
  return Status::OK();
}

Status RDB::SaveSetObject(const std::vector<std::string> &members) {
  if (members.size() > 0) {
    auto status = RdbSaveLen(members.size());
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }

    for (const auto &elem : members) {
      status = SaveStringObject(elem);
      if (!status.IsOK()) {
        return {Status::RedisExecErr, status.Msg()};
      }
    }
  } else {
    LOG(WARNING) << "the size of elems is zero";
    return {Status::NotOK, "the size of elems is zero"};
  }
  return Status::OK();
}

Status RDB::SaveZSetObject(const std::vector<MemberScore> &member_scores) {
  if (member_scores.size() > 0) {
    auto status = RdbSaveLen(member_scores.size());
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }

    for (const auto &elem : member_scores) {
      status = SaveStringObject(elem.member);
      if (!status.IsOK()) {
        return {Status::RedisExecErr, status.Msg()};
      }

      status = rdbSaveBinaryDoubleValue(elem.score);
      if (!status.IsOK()) {
        return {Status::RedisExecErr, status.Msg()};
      }
    }
  } else {
    LOG(WARNING) << "the size of member_scores is zero";
    return {Status::NotOK, "the size of ZSet is 0"};
  }
  return Status::OK();
}

Status RDB::SaveHashObject(const std::vector<FieldValue> &field_values) {
  if (field_values.size() > 0) {
    auto status = RdbSaveLen(field_values.size());
    if (!status.IsOK()) {
      return {Status::RedisExecErr, status.Msg()};
    }

    for (const auto &p : field_values) {
      status = SaveStringObject(p.field);
      if (!status.IsOK()) {
        return {Status::RedisExecErr, status.Msg()};
      }

      status = SaveStringObject(p.value);
      if (!status.IsOK()) {
        return {Status::RedisExecErr, status.Msg()};
      }
    }
  } else {
    LOG(WARNING) << "the size of field_values is zero";
    return {Status::NotOK, "the size of Hash is 0"};
  }
  return Status::OK();
}

int RDB::rdbEncodeInteger(const long long value, unsigned char *enc) {
  if (value >= -(1 << 7) && value <= (1 << 7) - 1) {
    enc[0] = (RDBEncVal << 6) | RDBEncInt8;
    enc[1] = value & 0xFF;
    return 2;
  } else if (value >= -(1 << 15) && value <= (1 << 15) - 1) {
    enc[0] = (RDBEncVal << 6) | RDBEncInt16;
    enc[1] = value & 0xFF;
    enc[2] = (value >> 8) & 0xFF;
    return 3;
  } else if (value >= -((long long)1 << 31) && value <= ((long long)1 << 31) - 1) {
    enc[0] = (RDBEncVal << 6) | RDBEncInt32;
    enc[1] = value & 0xFF;
    enc[2] = (value >> 8) & 0xFF;
    enc[3] = (value >> 16) & 0xFF;
    enc[4] = (value >> 24) & 0xFF;
    return 5;
  } else {
    return 0;
  }
}

Status RDB::rdbSaveBinaryDoubleValue(double val) {
  memrev64ifbe(&val);
  return stream_->Write((const char *)(&val), sizeof(val));
}
