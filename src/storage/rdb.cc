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

// The current support RDB version
constexpr const int RDBVersion = 10;

// NOLINTNEXTLINE
#define GET_OR_RETWITHLOG(...)                                                                         \
  ({                                                                                                   \
    auto &&status = (__VA_ARGS__);                                                                     \
    if (!status) {                                                                                     \
      LOG(WARNING) << "Short read or unsupported type loading DB. Unrecoverable error, aborting now."; \
      LOG(ERROR) << "Unexpected EOF reading RDB file";                                                 \
      return std::forward<decltype(status)>(status);                                                   \
    }                                                                                                  \
    std::forward<decltype(status)>(status);                                                            \
  }).GetValue()

Status RDB::VerifyPayloadChecksum(const std::string_view &payload) {
  if (payload.size() < 10) {
    return {Status::NotOK, "invalid payload length"};
  }
  auto footer = payload.substr(payload.size() - 10);
  auto rdb_version = (footer[1] << 8) | footer[0];
  // For now, the max redis rdb version is 11
  if (rdb_version > 11) {
    return {Status::NotOK, fmt::format("invalid or unsupported rdb version: {}", rdb_version)};
  }
  auto crc = GET_OR_RET(stream_->GetCheckSum());
  if (memcmp(&crc, footer.data() + 2, 8)) {
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
        GET_OR_RET(stream_->Read(reinterpret_cast<char *>(&len), 4));
        return ntohl(len);
      } else if (c == RDB64BitLen) {
        GET_OR_RET(stream_->Read(reinterpret_cast<char *>(&len), 8));
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
  std::vector<char> vec(len);
  GET_OR_RET(stream_->Read(vec.data(), len));
  return std::string(vec.data(), len);
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

  return saveRdbOject(type, key, value, ttl_ms); // NOLINT
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
  } else {
    return {Status::RedisParseErr, fmt::format("unsupported type: {}", type)};
  }

  // can't be here
  return Status::OK();
}

Status RDB::saveRdbOject(int type, const std::string &key, const RedisObjValue &obj, uint64_t ttl_ms) {
  rocksdb::Status db_status;
  if (type == RDBTypeString) {
    const auto &value = std::get<std::string>(obj);
    redis::String string_db(storage_, ns_);
    db_status = string_db.SetEX(key, value, ttl_ms);
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
    db_status = zset_db.Add(key, ZAddFlags(0), (redis::ZSet::MemberScores *)&member_scores, &count);
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

StatusOr<int32_t> RDB::loadTime() {
  int32_t t32 = 0;
  GET_OR_RET(stream_->Read(reinterpret_cast<char *>(&t32), 4));
  return t32;
}

StatusOr<int64_t> RDB::loadMillisecondTime(int rdb_version) {
  int64_t t64 = 0;
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

// Load RDB file: copy from redis/src/rdb.c£¨branch 7.0, 76b9c13d£©
Status RDB::LoadRdb() {
  char buf[1024] = {0};
  GET_OR_RETWITHLOG(stream_->Read(buf, 9));
  buf[9] = '\0';

  if (memcmp(buf, "REDIS", 5) != 0) {
    LOG(WARNING) << "Wrong signature trying to load DB from file";
    return {Status::NotOK};
  }

  auto rdb_ver = std::atoi(buf + 5);
  if (rdb_ver < 1 || rdb_ver > RDBVersion) {
    LOG(WARNING) << "Can't handle RDB format version " << rdb_ver;
    return {Status::NotOK};
  }

  int64_t expire_time = -1;
  int64_t expire_keys = 0;
  int64_t load_keys = 0;
  int64_t empty_keys_skipped = 0;
  auto now = static_cast<int64_t>(util::GetTimeStampMS());
  std::string cur_ns = ns_;  // current namespace, when select db, will change this value to ns + "_" + db_id

  while (true) {
    auto type = GET_OR_RETWITHLOG(loadRdbType());
    if (type == RDBOpcodeExpireTime) {
      expire_time = GET_OR_RETWITHLOG(loadTime());
      expire_time *= 1000;
      continue;
    } else if (type == RDBOpcodeExpireTimeMs) {
      expire_time = GET_OR_RETWITHLOG(loadMillisecondTime(rdb_ver));
      continue;
    } else if (type == RDBOpcodeFreq) {        // LFU frequency: not use in kvrocks
      GET_OR_RETWITHLOG(stream_->ReadByte());  // discard the value
      continue;
    } else if (type == RDBOpcodeIdle) {  // LRU idle time: not use in kvrocks
      uint64_t discard = 0;
      GET_OR_RETWITHLOG(stream_->Read(reinterpret_cast<char *>(&discard), 8));
      continue;
    } else if (type == RDBOpcodeEof) {
      break;
    } else if (type == RDBOpcodeSelectDB) {
      auto db_id = GET_OR_RETWITHLOG(loadObjectLen(nullptr));
      if (db_id != 0) {  // change namespace to ns + "_" + db_id
        cur_ns = ns_ + "_" + std::to_string(db_id);
      } else {  // use origin namespace
        cur_ns = ns_;
      }
      LOG(INFO) << "select db: " << db_id << ", change to namespace: " << cur_ns;
      continue;
    } else if (type == RDBOpcodeResizeDB) {       // not use in kvrocks, hint redis for hash table resize
      GET_OR_RETWITHLOG(loadObjectLen(nullptr));  // db_size
      GET_OR_RETWITHLOG(loadObjectLen(nullptr));  // expires_size
      continue;
    } else if (type == RDBOpcodeAux) {
      /* AUX: generic string-string fields. Use to add state to RDB
       * which is backward compatible. Implementations of RDB loading
       * are required to skip AUX fields they don't understand.
       *
       * An AUX field is composed of two strings: key and value. */
      auto key = GET_OR_RETWITHLOG(LoadStringObject());
      auto value = GET_OR_RETWITHLOG(LoadStringObject());
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
        return {Status::NotOK, "Invalid or Not supported object type"};
      }
    }

    auto key = GET_OR_RETWITHLOG(LoadStringObject());
    auto value = GET_OR_RETWITHLOG(loadRdbObject(type, key));

    if (isEmptyRedisObject(value)) {  // compatible with empty value
      /* Since we used to have bug that could lead to empty keys
       * (See #8453), we rather not fail when empty key is encountered
       * in an RDB file, instead we will silently discard it and
       * continue loading. */
      if (empty_keys_skipped++ < 10) {
        LOG(WARNING) << "skipping empty key: " << key;
      }
      continue;
    } else if (expire_time != -1 && expire_time < now) { // in redis this used to feed this deletion to any connected replicas
      expire_keys++;
      continue;
    }

    auto ret = saveRdbOject(type, key, value, expire_time);
    load_keys++;
    if (!ret.IsOK()) {
      LOG(WARNING) << "save rdb object key " << key << " failed: " << ret.Msg();
    }
  }

  // Verify the checksum if RDB version is >= 5
  if (rdb_ver >= 5) {
    uint64_t chk_sum = 0;
    auto expected = GET_OR_RETWITHLOG(stream_->GetCheckSum());
    GET_OR_RETWITHLOG(stream_->Read(reinterpret_cast<char *>(&chk_sum), 8));
    if (chk_sum == 0) {
      LOG(WARNING) << "RDB file was saved with checksum disabled: no check performed.";
    } else if (chk_sum != expected) {
      LOG(WARNING) << "Wrong RDB checksum expected: " << chk_sum << " got: " << expected;
      return {Status::NotOK, "Wrong RDB checksum"};
    }
  }

  if (empty_keys_skipped > 0) {
    LOG(INFO) << "Done loading RDB,  keys loaded: " << load_keys << ", keys expired:" << expire_keys
              << ", empty keys skipped: " << empty_keys_skipped;
  } else {
    LOG(INFO) << "Done loading RDB,  keys loaded: " << load_keys << ", keys expired:" << expire_keys;
  }

  return Status::OK();
}