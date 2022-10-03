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

#include <string>
#include <vector>

#include "redis_string.h"
#include "redis_list.h"
#include "redis_set.h"
#include "redis_hash.h"
#include "redis_zset.h"
#include "redis_bitmap.h"
#include "db_util.h"

#include "redis_rdb.h"


namespace Redis {
Status RedisDatabase::Dump(const std::string &file_name) {
  Redis::String string_db(storage_, namespace_);
  Redis::Hash hash_db(storage_, namespace_);
  Redis::List list_db(storage_, namespace_);
  Redis::Set set_db(storage_, namespace_);
  Redis::ZSet zset_db(storage_, namespace_);
  Redis::Bitmap bitmap_db(storage_, namespace_);

  std::vector<FieldValue> field_values;
  std::vector<std::string> members;
  std::vector<std::string> elems;
  std::vector<MemberScore> member_scores;
  std::string value;

  rocksdb::ReadOptions read_options;
  read_options.snapshot = db_->GetSnapshot();
  auto iter = DBUtil::UniqueIterator(db_, read_options, metadata_cf_handle_);

  std::string ns_prefix;
  AppendNamespacePrefix(std::string(), &ns_prefix);

  Status s;
  rocksdb::Status rs;
  Util util(file_name);
  s = util.SaveMeta();
  if (!s.IsOK()) return s;
  for (iter->Seek(ns_prefix); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(ns_prefix)) {
        break;
    }
    Metadata metadata(kRedisNone, false);
    metadata.Decode(iter->value().ToString());
    if (metadata.Expired()) {
        continue;
    }
    std::string ns, user_key;
    ExtractNamespaceKey(iter->key(), &ns, &user_key, storage_->IsSlotIdEncoded());
    int32_t ttl = metadata.TTL();
    RedisType type = metadata.Type();

    s = util.SaveMetaKeyPair(user_key, ttl, type);
    if (!s.IsOK()) return s;
    switch (type) {
        case kRedisString:
            rs = string_db.Get(user_key, &value);
            if (rs.ok()) {
               s = util.SaveStringObject(value);
               if (!s.IsOK()) return s;
            }
            break;
        case kRedisHash:
            rs = hash_db.GetAll(user_key, &field_values, HashFetchType::kAll);
            if (rs.ok()) {
                s = util.SaveLen(field_values.size());
                if (!s.IsOK()) return s;
                for (const auto fv : field_values) {
                    s = util.SaveStringObject(fv.field);
                    if (!s.IsOK()) return s;
                    s = util.SaveStringObject(fv.value);
                    if (!s.IsOK()) return s;
                }
            }
            break;
        case kRedisList:
            rs = list_db.Range(user_key, 0, -1, &elems);
            if (rs.ok()) {
                s = util.SaveLen(elems.size());
                if (!s.IsOK()) return s;
                for (const auto item : elems) {
                    s = util.SaveStringObject(item);
                    if (!s.IsOK()) return s;
                }
            }
            break;
        case kRedisSet:
            rs = set_db.Members(user_key, &members);
            if (rs.ok()) {
                s = util.SaveLen(members.size());
                if (!s.IsOK()) return s;
                for (const auto &item : members) {
                    s = util.SaveStringObject(item);
                    if (!s.IsOK()) return s;
                }
            }
            break;
        case kRedisZSet:
            rs = zset_db.Range(user_key, 0, -1, 0, &member_scores);
            if (rs.ok()) {
                s = util.SaveLen(member_scores.size());
                for (const auto &item : member_scores) {
                    s = util.SaveStringObject(item.member);
                    if (!s.IsOK()) return s;
                    std::string score = std::to_string(item.score);
                    s = util.SaveStringObject(score);
                    if (!s.IsOK()) return s;
                }
            }
            break;
            case kRedisBitmap:
            rs = bitmap_db.GetString(user_key, UINT32_MAX, &value);
            if (rs.ok()) {
                s = util.SaveStringObject(value);
                if (!s.IsOK()) return s;
            }
            break;
        default:
            break;
    }
    if (!rs.ok()) return Status(Status::NotOK, rs.ToString());
  }
  s = util.SaveTail();
  return s;
}

RedisDatabase::Util::Util(const std::string &file_name) {
    file_.open(file_name, std::ofstream::binary);
}

RedisDatabase::Util::~Util() {
    file_.close();
}

Status RedisDatabase::Util::SaveMetaKeyPair(const std::string &key,
                                            const int32_t &remain,
                                            const RedisType &type) {
  Status s;
  if (remain != -1) {
      s = saveType(RDB_OPCODE_EXPIRETIME);
      if (!s.IsOK()) return s;
      s = saveSecondTime(remain);
      if (!s.IsOK()) return s;
  }
  s = saveObjectType(type);
  if (!s.IsOK()) return s;
  s = SaveStringObject(key);
  return s;
}

Status RedisDatabase::Util::SaveTail() {
    Status s;
    s = saveType(RDB_OPCODE_EOF);
    if (!s.IsOK()) return s;
    int32_t temp = 0;
    s = saveRaw(&temp, 4);
    if (!s.IsOK()) return s;
    s = saveRaw(&temp, 4);
    return s;
}

int RedisDatabase::Util::tryIntegerEncoding(const char *s, const uint64_t &len, unsigned char *enc) {
    int64_t value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
    value = strtoll(s, &endptr, 10);
    if (endptr[0] != '\0')
        return ENCODE_INT_ERR;
    ll2string(buf, 32, value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    if (strlen(buf) != len || memcmp(buf, s, len))
        return ENCODE_INT_ERR;

    return encodeInteger(value, enc);
}

uint32_t RedisDatabase::Util::digits10(uint64_t v) {
  if (v < 10)   return 1;
  if (v < 100)  return 2;
  if (v < 1000) return 3;
  if (v < 1000000000000UL) {
    if (v < 100000000UL) {
      if (v < 1000000) {
        if (v < 10000)  return 4;
        return 5 + (v >= 100000);
      }
      return 7 + (v >= 10000000UL);
    }
    if (v < 10000000000UL) {
      return 9 + (v >= 1000000000UL);
    }
    return 11 + (v >= 100000000000UL);
    }
  return 12 + digits10(v / 1000000000000UL);
}

int RedisDatabase::Util::encodeInteger(int64_t value, unsigned char *enc) {
    if (value >= -(1 << 7) && value <= (1 << 7) - 1) {
        enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT8;
        enc[1] = value & 0xFF;
        return 2;
    } else if (value >= -(1 << 15) && value <= (1 << 15) - 1) {
        enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT16;
        enc[1] = value & 0xFF;
        enc[2] = (value >> 8) & 0xFF;
        return 3;
    } else if (value >= -((int64_t)1 << 31) && value <= ((int64_t)1 << 31) - 1) {
        enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT32;
        enc[1] = value & 0xFF;
        enc[2] = (value >> 8) & 0xFF;
        enc[3] = (value >> 16) & 0xFF;
        enc[4] = (value >> 24) & 0xFF;
        return 5;
    } else {
        return ENCODE_INT_ERR;
    }
}

int RedisDatabase::Util::ll2string(char *dst, size_t dstlen, int64_t svalue) {
  static const char digits[201] =
      "0001020304050607080910111213141516171819"
      "2021222324252627282930313233343536373839"
      "4041424344454647484950515253545556575859"
      "6061626364656667686970717273747576777879"
      "8081828384858687888990919293949596979899";
  int negative;
  uint64_t value;

  /* The main loop works with 64bit unsigned integers for simplicity, so
     * we convert the number here and remember if it is negative. */
  if (svalue < 0) {
    if (svalue != LLONG_MIN) {
      value = -svalue;
    } else {
      value = ((uint64_t)LLONG_MAX) + 1;
    }
    negative = 1;
  } else {
    value = svalue;
    negative = 0;
  }

  /* Check length. */
  uint32_t const length = digits10(value) + negative;
  if (length >= dstlen) return 0;

  /* Null term. */
  uint32_t next = length;
  dst[next] = '\0';
  next--;
  while (value >= 100) {
    int const i = (value % 100) * 2;
    value /= 100;
    dst[next] = digits[i + 1];
    dst[next - 1] = digits[i];
    next -= 2;
  }

  /* Handle last 1-2 digits. */
  if (value < 10) {
    dst[next] = '0' + (uint32_t)value;
  } else {
    int i = (uint32_t)value * 2;
    dst[next] = digits[i + 1];
    dst[next - 1] = digits[i];
  }

  /* Add sign. */
  if (negative) dst[0] = '-';
  return length;
}

Status RedisDatabase::Util::SaveStringObject(const std::string &str) {
    Status s;
    auto len = str.length();
    int enclen;
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = tryIntegerEncoding(str.c_str(), len, buf)) > 0) {
            s = saveRaw(buf, enclen);
            return s;
        }
    }
    s = SaveLen(len);
    if (!s.IsOK()) {
        return s;
    }
    if (len > 0) {
       s = saveRaw(str.c_str(), len);
    }
    return s;
}

Status RedisDatabase::Util::SaveMeta() {
    char bytes[10];
    snprintf(bytes, sizeof(bytes), "REDIS%04d", RDB_VERSION);
    Status s;
    s = saveRaw(bytes, 9);
    if (!s.IsOK()) return s;
    s = saveType(RDB_OPCODE_SELECTDB);
    if (!s.IsOK()) return s;
    s = SaveLen(0);
    return s;
}

Status RedisDatabase::Util::SaveLen(uint64_t len) {
    unsigned char buf[2];
    Status s;
    if (len < (1 << 6)) {
        /* Save a 6 bit len */
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
        s = saveRaw(buf, 1);
        if (!s.IsOK()) return s;
    } else if (len < (1 << 14)) {
        /* Save a 14 bit len */
        buf[0] = ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        s = saveRaw(buf, 1);
        if (!s.IsOK()) return s;
    } else if (len <= UINT32_MAX) {
        /* Save a 32 bit len */
        buf[0] = RDB_32BITLEN;
         s = saveRaw(buf, 1);
        if (!s.IsOK()) return s;
        uint32_t len32 = htonl(len);
        s = saveRaw(&len32, 4);
    }
    return s;
}

Status RedisDatabase::Util::saveObjectType(const RedisType &type) {
    switch (type) {
        case kRedisString:
            return saveType(RDB_TYPE_STRING);
        case kRedisHash:
            return saveType(RDB_TYPE_HASH);
        case kRedisList:
            return saveType(RDB_TYPE_LIST);
        case kRedisSet:
            return saveType(RDB_TYPE_SET);
        case kRedisZSet:
            return saveType(RDB_TYPE_ZSET);
        case kRedisBitmap:
            return saveType(RDB_TYPE_STRING);
        default:
            return Status(Status::NotOK, "illegal type");
    }
}

Status RedisDatabase::Util::saveSecondTime(const int32_t &remain) {
    int64_t now;
    int32_t expiretime;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    expiretime = static_cast<int32_t>(now) + remain;
    return saveRaw(&expiretime, 4);
}

Status RedisDatabase::Util::saveType(unsigned char type) {
    return saveRaw(&type, 1);
}

Status RedisDatabase::Util::saveRaw(const void *p, const int &len) {
    file_.write((const char *)p, len);
    if (file_.bad())
        return Status(Status::NotOK, "write faild");
    return Status::OK();
}


}  // namespace Redis
