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
#define RDB_VERSION 6
#define RDB_6BITLEN 0
#define RDB_14BITLEN 1
#define RDB_32BITLEN 0x80

#define RDB_OPCODE_EXPIRETIME 253
#define RDB_OPCODE_SELECTDB 254
#define RDB_OPCODE_EOF 255

#define ENCODE_INT_ERR 0

#define RDB_TYPE_STRING 0
#define RDB_TYPE_LIST 1
#define RDB_TYPE_SET 2
#define RDB_TYPE_ZSET 3
#define RDB_TYPE_HASH 4

#define RDB_ENCVAL 3
#define RDB_ENC_INT8 0  /* 8 bit signed integer */
#define RDB_ENC_INT16 1 /* 16 bit signed integer */
#define RDB_ENC_INT32 2 /* 32 bit signed integer */

#pragma once

#include <fstream>
#include <string>

#include "redis_db.h"

namespace Redis {
class RedisDatabase : public Database {
 public:
  RedisDatabase(Engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  Status Dump(const std::string &file_name);

 private:
  class Util {
   public:
    explicit Util(const std::string &file_name);
    ~Util();
    Status SaveMetaKeyPair(const std::string &key,
                            const int &remain,
                            const RedisType &type);
    Status SaveStringObject(const std::string &str);
    Status SaveLen(uint64_t len);
    Status SaveMeta();
    Status SaveTail();

   private:
    std::ofstream file_;
    Status saveType(unsigned char type);
    Status saveObjectType(const RedisType &type);
    Status saveSecondTime(const int32_t &remain);
    Status saveRaw(const void *p, const int &len);
    int tryIntegerEncoding(const char *s, const uint64_t &len, unsigned char *enc);
    int ll2string(char *dst, size_t dstlen, int64_t svalue);
    int encodeInteger(int64_t value, unsigned char *enc);
    uint32_t digits10(uint64_t v);
  };
};

}  // namespace Redis
