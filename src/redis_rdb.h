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

#include <fstream>
#include <string>

#include "redis_db.h"

namespace Redis {
class RedisDatabase : public Database {
 public:
  RedisDatabase(Engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  Status Dump(const std::string &file_name);

 private:
  class Composer {
   public:
    explicit Composer(const std::string &file_name);
    Status SaveMetaKeyPair(const std::string &key, int remain, const RedisType &type);
    Status SaveStringObject(const std::string &str);
    Status SaveLen(uint64_t len);
    Status SaveMeta();
    Status SaveTail();

   private:
    std::ofstream file_;
    Status saveType(unsigned char type);
    Status saveObjectType(const RedisType &type);
    Status saveSecondTime(int32_t remain);
    Status saveRaw(const void *p, uint32_t len);
    int tryIntegerEncoding(const char *s, size_t len, unsigned char *enc);
    int ll2string(char *dst, size_t dstlen, int64_t svalue);
    int encodeInteger(int64_t value, unsigned char *enc);
    uint32_t digits10(uint64_t v);
  };
};

}  // namespace Redis
