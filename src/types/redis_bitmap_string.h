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

#include <string>
#include <vector>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace Redis {

class BitmapString : public Database {
 public:
  BitmapString(Engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  static rocksdb::Status GetBit(const std::string &raw_value, uint32_t offset, bool *bit);
  rocksdb::Status SetBit(const Slice &ns_key, std::string *raw_value, uint32_t offset, bool new_bit, bool *old_bit);
  static rocksdb::Status BitCount(const std::string &raw_value, int64_t start, int64_t stop, uint32_t *cnt);
  static rocksdb::Status BitPos(const std::string &raw_value, bool bit, int64_t start, int64_t stop, bool stop_given,
                                int64_t *pos);

 private:
  static size_t redisPopcount(unsigned char *p, int64_t count);
  static int64_t redisBitpos(unsigned char *c, int64_t count, int bit);
};

}  // namespace Redis
