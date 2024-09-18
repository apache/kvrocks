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

#include "cuckoo.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "vendor/murmurhash2.h"

namespace redis {

struct CuckooFilterInfo {
  uint64_t size;
  uint32_t num_buckets;
  uint16_t num_filters;
  uint64_t num_items;
  uint64_t num_deletes;
  uint16_t bucket_size;
  uint16_t expansion;
  uint16_t max_iterations;
};

class CFilter : public Database {
 public:
  explicit CFilter(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, const std::string &element, int *ret);
  rocksdb::Status AddNX(engine::Context &ctx, const Slice &user_key, const std::string &element, int *ret);
  rocksdb::Status Count(engine::Context &ctx, const Slice &user_key, const std::string &element, uint64_t *ret);
  rocksdb::Status Del(engine::Context &ctx, const Slice &user_key, const std::string &element, int *ret);
  rocksdb::Status Exists(engine::Context &ctx, const Slice &user_key, const std::string &element, int *ret);
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, redis::CuckooFilterInfo *ret);
  rocksdb::Status Insert(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements, std::vector<int> *ret, uint64_t capacity, bool no_create);
  rocksdb::Status InsertNX(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements, std::vector<int> *ret, uint64_t capacity, bool no_create);
  rocksdb::Status MExists(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements, std::vector<int> *ret);
  rocksdb::Status Reserve(engine::Context &ctx, const Slice &user_key, uint64_t capacity, uint8_t bucket_size, uint16_t max_iterations, uint16_t expansion);

 private:
  [[nodiscard]] rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &ns_key, CuckooFilterMetadata *metadata);
  [[nodiscard]] static uint64_t cfMHash(const std::string &str, uint32_t seed) {
      return HllMurMurHash64A(str.data(), static_cast<int>(str.size()), seed);
  }
  static void updateMetadata(CuckooFilter &cf, CuckooFilterMetadata *metadata);
};

} // namespace redis