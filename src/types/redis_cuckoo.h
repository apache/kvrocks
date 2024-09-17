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
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, uint64_t element);
  rocksdb::Status AddNX(engine::Context &ctx, const Slice &user_key, uint64_t element);
  rocksdb::Status Count(engine::Context &ctx, const Slice &user_key, uint64_t count, uint64_t *ret);
  rocksdb::Status Del(engine::Context &ctx, const Slice &user_key, uint64_t element);
  rocksdb::Status Exists(engine::Context &ctx, const Slice &user_key, uint64_t element, bool *ret);
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, redis::CuckooFilterInfo *ret);
  rocksdb::Status Insert(engine::Context &ctx, const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements);
  rocksdb::Status InsertNX(engine::Context &ctx, const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements);
  rocksdb::Status LoadChunk(engine::Context &ctx, const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements);
  rocksdb::Status MExists(engine::Context &ctx, const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements);
  rocksdb::Status Reserve(engine::Context &ctx, const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements);
  rocksdb::Status ScanDump(engine::Context &ctx, const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements);
 
 private:
  [[nodiscard]] rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &ns_key, CuckooFilterMetadata *metadata);
};

} // namespace redis