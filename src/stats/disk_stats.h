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

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

class Disk : public Database {
 public:
  explicit Disk(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {
    option_.include_memtables = true;
    option_.include_files = true;
  }
  rocksdb::Status GetApproximateSizes(const Metadata &metadata, const Slice &ns_key,
                                      rocksdb::ColumnFamilyHandle *column_family, uint64_t *key_size,
                                      Slice subkeyleft = Slice(), Slice subkeyright = Slice());
  rocksdb::Status GetStringSize(const Slice &ns_key, uint64_t *key_size);
  rocksdb::Status GetHashSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size);
  rocksdb::Status GetSetSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size);
  rocksdb::Status GetListSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size);
  rocksdb::Status GetZsetSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size);
  rocksdb::Status GetBitmapSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size);
  rocksdb::Status GetSortedintSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size);
  rocksdb::Status GetStreamSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size);
  rocksdb::Status GetKeySize(engine::Context &ctx, const Slice &user_key, RedisType type, uint64_t *key_size);

 private:
  rocksdb::SizeApproximationOptions option_;
};

}  // namespace redis
