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

#include "cuckoo_filter.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

// Default values for a newly created cuckoo filter.
const uint32_t kCFDefaultCapacity = 1024;
const uint8_t kCFDefaultBucketSize = 4;  // 4 fingerprints per bucket
const uint16_t kCFDefaultMaxIterations = 500;
const uint16_t kCFDefaultExpansion = 2;
const uint16_t kCFMaxExpansion = 32768;

class CuckooChain : public Database {
 public:
  CuckooChain(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}

  // Creates a new cuckoo filter with the specified parameters.
  rocksdb::Status Reserve(engine::Context &ctx, const Slice &user_key, uint64_t capacity, uint8_t bucket_size,
                          uint16_t max_iterations, uint16_t expansion, uint32_t page_size);

  // Adds one item to the cuckoo filter.
  // Duplicate items are allowed, so added is true whenever insertion succeeds.
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, const Slice &item, bool *added);

 private:
  // Loads metadata for a cuckoo filter key.
  rocksdb::Status getCuckooChainMetadata(engine::Context &ctx, const Slice &ns_key, CuckooChainMetadata *metadata);

  static rocksdb::Status validateMetadata(const CuckooChainMetadata &metadata);
};

}  // namespace redis
