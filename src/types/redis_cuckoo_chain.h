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

#include <vector>

#include "cuckoo_filter.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

class CuckooPageCache;

// Default values for a newly created cuckoo filter.
const uint32_t kCFDefaultCapacity = 1024;
const uint8_t kCFDefaultBucketSize = 2;
const uint16_t kCFDefaultMaxIterations = 20;
const uint16_t kCFDefaultExpansion = 1;
const uint16_t kCFMaxExpansion = 32768;

struct CuckooFilterInfo {
  // Logical bucket storage across all sub-filters, in bytes.
  uint64_t bucket_bytes = 0;
  // Bucket count of the initial sub-filter. RedisBloom reports the same value as "Number of buckets".
  uint64_t base_buckets = 0;
  uint16_t n_filters = 0;
  // Number of live items (inserts minus deletes).
  uint64_t item_count = 0;
  // Cumulative number of deleted items.
  uint64_t n_items_deleted = 0;
  uint8_t bucket_size = 0;
  uint16_t expansion = 0;
  uint16_t max_iterations = 0;
};

enum class CuckooFilterInsertResult {
  kOk,
  kFull,
  kExists,
};

struct CuckooFilterInsertOptions {
  uint64_t capacity = kCFDefaultCapacity;
  bool auto_create = true;
  bool check_items_exists = false;
};

// Command tags recorded in WriteBatchLogData for cuckoo filter mutations.
constexpr const char *kCFLogDataReserve = "reserve";
constexpr const char *kCFLogDataAdd = "add";
constexpr const char *kCFLogDataInsert = "insert";
constexpr const char *kCFLogDataDel = "del";

class CuckooChain : public Database {
 public:
  CuckooChain(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}

  // Creates a new cuckoo filter with the specified parameters.
  rocksdb::Status Reserve(engine::Context &ctx, const Slice &user_key, uint64_t capacity, uint8_t bucket_size,
                          uint16_t max_iterations, uint16_t expansion, uint32_t page_size);

  // Adds one item to the cuckoo filter.
  // Duplicate items are allowed, so added is true whenever insertion succeeds.
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, const Slice &item, bool *added);

  // Counts how many times an item may be present in the cuckoo filter. Returns 0 when the filter does not exist.
  rocksdb::Status Count(engine::Context &ctx, const Slice &user_key, const Slice &item, uint64_t *count);

  // Returns true if the item might exist, and false if it definitely does not.
  rocksdb::Status Exists(engine::Context &ctx, const Slice &user_key, const Slice &item, bool *exists);

  // Returns whether each item might exist in the cuckoo filter.
  rocksdb::Status MExists(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                          std::vector<bool> *exists);
  // Deletes one matching fingerprint from the cuckoo filter.
  rocksdb::Status Delete(engine::Context &ctx, const Slice &user_key, const Slice &item, bool *deleted);

  // Retrieves information about the cuckoo filter associated with the given key.
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, CuckooFilterInfo *info);

  // Stages all items and commits successful insertions once. Full filters produce per-item failures.
  rocksdb::Status Insert(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                         const CuckooFilterInsertOptions &options, std::vector<CuckooFilterInsertResult> &rets);

 private:
  // Loads metadata for a cuckoo filter key.
  rocksdb::Status getCuckooChainMetadata(engine::Context &ctx, const Slice &ns_key, CuckooChainMetadata *metadata);

  static rocksdb::Status validateMetadata(const CuckooChainMetadata &metadata);

  // Calculates the total number of buckets across all sub-filters.
  static rocksdb::Status calculateTotalBuckets(const CuckooChainMetadata &metadata, uint32_t base_buckets,
                                               uint64_t *total_buckets);

  static rocksdb::Status tryInsertFingerprint(CuckooChainMetadata *metadata, CuckooPageCache &pages, uint64_t hash,
                                              uint8_t fingerprint, bool *inserted, bool error_on_limit = true);
  static rocksdb::Status tryCuckooInsert(const CuckooChainMetadata *metadata, CuckooPageCache &pages, uint64_t hash,
                                         uint8_t fingerprint, bool *inserted);
  static rocksdb::Status tryCuckooKickOut(const CuckooChainMetadata *metadata, CuckooPageCache &pages, uint64_t hash,
                                          uint8_t fingerprint, bool *inserted);
  static rocksdb::Status expandAndInsertCuckooChain(CuckooChainMetadata *metadata, CuckooPageCache &pages,
                                                    uint64_t hash, uint8_t fingerprint, bool *inserted,
                                                    bool error_on_limit);
  rocksdb::Status commitPagesAndMetadata(engine::Context &ctx, const Slice &user_key, const std::string &ns_key,
                                         const CuckooChainMetadata *metadata, CuckooPageCache &pages,
                                         const std::string &command);
  rocksdb::Status MExists(const std::vector<std::string> &items, const CuckooChainMetadata &metadata,
                          CuckooPageCache &pages, std::vector<bool> *exists);
};

}  // namespace redis
