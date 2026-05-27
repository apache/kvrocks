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

#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

#include <cstdint>

#include "cuckoo_filter_page.h"

namespace redis {

class CuckooSubFilter {
 public:
  CuckooSubFilter(engine::Storage *storage, engine::Context &ctx, const Slice &ns_key, bool slot_id_encoded,
                  uint64_t version, uint8_t bucket_size, uint32_t page_size, uint16_t filter_index,
                  uint32_t num_buckets);

  uint16_t Index() const { return filter_index_; }
  uint32_t NumBuckets() const { return num_buckets_; }

  rocksdb::Status TryInsert(uint64_t hash, uint8_t fingerprint, bool *inserted);
  // Performs speculative kick-out mutations in the page cache. On success, dirty pages remain staged for
  // WriteToBatch(); on inserted=false or non-OK status, cached pages are discarded before returning.
  rocksdb::Status TryKickOutInsert(uint64_t hash, uint8_t fingerprint, uint16_t max_iterations, bool *inserted);
  rocksdb::Status WriteToBatch(rocksdb::WriteBatchBase *batch);

 private:
  uint32_t getPrimaryBucketIndex(uint64_t hash) const;
  uint32_t getSecondaryBucketIndex(uint64_t hash, uint8_t fingerprint) const;

  uint8_t bucket_size_ = 0;
  uint16_t filter_index_ = 0;
  uint32_t num_buckets_ = 0;
  CuckooPageCache pages_;
};

}  // namespace redis
