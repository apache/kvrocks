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

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

class HyperLogLog : public Database {
 public:
  explicit HyperLogLog(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, const std::vector<uint64_t> &element_hashes,
                      uint64_t *ret);
  rocksdb::Status Count(engine::Context &ctx, const Slice &user_key, uint64_t *ret);
  /// The count when user_keys.size() is greater than 1.
  rocksdb::Status CountMultiple(engine::Context &ctx, const std::vector<Slice> &user_key, uint64_t *ret);
  rocksdb::Status Merge(engine::Context &ctx, const Slice &dest_user_key, const std::vector<Slice> &source_user_keys);

  static uint64_t HllHash(std::string_view);

 private:
  [[nodiscard]] rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &ns_key, HyperLogLogMetadata *metadata);

  [[nodiscard]] rocksdb::Status mergeUserKeys(engine::Context &ctx, const std::vector<Slice> &user_keys,
                                              std::vector<std::string> *register_segments);
  /// Using multi-get to acquire the register_segments
  ///
  /// If the metadata is not found, register_segments will be initialized with 16 empty slices.
  [[nodiscard]] rocksdb::Status getRegisters(engine::Context &ctx, const Slice &ns_key,
                                             std::vector<rocksdb::PinnableSlice> *register_segments);
  /// Same with getRegisters, but the result is stored in a vector of strings.
  [[nodiscard]] rocksdb::Status getRegisters(engine::Context &ctx, const Slice &ns_key,
                                             std::vector<std::string> *register_segments);
};

}  // namespace redis
