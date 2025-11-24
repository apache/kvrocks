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
#include "topk.h"

namespace redis {

enum class TopKInfoType { kAll, kTopK, kWidth, kDepth, kDecay };

struct TopKInfo {
  uint32_t k;
  uint32_t width;
  uint32_t depth;
  double decay;
};

class TopK : public SubKeyScanner {
 public:
  using Slice = rocksdb::Slice;

  explicit TopK(engine::Storage *storage, const std::string &ns) : SubKeyScanner(storage, ns) {}

  rocksdb::Status Reserve(engine::Context &ctx, const Slice &user_key, uint32_t k, uint32_t width, uint32_t depth,
                          double decay);
  rocksdb::Status Query(engine::Context &ctx, const Slice &user_key, const Slice &items, bool *exists);
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, const Slice &items);
  rocksdb::Status List(engine::Context &ctx, const Slice &user_key, std::vector<std::string> &items);
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, TopKInfo *info);
  rocksdb::Status IncrBy(engine::Context &ctx, const Slice &user_key, const Slice &items, uint32_t incr);

 private:
  rocksdb::Status getTopKMetadata(engine::Context &ctx, const Slice &ns_key, TopKMetadata *metadata);
  rocksdb::Status createTopK(engine::Context &ctx, const Slice &ns_key, uint32_t k, uint32_t width, uint32_t depth,
                             double decay, TopKMetadata *metadata);

  rocksdb::Status getTopKData(engine::Context &ctx, const Slice &ns_key, const TopKMetadata &metadata,
                              BlockSplitTopK *topk);
  rocksdb::Status setTopkData(engine::Context &ctx, const Slice &ns_key, const TopKMetadata &metadata,
                              const BlockSplitTopK &topk, const std::vector<bool> &is_dirty_buckets,
                              const std::vector<bool> &is_dirty_heaps);

  std::string getTKKey(const Slice &ns_key, const TopKMetadata &metadata, uint8_t index);

  std::string getSubKey(const Slice &ns_key, const TopKMetadata &metadata, uint8_t topk_index, uint32_t sub_index, uint8_t index);
};

}  // namespace redis