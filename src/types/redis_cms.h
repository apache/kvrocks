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

#include "cms.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

class CMS : public Database {
 public:
  explicit CMS(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}

  rocksdb::Status IncrBy(engine::Context &ctx, const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements);
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, CMSketch::CMSInfo *ret);
  rocksdb::Status InitByDim(engine::Context &ctx, const Slice &user_key, uint32_t width, uint32_t depth);
  rocksdb::Status InitByProb(engine::Context &ctx, const Slice &user_key, double error, double delta);
  rocksdb::Status Query(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements,
                        std::vector<uint32_t> &counters);

 private:
  [[nodiscard]] rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &ns_key,
                                            CountMinSketchMetadata *metadata);

  // TODO (jonathanc-n)
  [[nodiscard]] rocksdb::Status mergeUserKeys(engine::Context &ctx, const std::vector<Slice> &user_keys,
                                              std::vector<std::string> *register_segments);
};

}  // namespace redis
