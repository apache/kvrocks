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
  explicit CMS(engine::Storage *storage, const std::string &ns): Database(storage, ns) {}

  rocksdb::Status IncrBy(const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements);
  rocksdb::Status Info(const Slice &user_key, std::vector<uint64_t> *ret);
  rocksdb::Status InitByDim(const Slice &user_key, uint32_t width, uint32_t depth);
  rocksdb::Status InitByProb(const Slice &user_key, double error, double delta);
  // rocksdb::Status Merge(const std::vector<Slice> &user_keys, const std::vector<Slice> &source_user_keys);
  rocksdb::Status Query(const Slice &user_key, const std::vector<std::string> &elements, std::vector<uint32_t> &counters);

 private:
  [[nodiscard]] rocksdb::Status GetMetadata(Database::GetOptions get_options, const Slice &ns_key,
                                            CountMinSketchMetadata *metadata);

  // [[nodiscard]] rocksdb::Status mergeUserKeys(Database::GetOptions get_options, const std::vector<Slice> &user_keys,
                                              // std::vector<std::string> *register_segments);
};

}  // namespace redis
