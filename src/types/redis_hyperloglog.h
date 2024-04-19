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

#include "hyperloglog.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

class HyperLogLog : public Database {
 public:
  explicit HyperLogLog(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Add(const Slice &user_key, const std::vector<Slice> &elements, uint64_t *ret);
  rocksdb::Status Count(const Slice &user_key, uint64_t *ret);
  rocksdb::Status Merge(const std::vector<Slice> &user_keys);

 private:
  rocksdb::Status GetMetadata(Database::GetOptions get_options, const Slice &ns_key, HyperloglogMetadata *metadata);
  rocksdb::Status getRegisters(const Slice &user_key, std::vector<uint8_t> *registers);
};

}  // namespace redis
