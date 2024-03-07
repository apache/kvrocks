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
class Hyperloglog: public Database {
 public:
  explicit Hyperloglog(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Add(const Slice &user_key, const std::vector<Slice> &elements, int *ret);
  rocksdb::Status Count(const Slice &user_key, int *ret);
  rocksdb::Status Merge(const std::vector<Slice> &user_keys);

 private:
  uint64_t hllCount(const std::vector<uint8_t> &counts);
  void hllMerge(uint8_t *max, const std::vector<uint8_t> &counts);
  rocksdb::Status getRegisters(const Slice &user_key, std::vector<uint8_t> *registers);

  rocksdb::Status GetMetadata(const Slice &ns_key, HyperloglogMetadata *metadata);
  int hllPatLen(unsigned char *ele, size_t elesize, long *regp);
};
}  // namespace Redis