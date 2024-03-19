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
#include <vector>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

class Set : public SubKeyScanner {
 public:
  explicit Set(engine::Storage *storage, const std::string &ns) : SubKeyScanner(storage, ns) {}

  rocksdb::Status Card(const Slice &user_key, uint64_t *size);
  rocksdb::Status IsMember(const Slice &user_key, const Slice &member, bool *flag);
  rocksdb::Status MIsMember(const Slice &user_key, const std::vector<Slice> &members, std::vector<int> *exists);
  rocksdb::Status Add(const Slice &user_key, const std::vector<Slice> &members, uint64_t *added_cnt);
  rocksdb::Status Remove(const Slice &user_key, const std::vector<Slice> &members, uint64_t *removed_cnt);
  rocksdb::Status Members(const Slice &user_key, std::vector<std::string> *members);
  rocksdb::Status Move(const Slice &src, const Slice &dst, const Slice &member, bool *flag);
  rocksdb::Status Take(const Slice &user_key, std::vector<std::string> *members, int count, bool pop);
  rocksdb::Status Diff(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Union(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Inter(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status InterCard(const std::vector<Slice> &keys, uint64_t limit, uint64_t *cardinality);
  rocksdb::Status Overwrite(Slice user_key, const std::vector<std::string> &members);
  rocksdb::Status DiffStore(const Slice &dst, const std::vector<Slice> &keys, uint64_t *saved_cnt);
  rocksdb::Status UnionStore(const Slice &dst, const std::vector<Slice> &keys, uint64_t *save_cnt);
  rocksdb::Status InterStore(const Slice &dst, const std::vector<Slice> &keys, uint64_t *saved_cnt);
  rocksdb::Status Scan(const Slice &user_key, const std::string &cursor, uint64_t limit,
                       const std::string &member_prefix, std::vector<std::string> *members);

 private:
  rocksdb::Status GetMetadata(Database::GetOptions options, const Slice &ns_key, SetMetadata *metadata);
};

}  // namespace redis
