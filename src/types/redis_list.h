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

#include <stdint.h>

#include <optional>
#include <string>
#include <vector>

#include "encoding.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

struct PosSpec {
  int64_t rank = 1;
  std::optional<int64_t> count = std::nullopt;
  int64_t max_len = 0;
  explicit PosSpec() = default;
};

namespace redis {
class List : public Database {
 public:
  explicit List(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Size(engine::Context &ctx, const Slice &user_key, uint64_t *size);
  rocksdb::Status Trim(engine::Context &ctx, const Slice &user_key, int start, int stop);
  rocksdb::Status Set(engine::Context &ctx, const Slice &user_key, int index, Slice elem);
  rocksdb::Status Insert(engine::Context &ctx, const Slice &user_key, const Slice &pivot, const Slice &elem,
                         bool before, int *new_size);
  rocksdb::Status Pop(engine::Context &ctx, const Slice &user_key, bool left, std::string *elem);
  rocksdb::Status PopMulti(engine::Context &ctx, const Slice &user_key, bool left, uint32_t count,
                           std::vector<std::string> *elems);
  rocksdb::Status Rem(engine::Context &ctx, const Slice &user_key, int count, const Slice &elem, uint64_t *removed_cnt);
  rocksdb::Status Index(engine::Context &ctx, const Slice &user_key, int index, std::string *elem);
  rocksdb::Status LMove(engine::Context &ctx, const Slice &src, const Slice &dst, bool src_left, bool dst_left,
                        std::string *elem);
  rocksdb::Status Push(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &elems, bool left,
                       uint64_t *new_size);
  rocksdb::Status PushX(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &elems, bool left,
                        uint64_t *new_size);
  rocksdb::Status Range(engine::Context &ctx, const Slice &user_key, int start, int stop,
                        std::vector<std::string> *elems);
  rocksdb::Status Pos(engine::Context &ctx, const Slice &user_key, const Slice &elem, const PosSpec &spec,
                      std::vector<int64_t> *indexes);

 private:
  rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &ns_key, ListMetadata *metadata);
  rocksdb::Status push(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &elems,
                       bool create_if_missing, bool left, uint64_t *new_size);
  rocksdb::Status lmoveOnSingleList(engine::Context &ctx, const Slice &src, bool src_left, bool dst_left,
                                    std::string *elem);
  rocksdb::Status lmoveOnTwoLists(engine::Context &ctx, const Slice &src, const Slice &dst, bool src_left,
                                  bool dst_left, std::string *elem);
};
}  // namespace redis
