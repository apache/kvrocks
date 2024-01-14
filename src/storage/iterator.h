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

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>

#include "storage.h"

namespace engine {

class SubKeyIterator {
 public:
  explicit SubKeyIterator(Storage *storage, rocksdb::ReadOptions read_options, RedisType type, std::string prefix);
  ~SubKeyIterator() = default;
  bool Valid() const;
  void Seek();
  void Next();
  // return the raw key in rocksdb
  Slice Key() const;
  // return the user key without prefix
  Slice UserKey() const;
  rocksdb::ColumnFamilyHandle *ColumnFamilyHandle() const;
  Slice Value() const;
  void Reset();

 private:
  Storage *storage_;
  rocksdb::ReadOptions read_options_;
  RedisType type_;
  std::string prefix_;
  std::unique_ptr<rocksdb::Iterator> iter_;
  rocksdb::ColumnFamilyHandle *cf_handle_ = nullptr;
};

class DBIterator {
 public:
  explicit DBIterator(Storage *storage, rocksdb::ReadOptions read_options, int slot = -1);
  ~DBIterator() = default;

  bool Valid() const;
  void Seek(const std::string &target = "");
  void Next();
  // return the raw key in rocksdb
  Slice Key() const;
  // return the namespace and user key without prefix
  std::tuple<Slice, Slice> UserKey() const;
  Slice Value() const;
  RedisType Type() const;
  void Reset();
  std::unique_ptr<SubKeyIterator> GetSubKeyIterator() const;

 private:
  void nextUntilValid();

  Storage *storage_;
  rocksdb::ReadOptions read_options_;
  int slot_ = -1;
  Metadata metadata_ = Metadata(kRedisNone, false);

  rocksdb::ColumnFamilyHandle *metadata_cf_handle_ = nullptr;
  std::unique_ptr<rocksdb::Iterator> metadata_iter_;
  std::unique_ptr<SubKeyIterator> subkey_iter_;
};

}  // namespace engine
