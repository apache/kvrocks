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

#include "storage/redis_metadata.h"
#include "storage/storage.h"

struct MigrateItem {
  rocksdb::ColumnFamilyHandle *cf;
  std::string key;
  std::string value;
};

class MigrateIterator {
 public:
  MigrateIterator(engine::Storage *storage, const rocksdb::ReadOptions &read_options);
  bool Valid() const;
  void Seek(const rocksdb::Slice &target);
  void Next();
  const std::vector<MigrateItem> &GetItems() const;
  std::string GetLogData() const;

 private:
  void findMetaData();
  void initSubData();
  void findSubData();

  rocksdb::ColumnFamilyHandle *metadata_cf_;
  rocksdb::ColumnFamilyHandle *subkey_cf_;
  rocksdb::ColumnFamilyHandle *zset_score_cf_;
  rocksdb::ColumnFamilyHandle *stream_cf_;

  std::unique_ptr<rocksdb::Iterator> metadata_iter_;
  std::unique_ptr<rocksdb::Iterator> subdata_iter_;
  std::unique_ptr<rocksdb::Iterator> stream_iter_;

  bool valid_;
  Metadata metadata_;
  std::string metakey_prefix_;
  std::string subkey_prefix_;
  std::string log_data_;
  std::vector<MigrateItem> items_;
};
