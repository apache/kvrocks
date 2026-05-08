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

#include <memory>
#include <utility>

#include "fmt/ostream.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/backup_engine.h"
#include "storage/storage.h"

namespace util {

struct UniqueIterator : std::unique_ptr<rocksdb::Iterator> {
  using BaseType = std::unique_ptr<rocksdb::Iterator>;

  explicit UniqueIterator(rocksdb::Iterator* iter) : BaseType(iter) {}
  UniqueIterator(engine::Context& ctx, const rocksdb::ReadOptions& options, rocksdb::ColumnFamilyHandle* column_family)
      : BaseType(ctx.storage->NewIterator(ctx, options, column_family)) {}
  UniqueIterator(engine::Context& ctx, const rocksdb::ReadOptions& options, ColumnFamilyID cf)
      : BaseType(ctx.storage->NewIterator(ctx, options, ctx.storage->GetCFHandle(cf))) {}
  UniqueIterator(engine::Context& ctx, const rocksdb::ReadOptions& options)
      : BaseType(ctx.storage->NewIterator(ctx, options)) {}
};

inline StatusOr<std::unique_ptr<rocksdb::DB>> DBOpen(const rocksdb::Options& options, const std::string& dbname) {
  std::unique_ptr<rocksdb::DB> db;
  auto s = rocksdb::DB::Open(options, dbname, &db);
  if (!s.ok()) return {Status::DBOpenErr, s.ToString()};
  return std::move(db);
}

inline StatusOr<std::unique_ptr<rocksdb::DB>> DBOpen(
    const rocksdb::DBOptions& db_options, const std::string& dbname,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  std::unique_ptr<rocksdb::DB> db;
  auto s = rocksdb::DB::Open(db_options, dbname, column_families, handles, &db);
  if (!s.ok()) return {Status::DBOpenErr, s.ToString()};
  return std::move(db);
}

inline StatusOr<std::unique_ptr<rocksdb::DB>> DBOpenForReadOnly(
    const rocksdb::DBOptions& db_options, const std::string& dbname,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  std::unique_ptr<rocksdb::DB> db;
  auto s = rocksdb::DB::OpenForReadOnly(db_options, dbname, column_families, handles, &db);
  if (!s.ok()) return {Status::DBOpenErr, s.ToString()};
  return std::move(db);
}

inline StatusOr<std::unique_ptr<rocksdb::DB>> DBOpenAsSecondaryInstance(
    const rocksdb::DBOptions& db_options, const std::string& dbname, const std::string& secondary_path,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  std::unique_ptr<rocksdb::DB> db;
  auto s = rocksdb::DB::OpenAsSecondary(db_options, dbname, secondary_path, column_families, handles, &db);
  if (!s.ok()) return {Status::DBOpenErr, s.ToString()};
  return std::move(db);
}

inline StatusOr<std::unique_ptr<rocksdb::BackupEngine>> BackupEngineOpen(rocksdb::Env* db_env,
                                                                         const rocksdb::BackupEngineOptions& options) {
  rocksdb::BackupEngine* backup_engine = nullptr;
  auto s = rocksdb::BackupEngine::Open(options, db_env, &backup_engine);
  if (!s.ok()) return {Status::DBBackupErr, s.ToString()};
  return std::unique_ptr<rocksdb::BackupEngine>(backup_engine);
}

}  // namespace util

namespace rocksdb {

inline std::ostream& operator<<(std::ostream& os, const Slice& slice) {
  return os << std::string_view{slice.data(), slice.size()};
}

}  // namespace rocksdb

template <>
struct fmt::formatter<rocksdb::Slice> : fmt::ostream_formatter {};
