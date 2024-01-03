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

#include "fmt/ostream.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/backup_engine.h"
#include "storage/storage.h"

namespace util {

struct UniqueIterator : std::unique_ptr<rocksdb::Iterator> {
  using BaseType = std::unique_ptr<rocksdb::Iterator>;

  explicit UniqueIterator(rocksdb::Iterator* iter) : BaseType(iter) {}
  UniqueIterator(engine::Storage* storage, const rocksdb::ReadOptions& options,
                 rocksdb::ColumnFamilyHandle* column_family)
      : BaseType(storage->NewIterator(options, column_family)) {}
  UniqueIterator(engine::Storage* storage, const rocksdb::ReadOptions& options)
      : BaseType(storage->NewIterator(options)) {}
};

namespace details {

template <typename T, auto* F, Status::Code C = Status::NotOK, typename... Args>
StatusOr<std::unique_ptr<T>> WrapOutPtrToUnique(Args&&... args) {
  T* ptr = nullptr;
  auto s = (*F)(std::forward<Args>(args)..., &ptr);

  if (!s.ok()) {
    return {C, s.ToString()};
  }

  return ptr;
}

[[nodiscard]] inline rocksdb::Status DBOpenForReadOnly(
    const rocksdb::DBOptions& db_options, const std::string& dbname,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles, rocksdb::DB** dbptr) {
  return rocksdb::DB::OpenForReadOnly(db_options, dbname, column_families, handles, dbptr);
}

[[nodiscard]] inline rocksdb::Status DBOpenForSecondaryInstance(
    const rocksdb::DBOptions& db_options, const std::string& dbname, const std::string& secondary_path,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles, rocksdb::DB** dbptr) {
  return rocksdb::DB::OpenAsSecondary(db_options, dbname, secondary_path, column_families, handles, dbptr);
}

}  // namespace details

inline StatusOr<std::unique_ptr<rocksdb::DB>> DBOpen(const rocksdb::Options& options, const std::string& dbname) {
  return details::WrapOutPtrToUnique<
      rocksdb::DB,
      static_cast<rocksdb::Status (*)(const rocksdb::Options&, const std::string&, rocksdb::DB**)>(rocksdb::DB::Open),
      Status::DBOpenErr>(options, dbname);
}

inline StatusOr<std::unique_ptr<rocksdb::DB>> DBOpen(
    const rocksdb::DBOptions& db_options, const std::string& dbname,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  return details::WrapOutPtrToUnique<
      rocksdb::DB,
      static_cast<rocksdb::Status (*)(const rocksdb::DBOptions&, const std::string&,
                                      const std::vector<rocksdb::ColumnFamilyDescriptor>&,
                                      std::vector<rocksdb::ColumnFamilyHandle*>*, rocksdb::DB**)>(rocksdb::DB::Open),
      Status::DBOpenErr>(db_options, dbname, column_families, handles);
}

inline StatusOr<std::unique_ptr<rocksdb::DB>> DBOpenForReadOnly(
    const rocksdb::DBOptions& db_options, const std::string& dbname,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  return details::WrapOutPtrToUnique<
      rocksdb::DB,
      static_cast<rocksdb::Status (*)(
          const rocksdb::DBOptions&, const std::string&, const std::vector<rocksdb::ColumnFamilyDescriptor>&,
          std::vector<rocksdb::ColumnFamilyHandle*>*, rocksdb::DB**)>(details::DBOpenForReadOnly),
      Status::DBOpenErr>(db_options, dbname, column_families, handles);
}

inline StatusOr<std::unique_ptr<rocksdb::DB>> DBOpenAsSecondaryInstance(
    const rocksdb::DBOptions& db_options, const std::string& dbname, const std::string& secondary_path,
    const std::vector<rocksdb::ColumnFamilyDescriptor>& column_families,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  return details::WrapOutPtrToUnique<
      rocksdb::DB,
      static_cast<rocksdb::Status (*)(const rocksdb::DBOptions&, const std::string&, const std::string&,
                                      const std::vector<rocksdb::ColumnFamilyDescriptor>&,
                                      std::vector<rocksdb::ColumnFamilyHandle*>*, rocksdb::DB**)>(
          details::DBOpenForSecondaryInstance),
      Status::DBOpenErr>(db_options, dbname, secondary_path, column_families, handles);
}

inline StatusOr<std::unique_ptr<rocksdb::BackupEngine>> BackupEngineOpen(rocksdb::Env* db_env,
                                                                         const rocksdb::BackupEngineOptions& options) {
  return details::WrapOutPtrToUnique<
      rocksdb::BackupEngine,
      static_cast<rocksdb::IOStatus (*)(rocksdb::Env*, const rocksdb::BackupEngineOptions&, rocksdb::BackupEngine**)>(
          rocksdb::BackupEngine::Open),
      Status::DBBackupErr>(db_env, options);
}

}  // namespace util

namespace rocksdb {

inline std::ostream& operator<<(std::ostream& os, const Slice& slice) {
  return os << std::string_view{slice.data(), slice.size()};
}

}  // namespace rocksdb

template <>
struct fmt::formatter<rocksdb::Slice> : fmt::ostream_formatter {};
