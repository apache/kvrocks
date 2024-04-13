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

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "redis_metadata.h"
#include "storage.h"

namespace redis {

/// Database is a wrapper of underlying storage engine, it provides
/// some common operations for redis commands.
class Database {
 public:
  static constexpr uint64_t RANDOM_KEY_SCAN_LIMIT = 60;

  struct GetOptions {
    // If snapshot is not nullptr, read from the specified snapshot,
    // otherwise read from the "latest" snapshot.
    const rocksdb::Snapshot *snapshot = nullptr;

    GetOptions() = default;
    explicit GetOptions(const rocksdb::Snapshot *ss) : snapshot(ss) {}
  };

  explicit Database(engine::Storage *storage, std::string ns = "");
  /// Parsing metadata with type of `types` from bytes, the metadata is a base class of all metadata.
  /// When parsing, the bytes will be consumed.
  [[nodiscard]] rocksdb::Status ParseMetadata(RedisTypes types, Slice *bytes, Metadata *metadata);
  /// GetMetadata is a helper function to get metadata from the database. It will read the "raw metadata"
  /// from underlying storage, and then parse the raw metadata to the specified metadata type.
  ///
  /// \param options The read options, including whether uses a snapshot during reading the metadata.
  /// \param types The candidate types of the metadata.
  /// \param ns_key The key with namespace of the metadata.
  /// \param metadata The output metadata.
  [[nodiscard]] rocksdb::Status GetMetadata(GetOptions options, RedisTypes types, const Slice &ns_key,
                                            Metadata *metadata);
  /// GetMetadata is a helper function to get metadata from the database. It will read the "raw metadata"
  /// from underlying storage, and then parse the raw metadata to the specified metadata type.
  ///
  /// Compared with the above function, this function will also return the rest of the bytes
  /// after parsing the metadata.
  ///
  /// \param options The read options, including whether uses a snapshot during reading the metadata.
  /// \param types The candidate types of the metadata.
  /// \param ns_key The key with namespace of the metadata.
  /// \param raw_value Holding the raw metadata.
  /// \param metadata The output metadata.
  /// \param rest The rest of the bytes after parsing the metadata.
  [[nodiscard]] rocksdb::Status GetMetadata(GetOptions options, RedisTypes types, const Slice &ns_key,
                                            std::string *raw_value, Metadata *metadata, Slice *rest);
  /// GetRawMetadata is a helper function to get the "raw metadata" from the database without parsing
  /// it to the specified metadata type.
  ///
  /// \param options The read options, including whether uses a snapshot during reading the metadata.
  /// \param ns_key The key with namespace of the metadata.
  /// \param bytes The output raw metadata.
  [[nodiscard]] rocksdb::Status GetRawMetadata(GetOptions options, const Slice &ns_key, std::string *bytes);
  [[nodiscard]] rocksdb::Status Expire(const Slice &user_key, uint64_t timestamp);
  [[nodiscard]] rocksdb::Status Del(const Slice &user_key);
  [[nodiscard]] rocksdb::Status MDel(const std::vector<Slice> &keys, uint64_t *deleted_cnt);
  [[nodiscard]] rocksdb::Status Exists(const std::vector<Slice> &keys, int *ret);
  [[nodiscard]] rocksdb::Status TTL(const Slice &user_key, int64_t *ttl);
  [[nodiscard]] rocksdb::Status GetExpireTime(const Slice &user_key, uint64_t *timestamp);
  [[nodiscard]] rocksdb::Status Type(const Slice &key, RedisType *type);
  [[nodiscard]] rocksdb::Status Dump(const Slice &user_key, std::vector<std::string> *infos);
  [[nodiscard]] rocksdb::Status FlushDB();
  [[nodiscard]] rocksdb::Status FlushAll();
  [[nodiscard]] rocksdb::Status GetKeyNumStats(const std::string &prefix, KeyNumStats *stats);
  [[nodiscard]] rocksdb::Status Keys(const std::string &prefix, std::vector<std::string> *keys = nullptr,
                                     KeyNumStats *stats = nullptr);
  [[nodiscard]] rocksdb::Status Scan(const std::string &cursor, uint64_t limit, const std::string &prefix,
                                     std::vector<std::string> *keys, std::string *end_cursor = nullptr);
  [[nodiscard]] rocksdb::Status RandomKey(const std::string &cursor, std::string *key);
  std::string AppendNamespacePrefix(const Slice &user_key);
  [[nodiscard]] rocksdb::Status FindKeyRangeWithPrefix(const std::string &prefix, const std::string &prefix_end,
                                                       std::string *begin, std::string *end,
                                                       rocksdb::ColumnFamilyHandle *cf_handle = nullptr);
  [[nodiscard]] rocksdb::Status ClearKeysOfSlot(const rocksdb::Slice &ns, int slot);
  [[nodiscard]] rocksdb::Status KeyExist(const std::string &key);

  // Copy <key,value> to <new_key,value> (already an internal key)
  enum class CopyResult { KEY_NOT_EXIST, KEY_ALREADY_EXIST, DONE };
  [[nodiscard]] rocksdb::Status Copy(const std::string &key, const std::string &new_key, bool nx, bool delete_old,
                                     CopyResult *res);

 protected:
  engine::Storage *storage_;
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_;
  std::string namespace_;

  friend class LatestSnapShot;

 private:
  // Already internal keys
  [[nodiscard]] rocksdb::Status existsInternal(const std::vector<std::string> &keys, int *ret);
  [[nodiscard]] rocksdb::Status typeInternal(const Slice &key, RedisType *type);
};
class LatestSnapShot {
 public:
  explicit LatestSnapShot(engine::Storage *storage) : storage_(storage), snapshot_(storage_->GetDB()->GetSnapshot()) {}
  ~LatestSnapShot() { storage_->GetDB()->ReleaseSnapshot(snapshot_); }
  const rocksdb::Snapshot *GetSnapShot() const { return snapshot_; }

  LatestSnapShot(const LatestSnapShot &) = delete;
  LatestSnapShot &operator=(const LatestSnapShot &) = delete;

 private:
  engine::Storage *storage_ = nullptr;
  const rocksdb::Snapshot *snapshot_ = nullptr;
};

class SubKeyScanner : public redis::Database {
 public:
  explicit SubKeyScanner(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Scan(RedisType type, const Slice &user_key, const std::string &cursor, uint64_t limit,
                       const std::string &subkey_prefix, std::vector<std::string> *keys,
                       std::vector<std::string> *values = nullptr);
};

class WriteBatchLogData {
 public:
  WriteBatchLogData() = default;
  explicit WriteBatchLogData(RedisType type) : type_(type) {}
  explicit WriteBatchLogData(RedisType type, std::vector<std::string> &&args) : type_(type), args_(std::move(args)) {}

  RedisType GetRedisType() const;
  std::vector<std::string> *GetArguments();
  std::string Encode() const;
  Status Decode(const rocksdb::Slice &blob);

 private:
  RedisType type_ = kRedisNone;
  std::vector<std::string> args_;
};

}  // namespace redis
