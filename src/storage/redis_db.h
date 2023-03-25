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

namespace Redis {
class Database {
 public:
  static constexpr uint64_t RANDOM_KEY_SCAN_LIMIT = 60;

  explicit Database(Engine::Storage *storage, std::string ns = "");
  rocksdb::Status GetMetadata(RedisType type, const Slice &ns_key, Metadata *metadata);
  rocksdb::Status GetRawMetadata(const Slice &ns_key, std::string *bytes);
  rocksdb::Status GetRawMetadataByUserKey(const Slice &user_key, std::string *bytes);
  rocksdb::Status Expire(const Slice &user_key, uint64_t timestamp);
  rocksdb::Status Del(const Slice &user_key);
  rocksdb::Status Exists(const std::vector<Slice> &keys, int *ret);
  rocksdb::Status TTL(const Slice &user_key, int64_t *ttl);
  rocksdb::Status Type(const Slice &user_key, RedisType *type);
  rocksdb::Status Dump(const Slice &user_key, std::vector<std::string> *infos);
  rocksdb::Status FlushDB();
  rocksdb::Status FlushAll();
  void GetKeyNumStats(const std::string &prefix, KeyNumStats *stats);
  void Keys(const std::string &prefix, std::vector<std::string> *keys = nullptr, KeyNumStats *stats = nullptr);
  rocksdb::Status Scan(const std::string &cursor, uint64_t limit, const std::string &prefix,
                       std::vector<std::string> *keys, std::string *end_cursor = nullptr);
  rocksdb::Status RandomKey(const std::string &cursor, std::string *key);
  void AppendNamespacePrefix(const Slice &user_key, std::string *output);
  rocksdb::Status FindKeyRangeWithPrefix(const std::string &prefix, const std::string &prefix_end, std::string *begin,
                                         std::string *end, rocksdb::ColumnFamilyHandle *cf_handle = nullptr);
  rocksdb::Status ClearKeysOfSlot(const rocksdb::Slice &ns, int slot);
  rocksdb::Status GetSlotKeysInfo(int slot, std::map<int, uint64_t> *slotskeys, std::vector<std::string> *keys,
                                  int count);

 protected:
  Engine::Storage *storage_;
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_;
  std::string namespace_;

  friend class LatestSnapShot;
  class LatestSnapShot {
   public:
    explicit LatestSnapShot(Engine::Storage *storage)
        : storage_(storage), snapshot_(storage_->GetDB()->GetSnapshot()) {}
    ~LatestSnapShot() { storage_->GetDB()->ReleaseSnapshot(snapshot_); }
    const rocksdb::Snapshot *GetSnapShot() { return snapshot_; }

    LatestSnapShot(const LatestSnapShot &) = delete;
    LatestSnapShot &operator=(const LatestSnapShot &) = delete;

   private:
    Engine::Storage *storage_ = nullptr;
    const rocksdb::Snapshot *snapshot_ = nullptr;
  };
};

class SubKeyScanner : public Redis::Database {
 public:
  explicit SubKeyScanner(Engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Scan(RedisType type, const Slice &user_key, const std::string &cursor, uint64_t limit,
                       const std::string &subkey_prefix, std::vector<std::string> *keys,
                       std::vector<std::string> *values = nullptr);
};

class WriteBatchLogData {
 public:
  WriteBatchLogData() = default;
  explicit WriteBatchLogData(RedisType type) : type_(type) {}
  explicit WriteBatchLogData(RedisType type, std::vector<std::string> &&args) : type_(type), args_(std::move(args)) {}

  RedisType GetRedisType();
  std::vector<std::string> *GetArguments();
  std::string Encode();
  Status Decode(const rocksdb::Slice &blob);

 private:
  RedisType type_ = kRedisNone;
  std::vector<std::string> args_;
};

}  // namespace Redis
