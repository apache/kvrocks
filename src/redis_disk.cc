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

#include <vector>
#include <memory>
#include <utility>
#include <algorithm>
#include <string>

#include "db_util.h"
#include "redis_metadata.h"
#include "redis_sortedint.h"
#include "rocksdb/status.h"
#include "redis_zset.h"
#include "redis_bitmap.h"
#include "redis_disk.h"
#include "status.h"

namespace Redis {
rocksdb::Status Disk::GetApproximateSizes(const Metadata &metadata, const Slice &ns_key,
                                          rocksdb::ColumnFamilyHandle *column_family,
                                          uint64_t *key_size, Slice subkeyleft ,
                                          Slice subkeyright) {
    std::string prefix_key, next_version_prefix_key;
    InternalKey(ns_key, subkeyleft, metadata.version,
                storage_->IsSlotIdEncoded()).Encode(&prefix_key);
    InternalKey(ns_key, subkeyright, metadata.version + 1,
                storage_->IsSlotIdEncoded()).Encode(&next_version_prefix_key);
    auto key_range = rocksdb::Range(prefix_key, next_version_prefix_key);
    uint64_t tmp_size = 0;
    rocksdb::Status s = db_->GetApproximateSizes(this->option, column_family,
                                                 &key_range, 1, &tmp_size);
    if(!s.ok())return s;
    *key_size += tmp_size;
    return rocksdb::Status::OK();
}
rocksdb::Status Disk::GetStringSize(const Slice &user_key, uint64_t *key_size) {
    std::string ns_key;
    AppendNamespacePrefix(user_key, &ns_key);
    auto key_range = rocksdb::Range(Slice(ns_key), Slice(ns_key + static_cast<char>(0)));
    return db_->GetApproximateSizes(this->option, metadata_cf_handle_, &key_range, 1, key_size);
}

rocksdb::Status Disk::GetHashSize(const Slice &user_key, uint64_t *key_size) {
    *key_size = 0;
    std::string ns_key;
    AppendNamespacePrefix(user_key, &ns_key);
    HashMetadata metadata(false);
    rocksdb::Status s = Database::GetMetadata(kRedisHash, ns_key, &metadata);
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
    return this->GetApproximateSizes(metadata, ns_key,
                                     storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName),
                                     key_size);
}


rocksdb::Status Disk::GetSetSize(const Slice &user_key, uint64_t *key_size) {
    *key_size = 0;
    std::string ns_key;
    AppendNamespacePrefix(user_key, &ns_key);
    HashMetadata metadata(false);
    rocksdb::Status s = Database::GetMetadata(kRedisSet, ns_key, &metadata);
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
    return this->GetApproximateSizes(metadata, ns_key,
                                     storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName),
                                     key_size);
}

rocksdb::Status Disk::GetListSize(const Slice &user_key, uint64_t *key_size) {
    *key_size = 0;
    std::string ns_key;
    AppendNamespacePrefix(user_key, &ns_key);
    ListMetadata metadata(false);
    rocksdb::Status s = Database::GetMetadata(kRedisList, ns_key, &metadata);
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
    std::string buf;
    PutFixed64(&buf, metadata.head);
    return this->GetApproximateSizes(metadata, ns_key,
                                     storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName),
                                     key_size, buf);
}

rocksdb::Status Disk::GetZsetSize(const Slice &user_key, uint64_t *key_size) {
    *key_size = 0;
    std::string ns_key;
    AppendNamespacePrefix(user_key, &ns_key);
    ZSetMetadata metadata(false);
    rocksdb::Status s = Database::GetMetadata(kRedisZSet, ns_key, &metadata);
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
    std::string score_bytes;
    PutDouble(&score_bytes, kMinScore);
    s = this->GetApproximateSizes(metadata, ns_key,
                                  storage_->GetCFHandle(Engine::kZSetScoreColumnFamilyName),
                                  key_size, score_bytes, score_bytes);
    if (!s.ok()) return s; 
    return s = this->GetApproximateSizes(metadata, ns_key,
                                         storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName),
                                         key_size);
}

rocksdb::Status Disk::GetBitmapSize(const Slice &user_key, uint64_t *key_size) {
    *key_size = 0;
    std::string ns_key;
    AppendNamespacePrefix(user_key, &ns_key);
    BitmapMetadata metadata(false);
    rocksdb::Status s = Database::GetMetadata(kRedisBitmap, ns_key, &metadata);
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
    std::string score_bytes;
    PutDouble(&score_bytes, kMinScore);
    return s = this->GetApproximateSizes(metadata, ns_key,
                                         storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName),
                                         key_size, std::to_string(0), std::to_string(0));
}

rocksdb::Status Disk::GetSortedintSize(const Slice &user_key, uint64_t *key_size) {
    *key_size = 0;
    std::string ns_key;
    AppendNamespacePrefix(user_key, &ns_key);
    SortedintMetadata metadata(false);
    rocksdb::Status s = Database::GetMetadata(kRedisSortedint, ns_key, &metadata);
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
    std::string prefix_key, next_version_prefix_key, start_buf;
    PutFixed64(&start_buf, 0);
    return s = this->GetApproximateSizes(metadata, ns_key,
                                         storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName),
                                         key_size, start_buf, start_buf); 
}

}  // namespace Redis
