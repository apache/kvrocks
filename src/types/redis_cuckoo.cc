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

#include "redis_cuckoo.h"

#include "storage/redis_metadata.h"
#include "types/cuckoo.h"

namespace redis {

rocksdb::Status CFilter::Add(engine::Context &ctx, const Slice &user_key, uint64_t element) {
    std::string ns_key = AppendNamespacePrefix(user_key);

    LockGuard guard(storage_->GetLockManager(), ns_key);
    CuckooFilterMetadata metadata{};
    rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
    if (!s.ok() && !s.IsNotFound()) {
        return s;
    }

    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisCuckooFilter);
    batch->PutLogData(log_data.Encode());

    CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion, metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes, metadata.filters);

    CuckooFilter::CuckooInsertStatus status = cf.Insert(element);
    if (status == CuckooFilter::NoSpace) {
        return rocksdb::Status::NoSpace();
    } else if (status == CuckooFilter::MemAllocFailed) {
        return rocksdb::Status::Aborted();
    }

    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);

    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CFilter::AddNX(engine::Context &ctx, const Slice &user_key, uint64_t element) {
    std::string ns_key = AppendNamespacePrefix(user_key);

    LockGuard guard(storage_->GetLockManager(), ns_key);
    CuckooFilterMetadata metadata{};
    rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
    if (!s.ok() && !s.IsNotFound()) {
        return s;
    }

    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisCuckooFilter);
    batch->PutLogData(log_data.Encode());

    CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion, metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes, metadata.filters);

    CuckooFilter::CuckooInsertStatus status = cf.InsertUnique(element);

    if (status == CuckooFilter::Exists) {
        return rocksdb::Status::Aborted();
    } else if (status == CuckooFilter::NoSpace) {
        return rocksdb::Status::NoSpace();
    } else if (status == CuckooFilter::MemAllocFailed) {
        return rocksdb::Status::Aborted();
    }

    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);

    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CFilter::Count(engine::Context &ctx, const Slice &user_key, uint64_t element, uint64_t *ret) {
    std::string ns_key = AppendNamespacePrefix(user_key);

    LockGuard guard(storage_->GetLockManager(), ns_key);
    CuckooFilterMetadata metadata{};
    rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
    if (!s.ok() && !s.IsNotFound()) {
        return s;
    }

    CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion, metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes, metadata.filters);

    *ret = cf.Count(element);

    return rocksdb::Status::OK();
}

rocksdb::Status CFilter::Del(engine::Context &ctx, const Slice &user_key, uint64_t element) {
    std::string ns_key = AppendNamespacePrefix(user_key);

    LockGuard guard(storage_->GetLockManager(), ns_key);
    CuckooFilterMetadata metadata{};
    rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
    if (!s.ok() && !s.IsNotFound()) {
        return s;
    }

    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisCuckooFilter);
    batch->PutLogData(log_data.Encode());

    CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion, metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes, metadata.filters);

    if (!cf.Remove(element)) {
      return rocksdb::Status::NotFound();
    }

    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);

    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CFilter::Exists(engine::Context &ctx, const Slice &user_key, uint64_t element, bool *ret) {
    std::string ns_key = AppendNamespacePrefix(user_key);

    LockGuard guard(storage_->GetLockManager(), ns_key);
    CuckooFilterMetadata metadata{};
    rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
    if (!s.ok() && !s.IsNotFound()) {
        return s;
    }

    CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion, metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes, metadata.filters);

    *ret = cf.Contains(element);

    return rocksdb::Status::OK();
}

rocksdb::Status CFilter::Info(engine::Context &ctx, const Slice &user_key, CuckooFilterInfo *ret) {
    std::string ns_key = AppendNamespacePrefix(user_key);

    LockGuard guard(storage_->GetLockManager(), ns_key);
    CuckooFilterMetadata metadata{};
    rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
    if (!s.ok() && !s.IsNotFound()) {
        return s;
    }

    CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion, metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes, metadata.filters);

    CuckooFilterInfo info = {
        metadata.capacity,
        metadata.num_buckets,
        metadata.num_filters,
        metadata.num_items,
        metadata.num_deletes,
        metadata.bucket_size,
        metadata.expansion,
        metadata.max_iterations
    };
    *ret = info;
    return rocksdb::Status::OK();
}




}