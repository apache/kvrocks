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

#include "rocksdb/status.h"
#include "storage/redis_metadata.h"
#include "types/cuckoo.h"

namespace redis {

rocksdb::Status CFilter::GetMetadata(engine::Context &ctx, const Slice &ns_key, CuckooFilterMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCuckooFilter}, ns_key, metadata);
}

void CFilter::updateMetadata(CuckooFilter &cf, CuckooFilterMetadata *metadata) {
  metadata->capacity = cf.GetCapacity();
  metadata->num_buckets = cf.GetNumBuckets();
  metadata->num_filters = cf.GetNumFilters();
  metadata->num_items = cf.GetNumItems();
  metadata->num_deletes = cf.GetNumDeletes();
  cf.GetFilter(metadata->filters);
}

void InitializeDefaultCuckooFilterMetadata(CuckooFilterMetadata &metadata) {
  metadata.capacity = 1024;
  metadata.bucket_size = 4;
  metadata.max_iterations = 500;
  metadata.expansion = 2;
  metadata.num_buckets = metadata.capacity / metadata.bucket_size;
  metadata.num_filters = 1;
  metadata.num_items = 0;
  metadata.num_deletes = 0;
  metadata.filters.clear();

  SubCF initial_filter;
  initial_filter.bucket_size = metadata.bucket_size;
  initial_filter.num_buckets = metadata.num_buckets;
  initial_filter.data.resize(metadata.num_buckets * metadata.bucket_size, CUCKOO_NULLFP);
  metadata.filters.push_back(initial_filter);
}

void InitializeCuckooFilterMetadata(CuckooFilterMetadata &metadata, uint64_t capacity) {
  metadata.capacity = capacity;
  metadata.bucket_size = 4;
  metadata.max_iterations = 500;
  metadata.expansion = 2;
  metadata.num_buckets = metadata.capacity / metadata.bucket_size;
  metadata.num_filters = 1;
  metadata.num_items = 0;
  metadata.num_deletes = 0;
  metadata.filters.clear();

  SubCF initial_filter;
  initial_filter.bucket_size = metadata.bucket_size;
  initial_filter.num_buckets = metadata.num_buckets;
  initial_filter.data.resize(metadata.num_buckets * metadata.bucket_size, CUCKOO_NULLFP);
  metadata.filters.push_back(initial_filter);
}


rocksdb::Status CFilter::Add(engine::Context &ctx, const Slice &user_key, const std::string &element, int *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    InitializeDefaultCuckooFilterMetadata(metadata);
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter);
  batch->PutLogData(log_data.Encode());

  CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion,
                  metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes,
                  metadata.filters);

  uint64_t hash = cfMHash(element, 0);
  CuckooFilter::CuckooInsertStatus status = cf.Insert(hash);

  if (status == CuckooFilter::NoSpace) {
    *ret = 0;
    return rocksdb::Status::NoSpace();
  } else if (status == CuckooFilter::MemAllocFailed) {
    *ret = 0;
    return rocksdb::Status::Aborted();
  }
  *ret = 1;

  updateMetadata(cf, &metadata);

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CFilter::AddNX(engine::Context &ctx, const Slice &user_key, const std::string &element, int *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    InitializeDefaultCuckooFilterMetadata(metadata);
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter);
  batch->PutLogData(log_data.Encode());

  CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion,
                  metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes,
                  metadata.filters);

  uint64_t hash = cfMHash(element, 0);
  CuckooFilter::CuckooInsertStatus status = cf.InsertUnique(hash);

  if (status == CuckooFilter::Exists) {
    *ret = 0;
    return rocksdb::Status::Aborted();
  } else if (status == CuckooFilter::NoSpace) {
    *ret = -1;
    return rocksdb::Status::NoSpace();
  } else if (status == CuckooFilter::MemAllocFailed) {
    *ret = -1;
    return rocksdb::Status::Aborted();
  }
  *ret = 1;

  updateMetadata(cf, &metadata);

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CFilter::Count(engine::Context &ctx, const Slice &user_key, const std::string &element, uint64_t *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    *ret = 0;
    return rocksdb::Status::OK();
  }

  CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion,
                  metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes,
                  metadata.filters);

  uint64_t hash = cfMHash(element, 0);
  *ret = cf.Count(hash);

  return rocksdb::Status::OK();
}

rocksdb::Status CFilter::Del(engine::Context &ctx, const Slice &user_key, const std::string &element, int *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    *ret = 0;
    return rocksdb::Status::NotFound();
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter);
  batch->PutLogData(log_data.Encode());

  CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion,
                  metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes,
                  metadata.filters);

  uint64_t hash = cfMHash(element, 0);

  bool status = cf.Remove(hash);

  if (!status) {
    *ret = 0;
    return rocksdb::Status::NotFound();
  }

  *ret = 1;

  updateMetadata(cf, &metadata);
  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CFilter::Exists(engine::Context &ctx, const Slice &user_key, const std::string &element, int *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    *ret = 0;
    return rocksdb::Status::OK();
  }

  CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion,
                  metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes,
                  metadata.filters);
  uint64_t hash = cfMHash(element, 0);
  bool exists = cf.Contains(hash);
  *ret = exists ? 1 : 0;

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

  if (s.IsNotFound()) {
    return rocksdb::Status::NotFound();
  }

  CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion,
                  metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes,
                  metadata.filters);

  CuckooFilterInfo info = {metadata.capacity,    metadata.num_buckets, metadata.num_filters, metadata.num_items,
                           metadata.num_deletes, metadata.bucket_size, metadata.expansion,   metadata.max_iterations};
  *ret = info;
  return rocksdb::Status::OK();
}

rocksdb::Status CFilter::Insert(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements,
                                std::vector<int> *ret, uint64_t capacity, bool no_create) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    if (!no_create) {
      InitializeCuckooFilterMetadata(metadata, capacity);
    } else {
      return rocksdb::Status::NotFound();
    }
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter);
  batch->PutLogData(log_data.Encode());

  CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion,
                  metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes,
                  metadata.filters);

  ret->resize(elements.size());

  for (size_t i = 0; i < elements.size(); ++i) {
    uint64_t hash = cfMHash(elements[i], 0);
    CuckooFilter::CuckooInsertStatus status = cf.Insert(hash);

    if (status == CuckooFilter::MemAllocFailed || status == CuckooFilter::NoSpace) {
      (*ret)[i] = -1;
    } else {
      (*ret)[i] = 1;
    }
  }

  updateMetadata(cf, &metadata);

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CFilter::InsertNX(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements,
                                  std::vector<int> *ret, uint64_t capacity, bool no_create) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    if (!no_create) {
      InitializeCuckooFilterMetadata(metadata, capacity);
    } else {
      return rocksdb::Status::NotFound();
    }
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter);
  batch->PutLogData(log_data.Encode());

  CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion,
                  metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes,
                  metadata.filters);

  ret->resize(elements.size());

  for (size_t i = 0; i < elements.size(); ++i) {
    uint64_t hash = cfMHash(elements[i], 0);
    CuckooFilter::CuckooInsertStatus status = cf.InsertUnique(hash);

    if (status == CuckooFilter::Exists) {
      (*ret)[i] = 0;
    } else if (status == CuckooFilter::MemAllocFailed || status == CuckooFilter::NoSpace) {
      (*ret)[i] = -1;
    } else {
      (*ret)[i] = 1;
    }
  }

  updateMetadata(cf, &metadata);

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CFilter::MExists(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements,
                                 std::vector<int> *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    ret->resize(elements.size(), 0);
    return rocksdb::Status::OK();
  }

  CuckooFilter cf(metadata.capacity, metadata.bucket_size, metadata.max_iterations, metadata.expansion,
                  metadata.num_buckets, metadata.num_filters, metadata.num_items, metadata.num_deletes,
                  metadata.filters);

  ret->resize(elements.size());

  for (size_t i = 0; i < elements.size(); ++i) {
    uint64_t hash = cfMHash(elements[i], 0);
    bool exists = cf.Contains(hash);

    (*ret)[i] = exists ? 1 : 0;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status CFilter::Reserve(engine::Context &ctx, const Slice &user_key, uint64_t capacity, uint8_t bucket_size,
                                 uint16_t max_iterations, uint16_t expansion) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (s.ok()) {
    return rocksdb::Status::InvalidArgument("the key already exists");
  }

  if (!s.IsNotFound()) {
    return s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter);
  batch->PutLogData(log_data.Encode());

  uint64_t num_buckets = capacity / bucket_size;
  uint16_t num_filters = 1;

  std::vector<SubCF> initial_filters;
  SubCF initial_filter;
  initial_filter.bucket_size = bucket_size;
  initial_filter.num_buckets = num_buckets;
  initial_filter.data.resize(num_buckets * bucket_size, CUCKOO_NULLFP);
  initial_filters.push_back(initial_filter);

  CuckooFilter cf(capacity, bucket_size, max_iterations, expansion, num_buckets, num_filters, 0, 0, initial_filters);

  // Set metadata for the new filter
  metadata.capacity = capacity;
  metadata.bucket_size = bucket_size;
  metadata.max_iterations = max_iterations;
  metadata.expansion = expansion;
  metadata.num_buckets = num_buckets;
  metadata.num_filters = num_filters;
  metadata.num_items = 0;
  metadata.num_deletes = 0;
  metadata.filters = initial_filters;

  // Encode metadata and write to the batch
  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  // Write batch to storage
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

}  // namespace redis
