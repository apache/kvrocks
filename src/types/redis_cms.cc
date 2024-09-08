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

#include "redis_cms.h"

#include <stdint.h>

#include "cms.h"

namespace redis {

rocksdb::Status CMS::GetMetadata(engine::Context &ctx, const Slice &ns_key,
                                 CountMinSketchMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCountMinSketch}, ns_key, metadata);
}

rocksdb::Status CMS::IncrBy(engine::Context &ctx, const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  CMSketch cms(metadata.width, metadata.depth, metadata.counter, metadata.array);

  if (elements.empty()) {
    return rocksdb::Status::OK();
  }

  for (auto &element : elements) {
    cms.IncrBy(element.first.data(), element.second);
    metadata.counter += element.second;
  }

  metadata.array = std::move(cms.GetArray());

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CMS::Info(engine::Context &ctx, const Slice &user_key, std::vector<uint64_t> *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (!s.ok() || s.IsNotFound()) {
    return rocksdb::Status::NotFound();
  }

  ret->emplace_back(metadata.width);
  ret->emplace_back(metadata.depth);
  ret->emplace_back(metadata.counter);

  return rocksdb::Status::OK();
};

rocksdb::Status CMS::InitByDim(engine::Context &ctx, const Slice &user_key, uint32_t width, uint32_t depth) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};

  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (!s.IsNotFound()) {
    return s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  metadata.width = width;
  metadata.depth = depth;
  metadata.counter = 0;
  metadata.array = std::vector<uint32_t>(width * depth, 0);

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
};

rocksdb::Status CMS::InitByProb(engine::Context &ctx, const Slice &user_key, double error, double delta) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};

  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.IsNotFound()) {
    return s;
  }
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  CMSketch cms { 0, 0, 0 };
  CMSketch::CMSketchDimensions dim = cms.CMSDimFromProb(error, delta);

  metadata.width = dim.width;
  metadata.depth = dim.depth;
  metadata.counter = 0;
  metadata.array = std::vector<uint32_t>(dim.width * dim.depth, 0);

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
};

rocksdb::Status CMS::Query(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements,
                           std::vector<uint32_t> &counters) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};

  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (s.IsNotFound()) {
    counters.assign(elements.size(), 0);
    return rocksdb::Status::OK();
  } else if (!s.ok()) {
    return s;
  }

  CMSketch cms(metadata.width, metadata.depth, metadata.counter, metadata.array);

  for (auto &element : elements) {
    counters.push_back(cms.Query(element.data()));
  }
  return rocksdb::Status::OK();
};

}  // namespace redis
