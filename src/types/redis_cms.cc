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
 
 #include "cms.h"
 #include <stdint.h>

 #include "cms.h"
 #include "vendor/murmurhash2.h"

namespace redis {

rocksdb::Status CMS::GetMetadata(Database::GetOptions get_options, const Slice &ns_key,
                                         CountMinSketchMetadata *metadata) {
  return Database::GetMetadata(get_options, {kRedisCountMinSketch}, ns_key, metadata);
}

rocksdb::Status CMS::IncrBy(const Slice &user_key, const std::unordered_map<std::string, uint64_t> &elements) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  
  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};
  rocksdb::Status s = GetMetadata(GetOptions(), ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) { return s; }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  CMSketch cms(metadata.width, metadata.depth, metadata.counter,metadata.array);
  
  if (elements.empty()) {
    return rocksdb::Status::OK();
  }

  for (auto &element : elements) {
    cms.IncrBy(element.first.data(), element.first.size(), element.second);
    metadata.counter += element.second;
  }

  metadata.array = std::move(cms.GetArray());

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CMS::Info(const Slice &user_key, std::vector<uint64_t> *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  
  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};
  rocksdb::Status s = GetMetadata(GetOptions(), ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) { return s; }

  ret->emplace_back(metadata.width);
  ret->emplace_back(metadata.depth);
  ret->emplace_back(metadata.counter);

  return rocksdb::Status::OK();
};

rocksdb::Status CMS::InitByDim(const Slice &user_key, uint32_t width, uint32_t depth) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  
  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};

  rocksdb::Status s = GetMetadata(GetOptions(), ns_key, &metadata);
  if (!s.IsNotFound()) {
    return s;
  }
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  metadata.counter = 0;
  metadata.width = width;
  metadata.depth = depth;
  metadata.array = std::vector<uint32_t>(width*depth, 0);

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
};

rocksdb::Status CMS::InitByProb(const Slice &user_key, double error, double delta) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  
  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};

  rocksdb::Status s = GetMetadata(GetOptions(), ns_key, &metadata);
  if (!s.IsNotFound()) {
    return s;
  }
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  CMSketch cms;
  size_t width = 0;
  size_t depth = 0;
  cms.CMSDimFromProb(error, delta, width, depth);

  metadata.width = width;
  metadata.depth = depth;
  metadata.counter = cms.GetCounter();
  metadata.array = std::move(cms.GetArray());

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
};


rocksdb::Status CMS::Query(const Slice &user_key, const std::vector<std::string> &elements, std::vector<uint32_t> &counters) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  
  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};

  rocksdb::Status s = GetMetadata(GetOptions(), ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) { return s; }
  if (s.IsNotFound()) {
    counters.assign(elements.size(), 0); 
    return rocksdb::Status::NotFound();
  }

  CMSketch cms(metadata.width, metadata.depth, metadata.counter, metadata.array);

  for (auto &element : elements) {
    counters.push_back(cms.Query(element.data(), element.size()));
  }
  return rocksdb::Status::OK();
};

} // namespace redis

