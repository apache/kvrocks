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
#include "rocksdb/status.h"

namespace redis {

rocksdb::Status CMS::GetMetadata(engine::Context &ctx, const Slice &ns_key, CountMinSketchMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCountMinSketch}, ns_key, metadata);
}

rocksdb::Status CMS::IncrBy(engine::Context &ctx, const Slice &user_key,
                            const std::unordered_map<std::string, uint64_t> &elements) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (s.IsNotFound()) {
    return rocksdb::Status::NotFound();
  }
  if (!s.ok()) {
    return s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  CMSketch cms(metadata.width, metadata.depth, metadata.counter, metadata.array);

  for (auto &element : elements) {
    if (element.second > 0 && metadata.counter > std::numeric_limits<uint64_t>::max() - element.second) {
      return rocksdb::Status::InvalidArgument("Overflow error: IncrBy would result in counter overflow");
    }
    cms.IncrBy(element.first, element.second);
    metadata.counter += element.second;
  }

  metadata.array = std::move(cms.GetArray());

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CMS::Info(engine::Context &ctx, const Slice &user_key, CMSketch::CMSInfo *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (!s.ok()) {
    return s;
  }

  ret->width = metadata.width;
  ret->depth = metadata.depth;
  ret->count = metadata.counter;

  return rocksdb::Status::OK();
};

rocksdb::Status CMS::InitByDim(engine::Context &ctx, const Slice &user_key, uint32_t width, uint32_t depth) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};

  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (s.ok()) {
    return rocksdb::Status::InvalidArgument("Key already exists.");
  }

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
  if (error <= 0 || error >= 1) {
    return rocksdb::Status::InvalidArgument("Error must be between 0 and 1 (exclusive).");
  }
  if (delta <= 0 || delta >= 1) {
    return rocksdb::Status::InvalidArgument("Delta must be between 0 and 1 (exclusive).");
  }
  CMSketch::CMSketchDimensions dim = CMSketch::CMSDimFromProb(error, delta);
  auto s = InitByDim(ctx, user_key, dim.width, dim.depth);
  if (!s.ok()) {
    return s;
  }

  size_t memory_used = dim.width * dim.depth * sizeof(uint32_t);
  const size_t max_memory = 50 * 1024 * 1024;

  if (memory_used == 0) {
    return rocksdb::Status::InvalidArgument("Memory usage must be greater than 0.");
  }
  if (memory_used > max_memory) {
    return rocksdb::Status::InvalidArgument("Memory usage exceeds 50MB.");
  }
  return rocksdb::Status::OK();
};

rocksdb::Status CMS::MergeUserKeys(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &src_keys,
                                   const std::vector<uint32_t> &src_weights) {
  size_t num_sources = src_keys.size();
  if (num_sources == 0) {
    return rocksdb::Status::InvalidArgument("No source keys provided for merge.");
  }
  if (src_weights.size() != num_sources) {
    return rocksdb::Status::InvalidArgument("Number of weights must match number of source keys.");
  }

  std::string dest_ns_key = AppendNamespacePrefix(user_key);
  LockGuard guard(storage_->GetLockManager(), dest_ns_key);

  CountMinSketchMetadata dest_metadata{};
  rocksdb::Status dest_status = GetMetadata(ctx, dest_ns_key, &dest_metadata);
  if (!dest_status.ok()) {
    if (dest_status.IsNotFound()) {
      return rocksdb::Status::InvalidArgument("Destination CMS does not exist.");
    }
    return dest_status;
  }

  CMSketch dest_cms(dest_metadata.width, dest_metadata.depth, dest_metadata.counter, dest_metadata.array);

  std::vector<CMSketch> src_cms_objects;
  src_cms_objects.reserve(num_sources);
  std::vector<const CMSketch *> src_cms_pointers;
  src_cms_pointers.reserve(num_sources);
  std::vector<int64_t> weights_long;
  weights_long.reserve(num_sources);

  for (size_t i = 0; i < num_sources; ++i) {
    std::string src_ns_key = AppendNamespacePrefix(src_keys[i]);
    LockGuard guard(storage_->GetLockManager(), src_ns_key);

    CountMinSketchMetadata src_metadata{};
    rocksdb::Status src_status = GetMetadata(ctx, src_ns_key, &src_metadata);
    if (!src_status.ok()) {
      if (src_status.IsNotFound()) {
        return rocksdb::Status::InvalidArgument("Source CMS key not found.");
      }
      return src_status;
    }

    if (src_metadata.width != dest_metadata.width || src_metadata.depth != dest_metadata.depth) {
      return rocksdb::Status::InvalidArgument("Source CMS dimensions do not match destination CMS.");
    }

    CMSketch src_cms(src_metadata.width, src_metadata.depth, src_metadata.counter, src_metadata.array);
    src_cms_objects.emplace_back(std::move(src_cms));
    src_cms_pointers.push_back(&src_cms_objects.back());

    weights_long.push_back(static_cast<long long>(src_weights[i]));
  }

  CMSketch::MergeParams merge_params;
  merge_params.dest = &dest_cms;
  merge_params.num_keys = num_sources;
  merge_params.cms_array = src_cms_pointers;
  merge_params.weights = weights_long;

  auto merge_result = CMSketch::Merge(merge_params);
  if (!merge_result.IsOK()) {
    return rocksdb::Status::InvalidArgument("Merge operation failed due to overflow or invalid dimensions.");
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  dest_metadata.counter = dest_cms.GetCounter();
  dest_metadata.array = dest_cms.GetArray();

  std::string encoded_metadata;
  dest_metadata.Encode(&encoded_metadata);
  batch->Put(metadata_cf_handle_, dest_ns_key, encoded_metadata);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CMS::Query(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements,
                           std::vector<uint32_t> &counters) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  counters.resize(elements.size(), 0);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};

  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (s.IsNotFound()) {
    return rocksdb::Status::NotFound();
  } else if (!s.ok()) {
    return s;
  }

  CMSketch cms(metadata.width, metadata.depth, metadata.counter, metadata.array);

  for (size_t i = 0; i < elements.size(); ++i) {
    counters[i] = cms.Query(elements[i]);
  }

  return rocksdb::Status::OK();
};

}  // namespace redis
