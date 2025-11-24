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

#include "redis_topk.h"

#include "commands/ttl_util.h"
#include "topk.h"

namespace redis {

rocksdb::Status TopK::Reserve(engine::Context &ctx, const Slice &user_key, uint32_t k, uint32_t width, uint32_t depth,
                              double decay) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TopKMetadata topk_metadata;
  rocksdb::Status s = getTopKMetadata(ctx, ns_key, &topk_metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (!s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument("TopK already exists");
  }

  return createTopK(ctx, ns_key, k, width, depth, decay, &topk_metadata);
}

rocksdb::Status TopK::Add(engine::Context &ctx, const Slice &user_key, const Slice &items) {
  return IncrBy(ctx, user_key, items, 1);
}

rocksdb::Status TopK::IncrBy(engine::Context &ctx, const Slice &user_key, const Slice &items, uint32_t incr) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TopKMetadata topk_metadata;
  rocksdb::Status s = getTopKMetadata(ctx, ns_key, &topk_metadata);
  if (!s.ok()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTopK, {"IncrBy"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  BlockSplitTopK topk(topk_metadata.top_k, topk_metadata.width, topk_metadata.depth, topk_metadata.decay);
  s = getTopKData(ctx, ns_key, topk_metadata, &topk);
  if (!s.ok()) return s;

  std::vector<bool> is_dirty_buckets(topk_metadata.width * topk_metadata.depth, false);
  std::vector<bool> is_dirty_heaps(topk_metadata.top_k, false);
  topk.Add(items.data_, incr);

  s = setTopkData(ctx, ns_key, topk_metadata, topk);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TopK::Query(engine::Context &ctx, const Slice &user_key, const Slice &items, bool *exists) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TopKMetadata topk_metadata;
  rocksdb::Status s = getTopKMetadata(ctx, ns_key, &topk_metadata);
  if (!s.ok()) return s;

  BlockSplitTopK topk(topk_metadata.top_k, topk_metadata.width, topk_metadata.depth, topk_metadata.decay);
  s = getTopKData(ctx, ns_key, topk_metadata, &topk);
  if (!s.ok()) return s;

  *exists = topk.Query(items.data_);

  return rocksdb::Status::OK();
}

rocksdb::Status TopK::List(engine::Context &ctx, const Slice &user_key, std::vector<std::string> &items) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TopKMetadata topk_metadata;
  rocksdb::Status s = getTopKMetadata(ctx, ns_key, &topk_metadata);
  if (!s.ok()) return s;

  BlockSplitTopK topk(topk_metadata.top_k, topk_metadata.width, topk_metadata.depth, topk_metadata.decay);
  s = getTopKData(ctx, ns_key, topk_metadata, &topk);
  if (!s.ok()) return s;

  auto heap_buckets = topk.List();
  for (auto &bucket : heap_buckets) {
    items.emplace_back(bucket.item);
  }

  return rocksdb::Status::OK();
}

rocksdb::Status TopK::Info(engine::Context &ctx, const Slice &user_key, TopKInfo *info) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TopKMetadata metadata;
  auto s = getTopKMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  info->k = metadata.top_k;
  info->width = metadata.width;
  info->depth = metadata.depth;
  info->decay = metadata.decay;

  return rocksdb::Status::OK();
}

rocksdb::Status TopK::getTopKMetadata(engine::Context &ctx, const Slice &ns_key, TopKMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisTopK}, ns_key, metadata);
}

rocksdb::Status TopK::createTopK(engine::Context &ctx, const Slice &ns_key, uint32_t k, uint32_t width, uint32_t depth,
                                 double decay, TopKMetadata *metadata) {
  metadata->top_k = k;
  metadata->width = width;
  metadata->depth = depth;
  metadata->decay = decay;

  BlockSplitTopK block_split_top_k(k, width, depth, decay);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTopK, {"createTopK"});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string top_k_meta_bytes;
  metadata->Encode(&top_k_meta_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, top_k_meta_bytes);
  if (!s.ok()) return s;

  // is dirty vector to optimize writes
  std::vector<bool> is_dirty_buckets(width * depth, true);
  std::vector<bool> is_dirty_heaps(k, true);
  s = setTopkData(ctx, ns_key, *metadata, block_split_top_k, is_dirty_buckets, is_dirty_heaps);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TopK::getTopKData(engine::Context &ctx, const Slice &ns_key, const TopKMetadata &metadata,
                                  BlockSplitTopK *topk) {
  for (uint8_t i = 0; i < 3; i++) {
    std::string tk_key = getTKKey(ns_key, metadata, i);
    rocksdb::PinnableSlice pinnable_value;
    rocksdb::Status s = storage_->Get(ctx, ctx.GetReadOptions(), tk_key, &pinnable_value);
    if (!s.ok()) return s;
    if (i == 0) {
      // get buckets of topk structure
      for (uint32_t j = 0; j < metadata.width * metadata.depth; j++) {
        for (uint8_t k = 0; k < 2; k++) {
          std::string bk_key = getSubKey(ns_key, metadata, i, j, k);
          rocksdb::PinnableSlice bk_value;
          rocksdb::Status s = storage_->Get(ctx, ctx.GetReadOptions(), bk_key, &bk_value);
          if (!s.ok()) return s;
          
          int dep = j / metadata.width;
          int wid = j % metadata.width;
          if (k == 0) {
            topk->buckets[dep][wid].fp = static_cast<uint32_t>(std::stoul(pinnable_value.data()));
          } else {
            topk->buckets[dep][wid].count = static_cast<uint32_t>(std::stoul(pinnable_value.data()));
          }
        }
      }
    } else if (i == 1) {
      // get heapbucket of topk structure
      for (uint32_t j = 0; j < metadata.top_k; j++) {
        for (uint8_t k = 0; k < 3; k++) {
          std::string hb_key = getSubKey(ns_key, metadata, i, j, k);
          rocksdb::PinnableSlice hb_value;
          rocksdb::Status s = storage_->Get(ctx, ctx.GetReadOptions(), hb_key, &hb_value);
          if (!s.ok()) return s;

          if (k == 0) {
            topk->heap[j].count = static_cast<uint32_t>(std::stoul(pinnable_value.data()));
          } else if (k == 1) {
            topk->heap[j].fp = static_cast<uint32_t>(std::stoul(pinnable_value.data()));
          } else {
            topk->heap[j].item = hb_value.data();
          }
        }
      }
    } else {
      topk->heap_size = static_cast<int>(std::stoul(pinnable_value.data()));
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TopK::setTopkData(engine::Context &ctx, const Slice &ns_key, const TopKMetadata &metadata,
                                  const BlockSplitTopK &topk, const std::vector<bool> &is_dirty_buckets, 
                                  const std::vector<bool> &is_dirty_heaps) {
  auto batch = storage_->GetWriteBatchBase();

  for (uint8_t i = 0; i < 3; i++) {
    if (i == 0) {
      for (uint32_t j = 0; j < metadata.width * metadata.depth; j++) {
        if (!is_dirty_buckets[j]) {
          continue;
        }
        for (uint32_t k = 0; k < 2; k++) {
          std::string sub_key = getSubKey(ns_key, metadata, i, j, k);
          std::string sub_value;
          int dep = j / metadata.width;
          int wid = j % metadata.width;
          if (k == 0) {
            sub_value = std::to_string(topk.buckets[dep][wid].fp);
          } else {
            sub_value = std::to_string(topk.buckets[dep][wid].count);
          }
          rocksdb::Status s = batch->Put(sub_key, sub_value);
          if (!s.ok()) return s;
        }
      }
    } else if (i == 1) {
      for (uint32_t j = 0; j < metadata.top_k; j++) {
        if (!is_dirty_heaps[j]) {
          continue;
        }
        for (uint8_t k = 0; k < 3; k++) {
          std::string sub_key = getSubKey(ns_key, metadata, i, j, k);
          std::string sub_value;
          if (k == 0) {
            sub_value = std::to_string(topk.heap[j].count);
          } else if (k == 1) {
            sub_value = std::to_string(topk.heap[j].fp);
          } else {
            sub_value = topk.heap[j].item;
          }
          rocksdb::Status s = batch->Put(sub_key, sub_value);
          if (!s.ok()) return s;
        }
      }
    } else {
      std::string tk_key = getTKKey(ns_key, metadata, i);
      std::string tk_value;
      tk_value = std::to_string(topk.heap_size);
      rocksdb::Status s = batch->Put(tk_key, tk_value);
      if (!s.ok()) return s;
    }
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

std::string TopK::getTKKey(const Slice &ns_key, const TopKMetadata &metadata, uint8_t index) {
  std::string sub_key;
  PutFixed8(&sub_key, index);
  std::string bf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return bf_key;
}

std::string TopK::getSubKey(const Slice &ns_key, const TopKMetadata &metadata, uint8_t topk_index, uint32_t sub_index, uint8_t index) {
  std::string sub_key;
  PutFixed8(&sub_key, topk_index);
  PutFixed32(&sub_key, sub_index);
  PutFixed8(&sub_key, index);
  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

}  // namespace redis