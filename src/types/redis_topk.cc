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
  std::string ns_key = AppendNamespacePrefix(user_key);

  TopKMetadata topk_metadata;
  rocksdb::Status s = getTopKMetadata(ctx, ns_key, &topk_metadata);
  if (!s.ok()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTopK, {"Add"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  BlockSplitTopK topk(topk_metadata.top_k, topk_metadata.width, topk_metadata.depth, topk_metadata.decay);
  s = getTopKData(ctx, ns_key, topk_metadata, &topk);
  if (!s.ok()) return s;

  topk.Add(items.data_, 1);

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
    items.emplace_back(bucket.item, bucket.itemlen);
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

  s = setTopkData(ctx, ns_key, *metadata, block_split_top_k);
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
      if (pinnable_value.size() != metadata.width * metadata.depth * sizeof(Bucket)) {
        return rocksdb::Status::Corruption("TopK data corrupted: buckets size mismatch");
      }
      memcpy(topk->buckets, pinnable_value.data(), pinnable_value.size());
    } else if (i == 1) {
      if (pinnable_value.size() != metadata.top_k * sizeof(HeapBucket)) {
        return rocksdb::Status::Corruption("TopK data corrupted: heap size mismatch");
      }
      memcpy(topk->heap, pinnable_value.data(), pinnable_value.size());
      for (uint32_t j = 0; j < metadata.top_k; j++) {
        std::string hb_key = getHBKey(ns_key, metadata, i, j);
        rocksdb::PinnableSlice hb_value;
        rocksdb::Status s = storage_->Get(ctx, ctx.GetReadOptions(), hb_key, &hb_value);
        if (!s.ok()) return s;
        if (hb_value.size() != topk->heap[j].itemlen) {
          return rocksdb::Status::Corruption("TopK data corrupted: heap bucket size mismatch");
        }
        topk->heap[j].item = new char[topk->heap[j].itemlen];
        memcpy(topk->heap[j].item, hb_value.data(), hb_value.size());
      }
    } else {
      topk->heap_size = static_cast<uint32_t>(std::stoul(pinnable_value.data()));
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TopK::setTopkData(engine::Context &ctx, const Slice &ns_key, const TopKMetadata &metadata,
                                  const BlockSplitTopK &topk) {
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTopK, {"setTopkData"});
  rocksdb::Status s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  for (uint8_t i = 0; i < 3; i++) {
    std::string tk_key = getTKKey(ns_key, metadata, i);
    std::string tk_value;
    if (i == 0) {
      tk_value.assign(reinterpret_cast<const char *>(topk.buckets), metadata.width * metadata.depth * sizeof(Bucket));
    } else if (i == 1) {
      tk_value.assign(reinterpret_cast<const char *>(topk.heap), metadata.top_k * sizeof(HeapBucket));
      for (uint32_t j = 0; j < metadata.top_k; j++) {
        std::string hb_key = getHBKey(ns_key, metadata, i, j);
        std::string hb_value(topk.heap[j].item, topk.heap[j].itemlen);
        s = batch->Put(hb_key, hb_value);
        if (!s.ok()) return s;
      }
    } else {
      tk_value = std::to_string(topk.heap_size);
    }
    rocksdb::Status s = batch->Put(tk_key, tk_value);
    if (!s.ok()) return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

std::string TopK::getTKKey(const Slice &ns_key, const TopKMetadata &metadata, uint8_t index) {
  std::string sub_key;
  PutFixed8(&sub_key, index);
  std::string bf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return bf_key;
}

std::string TopK::getHBKey(const Slice &ns_key, const TopKMetadata &metadata, uint8_t topk_index, uint32_t hp_index) {
  std::string sub_key;
  PutFixed8(&sub_key, topk_index);
  PutFixed32(&sub_key, hp_index);
  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

}  // namespace redis