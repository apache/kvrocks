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
#include "topk.h"
#include "commands/ttl_util.h"

namespace redis {
    
rocksdb::Status TopK::Reserve(engine::Context &ctx, const Slice& user_key, 
                              uint32_t k, uint32_t width, uint32_t depth, double decay) {
    std::string ns_key = AppendNamespacePrefix(user_key);

    TopKMetadata topk_metadata;
    rocksdb::Status s = getTopKMetadata(ctx, ns_key, &topk_metadata);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (!s.IsNotFound()) {
        return rocksdb::Status::InvalidArgument("TopK already exists");
    }

    return createTopK(ctx, ns_key, k, width, depth, decay, &topk_metadata);
}

/* TODO: implemention */
rocksdb::Status TopK::Add([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] const Slice &user_key, 
                          [[maybe_unused]] const Slice &items) {
    return rocksdb::Status::OK();
}

/* TODO: implemention */
rocksdb::Status TopK::Query([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] const Slice& user_key, 
                            [[maybe_unused]] const Slice &items, [[maybe_unused]] bool *exists) { 
    return rocksdb::Status::OK();
}

/* TODO: implemention */
rocksdb::Status TopK::List([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] const Slice& user_key, 
                            [[maybe_unused]] std::vector<std::string> &items) {
    return rocksdb::Status::OK();
}

rocksdb::Status TopK::Info(engine::Context &ctx, const Slice& user_key, TopKInfo *info) {
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

rocksdb::Status TopK::createTopK(engine::Context &ctx, const Slice &ns_key, 
                                 uint32_t k, uint32_t width, uint32_t depth, double decay,
                                 TopKMetadata *metadata) {
    metadata->top_k = k;
    metadata->width = width;
    metadata->depth = depth;
    metadata->decay = decay;
    
    auto block_split_top_k = CreateBlockSplitTopK(k, width, depth, decay);

    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisTopK, {"createTopK"});
    auto s = batch->PutLogData(log_data.Encode());
    if (!s.ok()) return s;
    
    std::string top_k_meta_bytes;
    metadata->Encode(&top_k_meta_bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, top_k_meta_bytes);
    if (!s.ok()) return s;

    std::string tk_key = getTKKey(ns_key, *metadata);
    // TODO: how to save data structure---block split topk
    s = batch->Put(ns_key, block_split_top_k.GetData());
    if (!s.ok()) return s;

    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

std::string TopK::getTKKey(const Slice &ns_key, const TopKMetadata &metadata) {
    std::string sub_key;

    return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

    
} // namespace redis