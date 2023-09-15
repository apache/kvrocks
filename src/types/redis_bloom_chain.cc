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

#include "redis_bloom_chain.h"

namespace redis {

std::string BloomChain::getBFKey(const Slice &ns_key, const BloomChainMetadata &metadata, uint16_t filters_index) {
  std::string sub_key;
  PutFixed16(&sub_key, filters_index);
  std::string bf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return bf_key;
}

void BloomChain::getBFKeyList(const Slice &ns_key, const BloomChainMetadata &metadata,
                              std::vector<std::string> *bf_key_list) {
  bf_key_list->reserve(metadata.n_filters);
  for (uint16_t i = 0; i < metadata.n_filters; ++i) {
    std::string bf_key = getBFKey(ns_key, metadata, i);
    bf_key_list->push_back(std::move(bf_key));
  }
}

rocksdb::Status BloomChain::getBloomChainMetadata(const Slice &ns_key, BloomChainMetadata *metadata) {
  return Database::GetMetadata(kRedisBloomFilter, ns_key, metadata);
}

rocksdb::Status BloomChain::createBloomChain(const Slice &ns_key, double error_rate, uint32_t capacity,
                                             uint16_t expansion, BloomChainMetadata *metadata) {
  metadata->n_filters = 1;
  metadata->expansion = expansion;
  metadata->size = 0;

  metadata->error_rate = error_rate;
  metadata->base_capacity = capacity;
  metadata->bloom_bytes = BlockSplitBloomFilter::OptimalNumOfBytes(capacity, error_rate);

  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(metadata->bloom_bytes);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"createBloomChain"});
  batch->PutLogData(log_data.Encode());

  std::string bloom_chain_meta_bytes;
  metadata->Encode(&bloom_chain_meta_bytes);
  batch->Put(metadata_cf_handle_, ns_key, bloom_chain_meta_bytes);

  std::string bf_key = getBFKey(ns_key, *metadata, metadata->n_filters - 1);
  batch->Put(bf_key, block_split_bloom_filter.GetData());

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

void BloomChain::createBloomFilterInBatch(const Slice &ns_key, BloomChainMetadata *metadata,
                                          ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch, std::string *bf_data) {
  uint32_t bloom_filter_bytes = BlockSplitBloomFilter::OptimalNumOfBytes(
      static_cast<uint32_t>(metadata->base_capacity * pow(metadata->expansion, metadata->n_filters)),
      metadata->error_rate);
  metadata->n_filters += 1;
  metadata->bloom_bytes += bloom_filter_bytes;

  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(bloom_filter_bytes);
  *bf_data = std::move(block_split_bloom_filter).GetData();

  std::string bloom_chain_meta_bytes;
  metadata->Encode(&bloom_chain_meta_bytes);
  batch->Put(metadata_cf_handle_, ns_key, bloom_chain_meta_bytes);
}

rocksdb::Status BloomChain::getBFDataList(const std::vector<std::string> &bf_key_list,
                                          std::vector<std::string> *bf_data_list) {
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();

  bf_data_list->reserve(bf_key_list.size());
  for (const auto &bf_key : bf_key_list) {
    std::string bf_data;
    rocksdb::Status s = storage_->Get(read_options, bf_key, &bf_data);
    if (!s.ok()) return s;
    bf_data_list->push_back(std::move(bf_data));
  }
  return rocksdb::Status::OK();
}

void BloomChain::bloomAdd(const Slice &item, std::string *bf_data) {
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(std::move(*bf_data));

  uint64_t h = BlockSplitBloomFilter::Hash(item.data(), item.size());
  block_split_bloom_filter.InsertHash(h);
  *bf_data = std::move(block_split_bloom_filter).GetData();
}

bool BloomChain::bloomCheck(const Slice &item, std::string &bf_data) {
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(bf_data);

  uint64_t h = BlockSplitBloomFilter::Hash(item.data(), item.size());
  return block_split_bloom_filter.FindHash(h);
}

rocksdb::Status BloomChain::Reserve(const Slice &user_key, uint32_t capacity, double error_rate, uint16_t expansion) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  BloomChainMetadata bloom_chain_metadata;
  rocksdb::Status s = getBloomChainMetadata(ns_key, &bloom_chain_metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (!s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument("the key already exists");
  }

  return createBloomChain(ns_key, error_rate, capacity, expansion, &bloom_chain_metadata);
}

rocksdb::Status BloomChain::Add(const Slice &user_key, const Slice &item, int *ret) {
  std::vector<int> tmp{0};
  rocksdb::Status s = MAdd(user_key, {item}, &tmp);
  *ret = tmp[0];
  return s;
}

rocksdb::Status BloomChain::MAdd(const Slice &user_key, const std::vector<Slice> &items, std::vector<int> *rets) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  BloomChainMetadata metadata;
  rocksdb::Status s = getBloomChainMetadata(ns_key, &metadata);

  if (s.IsNotFound()) {
    s = createBloomChain(ns_key, kBFDefaultErrorRate, kBFDefaultInitCapacity, kBFDefaultExpansion, &metadata);
  }
  if (!s.ok()) return s;

  std::vector<std::string> bf_key_list;
  getBFKeyList(ns_key, metadata, &bf_key_list);

  std::vector<std::string> bf_data_list;
  s = getBFDataList(bf_key_list, &bf_data_list);
  if (!s.ok()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"insert"});
  batch->PutLogData(log_data.Encode());

  for (size_t i = 0; i < items.size(); ++i) {
    // check
    bool exists = false;
    // TODO: to test which direction for searching is better
    for (int ii = static_cast<int>(bf_data_list.size()) - 1; ii >= 0; --ii) {
      exists = bloomCheck(items[i], bf_data_list[ii]);
      if (exists) break;
    }

    // insert
    if (exists) {
      (*rets)[i] = 0;
    } else {
      if (metadata.size + 1 > metadata.GetCapacity()) {
        if (metadata.IsScaling()) {
          batch->Put(bf_key_list.back(), bf_data_list.back());
          std::string bf_data;
          createBloomFilterInBatch(ns_key, &metadata, batch, &bf_data);
          bf_data_list.push_back(std::move(bf_data));
          bf_key_list.push_back(getBFKey(ns_key, metadata, metadata.n_filters - 1));
        } else {
          (*rets)[i] = -1;  // -1 means nonscaling filter is full
          continue;
        }
      }
      bloomAdd(items[i], &bf_data_list.back());
      (*rets)[i] = 1;
      metadata.size += 1;
    }
  }

  std::string bloom_chain_metadata_bytes;
  metadata.Encode(&bloom_chain_metadata_bytes);
  batch->Put(metadata_cf_handle_, ns_key, bloom_chain_metadata_bytes);
  batch->Put(bf_key_list.back(), bf_data_list.back());
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BloomChain::Exists(const Slice &user_key, const Slice &item, int *ret) {
  std::vector<int> tmp{0};
  rocksdb::Status s = MExists(user_key, {item}, &tmp);
  *ret = tmp[0];
  return s;
}

rocksdb::Status BloomChain::MExists(const Slice &user_key, const std::vector<Slice> &items, std::vector<int> *rets) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  BloomChainMetadata metadata;
  rocksdb::Status s = getBloomChainMetadata(ns_key, &metadata);
  if (s.IsNotFound()) {
    std::fill(rets->begin(), rets->end(), 0);
    return rocksdb::Status::OK();
  }
  if (!s.ok()) return s;

  std::vector<std::string> bf_key_list;
  getBFKeyList(ns_key, metadata, &bf_key_list);

  std::vector<std::string> bf_data_list;
  s = getBFDataList(bf_key_list, &bf_data_list);
  if (!s.ok()) return s;

  for (size_t i = 0; i < items.size(); ++i) {
    // check
    bool exists = false;
    // TODO: to test which direction for searching is better
    for (int ii = static_cast<int>(bf_data_list.size()) - 1; ii >= 0; --ii) {
      exists = bloomCheck(items[i], bf_data_list[ii]);
      if (exists) break;
    }
    (*rets)[i] = exists ? 1 : 0;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status BloomChain::Info(const Slice &user_key, BloomFilterInfo *info) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  BloomChainMetadata metadata;
  rocksdb::Status s = getBloomChainMetadata(ns_key, &metadata);
  if (!s.ok()) return s;

  info->capacity = metadata.GetCapacity();
  info->bloom_bytes = metadata.bloom_bytes;
  info->n_filters = metadata.n_filters;
  info->size = metadata.size;
  info->expansion = metadata.expansion;

  return rocksdb::Status::OK();
}

}  // namespace redis
