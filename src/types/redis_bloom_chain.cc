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
  sub_key.append(kBloomFilterSeparator);
  PutFixed16(&sub_key, filters_index);

  std::string bf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return bf_key;
}

void BloomChain::getBFKeyList(const Slice &ns_key, const BloomChainMetadata &metadata,
                              std::vector<std::string> &bf_key_list) {
  bf_key_list.reserve(metadata.n_filters);
  for (uint16_t i = 0; i < metadata.n_filters; ++i) {
    std::string bf_key = getBFKey(ns_key, metadata, i);
    bf_key_list.push_back(std::move(bf_key));
  }
}

rocksdb::Status BloomChain::getBloomChainMetadata(const Slice &ns_key, BloomChainMetadata *metadata) {
  return Database::GetMetadata(kRedisBloomFilter, ns_key, metadata);
}

rocksdb::Status BloomChain::createBloomChain(const Slice &ns_key, double error_rate, uint32_t capacity,
                                             uint16_t expansion, BloomChainMetadata &sb_chain_metadata) {
  sb_chain_metadata.n_filters = 1;
  sb_chain_metadata.expansion = expansion;
  sb_chain_metadata.size = 0;

  sb_chain_metadata.error_rate = error_rate;
  sb_chain_metadata.base_capacity = capacity;
  sb_chain_metadata.bloom_bytes = BlockSplitBloomFilter::OptimalNumOfBytes(capacity, error_rate);

  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(sb_chain_metadata.bloom_bytes);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"createSBChain"});
  batch->PutLogData(log_data.Encode());

  std::string sb_chain_meta_bytes;
  sb_chain_metadata.Encode(&sb_chain_meta_bytes);
  batch->Put(metadata_cf_handle_, ns_key, sb_chain_meta_bytes);

  std::string bf_key = getBFKey(ns_key, sb_chain_metadata, sb_chain_metadata.n_filters - 1);
  batch->Put(bf_key, block_split_bloom_filter.GetData());

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BloomChain::bloomCheckAdd(const Slice &bf_key, const std::string &item, ReadWriteMode mode, int &ret) {
  std::string bf_data;
  rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), bf_key, &bf_data);
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(std::move(bf_data));

  uint64_t h = BlockSplitBloomFilter::Hash(item.data(), item.size());
  bool found = block_split_bloom_filter.FindHash(h);

  ret = 0;
  if (found && mode == ReadWriteMode::READ) {
    ret = 1;
  } else if (!found && mode == ReadWriteMode::WRITE) {
    auto batch = storage_->GetWriteBatchBase();
    ret = 1;
    block_split_bloom_filter.InsertHash(h);
    batch->Put(bf_key, block_split_bloom_filter.GetData());
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }

  return rocksdb::Status::OK();
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

  return createBloomChain(ns_key, error_rate, capacity, expansion, bloom_chain_metadata);
}

rocksdb::Status BloomChain::Add(const Slice &user_key, const Slice &item, int &ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  BloomChainMetadata sb_chain_metadata;
  rocksdb::Status s = getBloomChainMetadata(ns_key, &sb_chain_metadata);

  if (s.IsNotFound()) {
    rocksdb::Status create_s =
        createBloomChain(ns_key, kBFDefaultErrorRate, kBFDefaultInitCapacity, kBFDefaultExpansion, sb_chain_metadata);
    if (!create_s.ok()) return create_s;
  } else if (!s.ok()) {
    return s;
  }

  std::vector<std::string> bf_key_list;
  getBFKeyList(ns_key, sb_chain_metadata, bf_key_list);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"insert"});
  batch->PutLogData(log_data.Encode());

  bool item_exist = false;
  std::string item_string = item.ToString();

  // check
  for (int i = sb_chain_metadata.n_filters - 1; i >= 0; --i) {  // TODO: to test which direction for searching is better
    s = bloomCheckAdd(bf_key_list[i], item_string, ReadWriteMode::READ, ret);
    if (ret == 1) {
      item_exist = true;
      break;
    }
  }

  // insert
  if (!item_exist) {
    if (sb_chain_metadata.size + 1 > sb_chain_metadata.GetCapacity()) {  // TODO: scaling would be supported later
      return rocksdb::Status::Aborted("filter is full");
    }
    s = bloomCheckAdd(bf_key_list.back(), item_string, ReadWriteMode::WRITE, ret);
    sb_chain_metadata.size += ret;
  }

  std::string sb_chain_metadata_bytes;
  sb_chain_metadata.Encode(&sb_chain_metadata_bytes);
  batch->Put(metadata_cf_handle_, ns_key, sb_chain_metadata_bytes);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BloomChain::Exist(const Slice &user_key, const Slice &item, int &ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  BloomChainMetadata sb_chain_metadata;
  rocksdb::Status s = getBloomChainMetadata(ns_key, &sb_chain_metadata);
  if (s.IsNotFound()) return rocksdb::Status::NotFound("key is not found");
  if (!s.ok()) return s;

  std::vector<std::string> bf_key_list;
  getBFKeyList(ns_key, sb_chain_metadata, bf_key_list);

  std::string item_string = item.ToString();
  // check
  for (int i = sb_chain_metadata.n_filters - 1; i >= 0; --i) {  // TODO: to test which direction for searching is better
    s = bloomCheckAdd(bf_key_list[i], item_string, ReadWriteMode::READ, ret);
    if (ret == 1) {
      break;
    }
  }

  return rocksdb::Status::OK();
}
}  // namespace redis
