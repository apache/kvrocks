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

#include "redis_sb_chain.h"

namespace redis {

void SBChain::bfInit(BFMetadata *bf_metadata, std::string *bf_data, uint32_t entries, double error) {
  bf_metadata->entries = entries;
  bf_metadata->error = error;
  bf_metadata->size = 0;

  uint32_t bf_bytes = BlockSplitBloomFilter::OptimalNumOfBytes(entries, error);
  bf_metadata->bf_bytes = bf_bytes;

  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(bf_bytes);
  *bf_data = block_split_bloom_filter.GetData();
}

std::string SBChain::getBFKey(const Slice &ns_key, const SBChainMetadata &metadata, uint16_t filters_index) {
  std::string sub_key;
  sub_key.append(kBloomFilterSeparator);
  PutFixed16(&sub_key, filters_index);

  std::string bf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return bf_key;
}

std::string SBChain::getBFMetaKey(const Slice &ns_key, const SBChainMetadata &metadata, uint16_t filters_index) {
  std::string sub_key;
  sub_key.append(kBloomFilterSeparator);
  PutFixed16(&sub_key, filters_index);
  sub_key.append(kBloomFilterSeparator);
  sub_key.append("meta");

  std::string bf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return bf_key;
}

rocksdb::Status SBChain::getSBChainMetadata(const Slice &ns_key, SBChainMetadata *metadata) {
  return Database::GetMetadata(kRedisBloomFilter, ns_key, metadata);
}

rocksdb::Status SBChain::getBFMetadata(const Slice &bf_meta_key, BFMetadata *metadata) {
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string bytes;
  auto s = storage_->Get(read_options, bf_meta_key, &bytes);

  std::string old_metadata;
  metadata->Encode(&old_metadata);

  if (!s.ok()) return s;
  metadata->Decode(bytes);

  if (metadata->Expired()) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound(kErrMsgKeyExpired);
  }
  return s;
}

rocksdb::Status SBChain::createSBChain(const Slice &ns_key, double error_rate, uint32_t capacity, uint16_t expansion,
                                       uint16_t scaling, SBChainMetadata &sb_chain_metadata) {
  sb_chain_metadata.n_filters = 1;
  sb_chain_metadata.expansion = expansion;
  sb_chain_metadata.scaling = scaling;
  sb_chain_metadata.size = 0;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"createSBChain"});
  batch->PutLogData(log_data.Encode());

  std::string sb_chain_meta_bytes;
  sb_chain_metadata.Encode(&sb_chain_meta_bytes);
  batch->Put(metadata_cf_handle_, ns_key, sb_chain_meta_bytes);

  std::string bf_key = getBFKey(ns_key, sb_chain_metadata, sb_chain_metadata.n_filters - 1);
  std::string bf_meta_key = getBFMetaKey(ns_key, sb_chain_metadata, sb_chain_metadata.n_filters - 1);
  BFMetadata bf_metadata;
  std::string bf_data;
  bfInit(&bf_metadata, &bf_data, capacity, error_rate);
  std::string bf_meta_bytes;
  bf_metadata.Encode(&bf_meta_bytes);
  batch->Put(bf_meta_key, bf_meta_bytes);
  batch->Put(bf_key, bf_data);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status SBChain::bloomCheckAdd(const Slice &bf_key, const std::string &item, ReadWriteMode mode, int &ret) {
  std::string bf_data;
  rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), bf_key, &bf_data);
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(bf_data);

  uint64_t h = BlockSplitBloomFilter::Hash(item.data(), item.size());
  bool found = block_split_bloom_filter.FindHash(h);

  ret = 0;
  if (found && mode == ReadWriteMode::READ) {
    ret = 1;
  } else if (!found && mode == ReadWriteMode::WRITE) {
    auto batch = storage_->GetWriteBatchBase();
    ret = 1;
    block_split_bloom_filter.InsertHash(h);
    bf_data = block_split_bloom_filter.GetData();
    batch->Put(bf_key, bf_data);
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }

  return rocksdb::Status::OK();
}

rocksdb::Status SBChain::Reserve(const Slice &user_key, uint32_t capacity, double error_rate, uint16_t expansion,
                                 uint16_t scaling) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  SBChainMetadata sb_chain_metadata;
  rocksdb::Status s = getSBChainMetadata(ns_key, &sb_chain_metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (!s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument("the key already exists");
  }

  return createSBChain(ns_key, error_rate, capacity, expansion, scaling, sb_chain_metadata);
}

rocksdb::Status SBChain::Add(const Slice &user_key, const Slice &item, int &ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  SBChainMetadata sb_chain_metadata;
  rocksdb::Status s = getSBChainMetadata(ns_key, &sb_chain_metadata);

  if (s.IsNotFound()) {
    rocksdb::Status create_s =
        createSBChain(ns_key, kBFDefaultErrorRate, kBFDefaultInitCapacity, kBFDefaultExpansion, 0, sb_chain_metadata);
    if (!create_s.ok()) return create_s;
  } else if (!s.ok()) {
    return s;
  }

  std::vector<std::string> bf_key_list;
  std::vector<BFMetadata> bf_metadata_list;
  for (u_int16_t i = 0; i < sb_chain_metadata.n_filters; ++i) {
    std::string bf_key = getBFKey(ns_key, sb_chain_metadata, i);
    std::string bf_meta_key = getBFMetaKey(ns_key, sb_chain_metadata, i);
    BFMetadata bf_metadata;
    s = getBFMetadata(bf_meta_key, &bf_metadata);
    if (!s.ok()) return s;
    bf_key_list.push_back(bf_key);
    bf_metadata_list.push_back(bf_metadata);
  }

  BFMetadata cur_bf_metadata = bf_metadata_list.back();
  std::string cur_bf_key = bf_key_list.back();
  if (cur_bf_metadata.size + 1 > cur_bf_metadata.entries) {
    return rocksdb::Status::Aborted("filter is full");
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"insert"});
  batch->PutLogData(log_data.Encode());

  bool item_exist = false;
  const auto &item_string = item.ToString();

  // check
  for (int i = sb_chain_metadata.n_filters - 1; i >= 0; --i) {
    s = bloomCheckAdd(bf_key_list[i], item_string, ReadWriteMode::READ, ret);
    if (ret == 1) {
      item_exist = true;
      break;
    }
  }

  // insert
  if (!item_exist) {
    s = bloomCheckAdd(cur_bf_key, item_string, ReadWriteMode::WRITE, ret);
    cur_bf_metadata.size += ret;
    sb_chain_metadata.size += ret;
  }
  std::string cur_bf_meta_key = getBFMetaKey(ns_key, sb_chain_metadata, sb_chain_metadata.n_filters - 1);
  std::string cur_bf_metadata_bytes, sb_chain_metadata_bytes;
  cur_bf_metadata.Encode(&cur_bf_metadata_bytes);
  sb_chain_metadata.Encode(&sb_chain_metadata_bytes);
  batch->Put(cur_bf_meta_key, cur_bf_metadata_bytes);
  batch->Put(metadata_cf_handle_, ns_key, sb_chain_metadata_bytes);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status SBChain::Exist(const Slice &user_key, const Slice &item, int &ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  SBChainMetadata sb_chain_metadata;
  rocksdb::Status s = getSBChainMetadata(ns_key, &sb_chain_metadata);
  if (s.IsNotFound()) return rocksdb::Status::NotFound("key is not found");
  if (!s.ok()) return s;

  std::vector<std::string> bf_key_list;
  for (uint16_t i = 0; i < sb_chain_metadata.n_filters; ++i) {
    std::string bf_key = getBFKey(ns_key, sb_chain_metadata, i);
    bf_key_list.push_back(bf_key);
  }

  const auto &item_string = item.ToString();
  // check
  for (int i = sb_chain_metadata.n_filters - 1; i >= 0; --i) {
    s = bloomCheckAdd(bf_key_list[i], item_string, ReadWriteMode::READ, ret);
    if (ret == 1) {
      break;
    }
  }

  return rocksdb::Status::OK();
}

}  // namespace redis
