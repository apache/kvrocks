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

#include "redis_bloomfilter.h"

namespace redis {

void SBChain::appendBFSuffix(const Slice &ns_key, uint16_t filters_index, std::string *output) {
  output->clear();

  output->append(ns_key.data(), ns_key.size());
  output->append(kBloomFilterSeparator);
  PutFixed16(output, filters_index);
}

void SBChain ::appendBFMetaSuffix(const Slice &bf_key, std::string *output) {
  output->clear();

  output->append(bf_key.data(), bf_key.size());
  output->append(kBloomFilterSeparator);
  output->append("meta");
}

void SBChain::bfInit(BFMetadata *bf_metadata, std::string *bf_data, uint32_t entries, double error) {
  bf_metadata->error = error;
  bf_metadata->entries = entries;
  bf_metadata->size = 0;

  uint32_t bf_bytes = BlockSplitBloomFilter::OptimalNumOfBytes(entries, error);
  bf_metadata->bf_bytes = bf_bytes;

  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(bf_bytes);
  *bf_data = block_split_bloom_filter.GetData();
}

rocksdb::Status SBChain::getSBChainMetadata(const Slice &ns_key, SBChainMetadata *metadata) {
  return Database::GetMetadata(kRedisBloomFilter, ns_key, metadata);
}

rocksdb::Status SBChain::getBFMetadata(const Slice &bf_meta_key, BFMetadata *metadata) {
  return Database::GetMetadata(kRedisBloomFilter, bf_meta_key, metadata);
}

rocksdb::Status SBChain::createSBChain(const Slice &ns_key, double error_rate, uint32_t capacity, uint16_t expansion,
                                       uint16_t scaling, SBChainMetadata &sb_chain_metadata) {
  sb_chain_metadata.n_filters = 1;
  sb_chain_metadata.growth = expansion;
  sb_chain_metadata.options = scaling;
  sb_chain_metadata.size = 0;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"createSBChain"});
  batch->PutLogData(log_data.Encode());

  std::string sb_chain_bytes;
  sb_chain_metadata.Encode(&sb_chain_bytes);
  batch->Put(metadata_cf_handle_, ns_key, sb_chain_bytes);

  std::string bf_key;
  appendBFSuffix(ns_key, sb_chain_metadata.n_filters - 1, &bf_key);
  std::string bf_meta_key;
  appendBFMetaSuffix(bf_key, &bf_meta_key);
  BFMetadata bf_metadata;
  std::string bf_data;
  bfInit(&bf_metadata, &bf_data, capacity, error_rate);
  std::string bf_meta_bytes;
  bf_metadata.Encode(&bf_meta_bytes);
  batch->Put(metadata_cf_handle_, bf_meta_key, bf_meta_bytes);
  batch->Put(bf_key, bf_data);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status SBChain::createBloomFilter(const Slice &ns_key, SBChainMetadata &sb_chain_metadata,
                                           BFMetadata &bf_metadata, std::string &bf_key) {
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"createBloomFilter"});
  batch->PutLogData(log_data.Encode());

  BFMetadata old_bf_metadata = bf_metadata;
  sb_chain_metadata.n_filters += 1;
  appendBFSuffix(ns_key, sb_chain_metadata.n_filters - 1, &bf_key);
  std::string bf_meta_key;
  appendBFMetaSuffix(bf_key, &bf_meta_key);
  std::string bf_data;
  bfInit(&bf_metadata, &bf_data, sb_chain_metadata.growth * old_bf_metadata.entries,
         old_bf_metadata.error * kErrorTighteningRatio);
  std::string bf_meta_bytes;
  bf_metadata.Encode(&bf_meta_bytes);
  batch->Put(metadata_cf_handle_, bf_meta_key, bf_meta_bytes);
  batch->Put(bf_key, bf_data);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status SBChain::bloomCheckAdd64(const Slice &bf_key, BFMetadata &bf_metadata, const std::string &item,
                                         ReadWriteMode mode, ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                         int &ret) {
  std::string bf_data;
  rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), bf_key, &bf_data);
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(bf_data);

  uint64_t h = BlockSplitBloomFilter::Hash(item.data(), item.size());
  bool found = block_split_bloom_filter.FindHash(h);

  ret = 0;
  if (found && mode == ReadWriteMode::MODE_READ) {
    ret = 1;
  } else if (!found && mode == ReadWriteMode::MODE_WRITE) {
    ret = 1;
    block_split_bloom_filter.InsertHash(h);
    bf_data = block_split_bloom_filter.GetData();
    batch->Put(bf_key, bf_data);
    storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }

  return rocksdb::Status::OK();
}

rocksdb::Status SBChain::Reserve(const Slice &user_key, double error_rate, uint32_t capacity, uint16_t expansion,
                                 uint16_t scaling) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  SBChainMetadata sb_chain_metadata;
  rocksdb::Status s = getSBChainMetadata(ns_key, &sb_chain_metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (!s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument("item exists");
  }

  return createSBChain(ns_key, error_rate, capacity, expansion, scaling, sb_chain_metadata);
}

rocksdb::Status SBChain::Add(const Slice &user_key, const Slice &item, int &ret) {
  std::vector<int> tmp(1, 0);
  rocksdb::Status s = MAdd(user_key, {item}, tmp);
  ret = tmp[0];
  return s;
}

rocksdb::Status SBChain::MAdd(const Slice &user_key, const std::vector<Slice> &items, std::vector<int> &rets) {
  BFInsertOptions insert_options = {.capacity = kBFDefaultInitCapacity,
                                    .error_rate = kBFDefaultErrorRate,
                                    .autocreate = 1,
                                    .expansion = kBFDefaultExpansion,
                                    .scaling = 1};
  return InsertCommon(user_key, items, &insert_options, rets);
}

rocksdb::Status SBChain::InsertCommon(const Slice &user_key, const std::vector<Slice> &items,
                                      const BFInsertOptions *insert_options, std::vector<int> &rets) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  SBChainMetadata sb_chain_metadata;
  rocksdb::Status s = getSBChainMetadata(ns_key, &sb_chain_metadata);

  if (s.IsNotFound() && insert_options->autocreate) {
    rocksdb::Status create_s = createSBChain(ns_key, insert_options->error_rate, insert_options->capacity,
                                             insert_options->expansion, insert_options->scaling, sb_chain_metadata);
    if (!create_s.ok()) return create_s;
  } else if (!s.ok()) {
    return s;
  }

  std::vector<BFMetadata> bf_metadata_list;
  for (u_int16_t i = 0; i < sb_chain_metadata.n_filters; ++i) {
    std::string bf_key, bf_meta_key;
    appendBFSuffix(ns_key, i, &bf_key);
    appendBFMetaSuffix(bf_key, &bf_meta_key);
    BFMetadata bf_metadata;
    s = getBFMetadata(bf_meta_key, &bf_metadata);
    if (!s.ok()) return s;
    bf_metadata_list.push_back(bf_metadata);
  }

  BFMetadata cur_bf_metadata = bf_metadata_list.back();
  std::string cur_bf_key;
  appendBFSuffix(ns_key, sb_chain_metadata.n_filters - 1, &cur_bf_key);
  if (!sb_chain_metadata.options && cur_bf_metadata.size + items.size() > cur_bf_metadata.entries) {
    return rocksdb::Status::Aborted("filter is full and is nonscaling");
  }

  int suc_insert = 0;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"insert"});
  batch->PutLogData(log_data.Encode());
  for (size_t i = 0; i < items.size(); ++i) {
    bool item_exist = false;
    const auto &item = items[i].ToString();

    // check
    for (int ii = sb_chain_metadata.n_filters - 1; ii >= 0; --ii) {
      std::string bf_key;
      appendBFSuffix(ns_key, ii, &bf_key);
      int ret = 0;
      s = bloomCheckAdd64(bf_key, bf_metadata_list[ii], item, ReadWriteMode::MODE_READ, batch, ret);
      if (ret == 1) {
        item_exist = true;
        break;
      }
    }

    // insert
    if (!item_exist) {
      if (cur_bf_metadata.size >= cur_bf_metadata.entries) {
        std::string old_bf_metadata_bytes, cur_bf_meta_key;
        cur_bf_metadata.Encode(&old_bf_metadata_bytes);
        appendBFMetaSuffix(cur_bf_key, &cur_bf_meta_key);
        batch->Put(metadata_cf_handle_, cur_bf_meta_key, old_bf_metadata_bytes);
        createBloomFilter(ns_key, sb_chain_metadata, cur_bf_metadata,
                          cur_bf_key);  // todo: the scaling function need to test
      }
      int ret = 0;
      s = bloomCheckAdd64(cur_bf_key, cur_bf_metadata, item, ReadWriteMode::MODE_WRITE, batch, ret);
      rets[i] = ret;
      suc_insert += ret;
      cur_bf_metadata.size += ret;
    }
  }
  sb_chain_metadata.size += suc_insert;

  std::string cur_bf_metadata_bytes, cur_bf_meta_key, sb_chain_metadata_bytes;
  cur_bf_metadata.Encode(&cur_bf_metadata_bytes);
  sb_chain_metadata.Encode(&sb_chain_metadata_bytes);
  appendBFMetaSuffix(cur_bf_key, &cur_bf_meta_key);
  batch->Put(metadata_cf_handle_, cur_bf_meta_key, cur_bf_metadata_bytes);
  batch->Put(metadata_cf_handle_, ns_key, sb_chain_metadata_bytes);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status SBChain::Exist(const Slice &user_key, const Slice &item, int &ret) {
  std::vector<int> tmp(1, 0);
  rocksdb::Status s = MExist(user_key, {item}, tmp);
  ret = tmp[0];
  return s;
}

rocksdb::Status SBChain::MExist(const Slice &user_key, const std::vector<Slice> &items, std::vector<int> &rets) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  SBChainMetadata sb_chain_metadata;
  rocksdb::Status s = getSBChainMetadata(ns_key, &sb_chain_metadata);
  if (s.IsNotFound()) return rocksdb::Status::NotFound("key is not found");
  if (!s.ok()) return s;

  std::vector<BFMetadata> bf_metadata_list;
  for (uint16_t i = 0; i < sb_chain_metadata.n_filters; ++i) {
    std::string bf_key, bf_meta_key;
    appendBFSuffix(ns_key, i, &bf_key);
    appendBFMetaSuffix(bf_key, &bf_meta_key);
    BFMetadata bf_metadata;
    s = getBFMetadata(bf_meta_key, &bf_metadata);
    if (!s.ok()) return s;
    bf_metadata_list.push_back(bf_metadata);
  }

  for (size_t i = 0; i < items.size(); ++i) {
    const auto &item = items[i].ToString();
    // check
    auto batch = storage_->GetWriteBatchBase();  // todo: as placeholder, change the function to delete this
    for (int ii = sb_chain_metadata.n_filters - 1; ii >= 0; --ii) {
      std::string bf_key;
      appendBFSuffix(ns_key, ii, &bf_key);
      int ret = 0;
      s = bloomCheckAdd64(bf_key, bf_metadata_list[ii], item, ReadWriteMode::MODE_READ, batch, ret);
      if (ret == 1) {
        rets[i] = 1;
        break;
      }
    }
  }

  return rocksdb::Status::OK();
}
rocksdb::Status SBChain::Info(const Slice &user_key, const std::string &info, std::vector<int> &rets) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  SBChainMetadata sb_chain_metadata;
  rocksdb::Status s = getSBChainMetadata(ns_key, &sb_chain_metadata);
  if (s.IsNotFound()) return rocksdb::Status::NotFound("key is not found");
  if (!s.ok()) return s;

  std::vector<BFMetadata> bf_metadata_list;
  for (uint16_t i = 0; i < sb_chain_metadata.n_filters; ++i) {
    std::string bf_key, bf_meta_key;
    appendBFSuffix(ns_key, i, &bf_key);
    appendBFMetaSuffix(bf_key, &bf_meta_key);
    BFMetadata bf_metadata;
    s = getBFMetadata(bf_meta_key, &bf_metadata);
    if (!s.ok()) return s;
    bf_metadata_list.push_back(bf_metadata);
  }

  uint32_t capacity = 0;
  uint32_t size = 0;
  uint16_t filters = sb_chain_metadata.n_filters;
  uint64_t items = sb_chain_metadata.size;
  uint16_t expansion = sb_chain_metadata.growth;

  for (uint16_t i = 0; i < sb_chain_metadata.n_filters; ++i) {
    capacity += bf_metadata_list[i].entries;
    size += bf_metadata_list[i].bf_bytes;
  }

  if (info == "ALL") {
    rets[0] = static_cast<int>(capacity);
    rets[1] = static_cast<int>(size);
    rets[2] = static_cast<int>(filters);
    rets[3] = static_cast<int>(items);
    rets[4] = static_cast<int>(expansion);
  } else if (info == "CAPACITY") {
    rets[0] = static_cast<int>(capacity);
  } else if (info == "SIZE") {
    rets[0] = static_cast<int>(size);
  } else if (info == "FILTERS") {
    rets[0] = static_cast<int>(filters);
  } else if (info == "ITEMS") {
    rets[0] = static_cast<int>(items);
  } else if (info == "EXPANSION") {
    rets[0] = static_cast<int>(expansion);
  }

  return rocksdb::Status::OK();
}

}  // namespace redis