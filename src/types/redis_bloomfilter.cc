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

double BloomFilter::calcBpe(double error) {
  static const double denom = 0.480453013918201;  // ln(2)^2
  double num = log(error);

  double bpe = -(num / denom);
  if (bpe < 0) {
    bpe = -bpe;
  }
  return bpe;
}

void BloomFilter::appendBFSuffix(const Slice &ns_key, uint16_t filters_index, std::string *output) {
  output->clear();

  output->append(ns_key.data(), ns_key.size());
  output->append(kBloomFilterSeparator);
  PutFixed16(output, filters_index);
}

void BloomFilter ::appendBFMetaSuffix(const Slice &bf_key, std::string *output) {
  output->clear();

  output->append(bf_key.data(), bf_key.size());
  output->append(kBloomFilterSeparator);
  output->append("meta");
}

void BloomFilter::bfInit(BFMetadata *bf_metadata, uint64_t entries, double error, unsigned int options) {
  bf_metadata->error = error;
  bf_metadata->bf_bits = 0;
  bf_metadata->entries = entries;
  bf_metadata->bpe = calcBpe(error);
  bf_metadata->size = 0;

  uint64_t bits = 0;

  {
    double bn2 = logb(static_cast<double>(entries) * bf_metadata->bpe);
    //    if (bn2 > 63 || bn2 == INFINITY) {
    //      return 1;
    //    }
    bf_metadata->n2 = static_cast<uint8_t>(bn2) + 1;
    bits = 1LLU << bf_metadata->n2;

    // Determine the number of extra bits available for more items. We rounded
    // up the number of bits to the next-highest power of two. This means we
    // might have up to 2x the bits available to us.
    uint64_t bit_diff = bits - static_cast<uint64_t>(static_cast<double>(entries) * bf_metadata->bpe);
    // The number of additional items we can store is the extra number of bits
    // divided by bits-per-element
    auto item_diff = static_cast<uint64_t>(static_cast<double>(bit_diff) / bf_metadata->bpe);
    bf_metadata->entries += item_diff;
  }

  if (bits % 64) {
    bf_metadata->bf_bytes = ((bits / 64) + 1) * 8;
  } else {
    bf_metadata->bf_bytes = bits / 8;
  }
  bf_metadata->bf_bits = bf_metadata->bf_bytes * 8;

  bf_metadata->hashes = (int)ceil(0.693147180559945 * bf_metadata->bpe);  // ln(2)
}

BloomHashval BloomFilter::bloomCalcHash64(const void *buffer, int len) {
  BloomHashval rv;
  rv.a = MurmurHash64A_Bloom(buffer, len, 0xc6a4a7935bd1e995ULL);
  rv.b = MurmurHash64A_Bloom(buffer, len, rv.a);
  return rv;
}

int BloomFilter::testBitSetBit(std::string &value, uint64_t x, ReadWriteMode mode) {
  uint8_t mask = 1 << (x % 8);
  uint32_t byte_index = (x / 8) % kBloomFilterSegmentBytes;
  uint8_t c = value[byte_index];  // expensive memory access

  if (c & mask) {
    return 1;
  } else {
    if (mode == ReadWriteMode::MODE_WRITE) {
      uint8_t s = c | mask;
      value[byte_index] = static_cast<char>(s);
    }
    return 0;
  }
}

rocksdb::Status BloomFilter::getSBChainMetadata(const Slice &ns_key, SBChainMetadata *metadata) {
  return Database::GetMetadata(kRedisBloomFilter, ns_key, metadata);
}

rocksdb::Status BloomFilter::getBFMetadata(const Slice &bf_meta_key, BFMetadata *metadata) {
  return Database::GetMetadata(kRedisBloomFilter, bf_meta_key, metadata);
}

rocksdb::Status BloomFilter::createSBChain(const Slice &ns_key, double error_rate, uint64_t capacity,
                                           uint16_t expansion, uint16_t scaling, SBChainMetadata &sb_chain_metadata) {
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
  bfInit(&bf_metadata, capacity, error_rate, scaling);
  std::string bf_meta_bytes;
  bf_metadata.Encode(&bf_meta_bytes);
  batch->Put(metadata_cf_handle_, bf_meta_key, bf_meta_bytes);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BloomFilter::createBloomFilter(const Slice &ns_key, SBChainMetadata &sb_chain_metadata,
                                               BFMetadata &bf_metadata, std::string &bf_key) {
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"createBloomFilter"});
  batch->PutLogData(log_data.Encode());

  BFMetadata old_bf_metadata = bf_metadata;
  sb_chain_metadata.n_filters += 1;
  appendBFSuffix(ns_key, sb_chain_metadata.n_filters - 1, &bf_key);
  std::string bf_meta_key;
  appendBFMetaSuffix(bf_key, &bf_meta_key);
  bfInit(&bf_metadata, sb_chain_metadata.growth * old_bf_metadata.entries,
         old_bf_metadata.error * kErrorTighteningRatio, sb_chain_metadata.options);
  std::string bf_meta_bytes;
  bf_metadata.Encode(&bf_meta_bytes);
  batch->Put(metadata_cf_handle_, bf_meta_key, bf_meta_bytes);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BloomFilter::bloomCheckAdd64(const Slice &bf_key, BFMetadata &bf_metadata, BloomHashval hashval,
                                             ReadWriteMode mode, ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                             int &ret) {
  int found_unset = 0;
  const uint64_t mod = (1LLU << bf_metadata.n2);
  for (uint64_t i = 0; i < bf_metadata.hashes; i++) {
    uint64_t x = ((hashval.a + i * hashval.b)) % mod;
    uint32_t index = (x / kBloomFilterSegmentBits) * kBloomFilterSegmentBytes;
    std::string sub_key, value;
    InternalKey(bf_key, std::to_string(index), bf_metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
    rocksdb::Status s =
        storage_->Get(rocksdb::ReadOptions(), sub_key, &value);  // todo: use bf_key in InternalKey maybe error
    if (!s.ok() && !s.IsNotFound()) return s;
    uint32_t byte_index = (x / 8) % kBloomFilterSegmentBytes;
    if (byte_index >= value.size()) {  // expand the bloomfilter
      size_t expand_size = 0;
      if (byte_index >= value.size() * 2) {
        expand_size = byte_index - value.size() + 1;
      } else if (value.size() * 2 > kBloomFilterSegmentBytes) {
        expand_size = kBloomFilterSegmentBytes - value.size();
      } else {
        expand_size = value.size();
      }
      value.append(expand_size, 0);
    }

    if (!testBitSetBit(value, x, mode)) {
      if (mode == ReadWriteMode::MODE_READ) {
        ret = 0;
        return rocksdb::Status::OK();
      } else {
        found_unset = 1;
        batch->Put(sub_key, value);
        storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
        // todo: if the indexs are same, the value need to be updated immediately, if have other ways to compl it
      }
    }
  }
  if (mode == ReadWriteMode::MODE_READ) {
    ret = 1;
  } else {
    ret = found_unset;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status BloomFilter::Reserve(const Slice &user_key, double error_rate, uint64_t capacity, uint16_t expansion,
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

rocksdb::Status BloomFilter::Add(const Slice &user_key, const Slice &item, int &ret) {
  std::vector<int> tmp(1, 0);
  rocksdb::Status s = MAdd(user_key, {item}, tmp);
  ret = tmp[0];
  return s;
}

rocksdb::Status BloomFilter::MAdd(const Slice &user_key, const std::vector<Slice> &items, std::vector<int> &rets) {
  BFInsertOptions insert_options = {.capacity = kBFDefaultInitCapacity,
                                    .error_rate = kBFDefaultErrorRate,
                                    .autocreate = 1,
                                    .expansion = kBFDefaultExpansion,
                                    .scaling = 1};
  return InsertCommon(user_key, items, &insert_options, rets);
}

rocksdb::Status BloomFilter::InsertCommon(const Slice &user_key, const std::vector<Slice> &items,
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
    const auto &item = items[i];
    BloomHashval h = bloomCalcHash64(item.data(), static_cast<int>(item.size()));

    // check
    for (int ii = sb_chain_metadata.n_filters - 1; ii >= 0; --ii) {
      std::string bf_key;
      appendBFSuffix(ns_key, ii, &bf_key);
      int ret = 0;
      s = bloomCheckAdd64(bf_key, bf_metadata_list[ii], h, ReadWriteMode::MODE_READ, batch, ret);
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
      s = bloomCheckAdd64(cur_bf_key, cur_bf_metadata, h, ReadWriteMode::MODE_WRITE, batch, ret);
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

rocksdb::Status BloomFilter::Exist(const Slice &user_key, const Slice &item, int &ret) {
  std::vector<int> tmp(1, 0);
  rocksdb::Status s = MExist(user_key, {item}, tmp);
  ret = tmp[0];
  return s;
}

rocksdb::Status BloomFilter::MExist(const Slice &user_key, const std::vector<Slice> &items, std::vector<int> &rets) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  SBChainMetadata sb_chain_metadata;
  rocksdb::Status s = getSBChainMetadata(ns_key, &sb_chain_metadata);
  if (!s.ok()) return s;

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

  for (size_t i = 0; i < items.size(); ++i) {
    const auto &item = items[i];
    BloomHashval h = bloomCalcHash64(item.data(), static_cast<int>(item.size()));
    // check
    auto batch = storage_->GetWriteBatchBase();  // todo: as placeholder, change the function to delete this
    for (int ii = sb_chain_metadata.n_filters - 1; ii >= 0; --ii) {
      std::string bf_key;
      appendBFSuffix(ns_key, ii, &bf_key);
      int ret = 0;
      s = bloomCheckAdd64(bf_key, bf_metadata_list[ii], h, ReadWriteMode::MODE_READ, batch, ret);
      if (ret == 1) {
        rets[i] = 1;
        break;
      }
    }
  }

  return rocksdb::Status::OK();
}

}  // namespace redis