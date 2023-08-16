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

#pragma once

#include "bloom_filter.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

const char kBloomFilterSeparator[] = ":";
const uint32_t kBloomFilterSegmentBits = 1024 * 8;
const uint32_t kBloomFilterSegmentBytes = 1024;
const uint64_t kBFDefaultInitCapacity = 100;
const double kBFDefaultErrorRate = 0.01;
const uint16_t kBFDefaultExpansion = 2;
const double kErrorTighteningRatio = 0.5;

struct BFInsertOptions {
  uint64_t capacity;
  double error_rate;
  uint16_t autocreate;
  //  int must_exist;
  //  int is_multi;
  uint16_t expansion;
  uint16_t scaling;
};

enum class ReadWriteMode {
  MODE_READ = 0,
  MODE_WRITE = 1,
};

class SBChain : public Database {
 public:
  SBChain(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Reserve(const Slice &user_key, double error_rate, uint32_t capacity, uint16_t expansion,
                          uint16_t scaling);
  rocksdb::Status Add(const Slice &user_key, const Slice &item, int &ret);
  rocksdb::Status MAdd(const Slice &user_key, const std::vector<Slice> &items, std::vector<int> &rets);
  rocksdb::Status InsertCommon(const Slice &user_key, const std::vector<Slice> &items,
                               const BFInsertOptions *insert_options, std::vector<int> &rets);
  rocksdb::Status Exist(const Slice &user_key, const Slice &item, int &ret);
  rocksdb::Status MExist(const Slice &user_key, const std::vector<Slice> &items, std::vector<int> &rets);

 private:
  rocksdb::Status getSBChainMetadata(const Slice &ns_key, SBChainMetadata *metadata);
  rocksdb::Status getBFMetadata(const Slice &bf_meta_key,
                                BFMetadata *metadata);  // todo: where bfmeta should be placed ?
  rocksdb::Status createSBChain(const Slice &ns_key, double error_rate, uint32_t capacity, uint16_t expansion,
                                uint16_t scaling, SBChainMetadata &sb_chain_metadata);
  rocksdb::Status createBloomFilter(const Slice &ns_key, SBChainMetadata &sb_chain_metadata, BFMetadata &bf_metadata,
                                    std::string &bf_key);
  rocksdb::Status bloomCheckAdd64(const Slice &bf_key, BFMetadata &bf_metadata, const std::string &item,
                                  ReadWriteMode mode, ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch, int &ret);

  static void appendBFSuffix(const Slice &ns_key, uint16_t filters_index, std::string *output);
  static void appendBFMetaSuffix(const Slice &bf_key, std::string *output);
  static void bfInit(BFMetadata *bf_metadata, std::string *bf_data,uint32_t entries, double error);
};
}  // namespace redis