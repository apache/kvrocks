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
const uint32_t kBFDefaultInitCapacity = 100;
const double kBFDefaultErrorRate = 0.01;
const uint16_t kBFDefaultExpansion = 2;
const double kErrorTighteningRatio = 0.5;

enum class ReadWriteMode {
  READ = 0,
  WRITE = 1,
};

class SBChain : public Database {
 public:
  SBChain(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Reserve(const Slice &user_key, uint32_t capacity, double error_rate, uint16_t expansion,
                          uint16_t scaling);
  rocksdb::Status Add(const Slice &user_key, const Slice &item, int &ret);
  rocksdb::Status Exist(const Slice &user_key, const Slice &item, int &ret);

 private:
  std::string getBFKey(const Slice &ns_key, const SBChainMetadata &metadata, uint16_t filters_index);
  std::string getBFMetaKey(const Slice &ns_key, const SBChainMetadata &metadata, uint16_t filters_index);
  rocksdb::Status getSBChainMetadata(const Slice &ns_key, SBChainMetadata *metadata);
  rocksdb::Status getBFMetadata(const Slice &bf_meta_key, BFMetadata *metadata);
  rocksdb::Status createSBChain(const Slice &ns_key, double error_rate, uint32_t capacity, uint16_t expansion,
                                uint16_t scaling, SBChainMetadata &sb_chain_metadata);
  rocksdb::Status bloomCheckAdd(const Slice &bf_key, const std::string &item, ReadWriteMode mode, int &ret);

  static void bfInit(BFMetadata *bf_metadata, std::string *bf_data, uint32_t entries, double error);
};
}  // namespace redis
