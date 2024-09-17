// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/*
This project uses Redis, which is licensed under your choice of the
Redis Source Available License 2.0 (RSALv2) or the Server Side Public License v1 (SSPLv1).

For more information on Redis licensing, visit:
https://redis.io/docs/about/license/
*/

#pragma once

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

struct SubCF {
    uint16_t bucket_size;
    uint64_t num_buckets;
    std::vector<uint8_t> data;
};

class CuckooFilter {
 public:

  CuckooFilter(uint64_t capacity, uint16_t bucket_size, uint16_t max_iterations, uint16_t expansion, uint64_t num_buckets, uint16_t num_filters, uint64_t num_items, uint64_t num_deletes, const std::vector<SubCF> &filters);

  ~CuckooFilter();

  CuckooFilter(const CuckooFilter &other);
  
  CuckooFilter &operator=(const CuckooFilter &other);
  
  CuckooFilter(CuckooFilter &&other) noexcept;
  
  CuckooFilter &operator=(CuckooFilter &&other) noexcept;

  enum CuckooInsertStatus { Inserted, NoSpace, MemAllocFailed, Exists };

  CuckooInsertStatus Insert(uint64_t hash);
  CuckooInsertStatus InsertUnique(uint64_t hash);
  bool Contains(uint64_t hash) const;
  bool Remove(uint64_t hash);
  uint64_t Count(uint64_t hash) const;
  void Compact(bool cont = true);
  bool ValidateIntegrity() const;

 private:
  uint64_t capacity_;
  uint16_t bucket_size_;
  uint16_t max_iterations_;  
  uint16_t expansion_;
  uint64_t num_buckets_;
  uint16_t num_filters_;     
  uint64_t num_items_;
  uint64_t num_deletes_;

  std::vector<SubCF> filters_;

  static bool isPowerOf2(uint64_t num);
  static uint64_t getNextPowerOf2(uint64_t n);
  static uint64_t getAltHash(uint8_t fingerprint, uint64_t index);
  static void getLookupParams(uint64_t hash, uint8_t &fingerprint, uint64_t &h1, uint64_t &h2);

  bool grow();
  bool filterFind(const SubCF &filter, uint8_t fingerprint, uint64_t h1, uint64_t h2) const;
  bool bucketDelete(std::vector<uint8_t>::iterator bucket_start, uint8_t fingerprint) const;
  bool filterDelete(SubCF &filter, uint8_t fingerprint, uint64_t h1, uint64_t h2);
  bool filterInsert(SubCF &filter, uint8_t fingerprint, uint64_t h1, uint64_t h2) const;
  CuckooInsertStatus filterKickoutInsert(SubCF &filter, uint8_t fingerprint, uint64_t h1) const;

  bool relocateSlot(std::vector<uint8_t> &bucket, uint16_t filter_index, uint64_t bucket_index, uint16_t slot_index);
  bool compactSingle(uint16_t filter_index);
};
