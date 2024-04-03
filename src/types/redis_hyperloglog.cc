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

/* Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis_hyperloglog.h"

#include <math.h>
#include <stdint.h>

#include "db_util.h"
#include "murmurhash2.h"

namespace redis {

/* Store the value of the register at position 'index' into variable 'val'.
 * 'registers' is an array of unsigned bytes. */
void HllDenseGetRegister(uint8_t *val, uint8_t *registers, uint32_t index) {
  uint32_t byte = index * kHyperLogLogBits / 8;
  uint8_t fb = index * kHyperLogLogBits & 7;
  uint8_t fb8 = 8 - fb;
  uint8_t b0 = registers[byte];
  uint8_t b1 = registers[byte + 1];
  *val = ((b0 >> fb) | (b1 << fb8)) & kHyperLogLogRegisterMax;
}

/* Set the value of the register at position 'index' to 'val'.
 * 'registers' is an array of unsigned bytes. */
void HllDenseSetRegister(uint8_t *registers, uint32_t index, uint8_t val) {
  uint32_t byte = index * kHyperLogLogBits / 8;
  uint8_t fb = index * kHyperLogLogBits & 7;
  uint8_t fb8 = 8 - fb;
  uint8_t v = val;
  registers[byte] &= ~(kHyperLogLogRegisterMax << fb);
  registers[byte] |= v << fb;
  registers[byte + 1] &= ~(kHyperLogLogRegisterMax >> fb8);
  registers[byte + 1] |= v >> fb8;
}

rocksdb::Status HyperLogLog::GetMetadata(const Slice &ns_key, HyperloglogMetadata *metadata) {
  return Database::GetMetadata({kRedisHyperLogLog}, ns_key, metadata);
}

/* the max 0 pattern counter of the subset the element belongs to is incremented if needed */
rocksdb::Status HyperLogLog::Add(const Slice &user_key, const std::vector<Slice> &elements, uint64_t *ret) {
  *ret = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HyperloglogMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHyperLogLog);
  batch->PutLogData(log_data.Encode());
  if (s.IsNotFound()) {
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }

  Bitmap::SegmentCacheStore cache(storage_, metadata_cf_handle_, ns_key, metadata);
  for (const auto &element : elements) {
    uint32_t register_index = 0;
    auto ele_str = element.ToString();
    std::vector<uint8_t> ele(ele_str.begin(), ele_str.end());
    uint8_t count = HllPatLen(ele, &register_index);
    uint32_t segment_index = register_index / kHyperLogLogRegisterCountPerSegment;
    uint32_t register_index_in_segment = register_index % kHyperLogLogRegisterCountPerSegment;

    std::string *segment = nullptr;
    auto s = cache.GetMut(segment_index, &segment);
    if (!s.ok()) return s;
    if (segment->size() == 0) {
      segment->resize(kHyperLogLogRegisterBytesPerSegment, 0);
    }

    uint8_t old_count = 0;
    HllDenseGetRegister(&old_count, reinterpret_cast<uint8_t *>(segment->data()), register_index_in_segment);
    if (count > old_count) {
      HllDenseSetRegister(reinterpret_cast<uint8_t *>(segment->data()), register_index_in_segment, count);
      *ret = 1;
    }
  }
  cache.BatchForFlush(batch);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status HyperLogLog::Count(const Slice &user_key, uint64_t *ret) {
  *ret = 0;
  std::vector<uint8_t> registers(kHyperLogLogRegisterBytes);
  auto s = getRegisters(user_key, &registers);
  if (!s.ok()) return s;
  *ret = HllCount(registers);
  return rocksdb::Status::OK();
}

rocksdb::Status HyperLogLog::Merge(const std::vector<Slice> &user_keys) {
  std::vector<uint8_t> max(kHyperLogLogRegisterBytes);
  for (const auto &user_key : user_keys) {
    std::vector<uint8_t> registers(kHyperLogLogRegisterBytes);
    auto s = getRegisters(user_key, &registers);
    if (!s.ok()) return s;
    HllMerge(&max, registers);
  }

  std::string ns_key = AppendNamespacePrefix(user_keys[0]);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HyperloglogMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHyperLogLog);
  batch->PutLogData(log_data.Encode());
  if (s.IsNotFound()) {
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }

  Bitmap::SegmentCacheStore cache(storage_, metadata_cf_handle_, ns_key, metadata);
  for (uint32_t segment_index = 0; segment_index < kHyperLogLogSegmentCount; segment_index++) {
    std::string registers(max.begin() + segment_index * kHyperLogLogRegisterBytesPerSegment,
                          max.begin() + (segment_index + 1) * kHyperLogLogRegisterBytesPerSegment);
    std::string *segment = nullptr;
    s = cache.GetMut(segment_index, &segment);
    if (!s.ok()) return s;
    if (segment->size() == 0) {
      *segment = registers;
    }
  }
  cache.BatchForFlush(batch);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

/* ========================= HyperLogLog algorithm  ========================= */

/* Given a string element to add to the HyperLogLog, returns the length
 * of the pattern 000..1 of the element hash. As a side effect 'regp' is
 * set to the register index this element hashes to. */
uint8_t HyperLogLog::HllPatLen(const std::vector<uint8_t> &element, uint32_t *register_index) {
  int elesize = static_cast<int>(element.size());
  uint64_t hash = 0, bit = 0, index = 0;
  int count = 0;

  /* Count the number of zeroes starting from bit kHyperLogLogRegisterCount
   * (that is a power of two corresponding to the first bit we don't use
   * as index). The max run can be 64-P+1 = Q+1 bits.
   *
   * Note that the final "1" ending the sequence of zeroes must be
   * included in the count, so if we find "001" the count is 3, and
   * the smallest count possible is no zeroes at all, just a 1 bit
   * at the first position, that is a count of 1.
   *
   * This may sound like inefficient, but actually in the average case
   * there are high probabilities to find a 1 after a few iterations. */
  hash = MurmurHash64A(element.data(), elesize, 0xadc83b19ULL);
  index = hash & kHyperLogLogRegisterCountMask;      /* Register index. */
  hash >>= kHyperLogLogRegisterCountPow;             /* Remove bits used to address the register. */
  hash |= ((uint64_t)1 << kHyperLogLogHashBitCount); /* Make sure the loop terminates
                                     and count will be <= Q+1. */
  bit = 1;
  count = 1; /* Initialized to 1 since we count the "00000...1" pattern. */
  while ((hash & bit) == 0) {
    count++;
    bit <<= 1;
  }
  *register_index = (int)index;
  return count;
}

/* Compute the register histogram in the dense representation. */
void HllDenseRegHisto(uint8_t *registers, int *reghisto) {
  for (uint32_t j = 0; j < kHyperLogLogRegisterCount; j++) {
    uint8_t reg = 0;
    HllDenseGetRegister(&reg, registers, j);
    reghisto[reg]++;
  }
}

/* ========================= HyperLogLog Count ==============================
 * This is the core of the algorithm where the approximated count is computed.
 * The function uses the lower level HllDenseRegHisto() and hllSparseRegHisto()
 * functions as helpers to compute histogram of register values part of the
 * computation, which is representation-specific, while all the rest is common. */

/* Helper function sigma as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double HllSigma(double x) {
  if (x == 1.) return INFINITY;
  double z_prime = NAN;
  double y = 1;
  double z = x;
  do {
    x *= x;
    z_prime = z;
    z += x * y;
    y += y;
  } while (z_prime != z);
  return z;
}

/* Helper function tau as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double HllTau(double x) {
  if (x == 0. || x == 1.) return 0.;
  double z_prime = NAN;
  double y = 1.0;
  double z = 1 - x;
  do {
    x = sqrt(x);
    z_prime = z;
    y *= 0.5;
    z -= pow(1 - x, 2) * y;
  } while (z_prime != z);
  return z / 3;
}

/* Return the approximated cardinality of the set based on the harmonic
 * mean of the registers values. */
uint64_t HyperLogLog::HllCount(const std::vector<uint8_t> &registers) {
  double m = kHyperLogLogRegisterCount;
  double e = NAN;
  int j = 0;
  /* Note that reghisto size could be just kHyperLogLogHashBitCount+2, because kHyperLogLogHashBitCount+1 is
   * the maximum frequency of the "000...1" sequence the hash function is
   * able to return. However it is slow to check for sanity of the
   * input: instead we history array at a safe size: overflows will
   * just write data to wrong, but correctly allocated, places. */
  int reghisto[64] = {0};

  /* Compute register histogram */
  HllDenseRegHisto((uint8_t *)(registers.data()), reghisto);

  /* Estimate cardinality from register histogram. See:
   * "New cardinality estimation algorithms for HyperLogLog sketches"
   * Otmar Ertl, arXiv:1702.01284 */
  double z = m * HllTau((m - reghisto[kHyperLogLogHashBitCount + 1]) / (double)m);
  for (j = kHyperLogLogHashBitCount; j >= 1; --j) {
    z += reghisto[j];
    z *= 0.5;
  }
  z += m * HllSigma(reghisto[0] / (double)m);
  e = static_cast<double>(llroundl(kHyperLogLogAlphaInf * m * m / z));

  return (uint64_t)e;
}

/* Merge by computing MAX(registers[i],hll[i]) the HyperLogLog 'hll'
 * with an array of uint8_t kHyperLogLogRegisterCount registers pointed by 'max'.
 *
 * The hll object must be already validated via isHLLObjectOrReply()
 * or in some other way. */
void HyperLogLog::HllMerge(std::vector<uint8_t> *registers_max, const std::vector<uint8_t> &registers) {
  uint8_t val = 0, max_val = 0;

  for (uint32_t i = 0; i < kHyperLogLogRegisterCount; i++) {
    HllDenseGetRegister(&val, (uint8_t *)(registers.data()), i);
    HllDenseGetRegister(&max_val, reinterpret_cast<uint8_t *>(registers_max->data()), i);
    if (val > *(registers_max->data() + i)) {
      HllDenseSetRegister(reinterpret_cast<uint8_t *>(registers_max->data()), i, val);
    }
  }
}

rocksdb::Status HyperLogLog::getRegisters(const Slice &user_key, std::vector<uint8_t> *registers) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  HyperloglogMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());

    int register_index = std::stoi(ikey.GetSubKey().ToString());
    auto val = iter->value().ToString();
    if (val.size() != kHyperLogLogRegisterBytesPerSegment) {
      return rocksdb::Status::Corruption("Value size must be kHyperLogLogRegisterBytesPerSegment");
    }
    auto register_byte_offset = register_index / 8 * kHyperLogLogBits;
    std::copy(val.begin(), val.end(), registers->data() + register_byte_offset);
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
