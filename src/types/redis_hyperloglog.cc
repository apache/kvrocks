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

#include "redis_hyperloglog.h"

#include <db_util.h>
#include <stdint.h>

namespace redis {

rocksdb::Status HyperLogLog::GetMetadata(Database::GetOptions get_options, const Slice &ns_key,
                                         HyperloglogMetadata *metadata) {
  return Database::GetMetadata(get_options, {kRedisHyperLogLog}, ns_key, metadata);
}

/* the max 0 pattern counter of the subset the element belongs to is incremented if needed */
rocksdb::Status HyperLogLog::Add(const Slice &user_key, const std::vector<Slice> &elements, uint64_t *ret) {
  *ret = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HyperloglogMetadata metadata;
  rocksdb::Status s = GetMetadata(GetOptions(), ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHyperLogLog);
  batch->PutLogData(log_data.Encode());

  Bitmap::SegmentCacheStore cache(storage_, metadata_cf_handle_, ns_key, &metadata);
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
  rocksdb::Status s = GetMetadata(GetOptions(), ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHyperLogLog);
  batch->PutLogData(log_data.Encode());

  Bitmap::SegmentCacheStore cache(storage_, metadata_cf_handle_, ns_key, &metadata);
  for (uint32_t segment_index = 0; segment_index < kHyperLogLogSegmentCount; segment_index++) {
    std::string *segment = nullptr;
    s = cache.GetMut(segment_index, &segment);
    if (!s.ok()) return s;
    (*segment).assign(reinterpret_cast<const char *>(max.data()) + segment_index * kHyperLogLogRegisterBytesPerSegment,
                      kHyperLogLogRegisterBytesPerSegment);
  }
  cache.BatchForFlush(batch);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status HyperLogLog::getRegisters(const Slice &user_key, std::vector<uint8_t> *registers) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  HyperloglogMetadata metadata;
  LatestSnapShot ss(storage_);

  rocksdb::Status s = GetMetadata(Database::GetOptions{ss.GetSnapShot()}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());

    int register_index = std::stoi(ikey.GetSubKey().ToString());
    if (register_index / kHyperLogLogRegisterCountPerSegment < 0 ||
        register_index / kHyperLogLogRegisterCountPerSegment >= kHyperLogLogSegmentCount ||
        register_index % kHyperLogLogRegisterCountPerSegment != 0) {
      return rocksdb::Status::Corruption("invalid subkey index: idx=" + ikey.GetSubKey().ToString());
    }
    auto val = iter->value().ToString();
    if (val.size() != kHyperLogLogRegisterBytesPerSegment) {
      return rocksdb::Status::Corruption(
          "insufficient length subkey value size: expect=" + std::to_string(kHyperLogLogRegisterBytesPerSegment) +
          ", actual=" + std::to_string(val.size()));
    }

    auto register_byte_offset = register_index / 8 * kHyperLogLogBits;
    std::copy(val.begin(), val.end(), registers->data() + register_byte_offset);
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
