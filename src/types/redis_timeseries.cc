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

#include "redis_timeseries.h"

void TSDownStreamMeta::Encode(std::string *dst) const {
  PutFixed8(dst, static_cast<uint8_t>(aggregator));
  PutFixed64(dst, bucket_duration);
  PutFixed64(dst, alignment);
  PutFixed64(dst, latest_bucket_idx);
  PutFixed8(dst, static_cast<uint8_t>(u64_auxs.size()));
  for (const auto &aux : u64_auxs) {
    PutFixed64(dst, aux);
  }
  for (const auto &aux : f64_auxs) {
    PutDouble(dst, aux);
  }
}

rocksdb::Status TSDownStreamMeta::Decode(Slice *input) {
  if (input->size() < sizeof(uint8_t) + sizeof(uint64_t) * 3) {
    return rocksdb::Status::InvalidArgument("TSDownStreamMeta size is too short");
  }

  GetFixed8(input, reinterpret_cast<uint8_t *>(&aggregator));
  GetFixed64(input, &bucket_duration);
  GetFixed64(input, &alignment);
  GetFixed64(input, &latest_bucket_idx);
  uint8_t u64_auxs_size;
  GetFixed8(input, &u64_auxs_size);

  if (input->size() < sizeof(uint64_t) * u64_auxs_size || input->size() % sizeof(uint64_t)) {
    return rocksdb::Status::InvalidArgument("Invalid auxinfo size");
  }

  for (uint8_t i = 0; i < u64_auxs_size; i++) {
    uint64_t aux;
    GetFixed64(input, &aux);
    u64_auxs.push_back(std::move(aux));
  }
  while (input->size() > 0) {
    double aux;
    if (!GetDouble(input, &aux)) {
      return rocksdb::Status::InvalidArgument("Invalid auxinfo size");
    }
    f64_auxs.push_back(std::move(aux));
  }

  return rocksdb::Status::OK();
}

namespace redis {
TSRevLabelKey::TSRevLabelKey(Slice ns_key, Slice label_key, Slice label_value, bool slot_id_encoded)
    : label_key(label_key), label_value(label_value) {
  uint8_t namespace_size = 0;
  GetFixed8(&ns_key, &namespace_size);
  ns = Slice(ns_key.data(), namespace_size);
  ns_key.remove_prefix(namespace_size);

  slot_id_encoded_ = slot_id_encoded;
  if (slot_id_encoded_) {
    GetFixed16(&ns_key, &slot_id);
  }
  user_key = ns_key;
}

std::string TSRevLabelKey::Encode() const {
  std::string encoded;
  size_t total = 1 + ns.size() + 4 + label_key.size() + 4 + label_value.size() + user_key.size();
  if (slot_id_encoded_) {
    total += 2;
  }
  encoded.resize(total);
  auto buf = encoded.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(ns.size()));
  buf = EncodeBuffer(buf, ns);
  if (slot_id_encoded_) {
    buf = EncodeFixed16(buf, slot_id);
  }
  buf = EncodeFixed32(buf, static_cast<uint32_t>(label_key.size()));
  buf = EncodeBuffer(buf, label_key);
  buf = EncodeFixed32(buf, static_cast<uint32_t>(label_value.size()));
  buf = EncodeBuffer(buf, label_value);
  buf = EncodeBuffer(buf, user_key);

  return encoded;
}

std::string TimeSeries::internalKeyFromChunkID(const std::string &ns_key, const TimeSeriesMetadata &metadata,
                                               uint64_t id) const {
  std::string sub_key;
  PutFixed8(&sub_key, static_cast<uint8_t>(TSubkeyType::CHUNK));
  PutFixed64(&sub_key, id);

  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TimeSeries::internalKeyFromLabelKey(const std::string &ns_key, const TimeSeriesMetadata &metadata,
                                                Slice label_key) const {
  std::string sub_key;
  sub_key.resize(1 + label_key.size());
  auto buf = sub_key.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(TSubkeyType::LABEL));
  buf = EncodeBuffer(buf, label_key);

  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TimeSeries::internalKeyFromDownstreamKey(const std::string &ns_key, const TimeSeriesMetadata &metadata,
                                                     Slice downstream_key) const {
  std::string sub_key;
  sub_key.resize(1 + downstream_key.size());
  auto buf = sub_key.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(TSubkeyType::DOWNSTREAM));
  buf = EncodeBuffer(buf, downstream_key);

  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

}  // namespace redis