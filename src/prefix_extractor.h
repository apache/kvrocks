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

#include <rocksdb/slice_transform.h>

class SubkeyPrefixTransform : public rocksdb::SliceTransform {
 private:
  bool cluster_enabled_;
  uint8_t prefix_base_len_;

 public:
  explicit SubkeyPrefixTransform(bool cluster_enabled) : cluster_enabled_(cluster_enabled) {
    // Subkey format:
    // 1(namespace_len) + N(namespace) + 2(slot_id) + 4(user_key_len) + N(user_key) + 8(version) + N(field) + ..
    // If cluster_enabled is true, length of Subkey is not smaller than 15
    // If cluster_enabled is false, length of Subkey is not smaller than 13
    prefix_base_len_ = cluster_enabled ? 15 : 13;
  }

  const char* Name() const override { return "Kvrocks.SubkeyPrefix"; }

  rocksdb::Slice Transform(const rocksdb::Slice& src) const override {
    assert(InDomain(src));
    size_t prefix_len = GetPrefixLen(src);
    return rocksdb::Slice(src.data(), prefix_len);
  }

  bool InDomain(const rocksdb::Slice& src) const override {
    return (src.size() >= prefix_base_len_);
  }

  size_t GetPrefixLen(const rocksdb::Slice& input) const;
};

extern const rocksdb::SliceTransform* NewSubkeyPrefixTransform(bool cluster_enabled);
