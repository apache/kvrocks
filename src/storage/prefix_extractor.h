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

#include "common/encoding.h"

class SubkeyPrefixExtractor : public rocksdb::SliceTransform {
 private:
  bool cluster_enabled_;

 public:
  explicit SubkeyPrefixExtractor(bool cluster_enabled) : cluster_enabled_(cluster_enabled) {}

  const char *Name() const override { return "sub_key_prefix_extractor"; }

  rocksdb::Slice Transform(const rocksdb::Slice &key) const override {
    const char *ptr = key.data();
    uint8_t ns_size = ptr[0];
    ptr += 1 + ns_size;  // drop ns encoding size and ns
    if (cluster_enabled_) {
      // add the cluster slot size if it's enabled
      ptr += 2;
    }

    // return the origin key if it's too short
    if (ptr > key.data() + key.size()) return key;
    uint32_t key_size = DecodeFixed32(ptr);

    ptr += 4 + key_size;  // drop key encoding size and key
    // return the origin key if it's too short
    if (ptr > key.data() + key.size()) return key;

    return {key.data(), ptr - key.data()};
  }

  bool InDomain(const rocksdb::Slice &key) const override {
    size_t min_key_length = 1 /* ns encoding size */ + 4 /* encoding key size */ + 8 /* version */;
    if (cluster_enabled_) {
      min_key_length += 2;
    }
    return key.size() >= min_key_length;
  }
};
