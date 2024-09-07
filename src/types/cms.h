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

#include <vector>
#include "vendor/murmurhash2.h"

class CMSketch {
 public:
  explicit CMSketch(uint32_t width = 0, uint32_t depth = 0, uint64_t counter = 0, std::vector<uint32_t> array = {})
      : width_(width),
        depth_(depth),
        counter_(counter),
        array_(array.empty() ? std::vector<uint32_t>(width * depth, 0) : std::move(array)) {}

  ~CMSketch() = default;

  static CMSketch* NewCMSketch(size_t width, size_t depth) { return new CMSketch(width, depth); }

  static void CMSDimFromProb(double error, double delta, uint32_t& width, uint32_t& depth);

  size_t IncrBy(const char* item, size_t item_len, size_t value);

  size_t Query(const char* item, size_t item_len) const;

  static int Merge(CMSketch* dest, size_t quantity, const std::vector<const CMSketch*>& src,
                   const std::vector<long long>& weights);

  struct MergeParams {
    CMSketch* dest;
    size_t num_keys;
    std::vector<const CMSketch*> cms_array;
    std::vector<long long> weights;
  };

  int CMSMergeParams(const MergeParams& params);

  uint64_t& GetCounter() { return counter_; }
  std::vector<uint32_t>& GetArray() { return array_; }

  const uint64_t& GetCounter() const { return counter_; }
  const std::vector<uint32_t>& GetArray() const { return array_; }

  size_t GetWidth() const { return width_; }
  size_t GetDepth() const { return depth_; }

 private:
  size_t width_;
  size_t depth_;
  uint64_t counter_;
  std::vector<uint32_t> array_;

  static uint32_t hllMurMurHash64A(const char* item, size_t item_len, size_t i) {
    return HllMurMurHash64A(item, static_cast<int>(item_len), i);
  }

  static int checkOverflow(CMSketch* dest, size_t quantity, const std::vector<const CMSketch*>& src,
                           const std::vector<long long>& weights);
};