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

#include "cms.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

#include "xxhash.h"

CMSketch::CMSketchDimensions CMSketch::CMSDimFromProb(double error, double delta) {
  CMSketchDimensions dims;
  dims.width = std::ceil(2 / error);
  dims.depth = std::ceil(std::log10(delta) / std::log10(0.5));
  return dims;
}

size_t CMSketch::IncrBy(std::string_view item, uint32_t value) {
  size_t min_count = std::numeric_limits<size_t>::max();

  for (size_t i = 0; i < depth_; ++i) {
    uint64_t hash = XXH32(item.data(), static_cast<int>(item.size()), i);
    size_t loc = GetLocationForHash(hash, i);
    if (array_[loc] > UINT32_MAX - value) {
      array_[loc] = UINT32_MAX;
    } else {
      array_[loc] += value;
    }
    min_count = std::min(min_count, static_cast<size_t>(array_[loc]));
  }
  counter_ += value;
  return min_count;
}

size_t CMSketch::Query(std::string_view item) const {
  size_t min_count = std::numeric_limits<size_t>::max();

  for (size_t i = 0; i < depth_; ++i) {
    uint64_t hash = XXH32(item.data(), static_cast<int>(item.size()), i);
    min_count = std::min(min_count, static_cast<size_t>(array_[GetLocationForHash(hash, i)]));
  }
  return min_count;
}

Status CMSketch::Merge(CMSketch* dest, size_t num_keys, std::vector<const CMSketch*> cms_array,
                       std::vector<uint32_t> weights) {
  // Perform overflow check
  if (CMSketch::CheckOverflow(dest, num_keys, cms_array, weights) != 0) {
    return {Status::NotOK, "Overflow error."};
  }

  size_t dest_depth = dest->GetDepth();
  size_t dest_width = dest->GetWidth();

  // Merge source CMSes into the destination CMS
  for (size_t i = 0; i < dest_depth; ++i) {
    for (size_t j = 0; j < dest_width; ++j) {
      int64_t item_count = 0;
      for (size_t k = 0; k < num_keys; ++k) {
        item_count += static_cast<int64_t>(cms_array[k]->array_[(i * dest_width) + j]) * weights[k];
      }
      dest->GetArray()[(i * dest_width) + j] += static_cast<uint32_t>(item_count);
    }
  }

  for (size_t i = 0; i < num_keys; ++i) {
    dest->GetCounter() += cms_array[i]->GetCounter() * weights[i];
  }

  return Status::OK();
}

int CMSketch::CheckOverflow(CMSketch* dest, size_t quantity, const std::vector<const CMSketch*>& src,
                            const std::vector<uint32_t>& weights) {
  int64_t item_count = 0;
  int64_t cms_count = 0;
  size_t width = dest->GetWidth();
  size_t depth = dest->GetDepth();

  for (size_t i = 0; i < depth; ++i) {
    for (size_t j = 0; j < width; ++j) {
      item_count = 0;
      for (size_t k = 0; k < quantity; ++k) {
        int64_t mul = 0;

        if (__builtin_mul_overflow(src[k]->GetArray()[(i * width) + j], weights[k], &mul) ||
            (__builtin_add_overflow(item_count, mul, &item_count))) {
          return -1;
        }
      }

      if (item_count < 0 || item_count > UINT32_MAX) {
        return -1;
      }
    }
  }

  for (size_t i = 0; i < quantity; ++i) {
    int64_t mul = 0;

    if (__builtin_mul_overflow(src[i]->GetCounter(), weights[i], &mul) ||
        (__builtin_add_overflow(cms_count, mul, &cms_count))) {
      return -1;
    }
  }

  if (cms_count < 0) {
    return -1;
  }

  return 0;
}
