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

#include <rocksdb/status.h>

#include <random>
#include <vector>

/// ExtractRandMemberFromSet is a helper function to extract random elements from a kvrocks structure.
///
/// The complexity of the function is O(N) where N is the number of elements inside the structure.
template <typename ElementType, typename GetAllMemberFnType>
rocksdb::Status ExtractRandMemberFromSet(bool unique, size_t count, const GetAllMemberFnType &get_all_member_fn,
                                         std::vector<ElementType> *elements) {
  elements->clear();
  std::vector<ElementType> samples;
  rocksdb::Status s = get_all_member_fn(&samples);
  if (!s.ok() || samples.empty()) return s;

  size_t all_element_size = samples.size();
  DCHECK_GE(all_element_size, 1U);
  elements->reserve(std::min(all_element_size, count));

  if (!unique || count == 1) {
    // Case 1: Negative count, randomly select elements or without parameter
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dist(0, all_element_size - 1);
    for (uint64_t i = 0; i < count; i++) {
      uint64_t index = dist(gen);
      elements->emplace_back(samples[index]);
    }
  } else if (all_element_size <= count) {
    // Case 2: Requested count is greater than or equal to the number of elements inside the structure
    for (auto &sample : samples) {
      elements->push_back(std::move(sample));
    }
  } else {
    // Case 3: Requested count is less than the number of elements inside the structure
    std::mt19937 gen(std::random_device{}());
    // use Fisher-Yates shuffle algorithm to randomize the order
    std::shuffle(samples.begin(), samples.end(), gen);
    // then pick the first `count` ones.
    for (uint64_t i = 0; i < count; i++) {
      elements->emplace_back(std::move(samples[i]));
    }
  }
  return rocksdb::Status::OK();
}
