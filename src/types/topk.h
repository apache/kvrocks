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

#include <stdint.h>

#include <cmath>
#include <queue>
#include <string>
#include <vector>

static constexpr int TOPK_DECAY_LOOKUP_TABLE = 256;

using CounterT = uint32_t;

struct HeapBucket {
  uint32_t fp;
  uint32_t itemlen;
  char *item;
  CounterT count;
};

struct Bucket {
  uint32_t fp;
  CounterT count;
};

class BlockSplitTopK {
 public:
  BlockSplitTopK() = delete;
  BlockSplitTopK(const BlockSplitTopK &) = delete;
  BlockSplitTopK &operator=(const BlockSplitTopK &) = delete;
  BlockSplitTopK(BlockSplitTopK &&) = default;
  BlockSplitTopK &operator=(BlockSplitTopK &&) = default;

  explicit BlockSplitTopK(uint32_t k, uint32_t width, uint32_t depth, double decay)
      : k(k),
        width(width),
        depth(depth),
        decay(decay),
        heap_size(0),
        buckets(new Bucket[width * depth]),
        heap(new HeapBucket[k]) {
    std::fill_n(buckets, width * depth, Bucket{0, 0});
    std::fill_n(heap, k, HeapBucket{0, 0, nullptr, 0});
    for (int i = 0; i < TOPK_DECAY_LOOKUP_TABLE; ++i) {
      lookup_table[i] = pow(decay, i);
    }
  }

  ~BlockSplitTopK() {
    for (size_t i = 0; i < k; ++i) {
      delete[] heap[i].item;
    }
    delete[] buckets;
    delete[] heap;
  }

  void Add(const std::string &item, uint32_t increment);
  bool Query(const std::string &item) const;
  std::vector<HeapBucket> List();

  void HeapifyDown(int start) const;
  void HeapifyUp(int start) const;
  int CheckExistInHeap(const std::string &item) const;
  static int CmpHeapBucketCount(const HeapBucket &a, const HeapBucket &b);

  uint32_t k;
  uint32_t width;
  uint32_t depth;
  double decay;

  int heap_size;

  Bucket *buckets;
  HeapBucket *heap;
  double lookup_table[TOPK_DECAY_LOOKUP_TABLE];
};