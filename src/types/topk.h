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
#include <memory>
#include <queue>
#include <string>
#include <vector>

static constexpr int TOPK_DECAY_LOOKUP_TABLE = 256;

using CounterT = uint32_t;

struct HeapBucket {
  uint32_t fp;
  CounterT count;
  std::string item;

  HeapBucket() = default;

  HeapBucket(uint32_t fp, CounterT count, std::string item) : fp(fp), count(count), item(std::move(item)) {}

  HeapBucket(const HeapBucket &other) {
    if (this != &other) {
      fp = other.fp;
      count = other.count;
      item = other.item;
    }
  }

  HeapBucket(const HeapBucket &&other) noexcept {
    if (this != &other) {
      fp = other.fp;
      count = other.count;
      item = other.item;
    }
  }

  HeapBucket &operator=(const HeapBucket &other) {
    if (this != &other) {
      fp = other.fp;
      count = other.count;
      item = other.item;
    }
    return *this;
  }

  HeapBucket &operator=(const HeapBucket &&other) noexcept {
    if (this != &other) {
      fp = other.fp;
      count = other.count;
      item = other.item;
    }
    return *this;
  }

  ~HeapBucket() = default;
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
  BlockSplitTopK(BlockSplitTopK &&) = delete;
  BlockSplitTopK &operator=(BlockSplitTopK &&) = delete;

  explicit BlockSplitTopK(uint32_t k, uint32_t width, uint32_t depth, double decay)
      : k(k),
        width(width),
        depth(depth),
        decay(decay),
        heap_size(0),
        buckets(depth, std::vector<Bucket>(width, Bucket{0, 0})),
        heap(k, HeapBucket{0, 0, ""}) {
    for (int i = 0; i < TOPK_DECAY_LOOKUP_TABLE; ++i) {
      lookup_table[i] = pow(decay, i);
    }
  }

  ~BlockSplitTopK() = default;

  void Add(const std::string &item, uint32_t increment, std::vector<bool> &is_dirty_buckets,
           std::vector<bool> &is_dirty_heaps);
  bool Query(const std::string &item) const;
  std::vector<HeapBucket> List();

  void HeapifyDown(int start, std::vector<bool> &is_dirty_heaps);
  void HeapifyUp(int start, std::vector<bool> &is_dirty_heaps);
  int CheckExistInHeap(const std::string &item) const;
  static int CmpHeapBucketCount(const HeapBucket &a, const HeapBucket &b);

  uint32_t k;
  uint32_t width;
  uint32_t depth;
  double decay;

  int heap_size;

  std::vector<std::vector<Bucket>> buckets;
  std::vector<HeapBucket> heap;
  double lookup_table[TOPK_DECAY_LOOKUP_TABLE];
};