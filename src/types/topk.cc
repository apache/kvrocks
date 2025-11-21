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

#include "topk.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>

//-----------------------------------------------------------------------------
// MurmurHash2 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

// Note - This code makes a few assumptions about how your machine behaves -

// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4

// And it has a few limitations -

// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.

//-----------------------------------------------------------------------------

static uint32_t MurmurHash2(const void *key, int len, uint32_t seed) {
  // 'm' and 'r' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.

  const uint32_t m = 0x5bd1e995;
  const int r = 24;

  // Initialize the hash to a 'random' value

  uint32_t h = seed ^ len;

  // Mix 4 bytes at a time into the hash

  auto *data = reinterpret_cast<const unsigned char *>(key);

  while (len >= 4) {
    uint32_t k = *(uint32_t *)data;

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  // Handle the last few bytes of the input heap

  switch (len) {
    case 3:
      h ^= data[2] << 16;
    case 2:
      h ^= data[1] << 8;
    case 1:
      h ^= data[0];
      h *= m;
  };

  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

static uint32_t TopkHash(const void *item, int itemlen, uint32_t i) { return MurmurHash2(item, itemlen, i); }
constexpr uint32_t GA = 1919;

/* ---------------------------------------------------------------------- */
void BlockSplitTopK::HeapifyDown(int start) const {
  int child = start;

  // check whether larger than children
  if (heap_size < 2 || (heap_size - 2) / 2 < child) {
    return;
  }

  child = 2 * child + 1;
  if ((child + 1) < heap_size && (heap[child].count > heap[child + 1].count)) {
    ++child;
  }
  if (heap[child].count > heap[start].count) {
    return;
  }

  HeapBucket top;
  memcpy(&top, &heap[start], sizeof(HeapBucket));
  do {
    memcpy(&heap[start], &heap[child], sizeof(HeapBucket));
    start = child;

    if ((heap_size - 2) / 2 < child) {
      break;
    }
    child = 2 * child + 1;

    if ((child + 1) < heap_size && (heap[child].count > heap[child + 1].count)) {
      ++child;
    }
  } while (heap[child].count < top.count);
  memcpy(&heap[start], &top, sizeof(HeapBucket));
}

void BlockSplitTopK::HeapifyUp(int start) const {
  int parent = start;

  // check whether smaller than parent
  if (heap_size < 2 || parent == 0) {
    return;
  }

  parent = (parent - 1) / 2;
  if (heap[parent].count > heap[start].count) {
    return;
  }

  HeapBucket bottom;
  memcpy(&bottom, &heap[start], sizeof(HeapBucket));
  do {
    memcpy(&heap[start], &heap[parent], sizeof(HeapBucket));
    start = parent;

    if (start == 0) {
      break;
    }
    parent = (parent - 1) / 2;
  } while (heap[parent].count > bottom.count);
  memcpy(&heap[start], &bottom, sizeof(HeapBucket));
}

int BlockSplitTopK::CheckExistInHeap(const std::string &item) const {
  uint32_t itemlen = item.size();
  const char *data = item.c_str();
  for (int i = heap_size - 1; i >= 0; --i) {
    if (heap[i].itemlen == itemlen && memcmp(heap[i].item, data, itemlen) == 0) {
      return i;
    }
  }
  return -1;
}

int BlockSplitTopK::CmpHeapBucketCount(const HeapBucket &a, const HeapBucket &b) {
  return a.count < b.count ? 1 : a.count > b.count ? -1 : 0;
}

void BlockSplitTopK::Add(const std::string &item, uint32_t increment) {
  uint32_t itemlen = item.size();
  const char *data = item.c_str();
  CounterT max_count = 0;
  uint32_t fp = TopkHash(data, (int)itemlen, GA);

  int location = CheckExistInHeap(item);

  for (size_t i = 0; i < depth; ++i) {
    uint32_t loc = TopkHash(data, (int)itemlen, i) % width;

    loc += i * width;
    if (buckets[loc].count == 0) {
      buckets[loc].fp = fp;
      buckets[loc].count = increment;
      max_count = std::max(max_count, buckets[loc].count);
    } else if (buckets[loc].fp == fp && location != -1) {
      buckets[loc].count += increment;
      max_count = std::max(max_count, buckets[loc].count);
    } else {
      // decay
      uint32_t local_incr = increment;
      for (; local_incr > 0; --local_incr) {
        double decay = 0.0;
        if (buckets[loc].count < TOPK_DECAY_LOOKUP_TABLE) {
          decay = lookup_table[buckets[loc].count];
        } else {
          decay = pow(lookup_table[TOPK_DECAY_LOOKUP_TABLE - 1], (buckets[loc].count / (TOPK_DECAY_LOOKUP_TABLE - 1))) *
                  lookup_table[buckets[loc].count % (TOPK_DECAY_LOOKUP_TABLE - 1)];
        }
        double chance = rand() / (double)RAND_MAX;
        if (chance < decay) {
          --buckets[loc].count;
          if (buckets[loc].count == 0) {
            buckets[loc].fp = fp;
            buckets[loc].count = 1;
            max_count = std::max(max_count, buckets[loc].count);
            break;
          }
        }
      }
    }
  }

  if (k == (uint32_t)heap_size) {
    if (location == -1) {
      if (heap[0].count == max_count || heap[0].count + 1 == max_count) {
        heap[0].fp = fp;
        heap[0].itemlen = itemlen;
        delete heap[0].item;
        heap[0].item = new char[itemlen];
        memcpy(heap[0].item, data, itemlen);

        heap[0].count = max_count;

        HeapifyDown(0);
      }
    } else {
      heap[location].count += increment;
      HeapifyDown(location);
    }
  } else {
    heap[heap_size].fp = fp;
    heap[heap_size].itemlen = itemlen;
    heap[heap_size].item = new char[itemlen];
    memcpy(heap[heap_size].item, data, itemlen);
    heap[heap_size].count = max_count;

    HeapifyUp((int)heap_size);
    heap_size++;
  }
}

bool BlockSplitTopK::Query(const std::string &item) const { return CheckExistInHeap(item) != -1; }

std::vector<HeapBucket> BlockSplitTopK::List() {
  std::vector<HeapBucket> result(heap_size);
  for (int i = 0; i < heap_size; i++) {
    result[i] = heap[i];
  }
  std::sort(result.begin(), result.end(),
            [this](const HeapBucket &a, const HeapBucket &b) { return CmpHeapBucketCount(a, b) > 0; });
  return result;
}