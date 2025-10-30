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

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <algorithm>
#include <memory>

#define TOPK_HASH(item, itemlen, i) MurmurHash2(item, itemlen, i)
#define GA 1919

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
#define BIG_CONSTANT(x) (x##LLU)

//-----------------------------------------------------------------------------

static uint32_t MurmurHash2(const void *key, int len, uint32_t seed) {
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.

    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    // Initialize the hash to a 'random' value

    uint32_t h = seed ^ len;

    // Mix 4 bytes at a time into the hash

    const unsigned char *data = (const unsigned char *)key;

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

//-----------------------------------------------------------------------------
// MurmurHash2, 64-bit versions, by Austin Appleby

// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment
// and endian-ness issues if used across multiple platforms.

// 64-bit hash for 64-bit platforms

[[maybe_unused]]static uint64_t MurmurHash64A_Bloom(const void *key, int len, uint64_t seed) {
    const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
    const int r = 47;

    uint64_t h = seed ^ (len * m);

    const uint64_t *data = (const uint64_t *)key;
    const uint64_t *end = data + (len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *)data;

    switch (len & 7) {
    case 7:
        h ^= ((uint64_t)data2[6]) << 48;
    case 6:
        h ^= ((uint64_t)data2[5]) << 40;
    case 5:
        h ^= ((uint64_t)data2[4]) << 32;
    case 4:
        h ^= ((uint64_t)data2[3]) << 24;
    case 3:
        h ^= ((uint64_t)data2[2]) << 16;
    case 2:
        h ^= ((uint64_t)data2[1]) << 8;
    case 1:
        h ^= ((uint64_t)data2[0]);
        h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

// 64-bit hash for 32-bit platforms

[[maybe_unused]]static uint64_t MurmurHash64B(const void *key, int len, uint64_t seed) {
    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    uint32_t h1 = (uint32_t)(seed ^ len);
    uint32_t h2 = (uint32_t)(seed >> 32);

    const uint32_t *data = (const uint32_t *)key;

    while (len >= 8) {
        uint32_t k1 = *data++;
        k1 *= m;
        k1 ^= k1 >> r;
        k1 *= m;
        h1 *= m;
        h1 ^= k1;
        len -= 4;

        uint32_t k2 = *data++;
        k2 *= m;
        k2 ^= k2 >> r;
        k2 *= m;
        h2 *= m;
        h2 ^= k2;
        len -= 4;
    }

    if (len >= 4) {
        uint32_t k1 = *data++;
        k1 *= m;
        k1 ^= k1 >> r;
        k1 *= m;
        h1 *= m;
        h1 ^= k1;
        len -= 4;
    }

    switch (len) {
    case 3:
        h2 ^= ((unsigned char *)data)[2] << 16;
    case 2:
        h2 ^= ((unsigned char *)data)[1] << 8;
    case 1:
        h2 ^= ((unsigned char *)data)[0];
        h2 *= m;
    };

    h1 ^= h2 >> 18;
    h1 *= m;
    h2 ^= h1 >> 22;
    h2 *= m;
    h1 ^= h2 >> 17;
    h1 *= m;
    h2 ^= h1 >> 19;
    h2 *= m;

    uint64_t h = h1;

    h = (h << 32) | h2;

    return h;
}

/* ---------------------------------------------------------------------- */
void BlockSplitTopK::heapifyDown(int start) {
    size_t child = start;

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

void BlockSplitTopK::heapifyUp(int start) {
    size_t parent = start;

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

int BlockSplitTopK::checkExistInHeap(const std::string &item) {
    uint32_t itemlen = item.size();
    const char *data = item.c_str();
    for (int32_t i = heap_size - 1; i >= 0; --i) {
        if (heap[i].itemlen == itemlen && memcmp(heap[i].item, data, itemlen) == 0) {
            return i;
        }
    }
    return -1;
}

int BlockSplitTopK::cmpHeapBucketCount(const HeapBucket &a, const HeapBucket &b) {
    return a.count < b.count ? 1 : a.count > b.count ? -1 : 0;
}

void BlockSplitTopK::Add(const std::string &item, uint32_t increment) {
    uint32_t itemlen = item.size();
    const char *data = item.c_str();
    counter_t maxCount = 0;
    uint32_t fp = TOPK_HASH(data, itemlen, GA);

    int location = checkExistInHeap(item);

    for (size_t i = 0; i < depth; ++i) {
        uint32_t loc = TOPK_HASH(data, itemlen, i) % width;

        loc += i * width;
        if (buckets[loc].count == 0) {
            buckets[loc].fp = fp;
            buckets[loc].count = increment;
            maxCount = std::max(maxCount, buckets[loc].count);
        } else if (buckets[loc].fp == fp && location != -1) {
            buckets[loc].count += increment;
            maxCount = std::max(maxCount, buckets[loc].count);
        } else {
            // decay
            uint32_t local_incr = increment;
            for (; local_incr > 0; --local_incr) {
                double decay;
                if (buckets[loc].count < TOPK_DECAY_LOOKUP_TABLE) {
                    decay = lookupTable[buckets[loc].count];
                } else {
                    decay = pow(lookupTable[TOPK_DECAY_LOOKUP_TABLE - 1], 
                                (buckets[loc].count / (TOPK_DECAY_LOOKUP_TABLE - 1))) *
                            lookupTable[buckets[loc].count % (TOPK_DECAY_LOOKUP_TABLE - 1)];
                }
                double chance = rand() / (double)RAND_MAX;
                if (chance < decay) {
                    -- buckets[loc].count;
                    if (buckets[loc].count == 0) {
                        buckets[loc].fp = fp;
                        buckets[loc].count = 1;
                        maxCount = std::max(maxCount, buckets[loc].count);
                        break;
                    }
                }
            }
        }
    }

    if (k == heap_size) {
        if (location == -1) {
            if (heap[0].count == maxCount || heap[0].count + 1 == maxCount) {
                heap[0].fp = fp;
                heap[0].itemlen = itemlen;
                delete heap[0].item;
                heap[0].item = new char[itemlen];
                memcpy(heap[0].item, data, itemlen);
                
                heap[0].count = maxCount;

                heapifyDown(0);
            }
        } else {
            heap[location].count += increment;
            heapifyDown(location);
        }
    } else {
        heap[heap_size].fp = fp;
        heap[heap_size].itemlen = itemlen;
        heap[heap_size].item = new char[itemlen];
        memcpy(heap[heap_size].item, data, itemlen);
        heap[heap_size].count = maxCount;

        heapifyUp(heap_size);
        heap_size ++;
    }
}

bool BlockSplitTopK::Query(const std::string &item) {
    return checkExistInHeap(item) != -1;
}

std::vector<HeapBucket> BlockSplitTopK::List() {
    std::vector<HeapBucket> result(heap_size);
    for (uint32_t i = 0; i < heap_size; i ++) {
        result[i] = heap[i];
    }
    std::sort(result.begin(), result.end(), [this] (const HeapBucket &a, const HeapBucket &b) {
        return cmpHeapBucketCount(a, b) > 0;
    });
    return result;
}