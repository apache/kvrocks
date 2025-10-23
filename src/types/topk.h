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
#include <vector>
#include <queue>
#include <cmath>
#include <string>

static constexpr int TOPK_DECAY_LOOKUP_TABLE = 256;

using counter_t = uint32_t;

struct HeapBucket {
    uint32_t fp;
    uint32_t itemlen;
    char* item;
    counter_t count;

    HeapBucket& operator=(const HeapBucket& other) {
        if (this != &other) {
            fp = other.fp;
            itemlen = other.itemlen;
            item = other.item;
            count = other.count;
        }
        return *this;
    }
};

struct Bucket {
    uint32_t fp;
    counter_t count;
};

class BlockSplitTopK {
public:
    explicit BlockSplitTopK(uint32_t k, uint32_t width, uint32_t depth, double decay) :
        k(k), width(width), depth(depth), decay(decay), heap_size(0) {
        buckets.resize(depth, std::vector<Bucket>(width, Bucket{0, 0}));
        heap.resize(k, HeapBucket{0, 0, nullptr, 0});
        for (int i = 0; i < TOPK_DECAY_LOOKUP_TABLE; ++i) {
            lookupTable[i] = pow(decay, i);
        }
    }

    void TopkAdd(const std::string &item, uint32_t increment);
    bool TopkQuery(const std::string &item);
    std::vector<HeapBucket> TopkList();

    std::string_view GetData() const;
private:
    void heapifyDown(int start);
    void heapifyUp(int start);
    int checkExistInHeap(const std::string &item);
    int cmpHeapBucketCount(const HeapBucket& a, const HeapBucket& b);
    bool cmpHeapBucketItem(const HeapBucket& a, const HeapBucket& b);
    void swapHeapBucket(HeapBucket& a, HeapBucket& b);

    uint32_t k;
    uint32_t width;
    uint32_t depth;
    double decay;

    size_t heap_size;

    std::vector<std::vector<Bucket>> buckets;
    std::vector<HeapBucket> heap;
    double lookupTable[TOPK_DECAY_LOOKUP_TABLE];
};

BlockSplitTopK CreateBlockSplitTopK(uint32_t k, uint32_t width, uint32_t depth, double decay);