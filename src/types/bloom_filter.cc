// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "bloom_filter.h"

#include <cstdint>
#include <memory>

#include "xxh3.h"

OwnedBlockSplitBloomFilter CreateBlockSplitBloomFilter(uint32_t num_bytes) {
  if (num_bytes < kMinimumBloomFilterBytes) {
    num_bytes = kMinimumBloomFilterBytes;
  }

  // Get next power of 2 if it is not power of 2.
  if ((num_bytes & (num_bytes - 1)) != 0) {
    num_bytes = static_cast<uint32_t>(NextPower2(num_bytes));
  }

  if (num_bytes > kMaximumBloomFilterBytes) {
    num_bytes = kMaximumBloomFilterBytes;
  }

  std::string data(num_bytes, 0);
  return {BlockSplitBloomFilter(data), data};
}

StatusOr<BlockSplitBloomFilter> CreateBlockSplitBloomFilter(uint8_t* bitset, uint32_t num_bytes) {
  if (num_bytes < kMinimumBloomFilterBytes || num_bytes > kMaximumBloomFilterBytes ||
      (num_bytes & (num_bytes - 1)) != 0) {
    return {Status::NotOK, "invalid input bitset length"};
  }

  return BlockSplitBloomFilter({reinterpret_cast<char*>(bitset), num_bytes});
}

StatusOr<BlockSplitBloomFilter> CreateBlockSplitBloomFilter(std::string &bitset) {
  if (bitset.size() < kMinimumBloomFilterBytes || bitset.size() > kMaximumBloomFilterBytes ||
      (bitset.size() & (bitset.size() - 1)) != 0) {
    return {Status::NotOK, "invalid input bitset length"};
  }

  return BlockSplitBloomFilter(bitset);
}

bool BlockSplitBloomFilter::FindHash(uint64_t hash) const {
  const auto bucket_index = static_cast<uint32_t>(((hash >> 32) * (data_.size() / kBytesPerFilterBlock)) >> 32);
  const auto key = static_cast<uint32_t>(hash);
  const auto* bitset32 = reinterpret_cast<const uint32_t*>(data_.data());

  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    // Calculate mask for key in the given bitset.
    const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
    if ((0 == (bitset32[kBitsSetPerBlock * bucket_index + i] & mask))) {
      return false;
    }
  }
  return true;
}

void BlockSplitBloomFilter::InsertHash(uint64_t hash) {
  const auto bucket_index = static_cast<uint32_t>(((hash >> 32) * (data_.size() / kBytesPerFilterBlock)) >> 32);
  const auto key = static_cast<uint32_t>(hash);
  auto* bitset32 = reinterpret_cast<uint32_t*>(data_.data());

  for (int i = 0; i < kBitsSetPerBlock; i++) {
    // Calculate mask for key in the given bitset.
    const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
    bitset32[bucket_index * kBitsSetPerBlock + i] |= mask;
  }
}

uint64_t BlockSplitBloomFilter::Hash(const char* data, size_t length) { return XXH64(data, length, /*seed=*/0); }
