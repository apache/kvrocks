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

#pragma once

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>

// Returns the smallest power of two that contains v.  If v is already a
// power of two, it is returned as is.
static inline int64_t NextPower2(int64_t n) {
  // Taken from
  // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  n++;
  return n;
}

constexpr bool IsMultipleOf64(int64_t n) { return (n & 63) == 0; }

constexpr bool IsMultipleOf8(int64_t n) { return (n & 7) == 0; }

// Maximum Bloom filter size, it sets to HDFS default block size 128MB
// This value will be reconsidered when implementing Bloom filter producer.
static constexpr uint32_t kMaximumBloomFilterBytes = 128 * 1024 * 1024;

/// The BlockSplitBloomFilter is implemented using block-based Bloom filters from
/// Putze et al.'s "Cache-,Hash- and Space-Efficient Bloom filters". The basic idea is to
/// hash the item to a tiny Bloom filter which size fit a single cache line or smaller.
///
/// This implementation sets 8 bits in each tiny Bloom filter. Each tiny Bloom
/// filter is 32 bytes to take advantage of 32-byte SIMD instructions.
class BlockSplitBloomFilter {
 public:
  /// The constructor of BlockSplitBloomFilter. It uses XXH64 as hash function.
  BlockSplitBloomFilter();

  /// Initialize the BlockSplitBloomFilter. The range of num_bytes should be within
  /// [kMinimumBloomFilterBytes, kMaximumBloomFilterBytes], it will be
  /// rounded up/down to lower/upper bound if num_bytes is out of range and also
  /// will be rounded up to a power of 2.
  ///
  /// @param num_bytes The number of bytes to store Bloom filter bitset.
  void Init(uint32_t num_bytes);

  /// Initialize the BlockSplitBloomFilter. It copies the bitset as underlying
  /// bitset because the given bitset may not satisfy the 32-byte alignment requirement
  /// which may lead to segfault when performing SIMD instructions. It is the caller's
  /// responsibility to free the bitset passed in.
  ///
  /// @param bitset The given bitset to initialize the Bloom filter.
  /// @param num_bytes  The number of bytes of given bitset.
  /// @return false if the number of bytes of Bloom filter bitset is not a power of 2, and true means successfully init
  bool Init(const uint8_t* bitset, uint32_t num_bytes);

  /// Initialize the BlockSplitBloomFilter. It copies the bitset as underlying
  /// bitset because the given bitset may not satisfy the 32-byte alignment requirement
  /// which may lead to segfault when performing SIMD instructions. It is the caller's
  /// responsibility to free the bitset passed in.
  ///
  /// @param bitset The given bitset to initialize the Bloom filter.
  /// @return false if the number of bytes of Bloom filter bitset is not a power of 2, and true means successfully init
  bool Init(std::string bitset);

  /// Create the read-only BlockSplitBloomFilter. It use the caller's bitset as underlying bitset. It is the caller's
  /// responsibility to ensure the bitset would not to change.
  ///
  /// @param bitset The given bitset for the Bloom filter underlying bitset.
  /// @return the unique_ptr of the const non-owned BlockSplitBloomFilter
  static std::unique_ptr<const BlockSplitBloomFilter> CreateReadOnlyBloomFilter(const std::string& bitset);

  /// Minimum Bloom filter size, it sets to 32 bytes to fit a tiny Bloom filter.
  static constexpr uint32_t kMinimumBloomFilterBytes = 32;

  /// Calculate optimal size according to the number of distinct values and false
  /// positive probability.
  ///
  /// @param ndv The number of distinct values.
  /// @param fpp The false positive probability.
  /// @return it always return a value between kMinimumBloomFilterBytes and
  /// kMaximumBloomFilterBytes, and the return value is always a power of 2
  static uint32_t OptimalNumOfBytes(uint32_t ndv, double fpp) {
    uint32_t optimal_num_of_bits = OptimalNumOfBits(ndv, fpp);
    //    CHECK(IsMultipleOf8(optimal_num_of_bits));
    return optimal_num_of_bits >> 3;
  }

  /// Calculate optimal size according to the number of distinct values and false
  /// positive probability.
  ///
  /// @param ndv The number of distinct values.
  /// @param fpp The false positive probability.
  /// @return it always return a value between kMinimumBloomFilterBytes * 8 and
  /// kMaximumBloomFilterBytes * 8, and the return value is always a power of 16
  static uint32_t OptimalNumOfBits(uint32_t ndv, double fpp) {
    //    CHECK(fpp > 0.0 && fpp < 1.0);
    const double m = -8.0 * ndv / log(1 - pow(fpp, 1.0 / 8));
    uint32_t num_bits = 0;

    // Handle overflow.
    if (m < 0 || m > kMaximumBloomFilterBytes << 3) {
      num_bits = static_cast<uint32_t>(kMaximumBloomFilterBytes << 3);
    } else {
      num_bits = static_cast<uint32_t>(m);
    }

    // Round up to lower bound
    if (num_bits < kMinimumBloomFilterBytes << 3) {
      num_bits = kMinimumBloomFilterBytes << 3;
    }

    // Get next power of 2 if bits is not power of 2.
    if ((num_bits & (num_bits - 1)) != 0) {
      num_bits = static_cast<uint32_t>(NextPower2(num_bits));
    }

    // Round down to upper bound
    if (num_bits > kMaximumBloomFilterBytes << 3) {
      num_bits = kMaximumBloomFilterBytes << 3;
    }

    return num_bits;
  }

  /// Determine whether an element exist in set or not.
  ///
  /// @param hash the element to contain.
  /// @return false if value is definitely not in set, and true means PROBABLY
  /// in set.
  bool FindHash(uint64_t hash) const;

  /// Insert element to set represented by Bloom filter bitset.
  ///
  /// @param hash the hash of value to insert into Bloom filter.
  void InsertHash(uint64_t hash);

  uint32_t GetBitsetSize() const { return num_bytes_; }

  /// Get the plain bitset value from the Bloom filter bitset.
  ///
  /// @return bitset value;
  const std::string& GetData() const& { return data_; }

  std::string&& GetData() && { return std::move(data_); }

  /// Compute hash for string value by using its plain encoding result.
  ///
  /// @param value the value address.
  /// @param len the value length.
  /// @return hash result.
  static uint64_t Hash(const char* data, size_t length);

 private:
  // The private constructor of BlockSplitBloomFilter. It's only used for CreateReadOnlyBloomFilter
  explicit BlockSplitBloomFilter(const std::string& bitset);

  // Bytes in a tiny Bloom filter block.
  static constexpr int kBytesPerFilterBlock = 32;

  // The number of bits to be set in each tiny Bloom filter
  static constexpr int kBitsSetPerBlock = 8;

  // The block-based algorithm needs eight odd SALT values to calculate eight indexes
  // of bit to set, one bit in each 32-bit word.
  static constexpr uint32_t SALT[kBitsSetPerBlock] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
                                                      0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

  // The underlying buffer of bitset.
  std::string data_;

  // The view of data_
  std::string_view data_view_;

  // The number of bytes of Bloom filter bitset.
  uint32_t num_bytes_;
};
