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

namespace util {

/* Count number of bits set in the binary array pointed by 's' and long
 * 'count' bytes. The implementation of this function is required to
 * work with a input string length up to 512 MB.
 * */
inline size_t RawPopcount(const uint8_t *p, int64_t count) {
  size_t bits = 0;

  for (; count >= 8; p += 8, count -= 8) {
    bits += __builtin_popcountll(*reinterpret_cast<const uint64_t *>(p));
  }

  if (count > 0) {
    uint64_t v = 0;
    __builtin_memcpy(&v, p, count);
    bits += __builtin_popcountll(v);
  }

  return bits;
}

template <typename T = void>
inline int ClzllWithEndian(uint64_t x) {
  if constexpr (IsLittleEndian()) {
    return __builtin_clzll(__builtin_bswap64(x));
  } else if constexpr (IsBigEndian()) {
    return __builtin_clzll(x);
  } else {
    static_assert(AlwaysFalse<T>);
  }
}

// Return the number of bytes needed to fit the given number of bits
constexpr int64_t BytesForBits(int64_t bits) {
  // This formula avoids integer overflow on very large `bits`
  return (bits >> 3) + ((bits & 7) != 0);
}

namespace lsb {
static constexpr bool GetBit(const uint8_t *bits, uint64_t i) { return (bits[i >> 3] >> (i & 0x07)) & 1; }

// Bitmask selecting the k-th bit in a byte
static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

// Gets the i-th bit from a byte. Should only be used with i <= 7.
static constexpr bool GetBitFromByte(uint8_t byte, uint8_t i) { return byte & kBitmask[i]; }

static inline void SetBitTo(uint8_t *bits, int64_t i, bool bit_is_set) {
  // https://graphics.stanford.edu/~seander/bithacks.html
  // "Conditionally set or clear bits without branching"
  // NOTE: this seems to confuse Valgrind as it reads from potentially
  // uninitialized memory
  bits[i / 8] ^= static_cast<uint8_t>(-static_cast<uint8_t>(bit_is_set) ^ bits[i / 8]) & kBitmask[i % 8];
}
}  // namespace lsb

namespace msb {
static constexpr bool GetBit(const uint8_t *bits, uint64_t i) { return (bits[i >> 3] >> (7 - (i & 0x07))) & 1; }

// Bitmask selecting the k-th bit in a byte
static constexpr uint8_t kBitmask[] = {128, 64, 32, 16, 8, 4, 2, 1};

// Gets the i-th bit from a byte. Should only be used with i <= 7.
static constexpr bool GetBitFromByte(uint8_t byte, uint8_t i) { return byte & kBitmask[i]; }

static inline void SetBitTo(uint8_t *bits, int64_t i, bool bit_is_set) {
  // https://graphics.stanford.edu/~seander/bithacks.html
  // "Conditionally set or clear bits without branching"
  // NOTE: this seems to confuse Valgrind as it reads from potentially
  // uninitialized memory
  bits[i / 8] ^= static_cast<uint8_t>(-static_cast<uint8_t>(bit_is_set) ^ bits[i / 8]) & kBitmask[i % 8];
}

inline int64_t RawBitposBytes(const uint8_t *c, int64_t count, bool bit) {
  int64_t res = 0;

  if (bit) {
    int64_t ct = count;

    for (; count >= 8; c += 8, count -= 8) {
      uint64_t x = *reinterpret_cast<const uint64_t *>(c);
      if (x != 0) {
        return res + ClzllWithEndian(x);
      }
      res += 64;
    }

    if (count > 0) {
      uint64_t v = 0;
      __builtin_memcpy(&v, c, count);
      res += v == 0 ? count * 8 : ClzllWithEndian(v);
    }

    if (res == ct * 8) {
      return -1;
    }
  } else {
    for (; count >= 8; c += 8, count -= 8) {
      uint64_t x = *reinterpret_cast<const uint64_t *>(c);
      if (x != (uint64_t)-1) {
        return res + ClzllWithEndian(~x);
      }
      res += 64;
    }

    if (count > 0) {
      uint64_t v = -1;
      __builtin_memcpy(&v, c, count);
      res += v == (uint64_t)-1 ? count * 8 : ClzllWithEndian(~v);
    }
  }

  return res;
}

/* Return the position of the first bit set to one (if 'bit' is 1) or
 * zero (if 'bit' is 0) in the bitmap 's' starting at 'start_bit' and
 * ending at 'end_bit'
 *
 * The function is guaranteed to return a value >= 0 if 'bit' is 0 since if
 * no zero bit is found, it returns count*8 assuming the string is zero
 * padded on the right. However if 'bit' is 1 it is possible that there is
 * not a single set bit in the bitmap. In this special case -1 is returned.
 * */
inline int64_t RawBitpos(const uint8_t *s, int64_t start_bit, int64_t end_bit, bool bit) {
  int64_t start_byte = start_bit / 8;
  int64_t end_byte = end_bit / 8;
  int64_t start_bit_in_byte = start_bit % 8;
  int64_t end_bit_in_byte = end_bit % 8;

  // If the range is contained in a single byte
  if (start_byte == end_byte) {
    for (int64_t i = start_bit_in_byte; i <= end_bit_in_byte; i++) {
      if (msb::GetBitFromByte(s[start_byte], i) == bit) {
        return i + start_byte * 8;
      }
    }

    // return count if no bit is found and bit is 0, else return -1
    return bit ? -1 : (end_bit - start_bit + 1);
  }

  // Check the start byte
  for (int64_t i = start_bit_in_byte; i < 8; i++) {
    if (msb::GetBitFromByte(s[start_byte], i) == bit) {
      return i + start_byte * 8;
    }
  }

  // iterate over long bytes in the middle
  int64_t res = msb::RawBitposBytes(s + start_byte + 1, end_byte - start_byte - 1, bit);
  if (res != -1 && res != (end_byte - start_byte - 1) * 8) {
    return res + (start_byte + 1) * 8;
  }

  // check the last byte
  for (int64_t i = 0; i <= end_bit_in_byte; i++) {
    if (msb::GetBitFromByte(s[end_byte], i) == bit) {
      return i + end_byte * 8;
    }
  }

  return bit ? -1 : (end_bit - start_bit + 1);
}

}  // namespace msb

}  // namespace util
