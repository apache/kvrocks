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

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

#include "status.h"

enum class BitfieldOverflowBehavior : uint8_t { kWrap, kSat, kFail };

class BitfieldEncoding {
 public:
  enum class Type : uint8_t { kSigned, kUnsigned };

  // check whether the bit length is fit to given number sign.
  // Redis has bits length limitation to bitfield.
  // Quote:
  // The supported encodings are up to 64 bits for signed integers, and up to 63 bits for unsigned integers. This
  // limitation with unsigned integers is due to the fact that currently the Redis protocol is unable to return 64 bit
  // unsigned integers as replies.
  // see also https://redis.io/commands/bitfield/
  static bool IsSupportedBitLengths(Type type, uint8_t bits) {
    uint8_t max_bits = 64;
    if (type == Type::kUnsigned) {
      max_bits = 63;
    }
    return 1 <= bits && bits <= max_bits;
  }

  static StatusOr<BitfieldEncoding> Create(Type type, uint8_t bits) {
    if (!IsSupportedBitLengths(type, bits)) {
      return Status::NotOK;
    }
    BitfieldEncoding enc;
    enc.type_ = type;
    enc.bits_ = bits;
    return enc;
  }

  Type GetType() const noexcept { return type_; }

  bool IsSigned() const noexcept { return type_ == Type::kSigned; }

  bool IsUnsigned() const noexcept { return type_ == Type::kUnsigned; }

  uint8_t Bits() const noexcept { return bits_; }

  Status SetBits(uint8_t new_bits) {
    if (!IsSupportedBitLengths(type_, new_bits)) {
      return Status::NotOK;
    }
    bits_ = new_bits;
    return Status::OK();
  }

  Status SetType(Type new_type) {
    if (!IsSupportedBitLengths(new_type, bits_)) {
      return Status::NotOK;
    }
    type_ = new_type;
    return Status::OK();
  }

  std::string ToString() const noexcept { return (IsSigned() ? "i" : "u") + std::to_string(static_cast<int>(bits_)); }

 private:
  BitfieldEncoding() = default;

  Type type_;
  uint8_t bits_;
};

struct BitfieldOperation {
  // see https://redis.io/commands/bitfield/ to get more details.
  enum class Type : uint8_t { kGet, kSet, kIncrBy };

  Type type;
  BitfieldOverflowBehavior overflow{BitfieldOverflowBehavior::kWrap};
  BitfieldEncoding encoding{BitfieldEncoding::Create(BitfieldEncoding::Type::kSigned, 32).GetValue()};
  uint32_t offset;
  // INCRBY amount or SET value
  int64_t value;
};

namespace detail {
// return true if overflow. Status is not ok iff calling BitfieldEncoding::IsSupportedBitLengths()
// for given op return false.
StatusOr<bool> SignedBitfieldPlus(uint64_t value, int64_t incr, uint8_t bits, BitfieldOverflowBehavior overflow,
                                  uint64_t *dst);

// return true if overflow. Status is not ok iff calling BitfieldEncoding::IsSupportedBitLengths()
// for given op return false.
StatusOr<bool> UnsignedBitfieldPlus(uint64_t value, int64_t incr, uint8_t bits, BitfieldOverflowBehavior overflow,
                                    uint64_t *dst);
}  // namespace detail

// safe cast from unsigned to signed, without any bit changes.
// see also "Integral conversions" on https://en.cppreference.com/w/cpp/language/implicit_conversion
// If the destination type is signed, the result when overflow is implementation-defined until C++20
inline int64_t CastToSignedWithoutBitChanges(uint64_t x) {
  int64_t res = 0;
  memcpy(&res, &x, sizeof(res));
  return res;
}

class BitfieldValue {
 public:
  BitfieldValue(BitfieldEncoding encoding, uint64_t value) noexcept : encoding_(encoding), value_(value) {}

  template <class T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
  bool operator==(T rhs) const {
    return value_ == static_cast<uint64_t>(rhs);
  }

  template <class T>
  friend bool operator==(T lhs, const BitfieldValue &rhs) {
    return rhs == lhs;
  }

  BitfieldEncoding Encoding() const noexcept { return encoding_; }

  uint64_t Value() const noexcept { return value_; }

 private:
  BitfieldEncoding encoding_;
  uint64_t value_;
};

// return true if overflow. Status is not ok iff calling BitfieldEncoding::IsSupportedBitLengths()
// for given op return false.
inline StatusOr<bool> BitfieldPlus(uint64_t value, int64_t incr, BitfieldEncoding enc,
                                   BitfieldOverflowBehavior overflow, uint64_t *dst) {
  if (enc.IsSigned()) {
    return detail::SignedBitfieldPlus(value, incr, enc.Bits(), overflow, dst);
  }
  return detail::UnsignedBitfieldPlus(value, incr, enc.Bits(), overflow, dst);
}

// return true if successful. Status is not ok iff calling BitfieldEncoding::IsSupportedBitLengths()
// for given op return false.
inline StatusOr<bool> BitfieldOp(BitfieldOperation op, uint64_t old_value, uint64_t *new_value) {
  if (op.type == BitfieldOperation::Type::kGet) {
    *new_value = old_value;
    return true;
  }

  bool overflow = false;
  if (op.type == BitfieldOperation::Type::kSet) {
    overflow = BitfieldPlus(op.value, 0, op.encoding, op.overflow, new_value).GetValue();
  } else {
    overflow = BitfieldPlus(old_value, op.value, op.encoding, op.overflow, new_value).GetValue();
  }

  return op.overflow != BitfieldOverflowBehavior::kFail || !overflow;
}

// Use a small buffer to store a range of bytes on a bitmap.
// If you try to visit other place on bitmap. It will failed.
class ArrayBitfieldBitmap {
 public:
  // offset is the byte offset of view in entire bitmap.
  explicit ArrayBitfieldBitmap(uint32_t offset = 0) noexcept : offset_(offset) { Reset(); }

  ArrayBitfieldBitmap(const ArrayBitfieldBitmap &) = delete;
  ArrayBitfieldBitmap &operator=(const ArrayBitfieldBitmap &) = delete;
  ~ArrayBitfieldBitmap() = default;

  // Change the position this represents.
  void SetOffset(uint64_t offset) noexcept { offset_ = offset; }

  void Reset() { memset(buf_, 0, sizeof(buf_)); }

  Status Set(uint32_t offset, uint32_t bytes, const uint8_t *src) {
    if (isOutOfBound(offset, bytes)) {
      return Status::NotOK;
    }
    offset -= offset_;
    memcpy(buf_ + offset, src, bytes);
    return Status::OK();
  }

  Status Get(uint32_t offset, uint32_t bytes, uint8_t *dst) const {
    if (isOutOfBound(offset, bytes)) {
      return Status::NotOK;
    }
    offset -= offset_;
    memcpy(dst, buf_ + offset, bytes);
    return Status::OK();
  }

  StatusOr<uint64_t> GetUnsignedBitfield(uint64_t offset, uint64_t bits) const {
    if (!BitfieldEncoding::IsSupportedBitLengths(BitfieldEncoding::Type::kUnsigned, bits)) {
      return Status::NotOK;
    }
    return getBitfield(offset, bits);
  }

  StatusOr<int64_t> GetSignedBitfield(uint64_t offset, uint64_t bits) const {
    if (!BitfieldEncoding::IsSupportedBitLengths(BitfieldEncoding::Type::kSigned, bits)) {
      return Status::NotOK;
    }
    auto status = getBitfield(offset, bits);
    if (!status) {
      return status;
    }

    int64_t value = CastToSignedWithoutBitChanges(status.GetValue());

    // for a bits of k signed number, 1 << (bits - 1) is the MSB (most-significant bit).
    // the number is a negative when the MSB is "1".
    auto msb = static_cast<uint64_t>(1) << (bits - 1);
    if ((value & msb) != 0) {
      // The way of enlarge width of a signed integer is sign-extended.
      // The values of higher bits should all "1", when the number is negative.
      // For example:
      // constexpr int32_t a = -128;
      // static_assert(a == 0xffffff80);
      // constexpr int64_t b = a; // b is -128 too.
      // static_assert(b == 0xffffffffffffff80);
      value |= CastToSignedWithoutBitChanges(std::numeric_limits<uint64_t>::max() << bits);
    }
    return value;
  }

  Status SetBitfield(uint32_t offset, uint32_t bits, uint64_t value) {
    uint32_t first_byte = offset / 8;
    uint32_t last_byte = (offset + bits - 1) / 8 + 1;
    if (isOutOfBound(first_byte, last_byte - first_byte)) {
      return Status::NotOK;
    }

    offset -= offset_ * 8;
    while (bits--) {
      bool v = (value & (uint64_t(1) << bits)) != 0;
      uint32_t byte = offset >> 3;
      uint32_t bit = 7 - (offset & 7);
      uint32_t byteval = buf_[byte];
      byteval &= ~(1 << bit);
      byteval |= int(v) << bit;
      buf_[byte] = char(byteval);
      offset++;
    }
    return Status::OK();
  }

 private:
  // The bit field cannot exceed 64 bits, takes an extra byte when it unaligned.
  static constexpr uint32_t kSize = 8 + 1;

  bool isOutOfBound(uint32_t offset, uint32_t bytes) const noexcept {
    return offset < offset_ || offset_ + kSize < offset + bytes;
  }

  StatusOr<uint64_t> getBitfield(uint64_t offset, uint8_t bits) const {
    uint32_t first_byte = offset / 8;
    uint32_t last_byte = (offset + bits - 1) / 8 + 1;
    if (isOutOfBound(first_byte, last_byte - first_byte)) {
      return Status::NotOK;
    }

    offset -= offset_ * 8;
    uint64_t value = 0;
    while (bits--) {
      uint32_t byte = offset >> 3;
      uint32_t bit = 7 - (offset & 0x7);
      uint32_t byteval = buf_[byte];
      uint32_t bitval = (byteval >> bit) & 1;
      value = (value << 1) | bitval;
      offset++;
    }
    return value;
  }

  uint8_t buf_[kSize];
  uint32_t offset_;
};
