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
  static Status CheckSupportedBitLengths(Type type, uint8_t bits) noexcept {
    uint8_t max_bits = 64;
    if (type == Type::kUnsigned) {
      max_bits = 63;
    }
    if (1 <= bits && bits <= max_bits) {
      return Status::OK();
    }

    return {Status::NotOK, "Unsupported new bits length, only i1~i64, u1~u63 are supported."};
  }

  static StatusOr<BitfieldEncoding> Create(Type type, uint8_t bits) noexcept {
    Status bits_status(CheckSupportedBitLengths(type, bits));
    if (!bits_status) {
      return bits_status;
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

  Status SetBitsCount(uint8_t new_bits) noexcept {
    Status bits_status(CheckSupportedBitLengths(type_, new_bits));
    if (!bits_status) {
      return bits_status;
    }

    bits_ = new_bits;
    return Status::OK();
  }

  Status SetType(Type new_type) noexcept {
    Status bits_status(CheckSupportedBitLengths(new_type, bits_));
    if (!bits_status) {
      return bits_status;
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
// Let value add incr, according to the bits limit and overflow rule. The value is reguarded as a signed integer.
// Return true if overflow. Status is not ok iff calling BitfieldEncoding::IsSupportedBitLengths()
// for given op return false.
StatusOr<bool> SignedBitfieldPlus(uint64_t value, int64_t incr, uint8_t bits, BitfieldOverflowBehavior overflow,
                                  uint64_t *dst);

// Let value add incr, according to the bits limit and overflow rule.  The value is reguarded as an unsigned integer.
// Return true if overflow. Status is not ok iff calling BitfieldEncoding::IsSupportedBitLengths()
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

// Let value add incr, according to the encoding and overflow rule.
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
    overflow = GET_OR_RET(BitfieldPlus(op.value, 0, op.encoding, op.overflow, new_value));
  } else {
    overflow = GET_OR_RET(BitfieldPlus(old_value, op.value, op.encoding, op.overflow, new_value));
  }

  return op.overflow != BitfieldOverflowBehavior::kFail || !overflow;
}

// Use a small buffer to store a range of bytes on a bitmap.
// If you try to visit other place on bitmap, it will failed.
class ArrayBitfieldBitmap {
 public:
  // byte_offset is the byte offset of view in entire bitmap.
  explicit ArrayBitfieldBitmap(uint32_t byte_offset = 0) noexcept : byte_offset_(byte_offset) { Reset(); }

  ArrayBitfieldBitmap(const ArrayBitfieldBitmap &) = delete;
  ArrayBitfieldBitmap &operator=(const ArrayBitfieldBitmap &) = delete;
  ~ArrayBitfieldBitmap() = default;

  // Change the position this represents.
  void SetByteOffset(uint64_t byte_offset) noexcept { byte_offset_ = byte_offset; }

  void Reset() { memset(buf_, 0, sizeof(buf_)); }

  Status Set(uint32_t byte_offset, uint32_t bytes, const uint8_t *src) {
    Status bound_status(checkLegalBound(byte_offset, bytes));
    if (!bound_status) {
      return bound_status;
    }
    byte_offset -= byte_offset_;
    memcpy(buf_ + byte_offset, src, bytes);
    return Status::OK();
  }

  Status Get(uint32_t byte_offset, uint32_t bytes, uint8_t *dst) const {
    Status bound_status(checkLegalBound(byte_offset, bytes));
    if (!bound_status) {
      return bound_status;
    }
    byte_offset -= byte_offset_;
    memcpy(dst, buf_ + byte_offset, bytes);
    return Status::OK();
  }

  StatusOr<uint64_t> GetUnsignedBitfield(uint64_t bit_offset, uint64_t bits) const {
    Status bits_status(BitfieldEncoding::CheckSupportedBitLengths(BitfieldEncoding::Type::kUnsigned, bits));
    if (!bits_status) {
      return bits_status;
    }
    return getBitfield(bit_offset, bits);
  }

  StatusOr<int64_t> GetSignedBitfield(uint64_t bit_offset, uint64_t bits) const {
    Status bits_status(BitfieldEncoding::CheckSupportedBitLengths(BitfieldEncoding::Type::kSigned, bits));
    if (!bits_status) {
      return bits_status;
    }
    uint64_t bitfield = GET_OR_RET(getBitfield(bit_offset, bits));
    int64_t value = CastToSignedWithoutBitChanges(bitfield);

    // for a bits of k signed number, 1 << (bits - 1) is the MSB (most-significant bit).
    // the number is a negative when the MSB is "1".
    auto msb = static_cast<uint64_t>(1) << (bits - 1);  // NOLINT
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

  Status SetBitfield(uint32_t bit_offset, uint32_t bits, uint64_t value) {
    uint32_t first_byte = bit_offset / 8;
    uint32_t last_byte = (bit_offset + bits - 1) / 8 + 1;
    Status bound_status(checkLegalBound(first_byte, last_byte - first_byte));
    if (!bound_status) {
      return bound_status;
    }

    bit_offset -= byte_offset_ * 8;
    while (bits--) {
      bool v = (value & (uint64_t(1) << bits)) != 0;
      uint32_t byte = bit_offset >> 3;
      uint32_t bit = 7 - (bit_offset & 7);
      uint32_t byteval = buf_[byte];
      byteval &= ~(1 << bit);
      byteval |= int(v) << bit;
      buf_[byte] = char(byteval);
      bit_offset++;
    }
    return Status::OK();
  }

 private:
  // The bit field cannot exceed 64 bits, takes an extra byte when it unaligned.
  static constexpr uint32_t kSize = 8 + 1;

  Status checkLegalBound(uint32_t byte_offset, uint32_t bytes) const noexcept {
    if (byte_offset < byte_offset_ || byte_offset_ + kSize < byte_offset + bytes) {
      return {Status::NotOK, "The range [offset, offset + bytes) is out of bitfield."};
    }
    return Status::OK();
  }

  StatusOr<uint64_t> getBitfield(uint32_t bit_offset, uint8_t bits) const {
    uint32_t first_byte = bit_offset / 8;
    uint32_t last_byte = (bit_offset + bits - 1) / 8 + 1;
    Status bound_status(checkLegalBound(first_byte, last_byte - first_byte));
    if (!bound_status) {
      return bound_status;
    }

    bit_offset -= byte_offset_ * 8;
    uint64_t value = 0;
    while (bits--) {
      uint32_t byte = bit_offset >> 3;
      uint32_t bit = 7 - (bit_offset & 0x7);
      uint32_t byteval = buf_[byte];
      uint32_t bitval = (byteval >> bit) & 1;
      value = (value << 1) | bitval;
      bit_offset++;
    }
    return value;
  }

  uint8_t buf_[kSize];
  uint32_t byte_offset_;
};
