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

#include "bitfield_util.h"

namespace detail {

static uint64_t WrappedSignedBitfieldPlus(uint64_t value, int64_t incr, uint8_t bits) {
  uint64_t res = value + static_cast<uint64_t>(incr);
  if (bits < 64) {
    auto mask = std::numeric_limits<uint64_t>::max() << bits;
    if ((res & (1 << (bits - 1))) != 0) {
      res |= mask;
    } else {
      res &= ~mask;
    }
  }
  return res;
}

// See also https://github.com/redis/redis/blob/7f4bae817614988c43c3024402d16edcbf3b3277/src/bitops.c#L325
StatusOr<bool> SignedBitfieldPlus(uint64_t value, int64_t incr, uint8_t bits, BitfieldOverflowBehavior overflow,
                                  uint64_t *dst) {
  Status bits_status(BitfieldEncoding::CheckSupportedBitLengths(BitfieldEncoding::Type::kSigned, bits));
  if (!bits_status) {
    return bits_status;
  }

  auto max = std::numeric_limits<int64_t>::max();
  if (bits != 64) {
    max = (static_cast<int64_t>(1) << (bits - 1)) - 1;
  }
  int64_t min = -max - 1;

  int64_t signed_value = CastToSignedWithoutBitChanges(value);
  int64_t max_incr = CastToSignedWithoutBitChanges(static_cast<uint64_t>(max) - value);
  int64_t min_incr = min - signed_value;

  if (signed_value > max || (bits != 64 && incr > max_incr) || (signed_value >= 0 && incr >= 0 && incr > max_incr)) {
    if (overflow == BitfieldOverflowBehavior::kWrap) {
      *dst = WrappedSignedBitfieldPlus(value, incr, bits);
    } else if (overflow == BitfieldOverflowBehavior::kSat) {
      *dst = max;
    } else {
      DCHECK(overflow == BitfieldOverflowBehavior::kFail);
    }
    return true;
  } else if (signed_value < min || (bits != 64 && incr < min_incr) ||
             (signed_value < 0 && incr < 0 && incr < min_incr)) {
    if (overflow == BitfieldOverflowBehavior::kWrap) {
      *dst = WrappedSignedBitfieldPlus(value, incr, bits);
    } else if (overflow == BitfieldOverflowBehavior::kSat) {
      *dst = min;
    } else {
      DCHECK(overflow == BitfieldOverflowBehavior::kFail);
    }
    return true;
  }

  *dst = signed_value + incr;
  return false;
}

static uint64_t WrappedUnsignedBitfieldPlus(uint64_t value, int64_t incr, uint8_t bits) {
  uint64_t mask = std::numeric_limits<uint64_t>::max() << bits;
  uint64_t res = value + incr;
  res &= ~mask;
  return res;
}

// See also https://github.com/redis/redis/blob/7f4bae817614988c43c3024402d16edcbf3b3277/src/bitops.c#L288
StatusOr<bool> UnsignedBitfieldPlus(uint64_t value, int64_t incr, uint8_t bits, BitfieldOverflowBehavior overflow,
                                    uint64_t *dst) {
  Status bits_status(BitfieldEncoding::CheckSupportedBitLengths(BitfieldEncoding::Type::kUnsigned, bits));
  if (!bits_status) {
    return bits_status;
  }

  auto max = (static_cast<uint64_t>(1) << bits) - 1;
  int64_t max_incr = CastToSignedWithoutBitChanges(max - value);
  int64_t min_incr = CastToSignedWithoutBitChanges((~value) + 1);

  if (value > max || (incr > 0 && incr > max_incr)) {
    if (overflow == BitfieldOverflowBehavior::kWrap) {
      *dst = WrappedUnsignedBitfieldPlus(value, incr, bits);
    } else if (overflow == BitfieldOverflowBehavior::kSat) {
      *dst = max;
    } else {
      DCHECK(overflow == BitfieldOverflowBehavior::kFail);
    }
    return true;
  } else if (incr < 0 && incr < min_incr) {
    if (overflow == BitfieldOverflowBehavior::kWrap) {
      *dst = WrappedUnsignedBitfieldPlus(value, incr, bits);
    } else if (overflow == BitfieldOverflowBehavior::kSat) {
      *dst = 0;
    } else {
      DCHECK(overflow == BitfieldOverflowBehavior::kFail);
    }
    return true;
  }

  *dst = value + incr;
  return false;
}

}  // namespace detail
