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

StatusOr<bool> SignedBitfieldPlus(uint64_t value, int64_t incr, uint8_t bits, BitfieldOverflowBehavior overflow,
                                  uint64_t *dst) {
  if (!BitfieldEncoding::IsSupportedBitLengths(BitfieldEncoding::Type::kSigned, bits)) {
    return Status::NotOK;
  }

  auto max = std::numeric_limits<int64_t>::max();
  if (bits != 64) {
    max = (static_cast<int64_t>(1) << (bits - 1)) - 1;
  }
  int64_t min = -max - 1;

  int64_t signed_value = CastToSignedWithoutBitChanges(value);
  int64_t max_incr = CastToSignedWithoutBitChanges(static_cast<uint64_t>(max) - value);
  int64_t min_incr = min - signed_value;

  auto wrap = [=]() {
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
  };

  if (signed_value > max || (bits != 64 && incr > max_incr) || (signed_value >= 0 && incr >= 0 && incr > max_incr)) {
    if (overflow == BitfieldOverflowBehavior::kWrap) {
      *dst = wrap();
    } else {
      *dst = max;
    }
    return true;
  } else if (signed_value < min || (bits != 64 && incr < min_incr) ||
             (signed_value < 0 && incr < 0 && incr < min_incr)) {
    if (overflow == BitfieldOverflowBehavior::kWrap) {
      *dst = wrap();
    } else {
      *dst = min;
    }
    return true;
  }

  *dst = signed_value + incr;
  return false;
}

// return true if overflow.
StatusOr<bool> UnsignedBitfieldPlus(uint64_t value, int64_t incr, uint8_t bits, BitfieldOverflowBehavior overflow,
                                    uint64_t *dst) {
  if (!BitfieldEncoding::IsSupportedBitLengths(BitfieldEncoding::Type::kUnsigned, bits)) {
    return Status::NotOK;
  }
  auto max = (static_cast<uint64_t>(1) << bits) - 1;
  int64_t max_incr = CastToSignedWithoutBitChanges(max - value);
  int64_t min_incr = CastToSignedWithoutBitChanges((~value) + 1);

  auto wrap = [=]() {
    uint64_t mask = std::numeric_limits<uint64_t>::max() << bits;
    uint64_t res = value + incr;
    res &= ~mask;
    return res;
  };

  if (value > max || (incr > 0 && incr > max_incr)) {
    if (overflow == BitfieldOverflowBehavior::kWrap) {
      *dst = wrap();
    } else if (overflow == BitfieldOverflowBehavior::kSat) {
      *dst = max;
    }
    return true;
  } else if (incr < 0 && incr < min_incr) {
    if (overflow == BitfieldOverflowBehavior::kWrap) {
      *dst = wrap();
    } else if (overflow == BitfieldOverflowBehavior::kSat) {
      *dst = 0;
    }
    return true;
  }

  *dst = value + incr;
  return false;
}

}  // namespace detail
