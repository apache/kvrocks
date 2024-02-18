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

#include <optional>
#include <string>
#include <vector>

#include "common/bitfield_util.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

class BitmapString : public Database {
 public:
  BitmapString(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  static rocksdb::Status GetBit(const std::string &raw_value, uint32_t offset, bool *bit);
  rocksdb::Status SetBit(const Slice &ns_key, std::string *raw_value, uint32_t bit_offset, bool new_bit, bool *old_bit);
  static rocksdb::Status BitCount(const std::string &raw_value, int64_t start, int64_t stop, bool is_bit_index,
                                  uint32_t *cnt);
  static rocksdb::Status BitPos(const std::string &raw_value, bool bit, int64_t start, int64_t stop, bool stop_given,
                                int64_t *pos);
  rocksdb::Status Bitfield(const Slice &ns_key, std::string *raw_value, const std::vector<BitfieldOperation> &ops,
                           std::vector<std::optional<BitfieldValue>> *rets);
  static rocksdb::Status BitfieldReadOnly(const Slice &ns_key, const std::string &raw_value,
                                          const std::vector<BitfieldOperation> &ops,
                                          std::vector<std::optional<BitfieldValue>> *rets);

  static size_t RawPopcount(const uint8_t *p, int64_t count);
  static int64_t RawBitpos(const uint8_t *c, int64_t count, bool bit);

  // NormalizeRange converts a range to a normalized range, which is a range with start and stop in [0, length).
  //
  // If start/end is negative, it will be converted to positive by adding length to it, and if the result is still
  // negative, it will be converted to 0.
  // If start/end is larger than length, it will be converted to length - 1.
  //
  // Return:
  //  The normalized [start, end] range.
  static std::pair<int64_t, int64_t> NormalizeRange(int64_t origin_start, int64_t origin_end, int64_t length);

  // NormalizeToByteRangeWithPaddingMask converts input index range to a normalized byte index range.
  // If the is_bit_index is false, it does nothing.
  // If the index_it_bit is true, it convert the bit index range to a normalized byte index range, and
  // pad the first byte negative mask and last byte negative mask.
  // Such as, If the starting bit is the third bit of the first byte like '00010000', the first_byte_neg_mask will be
  // padded to '11100000', if the end bit is in the fifth bit of the last byte like '00000100', the last_byte_neg_mask
  // will be padded to '00000011'.
  //
  // Return:
  //  The normalized [start_byte, stop_byte]
  static std::pair<int64_t, int64_t> NormalizeToByteRangeWithPaddingMask(bool is_bit_index, int64_t origin_start,
                                                                         int64_t origin_end,
                                                                         uint8_t *first_byte_neg_mask,
                                                                         uint8_t *last_byte_neg_mask);
};

}  // namespace redis
