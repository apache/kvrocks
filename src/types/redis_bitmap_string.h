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
  rocksdb::Status SetBit(const Slice &ns_key, std::string *raw_value, uint32_t offset, bool new_bit, bool *old_bit);
  static rocksdb::Status BitCount(const std::string &raw_value, int64_t start, int64_t stop, bool is_bit,
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

  static std::pair<int64_t, int64_t> AdjustMaskWithRange(bool is_bit, int64_t origin_start, int64_t origin_end,
                                                         uint8_t *first_byte_neg_mask, uint8_t *last_byte_neg_mask);
};

}  // namespace redis
