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
#include "common/port.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

enum BitOpFlags {
  kBitOpAnd,
  kBitOpOr,
  kBitOpXor,
  kBitOpNot,
};

namespace redis {

// We use least-significant bit (LSB) numbering (also known as bit-endianness).
// This means that within a group of 8 bits, we read right-to-left.
// This is different from applying "bit" commands to string, which uses MSB.
class Bitmap : public Database {
 public:
  class SegmentCacheStore;

  Bitmap(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status GetBit(const Slice &user_key, uint32_t bit_offset, bool *bit);
  rocksdb::Status GetString(const Slice &user_key, uint32_t max_btos_size, std::string *value);
  rocksdb::Status SetBit(const Slice &user_key, uint32_t bit_offset, bool new_bit, bool *old_bit);
  rocksdb::Status BitCount(const Slice &user_key, int64_t start, int64_t stop, bool is_bit_index, uint32_t *cnt);
  rocksdb::Status BitPos(const Slice &user_key, bool bit, int64_t start, int64_t stop, bool stop_given, int64_t *pos);
  rocksdb::Status BitOp(BitOpFlags op_flag, const std::string &op_name, const Slice &user_key,
                        const std::vector<Slice> &op_keys, int64_t *len);
  rocksdb::Status Bitfield(const Slice &user_key, const std::vector<BitfieldOperation> &ops,
                           std::vector<std::optional<BitfieldValue>> *rets) {
    return bitfield<false>(user_key, ops, rets);
  }
  // read-only version for Bitfield(), if there is a write operation in ops, the function will return with failed
  // status.
  rocksdb::Status BitfieldReadOnly(const Slice &user_key, const std::vector<BitfieldOperation> &ops,
                                   std::vector<std::optional<BitfieldValue>> *rets) {
    return bitfield<true>(user_key, ops, rets);
  }
  static bool GetBitFromValueAndOffset(std::string_view value, uint32_t bit_offset);
  static bool IsEmptySegment(const Slice &segment);

 private:
  template <bool ReadOnly>
  rocksdb::Status bitfield(const Slice &user_key, const std::vector<BitfieldOperation> &ops,
                           std::vector<std::optional<BitfieldValue>> *rets);
  static bool bitfieldWriteAheadLog(const ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                    const std::vector<BitfieldOperation> &ops);
  rocksdb::Status GetMetadata(const Slice &ns_key, BitmapMetadata *metadata, std::string *raw_value);

  template <bool ReadOnly>
  static rocksdb::Status runBitfieldOperationsWithCache(SegmentCacheStore &cache,
                                                        const std::vector<BitfieldOperation> &ops,
                                                        std::vector<std::optional<BitfieldValue>> *rets);
};

}  // namespace redis
