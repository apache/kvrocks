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

#include "redis_bitmap_string.h"

#include <glog/logging.h>

#include <cstdint>

#include "common/bit_util.h"
#include "common/type_util.h"
#include "redis_string.h"
#include "server/redis_reply.h"
#include "storage/redis_metadata.h"

namespace redis {

rocksdb::Status BitmapString::GetBit(const std::string &raw_value, uint32_t offset, bool *bit) {
  std::string_view string_value = std::string_view{raw_value}.substr(Metadata::GetOffsetAfterExpire(raw_value[0]));
  if (util::BytesForBits(offset) > static_cast<int64_t>(string_value.size())) {
    // When offset is beyond the string length, the string is assumed to be a contiguous space with 0 bits.
    return rocksdb::Status::OK();
  }
  *bit = util::GetBit(reinterpret_cast<const uint8_t *>(string_value.data()), offset);
  return rocksdb::Status::OK();
}

rocksdb::Status BitmapString::SetBit(const Slice &ns_key, std::string *raw_value, uint32_t offset, bool new_bit,
                                     bool *old_bit) {
  size_t header_offset = Metadata::GetOffsetAfterExpire((*raw_value)[0]);
  std::string string_value = raw_value->substr(header_offset);
  uint32_t byte_index = util::BytesForBits(offset);
  if (byte_index >= string_value.size()) {  // expand the bitmap
    string_value.append(byte_index - string_value.size() + 1, 0);
  }
  DCHECK(string_value.size() > byte_index);
  *old_bit = util::GetBit(reinterpret_cast<const uint8_t *>(string_value.data()), offset);
  util::SetBitTo(reinterpret_cast<uint8_t *>(string_value.data()), offset, new_bit);

  // Concat header and string value.
  *raw_value = raw_value->substr(0, header_offset) + string_value;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisString);
  batch->PutLogData(log_data.Encode());
  batch->Put(metadata_cf_handle_, ns_key, *raw_value);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BitmapString::BitCount(const std::string &raw_value, int64_t start, int64_t stop, uint32_t *cnt) {
  *cnt = 0;
  std::string_view string_value = std::string_view{raw_value}.substr(Metadata::GetOffsetAfterExpire(raw_value[0]));
  auto strlen = static_cast<int64_t>(string_value.size());
  /* Normalize range */
  std::tie(start, stop) = NormalizeRange(start, stop, strlen);
  if (start > stop) {
    return rocksdb::Status::OK();
  }
  /* Precondition: end >= 0 && end < strlen, so the only condition where
   * zero can be returned is: start > stop. */
  DCHECK(stop >= 0);
  DCHECK(stop < strlen);
  int64_t bytes = stop - start + 1;
  *cnt = util::RawPopcount(reinterpret_cast<const uint8_t *>(string_value.data()) + start, bytes);
  return rocksdb::Status::OK();
}

rocksdb::Status BitmapString::BitPos(const std::string &raw_value, bool bit, int64_t start, int64_t stop,
                                     bool stop_given, int64_t *pos) {
  std::string_view string_value = std::string_view{raw_value}.substr(Metadata::GetOffsetAfterExpire(raw_value[0]));
  auto strlen = static_cast<int64_t>(string_value.size());
  /* Convert negative and out-of-bound indexes */
  std::tie(start, stop) = NormalizeRange(start, stop, strlen);

  if (start > stop) {
    *pos = -1;
    return rocksdb::Status::OK();
  }
  // Searching bitpos within the range [start, stop].
  int64_t bytes = stop - start + 1;
  *pos = util::RawBitpos(reinterpret_cast<const uint8_t *>(string_value.data()) + start, bytes, bit);
  /* If we are looking for clear bits, and the user specified an exact
   * range with start-end, we can't consider the right of the range as
   * zero padded (as we do when no explicit end is given).
   *
   * So if redisBitpos() returns the first bit outside the range,
   * we return -1 to the caller, to mean, in the specified range there
   * is not a single "0" bit. */
  if (stop_given && bit == 0 && *pos == bytes * 8) {
    *pos = -1;
    return rocksdb::Status::OK();
  }
  if (*pos != -1) *pos += start * 8; /* Adjust for the bytes we skipped. */
  return rocksdb::Status::OK();
}

std::pair<int64_t, int64_t> BitmapString::NormalizeRange(int64_t origin_start, int64_t origin_end, int64_t length) {
  if (origin_start < 0) origin_start = length + origin_start;
  if (origin_end < 0) origin_end = length + origin_end;
  if (origin_start < 0) origin_start = 0;
  if (origin_end < 0) origin_end = 0;
  if (origin_end >= length) origin_end = length - 1;
  return {origin_start, origin_end};
}

rocksdb::Status BitmapString::Bitfield(const Slice &ns_key, std::string *raw_value,
                                       const std::vector<BitfieldOperation> &ops,
                                       std::vector<std::optional<BitfieldValue>> *rets) {
  auto header_offset = Metadata::GetOffsetAfterExpire((*raw_value)[0]);
  std::string string_value = raw_value->substr(header_offset);
  for (BitfieldOperation op : ops) {
    // [first_byte, last_byte)
    uint32_t first_byte = op.offset / 8;
    uint32_t last_byte = (op.offset + op.encoding.Bits() - 1) / 8 + 1;

    // expand string if need.
    if (string_value.size() < last_byte) {
      string_value.resize(last_byte);
    }

    ArrayBitfieldBitmap bitfield(first_byte);
    const auto *str = reinterpret_cast<const uint8_t *>(string_value.data() + first_byte);
    auto s = bitfield.Set(/*byte_offset=*/first_byte, /*bytes=*/last_byte - first_byte, /*src=*/str);
    if (!s.IsOK()) {
      return rocksdb::Status::IOError(s.Msg());
    }

    uint64_t unsigned_old_value = 0;
    if (op.encoding.IsSigned()) {
      unsigned_old_value = bitfield.GetSignedBitfield(op.offset, op.encoding.Bits()).GetValue();
    } else {
      unsigned_old_value = bitfield.GetUnsignedBitfield(op.offset, op.encoding.Bits()).GetValue();
    }

    uint64_t unsigned_new_value = 0;
    std::optional<BitfieldValue> &ret = rets->emplace_back();
    StatusOr<bool> bitfield_op = BitfieldOp(op, unsigned_old_value, &unsigned_new_value);
    if (!bitfield_op.IsOK()) {
      return rocksdb::Status::InvalidArgument(bitfield_op.Msg());
    }
    if (bitfield_op.GetValue()) {
      if (op.type != BitfieldOperation::Type::kGet) {
        // never failed.
        s = bitfield.SetBitfield(op.offset, op.encoding.Bits(), unsigned_new_value);
        CHECK(s.IsOK());
        auto *dst = reinterpret_cast<uint8_t *>(string_value.data()) + first_byte;
        s = bitfield.Get(first_byte, last_byte - first_byte, dst);
        CHECK(s.IsOK());
      }

      if (op.type == BitfieldOperation::Type::kSet) {
        unsigned_new_value = unsigned_old_value;
      }

      ret = {op.encoding, unsigned_new_value};
    }
  }

  raw_value->resize(header_offset);
  raw_value->append(string_value);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisString);
  batch->PutLogData(log_data.Encode());
  batch->Put(metadata_cf_handle_, ns_key, *raw_value);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BitmapString::BitfieldReadOnly(const Slice &ns_key, const std::string &raw_value,
                                               const std::vector<BitfieldOperation> &ops,
                                               std::vector<std::optional<BitfieldValue>> *rets) {
  std::string_view string_value = raw_value;
  string_value = string_value.substr(Metadata::GetOffsetAfterExpire(string_value[0]));

  for (BitfieldOperation op : ops) {
    if (op.type != BitfieldOperation::Type::kGet) {
      return rocksdb::Status::InvalidArgument("Write bitfield in read-only mode.");
    }

    uint32_t first_byte = op.offset / 8;
    uint32_t last_byte = (op.offset + op.encoding.Bits() - 1) / 8 + 1;

    ArrayBitfieldBitmap bitfield(first_byte);
    auto s = bitfield.Set(first_byte, last_byte - first_byte,
                          reinterpret_cast<const uint8_t *>(string_value.data() + first_byte));
    if (!s.IsOK()) {
      return rocksdb::Status::IOError(s.Msg());
    }

    if (op.encoding.IsSigned()) {
      int64_t value = bitfield.GetSignedBitfield(op.offset, op.encoding.Bits()).GetValue();
      rets->emplace_back(std::in_place, op.encoding, static_cast<uint64_t>(value));
    } else {
      uint64_t value = bitfield.GetUnsignedBitfield(op.offset, op.encoding.Bits()).GetValue();
      rets->emplace_back(std::in_place, op.encoding, value);
    }
  }

  return rocksdb::Status::OK();
}

}  // namespace redis
