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

#include <rocksdb/status.h>

#include <atomic>
#include <string>
#include <vector>

#include "encoding.h"
#include "types/redis_stream_base.h"

constexpr bool USE_64BIT_COMMON_FIELD_DEFAULT =
#ifdef ENABLE_NEW_ENCODING
    true
#else
    false
#endif
    ;

enum RedisType {
  kRedisNone,
  kRedisString,
  kRedisHash,
  kRedisList,
  kRedisSet,
  kRedisZSet,
  kRedisBitmap,
  kRedisSortedint,
  kRedisStream,
};

enum RedisCommand {
  kRedisCmdLSet,
  kRedisCmdLInsert,
  kRedisCmdLTrim,
  kRedisCmdLPop,
  kRedisCmdRPop,
  kRedisCmdLRem,
  kRedisCmdLPush,
  kRedisCmdRPush,
  kRedisCmdExpire,
  kRedisCmdSetBit,
  kRedisCmdBitOp,
  kRedisCmdLMove,
};

const std::vector<std::string> RedisTypeNames = {"none", "string", "hash",      "list",  "set",
                                                 "zset", "bitmap", "sortedint", "stream"};

constexpr const char *kErrMsgWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value";
constexpr const char *kErrMsgKeyExpired = "the key was expired";

using rocksdb::Slice;

struct KeyNumStats {
  uint64_t n_key = 0;
  uint64_t n_expires = 0;
  uint64_t n_expired = 0;
  uint64_t avg_ttl = 0;
};

void ExtractNamespaceKey(Slice ns_key, std::string *ns, std::string *key, bool slot_id_encoded);
void ComposeNamespaceKey(const Slice &ns, const Slice &key, std::string *ns_key, bool slot_id_encoded);
void ComposeSlotKeyPrefix(const Slice &ns, int slotid, std::string *output);

class InternalKey {
 public:
  explicit InternalKey(Slice ns_key, Slice sub_key, uint64_t version, bool slot_id_encoded);
  explicit InternalKey(Slice input, bool slot_id_encoded);
  ~InternalKey() = default;

  Slice GetNamespace() const;
  Slice GetKey() const;
  Slice GetSubKey() const;
  uint64_t GetVersion() const;
  void Encode(std::string *out);
  bool operator==(const InternalKey &that) const;

 private:
  Slice namespace_;
  Slice key_;
  Slice sub_key_;
  uint64_t version_;
  uint16_t slotid_;
  bool slot_id_encoded_;
};

constexpr uint8_t METADATA_64BIT_ENCODING_MASK = 0x80;
constexpr uint8_t METADATA_TYPE_MASK = 0x0f;

class Metadata {
 public:
  // metadata flags
  // <(1-bit) 64bit-common-field-indicator> 0 0 0 <(4-bit) redis-type>
  // 64bit-common-field-indicator: make `expire` and `size` 64bit instead of 32bit
  // NOTE: `expire` is stored in milliseconds for 64bit, seconds for 32bit
  // redis-type: RedisType for the key-value
  uint8_t flags;

  // expire timestamp, in milliseconds
  uint64_t expire;

  // the current version: 53bit timestamp + 11bit counter
  uint64_t version;

  // element size of the key-value
  uint64_t size;

  explicit Metadata(RedisType type, bool generate_version = true,
                    bool use_64bit_common_field = USE_64BIT_COMMON_FIELD_DEFAULT);
  static void InitVersionCounter();

  static size_t GetOffsetAfterExpire(uint8_t flags);
  static size_t GetOffsetAfterSize(uint8_t flags);
  static uint64_t ExpireMsToS(uint64_t ms);

  bool Is64BitEncoded() const;
  bool GetFixedCommon(rocksdb::Slice *input, uint64_t *value) const;
  bool GetExpire(rocksdb::Slice *input);
  void PutFixedCommon(std::string *dst, uint64_t value) const;
  void PutExpire(std::string *dst) const;

  RedisType Type() const;
  size_t CommonEncodedSize() const;
  int64_t TTL() const;
  timeval Time() const;
  bool Expired() const;
  bool ExpireAt(uint64_t expired_ts) const;
  virtual void Encode(std::string *dst);
  virtual rocksdb::Status Decode(const std::string &bytes);
  bool operator==(const Metadata &that) const;

 private:
  static uint64_t generateVersion();
};

class HashMetadata : public Metadata {
 public:
  explicit HashMetadata(bool generate_version = true) : Metadata(kRedisHash, generate_version) {}
};

class SetMetadata : public Metadata {
 public:
  explicit SetMetadata(bool generate_version = true) : Metadata(kRedisSet, generate_version) {}
};

class ZSetMetadata : public Metadata {
 public:
  explicit ZSetMetadata(bool generate_version = true) : Metadata(kRedisZSet, generate_version) {}
};

class BitmapMetadata : public Metadata {
 public:
  explicit BitmapMetadata(bool generate_version = true) : Metadata(kRedisBitmap, generate_version) {}
};

class SortedintMetadata : public Metadata {
 public:
  explicit SortedintMetadata(bool generate_version = true) : Metadata(kRedisSortedint, generate_version) {}
};

class ListMetadata : public Metadata {
 public:
  uint64_t head;
  uint64_t tail;
  explicit ListMetadata(bool generate_version = true);

  void Encode(std::string *dst) override;
  rocksdb::Status Decode(const std::string &bytes) override;
};

class StreamMetadata : public Metadata {
 public:
  Redis::StreamEntryID last_generated_id;
  Redis::StreamEntryID recorded_first_entry_id;
  Redis::StreamEntryID max_deleted_entry_id;
  Redis::StreamEntryID first_entry_id;
  Redis::StreamEntryID last_entry_id;
  uint64_t entries_added = 0;

  explicit StreamMetadata(bool generate_version = true) : Metadata(kRedisStream, generate_version) {}

  void Encode(std::string *dst) override;
  rocksdb::Status Decode(const std::string &bytes) override;
};
