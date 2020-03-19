#pragma once

#include <rocksdb/status.h>

#include <string>
#include <vector>
#include <atomic>

#include "encoding.h"

enum RedisType {
  kRedisNone,
  kRedisString,
  kRedisHash,
  kRedisList,
  kRedisSet,
  kRedisZSet,
  kRedisBitmap,
  kRedisSortedint,
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
};

const std::vector<std::string> RedisTypeNames = {
    "none", "string", "hash",
    "list", "set", "zset"
};

using rocksdb::Slice;

struct KeyNumStats {
  uint64_t n_key = 0;
  uint64_t n_expires = 0;
  uint64_t n_expired = 0;
  uint64_t avg_ttl = 0;
};

// 52 bit for microseconds and 11 bit for counter
const int VersionCounterBits = 11;
static std::atomic<uint64_t> version_counter_;

void ExtractNamespaceKey(Slice ns_key, std::string *ns, std::string *key);
void ComposeNamespaceKey(const Slice &ns, const Slice &key, std::string *ns_key);

class InternalKey {
 public:
  explicit InternalKey(Slice ns_key, Slice sub_key, uint64_t version);
  explicit InternalKey(Slice input);
  ~InternalKey();

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
  char *buf_;
  char prealloc_[256];
};

class Metadata {
 public:
  uint8_t flags;
  int expire;
  uint64_t version;
  uint32_t size;

 public:
  explicit Metadata(RedisType type, bool readonly=false);
  static void InitVersionCounter();

  RedisType Type() const;
  virtual int32_t TTL() const;
  virtual timeval Time() const;
  virtual bool Expired() const;
  virtual void Encode(std::string *dst);
  virtual rocksdb::Status Decode(const std::string &bytes);
  bool operator==(const Metadata &that) const;

 private:
  uint64_t generateVersion();
};

class HashMetadata : public Metadata {
 public:
  HashMetadata(bool readonly=false):Metadata(kRedisHash, readonly){}
};

class SetMetadata : public Metadata {
 public:
  SetMetadata(bool readonly=false): Metadata(kRedisSet, readonly) {}
};

class ZSetMetadata : public Metadata {
 public:
  ZSetMetadata(bool readonly=false): Metadata(kRedisZSet, readonly){}
};

class BitmapMetadata : public Metadata {
 public:
  BitmapMetadata(bool readonly=false): Metadata(kRedisBitmap, readonly){}
};

class SortedintMetadata : public Metadata {
 public:
  SortedintMetadata(bool readonly=false) : Metadata(kRedisSortedint, readonly) {}
};

class ListMetadata : public Metadata {
 public:
  uint64_t head;
  uint64_t tail;
  ListMetadata(bool readonly=false);
 public:
  void Encode(std::string *dst) override;
  rocksdb::Status Decode(const std::string &bytes) override;
};
