#pragma once

#include <string>
#include <vector>

#include "encoding.h"
#include "storage.h"

enum RedisType {
  kRedisNone,
  kRedisString,
  kRedisHash,
  kRedisList,
  kRedisSet,
  kRedisZSet,
  kRedisBitmap
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

using rocksdb::Slice;

struct KeyNumStats {
  uint64_t n_key = 0;
  uint64_t n_expires = 0;
  uint64_t n_expired = 0;
  uint64_t avg_ttl = 0;
};

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
  explicit Metadata(RedisType type);

  RedisType Type() const;
  virtual int32_t TTL() const;
  virtual bool Expired() const;
  virtual void Encode(std::string *dst);
  virtual rocksdb::Status Decode(const std::string &bytes);
  bool operator==(const Metadata &that) const;

 private:
  uint64_t generateVersion();
};

class HashMetadata : public Metadata {
 public:
  HashMetadata():Metadata(kRedisHash){}
};

class SetMetadata : public Metadata {
 public:
  SetMetadata(): Metadata(kRedisSet) {}
};

class ZSetMetadata : public Metadata {
 public:
  ZSetMetadata(): Metadata(kRedisZSet){}
};

class BitmapMetadata : public Metadata {
 public:
  BitmapMetadata(): Metadata(kRedisBitmap){}
};

class ListMetadata : public Metadata {
 public:
  uint64_t head;
  uint64_t tail;
  ListMetadata();
 public:
  void Encode(std::string *dst) override;
  rocksdb::Status Decode(const std::string &bytes) override;
};
