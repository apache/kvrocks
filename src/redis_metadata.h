#pragma once

#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <rocksdb/db.h>
#include <string>
#include <vector>

#include "redis_encoding.h"
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

using rocksdb::Slice;

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

class RedisDB {
 public:
  explicit RedisDB(Engine::Storage *storage, std::string ns);
  rocksdb::Status GetMetadata(RedisType type, Slice key, Metadata *metadata);
  rocksdb::Status Expire(Slice key, int timestamp);
  rocksdb::Status Del(Slice key);
  rocksdb::Status Exists(std::vector<Slice> keys, int *ret);
  rocksdb::Status TTL(Slice key, int *ttl);
  rocksdb::Status Type(Slice key, RedisType *type);
  rocksdb::Status FlushAll();
  uint64_t GetKeyNum(const std::string &prefix = "");
  uint64_t Keys(std::string prefix, std::vector<std::string> *keys);
  uint64_t Scan(const std::string &cursor,
                const uint64_t &limit,
                const std::string &prefix,
                std::vector<std::string> *keys);
  void AppendNamespacePrefix(const Slice &key, std::string *output);

 protected:
  Engine::Storage *storage_;
  rocksdb::DB *db_;
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_;
  std::string namespace_;

  class LatestSnapShot {
   public:
    explicit LatestSnapShot(rocksdb::DB *db): db_(db) {
      snapshot_ = db_->GetSnapshot();
    }
    ~LatestSnapShot() {
      db_->ReleaseSnapshot(snapshot_);
    }
    const rocksdb::Snapshot *GetSnapShot() { return snapshot_; }
   private:
    rocksdb::DB *db_ = nullptr;
    const rocksdb::Snapshot* snapshot_ = nullptr;
  };
};

class RedisSubKeyScanner : public RedisDB {
 public:
  explicit RedisSubKeyScanner(Engine::Storage *storage, const std::string &ns)
      : RedisDB(storage, ns) {}
  uint64_t Scan(RedisType type,
                Slice key,
                const std::string &cursor,
                const uint64_t &limit,
                const std::string &subkey_prefix,
                std::vector<std::string> *keys);
};

class LockGuard {
 public:
  explicit LockGuard(LockManager *lock_mgr, Slice key):
      lock_mgr_(lock_mgr),
      key_(key) {
      lock_mgr->Lock(key_);
  }
  ~LockGuard() {
    lock_mgr_->UnLock(key_);
  }
 private:
  LockManager *lock_mgr_ = nullptr;
  Slice key_;
};
