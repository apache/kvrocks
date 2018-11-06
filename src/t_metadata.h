#ifndef KVROCKS_T_METADATA_H
#define KVROCKS_T_METADATA_H

#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <string>
#include <rocksdb/db.h>
#include "t_encoding.h"
#include "storage.h"

enum RedisType {
  kRedisNone,
  kRedisString,
  kRedisHash,
  kRedisList,
  kRedisSet,
  kRedisZSet
};

using rocksdb::Slice;

class InternalKey {
public:
  explicit InternalKey(Slice key, Slice sub_key, uint64_t version);
  explicit InternalKey(Slice input);
  ~InternalKey();

  Slice GetKey();
  Slice GetSubKey();
  uint64_t GetVersion();
  void Encode(std::string *out);
  bool operator==(const InternalKey &that) const;

private:
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
  virtual rocksdb::Status Decode(std::string &bytes);
  bool operator==(const Metadata &that) const;

private:
  uint64_t generateVersion();
};

class HashMetadata : public Metadata {
public:
  explicit HashMetadata():Metadata(kRedisHash){}
};

class SetMetadata : public Metadata {
public:
 explicit SetMetadata(): Metadata(kRedisSet) {}
};

class ZSetMetadata : public Metadata {
public:
  explicit ZSetMetadata(): Metadata(kRedisZSet){}
};

class ListMetadata : public Metadata {
public:
  uint64_t head;
  uint64_t tail;
  explicit ListMetadata();
public:
  void Encode(std::string *dst) override;
  rocksdb::Status Decode(std::string &bytes) override;
};

class RedisDB {
public:
  explicit RedisDB(Engine::Storage *storage);
  rocksdb::Status GetMetadata(RedisType type, Slice key, Metadata *metadata);
  rocksdb::Status Expire(Slice key, int timestamp);
  rocksdb::Status Del(Slice key);
  rocksdb::Status Exists(std::vector<Slice> keys, int *ret);
  rocksdb::Status TTL(Slice key, int *ttl);
  rocksdb::Status Type(Slice key, RedisType *type);

protected:
  Engine::Storage *storage;
  rocksdb::DB *db_;
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_;

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

class RWLocksGuard {
 public:
  explicit RWLocksGuard(RWLocks *locks, Slice key, bool is_write = true):
      locks_(locks),
      key_(key),
      is_write_(is_write) {
    is_write_ ? locks_->Lock(key_.ToString()):locks_->RLock(key_.ToString());
  };
  ~RWLocksGuard() {
    is_write_ ? locks_->UnLock(key_.ToString()):locks_->RUnLock(key_.ToString());
  }
 private:
  RWLocks *locks_ = nullptr;
  Slice key_;
  bool is_write_;
};
#endif