#pragma once

#include <string>
#include <vector>

#include "redis_metadata.h"

class RedisSet : public RedisSubKeyScanner {
 public:
  explicit RedisSet(Engine::Storage *storage, std::string ns)
      : RedisSubKeyScanner(storage, std::move(ns)) {}

  rocksdb::Status Card(Slice key, int *ret);
  rocksdb::Status IsMember(Slice key, Slice member, int *ret);
  rocksdb::Status Add(Slice key, const std::vector<Slice> &members, int *ret);
  rocksdb::Status Remove(Slice key, std::vector<Slice> members, int *ret);
  rocksdb::Status Members(Slice key, std::vector<std::string> *members);
  rocksdb::Status Move(Slice src, Slice dst, Slice member, int *ret);
  rocksdb::Status Take(Slice key, std::vector<std::string> *members, int count, bool pop);
  rocksdb::Status Diff(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Union(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Inter(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Overwrite(Slice key, const std::vector<std::string> &members);
  rocksdb::Status DiffStore(const Slice &dst, const std::vector<Slice> &keys, int *ret);
  rocksdb::Status UnionStore(const Slice &dst, const std::vector<Slice> &keys, int *ret);
  rocksdb::Status InterStore(const Slice &dst, const std::vector<Slice> &keys, int *ret);
  uint64_t Scan(Slice key,
                const std::string &cursor,
                const uint64_t &limit,
                const std::string &member_prefix,
                std::vector<std::string> *members);

 private:
  rocksdb::Status GetMetadata(Slice key, SetMetadata *metadata);
};
