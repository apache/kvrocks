//
// Created by hulk on 2018/10/9.
//

#ifndef KVROCKS_T_SET_H
#define KVROCKS_T_SET_H

#include "redis_metadata.h"

class RedisSet : public RedisDB {
public:
  explicit RedisSet(Engine::Storage *storage, std::string ns) : RedisDB(storage, std::move(ns)) {}
  rocksdb::Status Add(Slice key, std::vector<Slice> members, int *ret);
  rocksdb::Status Remove(Slice key, std::vector<Slice> members, int *ret);
  rocksdb::Status Card(Slice key, int *ret);
  rocksdb::Status Members(Slice key, std::vector<std::string> *members);
  rocksdb::Status IsMember(Slice key, Slice member, int *ret);
  rocksdb::Status Take(Slice key, std::vector<std::string> *members, int count, bool pop);
  rocksdb::Status Move(Slice src, Slice dst, Slice member, int *ret);
private:
  rocksdb::Status GetMetadata(Slice key, SetMetadata *metadata);
};

#endif //KVROCKS_T_SET_H
