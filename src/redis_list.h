//
// Created by hulk on 2018/9/29.
//

#ifndef KVROCKS_T_LIST_H
#define KVROCKS_T_LIST_H

#include "redis_metadata.h"
#include "redis_encoding.h"
#include <stdint.h>

class RedisList :public RedisDB {
public:
  explicit RedisList(Engine::Storage *storage, std::string ns) : RedisDB(storage, std::move(ns)) {}
  rocksdb::Status Size(Slice key, uint32_t *ret);
  rocksdb::Status Push(Slice key, std::vector<Slice> elems, bool left, int *ret);
  rocksdb::Status PushX(Slice key, std::vector<Slice> elems, bool left, int *ret);
  rocksdb::Status Pop(Slice key, std::string *elem, bool left);
  rocksdb::Status Index(Slice key, int index, std::string *elem);
  rocksdb::Status Range(Slice key, int start, int stop, std::vector<std::string> *elems);
  rocksdb::Status Set(Slice key, int index, Slice elem);
  rocksdb::Status Trim(Slice key, int start, int stop);
  rocksdb::Status RPopLPush(Slice src, Slice dst, std::string *elem);

private:
  rocksdb::Status GetMetadata(Slice key, ListMetadata *metadata);
  rocksdb::Status push(Slice key, std::vector<Slice> elems, bool create_if_missing, bool left, int *ret);
};

#endif //KVROCKS_T_LIST_H
