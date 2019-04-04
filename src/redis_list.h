#pragma once

#include <stdint.h>
#include <vector>
#include <string>

#include "redis_metadata.h"
#include "redis_encoding.h"

class RedisList :public RedisDB {
 public:
  explicit RedisList(Engine::Storage *storage, const std::string &ns) : RedisDB(storage, ns) {}
  rocksdb::Status Size(Slice key, uint32_t *ret);
  rocksdb::Status Trim(Slice key, int start, int stop);
  rocksdb::Status Set(Slice key, int index, Slice elem);
  rocksdb::Status Insert(Slice key, Slice pivot, Slice elem, bool before, int *ret);
  rocksdb::Status Pop(Slice key, std::string *elem, bool left);
  rocksdb::Status Rem(Slice key, int count, const Slice &elem, int *ret);
  rocksdb::Status Index(Slice key, int index, std::string *elem);
  rocksdb::Status RPopLPush(Slice src, Slice dst, std::string *elem);
  rocksdb::Status Push(Slice key, const std::vector<Slice> &elems, bool left, int *ret);
  rocksdb::Status PushX(Slice key, const std::vector<Slice> &elems, bool left, int *ret);
  rocksdb::Status Range(Slice key, int start, int stop, std::vector<std::string> *elems);

 private:
  rocksdb::Status GetMetadata(Slice key, ListMetadata *metadata);
  rocksdb::Status push(Slice key, std::vector<Slice> elems, bool create_if_missing, bool left, int *ret);
};
