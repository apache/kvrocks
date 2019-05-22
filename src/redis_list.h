#pragma once

#include <stdint.h>
#include <vector>
#include <string>

#include "redis_metadata.h"
#include "redis_encoding.h"

class RedisList :public RedisDB {
 public:
  explicit RedisList(Engine::Storage *storage, const std::string &ns) : RedisDB(storage, ns) {}
  rocksdb::Status Size(Slice user_key, uint32_t *ret);
  rocksdb::Status Trim(Slice user_key, int start, int stop);
  rocksdb::Status Set(Slice user_key, int index, Slice elem);
  rocksdb::Status Insert(Slice user_key, Slice pivot, Slice elem, bool before, int *ret);
  rocksdb::Status Pop(Slice user_key, std::string *elem, bool left);
  rocksdb::Status Rem(Slice user_key, int count, const Slice &elem, int *ret);
  rocksdb::Status Index(Slice user_key, int index, std::string *elem);
  rocksdb::Status RPopLPush(Slice src, Slice dst, std::string *elem);
  rocksdb::Status Push(Slice user_key, const std::vector<Slice> &elems, bool left, int *ret);
  rocksdb::Status PushX(Slice user_key, const std::vector<Slice> &elems, bool left, int *ret);
  rocksdb::Status Range(Slice user_key, int start, int stop, std::vector<std::string> *elems);

 private:
  rocksdb::Status GetMetadata(Slice ns_key, ListMetadata *metadata);
  rocksdb::Status push(Slice user_key, std::vector<Slice> elems, bool create_if_missing, bool left, int *ret);
};
