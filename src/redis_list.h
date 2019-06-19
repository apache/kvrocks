#pragma once

#include <stdint.h>
#include <vector>
#include <string>

#include "redis_db.h"
#include "redis_metadata.h"
#include "encoding.h"

namespace Redis {
class List : public Database {
 public:
  explicit List(Engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Size(const Slice &user_key, uint32_t *ret);
  rocksdb::Status Trim(const Slice &user_key, int start, int stop);
  rocksdb::Status Set(const Slice &user_key, int index, Slice elem);
  rocksdb::Status Insert(const Slice &user_key, const Slice &pivot, const Slice &elem, bool before, int *ret);
  rocksdb::Status Pop(const Slice &user_key, std::string *elem, bool left);
  rocksdb::Status Rem(const Slice &user_key, int count, const Slice &elem, int *ret);
  rocksdb::Status Index(const Slice &user_key, int index, std::string *elem);
  rocksdb::Status RPopLPush(const Slice &src, const Slice &dst, std::string *elem);
  rocksdb::Status Push(const Slice &user_key, const std::vector<Slice> &elems, bool left, int *ret);
  rocksdb::Status PushX(const Slice &user_key, const std::vector<Slice> &elems, bool left, int *ret);
  rocksdb::Status Range(const Slice &user_key, int start, int stop, std::vector<std::string> *elems);

 private:
  rocksdb::Status GetMetadata(const Slice &ns_key, ListMetadata *metadata);
  rocksdb::Status push(const Slice &user_key, std::vector<Slice> elems, bool create_if_missing, bool left, int *ret);
};
}  // namespace Redis
