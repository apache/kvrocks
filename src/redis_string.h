#pragma once

#include <vector>
#include <string>

#include "redis_metadata.h"

typedef struct {
  Slice key;
  Slice value;
} StringPair;

class RedisString :public RedisDB {
 public:
  explicit RedisString(Engine::Storage *storage, const std::string &ns) : RedisDB(storage, ns) {}
  rocksdb::Status Append(const Slice &user_key, const Slice &value, int *ret);
  rocksdb::Status Get(const Slice &user_key, std::string *value);
  rocksdb::Status GetSet(const Slice &user_key, const Slice &new_value, std::string *old_value);
  rocksdb::Status Set(const Slice &user_key, const Slice &value);
  rocksdb::Status SetEX(const Slice &user_key, const Slice &value, int ttl);
  rocksdb::Status SetNX(const Slice &user_key, const Slice &value, int ttl, int *ret);
  rocksdb::Status SetXX(const Slice &user_key, const Slice &value, int ttl, int *ret);
  rocksdb::Status SetRange(const Slice &user_key, int offset, Slice value, int *ret);
  rocksdb::Status IncrBy(const Slice &user_key, int64_t increment, int64_t *ret);
  rocksdb::Status IncrByFloat(const Slice &user_key, float increment, float *ret);
  std::vector<rocksdb::Status> MGet(const std::vector<Slice> &keys, std::vector<std::string> *values);
  rocksdb::Status MSet(const std::vector<StringPair> &pairs, int ttl = 0);
  rocksdb::Status MSetNX(const std::vector<StringPair> &pairs, int ttl, int *ret);

 private:
  rocksdb::Status getValue(const Slice &ns_key, std::string *raw_value, std::string *value = nullptr);
  rocksdb::Status updateValue(const Slice &ns_key, const Slice &raw_value, const Slice &new_value);
};
