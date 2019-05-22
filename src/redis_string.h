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
  rocksdb::Status Append(Slice user_key, Slice value, int *ret);
  rocksdb::Status Get(Slice user_key, std::string *value);
  rocksdb::Status GetSet(Slice user_key, Slice new_value, std::string *old_value);
  rocksdb::Status Set(Slice user_key, Slice value);
  rocksdb::Status SetEX(Slice user_key, Slice value, int ttl);
  rocksdb::Status SetNX(Slice user_key, Slice value, int ttl, int *ret);
  rocksdb::Status SetXX(Slice user_key, Slice value, int ttl, int *ret);
  rocksdb::Status SetRange(Slice user_key, int offset, Slice value, int *ret);
  rocksdb::Status IncrBy(Slice user_key, int64_t increment, int64_t *ret);
  rocksdb::Status IncrByFloat(Slice user_key, float increment, float *ret);
  std::vector<rocksdb::Status> MGet(const std::vector<Slice> &keys, std::vector<std::string> *values);
  rocksdb::Status MSet(const std::vector<StringPair> &pairs, int ttl = 0);
  rocksdb::Status MSetNX(const std::vector<StringPair> &pairs, int ttl, int *ret);

 private:
  rocksdb::Status getValue(Slice ns_key, std::string *raw_value, std::string *value = nullptr);
  rocksdb::Status updateValue(Slice ns_key, Slice raw_value, Slice new_value);
};
