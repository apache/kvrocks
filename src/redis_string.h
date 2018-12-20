#pragma once

#include "redis_metadata.h"

typedef struct {
  Slice key;
  Slice value;
} StringPair;

class RedisString :public RedisDB {
public:
  explicit RedisString(Engine::Storage *storage, std::string ns) : RedisDB(storage, std::move(ns)) {}
  rocksdb::Status Append(Slice key, Slice value, int *ret);
  rocksdb::Status BitCount(Slice key, int start, int stop, int *ret);
  rocksdb::Status Get(Slice key, std::string *value);
  rocksdb::Status GetSet(Slice key, Slice new_value, std::string *old_value);
  rocksdb::Status Set(Slice key, Slice value);
  rocksdb::Status SetEX(Slice key, Slice value, int ttl);
  rocksdb::Status SetNX(Slice key, Slice value, int *ret);
  rocksdb::Status SetRange(Slice key, Slice value, int offset, int *ret);
  rocksdb::Status IncrBy(Slice key, int64_t increment, int64_t *ret);
  std::vector<rocksdb::Status> MGet(std::vector<Slice> keys, std::vector<std::string> *values);
  rocksdb::Status MSet(std::vector<StringPair> pairs, int ttl = 0);
  rocksdb::Status MSetNX(std::vector<StringPair> pairs, int *ret);

private:
  rocksdb::Status getValue(Slice key, std::string *raw_value, std::string *value = nullptr);
  rocksdb::Status updateValue(Slice key, Slice raw_value, Slice new_value);
};