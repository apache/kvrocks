#pragma once

#include <string>
#include <vector>
#include <rocksdb/status.h>

#include "redis_encoding.h"
#include "redis_metadata.h"

typedef struct FieldValue {
  std::string field;
  std::string value;
} FieldValue;

class RedisHash : public RedisDB {
public:
  RedisHash(Engine::Storage *storage, std::string ns) : RedisDB(storage, std::move(ns)) {}
  rocksdb::Status Size(Slice key, uint32_t *ret);
  rocksdb::Status IncrBy(Slice key, Slice field, long long increment, long long *ret);
  rocksdb::Status IncrByFloat(Slice key, Slice field, float increment, float *ret);
  rocksdb::Status Get(Slice key, Slice field, std::string *value);
  rocksdb::Status MGet(Slice key, std::vector<Slice> &fields, std::vector<std::string> *values);
  rocksdb::Status Delete(Slice key, std::vector<Slice> &fields, int *ret);
  rocksdb::Status Set(Slice key, Slice field, Slice value, int *ret);
  rocksdb::Status SetNX(Slice key, Slice field, Slice value, int *ret);
  rocksdb::Status MSet(Slice key, std::vector<FieldValue> &field_values, bool nx, int *ret);
  rocksdb::Status GetAll(Slice key, std::vector<FieldValue> *field_values, int type = 0);
  uint64_t Scan(Slice key,
                const std::string &cursor,
                const uint64_t &limit,
                const std::string &prefix,
                std::vector<std::string> *fields);
private:
  rocksdb::Status GetMetadata(Slice key, HashMetadata *metadata);
};