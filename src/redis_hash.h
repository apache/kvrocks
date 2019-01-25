#pragma once

#include <rocksdb/status.h>
#include <string>
#include <vector>

#include "redis_encoding.h"
#include "redis_metadata.h"

typedef struct FieldValue {
  std::string field;
  std::string value;
} FieldValue;

class RedisHash : public RedisSubKeyScanner {
 public:
  RedisHash(Engine::Storage *storage, std::string ns) : RedisSubKeyScanner(storage, std::move(ns)) {}
  rocksdb::Status Size(Slice key, uint32_t *ret);
  rocksdb::Status Get(Slice key, Slice field, std::string *value);
  rocksdb::Status Set(Slice key, Slice field, Slice value, int *ret);
  rocksdb::Status SetNX(Slice key, Slice field, Slice value, int *ret);
  rocksdb::Status Delete(Slice key, const std::vector<Slice> &fields, int *ret);
  rocksdb::Status IncrBy(Slice key, Slice field, int64_t increment, int64_t *ret);
  rocksdb::Status IncrByFloat(Slice key, Slice field, float increment, float *ret);
  rocksdb::Status MSet(Slice key, const std::vector<FieldValue> &field_values, bool nx, int *ret);
  rocksdb::Status MGet(Slice key, const std::vector<Slice> &fields, std::vector<std::string> *values);
  rocksdb::Status GetAll(Slice key, std::vector<FieldValue> *field_values, int type = 0);
  uint64_t Scan(Slice key,
                const std::string &cursor,
                const uint64_t &limit,
                const std::string &field_prefix,
                std::vector<std::string> *fields);
 private:
  rocksdb::Status GetMetadata(Slice key, HashMetadata *metadata);
};
