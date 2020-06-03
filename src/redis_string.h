#pragma once

#include <vector>
#include <string>

#include "redis_db.h"
#include "redis_metadata.h"

typedef struct {
  Slice key;
  Slice value;
} StringPair;

namespace Redis {

const int STRING_HDR_SIZE = 5;

class String : public Database {
 public:
  explicit String(Engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Append(const std::string &user_key, const std::string &value, int *ret);
  rocksdb::Status Get(const std::string &user_key, std::string *value);
  rocksdb::Status GetSet(const std::string &user_key, const std::string &new_value, std::string *old_value);
  rocksdb::Status Set(const std::string &user_key, const std::string &value);
  rocksdb::Status SetEX(const std::string &user_key, const std::string &value, int ttl);
  rocksdb::Status SetNX(const std::string &user_key, const std::string &value, int ttl, int *ret);
  rocksdb::Status SetXX(const std::string &user_key, const std::string &value, int ttl, int *ret);
  rocksdb::Status SetRange(const std::string &user_key, int offset, const std::string &value, int *ret);
  rocksdb::Status IncrBy(const std::string &user_key, int64_t increment, int64_t *ret);
  rocksdb::Status IncrByFloat(const std::string &user_key, float increment, float *ret);
  std::vector<rocksdb::Status> MGet(const std::vector<Slice> &keys, std::vector<std::string> *values);
  rocksdb::Status MSet(const std::vector<StringPair> &pairs, int ttl = 0);
  rocksdb::Status MSetNX(const std::vector<StringPair> &pairs, int ttl, int *ret);

 private:
  rocksdb::Status getValue(const std::string &ns_key, std::string *value);
  rocksdb::Status getRawValue(const std::string &ns_key, std::string *raw_value);
  rocksdb::Status updateRawValue(const std::string &ns_key, const std::string &raw_value);
};

}  // namespace Redis
