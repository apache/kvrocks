#pragma once

#include <rocksdb/status.h>
#include <string>
#include <vector>

#include "redis_db.h"
#include "encoding.h"
#include "redis_metadata.h"
#include "redis_hash_compressed.h"

namespace Redis {
class Hash : public SubKeyScanner {
 public:
  Hash(Engine::Storage *storage, const std::string &ns, int small_hash_compress_to_meta_threshold = 0) : SubKeyScanner(
      storage,
      ns) {
    small_hash_compress_to_meta_threshold_ = small_hash_compress_to_meta_threshold;
  }
  rocksdb::Status Size(const Slice &user_key, uint32_t *ret);
  rocksdb::Status Get(const Slice &user_key, const Slice &field, std::string *value);
  rocksdb::Status Set(const Slice &user_key, const Slice &field, const Slice &value, int *ret);
  rocksdb::Status SetNX(const Slice &user_key, const Slice &field, Slice value, int *ret);
  rocksdb::Status Delete(const Slice &user_key, const std::vector<Slice> &fields, int *ret);
  rocksdb::Status IncrBy(const Slice &user_key, const Slice &field, int64_t increment, int64_t *ret);
  rocksdb::Status IncrByFloat(const Slice &user_key, const Slice &field, float increment, float *ret);
  rocksdb::Status MSet(const Slice &user_key, const std::vector<FieldValue> &field_values, bool nx, int *ret);
  rocksdb::Status MGet(const Slice &user_key, const std::vector<Slice> &fields, std::vector<std::string> *values);
  rocksdb::Status GetAll(const Slice &user_key,
                         std::vector<FieldValue> *field_values,
                         HashFetchType type = HashFetchType::kAll);
  rocksdb::Status Scan(const Slice &user_key,
                       const std::string &cursor,
                       uint64_t limit,
                       const std::string &field_prefix,
                       std::vector<std::string> *fields);
 private:
  rocksdb::Status GetMetadata(const Slice &ns_key, HashMetadata *metadata);
  int small_hash_compress_to_meta_threshold_ = 0;
};
}  // namespace Redis
