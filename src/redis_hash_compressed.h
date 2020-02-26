#pragma once

#include <rocksdb/status.h>
#include <string>
#include <vector>

#include "redis_db.h"
#include "encoding.h"
#include "redis_metadata.h"

typedef struct FieldValue {
  std::string field;
  std::string value;
} FieldValue;

enum class HashFetchType {
  kAll = 0,
  kOnlyKey = 1,
  kOnlyValue = 2
};

namespace Redis {
class HashCompressed : public SubKeyScanner {
 public:
  HashCompressed(Engine::Storage *storage, const std::string &ns) : SubKeyScanner(storage, ns) {}
  rocksdb::Status Get(const HashMetadata &metadata, const Slice &field, std::string *value);
  rocksdb::Status Delete(HashMetadata *metadata, const Slice &ns_key, const std::vector<Slice> &fields, int *ret);
  rocksdb::Status IncrBy(HashMetadata *metadata,
                         const Slice &ns_key, const Slice &field, int64_t increment, int64_t *ret);
  rocksdb::Status IncrByFloat(HashMetadata *metadata,
                              const Slice &ns_key, const Slice &field, float increment, float *ret);
  rocksdb::Status MSet(HashMetadata *metadata,
                       const Slice &ns_key, const std::vector<FieldValue> &field_values, bool nx, int *ret);
  rocksdb::Status MGet(const HashMetadata &metadata, const std::vector<Slice> &fields, std::vector<std::string> *values);
  rocksdb::Status GetAll(const HashMetadata &metadata, std::vector<FieldValue> *field_values);
  rocksdb::Status Scan(const HashMetadata &metadata,
                       const std::string &cursor,
                       uint64_t limit,
                       const std::string &field_prefix,
                       std::vector<std::string> *fields);

  rocksdb::Status Uncompressed(HashMetadata *metadata,
                               const Slice &ns_key, const std::vector<FieldValue> &field_values);
 private:
  rocksdb::Status GetAllFieldValues(const HashMetadata &metadata,
                                    std::vector<FieldValue> *field_values);
  rocksdb::Status EncodeFieldValues(std::vector<FieldValue> *field_values, std::string *field_value_bytes);
  static bool compareFieldValue(FieldValue fv1, FieldValue fv2);

};
}  // namespace Redis
