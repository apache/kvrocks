#pragma once

#include <string>
#include <vector>

#include <rocksdb/status.h>

#include "redis_db.h"
#include "encoding.h"
#include "redis_metadata.h"

typedef struct FieldValue {
  std::string field;
  std::string value;
  uint32_t expire;
} FieldValue;

void ExtractFieldRawValue(const std::string &field_raw_value, std::string *field_value, uint32_t *field_expire);
void ComposeFieldRawValue(const std::string &field_value, uint32_t field_expire, std::string *field_raw_value);
bool Expired(uint32_t expire);

namespace Redis {
class Hash : public SubKeyScanner {
 public:
  Hash(Engine::Storage *storage, const std::string &ns,
       bool support_field_expire = false) : SubKeyScanner(storage, ns) {
    support_field_expire_ = support_field_expire;
  }
  // When support field expire, the size in the metadata is inaccurate and will contain expired fields.
  // To get the exact size of fields that are not expired, we must traverse all fields, which is a
  // time-consuming operation, so we add a parameter to control whether we get the exact size.
  rocksdb::Status Size(const Slice &user_key, uint32_t *ret, bool exact = false);
  rocksdb::Status Get(const Slice &user_key, const Slice &field, std::string *value);
  // when support field expire:
  //  if key or field not found(or expired), field_ttl will be return -2
  //  if not set field expire, field_ttl will be return -1
  rocksdb::Status Get(const Slice &user_key, const Slice &field, std::string *value, int32_t *field_ttl);
  rocksdb::Status Set(const Slice &user_key, const Slice &field, const Slice &value, int *ret);
  rocksdb::Status Set(const Slice &user_key, const Slice &field, const Slice &value, int32_t field_ttl, int *ret);
  rocksdb::Status SetNX(const Slice &user_key, const Slice &field, Slice value, int *ret);
  rocksdb::Status Delete(const Slice &user_key, const std::vector<Slice> &fields, int *ret);
  rocksdb::Status IncrBy(const Slice &user_key, const Slice &field, int64_t increment, int64_t *ret);
  rocksdb::Status IncrByFloat(const Slice &user_key, const Slice &field, double increment, double *ret);
  rocksdb::Status MSet(const Slice &user_key, const std::vector<FieldValue> &field_values, bool nx, int *ret);
  rocksdb::Status MGet(const Slice &user_key, const std::vector<Slice> &fields,
                       std::vector<std::string> *values, std::vector<rocksdb::Status> *statuses);
  rocksdb::Status MGet(const Slice &user_key, const std::vector<Slice> &fields,
                       std::vector<std::string> *values, std::vector<int32_t> *field_ttls,
                       std::vector<rocksdb::Status> *statuses);
  rocksdb::Status GetAll(const Slice &user_key, std::vector<FieldValue> *field_values);
  rocksdb::Status Scan(const Slice &user_key,
                       const std::string &cursor,
                       uint64_t limit,
                       const std::string &field_prefix,
                       std::vector<std::string> *fields,
                       std::vector<std::string> *values = nullptr);

 private:
  bool support_field_expire_;
  rocksdb::Status GetMetadata(const Slice &ns_key, Metadata *metadata);
};
}  // namespace Redis
