#include "redis_string.h"
#include <utility>
#include <string>
#include <limits>

namespace Redis {

rocksdb::Status String::getRawValue(const std::string &ns_key, std::string *raw_value) {
  raw_value->clear();

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Status s = db_->Get(read_options, metadata_cf_handle_, ns_key, raw_value);
  if (!s.ok()) return s;

  Metadata metadata(kRedisNone, false);
  metadata.Decode(*raw_value);
  if (metadata.Expired()) {
    raw_value->clear();
    return rocksdb::Status::NotFound("the key was expired");
  }
  if (metadata.Type() != kRedisString && metadata.size > 0) {
    return rocksdb::Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status String::getValue(const std::string &ns_key, std::string *value) {
  value->clear();
  std::string raw_value;
  auto s = getRawValue(ns_key, &raw_value);
  if (!s.ok()) return s;
  *value = raw_value.substr(STRING_HDR_SIZE, raw_value.size()-STRING_HDR_SIZE);
  return rocksdb::Status::OK();
}

rocksdb::Status String::updateRawValue(const std::string &ns_key, const std::string &raw_value) {
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisString);
  batch.PutLogData(log_data.Encode());
  batch.Put(metadata_cf_handle_, ns_key, raw_value);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status String::Append(const std::string &user_key, const std::string &value, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  std::string raw_value;
  rocksdb::Status s = getRawValue(ns_key, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    Metadata metadata(kRedisString, false);
    metadata.Encode(&raw_value);
  }
  raw_value.append(value);
  *ret = static_cast<int>(raw_value.size()-STRING_HDR_SIZE);
  return updateRawValue(ns_key, raw_value);
}

std::vector<rocksdb::Status> String::MGet(const std::vector<Slice> &keys, std::vector<std::string> *values) {
  std::string ns_key;
  std::string value;
  std::vector<rocksdb::Status> statuses;
  for (size_t i = 0; i < keys.size(); i++) {
    AppendNamespacePrefix(keys[i], &ns_key);
    statuses.emplace_back(getValue(ns_key, &value));
    values->emplace_back(value);
  }
  return statuses;
}

rocksdb::Status String::Get(const std::string &user_key, std::string *value) {
  std::vector<Slice> keys{user_key};
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses = MGet(keys, &values);
  *value = std::move(values[0]);
  return statuses[0];
}

rocksdb::Status String::GetSet(const std::string &user_key, const std::string &new_value, std::string *old_value) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = getValue(ns_key, old_value);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string raw_value;
  Metadata metadata(kRedisString, false);
  metadata.Encode(&raw_value);
  raw_value.append(new_value);
  return updateRawValue(ns_key, raw_value);
}

rocksdb::Status String::Set(const std::string &user_key, const std::string &value) {
  std::vector<StringPair> pairs{StringPair{user_key, value}};
  return MSet(pairs, 0);
}

rocksdb::Status String::SetEX(const std::string &user_key, const std::string &value, int ttl) {
  std::vector<StringPair> pairs{StringPair{user_key, value}};
  return MSet(pairs, ttl);
}

rocksdb::Status String::SetNX(const std::string &user_key, const std::string &value, int ttl, int *ret) {
  std::vector<StringPair> pairs{StringPair{user_key, value}};
  return MSetNX(pairs, ttl, ret);
}

rocksdb::Status String::SetXX(const std::string &user_key, const std::string &value, int ttl, int *ret) {
  *ret = 0;
  int exists = 0;
  uint32_t expire = 0;
  if (ttl > 0) {
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    expire = uint32_t(now) + ttl;
  }

  std::string ns_key;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisString);
  batch.PutLogData(log_data.Encode());

  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  Exists({user_key}, &exists);
  if (exists != 1) return rocksdb::Status::OK();

  *ret = 1;
  std::string raw_value;
  Metadata metadata(kRedisString, false);
  metadata.expire = expire;
  metadata.Encode(&raw_value);
  raw_value.append(value);
  return updateRawValue(ns_key, raw_value);
}

rocksdb::Status String::SetRange(const std::string &user_key, int offset, const std::string &value, int *ret) {
  int size;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  std::string raw_value;
  rocksdb::Status s = getRawValue(ns_key, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;

  if (s.IsNotFound()) {
    Metadata metadata(kRedisString, false);
    metadata.Encode(&raw_value);
  }
  size = static_cast<int>(raw_value.size());
  offset += STRING_HDR_SIZE;
  if (offset > size) {
    // padding the value with zero byte while offset is longer than value size
    int paddings = offset-size;
    raw_value.append(paddings, '\0');
  }
  if (offset + static_cast<int>(value.size()) >= size) {
    raw_value = raw_value.substr(0, offset);
    raw_value.append(value);
  } else {
    for (size_t i = 0; i < value.size(); i++) {
      raw_value[offset+i] = value[i];
    }
  }
  *ret = static_cast<int>(raw_value.size()-STRING_HDR_SIZE);
  return updateRawValue(ns_key, raw_value);
}

rocksdb::Status String::IncrBy(const std::string &user_key, int64_t increment, int64_t *ret) {
  std::string ns_key, value;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  std::string raw_value;
  rocksdb::Status s = getRawValue(ns_key, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    Metadata metadata(kRedisString, false);
    metadata.Encode(&raw_value);
  }

  value = raw_value.substr(STRING_HDR_SIZE, raw_value.size()-STRING_HDR_SIZE);
  int64_t n = 0;
  if (!value.empty()) {
    try {
      n = std::stoll(value);
    } catch(std::exception &e) {
      return rocksdb::Status::InvalidArgument("value is not an integer or out of range");
    }
  }
  if ((increment < 0 && n <= 0 && increment < (LLONG_MIN-n))
      || (increment > 0 && n >= 0 && increment > (LLONG_MAX-n))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }
  n += increment;
  *ret = n;

  raw_value = raw_value.substr(0, STRING_HDR_SIZE);
  raw_value.append(std::to_string(n));
  return updateRawValue(ns_key, raw_value);
}

rocksdb::Status String::IncrByFloat(const std::string &user_key, float increment, float *ret) {
  std::string ns_key, value;
  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  std::string raw_value;
  rocksdb::Status s = getRawValue(ns_key, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;

  if (s.IsNotFound()) {
    Metadata metadata(kRedisString, false);
    metadata.Encode(&raw_value);
  }
  value = raw_value.substr(STRING_HDR_SIZE, raw_value.size()-STRING_HDR_SIZE);
  float n = 0;
  if (!value.empty()) {
    try {
      n = std::stof(value);
    } catch(std::exception &e) {
      return rocksdb::Status::InvalidArgument("value is not an integer");
    }
  }
  auto float_min = std::numeric_limits<float>::min();
  auto float_max = std::numeric_limits<float>::max();
  if ((increment < 0 && n < 0 && increment < (float_min-n))
      || (increment > 0 && n > 0 && increment > (float_max-n))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }
  n += increment;
  *ret = n;

  raw_value = raw_value.substr(0, STRING_HDR_SIZE);
  raw_value.append(std::to_string(n));
  return updateRawValue(ns_key, raw_value);
}

rocksdb::Status String::MSet(const std::vector<StringPair> &pairs, int ttl) {
  uint32_t expire = 0;
  if (ttl > 0) {
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    expire = uint32_t(now) + ttl;
  }

  // Data race, key string maybe overwrite by other key while didn't lock the key here,
  // to improve the set performance
  std::string ns_key;
  for (const auto &pair : pairs) {
    std::string bytes;
    Metadata metadata(kRedisString, false);
    metadata.expire = expire;
    metadata.Encode(&bytes);
    bytes.append(pair.value.ToString());
    rocksdb::WriteBatch batch;
    WriteBatchLogData log_data(kRedisString);
    batch.PutLogData(log_data.Encode());
    AppendNamespacePrefix(pair.key, &ns_key);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
    LockGuard guard(storage_->GetLockManager(), ns_key);
    auto s = storage_->Write(rocksdb::WriteOptions(), &batch);
    if (!s.ok()) return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status String::MSetNX(const std::vector<StringPair> &pairs, int ttl, int *ret) {
  *ret = 0;

  uint32_t expire = 0;
  if (ttl > 0) {
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    expire = uint32_t(now) + ttl;
  }

  int exists;
  std::string ns_key;
  for (StringPair pair : pairs) {
    AppendNamespacePrefix(pair.key, &ns_key);
    LockGuard guard(storage_->GetLockManager(), ns_key);
    if (Exists({pair.key}, &exists).ok() && exists == 1) {
      return rocksdb::Status::OK();
    }
    std::string bytes;
    Metadata metadata(kRedisString, false);
    metadata.expire = expire;
    metadata.Encode(&bytes);
    bytes.append(pair.value.ToString());
    rocksdb::WriteBatch batch;
    WriteBatchLogData log_data(kRedisString);
    batch.PutLogData(log_data.Encode());
    batch.Put(metadata_cf_handle_, ns_key, bytes);
    auto s = storage_->Write(rocksdb::WriteOptions(), &batch);
    if (!s.ok()) return s;
  }
  *ret = 1;
  return rocksdb::Status::OK();
}
}  // namespace Redis
