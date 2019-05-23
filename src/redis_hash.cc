#include "redis_hash.h"
#include <rocksdb/status.h>
#include <limits>
#include <iostream>

rocksdb::Status RedisHash::GetMetadata(const Slice &ns_key, HashMetadata *metadata) {
  return RedisDB::GetMetadata(kRedisHash, ns_key, metadata);
}

rocksdb::Status RedisHash::Size(const Slice &user_key, uint32_t *ret) {
  *ret = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;
  *ret = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisHash::Get(const Slice &user_key, const Slice &field, std::string *value) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key;
  InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
  return db_->Get(read_options, sub_key, value);
}

rocksdb::Status RedisHash::IncrBy(const Slice &user_key, const Slice &field, int64_t increment, int64_t *ret) {
  bool exists = false;
  int64_t old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key;
  InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
  if (s.ok()) {
    std::string value_bytes;
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      try {
        old_value = std::stoll(value_bytes);
      } catch (std::exception &e) {
        return rocksdb::Status::InvalidArgument(e.what());
      }
    }
    exists = true;
  }
  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN-old_value))
      || (increment > 0 && old_value > 0 && increment > (LLONG_MAX-old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *ret = old_value + increment;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  batch.Put(sub_key, std::to_string(*ret));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::IncrByFloat(const Slice &user_key, const Slice &field, float increment, float *ret) {
  bool exists = false;
  float old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key;
  InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
  if (s.ok()) {
    std::string value_bytes;
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      try {
        old_value = std::stof(value_bytes);
      } catch (std::exception &e) {
        return rocksdb::Status::InvalidArgument(e.what());
      }
    }
    exists = true;
  }
  if ((increment < 0 && old_value < 0 && increment < (std::numeric_limits<float>::lowest()-old_value))
      || (increment > 0 && old_value > 0 && increment > (std::numeric_limits<float>::max()-old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *ret = old_value + increment;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  batch.Put(sub_key, std::to_string(*ret));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::MGet(const Slice &user_key,
                                const std::vector<Slice> &fields,
                                std::vector<std::string> *values) {
  values->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key, value;
  for (const auto &field : fields) {
    InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
    db_->Get(read_options, sub_key, &value);
    values->emplace_back(value);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RedisHash::Set(const Slice &user_key, const Slice &field, const Slice &value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString()};
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(user_key, fvs, false, ret);
}

rocksdb::Status RedisHash::SetNX(const Slice &user_key, const Slice &field, Slice value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString()};
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(user_key, fvs, false, ret);
}

rocksdb::Status RedisHash::Delete(const Slice &user_key, const std::vector<Slice> &fields, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  HashMetadata metadata;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string sub_key, value;
  for (const auto &field : fields) {
    InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (s.ok()) {
      *ret += 1;
      batch.Delete(sub_key);
    }
  }
  // size was updated
  if (*ret > 0) {
    metadata.size -= *ret;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::MSet(const Slice &user_key, const std::vector<FieldValue> &field_values, bool nx, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  int added = 0;
  bool exists = false;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  for (const auto &fv : field_values) {
    std::string sub_key;
    InternalKey(ns_key, fv.field, metadata.version).Encode(&sub_key);
    if (metadata.size > 0) {
      std::string fieldValue;
      s = db_->Get(rocksdb::ReadOptions(), sub_key, &fieldValue);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (s.ok()) {
        if (((fieldValue == fv.value) || nx)) continue;
        exists = true;
      }
    }
    if (!exists) added++;
    batch.Put(sub_key, fv.value);
  }
  if (added > 0) {
    *ret = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::GetAll(const Slice &user_key, std::vector<FieldValue> *field_values, int type) {
  field_values->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);
  std::string prefix_key;
  InternalKey(ns_key, "", metadata.version).Encode(&prefix_key);
  for (iter->Seek(prefix_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       iter->Next()) {
    FieldValue fv;
    if (type == 1) {  // only keys
      InternalKey ikey(iter->key());
      fv.field = ikey.GetSubKey().ToString();
    } else if (type == 2) {  // only values
      fv.value = iter->value().ToString();
    } else {
      InternalKey ikey(iter->key());
      fv.field = ikey.GetSubKey().ToString();
      fv.value = iter->value().ToString();
    }
    field_values->emplace_back(fv);
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisHash::Scan(const Slice &user_key,
                                const std::string &cursor,
                                uint64_t limit,
                                const std::string &field_prefix,
                                std::vector<std::string> *fields) {
  return RedisSubKeyScanner::Scan(kRedisHash, user_key, cursor, limit, field_prefix, fields);
}
