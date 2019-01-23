#include "redis_hash.h"
#include <iostream>
#include <rocksdb/status.h>

rocksdb::Status RedisHash::GetMetadata(Slice key, HashMetadata *metadata) {
  return RedisDB::GetMetadata(kRedisHash, key, metadata);
}

rocksdb::Status RedisHash::Size(Slice key, uint32_t *ret) {
  *ret = 0;

  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s;
  *ret = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisHash::Get(Slice key, Slice field, std::string *value) {
  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key;
  InternalKey(key, field, metadata.version).Encode(&sub_key);
  return db_->Get(read_options, sub_key, value);
}

rocksdb::Status RedisHash::IncrBy(Slice key, Slice field, long long increment, long long *ret) {
  bool exists = false;
  long long old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);

  LockGuard guard(storage_->GetLockManager(), key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key;
  InternalKey(key, field, metadata.version).Encode(&sub_key);
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
  batch.Put(sub_key, std::to_string(*ret));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::IncrByFloat(Slice key, Slice field, float increment, float *ret) {
  bool exists = false;
  float old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);

  LockGuard guard(storage_->GetLockManager(), key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key;
  InternalKey(key, field, metadata.version).Encode(&sub_key);
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
  batch.Put(sub_key, std::to_string(*ret));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::MGet(Slice key, std::vector<Slice> &fields, std::vector<std::string> *values) {
  values->clear();

  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) {
    return s;
  }

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key, value;
  for (const auto &field : fields) {
    InternalKey(key, field, metadata.version).Encode(&sub_key);
    db_->Get(read_options, sub_key, &value);
    values->emplace_back(value);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RedisHash::Set(Slice key, Slice field, Slice value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString()};
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(key, fvs, false, ret);
}

rocksdb::Status RedisHash::SetNX(Slice key, Slice field, Slice value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString()};
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(key, fvs, false, ret);
}

rocksdb::Status RedisHash::Delete(Slice key, std::vector<rocksdb::Slice> &fields, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);

  HashMetadata metadata;
  rocksdb::WriteBatch batch;
  LockGuard guard(storage_->GetLockManager(), key);
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string sub_key, value;
  for (const auto &field : fields) {
    InternalKey(key, field, metadata.version).Encode(&sub_key);
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
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::MSet(Slice key, std::vector<FieldValue> &field_values, bool nx, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);

  LockGuard guard(storage_->GetLockManager(), key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  int added = 0;
  rocksdb::WriteBatch batch;
  for (const auto &fv : field_values) {
    std::string sub_key;
    InternalKey(key, fv.field, metadata.version).Encode(&sub_key);
    if (metadata.size > 0) {
      std::string fieldValue;
      s = db_->Get(rocksdb::ReadOptions(), sub_key, &fieldValue);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (s.ok() && ((fieldValue == fv.value) || nx)) continue;
    }
    added++;
    batch.Put(sub_key, fv.value);
  }
  if (added > 0) {
    *ret = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::GetAll(Slice key, std::vector<FieldValue> *field_values, int type) {
  field_values->clear();

  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);
  std::string prefix_key;
  InternalKey(key, "", metadata.version).Encode(&prefix_key);
  for (iter->Seek(prefix_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       iter->Next()) {
    FieldValue fv;
    if (type == 1) { // only keys
      InternalKey ikey(iter->key());
      fv.field = ikey.GetSubKey().ToString();
    } else if (type == 2){ // only values
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

uint64_t RedisHash::Scan(Slice key,
                         const std::string &cursor,
                         const uint64_t &limit,
                         const std::string &field_prefix,
                         std::vector<std::string> *fields) {
  uint64_t cnt = 0;
  if (fields == nullptr) {
    return cnt;
  }

  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return cnt;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);

  std::string match_prefix_key;
  if (!field_prefix.empty()) {
    InternalKey(key, field_prefix, metadata.version).Encode(&match_prefix_key);
  } else {
    InternalKey(key, "", metadata.version).Encode(&match_prefix_key);
  }

  std::string start_key;
  if (!cursor.empty()) {
    InternalKey(key, cursor, metadata.version).Encode(&start_key);
  } else {
    start_key = match_prefix_key;
  }

  for (iter->Seek(start_key); iter->Valid() && cnt < limit; iter->Next()) {
    if (!cursor.empty() && iter->key() == start_key) {
      //if cursor is not empty, then we need to skip start_key
      //because we already return that key in the last scan
      continue;
    }
    if (!iter->key().starts_with(match_prefix_key)) {
      break;
    }
    InternalKey ikey(iter->key());
    fields->emplace_back(ikey.GetSubKey().ToString());
    cnt++;
  }

  delete iter;
  return cnt;
}