#include "redis_hash.h"
#include <utility>
#include <limits>
#include <cmath>
#include <iostream>
#include <memory>
#include <rocksdb/status.h>

// field raw value format is |expire|field_value|
void ExtractFieldRawValue(const std::string &field_raw_value, std::string *field_value, uint32_t *field_expire) {
  Slice input(field_raw_value);
  GetFixed32(&input, field_expire);
  *field_value = input.ToString();
}

void ComposeFieldRawValue(const std::string &field_value, uint32_t field_expire, std::string *field_raw_value) {
  PutFixed32(field_raw_value, field_expire);
  field_raw_value->append(field_value);
}

bool Expired(uint32_t field_expire) {
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  if (field_expire > 0 && field_expire < now) {
    return true;
  }
  return false;
}

int32_t GetTTL(uint32_t field_expire) {
  if (field_expire == 0) {
    return -1;
  }
  int64_t now;
  int32_t ttl;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  ttl = field_expire - now;
  if (ttl < 0) {
    return -2;
  }
  return ttl;
}

namespace Redis {
rocksdb::Status Hash::GetMetadata(const Slice &ns_key, Metadata *metadata) {
  if (support_field_expire_) {
    return Database::GetMetadata(kRedisExHash, ns_key, metadata);
  }
  return Database::GetMetadata(kRedisHash, ns_key, metadata);
}

rocksdb::Status Hash::Size(const Slice &user_key, uint32_t *ret, bool exact) {
  *ret = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  Metadata *p;
  if (support_field_expire_) {
    p = new ExHashMetadata(false);
  } else {
    p = new HashMetadata(false);
  }
  std::unique_ptr<Metadata> metadata(p);

  rocksdb::Status s = GetMetadata(ns_key, metadata.get());
  if (!s.ok()) return s;
  if (support_field_expire_ && exact) {
    LatestSnapShot ss(db_);
    rocksdb::ReadOptions read_options;
    read_options.snapshot = ss.GetSnapShot();
    read_options.fill_cache = false;
    auto iter = db_->NewIterator(read_options);
    std::string prefix_key;
    InternalKey(ns_key, "", metadata->version, storage_->IsSlotIdEncoded()).Encode(&prefix_key);
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    for (iter->Seek(prefix_key);
         iter->Valid() && iter->key().starts_with(prefix_key);
         iter->Next()) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      std::string field_value;
      uint32_t field_expire;
      ExtractFieldRawValue(iter->value().ToString(), &field_value, &field_expire);
      // skip expired field
      if (field_expire != 0 && field_expire < now) {
        continue;
      }
      (*ret)++;
    }
    delete iter;
  } else {
    *ret = metadata->size;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Get(const Slice &user_key, const Slice &field, std::string *value) {
  int32_t ttl;
  return Get(user_key, field, value, &ttl);
}

rocksdb::Status Hash::Get(const Slice &user_key, const Slice &field, std::string *value, int32_t *field_ttl) {
  *field_ttl = -2;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  Metadata *p;
  if (support_field_expire_) {
    p = new ExHashMetadata(false);
  } else {
    p = new HashMetadata(false);
  }
  std::unique_ptr<Metadata> metadata(p);

  rocksdb::Status s = GetMetadata(ns_key, metadata.get());
  if (!s.ok()) return s;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key;
  InternalKey(ns_key, field, metadata->version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  std::string field_raw_value;

  s = db_->Get(read_options, sub_key, &field_raw_value);
  if (!s.ok()) return s;
  // Get field and check ttl
  if (support_field_expire_) {
    uint32_t field_expire;
    ExtractFieldRawValue(field_raw_value, value, &field_expire);
    if (Expired(field_expire)) {
      return rocksdb::Status::NotFound(kErrMsgFieldExpired);
    }
    *field_ttl = GetTTL(field_expire);
  } else {
    *value = field_raw_value;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Hash::IncrBy(const Slice &user_key, const Slice &field, int64_t increment, int64_t *ret) {
  bool exists = false;
  int64_t old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);

  Metadata *p;
  if (support_field_expire_) {
    p = new ExHashMetadata();
  } else {
    p = new HashMetadata();
  }
  std::unique_ptr<Metadata> metadata(p);

  rocksdb::Status s = GetMetadata(ns_key, metadata.get());
  if (!s.ok() && !s.IsNotFound()) return s;
  std::string sub_key;
  InternalKey(ns_key, field, metadata->version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  uint32_t field_expire = 0;
  if (s.ok()) {
    std::string field_raw_value;
    std::size_t idx = 0;
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &field_raw_value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      std::string field_value;
      if (support_field_expire_) {
        ExtractFieldRawValue(field_raw_value, &field_value, &field_expire);
        if (Expired(field_expire)) {
          return rocksdb::Status::NotFound(kErrMsgFieldExpired);
        }
      } else {
        field_value = field_raw_value;
      }
      try {
        old_value = std::stoll(field_value, &idx);
      } catch (std::exception &e) {
        return rocksdb::Status::InvalidArgument(e.what());
      }
      if (isspace(field_value[0]) || idx != field_value.size()) {
        return rocksdb::Status::InvalidArgument("value is not an integer");
      }
      exists = true;
    }
  }
  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN-old_value))
      || (increment > 0 && old_value > 0 && increment > (LLONG_MAX-old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *ret = old_value + increment;
  rocksdb::WriteBatch batch;

  if (support_field_expire_) {
    WriteBatchLogData log_data(kRedisExHash);
    batch.PutLogData(log_data.Encode());
    std::string raw_value;
    ComposeFieldRawValue(std::to_string(*ret), field_expire, &raw_value);
    batch.Put(sub_key, raw_value);
  } else {
    WriteBatchLogData log_data(kRedisHash);
    batch.PutLogData(log_data.Encode());
    batch.Put(sub_key, std::to_string(*ret));
  }

  if (!exists) {
    metadata->size += 1;
    std::string bytes;
    metadata->Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Hash::IncrByFloat(const Slice &user_key, const Slice &field, double increment, double *ret) {
  bool exists = false;
  float old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  Metadata *p;
  if (support_field_expire_) {
    p = new ExHashMetadata();
  } else {
    p = new HashMetadata();
  }
  std::unique_ptr<Metadata> metadata(p);

  rocksdb::Status s = GetMetadata(ns_key, metadata.get());
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key;
  InternalKey(ns_key, field, metadata->version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  uint32_t field_expire = 0;
  if (s.ok()) {
    std::string field_raw_value;
    std::size_t idx = 0;
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &field_raw_value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      std::string field_value;
      if (support_field_expire_) {
        ExtractFieldRawValue(field_raw_value, &field_value, &field_expire);
        if (Expired(field_expire)) {
          return rocksdb::Status::NotFound(kErrMsgFieldExpired);
        }
      } else {
        field_value = field_raw_value;
      }
      try {
        old_value = std::stod(field_value, &idx);
      } catch (std::exception &e) {
        return rocksdb::Status::InvalidArgument(e.what());
      }
      if (isspace(field_value[0]) || idx != field_value.size()) {
        return rocksdb::Status::InvalidArgument("value is not an float");
      }
      exists = true;
    }
  }
  double n = old_value + increment;
  if (std::isinf(n) || std::isnan(n)) {
    return rocksdb::Status::InvalidArgument("increment would produce NaN or Infinity");
  }

  *ret = n;
  rocksdb::WriteBatch batch;

  if (support_field_expire_) {
    WriteBatchLogData log_data(kRedisExHash);
    batch.PutLogData(log_data.Encode());
    std::string raw_value;
    ComposeFieldRawValue(std::to_string(*ret), field_expire, &raw_value);
    batch.Put(sub_key, raw_value);
  } else {
    WriteBatchLogData log_data(kRedisHash);
    batch.PutLogData(log_data.Encode());
    batch.Put(sub_key, std::to_string(*ret));
  }

  if (!exists) {
    metadata->size += 1;
    std::string bytes;
    metadata->Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Hash::MGet(const Slice &user_key, const std::vector<Slice> &fields,
                           std::vector<std::string> *values, std::vector<rocksdb::Status> *statuses) {
  return MGet(user_key, fields, values, nullptr, statuses);
}

rocksdb::Status Hash::MGet(const Slice &user_key, const std::vector<Slice> &fields,
                           std::vector<std::string> *values, std::vector<int32_t> *field_ttls,
                           std::vector<rocksdb::Status> *statuses) {
  assert(values != nullptr && statuses != nullptr);
  values->clear();
  statuses->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  Metadata *p;
  if (support_field_expire_) {
    p = new ExHashMetadata(false);
  } else {
    p = new HashMetadata(false);
  }
  std::unique_ptr<Metadata> metadata(p);

  rocksdb::Status s = GetMetadata(ns_key, metadata.get());
  if (!s.ok()) {
    return s;
  }

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key, field_raw_value, value;
  uint32_t field_expire;
  int32_t ttl;
  for (const auto &field : fields) {
    InternalKey(ns_key, field, metadata->version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
    field_raw_value.clear();
    value.clear();
    field_expire = 0;
    ttl = -2;
    auto s = db_->Get(read_options, sub_key, &field_raw_value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (support_field_expire_) {
      ExtractFieldRawValue(field_raw_value, &value, &field_expire);
      if (Expired(field_expire)) {
        s = rocksdb::Status::NotFound(kErrMsgFieldExpired);
      } else if (field_ttls != nullptr) {
        ttl = GetTTL(field_expire);
      }
      values->emplace_back(value);
    } else {
      values->emplace_back(field_raw_value);
    }
    if (field_ttls != nullptr) {
      field_ttls->emplace_back(ttl);
    }
    statuses->emplace_back(s);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Set(const Slice &user_key, const Slice &field, const Slice &value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString(), 0};
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(user_key, fvs, false, ret);
}

rocksdb::Status Hash::Set(const Slice &user_key, const Slice &field, const Slice &value, int32_t field_ttl, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString(), 0};
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  if (field_ttl > 0) {
    fv.expire = uint32_t(now) + field_ttl;
  }
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(user_key, fvs, false, ret);
}

rocksdb::Status Hash::SetNX(const Slice &user_key, const Slice &field, Slice value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString(), 0};
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(user_key, fvs, true, ret);
}

rocksdb::Status Hash::Delete(const Slice &user_key, const std::vector<Slice> &fields, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  Metadata *p;
  if (support_field_expire_) {
    p = new ExHashMetadata(false);
  } else {
    p = new HashMetadata(false);
  }
  std::unique_ptr<Metadata> metadata(p);
  rocksdb::WriteBatch batch;

  if (support_field_expire_) {
    WriteBatchLogData log_data(kRedisExHash);
    batch.PutLogData(log_data.Encode());
  } else {
    WriteBatchLogData log_data(kRedisHash);
    batch.PutLogData(log_data.Encode());
  }

  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = GetMetadata(ns_key, metadata.get());
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string sub_key, value;
  for (const auto &field : fields) {
    InternalKey(ns_key, field, metadata->version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (s.ok()) {
      *ret += 1;
      batch.Delete(sub_key);
    }
  }
  if (*ret == 0) {
    return rocksdb::Status::OK();
  }
  metadata->size -= *ret;
  std::string bytes;
  metadata->Encode(&bytes);
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Hash::MSet(const Slice &user_key, const std::vector<FieldValue> &field_values, bool nx, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);

  Metadata *p;
  if (support_field_expire_) {
    p = new ExHashMetadata();
  } else {
    p = new HashMetadata();
  }
  std::unique_ptr<Metadata> metadata(p);

  rocksdb::Status s = GetMetadata(ns_key, metadata.get());
  if (!s.ok() && !s.IsNotFound()) return s;

  int added = 0;
  bool exists = false;
  rocksdb::WriteBatch batch;

  if (support_field_expire_) {
    WriteBatchLogData log_data(kRedisExHash);
    batch.PutLogData(log_data.Encode());
  } else {
    WriteBatchLogData log_data(kRedisHash);
    batch.PutLogData(log_data.Encode());
  }

  for (const auto &fv : field_values) {
    exists = false;
    std::string sub_key;
    InternalKey(ns_key, fv.field, metadata->version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
    std::string new_value;

    if (support_field_expire_) {
      ComposeFieldRawValue(fv.value, fv.expire, &new_value);
    } else {
      new_value = fv.value;
    }

    if (metadata->size > 0) {
      std::string field_raw_value;
      s = db_->Get(rocksdb::ReadOptions(), sub_key, &field_raw_value);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (s.ok()) {
        if (field_raw_value == new_value || nx) continue;
        exists = true;
      }
    }
    if (!exists) added++;
    batch.Put(sub_key, new_value);
  }

  if (added > 0) {
    *ret = added;
    metadata->size += added;
    std::string bytes;
    metadata->Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Hash::GetAll(const Slice &user_key, std::vector<FieldValue> *field_values) {
  field_values->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  Metadata *p;
  if (support_field_expire_) {
    p = new ExHashMetadata(false);
  } else {
    p = new HashMetadata(false);
  }
  std::unique_ptr<Metadata> metadata(p);

  rocksdb::Status s = GetMetadata(ns_key, metadata.get());
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);
  std::string prefix_key;
  InternalKey(ns_key, "", metadata->version, storage_->IsSlotIdEncoded()).Encode(&prefix_key);
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  for (iter->Seek(prefix_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       iter->Next()) {
    FieldValue fv;
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    fv.field = ikey.GetSubKey().ToString();
    if (support_field_expire_) {
      ExtractFieldRawValue(iter->value().ToString(), &fv.value, &fv.expire);
      // skip expired field
      if (fv.expire != 0 && fv.expire < now) {
        continue;
      }
    } else {
      fv.value = iter->value().ToString();
    }
    field_values->emplace_back(fv);
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Scan(const Slice &user_key,
                           const std::string &cursor,
                           uint64_t limit,
                           const std::string &field_prefix,
                           std::vector<std::string> *fields,
                           std::vector<std::string> *values) {
  if (!support_field_expire_) {
    return SubKeyScanner::Scan(kRedisHash, user_key, cursor, limit, field_prefix, fields, values);
  }

  std::vector<std::string> exist_fields;
  std::vector<std::string> raw_values;

  auto s = SubKeyScanner::Scan(kRedisExHash, user_key, cursor, limit, field_prefix, &exist_fields, &raw_values);
  if (!s.ok()) return s;

  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);

  for (size_t i = 0; i < raw_values.size(); i++) {
    std::string value;
    uint32_t expire;
    ExtractFieldRawValue(raw_values[i], &value, &expire);
    if (expire == 0 || expire > now) {
      fields->emplace_back(exist_fields[i]);
      values->emplace_back(value);
    }
  }

  return rocksdb::Status::OK();
}

}  // namespace Redis
