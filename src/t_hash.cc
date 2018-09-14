#include "t_hash.h"
#include <iostream>
#include <rocksdb/status.h>

// lock outside before use GetMetadata
rocksdb::Status RedisHash::GetMetadata(Slice key, HashMetadata *metadata) {
  return RedisDB::GetMetadata(kRedisHash, key, metadata);
}

rocksdb::Status RedisHash::Size(Slice key, uint32_t *ret) {
  *ret = 0;
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s;
  *ret = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisHash::Get(Slice key, Slice field, std::string *value) {
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s;
  Slice sub_key = InternalKey(key, field, metadata.version).Encode();
  return db_->Get(rocksdb::ReadOptions(), sub_key, value);
}

rocksdb::Status RedisHash::IncrBy(Slice key, Slice field, long long increment, long long *ret) {
  bool exists = false;
  long long old_value = 0;
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  Slice sub_key = InternalKey(key, field, metadata.version).Encode();
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
  return db_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::IncrByFloat(Slice key, Slice field, float increment, float *ret) {
  bool exists = false;
  float old_value = 0;
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  Slice sub_key = InternalKey(key, field, metadata.version).Encode();
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
  return db_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::MGet(Slice key, std::vector<Slice> &fields, std::vector<std::string> *values) {
  values->clear();
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) {
    return s;
  }

  std::vector<InternalKey*> ikeys;
  std::vector<Slice> sub_keys;
  for (auto field : fields) {
    auto ikey = new InternalKey(key, field, metadata.version);
    sub_keys.emplace_back(ikey->Encode());
    ikeys.push_back(ikey);
  }
  db_->MultiGet(rocksdb::ReadOptions(), sub_keys, values);
  for (auto ikey : ikeys) delete ikey;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisHash::Set(Slice key, Slice field, Slice value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString()};
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(key, fvs, ret);
}

rocksdb::Status RedisHash::SetNX(Slice key, Slice field, Slice value, int *ret) {
  *ret = 0;
  std::string v;
  rocksdb::Status s = Get(key, field, &v);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }
  return Set(key, field, value, ret);
}

rocksdb::Status RedisHash::Delete(Slice key, std::vector<rocksdb::Slice> &fields, int *ret) {
  *ret = 0;
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  std::vector<InternalKey*> ikeys;
  std::vector<Slice> sub_keys;
  for (auto field : fields) {
    auto *ikey = new InternalKey(key, field, metadata.version);
    sub_keys.push_back(ikey->Encode());
    ikeys.push_back(ikey);
  }
  std::vector<std::string> values;
  rocksdb::WriteBatch batch;
  std::vector<rocksdb::Status> statuses = db_->MultiGet(rocksdb::ReadOptions(), sub_keys, &values);
  for (int i = 0; i < statuses.size(); i++) {
    if (!statuses[i].ok()) continue; // skip key if not exists
    *ret += 1;
    batch.Delete(sub_keys[i]);
  }
  // size was updated
  if (*ret > 0) {
    metadata.size -= *ret;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  s = db_->Write(rocksdb::WriteOptions(), &batch);
  for (auto ikey : ikeys) delete ikey;
  return s;
}

rocksdb::Status RedisHash::MSet(Slice key, std::vector<FieldValue> &field_values, int *ret) {
  *ret = 0;

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  int added = 0;
  rocksdb::WriteBatch batch;
  for (auto fv : field_values) {
    Slice sub_key = InternalKey(key, fv.field, metadata.version).Encode();
    if (metadata.size > 0) {
      std::string fieldValue;
      s = db_->Get(rocksdb::ReadOptions(), sub_key, &fieldValue);
      if (s.ok() && fieldValue == fv.value) continue;
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
  return db_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisHash::GetAll(Slice key, std::vector<FieldValue> *field_values, int type) {
  field_values->clear();
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  auto iter = db_->NewIterator(opts);
  Slice prefix_key = InternalKey(key, "", metadata.version).Encode();
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
  return rocksdb::Status::OK();
}