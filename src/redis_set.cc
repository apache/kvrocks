//
// Created by hulk on 2018/10/9.
//

#include "redis_set.h"
#include <iostream>

rocksdb::Status RedisSet::GetMetadata(Slice key, SetMetadata*metadata) {
  return RedisDB::GetMetadata(kRedisSet, key, metadata);
}

rocksdb::Status RedisSet::Add(Slice key, std::vector<Slice> members, int *ret) {
  *ret = 0;

  std::string ns_key;
  AppendNamepacePrefix(key, &ns_key);
  key = Slice(ns_key);

  LockGuard guard(storage_->GetLockManager(), key);
  SetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::vector<Slice> new_members;
  std::string value;
  rocksdb::WriteBatch batch;
  std::string sub_key;
  for (const auto &member : members) {
    InternalKey(key, member, metadata.version).Encode(&sub_key);
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (s.ok()) continue;
    batch.Put(sub_key, Slice());
    *ret += 1;
  }
  if (*ret > 0) {
    metadata.size += *ret;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisSet::Remove(Slice key, std::vector<Slice> members, int *ret) {
  *ret = 0;

  std::string ns_key;
  AppendNamepacePrefix(key, &ns_key);
  key = Slice(ns_key);

  LockGuard guard(storage_->GetLockManager(), key);
  SetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string value, sub_key;
  rocksdb::WriteBatch batch;
  for (const auto &member : members) {
    InternalKey(key, member, metadata.version).Encode(&sub_key);
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (!s.ok()) continue;
    batch.Delete(sub_key);
    *ret += 1;
  }
  if (*ret > 0) {
    metadata.size -= *ret;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisSet::Card(Slice key, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamepacePrefix(key, &ns_key);
  key = Slice(ns_key);
  SetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  *ret = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisSet::Members(Slice key, std::vector<std::string> *members) {
  members->clear();

  std::string ns_key;
  AppendNamepacePrefix(key, &ns_key);
  key = Slice(ns_key);
  SetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix;
  InternalKey(key, "", metadata.version).Encode(&prefix);
  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);
  for (iter->Seek(prefix);
       iter->Valid() && iter->key().starts_with(prefix);
       iter->Next()) {
    InternalKey ikey(iter->key());
    members->emplace_back(ikey.GetSubKey().ToString());
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisSet::IsMember(Slice key, Slice member, int *ret) {
  *ret = 0;

  std::string ns_key;
  AppendNamepacePrefix(key, &ns_key);
  key = Slice(ns_key);
  SetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key;
  InternalKey(key, member, metadata.version).Encode(&sub_key);
  std::string value;
  s = db_->Get(read_options, sub_key, &value);
  if (s.ok()) {
    *ret = 1;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RedisSet::Take(Slice key, std::vector<std::string> *members, int count, bool pop) {
  int n = 0;
  members->clear();
  if (count <= 0) return rocksdb::Status::OK();

  std::string ns_key;
  AppendNamepacePrefix(key, &ns_key);
  key = Slice(ns_key);

  if (pop) LockGuard guard(storage_->GetLockManager(), key);
  SetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);
  std::string prefix;
  InternalKey(key, "", metadata.version).Encode(&prefix);
  for (iter->Seek(prefix);
       iter->Valid() && iter->key().starts_with(prefix);
       iter->Next()) {
    InternalKey ikey(iter->key());
    members->emplace_back(ikey.GetSubKey().ToString());
    if (pop) batch.Delete(iter->key());
    if (++n >= count) break;
  }
  delete iter;
  if (pop && n > 0) {
    metadata.size -= n;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisSet::Move(Slice src, Slice dst, Slice member, int *ret) {
  std::vector<Slice> members{member};
  rocksdb::Status s = Remove(src, members, ret);
  if (!s.ok() || *ret == 0) {
    return s;
  }
  return Add(dst, members, ret);
}