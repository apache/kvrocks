#include "redis_sortedint.h"

#include <map>
#include <iostream>
#include <limits>

namespace Redis {

rocksdb::Status Sortedint::GetMetadata(const Slice &ns_key, SortedintMetadata *metadata) {
  return Database::GetMetadata(kRedisSortedint, ns_key, metadata);
}

rocksdb::Status Sortedint::Add(const Slice &user_key, std::vector<uint64_t> ids, int *ret) {
  *ret = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  SortedintMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string value;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisSortedint);
  batch.PutLogData(log_data.Encode());
  std::string sub_key;
  for (const auto id : ids) {
    std::string id_buf;
    PutFixed64(&id_buf, id);
    InternalKey(ns_key, id_buf, metadata.version).Encode(&sub_key);
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (s.ok()) continue;
    batch.Put(sub_key, Slice());
    *ret += 1;
  }
  if (*ret > 0) {
    metadata.size += *ret;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Sortedint::Remove(const Slice &user_key, std::vector<uint64_t> ids, int *ret) {
  *ret = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  SortedintMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string value, sub_key;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisSortedint);
  batch.PutLogData(log_data.Encode());
  for (const auto id : ids) {
    std::string id_buf;
    PutFixed64(&id_buf, id);
    InternalKey(ns_key, id_buf, metadata.version).Encode(&sub_key);
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (!s.ok()) continue;
    batch.Delete(sub_key);
    *ret += 1;
  }
  if (*ret == 0) return rocksdb::Status::OK();
  metadata.size -= *ret;
  std::string bytes;
  metadata.Encode(&bytes);
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Sortedint::Card(const Slice &user_key, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  SortedintMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  *ret = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status Sortedint::Range(const Slice &user_key,
                               uint64_t cursor_id,
                               uint64_t offset,
                               uint64_t limit,
                               bool reversed,
                               std::vector<uint64_t> *ids) {
  ids->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  SortedintMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix, start_key, start_buf;
  uint64_t start_id = cursor_id;
  if (reversed && cursor_id == 0) {
    start_id = std::numeric_limits<uint64_t>::max();
  }
  PutFixed64(&start_buf, start_id);
  InternalKey(ns_key, start_buf, metadata.version).Encode(&start_key);
  InternalKey(ns_key, "", metadata.version).Encode(&prefix);
  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  uint64_t id, pos = 0;
  auto iter = db_->NewIterator(read_options);
  for (!reversed ? iter->Seek(start_key) : iter->SeekForPrev(start_key);
       iter->Valid() && iter->key().starts_with(prefix);
       !reversed ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key());
    Slice sub_key = ikey.GetSubKey();
    GetFixed64(&sub_key, &id);
    if ( id == cursor_id || pos++ < offset ) continue;
    ids->emplace_back(id);
    if (limit > 0 && ids && ids->size() >= limit) break;
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status Sortedint::MExist(const Slice &user_key, std::vector<uint64_t> ids, std::vector<int> *exists) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  SortedintMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key, value;
  for (const auto id : ids) {
    std::string id_buf;
    PutFixed64(&id_buf, id);
    InternalKey(ns_key, id_buf, metadata.version).Encode(&sub_key);
    s = db_->Get(read_options, sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      exists->emplace_back(0);
    } else {
      exists->emplace_back(1);
    }
  }
  return rocksdb::Status::OK();
}
}  // namespace Redis
