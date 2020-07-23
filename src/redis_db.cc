#include "redis_db.h"
#include <ctime>
#include "server.h"
#include "util.h"

namespace Redis {

Database::Database(Engine::Storage *storage, const std::string &ns) {
  storage_ = storage;
  metadata_cf_handle_ = storage->GetCFHandle("metadata");
  db_ = storage->GetDB();
  namespace_ = ns;
}

rocksdb::Status Database::GetMetadata(RedisType type, const Slice &ns_key, Metadata *metadata) {
  std::string old_metadata;
  metadata->Encode(&old_metadata);
  std::string bytes;
  auto s = GetRawMetadata(ns_key, &bytes);
  if (!s.ok()) return s;
  metadata->Decode(bytes);

  if (metadata->Expired()) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound("the key was Expired");
  }
  if (metadata->Type() != type && (metadata->size > 0 || metadata->Type() == kRedisString)) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  if (metadata->size == 0) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound("no elements");
  }
  return s;
}

rocksdb::Status Database::GetRawMetadata(const Slice &ns_key, std::string *bytes) {
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  return db_->Get(read_options, metadata_cf_handle_, ns_key, bytes);
}

rocksdb::Status Database::GetRawMetadataByUserKey(const Slice &user_key, std::string *bytes) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  return GetRawMetadata(ns_key, bytes);
}

rocksdb::Status Database::Expire(const Slice &user_key, int timestamp) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  std::string value;
  Metadata metadata(kRedisNone, false);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s;
  metadata.Decode(value);
  if (metadata.Expired()) {
    return rocksdb::Status::NotFound("the key was expired");
  }
  if (metadata.Type() != kRedisString && metadata.size == 0) {
    return rocksdb::Status::NotFound("no elements");
  }
  if (metadata.expire == timestamp) return rocksdb::Status::OK();

  char *buf = new char[value.size()];
  memcpy(buf, value.data(), value.size());
  // +1 to skip the flags
  EncodeFixed32(buf + 1, (uint32_t) timestamp);
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisNone, {std::to_string(kRedisCmdExpire)});
  batch.PutLogData(log_data.Encode());
  batch.Put(metadata_cf_handle_, ns_key, Slice(buf, value.size()));
  s = storage_->Write(rocksdb::WriteOptions(), &batch);
  delete[]buf;
  return s;
}

rocksdb::Status Database::Del(const Slice &user_key) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  std::string value;
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s;
  Metadata metadata(kRedisNone, false);
  metadata.Decode(value);
  if (metadata.Expired()) {
    return rocksdb::Status::NotFound("the key was expired");
  }
  return storage_->Delete(rocksdb::WriteOptions(), metadata_cf_handle_, ns_key);
}

rocksdb::Status Database::Exists(const std::vector<Slice> &keys, int *ret) {
  *ret = 0;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();

  rocksdb::Status s;
  std::string ns_key, value;
  for (const auto &key : keys) {
    AppendNamespacePrefix(key, &ns_key);
    s = db_->Get(read_options, metadata_cf_handle_, ns_key, &value);
    if (s.ok()) {
      Metadata metadata(kRedisNone, false);
      metadata.Decode(value);
      if (!metadata.Expired()) *ret += 1;
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::TTL(const Slice &user_key, int *ttl) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  *ttl = -2;  // ttl is -2 when the key does not exist or expired
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = db_->Get(read_options, metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  Metadata metadata(kRedisNone, false);
  metadata.Decode(value);
  *ttl = metadata.TTL();
  return rocksdb::Status::OK();
}

void Database::GetKeyNumStats(const std::string &prefix, KeyNumStats *stats) {
  Keys(prefix, nullptr, stats);
}

void Database::Keys(std::string prefix, std::vector<std::string> *keys, KeyNumStats *stats) {
  std::string ns_prefix, ns, user_key, value;
  if (namespace_ != kDefaultNamespace || keys != nullptr) {
    AppendNamespacePrefix(prefix, &ns_prefix);
    prefix = ns_prefix;
  }

  uint64_t ttl_sum = 0;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, metadata_cf_handle_);
  prefix.empty() ? iter->SeekToFirst() : iter->Seek(prefix);
  for (; iter->Valid(); iter->Next()) {
    if (!prefix.empty() && !iter->key().starts_with(prefix)) {
      break;
    }
    Metadata metadata(kRedisNone, false);
    value = iter->value().ToString();
    metadata.Decode(value);
    if (metadata.Expired()) {
      if (stats) stats->n_expired++;
      continue;
    }
    if (stats) {
      int32_t ttl = metadata.TTL();
      stats->n_key++;
      if (ttl != -1) {
        stats->n_expires++;
        if (ttl > 0) ttl_sum += ttl;
      }
    }
    if (keys) {
      ExtractNamespaceKey(iter->key(), &ns, &user_key);
      keys->emplace_back(user_key);
    }
  }
  if (stats && stats->n_expires > 0) {
    stats->avg_ttl = ttl_sum / stats->n_expires;
  }
  delete iter;
}

rocksdb::Status Database::Scan(const std::string &cursor,
                         uint64_t limit,
                         const std::string &prefix,
                         std::vector<std::string> *keys) {
  uint64_t cnt = 0;
  std::string ns_prefix, ns_cursor, ns, user_key, value;
  AppendNamespacePrefix(prefix, &ns_prefix);
  AppendNamespacePrefix(cursor, &ns_cursor);

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, metadata_cf_handle_);
  if (!cursor.empty()) {
    iter->Seek(ns_cursor);
    if (iter->Valid()) {
      iter->Next();
    }
  } else if (ns_prefix.empty()) {
    iter->SeekToFirst();
  } else {
    iter->Seek(ns_prefix);
  }

  for (; iter->Valid() && cnt < limit; iter->Next()) {
    if (!ns_prefix.empty() && !iter->key().starts_with(ns_prefix)) {
      break;
    }
    Metadata metadata(kRedisNone, false);
    value = iter->value().ToString();
    metadata.Decode(value);
    if (metadata.Expired()) continue;
    ExtractNamespaceKey(iter->key(), &ns, &user_key);
    keys->emplace_back(user_key);
    cnt++;
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status Database::RandomKey(const std::string &cursor, std::string *key) {
  key->clear();

  std::vector<std::string> keys;
  auto s = Scan(cursor, 60, "", &keys);
  if (!s.ok()) {
    return s;
  }
  if (keys.empty() && !cursor.empty()) {
    // if reach the end, restart from begining
    auto s = Scan("", 60, "", &keys);
    if (!s.ok()) {
      return s;
    }
  }
  if (!keys.empty()) {
    unsigned int seed = time(NULL);
    *key = keys.at(rand_r(&seed) % keys.size());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::FlushDB() {
  std::string prefix, begin_key, end_key;
  AppendNamespacePrefix("", &prefix);
  auto s = FindKeyRangeWithPrefix(prefix, &begin_key, &end_key);
  if (!s.ok()) {
    return rocksdb::Status::OK();
  }
  s = storage_->DeleteRange(begin_key, end_key);
  if (!s.ok()) {
    return s;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Database::FlushAll() {
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, metadata_cf_handle_);
  iter->SeekToFirst();
  if (!iter->Valid()) {
    delete iter;
    return rocksdb::Status::OK();
  }
  auto first_key = iter->key().ToString();
  iter->SeekToLast();
  if (!iter->Valid()) {
    delete iter;
    return rocksdb::Status::OK();
  }
  auto last_key = iter->key().ToString();
  auto s = storage_->DeleteRange(first_key, last_key);
  if (!s.ok()) {
    delete iter;
    return s;
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status Database::Dump(const Slice &user_key, std::vector<std::string> *infos) {
  infos->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = db_->Get(read_options, metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  Metadata metadata(kRedisNone, false);
  metadata.Decode(value);

  infos->emplace_back("namespace");
  infos->emplace_back(namespace_);
  infos->emplace_back("type");
  infos->emplace_back(RedisTypeNames[metadata.Type()]);
  infos->emplace_back("version");
  infos->emplace_back(std::to_string(metadata.version));
  infos->emplace_back("expire");
  infos->emplace_back(std::to_string(metadata.expire));
  infos->emplace_back("size");
  infos->emplace_back(std::to_string(metadata.size));

  infos->emplace_back("created_at");
  struct timeval created_at = metadata.Time();
  std::time_t tm = created_at.tv_sec;
  char time_str[25];
  if (!std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", std::localtime(&tm))) {
    return rocksdb::Status::TryAgain("Fail to format local time_str");
  }
  std::string created_at_str(time_str);
  infos->emplace_back(created_at_str + "." + std::to_string(created_at.tv_usec));

  if (metadata.Type() == kRedisList) {
    ListMetadata metadata(false);
    GetMetadata(kRedisList, ns_key, &metadata);
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
    infos->emplace_back("head");
    infos->emplace_back(std::to_string(metadata.head));
    infos->emplace_back("tail");
    infos->emplace_back(std::to_string(metadata.tail));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Database::Type(const Slice &user_key, RedisType *type) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  *type = kRedisNone;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = db_->Get(read_options, metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  Metadata metadata(kRedisNone, false);
  metadata.Decode(value);
  *type = metadata.Type();
  return rocksdb::Status::OK();
}

void Database::AppendNamespacePrefix(const Slice &user_key, std::string *output) {
  ComposeNamespaceKey(namespace_, user_key, output);
}

rocksdb::Status Database::FindKeyRangeWithPrefix(const std::string &prefix, std::string *begin, std::string *end) {
  begin->clear();
  end->clear();

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, metadata_cf_handle_);
  iter->Seek(prefix);
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    delete iter;
    return rocksdb::Status::NotFound();
  }
  *begin = iter->key().ToString();

  // it's ok to increase the last char in prefix as the boundary of the prefix
  // while we limit the namespace last char shouldn't be larger than 128.
  std::string next_prefix = prefix;
  char last_char = next_prefix.back();
  last_char++;
  next_prefix.pop_back();
  next_prefix.push_back(last_char);
  iter->SeekForPrev(next_prefix);
  int max_prev_limit = 128;  // prevent unpredicted long while loop
  int i = 0;
  // reversed seek the key til with prefix or end of the iterator
  while (i++ < max_prev_limit && iter->Valid() && !iter->key().starts_with(prefix)) {
    iter->Prev();
  }
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    delete iter;
    return rocksdb::Status::NotFound();
  }
  *end = iter->key().ToString();
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status SubKeyScanner::Scan(RedisType type,
                                    const Slice &user_key,
                                    const std::string &cursor,
                                    uint64_t limit,
                                    const std::string &subkey_prefix,
                                    std::vector<std::string> *keys,
                                    std::vector<std::string> *values) {
  uint64_t cnt = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  Metadata metadata(type, false);
  rocksdb::Status s = GetMetadata(type, ns_key, &metadata);
  if (!s.ok()) return s;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);
  std::string match_prefix_key;
  if (!subkey_prefix.empty()) {
    InternalKey(ns_key, subkey_prefix, metadata.version).Encode(&match_prefix_key);
  } else {
    InternalKey(ns_key, "", metadata.version).Encode(&match_prefix_key);
  }

  std::string start_key;
  if (!cursor.empty()) {
    InternalKey(ns_key, cursor, metadata.version).Encode(&start_key);
  } else {
    start_key = match_prefix_key;
  }
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    if (!cursor.empty() && iter->key() == start_key) {
      // if cursor is not empty, then we need to skip start_key
      // because we already return that key in the last scan
      continue;
    }
    if (!iter->key().starts_with(match_prefix_key)) {
      break;
    }
    InternalKey ikey(iter->key());
    keys->emplace_back(ikey.GetSubKey().ToString());
    if (values != nullptr) {
      values->emplace_back(iter->value().ToString());
    }
    cnt++;
    if (limit > 0 && cnt >= limit) {
      break;
    }
  }
  delete iter;
  return rocksdb::Status::OK();
}

RedisType WriteBatchLogData::GetRedisType() {
  return type_;
}

std::vector<std::string> *WriteBatchLogData::GetArguments() {
  return &args_;
}

std::string WriteBatchLogData::Encode() {
  std::string ret = std::to_string(type_);
  for (size_t i = 0; i < args_.size(); i++) {
    ret += " " + args_[i];
  }
  return ret;
}

Status WriteBatchLogData::Decode(const rocksdb::Slice &blob) {
  std::string log_data = blob.ToString();
  std::vector<std::string> args;
  Util::Split(log_data, " ", &args);
  type_ = static_cast<RedisType >(std::stoi(args[0]));
  args_ = std::vector<std::string>(args.begin() + 1, args.end());

  return Status::OK();
}

void WriteBatchExtractor::LogData(const rocksdb::Slice &blob) {
  log_data_.Decode(blob);
}

rocksdb::Status WriteBatchExtractor::PutCF(uint32_t column_family_id, const Slice &key,
                                           const Slice &value) {
  std::string ns, user_key;
  std::vector<std::string> command_args;
  if (column_family_id == kColumnFamilyIDMetadata) {
    ExtractNamespaceKey(key, &ns, &user_key);
    put_keys_.emplace_back(user_key);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchExtractor::DeleteCF(uint32_t column_family_id, const Slice &key) {
  std::string ns, user_key;
  std::vector<std::string> command_args;
  if (column_family_id == kColumnFamilyIDMetadata) {
    ExtractNamespaceKey(key, &ns, &user_key);
    delete_keys_.emplace_back(user_key);
  }
  return rocksdb::Status::OK();
}

}  // namespace Redis
