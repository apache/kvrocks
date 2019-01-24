#include "redis_metadata.h"
#include <time.h>

InternalKey::InternalKey(Slice input) {
  uint32_t key_size;
  uint8_t namespace_size;
  GetFixed8(&input, &namespace_size);
  namespace_ = Slice(input.data(), namespace_size);
  input.remove_prefix(namespace_size);
  GetFixed32(&input, &key_size);
  key_ = Slice(input.data(), key_size);
  input.remove_prefix(key_size);
  GetFixed64(&input, &version_);
  sub_key_ = Slice(input.data(), input.size());
  buf_ = nullptr;
}

InternalKey::InternalKey(Slice ns_key, Slice sub_key, uint64_t version) {
  uint8_t namespace_size;
  GetFixed8(&ns_key, &namespace_size);
  namespace_ = Slice(ns_key.data(), namespace_size);
  ns_key.remove_prefix(namespace_size); // +4 remove the key size
  key_ = ns_key;
  sub_key_ = sub_key;
  version_ = version;
  buf_ = nullptr;
}

InternalKey::~InternalKey() {
  if (buf_ != nullptr && buf_!=prealloc_) delete []buf_;
}

Slice InternalKey::GetNamespace() {
  return namespace_;
}

Slice InternalKey::GetKey() {
  return key_;
}

Slice InternalKey::GetSubKey() {
  return sub_key_;
}

uint64_t InternalKey::GetVersion() {
  return version_;
}

void InternalKey::Encode(std::string *out) {
  out->clear();
  size_t pos = 0;
  size_t total = 1+namespace_.size()+4+key_.size()+8+sub_key_.size();
  if (total < sizeof(prealloc_)) {
    buf_ = prealloc_;
  } else {
    buf_ = new char[total];
  }
  EncodeFixed8(buf_+pos, static_cast<uint8_t>(namespace_.size()));
  pos += 1;
  memcpy(buf_+pos, namespace_.data(), namespace_.size());
  pos += namespace_.size();
  EncodeFixed32(buf_+pos, static_cast<uint32_t>(key_.size()));
  pos += 4;
  memcpy(buf_+pos, key_.data(), key_.size());
  pos += key_.size();
  EncodeFixed64(buf_+pos, version_);
  pos += 8;
  memcpy(buf_+pos, sub_key_.data(), sub_key_.size());
  pos += sub_key_.size();
  out->assign(buf_, pos);
}

bool InternalKey::operator==(const InternalKey &that) const {
  if (key_ != that.key_) return false;
  if (sub_key_ != that.sub_key_) return false;
  return version_ == that.version_;
}

void ExtractNamespaceKey(Slice ns_key, std::string *ns, std::string *key) {
  uint8_t namespace_size;
  GetFixed8(&ns_key, &namespace_size);
  *ns = ns_key.ToString().substr(0, namespace_size);
  ns_key.remove_prefix(namespace_size);
  *key = ns_key.ToString();
}

void ComposeNamespaceKey(const Slice ns, const Slice key, std::string *ns_key) {
  ns_key->clear();
  PutFixed8(ns_key, static_cast<uint8_t>(ns.size()));
  ns_key->append(ns.ToString());
  ns_key->append(key.ToString());
}
Metadata::Metadata(RedisType type) {
  flags = (uint8_t)0x0f & type;
  expire = -1;
  version = 0;
  size = 0;
  version = generateVersion();
}

rocksdb::Status Metadata::Decode(std::string &bytes) {
  // flags(1byte) + expire (4byte)
  if (bytes.size() < 5) {
    return rocksdb::Status::InvalidArgument("the metadata was too short");
  }
  Slice input(bytes);
  GetFixed8(&input, &flags);
  GetFixed32(&input, (uint32_t *) &expire);
  if (Type() != kRedisString) {
    if (input.size() < 12) rocksdb::Status::InvalidArgument("the metadata was too short");
    GetFixed64(&input, &version);
    GetFixed32(&input, &size);
  }
  return rocksdb::Status::OK();
}

void Metadata::Encode(std::string *dst) {
  PutFixed8(dst, flags);
  PutFixed32(dst, (uint32_t) expire);
  if (Type() != kRedisString) {
    PutFixed64(dst, version);
    PutFixed32(dst, size);
  }
}

uint64_t Metadata::generateVersion() {
  struct timespec now{0, 0};
  clock_gettime(CLOCK_REALTIME, &now);
  return int64_t(now.tv_sec) * 1000000000 + uint64_t(now.tv_nsec);
}

bool Metadata::operator==(const Metadata &that) const {
  if (flags != that.flags) return false;
  if (expire != that.expire) return false;
  if (Type() != kRedisString) {
    if (size != that.size) return false;
    if (version != that.version) return false;
  }
  return true;
}

RedisType Metadata::Type() const {
  return static_cast<RedisType>(flags & (uint8_t)0x0f);
}

int32_t Metadata::TTL() const {
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  if (expire != 0 && expire < now) {
    return -2;
  }
  return expire == 0 ? -1 : int32_t (expire - now);
}

bool Metadata::Expired() const {
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  // version is nanosecond
  if (Type() != kRedisString && version >= static_cast<uint64_t>(now * 1000000000)) { // creating the key metadata
    return false;
  }
  if (expire > 0 && expire < now) {
    return true;
  }
  return Type() != kRedisString && size == 0;
}

ListMetadata::ListMetadata() : Metadata(kRedisList){
  head = UINT64_MAX/2;
  tail = head;
}

void ListMetadata::Encode(std::string *dst) {
  Metadata::Encode(dst);
  PutFixed64(dst, head);
  PutFixed64(dst, tail);
}

rocksdb::Status ListMetadata::Decode(std::string &bytes) {
  Slice input(bytes);
  GetFixed8(&input, &flags);
  GetFixed32(&input, (uint32_t *) &expire);
  if (Type() != kRedisString) {
    if (input.size() < 12) rocksdb::Status::InvalidArgument("the metadata was too short");
    GetFixed64(&input, &version);
    GetFixed32(&input, &size);
  }
  if (Type() == kRedisList) {
    if (input.size() < 16) rocksdb::Status::InvalidArgument("the metadata was too short");
    GetFixed64(&input, &head);
    GetFixed64(&input, &tail);
  }
  return rocksdb::Status();
}

RedisDB::RedisDB(Engine::Storage *storage, std::string ns) {
  storage_ = storage;
  metadata_cf_handle_ = storage->GetCFHandle("metadata");
  db_ = storage->GetDB();
  namespace_ = std::move(ns);
}

rocksdb::Status RedisDB::GetMetadata(RedisType type, Slice key, Metadata *metadata) {
  std::string old_metadata;
  metadata->Encode(&old_metadata);
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string bytes;
  rocksdb::Status s = db_->Get(read_options, metadata_cf_handle_, key, &bytes);
  if (!s.ok()) {
    return rocksdb::Status::NotFound();
  }
  metadata->Decode(bytes);

  if (metadata->Expired()) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound("the key was Expired");
  }
  if (metadata->Type() != type && (metadata->size > 0 || metadata->Type() == kRedisString))  {
    metadata->Decode(old_metadata);
    return rocksdb::Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  if (metadata->size == 0) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound("no elements");
  }
  return s;
}

rocksdb::Status RedisDB::Expire(Slice key, int timestamp) {
  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);

  std::string value;
  Metadata metadata(kRedisNone);
  LockGuard guard(storage_->GetLockManager(), key);
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, key, &value);
  if (!s.ok()) return s;
  metadata.Decode(value);
  if (metadata.Expired()) {
    return rocksdb::Status::NotFound("the key was expired");
  }
  if (metadata.Type() != kRedisString && metadata.size == 0) {
    return rocksdb::Status::NotFound("no elements");
  }

  char *buf = new char[value.size()];
  memcpy(buf, value.data(), value.size());
  // +1 to skip the flags
  EncodeFixed32(buf+1, (uint32_t)timestamp);
  rocksdb::WriteBatch batch;
  batch.Put(metadata_cf_handle_, key, Slice(buf, value.size()));
  s = storage_->Write(rocksdb::WriteOptions(), &batch);
  delete []buf;
  return s;
}

rocksdb::Status RedisDB::Del(Slice key) {
  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);

  std::string value;
  LockGuard guard(storage_->GetLockManager(), key);
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, key, &value);
  if (!s.ok()) return s;
  return db_->Delete(rocksdb::WriteOptions(), metadata_cf_handle_, key);
}

rocksdb::Status RedisDB::Exists(std::vector<Slice> keys, int *ret) {
  *ret = 0;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();

  rocksdb::Status s;
  std::string ns_key, value;
  for (const auto &key : keys) {
    AppendNamespacePrefix(key, &ns_key);
    s = db_->Get(read_options, metadata_cf_handle_, ns_key, &value);
    if (s.ok()) *ret += 1;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RedisDB::TTL(Slice key, int *ttl) {
  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);
  *ttl = -2; // ttl is -2 when the key does not exist or expired
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = db_->Get(read_options, metadata_cf_handle_, key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK():s;

  Metadata metadata(kRedisNone);
  metadata.Decode(value);
  *ttl = metadata.TTL();
  return rocksdb::Status::OK();
}

uint64_t RedisDB::GetKeyNum(std::string prefix) {
  return Keys(prefix, nullptr);
}

uint64_t RedisDB::Keys(std::string prefix, std::vector<std::string> *keys) {
  uint64_t  cnt = 0;
  std::string ns_prefix, ns, real_key, value;
  AppendNamespacePrefix(prefix, &ns_prefix);
  prefix = ns_prefix;

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
    Metadata metadata(kRedisNone);
    value = iter->value().ToString();
    metadata.Decode(value);
    if (metadata.Expired()) continue;
    if (keys) {
      ExtractNamespaceKey(iter->key(), &ns, &real_key);
      keys->emplace_back(real_key);
    }
    cnt++;
  }
  delete iter;
  return cnt;
}

uint64_t RedisDB::Scan(const std::string &cursor,
                       const uint64_t &limit,
                       const std::string &prefix,
                       std::vector<std::string> *keys) {
  uint64_t cnt = 0;
  if (keys == nullptr) {
    return cnt;
  }
  std::string ns_prefix, ns_cursor, ns, real_key, value;
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
    Metadata metadata(kRedisNone);
    value = iter->value().ToString();
    metadata.Decode(value);
    if (metadata.Expired()) continue;
    ExtractNamespaceKey(iter->key(), &ns, &real_key);
    keys->emplace_back(real_key);
    cnt++;
  }
  delete iter;
  return cnt;
}

rocksdb::Status RedisDB::FlushAll() {
  std::string prefix;
  AppendNamespacePrefix("", &prefix);
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, metadata_cf_handle_);
  for (iter->Seek(prefix);
       iter->Valid() && iter->key().starts_with(prefix);
       iter->Next()) {
    db_->Delete(rocksdb::WriteOptions(), metadata_cf_handle_, iter->key());
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisDB::Type(Slice key, RedisType *type) {
  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);

  *type = kRedisNone;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = db_->Get(read_options, metadata_cf_handle_, key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK():s;

  Metadata metadata(kRedisNone);
  metadata.Decode(value);
  *type = metadata.Type();
  return rocksdb::Status::OK();
}

void RedisDB::AppendNamespacePrefix(const Slice &key, std::string *output) {
  ComposeNamespaceKey(namespace_, key, output);
}

uint64_t RedisDBSubKeyScanner::Scan(RedisType type,
                                    Slice key,
                                    const std::string &cursor,
                                    const uint64_t &limit,
                                    const std::string &subkey_prefix,
                                    std::vector<std::string> *keys) {
  uint64_t cnt = 0;
  if (keys == nullptr ||
      type == kRedisString
      ) {
    return cnt;
  }

  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  key = Slice(ns_key);

  Metadata metadata(type);
  rocksdb::Status s = GetMetadata(type, key, &metadata);
  if (!s.ok()) return cnt;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);

  std::string match_prefix_key;
  if (!subkey_prefix.empty()) {
    InternalKey(key, subkey_prefix, metadata.version).Encode(&match_prefix_key);
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
    keys->emplace_back(ikey.GetSubKey().ToString());
    cnt++;
  }

  delete iter;
  return cnt;
}
