#include "t_metadata.h"
#include <time.h>

InternalKey::InternalKey(Slice input) {
  uint32_t key_size;
  GetFixed32(&input, &key_size);
  key_ = Slice(input.data(), key_size);
  input.remove_prefix(key_size);
  GetFixed64(&input, &version_);
  sub_key_ = Slice(input.data(), input.size());
  buf_ = nullptr;
}

InternalKey::InternalKey(Slice key, Slice sub_key, uint64_t version) {
  key_ = key;
  sub_key_ = sub_key;
  version_ = version;
  buf_ = nullptr;
}

InternalKey::~InternalKey() {
  if (buf_ != nullptr && buf_!=prealloc_) delete []buf_;
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
  size_t total = 4+key_.size()+8+sub_key_.size();
  if (total < sizeof(prealloc_)) {
    buf_ = prealloc_;
  } else {
    buf_ = new char[total];
  }
  EncodeFixed32(buf_+pos, (uint32_t) key_.size());
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
    GetFixed64(&input, &version);
    GetFixed32(&input, &size);
  }
  if (Type() == kRedisList) {
    GetFixed64(&input, &head);
    GetFixed64(&input, &tail);
  }
  return rocksdb::Status();
}

RedisDB::RedisDB(Engine::Storage *db_wrapper) {
  storage = db_wrapper;
  metadata_cf_handle_ = db_wrapper->GetCFHandle("metadata");
  db_ = db_wrapper->GetDB();
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
  std::string value;
  Metadata metadata(kRedisNone);
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, key, &value);
  if (!s.ok()) return s;
  metadata.Decode(value);
  if (metadata.Expired()) {
    return rocksdb::Status::NotFound("the key was expired");
  }
  if (metadata.Type() != kRedisString && metadata.size == 0) {
    return rocksdb::Status::NotFound("no elements");
  }

  RWLocksGuard guard(storage->GetLocks(), key);
  char *buf = new char[value.size()];
  memcpy(buf, value.data(), value.size());
  // +1 to skip the flags
  EncodeFixed32(buf+1, (uint32_t)timestamp);
  s = db_->Put(rocksdb::WriteOptions(), metadata_cf_handle_, key, Slice(buf, value.size()));
  delete []buf;
  return s;
}

rocksdb::Status RedisDB::Del(Slice key) {
  std::string value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, key, &value);
  if (!s.ok()) return s;

  RWLocksGuard guard(storage->GetLocks(), key);
  return db_->Delete(rocksdb::WriteOptions(), metadata_cf_handle_, key);
}

rocksdb::Status RedisDB::Exists(std::vector<Slice> keys, int *ret) {
  *ret = 0;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::vector<std::string> values;
  std::vector<rocksdb::ColumnFamilyHandle *>handles{metadata_cf_handle_};
  std::vector<rocksdb::Status> statuses = db_->MultiGet(read_options, handles, keys, &values);
  for (const auto status : statuses) {
    if (status.ok()) *ret += 1;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RedisDB::TTL(Slice key, int *ttl) {
  *ttl = -2; // ttl is -2 when the key does not exist or expired
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK():s;

  Metadata metadata(kRedisNone);
  metadata.Decode(value);
  *ttl = metadata.TTL();
  return rocksdb::Status::OK();
}

rocksdb::Status RedisDB::Type(Slice key, RedisType *type) {
  *type = kRedisNone;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK():s;

  Metadata metadata(kRedisNone);
  metadata.Decode(value);
  *type = metadata.Type();
  return rocksdb::Status::OK();
}
