#include "redis_slot.h"
#include <time.h>
#include <stdlib.h>
#include <sys/time.h>
#include <rocksdb/env.h>

#include <vector>
#include <cstdlib>
#include <atomic>
#include <string>
#include <map>

#include "util.h"
#include "redis_reply.h"
#include "encoding.h"

uint32_t crc32tab[256];
void CRC32TableInit(uint32_t poly) {
  int i, j;
  for (i = 0; i < 256; i++) {
    uint32_t crc = i;
    for (j = 0; j < 8; j++) {
      if (crc & 1) {
        crc = (crc >> 1) ^ poly;
      } else {
        crc = (crc >> 1);
      }
    }
    crc32tab[i] = crc;
  }
}

void InitCRC32Table() {
  CRC32TableInit(IEEE_POLY);
}

uint32_t CRC32Update(uint32_t crc, const char *buf, int len) {
  int i;
  crc = ~crc;
  for (i = 0; i < len; i++) {
    crc = crc32tab[(uint8_t) (static_cast<char>(crc) ^ buf[i])] ^ (crc >> 8);
  }
  return ~crc;
}

uint32_t GetSlotNumFromKey(const std::string &key) {
  auto tag = GetTagFromKey(key);
  if (tag.empty()) {
    tag = key;
  }
  auto crc = CRC32Update(0, tag.data(), static_cast<int>(tag.size()));
  return static_cast<int>(crc & HASH_SLOTS_MASK);
}

std::string GetTagFromKey(const std::string &key) {
  auto left_pos = key.find("{");
  if (left_pos == std::string::npos) return std::string();
  auto right_pos = key.find("}");
  if (right_pos == std::string::npos || right_pos < left_pos) return std::string();

  return key.substr(left_pos + 1, right_pos - left_pos - 1);
}

SlotInternalKey::SlotInternalKey(rocksdb::Slice input) {
  GetFixed32(&input, &slot_num_);
  GetFixed64(&input, &version_);
  key_ = Slice(input.data(), input.size());
  buf_ = nullptr;
  memset(prealloc_, '\0', sizeof(prealloc_));
}

SlotInternalKey::SlotInternalKey(rocksdb::Slice key, uint64_t version) {
  slot_num_ = GetSlotNumFromKey(key.ToString());
  key_ = key;
  version_ = version;
  buf_ = nullptr;
  memset(prealloc_, '\0', sizeof(prealloc_));
}

SlotInternalKey::SlotInternalKey(uint32_t slot_num, uint64_t version) {
  slot_num_ = slot_num;
  version_ = version;
  buf_ = nullptr;
  memset(prealloc_, '\0', sizeof(prealloc_));
}

SlotInternalKey::~SlotInternalKey() {
  if (buf_ != nullptr && buf_ != prealloc_) delete[]buf_;
}

rocksdb::Slice SlotInternalKey::GetKey() const {
  return key_;
}

uint64_t SlotInternalKey::GetSlotNum() const {
  return slot_num_;
}

uint64_t SlotInternalKey::GetVersion() const {
  return version_;
}

void SlotInternalKey::Encode(std::string *out) {
  out->clear();
  size_t pos = 0;
  size_t total = 4 + 8 + key_.size();
  if (total < sizeof(prealloc_)) {
    buf_ = prealloc_;
  } else {
    buf_ = new char[total];
  }
  EncodeFixed32(buf_ + pos, slot_num_);
  pos += 4;
  EncodeFixed64(buf_ + pos, version_);
  pos += 8;
  memcpy(buf_ + pos, key_.data(), key_.size());
  pos += key_.size();
  out->assign(buf_, pos);
}

bool SlotInternalKey::operator==(const SlotInternalKey &that) const {
  if (key_ != that.key_) return false;
  return version_ == that.version_;
}

SlotMetadata::SlotMetadata() {
  version = generateVersion();
  size = 0;
}

rocksdb::Status SlotMetadata::Decode(const std::string &bytes) {
  // version(8bytes) + size (4byte)
  if (bytes.size() < 12) {
    return rocksdb::Status::InvalidArgument("the metadata was too short");
  }
  rocksdb::Slice input(bytes);
  GetFixed64(&input, &version);
  GetFixed32(&input, &size);
  return rocksdb::Status::OK();
}

void SlotMetadata::Encode(std::string *dst) const {
  PutFixed64(dst, version);
  PutFixed32(dst, size);
}

uint64_t SlotMetadata::generateVersion() {
  struct timeval now;
  gettimeofday(&now, nullptr);
  uint64_t version = static_cast<uint64_t >(now.tv_sec) * 1000000;
  version += static_cast<uint64_t>(now.tv_usec);
  // use random position for initial counter to avoid conflicts,
  // when the slave was promoted as master and the system clock may backoff
  srand(static_cast<unsigned>(now.tv_sec));
  static std::atomic<uint64_t> version_counter_{static_cast<uint64_t>(std::rand())};
  uint64_t counter = version_counter_.fetch_add(1);
  return (version << VersionCounterBits) + (counter % (1 << VersionCounterBits));
}

bool SlotMetadata::operator==(const SlotMetadata &that) const {
  if (size != that.size) return false;
  if (version != that.version) return false;
  return true;
}

namespace Redis {

rocksdb::Status Slot::GetMetadata(uint32_t slot_num, SlotMetadata *metadata) {
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string bytes, metadata_key;
  PutFixed32(&metadata_key, slot_num);
  rocksdb::Status s = db_->Get(read_options, slot_metadata_cf_handle_, metadata_key, &bytes);
  if (!s.ok()) return s;
  metadata->Decode(bytes);
  return s;
}

rocksdb::Status Slot::IsKeyExist(const Slice &key) {
  auto slot_num = GetSlotNumFromKey(key.ToString());
  SlotMetadata metadata;
  rocksdb::Status s = GetMetadata(slot_num, &metadata);
  if (!s.ok()) return s;

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();

  std::string slot_key, raw_bytes;
  SlotInternalKey(key, metadata.version).Encode(&slot_key);
  s = db_->Get(read_options, slot_key_cf_handle_, slot_key, &raw_bytes);
  if (!s.ok()) return s;
  return rocksdb::Status::OK();
}

Status Slot::MigrateOne(const std::string &host,
                        uint32_t port,
                        uint64_t timeout,
                        const rocksdb::Slice &key) {
  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string bytes;
  rocksdb::Status st = db_->Get(read_options, metadata_cf_handle_, ns_key, &bytes);
  if (!st.ok()) return Status(Status::RedisExecErr, st.ToString());

  Metadata metadata(kRedisNone);
  metadata.Decode(bytes);

  if (metadata.Expired()) {
    return Status(Status::RedisExecErr, "the key was Expired");
  }
  if (metadata.Type() != kRedisString && metadata.size == 0) {
    return Status(Status::RedisExecErr, "no elements");
  }

  int sock_fd;
  Status s = Util::SockConnect(host, port, &sock_fd, timeout, timeout);
  if (!s.IsOK()) {
    return Status(Status::NotOK, "connect the server err: " + s.Msg());
  }

  size_t line_len;
  evbuffer *evbuf = evbuffer_new();
  std::string restore_command;
  switch (metadata.Type()) {
    case kRedisString:
      restore_command =
          Redis::MultiBulkString({"setex", key.ToString(), std::to_string(metadata.expire),
                                  bytes.substr(5, bytes.size() - 5)});
      break;
    case kRedisList:
    case kRedisZSet:
    case kRedisBitmap:
    case kRedisHash:
    case kRedisSet:
    case kRedisSortedint: {
      auto s = generateMigrateCommandComplexKV(key, metadata, &restore_command);
      if (!s.IsOK()) {
        return s;
      }
    }
    default:break;  // should never get here
  }
  s = Util::SockSend(sock_fd, restore_command);
  if (!s.IsOK()) {
    close(sock_fd);
    return Status(Status::NotOK, "[slotsrestore] send command err:" + s.Msg());
  }
  while (true) {
    if (evbuffer_read(evbuf, sock_fd, -1) <= 0) {
      evbuffer_free(evbuf);
      close(sock_fd);
      return Status(Status::NotOK, std::string("[slotsrestore] read response err: ") + strerror(errno));
    }
    char *line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) continue;
    if (line[0] == '-') {
      auto error_msg = "[slotsrestore] got invalid response: " + std::string(line);
      free(line);
      evbuffer_free(evbuf);
      close(sock_fd);
      return Status(Status::NotOK, error_msg);
    }
    free(line);
    break;
  }
  if (metadata.Type() != kRedisString && metadata.expire != 0) {
    auto ttl_command =
        Redis::MultiBulkString({"EXPIREAT", key.ToString(), std::to_string(metadata.expire)});
    s = Util::SockSend(sock_fd, ttl_command);
    if (!s.IsOK()) {
      close(sock_fd);
      return Status(Status::NotOK, "[slotsrestore] send expire command err:" + s.Msg());
    }
    while (true) {
      if (evbuffer_read(evbuf, sock_fd, -1) <= 0) {
        evbuffer_free(evbuf);
        close(sock_fd);
        return Status(Status::NotOK,
                      std::string("[slotsrestore] expire command read response err: ") + strerror(errno));
      }
      char *line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) continue;
      if (line[0] == '-') {
        free(line);
        evbuffer_free(evbuf);
        close(sock_fd);
        return Status(Status::NotOK, "[slotsrestore] expire command got invalid response");
      }
      free(line);
      break;
    }
  }
  evbuffer_free(evbuf);
  close(sock_fd);
  Database::Del(key);
  return Status::OK();
}

Status Slot::MigrateSlotRandomOne(const std::string &host,
                                  uint32_t port,
                                  uint64_t timeout,
                                  uint32_t slot_num) {
  std::vector<std::string> keys;
  auto s = Scan(slot_num, std::string(), 1, &keys);
  if (!s.ok()) {
    return Status(Status::RedisExecErr, s.ToString());
  }
  if (keys.size() == 0) {
    return Status(Status::NotOK, "slot is empty");
  }
  return MigrateOne(host, port, timeout, keys.back());
}

Status Slot::MigrateTagSlot(const std::string &host,
                            uint32_t port,
                            uint64_t timeout,
                            uint32_t slot_num,
                            int *ret) {
  *ret = 0;

  std::vector<std::string> keys;
  auto s = Scan(slot_num, std::string(), 1, &keys);
  if (!s.ok()) {
    return Status(Status::RedisExecErr, s.ToString());
  }
  if (keys.size() == 0) {
    return Status(Status::NotOK, "slot is empty");
  }
  return MigrateTag(host, port, timeout, keys.back(), ret);
}

Status Slot::MigrateTag(const std::string &host,
                        uint32_t port,
                        uint64_t timeout,
                        const std::string &key,
                        int *ret) {
  *ret = 0;

  auto tag = GetTagFromKey(key);
  if (tag.empty()) {
    return MigrateOne(host, port, timeout, key);
  }

  auto slot_num = GetSlotNumFromKey(tag);
  SlotMetadata metadata;
  rocksdb::Status s = GetMetadata(slot_num, &metadata);
  if (!s.ok()) return Status(Status::NotOK, s.ToString());

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, slot_key_cf_handle_);
  std::string prefix_key;
  SlotInternalKey(slot_num, metadata.version).Encode(&prefix_key);
  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix_key)) {
      break;
    }
    SlotInternalKey ikey(iter->key());
    auto k = ikey.GetKey().ToString();
    auto t = GetTagFromKey(k);
    if (t == tag) {
      auto s = MigrateOne(host, port, timeout, k);
      if (s.IsOK()) {
        *ret += 1;
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status Slot::generateMigrateCommandComplexKV(const Slice &key, const Metadata &metadata, std::string *output) {
  output->clear();

  std::string cmd;
  switch (metadata.Type()) {
    case kRedisList:cmd = "rpush";
      break;
    case kRedisZSet:cmd = "zadd";
      break;
    case kRedisBitmap:cmd = "msetbit";
      break;
    case kRedisHash:cmd = "hmset";
      break;
    case kRedisSet:cmd = "sadd";
      break;
    case kRedisSortedint:cmd = "siadd";
      break;
    default:break;  // should never get here
  }
  std::vector<std::string> list = {cmd, key.ToString()};

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);
  std::string prefix_key, ns_key;
  AppendNamespacePrefix(key, &ns_key);
  InternalKey(ns_key, "", metadata.version).Encode(&prefix_key);
  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix_key)) {
      break;
    }
    InternalKey ikey(iter->key());
    switch (metadata.Type()) {
      case kRedisSet: {
        list.emplace_back(ikey.GetSubKey().ToString());
        break;
      }
      case kRedisSortedint: {
        auto id = DecodeFixed64(ikey.GetSubKey().ToString().data());
        list.emplace_back(std::to_string(id));
        break;
      }
      case kRedisZSet: {
        auto score = DecodeDouble(iter->value().ToString().data());
        list.emplace_back(std::to_string(score));
        list.emplace_back(ikey.GetSubKey().ToString());
        break;
      }
      case kRedisBitmap:
      case kRedisHash: {
        list.emplace_back(ikey.GetSubKey().ToString());
        list.emplace_back(iter->value().ToString());
        break;
      }
      case kRedisList: {
        list.emplace_back(iter->value().ToString());
        break;
      }
      default:break;  // should never get here
    }
  }
  delete iter;

  *output = Redis::MultiBulkString(list);
  return Status::OK();
}

rocksdb::Status Slot::Check() {
  std::string ns, user_key, value;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  // check cf_metadata against cf_slot
  auto iter = db_->NewIterator(read_options, metadata_cf_handle_);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Metadata metadata(kRedisNone);
    value = iter->value().ToString();
    metadata.Decode(value);
    if (metadata.Expired()) continue;
    ExtractNamespaceKey(iter->key(), &ns, &user_key);

    auto s = IsKeyExist(user_key);
    if (!s.ok()) {
      return rocksdb::Status::NotFound("cf_metadata key not in cf_slot: " + user_key);
    }
  }
  delete iter;
  // check cf_slot against cf_metadata
  iter = db_->NewIterator(read_options, slot_key_cf_handle_);
  int ret;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    SlotInternalKey ikey(iter->key());
    auto key = ikey.GetKey().ToString();
    auto slot_num = GetSlotNumFromKey(key);
    SlotMetadata metadata;
    rocksdb::Status s = GetMetadata(slot_num, &metadata);
    if (!s.ok()) {
      continue;
    }
    if (ikey.GetVersion() != metadata.version) {
      continue;
    }
    Exists({key}, &ret);
    if (ret != 1) {
      return rocksdb::Status::NotFound("cf_slot key not in cf_metadata: " + key);
    }
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status Slot::GetInfo(uint64_t start, uint64_t count, std::vector<SlotCount> *slot_counts) {
  std::string value;
  auto max_slot_num = start + count;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, slot_metadata_cf_handle_);

  std::string start_key;
  PutFixed32(&start_key, 0);

  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    auto key = iter->key().ToString();
    if (key == codis_enabled_status_key_) continue;
    auto slot_num = DecodeFixed32(key.data());
    if (slot_num > max_slot_num) {
      break;
    }
    SlotMetadata metadata;
    metadata.Decode(iter->value().ToString());
    slot_counts->emplace_back(SlotCount{(uint32_t) slot_num, metadata.size});
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status Slot::Del(uint32_t slot_num) {
  LockGuard guard(storage_->GetLockManager(), std::to_string(slot_num));

  std::string value, metadata_key;
  PutFixed32(&metadata_key, slot_num);
  rocksdb::Status
      s = db_->Get(rocksdb::ReadOptions(), slot_metadata_cf_handle_, metadata_key, &value);
  if (!s.ok()) return s;
  return storage_->Delete(rocksdb::WriteOptions(), slot_metadata_cf_handle_, metadata_key);
}

rocksdb::Status Slot::DeleteAll() {
  LockGuard guard(storage_->GetLockManager(), "slots_all");

  std::string first_key, last_key;
  PutFixed32(&first_key, 0);
  PutFixed32(&last_key, HASH_SLOTS_SIZE);
  return db_->DeleteRange(rocksdb::WriteOptions(), slot_metadata_cf_handle_, first_key, last_key);
}

rocksdb::Status Slot::Size(uint32_t slot_num, uint32_t *ret) {
  *ret = 0;

  SlotMetadata metadata;
  auto s = GetMetadata(slot_num, &metadata);
  if (!s.ok()) return s;
  *ret = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status Slot::UpdateKeys(const std::vector<std::string> &put_keys,
                                 const std::vector<std::string> &delete_keys,
                                 rocksdb::WriteBatch *updates) {
  std::map<uint32_t, SlotMetadata> metadatas;

  for (const auto &key : put_keys) {
    auto slot_num = GetSlotNumFromKey(key);
    auto iter = metadatas.find(slot_num);
    if (iter == metadatas.end()) {
      SlotMetadata metadata;
      auto s = GetMetadata(slot_num, &metadata);
      if (!s.ok() && !s.IsNotFound()) return s;
      metadatas[slot_num] = metadata;
    }
    auto s = IsKeyExist(key);
    if (!s.ok()) {
      std::string slot_key;
      SlotInternalKey(key, metadatas[slot_num].version).Encode(&slot_key);
      metadatas[slot_num].size++;
      updates->Put(slot_key_cf_handle_, slot_key, NULL);
    }
  }

  for (const auto &key : delete_keys) {
    auto slot_num = GetSlotNumFromKey(key);
    auto iter = metadatas.find(slot_num);
    if (iter == metadatas.end()) {
      SlotMetadata metadata;
      auto s = GetMetadata(slot_num, &metadata);
      if (!s.ok() && !s.IsNotFound()) return s;
      metadatas[slot_num] = metadata;
    }
    std::string slot_key;
    SlotInternalKey(key, metadatas[slot_num].version).Encode(&slot_key);
    auto s = IsKeyExist(key);
    if (s.ok()) {
      metadatas[slot_num].size--;
      updates->Delete(slot_key_cf_handle_, slot_key);
    }
  }

  for (const auto &iter : metadatas) {
    std::string bytes, metadata_key;
    PutFixed32(&metadata_key, iter.first);
    iter.second.Encode(&bytes);
    updates->Put(slot_metadata_cf_handle_, metadata_key, bytes);
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Slot::Scan(uint32_t slot_num,
                           const std::string &cursor,
                           uint64_t limit,
                           std::vector<std::string> *keys) {
  uint64_t cnt = 0;
  SlotMetadata metadata;
  rocksdb::Status s = GetMetadata(slot_num, &metadata);
  if (!s.ok()) return s;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, slot_key_cf_handle_);
  std::string prefix_key, start_key;
  SlotInternalKey(slot_num, metadata.version).Encode(&prefix_key);
  if (!cursor.empty()) {
    SlotInternalKey(cursor, metadata.version).Encode(&start_key);
  } else {
    start_key = prefix_key;
  }
  for (iter->Seek(start_key); iter->Valid() && cnt < limit; iter->Next()) {
    if (!cursor.empty() && iter->key() == start_key) {
      // if cursor is not empty, then we need to skip start_key
      // because we already return that key in the last scan
      continue;
    }
    if (!iter->key().starts_with(prefix_key)) {
      break;
    }
    SlotInternalKey ikey(iter->key());
    keys->emplace_back(ikey.GetKey().ToString());
    cnt++;
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status Slot::Restore(const std::vector<KeyValue> &key_values) {
  return rocksdb::Status::OK();
}

Status Slot::CheckCodisEnabledStatus(bool enabled) {
  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  std::string raw_bytes;
  auto s = db_->Get(read_options, slot_metadata_cf_handle_, codis_enabled_status_key_, &raw_bytes);
  if (!s.ok() && !s.IsNotFound()) return Status(Status::DBOpenErr, "get codis enabled status error");

  if (s.IsNotFound()) {
    db_->Put(rocksdb::WriteOptions(),
             slot_metadata_cf_handle_,
             codis_enabled_status_key_,
             std::to_string(enabled));
  } else {
    auto codis_enabled = raw_bytes.data();
    if (codis_enabled != std::to_string(enabled)) {
      return Status(Status::DBOpenErr, "codis enabled status mismatch");
    }
  }
  return Status::OK();
}

}  // namespace Redis




