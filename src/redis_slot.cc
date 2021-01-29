#include "redis_slot.h"
#include <time.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>
#include <vector>
#include <cstdlib>
#include <atomic>
#include <string>
#include <algorithm>
#include <map>
#include <rocksdb/env.h>
#include <glog/logging.h>

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
  auto right_pos = key.find("}", left_pos + 1);
  // Note that we hash the whole key if there is nothing between {}.
  if (right_pos == std::string::npos || right_pos <= left_pos + 1) {
    return std::string();
  }

  return key.substr(left_pos + 1, right_pos - left_pos - 1);
}

static void PthreadCall(const char *label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

// Return false if timeout
static bool PthreadTimeoutCall(const char *label, int result) {
  if (result != 0) {
    if (result == ETIMEDOUT) {
      return false;
    }
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
  return true;
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

SlotMetadata::SlotMetadata(bool generate_version) {
  if (generate_version) version = generateVersion();
  size = 0;
}

rocksdb::Status SlotMetadata::Decode(const std::string &bytes) {
  // version(8bytes) + size (8byte)
  if (bytes.size() < 16) {
    return rocksdb::Status::InvalidArgument("the metadata was too short");
  }
  rocksdb::Slice input(bytes);
  GetFixed64(&input, &version);
  GetFixed64(&input, &size);
  return rocksdb::Status::OK();
}

void SlotMetadata::Encode(std::string *dst) const {
  PutFixed64(dst, version);
  PutFixed64(dst, size);
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

Mutex::Mutex() {
  PthreadCall("init mutex", pthread_mutex_init(&mu_, NULL));
}

Mutex::~Mutex() {
  PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_));
}

void Mutex::Lock() {
  PthreadCall("lock", pthread_mutex_lock(&mu_));
}

void Mutex::Unlock() {
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

CondVar::CondVar(Mutex *mu)
    : mu_(mu) {
  PthreadCall("init cv", pthread_cond_init(&cv_, NULL));
}

CondVar::~CondVar() {
  PthreadCall("destroy cv", pthread_cond_destroy(&cv_));
}

void CondVar::Wait() {
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
}

// return false if timeout
bool CondVar::TimedWait(uint32_t timeout) {
  /*
   * pthread_cond_timedwait api use absolute API
   * so we need gettimeofday + timeout
   */
  struct timeval now;
  gettimeofday(&now, NULL);
  struct timespec tsp;

  int64_t usec = now.tv_usec + timeout * 1000LL;
  tsp.tv_sec = now.tv_sec + usec / 1000000;
  tsp.tv_nsec = (usec % 1000000) * 1000;

  return PthreadTimeoutCall("timewait",
                            pthread_cond_timedwait(&cv_, &mu_->mu_, &tsp));
}

void CondVar::Signal() {
  PthreadCall("signal", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&cv_));
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
  SlotMetadata metadata(false);
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
                        int port,
                        uint64_t timeout,
                        const rocksdb::Slice &key) {
  int sock_fd;
  auto s = Util::SockConnect(host, port, &sock_fd, timeout, timeout);
  if (!s.IsOK()) {
    return Status(Status::NotOK, "connect the server err: " + s.Msg());
  }
  s = MigrateOneKey(sock_fd, key);
  close(sock_fd);
  return s;
}

Status Slot::MigrateOneKey(int sock_fd, const rocksdb::Slice &key) {
  std::string ns_key;
  AppendNamespacePrefix(key, &ns_key);
  std::string bytes;
  auto st = Database::GetRawMetadata(ns_key, &bytes);
  if (!st.ok()) return Status(Status::NotFound, st.ToString());
  Metadata metadata(kRedisNone, false);
  metadata.Decode(bytes);
  if (metadata.Expired()) {
    return Status(Status::NotFound, "the key was Expired");
  }
  if (metadata.Type() != kRedisString && metadata.size == 0) {
    return Status(Status::NotFound, "no elements");
  }

  size_t line_len;
  std::string restore_command;
  switch (metadata.Type()) {
    case kRedisString: {
      std::vector<std::string> commands = {"set", key.ToString(),
                                           bytes.substr(5, bytes.size() - 5)};
      if (metadata.expire > 0) {
        commands.emplace_back("EX");
        commands.emplace_back(std::to_string(metadata.expire));
      }
      restore_command = Redis::MultiBulkString(commands);
      break;
    }
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
  auto s = Util::SockSend(sock_fd, restore_command);
  if (!s.IsOK()) {
    return Status(Status::NotOK, "[slotsrestore] send command err:" + s.Msg());
  }
  evbuffer *evbuf = evbuffer_new();
  while (true) {
    if (evbuffer_read(evbuf, sock_fd, -1) <= 0) {
      evbuffer_free(evbuf);
      return Status(Status::NotOK, std::string("[slotsrestore] read response err: ") + strerror(errno));
    }
    char *line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) continue;
    if (line[0] == '-') {
      auto error_msg = "[slotsrestore] got invalid response: " + std::string(line);
      free(line);
      evbuffer_free(evbuf);
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
      evbuffer_free(evbuf);
      return Status(Status::NotOK, "[slotsrestore] send expire command err:" + s.Msg());
    }
    while (true) {
      if (evbuffer_read(evbuf, sock_fd, -1) <= 0) {
        evbuffer_free(evbuf);
        return Status(Status::NotOK,
                      std::string("[slotsrestore] expire command read response err: ") + strerror(errno));
      }
      char *line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) continue;
      if (line[0] == '-') {
        free(line);
        evbuffer_free(evbuf);
        return Status(Status::NotOK, "[slotsrestore] expire command got invalid response");
      }
      free(line);
      break;
    }
  }
  evbuffer_free(evbuf);
  Database::Del(key);
  return Status::OK();
}

Status Slot::MigrateSlotRandomOne(const std::string &host,
                                  int port,
                                  uint64_t timeout,
                                  uint32_t slot_num) {
  std::vector<std::string> keys;
  auto s = Scan(slot_num, std::string(), 1, &keys);
  if (!s.ok()) {
    return Status(Status::NotOK, s.ToString());
  }
  if (keys.size() == 0) {
    return Status(Status::NotOK, "slot is empty");
  }
  return MigrateOne(host, port, timeout, keys.back());
}

Status Slot::MigrateTagSlot(const std::string &host,
                            int port,
                            uint64_t timeout,
                            uint32_t slot_num,
                            int *ret) {
  *ret = 0;

  std::vector<std::string> keys;
  auto s = Scan(slot_num, std::string(), 1, &keys);
  if (!s.ok()) {
    return Status(Status::NotOK, s.ToString());
  }
  if (keys.size() == 0) {
    return Status(Status::NotOK, "slot is empty");
  }
  return MigrateTag(host, port, timeout, keys.back(), ret);
}

Status Slot::MigrateTag(const std::string &host,
                        int port,
                        uint64_t timeout,
                        const std::string &key,
                        int *ret) {
  *ret = 0;

  auto tag = GetTagFromKey(key);
  if (tag.empty()) {
    return MigrateOne(host, port, timeout, key);
  }

  auto slot_num = GetSlotNumFromKey(tag);
  SlotMetadata metadata(false);
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
        list.emplace_back(Util::Float2String(score));
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
    Metadata metadata(kRedisNone, false);
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
    SlotMetadata metadata(false);
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

rocksdb::Status Slot::GetInfo(uint32_t start, int count, std::vector<SlotCount> *slot_counts) {
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
    auto slot_num = DecodeFixed32(key.data());
    if (slot_num > max_slot_num) {
      break;
    }
    SlotMetadata metadata(false);
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
  auto s = db_->Get(rocksdb::ReadOptions(), slot_metadata_cf_handle_, metadata_key, &value);
  if (!s.ok()) return s;
  return storage_->Delete(rocksdb::WriteOptions(), slot_metadata_cf_handle_, metadata_key);
}

rocksdb::Status Slot::AddKey(const Slice &key) {
  LockGuard guard(storage_->GetLockManager(), key);

  auto slot_num = GetSlotNumFromKey(key.ToString());
  SlotMetadata metadata;
  rocksdb::Status s = GetMetadata(slot_num, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();

  std::string slot_key, raw_bytes;
  SlotInternalKey(key, metadata.version).Encode(&slot_key);
  s = db_->Get(read_options, slot_key_cf_handle_, slot_key, &raw_bytes);
  if (s.ok()) {
    return rocksdb::Status::OK();
  } else if (!s.IsNotFound()) {
    return s;
  }

  rocksdb::WriteBatch batch;
  batch.Put(slot_key_cf_handle_, slot_key, NULL);

  metadata.size++;
  std::string bytes, metadata_key;
  PutFixed32(&metadata_key, slot_num);
  metadata.Encode(&bytes);
  batch.Put(slot_metadata_cf_handle_, metadata_key, bytes);

  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Slot::DeleteKey(const Slice &key) {
  LockGuard guard(storage_->GetLockManager(), key);

  auto slot_num = GetSlotNumFromKey(key.ToString());
  SlotMetadata metadata(false);
  rocksdb::Status s = GetMetadata(slot_num, &metadata);
  if (!s.ok()) return s;

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();

  std::string slot_key, raw_bytes;
  SlotInternalKey(key, metadata.version).Encode(&slot_key);
  s = db_->Get(read_options, slot_key_cf_handle_, slot_key, &raw_bytes);
  if (!s.ok()) return s;

  rocksdb::WriteBatch batch;
  batch.Delete(slot_key_cf_handle_, slot_key);

  metadata.size--;
  std::string bytes, metadata_key;
  PutFixed32(&metadata_key, slot_num);
  metadata.Encode(&bytes);
  batch.Put(slot_metadata_cf_handle_, metadata_key, bytes);

  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Slot::DeleteAll() {
  LockGuard guard(storage_->GetLockManager(), "slots_all");

  std::string first_key, last_key;
  PutFixed32(&first_key, 0);
  PutFixed32(&last_key, HASH_SLOTS_SIZE);
  return db_->DeleteRange(rocksdb::WriteOptions(), slot_metadata_cf_handle_, first_key, last_key);
}

rocksdb::Status Slot::Size(uint32_t slot_num, uint64_t *ret) {
  *ret = 0;

  SlotMetadata metadata(false);
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
  SlotMetadata metadata(false);
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

SlotsMgrtSenderThread::~SlotsMgrtSenderThread() {
  if (is_migrating_) {
    StopMigrateSlot();
    slotsmgrt_cond_.SignalAll();
  }
}

Status SlotsMgrtSenderThread::Start() {
  try {
    t_ = std::thread([this]() {
      Util::ThreadSetName("slots-mgrt-sender-thread");
      this->loop();
    });
  } catch (const std::system_error &e) {
    return Status(Status::NotOK, e.what());
  }
  return Status::OK();
}

void SlotsMgrtSenderThread::Stop() {
  stop_ = true;
  LOG(WARNING) << "[slots-mgrt-sender-thread] stopped";
}

void SlotsMgrtSenderThread::StopMigrateSlot() {
  is_migrating_ = false;
  LOG(WARNING) << "[slots-mgrt-sender-thread] migrate slot " + std::to_string(slot_num_) + " stopped";
}

void SlotsMgrtSenderThread::Join() {
  if (t_.joinable()) t_.join();
}

Status SlotsMgrtSenderThread::SlotsMigrateOne(const std::string &key, int *ret) {
  std::lock_guard<std::mutex> guard(db_mu_);
  std::lock_guard<std::mutex> ones_guard(ones_mu_);
  Redis::Slot slot_db(storage_);

  std::string bytes;
  auto s = slot_db.GetRawMetadataByUserKey(key, &bytes);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      *ret = 0;
      return Status(Status::NotOK, "Migrate key: " + key + " not found");
    } else {
      *ret = -1;
      return Status(Status::NotOK, "Migrate one key: " + key + " error: " + s.ToString());
    }
  }
  Metadata metadata(kRedisNone, false);
  metadata.Decode(bytes);
  if (metadata.Expired()) {
    *ret = 0;
    return Status(Status::NotOK, "the key was Expired");
  }
  if (metadata.Type() != kRedisString && metadata.size == 0) {
    *ret = 0;
    return Status(Status::NotOK, "no elements");
  }

  // when this slot has been finished migrating, and the thread migrating status is reset, but the result
  // has not been returned to proxy, during this time, some more request on this slot are sent to the server,
  // so need to check if the key exists first, if not exists the proxy forwards the request to destination server
  // if the key exists, it is an error which should not happen.
  auto slot_num = GetSlotNumFromKey(key);
  if (slot_num != slot_num_) {
    *ret = -1;
    return Status(Status::NotOK,
                  "Slot : " + std::to_string(slot_num) + " is not the migrating slot:" + std::to_string(slot_num_));
  } else if (!is_migrating_) {
    *ret = -1;
    return Status(Status::NotOK, "Slot : " + std::to_string(slot_num) + " is not migrating");
  }

  if (std::find(migrating_ones_.begin(), migrating_ones_.end(), key) != migrating_ones_.end()) {
    *ret = 1;
    return Status::OK();
  }

  migrating_ones_.push_back(key);
  *ret = 1;
  return Status::OK();
}

Status SlotsMgrtSenderThread::SlotsMigrateBatch(const std::string &ip,
                                                int port,
                                                uint64_t time_out,
                                                uint32_t slot,
                                                int keys_num) {
  MutexLock guard(&slotsmgrt_cond_mu_);
  if (is_migrating_) {
    if (!(dest_ip_ == ip && dest_port_ == port && slot_num_ == slot)) {
      return Status(Status::NotOK, "wrong dest_ip, dest_port or slot_num");
    }
    timeout_ms_ = time_out;
    keys_num_.fetch_add(keys_num, std::memory_order_relaxed);
    return Status::OK();
  } else {
    Redis::Slot slot_db(storage_);
    auto s = slot_db.Size(slot_num_, &remained_keys_num_);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::NotOK, "get slot_size error");
    }
    dest_ip_ = ip;
    dest_port_ = port;
    timeout_ms_ = time_out;
    slot_num_ = slot;
    keys_num_ = keys_num;
    is_migrating_ = true;
    error_ = false;
    LOG(INFO) << "[slots-mgrt-sender-thread] Migrate batch slot: " << slot;
  }
  return Status::OK();
}

Status SlotsMgrtSenderThread::GetSlotsMigrateResult(uint64_t *moved, uint64_t *remained) {
  MutexLock guard(&slotsmgrt_cond_mu_);
  slotsmgrt_cond_.TimedWait(timeout_ms_);
  *moved = moved_keys_num_;
  *remained = remained_keys_num_;
  return Status::OK();
}

Status SlotsMgrtSenderThread::GetSlotsMgrtSenderStatus(std::string *ip,
                                                       int *port,
                                                       uint32_t *slot_num,
                                                       bool *migrating,
                                                       uint64_t *moved,
                                                       uint64_t *remained) {
  std::lock_guard<std::mutex> guard(db_mu_);
  std::lock_guard<std::mutex> ones_guard(ones_mu_);
  *ip = dest_ip_;
  *port = dest_port_;
  *slot_num = slot_num_;
  *migrating = is_migrating_;
  *moved = moved_keys_num_;
  *remained = remained_keys_num_;
  return Status::OK();
}

Status SlotsMgrtSenderThread::SlotsMigrateAsyncCancel() {
  std::lock_guard<std::mutex> guard(db_mu_);
  dest_ip_ = "none";
  dest_port_ = 0;
  timeout_ms_ = 3000;
  slot_num_ = 0;
  moved_keys_num_ = 0;
  moved_keys_all_ = 0;
  remained_keys_num_ = 0;
  StopMigrateSlot();
  std::vector<std::string>().swap(migrating_ones_);
  return Status::OK();
}

Status SlotsMgrtSenderThread::ElectMigrateKeys(std::vector<std::string> *keys) {
  std::lock_guard<std::mutex> guard(db_mu_);
  Redis::Slot slot_db(storage_);

  SlotMetadata metadata(false);
  auto s = slot_db.GetMetadata(slot_num_, &metadata);
  if (!s.ok()) {
    StopMigrateSlot();
    return Status(Status::NotOK, s.ToString());
  }
  remained_keys_num_ = metadata.size;
  if (remained_keys_num_ == 0) {
    LOG(WARNING) << "[slots-mgrt-sender-thread] No keys in slot: " << slot_num_;
    return Status::OK();
  }
  auto ss = slot_db.Scan(slot_num_, std::string(), keys_num_, keys);
  keys_num_ = 0;
  if (!ss.ok()) {
    return Status(Status::NotOK, s.ToString());
  }
  if (keys->size() == 0) {
    LOG(WARNING) << "No keys in slot: " << slot_num_;
    StopMigrateSlot();
    return Status(Status::NotOK, "slot is empty");
  }
  return Status::OK();
}

void SlotsMgrtSenderThread::loop() {
  Redis::Slot slot_db(storage_);

  while (!IsStopped()) {
    if (!is_migrating_) {
      sleep(1);
      continue;
    }
    LOG(INFO) << "[slots-mgrt-sender-thread] Start migrate slot:" << slot_num_;
    int sock_fd = 0;
    auto s = Util::SockConnect(dest_ip_, dest_port_, &sock_fd, timeout_ms_, timeout_ms_);
    if (!s.IsOK()) {
      LOG(WARNING) << "[slots-mgrt-sender-thread] Failed to connect the server ("
                   << dest_ip_ << ":" << dest_port_ << ") " << s.Msg();
      slotsmgrt_cond_.Signal();
      StopMigrateSlot();
      sleep(1);
      continue;
    }
    LOG(INFO) << "[slots-mgrt-sender-thread] Succ Connect server (" << dest_ip_ << ":" << dest_port_ << ") ";
    moved_keys_all_ = 0;
    while (is_migrating_) {
      if (keys_num_ <= 0) {
        sleep(1);
        continue;
      }
      std::vector<std::string> migrate_batch_keys;
      auto s = ElectMigrateKeys(&migrate_batch_keys);
      if (!s.IsOK()) {
        LOG(WARNING) << "[slots-mgrt-sender-thread] Failed to get batch keys: " + s.Msg();
        slotsmgrt_cond_.Signal();
        StopMigrateSlot();
        break;
      } else if (remained_keys_num_ == 0) {
        StopMigrateSlot();
        break;
      }

      {
        std::lock_guard<std::mutex> ones_guard(ones_mu_);
        // add ones to batch end; empty ones
        std::copy(migrating_ones_.begin(), migrating_ones_.end(), std::back_inserter(migrate_batch_keys));
        if (migrate_batch_keys.size() != 0) {
          moved_keys_num_ = 0;
        }
        std::vector<std::string>().swap(migrating_ones_);
      }

      for (auto const &key : migrate_batch_keys) {
        auto s = slot_db.MigrateOneKey(sock_fd, key);
        if (!s.IsOK()) {
          LOG(WARNING) << "[slots-mgrt-sender-thread] Failed to Migrate batch key " << key << ": " << s.Msg();
          if (!s.IsNotFound()) {
            slot_db.AddKey(key);
          }
          slotsmgrt_cond_.Signal();
          StopMigrateSlot();
          error_ = true;
          break;
        }
        moved_keys_num_++;
        moved_keys_all_++;
        remained_keys_num_--;
      }
      if (error_) {
        break;
      }

      if (remained_keys_num_ == 0) {
        LOG(INFO) << "[slots-mgrt-sender-thread] Migrate slot: " << slot_num_ << " finished";
        slotsmgrt_cond_.Signal();
        StopMigrateSlot();
        break;
      }
    }
    if (sock_fd != 0) {
      close(sock_fd);
    }
    if (error_) {
      sleep(1);
    }
  }
  LOG(INFO) << "[slots-mgrt-sender-thread] Stopped!";
  return;
}

}  // namespace Redis




