/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "redis_db.h"

#include <ctime>
#include <map>
#include <utility>

#include "cluster/redis_slot.h"
#include "db_util.h"
#include "parse_util.h"
#include "rocksdb/iterator.h"
#include "server/server.h"
#include "storage/redis_metadata.h"
#include "time_util.h"

namespace Redis {

Database::Database(Engine::Storage *storage, std::string ns) : storage_(storage), namespace_(std::move(ns)) {
  metadata_cf_handle_ = storage->GetCFHandle("metadata");
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
    return rocksdb::Status::NotFound(kErrMsgKeyExpired);
  }
  if (metadata->Type() != type &&
      (metadata->size > 0 || metadata->Type() == kRedisString || metadata->Type() == kRedisStream)) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::InvalidArgument(kErrMsgWrongType);
  }
  if (metadata->size == 0 && type != kRedisStream) {  // stream is allowed to be empty
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound("no elements");
  }
  return s;
}

rocksdb::Status Database::GetRawMetadata(const Slice &ns_key, std::string *bytes) {
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  return storage_->Get(read_options, metadata_cf_handle_, ns_key, bytes);
}

rocksdb::Status Database::GetRawMetadataByUserKey(const Slice &user_key, std::string *bytes) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  return GetRawMetadata(ns_key, bytes);
}

rocksdb::Status Database::Expire(const Slice &user_key, uint64_t timestamp) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  std::string value;
  Metadata metadata(kRedisNone, false);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s;
  metadata.Decode(value);
  if (metadata.Expired()) {
    return rocksdb::Status::NotFound(kErrMsgKeyExpired);
  }
  if (metadata.Type() != kRedisString && metadata.size == 0) {
    return rocksdb::Status::NotFound("no elements");
  }
  if (metadata.expire == timestamp) return rocksdb::Status::OK();

  // +1 to skip the flags
  if (metadata.Is64BitEncoded()) {
    EncodeFixed64(value.data() + 1, timestamp);
  } else {
    EncodeFixed32(value.data() + 1, Metadata::ExpireMsToS(timestamp));
  }
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisNone, {std::to_string(kRedisCmdExpire)});
  batch->PutLogData(log_data.Encode());
  batch->Put(metadata_cf_handle_, ns_key, value);
  s = storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  return s;
}

rocksdb::Status Database::Del(const Slice &user_key) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  std::string value;
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s;
  Metadata metadata(kRedisNone, false);
  metadata.Decode(value);
  if (metadata.Expired()) {
    return rocksdb::Status::NotFound(kErrMsgKeyExpired);
  }
  return storage_->Delete(storage_->DefaultWriteOptions(), metadata_cf_handle_, ns_key);
}

rocksdb::Status Database::Exists(const std::vector<Slice> &keys, int *ret) {
  *ret = 0;
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();

  rocksdb::Status s;
  std::string ns_key, value;
  for (const auto &key : keys) {
    AppendNamespacePrefix(key, &ns_key);
    s = storage_->Get(read_options, metadata_cf_handle_, ns_key, &value);
    if (s.ok()) {
      Metadata metadata(kRedisNone, false);
      metadata.Decode(value);
      if (!metadata.Expired()) *ret += 1;
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::TTL(const Slice &user_key, int64_t *ttl) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  *ttl = -2;  // ttl is -2 when the key does not exist or expired
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = storage_->Get(read_options, metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  Metadata metadata(kRedisNone, false);
  metadata.Decode(value);
  *ttl = metadata.TTL();

  return rocksdb::Status::OK();
}

void Database::GetKeyNumStats(const std::string &prefix, KeyNumStats *stats) { Keys(prefix, nullptr, stats); }

void Database::Keys(const std::string &prefix, std::vector<std::string> *keys, KeyNumStats *stats) {
  uint16_t slot_id = 0;
  std::string ns_prefix, ns, user_key, value;
  if (namespace_ != kDefaultNamespace || keys != nullptr) {
    if (storage_->IsSlotIdEncoded()) {
      ComposeNamespaceKey(namespace_, "", &ns_prefix, false);
      if (!prefix.empty()) {
        PutFixed16(&ns_prefix, slot_id);
        ns_prefix.append(prefix);
      }
    } else {
      AppendNamespacePrefix(prefix, &ns_prefix);
    }
  }

  uint64_t ttl_sum = 0;
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  storage_->SetReadOptions(read_options);
  auto iter = DBUtil::UniqueIterator(storage_, read_options, metadata_cf_handle_);

  while (true) {
    ns_prefix.empty() ? iter->SeekToFirst() : iter->Seek(ns_prefix);
    for (; iter->Valid(); iter->Next()) {
      if (!ns_prefix.empty() && !iter->key().starts_with(ns_prefix)) {
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
        int64_t ttl = metadata.TTL();
        stats->n_key++;
        if (ttl != -1) {
          stats->n_expires++;
          if (ttl > 0) ttl_sum += ttl;
        }
      }
      if (keys) {
        ExtractNamespaceKey(iter->key(), &ns, &user_key, storage_->IsSlotIdEncoded());
        keys->emplace_back(user_key);
      }
    }

    if (!storage_->IsSlotIdEncoded()) break;
    if (prefix.empty()) break;
    if (++slot_id >= HASH_SLOTS_SIZE) break;

    ComposeNamespaceKey(namespace_, "", &ns_prefix, false);
    PutFixed16(&ns_prefix, slot_id);
    ns_prefix.append(prefix);
  }

  if (stats && stats->n_expires > 0) {
    stats->avg_ttl = ttl_sum / stats->n_expires / 1000;
  }
}

rocksdb::Status Database::Scan(const std::string &cursor, uint64_t limit, const std::string &prefix,
                               std::vector<std::string> *keys, std::string *end_cursor) {
  end_cursor->clear();
  uint64_t cnt = 0;
  uint16_t slot_start = 0;
  std::string ns_prefix, ns_cursor, ns, user_key, value, index_key;

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  storage_->SetReadOptions(read_options);
  auto iter = DBUtil::UniqueIterator(storage_, read_options, metadata_cf_handle_);

  AppendNamespacePrefix(cursor, &ns_cursor);
  if (storage_->IsSlotIdEncoded()) {
    slot_start = cursor.empty() ? 0 : GetSlotNumFromKey(cursor);
    ComposeNamespaceKey(namespace_, "", &ns_prefix, false);
    if (!prefix.empty()) {
      PutFixed16(&ns_prefix, slot_start);
      ns_prefix.append(prefix);
    }
  } else {
    AppendNamespacePrefix(prefix, &ns_prefix);
  }

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

  uint16_t slot_id = slot_start;
  while (true) {
    for (; iter->Valid() && cnt < limit; iter->Next()) {
      if (!ns_prefix.empty() && !iter->key().starts_with(ns_prefix)) {
        break;
      }
      Metadata metadata(kRedisNone, false);
      value = iter->value().ToString();
      metadata.Decode(value);
      if (metadata.Expired()) continue;
      ExtractNamespaceKey(iter->key(), &ns, &user_key, storage_->IsSlotIdEncoded());
      keys->emplace_back(user_key);
      cnt++;
    }

    if (!storage_->IsSlotIdEncoded() || prefix.empty()) {
      if (!keys->empty()) {
        end_cursor->append(user_key);
      }
      break;
    }

    if (cnt >= limit) {
      end_cursor->append(user_key);
      break;
    }

    if (++slot_id >= HASH_SLOTS_SIZE) {
      break;
    }

    if (slot_id > slot_start + HASH_SLOTS_MAX_ITERATIONS) {
      if (keys->empty()) {
        if (iter->Valid()) {
          ExtractNamespaceKey(iter->key(), &ns, &user_key, storage_->IsSlotIdEncoded());
          auto res = std::mismatch(prefix.begin(), prefix.end(), user_key.begin());
          if (res.first == prefix.end()) {
            keys->emplace_back(user_key);
          }

          end_cursor->append(user_key);
        }
      } else {
        end_cursor->append(user_key);
      }
      break;
    }

    ComposeNamespaceKey(namespace_, "", &ns_prefix, false);
    PutFixed16(&ns_prefix, slot_id);
    ns_prefix.append(prefix);
    iter->Seek(ns_prefix);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::RandomKey(const std::string &cursor, std::string *key) {
  key->clear();

  std::string end_cursor;
  std::vector<std::string> keys;
  auto s = Scan(cursor, RANDOM_KEY_SCAN_LIMIT, "", &keys, &end_cursor);
  if (!s.ok()) {
    return s;
  }
  if (keys.empty() && !cursor.empty()) {
    // if reach the end, restart from beginning
    s = Scan("", RANDOM_KEY_SCAN_LIMIT, "", &keys, &end_cursor);
    if (!s.ok()) {
      return s;
    }
  }
  if (!keys.empty()) {
    unsigned int seed = Util::GetTimeStamp();
    *key = keys.at(rand_r(&seed) % keys.size());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::FlushDB() {
  std::string prefix, begin_key, end_key;
  ComposeNamespaceKey(namespace_, "", &prefix, false);
  auto s = FindKeyRangeWithPrefix(prefix, std::string(), &begin_key, &end_key);
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
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  storage_->SetReadOptions(read_options);
  auto iter = DBUtil::UniqueIterator(storage_, read_options, metadata_cf_handle_);
  iter->SeekToFirst();
  if (!iter->Valid()) {
    return rocksdb::Status::OK();
  }
  auto first_key = iter->key().ToString();
  iter->SeekToLast();
  if (!iter->Valid()) {
    return rocksdb::Status::OK();
  }
  auto last_key = iter->key().ToString();
  auto s = storage_->DeleteRange(first_key, last_key);
  if (!s.ok()) {
    return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::Dump(const Slice &user_key, std::vector<std::string> *infos) {
  infos->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = storage_->Get(read_options, metadata_cf_handle_, ns_key, &value);
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
  infos->emplace_back(std::to_string(Metadata::ExpireMsToS(metadata.expire)));
  infos->emplace_back("pexpire");
  infos->emplace_back(std::to_string(metadata.expire));
  infos->emplace_back("size");
  infos->emplace_back(std::to_string(metadata.size));
  infos->emplace_back("is_64bit_common_field");
  infos->emplace_back(std::to_string(metadata.Is64BitEncoded()));

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
    ListMetadata list_metadata(false);
    GetMetadata(kRedisList, ns_key, &list_metadata);
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
    infos->emplace_back("head");
    infos->emplace_back(std::to_string(list_metadata.head));
    infos->emplace_back("tail");
    infos->emplace_back(std::to_string(list_metadata.tail));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Database::Type(const Slice &user_key, RedisType *type) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  *type = kRedisNone;
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = storage_->Get(read_options, metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  Metadata metadata(kRedisNone, false);
  metadata.Decode(value);
  if (metadata.Expired()) {
    *type = kRedisNone;
  } else {
    *type = metadata.Type();
  }
  return rocksdb::Status::OK();
}

void Database::AppendNamespacePrefix(const Slice &user_key, std::string *output) {
  ComposeNamespaceKey(namespace_, user_key, output, storage_->IsSlotIdEncoded());
}

rocksdb::Status Database::FindKeyRangeWithPrefix(const std::string &prefix, const std::string &prefix_end,
                                                 std::string *begin, std::string *end,
                                                 rocksdb::ColumnFamilyHandle *cf_handle) {
  if (cf_handle == nullptr) {
    cf_handle = metadata_cf_handle_;
  }
  if (prefix.empty()) {
    return rocksdb::Status::NotFound();
  }
  begin->clear();
  end->clear();

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  storage_->SetReadOptions(read_options);
  auto iter = DBUtil::UniqueIterator(storage_, read_options, cf_handle);
  iter->Seek(prefix);
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    return rocksdb::Status::NotFound();
  }
  *begin = iter->key().ToString();

  // it's ok to increase the last char in prefix as the boundary of the prefix
  // while we limit the namespace last char shouldn't be larger than 128.
  std::string next_prefix;
  if (!prefix_end.empty()) {
    next_prefix = prefix_end;
  } else {
    next_prefix = prefix;
    char last_char = next_prefix.back();
    last_char++;
    next_prefix.pop_back();
    next_prefix.push_back(last_char);
  }
  iter->SeekForPrev(next_prefix);
  int max_prev_limit = 128;  // prevent unpredicted long while loop
  int i = 0;
  // reversed seek the key til with prefix or end of the iterator
  while (i++ < max_prev_limit && iter->Valid() && !iter->key().starts_with(prefix)) {
    iter->Prev();
  }
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    return rocksdb::Status::NotFound();
  }
  *end = iter->key().ToString();
  return rocksdb::Status::OK();
}

rocksdb::Status Database::ClearKeysOfSlot(const rocksdb::Slice &ns, int slot) {
  if (!storage_->IsSlotIdEncoded()) {
    return rocksdb::Status::Aborted("It is not in cluster mode");
  }

  std::string prefix, prefix_end;
  ComposeSlotKeyPrefix(ns, slot, &prefix);
  ComposeSlotKeyPrefix(ns, slot + 1, &prefix_end);
  auto s = storage_->DeleteRange(prefix, prefix_end);
  if (!s.ok()) {
    return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::GetSlotKeysInfo(int slot, std::map<int, uint64_t> *slotskeys, std::vector<std::string> *keys,
                                          int count) {
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  storage_->SetReadOptions(read_options);

  auto iter = DBUtil::UniqueIterator(storage_, read_options, metadata_cf_handle_);
  bool end = false;
  for (int i = 0; i < HASH_SLOTS_SIZE; i++) {
    std::string prefix;
    ComposeSlotKeyPrefix(namespace_, i, &prefix);
    uint64_t total = 0;
    int cnt = 0;
    if (slot != -1 && i != slot) {
      (*slotskeys)[i] = total;
      continue;
    }
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
      if (!iter->key().starts_with(prefix)) {
        break;
      }
      total++;
      if (slot != -1 && count > 0 && !end) {
        // Get user key
        if (cnt < count) {
          std::string ns, user_key;
          ExtractNamespaceKey(iter->key(), &ns, &user_key, true);
          keys->push_back(user_key);
          cnt++;
        }
      }
    }
    // Maybe cnt < count
    if (cnt > 0) end = true;
    (*slotskeys)[i] = total;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status SubKeyScanner::Scan(RedisType type, const Slice &user_key, const std::string &cursor, uint64_t limit,
                                    const std::string &subkey_prefix, std::vector<std::string> *keys,
                                    std::vector<std::string> *values) {
  uint64_t cnt = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  Metadata metadata(type, false);
  rocksdb::Status s = GetMetadata(type, ns_key, &metadata);
  if (!s.ok()) return s;

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  storage_->SetReadOptions(read_options);
  auto iter = DBUtil::UniqueIterator(storage_, read_options);
  std::string match_prefix_key;
  if (!subkey_prefix.empty()) {
    InternalKey(ns_key, subkey_prefix, metadata.version, storage_->IsSlotIdEncoded()).Encode(&match_prefix_key);
  } else {
    InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode(&match_prefix_key);
  }

  std::string start_key;
  if (!cursor.empty()) {
    InternalKey(ns_key, cursor, metadata.version, storage_->IsSlotIdEncoded()).Encode(&start_key);
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
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    keys->emplace_back(ikey.GetSubKey().ToString());
    if (values != nullptr) {
      values->emplace_back(iter->value().ToString());
    }
    cnt++;
    if (limit > 0 && cnt >= limit) {
      break;
    }
  }
  return rocksdb::Status::OK();
}

RedisType WriteBatchLogData::GetRedisType() { return type_; }

std::vector<std::string> *WriteBatchLogData::GetArguments() { return &args_; }

std::string WriteBatchLogData::Encode() {
  std::string ret = std::to_string(type_);
  for (const auto &arg : args_) {
    ret += " " + arg;
  }
  return ret;
}

Status WriteBatchLogData::Decode(const rocksdb::Slice &blob) {
  const std::string &log_data = blob.ToString();
  std::vector<std::string> args = Util::Split(log_data, " ");
  auto parse_result = ParseInt<int>(args[0], 10);
  if (!parse_result) {
    return parse_result.ToStatus();
  }
  type_ = static_cast<RedisType>(*parse_result);
  args_ = std::vector<std::string>(args.begin() + 1, args.end());

  return Status::OK();
}
}  // namespace Redis
