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
#include "common/scope_exit.h"
#include "db_util.h"
#include "parse_util.h"
#include "rocksdb/iterator.h"
#include "rocksdb/status.h"
#include "server/server.h"
#include "storage/iterator.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "time_util.h"

namespace redis {

Database::Database(engine::Storage *storage, std::string ns) : storage_(storage), namespace_(std::move(ns)) {
  metadata_cf_handle_ = storage->GetCFHandle(engine::kMetadataColumnFamilyName);
}

// Some data types may support reading multiple types of metadata.
// For example, bitmap supports reading string metadata and bitmap metadata.
rocksdb::Status Database::ParseMetadata(RedisTypes types, Slice *bytes, Metadata *metadata) {
  std::string old_metadata;
  metadata->Encode(&old_metadata);

  bool is_keyspace_hit = false;
  ScopeExit se([this, &is_keyspace_hit] {
    if (is_keyspace_hit) {
      storage_->RecordStat(engine::StatType::KeyspaceHits, 1);
    } else {
      storage_->RecordStat(engine::StatType::KeyspaceMisses, 1);
    }
  });

  auto s = metadata->Decode(bytes);
  // delay InvalidArgument error check after type match check
  if (!s.ok() && !s.IsInvalidArgument()) return s;

  if (metadata->Expired()) {
    // error discarded here since it already failed
    auto _ [[maybe_unused]] = metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound(kErrMsgKeyExpired);
  }

  // if type is not matched, we still need to check if the metadata is valid.
  if (!types.Contains(metadata->Type()) && (metadata->size > 0 || metadata->IsEmptyableType())) {
    // error discarded here since it already failed
    auto _ [[maybe_unused]] = metadata->Decode(old_metadata);
    return rocksdb::Status::InvalidArgument(kErrMsgWrongType);
  }
  if (s.IsInvalidArgument()) return s;

  if (metadata->size == 0 && !metadata->IsEmptyableType()) {
    // error discarded here since it already failed
    auto _ [[maybe_unused]] = metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound("no element found");
  }
  is_keyspace_hit = true;
  return s;
}

rocksdb::Status Database::GetMetadata(GetOptions options, RedisTypes types, const Slice &ns_key, Metadata *metadata) {
  std::string raw_value;
  Slice rest;
  return GetMetadata(options, types, ns_key, &raw_value, metadata, &rest);
}

rocksdb::Status Database::GetMetadata(GetOptions options, RedisTypes types, const Slice &ns_key, std::string *raw_value,
                                      Metadata *metadata, Slice *rest) {
  auto s = GetRawMetadata(options, ns_key, raw_value);
  *rest = *raw_value;
  if (!s.ok()) return s;
  return ParseMetadata(types, rest, metadata);
}

rocksdb::Status Database::GetRawMetadata(GetOptions options, const Slice &ns_key, std::string *bytes) {
  rocksdb::ReadOptions opts;
  // If options.snapshot == nullptr, we can avoid allocating a snapshot here.
  opts.snapshot = options.snapshot;
  return storage_->Get(opts, metadata_cf_handle_, ns_key, bytes);
}

rocksdb::Status Database::Expire(const Slice &user_key, uint64_t timestamp) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  std::string value;
  Metadata metadata(kRedisNone, false);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s;

  s = metadata.Decode(value);
  if (!s.ok()) return s;
  if (metadata.Expired()) {
    return rocksdb::Status::NotFound(kErrMsgKeyExpired);
  }
  if (!metadata.IsEmptyableType() && metadata.size == 0) {
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
  std::string ns_key = AppendNamespacePrefix(user_key);

  std::string value;
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = storage_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s;
  Metadata metadata(kRedisNone, false);
  s = metadata.Decode(value);
  if (!s.ok()) return s;
  if (metadata.Expired()) {
    return rocksdb::Status::NotFound(kErrMsgKeyExpired);
  }
  return storage_->Delete(storage_->DefaultWriteOptions(), metadata_cf_handle_, ns_key);
}

rocksdb::Status Database::MDel(const std::vector<Slice> &keys, uint64_t *deleted_cnt) {
  *deleted_cnt = 0;

  std::vector<std::string> lock_keys;
  lock_keys.reserve(keys.size());
  for (const auto &key : keys) {
    std::string ns_key = AppendNamespacePrefix(key);
    lock_keys.emplace_back(std::move(ns_key));
  }
  MultiLockGuard guard(storage_->GetLockManager(), lock_keys);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisNone);
  batch->PutLogData(log_data.Encode());

  std::vector<Slice> slice_keys;
  slice_keys.reserve(lock_keys.size());
  for (const auto &ns_key : lock_keys) {
    slice_keys.emplace_back(ns_key);
  }

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options = storage_->DefaultMultiGetOptions();
  read_options.snapshot = ss.GetSnapShot();
  std::vector<rocksdb::Status> statuses(slice_keys.size());
  std::vector<rocksdb::PinnableSlice> pin_values(slice_keys.size());
  storage_->MultiGet(read_options, metadata_cf_handle_, slice_keys.size(), slice_keys.data(), pin_values.data(),
                     statuses.data());

  for (size_t i = 0; i < slice_keys.size(); i++) {
    if (!statuses[i].ok() && !statuses[i].IsNotFound()) return statuses[i];
    if (statuses[i].IsNotFound()) continue;

    Metadata metadata(kRedisNone, false);
    // Explicit construct a rocksdb::Slice to avoid the implicit conversion from
    // PinnableSlice to Slice.
    auto s = metadata.Decode(rocksdb::Slice(pin_values[i].data(), pin_values[i].size()));
    if (!s.ok()) continue;
    if (metadata.Expired()) continue;

    batch->Delete(metadata_cf_handle_, lock_keys[i]);
    *deleted_cnt += 1;
  }

  if (*deleted_cnt == 0) return rocksdb::Status::OK();

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Database::Exists(const std::vector<Slice> &keys, int *ret) {
  std::vector<std::string> ns_keys;
  ns_keys.reserve(keys.size());
  for (const auto &key : keys) {
    ns_keys.emplace_back(AppendNamespacePrefix(key));
  }
  return existsInternal(ns_keys, ret);
}

rocksdb::Status Database::TTL(const Slice &user_key, int64_t *ttl) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  *ttl = -2;  // ttl is -2 when the key does not exist or expired
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = storage_->Get(read_options, metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  Metadata metadata(kRedisNone, false);
  s = metadata.Decode(value);
  if (!s.ok()) return s;
  *ttl = metadata.TTL();

  return rocksdb::Status::OK();
}

rocksdb::Status Database::GetExpireTime(const Slice &user_key, uint64_t *timestamp) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  Metadata metadata(kRedisNone, false);
  auto s = GetMetadata(GetOptions{}, RedisTypes::All(), ns_key, &metadata);
  if (!s.ok()) return s;
  *timestamp = metadata.expire;

  return rocksdb::Status::OK();
}

rocksdb::Status Database::GetKeyNumStats(const std::string &prefix, KeyNumStats *stats) {
  return Keys(prefix, nullptr, stats);
}

rocksdb::Status Database::Keys(const std::string &prefix, std::vector<std::string> *keys, KeyNumStats *stats) {
  uint16_t slot_id = 0;
  std::string ns_prefix;
  if (namespace_ != kDefaultNamespace || keys != nullptr) {
    if (storage_->IsSlotIdEncoded()) {
      ns_prefix = ComposeNamespaceKey(namespace_, "", false);
      if (!prefix.empty()) {
        PutFixed16(&ns_prefix, slot_id);
        ns_prefix.append(prefix);
      }
    } else {
      ns_prefix = AppendNamespacePrefix(prefix);
    }
  }

  uint64_t ttl_sum = 0;
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = ss.GetSnapShot();
  auto iter = util::UniqueIterator(storage_, read_options, metadata_cf_handle_);

  while (true) {
    ns_prefix.empty() ? iter->SeekToFirst() : iter->Seek(ns_prefix);
    for (; iter->Valid(); iter->Next()) {
      if (!ns_prefix.empty() && !iter->key().starts_with(ns_prefix)) {
        break;
      }
      Metadata metadata(kRedisNone, false);
      auto s = metadata.Decode(iter->value());
      if (!s.ok()) continue;
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
        auto [_, user_key] = ExtractNamespaceKey(iter->key(), storage_->IsSlotIdEncoded());
        keys->emplace_back(user_key.ToString());
      }
    }

    if (!storage_->IsSlotIdEncoded()) break;
    if (prefix.empty()) break;
    if (++slot_id >= HASH_SLOTS_SIZE) break;

    ns_prefix = ComposeNamespaceKey(namespace_, "", false);
    PutFixed16(&ns_prefix, slot_id);
    ns_prefix.append(prefix);
  }

  if (stats && stats->n_expires > 0) {
    stats->avg_ttl = ttl_sum / stats->n_expires / 1000;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Database::Scan(const std::string &cursor, uint64_t limit, const std::string &prefix,
                               std::vector<std::string> *keys, std::string *end_cursor) {
  end_cursor->clear();
  uint64_t cnt = 0;
  uint16_t slot_start = 0;
  std::string ns_prefix;
  std::string user_key;

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = ss.GetSnapShot();
  auto iter = util::UniqueIterator(storage_, read_options, metadata_cf_handle_);

  std::string ns_cursor = AppendNamespacePrefix(cursor);
  if (storage_->IsSlotIdEncoded()) {
    slot_start = cursor.empty() ? 0 : GetSlotIdFromKey(cursor);
    ns_prefix = ComposeNamespaceKey(namespace_, "", false);
    if (!prefix.empty()) {
      PutFixed16(&ns_prefix, slot_start);
      ns_prefix.append(prefix);
    }
  } else {
    ns_prefix = AppendNamespacePrefix(prefix);
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
      auto s = metadata.Decode(iter->value());
      if (!s.ok()) continue;

      if (metadata.Expired()) continue;
      std::tie(std::ignore, user_key) = ExtractNamespaceKey<std::string>(iter->key(), storage_->IsSlotIdEncoded());
      keys->emplace_back(user_key);
      cnt++;
    }
    if (!storage_->IsSlotIdEncoded() || prefix.empty()) {
      if (!keys->empty() && cnt >= limit) {
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
          std::tie(std::ignore, user_key) = ExtractNamespaceKey<std::string>(iter->key(), storage_->IsSlotIdEncoded());
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

    ns_prefix = ComposeNamespaceKey(namespace_, "", false);
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
    unsigned int seed = util::GetTimeStamp();
    *key = keys.at(rand_r(&seed) % keys.size());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::FlushDB() {
  std::string begin_key, end_key;
  std::string prefix = ComposeNamespaceKey(namespace_, "", false);
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
  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = ss.GetSnapShot();
  auto iter = util::UniqueIterator(storage_, read_options, metadata_cf_handle_);
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

  std::string ns_key = AppendNamespacePrefix(user_key);

  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = storage_->Get(read_options, metadata_cf_handle_, ns_key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  Metadata metadata(kRedisNone, false);
  s = metadata.Decode(value);
  if (!s.ok()) return s;

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
  timeval created_at = metadata.Time();
  std::time_t tm = created_at.tv_sec;
  char time_str[25];
  if (!std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", std::localtime(&tm))) {
    return rocksdb::Status::TryAgain("Fail to format local time_str");
  }
  std::string created_at_str(time_str);
  infos->emplace_back(created_at_str + "." + std::to_string(created_at.tv_usec));

  if (metadata.Type() == kRedisList) {
    ListMetadata list_metadata(false);
    s = GetMetadata(GetOptions{}, {kRedisList}, ns_key, &list_metadata);
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
    infos->emplace_back("head");
    infos->emplace_back(std::to_string(list_metadata.head));
    infos->emplace_back("tail");
    infos->emplace_back(std::to_string(list_metadata.tail));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Database::Type(const Slice &key, RedisType *type) {
  std::string ns_key = AppendNamespacePrefix(key);
  return typeInternal(ns_key, type);
}

std::string Database::AppendNamespacePrefix(const Slice &user_key) {
  return ComposeNamespaceKey(namespace_, user_key, storage_->IsSlotIdEncoded());
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
  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  read_options.snapshot = ss.GetSnapShot();
  auto iter = util::UniqueIterator(storage_, read_options, cf_handle);
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

  std::string prefix = ComposeSlotKeyPrefix(ns, slot);
  std::string prefix_end = ComposeSlotKeyPrefix(ns, slot + 1);
  auto s = storage_->DeleteRange(prefix, prefix_end);
  if (!s.ok()) {
    return s;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::KeyExist(const std::string &key) {
  int cnt = 0;
  std::vector<rocksdb::Slice> keys{key};
  auto s = Exists(keys, &cnt);
  if (!s.ok()) {
    return s;
  }
  if (cnt == 0) {
    return rocksdb::Status::NotFound();
  }
  return rocksdb::Status::OK();
}

rocksdb::Status SubKeyScanner::Scan(RedisType type, const Slice &user_key, const std::string &cursor, uint64_t limit,
                                    const std::string &subkey_prefix, std::vector<std::string> *keys,
                                    std::vector<std::string> *values) {
  uint64_t cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);
  Metadata metadata(type, false);
  LatestSnapShot ss(storage_);
  rocksdb::Status s = GetMetadata(GetOptions{ss.GetSnapShot()}, {type}, ns_key, &metadata);
  if (!s.ok()) return s;

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  auto iter = util::UniqueIterator(storage_, read_options);
  std::string match_prefix_key =
      InternalKey(ns_key, subkey_prefix, metadata.version, storage_->IsSlotIdEncoded()).Encode();

  std::string start_key;
  if (!cursor.empty()) {
    start_key = InternalKey(ns_key, cursor, metadata.version, storage_->IsSlotIdEncoded()).Encode();
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

RedisType WriteBatchLogData::GetRedisType() const { return type_; }

std::vector<std::string> *WriteBatchLogData::GetArguments() { return &args_; }

std::string WriteBatchLogData::Encode() const {
  std::string ret = std::to_string(type_);
  for (const auto &arg : args_) {
    ret += " " + arg;
  }
  return ret;
}

Status WriteBatchLogData::Decode(const rocksdb::Slice &blob) {
  const std::string &log_data = blob.ToString();
  std::vector<std::string> args = util::Split(log_data, " ");
  auto parse_result = ParseInt<int>(args[0], 10);
  if (!parse_result) {
    return parse_result.ToStatus();
  }
  type_ = static_cast<RedisType>(*parse_result);
  args_ = std::vector<std::string>(args.begin() + 1, args.end());

  return Status::OK();
}

rocksdb::Status Database::existsInternal(const std::vector<std::string> &keys, int *ret) {
  *ret = 0;
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();

  rocksdb::Status s;
  std::string value;
  for (const auto &key : keys) {
    s = storage_->Get(read_options, metadata_cf_handle_, key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      Metadata metadata(kRedisNone, false);
      s = metadata.Decode(value);
      if (!s.ok()) return s;
      if (!metadata.Expired()) *ret += 1;
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::typeInternal(const Slice &key, RedisType *type) {
  *type = kRedisNone;
  LatestSnapShot ss(storage_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string value;
  rocksdb::Status s = storage_->Get(read_options, metadata_cf_handle_, key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  Metadata metadata(kRedisNone, false);
  s = metadata.Decode(value);
  if (!s.ok()) return s;
  if (metadata.Expired()) {
    *type = kRedisNone;
  } else {
    *type = metadata.Type();
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Database::Copy(const std::string &key, const std::string &new_key, bool nx, bool delete_old,
                               CopyResult *res) {
  std::vector<std::string> lock_keys = {key, new_key};
  MultiLockGuard guard(storage_->GetLockManager(), lock_keys);

  RedisType type = kRedisNone;
  auto s = typeInternal(key, &type);
  if (!s.ok()) return s;
  if (type == kRedisNone) {
    *res = CopyResult::KEY_NOT_EXIST;
    return rocksdb::Status::OK();
  }

  if (nx) {
    int exist = 0;
    if (s = existsInternal({new_key}, &exist), !s.ok()) return s;
    if (exist > 0) {
      *res = CopyResult::KEY_ALREADY_EXIST;
      return rocksdb::Status::OK();
    }
  }

  *res = CopyResult::DONE;

  if (key == new_key) return rocksdb::Status::OK();

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(type);
  batch->PutLogData(log_data.Encode());

  engine::DBIterator iter(storage_, rocksdb::ReadOptions());
  iter.Seek(key);

  if (delete_old) {
    batch->Delete(metadata_cf_handle_, key);
  }
  // copy metadata
  batch->Put(metadata_cf_handle_, new_key, iter.Value());

  auto subkey_iter = iter.GetSubKeyIterator();

  if (subkey_iter != nullptr) {
    auto zset_score_cf = type == kRedisZSet ? storage_->GetCFHandle(engine::kZSetScoreColumnFamilyName) : nullptr;

    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      InternalKey from_ikey(subkey_iter->Key(), storage_->IsSlotIdEncoded());
      std::string to_ikey =
          InternalKey(new_key, from_ikey.GetSubKey(), from_ikey.GetVersion(), storage_->IsSlotIdEncoded()).Encode();
      // copy sub key
      batch->Put(subkey_iter->ColumnFamilyHandle(), to_ikey, subkey_iter->Value());

      // The ZSET type stores an extra score and member field inside `zset_score` column family
      // while compared to other composed data structures. The purpose is to allow to seek by score.
      if (type == kRedisZSet) {
        std::string score_bytes = subkey_iter->Value().ToString();
        score_bytes.append(from_ikey.GetSubKey().ToString());
        // copy score key
        std::string score_key =
            InternalKey(new_key, score_bytes, from_ikey.GetVersion(), storage_->IsSlotIdEncoded()).Encode();
        batch->Put(zset_score_cf, score_key, Slice());
      }
    }
  }

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

}  // namespace redis
