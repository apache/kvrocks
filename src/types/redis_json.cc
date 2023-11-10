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

#include "redis_json.h"

#include "json.h"
#include "lock_manager.h"
#include "storage/redis_metadata.h"

namespace redis {

rocksdb::Status Json::write(Slice ns_key, JsonMetadata *metadata, const JsonValue &json_val) {
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisJson);
  batch->PutLogData(log_data.Encode());

  auto format = storage_->GetConfig()->json_storage_format;
  metadata->format = format;

  std::string val;
  metadata->Encode(&val);

  Status s;
  if (format == JsonStorageFormat::JSON) {
    s = json_val.Dump(&val, storage_->GetConfig()->json_max_nesting_depth);
  } else if (format == JsonStorageFormat::CBOR) {
    s = json_val.DumpCBOR(&val, storage_->GetConfig()->json_max_nesting_depth);
  } else {
    return rocksdb::Status::InvalidArgument("JSON storage format not supported");
  }
  if (!s) {
    return rocksdb::Status::InvalidArgument("Failed to encode JSON into storage: " + s.Msg());
  }

  batch->Put(metadata_cf_handle_, ns_key, val);

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Json::read(const Slice &ns_key, JsonMetadata *metadata, JsonValue *value) {
  std::string bytes;
  Slice rest;

  auto s = GetMetadata(kRedisJson, ns_key, &bytes, metadata, &rest);
  if (!s.ok()) return s;

  if (metadata->format == JsonStorageFormat::JSON) {
    auto origin_res = JsonValue::FromString(rest.ToStringView());
    if (!origin_res) return rocksdb::Status::Corruption(origin_res.Msg());
    *value = *std::move(origin_res);
  } else if (metadata->format == JsonStorageFormat::CBOR) {
    auto origin_res = JsonValue::FromCBOR(rest.ToStringView());
    if (!origin_res) return rocksdb::Status::Corruption(origin_res.Msg());
    *value = *std::move(origin_res);
  } else {
    return rocksdb::Status::NotSupported("JSON storage format not supported");
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Json::create(const std::string &ns_key, JsonMetadata &metadata, const std::string &value) {
  auto json_res = JsonValue::FromString(value, storage_->GetConfig()->json_max_nesting_depth);
  if (!json_res) return rocksdb::Status::InvalidArgument(json_res.Msg());
  auto json_val = *std::move(json_res);

  return write(ns_key, &metadata, json_val);
}

rocksdb::Status Json::Info(const std::string &user_key, JsonStorageFormat *storage_format) {
  auto ns_key = AppendNamespacePrefix(user_key);

  std::string bytes;
  Slice rest;
  JsonMetadata metadata;

  auto s = GetMetadata(kRedisJson, ns_key, &bytes, &metadata, &rest);
  if (!s.ok()) return s;

  *storage_format = metadata.format;

  return rocksdb::Status::OK();
}

rocksdb::Status Json::Set(const std::string &user_key, const std::string &path, const std::string &value) {
  auto ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);

  JsonMetadata metadata;
  JsonValue origin;
  auto s = read(ns_key, &metadata, &origin);

  if (s.IsNotFound()) {
    if (path != "$") return rocksdb::Status::InvalidArgument("new objects must be created at the root");

    return create(ns_key, metadata, value);
  }

  if (!s.ok()) return s;

  auto new_res = JsonValue::FromString(value, storage_->GetConfig()->json_max_nesting_depth);
  if (!new_res) return rocksdb::Status::InvalidArgument(new_res.Msg());
  auto new_val = *std::move(new_res);

  auto set_res = origin.Set(path, std::move(new_val));
  if (!set_res) return rocksdb::Status::InvalidArgument(set_res.Msg());

  return write(ns_key, &metadata, origin);
}

rocksdb::Status Json::Get(const std::string &user_key, const std::vector<std::string> &paths, JsonValue *result) {
  auto ns_key = AppendNamespacePrefix(user_key);

  JsonMetadata metadata;
  JsonValue json_val;
  auto s = read(ns_key, &metadata, &json_val);
  if (!s.ok()) return s;

  JsonValue res;

  if (paths.empty()) {
    res = std::move(json_val);
  } else if (paths.size() == 1) {
    auto get_res = json_val.Get(paths[0]);
    if (!get_res) return rocksdb::Status::InvalidArgument(get_res.Msg());
    res = *std::move(get_res);
  } else {
    for (const auto &path : paths) {
      auto get_res = json_val.Get(path);
      if (!get_res) return rocksdb::Status::InvalidArgument(get_res.Msg());
      res.value.insert_or_assign(path, std::move(get_res->value));
    }
  }

  *result = std::move(res);
  return rocksdb::Status::OK();
}

rocksdb::Status Json::ArrAppend(const std::string &user_key, const std::string &path,
                                const std::vector<std::string> &values, std::vector<size_t> *result_count) {
  auto ns_key = AppendNamespacePrefix(user_key);

  std::vector<jsoncons::json> append_values;
  append_values.reserve(values.size());
  for (auto &v : values) {
    auto value_res = JsonValue::FromString(v, storage_->GetConfig()->json_max_nesting_depth);
    if (!value_res) return rocksdb::Status::InvalidArgument(value_res.Msg());
    auto value = *std::move(value_res);
    append_values.emplace_back(std::move(value.value));
  }

  LockGuard guard(storage_->GetLockManager(), ns_key);

  JsonMetadata metadata;
  JsonValue value;
  auto s = read(ns_key, &metadata, &value);
  if (!s.ok()) return s;

  auto append_res = value.ArrAppend(path, append_values);
  if (!append_res) return rocksdb::Status::InvalidArgument(append_res.Msg());
  *result_count = *append_res;

  bool is_write = std::any_of(result_count->begin(), result_count->end(), [](uint64_t c) { return c > 0; });
  if (!is_write) return rocksdb::Status::OK();

  return write(ns_key, &metadata, value);
}

rocksdb::Status Json::ArrIndex(const std::string &user_key, const std::string &path, const std::string &needle,
                               ssize_t start, ssize_t end, std::vector<ssize_t> *result) {
  auto ns_key = AppendNamespacePrefix(user_key);

  auto needle_res = JsonValue::FromString(needle, storage_->GetConfig()->json_max_nesting_depth);
  if (!needle_res) return rocksdb::Status::InvalidArgument(needle_res.Msg());
  auto needle_value = *std::move(needle_res);

  JsonMetadata metadata;
  JsonValue value;
  auto s = read(ns_key, &metadata, &value);
  if (!s.ok()) return s;

  auto index_res = value.ArrIndex(path, needle_value.value, start, end);
  if (!index_res) return rocksdb::Status::InvalidArgument(index_res.Msg());
  *result = *index_res;

  return rocksdb::Status::OK();
}

rocksdb::Status Json::Type(const std::string &user_key, const std::string &path, std::vector<std::string> *results) {
  auto ns_key = AppendNamespacePrefix(user_key);

  JsonMetadata metadata;
  JsonValue json_val;
  auto s = read(ns_key, &metadata, &json_val);
  if (!s.ok()) return s;

  auto res = json_val.Type(path);
  if (!res) return rocksdb::Status::InvalidArgument(res.Msg());

  *results = *res;
  return rocksdb::Status::OK();
}

rocksdb::Status Json::Merge(const std::string &user_key, const std::string &path, const std::string &merge_value,
                            bool &result) {
  auto ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);

  JsonMetadata metadata;
  JsonValue json_val;

  auto s = read(ns_key, &metadata, &json_val);

  if (s.IsNotFound()) {
    if (path != "$") return rocksdb::Status::InvalidArgument("new objects must be created at the root");
    result = true;
    return create(ns_key, metadata, merge_value);
  }

  if (!s.ok()) return s;

  auto res = json_val.Merge(path, merge_value);

  if (!res.IsOK()) return s;

  result = static_cast<bool>(res.GetValue());
  if (!res) {
    return rocksdb::Status::OK();
  }

  return write(ns_key, &metadata, json_val);
}

rocksdb::Status Json::Clear(const std::string &user_key, const std::string &path, size_t *result) {
  auto ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);

  JsonValue json_val;
  JsonMetadata metadata;
  auto s = read(ns_key, &metadata, &json_val);

  if (!s.ok()) return s;

  auto res = json_val.Clear(path);
  if (!res) return rocksdb::Status::InvalidArgument(res.Msg());

  *result = *res;
  if (*result == 0) {
    return rocksdb::Status::OK();
  }

  return write(ns_key, &metadata, json_val);
}

rocksdb::Status Json::ArrLen(const std::string &user_key, const std::string &path,
                             std::vector<std::optional<uint64_t>> &arr_lens) {
  auto ns_key = AppendNamespacePrefix(user_key);
  JsonMetadata metadata;
  JsonValue json_val;
  auto s = read(ns_key, &metadata, &json_val);
  if (!s.ok()) return s;

  auto len_res = json_val.ArrLen(path, arr_lens);
  if (!len_res) return rocksdb::Status::InvalidArgument(len_res.Msg());

  return rocksdb::Status::OK();
}

rocksdb::Status Json::Toggle(const std::string &user_key, const std::string &path,
                             std::vector<std::optional<bool>> &result) {
  auto ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);

  JsonMetadata metadata;
  JsonValue origin;
  auto s = read(ns_key, &metadata, &origin);
  if (!s.ok()) return s;

  auto toggle_res = origin.Toggle(path);
  if (!toggle_res) return rocksdb::Status::InvalidArgument(toggle_res.Msg());
  result = *toggle_res;

  return write(ns_key, &metadata, origin);
}

rocksdb::Status Json::ArrPop(const std::string &user_key, const std::string &path, int64_t index,
                             std::vector<std::optional<JsonValue>> *results) {
  auto ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);

  JsonMetadata metadata;
  JsonValue json_val;
  auto s = read(ns_key, &metadata, &json_val);
  if (!s.ok()) return s;

  auto pop_res = json_val.ArrPop(path, index);
  if (!pop_res) return rocksdb::Status::InvalidArgument(pop_res.Msg());
  *results = *pop_res;

  bool is_write = std::any_of(pop_res->begin(), pop_res->end(),
                              [](const std::optional<JsonValue> &val) { return val.has_value(); });
  if (!is_write) return rocksdb::Status::OK();

  return write(ns_key, &metadata, json_val);
}

rocksdb::Status Json::ObjKeys(const std::string &user_key, const std::string &path,
                              std::vector<std::optional<std::vector<std::string>>> &keys) {
  auto ns_key = AppendNamespacePrefix(user_key);
  JsonMetadata metadata;
  JsonValue json_val;
  auto s = read(ns_key, &metadata, &json_val);
  if (!s.ok()) return s;
  auto keys_res = json_val.ObjKeys(path, keys);
  if (!keys_res) return rocksdb::Status::InvalidArgument(keys_res.Msg());

  return rocksdb::Status::OK();
}

}  // namespace redis
