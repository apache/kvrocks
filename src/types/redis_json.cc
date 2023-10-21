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

  metadata->format = JsonStorageFormat::JSON;

  std::string val;
  metadata->Encode(&val);
  auto s = json_val.Dump(&val, storage_->GetConfig()->json_max_nesting_depth);
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

  if (metadata->format != JsonStorageFormat::JSON)
    return rocksdb::Status::NotSupported("JSON storage format not supported");

  auto origin_res = JsonValue::FromString(rest.ToStringView());
  if (!origin_res) return rocksdb::Status::Corruption(origin_res.Msg());
  *value = *std::move(origin_res);

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

    auto json_res = JsonValue::FromString(value, storage_->GetConfig()->json_max_nesting_depth);
    if (!json_res) return rocksdb::Status::InvalidArgument(json_res.Msg());
    auto json_val = *std::move(json_res);

    return write(ns_key, &metadata, json_val);
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

rocksdb::Status Json::Type(const std::string &user_key, const std::string &path, std::vector<std::string> *results) {
  auto ns_key = AppendNamespacePrefix(user_key);

  JsonMetadata metadata;
  JsonValue json_val;
  auto s = read(ns_key, &metadata, &json_val);
  if (!s.ok()) return s;

  auto res = json_val.Type(path, results);
  return rocksdb::Status::OK();
}

}  // namespace redis
