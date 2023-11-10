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

#pragma once

#include <storage/redis_db.h>

#include <string>

#include "json.h"
#include "storage/redis_metadata.h"

namespace redis {

class Json : public Database {
 public:
  Json(engine::Storage *storage, std::string ns) : Database(storage, std::move(ns)) {}

  rocksdb::Status Set(const std::string &user_key, const std::string &path, const std::string &value);
  rocksdb::Status Get(const std::string &user_key, const std::vector<std::string> &paths, JsonValue *result);
  rocksdb::Status Info(const std::string &user_key, JsonStorageFormat *storage_format);
  rocksdb::Status Type(const std::string &user_key, const std::string &path, std::vector<std::string> *results);
  rocksdb::Status ArrAppend(const std::string &user_key, const std::string &path,
                            const std::vector<std::string> &values, std::vector<size_t> *result_count);
  rocksdb::Status Clear(const std::string &user_key, const std::string &path, size_t *result);
  rocksdb::Status ArrLen(const std::string &user_key, const std::string &path,
                         std::vector<std::optional<uint64_t>> &arr_lens);
  rocksdb::Status Toggle(const std::string &user_key, const std::string &path,
                         std::vector<std::optional<bool>> &result);
  rocksdb::Status ObjKeys(const std::string &user_key, const std::string &path,
                          std::vector<std::optional<std::vector<std::string>>> &keys);
  rocksdb::Status ArrPop(const std::string &user_key, const std::string &path, int64_t index,
                         std::vector<std::optional<JsonValue>> *results);
  rocksdb::Status ArrIndex(const std::string &user_key, const std::string &path, const std::string &needle,
                           ssize_t start, ssize_t end, std::vector<ssize_t> *result);

  rocksdb::Status ArrTrim(const std::string &user_key, const std::string &path, int64_t start, int64_t stop,
                          std::vector<std::optional<uint64_t>> &results);

 private:
  rocksdb::Status write(Slice ns_key, JsonMetadata *metadata, const JsonValue &json_val);
  rocksdb::Status read(const Slice &ns_key, JsonMetadata *metadata, JsonValue *value);
};

}  // namespace redis
