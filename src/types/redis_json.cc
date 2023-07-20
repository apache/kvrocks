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

namespace redis {

rocksdb::Status RedisJson::JsonDel(const std::string &user_key, const JsonPath &path) { return rocksdb::Status::OK(); }
rocksdb::Status RedisJson::JsonGet(const std::string &user_key, const std::vector<JsonPath> &path,
                                   std::string *values) {
  return rocksdb::Status::OK();
}

rocksdb::Status RedisJson::JsonSet(const std::string &user_key, const JsonPath &path, const std::string &set_value,
                                   JsonSetFlags set_flags, bool *set_ok) {
  *set_ok = false;
  auto input_json = ParseJson(set_value);
  if (!input_json.IsOK()) {
    // Parse input json failed.
    return rocksdb::Status::IOError(input_json.ToStatus().Msg());
  }
  rocksdb::ReadOptions read_options;
  std::string exist_raw_value;
  auto s = this->Get(user_key, &exist_raw_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.IsNotFound()) {
    if (set_flags == JsonSetFlags::kJsonSetXX) {
      // Already exists
      *set_ok = false;
      return rocksdb::Status::OK();
    }
    if (!path.IsRootPath()) {
      return rocksdb::Status::IOError("ERR new objects must be created at the root");
    }
    s = this->Set(user_key, ToString(input_json.GetValue()));
    if (!s.ok()) {
      return s;
    }
    *set_ok = true;
    return rocksdb::Status::OK();
  }
  if (path.IsRootPath()) {
    if (set_flags == JsonSetFlags::kJsonSetNX) {
      // Already exists
      *set_ok = false;
      return rocksdb::Status::OK();
    }
    s = this->Set(user_key, ToString(input_json.GetValue()));
    if (!s.ok()) {
      return s;
    }
    *set_ok = true;
    return rocksdb::Status::OK();
  }
  auto origin_json_value = ParseJson(exist_raw_value);
  if (!origin_json_value.IsOK()) {
    // FIXME(mwish): add an extra ec for existing value?
    return rocksdb::Status::IOError(input_json.ToStatus().Msg());
  }
  if (set_flags == JsonSetFlags::kJsonSetXX) {
    path.EvalReplaceExpression(
        origin_json_value.GetValue(),
        [&input_json](const std::string_view &path, JsonType &value) { value = input_json.GetValue(); });
    s = this->Set(user_key, ToString(input_json.GetValue()));
    if (!s.ok()) {
      return s;
    }
  }
  // TODO(mwish): add non-exist path is not implemented now.
  return rocksdb::Status::IOError("ERR not implemented");
}

}  // namespace redis