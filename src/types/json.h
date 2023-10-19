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

#include <jsoncons/json.hpp>
#include <jsoncons/json_error.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>

#include "jsoncons_ext/jsonpath/jsonpath_error.hpp"
#include "status.h"

struct JsonValue {
  JsonValue() = default;
  explicit JsonValue(jsoncons::basic_json<char> value) : value(std::move(value)) {}

  static StatusOr<JsonValue> FromString(std::string_view str) {
    jsoncons::json val;
    try {
      val = jsoncons::json::parse(str);
    } catch (const jsoncons::ser_error &e) {
      return {Status::NotOK, e.what()};
    }

    return JsonValue(std::move(val));
  }

  std::string Dump() const {
    std::string res;
    Dump(&res);
    return res;
  }

  void Dump(std::string *buffer) const {
    jsoncons::compact_json_string_encoder encoder{*buffer};
    value.dump(encoder);
  }

  Status Set(std::string_view path, JsonValue &&new_value) {
    try {
      jsoncons::jsonpath::json_replace(value, path, [&new_value](const std::string & /*path*/, jsoncons::json &origin) {
        origin = new_value.value;
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return Status::OK();
  }

  StatusOr<JsonValue> Get(std::string_view path) const {
    try {
      return jsoncons::jsonpath::json_query(value, path);
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
  }

  Status ArrAppend(std::string_view path, const std::vector<jsoncons::json> &append_values,
                   std::vector<uint64_t> *result_count) {
    try {
      jsoncons::jsonpath::json_replace(
          value, path, [&append_values, result_count](const std::string &path, jsoncons::json &val) {
            if (val.is_array()) {
              val.insert(val.array_range().end(), append_values.begin(), append_values.end());
              result_count->emplace_back(val.size());
            } else {
              result_count->emplace_back(0);
            }
          });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return Status::OK();
  }

  JsonValue(const JsonValue &) = default;
  JsonValue(JsonValue &&) = default;

  JsonValue &operator=(const JsonValue &) = default;
  JsonValue &operator=(JsonValue &&) = default;

  ~JsonValue() = default;

  jsoncons::json value;
};
