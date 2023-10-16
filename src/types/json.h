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

#include "json_path.h"
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
    auto path_express_status = JsonPath::BuildJsonPath(path);
    if (!path_express_status) {
      return path_express_status.ToStatus();
    }
    return Set(path_express_status.GetValue(), std::move(new_value));
  }

  Status Set(const JsonPath &path, JsonValue &&new_value) {
    try {
      path.EvalReplaceExpression(
          value, [&new_value](const std::string_view & /*path*/, jsoncons::json &origin) { origin = new_value.value; });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return Status::OK();
  }

  StatusOr<JsonValue> Get(std::string_view path) const {
    auto path_express_status = JsonPath::BuildJsonPath(path);
    if (!path_express_status) {
      return path_express_status.ToStatus();
    }
    return Get(path_express_status.GetValue());
  }

  StatusOr<JsonValue> Get(const JsonPath &json_path) const {
    try {
      return json_path.EvalQueryExpression(value);
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
  }

  JsonValue(const JsonValue &) = default;
  JsonValue(JsonValue &&) = default;

  JsonValue &operator=(const JsonValue &) = default;
  JsonValue &operator=(JsonValue &&) = default;

  ~JsonValue() = default;

  jsoncons::json value;
};
