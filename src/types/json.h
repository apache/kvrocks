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
#include <jsoncons_ext/jsonpath/jsonpath_error.hpp>

#include "status.h"

class JsonPath {
 public:
  using JsonType = jsoncons::json;
  using JsonPathExpression = jsoncons::jsonpath::jsonpath_expression<JsonType, const JsonType &>;

  static constexpr std::string_view ROOT_PATH = "$";

  static StatusOr<JsonPath> BuildJsonPath(std::string_view path);

  bool IsLegacy() const noexcept { return is_legacy_; }

  std::string_view Path() const { return path_; }

  bool IsRootPath() const { return Path() == ROOT_PATH; }

  JsonType EvalJsonQuery(const JsonType &json_value) const {
    return expression_.evaluate(const_cast<JsonType &>(json_value));
  }

  template <typename BinaryJsonFunction>
  void EvalJsonReplace(JsonType &json_value, const BinaryJsonFunction &callback) const {
    auto wrapped_cb = [&callback](const std::string_view &path, const JsonType &json) {
      // Though JsonPath supports mutable reference, `jsoncons::make_expression`
      // only supports const reference, so const_cast is used as a workaround.
      callback(path, const_cast<JsonType &>(json));
    };
    expression_.evaluate(json_value, wrapped_cb);
  }

 private:
  static std::optional<std::string> tryConvertLegacyToJsonPath(std::string_view path);

  JsonPath(std::string path, bool is_legacy, JsonPathExpression path_expression)
      : path_(std::move(path)), is_legacy_(is_legacy), expression_(std::move(path_expression)) {}

  std::string path_;
  bool is_legacy_;
  // Pre-build the expression to avoid built it multiple times.
  JsonPathExpression expression_;
};

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
    auto path_expression = GET_OR_RET(JsonPath::BuildJsonPath(path));
    return Set(path_expression, std::move(new_value));
  }

  Status Set(const JsonPath &path, JsonValue &&new_value) {
    try {
      path.EvalJsonReplace(
          value, [&new_value](const std::string_view & /*path*/, jsoncons::json &origin) { origin = new_value.value; });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return Status::OK();
  }

  StatusOr<JsonValue> Get(std::string_view path) const {
    auto path_expression = GET_OR_RET(JsonPath::BuildJsonPath(path));
    return Get(path_expression);
  }

  StatusOr<JsonValue> Get(const JsonPath &json_path) const {
    try {
      return json_path.EvalJsonQuery(value);
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
