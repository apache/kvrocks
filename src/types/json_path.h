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

#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons_ext/jsonpath/jsonpath_error.hpp>

#include "jsoncons_ext/jsonpath/jsonpath_error.hpp"
#include "status.h"

class JsonPath {
 public:
  using JsonType = jsoncons::basic_json<char>;
  using JsonPathExpression = jsoncons::jsonpath::jsonpath_expression<JsonType, const JsonType&>;
  using JsonQueryCallback = std::function<void(const std::string_view&, const JsonType&)>;
  using JsonReplaceCallback = std::function<void(const std::string_view&, JsonType&)>;

  static constexpr std::string_view ROOT_PATH = "$";

  static StatusOr<JsonPath> BuildJsonPath(std::string_view path);
  static JsonPath BuildJsonRootPath() { return BuildJsonPath(std::string(ROOT_PATH)).GetValue(); }

  bool IsLegacy() const noexcept { return !fixed_path_.empty(); }

  std::string_view Path() const {
    if (IsLegacy()) {
      return fixed_path_;
    }
    return origin_;
  }

  std::string_view OriginPath() const { return origin_; }

  bool IsRootPath() const { return Path() == ROOT_PATH; }

  JsonType EvalQueryExpression(const JsonType& json_value) const {
    return expression_.evaluate(const_cast<JsonType&>(json_value));
  }

  void EvalReplaceExpression(JsonType& json_value, const JsonReplaceCallback& callback) const {
    auto wrapped_cb = [&callback](const std::string_view& path, const JsonType& json) {
      // Though JsonPath supports mutable reference, `jsoncons::make_expression`
      // only supports const reference, so const_cast is used as a workaround.
      callback(path, const_cast<JsonType&>(json));
    };
    expression_.evaluate(json_value, wrapped_cb);
  }

 private:
  static std::optional<std::string> tryConvertLegacyToJsonPath(std::string_view path);

  JsonPath(std::string path, std::string fixed_path, JsonPathExpression path_expression)
      : origin_(std::move(path)), fixed_path_(std::move(fixed_path)), expression_(std::move(path_expression)) {}

  std::string origin_;
  std::string fixed_path_;
  // Pre-build the expression to avoid built it multiple times.
  JsonPathExpression expression_;
};
