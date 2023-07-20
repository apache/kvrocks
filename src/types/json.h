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
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons_ext/jsonpath/jsonpath_error.hpp>
#include <string>

#include "encoding.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

using JsonType = jsoncons::basic_json<char, jsoncons::sorted_policy>;
using JsonPathExpression = jsoncons::jsonpath::jsonpath_expression<JsonType>;
using JsonQueryCallback = std::function<void(const std::string_view&, const JsonType&)>;
using JsonReplaceCallback = std::function<void(const std::string_view&, JsonType&)>;

class JsonPath {
 public:
  static constexpr std::string_view ROOT_PATH = "$";

  static StatusOr<JsonPath> BuildJsonPath(std::string path);
  static JsonPath BuildJsonRootPath() { return BuildJsonPath(std::string(ROOT_PATH)).GetValue(); }

  bool IsLegacy() const noexcept { return !fixed_path_.empty(); }

  std::string_view Path() const {
    if (IsLegacy()) {
      return fixed_path_;
    }
    return origin_;
  }

  bool IsRootPath() const { return Path() == ROOT_PATH; }

  void EvalQueryExpression(const JsonType& json_value, const JsonQueryCallback& cb) const {
    expression_.evaluate(json_value, cb);
  }

  void EvalReplaceExpression(const JsonType& json_value, const JsonReplaceCallback& cb) const {
    auto wrapped_cb = [&cb](const std::string_view& path, const JsonType& json) {
      // NOTE: This ugly code is just for POC.
      // Though JsonPath supports mutable reference, `jsoncons::make_expression`
      // only supports const reference, so const_cast is used as a workaround.
      cb(path, const_cast<JsonType&>(json));
    };
    expression_.evaluate(json_value, wrapped_cb);
  }

 private:
  static std::optional<std::string> tryConvertLegacyToJsonPath(Slice path);

  JsonPath(std::string path, std::string fixed_path, JsonPathExpression path_expression)
      : origin_(std::move(path)), fixed_path_(std::move(fixed_path)), expression_(std::move(path_expression)) {}

  std::string origin_;
  std::string fixed_path_;
  JsonPathExpression expression_;
};

StatusOr<JsonType> ParseJson(std::string_view data);
std::string ToString(const JsonType& json_value);

enum class JsonSetFlags { kNone, kJsonSetNX, kJsonSetXX };

}  // namespace redis
