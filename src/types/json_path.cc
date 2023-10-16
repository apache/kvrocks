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

#include "json_path.h"

StatusOr<JsonPath> JsonPath::BuildJsonPath(std::string_view path) {
  std::string fixed_path;
  std::string_view json_string;
  auto converted = tryConvertLegacyToJsonPath(path);
  if (converted.has_value()) {
    fixed_path = std::move(converted.value());
  }
  if (fixed_path.empty()) {
    json_string = path;
  } else {
    json_string = fixed_path;
  }

  std::error_code json_parse_error;
  auto path_expression = jsoncons::jsonpath::make_expression<JsonType>(json_string, json_parse_error);
  if (json_parse_error) {
    return {Status::NotOK, json_parse_error.message()};
  }
  return JsonPath(std::string(path), std::move(fixed_path), std::move(path_expression));
}

// https://redis.io/docs/data-types/json/path/#legacy-path-syntax
// The logic here is port from RedisJson `Path::new`.
std::optional<std::string> JsonPath::tryConvertLegacyToJsonPath(std::string_view path) {
  if (path.empty()) {
    return std::nullopt;
  }
  if (path[0] == '$') {
    if (path.size() == 1) {
      return std::nullopt;
    }
    if (path[1] == '.' || path[1] == '[') {
      return std::nullopt;
    }
  }
  if (path[0] == '.') {
    if (path.size() == 1) {
      return "$";
    }
    return std::string("$") + std::string(path);
  }
  return std::string("$.") + std::string(path);
}
