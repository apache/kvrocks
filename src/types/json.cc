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

#include "json.h"

namespace redis {

StatusOr<JsonPath> JsonPath::BuildJsonPath(std::string path) {
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
    return Status::FromErrno(json_parse_error.message());
  }
  return JsonPath(std::move(path), std::move(fixed_path), std::move(path_expression));
}

// https://redis.io/docs/data-types/json/path/#legacy-path-syntax
std::optional<std::string> JsonPath::tryConvertLegacyToJsonPath(Slice path) {
  // TODO(mwish): currently I just handle the simplest logic,
  //  port from RedisJson JsonPathParser::parse later.
  if (path == ".") {
    return "$";
  }
  return std::nullopt;
}

StatusOr<JsonType> ParseJson(std::string_view data) {
  jsoncons::json_parser parser;
  parser.update(data.data(), data.size());
  std::error_code ec;
  jsoncons::json_decoder<JsonType> json_decoder;
  parser.finish_parse(json_decoder, ec);
  if (!json_decoder.is_valid()) {
    return {ec.message()};
  }
  return json_decoder.get_result();
}

std::string ToString(const JsonType& json_value) {
  std::string json_str;
  jsoncons::encode_json(json_value, json_str);
  return json_str;
}

}  // namespace redis
