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
std::optional<std::string_view> JsonPath::tryConvertLegacyToJsonPath(std::string_view path) {
  // TODO(mwish): currently I just handle the simplest logic,
  //  port from RedisJson JsonPathParser::parse later.
  if (path == ".") {
    return "$";
  }
  return std::nullopt;
}