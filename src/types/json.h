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
#include <jsoncons/json_options.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <jsoncons_ext/jsonpath/jsonpath_error.hpp>
#include <jsoncons_ext/mergepatch/mergepatch.hpp>
#include <limits>

#include "status.h"

struct JsonValue {
  JsonValue() = default;
  explicit JsonValue(jsoncons::basic_json<char> value) : value(std::move(value)) {}

  static StatusOr<JsonValue> FromString(std::string_view str, int max_nesting_depth = std::numeric_limits<int>::max()) {
    jsoncons::json val;

    jsoncons::json_options options;
    options.max_nesting_depth(max_nesting_depth);

    try {
      val = jsoncons::json::parse(str, options);
    } catch (const jsoncons::ser_error &e) {
      return {Status::NotOK, e.what()};
    }

    return JsonValue(std::move(val));
  }

  StatusOr<std::string> Dump(int max_nesting_depth = std::numeric_limits<int>::max()) const {
    std::string res;
    GET_OR_RET(Dump(&res, max_nesting_depth));
    return res;
  }

  Status Dump(std::string *buffer, int max_nesting_depth = std::numeric_limits<int>::max()) const {
    jsoncons::json_options options;
    options.max_nesting_depth(max_nesting_depth);

    jsoncons::compact_json_string_encoder encoder{*buffer, options};
    std::error_code ec;
    value.dump(encoder, ec);
    if (ec) {
      return {Status::NotOK, ec.message()};
    }

    return Status::OK();
  }

  StatusOr<std::string> Print(uint8_t indent_size = 0, bool spaces_after_colon = false,
                              const std::string &new_line_chars = "") const {
    std::string res;
    GET_OR_RET(Print(&res, indent_size, spaces_after_colon, new_line_chars));
    return res;
  }

  Status Print(std::string *buffer, uint8_t indent_size = 0, bool spaces_after_colon = false,
               const std::string &new_line_chars = "") const {
    jsoncons::json_options options;
    options.indent_size(indent_size);
    options.spaces_around_colon(spaces_after_colon ? jsoncons::spaces_option::space_after
                                                   : jsoncons::spaces_option::no_spaces);
    options.spaces_around_comma(jsoncons::spaces_option::no_spaces);
    options.new_line_chars(new_line_chars);

    jsoncons::json_string_encoder encoder{*buffer, options};
    std::error_code ec;
    value.dump(encoder, ec);
    if (ec) {
      return {Status::NotOK, ec.message()};
    }

    return Status::OK();
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

  StatusOr<std::vector<size_t>> ArrAppend(std::string_view path, const std::vector<jsoncons::json> &append_values) {
    std::vector<size_t> result_count;

    try {
      jsoncons::jsonpath::json_replace(
          value, path, [&append_values, &result_count](const std::string & /*path*/, jsoncons::json &val) {
            if (val.is_array()) {
              val.insert(val.array_range().end(), append_values.begin(), append_values.end());
              result_count.emplace_back(val.size());
            } else {
              result_count.emplace_back(0);
            }
          });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return result_count;
  }

  Status Type(std::string_view path, std::vector<std::string> *types) const {
    types->clear();
    try {
      jsoncons::jsonpath::json_query(value, path, [&types](const std::string & /*path*/, const jsoncons::json &val) {
        switch (val.type()) {
          case jsoncons::json_type::null_value:
            types->emplace_back("null");
            break;
          case jsoncons::json_type::bool_value:
            types->emplace_back("boolean");
            break;
          case jsoncons::json_type::int64_value:
          case jsoncons::json_type::uint64_value:
            types->emplace_back("integer");
            break;
          case jsoncons::json_type::double_value:
            types->emplace_back("number");
            break;
          case jsoncons::json_type::string_value:
            types->emplace_back("string");
            break;
          case jsoncons::json_type::array_value:
            types->emplace_back("array");
            break;
          case jsoncons::json_type::object_value:
            types->emplace_back("object");
            break;
          default:
            types->emplace_back("unknown");
            break;
        }
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return Status::OK();
  }

  Status Merge(const std::string_view path, const std::string &merge_value) {
    try {
      jsoncons::json patch_value = jsoncons::json::parse(merge_value);
      bool not_exists = jsoncons::jsonpath::json_query(value, path).empty();

      if (path == "$" || not_exists) {
        //  For non-existing keys the path must be $
        if (not_exists) {
          // create an actual object from non-existing path
          jsoncons::json created_object = GenerateJsonFromPath(path.data(), patch_value);

          // merge it with patch value
          auto patch = jsoncons::mergepatch::from_diff(patch_value, created_object);
          jsoncons::mergepatch::apply_merge_patch(patch_value, patch);
        }

        // simply merge with the current value
        // merge patch function complies with RFC7396 Json Merge Patch
        jsoncons::mergepatch::apply_merge_patch(value, patch_value);
        return Status::OK();
      }

      // simply replace value by path
      if (!patch_value.is_null() && !not_exists) {
        jsoncons::jsonpath::json_replace(
            value, path,
            [&patch_value](const std::string & /*path*/, jsoncons::json &target) { target = patch_value; });
        return Status::OK();
      }

      // handle null case
      if (patch_value.is_null() && !not_exists) {
        size_t last_dot = path.find_last_of('.');
        if (last_dot != std::string::npos) {
          std::string key_to_remove = std::string(path.substr(last_dot + 1));
          auto parent_path = std::string(path.substr(0, last_dot));

          jsoncons::jsonpath::json_replace(value, parent_path,
                                           [&key_to_remove](const std::string & /*path*/, jsoncons::json &target) {
                                             if (target.is_object()) {
                                               target.erase(key_to_remove);
                                             }
                                           });
        }
        return Status::OK();
      }
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    } catch (const jsoncons::ser_error &e) {
      return {Status::NotOK, e.what()};
    }

    return {Status::NotOK, "Nothing to merge."};
  }

  static jsoncons::json GenerateJsonFromPath(const std::string &path, const jsoncons::json &value) {
    std::vector<std::string> parts;
    std::stringstream ss(path);
    std::string part;

    while (std::getline(ss, part, '.')) {
      if (part != "$") {
        parts.push_back(part);
      }
    }

    jsoncons::json result = value;
    for (auto it = parts.rbegin(); it != parts.rend(); ++it) {
      jsoncons::json obj;
      obj[*it] = result;
      result = obj;
    }

    return result;
  }

  JsonValue(const JsonValue &) = default;
  JsonValue(JsonValue &&) = default;

  JsonValue &operator=(const JsonValue &) = default;
  JsonValue &operator=(JsonValue &&) = default;

  ~JsonValue() = default;

  jsoncons::json value;
};
