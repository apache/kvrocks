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

#include <algorithm>
#include <jsoncons/json.hpp>
#include <jsoncons/json_error.hpp>
#include <jsoncons/json_options.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/cbor/cbor_encoder.hpp>
#include <jsoncons_ext/cbor/cbor_options.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <jsoncons_ext/jsonpath/jsonpath_error.hpp>
#include <limits>
#include <string>

#include "status.h"

constexpr ssize_t NOT_FOUND_INDEX = -1;
constexpr ssize_t NOT_ARRAY = -2;

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

  static StatusOr<JsonValue> FromCBOR(std::string_view str, int max_nesting_depth = std::numeric_limits<int>::max()) {
    jsoncons::json val;

    jsoncons::cbor::cbor_options options;
    options.max_nesting_depth(max_nesting_depth);

    try {
      val = jsoncons::cbor::decode_cbor<jsoncons::json>(str, options);
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

  StatusOr<std::string> DumpCBOR(int max_nesting_depth = std::numeric_limits<int>::max()) const {
    std::string res;
    GET_OR_RET(DumpCBOR(&res, max_nesting_depth));
    return res;
  }

  Status DumpCBOR(std::string *buffer, int max_nesting_depth = std::numeric_limits<int>::max()) const {
    jsoncons::cbor::cbor_options options;
    options.max_nesting_depth(max_nesting_depth);

    jsoncons::cbor::basic_cbor_encoder<jsoncons::string_sink<std::string>> encoder{*buffer, options};
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

  static std::pair<ssize_t, ssize_t> NormalizeArrIndices(ssize_t start, ssize_t end, ssize_t len) {
    if (start < 0) {
      start = std::max<ssize_t>(0, len + start);
    } else {
      start = std::min<ssize_t>(start, len - 1);
    }
    if (end == 0) {
      end = len;
    } else if (end < 0) {
      end = std::max<ssize_t>(0, len + end);
    }
    end = std::min<ssize_t>(end, len);
    return {start, end};
  }

  StatusOr<std::vector<ssize_t>> ArrIndex(std::string_view path, const jsoncons::json &needle, ssize_t start,
                                          ssize_t end) const {
    std::vector<ssize_t> result;
    try {
      jsoncons::jsonpath::json_query(value, path, [&](const std::string & /*path*/, const jsoncons::json &val) {
        if (!val.is_array()) {
          result.emplace_back(NOT_ARRAY);
          return;
        }
        auto [pstart, pend] = NormalizeArrIndices(start, end, static_cast<ssize_t>(val.size()));
        auto arr_begin = val.array_range().begin();
        auto begin_it = arr_begin + pstart;

        auto end_it = arr_begin + pend;
        auto it = std::find(begin_it, end_it, needle);
        if (it != end_it) {
          result.emplace_back(it - arr_begin);
          return;
        }
        result.emplace_back(NOT_FOUND_INDEX);
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return result;
  }

  StatusOr<std::vector<std::string>> Type(std::string_view path) const {
    std::vector<std::string> types;
    try {
      jsoncons::jsonpath::json_query(value, path, [&types](const std::string & /*path*/, const jsoncons::json &val) {
        switch (val.type()) {
          case jsoncons::json_type::null_value:
            types.emplace_back("null");
            break;
          case jsoncons::json_type::bool_value:
            types.emplace_back("boolean");
            break;
          case jsoncons::json_type::int64_value:
          case jsoncons::json_type::uint64_value:
            types.emplace_back("integer");
            break;
          case jsoncons::json_type::double_value:
            types.emplace_back("number");
            break;
          case jsoncons::json_type::string_value:
            types.emplace_back("string");
            break;
          case jsoncons::json_type::array_value:
            types.emplace_back("array");
            break;
          case jsoncons::json_type::object_value:
            types.emplace_back("object");
            break;
          default:
            types.emplace_back("unknown");
            break;
        }
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return types;
  }

  StatusOr<std::vector<std::optional<bool>>> Toggle(std::string_view path) {
    std::vector<std::optional<bool>> result;
    try {
      jsoncons::jsonpath::json_replace(value, path, [&result](const std::string & /*path*/, jsoncons::json &val) {
        if (val.is_bool()) {
          val = !val.as_bool();
          result.emplace_back(val.as_bool());
        } else {
          result.emplace_back(std::nullopt);
        }
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return result;
  }

  StatusOr<size_t> Clear(std::string_view path) {
    size_t count = 0;
    try {
      jsoncons::jsonpath::json_replace(value, path, [&count](const std::string & /*path*/, jsoncons::json &val) {
        bool is_array = val.is_array() && !val.empty();
        bool is_object = val.is_object() && !val.empty();
        bool is_number = val.is_number() && val.as<double>() != 0;

        if (is_array)
          val = jsoncons::json::array();
        else if (is_object)
          val = jsoncons::json::object();
        else if (is_number)
          val = 0;
        else
          return;

        count++;
      });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return count;
  }

  Status ArrLen(std::string_view path, std::vector<std::optional<uint64_t>> &arr_lens) const {
    try {
      jsoncons::jsonpath::json_query(value, path,
                                     [&arr_lens](const std::string & /*path*/, const jsoncons::json &basic_json) {
                                       if (basic_json.is_array()) {
                                         arr_lens.emplace_back(static_cast<uint64_t>(basic_json.size()));
                                       } else {
                                         arr_lens.emplace_back(std::nullopt);
                                       }
                                     });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return Status::OK();
  }

  Status ObjKeys(std::string_view path, std::vector<std::optional<std::vector<std::string>>> &keys) const {
    try {
      jsoncons::jsonpath::json_query(value, path,
                                     [&keys](const std::string & /*path*/, const jsoncons::json &basic_json) {
                                       if (basic_json.is_object()) {
                                         std::vector<std::string> ret;
                                         for (const auto &member : basic_json.object_range()) {
                                           ret.push_back(member.key());
                                         }
                                         keys.emplace_back(ret);
                                       } else {
                                         keys.emplace_back(std::nullopt);
                                       }
                                     });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }
    return Status::OK();
  }

  StatusOr<std::vector<std::optional<JsonValue>>> ArrPop(std::string_view path, int64_t index = -1) {
    std::vector<std::optional<JsonValue>> popped_values;

    try {
      jsoncons::jsonpath::json_replace(value, path,
                                       [&popped_values, index](const std::string & /*path*/, jsoncons::json &val) {
                                         if (val.is_array() && !val.empty()) {
                                           auto len = static_cast<int64_t>(val.size());
                                           auto popped_iter = val.array_range().begin();
                                           if (index < 0) {
                                             popped_iter += len - std::min(len, -index);
                                           } else if (index > 0) {
                                             popped_iter += std::min(len - 1, index);
                                           }
                                           popped_values.emplace_back(*popped_iter);
                                           val.erase(popped_iter);
                                         } else {
                                           popped_values.emplace_back(std::nullopt);
                                         }
                                       });
    } catch (const jsoncons::jsonpath::jsonpath_error &e) {
      return {Status::NotOK, e.what()};
    }

    return popped_values;
  }

  Status ArrTrim(std::string_view path, int64_t start, int64_t stop, std::vector<std::optional<uint64_t>> &results) {
    try {
      jsoncons::jsonpath::json_replace(
          value, path, [&results, start, stop](const std::string & /*path*/, jsoncons::json &val) {
            if (val.is_array()) {
              auto len = static_cast<int64_t>(val.size());
              auto begin_index = start < 0 ? std::max(len + start, static_cast<int64_t>(0)) : start;
              auto end_index = std::min(stop < 0 ? std::max(len + stop, static_cast<int64_t>(0)) : stop, len - 1);

              if (begin_index >= len || begin_index > end_index) {
                val = jsoncons::json::array();
                results.emplace_back(0);
                return;
              }

              auto n_val = jsoncons::json::array();
              auto begin_iter = val.array_range().begin();

              n_val.insert(n_val.end(), begin_iter + begin_index, begin_iter + end_index + 1);
              val = n_val;
              results.emplace_back(static_cast<int64_t>(n_val.size()));
            } else {
              results.emplace_back(std::nullopt);
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
