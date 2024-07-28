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

#include <map>
#include <tao/pegtl/contrib/parse_tree.hpp>
#include <tao/pegtl/contrib/unescape.hpp>
#include <tao/pegtl/demangle.hpp>

#include "common_parser.h"
#include "status.h"

namespace kqir {

using ParamMap = std::map<std::string, std::string, std::less<>>;

struct TreeTransformer {
  using TreeNode = std::unique_ptr<peg::parse_tree::node>;

  const ParamMap& param_map;

  explicit TreeTransformer(const ParamMap& param_map) : param_map(param_map) {}

  StatusOr<std::string> GetParam(const TreeNode& node) {
    // node->type must be Param here
    auto name = node->string_view().substr(1);

    auto iter = param_map.find(name);
    if (iter == param_map.end()) {
      return {Status::NotOK, fmt::format("parameter with name `{}` not found", name)};
    }

    return iter->second;
  }

  template <typename T>
  static bool Is(const TreeNode& node) {
    return node->type == peg::demangle<T>();
  }

  static bool IsRoot(const TreeNode& node) { return node->type.empty(); }

  static StatusOr<std::string> UnescapeString(std::string_view str) {
    str = str.substr(1, str.size() - 2);

    std::string result;
    while (!str.empty()) {
      if (str[0] == '\\') {
        str.remove_prefix(1);
        switch (str[0]) {
          case '\\':
          case '"':
            result.push_back(str[0]);
            break;
          case 'b':
            result.push_back('\b');
            break;
          case 'f':
            result.push_back('\f');
            break;
          case 'n':
            result.push_back('\n');
            break;
          case 'r':
            result.push_back('\r');
            break;
          case 't':
            result.push_back('\t');
            break;
          case 'u':
            if (!peg::unescape::utf8_append_utf32(
                    result, peg::unescape::unhex_string<unsigned>(str.data() + 1, str.data() + 5))) {
              return {Status::NotOK,
                      fmt::format("invalid Unicode code point '{}' in string literal", std::string(str.data() + 1, 4))};
            }
            str.remove_prefix(4);
            break;
          default:
            __builtin_unreachable();
        };
        str.remove_prefix(1);
      } else {
        result.push_back(str[0]);
        str.remove_prefix(1);
      }
    }

    return result;
  }

  static StatusOr<std::vector<char>> Binary2Chars(std::string_view str) {
    std::vector<char> data;
    size_t i = 0;

    auto hex_char_to_binary = [](char c) -> StatusOr<char> {
      if (c >= '0' && c <= '9') return c - '0';
      if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
      if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
      return {Status::NotOK, "invalid hexadecimal character"};
    };

    while (i + 3 < str.size()) {
      if (str[i] == '\\' && str[i + 1] == 'x') {
        auto high = GET_OR_RET(hex_char_to_binary(str[i + 2]));
        auto low = GET_OR_RET(hex_char_to_binary(str[i + 3]));
        data.push_back((high << 4) | low);
        i += 4;
      } else {
        return {Status::NotOK, "invalid binary representation or unsupported character"};
      }
    }

    if (i != str.size()) {
      return {Status::NotOK, "input string does not align with expected length"};
    }

    return data;
  }

  template <typename T>
  StatusOr<std::vector<T>> CharsToVector(const std::vector<char>& data) {
    if (data.size() % sizeof(T) != 0) {
      return {Status::NotOK, "Data size is not a multiple of the target type size"};
    }

    std::vector<T> converted_data;
    converted_data.reserve(data.size() / sizeof(T));

    for (size_t i = 0; i < data.size(); i += sizeof(T)) {
      T value;
      std::memcpy(&value, &data[i], sizeof(T));
      converted_data.push_back(value);
    }

    return converted_data;
  }
};

}  // namespace kqir
