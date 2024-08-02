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

  template <typename T = double>
  static StatusOr<std::vector<T>> Binary2Vector(std::string_view str) {
    if (str.size() % sizeof(T) != 0) {
      return {Status::NotOK, "data size is not a multiple of the target type size"};
    }

    std::vector<T> values;
    const size_t type_size = sizeof(T);
    values.reserve(str.size() / type_size);

    while (!str.empty()) {
      T value;
      memcpy(&value, str.data(), type_size);
      values.push_back(value);
      str.remove_prefix(type_size);
    }

    return values;
  }
};

}  // namespace kqir
