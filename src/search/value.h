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

#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "fmt/core.h"
#include "search/search_encoding.h"
#include "string_util.h"

namespace kqir {

using Null = std::monostate;

using Numeric = double;  // used for numeric fields

using String = std::string;  // e.g. a single tag

using NumericArray = std::vector<Numeric>;  // used for vector fields
using StringArray = std::vector<String>;    // used for tag fields, e.g. a list for tags

struct Value : std::variant<Null, Numeric, String, StringArray, NumericArray> {
  using Base = std::variant<Null, Numeric, String, StringArray, NumericArray>;

  using Base::Base;

  bool IsNull() const { return Is<Null>(); }

  template <typename T>
  bool Is() const {
    return std::holds_alternative<T>(*this);
  }

  template <typename T>
  bool IsOrNull() const {
    return Is<T>() || IsNull();
  }

  template <typename T>
  const auto &Get() const {
    CHECK(Is<T>());
    return std::get<T>(*this);
  }

  template <typename T>
  auto &Get() {
    CHECK(Is<T>());
    return std::get<T>(*this);
  }

  std::string ToString(const std::string &sep = ",") const {
    if (IsNull()) {
      return "";
    } else if (Is<Numeric>()) {
      return fmt::format("{}", Get<Numeric>());
    } else if (Is<String>()) {
      return Get<String>();
    } else if (Is<StringArray>()) {
      return util::StringJoin(
          Get<StringArray>(), [](const auto &v) -> decltype(auto) { return v; }, sep);
    } else if (Is<NumericArray>()) {
      return util::StringJoin(
          Get<NumericArray>(), [](const auto &v) -> decltype(auto) { return std::to_string(v); }, sep);
    }

    __builtin_unreachable();
  }

  std::string ToString(redis::IndexFieldMetadata *meta) const {
    if (IsNull()) {
      return "";
    } else if (Is<Numeric>()) {
      return fmt::format("{}", Get<Numeric>());
    } else if (Is<String>()) {
      return Get<String>();
    } else if (Is<StringArray>()) {
      auto tag = dynamic_cast<redis::TagFieldMetadata *>(meta);
      char sep = tag ? tag->separator : ',';
      return util::StringJoin(
          Get<StringArray>(), [](const auto &v) -> decltype(auto) { return v; }, std::string(1, sep));
    } else if (Is<NumericArray>()) {
      return util::StringJoin(Get<NumericArray>(), [](const auto &v) -> decltype(auto) { return std::to_string(v); });
    }

    __builtin_unreachable();
  }
};

template <typename T, typename... Args>
auto MakeValue(Args &&...args) {
  return Value(std::in_place_type<T>, std::forward<Args>(args)...);
}

}  // namespace kqir
