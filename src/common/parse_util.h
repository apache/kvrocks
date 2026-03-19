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

#include <charconv>
#include <string_view>
#include <tuple>
#include <type_traits>

#include "fast_float/fast_float.h"
#include "status.h"

template <typename T>
using ParseResultAndPos = std::tuple<T, const char *>;

// TryParseInt parses a string to an integer,
// if non-integer characters is encountered, it stop parsing and
// return the result integer and the current string position.
// e.g. TryParseInt("100MB") -> {100, "MB"}
// if no integer can be parsed or out of type range, an error will be returned
// base can be in {2, ..., 36}
template <typename T = long long, std::enable_if_t<std::is_integral_v<T>, int> = 0>  // NOLINT
StatusOr<ParseResultAndPos<T>> TryParseInt(std::string_view v, int base = 10) {
  T res = 0;
  auto [end, ec] = std::from_chars(v.data(), v.data() + v.size(), res, base);

  if (v.data() == end) {
    return {Status::NotOK, "not started as an integer"};
  }

  if (ec != std::errc()) {
    if (ec == std::errc::result_out_of_range) {
      return {Status::NotOK, "out of range of integer type"};
    }
    return {Status::NotOK, std::make_error_code(ec).message()};
  }

  return ParseResultAndPos<T>{res, end};
}

// ParseInt parses a string to a integer,
// not like TryParseInt, the whole string need to be parsed as an integer,
// e.g. ParseInt("100MB") -> error status
template <typename T = long long>  // NOLINT
StatusOr<T> ParseInt(std::string_view v, int base = 10) {
  auto res = TryParseInt<T>(v, base);

  if (!res) return res;

  if (std::get<1>(*res) != v.data() + v.size()) {
    return {Status::NotOK, "encounter non-integer characters"};
  }

  return std::get<0>(*res);
}

template <typename T>
using NumericRange = std::tuple<T, T>;

// this overload accepts a range {min, max},
// integer out of the range will trigger an error status
template <typename T = long long>  // NOLINT
StatusOr<T> ParseInt(std::string_view v, NumericRange<T> range, int base = 10) {
  auto res = ParseInt<T>(v, base);

  if (!res) return res;

  if (*res < std::get<0>(range) || *res > std::get<1>(range)) {
    return {Status::NotOK, "out of numeric range"};
  }

  return *res;
}

// available units: K, M, G, T, P
StatusOr<std::uint64_t> ParseSizeAndUnit(std::string_view v);

// TryParseFloat parses a string to a floating-point number,
// it returns the first unmatched character position instead of an error status
template <typename T = double, std::enable_if_t<std::is_same_v<T, float> || std::is_same_v<T, double>, int> = 0>
StatusOr<ParseResultAndPos<T>> TryParseFloat(std::string_view v) {
  T result = 0;
  auto [end, ec] =
      fast_float::from_chars(v.data(), v.data() + v.size(), result,
                             fast_float::chars_format::general | fast_float::chars_format::allow_leading_plus);

  if (v.data() == end) {
    return {Status::NotOK, "not started as a number"};
  }

  if (ec != std::errc()) {
    return {Status::NotOK, std::make_error_code(ec).message()};
  }

  return {result, end};
}

// ParseFloat parses a string to a floating-point number
template <typename T = double>
StatusOr<T> ParseFloat(std::string_view str) {
  auto [result, pos] = GET_OR_RET(TryParseFloat<T>(str));

  if (pos != str.data() + str.size()) {
    return {Status::NotOK, "encounter non-number characters"};
  }

  return result;
}
