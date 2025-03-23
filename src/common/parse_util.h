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

#include <cctype>
#include <charconv>
#include <cstdlib>
#include <string>
#include <string_view>
#include <tuple>

#include "status.h"

template <typename T>
using ParseResultAndPos = std::tuple<T, const char *>;

// TryParseInt parses a string to a integer,
// if non-integer characters is encountered, it stop parsing and
// return the result integer and the current string position.
// e.g. TryParseInt("100MB") -> {100, "MB"}
// if no integer can be parsed or out of type range, an error will be returned
// base can be in {0, 2, ..., 36}
template <typename T = int64_t>
StatusOr<ParseResultAndPos<T>> TryParseInt(std::string_view v, int base = 0) {
  static const std::string ErrNotInteger = "not started as an integer";

  T res;

  // Skip leading spaces
  const char *p = v.data();
  const char *end = v.data() + v.size();
  while (p < end && std::isspace(static_cast<unsigned char>(*p))) {
    ++p;
  }

  if (p == end) {
    return {Status::NotOK, ErrNotInteger};
  }

  if (base == 0) {
    if (*p == '0') {
      if (p + 1 < end) {
        if (std::tolower(*(p + 1)) == 'x') {
          base = 16;
          p += 2;
        } else if (std::tolower(*(p + 1)) == 'b') {
          base = 2;
          p += 2;
        } else {
          base = 8;
          p += 1;
        }
      }
    } else {
      base = 10;
    }
  } else if (base < 2 || base > 36) {
    return {Status::NotOK, "invalid base (must be 2~36 or 0)"};
  }

  auto [ptr, ec] = std::from_chars(p, end, res, base);
  if (ec == std::errc::invalid_argument) {
    return {Status::NotOK, ErrNotInteger};
  } else if (ec == std::errc::result_out_of_range) {
    return {Status::NotOK, "out of range of integer type"};
  }

  if (ptr == p) {
    return {Status::NotOK, ErrNotInteger};
  }

  return ParseResultAndPos<T>{res, ptr};
}

// ParseInt parses a string to a integer,
// not like TryParseInt, the whole string need to be parsed as an integer,
// e.g. ParseInt("100MB") -> error status
template <typename T = long long>  // NOLINT
StatusOr<T> ParseInt(std::string_view v, int base = 0) {
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
StatusOr<T> ParseInt(std::string_view v, NumericRange<T> range, int base = 0) {
  auto res = ParseInt<T>(v, base);

  if (!res) return res;

  if (*res < std::get<0>(range) || *res > std::get<1>(range)) {
    return {Status::NotOK, "out of numeric range"};
  }

  return *res;
}

// available units: K, M, G, T, P
StatusOr<std::uint64_t> ParseSizeAndUnit(const std::string &v);

template <typename>
struct ParseFloatFunc;

template <>
struct ParseFloatFunc<float> {
  constexpr static const auto value = strtof;
};

template <>
struct ParseFloatFunc<double> {
  constexpr static const auto value = strtod;
};

template <>
struct ParseFloatFunc<long double> {
  constexpr static const auto value = strtold;
};

// TryParseFloat parses a string to a floating-point number,
// it returns the first unmatched character position instead of an error status
template <typename T = double>  // float or double
StatusOr<ParseResultAndPos<T>> TryParseFloat(const char *str) {
  char *end = nullptr;

  errno = 0;
  T result = ParseFloatFunc<T>::value(str, &end);

  if (str == end) {
    return {Status::NotOK, "not started as a number"};
  }

  if (errno) {
    return Status::FromErrno();
  }

  return {result, end};
}

// ParseFloat parses a string to a floating-point number
template <typename T = double>  // float or double
StatusOr<T> ParseFloat(const std::string &str) {
  const char *begin = str.c_str();
  auto [result, pos] = GET_OR_RET(TryParseFloat<T>(begin));

  if (pos != begin + str.size()) {
    return {Status::NotOK, "encounter non-number characters"};
  }

  return result;
}
