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

#include <cstdlib>
#include <limits>
#include <string>
#include <tuple>

#include "status.h"
#include "string_util.h"

namespace details {

template <typename>
struct ParseIntFunc;

template <>
struct ParseIntFunc<char> {  // NOLINT
  constexpr static const auto value = std::strtol;
};

template <>
struct ParseIntFunc<short> {  // NOLINT
  constexpr static const auto value = std::strtol;
};

template <>
struct ParseIntFunc<int> {  // NOLINT
  constexpr static const auto value = std::strtol;
};

template <>
struct ParseIntFunc<long> {  // NOLINT
  constexpr static const auto value = std::strtol;
};

template <>
struct ParseIntFunc<long long> {  // NOLINT
  constexpr static const auto value = std::strtoll;
};

template <>
struct ParseIntFunc<unsigned char> {  // NOLINT
  constexpr static const auto value = std::strtoul;
};

template <>
struct ParseIntFunc<unsigned short> {  // NOLINT
  constexpr static const auto value = std::strtoul;
};

template <>
struct ParseIntFunc<unsigned> {  // NOLINT
  constexpr static const auto value = std::strtoul;
};

template <>
struct ParseIntFunc<unsigned long> {  // NOLINT
  constexpr static const auto value = std::strtoul;
};

template <>
struct ParseIntFunc<unsigned long long> {  // NOLINT
  constexpr static const auto value = std::strtoull;
};

}  // namespace details

template <typename T>
using ParseResultAndPos = std::tuple<T, const char *>;

// TryParseInt parses a string to a integer,
// if non-integer characters is encountered, it stop parsing and
// return the result integer and the current string position.
// e.g. TryParseInt("100MB") -> {100, "MB"}
// if no integer can be parsed or out of type range, an error will be returned
// base can be in {0, 2, ..., 36}, refer to strto* in standard c for more details
template <typename T = long long>  // NOLINT
StatusOr<ParseResultAndPos<T>> TryParseInt(const char *v, int base = 0) {
  char *end = nullptr;

  errno = 0;
  auto res = details::ParseIntFunc<T>::value(v, &end, base);

  if (v == end) {
    return {Status::NotOK, "not started as an integer"};
  }

  if (errno) {
    return Status::FromErrno();
  }

  if (!std::is_same<T, decltype(res)>::value &&
      (res < std::numeric_limits<T>::min() || res > std::numeric_limits<T>::max())) {
    return {Status::NotOK, "out of range of integer type"};
  }

  return ParseResultAndPos<T>{res, end};
}

// ParseInt parses a string to a integer,
// not like TryParseInt, the whole string need to be parsed as an integer,
// e.g. ParseInt("100MB") -> error status
template <typename T = long long>  // NOLINT
StatusOr<T> ParseInt(const std::string &v, int base = 0) {
  const char *begin = v.c_str();
  auto res = TryParseInt<T>(begin, base);

  if (!res) return res;

  if (std::get<1>(*res) != begin + v.size()) {
    return {Status::NotOK, "encounter non-integer characters"};
  }

  return std::get<0>(*res);
}

template <typename T>
using NumericRange = std::tuple<T, T>;

// this overload accepts a range {min, max},
// integer out of the range will trigger an error status
template <typename T = long long>  // NOLINT
StatusOr<T> ParseInt(const std::string &v, NumericRange<T> range, int base = 0) {
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

// ParseFloatArray parses a string to an array of floating-point number
// e.g. ParseFloatArray("1,2,3") -> {1.0, 2.0, 3.0}
template <typename T = double>
StatusOr<std::vector<T>> ParseFloatArray(const std::string &str) {
  std::vector<T> result;
  const char *current = str.c_str();
  const char *end = current + str.size();

  while (current < end) {
    auto [value, next_pos] = GET_OR_RET(TryParseFloat<T>(current));

    result.push_back(value);

    if (next_pos < end && *next_pos == ',') {
      next_pos++;
    }
    current = next_pos;

    while (current < end && std::isspace(*current)) {
      current++;
    }
  }

  return result;
}
