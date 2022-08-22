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

#include <limits>
#include <status.h>
#include <cstdlib>

namespace details {

template <typename>
struct ParseIntFunc;

template <>
struct ParseIntFunc<short> {
  constexpr static const auto value = std::strtol;
};

template <>
struct ParseIntFunc<int> {
  constexpr static const auto value = std::strtol;
};

template <>
struct ParseIntFunc<long> {
  constexpr static const auto value = std::strtol;
};

template <>
struct ParseIntFunc<long long> {
  constexpr static const auto value = std::strtoll;
};

template <>
struct ParseIntFunc<unsigned short> {
  constexpr static const auto value = std::strtoul;
};

template <>
struct ParseIntFunc<unsigned> {
  constexpr static const auto value = std::strtoul;
};

template <>
struct ParseIntFunc<unsigned long> {
  constexpr static const auto value = std::strtoul;
};

template <>
struct ParseIntFunc<unsigned long long> {
  constexpr static const auto value = std::strtoull;
};

}

template <typename T>
using ParseResultAndPos = std::tuple<T, const char *>;

template <typename T = long long>
StatusOr<ParseResultAndPos<T>> TryParseInt(const char *v, int base = 0) {
  char *end;

  errno = 0;
  auto res = details::ParseIntFunc<T>::value(v, &end, base);

  if(v == end) {
    return {Status::NotOK, "TryParseInt: not started as an integer"};
  }

  if(errno) {
    return {Status::NotOK, std::string{"TryParseInt: "} + std::strerror(errno)};
  }

  if(!std::is_same<T, decltype(res)>::value &&
    (res < std::numeric_limits<T>::min() || res > std::numeric_limits<T>::max())) {
    return {Status::NotOK, "TryParseInt: out of range of integer type"};
  }

  return ParseResultAndPos<T>{res, end};
}

template <typename T = long long>
StatusOr<T> ParseInt(const std::string& v, int base = 0) {
  const char *begin = v.c_str();
  auto res = TryParseInt<T>(begin, base);

  if(!res) return res;

  if(std::get<1>(*res) != begin + v.size()) {
    return {Status::NotOK, "ParseInt: encounter non-integer characters"};
  }

  return std::get<0>(*res);
}

template <typename T>
using NumericRange = std::tuple<T, T>;

template <typename T = long long>
StatusOr<T> ParseInt(const std::string& v, NumericRange<T> range, int base = 0) {
  auto res = ParseInt<T>(v, base);

  if(!res) return res;

  if(*res < std::get<0>(range) || *res > std::get<1>(range)) {
    return {Status::NotOK, "ParseInt: out of numeric range"};
  }

  return *res;
}
