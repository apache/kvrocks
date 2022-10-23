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
#include <cctype>
#include <functional>
#include <iterator>

#include "parse_util.h"
#include "status.h"

template <typename Iter>
struct MoveIterator : Iter {
  using Iter::Iter;

  typename Iter::value_type&& operator*() const { return std::move(this->Iter::operator*()); }
};

template <typename Iter>
struct CommandParser {
 public:
  CommandParser(Iter begin, Iter end) : begin(begin), end(end) {}

  template <typename Container>
  explicit CommandParser(const Container& con, size_t skip_num = 0)
      : begin(std::begin(con) + skip_num), end(std::end(con)) {}

  template <typename Container>
  explicit CommandParser(Container&& con, size_t skip_num = 0)
      : begin(MoveIterator(std::begin(con) + skip_num)), end(MoveIterator(std::end(con))) {}

  decltype(auto) RawPeek() { return *begin; }

  decltype(auto) RawTake() { return *begin++; }

  decltype(auto) RawNext() { ++begin; }

  bool Good() { return begin != end; }

  template <typename Pred>
  bool EatPred(Pred&& pred) {
    if (Good() && std::forward<Pred>(pred)(RawPeek())) {
      RawNext();
      return true;
    } else {
      return false;
    }
  }

  bool EatEqICase(std::string_view str) {
    return EatPred([str](const auto& v) {
      return std::equal(v.begin(), v.end(), str.begin(),
                        [](char l, char r) { return std::tolower(l) == std::tolower(r); });
    });
  }

  bool EatEqICaseFlag(std::string_view str, std::string_view& flag) {
    if (str == flag || flag.empty()) {
      if (EatEqICase(str)) {
        flag = str;
        return true;
      }
    }

    return false;
  }

  template <typename Pred>
  auto TakePred(Pred&& pred) {
    if (!Good()) return Status{Status::RedisParseErr, "no more item to parse"};

    auto status = std::forward<Pred>(pred)(RawPeek());
    if (status) {
      RawNext();
    }

    return status;
  }

  StatusOr<std::string> TakeStr() {
    if (!Good()) return {Status::RedisParseErr, "no more item to parse"};

    return *begin;
  }

  template <typename T, typename... Args>
  StatusOr<T> TakeInt(Args&&... args) {
    if (!Good()) return {Status::RedisParseErr, "no more item to parse"};

    auto res = ParseInt<T>(RawPeek(), std::forward<Args>(args)...);

    if (res) {
      RawNext();
    }

    return res;
  }

 private:
  Iter begin;
  Iter end;
};

template <typename Container>
CommandParser(const Container& con, size_t) -> CommandParser<typename Container::const_iterator>;

template <typename Container>
CommandParser(Container&& con, size_t) -> CommandParser<MoveIterator<typename Container::iterator>>;
