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
#include "string_util.h"

template <typename Iter>
struct MoveIterator : Iter {
  explicit MoveIterator(Iter iter) : Iter(iter){};

  typename Iter::value_type&& operator*() const { return std::move(this->Iter::operator*()); }
};

template <typename Iter>
struct CommandParser {
 public:
  using ValueType = typename Iter::value_type;

  CommandParser(Iter begin, Iter end) : begin_(std::move(begin)), end_(std::move(end)) {}

  template <typename Container>
  explicit CommandParser(const Container& con, size_t skip_num = 0) : CommandParser(std::begin(con), std::end(con)) {
    std::advance(begin_, skip_num);
  }

  template <typename Container>
  explicit CommandParser(Container&& con, size_t skip_num = 0)
      : CommandParser(MoveIterator(std::begin(con)), MoveIterator(std::end(con))) {
    std::advance(begin_, skip_num);
  }

  decltype(auto) RawPeek() const { return *begin_; }

  decltype(auto) RawTake() { return *begin_++; }

  decltype(auto) RawNext() { ++begin_; }

  bool Good() const { return begin_ != end_; }

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
    return EatPred([str](const auto& v) { return util::EqualICase(str, v); });
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

  StatusOr<ValueType> TakeStr() {
    if (!Good()) return {Status::RedisParseErr, "no more item to parse"};

    return RawTake();
  }

  template <typename T = long long, typename... Args>
  StatusOr<T> TakeInt(Args&&... args) {
    if (!Good()) return {Status::RedisParseErr, "no more item to parse"};

    auto res = ParseInt<T>(RawPeek(), std::forward<Args>(args)...);

    if (res) {
      RawNext();
    }

    return res;
  }

  static Status InvalidSyntax() { return {Status::RedisParseErr, "syntax error"}; }

 private:
  Iter begin_;
  Iter end_;
};

template <typename Container>
CommandParser(const Container&, size_t = 0) -> CommandParser<typename Container::const_iterator>;

template <typename Container>
CommandParser(Container&&, size_t = 0) -> CommandParser<MoveIterator<typename Container::iterator>>;
