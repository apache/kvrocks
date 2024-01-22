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
#include <type_traits>

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

  static constexpr bool IsRandomAccessIter =
      std::is_base_of_v<std::random_access_iterator_tag, typename std::iterator_traits<Iter>::iterator_category>;

  CommandParser(Iter begin, Iter end) : begin_(std::move(begin)), end_(std::move(end)) {}

  template <typename Container, std::enable_if<std::is_lvalue_reference_v<Container>, int> = 0>
  explicit CommandParser(Container&& con, size_t skip_num = 0) : CommandParser(std::begin(con), std::end(con)) {
    std::advance(begin_, skip_num);
  }

  template <typename Container, std::enable_if<!std::is_lvalue_reference_v<Container>, int> = 0>
  explicit CommandParser(Container&& con, size_t skip_num = 0)
      : CommandParser(MoveIterator(std::begin(con)), MoveIterator(std::end(con))) {
    std::advance(begin_, skip_num);
  }

  CommandParser(const CommandParser&) = default;
  CommandParser(CommandParser&&) = default;

  CommandParser& operator=(const CommandParser&) = default;
  CommandParser& operator=(CommandParser&&) = default;

  ~CommandParser() = default;

  decltype(auto) RawPeek() const { return *begin_; }

  decltype(auto) operator[](size_t index) const {
    Iter iter = begin_;
    std::advance(iter, index);
    return *iter;
  }

  decltype(auto) RawTake() { return *begin_++; }

  decltype(auto) RawNext() { ++begin_; }

  bool Good() const { return begin_ != end_; }

  std::enable_if_t<IsRandomAccessIter, size_t> Remains() const {
    // O(1) iff Iter is random access iterator.
    auto d = std::distance(begin_, end_);
    DCHECK(d >= 0);
    return d;
  }

  size_t Skip(size_t count) {
    if constexpr (IsRandomAccessIter) {
      size_t steps = std::min(Remains(), count);
      begin_ += steps;
      return steps;
    } else {
      size_t steps = 0;
      while (count != 0 && Good()) {
        ++begin_;
        ++steps;
        --count;
      }
      return steps;
    }
  }

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

  template <typename T = double>
  StatusOr<T> TakeFloat() {
    if (!Good()) return {Status::RedisParseErr, "no more item to parse"};

    auto res = ParseFloat<T>(RawPeek());

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
