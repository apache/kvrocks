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

#include <cmath>
#include <limits>
#include <map>
#include <string>

#include "fmt/format.h"
#include "search/ir.h"
#include "string_util.h"

namespace kqir {

struct Interval {
  double l, r;  // [l, r)

  static inline const double inf = std::numeric_limits<double>::infinity();
  static inline const double minf = -inf;

  Interval(double l, double r) : l(l), r(r) {}

  bool IsEmpty() const { return l >= r; }
  static Interval Full() { return {minf, inf}; }

  bool operator==(const Interval &other) const { return l == other.l && r == other.r; }
  bool operator!=(const Interval &other) const { return !(*this == other); }

  std::string ToString() const { return fmt::format("[{}, {})", l, r); }
};

template <typename Iter1, typename Iter2, typename F>
void ForEachMerged(Iter1 first1, Iter1 last1, Iter2 first2, Iter2 last2, F &&f) {
  while (first1 != last1) {
    if (first2 == last2) {
      std::for_each(first1, last1, std::forward<F>(f));
      return;
    }

    if (*first2 < *first1) {
      std::forward<F>(f)(*first2);
      ++first2;
    } else {
      std::forward<F>(f)(*first1);
      ++first1;
    }
  }
  std::for_each(first2, last2, std::forward<F>(f));
}

struct IntervalSet {
  // NOTE: element must be sorted in this vector
  // but we don't need to use map here
  using DataType = std::vector<std::pair<double, double>>;
  DataType intervals;

  static inline const double inf = Interval::inf;
  static inline const double minf = Interval::minf;

  static double NextNum(double val) { return std::nextafter(val, inf); }

  static double PrevNum(double val) { return std::nextafter(val, minf); }

  explicit IntervalSet() = default;

  struct Full {};
  static constexpr const Full full{};

  explicit IntervalSet(Full) { intervals.emplace_back(minf, inf); }

  explicit IntervalSet(Interval range) {
    if (!range.IsEmpty()) intervals.emplace_back(range.l, range.r);
  }

  IntervalSet(NumericCompareExpr::Op op, double val) {
    if (op == NumericCompareExpr::EQ) {
      intervals.emplace_back(val, NextNum(val));
    } else if (op == NumericCompareExpr::NE) {
      intervals.emplace_back(minf, val);
      intervals.emplace_back(NextNum(val), inf);
    } else if (op == NumericCompareExpr::LT) {
      intervals.emplace_back(minf, val);
    } else if (op == NumericCompareExpr::GT) {
      intervals.emplace_back(NextNum(val), inf);
    } else if (op == NumericCompareExpr::LET) {
      intervals.emplace_back(minf, NextNum(val));
    } else if (op == NumericCompareExpr::GET) {
      intervals.emplace_back(val, inf);
    }
  }

  bool operator==(const IntervalSet &other) const { return intervals == other.intervals; }
  bool operator!=(const IntervalSet &other) const { return intervals != other.intervals; }

  std::string ToString() const {
    if (IsEmpty()) return "empty set";
    return util::StringJoin(intervals, [](const auto &i) { return Interval(i.first, i.second).ToString(); }, " or ");
  }

  friend std::ostream &operator<<(std::ostream &os, const IntervalSet &is) { return os << is.ToString(); }

  bool IsEmpty() const { return intervals.empty(); }
  bool IsFull() const {
    if (intervals.size() != 1) return false;

    const auto &v = *intervals.begin();
    return std::isinf(v.first) && std::isinf(v.second) && v.first * v.second < 0;
  }

  friend IntervalSet operator&(const IntervalSet &l, const IntervalSet &r) {
    IntervalSet result;

    if (l.intervals.empty() || r.intervals.empty()) {
      return result;
    }

    auto it_l = l.intervals.begin();
    auto it_r = r.intervals.begin();

    while (it_l != l.intervals.end() && it_r != r.intervals.end()) {
      // Find overlap between current intervals
      double start = std::max(it_l->first, it_r->first);
      double end = std::min(it_l->second, it_r->second);

      if (start <= end) {
        result.intervals.emplace_back(start, end);
      }

      if (it_l->second < it_r->second) {
        ++it_l;
      } else {
        ++it_r;
      }
    }

    return result;
  }

  friend IntervalSet operator|(const IntervalSet &l, const IntervalSet &r) {
    if (l.IsEmpty()) {
      return r;
    }

    if (r.IsEmpty()) {
      return l;
    }

    IntervalSet result;
    ForEachMerged(l.intervals.begin(), l.intervals.end(), r.intervals.begin(), r.intervals.end(),
                  [&result](const auto &v) {
                    if (result.IsEmpty() || result.intervals.rbegin()->second < v.first) {
                      result.intervals.emplace_back(v.first, v.second);
                    } else {
                      result.intervals.rbegin()->second = std::max(result.intervals.rbegin()->second, v.second);
                    }
                  });

    return result;
  }

  friend IntervalSet operator~(const IntervalSet &v) {
    if (v.IsEmpty()) {
      return IntervalSet(full);
    }

    IntervalSet result;

    auto iter = v.intervals.begin();
    if (!std::isinf(iter->first)) {
      result.intervals.emplace_back(minf, iter->first);
    }

    double last = iter->second;
    ++iter;
    while (iter != v.intervals.end()) {
      result.intervals.emplace_back(last, iter->first);

      last = iter->second;
      ++iter;
    }

    if (!std::isinf(last)) {
      result.intervals.emplace_back(last, inf);
    }

    return result;
  }
};

}  // namespace kqir
