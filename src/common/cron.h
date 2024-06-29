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

#include <ctime>
#include <iostream>
#include <string>
#include <variant>
#include <vector>

#include "parse_util.h"
#include "status.h"
#include "string_util.h"

struct CronPattern {
  using Number = int;
  using Range = std::pair<int, int>;

  struct Interval {
    int interval;
  };                                                         // */n
  struct Any {};                                             // *
  using Numbers = std::vector<std::variant<Number, Range>>;  // 1,2,3-6,7

  std::variant<Numbers, Interval, Any> val;

  static StatusOr<CronPattern> Parse(std::string_view str, std::tuple<int, int> minmax) {
    if (str == "*") {
      return CronPattern{Any{}};
    } else if (str.rfind("*/", 0) == 0) {
      auto num_str = str.substr(2);
      auto interval = GET_OR_RET(ParseInt<int>(std::string(num_str.begin(), num_str.end()), minmax)
                                     .Prefixed("an integer is expected after `*/` in a cron expression"));

      if (interval == 0) {
        return {Status::NotOK, "interval value after `*/` cannot be zero"};
      }

      return CronPattern{Interval{interval}};
    } else {
      auto num_strs = util::Split(str, ",");

      Numbers results;
      for (const auto &num_str : num_strs) {
        if (auto pos = num_str.find('-'); pos != num_str.npos) {
          auto l_str = num_str.substr(0, pos);
          auto r_str = num_str.substr(pos + 1);
          auto l = GET_OR_RET(ParseInt<int>(std::string(num_str.begin(), num_str.end()), minmax)
                                  .Prefixed("an integer is expected before `-` in a cron expression"));
          auto r = GET_OR_RET(ParseInt<int>(std::string(num_str.begin(), num_str.end()), minmax)
                                  .Prefixed("an integer is expected after `-` in a cron expression"));

          if (l >= r) {
            return {Status::NotOK, "for pattern `l-r` in cron expression, r should be larger than l"};
          }
          results.push_back(Range(l, r));
        } else {
          auto n = GET_OR_RET(ParseInt<int>(std::string(num_str.begin(), num_str.end()), minmax)
                                  .Prefixed("an integer is expected in a cron expression"));
          results.push_back(n);
        }
      }

      if (results.empty()) {
        return {Status::NotOK, "invalid cron expression"};
      }

      return CronPattern{results};
    }
  }

  std::string ToString() const {
    if (std::holds_alternative<Numbers>(val)) {
      std::string result;
      bool first = true;

      for (const auto &v : std::get<Numbers>(val)) {
        if (first)
          first = false;
        else
          result += ",";

        if (std::holds_alternative<Number>(v)) {
          result += std::to_string(std::get<Number>(v));
        } else {
          auto range = std::get<Range>(v);
          result += std::to_string(range.first) + "-" + std::to_string(range.second);
        }
      }

      return result;
    } else if (std::holds_alternative<Interval>(val)) {
      return "*/" + std::to_string(std::get<Interval>(val).interval);
    } else if (std::holds_alternative<Any>(val)) {
      return "*";
    }

    __builtin_unreachable();
  }

  bool IsMatch(int input, int interval_offset = 0) const {
    if (std::holds_alternative<Numbers>(val)) {
      bool result = false;
      for (const auto &v : std::get<Numbers>(val)) {
        if (std::holds_alternative<Number>(v)) {
          result = result || input == std::get<Number>(v);
        } else {
          auto range = std::get<Range>(v);
          result = result || (range.first <= input && input <= range.second);
        }
      }

      return result;
    } else if (std::holds_alternative<Interval>(val)) {
      return (input - interval_offset) % std::get<Interval>(val).interval == 0;
    } else if (std::holds_alternative<Any>(val)) {
      return true;
    }

    __builtin_unreachable();
  }
};

struct CronScheduler {
  CronPattern minute;
  CronPattern hour;
  CronPattern mday;
  CronPattern month;
  CronPattern wday;

  std::string ToString() const;
};

class Cron {
 public:
  Cron() = default;
  ~Cron() = default;

  Status SetScheduleTime(const std::vector<std::string> &args);
  bool IsTimeMatch(const tm *tm);
  std::string ToString() const;
  bool IsEnabled() const;

 private:
  std::vector<CronScheduler> schedulers_;
  tm last_tm_ = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, nullptr};

  static StatusOr<CronScheduler> convertToScheduleTime(const std::string &minute, const std::string &hour,
                                                       const std::string &mday, const std::string &month,
                                                       const std::string &wday);
};
