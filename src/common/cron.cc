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

#include "cron.h"

#include <stdexcept>
#include <utility>

#include "parse_util.h"
#include "string_util.h"

std::string Scheduler::ToString() const {
  auto param2string = [](int n, bool is_interval) -> std::string {
    if (n == -1) return "*";
    return is_interval ? "*/" + std::to_string(n) : std::to_string(n);
  };
  return param2string(minute, minute_interval) + " " + param2string(hour, hour_interval) + " " +
         param2string(mday, mday_interval) + " " + param2string(month, month_interval) + " " +
         param2string(wday, wday_interval);
}

Status Cron::SetScheduleTime(const std::vector<std::string> &args) {
  if (args.empty()) {
    schedulers_.clear();
    return Status::OK();
  }
  if (args.size() % 5 != 0) {
    return {Status::NotOK, "time expression format error,should only contain 5x fields"};
  }

  std::vector<Scheduler> new_schedulers;
  for (size_t i = 0; i < args.size(); i += 5) {
    auto s = convertToScheduleTime(args[i], args[i + 1], args[i + 2], args[i + 3], args[i + 4]);
    if (!s.IsOK()) {
      return std::move(s).Prefixed("time expression format error");
    }
    new_schedulers.push_back(*s);
  }
  schedulers_ = std::move(new_schedulers);
  return Status::OK();
}

bool Cron::IsTimeMatch(const tm *tm) {
  if (tm->tm_min == last_tm_.tm_min && tm->tm_hour == last_tm_.tm_hour && tm->tm_mday == last_tm_.tm_mday &&
      tm->tm_mon == last_tm_.tm_mon && tm->tm_wday == last_tm_.tm_wday) {
    return false;
  }
  for (const auto &st : schedulers_) {
    bool minute_match = (st.minute == -1 || tm->tm_min == st.minute ||
                         (st.minute > 0 && st.minute <= 59 && tm->tm_min % st.minute == 0 && st.minute_interval));
    bool hour_match = (st.hour == -1 || tm->tm_hour == st.hour ||
                       (st.hour > 0 && st.hour <= 23 && tm->tm_hour % st.hour == 0 && st.hour_interval));
    bool mday_match = (st.mday == -1 || tm->tm_mday == st.mday ||
                       (st.mday > 0 && st.mday <= 31 && tm->tm_mday % st.mday == 0 && st.mday_interval) ||
                       (st.mday == 0 && tm->tm_mday == 1));
    bool month_match = (st.month == -1 || tm->tm_mon == st.month ||
                        (st.month > 0 && st.month <= 12 && (tm->tm_mon - 1) % st.month == 0 && st.month_interval));
    bool wday_match = (st.wday == -1 || tm->tm_wday == st.wday ||
                       (st.wday >= 0 && st.wday <= 6 && tm->tm_wday % st.wday == 0 && st.wday_interval));

    if (minute_match && hour_match && mday_match && month_match && wday_match) {
      last_tm_ = *tm;
      return true;
    }
  }
  return false;
}

bool Cron::IsEnabled() const { return !schedulers_.empty(); }

std::string Cron::ToString() const {
  std::string ret;
  for (size_t i = 0; i < schedulers_.size(); i++) {
    ret += schedulers_[i].ToString();
    if (i != schedulers_.size() - 1) ret += " ";
  }
  return ret;
}

StatusOr<Scheduler> Cron::convertToScheduleTime(const std::string &minute, const std::string &hour,
                                                const std::string &mday, const std::string &month,
                                                const std::string &wday) {
  Scheduler st;

  st.minute = GET_OR_RET(convertParam(minute, 0, 59, st.minute_interval));
  st.hour = GET_OR_RET(convertParam(hour, 0, 23, st.hour_interval));
  st.mday = GET_OR_RET(convertParam(mday, 1, 31, st.mday_interval));
  st.month = GET_OR_RET(convertParam(month, 1, 12, st.month_interval));
  st.wday = GET_OR_RET(convertParam(wday, 0, 6, st.wday_interval));

  return st;
}

StatusOr<int> Cron::convertParam(const std::string &param, int lower_bound, int upper_bound, bool &is_interval) {
  if (param == "*") {
    return -1;
  }

  // Check for interval syntax (*/n)
  if (util::HasPrefix(param, "*/")) {
    auto s = ParseInt<int>(param.substr(2), {lower_bound, upper_bound}, 10);
    if (!s || *s == 0) {
      return std::move(s).Prefixed(fmt::format("malformed cron token `{}`", param));
    }
    is_interval = true;
    return *s;
  }

  auto s = ParseInt<int>(param, {lower_bound, upper_bound}, 10);
  if (!s) {
    return std::move(s).Prefixed(fmt::format("malformed cron token `{}`", param));
  }

  return *s;
}
