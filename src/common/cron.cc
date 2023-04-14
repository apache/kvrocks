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

std::string Scheduler::ToString() const {
  auto param2string = [](int n) -> std::string { return n == -1 ? "*" : std::to_string(n); };
  return param2string(minute_) + " " + param2string(hour_) + " " + param2string(mday_) + " " + param2string(month_) +
         " " + param2string(wday_);
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

bool Cron::IsTimeMatch(struct tm *tm) {
  if (tm->tm_min == last_tm_.tm_min && tm->tm_hour == last_tm_.tm_hour && tm->tm_mday == last_tm_.tm_mday &&
      tm->tm_mon == last_tm_.tm_mon && tm->tm_wday == last_tm_.tm_wday) {
    return false;
  }
  for (const auto &st : schedulers_) {
    if ((st.minute_ == -1 || tm->tm_min == st.minute_) && (st.hour_ == -1 || tm->tm_hour == st.hour_) &&
        (st.mday_ == -1 || tm->tm_mday == st.mday_) && (st.month_ == -1 || (tm->tm_mon + 1) == st.month_) &&
        (st.wday_ == -1 || tm->tm_wday == st.wday_)) {
      last_tm_ = *tm;
      return true;
    }
  }
  return false;
}

bool Cron::IsEnabled() { return !schedulers_.empty(); }

std::string Cron::ToString() {
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

  st.minute_ = GET_OR_RET(convertParam(minute, 0, 59));
  st.hour_ = GET_OR_RET(convertParam(hour, 0, 23));
  st.mday_ = GET_OR_RET(convertParam(mday, 1, 31));
  st.month_ = GET_OR_RET(convertParam(month, 1, 12));
  st.wday_ = GET_OR_RET(convertParam(wday, 0, 6));

  return st;
}

StatusOr<int> Cron::convertParam(const std::string &param, int lower_bound, int upper_bound) {
  if (param == "*") {
    return -1;
  }

  auto s = ParseInt<int>(param, {lower_bound, upper_bound}, 10);
  if (!s) {
    return std::move(s).Prefixed(fmt::format("malformed cron token `{}`", param));
  }

  return *s;
}
