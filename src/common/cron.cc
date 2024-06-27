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

#include "fmt/core.h"
#include "parse_util.h"
#include "string_util.h"

std::string CronScheduler::ToString() const {
  return fmt::format("{} {} {} {} {}", minute.ToString(), hour.ToString(), mday.ToString(), month.ToString(),
                     wday.ToString());
}

Status Cron::SetScheduleTime(const std::vector<std::string> &args) {
  if (args.empty()) {
    schedulers_.clear();
    return Status::OK();
  }
  if (args.size() % 5 != 0) {
    return {Status::NotOK, "cron expression format error, should only contain 5x fields"};
  }

  std::vector<CronScheduler> new_schedulers;
  for (size_t i = 0; i < args.size(); i += 5) {
    auto s = convertToScheduleTime(args[i], args[i + 1], args[i + 2], args[i + 3], args[i + 4]);
    if (!s.IsOK()) {
      return std::move(s).Prefixed("cron expression format error");
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
    bool minute_match = st.minute.IsMatch(tm->tm_min);
    bool hour_match = st.hour.IsMatch(tm->tm_hour);
    bool mday_match = st.mday.IsMatch(tm->tm_mday, 1);
    bool month_match = st.month.IsMatch(tm->tm_mon + 1, 1);
    bool wday_match = st.wday.IsMatch(tm->tm_wday);

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

StatusOr<CronScheduler> Cron::convertToScheduleTime(const std::string &minute, const std::string &hour,
                                                    const std::string &mday, const std::string &month,
                                                    const std::string &wday) {
  CronScheduler st;

  st.minute = GET_OR_RET(CronPattern::Parse(minute, {0, 59}));
  st.hour = GET_OR_RET(CronPattern::Parse(hour, {0, 23}));
  st.mday = GET_OR_RET(CronPattern::Parse(mday, {1, 31}));
  st.month = GET_OR_RET(CronPattern::Parse(month, {1, 12}));
  st.wday = GET_OR_RET(CronPattern::Parse(wday, {0, 6}));

  return st;
}
