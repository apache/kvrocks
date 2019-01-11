#include "cron.h"
#include <stdexcept>

Status Cron::SetScheduleTime(const std::vector<std::string> &args) {
  if (args.empty()) {
    schedule_times_.clear();
    return Status::OK();
  }
  if (args.size() % 5 != 0) {
    return Status(Status::NotOK, "time expression format error,should only contain 5x fields");
  }

  std::vector<schedule_time> new_schedule_times;
  schedule_time st;
  for (size_t i = 0; i < args.size(); i += 5) {
    Status s = convertToScheduleTime(args[i], args[i + 1], args[i + 2], args[i + 3], args[i + 4], &st);
    if (!s.IsOK()) {
      return Status(Status::NotOK, "time expression format error : " + s.Msg());
    }
    new_schedule_times.push_back(st);
  }

  schedule_times_.clear();
  for (unsigned long i = 0; i < new_schedule_times.size(); i++) {
    schedule_times_.push_back(new_schedule_times[i]);
  }

  return Status::OK();
}

int Cron::IsTimeMatch(struct tm *tm) {
  for (const auto &st : schedule_times_) {
    if ((st.minute == PARAM_WILDCARD_INT || tm->tm_min == st.minute) &&
        (st.hour == PARAM_WILDCARD_INT || tm->tm_hour == st.hour) &&
        (st.mday == PARAM_WILDCARD_INT || tm->tm_mday == st.mday) &&
        (st.month == PARAM_WILDCARD_INT || (tm->tm_mon + 1) == st.month) &&
        (st.wday == PARAM_WILDCARD_INT || tm->tm_wday == st.wday)) {
      return 1;
    }
  }

  return 0;
}

bool Cron::IsEnabled() {
  return !schedule_times_.empty();
}

std::string Cron::ToString() {
  std::string schedule_times_string = "";
  for (const auto &st : schedule_times_) {
    if (schedule_times_string != "") {
      schedule_times_string += " ";
    }
    schedule_times_string += paramToString(st.minute) + " " +
        paramToString(st.hour) + " " +
        paramToString(st.mday) + " " +
        paramToString(st.month) + " " +
        paramToString(st.wday);
  }
  return schedule_times_string;
}

std::vector<std::string> Cron::ToConfParamVector() {
  std::vector<std::string> sts;
  for (const auto &st : schedule_times_) {
    sts.push_back(paramToString(st.minute));
    sts.push_back(paramToString(st.hour));
    sts.push_back(paramToString(st.mday));
    sts.push_back(paramToString(st.month));
    sts.push_back(paramToString(st.wday));
  }
  return sts;
}

Status Cron::convertToScheduleTime(const std::string &minute,
                                   const std::string &hour,
                                   const std::string &mday,
                                   const std::string &month,
                                   const std::string &wday,
                                   schedule_time *st) {
  Status s;
  s = convertParam(minute, 0, 59, &st->minute);
  if (!s.IsOK()) return s;
  s = convertParam(hour, 0, 23, &st->hour);
  if (!s.IsOK()) return s;
  s = convertParam(mday, 1, 31, &st->mday);
  if (!s.IsOK()) return s;
  s = convertParam(month, 1, 12, &st->month);
  if (!s.IsOK()) return s;
  s = convertParam(wday, 0, 6, &st->wday);
  return s;
}

std::string Cron::paramToString(const int &param) {
  if (param == PARAM_WILDCARD_INT) {
    return PARAM_WILDCARD;
  }
  return std::to_string(param);
}

Status Cron::convertParam(const std::string &param, int lower_bound, int upper_bound, int *value) {
  if (param == PARAM_WILDCARD) {
    *value = PARAM_WILDCARD_INT;
    return Status::OK();
  }

  try {
    *value = std::stoi(param);
  } catch (const std::invalid_argument &e) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) not an integer or " + PARAM_WILDCARD);
  } catch (const std::out_of_range &e) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) not convertable to int");
  }
  if (*value < lower_bound || *value > upper_bound) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) out of bound");
  }
  return Status::OK();
}

