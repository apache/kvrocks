#include "cron.h"
#include <stdexcept>

Status Cron::SetScheduleTime(const std::vector<std::string> &args) {
  if (args.empty()) {
    schedule_times.clear();
    return Status::OK();
  }
  if (args.size() % 5 != 0) {
    return Status(Status::NotOK, "time expression format error,should only contain 5x fields");
  }

  std::vector<schedule_time> new_schedule_times;
  schedule_time st;
  for (size_t i = 0; i < args.size(); i += 5) {
    Status s = convertConfToScheduleTime(args[i], args[i + 1], args[i + 2], args[i + 3], args[i + 4], &st);
    if (!s.IsOK()) {
      return Status(Status::NotOK, "time expression format error : " + s.Msg());
    }
    new_schedule_times.push_back(st);
  }

  schedule_times.clear();
  for (unsigned long i = 0; i < new_schedule_times.size(); i++) {
    schedule_times.push_back(new_schedule_times[i]);
  }

  return Status::OK();
}

int Cron::IsTimeMatch(struct tm *tm) {
  for (const auto &st : schedule_times) {
    if ((st.minute == PARAM_ALL_INT || tm->tm_min == st.minute) &&
        (st.hour == PARAM_ALL_INT || tm->tm_hour == st.hour) &&
        (st.mday == PARAM_ALL_INT || tm->tm_mday == st.mday) &&
        (st.month == PARAM_ALL_INT || (tm->tm_mon + 1) == st.month) &&
        (st.wday == PARAM_ALL_INT || tm->tm_wday == st.wday)) {
      return 1;
    }
  }

  return 0;
}

bool Cron::IsEnabled() {
  return !schedule_times.empty();
}

std::string Cron::ToString() {
  std::string schedule_times_string = "";
  for (const auto &st : schedule_times) {
    if (schedule_times_string != "") {
      schedule_times_string += " ";
    }
    schedule_times_string += convertScheduleTimeParamToConfParam(st.minute) + " " +
        convertScheduleTimeParamToConfParam(st.hour) + " " +
        convertScheduleTimeParamToConfParam(st.mday) + " " +
        convertScheduleTimeParamToConfParam(st.month) + " " +
        convertScheduleTimeParamToConfParam(st.wday);
  }
  return schedule_times_string;
}

Status Cron::convertConfToScheduleTime(const std::string &minute,
                                       const std::string &hour,
                                       const std::string &mday,
                                       const std::string &month,
                                       const std::string &wday,
                                       schedule_time *st) {
  Status s;
  s = verifyAndConvertParam(minute, 0, 59, &st->minute);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(hour, 0, 23, &st->hour);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(mday, 1, 31, &st->mday);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(month, 1, 12, &st->month);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(wday, 0, 6, &st->wday);
  return s;
}

std::string Cron::convertScheduleTimeParamToConfParam(const int &param) {
  if (param == PARAM_ALL_INT) {
    return PARAM_ALL;
  }
  return std::to_string(param);
}

Status Cron::verifyAndConvertParam(const std::string &param, int lower_bound, int upper_bound, int *value) {
  if (param == PARAM_ALL) {
    *value = PARAM_ALL_INT;
    return Status::OK();
  }

  try {
    *value = std::stoi(param);
  } catch (const std::invalid_argument &e) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) not an integer or " + PARAM_ALL);
  } catch (const std::out_of_range &e) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) not convertable to int");
  }
  if (*value < lower_bound || *value > upper_bound) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) out of bound");
  }
  return Status::OK();
}

