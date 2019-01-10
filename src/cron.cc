#include "cron.h"
#include <stdexcept>

Status Cron::SetScheduleTime(const std::vector<std::string> &args) {
  if (args.empty()) {
    schedule_times.clear();
    return Status::OK();
  }
  if (args.size() % 5 != 0) {
    return Status(Status::NotOK, "time expression format error,should only contain 5x field");
  }

  std::vector<schedule_time> new_schedule_times;
  schedule_time t;
  for (size_t i = 0; i < args.size(); i += 5) {
    Status s = convertConfToScheduleTime(args[i], args[i + 1], args[i + 2], args[i + 3], args[i + 4], t);
    if (!s.IsOK()) {
      return Status(Status::NotOK, "time expression format error : " + s.Msg());
    }
    new_schedule_times.push_back(t);
  }

  schedule_times.clear();
  for (unsigned long i = 0; i < new_schedule_times.size(); i++) {
    schedule_times.push_back(new_schedule_times[i]);
  }

  return Status::OK();
}

int Cron::IsTimeMatch(struct tm *tm) {
  for (schedule_time n : schedule_times) {
    if ((n.minute == PARAM_ALL_INT || tm->tm_min == n.minute) &&
        (n.hour == PARAM_ALL_INT || tm->tm_hour == n.hour) &&
        (n.mday == PARAM_ALL_INT || tm->tm_mday == n.mday) &&
        (n.month == PARAM_ALL_INT || (tm->tm_mon + 1) == n.month) &&
        (n.wday == PARAM_ALL_INT || tm->tm_wday == n.wday)) {
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
  for (schedule_time n : schedule_times) {
    if (schedule_times_string != "") {
      schedule_times_string += " ";
    }
    schedule_times_string += convertScheduleTimeParamToConfParam(n.minute) + " " +
        convertScheduleTimeParamToConfParam(n.hour) + " " +
        convertScheduleTimeParamToConfParam(n.mday) + " " +
        convertScheduleTimeParamToConfParam(n.month) + " " +
        convertScheduleTimeParamToConfParam(n.wday);
  }
  return schedule_times_string;
}

Status Cron::convertConfToScheduleTime(const std::string &minute,
                                       const std::string &hour,
                                       const std::string &mday,
                                       const std::string &month,
                                       const std::string &wday,
                                       schedule_time &t) {
  Status s;
  s = verifyAndConvertParam(minute, 0, 59, &t.minute);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(hour, 0, 23, &t.hour);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(mday, 1, 31, &t.mday);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(month, 1, 12, &t.month);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(wday, 0, 6, &t.wday);
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

