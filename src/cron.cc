#include "cron.h"
#include <stdexcept>

Status Cron::SetParams(const std::string &minute,
                       const std::string &hour,
                       const std::string &mday,
                       const std::string &month,
                       const std::string &wday) {
  Status s;
  s = verifyAndConvertParam(minute, 0, 59, &schedule_time.minute);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(hour, 0, 23, &schedule_time.hour);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(mday, 1, 31, &schedule_time.mday);
  if (!s.IsOK()) return s;
  s = verifyAndConvertParam(month, 1, 12, &schedule_time.month);
  if (!s.IsOK()) return s;
  return verifyAndConvertParam(wday, 0, 6, &schedule_time.wday);
}

int Cron::IsTimeMatch(struct tm *tm) {
  if ((schedule_time.minute == -1 || tm->tm_min == schedule_time.minute) &&
      (schedule_time.hour == -1 || tm->tm_hour == schedule_time.hour) &&
      (schedule_time.mday == -1 || tm->tm_mday == schedule_time.mday) &&
      (schedule_time.month == -1 || (tm->tm_mon+1) == schedule_time.month) &&
      (schedule_time.wday == -1 || tm->tm_wday == schedule_time.wday)) {
    return 1;
  }
  return 0;
}

Status Cron::verifyAndConvertParam(const std::string &param, int lower_bound, int upper_bound, int *value) {
  if (param == "*") {
    *value = -1;
    return Status::OK();
  }

  try {
    *value = std::stoi(param);
  } catch (const std::invalid_argument &e) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) not an integer or *");
  } catch (const std::out_of_range &e) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) not convertable to int");
  }
  if (*value < lower_bound || *value > upper_bound) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) out of bound");
  }
  return Status::OK();
}