#include "cron.h"

Cron::Cron(std::string *err,
           std::string minute,
           std::string hour,
           std::string mday,
           std::string month,
           std::string wday) {
  verifyAndSet(err, minute, schedule_time.minute, 0, 59);
  verifyAndSet(err, hour, schedule_time.hour, 0, 23);
  verifyAndSet(err, mday, schedule_time.mday, 1, 31);
  verifyAndSet(err, month, schedule_time.month, 1, 12, true);
  verifyAndSet(err, wday, schedule_time.wday, 0, 6);
}

int Cron::IsTimeMatch(struct tm *tm) {
  if ((schedule_time.minute == -1 || tm->tm_min == schedule_time.minute) &&
      (schedule_time.hour == -1 || tm->tm_hour == schedule_time.hour) &&
      (schedule_time.mday == -1 || tm->tm_mday == schedule_time.mday) &&
      (schedule_time.month == -1 || tm->tm_mon == schedule_time.month) &&
      (schedule_time.wday == -1 || tm->tm_wday == schedule_time.wday)) {
    return 1;
  }
  return 0;
}

bool Cron::verifyAndSet(std::string *err, const std::string &token, int &field, const int lower_bound,
                        const int upper_bound, const bool adjust) {
  if (token == "*") {
    field = -1;
  } else {
    try {
      field = std::stoi(token);
    } catch (const std::invalid_argument &e) {
      *err = "malformed cron string (`" + token + "` not an integer or *): ";
      return false;
    } catch (const std::out_of_range &e) {
      *err = "malformed cron string (`" + token + "` not convertable to int): ";
      return false;
    }
    if (field < lower_bound || field > upper_bound) {
      *err = "malformed cron string (`" + token + "` must be <= " + std::to_string(upper_bound) + " and >= "
          + std::to_string(lower_bound);
      return false;
    }
    if (adjust) {
      field--;
    }
  }

  return true;
}