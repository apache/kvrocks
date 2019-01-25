#pragma once

#include <ctime>
#include <iostream>
#include <vector>
#include <string>
#include "status.h"

struct schedule_time {
  int minute;
  int hour;
  int mday;
  int month;
  int wday;
};

class Cron {
 public:
  Cron() = default;
  ~Cron() = default;

  Status SetScheduleTime(const std::vector<std::string> &args);
  int IsTimeMatch(struct tm *tm);
  std::string ToString();
  std::vector<std::string> ToConfParamVector();
  bool IsEnabled();

 private:
  const std::string PARAM_WILDCARD = "*";
  const int PARAM_WILDCARD_INT = -1;
  std::vector<schedule_time> schedule_times_;

  Status convertToScheduleTime(
      const std::string &minute,
      const std::string &hour,
      const std::string &mday,
      const std::string &month,
      const std::string &wday,
      schedule_time *st);
  Status convertParam(const std::string &param, int lower_bound, int upper_bound, int *value);
  std::string paramToString(const int &param);
};
