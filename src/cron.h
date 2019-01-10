#pragma once

#include <ctime>
#include <iostream>
#include "status.h"
#include <vector>

struct schedule_time {
  int minute;
  int hour;
  int mday;
  int month;
  int wday;
};

class Cron {
 public:
  explicit Cron() = default;
  ~Cron() = default;

  Status SetScheduleTime(const std::vector<std::string> &args);
  int IsTimeMatch(struct tm *tm);
  std::string ToString();
  bool IsEnabled();

 private:
  const std::string PARAM_ALL = "*";
  const int PARAM_ALL_INT = -1;
  std::vector<schedule_time> schedule_times;

 private:
  Status convertConfToScheduleTime(
      const std::string &minute,
      const std::string &hour,
      const std::string &mday,
      const std::string &month,
      const std::string &wday,
      schedule_time *st);
  Status verifyAndConvertParam(const std::string &param, int lower_bound, int upper_bound, int *value);
  std::string convertScheduleTimeParamToConfParam(const int &param);
};