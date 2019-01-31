#pragma once

#include <ctime>
#include <iostream>
#include <vector>
#include <string>
#include "status.h"

struct Scheduler {
  int minute;
  int hour;
  int mday;
  int month;
  int wday;

  std::string ToString() const;
};

class Cron {
 public:
  Cron() = default;
  ~Cron() = default;

  Status SetScheduleTime(const std::vector<std::string> &args);
  int IsTimeMatch(struct tm *tm);
  std::string ToString();
  bool IsEnabled();

 private:
  std::vector<Scheduler> schedulers_;

  Status convertToScheduleTime(
      const std::string &minute,
      const std::string &hour,
      const std::string &mday,
      const std::string &month,
      const std::string &wday,
      Scheduler *st);
  Status convertParam(const std::string &param, int lower_bound, int upper_bound, int *value);
};
