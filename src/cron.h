#pragma once

#include <ctime>
#include <iostream>
#include "status.h"

class Cron {
 public:
  struct {
    int minute;
    int hour;
    int mday;
    int month;
    int wday;
  } schedule_time;

 public:
  explicit Cron() = default;
  ~Cron() = default;

  int IsTimeMatch(struct tm *tm);
  Status SetParams(
      const std::string &minute,
      const std::string &hour,
      const std::string &mday,
      const std::string &month,
      const std::string &wday);

 private:
  Status verifyAndConvertParam(const std::string &param, int lower_bound, int upper_bound, int *value);
};