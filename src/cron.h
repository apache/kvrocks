#pragma once

#include <ctime>
#include <iostream>

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
  Cron(std::string minute, std::string hour, std::string mday, std::string month, std::string wday);
  ~Cron() = default;

  int IsTimeMatch(struct tm *tm);

 private:
  void verifyAndSet(const std::string &token, int &field, const int lower_bound,
                    const int upper_bound, const bool adjust = false);
};