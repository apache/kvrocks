/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include <ctime>
#include <iostream>
#include <string>
#include <vector>

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
  bool IsTimeMatch(struct tm *tm);
  std::string ToString();
  bool IsEnabled();

 private:
  std::vector<Scheduler> schedulers_;
  struct tm last_tm_ = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, nullptr};

  static StatusOr<Scheduler> convertToScheduleTime(const std::string &minute, const std::string &hour,
                                                   const std::string &mday, const std::string &month,
                                                   const std::string &wday);
  static StatusOr<int> convertParam(const std::string &param, int lower_bound, int upper_bound);
};
