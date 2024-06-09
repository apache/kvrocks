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

#include "cron.h"

#include <gtest/gtest.h>

#include <memory>

class CronTest : public testing::Test {
 protected:
  explicit CronTest() {
    cron_ = std::make_unique<Cron>();
    std::vector<std::string> schedule{"*", "3", "*", "*", "*"};
    auto s = cron_->SetScheduleTime(schedule);
    EXPECT_TRUE(s.IsOK());
  }
  ~CronTest() override = default;

  std::unique_ptr<Cron> cron_;
};

TEST_F(CronTest, IsTimeMatch) {
  std::time_t t = std::time(nullptr);
  std::tm *now = std::localtime(&t);
  now->tm_hour = 3;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 4;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
}

TEST_F(CronTest, ToString) {
  std::string got = cron_->ToString();
  ASSERT_EQ("* 3 * * *", got);
}

class CronTestMinInterval : public testing::Test {
 protected:
  explicit CronTestMinInterval() {
    cron_ = std::make_unique<Cron>();
    std::vector<std::string> schedule{"*/4", "*", "*", "*", "*"};
    auto s = cron_->SetScheduleTime(schedule);
    EXPECT_TRUE(s.IsOK());
  }
  ~CronTestMinInterval() override = default;

  std::unique_ptr<Cron> cron_;
};

TEST_F(CronTestMinInterval, IsTimeMatch) {
  std::time_t t = std::time(nullptr);
  std::tm *now = std::localtime(&t);
  now->tm_hour = 0;
  now->tm_min = 0;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_min = 4;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_min = 8;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_min = 12;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_min = 3;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
  now->tm_min = 99;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
}

TEST_F(CronTestMinInterval, ToString) {
  std::string got = cron_->ToString();
  ASSERT_EQ("*/4 * * * *", got);
}

class CronTestHourInterval : public testing::Test {
 protected:
  explicit CronTestHourInterval() {
    cron_ = std::make_unique<Cron>();
    std::vector<std::string> schedule{"0", "*/4", "*", "*", "*"};
    auto s = cron_->SetScheduleTime(schedule);
    EXPECT_TRUE(s.IsOK());
  }
  ~CronTestHourInterval() override = default;

  std::unique_ptr<Cron> cron_;
};

TEST_F(CronTestHourInterval, IsTimeMatch) {
  std::time_t t = std::time(nullptr);
  std::tm *now = std::localtime(&t);
  now->tm_hour = 0;
  now->tm_min = 0;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 4;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 8;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 12;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 3;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
  now->tm_hour = 55;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
}

TEST_F(CronTestHourInterval, ToString) {
  std::string got = cron_->ToString();
  ASSERT_EQ("0 */4 * * *", got);
}

class CronTestMonthDayInterval : public testing::Test {
 protected:
  explicit CronTestMonthDayInterval() {
    cron_ = std::make_unique<Cron>();
    std::vector<std::string> schedule{"0", "*", "*/4", "*", "*"};
    auto s = cron_->SetScheduleTime(schedule);
    EXPECT_TRUE(s.IsOK());
  }
  ~CronTestMonthDayInterval() override = default;

  std::unique_ptr<Cron> cron_;
};

TEST_F(CronTestMonthDayInterval, IsTimeMatch) {
  std::time_t t = std::time(nullptr);
  std::tm *now = std::localtime(&t);
  now->tm_hour = 0;
  now->tm_min = 0;
  now->tm_mday = 0;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 3;
  now->tm_mday = 4;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 5;
  now->tm_mday = 8;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 6;
  now->tm_mday = 12;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 1;
  now->tm_mday = 3;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
  now->tm_hour = 1;
  now->tm_mday = 99;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
}

TEST_F(CronTestMonthDayInterval, ToString) {
  std::string got = cron_->ToString();
  ASSERT_EQ("0 * */4 * *", got);
}

class CronTestMonthInterval : public testing::Test {
 protected:
  explicit CronTestMonthInterval() {
    cron_ = std::make_unique<Cron>();
    std::vector<std::string> schedule{"0", "*", "*", "*/4", "*"};
    auto s = cron_->SetScheduleTime(schedule);
    EXPECT_TRUE(s.IsOK());
  }
  ~CronTestMonthInterval() override = default;

  std::unique_ptr<Cron> cron_;
};

TEST_F(CronTestMonthInterval, IsTimeMatch) {
  std::time_t t = std::time(nullptr);
  std::tm *now = std::localtime(&t);
  now->tm_hour = 0;
  now->tm_min = 0;
  now->tm_mon = 5;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 5;
  now->tm_mon = 9;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 1;
  now->tm_mon = 3;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
  now->tm_hour = 1;
  now->tm_mon = 99;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
}

TEST_F(CronTestMonthInterval, ToString) {
  std::string got = cron_->ToString();
  ASSERT_EQ("0 * * */4 *", got);
}

class CronTestWeekDayInterval : public testing::Test {
 protected:
  explicit CronTestWeekDayInterval() {
    cron_ = std::make_unique<Cron>();
    std::vector<std::string> schedule{"0", "*", "*", "*", "*/4"};
    auto s = cron_->SetScheduleTime(schedule);
    EXPECT_TRUE(s.IsOK());
  }
  ~CronTestWeekDayInterval() override = default;

  std::unique_ptr<Cron> cron_;
};

TEST_F(CronTestWeekDayInterval, IsTimeMatch) {
  std::time_t t = std::time(nullptr);
  std::tm *now = std::localtime(&t);
  now->tm_hour = 0;
  now->tm_min = 0;
  now->tm_hour = 3;
  now->tm_wday = 4;
  ASSERT_TRUE(cron_->IsTimeMatch(now));
  now->tm_hour = 5;
  now->tm_wday = 3;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
  now->tm_hour = 1;
  now->tm_wday = 99;
  ASSERT_FALSE(cron_->IsTimeMatch(now));
}

TEST_F(CronTestWeekDayInterval, ToString) {
  std::string got = cron_->ToString();
  ASSERT_EQ("0 * * * */4", got);
}



