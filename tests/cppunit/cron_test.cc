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
    cron = std::make_unique<Cron>();
    std::vector<std::string> schedule{"*", "3", "*", "*", "*"};
    auto s = cron->SetScheduleTime(schedule);
    EXPECT_TRUE(s.IsOK());
  }
  ~CronTest() override = default;

  std::unique_ptr<Cron> cron;
};

TEST_F(CronTest, IsTimeMatch) {
  std::time_t t = std::time(nullptr);
  std::tm *now = std::localtime(&t);
  now->tm_hour = 3;
  ASSERT_TRUE(cron->IsTimeMatch(now));
  now->tm_hour = 4;
  ASSERT_FALSE(cron->IsTimeMatch(now));
}

TEST_F(CronTest, ToString) {
  std::string got = cron->ToString();
  ASSERT_EQ("* 3 * * *", got);
}
