#include "cron.h"
#include <gtest/gtest.h>

class CronTest : public testing::Test {
 protected:
  explicit CronTest() {
    cron = new Cron();
    std::vector<std::string> schedule{"*", "3", "*", "*", "*"};
    cron->SetScheduleTime(schedule);
  }
  ~CronTest() {
    delete cron;
  }

 protected:
  Cron *cron;
};

TEST_F(CronTest, IsTimeMatch) {
  std::time_t t = std::time(0);
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
