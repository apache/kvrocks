#include "cron.h"
#include <gtest/gtest.h>

class CronTest : public testing::Test {
 protected:
  explicit CronTest() {
    cron = new Cron();
    cron->SetParams("*", "3", "*", "*", "*");
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
  int got = cron->IsTimeMatch(now);
  ASSERT_EQ(1, got);
  now->tm_hour = 4;
  got = cron->IsTimeMatch(now);
  ASSERT_EQ(0, got);
}

TEST_F(CronTest, ToString) {
  std::string got = cron->ToString();
  ASSERT_EQ("* 3 * * *", got);
}
