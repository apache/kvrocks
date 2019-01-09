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
  std::time_t t = 1546282800; // 2019.1.1 03:00
  std::tm *now = std::localtime(&t);
  int got = cron->IsTimeMatch(now);
  ASSERT_EQ(1, got);
  t = 1546286400; //2019.1.1 04:00
  now = std::localtime(&t);
  got = cron->IsTimeMatch(now);
  ASSERT_EQ(0, got);
}

TEST_F(CronTest, ToString) {
  std::string got = cron->ToString();
  ASSERT_EQ("* 3 * * *", got);
}
