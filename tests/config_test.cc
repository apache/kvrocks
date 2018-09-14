#include "conf.h"
#include <gtest/gtest.h>

TEST(Config, Load) {
  Config config;
  config.Load("test.conf", nullptr);
}
