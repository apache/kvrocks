#include <gtest/gtest.h>

#include "util.h"

TEST(Util, Host2IP) {
  char ip_buf[16];
  Status s = Util::Host2IP("127.0.0.1", ip_buf, 16);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(std::string("127.0.0.1"), std::string(ip_buf));

  s = Util::Host2IP("localhost", ip_buf, 16);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(std::string("127.0.0.1"), std::string(ip_buf));
}
